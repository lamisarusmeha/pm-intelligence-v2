"""
Paper Trading Engine v4.1 — Kelly Criterion + self-learning + full memory integration.

v4.1 FIXES:
- ARBITRAGE nerfed: Kelly 0.62 (was 0.99), SL 15% (was 100%), cap $100 (was $500)
- ARBITRAGE circuit breaker exemption REMOVED — all strategies go through CB
- ARBITRAGE special Kelly branch REMOVED — uses standard sizing
- memory_system.store_trade_reasoning() called on ENTRY
- memory_system.record_trade_outcome() + llm_agent.evaluate_trade_outcome() called on EXIT
- Removed local _adjust_weights() — SIE handles all weight adjustment exclusively
- _extract_factors() called once, result passed to both blocks
"""

import json
from datetime import datetime, timedelta
from typing import Optional, Tuple
import traceback

import database as db
from trade_explainer import explain_entry, explain_exit, generate_lesson
import self_improvement_engine as sie
import memory_system

try:
    from llm_agent import evaluate_trade_outcome
    HAS_LLM_EVAL = True
except ImportError:
    HAS_LLM_EVAL = False

# Learning error tracking (max 20, FIFO) — exposed via /api/llm/debug
_learning_errors = []

# ── Portfolio Circuit Breakers ─────────────────────────────────────────────────
_daily_pnl = {"date": "", "total": 0.0, "trades_closed": 0}
_session_peak_balance = 0.0
_circuit_breaker_active = False
_circuit_breaker_reason = ""

DAILY_LOSS_LIMIT_PCT = 0.03      # 3% of portfolio
DRAWDOWN_PAUSE_PCT = 0.10        # 10% drawdown from peak

RANGE_BLACKLIST_WORDS = ("between", "be between", "range")

# v4.1 FIX: ARBITRAGE stop-loss changed from 1.0 (never) to 0.15 (15%)
PCT_STOP_LOSS = {
    "BINANCE_ARB": 0.08,
    "SHORT_DURATION": 0.10,
    "NEAR_CERTAINTY": 0.12,
    "VOLUME_SPIKE": 0.15,
    "LLM_ANALYSIS": 0.15,
    "ARBITRAGE": 0.15,          # v4.1 FIX: was 1.0 (never stopped out)
}
DEFAULT_PCT_STOP_LOSS = 0.20


def _market_days_left(market: Optional[dict]) -> float:
    if not market:
        return 9999.0
    end_date_str = market.get("end_date", "")
    if not end_date_str:
        return 9999.0
    try:
        end_date_str = end_date_str.replace("Z", "+00:00")
        if "T" in end_date_str:
            end_dt = datetime.fromisoformat(end_date_str).replace(tzinfo=None)
        else:
            end_dt = datetime.strptime(end_date_str[:10], "%Y-%m-%d")
        return max(0, (end_dt - datetime.utcnow()).total_seconds() / 86400)
    except Exception:
        return 9999.0


def _lock_in_exit_params(market: Optional[dict]) -> Tuple[float, float, float]:
    days = _market_days_left(market)
    if days <= 0.5:
        return 0.12, 0.09, 36.0
    if days <= 2:
        return 0.07, 0.09, 24.0
    return 0.05, 0.09, 12.0


def _is_crypto_market(question: str) -> bool:
    q = question.lower()
    return any(w in q for w in (
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
        "crypto", "xrp", "doge", "up or down",
    ))


# ── Strategy Exit Constants ────────────────────────────────────────────────────

NEAR_CERTAINTY_HOLD_HOURS = 48.0
VOLUME_SPIKE_TP = 0.04
VOLUME_SPIKE_SL = 0.03
VOLUME_SPIKE_HOLD_HOURS = 2.0
BINANCE_ARB_HOLD_HOURS = 0.15
SHORT_DURATION_HOLD_HOURS = 0.5
ARBITRAGE_TP = 0.08          # v4.2: Take profit at +8c (was None/resolution only)
ARBITRAGE_SL = 0.06          # v4.2: Stop loss at -6c (was None/resolution only)
ARBITRAGE_HOLD_HOURS = 24.0  # v4.2: Reduced from 48h — don't hold stale arb bets

COPY_TRADE_TP = 0.04
COPY_TRADE_SL = 0.03
COPY_TRADE_HOLD_HOURS = 2
BUY_NO_EARLY_TP = 0.06
BUY_NO_EARLY_SL = 0.05
BUY_NO_EARLY_HOLD_HOURS = 6
LOCK_IN_TP = 0.05
LOCK_IN_SL = 0.09
LOCK_IN_HOLD_HOURS = 12
MOMENTUM_TP = 0.06
MOMENTUM_SL = 0.04
MOMENTUM_HOLD_HOURS = 2

LLM_ANALYSIS_TP = 0.06
LLM_ANALYSIS_SL = 0.05
LLM_ANALYSIS_HOLD_HOURS = 8.0

# Shared constants
ENTRY_THRESHOLD = 40
MAX_OPEN_TRADES = 40
BASE_RISK_PCT = 0.005
LEARN_RATE = 0.20

# v4.1 FIX: ARBITRAGE Kelly probability lowered from 0.99 to 0.62
KELLY_WIN_PROBS = {
    "NEAR_CERTAINTY": 0.85,
    "VOLUME_SPIKE":   0.65,
    "BINANCE_ARB":    0.72,
    "SHORT_DURATION": 0.80,
    "ARBITRAGE":      0.62,     # v4.1 FIX: was 0.99 (fake "risk-free")
    "COPY_TRADE":     0.75,
    "BUY_NO_EARLY":   0.70,
    "LOCK_IN":        0.78,
    "MOMENTUM":       0.55,
    "LLM_ANALYSIS":   0.65,
}

NO_ALLOWED_TYPES = {
    "BUY_NO_EARLY", "LOCK_IN", "LLM_ANALYSIS",
    "NEAR_CERTAINTY", "VOLUME_SPIKE", "BINANCE_ARB",
    "SHORT_DURATION", "ARBITRAGE",
}

MAX_POS_NEAR_CERT = 100
MAX_POS_BINANCE_ARB = 75
MAX_POS_SHORT_DURATION = 100
MAX_POS_ARBITRAGE = 100         # v4.1 FIX: was 500
MAX_POS_LLM = 150
MAX_POS_DEFAULT = 100


# ── Risk Status ────────────────────────────────────────────────────────────────

def get_risk_status() -> dict:
    return {
        "daily_pnl": round(_daily_pnl.get("total", 0), 2),
        "daily_date": _daily_pnl.get("date", ""),
        "daily_trades_closed": _daily_pnl.get("trades_closed", 0),
        "session_peak_balance": round(_session_peak_balance, 2),
        "circuit_breaker_active": _circuit_breaker_active,
        "circuit_breaker_reason": _circuit_breaker_reason,
        "daily_loss_limit_pct": DAILY_LOSS_LIMIT_PCT,
        "drawdown_pause_pct": DRAWDOWN_PAUSE_PCT,
    }
        "circuit_breaker_reason": _circuit_breaker_reason,
        "daily_loss_limit_pct": DAILY_LOSS_LIMIT_PCT,
        "drawdown_pause_pct": DRAWDOWN_PAUSE_PCT,
        "pct_stop_losses": PCT_STOP_LOSS,
    }


def _update_daily_pnl(pnl: float):
    global _circuit_breaker_active, _circuit_breaker_reason
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if _daily_pnl["date"] != today:
        _daily_pnl["date"] = today
        _daily_pnl["total"] = 0.0
        _daily_pnl["trades_closed"] = 0
        _circuit_breaker_active = False
        _circuit_breaker_reason = ""
    _daily_pnl["total"] += pnl
    _daily_pnl["trades_closed"] += 1


def _check_circuit_breakers(portfolio: dict) -> bool:
    global _circuit_breaker_active, _circuit_breaker_reason, _session_peak_balance

    cash = portfolio.get("cash_balance", 100000)
    invested = portfolio.get("total_invested", 0)
    total_value = cash + invested

    if total_value > _session_peak_balance:
        _session_peak_balance = total_value

    daily_limit = total_value * DAILY_LOSS_LIMIT_PCT
    if _daily_pnl.get("total", 0) < -daily_limit:
        _circuit_breaker_active = True
        _circuit_breaker_reason = f"Daily loss ${_daily_pnl['total']:.2f} exceeds {DAILY_LOSS_LIMIT_PCT*100:.0f}% limit (${-daily_limit:.2f})"
        return True

    if _session_peak_balance > 0:
        drawdown = (_session_peak_balance - total_value) / _session_peak_balance
        if drawdown >= DRAWDOWN_PAUSE_PCT:
            _circuit_breaker_active = True
            _circuit_breaker_reason = f"Drawdown {drawdown*100:.1f}% from peak ${_session_peak_balance:,.0f} exceeds {DRAWDOWN_PAUSE_PCT*100:.0f}% limit"
            return True

    return False


# ── Position Cap ───────────────────────────────────────────────────────────────

def _get_position_cap(signal: dict) -> float:
    market_type = signal.get("market_type", "")
    if market_type == "BINANCE_ARB":
        return MAX_POS_BINANCE_ARB
    if market_type == "SHORT_DURATION":
        return MAX_POS_SHORT_DURATION
    if market_type == "ARBITRAGE":
        return MAX_POS_ARBITRAGE
    if market_type == "LLM_ANALYSIS":
        return MAX_POS_LLM
    if market_type == "NEAR_CERTAINTY":
        return MAX_POS_NEAR_CERT
    return MAX_POS_DEFAULT


# ── Kelly Criterion ────────────────────────────────────────────────────────────

def _kelly_position_size(portfolio: dict, signal: dict) -> float:
    cash = portfolio.get("cash_balance", 10000)
    market_type = signal.get("market_type", "MOMENTUM")
    direction = signal.get("direction", "YES")
    yes_price = signal.get("yes_price", 0.5)
    entry_price = yes_price if direction == "YES" else (1 - yes_price)

    if entry_price <= 0 or entry_price >= 1:
        return round(cash * BASE_RISK_PCT, 2)

    p = KELLY_WIN_PROBS.get(market_type, 0.58)
    q = 1 - p
    b = (1 - entry_price) / entry_price

    if b <= 0:
        return round(cash * BASE_RISK_PCT, 2)

    kelly = (b * p - q) / b
    if kelly <= 0:
        return round(cash * 0.001, 2)

    kelly_frac = kelly * 0.25

    score_bonus = min(0.2, (signal.get("score", 50) - 50) / 250)
    kelly_frac = kelly_frac * (1 + score_bonus)

    bet = cash * kelly_frac

    cap = _get_position_cap(signal)

    # v4.1 FIX: Removed special ARBITRAGE branch that forced 0.3-0.5% bets
    # All strategies now use standard sizing

    # SHORT_DURATION: smaller positions since these resolve fast
    if market_type == "SHORT_DURATION":
        bet = max(cash * 0.001, min(cash * 0.003, bet))
        return round(min(bet, cap), 2)

    # Standard: 0.2%-0.5% per trade
    bet = max(cash * 0.002, min(cash * 0.005, bet))
    return round(min(bet, cap), 2)


# ── Entry ──────────────────────────────────────────────────────────────────────

async def maybe_enter_trade(signal: dict) -> Optional[dict]:
    mt = signal.get("market_type", "?")
    q = signal.get("market_question", "")[:40]

    score = signal.get("score", 0)
    if score < ENTRY_THRESHOLD:
        print(f"[GATE] score {score} < {ENTRY_THRESHOLD} — skip '{q}'")
        return None

    if not signal.get("can_enter", False):
        print(f"[GATE] can_enter=False — skip '{q}'")
        return None

    open_trades = await db.get_open_paper_trades()
    if len(open_trades) >= MAX_OPEN_TRADES:
        print(f"[GATE] max trades ({len(open_trades)}/{MAX_OPEN_TRADES}) — skip")
        return None

    if signal["market_id"] in {t["market_id"] for t in open_trades}:
        return None  # Silent — expected for duplicate markets

    portfolio = await db.get_portfolio()

    # v4.1 FIX: Circuit breaker applies to ALL strategies (removed ARBITRAGE exemption)
    if _check_circuit_breakers(portfolio):
        print(f"[GATE] CIRCUIT BREAKER: {_circuit_breaker_reason}")
        return None

    cost = _kelly_position_size(portfolio, signal)
    if cost < 1.0:
        print(f"[GATE] Kelly size too small (${cost:.2f}) for {mt} — skip '{q}'")
        return None
    if cost > portfolio.get("cash_balance", 0):
        print(f"[GATE] Insufficient cash for ${cost:.2f} — skip")
        return None

    direction = signal.get("direction", "YES")
    market_type = signal.get("market_type", "MOMENTUM")

    if direction == "NO" and market_type not in NO_ALLOWED_TYPES:
        print(f"[GATE] NO not allowed for {market_type} — skip '{q}'")
        return None

    yes_price = signal.get("yes_price", 0.5)
    entry_price = yes_price if direction == "YES" else (1 - yes_price)
    if entry_price <= 0:
        print(f"[GATE] entry_price <= 0 — skip '{q}'")
        return None

    # Range market blacklist
    question_lower = signal.get("market_question", "").lower()
    if market_type in ("NEAR_CERTAINTY", "LLM_ANALYSIS"):
        if any(word in question_lower for word in RANGE_BLACKLIST_WORDS):
            print(f"[GATE] RANGE market blacklisted – skip '{q}'")
            return None

    # Extreme price guard
    max_price = 0.93 if market_type == "NEAR_CERTAINTY" else 0.95
    if entry_price > max_price or entry_price < 0.05:
        print(f"[GATE] EXTREME price {entry_price:.4f} (max={max_price}) – skip '{q}'")
        return None

    shares = round(cost / entry_price, 4)
    signal_id = await db.save_signal(signal)
    now = datetime.utcnow().isoformat()

    trade = {
        "signal_id": signal_id,
        "market_id": signal["market_id"],
        "market_question": signal["market_question"],
        "direction": direction,
        "entry_price": entry_price,
        "shares": shares,
        "cost": cost,
        "market_type": market_type,
        "status": "OPEN",
        "created_at": now,
    }
    trade_id = await db.save_paper_trade(trade)
    trade["id"] = trade_id

    await db.update_portfolio(cash_delta=-cost, invested_delta=cost)

    # ── TRADE EXPLANATION ──────────────────────────────────────────────────
    try:
        entry_expl = explain_entry(signal, trade)
        await db.save_trade_explanation({
            "trade_id": trade_id,
            "market_question": signal["market_question"],
            "direction": direction,
            "entry_explanation": entry_expl,
            "factors_json": signal.get("factors_json", "{}"),
            "score": score,
            "created_at": now,
        })
    except Exception as e:
        print(f"[EXPLAINER] Entry failed: {e}")

    # ── v4.1 NEW: STORE TRADE REASONING IN MEMORY SYSTEM ──────────────────
    try:
        factors_raw = signal.get("factors_json", "{}")
        if isinstance(factors_raw, str):
            factors = json.loads(factors_raw)
        else:
            factors = factors_raw

        await memory_system.store_trade_reasoning(
            trade_id=trade_id,
            market_question=signal["market_question"],
            direction=direction,
            entry_price=entry_price,
            confidence=signal.get("confidence", 0),
            estimated_probability=factors.get("estimated_probability", 0.5),
            edge=factors.get("edge", 0),
            reasoning=factors.get("reasoning", signal.get("entry_reason", "")),
            model=factors.get("model", market_type),
            category=signal.get("category", ""),
        )
    except Exception as e:
        print(f"[MEMORY] Store reasoning failed: {e}")

    print(f"[TRADE] {market_type} {direction} "
          f"'{signal['market_question'][:48]}' "
          f"@ {entry_price:.3f} | ${cost:.2f} | score={score:.0f} | "
          f"{signal.get('entry_reason', '')}")
    return trade


# ── Exit Helpers ───────────────────────────────────────────────────────────────

async def _close_at_price(trade: dict, exit_price: float, reason: str):
    """Close a trade, update portfolio, trigger ALL learning systems."""
    trade_id = trade["id"]
    shares = trade["shares"]
    cost = trade["cost"]
    direction = trade.get("direction", "YES")

    payout = shares * exit_price
    pnl = round(payout - cost, 2)
    won = pnl > 0

    outcome = "WIN" if won else "LOSS"
    if reason in ("STOP_LOSS", "TIMEOUT") and not won:
        outcome = reason

    await db.close_paper_trade(trade_id, exit_price, pnl, outcome)
    await db.update_portfolio(cash_delta=payout, pnl_delta=pnl,
                               invested_delta=-cost, win=won)

    # Update daily P&L for circuit breaker
    _update_daily_pnl(pnl)

    pnl_pct = round((pnl / cost) * 100, 1) if cost else 0
    try:
        await db.resolve_signal(trade["signal_id"], outcome, pnl_pct)
    except Exception:
        pass

    # ── v4.1 FIX: Extract factors ONCE, pass to all blocks ─────────────────
    factors = {}
    try:
        factors = _extract_factors(trade_id, await db.get_trade_explanations(200))
    except Exception as e:
        err = {"ts": datetime.utcnow().isoformat(), "stage": "factor_extraction",
               "trade_id": trade_id, "error": str(e), "tb": traceback.format_exc()}
        _learning_errors.append(err)
        if len(_learning_errors) > 20:
            _learning_errors.pop(0)
        print(f"[SELF-IMPROVE] Factor extraction failed: {e}")

    # ── SELF-LEARNING: Record to SIE (handles weight adjustment) ───────────
    try:
        await sie.record_trade_result(
            trade_id=trade_id,
            market_type=trade.get("market_type", "UNKNOWN"),
            direction=direction,
            entry_price=trade.get("entry_price", 0),
            exit_price=exit_price,
            pnl=pnl,
            won=won,
            signal_factors=factors,
        )
    except Exception as e:
        err = {"ts": datetime.utcnow().isoformat(), "stage": "record_trade_result",
               "trade_id": trade_id, "error": str(e), "tb": traceback.format_exc()}
        _learning_errors.append(err)
        if len(_learning_errors) > 20:
            _learning_errors.pop(0)
        print(f"[SELF-IMPROVE] Record failed: {e}")

    # ── v4.1 NEW: LLM LESSON EXTRACTION ────────────────────────────────────
    lesson_text = ""
    if HAS_LLM_EVAL:
        try:
            original_reasoning = factors.get("reasoning", "")
            lesson_text = await evaluate_trade_outcome(
                trade={**trade, "exit_price": exit_price, "pnl": pnl},
                original_reasoning=original_reasoning,
                outcome=outcome,
                pnl=pnl,
            ) or ""
        except Exception as e:
            print(f"[LLM] Lesson extraction failed: {e}")

    # ── v4.1 NEW: RECORD OUTCOME IN MEMORY SYSTEM ─────────────────────────
    try:
        await memory_system.record_trade_outcome(
            trade_id=trade_id,
            exit_price=exit_price,
            pnl=pnl,
            outcome=outcome,
            lesson=lesson_text,
        )
    except Exception as e:
        print(f"[MEMORY] Record outcome failed: {e}")

    # ── TRADE EXPLANATION (uses pre-extracted factors) ──────────────────────
    try:
        weights = await db.get_signal_weights()
        trade["exit_price"] = exit_price
        trade["pnl"] = pnl

        exit_expl = explain_exit(trade, reason, pnl)
        lesson = generate_lesson(factors, pnl, weights, reason)
        await db.update_trade_explanation_exit(trade_id, exit_expl, lesson, outcome, pnl)

        # v4.1 FIX: Removed local _adjust_weights() call
        # SIE handles all weight adjustment exclusively via run_improvement_cycle()
    except Exception as e:
        print(f"[EXPLAINER] Exit failed: {e}")

    # ── LOG ────────────────────────────────────────────────────────────────
    pnl_sign = "+" if pnl >= 0 else ""
    emoji = "+" if won else "X"
    mode = trade.get("market_type", "?")
    print(f"[TRADE] {emoji} CLOSE [{mode}] {direction} "
          f"'{trade['market_question'][:38]}' "
          f"@ {exit_price:.3f} | PNL={pnl_sign}{pnl:.2f} | {outcome}"
          f"{' | lesson: ' + lesson_text[:60] if lesson_text else ''}")


def _extract_factors(trade_id: int, explanations: list) -> dict:
    for ex in explanations:
        if ex.get("trade_id") == trade_id:
            raw = ex.get("factors_json", ex.get("factors", "{}"))
            if isinstance(raw, dict):
                return raw
            try:
                return json.loads(raw) if isinstance(raw, str) else {}
            except Exception:
                return {}
    return {}


# v4.1 FIX: _adjust_weights() REMOVED entirely
# SIE (self_improvement_engine) handles all weight adjustment via run_improvement_cycle()
# Having two systems (SIE + local _adjust_weights) caused oscillating, unstable weights


# ── Leverage (disabled) ────────────────────────────────────────────────────────

async def maybe_enter_leverage_trade(signal: dict) -> Optional[dict]:
    return None

async def check_leverage_exits(markets_by_id: dict):
    open_lev = await db.get_open_leverage_trades()
    if not open_lev:
        return
    now = datetime.utcnow()
    for trade in open_lev:
        try:
            created = datetime.fromisoformat(trade["created_at"])
            age_hours = (now - created).total_seconds() / 3600
            if age_hours > 4:
                market = markets_by_id.get(trade["market_id"])
                entry_px = trade.get("entry_price", 0)
                direction = trade.get("direction", "YES")
                cost = trade.get("cost", 0)
                yes_price = market.get("yes_price") if market else None
                cur_price = (yes_price if direction == "YES" else (1 - yes_price)) if yes_price is not None else entry_px
                pnl = round((cur_price - entry_px) * trade["shares"], 2)
                won = pnl > 0
                outcome = "WIN" if won else "LOSS"
                await db.close_leverage_trade(trade["id"], cur_price, pnl, outcome)
                payout = cost + pnl
                await db.update_leverage_portfolio(cash_delta=payout, invested_delta=-cost)
                print(f"[LEV] DRAIN {direction} '{trade['market_question'][:35]}' PNL=${pnl:+.2f}")
        except Exception:
            continue


# ── Exit Scan ──────────────────────────────────────────────────────────────────

async def check_exits(markets_by_id: dict):
    open_trades = await db.get_open_paper_trades()
    if not open_trades:
        return

    now = datetime.utcnow()
    for trade in open_trades:
        market_id = trade["market_id"]
        entry_px = trade.get("entry_price", 0)
        direction = trade.get("direction", "YES")
        market_type = trade.get("market_type", "MOMENTUM")

        try:
            created = datetime.fromisoformat(trade["created_at"])
        except Exception:
            continue

        market = markets_by_id.get(market_id)

        # Determine exit params by market type
        if market_type == "NEAR_CERTAINTY":
            take_profit_delta = None
            stop_loss_delta = None
            max_hold_hours = NEAR_CERTAINTY_HOLD_HOURS
        elif market_type == "SHORT_DURATION":
            take_profit_delta = None
            stop_loss_delta = None
            max_hold_hours = SHORT_DURATION_HOLD_HOURS
        elif market_type == "VOLUME_SPIKE":
            take_profit_delta = VOLUME_SPIKE_TP
            stop_loss_delta = VOLUME_SPIKE_SL
            max_hold_hours = VOLUME_SPIKE_HOLD_HOURS
        elif market_type == "BINANCE_ARB":
            take_profit_delta = None
            stop_loss_delta = None
            max_hold_hours = BINANCE_ARB_HOLD_HOURS
        elif market_type == "ARBITRAGE":
            take_profit_delta = ARBITRAGE_TP    # v4.2: was None
            stop_loss_delta = ARBITRAGE_SL      # v4.2: was None
            max_hold_hours = ARBITRAGE_HOLD_HOURS
        elif market_type == "LLM_ANALYSIS":
            take_profit_delta = LLM_ANALYSIS_TP
            stop_loss_delta = LLM_ANALYSIS_SL
            max_hold_hours = LLM_ANALYSIS_HOLD_HOURS
        elif market_type == "COPY_TRADE":
            take_profit_delta = COPY_TRADE_TP
            stop_loss_delta = COPY_TRADE_SL
            max_hold_hours = COPY_TRADE_HOLD_HOURS
        elif market_type == "BUY_NO_EARLY":
            take_profit_delta = BUY_NO_EARLY_TP
            stop_loss_delta = BUY_NO_EARLY_SL
            max_hold_hours = BUY_NO_EARLY_HOLD_HOURS
        elif market_type == "LOCK_IN":
            take_profit_delta, stop_loss_delta, max_hold_hours = _lock_in_exit_params(market)
        else:
            take_profit_delta = MOMENTUM_TP
            stop_loss_delta = MOMENTUM_SL
            max_hold_hours = MOMENTUM_HOLD_HOURS

        # Get current price
        yes_price = None
        if market:
            yes_price = market.get("yes_price")
        cur_price = (yes_price if direction == "YES" else (1 - yes_price)) if yes_price is not None else entry_px

        # Timeout check
        age_hours = (now - created).total_seconds() / 3600
        if age_hours > max_hold_hours:
            await _close_at_price(trade, cur_price, "TIMEOUT")
            continue

        # Market closed/resolved
        if market and market.get("closed"):
            reason = "TAKE_PROFIT" if cur_price > entry_px else "STOP_LOSS"
            await _close_at_price(trade, cur_price, reason)
            continue

        # Market not in fetch
        if yes_price is None:
            if age_hours > 4:
                await _close_at_price(trade, entry_px, "TIMEOUT")
            continue

        # Early resolution guard
        if direction == "YES" and yes_price < 0.04:
            await _close_at_price(trade, cur_price, "STOP_LOSS")
            continue
        if direction == "NO" and yes_price > 0.96:
            await _close_at_price(trade, cur_price, "STOP_LOSS")
            continue

        # Resolution-based strategies: close on resolution + percentage stop-loss
        # v4.2: ARBITRAGE removed — now uses delta TP/SL like other strategies
        if market_type in ("NEAR_CERTAINTY", "SHORT_DURATION", "BINANCE_ARB"):
            # Take profit: resolved in our favor
            if direction == "YES" and yes_price >= 0.97:
                await _close_at_price(trade, cur_price, "TAKE_PROFIT")
                continue
            if direction == "NO" and yes_price <= 0.03:
                await _close_at_price(trade, cur_price, "TAKE_PROFIT")
                continue
            # Stop loss: resolved against us
            if direction == "YES" and yes_price <= 0.05:
                await _close_at_price(trade, cur_price, "STOP_LOSS")
                continue
            if direction == "NO" and yes_price >= 0.95:
                await _close_at_price(trade, cur_price, "STOP_LOSS")
                continue

            # Percentage stop-loss for resolution-based strategies
            if entry_px > 0:
                loss_pct = (cur_price - entry_px) / entry_px
                stop_threshold = PCT_STOP_LOSS.get(market_type, DEFAULT_PCT_STOP_LOSS)
                if market_type == "NEAR_CERTAINTY" and _is_crypto_market(trade.get("market_question", "")):
                    stop_threshold = 0.10
                if loss_pct <= -stop_threshold:
                    print(f"[SL] {market_type} {stop_threshold*100:.0f}% stop hit (loss={loss_pct*100:.1f}%)")
                    await _close_at_price(trade, cur_price, "STOP_LOSS")
                    continue
            continue

        # Standard TP/SL for all other types (including ARBITRAGE v4.2)
        if take_profit_delta is None or stop_loss_delta is None:
            continue

        move = cur_price - entry_px
        if move >= take_profit_delta:
            await _close_at_price(trade, cur_price, "TAKE_PROFIT")
        elif move <= -stop_loss_delta:
            await _close_at_price(trade, cur_price, "STOP_LOSS")
