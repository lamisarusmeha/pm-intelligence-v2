"""
PM Intelligence v4.1 — Self-Improvement Engine

The actual brain of the bot. Learns from every closed trade.

v4.1 FIXES:
- THRESHOLD_BOUNDS expanded from 4 legacy strategies to ALL 9 strategy types
- DEFAULT_THRESHOLDS expanded to ALL 9 strategy types
- get_current_thresholds() returns all 9 types
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import database as db

logger = logging.getLogger(__name__)

# -- Configuration --

WIN_RATE_TARGET     = 0.80   # 80% overall win rate goal
RETRAIN_EVERY       = 5      # full re-evaluation after every N closed trades (was 20 — learn faster)
MIN_SAMPLE_SIZE     = 3      # minimum trades before adjusting a strategy (was 5 — adapt sooner)
DISABLE_THRESHOLD   = 0.25   # disable strategy if WR < 25% with >= MIN_SAMPLE_SIZE

# v4.1 FIX: Per-strategy threshold bounds for ALL 9 strategy types (was only 4)
THRESHOLD_BOUNDS = {
    # Legacy heuristic strategies
    "COPY_TRADE":       (70, 95),
    "LOCK_IN":          (55, 85),
    "BUY_NO_EARLY":     (45, 80),
    "MOMENTUM":         (40, 75),
    # v4.0+ strategies
    "NEAR_CERTAINTY":   (60, 95),
    "VOLUME_SPIKE":     (50, 85),
    "BINANCE_ARB":      (55, 90),
    "SHORT_DURATION":   (50, 85),
    "ARBITRAGE":        (60, 90),
    # LLM-driven
    "LLM_ANALYSIS":     (50, 90),
}

# v4.1 FIX: Default starting thresholds for ALL 9 strategy types (was only 4)
DEFAULT_THRESHOLDS = {
    # Legacy heuristic strategies
    "COPY_TRADE":       85,
    "LOCK_IN":          65,
    "BUY_NO_EARLY":     60,
    "MOMENTUM":         50,
    # v4.0+ strategies
    "NEAR_CERTAINTY":   75,
    "VOLUME_SPIKE":     65,
    "BINANCE_ARB":      70,
    "SHORT_DURATION":   65,
    "ARBITRAGE":        75,
    # LLM-driven
    "LLM_ANALYSIS":     65,
}


# -- Core functions --

async def record_trade_result(trade_id: int, market_type: str, direction: str,
                               entry_price: float, exit_price: float,
                               pnl: float, won: bool,
                               signal_factors: Optional[dict] = None):
    """
    Call this every time a paper trade closes.
    Records the result into signal_performance table for the learning loop.
    """
    async with db.aiosqlite.connect(db.DB_PATH) as conn:
        await conn.execute("""
            INSERT INTO signal_performance
                (trade_id, market_type, direction, entry_price, exit_price,
                 pnl, won, signal_factors_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade_id, market_type, direction,
            entry_price, exit_price, pnl,
            1 if won else 0,
            json.dumps(signal_factors or {}),
            datetime.utcnow().isoformat()
        ))
        await conn.commit()

    # Trigger full re-evaluation every RETRAIN_EVERY closed trades
    total_closed = await _count_total_closed()
    if total_closed > 0 and total_closed % RETRAIN_EVERY == 0:
        logger.info(f"[SELF-IMPROVE] Reached {total_closed} closed trades -- triggering full re-evaluation")
        await run_improvement_cycle()


async def run_improvement_cycle():
    """
    The main improvement loop. Called automatically every RETRAIN_EVERY trades.

    Steps:
    1. Calculate performance per strategy type
    2. Compare to 80% target
    3. Adjust thresholds and weights
    4. Log every change
    5. Update strategy_params in DB so signal_engine picks them up
    """
    logger.info("[SELF-IMPROVE] === Running improvement cycle ===")

    stats = await _get_performance_by_type()
    if not stats:
        logger.info("[SELF-IMPROVE] No closed trades yet -- nothing to learn from")
        return

    overall_wr = await _get_overall_win_rate()
    gap = WIN_RATE_TARGET - overall_wr

    logger.info(f"[SELF-IMPROVE] Overall WR: {overall_wr:.1%} | Target: {WIN_RATE_TARGET:.1%} | Gap: {gap:+.1%}")

    # Load current params from DB
    current_params = await _load_strategy_params()
    new_params = dict(current_params)
    changes = []

    for strategy, perf in stats.items():
        wr     = perf["win_rate"]
        trades = perf["total_trades"]
        avg_pnl = perf["avg_pnl"]

        if trades < MIN_SAMPLE_SIZE:
            logger.info(f"[SELF-IMPROVE] {strategy}: only {trades} trades -- need {MIN_SAMPLE_SIZE} to adjust")
            continue

        current_threshold = current_params.get(f"{strategy}_threshold", DEFAULT_THRESHOLDS.get(strategy, 60))
        current_enabled   = current_params.get(f"{strategy}_enabled", True)
        lo, hi = THRESHOLD_BOUNDS.get(strategy, (40, 90))

        # --- Decision logic ---

        # DISABLE: consistently terrible performance
        if wr < DISABLE_THRESHOLD and trades >= MIN_SAMPLE_SIZE:
            if current_enabled:
                new_params[f"{strategy}_enabled"] = False
                changes.append({
                    "strategy": strategy,
                    "param": "enabled",
                    "old": True, "new": False,
                    "reason": f"WR={wr:.1%} < disable threshold {DISABLE_THRESHOLD:.1%} after {trades} trades"
                })
            continue

        # RE-ENABLE: was disabled but performance improved
        if not current_enabled and wr >= 0.45 and trades >= MIN_SAMPLE_SIZE * 2:
            new_params[f"{strategy}_enabled"] = True
            changes.append({
                "strategy": strategy,
                "param": "enabled",
                "old": False, "new": True,
                "reason": f"Re-enabling: WR improved to {wr:.1%} over {trades} trades"
            })

        # THRESHOLD ADJUSTMENT
        new_threshold = current_threshold

        if wr < 0.40:
            adjust = min(8, int((0.40 - wr) * 40))
            new_threshold = min(hi, current_threshold + adjust)
            reason = f"WR={wr:.1%} too low -- raising threshold to be more selective"

        elif wr < 0.60:
            adjust = min(4, int((0.60 - wr) * 20))
            new_threshold = min(hi, current_threshold + adjust)
            reason = f"WR={wr:.1%} below 60% -- raising threshold slightly"

        elif wr >= WIN_RATE_TARGET and gap < 0:
            new_threshold = current_threshold
            reason = f"WR={wr:.1%} at/above target -- maintaining threshold"

        elif wr >= WIN_RATE_TARGET and gap > 0.05:
            adjust = min(3, int((wr - WIN_RATE_TARGET) * 15))
            new_threshold = max(lo, current_threshold - adjust)
            reason = f"WR={wr:.1%} strong but overall gap is {gap:+.1%} -- lowering threshold to get more trades"

        elif wr >= 0.70:
            new_threshold = max(lo, current_threshold - 2)
            reason = f"WR={wr:.1%} performing well -- minor threshold reduction"

        else:
            new_threshold = current_threshold
            reason = f"WR={wr:.1%} within acceptable range -- no change needed"

        new_threshold = round(new_threshold)

        if new_threshold != current_threshold:
            new_params[f"{strategy}_threshold"] = new_threshold
            changes.append({
                "strategy": strategy,
                "param": "threshold",
                "old": current_threshold,
                "new": new_threshold,
                "reason": reason,
                "stats": {"wr": round(wr, 3), "trades": trades, "avg_pnl": round(avg_pnl, 4)}
            })

    # Update signal weights based on relative performance
    weight_changes = await _adjust_signal_weights(stats, overall_wr)
    changes.extend(weight_changes)

    # Save everything to DB
    await _save_strategy_params(new_params)
    await _log_improvement_run(overall_wr, gap, stats, changes)

    if changes:
        logger.info(f"[SELF-IMPROVE] Made {len(changes)} parameter changes:")
        for c in changes:
            logger.info(f"  -> {c['strategy']}.{c['param']}: {c['old']} -> {c['new']} | {c['reason']}")
    else:
        logger.info("[SELF-IMPROVE] No changes needed -- parameters look good")

    logger.info(f"[SELF-IMPROVE] === Cycle complete. Next at {RETRAIN_EVERY} more closed trades ===")
    return changes


async def _adjust_signal_weights(stats: dict, overall_wr: float) -> list:
    """
    Re-weight the scoring factors based on which ones correlate
    with winning trades vs losing trades.
    """
    changes = []

    try:
        factor_performance = await _get_factor_win_correlation()
        if not factor_performance:
            return changes

        current_weights = await db.get_signal_weights()

        for factor, corr_data in factor_performance.items():
            if corr_data["total"] < MIN_SAMPLE_SIZE:
                continue

            win_avg   = corr_data.get("win_avg_score", 50)
            lose_avg  = corr_data.get("lose_avg_score", 50)
            current_w = current_weights.get(factor, 1.0)

            diff = win_avg - lose_avg

            if diff > 15:
                new_w = min(4.0, round(current_w + 0.3, 2))
                if new_w != current_w:
                    changes.append({
                        "strategy": f"weight:{factor}",
                        "param": "weight",
                        "old": current_w, "new": new_w,
                        "reason": f"Factor {factor} scores {diff:.1f}pts higher in wins -- increasing weight"
                    })
                    current_weights[factor] = new_w

            elif diff < -15:
                new_w = max(0.3, round(current_w - 0.2, 2))
                if new_w != current_w:
                    changes.append({
                        "strategy": f"weight:{factor}",
                        "param": "weight",
                        "old": current_w, "new": new_w,
                        "reason": f"Factor {factor} scores {abs(diff):.1f}pts higher in losses -- reducing weight"
                    })
                    current_weights[factor] = new_w

        if changes:
            await db.set_signal_weights(current_weights)

    except Exception as e:
        logger.warning(f"[SELF-IMPROVE] Weight adjustment failed: {e}")

    return changes


async def get_current_thresholds() -> dict:
    """
    Called by signal_engine.py to get the current dynamic thresholds.
    v4.1: Returns all 9 strategy types (was only 4 legacy).
    """
    params = await _load_strategy_params()
    result = {}
    for strategy, default in DEFAULT_THRESHOLDS.items():
        result[f"{strategy}_threshold"] = params.get(f"{strategy}_threshold", default)
        # Default enabled state: MOMENTUM starts disabled, everything else enabled
        default_enabled = strategy != "MOMENTUM"
        result[f"{strategy}_enabled"] = params.get(f"{strategy}_enabled", default_enabled)
    return result


async def get_performance_summary() -> dict:
    """Returns a summary of current bot performance for the dashboard."""
    overall_wr    = await _get_overall_win_rate()
    total_closed  = await _count_total_closed()
    stats_by_type = await _get_performance_by_type()
    last_run      = await _get_last_improvement_run()
    params        = await _load_strategy_params()

    return {
        "overall_win_rate":    round(overall_wr, 3),
        "target_win_rate":     WIN_RATE_TARGET,
        "gap_to_target":       round(WIN_RATE_TARGET - overall_wr, 3),
        "total_closed_trades": total_closed,
        "next_retrain_at":     RETRAIN_EVERY - (total_closed % RETRAIN_EVERY),
        "strategy_performance": stats_by_type,
        "current_thresholds":  {
            k: v for k, v in params.items() if "threshold" in k or "enabled" in k
        },
        "last_improvement_run": last_run,
    }


# -- DB helpers --

async def _count_total_closed() -> int:
    async with db.aiosqlite.connect(db.DB_PATH) as conn:
        row = await (await conn.execute(
            "SELECT COUNT(*) FROM signal_performance"
        )).fetchone()
        return row[0] if row else 0


async def _get_overall_win_rate() -> float:
    async with db.aiosqlite.connect(db.DB_PATH) as conn:
        row = await (await conn.execute(
            "SELECT COUNT(*), SUM(won) FROM signal_performance"
        )).fetchone()
        if not row or not row[0]:
            return 0.0
        return (row[1] or 0) / row[0]


async def _get_performance_by_type() -> Dict[str, Any]:
    async with db.aiosqlite.connect(db.DB_PATH) as conn:
        rows = await (await conn.execute("""
            SELECT market_type,
                   COUNT(*) as total,
                   SUM(won) as wins,
                   AVG(pnl) as avg_pnl,
                   MAX(pnl) as best_pnl,
                   MIN(pnl) as worst_pnl
            FROM signal_performance
            GROUP BY market_type
        """)).fetchall()

    result = {}
    for row in rows:
        mt, total, wins, avg_pnl, best, worst = row
        result[mt] = {
            "total_trades": total,
            "wins":         wins or 0,
            "losses":       total - (wins or 0),
            "win_rate":     (wins or 0) / total if total else 0.0,
            "avg_pnl":      avg_pnl or 0.0,
            "best_pnl":     best or 0.0,
            "worst_pnl":    worst or 0.0,
        }
    return result


async def _get_factor_win_correlation() -> Dict[str, Any]:
    """
    For each signal factor, calculate average score in winning vs losing trades.
    """
    async with db.aiosqlite.connect(db.DB_PATH) as conn:
        rows = await (await conn.execute(
            "SELECT won, signal_factors_json FROM signal_performance WHERE signal_factors_json != '{}'"
        )).fetchall()

    if not rows:
        return {}

    factor_data: Dict[str, Dict] = {}
    for won, factors_json in rows:
        try:
            factors = json.loads(factors_json)
        except Exception:
            continue

        for factor, score in factors.items():
            if factor == "days_left":
                continue
            if factor not in factor_data:
                factor_data[factor] = {"win_scores": [], "lose_scores": [], "total": 0}
            factor_data[factor]["total"] += 1
            if won:
                factor_data[factor]["win_scores"].append(score)
            else:
                factor_data[factor]["lose_scores"].append(score)

    result = {}
    for factor, data in factor_data.items():
        win_scores  = data["win_scores"]
        lose_scores = data["lose_scores"]
        result[factor] = {
            "total":          data["total"],
            "win_avg_score":  sum(win_scores)  / len(win_scores)  if win_scores  else 50,
            "lose_avg_score": sum(lose_scores) / len(lose_scores) if lose_scores else 50,
        }
    return result


async def _load_strategy_params() -> dict:
    async with db.aiosqlite.connect(db.DB_PATH) as conn:
        rows = await (await conn.execute(
            "SELECT param_name, param_value FROM strategy_params"
        )).fetchall()

    params = {}
    for name, value in rows:
        if value in ("True", "False"):
            params[name] = value == "True"
        else:
            try:
                params[name] = float(value)
            except Exception:
                params[name] = value
    return params


async def _save_strategy_params(params: dict):
    async with db.aiosqlite.connect(db.DB_PATH) as conn:
        for name, value in params.items():
            await conn.execute("""
                INSERT INTO strategy_params (param_name, param_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(param_name) DO UPDATE SET
                    param_value = excluded.param_value,
                    updated_at  = excluded.updated_at
            """, (name, str(value), datetime.utcnow().isoformat()))
        await conn.commit()


async def _log_improvement_run(overall_wr: float, gap: float,
                                stats: dict, changes: list):
    async with db.aiosqlite.connect(db.DB_PATH) as conn:
        await conn.execute("""
            INSERT INTO improvement_log
                (overall_win_rate, gap_to_target, stats_json, changes_json, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            round(overall_wr, 4),
            round(gap, 4),
            json.dumps(stats),
            json.dumps(changes),
            datetime.utcnow().isoformat()
        ))
        await conn.commit()


async def _get_last_improvement_run() -> Optional[str]:
    async with db.aiosqlite.connect(db.DB_PATH) as conn:
        row = await (await conn.execute(
            "SELECT created_at FROM improvement_log ORDER BY id DESC LIMIT 1"
        )).fetchone()
    return row[0] if row else None
