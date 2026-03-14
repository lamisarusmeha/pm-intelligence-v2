"""
Binance Momentum Arbitrage v5.1 — BTC-focused last-30-second edge exploitation.

Core edge: Polymarket's rolling 5M/15M crypto markets resolve based on the same
underlying price that Binance shows in real-time. But Polymarket odds are SLOW to
reprice. If BTC moved +0.1% on Binance with 25 seconds left, the "Up" side should
be ~90%+ but Polymarket might still show 0.60. That's free money.

v5.1 CHANGES (user-discovered edge):
- BTC is PRIMARY target (most liquid on both Binance and Polymarket)
- Last-30-second entries get MAXIMUM priority (highest win rate)
- Lowered move threshold for last-30s (even 0.1% is reliable when time is short)
- All 7 assets supported but BTC gets aggressive parameters
- Both 5M and 15M timeframes

Called every loop from main.py (speed critical — these windows are short).
"""

import re
from datetime import datetime, timedelta
from typing import List, Optional

try:
    from binance_feed import get_price, get_status as get_binance_status
except ImportError:
    def get_price(symbol): return 0
    def get_binance_status(): return {}


# -- Configuration --

# Standard thresholds (>30 seconds remaining)
ARB_MOVE_THRESHOLD     = 0.0015   # 0.15% minimum Binance move
ARB_MAX_POLY_PRICE     = 0.88     # Max Polymarket price for our side
ARB_MIN_EDGE           = 0.05     # 5% minimum edge

# AGGRESSIVE thresholds for last-30-second window (the discovered edge)
# User confirmed: entering BTC 5M in last 30s is "guaranteed profit"
LAST30_MOVE_THRESHOLD  = 0.0002   # 0.02% — any detectable Binance move is enough
LAST30_MAX_POLY_PRICE  = 0.95     # Allow very high entry — resolution is imminent
LAST30_MIN_EDGE        = 0.01     # 1% edge is enough when resolution is seconds away

# Timing
ARB_MIN_SECS_LEFT      = 5        # Enter up to 5 seconds before close
ARB_MAX_SECS_LEFT      = 120      # Don't enter too early on 5M markets
ARB_MAX_SECS_LEFT_15M  = 300      # 15M markets: wider window

# BTC priority boost
BTC_SCORE_BONUS        = 5        # BTC markets get +5 score (most liquid/reliable)

# -- Module State --

_arb_reference_prices = {}   # {market_id: {"ref_price": float, "window_start": datetime, "asset": str}}
_arb_entered_markets = set()  # Track entered markets to prevent double-entry

# -- Rolling market detection --

_ROLLING_SLUG_PATTERN = re.compile(
    r"(btc|eth|sol|xrp|bnb|doge|hype)-updown-(5|15)m-\d+", re.IGNORECASE
)

_ASSET_MAP = {
    "btc": "BTC", "eth": "ETH", "sol": "SOL", "xrp": "XRP",
    "bnb": "BNB", "doge": "DOGE", "hype": "HYPE",
}


def _is_rolling_crypto_market(market: dict) -> Optional[dict]:
    """
    Detect rolling 5m/15m crypto up/down markets.
    Returns {"asset": str, "timeframe": int} or None.
    """
    slug = market.get("slug", "").lower()

    match = _ROLLING_SLUG_PATTERN.match(slug)
    if match:
        asset_key = match.group(1).lower()
        tf = int(match.group(2))
        asset = _ASSET_MAP.get(asset_key, asset_key.upper())
        return {"asset": asset, "timeframe": tf}

    # Fallback: question-based
    question = market.get("question", "").lower()
    for key, symbol in _ASSET_MAP.items():
        if key in question and ("5 min" in question or "5-min" in question or "5m" in question):
            if "up" in question or "down" in question:
                return {"asset": symbol, "timeframe": 5}
        if key in question and ("15 min" in question or "15-min" in question or "15m" in question):
            if "up" in question or "down" in question:
                return {"asset": symbol, "timeframe": 15}

    return None


def _estimate_seconds_remaining(market: dict) -> float:
    """Estimate seconds until market closes based on end_date."""
    end_date_str = market.get("end_date", "")
    if not end_date_str:
        return 9999.0
    try:
        end_date_str = end_date_str.replace("Z", "+00:00")
        if "T" in end_date_str:
            end_dt = datetime.fromisoformat(end_date_str).replace(tzinfo=None)
        else:
            end_dt = datetime.strptime(end_date_str[:10], "%Y-%m-%d")
        return max(0, (end_dt - datetime.utcnow()).total_seconds())
    except Exception:
        return 9999.0


# -- Main Signal Generator --

def generate_arb_signals(markets: list) -> list:
    """
    Scan markets for rolling crypto arbitrage opportunities.
    BTC is the primary target. Last-30-second entries get maximum priority.

    Called EVERY loop from main.py trading_loop() (speed critical).
    Returns list of signal dicts ready for maybe_enter_trade().
    """
    signals = []
    now = datetime.utcnow()

    # Find rolling crypto markets — only LIVE ones (secs_left > 0)
    rolling_markets = []
    for m in markets:
        info = _is_rolling_crypto_market(m)
        if info:
            secs = _estimate_seconds_remaining(m)
            if secs > 0:  # Only live (non-expired) markets
                rolling_markets.append((m, info))

    if not rolling_markets:
        return []

    # Check Binance feed health — at minimum BTC must be live
    btc_price = get_price("BTC")
    if not btc_price or btc_price <= 0:
        # No BTC feed = skip entirely (BTC is our primary edge)
        return []

    binance_status = get_binance_status()

    for market, info in rolling_markets:
        asset = info["asset"]
        timeframe = info["timeframe"]
        current_price = get_price(asset)
        if not current_price or current_price <= 0:
            continue

        market_id = market["id"]

        # Track new markets with reference price
        if market_id not in _arb_reference_prices:
            _arb_reference_prices[market_id] = {
                "ref_price": current_price,
                "window_start": now,
                "asset": asset,
            }
            continue  # Don't signal on first sight -- need to observe movement

        ref_data = _arb_reference_prices[market_id]
        ref_price = ref_data["ref_price"]

        # Skip if already entered
        if market_id in _arb_entered_markets:
            continue

        # Calculate price move since window opened
        move = (current_price / ref_price) - 1

        # Check timing window
        secs_left = _estimate_seconds_remaining(market)
        max_secs = ARB_MAX_SECS_LEFT_15M if timeframe == 15 else ARB_MAX_SECS_LEFT

        if secs_left < ARB_MIN_SECS_LEFT or secs_left > max_secs:
            continue

        # -- Determine if this is a LAST-30-SECOND entry (the sweet spot) --
        is_last30 = secs_left <= 30
        is_btc = asset == "BTC"

        # Use aggressive thresholds for last-30s entries
        move_threshold = LAST30_MOVE_THRESHOLD if is_last30 else ARB_MOVE_THRESHOLD
        max_poly = LAST30_MAX_POLY_PRICE if is_last30 else ARB_MAX_POLY_PRICE
        min_edge = LAST30_MIN_EDGE if is_last30 else ARB_MIN_EDGE

        # Not enough signal
        if abs(move) < move_threshold:
            # Debug log for BTC in last 120s (so we can see what's happening)
            if is_btc and secs_left <= 120:
                tag = "LAST30" if is_last30 else "STD"
                print(
                    f"[ARB-DBG] BTC {timeframe}m [{tag}] move={move:+.5%} "
                    f"(need {move_threshold:.4%}) {secs_left:.0f}s left "
                    f"ref=${ref_price:,.2f} now=${current_price:,.2f}"
                )
            continue

        # Determine direction: price up -> YES, price down -> NO
        direction = "YES" if move > 0 else "NO"
        yes_price = market.get("yes_price", 0.5)

        # Get Polymarket price for OUR side
        poly_price = yes_price if direction == "YES" else (1 - yes_price)

        # Check if there's still an edge (Polymarket hasn't caught up)
        if poly_price > max_poly:
            continue

        # Calculate edge: our estimated true probability minus what we're paying
        # With 25s left and BTC up 0.2%, true probability of "Up" is very high
        estimated_true_prob = min(0.98, 0.50 + abs(move) * 100)  # More aggressive estimate
        if is_last30:
            # Near resolution: if price has moved, it's very unlikely to reverse
            estimated_true_prob = min(0.98, 0.55 + abs(move) * 150)

        edge = estimated_true_prob - poly_price
        if edge < min_edge:
            continue

        # -- Score the signal --
        score = 78

        # Move strength
        if abs(move) > 0.003:
            score += 8   # Strong move (0.3%+)
        if abs(move) > 0.005:
            score += 5   # Very strong (0.5%+)
        if abs(move) > 0.008:
            score += 3   # Massive (0.8%+)

        # Edge size
        if edge > 0.15:
            score += 5   # Big edge
        if edge > 0.25:
            score += 3   # Huge edge

        # THE KEY BONUS: Last-30-second entries
        if is_last30:
            score += 8   # Maximum edge window — user's discovered sweet spot
        elif secs_left <= 60:
            score += 4   # Still good
        elif secs_left <= 90:
            score += 2

        # BTC bonus (most liquid, most reliable)
        if is_btc:
            score += BTC_SCORE_BONUS

        score = min(99, max(75, score))

        # -- Build signal --
        tag = "LAST30" if is_last30 else "STD"
        signal = {
            "market_id": market_id,
            "market_question": market.get("question", ""),
            "score": score,
            "confidence": min(0.97, estimated_true_prob),
            "direction": direction,
            "yes_price": yes_price,
            "market_type": "BINANCE_ARB",
            "can_enter": True,
            "entry_reason": (
                f"ARB[{tag}]: {asset} {move:+.3%} "
                f"(${ref_price:,.2f}->${current_price:,.2f}), "
                f"{direction}@{poly_price:.2f}, edge={edge:.0%}, "
                f"{secs_left:.0f}s left, score={score}"
            ),
            "factors_json": {
                "reference_price": ref_price,
                "current_price": current_price,
                "move_pct": round(move, 6),
                "polymarket_price": round(poly_price, 4),
                "edge_pct": round(edge, 4),
                "estimated_true_prob": round(estimated_true_prob, 4),
                "seconds_remaining": round(secs_left, 0),
                "asset": asset,
                "timeframe_minutes": timeframe,
                "is_last30_entry": is_last30,
                "is_btc": is_btc,
            },
            "created_at": now.isoformat(),
            "clob_token_ids": market.get("clob_token_ids", []),
            "condition_id": market.get("condition_id", ""),
            "liquidity": market.get("liquidity", 0),
        }
        signals.append(signal)

        emoji = "***" if (is_last30 and is_btc) else "**" if is_last30 else "*" if is_btc else ""
        print(
            f"[ARB]{emoji} {tag} {asset} {timeframe}m {move:+.3%} | "
            f"{direction}@{poly_price:.2f} edge={edge:.0%} | "
            f"{secs_left:.0f}s left | score={score} | "
            f"'{market.get('question', '')[:45]}'"
        )

    # Sort: BTC last-30s first, then by score
    signals.sort(key=lambda s: (
        s["factors_json"].get("is_btc", False) and s["factors_json"].get("is_last30_entry", False),
        s["score"],
    ), reverse=True)

    # Clean up expired markets (older than 20 minutes)
    expired = []
    for mid, data in _arb_reference_prices.items():
        age = (now - data["window_start"]).total_seconds()
        if age > 1200:  # 20 minutes (covers 15M markets)
            expired.append(mid)
    for mid in expired:
        del _arb_reference_prices[mid]
        _arb_entered_markets.discard(mid)

    return signals
