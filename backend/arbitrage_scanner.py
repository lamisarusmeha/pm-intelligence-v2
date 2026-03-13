"""
PM Intelligence v4.2 — Arbitrage/Value Bet Scanner (Strategy 5)

NERFED in v4.1:
- YES price range tightened: 0.15-0.35 (was 0.10-0.40)
- NO price range tightened: 0.65-0.85 (was 0.60-0.90)
- Min liquidity raised: $10,000 (was $1,000)
- Days left: <=0.5 day / 12 hours (was <=1 day)
- Added Haiku direction verification before entry

v4.2 FIXES:
- Added SPORTS_BLACKLIST — no college basketball, football, soccer, etc.
- Added CRYPTO_BOOST — prefer crypto markets where we have data edge (Binance feed)
- Require Haiku verify to PASS (was failing silently and allowing through)
- Reduced max signals per scan to 3 (was unlimited)
"""

import json
import os
from datetime import datetime
from typing import Optional

# Minimum spread after fees to be worth entering
MIN_SPREAD_PCT = 0.025  # 2.5% minimum spread (0.5% profit after fees)
MIN_LIQUIDITY = 10000    # v4.1 FIX: $10K minimum (was $1K)
MAX_ENTRY_PRICE = 0.98
MAX_SIGNALS_PER_SCAN = 3  # v4.2: Don't flood with arb signals

# Track entered arbitrage markets to prevent double-entry
_arb_entered: set = set()

# v4.2: Track Haiku-rejected markets to avoid re-verifying every loop
_haiku_rejected: set = set()

# v4.2: Sports/entertainment blacklist — zero informational edge on these
SPORTS_BLACKLIST = (
    # College sports
    "aggies", "titans", "spartans", "lobos", "bulldogs", "wildcats",
    "bears", "tigers", "eagles", "hawks", "mustangs", "cougars",
    "huskies", "panthers", "cardinals", "longhorns", "wolverines",
    "buckeyes", "crimson", "jayhawks", "hoosiers", "badgers",
    # Sports terms
    "vs.", "vs ", "match", "game score", "championship", "tournament",
    "ncaa", "nba", "nfl", "mlb", "nhl", "mls", "ufc", "wwe",
    "premier league", "la liga", "serie a", "bundesliga", "ligue 1",
    "super bowl", "world cup", "playoff", "semifinals", "quarterfinal",
    "march madness", "bowl game", "all-star",
    # Entertainment
    "oscar", "grammy", "emmy", "golden globe", "academy award",
    "bachelor", "bachelorette", "survivor", "idol",
    "box office", "opening weekend",
)

# v4.2: Crypto keywords — markets where we have Binance data edge
CRYPTO_KEYWORDS = (
    "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
    "crypto", "xrp", "doge", "up or down", "price of",
    "market cap", "above $", "below $", "dip to",
)

# v4.1: Haiku verification for direction
try:
    import anthropic
    _HAS_ANTHROPIC = True
    _API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
except ImportError:
    _HAS_ANTHROPIC = False
    _API_KEY = ""


def _is_sports_market(question: str) -> bool:
    """Check if market is sports/entertainment — we have zero edge on these."""
    q = question.lower()
    return any(word in q for word in SPORTS_BLACKLIST)


def _is_crypto_market(question: str) -> bool:
    """Check if market is crypto — we have Binance data edge."""
    q = question.lower()
    return any(word in q for word in CRYPTO_KEYWORDS)


async def _verify_direction_haiku(question: str, direction: str, price: float) -> bool:
    """Use Haiku to verify if the direction makes sense for this market.
    v4.2: On error/rate limit, reject (conservative).
    """
    if not _HAS_ANTHROPIC or not _API_KEY:
        return False  # v4.2 FIX: Can't verify = DON'T enter (was True)

    client = anthropic.AsyncAnthropic(api_key=_API_KEY)
    model = os.getenv("LLM_SCREEN_MODEL", "claude-haiku-4-5-20251001")

    prompt = f"""Quick check: Should we BUY {direction} on this prediction market?

Question: "{question}"
Current YES price: ${price:.2f}
This market resolves within 12 hours.

Consider: Do we have any informational edge here? Is the price likely mispriced?
If this is a sports game, political event, or anything unpredictable, answer NO.
Only answer YES if there's a clear reason the market is mispriced.

Answer ONLY "YES" or "NO". YES means the trade makes sense, NO means it's likely a bad bet."""

    try:
        response = await client.messages.create(
            model=model,
            max_tokens=10,
            temperature=0.1,
            messages=[{"role": "user", "content": prompt}],
        )
        answer = response.content[0].text.strip().upper()
        return answer.startswith("YES")

    except Exception as e:
        print(f"[ARB] Haiku verify error: {e}")
        return False  # On any error (including rate limit), DON'T enter


def scan_arbitrage_opportunities(markets: list) -> list:
    """
    Scan all markets for arbitrage/mispricing opportunities.

    v4.2: Added sports blacklist, crypto preference, signal cap.
    """
    signals = []

    for market in markets:
        try:
            market_id = market.get("id", "")
            question = market.get("question", "")
            yes_price = market.get("yes_price", 0.5)
            liquidity = market.get("liquidity", 0) or 0

            # Skip if already entered, rejected, or low liquidity
            if market_id in _arb_entered:
                continue
            if market_id in _haiku_rejected:
                continue
            if liquidity < MIN_LIQUIDITY:
                continue

            # Skip closed or inactive markets
            if market.get("closed", False) or not market.get("active", True):
                continue

            # v4.2 FIX: Skip sports/entertainment markets — zero edge
            if _is_sports_market(question):
                continue

            no_price = 1 - yes_price

            end_date = market.get("end_date", "")
            days_left = _days_left(end_date)

            # v4.1 FIX: <=0.5 day (12 hours) instead of <=1 day
            if days_left <= 0.5 and liquidity >= MIN_LIQUIDITY:
                # v4.1 FIX: Tightened price ranges
                # YES range: 0.15-0.35 (was 0.10-0.40)
                if 0.15 <= yes_price <= 0.35:
                    signal = _build_arb_signal(
                        market, "YES", yes_price, liquidity, days_left,
                        f"MISPRICING: YES@{yes_price:.2f} on high-liq market resolving <12h"
                    )
                    if signal:
                        # v4.2: Boost score for crypto markets (data edge)
                        if _is_crypto_market(question):
                            signal["score"] = min(95, signal["score"] + 10)
                        signal["_needs_haiku_verify"] = True
                        signals.append(signal)

                # NO range: 0.65-0.85 (was 0.60-0.90)
                elif 0.65 <= yes_price <= 0.85:
                    signal = _build_arb_signal(
                        market, "NO", no_price, liquidity, days_left,
                        f"MISPRICING: NO@{no_price:.2f} on high-liq market resolving <12h"
                    )
                    if signal:
                        if _is_crypto_market(question):
                            signal["score"] = min(95, signal["score"] + 10)
                        signal["_needs_haiku_verify"] = True
                        signals.append(signal)

        except Exception:
            continue

    # v4.2: Cap signals per scan — prioritize by score
    if len(signals) > MAX_SIGNALS_PER_SCAN:
        signals.sort(key=lambda s: -s["score"])
        signals = signals[:MAX_SIGNALS_PER_SCAN]

    if signals:
        print(f"[ARB-SCAN] Found {len(signals)} potential value bet signals")
    return signals


def _build_arb_signal(market: dict, direction: str, entry_price: float,
                       liquidity: float, days_left: float, reason: str) -> Optional[dict]:
    """Build an arbitrage/mispricing signal."""
    if entry_price < 0.05 or entry_price > 0.95:
        return None

    score = 75
    if liquidity > 20000:
        score += 5
    if liquidity > 50000:
        score += 5
    if days_left < 0.25:
        score += 5  # Resolving within 6 hours
    if days_left < 0.1:
        score += 5  # Resolving within ~2 hours

    score = min(95, score)

    return {
        "market_id": market.get("id", ""),
        "market_question": market.get("question", ""),
        "score": score,
        "confidence": entry_price,
        "direction": direction,
        "yes_price": market.get("yes_price", 0.5),
        "market_type": "ARBITRAGE",
        "can_enter": True,
        "entry_reason": reason,
        "factors_json": json.dumps({
            "strategy": "arbitrage_scanner",
            "entry_price": entry_price,
            "liquidity": liquidity,
            "days_left": round(days_left, 2),
        }),
        "created_at": datetime.utcnow().isoformat(),
        "clob_token_ids": market.get("clob_token_ids", []),
        "condition_id": market.get("condition_id", ""),
        "liquidity": liquidity,
    }


def _days_left(end_date_str: str) -> float:
    """Calculate days until market resolution."""
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
