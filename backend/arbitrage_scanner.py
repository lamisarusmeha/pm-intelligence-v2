"""
PM Intelligence v4.1 — Arbitrage/Value Bet Scanner (Strategy 5)

NERFED in v4.1:
- YES price range tightened: 0.15-0.35 (was 0.10-0.40)
- NO price range tightened: 0.65-0.85 (was 0.60-0.90)
- Min liquidity raised: $10,000 (was $1,000)
- Days left: <=0.5 day / 12 hours (was <=1 day)
- Added Haiku direction verification before entry
"""

import json
from datetime import datetime
from typing import Optional

# Minimum spread after fees to be worth entering
MIN_SPREAD_PCT = 0.025  # 2.5% minimum spread (0.5% profit after fees)
MIN_LIQUIDITY = 10000    # v4.1 FIX: $10K minimum (was $1K)
MAX_ENTRY_PRICE = 0.98

# Track entered arbitrage markets to prevent double-entry
_arb_entered: set = set()

# v4.1: Haiku verification for direction
try:
    import anthropic
    import os
    _HAS_ANTHROPIC = True
    _API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
except ImportError:
    _HAS_ANTHROPIC = False
    _API_KEY = ""


async def _verify_direction_haiku(question: str, direction: str, price: float) -> bool:
    """Use Haiku to verify if the direction makes sense for this market."""
    if not _HAS_ANTHROPIC or not _API_KEY:
        return True  # Can't verify, allow through

    try:
        client = anthropic.AsyncAnthropic(api_key=_API_KEY)
        model = os.getenv("LLM_SCREEN_MODEL", "claude-haiku-4-5-20251001")

        prompt = f"""Quick check: Should we BUY {direction} on this prediction market?

Question: "{question}"
Current YES price: ${price:.2f}
This market resolves within 12 hours.

Answer ONLY "YES" or "NO". YES means the trade makes sense, NO means it's likely a bad bet."""

        response = await client.messages.create(
            model=model,
            max_tokens=10,
            temperature=0.1,
            messages=[{"role": "user", "content": prompt}],
        )
        answer = response.content[0].text.strip().upper()
        return answer.startswith("YES")

    except Exception:
        return True  # On error, allow through


def scan_arbitrage_opportunities(markets: list) -> list:
    """
    Scan all markets for arbitrage/mispricing opportunities.

    v4.1 NERFED: Tighter price ranges, higher liquidity, shorter resolution window.
    Haiku verification added (called async from main.py wrapper).
    """
    signals = []

    for market in markets:
        try:
            market_id = market.get("id", "")
            question = market.get("question", "")
            yes_price = market.get("yes_price", 0.5)
            liquidity = market.get("liquidity", 0) or 0

            # Skip if already entered or low liquidity
            if market_id in _arb_entered:
                continue
            # v4.1 FIX: $10K minimum (was $1K)
            if liquidity < MIN_LIQUIDITY:
                continue

            # Skip closed or inactive markets
            if market.get("closed", False) or not market.get("active", True):
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
                        signal["_needs_haiku_verify"] = True
                        signals.append(signal)

                # NO range: 0.65-0.85 (was 0.60-0.90)
                elif 0.65 <= yes_price <= 0.85:
                    signal = _build_arb_signal(
                        market, "NO", no_price, liquidity, days_left,
                        f"MISPRICING: NO@{no_price:.2f} on high-liq market resolving <12h"
                    )
                    if signal:
                        signal["_needs_haiku_verify"] = True
                        signals.append(signal)

        except Exception:
            continue

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
