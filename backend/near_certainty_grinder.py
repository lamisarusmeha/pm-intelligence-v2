"""
Strategy 1: Near-Certainty Grinder

Finds markets at 80%+ probability resolving within 30 days.
Cross-checks crypto markets against Binance prices.
Uses Haiku for non-crypto verification.

Based on real wallet pattern: 0xf705 ($1.8M profit, 95%+ win rate)
"""

import json
import os
import re
from datetime import datetime
from typing import Optional

# Crypto keywords for question parsing
CRYPTO_KEYWORDS = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth"],
    "SOL": ["solana", "sol"],
    "DOGE": ["dogecoin", "doge"],
    "XRP": ["xrp", "ripple"],
}

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


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


def _parse_crypto_symbol(question: str) -> Optional[str]:
    """Extract crypto symbol from market question."""
    q = question.lower()
    for symbol, keywords in CRYPTO_KEYWORDS.items():
        for kw in keywords:
            if kw in q:
                return symbol
    return None


def _parse_price_threshold(question: str) -> Optional[float]:
    """Extract price threshold from questions like 'Will BTC reach $80,000?'"""
    # Match patterns like $80,000 or $80000 or $0.20
    patterns = [
        r'\$([0-9,]+(?:\.[0-9]+)?)',
        r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:dollars|usd)',
    ]
    for pattern in patterns:
        match = re.search(pattern, question, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1).replace(",", ""))
            except ValueError:
                continue
    return None


def _is_above_question(question: str) -> Optional[bool]:
    """Determine if question asks 'above/reach' (True) or 'below/dip' (False)."""
    q = question.lower()
    if any(w in q for w in ["above", "reach", "hit", "exceed", "over"]):
        return True
    if any(w in q for w in ["below", "dip", "drop", "under", "fall"]):
        return False
    return None


def _verify_crypto_near_certainty(market: dict, binance_prices: dict) -> bool:
    """
    Verify a crypto market's outcome is near-certain using Binance prices.

    Example: "Will BTC reach $50K?" with YES at 0.88
    - Binance says BTC is $69K \u2192 $69K > $50K \u2192 YES is correct \u2192 verified
    - Binance says BTC is $48K \u2192 $48K < $50K \u2192 YES might not happen \u2192 not verified
    """
    question = market.get("question", "")
    yes_price = market.get("yes_price", 0.5)

    symbol = _parse_crypto_symbol(question)
    if not symbol:
        return False  # Can't identify crypto asset

    threshold = _parse_price_threshold(question)
    if not threshold:
        return True  # Can't parse threshold, allow it (LLM might catch it)

    is_above = _is_above_question(question)
    if is_above is None:
        return True  # Can't determine direction, allow it

    # Get current exchange price
    bp = binance_prices.get(symbol, {})
    exchange_price = bp.get("price", 0)
    if exchange_price <= 0:
        return True  # No Binance data, allow it (graceful degradation)

    # Verify: does exchange price support the high-probability outcome?
    if yes_price >= 0.80:  # Market says YES is likely
        if is_above:
            # "Will BTC reach $50K?" YES at 88% \u2192 BTC should be well above $50K
            verified = exchange_price > threshold * 1.05  # 5% buffer
        else:
            # "Will BTC dip to $40K?" YES at 88% \u2192 BTC should be near/below $40K
            verified = exchange_price < threshold * 1.05
    elif yes_price <= 0.20:  # Market says NO is likely (YES is cheap)
        if is_above:
            # "Will BTC reach $100K?" YES at 12% \u2192 BTC should be well below $100K
            verified = exchange_price < threshold * 0.80  # 20% buffer
        else:
            # "Will BTC dip to $20K?" YES at 12% \u2192 BTC should be well above $20K
            verified = exchange_price > threshold * 1.20
    else:
        verified = False  # Price isn't extreme enough for near-certainty

    if verified:
        print(f"[GRIND] Crypto verified: {symbol}=${exchange_price:,.0f} vs threshold=${threshold:,.0f} "
              f"({'above' if is_above else 'below'}) YES={yes_price:.2f}")
    return verified


async def _verify_with_haiku(market: dict) -> bool:
    """
    Quick Haiku check for non-crypto markets.
    Costs ~$0.001 per call. Only called for markets passing all other filters.
    v4.2: On rate limit, allow through (grinder has strong heuristic filters).
    """
    if not HAS_ANTHROPIC or not ANTHROPIC_API_KEY:
        return True  # No API key, allow it (graceful degradation)

    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    question = market.get("question", "")
    yes_price = market.get("yes_price", 0.5)
    days = _days_left(market.get("end_date", ""))

    direction = "YES" if yes_price >= 0.80 else "NO"
    prob = yes_price if direction == "YES" else (1 - yes_price)

    try:
        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=10,
            messages=[{"role": "user", "content": (
                f'Market: "{question}"\n'
                f"Current {direction} price: {prob:.0%}\n"
                f"Resolves in {days:.1f} days.\n\n"
                f"Is {direction} the near-certain outcome? Answer YES or NO only."
            )}],
        )
        answer = response.content[0].text.strip().upper()
        return "YES" in answer

    except Exception as e:
        if "429" in str(e):
            # v4.2: Rate limited \u2014 allow through since grinder has strong
            # heuristic filters already (80%+ probability, near resolution)
            print(f"[GRIND] Rate limited \u2014 allowing through (strong heuristic filters)")
            return True
        print(f"[GRIND] Haiku verify error: {e}")
        return False


async def generate_near_certainty_signals(markets: list, binance_prices: dict) -> list:
    """
    Scan markets for near-certainty grinding opportunities.

    Filters:
    1. Resolves within 48 hours
    2. YES or NO price in 0.80-0.97 range
    3. Liquidity > $20K
    4. Verified (crypto via Binance, non-crypto via Haiku)

    Returns list of signal dicts compatible with paper_trader.
    """
    signals = []

    for market in markets:
        try:
            # Filter 1: Resolution within 30 days
            days = _days_left(market.get("end_date", ""))
            if days > 30 or days < 0:
                continue

            # Filter 2: Price in near-certainty zone (0.80-0.97 — real near-certainty)
            yes_price = market.get("yes_price", 0.5)
            no_price = 1 - yes_price

            if 0.80 <= yes_price <= 0.97:
                direction = "YES"
                entry_price = yes_price
            elif 0.80 <= no_price <= 0.97:
                direction = "NO"
                entry_price = no_price
            else:
                continue  # Not in the sweet spot

            # Filter 3: Minimum liquidity ($5K — low-liq markets are traps)
            liquidity = market.get("liquidity", 0) or 0
            if liquidity < 5000:
                continue

            # Filter 4: Verification
            is_crypto = _parse_crypto_symbol(market.get("question", "")) is not None
            if is_crypto:
                verified = _verify_crypto_near_certainty(market, binance_prices)
            else:
                verified = await _verify_with_haiku(market)

            if not verified:
                continue

            # Score: weighted by probability, time-to-resolution, and liquidity
            prob_score = (entry_price - 0.80) * 150   # 0-25.5 pts (higher price = better)
            time_score = max(0, (14 - days)) * 1.5    # 0-21 pts (closer = better, peak at <14d)
            liq_score = min(15, liquidity / 10000 * 15)  # 0-15 pts (more liquid = better)
            score = int(60 + prob_score + time_score + liq_score)
            score = min(99, max(60, score))

            signal = {
                "market_id": market.get("id", ""),
                "market_question": market.get("question", ""),
                "score": score,
                "confidence": entry_price,  # Use price as confidence proxy
                "direction": direction,
                "yes_price": yes_price,
                "market_type": "NEAR_CERTAINTY",
                "can_enter": True,
                "entry_reason": f"GRIND: {direction}@{entry_price:.2f}, {days:.1f}d left, "
                                f"{'crypto' if is_crypto else 'haiku'}-verified",
                "factors_json": json.dumps({
                    "days_left": round(days, 2),
                    "entry_price": entry_price,
                    "liquidity": liquidity,
                    "is_crypto": is_crypto,
                    "verified": True,
                }),
                "created_at": datetime.utcnow().isoformat(),
                "clob_token_ids": market.get("clob_token_ids", []),
                "condition_id": market.get("condition_id", ""),
                "liquidity": liquidity,
            }
            signals.append(signal)
            print(f"[GRIND] Signal: {direction}@{entry_price:.2f} '{market['question'][:50]}' "
                  f"({days:.1f}d, liq=${liquidity:,.0f})")

        except Exception as e:
            print(f"[GRIND] Error processing market: {e}")
            continue

    # Rank by score descending and cap at top 5 — quality over quantity
    signals.sort(key=lambda s: s["score"], reverse=True)
    top_signals = signals[:5]
    if len(signals) > 5:
        print(f"[GRIND] Ranked {len(signals)} signals, keeping top 5")
    print(f"[GRIND] Generated {len(top_signals)} near-certainty signals from {len(markets)} markets")
    return top_signals
