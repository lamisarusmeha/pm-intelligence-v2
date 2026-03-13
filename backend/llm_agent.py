"""
PM Intelligence v4.1 — Dual-Model LLM Brain

Two-stage analysis pipeline:
  Stage 1: Claude Haiku screens all candidates (cheap)
  Stage 2: Claude Sonnet deep-dives on high-edge opportunities (accurate)

v4.1 FIXES:
- DEEP_ANALYSIS_EDGE lowered from 0.12 to 0.08 (Sonnet actually fires now)
- System prompt aligned: >5% edge (was >10% — LLM was self-censoring)
- Analysis prompt aligned: >5% edge + 0.4 confidence (was >10% + 0.6)
- Lesson extraction win filter: $5 (was $50 — now extracts from most wins)
"""

import os
import json
import asyncio
from datetime import datetime
from typing import Optional

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
SCREENING_MODEL = os.getenv("LLM_SCREEN_MODEL", "claude-haiku-4-5-20251001")
DEEP_MODEL = os.getenv("LLM_DEEP_MODEL", "claude-sonnet-4-20250514")

# v4.1 FIX: Lowered from 0.12 to 0.08
# Haiku reports >8% edge → escalate to Sonnet for confirmation
DEEP_ANALYSIS_EDGE = 0.08

_cost_tracking = {
    "haiku_calls": 0, "haiku_input": 0, "haiku_output": 0,
    "sonnet_calls": 0, "sonnet_input": 0, "sonnet_output": 0,
}


def get_cost_summary() -> dict:
    haiku_cost = (
        (_cost_tracking["haiku_input"] / 1_000_000) * 0.25 +
        (_cost_tracking["haiku_output"] / 1_000_000) * 1.25
    )
    sonnet_cost = (
        (_cost_tracking["sonnet_input"] / 1_000_000) * 3.0 +
        (_cost_tracking["sonnet_output"] / 1_000_000) * 15.0
    )
    return {
        "haiku_calls": _cost_tracking["haiku_calls"],
        "sonnet_calls": _cost_tracking["sonnet_calls"],
        "total_calls": _cost_tracking["haiku_calls"] + _cost_tracking["sonnet_calls"],
        "haiku_cost_usd": round(haiku_cost, 4),
        "sonnet_cost_usd": round(sonnet_cost, 4),
        "total_cost_usd": round(haiku_cost + sonnet_cost, 4),
        "savings_vs_all_sonnet": round(
            max(0, ((_cost_tracking["haiku_calls"] * 0.08) - haiku_cost)), 4
        ),
    }


async def analyze_market(
    market: dict,
    news_context: str,
    volume_profile: dict,
    memory_lessons: list,
    portfolio_state: dict,
) -> Optional[dict]:
    if not HAS_ANTHROPIC or not ANTHROPIC_API_KEY:
        return _fallback_analysis(market, volume_profile)

    haiku_result = await _call_llm(
        market, news_context, volume_profile, memory_lessons,
        portfolio_state, model=SCREENING_MODEL, max_tokens=500
    )

    if not haiku_result or haiku_result["action"] == "SKIP":
        return haiku_result

    # v4.1 FIX: 0.08 threshold (was 0.12) — Sonnet actually fires now
    if abs(haiku_result.get("edge", 0)) >= DEEP_ANALYSIS_EDGE:
        print(f"[LLM] Edge={haiku_result['edge']:.1%} — escalating to Sonnet")
        sonnet_result = await _call_llm(
            market, news_context, volume_profile, memory_lessons,
            portfolio_state, model=DEEP_MODEL, max_tokens=800
        )
        if sonnet_result:
            return sonnet_result

    return haiku_result


async def _call_llm(
    market: dict,
    news_context: str,
    volume_profile: dict,
    memory_lessons: list,
    portfolio_state: dict,
    model: str,
    max_tokens: int = 500,
) -> Optional[dict]:
    prompt = _build_analysis_prompt(
        market, news_context, volume_profile, memory_lessons, portfolio_state
    )

    is_haiku = "haiku" in model.lower()
    cost_key = "haiku" if is_haiku else "sonnet"

    try:
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        response = await client.messages.create(
            model=model,
            max_tokens=max_tokens,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
            # v4.1 FIX: System prompt aligned to >5% edge (was >10%)
            system=(
                "You are a prediction market analyst. Analyze Polymarket markets "
                "and estimate true probabilities. Respond ONLY with valid JSON. "
                "Be calibrated and honest about uncertainty. Flag potential trades "
                "if you see >5% edge — the code will filter further. "
                "Sanity checks: never bet NO on already-true conditions; "
                "avoid extreme prices (<$0.05 or >$0.95). "
                "For crypto price markets, always verify the current price "
                "satisfies the condition before recommending."
            ),
        )

        _cost_tracking[f"{cost_key}_calls"] += 1
        _cost_tracking[f"{cost_key}_input"] += response.usage.input_tokens
        _cost_tracking[f"{cost_key}_output"] += response.usage.output_tokens

        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()

        decision = json.loads(raw)

        action = decision.get("action", "SKIP")
        if action not in ("BUY_YES", "BUY_NO", "SKIP"):
            action = "SKIP"

        confidence = min(1.0, max(0.0, float(decision.get("confidence", 0))))
        est_prob = min(1.0, max(0.0, float(decision.get("estimated_probability", 0.5))))

        market_price = market.get("yes_price", 0.5)
        if action == "BUY_YES":
            edge = est_prob - market_price
        elif action == "BUY_NO":
            edge = (1 - est_prob) - (1 - market_price)
        else:
            edge = 0.0

        return {
            "action": action,
            "confidence": confidence,
            "reasoning": decision.get("reasoning", "No reasoning provided"),
            "estimated_probability": est_prob,
            "edge": round(edge, 4),
            "risk_factors": decision.get("risk_factors", []),
            "key_evidence": decision.get("key_evidence", []),
            "model": model,
            "tokens_used": response.usage.input_tokens + response.usage.output_tokens,
        }

    except json.JSONDecodeError as e:
        print(f"[LLM] JSON parse error ({model}): {e}")
        return None
    except Exception as e:
        print(f"[LLM] API error ({model}): {e}")
        return None


def _build_analysis_prompt(
    market: dict,
    news_context: str,
    volume_profile: dict,
    memory_lessons: list,
    portfolio_state: dict,
) -> str:

    vol_alerts = volume_profile.get("recent_alerts", [])
    vol_text = "No unusual volume activity."
    if vol_alerts:
        vol_lines = []
        for a in vol_alerts[:3]:
            vol_lines.append(f"- {a.get('alert_type', 'UNKNOWN')}: {a.get('description', '')}")
        vol_text = "\n".join(vol_lines)

    has_spike = volume_profile.get("has_recent_spike", False)
    spike_note = ""
    if has_spike:
        spike_note = (
            "\n**IMPORTANT: Recent volume spike detected. Could signal informed "
            "trading or insider activity. Weight heavily in analysis.**"
        )

    lessons_text = "No previous lessons for similar markets."
    if memory_lessons:
        lesson_lines = [f"- {l}" for l in memory_lessons[:5]]
        lessons_text = "\n".join(lesson_lines)

    cash = portfolio_state.get("cash_balance", 10000)
    open_positions = portfolio_state.get("invested", 0)
    win_rate = portfolio_state.get("win_rate", 0)

    # v4.1 FIX: Rule 4 aligned to >5% edge + 0.4 confidence (was >10% + 0.6)
    prompt = f"""Analyze this Polymarket prediction market and decide whether to trade.

## Market
- **Question:** {market.get('question', 'Unknown')}
- **Category:** {market.get('category', 'Unknown')}
- **Current YES price:** ${market.get('yes_price', 0.5):.4f}
- **Current NO price:** ${market.get('no_price', 0.5):.4f}
- **24h Volume:** ${market.get('volume24hr', 0):,.0f}
- **Total Volume:** ${market.get('volume', 0):,.0f}
- **Liquidity:** ${market.get('liquidity', 0):,.0f}
- **End Date:** {market.get('end_date', 'Unknown')}

## Recent News Context
{news_context if news_context else "No relevant news found."}

## Volume Activity
{vol_text}
{spike_note}

## Lessons from Past Trades
{lessons_text}

## Portfolio
- Cash: ${cash:,.0f}
- Open positions value: ${open_positions:,.0f}
- Current win rate: {win_rate:.0f}%

## Your Task
1. Estimate TRUE probability of YES outcome
2. Compare to current market price to find edge
3. Decide: BUY_YES, BUY_NO, or SKIP
4. Flag trades with >5% edge AND confidence >= 0.4 (code filters further)
5. NEVER bet NO on a condition that is ALREADY TRUE (e.g. BTC above $64k when BTC is $69k)
6. NEVER recommend trades at extreme prices (>$0.95 or <$0.05)
7. For crypto/price markets: verify current price satisfies condition before recommending

Respond with ONLY this JSON:
{{
    "action": "BUY_YES" or "BUY_NO" or "SKIP",
    "confidence": 0.0 to 1.0,
    "estimated_probability": 0.0 to 1.0,
    "reasoning": "2-3 sentence explanation",
    "risk_factors": ["list", "of", "risks"],
    "key_evidence": ["list", "of", "evidence"]
}}"""

    return prompt


def _fallback_analysis(market: dict, volume_profile: dict) -> Optional[dict]:
    has_spike = volume_profile.get("has_recent_spike", False)
    price = market.get("yes_price", 0.5)

    if not has_spike:
        return {
            "action": "SKIP",
            "confidence": 0.0,
            "reasoning": "No LLM available and no volume spike. Skipping.",
            "estimated_probability": price,
            "edge": 0.0,
            "risk_factors": ["No LLM analysis available"],
            "key_evidence": [],
            "model": "fallback_heuristic",
            "tokens_used": 0,
        }

    alerts = volume_profile.get("recent_alerts", [])
    spike_alert = alerts[0] if alerts else {}
    price_at_spike = spike_alert.get("price_at_alert", price)

    if price > price_at_spike:
        action = "BUY_YES"
        est_prob = min(0.95, price + 0.10)
    else:
        action = "BUY_NO"
        est_prob = max(0.05, price - 0.10)

    return {
        "action": action,
        "confidence": 0.5,
        "reasoning": "Volume spike detected (fallback). Following smart money.",
        "estimated_probability": est_prob,
        "edge": 0.10,
        "risk_factors": ["Fallback analysis — no LLM reasoning"],
        "key_evidence": [spike_alert.get("description", "Volume spike")],
        "model": "fallback_heuristic",
        "tokens_used": 0,
    }


async def evaluate_trade_outcome(
    trade: dict,
    original_reasoning: str,
    outcome: str,
    pnl: float,
) -> Optional[str]:
    """Extract a lesson from a closed trade using Haiku.

    v4.1 FIX: Win filter lowered from $50 to $5.
    With $100-200 positions, most wins are $5-15. Old filter skipped them all.
    """
    if not HAS_ANTHROPIC or not ANTHROPIC_API_KEY:
        if outcome == "LOSS":
            return f"Lost ${abs(pnl):.2f} on {trade.get('market_question', 'unknown')}."
        return None

    # v4.1 FIX: Only skip trivial wins under $5 (was $50)
    if outcome == "WIN" and abs(pnl) < 5:
        return f"Won ${pnl:.2f}. Strategy worked as expected."

    prompt = f"""A trade closed. Extract one lesson.

## Trade Details
- Market: {trade.get('market_question', 'Unknown')}
- Direction: {trade.get('direction', 'Unknown')}
- Market Type: {trade.get('market_type', 'Unknown')}
- Entry: {trade.get('entry_price', 0)}
- Exit: {trade.get('exit_price', 0)}
- P&L: ${pnl:.2f}
- Outcome: {outcome}

## Original Reasoning
{original_reasoning}

## Task
Write ONE concise lesson (1-2 sentences) for future trades. Focus on what
reasoning got wrong (loss) or right (win). Be specific and actionable.
Return ONLY the lesson text, no JSON."""

    try:
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        response = await client.messages.create(
            model=SCREENING_MODEL,
            max_tokens=150,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}],
        )
        _cost_tracking["haiku_calls"] += 1
        _cost_tracking["haiku_input"] += response.usage.input_tokens
        _cost_tracking["haiku_output"] += response.usage.output_tokens

        return response.content[0].text.strip()

    except Exception as e:
        print(f"[LLM] Lesson extraction error: {e}")
        return f"{'Won' if outcome == 'WIN' else 'Lost'} ${abs(pnl):.2f}."
