"""
Research Agent v3 — Multi-Source Intelligence Pipeline

Sources (priority order):
1. Tavily Search API (AI-optimized, returns page content — 1,000 free/month)
2. DuckDuckGo Search + BeautifulSoup scraping (unlimited fallback)
3. NewsAPI headlines (80k+ sources — 100 free/day)
4. Free crypto news aggregator (no API key)
5. Live crypto prices from CoinGecko (free, no key)
6. Sports odds from The Odds API (free tier 500 req/month)
7. Polymarket cross-market correlation
8. Market metadata (description, resolution criteria)

v3 Upgrades:
- Tavily as primary search (returns clean content, no scraping needed)
- NewsAPI for breaking headlines by category
- Free crypto news feed (no key required)
- Research quality gate: returns quality score with context
- DDG + scraping as fallback when Tavily quota exhausted
"""

import httpx
import asyncio
import os
import json
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple

try:
    from ddgs import DDGS
    HAS_DDGS = True
except ImportError:
    try:
        from duckduckgo_search import DDGS
        HAS_DDGS = True
    except ImportError:
        HAS_DDGS = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# API Keys
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

# Cache to avoid redundant API calls
_search_cache = {}
_price_cache = {}
_scrape_cache = {}
_news_cache = {}
_tavily_cache = {}
_cache_ttl = timedelta(minutes=20)
_price_cache_ttl = timedelta(minutes=5)
_news_cache_ttl = timedelta(minutes=15)

# Tavily usage tracking (reset daily)
_tavily_usage = {"date": "", "count": 0}
TAVILY_DAILY_LIMIT = 30  # Conservative: 1000/month ÷ 30 days ≈ 33/day

# Research quality tracking
_research_stats = {
    "total_researched": 0,
    "tavily_used": 0,
    "ddg_used": 0,
    "newsapi_used": 0,
    "crypto_news_used": 0,
    "avg_context_chars": 0,
    "quality_gates_failed": 0,
}


def get_research_stats() -> dict:
    return {**_research_stats, "tavily_today": _tavily_usage.get("count", 0)}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def gather_market_context(market: dict) -> str:
    """
    Gather all available context for a market.
    Returns a formatted string for the LLM prompt.

    v3: Tavily + NewsAPI + crypto news + quality gate.
    """
    question = market.get("question", "")
    category = market.get("category", "")

    # Run ALL data sources concurrently
    tasks = [
        _get_market_metadata(market),
        _search_primary(question, category),       # Tavily or DDG
        _get_news_headlines(question, category),    # NewsAPI
        _get_crypto_news(question, category),       # Free crypto news
        _get_crypto_context(question, category),    # CoinGecko prices
        _get_sports_context(question, category),    # The Odds API
        _find_related_markets(market),              # Polymarket cross-ref
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    context_parts = []
    for r in results:
        if isinstance(r, str) and r:
            context_parts.append(r)

    context = "\n\n".join(context_parts) if context_parts else ""

    # Track stats
    _research_stats["total_researched"] += 1
    total = _research_stats["total_researched"]
    prev_avg = _research_stats["avg_context_chars"]
    _research_stats["avg_context_chars"] = int(
        (prev_avg * (total - 1) + len(context)) / total
    )

    return context


def get_research_quality(context: str) -> dict:
    """
    Evaluate research quality. Used by LLM cycle to decide if context
    is rich enough to send to the LLM (quality gate).
    """
    if not context:
        return {"score": 0, "sufficient": False, "reason": "No context gathered"}

    char_count = len(context)
    has_live_data = any(kw in context for kw in [
        "LIVE CRYPTO DATA", "LIVE CRYPTO NEWS", "LIVE",
        "ODDS:", "Current price:", "Breaking:"
    ])
    has_search = "Search results:" in context or "Web research:" in context
    has_news = "NEWS HEADLINES" in context or "CRYPTO NEWS" in context
    has_scraped = "Content from [" in context or "Page content:" in context

    # Score: 0-100
    score = 0
    if char_count > 200: score += 15
    if char_count > 500: score += 15
    if char_count > 1000: score += 10
    if char_count > 2000: score += 10
    if has_live_data: score += 20
    if has_search: score += 15
    if has_news: score += 10
    if has_scraped: score += 5

    sufficient = score >= 25  # Minimum: at least search results OR live data

    reason = []
    if not has_search and not has_scraped:
        reason.append("no web search results")
    if char_count < 200:
        reason.append(f"thin context ({char_count} chars)")

    return {
        "score": score,
        "sufficient": sufficient,
        "reason": ", ".join(reason) if reason else "adequate",
        "char_count": char_count,
        "has_live_data": has_live_data,
        "has_search": has_search,
        "has_news": has_news,
    }


# ---------------------------------------------------------------------------
# Tavily Search (PRIMARY — AI-optimized, returns page content)
# ---------------------------------------------------------------------------

async def _tavily_search(question: str, category: str) -> str:
    """
    Tavily Search API — designed for AI agents.
    Returns actual page content (no scraping needed).
    Free tier: 1,000 searches/month.
    """
    if not TAVILY_API_KEY:
        return ""

    # Daily rate limit
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if _tavily_usage["date"] != today:
        _tavily_usage["date"] = today
        _tavily_usage["count"] = 0
    if _tavily_usage["count"] >= TAVILY_DAILY_LIMIT:
        return ""  # Quota exhausted, fall through to DDG

    cache_key = f"tavily_{question[:60]}"
    if cache_key in _tavily_cache:
        cached_time, cached_result = _tavily_cache[cache_key]
        if datetime.utcnow() - cached_time < _cache_ttl:
            return cached_result

    search_query = _build_search_query(question, category)
    if not search_query:
        return ""

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": TAVILY_API_KEY,
                    "query": search_query,
                    "search_depth": "basic",  # "basic" is free, "advanced" costs more
                    "include_answer": True,
                    "include_raw_content": False,
                    "max_results": 5,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                _tavily_usage["count"] += 1
                _research_stats["tavily_used"] += 1

                parts = []

                # Tavily's AI-generated answer (most valuable)
                answer = data.get("answer", "")
                if answer:
                    parts.append(f"**AI Research Summary:**\n{answer[:800]}")

                # Individual results with content
                results = data.get("results", [])
                for r in results[:4]:
                    title = r.get("title", "")
                    content = r.get("content", "")
                    if content:
                        parts.append(f"[{title}]: {content[:400]}")

                result = "\n\n".join(parts) if parts else ""
                if result:
                    result = f"Web research:\n{result}"
                _tavily_cache[cache_key] = (datetime.utcnow(), result)
                return result

    except Exception as e:
        print(f"[Research] Tavily error: {e}")
    return ""


# ---------------------------------------------------------------------------
# Primary search: Tavily with DDG fallback
# ---------------------------------------------------------------------------

async def _search_primary(question: str, category: str) -> str:
    """Try Tavily first, fall back to DDG + scraping."""
    # Try Tavily (returns content, no scraping needed)
    tavily_result = await _tavily_search(question, category)
    if tavily_result:
        return tavily_result

    # Fallback: DDG search + page scraping
    return await _search_and_scrape(question, category)


# ---------------------------------------------------------------------------
# NewsAPI Integration (breaking headlines — 100 free/day)
# ---------------------------------------------------------------------------

async def _get_news_headlines(question: str, category: str) -> str:
    """
    Fetch breaking news headlines from NewsAPI.
    Free tier: 100 requests/day, headlines only.
    """
    if not NEWS_API_KEY:
        return ""

    # Map category to NewsAPI category
    cat_lower = (category or "").lower()
    q_lower = question.lower()

    # Build search query — extract key terms
    search_q = _build_news_query(question, category)
    if not search_q:
        return ""

    cache_key = f"news_{search_q[:40]}"
    if cache_key in _news_cache:
        cached_time, cached_result = _news_cache[cache_key]
        if datetime.utcnow() - cached_time < _news_cache_ttl:
            return cached_result

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            # Use /everything endpoint for broader coverage
            resp = await client.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": search_q,
                    "sortBy": "publishedAt",
                    "pageSize": 5,
                    "language": "en",
                    "apiKey": NEWS_API_KEY,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                articles = data.get("articles", [])
                _research_stats["newsapi_used"] += 1

                if not articles:
                    return ""

                parts = ["**NEWS HEADLINES:**"]
                for a in articles[:5]:
                    title = a.get("title", "")
                    desc = a.get("description", "")
                    source = a.get("source", {}).get("name", "")
                    published = a.get("publishedAt", "")[:10]
                    if title and title != "[Removed]":
                        line = f"- [{source} {published}] {title}"
                        if desc and desc != "[Removed]":
                            line += f" — {desc[:150]}"
                        parts.append(line)

                result = "\n".join(parts) if len(parts) > 1 else ""
                _news_cache[cache_key] = (datetime.utcnow(), result)
                return result

    except Exception as e:
        print(f"[Research] NewsAPI error: {e}")
    return ""


def _build_news_query(question: str, category: str) -> str:
    """Build a concise news search query."""
    # Extract the most important terms (names, numbers, key nouns)
    removals = {
        "will", "by", "before", "after", "in", "the", "be", "to", "of",
        "a", "an", "on", "at", "or", "up", "down", "above", "below",
        "over", "under", "this", "that", "market", "resolves", "resolution",
        "yes", "no", "reach", "hit", "go", "get", "make",
    }
    words = question.split()
    filtered = [w.strip("?!.,\"'") for w in words
                if w.lower() not in removals and len(w) > 2]
    return " ".join(filtered[:5])


# ---------------------------------------------------------------------------
# Free Crypto News (no API key needed)
# ---------------------------------------------------------------------------

async def _get_crypto_news(question: str, category: str) -> str:
    """
    Fetch crypto news from free sources (no API key needed).
    Uses CoinGecko's free news endpoint and other free feeds.
    """
    q_lower = question.lower()
    cat_lower = (category or "").lower()

    # Only for crypto markets
    is_crypto = any(kw in q_lower or kw in cat_lower for kw in [
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "crypto",
        "xrp", "doge", "cardano", "bnb", "avalanche", "polkadot",
    ])
    if not is_crypto:
        return ""

    cache_key = f"crypto_news_{q_lower[:30]}"
    if cache_key in _news_cache:
        cached_time, cached_result = _news_cache[cache_key]
        if datetime.utcnow() - cached_time < _news_cache_ttl:
            return cached_result

    # Determine which coin
    coin_term = "bitcoin"  # default
    for kw in ["ethereum", "eth", "solana", "sol", "xrp", "doge", "cardano",
               "bnb", "avalanche", "polkadot"]:
        if kw in q_lower:
            coin_term = kw
            break

    results = []

    # Source 1: CryptoCompare news (free, no key needed for basic)
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                "https://min-api.cryptocompare.com/data/v2/news/",
                params={"lang": "EN", "sortOrder": "latest"},
            )
            if resp.status_code == 200:
                data = resp.json()
                articles = data.get("Data", [])
                for a in articles[:5]:
                    title = a.get("title", "")
                    body = a.get("body", "")[:200]
                    source = a.get("source", "")
                    # Filter: only include if relevant to our market
                    if any(kw in title.lower() or kw in body.lower()
                           for kw in [coin_term, "crypto", "market"]):
                        results.append(f"- [{source}] {title}")
                        if body:
                            results.append(f"  {body}")
    except Exception:
        pass

    # Source 2: CoinGecko trending (free)
    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            resp = await client.get(
                "https://api.coingecko.com/api/v3/search/trending",
            )
            if resp.status_code == 200:
                data = resp.json()
                coins = data.get("coins", [])[:3]
                if coins:
                    trending = [c.get("item", {}).get("name", "") for c in coins]
                    results.append(f"- Trending coins: {', '.join(trending)}")
    except Exception:
        pass

    if results:
        _research_stats["crypto_news_used"] += 1
        result = "**LIVE CRYPTO NEWS:**\n" + "\n".join(results[:8])
        _news_cache[cache_key] = (datetime.utcnow(), result)
        return result

    return ""


# ---------------------------------------------------------------------------
# DDG Search + Page Scraping (fallback when Tavily unavailable)
# ---------------------------------------------------------------------------

async def _search_and_scrape(question: str, category: str) -> str:
    """
    Fallback: DuckDuckGo search + page scraping.
    1. Search DuckDuckGo for the market question
    2. Scrape top 2 result pages for actual content
    3. Return relevant excerpts
    """
    cache_key = f"search_{question[:60]}"
    if cache_key in _search_cache:
        cached_time, cached_result = _search_cache[cache_key]
        if datetime.utcnow() - cached_time < _cache_ttl:
            return cached_result

    search_results = await _ddg_search(question, category)
    if not search_results:
        return ""

    _research_stats["ddg_used"] += 1
    scraped_parts = []

    # Include search snippets
    snippet_text = "\n".join(
        f"- [{r['title']}]: {r['snippet']}"
        for r in search_results[:5] if r.get('snippet')
    )
    if snippet_text:
        scraped_parts.append(f"Search results:\n{snippet_text}")

    # Scrape top 2 pages for deeper context
    scrape_tasks = []
    for r in search_results[:2]:
        url = r.get("url", "")
        if url and _is_scrapeable(url):
            scrape_tasks.append(_scrape_page(url))

    if scrape_tasks:
        scrape_results = await asyncio.gather(*scrape_tasks, return_exceptions=True)
        for i, content in enumerate(scrape_results):
            if isinstance(content, str) and content:
                source = search_results[i].get("title", "Unknown")
                trimmed = content[:1500]
                scraped_parts.append(f"Content from [{source}]:\n{trimmed}")

    result = "\n\n".join(scraped_parts) if scraped_parts else ""
    _search_cache[cache_key] = (datetime.utcnow(), result)
    return result


async def _ddg_search(question: str, category: str) -> list:
    """Search DuckDuckGo using the ddgs library (real results)."""
    if not HAS_DDGS:
        return await _ddg_html_fallback(question, category)

    search_terms = _build_search_query(question, category)
    if not search_terms:
        return []

    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None, lambda: _ddg_search_sync(search_terms)
        )
        return results
    except Exception as e:
        print(f"[Research] DDG search error: {e}")
        return await _ddg_html_fallback(question, category)


def _ddg_search_sync(query: str) -> list:
    """Synchronous DuckDuckGo search. Combines web + news results."""
    results = []
    try:
        with DDGS() as ddgs:
            web = list(ddgs.text(query, max_results=5))
            for r in web:
                results.append({
                    "title": r.get("title", ""),
                    "url": r.get("href", r.get("link", "")),
                    "snippet": r.get("body", r.get("snippet", "")),
                })
            try:
                news = list(ddgs.news(query, max_results=3))
                for r in news:
                    results.append({
                        "title": r.get("title", ""),
                        "url": r.get("url", r.get("link", "")),
                        "snippet": r.get("body", r.get("excerpt", "")),
                    })
            except Exception:
                pass
    except Exception as e:
        print(f"[Research] DDG sync error: {e}")
    return results


async def _ddg_html_fallback(question: str, category: str) -> list:
    """Fallback: DuckDuckGo HTML scraping if ddgs library unavailable."""
    search_terms = _build_search_query(question, category)
    if not search_terms:
        return []

    try:
        async with httpx.AsyncClient(
            timeout=8.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            },
        ) as client:
            resp = await client.get(
                "https://html.duckduckgo.com/html/",
                params={"q": search_terms},
            )
            if resp.status_code == 200 and HAS_BS4:
                soup = BeautifulSoup(resp.text, "html.parser")
                results = []
                for r in soup.select(".result")[:5]:
                    title_el = r.select_one(".result__a")
                    snippet_el = r.select_one(".result__snippet")
                    if title_el:
                        results.append({
                            "title": title_el.get_text(strip=True),
                            "url": title_el.get("href", ""),
                            "snippet": snippet_el.get_text(strip=True) if snippet_el else "",
                        })
                return results
    except Exception:
        pass
    return []


def _build_search_query(question: str, category: str) -> str:
    """Build an effective search query from market question."""
    removals = {
        "will", "by", "before", "after", "in", "the", "be", "to", "of",
        "a", "an", "on", "at", "or", "up", "down", "above", "below",
        "over", "under", "?", "!", ".", ",", "'", '"', "this", "that",
        "market", "resolves", "resolution",
    }
    words = question.split()
    filtered = [w.strip("?!.,\"'") for w in words
                if w.lower() not in removals and len(w) > 2]
    terms = " ".join(filtered[:8])

    if category:
        cat_lower = category.lower()
        if "crypto" in cat_lower:
            terms += " crypto price"
        elif "sport" in cat_lower or "nba" in question.lower() or "nfl" in question.lower():
            terms += " odds prediction"
        elif "politic" in cat_lower:
            terms += " news latest"
        else:
            terms += " latest news"
    else:
        terms += " latest 2026"

    return terms


def _is_scrapeable(url: str) -> bool:
    """Check if URL is worth scraping."""
    skip_domains = [
        "youtube.com", "twitter.com", "x.com", "facebook.com",
        "instagram.com", "tiktok.com", "reddit.com", "discord.com",
        ".pdf", ".jpg", ".png", ".gif",
    ]
    url_lower = url.lower()
    return not any(d in url_lower for d in skip_domains)


async def _scrape_page(url: str) -> str:
    """Scrape a webpage and extract clean text content."""
    if url in _scrape_cache:
        cached_time, cached_result = _scrape_cache[url]
        if datetime.utcnow() - cached_time < _cache_ttl:
            return cached_result

    if not HAS_BS4:
        return ""

    try:
        async with httpx.AsyncClient(
            timeout=10.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
        ) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return ""

            content_type = resp.headers.get("content-type", "")
            if "text/html" not in content_type:
                return ""

            soup = BeautifulSoup(resp.text, "html.parser")

            for tag in soup.find_all(["script", "style", "nav", "footer",
                                       "header", "aside", "iframe", "noscript"]):
                tag.decompose()

            main = (
                soup.find("article") or
                soup.find("main") or
                soup.find(class_=re.compile(r"article|content|post|entry|story", re.I)) or
                soup.find("body")
            )

            if not main:
                return ""

            text = main.get_text(separator="\n", strip=True)
            lines = [line.strip() for line in text.split("\n") if line.strip()]
            lines = [l for l in lines if len(l) > 20]
            clean_text = "\n".join(lines[:50])

            _scrape_cache[url] = (datetime.utcnow(), clean_text)
            return clean_text

    except Exception as e:
        print(f"[Research] Scrape error for {url}: {e}")
        return ""


# ---------------------------------------------------------------------------
# Crypto price fetcher (CoinGecko — free, no API key)
# ---------------------------------------------------------------------------

CRYPTO_KEYWORDS = {
    "bitcoin": "bitcoin", "btc": "bitcoin",
    "ethereum": "ethereum", "eth": "ethereum",
    "solana": "solana", "sol": "solana",
    "dogecoin": "dogecoin", "doge": "dogecoin",
    "xrp": "ripple", "ripple": "ripple",
    "cardano": "cardano", "ada": "cardano",
    "polygon": "matic-network", "matic": "matic-network",
    "avalanche": "avalanche-2", "avax": "avalanche-2",
    "chainlink": "chainlink", "link": "chainlink",
    "polkadot": "polkadot", "dot": "polkadot",
    "litecoin": "litecoin", "ltc": "litecoin",
    "bnb": "binancecoin",
    "sui": "sui",
    "near": "near",
    "arbitrum": "arbitrum", "arb": "arbitrum",
    "optimism": "optimism", "op": "optimism",
}


async def _get_crypto_context(question: str, category: str) -> str:
    """Fetch live crypto prices if this is a crypto-related market."""
    q_lower = question.lower()
    cat_lower = (category or "").lower()

    coin_id = None
    for keyword, cg_id in CRYPTO_KEYWORDS.items():
        if keyword in q_lower or keyword in cat_lower:
            coin_id = cg_id
            break

    if not coin_id:
        return ""

    cache_key = coin_id
    if cache_key in _price_cache:
        cached_time, cached_result = _price_cache[cache_key]
        if datetime.utcnow() - cached_time < _price_cache_ttl:
            return cached_result

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={
                    "ids": coin_id,
                    "vs_currencies": "usd",
                    "include_24hr_change": "true",
                    "include_24hr_vol": "true",
                    "include_market_cap": "true",
                },
                headers={"Accept": "application/json"},
            )
            if resp.status_code == 200:
                data = resp.json().get(coin_id, {})
                price = data.get("usd", 0)
                change_24h = data.get("usd_24h_change", 0)
                vol = data.get("usd_24h_vol", 0)
                mcap = data.get("usd_market_cap", 0)

                if price > 0:
                    price_target = _extract_price_target(question)
                    target_note = ""
                    if price_target:
                        diff_pct = ((price_target - price) / price) * 100
                        direction = "above" if price > price_target else "below"
                        target_note = (
                            f"\n- Market target: ${price_target:,.0f} "
                            f"(current price is {direction} by {abs(diff_pct):.1f}%)"
                        )

                    result = (
                        f"**LIVE CRYPTO DATA ({coin_id.upper()}):**\n"
                        f"- Current price: ${price:,.2f}\n"
                        f"- 24h change: {change_24h:+.2f}%\n"
                        f"- 24h volume: ${vol:,.0f}\n"
                        f"- Market cap: ${mcap:,.0f}"
                        f"{target_note}"
                    )
                    _price_cache[cache_key] = (datetime.utcnow(), result)
                    return result

    except Exception:
        pass
    return ""


def _extract_price_target(question: str) -> Optional[float]:
    """Extract price targets from questions like 'Bitcoin above $100,000'."""
    patterns = [
        r'\$?([\d,]+(?:\.\d+)?)\s*(?:k|K)',
        r'\$([\d,]+(?:\.\d+)?)',
        r'(?:above|below|over|under|reach|hit)\s+\$?([\d,]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, question)
        if match:
            num_str = match.group(1).replace(",", "")
            try:
                val = float(num_str)
                if "k" in question[match.start():match.end()].lower():
                    val *= 1000
                if val > 100:
                    return val
            except ValueError:
                pass
    return None


# ---------------------------------------------------------------------------
# Sports context (The Odds API — free tier 500 req/month)
# ---------------------------------------------------------------------------

SPORTS_KEYWORDS = {
    "nba": "basketball_nba",
    "nfl": "americanfootball_nfl",
    "mlb": "baseball_mlb",
    "nhl": "icehockey_nhl",
    "premier league": "soccer_epl",
    "la liga": "soccer_spain_la_liga",
    "champions league": "soccer_uefa_champs_league",
    "serie a": "soccer_italy_serie_a",
    "bundesliga": "soccer_germany_bundesliga",
    "mls": "soccer_usa_mls",
    "ufc": "mma_mixed_martial_arts",
}


async def _get_sports_context(question: str, category: str) -> str:
    """Fetch sports odds if this is a sports-related market."""
    if not ODDS_API_KEY:
        return ""

    q_lower = question.lower()
    cat_lower = (category or "").lower()

    sport_key = None
    for keyword, api_key in SPORTS_KEYWORDS.items():
        if keyword in q_lower or keyword in cat_lower:
            sport_key = api_key
            break

    if not sport_key:
        return ""

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds",
                params={
                    "apiKey": ODDS_API_KEY,
                    "regions": "us",
                    "markets": "h2h",
                    "oddsFormat": "american",
                },
            )
            if resp.status_code == 200:
                games = resp.json()
                if not games:
                    return ""

                parts = [f"**LIVE {sport_key.upper()} ODDS:**"]
                for game in games[:3]:
                    home = game.get("home_team", "")
                    away = game.get("away_team", "")
                    start = game.get("commence_time", "")
                    bookmakers = game.get("bookmakers", [])
                    if bookmakers:
                        outcomes = bookmakers[0].get("markets", [{}])[0].get("outcomes", [])
                        odds_text = ", ".join(
                            f"{o['name']}: {o['price']}" for o in outcomes
                        )
                        parts.append(f"- {away} @ {home} ({start[:10]}): {odds_text}")

                return "\n".join(parts) if len(parts) > 1 else ""

    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# Related markets on Polymarket
# ---------------------------------------------------------------------------

async def _find_related_markets(market: dict) -> str:
    """Find related markets on Polymarket for cross-reference."""
    category = market.get("category", "")
    question = market.get("question", "")

    if not category:
        return ""

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                "https://gamma-api.polymarket.com/markets",
                params={
                    "closed": "false",
                    "limit": 5,
                    "order": "volume24hr",
                    "ascending": "false",
                    "tag_slug": category.lower().replace(" ", "-"),
                }
            )
            if resp.status_code == 200:
                markets = resp.json()
                related = []
                for m in markets:
                    q = m.get("question", "")
                    if q and q != question:
                        tokens = m.get("tokens", [])
                        yes_price = 0.5
                        for t in tokens:
                            if t.get("outcome", "").upper() == "YES":
                                yes_price = float(t.get("price", 0.5))
                        related.append(
                            f"- \"{q[:80]}\" — YES at ${yes_price:.2f}"
                        )

                if related:
                    return "Related Polymarket markets:\n" + "\n".join(related[:3])

    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# Market metadata
# ---------------------------------------------------------------------------

async def _get_market_metadata(market: dict) -> str:
    """Extract useful metadata from the market object."""
    parts = []
    description = market.get("description", "")
    if description:
        desc = description[:500] + "..." if len(description) > 500 else description
        parts.append(f"Market description: {desc}")

    end_date = market.get("end_date", "")
    if end_date:
        parts.append(f"Resolution date: {end_date}")

    vol_24h = market.get("volume24hr", 0)
    total_vol = market.get("volume", 0)
    liquidity = market.get("liquidity", 0)

    if vol_24h > 0 and total_vol > 0:
        vol_ratio = vol_24h / total_vol
        if vol_ratio > 0.3:
            parts.append(
                f"HIGH ACTIVITY: 24h volume is {vol_ratio*100:.0f}% of total. "
                f"Unusual recent interest."
            )

    if liquidity > 0:
        if liquidity < 5000:
            parts.append("LOW LIQUIDITY (<$5k). Prices may not reflect true probability.")
        elif liquidity > 100000:
            parts.append("High liquidity (>$100k). Well-informed consensus likely.")

    return "\n".join(parts) if parts else ""


# ---------------------------------------------------------------------------
# Sentiment analysis
# ---------------------------------------------------------------------------

async def get_market_sentiment(markets: list) -> dict:
    """Analyze overall market sentiment from a batch of markets."""
    if not markets:
        return {"sentiment": "neutral", "confidence": 0}

    bullish = sum(1 for m in markets if m.get("yes_price", 0.5) > 0.6)
    bearish = sum(1 for m in markets if m.get("yes_price", 0.5) < 0.4)
    neutral = len(markets) - bullish - bearish
    total = len(markets)

    if bullish > bearish * 2:
        sentiment = "bullish"
    elif bearish > bullish * 2:
        sentiment = "bearish"
    else:
        sentiment = "mixed"

    return {
        "sentiment": sentiment,
        "bullish_markets": bullish,
        "bearish_markets": bearish,
        "neutral_markets": neutral,
        "total_analyzed": total,
    }
