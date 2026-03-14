"""
Binance Price Feed — Real-time crypto prices for rolling market trading.

Primary: REST API polling every 3 seconds (works on Railway).
Backup: WebSocket streaming (may be blocked by Railway firewall).

v5.0: Supports BTC, ETH, SOL, XRP, BNB, DOGE (+ HYPE via CoinGecko only).
"""

import asyncio
import json
import time
import urllib.request
from collections import deque

# Global state — updated by feed, read by strategies
binance_prices = {
    "BTC":  {"price": 0.0, "timestamp": 0, "prices_5m": deque(maxlen=300), "prices_15m": deque(maxlen=900)},
    "ETH":  {"price": 0.0, "timestamp": 0, "prices_5m": deque(maxlen=300), "prices_15m": deque(maxlen=900)},
    "SOL":  {"price": 0.0, "timestamp": 0, "prices_5m": deque(maxlen=300), "prices_15m": deque(maxlen=900)},
    "XRP":  {"price": 0.0, "timestamp": 0, "prices_5m": deque(maxlen=300), "prices_15m": deque(maxlen=900)},
    "BNB":  {"price": 0.0, "timestamp": 0, "prices_5m": deque(maxlen=300), "prices_15m": deque(maxlen=900)},
    "DOGE": {"price": 0.0, "timestamp": 0, "prices_5m": deque(maxlen=300), "prices_15m": deque(maxlen=900)},
    "HYPE": {"price": 0.0, "timestamp": 0, "prices_5m": deque(maxlen=300), "prices_15m": deque(maxlen=900)},
}

# REST API config — Binance.us for US servers (Binance.com returns 451)
BINANCE_US_URL = "https://api.binance.us/api/v3/ticker/price"
REST_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT"]
REST_INTERVAL = 3  # seconds between polls

# CoinGecko for all assets (free, no geo-block)
COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=bitcoin,ethereum,solana,ripple,binancecoin,dogecoin,hyperliquid"
    "&vs_currencies=usd"
)
COINGECKO_MAP = {
    "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
    "ripple": "XRP", "binancecoin": "BNB", "dogecoin": "DOGE",
    "hyperliquid": "HYPE",
}

# WebSocket config (backup)
STREAMS = [
    "btcusdt@trade", "ethusdt@trade", "solusdt@trade",
    "xrpusdt@trade", "bnbusdt@trade", "dogeusdt@trade",
]
COMBINED_URL = f"wss://stream.binance.us:9443/stream?streams={'/'.join(STREAMS)}"

SYMBOL_MAP = {
    "BTCUSDT": "BTC", "btcusdt": "BTC",
    "ETHUSDT": "ETH", "ethusdt": "ETH",
    "SOLUSDT": "SOL", "solusdt": "SOL",
    "XRPUSDT": "XRP", "xrpusdt": "XRP",
    "BNBUSDT": "BNB", "bnbusdt": "BNB",
    "DOGEUSDT": "DOGE", "dogeusdt": "DOGE",
}

_feed_source = "none"  # Track which feed is active


def get_price(symbol: str) -> float:
    """Get current price for any tracked crypto asset."""
    return binance_prices.get(symbol, {}).get("price", 0.0)


def get_change(symbol: str, minutes: int = 5) -> float:
    """Get price change over last N minutes as a decimal (e.g., 0.02 = 2% up)."""
    data = binance_prices.get(symbol)
    if not data:
        return 0.0
    key = "prices_5m" if minutes <= 5 else "prices_15m"
    prices = data.get(key, deque())
    if len(prices) < 3:
        return 0.0  # Not enough data yet
    current = data["price"]
    # REST polls every 3s = ~20 samples per minute
    target_idx = min(len(prices) - 1, minutes * 20)
    old_price = prices[-target_idx] if target_idx < len(prices) else prices[0]
    if old_price <= 0:
        return 0.0
    return (current - old_price) / old_price


def get_status() -> dict:
    """Get feed status for debugging."""
    now = time.time()
    result = {}
    for symbol, data in binance_prices.items():
        result[symbol] = {
            "price": data["price"],
            "age_seconds": round(now - data["timestamp"], 1) if data["timestamp"] > 0 else -1,
            "samples_5m": len(data["prices_5m"]),
            "change_5m": round(get_change(symbol, 5) * 100, 2),
            "change_15m": round(get_change(symbol, 15) * 100, 2),
        }
    result["feed_source"] = _feed_source
    return result


def _update_price(symbol: str, price: float):
    """Update global price state for a symbol."""
    if price <= 0 or symbol not in binance_prices:
        return
    entry = binance_prices[symbol]
    entry["price"] = price
    entry["timestamp"] = time.time()
    entry["prices_5m"].append(price)
    entry["prices_15m"].append(price)


def _fetch_rest_prices() -> dict:
    """Fetch prices from Binance.us REST API, with CoinGecko fallback."""
    prices = {}

    # Try Binance.us first (works from US)
    try:
        for sym in REST_SYMBOLS:
            url = f"{BINANCE_US_URL}?symbol={sym}"
            req = urllib.request.Request(url, headers={"User-Agent": "PM-Intelligence/1.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                price = float(data.get("price", 0))
                mapped = SYMBOL_MAP.get(sym)
                if mapped and price > 0:
                    prices[mapped] = price
        if prices:
            return prices
    except Exception:
        pass

    # Fallback: CoinGecko (single request for all assets, free, no geo-block)
    try:
        req = urllib.request.Request(COINGECKO_URL, headers={"User-Agent": "PM-Intelligence/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            for gecko_id, symbol in COINGECKO_MAP.items():
                price = data.get(gecko_id, {}).get("usd", 0)
                if price > 0:
                    prices[symbol] = float(price)
    except Exception:
        pass

    return prices


def _fetch_coingecko_extras() -> dict:
    """Fetch prices for assets not on Binance.us (HYPE) via CoinGecko."""
    prices = {}
    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=hyperliquid&vs_currencies=usd"
        req = urllib.request.Request(url, headers={"User-Agent": "PM-Intelligence/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            price = data.get("hyperliquid", {}).get("usd", 0)
            if price > 0:
                prices["HYPE"] = float(price)
    except Exception:
        pass
    return prices


async def _rest_polling_loop():
    """Primary price feed: poll Binance REST API every 3 seconds."""
    global _feed_source
    loop = asyncio.get_event_loop()
    fail_count = 0
    coingecko_counter = 0

    while True:
        try:
            prices = await loop.run_in_executor(None, _fetch_rest_prices)
            if prices:
                for symbol, price in prices.items():
                    _update_price(symbol, price)
                if _feed_source != "rest":
                    _feed_source = "rest"
                    active = ', '.join(f'{s}=${p:,.2f}' for s, p in sorted(prices.items()))
                    print(f"[BINANCE] REST feed active: {active}")
                fail_count = 0
            else:
                fail_count += 1
                if fail_count % 20 == 1:
                    print(f"[BINANCE] REST fetch returned no prices (fail #{fail_count})")

            # Fetch HYPE from CoinGecko every 10th poll (~30s)
            coingecko_counter += 1
            if coingecko_counter % 10 == 0:
                extras = await loop.run_in_executor(None, _fetch_coingecko_extras)
                for symbol, price in extras.items():
                    _update_price(symbol, price)

        except Exception as e:
            fail_count += 1
            if fail_count % 20 == 1:
                print(f"[BINANCE] REST error: {e}")

        await asyncio.sleep(REST_INTERVAL)


async def _websocket_loop():
    """Backup price feed: Binance WebSocket streaming."""
    global _feed_source
    backoff = 1
    last_update = {s: 0 for s in binance_prices}
    throttle_ms = 500

    while True:
        try:
            import websockets
            async with websockets.connect(COMBINED_URL, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1
                if _feed_source != "websocket":
                    _feed_source = "websocket"
                    print("[BINANCE] WebSocket connected! Streaming prices.")

                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                        data = msg.get("data", msg)
                        raw_symbol = data.get("s", "").upper()
                        symbol = SYMBOL_MAP.get(raw_symbol) or SYMBOL_MAP.get(raw_symbol.lower())
                        if not symbol:
                            continue
                        price = float(data.get("p", 0))
                        if price <= 0:
                            continue

                        now_ms = int(time.time() * 1000)
                        if now_ms - last_update.get(symbol, 0) < throttle_ms:
                            continue
                        last_update[symbol] = now_ms
                        _update_price(symbol, price)
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue

        except ImportError:
            print("[BINANCE] websockets package not installed — WebSocket disabled, using REST only")
            return
        except Exception as e:
            if backoff == 1:
                print(f"[BINANCE] WebSocket failed: {e} — REST is primary feed")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)


async def binance_websocket_loop():
    """
    Main entry point: launch both REST polling (primary) and WebSocket (backup).
    REST starts immediately and works on Railway.
    WebSocket attempts connection in background — if it connects, both run.
    """
    print("[BINANCE] Starting price feeds (REST primary + WebSocket backup)...")
    print(f"[BINANCE] Tracking: {', '.join(sorted(binance_prices.keys()))}")
    await asyncio.gather(
        _rest_polling_loop(),
        _websocket_loop(),
        return_exceptions=True,
    )
