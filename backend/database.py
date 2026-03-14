"""Database layer — SQLite via aiosqlite."""

import aiosqlite
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

# DB_PATH: use env var when set, otherwise use a path next to this file.
# If that path is on a FUSE/network mount (causes disk I/O errors), fall back to /tmp.
_db_env = os.getenv("DB_PATH")
if _db_env:
    DB_PATH = Path(_db_env)
else:
    _default = Path(__file__).parent / "pm_trading.db"
    # Detect FUSE mount: path contains known sandbox pattern
    if "mnt/elena" in str(_default):
        DB_PATH = Path("/tmp/pm_trading.db")
    else:
        DB_PATH = _default


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS markets (
                id TEXT PRIMARY KEY, question TEXT, slug TEXT, category TEXT,
                yes_price REAL DEFAULT 0.5, no_price REAL DEFAULT 0.5,
                volume REAL DEFAULT 0, volume24hr REAL DEFAULT 0,
                liquidity REAL DEFAULT 0, active INTEGER DEFAULT 1,
                closed INTEGER DEFAULT 0, end_date TEXT, last_updated TEXT
            );
            CREATE TABLE IF NOT EXISTS market_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT, market_id TEXT,
                yes_price REAL, volume REAL, volume24hr REAL, liquidity REAL,
                timestamp TEXT, FOREIGN KEY (market_id) REFERENCES markets(id)
            );
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT, market_id TEXT,
                market_question TEXT, score REAL, confidence REAL, direction TEXT,
                factors_json TEXT, yes_price REAL, outcome TEXT DEFAULT 'PENDING',
                pnl_pct REAL, created_at TEXT, resolved_at TEXT
            );
            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id INTEGER,
                market_id TEXT, market_question TEXT, direction TEXT,
                entry_price REAL, exit_price REAL, shares REAL, cost REAL,
                pnl REAL, status TEXT DEFAULT 'OPEN', created_at TEXT, closed_at TEXT,
                market_type TEXT DEFAULT 'MOMENTUM', days_left INTEGER,
                hold_hours REAL, tp_price REAL, sl_price REAL
            );
            CREATE TABLE IF NOT EXISTS portfolio (
                id INTEGER PRIMARY KEY, cash_balance REAL DEFAULT 10000.0,
                total_invested REAL DEFAULT 0.0, total_pnl REAL DEFAULT 0.0,
                win_count INTEGER DEFAULT 0, loss_count INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS signal_weights (
                factor TEXT PRIMARY KEY, weight REAL DEFAULT 1.0
            );
            CREATE TABLE IF NOT EXISTS trade_explanations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id INTEGER,
                market_question TEXT, direction TEXT, entry_explanation TEXT,
                exit_explanation TEXT, lesson TEXT, factors_json TEXT, score REAL,
                outcome TEXT DEFAULT 'PENDING', pnl REAL, created_at TEXT, closed_at TEXT,
                FOREIGN KEY (trade_id) REFERENCES paper_trades(id)
            );
            CREATE TABLE IF NOT EXISTS crypto_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, direction TEXT,
                entry_price REAL, exit_price REAL, quantity REAL, cost REAL,
                leveraged_exposure REAL, pnl REAL, leverage_multiplier INTEGER DEFAULT 2,
                signal_reason TEXT, status TEXT DEFAULT 'OPEN', created_at TEXT, closed_at TEXT
            );
            CREATE TABLE IF NOT EXISTS crypto_portfolio (
                id INTEGER PRIMARY KEY, cash_balance REAL DEFAULT 10000.0,
                total_invested REAL DEFAULT 0.0, total_pnl REAL DEFAULT 0.0,
                win_count INTEGER DEFAULT 0, loss_count INTEGER DEFAULT 0,
                leverage_multiplier INTEGER DEFAULT 2
            );

            -- ── Self-Improvement Engine Tables ────────────────────────────────
            -- Records every closed trade result for the learning loop
            CREATE TABLE IF NOT EXISTS signal_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER,
                market_type TEXT,
                direction TEXT,
                entry_price REAL,
                exit_price REAL,
                pnl REAL,
                won INTEGER DEFAULT 0,
                signal_factors_json TEXT DEFAULT '{}',
                created_at TEXT
            );

            -- Stores current dynamic thresholds and enabled/disabled state per strategy
            CREATE TABLE IF NOT EXISTS strategy_params (
                param_name TEXT PRIMARY KEY,
                param_value TEXT,
                updated_at TEXT
            );

            -- Audit log of every parameter change the engine makes, with reasoning
            CREATE TABLE IF NOT EXISTS improvement_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                overall_win_rate REAL,
                gap_to_target REAL,
                stats_json TEXT,
                changes_json TEXT,
                created_at TEXT
            );

            CREATE TABLE IF NOT EXISTS crypto_trade_meta (
                trade_id INTEGER PRIMARY KEY, snapshot_json TEXT,
                FOREIGN KEY (trade_id) REFERENCES crypto_trades(id)
            );
            CREATE TABLE IF NOT EXISTS crypto_factor_weights (
                factor TEXT PRIMARY KEY, weight REAL DEFAULT 1.0, updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS news_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                headline TEXT, source TEXT, impact_score REAL,
                impact_level TEXT, published TEXT, created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS smart_wallet_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT, address TEXT, side TEXT,
                size REAL, price REAL, win_rate REAL,
                timestamp TEXT, created_at TEXT
            );

            -- ── Decision Log — tracks EVERY market encounter ────────────────
            CREATE TABLE IF NOT EXISTS decision_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT,
                market_question TEXT,
                strategy TEXT,
                score REAL,
                decision TEXT,
                reason TEXT,
                yes_price REAL,
                direction TEXT,
                factors_json TEXT DEFAULT '{}',
                created_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_decision_log_created ON decision_log(created_at);
            CREATE INDEX IF NOT EXISTS idx_decision_log_decision ON decision_log(decision);

            -- ── Trade Memory — schema owned by memory_system.py ──────────────
            -- DO NOT modify here — memory_system.init_memory() is the source of truth
            CREATE TABLE IF NOT EXISTS trade_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER,
                market_id TEXT,
                market_question TEXT,
                category TEXT,
                direction TEXT,
                action TEXT,
                entry_price REAL,
                exit_price REAL,
                pnl REAL,
                outcome TEXT,
                confidence REAL,
                estimated_probability REAL,
                edge REAL,
                reasoning TEXT,
                key_evidence TEXT,
                risk_factors TEXT,
                lesson TEXT,
                volume_spike INTEGER DEFAULT 0,
                model_used TEXT,
                tokens_used INTEGER DEFAULT 0,
                created_at TEXT,
                resolved_at TEXT
            );

            -- ── Agent Lessons — schema owned by memory_system.py ─────────────
            CREATE TABLE IF NOT EXISTS agent_lessons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lesson TEXT NOT NULL,
                category TEXT,
                market_type TEXT,
                outcome TEXT,
                importance REAL DEFAULT 1.0,
                times_referenced INTEGER DEFAULT 0,
                created_at TEXT NOT NULL
            );

            -- ── Category Stats — schema owned by memory_system.py ────────────
            CREATE TABLE IF NOT EXISTS category_stats (
                category TEXT PRIMARY KEY,
                total_trades INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                total_pnl REAL DEFAULT 0,
                avg_confidence REAL DEFAULT 0,
                avg_edge REAL DEFAULT 0,
                last_updated TEXT
            );
        """)
        # Leverage trading tables
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS leverage_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                market_id TEXT,
                market_question TEXT,
                direction TEXT,
                entry_price REAL,
                exit_price REAL,
                shares REAL,
                cost REAL,
                leverage_multiplier INTEGER DEFAULT 2,
                pnl REAL,
                status TEXT DEFAULT 'OPEN',
                created_at TEXT,
                closed_at TEXT
            );
            CREATE TABLE IF NOT EXISTS leverage_portfolio (
                id INTEGER PRIMARY KEY,
                cash_balance REAL DEFAULT 10000.0,
                total_invested REAL DEFAULT 0.0,
                total_pnl REAL DEFAULT 0.0,
                win_count INTEGER DEFAULT 0,
                loss_count INTEGER DEFAULT 0,
                leverage_multiplier INTEGER DEFAULT 2
            );
        """)
        # ── Live trading tables ────────────────────────────────────────────────
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS live_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT,
                market_question TEXT,
                direction TEXT,
                market_type TEXT,
                entry_price REAL,
                exit_price REAL,
                shares REAL,
                cost REAL,
                pnl REAL,
                clob_order_id TEXT,
                token_id TEXT,
                status TEXT DEFAULT 'OPEN',
                created_at TEXT,
                closed_at TEXT
            );
            CREATE TABLE IF NOT EXISTS live_portfolio (
                id INTEGER PRIMARY KEY,
                cash_balance REAL DEFAULT 0.0,
                total_invested REAL DEFAULT 0.0,
                total_pnl REAL DEFAULT 0.0,
                win_count INTEGER DEFAULT 0,
                loss_count INTEGER DEFAULT 0
            );
        """)
        # ── Auto-migrate: add columns added after initial release ────────────
        existing = [r[1] for r in await (await db.execute("PRAGMA table_info(paper_trades)")).fetchall()]
        for col, typedef in [("market_type","TEXT DEFAULT 'MOMENTUM'"),("days_left","INTEGER"),
                              ("hold_hours","REAL"),("tp_price","REAL"),("sl_price","REAL")]:
            if col not in existing:
                await db.execute(f"ALTER TABLE paper_trades ADD COLUMN {col} {typedef}")
        await db.commit()
        await db.execute("INSERT OR IGNORE INTO portfolio (id, cash_balance) VALUES (1, 100000.0)")
        await db.execute("INSERT OR IGNORE INTO crypto_portfolio (id, cash_balance, leverage_multiplier) VALUES (1, 10000.0, 2)")
        await db.execute("INSERT OR IGNORE INTO leverage_portfolio (id, cash_balance, leverage_multiplier) VALUES (1, 10000.0, 2)")
        await db.execute("INSERT OR IGNORE INTO live_portfolio (id, cash_balance) VALUES (1, 0.0)")
        # Seed all 9 signal weights — volume_spike raised to 3.5 (insider signal)
        for factor, weight in [
            ("volume_spike", 3.5), ("price_zone", 1.0), ("liquidity", 1.0),
            ("momentum", 1.0), ("category", 1.0),
            ("news_impact", 1.5), ("smart_wallet", 1.5), ("end_date", 1.2),
            ("buy_no_early", 2.0),  # High weight — proven behavioral bias edge
        ]:
            await db.execute("INSERT OR IGNORE INTO signal_weights (factor, weight) VALUES (?, ?)", (factor, weight))
        await db.commit()


async def upsert_market(market: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO markets (id,question,slug,category,yes_price,no_price,volume,volume24hr,liquidity,active,closed,end_date,last_updated)
            VALUES (:id,:question,:slug,:category,:yes_price,:no_price,:volume,:volume24hr,:liquidity,:active,:closed,:end_date,:last_updated)
            ON CONFLICT(id) DO UPDATE SET yes_price=excluded.yes_price, no_price=excluded.no_price,
            volume=excluded.volume, volume24hr=excluded.volume24hr, liquidity=excluded.liquidity,
            active=excluded.active, closed=excluded.closed, last_updated=excluded.last_updated
        """, market)
        await db.commit()


async def save_market_snapshot(market_id, yes_price, volume, volume24hr, liquidity):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO market_history (market_id,yes_price,volume,volume24hr,liquidity,timestamp) VALUES (?,?,?,?,?,?)",
            (market_id, yes_price, volume, volume24hr, liquidity, datetime.utcnow().isoformat()))
        await db.execute("DELETE FROM market_history WHERE market_id=? AND id NOT IN (SELECT id FROM market_history WHERE market_id=? ORDER BY id DESC LIMIT 50)", (market_id, market_id))
        await db.commit()


async def get_market_history(market_id: str, limit: int = 20) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM market_history WHERE market_id=? ORDER BY id DESC LIMIT ?", (market_id, limit)) as c:
            rows = await c.fetchall()
    return [dict(r) for r in reversed(rows)]


async def get_all_markets(limit: int = 100) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM markets WHERE active=1 AND closed=0 ORDER BY volume24hr DESC LIMIT ?", (limit,)) as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def get_market(market_id: str) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM markets WHERE id=?", (market_id,)) as c:
            row = await c.fetchone()
    return dict(row) if row else None


async def save_signal(signal: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO signals (market_id,market_question,score,confidence,direction,factors_json,yes_price,created_at)
            VALUES (:market_id,:market_question,:score,:confidence,:direction,:factors_json,:yes_price,:created_at)
        """, signal)
        await db.commit()
        return cursor.lastrowid


async def get_recent_signals(limit: int = 30) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)) as c:
            rows = await c.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["factors"] = json.loads(d.get("factors_json") or "{}")
        result.append(d)
    return result


async def resolve_signal(signal_id: int, outcome: str, pnl_pct: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE signals SET outcome=?,pnl_pct=?,resolved_at=? WHERE id=?",
            (outcome, pnl_pct, datetime.utcnow().isoformat(), signal_id))
        await db.commit()


async def save_paper_trade(trade: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO paper_trades
                (signal_id,market_id,market_question,direction,entry_price,shares,cost,market_type,status,created_at)
            VALUES
                (:signal_id,:market_id,:market_question,:direction,:entry_price,:shares,:cost,:market_type,:status,:created_at)
        """, {
            "signal_id":       trade.get("signal_id"),
            "market_id":       trade.get("market_id"),
            "market_question": trade.get("market_question"),
            "direction":       trade.get("direction"),
            "entry_price":     trade.get("entry_price"),
            "shares":          trade.get("shares"),
            "cost":            trade.get("cost"),
            "market_type":     trade.get("market_type", "MOMENTUM"),
            "status":          trade.get("status", "OPEN"),
            "created_at":      trade.get("created_at"),
        })
        await db.commit()
        return cursor.lastrowid


async def get_open_paper_trades() -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM paper_trades WHERE status='OPEN' ORDER BY id DESC") as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def get_all_paper_trades(limit: int = 50) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM paper_trades ORDER BY id DESC LIMIT ?", (limit,)) as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def close_paper_trade(trade_id: int, exit_price: float, pnl: float, outcome: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE paper_trades SET exit_price=?,pnl=?,status=?,closed_at=? WHERE id=?",
            (exit_price, pnl, outcome, datetime.utcnow().isoformat(), trade_id))
        await db.commit()
    # Sync win/loss counts from trades table — avoids increment drift bugs
    await _sync_portfolio_stats()


async def _sync_portfolio_stats():
    """Recalculate win_count, loss_count, total_pnl from paper_trades.
    Called after every trade close so counters are always accurate."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE portfolio SET
                win_count  = (SELECT COUNT(*) FROM paper_trades
                               WHERE pnl > 0 AND status NOT IN ('OPEN')),
                loss_count = (SELECT COUNT(*) FROM paper_trades
                               WHERE pnl < 0 AND status NOT IN ('OPEN')),
                total_pnl  = (SELECT COALESCE(SUM(pnl), 0) FROM paper_trades
                               WHERE status NOT IN ('OPEN') AND pnl IS NOT NULL)
            WHERE id = 1
        """)
        await db.commit()


async def get_portfolio() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM portfolio WHERE id=1") as c:
            row = await c.fetchone()
    return dict(row) if row else {}


async def update_portfolio(cash_delta: float = 0, pnl_delta: float = 0,
                            invested_delta: float = 0, win: Optional[bool] = None):
    """Update cash + invested. Win/loss counts are synced separately via close_paper_trade."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE portfolio SET cash_balance=cash_balance+?,total_invested=total_invested+? WHERE id=1",
            (cash_delta, invested_delta)
        )
        await db.commit()


async def get_signal_weights() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM signal_weights") as c:
            rows = await c.fetchall()
    return {r["factor"]: r["weight"] for r in rows}


async def update_signal_weight(factor: str, new_weight: float):
    async with aiosqlite.connect(DB_PATH) as db:
        # UPSERT — creates row if factor doesn't exist yet
        await db.execute(
            "INSERT OR REPLACE INTO signal_weights (factor, weight) VALUES (?, ?)",
            (factor, max(0.1, min(3.0, new_weight)))
        )
        await db.commit()


async def save_trade_explanation(expl: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO trade_explanations (trade_id,market_question,direction,entry_explanation,factors_json,score,outcome,created_at)
            VALUES (?,?,?,?,?,?,'PENDING',?)
        """, (expl["trade_id"], expl["market_question"], expl["direction"],
              expl["entry_explanation"], expl.get("factors_json","{}"), expl.get("score",0), expl["created_at"]))
        await db.commit()
        return cursor.lastrowid


async def update_trade_explanation_exit(trade_id: int, exit_explanation: str,
                                         lesson: str, outcome: str, pnl: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE trade_explanations SET exit_explanation=?,lesson=?,outcome=?,pnl=?,closed_at=? WHERE trade_id=?
        """, (exit_explanation, lesson, outcome, pnl, datetime.utcnow().isoformat(), trade_id))
        await db.commit()


async def get_trade_explanations(limit: int = 30) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM trade_explanations ORDER BY id DESC LIMIT ?", (limit,)) as c:
            rows = await c.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["factors"] = json.loads(d.get("factors_json") or "{}")
        result.append(d)
    return result


async def save_crypto_trade(trade: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO crypto_trades (symbol,direction,entry_price,quantity,cost,leveraged_exposure,leverage_multiplier,signal_reason,status,created_at)
            VALUES (:symbol,:direction,:entry_price,:quantity,:cost,:leveraged_exposure,:leverage_multiplier,:signal_reason,:status,:created_at)
        """, trade)
        await db.commit()
        return cursor.lastrowid


async def get_open_crypto_trades() -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM crypto_trades WHERE status='OPEN' ORDER BY id DESC") as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def get_all_crypto_trades(limit: int = 50) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM crypto_trades ORDER BY id DESC LIMIT ?", (limit,)) as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def close_crypto_trade(trade_id: int, exit_price: float, pnl: float, outcome: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE crypto_trades SET exit_price=?,pnl=?,status=?,closed_at=? WHERE id=?",
            (exit_price, pnl, outcome, datetime.utcnow().isoformat(), trade_id))
        await db.commit()


async def get_crypto_portfolio() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM crypto_portfolio WHERE id=1") as c:
            row = await c.fetchone()
    return dict(row) if row else {}


async def update_crypto_portfolio(cash_delta: float = 0, pnl_delta: float = 0,
                                   invested_delta: float = 0, win: Optional[bool] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        if win is True:
            await db.execute("UPDATE crypto_portfolio SET cash_balance=cash_balance+?,total_pnl=total_pnl+?,total_invested=total_invested+?,win_count=win_count+1 WHERE id=1",
                (cash_delta, pnl_delta, invested_delta))
        elif win is False:
            await db.execute("UPDATE crypto_portfolio SET cash_balance=cash_balance+?,total_pnl=total_pnl+?,total_invested=total_invested+?,loss_count=loss_count+1 WHERE id=1",
                (cash_delta, pnl_delta, invested_delta))
        else:
            await db.execute("UPDATE crypto_portfolio SET cash_balance=cash_balance+?,total_pnl=total_pnl+?,total_invested=total_invested+? WHERE id=1",
                (cash_delta, pnl_delta, invested_delta))
        await db.commit()


async def set_crypto_leverage(multiplier: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE crypto_portfolio SET leverage_multiplier=? WHERE id=1", (multiplier,))
        await db.commit()


async def save_crypto_trade_meta(trade_id: int, snapshot_json: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO crypto_trade_meta (trade_id,snapshot_json) VALUES (?,?)", (trade_id, snapshot_json))
        await db.commit()


async def get_closed_trades_with_meta(limit: int = 40) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT ct.id,ct.symbol,ct.direction,ct.pnl,ct.status,ct.leverage_multiplier,ct.created_at,ct.closed_at,ctm.snapshot_json
            FROM crypto_trades ct LEFT JOIN crypto_trade_meta ctm ON ctm.trade_id=ct.id
            WHERE ct.status!='OPEN' ORDER BY ct.id DESC LIMIT ?
        """, (limit,)) as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def get_crypto_factor_weights() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT factor,weight FROM crypto_factor_weights") as c:
            rows = await c.fetchall()
    return {r["factor"]: r["weight"] for r in rows}


async def save_crypto_factor_weights(weights: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        for factor, weight in weights.items():
            await db.execute("INSERT OR REPLACE INTO crypto_factor_weights (factor,weight,updated_at) VALUES (?,?,?)",
                (factor, round(weight,5), datetime.utcnow().isoformat()))
        await db.commit()


async def count_closed_crypto_trades() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM crypto_trades WHERE status!='OPEN'") as c:
            row = await c.fetchone()
    return row[0] if row else 0


async def save_news_events(events: list):
    """Save news headlines to DB (keep last 200)."""
    async with aiosqlite.connect(DB_PATH) as db:
        now = datetime.utcnow().isoformat()
        for e in events[:50]:  # batch limit
            await db.execute(
                "INSERT INTO news_events (headline,source,impact_score,impact_level,published,created_at) VALUES (?,?,?,?,?,?)",
                (e.get("headline",""), e.get("source",""), e.get("impact_score",0),
                 e.get("impact_level","LOW"), e.get("published",""), now)
            )
        await db.execute("DELETE FROM news_events WHERE id NOT IN (SELECT id FROM news_events ORDER BY id DESC LIMIT 200)")
        await db.commit()


async def save_smart_wallet_activity(activity: dict):
    """Save smart wallet market activity to DB."""
    async with aiosqlite.connect(DB_PATH) as db:
        now = datetime.utcnow().isoformat()
        for market_id, entries in activity.items():
            for e in entries[:10]:
                await db.execute(
                    "INSERT INTO smart_wallet_activity (market_id,address,side,size,price,win_rate,timestamp,created_at) VALUES (?,?,?,?,?,?,?,?)",
                    (market_id, e.get("address",""), e.get("side",""), e.get("size",0),
                     e.get("price",0), e.get("win_rate",0), e.get("timestamp",""), now)
                )
        await db.execute("DELETE FROM smart_wallet_activity WHERE id NOT IN (SELECT id FROM smart_wallet_activity ORDER BY id DESC LIMIT 1000)")
        await db.commit()


async def get_signal_performance_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT COUNT(*) as total,
            SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END) as losses,
            AVG(CASE WHEN outcome!='PENDING' THEN pnl_pct ELSE NULL END) as avg_pnl
            FROM signals
        """) as c:
            row = await c.fetchone()
    return dict(row) if row else {}


# ── Leverage Trading CRUD ──────────────────────────────────────────────────────

async def save_leverage_trade(trade: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO leverage_trades
                (signal_id,market_id,market_question,direction,entry_price,shares,cost,
                 leverage_multiplier,status,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            trade.get("signal_id"), trade.get("market_id"), trade.get("market_question"),
            trade.get("direction"), trade.get("entry_price"), trade.get("shares"),
            trade.get("cost"), trade.get("leverage_multiplier", 2),
            trade.get("status", "OPEN"), trade.get("created_at"),
        ))
        await db.commit()
        return cursor.lastrowid


async def get_open_leverage_trades() -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM leverage_trades WHERE status='OPEN' ORDER BY id DESC") as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def get_all_leverage_trades(limit: int = 50) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM leverage_trades ORDER BY id DESC LIMIT ?", (limit,)) as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def close_leverage_trade(trade_id: int, exit_price: float, pnl: float, outcome: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE leverage_trades SET exit_price=?,pnl=?,status=?,closed_at=? WHERE id=?",
            (exit_price, pnl, outcome, datetime.utcnow().isoformat(), trade_id)
        )
        await db.commit()
    await _sync_leverage_stats()


async def _sync_leverage_stats():
    """Recalculate leverage portfolio win/loss/pnl from leverage_trades."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE leverage_portfolio SET
                win_count  = (SELECT COUNT(*) FROM leverage_trades
                               WHERE pnl > 0 AND status NOT IN ('OPEN')),
                loss_count = (SELECT COUNT(*) FROM leverage_trades
                               WHERE pnl < 0 AND status NOT IN ('OPEN')),
                total_pnl  = (SELECT COALESCE(SUM(pnl), 0) FROM leverage_trades
                               WHERE status NOT IN ('OPEN') AND pnl IS NOT NULL)
            WHERE id = 1
        """)
        await db.commit()


async def get_leverage_portfolio() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM leverage_portfolio WHERE id=1") as c:
            row = await c.fetchone()
    return dict(row) if row else {}


async def update_leverage_portfolio(cash_delta: float = 0, invested_delta: float = 0):
    """Update cash + invested. Win/loss synced via close_leverage_trade."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE leverage_portfolio SET cash_balance=cash_balance+?,total_invested=total_invested+? WHERE id=1",
            (cash_delta, invested_delta)
        )
        await db.commit()


async def set_leverage_multiplier(multiplier: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE leverage_portfolio SET leverage_multiplier=? WHERE id=1", (multiplier,))
        await db.commit()


async def get_recent_news(limit: int = 30) -> list:
    """Fetch most recent news events from DB."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT headline, source, impact_score, impact_level, published, created_at "
            "FROM news_events ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ) as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


# ── Live trading DB functions ──────────────────────────────────────────────────

async def save_live_trade(trade: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """INSERT INTO live_trades
               (market_id, market_question, direction, market_type, entry_price,
                shares, cost, clob_order_id, token_id, status, created_at)
               VALUES (:market_id,:market_question,:direction,:market_type,:entry_price,
                       :shares,:cost,:clob_order_id,:token_id,:status,:created_at)""",
            trade
        )
        await db.commit()
        return cur.lastrowid


async def close_live_trade(trade_id: int, exit_price: float, pnl: float, outcome: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE live_trades SET exit_price=?,pnl=?,status=?,closed_at=? WHERE id=?",
            (exit_price, pnl, outcome, datetime.utcnow().isoformat(), trade_id)
        )
        await db.commit()


async def get_open_live_trades() -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM live_trades WHERE status='OPEN'") as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def get_all_live_trades(limit: int = 100) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM live_trades ORDER BY created_at DESC LIMIT ?", (limit,)
        ) as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def get_live_portfolio() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM live_portfolio WHERE id=1") as c:
            row = await c.fetchone()
    return dict(row) if row else {}


async def update_live_portfolio(
    cash_delta: float = 0, invested_delta: float = 0,
    pnl_delta: float = 0, win: Optional[bool] = None
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE live_portfolio SET
               cash_balance=cash_balance+?,
               total_invested=total_invested+?,
               total_pnl=total_pnl+?
               WHERE id=1""",
            (cash_delta, invested_delta, pnl_delta)
        )
        if win is True:
            await db.execute("UPDATE live_portfolio SET win_count=win_count+1 WHERE id=1")
        elif win is False:
            await db.execute("UPDATE live_portfolio SET loss_count=loss_count+1 WHERE id=1")
        await db.commit()


async def set_live_balance(balance: float):
    """Set the starting live balance when user funds their Polymarket account."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE live_portfolio SET cash_balance=? WHERE id=1", (balance,)
        )
        await db.commit()


async def set_signal_weights(weights: dict):
    """Update signal factor weights — called by self-improvement engine."""
    async with aiosqlite.connect(DB_PATH) as db:
        for factor, weight in weights.items():
            await db.execute("""
                INSERT INTO signal_weights (factor, weight) VALUES (?, ?)
                ON CONFLICT(factor) DO UPDATE SET weight = excluded.weight
            """, (factor, weight))
        await db.commit()


# ── Decision Log CRUD ────────────────────────────────────────────────────────

async def save_decision_log(entry: dict):
    """Log a market decision (ENTER, SKIP, GATE_REJECT, STRATEGY_CYCLE)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO decision_log
                (market_id, market_question, strategy, score, decision, reason,
                 yes_price, direction, factors_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry.get("market_id", ""),
            entry.get("market_question", ""),
            entry.get("strategy", ""),
            entry.get("score", 0),
            entry.get("decision", "SKIP"),
            entry.get("reason", ""),
            entry.get("yes_price", 0),
            entry.get("direction", ""),
            json.dumps(entry.get("factors", {})),
            entry.get("created_at", datetime.utcnow().isoformat()),
        ))
        # Auto-prune to last 5000 rows
        await db.execute("""
            DELETE FROM decision_log WHERE id NOT IN (
                SELECT id FROM decision_log ORDER BY id DESC LIMIT 5000
            )
        """)
        await db.commit()


async def get_decision_log(limit: int = 100, offset: int = 0,
                           decision_filter: str = None,
                           strategy_filter: str = None) -> list:
    """Paginated decision log with optional filters."""
    query = "SELECT * FROM decision_log WHERE 1=1"
    params = []
    if decision_filter:
        query += " AND decision = ?"
        params.append(decision_filter)
    if strategy_filter:
        query += " AND strategy = ?"
        params.append(strategy_filter)
    query += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as c:
            rows = await c.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["factors"] = json.loads(d.get("factors_json") or "{}")
        result.append(d)
    return result


async def get_decision_stats() -> dict:
    """Aggregate decision counts by type and strategy."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # By decision type
        async with db.execute("""
            SELECT decision, COUNT(*) as count FROM decision_log GROUP BY decision
        """) as c:
            by_decision = {r["decision"]: r["count"] for r in await c.fetchall()}
        # By strategy
        async with db.execute("""
            SELECT strategy, COUNT(*) as count FROM decision_log GROUP BY strategy
        """) as c:
            by_strategy = {r["strategy"]: r["count"] for r in await c.fetchall()}
        # Total
        async with db.execute("SELECT COUNT(*) as total FROM decision_log") as c:
            total = (await c.fetchone())["total"]
    return {"total": total, "by_decision": by_decision, "by_strategy": by_strategy}


# ── Brain Export/Import helpers ──────────────────────────────────────────────

async def export_brain() -> dict:
    """Export all learning state as a JSON-serializable dict."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        data = {}
        for table in ["agent_lessons", "trade_memory", "category_stats",
                       "signal_weights", "signal_performance", "strategy_params",
                       "improvement_log", "trade_explanations", "decision_log"]:
            try:
                async with db.execute(f"SELECT * FROM {table}") as c:
                    data[table] = [dict(r) for r in await c.fetchall()]
            except Exception:
                data[table] = []
        # Also include portfolio stats
        async with db.execute("SELECT * FROM portfolio WHERE id=1") as c:
            row = await c.fetchone()
            data["portfolio"] = dict(row) if row else {}
        # Paper trades
        async with db.execute("SELECT * FROM paper_trades ORDER BY id DESC LIMIT 500") as c:
            data["paper_trades"] = [dict(r) for r in await c.fetchall()]
    return data


async def import_brain(data: dict):
    """Restore learning state from exported JSON."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Restore agent_lessons
        for lesson in data.get("agent_lessons", []):
            await db.execute("""
                INSERT OR IGNORE INTO agent_lessons
                    (lesson, source_trade_id, category, outcome, times_referenced, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (lesson.get("lesson",""), lesson.get("source_trade_id"),
                  lesson.get("category",""), lesson.get("outcome",""),
                  lesson.get("times_referenced",0), lesson.get("created_at","")))
        # Restore trade_memory
        for mem in data.get("trade_memory", []):
            await db.execute("""
                INSERT OR IGNORE INTO trade_memory
                    (trade_id, market_question, direction, entry_reasoning, exit_outcome,
                     lesson, category, pnl, won, created_at, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (mem.get("trade_id"), mem.get("market_question",""),
                  mem.get("direction",""), mem.get("entry_reasoning",""),
                  mem.get("exit_outcome",""), mem.get("lesson",""),
                  mem.get("category",""), mem.get("pnl",0), mem.get("won",0),
                  mem.get("created_at",""), mem.get("closed_at","")))
        # Restore signal_weights
        for sw in data.get("signal_weights", []):
            await db.execute("""
                INSERT OR REPLACE INTO signal_weights (factor, weight)
                VALUES (?, ?)
            """, (sw.get("factor",""), sw.get("weight",1.0)))
        # Restore strategy_params
        for sp in data.get("strategy_params", []):
            await db.execute("""
                INSERT OR REPLACE INTO strategy_params (param_name, param_value, updated_at)
                VALUES (?, ?, ?)
            """, (sp.get("param_name",""), sp.get("param_value",""), sp.get("updated_at","")))
        # Restore improvement_log
        for il in data.get("improvement_log", []):
            await db.execute("""
                INSERT OR IGNORE INTO improvement_log
                    (overall_win_rate, gap_to_target, stats_json, changes_json, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (il.get("overall_win_rate",0), il.get("gap_to_target",0),
                  il.get("stats_json","{}"), il.get("changes_json","{}"),
                  il.get("created_at","")))
        # Restore category_stats
        for cs in data.get("category_stats", []):
            await db.execute("""
                INSERT OR REPLACE INTO category_stats
                    (category, total_trades, wins, losses, total_pnl, avg_pnl, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (cs.get("category",""), cs.get("total_trades",0), cs.get("wins",0),
                  cs.get("losses",0), cs.get("total_pnl",0), cs.get("avg_pnl",0),
                  cs.get("updated_at","")))
        await db.commit()


# ── Brain query helpers ──────────────────────────────────────────────────────

async def get_all_lessons(limit: int = 200) -> list:
    """Get all agent lessons sorted by times_referenced."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM agent_lessons ORDER BY times_referenced DESC LIMIT ?", (limit,)
        ) as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def get_trade_timeline(limit: int = 100) -> list:
    """Get trades joined with explanations and memory for full reasoning chain."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT pt.*, te.entry_explanation, te.exit_explanation, te.lesson as te_lesson,
                   te.factors_json as te_factors, te.score as te_score,
                   tm.reasoning as entry_reasoning, tm.outcome as exit_outcome,
                   tm.lesson as tm_lesson, tm.category as tm_category,
                   tm.confidence as tm_confidence, tm.edge as tm_edge,
                   tm.key_evidence as tm_key_evidence, tm.risk_factors as tm_risk_factors,
                   tm.model_used as tm_model
            FROM paper_trades pt
            LEFT JOIN trade_explanations te ON te.trade_id = pt.id
            LEFT JOIN trade_memory tm ON tm.trade_id = pt.id
            ORDER BY pt.id DESC LIMIT ?
        """, (limit,)) as c:
            rows = await c.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["te_factors_parsed"] = json.loads(d.get("te_factors") or "{}")
        result.append(d)
    return result


async def get_improvement_history(limit: int = 50) -> list:
    """Get all improvement_log entries with parsed JSON."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM improvement_log ORDER BY id DESC LIMIT ?", (limit,)
        ) as c:
            rows = await c.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["stats"] = json.loads(d.get("stats_json") or "{}")
        d["changes"] = json.loads(d.get("changes_json") or "{}")
        result.append(d)
    return result


async def get_weights_history() -> list:
    """Extract weight changes from improvement_log."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT changes_json, created_at FROM improvement_log ORDER BY id ASC"
        ) as c:
            rows = await c.fetchall()
    history = []
    for r in rows:
        try:
            changes = json.loads(r["changes_json"] or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(changes, dict):
            continue
        weight_changes = {k: v for k, v in changes.items() if "weight" in k.lower() or "signal" in k.lower()}
        if weight_changes:
            history.append({"changes": weight_changes, "created_at": r["created_at"]})
    return history


async def get_category_stats() -> list:
    """Get all category performance stats."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM category_stats ORDER BY total_trades DESC"
        ) as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def get_strategy_params() -> list:
    """Get all current strategy parameters."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM strategy_params ORDER BY param_name") as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]
