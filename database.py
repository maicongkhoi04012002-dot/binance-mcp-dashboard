"""
database.py - Shared SQLite layer for TradingView signals
"""
import aiosqlite
import sqlite3
import json
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "signals.db")


def init_db_sync():
    """Initialize DB synchronously (called at startup)."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT    NOT NULL,
            ticker      TEXT    NOT NULL,
            timeframe   TEXT,
            action      TEXT    NOT NULL,
            price       REAL,
            rsi         REAL,
            ema9        REAL,
            wma45       REAL,
            close       REAL,
            open        REAL,
            high        REAL,
            low         REAL,
            volume      REAL,
            extra       TEXT,
            raw         TEXT    NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ticker ON signals(ticker)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_action  ON signals(action)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_time    ON signals(received_at)")
    conn.commit()
    conn.close()


async def insert_signal(payload: dict) -> int:
    """Insert a new signal, return its ID."""
    now = datetime.utcnow().isoformat()
    # Extract known fields; anything else goes into 'extra'
    known = {"ticker", "timeframe", "action", "price",
              "rsi", "ema9", "wma45", "close", "open", "high", "low", "volume"}
    extra = {k: v for k, v in payload.items() if k not in known}

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            INSERT INTO signals
              (received_at, ticker, timeframe, action, price,
               rsi, ema9, wma45, close, open, high, low, volume,
               extra, raw)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now,
            payload.get("ticker", "UNKNOWN"),
            payload.get("timeframe", ""),
            payload.get("action", ""),
            payload.get("price"),
            payload.get("rsi"),
            payload.get("ema9"),
            payload.get("wma45"),
            payload.get("close"),
            payload.get("open"),
            payload.get("high"),
            payload.get("low"),
            payload.get("volume"),
            json.dumps(extra) if extra else None,
            json.dumps(payload),
        ))
        await db.commit()
        return cur.lastrowid


async def fetch_latest(ticker: str | None = None, limit: int = 10) -> list[dict]:
    """Fetch the most recent signals, optionally filtered by ticker."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if ticker:
            cur = await db.execute(
                "SELECT * FROM signals WHERE ticker=? ORDER BY id DESC LIMIT ?",
                (ticker.upper(), limit)
            )
        else:
            cur = await db.execute(
                "SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)
            )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def fetch_active(ticker: str | None = None) -> list[dict]:
    """
    Return signals where the most recent signal per ticker is BUY or SELL
    (i.e., not yet closed/exited).
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        query = """
            SELECT s.*
            FROM signals s
            INNER JOIN (
                SELECT ticker, MAX(id) AS max_id
                FROM signals
                GROUP BY ticker
            ) latest ON s.id = latest.max_id
            WHERE UPPER(s.action) IN ('BUY','SELL','LONG','SHORT')
        """
        if ticker:
            query += " AND s.ticker = ?"
            cur = await db.execute(query, (ticker.upper(),))
        else:
            cur = await db.execute(query)
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def fetch_stats(ticker: str | None = None) -> dict:
    """Return signal count breakdown by action."""
    async with aiosqlite.connect(DB_PATH) as db:
        base = "SELECT action, COUNT(*) as cnt FROM signals"
        if ticker:
            cur = await db.execute(
                base + " WHERE ticker=? GROUP BY action", (ticker.upper(),)
            )
        else:
            cur = await db.execute(base + " GROUP BY action")
        rows = await cur.fetchall()
        stats: dict = {row[0]: row[1] for row in rows}

        # Total
        if ticker:
            cur2 = await db.execute(
                "SELECT COUNT(*) FROM signals WHERE ticker=?", (ticker.upper(),)
            )
        else:
            cur2 = await db.execute("SELECT COUNT(*) FROM signals")
        total = (await cur2.fetchone())[0]
        stats["_total"] = total
        return stats


async def fetch_by_ticker_timeframe(
    ticker: str, timeframe: str, limit: int = 20
) -> list[dict]:
    """Fetch signals filtered by ticker AND timeframe."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """SELECT * FROM signals
               WHERE ticker=? AND timeframe=?
               ORDER BY id DESC LIMIT ?""",
            (ticker.upper(), timeframe, limit)
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]
