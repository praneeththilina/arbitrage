import aiosqlite
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional
from contextlib import asynccontextmanager
from config import settings

@asynccontextmanager
async def get_conn():
    conn = await aiosqlite.connect(settings.db_path)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield conn
    finally:
        await conn.close()

async def init_db():
    async with get_conn() as conn:
        await conn.executescript("""
            CREATE TABLE IF NOT EXISTS opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                symbol TEXT,
                details TEXT,
                expected_profit_pct REAL,
                expected_profit_usdt REAL,
                confidence REAL,
                timestamp TEXT NOT NULL,
                executed INTEGER DEFAULT 0
            )
        """)
        await conn.executescript("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                opportunity_id INTEGER,
                strategy TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL,
                quantity REAL,
                status TEXT DEFAULT 'pending',
                created_at TEXT NOT NULL,
                filled_at TEXT,
                fee REAL DEFAULT 0,
                pnl REAL DEFAULT 0,
                FOREIGN KEY (opportunity_id) REFERENCES opportunities(id)
            )
        """)
        await conn.executescript("""
            CREATE TABLE IF NOT EXISTS balances (
                asset TEXT PRIMARY KEY,
                free REAL DEFAULT 0,
                locked REAL DEFAULT 0,
                usdt_value REAL DEFAULT 0
            )
        """)
        await conn.commit()

async def save_opportunity(op_type: str, symbol: str, details: dict, profit_pct: float, profit_usdt: float, confidence: float) -> int:
    async with get_conn() as conn:
        cursor = await conn.execute(
            "INSERT INTO opportunities (type, symbol, details, expected_profit_pct, expected_profit_usdt, confidence, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (op_type, symbol, json.dumps(details), profit_pct, profit_usdt, confidence, datetime.now(timezone.utc).isoformat())
        )
        await conn.commit()
        return cursor.lastrowid

async def save_order(opportunity_id: int, strategy: str, symbol: str, side: str, price: float, quantity: float) -> int:
    async with get_conn() as conn:
        cursor = await conn.execute(
            "INSERT INTO orders (opportunity_id, strategy, symbol, side, price, quantity, status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
            (opportunity_id, strategy, symbol, side, price, quantity, datetime.now(timezone.utc).isoformat())
        )
        await conn.commit()
        return cursor.lastrowid

async def update_order(order_id: int, status: str, filled_at: Optional[str] = None, fee: float = 0, pnl: float = 0):
    async with get_conn() as conn:
        await conn.execute(
            "UPDATE orders SET status=?, filled_at=COALESCE(?, filled_at), fee=?, pnl=? WHERE id=?",
            (status, filled_at, fee, pnl, order_id)
        )
        await conn.commit()

async def update_balance(asset: str, free: float, locked: float, usdt_value: float):
    async with get_conn() as conn:
        await conn.execute(
            "INSERT OR REPLACE INTO balances (asset, free, locked, usdt_value) VALUES (?, ?, ?, ?)",
            (asset, free, locked, usdt_value)
        )
        await conn.commit()

async def get_recent_opportunities(limit: int = 50) -> List[Dict]:
    async with get_conn() as conn:
        async with conn.execute("SELECT * FROM opportunities ORDER BY timestamp DESC LIMIT ?", (limit,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

async def get_open_orders() -> List[Dict]:
    async with get_conn() as conn:
        async with conn.execute("SELECT * FROM orders WHERE status IN ('pending', 'open') ORDER BY created_at DESC") as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

async def get_balances() -> List[Dict]:
    async with get_conn() as conn:
        async with conn.execute("SELECT * FROM balances") as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]