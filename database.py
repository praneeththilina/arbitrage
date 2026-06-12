import aiosqlite
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional
from contextlib import asynccontextmanager
from config import settings

_global_conn = None

@asynccontextmanager
async def get_conn():
    global _global_conn
    if _global_conn is None:
        _global_conn = await aiosqlite.connect(settings.db_path)
        _global_conn.row_factory = aiosqlite.Row
        await _global_conn.execute("PRAGMA journal_mode=WAL")
        await _global_conn.execute("PRAGMA synchronous=NORMAL")
        await _global_conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield _global_conn
    finally:
        pass  # Do not close the connection

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
        await conn.executescript("""
            CREATE TABLE IF NOT EXISTS swing_positions (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                current_price REAL,
                quantity REAL NOT NULL,
                leverage INTEGER DEFAULT 1,
                stop_loss REAL,
                take_profit REAL,
                unrealized_pnl REAL DEFAULT 0,
                total_pnl REAL DEFAULT 0,
                status TEXT DEFAULT 'open',
                opened_at TEXT NOT NULL,
                closed_at TEXT
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

async def save_swing_position(pos_id: str, symbol: str, side: str, entry_price: float, quantity: float, leverage: int, stop_loss: Optional[float] = None, take_profit: Optional[float] = None):
    async with get_conn() as conn:
        await conn.execute(
            "INSERT INTO swing_positions (id, symbol, side, entry_price, current_price, quantity, leverage, stop_loss, take_profit, unrealized_pnl, total_pnl, status, opened_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0.0, 0.0, 'open', ?)",
            (pos_id, symbol, side, entry_price, entry_price, quantity, leverage, stop_loss, take_profit, datetime.now(timezone.utc).isoformat())
        )
        await conn.commit()

async def update_swing_position_price(pos_id: str, current_price: float, unrealized_pnl: float):
    async with get_conn() as conn:
        await conn.execute(
            "UPDATE swing_positions SET current_price=?, unrealized_pnl=? WHERE id=?",
            (current_price, unrealized_pnl, pos_id)
        )
        await conn.commit()

async def close_swing_position(pos_id: str, total_pnl: float, exit_price: float):
    async with get_conn() as conn:
        await conn.execute(
            "UPDATE swing_positions SET status='closed', current_price=?, unrealized_pnl=0.0, total_pnl=?, closed_at=? WHERE id=?",
            (exit_price, total_pnl, datetime.now(timezone.utc).isoformat(), pos_id)
        )
        await conn.commit()

async def get_open_swing_positions() -> List[Dict]:
    async with get_conn() as conn:
        async with conn.execute("SELECT * FROM swing_positions WHERE status='open' ORDER BY opened_at DESC") as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

async def get_closed_swing_positions(limit: int = 50) -> List[Dict]:
    async with get_conn() as conn:
        async with conn.execute("SELECT * FROM swing_positions WHERE status='closed' ORDER BY closed_at DESC LIMIT ?", (limit,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

async def get_swing_stats() -> Dict:
    async with get_conn() as conn:
        async with conn.execute("SELECT COUNT(*), SUM(total_pnl) FROM swing_positions WHERE status='closed'") as cursor:
            row = await cursor.fetchone()
            total_closed = row[0] if row else 0
            net_profit = row[1] if row and row[1] is not None else 0.0
        
        async with conn.execute("SELECT COUNT(*) FROM swing_positions WHERE status='closed' AND total_pnl > 0") as cursor:
            row = await cursor.fetchone()
            wins = row[0] if row else 0
            
        win_rate = (wins / total_closed * 100) if total_closed > 0 else 0.0
        
        return {
            "win_rate": round(win_rate, 2),
            "net_profit": round(net_profit, 2),
            "total_closed": total_closed
        }