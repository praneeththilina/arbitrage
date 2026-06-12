import sqlite3
import json
from datetime import datetime, timezone
from typing import Optional
from config import settings


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
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
        );

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
        );

        CREATE TABLE IF NOT EXISTS balances (
            asset TEXT PRIMARY KEY,
            free REAL DEFAULT 0,
            locked REAL DEFAULT 0,
            usdt_value REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL,
            quantity REAL,
            fee REAL,
            realized_pnl REAL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id)
        );
    """)
    conn.commit()
    conn.close()


def save_opportunity(op_type: str, symbol: str, details: dict,
                     profit_pct: float, profit_usdt: float, confidence: float) -> int:
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO opportunities (type, symbol, details, expected_profit_pct, expected_profit_usdt, confidence, timestamp) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (op_type, symbol, json.dumps(details), profit_pct, profit_usdt, confidence,
         datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    op_id = cur.lastrowid
    conn.close()
    return op_id


def save_order(opportunity_id: int, strategy: str, symbol: str, side: str,
               price: float, quantity: float) -> int:
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO orders (opportunity_id, strategy, symbol, side, price, quantity, status, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
        (opportunity_id, strategy, symbol, side, price, quantity,
         datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    order_id = cur.lastrowid
    conn.close()
    return order_id


def update_order(order_id: int, status: str, filled_at: Optional[str] = None,
                 fee: float = 0, pnl: float = 0):
    conn = get_conn()
    conn.execute(
        "UPDATE orders SET status=?, filled_at=COALESCE(?, filled_at), fee=?, pnl=? WHERE id=?",
        (status, filled_at, fee, pnl, order_id)
    )
    conn.commit()
    conn.close()


def update_balance(asset: str, free: float, locked: float, usdt_value: float):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO balances (asset, free, locked, usdt_value) VALUES (?, ?, ?, ?)",
        (asset, free, locked, usdt_value)
    )
    conn.commit()
    conn.close()


def get_recent_opportunities(limit: int = 50):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM opportunities ORDER BY timestamp DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_open_orders():
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM orders WHERE status IN ('pending', 'open') ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_balances():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM balances").fetchall()
    conn.close()
    return [dict(r) for r in rows]
