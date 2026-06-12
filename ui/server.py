import asyncio
import json
import time
import logging
from typing import Set, Dict, Any, Optional
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse

from config import settings
from exchange.client import BinanceClient
from strategies.funding_arb import FundingArbitrage
from strategies.triangular_arb import TriangularArbitrage
from trading.engine import PaperEngine
from trading.live_engine import LiveEngine
from database import init_db

logger = logging.getLogger(__name__)

client: BinanceClient = None
funding_strategy: FundingArbitrage = None
triangular_strategy: TriangularArbitrage = None
engine = None
start_time: float = 0
_funding_task: asyncio.Task = None
_triangular_task: asyncio.Task = None

TEMPLATES_DIR = Path(__file__).parent / "templates"


class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)

    async def broadcast(self, data: dict):
        dead = set()
        for ws in list(self.active):
            try:
                await ws.send_json(data)
            except Exception:
                dead.add(ws)
        self.active -= dead


manager = ConnectionManager()

def _task_active(task: Optional[asyncio.Task]) -> bool:
    return task is not None and not task.done()


def _strategy_status() -> Dict[str, Any]:
    funding_running = bool(funding_strategy and funding_strategy.is_running)
    triangular_running = bool(triangular_strategy and triangular_strategy.is_running)
    return {
        "funding": {
            "available": funding_strategy is not None,
            "running": funding_running,
            "task_active": _task_active(_funding_task),
        },
        "triangular": {
            "available": triangular_strategy is not None,
            "running": triangular_running,
            "task_active": _task_active(_triangular_task),
            "paths": len(triangular_strategy.paths) if triangular_strategy else 0,
        },
    }


def _cancel_task(task: Optional[asyncio.Task]) -> None:
    if _task_active(task):
        task.cancel()


async def ticker_broadcaster():
    global client, engine, start_time, triangular_strategy
    while True:
        try:
            now = time.time()
            if client and engine:
                tickers_data = {}
                
                for sym, t in list(client.tickers.items())[:settings.max_symbols]:
                    if t.spot_price > 0 and t.futures_price > 0:
                        tickers_data[sym] = {
                            "spot": t.spot_price,
                            "futures": t.futures_price,
                            "funding": t.funding_rate,
                            "next_funding_time": t.next_funding_time,
                            "spot_vol": t.spot_volume_24h,
                            "futures_vol": t.futures_volume_24h,
                            "bid": t.bid,
                            "ask": t.ask,
                        }

                await manager.broadcast({
                    "type": "tickers",
                    "data": tickers_data
                })

                status_data = await engine.get_status()
                status_data["uptime"] = int(now - start_time)
                status_data["open_positions_list"] = engine.get_open_positions(client)
                status_data["triangular_paths"] = len(triangular_strategy.paths) if triangular_strategy else 0
                status_data["strategies"] = _strategy_status()
                
                await manager.broadcast({
                    "type": "account",
                    "data": status_data
                })

        except Exception as e:
            logger.error(f"Error in ticker broadcaster: {e}")
        
        await asyncio.sleep(0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, funding_strategy, triangular_strategy, engine, start_time
    await init_db()
    
    start_time = time.time()
    client = BinanceClient(
        api_key=settings.binance_api_key,
        secret_key=settings.binance_secret_key,
    )

    symbols = await client.get_common_symbols(
        limit=settings.max_symbols,
        min_volume=settings.min_volume_24h_usdt,
    )
    logger.info(f"Tracking {len(symbols)} symbols")

    spot_exchange = await client.get_exchange_info()

    if settings.trading_mode == "live":
        logger.info("Initializing LIVE Engine")
        engine = LiveEngine()
    else:
        logger.info("Initializing PAPER Engine")
        engine = PaperEngine()
        
    engine.set_client(client)
    await engine.start()

    funding_strategy = FundingArbitrage(client)
    
    triangular_strategy = TriangularArbitrage(client)
    triangular_strategy.resolve_dynamic_paths(symbols, spot_exchange)

    cross_symbols = {p.cross_symbol for p in triangular_strategy.paths}

    async def on_funding_op(op):
        try:
            from database import save_opportunity
            db_op_id = await save_opportunity("funding", op.symbol, op.details, op.expected_apr / 100, 0, op.confidence)
        except Exception as e:
            logger.error(f"DB Error: {e}")
            db_op_id = None

        rejection_reason = None
        validation = engine.validate_funding_op(op)
        if not validation["valid"]:
            rejection_reason = validation["reason"]
        elif not engine._auto_trade:
            rejection_reason = "auto-trade disabled"

        await manager.broadcast({
            "type": "opportunity",
            "strategy": "funding",
            "data": {
                "symbol": op.symbol,
                "funding_rate": op.funding_rate,
                "basis_pct": round(op.basis_pct, 4),
                "spot_price": op.spot_price,
                "futures_price": op.futures_price,
                "action": op.action,
                "expected_apr": round(op.expected_apr, 2),
                "confidence": round(op.confidence, 4),
                "db_id": db_op_id,
                "next_funding_time": op.details.get("next_funding_time", 0),
                "rejection_reason": rejection_reason,
            }
        })

        trade = await engine.evaluate_and_execute("funding", op)
        if trade:
            logger.info(f"Funding arb executed: {op.symbol} ${trade['notional']}")

    async def on_triangular_op(op):
        try:
            from database import save_opportunity
            db_op_id = await save_opportunity("triangular", f"{op.symbol_a}/{op.symbol_b}/{op.symbol_c}", op.details, op.profit_pct / 100, 0, op.confidence)
        except Exception as e:
            logger.error(f"DB Error: {e}")
            db_op_id = None

        rejection_reason = None
        validation = engine.validate_triangular_op(op)
        if not validation["valid"]:
            rejection_reason = validation["reason"]
        elif not engine._auto_trade:
            rejection_reason = "auto-trade disabled"

        await manager.broadcast({
            "type": "opportunity",
            "strategy": "triangular",
            "data": {
                "path": op.path,
                "profit_pct": round(op.profit_pct, 4),
                "rate": round(op.effective_rate, 6),
                "legs": op.legs,
                "confidence": round(op.confidence, 4),
                "db_id": db_op_id,
                "rejection_reason": rejection_reason,
            }
        })

        trade = await engine.evaluate_and_execute("triangular", op)
        if trade:
            logger.info(f"Triangular arb executed: {op.path} ${trade['notional']}")

    async def on_trade(result):
        await manager.broadcast({"type": "trade", "data": result})

    funding_strategy.on_opportunity(on_funding_op)
    triangular_strategy.on_opportunity(on_triangular_op)
    engine.on_trade(on_trade)

    asyncio.create_task(client.start(symbols, list(cross_symbols)))
    asyncio.create_task(ticker_broadcaster())

    yield

    if funding_strategy:
        funding_strategy.stop()
    if triangular_strategy:
        triangular_strategy.stop()
    if engine:
        engine.stop()
    if client:
        await client.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    html_path = TEMPLATES_DIR / "index.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>index.html not found</h1>", status_code=404)


@app.get("/api/status")
async def get_status():
    if engine:
        status = await engine.get_status()
        status["strategies"] = _strategy_status()
        return status
    return {"status": "error", "message": "Engine not initialized", "strategies": _strategy_status()}


@app.post("/api/start/{strategy}")
async def start_strategy(strategy: str):
    global _funding_task, _triangular_task
    if strategy == "funding" and funding_strategy:
        if funding_strategy.is_running or _task_active(_funding_task):
            return {"status": "already_running", "strategy": strategy, "strategies": _strategy_status()}
        _funding_task = asyncio.create_task(funding_strategy.start())
        return {"status": "started", "strategy": strategy, "strategies": _strategy_status()}
    elif strategy == "triangular" and triangular_strategy:
        if triangular_strategy.is_running or _task_active(_triangular_task):
            return {"status": "already_running", "strategy": strategy, "strategies": _strategy_status()}
        _triangular_task = asyncio.create_task(triangular_strategy.start())
        return {"status": "started", "strategy": strategy, "strategies": _strategy_status()}
    return {"status": "error", "message": "unknown strategy", "strategies": _strategy_status()}


@app.post("/api/stop/{strategy}")
async def stop_strategy(strategy: str):
    global _funding_task, _triangular_task
    if strategy == "funding" and funding_strategy:
        funding_strategy.stop()
        _cancel_task(_funding_task)
        _funding_task = None
        return {"status": "stopped", "strategy": strategy, "strategies": _strategy_status()}
    elif strategy == "triangular" and triangular_strategy:
        triangular_strategy.stop()
        _cancel_task(_triangular_task)
        _triangular_task = None
        return {"status": "stopped", "strategy": strategy, "strategies": _strategy_status()}
    return {"status": "error", "message": "unknown strategy", "strategies": _strategy_status()}


@app.post("/api/close/{position_id}")
async def close_position(position_id: str):
    if engine:
        result = await engine.close_position(position_id, client=client)
        if result:
            return {"status": "closed", "position_id": position_id, "result": result}
        return {"status": "error", "message": "position not found or already closed"}
    return {"status": "error", "message": "engine not initialized"}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            data = await ws.receive_text()
            try:
                msg = json.loads(data)
                action = msg.get("action")

                if action == "ping":
                    await ws.send_json({"type": "pong"})

                elif action == "set_auto_trade":
                    val = msg.get("value", False)
                    if engine:
                        engine.auto_trade = bool(val)
                        logger.info(f"auto_trade -> {engine.auto_trade}")
                    await ws.send_json({"type": "config_ack", "auto_trade": bool(val)})

                elif action == "set_trade_size":
                    pct = float(msg.get("value", 10))
                    if engine:
                        engine.trade_size_pct = pct
                    await ws.send_json({"type": "config_ack", "trade_size_pct": pct})

            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)