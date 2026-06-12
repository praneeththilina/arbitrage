import asyncio
import json
import time
import logging
from typing import Set, Dict, Any
from pathlib import Path

from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse

from config import settings
from exchange.client import BinanceClient
from strategies.funding_arb import FundingArbitrage
from strategies.triangular_arb import TriangularArbitrage, TRIANGLE_TEMPLATES
from trading.engine import PaperEngine
from database import init_db, get_recent_opportunities, get_open_orders, get_balances

client: BinanceClient = None
funding_strategy: FundingArbitrage = None
triangular_strategy: TriangularArbitrage = None
paper: PaperEngine = None
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
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.add(ws)
        self.active -= dead


manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, funding_strategy, triangular_strategy, paper, start_time
    start_time = time.time()
    init_db()

    client = BinanceClient(
        api_key=settings.binance_api_key,
        secret_key=settings.binance_secret_key,
    )

    symbols = await client.get_common_symbols(
        limit=settings.max_symbols,
        min_volume=settings.min_volume_24h_usdt,
    )
    logger.info(f"Tracking {len(symbols)} symbols (spot+futures): {symbols}")

    spot_exchange = await client.get_exchange_info()
    spot_symbols = {s["symbol"] for s in spot_exchange}

    cross_symbols = set()
    for _, b1, b2 in TRIANGLE_TEMPLATES:
        for sym in [f"{b1}{b2}", f"{b2}{b1}"]:
            if sym in spot_symbols:
                cross_symbols.add(sym)
    logger.info(f"Cross-pair symbols to track: {cross_symbols}")

    paper = PaperEngine()
    await paper.start()

    funding_strategy = FundingArbitrage(client)
    triangular_strategy = TriangularArbitrage(client)
    triangular_strategy.resolve_paths(spot_symbols)

    async def on_funding_op(op):
        db_op_id = None
        try:
            from database import save_opportunity
            db_op_id = save_opportunity(
                "funding", op.symbol, op.details,
                op.expected_apr / 100, 0, op.confidence
            )
        except:
            pass

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
                "details": op.details,
                "db_id": db_op_id,
                "next_funding_time": op.details.get("next_funding_time", 0),
            }
        })

        trade = await paper.evaluate_and_execute("funding", op)
        if trade:
            logger.info(f"Funding arb executed: {op.symbol} ${trade['notional']}")

    async def on_triangular_op(op):
        db_op_id = None
        try:
            from database import save_opportunity
            db_op_id = save_opportunity(
                "triangular", f"{op.symbol_a}/{op.symbol_b}/{op.symbol_c}",
                op.details, op.profit_pct / 100, 0, op.confidence
            )
        except:
            pass

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
            }
        })

        trade = await paper.evaluate_and_execute("triangular", op)
        if trade:
            logger.info(f"Triangular arb executed: {op.path} ${trade['notional']}")

    async def on_trade(result):
        await manager.broadcast({
            "type": "trade",
            "data": result,
        })

    funding_strategy.on_opportunity(on_funding_op)
    triangular_strategy.on_opportunity(on_triangular_op)
    paper.on_trade(on_trade)

    asyncio.create_task(client.start(symbols, extra_spot_symbols=list(cross_symbols)))
    asyncio.create_task(ticker_broadcaster())

    yield

    funding_strategy.stop()
    triangular_strategy.stop()
    paper.stop()
    await client.stop()


app = FastAPI(title="Arbitrage Dashboard", lifespan=lifespan)


async def ticker_broadcaster():
    while True:
        now = time.time()
        if client and client.tickers:
            tickers = {}
            for sym, t in list(client.tickers.items())[:settings.max_symbols]:
                tickers[sym] = {
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
                "data": tickers,
                "timestamp": int(now * 1000),
            })

        if paper:
            status = await paper.get_status()
            status["uptime"] = int(now - start_time)
            await manager.broadcast({
                "type": "account",
                "data": status,
            })

        await asyncio.sleep(0.5)


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = TEMPLATES_DIR / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/api/opportunities")
async def list_opportunities():
    return get_recent_opportunities(100)


@app.get("/api/orders")
async def list_orders():
    return get_open_orders()


@app.get("/api/balances")
async def list_balances():
    return get_balances()


@app.get("/api/account")
async def account_status():
    if paper:
        status = await paper.get_status()
        status["uptime"] = int(time.time() - start_time)
        return status
    return {}


@app.get("/api/config")
async def get_config():
    if paper:
        return await paper.get_config()
    return {}


@app.post("/api/config")
async def update_config(req: Request):
    body = await req.json()
    if paper:
        if "auto_trade" in body:
            paper.auto_trade = bool(body["auto_trade"])
            logger.info(f"auto_trade -> {paper.auto_trade}")
        if "trade_size_pct" in body:
            paper.trade_size_pct = float(body["trade_size_pct"])
            logger.info(f"trade_size_pct -> {paper.trade_size_pct}%")
        return {"status": "ok", "config": await paper.get_config()}
    return {"status": "error"}


@app.post("/api/start/{strategy}")
async def start_strategy(strategy: str):
    global _funding_task, _triangular_task
    if strategy == "funding" and funding_strategy:
        if _funding_task and not _funding_task.done():
            return {"status": "already_running", "strategy": strategy}
        _funding_task = asyncio.create_task(funding_strategy.start())
        return {"status": "started", "strategy": strategy}
    elif strategy == "triangular" and triangular_strategy:
        if _triangular_task and not _triangular_task.done():
            return {"status": "already_running", "strategy": strategy}
        _triangular_task = asyncio.create_task(triangular_strategy.start())
        return {"status": "started", "strategy": strategy}
    return {"status": "error", "message": "unknown strategy"}


@app.post("/api/stop/{strategy}")
async def stop_strategy(strategy: str):
    if strategy == "funding" and funding_strategy:
        funding_strategy.stop()
        return {"status": "stopped", "strategy": strategy}
    elif strategy == "triangular" and triangular_strategy:
        triangular_strategy.stop()
        return {"status": "stopped", "strategy": strategy}
    return {"status": "error", "message": "unknown strategy"}


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
                    if paper:
                        paper.auto_trade = bool(val)
                        logger.info(f"auto_trade -> {paper.auto_trade}")
                    await ws.send_json({"type": "config_ack", "auto_trade": bool(val)})

                elif action == "set_trade_size":
                    pct = float(msg.get("value", 10))
                    if paper:
                        paper.trade_size_pct = pct
                    await ws.send_json({"type": "config_ack", "trade_size_pct": pct})

            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)
