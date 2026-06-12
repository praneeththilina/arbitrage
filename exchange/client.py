import asyncio
import json
import time
import hmac
import hashlib
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field

import httpx
import websockets


@dataclass
class TickerData:
    symbol: str
    spot_price: float = 0.0
    futures_price: float = 0.0
    funding_rate: float = 0.0
    next_funding_time: int = 0
    mark_price: float = 0.0
    spot_volume_24h: float = 0.0
    futures_volume_24h: float = 0.0
    spot_change_24h: float = 0.0
    futures_change_24h: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    last_price: float = 0.0


@dataclass
class OrderBookLevel:
    price: float
    qty: float


@dataclass
class OrderBook:
    symbol: str
    bids: List[OrderBookLevel] = field(default_factory=list)
    asks: List[OrderBookLevel] = field(default_factory=list)
    timestamp: int = 0


class BinanceClient:
    SPOT_BASE = "wss://stream.binance.com:9443/ws"
    FUTURES_BASE = "wss://fstream.binance.com/ws"
    REST_BASE = "https://api.binance.com"
    FUTURES_REST_BASE = "https://fapi.binance.com"

    def __init__(self, api_key: str = "", secret_key: str = ""):
        self.api_key = api_key
        self.secret_key = secret_key
        self.tickers: Dict[str, TickerData] = {}
        self.orderbooks: Dict[str, OrderBook] = {}
        self._spot_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._futures_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._callbacks: Dict[str, List[Callable]] = {
            "ticker": [],
            "funding": [],
            "orderbook": [],
            "trade": [],
        }

    def on(self, event: str, cb: Callable):
        self._callbacks[event].append(cb)

    def _emit(self, event: str, data: Any):
        for cb in self._callbacks.get(event, []):
            try:
                cb(data)
            except Exception as e:
                print(f"[Callback error] {event}: {e}")

    async def get_exchange_info(self) -> List[Dict]:
        async with httpx.AsyncClient() as cli:
            resp = await cli.get(f"{self.REST_BASE}/api/v3/exchangeInfo")
            data = resp.json()
            return [s for s in data["symbols"] if s["status"] == "TRADING"]

    async def get_futures_exchange_info(self) -> List[Dict]:
        async with httpx.AsyncClient() as cli:
            resp = await cli.get(f"{self.FUTURES_REST_BASE}/fapi/v1/exchangeInfo")
            data = resp.json()
            return [s for s in data["symbols"] if s["status"] == "TRADING"]

    async def get_top_symbols(self, limit: int = 30, min_volume: float = 10_000_000) -> List[str]:
        async with httpx.AsyncClient() as cli:
            tickers_resp = await cli.get(f"{self.REST_BASE}/api/v3/ticker/24hr")
            tickers = tickers_resp.json()

        volumes = []
        for t in tickers:
            vol_usdt = float(t.get("quoteVolume", 0))
            if vol_usdt >= min_volume and t["symbol"].endswith("USDT"):
                volumes.append((t["symbol"], vol_usdt))

        volumes.sort(key=lambda x: x[1], reverse=True)
        return [s for s, v in volumes[:limit]]

    async def get_funding_rates(self, symbols: List[str]) -> Dict[str, float]:
        async with httpx.AsyncClient() as cli:
            resp = await cli.get(f"{self.FUTURES_REST_BASE}/fapi/v1/premiumIndex")
            data = resp.json()

        result = {}
        for item in data:
            if item["symbol"] in symbols:
                result[item["symbol"]] = float(item["lastFundingRate"])
        return result

    def _sign_request(self, params: Dict) -> Dict:
        query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        signature = hmac.new(
            self.secret_key.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
        return params

    async def _spot_ws_loop(self, symbols: List[str]):
        streams = "/".join(f"{s.lower()}@ticker" for s in symbols)
        uri = f"{self.SPOT_BASE}/stream?streams={streams}"
        while self._running:
            try:
                async with websockets.connect(uri, ping_interval=20) as ws:
                    self._spot_ws = ws
                    async for msg in ws:
                        data = json.loads(msg)
                        if data.get("stream") and data.get("data"):
                            self._handle_spot_ticker(data["data"])
            except Exception as e:
                if self._running:
                    await asyncio.sleep(5)

    async def _futures_ws_loop(self, symbols: List[str]):
        ticker_streams = "/".join(f"{s.lower()}@ticker" for s in symbols)
        mark_streams = "/".join(f"{s.lower()}@markPrice" for s in symbols)
        streams = f"{ticker_streams}/{mark_streams}"
        uri = f"{self.FUTURES_BASE}/stream?streams={streams}"
        while self._running:
            try:
                async with websockets.connect(uri, ping_interval=20) as ws:
                    self._futures_ws = ws
                    async for msg in ws:
                        data = json.loads(msg)
                        if data.get("stream") and data.get("data"):
                            stream = data["stream"]
                            d = data["data"]
                            if "markPrice" in stream:
                                self._handle_mark_price(d)
                            else:
                                self._handle_futures_ticker(d)
            except Exception as e:
                if self._running:
                    await asyncio.sleep(5)

    def _handle_spot_ticker(self, d: Dict):
        sym = d["s"]
        if sym not in self.tickers:
            self.tickers[sym] = TickerData(symbol=sym)
        t = self.tickers[sym]
        t.spot_price = float(d["c"])
        t.spot_volume_24h = float(d["q"])
        t.spot_change_24h = float(d["P"])
        t.bid = float(d["b"])
        t.ask = float(d["a"])
        t.last_price = float(d["c"])
        self._emit("ticker", (sym, "spot", t))

    def _handle_futures_ticker(self, d: Dict):
        sym = d["s"]
        if sym not in self.tickers:
            self.tickers[sym] = TickerData(symbol=sym)
        t = self.tickers[sym]
        t.futures_price = float(d["c"])
        t.futures_volume_24h = float(d["q"])
        t.futures_change_24h = float(d["P"])
        self._emit("ticker", (sym, "futures", t))

    def _handle_mark_price(self, d: Dict):
        sym = d["s"]
        if sym not in self.tickers:
            self.tickers[sym] = TickerData(symbol=sym)
        t = self.tickers[sym]
        t.funding_rate = float(d["r"])
        t.next_funding_time = int(d["T"])
        t.mark_price = float(d["p"])
        self._emit("funding", (sym, t.funding_rate, t.next_funding_time))

    async def start(self, symbols: List[str]):
        self._running = True
        await asyncio.gather(
            self._spot_ws_loop(symbols),
            self._futures_ws_loop(symbols),
        )

    async def stop(self):
        self._running = False
        if self._spot_ws:
            await self._spot_ws.close()
        if self._futures_ws:
            await self._futures_ws.close()

    async def place_order(self, symbol: str, side: str, order_type: str,
                          quantity: float, price: Optional[float] = None) -> Dict:
        if not self.api_key:
            return {"error": "no_api_key"}
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": order_type.upper(),
            "quantity": quantity,
            "timestamp": int(time.time() * 1000),
        }
        if price:
            params["price"] = price
        params = self._sign_request(params)

        async with httpx.AsyncClient() as cli:
            resp = await cli.post(
                f"{self.REST_BASE}/api/v3/order",
                headers={"X-MBX-APIKEY": self.api_key},
                params=params,
            )
            return resp.json()

    async def get_account(self) -> Dict:
        if not self.api_key:
            return {}
        params = {"timestamp": int(time.time() * 1000)}
        params = self._sign_request(params)
        async with httpx.AsyncClient() as cli:
            resp = await cli.get(
                f"{self.REST_BASE}/api/v3/account",
                headers={"X-MBX-APIKEY": self.api_key},
                params=params,
            )
            return resp.json()
