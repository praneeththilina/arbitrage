import asyncio
import aiohttp
import logging
import time
import hmac
import hashlib
import json
import websockets
from urllib.parse import urlencode
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from config import settings

logger = logging.getLogger(__name__)

@dataclass
class TickerData:
    spot_price: float = 0.0
    futures_price: float = 0.0
    funding_rate: float = 0.0
    next_funding_time: int = 0
    spot_volume_24h: float = 0.0
    futures_volume_24h: float = 0.0
    bid: float = 0.0
    ask: float = 0.0

class BinanceClient:
    def __init__(self, api_key: str = "", secret_key: str = ""):
        self.api_key = api_key or settings.binance_api_key
        self.secret_key = secret_key or settings.binance_secret_key
        self.tickers: Dict[str, TickerData] = {}
        self._running = False
        self.session: Optional[aiohttp.ClientSession] = None
        
        self.testnet = settings.testnet
        
        # REST URLs
        self.spot_url = "https://testnet.binance.vision" if self.testnet else "https://api.binance.com"
        self.fapi_url = "https://testnet.binancefuture.com" if self.testnet else "https://fapi.binance.com"
        
        # WS URLs
        self.spot_ws_url = "wss://testnet.binance.vision/stream" if self.testnet else "wss://stream.binance.com:9443/stream"
        self.fapi_ws_url = "wss://stream.binancefuture.com/stream" if self.testnet else "wss://fstream.binance.com/stream"

    def _generate_signature(self, query_string: str) -> str:
        return hmac.new(
            self.secret_key.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    async def _request(self, method: str, url: str, signed: bool = False, params: dict = None) -> dict:
        if not self.session:
            self.session = aiohttp.ClientSession()
            
        if params is None:
            params = {}
            
        headers = {}
        if signed:
            headers['X-MBX-APIKEY'] = self.api_key
            params['timestamp'] = int(time.time() * 1000)
            query_string = urlencode(params)
            signature = self._generate_signature(query_string)
            params['signature'] = signature
            
        async with self.session.request(method, url, headers=headers, params=params) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                logger.error(f"API Error {resp.status}: {data}")
            return data

    async def get_common_symbols(self, limit: int = 30, min_volume: float = 0) -> List[str]:
        url = f"{self.fapi_url}/fapi/v1/ticker/24hr"
        data = await self._request("GET", url)
        if isinstance(data, list):
            # On testnet, volumes are low, so we might want to lower or ignore min_volume
            eff_volume = min_volume if not self.testnet else 0
            valid = [d for d in data if float(d.get("quoteVolume", 0)) >= eff_volume and d["symbol"].endswith("USDT")]
            sorted_data = sorted(valid, key=lambda x: float(x["quoteVolume"]), reverse=True)
            return [d["symbol"] for d in sorted_data[:limit]]
        return []

    async def get_exchange_info(self) -> Dict[str, Dict]:
        url = f"{self.spot_url}/api/v3/exchangeInfo"
        data = await self._request("GET", url)
        return {
            s["symbol"]: {"base": s["baseAsset"], "quote": s["quoteAsset"]}
            for s in data.get("symbols", [])
            if s["status"] == "TRADING"
        }

    async def start(self, symbols: List[str], extra_spot_symbols: List[str]):
        self._running = True
        if not self.session:
            self.session = aiohttp.ClientSession()
            
        all_spot = set(symbols + extra_spot_symbols)
        
        for sym in symbols:
            if sym not in self.tickers:
                self.tickers[sym] = TickerData()
        for sym in extra_spot_symbols:
            if sym not in self.tickers:
                self.tickers[sym] = TickerData()

        logger.info("Starting Binance WebSocket streams")
        
        asyncio.create_task(self._spot_ws_loop(all_spot))
        asyncio.create_task(self._futures_ws_loop(symbols))
        asyncio.create_task(self._volume_loop(all_spot, set(symbols)))

    async def _spot_ws_loop(self, symbols: set):
        streams = [f"{sym.lower()}@bookTicker" for sym in symbols]
        # Max 1024 streams per connection, split if necessary (assume < 1000 for now)
        url = f"{self.spot_ws_url}?streams={'/'.join(streams)}"
        
        while self._running:
            try:
                async with websockets.connect(url) as ws:
                    logger.info("Connected to Spot WebSocket")
                    while self._running:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        if "data" in data:
                            d = data["data"]
                            sym = d["s"]
                            if sym in self.tickers:
                                self.tickers[sym].bid = float(d["b"])
                                self.tickers[sym].ask = float(d["a"])
                                self.tickers[sym].spot_price = (self.tickers[sym].bid + self.tickers[sym].ask) / 2
            except Exception as e:
                logger.error(f"Spot WS error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)

    async def _futures_ws_loop(self, symbols: List[str]):
        # The WebSocket for Futures is geo-blocked in some regions, dropping data silently.
        # As a robust fallback, we will use the REST API `premiumIndex` endpoint which returns mark prices for all symbols.
        url = f"{self.fapi_url}/fapi/v1/premiumIndex"
        symbol_set = set(symbols)
        
        while self._running:
            try:
                data = await self._request("GET", url)
                if isinstance(data, list):
                    for item in data:
                        sym = item.get("symbol")
                        if sym in symbol_set and sym in self.tickers:
                            self.tickers[sym].futures_price = float(item.get("markPrice", 0))
                            self.tickers[sym].funding_rate = float(item.get("lastFundingRate", 0))
                            self.tickers[sym].next_funding_time = int(item.get("nextFundingTime", 0))
                # Poll every 2 seconds (Weight=10 per request -> 300 weight/min, well under 2400 limit)
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Futures REST Error: {e}, retrying in 5s...")
                await asyncio.sleep(5)

    async def _volume_loop(self, spot_symbols: set, futures_symbols: set):
        # We can still poll volume infrequently via REST
        while self._running:
            try:
                await asyncio.gather(
                    self._fetch_spot_volumes(spot_symbols),
                    self._fetch_futures_volumes(futures_symbols)
                )
            except Exception as e:
                logger.error(f"Volume fetch error: {e}")
            await asyncio.sleep(60) 

    async def _fetch_spot_volumes(self, symbols: set):
        data = await self._request("GET", f"{self.spot_url}/api/v3/ticker/24hr")
        if isinstance(data, list):
            for item in data:
                sym = item["symbol"]
                if sym in symbols and sym in self.tickers:
                    self.tickers[sym].spot_volume_24h = float(item["quoteVolume"])

    async def _fetch_futures_volumes(self, symbols: set):
        data = await self._request("GET", f"{self.fapi_url}/fapi/v1/ticker/24hr")
        if isinstance(data, list):
            for item in data:
                sym = item["symbol"]
                if sym in symbols and sym in self.tickers:
                    self.tickers[sym].futures_volume_24h = float(item["quoteVolume"])

    async def stop(self):
        self._running = False
        if self.session:
            await self.session.close()

    # --- Live Execution Methods ---

    async def get_spot_balance(self, asset: str = "USDT") -> float:
        data = await self._request("GET", f"{self.spot_url}/api/v3/account", signed=True)
        if "balances" in data:
            for b in data["balances"]:
                if b["asset"] == asset:
                    return float(b["free"])
        return 0.0

    async def get_futures_balance(self, asset: str = "USDT") -> float:
        data = await self._request("GET", f"{self.fapi_url}/fapi/v2/balance", signed=True)
        if isinstance(data, list):
            for b in data:
                if b["asset"] == asset:
                    return float(b["availableBalance"])
        return 0.0

    async def create_spot_order(self, symbol: str, side: str, order_type: str, quantity: float, price: float = None, time_in_force: str = "GTC") -> dict:
        params = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": quantity
        }
        if order_type == "LIMIT":
            params["price"] = price
            params["timeInForce"] = time_in_force
            
        return await self._request("POST", f"{self.spot_url}/api/v3/order", signed=True, params=params)

    async def create_futures_order(self, symbol: str, side: str, order_type: str, quantity: float, price: float = None, time_in_force: str = "GTC") -> dict:
        params = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": quantity
        }
        if order_type == "LIMIT":
            params["price"] = price
            params["timeInForce"] = time_in_force
            
        return await self._request("POST", f"{self.fapi_url}/fapi/v1/order", signed=True, params=params)

    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> dict:
        params = {
            "symbol": symbol,
            "marginType": margin_type
        }
        # Ignored if already set
        return await self._request("POST", f"{self.fapi_url}/fapi/v1/marginType", signed=True, params=params)