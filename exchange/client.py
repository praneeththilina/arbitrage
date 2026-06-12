import asyncio
import aiohttp
import logging
from typing import Dict, List
from dataclasses import dataclass

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
        self.api_key = api_key
        self.secret_key = secret_key
        self.tickers: Dict[str, TickerData] = {}
        self._running = False
        self.session = None

    async def get_common_symbols(self, limit: int = 30, min_volume: float = 0) -> List[str]:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr") as resp:
                data = await resp.json()
                valid = [d for d in data if float(d.get("quoteVolume", 0)) > min_volume and d["symbol"].endswith("USDT")]
                sorted_data = sorted(valid, key=lambda x: float(x["quoteVolume"]), reverse=True)
                return [d["symbol"] for d in sorted_data[:limit]]

    async def get_exchange_info(self) -> Dict[str, Dict]:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.binance.com/api/v3/exchangeInfo") as resp:
                data = await resp.json()
                return {
                    s["symbol"]: {"base": s["baseAsset"], "quote": s["quoteAsset"]}
                    for s in data.get("symbols", [])
                    if s["status"] == "TRADING"
                }

    async def start(self, symbols: List[str], extra_spot_symbols: List[str]):
        self._running = True
        self.session = aiohttp.ClientSession()
        all_spot = set(symbols + extra_spot_symbols)
        
        for sym in symbols:
            if sym not in self.tickers:
                self.tickers[sym] = TickerData()
        for sym in extra_spot_symbols:
            if sym not in self.tickers:
                self.tickers[sym] = TickerData()

        logger.info("Starting Binance data streams")
        
        asyncio.create_task(self._volume_loop(all_spot, set(symbols)))
        
        while self._running:
            try:
                await asyncio.gather(
                    self._fetch_spot_prices(all_spot),
                    self._fetch_futures_data(symbols)
                )
            except Exception as e:
                logger.error(f"Data fetch error: {e}")
            await asyncio.sleep(1)

    async def _volume_loop(self, spot_symbols: set, futures_symbols: set):
        while self._running:
            try:
                await asyncio.gather(
                    self._fetch_spot_volumes(spot_symbols),
                    self._fetch_futures_volumes(futures_symbols)
                )
            except Exception as e:
                logger.error(f"Volume fetch error: {e}")
            await asyncio.sleep(10) 

    async def _fetch_spot_prices(self, symbols: set):
        async with self.session.get("https://api.binance.com/api/v3/ticker/bookTicker") as resp:
            data = await resp.json()
            for item in data:
                sym = item["symbol"]
                if sym in symbols:
                    if sym not in self.tickers:
                        self.tickers[sym] = TickerData()
                    self.tickers[sym].bid = float(item["bidPrice"])
                    self.tickers[sym].ask = float(item["askPrice"])
                    self.tickers[sym].spot_price = (self.tickers[sym].bid + self.tickers[sym].ask) / 2

    async def _fetch_futures_data(self, symbols: List[str]):
        async with self.session.get("https://fapi.binance.com/fapi/v1/premiumIndex") as resp:
            data = await resp.json()
            for item in data:
                sym = item["symbol"]
                if sym in symbols:
                    if sym not in self.tickers:
                        self.tickers[sym] = TickerData()
                    self.tickers[sym].futures_price = float(item["markPrice"])
                    self.tickers[sym].funding_rate = float(item["lastFundingRate"])
                    self.tickers[sym].next_funding_time = int(item["nextFundingTime"])

    async def _fetch_spot_volumes(self, symbols: set):
        async with self.session.get("https://api.binance.com/api/v3/ticker/24hr") as resp:
            data = await resp.json()
            for item in data:
                sym = item["symbol"]
                if sym in symbols and sym in self.tickers:
                    self.tickers[sym].spot_volume_24h = float(item["quoteVolume"])

    async def _fetch_futures_volumes(self, symbols: set):
        async with self.session.get("https://fapi.binance.com/fapi/v1/ticker/24hr") as resp:
            data = await resp.json()
            for item in data:
                sym = item["symbol"]
                if sym in symbols and sym in self.tickers:
                    self.tickers[sym].futures_volume_24h = float(item["quoteVolume"])

    async def stop(self):
        self._running = False
        if self.session:
            await self.session.close()