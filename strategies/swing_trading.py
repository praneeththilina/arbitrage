import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Callable, Optional

logger = logging.getLogger(__name__)


class SwingTrading:
    """Calculates swing indicators (RSI, EMAs) and generates buy/sell trading signals."""

    def __init__(self, client):
        self.client = client
        self._running = False
        self._on_signal: Optional[Callable] = None
        self.history: Dict[str, List[float]] = {}
        self.max_history = 50

    @property
    def is_running(self) -> bool:
        return self._running

    def on_opportunity(self, cb: Callable):
        # Named on_opportunity to match the general strategy listener interface
        self._on_signal = cb

    def stop(self):
        self._running = False

    async def start(self):
        if self._running:
            logger.info("Swing trading scanner is already running")
            return

        self._running = True
        logger.info("Swing trading scanner started")
        try:
            while self._running:
                for sym, t in list(self.client.tickers.items()):
                    if not t.spot_price or t.spot_price <= 0:
                        continue

                    price = t.spot_price
                    if sym not in self.history:
                        self.history[sym] = [price]
                    else:
                        self.history[sym].append(price)
                        if len(self.history[sym]) > self.max_history:
                            self.history[sym].pop(0)

                    hist = self.history[sym]
                    if len(hist) < 15:
                        # Use default stats until we accumulate enough price periods
                        rsi = 50.0
                        ema9 = price
                        ema21 = price
                        signal = "HOLD"
                    else:
                        rsi = self._calculate_rsi(hist, 14)
                        ema9 = self._calculate_ema(hist, 9)
                        ema21 = self._calculate_ema(hist, 21)

                        # Signal trigger conditions
                        if rsi < 35 and price > ema9:
                            signal = "BUY"
                        elif rsi > 65 and price < ema9:
                            signal = "SELL"
                        else:
                            signal = "HOLD"

                    if self._on_signal:
                        data = {
                            "symbol": sym,
                            "price": price,
                            "rsi": round(rsi, 2),
                            "ema_9": round(ema9, 2),
                            "ema_21": round(ema21, 2),
                            "signal": signal,
                        }
                        asyncio.create_task(self._on_signal(data))

                await asyncio.sleep(2.0)
        finally:
            self._running = False
            logger.info("Swing trading scanner stopped")

    def _calculate_ema(self, prices: List[float], period: int) -> float:
        if len(prices) < period:
            return prices[-1]

        ema = sum(prices[:period]) / period
        multiplier = 2 / (period + 1)

        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        return ema

    def _calculate_rsi(self, prices: List[float], period: int) -> float:
        if len(prices) < period + 1:
            return 50.0

        gains = []
        losses = []
        for i in range(1, len(prices)):
            change = prices[i] - prices[i - 1]
            if change > 0:
                gains.append(change)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(change))

        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        if avg_loss == 0:
            return 50.0 if avg_gain == 0 else 100.0

        # Smooth changes
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            return 50.0 if avg_gain == 0 else 100.0

        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))
