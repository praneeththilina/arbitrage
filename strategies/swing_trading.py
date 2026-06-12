import asyncio
import logging
from typing import Dict, List, Callable, Optional, Tuple

logger = logging.getLogger(__name__)

class SwingTrading:
    """Calculates swing indicators and generates signals based on dynamically calibrated strategy models."""

    def __init__(self, client):
        self.client = client
        self._running = False
        self._on_signal: Optional[Callable] = None
        self.history: Dict[str, List[float]] = {}
        self.max_history = 100
        
        # Optimal strategy configuration per symbol (calibrated in background)
        self.calibrated_strategies: Dict[str, Tuple[str, dict]] = {}
        
        # Populate defaults
        for sym in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'SUIUSDT']:
            self.calibrated_strategies[sym] = ("EMA_RSI", {
                "ema_fast": 9,
                "ema_slow": 21,
                "rsi_period": 14,
                "rsi_oversold": 35,
                "rsi_overbought": 65
            })

    @property
    def is_running(self) -> bool:
        return self._running

    def on_opportunity(self, cb: Callable):
        self._on_signal = cb

    def stop(self):
        self._running = False

    def update_calibrated_strategy(self, symbol: str, strategy_name: str, parameters: dict):
        self.calibrated_strategies[symbol] = (strategy_name, parameters)
        logger.info(f"Updated live strategy settings for {symbol}: {strategy_name} -> {parameters}")

    async def initialize_history(self):
        logger.info("Seeding price history for swing symbols from REST API...")
        for sym in list(self.calibrated_strategies.keys()):
            try:
                klines = await self.client.get_historical_klines(sym, interval="1h", limit=100)
                if klines:
                    self.history[sym] = [float(k[4]) for k in klines]  # Use close prices
                    logger.info(f"Seeded history for {sym} with {len(self.history[sym])} candles")
            except Exception as e:
                logger.error(f"Error seeding history for {sym}: {e}")

    async def start(self):
        if self._running:
            logger.info("Swing trading scanner is already running")
            return

        self._running = True
        logger.info("Swing trading strategy scanner starting...")
        
        # Seed history on startup
        await self.initialize_history()

        try:
            while self._running:
                for sym in list(self.calibrated_strategies.keys()):
                    if sym not in self.client.tickers:
                        continue
                        
                    t = self.client.tickers[sym]
                    if not t.spot_price or t.spot_price <= 0:
                        continue

                    price = t.spot_price
                    
                    # Manage rolling price buffer
                    if sym not in self.history:
                        self.history[sym] = [price]
                    else:
                        # Replace last tick value to simulate current unclosed candle
                        if len(self.history[sym]) > 0:
                            self.history[sym][-1] = price
                        else:
                            self.history[sym].append(price)

                    hist = self.history[sym]
                    
                    # Ensure we have enough history to evaluate strategies
                    if len(hist) < 35:
                        rsi = 50.0
                        signal = "HOLD"
                        strategy_name, params = self.calibrated_strategies.get(sym, ("EMA_RSI", {}))
                    else:
                        strategy_name, params = self.calibrated_strategies.get(sym, ("EMA_RSI", {}))
                        
                        if strategy_name == "EMA_RSI":
                            rsi = self._calculate_rsi(hist, params["rsi_period"])
                            ema_fast = self._calculate_ema(hist, params["ema_fast"])
                            
                            if rsi < params["rsi_oversold"] and price > ema_fast:
                                signal = "BUY"
                            elif rsi > params["rsi_overbought"] and price < ema_fast:
                                signal = "SELL"
                            else:
                                signal = "HOLD"
                                
                        elif strategy_name == "MACD":
                            macd, sig, cross_above, cross_below = self._calculate_live_macd(
                                hist, params["macd_fast"], params["macd_slow"], params["macd_signal"]
                            )
                            # Estimate RSI for UI purposes
                            rsi = self._calculate_rsi(hist, 14)
                            
                            if cross_above and macd < 0:
                                signal = "BUY"
                            elif cross_below and macd > 0:
                                signal = "SELL"
                            else:
                                signal = "HOLD"
                                
                        elif strategy_name == "Bollinger Bands":
                            sma = sum(hist[-params["bb_period"]:]) / params["bb_period"]
                            variance = sum((x - sma) ** 2 for x in hist[-params["bb_period"]:]) / params["bb_period"]
                            std_dev = variance ** 0.5
                            upper = sma + params["bb_std_dev"] * std_dev
                            lower = sma - params["bb_std_dev"] * std_dev
                            
                            # Estimate RSI for UI purposes
                            rsi = self._calculate_rsi(hist, 14)
                            
                            if price < lower:
                                signal = "BUY"
                            elif price > upper:
                                signal = "SELL"
                            else:
                                signal = "HOLD"
                        else:
                            rsi = 50.0
                            signal = "HOLD"

                    if self._on_signal:
                        # Package strategy details for broadcast
                        data = {
                            "symbol": sym,
                            "price": price,
                            "rsi": round(rsi, 2),
                            "optimal_strategy": strategy_name,
                            "parameters": params,
                            "signal": signal,
                        }
                        asyncio.create_task(self._on_signal(data))

                # Periodically slide candle window (once an hour)
                # But for ticker updates, we evaluate every 2 seconds
                await asyncio.sleep(2.0)
        finally:
            self._running = False
            logger.info("Swing trading strategy scanner stopped")

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

    def _calculate_live_macd(self, hist: List[float], fast_period: int, slow_period: int, signal_period: int) -> Tuple[float, float, bool, bool]:
        macd_history = []
        needed_macd_count = signal_period + 5
        
        # Compute historical MACD line points to form the Signal line
        for i in range(needed_macd_count):
            idx = len(hist) - needed_macd_count + i
            sub_prices = hist[:idx + 1]
            if len(sub_prices) < slow_period:
                continue
            macd_val = self._calculate_ema(sub_prices, fast_period) - self._calculate_ema(sub_prices, slow_period)
            macd_history.append(macd_val)
            
        if len(macd_history) < signal_period:
            return 0.0, 0.0, False, False
            
        current_macd = macd_history[-1]
        prev_macd = macd_history[-2]
        
        current_signal = self._calculate_ema(macd_history, signal_period)
        prev_signal = self._calculate_ema(macd_history[:-1], signal_period)
        
        cross_above = (current_macd > current_signal) and (prev_macd <= prev_signal)
        cross_below = (current_macd < current_signal) and (prev_macd >= prev_signal)
        return current_macd, current_signal, cross_above, cross_below
