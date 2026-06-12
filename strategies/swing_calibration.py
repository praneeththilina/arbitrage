import asyncio
import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional

logger = logging.getLogger(__name__)

class SwingCalibrator:
    """Auto-calibration engine running parameter optimization backtests on past candle data."""

    def __init__(self, client, swing_trading_strategy=None, symbols: List[str] = None):
        self.client = client
        self.swing_trading_strategy = swing_trading_strategy
        self.symbols = symbols or ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'SUIUSDT']
        self._running = False

    def stop(self):
        self._running = False

    async def start(self):
        if self._running:
            logger.info("Swing auto-calibrator is already running")
            return

        self._running = True
        logger.info("Swing auto-calibrator loop started")
        
        # Run initial calibration immediately on startup
        await self.run_calibration_cycle()

        try:
            while self._running:
                # Sleep for 1 hour before next cycle
                for _ in range(3600):
                    if not self._running:
                        break
                    await asyncio.sleep(1)
                
                if self._running:
                    await self.run_calibration_cycle()
        finally:
            self._running = False
            logger.info("Swing auto-calibrator loop stopped")

    async def run_calibration_cycle(self):
        logger.info("Starting a new swing strategy calibration cycle")
        for symbol in self.symbols:
            if not self._running:
                break
            try:
                # Fetch recent candles (300 periods of 1-hour candles)
                klines = await self.client.get_historical_klines(symbol, interval="1h", limit=300)
                if not klines or len(klines) < 50:
                    logger.warning(f"Insufficient kline data to calibrate {symbol}")
                    continue

                # Run backtesting parameter sweep
                best_strat, best_params, pnl, win_rate, trades = self.calibrate(symbol, klines)
                
                # Persist in DB
                from database import save_swing_calibration
                await save_swing_calibration(symbol, best_strat, best_params, pnl, win_rate, trades)

                # Push to live swing trading strategy cache
                if self.swing_trading_strategy:
                    self.swing_trading_strategy.update_calibrated_strategy(symbol, best_strat, best_params)

                logger.info(f"Auto-calibrated {symbol}: Strategy={best_strat}, Win Rate={win_rate:.2f}%, PnL={pnl:.2f}%")
            except Exception as e:
                logger.error(f"Error calibrating {symbol}: {e}")
            
            # Short rest between symbols
            await asyncio.sleep(2.0)

    def calibrate(self, symbol: str, klines: List[List]) -> Tuple[str, dict, float, float, int]:
        # Convert klines to Pandas DataFrame
        df = pd.DataFrame(klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "count", "taker_buy_base", "taker_buy_quote", "ignore"
        ])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        best_strategy = "EMA_RSI"
        best_params = {
            "ema_fast": 9,
            "ema_slow": 21,
            "rsi_period": 14,
            "rsi_oversold": 35,
            "rsi_overbought": 65
        }
        best_pnl = -999.0
        best_win_rate = 0.0
        best_trades = 0

        # Define grid spaces
        ema_rsi_grid = []
        for fast in [9, 12]:
            for slow in [21, 26]:
                for oversold in [30, 35]:
                    for overbought in [65, 70]:
                        ema_rsi_grid.append({
                            "ema_fast": fast,
                            "ema_slow": slow,
                            "rsi_period": 14,
                            "rsi_oversold": oversold,
                            "rsi_overbought": overbought
                        })

        macd_grid = []
        for fast in [12, 15]:
            for slow in [26, 30]:
                for signal in [9]:
                    macd_grid.append({
                        "macd_fast": fast,
                        "macd_slow": slow,
                        "macd_signal": signal
                    })

        bb_grid = []
        for period in [20, 25]:
            for std_dev in [1.8, 2.0, 2.2]:
                bb_grid.append({
                    "bb_period": period,
                    "bb_std_dev": std_dev
                })

        # Sweep EMA_RSI
        for p in ema_rsi_grid:
            pnl, wr, t_count = self._backtest_ema_rsi(df, p)
            if pnl > best_pnl and t_count > 0:
                best_pnl = pnl
                best_win_rate = wr
                best_trades = t_count
                best_strategy = "EMA_RSI"
                best_params = p

        # Sweep MACD
        for p in macd_grid:
            pnl, wr, t_count = self._backtest_macd(df, p)
            if pnl > best_pnl and t_count > 0:
                best_pnl = pnl
                best_win_rate = wr
                best_trades = t_count
                best_strategy = "MACD"
                best_params = p

        # Sweep Bollinger Bands
        for p in bb_grid:
            pnl, wr, t_count = self._backtest_bb(df, p)
            if pnl > best_pnl and t_count > 0:
                best_pnl = pnl
                best_win_rate = wr
                best_trades = t_count
                best_strategy = "Bollinger Bands"
                best_params = p

        # Fallback in case no trades were triggered
        if best_pnl == -999.0:
            best_pnl = 0.0

        return best_strategy, best_params, best_pnl, best_win_rate, best_trades

    def _run_trade_simulation(self, df: pd.DataFrame, signals: pd.Series) -> Tuple[float, float, int]:
        initial_capital = 10000.0
        capital = initial_capital
        position = 0  # 0: flat, 1: long, -1: short
        entry_price = 0.0
        trades_count = 0
        winning_trades = 0
        
        prices = df['close'].values
        sig_vals = signals.values
        
        for i in range(len(prices)):
            price = prices[i]
            sig = sig_vals[i]
            
            if sig == 'BUY':
                if position == -1:  # exit short
                    pnl = (entry_price - price) / entry_price * capital
                    capital += pnl
                    trades_count += 1
                    if pnl > 0:
                        winning_trades += 1
                    position = 0
                if position == 0:  # enter long
                    entry_price = price
                    position = 1
            elif sig == 'SELL':
                if position == 1:  # exit long
                    pnl = (price - entry_price) / entry_price * capital
                    capital += pnl
                    trades_count += 1
                    if pnl > 0:
                        winning_trades += 1
                    position = 0
                if position == 0:  # enter short
                    entry_price = price
                    position = -1
                    
        # Close open position at final price
        if position != 0:
            final_price = prices[-1]
            if position == 1:
                pnl = (final_price - entry_price) / entry_price * capital
            else:
                pnl = (entry_price - final_price) / entry_price * capital
            capital += pnl
            trades_count += 1
            if pnl > 0:
                winning_trades += 1
                
        total_return_pct = (capital - initial_capital) / initial_capital * 100.0
        win_rate = (winning_trades / trades_count * 100.0) if trades_count > 0 else 0.0
        return total_return_pct, win_rate, trades_count

    def _backtest_ema_rsi(self, df: pd.DataFrame, p: dict) -> Tuple[float, float, int]:
        df = df.copy()
        df['ema_fast'] = df['close'].ewm(span=p['ema_fast'], adjust=False).mean()
        
        # Calculate RSI
        delta = df['close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(com=p['rsi_period'] - 1, adjust=False).mean()
        avg_loss = loss.ewm(com=p['rsi_period'] - 1, adjust=False).mean()
        rs = avg_gain / (avg_loss + 1e-10)
        df['rsi'] = 100 - (100 / (1 + rs))
        
        signals = pd.Series('HOLD', index=df.index)
        signals.loc[(df['rsi'] < p['rsi_oversold']) & (df['close'] > df['ema_fast'])] = 'BUY'
        signals.loc[(df['rsi'] > p['rsi_overbought']) & (df['close'] < df['ema_fast'])] = 'SELL'
        return self._run_trade_simulation(df, signals)

    def _backtest_macd(self, df: pd.DataFrame, p: dict) -> Tuple[float, float, int]:
        df = df.copy()
        fast_ema = df['close'].ewm(span=p['macd_fast'], adjust=False).mean()
        slow_ema = df['close'].ewm(span=p['macd_slow'], adjust=False).mean()
        df['macd'] = fast_ema - slow_ema
        df['signal'] = df['macd'].ewm(span=p['macd_signal'], adjust=False).mean()
        
        signals = pd.Series('HOLD', index=df.index)
        macd_prev = df['macd'].shift(1)
        sig_prev = df['signal'].shift(1)
        
        buy_cond = (df['macd'] > df['signal']) & (macd_prev <= sig_prev) & (df['macd'] < 0)
        sell_cond = (df['macd'] < df['signal']) & (macd_prev >= sig_prev) & (df['macd'] > 0)
        
        signals.loc[buy_cond] = 'BUY'
        signals.loc[sell_cond] = 'SELL'
        return self._run_trade_simulation(df, signals)

    def _backtest_bb(self, df: pd.DataFrame, p: dict) -> Tuple[float, float, int]:
        df = df.copy()
        df['sma'] = df['close'].rolling(window=p['bb_period']).mean()
        df['std'] = df['close'].rolling(window=p['bb_period']).std()
        df['upper'] = df['sma'] + p['bb_std_dev'] * df['std']
        df['lower'] = df['sma'] - p['bb_std_dev'] * df['std']
        
        signals = pd.Series('HOLD', index=df.index)
        signals.loc[df['close'] < df['lower']] = 'BUY'
        signals.loc[df['close'] > df['upper']] = 'SELL'
        return self._run_trade_simulation(df, signals)
