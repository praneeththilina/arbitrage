import pandas as pd
import numpy as np
from strategies.swing_calibration import SwingCalibrator

class DummyClient:
    pass

def test_trade_simulation():
    calibrator = SwingCalibrator(DummyClient())
    
    df = pd.DataFrame({
        "close": [100.0, 110.0, 120.0, 110.0, 100.0]
    })
    signals = pd.Series(["BUY", "HOLD", "HOLD", "SELL", "HOLD"])
    
    pnl, win_rate, trades = calibrator._run_trade_simulation(df, signals)
    assert trades == 2
    assert win_rate == 100.0
    assert pnl > 0.0

def test_calibration_on_mock_klines():
    calibrator = SwingCalibrator(DummyClient())
    
    klines = []
    base_time = 1700000000000
    for i in range(100):
        close_price = 100.0 + i
        kline = [
            base_time + i * 3600000,
            str(close_price - 0.5),
            str(close_price + 1.0),
            str(close_price - 1.0),
            str(close_price),
            "1000",
            base_time + i * 3600000 + 3599999,
            "100000",
            100,
            "500",
            "50000",
            "0"
        ]
        klines.append(kline)
        
    best_strat, best_params, pnl, win_rate, trades = calibrator.calibrate("BTCUSDT", klines)
    assert best_strat in ["EMA_RSI", "MACD", "Bollinger Bands"]
    assert trades >= 0
    assert win_rate >= 0
