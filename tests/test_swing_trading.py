import asyncio
from strategies.swing_trading import SwingTrading

class DummyTicker:
    def __init__(self, price):
        self.spot_price = price

class DummyClient:
    def __init__(self):
        self.tickers = {}

    async def get_historical_klines(self, symbol, interval, limit):
        return []

def test_ema_calculation():
    client = DummyClient()
    swing = SwingTrading(client)
    prices = [100.0] * 20
    ema = swing._calculate_ema(prices, 9)
    assert abs(ema - 100.0) < 0.01

def test_rsi_calculation():
    client = DummyClient()
    swing = SwingTrading(client)
    
    # constant price series -> RSI = 50.0
    prices = [100.0] * 15
    rsi = swing._calculate_rsi(prices, 14)
    assert rsi == 50.0

    # uptrend -> RSI should be high
    prices = [100.0 + i for i in range(20)]
    rsi = swing._calculate_rsi(prices, 14)
    assert rsi > 70.0

    # downtrend -> RSI should be low
    prices = [100.0 - i for i in range(20)]
    rsi = swing._calculate_rsi(prices, 14)
    assert rsi < 30.0

async def test_swing_trading_signals():
    client = DummyClient()
    swing = SwingTrading(client)
    
    signals = []
    async def capture_signal(data):
        signals.append(data)
        
    swing.on_opportunity(capture_signal)
    
    client.tickers["BTCUSDT"] = DummyTicker(100.0)
    task = asyncio.create_task(swing.start())
    await asyncio.sleep(0.1)
    
    assert len(signals) > 0
    assert signals[-1]["signal"] == "HOLD"
    
    swing.stop()
    try:
        await asyncio.wait_for(task, timeout=1.0)
    except Exception:
        pass
