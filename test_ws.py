import asyncio
import websockets
import json

async def test_futures_auth():
    url = "wss://fstream-auth.binance.com/stream?streams=btcusdt@markPrice"
    try:
        async with websockets.connect(url) as ws:
            print("Auth Connected!")
            msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
            print("Auth Msg:", msg[:100])
    except Exception as e:
        print("Auth Error:", e)

asyncio.run(test_futures_auth())
