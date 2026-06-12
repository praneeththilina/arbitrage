import aiohttp
import asyncio
import json

async def test_rest():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT") as resp:
                data = await resp.json()
                print("REST Status:", resp.status)
                print("REST Data:", data)
    except Exception as e:
        print("REST Error:", e)

asyncio.run(test_rest())
