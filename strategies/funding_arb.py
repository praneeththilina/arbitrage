import asyncio
import math
from typing import Dict, Optional, Callable, Awaitable
from dataclasses import dataclass, field

from config import settings
from exchange.client import BinanceClient, TickerData


@dataclass
class FundingOpportunity:
    symbol: str
    funding_rate: float
    basis_pct: float
    spot_price: float
    futures_price: float
    action: str
    expected_apr: float
    confidence: float
    details: dict = field(default_factory=dict)


class FundingArbitrage:
    def __init__(self, client: BinanceClient):
        self.client = client
        self._running = False
        self._on_opportunity: Optional[Callable] = None

    def on_opportunity(self, cb: Callable):
        self._on_opportunity = cb

    async def start(self):
        self._running = True
        while self._running:
            for sym, t in list(self.client.tickers.items()):
                op = self._evaluate(sym, t)
                if op:
                    if self._on_opportunity:
                        asyncio.create_task(self._on_opportunity(op))
            await asyncio.sleep(settings.update_interval_ms / 1000)

    def stop(self):
        self._running = False

    def _evaluate(self, symbol: str, t: TickerData) -> Optional[FundingOpportunity]:
        if t.spot_price <= 0 or t.futures_price <= 0:
            return None

        basis = ((t.futures_price - t.spot_price) / t.spot_price) * 100
        fr = t.funding_rate
        abs_fr = abs(fr)

        if abs_fr < settings.min_funding_rate_abs:
            return None

        if abs(basis) < settings.min_spread_pct:
            return None

        if fr > 0:
            action = "short_perp_long_spot"
        else:
            action = "long_perp_short_spot"

        funding_per_day = abs(fr) * 3
        expected_apr = funding_per_day * 365 * 100

        funding_pos = "longs_pay" if fr > 0 else "shorts_pay"
        confidence = min(abs_fr * 500, 0.95)

        details = {
            "funding_rate": fr,
            "funding_positions": funding_pos,
            "basis_pct": round(basis, 4),
            "action": action,
            "next_funding_time": t.next_funding_time,
            "spot_volume_24h": t.spot_volume_24h,
        }

        return FundingOpportunity(
            symbol=symbol,
            funding_rate=fr,
            basis_pct=basis,
            spot_price=t.spot_price,
            futures_price=t.futures_price,
            action=action,
            expected_apr=expected_apr,
            confidence=confidence,
            details=details,
        )
