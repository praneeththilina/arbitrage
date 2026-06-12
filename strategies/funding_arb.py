import asyncio
import logging
from typing import Optional, Callable
from dataclasses import dataclass, field

from config import settings

logger = logging.getLogger(__name__)

@dataclass
class FundingOpportunity:
    symbol: str
    funding_rate: float
    basis_pct: float
    net_basis_pct: float
    spot_price: float
    futures_price: float
    action: str
    expected_apr: float
    confidence: float
    details: dict = field(default_factory=dict)

class FundingArbitrage:
    def __init__(self, client):
        self.client = client
        self._running = False
        self._on_opportunity: Optional[Callable] = None

    def on_opportunity(self, cb: Callable):
        self._on_opportunity = cb

    async def start(self):
        self._running = True
        while self._running:
            ops = []
            for sym, t in list(self.client.tickers.items()):
                op = self._evaluate(sym, t)
                if op:
                    ops.append(op)
            
            ops.sort(key=lambda x: x.expected_apr, reverse=True)
            for op in ops[:5]:
                if self._on_opportunity:
                    asyncio.create_task(self._on_opportunity(op))
            await asyncio.sleep(settings.update_interval_ms / 1000)

    def stop(self):
        self._running = False

    def _evaluate(self, symbol: str, t) -> Optional[FundingOpportunity]:
        if not t.spot_price or not t.futures_price or t.spot_price <= 0 or t.futures_price <= 0:
            return None

        fr = t.funding_rate
        abs_fr = abs(fr)

        # Removed funding rate filter for visualization

        spot_ask = t.ask if t.ask > 0 else t.spot_price
        spot_bid = t.bid if t.bid > 0 else t.spot_price
        
        futures_fee_rate = 0.0005
        spot_fee_rate = settings.taker_fee
        total_friction_pct = (spot_fee_rate + futures_fee_rate) * 200

        if fr > 0:
            action = "short_perp_long_spot"
            raw_basis = ((t.futures_price - spot_ask) / spot_ask) * 100
            net_basis = raw_basis - total_friction_pct
        else:
            action = "long_perp_short_spot"
            raw_basis = ((spot_bid - t.futures_price) / t.futures_price) * 100
            net_basis = raw_basis - total_friction_pct

        # Removed hard filters to allow visualization of top opportunities
        
        funding_per_day = abs_fr * 3
        expected_apr = funding_per_day * 365 * 100

        funding_pos = "longs_pay" if fr > 0 else "shorts_pay"
        confidence = max(0.0, min(abs_fr * 500, 0.95))

        details = {
            "funding_rate": fr,
            "funding_positions": funding_pos,
            "raw_basis_pct": round(raw_basis, 4),
            "net_basis_pct": round(net_basis, 4),
            "action": action,
            "next_funding_time": t.next_funding_time,
            "spot_volume_24h": t.spot_volume_24h,
        }

        return FundingOpportunity(
            symbol=symbol,
            funding_rate=fr,
            basis_pct=raw_basis,
            net_basis_pct=net_basis,
            spot_price=t.spot_price,
            futures_price=t.futures_price,
            action=action,
            expected_apr=expected_apr,
            confidence=confidence,
            details=details,
        )