import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable, Optional

from config import settings

logger = logging.getLogger(__name__)


@dataclass
class BasisOpportunity:
    symbol: str
    spot_price: float
    futures_price: float
    basis_pct: float
    net_basis_pct: float
    action: str
    expected_profit_pct: float
    expected_profit_usdt: float
    confidence: float
    details: dict = field(default_factory=dict)


class BasisArbitrage:
    """Scan Binance spot/perpetual pairs for convergence arbitrage candidates."""

    def __init__(self, client):
        self.client = client
        self._running = False
        self._on_opportunity: Optional[Callable] = None

    @property
    def is_running(self) -> bool:
        return self._running

    def on_opportunity(self, cb: Callable):
        self._on_opportunity = cb

    async def start(self):
        if self._running:
            logger.info("Basis arbitrage scanner is already running")
            return

        self._running = True
        logger.info("Basis arbitrage scanner started")
        try:
            while self._running:
                ops = []
                for sym, t in list(self.client.tickers.items()):
                    op = self._evaluate(sym, t)
                    if op:
                        ops.append(op)

                ops.sort(key=lambda x: x.expected_profit_pct, reverse=True)
                for op in ops[:5]:
                    if self._on_opportunity:
                        asyncio.create_task(self._on_opportunity(op))
                await asyncio.sleep(settings.update_interval_ms / 1000)
        finally:
            self._running = False
            logger.info("Basis arbitrage scanner stopped")

    def stop(self):
        self._running = False

    def _evaluate(self, symbol: str, t) -> Optional[BasisOpportunity]:
        if not t.spot_price or not t.futures_price or t.spot_price <= 0 or t.futures_price <= 0:
            return None

        spot_ask = t.ask if t.ask > 0 else t.spot_price
        spot_bid = t.bid if t.bid > 0 else t.spot_price
        futures_price = t.futures_price

        # Round-trip spot + futures taker fees plus configurable slippage buffer.
        total_friction_pct = ((settings.taker_fee + 0.0005) * 2 + (settings.slippage_pct / 100.0)) * 100

        premium_pct = ((futures_price - spot_ask) / spot_ask) * 100
        discount_pct = ((spot_bid - futures_price) / futures_price) * 100

        if premium_pct >= discount_pct:
            action = "short_perp_long_spot"
            raw_basis = premium_pct
        else:
            action = "long_perp_short_spot"
            raw_basis = discount_pct

        net_basis = raw_basis - total_friction_pct
        expected_profit_pct = max(net_basis, 0.0)
        expected_profit_usdt = settings.max_position_size_usdt * (expected_profit_pct / 100.0)
        confidence = max(0.0, min(expected_profit_pct / max(settings.min_basis_profit_pct, 0.0001), 0.95))

        details = {
            "raw_basis_pct": round(raw_basis, 4),
            "net_basis_pct": round(net_basis, 4),
            "total_friction_pct": round(total_friction_pct, 4),
            "premium_pct": round(premium_pct, 4),
            "discount_pct": round(discount_pct, 4),
            "action": action,
            "spot_volume_24h": t.spot_volume_24h,
            "futures_volume_24h": t.futures_volume_24h,
        }

        return BasisOpportunity(
            symbol=symbol,
            spot_price=t.spot_price,
            futures_price=futures_price,
            basis_pct=raw_basis,
            net_basis_pct=net_basis,
            action=action,
            expected_profit_pct=expected_profit_pct,
            expected_profit_usdt=expected_profit_usdt,
            confidence=confidence,
            details=details,
        )
