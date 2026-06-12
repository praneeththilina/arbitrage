import asyncio
import logging
from typing import Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from config import settings

logger = logging.getLogger(__name__)

MIN_NET_PROFIT_PCT = 0.01

@dataclass
class TrianglePath:
    legs: List[Tuple[str, str, str]]
    description: str
    cross_symbol: str

@dataclass
class TriangularOpportunity:
    symbol_a: str
    symbol_b: str
    symbol_c: str
    path: str
    effective_rate: float
    profit_pct: float
    legs: List[dict]
    confidence: float
    details: dict = field(default_factory=dict)

class TriangularArbitrage:
    def __init__(self, client):
        self.client = client
        self._running = False
        self._on_opportunity: Optional[Callable] = None
        self.paths: List[TrianglePath] = []

    @property
    def is_running(self) -> bool:
        return self._running

    def on_opportunity(self, cb: Callable):
        self._on_opportunity = cb

    def resolve_dynamic_paths(self, top_usdt_symbols: List[str], exchange_info: Dict[str, Dict]):
        self.paths = []
        base_assets = []

        for sym in top_usdt_symbols:
            info = exchange_info.get(sym)
            if info and info["quote"] == "USDT":
                base_assets.append(info["base"])

        for i in range(len(base_assets)):
            for j in range(i + 1, len(base_assets)):
                base1 = base_assets[i]
                base2 = base_assets[j]

                cross_a = f"{base1}{base2}"
                cross_b = f"{base2}{base1}"

                cross = cross_a if cross_a in exchange_info else (cross_b if cross_b in exchange_info else None)

                if not cross:
                    continue

                sym1 = f"{base1}USDT"
                sym2 = f"{base2}USDT"
                p1_side, p2_side = ("SELL", "BUY") if cross == cross_a else ("BUY", "SELL")

                self.paths.extend([
                    TrianglePath(
                        legs=[(sym1, "BUY", f"BUY {base1}"), (cross, p1_side, f"{p1_side} {cross}"), (sym2, "SELL", f"SELL {base2}")],
                        description=f"{base1}/USDT -> {cross} -> {base2}/USDT",
                        cross_symbol=cross,
                    ),
                    TrianglePath(
                        legs=[(sym2, "BUY", f"BUY {base2}"), (cross, p2_side, f"{p2_side} {cross}"), (sym1, "SELL", f"SELL {base1}")],
                        description=f"{base2}/USDT -> {cross} -> {base1}/USDT",
                        cross_symbol=cross,
                    )
                ])

        logger.info(f"Resolved {len(self.paths)} dynamic triangle paths")

    async def start(self):
        if self._running:
            logger.info("Triangular arbitrage scanner is already running")
            return

        self._running = True
        logger.info("Triangular arbitrage scanner started")
        try:
            while self._running:
                ops = []
                for path in self.paths:
                    op = self._evaluate_path(path)
                    if op:
                        ops.append(op)

                # Sort by profit and broadcast the top 5 closest to profitability
                ops.sort(key=lambda x: x.profit_pct, reverse=True)
                for op in ops[:5]:
                    if self._on_opportunity:
                        asyncio.create_task(self._on_opportunity(op))

                await asyncio.sleep(settings.update_interval_ms / 1000)
        finally:
            self._running = False
            logger.info("Triangular arbitrage scanner stopped")

    def stop(self):
        self._running = False

    def _get_price(self, symbol: str, side: str) -> Optional[float]:
        t = self.client.tickers.get(symbol)
        if not t:
            return None
        return t.ask if side == "BUY" and t.ask > 0 else (t.bid if t.bid > 0 else t.spot_price)

    def _evaluate_path(self, path: TrianglePath) -> Optional[TriangularOpportunity]:
        prices = []
        for sym, side, _ in path.legs:
            p = self._get_price(sym, side)
            if not p or p <= 0:
                return None
            prices.append(p)

        rate = 1.0
        leg_details = []
        fee_multiplier = 1.0 - settings.taker_fee

        for i, (sym, side, desc) in enumerate(path.legs):
            rate = (rate / prices[i]) * fee_multiplier if side == "BUY" else (rate * prices[i]) * fee_multiplier
            leg_details.append({"symbol": sym, "side": side, "price": prices[i], "description": desc})

        profit_pct = (rate - 1.0) * 100

        # Don't hard-filter out unprofitable ones so they can be shown on the dashboard
        return TriangularOpportunity(
            symbol_a=path.legs[0][0],
            symbol_b=path.legs[1][0],
            symbol_c=path.legs[2][0],
            path=path.description,
            effective_rate=rate,
            profit_pct=profit_pct,
            legs=leg_details,
            confidence=max(0.0, min(profit_pct / 2, 0.95)),
            details={"prices": prices, "net_rate": rate},
        )
