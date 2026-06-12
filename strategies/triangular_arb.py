import asyncio
import logging
from typing import Dict, List, Optional, Callable, Tuple, Set
from dataclasses import dataclass, field

from config import settings

logger = logging.getLogger(__name__)

TRIANGLE_TEMPLATES = [
    ("USDT", "BTC", "ETH"),
    ("USDT", "BTC", "BNB"),
    ("USDT", "BTC", "SOL"),
    ("USDT", "BTC", "XRP"),
    ("USDT", "BTC", "ADA"),
    ("USDT", "BTC", "DOGE"),
    ("USDT", "BTC", "DOT"),
    ("USDT", "ETH", "BNB"),
    ("USDT", "ETH", "SOL"),
    ("USDT", "ETH", "XRP"),
    ("USDT", "BNB", "SOL"),
]

MIN_TRIANGULAR_SPREAD_PCT = 0.01


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

    def on_opportunity(self, cb: Callable):
        self._on_opportunity = cb

    def resolve_paths(self, spot_symbols: Set[str]):
        self.paths = []
        for quote, base1, base2 in TRIANGLE_TEMPLATES:
            sym1 = f"{base1}{quote}"
            sym2 = f"{base2}{quote}"

            if sym1 not in spot_symbols or sym2 not in spot_symbols:
                continue

            cross_a = f"{base1}{base2}"
            cross_b = f"{base2}{base1}"
            cross = cross_a if cross_a in spot_symbols else (cross_b if cross_b in spot_symbols else None)
            if not cross:
                logger.debug(f"No cross pair for {base1}/{base2}, skipping")
                continue

            if cross == cross_a:
                p1_side = "SELL"
                p2_side = "BUY"
            else:
                p1_side = "BUY"
                p2_side = "SELL"

            self.paths.append(TrianglePath(
                legs=[
                    (sym1, "BUY", f"BUY {base1} with {quote}"),
                    (cross, p1_side, f"{p1_side} {cross}"),
                    (sym2, "SELL", f"SELL {base2} for {quote}"),
                ],
                description=f"{base1}/{quote} -> {cross} -> {base2}/{quote}",
                cross_symbol=cross,
            ))
            self.paths.append(TrianglePath(
                legs=[
                    (sym2, "BUY", f"BUY {base2} with {quote}"),
                    (cross, p2_side, f"{p2_side} {cross}"),
                    (sym1, "SELL", f"SELL {base1} for {quote}"),
                ],
                description=f"{base2}/{quote} -> {cross} -> {base1}/{quote}",
                cross_symbol=cross,
            ))

        logger.info(f"Resolved {len(self.paths)} triangle paths from {len(TRIANGLE_TEMPLATES)} templates")

    async def start(self):
        self._running = True
        while self._running:
            for path in self.paths:
                op = self._evaluate_path(path)
                if op:
                    if self._on_opportunity:
                        asyncio.create_task(self._on_opportunity(op))
            await asyncio.sleep(settings.update_interval_ms / 1000)

    def stop(self):
        self._running = False

    def _get_price(self, symbol: str, side: str) -> Optional[float]:
        t = self.client.tickers.get(symbol)
        if not t:
            return None
        if side == "BUY":
            return t.ask if t.ask > 0 else t.spot_price
        return t.bid if t.bid > 0 else t.spot_price

    def _evaluate_path(self, path: TrianglePath) -> Optional[TriangularOpportunity]:
        prices = []
        for sym, side, _ in path.legs:
            p = self._get_price(sym, side)
            if not p or p <= 0:
                return None
            prices.append(p)

        rate = 1.0
        leg_details = []
        for i, (sym, side, desc) in enumerate(path.legs):
            if side == "BUY":
                rate /= prices[i]
            else:
                rate *= prices[i]
            leg_details.append({
                "symbol": sym,
                "side": side,
                "price": prices[i],
                "description": desc,
            })

        profit_pct = (rate - 1.0) * 100

        if profit_pct <= MIN_TRIANGULAR_SPREAD_PCT:
            return None

        confidence = min(profit_pct / 10, 0.95)

        return TriangularOpportunity(
            symbol_a=path.legs[0][0],
            symbol_b=path.legs[1][0],
            symbol_c=path.legs[2][0],
            path=path.description,
            effective_rate=rate,
            profit_pct=profit_pct,
            legs=leg_details,
            confidence=confidence,
            details={
                "prices": prices,
                "rate": rate,
            },
        )
