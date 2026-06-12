import asyncio
import itertools
import math
from typing import Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field

from config import settings
from exchange.client import BinanceClient


TRIANGLE_TEMPLATES = [
    ("USDT", "BTC", "ETH"),
    ("USDT", "BTC", "BNB"),
    ("USDT", "BTC", "SOL"),
    ("USDT", "BTC", "XRP"),
    ("USDT", "BTC", "ADA"),
    ("USDT", "BTC", "DOGE"),
    ("USDT", "BTC", "DOT"),
    ("USDT", "BTC", "MATIC"),
    ("USDT", "ETH", "BNB"),
    ("USDT", "ETH", "SOL"),
    ("USDT", "ETH", "XRP"),
    ("USDT", "BNB", "SOL"),
    ("USDT", "SOL", "XRP"),
]


@dataclass
class TrianglePath:
    legs: List[Tuple[str, str, str]]
    description: str


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


def build_triangle_paths(templates: List[Tuple[str, str, str]]) -> List[TrianglePath]:
    paths = []
    for quote, base1, base2 in templates:
        sym1 = f"{base1}{quote}"
        sym2 = f"{base2}{quote}"
        cross = f"{base2}{base1}" if f"{base2}{base1}" < f"{base1}{base2}" else f"{base1}{base2}"
        cross_inv = f"{base1}{base2}"

        pair12 = (sym1, sym2)
        paths.append(TrianglePath(
            legs=[
                (sym1, "BUY", f"BUY {base1} with {quote}"),
                (cross, "BUY" if cross.endswith(base2) else "SELL", f"BUY {base2} with {base1}"),
                (sym2, "SELL", f"SELL {base2} for {quote}"),
            ],
            description=f"{base1}/{quote} -> {base2}/{base1} -> {base2}/{quote}"
        ))
        paths.append(TrianglePath(
            legs=[
                (sym2, "BUY", f"BUY {base2} with {quote}"),
                (cross_inv, "BUY" if cross_inv.endswith(base1) else "SELL", f"BUY {base1} with {base2}"),
                (sym1, "SELL", f"SELL {base1} for {quote}"),
            ],
            description=f"{base2}/{quote} -> {base1}/{base2} -> {base1}/{quote}"
        ))
    return paths


class TriangularArbitrage:
    def __init__(self, client: BinanceClient):
        self.client = client
        self._running = False
        self._on_opportunity: Optional[Callable] = None
        self.paths = build_triangle_paths(TRIANGLE_TEMPLATES)

    def on_opportunity(self, cb: Callable):
        self._on_opportunity = cb

    async def start(self):
        self._running = True
        while self._running:
            for path in self.paths:
                op = self._evaluate_path(path)
                if op:
                    if self._on_opportunity:
                        self._on_opportunity(op)
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

        if profit_pct <= settings.min_spread_pct:
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
