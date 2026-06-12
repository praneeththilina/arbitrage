import asyncio
import time
import math
from typing import Dict, Optional, Callable, Any
from dataclasses import dataclass, field

from config import settings
from database import save_opportunity, save_order, update_order, update_balance


@dataclass
class PaperPosition:
    symbol: str
    side: str
    entry_price: float
    quantity: float
    notional: float
    opened_at: float
    pnl: float = 0.0
    fees_paid: float = 0.0


class PaperEngine:
    def __init__(self):
        self.balance_usdt: float = settings.paper_initial_balance
        self.positions: Dict[str, PaperPosition] = {}
        self.trade_history: list = []
        self._running = False
        self._auto_trade = settings.auto_trade_enabled
        self._trade_size_pct = settings.trade_size_pct
        self._on_trade: Optional[Callable] = None
        self._cooldowns: Dict[str, float] = {}

    @property
    def auto_trade(self) -> bool:
        return self._auto_trade

    @auto_trade.setter
    def auto_trade(self, val: bool):
        self._auto_trade = val

    @property
    def trade_size_pct(self) -> float:
        return self._trade_size_pct

    @trade_size_pct.setter
    def trade_size_pct(self, val: float):
        self._trade_size_pct = max(0.5, min(100.0, val))

    def on_trade(self, cb: Callable):
        self._on_trade = cb

    async def start(self):
        self._running = True
        update_balance("USDT", self.balance_usdt, 0, self.balance_usdt)

    def stop(self):
        self._running = False

    def _calculate_fee(self, notional: float, is_maker: bool = False) -> float:
        fee_rate = settings.maker_fee if is_maker else settings.taker_fee
        return notional * fee_rate

    def _get_position_size(self) -> float:
        return self.balance_usdt * (self._trade_size_pct / 100.0)

    def _is_on_cooldown(self, key: str) -> bool:
        if key in self._cooldowns:
            if time.time() - self._cooldowns[key] < settings.cooldown_seconds:
                return True
        return False

    def _set_cooldown(self, key: str):
        self._cooldowns[key] = time.time()

    def validate_funding_op(self, op) -> Dict:
        result = {"valid": False, "reason": "", "net_profit": 0.0, "pos_size": 0.0}

        if not op.spot_price or not op.futures_price or op.spot_price <= 0:
            result["reason"] = "invalid prices"
            return result

        pos_size = self._get_position_size()
        if pos_size <= 0:
            result["reason"] = "zero balance"
            return result

        pos_size = min(pos_size, settings.max_position_size_usdt)

        entry_fee = self._calculate_fee(pos_size) * 2
        daily_funding_income = abs(op.funding_rate) * pos_size * 3
        exit_fee = self._calculate_fee(pos_size * (1 + 0.001)) * 2

        expected_income = daily_funding_income
        total_costs = entry_fee + exit_fee
        net_profit = expected_income - total_costs

        if net_profit < settings.min_net_profit_usdt:
            result["reason"] = f"net profit ${net_profit:.2f} < min ${settings.min_net_profit_usdt:.2f}"
            return result

        if len(self.positions) >= settings.max_concurrent_positions:
            result["reason"] = "max concurrent positions reached"
            return result

        cooldown_key = f"funding_{op.symbol}"
        if self._is_on_cooldown(cooldown_key):
            result["reason"] = "on cooldown"
            return result

        result["valid"] = True
        result["net_profit"] = net_profit
        result["pos_size"] = pos_size
        result["entry_fee"] = entry_fee
        result["exit_fee"] = exit_fee
        return result

    def validate_triangular_op(self, op) -> Dict:
        result = {"valid": False, "reason": "", "net_profit": 0.0, "pos_size": 0.0}

        pos_size = self._get_position_size() * 0.5
        if pos_size <= 0:
            result["reason"] = "zero balance"
            return result

        pos_size = min(pos_size, settings.max_position_size_usdt * 0.5)

        total_fees = 0
        for leg in op.legs:
            notional = pos_size if leg == op.legs[0] else pos_size * 0.5
            total_fees += self._calculate_fee(notional)

        net_profit_pct = op.profit_pct - (total_fees / pos_size * 100)
        net_profit_usdt = pos_size * (net_profit_pct / 100)

        if net_profit_usdt < settings.min_net_profit_usdt:
            result["reason"] = f"net profit ${net_profit_usdt:.2f} < min ${settings.min_net_profit_usdt:.2f}"
            return result

        if len(self.positions) >= settings.max_concurrent_positions:
            result["reason"] = "max concurrent positions"
            return result

        path_key = f"tri_{op.symbol_a}_{op.symbol_b}_{op.symbol_c}"
        if self._is_on_cooldown(path_key):
            result["reason"] = "on cooldown"
            return result

        result["valid"] = True
        result["net_profit"] = net_profit_usdt
        result["pos_size"] = pos_size
        result["total_fees"] = total_fees
        return result

    async def evaluate_and_execute(self, op_type: str, op) -> Optional[Dict]:
        if not self._auto_trade:
            return None

        if not self._running:
            return None

        if op_type == "funding":
            validation = self.validate_funding_op(op)
            if not validation["valid"]:
                return None
            self._set_cooldown(f"funding_{op.symbol}")
            return await self._execute_funding(op, validation)
        elif op_type == "triangular":
            validation = self.validate_triangular_op(op)
            if not validation["valid"]:
                return None
            self._set_cooldown(f"tri_{op.symbol_a}_{op.symbol_b}_{op.symbol_c}")
            return await self._execute_triangular(op, validation)

        return None

    async def _execute_funding(self, op, validation: Dict) -> Dict:
        pos_size = validation["pos_size"]
        entry_fee = validation["entry_fee"]

        op_id = save_opportunity(
            "funding", op.symbol, op.details,
            op.expected_apr / 100, validation["net_profit"],
            op.confidence
        )

        spot_qty = pos_size / op.spot_price if op.spot_price > 0 else 0
        futures_qty = pos_size / op.futures_price if op.futures_price > 0 else 0

        orders = []
        spot_side = "SELL" if "short" in op.action else "BUY"
        futures_side = "BUY" if "long_perp" in op.action else "SELL"

        spot_order_id = save_order(op_id, "funding_arb", op.symbol, spot_side, op.spot_price, spot_qty)
        orders.append({"id": spot_order_id, "symbol": op.symbol, "side": spot_side, "price": op.spot_price, "qty": spot_qty})

        fut_order_id = save_order(op_id, "funding_arb", f"{op.symbol}_PERP", futures_side, op.futures_price, futures_qty)
        orders.append({"id": fut_order_id, "symbol": f"{op.symbol}_PERP", "side": futures_side, "price": op.futures_price, "qty": futures_qty})

        self.balance_usdt -= entry_fee

        for o in orders:
            update_order(o["id"], "filled", time.strftime("%Y-%m-%dT%H:%M:%SZ"), entry_fee / 2, 0)

        pos_key = f"{op.symbol}_funding"
        self.positions[pos_key] = PaperPosition(
            symbol=op.symbol,
            side=op.action,
            entry_price=op.spot_price,
            quantity=spot_qty,
            notional=pos_size,
            opened_at=time.time(),
            fees_paid=entry_fee,
        )

        update_balance("USDT", self.balance_usdt, 0, self.balance_usdt)

        result = {
            "type": "funding",
            "symbol": op.symbol,
            "orders": orders,
            "notional": round(pos_size, 2),
            "fee": round(entry_fee, 2),
            "net_profit_expected": round(validation["net_profit"], 2),
            "balance_remaining": round(self.balance_usdt, 2),
            "action": op.action,
            "funding_rate": op.funding_rate,
            "basis_pct": round(op.basis_pct, 4),
        }

        if self._on_trade:
            self._on_trade(result)
        return result

    async def _execute_triangular(self, op, validation: Dict) -> Dict:
        pos_size = validation["pos_size"]
        total_fees = validation["total_fees"]

        op_id = save_opportunity(
            "triangular", f"{op.symbol_a}-{op.symbol_b}-{op.symbol_c}",
            op.details, op.profit_pct / 100, validation["net_profit"],
            op.confidence
        )

        orders = []
        current_notional = pos_size

        for leg in op.legs:
            qty = current_notional / leg["price"] if leg["price"] > 0 else 0
            oid = save_order(op_id, "triangular_arb", leg["symbol"], leg["side"], leg["price"], qty)
            update_order(oid, "filled", time.strftime("%Y-%m-%dT%H:%M:%SZ"), total_fees / len(op.legs), 0)
            orders.append({"id": oid, "symbol": leg["symbol"], "side": leg["side"], "price": leg["price"], "qty": qty})
            if leg["side"] == "BUY":
                current_notional = qty
            else:
                current_notional = qty * leg["price"]

        self.balance_usdt += validation["net_profit"]

        update_balance("USDT", self.balance_usdt, 0, self.balance_usdt)

        result = {
            "type": "triangular",
            "path": op.path,
            "orders": orders,
            "notional": round(pos_size, 2),
            "fee": round(total_fees, 2),
            "net_profit_expected": round(validation["net_profit"], 2),
            "balance_remaining": round(self.balance_usdt, 2),
            "profit_pct": round(op.profit_pct, 4),
        }

        if self._on_trade:
            self._on_trade(result)
        return result

    async def get_status(self) -> Dict:
        total_pnl = sum(p.pnl for p in self.positions.values())
        total_fees = sum(p.fees_paid for p in self.positions.values())
        total_return = self.balance_usdt - settings.paper_initial_balance
        return_pct = (total_return / settings.paper_initial_balance * 100) if settings.paper_initial_balance > 0 else 0
        return {
            "balance_usdt": round(self.balance_usdt, 2),
            "total_pnl": round(total_pnl, 2),
            "total_fees": round(total_fees, 2),
            "total_return": round(total_return, 2),
            "open_positions": len(self.positions),
            "initial_balance": settings.paper_initial_balance,
            "return_pct": round(return_pct, 2),
            "auto_trade": self._auto_trade,
            "trade_size_pct": self._trade_size_pct,
        }

    async def get_config(self) -> Dict:
        return {
            "auto_trade": self._auto_trade,
            "trade_size_pct": self._trade_size_pct,
            "min_net_profit_usdt": settings.min_net_profit_usdt,
            "max_concurrent_positions": settings.max_concurrent_positions,
            "cooldown_seconds": settings.cooldown_seconds,
        }
