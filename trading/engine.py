import asyncio
import time
import uuid
import logging
from typing import Dict, Optional, Callable, List
from dataclasses import dataclass

from config import settings
from database import save_opportunity, save_order, update_order, update_balance

logger = logging.getLogger(__name__)

@dataclass
class PaperPosition:
    id: str
    symbol: str
    strategy: str
    side: str
    entry_spot_price: float
    entry_futures_price: float
    quantity_spot: float
    quantity_futures: float
    notional: float
    opened_at: float
    fees_paid: float
    funding_received: float = 0.0
    entry_funding_rate: float = 0.0
    next_funding_time: float = 0.0
    is_short_perp: bool = True
    status: str = "open"

class PaperEngine:
    def __init__(self):
        self.balance_usdt: float = settings.paper_initial_balance
        self.locked_usdt: float = 0.0
        self.positions: Dict[str, PaperPosition] = {}
        self.closed_trades: list = []
        self._running = False
        self._auto_trade = settings.auto_trade_enabled
        self._trade_size_pct = settings.trade_size_pct
        self._on_trade: Optional[Callable] = None
        self._cooldowns: Dict[str, float] = {}
        self._client = None

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

    def set_client(self, client):
        self._client = client

    async def start(self):
        self._running = True
        await update_balance("USDT", self.balance_usdt, 0, self.balance_usdt)
        asyncio.create_task(self._settlement_loop())

    def stop(self):
        self._running = False

    def _calculate_spot_fee(self, notional: float) -> float:
        return notional * settings.taker_fee

    def _calculate_futures_fee(self, notional: float) -> float:
        return notional * 0.0005

    def _get_position_size(self) -> float:
        return (self.balance_usdt + self.locked_usdt) * (self._trade_size_pct / 100.0)

    def _available_balance(self) -> float:
        return self.balance_usdt

    def _is_on_cooldown(self, key: str) -> bool:
        if key in self._cooldowns:
            if time.time() - self._cooldowns[key] < settings.cooldown_seconds:
                return True
        return False

    def _set_cooldown(self, key: str):
        self._cooldowns[key] = time.time()

    async def _settlement_loop(self):
        while self._running:
            now = time.time()
            for pos in list(self.positions.values()):
                if pos.strategy != "funding" or pos.status != "open":
                    continue
                if pos.next_funding_time > 0 and now >= (pos.next_funding_time / 1000):
                    await self._process_funding_settlement(pos)
            await asyncio.sleep(10)

    async def _process_funding_settlement(self, pos: PaperPosition):
        if not self._client:
            return
        t = self._client.tickers.get(pos.symbol)
        if not t:
            return

        fr = t.funding_rate
        if fr == 0:
            pos.next_funding_time = t.next_funding_time
            return

        payment = fr * pos.notional if pos.is_short_perp else -fr * pos.notional

        self.balance_usdt += payment
        pos.funding_received += payment
        pos.next_funding_time = t.next_funding_time

        await update_balance("USDT", self.balance_usdt, 0, self.balance_usdt + self.locked_usdt)

        if self._on_trade:
            await self._on_trade({
                "event": "funding_payment",
                "symbol": pos.symbol,
                "amount": round(payment, 2),
                "funding_rate": fr,
                "total_received": round(pos.funding_received, 2),
                "balance_remaining": round(self.balance_usdt, 2),
                "trade_time": time.strftime("%H:%M:%S"),
            })

    def validate_funding_op(self, op) -> Dict:
        result = {"valid": False, "reason": "", "net_profit": 0.0, "pos_size": 0.0}

        if not op.spot_price or not op.futures_price or op.spot_price <= 0:
            result["reason"] = "invalid prices"
            return result

        pos_size = self._get_position_size()
        pos_size = min(pos_size, settings.max_position_size_usdt)

        if pos_size > self._available_balance() or pos_size <= 0:
            result["reason"] = "insufficient balance"
            return result

        entry_fee = self._calculate_spot_fee(pos_size) + self._calculate_futures_fee(pos_size)
        exit_fee = self._calculate_spot_fee(pos_size) + self._calculate_futures_fee(pos_size)
        
        per_settlement_income = abs(op.funding_rate) * pos_size
        basis_value = (op.net_basis_pct / 100.0) * pos_size
        total_costs = entry_fee + exit_fee - basis_value

        settlements_to_breakeven = total_costs / per_settlement_income if per_settlement_income > 0 else 999

        if settlements_to_breakeven > 5:
            result["reason"] = f"breakeven requires {settlements_to_breakeven:.1f} intervals"
            return result

        if len(self.positions) >= settings.max_concurrent_positions:
            result["reason"] = "max concurrent positions reached"
            return result

        if any(p.symbol == op.symbol and p.status == "open" for p in self.positions.values()):
            result["reason"] = "position active"
            return result

        if self._is_on_cooldown(f"funding_{op.symbol}"):
            result["reason"] = "on cooldown"
            return result

        result["valid"] = True
        result["pos_size"] = pos_size
        result["entry_fee"] = entry_fee
        result["exit_fee"] = exit_fee
        return result

    def validate_basis_op(self, op) -> Dict:
        result = {"valid": False, "reason": "", "net_profit": 0.0, "pos_size": 0.0}

        if not op.spot_price or not op.futures_price or op.spot_price <= 0 or op.futures_price <= 0:
            result["reason"] = "invalid prices"
            return result

        pos_size = min(self._get_position_size(), settings.max_position_size_usdt)
        if pos_size > self._available_balance() or pos_size <= 0:
            result["reason"] = "insufficient balance"
            return result

        net_profit_usdt = pos_size * (op.expected_profit_pct / 100.0)
        if net_profit_usdt < settings.min_net_profit_usdt:
            result["reason"] = "below minimum net profit"
            return result

        if op.net_basis_pct < settings.min_basis_profit_pct:
            result["reason"] = "basis below threshold"
            return result

        if len(self.positions) >= settings.max_concurrent_positions:
            result["reason"] = "max concurrent positions reached"
            return result

        if any(p.symbol == op.symbol and p.status == "open" for p in self.positions.values()):
            result["reason"] = "position active"
            return result

        if self._is_on_cooldown(f"basis_{op.symbol}"):
            result["reason"] = "on cooldown"
            return result

        result["valid"] = True
        result["net_profit"] = net_profit_usdt
        result["pos_size"] = pos_size
        result["entry_fee"] = self._calculate_spot_fee(pos_size) + self._calculate_futures_fee(pos_size)
        return result

    def validate_triangular_op(self, op) -> Dict:
        result = {"valid": False, "reason": "", "net_profit": 0.0, "pos_size": 0.0}

        pos_size = min(self._get_position_size(), settings.max_position_size_usdt) * 0.5
        if pos_size <= 0:
            result["reason"] = "zero balance"
            return result

        net_profit_usdt = pos_size * (op.profit_pct / 100.0)

        if net_profit_usdt < 0.01 or self._is_on_cooldown(f"tri_{op.symbol_a}_{op.symbol_b}_{op.symbol_c}"):
            result["reason"] = "invalid net profit or cooldown"
            return result

        total_fees = sum([self._calculate_spot_fee(pos_size) for _ in op.legs])

        result["valid"] = True
        result["net_profit"] = net_profit_usdt
        result["pos_size"] = pos_size
        result["total_fees"] = total_fees
        return result

    async def evaluate_and_execute(self, op_type: str, op) -> Optional[Dict]:
        if not self._auto_trade or not self._running:
            return None

        if op_type == "funding":
            validation = self.validate_funding_op(op)
            if validation["valid"]:
                self._set_cooldown(f"funding_{op.symbol}")
                return await self._open_funding_position(op, validation)

        elif op_type == "basis":
            validation = self.validate_basis_op(op)
            if validation["valid"]:
                self._set_cooldown(f"basis_{op.symbol}")
                return await self._open_basis_position(op, validation)

        elif op_type == "triangular":
            validation = self.validate_triangular_op(op)
            if validation["valid"]:
                self._set_cooldown(f"tri_{op.symbol_a}_{op.symbol_b}_{op.symbol_c}")
                return await self._execute_triangular(op, validation)

        return None

    async def _open_funding_position(self, op, validation: Dict) -> Dict:
        pos_size = validation["pos_size"]
        entry_fee = validation["entry_fee"]
        is_short = "short" in op.action

        op_id = await save_opportunity("funding", op.symbol, op.details, op.expected_apr / 100, 0, op.confidence)

        spot_qty = pos_size / op.spot_price if op.spot_price > 0 else 0
        futures_qty = pos_size / op.futures_price if op.futures_price > 0 else 0

        spot_side = "BUY" if is_short else "SELL"
        futures_side = "SELL" if is_short else "BUY"

        spot_order_id = await save_order(op_id, "funding_arb", op.symbol, spot_side, op.spot_price, spot_qty)
        fut_order_id = await save_order(op_id, "funding_arb", f"{op.symbol}_PERP", futures_side, op.futures_price, futures_qty)

        orders = [
            {"id": spot_order_id, "symbol": op.symbol, "side": spot_side, "price": op.spot_price, "qty": spot_qty},
            {"id": fut_order_id, "symbol": f"{op.symbol}_PERP", "side": futures_side, "price": op.futures_price, "qty": futures_qty}
        ]

        self.balance_usdt -= pos_size
        self.locked_usdt += pos_size

        for o in orders:
            await update_order(o["id"], "filled", time.strftime("%Y-%m-%dT%H:%M:%SZ"), entry_fee / 2, 0)

        pos_id = f"{op.symbol}_funding_{uuid.uuid4().hex[:8]}"
        self.positions[pos_id] = PaperPosition(
            id=pos_id,
            symbol=op.symbol,
            strategy="funding",
            side=op.action,
            entry_spot_price=op.spot_price,
            entry_futures_price=op.futures_price,
            quantity_spot=spot_qty,
            quantity_futures=futures_qty,
            notional=pos_size,
            opened_at=time.time(),
            fees_paid=entry_fee,
            entry_funding_rate=op.funding_rate,
            next_funding_time=op.details.get("next_funding_time", 0),
            is_short_perp=is_short,
        )

        await update_balance("USDT", self.balance_usdt, 0, self.balance_usdt + self.locked_usdt)

        result = {
            "event": "position_opened",
            "type": "funding",
            "symbol": op.symbol,
            "orders": orders,
            "notional": round(pos_size, 2),
            "fee": round(entry_fee, 2),
            "balance_remaining": round(self.balance_usdt, 2),
            "locked": round(self.locked_usdt, 2),
            "action": op.action,
            "funding_rate": op.funding_rate,
            "spot_price": op.spot_price,
            "futures_price": op.futures_price,
            "expected_apr": round(op.expected_apr, 2),
            "next_funding_time": op.details.get("next_funding_time", 0),
            "position_id": pos_id,
            "trade_time": time.strftime("%H:%M:%S"),
        }

        if self._on_trade:
            await self._on_trade(result)
        return result

    async def close_position(self, position_id: str, client=None) -> Optional[Dict]:
        pos = self.positions.get(position_id)
        if not pos or pos.status != "open":
            return None

        spot_price = pos.entry_spot_price
        futures_price = pos.entry_futures_price
        
        if client:
            t = client.tickers.get(pos.symbol)
            if t:
                spot_price = t.spot_price or spot_price
                futures_price = t.futures_price or futures_price

        short_pnl = ((pos.entry_futures_price - futures_price) / pos.entry_futures_price) * pos.notional if pos.entry_futures_price > 0 else 0
        long_pnl = ((spot_price - pos.entry_spot_price) / pos.entry_spot_price) * pos.notional if pos.entry_spot_price > 0 else 0

        price_pnl = short_pnl + long_pnl if pos.is_short_perp else -short_pnl - long_pnl

        exit_fee = self._calculate_spot_fee(pos.notional) + self._calculate_futures_fee(pos.notional)
        total_pnl = price_pnl + pos.funding_received - pos.fees_paid - exit_fee

        self.balance_usdt += pos.notional + total_pnl
        self.locked_usdt -= pos.notional
        pos.status = "closed"

        await update_balance("USDT", self.balance_usdt, 0, self.balance_usdt + self.locked_usdt)

        result = {
            "event": "position_closed",
            "type": pos.strategy,
            "symbol": pos.symbol,
            "notional": round(pos.notional, 2),
            "price_pnl": round(price_pnl, 2),
            "funding_received": round(pos.funding_received, 2),
            "fees": round(pos.fees_paid + exit_fee, 2),
            "total_pnl": round(total_pnl, 2),
            "balance_remaining": round(self.balance_usdt, 2),
            "locked": round(self.locked_usdt, 2),
            "trade_time": time.strftime("%H:%M:%S"),
            "position_id": position_id,
        }

        if self._on_trade:
            await self._on_trade(result)
        return result

    async def _open_basis_position(self, op, validation: Dict) -> Dict:
        pos_size = validation["pos_size"]
        entry_fee = validation["entry_fee"]
        is_short = "short" in op.action

        op_id = await save_opportunity("basis", op.symbol, op.details, op.expected_profit_pct / 100, validation["net_profit"], op.confidence)

        spot_qty = pos_size / op.spot_price if op.spot_price > 0 else 0
        futures_qty = pos_size / op.futures_price if op.futures_price > 0 else 0
        spot_side = "BUY" if is_short else "SELL"
        futures_side = "SELL" if is_short else "BUY"

        spot_order_id = await save_order(op_id, "basis_arb", op.symbol, spot_side, op.spot_price, spot_qty)
        fut_order_id = await save_order(op_id, "basis_arb", f"{op.symbol}_PERP", futures_side, op.futures_price, futures_qty)

        self.balance_usdt -= pos_size
        self.locked_usdt += pos_size

        for order_id in (spot_order_id, fut_order_id):
            await update_order(order_id, "filled", time.strftime("%Y-%m-%dT%H:%M:%SZ"), entry_fee / 2, 0)

        pos_id = f"{op.symbol}_basis_{uuid.uuid4().hex[:8]}"
        self.positions[pos_id] = PaperPosition(
            id=pos_id,
            symbol=op.symbol,
            strategy="basis",
            side=op.action,
            entry_spot_price=op.spot_price,
            entry_futures_price=op.futures_price,
            quantity_spot=spot_qty,
            quantity_futures=futures_qty,
            notional=pos_size,
            opened_at=time.time(),
            fees_paid=entry_fee,
            is_short_perp=is_short,
        )

        await update_balance("USDT", self.balance_usdt, self.locked_usdt, self.balance_usdt + self.locked_usdt)

        result = {
            "event": "position_opened",
            "type": "basis",
            "symbol": op.symbol,
            "action": op.action,
            "notional": round(pos_size, 2),
            "basis_pct": round(op.basis_pct, 4),
            "net_basis_pct": round(op.net_basis_pct, 4),
            "expected_profit": round(validation["net_profit"], 2),
            "fee": round(entry_fee, 2),
            "fees": round(entry_fee, 2),
            "balance_remaining": round(self.balance_usdt, 2),
            "locked": round(self.locked_usdt, 2),
            "trade_time": time.strftime("%H:%M:%S"),
            "position_id": pos_id,
        }

        if self._on_trade:
            await self._on_trade(result)
        return result

    async def _execute_triangular(self, op, validation: Dict) -> Dict:
        pos_size = validation["pos_size"]
        total_fees = validation["total_fees"]
        net_profit = validation["net_profit"]

        op_id = await save_opportunity("triangular", f"{op.symbol_a}-{op.symbol_b}-{op.symbol_c}", op.details, op.profit_pct / 100, validation["net_profit"], op.confidence)

        orders = []
        current_notional = pos_size

        for leg in op.legs:
            qty = current_notional / leg["price"] if leg["price"] > 0 else 0
            oid = await save_order(op_id, "triangular_arb", leg["symbol"], leg["side"], leg["price"], qty)
            await update_order(oid, "filled", time.strftime("%Y-%m-%dT%H:%M:%SZ"), self._calculate_spot_fee(current_notional), 0)
            orders.append({"id": oid, "symbol": leg["symbol"], "side": leg["side"], "price": leg["price"], "qty": qty})
            current_notional = qty if leg["side"] == "BUY" else qty * leg["price"]

        self.balance_usdt += net_profit
        await update_balance("USDT", self.balance_usdt, 0, self.balance_usdt)

        result = {
            "event": "trade_completed",
            "type": "triangular",
            "path": op.path,
            "orders": orders,
            "notional": round(pos_size, 2),
            "fee": round(total_fees, 2),
            "net_profit": round(net_profit, 2),
            "balance_remaining": round(self.balance_usdt, 2),
            "profit_pct": round(op.profit_pct, 4),
            "legs": op.legs,
            "trade_time": time.strftime("%H:%M:%S"),
        }

        if self._on_trade:
            await self._on_trade(result)
        return result

    def get_open_positions(self, client=None) -> List[Dict]:
        result = []
        for p in self.positions.values():
            if p.status != "open":
                continue
            entry = {
                "id": p.id,
                "symbol": p.symbol,
                "side": p.side,
                "notional": round(p.notional, 2),
                "entry_spot": round(p.entry_spot_price, 6),
                "entry_futures": round(p.entry_futures_price, 6),
                "funding_received": round(p.funding_received, 2),
                "fees_paid": round(p.fees_paid, 2),
                "opened_at": time.strftime("%H:%M:%S", time.localtime(p.opened_at)),
            }
            if client:
                t = client.tickers.get(p.symbol)
                if t and t.spot_price > 0 and t.futures_price > 0:
                    cur_short = ((p.entry_futures_price - t.futures_price) / p.entry_futures_price) * p.notional
                    cur_long = ((t.spot_price - p.entry_spot_price) / p.entry_spot_price) * p.notional
                    if p.is_short_perp:
                        upnl = cur_short + cur_long
                    else:
                        upnl = -cur_short + cur_long
                    cur_basis = ((t.futures_price - t.spot_price) / t.spot_price) * 100
                    entry_basis = ((p.entry_futures_price - p.entry_spot_price) / p.entry_spot_price) * 100
                    entry["unrealized_pnl"] = round(upnl, 2)
                    entry["current_spot"] = t.spot_price
                    entry["current_futures"] = t.futures_price
                    entry["current_basis"] = round(cur_basis, 4)
                    entry["entry_basis"] = round(entry_basis, 4)
                    entry["total_pnl"] = round(upnl + p.funding_received - p.fees_paid, 2)
            result.append(entry)
        return result

    async def get_status(self) -> Dict:
        total_fees = sum(p.fees_paid for p in self.positions.values())
        total_funding = sum(p.funding_received for p in self.positions.values())
        total_equity = self.balance_usdt + self.locked_usdt
        total_return = total_equity - settings.paper_initial_balance
        return_pct = (total_return / settings.paper_initial_balance * 100) if settings.paper_initial_balance > 0 else 0
        return {
            "balance_usdt": round(self.balance_usdt, 2),
            "locked_usdt": round(self.locked_usdt, 2),
            "total_equity": round(total_equity, 2),
            "total_fees": round(total_fees, 2),
            "total_funding": round(total_funding, 2),
            "total_return": round(total_return, 2),
            "open_positions": len([p for p in self.positions.values() if p.status == "open"]),
            "initial_balance": settings.paper_initial_balance,
            "return_pct": round(return_pct, 2),
            "auto_trade": self._auto_trade,
            "trade_size_pct": self._trade_size_pct,
        }