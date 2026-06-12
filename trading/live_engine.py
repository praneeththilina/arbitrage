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
class LivePosition:
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

class LiveEngine:
    def __init__(self):
        self.balance_usdt: float = 0.0
        self.locked_usdt: float = 0.0
        self.positions: Dict[str, LivePosition] = {}
        self._running = False
        self._auto_trade = settings.auto_trade_enabled
        self._trade_size_pct = settings.trade_size_pct
        self._on_trade: Optional[Callable] = None
        self._cooldowns: Dict[str, float] = {}
        self._client = None
        self._initial_balance = 0.0

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
        if self._client:
            spot_bal = await self._client.get_spot_balance("USDT")
            self.balance_usdt = spot_bal
            self._initial_balance = spot_bal
            await update_balance("USDT", self.balance_usdt, 0, self.balance_usdt)
        asyncio.create_task(self._sync_balance_loop())

    def stop(self):
        self._running = False

    async def _sync_balance_loop(self):
        while self._running:
            if self._client:
                try:
                    spot_bal = await self._client.get_spot_balance("USDT")
                    self.balance_usdt = spot_bal
                except Exception as e:
                    logger.error(f"Error syncing balance: {e}")
            await asyncio.sleep(60)

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
        pos_size = min(pos_size, settings.max_position_size_usdt)

        if pos_size > self.balance_usdt or pos_size <= 10:  # Binance minimum order is typically ~10 USDT
            result["reason"] = "insufficient balance or below minimum"
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
        return result

    def validate_triangular_op(self, op) -> Dict:
        result = {"valid": False, "reason": "", "net_profit": 0.0, "pos_size": 0.0}

        pos_size = min(self._get_position_size(), settings.max_position_size_usdt) * 0.5
        if pos_size <= 10:
            result["reason"] = "insufficient balance or below minimum"
            return result

        net_profit_usdt = pos_size * (op.profit_pct / 100.0)

        if net_profit_usdt < settings.min_net_profit_usdt or self._is_on_cooldown(f"tri_{op.symbol_a}_{op.symbol_b}_{op.symbol_c}"):
            result["reason"] = "invalid net profit or cooldown"
            return result

        result["valid"] = True
        result["net_profit"] = net_profit_usdt
        result["pos_size"] = pos_size
        return result

    async def evaluate_and_execute(self, op_type: str, op) -> Optional[Dict]:
        if not self._auto_trade or not self._running or not self._client:
            return None

        if op_type == "funding":
            validation = self.validate_funding_op(op)
            if validation["valid"]:
                self._set_cooldown(f"funding_{op.symbol}")
                return await self._open_funding_position(op, validation)
            
        elif op_type == "triangular":
            validation = self.validate_triangular_op(op)
            if validation["valid"]:
                self._set_cooldown(f"tri_{op.symbol_a}_{op.symbol_b}_{op.symbol_c}")
                return await self._execute_triangular(op, validation)

        return None

    async def _open_funding_position(self, op, validation: Dict) -> Dict:
        pos_size = validation["pos_size"]
        is_short = "short" in op.action

        op_id = await save_opportunity("funding", op.symbol, op.details, op.expected_apr / 100, 0, op.confidence)

        spot_qty = pos_size / op.spot_price
        futures_qty = pos_size / op.futures_price
        
        # Round quantities to roughly 3 decimals to avoid precision errors on Binance
        # In a real implementation we would fetch LOT_SIZE and STEP_SIZE from exchange_info
        spot_qty = round(spot_qty, 3)
        futures_qty = round(futures_qty, 3)

        spot_side = "SELL" if is_short else "BUY"
        futures_side = "BUY" if "long_perp" in op.action else "SELL"

        # Set Isolated Margin before opening futures position
        await self._client.set_margin_type(op.symbol, "ISOLATED")

        # Execute Live Orders Concurrent/Sequential
        logger.info(f"LIVE EXECUTION: Funding Arb {op.symbol}")
        
        spot_order = await self._client.create_spot_order(op.symbol, spot_side, "MARKET", spot_qty)
        fut_order = await self._client.create_futures_order(op.symbol, futures_side, "MARKET", futures_qty)

        # Check errors
        if "orderId" not in spot_order or "orderId" not in fut_order:
            logger.error(f"Funding execution failed! Spot: {spot_order}, Fut: {fut_order}")
            # Potential mitigation: If spot passed but fut failed, we should liquidate spot.
            return None

        spot_order_id = await save_order(op_id, "funding_arb", op.symbol, spot_side, op.spot_price, spot_qty)
        fut_order_id = await save_order(op_id, "funding_arb", f"{op.symbol}_PERP", futures_side, op.futures_price, futures_qty)

        orders = [
            {"id": spot_order_id, "symbol": op.symbol, "side": spot_side, "price": op.spot_price, "qty": spot_qty, "exchange_id": spot_order.get("orderId")},
            {"id": fut_order_id, "symbol": f"{op.symbol}_PERP", "side": futures_side, "price": op.futures_price, "qty": futures_qty, "exchange_id": fut_order.get("orderId")}
        ]

        pos_id = f"{op.symbol}_funding_{uuid.uuid4().hex[:8]}"
        self.positions[pos_id] = LivePosition(
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
            fees_paid=0.0, # Will be synced or calculated
            entry_funding_rate=op.funding_rate,
            next_funding_time=op.details.get("next_funding_time", 0),
            is_short_perp=is_short,
        )

        result = {
            "event": "position_opened",
            "type": "funding",
            "symbol": op.symbol,
            "orders": orders,
            "notional": round(pos_size, 2),
            "fee": 0,
            "balance_remaining": self.balance_usdt,
            "locked": self.locked_usdt,
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

        # Execute market orders to close
        spot_side = "BUY" if pos.side.startswith("short_perp_long_spot") else "SELL"
        futures_side = "BUY" if pos.is_short_perp else "SELL"

        logger.info(f"LIVE EXECUTION: Closing Funding Arb {pos.symbol}")

        await self._client.create_spot_order(pos.symbol, spot_side, "MARKET", pos.quantity_spot)
        await self._client.create_futures_order(pos.symbol, futures_side, "MARKET", pos.quantity_futures)

        pos.status = "closed"

        result = {
            "event": "position_closed",
            "type": "funding",
            "symbol": pos.symbol,
            "notional": round(pos.notional, 2),
            "trade_time": time.strftime("%H:%M:%S"),
            "position_id": position_id,
        }

        if self._on_trade:
            await self._on_trade(result)
        return result

    async def _execute_triangular(self, op, validation: Dict) -> Dict:
        pos_size = validation["pos_size"]
        
        op_id = await save_opportunity("triangular", f"{op.symbol_a}-{op.symbol_b}-{op.symbol_c}", op.details, op.profit_pct / 100, validation["net_profit"], op.confidence)

        orders = []
        current_notional = pos_size
        
        logger.info(f"LIVE EXECUTION: Triangular Arb {op.path}")

        # Execute Sequential as per user preference
        for leg in op.legs:
            qty = current_notional / leg["price"] if leg["price"] > 0 else 0
            qty = round(qty, 3) # Simplified LOT_SIZE handling
            
            # Use Immediate Or Cancel (IOC) for slippage protection if supported, else MARKET
            leg_order = await self._client.create_spot_order(leg["symbol"], leg["side"], "MARKET", qty)
            
            if "orderId" not in leg_order:
                logger.error(f"Triangular Execution Failed at leg {leg['symbol']}. Aborting remaining legs. DANGER!")
                break
                
            oid = await save_order(op_id, "triangular_arb", leg["symbol"], leg["side"], leg["price"], qty)
            orders.append({"id": oid, "symbol": leg["symbol"], "side": leg["side"], "price": leg["price"], "qty": qty, "exchange_id": leg_order.get("orderId")})
            
            # Update notional based on real fill if possible, otherwise use estimated
            if "cummulativeQuoteQty" in leg_order and float(leg_order["cummulativeQuoteQty"]) > 0:
                current_notional = float(leg_order["executedQty"]) if leg["side"] == "BUY" else float(leg_order["cummulativeQuoteQty"])
            else:
                current_notional = qty if leg["side"] == "BUY" else qty * leg["price"]

        result = {
            "event": "trade_completed",
            "type": "triangular",
            "path": op.path,
            "orders": orders,
            "notional": round(pos_size, 2),
            "fee": 0.0,
            "net_profit": round(validation["net_profit"], 2),
            "balance_remaining": self.balance_usdt,
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
            if self._client:
                t = self._client.tickers.get(p.symbol)
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
        total_equity = self.balance_usdt + self.locked_usdt
        total_return = total_equity - self._initial_balance
        return_pct = (total_return / self._initial_balance * 100) if self._initial_balance > 0 else 0
        return {
            "balance_usdt": round(self.balance_usdt, 2),
            "locked_usdt": round(self.locked_usdt, 2),
            "total_equity": round(total_equity, 2),
            "total_return": round(total_return, 2),
            "open_positions": len([p for p in self.positions.values() if p.status == "open"]),
            "initial_balance": round(self._initial_balance, 2),
            "return_pct": round(return_pct, 2),
            "auto_trade": self._auto_trade,
            "trade_size_pct": self._trade_size_pct,
            "mode": "LIVE" if not settings.testnet else "TESTNET",
        }
