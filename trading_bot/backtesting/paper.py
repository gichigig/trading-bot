"""
Paper Trading Engine
=====================

Realistic paper trading with live data for validation.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional
import random

from trading_bot.core.types import Candle, Signal, Order, Position, Side, OrderStatus, OrderType
from trading_bot.core.events import Event, EventType, EventBus, get_event_bus

logger = logging.getLogger(__name__)


@dataclass
class PaperOrder:
    """Paper trading order."""
    id: str
    symbol: str
    side: Side
    quantity: float
    order_type: OrderType
    price: Optional[float]
    stop_price: Optional[float]
    status: OrderStatus
    created_at: datetime
    filled_at: Optional[datetime] = None
    filled_price: Optional[float] = None
    filled_quantity: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "symbol": self.symbol,
            "side": self.side.value,
            "quantity": self.quantity,
            "order_type": self.order_type.value,
            "price": self.price,
            "stop_price": self.stop_price,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "filled_at": self.filled_at.isoformat() if self.filled_at else None,
            "filled_price": self.filled_price,
            "filled_quantity": self.filled_quantity,
        }


@dataclass
class PaperPosition:
    """Paper trading position."""
    id: str
    symbol: str
    side: Side
    quantity: float
    entry_price: float
    entry_time: datetime
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    unrealized_pnl: float = 0.0
    current_price: float = 0.0
    max_favorable: float = 0.0
    max_adverse: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "symbol": self.symbol,
            "side": self.side.value,
            "quantity": self.quantity,
            "entry_price": self.entry_price,
            "entry_time": self.entry_time.isoformat(),
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "unrealized_pnl": self.unrealized_pnl,
            "current_price": self.current_price,
        }


class PaperTradingEngine:
    """
    Paper trading engine for live testing without real money.
    
    Features:
    - Realistic fill simulation
    - Slippage modeling
    - Live data integration
    - Full position tracking
    - Parity with live trading
    """
    
    def __init__(
        self,
        initial_balance: float = 100000.0,
        commission_pct: float = 0.1,
        slippage_pct: float = 0.05,
        event_bus: Optional[EventBus] = None,
    ):
        self._initial_balance = initial_balance
        self._balance = initial_balance
        self._commission_pct = commission_pct
        self._slippage_pct = slippage_pct
        self._event_bus = event_bus or get_event_bus()
        
        # State
        self._positions: Dict[str, PaperPosition] = {}
        self._orders: Dict[str, PaperOrder] = {}
        self._completed_orders: List[PaperOrder] = []
        self._closed_positions: List[Dict[str, Any]] = []
        
        # Current prices
        self._current_prices: Dict[str, float] = {}
        
        # Statistics
        self._total_commission = 0.0
        self._total_slippage = 0.0
        self._trade_count = 0
        self._order_count = 0
        
        # Fill simulation settings
        self._fill_delay_ms = (50, 200)  # Random delay range
        self._partial_fill_chance = 0.1
    
    async def start(self) -> None:
        """Start paper trading engine."""
        logger.info(f"Paper trading started with ${self._balance:,.2f}")
        
        self._event_bus.publish(Event(
            event_type=EventType.BOT_STARTED,
            source="paper_trading",
            data={"mode": "paper", "balance": self._balance},
        ))
    
    async def stop(self) -> None:
        """Stop paper trading engine."""
        # Cancel all pending orders
        for order_id in list(self._orders.keys()):
            await self.cancel_order(order_id)
        
        logger.info("Paper trading stopped")
        
        self._event_bus.publish(Event(
            event_type=EventType.BOT_STOPPED,
            source="paper_trading",
            data={"final_balance": self._balance},
        ))
    
    def update_price(self, symbol: str, price: float) -> None:
        """Update current price for a symbol."""
        self._current_prices[symbol] = price
        
        # Check pending orders
        asyncio.create_task(self._check_pending_orders(symbol, price))
        
        # Update positions
        self._update_positions(symbol, price)
    
    def on_candle(self, candle: Candle) -> None:
        """Process incoming candle data."""
        self.update_price(candle.symbol, candle.close)
        
        # Check stops/targets with high/low
        self._check_stops_targets(candle)
    
    async def submit_order(
        self,
        symbol: str,
        side: Side,
        quantity: float,
        order_type: OrderType = OrderType.MARKET,
        price: Optional[float] = None,
        stop_price: Optional[float] = None,
    ) -> Optional[PaperOrder]:
        """Submit a paper order."""
        self._order_count += 1
        order_id = f"paper_order_{self._order_count}"
        
        order = PaperOrder(
            id=order_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=order_type,
            price=price,
            stop_price=stop_price,
            status=OrderStatus.PENDING,
            created_at=datetime.utcnow(),
        )
        
        # Validate order
        if not self._validate_order(order):
            order.status = OrderStatus.REJECTED
            return order
        
        # Market orders fill immediately with simulated delay
        if order_type == OrderType.MARKET:
            await self._simulate_fill_delay()
            await self._fill_order(order)
        else:
            # Limit/stop orders go pending
            self._orders[order_id] = order
            order.status = OrderStatus.OPEN
        
        self._event_bus.publish(Event(
            event_type=EventType.ORDER_CREATED,
            source="paper_trading",
            data=order.to_dict(),
        ))
        
        return order
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order."""
        if order_id not in self._orders:
            return False
        
        order = self._orders.pop(order_id)
        order.status = OrderStatus.CANCELLED
        self._completed_orders.append(order)
        
        self._event_bus.publish(Event(
            event_type=EventType.ORDER_CANCELLED,
            source="paper_trading",
            data=order.to_dict(),
        ))
        
        return True
    
    def _validate_order(self, order: PaperOrder) -> bool:
        """Validate order before submission."""
        current_price = self._current_prices.get(order.symbol)
        
        if not current_price:
            logger.warning(f"No price available for {order.symbol}")
            return False
        
        # Check balance for buys
        if order.side == Side.BUY:
            cost = (order.price or current_price) * order.quantity
            if cost > self._balance:
                logger.warning(f"Insufficient balance: {cost} > {self._balance}")
                return False
        
        return True
    
    async def _fill_order(self, order: PaperOrder) -> None:
        """Fill an order with slippage simulation."""
        current_price = self._current_prices.get(order.symbol)
        
        if not current_price:
            order.status = OrderStatus.REJECTED
            return
        
        # Calculate fill price with slippage
        slippage = current_price * self._slippage_pct / 100
        
        if order.side == Side.BUY:
            fill_price = current_price + slippage
        else:
            fill_price = current_price - slippage
        
        # Simulate partial fills occasionally
        fill_quantity = order.quantity
        if random.random() < self._partial_fill_chance:
            fill_quantity = order.quantity * random.uniform(0.5, 0.9)
            order.status = OrderStatus.PARTIALLY_FILLED
        else:
            order.status = OrderStatus.FILLED
        
        order.filled_price = fill_price
        order.filled_quantity = fill_quantity
        order.filled_at = datetime.utcnow()
        
        # Calculate commission
        commission = fill_price * fill_quantity * self._commission_pct / 100
        self._total_commission += commission
        self._total_slippage += abs(slippage * fill_quantity)
        
        # Update balance and positions
        if order.side == Side.BUY:
            self._balance -= fill_price * fill_quantity + commission
            self._open_position(order, fill_price, fill_quantity)
        else:
            self._balance += fill_price * fill_quantity - commission
            self._close_position_for_order(order, fill_price, fill_quantity)
        
        # Move to completed
        if order.id in self._orders:
            del self._orders[order.id]
        self._completed_orders.append(order)
        
        self._event_bus.publish(Event(
            event_type=EventType.ORDER_FILLED,
            source="paper_trading",
            data=order.to_dict(),
        ))
    
    def _open_position(
        self,
        order: PaperOrder,
        fill_price: float,
        quantity: float,
    ) -> None:
        """Open or add to a position."""
        self._trade_count += 1
        
        if order.symbol in self._positions:
            # Add to existing position
            pos = self._positions[order.symbol]
            total_cost = pos.entry_price * pos.quantity + fill_price * quantity
            pos.quantity += quantity
            pos.entry_price = total_cost / pos.quantity
        else:
            # New position
            pos = PaperPosition(
                id=f"paper_pos_{self._trade_count}",
                symbol=order.symbol,
                side=order.side,
                quantity=quantity,
                entry_price=fill_price,
                entry_time=datetime.utcnow(),
                current_price=fill_price,
            )
            self._positions[order.symbol] = pos
        
        self._event_bus.publish(Event(
            event_type=EventType.POSITION_OPENED,
            source="paper_trading",
            data=self._positions[order.symbol].to_dict(),
        ))
    
    def _close_position_for_order(
        self,
        order: PaperOrder,
        fill_price: float,
        quantity: float,
    ) -> None:
        """Close or reduce a position."""
        if order.symbol not in self._positions:
            return
        
        pos = self._positions[order.symbol]
        
        # Calculate P&L
        if pos.side == Side.BUY:
            pnl = (fill_price - pos.entry_price) * min(quantity, pos.quantity)
        else:
            pnl = (pos.entry_price - fill_price) * min(quantity, pos.quantity)
        
        # Return capital
        self._balance += pos.entry_price * min(quantity, pos.quantity) + pnl
        
        if quantity >= pos.quantity:
            # Full close
            self._closed_positions.append({
                **pos.to_dict(),
                "exit_price": fill_price,
                "exit_time": datetime.utcnow().isoformat(),
                "pnl": pnl,
            })
            del self._positions[order.symbol]
            
            self._event_bus.publish(Event(
                event_type=EventType.POSITION_CLOSED,
                source="paper_trading",
                data={"symbol": order.symbol, "pnl": pnl},
            ))
        else:
            # Partial close
            pos.quantity -= quantity
    
    async def _check_pending_orders(self, symbol: str, price: float) -> None:
        """Check if any pending orders should fill."""
        for order_id, order in list(self._orders.items()):
            if order.symbol != symbol:
                continue
            
            should_fill = False
            
            if order.order_type == OrderType.LIMIT:
                if order.side == Side.BUY and price <= order.price:
                    should_fill = True
                elif order.side == Side.SELL and price >= order.price:
                    should_fill = True
            
            elif order.order_type == OrderType.STOP:
                if order.side == Side.BUY and price >= order.stop_price:
                    should_fill = True
                elif order.side == Side.SELL and price <= order.stop_price:
                    should_fill = True
            
            if should_fill:
                await self._simulate_fill_delay()
                await self._fill_order(order)
    
    def _update_positions(self, symbol: str, price: float) -> None:
        """Update position mark-to-market."""
        if symbol not in self._positions:
            return
        
        pos = self._positions[symbol]
        pos.current_price = price
        
        if pos.side == Side.BUY:
            pos.unrealized_pnl = (price - pos.entry_price) * pos.quantity
        else:
            pos.unrealized_pnl = (pos.entry_price - price) * pos.quantity
        
        # Track excursions
        if pos.unrealized_pnl > pos.max_favorable:
            pos.max_favorable = pos.unrealized_pnl
        if pos.unrealized_pnl < pos.max_adverse:
            pos.max_adverse = pos.unrealized_pnl
    
    def _check_stops_targets(self, candle: Candle) -> None:
        """Check stop loss and take profit levels."""
        if candle.symbol not in self._positions:
            return
        
        pos = self._positions[candle.symbol]
        
        # Check stop loss
        if pos.stop_loss:
            if pos.side == Side.BUY and candle.low <= pos.stop_loss:
                asyncio.create_task(self._close_at_stop(pos, pos.stop_loss))
                return
            elif pos.side == Side.SELL and candle.high >= pos.stop_loss:
                asyncio.create_task(self._close_at_stop(pos, pos.stop_loss))
                return
        
        # Check take profit
        if pos.take_profit:
            if pos.side == Side.BUY and candle.high >= pos.take_profit:
                asyncio.create_task(self._close_at_target(pos, pos.take_profit))
                return
            elif pos.side == Side.SELL and candle.low <= pos.take_profit:
                asyncio.create_task(self._close_at_target(pos, pos.take_profit))
                return
    
    async def _close_at_stop(self, position: PaperPosition, price: float) -> None:
        """Close position at stop loss."""
        close_side = Side.SELL if position.side == Side.BUY else Side.BUY
        
        await self.submit_order(
            symbol=position.symbol,
            side=close_side,
            quantity=position.quantity,
            order_type=OrderType.MARKET,
        )
        
        self._event_bus.publish(Event(
            event_type=EventType.POSITION_STOPPED,
            source="paper_trading",
            data={"symbol": position.symbol, "stop_price": price},
        ))
    
    async def _close_at_target(self, position: PaperPosition, price: float) -> None:
        """Close position at take profit."""
        close_side = Side.SELL if position.side == Side.BUY else Side.BUY
        
        await self.submit_order(
            symbol=position.symbol,
            side=close_side,
            quantity=position.quantity,
            order_type=OrderType.MARKET,
        )
    
    async def _simulate_fill_delay(self) -> None:
        """Simulate realistic fill delay."""
        delay_ms = random.randint(*self._fill_delay_ms)
        await asyncio.sleep(delay_ms / 1000)
    
    # =========== Getters ===========
    
    def get_balance(self) -> float:
        """Get current cash balance."""
        return self._balance
    
    def get_equity(self) -> float:
        """Get total equity (balance + unrealized P&L)."""
        equity = self._balance
        
        for pos in self._positions.values():
            equity += pos.entry_price * pos.quantity + pos.unrealized_pnl
        
        return equity
    
    def get_positions(self) -> Dict[str, PaperPosition]:
        """Get all open positions."""
        return self._positions.copy()
    
    def get_pending_orders(self) -> Dict[str, PaperOrder]:
        """Get all pending orders."""
        return self._orders.copy()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get trading statistics."""
        return {
            "initial_balance": self._initial_balance,
            "current_balance": self._balance,
            "equity": self.get_equity(),
            "return_pct": (self.get_equity() - self._initial_balance) / self._initial_balance * 100,
            "open_positions": len(self._positions),
            "pending_orders": len(self._orders),
            "total_trades": self._trade_count,
            "total_commission": self._total_commission,
            "total_slippage": self._total_slippage,
        }
