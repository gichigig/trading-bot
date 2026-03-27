"""
Execution Manager
==================

Central execution coordinator.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
import asyncio
import logging

from trading_bot.core.types import (
    Signal, Position, Trade, Order, Side, SignalType,
    PositionStatus
)
from trading_bot.core.config import ExecutionConfig
from trading_bot.core.events import Event, EventType, EventBus, get_event_bus
from trading_bot.execution.orders import OrderManager, OrderRequest
from trading_bot.execution.lifecycle import (
    TradeLifecycleManager, TrailingStopConfig, BreakEvenConfig, TimeBasedExitConfig
)
from trading_bot.risk.position_sizer import PositionSizeResult

logger = logging.getLogger(__name__)


class ExecutionManager:
    """
    Central execution coordinator.
    
    Responsibilities:
    - Convert signals to orders
    - Manage order execution
    - Coordinate trade lifecycle
    - Handle position tracking
    """
    
    def __init__(
        self,
        execution_config: ExecutionConfig,
        event_bus: Optional[EventBus] = None,
    ):
        self.config = execution_config
        self._event_bus = event_bus or get_event_bus()
        
        # Components
        self._order_manager = OrderManager(execution_config, event_bus)
        self._lifecycle_manager = TradeLifecycleManager(event_bus)
        
        # Position tracking
        self._positions: Dict[str, Position] = {}
        self._trades: List[Trade] = []
        
        # Signal to position mapping
        self._signal_position_map: Dict[str, str] = {}
        
        # Callbacks
        self._on_position_opened: Optional[callable] = None
        self._on_position_closed: Optional[callable] = None
    
    def set_exchange_adapter(self, adapter: Any) -> None:
        """Set exchange adapter for order execution."""
        self._order_manager.set_exchange_adapter(adapter)
    
    def set_callbacks(
        self,
        on_position_opened: Optional[callable] = None,
        on_position_closed: Optional[callable] = None,
    ) -> None:
        """Set position callbacks."""
        self._on_position_opened = on_position_opened
        self._on_position_closed = on_position_closed
    
    async def execute_signal(
        self,
        signal: Signal,
        position_size: PositionSizeResult,
    ) -> Optional[Position]:
        """
        Execute a trading signal.
        
        Args:
            signal: The trading signal
            position_size: Calculated position size from risk engine
            
        Returns:
            Position if opened, None if execution failed
        """
        if not position_size.approved:
            logger.warning(f"Signal rejected by risk: {position_size.rejection_reason}")
            return None
        
        # Create order request
        entry_price = signal.entry_price or signal.price
        
        request = OrderRequest(
            symbol=signal.symbol,
            side=Side.BUY if signal.signal_type in [SignalType.ENTRY_LONG, SignalType.SCALE_IN] else Side.SELL,
            quantity=position_size.quantity,
            order_type=self._get_order_type(signal),
            price=entry_price if self.config.default_order_type == "limit" else None,
            signal_id=signal.signal_id,
            max_slippage_pct=self.config.max_slippage_pct,
            max_retries=self.config.retry_attempts,
            use_post_only=self.config.use_post_only,
            timeout_seconds=self.config.order_timeout_seconds,
        )
        
        # Execute order
        result = await self._order_manager.execute_order(request)
        
        if not result.success:
            logger.error(f"Order execution failed: {result.error}")
            return None
        
        # Create position
        order = result.order
        position = Position(
            position_id=f"pos_{signal.signal_id[:8]}",
            symbol=signal.symbol,
            side=request.side,
            quantity=order.filled_quantity,
            entry_price=order.average_fill_price,
            current_price=order.average_fill_price,
            stop_loss=signal.stop_loss,
            original_stop_loss=signal.stop_loss,
            strategy_name=signal.strategy_name,
            strategy_version=signal.strategy_version,
            signal_id=signal.signal_id,
            entry_reason=signal.reason,
        )
        position.entry_orders.append(order.order_id)
        
        # Store position
        self._positions[position.position_id] = position
        self._signal_position_map[signal.signal_id] = position.position_id
        
        # Set up lifecycle management
        trailing_config = None
        if signal.metadata.get("trailing_stop"):
            trailing_config = TrailingStopConfig(
                enabled=True,
                distance=signal.metadata.get("trailing_stop_distance", 2.0),
                distance_type=signal.metadata.get("trailing_stop_type", "atr"),
            )
        
        break_even_config = None
        if signal.metadata.get("break_even"):
            break_even_config = BreakEvenConfig(
                enabled=True,
                trigger_profit_pct=signal.metadata.get("break_even_trigger", 1.0),
            )
        
        self._lifecycle_manager.manage_position(
            position=position,
            take_profits=signal.take_profits,
            trailing_stop=trailing_config,
            break_even=break_even_config,
        )
        
        # Place stop loss order
        if signal.stop_loss:
            await self._place_stop_order(position, signal.stop_loss)
        
        # Place take profit orders
        for tp in signal.take_profits:
            await self._place_take_profit_order(position, tp)
        
        # Publish event
        self._event_bus.publish(Event(
            event_type=EventType.POSITION_OPENED,
            source="execution_manager",
            data=position.to_dict(),
        ))
        
        # Callback
        if self._on_position_opened:
            self._on_position_opened(position)
        
        logger.info(f"Position opened: {position.symbol} {position.side.value} {position.quantity} @ {position.entry_price}")
        
        return position
    
    async def close_position(
        self,
        position_id: str,
        reason: str = "Manual close",
    ) -> Optional[Trade]:
        """Close a position."""
        position = self._positions.get(position_id)
        if not position:
            logger.warning(f"Position not found: {position_id}")
            return None
        
        position.status = PositionStatus.CLOSING
        
        # Cancel any open orders for this position
        for order_id in position.stop_orders + position.take_profit_orders:
            await self._order_manager.cancel_order(order_id)
        
        # Place market close order
        request = OrderRequest(
            symbol=position.symbol,
            side=position.side.opposite,
            quantity=position.quantity,
            order_type=OrderType.MARKET,
            position_id=position_id,
            reduce_only=True,
        )
        
        result = await self._order_manager.execute_order(request)
        
        if not result.success:
            logger.error(f"Failed to close position: {result.error}")
            position.status = PositionStatus.OPEN
            return None
        
        # Create trade record
        trade = self._create_trade_record(position, result.order, reason)
        
        # Update position
        position.status = PositionStatus.CLOSED
        position.closed_at = datetime.utcnow()
        position.exit_reason = reason
        position.realized_pnl = trade.net_pnl
        
        # Clean up
        self._lifecycle_manager.on_position_closed(position_id)
        
        # Publish event
        self._event_bus.publish(Event(
            event_type=EventType.POSITION_CLOSED,
            source="execution_manager",
            data={
                "position": position.to_dict(),
                "trade": trade.to_dict(),
            },
        ))
        
        # Callback
        if self._on_position_closed:
            self._on_position_closed(position, trade)
        
        logger.info(f"Position closed: {position.symbol} PnL=${trade.net_pnl:.2f}")
        
        return trade
    
    async def _place_stop_order(self, position: Position, stop_price: float) -> Optional[str]:
        """Place stop loss order for position."""
        from trading_bot.core.types import OrderType
        
        request = OrderRequest(
            symbol=position.symbol,
            side=position.side.opposite,
            quantity=position.quantity,
            order_type=OrderType.STOP,
            stop_price=stop_price,
            position_id=position.position_id,
            reduce_only=True,
        )
        
        result = await self._order_manager.execute_order(request)
        
        if result.success and result.order:
            position.stop_orders.append(result.order.order_id)
            return result.order.order_id
        
        return None
    
    async def _place_take_profit_order(
        self,
        position: Position,
        take_profit: Dict[str, float],
    ) -> Optional[str]:
        """Place take profit order."""
        from trading_bot.core.types import OrderType
        
        price = take_profit.get("price", 0)
        pct = take_profit.get("pct", take_profit.get("quantity_pct", 100))
        quantity = position.quantity * (pct / 100)
        
        request = OrderRequest(
            symbol=position.symbol,
            side=position.side.opposite,
            quantity=quantity,
            order_type=OrderType.LIMIT,
            price=price,
            position_id=position.position_id,
            reduce_only=True,
        )
        
        result = await self._order_manager.execute_order(request)
        
        if result.success and result.order:
            position.take_profit_orders.append(result.order.order_id)
            return result.order.order_id
        
        return None
    
    async def modify_stop_loss(
        self,
        position_id: str,
        new_stop: float,
    ) -> bool:
        """Modify stop loss for a position."""
        position = self._positions.get(position_id)
        if not position:
            return False
        
        # Cancel existing stop orders
        for order_id in position.stop_orders:
            await self._order_manager.cancel_order(order_id)
        position.stop_orders.clear()
        
        # Place new stop order
        order_id = await self._place_stop_order(position, new_stop)
        
        if order_id:
            position.stop_loss = new_stop
            return True
        
        return False
    
    def _get_order_type(self, signal: Signal) -> 'OrderType':
        """Determine order type from signal and config."""
        from trading_bot.core.types import OrderType
        
        if self.config.default_order_type == "market":
            return OrderType.MARKET
        return OrderType.LIMIT
    
    def _create_trade_record(
        self,
        position: Position,
        exit_order: Order,
        reason: str,
    ) -> Trade:
        """Create trade record from closed position."""
        import uuid
        
        # Calculate PnL
        if position.side == Side.BUY:
            gross_pnl = (exit_order.average_fill_price - position.entry_price) * position.quantity
        else:
            gross_pnl = (position.entry_price - exit_order.average_fill_price) * position.quantity
        
        commission = position.total_commission + exit_order.commission
        net_pnl = gross_pnl - commission
        pnl_pct = (net_pnl / (position.entry_price * position.quantity)) * 100
        
        trade = Trade(
            trade_id=f"trade_{uuid.uuid4().hex[:8]}",
            position_id=position.position_id,
            symbol=position.symbol,
            side=position.side,
            entry_price=position.entry_price,
            exit_price=exit_order.average_fill_price,
            quantity=position.quantity,
            gross_pnl=gross_pnl,
            commission=commission,
            net_pnl=net_pnl,
            pnl_pct=pnl_pct,
            entry_time=position.opened_at,
            exit_time=datetime.utcnow(),
            duration_seconds=position.duration_seconds,
            max_favorable_excursion=position.max_favorable_excursion,
            max_adverse_excursion=position.max_adverse_excursion,
            strategy_name=position.strategy_name,
            strategy_version=position.strategy_version,
            entry_reason=position.entry_reason,
            exit_reason=reason,
        )
        
        self._trades.append(trade)
        return trade
    
    def update_prices(self, symbol: str, price: float, atr: Optional[float] = None) -> None:
        """Update prices and check lifecycle events."""
        # Update positions
        for position in self._positions.values():
            if position.symbol == symbol:
                position.update_price(price)
        
        # Check lifecycle manager
        actions = self._lifecycle_manager.update_price(symbol, price, atr)
        
        # Process actions asynchronously
        for action in actions:
            asyncio.create_task(self._process_lifecycle_action(action))
    
    async def _process_lifecycle_action(self, action: Dict[str, Any]) -> None:
        """Process a lifecycle action."""
        action_type = action.get("action")
        position_id = action.get("position_id")
        
        if action_type == "close_position":
            await self.close_position(position_id, action.get("reason", "Lifecycle exit"))
        
        elif action_type == "modify_stop":
            await self.modify_stop_loss(position_id, action.get("new_stop"))
        
        elif action_type == "partial_close":
            # Handle partial close
            position = self._positions.get(position_id)
            if position:
                quantity = action.get("quantity", 0)
                # Place partial close order
                request = OrderRequest(
                    symbol=position.symbol,
                    side=position.side.opposite,
                    quantity=quantity,
                    order_type=OrderType.MARKET,
                    position_id=position_id,
                    reduce_only=True,
                )
                await self._order_manager.execute_order(request)
    
    def get_position(self, position_id: str) -> Optional[Position]:
        """Get position by ID."""
        return self._positions.get(position_id)
    
    def get_all_positions(self) -> List[Position]:
        """Get all open positions."""
        return [p for p in self._positions.values() if p.is_open]
    
    def get_trades(self, limit: int = 100) -> List[Trade]:
        """Get recent trades."""
        return self._trades[-limit:]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get execution statistics."""
        return {
            "open_positions": len(self.get_all_positions()),
            "total_trades": len(self._trades),
            "order_stats": self._order_manager.get_stats(),
        }
