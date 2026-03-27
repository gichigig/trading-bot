"""
Order Manager
==============

Smart order execution with retries, slippage control, and partial fill handling.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
import asyncio
import logging
import uuid

from trading_bot.core.types import Order, OrderType, OrderStatus, Side
from trading_bot.core.config import ExecutionConfig
from trading_bot.core.events import Event, EventType, EventBus, get_event_bus

logger = logging.getLogger(__name__)


@dataclass
class OrderRequest:
    """Order request with execution preferences."""
    symbol: str
    side: Side
    quantity: float
    order_type: OrderType = OrderType.LIMIT
    price: Optional[float] = None
    stop_price: Optional[float] = None
    
    # Execution preferences
    use_post_only: bool = True
    time_in_force: str = "GTC"
    reduce_only: bool = False
    
    # Linking
    position_id: str = ""
    signal_id: str = ""
    
    # Slippage control
    max_slippage_pct: float = 0.1
    
    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    
    # Timeout
    timeout_seconds: int = 30


@dataclass
class ExecutionResult:
    """Result of order execution attempt."""
    success: bool
    order: Optional[Order] = None
    error: str = ""
    slippage: float = 0.0
    execution_time_ms: float = 0.0
    retries_used: int = 0


class OrderManager:
    """
    Manages order execution with smart features.
    
    Features:
    - Limit vs market logic
    - Post-only where possible
    - Order retries with backoff
    - Partial fill handling
    - Slippage monitoring
    """
    
    def __init__(
        self,
        execution_config: ExecutionConfig,
        event_bus: Optional[EventBus] = None,
    ):
        self.config = execution_config
        self._event_bus = event_bus or get_event_bus()
        
        # Order tracking
        self._pending_orders: Dict[str, Order] = {}
        self._active_orders: Dict[str, Order] = {}
        self._completed_orders: Dict[str, Order] = {}
        
        # Exchange adapter (to be set)
        self._exchange_adapter: Optional[Any] = None
        
        # Statistics
        self._total_orders = 0
        self._successful_orders = 0
        self._failed_orders = 0
        self._total_slippage = 0.0
        self._avg_execution_time = 0.0
    
    def set_exchange_adapter(self, adapter: Any) -> None:
        """Set the exchange adapter for order execution."""
        self._exchange_adapter = adapter
    
    async def execute_order(
        self,
        request: OrderRequest,
    ) -> ExecutionResult:
        """
        Execute an order with smart routing and retry logic.
        """
        start_time = datetime.utcnow()
        self._total_orders += 1
        
        # Create order object
        order = Order(
            order_id=str(uuid.uuid4()),
            client_order_id=f"bot_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}",
            symbol=request.symbol,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            price=request.price,
            stop_price=request.stop_price,
            post_only=request.use_post_only and request.order_type == OrderType.LIMIT,
            reduce_only=request.reduce_only,
            time_in_force=request.time_in_force,
            signal_id=request.signal_id,
            position_id=request.position_id,
        )
        
        self._pending_orders[order.order_id] = order
        
        # Publish order created event
        self._event_bus.publish(Event(
            event_type=EventType.ORDER_CREATED,
            source="order_manager",
            data=order.to_dict(),
        ))
        
        # Execute with retries
        result = await self._execute_with_retries(order, request)
        
        # Calculate execution time
        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        result.execution_time_ms = execution_time
        
        # Update statistics
        if result.success:
            self._successful_orders += 1
            self._total_slippage += abs(result.slippage)
            self._avg_execution_time = (
                (self._avg_execution_time * (self._successful_orders - 1) + execution_time)
                / self._successful_orders
            )
        else:
            self._failed_orders += 1
        
        # Move order to appropriate dict
        self._pending_orders.pop(order.order_id, None)
        if result.success and result.order:
            if result.order.is_filled:
                self._completed_orders[result.order.order_id] = result.order
            else:
                self._active_orders[result.order.order_id] = result.order
        
        return result
    
    async def _execute_with_retries(
        self,
        order: Order,
        request: OrderRequest,
    ) -> ExecutionResult:
        """Execute order with retry logic."""
        retries = 0
        last_error = ""
        
        while retries <= request.max_retries:
            try:
                # Decide order type
                order_type, price = self._decide_order_type(request)
                order.order_type = order_type
                order.price = price
                
                # Submit order
                result = await self._submit_order(order)
                
                if result.success:
                    # Check slippage
                    if result.order and result.order.average_fill_price > 0:
                        expected_price = request.price or result.order.price or 0
                        if expected_price > 0:
                            slippage = self._calculate_slippage(
                                expected_price,
                                result.order.average_fill_price,
                                request.side,
                            )
                            result.slippage = slippage
                            
                            # Check if slippage is acceptable
                            if abs(slippage) > request.max_slippage_pct:
                                logger.warning(f"High slippage detected: {slippage:.4f}%")
                    
                    result.retries_used = retries
                    return result
                
                last_error = result.error
                
            except Exception as e:
                last_error = str(e)
                logger.error(f"Order execution error: {e}")
            
            retries += 1
            if retries <= request.max_retries:
                delay = request.retry_delay_seconds * (2 ** (retries - 1))  # Exponential backoff
                logger.info(f"Retrying order in {delay}s (attempt {retries}/{request.max_retries})")
                await asyncio.sleep(delay)
        
        # All retries failed
        order.status = OrderStatus.REJECTED
        order.last_error = last_error
        
        self._event_bus.publish(Event(
            event_type=EventType.ORDER_REJECTED,
            source="order_manager",
            data={**order.to_dict(), "error": last_error},
        ))
        
        return ExecutionResult(
            success=False,
            order=order,
            error=last_error,
            retries_used=retries,
        )
    
    def _decide_order_type(
        self,
        request: OrderRequest,
    ) -> Tuple[OrderType, Optional[float]]:
        """
        Decide optimal order type based on conditions.
        
        Returns (order_type, price)
        """
        # If explicitly market order
        if request.order_type == OrderType.MARKET:
            return OrderType.MARKET, None
        
        # If we have a price, use limit
        if request.price:
            return OrderType.LIMIT, request.price
        
        # Default to market if no price
        return OrderType.MARKET, None
    
    async def _submit_order(self, order: Order) -> ExecutionResult:
        """Submit order to exchange."""
        if not self._exchange_adapter:
            return ExecutionResult(
                success=False,
                order=order,
                error="No exchange adapter configured",
            )
        
        try:
            order.status = OrderStatus.SUBMITTED
            order.submitted_at = datetime.utcnow()
            
            self._event_bus.publish(Event(
                event_type=EventType.ORDER_SUBMITTED,
                source="order_manager",
                data=order.to_dict(),
            ))
            
            # Call exchange adapter
            exchange_result = await self._exchange_adapter.submit_order(order)
            
            if exchange_result.get("success"):
                order.exchange_order_id = exchange_result.get("order_id", "")
                order.status = OrderStatus(exchange_result.get("status", "open"))
                order.filled_quantity = exchange_result.get("filled_quantity", 0)
                order.average_fill_price = exchange_result.get("avg_price", 0)
                
                if order.is_filled:
                    order.filled_at = datetime.utcnow()
                    self._event_bus.publish(Event(
                        event_type=EventType.ORDER_FILLED,
                        source="order_manager",
                        data=order.to_dict(),
                    ))
                
                return ExecutionResult(success=True, order=order)
            else:
                return ExecutionResult(
                    success=False,
                    order=order,
                    error=exchange_result.get("error", "Unknown error"),
                )
                
        except Exception as e:
            return ExecutionResult(
                success=False,
                order=order,
                error=str(e),
            )
    
    def _calculate_slippage(
        self,
        expected_price: float,
        actual_price: float,
        side: Side,
    ) -> float:
        """Calculate slippage percentage."""
        if expected_price == 0:
            return 0.0
        
        diff = actual_price - expected_price
        slippage_pct = (diff / expected_price) * 100
        
        # For buys, positive slippage is bad (paid more)
        # For sells, negative slippage is bad (received less)
        if side == Side.BUY:
            return slippage_pct
        else:
            return -slippage_pct
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an active order."""
        order = self._active_orders.get(order_id)
        if not order:
            return False
        
        try:
            if self._exchange_adapter:
                await self._exchange_adapter.cancel_order(order.exchange_order_id)
            
            order.status = OrderStatus.CANCELLED
            order.cancelled_at = datetime.utcnow()
            
            self._active_orders.pop(order_id, None)
            self._completed_orders[order_id] = order
            
            self._event_bus.publish(Event(
                event_type=EventType.ORDER_CANCELLED,
                source="order_manager",
                data=order.to_dict(),
            ))
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to cancel order: {e}")
            return False
    
    async def cancel_all_orders(self, symbol: Optional[str] = None) -> int:
        """Cancel all active orders, optionally filtered by symbol."""
        cancelled = 0
        
        for order_id, order in list(self._active_orders.items()):
            if symbol and order.symbol != symbol:
                continue
            
            if await self.cancel_order(order_id):
                cancelled += 1
        
        return cancelled
    
    def on_order_update(
        self,
        exchange_order_id: str,
        status: str,
        filled_quantity: float,
        avg_price: float,
    ) -> None:
        """Handle order update from exchange."""
        # Find order by exchange ID
        order = None
        for o in list(self._active_orders.values()) + list(self._pending_orders.values()):
            if o.exchange_order_id == exchange_order_id:
                order = o
                break
        
        if not order:
            logger.warning(f"Received update for unknown order: {exchange_order_id}")
            return
        
        old_status = order.status
        order.status = OrderStatus(status)
        order.filled_quantity = filled_quantity
        order.average_fill_price = avg_price
        
        # Handle status transitions
        if order.status == OrderStatus.FILLED and old_status != OrderStatus.FILLED:
            order.filled_at = datetime.utcnow()
            self._active_orders.pop(order.order_id, None)
            self._completed_orders[order.order_id] = order
            
            self._event_bus.publish(Event(
                event_type=EventType.ORDER_FILLED,
                source="order_manager",
                data=order.to_dict(),
            ))
        
        elif order.status == OrderStatus.PARTIALLY_FILLED:
            self._event_bus.publish(Event(
                event_type=EventType.ORDER_PARTIAL_FILL,
                source="order_manager",
                data=order.to_dict(),
            ))
    
    def get_active_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all active orders."""
        orders = list(self._active_orders.values())
        if symbol:
            orders = [o for o in orders if o.symbol == symbol]
        return orders
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID."""
        return (
            self._pending_orders.get(order_id)
            or self._active_orders.get(order_id)
            or self._completed_orders.get(order_id)
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get order manager statistics."""
        fill_rate = (
            (self._successful_orders / self._total_orders * 100)
            if self._total_orders else 0
        )
        
        return {
            "total_orders": self._total_orders,
            "successful_orders": self._successful_orders,
            "failed_orders": self._failed_orders,
            "fill_rate_pct": fill_rate,
            "avg_slippage_pct": (
                self._total_slippage / self._successful_orders
                if self._successful_orders else 0
            ),
            "avg_execution_time_ms": self._avg_execution_time,
            "active_orders": len(self._active_orders),
            "pending_orders": len(self._pending_orders),
        }
