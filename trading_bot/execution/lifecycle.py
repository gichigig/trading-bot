"""
Trade Lifecycle Manager
========================

Manages the complete lifecycle of a trade from entry to exit.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
import logging
import uuid

from trading_bot.core.types import (
    Position, Order, Trade, Signal, Side, OrderType, OrderStatus,
    PositionStatus, SignalType
)
from trading_bot.core.events import Event, EventType, EventBus, get_event_bus

logger = logging.getLogger(__name__)


@dataclass
class TakeProfit:
    """Take profit level configuration."""
    price: float
    quantity_pct: float  # Percentage of position to close
    order_id: Optional[str] = None
    filled: bool = False
    filled_at: Optional[datetime] = None


@dataclass
class TrailingStopConfig:
    """Trailing stop configuration."""
    enabled: bool = False
    distance: float = 0.0  # ATR multiplier or fixed distance
    distance_type: str = "atr"  # "atr" or "fixed" or "percent"
    activation_profit_pct: float = 0.0  # Activate after X% profit
    step: float = 0.0  # Minimum step to trail


@dataclass
class BreakEvenConfig:
    """Break-even configuration."""
    enabled: bool = False
    trigger_profit_pct: float = 1.0  # Move to BE after X% profit
    offset: float = 0.0  # Offset from entry (e.g., +2 pips)


@dataclass
class TimeBasedExitConfig:
    """Time-based exit configuration."""
    enabled: bool = False
    max_duration_minutes: int = 0
    exit_before_close_minutes: int = 0  # Exit before market close


class TradeLifecycleManager:
    """
    Manages trade lifecycle with:
    - Partial take profits
    - Trailing stops
    - Break-even logic
    - Time-based exits
    - Position scaling
    """
    
    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        on_order_request: Optional[Callable] = None,
    ):
        self._event_bus = event_bus or get_event_bus()
        self._on_order_request = on_order_request
        
        # Active positions under management
        self._positions: Dict[str, Position] = {}
        
        # Take profit levels per position
        self._take_profits: Dict[str, List[TakeProfit]] = {}
        
        # Trailing stop configs
        self._trailing_configs: Dict[str, TrailingStopConfig] = {}
        self._trailing_highs: Dict[str, float] = {}  # Best price for trailing
        self._trailing_lows: Dict[str, float] = {}
        
        # Break-even configs
        self._break_even_configs: Dict[str, BreakEvenConfig] = {}
        
        # Time-based exit configs
        self._time_configs: Dict[str, TimeBasedExitConfig] = {}
        
        # ATR values for trailing calculation
        self._atr_values: Dict[str, float] = {}
    
    def manage_position(
        self,
        position: Position,
        take_profits: Optional[List[Dict[str, float]]] = None,
        trailing_stop: Optional[TrailingStopConfig] = None,
        break_even: Optional[BreakEvenConfig] = None,
        time_exit: Optional[TimeBasedExitConfig] = None,
    ) -> None:
        """
        Start managing a position's lifecycle.
        
        Args:
            position: The position to manage
            take_profits: List of {price, quantity_pct} for partial TPs
            trailing_stop: Trailing stop configuration
            break_even: Break-even configuration
            time_exit: Time-based exit configuration
        """
        self._positions[position.position_id] = position
        
        # Set up take profits
        if take_profits:
            self._take_profits[position.position_id] = [
                TakeProfit(
                    price=tp.get("price", 0),
                    quantity_pct=tp.get("pct", tp.get("quantity_pct", 100)),
                )
                for tp in take_profits
            ]
        
        # Set up trailing stop
        if trailing_stop:
            self._trailing_configs[position.position_id] = trailing_stop
            if position.side == Side.BUY:
                self._trailing_highs[position.position_id] = position.entry_price
            else:
                self._trailing_lows[position.position_id] = position.entry_price
        
        # Set up break-even
        if break_even:
            self._break_even_configs[position.position_id] = break_even
        
        # Set up time-based exit
        if time_exit:
            self._time_configs[position.position_id] = time_exit
        
        logger.info(f"Managing position {position.position_id}: TPs={len(take_profits or [])}, "
                   f"trailing={trailing_stop is not None}, BE={break_even is not None}")
    
    def update_price(
        self,
        symbol: str,
        price: float,
        atr: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Update price and check for lifecycle events.
        
        Returns list of actions to take (orders to place/modify).
        """
        actions = []
        
        # Store ATR
        if atr:
            self._atr_values[symbol] = atr
        
        # Check all positions for this symbol
        for pos_id, position in list(self._positions.items()):
            if position.symbol != symbol:
                continue
            
            # Update position price
            position.update_price(price)
            
            # Check take profits
            tp_actions = self._check_take_profits(position, price)
            actions.extend(tp_actions)
            
            # Check trailing stop
            trail_action = self._check_trailing_stop(position, price)
            if trail_action:
                actions.append(trail_action)
            
            # Check break-even
            be_action = self._check_break_even(position, price)
            if be_action:
                actions.append(be_action)
        
        return actions
    
    def check_time_exits(self) -> List[Dict[str, Any]]:
        """
        Check for time-based exits.
        
        Should be called periodically (e.g., every minute).
        """
        actions = []
        now = datetime.utcnow()
        
        for pos_id, config in self._time_configs.items():
            if not config.enabled:
                continue
            
            position = self._positions.get(pos_id)
            if not position:
                continue
            
            # Check max duration
            if config.max_duration_minutes > 0:
                duration = (now - position.opened_at).total_seconds() / 60
                if duration >= config.max_duration_minutes:
                    actions.append({
                        "action": "close_position",
                        "position_id": pos_id,
                        "reason": f"Time exit: max duration ({config.max_duration_minutes}m) reached",
                    })
        
        return actions
    
    def _check_take_profits(
        self,
        position: Position,
        price: float,
    ) -> List[Dict[str, Any]]:
        """Check and trigger take profit levels."""
        actions = []
        take_profits = self._take_profits.get(position.position_id, [])
        
        for tp in take_profits:
            if tp.filled:
                continue
            
            # Check if price hit TP
            triggered = False
            if position.side == Side.BUY and price >= tp.price:
                triggered = True
            elif position.side == Side.SELL and price <= tp.price:
                triggered = True
            
            if triggered:
                quantity = position.quantity * (tp.quantity_pct / 100)
                
                actions.append({
                    "action": "partial_close",
                    "position_id": position.position_id,
                    "quantity": quantity,
                    "price": tp.price,
                    "reason": f"Take profit at {tp.price}",
                })
                
                tp.filled = True
                tp.filled_at = datetime.utcnow()
                
                logger.info(f"Take profit triggered: {position.symbol} @ {tp.price}")
        
        return actions
    
    def _check_trailing_stop(
        self,
        position: Position,
        price: float,
    ) -> Optional[Dict[str, Any]]:
        """Check and update trailing stop."""
        config = self._trailing_configs.get(position.position_id)
        if not config or not config.enabled:
            return None
        
        # Check activation condition
        if config.activation_profit_pct > 0:
            profit_pct = position.pnl_pct
            if profit_pct < config.activation_profit_pct:
                return None
            
            # Mark as activated
            if not position.trailing_stop_activated:
                position.trailing_stop_activated = True
                logger.info(f"Trailing stop activated for {position.symbol}")
        
        # Calculate trail distance
        if config.distance_type == "atr":
            atr = self._atr_values.get(position.symbol, 0)
            distance = atr * config.distance
        elif config.distance_type == "percent":
            distance = price * (config.distance / 100)
        else:
            distance = config.distance
        
        new_stop = None
        
        if position.side == Side.BUY:
            # Update high watermark
            high = self._trailing_highs.get(position.position_id, price)
            if price > high:
                self._trailing_highs[position.position_id] = price
                high = price
            
            # Calculate new stop
            potential_stop = high - distance
            current_stop = position.stop_loss or 0
            
            # Only move stop up (or set if not exists)
            if potential_stop > current_stop:
                # Check minimum step
                if config.step > 0 and current_stop > 0:
                    if potential_stop - current_stop < config.step:
                        return None
                
                new_stop = potential_stop
                
        else:  # SHORT
            # Update low watermark
            low = self._trailing_lows.get(position.position_id, price)
            if price < low:
                self._trailing_lows[position.position_id] = price
                low = price
            
            # Calculate new stop
            potential_stop = low + distance
            current_stop = position.stop_loss or float('inf')
            
            # Only move stop down
            if potential_stop < current_stop:
                if config.step > 0 and current_stop < float('inf'):
                    if current_stop - potential_stop < config.step:
                        return None
                
                new_stop = potential_stop
        
        if new_stop:
            return {
                "action": "modify_stop",
                "position_id": position.position_id,
                "new_stop": new_stop,
                "reason": f"Trailing stop adjustment",
            }
        
        return None
    
    def _check_break_even(
        self,
        position: Position,
        price: float,
    ) -> Optional[Dict[str, Any]]:
        """Check and trigger break-even."""
        config = self._break_even_configs.get(position.position_id)
        if not config or not config.enabled:
            return None
        
        # Already triggered
        if position.break_even_triggered:
            return None
        
        # Check profit threshold
        profit_pct = position.pnl_pct
        if profit_pct < config.trigger_profit_pct:
            return None
        
        # Calculate break-even stop
        if position.side == Side.BUY:
            be_stop = position.entry_price + config.offset
            # Only if better than current stop
            if position.stop_loss and be_stop <= position.stop_loss:
                return None
        else:
            be_stop = position.entry_price - config.offset
            if position.stop_loss and be_stop >= position.stop_loss:
                return None
        
        position.break_even_triggered = True
        
        return {
            "action": "modify_stop",
            "position_id": position.position_id,
            "new_stop": be_stop,
            "reason": f"Break-even triggered at {profit_pct:.1f}% profit",
        }
    
    def scale_in(
        self,
        position_id: str,
        additional_quantity: float,
        price: float,
    ) -> Optional[Dict[str, Any]]:
        """Add to an existing position."""
        position = self._positions.get(position_id)
        if not position:
            return None
        
        return {
            "action": "scale_in",
            "position_id": position_id,
            "quantity": additional_quantity,
            "price": price,
        }
    
    def scale_out(
        self,
        position_id: str,
        quantity: float,
        reason: str = "Manual scale out",
    ) -> Optional[Dict[str, Any]]:
        """Reduce position size."""
        position = self._positions.get(position_id)
        if not position:
            return None
        
        return {
            "action": "scale_out",
            "position_id": position_id,
            "quantity": quantity,
            "reason": reason,
        }
    
    def close_position(
        self,
        position_id: str,
        reason: str = "Manual close",
    ) -> Optional[Dict[str, Any]]:
        """Request full position close."""
        position = self._positions.get(position_id)
        if not position:
            return None
        
        return {
            "action": "close_position",
            "position_id": position_id,
            "reason": reason,
        }
    
    def on_position_updated(
        self,
        position_id: str,
        new_quantity: float,
        new_stop: Optional[float] = None,
    ) -> None:
        """Handle position update notification."""
        position = self._positions.get(position_id)
        if not position:
            return
        
        position.quantity = new_quantity
        if new_stop:
            position.stop_loss = new_stop
    
    def on_position_closed(self, position_id: str) -> None:
        """Handle position closed notification."""
        self._positions.pop(position_id, None)
        self._take_profits.pop(position_id, None)
        self._trailing_configs.pop(position_id, None)
        self._trailing_highs.pop(position_id, None)
        self._trailing_lows.pop(position_id, None)
        self._break_even_configs.pop(position_id, None)
        self._time_configs.pop(position_id, None)
    
    def get_position_status(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed status of a managed position."""
        position = self._positions.get(position_id)
        if not position:
            return None
        
        take_profits = self._take_profits.get(position_id, [])
        tp_status = [
            {
                "price": tp.price,
                "pct": tp.quantity_pct,
                "filled": tp.filled,
                "filled_at": tp.filled_at.isoformat() if tp.filled_at else None,
            }
            for tp in take_profits
        ]
        
        return {
            "position": position.to_dict(),
            "take_profits": tp_status,
            "trailing_stop": {
                "enabled": self._trailing_configs.get(position_id, TrailingStopConfig()).enabled,
                "activated": position.trailing_stop_activated,
                "high_watermark": self._trailing_highs.get(position_id),
                "low_watermark": self._trailing_lows.get(position_id),
            },
            "break_even": {
                "enabled": self._break_even_configs.get(position_id, BreakEvenConfig()).enabled,
                "triggered": position.break_even_triggered,
            },
            "time_exit": {
                "enabled": self._time_configs.get(position_id, TimeBasedExitConfig()).enabled,
                "duration_minutes": position.duration_minutes,
            },
        }
    
    def get_all_positions(self) -> List[Dict[str, Any]]:
        """Get status of all managed positions."""
        return [
            self.get_position_status(pos_id)
            for pos_id in self._positions
        ]
