"""
State Manager
==============

Manages bot state persistence for fault tolerance.
The bot must remember who it is after restarts.
"""

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
import logging
import copy

from trading_bot.persistence.store import StateStore, FileStore
from trading_bot.core.events import Event, EventType, EventBus, get_event_bus
from trading_bot.core.types import Order, Position, Signal

logger = logging.getLogger(__name__)


@dataclass
class BotState:
    """Complete bot state for persistence."""
    # Metadata
    bot_id: str
    version: str
    last_updated: str
    
    # Positions
    open_positions: Dict[str, Dict]  # position_id -> position dict
    
    # Orders
    pending_orders: Dict[str, Dict]  # order_id -> order dict
    active_orders: Dict[str, Dict]
    
    # Signals
    last_signals: Dict[str, Dict]  # strategy_id -> last signal
    pending_signals: Dict[str, Dict]
    
    # Risk state
    daily_pnl: float
    daily_trades: int
    consecutive_losses: int
    peak_equity: float
    current_drawdown: float
    
    # Strategy state
    strategy_states: Dict[str, Dict]  # strategy_id -> state dict
    
    # Session info
    session_start: str
    total_trades_session: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BotState':
        """Create from dictionary."""
        return cls(**data)
    
    @classmethod
    def create_empty(cls, bot_id: str, version: str) -> 'BotState':
        """Create empty state."""
        return cls(
            bot_id=bot_id,
            version=version,
            last_updated=datetime.utcnow().isoformat(),
            open_positions={},
            pending_orders={},
            active_orders={},
            last_signals={},
            pending_signals={},
            daily_pnl=0.0,
            daily_trades=0,
            consecutive_losses=0,
            peak_equity=0.0,
            current_drawdown=0.0,
            strategy_states={},
            session_start=datetime.utcnow().isoformat(),
            total_trades_session=0,
        )


class StateManager:
    """
    Manages bot state persistence.
    
    Responsibilities:
    - Save and restore complete bot state
    - Track state changes
    - Handle state versioning
    - Provide atomic updates
    """
    
    def __init__(
        self,
        store: Optional[StateStore] = None,
        bot_id: str = "trading_bot",
        version: str = "1.0.0",
        event_bus: Optional[EventBus] = None,
        auto_save_interval: int = 60,
    ):
        self._store = store or FileStore()
        self._bot_id = bot_id
        self._version = version
        self._event_bus = event_bus or get_event_bus()
        self._auto_save_interval = auto_save_interval
        
        # Current state
        self._state: Optional[BotState] = None
        self._dirty = False
        self._last_save: Optional[datetime] = None
        
        # State history for rollback
        self._state_history: List[BotState] = []
        self._max_history = 10
        
        # Subscribe to events
        self._subscribe_events()
    
    def _subscribe_events(self) -> None:
        """Subscribe to relevant events for state tracking."""
        self._event_bus.subscribe(EventType.POSITION_OPENED, self._on_position_opened)
        self._event_bus.subscribe(EventType.POSITION_CLOSED, self._on_position_closed)
        self._event_bus.subscribe(EventType.ORDER_CREATED, self._on_order_created)
        self._event_bus.subscribe(EventType.ORDER_FILLED, self._on_order_filled)
        self._event_bus.subscribe(EventType.ORDER_CANCELLED, self._on_order_cancelled)
    
    def initialize(self) -> bool:
        """
        Initialize state manager.
        
        Attempts to restore previous state, or creates new state.
        """
        try:
            # Try to restore previous state
            saved_state = self._store.load("bot_state", category="state")
            
            if saved_state:
                self._state = BotState.from_dict(saved_state)
                logger.info(f"Restored bot state from {self._state.last_updated}")
                
                # Validate restored state
                if not self._validate_state():
                    logger.warning("Restored state validation failed, creating new state")
                    self._state = BotState.create_empty(self._bot_id, self._version)
                else:
                    self._event_bus.publish(Event(
                        event_type=EventType.STATE_RESTORED,
                        source="state_manager",
                        data={"positions": len(self._state.open_positions)},
                    ))
            else:
                self._state = BotState.create_empty(self._bot_id, self._version)
                logger.info("Created new bot state")
            
            return True
            
        except Exception as e:
            logger.error(f"Error initializing state manager: {e}")
            self._state = BotState.create_empty(self._bot_id, self._version)
            return False
    
    def _validate_state(self) -> bool:
        """Validate restored state integrity."""
        if not self._state:
            return False
        
        # Check version compatibility
        # In production, implement proper version migration
        
        # Validate positions have required fields
        for pos_id, pos in self._state.open_positions.items():
            if not all(k in pos for k in ["symbol", "side", "quantity"]):
                logger.warning(f"Invalid position data for {pos_id}")
                return False
        
        return True
    
    def save(self, force: bool = False) -> bool:
        """
        Save current state to storage.
        
        Args:
            force: Force save even if not dirty
        """
        if not self._state:
            return False
        
        if not self._dirty and not force:
            return True
        
        try:
            # Save to history for rollback
            self._save_to_history()
            
            # Update timestamp
            self._state.last_updated = datetime.utcnow().isoformat()
            
            # Save to store
            success = self._store.save(
                "bot_state",
                self._state.to_dict(),
                category="state"
            )
            
            if success:
                self._dirty = False
                self._last_save = datetime.utcnow()
                
                self._event_bus.publish(Event(
                    event_type=EventType.STATE_SAVED,
                    source="state_manager",
                    data={"timestamp": self._last_save.isoformat()},
                ))
                
                logger.debug("State saved successfully")
            
            return success
            
        except Exception as e:
            logger.error(f"Error saving state: {e}")
            return False
    
    def _save_to_history(self) -> None:
        """Save current state to history for rollback."""
        if self._state:
            state_copy = BotState.from_dict(copy.deepcopy(self._state.to_dict()))
            self._state_history.append(state_copy)
            
            # Trim history
            if len(self._state_history) > self._max_history:
                self._state_history = self._state_history[-self._max_history:]
    
    def rollback(self) -> bool:
        """Rollback to previous state."""
        if not self._state_history:
            logger.warning("No state history available for rollback")
            return False
        
        try:
            self._state = self._state_history.pop()
            self._dirty = True
            logger.info(f"Rolled back to state from {self._state.last_updated}")
            return True
            
        except Exception as e:
            logger.error(f"Error rolling back state: {e}")
            return False
    
    # =========== Position Management ===========
    
    def add_position(self, position: Position) -> None:
        """Add a new position to state."""
        if self._state:
            self._state.open_positions[position.id] = position.to_dict()
            self._dirty = True
    
    def update_position(self, position: Position) -> None:
        """Update an existing position."""
        if self._state and position.id in self._state.open_positions:
            self._state.open_positions[position.id] = position.to_dict()
            self._dirty = True
    
    def remove_position(self, position_id: str) -> None:
        """Remove a position from state."""
        if self._state and position_id in self._state.open_positions:
            del self._state.open_positions[position_id]
            self._dirty = True
    
    def get_open_positions(self) -> List[Dict]:
        """Get all open positions."""
        if self._state:
            return list(self._state.open_positions.values())
        return []
    
    # =========== Order Management ===========
    
    def add_order(self, order: Order, pending: bool = True) -> None:
        """Add an order to state."""
        if self._state:
            if pending:
                self._state.pending_orders[order.id] = order.to_dict()
            else:
                self._state.active_orders[order.id] = order.to_dict()
            self._dirty = True
    
    def update_order(self, order: Order) -> None:
        """Update an existing order."""
        if not self._state:
            return
        
        order_dict = order.to_dict()
        
        if order.id in self._state.pending_orders:
            self._state.pending_orders[order.id] = order_dict
        elif order.id in self._state.active_orders:
            self._state.active_orders[order.id] = order_dict
        
        self._dirty = True
    
    def remove_order(self, order_id: str) -> None:
        """Remove an order from state."""
        if not self._state:
            return
        
        if order_id in self._state.pending_orders:
            del self._state.pending_orders[order_id]
        elif order_id in self._state.active_orders:
            del self._state.active_orders[order_id]
        
        self._dirty = True
    
    def move_order_to_active(self, order_id: str) -> None:
        """Move order from pending to active."""
        if not self._state:
            return
        
        if order_id in self._state.pending_orders:
            self._state.active_orders[order_id] = self._state.pending_orders.pop(order_id)
            self._dirty = True
    
    # =========== Signal Management ===========
    
    def record_signal(self, strategy_id: str, signal: Signal) -> None:
        """Record the last signal from a strategy."""
        if self._state:
            self._state.last_signals[strategy_id] = signal.to_dict()
            self._dirty = True
    
    def get_last_signal(self, strategy_id: str) -> Optional[Dict]:
        """Get the last signal from a strategy."""
        if self._state:
            return self._state.last_signals.get(strategy_id)
        return None
    
    # =========== Risk State ===========
    
    def update_risk_state(
        self,
        daily_pnl: Optional[float] = None,
        daily_trades: Optional[int] = None,
        consecutive_losses: Optional[int] = None,
        peak_equity: Optional[float] = None,
        current_drawdown: Optional[float] = None,
    ) -> None:
        """Update risk-related state."""
        if not self._state:
            return
        
        if daily_pnl is not None:
            self._state.daily_pnl = daily_pnl
        if daily_trades is not None:
            self._state.daily_trades = daily_trades
        if consecutive_losses is not None:
            self._state.consecutive_losses = consecutive_losses
        if peak_equity is not None:
            self._state.peak_equity = peak_equity
        if current_drawdown is not None:
            self._state.current_drawdown = current_drawdown
        
        self._dirty = True
    
    def get_risk_state(self) -> Dict[str, Any]:
        """Get current risk state."""
        if self._state:
            return {
                "daily_pnl": self._state.daily_pnl,
                "daily_trades": self._state.daily_trades,
                "consecutive_losses": self._state.consecutive_losses,
                "peak_equity": self._state.peak_equity,
                "current_drawdown": self._state.current_drawdown,
            }
        return {}
    
    # =========== Strategy State ===========
    
    def save_strategy_state(self, strategy_id: str, state: Dict[str, Any]) -> None:
        """Save strategy-specific state."""
        if self._state:
            self._state.strategy_states[strategy_id] = state
            self._dirty = True
    
    def get_strategy_state(self, strategy_id: str) -> Optional[Dict[str, Any]]:
        """Get strategy-specific state."""
        if self._state:
            return self._state.strategy_states.get(strategy_id)
        return None
    
    # =========== Event Handlers ===========
    
    def _on_position_opened(self, event: Event) -> None:
        """Handle position opened event."""
        pos_data = event.data
        if self._state and "id" in pos_data:
            self._state.open_positions[pos_data["id"]] = pos_data
            self._dirty = True
    
    def _on_position_closed(self, event: Event) -> None:
        """Handle position closed event."""
        pos_data = event.data
        if self._state and "id" in pos_data:
            self._state.open_positions.pop(pos_data["id"], None)
            self._state.total_trades_session += 1
            self._dirty = True
    
    def _on_order_created(self, event: Event) -> None:
        """Handle order created event."""
        order_data = event.data
        if self._state and "id" in order_data:
            self._state.pending_orders[order_data["id"]] = order_data
            self._dirty = True
    
    def _on_order_filled(self, event: Event) -> None:
        """Handle order filled event."""
        order_data = event.data
        if self._state and "id" in order_data:
            self._state.pending_orders.pop(order_data["id"], None)
            self._state.active_orders.pop(order_data["id"], None)
            self._dirty = True
    
    def _on_order_cancelled(self, event: Event) -> None:
        """Handle order cancelled event."""
        order_data = event.data
        if self._state and "id" in order_data:
            self._state.pending_orders.pop(order_data["id"], None)
            self._state.active_orders.pop(order_data["id"], None)
            self._dirty = True
    
    # =========== Utilities ===========
    
    def reset_daily_stats(self) -> None:
        """Reset daily statistics (call at day boundary)."""
        if self._state:
            self._state.daily_pnl = 0.0
            self._state.daily_trades = 0
            self._dirty = True
            logger.info("Daily stats reset")
    
    def get_state_summary(self) -> Dict[str, Any]:
        """Get summary of current state."""
        if not self._state:
            return {}
        
        return {
            "bot_id": self._state.bot_id,
            "version": self._state.version,
            "last_updated": self._state.last_updated,
            "open_positions_count": len(self._state.open_positions),
            "pending_orders_count": len(self._state.pending_orders),
            "active_orders_count": len(self._state.active_orders),
            "daily_pnl": self._state.daily_pnl,
            "daily_trades": self._state.daily_trades,
            "consecutive_losses": self._state.consecutive_losses,
            "current_drawdown": self._state.current_drawdown,
        }
