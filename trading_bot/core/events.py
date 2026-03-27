"""
Event System
=============

Central event bus for decoupled communication between components.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Types of events in the trading system."""
    # Market Data Events
    TICK = "tick"
    CANDLE = "candle"
    ORDERBOOK = "orderbook"
    TRADE = "trade"
    
    # Signal Events
    SIGNAL_GENERATED = "signal_generated"
    SIGNAL_FILTERED = "signal_filtered"
    SIGNAL_APPROVED = "signal_approved"
    SIGNAL_REJECTED = "signal_rejected"
    
    # Order Events
    ORDER_CREATED = "order_created"
    ORDER_SUBMITTED = "order_submitted"
    ORDER_FILLED = "order_filled"
    ORDER_PARTIAL_FILL = "order_partial_fill"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_REJECTED = "order_rejected"
    ORDER_EXPIRED = "order_expired"
    
    # Position Events
    POSITION_OPENED = "position_opened"
    POSITION_UPDATED = "position_updated"
    POSITION_CLOSED = "position_closed"
    POSITION_STOPPED = "position_stopped"
    
    # Risk Events
    RISK_CHECK_PASSED = "risk_check_passed"
    RISK_CHECK_FAILED = "risk_check_failed"
    RISK_LIMIT_BREACH = "risk_limit_breach"
    DAILY_LOSS_LIMIT = "daily_loss_limit"
    DRAWDOWN_LIMIT = "drawdown_limit"
    PANIC_STOP = "panic_stop"
    
    # Strategy Events
    STRATEGY_LOADED = "strategy_loaded"
    STRATEGY_STARTED = "strategy_started"
    STRATEGY_STOPPED = "strategy_stopped"
    STRATEGY_ERROR = "strategy_error"
    
    # Regime Events
    REGIME_CHANGED = "regime_changed"
    SESSION_CHANGED = "session_changed"
    NEWS_BLACKOUT_START = "news_blackout_start"
    NEWS_BLACKOUT_END = "news_blackout_end"
    
    # System Events
    BOT_STARTED = "bot_started"
    BOT_STOPPED = "bot_stopped"
    BOT_ERROR = "bot_error"
    CONNECTION_LOST = "connection_lost"
    CONNECTION_RESTORED = "connection_restored"
    STATE_SAVED = "state_saved"
    STATE_RESTORED = "state_restored"
    
    # Alert Events
    ALERT_INFO = "alert_info"
    ALERT_WARNING = "alert_warning"
    ALERT_ERROR = "alert_error"
    ALERT_CRITICAL = "alert_critical"


@dataclass
class Event:
    """Base event class."""
    event_type: EventType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    source: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    
    def __post_init__(self):
        if not self.correlation_id:
            self.correlation_id = f"{self.event_type.value}_{self.timestamp.timestamp()}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "data": self.data,
            "correlation_id": self.correlation_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Create event from dictionary."""
        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            source=data.get("source", ""),
            data=data.get("data", {}),
            correlation_id=data.get("correlation_id"),
        )


# Type alias for event handlers
EventHandler = Callable[[Event], None]
AsyncEventHandler = Callable[[Event], Any]


class EventBus:
    """
    Central event bus for the trading system.
    
    Supports both synchronous and asynchronous handlers.
    """
    
    def __init__(self):
        self._handlers: Dict[EventType, List[EventHandler]] = {}
        self._async_handlers: Dict[EventType, List[AsyncEventHandler]] = {}
        self._global_handlers: List[EventHandler] = []
        self._async_global_handlers: List[AsyncEventHandler] = []
        self._event_history: List[Event] = []
        self._max_history: int = 10000
        self._paused: bool = False
        self._filters: Dict[EventType, List[Callable[[Event], bool]]] = {}
    
    def subscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """Subscribe to a specific event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
        logger.debug(f"Handler subscribed to {event_type.value}")
    
    def subscribe_async(self, event_type: EventType, handler: AsyncEventHandler) -> None:
        """Subscribe an async handler to a specific event type."""
        if event_type not in self._async_handlers:
            self._async_handlers[event_type] = []
        self._async_handlers[event_type].append(handler)
        logger.debug(f"Async handler subscribed to {event_type.value}")
    
    def subscribe_all(self, handler: EventHandler) -> None:
        """Subscribe to all events."""
        self._global_handlers.append(handler)
    
    def subscribe_all_async(self, handler: AsyncEventHandler) -> None:
        """Subscribe an async handler to all events."""
        self._async_global_handlers.append(handler)
    
    def unsubscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """Unsubscribe from a specific event type."""
        if event_type in self._handlers:
            try:
                self._handlers[event_type].remove(handler)
            except ValueError:
                pass
    
    def add_filter(self, event_type: EventType, filter_func: Callable[[Event], bool]) -> None:
        """Add a filter for an event type. Event is only published if filter returns True."""
        if event_type not in self._filters:
            self._filters[event_type] = []
        self._filters[event_type].append(filter_func)
    
    def publish(self, event: Event) -> None:
        """Publish an event synchronously."""
        if self._paused:
            return
        
        # Apply filters
        if event.event_type in self._filters:
            for filter_func in self._filters[event.event_type]:
                if not filter_func(event):
                    logger.debug(f"Event {event.event_type.value} filtered out")
                    return
        
        # Store in history
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]
        
        # Call specific handlers
        if event.event_type in self._handlers:
            for handler in self._handlers[event.event_type]:
                try:
                    handler(event)
                except Exception as e:
                    logger.error(f"Error in event handler: {e}", exc_info=True)
        
        # Call global handlers
        for handler in self._global_handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Error in global event handler: {e}", exc_info=True)
    
    async def publish_async(self, event: Event) -> None:
        """Publish an event asynchronously."""
        if self._paused:
            return
        
        # Apply filters
        if event.event_type in self._filters:
            for filter_func in self._filters[event.event_type]:
                if not filter_func(event):
                    return
        
        # Store in history
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]
        
        tasks = []
        
        # Call async handlers
        if event.event_type in self._async_handlers:
            for handler in self._async_handlers[event.event_type]:
                tasks.append(self._safe_async_call(handler, event))
        
        # Call async global handlers
        for handler in self._async_global_handlers:
            tasks.append(self._safe_async_call(handler, event))
        
        # Call sync handlers
        self.publish(event)
        
        # Wait for async handlers
        if tasks:
            await asyncio.gather(*tasks)
    
    async def _safe_async_call(self, handler: AsyncEventHandler, event: Event) -> None:
        """Safely call an async handler."""
        try:
            await handler(event)
        except Exception as e:
            logger.error(f"Error in async event handler: {e}", exc_info=True)
    
    def pause(self) -> None:
        """Pause event publishing."""
        self._paused = True
        logger.info("Event bus paused")
    
    def resume(self) -> None:
        """Resume event publishing."""
        self._paused = False
        logger.info("Event bus resumed")
    
    def get_history(
        self,
        event_type: Optional[EventType] = None,
        limit: int = 100,
        source: Optional[str] = None,
    ) -> List[Event]:
        """Get event history with optional filtering."""
        events = self._event_history
        
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        
        if source:
            events = [e for e in events if e.source == source]
        
        return events[-limit:]
    
    def clear_history(self) -> None:
        """Clear event history."""
        self._event_history.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics."""
        event_counts: Dict[str, int] = {}
        for event in self._event_history:
            event_type = event.event_type.value
            event_counts[event_type] = event_counts.get(event_type, 0) + 1
        
        return {
            "total_events": len(self._event_history),
            "event_counts": event_counts,
            "handlers_count": sum(len(h) for h in self._handlers.values()),
            "async_handlers_count": sum(len(h) for h in self._async_handlers.values()),
            "global_handlers_count": len(self._global_handlers),
            "paused": self._paused,
        }


# Global event bus instance
_event_bus: Optional[EventBus] = None


def get_event_bus() -> EventBus:
    """Get the global event bus instance."""
    global _event_bus
    if _event_bus is None:
        _event_bus = EventBus()
    return _event_bus


def reset_event_bus() -> None:
    """Reset the global event bus (mainly for testing)."""
    global _event_bus
    _event_bus = None
