"""
Alert Manager
==============

Central alert management system.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
import logging

from trading_bot.core.events import Event, EventType, EventBus, get_event_bus

logger = logging.getLogger(__name__)


class AlertPriority(Enum):
    """Alert priority levels."""
    LOW = "low"           # Informational
    MEDIUM = "medium"     # Important but not urgent
    HIGH = "high"         # Urgent, requires attention soon
    CRITICAL = "critical" # Immediate action required


class AlertCategory(Enum):
    """Alert categories."""
    TRADE = "trade"
    RISK = "risk"
    SYSTEM = "system"
    PERFORMANCE = "performance"
    ERROR = "error"


@dataclass
class Alert:
    """Alert message."""
    alert_id: str
    category: AlertCategory
    priority: AlertPriority
    title: str
    message: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    data: Dict[str, Any] = field(default_factory=dict)
    acknowledged: bool = False
    sent: bool = False
    send_channels: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "category": self.category.value,
            "priority": self.priority.value,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "acknowledged": self.acknowledged,
        }
    
    def format_message(self, include_timestamp: bool = True) -> str:
        """Format alert for display."""
        emoji = {
            AlertPriority.LOW: "ℹ️",
            AlertPriority.MEDIUM: "⚠️",
            AlertPriority.HIGH: "🔴",
            AlertPriority.CRITICAL: "🚨",
        }.get(self.priority, "")
        
        lines = [f"{emoji} [{self.priority.value.upper()}] {self.title}"]
        
        if include_timestamp:
            lines.append(f"Time: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        lines.append(f"\n{self.message}")
        
        if self.data:
            lines.append("\nDetails:")
            for key, value in self.data.items():
                lines.append(f"  • {key}: {value}")
        
        return "\n".join(lines)


class AlertManager:
    """
    Central alert management.
    
    Features:
    - Multiple notification channels
    - Priority-based routing
    - Alert deduplication
    - Rate limiting
    - Alert history
    """
    
    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        rate_limit_seconds: int = 60,
        max_alerts_per_minute: int = 10,
    ):
        self._event_bus = event_bus or get_event_bus()
        self._rate_limit_seconds = rate_limit_seconds
        self._max_alerts_per_minute = max_alerts_per_minute
        
        # Notification channels
        self._channels: Dict[str, 'NotificationChannel'] = {}
        
        # Alert tracking
        self._alert_history: List[Alert] = []
        self._recent_alerts: Dict[str, datetime] = {}  # For deduplication
        self._alerts_this_minute: int = 0
        self._last_minute_reset: datetime = datetime.utcnow()
        
        # Priority routing
        self._priority_channels: Dict[AlertPriority, List[str]] = {
            AlertPriority.LOW: [],
            AlertPriority.MEDIUM: [],
            AlertPriority.HIGH: [],
            AlertPriority.CRITICAL: [],
        }
        
        # Category subscriptions
        self._category_channels: Dict[AlertCategory, List[str]] = {
            category: [] for category in AlertCategory
        }
        
        # Subscribe to events
        self._subscribe_events()
        
        # Alert counter
        self._alert_counter = 0
    
    def _subscribe_events(self) -> None:
        """Subscribe to relevant events."""
        # Trade events
        self._event_bus.subscribe(EventType.ORDER_FILLED, self._on_order_filled)
        self._event_bus.subscribe(EventType.POSITION_OPENED, self._on_position_opened)
        self._event_bus.subscribe(EventType.POSITION_CLOSED, self._on_position_closed)
        
        # Risk events
        self._event_bus.subscribe(EventType.RISK_LIMIT_BREACH, self._on_risk_breach)
        self._event_bus.subscribe(EventType.DAILY_LOSS_LIMIT, self._on_daily_loss_limit)
        self._event_bus.subscribe(EventType.PANIC_STOP, self._on_panic_stop)
        
        # System events
        self._event_bus.subscribe(EventType.BOT_ERROR, self._on_bot_error)
        self._event_bus.subscribe(EventType.CONNECTION_LOST, self._on_connection_lost)
        self._event_bus.subscribe(EventType.BOT_STARTED, self._on_bot_started)
        self._event_bus.subscribe(EventType.BOT_STOPPED, self._on_bot_stopped)
    
    def register_channel(
        self,
        name: str,
        channel: 'NotificationChannel',
        priorities: Optional[List[AlertPriority]] = None,
        categories: Optional[List[AlertCategory]] = None,
    ) -> None:
        """
        Register a notification channel.
        
        Args:
            name: Channel identifier
            channel: NotificationChannel implementation
            priorities: Which priorities to route to this channel
            categories: Which categories to route to this channel
        """
        self._channels[name] = channel
        
        # Default to all priorities/categories
        priorities = priorities or list(AlertPriority)
        categories = categories or list(AlertCategory)
        
        for priority in priorities:
            if name not in self._priority_channels[priority]:
                self._priority_channels[priority].append(name)
        
        for category in categories:
            if name not in self._category_channels[category]:
                self._category_channels[category].append(name)
        
        logger.info(f"Registered alert channel: {name}")
    
    def send_alert(
        self,
        category: AlertCategory,
        priority: AlertPriority,
        title: str,
        message: str,
        data: Optional[Dict[str, Any]] = None,
        dedupe_key: Optional[str] = None,
    ) -> Optional[str]:
        """
        Send an alert.
        
        Args:
            category: Alert category
            priority: Alert priority
            title: Alert title
            message: Alert message
            data: Additional data
            dedupe_key: Key for deduplication
        
        Returns:
            Alert ID if sent, None if filtered
        """
        # Rate limiting check
        if not self._check_rate_limit():
            logger.warning("Alert rate limit exceeded")
            return None
        
        # Deduplication check
        dedupe_key = dedupe_key or f"{category.value}:{title}"
        if not self._check_deduplication(dedupe_key):
            logger.debug(f"Alert deduplicated: {dedupe_key}")
            return None
        
        # Create alert
        self._alert_counter += 1
        alert = Alert(
            alert_id=f"alert_{self._alert_counter}_{datetime.utcnow().timestamp()}",
            category=category,
            priority=priority,
            title=title,
            message=message,
            data=data or {},
        )
        
        # Determine channels
        channels = self._get_channels_for_alert(alert)
        alert.send_channels = channels
        
        # Send to channels
        for channel_name in channels:
            channel = self._channels.get(channel_name)
            if channel:
                try:
                    success = channel.send(alert)
                    if success:
                        alert.sent = True
                except Exception as e:
                    logger.error(f"Error sending to channel {channel_name}: {e}")
        
        # Store in history
        self._alert_history.append(alert)
        self._recent_alerts[dedupe_key] = datetime.utcnow()
        
        # Clean old history
        self._cleanup_history()
        
        return alert.alert_id
    
    def _check_rate_limit(self) -> bool:
        """Check if we're within rate limits."""
        now = datetime.utcnow()
        
        # Reset counter every minute
        if (now - self._last_minute_reset).seconds >= 60:
            self._alerts_this_minute = 0
            self._last_minute_reset = now
        
        if self._alerts_this_minute >= self._max_alerts_per_minute:
            return False
        
        self._alerts_this_minute += 1
        return True
    
    def _check_deduplication(self, dedupe_key: str) -> bool:
        """Check if this alert should be deduplicated."""
        last_sent = self._recent_alerts.get(dedupe_key)
        
        if last_sent:
            if (datetime.utcnow() - last_sent).seconds < self._rate_limit_seconds:
                return False
        
        return True
    
    def _get_channels_for_alert(self, alert: Alert) -> List[str]:
        """Get channels that should receive this alert."""
        # Get channels by priority
        priority_channels = set(self._priority_channels.get(alert.priority, []))
        
        # Get channels by category
        category_channels = set(self._category_channels.get(alert.category, []))
        
        # Intersection (channel must be subscribed to both)
        return list(priority_channels & category_channels)
    
    def _cleanup_history(self, max_age_hours: int = 24) -> None:
        """Clean up old alerts from history."""
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
        
        self._alert_history = [
            alert for alert in self._alert_history
            if alert.timestamp > cutoff
        ]
        
        self._recent_alerts = {
            key: ts for key, ts in self._recent_alerts.items()
            if ts > cutoff
        }
    
    # =========== Event Handlers ===========
    
    def _on_order_filled(self, event: Event) -> None:
        """Handle order filled event."""
        self.send_alert(
            category=AlertCategory.TRADE,
            priority=AlertPriority.MEDIUM,
            title="Order Filled",
            message=f"Order {event.data.get('id')} filled for {event.data.get('symbol')}",
            data=event.data,
            dedupe_key=f"order_filled:{event.data.get('id')}",
        )
    
    def _on_position_opened(self, event: Event) -> None:
        """Handle position opened event."""
        self.send_alert(
            category=AlertCategory.TRADE,
            priority=AlertPriority.MEDIUM,
            title="Position Opened",
            message=f"New {event.data.get('side')} position in {event.data.get('symbol')}",
            data=event.data,
        )
    
    def _on_position_closed(self, event: Event) -> None:
        """Handle position closed event."""
        pnl = event.data.get('pnl', 0)
        priority = AlertPriority.MEDIUM if pnl >= 0 else AlertPriority.HIGH
        
        self.send_alert(
            category=AlertCategory.TRADE,
            priority=priority,
            title="Position Closed",
            message=f"Position in {event.data.get('symbol')} closed. PnL: {pnl}",
            data=event.data,
        )
    
    def _on_risk_breach(self, event: Event) -> None:
        """Handle risk limit breach."""
        self.send_alert(
            category=AlertCategory.RISK,
            priority=AlertPriority.HIGH,
            title="Risk Limit Breach",
            message=f"Risk limit breached: {event.data.get('reason')}",
            data=event.data,
        )
    
    def _on_daily_loss_limit(self, event: Event) -> None:
        """Handle daily loss limit hit."""
        self.send_alert(
            category=AlertCategory.RISK,
            priority=AlertPriority.CRITICAL,
            title="Daily Loss Limit Hit",
            message="Daily loss limit reached. Trading halted.",
            data=event.data,
        )
    
    def _on_panic_stop(self, event: Event) -> None:
        """Handle panic stop triggered."""
        self.send_alert(
            category=AlertCategory.RISK,
            priority=AlertPriority.CRITICAL,
            title="PANIC STOP TRIGGERED",
            message="Emergency stop activated. All positions being closed.",
            data=event.data,
        )
    
    def _on_bot_error(self, event: Event) -> None:
        """Handle bot error."""
        self.send_alert(
            category=AlertCategory.ERROR,
            priority=AlertPriority.HIGH,
            title="Bot Error",
            message=f"Error: {event.data.get('error')}",
            data=event.data,
        )
    
    def _on_connection_lost(self, event: Event) -> None:
        """Handle connection lost."""
        self.send_alert(
            category=AlertCategory.SYSTEM,
            priority=AlertPriority.HIGH,
            title="Connection Lost",
            message=f"Lost connection to {event.data.get('exchange', 'exchange')}",
            data=event.data,
        )
    
    def _on_bot_started(self, event: Event) -> None:
        """Handle bot started."""
        self.send_alert(
            category=AlertCategory.SYSTEM,
            priority=AlertPriority.LOW,
            title="Bot Started",
            message="Trading bot has started",
            data=event.data,
        )
    
    def _on_bot_stopped(self, event: Event) -> None:
        """Handle bot stopped."""
        self.send_alert(
            category=AlertCategory.SYSTEM,
            priority=AlertPriority.MEDIUM,
            title="Bot Stopped",
            message="Trading bot has stopped",
            data=event.data,
        )
    
    # =========== Convenience Methods ===========
    
    def alert_trade(
        self,
        title: str,
        message: str,
        priority: AlertPriority = AlertPriority.MEDIUM,
        **data,
    ) -> Optional[str]:
        """Send a trade-related alert."""
        return self.send_alert(AlertCategory.TRADE, priority, title, message, data)
    
    def alert_risk(
        self,
        title: str,
        message: str,
        priority: AlertPriority = AlertPriority.HIGH,
        **data,
    ) -> Optional[str]:
        """Send a risk-related alert."""
        return self.send_alert(AlertCategory.RISK, priority, title, message, data)
    
    def alert_error(
        self,
        title: str,
        message: str,
        priority: AlertPriority = AlertPriority.HIGH,
        **data,
    ) -> Optional[str]:
        """Send an error alert."""
        return self.send_alert(AlertCategory.ERROR, priority, title, message, data)
    
    def alert_system(
        self,
        title: str,
        message: str,
        priority: AlertPriority = AlertPriority.LOW,
        **data,
    ) -> Optional[str]:
        """Send a system alert."""
        return self.send_alert(AlertCategory.SYSTEM, priority, title, message, data)
    
    def get_recent_alerts(
        self,
        category: Optional[AlertCategory] = None,
        priority: Optional[AlertPriority] = None,
        limit: int = 50,
    ) -> List[Alert]:
        """Get recent alerts with optional filters."""
        alerts = self._alert_history
        
        if category:
            alerts = [a for a in alerts if a.category == category]
        
        if priority:
            alerts = [a for a in alerts if a.priority == priority]
        
        return sorted(alerts, key=lambda a: a.timestamp, reverse=True)[:limit]


# Import at end to avoid circular imports
from trading_bot.alerts.channels import NotificationChannel
