"""
News Blackout Manager
======================

Manages news event blackout windows to avoid trading during high-impact events.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import logging

from trading_bot.core.events import Event, EventType, EventBus, get_event_bus

logger = logging.getLogger(__name__)


@dataclass
class NewsEvent:
    """Economic news event."""
    event_id: str
    title: str
    currency: str
    impact: str  # low, medium, high
    timestamp: datetime
    
    # Blackout window
    blackout_before_minutes: int = 30
    blackout_after_minutes: int = 15
    
    # Metadata
    actual: Optional[str] = None
    forecast: Optional[str] = None
    previous: Optional[str] = None
    
    @property
    def blackout_start(self) -> datetime:
        return self.timestamp - timedelta(minutes=self.blackout_before_minutes)
    
    @property
    def blackout_end(self) -> datetime:
        return self.timestamp + timedelta(minutes=self.blackout_after_minutes)
    
    def is_in_blackout(self, timestamp: Optional[datetime] = None) -> bool:
        now = timestamp or datetime.utcnow()
        return self.blackout_start <= now <= self.blackout_end
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "title": self.title,
            "currency": self.currency,
            "impact": self.impact,
            "timestamp": self.timestamp.isoformat(),
            "blackout_start": self.blackout_start.isoformat(),
            "blackout_end": self.blackout_end.isoformat(),
        }


class NewsBlackoutManager:
    """
    Manages news event blackout periods.
    
    Features:
    - Track upcoming high-impact events
    - Blackout windows before/after events
    - Symbol-specific blackouts based on currency
    - Manual blackout periods
    """
    
    def __init__(
        self,
        default_blackout_before: int = 30,
        default_blackout_after: int = 15,
        min_impact_level: str = "high",  # Only blackout for high impact by default
        event_bus: Optional[EventBus] = None,
    ):
        self.default_blackout_before = default_blackout_before
        self.default_blackout_after = default_blackout_after
        self.min_impact_level = min_impact_level
        
        self._event_bus = event_bus or get_event_bus()
        
        # Upcoming events
        self._events: List[NewsEvent] = []
        
        # Manual blackouts
        self._manual_blackouts: List[Dict[str, Any]] = []
        
        # Currency to symbol mapping
        self._currency_symbols: Dict[str, List[str]] = {
            "USD": ["EURUSD", "GBPUSD", "USDJPY", "USDCAD", "AUDUSD", "NZDUSD", "USDCHF", "BTCUSD", "ETHUSD"],
            "EUR": ["EURUSD", "EURGBP", "EURJPY", "EURAUD", "EURCHF"],
            "GBP": ["GBPUSD", "EURGBP", "GBPJPY", "GBPAUD"],
            "JPY": ["USDJPY", "EURJPY", "GBPJPY", "AUDJPY"],
            "AUD": ["AUDUSD", "EURAUD", "GBPAUD", "AUDJPY"],
            "CAD": ["USDCAD", "EURCAD", "GBPCAD"],
            "CHF": ["USDCHF", "EURCHF", "GBPCHF"],
            "NZD": ["NZDUSD", "EURNZD", "GBPNZD"],
            "BTC": ["BTCUSD", "BTCEUR"],
            "ETH": ["ETHUSD", "ETHEUR", "ETHBTC"],
        }
        
        # Impact level ordering
        self._impact_levels = {"low": 1, "medium": 2, "high": 3}
        
        # Current blackout state
        self._in_blackout = False
        self._current_blackout_events: List[NewsEvent] = []
    
    def add_event(
        self,
        event_id: str,
        title: str,
        currency: str,
        timestamp: datetime,
        impact: str = "high",
        blackout_before: Optional[int] = None,
        blackout_after: Optional[int] = None,
    ) -> NewsEvent:
        """Add a news event."""
        event = NewsEvent(
            event_id=event_id,
            title=title,
            currency=currency.upper(),
            impact=impact.lower(),
            timestamp=timestamp,
            blackout_before_minutes=blackout_before or self.default_blackout_before,
            blackout_after_minutes=blackout_after or self.default_blackout_after,
        )
        
        self._events.append(event)
        self._events.sort(key=lambda e: e.timestamp)
        
        logger.info(f"Added news event: {title} ({currency}) at {timestamp}")
        return event
    
    def remove_event(self, event_id: str) -> bool:
        """Remove a news event."""
        for i, event in enumerate(self._events):
            if event.event_id == event_id:
                del self._events[i]
                return True
        return False
    
    def add_manual_blackout(
        self,
        start: datetime,
        end: datetime,
        reason: str = "Manual blackout",
        symbols: Optional[List[str]] = None,
    ) -> None:
        """Add a manual blackout period."""
        self._manual_blackouts.append({
            "start": start,
            "end": end,
            "reason": reason,
            "symbols": symbols,  # None means all symbols
        })
        logger.info(f"Added manual blackout: {reason} ({start} to {end})")
    
    def is_in_blackout(
        self,
        symbol: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> tuple:
        """
        Check if currently in a blackout period.
        
        Args:
            symbol: Optional symbol to check (filters by currency)
            timestamp: Optional timestamp (defaults to now)
            
        Returns:
            (is_blackout, reason, events)
        """
        now = timestamp or datetime.utcnow()
        blackout_events = []
        
        # Check manual blackouts
        for blackout in self._manual_blackouts:
            if blackout["start"] <= now <= blackout["end"]:
                if symbol is None or blackout["symbols"] is None or symbol in blackout["symbols"]:
                    return True, blackout["reason"], []
        
        # Check news events
        min_level = self._impact_levels.get(self.min_impact_level, 3)
        
        for event in self._events:
            event_level = self._impact_levels.get(event.impact, 0)
            
            # Skip if below minimum impact
            if event_level < min_level:
                continue
            
            # Check if symbol is affected
            if symbol:
                affected_symbols = self._currency_symbols.get(event.currency, [])
                if symbol not in affected_symbols:
                    continue
            
            # Check blackout window
            if event.is_in_blackout(now):
                blackout_events.append(event)
        
        if blackout_events:
            reasons = [f"{e.title} ({e.currency})" for e in blackout_events]
            return True, f"News blackout: {', '.join(reasons)}", blackout_events
        
        return False, "", []
    
    def update(self, timestamp: Optional[datetime] = None) -> None:
        """Update blackout state and trigger events."""
        now = timestamp or datetime.utcnow()
        
        is_blackout, reason, events = self.is_in_blackout(timestamp=now)
        
        # Check for state change
        if is_blackout and not self._in_blackout:
            self._in_blackout = True
            self._current_blackout_events = events
            
            self._event_bus.publish(Event(
                event_type=EventType.NEWS_BLACKOUT_START,
                source="news_blackout_manager",
                data={
                    "reason": reason,
                    "events": [e.to_dict() for e in events],
                },
            ))
            logger.warning(f"Entering news blackout: {reason}")
            
        elif not is_blackout and self._in_blackout:
            self._in_blackout = False
            self._current_blackout_events = []
            
            self._event_bus.publish(Event(
                event_type=EventType.NEWS_BLACKOUT_END,
                source="news_blackout_manager",
                data={"timestamp": now.isoformat()},
            ))
            logger.info("Exiting news blackout")
        
        # Clean up old events
        self._cleanup_old_events(now)
    
    def _cleanup_old_events(self, now: datetime) -> None:
        """Remove events that are past their blackout window."""
        self._events = [
            e for e in self._events 
            if e.blackout_end > now
        ]
        
        # Clean up old manual blackouts
        self._manual_blackouts = [
            b for b in self._manual_blackouts
            if b["end"] > now
        ]
    
    def get_upcoming_events(
        self,
        hours_ahead: int = 24,
        currency: Optional[str] = None,
        min_impact: Optional[str] = None,
    ) -> List[NewsEvent]:
        """Get upcoming news events."""
        now = datetime.utcnow()
        cutoff = now + timedelta(hours=hours_ahead)
        
        min_level = self._impact_levels.get(min_impact or "low", 1)
        
        events = []
        for event in self._events:
            if event.timestamp > cutoff:
                break
            
            if event.timestamp < now:
                continue
            
            if currency and event.currency != currency.upper():
                continue
            
            if self._impact_levels.get(event.impact, 0) < min_level:
                continue
            
            events.append(event)
        
        return events
    
    def get_next_blackout(
        self,
        symbol: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get the next blackout window."""
        now = datetime.utcnow()
        
        # Check manual blackouts first
        for blackout in sorted(self._manual_blackouts, key=lambda b: b["start"]):
            if blackout["start"] > now:
                if symbol is None or blackout["symbols"] is None or symbol in blackout["symbols"]:
                    return {
                        "type": "manual",
                        "start": blackout["start"],
                        "end": blackout["end"],
                        "reason": blackout["reason"],
                    }
        
        # Check news events
        min_level = self._impact_levels.get(self.min_impact_level, 3)
        
        for event in self._events:
            if event.blackout_start < now:
                continue
            
            event_level = self._impact_levels.get(event.impact, 0)
            if event_level < min_level:
                continue
            
            if symbol:
                affected = self._currency_symbols.get(event.currency, [])
                if symbol not in affected:
                    continue
            
            return {
                "type": "news",
                "start": event.blackout_start,
                "end": event.blackout_end,
                "event": event.to_dict(),
            }
        
        return None
    
    def load_events_from_calendar(self, events_data: List[Dict[str, Any]]) -> int:
        """
        Load events from an economic calendar data source.
        
        Expected format:
        [
            {
                "id": "unique_id",
                "title": "Event Name",
                "currency": "USD",
                "impact": "high",
                "timestamp": "2024-01-15T14:30:00Z"
            },
            ...
        ]
        """
        loaded = 0
        
        for event_data in events_data:
            try:
                timestamp = event_data.get("timestamp")
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                
                self.add_event(
                    event_id=event_data.get("id", f"event_{loaded}"),
                    title=event_data.get("title", "Unknown Event"),
                    currency=event_data.get("currency", "USD"),
                    timestamp=timestamp,
                    impact=event_data.get("impact", "medium"),
                )
                loaded += 1
                
            except Exception as e:
                logger.warning(f"Failed to load event: {e}")
        
        logger.info(f"Loaded {loaded} news events")
        return loaded
    
    def get_status(self) -> Dict[str, Any]:
        """Get current blackout status."""
        return {
            "in_blackout": self._in_blackout,
            "current_events": [e.to_dict() for e in self._current_blackout_events],
            "upcoming_events": len(self.get_upcoming_events(hours_ahead=24)),
            "next_blackout": self.get_next_blackout(),
            "total_events": len(self._events),
            "manual_blackouts": len(self._manual_blackouts),
        }
