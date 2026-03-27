"""
Session Manager
================

Trading session awareness (Asia, London, NY) with overlap detection.
"""

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
import logging

from trading_bot.core.events import Event, EventType, EventBus, get_event_bus

logger = logging.getLogger(__name__)


class TradingSession(Enum):
    """Major trading sessions."""
    ASIA = "asia"
    LONDON = "london"
    NEW_YORK = "new_york"
    ASIA_LONDON_OVERLAP = "asia_london_overlap"
    LONDON_NY_OVERLAP = "london_ny_overlap"
    OFF_HOURS = "off_hours"


@dataclass
class SessionConfig:
    """Session time configuration."""
    name: TradingSession
    start_hour: int
    start_minute: int = 0
    end_hour: int = 0
    end_minute: int = 0
    timezone: str = "UTC"
    typical_volatility: str = "medium"  # low, medium, high
    description: str = ""


# Default session times (UTC)
DEFAULT_SESSIONS = [
    SessionConfig(
        name=TradingSession.ASIA,
        start_hour=0,
        end_hour=8,
        typical_volatility="low",
        description="Asian session - Tokyo, Sydney, Hong Kong",
    ),
    SessionConfig(
        name=TradingSession.ASIA_LONDON_OVERLAP,
        start_hour=7,
        end_hour=9,
        typical_volatility="medium",
        description="Asia-London overlap",
    ),
    SessionConfig(
        name=TradingSession.LONDON,
        start_hour=8,
        end_hour=16,
        typical_volatility="high",
        description="London session - highest forex volume",
    ),
    SessionConfig(
        name=TradingSession.LONDON_NY_OVERLAP,
        start_hour=13,
        end_hour=16,
        typical_volatility="high",
        description="London-NY overlap - highest volatility",
    ),
    SessionConfig(
        name=TradingSession.NEW_YORK,
        start_hour=13,
        end_hour=21,
        typical_volatility="high",
        description="New York session",
    ),
]


class SessionManager:
    """
    Manages trading session awareness.
    
    Features:
    - Current session detection
    - Session change notifications
    - Session-based trading filters
    - Weekend detection
    """
    
    def __init__(
        self,
        sessions: Optional[List[SessionConfig]] = None,
        event_bus: Optional[EventBus] = None,
    ):
        self.sessions = sessions or DEFAULT_SESSIONS
        self._event_bus = event_bus or get_event_bus()
        
        self._current_session: Optional[TradingSession] = None
        self._session_start_time: Optional[datetime] = None
        
        # Trading preferences per session
        self._session_preferences: Dict[TradingSession, Dict[str, Any]] = {
            TradingSession.ASIA: {
                "trade_allowed": True,
                "reduce_size_pct": 0,
                "preferred_pairs": ["USDJPY", "AUDUSD", "NZDUSD"],
            },
            TradingSession.LONDON: {
                "trade_allowed": True,
                "reduce_size_pct": 0,
                "preferred_pairs": ["EURUSD", "GBPUSD", "EURGBP"],
            },
            TradingSession.NEW_YORK: {
                "trade_allowed": True,
                "reduce_size_pct": 0,
                "preferred_pairs": ["EURUSD", "USDCAD", "USDJPY"],
            },
            TradingSession.LONDON_NY_OVERLAP: {
                "trade_allowed": True,
                "reduce_size_pct": 0,
                "preferred_pairs": ["EURUSD", "GBPUSD"],
            },
            TradingSession.ASIA_LONDON_OVERLAP: {
                "trade_allowed": True,
                "reduce_size_pct": 0,
                "preferred_pairs": ["EURJPY", "GBPJPY"],
            },
            TradingSession.OFF_HOURS: {
                "trade_allowed": False,
                "reduce_size_pct": 50,
                "preferred_pairs": [],
            },
        }
    
    def get_current_session(self, timestamp: Optional[datetime] = None) -> TradingSession:
        """Get the current trading session."""
        now = timestamp or datetime.utcnow()
        
        # Check for weekend
        if self.is_weekend(now):
            return TradingSession.OFF_HOURS
        
        current_time = now.time()
        
        # Check overlaps first (more specific)
        for session in self.sessions:
            if session.name in [TradingSession.ASIA_LONDON_OVERLAP, TradingSession.LONDON_NY_OVERLAP]:
                if self._is_in_session(current_time, session):
                    return session.name
        
        # Check main sessions
        for session in self.sessions:
            if session.name not in [TradingSession.ASIA_LONDON_OVERLAP, TradingSession.LONDON_NY_OVERLAP]:
                if self._is_in_session(current_time, session):
                    return session.name
        
        return TradingSession.OFF_HOURS
    
    def _is_in_session(self, current_time: time, session: SessionConfig) -> bool:
        """Check if time is within session hours."""
        start = time(session.start_hour, session.start_minute)
        end = time(session.end_hour, session.end_minute)
        
        if start <= end:
            return start <= current_time < end
        else:
            # Session crosses midnight
            return current_time >= start or current_time < end
    
    def is_weekend(self, timestamp: Optional[datetime] = None) -> bool:
        """Check if it's weekend (forex market closed)."""
        now = timestamp or datetime.utcnow()
        
        # Forex closes Friday 21:00 UTC, opens Sunday 21:00 UTC
        if now.weekday() == 4 and now.hour >= 21:  # Friday after 21:00
            return True
        if now.weekday() == 5:  # Saturday
            return True
        if now.weekday() == 6 and now.hour < 21:  # Sunday before 21:00
            return True
        
        return False
    
    def update(self, timestamp: Optional[datetime] = None) -> Optional[TradingSession]:
        """
        Update session state and return new session if changed.
        """
        now = timestamp or datetime.utcnow()
        new_session = self.get_current_session(now)
        
        if new_session != self._current_session:
            old_session = self._current_session
            self._current_session = new_session
            self._session_start_time = now
            
            logger.info(f"Session changed: {old_session} -> {new_session}")
            
            self._event_bus.publish(Event(
                event_type=EventType.SESSION_CHANGED,
                source="session_manager",
                data={
                    "old_session": old_session.value if old_session else None,
                    "new_session": new_session.value,
                    "timestamp": now.isoformat(),
                },
            ))
            
            return new_session
        
        return None
    
    def get_time_until_session_change(self, timestamp: Optional[datetime] = None) -> timedelta:
        """Get time until the next session change."""
        now = timestamp or datetime.utcnow()
        current_session = self.get_current_session(now)
        
        # Find current session config
        current_config = None
        for session in self.sessions:
            if session.name == current_session:
                current_config = session
                break
        
        if not current_config:
            return timedelta(hours=1)  # Default
        
        # Calculate end time
        end_time = now.replace(
            hour=current_config.end_hour,
            minute=current_config.end_minute,
            second=0,
            microsecond=0,
        )
        
        if end_time <= now:
            end_time += timedelta(days=1)
        
        return end_time - now
    
    def should_trade_in_session(
        self,
        session: Optional[TradingSession] = None,
    ) -> Tuple[bool, str]:
        """
        Check if trading is allowed in current/specified session.
        
        Returns (is_allowed, reason)
        """
        session = session or self._current_session
        
        if not session:
            return True, "Session unknown"
        
        prefs = self._session_preferences.get(session, {})
        is_allowed = prefs.get("trade_allowed", True)
        
        if not is_allowed:
            return False, f"Trading disabled for session: {session.value}"
        
        return True, f"Trading allowed in {session.value}"
    
    def get_session_volatility(
        self,
        session: Optional[TradingSession] = None,
    ) -> str:
        """Get expected volatility level for session."""
        session = session or self._current_session
        
        for config in self.sessions:
            if config.name == session:
                return config.typical_volatility
        
        return "unknown"
    
    def get_preferred_pairs(
        self,
        session: Optional[TradingSession] = None,
    ) -> List[str]:
        """Get preferred trading pairs for session."""
        session = session or self._current_session
        prefs = self._session_preferences.get(session, {})
        return prefs.get("preferred_pairs", [])
    
    def get_position_size_adjustment(
        self,
        session: Optional[TradingSession] = None,
    ) -> float:
        """
        Get position size adjustment multiplier for session.
        
        Returns multiplier (e.g., 0.5 for 50% reduction)
        """
        session = session or self._current_session
        prefs = self._session_preferences.get(session, {})
        reduction = prefs.get("reduce_size_pct", 0)
        return 1.0 - (reduction / 100)
    
    def set_session_preference(
        self,
        session: TradingSession,
        key: str,
        value: Any,
    ) -> None:
        """Set a session preference."""
        if session not in self._session_preferences:
            self._session_preferences[session] = {}
        self._session_preferences[session][key] = value
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get current session information."""
        return {
            "current_session": self._current_session.value if self._current_session else None,
            "session_start": self._session_start_time.isoformat() if self._session_start_time else None,
            "is_weekend": self.is_weekend(),
            "volatility": self.get_session_volatility(),
            "time_until_change": str(self.get_time_until_session_change()),
        }
