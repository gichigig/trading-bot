"""
Data Manager
=============

Central data management with caching, validation, and distribution.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set
from collections import defaultdict
import logging

from trading_bot.core.types import Candle, Tick
from trading_bot.core.events import Event, EventType, EventBus, get_event_bus
from trading_bot.data.timeframe import TimeframeManager, Timeframe

logger = logging.getLogger(__name__)


class DataManager:
    """
    Central data management system.
    
    Responsibilities:
    - Receive and validate market data
    - Distribute data to strategies and components
    - Handle data gaps and clock drift
    - Manage multi-timeframe aggregation
    """
    
    def __init__(self, event_bus: Optional[EventBus] = None):
        self._event_bus = event_bus or get_event_bus()
        self._timeframe_manager = TimeframeManager()
        
        # Data subscribers
        self._candle_subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._tick_subscribers: Dict[str, List[Callable]] = defaultdict(list)
        
        # Data validation
        self._last_timestamps: Dict[str, datetime] = {}  # symbol -> last timestamp
        self._clock_drift_threshold = timedelta(seconds=30)
        self._gap_threshold = timedelta(minutes=5)
        
        # Statistics
        self._candles_received = 0
        self._ticks_received = 0
        self._gaps_detected = 0
        self._duplicates_filtered = 0
        
        # Subscribed symbols
        self._subscribed_symbols: Set[str] = set()
        self._subscribed_timeframes: Dict[str, Set[str]] = defaultdict(set)
    
    def subscribe_candles(
        self,
        symbol: str,
        timeframe: str,
        callback: Callable[[Candle], None],
    ) -> None:
        """Subscribe to candle updates for a symbol/timeframe."""
        key = f"{symbol}:{timeframe}"
        self._candle_subscribers[key].append(callback)
        self._subscribed_symbols.add(symbol)
        self._subscribed_timeframes[symbol].add(timeframe)
        logger.debug(f"Subscribed to candles: {key}")
    
    def subscribe_ticks(
        self,
        symbol: str,
        callback: Callable[[Tick], None],
    ) -> None:
        """Subscribe to tick updates for a symbol."""
        self._tick_subscribers[symbol].append(callback)
        self._subscribed_symbols.add(symbol)
        logger.debug(f"Subscribed to ticks: {symbol}")
    
    def unsubscribe_candles(
        self,
        symbol: str,
        timeframe: str,
        callback: Callable[[Candle], None],
    ) -> None:
        """Unsubscribe from candle updates."""
        key = f"{symbol}:{timeframe}"
        if callback in self._candle_subscribers[key]:
            self._candle_subscribers[key].remove(callback)
    
    def on_candle(self, candle: Candle) -> None:
        """
        Process incoming candle data.
        
        Validates, aggregates, and distributes candle data.
        """
        # Validate candle
        if not self._validate_candle(candle):
            return
        
        self._candles_received += 1
        
        # Check for gaps
        self._check_for_gaps(candle)
        
        # Add to timeframe manager and get aggregated candles
        completed_candles = self._timeframe_manager.add_candle(candle)
        
        # Distribute to subscribers
        for tf, completed_candle in completed_candles.items():
            if completed_candle:
                self._distribute_candle(completed_candle)
        
        # Publish event
        self._event_bus.publish(Event(
            event_type=EventType.CANDLE,
            source="data_manager",
            data=candle.to_dict(),
        ))
    
    def on_tick(self, tick: Tick) -> None:
        """Process incoming tick data."""
        self._ticks_received += 1
        
        # Distribute to subscribers
        for callback in self._tick_subscribers.get(tick.symbol, []):
            try:
                callback(tick)
            except Exception as e:
                logger.error(f"Error in tick callback: {e}")
        
        # Publish event
        self._event_bus.publish(Event(
            event_type=EventType.TICK,
            source="data_manager",
            data={
                "symbol": tick.symbol,
                "bid": tick.bid,
                "ask": tick.ask,
                "timestamp": tick.timestamp.isoformat(),
            },
        ))
    
    def _validate_candle(self, candle: Candle) -> bool:
        """Validate candle data integrity."""
        # Basic validation
        if candle.high < candle.low:
            logger.warning(f"Invalid candle: high < low for {candle.symbol}")
            return False
        
        if candle.high < candle.open or candle.high < candle.close:
            logger.warning(f"Invalid candle: high not highest for {candle.symbol}")
            return False
        
        if candle.low > candle.open or candle.low > candle.close:
            logger.warning(f"Invalid candle: low not lowest for {candle.symbol}")
            return False
        
        if candle.volume < 0:
            logger.warning(f"Invalid candle: negative volume for {candle.symbol}")
            return False
        
        # Check for duplicates
        key = f"{candle.symbol}:{candle.timeframe}"
        last_ts = self._last_timestamps.get(key)
        
        if last_ts and candle.timestamp <= last_ts:
            self._duplicates_filtered += 1
            return False
        
        self._last_timestamps[key] = candle.timestamp
        
        # Check clock drift
        now = datetime.utcnow()
        if abs(now - candle.timestamp) > self._clock_drift_threshold:
            logger.warning(f"Possible clock drift for {candle.symbol}: {abs(now - candle.timestamp)}")
        
        return True
    
    def _check_for_gaps(self, candle: Candle) -> None:
        """Check for data gaps."""
        key = f"{candle.symbol}:{candle.timeframe}"
        last_ts = self._last_timestamps.get(key)
        
        if last_ts:
            tf = Timeframe.from_string(candle.timeframe)
            expected_gap = timedelta(minutes=tf.minutes)
            actual_gap = candle.timestamp - last_ts
            
            if actual_gap > expected_gap + self._gap_threshold:
                self._gaps_detected += 1
                logger.warning(f"Data gap detected for {key}: {actual_gap}")
                
                self._event_bus.publish(Event(
                    event_type=EventType.BOT_ERROR,
                    source="data_manager",
                    data={
                        "error_type": "data_gap",
                        "symbol": candle.symbol,
                        "timeframe": candle.timeframe,
                        "gap_duration": str(actual_gap),
                    },
                ))
    
    def _distribute_candle(self, candle: Candle) -> None:
        """Distribute candle to subscribers."""
        key = f"{candle.symbol}:{candle.timeframe}"
        
        for callback in self._candle_subscribers.get(key, []):
            try:
                callback(candle)
            except Exception as e:
                logger.error(f"Error in candle callback: {e}")
    
    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        count: Optional[int] = None,
    ) -> List[Candle]:
        """Get historical candles from buffer."""
        return self._timeframe_manager.get_candles(symbol, timeframe, count)
    
    def get_multi_timeframe_data(
        self,
        symbol: str,
        timeframes: List[str],
    ) -> Dict[str, List[Candle]]:
        """Get data for multiple timeframes."""
        return self._timeframe_manager.get_multi_timeframe_data(symbol, timeframes)
    
    def get_htf_bias(
        self,
        symbol: str,
        timeframe: str,
        lookback: int = 20,
    ) -> tuple:
        """Get higher timeframe bias."""
        return self._timeframe_manager.calculate_htf_bias(symbol, timeframe, lookback)
    
    def check_timeframe_alignment(
        self,
        symbol: str,
        timeframes: List[str],
        direction: str,
    ) -> tuple:
        """Check timeframe alignment."""
        return self._timeframe_manager.check_timeframe_alignment(
            symbol, timeframes, direction
        )
    
    def get_subscribed_symbols(self) -> Set[str]:
        """Get all subscribed symbols."""
        return self._subscribed_symbols.copy()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get data manager statistics."""
        return {
            "candles_received": self._candles_received,
            "ticks_received": self._ticks_received,
            "gaps_detected": self._gaps_detected,
            "duplicates_filtered": self._duplicates_filtered,
            "subscribed_symbols": list(self._subscribed_symbols),
            "buffer_stats": self._timeframe_manager.get_stats(),
        }
    
    def reset(self) -> None:
        """Reset data manager state."""
        self._timeframe_manager.clear()
        self._last_timestamps.clear()
        self._candles_received = 0
        self._ticks_received = 0
        self._gaps_detected = 0
        self._duplicates_filtered = 0
