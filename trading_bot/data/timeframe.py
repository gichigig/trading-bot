"""
Timeframe Management
=====================

Multi-timeframe data handling with alignment and aggregation.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from enum import Enum
import logging

from trading_bot.core.types import Candle

logger = logging.getLogger(__name__)


class Timeframe(Enum):
    """Standard timeframe definitions."""
    M1 = "1m"
    M3 = "3m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    H6 = "6h"
    H8 = "8h"
    H12 = "12h"
    D1 = "1d"
    D3 = "3d"
    W1 = "1w"
    MN1 = "1M"
    
    @property
    def minutes(self) -> int:
        """Get timeframe duration in minutes."""
        mapping = {
            "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "2h": 120, "4h": 240, "6h": 360, "8h": 480, "12h": 720,
            "1d": 1440, "3d": 4320, "1w": 10080, "1M": 43200
        }
        return mapping.get(self.value, 60)
    
    @property
    def seconds(self) -> int:
        """Get timeframe duration in seconds."""
        return self.minutes * 60
    
    @classmethod
    def from_string(cls, tf_string: str) -> 'Timeframe':
        """Create timeframe from string."""
        for tf in cls:
            if tf.value == tf_string:
                return tf
        raise ValueError(f"Unknown timeframe: {tf_string}")
    
    def is_higher_than(self, other: 'Timeframe') -> bool:
        """Check if this timeframe is higher than another."""
        return self.minutes > other.minutes


@dataclass
class TimeframeConfig:
    """Configuration for multi-timeframe analysis."""
    execution_tf: Timeframe  # Timeframe for entries
    bias_tf: Timeframe  # Higher timeframe for direction bias
    confirmation_tfs: List[Timeframe]  # Additional confirmation timeframes
    alignment_required: bool = True  # Require all timeframes to align
    
    def validate(self) -> bool:
        """Validate timeframe configuration."""
        if not self.bias_tf.is_higher_than(self.execution_tf):
            logger.warning("Bias timeframe should be higher than execution timeframe")
            return False
        return True


class TimeframeManager:
    """
    Manages multi-timeframe data with aggregation and alignment.
    
    Features:
    - Automatic candle aggregation from lower to higher timeframes
    - Timeframe alignment checks
    - Cross-timeframe bias calculation
    """
    
    def __init__(self, base_timeframe: Timeframe = Timeframe.M1):
        self.base_timeframe = base_timeframe
        self._candle_buffers: Dict[str, Dict[str, List[Candle]]] = {}  # symbol -> tf -> candles
        self._max_candles = 1000  # Max candles per buffer
    
    def add_candle(self, candle: Candle) -> Dict[str, Optional[Candle]]:
        """
        Add a candle and return any completed higher timeframe candles.
        
        Returns dict of timeframe -> completed candle (or None if incomplete)
        """
        symbol = candle.symbol
        
        if symbol not in self._candle_buffers:
            self._candle_buffers[symbol] = {}
        
        # Add to base timeframe buffer
        if candle.timeframe not in self._candle_buffers[symbol]:
            self._candle_buffers[symbol][candle.timeframe] = []
        
        self._candle_buffers[symbol][candle.timeframe].append(candle)
        self._trim_buffer(symbol, candle.timeframe)
        
        # Aggregate to higher timeframes
        completed = {candle.timeframe: candle}
        
        for tf in Timeframe:
            if tf.minutes > Timeframe.from_string(candle.timeframe).minutes:
                aggregated = self._aggregate_to_timeframe(symbol, candle, tf)
                if aggregated:
                    completed[tf.value] = aggregated
        
        return completed
    
    def _aggregate_to_timeframe(
        self,
        symbol: str,
        latest_candle: Candle,
        target_tf: Timeframe,
    ) -> Optional[Candle]:
        """Aggregate candles to a higher timeframe."""
        source_tf = latest_candle.timeframe
        source_minutes = Timeframe.from_string(source_tf).minutes
        target_minutes = target_tf.minutes
        
        # Calculate how many source candles make one target candle
        candles_needed = target_minutes // source_minutes
        
        # Get buffer
        buffer = self._candle_buffers.get(symbol, {}).get(source_tf, [])
        
        if len(buffer) < candles_needed:
            return None
        
        # Check if we're at a timeframe boundary
        candle_time = latest_candle.timestamp
        boundary_check = candle_time.minute % target_minutes == (target_minutes - source_minutes)
        
        if target_minutes >= 60:
            # For hourly+ timeframes, also check hours
            hours_in_tf = target_minutes // 60
            boundary_check = (
                candle_time.minute == 60 - source_minutes and
                (candle_time.hour + 1) % hours_in_tf == 0
            )
        
        if not boundary_check:
            return None
        
        # Get candles for aggregation
        candles_to_aggregate = buffer[-candles_needed:]
        
        # Create aggregated candle
        aggregated = Candle(
            symbol=symbol,
            timeframe=target_tf.value,
            timestamp=candles_to_aggregate[0].timestamp,
            open=candles_to_aggregate[0].open,
            high=max(c.high for c in candles_to_aggregate),
            low=min(c.low for c in candles_to_aggregate),
            close=candles_to_aggregate[-1].close,
            volume=sum(c.volume for c in candles_to_aggregate),
        )
        
        # Store aggregated candle
        if target_tf.value not in self._candle_buffers[symbol]:
            self._candle_buffers[symbol][target_tf.value] = []
        self._candle_buffers[symbol][target_tf.value].append(aggregated)
        self._trim_buffer(symbol, target_tf.value)
        
        return aggregated
    
    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        count: Optional[int] = None,
    ) -> List[Candle]:
        """Get candles for a symbol and timeframe."""
        buffer = self._candle_buffers.get(symbol, {}).get(timeframe, [])
        if count:
            return buffer[-count:]
        return buffer.copy()
    
    def get_multi_timeframe_data(
        self,
        symbol: str,
        timeframes: List[str],
    ) -> Dict[str, List[Candle]]:
        """Get candle data for multiple timeframes."""
        result = {}
        for tf in timeframes:
            result[tf] = self.get_candles(symbol, tf)
        return result
    
    def calculate_htf_bias(
        self,
        symbol: str,
        timeframe: str,
        lookback: int = 20,
    ) -> Tuple[str, float]:
        """
        Calculate higher timeframe bias.
        
        Returns (direction, strength) where:
        - direction: "bullish", "bearish", or "neutral"
        - strength: 0.0 to 1.0
        """
        candles = self.get_candles(symbol, timeframe, lookback)
        
        if len(candles) < 2:
            return "neutral", 0.0
        
        # Simple trend detection using price action
        closes = [c.close for c in candles]
        current = closes[-1]
        
        # Calculate simple moving average
        sma = sum(closes) / len(closes)
        
        # Calculate recent momentum
        recent_half = closes[len(closes)//2:]
        older_half = closes[:len(closes)//2]
        recent_avg = sum(recent_half) / len(recent_half)
        older_avg = sum(older_half) / len(older_half) if older_half else recent_avg
        
        # Determine direction
        if current > sma and recent_avg > older_avg:
            direction = "bullish"
            strength = min((current - sma) / sma * 100, 1.0)
        elif current < sma and recent_avg < older_avg:
            direction = "bearish"
            strength = min((sma - current) / sma * 100, 1.0)
        else:
            direction = "neutral"
            strength = 0.0
        
        return direction, strength
    
    def check_timeframe_alignment(
        self,
        symbol: str,
        timeframes: List[str],
        direction: str,
    ) -> Tuple[bool, Dict[str, str]]:
        """
        Check if multiple timeframes align in the same direction.
        
        Returns (is_aligned, {timeframe: direction})
        """
        biases = {}
        
        for tf in timeframes:
            tf_direction, _ = self.calculate_htf_bias(symbol, tf)
            biases[tf] = tf_direction
        
        is_aligned = all(
            bias == direction or bias == "neutral"
            for bias in biases.values()
        )
        
        return is_aligned, biases
    
    def is_candle_complete(self, candle: Candle) -> bool:
        """Check if a candle is complete (closed)."""
        tf = Timeframe.from_string(candle.timeframe)
        candle_end = candle.timestamp + timedelta(minutes=tf.minutes)
        return datetime.utcnow() >= candle_end
    
    def get_time_to_candle_close(self, timeframe: str) -> timedelta:
        """Get time remaining until current candle closes."""
        tf = Timeframe.from_string(timeframe)
        now = datetime.utcnow()
        
        # Calculate current candle start
        minutes_since_midnight = now.hour * 60 + now.minute
        candle_start_minutes = (minutes_since_midnight // tf.minutes) * tf.minutes
        
        # Calculate next candle time
        next_candle_minutes = candle_start_minutes + tf.minutes
        next_candle = now.replace(
            hour=next_candle_minutes // 60,
            minute=next_candle_minutes % 60,
            second=0,
            microsecond=0,
        )
        
        return next_candle - now
    
    def _trim_buffer(self, symbol: str, timeframe: str) -> None:
        """Trim buffer to max size."""
        buffer = self._candle_buffers.get(symbol, {}).get(timeframe, [])
        if len(buffer) > self._max_candles:
            self._candle_buffers[symbol][timeframe] = buffer[-self._max_candles:]
    
    def clear(self, symbol: Optional[str] = None) -> None:
        """Clear candle buffers."""
        if symbol:
            self._candle_buffers.pop(symbol, None)
        else:
            self._candle_buffers.clear()
    
    def get_stats(self) -> Dict[str, any]:
        """Get buffer statistics."""
        stats = {}
        for symbol, timeframes in self._candle_buffers.items():
            stats[symbol] = {tf: len(candles) for tf, candles in timeframes.items()}
        return stats
