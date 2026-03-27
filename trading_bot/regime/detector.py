"""
Regime Detector
================

Detects market regime (trending, ranging, volatility states).
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import logging

from trading_bot.core.types import Candle, Regime
from trading_bot.core.events import Event, EventType, EventBus, get_event_bus

logger = logging.getLogger(__name__)


@dataclass
class RegimeState:
    """Current regime state."""
    regime: Regime
    confidence: float  # 0.0 to 1.0
    timestamp: datetime
    
    # Volatility metrics
    current_volatility: float
    avg_volatility: float
    volatility_percentile: float
    
    # Trend metrics
    trend_strength: float  # -1.0 (strong down) to 1.0 (strong up)
    trend_duration: int  # bars in current trend
    
    # Additional info
    indicators: Dict[str, float]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "regime": self.regime.value,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
            "current_volatility": self.current_volatility,
            "volatility_percentile": self.volatility_percentile,
            "trend_strength": self.trend_strength,
            "trend_duration": self.trend_duration,
            "indicators": self.indicators,
        }


class RegimeDetector:
    """
    Detects current market regime using multiple indicators.
    
    Regimes:
    - TRENDING_UP: Strong upward momentum
    - TRENDING_DOWN: Strong downward momentum
    - RANGING: Price oscillating in a range
    - HIGH_VOLATILITY: Above-average volatility
    - LOW_VOLATILITY: Below-average volatility
    
    Uses:
    - ATR for volatility
    - ADX for trend strength
    - Price action analysis
    """
    
    def __init__(
        self,
        atr_period: int = 14,
        adx_period: int = 14,
        lookback_period: int = 100,
        trend_threshold: float = 25.0,  # ADX threshold for trending
        volatility_high_percentile: float = 75.0,
        volatility_low_percentile: float = 25.0,
        event_bus: Optional[EventBus] = None,
    ):
        self.atr_period = atr_period
        self.adx_period = adx_period
        self.lookback_period = lookback_period
        self.trend_threshold = trend_threshold
        self.volatility_high_percentile = volatility_high_percentile
        self.volatility_low_percentile = volatility_low_percentile
        
        self._event_bus = event_bus or get_event_bus()
        
        # Historical volatility for percentile calculation
        self._volatility_history: Dict[str, List[float]] = {}
        self._max_volatility_history = 500
        
        # Current state per symbol
        self._current_states: Dict[str, RegimeState] = {}
        self._previous_regimes: Dict[str, Regime] = {}
    
    def detect_regime(
        self,
        symbol: str,
        candles: List[Candle],
    ) -> RegimeState:
        """
        Detect current market regime.
        
        Args:
            symbol: Trading symbol
            candles: Historical candles (newest last)
            
        Returns:
            RegimeState with current regime and metrics
        """
        if len(candles) < self.lookback_period:
            return RegimeState(
                regime=Regime.UNKNOWN,
                confidence=0.0,
                timestamp=datetime.utcnow(),
                current_volatility=0.0,
                avg_volatility=0.0,
                volatility_percentile=50.0,
                trend_strength=0.0,
                trend_duration=0,
                indicators={},
            )
        
        # Extract price data
        highs = np.array([c.high for c in candles])
        lows = np.array([c.low for c in candles])
        closes = np.array([c.close for c in candles])
        
        # Calculate indicators
        atr = self._calculate_atr(highs, lows, closes)
        adx, plus_di, minus_di = self._calculate_adx(highs, lows, closes)
        volatility_percentile = self._get_volatility_percentile(symbol, atr)
        trend_strength = self._calculate_trend_strength(closes, plus_di, minus_di)
        trend_duration = self._calculate_trend_duration(closes)
        
        # Determine regime
        regime, confidence = self._classify_regime(
            adx=adx,
            trend_strength=trend_strength,
            volatility_percentile=volatility_percentile,
        )
        
        # Store volatility history
        self._update_volatility_history(symbol, atr)
        
        # Create state
        state = RegimeState(
            regime=regime,
            confidence=confidence,
            timestamp=candles[-1].timestamp,
            current_volatility=atr,
            avg_volatility=np.mean(self._volatility_history.get(symbol, [atr])),
            volatility_percentile=volatility_percentile,
            trend_strength=trend_strength,
            trend_duration=trend_duration,
            indicators={
                "atr": atr,
                "adx": adx,
                "plus_di": plus_di,
                "minus_di": minus_di,
            },
        )
        
        # Check for regime change
        previous_regime = self._previous_regimes.get(symbol)
        if previous_regime and previous_regime != regime:
            self._on_regime_change(symbol, previous_regime, regime, state)
        
        self._current_states[symbol] = state
        self._previous_regimes[symbol] = regime
        
        return state
    
    def _classify_regime(
        self,
        adx: float,
        trend_strength: float,
        volatility_percentile: float,
    ) -> Tuple[Regime, float]:
        """Classify regime based on indicators."""
        confidence = 0.5
        
        # High/Low volatility takes precedence if extreme
        if volatility_percentile >= self.volatility_high_percentile:
            confidence = (volatility_percentile - self.volatility_high_percentile) / 25 + 0.5
            return Regime.HIGH_VOLATILITY, min(confidence, 1.0)
        
        if volatility_percentile <= self.volatility_low_percentile:
            confidence = (self.volatility_low_percentile - volatility_percentile) / 25 + 0.5
            return Regime.LOW_VOLATILITY, min(confidence, 1.0)
        
        # Check for trending
        if adx >= self.trend_threshold:
            confidence = min((adx - self.trend_threshold) / 25 + 0.5, 1.0)
            
            if trend_strength > 0.3:
                return Regime.TRENDING_UP, confidence
            elif trend_strength < -0.3:
                return Regime.TRENDING_DOWN, confidence
        
        # Default to ranging
        confidence = 1.0 - (adx / self.trend_threshold) * 0.5
        return Regime.RANGING, max(confidence, 0.3)
    
    def _calculate_atr(
        self,
        highs: np.ndarray,
        lows: np.ndarray,
        closes: np.ndarray,
    ) -> float:
        """Calculate Average True Range."""
        if len(closes) < 2:
            return float(np.mean(highs - lows))
        
        tr1 = highs[1:] - lows[1:]
        tr2 = np.abs(highs[1:] - closes[:-1])
        tr3 = np.abs(lows[1:] - closes[:-1])
        
        tr = np.maximum(tr1, np.maximum(tr2, tr3))
        atr = np.mean(tr[-self.atr_period:])
        
        return float(atr)
    
    def _calculate_adx(
        self,
        highs: np.ndarray,
        lows: np.ndarray,
        closes: np.ndarray,
    ) -> Tuple[float, float, float]:
        """Calculate Average Directional Index with +DI and -DI."""
        if len(closes) < self.adx_period + 1:
            return 0.0, 0.0, 0.0
        
        # Calculate +DM and -DM
        high_diff = np.diff(highs)
        low_diff = -np.diff(lows)
        
        plus_dm = np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0)
        minus_dm = np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0)
        
        # Calculate ATR for normalization
        tr1 = highs[1:] - lows[1:]
        tr2 = np.abs(highs[1:] - closes[:-1])
        tr3 = np.abs(lows[1:] - closes[:-1])
        tr = np.maximum(tr1, np.maximum(tr2, tr3))
        
        # Smooth using Wilder's smoothing
        period = self.adx_period
        
        def wilder_smooth(data: np.ndarray, period: int) -> np.ndarray:
            result = np.zeros_like(data)
            result[period-1] = np.mean(data[:period])
            for i in range(period, len(data)):
                result[i] = (result[i-1] * (period - 1) + data[i]) / period
            return result
        
        smooth_tr = wilder_smooth(tr, period)
        smooth_plus_dm = wilder_smooth(plus_dm, period)
        smooth_minus_dm = wilder_smooth(minus_dm, period)
        
        # Calculate +DI and -DI
        plus_di = 100 * smooth_plus_dm / np.where(smooth_tr == 0, 1, smooth_tr)
        minus_di = 100 * smooth_minus_dm / np.where(smooth_tr == 0, 1, smooth_tr)
        
        # Calculate DX and ADX
        di_sum = plus_di + minus_di
        di_diff = np.abs(plus_di - minus_di)
        dx = 100 * di_diff / np.where(di_sum == 0, 1, di_sum)
        
        adx = wilder_smooth(dx[period:], period)
        
        if len(adx) == 0:
            return 0.0, float(plus_di[-1]), float(minus_di[-1])
        
        return float(adx[-1]), float(plus_di[-1]), float(minus_di[-1])
    
    def _calculate_trend_strength(
        self,
        closes: np.ndarray,
        plus_di: float,
        minus_di: float,
    ) -> float:
        """
        Calculate trend strength from -1.0 (strong down) to 1.0 (strong up).
        """
        # DI-based strength
        di_total = plus_di + minus_di
        if di_total == 0:
            di_strength = 0.0
        else:
            di_strength = (plus_di - minus_di) / di_total
        
        # Price-based confirmation
        recent = closes[-20:]
        older = closes[-40:-20]
        
        if len(recent) < 20 or len(older) < 20:
            price_strength = 0.0
        else:
            recent_avg = np.mean(recent)
            older_avg = np.mean(older)
            price_change = (recent_avg - older_avg) / older_avg
            price_strength = np.clip(price_change * 10, -1.0, 1.0)
        
        # Combine
        return float((di_strength * 0.6 + price_strength * 0.4))
    
    def _calculate_trend_duration(self, closes: np.ndarray) -> int:
        """Calculate how many bars the current trend has lasted."""
        if len(closes) < 3:
            return 0
        
        # Simple approach: count bars since last direction change
        changes = np.diff(closes)
        current_direction = 1 if changes[-1] > 0 else -1
        
        duration = 1
        for i in range(len(changes) - 2, -1, -1):
            if (changes[i] > 0 and current_direction > 0) or \
               (changes[i] < 0 and current_direction < 0):
                duration += 1
            else:
                break
        
        return duration
    
    def _get_volatility_percentile(self, symbol: str, current_volatility: float) -> float:
        """Get percentile of current volatility vs history."""
        history = self._volatility_history.get(symbol, [])
        
        if len(history) < 10:
            return 50.0
        
        count_below = sum(1 for v in history if v < current_volatility)
        percentile = (count_below / len(history)) * 100
        
        return float(percentile)
    
    def _update_volatility_history(self, symbol: str, volatility: float) -> None:
        """Update volatility history."""
        if symbol not in self._volatility_history:
            self._volatility_history[symbol] = []
        
        self._volatility_history[symbol].append(volatility)
        
        # Trim history
        if len(self._volatility_history[symbol]) > self._max_volatility_history:
            self._volatility_history[symbol] = self._volatility_history[symbol][-self._max_volatility_history:]
    
    def _on_regime_change(
        self,
        symbol: str,
        old_regime: Regime,
        new_regime: Regime,
        state: RegimeState,
    ) -> None:
        """Handle regime change event."""
        logger.info(f"Regime change for {symbol}: {old_regime.value} -> {new_regime.value}")
        
        self._event_bus.publish(Event(
            event_type=EventType.REGIME_CHANGED,
            source="regime_detector",
            data={
                "symbol": symbol,
                "old_regime": old_regime.value,
                "new_regime": new_regime.value,
                "confidence": state.confidence,
                "indicators": state.indicators,
            },
        ))
    
    def get_current_state(self, symbol: str) -> Optional[RegimeState]:
        """Get current regime state for a symbol."""
        return self._current_states.get(symbol)
    
    def should_trade_in_regime(
        self,
        regime: Regime,
        suitable_regimes: List[Regime],
    ) -> bool:
        """Check if trading is suitable for current regime."""
        if not suitable_regimes:
            return True  # No restrictions
        
        return regime in suitable_regimes or regime == Regime.UNKNOWN
    
    def get_regime_filter_result(
        self,
        symbol: str,
        suitable_regimes: List[Regime],
    ) -> Tuple[bool, str]:
        """
        Check if current regime allows trading.
        
        Returns (is_allowed, reason)
        """
        state = self._current_states.get(symbol)
        
        if not state:
            return True, "No regime data available"
        
        if not suitable_regimes:
            return True, "No regime restrictions"
        
        if state.regime in suitable_regimes:
            return True, f"Regime {state.regime.value} is suitable"
        
        return False, f"Regime {state.regime.value} not in suitable regimes: {[r.value for r in suitable_regimes]}"
    
    def get_stats(self) -> Dict[str, Any]:
        """Get detector statistics."""
        return {
            "symbols_tracked": list(self._current_states.keys()),
            "current_regimes": {
                s: state.regime.value 
                for s, state in self._current_states.items()
            },
            "volatility_history_sizes": {
                s: len(h) for s, h in self._volatility_history.items()
            },
        }
