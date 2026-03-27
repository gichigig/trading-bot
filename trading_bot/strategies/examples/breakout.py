"""
Breakout Strategy
==================

A volatility breakout strategy that trades range expansions.
Demonstrates:
- ATR-based breakout detection
- Volume confirmation
- Volatility compression patterns
"""

from typing import Any, Dict, List, Optional
import numpy as np

from trading_bot.strategies.base import BaseStrategy, StrategyMetadata, StrategyContext
from trading_bot.strategies.registry import register_strategy
from trading_bot.core.types import Signal, SignalType, Candle, Regime


@register_strategy
class BreakoutStrategy(BaseStrategy):
    """
    Breakout strategy for volatility expansions.
    
    Entry conditions:
    - Price breaks recent high/low
    - Volume confirms breakout
    - Prior volatility compression
    
    Exit conditions:
    - Trailing stop hit
    - Time-based exit
    - Take profit reached
    """
    
    def _define_metadata(self) -> StrategyMetadata:
        return StrategyMetadata(
            name="breakout",
            version="1.0.0",
            author="Trading Bot Framework",
            description="Volatility breakout strategy with volume confirmation",
            category="breakout",
            suitable_regimes=[Regime.HIGH_VOLATILITY, Regime.UNKNOWN],
            min_timeframe="15m",
            preferred_timeframe="4h",
            warmup_periods=100,
            max_concurrent_signals=1,
            parameter_schema={
                "lookback_period": {"type": int, "min": 10, "max": 100, "default": 20},
                "atr_period": {"type": int, "min": 7, "max": 21, "default": 14},
                "atr_breakout_mult": {"type": float, "min": 0.5, "max": 3.0, "default": 1.0},
                "volume_mult": {"type": float, "min": 1.0, "max": 3.0, "default": 1.5},
                "stop_loss_atr_mult": {"type": float, "min": 1.0, "max": 4.0, "default": 1.5},
                "trailing_stop_atr_mult": {"type": float, "min": 1.0, "max": 5.0, "default": 2.0},
                "max_bars_in_trade": {"type": int, "min": 10, "max": 100, "default": 50},
            }
        )
    
    def _get_default_parameters(self) -> Dict[str, Any]:
        return {
            "lookback_period": 20,
            "atr_period": 14,
            "atr_breakout_mult": 1.0,
            "volume_mult": 1.5,
            "stop_loss_atr_mult": 1.5,
            "trailing_stop_atr_mult": 2.0,
            "max_bars_in_trade": 50,
            "require_compression": True,
            "compression_threshold": 0.7,  # ATR must be below 70% of average
        }
    
    def generate_signal(self, context: StrategyContext) -> Optional[Signal]:
        """Generate breakout signal."""
        candles = context.candles.get(context.timeframe, [])
        
        if len(candles) < self.warmup_periods:
            return None
        
        # Calculate indicators
        indicators = self.calculate_indicators(candles)
        context.indicators = indicators
        
        current_candle = candles[-1]
        current_price = current_candle.close
        
        # Check for existing position
        if context.has_position:
            return self._check_exit_signal(context, indicators)
        
        lookback = self.get_parameter("lookback_period")
        atr = indicators.get("atr", 0)
        avg_atr = indicators.get("avg_atr", atr)
        current_volume = current_candle.volume
        avg_volume = indicators.get("avg_volume", current_volume)
        
        # Get recent high/low
        recent_high = indicators.get("recent_high", current_price)
        recent_low = indicators.get("recent_low", current_price)
        
        # Check for volatility compression (setup condition)
        if self.get_parameter("require_compression"):
            compression_threshold = self.get_parameter("compression_threshold")
            if atr > avg_atr * compression_threshold:
                return None  # No compression, skip
        
        # Check volume confirmation
        volume_mult = self.get_parameter("volume_mult")
        has_volume = current_volume > avg_volume * volume_mult
        
        atr_breakout_mult = self.get_parameter("atr_breakout_mult")
        breakout_threshold = atr * atr_breakout_mult
        
        stop_loss_atr = self.get_parameter("stop_loss_atr_mult")
        
        # Bullish breakout
        if current_price > recent_high + breakout_threshold:
            if not has_volume:
                # Log but don't trade without volume
                return None
            
            stop_loss = current_price - (atr * stop_loss_atr)
            
            return self._create_signal(
                signal_type=SignalType.ENTRY_LONG,
                price=current_price,
                context=context,
                stop_loss=stop_loss,
                take_profits=[
                    {"price": current_price + (atr * 3), "pct": 50},
                    {"price": current_price + (atr * 5), "pct": 50},
                ],
                confidence=self._calculate_confidence(indicators, "long"),
                reason=f"Bullish breakout above {recent_high:.2f}, volume={current_volume/avg_volume:.1f}x",
            )
        
        # Bearish breakout
        if current_price < recent_low - breakout_threshold:
            if not has_volume:
                return None
            
            stop_loss = current_price + (atr * stop_loss_atr)
            
            return self._create_signal(
                signal_type=SignalType.ENTRY_SHORT,
                price=current_price,
                context=context,
                stop_loss=stop_loss,
                take_profits=[
                    {"price": current_price - (atr * 3), "pct": 50},
                    {"price": current_price - (atr * 5), "pct": 50},
                ],
                confidence=self._calculate_confidence(indicators, "short"),
                reason=f"Bearish breakout below {recent_low:.2f}, volume={current_volume/avg_volume:.1f}x",
            )
        
        return None
    
    def _check_exit_signal(
        self,
        context: StrategyContext,
        indicators: Dict[str, float],
    ) -> Optional[Signal]:
        """Check for exit signals."""
        # Time-based exit logic would be handled by Trade Lifecycle Manager
        # Here we just check for reversal signals
        
        current_price = context.current_candle.close if context.current_candle else 0
        atr = indicators.get("atr", 0)
        
        # Exit on momentum loss (simplified)
        if context.position_side and context.position_side.value == "buy":
            # Check for bearish reversal
            recent_low = indicators.get("recent_low", current_price)
            if current_price < recent_low:
                return self._create_signal(
                    signal_type=SignalType.EXIT_LONG,
                    price=current_price,
                    context=context,
                    reason="Breakout failure - price back below range",
                )
        else:
            # Check for bullish reversal
            recent_high = indicators.get("recent_high", current_price)
            if current_price > recent_high:
                return self._create_signal(
                    signal_type=SignalType.EXIT_SHORT,
                    price=current_price,
                    context=context,
                    reason="Breakout failure - price back above range",
                )
        
        return None
    
    def calculate_indicators(self, candles: List[Candle]) -> Dict[str, float]:
        """Calculate breakout indicators."""
        closes = np.array([c.close for c in candles])
        highs = np.array([c.high for c in candles])
        lows = np.array([c.low for c in candles])
        volumes = np.array([c.volume for c in candles])
        
        lookback = self.get_parameter("lookback_period")
        atr_period = self.get_parameter("atr_period")
        
        # ATR
        atr = self._atr(highs, lows, closes, atr_period)
        
        # Average ATR for compression detection
        atr_values = []
        for i in range(atr_period, len(closes)):
            atr_val = self._atr(
                highs[i-atr_period:i+1],
                lows[i-atr_period:i+1],
                closes[i-atr_period:i+1],
                atr_period
            )
            atr_values.append(atr_val)
        avg_atr = np.mean(atr_values[-lookback:]) if atr_values else atr
        
        # Recent high/low (excluding current candle for breakout detection)
        recent_high = np.max(highs[-lookback-1:-1])
        recent_low = np.min(lows[-lookback-1:-1])
        
        # Volume
        avg_volume = np.mean(volumes[-lookback:])
        
        return {
            "atr": atr,
            "avg_atr": avg_atr,
            "recent_high": recent_high,
            "recent_low": recent_low,
            "avg_volume": avg_volume,
            "close": closes[-1],
            "compression_ratio": atr / avg_atr if avg_atr else 1.0,
        }
    
    def _calculate_confidence(self, indicators: Dict[str, float], direction: str) -> float:
        """Calculate signal confidence."""
        confidence = 0.5
        
        compression_ratio = indicators.get("compression_ratio", 1.0)
        
        # Better compression = higher confidence
        if compression_ratio < 0.5:
            confidence += 0.3
        elif compression_ratio < 0.7:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    @staticmethod
    def _atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
        """Calculate Average True Range."""
        if len(closes) < period + 1:
            return float(np.mean(highs - lows)) if len(highs) > 0 else 0.0
        
        tr1 = highs[1:] - lows[1:]
        tr2 = np.abs(highs[1:] - closes[:-1])
        tr3 = np.abs(lows[1:] - closes[:-1])
        
        tr = np.maximum(tr1, np.maximum(tr2, tr3))
        atr = np.mean(tr[-period:])
        
        return float(atr)
