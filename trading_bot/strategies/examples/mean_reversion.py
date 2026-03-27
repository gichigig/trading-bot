"""
Mean Reversion Strategy
========================

A counter-trend strategy that trades reversions to the mean.
Demonstrates:
- Bollinger Bands
- RSI extremes
- Regime filtering (only trade in ranging markets)
"""

from typing import Any, Dict, List, Optional
import numpy as np

from trading_bot.strategies.base import BaseStrategy, StrategyMetadata, StrategyContext
from trading_bot.strategies.registry import register_strategy
from trading_bot.core.types import Signal, SignalType, Candle, Regime


@register_strategy
class MeanReversionStrategy(BaseStrategy):
    """
    Mean reversion strategy for ranging markets.
    
    Entry conditions:
    - Price touches or exceeds Bollinger Band
    - RSI at extreme levels
    - Market in ranging regime
    
    Exit conditions:
    - Price returns to middle band
    - RSI normalizes
    - Stop loss hit
    """
    
    def _define_metadata(self) -> StrategyMetadata:
        return StrategyMetadata(
            name="mean_reversion",
            version="1.0.0",
            author="Trading Bot Framework",
            description="Counter-trend mean reversion using Bollinger Bands",
            category="mean_reversion",
            suitable_regimes=[Regime.RANGING, Regime.LOW_VOLATILITY],
            min_timeframe="15m",
            preferred_timeframe="1h",
            warmup_periods=50,
            max_concurrent_signals=2,
            parameter_schema={
                "bb_period": {"type": int, "min": 10, "max": 50, "default": 20},
                "bb_std": {"type": float, "min": 1.0, "max": 4.0, "default": 2.0},
                "rsi_period": {"type": int, "min": 7, "max": 21, "default": 14},
                "rsi_extreme_high": {"type": int, "min": 70, "max": 95, "default": 80},
                "rsi_extreme_low": {"type": int, "min": 5, "max": 30, "default": 20},
                "stop_loss_pct": {"type": float, "min": 0.5, "max": 5.0, "default": 1.5},
                "take_profit_pct": {"type": float, "min": 0.5, "max": 5.0, "default": 1.0},
            }
        )
    
    def _get_default_parameters(self) -> Dict[str, Any]:
        return {
            "bb_period": 20,
            "bb_std": 2.0,
            "rsi_period": 14,
            "rsi_extreme_high": 80,
            "rsi_extreme_low": 20,
            "stop_loss_pct": 1.5,
            "take_profit_pct": 1.0,
            "min_bb_width_pct": 1.0,  # Minimum BB width for trades
        }
    
    def generate_signal(self, context: StrategyContext) -> Optional[Signal]:
        """Generate mean reversion signal."""
        candles = context.candles.get(context.timeframe, [])
        
        if len(candles) < self.warmup_periods:
            return None
        
        # Calculate indicators
        indicators = self.calculate_indicators(candles)
        context.indicators = indicators
        
        # Only trade in ranging markets
        if context.current_regime in [Regime.TRENDING_UP, Regime.TRENDING_DOWN]:
            return None
        
        current_price = candles[-1].close
        upper_band = indicators.get("bb_upper", 0)
        lower_band = indicators.get("bb_lower", 0)
        middle_band = indicators.get("bb_middle", 0)
        bb_width_pct = indicators.get("bb_width_pct", 0)
        rsi = indicators.get("rsi", 50)
        
        # Skip if already in position
        if context.has_position:
            return self._check_exit_signal(context, indicators)
        
        # Skip if bands too narrow (low volatility)
        if bb_width_pct < self.get_parameter("min_bb_width_pct"):
            return None
        
        stop_loss_pct = self.get_parameter("stop_loss_pct") / 100
        take_profit_pct = self.get_parameter("take_profit_pct") / 100
        
        # Long signal: Price below lower band + RSI oversold
        if current_price <= lower_band and rsi < self.get_parameter("rsi_extreme_low"):
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = middle_band  # Target middle band
            
            return self._create_signal(
                signal_type=SignalType.ENTRY_LONG,
                price=current_price,
                context=context,
                stop_loss=stop_loss,
                take_profits=[{"price": take_profit, "pct": 100}],
                confidence=self._calculate_confidence(indicators, "long"),
                reason=f"Price below BB lower ({lower_band:.2f}), RSI={rsi:.1f}",
            )
        
        # Short signal: Price above upper band + RSI overbought
        if current_price >= upper_band and rsi > self.get_parameter("rsi_extreme_high"):
            stop_loss = current_price * (1 + stop_loss_pct)
            take_profit = middle_band  # Target middle band
            
            return self._create_signal(
                signal_type=SignalType.ENTRY_SHORT,
                price=current_price,
                context=context,
                stop_loss=stop_loss,
                take_profits=[{"price": take_profit, "pct": 100}],
                confidence=self._calculate_confidence(indicators, "short"),
                reason=f"Price above BB upper ({upper_band:.2f}), RSI={rsi:.1f}",
            )
        
        return None
    
    def _check_exit_signal(
        self,
        context: StrategyContext,
        indicators: Dict[str, float],
    ) -> Optional[Signal]:
        """Check for exit signals."""
        current_price = context.current_candle.close if context.current_candle else 0
        middle_band = indicators.get("bb_middle", current_price)
        rsi = indicators.get("rsi", 50)
        
        if context.position_side and context.position_side.value == "buy":
            # Exit long when price reaches middle band or RSI normalizes
            if current_price >= middle_band or rsi > 50:
                return self._create_signal(
                    signal_type=SignalType.EXIT_LONG,
                    price=current_price,
                    context=context,
                    reason=f"Mean reversion target reached, RSI={rsi:.1f}",
                )
        else:
            # Exit short when price reaches middle band or RSI normalizes
            if current_price <= middle_band or rsi < 50:
                return self._create_signal(
                    signal_type=SignalType.EXIT_SHORT,
                    price=current_price,
                    context=context,
                    reason=f"Mean reversion target reached, RSI={rsi:.1f}",
                )
        
        return None
    
    def calculate_indicators(self, candles: List[Candle]) -> Dict[str, float]:
        """Calculate Bollinger Bands and RSI."""
        closes = np.array([c.close for c in candles])
        
        bb_period = self.get_parameter("bb_period")
        bb_std = self.get_parameter("bb_std")
        rsi_period = self.get_parameter("rsi_period")
        
        # Bollinger Bands
        if len(closes) >= bb_period:
            middle = np.mean(closes[-bb_period:])
            std = np.std(closes[-bb_period:])
            upper = middle + (bb_std * std)
            lower = middle - (bb_std * std)
            bb_width_pct = ((upper - lower) / middle) * 100
        else:
            middle = closes[-1]
            upper = middle * 1.02
            lower = middle * 0.98
            bb_width_pct = 4.0
        
        # RSI
        rsi = self._rsi(closes, rsi_period)
        
        return {
            "bb_upper": upper,
            "bb_middle": middle,
            "bb_lower": lower,
            "bb_width_pct": bb_width_pct,
            "rsi": rsi,
            "close": closes[-1],
        }
    
    def _calculate_confidence(self, indicators: Dict[str, float], direction: str) -> float:
        """Calculate signal confidence."""
        confidence = 0.5
        
        rsi = indicators.get("rsi", 50)
        bb_width_pct = indicators.get("bb_width_pct", 0)
        
        if direction == "long":
            # More oversold = higher confidence
            if rsi < 15:
                confidence += 0.3
            elif rsi < 20:
                confidence += 0.2
        else:
            # More overbought = higher confidence
            if rsi > 85:
                confidence += 0.3
            elif rsi > 80:
                confidence += 0.2
        
        # Wider bands = higher confidence (more volatility to revert)
        if bb_width_pct > 3:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    @staticmethod
    def _rsi(data: np.ndarray, period: int = 14) -> float:
        """Calculate RSI."""
        if len(data) < period + 1:
            return 50.0
        
        deltas = np.diff(data)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        return float(100 - (100 / (1 + rs)))
