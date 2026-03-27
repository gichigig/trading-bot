"""
Momentum Strategy
==================

A trend-following momentum strategy using RSI and EMA crossovers.
Demonstrates:
- Configurable parameters
- Multi-timeframe confirmation
- Risk-defined entries
"""

from typing import Any, Dict, List, Optional
import numpy as np

from trading_bot.strategies.base import BaseStrategy, StrategyMetadata, StrategyContext
from trading_bot.strategies.registry import register_strategy
from trading_bot.core.types import Signal, SignalType, Candle, Regime


@register_strategy
class MomentumStrategy(BaseStrategy):
    """
    Momentum strategy that trades in the direction of the trend.
    
    Entry conditions:
    - Fast EMA crosses above/below slow EMA
    - RSI confirms momentum direction
    - Higher timeframe bias aligns
    
    Exit conditions:
    - RSI reaches overbought/oversold
    - EMA crossover reversal
    - Stop loss / Take profit hit
    """
    
    def _define_metadata(self) -> StrategyMetadata:
        return StrategyMetadata(
            name="momentum",
            version="1.0.0",
            author="Trading Bot Framework",
            description="Trend-following momentum strategy using EMA crossovers and RSI",
            category="momentum",
            suitable_regimes=[Regime.TRENDING_UP, Regime.TRENDING_DOWN],
            min_timeframe="5m",
            preferred_timeframe="1h",
            warmup_periods=200,
            max_concurrent_signals=1,
            parameter_schema={
                "fast_ema_period": {"type": int, "min": 5, "max": 50, "default": 12},
                "slow_ema_period": {"type": int, "min": 20, "max": 200, "default": 26},
                "rsi_period": {"type": int, "min": 7, "max": 21, "default": 14},
                "rsi_overbought": {"type": int, "min": 60, "max": 90, "default": 70},
                "rsi_oversold": {"type": int, "min": 10, "max": 40, "default": 30},
                "stop_loss_atr_mult": {"type": float, "min": 1.0, "max": 5.0, "default": 2.0},
                "take_profit_atr_mult": {"type": float, "min": 1.5, "max": 10.0, "default": 3.0},
                "require_htf_confirmation": {"type": bool, "default": True},
            }
        )
    
    def _get_default_parameters(self) -> Dict[str, Any]:
        return {
            "fast_ema_period": 12,
            "slow_ema_period": 26,
            "rsi_period": 14,
            "rsi_overbought": 70,
            "rsi_oversold": 30,
            "stop_loss_atr_mult": 2.0,
            "take_profit_atr_mult": 3.0,
            "require_htf_confirmation": True,
            "atr_period": 14,
        }
    
    def generate_signal(self, context: StrategyContext) -> Optional[Signal]:
        """Generate momentum trading signal."""
        candles = context.candles.get(context.timeframe, [])
        
        if len(candles) < self.warmup_periods:
            return None
        
        # Calculate indicators
        indicators = self.calculate_indicators(candles)
        context.indicators = indicators
        
        fast_ema = indicators.get("fast_ema", 0)
        slow_ema = indicators.get("slow_ema", 0)
        prev_fast_ema = indicators.get("prev_fast_ema", 0)
        prev_slow_ema = indicators.get("prev_slow_ema", 0)
        rsi = indicators.get("rsi", 50)
        atr = indicators.get("atr", 0)
        
        current_price = candles[-1].close
        
        # Check for EMA crossover
        bullish_cross = prev_fast_ema <= prev_slow_ema and fast_ema > slow_ema
        bearish_cross = prev_fast_ema >= prev_slow_ema and fast_ema < slow_ema
        
        rsi_oversold = self.get_parameter("rsi_oversold")
        rsi_overbought = self.get_parameter("rsi_overbought")
        require_htf = self.get_parameter("require_htf_confirmation")
        
        # Skip if already in position
        if context.has_position:
            return self._check_exit_signal(context, indicators)
        
        # Check regime filter
        if context.current_regime not in [Regime.TRENDING_UP, Regime.TRENDING_DOWN, Regime.UNKNOWN]:
            return None
        
        # Long signal
        if bullish_cross and rsi < rsi_overbought:
            # Check higher timeframe confirmation
            if require_htf and context.higher_tf_bias == "bearish":
                return None
            
            # Calculate stops
            stop_loss = current_price - (atr * self.get_parameter("stop_loss_atr_mult"))
            take_profit = current_price + (atr * self.get_parameter("take_profit_atr_mult"))
            
            return self._create_signal(
                signal_type=SignalType.ENTRY_LONG,
                price=current_price,
                context=context,
                stop_loss=stop_loss,
                take_profits=[
                    {"price": take_profit * 0.6 + current_price * 0.4, "pct": 50},  # Partial TP
                    {"price": take_profit, "pct": 50},  # Final TP
                ],
                confidence=self._calculate_confidence(indicators, "long"),
                reason=f"Bullish EMA crossover with RSI={rsi:.1f}",
            )
        
        # Short signal
        if bearish_cross and rsi > rsi_oversold:
            # Check higher timeframe confirmation
            if require_htf and context.higher_tf_bias == "bullish":
                return None
            
            # Calculate stops
            stop_loss = current_price + (atr * self.get_parameter("stop_loss_atr_mult"))
            take_profit = current_price - (atr * self.get_parameter("take_profit_atr_mult"))
            
            return self._create_signal(
                signal_type=SignalType.ENTRY_SHORT,
                price=current_price,
                context=context,
                stop_loss=stop_loss,
                take_profits=[
                    {"price": take_profit * 0.6 + current_price * 0.4, "pct": 50},
                    {"price": take_profit, "pct": 50},
                ],
                confidence=self._calculate_confidence(indicators, "short"),
                reason=f"Bearish EMA crossover with RSI={rsi:.1f}",
            )
        
        return None
    
    def _check_exit_signal(
        self,
        context: StrategyContext,
        indicators: Dict[str, float],
    ) -> Optional[Signal]:
        """Check for exit signals when in position."""
        rsi = indicators.get("rsi", 50)
        fast_ema = indicators.get("fast_ema", 0)
        slow_ema = indicators.get("slow_ema", 0)
        
        current_price = context.current_candle.close if context.current_candle else 0
        
        if context.position_side and context.position_side.value == "buy":
            # Exit long on RSI overbought or bearish cross
            if rsi > self.get_parameter("rsi_overbought") or fast_ema < slow_ema:
                return self._create_signal(
                    signal_type=SignalType.EXIT_LONG,
                    price=current_price,
                    context=context,
                    reason=f"Exit long: RSI={rsi:.1f}, EMA bearish={fast_ema < slow_ema}",
                )
        else:
            # Exit short on RSI oversold or bullish cross
            if rsi < self.get_parameter("rsi_oversold") or fast_ema > slow_ema:
                return self._create_signal(
                    signal_type=SignalType.EXIT_SHORT,
                    price=current_price,
                    context=context,
                    reason=f"Exit short: RSI={rsi:.1f}, EMA bullish={fast_ema > slow_ema}",
                )
        
        return None
    
    def calculate_indicators(self, candles: List[Candle]) -> Dict[str, float]:
        """Calculate strategy indicators."""
        closes = np.array([c.close for c in candles])
        highs = np.array([c.high for c in candles])
        lows = np.array([c.low for c in candles])
        
        fast_period = self.get_parameter("fast_ema_period")
        slow_period = self.get_parameter("slow_ema_period")
        rsi_period = self.get_parameter("rsi_period")
        atr_period = self.get_parameter("atr_period")
        
        # Calculate EMAs
        fast_ema = self._ema(closes, fast_period)
        slow_ema = self._ema(closes, slow_period)
        
        prev_fast_ema = self._ema(closes[:-1], fast_period) if len(closes) > 1 else fast_ema
        prev_slow_ema = self._ema(closes[:-1], slow_period) if len(closes) > 1 else slow_ema
        
        # Calculate RSI
        rsi = self._rsi(closes, rsi_period)
        
        # Calculate ATR
        atr = self._atr(highs, lows, closes, atr_period)
        
        return {
            "fast_ema": fast_ema,
            "slow_ema": slow_ema,
            "prev_fast_ema": prev_fast_ema,
            "prev_slow_ema": prev_slow_ema,
            "rsi": rsi,
            "atr": atr,
            "close": closes[-1],
        }
    
    def _calculate_confidence(self, indicators: Dict[str, float], direction: str) -> float:
        """Calculate signal confidence based on indicator alignment."""
        confidence = 0.5
        
        rsi = indicators.get("rsi", 50)
        fast_ema = indicators.get("fast_ema", 0)
        slow_ema = indicators.get("slow_ema", 0)
        
        if direction == "long":
            # RSI in favorable zone
            if 30 < rsi < 50:
                confidence += 0.2
            elif rsi < 30:
                confidence += 0.1
            
            # EMA gap strength
            gap_pct = (fast_ema - slow_ema) / slow_ema * 100 if slow_ema else 0
            if gap_pct > 0.5:
                confidence += 0.2
        else:
            # RSI in favorable zone
            if 50 < rsi < 70:
                confidence += 0.2
            elif rsi > 70:
                confidence += 0.1
            
            # EMA gap strength
            gap_pct = (slow_ema - fast_ema) / slow_ema * 100 if slow_ema else 0
            if gap_pct > 0.5:
                confidence += 0.2
        
        return min(confidence, 1.0)
    
    @staticmethod
    def _ema(data: np.ndarray, period: int) -> float:
        """Calculate Exponential Moving Average."""
        if len(data) < period:
            return float(data[-1]) if len(data) > 0 else 0.0
        
        multiplier = 2 / (period + 1)
        ema = float(data[:period].mean())
        
        for price in data[period:]:
            ema = (price - ema) * multiplier + ema
        
        return ema
    
    @staticmethod
    def _rsi(data: np.ndarray, period: int = 14) -> float:
        """Calculate Relative Strength Index."""
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
        rsi = 100 - (100 / (1 + rs))
        
        return float(rsi)
    
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
