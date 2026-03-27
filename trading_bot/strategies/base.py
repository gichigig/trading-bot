"""
Base Strategy Class
====================

Abstract base class for all trading strategies.
Supports pluggable architecture, versioning, and configurable parameters.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
import logging
import hashlib
import json

from trading_bot.core.types import Candle, Signal, SignalType, Side, Regime
from trading_bot.core.events import Event, EventType, EventBus, get_event_bus

logger = logging.getLogger(__name__)


class StrategyState(Enum):
    """Strategy lifecycle states."""
    INITIALIZED = "initialized"
    WARMING_UP = "warming_up"
    READY = "ready"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class StrategyMetadata:
    """Strategy metadata for versioning and tracking."""
    name: str
    version: str
    author: str = ""
    description: str = ""
    category: str = ""  # momentum, mean_reversion, arbitrage, etc.
    suitable_regimes: List[Regime] = field(default_factory=list)
    min_timeframe: str = "1m"
    preferred_timeframe: str = "1h"
    symbols: List[str] = field(default_factory=list)
    
    # Performance hints
    warmup_periods: int = 100  # Candles needed before generating signals
    max_concurrent_signals: int = 1
    
    # Configuration schema (for validation)
    parameter_schema: Dict[str, Any] = field(default_factory=dict)
    
    def get_version_hash(self) -> str:
        """Generate a hash for this strategy version."""
        version_data = f"{self.name}:{self.version}"
        return hashlib.md5(version_data.encode()).hexdigest()[:8]


@dataclass
class StrategyContext:
    """Context passed to strategy on each tick/candle."""
    timestamp: datetime
    symbol: str
    timeframe: str
    
    # Market data
    candles: Dict[str, List[Candle]]  # timeframe -> candles
    current_candle: Optional[Candle] = None
    
    # Multi-timeframe bias
    higher_tf_bias: Optional[str] = None  # "bullish", "bearish", "neutral"
    higher_tf_regime: Optional[Regime] = None
    
    # Current state
    current_regime: Regime = Regime.UNKNOWN
    current_session: str = ""
    is_news_blackout: bool = False
    
    # Account state
    account_balance: float = 0.0
    available_margin: float = 0.0
    current_positions: int = 0
    
    # Existing positions for this strategy
    has_position: bool = False
    position_side: Optional[Side] = None
    position_size: float = 0.0
    position_pnl: float = 0.0
    
    # Indicator values (pre-calculated or calculated by strategy)
    indicators: Dict[str, float] = field(default_factory=dict)


class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies.
    
    Strategies are plugins that can be swapped, configured, and versioned.
    """
    
    def __init__(
        self,
        parameters: Optional[Dict[str, Any]] = None,
        event_bus: Optional[EventBus] = None,
    ):
        self._parameters = parameters or {}
        self._event_bus = event_bus or get_event_bus()
        self._state = StrategyState.INITIALIZED
        self._metadata = self._define_metadata()
        
        # Candle buffer for warmup
        self._candle_buffer: Dict[str, List[Candle]] = {}
        self._warmup_complete = False
        
        # Signal tracking
        self._last_signal: Optional[Signal] = None
        self._signal_count = 0
        self._active_signals: List[Signal] = []
        
        # Performance tracking
        self._signals_generated = 0
        self._signals_filtered = 0
        self._trades_taken = 0
        self._winning_trades = 0
        
        # Apply default parameters
        self._apply_defaults()
        
        logger.info(f"Strategy initialized: {self.name} v{self.version}")
    
    @abstractmethod
    def _define_metadata(self) -> StrategyMetadata:
        """Define strategy metadata. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def _get_default_parameters(self) -> Dict[str, Any]:
        """Return default parameters. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def generate_signal(self, context: StrategyContext) -> Optional[Signal]:
        """
        Generate trading signal based on current context.
        Must be implemented by subclasses.
        
        Returns None if no signal, or a Signal object.
        """
        pass
    
    def _apply_defaults(self) -> None:
        """Apply default parameters where not specified."""
        defaults = self._get_default_parameters()
        for key, value in defaults.items():
            if key not in self._parameters:
                self._parameters[key] = value
    
    @property
    def name(self) -> str:
        return self._metadata.name
    
    @property
    def version(self) -> str:
        return self._metadata.version
    
    @property
    def version_hash(self) -> str:
        return self._metadata.get_version_hash()
    
    @property
    def state(self) -> StrategyState:
        return self._state
    
    @property
    def is_ready(self) -> bool:
        return self._state in [StrategyState.READY, StrategyState.RUNNING]
    
    @property
    def warmup_periods(self) -> int:
        return self._metadata.warmup_periods
    
    def get_parameter(self, key: str, default: Any = None) -> Any:
        """Get a parameter value."""
        return self._parameters.get(key, default)
    
    def set_parameter(self, key: str, value: Any) -> None:
        """Set a parameter value at runtime."""
        old_value = self._parameters.get(key)
        self._parameters[key] = value
        logger.info(f"Strategy {self.name}: parameter {key} changed from {old_value} to {value}")
    
    def get_all_parameters(self) -> Dict[str, Any]:
        """Get all current parameters."""
        return self._parameters.copy()
    
    def validate_parameters(self) -> List[str]:
        """Validate current parameters. Returns list of errors."""
        errors = []
        schema = self._metadata.parameter_schema
        
        for param_name, param_spec in schema.items():
            if param_spec.get("required", False) and param_name not in self._parameters:
                errors.append(f"Missing required parameter: {param_name}")
            
            if param_name in self._parameters:
                value = self._parameters[param_name]
                
                # Type check
                expected_type = param_spec.get("type")
                if expected_type and not isinstance(value, expected_type):
                    errors.append(f"Parameter {param_name} should be {expected_type}, got {type(value)}")
                
                # Range check
                min_val = param_spec.get("min")
                max_val = param_spec.get("max")
                if min_val is not None and value < min_val:
                    errors.append(f"Parameter {param_name} ({value}) below minimum ({min_val})")
                if max_val is not None and value > max_val:
                    errors.append(f"Parameter {param_name} ({value}) above maximum ({max_val})")
        
        return errors
    
    def initialize(self) -> None:
        """Initialize strategy. Called once at startup."""
        self._state = StrategyState.WARMING_UP
        self._publish_event(EventType.STRATEGY_LOADED, {"status": "initialized"})
    
    def start(self) -> None:
        """Start strategy trading."""
        if self._warmup_complete:
            self._state = StrategyState.RUNNING
        else:
            self._state = StrategyState.WARMING_UP
        self._publish_event(EventType.STRATEGY_STARTED, {"status": "started"})
    
    def stop(self) -> None:
        """Stop strategy trading."""
        self._state = StrategyState.STOPPED
        self._publish_event(EventType.STRATEGY_STOPPED, {"status": "stopped"})
    
    def pause(self) -> None:
        """Pause strategy (no new signals)."""
        self._state = StrategyState.PAUSED
    
    def resume(self) -> None:
        """Resume strategy after pause."""
        if self._warmup_complete:
            self._state = StrategyState.RUNNING
        else:
            self._state = StrategyState.WARMING_UP
    
    def on_candle(self, candle: Candle, context: StrategyContext) -> Optional[Signal]:
        """
        Process a new candle. Called by the bot on each new candle.
        
        Handles warmup, then delegates to generate_signal.
        """
        # Update candle buffer
        tf = candle.timeframe
        if tf not in self._candle_buffer:
            self._candle_buffer[tf] = []
        
        self._candle_buffer[tf].append(candle)
        
        # Trim buffer
        max_buffer = self.warmup_periods * 2
        if len(self._candle_buffer[tf]) > max_buffer:
            self._candle_buffer[tf] = self._candle_buffer[tf][-max_buffer:]
        
        # Check warmup
        if not self._warmup_complete:
            if len(self._candle_buffer.get(self._metadata.preferred_timeframe, [])) >= self.warmup_periods:
                self._warmup_complete = True
                self._state = StrategyState.RUNNING
                logger.info(f"Strategy {self.name}: warmup complete")
            else:
                return None
        
        # Don't generate signals if paused or stopped
        if self._state not in [StrategyState.RUNNING, StrategyState.READY]:
            return None
        
        # Add candle buffer to context
        context.candles = self._candle_buffer
        context.current_candle = candle
        
        # Generate signal
        try:
            signal = self.generate_signal(context)
            
            if signal:
                self._signals_generated += 1
                self._last_signal = signal
                self._publish_event(EventType.SIGNAL_GENERATED, signal.to_dict())
            
            return signal
            
        except Exception as e:
            logger.error(f"Strategy {self.name} error: {e}", exc_info=True)
            self._state = StrategyState.ERROR
            self._publish_event(EventType.STRATEGY_ERROR, {"error": str(e)})
            return None
    
    def on_tick(self, price: float, context: StrategyContext) -> Optional[Signal]:
        """
        Process a price tick. Override for tick-based strategies.
        Default implementation does nothing.
        """
        return None
    
    def on_position_opened(self, position_id: str, entry_price: float, quantity: float) -> None:
        """Called when a position is opened from this strategy's signal."""
        self._trades_taken += 1
    
    def on_position_closed(self, position_id: str, exit_price: float, pnl: float) -> None:
        """Called when a position from this strategy is closed."""
        if pnl > 0:
            self._winning_trades += 1
    
    def calculate_indicators(self, candles: List[Candle]) -> Dict[str, float]:
        """
        Calculate indicators for decision making.
        Override to add custom indicators.
        """
        return {}
    
    def should_filter_signal(self, signal: Signal, context: StrategyContext) -> Tuple[bool, str]:
        """
        Check if signal should be filtered out.
        Override to add custom filters.
        
        Returns (should_filter, reason)
        """
        # Default: don't filter
        return False, ""
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get strategy statistics."""
        win_rate = (self._winning_trades / self._trades_taken * 100) if self._trades_taken else 0
        
        return {
            "name": self.name,
            "version": self.version,
            "state": self._state.value,
            "warmup_complete": self._warmup_complete,
            "signals_generated": self._signals_generated,
            "signals_filtered": self._signals_filtered,
            "trades_taken": self._trades_taken,
            "winning_trades": self._winning_trades,
            "win_rate": win_rate,
            "parameters": self._parameters,
        }
    
    def _publish_event(self, event_type: EventType, data: Dict[str, Any]) -> None:
        """Publish an event."""
        event = Event(
            event_type=event_type,
            source=f"strategy:{self.name}",
            data={**data, "strategy_name": self.name, "strategy_version": self.version},
        )
        self._event_bus.publish(event)
    
    def _create_signal(
        self,
        signal_type: SignalType,
        price: float,
        context: StrategyContext,
        stop_loss: Optional[float] = None,
        take_profits: Optional[List[Dict[str, float]]] = None,
        confidence: float = 1.0,
        reason: str = "",
    ) -> Signal:
        """Helper to create a properly formatted signal."""
        import uuid
        
        signal = Signal(
            signal_id=str(uuid.uuid4()),
            strategy_name=self.name,
            strategy_version=self.version,
            symbol=context.symbol,
            signal_type=signal_type,
            timestamp=context.timestamp,
            price=price,
            confidence=confidence,
            entry_price=price,
            stop_loss=stop_loss,
            take_profits=take_profits or [],
            timeframe=context.timeframe,
            bias_timeframe=self._metadata.preferred_timeframe,
            bias_direction=context.higher_tf_bias,
            indicators=context.indicators.copy(),
            reason=reason,
            tags=[self._metadata.category] if self._metadata.category else [],
        )
        
        # Calculate risk/reward if stop loss provided
        if stop_loss and take_profits:
            risk = abs(price - stop_loss)
            reward = abs(take_profits[0].get("price", price) - price)
            signal.risk_reward_ratio = reward / risk if risk else 0
        
        return signal
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, version={self.version}, state={self._state.value})"
