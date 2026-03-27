"""
Risk Engine
============

Central risk management engine coordinating all risk checks.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import logging

from trading_bot.core.types import Signal, Position, Trade, Side, RiskMetrics
from trading_bot.core.config import RiskConfig
from trading_bot.core.events import Event, EventType, EventBus, get_event_bus
from trading_bot.risk.position_sizer import PositionSizer, PositionSizeResult
from trading_bot.risk.circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)


@dataclass
class RiskCheckResult:
    """Result of a risk check."""
    approved: bool
    checks_passed: List[str]
    checks_failed: List[str]
    warnings: List[str]
    position_size: Optional[PositionSizeResult] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "approved": self.approved,
            "checks_passed": self.checks_passed,
            "checks_failed": self.checks_failed,
            "warnings": self.warnings,
            "position_size": self.position_size.to_dict() if self.position_size else None,
        }


class RiskEngine:
    """
    Central risk management engine.
    
    Responsibilities:
    - Coordinate all risk checks
    - Calculate position sizes
    - Enforce circuit breakers
    - Track exposure and correlations
    - Generate risk events
    """
    
    def __init__(
        self,
        risk_config: RiskConfig,
        event_bus: Optional[EventBus] = None,
    ):
        self.config = risk_config
        self._event_bus = event_bus or get_event_bus()
        
        # Components
        self._position_sizer = PositionSizer(risk_config)
        self._circuit_breaker = CircuitBreaker(risk_config, event_bus)
        
        # Current state
        self._account_balance: float = 0.0
        self._positions: Dict[str, Position] = {}
        self._pending_signals: Dict[str, Signal] = {}
        
        # Daily reset
        self._last_day: Optional[datetime] = None
    
    def check_signal(
        self,
        signal: Signal,
        account_balance: float,
        current_atr: Optional[float] = None,
    ) -> RiskCheckResult:
        """
        Perform comprehensive risk check on a signal.
        
        Returns RiskCheckResult with approval status and position size.
        """
        self._account_balance = account_balance
        
        checks_passed = []
        checks_failed = []
        warnings = []
        
        # Check 1: Circuit breaker
        trading_allowed, reason = self._circuit_breaker.check_trade_allowed()
        if not trading_allowed:
            checks_failed.append(f"Circuit breaker: {reason}")
            return RiskCheckResult(
                approved=False,
                checks_passed=checks_passed,
                checks_failed=checks_failed,
                warnings=warnings,
            )
        checks_passed.append("Circuit breaker OK")
        
        # Check 2: Maximum positions
        if len(self._positions) >= self.config.max_positions:
            checks_failed.append(f"Max positions ({self.config.max_positions}) reached")
            return RiskCheckResult(
                approved=False,
                checks_passed=checks_passed,
                checks_failed=checks_failed,
                warnings=warnings,
            )
        checks_passed.append(f"Position count OK ({len(self._positions)}/{self.config.max_positions})")
        
        # Check 3: Symbol already has position
        if signal.symbol in self._positions:
            warnings.append(f"Already have position in {signal.symbol}")
        
        # Check 4: Calculate position size
        if not signal.stop_loss:
            checks_failed.append("Signal missing stop loss")
            return RiskCheckResult(
                approved=False,
                checks_passed=checks_passed,
                checks_failed=checks_failed,
                warnings=warnings,
            )
        
        entry_price = signal.entry_price or signal.price
        position_size_result = self._position_sizer.calculate_position_size(
            signal=signal,
            account_balance=account_balance,
            entry_price=entry_price,
            stop_loss=signal.stop_loss,
            current_atr=current_atr,
        )
        
        if not position_size_result.approved:
            checks_failed.append(f"Position size: {position_size_result.rejection_reason}")
            return RiskCheckResult(
                approved=False,
                checks_passed=checks_passed,
                checks_failed=checks_failed,
                warnings=warnings,
                position_size=position_size_result,
            )
        checks_passed.append(f"Position size OK ({position_size_result.quantity})")
        
        # Check 5: Risk-reward ratio
        if signal.risk_reward_ratio > 0:
            if signal.risk_reward_ratio < 1.0:
                warnings.append(f"Low R:R ratio ({signal.risk_reward_ratio:.2f})")
            elif signal.risk_reward_ratio >= 2.0:
                checks_passed.append(f"Good R:R ratio ({signal.risk_reward_ratio:.2f})")
        
        # Check 6: Volatility adjustment warnings
        if position_size_result.volatility_multiplier < 0.7:
            warnings.append(f"Position reduced due to high volatility (mult={position_size_result.volatility_multiplier:.2f})")
        
        # Check 7: Correlation warnings
        if position_size_result.correlation_adjustment < 1.0:
            warnings.append(f"Position reduced due to correlation (adj={position_size_result.correlation_adjustment:.2f})")
        
        # Publish risk check event
        self._event_bus.publish(Event(
            event_type=EventType.RISK_CHECK_PASSED,
            source="risk_engine",
            data={
                "signal_id": signal.signal_id,
                "symbol": signal.symbol,
                "checks_passed": checks_passed,
                "warnings": warnings,
                "position_size": position_size_result.quantity,
            },
        ))
        
        return RiskCheckResult(
            approved=True,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            warnings=warnings,
            position_size=position_size_result,
        )
    
    def on_position_opened(self, position: Position) -> None:
        """Handle position opened event."""
        self._positions[position.symbol] = position
        
        # Register with position sizer
        self._position_sizer.register_position(
            symbol=position.symbol,
            side=position.side,
            quantity=position.quantity,
            entry_price=position.entry_price,
        )
        
        logger.info(f"Position opened: {position.symbol} {position.side.value} {position.quantity}")
    
    def on_position_closed(self, position: Position, trade: Trade) -> None:
        """Handle position closed event."""
        # Remove from tracking
        self._positions.pop(position.symbol, None)
        self._position_sizer.unregister_position(position.symbol)
        
        # Update circuit breaker
        self._circuit_breaker.on_trade_closed(trade)
        
        logger.info(f"Position closed: {position.symbol} PnL=${trade.net_pnl:.2f}")
    
    def update_equity(self, equity: float) -> None:
        """Update current account equity."""
        self._account_balance = equity
        self._circuit_breaker.update_equity(equity)
        
        # Check for new day
        today = datetime.utcnow().date()
        if self._last_day != today:
            self._circuit_breaker.new_day(equity)
            self._last_day = today
    
    def update_volatility(
        self,
        symbol: str,
        current_atr: float,
        avg_atr: float,
    ) -> None:
        """Update volatility data for a symbol."""
        self._position_sizer.update_volatility_data(symbol, current_atr, avg_atr)
    
    def update_correlation(
        self,
        symbol1: str,
        symbol2: str,
        correlation: float,
    ) -> None:
        """Update correlation between symbols."""
        self._position_sizer.update_correlation(symbol1, symbol2, correlation)
    
    def emergency_stop(self, reason: str = "Emergency stop") -> None:
        """Trigger emergency stop - close all positions."""
        logger.critical(f"EMERGENCY STOP: {reason}")
        
        self._circuit_breaker.manual_stop(reason)
        
        self._event_bus.publish(Event(
            event_type=EventType.PANIC_STOP,
            source="risk_engine",
            data={
                "reason": reason,
                "positions_to_close": list(self._positions.keys()),
            },
        ))
    
    def resume_trading(self) -> bool:
        """Resume trading after manual stop."""
        self._circuit_breaker.manual_resume()
        return self._circuit_breaker.check_trade_allowed()[0]
    
    def reset_circuit_breaker(self, force: bool = False) -> bool:
        """Reset the circuit breaker."""
        return self._circuit_breaker.reset(force)
    
    def get_risk_metrics(self) -> RiskMetrics:
        """Get current risk metrics."""
        metrics = self._circuit_breaker.get_risk_metrics()
        
        # Add position-specific metrics
        metrics.position_count = len(self._positions)
        metrics.total_exposure = sum(
            p.notional_value for p in self._positions.values()
        )
        metrics.exposure_pct = (
            (metrics.total_exposure / self._account_balance * 100)
            if self._account_balance else 0
        )
        
        return metrics
    
    def get_position_summary(self) -> Dict[str, Any]:
        """Get summary of all positions."""
        total_unrealized = sum(p.unrealized_pnl for p in self._positions.values())
        total_notional = sum(p.notional_value for p in self._positions.values())
        
        return {
            "count": len(self._positions),
            "total_unrealized_pnl": total_unrealized,
            "total_notional": total_notional,
            "symbols": list(self._positions.keys()),
            "positions": [
                {
                    "symbol": p.symbol,
                    "side": p.side.value,
                    "quantity": p.quantity,
                    "entry_price": p.entry_price,
                    "unrealized_pnl": p.unrealized_pnl,
                }
                for p in self._positions.values()
            ],
        }
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive risk engine status."""
        return {
            "trading_allowed": self._circuit_breaker.check_trade_allowed()[0],
            "circuit_breaker": self._circuit_breaker.get_status(),
            "position_sizer": self._position_sizer.get_stats(),
            "positions": self.get_position_summary(),
            "risk_metrics": self.get_risk_metrics().to_dict(),
        }
