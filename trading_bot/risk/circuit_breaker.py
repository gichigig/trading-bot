"""
Circuit Breaker
================

Hard risk limits and emergency stop mechanisms.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
import logging

from trading_bot.core.types import RiskMetrics, Trade
from trading_bot.core.config import RiskConfig
from trading_bot.core.events import Event, EventType, EventBus, get_event_bus

logger = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    NORMAL = "normal"
    WARNING = "warning"
    TRIGGERED = "triggered"
    PANIC = "panic"


@dataclass
class CircuitBreakerTrigger:
    """Record of a circuit breaker trigger."""
    trigger_type: str
    triggered_at: datetime
    value: float
    threshold: float
    message: str
    auto_reset_at: Optional[datetime] = None


class CircuitBreaker:
    """
    Circuit breaker for hard risk limits.
    
    Features:
    - Daily loss limit
    - Maximum consecutive losses
    - Drawdown kill-switch
    - Global panic stop
    - Automatic and manual reset
    """
    
    def __init__(
        self,
        risk_config: RiskConfig,
        event_bus: Optional[EventBus] = None,
    ):
        self.config = risk_config
        self._event_bus = event_bus or get_event_bus()
        
        # State
        self._state = CircuitBreakerState.NORMAL
        self._trading_allowed = True
        
        # Daily tracking
        self._daily_pnl: float = 0.0
        self._daily_trades: int = 0
        self._daily_wins: int = 0
        self._daily_losses: int = 0
        self._day_start_equity: float = 0.0
        self._current_day: Optional[datetime] = None
        
        # Consecutive loss tracking
        self._consecutive_losses: int = 0
        self._consecutive_wins: int = 0
        
        # Drawdown tracking
        self._peak_equity: float = 0.0
        self._current_equity: float = 0.0
        self._current_drawdown: float = 0.0
        self._current_drawdown_pct: float = 0.0
        self._max_drawdown: float = 0.0
        self._max_drawdown_pct: float = 0.0
        
        # Trigger history
        self._triggers: List[CircuitBreakerTrigger] = []
        self._active_trigger: Optional[CircuitBreakerTrigger] = None
        
        # Manual override
        self._manual_stop: bool = False
        self._manual_stop_reason: str = ""
    
    def check_trade_allowed(self) -> Tuple[bool, str]:
        """
        Check if trading is currently allowed.
        
        Returns (is_allowed, reason)
        """
        if self._manual_stop:
            return False, f"Manual stop: {self._manual_stop_reason}"
        
        if self._state == CircuitBreakerState.PANIC:
            return False, "Panic stop active"
        
        if self._state == CircuitBreakerState.TRIGGERED:
            if self._active_trigger:
                return False, f"Circuit breaker triggered: {self._active_trigger.message}"
            return False, "Circuit breaker triggered"
        
        return True, "Trading allowed"
    
    def on_trade_closed(self, trade: Trade) -> None:
        """Process a closed trade and update metrics."""
        # Update daily stats
        self._daily_pnl += trade.net_pnl
        self._daily_trades += 1
        
        if trade.net_pnl >= 0:
            self._daily_wins += 1
            self._consecutive_wins += 1
            self._consecutive_losses = 0
        else:
            self._daily_losses += 1
            self._consecutive_losses += 1
            self._consecutive_wins = 0
        
        # Check circuit breakers
        self._check_all_limits()
    
    def update_equity(self, equity: float) -> None:
        """Update current equity and check drawdown limits."""
        self._current_equity = equity
        
        # Initialize if needed
        if self._peak_equity == 0:
            self._peak_equity = equity
            self._day_start_equity = equity
        
        # Update peak
        if equity > self._peak_equity:
            self._peak_equity = equity
        
        # Calculate drawdown
        self._current_drawdown = self._peak_equity - equity
        self._current_drawdown_pct = (self._current_drawdown / self._peak_equity * 100) if self._peak_equity else 0
        
        # Update max drawdown
        if self._current_drawdown > self._max_drawdown:
            self._max_drawdown = self._current_drawdown
            self._max_drawdown_pct = self._current_drawdown_pct
        
        # Check drawdown limits
        self._check_drawdown_limit()
    
    def new_day(self, equity: float) -> None:
        """Reset daily tracking for a new trading day."""
        self._daily_pnl = 0.0
        self._daily_trades = 0
        self._daily_wins = 0
        self._daily_losses = 0
        self._day_start_equity = equity
        self._current_day = datetime.utcnow().date()
        
        # Check for auto-reset
        if self._state == CircuitBreakerState.TRIGGERED:
            if self._active_trigger and self._active_trigger.auto_reset_at:
                if datetime.utcnow() >= self._active_trigger.auto_reset_at:
                    self.reset()
        
        logger.info(f"New trading day started. Equity: {equity}")
    
    def _check_all_limits(self) -> None:
        """Check all circuit breaker conditions."""
        self._check_daily_loss_limit()
        self._check_consecutive_losses()
        self._check_drawdown_limit()
        self._check_panic_conditions()
    
    def _check_daily_loss_limit(self) -> None:
        """Check if daily loss limit is breached."""
        if self._state in [CircuitBreakerState.TRIGGERED, CircuitBreakerState.PANIC]:
            return
        
        # Check absolute limit
        if self.config.daily_max_loss_absolute > 0:
            if abs(self._daily_pnl) >= self.config.daily_max_loss_absolute and self._daily_pnl < 0:
                self._trigger(
                    trigger_type="daily_loss_absolute",
                    value=self._daily_pnl,
                    threshold=self.config.daily_max_loss_absolute,
                    message=f"Daily loss (${abs(self._daily_pnl):.2f}) hit absolute limit (${self.config.daily_max_loss_absolute})",
                )
                return
        
        # Check percentage limit
        if self._day_start_equity > 0:
            daily_loss_pct = (self._daily_pnl / self._day_start_equity) * 100
            
            if daily_loss_pct <= -self.config.daily_max_loss_pct:
                self._trigger(
                    trigger_type="daily_loss_pct",
                    value=daily_loss_pct,
                    threshold=self.config.daily_max_loss_pct,
                    message=f"Daily loss ({abs(daily_loss_pct):.2f}%) hit percentage limit ({self.config.daily_max_loss_pct}%)",
                )
    
    def _check_consecutive_losses(self) -> None:
        """Check if consecutive loss limit is breached."""
        if self._state in [CircuitBreakerState.TRIGGERED, CircuitBreakerState.PANIC]:
            return
        
        if self._consecutive_losses >= self.config.max_consecutive_losses:
            self._trigger(
                trigger_type="consecutive_losses",
                value=self._consecutive_losses,
                threshold=self.config.max_consecutive_losses,
                message=f"Consecutive losses ({self._consecutive_losses}) hit limit ({self.config.max_consecutive_losses})",
            )
    
    def _check_drawdown_limit(self) -> None:
        """Check if drawdown limit is breached."""
        if self._state == CircuitBreakerState.PANIC:
            return
        
        # Check max drawdown
        if self._current_drawdown_pct >= self.config.max_drawdown_pct:
            self._trigger(
                trigger_type="max_drawdown",
                value=self._current_drawdown_pct,
                threshold=self.config.max_drawdown_pct,
                message=f"Drawdown ({self._current_drawdown_pct:.2f}%) hit limit ({self.config.max_drawdown_pct}%)",
            )
    
    def _check_panic_conditions(self) -> None:
        """Check for panic stop conditions."""
        if not self.config.panic_stop_enabled:
            return
        
        if self._current_drawdown_pct >= self.config.panic_stop_drawdown_pct:
            self._panic_stop(
                f"Panic drawdown ({self._current_drawdown_pct:.2f}%) hit ({self.config.panic_stop_drawdown_pct}%)"
            )
    
    def _trigger(
        self,
        trigger_type: str,
        value: float,
        threshold: float,
        message: str,
        auto_reset_hours: int = 24,
    ) -> None:
        """Trigger the circuit breaker."""
        logger.warning(f"Circuit breaker triggered: {message}")
        
        self._state = CircuitBreakerState.TRIGGERED
        self._trading_allowed = False
        
        trigger = CircuitBreakerTrigger(
            trigger_type=trigger_type,
            triggered_at=datetime.utcnow(),
            value=value,
            threshold=threshold,
            message=message,
            auto_reset_at=datetime.utcnow() + timedelta(hours=auto_reset_hours),
        )
        
        self._active_trigger = trigger
        self._triggers.append(trigger)
        
        # Publish event
        self._event_bus.publish(Event(
            event_type=EventType.RISK_LIMIT_BREACH,
            source="circuit_breaker",
            data={
                "trigger_type": trigger_type,
                "value": value,
                "threshold": threshold,
                "message": message,
                "state": self._state.value,
            },
        ))
    
    def _panic_stop(self, reason: str) -> None:
        """Activate panic stop - emergency shutdown."""
        logger.critical(f"PANIC STOP ACTIVATED: {reason}")
        
        self._state = CircuitBreakerState.PANIC
        self._trading_allowed = False
        
        trigger = CircuitBreakerTrigger(
            trigger_type="panic",
            triggered_at=datetime.utcnow(),
            value=self._current_drawdown_pct,
            threshold=self.config.panic_stop_drawdown_pct,
            message=reason,
            auto_reset_at=None,  # Panic requires manual reset
        )
        
        self._active_trigger = trigger
        self._triggers.append(trigger)
        
        # Publish panic event
        self._event_bus.publish(Event(
            event_type=EventType.PANIC_STOP,
            source="circuit_breaker",
            data={
                "reason": reason,
                "drawdown_pct": self._current_drawdown_pct,
                "daily_pnl": self._daily_pnl,
            },
        ))
    
    def manual_stop(self, reason: str = "Manual intervention") -> None:
        """Manually stop trading."""
        self._manual_stop = True
        self._manual_stop_reason = reason
        self._trading_allowed = False
        
        logger.warning(f"Manual stop activated: {reason}")
        
        self._event_bus.publish(Event(
            event_type=EventType.ALERT_WARNING,
            source="circuit_breaker",
            data={"message": f"Manual stop: {reason}"},
        ))
    
    def manual_resume(self) -> None:
        """Resume from manual stop."""
        self._manual_stop = False
        self._manual_stop_reason = ""
        
        if self._state not in [CircuitBreakerState.TRIGGERED, CircuitBreakerState.PANIC]:
            self._trading_allowed = True
            logger.info("Manual stop cleared, trading resumed")
    
    def reset(self, force: bool = False) -> bool:
        """
        Reset circuit breaker to normal state.
        
        Args:
            force: Force reset even from panic state
            
        Returns:
            True if reset successful
        """
        if self._state == CircuitBreakerState.PANIC and not force:
            logger.warning("Cannot reset from panic state without force=True")
            return False
        
        self._state = CircuitBreakerState.NORMAL
        self._trading_allowed = not self._manual_stop
        self._active_trigger = None
        self._consecutive_losses = 0  # Reset consecutive counter
        
        logger.info("Circuit breaker reset to normal")
        
        self._event_bus.publish(Event(
            event_type=EventType.ALERT_INFO,
            source="circuit_breaker",
            data={"message": "Circuit breaker reset to normal"},
        ))
        
        return True
    
    def get_risk_metrics(self) -> RiskMetrics:
        """Get current risk metrics."""
        return RiskMetrics(
            timestamp=datetime.utcnow(),
            daily_pnl=self._daily_pnl,
            daily_pnl_pct=(self._daily_pnl / self._day_start_equity * 100) if self._day_start_equity else 0,
            daily_trades=self._daily_trades,
            daily_wins=self._daily_wins,
            daily_losses=self._daily_losses,
            consecutive_wins=self._consecutive_wins,
            consecutive_losses=self._consecutive_losses,
            peak_equity=self._peak_equity,
            current_equity=self._current_equity,
            current_drawdown=self._current_drawdown,
            current_drawdown_pct=self._current_drawdown_pct,
            max_drawdown=self._max_drawdown,
            max_drawdown_pct=self._max_drawdown_pct,
            daily_loss_limit_pct=self.config.daily_max_loss_pct,
            drawdown_limit_pct=self.config.max_drawdown_pct,
            is_trading_allowed=self._trading_allowed,
            limit_breach_reason=self._active_trigger.message if self._active_trigger else "",
        )
    
    def get_status(self) -> Dict[str, Any]:
        """Get circuit breaker status."""
        return {
            "state": self._state.value,
            "trading_allowed": self._trading_allowed,
            "manual_stop": self._manual_stop,
            "daily_pnl": self._daily_pnl,
            "daily_trades": self._daily_trades,
            "consecutive_losses": self._consecutive_losses,
            "current_drawdown_pct": self._current_drawdown_pct,
            "max_drawdown_pct": self._max_drawdown_pct,
            "active_trigger": self._active_trigger.message if self._active_trigger else None,
            "triggers_today": len([t for t in self._triggers if t.triggered_at.date() == datetime.utcnow().date()]),
        }
