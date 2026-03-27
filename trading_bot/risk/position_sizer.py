"""
Position Sizer
===============

Dynamic position sizing with volatility adjustment and risk limits.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import logging
import math

from trading_bot.core.types import Signal, Side
from trading_bot.core.config import RiskConfig

logger = logging.getLogger(__name__)


@dataclass
class PositionSizeResult:
    """Result of position size calculation."""
    quantity: float
    notional_value: float
    risk_amount: float
    risk_pct: float
    
    # Adjustments applied
    base_size: float
    volatility_multiplier: float
    correlation_adjustment: float
    max_size_cap_applied: bool
    
    # Rejection info
    approved: bool
    rejection_reason: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "quantity": self.quantity,
            "notional_value": self.notional_value,
            "risk_amount": self.risk_amount,
            "risk_pct": self.risk_pct,
            "base_size": self.base_size,
            "volatility_multiplier": self.volatility_multiplier,
            "correlation_adjustment": self.correlation_adjustment,
            "max_size_cap_applied": self.max_size_cap_applied,
            "approved": self.approved,
            "rejection_reason": self.rejection_reason,
        }


class PositionSizer:
    """
    Calculates position sizes based on risk parameters.
    
    Methods:
    - Fixed percentage risk per trade
    - Volatility-adjusted sizing (ATR-based)
    - Kelly criterion (optional)
    - Correlation-aware limits
    """
    
    def __init__(self, risk_config: RiskConfig):
        self.config = risk_config
        
        # Current account state
        self._account_balance: float = 0.0
        self._current_exposure: float = 0.0
        
        # Volatility data per symbol
        self._volatility_data: Dict[str, Dict[str, float]] = {}
        
        # Correlation matrix (symbol pairs)
        self._correlations: Dict[Tuple[str, str], float] = {}
        
        # Current positions for correlation check
        self._open_positions: Dict[str, Dict[str, Any]] = {}
    
    def calculate_position_size(
        self,
        signal: Signal,
        account_balance: float,
        entry_price: float,
        stop_loss: float,
        current_atr: Optional[float] = None,
    ) -> PositionSizeResult:
        """
        Calculate position size based on risk parameters.
        
        Args:
            signal: Trading signal
            account_balance: Current account balance
            entry_price: Planned entry price
            stop_loss: Stop loss price
            current_atr: Current ATR for volatility adjustment
            
        Returns:
            PositionSizeResult with calculated size and adjustments
        """
        self._account_balance = account_balance
        
        # Calculate risk per share/unit
        risk_per_unit = abs(entry_price - stop_loss)
        
        if risk_per_unit == 0:
            return PositionSizeResult(
                quantity=0,
                notional_value=0,
                risk_amount=0,
                risk_pct=0,
                base_size=0,
                volatility_multiplier=1.0,
                correlation_adjustment=1.0,
                max_size_cap_applied=False,
                approved=False,
                rejection_reason="Invalid stop loss (zero risk per unit)",
            )
        
        # Calculate base position size (fixed % risk)
        risk_amount = account_balance * (self.config.risk_per_trade_pct / 100)
        base_quantity = risk_amount / risk_per_unit
        
        # Apply volatility adjustment
        volatility_mult = self._calculate_volatility_multiplier(
            signal.symbol, current_atr
        )
        adjusted_quantity = base_quantity * volatility_mult
        
        # Apply correlation adjustment
        correlation_adj = self._calculate_correlation_adjustment(signal.symbol)
        adjusted_quantity *= correlation_adj
        
        # Apply maximum size cap
        max_size_cap_applied = False
        notional_value = adjusted_quantity * entry_price
        
        if notional_value > self.config.max_position_size:
            adjusted_quantity = self.config.max_position_size / entry_price
            notional_value = self.config.max_position_size
            max_size_cap_applied = True
        
        # Check exposure limits
        new_exposure = self._current_exposure + notional_value
        exposure_pct = (new_exposure / account_balance) * 100 if account_balance else 0
        
        # Final risk calculations
        final_risk_amount = adjusted_quantity * risk_per_unit
        final_risk_pct = (final_risk_amount / account_balance) * 100 if account_balance else 0
        
        # Validate against max risk per trade
        if final_risk_pct > self.config.max_risk_per_trade_pct:
            return PositionSizeResult(
                quantity=0,
                notional_value=0,
                risk_amount=0,
                risk_pct=0,
                base_size=base_quantity,
                volatility_multiplier=volatility_mult,
                correlation_adjustment=correlation_adj,
                max_size_cap_applied=max_size_cap_applied,
                approved=False,
                rejection_reason=f"Risk ({final_risk_pct:.2f}%) exceeds max ({self.config.max_risk_per_trade_pct}%)",
            )
        
        # Round to appropriate precision
        adjusted_quantity = self._round_quantity(adjusted_quantity, signal.symbol)
        
        return PositionSizeResult(
            quantity=adjusted_quantity,
            notional_value=adjusted_quantity * entry_price,
            risk_amount=final_risk_amount,
            risk_pct=final_risk_pct,
            base_size=base_quantity,
            volatility_multiplier=volatility_mult,
            correlation_adjustment=correlation_adj,
            max_size_cap_applied=max_size_cap_applied,
            approved=True,
        )
    
    def _calculate_volatility_multiplier(
        self,
        symbol: str,
        current_atr: Optional[float],
    ) -> float:
        """
        Calculate volatility adjustment multiplier.
        
        Higher volatility = smaller position
        Lower volatility = larger position (up to limits)
        """
        if not self.config.volatility_scaling or not current_atr:
            return 1.0
        
        # Get historical volatility data
        vol_data = self._volatility_data.get(symbol, {})
        avg_atr = vol_data.get("avg_atr", current_atr)
        
        if avg_atr == 0:
            return 1.0
        
        # Calculate ratio (inverse - higher vol = lower multiplier)
        ratio = avg_atr / current_atr
        
        # Clamp to configured limits
        multiplier = max(
            self.config.min_volatility_multiplier,
            min(ratio, self.config.max_volatility_multiplier)
        )
        
        return multiplier
    
    def _calculate_correlation_adjustment(self, symbol: str) -> float:
        """
        Calculate adjustment based on correlation with existing positions.
        
        Reduces size if highly correlated positions exist.
        """
        if not self._open_positions:
            return 1.0
        
        correlated_count = 0
        total_correlated_exposure = 0.0
        
        for pos_symbol, pos_data in self._open_positions.items():
            if pos_symbol == symbol:
                continue
            
            # Get correlation
            corr_key = tuple(sorted([symbol, pos_symbol]))
            correlation = self._correlations.get(corr_key, 0.0)
            
            if abs(correlation) >= self.config.correlation_threshold:
                correlated_count += 1
                total_correlated_exposure += pos_data.get("notional", 0)
        
        # Check max correlated positions
        if correlated_count >= self.config.max_correlated_positions:
            return 0.0  # No new position
        
        # Reduce size based on existing correlated exposure
        if total_correlated_exposure > 0:
            max_exposure = self.config.max_position_size * self.config.max_correlated_positions
            remaining_capacity = max(0, max_exposure - total_correlated_exposure)
            adjustment = remaining_capacity / max_exposure
            return max(0.3, adjustment)  # Minimum 30% size
        
        return 1.0
    
    def _round_quantity(self, quantity: float, symbol: str) -> float:
        """Round quantity to appropriate precision for symbol."""
        # Default rounding - could be customized per symbol
        if quantity >= 1:
            return round(quantity, 2)
        elif quantity >= 0.1:
            return round(quantity, 3)
        else:
            return round(quantity, 4)
    
    def update_volatility_data(
        self,
        symbol: str,
        current_atr: float,
        avg_atr: float,
    ) -> None:
        """Update volatility data for a symbol."""
        self._volatility_data[symbol] = {
            "current_atr": current_atr,
            "avg_atr": avg_atr,
            "updated_at": datetime.utcnow(),
        }
    
    def update_correlation(
        self,
        symbol1: str,
        symbol2: str,
        correlation: float,
    ) -> None:
        """Update correlation between two symbols."""
        key = tuple(sorted([symbol1, symbol2]))
        self._correlations[key] = correlation
    
    def register_position(
        self,
        symbol: str,
        side: Side,
        quantity: float,
        entry_price: float,
    ) -> None:
        """Register an open position for correlation tracking."""
        self._open_positions[symbol] = {
            "side": side,
            "quantity": quantity,
            "entry_price": entry_price,
            "notional": quantity * entry_price,
        }
        self._current_exposure += quantity * entry_price
    
    def unregister_position(self, symbol: str) -> None:
        """Remove a closed position."""
        if symbol in self._open_positions:
            pos = self._open_positions.pop(symbol)
            self._current_exposure -= pos.get("notional", 0)
    
    def calculate_kelly_size(
        self,
        win_rate: float,
        avg_win: float,
        avg_loss: float,
        account_balance: float,
        kelly_fraction: float = 0.25,  # Use 25% Kelly for safety
    ) -> float:
        """
        Calculate position size using Kelly Criterion.
        
        Kelly % = W - [(1-W) / R]
        Where W = win rate, R = win/loss ratio
        """
        if avg_loss == 0 or win_rate <= 0:
            return 0.0
        
        win_loss_ratio = avg_win / avg_loss
        kelly_pct = win_rate - ((1 - win_rate) / win_loss_ratio)
        
        # Apply fraction for safety
        kelly_pct *= kelly_fraction
        
        # Clamp to reasonable range
        kelly_pct = max(0, min(kelly_pct, 0.25))  # Max 25%
        
        return account_balance * kelly_pct
    
    def get_max_position_for_symbol(
        self,
        symbol: str,
        entry_price: float,
    ) -> float:
        """Get maximum position size allowed for a symbol."""
        # Based on max position size config
        max_by_value = self.config.max_position_size / entry_price
        
        # Based on correlation limits
        correlation_adj = self._calculate_correlation_adjustment(symbol)
        max_by_correlation = max_by_value * correlation_adj
        
        return min(max_by_value, max_by_correlation)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get position sizer statistics."""
        return {
            "account_balance": self._account_balance,
            "current_exposure": self._current_exposure,
            "exposure_pct": (self._current_exposure / self._account_balance * 100) if self._account_balance else 0,
            "open_positions": len(self._open_positions),
            "symbols_tracked": list(self._volatility_data.keys()),
            "correlations_tracked": len(self._correlations),
        }
