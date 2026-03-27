"""
Metrics Collector
==================

Track what matters: win rate, expectancy, drawdown, Sharpe, and more.
Profit alone is a liar.
"""

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from collections import deque
import math
import statistics


@dataclass
class TradeMetrics:
    """Metrics for a single trade."""
    trade_id: str
    strategy: str
    symbol: str
    side: str
    entry_time: datetime
    exit_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    entry_price: float = 0.0
    exit_price: float = 0.0
    quantity: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    fees: float = 0.0
    slippage: float = 0.0
    expected_slippage: float = 0.0
    max_favorable_excursion: float = 0.0  # Best unrealized P&L
    max_adverse_excursion: float = 0.0    # Worst unrealized P&L
    risk_reward_actual: float = 0.0
    risk_reward_planned: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["entry_time"] = self.entry_time.isoformat()
        result["exit_time"] = self.exit_time.isoformat() if self.exit_time else None
        return result


@dataclass
class StrategyMetrics:
    """Aggregated metrics for a strategy."""
    strategy_name: str
    strategy_version: str
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    breakeven_trades: int = 0
    
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    net_profit: float = 0.0
    total_fees: float = 0.0
    
    win_rate: float = 0.0
    loss_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    largest_win: float = 0.0
    largest_loss: float = 0.0
    
    expectancy: float = 0.0
    profit_factor: float = 0.0
    payoff_ratio: float = 0.0
    
    avg_trade_duration_seconds: float = 0.0
    avg_winning_duration_seconds: float = 0.0
    avg_losing_duration_seconds: float = 0.0
    
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    current_streak: int = 0
    
    total_slippage: float = 0.0
    avg_slippage: float = 0.0
    slippage_vs_expected: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MetricsCollector:
    """
    Collects and analyzes trading metrics.
    
    Tracks:
    - Win rate (with proper context)
    - Expectancy (expected value per trade)
    - Maximum drawdown
    - Sharpe/Sortino ratios
    - Trade duration statistics
    - Slippage analysis
    """
    
    def __init__(
        self,
        risk_free_rate: float = 0.02,  # Annual risk-free rate
        rolling_window: int = 100,      # Trades for rolling metrics
    ):
        self.risk_free_rate = risk_free_rate
        self.rolling_window = rolling_window
        
        # Trade history
        self._trades: List[TradeMetrics] = []
        self._trades_by_strategy: Dict[str, List[TradeMetrics]] = {}
        
        # Rolling metrics
        self._rolling_returns: deque = deque(maxlen=rolling_window)
        
        # Equity tracking
        self._equity_curve: List[Tuple[datetime, float]] = []
        self._peak_equity: float = 0.0
        self._current_equity: float = 0.0
        self._max_drawdown: float = 0.0
        self._max_drawdown_duration: timedelta = timedelta()
        self._drawdown_start: Optional[datetime] = None
        
        # Daily returns for Sharpe
        self._daily_returns: List[float] = []
        self._last_daily_equity: float = 0.0
    
    def record_trade(self, trade: TradeMetrics) -> None:
        """Record a completed trade."""
        self._trades.append(trade)
        
        # Group by strategy
        if trade.strategy not in self._trades_by_strategy:
            self._trades_by_strategy[trade.strategy] = []
        self._trades_by_strategy[trade.strategy].append(trade)
        
        # Update rolling returns
        self._rolling_returns.append(trade.pnl_pct)
    
    def update_equity(self, equity: float, timestamp: Optional[datetime] = None) -> None:
        """Update current equity and track drawdown."""
        timestamp = timestamp or datetime.utcnow()
        self._current_equity = equity
        self._equity_curve.append((timestamp, equity))
        
        # Update peak and drawdown
        if equity > self._peak_equity:
            self._peak_equity = equity
            self._drawdown_start = None
        else:
            drawdown = (self._peak_equity - equity) / self._peak_equity if self._peak_equity > 0 else 0
            if drawdown > self._max_drawdown:
                self._max_drawdown = drawdown
            
            if self._drawdown_start is None:
                self._drawdown_start = timestamp
    
    def record_daily_return(self, equity: float) -> None:
        """Record daily equity for Sharpe calculation."""
        if self._last_daily_equity > 0:
            daily_return = (equity - self._last_daily_equity) / self._last_daily_equity
            self._daily_returns.append(daily_return)
        self._last_daily_equity = equity
    
    def get_strategy_metrics(self, strategy_name: str) -> Optional[StrategyMetrics]:
        """Calculate metrics for a specific strategy."""
        trades = self._trades_by_strategy.get(strategy_name)
        if not trades:
            return None
        
        return self._calculate_metrics(trades, strategy_name, "")
    
    def get_overall_metrics(self) -> StrategyMetrics:
        """Calculate overall portfolio metrics."""
        return self._calculate_metrics(self._trades, "overall", "")
    
    def _calculate_metrics(
        self,
        trades: List[TradeMetrics],
        strategy_name: str,
        version: str,
    ) -> StrategyMetrics:
        """Calculate comprehensive metrics from trade list."""
        if not trades:
            return StrategyMetrics(strategy_name=strategy_name, strategy_version=version)
        
        metrics = StrategyMetrics(
            strategy_name=strategy_name,
            strategy_version=version,
            total_trades=len(trades),
        )
        
        # Separate winners and losers
        winners = [t for t in trades if t.pnl > 0]
        losers = [t for t in trades if t.pnl < 0]
        breakeven = [t for t in trades if t.pnl == 0]
        
        metrics.winning_trades = len(winners)
        metrics.losing_trades = len(losers)
        metrics.breakeven_trades = len(breakeven)
        
        # P&L metrics
        metrics.gross_profit = sum(t.pnl for t in winners)
        metrics.gross_loss = abs(sum(t.pnl for t in losers))
        metrics.net_profit = sum(t.pnl for t in trades)
        metrics.total_fees = sum(t.fees for t in trades)
        
        # Win/loss statistics
        metrics.win_rate = len(winners) / len(trades) if trades else 0
        metrics.loss_rate = len(losers) / len(trades) if trades else 0
        
        metrics.avg_win = statistics.mean([t.pnl for t in winners]) if winners else 0
        metrics.avg_loss = abs(statistics.mean([t.pnl for t in losers])) if losers else 0
        
        metrics.largest_win = max([t.pnl for t in winners]) if winners else 0
        metrics.largest_loss = abs(min([t.pnl for t in losers])) if losers else 0
        
        # Expectancy: E = (Win% × AvgWin) - (Loss% × AvgLoss)
        metrics.expectancy = (
            metrics.win_rate * metrics.avg_win - 
            metrics.loss_rate * metrics.avg_loss
        )
        
        # Profit factor: Gross Profit / Gross Loss
        metrics.profit_factor = (
            metrics.gross_profit / metrics.gross_loss 
            if metrics.gross_loss > 0 else float('inf')
        )
        
        # Payoff ratio: Avg Win / Avg Loss
        metrics.payoff_ratio = (
            metrics.avg_win / metrics.avg_loss 
            if metrics.avg_loss > 0 else float('inf')
        )
        
        # Duration statistics
        durations = [t.duration_seconds for t in trades if t.duration_seconds > 0]
        if durations:
            metrics.avg_trade_duration_seconds = statistics.mean(durations)
        
        winning_durations = [t.duration_seconds for t in winners if t.duration_seconds > 0]
        if winning_durations:
            metrics.avg_winning_duration_seconds = statistics.mean(winning_durations)
        
        losing_durations = [t.duration_seconds for t in losers if t.duration_seconds > 0]
        if losing_durations:
            metrics.avg_losing_duration_seconds = statistics.mean(losing_durations)
        
        # Streak analysis
        current_streak = 0
        max_win_streak = 0
        max_loss_streak = 0
        
        for trade in trades:
            if trade.pnl > 0:
                if current_streak >= 0:
                    current_streak += 1
                else:
                    current_streak = 1
                max_win_streak = max(max_win_streak, current_streak)
            elif trade.pnl < 0:
                if current_streak <= 0:
                    current_streak -= 1
                else:
                    current_streak = -1
                max_loss_streak = max(max_loss_streak, abs(current_streak))
        
        metrics.max_consecutive_wins = max_win_streak
        metrics.max_consecutive_losses = max_loss_streak
        metrics.current_streak = current_streak
        
        # Slippage analysis
        metrics.total_slippage = sum(t.slippage for t in trades)
        metrics.avg_slippage = statistics.mean([t.slippage for t in trades]) if trades else 0
        
        expected_slippages = [t.expected_slippage for t in trades if t.expected_slippage > 0]
        actual_slippages = [t.slippage for t in trades if t.expected_slippage > 0]
        if expected_slippages and actual_slippages:
            metrics.slippage_vs_expected = (
                sum(actual_slippages) / sum(expected_slippages) - 1
            ) * 100
        
        return metrics
    
    def calculate_sharpe_ratio(self, period: str = "daily") -> float:
        """
        Calculate Sharpe ratio.
        
        Sharpe = (Mean Return - Risk-Free Rate) / Std Dev of Returns
        """
        returns = self._daily_returns if period == "daily" else list(self._rolling_returns)
        
        if len(returns) < 2:
            return 0.0
        
        mean_return = statistics.mean(returns)
        std_return = statistics.stdev(returns)
        
        if std_return == 0:
            return 0.0
        
        # Annualize based on period
        periods_per_year = 252 if period == "daily" else 252 * 6  # Assume 6 trades/day
        rf_per_period = self.risk_free_rate / periods_per_year
        
        sharpe = (mean_return - rf_per_period) / std_return
        annualized_sharpe = sharpe * math.sqrt(periods_per_year)
        
        return round(annualized_sharpe, 3)
    
    def calculate_sortino_ratio(self, period: str = "daily") -> float:
        """
        Calculate Sortino ratio.
        
        Like Sharpe but only considers downside deviation.
        """
        returns = self._daily_returns if period == "daily" else list(self._rolling_returns)
        
        if len(returns) < 2:
            return 0.0
        
        mean_return = statistics.mean(returns)
        
        # Downside deviation (only negative returns)
        negative_returns = [r for r in returns if r < 0]
        if len(negative_returns) < 2:
            return float('inf') if mean_return > 0 else 0.0
        
        downside_dev = statistics.stdev(negative_returns)
        
        if downside_dev == 0:
            return float('inf') if mean_return > 0 else 0.0
        
        periods_per_year = 252 if period == "daily" else 252 * 6
        rf_per_period = self.risk_free_rate / periods_per_year
        
        sortino = (mean_return - rf_per_period) / downside_dev
        annualized_sortino = sortino * math.sqrt(periods_per_year)
        
        return round(annualized_sortino, 3)
    
    def get_drawdown_info(self) -> Dict[str, Any]:
        """Get current and maximum drawdown information."""
        current_dd = 0.0
        if self._peak_equity > 0 and self._current_equity > 0:
            current_dd = (self._peak_equity - self._current_equity) / self._peak_equity
        
        return {
            "current_drawdown_pct": round(current_dd * 100, 2),
            "max_drawdown_pct": round(self._max_drawdown * 100, 2),
            "peak_equity": self._peak_equity,
            "current_equity": self._current_equity,
            "in_drawdown": current_dd > 0,
        }
    
    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary."""
        overall = self.get_overall_metrics()
        
        return {
            "overall": overall.to_dict(),
            "sharpe_ratio": self.calculate_sharpe_ratio(),
            "sortino_ratio": self.calculate_sortino_ratio(),
            "drawdown": self.get_drawdown_info(),
            "strategies": {
                name: self.get_strategy_metrics(name).to_dict()
                for name in self._trades_by_strategy.keys()
            },
            "total_trades": len(self._trades),
            "trading_days": len(self._daily_returns),
        }
    
    def export_trades(self) -> List[Dict[str, Any]]:
        """Export all trades for analysis."""
        return [t.to_dict() for t in self._trades]
    
    def export_equity_curve(self) -> List[Dict[str, Any]]:
        """Export equity curve data."""
        return [
            {"timestamp": ts.isoformat(), "equity": equity}
            for ts, equity in self._equity_curve
        ]
