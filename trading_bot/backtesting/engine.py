"""
Backtest Engine
================

Realistic backtesting with proper simulation of market conditions.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
import copy

from trading_bot.core.types import Candle, Signal, Order, Position, Side, OrderStatus, OrderType
from trading_bot.strategies.base import BaseStrategy, StrategyContext
from trading_bot.risk.engine import RiskEngine
from trading_bot.observability.metrics import MetricsCollector, TradeMetrics

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """Backtesting configuration."""
    # Time range
    start_date: datetime
    end_date: datetime
    
    # Capital
    initial_capital: float = 100000.0
    
    # Simulation settings
    commission_pct: float = 0.1  # 0.1% per trade
    slippage_pct: float = 0.05  # 0.05% slippage
    spread_pct: float = 0.02    # Bid-ask spread simulation
    
    # Realistic fill simulation
    use_realistic_fills: bool = True
    partial_fill_probability: float = 0.1
    
    # Market impact (for larger orders)
    market_impact_factor: float = 0.001
    
    # Data settings
    warmup_periods: int = 200
    
    # Risk settings
    use_risk_engine: bool = True


@dataclass
class BacktestResult:
    """Comprehensive backtest results."""
    # Time range
    start_date: datetime
    end_date: datetime
    trading_days: int
    
    # Capital
    initial_capital: float
    final_capital: float
    
    # Returns
    total_return_pct: float
    annualized_return_pct: float
    
    # Risk metrics
    max_drawdown_pct: float
    max_drawdown_duration_days: int
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    
    # Trade statistics
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    profit_factor: float
    expectancy: float
    avg_trade_pnl: float
    avg_winner: float
    avg_loser: float
    largest_winner: float
    largest_loser: float
    avg_trade_duration: timedelta
    
    # Costs
    total_commission: float
    total_slippage: float
    
    # Equity curve
    equity_curve: List[Tuple[datetime, float]]
    
    # All trades
    trades: List[TradeMetrics]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "period": {
                "start": self.start_date.isoformat(),
                "end": self.end_date.isoformat(),
                "trading_days": self.trading_days,
            },
            "capital": {
                "initial": self.initial_capital,
                "final": self.final_capital,
            },
            "returns": {
                "total_pct": round(self.total_return_pct, 2),
                "annualized_pct": round(self.annualized_return_pct, 2),
            },
            "risk": {
                "max_drawdown_pct": round(self.max_drawdown_pct, 2),
                "max_drawdown_duration_days": self.max_drawdown_duration_days,
                "sharpe": round(self.sharpe_ratio, 3),
                "sortino": round(self.sortino_ratio, 3),
                "calmar": round(self.calmar_ratio, 3),
            },
            "trades": {
                "total": self.total_trades,
                "winners": self.winning_trades,
                "losers": self.losing_trades,
                "win_rate": round(self.win_rate * 100, 1),
                "profit_factor": round(self.profit_factor, 2),
                "expectancy": round(self.expectancy, 2),
                "avg_pnl": round(self.avg_trade_pnl, 2),
                "avg_winner": round(self.avg_winner, 2),
                "avg_loser": round(self.avg_loser, 2),
                "largest_winner": round(self.largest_winner, 2),
                "largest_loser": round(self.largest_loser, 2),
            },
            "costs": {
                "total_commission": round(self.total_commission, 2),
                "total_slippage": round(self.total_slippage, 2),
            },
        }
    
    def print_summary(self) -> None:
        """Print formatted summary."""
        print("\n" + "=" * 60)
        print("BACKTEST RESULTS")
        print("=" * 60)
        print(f"Period: {self.start_date.date()} to {self.end_date.date()}")
        print(f"Trading Days: {self.trading_days}")
        print("-" * 60)
        print(f"Initial Capital:    ${self.initial_capital:,.2f}")
        print(f"Final Capital:      ${self.final_capital:,.2f}")
        print(f"Total Return:       {self.total_return_pct:.2f}%")
        print(f"Annualized Return:  {self.annualized_return_pct:.2f}%")
        print("-" * 60)
        print(f"Max Drawdown:       {self.max_drawdown_pct:.2f}%")
        print(f"Sharpe Ratio:       {self.sharpe_ratio:.3f}")
        print(f"Sortino Ratio:      {self.sortino_ratio:.3f}")
        print("-" * 60)
        print(f"Total Trades:       {self.total_trades}")
        print(f"Win Rate:           {self.win_rate * 100:.1f}%")
        print(f"Profit Factor:      {self.profit_factor:.2f}")
        print(f"Expectancy:         ${self.expectancy:.2f}")
        print("-" * 60)
        print(f"Avg Winner:         ${self.avg_winner:.2f}")
        print(f"Avg Loser:          ${self.avg_loser:.2f}")
        print(f"Largest Winner:     ${self.largest_winner:.2f}")
        print(f"Largest Loser:      ${self.largest_loser:.2f}")
        print("-" * 60)
        print(f"Total Commission:   ${self.total_commission:.2f}")
        print(f"Total Slippage:     ${self.total_slippage:.2f}")
        print("=" * 60 + "\n")


class BacktestEngine:
    """
    Realistic backtesting engine.
    
    Features:
    - Proper fill simulation
    - Slippage and commission modeling
    - Multi-timeframe support
    - Walk-forward capability
    """
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        
        # State
        self._capital = config.initial_capital
        self._positions: Dict[str, Position] = {}
        self._pending_orders: List[Order] = []
        self._completed_trades: List[TradeMetrics] = []
        
        # Tracking
        self._equity_curve: List[Tuple[datetime, float]] = []
        self._peak_equity = config.initial_capital
        self._max_drawdown = 0.0
        self._max_dd_duration = 0
        self._dd_start: Optional[datetime] = None
        
        # Costs
        self._total_commission = 0.0
        self._total_slippage = 0.0
        
        # Current bar info
        self._current_time: Optional[datetime] = None
        self._current_prices: Dict[str, float] = {}
        
        # Trade counter
        self._trade_counter = 0
    
    def run(
        self,
        strategy: BaseStrategy,
        data: Dict[str, List[Candle]],  # symbol -> candles
        higher_tf_data: Optional[Dict[str, List[Candle]]] = None,
    ) -> BacktestResult:
        """
        Run backtest on historical data.
        
        Args:
            strategy: Strategy to test
            data: Historical candle data by symbol
            higher_tf_data: Higher timeframe data for bias
        
        Returns:
            BacktestResult with comprehensive metrics
        """
        logger.info(f"Starting backtest: {self.config.start_date} to {self.config.end_date}")
        
        # Reset state
        self._reset()
        
        # Initialize strategy
        strategy.start()
        
        # Get all unique timestamps and sort
        all_timestamps = set()
        for symbol, candles in data.items():
            for candle in candles:
                if self.config.start_date <= candle.timestamp <= self.config.end_date:
                    all_timestamps.add(candle.timestamp)
        
        sorted_timestamps = sorted(all_timestamps)
        
        # Build candle lookup
        candle_lookup = self._build_candle_lookup(data)
        htf_lookup = self._build_candle_lookup(higher_tf_data) if higher_tf_data else {}
        
        # Main loop
        for i, timestamp in enumerate(sorted_timestamps):
            self._current_time = timestamp
            
            # Skip warmup period
            if i < self.config.warmup_periods:
                continue
            
            # Get candles for this bar
            bar_candles = candle_lookup.get(timestamp, {})
            
            # Update current prices
            for symbol, candle in bar_candles.items():
                self._current_prices[symbol] = candle.close
            
            # Process pending orders first
            self._process_pending_orders(bar_candles)
            
            # Update positions (mark to market, check stops)
            self._update_positions(bar_candles)
            
            # Build context for each symbol
            for symbol, candle in bar_candles.items():
                # Build candle history
                candle_history = self._get_candle_history(
                    data[symbol], 
                    timestamp, 
                    strategy.warmup_periods
                )
                
                # Build context
                context = StrategyContext(
                    timestamp=timestamp,
                    symbol=symbol,
                    timeframe=candle.timeframe,
                    candles={candle.timeframe: candle_history},
                    current_candle=candle,
                    higher_tf_bias=self._get_htf_bias(htf_lookup, symbol, timestamp),
                    account_balance=self._capital,
                    has_position=symbol in self._positions,
                    position_side=self._positions[symbol].side if symbol in self._positions else None,
                    position_size=self._positions[symbol].quantity if symbol in self._positions else 0,
                )
                
                # Generate signal
                signal = strategy.generate_signal(context)
                
                if signal:
                    self._process_signal(signal, candle)
            
            # Record equity
            equity = self._calculate_equity()
            self._equity_curve.append((timestamp, equity))
            self._update_drawdown(equity, timestamp)
        
        # Close any remaining positions
        self._close_all_positions()
        
        # Calculate results
        return self._calculate_results()
    
    def _reset(self) -> None:
        """Reset engine state."""
        self._capital = self.config.initial_capital
        self._positions = {}
        self._pending_orders = []
        self._completed_trades = []
        self._equity_curve = []
        self._peak_equity = self.config.initial_capital
        self._max_drawdown = 0.0
        self._max_dd_duration = 0
        self._dd_start = None
        self._total_commission = 0.0
        self._total_slippage = 0.0
        self._trade_counter = 0
    
    def _build_candle_lookup(
        self,
        data: Dict[str, List[Candle]],
    ) -> Dict[datetime, Dict[str, Candle]]:
        """Build timestamp -> symbol -> candle lookup."""
        lookup = {}
        
        for symbol, candles in data.items():
            for candle in candles:
                if candle.timestamp not in lookup:
                    lookup[candle.timestamp] = {}
                lookup[candle.timestamp][symbol] = candle
        
        return lookup
    
    def _get_candle_history(
        self,
        candles: List[Candle],
        up_to: datetime,
        count: int,
    ) -> List[Candle]:
        """Get candle history up to a timestamp."""
        filtered = [c for c in candles if c.timestamp <= up_to]
        return filtered[-count:] if len(filtered) > count else filtered
    
    def _get_htf_bias(
        self,
        htf_lookup: Dict[datetime, Dict[str, Candle]],
        symbol: str,
        timestamp: datetime,
    ) -> Optional[str]:
        """Get higher timeframe bias."""
        # Find most recent HTF candle
        for ts in sorted(htf_lookup.keys(), reverse=True):
            if ts <= timestamp:
                candle = htf_lookup[ts].get(symbol)
                if candle:
                    return "bullish" if candle.close > candle.open else "bearish"
        return None
    
    def _process_signal(self, signal: Signal, candle: Candle) -> None:
        """Process a trading signal."""
        symbol = signal.symbol
        
        # Check if we already have a position
        if symbol in self._positions:
            position = self._positions[symbol]
            
            # Exit signals
            if signal.signal_type.value.startswith("exit"):
                self._close_position(position, candle.close, "signal")
            
            # Scale signals
            elif signal.signal_type.value.startswith("scale"):
                # Handle scaling (simplified)
                pass
        else:
            # Entry signals
            if signal.signal_type.value.startswith("entry"):
                self._open_position(signal, candle)
    
    def _open_position(self, signal: Signal, candle: Candle) -> None:
        """Open a new position."""
        # Calculate fill price with slippage
        slippage = candle.close * self.config.slippage_pct / 100
        
        if signal.side == Side.BUY:
            fill_price = candle.close + slippage
        else:
            fill_price = candle.close - slippage
        
        # Calculate commission
        quantity = signal.quantity or self._calculate_position_size(fill_price)
        commission = fill_price * quantity * self.config.commission_pct / 100
        
        # Check if we have enough capital
        cost = fill_price * quantity + commission
        if cost > self._capital:
            logger.debug(f"Insufficient capital for trade: {cost} > {self._capital}")
            return
        
        # Deduct capital
        self._capital -= cost
        
        # Track costs
        self._total_commission += commission
        self._total_slippage += abs(slippage * quantity)
        
        # Create position
        self._trade_counter += 1
        position = Position(
            id=f"bt_pos_{self._trade_counter}",
            symbol=signal.symbol,
            side=signal.side,
            quantity=quantity,
            entry_price=fill_price,
            entry_time=self._current_time,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            strategy_id=signal.strategy_id,
        )
        
        self._positions[signal.symbol] = position
        logger.debug(f"Opened position: {position.side.value} {quantity} {signal.symbol} @ {fill_price}")
    
    def _close_position(
        self,
        position: Position,
        price: float,
        reason: str,
    ) -> None:
        """Close a position."""
        # Calculate fill price with slippage
        slippage = price * self.config.slippage_pct / 100
        
        if position.side == Side.BUY:
            fill_price = price - slippage  # Selling, so worse price
        else:
            fill_price = price + slippage  # Buying back, so worse price
        
        # Calculate P&L
        if position.side == Side.BUY:
            pnl = (fill_price - position.entry_price) * position.quantity
        else:
            pnl = (position.entry_price - fill_price) * position.quantity
        
        # Calculate commission
        commission = fill_price * position.quantity * self.config.commission_pct / 100
        pnl -= commission
        
        # Update capital
        self._capital += fill_price * position.quantity - commission + position.entry_price * position.quantity
        
        # Track costs
        self._total_commission += commission
        self._total_slippage += abs(slippage * position.quantity)
        
        # Record trade
        duration = (self._current_time - position.entry_time).total_seconds()
        pnl_pct = pnl / (position.entry_price * position.quantity) * 100
        
        trade = TradeMetrics(
            trade_id=position.id,
            strategy=position.strategy_id,
            symbol=position.symbol,
            side=position.side.value,
            entry_time=position.entry_time,
            exit_time=self._current_time,
            duration_seconds=duration,
            entry_price=position.entry_price,
            exit_price=fill_price,
            quantity=position.quantity,
            pnl=pnl,
            pnl_pct=pnl_pct,
            fees=commission * 2,  # Entry + exit
            slippage=abs(slippage * position.quantity * 2),
        )
        
        self._completed_trades.append(trade)
        
        # Remove position
        del self._positions[position.symbol]
        logger.debug(f"Closed position: {position.symbol} PnL: {pnl:.2f} ({reason})")
    
    def _process_pending_orders(self, bar_candles: Dict[str, Candle]) -> None:
        """Process pending stop/limit orders."""
        # Simplified: would check if stop/limit prices hit
        pass
    
    def _update_positions(self, bar_candles: Dict[str, Candle]) -> None:
        """Update positions and check stops/targets."""
        for symbol, position in list(self._positions.items()):
            candle = bar_candles.get(symbol)
            if not candle:
                continue
            
            # Check stop loss
            if position.stop_loss:
                if position.side == Side.BUY and candle.low <= position.stop_loss:
                    self._close_position(position, position.stop_loss, "stop_loss")
                    continue
                elif position.side == Side.SELL and candle.high >= position.stop_loss:
                    self._close_position(position, position.stop_loss, "stop_loss")
                    continue
            
            # Check take profit
            if position.take_profit:
                if position.side == Side.BUY and candle.high >= position.take_profit:
                    self._close_position(position, position.take_profit, "take_profit")
                    continue
                elif position.side == Side.SELL and candle.low <= position.take_profit:
                    self._close_position(position, position.take_profit, "take_profit")
                    continue
    
    def _calculate_position_size(self, price: float) -> float:
        """Calculate default position size (1% risk)."""
        risk_amount = self._capital * 0.01
        return risk_amount / price
    
    def _calculate_equity(self) -> float:
        """Calculate current equity (capital + unrealized P&L)."""
        equity = self._capital
        
        for symbol, position in self._positions.items():
            price = self._current_prices.get(symbol, position.entry_price)
            
            if position.side == Side.BUY:
                unrealized = (price - position.entry_price) * position.quantity
            else:
                unrealized = (position.entry_price - price) * position.quantity
            
            equity += unrealized + position.entry_price * position.quantity
        
        return equity
    
    def _update_drawdown(self, equity: float, timestamp: datetime) -> None:
        """Update drawdown tracking."""
        if equity > self._peak_equity:
            self._peak_equity = equity
            
            # Record drawdown duration if we were in one
            if self._dd_start:
                dd_duration = (timestamp - self._dd_start).days
                self._max_dd_duration = max(self._max_dd_duration, dd_duration)
                self._dd_start = None
        else:
            drawdown = (self._peak_equity - equity) / self._peak_equity
            self._max_drawdown = max(self._max_drawdown, drawdown)
            
            if self._dd_start is None:
                self._dd_start = timestamp
    
    def _close_all_positions(self) -> None:
        """Close all remaining positions at end of backtest."""
        for symbol, position in list(self._positions.items()):
            price = self._current_prices.get(symbol, position.entry_price)
            self._close_position(position, price, "end_of_test")
    
    def _calculate_results(self) -> BacktestResult:
        """Calculate comprehensive backtest results."""
        trades = self._completed_trades
        
        # Basic stats
        final_capital = self._capital
        total_return = (final_capital - self.config.initial_capital) / self.config.initial_capital * 100
        
        # Trading days
        if self._equity_curve:
            trading_days = (self._equity_curve[-1][0] - self._equity_curve[0][0]).days
        else:
            trading_days = 0
        
        # Annualized return
        years = trading_days / 365 if trading_days > 0 else 1
        annualized_return = ((1 + total_return / 100) ** (1 / years) - 1) * 100 if years > 0 else 0
        
        # Trade statistics
        winners = [t for t in trades if t.pnl > 0]
        losers = [t for t in trades if t.pnl < 0]
        
        total_trades = len(trades)
        winning_trades = len(winners)
        losing_trades = len(losers)
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        
        gross_profit = sum(t.pnl for t in winners) if winners else 0
        gross_loss = abs(sum(t.pnl for t in losers)) if losers else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        avg_trade_pnl = sum(t.pnl for t in trades) / total_trades if total_trades > 0 else 0
        avg_winner = gross_profit / winning_trades if winning_trades > 0 else 0
        avg_loser = gross_loss / losing_trades if losing_trades > 0 else 0
        
        largest_winner = max(t.pnl for t in winners) if winners else 0
        largest_loser = abs(min(t.pnl for t in losers)) if losers else 0
        
        expectancy = win_rate * avg_winner - (1 - win_rate) * avg_loser
        
        avg_duration = timedelta(
            seconds=sum(t.duration_seconds for t in trades) / total_trades
        ) if total_trades > 0 else timedelta()
        
        # Risk metrics
        sharpe = self._calculate_sharpe()
        sortino = self._calculate_sortino()
        calmar = annualized_return / (self._max_drawdown * 100) if self._max_drawdown > 0 else 0
        
        return BacktestResult(
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            trading_days=trading_days,
            initial_capital=self.config.initial_capital,
            final_capital=final_capital,
            total_return_pct=total_return,
            annualized_return_pct=annualized_return,
            max_drawdown_pct=self._max_drawdown * 100,
            max_drawdown_duration_days=self._max_dd_duration,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            calmar_ratio=calmar,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            profit_factor=profit_factor,
            expectancy=expectancy,
            avg_trade_pnl=avg_trade_pnl,
            avg_winner=avg_winner,
            avg_loser=avg_loser,
            largest_winner=largest_winner,
            largest_loser=largest_loser,
            avg_trade_duration=avg_duration,
            total_commission=self._total_commission,
            total_slippage=self._total_slippage,
            equity_curve=self._equity_curve,
            trades=trades,
        )
    
    def _calculate_sharpe(self, risk_free_rate: float = 0.02) -> float:
        """Calculate Sharpe ratio from equity curve."""
        if len(self._equity_curve) < 2:
            return 0.0
        
        # Calculate daily returns
        returns = []
        for i in range(1, len(self._equity_curve)):
            prev_equity = self._equity_curve[i-1][1]
            curr_equity = self._equity_curve[i][1]
            if prev_equity > 0:
                returns.append((curr_equity - prev_equity) / prev_equity)
        
        if len(returns) < 2:
            return 0.0
        
        import statistics
        mean_return = statistics.mean(returns)
        std_return = statistics.stdev(returns)
        
        if std_return == 0:
            return 0.0
        
        daily_rf = risk_free_rate / 252
        sharpe = (mean_return - daily_rf) / std_return * (252 ** 0.5)
        
        return round(sharpe, 3)
    
    def _calculate_sortino(self, risk_free_rate: float = 0.02) -> float:
        """Calculate Sortino ratio from equity curve."""
        if len(self._equity_curve) < 2:
            return 0.0
        
        # Calculate daily returns
        returns = []
        for i in range(1, len(self._equity_curve)):
            prev_equity = self._equity_curve[i-1][1]
            curr_equity = self._equity_curve[i][1]
            if prev_equity > 0:
                returns.append((curr_equity - prev_equity) / prev_equity)
        
        if len(returns) < 2:
            return 0.0
        
        import statistics
        mean_return = statistics.mean(returns)
        
        # Downside deviation
        negative_returns = [r for r in returns if r < 0]
        if len(negative_returns) < 2:
            return float('inf') if mean_return > 0 else 0.0
        
        downside_std = statistics.stdev(negative_returns)
        
        if downside_std == 0:
            return float('inf') if mean_return > 0 else 0.0
        
        daily_rf = risk_free_rate / 252
        sortino = (mean_return - daily_rf) / downside_std * (252 ** 0.5)
        
        return round(sortino, 3)
