"""
Performance Analyzer
=====================

Deep analysis of trading performance for continuous improvement.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import statistics
import json

from trading_bot.observability.metrics import TradeMetrics, MetricsCollector


@dataclass
class TimeAnalysis:
    """Analysis by time of day/week."""
    hour_performance: Dict[int, Dict[str, float]]  # hour -> metrics
    day_performance: Dict[int, Dict[str, float]]   # day of week -> metrics
    session_performance: Dict[str, Dict[str, float]]  # session name -> metrics


@dataclass
class SymbolAnalysis:
    """Analysis by symbol."""
    symbol_performance: Dict[str, Dict[str, float]]
    best_symbols: List[str]
    worst_symbols: List[str]
    correlation_issues: List[str]


class PerformanceAnalyzer:
    """
    Advanced performance analysis.
    
    Provides insights for strategy improvement:
    - Time-based analysis (when to trade)
    - Symbol analysis (what to trade)
    - Entry/exit timing analysis
    - Risk-adjusted returns
    - Regime performance
    """
    
    def __init__(self, metrics_collector: MetricsCollector):
        self._metrics = metrics_collector
    
    def analyze_by_time(self, trades: List[TradeMetrics]) -> TimeAnalysis:
        """Analyze performance by time of day and day of week."""
        hour_buckets: Dict[int, List[TradeMetrics]] = {i: [] for i in range(24)}
        day_buckets: Dict[int, List[TradeMetrics]] = {i: [] for i in range(7)}
        session_buckets: Dict[str, List[TradeMetrics]] = {
            "asia": [],
            "london": [],
            "new_york": [],
            "overlap": [],
        }
        
        for trade in trades:
            hour = trade.entry_time.hour
            day = trade.entry_time.weekday()
            
            hour_buckets[hour].append(trade)
            day_buckets[day].append(trade)
            
            # Classify by session (simplified)
            if 0 <= hour < 8:
                session_buckets["asia"].append(trade)
            elif 8 <= hour < 12:
                session_buckets["london"].append(trade)
            elif 12 <= hour < 16:
                session_buckets["overlap"].append(trade)
            else:
                session_buckets["new_york"].append(trade)
        
        return TimeAnalysis(
            hour_performance=self._calc_bucket_metrics(hour_buckets),
            day_performance=self._calc_bucket_metrics(day_buckets),
            session_performance=self._calc_bucket_metrics(session_buckets),
        )
    
    def analyze_by_symbol(self, trades: List[TradeMetrics]) -> SymbolAnalysis:
        """Analyze performance by trading symbol."""
        symbol_buckets: Dict[str, List[TradeMetrics]] = {}
        
        for trade in trades:
            if trade.symbol not in symbol_buckets:
                symbol_buckets[trade.symbol] = []
            symbol_buckets[trade.symbol].append(trade)
        
        symbol_metrics = self._calc_bucket_metrics(symbol_buckets)
        
        # Rank symbols by profit factor
        ranked = sorted(
            symbol_metrics.items(),
            key=lambda x: x[1].get("net_profit", 0),
            reverse=True,
        )
        
        return SymbolAnalysis(
            symbol_performance=symbol_metrics,
            best_symbols=[s[0] for s in ranked[:5]],
            worst_symbols=[s[0] for s in ranked[-5:]],
            correlation_issues=[],  # Would need position data
        )
    
    def _calc_bucket_metrics(
        self,
        buckets: Dict[Any, List[TradeMetrics]],
    ) -> Dict[Any, Dict[str, float]]:
        """Calculate metrics for bucketed trades."""
        result = {}
        
        for key, trades in buckets.items():
            if not trades:
                result[key] = {
                    "trade_count": 0,
                    "win_rate": 0,
                    "net_profit": 0,
                    "avg_pnl": 0,
                    "profit_factor": 0,
                }
                continue
            
            winners = [t for t in trades if t.pnl > 0]
            losers = [t for t in trades if t.pnl < 0]
            
            gross_profit = sum(t.pnl for t in winners)
            gross_loss = abs(sum(t.pnl for t in losers))
            
            result[key] = {
                "trade_count": len(trades),
                "win_rate": len(winners) / len(trades) if trades else 0,
                "net_profit": sum(t.pnl for t in trades),
                "avg_pnl": statistics.mean([t.pnl for t in trades]),
                "profit_factor": gross_profit / gross_loss if gross_loss > 0 else float('inf'),
            }
        
        return result
    
    def analyze_entry_timing(self, trades: List[TradeMetrics]) -> Dict[str, Any]:
        """
        Analyze entry timing quality.
        
        Uses Maximum Favorable Excursion (MFE) to assess entry quality.
        """
        if not trades:
            return {}
        
        # MFE analysis
        mfe_values = [t.max_favorable_excursion for t in trades if t.max_favorable_excursion > 0]
        
        # How often did we capture most of the MFE?
        capture_ratios = []
        for trade in trades:
            if trade.max_favorable_excursion > 0:
                capture = trade.pnl / trade.max_favorable_excursion if trade.pnl > 0 else 0
                capture_ratios.append(capture)
        
        return {
            "avg_mfe": statistics.mean(mfe_values) if mfe_values else 0,
            "avg_mfe_capture": statistics.mean(capture_ratios) if capture_ratios else 0,
            "trades_with_mfe": len([t for t in trades if t.max_favorable_excursion > 0]),
            "recommendation": self._entry_recommendation(capture_ratios),
        }
    
    def analyze_exit_timing(self, trades: List[TradeMetrics]) -> Dict[str, Any]:
        """
        Analyze exit timing quality.
        
        Uses Maximum Adverse Excursion (MAE) to assess stop placement.
        """
        if not trades:
            return {}
        
        # MAE analysis for stopped trades
        mae_values = [t.max_adverse_excursion for t in trades if t.max_adverse_excursion < 0]
        
        # How many trades hit max MAE before recovering?
        mae_to_exit = []
        for trade in trades:
            if trade.max_adverse_excursion < 0:
                mae_to_exit.append(trade.max_adverse_excursion)
        
        # Premature exits (exited with profit but could have made more)
        premature = [
            t for t in trades 
            if t.pnl > 0 and t.max_favorable_excursion > t.pnl * 1.5
        ]
        
        return {
            "avg_mae": statistics.mean(mae_values) if mae_values else 0,
            "premature_exit_count": len(premature),
            "premature_exit_pct": len(premature) / len(trades) * 100 if trades else 0,
            "avg_left_on_table": statistics.mean([
                t.max_favorable_excursion - t.pnl for t in premature
            ]) if premature else 0,
            "recommendation": self._exit_recommendation(premature, trades),
        }
    
    def _entry_recommendation(self, capture_ratios: List[float]) -> str:
        """Generate entry timing recommendation."""
        if not capture_ratios:
            return "Insufficient data for entry analysis"
        
        avg_capture = statistics.mean(capture_ratios)
        
        if avg_capture > 0.7:
            return "Entry timing is good - capturing most of available moves"
        elif avg_capture > 0.5:
            return "Entry timing is acceptable - consider waiting for better setups"
        else:
            return "Entry timing needs improvement - many entries are poorly timed"
    
    def _exit_recommendation(
        self,
        premature: List[TradeMetrics],
        all_trades: List[TradeMetrics],
    ) -> str:
        """Generate exit timing recommendation."""
        if not all_trades:
            return "Insufficient data for exit analysis"
        
        premature_pct = len(premature) / len(all_trades)
        
        if premature_pct > 0.3:
            return "Consider trailing stops or wider targets - exiting too early on many trades"
        elif premature_pct > 0.15:
            return "Some premature exits - review target placement methodology"
        else:
            return "Exit timing is appropriate"
    
    def analyze_risk_adjusted(self, trades: List[TradeMetrics]) -> Dict[str, Any]:
        """Calculate risk-adjusted performance metrics."""
        if not trades:
            return {}
        
        returns = [t.pnl_pct for t in trades]
        
        # Calmar ratio (return / max drawdown)
        total_return = sum(returns)
        max_dd = self._calculate_max_dd(trades)
        calmar = total_return / abs(max_dd) if max_dd != 0 else float('inf')
        
        # Recovery factor
        gross_loss = abs(sum(t.pnl for t in trades if t.pnl < 0))
        net_profit = sum(t.pnl for t in trades)
        recovery_factor = net_profit / gross_loss if gross_loss > 0 else float('inf')
        
        # System Quality Number (SQN)
        if len(returns) >= 30:
            avg_return = statistics.mean(returns)
            std_return = statistics.stdev(returns)
            sqn = (avg_return / std_return) * (len(returns) ** 0.5) if std_return > 0 else 0
        else:
            sqn = 0
        
        return {
            "calmar_ratio": round(calmar, 3),
            "recovery_factor": round(recovery_factor, 3),
            "sqn": round(sqn, 2),
            "sqn_interpretation": self._interpret_sqn(sqn),
        }
    
    def _calculate_max_dd(self, trades: List[TradeMetrics]) -> float:
        """Calculate maximum drawdown from trades."""
        if not trades:
            return 0
        
        cumulative_pnl = 0
        peak = 0
        max_dd = 0
        
        for trade in sorted(trades, key=lambda t: t.exit_time or t.entry_time):
            cumulative_pnl += trade.pnl
            if cumulative_pnl > peak:
                peak = cumulative_pnl
            dd = cumulative_pnl - peak
            if dd < max_dd:
                max_dd = dd
        
        return max_dd
    
    def _interpret_sqn(self, sqn: float) -> str:
        """Interpret System Quality Number."""
        if sqn >= 7:
            return "Holy Grail (extremely rare)"
        elif sqn >= 5:
            return "Excellent"
        elif sqn >= 3:
            return "Very good"
        elif sqn >= 2:
            return "Good"
        elif sqn >= 1.5:
            return "Average"
        elif sqn >= 0:
            return "Below average"
        else:
            return "Poor - system needs work"
    
    def generate_report(self, trades: List[TradeMetrics]) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        if not trades:
            return {"error": "No trades to analyze"}
        
        return {
            "period": {
                "start": min(t.entry_time for t in trades).isoformat(),
                "end": max(t.exit_time or t.entry_time for t in trades).isoformat(),
                "total_trades": len(trades),
            },
            "time_analysis": {
                "hour": self.analyze_by_time(trades).hour_performance,
                "day_of_week": self.analyze_by_time(trades).day_performance,
                "session": self.analyze_by_time(trades).session_performance,
            },
            "symbol_analysis": {
                "by_symbol": self.analyze_by_symbol(trades).symbol_performance,
                "best": self.analyze_by_symbol(trades).best_symbols,
                "worst": self.analyze_by_symbol(trades).worst_symbols,
            },
            "timing_analysis": {
                "entry": self.analyze_entry_timing(trades),
                "exit": self.analyze_exit_timing(trades),
            },
            "risk_adjusted": self.analyze_risk_adjusted(trades),
            "recommendations": self._generate_recommendations(trades),
        }
    
    def _generate_recommendations(self, trades: List[TradeMetrics]) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Win rate recommendation
        winners = [t for t in trades if t.pnl > 0]
        win_rate = len(winners) / len(trades) if trades else 0
        
        if win_rate < 0.4:
            recommendations.append(
                "Win rate is below 40%. Focus on better entry criteria or "
                "ensure your risk:reward compensates (need > 1.5 R:R)."
            )
        
        # Check for time-based patterns
        time_analysis = self.analyze_by_time(trades)
        worst_hour = min(
            time_analysis.hour_performance.items(),
            key=lambda x: x[1].get("net_profit", 0)
        )
        if worst_hour[1].get("trade_count", 0) > 5 and worst_hour[1].get("net_profit", 0) < 0:
            recommendations.append(
                f"Hour {worst_hour[0]}:00 UTC shows consistent losses. "
                "Consider avoiding trades during this time."
            )
        
        # Check for symbol patterns
        symbol_analysis = self.analyze_by_symbol(trades)
        for symbol in symbol_analysis.worst_symbols[:2]:
            perf = symbol_analysis.symbol_performance.get(symbol, {})
            if perf.get("trade_count", 0) > 5 and perf.get("net_profit", 0) < 0:
                recommendations.append(
                    f"Symbol {symbol} has consistent losses. "
                    "Consider removing from trading universe or adjusting parameters."
                )
        
        # Exit timing recommendation
        exit_analysis = self.analyze_exit_timing(trades)
        if exit_analysis.get("premature_exit_pct", 0) > 25:
            recommendations.append(
                f"{exit_analysis['premature_exit_pct']:.0f}% of trades exit prematurely. "
                "Consider using trailing stops or partial take profits."
            )
        
        if not recommendations:
            recommendations.append("Performance is solid. Continue monitoring for consistency.")
        
        return recommendations
