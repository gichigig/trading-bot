"""
Backtesting Module
===================

Realistic backtesting and paper trading with proper parity.
"""

from trading_bot.backtesting.engine import BacktestEngine, BacktestConfig, BacktestResult
from trading_bot.backtesting.paper import PaperTradingEngine
from trading_bot.backtesting.simulator import MarketSimulator

__all__ = [
    "BacktestEngine",
    "BacktestConfig",
    "BacktestResult",
    "PaperTradingEngine",
    "MarketSimulator",
]
