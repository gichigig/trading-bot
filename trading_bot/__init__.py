"""
Advanced Trading Bot Framework
==============================

A professional-grade, modular trading bot with:
- Pluggable strategy architecture
- Multi-timeframe analysis
- Regime detection
- Dynamic risk management
- Smart order execution
- Exchange abstraction
- State persistence
- Comprehensive observability

Author: Trading Bot Framework
Version: 1.0.0
"""

__version__ = "1.0.0"
__author__ = "Trading Bot Framework"

# Core
from trading_bot.bot import TradingBot
from trading_bot.core.config import BotConfig, load_config, TradingMode

# Strategies
from trading_bot.strategies.base import BaseStrategy
from trading_bot.strategies.registry import register_strategy, get_strategy

# Types
from trading_bot.core.types import (
    Signal, SignalType, Order, OrderType, OrderStatus,
    Position, Side, Candle, Regime
)

# Risk
from trading_bot.risk.engine import RiskEngine

# Backtesting
from trading_bot.backtesting.engine import BacktestEngine, BacktestConfig

__all__ = [
    # Core
    "TradingBot",
    "BotConfig",
    "load_config",
    "TradingMode",
    
    # Strategies
    "BaseStrategy",
    "register_strategy",
    "get_strategy",
    
    # Types
    "Signal",
    "SignalType", 
    "Order",
    "OrderType",
    "OrderStatus",
    "Position",
    "Side",
    "Candle",
    "Regime",
    
    # Risk
    "RiskEngine",
    
    # Backtesting
    "BacktestEngine",
    "BacktestConfig",
    
    "__version__",
]
