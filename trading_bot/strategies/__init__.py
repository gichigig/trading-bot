"""
Strategy Module
================

Pluggable strategy system with versioning and configuration.
"""

from trading_bot.strategies.base import BaseStrategy, StrategyState
from trading_bot.strategies.registry import StrategyRegistry, register_strategy, get_strategy

__all__ = [
    "BaseStrategy",
    "StrategyState",
    "StrategyRegistry",
    "register_strategy",
    "get_strategy",
]
