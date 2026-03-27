"""
Example Strategies
===================

Ready-to-use strategy implementations demonstrating the plugin architecture.
"""

from trading_bot.strategies.examples.momentum import MomentumStrategy
from trading_bot.strategies.examples.mean_reversion import MeanReversionStrategy
from trading_bot.strategies.examples.breakout import BreakoutStrategy

__all__ = [
    "MomentumStrategy",
    "MeanReversionStrategy", 
    "BreakoutStrategy",
]
