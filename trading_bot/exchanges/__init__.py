"""
Exchange Module
===============

Exchange abstraction layer for broker-agnostic trading.
"""

from trading_bot.exchanges.base import BaseExchange, ExchangeConfig, ExchangeStatus
from trading_bot.exchanges.adapters import (
    SimulatedExchange,
    BinanceAdapter,
    BybitAdapter,
    CCXTExchange,
)
from trading_bot.exchanges.factory import ExchangeFactory

__all__ = [
    "BaseExchange",
    "ExchangeConfig",
    "ExchangeStatus",
    "SimulatedExchange",
    "BinanceAdapter",
    "BybitAdapter",
    "CCXTExchange",
    "ExchangeFactory",
]
