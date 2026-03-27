"""
Exchange Factory
================

Factory for creating exchange adapters.
Never marry an exchange - use the factory to swap easily.
"""

from typing import Dict, Type, Optional
import logging

from trading_bot.exchanges.base import BaseExchange, ExchangeConfig
from trading_bot.exchanges.adapters import (
    SimulatedExchange,
    BinanceAdapter,
    BybitAdapter,
)

logger = logging.getLogger(__name__)


class ExchangeFactory:
    """
    Factory for creating exchange adapters.
    
    Supports registration of custom adapters.
    
    Usage:
        factory = ExchangeFactory()
        exchange = factory.create("binance", config)
    """
    
    # Registry of exchange adapters
    _registry: Dict[str, Type[BaseExchange]] = {
        "simulated": SimulatedExchange,
        "paper": SimulatedExchange,
        "backtest": SimulatedExchange,
        "binance": BinanceAdapter,
        "binance_futures": BinanceAdapter,
        "bybit": BybitAdapter,
    }
    
    @classmethod
    def register(cls, exchange_id: str, adapter_class: Type[BaseExchange]) -> None:
        """
        Register a custom exchange adapter.
        
        Args:
            exchange_id: Unique identifier for the exchange
            adapter_class: Exchange adapter class
        """
        if not issubclass(adapter_class, BaseExchange):
            raise ValueError(f"Adapter must inherit from BaseExchange")
        
        cls._registry[exchange_id.lower()] = adapter_class
        logger.info(f"Registered exchange adapter: {exchange_id}")
    
    @classmethod
    def create(
        cls,
        exchange_id: str,
        config: Optional[ExchangeConfig] = None,
        **kwargs,
    ) -> BaseExchange:
        """
        Create an exchange adapter instance.
        
        Args:
            exchange_id: Exchange identifier (e.g., "binance", "bybit")
            config: Exchange configuration
            **kwargs: Additional arguments passed to adapter
            
        Returns:
            Exchange adapter instance
            
        Raises:
            ValueError: If exchange is not supported
        """
        exchange_id_lower = exchange_id.lower()
        
        if exchange_id_lower not in cls._registry:
            supported = ", ".join(cls._registry.keys())
            raise ValueError(
                f"Unsupported exchange: {exchange_id}. "
                f"Supported exchanges: {supported}"
            )
        
        adapter_class = cls._registry[exchange_id_lower]
        
        # Create default config if not provided
        if config is None:
            config = ExchangeConfig(exchange_id=exchange_id_lower)
        
        logger.info(f"Creating exchange adapter: {exchange_id}")
        
        return adapter_class(config, **kwargs)
    
    @classmethod
    def list_exchanges(cls) -> list:
        """Get list of supported exchanges."""
        return list(cls._registry.keys())
    
    @classmethod
    def is_supported(cls, exchange_id: str) -> bool:
        """Check if exchange is supported."""
        return exchange_id.lower() in cls._registry
    
    @classmethod
    def create_with_failover(
        cls,
        primary_exchange: str,
        failover_exchanges: list,
        config: ExchangeConfig,
    ) -> "ExchangeWithFailover":
        """
        Create exchange with automatic failover.
        
        Args:
            primary_exchange: Primary exchange ID
            failover_exchanges: List of failover exchange IDs
            config: Exchange configuration (same credentials assumed)
            
        Returns:
            ExchangeWithFailover wrapper
        """
        primary = cls.create(primary_exchange, config)
        failovers = [cls.create(ex, config) for ex in failover_exchanges]
        
        return ExchangeWithFailover(primary, failovers)


class ExchangeWithFailover:
    """
    Exchange wrapper with automatic failover support.
    
    If primary exchange fails, automatically switches to failover.
    """
    
    def __init__(
        self,
        primary: BaseExchange,
        failovers: list,
    ):
        self.primary = primary
        self.failovers = failovers
        self._current = primary
        self._failover_index = -1
    
    @property
    def current_exchange(self) -> BaseExchange:
        """Get currently active exchange."""
        return self._current
    
    async def connect(self) -> bool:
        """Connect with failover support."""
        if await self.primary.connect():
            self._current = self.primary
            self._failover_index = -1
            return True
        
        # Try failovers
        for i, exchange in enumerate(self.failovers):
            logger.warning(f"Primary exchange failed, trying failover {i+1}")
            if await exchange.connect():
                self._current = exchange
                self._failover_index = i
                return True
        
        return False
    
    async def disconnect(self) -> None:
        """Disconnect all exchanges."""
        await self.primary.disconnect()
        for exchange in self.failovers:
            await exchange.disconnect()
    
    async def switch_to_failover(self) -> bool:
        """
        Manually switch to next failover exchange.
        
        Returns:
            True if switched successfully
        """
        self._failover_index += 1
        
        if self._failover_index >= len(self.failovers):
            logger.error("No more failover exchanges available")
            return False
        
        failover = self.failovers[self._failover_index]
        
        if await failover.connect():
            await self._current.disconnect()
            self._current = failover
            logger.info(f"Switched to failover exchange: {failover.exchange_id}")
            return True
        
        return await self.switch_to_failover()  # Try next
    
    async def reset_to_primary(self) -> bool:
        """
        Reset to primary exchange.
        
        Returns:
            True if primary reconnected successfully
        """
        if await self.primary.connect():
            await self._current.disconnect()
            self._current = self.primary
            self._failover_index = -1
            logger.info("Reset to primary exchange")
            return True
        
        return False
    
    def __getattr__(self, name):
        """Proxy all other calls to current exchange."""
        return getattr(self._current, name)
