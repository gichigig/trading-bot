"""
Base Exchange
=============

Abstract base class for exchange adapters.
Never marry an exchange - use adapters.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from enum import Enum
import asyncio
import logging

from trading_bot.core.types import (
    Order, OrderType, OrderStatus, Side, Candle, Tick
)

logger = logging.getLogger(__name__)


class ExchangeStatus(Enum):
    """Exchange connection status."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"
    MAINTENANCE = "maintenance"


@dataclass
class ExchangeConfig:
    """Exchange configuration."""
    exchange_id: str
    api_key: str = ""
    api_secret: str = ""
    passphrase: str = ""  # For some exchanges like Coinbase
    testnet: bool = False
    sandbox: bool = False
    rate_limit: int = 10  # Requests per second
    timeout_seconds: int = 30
    enable_websocket: bool = True
    retry_on_rate_limit: bool = True
    max_retries: int = 3
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderBook:
    """Order book snapshot."""
    symbol: str
    bids: List[tuple]  # (price, quantity)
    asks: List[tuple]  # (price, quantity)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0][0] if self.bids else None
    
    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0][0] if self.asks else None
    
    @property
    def spread(self) -> Optional[float]:
        if self.best_bid and self.best_ask:
            return self.best_ask - self.best_bid
        return None
    
    @property
    def spread_pct(self) -> Optional[float]:
        if self.spread and self.best_bid:
            return (self.spread / self.best_bid) * 100
        return None


@dataclass
class AccountBalance:
    """Account balance information."""
    currency: str
    total: float
    available: float
    locked: float = 0.0
    in_orders: float = 0.0
    unrealized_pnl: float = 0.0


@dataclass
class MarketInfo:
    """Market/symbol information."""
    symbol: str
    base_currency: str
    quote_currency: str
    price_precision: int
    quantity_precision: int
    min_quantity: float
    max_quantity: float
    min_notional: float
    tick_size: float
    lot_size: float
    is_active: bool = True
    is_spot: bool = True
    is_futures: bool = False
    leverage_max: int = 1


class BaseExchange(ABC):
    """
    Abstract base exchange adapter.
    
    All exchange integrations must implement this interface.
    This ensures we can swap exchanges without code changes.
    """
    
    def __init__(self, config: ExchangeConfig):
        self.config = config
        self.exchange_id = config.exchange_id
        self.status = ExchangeStatus.DISCONNECTED
        
        # Rate limiting
        self._rate_limiter = asyncio.Semaphore(config.rate_limit)
        self._last_request_time = 0.0
        
        # Callbacks
        self._on_order_update: Optional[Callable[[Order], None]] = None
        self._on_position_update: Optional[Callable[[Dict], None]] = None
        self._on_balance_update: Optional[Callable[[AccountBalance], None]] = None
        self._on_tick: Optional[Callable[[Tick], None]] = None
        self._on_candle: Optional[Callable[[Candle], None]] = None
        
        # Market info cache
        self._market_info_cache: Dict[str, MarketInfo] = {}
    
    # =========== Connection Management ===========
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        Establish connection to exchange.
        
        Returns:
            True if connected successfully
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from exchange."""
        pass
    
    @abstractmethod
    async def ping(self) -> bool:
        """
        Check if connection is alive.
        
        Returns:
            True if connection is healthy
        """
        pass
    
    async def reconnect(self) -> bool:
        """Reconnect to exchange."""
        self.status = ExchangeStatus.RECONNECTING
        await self.disconnect()
        return await self.connect()
    
    # =========== Account Operations ===========
    
    @abstractmethod
    async def get_balance(self, currency: str = None) -> List[AccountBalance]:
        """
        Get account balances.
        
        Args:
            currency: Specific currency, or None for all
            
        Returns:
            List of account balances
        """
        pass
    
    @abstractmethod
    async def get_positions(self, symbol: str = None) -> List[Dict[str, Any]]:
        """
        Get open positions (for futures/margin).
        
        Args:
            symbol: Specific symbol, or None for all
            
        Returns:
            List of position data
        """
        pass
    
    # =========== Market Data ===========
    
    @abstractmethod
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get current ticker for symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Ticker data including last price, bid, ask, volume
        """
        pass
    
    @abstractmethod
    async def get_orderbook(self, symbol: str, limit: int = 20) -> OrderBook:
        """
        Get order book for symbol.
        
        Args:
            symbol: Trading pair symbol
            limit: Depth limit
            
        Returns:
            Order book snapshot
        """
        pass
    
    @abstractmethod
    async def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
        start_time: datetime = None,
        end_time: datetime = None,
    ) -> List[Candle]:
        """
        Get historical OHLCV candles.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe (1m, 5m, 1h, etc.)
            limit: Number of candles
            start_time: Start time filter
            end_time: End time filter
            
        Returns:
            List of Candle objects
        """
        pass
    
    @abstractmethod
    async def get_market_info(self, symbol: str) -> MarketInfo:
        """
        Get market/symbol information.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Market info including precision, limits, etc.
        """
        pass
    
    # =========== Order Operations ===========
    
    @abstractmethod
    async def place_order(
        self,
        symbol: str,
        side: Side,
        order_type: OrderType,
        quantity: float,
        price: float = None,
        stop_price: float = None,
        reduce_only: bool = False,
        post_only: bool = False,
        client_order_id: str = None,
        **kwargs,
    ) -> Order:
        """
        Place an order.
        
        Args:
            symbol: Trading pair
            side: Buy or sell
            order_type: Market, limit, stop, etc.
            quantity: Order quantity
            price: Limit price (for limit orders)
            stop_price: Stop/trigger price
            reduce_only: Only reduce position
            post_only: Reject if would be taker
            client_order_id: Custom order ID
            
        Returns:
            Order object with order_id
        """
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        """
        Cancel an order.
        
        Args:
            order_id: Exchange order ID
            symbol: Trading pair (required by some exchanges)
            
        Returns:
            True if cancelled successfully
        """
        pass
    
    @abstractmethod
    async def cancel_all_orders(self, symbol: str = None) -> int:
        """
        Cancel all open orders.
        
        Args:
            symbol: Cancel for specific symbol, or all
            
        Returns:
            Number of orders cancelled
        """
        pass
    
    @abstractmethod
    async def get_order(self, order_id: str, symbol: str = None) -> Optional[Order]:
        """
        Get order by ID.
        
        Args:
            order_id: Exchange order ID
            symbol: Trading pair
            
        Returns:
            Order object or None if not found
        """
        pass
    
    @abstractmethod
    async def get_open_orders(self, symbol: str = None) -> List[Order]:
        """
        Get all open orders.
        
        Args:
            symbol: Filter by symbol
            
        Returns:
            List of open orders
        """
        pass
    
    @abstractmethod
    async def get_order_history(
        self,
        symbol: str = None,
        limit: int = 100,
        start_time: datetime = None,
    ) -> List[Order]:
        """
        Get order history.
        
        Args:
            symbol: Filter by symbol
            limit: Number of orders
            start_time: Start time filter
            
        Returns:
            List of historical orders
        """
        pass
    
    # =========== WebSocket Subscriptions ===========
    
    @abstractmethod
    async def subscribe_ticker(self, symbol: str) -> bool:
        """Subscribe to ticker updates."""
        pass
    
    @abstractmethod
    async def subscribe_orderbook(self, symbol: str, depth: int = 20) -> bool:
        """Subscribe to order book updates."""
        pass
    
    @abstractmethod
    async def subscribe_candles(self, symbol: str, timeframe: str) -> bool:
        """Subscribe to candle updates."""
        pass
    
    @abstractmethod
    async def subscribe_user_data(self) -> bool:
        """Subscribe to user data (orders, positions, balances)."""
        pass
    
    async def unsubscribe_all(self) -> None:
        """Unsubscribe from all streams."""
        pass
    
    # =========== Callbacks ===========
    
    def set_callbacks(
        self,
        on_order_update: Callable[[Order], None] = None,
        on_position_update: Callable[[Dict], None] = None,
        on_balance_update: Callable[[AccountBalance], None] = None,
        on_tick: Callable[[Tick], None] = None,
        on_candle: Callable[[Candle], None] = None,
    ) -> None:
        """Set event callbacks."""
        if on_order_update:
            self._on_order_update = on_order_update
        if on_position_update:
            self._on_position_update = on_position_update
        if on_balance_update:
            self._on_balance_update = on_balance_update
        if on_tick:
            self._on_tick = on_tick
        if on_candle:
            self._on_candle = on_candle
    
    # =========== Utility Methods ===========
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol to exchange format.
        Override in adapters for exchange-specific formats.
        
        Args:
            symbol: Standard symbol (e.g., "BTC/USDT")
            
        Returns:
            Exchange-specific symbol format
        """
        return symbol.replace("/", "")
    
    def denormalize_symbol(self, symbol: str) -> str:
        """
        Convert exchange symbol to standard format.
        Override in adapters.
        
        Args:
            symbol: Exchange-specific symbol
            
        Returns:
            Standard format (e.g., "BTC/USDT")
        """
        return symbol
    
    def format_quantity(self, symbol: str, quantity: float) -> float:
        """
        Format quantity to exchange precision.
        
        Args:
            symbol: Trading pair
            quantity: Raw quantity
            
        Returns:
            Formatted quantity
        """
        market_info = self._market_info_cache.get(symbol)
        if market_info:
            precision = market_info.quantity_precision
            return round(quantity, precision)
        return quantity
    
    def format_price(self, symbol: str, price: float) -> float:
        """
        Format price to exchange precision.
        
        Args:
            symbol: Trading pair
            price: Raw price
            
        Returns:
            Formatted price
        """
        market_info = self._market_info_cache.get(symbol)
        if market_info:
            precision = market_info.price_precision
            return round(price, precision)
        return price
    
    async def _rate_limit(self) -> None:
        """Apply rate limiting before API call."""
        async with self._rate_limiter:
            now = asyncio.get_event_loop().time()
            wait_time = max(0, (1 / self.config.rate_limit) - (now - self._last_request_time))
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last_request_time = asyncio.get_event_loop().time()
    
    def is_connected(self) -> bool:
        """Check if exchange is connected."""
        return self.status == ExchangeStatus.CONNECTED
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} exchange_id={self.exchange_id} status={self.status.value}>"
