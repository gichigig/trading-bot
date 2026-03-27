"""
Data Feeds
===========

Abstract data feed implementations for live and historical data.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, AsyncIterator
import asyncio
import logging
import csv
from pathlib import Path

from trading_bot.core.types import Candle, Tick

logger = logging.getLogger(__name__)


class DataFeed(ABC):
    """Abstract base class for data feeds."""
    
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the data source."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the data source."""
        pass
    
    @abstractmethod
    async def subscribe(self, symbol: str, timeframe: str) -> bool:
        """Subscribe to a symbol/timeframe."""
        pass
    
    @abstractmethod
    async def unsubscribe(self, symbol: str, timeframe: str) -> bool:
        """Unsubscribe from a symbol/timeframe."""
        pass
    
    @abstractmethod
    def set_candle_callback(self, callback: Callable[[Candle], None]) -> None:
        """Set callback for candle updates."""
        pass
    
    @abstractmethod
    def set_tick_callback(self, callback: Callable[[Tick], None]) -> None:
        """Set callback for tick updates."""
        pass
    
    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if connected."""
        pass


class HistoricalDataFeed(DataFeed):
    """
    Historical data feed for backtesting.
    
    Reads data from CSV files or databases and replays them.
    """
    
    def __init__(
        self,
        data_dir: str = "data/historical",
        speed_multiplier: float = 1.0,  # 1.0 = realtime, 0 = as fast as possible
    ):
        self.data_dir = Path(data_dir)
        self.speed_multiplier = speed_multiplier
        
        self._connected = False
        self._candle_callback: Optional[Callable[[Candle], None]] = None
        self._tick_callback: Optional[Callable[[Tick], None]] = None
        
        self._subscriptions: Dict[str, Dict[str, bool]] = {}  # symbol -> {timeframe: active}
        self._data_cache: Dict[str, List[Candle]] = {}  # key -> candles
        
        self._replay_task: Optional[asyncio.Task] = None
        self._is_replaying = False
    
    async def connect(self) -> bool:
        """Initialize historical data feed."""
        if not self.data_dir.exists():
            logger.warning(f"Data directory does not exist: {self.data_dir}")
            self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self._connected = True
        logger.info(f"Historical data feed connected: {self.data_dir}")
        return True
    
    async def disconnect(self) -> None:
        """Stop data feed."""
        self._is_replaying = False
        if self._replay_task:
            self._replay_task.cancel()
            try:
                await self._replay_task
            except asyncio.CancelledError:
                pass
        self._connected = False
        logger.info("Historical data feed disconnected")
    
    async def subscribe(self, symbol: str, timeframe: str) -> bool:
        """Subscribe and load historical data."""
        if symbol not in self._subscriptions:
            self._subscriptions[symbol] = {}
        
        self._subscriptions[symbol][timeframe] = True
        
        # Load data
        key = f"{symbol}:{timeframe}"
        if key not in self._data_cache:
            candles = self._load_data(symbol, timeframe)
            self._data_cache[key] = candles
            logger.info(f"Loaded {len(candles)} candles for {key}")
        
        return True
    
    async def unsubscribe(self, symbol: str, timeframe: str) -> bool:
        """Unsubscribe from data."""
        if symbol in self._subscriptions:
            self._subscriptions[symbol].pop(timeframe, None)
        return True
    
    def set_candle_callback(self, callback: Callable[[Candle], None]) -> None:
        """Set candle callback."""
        self._candle_callback = callback
    
    def set_tick_callback(self, callback: Callable[[Tick], None]) -> None:
        """Set tick callback."""
        self._tick_callback = callback
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def _load_data(self, symbol: str, timeframe: str) -> List[Candle]:
        """Load data from file."""
        # Try different file formats
        base_name = f"{symbol.replace('/', '_')}_{timeframe}"
        
        # Try CSV
        csv_path = self.data_dir / f"{base_name}.csv"
        if csv_path.exists():
            return self._load_csv(csv_path, symbol, timeframe)
        
        logger.warning(f"No data file found for {symbol} {timeframe}")
        return []
    
    def _load_csv(self, path: Path, symbol: str, timeframe: str) -> List[Candle]:
        """Load candles from CSV file."""
        candles = []
        
        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                try:
                    # Support multiple timestamp formats
                    timestamp_str = row.get('timestamp') or row.get('time') or row.get('date')
                    if timestamp_str:
                        # Try parsing different formats
                        for fmt in [
                            "%Y-%m-%d %H:%M:%S",
                            "%Y-%m-%dT%H:%M:%S",
                            "%Y-%m-%d",
                        ]:
                            try:
                                timestamp = datetime.strptime(timestamp_str, fmt)
                                break
                            except ValueError:
                                continue
                        else:
                            # Try parsing as Unix timestamp
                            timestamp = datetime.utcfromtimestamp(float(timestamp_str))
                    
                    candle = Candle(
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=timestamp,
                        open=float(row.get('open', 0)),
                        high=float(row.get('high', 0)),
                        low=float(row.get('low', 0)),
                        close=float(row.get('close', 0)),
                        volume=float(row.get('volume', 0)),
                    )
                    candles.append(candle)
                    
                except Exception as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue
        
        # Sort by timestamp
        candles.sort(key=lambda c: c.timestamp)
        return candles
    
    async def replay(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> None:
        """
        Replay historical data through callbacks.
        
        Args:
            start_date: Start of replay period
            end_date: End of replay period
        """
        self._is_replaying = True
        
        # Collect all candles and sort by timestamp
        all_candles: List[Candle] = []
        
        for key, candles in self._data_cache.items():
            for candle in candles:
                if start_date and candle.timestamp < start_date:
                    continue
                if end_date and candle.timestamp > end_date:
                    continue
                all_candles.append(candle)
        
        all_candles.sort(key=lambda c: c.timestamp)
        
        logger.info(f"Replaying {len(all_candles)} candles")
        
        last_timestamp: Optional[datetime] = None
        
        for candle in all_candles:
            if not self._is_replaying:
                break
            
            # Simulate time delay
            if self.speed_multiplier > 0 and last_timestamp:
                time_diff = (candle.timestamp - last_timestamp).total_seconds()
                await asyncio.sleep(time_diff / self.speed_multiplier)
            
            # Send through callback
            if self._candle_callback:
                self._candle_callback(candle)
            
            last_timestamp = candle.timestamp
        
        self._is_replaying = False
        logger.info("Replay complete")
    
    async def replay_iter(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> AsyncIterator[Candle]:
        """
        Iterate through historical candles.
        
        Useful for backtesting engines that want to control replay.
        """
        all_candles: List[Candle] = []
        
        for key, candles in self._data_cache.items():
            for candle in candles:
                if start_date and candle.timestamp < start_date:
                    continue
                if end_date and candle.timestamp > end_date:
                    continue
                all_candles.append(candle)
        
        all_candles.sort(key=lambda c: c.timestamp)
        
        for candle in all_candles:
            yield candle
    
    def get_date_range(self) -> tuple:
        """Get the date range of loaded data."""
        all_timestamps = []
        
        for candles in self._data_cache.values():
            if candles:
                all_timestamps.extend([c.timestamp for c in candles])
        
        if not all_timestamps:
            return None, None
        
        return min(all_timestamps), max(all_timestamps)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get data feed statistics."""
        return {
            "connected": self._connected,
            "replaying": self._is_replaying,
            "subscriptions": self._subscriptions,
            "cached_series": len(self._data_cache),
            "total_candles": sum(len(c) for c in self._data_cache.values()),
            "date_range": self.get_date_range(),
        }


class LiveDataFeed(DataFeed):
    """
    Live data feed base class.
    
    Concrete implementations should inherit from this for specific exchanges.
    """
    
    def __init__(self):
        self._connected = False
        self._candle_callback: Optional[Callable[[Candle], None]] = None
        self._tick_callback: Optional[Callable[[Tick], None]] = None
        self._subscriptions: Dict[str, Dict[str, bool]] = {}
        
        # Reconnection settings
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 10
        self._reconnect_delay = 1.0  # seconds
        self._reconnect_backoff = 2.0  # exponential backoff multiplier
    
    async def connect(self) -> bool:
        """Connect to live data source."""
        raise NotImplementedError("Subclasses must implement connect()")
    
    async def disconnect(self) -> None:
        """Disconnect from live data source."""
        raise NotImplementedError("Subclasses must implement disconnect()")
    
    async def subscribe(self, symbol: str, timeframe: str) -> bool:
        """Subscribe to live data."""
        raise NotImplementedError("Subclasses must implement subscribe()")
    
    async def unsubscribe(self, symbol: str, timeframe: str) -> bool:
        """Unsubscribe from live data."""
        raise NotImplementedError("Subclasses must implement unsubscribe()")
    
    def set_candle_callback(self, callback: Callable[[Candle], None]) -> None:
        self._candle_callback = callback
    
    def set_tick_callback(self, callback: Callable[[Tick], None]) -> None:
        self._tick_callback = callback
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    async def _handle_reconnect(self) -> bool:
        """Handle reconnection with exponential backoff."""
        while self._reconnect_attempts < self._max_reconnect_attempts:
            self._reconnect_attempts += 1
            delay = self._reconnect_delay * (self._reconnect_backoff ** (self._reconnect_attempts - 1))
            
            logger.warning(f"Reconnecting (attempt {self._reconnect_attempts}) in {delay}s...")
            await asyncio.sleep(delay)
            
            try:
                if await self.connect():
                    self._reconnect_attempts = 0
                    
                    # Resubscribe to all symbols
                    for symbol, timeframes in self._subscriptions.items():
                        for timeframe in timeframes:
                            await self.subscribe(symbol, timeframe)
                    
                    return True
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
        
        logger.error("Max reconnection attempts reached")
        return False


class CCXTLiveDataFeed(LiveDataFeed):
    """
    Live data feed using CCXT library.
    
    Supports 100+ cryptocurrency exchanges including:
    - Binance, Binance US, Binance Futures
    - Coinbase, Coinbase Pro
    - Kraken, Kraken Futures  
    - Bybit, OKX, KuCoin, Gate.io
    - And many more...
    
    Usage:
        feed = CCXTLiveDataFeed("binance")
        await feed.connect()
        feed.set_candle_callback(my_handler)
        await feed.subscribe("BTC/USDT", "1h")
        await feed.start_streaming()
    """
    
    def __init__(
        self,
        exchange_id: str,
        api_key: str = None,
        api_secret: str = None,
        sandbox: bool = False,
        rate_limit: bool = True,
    ):
        super().__init__()
        self.exchange_id = exchange_id.lower()
        self.api_key = api_key
        self.api_secret = api_secret
        self.sandbox = sandbox
        self.rate_limit = rate_limit
        
        self._exchange = None
        self._ws_exchange = None  # For WebSocket if supported
        self._streaming_task: Optional[asyncio.Task] = None
        self._stop_streaming = False
        self._poll_interval = 1.0  # seconds between polls
        self._last_candle_times: Dict[str, datetime] = {}
    
    async def connect(self) -> bool:
        """Connect to exchange via CCXT."""
        try:
            import ccxt.async_support as ccxt
            
            # Get exchange class
            if not hasattr(ccxt, self.exchange_id):
                logger.error(f"Exchange not supported: {self.exchange_id}")
                logger.info(f"Supported exchanges: {', '.join(ccxt.exchanges[:20])}...")
                return False
            
            exchange_class = getattr(ccxt, self.exchange_id)
            
            # Create exchange instance
            config = {
                'enableRateLimit': self.rate_limit,
            }
            
            if self.api_key:
                config['apiKey'] = self.api_key
            if self.api_secret:
                config['secret'] = self.api_secret
            
            self._exchange = exchange_class(config)
            
            if self.sandbox:
                self._exchange.set_sandbox_mode(True)
            
            # Load markets
            await self._exchange.load_markets()
            
            self._connected = True
            logger.info(f"Connected to {self.exchange_id} - {len(self._exchange.markets)} markets available")
            
            return True
            
        except ImportError:
            logger.error("CCXT not installed. Run: pip install ccxt")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to {self.exchange_id}: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from exchange."""
        self._stop_streaming = True
        
        if self._streaming_task:
            self._streaming_task.cancel()
            try:
                await self._streaming_task
            except asyncio.CancelledError:
                pass
        
        if self._exchange:
            await self._exchange.close()
            self._exchange = None
        
        self._connected = False
        logger.info(f"Disconnected from {self.exchange_id}")
    
    async def subscribe(self, symbol: str, timeframe: str) -> bool:
        """Subscribe to symbol/timeframe."""
        if not self._connected:
            logger.error("Not connected to exchange")
            return False
        
        # Validate symbol
        if symbol not in self._exchange.markets:
            logger.error(f"Symbol not found: {symbol}")
            logger.info(f"Available symbols: {list(self._exchange.markets.keys())[:10]}...")
            return False
        
        # Validate timeframe
        if hasattr(self._exchange, 'timeframes'):
            if timeframe not in self._exchange.timeframes:
                logger.error(f"Timeframe not supported: {timeframe}")
                logger.info(f"Supported timeframes: {list(self._exchange.timeframes.keys())}")
                return False
        
        if symbol not in self._subscriptions:
            self._subscriptions[symbol] = {}
        
        self._subscriptions[symbol][timeframe] = True
        logger.info(f"Subscribed to {symbol} {timeframe}")
        
        return True
    
    async def unsubscribe(self, symbol: str, timeframe: str) -> bool:
        """Unsubscribe from symbol/timeframe."""
        if symbol in self._subscriptions:
            self._subscriptions[symbol].pop(timeframe, None)
            if not self._subscriptions[symbol]:
                del self._subscriptions[symbol]
        
        key = f"{symbol}:{timeframe}"
        self._last_candle_times.pop(key, None)
        
        logger.info(f"Unsubscribed from {symbol} {timeframe}")
        return True
    
    async def start_streaming(self) -> None:
        """Start streaming data for all subscriptions."""
        if not self._subscriptions:
            logger.warning("No subscriptions to stream")
            return
        
        self._stop_streaming = False
        self._streaming_task = asyncio.create_task(self._stream_loop())
        logger.info("Started data streaming")
    
    async def stop_streaming(self) -> None:
        """Stop streaming data."""
        self._stop_streaming = True
        if self._streaming_task:
            self._streaming_task.cancel()
            try:
                await self._streaming_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped data streaming")
    
    async def _stream_loop(self) -> None:
        """Main streaming loop - polls for new candles."""
        while not self._stop_streaming:
            try:
                for symbol, timeframes in list(self._subscriptions.items()):
                    for timeframe in list(timeframes.keys()):
                        await self._fetch_new_candles(symbol, timeframe)
                
                await asyncio.sleep(self._poll_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Streaming error: {e}")
                await asyncio.sleep(5)
    
    async def _fetch_new_candles(self, symbol: str, timeframe: str) -> None:
        """Fetch new candles for a symbol/timeframe."""
        key = f"{symbol}:{timeframe}"
        
        try:
            # Fetch recent candles
            ohlcv = await self._exchange.fetch_ohlcv(
                symbol, 
                timeframe, 
                limit=5  # Get last 5 candles to catch any we missed
            )
            
            if not ohlcv:
                return
            
            last_known_time = self._last_candle_times.get(key)
            
            for candle_data in ohlcv:
                timestamp = datetime.utcfromtimestamp(candle_data[0] / 1000)
                
                # Skip old candles
                if last_known_time and timestamp <= last_known_time:
                    continue
                
                candle = Candle(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=timestamp,
                    open=float(candle_data[1]),
                    high=float(candle_data[2]),
                    low=float(candle_data[3]),
                    close=float(candle_data[4]),
                    volume=float(candle_data[5]),
                )
                
                # Update last known time
                self._last_candle_times[key] = timestamp
                
                # Send through callback
                if self._candle_callback:
                    self._candle_callback(candle)
                    
        except Exception as e:
            logger.error(f"Error fetching candles for {symbol} {timeframe}: {e}")
    
    async def fetch_historical(
        self,
        symbol: str,
        timeframe: str,
        since: datetime = None,
        limit: int = 1000,
    ) -> List[Candle]:
        """
        Fetch historical candles.
        
        Args:
            symbol: Trading pair (e.g., "BTC/USDT")
            timeframe: Candle timeframe (e.g., "1h", "4h", "1d")
            since: Start datetime
            limit: Max candles to fetch
            
        Returns:
            List of Candle objects
        """
        if not self._connected:
            logger.error("Not connected to exchange")
            return []
        
        try:
            since_ts = int(since.timestamp() * 1000) if since else None
            
            ohlcv = await self._exchange.fetch_ohlcv(
                symbol,
                timeframe,
                since=since_ts,
                limit=limit,
            )
            
            candles = []
            for data in ohlcv:
                candle = Candle(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=datetime.utcfromtimestamp(data[0] / 1000),
                    open=float(data[1]),
                    high=float(data[2]),
                    low=float(data[3]),
                    close=float(data[4]),
                    volume=float(data[5]),
                )
                candles.append(candle)
            
            logger.info(f"Fetched {len(candles)} historical candles for {symbol} {timeframe}")
            return candles
            
        except Exception as e:
            logger.error(f"Failed to fetch historical data: {e}")
            return []
    
    async def fetch_all_historical(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime = None,
    ) -> List[Candle]:
        """
        Fetch all historical candles between dates (handles pagination).
        
        Useful for building large datasets for backtesting.
        """
        if not self._connected:
            return []
        
        end_date = end_date or datetime.utcnow()
        all_candles = []
        current_since = start_date
        
        while current_since < end_date:
            candles = await self.fetch_historical(
                symbol, timeframe, since=current_since, limit=1000
            )
            
            if not candles:
                break
            
            all_candles.extend(candles)
            
            # Move to after last candle
            current_since = candles[-1].timestamp + timedelta(seconds=1)
            
            # Rate limiting
            await asyncio.sleep(self._exchange.rateLimit / 1000)
            
            logger.debug(f"Fetched {len(all_candles)} candles so far...")
        
        # Remove duplicates and sort
        seen = set()
        unique_candles = []
        for c in all_candles:
            if c.timestamp not in seen:
                seen.add(c.timestamp)
                unique_candles.append(c)
        
        unique_candles.sort(key=lambda c: c.timestamp)
        
        logger.info(f"Fetched {len(unique_candles)} total candles for {symbol} {timeframe}")
        return unique_candles
    
    async def get_ticker(self, symbol: str) -> Optional[Tick]:
        """Get current ticker for symbol."""
        if not self._connected:
            return None
        
        try:
            ticker = await self._exchange.fetch_ticker(symbol)
            
            return Tick(
                symbol=symbol,
                timestamp=datetime.utcnow(),
                bid=float(ticker['bid']) if ticker['bid'] else 0,
                ask=float(ticker['ask']) if ticker['ask'] else 0,
                last=float(ticker['last']) if ticker['last'] else 0,
                volume=float(ticker['quoteVolume']) if ticker['quoteVolume'] else 0,
            )
            
        except Exception as e:
            logger.error(f"Failed to get ticker: {e}")
            return None
    
    def get_available_symbols(self) -> List[str]:
        """Get list of available trading pairs."""
        if not self._exchange:
            return []
        return list(self._exchange.markets.keys())
    
    def get_available_timeframes(self) -> List[str]:
        """Get list of available timeframes."""
        if not self._exchange or not hasattr(self._exchange, 'timeframes'):
            return ['1m', '5m', '15m', '1h', '4h', '1d']
        return list(self._exchange.timeframes.keys())
