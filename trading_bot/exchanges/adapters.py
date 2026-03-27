"""
Exchange Adapters
=================

Concrete exchange implementations.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
import asyncio
import logging
import random
import uuid

from trading_bot.core.types import (
    Order, OrderType, OrderStatus, Side, Candle, Tick
)
from trading_bot.exchanges.base import (
    BaseExchange, ExchangeConfig, ExchangeStatus,
    OrderBook, AccountBalance, MarketInfo
)

logger = logging.getLogger(__name__)


class SimulatedExchange(BaseExchange):
    """
    Simulated exchange for backtesting and paper trading.
    
    Provides realistic simulation with:
    - Configurable latency and slippage
    - Order matching simulation
    - Balance tracking
    """
    
    def __init__(
        self,
        config: ExchangeConfig,
        initial_balance: float = 10000.0,
        base_currency: str = "USDT",
        slippage_pct: float = 0.05,
        latency_ms: tuple = (10, 50),
        fill_probability: float = 0.95,
    ):
        super().__init__(config)
        
        self._initial_balance = initial_balance
        self._base_currency = base_currency
        self._slippage_pct = slippage_pct
        self._latency_range = latency_ms
        self._fill_probability = fill_probability
        
        # State
        self._balances: Dict[str, AccountBalance] = {}
        self._orders: Dict[str, Order] = {}
        self._positions: Dict[str, Dict] = {}
        self._prices: Dict[str, float] = {}
        self._order_counter = 0
        
        # Initialize balance
        self._balances[base_currency] = AccountBalance(
            currency=base_currency,
            total=initial_balance,
            available=initial_balance,
        )
    
    async def connect(self) -> bool:
        """Simulate connection."""
        await self._simulate_latency()
        self.status = ExchangeStatus.CONNECTED
        logger.info(f"Simulated exchange connected")
        return True
    
    async def disconnect(self) -> None:
        """Simulate disconnection."""
        self.status = ExchangeStatus.DISCONNECTED
        logger.info(f"Simulated exchange disconnected")
    
    async def ping(self) -> bool:
        """Simulate ping."""
        return self.status == ExchangeStatus.CONNECTED
    
    async def get_balance(self, currency: str = None) -> List[AccountBalance]:
        """Get simulated balances."""
        if currency:
            bal = self._balances.get(currency)
            return [bal] if bal else []
        return list(self._balances.values())
    
    async def get_positions(self, symbol: str = None) -> List[Dict[str, Any]]:
        """Get simulated positions."""
        if symbol:
            pos = self._positions.get(symbol)
            return [pos] if pos else []
        return list(self._positions.values())
    
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get simulated ticker."""
        price = self._prices.get(symbol, 100.0)
        spread = price * 0.0005  # 0.05% spread
        
        return {
            "symbol": symbol,
            "last": price,
            "bid": price - spread / 2,
            "ask": price + spread / 2,
            "volume_24h": 1000000,
            "timestamp": datetime.utcnow(),
        }
    
    async def get_orderbook(self, symbol: str, limit: int = 20) -> OrderBook:
        """Get simulated order book."""
        price = self._prices.get(symbol, 100.0)
        spread = price * 0.0005
        
        bids = [(price - spread / 2 - i * 0.01, random.uniform(1, 10)) for i in range(limit)]
        asks = [(price + spread / 2 + i * 0.01, random.uniform(1, 10)) for i in range(limit)]
        
        return OrderBook(symbol=symbol, bids=bids, asks=asks)
    
    async def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
        start_time: datetime = None,
        end_time: datetime = None,
    ) -> List[Candle]:
        """Get simulated candles."""
        # Return empty - should be populated by data feed
        return []
    
    async def get_market_info(self, symbol: str) -> MarketInfo:
        """Get simulated market info."""
        return MarketInfo(
            symbol=symbol,
            base_currency=symbol.split("/")[0] if "/" in symbol else symbol[:3],
            quote_currency=symbol.split("/")[1] if "/" in symbol else symbol[3:],
            price_precision=2,
            quantity_precision=4,
            min_quantity=0.001,
            max_quantity=10000,
            min_notional=10.0,
            tick_size=0.01,
            lot_size=0.001,
        )
    
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
        """Place simulated order."""
        await self._simulate_latency()
        
        # Generate order ID
        self._order_counter += 1
        order_id = f"sim_{self._order_counter:08d}"
        
        # Get current price
        current_price = self._prices.get(symbol, price or 100.0)
        
        # Determine fill price with slippage
        if order_type == OrderType.MARKET:
            slippage = current_price * (self._slippage_pct / 100)
            fill_price = current_price + slippage if side == Side.BUY else current_price - slippage
        else:
            fill_price = price or current_price
        
        # Determine fill status
        filled = random.random() < self._fill_probability
        
        # Create order
        order = Order(
            order_id=order_id,
            client_order_id=client_order_id or str(uuid.uuid4()),
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            status=OrderStatus.FILLED if filled and order_type == OrderType.MARKET else OrderStatus.NEW,
            filled_quantity=quantity if filled and order_type == OrderType.MARKET else 0.0,
            average_fill_price=fill_price if filled and order_type == OrderType.MARKET else 0.0,
            reduce_only=reduce_only,
        )
        
        if filled and order_type == OrderType.MARKET:
            # Update balance
            cost = fill_price * quantity
            if side == Side.BUY:
                self._balances[self._base_currency].available -= cost
                self._balances[self._base_currency].total -= cost
            else:
                self._balances[self._base_currency].available += cost
                self._balances[self._base_currency].total += cost
        
        self._orders[order_id] = order
        
        logger.debug(f"Simulated order placed: {order}")
        
        return order
    
    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        """Cancel simulated order."""
        await self._simulate_latency()
        
        order = self._orders.get(order_id)
        if order and order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
            order.status = OrderStatus.CANCELLED
            return True
        return False
    
    async def cancel_all_orders(self, symbol: str = None) -> int:
        """Cancel all simulated orders."""
        count = 0
        for order in self._orders.values():
            if (symbol is None or order.symbol == symbol) and order.status == OrderStatus.NEW:
                order.status = OrderStatus.CANCELLED
                count += 1
        return count
    
    async def get_order(self, order_id: str, symbol: str = None) -> Optional[Order]:
        """Get simulated order."""
        return self._orders.get(order_id)
    
    async def get_open_orders(self, symbol: str = None) -> List[Order]:
        """Get simulated open orders."""
        orders = [
            o for o in self._orders.values()
            if o.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]
        ]
        if symbol:
            orders = [o for o in orders if o.symbol == symbol]
        return orders
    
    async def get_order_history(
        self,
        symbol: str = None,
        limit: int = 100,
        start_time: datetime = None,
    ) -> List[Order]:
        """Get simulated order history."""
        orders = list(self._orders.values())
        if symbol:
            orders = [o for o in orders if o.symbol == symbol]
        return orders[-limit:]
    
    async def subscribe_ticker(self, symbol: str) -> bool:
        """Subscribe to ticker (no-op for simulation)."""
        return True
    
    async def subscribe_orderbook(self, symbol: str, depth: int = 20) -> bool:
        """Subscribe to order book (no-op for simulation)."""
        return True
    
    async def subscribe_candles(self, symbol: str, timeframe: str) -> bool:
        """Subscribe to candles (no-op for simulation)."""
        return True
    
    async def subscribe_user_data(self) -> bool:
        """Subscribe to user data (no-op for simulation)."""
        return True
    
    def set_price(self, symbol: str, price: float) -> None:
        """Set current price for simulation."""
        self._prices[symbol] = price
    
    def fill_order(self, order_id: str, fill_price: float = None) -> Optional[Order]:
        """Manually fill a pending order."""
        order = self._orders.get(order_id)
        if order and order.status == OrderStatus.NEW:
            order.status = OrderStatus.FILLED
            order.filled_quantity = order.quantity
            order.average_fill_price = fill_price or order.price or self._prices.get(order.symbol, 100.0)
            return order
        return None
    
    def reset(self) -> None:
        """Reset simulation state."""
        self._orders.clear()
        self._positions.clear()
        self._balances.clear()
        self._balances[self._base_currency] = AccountBalance(
            currency=self._base_currency,
            total=self._initial_balance,
            available=self._initial_balance,
        )
    
    async def _simulate_latency(self) -> None:
        """Simulate network latency."""
        latency = random.uniform(*self._latency_range) / 1000
        await asyncio.sleep(latency)


class BinanceAdapter(BaseExchange):
    """
    Binance exchange adapter.
    
    Supports both spot and futures trading.
    Uses ccxt under the hood if available, otherwise native API.
    """
    
    def __init__(self, config: ExchangeConfig):
        super().__init__(config)
        self._client = None
        self._ws_client = None
    
    async def connect(self) -> bool:
        """Connect to Binance."""
        try:
            # Try to use ccxt if available
            try:
                import ccxt.async_support as ccxt
                
                self._client = ccxt.binance({
                    'apiKey': self.config.api_key,
                    'secret': self.config.api_secret,
                    'sandbox': self.config.testnet,
                    'options': {
                        'defaultType': 'future' if self.config.extra.get('futures') else 'spot',
                    }
                })
                
            except ImportError:
                logger.warning("ccxt not installed, using native API (limited)")
                # Would implement native API here
                pass
            
            self.status = ExchangeStatus.CONNECTED
            logger.info("Connected to Binance")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Binance: {e}")
            self.status = ExchangeStatus.ERROR
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from Binance."""
        if self._client:
            await self._client.close()
        self.status = ExchangeStatus.DISCONNECTED
    
    async def ping(self) -> bool:
        """Ping Binance."""
        try:
            await self._client.fetch_time()
            return True
        except Exception:
            return False
    
    async def get_balance(self, currency: str = None) -> List[AccountBalance]:
        """Get Binance balances."""
        await self._rate_limit()
        
        try:
            balance = await self._client.fetch_balance()
            result = []
            
            for curr, data in balance.items():
                if curr in ['info', 'free', 'used', 'total', 'timestamp', 'datetime']:
                    continue
                if currency and curr != currency:
                    continue
                if data.get('total', 0) > 0:
                    result.append(AccountBalance(
                        currency=curr,
                        total=data.get('total', 0),
                        available=data.get('free', 0),
                        locked=data.get('used', 0),
                    ))
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            return []
    
    async def get_positions(self, symbol: str = None) -> List[Dict[str, Any]]:
        """Get Binance futures positions."""
        await self._rate_limit()
        
        try:
            positions = await self._client.fetch_positions([symbol] if symbol else None)
            return [p for p in positions if float(p.get('contracts', 0)) != 0]
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
            return []
    
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get Binance ticker."""
        await self._rate_limit()
        
        try:
            ticker = await self._client.fetch_ticker(symbol)
            return {
                "symbol": symbol,
                "last": ticker['last'],
                "bid": ticker['bid'],
                "ask": ticker['ask'],
                "volume_24h": ticker['quoteVolume'],
                "change_24h_pct": ticker['percentage'],
                "timestamp": datetime.utcnow(),
            }
        except Exception as e:
            logger.error(f"Failed to get ticker: {e}")
            return {}
    
    async def get_orderbook(self, symbol: str, limit: int = 20) -> OrderBook:
        """Get Binance order book."""
        await self._rate_limit()
        
        try:
            book = await self._client.fetch_order_book(symbol, limit)
            return OrderBook(
                symbol=symbol,
                bids=[(b[0], b[1]) for b in book['bids']],
                asks=[(a[0], a[1]) for a in book['asks']],
            )
        except Exception as e:
            logger.error(f"Failed to get order book: {e}")
            return OrderBook(symbol=symbol, bids=[], asks=[])
    
    async def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
        start_time: datetime = None,
        end_time: datetime = None,
    ) -> List[Candle]:
        """Get Binance candles."""
        await self._rate_limit()
        
        try:
            since = int(start_time.timestamp() * 1000) if start_time else None
            ohlcv = await self._client.fetch_ohlcv(symbol, timeframe, since, limit)
            
            candles = []
            for o in ohlcv:
                candles.append(Candle(
                    symbol=symbol,
                    timestamp=datetime.fromtimestamp(o[0] / 1000),
                    open=o[1],
                    high=o[2],
                    low=o[3],
                    close=o[4],
                    volume=o[5],
                    timeframe=timeframe,
                ))
            
            return candles
            
        except Exception as e:
            logger.error(f"Failed to get candles: {e}")
            return []
    
    async def get_market_info(self, symbol: str) -> MarketInfo:
        """Get Binance market info."""
        await self._rate_limit()
        
        try:
            markets = await self._client.load_markets()
            market = markets.get(symbol)
            
            if not market:
                raise ValueError(f"Market not found: {symbol}")
            
            return MarketInfo(
                symbol=symbol,
                base_currency=market['base'],
                quote_currency=market['quote'],
                price_precision=market['precision']['price'],
                quantity_precision=market['precision']['amount'],
                min_quantity=market['limits']['amount']['min'],
                max_quantity=market['limits']['amount']['max'],
                min_notional=market['limits']['cost']['min'] or 10.0,
                tick_size=market['precision']['price'],
                lot_size=market['precision']['amount'],
                is_spot=market['spot'],
                is_futures=market.get('future', False),
            )
            
        except Exception as e:
            logger.error(f"Failed to get market info: {e}")
            return None
    
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
        """Place order on Binance."""
        await self._rate_limit()
        
        try:
            order_type_str = order_type.value.lower()
            side_str = side.value.lower()
            
            params = {}
            if reduce_only:
                params['reduceOnly'] = True
            if post_only:
                params['postOnly'] = True
            if stop_price:
                params['stopPrice'] = stop_price
            
            result = await self._client.create_order(
                symbol=symbol,
                type=order_type_str,
                side=side_str,
                amount=quantity,
                price=price,
                params=params,
            )
            
            return Order(
                order_id=str(result['id']),
                client_order_id=result.get('clientOrderId', ''),
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                stop_price=stop_price,
                status=self._map_order_status(result['status']),
                filled_quantity=result.get('filled', 0),
                average_fill_price=result.get('average', 0),
                reduce_only=reduce_only,
            )
            
        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        """Cancel order on Binance."""
        await self._rate_limit()
        
        try:
            await self._client.cancel_order(order_id, symbol)
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order: {e}")
            return False
    
    async def cancel_all_orders(self, symbol: str = None) -> int:
        """Cancel all orders on Binance."""
        await self._rate_limit()
        
        try:
            result = await self._client.cancel_all_orders(symbol)
            return len(result) if isinstance(result, list) else 1
        except Exception as e:
            logger.error(f"Failed to cancel all orders: {e}")
            return 0
    
    async def get_order(self, order_id: str, symbol: str = None) -> Optional[Order]:
        """Get order from Binance."""
        await self._rate_limit()
        
        try:
            result = await self._client.fetch_order(order_id, symbol)
            return self._parse_order(result)
        except Exception as e:
            logger.error(f"Failed to get order: {e}")
            return None
    
    async def get_open_orders(self, symbol: str = None) -> List[Order]:
        """Get open orders from Binance."""
        await self._rate_limit()
        
        try:
            orders = await self._client.fetch_open_orders(symbol)
            return [self._parse_order(o) for o in orders]
        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            return []
    
    async def get_order_history(
        self,
        symbol: str = None,
        limit: int = 100,
        start_time: datetime = None,
    ) -> List[Order]:
        """Get order history from Binance."""
        await self._rate_limit()
        
        try:
            since = int(start_time.timestamp() * 1000) if start_time else None
            orders = await self._client.fetch_closed_orders(symbol, since, limit)
            return [self._parse_order(o) for o in orders]
        except Exception as e:
            logger.error(f"Failed to get order history: {e}")
            return []
    
    async def subscribe_ticker(self, symbol: str) -> bool:
        """Subscribe to Binance ticker stream."""
        # WebSocket implementation would go here
        logger.info(f"Subscribed to ticker: {symbol}")
        return True
    
    async def subscribe_orderbook(self, symbol: str, depth: int = 20) -> bool:
        """Subscribe to Binance order book stream."""
        logger.info(f"Subscribed to orderbook: {symbol}")
        return True
    
    async def subscribe_candles(self, symbol: str, timeframe: str) -> bool:
        """Subscribe to Binance candle stream."""
        logger.info(f"Subscribed to candles: {symbol} {timeframe}")
        return True
    
    async def subscribe_user_data(self) -> bool:
        """Subscribe to Binance user data stream."""
        logger.info("Subscribed to user data")
        return True
    
    def _parse_order(self, data: Dict) -> Order:
        """Parse Binance order data to Order object."""
        return Order(
            order_id=str(data['id']),
            client_order_id=data.get('clientOrderId', ''),
            symbol=data['symbol'],
            side=Side.BUY if data['side'] == 'buy' else Side.SELL,
            order_type=OrderType(data['type'].upper()),
            quantity=data['amount'],
            price=data.get('price'),
            status=self._map_order_status(data['status']),
            filled_quantity=data.get('filled', 0),
            average_fill_price=data.get('average', 0),
        )
    
    def _map_order_status(self, status: str) -> OrderStatus:
        """Map Binance order status to OrderStatus enum."""
        status_map = {
            'open': OrderStatus.NEW,
            'closed': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELLED,
            'expired': OrderStatus.EXPIRED,
            'rejected': OrderStatus.REJECTED,
        }
        return status_map.get(status.lower(), OrderStatus.NEW)


class BybitAdapter(BaseExchange):
    """
    Bybit exchange adapter.
    
    Similar structure to Binance adapter.
    """
    
    def __init__(self, config: ExchangeConfig):
        super().__init__(config)
        self._client = None
    
    async def connect(self) -> bool:
        """Connect to Bybit."""
        try:
            try:
                import ccxt.async_support as ccxt
                
                self._client = ccxt.bybit({
                    'apiKey': self.config.api_key,
                    'secret': self.config.api_secret,
                    'sandbox': self.config.testnet,
                })
                
            except ImportError:
                logger.warning("ccxt not installed")
                pass
            
            self.status = ExchangeStatus.CONNECTED
            logger.info("Connected to Bybit")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Bybit: {e}")
            self.status = ExchangeStatus.ERROR
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from Bybit."""
        if self._client:
            await self._client.close()
        self.status = ExchangeStatus.DISCONNECTED
    
    async def ping(self) -> bool:
        """Ping Bybit."""
        try:
            await self._client.fetch_time()
            return True
        except Exception:
            return False
    
    # Implement remaining methods similar to BinanceAdapter
    # For brevity, using placeholder implementations
    
    async def get_balance(self, currency: str = None) -> List[AccountBalance]:
        await self._rate_limit()
        try:
            balance = await self._client.fetch_balance()
            result = []
            for curr, data in balance.items():
                if curr in ['info', 'free', 'used', 'total', 'timestamp', 'datetime']:
                    continue
                if currency and curr != currency:
                    continue
                if data.get('total', 0) > 0:
                    result.append(AccountBalance(
                        currency=curr,
                        total=data.get('total', 0),
                        available=data.get('free', 0),
                        locked=data.get('used', 0),
                    ))
            return result
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            return []
    
    async def get_positions(self, symbol: str = None) -> List[Dict[str, Any]]:
        await self._rate_limit()
        try:
            positions = await self._client.fetch_positions([symbol] if symbol else None)
            return [p for p in positions if float(p.get('contracts', 0)) != 0]
        except Exception:
            return []
    
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        await self._rate_limit()
        try:
            ticker = await self._client.fetch_ticker(symbol)
            return {
                "symbol": symbol,
                "last": ticker['last'],
                "bid": ticker['bid'],
                "ask": ticker['ask'],
                "volume_24h": ticker['quoteVolume'],
                "timestamp": datetime.utcnow(),
            }
        except Exception:
            return {}
    
    async def get_orderbook(self, symbol: str, limit: int = 20) -> OrderBook:
        await self._rate_limit()
        try:
            book = await self._client.fetch_order_book(symbol, limit)
            return OrderBook(
                symbol=symbol,
                bids=[(b[0], b[1]) for b in book['bids']],
                asks=[(a[0], a[1]) for a in book['asks']],
            )
        except Exception:
            return OrderBook(symbol=symbol, bids=[], asks=[])
    
    async def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
        start_time: datetime = None,
        end_time: datetime = None,
    ) -> List[Candle]:
        await self._rate_limit()
        try:
            since = int(start_time.timestamp() * 1000) if start_time else None
            ohlcv = await self._client.fetch_ohlcv(symbol, timeframe, since, limit)
            return [
                Candle(
                    symbol=symbol,
                    timestamp=datetime.fromtimestamp(o[0] / 1000),
                    open=o[1], high=o[2], low=o[3], close=o[4], volume=o[5],
                    timeframe=timeframe,
                )
                for o in ohlcv
            ]
        except Exception:
            return []
    
    async def get_market_info(self, symbol: str) -> MarketInfo:
        await self._rate_limit()
        try:
            markets = await self._client.load_markets()
            market = markets.get(symbol)
            if not market:
                return None
            return MarketInfo(
                symbol=symbol,
                base_currency=market['base'],
                quote_currency=market['quote'],
                price_precision=market['precision']['price'],
                quantity_precision=market['precision']['amount'],
                min_quantity=market['limits']['amount']['min'],
                max_quantity=market['limits']['amount']['max'],
                min_notional=market['limits']['cost']['min'] or 10.0,
                tick_size=market['precision']['price'],
                lot_size=market['precision']['amount'],
            )
        except Exception:
            return None
    
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
        await self._rate_limit()
        try:
            params = {}
            if reduce_only:
                params['reduceOnly'] = True
            if post_only:
                params['postOnly'] = True
            if stop_price:
                params['stopPrice'] = stop_price
            
            result = await self._client.create_order(
                symbol=symbol,
                type=order_type.value.lower(),
                side=side.value.lower(),
                amount=quantity,
                price=price,
                params=params,
            )
            
            return Order(
                order_id=str(result['id']),
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                status=OrderStatus.NEW,
            )
        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        await self._rate_limit()
        try:
            await self._client.cancel_order(order_id, symbol)
            return True
        except Exception:
            return False
    
    async def cancel_all_orders(self, symbol: str = None) -> int:
        await self._rate_limit()
        try:
            result = await self._client.cancel_all_orders(symbol)
            return len(result) if isinstance(result, list) else 1
        except Exception:
            return 0
    
    async def get_order(self, order_id: str, symbol: str = None) -> Optional[Order]:
        await self._rate_limit()
        try:
            result = await self._client.fetch_order(order_id, symbol)
            return Order(
                order_id=str(result['id']),
                symbol=result['symbol'],
                side=Side.BUY if result['side'] == 'buy' else Side.SELL,
                order_type=OrderType(result['type'].upper()),
                quantity=result['amount'],
                status=OrderStatus.FILLED if result['status'] == 'closed' else OrderStatus.NEW,
            )
        except Exception:
            return None
    
    async def get_open_orders(self, symbol: str = None) -> List[Order]:
        await self._rate_limit()
        try:
            orders = await self._client.fetch_open_orders(symbol)
            return [
                Order(
                    order_id=str(o['id']),
                    symbol=o['symbol'],
                    side=Side.BUY if o['side'] == 'buy' else Side.SELL,
                    order_type=OrderType(o['type'].upper()),
                    quantity=o['amount'],
                    status=OrderStatus.NEW,
                )
                for o in orders
            ]
        except Exception:
            return []
    
    async def get_order_history(
        self,
        symbol: str = None,
        limit: int = 100,
        start_time: datetime = None,
    ) -> List[Order]:
        await self._rate_limit()
        try:
            since = int(start_time.timestamp() * 1000) if start_time else None
            orders = await self._client.fetch_closed_orders(symbol, since, limit)
            return [
                Order(
                    order_id=str(o['id']),
                    symbol=o['symbol'],
                    side=Side.BUY if o['side'] == 'buy' else Side.SELL,
                    order_type=OrderType(o['type'].upper()),
                    quantity=o['amount'],
                    status=OrderStatus.FILLED,
                )
                for o in orders
            ]
        except Exception:
            return []
    
    async def subscribe_ticker(self, symbol: str) -> bool:
        return True
    
    async def subscribe_orderbook(self, symbol: str, depth: int = 20) -> bool:
        return True
    
    async def subscribe_candles(self, symbol: str, timeframe: str) -> bool:
        return True
    
    async def subscribe_user_data(self) -> bool:
        return True


class CCXTExchange(BaseExchange):
    """
    Generic CCXT-based exchange adapter.
    
    Works with ANY exchange supported by CCXT (100+ exchanges):
    - Binance, Binance Futures
    - Coinbase, Coinbase Pro
    - Kraken, Kraken Futures
    - Bybit, OKX, KuCoin
    - Bitfinex, Bitstamp, Gemini
    - And many more...
    
    Usage:
        config = ExchangeConfig(
            exchange_id="binance",  # or "kraken", "coinbase", etc.
            api_key="your_key",
            api_secret="your_secret",
        )
        exchange = CCXTExchange(config)
        await exchange.connect()
    """
    
    SUPPORTED_EXCHANGES = [
        'binance', 'binanceus', 'binanceusdm',  # Binance family
        'coinbase', 'coinbasepro', 'coinbaseadvanced',  # Coinbase family
        'kraken', 'krakenfutures',  # Kraken
        'bybit',  # Bybit
        'okx',  # OKX (formerly OKEx)
        'kucoin', 'kucoinfutures',  # KuCoin
        'gate', 'gateio',  # Gate.io
        'bitfinex', 'bitfinex2',  # Bitfinex
        'bitstamp',  # Bitstamp
        'gemini',  # Gemini
        'huobi', 'htx',  # Huobi/HTX
        'mexc',  # MEXC
        'bitget',  # Bitget
    ]
    
    def __init__(self, config: ExchangeConfig):
        super().__init__(config)
        self._client = None
        self._markets_loaded = False
    
    async def connect(self) -> bool:
        """Connect to exchange via CCXT."""
        try:
            import ccxt.async_support as ccxt
            
            exchange_id = self.config.exchange_id.lower()
            
            # Get exchange class
            if not hasattr(ccxt, exchange_id):
                logger.error(f"Exchange not supported: {exchange_id}")
                logger.info(f"Available exchanges: {ccxt.exchanges}")
                return False
            
            exchange_class = getattr(ccxt, exchange_id)
            
            # Build configuration
            exchange_config = {
                'enableRateLimit': True,
                'timeout': self.config.timeout_seconds * 1000,
            }
            
            if self.config.api_key:
                exchange_config['apiKey'] = self.config.api_key
            if self.config.api_secret:
                exchange_config['secret'] = self.config.api_secret
            if self.config.passphrase:
                exchange_config['password'] = self.config.passphrase
            
            # Exchange-specific options
            if self.config.extra:
                exchange_config['options'] = self.config.extra
            
            self._client = exchange_class(exchange_config)
            
            # Enable sandbox/testnet mode
            if self.config.testnet or self.config.sandbox:
                if hasattr(self._client, 'set_sandbox_mode'):
                    self._client.set_sandbox_mode(True)
                    logger.info(f"Enabled sandbox mode for {exchange_id}")
            
            # Load markets
            await self._client.load_markets()
            self._markets_loaded = True
            
            self.status = ExchangeStatus.CONNECTED
            logger.info(f"Connected to {exchange_id} - {len(self._client.markets)} markets available")
            
            return True
            
        except ImportError:
            logger.error("CCXT not installed. Run: pip install ccxt")
            self.status = ExchangeStatus.ERROR
            return False
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            self.status = ExchangeStatus.ERROR
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from exchange."""
        if self._client:
            await self._client.close()
            self._client = None
        self.status = ExchangeStatus.DISCONNECTED
        logger.info(f"Disconnected from {self.config.exchange_id}")
    
    async def ping(self) -> bool:
        """Check connection."""
        try:
            await self._client.fetch_time()
            return True
        except Exception:
            return False
    
    async def get_balance(self, currency: str = None) -> List[AccountBalance]:
        """Get account balances."""
        await self._rate_limit()
        
        try:
            balance = await self._client.fetch_balance()
            result = []
            
            for curr, data in balance.items():
                if curr in ['info', 'free', 'used', 'total', 'timestamp', 'datetime']:
                    continue
                if currency and curr != currency:
                    continue
                    
                total = data.get('total', 0) or 0
                if total > 0:
                    result.append(AccountBalance(
                        currency=curr,
                        total=total,
                        available=data.get('free', 0) or 0,
                        locked=data.get('used', 0) or 0,
                    ))
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            return []
    
    async def get_positions(self, symbol: str = None) -> List[Dict[str, Any]]:
        """Get open positions (futures/margin)."""
        await self._rate_limit()
        
        try:
            if hasattr(self._client, 'fetch_positions'):
                positions = await self._client.fetch_positions([symbol] if symbol else None)
                return [p for p in positions if float(p.get('contracts', 0)) != 0]
            return []
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
            return []
    
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get current ticker."""
        await self._rate_limit()
        
        try:
            ticker = await self._client.fetch_ticker(symbol)
            return {
                "symbol": symbol,
                "last": ticker.get('last'),
                "bid": ticker.get('bid'),
                "ask": ticker.get('ask'),
                "volume_24h": ticker.get('quoteVolume'),
                "change_24h_pct": ticker.get('percentage'),
                "high_24h": ticker.get('high'),
                "low_24h": ticker.get('low'),
                "timestamp": datetime.utcnow(),
            }
        except Exception as e:
            logger.error(f"Failed to get ticker: {e}")
            return {}
    
    async def get_orderbook(self, symbol: str, limit: int = 20) -> OrderBook:
        """Get order book."""
        await self._rate_limit()
        
        try:
            book = await self._client.fetch_order_book(symbol, limit)
            return OrderBook(
                symbol=symbol,
                bids=[(b[0], b[1]) for b in book.get('bids', [])],
                asks=[(a[0], a[1]) for a in book.get('asks', [])],
            )
        except Exception as e:
            logger.error(f"Failed to get order book: {e}")
            return OrderBook(symbol=symbol, bids=[], asks=[])
    
    async def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
        start_time: datetime = None,
        end_time: datetime = None,
    ) -> List[Candle]:
        """Get OHLCV candles."""
        await self._rate_limit()
        
        try:
            since = int(start_time.timestamp() * 1000) if start_time else None
            ohlcv = await self._client.fetch_ohlcv(symbol, timeframe, since, limit)
            
            candles = []
            for o in ohlcv:
                ts = datetime.utcfromtimestamp(o[0] / 1000)
                if end_time and ts > end_time:
                    continue
                candles.append(Candle(
                    symbol=symbol,
                    timestamp=ts,
                    open=float(o[1]),
                    high=float(o[2]),
                    low=float(o[3]),
                    close=float(o[4]),
                    volume=float(o[5]),
                    timeframe=timeframe,
                ))
            
            return candles
            
        except Exception as e:
            logger.error(f"Failed to get candles: {e}")
            return []
    
    async def get_market_info(self, symbol: str) -> Optional[MarketInfo]:
        """Get market/symbol information."""
        await self._rate_limit()
        
        try:
            if not self._markets_loaded:
                await self._client.load_markets()
                self._markets_loaded = True
            
            market = self._client.markets.get(symbol)
            if not market:
                return None
            
            limits = market.get('limits', {})
            precision = market.get('precision', {})
            
            return MarketInfo(
                symbol=symbol,
                base_currency=market.get('base', ''),
                quote_currency=market.get('quote', ''),
                price_precision=precision.get('price', 8),
                quantity_precision=precision.get('amount', 8),
                min_quantity=limits.get('amount', {}).get('min', 0) or 0,
                max_quantity=limits.get('amount', {}).get('max', float('inf')),
                min_notional=limits.get('cost', {}).get('min', 0) or 0,
                tick_size=precision.get('price', 0.00000001),
                lot_size=precision.get('amount', 0.00000001),
                is_spot=market.get('spot', False),
                is_futures=market.get('future', False) or market.get('swap', False),
            )
            
        except Exception as e:
            logger.error(f"Failed to get market info: {e}")
            return None
    
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
        """Place an order."""
        await self._rate_limit()
        
        try:
            params = {}
            if reduce_only:
                params['reduceOnly'] = True
            if post_only:
                params['postOnly'] = True
            if stop_price:
                params['stopPrice'] = stop_price
            if client_order_id:
                params['clientOrderId'] = client_order_id
            
            # Add any extra parameters
            params.update(kwargs)
            
            result = await self._client.create_order(
                symbol=symbol,
                type=order_type.value.lower(),
                side=side.value.lower(),
                amount=quantity,
                price=price,
                params=params,
            )
            
            return Order(
                order_id=str(result['id']),
                client_order_id=result.get('clientOrderId', client_order_id or ''),
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                stop_price=stop_price,
                status=self._map_order_status(result.get('status', 'open')),
                filled_quantity=float(result.get('filled', 0) or 0),
                average_fill_price=float(result.get('average', 0) or 0),
                reduce_only=reduce_only,
                created_at=datetime.utcnow(),
            )
            
        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        """Cancel an order."""
        await self._rate_limit()
        
        try:
            await self._client.cancel_order(order_id, symbol)
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order: {e}")
            return False
    
    async def cancel_all_orders(self, symbol: str = None) -> int:
        """Cancel all open orders."""
        await self._rate_limit()
        
        try:
            if hasattr(self._client, 'cancel_all_orders'):
                result = await self._client.cancel_all_orders(symbol)
                return len(result) if isinstance(result, list) else 1
            else:
                # Fallback: cancel orders one by one
                orders = await self.get_open_orders(symbol)
                count = 0
                for order in orders:
                    if await self.cancel_order(order.order_id, order.symbol):
                        count += 1
                return count
        except Exception as e:
            logger.error(f"Failed to cancel all orders: {e}")
            return 0
    
    async def get_order(self, order_id: str, symbol: str = None) -> Optional[Order]:
        """Get order by ID."""
        await self._rate_limit()
        
        try:
            result = await self._client.fetch_order(order_id, symbol)
            return self._parse_order(result)
        except Exception as e:
            logger.error(f"Failed to get order: {e}")
            return None
    
    async def get_open_orders(self, symbol: str = None) -> List[Order]:
        """Get all open orders."""
        await self._rate_limit()
        
        try:
            orders = await self._client.fetch_open_orders(symbol)
            return [self._parse_order(o) for o in orders]
        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            return []
    
    async def get_order_history(
        self,
        symbol: str = None,
        limit: int = 100,
        start_time: datetime = None,
    ) -> List[Order]:
        """Get closed orders history."""
        await self._rate_limit()
        
        try:
            since = int(start_time.timestamp() * 1000) if start_time else None
            orders = await self._client.fetch_closed_orders(symbol, since, limit)
            return [self._parse_order(o) for o in orders]
        except Exception as e:
            logger.error(f"Failed to get order history: {e}")
            return []
    
    async def subscribe_ticker(self, symbol: str) -> bool:
        """Subscribe to ticker updates (placeholder for WebSocket)."""
        logger.info(f"Ticker subscription: {symbol}")
        return True
    
    async def subscribe_orderbook(self, symbol: str, depth: int = 20) -> bool:
        """Subscribe to order book updates."""
        logger.info(f"Order book subscription: {symbol}")
        return True
    
    async def subscribe_candles(self, symbol: str, timeframe: str) -> bool:
        """Subscribe to candle updates."""
        logger.info(f"Candle subscription: {symbol} {timeframe}")
        return True
    
    async def subscribe_user_data(self) -> bool:
        """Subscribe to user data (orders, fills, positions)."""
        logger.info("User data subscription")
        return True
    
    def _parse_order(self, data: Dict) -> Order:
        """Parse exchange order data to Order object."""
        side_str = data.get('side', 'buy').lower()
        type_str = data.get('type', 'limit').upper()
        
        return Order(
            order_id=str(data['id']),
            client_order_id=data.get('clientOrderId', ''),
            symbol=data['symbol'],
            side=Side.BUY if side_str == 'buy' else Side.SELL,
            order_type=OrderType(type_str) if type_str in [e.value for e in OrderType] else OrderType.LIMIT,
            quantity=float(data.get('amount', 0) or 0),
            price=float(data.get('price', 0) or 0) if data.get('price') else None,
            status=self._map_order_status(data.get('status', 'open')),
            filled_quantity=float(data.get('filled', 0) or 0),
            average_fill_price=float(data.get('average', 0) or 0),
            created_at=datetime.utcnow(),
        )
    
    def _map_order_status(self, status: str) -> OrderStatus:
        """Map exchange status to OrderStatus enum."""
        status_map = {
            'open': OrderStatus.NEW,
            'new': OrderStatus.NEW,
            'partially_filled': OrderStatus.PARTIALLY_FILLED,
            'closed': OrderStatus.FILLED,
            'filled': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELLED,
            'cancelled': OrderStatus.CANCELLED,
            'expired': OrderStatus.EXPIRED,
            'rejected': OrderStatus.REJECTED,
        }
        return status_map.get(status.lower(), OrderStatus.NEW)
    
    def get_supported_symbols(self) -> List[str]:
        """Get list of available trading pairs."""
        if not self._client:
            return []
        return list(self._client.markets.keys())
    
    def get_supported_timeframes(self) -> List[str]:
        """Get list of available timeframes."""
        if not self._client or not hasattr(self._client, 'timeframes'):
            return []
        return list(self._client.timeframes.keys())
