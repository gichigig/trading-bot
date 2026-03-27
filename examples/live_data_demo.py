"""
Live Market Data Demo
=====================

This script demonstrates how to connect to real exchanges
and fetch live market data using the CCXT integration.

No API keys required for public data (prices, candles, order book).
"""

import asyncio
from datetime import datetime, timedelta

# Add parent directory to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from trading_bot.data.feeds import CCXTLiveDataFeed
from trading_bot.exchanges.adapters import CCXTExchange
from trading_bot.exchanges.base import ExchangeConfig


async def demo_public_data():
    """
    Demonstrate fetching PUBLIC market data (no API keys needed).
    
    This works with any exchange supported by CCXT.
    """
    print("=" * 60)
    print("LIVE MARKET DATA DEMO (Public Data - No API Keys)")
    print("=" * 60)
    
    # Create data feed for Binance (works without API keys for public data)
    feed = CCXTLiveDataFeed(
        exchange_id="binance",  # Try: "kraken", "coinbase", "bybit", "okx"
        # No API key needed for public data
    )
    
    # Connect
    print("\nConnecting to Binance...")
    try:
        connected = await feed.connect()
    except Exception as e:
        print(f"Connection error: {e}")
        print("\n⚠️  Network issue detected. Possible causes:")
        print("  - Firewall blocking connections")
        print("  - VPN required (Binance blocked in some countries)")
        print("  - Proxy settings needed")
        print("\nTry a different exchange:")
        print("  feed = CCXTLiveDataFeed('kraken')  # or 'coinbase', 'bybit'")
        return
    
    if not connected:
        print("Failed to connect.")
        print("Check your internet connection and try again.")
        return
    
    print(f"✓ Connected!")
    print(f"  Available symbols: {len(feed.get_available_symbols())}")
    print(f"  Available timeframes: {feed.get_available_timeframes()}")
    
    # Show some popular symbols
    symbols = feed.get_available_symbols()
    btc_pairs = [s for s in symbols if 'BTC' in s][:5]
    print(f"  BTC pairs: {btc_pairs}")
    
    # Fetch current ticker
    print("\n--- Current Prices ---")
    for symbol in ["BTC/USDT", "ETH/USDT", "SOL/USDT"]:
        try:
            ticker = await feed.get_ticker(symbol)
            if ticker:
                print(f"  {symbol}: ${ticker.last:,.2f} (bid: ${ticker.bid:,.2f}, ask: ${ticker.ask:,.2f})")
        except Exception as e:
            print(f"  {symbol}: Error - {e}")
    
    # Fetch recent candles
    print("\n--- Recent BTC/USDT Candles (1h) ---")
    try:
        candles = await feed.fetch_historical(
            symbol="BTC/USDT",
            timeframe="1h",
            limit=5
        )
        
        for candle in candles[-5:]:
            print(f"  {candle.timestamp}: O={candle.open:,.2f} H={candle.high:,.2f} "
                  f"L={candle.low:,.2f} C={candle.close:,.2f} V={candle.volume:,.0f}")
    except Exception as e:
        print(f"  Error fetching candles: {e}")
    
    # Fetch historical data
    print("\n--- Historical Data (Last 7 days) ---")
    try:
        week_ago = datetime.utcnow() - timedelta(days=7)
        historical = await feed.fetch_historical(
            symbol="ETH/USDT",
            timeframe="4h",
            since=week_ago,
            limit=42  # ~7 days of 4h candles
        )
        print(f"  Fetched {len(historical)} candles for ETH/USDT (4h)")
        if historical:
            first, last = historical[0], historical[-1]
            change = ((last.close - first.close) / first.close) * 100
            print(f"  Price change: ${first.close:,.2f} -> ${last.close:,.2f} ({change:+.2f}%)")
    except Exception as e:
        print(f"  Error: {e}")
    
    # Disconnect
    await feed.disconnect()
    print("\n✓ Disconnected")


async def demo_live_streaming():
    """
    Demonstrate live streaming of candle data.
    """
    print("\n" + "=" * 60)
    print("LIVE STREAMING DEMO")
    print("=" * 60)
    
    feed = CCXTLiveDataFeed("binance")
    await feed.connect()
    
    # Define callback for new candles
    def on_candle(candle):
        print(f"  NEW: {candle.symbol} {candle.timeframe} - "
              f"Close: ${candle.close:,.2f} at {candle.timestamp}")
    
    feed.set_candle_callback(on_candle)
    
    # Subscribe to symbols
    await feed.subscribe("BTC/USDT", "1m")
    await feed.subscribe("ETH/USDT", "1m")
    
    print("\nStreaming live candles (15 seconds)...")
    print("  Watching BTC/USDT and ETH/USDT (1m candles)")
    
    # Start streaming
    await feed.start_streaming()
    
    # Run for 15 seconds
    await asyncio.sleep(15)
    
    # Stop
    await feed.stop_streaming()
    await feed.disconnect()
    print("\n✓ Streaming stopped")


async def demo_exchange_adapter():
    """
    Demonstrate using the CCXTExchange adapter for trading operations.
    
    NOTE: This only shows public endpoints. 
    For actual trading, you need valid API keys.
    """
    print("\n" + "=" * 60)
    print("EXCHANGE ADAPTER DEMO")
    print("=" * 60)
    
    # Create config (no API keys = public data only)
    config = ExchangeConfig(
        exchange_id="kraken",  # Try different exchanges
    )
    
    exchange = CCXTExchange(config)
    
    print("\nConnecting to Kraken...")
    if not await exchange.connect():
        print("Failed to connect")
        return
    
    print(f"✓ Connected to {config.exchange_id}")
    
    # Get market info
    print("\n--- Market Info ---")
    for symbol in ["BTC/USD", "ETH/USD"]:
        info = await exchange.get_market_info(symbol)
        if info:
            print(f"  {symbol}:")
            print(f"    Min quantity: {info.min_quantity}")
            print(f"    Price precision: {info.price_precision}")
    
    # Get order book
    print("\n--- Order Book (BTC/USD top 5) ---")
    book = await exchange.get_orderbook("BTC/USD", limit=5)
    print("  Bids:")
    for price, qty in book.bids[:3]:
        print(f"    ${price:,.2f} x {qty:.4f}")
    print("  Asks:")
    for price, qty in book.asks[:3]:
        print(f"    ${price:,.2f} x {qty:.4f}")
    
    if book.spread:
        print(f"  Spread: ${book.spread:.2f} ({book.spread_pct:.4f}%)")
    
    # Get candles
    print("\n--- Recent Candles ---")
    candles = await exchange.get_candles("ETH/USD", "1h", limit=3)
    for c in candles:
        print(f"  {c.timestamp}: Close=${c.close:,.2f}")
    
    await exchange.disconnect()
    print("\n✓ Disconnected")


async def list_supported_exchanges():
    """List some of the supported exchanges."""
    print("\n" + "=" * 60)
    print("SUPPORTED EXCHANGES")
    print("=" * 60)
    
    try:
        import ccxt
        
        popular = [
            'binance', 'binanceus', 'coinbase', 'kraken', 'bybit',
            'okx', 'kucoin', 'gate', 'bitfinex', 'bitstamp',
            'gemini', 'huobi', 'mexc', 'bitget', 'cryptocom',
        ]
        
        print(f"\nTotal exchanges supported by CCXT: {len(ccxt.exchanges)}")
        print(f"\nPopular exchanges:")
        for ex in popular:
            if ex in ccxt.exchanges:
                print(f"  ✓ {ex}")
            else:
                print(f"  ✗ {ex} (not available)")
        
        print(f"\nAll available: {ccxt.exchanges[:20]}...")
        
    except ImportError:
        print("CCXT not installed. Run: pip install ccxt")


async def main():
    """Run all demos."""
    
    # Check if ccxt is installed
    try:
        import ccxt
        print(f"CCXT version: {ccxt.__version__}")
    except ImportError:
        print("ERROR: CCXT not installed!")
        print("Run: pip install ccxt")
        return
    
    await list_supported_exchanges()
    await demo_public_data()
    await demo_exchange_adapter()
    
    # Uncomment to see live streaming:
    # await demo_live_streaming()
    
    print("\n" + "=" * 60)
    print("DEMO COMPLETE")
    print("=" * 60)
    print("""
Next steps to trade with real money:

1. Create API keys on your exchange (Binance, Kraken, etc.)
   - Enable "Spot Trading" permission
   - For safety, disable "Withdrawals" permission
   
2. Create config.yaml:
   exchange:
     id: binance
     api_key: "your_key_here"
     api_secret: "your_secret_here"
     testnet: true  # Start with testnet!

3. Run paper trading first:
   python run.py paper --config config.yaml

4. When ready for live:
   python run.py live --config config.yaml

⚠️  WARNING: Only trade with money you can afford to lose!
""")


if __name__ == "__main__":
    asyncio.run(main())
