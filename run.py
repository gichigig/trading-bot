"""
Trading Bot Runner
===================

Entry point for running the trading bot in different modes.
"""

import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))


def run_backtest():
    """Run backtesting mode."""
    from trading_bot.backtesting.engine import BacktestEngine, BacktestConfig
    from trading_bot.strategies.examples.momentum import MomentumStrategy
    from trading_bot.core.types import Candle
    
    print("=" * 60)
    print("BACKTEST MODE")
    print("=" * 60)
    
    # Configure backtest
    config = BacktestConfig(
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 12, 31),
        initial_capital=100000.0,
        commission_pct=0.1,
        slippage_pct=0.05,
    )
    
    # Create strategy
    strategy = MomentumStrategy({
        "fast_ema_period": 12,
        "slow_ema_period": 26,
        "rsi_period": 14,
    })
    
    # Generate sample data (replace with real data loading)
    print("\nGenerating sample data...")
    data = generate_sample_data("BTC/USDT", config.start_date, config.end_date)
    
    # Run backtest
    print("Running backtest...")
    engine = BacktestEngine(config)
    result = engine.run(strategy, {"BTC/USDT": data})
    
    # Print results
    result.print_summary()
    
    return result


def run_paper():
    """Run paper trading mode with REAL market data from Binance."""
    from trading_bot.backtesting.paper import PaperTradingEngine
    from trading_bot.strategies.examples.momentum import MomentumStrategy
    from trading_bot.core.types import Candle, Side
    from trading_bot.strategies.base import StrategyContext
    from trading_bot.data.feeds import CCXTLiveDataFeed
    from datetime import timezone
    
    print("=" * 60)
    print("PAPER TRADING MODE (Live Market Data)")
    print("=" * 60)
    
    async def paper_trading_loop():
        # Initialize paper trading engine
        engine = PaperTradingEngine(
            initial_balance=100000.0,
            commission_pct=0.1,
            slippage_pct=0.05,
        )
        
        # Initialize strategy
        strategy = MomentumStrategy({
            "fast_ema_period": 12,
            "slow_ema_period": 26,
        })
        strategy.start()
        
        await engine.start()
        
        # Connect to Binance for real market data
        print("\nConnecting to Binance for live market data...")
        feed = CCXTLiveDataFeed(
            exchange_id="binance",
            # No API key needed for public price data
        )
        
        connected = await feed.connect()
        if not connected:
            print("❌ Failed to connect to Binance. Using simulated prices...")
            use_live_data = False
        else:
            print("✓ Connected to Binance!")
            use_live_data = True
            
            # Fetch initial historical data for strategy warmup
            print("Fetching historical data for strategy warmup...")
            historical = await feed.fetch_historical(
                symbol="BTC/USDT",
                timeframe="1h",
                limit=100
            )
            print(f"✓ Loaded {len(historical)} historical candles")
        
        print(f"\nStarting balance: ${engine.get_balance():,.2f}")
        print("Trading pair: BTC/USDT")
        print("Press Ctrl+C to stop\n")
        
        candle_history = []
        
        # Pre-populate with historical data if available
        if use_live_data and historical:
            candle_history = historical.copy()
        
        last_price = 0.0
        
        try:
            while True:
                if use_live_data:
                    # Fetch REAL price from Binance
                    try:
                        ticker = await feed.get_ticker("BTC/USDT")
                        if ticker:
                            price = ticker.last
                        else:
                            await asyncio.sleep(1)
                            continue
                    except Exception as e:
                        print(f"\n⚠️ Error fetching price: {e}")
                        await asyncio.sleep(5)
                        continue
                else:
                    # Fallback to simulated prices
                    import random
                    if last_price == 0:
                        last_price = 50000.0
                    price = last_price * (1 + random.gauss(0, 0.001))
                
                last_price = price
                
                # Create candle from current price
                now = datetime.now(timezone.utc)
                candle = Candle(
                    symbol="BTC/USDT",
                    timeframe="1h",
                    timestamp=now,
                    open=price * 0.9999,
                    high=price * 1.0001,
                    low=price * 0.9998,
                    close=price,
                    volume=0,  # Not tracking volume in paper mode
                )
                
                candle_history.append(candle)
                if len(candle_history) > 200:
                    candle_history = candle_history[-200:]
                
                # Update engine with price
                engine.update_price("BTC/USDT", price)
                
                # Generate signal if enough data
                if len(candle_history) >= 50:
                    context = StrategyContext(
                        timestamp=now,
                        symbol="BTC/USDT",
                        timeframe="1h",
                        candles={"1h": candle_history},
                        current_candle=candle,
                        account_balance=engine.get_balance(),
                        has_position="BTC/USDT" in engine.get_positions(),
                    )
                    
                    signal = strategy.generate_signal(context)
                    
                    if signal:
                        print(f"\n📊 Signal: {signal.signal_type.value} @ ${price:,.2f}")
                        
                        # Execute signal
                        if signal.signal_type.value.startswith("entry"):
                            quantity = 0.01  # Fixed quantity for demo
                            # Determine side from signal type
                            if "long" in signal.signal_type.value:
                                side = Side.BUY
                                print(f"   🟢 BUY {quantity} BTC @ ${price:,.2f}")
                            else:
                                side = Side.SELL
                                print(f"   🔴 SELL {quantity} BTC @ ${price:,.2f}")
                            
                            await engine.submit_order(
                                symbol="BTC/USDT",
                                side=side,
                                quantity=quantity,
                            )
                
                # Print status
                stats = engine.get_statistics()
                data_source = "LIVE" if use_live_data else "SIM"
                print(f"\r[{data_source}] BTC: ${price:,.2f} | Equity: ${stats['equity']:,.2f} | "
                      f"Positions: {stats['open_positions']} | Trades: {stats['total_trades']}",
                      end="", flush=True)
                
                await asyncio.sleep(2)  # Poll every 2 seconds
                
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping paper trading...")
            
            if use_live_data:
                await feed.disconnect()
            
            await engine.stop()
            
            print("\n" + "=" * 40)
            print("FINAL STATISTICS")
            print("=" * 40)
            stats = engine.get_statistics()
            for key, value in stats.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:,.2f}")
                else:
                    print(f"  {key}: {value}")
    
    asyncio.run(paper_trading_loop())


def run_live():
    """Run live trading mode with real exchange execution."""
    from trading_bot.strategies.examples.momentum import MomentumStrategy
    from trading_bot.core.types import Candle, Side
    from trading_bot.strategies.base import StrategyContext
    from datetime import timezone
    import ccxt
    import yaml
    
    print("=" * 60)
    print("🔴 LIVE TRADING MODE")
    print("=" * 60)
    
    # Load config directly from YAML
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    exchange_config = config.get("exchange", {})
    
    is_testnet = exchange_config.get("testnet", False)
    
    if is_testnet:
        print("\n✅ TESTNET MODE - Using fake money for testing")
    else:
        print("\n⚠️  WARNING: REAL TRADING MODE - Using real money!")
        print("    Press Ctrl+C within 5 seconds to cancel...")
        try:
            import time
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n❌ Cancelled.")
            return
    
    async def live_trading_loop():
        exchange_id = exchange_config.get("id", "binance")
        api_key = exchange_config.get("api_key", "")
        api_secret = exchange_config.get("api_secret", "")
        
        if not api_key or not api_secret:
            print("❌ Error: API key and secret are required for live trading.")
            print("   Configure them in config.yaml")
            return
        
        # Initialize CCXT exchange
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
        })
        
        # Enable testnet if configured
        if is_testnet:
            exchange.set_sandbox_mode(True)
            print(f"\n🔗 Connecting to {exchange_id.upper()} TESTNET...")
        else:
            print(f"\n🔗 Connecting to {exchange_id.upper()}...")
        
        try:
            # Test connection and fetch balance
            balance = await asyncio.get_event_loop().run_in_executor(
                None, exchange.fetch_balance
            )
            
            usdt_balance = balance.get('USDT', {}).get('free', 0) or balance.get('total', {}).get('USDT', 0)
            btc_balance = balance.get('BTC', {}).get('free', 0) or balance.get('total', {}).get('BTC', 0)
            
            print(f"✅ Connected!")
            print(f"\n💰 Account Balance:")
            print(f"   USDT: {usdt_balance:,.2f}")
            print(f"   BTC:  {btc_balance:.8f}")
            
        except Exception as e:
            print(f"❌ Failed to connect: {e}")
            return
        
        # Initialize strategy
        strategy = MomentumStrategy({
            "fast_ema_period": 12,
            "slow_ema_period": 26,
        })
        strategy.start()
        
        # Create separate exchange for price data (mainnet, public API)
        print("\n📊 Setting up market data feed...")
        price_exchange = ccxt.binance({'enableRateLimit': True})
        
        # Fetch historical data for strategy warmup
        print("Fetching historical candles...")
        try:
            ohlcv = await asyncio.get_event_loop().run_in_executor(
                None, lambda: price_exchange.fetch_ohlcv("BTC/USDT", "1h", limit=100)
            )
            historical = []
            for row in ohlcv:
                historical.append(Candle(
                    symbol="BTC/USDT",
                    timeframe="1h",
                    timestamp=datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc),
                    open=row[1],
                    high=row[2],
                    low=row[3],
                    close=row[4],
                    volume=row[5],
                ))
            print(f"✅ Loaded {len(historical)} historical candles")
        except Exception as e:
            print(f"⚠️ Could not load historical data: {e}")
            historical = []
        
        candle_history = historical.copy() if historical else []
        
        print("\n" + "=" * 50)
        print("🚀 LIVE TRADING STARTED")
        print("=" * 50)
        print("Trading pair: BTC/USDT")
        print("Press Ctrl+C to stop\n")
        
        trade_count = 0
        
        try:
            while True:
                # Fetch REAL price from Binance mainnet
                try:
                    ticker = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: price_exchange.fetch_ticker("BTC/USDT")
                    )
                    price = ticker['last']
                except Exception as e:
                    print(f"\n⚠️ Error fetching price: {e}")
                    await asyncio.sleep(5)
                    continue
                
                # Create candle from current price
                now = datetime.now(timezone.utc)
                candle = Candle(
                    symbol="BTC/USDT",
                    timeframe="1h",
                    timestamp=now,
                    open=price * 0.9999,
                    high=price * 1.0001,
                    low=price * 0.9998,
                    close=price,
                    volume=0,
                )
                
                candle_history.append(candle)
                if len(candle_history) > 200:
                    candle_history = candle_history[-200:]
                
                # Generate signal if enough data
                if len(candle_history) >= 50:
                    # Refresh balance
                    try:
                        balance = await asyncio.get_event_loop().run_in_executor(
                            None, exchange.fetch_balance
                        )
                        usdt_balance = balance.get('USDT', {}).get('free', 0) or 0
                    except:
                        pass
                    
                    context = StrategyContext(
                        timestamp=now,
                        symbol="BTC/USDT",
                        timeframe="1h",
                        candles={"1h": candle_history},
                        current_candle=candle,
                        account_balance=usdt_balance,
                        has_position=btc_balance > 0.0001,
                    )
                    
                    signal = strategy.generate_signal(context)
                    
                    if signal and signal.signal_type.value.startswith("entry"):
                        quantity = 0.001  # Small quantity for safety (0.001 BTC)
                        
                        if "long" in signal.signal_type.value:
                            side = "buy"
                            emoji = "🟢"
                        else:
                            side = "sell"
                            emoji = "🔴"
                        
                        print(f"\n\n{emoji} SIGNAL: {side.upper()} {quantity} BTC @ ${price:,.2f}")
                        
                        # Execute REAL order on testnet
                        try:
                            order = await asyncio.get_event_loop().run_in_executor(
                                None,
                                lambda: exchange.create_market_order(
                                    symbol="BTC/USDT",
                                    side=side,
                                    amount=quantity
                                )
                            )
                            trade_count += 1
                            print(f"   ✅ Order executed! ID: {order.get('id', 'N/A')}")
                            print(f"   Filled: {order.get('filled', quantity)} @ ${order.get('average', price):,.2f}")
                        except Exception as e:
                            print(f"   ❌ Order failed: {e}")
                
                # Status line
                mode_label = "TESTNET" if is_testnet else "LIVE"
                print(f"\r[{mode_label}] BTC: ${price:,.2f} | USDT: ${usdt_balance:,.2f} | Trades: {trade_count}",
                      end="", flush=True)
                
                await asyncio.sleep(5)  # Poll every 5 seconds
                
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping live trading...")
            
            # Final balance
            try:
                balance = await asyncio.get_event_loop().run_in_executor(
                    None, exchange.fetch_balance
                )
                print("\n" + "=" * 40)
                print("FINAL BALANCE")
                print("=" * 40)
                print(f"  USDT: {balance.get('USDT', {}).get('free', 0):,.2f}")
                print(f"  BTC:  {balance.get('BTC', {}).get('free', 0):.8f}")
                print(f"  Total Trades: {trade_count}")
            except:
                pass
    
    asyncio.run(live_trading_loop())


def generate_sample_data(symbol: str, start: datetime, end: datetime):
    """Generate sample OHLCV data for testing."""
    from trading_bot.core.types import Candle
    import random
    
    candles = []
    price = 50000.0  # Starting price
    current = start
    
    while current <= end:
        # Skip weekends for demo
        if current.weekday() < 5:
            # Random walk with slight upward bias
            change = random.gauss(0.0001, 0.02)
            price *= (1 + change)
            
            # Generate OHLC
            open_price = price * random.uniform(0.998, 1.002)
            close_price = price
            high_price = max(open_price, close_price) * random.uniform(1.001, 1.01)
            low_price = min(open_price, close_price) * random.uniform(0.99, 0.999)
            
            candles.append(Candle(
                symbol=symbol,
                timeframe="1h",
                timestamp=current,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=random.uniform(100, 10000),
            ))
        
        current += timedelta(hours=1)
    
    return candles


def main():
    parser = argparse.ArgumentParser(
        description="Trading Bot Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py backtest          Run backtesting with sample data
  python run.py paper             Run paper trading simulation
  python run.py live              Run live trading (requires config)
  python run.py --config my.yaml  Use custom config file
        """
    )
    
    parser.add_argument(
        "mode",
        choices=["backtest", "paper", "live"],
        nargs="?",
        default="backtest",
        help="Trading mode (default: backtest)"
    )
    
    parser.add_argument(
        "--config", "-c",
        default="config.yaml",
        help="Path to configuration file"
    )
    
    parser.add_argument(
        "--strategy", "-s",
        default="momentum",
        help="Strategy to use (default: momentum)"
    )
    
    args = parser.parse_args()
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║           ADVANCED TRADING BOT FRAMEWORK v1.0.0              ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    if args.mode == "backtest":
        run_backtest()
    elif args.mode == "paper":
        run_paper()
    elif args.mode == "live":
        run_live()


if __name__ == "__main__":
    main()
