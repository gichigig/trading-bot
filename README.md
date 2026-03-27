# Advanced Trading Bot Framework

A professional-grade, modular trading bot in Python with enterprise-level features for algorithmic trading.

## 🚀 Features

### Strategy System
- **Pluggable Architecture**: Swap strategies without code changes
- **Configurable Parameters**: Tweak parameters via config files
- **Strategy Versioning**: Track which logic version made/lost money
- **Built-in Examples**: Momentum, Mean Reversion, Breakout strategies

### Multi-Timeframe Analysis
- Higher timeframe bias (e.g., 4H trend direction)
- Lower timeframe execution (e.g., 15m entries)
- Time alignment validation

### Regime Detection
- Volatility filters (ATR, Historical Volatility)
- Trend vs Range detection
- Session awareness (Asia/London/NY)
- News blackout windows

### Risk Management (The Heart)
- **Dynamic Position Sizing**: Volatility-adjusted, correlation-aware
- **Circuit Breakers**: Daily loss limits, consecutive loss limits, drawdown kill-switch
- **Trade Lifecycle**: Partial take profits, trailing stops, break-even logic

### Execution Layer
- Smart order execution with retry logic
- Limit vs market order logic
- Post-only order support
- Slippage tolerance thresholds
- Partial fill handling

### Exchange Abstraction
- **100+ Exchanges Supported**: via CCXT library
- Unified interface for multiple exchanges
- Exchange-specific quirk isolation
- Failover support

### Supported Exchanges
The bot uses [CCXT](https://github.com/ccxt/ccxt) for exchange connectivity:
- **Major**: Binance, Coinbase, Kraken, Bybit, OKX
- **Others**: KuCoin, Gate.io, Bitfinex, Bitstamp, Gemini, Huobi, MEXC, Bitget, and 100+ more

### Resilience & Safety
- **State Persistence**: Survives restarts
- **Fault Tolerance**: Handles API timeouts, websocket drops, data gaps
- **Security**: Encrypted API keys, no keys in logs

### Observability
- Structured logging with decision context
- Comprehensive metrics (Sharpe, Sortino, expectancy, drawdown)
- Performance analytics

### Backtesting
- Realistic fill simulation
- Slippage and commission modeling
- Paper trading with live data parity

## 📁 Project Structure

```
trading_bot/
├── __init__.py           # Package exports
├── bot.py                # Main orchestrator
├── core/
│   ├── config.py         # Configuration management
│   ├── events.py         # Event bus system
│   └── types.py          # Core data types
├── strategies/
│   ├── base.py           # Base strategy class
│   ├── registry.py       # Strategy registry
│   └── examples/         # Example strategies
├── data/
│   ├── feeds.py          # Data feed handlers
│   ├── manager.py        # Data management
│   └── timeframe.py      # Timeframe utilities
├── exchanges/
│   ├── base.py           # Exchange abstraction
│   ├── adapters.py       # Exchange implementations
│   └── factory.py        # Exchange factory
├── execution/
│   ├── manager.py        # Execution coordinator
│   ├── orders.py         # Order management
│   └── lifecycle.py      # Trade lifecycle
├── risk/
│   ├── engine.py         # Risk engine
│   ├── position_sizer.py # Position sizing
│   └── circuit_breaker.py# Circuit breakers
├── regime/
│   ├── detector.py       # Regime detection
│   ├── session.py        # Session management
│   └── news.py           # News handling
├── persistence/
│   ├── store.py          # State storage
│   ├── state.py          # State management
│   └── snapshots.py      # Snapshot management
├── observability/
│   ├── logging.py        # Structured logging
│   ├── metrics.py        # Metrics collection
│   └── analytics.py      # Performance analysis
├── alerts/
│   ├── notifier.py       # Alert management
│   └── channels.py       # Notification channels
└── backtesting/
    ├── engine.py         # Backtest engine
    ├── paper.py          # Paper trading
    └── simulator.py      # Market simulation
```

## 🛠️ Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/trading-bot.git
cd trading-bot

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

## 📋 Requirements

```txt
# requirements.txt
pyyaml>=6.0
cryptography>=41.0
ccxt>=4.0           # Exchange connectivity (100+ exchanges)
aiohttp>=3.9
requests>=2.31
websockets>=12.0
numpy>=1.24
pandas>=2.0
```

## 🔗 Exchange Setup

### Public Data (No API Keys)

You can fetch market data without API keys:

```python
from trading_bot.data.feeds import CCXTLiveDataFeed

feed = CCXTLiveDataFeed("binance")  # or "kraken", "coinbase", "bybit"
await feed.connect()

# Get current price
ticker = await feed.get_ticker("BTC/USDT")
print(f"BTC: ${ticker.last}")

# Get historical candles
candles = await feed.fetch_historical("ETH/USDT", "1h", limit=100)
```

### Live Trading (API Keys Required)

1. **Create API Keys** on your exchange:
   - Go to your exchange's API management
   - Create new API key with **Spot Trading** permission
   - ⚠️ **DISABLE Withdrawals** for security

2. **Configure** in `config.yaml`:
```yaml
exchange:
  id: "binance"  # or kraken, coinbase, bybit, okx, etc.
  api_key: "your_key"
  api_secret: "your_secret"
  testnet: true  # Use testnet first!
```

3. **Run with paper trading first**:
```bash
python run.py paper
```

### Testnet Resources
- **Binance**: https://testnet.binance.vision
- **Bybit**: https://testnet.bybit.com
- **OKX**: https://www.okx.com/docs-v5/en/#overview-demo-trading-services

## ⚙️ Configuration

Copy the example config and customize:

```bash
cp config.example.yaml config.yaml
```

Key configuration sections:

```yaml
# Trading mode
mode: "paper"  # backtest, paper, live

# Strategies
strategies:
  - name: "momentum"
    enabled: true
    parameters:
      fast_ema: 12
      slow_ema: 26

# Risk management
risk:
  risk_per_trade_pct: 1.0
  daily_max_loss_pct: 5.0
  max_consecutive_losses: 5
  panic_stop_drawdown_pct: 15.0
```

## 🚦 Quick Start

### 1. Create a Strategy

```python
from trading_bot.strategies.base import BaseStrategy, StrategyMetadata
from trading_bot.strategies.registry import register_strategy
from trading_bot.core.types import Signal, SignalType, Side

@register_strategy
class MyStrategy(BaseStrategy):
    
    def _define_metadata(self) -> StrategyMetadata:
        return StrategyMetadata(
            name="my_strategy",
            version="1.0.0",
            warmup_periods=100,
        )
    
    def _get_default_parameters(self):
        return {"period": 14}
    
    def generate_signal(self, context):
        # Your strategy logic here
        if should_buy:
            return Signal(
                symbol=context.symbol,
                signal_type=SignalType.ENTRY_LONG,
                side=Side.BUY,
                price=context.current_candle.close,
            )
        return None
```

### 2. Run Backtest

```python
from trading_bot.backtesting import BacktestEngine, BacktestConfig
from trading_bot.strategies.examples.momentum import MomentumStrategy
from datetime import datetime

config = BacktestConfig(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31),
    initial_capital=100000,
)

engine = BacktestEngine(config)
strategy = MomentumStrategy()

# Load your historical data
data = {"BTC/USDT": candles}

result = engine.run(strategy, data)
result.print_summary()
```

### 3. Run Paper Trading

```python
from trading_bot import TradingBot, load_config

config = load_config("config.yaml")
bot = TradingBot(config)

await bot.initialize()
await bot.start()
```

## 📊 Metrics Tracked

| Metric | Description |
|--------|-------------|
| Win Rate | Percentage of winning trades |
| Expectancy | Expected value per trade |
| Profit Factor | Gross profit / Gross loss |
| Max Drawdown | Maximum peak-to-trough decline |
| Sharpe Ratio | Risk-adjusted return |
| Sortino Ratio | Downside risk-adjusted return |
| Average Trade Duration | Mean time in position |
| Slippage Analysis | Actual vs expected slippage |

## 🔒 Security Best Practices

1. **Never commit API keys** - Use environment variables or encrypted config
2. **Use read-only keys when possible** - Separate keys for trading
3. **Enable IP whitelisting** - Restrict API access by IP
4. **Encrypt sensitive data** - Use the built-in encryption helpers

```python
# Generate encryption key
from cryptography.fernet import Fernet
key = Fernet.generate_key()
print(key.decode())  # Save this securely

# Set in environment
# TRADING_BOT_ENCRYPTION_KEY=your_key_here
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=trading_bot tests/

# Run specific test
pytest tests/test_strategies.py -v
```

## 📈 Example Strategies

### Momentum Strategy
Trend-following using EMA crossovers and RSI confirmation.

### Mean Reversion Strategy
Fade extreme moves using Bollinger Bands and RSI.

### Breakout Strategy
Trade breakouts from consolidation ranges.

## ⚠️ Disclaimer

**This software is for educational purposes only. Trading carries significant financial risk.**

- Past performance does not guarantee future results
- Always test thoroughly in paper mode before live trading
- Never risk more than you can afford to lose
- The authors are not responsible for any financial losses

## 🤝 Contributing

Contributions are welcome! Please read our contributing guidelines first.

## 📄 License

MIT License - see LICENSE file for details.

## 🙏 Acknowledgments

Built with best practices from professional trading systems and quantitative finance.
