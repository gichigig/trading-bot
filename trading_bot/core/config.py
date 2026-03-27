"""
Configuration Management System
================================

Handles all bot configuration with:
- YAML/JSON config file support
- Environment variable overrides
- Feature flags
- Strategy toggles
- Validation and defaults
"""

import os
import json
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum
import logging
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


class TradingMode(Enum):
    """Trading mode enumeration."""
    BACKTEST = "backtest"
    PAPER = "paper"
    LIVE = "live"


class LogLevel(Enum):
    """Log level enumeration."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class ExchangeConfig:
    """Exchange connection configuration."""
    name: str
    api_key_encrypted: str = ""
    api_secret_encrypted: str = ""
    passphrase_encrypted: str = ""
    testnet: bool = True
    rate_limit: int = 100
    timeout: int = 30
    max_retries: int = 3
    ip_whitelist: List[str] = field(default_factory=list)
    
    def get_api_key(self, cipher: 'Fernet') -> str:
        """Decrypt and return API key."""
        if not self.api_key_encrypted:
            return ""
        return cipher.decrypt(self.api_key_encrypted.encode()).decode()
    
    def get_api_secret(self, cipher: 'Fernet') -> str:
        """Decrypt and return API secret."""
        if not self.api_secret_encrypted:
            return ""
        return cipher.decrypt(self.api_secret_encrypted.encode()).decode()


@dataclass
class RiskConfig:
    """Risk management configuration."""
    # Per-trade risk
    risk_per_trade_pct: float = 1.0
    max_risk_per_trade_pct: float = 2.0
    
    # Position limits
    max_position_size: float = 100000.0
    max_positions: int = 5
    max_correlated_positions: int = 3
    correlation_threshold: float = 0.7
    
    # Daily limits (Circuit Breakers)
    daily_max_loss_pct: float = 5.0
    daily_max_loss_absolute: float = 1000.0
    max_consecutive_losses: int = 5
    max_drawdown_pct: float = 10.0
    
    # Volatility adjustments
    volatility_scaling: bool = True
    min_volatility_multiplier: float = 0.5
    max_volatility_multiplier: float = 2.0
    
    # Global panic stop
    panic_stop_enabled: bool = True
    panic_stop_drawdown_pct: float = 15.0


@dataclass
class StrategyConfig:
    """Individual strategy configuration."""
    name: str
    version: str
    enabled: bool = True
    allocation_pct: float = 100.0
    parameters: Dict[str, Any] = field(default_factory=dict)
    timeframes: List[str] = field(default_factory=lambda: ["1h"])
    symbols: List[str] = field(default_factory=list)
    
    # Multi-timeframe settings
    bias_timeframe: str = "4h"
    execution_timeframe: str = "15m"
    
    # Regime filters
    regime_filters: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class ExecutionConfig:
    """Order execution configuration."""
    default_order_type: str = "limit"
    use_post_only: bool = True
    max_slippage_pct: float = 0.1
    order_timeout_seconds: int = 30
    retry_attempts: int = 3
    retry_backoff_seconds: float = 1.0
    partial_fill_threshold: float = 0.9


@dataclass
class AlertConfig:
    """Alerting and notification configuration."""
    enabled: bool = True
    telegram_enabled: bool = False
    telegram_token: str = ""
    telegram_chat_id: str = ""
    discord_enabled: bool = False
    discord_webhook: str = ""
    email_enabled: bool = False
    email_smtp_server: str = ""
    email_smtp_port: int = 587
    email_username: str = ""
    email_password: str = ""
    email_recipients: List[str] = field(default_factory=list)
    
    # Alert triggers
    alert_on_trade: bool = True
    alert_on_error: bool = True
    alert_on_risk_limit: bool = True
    alert_on_unusual_behavior: bool = True
    alert_on_bot_status: bool = True


@dataclass
class BacktestConfig:
    """Backtesting configuration."""
    start_date: str = "2024-01-01"
    end_date: str = "2024-12-31"
    initial_capital: float = 100000.0
    commission_pct: float = 0.1
    slippage_pct: float = 0.05
    use_spread: bool = True
    spread_pct: float = 0.02
    data_source: str = "local"  # local, api, database


@dataclass
class LoggingConfig:
    """Logging configuration."""
    log_level: LogLevel = LogLevel.INFO
    log_dir: str = "logs"
    file_logging: bool = True
    log_rotation: str = "daily"
    max_log_files: int = 30
    log_trade_decisions: bool = True
    log_indicator_values: bool = True
    log_risk_state: bool = True


@dataclass
class PersistenceConfig:
    """State persistence configuration."""
    enabled: bool = True
    backend: str = "sqlite"  # sqlite, postgresql, redis
    database_path: str = "data/trading_bot.db"
    save_interval_seconds: int = 60
    backup_enabled: bool = True
    backup_interval_hours: int = 24


@dataclass
class BotConfig:
    """Main bot configuration."""
    bot_id: str = "trading_bot"
    version: str = "1.0.0"
    mode: TradingMode = TradingMode.PAPER
    data_dir: str = "./data"
    
    # Components
    exchanges: List[ExchangeConfig] = field(default_factory=list)
    strategies: List[StrategyConfig] = field(default_factory=list)
    risk: RiskConfig = field(default_factory=RiskConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    alerts: AlertConfig = field(default_factory=AlertConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    
    # Feature flags
    feature_flags: Dict[str, bool] = field(default_factory=lambda: {
        "regime_detection": True,
        "multi_timeframe": True,
        "ensemble_strategies": False,
        "adaptive_logic": False,
        "news_blackout": True,
        "correlation_limits": True,
    })


def load_config(config_path: str) -> BotConfig:
    """Load bot configuration from YAML or JSON file."""
    path = Path(config_path)
    
    if not path.exists():
        logger.warning(f"Config file not found: {config_path}, using defaults")
        return BotConfig()
    
    if path.suffix in ['.yaml', '.yml']:
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
    elif path.suffix == '.json':
        with open(path, 'r') as f:
            data = json.load(f)
    else:
        raise ValueError(f"Unsupported config format: {path.suffix}")
    
    return _parse_bot_config(data)


def _parse_bot_config(data: Dict[str, Any]) -> BotConfig:
    """Parse configuration dictionary into BotConfig."""
    config = BotConfig()
    
    # Basic fields
    config.bot_id = data.get('bot_id', config.bot_id)
    config.version = data.get('version', config.version)
    config.data_dir = data.get('data_dir', config.data_dir)
    
    if 'mode' in data:
        config.mode = TradingMode(data['mode'])
    
    # Exchanges
    if 'exchanges' in data:
        config.exchanges = [
            ExchangeConfig(**ex) for ex in data['exchanges']
        ]
    
    # Strategies
    if 'strategies' in data:
        config.strategies = [
            StrategyConfig(**s) for s in data['strategies']
        ]
    
    # Risk
    if 'risk' in data:
        config.risk = RiskConfig(**data['risk'])
    
    # Execution
    if 'execution' in data:
        config.execution = ExecutionConfig(**data['execution'])
    
    # Alerts
    if 'alerts' in data:
        config.alerts = AlertConfig(**data['alerts'])
    
    # Logging
    if 'logging' in data:
        log_data = data['logging']
        if 'log_level' in log_data:
            log_data['log_level'] = LogLevel(log_data['log_level'])
        config.logging = LoggingConfig(**log_data)
    
    # Feature flags
    if 'feature_flags' in data:
        config.feature_flags.update(data['feature_flags'])
    
    logger.info(f"Configuration loaded from file")
    return config


class Config:
    """
    Main configuration manager.
    
    Loads configuration from multiple sources with priority:
    1. Environment variables (highest)
    2. Config file
    3. Defaults (lowest)
    """
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self._encryption_key: Optional[bytes] = None
        self._cipher: Optional[Fernet] = None
        
        # Initialize all config sections
        self.mode: TradingMode = TradingMode.PAPER
        self.exchanges: Dict[str, ExchangeConfig] = {}
        self.risk: RiskConfig = RiskConfig()
        self.strategies: Dict[str, StrategyConfig] = {}
        self.execution: ExecutionConfig = ExecutionConfig()
        self.alerts: AlertConfig = AlertConfig()
        self.backtest: BacktestConfig = BacktestConfig()
        self.logging: LoggingConfig = LoggingConfig()
        self.persistence: PersistenceConfig = PersistenceConfig()
        
        # Feature flags
        self.feature_flags: Dict[str, bool] = {
            "regime_detection": True,
            "multi_timeframe": True,
            "ensemble_strategies": False,
            "adaptive_logic": False,
            "news_blackout": True,
            "correlation_limits": True,
        }
        
        # Load configuration
        if config_path:
            self.load(config_path)
        
        self._apply_env_overrides()
    
    def load(self, config_path: str) -> None:
        """Load configuration from file."""
        path = Path(config_path)
        
        if not path.exists():
            logger.warning(f"Config file not found: {config_path}")
            return
        
        if path.suffix in ['.yaml', '.yml']:
            with open(path, 'r') as f:
                data = yaml.safe_load(f)
        elif path.suffix == '.json':
            with open(path, 'r') as f:
                data = json.load(f)
        else:
            raise ValueError(f"Unsupported config format: {path.suffix}")
        
        self._parse_config(data)
        logger.info(f"Configuration loaded from {config_path}")
    
    def _parse_config(self, data: Dict[str, Any]) -> None:
        """Parse configuration dictionary."""
        # Trading mode
        if 'mode' in data:
            self.mode = TradingMode(data['mode'])
        
        # Exchanges
        if 'exchanges' in data:
            for name, exchange_data in data['exchanges'].items():
                self.exchanges[name] = ExchangeConfig(name=name, **exchange_data)
        
        # Risk configuration
        if 'risk' in data:
            self.risk = RiskConfig(**data['risk'])
        
        # Strategies
        if 'strategies' in data:
            for name, strategy_data in data['strategies'].items():
                self.strategies[name] = StrategyConfig(name=name, **strategy_data)
        
        # Execution
        if 'execution' in data:
            self.execution = ExecutionConfig(**data['execution'])
        
        # Alerts
        if 'alerts' in data:
            self.alerts = AlertConfig(**data['alerts'])
        
        # Backtest
        if 'backtest' in data:
            self.backtest = BacktestConfig(**data['backtest'])
        
        # Logging
        if 'logging' in data:
            self.logging = LoggingConfig(**data['logging'])
        
        # Persistence
        if 'persistence' in data:
            self.persistence = PersistenceConfig(**data['persistence'])
        
        # Feature flags
        if 'feature_flags' in data:
            self.feature_flags.update(data['feature_flags'])
    
    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides."""
        # Trading mode
        if mode := os.environ.get('TRADING_MODE'):
            self.mode = TradingMode(mode.lower())
        
        # Risk overrides
        if risk_pct := os.environ.get('RISK_PER_TRADE_PCT'):
            self.risk.risk_per_trade_pct = float(risk_pct)
        
        if daily_loss := os.environ.get('DAILY_MAX_LOSS_PCT'):
            self.risk.daily_max_loss_pct = float(daily_loss)
        
        # Encryption key
        if enc_key := os.environ.get('TRADING_BOT_ENCRYPTION_KEY'):
            self._encryption_key = enc_key.encode()
            self._cipher = Fernet(self._encryption_key)
        
        # Log level
        if log_level := os.environ.get('LOG_LEVEL'):
            self.logging.level = log_level.upper()
    
    def get_cipher(self) -> Optional[Fernet]:
        """Get the encryption cipher."""
        return self._cipher
    
    def is_feature_enabled(self, feature: str) -> bool:
        """Check if a feature flag is enabled."""
        return self.feature_flags.get(feature, False)
    
    def get_strategy_config(self, strategy_name: str) -> Optional[StrategyConfig]:
        """Get configuration for a specific strategy."""
        return self.strategies.get(strategy_name)
    
    def get_enabled_strategies(self) -> List[StrategyConfig]:
        """Get all enabled strategies."""
        return [s for s in self.strategies.values() if s.enabled]
    
    def save(self, config_path: Optional[str] = None) -> None:
        """Save current configuration to file."""
        path = Path(config_path or self.config_path)
        
        data = {
            'mode': self.mode.value,
            'risk': {
                'risk_per_trade_pct': self.risk.risk_per_trade_pct,
                'max_position_size': self.risk.max_position_size,
                'daily_max_loss_pct': self.risk.daily_max_loss_pct,
                'max_consecutive_losses': self.risk.max_consecutive_losses,
                'max_drawdown_pct': self.risk.max_drawdown_pct,
                'volatility_scaling': self.risk.volatility_scaling,
                'panic_stop_enabled': self.risk.panic_stop_enabled,
                'panic_stop_drawdown_pct': self.risk.panic_stop_drawdown_pct,
            },
            'execution': {
                'default_order_type': self.execution.default_order_type,
                'use_post_only': self.execution.use_post_only,
                'max_slippage_pct': self.execution.max_slippage_pct,
            },
            'feature_flags': self.feature_flags,
            'logging': {
                'level': self.logging.level,
                'file_logging': self.logging.file_logging,
                'log_trade_decisions': self.logging.log_trade_decisions,
            }
        }
        
        path.parent.mkdir(parents=True, exist_ok=True)
        
        if path.suffix in ['.yaml', '.yml']:
            with open(path, 'w') as f:
                yaml.dump(data, f, default_flow_style=False)
        else:
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        
        logger.info(f"Configuration saved to {path}")
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []
        
        # Risk validation
        if self.risk.risk_per_trade_pct <= 0 or self.risk.risk_per_trade_pct > 10:
            errors.append("Risk per trade should be between 0 and 10%")
        
        if self.risk.daily_max_loss_pct <= 0:
            errors.append("Daily max loss must be positive")
        
        if self.risk.max_drawdown_pct < self.risk.daily_max_loss_pct:
            errors.append("Max drawdown should be >= daily max loss")
        
        # Strategy validation
        total_allocation = sum(s.allocation_pct for s in self.strategies.values() if s.enabled)
        if total_allocation > 100:
            errors.append(f"Total strategy allocation ({total_allocation}%) exceeds 100%")
        
        # Exchange validation for live mode
        if self.mode == TradingMode.LIVE and not self.exchanges:
            errors.append("Live mode requires at least one exchange configured")
        
        return errors
    
    def __repr__(self) -> str:
        return f"Config(mode={self.mode.value}, strategies={len(self.strategies)}, exchanges={len(self.exchanges)})"
