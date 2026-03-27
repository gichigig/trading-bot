"""
Trading Logger
===============

Structured logging that tells a story, not just "order failed".
"""

import json
import logging
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add extra fields
        if hasattr(record, "extra_data"):
            log_data.update(record.extra_data)
        
        # Add exception info
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


class TradingFormatter(logging.Formatter):
    """Custom formatter for trading logs."""
    
    COLORS = {
        "DEBUG": "\033[36m",      # Cyan
        "INFO": "\033[32m",       # Green
        "WARNING": "\033[33m",    # Yellow
        "ERROR": "\033[31m",      # Red
        "CRITICAL": "\033[35m",   # Magenta
    }
    RESET = "\033[0m"
    
    def __init__(self, use_colors: bool = True):
        super().__init__()
        self.use_colors = use_colors
    
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level = record.levelname
        name = record.name.split(".")[-1]
        message = record.getMessage()
        
        if self.use_colors:
            color = self.COLORS.get(level, "")
            formatted = f"{timestamp} | {color}{level:8}{self.RESET} | {name:15} | {message}"
        else:
            formatted = f"{timestamp} | {level:8} | {name:15} | {message}"
        
        # Add extra context
        if hasattr(record, "extra_data"):
            extra = record.extra_data
            if extra:
                formatted += f" | {json.dumps(extra)}"
        
        if record.exc_info:
            formatted += f"\n{self.formatException(record.exc_info)}"
        
        return formatted


class TradingLogger:
    """
    Enhanced logger for trading operations.
    
    Features:
    - Structured context (why trades were taken/skipped)
    - Indicator values at decision time
    - Risk state snapshots
    - Trade correlation tracking
    """
    
    def __init__(self, name: str):
        self._logger = logging.getLogger(name)
        self._context: Dict[str, Any] = {}
    
    def set_context(self, **kwargs) -> None:
        """Set persistent context for all log messages."""
        self._context.update(kwargs)
    
    def clear_context(self) -> None:
        """Clear persistent context."""
        self._context = {}
    
    def _log(
        self,
        level: int,
        message: str,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Internal log method with extra data."""
        merged_data = {**self._context}
        if extra_data:
            merged_data.update(extra_data)
        
        record = self._logger.makeRecord(
            self._logger.name,
            level,
            "",
            0,
            message,
            (),
            None,
        )
        record.extra_data = merged_data
        self._logger.handle(record)
    
    def debug(self, message: str, **kwargs) -> None:
        self._log(logging.DEBUG, message, kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        self._log(logging.INFO, message, kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        self._log(logging.WARNING, message, kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        self._log(logging.ERROR, message, kwargs)
    
    def critical(self, message: str, **kwargs) -> None:
        self._log(logging.CRITICAL, message, kwargs)
    
    # =========== Trade Decision Logging ===========
    
    def log_signal_generated(
        self,
        strategy: str,
        symbol: str,
        signal_type: str,
        indicators: Dict[str, float],
        reason: str,
    ) -> None:
        """Log why a signal was generated."""
        self.info(
            f"Signal generated: {signal_type}",
            strategy=strategy,
            symbol=symbol,
            signal_type=signal_type,
            indicators=indicators,
            reason=reason,
        )
    
    def log_signal_skipped(
        self,
        strategy: str,
        symbol: str,
        reason: str,
        indicators: Optional[Dict[str, float]] = None,
    ) -> None:
        """Log why a potential signal was skipped."""
        self.debug(
            f"Signal skipped: {reason}",
            strategy=strategy,
            symbol=symbol,
            reason=reason,
            indicators=indicators,
        )
    
    def log_trade_decision(
        self,
        decision: str,  # "taken" or "rejected"
        signal_id: str,
        reason: str,
        risk_state: Dict[str, Any],
        indicators: Optional[Dict[str, float]] = None,
    ) -> None:
        """Log the decision to take or reject a trade."""
        level = logging.INFO if decision == "taken" else logging.DEBUG
        self._log(
            level,
            f"Trade {decision}: {reason}",
            {
                "signal_id": signal_id,
                "decision": decision,
                "reason": reason,
                "risk_state": risk_state,
                "indicators": indicators,
            }
        )
    
    def log_order_execution(
        self,
        order_id: str,
        symbol: str,
        side: str,
        quantity: float,
        order_type: str,
        status: str,
        price: Optional[float] = None,
        fill_price: Optional[float] = None,
        slippage: Optional[float] = None,
        execution_time_ms: Optional[float] = None,
    ) -> None:
        """Log order execution details."""
        self.info(
            f"Order {status}: {side} {quantity} {symbol}",
            order_id=order_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=order_type,
            status=status,
            price=price,
            fill_price=fill_price,
            slippage=slippage,
            execution_time_ms=execution_time_ms,
        )
    
    def log_position_update(
        self,
        position_id: str,
        symbol: str,
        side: str,
        quantity: float,
        entry_price: float,
        current_price: float,
        unrealized_pnl: float,
        event: str,  # "opened", "closed", "partial_close", "stop_hit"
    ) -> None:
        """Log position lifecycle events."""
        self.info(
            f"Position {event}: {symbol}",
            position_id=position_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            entry_price=entry_price,
            current_price=current_price,
            unrealized_pnl=unrealized_pnl,
            event=event,
        )
    
    def log_risk_event(
        self,
        event_type: str,
        message: str,
        current_risk: Dict[str, Any],
        threshold: Optional[float] = None,
        current_value: Optional[float] = None,
    ) -> None:
        """Log risk-related events."""
        level = logging.WARNING if event_type in ["limit_breach", "circuit_breaker"] else logging.INFO
        self._log(
            level,
            f"Risk event [{event_type}]: {message}",
            {
                "risk_event_type": event_type,
                "current_risk": current_risk,
                "threshold": threshold,
                "current_value": current_value,
            }
        )


def setup_logging(
    log_dir: str = "./logs",
    log_level: str = "INFO",
    console: bool = True,
    file: bool = True,
    json_format: bool = False,
    max_file_size_mb: int = 10,
    backup_count: int = 5,
) -> None:
    """
    Setup logging for the trading system.
    
    Args:
        log_dir: Directory for log files
        log_level: Minimum log level
        console: Enable console output
        file: Enable file output
        json_format: Use JSON format for file logs
        max_file_size_mb: Max size per log file
        backup_count: Number of backup files to keep
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    root_logger.handlers = []
    
    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(TradingFormatter(use_colors=True))
        root_logger.addHandler(console_handler)
    
    # File handler
    if file:
        file_handler = RotatingFileHandler(
            log_path / "trading_bot.log",
            maxBytes=max_file_size_mb * 1024 * 1024,
            backupCount=backup_count,
        )
        
        if json_format:
            file_handler.setFormatter(JsonFormatter())
        else:
            file_handler.setFormatter(TradingFormatter(use_colors=False))
        
        root_logger.addHandler(file_handler)
    
    # Separate file for trades only
    trade_handler = RotatingFileHandler(
        log_path / "trades.log",
        maxBytes=max_file_size_mb * 1024 * 1024,
        backupCount=backup_count,
    )
    trade_handler.setFormatter(JsonFormatter())
    trade_handler.addFilter(lambda r: "signal" in r.getMessage().lower() or "order" in r.getMessage().lower())
    root_logger.addHandler(trade_handler)
    
    # Separate file for errors
    error_handler = RotatingFileHandler(
        log_path / "errors.log",
        maxBytes=max_file_size_mb * 1024 * 1024,
        backupCount=backup_count,
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(JsonFormatter())
    root_logger.addHandler(error_handler)
    
    logging.info("Logging initialized", extra={"extra_data": {"log_level": log_level}})
