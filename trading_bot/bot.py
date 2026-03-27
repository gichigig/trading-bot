"""
Trading Bot - Main Orchestrator
=================================

The main trading bot that coordinates all components.
"""

import asyncio
import logging
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from pathlib import Path

from trading_bot.core.config import BotConfig, TradingMode, load_config
from trading_bot.core.events import Event, EventType, EventBus, get_event_bus
from trading_bot.core.types import Signal, Position, Candle

from trading_bot.strategies.base import BaseStrategy
from trading_bot.strategies.registry import get_strategy, list_strategies

from trading_bot.data.manager import DataManager
from trading_bot.risk.engine import RiskEngine
from trading_bot.execution.manager import ExecutionManager

from trading_bot.persistence.state import StateManager
from trading_bot.persistence.snapshots import SnapshotManager

from trading_bot.observability.logging import setup_logging, TradingLogger
from trading_bot.observability.metrics import MetricsCollector

from trading_bot.alerts.notifier import AlertManager, AlertCategory, AlertPriority
from trading_bot.alerts.channels import ConsoleChannel, TelegramChannel

from trading_bot.regime.detector import RegimeDetector
from trading_bot.regime.session import SessionManager

logger = logging.getLogger(__name__)


class TradingBot:
    """
    Main trading bot orchestrator.
    
    Coordinates:
    - Strategy execution
    - Risk management
    - Order execution
    - Data management
    - State persistence
    - Alerting
    """
    
    def __init__(
        self,
        config: BotConfig,
        event_bus: Optional[EventBus] = None,
    ):
        self.config = config
        self._event_bus = event_bus or get_event_bus()
        
        # Initialize logging
        setup_logging(
            log_dir=config.logging.log_dir,
            log_level=config.logging.log_level.value,
            console=True,
            file=True,
        )
        
        self._logger = TradingLogger("trading_bot")
        
        # Components
        self._data_manager: Optional[DataManager] = None
        self._risk_engine: Optional[RiskEngine] = None
        self._execution_manager: Optional[ExecutionManager] = None
        self._state_manager: Optional[StateManager] = None
        self._snapshot_manager: Optional[SnapshotManager] = None
        self._alert_manager: Optional[AlertManager] = None
        self._metrics: Optional[MetricsCollector] = None
        self._regime_detector: Optional[RegimeDetector] = None
        self._session_manager: Optional[SessionManager] = None
        
        # Strategies
        self._strategies: Dict[str, BaseStrategy] = {}
        self._active_strategies: Set[str] = set()
        
        # State
        self._running = False
        self._paused = False
        self._start_time: Optional[datetime] = None
        
        # Shutdown handling
        self._shutdown_event = asyncio.Event()
    
    async def initialize(self) -> bool:
        """Initialize all bot components."""
        try:
            logger.info("Initializing trading bot...")
            
            # Initialize components
            self._data_manager = DataManager(self._event_bus)
            self._risk_engine = RiskEngine(self.config.risk, self._event_bus)
            self._execution_manager = ExecutionManager(
                self.config.execution,
                self._event_bus,
            )
            
            # Persistence
            self._state_manager = StateManager(
                bot_id=self.config.bot_id,
                version=self.config.version,
                event_bus=self._event_bus,
            )
            self._state_manager.initialize()
            
            self._snapshot_manager = SnapshotManager(
                base_path=str(Path(self.config.data_dir) / "snapshots"),
            )
            
            # Metrics
            self._metrics = MetricsCollector()
            
            # Alerting
            self._alert_manager = AlertManager(self._event_bus)
            self._setup_alert_channels()
            
            # Regime detection
            self._regime_detector = RegimeDetector()
            self._session_manager = SessionManager()
            
            # Load strategies
            await self._load_strategies()
            
            # Restore state if exists
            await self._restore_state()
            
            # Subscribe to events
            self._subscribe_events()
            
            logger.info("Trading bot initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize bot: {e}", exc_info=True)
            return False
    
    def _setup_alert_channels(self) -> None:
        """Setup notification channels."""
        # Console always enabled
        self._alert_manager.register_channel(
            "console",
            ConsoleChannel(colors=True),
            priorities=[AlertPriority.HIGH, AlertPriority.CRITICAL],
        )
        
        # Telegram if configured
        if self.config.alerts.telegram_enabled:
            self._alert_manager.register_channel(
                "telegram",
                TelegramChannel(
                    bot_token=self.config.alerts.telegram_token,
                    chat_id=self.config.alerts.telegram_chat_id,
                ),
            )
    
    async def _load_strategies(self) -> None:
        """Load configured strategies."""
        for strategy_config in self.config.strategies:
            if not strategy_config.enabled:
                continue
            
            try:
                strategy_class = get_strategy(strategy_config.name)
                
                if not strategy_class:
                    logger.warning(f"Strategy not found: {strategy_config.name}")
                    continue
                
                strategy = strategy_class(strategy_config.parameters)
                strategy.set_version(strategy_config.version)
                
                self._strategies[strategy_config.name] = strategy
                self._active_strategies.add(strategy_config.name)
                
                logger.info(f"Loaded strategy: {strategy_config.name} v{strategy_config.version}")
                
            except Exception as e:
                logger.error(f"Failed to load strategy {strategy_config.name}: {e}")
    
    async def _restore_state(self) -> None:
        """Restore previous state if available."""
        try:
            # Get open positions from state
            positions = self._state_manager.get_open_positions()
            
            if positions:
                logger.info(f"Restoring {len(positions)} positions from state")
                
                # Restore to execution manager
                for pos_data in positions:
                    # Convert dict back to Position object
                    # self._execution_manager.restore_position(pos_data)
                    pass
            
            # Get risk state
            risk_state = self._state_manager.get_risk_state()
            if risk_state:
                logger.info(f"Restored risk state: {risk_state}")
                
        except Exception as e:
            logger.error(f"Error restoring state: {e}")
    
    def _subscribe_events(self) -> None:
        """Subscribe to relevant events."""
        self._event_bus.subscribe(EventType.CANDLE, self._on_candle)
        self._event_bus.subscribe(EventType.SIGNAL_GENERATED, self._on_signal)
        self._event_bus.subscribe(EventType.POSITION_CLOSED, self._on_position_closed)
        self._event_bus.subscribe(EventType.PANIC_STOP, self._on_panic_stop)
    
    async def start(self) -> None:
        """Start the trading bot."""
        if self._running:
            logger.warning("Bot is already running")
            return
        
        logger.info("Starting trading bot...")
        self._running = True
        self._start_time = datetime.utcnow()
        
        # Start strategies
        for name, strategy in self._strategies.items():
            if name in self._active_strategies:
                strategy.start()
                logger.info(f"Started strategy: {name}")
        
        # Publish start event
        self._event_bus.publish(Event(
            event_type=EventType.BOT_STARTED,
            source="trading_bot",
            data={
                "mode": self.config.mode.value,
                "strategies": list(self._active_strategies),
            },
        ))
        
        # Alert
        self._alert_manager.alert_system(
            "Trading Bot Started",
            f"Bot started in {self.config.mode.value} mode with "
            f"{len(self._active_strategies)} strategies",
            mode=self.config.mode.value,
        )
        
        # Main loop
        try:
            await self._main_loop()
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
        finally:
            await self.stop()
    
    async def _main_loop(self) -> None:
        """Main bot loop."""
        while self._running and not self._shutdown_event.is_set():
            try:
                # Check if paused
                if self._paused:
                    await asyncio.sleep(1)
                    continue
                
                # Periodic tasks
                await self._periodic_tasks()
                
                # Small sleep to prevent tight loop
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in main loop iteration: {e}")
                await asyncio.sleep(1)
    
    async def _periodic_tasks(self) -> None:
        """Run periodic maintenance tasks."""
        now = datetime.utcnow()
        
        # Save state periodically
        if hasattr(self, '_last_state_save'):
            if (now - self._last_state_save).seconds >= 60:
                self._state_manager.save()
                self._last_state_save = now
        else:
            self._last_state_save = now
        
        # Create periodic snapshot
        if hasattr(self, '_last_snapshot'):
            if (now - self._last_snapshot).seconds >= 3600:  # Hourly
                self._create_snapshot("scheduled")
                self._last_snapshot = now
        else:
            self._last_snapshot = now
        
        # Update session
        current_session = self._session_manager.get_current_session()
        # Could update strategies with session info
    
    async def stop(self) -> None:
        """Stop the trading bot gracefully."""
        if not self._running:
            return
        
        logger.info("Stopping trading bot...")
        self._running = False
        self._shutdown_event.set()
        
        # Stop strategies
        for name, strategy in self._strategies.items():
            strategy.stop()
            logger.info(f"Stopped strategy: {name}")
        
        # Save final state
        self._state_manager.save(force=True)
        
        # Create shutdown snapshot
        self._create_snapshot("shutdown")
        
        # Publish stop event
        self._event_bus.publish(Event(
            event_type=EventType.BOT_STOPPED,
            source="trading_bot",
            data={"runtime_seconds": (datetime.utcnow() - self._start_time).total_seconds()},
        ))
        
        # Alert
        self._alert_manager.alert_system(
            "Trading Bot Stopped",
            "Bot has stopped",
            priority=AlertPriority.MEDIUM,
        )
        
        logger.info("Trading bot stopped")
    
    def pause(self) -> None:
        """Pause trading (stop generating new signals)."""
        self._paused = True
        logger.info("Trading paused")
        
        for strategy in self._strategies.values():
            strategy.pause()
    
    def resume(self) -> None:
        """Resume trading."""
        self._paused = False
        logger.info("Trading resumed")
        
        for strategy in self._strategies.values():
            strategy.resume()
    
    def _create_snapshot(self, trigger: str) -> None:
        """Create a state snapshot."""
        try:
            self._snapshot_manager.create_recovery_point(
                bot_state=self._state_manager.get_state_summary(),
                risk_state=self._state_manager.get_risk_state(),
                strategy_states={
                    name: strategy.get_state()
                    for name, strategy in self._strategies.items()
                },
            )
        except Exception as e:
            logger.error(f"Error creating snapshot: {e}")
    
    # =========== Event Handlers ===========
    
    def _on_candle(self, event: Event) -> None:
        """Handle incoming candle data."""
        if self._paused:
            return
        
        # Update data manager (already done by data feed)
        # Process through strategies
        # This would normally come from the data feed subscription
    
    def _on_signal(self, event: Event) -> None:
        """Handle generated signal."""
        signal_data = event.data
        
        self._logger.log_signal_generated(
            strategy=signal_data.get("strategy", ""),
            symbol=signal_data.get("symbol", ""),
            signal_type=signal_data.get("type", ""),
            indicators=signal_data.get("indicators", {}),
            reason=signal_data.get("reason", ""),
        )
        
        # Risk check will be done by execution manager
    
    def _on_position_closed(self, event: Event) -> None:
        """Handle position closed event."""
        pnl = event.data.get("pnl", 0)
        
        # Update metrics
        self._metrics.update_equity(self._metrics._current_equity + pnl)
        
        # Update state
        self._state_manager.update_risk_state(
            daily_pnl=self._state_manager.get_risk_state().get("daily_pnl", 0) + pnl,
        )
    
    def _on_panic_stop(self, event: Event) -> None:
        """Handle panic stop event."""
        logger.critical("PANIC STOP TRIGGERED")
        
        # Stop all trading immediately
        self._paused = True
        
        # Cancel all pending orders
        # Close all positions (handled by circuit breaker)
        
        self._alert_manager.alert_risk(
            "PANIC STOP ACTIVATED",
            "Emergency stop triggered. All trading halted.",
            priority=AlertPriority.CRITICAL,
            reason=event.data.get("reason", "unknown"),
        )
    
    # =========== Public API ===========
    
    def get_status(self) -> Dict[str, Any]:
        """Get current bot status."""
        return {
            "running": self._running,
            "paused": self._paused,
            "mode": self.config.mode.value,
            "uptime_seconds": (
                (datetime.utcnow() - self._start_time).total_seconds()
                if self._start_time else 0
            ),
            "active_strategies": list(self._active_strategies),
            "positions": len(self._state_manager.get_open_positions()),
            "metrics": self._metrics.get_summary() if self._metrics else {},
        }
    
    def get_strategy(self, name: str) -> Optional[BaseStrategy]:
        """Get a strategy by name."""
        return self._strategies.get(name)
    
    def enable_strategy(self, name: str) -> bool:
        """Enable a strategy."""
        if name in self._strategies:
            self._active_strategies.add(name)
            self._strategies[name].start()
            return True
        return False
    
    def disable_strategy(self, name: str) -> bool:
        """Disable a strategy."""
        if name in self._active_strategies:
            self._active_strategies.discard(name)
            self._strategies[name].stop()
            return True
        return False


async def run_bot(config_path: str = "config.yaml") -> None:
    """Run the trading bot."""
    # Load config
    config = load_config(config_path)
    
    # Create bot
    bot = TradingBot(config)
    
    # Setup signal handlers
    def shutdown_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        asyncio.create_task(bot.stop())
    
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    # Initialize and start
    if await bot.initialize():
        await bot.start()
    else:
        logger.error("Failed to initialize bot")


if __name__ == "__main__":
    asyncio.run(run_bot())
