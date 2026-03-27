"""
Alerts Module
==============

Alerting and notification system for the trading bot.
Silence is suspicious - you should never be surprised.
"""

from trading_bot.alerts.notifier import AlertManager, Alert, AlertPriority
from trading_bot.alerts.channels import (
    NotificationChannel,
    TelegramChannel,
    DiscordChannel,
    EmailChannel,
    ConsoleChannel,
)

__all__ = [
    "AlertManager",
    "Alert",
    "AlertPriority",
    "NotificationChannel",
    "TelegramChannel",
    "DiscordChannel",
    "EmailChannel",
    "ConsoleChannel",
]
