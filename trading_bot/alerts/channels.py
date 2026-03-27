"""
Notification Channels
======================

Various notification channel implementations.
"""

import asyncio
import json
import smtplib
import ssl
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional
import logging

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

from trading_bot.alerts.notifier import Alert, AlertPriority

logger = logging.getLogger(__name__)


class NotificationChannel(ABC):
    """Abstract base class for notification channels."""
    
    @abstractmethod
    def send(self, alert: Alert) -> bool:
        """
        Send an alert through this channel.
        
        Returns:
            True if sent successfully
        """
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the channel is properly configured and working."""
        pass


class ConsoleChannel(NotificationChannel):
    """Console/stdout notification channel."""
    
    def __init__(self, colors: bool = True):
        self.colors = colors
        self._color_map = {
            AlertPriority.LOW: "\033[36m",      # Cyan
            AlertPriority.MEDIUM: "\033[33m",   # Yellow
            AlertPriority.HIGH: "\033[31m",     # Red
            AlertPriority.CRITICAL: "\033[35m", # Magenta
        }
        self._reset = "\033[0m"
    
    def send(self, alert: Alert) -> bool:
        """Print alert to console."""
        message = alert.format_message()
        
        if self.colors:
            color = self._color_map.get(alert.priority, "")
            print(f"{color}{message}{self._reset}")
        else:
            print(message)
        
        return True
    
    def test_connection(self) -> bool:
        """Console is always available."""
        return True


class TelegramChannel(NotificationChannel):
    """Telegram notification channel."""
    
    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        parse_mode: str = "HTML",
    ):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.parse_mode = parse_mode
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
    
    def send(self, alert: Alert) -> bool:
        """Send alert via Telegram."""
        if not REQUESTS_AVAILABLE:
            logger.error("requests library not available for Telegram")
            return False
        
        try:
            message = self._format_telegram_message(alert)
            
            response = requests.post(
                f"{self.base_url}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": self.parse_mode,
                },
                timeout=10,
            )
            
            if response.status_code == 200:
                return True
            else:
                logger.error(f"Telegram API error: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False
    
    def _format_telegram_message(self, alert: Alert) -> str:
        """Format alert for Telegram."""
        emoji = {
            AlertPriority.LOW: "ℹ️",
            AlertPriority.MEDIUM: "⚠️",
            AlertPriority.HIGH: "🔴",
            AlertPriority.CRITICAL: "🚨",
        }.get(alert.priority, "")
        
        lines = [
            f"{emoji} <b>[{alert.priority.value.upper()}] {alert.title}</b>",
            f"<i>{alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC</i>",
            "",
            alert.message,
        ]
        
        if alert.data:
            lines.append("")
            lines.append("<b>Details:</b>")
            for key, value in alert.data.items():
                lines.append(f"• {key}: <code>{value}</code>")
        
        return "\n".join(lines)
    
    def test_connection(self) -> bool:
        """Test Telegram connection."""
        if not REQUESTS_AVAILABLE:
            return False
        
        try:
            response = requests.get(
                f"{self.base_url}/getMe",
                timeout=10,
            )
            return response.status_code == 200
        except Exception:
            return False


class DiscordChannel(NotificationChannel):
    """Discord webhook notification channel."""
    
    def __init__(
        self,
        webhook_url: str,
        username: str = "Trading Bot",
        avatar_url: Optional[str] = None,
    ):
        self.webhook_url = webhook_url
        self.username = username
        self.avatar_url = avatar_url
    
    def send(self, alert: Alert) -> bool:
        """Send alert via Discord webhook."""
        if not REQUESTS_AVAILABLE:
            logger.error("requests library not available for Discord")
            return False
        
        try:
            embed = self._create_discord_embed(alert)
            
            payload = {
                "username": self.username,
                "embeds": [embed],
            }
            
            if self.avatar_url:
                payload["avatar_url"] = self.avatar_url
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10,
            )
            
            if response.status_code in (200, 204):
                return True
            else:
                logger.error(f"Discord webhook error: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending Discord message: {e}")
            return False
    
    def _create_discord_embed(self, alert: Alert) -> Dict[str, Any]:
        """Create Discord embed from alert."""
        color_map = {
            AlertPriority.LOW: 0x3498db,      # Blue
            AlertPriority.MEDIUM: 0xf39c12,   # Yellow/Orange
            AlertPriority.HIGH: 0xe74c3c,     # Red
            AlertPriority.CRITICAL: 0x9b59b6, # Purple
        }
        
        embed = {
            "title": f"[{alert.priority.value.upper()}] {alert.title}",
            "description": alert.message,
            "color": color_map.get(alert.priority, 0x95a5a6),
            "timestamp": alert.timestamp.isoformat(),
            "footer": {
                "text": f"Category: {alert.category.value}",
            },
        }
        
        if alert.data:
            embed["fields"] = [
                {"name": key, "value": str(value), "inline": True}
                for key, value in list(alert.data.items())[:25]  # Discord limit
            ]
        
        return embed
    
    def test_connection(self) -> bool:
        """Test Discord webhook."""
        # Discord webhooks don't have a test endpoint
        # We'd need to send a test message
        return REQUESTS_AVAILABLE


class EmailChannel(NotificationChannel):
    """Email notification channel."""
    
    def __init__(
        self,
        smtp_server: str,
        smtp_port: int,
        username: str,
        password: str,
        from_address: str,
        to_addresses: List[str],
        use_tls: bool = True,
    ):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_address = from_address
        self.to_addresses = to_addresses
        self.use_tls = use_tls
    
    def send(self, alert: Alert) -> bool:
        """Send alert via email."""
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[{alert.priority.value.upper()}] {alert.title}"
            msg["From"] = self.from_address
            msg["To"] = ", ".join(self.to_addresses)
            
            # Plain text version
            text_content = alert.format_message(include_timestamp=True)
            
            # HTML version
            html_content = self._format_email_html(alert)
            
            msg.attach(MIMEText(text_content, "plain"))
            msg.attach(MIMEText(html_content, "html"))
            
            # Connect and send
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls(context=context)
                server.login(self.username, self.password)
                server.send_message(msg)
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending email: {e}")
            return False
    
    def _format_email_html(self, alert: Alert) -> str:
        """Format alert as HTML email."""
        color_map = {
            AlertPriority.LOW: "#3498db",
            AlertPriority.MEDIUM: "#f39c12",
            AlertPriority.HIGH: "#e74c3c",
            AlertPriority.CRITICAL: "#9b59b6",
        }
        
        color = color_map.get(alert.priority, "#95a5a6")
        
        data_rows = ""
        if alert.data:
            for key, value in alert.data.items():
                data_rows += f"<tr><td><strong>{key}</strong></td><td>{value}</td></tr>"
        
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; padding: 20px;">
            <div style="border-left: 4px solid {color}; padding-left: 15px;">
                <h2 style="color: {color}; margin: 0;">
                    [{alert.priority.value.upper()}] {alert.title}
                </h2>
                <p style="color: #666; font-size: 12px;">
                    {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC
                </p>
                <p>{alert.message}</p>
                {f'<table style="border-collapse: collapse; margin-top: 10px;">{data_rows}</table>' if data_rows else ''}
            </div>
            <hr style="margin-top: 20px; border: none; border-top: 1px solid #ddd;">
            <p style="color: #999; font-size: 11px;">
                This is an automated message from Trading Bot.
            </p>
        </body>
        </html>
        """
    
    def test_connection(self) -> bool:
        """Test SMTP connection."""
        try:
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=10) as server:
                if self.use_tls:
                    server.starttls(context=context)
                server.login(self.username, self.password)
            
            return True
            
        except Exception as e:
            logger.error(f"Email connection test failed: {e}")
            return False


class WebhookChannel(NotificationChannel):
    """Generic webhook notification channel."""
    
    def __init__(
        self,
        url: str,
        method: str = "POST",
        headers: Optional[Dict[str, str]] = None,
        auth: Optional[tuple] = None,
    ):
        self.url = url
        self.method = method
        self.headers = headers or {"Content-Type": "application/json"}
        self.auth = auth
    
    def send(self, alert: Alert) -> bool:
        """Send alert via webhook."""
        if not REQUESTS_AVAILABLE:
            logger.error("requests library not available for webhook")
            return False
        
        try:
            payload = alert.to_dict()
            
            response = requests.request(
                method=self.method,
                url=self.url,
                json=payload,
                headers=self.headers,
                auth=self.auth,
                timeout=10,
            )
            
            if response.status_code in (200, 201, 202, 204):
                return True
            else:
                logger.error(f"Webhook error: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending webhook: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test webhook endpoint."""
        return REQUESTS_AVAILABLE
