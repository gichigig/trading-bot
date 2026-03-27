"""
Market Simulator
=================

Simulates realistic market conditions for backtesting.
"""

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional, Tuple
import math


@dataclass
class SimulatedTick:
    """Simulated market tick."""
    symbol: str
    timestamp: datetime
    bid: float
    ask: float
    last: float
    volume: float


class MarketSimulator:
    """
    Simulates realistic market conditions.
    
    Features:
    - Spread simulation
    - Volume patterns
    - Volatility clustering
    - Session patterns
    - News impact simulation
    """
    
    def __init__(
        self,
        base_spread_pct: float = 0.01,
        base_volatility: float = 0.001,
        tick_interval_ms: int = 100,
    ):
        self.base_spread_pct = base_spread_pct
        self.base_volatility = base_volatility
        self.tick_interval_ms = tick_interval_ms
        
        # Volatility clustering (GARCH-like)
        self._volatility_state = 1.0
        self._volatility_persistence = 0.95
        
        # Session multipliers (hour -> volatility multiplier)
        self._session_volatility = {
            0: 0.7,   # Quiet Asia
            1: 0.6,
            2: 0.5,
            3: 0.6,
            4: 0.7,
            5: 0.8,
            6: 0.9,
            7: 1.0,   # London open
            8: 1.3,
            9: 1.2,
            10: 1.1,
            11: 1.0,
            12: 1.1,
            13: 1.4,  # NY open / London-NY overlap
            14: 1.5,
            15: 1.3,
            16: 1.2,  # London close
            17: 1.0,
            18: 0.9,
            19: 0.8,
            20: 0.7,
            21: 0.6,
            22: 0.6,
            23: 0.6,
        }
    
    def generate_ticks(
        self,
        symbol: str,
        start_price: float,
        start_time: datetime,
        duration_hours: float = 1.0,
    ) -> Generator[SimulatedTick, None, None]:
        """
        Generate simulated tick data.
        
        Args:
            symbol: Trading symbol
            start_price: Initial price
            start_time: Start timestamp
            duration_hours: Duration in hours
        
        Yields:
            SimulatedTick objects
        """
        current_price = start_price
        current_time = start_time
        end_time = start_time + timedelta(hours=duration_hours)
        
        while current_time < end_time:
            # Get session volatility
            hour = current_time.hour
            session_mult = self._session_volatility.get(hour, 1.0)
            
            # Update volatility state (clustering)
            shock = random.gauss(0, 1)
            self._volatility_state = (
                self._volatility_persistence * self._volatility_state +
                (1 - self._volatility_persistence) * abs(shock)
            )
            
            # Calculate current volatility
            volatility = self.base_volatility * session_mult * self._volatility_state
            
            # Generate price movement
            returns = random.gauss(0, volatility)
            current_price *= (1 + returns)
            
            # Generate spread (wider in low liquidity)
            spread_mult = 1 + (2.0 - session_mult) * 0.5  # Inverse of session activity
            spread = current_price * self.base_spread_pct * spread_mult / 100
            
            # Generate bid/ask
            half_spread = spread / 2
            bid = current_price - half_spread
            ask = current_price + half_spread
            
            # Generate volume
            base_volume = 100
            volume = base_volume * session_mult * random.uniform(0.5, 2.0)
            
            yield SimulatedTick(
                symbol=symbol,
                timestamp=current_time,
                bid=round(bid, 5),
                ask=round(ask, 5),
                last=round(current_price, 5),
                volume=round(volume, 2),
            )
            
            current_time += timedelta(milliseconds=self.tick_interval_ms)
    
    def simulate_fill(
        self,
        side: str,
        quantity: float,
        current_price: float,
        spread_pct: Optional[float] = None,
        market_impact_factor: float = 0.0001,
    ) -> Tuple[float, float]:
        """
        Simulate realistic order fill.
        
        Args:
            side: 'buy' or 'sell'
            quantity: Order quantity
            current_price: Current market price
            spread_pct: Override spread percentage
            market_impact_factor: Price impact per unit
        
        Returns:
            (fill_price, slippage)
        """
        spread = spread_pct or self.base_spread_pct
        half_spread = current_price * spread / 100 / 2
        
        # Market impact (larger orders move price more)
        market_impact = quantity * market_impact_factor * current_price
        
        # Random slippage component
        random_slip = random.uniform(0, half_spread * 0.5)
        
        if side.lower() == 'buy':
            # Buy at ask + impact + random slip
            fill_price = current_price + half_spread + market_impact + random_slip
        else:
            # Sell at bid - impact - random slip
            fill_price = current_price - half_spread - market_impact - random_slip
        
        slippage = abs(fill_price - current_price)
        
        return round(fill_price, 5), round(slippage, 5)
    
    def simulate_partial_fill(
        self,
        quantity: float,
        fill_probability: float = 0.8,
    ) -> List[Tuple[float, float]]:
        """
        Simulate partial fills.
        
        Returns list of (fill_quantity, delay_ms) tuples.
        """
        fills = []
        remaining = quantity
        
        while remaining > 0:
            if random.random() < fill_probability:
                fill_pct = random.uniform(0.3, 1.0)
                fill_qty = min(remaining * fill_pct, remaining)
                delay = random.uniform(50, 500)
                fills.append((fill_qty, delay))
                remaining -= fill_qty
            else:
                # Order sits unfilled for a bit
                fills.append((0, random.uniform(100, 1000)))
        
        return fills
    
    def simulate_news_impact(
        self,
        price: float,
        impact_magnitude: str = "medium",
        direction: Optional[str] = None,
    ) -> Tuple[float, float, float]:
        """
        Simulate news impact on price.
        
        Args:
            price: Current price
            impact_magnitude: 'low', 'medium', 'high'
            direction: 'positive', 'negative', or None for random
        
        Returns:
            (new_price, spike_high, spike_low)
        """
        magnitude_map = {
            "low": (0.001, 0.003),
            "medium": (0.005, 0.015),
            "high": (0.02, 0.05),
        }
        
        min_impact, max_impact = magnitude_map.get(impact_magnitude, (0.005, 0.015))
        impact_pct = random.uniform(min_impact, max_impact)
        
        if direction is None:
            direction = random.choice(["positive", "negative"])
        
        if direction == "positive":
            move = price * impact_pct
        else:
            move = -price * impact_pct
        
        new_price = price + move
        
        # Spike calculation (overshoot then settle)
        overshoot = abs(move) * random.uniform(0.3, 0.8)
        
        if move > 0:
            spike_high = price + abs(move) + overshoot
            spike_low = price - abs(move) * 0.2
        else:
            spike_high = price + abs(move) * 0.2
            spike_low = price - abs(move) - overshoot
        
        return round(new_price, 5), round(spike_high, 5), round(spike_low, 5)
    
    def apply_session_characteristics(
        self,
        hour: int,
        base_volatility: float,
        base_spread: float,
    ) -> Tuple[float, float]:
        """
        Apply session-specific characteristics.
        
        Returns:
            (adjusted_volatility, adjusted_spread)
        """
        session_mult = self._session_volatility.get(hour, 1.0)
        
        # Higher volatility = tighter spreads (more activity)
        spread_mult = 1 / session_mult
        
        return (
            base_volatility * session_mult,
            base_spread * spread_mult,
        )
    
    def generate_realistic_candle(
        self,
        open_price: float,
        volatility: float = 0.01,
        trend_bias: float = 0.0,
        session_hour: int = 12,
    ) -> Dict[str, float]:
        """
        Generate a realistic OHLCV candle.
        
        Args:
            open_price: Opening price
            volatility: Base volatility
            trend_bias: Positive = bullish, negative = bearish
            session_hour: Hour of day for session effects
        
        Returns:
            Dict with open, high, low, close, volume
        """
        # Apply session effects
        vol_mult = self._session_volatility.get(session_hour, 1.0)
        actual_vol = volatility * vol_mult
        
        # Generate returns with bias
        base_return = random.gauss(trend_bias, actual_vol)
        
        # Close price
        close = open_price * (1 + base_return)
        
        # High and low (typically exceed the range)
        range_extension = random.uniform(1.1, 1.5)
        
        if close > open_price:
            # Bullish candle
            high = close + abs(close - open_price) * range_extension * random.uniform(0, 0.5)
            low = open_price - abs(close - open_price) * range_extension * random.uniform(0.1, 0.3)
        else:
            # Bearish candle
            high = open_price + abs(close - open_price) * range_extension * random.uniform(0.1, 0.3)
            low = close - abs(close - open_price) * range_extension * random.uniform(0, 0.5)
        
        # Volume (correlated with volatility and range)
        candle_range = high - low
        base_volume = 1000
        volume = base_volume * vol_mult * (1 + candle_range / open_price * 100)
        
        return {
            "open": round(open_price, 5),
            "high": round(max(high, open_price, close), 5),
            "low": round(min(low, open_price, close), 5),
            "close": round(close, 5),
            "volume": round(volume, 2),
        }
