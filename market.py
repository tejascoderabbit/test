"""Market - Central exchange that coordinates all trading activity."""

from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Enum
import time
import random

from event_bus import EventBus, EventType, Event
from order_book import OrderBook
from trader import Trader, TraderStrategy


class MarketState(Enum):
    CLOSED = "closed"
    PRE_MARKET = "pre_market"
    OPEN = "open"
    POST_MARKET = "post_market"


@dataclass
class MarketStats:
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    trade_count: int


class Market:
    def __init__(self, name: str = "SimpleExchange", initial_price: float = 100.0):
        self.name = name
        self.event_bus = EventBus()
        self.order_book = OrderBook()

        self._state = MarketState.CLOSED
        self._current_price = initial_price
        self._price_history: List[float] = [initial_price]
        self._traders: Dict[str, Trader] = {}
        self._tick_count = 0

        self._session_stats: Optional[MarketStats] = None
        self._volatility = 0.02

        self._setup_event_handlers()

    def _setup_event_handlers(self):
        """Subscribe to events that affect market state."""
        self.event_bus.subscribe(EventType.TRADE_EXECUTED, self._on_trade_executed)
        self.event_bus.subscribe(EventType.TRADER_REGISTERED, self._on_trader_registered)
        self.event_bus.subscribe(EventType.ORDER_MATCHED, self._on_order_matched)

    def _on_trade_executed(self, event: Event):
        """Update market price when trades execute."""
        trade = event.data.get("trade")
        if trade:
            old_price = self._current_price
            self._current_price = trade.price
            self._price_history.append(self._current_price)

            if old_price != self._current_price:
                self.event_bus.emit_simple(
                    EventType.PRICE_UPDATED,
                    {"price": self._current_price, "previous_price": old_price,
                     "change": self._current_price - old_price,
                     "change_pct": (self._current_price - old_price) / old_price * 100},
                    source="market"
                )

    def _on_trader_registered(self, event: Event):
        """Track registered traders."""
        trader_id = event.data.get("trader_id")
        if trader_id:
            print(f"[Market] Trader registered: {event.data.get('name')} ({event.data.get('strategy')})")

    def _on_order_matched(self, event: Event):
        """Log order matches."""
        pass  # Could add logging or analytics here

    def register_trader(self, trader: Trader):
        """Register a trader with the market."""
        self._traders[trader.id] = trader

    def create_trader(self, name: str, strategy: TraderStrategy,
                      initial_cash: float = 10000.0, initial_shares: float = 100.0) -> Trader:
        """Create and register a new trader."""
        trader = Trader(name, strategy, self.order_book, initial_cash, initial_shares)
        self.register_trader(trader)
        return trader

    def open_market(self):
        """Open the market for trading."""
        if self._state != MarketState.CLOSED:
            return

        self._state = MarketState.OPEN
        self._session_stats = MarketStats(
            open_price=self._current_price,
            high_price=self._current_price,
            low_price=self._current_price,
            close_price=self._current_price,
            volume=0,
            trade_count=0
        )

        print(f"\n{'='*50}")
        print(f"[Market] {self.name} OPENED at {self._current_price:.2f}")
        print(f"{'='*50}\n")

        self.event_bus.emit_simple(
            EventType.MARKET_OPENED,
            {"price": self._current_price, "market_name": self.name},
            source="market"
        )

    def close_market(self):
        """Close the market."""
        if self._state != MarketState.OPEN:
            return

        self._state = MarketState.CLOSED

        if self._session_stats:
            self._session_stats.close_price = self._current_price
            self._session_stats.volume = self.order_book.volume
            self._session_stats.trade_count = self.order_book.trade_count

        self.event_bus.emit_simple(
            EventType.MARKET_CLOSED,
            {"price": self._current_price, "stats": self._session_stats},
            source="market"
        )

        print(f"\n{'='*50}")
        print(f"[Market] {self.name} CLOSED at {self._current_price:.2f}")
        if self._session_stats:
            print(f"  Open: {self._session_stats.open_price:.2f}")
            print(f"  High: {self._session_stats.high_price:.2f}")
            print(f"  Low: {self._session_stats.low_price:.2f}")
            print(f"  Volume: {self._session_stats.volume:.2f}")
            print(f"  Trades: {self._session_stats.trade_count}")
        print(f"{'='*50}\n")

    def tick(self):
        """Advance market by one tick - simulates price movement and triggers trading."""
        if self._state != MarketState.OPEN:
            return

        self._tick_count += 1

        # Simulate natural price movement
        change = random.gauss(0, self._volatility) * self._current_price
        new_price = max(1.0, self._current_price + change)

        old_price = self._current_price
        self._current_price = round(new_price, 2)
        self._price_history.append(self._current_price)

        # Update session stats
        if self._session_stats:
            self._session_stats.high_price = max(self._session_stats.high_price, self._current_price)
            self._session_stats.low_price = min(self._session_stats.low_price, self._current_price)

        # Emit price update
        self.event_bus.emit_simple(
            EventType.PRICE_UPDATED,
            {"price": self._current_price, "previous_price": old_price,
             "change": self._current_price - old_price,
             "change_pct": (self._current_price - old_price) / old_price * 100,
             "tick": self._tick_count},
            source="market"
        )

    def run_session(self, ticks: int = 100, delay: float = 0.0):
        """Run a complete trading session."""
        self.open_market()

        for i in range(ticks):
            self.tick()
            if delay > 0:
                time.sleep(delay)

            # Progress update every 20%
            if (i + 1) % (ticks // 5) == 0:
                pct = (i + 1) / ticks * 100
                print(f"[Market] Progress: {pct:.0f}% | Price: {self._current_price:.2f} | "
                      f"Trades: {self.order_book.trade_count}")

        self.close_market()
        return self._session_stats

    def get_leaderboard(self) -> List[tuple]:
        """Get traders sorted by P&L."""
        results = []
        for trader in self._traders.values():
            pnl = trader.get_pnl(self._current_price)
            results.append((trader.name, trader.strategy.value, pnl,
                           trader.portfolio.cash, trader.portfolio.shares))
        return sorted(results, key=lambda x: x[2], reverse=True)

    def print_leaderboard(self):
        """Print the trader leaderboard."""
        leaderboard = self.get_leaderboard()

        print(f"\n{'='*70}")
        print(f"{'LEADERBOARD':^70}")
        print(f"{'='*70}")
        print(f"{'Rank':<6}{'Name':<15}{'Strategy':<18}{'P&L':>12}{'Cash':>10}{'Shares':>9}")
        print(f"{'-'*70}")

        for i, (name, strategy, pnl, cash, shares) in enumerate(leaderboard, 1):
            pnl_str = f"+{pnl:.2f}" if pnl >= 0 else f"{pnl:.2f}"
            print(f"{i:<6}{name:<15}{strategy:<18}{pnl_str:>12}{cash:>10.2f}{shares:>9.2f}")

        print(f"{'='*70}\n")

    @property
    def price(self) -> float:
        return self._current_price

    @property
    def state(self) -> MarketState:
        return self._state

    @property
    def spread(self) -> Optional[float]:
        return self.order_book.get_spread()


def main():
    """Run a market simulation."""
    # Reset the event bus singleton for clean state
    EventBus().reset()

    # Create market
    market = Market("CryptoExchange", initial_price=100.0)

    # Create diverse traders
    traders = [
        market.create_trader("Alice", TraderStrategy.TREND_FOLLOWER),
        market.create_trader("Bob", TraderStrategy.MEAN_REVERSION),
        market.create_trader("Charlie", TraderStrategy.MARKET_MAKER),
        market.create_trader("Diana", TraderStrategy.RANDOM),
        market.create_trader("Eve", TraderStrategy.TREND_FOLLOWER),
        market.create_trader("Frank", TraderStrategy.MEAN_REVERSION),
    ]

    # Add middleware to log high-value trades
    def log_big_trades(event: Event) -> Event:
        if event.type == EventType.ORDER_MATCHED:
            qty = event.data.get("quantity", 0)
            if qty > 5:
                print(f"  [BIG TRADE] {qty:.2f} units @ {event.data.get('price', 0):.2f}")
        return event

    market.event_bus.add_middleware(log_big_trades)

    # Run simulation
    print("\nStarting market simulation...")
    print(f"Traders: {len(traders)}")
    print(f"Initial price: {market.price:.2f}")

    stats = market.run_session(ticks=200)

    # Show results
    market.print_leaderboard()

    # Show event stats
    history = market.event_bus.get_history()
    event_counts = {}
    for event in history:
        event_counts[event.type.value] = event_counts.get(event.type.value, 0) + 1

    print("Event Summary:")
    for event_type, count in sorted(event_counts.items()):
        print(f"  {event_type}: {count}")

    return market, traders


if __name__ == "__main__":
    main()
