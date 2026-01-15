"""Trader - Autonomous trading agents with different strategies."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
from enum import Enum
from abc import ABC, abstractmethod
import random
import uuid

from event_bus import EventBus, EventType, Event
from order_book import OrderBook, Order, OrderSide, Trade


class TraderStrategy(Enum):
    RANDOM = "random"
    TREND_FOLLOWER = "trend_follower"
    MEAN_REVERSION = "mean_reversion"
    MARKET_MAKER = "market_maker"


@dataclass
class Portfolio:
    cash: float = 10000.0
    shares: float = 0.0

    @property
    def total_value(self) -> float:
        return self.cash + self.shares  # simplified: assumes price of 1

    def value_at(self, price: float) -> float:
        return self.cash + (self.shares * price)


class Trader:
    def __init__(self, name: str, strategy: TraderStrategy, order_book: OrderBook,
                 initial_cash: float = 10000.0, initial_shares: float = 100.0):
        self.id = str(uuid.uuid4())[:8]
        self.name = name
        self.strategy = strategy
        self.order_book = order_book
        self.event_bus = EventBus()

        self.portfolio = Portfolio(cash=initial_cash, shares=initial_shares)
        self._price_history: List[float] = []
        self._trade_history: List[Trade] = []
        self._pending_orders: Dict[str, Order] = {}
        self._active = True

        self._setup_event_handlers()

        self.event_bus.emit_simple(
            EventType.TRADER_REGISTERED,
            {"trader_id": self.id, "name": self.name, "strategy": self.strategy.value},
            source=f"trader:{self.id}"
        )

    def _setup_event_handlers(self):
        """Subscribe to relevant events."""
        self.event_bus.subscribe(EventType.PRICE_UPDATED, self._on_price_update)
        self.event_bus.subscribe(EventType.TRADE_EXECUTED, self._on_trade_executed)
        self.event_bus.subscribe(EventType.ORDER_CANCELLED, self._on_order_cancelled)
        self.event_bus.subscribe(EventType.MARKET_OPENED, self._on_market_opened)
        self.event_bus.subscribe(EventType.MARKET_CLOSED, self._on_market_closed)

    def _on_price_update(self, event: Event):
        """React to price updates based on strategy."""
        if not self._active:
            return

        price = event.data.get("price")
        if price:
            self._price_history.append(price)
            self._maybe_trade(price)

    def _on_trade_executed(self, event: Event):
        """Handle trade execution - update portfolio if we're involved."""
        trade: Trade = event.data.get("trade")
        if not trade:
            return

        if trade.buyer_id == self.id:
            self.portfolio.cash -= trade.price * trade.quantity
            self.portfolio.shares += trade.quantity
            self._trade_history.append(trade)
            self._emit_balance_change(trade.price)

        elif trade.seller_id == self.id:
            self.portfolio.cash += trade.price * trade.quantity
            self.portfolio.shares -= trade.quantity
            self._trade_history.append(trade)
            self._emit_balance_change(trade.price)

    def _on_order_cancelled(self, event: Event):
        """Remove cancelled orders from pending."""
        order_id = event.data.get("order_id")
        if order_id in self._pending_orders:
            del self._pending_orders[order_id]

    def _on_market_opened(self, event: Event):
        self._active = True

    def _on_market_closed(self, event: Event):
        self._active = False
        self._pending_orders.clear()

    def _emit_balance_change(self, current_price: float):
        self.event_bus.emit_simple(
            EventType.BALANCE_CHANGED,
            {"trader_id": self.id, "cash": self.portfolio.cash,
             "shares": self.portfolio.shares, "total_value": self.portfolio.value_at(current_price)},
            source=f"trader:{self.id}"
        )

    def _maybe_trade(self, current_price: float):
        """Decide whether to trade based on strategy."""
        decision = self._get_strategy_decision(current_price)
        if decision:
            side, price, quantity = decision
            self.place_order(side, price, quantity)

    def _get_strategy_decision(self, current_price: float) -> Optional[tuple]:
        """Get trading decision based on strategy."""
        if self.strategy == TraderStrategy.RANDOM:
            return self._random_strategy(current_price)
        elif self.strategy == TraderStrategy.TREND_FOLLOWER:
            return self._trend_follower_strategy(current_price)
        elif self.strategy == TraderStrategy.MEAN_REVERSION:
            return self._mean_reversion_strategy(current_price)
        elif self.strategy == TraderStrategy.MARKET_MAKER:
            return self._market_maker_strategy(current_price)
        return None

    def _random_strategy(self, price: float) -> Optional[tuple]:
        """Random trading - sometimes buy, sometimes sell."""
        if random.random() > 0.3:  # 70% chance to skip
            return None

        side = OrderSide.BUY if random.random() > 0.5 else OrderSide.SELL

        if side == OrderSide.BUY:
            max_qty = self.portfolio.cash / price
            if max_qty < 1:
                return None
            qty = random.uniform(1, min(10, max_qty))
            order_price = price * random.uniform(0.98, 1.02)
        else:
            if self.portfolio.shares < 1:
                return None
            qty = random.uniform(1, min(10, self.portfolio.shares))
            order_price = price * random.uniform(0.98, 1.02)

        return (side, order_price, qty)

    def _trend_follower_strategy(self, price: float) -> Optional[tuple]:
        """Follow the trend - buy when rising, sell when falling."""
        if len(self._price_history) < 5:
            return None

        recent = self._price_history[-5:]
        trend = (recent[-1] - recent[0]) / recent[0]

        if abs(trend) < 0.01:  # no significant trend
            return None

        if trend > 0:  # uptrend - buy
            max_qty = self.portfolio.cash / price
            if max_qty < 1:
                return None
            qty = min(5, max_qty) * min(1, abs(trend) * 10)
            return (OrderSide.BUY, price * 1.001, qty)
        else:  # downtrend - sell
            if self.portfolio.shares < 1:
                return None
            qty = min(5, self.portfolio.shares) * min(1, abs(trend) * 10)
            return (OrderSide.SELL, price * 0.999, qty)

    def _mean_reversion_strategy(self, price: float) -> Optional[tuple]:
        """Mean reversion - buy when below average, sell when above."""
        if len(self._price_history) < 10:
            return None

        avg = sum(self._price_history[-20:]) / len(self._price_history[-20:])
        deviation = (price - avg) / avg

        if abs(deviation) < 0.02:  # within 2% of mean
            return None

        if deviation < 0:  # below average - buy
            max_qty = self.portfolio.cash / price
            if max_qty < 1:
                return None
            qty = min(5, max_qty) * min(1, abs(deviation) * 5)
            return (OrderSide.BUY, price, qty)
        else:  # above average - sell
            if self.portfolio.shares < 1:
                return None
            qty = min(5, self.portfolio.shares) * min(1, abs(deviation) * 5)
            return (OrderSide.SELL, price, qty)

    def _market_maker_strategy(self, price: float) -> Optional[tuple]:
        """Market maker - place both buy and sell orders around current price."""
        if len(self._pending_orders) >= 4:
            return None

        spread = 0.02

        if random.random() > 0.5:
            max_qty = self.portfolio.cash / price
            if max_qty < 1:
                return None
            qty = random.uniform(1, min(5, max_qty))
            return (OrderSide.BUY, price * (1 - spread/2), qty)
        else:
            if self.portfolio.shares < 1:
                return None
            qty = random.uniform(1, min(5, self.portfolio.shares))
            return (OrderSide.SELL, price * (1 + spread/2), qty)

    def place_order(self, side: OrderSide, price: float, quantity: float) -> Optional[Order]:
        """Place an order."""
        if not self._active:
            return None

        if side == OrderSide.BUY and self.portfolio.cash < price * quantity:
            return None
        if side == OrderSide.SELL and self.portfolio.shares < quantity:
            return None

        order = Order(
            trader_id=self.id,
            side=side,
            price=round(price, 2),
            quantity=round(quantity, 2)
        )

        self._pending_orders[order.id] = order
        submitted_order, trades = self.order_book.submit_order(order)

        if submitted_order.status.value in ("filled", "cancelled"):
            if submitted_order.id in self._pending_orders:
                del self._pending_orders[submitted_order.id]

        return submitted_order

    def cancel_all_orders(self):
        """Cancel all pending orders."""
        for order_id in list(self._pending_orders.keys()):
            self.order_book.cancel_order(order_id)

    def get_pnl(self, current_price: float) -> float:
        """Calculate profit/loss."""
        initial_value = 10000.0 + (100.0 * current_price)
        current_value = self.portfolio.value_at(current_price)
        return current_value - initial_value

    def __repr__(self):
        return f"Trader({self.name}, {self.strategy.value}, cash={self.portfolio.cash:.2f}, shares={self.portfolio.shares:.2f})"
