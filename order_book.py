"""OrderBook - Manages buy/sell orders and performs matching."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum
import uuid
import time

from event_bus import EventBus, EventType, Event


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    PENDING = "pending"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"


@dataclass
class Order:
    trader_id: str
    side: OrderSide
    price: float
    quantity: float
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    timestamp: float = field(default_factory=time.time)
    filled_quantity: float = 0.0
    status: OrderStatus = OrderStatus.PENDING

    @property
    def remaining(self) -> float:
        return self.quantity - self.filled_quantity

    def fill(self, amount: float):
        self.filled_quantity += amount
        if self.filled_quantity >= self.quantity:
            self.status = OrderStatus.FILLED
        else:
            self.status = OrderStatus.PARTIAL


@dataclass
class Trade:
    buy_order_id: str
    sell_order_id: str
    buyer_id: str
    seller_id: str
    price: float
    quantity: float
    timestamp: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])


class OrderBook:
    def __init__(self):
        self.event_bus = EventBus()
        self._buy_orders: List[Order] = []  # sorted by price desc, then time asc
        self._sell_orders: List[Order] = []  # sorted by price asc, then time asc
        self._orders_by_id: Dict[str, Order] = {}
        self._orders_by_trader: Dict[str, List[Order]] = {}
        self._trades: List[Trade] = []
        self._last_trade_price: Optional[float] = None

        self.event_bus.subscribe(EventType.MARKET_CLOSED, self._on_market_closed)

    def submit_order(self, order: Order) -> Tuple[Order, List[Trade]]:
        """Submit an order and attempt to match it. Returns order and any resulting trades."""
        self._orders_by_id[order.id] = order

        if order.trader_id not in self._orders_by_trader:
            self._orders_by_trader[order.trader_id] = []
        self._orders_by_trader[order.trader_id].append(order)

        self.event_bus.emit_simple(
            EventType.ORDER_SUBMITTED,
            {"order_id": order.id, "trader_id": order.trader_id,
             "side": order.side.value, "price": order.price, "quantity": order.quantity},
            source="order_book"
        )

        trades = self._match_order(order)

        if order.status != OrderStatus.FILLED:
            self._insert_order(order)

        return order, trades

    def _insert_order(self, order: Order):
        """Insert order into the appropriate book maintaining sort order."""
        if order.side == OrderSide.BUY:
            self._buy_orders.append(order)
            self._buy_orders.sort(key=lambda o: (-o.price, o.timestamp))
        else:
            self._sell_orders.append(order)
            self._sell_orders.sort(key=lambda o: (o.price, o.timestamp))

    def _match_order(self, order: Order) -> List[Trade]:
        """Match incoming order against existing orders."""
        trades = []

        if order.side == OrderSide.BUY:
            opposite_book = self._sell_orders
            can_match = lambda o, against: o.price >= against.price
        else:
            opposite_book = self._buy_orders
            can_match = lambda o, against: o.price <= against.price

        i = 0
        while i < len(opposite_book) and order.remaining > 0:
            against = opposite_book[i]

            if not can_match(order, against):
                break

            trade_qty = min(order.remaining, against.remaining)
            trade_price = against.price  # price-time priority: use resting order's price

            order.fill(trade_qty)
            against.fill(trade_qty)

            if order.side == OrderSide.BUY:
                trade = Trade(
                    buy_order_id=order.id, sell_order_id=against.id,
                    buyer_id=order.trader_id, seller_id=against.trader_id,
                    price=trade_price, quantity=trade_qty
                )
            else:
                trade = Trade(
                    buy_order_id=against.id, sell_order_id=order.id,
                    buyer_id=against.trader_id, seller_id=order.trader_id,
                    price=trade_price, quantity=trade_qty
                )

            trades.append(trade)
            self._trades.append(trade)
            self._last_trade_price = trade_price

            self.event_bus.emit_simple(
                EventType.ORDER_MATCHED,
                {"trade_id": trade.id, "price": trade_price, "quantity": trade_qty,
                 "buyer_id": trade.buyer_id, "seller_id": trade.seller_id},
                source="order_book"
            )

            self.event_bus.emit_simple(
                EventType.TRADE_EXECUTED,
                {"trade": trade, "buy_order": self._orders_by_id[trade.buy_order_id],
                 "sell_order": self._orders_by_id[trade.sell_order_id]},
                source="order_book"
            )

            if against.status == OrderStatus.FILLED:
                opposite_book.pop(i)
            else:
                i += 1

        return trades

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order by ID."""
        if order_id not in self._orders_by_id:
            return False

        order = self._orders_by_id[order_id]
        if order.status in (OrderStatus.FILLED, OrderStatus.CANCELLED):
            return False

        order.status = OrderStatus.CANCELLED

        if order.side == OrderSide.BUY:
            self._buy_orders = [o for o in self._buy_orders if o.id != order_id]
        else:
            self._sell_orders = [o for o in self._sell_orders if o.id != order_id]

        self.event_bus.emit_simple(
            EventType.ORDER_CANCELLED,
            {"order_id": order_id, "trader_id": order.trader_id},
            source="order_book"
        )

        return True

    def get_best_bid(self) -> Optional[float]:
        """Get highest buy price."""
        return self._buy_orders[0].price if self._buy_orders else None

    def get_best_ask(self) -> Optional[float]:
        """Get lowest sell price."""
        return self._sell_orders[0].price if self._sell_orders else None

    def get_spread(self) -> Optional[float]:
        """Get bid-ask spread."""
        bid, ask = self.get_best_bid(), self.get_best_ask()
        if bid and ask:
            return ask - bid
        return None

    def get_mid_price(self) -> Optional[float]:
        """Get mid-point between best bid and ask."""
        bid, ask = self.get_best_bid(), self.get_best_ask()
        if bid and ask:
            return (bid + ask) / 2
        return None

    def get_trader_orders(self, trader_id: str, active_only: bool = True) -> List[Order]:
        """Get all orders for a trader."""
        orders = self._orders_by_trader.get(trader_id, [])
        if active_only:
            orders = [o for o in orders if o.status in (OrderStatus.PENDING, OrderStatus.PARTIAL)]
        return orders

    def get_depth(self, levels: int = 5) -> Dict[str, List[Tuple[float, float]]]:
        """Get order book depth."""
        bids = {}
        for o in self._buy_orders:
            bids[o.price] = bids.get(o.price, 0) + o.remaining

        asks = {}
        for o in self._sell_orders:
            asks[o.price] = asks.get(o.price, 0) + o.remaining

        return {
            "bids": sorted(bids.items(), reverse=True)[:levels],
            "asks": sorted(asks.items())[:levels]
        }

    def _on_market_closed(self, event: Event):
        """Cancel all open orders when market closes."""
        for order in list(self._buy_orders) + list(self._sell_orders):
            self.cancel_order(order.id)

    @property
    def last_price(self) -> Optional[float]:
        return self._last_trade_price

    @property
    def trade_count(self) -> int:
        return len(self._trades)

    @property
    def volume(self) -> float:
        return sum(t.quantity for t in self._trades)
