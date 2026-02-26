"""EventBus - Central nervous system for inter-component communication."""

from typing import Callable, Dict, List, Any
from dataclasses import dataclass, field
from enum import Enum
import time


class EventType(Enum):
    ORDER_SUBMITTED = "order_submitted"
    ORDER_MATCHED = "order_matched"
    ORDER_CANCELLED = "order_cancelled"
    PRICE_UPDATED = "price_updated"
    TRADE_EXECUTED = "trade_executed"
    MARKET_OPENED = "market_opened"
    MARKET_CLOSED = "market_closed"
    TRADER_REGISTERED = "trader_registered"
    BALANCE_CHANGED = "balance_changed"


@dataclass
class Event:
    type: EventType
    data: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    source: str = "unknown"


class EventBus:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._subscribers: Dict[EventType, List[Callable]] = {et: [] for et in EventType}
        self._event_history: List[Event] = []
        self._middleware: List[Callable] = []
        self._paused = False
        self._initialized = True

    def subscribe(self, event_type: EventType, callback: Callable) -> Callable:
        """Subscribe to an event type. Returns unsubscribe function."""
        self._subscribers[event_type].append(callback)
        def unsubscribe():
            self._subscribers[event_type].remove(callback)
        return unsubscribe

    def subscribe_many(self, event_types: List[EventType], callback: Callable) -> Callable:
        """Subscribe to multiple event types with one callback."""
        unsubscribers = [self.subscribe(et, callback) for et in event_types]
        def unsubscribe_all():
            for unsub in unsubscribers:
                unsub()
        return unsubscribe_all

    def add_middleware(self, middleware: Callable) -> None:
        """Add middleware that can transform or block events."""
        self._middleware.append(middleware)

    def emit(self, event: Event) -> bool:
        """Emit an event to all subscribers. Returns False if blocked by middleware."""
        if self._paused:
            return False

        for mw in self._middleware:
            event = mw(event)
            if event is None:
                return False

        self._event_history.append(event)

        for callback in self._subscribers[event.type]:
            try:
                callback(event)
            except Exception as e:
                print(f"[EventBus] Error in subscriber: {e}")

        return True

    def emit_simple(self, event_type: EventType, data: Dict[str, Any], source: str = "unknown") -> bool:
        """Convenience method to emit without creating Event object."""
        return self.emit(Event(type=event_type, data=data, source=source))

    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False

    def get_history(self, event_type: EventType = None, limit: int = None) -> List[Event]:
        """Get event history, optionally filtered by type."""
        history = self._event_history
        if event_type:
            history = [e for e in history if e.type == event_type]
        if limit:
            history = history[-limit:]
        return history

    def clear_history(self):
        self._event_history = []

    def reset(self):
        """Full reset - useful for testing."""
        self._subscribers = {et: [] for et in EventType}
        self._event_history = []
        self._middleware = []
        self._paused = False
