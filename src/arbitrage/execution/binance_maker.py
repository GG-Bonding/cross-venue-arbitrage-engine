from decimal import Decimal
from uuid import uuid4

from arbitrage.domain.enums import Direction, OrderState
from arbitrage.domain.order import MakerOrder
from arbitrage.domain.quote import Quote
from arbitrage.domain.specs import BinanceSpec, round_step


class PaperMaker:
    """Local resting orders only. No network, assumed fills, or MT5 orders."""

    def __init__(self, spec: BinanceSpec):
        self.spec = spec

    def place(self, direction: Direction, quote: Quote, quantity: Decimal, now: int) -> MakerOrder:
        sell = direction == Direction.SHORT_BINANCE
        price = round_step(quote.ask if sell else quote.bid, self.spec.tick_size, up=sell)
        if (sell and price <= quote.bid) or (not sell and price >= quote.ask):
            raise ValueError(f"Post only order would immediately match direction={direction}")
        qty = self.spec.quantity(quantity, price)
        return MakerOrder(uuid4().hex, direction, price, qty, now)

    def request_cancel(self, order: MakerOrder, now: int) -> None:
        if order.state != OrderState.MAKER_PENDING:
            raise ValueError(
                f"Invalid cancel request order_id={order.order_id} state={order.state}"
            )
        order.state = OrderState.CANCELING
        order.cancel_requested_at_ms = now

    def ack_cancel(self, order: MakerOrder) -> None:
        if order.state != OrderState.CANCELING:
            raise ValueError(f"Unexpected cancel ack order_id={order.order_id} state={order.state}")
        order.state = OrderState.CANCELED


class CancelPolicy:
    def __init__(self, threshold: Decimal, confirm_ms: int, max_pending_ms: int):
        self.threshold = threshold
        self.confirm_ms = confirm_ms
        self.max_pending_ms = max_pending_ms
        self.invalid_since: int | None = None

    def update(self, spread: Decimal, now: int, created_at: int) -> str | None:
        if now - created_at > self.max_pending_ms:
            return "pending_timeout"
        if spread >= self.threshold:
            self.invalid_since = None
            return None
        if self.invalid_since is None:
            self.invalid_since = now
        if now - self.invalid_since >= self.confirm_ms:
            return "spread_invalid"
        return None
