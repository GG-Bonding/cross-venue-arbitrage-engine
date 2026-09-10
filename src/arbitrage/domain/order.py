from dataclasses import dataclass
from decimal import Decimal

from arbitrage.domain.enums import Direction, OrderState


@dataclass
class MakerOrder:
    order_id: str
    direction: Direction
    price: Decimal
    quantity: Decimal
    created_at_ms: int
    state: OrderState = OrderState.MAKER_PENDING
    filled_qty: Decimal = Decimal("0")
    cancel_requested_at_ms: int | None = None
    time_in_force: str = "GTX"
    mode: str = "paper"
