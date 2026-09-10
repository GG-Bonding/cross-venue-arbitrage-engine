from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Quote:
    bid: Decimal
    ask: Decimal
    bid_qty: Decimal | None
    ask_qty: Decimal | None
    exchange_ts_ms: int
    local_ts_ms: int
    raw_exchange_ts_ms: int | None = None

    def __post_init__(self) -> None:
        for name in ("bid", "ask", "bid_qty", "ask_qty"):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, Decimal) or not value.is_finite() or value < 0
            ):
                raise ValueError(f"Invalid quote {name}={value!r}")
        if self.bid <= 0 or self.ask <= 0 or self.bid > self.ask:
            raise ValueError(f"Invalid quote bid={self.bid} ask={self.ask}")
        if self.exchange_ts_ms < 0 or self.local_ts_ms < 0:
            raise ValueError("Quote timestamps must be nonnegative UTC milliseconds")
        if self.raw_exchange_ts_ms is not None and self.raw_exchange_ts_ms < 0:
            raise ValueError("Raw quote timestamp must be nonnegative")
