from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal


def round_step(value: Decimal, step: Decimal, *, up: bool = False) -> Decimal:
    if not value.is_finite() or not step.is_finite() or step <= 0 or value < 0:
        raise ValueError(f"Invalid rounding value={value} step={step}")
    return (value / step).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR) * step


@dataclass(frozen=True)
class BinanceSpec:
    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal
    quantity_precision: int
    price_precision: int
    min_price: Decimal
    max_price: Decimal

    def __post_init__(self) -> None:
        values = (
            self.tick_size,
            self.step_size,
            self.min_qty,
            self.max_qty,
            self.min_notional,
            self.min_price,
            self.max_price,
        )
        if any(not v.is_finite() or v < 0 for v in values):
            raise ValueError(f"Invalid Binance filters symbol={self.symbol}")
        if self.tick_size <= 0 or self.step_size <= 0 or self.max_qty < self.min_qty:
            raise ValueError(f"Invalid Binance increments symbol={self.symbol}")

    def quantity(self, requested: Decimal, price: Decimal) -> Decimal:
        qty = round_step(requested, self.step_size)
        if qty <= 0 or not self.min_qty <= qty <= self.max_qty:
            raise ValueError(f"Binance quantity outside filters symbol={self.symbol} qty={qty}")
        if price * qty < self.min_notional:
            raise ValueError(f"Binance minNotional violation symbol={self.symbol} qty={qty}")
        if (self.min_price and price < self.min_price) or (
            self.max_price and price > self.max_price
        ):
            raise ValueError(f"Binance price outside filters symbol={self.symbol} price={price}")
        return qty


@dataclass(frozen=True)
class MT5Spec:
    symbol: str
    contract_size: Decimal
    volume_min: Decimal
    volume_max: Decimal
    volume_step: Decimal

    def __post_init__(self) -> None:
        if (
            any(
                not v.is_finite() or v <= 0
                for v in (self.contract_size, self.volume_min, self.volume_max, self.volume_step)
            )
            or self.volume_max < self.volume_min
        ):
            raise ValueError(f"Invalid MT5 contract specification symbol={self.symbol}")


@dataclass(frozen=True)
class HedgeCalculator:
    binance: BinanceSpec
    mt5: MT5Spec
    underlying_per_binance_qty: Decimal

    def __post_init__(self) -> None:
        if not self.underlying_per_binance_qty.is_finite() or self.underlying_per_binance_qty <= 0:
            raise ValueError(
                "Binance underlying multiplier must be explicitly verified and positive"
            )

    def lots_to_underlying(self, lots: Decimal) -> Decimal:
        return lots * self.mt5.contract_size

    def binance_to_lots(self, quantity: Decimal) -> Decimal:
        underlying = quantity * self.underlying_per_binance_qty
        lots = underlying / self.mt5.contract_size
        rounded = round_step(lots, self.mt5.volume_step)
        if rounded != lots or not self.mt5.volume_min <= rounded <= self.mt5.volume_max:
            raise ValueError(f"MT5 cannot represent exact hedge quantity={quantity} lots={lots}")
        return rounded
