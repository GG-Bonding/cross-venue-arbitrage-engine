from dataclasses import dataclass
from decimal import Decimal

from arbitrage.domain.enums import Direction
from arbitrage.domain.quote import Quote


@dataclass(frozen=True)
class Spread:
    raw_spread: Decimal
    edge: Decimal


def pending_spread(direction: Direction, maker_price: Decimal, mt5: Quote) -> Decimal:
    if direction == Direction.SHORT_BINANCE:
        return maker_price - mt5.ask
    if direction == Direction.LONG_BINANCE:
        return mt5.bid - maker_price
    raise ValueError(f"Unknown direction={direction}")


def entry_spread(direction: Direction, binance: Quote, mt5: Quote, threshold: Decimal) -> Spread:
    price = binance.ask if direction == Direction.SHORT_BINANCE else binance.bid
    raw = pending_spread(direction, price, mt5)
    return Spread(raw, raw - threshold)
