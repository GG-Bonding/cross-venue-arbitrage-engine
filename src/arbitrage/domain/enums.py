from enum import StrEnum


class Direction(StrEnum):
    SHORT_BINANCE = "SHORT_BINANCE"
    LONG_BINANCE = "LONG_BINANCE"


class EntryMode(StrEnum):
    BOTH = "both"
    A = "a"
    B = "b"

    def allows(self, direction: Direction) -> bool:
        return (
            self == self.BOTH
            or direction
            == {
                self.A: Direction.SHORT_BINANCE,
                self.B: Direction.LONG_BINANCE,
            }[self]
        )


class PlacementMode(StrEnum):
    PAUSED = "paused"
    ONCE = "once"
    LOOP = "loop"


class OrderState(StrEnum):
    MAKER_PENDING = "MAKER_PENDING"
    CANCELING = "CANCELING"
    CANCELED = "CANCELED"


class StrategyState(StrEnum):
    IDLE = "IDLE"
    CONFIRMING = "CONFIRMING"
    PLACING_MAKER = "PLACING_MAKER"
    MAKER_PENDING = "MAKER_PENDING"
    CANCELING = "CANCELING"
    SAFE_MODE = "SAFE_MODE"
    ERROR = "ERROR"
