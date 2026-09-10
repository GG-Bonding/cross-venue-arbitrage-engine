from dataclasses import dataclass
from decimal import Decimal
from typing import Literal


@dataclass(frozen=True)
class ConfirmationResult:
    state: Literal["IDLE", "CONFIRMING", "CONFIRMED"]
    count: int
    duration_ms: int


class EntryConfirmation:
    def __init__(self, min_ticks: int, min_duration_ms: int) -> None:
        if min_ticks < 2 or min_duration_ms < 0:
            raise ValueError("Confirmation requires min_ticks >= 2 and duration >= 0")
        self.min_ticks = min_ticks
        self.min_duration_ms = min_duration_ms
        self.reset()

    def reset(self) -> ConfirmationResult:
        self.start_ms: int | None = None
        self.last_ms: int | None = None
        self.result = ConfirmationResult("IDLE", 0, 0)
        return self.result

    def update(self, edge: Decimal, timestamp_ms: int, *, valid: bool = True) -> ConfirmationResult:
        if not valid or not edge.is_finite() or edge < 0:
            return self.reset()
        if self.last_ms is not None and timestamp_ms <= self.last_ms:
            return self.result
        if self.start_ms is None:
            self.start_ms = timestamp_ms
        self.last_ms = timestamp_ms
        count = self.result.count + 1
        duration = timestamp_ms - self.start_ms
        state = (
            "CONFIRMED"
            if count >= self.min_ticks and duration >= self.min_duration_ms
            else "CONFIRMING"
        )
        self.result = ConfirmationResult(state, count, duration)
        return self.result
