import json
import logging
import time
from collections import Counter
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def dumps(value: object) -> str:
    return json.dumps(value, default=json_default, ensure_ascii=False, allow_nan=False)


def log_event(event: str, *, level: int = logging.INFO, **context: object) -> None:
    timestamp = context.get("timestamp_ms", now_ms())
    utc = datetime.fromtimestamp(timestamp // 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    logging.getLogger("arbitrage").log(
        level,
        dumps(
            {
                "event": event,
                "timestamp_ms": timestamp,
                "time_utc": f"{utc}.{timestamp % 1000:03d}Z",
                "level": logging.getLevelName(level),
                **context,
            }
        ),
    )


class Metrics:
    """Counters and last-value gauges; bounded memory for continuous operation."""

    def __init__(self) -> None:
        self.counters: Counter[str] = Counter()
        self.gauges: dict[str, Decimal | int] = {}

    def snapshot(self) -> dict:
        return {"counters": dict(self.counters), "gauges": dict(self.gauges)}
