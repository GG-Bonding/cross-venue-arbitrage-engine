import asyncio
import json
import logging
from collections import deque
from dataclasses import asdict
from uuid import UUID

from arbitrage.config import Settings
from arbitrage.domain.enums import Direction, EntryMode, PlacementMode
from arbitrage.market.runtime import run_market
from arbitrage.observability import dumps, now_ms
from arbitrage.risk.quote_guard import QuoteGuard
from arbitrage.strategy.spread import entry_spread
from arbitrage.strategy.trade_metrics import settlement_totals


class EventBuffer(logging.Handler):
    def __init__(self, events: deque):
        super().__init__()
        self.events = events
        self.sequence = 0

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if message.startswith('{"event": "market_snapshot"'):
            return
        try:
            event = json.loads(message)
        except (ValueError, TypeError):
            return  # Only structured arbitrage events belong to this buffer.
        if isinstance(event, dict):
            self.sequence += 1
            self.events.append({**event, "sequence": self.sequence})


def describe_error(exc: BaseException) -> str:
    if isinstance(exc, BaseExceptionGroup):
        return " | ".join(describe_error(child) for child in exc.exceptions)
    return f"{type(exc).__name__}: {exc}"


class MonitorController:
    def __init__(self, settings: Settings, *, runner=None):
        settings.require_runtime()
        self.settings = settings
        self.runner = runner or run_market
        self.status = "STOPPED"
        self.error: str | None = None
        self.engine = None
        self.task: asyncio.Task | None = None
        self.stop_event = asyncio.Event()
        self.events: deque[dict] = deque(maxlen=200)
        self.handler = EventBuffer(self.events)
        self.placement_requests: dict[str, PlacementMode] = {}
        logging.getLogger("arbitrage").addHandler(self.handler)

    def start(self) -> bool:
        if self.task is not None and not self.task.done():
            return False
        self.settings.require_runtime()
        self.status, self.error, self.engine = "STARTING", None, None
        self.stop_event = asyncio.Event()
        self.task = asyncio.create_task(self._run(), name="paper-monitor-session")
        return True

    def _on_engine(self, engine) -> None:
        self.engine = engine
        engine.request_direction_mode(self.settings.entry.direction_mode)
        engine.request_placement(PlacementMode.PAUSED)
        if not self.stop_event.is_set():
            self.status = "RUNNING"

    def select_direction(self, mode: str) -> None:
        selected = EntryMode(mode)
        self.settings = self.settings.model_copy(
            update={"entry": self.settings.entry.model_copy(update={"direction_mode": selected})}
        )
        if self.engine:
            self.engine.request_direction_mode(selected)

    def control_placement(self, mode: str, request_id: str) -> None:
        selected = PlacementMode(mode)
        key = str(UUID(request_id))
        if key in self.placement_requests:
            if self.placement_requests[key] != selected:
                raise ValueError("Request ID already used for another command")
            return
        if self.status != "RUNNING" or self.engine is None:
            raise RuntimeError("Start quote monitoring before creating an order task")
        self.engine.request_placement(selected)
        self.placement_requests[key] = selected
        if len(self.placement_requests) > 200:
            del self.placement_requests[next(iter(self.placement_requests))]

    async def _run(self) -> None:
        try:
            await self.runner(self.settings, stop=self.stop_event, on_engine=self._on_engine)
        except Exception as exc:
            # Session boundary: the runner has unwound its feeds and cleanup before returning.
            self.error = describe_error(exc)
            self.status = "ERROR"
            self.events.append(
                {
                    "event": "session_error",
                    "timestamp_ms": now_ms(),
                    "level": "ERROR",
                    "error": self.error,
                }
            )
        else:
            self.status = "STOPPED"

    async def condition_command(self, action: str, payload) -> dict:
        if self.status != "RUNNING" or self.engine is None or not hasattr(self.engine, "enqueue"):
            raise ValueError("请先启动行情监控")
        future = self.engine.enqueue(action, payload)
        return await asyncio.wait_for(asyncio.shield(future), timeout=10)

    def stop(self) -> None:
        if self.task is not None and not self.task.done():
            self.status = "STOPPING"
            self.stop_event.set()

    async def wait_closed(self) -> None:
        if self.task is not None:
            await self.task

    async def close(self) -> None:
        self.stop()
        try:
            await self.wait_closed()
        finally:
            logging.getLogger("arbitrage").removeHandler(self.handler)

    def snapshot(
        self,
        *,
        now: int | None = None,
        compact: bool = False,
        include_config: bool = True,
    ) -> dict:
        now = now_ms() if now is None else now
        engine = self.engine
        b = engine.binance_quote if engine else None
        m = engine.mt5_quote if engine else None
        active = self.status == "RUNNING"
        config = self.settings
        guard = QuoteGuard(config.market.max_quote_age_ms, config.market.max_quote_skew_ms)
        reason = guard.check(b, m, now) if active else "session_inactive"
        valid = active and reason is None
        directions = {}
        for direction in Direction:
            result = {
                "raw_spread": None,
                "edge": None,
                "state": "IDLE",
                "count": 0,
                "duration_ms": 0,
            }
            if engine:
                result.update(asdict(engine.confirmations[direction].result))
            # Display both observed spreads even when timing rules block entry.
            # Eligibility remains in quotes_valid; this read does not feed the strategy.
            if b is not None and m is not None:
                result.update(asdict(entry_spread(direction, b, m, config.entry.threshold)))
            directions[direction] = result
        view = {
            "timestamp_ms": now,
            "mode": config.mode,
            "status": self.status,
            "conditions": engine.condition_views(now)
            if engine and hasattr(engine, "condition_views")
            else [],
            "active_condition_id": getattr(engine, "active_condition_id", None),
            "error": self.error or getattr(engine, "risk_error", None),
            "state": engine.state if engine else "IDLE",
            "quotes_valid": valid,
            "quote_reason": reason,
            "binance": b,
            "mt5": m,
            "directions": directions,
            "connections": {"binance": active and b is not None, "mt5": active and m is not None},
        }
        if not compact:
            view.update(
                {
                    "accounts": getattr(engine, "accounts", {}),
                    "accounts_updated_ms": getattr(engine, "accounts_updated_ms", None),
                    "accounts_error": getattr(engine, "accounts_error", None),
                    "trades": list(getattr(engine, "trades", {}).values()),
                    "settlement_totals": settlement_totals(getattr(engine, "trades", {}).values()),
                    "entry_selection": {
                        "requested": config.entry.direction_mode,
                        "active": engine.direction_mode if engine and active else None,
                        "pending": bool(
                            engine
                            and active
                            and engine.direction_mode != config.entry.direction_mode
                        ),
                    },
                    "manual_only": True,
                    "placement": {
                        "mode": engine.placement_mode if engine else PlacementMode.PAUSED,
                        "requested": (
                            engine.requested_placement_mode if engine else PlacementMode.PAUSED
                        ),
                        "pending": bool(
                            engine and engine.placement_mode != engine.requested_placement_mode
                        ),
                        "orders_created": engine.orders_created if engine else 0,
                    },
                    "order": engine.order if engine else None,
                    "metrics": (
                        engine.metrics.snapshot() if engine else {"counters": {}, "gauges": {}}
                    ),
                    "events": list(self.events)[-50:],
                }
            )
        if include_config:
            view["config"] = {
                "symbols": {"binance": config.symbol.binance, "mt5": config.symbol.mt5},
                "entry_threshold": config.entry.threshold,
                "direction_mode": config.entry.direction_mode,
                "cancel_threshold": config.maker.cancel_threshold,
                "min_ticks": config.entry.confirmation.min_ticks,
                "min_duration_ms": config.entry.confirmation.min_duration_ms,
                "max_quote_age_ms": config.market.max_quote_age_ms,
                "max_quote_skew_ms": config.market.max_quote_skew_ms,
                "max_pending_ms": config.maker.max_pending_ms,
                "binance_qty": config.trading.binance_qty,
                "mt5_tick_time_offset_minutes": config.mt5.tick_time_offset_minutes,
                "live_max_qty": config.live.max_binance_qty,
            }
        return json.loads(dumps(view))
