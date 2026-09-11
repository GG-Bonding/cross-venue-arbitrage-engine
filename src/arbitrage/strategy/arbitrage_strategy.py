from dataclasses import asdict

from arbitrage.config import Settings
from arbitrage.domain.enums import Direction, EntryMode, OrderState, PlacementMode, StrategyState
from arbitrage.domain.order import MakerOrder
from arbitrage.domain.quote import Quote
from arbitrage.domain.specs import BinanceSpec
from arbitrage.execution.binance_maker import CancelPolicy, PaperMaker
from arbitrage.observability import Metrics, log_event
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.risk.quote_guard import QuoteGuard
from arbitrage.strategy.confirmation import EntryConfirmation
from arbitrage.strategy.spread import entry_spread, pending_spread


class ArbitrageStrategy:
    """Single paper order slot; all calls serialized by the event consumer."""

    def __init__(self, settings: Settings, spec: BinanceSpec, repo: SQLiteRepository):
        settings.require_paper()
        self.settings = settings
        self.repo = repo
        self.maker = PaperMaker(spec)
        self.metrics = Metrics()
        self.guard = QuoteGuard(
            settings.market.max_quote_age_ms, settings.market.max_quote_skew_ms, self.metrics
        )
        c = settings.entry.confirmation
        self.confirmations = {
            d: EntryConfirmation(c.min_ticks, c.min_duration_ms) for d in Direction
        }
        self.state = StrategyState.SAFE_MODE
        self.order: MakerOrder | None = None
        self.cancel_policy: CancelPolicy | None = None
        self.binance_quote: Quote | None = None
        self.mt5_quote: Quote | None = None
        self.last_key: tuple | None = None
        self.last_sample_ms: int | None = None
        self.last_guard_reason: str | None = None
        self.direction_mode = settings.entry.direction_mode
        self.requested_direction_mode = self.direction_mode
        # CLI keeps its automatic behavior; the web controller pauses before feeds start.
        self.placement_mode = PlacementMode.LOOP
        self.requested_placement_mode = self.placement_mode
        self.once_remaining = 0
        self.orders_created = 0

    def request_placement(self, mode: PlacementMode) -> None:
        mode = PlacementMode(mode)
        if mode != PlacementMode.PAUSED and (
            self.state in (StrategyState.SAFE_MODE, StrategyState.ERROR)
            or self.order is not None
            or self.placement_mode != PlacementMode.PAUSED
            or self.requested_placement_mode != PlacementMode.PAUSED
        ):
            raise RuntimeError("Stop the existing order task before starting another")
        self.requested_placement_mode = mode

    def placement_enabled(self) -> bool:
        return self.placement_mode == self.requested_placement_mode and (
            self.placement_mode == PlacementMode.LOOP
            or (self.placement_mode == PlacementMode.ONCE and self.once_remaining > 0)
        )

    def request_direction_mode(self, mode: EntryMode) -> None:
        # The event consumer applies the request; HTTP handlers never mutate orders.
        self.requested_direction_mode = EntryMode(mode)

    async def start(self, now: int) -> None:
        unfinished = await self.repo.unfinished_orders()
        self.state = StrategyState.SAFE_MODE if unfinished else StrategyState.IDLE
        await self._record(
            "paper_reconciliation", now, unfinished_count=len(unfinished), state=self.state
        )

    async def _record(self, event: str, now: int, **payload) -> None:
        await self.repo.event("strategy_events", event, now, payload)
        log_event(event, timestamp_ms=now, **payload)

    async def _reset_confirmations(self, now: int, reason: str) -> None:
        for direction, confirmation in self.confirmations.items():
            if confirmation.result.count:
                await self._record(
                    "entry_confirmation_reset", now, direction=direction, reason=reason
                )
            confirmation.reset()
        if self.state == StrategyState.CONFIRMING:
            self.state = StrategyState.IDLE

    async def _cancel(self, now: int, reason: str) -> None:
        self.maker.request_cancel(self.order, now)
        self.state = StrategyState.CANCELING
        await self.repo.save_order(
            self.order,
            "maker_cancel_requested",
            now,
            reason=reason,
            cancel_evidence={
                "version": 1,
                "binance": self.binance_quote,
                "mt5": self.mt5_quote,
                "direction_mode": self.direction_mode,
                "placement_mode": self.placement_mode,
                "maker": self.settings.maker.model_dump(),
                "max_quote_age_ms": self.settings.market.max_quote_age_ms,
                "max_quote_skew_ms": self.settings.market.max_quote_skew_ms,
                "invalid_since_ms": self.cancel_policy.invalid_since
                if self.cancel_policy
                else None,
            },
        )
        log_event(
            "maker_cancel_requested", timestamp_ms=now, order_id=self.order.order_id, reason=reason
        )

    async def on_timer(self, now: int) -> None:
        # Timers enforce age/timeout but never count as market ticks.
        if self.placement_mode != self.requested_placement_mode:
            self.placement_mode = self.requested_placement_mode
            self.once_remaining = int(self.placement_mode == PlacementMode.ONCE)
            await self._reset_confirmations(now, "placement_changed")
            self.last_key = None
            await self._record("placement_mode_changed", now, placement=self.placement_mode)
        if (
            self.placement_mode == PlacementMode.PAUSED
            and self.order
            and self.order.state == OrderState.MAKER_PENDING
        ):
            await self._cancel(now, "placement_stopped")
            return
        if self.direction_mode != self.requested_direction_mode:
            self.direction_mode = self.requested_direction_mode
            await self._reset_confirmations(now, "direction_changed")
            self.last_key = None
            await self._record("entry_direction_changed", now, mode=self.direction_mode)
        if (
            self.order
            and self.order.state == OrderState.MAKER_PENDING
            and not self.direction_mode.allows(self.order.direction)
        ):
            await self._cancel(now, "direction_disabled")
            return
        if self.state in (StrategyState.SAFE_MODE, StrategyState.ERROR):
            return
        order = self.order
        if order and order.state == OrderState.CANCELING:
            if now - order.cancel_requested_at_ms >= self.settings.maker.paper_cancel_latency_ms:
                self.maker.ack_cancel(order)
                await self.repo.save_order(order, "maker_order_canceled", now)
                self.metrics.gauges["maker_cancel_latency_ms"] = now - order.cancel_requested_at_ms
                self.metrics.gauges["maker_order_pending_ms"] = now - order.created_at_ms
                log_event("maker_order_canceled", timestamp_ms=now, order_id=order.order_id)
                self.order = None
                self.state = StrategyState.IDLE
                await self._reset_confirmations(now, "order_finished")
                if self.placement_mode == PlacementMode.ONCE and self.once_remaining == 0:
                    self.placement_mode = self.requested_placement_mode = PlacementMode.PAUSED
                    await self._record("placement_once_completed", now)
            return
        reason = self.guard.check(self.binance_quote, self.mt5_quote, now)
        if reason:
            await self._reset_confirmations(now, reason)
            if reason != self.last_guard_reason:
                await self.repo.event("risk_events", reason, now, {"state": self.state})
                log_event(reason, timestamp_ms=now, state=self.state)
            self.last_guard_reason = reason
            if order:
                await self._cancel(now, reason)
            return
        self.last_guard_reason = None
        if order:
            spread = pending_spread(order.direction, order.price, self.mt5_quote)
            self.metrics.gauges["pending_spread"] = spread
            self.metrics.gauges["cancel_edge"] = spread - self.settings.maker.cancel_threshold
            self.metrics.gauges["maker_order_pending_ms"] = now - order.created_at_ms
            reason = self.cancel_policy.update(spread, now, order.created_at_ms)
            if reason:
                await self._cancel(now, reason)

    async def on_quotes(self, binance: Quote, mt5: Quote, now: int) -> dict:
        self.binance_quote, self.mt5_quote = binance, mt5
        await self.on_timer(now)
        valid = self.guard.check(binance, mt5, now) is None
        key = (
            binance.exchange_ts_ms,
            binance.bid,
            binance.ask,
            binance.bid_qty,
            binance.ask_qty,
            mt5.exchange_ts_ms,
            mt5.bid,
            mt5.ask,
        )
        is_new = key != self.last_key
        self.last_key = key
        signals = {}
        if valid:
            for direction in Direction:
                spread = entry_spread(direction, binance, mt5, self.settings.entry.threshold)
                signals[direction] = asdict(spread)
                self.metrics.gauges[f"entry_edge_{direction}"] = spread.edge
        if (
            valid
            and is_new
            and self.order is None
            and self.placement_enabled()
            and self.state in (StrategyState.IDLE, StrategyState.CONFIRMING)
        ):
            await self._confirm_and_place(signals, now)
        snapshot = {
            "timestamp_ms": now,
            "mode": "paper",
            "state": self.state,
            "quotes_valid": valid,
            "binance": binance,
            "mt5": mt5,
            "directions": {
                d: {**signals.get(d, {}), **asdict(c.result)} for d, c in self.confirmations.items()
            },
            "order": self.order,
        }
        if (
            self.last_sample_ms is None
            or now - self.last_sample_ms >= self.settings.database.quote_sample_ms
        ):
            await self.repo.event("quotes_sample", "quote_snapshot", now, snapshot)
            self.last_sample_ms = now
        log_event("market_snapshot", **snapshot)
        return snapshot

    async def _confirm_and_place(self, signals: dict, now: int) -> None:
        candidates = []
        for direction, confirmation in self.confirmations.items():
            if not self.direction_mode.allows(direction):
                confirmation.reset()
                continue
            previous = confirmation.result
            result = confirmation.update(signals[direction]["edge"], now)
            if result.state != previous.state:
                event = {
                    "IDLE": "entry_confirmation_reset",
                    "CONFIRMING": "entry_confirmation_start",
                    "CONFIRMED": "entry_confirmed",
                }[result.state]
                await self._record(event, now, direction=direction, **asdict(result))
            if result.state == "CONFIRMED":
                candidates.append(direction)
                self.metrics.gauges["entry_confirmation_duration_ms"] = result.duration_ms
        self.state = (
            StrategyState.CONFIRMING
            if any(c.result.count for c in self.confirmations.values())
            else StrategyState.IDLE
        )
        if not candidates:
            return
        direction = max(candidates, key=lambda d: signals[d]["edge"])
        # A control request can arrive while confirmation events are being persisted.
        if not self.placement_enabled() or not self.requested_direction_mode.allows(direction):
            return
        self.state = StrategyState.PLACING_MAKER
        order = self.maker.place(
            direction, self.binance_quote, self.settings.trading.binance_qty, now
        )
        await self.repo.save_order(
            order,
            "maker_order_created",
            now,
            entry_evidence={
                "version": 1,
                "binance": self.binance_quote,
                "mt5": self.mt5_quote,
                "spread": signals[direction],
                "confirmation": asdict(self.confirmations[direction].result),
                "direction_mode": self.direction_mode,
                "placement_mode": self.placement_mode,
                "spec": self.maker.spec,
                "entry": self.settings.entry.model_dump(),
                "maker": self.settings.maker.model_dump(),
                "quantity": self.settings.trading.binance_qty,
                "max_quote_age_ms": self.settings.market.max_quote_age_ms,
                "max_quote_skew_ms": self.settings.market.max_quote_skew_ms,
            },
        )
        self.order = order
        self.orders_created += 1
        if self.placement_mode == PlacementMode.ONCE:
            self.once_remaining = 0
        config = self.settings.maker
        self.cancel_policy = CancelPolicy(
            config.cancel_threshold, config.cancel_confirm_ms, config.max_pending_ms
        )
        self.state = StrategyState.MAKER_PENDING
        log_event("maker_order_created", timestamp_ms=now, order=order)

    async def shutdown(self, now: int) -> None:
        self.placement_mode = self.requested_placement_mode = PlacementMode.PAUSED
        self.once_remaining = 0
        if self.order and self.order.state == OrderState.MAKER_PENDING:
            await self._cancel(now, "shutdown")
        if self.order and self.order.state == OrderState.CANCELING:
            # The local simulator owns the order and can synchronously acknowledge removal.
            self.maker.ack_cancel(self.order)
            await self.repo.save_order(self.order, "maker_order_canceled", now, reason="shutdown")
            self.order = None
            self.state = StrategyState.IDLE
        await self._reset_confirmations(now, "shutdown")
        await self._record("paper_shutdown", now, metrics=self.metrics.snapshot())
