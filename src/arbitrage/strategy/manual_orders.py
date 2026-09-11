import asyncio
from dataclasses import asdict

from arbitrage.domain.conditional_order import ConditionalOrder, ConditionalRequest
from arbitrage.domain.enums import EntryMode, PlacementMode, StrategyState
from arbitrage.strategy.arbitrage_strategy import ArbitrageStrategy
from arbitrage.strategy.confirmation import EntryConfirmation
from arbitrage.strategy.spread import entry_spread

TERMINAL = {"DONE", "CANCELED", "FAILED"}


class ManualOrderStrategy(ArbitrageStrategy):
    """Manual, durable conditions feeding exactly one execution slot."""

    def __init__(self, settings, spec, repo):
        super().__init__(settings, spec, repo)
        self.base_settings = settings
        self.placement_mode = self.requested_placement_mode = PlacementMode.PAUSED
        self.conditions: dict[str, ConditionalOrder] = {}
        self.condition_confirmations: dict[str, EntryConfirmation] = {}
        self.active_condition_id: str | None = None
        self.commands: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.next_sequence = 0
        self.last_manual_key = None
        self.accepting = False

    def request_placement(self, mode) -> None:
        if PlacementMode(mode) != PlacementMode.PAUSED:
            raise ValueError("全局自动挂单已停用，请手动创建条件单")

    def request_direction_mode(self, mode) -> None:
        # Direction belongs to each manual request, never to a global selector.
        pass

    def placement_enabled(self) -> bool:
        return False  # The base strategy must never create unsolicited orders.

    def enqueue(self, action: str, payload) -> asyncio.Future:
        if not self.accepting:
            raise ValueError("请先启动行情监控")
        future = asyncio.get_running_loop().create_future()
        try:
            self.commands.put_nowait((action, payload, future))
        except asyncio.QueueFull as exc:
            raise ValueError("操作队列繁忙，请稍后重试") from exc
        return future

    def _sequence(self) -> int:
        self.next_sequence += 1
        return self.next_sequence

    def _confirmation(self, key: str) -> EntryConfirmation:
        if key not in self.condition_confirmations:
            c = self.base_settings.entry.confirmation
            self.condition_confirmations[key] = EntryConfirmation(c.min_ticks, c.min_duration_ms)
        return self.condition_confirmations[key]

    def _reset_waiting(self) -> None:
        for confirmation in self.condition_confirmations.values():
            confirmation.reset()

    async def start(self, now: int) -> None:
        await super().start(now)
        for payload in await self.repo.load_conditions():
            condition = ConditionalOrder.from_payload(payload)
            self.next_sequence = max(self.next_sequence, condition.queue_seq)
            if condition.state in {"EXECUTING", "CANCELING"}:
                condition.state = "REVIEW"
                await self.repo.save_condition(condition, "conditional_recovery_required", now)
            elif condition.state == "WAITING":
                condition.state = "PAUSED"
                await self.repo.save_condition(condition, "conditional_paused", now)
            self.conditions[condition.request_id] = condition
        self.accepting = True

    async def _apply_command(self, action: str, payload, now: int) -> dict:
        if action == "create":
            request = ConditionalRequest.model_validate(payload)
            existing = self.conditions.get(request.request_id)
            if existing is None:
                stored = await self.repo.find_condition(request.request_id)
                existing = ConditionalOrder.from_payload(stored) if stored else None
            if existing:
                if existing.request_fields() != request.model_dump():
                    raise ValueError("相同请求编号不能创建不同条件单")
                return asdict(existing)
            if self.state in {StrategyState.SAFE_MODE, StrategyState.ERROR}:
                raise ValueError("当前状态禁止新挂单，请先核对未完成订单")
            if sum(c.state not in TERMINAL for c in self.conditions.values()) >= 100:
                raise ValueError("最多保留 100 笔未结束的条件单")
            spec = self.maker.spec
            if (
                not spec.min_qty <= request.quantity <= spec.max_qty
                or request.quantity % spec.step_size
            ):
                raise ValueError("数量不符合 Binance 最小数量或步长")
            condition = ConditionalOrder(
                **request.model_dump(),
                queue_seq=self._sequence(),
                created_at_ms=now,
                updated_at_ms=now,
            )
            await self.repo.save_condition(condition, "conditional_created", now)
            self.conditions[condition.request_id] = condition
            self._prune_history()
            return asdict(condition)
        condition = self.conditions.get(payload)
        if condition is None:
            raise ValueError("条件单不存在")
        if action == "cancel":
            if condition.state in TERMINAL:
                return asdict(condition)
            condition.cancel_requested = True
            if self.active_condition_id == payload:
                condition.state = "CANCELING"
                self.requested_placement_mode = PlacementMode.PAUSED
            else:
                condition.state = "CANCELED"
            self._confirmation(payload).reset()
            await self.repo.save_condition(condition, "conditional_cancel_requested", now)
        elif action == "resume":
            if condition.state != "PAUSED" or self.state in {
                StrategyState.SAFE_MODE,
                StrategyState.ERROR,
            }:
                raise ValueError("此条件单当前不能恢复")
            condition.state = "WAITING"
            condition.cancel_requested = False
            condition.queue_seq = self._sequence()
            self._confirmation(payload).reset()
            await self.repo.save_condition(condition, "conditional_resumed", now)
        elif action == "pause":
            if condition.state != "WAITING":
                raise ValueError("只能暂停尚未触发的条件单")
            condition.state = "PAUSED"
            self._confirmation(payload).reset()
            await self.repo.save_condition(condition, "conditional_paused", now)
        else:
            raise ValueError("未知条件单操作")
        return asdict(condition)

    async def _drain_commands(self, now: int) -> None:
        for _ in range(8):
            if self.commands.empty():
                break
            action, payload, future = self.commands.get_nowait()
            try:
                result = await self._apply_command(action, payload, now)
            except ValueError as exc:
                if not future.done():
                    future.set_exception(exc)
            except Exception as exc:
                if not future.done():
                    future.set_exception(exc)
                raise  # Persistence failure stops the session, not just this command.
            else:
                if not future.done():
                    future.set_result(result)

    def _prune_history(self) -> None:
        finished = sorted(
            (c for c in self.conditions.values() if c.state in TERMINAL),
            key=lambda c: c.updated_at_ms,
            reverse=True,
        )
        for condition in finished[50:]:
            self.conditions.pop(condition.request_id)
            self.condition_confirmations.pop(condition.request_id, None)

    async def _finish_condition(self, now: int, *, stopping: bool = False) -> None:
        condition = self.conditions[self.active_condition_id]
        condition.last_result = "CANCELED"  # Phase 1 has no fill simulator.
        if condition.cancel_requested:
            condition.state = "CANCELED"
        elif condition.repeat:
            condition.state = "PAUSED" if stopping else "WAITING"
            condition.queue_seq = self._sequence()  # Repeating requests yield to waiting peers.
        else:
            condition.state = "DONE"
        await self.repo.save_condition(condition, "conditional_cycle_finished", now)
        self.active_condition_id = None
        self.settings = self.base_settings
        self.placement_mode = self.requested_placement_mode = PlacementMode.PAUSED
        self._reset_waiting()
        self._prune_history()

    async def on_timer(self, now: int) -> None:
        await self._drain_commands(now)
        await super().on_timer(now)
        if self.active_condition_id and self.order is None:
            await self._finish_condition(now)
        if self.guard.check(self.binance_quote, self.mt5_quote, now) is not None:
            self._reset_waiting()

    async def on_quotes(self, binance, mt5, now: int) -> dict:
        snapshot = await super().on_quotes(binance, mt5, now)
        key = tuple((q.exchange_ts_ms, q.bid, q.ask, q.bid_qty, q.ask_qty) for q in (binance, mt5))
        fresh_tick = key != self.last_manual_key
        self.last_manual_key = key
        if (
            self.order
            or self.active_condition_id
            or self.state in {StrategyState.SAFE_MODE, StrategyState.ERROR}
        ):
            self._reset_waiting()
            return snapshot
        if not snapshot["quotes_valid"] or not fresh_tick:
            return snapshot
        eligible = []
        for condition in self.conditions.values():
            if condition.state != "WAITING":
                continue
            spread = entry_spread(condition.direction, binance, mt5, condition.entry_threshold)
            result = self._confirmation(condition.request_id).update(spread.edge, now)
            if result.state == "CONFIRMED":
                eligible.append((condition, spread))
        if not eligible:
            return snapshot
        condition, spread = min(eligible, key=lambda pair: pair[0].queue_seq)
        self.active_condition_id = condition.request_id  # Reserve before the first await.
        condition.state = "EXECUTING"
        condition.execution_order_id = None
        await self.repo.save_condition(condition, "conditional_reserved", now)
        self.settings = self.base_settings.model_copy(
            update={
                "entry": self.base_settings.entry.model_copy(
                    update={"threshold": condition.entry_threshold}
                ),
                "maker": self.base_settings.maker.model_copy(
                    update={"cancel_threshold": condition.cancel_threshold}
                ),
                "trading": self.base_settings.trading.model_copy(
                    update={"binance_qty": condition.quantity}
                ),
            }
        )
        self.direction_mode = self.requested_direction_mode = (
            EntryMode.A if condition.direction == "SHORT_BINANCE" else EntryMode.B
        )
        self.placement_mode = self.requested_placement_mode = PlacementMode.ONCE
        self.once_remaining = 1
        self.confirmations[condition.direction] = self._confirmation(condition.request_id)
        try:
            await self._place_direction(
                condition.direction, {condition.direction: asdict(spread)}, now, condition=condition
            )
        except ValueError as exc:
            condition.state, condition.last_result = "FAILED", str(exc)
            await self.repo.save_condition(condition, "conditional_failed", now)
            self.active_condition_id = None
            self.settings = self.base_settings
            self.placement_mode = self.requested_placement_mode = PlacementMode.PAUSED
            self.state = StrategyState.IDLE
        else:
            condition.execution_count += 1
            condition.execution_order_id = self.order.order_id
            await self.repo.save_condition(condition, "conditional_executing", now)
        self._reset_waiting()
        return snapshot

    def condition_views(self, now: int) -> list[dict]:
        views = []
        for condition in sorted(
            self.conditions.values(), key=lambda c: (c.state in TERMINAL, c.queue_seq)
        ):
            view = asdict(condition)
            confirmation = self.condition_confirmations.get(condition.request_id)
            view["confirmation"] = (
                asdict(confirmation.result)
                if confirmation
                else {"state": "IDLE", "count": 0, "duration_ms": 0}
            )
            view["raw_spread"] = view["edge"] = None
            if self.binance_quote and self.mt5_quote:
                view.update(
                    asdict(
                        entry_spread(
                            condition.direction,
                            self.binance_quote,
                            self.mt5_quote,
                            condition.entry_threshold,
                        )
                    )
                )
            views.append(view)
        return views

    async def shutdown(self, now: int) -> None:
        self.accepting = False
        while not self.commands.empty():
            _, _, future = self.commands.get_nowait()
            if not future.done():
                future.set_exception(ValueError("监控已停止，操作未提交"))
        await super().shutdown(now)
        if self.active_condition_id:
            condition = self.conditions[self.active_condition_id]
            if condition.execution_order_id is None:
                condition.state = "REVIEW"
                condition.last_result = "执行中断，需核对保留记录"
                await self.repo.save_condition(condition, "conditional_recovery_required", now)
                self.active_condition_id = None
            else:
                await self._finish_condition(now, stopping=True)
        for condition in self.conditions.values():
            if condition.state == "WAITING":
                condition.state = "PAUSED"
                await self.repo.save_condition(condition, "conditional_paused", now)
        self._reset_waiting()
