"""Single execution worker, durable intents, post-only entry and ticket-specific hedge."""

import asyncio
from dataclasses import asdict
from decimal import Decimal
from time import monotonic_ns
from uuid import uuid4

from arbitrage.domain.conditional_order import ConditionalOrder, ConditionalRequest
from arbitrage.domain.enums import StrategyState
from arbitrage.domain.specs import HedgeCalculator, round_step
from arbitrage.execution.binance_maker import CancelPolicy
from arbitrage.execution.live_venues import ExecutionUnknown, OrderRejected, PostOnlyWouldMatch
from arbitrage.execution.user_stream import OrderUpdates
from arbitrage.observability import now_ms
from arbitrage.strategy.hedge_ledger import legs, remaining
from arbitrage.strategy.manual_orders import ManualOrderStrategy
from arbitrage.strategy.spread import entry_spread, pending_spread
from arbitrage.strategy.trade_metrics import expected_net_pnl, update_metrics

D = Decimal
END = {"FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"}


class LiveOrderStrategy(ManualOrderStrategy):
    def __init__(self, settings, spec, repo, binance, mt5, mt5_spec):
        if settings.mode != "live":
            raise ValueError("LiveOrderStrategy requires live mode")
        settings.require_runtime()
        # Reuse condition validation and quote views, never the Paper execution loop.
        super().__init__(settings.model_copy(update={"mode": "paper"}), spec, repo)
        self.settings = self.base_settings = settings
        self.binance, self.mt5 = binance, mt5
        self.hedge = HedgeCalculator(spec, mt5_spec, settings.trading.binance_underlying_per_qty)
        self.trades = {}
        self.worker = None
        self.stopping = False
        self.accounts = {}
        self.accounts_updated_ms = 0
        self.accounts_error = None
        self.risk_error = None
        self.account_worker = None
        self.execution_wakeup = asyncio.Event()
        self.user_updates = OrderUpdates(settings.symbol.binance)
        self.dispatch_starts = {}
        self.binance.dispatch_observer = self._dispatched
        self.mt5.dispatch_observer = self._dispatched

    def _dispatched(self, client, stamp):
        sample = self.dispatch_starts.pop(client, None)
        if sample:
            t, stage, start = sample
            t.setdefault("timings_ns", {})[stage] = stamp - start

    def _measure_dispatch(self, client, t, stage, start):
        self.dispatch_starts[client] = (t, stage, start)

    def _reset_waiting(self):
        # Entry reservations must not erase a different position's exit confirmation.
        for key, confirmation in self.condition_confirmations.items():
            if not key.startswith("exit:"):
                confirmation.reset()

    def _entry_capacity(self):
        return not any(t["state"] == "OPEN" for t in self.trades.values())

    def _close_cost(self, sell):
        b, m = self.binance_quote, self.mt5_quote
        price = round_step(b.bid if sell else b.ask, self.maker.spec.tick_size, up=not sell)
        return price - m.bid if sell else m.ask - price

    def _close_queued(self):
        return any(c.state == "OPEN" and c.close_requested for c in self.conditions.values())

    def on_user_event(self, payload):
        if self.user_updates.feed(payload):
            self.execution_wakeup.set()

    async def start(self, now):
        await self.repo.bind_mode("live")
        await self.binance.initialize()
        await self.mt5.initialize()
        await self.repo.bind_identity(
            ":".join(
                (
                    await self.binance.identity(),
                    await self.mt5.identity(),
                    self.settings.symbol.binance,
                    self.settings.symbol.mt5,
                    str(self.hedge.underlying_per_binance_qty),
                    str(self.hedge.mt5.contract_size),
                    str(self.settings.live.mt5_magic),
                )
            )
        )
        self.state = StrategyState.IDLE
        for payload in await self.repo.load_conditions():
            c = ConditionalOrder.from_payload(payload)
            c.close_requested = False
            c.close_reason = None
            self.next_sequence = max(self.next_sequence, c.queue_seq)
            if c.state == "WAITING":
                c.state = "PAUSED"
            elif c.state in {"EXECUTING", "CANCELING", "CLOSING"}:
                c.state = "REVIEW"
            self.conditions[c.request_id] = c
            await self.repo.save_condition(c, "live_condition_loaded", now)
        for trade in await self.repo.load_trades():
            self.trades[trade["trade_id"]] = trade
            if trade["state"] not in {"OPEN", "CLOSED", "CANCELED", "FAILED"}:
                trade["state"] = "REVIEW"
                self.state = StrategyState.SAFE_MODE
                await self._save(trade, "live_recovery_required")
        # A persisted OPEN pair can be closed after inventory verification; unknown intents cannot.
        try:
            await self._inventory_check()
            for t in self.trades.values():
                if t["state"] == "OPEN":
                    c = self.conditions.get(t["condition_id"])
                    if c is None or c.execution_order_id != t["trade_id"]:
                        raise ExecutionUnknown("持仓缺少条件单关联，请核对数据库")
                    c.state = "OPEN"
                    await self.repo.save_condition(c, "live_position_reconciled", now)
            if any(c.state == "REVIEW" for c in self.conditions.values()):
                raise ExecutionUnknown("存在未完成的条件单记录，请先核对执行结果")
        except Exception as exc:
            self.state = StrategyState.SAFE_MODE
            self.risk_error = str(exc)
        self.accepting = True
        await self._accounts()

    async def _inventory_check(self):
        orders, positions, inventory = await asyncio.gather(
            self.binance.open_orders(), self.mt5.positions(), self.binance.inventory()
        )
        if orders:
            raise ExecutionUnknown("Binance 存在未核对挂单，禁止继续执行")
        expected = {"LONG": D(0), "SHORT": D(0)}
        owned = set()
        for t in self.trades.values():
            if t["state"] == "REVIEW":
                raise ExecutionUnknown("存在中断交易，请在两平台核对后处理恢复记录")
            if t["state"] != "OPEN":
                continue
            expected[t["position_side"]] += remaining(t)
            for leg in legs(t):
                volume = D(leg["lots"]) - D(leg["closed_lots"])
                if not volume:
                    continue
                matches = [p for p in positions if p["ticket"] == leg["ticket"]]
                if len(matches) != 1 or D(matches[0]["volume"]) != volume:
                    raise ExecutionUnknown("MT5 recorded ticket volume mismatch")
                p = matches[0]
                if p["magic"] != self.settings.live.mt5_magic or p["type"] != (
                    0 if t["sell"] else 1
                ):
                    raise ExecutionUnknown("MT5 ticket ownership mismatch")
                owned.add(p["ticket"])
        if any(p["ticket"] not in owned for p in positions):
            raise ExecutionUnknown("MT5 本品种存在外部持仓；请使用独立账户或品种")
        actual = {"LONG": D(0), "SHORT": D(0)}
        for p in inventory:
            if p["positionSide"] not in actual:
                if D(p["positionAmt"]):
                    raise ExecutionUnknown("Binance 持仓模式不一致")
            else:
                actual[p["positionSide"]] += abs(D(p["positionAmt"]))
        if actual != expected:
            raise ExecutionUnknown("Binance 持仓与本地账本不一致，禁止下单")

    async def _apply_command(self, action, payload, now):
        if action == "close_all":
            if self.state == StrategyState.SAFE_MODE:
                raise ValueError("存在待核对执行，禁止批量平仓")
            count = 0
            for c in self.conditions.values():
                if c.state == "OPEN":
                    c.close_requested = True
                    c.close_reason = "manual"
                    await self.repo.save_condition(c, "live_close_queued", now)
                    count += 1
            return {"queued": count}
        if action == "create":
            r = ConditionalRequest.model_validate(payload)
            if r.quantity > self.settings.live.max_binance_qty:
                raise ValueError("数量超过实盘单笔上限")
            self.hedge.binance_to_lots(r.quantity)
        if action in {"close", "close_threshold"}:
            c = self.conditions.get(payload)
            if c is None:
                raise ValueError("条件单不存在")
            if c.state == "CLOSING":
                return asdict(c)
            if c.state != "OPEN" or self.state == StrategyState.SAFE_MODE:
                raise ValueError("此单当前不能平仓，请核对状态")
            c.close_requested = True
            c.close_reason = "threshold" if action == "close_threshold" else "manual"
            await self.repo.save_condition(c, "live_close_queued", now)
            self.execution_wakeup.set()
            return asdict(c)
        if action == "cancel":
            c = self.conditions.get(payload)
            if c and c.state in {"OPEN", "CLOSING", "REVIEW"}:
                raise ValueError("已开仓请使用平仓；待核对记录不能直接取消")
        return await super()._apply_command(action, payload, now)

    async def on_timer(self, now):
        if self.worker and self.worker.done() and self.worker.exception() is not None:
            self.state = StrategyState.SAFE_MODE
            raise RuntimeError(
                "实盘执行记录写入失败，停止会话核对订单"
            ) from self.worker.exception()
        await self._drain_commands(now)
        if (
            not self.stopping
            and self.state != StrategyState.SAFE_MODE
            and (self.worker is None or self.worker.done())
        ):
            for c in sorted(self.conditions.values(), key=lambda c: c.queue_seq):
                if c.state == "OPEN" and c.close_requested:
                    c.state = "CLOSING"
                    await self.repo.save_condition(c, "live_close_requested", now)
                    self.active_condition_id = c.request_id
                    self.worker = asyncio.create_task(
                        self._execute(c, self.trades[c.execution_order_id], closing=True)
                    )
                    break
        if self.guard.check(self.binance_quote, self.mt5_quote, now) is not None:
            for confirmation in self.condition_confirmations.values():
                confirmation.reset()
        if (
            (self.worker is None or self.worker.done())
            and not self.stopping
            and (self.account_worker is None or self.account_worker.done())
            and now - self.accounts_updated_ms > 3000
        ):
            self.account_worker = asyncio.create_task(self._accounts())

    async def on_quotes(self, binance, mt5, now):
        self.binance_quote, self.mt5_quote = binance, mt5
        self.execution_wakeup.set()
        await self.on_timer(now)
        key = tuple((q.exchange_ts_ms, q.bid, q.ask, q.bid_qty, q.ask_qty) for q in (binance, mt5))
        changed = key != self.last_manual_key
        self.last_manual_key = key
        if self.stopping or self.state == StrategyState.SAFE_MODE:
            self._reset_waiting()
            return {}
        if self.guard.check(binance, mt5, now) or not changed:
            return {}
        for c in sorted(self.conditions.values(), key=lambda c: c.queue_seq):
            if (
                c.state == "OPEN"
                and (c.exit_threshold is not None or c.min_net_profit is not None)
                and not c.close_requested
            ):
                cost = self._close_cost(c.direction == "SHORT_BINANCE")
                if (
                    self._confirmation("exit:" + c.request_id)
                    .update(self._exit_edge(c, self.trades[c.execution_order_id], cost), now)
                    .state
                    == "CONFIRMED"
                ):
                    await self._apply_command("close_threshold", c.request_id, now)
        if (
            self._close_queued()
            or (self.worker and not self.worker.done())
            or not self._entry_capacity()
        ):
            self._reset_waiting()
            return {}
        eligible = []
        for c in self.conditions.values():
            if c.state == "WAITING":
                spread = entry_spread(c.direction, binance, mt5, c.entry_threshold)
                if self._confirmation(c.request_id).update(spread.edge, now).state == "CONFIRMED":
                    eligible.append(c)
        if eligible:
            confirmed_ns = monotonic_ns()
            c = min(eligible, key=lambda c: c.queue_seq)
            c.state = "EXECUTING"
            self.active_condition_id = c.request_id
            await self.repo.save_condition(c, "live_condition_reserved", now)
            self.worker = asyncio.create_task(self._execute(c, confirmed_ns=confirmed_ns))
            self._reset_waiting()
        return {}

    async def _save(self, t, event):
        update_metrics(t, self.hedge.underlying_per_binance_qty)
        t["updated_at_ms"] = now_ms()
        await self.repo.save_trade(t, event)

    def condition_views(self, now):
        views = super().condition_views(now)
        valid = self.guard.check(self.binance_quote, self.mt5_quote, now) is None
        for view in views:
            t = self.trades.get(view["execution_order_id"], {})
            for key in (
                "signal_entry_spread",
                "actual_entry_spread",
                "actual_exit_spread",
                "entry_spread_loss",
                "gross_pnl",
                "compensation_gross_pnl",
            ):
                view[key] = t.get(key)
            view["estimated_spread_gain"] = None
            view["profit_estimate"] = None
            if t.get("state") == "OPEN" and t.get("actual_entry_spread") is not None and valid:
                cost = self._close_cost(t["sell"])
                view["estimated_spread_gain"] = str(D(t["actual_entry_spread"]) - cost)
                view["profit_estimate"] = expected_net_pnl(
                    t, cost, self.settings.live.profit_budget, self.hedge.underlying_per_binance_qty
                )
        return views

    async def _execute(self, c, trade=None, *, closing=False, confirmed_ns=None):
        t = trade
        try:
            await self._inventory_check()
            if closing:
                if c.close_reason == "threshold":
                    b, m, now = self.binance_quote, self.mt5_quote, now_ms()
                    invalid = self.guard.check(b, m, now)
                    cost = None if invalid else self._close_cost(t["sell"])
                    if invalid or self._exit_edge(c, t, cost) < 0:
                        c.state, c.close_requested, c.close_reason = "OPEN", False, None
                        return
                await self._close(t)
            else:
                # Preflight API calls may outlast quote freshness or change the spread.
                b, m, now = self.binance_quote, self.mt5_quote, now_ms()
                if (
                    self.stopping
                    or c.cancel_requested
                    or self._close_queued()
                    or self.guard.check(b, m, now)
                    or not self._entry_capacity()
                ):
                    c.state = "CANCELED" if c.cancel_requested else "WAITING"
                    return
                if entry_spread(c.direction, b, m, c.entry_threshold).edge < 0:
                    c.state = "WAITING"
                    return
                sell = c.direction == "SHORT_BINANCE"
                price = round_step(b.ask if sell else b.bid, self.maker.spec.tick_size, up=sell)
                self.maker.spec.quantity(c.quantity, price)
                trade_id = uuid4().hex
                t = dict(
                    trade_id=trade_id,
                    condition_id=c.request_id,
                    state="ENTRY_INTENT",
                    sell=sell,
                    position_side="SHORT" if sell else "LONG",
                    quantity=str(c.quantity),
                    price=str(price),
                    filled_qty="0",
                    lots="0",
                    mt5_contract_size=str(self.hedge.mt5.contract_size),
                    mt5_ticket=None,
                    created_at_ms=now,
                    updated_at_ms=now,
                    open_client_id="au" + trade_id,
                    direction=c.direction,
                    entry_spread=str(entry_spread(c.direction, b, m, c.entry_threshold).raw_spread),
                    signal_entry_spread=str(
                        entry_spread(c.direction, b, m, c.entry_threshold).raw_spread
                    ),
                )
                self.trades[trade_id] = t
                c.execution_order_id = trade_id
                await self.repo.save_condition(c, "live_execution_linked", now)
                await self._open(c, t, confirmed_ns=confirmed_ns)
            if t["state"] == "OPEN":
                c.state = "OPEN"
                if not closing:
                    c.execution_count += 1
            elif t["state"] in {"CLOSED", "CANCELED"}:
                c.close_requested = False
                c.close_reason = None
                c.last_result = t["state"]
                c.state = (
                    "CANCELED"
                    if c.cancel_requested
                    else "WAITING"
                    if c.repeat and not self.stopping
                    else "DONE"
                )
                if c.state == "WAITING":
                    c.queue_seq = self._sequence()
            else:
                c.state = "FAILED"
        except PostOnlyWouldMatch:
            if t is None:
                c.state = "WAITING"
            elif closing:
                t["state"], t["closing_maker"] = "OPEN", False
                c.state, c.close_requested, c.close_reason = "OPEN", False, None
                t["active_close"]["post_only_rejected"] = True
                await self._save(t, "live_post_only_rejected")
            else:
                t["state"] = "CANCELED"
                t["post_only_rejected"] = True
                c.state = "CANCELED" if c.cancel_requested else "WAITING"
                c.queue_seq = self._sequence()
                await self._save(t, "live_post_only_rejected")
        except OrderRejected as exc:
            # Only before any fill, or definitive validation failure, is a rejection safe.
            if t is not None and D(t.get("filled_qty", "0")) > 0:
                await self._review(c, t, exc)
            else:
                c.state, c.last_result = "FAILED", str(exc)
                if t:
                    t["state"], t["error"] = "FAILED", str(exc)
                    await self._save(t, "live_rejected")
        except Exception as exc:
            await self._review(c, t, exc)
        finally:
            if t:
                self.user_updates.forget(t["open_client_id"])
                self.dispatch_starts.pop(t["open_client_id"], None)
                self.dispatch_starts.pop("au" + t["trade_id"][:20], None)
                for tag, sample in list(self.dispatch_starts.items()):
                    if sample[0] is t:
                        self.dispatch_starts.pop(tag, None)
            self.active_condition_id = None
            self._reset_waiting()
            confirmation = self.condition_confirmations.get("exit:" + c.request_id)
            if confirmation:
                confirmation.reset()
            await self.repo.save_condition(c, "live_condition_updated", now_ms())

    async def _review(self, c, t, exc):
        self.state = StrategyState.SAFE_MODE
        self.risk_error = str(exc)
        c.state, c.last_result = "REVIEW", str(exc)
        if t:
            t["state"], t["error"] = "REVIEW", str(exc)
            await self._save(t, "live_execution_uncertain")

    async def _advance(self, t, result):
        from arbitrage.strategy.hedge_ledger import advance, cycle

        book = cycle(t)
        if D(result["executedQty"]) > 0:
            self.user_updates.first_fill_ns.setdefault(book["open_client_id"], monotonic_ns())
        try:
            await advance(self, t, result)
        except OrderRejected:
            # Definitive pre-send rejection: stop new hedges, compensate proven remainder later.
            book["hedge_rejected"] = True

    async def _cancel_with_hedges(self, c, t, result=None):
        from arbitrage.strategy.hedge_ledger import cycle

        book = cycle(t)
        stamp = monotonic_ns()
        if result is not None and D(result["executedQty"]) > 0:
            self.user_updates.first_fill_ns.setdefault(book["open_client_id"], stamp)
        t["state"] = "CANCEL_INTENT"
        await self._save(t, "live_cancel_intent")
        self._measure_dispatch(book["open_client_id"], t, "cancel_dispatch", stamp)
        task = asyncio.create_task(self.binance.cancel(book["open_client_id"]))
        try:
            if result is not None:
                await self._advance(t, result)
            while not task.done():
                self.execution_wakeup.clear()
                pushed = self.user_updates.get(book["open_client_id"])
                if pushed and D(pushed["executedQty"]) > D(book.get("cumulative_filled", "0")):
                    await self._advance(t, pushed)
                wake = asyncio.create_task(self.execution_wakeup.wait())
                try:
                    await asyncio.wait(
                        (task, wake),
                        timeout=self.settings.market.watchdog_ms / 1000,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    wake.cancel()
                    await asyncio.gather(wake, return_exceptions=True)
            final = task.result()
            if final.get("status") not in END or "executedQty" not in final:
                raise ExecutionUnknown("Final maker quantity unconfirmed")
            await self._advance(t, final)
            return final
        finally:
            # Never abandon a venue mutation or repeat a hedge after an uncertain response.
            await asyncio.shield(asyncio.gather(task, return_exceptions=True))

    def _pending_value(self, c, t):
        if t.get("closing_maker"):
            price = D(t["active_close"]["price"])
            cost = price - self.mt5_quote.bid if t["sell"] else self.mt5_quote.ask - price
            return D(0) if c.close_reason == "manual" else self._exit_edge(c, t, cost)
        return pending_spread(c.direction, D(t["price"]), self.mt5_quote)

    async def _open(self, c, t, *, confirmed_ns=None):
        await self._save(t, "live_entry_intent")
        self.user_updates.watch(
            t["open_client_id"],
            quantity=D(t["quantity"]),
            S="SELL" if t["sell"] else "BUY",
            ps=t["position_side"],
        )
        if confirmed_ns is not None:
            self._measure_dispatch(t["open_client_id"], t, "entry_dispatch", confirmed_ns)
        try:
            result = await self.binance.submit_maker(
                t["open_client_id"],
                "SELL" if t["sell"] else "BUY",
                t["position_side"],
                D(t["quantity"]),
                price=D(t["price"]),
            )
        except ExecutionUnknown:
            result = await self._cancel_with_hedges(c, t)
        self.orders_created += 1
        policy = CancelPolicy(
            c.cancel_threshold,
            self.settings.maker.cancel_confirm_ms,
            self.settings.maker.max_pending_ms,
        )
        if "status" not in result or "executedQty" not in result:
            result = await self._query_or_cancel(c, t, policy, {}, immediate=True)
        while result["status"] not in END:
            if D(result["executedQty"]) > 0:
                result = await self._cancel_with_hedges(c, t, result)
                break
            result = await self._query_or_cancel(c, t, policy, {})
        await self._advance(t, result)
        filled = D(t["filled_qty"])
        paired = D(t.get("mt5_hedged_qty", "0"))
        t["paired_qty"] = str(paired)
        if filled > paired:
            await self._compensate(t, filled - paired)
        t["state"] = "OPEN" if paired else "FAILED" if filled else "CANCELED"
        await self._save(t, "live_pair_opened" if paired else "live_entry_finished")

    async def _query_or_cancel(self, c, t, policy, timings, *, immediate=False):
        from arbitrage.strategy.hedge_ledger import cycle

        book = cycle(t)

        async def query(*, urgent=False):
            # Healthy pushes are primary; REST periodically checks for silent gaps.
            delay = 1 if self.user_updates.connected else self.settings.live.order_poll_ms / 1000
            await asyncio.sleep(0 if immediate or urgent else delay)
            return await self.binance.order(book["open_client_id"])

        task = asyncio.create_task(query())
        had_stream = self.user_updates.connected
        try:
            while True:
                self.execution_wakeup.clear()
                pushed = self.user_updates.get(book["open_client_id"])
                if pushed and (pushed["status"] in END or D(pushed["executedQty"]) > 0):
                    return pushed
                if had_stream and not self.user_updates.connected and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    task = asyncio.create_task(query(urgent=True))
                    had_stream = False
                now = now_ms()
                invalid = self.guard.check(self.binance_quote, self.mt5_quote, now)
                cancel = (
                    self.stopping
                    or c.cancel_requested
                    or (not t.get("closing_maker") and self._close_queued())
                    or invalid
                )
                if not cancel:
                    cancel = policy.update(
                        self._pending_value(c, t),
                        now,
                        book["created_at_ms"],
                    )
                if cancel:
                    return await self._cancel_with_hedges(c, t)
                if task.done():
                    try:
                        result = task.result()
                        if "status" not in result or "executedQty" not in result:
                            raise ExecutionUnknown("Binance query lacks execution evidence")
                        return result
                    except (ExecutionUnknown, OrderRejected):
                        return await self._cancel_with_hedges(c, t)
                wake = asyncio.create_task(self.execution_wakeup.wait())
                try:
                    await asyncio.wait(
                        (task, wake),
                        timeout=self.settings.market.watchdog_ms / 1000,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    wake.cancel()
                    await asyncio.gather(wake, return_exceptions=True)
        finally:
            task.cancel()  # This is a read-only GET, never an order submission.
            await asyncio.gather(task, return_exceptions=True)

    async def _compensate(self, t, quantity, *, restore=False):
        client = "au" + uuid4().hex
        t["compensate_client_id"] = client
        intent = dict(client_id=client, quantity=str(quantity), restore=restore, state="INTENT")
        t.setdefault("compensations", []).append(intent)
        await self._save(t, "live_compensation_intent")
        sell = t["sell"] if restore else not t["sell"]
        result = await self.binance.submit_market(
            client, "SELL" if sell else "BUY", t["position_side"], quantity
        )
        if result.get("status") != "FILLED" or D(result.get("executedQty", "-1")) != quantity:
            raise ExecutionUnknown("Compensation quantity unconfirmed")
        intent.update(state="DONE", result=result)
        if not t.get("mt5_hedged_qty"):
            t["binance_close"] = result
        await self._save(t, "live_compensation_done")

    def _exit_edge(self, c, t, cost):
        edge = c.exit_threshold - cost if c.exit_threshold is not None else D(0)
        if c.min_net_profit is not None:
            estimate = expected_net_pnl(
                t, cost, self.settings.live.profit_budget, self.hedge.underlying_per_binance_qty
            )
            value = estimate["expected_net_pnl"]
            if value is None:
                return D("-Infinity")
            if value < c.min_net_profit:
                return D(-1)
        return edge

    async def _close(self, t):
        from arbitrage.strategy.hedge_ledger import average, remaining

        c = self.conditions[t["condition_id"]]
        b = self.binance_quote
        if self.guard.check(b, self.mt5_quote, now_ms()):
            return
        price = round_step(
            b.bid if t["sell"] else b.ask, self.maker.spec.tick_size, up=not t["sell"]
        )
        quantity = remaining(t)
        self.maker.spec.quantity(quantity, price)
        client = "ac" + uuid4().hex
        book = dict(
            open_client_id=client,
            price=str(price),
            quantity=str(quantity),
            created_at_ms=now_ms(),
            cumulative_filled="0",
            processed_filled="0",
        )
        t["active_close"], t["closing_maker"], t["state"] = book, True, "CLOSE_INTENT"
        t["close_client_id"] = client
        t.setdefault("close_attempts", []).append(book)
        await self._save(t, "live_close_maker_intent")
        self.user_updates.watch(
            client, quantity=quantity, S="BUY" if t["sell"] else "SELL", ps=t["position_side"]
        )
        try:
            try:
                result = await self.binance.submit_maker(
                    client,
                    "BUY" if t["sell"] else "SELL",
                    t["position_side"],
                    quantity,
                    price=price,
                )
            except ExecutionUnknown:
                result = await self._cancel_with_hedges(c, t)
            policy = CancelPolicy(
                D(0), self.settings.maker.cancel_confirm_ms, self.settings.maker.max_pending_ms
            )
            if "status" not in result or "executedQty" not in result:
                result = await self._query_or_cancel(c, t, policy, {}, immediate=True)
            while result["status"] not in END:
                if D(result["executedQty"]) > 0:
                    result = await self._cancel_with_hedges(c, t, result)
                    break
                result = await self._query_or_cancel(c, t, policy, {})
            await self._advance(t, result)
            residue = D(book["cumulative_filled"]) - D(book["processed_filled"])
            if residue:
                # An unrepresentable exit fragment is restored, never over-close MT5.
                await self._compensate(t, residue, restore=True)
            results = [a["result"] for a in t["close_attempts"] if a.get("result")]
            t["binance_close"] = dict(avgPrice=average(results, "executedQty", "avgPrice"))
            t["state"] = "CLOSED" if remaining(t) == 0 else "OPEN"
            c.close_requested, c.close_reason = False, None
            t["closing_maker"] = False
            await self._save(t, "live_close_maker_finished")
        finally:
            self.user_updates.forget(client)

    async def _accounts(self):
        try:
            if self.worker and not self.worker.done():
                return
            binance_account = await self.binance.account()
            if self.worker and not self.worker.done():
                return
            self.accounts = {
                "binance": binance_account,
                "mt5": await self.mt5.account(),
            }
            # Fee and realized PnL evidence remains separately denominated, never invented.
            for t in list(self.trades.values()):
                if self.worker and not self.worker.done():
                    break
                compensated = (
                    t["state"] == "FAILED"
                    and bool(t.get("compensate_client_id"))
                    and "binance_close" in t
                )
                if (t["state"] == "CLOSED" or compensated) and "settlement" not in t:
                    trades = []
                    orders = [t["binance_open"]]
                    if "close_attempts" in t:
                        orders.extend(
                            a["result"]
                            for a in t["close_attempts"]
                            if a.get("result") and D(a["result"]["executedQty"]) > 0
                        )
                    elif not t.get("compensations"):
                        orders.append(t["binance_close"])
                    orders.extend(
                        a["result"] for a in t.get("compensations", []) if a.get("result")
                    )
                    for order in orders:
                        rows = await self.binance.trades(order["orderId"])
                        if sum((D(r["qty"]) for r in rows), D(0)) != D(order["executedQty"]):
                            break
                        trades.extend(rows)
                    else:
                        if self.worker and not self.worker.done():
                            break
                        deals = []
                        complete = True
                        for leg in [] if compensated else legs(t):
                            rows = await self.mt5.deals(leg["identifier"])
                            if any(
                                sum((D(d["volume"]) for d in rows if d["entry"] == side), D(0))
                                != D(leg["lots"])
                                for side in (0, 1)
                            ):
                                complete = False
                                break
                            deals.extend(rows)
                        if not complete:
                            continue
                        fees = {}
                        for r in trades:
                            asset = r["commissionAsset"]
                            fees[asset] = fees.get(asset, D(0)) + D(r["commission"])
                        summary = {
                            "binance_net": sum((D(r["realizedPnl"]) for r in trades), D(0))
                            - fees.get("USDT", D(0)),
                            "binance_fees": fees,
                            "mt5_net": sum(
                                (
                                    sum(
                                        (D(d[k]) for k in ("profit", "commission", "swap", "fee")),
                                        D(0),
                                    )
                                    for d in deals
                                ),
                                D(0),
                            ),
                            "mt5_fees": sum(
                                (D(d["commission"]) + D(d["fee"]) for d in deals), D(0)
                            ),
                        }
                        t["settlement"] = {
                            "binance_trades": trades,
                            "mt5_deals": deals,
                            "summary": summary,
                            "mt5_currency": self.accounts["mt5"]["currency"],
                        }
                        await self._save(t, "live_settlement_received")
            self.accounts_error = None
        except Exception as exc:
            self.accounts_error = str(exc)
        finally:
            self.accounts_updated_ms = now_ms()

    async def shutdown(self, now):
        self.stopping = True
        self.accepting = False
        # Keep the worker alive through cancel/hedge acknowledgement. Never cancel order_send.
        if self.worker:
            await asyncio.shield(self.worker)
        if self.account_worker:
            await self.account_worker
        while not self.commands.empty():
            _, _, future = self.commands.get_nowait()
            if not future.done():
                future.set_exception(ValueError("监控已停止"))
        for c in self.conditions.values():
            if c.state == "WAITING":
                c.state = "PAUSED"
                await self.repo.save_condition(c, "conditional_paused", now_ms())
