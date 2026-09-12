"""Single execution worker, durable intents, post-only entry and ticket-specific hedge."""

import asyncio
from dataclasses import asdict
from decimal import Decimal
from uuid import uuid4

from arbitrage.domain.conditional_order import ConditionalOrder, ConditionalRequest
from arbitrage.domain.enums import StrategyState
from arbitrage.domain.specs import HedgeCalculator, round_step
from arbitrage.execution.binance_maker import CancelPolicy
from arbitrage.execution.live_venues import ExecutionUnknown, OrderRejected
from arbitrage.observability import now_ms
from arbitrage.strategy.manual_orders import ManualOrderStrategy
from arbitrage.strategy.spread import entry_spread, pending_spread

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
        if await self.binance.open_orders():
            raise ExecutionUnknown("Binance 存在未核对挂单，禁止继续执行")
        expected = {"LONG": D(0), "SHORT": D(0)}
        positions = await self.mt5.positions()
        owned = set()
        for t in self.trades.values():
            if t["state"] == "REVIEW":
                raise ExecutionUnknown("存在中断交易，请在两平台核对后处理恢复记录")
            if t["state"] != "OPEN":
                continue
            expected[t["position_side"]] += D(t["filled_qty"])
            matches = [p for p in positions if p["ticket"] == t["mt5_ticket"]]
            if len(matches) != 1 or D(matches[0]["volume"]) != D(t["lots"]):
                raise ExecutionUnknown("MT5 已记录持仓与账户不一致")
            p = matches[0]
            if p["magic"] != self.settings.live.mt5_magic or p["type"] != (0 if t["sell"] else 1):
                raise ExecutionUnknown("MT5 持仓方向或归属不一致")
            owned.add(p["ticket"])
        if any(p["ticket"] not in owned for p in positions):
            raise ExecutionUnknown("MT5 本品种存在外部持仓；请使用独立账户或品种")
        actual = {"LONG": D(0), "SHORT": D(0)}
        for p in await self.binance.inventory():
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
            self._reset_waiting()
        if (
            self.account_worker is None or self.account_worker.done()
        ) and now - self.accounts_updated_ms > 3000:
            self.account_worker = asyncio.create_task(self._accounts())

    async def on_quotes(self, binance, mt5, now):
        self.binance_quote, self.mt5_quote = binance, mt5
        await self.on_timer(now)
        key = tuple((q.exchange_ts_ms, q.bid, q.ask, q.bid_qty, q.ask_qty) for q in (binance, mt5))
        changed = key != self.last_manual_key
        self.last_manual_key = key
        if (
            self.stopping
            or self.state == StrategyState.SAFE_MODE
            or (self.worker and not self.worker.done())
        ):
            self._reset_waiting()
            return {}
        if self.guard.check(binance, mt5, now) or not changed:
            return {}
        for c in sorted(self.conditions.values(), key=lambda c: c.queue_seq):
            if c.state == "OPEN" and c.exit_threshold is not None:
                cost = (
                    binance.ask - mt5.bid
                    if c.direction == "SHORT_BINANCE"
                    else mt5.ask - binance.bid
                )
                if (
                    self._confirmation("exit:" + c.request_id)
                    .update(c.exit_threshold - cost, now)
                    .state
                    == "CONFIRMED"
                ):
                    await self._apply_command("close_threshold", c.request_id, now)
                    return {}
        eligible = []
        for c in self.conditions.values():
            if c.state == "WAITING":
                spread = entry_spread(c.direction, binance, mt5, c.entry_threshold)
                if self._confirmation(c.request_id).update(spread.edge, now).state == "CONFIRMED":
                    eligible.append(c)
        if eligible:
            c = min(eligible, key=lambda c: c.queue_seq)
            c.state = "EXECUTING"
            self.active_condition_id = c.request_id
            await self.repo.save_condition(c, "live_condition_reserved", now)
            self.worker = asyncio.create_task(self._execute(c))
            self._reset_waiting()
        return {}

    async def _save(self, t, event):
        t["updated_at_ms"] = now_ms()
        await self.repo.save_trade(t, event)

    async def _execute(self, c, trade=None, *, closing=False):
        t = trade
        try:
            await self._inventory_check()
            if closing:
                if c.close_reason == "threshold":
                    b, m, now = self.binance_quote, self.mt5_quote, now_ms()
                    invalid = self.guard.check(b, m, now)
                    cost = None if invalid else b.ask - m.bid if t["sell"] else m.ask - b.bid
                    if invalid or cost > c.exit_threshold:
                        c.state, c.close_requested, c.close_reason = "OPEN", False, None
                        return
                await self._close(t)
            else:
                # Preflight API calls may outlast quote freshness or change the spread.
                b, m, now = self.binance_quote, self.mt5_quote, now_ms()
                if self.stopping or c.cancel_requested or self.guard.check(b, m, now):
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
                    mt5_ticket=None,
                    created_at_ms=now,
                    updated_at_ms=now,
                    open_client_id="au" + trade_id,
                    direction=c.direction,
                    entry_spread=str(entry_spread(c.direction, b, m, c.entry_threshold).raw_spread),
                )
                self.trades[trade_id] = t
                c.execution_order_id = trade_id
                await self.repo.save_condition(c, "live_execution_linked", now)
                await self._open(c, t)
            if t["state"] == "OPEN":
                c.state = "OPEN"
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
            self.active_condition_id = None
            self._reset_waiting()
            await self.repo.save_condition(c, "live_condition_updated", now_ms())

    async def _review(self, c, t, exc):
        self.state = StrategyState.SAFE_MODE
        self.risk_error = str(exc)
        c.state, c.last_result = "REVIEW", str(exc)
        if t:
            t["state"], t["error"] = "REVIEW", str(exc)
            await self._save(t, "live_execution_uncertain")

    async def _open(self, c, t):
        await self._save(t, "live_entry_intent")  # Durable before any venue mutation.
        try:
            result = await self.binance.submit(
                t["open_client_id"],
                "SELL" if t["sell"] else "BUY",
                t["position_side"],
                D(t["quantity"]),
                price=D(t["price"]),
            )
        except ExecutionUnknown:
            # Cancel by the durable client ID; never send a second entry POST.
            result = await self.binance.cancel(t["open_client_id"])
        self.orders_created += 1
        t["state"] = "MAKER_PENDING"
        policy = CancelPolicy(
            c.cancel_threshold,
            self.settings.maker.cancel_confirm_ms,
            self.settings.maker.max_pending_ms,
        )
        while result["status"] not in END:
            # Freeze cumulative fill before hedging: cancel remaining quantity on the first fill.
            now = now_ms()
            cancel = self.stopping or c.cancel_requested or D(result["executedQty"]) > 0
            if self.guard.check(self.binance_quote, self.mt5_quote, now):
                cancel = True
            else:
                spread = pending_spread(c.direction, D(t["price"]), self.mt5_quote)
                cancel = cancel or policy.update(spread, now, t["created_at_ms"])
            if cancel:
                t["state"] = "CANCEL_INTENT"
                await self._save(t, "live_cancel_intent")
                result = await self.binance.cancel(t["open_client_id"])
                break
            await asyncio.sleep(self.settings.live.order_poll_ms / 1000)
            try:
                result = await self.binance.order(t["open_client_id"])
            except (ExecutionUnknown, OrderRejected):
                result = await self.binance.cancel(t["open_client_id"])
        filled = D(result["executedQty"])
        t.update(binance_open=result, filled_qty=str(filled))
        if not filled:
            t["state"] = "CANCELED"
            await self._save(t, "live_entry_unfilled")
            return
        if filled > D(t["quantity"]):
            raise ExecutionUnknown("Binance fill exceeds requested quantity")
        try:
            lots = self.hedge.binance_to_lots(filled)
        except ValueError:
            await self._flatten_unhedged(t)
            return
        t.update(state="HEDGE_INTENT", lots=str(lots))
        await self._save(t, "live_hedge_intent")
        try:
            result = await self.mt5.send("au" + t["trade_id"][:20], t["sell"], lots)
        except OrderRejected:
            await self._flatten_unhedged(t)
            return
        t["mt5_open"] = result
        positions = await self.mt5.positions()
        matches = [p for p in positions if p["ticket"] == result["order"]]
        if len(matches) != 1 or D(matches[0]["volume"]) != lots:
            raise ExecutionUnknown("MT5 hedge filled but position ticket needs reconciliation")
        p = matches[0]
        t.update(mt5_ticket=p["ticket"], mt5_identifier=p["identifier"], state="OPEN")
        await self._save(t, "live_pair_opened")

    async def _market_close_binance(self, t, key):
        t[key] = "au" + uuid4().hex
        await self._save(t, "live_binance_close_intent")
        result = await self.binance.submit(
            t[key], "BUY" if t["sell"] else "SELL", t["position_side"], D(t["filled_qty"])
        )
        if result["status"] != "FILLED" or D(result["executedQty"]) != D(t["filled_qty"]):
            raise ExecutionUnknown("Binance close incomplete; do not retry")
        t["binance_close"] = result
        await self._save(t, "live_binance_closed")

    async def _flatten_unhedged(self, t):
        t["state"] = "COMPENSATE_INTENT"
        await self._market_close_binance(t, "compensate_client_id")
        t["state"] = "FAILED"
        t["error"] = "无法精确对冲，已撤销剩余挂单并平掉 Binance 实际成交量"
        await self._save(t, "live_unhedged_flattened")

    async def _close(self, t):
        t["state"] = "CLOSE_INTENT"
        await self._market_close_binance(t, "close_client_id")
        t["state"] = "MT5_CLOSE_INTENT"
        await self._save(t, "live_mt5_close_intent")
        t["mt5_close"] = await self.mt5.send(
            "ac" + t["trade_id"][:20], not t["sell"], D(t["lots"]), ticket=t["mt5_ticket"]
        )
        if any(p["ticket"] == t["mt5_ticket"] for p in await self.mt5.positions()):
            raise ExecutionUnknown("MT5 close acknowledged but position remains")
        t["state"] = "CLOSED"
        await self._save(t, "live_pair_closed")

    async def _accounts(self):
        try:
            self.accounts = {
                "binance": await self.binance.account(),
                "mt5": await self.mt5.account(),
            }
            # Fee and realized PnL evidence remains separately denominated, never invented.
            for t in list(self.trades.values()):
                if t["state"] == "CLOSED" and "settlement" not in t:
                    trades = []
                    for key in ("binance_open", "binance_close"):
                        rows = await self.binance.trades(t[key]["orderId"])
                        if sum((D(r["qty"]) for r in rows), D(0)) != D(t["filled_qty"]):
                            break
                        trades.extend(rows)
                    else:
                        deals = await self.mt5.deals(t["mt5_identifier"])
                        if any(
                            sum((D(d["volume"]) for d in deals if d["entry"] == side), D(0))
                            != D(t["lots"])
                            for side in (0, 1)
                        ):
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
