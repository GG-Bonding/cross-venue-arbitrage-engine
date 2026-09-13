import asyncio
from decimal import Decimal as D

import pytest
from test_core import quote
from test_execution import spec
from test_manual_orders import command, request

from arbitrage.config import Settings
from arbitrage.domain.specs import MT5Spec
from arbitrage.execution.live_venues import ExecutionUnknown, OrderRejected
from arbitrage.main import read_status
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.live_orders import LiveOrderStrategy


@pytest.mark.parametrize("direction", ["SHORT_BINANCE", "LONG_BINANCE"])
async def test_exit_confirmation_survives_busy_entry_worker(tmp_path, monkeypatch, direction):
    async with SQLiteRepository(tmp_path / "exit.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        c = e.conditions[r["request_id"]]
        c.direction, c.exit_threshold = direction, D("10")
        gate = asyncio.Event()
        e.worker = asyncio.create_task(gate.wait())
        try:
            for ts in (1300, 1400, 1500):
                await e.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
            assert c.close_requested
            assert c.close_reason == "threshold"
            assert len(e.binance.sent) == 1
        finally:
            gate.set()
            await e.worker
            await e.shutdown(1500)


async def test_actual_spreads_and_compensation_settlement(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "pnl.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        t = next(iter(e.trades.values()))
        assert D(t["actual_entry_spread"]) == 0  # Both actual fills are 4400.
        assert D(t["signal_entry_spread"]) > 4
        view = e.condition_views(1200)[0]
        assert D(view["estimated_spread_gain"]) < 0
        await command(e, "close", r["request_id"], 1200)
        await e.worker
        assert D(t["actual_exit_spread"]) == 0
        assert D(t["gross_pnl"]) == 0
        e.binance.fill = D("0.5")
        await command(e, "create", request())
        await trigger(e)
        failed = list(e.trades.values())[-1]
        assert failed["state"] == "FAILED"
        queried = []

        async def trades(order_id):
            queried.append(order_id)
            return [dict(qty="0.5", realizedPnl="-0.25", commission="0", commissionAsset="USDT")]

        e.binance.trades = trades
        await e._accounts()
        assert failed["binance_open"]["orderId"] in queried
        assert failed["settlement"]["summary"]["binance_net"] == D("-0.5")
        await e.shutdown(1400)


@pytest.mark.parametrize("outcome", ["zero", "filled", "unknown", "vanished"])
async def test_confirmed_exit_cancels_pending_entry_before_closing(tmp_path, monkeypatch, outcome):
    async with SQLiteRepository(tmp_path / "priority.db") as repo:
        e = engine(repo, monkeypatch)
        # Legacy multi-pair execution fixture: isolate exit scheduling from new V1 admission.
        monkeypatch.setattr(e, "_entry_capacity", lambda: True)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        old = e.conditions[r["request_id"]]
        current = [1200]
        monkeypatch.setattr("arbitrage.strategy.live_orders.now_ms", lambda: current[0])
        waiting = asyncio.Event()
        ids = []

        original_maker = e.binance.submit_maker

        async def maker(client, side, position_side, quantity, *, price):
            if side == "BUY":
                return await original_maker(client, side, position_side, quantity, price=price)
            ids.append(client)
            e.binance.sent.append((side, position_side, quantity, price))
            return {"orderId": 2}  # Initial ACK query is deliberately slow.

        async def query(client):
            assert client == ids[0]
            waiting.set()
            await asyncio.Event().wait()

        async def cancel(client):
            assert client == ids[0]
            e.binance.canceled += 1
            if outcome == "unknown":
                raise ExecutionUnknown("unconfirmed cancel")
            qty = D(1) if outcome == "filled" else D(0)
            e.binance.amounts["SHORT"] += qty
            return dict(status="CANCELED", executedQty=str(qty), orderId=2, avgPrice="4400")

        e.binance.submit_maker, e.binance.order, e.binance.cancel = maker, query, cancel
        await command(e, "create", request())
        for ts in (1300, 1400, 1500):
            current[0] = ts
            await e.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
        await asyncio.wait_for(waiting.wait(), 1)
        old.exit_threshold = D(5)
        try:
            for ts in (1600, 1700, 1800):
                current[0] = ts
                await e.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
            await asyncio.wait_for(asyncio.shield(e.worker), 1)
            assert e.binance.canceled == 1
            assert len(ids) == 1
            assert len(e.binance.sent) == 2  # No exit before original maker reconciliation.
            if outcome == "filled":
                assert len(e.mt5.sent) == 2  # Newly proven fill has been hedged first.
            if outcome == "vanished":
                e.binance_quote = quote("4426", "4427", 1800)
            await e.on_timer(1800)
            await e.worker
            if outcome in {"vanished", "unknown"}:
                assert len(e.binance.sent) == 2
                assert old.state == "OPEN"
            else:
                assert len(e.binance.sent) == 3
                assert e.mt5.sent[-1][2] == 101
                assert old.state == "DONE"
        finally:
            await e.shutdown(1800)


class Binance:
    async def identity(self):
        return "test-binance"

    def __init__(self):
        self.sent = []
        self.amounts = {"LONG": D(0), "SHORT": D(0)}
        self.fill = D(1)
        self.error = None
        self.result = None
        self.canceled = 0

    async def initialize(self):
        pass

    async def inventory(self):
        return [{"positionSide": k, "positionAmt": str(v)} for k, v in self.amounts.items()]

    async def open_orders(self):
        return []

    async def submit_maker(self, client_id, side, position_side, quantity, *, price):
        return await self._submit(client_id, side, position_side, quantity, price=price)

    async def submit_market(self, client_id, side, position_side, quantity):
        return await self._submit(client_id, side, position_side, quantity)

    async def _submit(self, client_id, side, position_side, quantity, *, price=None):
        from arbitrage.strategy.live_orders import monotonic_ns

        self.dispatch_observer(client_id, monotonic_ns())
        self.sent.append((side, position_side, quantity, price))
        if self.error:
            raise self.error
        filled = self.fill if price is not None else quantity
        opening = (side == "SELL") == (position_side == "SHORT")
        self.amounts[position_side] += filled if opening else -filled
        self.result = dict(
            status="FILLED" if filled == quantity else "PARTIALLY_FILLED",
            executedQty=str(filled),
            orderId=len(self.sent),
            avgPrice="4400",
        )
        return self.result

    async def order(self, client_id):
        return self.result

    async def cancel(self, client_id):
        self.canceled += 1
        return {**self.result, "status": "CANCELED"}

    async def account(self):
        return dict(currency="USDT", equity="1000", available="900", profit="0")

    async def trades(self, order_id):
        return []


class MT5:
    async def identity(self):
        return "test-mt5"

    def __init__(self):
        self.sent, self.rows = [], []
        self.error = None

    async def initialize(self):
        pass

    async def positions(self):
        return self.rows.copy()

    async def send(self, tag, buy, lots, *, ticket=None):
        from arbitrage.strategy.live_orders import monotonic_ns

        self.dispatch_observer(tag, monotonic_ns())
        self.sent.append((buy, lots, ticket))
        if self.error:
            raise self.error
        if ticket:
            for p in self.rows:
                if p["ticket"] == ticket:
                    p["volume"] = str(D(p["volume"]) - lots)
            self.rows = [p for p in self.rows if D(p["volume"]) > 0]
            order = ticket
        else:
            order = 100 + len(self.sent)
            self.rows.append(
                dict(
                    ticket=order,
                    identifier=order,
                    volume=str(lots),
                    type=0 if buy else 1,
                    magic=260911,
                )
            )
        return dict(order=order, deal=order, volume=str(lots), price="4400")

    async def account(self):
        return dict(currency="USD", equity="1000", available="900", profit="0")

    async def deals(self, identifier):
        return []


def engine(repo, monkeypatch):
    monkeypatch.setattr("arbitrage.strategy.live_orders.now_ms", lambda: 1200)
    monkeypatch.setenv("CONFIRM_LIVE_TRADING", "I_UNDERSTAND")
    monkeypatch.setenv("BINANCE_API_KEY", "test-only-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "test-only-secret")
    settings = Settings(
        mode="live", live={"enabled": True}, trading={"binance_underlying_per_qty": "1"}
    )
    return LiveOrderStrategy(
        settings,
        spec(),
        repo,
        Binance(),
        MT5(),
        MT5Spec("XAUUSD", D(100), D("0.01"), D(100), D("0.01")),
    )


async def trigger(e):
    for ts in (1000, 1100, 1200):
        await e.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
    if e.worker:
        await e.worker


@pytest.mark.parametrize("missing", ["both", "status", "executedQty"])
@pytest.mark.parametrize(
    "outcome", ["zero", "filled", "error_zero", "error_fill", "missing", "unknown"]
)
async def test_maker_ack_requires_original_order_reconciliation(
    tmp_path, monkeypatch, missing, outcome
):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        submitted, queried, canceled = [], [], []

        async def ack(client_id, side, position_side, quantity, *, price):
            submitted.append(client_id)
            result = {"orderId": 1, "status": "NEW", "executedQty": "0"}
            for field in ["status", "executedQty"] if missing == "both" else [missing]:
                result.pop(field)
            return result

        def final():
            filled = outcome in {"filled", "error_fill"}
            return dict(
                status="FILLED" if filled else "CANCELED",
                executedQty="1" if filled else "0",
                orderId=1,
            )

        async def query(client_id):
            assert not e.mt5.sent  # Acceptance alone must never hedge.
            queried.append(client_id)
            if outcome == "missing":
                raise OrderRejected("order not found")
            if outcome.startswith("error") or outcome == "unknown":
                raise ExecutionUnknown("query timeout")
            return final()

        async def cancel(client_id):
            canceled.append(client_id)
            if outcome in {"missing", "unknown"}:
                raise ExecutionUnknown("final quantity unknown")
            return final()

        e.binance.submit_maker = ack
        e.binance.order, e.binance.cancel = query, cancel
        await trigger(e)
        assert len(submitted) == 1
        assert queried == submitted
        assert canceled == (submitted if outcome not in {"zero", "filled"} else [])
        c = e.conditions[r["request_id"]]
        if outcome in {"missing", "unknown"}:
            assert c.state == "REVIEW" and e.state == "SAFE_MODE"
            assert not e.mt5.sent
        elif outcome in {"filled", "error_fill"}:
            assert c.state == "OPEN"
            assert e.mt5.sent == [(True, D("0.01"), None)]
        else:
            assert c.state == "DONE" and not e.mt5.sent
        await e.shutdown(1400)


@pytest.mark.parametrize("direction", ["SHORT_BINANCE", "LONG_BINANCE"])
async def test_live_cancel_uses_resting_price(tmp_path, monkeypatch, direction):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        clock = [1200]
        monkeypatch.setattr("arbitrage.strategy.live_orders.now_ms", lambda: clock[0])
        e.binance.fill = D(0)
        await e.start(900)
        r = request(direction=direction, entry_threshold="-10", cancel_threshold="-11")
        await command(e, "create", r)
        queries = []

        async def moving_book(client_id):
            queries.append(client_id)
            clock[0] += 50
            ts = clock[0]
            if direction == "SHORT_BINANCE":
                e.binance_quote, e.mt5_quote = quote("4440", "4441", ts), quote("4430", "4431", ts)
            else:
                e.binance_quote, e.mt5_quote = quote("4390", "4391", ts), quote("4400", "4401", ts)
            if len(queries) >= 3:
                raise ExecutionUnknown("test deadline: resting order was not canceled")
            return e.binance.result

        e.binance.order = moving_book
        await trigger(e)
        assert len(queries) == 2  # Two observations meet the 50 ms cancellation duration.
        assert e.binance.canceled == 1
        assert len(e.binance.sent) == 1
        assert not e.mt5.sent
        assert e.conditions[r["request_id"]].state == "DONE"
        await e.shutdown(clock[0])


async def test_live_no_manual_request_never_sends(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        await trigger(e)
        assert e.binance.sent == e.mt5.sent == []
        await e.shutdown(1400)


async def test_execution_timings_use_monotonic_clock_and_persist(tmp_path, monkeypatch):
    import itertools

    clock = itertools.count(0, 1000000)
    monkeypatch.setattr("arbitrage.strategy.live_orders.monotonic_ns", lambda: next(clock))
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        await command(e, "create", request())
        await trigger(e)
        saved = (await repo.load_trades())[0]
        assert saved["timings_ns"]["entry_dispatch"] > 0
        assert saved["timings_ns"]["hedge_dispatch"] > 0
        assert "cancel_dispatch" not in saved["timings_ns"]
        assert len(e.binance.sent) == len(e.mt5.sent) == 1
        await e.shutdown(1400)


async def test_quote_cancel_does_not_wait_for_slow_order_query(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        e.binance.fill = D(0)
        r = request()
        await command(e, "create", r)
        started, canceled = asyncio.Event(), asyncio.Event()

        async def slow_query(client_id):
            started.set()
            await asyncio.Event().wait()

        async def cancel(client_id):
            e.binance.canceled += 1
            canceled.set()
            return {"status": "CANCELED", "executedQty": "0"}

        e.binance.order, e.binance.cancel = slow_query, cancel
        for ts in (1000, 1100, 1200):
            await e.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
        await asyncio.wait_for(started.wait(), 1)
        e.conditions[r["request_id"]].cancel_requested = True
        try:
            await asyncio.wait_for(canceled.wait(), 0.5)
        finally:
            if not canceled.is_set():
                e.worker.cancel()
            await asyncio.gather(e.worker, return_exceptions=True)
        assert e.binance.canceled == 1
        assert len(e.binance.sent) == 1 and not e.mt5.sent


async def test_fill_push_before_ack_hedges_once_without_rest_query(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        await command(e, "create", request())

        async def submit(client, side, position_side, quantity, *, price):
            e.binance.sent.append(client)
            payload = {
                "e": "ORDER_TRADE_UPDATE",
                "T": 1200,
                "o": {
                    "s": "XAUUSDT",
                    "c": client,
                    "i": 1,
                    "X": "FILLED",
                    "z": "1",
                    "ap": "4417",
                    "ps": "SHORT",
                    "S": "SELL",
                    "q": "1",
                },
            }
            e.on_user_event(payload)
            e.on_user_event(payload)
            assert not e.mt5.sent
            return {"orderId": 1}

        async def unexpected_query(client):
            raise AssertionError("Complete push already supplies execution evidence")

        e.binance.submit_maker, e.binance.order = submit, unexpected_query
        await trigger(e)
        assert len(e.binance.sent) == len(e.mt5.sent) == 1
        assert next(iter(e.conditions.values())).state == "OPEN"
        await e.shutdown(1400)


async def test_inventory_checks_start_independently_and_accounts_defer(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        entered = []
        ready = asyncio.Event()

        async def read(name, result):
            entered.append(name)
            if len(entered) == 3:
                ready.set()
            await ready.wait()
            return result

        e.binance.open_orders = lambda: read("orders", [])
        e.binance.inventory = lambda: read("inventory", [])
        e.mt5.positions = lambda: read("mt5", [])
        await asyncio.wait_for(e._inventory_check(), 1)
        assert set(entered) == {"orders", "inventory", "mt5"}
        busy = asyncio.Event()
        e.worker = asyncio.create_task(busy.wait())
        e.accounts_updated_ms = 0
        await e.on_timer(10000)
        assert e.account_worker is None
        busy.set()
        await e.worker
        e.worker = None
        await e.shutdown(1400)


async def test_disconnected_stream_wakes_rest_without_healthy_stream_delay(tmp_path, monkeypatch):
    from arbitrage.execution.binance_maker import CancelPolicy

    async with SQLiteRepository(tmp_path / "disconnect.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        c = e.conditions[r["request_id"]]
        e.binance_quote, e.mt5_quote = quote("4416.90", "4416.98", 1200), quote(ts=1200)
        e.user_updates.connected = True
        queried = []

        async def query(client):
            queried.append(client)
            return dict(status="CANCELED", executedQty="0")

        e.binance.order = query
        t = dict(open_client_id="original", price="4416.98", created_at_ms=1200)
        task = asyncio.create_task(e._query_or_cancel(c, t, CancelPolicy(D(4), 50, 2000), {}))
        try:
            await asyncio.sleep(0)
            assert not queried
            e.user_updates.connected = False
            e.execution_wakeup.set()
            result = await asyncio.wait_for(task, 0.5)
            assert result["executedQty"] == "0"
            assert queried == ["original"]
            assert not e.binance.sent and not e.mt5.sent
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await e.shutdown(1200)


@pytest.mark.parametrize("direction", ["SHORT_BINANCE", "LONG_BINANCE"])
async def test_real_adapter_flow_opens_and_ticket_closes_both_sides(
    tmp_path, monkeypatch, direction
):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request(direction=direction, entry_threshold="-10", cancel_threshold="-11")
        await command(e, "create", r)
        await trigger(e)
        c = e.conditions[r["request_id"]]
        assert c.state == "OPEN"
        sell = direction == "SHORT_BINANCE"
        assert e.binance.sent[0][:3] == (
            "SELL" if sell else "BUY",
            "SHORT" if sell else "LONG",
            D(1),
        )
        assert e.mt5.sent[0] == (sell, D("0.01"), None)
        await command(e, "close", c.request_id, 1250)
        await e.worker
        assert c.state == "DONE" and c.last_result == "CLOSED"
        assert e.mt5.sent[1] == (not sell, D("0.01"), 101)
        assert all(v == 0 for v in e.binance.amounts.values()) and e.mt5.rows == []
        records = await repo.load_trades()
        assert records[0]["state"] == "CLOSED"
        status = read_status(tmp_path / "live.db")
        assert status["mode"] == "live" and status["unfinished_orders"] == []
        await e.shutdown(1400)


async def test_live_fifo_only_one_submission_per_confirmation(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        a, b = request(), request()
        await command(e, "create", a)
        await command(e, "create", b)
        await trigger(e)
        assert len(e.binance.sent) == 1
        assert e.conditions[a["request_id"]].state == "OPEN"
        assert e.conditions[b["request_id"]].state == "WAITING"
        await e.shutdown(1400)


async def test_partial_fill_unrepresentable_cancels_then_flattens(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        e.binance.fill = D("0.5")
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        assert e.binance.canceled == 1
        assert e.binance.sent[1] == ("BUY", "SHORT", D("0.5"), None)
        assert not e.mt5.sent
        assert e.conditions[r["request_id"]].state == "FAILED"
        assert e.binance.amounts["SHORT"] == 0
        await e.shutdown(1400)


@pytest.mark.parametrize("venue", ["binance", "mt5"])
async def test_unknown_execution_blocks_following_orders_and_survives_restart(
    tmp_path, monkeypatch, venue
):
    path = tmp_path / "live.db"
    async with SQLiteRepository(path) as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        getattr(e, venue).error = ExecutionUnknown("ambiguous acknowledgement")
        r = request()
        await command(e, "create", r)
        await trigger(e)
        assert e.state == "SAFE_MODE" and e.conditions[r["request_id"]].state == "REVIEW"
        assert len(e.binance.sent) == 1
        with pytest.raises(ValueError):
            await command(e, "create", request())
        with pytest.raises(ValueError):
            await command(e, "cancel", r["request_id"])
        await e.shutdown(1400)
    async with SQLiteRepository(path) as repo:
        restored = engine(repo, monkeypatch)
        await restored.start(1500)
        assert restored.state == "SAFE_MODE"
        await trigger(restored)
        assert not restored.binance.sent
        await restored.shutdown(1600)


async def test_definitive_hedge_precheck_rejection_flattens_binance(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        e.mt5.error = OrderRejected("precheck")
        r = request()
        await command(e, "create", r)
        await trigger(e)
        assert len(e.binance.sent) == 2 and e.binance.amounts["SHORT"] == 0
        assert e.conditions[r["request_id"]].state == "FAILED"
        await e.shutdown(1400)


async def test_external_positions_block_live_start(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        e.binance.amounts["LONG"] = D(1)
        await e.start(900)
        assert e.state == "SAFE_MODE"
        with pytest.raises(ValueError):
            await command(e, "create", request())
        await e.shutdown(1400)


async def test_paper_database_cannot_be_reused_for_live(tmp_path):
    async with SQLiteRepository(tmp_path / "db") as repo:
        await repo.bind_mode("paper")
        with pytest.raises(ValueError, match="数据库"):
            await repo.bind_mode("live")


async def test_bulk_close_runs_serially_and_repeat_requeues_after_close(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        # Existing multiple OPEN records can still be closed serially after migration.
        monkeypatch.setattr(e, "_entry_capacity", lambda: True)
        await e.start(900)
        a, b = request(repeat=True), request()
        await command(e, "create", a)
        await command(e, "create", b)
        await trigger(e)
        await trigger(e)
        assert [c.state for c in e.conditions.values()] == ["OPEN", "OPEN"]
        result = await command(e, "close_all", None, 1300)
        assert result["queued"] == 2
        await e.worker
        assert len(e.binance.sent) == 3
        assert e.conditions[b["request_id"]].state == "OPEN"
        await e.on_timer(1350)
        await e.worker
        assert len(e.binance.sent) == 4
        assert e.conditions[a["request_id"]].state == "WAITING"
        assert e.conditions[b["request_id"]].state == "DONE"
        await e.shutdown(1400)


async def test_conditional_exit_uses_maker_quotes_and_rechecks_before_send(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request(exit_threshold="4.39")
        await command(e, "create", r)
        await trigger(e)
        c = e.conditions[r["request_id"]]
        # Entry basis 4.36 passes, but closing BUY maker bid - MT5 bid = 4.40 does not.
        await trigger(e)
        assert c.state == "OPEN" and not c.close_requested
        c.exit_threshold = D("5")
        await trigger(e)
        assert c.close_requested
        await e.on_timer(1200)
        # Before preflight completes the exit price deteriorates; do not execute queued target.
        e.binance_quote = quote("4419.90", "4419.98", 1200)
        await e.worker
        assert c.state == "OPEN" and len(e.binance.sent) == 1
        await e.shutdown(1400)


async def test_known_open_pair_reconciles_after_restart_without_reopening(tmp_path, monkeypatch):
    path = tmp_path / "live.db"
    async with SQLiteRepository(path) as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        await e.shutdown(1400)
        venues = e.binance, e.mt5
    async with SQLiteRepository(path) as repo:
        restored = engine(repo, monkeypatch)
        restored.binance, restored.mt5 = venues
        await restored.start(1500)
        assert restored.state != "SAFE_MODE"
        assert restored.conditions[r["request_id"]].state == "OPEN"
        await trigger(restored)
        assert len(restored.binance.sent) == 1
        await restored.shutdown(1600)


async def test_failed_second_close_leg_blocks_every_later_execution(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        e.mt5.error = ExecutionUnknown("close acknowledgement lost")
        await command(e, "close", r["request_id"], 1300)
        await e.worker
        assert e.state == "SAFE_MODE" and len(e.binance.sent) == 2
        with pytest.raises(ValueError):
            await command(e, "close", r["request_id"], 1350)
        await e.shutdown(1400)


async def test_representable_partial_fill_hedges_actual_not_requested_quantity(
    tmp_path, monkeypatch
):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        e.settings = e.base_settings = e.settings.model_copy(
            update={"live": e.settings.live.model_copy(update={"max_binance_qty": D(2)})}
        )
        await e.start(900)
        r = request(quantity="2")
        await command(e, "create", r)
        await trigger(e)
        assert e.binance.canceled == 1
        assert e.conditions[r["request_id"]].state == "OPEN"
        assert e.mt5.sent == [(True, D("0.01"), None)]
        t = next(iter(e.trades.values()))
        assert t["quantity"] == "2" and t["filled_qty"] == "1"
        await e.shutdown(1400)


async def test_settlement_requires_complete_volume_and_keeps_fee_currencies(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        await command(e, "close", r["request_id"], 1300)
        await e.worker
        t = next(iter(e.trades.values()))
        await e._accounts()
        assert "settlement" not in t

        async def trades(order_id):
            return [
                dict(
                    qty="1",
                    realizedPnl="0" if order_id == 1 else "2",
                    commission="0.1",
                    commissionAsset="USDT" if order_id == 1 else "BNB",
                )
            ]

        async def deals(identifier):
            return [
                dict(entry=i, volume="0.01", profit=str(i), commission="-0.1", swap="0", fee="0")
                for i in (0, 1)
            ]

        e.binance.trades, e.mt5.deals = trades, deals
        await e._accounts()
        summary = t["settlement"]["summary"]
        assert summary["binance_net"] == D("1.9")
        assert summary["binance_fees"] == {"USDT": D("0.1"), "BNB": D("0.1")}
        assert summary["mt5_net"] == D("0.8")
        await e.shutdown(1400)
