import asyncio
from decimal import Decimal as D

from test_live_orders import engine, trigger
from test_manual_orders import command, request

from arbitrage.execution.live_venues import ExecutionUnknown, OrderRejected, PostOnlyWouldMatch
from arbitrage.persistence.sqlite_repository import SQLiteRepository


async def test_post_only_race_requeues_without_hedge(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "race.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        e.binance.error = PostOnlyWouldMatch("race")
        await command(e, "create", request())
        await trigger(e)
        c = next(iter(e.conditions.values()))
        assert c.state == "WAITING"
        assert not e.mt5.sent and len(e.binance.sent) == 1
        assert e._confirmation(c.request_id).result.count == 0
        await e.shutdown(1200)


async def test_unknown_increment_is_never_sent_twice(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "unknown.db") as repo:
        e = engine(repo, monkeypatch)
        e.settings = e.settings.model_copy(
            update={"live": e.settings.live.model_copy(update={"max_binance_qty": D(2)})}
        )
        await e.start(900)
        e.binance.fill = D(1)
        e.mt5.error = ExecutionUnknown("no acknowledgement")
        await command(e, "create", request(quantity="2"))
        await trigger(e)
        assert e.state == "SAFE_MODE"
        assert len(e.mt5.sent) == 1
        assert e.binance.canceled == 1
        assert len(e.binance.sent) == 1
        assert next(iter(e.trades.values()))["hedge_intents"][0]["state"] == "INTENT"
        await e.shutdown(1200)


async def test_representable_partial_is_kept_only_residual_compensated(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "remainder.db") as repo:
        e = engine(repo, monkeypatch)
        e.settings = e.settings.model_copy(
            update={"live": e.settings.live.model_copy(update={"max_binance_qty": D(2)})}
        )
        await e.start(900)
        e.binance.fill = D("1.3")
        await command(e, "create", request(quantity="2"))
        await trigger(e)
        t = next(iter(e.trades.values()))
        assert t["state"] == "OPEN"
        assert D(t["paired_qty"]) == 1
        assert e.binance.sent[-1][2:] == (D("0.3"), None)
        await e._inventory_check()
        e.binance.fill = D(1)
        await command(e, "close", t["condition_id"], 1200)
        await e.worker
        assert t["state"] == "CLOSED"
        assert e.binance.sent[-1][2] == 1
        await e._inventory_check()
        await e.shutdown(1200)


async def test_partial_maker_close_keeps_exact_remaining_ticket(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "partial-close.db") as repo:
        e = engine(repo, monkeypatch)
        e.settings = e.settings.model_copy(
            update={"live": e.settings.live.model_copy(update={"max_binance_qty": D(2)})}
        )
        await e.start(900)
        e.binance.fill = D(2)
        r = request(quantity="2")
        await command(e, "create", r)
        await trigger(e)
        e.binance.fill = D(1)
        await command(e, "close", r["request_id"], 1200)
        await e.worker
        t = next(iter(e.trades.values()))
        assert t["state"] == "OPEN"
        assert D(t["closed_qty"]) == 1
        assert e.mt5.rows[0]["volume"] == "0.01"
        await e._inventory_check()
        await command(e, "close", r["request_id"], 1200)
        await e.worker
        assert t["state"] == "CLOSED"
        assert e.binance.sent[-1][2] == 1
        assert not e.mt5.rows
        await e.shutdown(1200)


async def test_default_one_pair_blocks_second_entry(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "single.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        await command(e, "create", request())
        await trigger(e)
        await command(e, "create", request())
        await trigger(e)
        assert len(e.binance.sent) == 1
        await e.shutdown(1200)


async def test_partial_fill_hedges_before_cancel_finishes(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "delta.db") as repo:
        e = engine(repo, monkeypatch)
        e.settings = e.settings.model_copy(
            update={"live": e.settings.live.model_copy(update={"max_binance_qty": D(2)})}
        )
        await e.start(900)
        e.binance.fill = D(1)
        gate = asyncio.Event()
        entered = asyncio.Event()

        async def cancel(client):
            entered.set()
            await gate.wait()
            e.binance.amounts["SHORT"] = D(2)
            return dict(status="CANCELED", executedQty="2", orderId=1, avgPrice="4400")

        e.binance.cancel = cancel
        await command(e, "create", request(quantity="2"))
        task = asyncio.create_task(trigger(e))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            for _ in range(30):
                if e.mt5.sent:
                    break
                await asyncio.sleep(0.01)
            assert e.mt5.sent == [(True, D("0.01"), None)]
            t = next(iter(e.trades.values()))
            update = dict(
                e="ORDER_TRADE_UPDATE",
                T=1200,
                o=dict(
                    s="XAUUSDT",
                    c=t["open_client_id"],
                    i=1,
                    X="FILLED",
                    z="2",
                    q="2",
                    ap="4400",
                    ps="SHORT",
                    S="SELL",
                ),
            )
            e.on_user_event(update)
            e.on_user_event(update)
            for _ in range(30):
                if len(e.mt5.sent) == 2:
                    break
                await asyncio.sleep(0.01)
            assert len(e.mt5.sent) == 2  # New delta processed while DELETE is still outstanding.
            gate.set()
            await asyncio.wait_for(task, 1)
            assert len(e.mt5.sent) == 2
            t = next(iter(e.trades.values()))
            assert D(t["mt5_hedged_qty"]) == D(2)
            assert len(t["mt5_legs"]) == 2
        finally:
            gate.set()
            await task
            await e.shutdown(1200)


async def test_normal_close_uses_maker(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "close.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        await command(e, "close", r["request_id"], 1200)
        await e.worker
        assert e.binance.sent[-1][3] is not None
        await e.shutdown(1200)


async def test_rejected_mt5_close_restores_only_confirmed_unclosed_binance_qty(
    tmp_path, monkeypatch
):
    async with SQLiteRepository(tmp_path / "restore.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        e.mt5.error = OrderRejected("precheck rejects before send")
        await command(e, "close", r["request_id"], 1200)
        await e.worker
        t = next(iter(e.trades.values()))
        assert t["state"] == "OPEN"
        assert e.binance.sent[-1] == ("SELL", "SHORT", D(1), None)
        assert len(e.mt5.rows) == 1
        await e._inventory_check()
        await e.shutdown(1200)


async def test_net_target_unknown_budget_never_triggers(tmp_path, monkeypatch):
    from arbitrage.config import ProfitBudget

    async with SQLiteRepository(tmp_path / "profit.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request(min_net_profit="0.8")
        await command(e, "create", r)
        await trigger(e)
        c = e.conditions[r["request_id"]]
        t = next(iter(e.trades.values()))
        t["actual_entry_spread"] = "6"
        await trigger(e)
        assert not c.close_requested
        budget = ProfitBudget(
            quote_units_aligned=True, open_fees="0.1", close_fees="0.1", funding="0", swap="0"
        )
        e.settings = e.settings.model_copy(
            update={"live": e.settings.live.model_copy(update={"profit_budget": budget})}
        )
        await trigger(e)
        assert c.close_requested
        await e.shutdown(1200)


async def test_cancel_unknown_after_hedge_remains_review_without_compensation(
    tmp_path, monkeypatch
):
    async with SQLiteRepository(tmp_path / "cancel-unknown.db") as repo:
        e = engine(repo, monkeypatch)
        e.settings = e.settings.model_copy(
            update={"live": e.settings.live.model_copy(update={"max_binance_qty": D(2)})}
        )
        await e.start(900)
        e.binance.fill = D(1)

        async def cancel(client):
            await asyncio.sleep(0.05)
            raise ExecutionUnknown("terminal unknown")

        e.binance.cancel = cancel
        await command(e, "create", request(quantity="2"))
        await trigger(e)
        assert len(e.mt5.sent) == 1
        assert len(e.binance.sent) == 1
        assert e.state == "SAFE_MODE"
        assert next(iter(e.trades.values()))["state"] == "REVIEW"
        await e.shutdown(1200)
