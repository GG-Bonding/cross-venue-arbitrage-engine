from uuid import uuid4

import pytest
from test_core import quote
from test_execution import spec

from arbitrage.audit import audit_session
from arbitrage.config import Settings
from arbitrage.domain.enums import Direction, PlacementMode
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.manual_orders import ManualOrderStrategy


def request(**updates):
    return {
        "request_id": str(uuid4()),
        "direction": "SHORT_BINANCE",
        "entry_threshold": "4.20",
        "cancel_threshold": "4.00",
        "quantity": "1",
        "repeat": False,
        **updates,
    }


async def command(engine, action, payload, now=900):
    future = engine.enqueue(action, payload)
    await engine.on_timer(now)
    return await future


async def feed(engine, times):
    for ts in times:
        await engine.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)


async def test_no_orders_without_manual_request_and_global_loop_is_disabled(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ManualOrderStrategy(Settings(), spec(), repo)
        await engine.start(900)
        await feed(engine, (1000, 1100, 1200, 1300))
        assert engine.order is None and engine.orders_created == 0
        with pytest.raises(ValueError, match="全局"):
            engine.request_placement(PlacementMode.LOOP)
        await engine.shutdown(1400)


async def test_simultaneous_conditions_execute_fifo_one_at_a_time(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ManualOrderStrategy(Settings(), spec(), repo)
        await engine.start(900)
        a, b = (
            request(),
            request(direction="LONG_BINANCE", entry_threshold="-4.50", cancel_threshold="-4.70"),
        )
        await command(engine, "create", a)
        await command(engine, "create", b)
        await feed(engine, (1000, 1100, 1200))
        first = engine.order
        assert first.conditional_id == a["request_id"]
        assert first.direction == Direction.SHORT_BINANCE
        assert len(await repo.unfinished_orders()) == 1
        await feed(engine, (1250, 1300, 1350))
        assert engine.order is first
        await command(engine, "cancel", a["request_id"], 1360)
        assert engine.order is first and first.state == "CANCELING"
        await engine.on_timer(1380)
        assert first.state == "CANCELED"
        await feed(engine, (1400, 1500, 1600))
        assert engine.order.conditional_id == b["request_id"]
        assert engine.order.direction == Direction.LONG_BINANCE
        assert len(await repo.unfinished_orders()) == 1
        assert engine.orders_created == 2
        await engine.shutdown(1650)
    report = audit_session(tmp_path / "test.db")
    assert report["status"] == "pass"
    assert report["entry_evidence_checked"] == 2


async def test_repeat_yields_to_other_eligible_manual_order(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ManualOrderStrategy(Settings(), spec(), repo)
        await engine.start(900)
        a, b = request(repeat=True), request()
        await command(engine, "create", a)
        await command(engine, "create", b)
        await feed(engine, (1000, 1100, 1200))
        await engine.on_timer(1501)
        await engine.on_timer(1521)
        assert engine.conditions[a["request_id"]].state == "WAITING"
        await feed(engine, (1600, 1700, 1800))
        assert engine.order.conditional_id == b["request_id"]
        await engine.on_timer(2101)
        await engine.on_timer(2121)
        await feed(engine, (2200, 2300, 2400))
        assert engine.order.conditional_id == a["request_id"]
        assert engine.conditions[a["request_id"]].execution_count == 2
        await engine.shutdown(2450)


async def test_unmet_first_order_does_not_block_eligible_later_order(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ManualOrderStrategy(Settings(), spec(), repo)
        await engine.start(900)
        a, b = request(entry_threshold="9"), request()
        await command(engine, "create", a)
        await command(engine, "create", b)
        await feed(engine, (1000, 1100, 1200))
        assert engine.order.conditional_id == b["request_id"]
        assert engine.conditions[a["request_id"]].state == "WAITING"
        await engine.shutdown(1250)


async def test_idempotent_create_and_cancel_before_trigger(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ManualOrderStrategy(Settings(), spec(), repo)
        await engine.start(900)
        a = request()
        await command(engine, "create", a)
        await command(engine, "create", a)
        assert len(engine.conditions) == 1
        with pytest.raises(ValueError):
            await command(engine, "create", {**a, "quantity": "2"})
        with pytest.raises(ValueError):
            await command(engine, "create", request(quantity="0.15"))
        await feed(engine, (1000, 1100))
        await command(engine, "cancel", a["request_id"], 1150)
        await feed(engine, (1200, 1300, 1400))
        assert engine.order is None
        assert engine.conditions[a["request_id"]].state == "CANCELED"
        await engine.shutdown(1450)


async def test_invalid_quotes_cannot_trigger_manual_order(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ManualOrderStrategy(Settings(), spec(), repo)
        await engine.start(900)
        a = request()
        await command(engine, "create", a)
        for ts in (1000, 1100, 1200):
            await engine.on_quotes(quote("4416.90", "4416.98", ts + 100), quote(ts=ts), ts)
        assert engine.order is None
        assert engine._confirmation(a["request_id"]).result.count == 0
        await engine.shutdown(1250)


async def test_restart_keeps_manual_requests_paused_until_explicit_resume(tmp_path):
    path = tmp_path / "test.db"
    a = request()
    async with SQLiteRepository(path) as repo:
        engine = ManualOrderStrategy(Settings(), spec(), repo)
        await engine.start(900)
        await command(engine, "create", a)
        await engine.shutdown(950)
    async with SQLiteRepository(path) as repo:
        engine = ManualOrderStrategy(Settings(), spec(), repo)
        await engine.start(1000)
        assert engine.conditions[a["request_id"]].state == "PAUSED"
        await feed(engine, (1100, 1200, 1300))
        assert engine.order is None
        await command(engine, "resume", a["request_id"], 1400)
        await feed(engine, (1500, 1600, 1700))
        assert engine.order.conditional_id == a["request_id"]
        await engine.shutdown(1750)


async def test_crash_with_active_order_does_not_reexecute_request(tmp_path):
    path = tmp_path / "test.db"
    a = request()
    async with SQLiteRepository(path) as repo:
        engine = ManualOrderStrategy(Settings(), spec(), repo)
        await engine.start(900)
        await command(engine, "create", a)
        await feed(engine, (1000, 1100, 1200))
        # Close storage without normal engine shutdown to model a process crash.
    async with SQLiteRepository(path) as repo:
        engine = ManualOrderStrategy(Settings(), spec(), repo)
        await engine.start(1300)
        assert engine.state == "SAFE_MODE"
        assert engine.conditions[a["request_id"]].state == "REVIEW"
        with pytest.raises(ValueError):
            await command(engine, "resume", a["request_id"], 1300)
        await feed(engine, (1400, 1500, 1600))
        assert engine.order is None
        assert len(await repo.unfinished_orders()) == 1
        await engine.shutdown(1650)


async def test_failed_reservation_is_not_marked_as_completed(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ManualOrderStrategy(Settings(), spec(), repo)
        await engine.start(900)
        a = request()
        await command(engine, "create", a)
        save = repo.save_condition

        async def fail_reservation(condition, event, now):
            if event == "conditional_reserved":
                raise RuntimeError("storage unavailable")
            await save(condition, event, now)

        repo.save_condition = fail_reservation
        with pytest.raises(RuntimeError, match="storage unavailable"):
            await feed(engine, (1000, 1100, 1200))
        await engine.shutdown(1250)
        assert engine.order is None and engine.orders_created == 0
        assert engine.conditions[a["request_id"]].state == "REVIEW"
