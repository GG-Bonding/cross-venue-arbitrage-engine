from uuid import uuid4

import pytest
from test_core import quote
from test_execution import spec

from arbitrage.config import Settings
from arbitrage.domain.enums import OrderState, PlacementMode
from arbitrage.monitor.controller import MonitorController
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.arbitrage_strategy import ArbitrageStrategy


async def feed(engine, times):
    for ts in times:
        await engine.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)


async def prepare(engine):
    await engine.start(900)
    engine.request_placement(PlacementMode.PAUSED)
    await engine.on_timer(900)


async def test_monitor_only_then_single_task_creates_exactly_one_order(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ArbitrageStrategy(Settings(), spec(), repo)
        await prepare(engine)
        await feed(engine, (1000, 1100, 1200))
        assert engine.order is None
        assert all(c.result.count == 0 for c in engine.confirmations.values())
        engine.request_placement(PlacementMode.ONCE)
        with pytest.raises(RuntimeError):
            engine.request_placement(PlacementMode.ONCE)
        await feed(engine, (1300, 1400, 1500))
        old = engine.order
        assert old is not None and engine.orders_created == 1
        await engine.on_timer(1801)  # Stale quote cancels; a single task must not rearm.
        await engine.on_timer(1821)
        assert old.state == OrderState.CANCELED
        assert engine.placement_mode == PlacementMode.PAUSED
        await feed(engine, (1900, 2000, 2100))
        assert engine.order is None and engine.orders_created == 1


async def test_loop_rearms_after_cancel_and_stop_keeps_quotes_running(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ArbitrageStrategy(Settings(), spec(), repo)
        await prepare(engine)
        engine.request_placement(PlacementMode.LOOP)
        await feed(engine, (1000, 1100, 1200))
        first = engine.order
        await engine.on_timer(1501)
        assert engine.order is first and first.state == OrderState.CANCELING
        await engine.on_timer(1521)
        await feed(engine, (1600, 1700, 1800))
        second = engine.order
        assert second and second.order_id != first.order_id
        assert engine.orders_created == 2
        engine.request_placement(PlacementMode.PAUSED)
        await engine.on_timer(1810)
        assert second.state == OrderState.CANCELING
        await engine.on_timer(1830)
        assert second.state == OrderState.CANCELED
        await feed(engine, (1900, 2000, 2100))
        assert engine.binance_quote.exchange_ts_ms == 2100
        assert engine.order is None and engine.orders_created == 2
        assert await repo.unfinished_orders() == []


async def test_task_waits_for_valid_quotes_and_stop_before_confirmation(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ArbitrageStrategy(Settings(), spec(), repo)
        await prepare(engine)
        engine.request_placement(PlacementMode.ONCE)
        for ts in (1000, 1100, 1200):
            await engine.on_quotes(quote("4416.90", "4416.98", ts + 100), quote(ts=ts), ts)
        assert engine.order is None and engine.once_remaining == 1
        await feed(engine, (1300, 1400))
        engine.request_placement(PlacementMode.PAUSED)
        await feed(engine, (1500, 1600, 1700))
        assert engine.order is None
        assert all(c.result.count == 0 for c in engine.confirmations.values())


async def test_stop_arriving_during_confirmation_write_prevents_placement(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ArbitrageStrategy(Settings(), spec(), repo)
        await prepare(engine)
        engine.request_placement(PlacementMode.ONCE)
        await feed(engine, (1000, 1100))
        original_record = engine._record

        async def record(event, now, **payload):
            await original_record(event, now, **payload)
            if event == "entry_confirmed":
                engine.request_placement(PlacementMode.PAUSED)

        engine._record = record
        await feed(engine, (1200,))
        assert engine.order is None
        await engine.on_timer(1210)
        assert engine.placement_mode == PlacementMode.PAUSED


async def test_controller_idempotency_after_single_completion_and_session_restart(tmp_path):
    settings = Settings.model_validate({"database": {"path": tmp_path / "test.db"}})
    controller = MonitorController(settings)
    try:
        async with SQLiteRepository(settings.database.path) as repo:
            engine = ArbitrageStrategy(settings, spec(), repo)
            await engine.start(900)
            controller._on_engine(engine)
            await engine.on_timer(900)
            request_id = str(uuid4())
            controller.control_placement("once", request_id)
            controller.control_placement("once", request_id)
            await feed(engine, (1000, 1100, 1200))
            await engine.on_timer(1501)
            await engine.on_timer(1521)
            controller.control_placement("once", request_id)
            await feed(engine, (1600, 1700, 1800))
            assert engine.order is None and engine.orders_created == 1
            with pytest.raises(ValueError):
                controller.control_placement("loop", request_id)
            engine.state = "SAFE_MODE"
            with pytest.raises(RuntimeError):
                controller.control_placement("loop", str(uuid4()))
            await engine.shutdown(1900)
            controller.status = "STOPPED"
            with pytest.raises(RuntimeError):
                controller.control_placement("once", str(uuid4()))
            restarted = ArbitrageStrategy(settings, spec(), repo)
            await restarted.start(2000)
            controller._on_engine(restarted)
            await restarted.on_timer(2000)
            await feed(restarted, (2100, 2200, 2300))
            assert restarted.placement_mode == PlacementMode.PAUSED
            assert restarted.order is None
    finally:
        await controller.close()
