import asyncio
import logging
from contextlib import asynccontextmanager
from copy import deepcopy
from uuid import uuid4

import aiohttp
import pytest
from aiohttp import web
from test_core import quote
from test_execution import spec

from arbitrage.config import Settings
from arbitrage.domain.enums import Direction
from arbitrage.monitor.controller import MonitorController
from arbitrage.monitor.server import create_app
from arbitrage.observability import log_event
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.arbitrage_strategy import ArbitrageStrategy
from arbitrage.strategy.manual_orders import ManualOrderStrategy


@pytest.mark.parametrize(
    "now,mt5_ts,status,reason",
    [
        (1400, 1000, "RUNNING", "quote_stale"),
        (999, 1000, "RUNNING", "quote_future"),
        (1000, 750, "RUNNING", "quote_skew"),
        (1000, 1000, "STOPPED", "session_inactive"),
    ],
)
async def test_both_spreads_remain_visible_when_entry_is_blocked(
    tmp_path, now, mt5_ts, status, reason
):
    settings = Settings.model_validate({"database": {"path": tmp_path / "paper.db"}})
    controller = MonitorController(settings)
    try:
        async with SQLiteRepository(settings.database.path) as repo:
            engine = ArbitrageStrategy(settings, spec(), repo)
            await engine.start(1000)
            await engine.on_quotes(quote("4416.90", "4416.98"), quote(ts=mt5_ts), now)
            controller.engine, controller.status = engine, status
            before = deepcopy(engine.metrics.snapshot())
            confirmations = {d: deepcopy(engine.confirmations[d].result) for d in Direction}
            view = controller.snapshot(now=now)
            assert not view["quotes_valid"]
            assert view["quote_reason"] == reason
            assert view["directions"]["SHORT_BINANCE"]["raw_spread"] == "4.36"
            assert view["directions"]["SHORT_BINANCE"]["edge"] == "0.16"
            assert view["directions"]["LONG_BINANCE"]["raw_spread"] == "-4.40"
            assert view["directions"]["LONG_BINANCE"]["edge"] == "-8.60"
            assert engine.metrics.snapshot() == before
            assert {d: engine.confirmations[d].result for d in Direction} == confirmations
            assert engine.order is None
            engine.mt5_quote = None
            missing = controller.snapshot(now=now)
            assert all(d["raw_spread"] is None for d in missing["directions"].values())
    finally:
        await controller.close()


async def test_start_stop_is_single_run_and_keeps_decimal_quotes(tmp_path):
    settings = Settings.model_validate({"database": {"path": tmp_path / "paper.db"}})
    ready = asyncio.Event()
    starts = []

    async def runner(config, *, stop, on_engine):
        starts.append(1)
        async with SQLiteRepository(config.database.path) as repo:
            engine = ArbitrageStrategy(config, spec(), repo)
            await engine.start(1000)
            await engine.on_quotes(quote("4416.90", "4416.98"), quote(), 1000)
            on_engine(engine)
            ready.set()
            await stop.wait()
            await engine.shutdown(1100)

    controller = MonitorController(settings, runner=runner)
    try:
        assert controller.start() is True
        assert controller.start() is False
        await asyncio.wait_for(ready.wait(), 1)
        view = controller.snapshot(now=1000)
        assert view["status"] == "RUNNING"
        assert view["quotes_valid"] is True
        assert view["binance"]["ask"] == "4416.98"
        assert view["directions"]["SHORT_BINANCE"]["edge"] == "0.16"
        controller.select_direction("b")
        selection = controller.snapshot(now=1000)["entry_selection"]
        assert selection == {"requested": "b", "active": "both", "pending": True}
        await controller.engine.on_timer(1001)
        selection = controller.snapshot(now=1001)["entry_selection"]
        assert selection == {"requested": "b", "active": "b", "pending": False}
        assert controller.snapshot(now=1001)["directions"]["SHORT_BINANCE"]["raw_spread"] == "4.36"
        assert controller.snapshot(now=1400)["quotes_valid"] is False
        controller.stop()
        assert controller.start() is False  # STOPPING still owns the session.
        await controller.wait_closed()
        assert controller.snapshot(now=1100)["quotes_valid"] is False
        assert controller.status == "STOPPED"
        assert len(starts) == 1
    finally:
        await controller.close()


async def test_runner_failure_is_visible_and_blocks_stale_live_indication():
    async def runner(config, *, stop, on_engine):
        raise RuntimeError("MT5 disconnected symbol=XAUUSD")

    controller = MonitorController(Settings(), runner=runner)
    try:
        controller.start()
        await controller.wait_closed()
        view = controller.snapshot()
        assert view["status"] == "ERROR"
        assert "MT5 disconnected" in view["error"]
        assert not view["quotes_valid"]
    finally:
        await controller.close()


def test_web_monitor_live_requires_explicit_enable(monkeypatch):
    monkeypatch.setenv("CONFIRM_LIVE_TRADING", "I_UNDERSTAND")
    with pytest.raises(ValueError, match="live.enabled"):
        MonitorController(Settings(mode="live"))


async def test_log_buffer_is_bounded_and_does_not_store_every_tick():
    controller = MonitorController(Settings())
    logger = logging.getLogger("arbitrage")
    old = logger.level
    logger.setLevel(logging.INFO)
    try:
        for _ in range(250):
            log_event("quote_stale")
            log_event("market_snapshot", state="IDLE")
        assert len(controller.events) == 200
        assert all(e["event"] != "market_snapshot" for e in controller.events)
    finally:
        logger.setLevel(old)
        await controller.close()


@asynccontextmanager
async def monitor_server(settings, runner):
    controller = MonitorController(settings, runner=runner)
    app = create_app(controller)
    web_runner = web.AppRunner(app)
    await web_runner.setup()
    try:
        site = web.TCPSite(web_runner, "127.0.0.1", 0)
        await site.start()
        yield f"http://127.0.0.1:{web_runner.addresses[0][1]}", controller
    finally:
        await web_runner.cleanup()


async def test_dashboard_api_origin_controls_and_read_only_history(tmp_path):
    async def runner(config, *, stop, on_engine):
        await stop.wait()

    settings = Settings.model_validate({"database": {"path": tmp_path / "paper.db"}})
    async with monitor_server(settings, runner) as (base, controller):
        async with aiohttp.ClientSession() as client:
            async with client.get(base) as response:
                assert response.status == 200
                assert "Paper" in await response.text()
            async with client.get(base + "/api/status") as response:
                view = await response.json()
                token = view["control_token"]
                assert view["mode"] == "paper"
                assert "terminal_path" not in str(view)
                assert response.headers["Cache-Control"] == "no-store"
            async with client.get(base + "/api/status?compact=1") as response:
                compact = await response.json()
                assert "events" not in compact
                assert "trades" not in compact
                assert "metrics" not in compact
                assert "accounts" not in compact
                assert "config" not in compact
            async with client.get(base + "/api/status?compact=1&config=1") as response:
                assert "config" in await response.json()
            async with client.get(base + "/api/history") as response:
                assert (await response.json())["orders"] == []
            assert not settings.database.path.exists()
            async with client.post(base + "/api/start") as response:
                assert response.status == 403
            headers = {"X-Control-Token": token, "Origin": "https://unrelated.example"}
            async with client.post(base + "/api/start", headers=headers) as response:
                assert response.status == 403
            headers["Origin"] = base
            async with client.post(base + "/api/direction", json={"mode": "b"}) as response:
                assert response.status == 403
            for invalid in ({"mode": "invalid"}, {"mode": "a", "extra": 1}, [], {"mode": []}):
                async with client.post(
                    base + "/api/direction", headers=headers, json=invalid
                ) as response:
                    assert response.status == 410
            async with client.post(
                base + "/api/direction", headers=headers, json={"mode": "b"}
            ) as response:
                assert response.status == 410
            assert controller.settings.entry.direction_mode == "both"
            async with client.post(base + "/api/start", headers=headers) as response:
                assert response.status == 200
            async with client.post(base + "/api/stop", headers=headers) as response:
                assert response.status == 200
            await controller.wait_closed()
            assert controller.snapshot()["entry_selection"]["requested"] == "both"
            async with client.get(
                base + "/api/status", headers={"Host": "unrelated.example"}
            ) as response:
                assert response.status == 403


async def test_history_reads_persisted_paper_orders(tmp_path):
    settings = Settings.model_validate({"database": {"path": tmp_path / "paper.db"}})
    async with SQLiteRepository(settings.database.path) as repo:
        engine = ArbitrageStrategy(settings, spec(), repo)
        await engine.start(1000)
        for ts in (1000, 1100, 1200):
            await engine.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
        await engine.shutdown(1250)
    async with monitor_server(settings, None) as (base, _):
        async with aiohttp.ClientSession() as client:
            async with client.get(base + "/api/history") as response:
                history = await response.json()
                assert history["orders"][0]["state"] == "CANCELED"
                assert history["orders"][0]["filled_qty"] == "0"


async def test_manual_conditions_api_validates_controls_and_deduplicates(tmp_path):
    ready = asyncio.Event()

    async def runner(config, *, stop, on_engine):
        async with SQLiteRepository(config.database.path) as repo:
            engine = ManualOrderStrategy(config, spec(), repo)
            await engine.start(1000)
            on_engine(engine)
            await engine.on_timer(1000)
            ready.set()
            while not stop.is_set():
                await engine.on_timer(1000)
                try:
                    await asyncio.wait_for(stop.wait(), 0.01)
                except TimeoutError:
                    pass
            await engine.shutdown(2000)

    settings = Settings.model_validate({"database": {"path": tmp_path / "paper.db"}})
    async with monitor_server(settings, runner) as (base, controller):
        async with aiohttp.ClientSession() as client:
            view = await (await client.get(base + "/api/status")).json()
            headers = {"Origin": base, "X-Control-Token": view["control_token"]}
            payload = {
                "request_id": str(uuid4()),
                "direction": "SHORT_BINANCE",
                "entry_threshold": "4.2",
                "cancel_threshold": "4",
                "quantity": "1",
            }
            async with client.post(base + "/api/conditions", json=payload) as response:
                assert response.status == 403
            async with client.post(
                base + "/api/conditions", headers=headers, json=payload
            ) as response:
                assert response.status == 400  # No quote session yet.
            async with client.post(
                base + "/api/placement", headers=headers, json={"mode": "loop"}
            ) as response:
                assert response.status == 410
            controller.start()
            await asyncio.wait_for(ready.wait(), 1)
            for invalid in (
                {},
                {**payload, "request_id": "bad"},
                {**payload, "entry_threshold": "NaN"},
                {**payload, "cancel_threshold": "5"},
                {**payload, "direction": "both"},
            ):
                async with client.post(
                    base + "/api/conditions", headers=headers, json=invalid
                ) as response:
                    assert response.status == 400
            for _ in range(2):
                async with client.post(
                    base + "/api/conditions", headers=headers, json=payload
                ) as response:
                    assert response.status == 200
                    assert (await response.json())["state"] == "WAITING"
            assert len(controller.snapshot()["conditions"]) == 1
            async with client.post(
                base + "/api/conditions",
                headers=headers,
                json={**payload, "quantity": "2"},
            ) as response:
                assert response.status == 400
            async with client.post(
                base + f"/api/conditions/{payload['request_id']}/cancel",
                headers=headers,
            ) as response:
                assert response.status == 200
                assert (await response.json())["state"] == "CANCELED"
            assert controller.status == "RUNNING"
