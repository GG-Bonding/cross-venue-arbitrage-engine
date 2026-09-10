import asyncio
import logging
from contextlib import asynccontextmanager

import aiohttp
import pytest
from aiohttp import web
from test_core import quote
from test_execution import spec

from arbitrage.config import Settings
from arbitrage.monitor.controller import MonitorController
from arbitrage.monitor.server import create_app
from arbitrage.observability import log_event
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.arbitrage_strategy import ArbitrageStrategy


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


def test_web_monitor_cannot_construct_in_live_mode(monkeypatch):
    monkeypatch.setenv("CONFIRM_LIVE_TRADING", "I_UNDERSTAND")
    with pytest.raises(ValueError, match="Phase 1"):
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
            async with client.get(base + "/api/history") as response:
                assert (await response.json())["orders"] == []
            assert not settings.database.path.exists()
            async with client.post(base + "/api/start") as response:
                assert response.status == 403
            headers = {"X-Control-Token": token, "Origin": "https://unrelated.example"}
            async with client.post(base + "/api/start", headers=headers) as response:
                assert response.status == 403
            headers["Origin"] = base
            async with client.post(base + "/api/start", headers=headers) as response:
                assert response.status == 200
            async with client.post(base + "/api/stop", headers=headers) as response:
                assert response.status == 200
            await controller.wait_closed()
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
