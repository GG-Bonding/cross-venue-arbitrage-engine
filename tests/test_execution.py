from decimal import Decimal as D

import pytest
from test_core import quote

from arbitrage.config import Settings, load_settings
from arbitrage.domain.enums import Direction, EntryMode, OrderState
from arbitrage.domain.specs import BinanceSpec, HedgeCalculator, MT5Spec
from arbitrage.execution.binance_maker import CancelPolicy, PaperMaker
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.arbitrage_strategy import ArbitrageStrategy


def spec():
    return BinanceSpec(
        "XAUUSDT", D("0.05"), D("0.1"), D("0.1"), D("100"), D("5"), 1, 2, D("0.05"), D("100000")
    )


@pytest.mark.parametrize(
    "direction,expected",
    [(Direction.SHORT_BINANCE, "4417.00"), (Direction.LONG_BINANCE, "4416.90")],
)
def test_maker_rounding_and_no_assumed_fill(direction, expected):
    order = PaperMaker(spec()).place(direction, quote("4416.93", "4416.98"), D("1.29"), 1000)
    assert order.price == D(expected)
    assert order.quantity == D("1.2")
    assert order.state == OrderState.MAKER_PENDING
    assert order.filled_qty == 0
    assert order.time_in_force == "GTX"


def test_post_only_rejection_and_lot_conversion():
    with pytest.raises(ValueError, match="Post only"):
        PaperMaker(spec()).place(Direction.LONG_BINANCE, quote("10", "10"), D("1"), 1000)
    mt5 = MT5Spec("XAUUSD", D("100"), D("0.01"), D("10"), D("0.01"))
    calc = HedgeCalculator(spec(), mt5, D("1"))
    assert calc.lots_to_underlying(D("0.01")) == D("1")
    assert calc.binance_to_lots(D("1")) == D("0.01")
    with pytest.raises(ValueError, match="represent"):
        calc.binance_to_lots(D("0.5"))


def test_cancel_hysteresis_and_timeout():
    policy = CancelPolicy(D("4"), 50, 2000)
    assert policy.update(D("3.99"), 1100, 1000) is None
    assert policy.update(D("4"), 1140, 1000) is None
    assert policy.update(D("3.9"), 1200, 1000) is None
    assert policy.update(D("3.9"), 1249, 1000) is None
    assert policy.update(D("3.9"), 1250, 1000) == "spread_invalid"
    assert CancelPolicy(D("4"), 50, 2000).update(D("5"), 3001, 1000) == "pending_timeout"


def test_cancel_request_is_not_ack():
    maker = PaperMaker(spec())
    order = maker.place(Direction.SHORT_BINANCE, quote("4416", "4417"), D("1"), 1000)
    maker.request_cancel(order, 1200)
    assert order.state == OrderState.CANCELING
    maker.ack_cancel(order)
    assert order.state == OrderState.CANCELED


@pytest.mark.parametrize("confirmation", [None, "I_UNDERSTAND"])
def test_live_never_starts_in_phase1(tmp_path, monkeypatch, confirmation):
    path = tmp_path / "config.yaml"
    path.write_text("mode: paper\n", encoding="utf-8")
    monkeypatch.setenv("TRADING_MODE", "live")
    monkeypatch.delenv("CONFIRM_LIVE_TRADING", raising=False)
    if confirmation:
        monkeypatch.setenv("CONFIRM_LIVE_TRADING", confirmation)
    with pytest.raises(ValueError, match="CONFIRM_LIVE_TRADING|Phase 1"):
        load_settings(path)


def test_config_rejects_typos_and_bad_hysteresis():
    with pytest.raises(ValueError):
        Settings.model_validate({"maker": {"max_pendng_ms": 20}})
    with pytest.raises(ValueError):
        Settings.model_validate({"maker": {"cancel_threshold": "5"}})
    with pytest.raises(ValueError):
        Settings.model_validate({"entry": {"direction_mode": "invalid"}})


@pytest.mark.parametrize(
    "mode,bid,ask,expected",
    [
        ("a", "4416.90", "4416.98", Direction.SHORT_BINANCE),
        ("a", "4408.10", "4408.20", None),
        ("b", "4416.90", "4416.98", None),
        ("b", "4408.10", "4408.20", Direction.LONG_BINANCE),
        ("both", "4408.10", "4408.20", Direction.LONG_BINANCE),
    ],
)
async def test_only_selected_direction_can_place(tmp_path, mode, bid, ask, expected):
    config = Settings.model_validate({"entry": {"direction_mode": mode}})
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ArbitrageStrategy(config, spec(), repo)
        await engine.start(1000)
        for ts in (1000, 1100, 1200):
            await engine.on_quotes(quote(bid, ask, ts), quote(ts=ts), ts)
        assert (engine.order.direction if engine.order else None) == expected
        for direction, confirmation in engine.confirmations.items():
            if not config.entry.direction_mode.allows(direction):
                assert confirmation.result.count == 0
        await engine.shutdown(1250)


async def test_switch_cancels_old_direction_before_new_order(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ArbitrageStrategy(Settings(), spec(), repo)
        await engine.start(1000)
        for ts in (1000, 1100, 1200):
            await engine.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
        old = engine.order
        engine.request_direction_mode(EntryMode.B)
        assert old.state == OrderState.MAKER_PENDING  # Request never touches orders.
        await engine.on_timer(1210)
        assert old.state == OrderState.CANCELING
        await engine.on_quotes(quote("4408.10", "4408.20", 1220), quote(ts=1220), 1220)
        assert engine.order is old
        await engine.on_timer(1230)
        assert old.state == OrderState.CANCELED and engine.order is None
        for ts in (1240, 1340, 1440):
            await engine.on_quotes(quote("4408.10", "4408.20", ts), quote(ts=ts), ts)
        assert engine.order.direction == Direction.LONG_BINANCE
        assert engine.order.order_id != old.order_id
        await engine.shutdown(1450)


async def test_switch_resets_confirmation_and_timer_cannot_place(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ArbitrageStrategy(Settings(), spec(), repo)
        await engine.start(1000)
        for ts in (1000, 1100):
            await engine.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
        engine.request_direction_mode(EntryMode.A)
        await engine.on_timer(1150)
        assert all(c.result.count == 0 for c in engine.confirmations.values())
        assert engine.order is None
        for ts in (1200, 1300):
            await engine.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
            assert engine.order is None
        await engine.on_quotes(quote("4416.90", "4416.98", 1400), quote(ts=1400), 1400)
        assert engine.order.direction == Direction.SHORT_BINANCE
        await engine.shutdown(1450)


async def test_strategy_confirmation_pending_price_and_cancel(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ArbitrageStrategy(Settings(), spec(), repo)
        await engine.start(1000)
        for ts in (1000, 1100, 1200):
            await engine.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
        order = engine.order
        assert order is not None
        assert order.price == D("4417.00")
        # Moving Binance ask does not change the pending order's own spread.
        await engine.on_quotes(quote("4430", "4431", 1300), quote("4413.2", "4413.3", 1300), 1300)
        assert order.state == OrderState.MAKER_PENDING
        await engine.on_quotes(quote("4430", "4431", 1350), quote("4413.2", "4413.3", 1350), 1350)
        assert order.state == OrderState.CANCELING
        assert len(await repo.unfinished_orders()) == 1
        await engine.on_timer(1370)
        assert order.state == OrderState.CANCELED
        assert await repo.unfinished_orders() == []
        events = await repo.events()
        assert {row["event"] for row in events} >= {
            "entry_confirmation_start",
            "entry_confirmed",
            "maker_order_created",
            "maker_cancel_requested",
            "maker_order_canceled",
        }
        assert await repo.journal_mode() == "wal"


async def test_duplicate_no_confirmation_stale_cancels_without_new_tick(tmp_path):
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ArbitrageStrategy(Settings(), spec(), repo)
        await engine.start(1000)
        b, m = quote("4416.90", "4416.98"), quote()
        for ts in (1000, 1100, 1200):
            await engine.on_quotes(b, m, ts)
        assert engine.order is None
        for ts in (1300, 1400, 1500):
            await engine.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
        assert engine.order is not None
        await engine.on_timer(1801)
        assert engine.order.state == OrderState.CANCELING


async def test_restart_safe_mode_and_database_failure(tmp_path):
    path = tmp_path / "test.db"
    async with SQLiteRepository(path) as repo:
        engine = ArbitrageStrategy(Settings(), spec(), repo)
        await engine.start(1000)
        for ts in (1000, 1100, 1200):
            await engine.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
    async with SQLiteRepository(path) as repo:
        restarted = ArbitrageStrategy(Settings(), spec(), repo)
        await restarted.start(1300)
        assert restarted.state == "SAFE_MODE"
        for ts in (1400, 1500, 1600):
            await restarted.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
        assert restarted.order is None
        assert len(await repo.unfinished_orders()) == 1
        await repo.db.execute("PRAGMA query_only=ON")
        with pytest.raises(RuntimeError, match="SQLite.*event"):
            await repo.event("risk_events", "failure", 1700, {})
