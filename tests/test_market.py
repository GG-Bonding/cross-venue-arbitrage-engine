import asyncio
import threading
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from test_core import quote
from test_execution import spec

from arbitrage.config import Settings
from arbitrage.market.binance_market import parse_book_ticker, parse_exchange_info
from arbitrage.market.mt5_market import MT5Market
from arbitrage.market.runtime import consume_quotes, read_paper_accounts
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.arbitrage_strategy import ArbitrageStrategy


def exchange_info():
    return {
        "symbols": [
            {
                "symbol": "XAUUSDT",
                "status": "TRADING",
                "contractType": "PERPETUAL",
                "quoteAsset": "USDT",
                "quantityPrecision": 3,
                "pricePrecision": 2,
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "tickSize": "0.05",
                        "minPrice": "0.05",
                        "maxPrice": "100000",
                    },
                    {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.1", "maxQty": "100"},
                    {"filterType": "MIN_NOTIONAL", "notional": "5"},
                ],
            }
        ]
    }


def test_exchange_filters_not_precision():
    result = parse_exchange_info(exchange_info(), "XAUUSDT")
    assert result.step_size == D("0.1")
    assert result.tick_size == D("0.05")
    assert result.quantity_precision == 3
    with pytest.raises(ValueError, match="symbol=UNKNOWN"):
        parse_exchange_info(exchange_info(), "UNKNOWN")
    data = exchange_info()
    data["symbols"][0]["filters"].pop()
    with pytest.raises(ValueError, match="filters"):
        parse_exchange_info(data, "XAUUSDT")


def test_actual_gold_tradfi_perpetual_contract_type():
    data = exchange_info()
    data["symbols"][0]["contractType"] = "TRADIFI_PERPETUAL"
    assert parse_exchange_info(data, "XAUUSDT").symbol == "XAUUSDT"
    data["symbols"][0]["contractType"] = "CURRENT_QUARTER"
    with pytest.raises(ValueError, match="perpetual"):
        parse_exchange_info(data, "XAUUSDT")


def test_book_ticker_exact_prices_and_transaction_time():
    event = {
        "e": "bookTicker",
        "s": "XAUUSDT",
        "b": "4416.90",
        "a": "4416.98",
        "B": "1.25",
        "A": "2.50",
        "T": 1000,
        "E": 1001,
        "u": 123,
    }
    q = parse_book_ticker(event, "XAUUSDT", 1010)
    assert q == quote("4416.90", "4416.98", 1000, 1010).__class__(
        D("4416.90"), D("4416.98"), D("1.25"), D("2.50"), 1000, 1010
    )
    assert parse_book_ticker({"stream": "xauusdt@bookTicker", "data": event}, "XAUUSDT", 1010) == q
    with pytest.raises(ValueError, match="symbol"):
        parse_book_ticker(event, "BTCUSDT", 1010)


class FakeMT5:
    def __init__(self):
        self.threads = []
        self.tick = SimpleNamespace(bid=4412.5, ask=4412.62, time_msc=1000)
        self.connected = True
        self.closed = False

    def record(self):
        self.threads.append(threading.get_ident())

    def initialize(self, **kwargs):
        self.record()
        return True

    def terminal_info(self):
        self.record()
        return SimpleNamespace(connected=self.connected)

    def symbol_select(self, *args):
        self.record()
        return True

    def symbol_info(self, symbol):
        self.record()
        return SimpleNamespace(
            trade_contract_size=100, volume_min=0.01, volume_max=100, volume_step=0.01
        )

    def symbol_info_tick(self, symbol):
        self.record()
        return self.tick

    def last_error(self):
        return (1, "fixture failure")

    def shutdown(self):
        self.record()
        self.closed = True


async def test_mt5_single_worker_and_cached_tick_does_not_refresh_local_time():
    api = FakeMT5()
    market = MT5Market(Settings(), api=api, clock=lambda: 1010)
    async with market:
        assert market.spec.contract_size == D("100")
        q = await market.read_quote()
        assert q.local_ts_ms == 1010
        assert await market.read_quote() is None
        api.connected = False
        with pytest.raises(RuntimeError, match="MT5 disconnected"):
            await market.read_quote()
    assert api.closed
    assert len(set(api.threads)) == 1
    assert api.threads[0] != threading.get_ident()


async def test_mt5_none_error_has_context_and_closes():
    api = FakeMT5()
    async with MT5Market(Settings(), api=api) as market:
        api.tick = None
        with pytest.raises(RuntimeError, match="symbol=XAUUSD.*fixture failure"):
            await market.read_quote()
    assert api.closed


async def test_mt5_explicit_terminal_path_uses_documented_positional_argument():
    class PathMT5(FakeMT5):
        def initialize(self, path, /, **kwargs):
            self.record()
            assert path == "E:/project/MetaTrader_init/terminal64.exe"
            assert kwargs == {"timeout": 10000}
            return True

    api = PathMT5()
    settings = Settings.model_validate(
        {"mt5": {"terminal_path": "E:/project/MetaTrader_init/terminal64.exe"}}
    )
    async with MT5Market(settings, api=api) as market:
        assert market.spec.symbol == "XAUUSD"
    assert api.closed


async def test_mt5_broker_time_normalization_preserves_raw_timestamp():
    api = FakeMT5()
    api.tick.time_msc = 10_801_000
    settings = Settings.model_validate({"mt5": {"tick_time_offset_minutes": 180}})
    async with MT5Market(settings, api=api, clock=lambda: 1010) as market:
        q = await market.read_quote()
        assert q.exchange_ts_ms == 1000
        assert q.raw_exchange_ts_ms == 10_801_000
        assert q.local_ts_ms == 1010
        assert await market.read_quote() is None


async def test_mt5_default_does_not_guess_broker_offset():
    api = FakeMT5()
    api.tick.time_msc = 10_801_000
    async with MT5Market(Settings(), api=api, clock=lambda: 1010) as market:
        q = await market.read_quote()
        assert q.exchange_ts_ms == 10_801_000
        assert q.raw_exchange_ts_ms == 10_801_000


async def test_mt5_normalization_does_not_make_old_quotes_fresh():
    from arbitrage.risk.quote_guard import QuoteGuard

    api = FakeMT5()
    api.tick.time_msc = 10_801_000
    settings = Settings.model_validate({"mt5": {"tick_time_offset_minutes": 180}})
    async with MT5Market(settings, api=api, clock=lambda: 2000) as market:
        q = await market.read_quote()
        assert QuoteGuard(300, 200).check(quote(ts=2000), q, 2000) == "quote_stale"


async def test_mt5_initial_empty_tick_waits_without_inventing_a_quote():
    api = FakeMT5()
    api.tick = SimpleNamespace(bid=0.0, ask=0.0, time_msc=0)
    clock = [1000]
    async with MT5Market(Settings(), api=api, clock=lambda: clock[0]) as market:
        assert await market.read_quote() is None
        clock[0] = 1100
        api.tick = SimpleNamespace(bid=4412.5, ask=4412.62, time_msc=1090)
        assert (await market.read_quote()).exchange_ts_ms == 1090
        api.tick = SimpleNamespace(bid=0.0, ask=0.0, time_msc=0)
        with pytest.raises(RuntimeError, match="empty tick"):
            await market.read_quote()


async def test_mt5_initial_empty_tick_has_bounded_wait():
    api = FakeMT5()
    api.tick = SimpleNamespace(bid=0.0, ask=0.0, time_msc=0)
    clock = [1000]
    settings = Settings.model_validate({"mt5": {"quote_startup_timeout_ms": 500}})
    async with MT5Market(settings, api=api, clock=lambda: clock[0]) as market:
        assert await market.read_quote() is None
        clock[0] = 1501
        with pytest.raises(RuntimeError, match="empty tick.*symbol=XAUUSD"):
            await market.read_quote()


async def test_consumer_watchdog_runs_when_queue_is_silent(tmp_path, monkeypatch):
    settings = Settings.model_validate(
        {
            "market": {"max_quote_age_ms": 10, "watchdog_ms": 2},
            "entry": {"confirmation": {"min_duration_ms": 0}},
        }
    )
    async with SQLiteRepository(tmp_path / "test.db") as repo:
        engine = ArbitrageStrategy(settings, spec(), repo)
        await engine.start(1000)
        for ts in (1000, 1001, 1002):
            await engine.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
        queue = asyncio.Queue()
        stop = asyncio.Event()
        canceled = asyncio.Event()
        original_timer = engine.on_timer

        async def observe_timer(now):
            await original_timer(now)
            if engine.order.state == "CANCELING":
                canceled.set()

        monkeypatch.setattr(engine, "on_timer", observe_timer)
        task = asyncio.create_task(consume_quotes(engine, queue, stop, clock=lambda: 1013))
        try:
            await asyncio.wait_for(canceled.wait(), 1)
        finally:
            stop.set()
            await task
        assert engine.order.state == "CANCELING"


async def test_paper_account_reader_includes_both_venues_and_isolates_failures():
    engine = SimpleNamespace(accounts={})
    stop = asyncio.Event()

    async def binance():
        return {"currency": "USDT", "equity": "100", "available": "90", "profit": "1"}

    async def mt5():
        stop.set()
        return {"currency": "USD", "equity": "200", "available": "180", "profit": "2"}

    await read_paper_accounts(engine, {"mt5": mt5, "binance": binance}, stop, interval=0.01)
    assert set(engine.accounts) == {"binance", "mt5"}
    assert engine.accounts_error is None

    engine = SimpleNamespace(accounts={"binance": {"equity": "100"}})
    stop = asyncio.Event()

    async def unavailable():
        raise RuntimeError("temporarily unavailable")

    async def stop_after_mt5():
        stop.set()
        return {"equity": "200"}

    await read_paper_accounts(
        engine,
        {"binance": unavailable, "mt5": stop_after_mt5},
        stop,
        interval=0.01,
    )
    assert engine.accounts == {"binance": {"equity": "100"}, "mt5": {"equity": "200"}}
    assert engine.accounts_error == "binance: RuntimeError: temporarily unavailable"
