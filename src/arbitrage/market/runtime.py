import asyncio

import aiohttp

from arbitrage.config import Settings
from arbitrage.market.binance_market import BinanceMarket
from arbitrage.market.mt5_market import MT5Market
from arbitrage.observability import log_event, now_ms
from arbitrage.persistence.session_lock import SessionLock
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.manual_orders import ManualOrderStrategy


async def consume_quotes(engine, queue: asyncio.Queue, stop: asyncio.Event, *, clock=now_ms):
    latest = {}
    while not stop.is_set():
        try:
            venue, quote = await asyncio.wait_for(
                queue.get(), engine.settings.market.watchdog_ms / 1000
            )
        except TimeoutError:
            await engine.on_timer(clock())
            continue
        previous = latest.get(venue)
        if previous is None or quote.exchange_ts_ms >= previous.exchange_ts_ms:
            latest[venue] = quote
        now = clock()
        # Timer also runs during a busy queue; backlog cannot postpone cancellation.
        if "binance" in latest and "mt5" in latest:
            await engine.on_quotes(latest["binance"], latest["mt5"], now)
        else:
            await engine.on_timer(now)


async def run_market(
    settings: Settings,
    *,
    duration: float | None = None,
    stop: asyncio.Event | None = None,
    on_engine=None,
) -> None:
    settings.require_paper()
    with SessionLock(settings.database.path):
        await _run_market(settings, duration=duration, stop=stop, on_engine=on_engine)


async def _run_market(settings, *, duration, stop, on_engine) -> None:
    stop = stop if stop is not None else asyncio.Event()
    timeout = aiohttp.ClientTimeout(total=settings.market.http_timeout_ms / 1000)
    async with (
        SQLiteRepository(settings.database.path) as repo,
        aiohttp.ClientSession(timeout=timeout, trust_env=True) as session,
        MT5Market(settings) as mt5,
    ):
        binance = BinanceMarket(settings, session)
        spec = await binance.specification()
        log_event(
            "contract_specifications",
            binance=spec,
            mt5=mt5.spec,
            binance_underlying_per_qty=settings.trading.binance_underlying_per_qty,
        )
        engine = ManualOrderStrategy(settings, spec, repo)
        await engine.start(now_ms())
        if on_engine is not None:
            on_engine(engine)
        queue = asyncio.Queue(maxsize=settings.market.queue_size)
        loop = asyncio.get_running_loop()
        timer = loop.call_later(duration, stop.set) if duration is not None else None
        try:
            async with asyncio.TaskGroup() as group:
                tasks = [
                    group.create_task(binance.stream(queue, stop)),
                    group.create_task(mt5.stream(queue, stop)),
                    group.create_task(consume_quotes(engine, queue, stop)),
                ]
                await stop.wait()
                for task in tasks:
                    task.cancel()
        finally:
            if timer:
                timer.cancel()
            await engine.shutdown(now_ms())
