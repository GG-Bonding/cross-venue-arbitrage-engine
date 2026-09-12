import asyncio

import aiohttp

from arbitrage.config import Settings
from arbitrage.market.binance_market import BinanceMarket
from arbitrage.market.mt5_market import MT5Market
from arbitrage.observability import log_event, now_ms
from arbitrage.persistence.session_lock import SessionLock
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.manual_orders import ManualOrderStrategy


async def read_paper_accounts(engine, readers, stop, *, fixed_errors=None, interval=5):
    fixed_errors = fixed_errors or {}
    while not stop.is_set():
        names = list(readers)
        results = await asyncio.gather(*(readers[name]() for name in names), return_exceptions=True)
        accounts = dict(getattr(engine, "accounts", {}))
        errors = dict(fixed_errors)
        for name, result in zip(names, results, strict=True):
            if isinstance(result, Exception):
                errors[name] = f"{type(result).__name__}: {result}"
            else:
                accounts[name] = result
        engine.accounts = accounts
        engine.accounts_error = (
            " | ".join(f"{name}: {error}" for name, error in errors.items()) or None
        )
        engine.accounts_updated_ms = now_ms()
        try:
            await asyncio.wait_for(stop.wait(), interval)
        except TimeoutError:
            pass


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
    settings.require_runtime()
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
        if settings.mode == "live":
            from arbitrage.execution.live_venues import BinanceTrading, MT5Trading
            from arbitrage.strategy.live_orders import LiveOrderStrategy

            engine = LiveOrderStrategy(
                settings, spec, repo, BinanceTrading(settings, session), MT5Trading(mt5), mt5.spec
            )
        else:
            from arbitrage.execution.live_venues import BinanceTrading, MT5Trading

            await repo.bind_mode("paper")
            engine = ManualOrderStrategy(settings, spec, repo)
            paper_readers = {"mt5": MT5Trading(mt5).account}
            paper_account_errors = {}
            try:
                binance_account = BinanceTrading(settings, session)
            except ValueError as exc:
                paper_account_errors["binance"] = str(exc)
            else:
                binance_clock_ready = False

                async def read_binance_account():
                    nonlocal binance_clock_ready
                    try:
                        if not binance_clock_ready:
                            await binance_account.sync_clock()
                            binance_clock_ready = True
                        return await binance_account.account()
                    except Exception:
                        binance_clock_ready = False
                        raise

                paper_readers["binance"] = read_binance_account
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
                if settings.mode == "live":
                    from arbitrage.execution.user_stream import run_user_stream

                    tasks.append(group.create_task(run_user_stream(engine.binance, engine, stop)))
                if settings.mode == "paper":
                    tasks.append(
                        group.create_task(
                            read_paper_accounts(
                                engine,
                                paper_readers,
                                stop,
                                fixed_errors=paper_account_errors,
                            )
                        )
                    )
                await stop.wait()
                for task in tasks:
                    task.cancel()
        finally:
            if timer:
                timer.cancel()
            await engine.shutdown(now_ms())
