import json
from decimal import Decimal
from importlib.resources import files

from arbitrage.config import Settings
from arbitrage.domain.quote import Quote
from arbitrage.market.binance_market import parse_exchange_info
from arbitrage.observability import log_event, now_ms
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.arbitrage_strategy import ArbitrageStrategy


async def run_demo(settings: Settings) -> None:
    settings.require_paper()
    fixture = json.loads(
        files("arbitrage.market").joinpath("demo.json").read_text(encoding="utf-8")
    )
    spec = parse_exchange_info(fixture["exchange_info"], "XAUUSDT")
    log_event("demo_started", source="synthetic_fixture", fill_simulation=False)
    start = now_ms()
    async with SQLiteRepository(settings.database.path) as repo:
        engine = ArbitrageStrategy(settings, spec, repo)
        await engine.start(start)
        try:
            for item in fixture["ticks"]:
                ts = start + item["offset_ms"]
                b, m = item["binance"], item["mt5"]
                await engine.on_quotes(
                    Quote(Decimal(b[0]), Decimal(b[1]), Decimal("10"), Decimal("10"), ts, ts),
                    Quote(Decimal(m[0]), Decimal(m[1]), None, None, ts, ts),
                    ts,
                )
        finally:
            await engine.shutdown(start + fixture["ticks"][-1]["offset_ms"])
