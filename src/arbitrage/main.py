import argparse
import asyncio
import logging
import math
import os
import sqlite3
import sys
from pathlib import Path

from arbitrage.config import Settings, load_settings
from arbitrage.market.demo import run_demo
from arbitrage.market.runtime import run_market
from arbitrage.observability import dumps, log_event


def read_status(path: Path) -> dict:
    # mode=ro prevents accidental database creation or schema mutation.
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        orders = [
            dict(row) for row in db.execute("SELECT * FROM maker_orders WHERE state != 'CANCELED'")
        ]
        return {
            "database": str(path),
            "mode": "paper",
            "unfinished_orders": orders,
            "state": "SAFE_MODE" if orders else "IDLE",
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1: Binance / MT5 paper spread monitor")
    parser.add_argument("--config", type=Path, help="YAML config; omitted uses validated defaults")
    parser.add_argument("--database", type=Path, help="Override SQLite path")
    commands = parser.add_mutually_exclusive_group()
    commands.add_argument("--demo", action="store_true", help="Run finite offline synthetic quotes")
    commands.add_argument("--web", action="store_true", help="Local paper dashboard at 127.0.0.1")
    parser.add_argument(
        "--port", type=int, default=8765, help="Local dashboard port (default 8765)"
    )
    commands.add_argument(
        "--status", action="store_true", help="Read database status without feeds"
    )
    parser.add_argument(
        "--duration", type=float, help="Stop real market data after this many seconds"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    try:
        if not 1 <= args.port <= 65535:
            raise ValueError("--port must be between 1 and 65535")
        if args.web and args.duration is not None:
            raise ValueError("--web uses the page Start/Stop controls; omit --duration")
        if args.config:
            settings = load_settings(args.config)
        else:
            settings = Settings(mode=os.environ.get("TRADING_MODE", "paper"))
            settings.require_paper()
        if args.duration is not None and (args.duration <= 0 or not math.isfinite(args.duration)):
            raise ValueError("--duration must be finite and positive")
        path = args.database or (Path("data/demo.db") if args.demo else settings.database.path)
        settings = settings.model_copy(
            update={"database": settings.database.model_copy(update={"path": path})}
        )
        if args.web:
            from arbitrage.monitor.server import run_dashboard

            # The browser samples live memory; avoid duplicating every tick in a console log.
            for handler in logging.getLogger().handlers:
                handler.addFilter(
                    lambda record: not record.getMessage().startswith('{"event": "market_snapshot"')
                )
            asyncio.run(run_dashboard(settings, port=args.port))
        elif args.status:
            print(dumps(read_status(path)))
        elif args.demo:
            asyncio.run(run_demo(settings))
        else:
            asyncio.run(run_market(settings, duration=args.duration))
    except KeyboardInterrupt:
        log_event("shutdown_requested", reason="keyboard_interrupt")
    except Exception as exc:
        # Process boundary: report context and exit, never continue trading after a failure.
        print(
            dumps(
                {
                    "event": "fatal_error",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "causes": error_causes(exc),
                }
            ),
            file=sys.stderr,
        )
        raise SystemExit(1) from exc


def error_causes(exc: BaseException) -> list[str]:
    if isinstance(exc, BaseExceptionGroup):
        return [message for child in exc.exceptions for message in error_causes(child)]
    causes = [f"{type(exc).__name__}: {exc}"]
    if exc.__cause__ is not None:
        causes.extend(error_causes(exc.__cause__))
    return causes


if __name__ == "__main__":
    main()
