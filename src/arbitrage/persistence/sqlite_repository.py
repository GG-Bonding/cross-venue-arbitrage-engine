import sqlite3
from pathlib import Path

import aiosqlite

from arbitrage.domain.order import MakerOrder
from arbitrage.observability import dumps

EVENT_TABLES = frozenset(
    {
        "quotes_sample",
        "strategy_events",
        "maker_fills",
        "mt5_orders",
        "pairs",
        "pair_events",
        "fees",
        "risk_events",
    }
)


class SQLiteRepository:
    def __init__(self, path: Path):
        self.path = path
        self.db: aiosqlite.Connection | None = None

    async def __aenter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = await aiosqlite.connect(self.path)
        self.db.row_factory = aiosqlite.Row
        try:
            await self.db.execute("PRAGMA journal_mode=WAL")
            await self.db.execute("PRAGMA synchronous=FULL")
            await self.db.execute("PRAGMA busy_timeout=5000")
            for table in sorted(EVENT_TABLES):
                await self.db.execute(
                    f"CREATE TABLE IF NOT EXISTS {table} ("
                    "id INTEGER PRIMARY KEY, event TEXT NOT NULL, timestamp_ms INTEGER NOT NULL, "
                    "payload TEXT NOT NULL)"
                )
            await self.db.execute(
                "CREATE TABLE IF NOT EXISTS maker_orders (order_id TEXT PRIMARY KEY, "
                "state TEXT NOT NULL, mode TEXT NOT NULL, payload TEXT NOT NULL)"
            )
            await self.db.commit()
        except BaseException:
            await self.db.close()
            raise
        return self

    async def __aexit__(self, *_):
        if self.db is not None:
            await self.db.close()

    async def event(self, table: str, event: str, now: int, payload: object) -> None:
        if table not in EVENT_TABLES:
            raise ValueError(f"Unknown event table={table}")
        try:
            await self.db.execute(
                f"INSERT INTO {table}(event,timestamp_ms,payload) VALUES (?,?,?)",
                (event, now, dumps(payload)),
            )
            await self.db.commit()
        except sqlite3.Error as exc:
            await self.db.rollback()
            raise RuntimeError(f"SQLite event write failed table={table} event={event}") from exc

    async def save_order(self, order: MakerOrder, event: str, now: int, **context) -> None:
        try:
            await self.db.execute(
                "INSERT INTO maker_orders VALUES (?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET "
                "state=excluded.state, payload=excluded.payload",
                (order.order_id, order.state, order.mode, dumps(order)),
            )
            await self.db.execute(
                "INSERT INTO strategy_events(event,timestamp_ms,payload) VALUES (?,?,?)",
                (event, now, dumps({"order": order, **context})),
            )
            await self.db.commit()
        except sqlite3.Error as exc:
            await self.db.rollback()
            raise RuntimeError(
                f"SQLite order transaction failed order_id={order.order_id} event={event}"
            ) from exc

    async def unfinished_orders(self) -> list[dict]:
        async with self.db.execute("SELECT * FROM maker_orders WHERE state != 'CANCELED'") as c:
            return [dict(row) for row in await c.fetchall()]

    async def events(self) -> list[dict]:
        async with self.db.execute("SELECT * FROM strategy_events ORDER BY id") as c:
            return [dict(row) for row in await c.fetchall()]

    async def journal_mode(self) -> str:
        async with self.db.execute("PRAGMA journal_mode") as c:
            return (await c.fetchone())[0]
