import asyncio
import json
import sqlite3
from pathlib import Path

import aiosqlite

from arbitrage.domain.order import MakerOrder
from arbitrage.observability import dumps, log_event

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
        self.write_lock = asyncio.Lock()

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
            await self.db.execute(
                "CREATE TABLE IF NOT EXISTS conditional_orders ("
                "request_id TEXT PRIMARY KEY, state TEXT NOT NULL, updated_at_ms INTEGER NOT NULL, "
                "payload TEXT NOT NULL)"
            )
            await self.db.execute(
                "CREATE TABLE IF NOT EXISTS live_trades ("
                "trade_id TEXT PRIMARY KEY, state TEXT NOT NULL, payload TEXT NOT NULL)"
            )
            await self.db.execute(
                "CREATE TABLE IF NOT EXISTS runtime_identity "
                "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
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

    async def save_condition(self, condition, event: str, now: int) -> None:
        async with self.write_lock:
            await self._save_condition(condition, event, now)

    async def _save_condition(self, condition, event: str, now: int) -> None:
        condition.updated_at_ms = now
        try:
            await self.db.execute(
                "INSERT INTO conditional_orders VALUES (?,?,?,?) ON CONFLICT(request_id) "
                "DO UPDATE SET state=excluded.state, updated_at_ms=excluded.updated_at_ms, "
                "payload=excluded.payload",
                (condition.request_id, condition.state, now, dumps(condition)),
            )
            await self.db.execute(
                "INSERT INTO strategy_events(event,timestamp_ms,payload) VALUES (?,?,?)",
                (event, now, dumps({"condition": condition})),
            )
            await self.db.commit()
        except sqlite3.Error as exc:
            await self.db.rollback()
            raise RuntimeError(f"SQLite conditional order write failed event={event}") from exc
        log_event(event, timestamp_ms=now, condition=condition)

    async def bind_mode(self, mode):
        async with self.db.execute("SELECT value FROM runtime_identity WHERE key='mode'") as c:
            row = await c.fetchone()
        if row and row[0] != mode:
            raise ValueError("Paper 与实盘必须使用不同数据库")
        if not row and mode == "live":
            async with self.db.execute(
                "SELECT (SELECT count(*) FROM conditional_orders) + "
                "(SELECT count(*) FROM maker_orders)"
            ) as c:
                if (await c.fetchone())[0]:
                    raise ValueError("实盘必须使用独立的新数据库，不能导入 Paper 条件单")
        await self.db.execute("INSERT OR IGNORE INTO runtime_identity VALUES ('mode',?)", (mode,))
        await self.db.commit()

    async def save_trade(self, trade, event):
        async with self.write_lock:
            try:
                await self.db.execute(
                    "INSERT INTO live_trades VALUES (?,?,?) ON CONFLICT(trade_id) DO UPDATE SET "
                    "state=excluded.state,payload=excluded.payload",
                    (trade["trade_id"], trade["state"], dumps(trade)),
                )
                await self.db.execute(
                    "INSERT INTO pair_events(event,timestamp_ms,payload) VALUES (?,?,?)",
                    (event, trade["updated_at_ms"], dumps(trade)),
                )
                await self.db.commit()
            except BaseException:
                await self.db.rollback()
                raise
        log_event(event, trade_id=trade["trade_id"], state=trade["state"])

    async def bind_identity(self, identity):
        async with self.db.execute("SELECT value FROM runtime_identity WHERE key='accounts'") as c:
            row = await c.fetchone()
        if row and row[0] != identity:
            raise ValueError("实盘数据库绑定的账户或 API Key 已改变，请核对账户归属")
        await self.db.execute(
            "INSERT OR IGNORE INTO runtime_identity VALUES ('accounts',?)", (identity,)
        )
        await self.db.commit()

    async def load_trades(self):
        async with self.db.execute("SELECT payload FROM live_trades ORDER BY rowid") as c:
            return [json.loads(r[0]) for r in await c.fetchall()]

    async def load_conditions(self) -> list[dict]:
        import json

        async with self.db.execute(
            "SELECT payload FROM conditional_orders WHERE state IN "
            "('WAITING','EXECUTING','CANCELING','PAUSED','REVIEW','OPEN','CLOSING') "
            "OR request_id IN "
            "(SELECT request_id FROM conditional_orders ORDER BY updated_at_ms DESC LIMIT 50)"
        ) as cursor:
            return [json.loads(row[0]) for row in await cursor.fetchall()]

    async def find_condition(self, request_id: str) -> dict | None:
        import json

        async with self.db.execute(
            "SELECT payload FROM conditional_orders WHERE request_id=?", (request_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return json.loads(row[0]) if row else None

    async def events(self) -> list[dict]:
        async with self.db.execute("SELECT * FROM strategy_events ORDER BY id") as c:
            return [dict(row) for row in await c.fetchall()]

    async def journal_mode(self) -> str:
        async with self.db.execute("PRAGMA journal_mode") as c:
            return (await c.fetchone())[0]
