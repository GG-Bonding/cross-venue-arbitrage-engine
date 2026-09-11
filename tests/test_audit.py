import hashlib
import json
import sqlite3

import pytest
from test_core import quote
from test_execution import spec

from arbitrage.audit import audit_session
from arbitrage.config import Settings
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.arbitrage_strategy import ArbitrageStrategy


async def recorded_order(path, *, bid="4416.90", ask="4416.98"):
    async with SQLiteRepository(path) as repo:
        engine = ArbitrageStrategy(Settings(), spec(), repo)
        await engine.start(900)
        for ts in (1000, 1100, 1200):
            await engine.on_quotes(quote(bid, ask, ts), quote(ts=ts), ts)
        await engine.shutdown(1250)


@pytest.mark.parametrize("prices", [("4416.90", "4416.98"), ("4408.10", "4408.20")])
async def test_audit_checks_both_directions_and_does_not_write_database(tmp_path, prices):
    path = tmp_path / "audit.db"
    await recorded_order(path, bid=prices[0], ask=prices[1])
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    report = audit_session(path)
    assert report["status"] == "pass"
    assert report["entry_evidence_checked"] == report["cancel_evidence_checked"] == 1
    assert report["max_simultaneous_orders"] == 1
    assert report["database_unfinished_order_ids"] == []
    assert report["failures"] == []
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


async def test_legacy_orders_are_limited_not_falsely_verified(tmp_path):
    path = tmp_path / "audit.db"
    await recorded_order(path)
    with sqlite3.connect(path) as db:
        for row_id, payload in db.execute("SELECT id,payload FROM strategy_events").fetchall():
            data = json.loads(payload)
            data.pop("entry_evidence", None)
            data.pop("cancel_evidence", None)
            db.execute(
                "UPDATE strategy_events SET payload=? WHERE id=?", (json.dumps(data), row_id)
            )
    report = audit_session(path)
    assert report["status"] == "limited"
    assert report["entry_evidence_missing"] == report["cancel_evidence_missing"] == 1
    assert report["failures"] == []


@pytest.mark.parametrize(
    "corruption,expected",
    [
        ("price", "maker_price"),
        ("future", "quote_age"),
        ("confirmation", "confirmation"),
        ("cancel", "cancel_reason"),
        ("duplicate", "lifecycle_sequence"),
    ],
)
async def test_audit_detects_corrupted_evidence_or_lifecycle(tmp_path, corruption, expected):
    path = tmp_path / "audit.db"
    await recorded_order(path)
    with sqlite3.connect(path) as db:
        row_id, timestamp, payload = db.execute(
            "SELECT id,timestamp_ms,payload FROM strategy_events WHERE event='maker_order_created'"
        ).fetchone()
        data = json.loads(payload)
        if corruption == "price":
            data["order"]["price"] = "4500.00"
        elif corruption == "future":
            data["entry_evidence"]["binance"]["exchange_ts_ms"] = timestamp + 1
        elif corruption == "confirmation":
            data["entry_evidence"]["confirmation"]["count"] = 1
        elif corruption == "cancel":
            row_id, payload = db.execute(
                "SELECT id,payload FROM strategy_events WHERE event='maker_cancel_requested'"
            ).fetchone()
            data = json.loads(payload)
            data["reason"] = "quote_skew"
        elif corruption == "duplicate":
            db.execute(
                "INSERT INTO strategy_events(event,timestamp_ms,payload) VALUES (?,?,?)",
                ("maker_order_created", timestamp, payload),
            )
        db.execute("UPDATE strategy_events SET payload=? WHERE id=?", (json.dumps(data), row_id))
    report = audit_session(path)
    assert report["status"] == "fail"
    assert expected in {f["check"] for f in report["failures"]}


def test_audit_missing_database_is_not_created(tmp_path):
    path = tmp_path / "missing.db"
    with pytest.raises(sqlite3.OperationalError):
        audit_session(path)
    assert not path.exists()


async def test_audit_detects_overlapping_order_lifetimes(tmp_path):
    path = tmp_path / "audit.db"
    await recorded_order(path)
    with sqlite3.connect(path) as db:
        rows = db.execute(
            "SELECT event,timestamp_ms,payload FROM strategy_events ORDER BY id"
        ).fetchall()
        state, mode, payload = db.execute("SELECT state,mode,payload FROM maker_orders").fetchone()
        clone = json.loads(payload)
        clone["order_id"] = "parallel-order"
        db.execute(
            "INSERT INTO maker_orders VALUES (?,?,?,?)",
            (clone["order_id"], state, mode, json.dumps(clone)),
        )
        db.execute("DELETE FROM strategy_events")  # Isolated test fixture only.
        for kind, ts, payload in rows:
            db.execute(
                "INSERT INTO strategy_events(event,timestamp_ms,payload) VALUES (?,?,?)",
                (kind, ts, payload),
            )
            if kind.startswith("maker_"):
                clone_event = json.loads(payload)
                clone_event["order"]["order_id"] = "parallel-order"
                db.execute(
                    "INSERT INTO strategy_events(event,timestamp_ms,payload) VALUES (?,?,?)",
                    (kind, ts, json.dumps(clone_event)),
                )
    report = audit_session(path)
    assert report["status"] == "fail"
    assert report["max_simultaneous_orders"] == 2
    assert {"check": "overlapping_orders"} in report["failures"]
