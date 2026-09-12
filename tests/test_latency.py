import json
import sqlite3

from arbitrage.latency import summarize


def test_latency_report_reads_latest_trade_once_without_creating_database(tmp_path):
    missing = tmp_path / "missing.db"
    import pytest

    with pytest.raises(sqlite3.OperationalError):
        summarize(missing)
    assert not missing.exists()
    path = tmp_path / "sample.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE live_trades (payload TEXT)")
        for value in range(1, 101):
            db.execute(
                "INSERT INTO live_trades VALUES (?)",
                (json.dumps({"timings_ns": {"entry_dispatch": value * 1000000}}),),
            )
        db.execute("INSERT INTO live_trades VALUES ('{}')")
    report = summarize(path)
    assert report["entry_dispatch"] == {"count": 100, "p50_ms": 50, "p95_ms": 95, "p99_ms": 99}
    assert report["hedge_dispatch"]["count"] == 0
    assert report["hedge_dispatch"]["p50_ms"] is None
