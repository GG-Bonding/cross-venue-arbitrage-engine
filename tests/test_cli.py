import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path


def run_cli(*args, env=None):
    child_env = {**os.environ, "PYTHONPATH": str(Path("src").resolve()), "TRADING_MODE": "paper"}
    child_env.pop("CONFIRM_LIVE_TRADING", None)
    child_env.update(env or {})
    return subprocess.run(
        [sys.executable, "-m", "arbitrage.main", *args],
        capture_output=True,
        text=True,
        timeout=15,
        env=child_env,
    )


def test_demo_and_read_only_status(tmp_path):
    path = tmp_path / "demo.db"
    result = run_cli("--demo", "--database", str(path))
    assert result.returncode == 0, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines()]
    assert "maker_order_created" in {e["event"] for e in events}
    snapshots = [e for e in events if e["event"] == "market_snapshot"]
    assert any(s["directions"]["SHORT_BINANCE"]["state"] == "CONFIRMED" for s in snapshots)
    assert snapshots[0]["directions"]["SHORT_BINANCE"]["raw_spread"] == "4.36"
    with sqlite3.connect(path) as db:
        assert (
            db.execute("SELECT count(*) FROM maker_orders WHERE state='CANCELED'").fetchone()[0]
            == 1
        )
        assert db.execute("SELECT count(*) FROM maker_fills").fetchone()[0] == 0
    status = run_cli("--status", "--database", str(path))
    assert status.returncode == 0, status.stderr
    assert json.loads(status.stdout)["unfinished_orders"] == []


def test_cli_live_guard_before_database_creation(tmp_path):
    path = tmp_path / "never.db"
    result = run_cli("--demo", "--database", str(path), env={"TRADING_MODE": "live"})
    assert result.returncode != 0
    assert "CONFIRM_LIVE_TRADING" in result.stderr
    assert not path.exists()


def test_status_missing_database_does_not_create_it(tmp_path):
    path = tmp_path / "missing.db"
    result = run_cli("--status", "--database", str(path))
    assert result.returncode != 0
    assert not path.exists()
