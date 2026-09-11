"""Read-only, consistent-snapshot audit of the latest Paper session."""

import argparse
import json
import math
import sqlite3
from collections import Counter, defaultdict
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path
from statistics import median

from arbitrage.observability import dumps, now_ms


def distribution(values: list[int]) -> dict | None:
    if not values:
        return None
    ordered = sorted(values)
    return {
        "min": ordered[0],
        "median": median(ordered),
        "p95": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "max": ordered[-1],
    }


def check_entry(order: dict, evidence: dict, timestamp: int) -> list[str]:
    """Recompute against the recorded inputs, never today's settings or a nearby tick."""
    failures = []

    def check(condition, name):
        if not condition:
            failures.append(name)

    b, m, spec = evidence["binance"], evidence["mt5"], evidence["spec"]
    sell = order["direction"] == "SHORT_BINANCE"
    check(order["direction"] in {"SHORT_BINANCE", "LONG_BINANCE"}, "direction")
    mode = evidence["direction_mode"]
    check(mode == "both" or mode == ("a" if sell else "b"), "direction_enabled")
    check(evidence["placement_mode"] in {"once", "loop"}, "placement_enabled")
    if order.get("conditional_id"):
        condition = evidence.get("conditional_order")
        check(
            bool(condition)
            and condition["request_id"] == order["conditional_id"]
            and condition["direction"] == order["direction"]
            and Decimal(condition["entry_threshold"]) == Decimal(evidence["entry"]["threshold"])
            and Decimal(condition["quantity"]) == Decimal(evidence["quantity"])
            and condition["created_at_ms"] <= timestamp,
            "manual_condition_link",
        )
    for q in (b, m):
        for field in ("exchange_ts_ms", "local_ts_ms"):
            check(0 <= timestamp - q[field] <= evidence["max_quote_age_ms"], "quote_age")
    check(
        abs(b["exchange_ts_ms"] - m["exchange_ts_ms"]) <= evidence["max_quote_skew_ms"],
        "quote_skew",
    )
    raw = Decimal(b["ask"]) - Decimal(m["ask"]) if sell else Decimal(m["bid"]) - Decimal(b["bid"])
    edge = raw - Decimal(evidence["entry"]["threshold"])
    check(raw == Decimal(evidence["spread"]["raw_spread"]), "raw_spread")
    check(edge == Decimal(evidence["spread"]["edge"]) and edge >= 0, "entry_edge")
    confirmation = evidence["confirmation"]
    required = evidence["entry"]["confirmation"]
    check(
        confirmation["state"] == "CONFIRMED"
        and confirmation["count"] >= required["min_ticks"]
        and confirmation["duration_ms"] >= required["min_duration_ms"],
        "confirmation",
    )
    tick, step = Decimal(spec["tick_size"]), Decimal(spec["step_size"])
    price, qty = Decimal(order["price"]), Decimal(order["quantity"])
    expected_price = (Decimal(b["ask" if sell else "bid"]) / tick).to_integral_value(
        rounding=ROUND_CEILING if sell else ROUND_FLOOR
    ) * tick
    check(price == expected_price, "maker_price")
    check(price > Decimal(b["bid"]) if sell else price < Decimal(b["ask"]), "post_only")
    expected_qty = (Decimal(evidence["quantity"]) / step).to_integral_value(
        rounding=ROUND_FLOOR
    ) * step
    check(
        qty == expected_qty and Decimal(spec["min_qty"]) <= qty <= Decimal(spec["max_qty"]),
        "quantity",
    )
    check(qty * price >= Decimal(spec["min_notional"]), "min_notional")
    check(not Decimal(spec["min_price"]) or price >= Decimal(spec["min_price"]), "min_price")
    check(not Decimal(spec["max_price"]) or price <= Decimal(spec["max_price"]), "max_price")
    return failures


def check_cancel(order: dict, payload: dict, timestamp: int) -> list[str]:
    evidence, reason = payload["cancel_evidence"], payload["reason"]
    b, m, config = evidence["binance"], evidence["mt5"], evidence["maker"]
    guard_reason = None
    if b is None or m is None:
        guard_reason = "quote_missing"
    else:
        times = [q[f] for q in (b, m) for f in ("local_ts_ms", "exchange_ts_ms")]
        if any(ts > timestamp for ts in times):
            guard_reason = "quote_future"
        elif any(timestamp - ts > evidence["max_quote_age_ms"] for ts in times):
            guard_reason = "quote_stale"
        elif abs(b["exchange_ts_ms"] - m["exchange_ts_ms"]) > evidence["max_quote_skew_ms"]:
            guard_reason = "quote_skew"
    if reason.startswith("quote_"):
        valid = reason == guard_reason
    elif reason == "pending_timeout":
        valid = (
            guard_reason is None and timestamp - order["created_at_ms"] > config["max_pending_ms"]
        )
    elif reason == "spread_invalid":
        pending = (
            (
                Decimal(order["price"]) - Decimal(m["ask"])
                if order["direction"] == "SHORT_BINANCE"
                else Decimal(m["bid"]) - Decimal(order["price"])
            )
            if m
            else None
        )
        since = evidence["invalid_since_ms"]
        valid = (
            guard_reason is None
            and pending < Decimal(config["cancel_threshold"])
            and since is not None
            and timestamp - since >= config["cancel_confirm_ms"]
        )
    elif reason == "direction_disabled":
        valid = evidence["direction_mode"] == (
            "b" if order["direction"] == "SHORT_BINANCE" else "a"
        )
    elif reason == "placement_stopped":
        valid = evidence["placement_mode"] == "paused"
    else:
        valid = reason == "shutdown"
    return [] if valid else ["cancel_reason"]


def audit_session(path: Path) -> dict:
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5) as db:
        db.execute("BEGIN")
        start = db.execute(
            "SELECT id,timestamp_ms FROM strategy_events WHERE event='paper_reconciliation' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if start is None:
            raise ValueError("No Paper session found")
        events = [
            (row[0], row[1], row[2], json.loads(row[3]))
            for row in db.execute(
                "SELECT id,event,timestamp_ms,payload FROM strategy_events WHERE id>=? ORDER BY id",
                (start[0],),
            )
        ]
        stored = {
            row[0]: (row[1], row[2], json.loads(row[3]))
            for row in db.execute("SELECT * FROM maker_orders")
        }
        samples = [
            (ts, json.loads(payload))
            for ts, payload in db.execute(
                "SELECT timestamp_ms,payload FROM quotes_sample WHERE timestamp_ms>=? ORDER BY id",
                (start[1],),
            )
        ]
        fills = {
            table: db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("maker_fills", "mt5_orders")
        }
        captured = now_ms()
    grouped, reasons, directions = defaultdict(list), Counter(), Counter()
    active, maximum, failures = set(), 0, []
    lifetimes, waits, latencies, counts, durations = [], [], [], [], []
    checked, cancel_checked, cancel_total = 0, 0, 0
    kinds = ["maker_order_created", "maker_cancel_requested", "maker_order_canceled"]
    for _event_id, kind, ts, payload in events:
        if kind not in kinds:
            continue
        order = payload["order"]
        oid = order["order_id"]
        grouped[oid].append((kind, ts, payload))
        if kind == kinds[0]:
            active.add(oid)
            maximum = max(maximum, len(active))
        elif kind == kinds[2]:
            active.discard(oid)
    for oid, records in grouped.items():
        order = records[0][2]["order"]
        errors = []
        sequence = [r[0] for r in records]
        if sequence != kinds[: len(sequence)]:
            errors.append("lifecycle_sequence")
        for kind, _ts, payload in records:
            current = payload["order"]
            if any(
                current[f] != order[f] for f in ("direction", "price", "quantity", "created_at_ms")
            ):
                errors.append("immutable_order_fields")
            if (
                current["mode"] != "paper"
                or current["time_in_force"] != "GTX"
                or Decimal(current["filled_qty"]) != 0
            ):
                errors.append("phase1_order_contract")
            if (
                current["state"]
                != dict(zip(kinds, ("MAKER_PENDING", "CANCELING", "CANCELED"), strict=True))[kind]
            ):
                errors.append("event_order_state")
        if stored.get(oid) != (records[-1][2]["order"]["state"], "paper", records[-1][2]["order"]):
            errors.append("database_order_mismatch")
        if records[0][0] == kinds[0]:
            directions[order["direction"]] += 1
            if order["created_at_ms"] != records[0][1]:
                errors.append("creation_time")
            evidence = records[0][2].get("entry_evidence")
            if evidence:
                checked += 1
                errors.extend(check_entry(order, evidence, records[0][1]))
                counts.append(evidence["confirmation"]["count"])
                durations.append(evidence["confirmation"]["duration_ms"])
        if len(records) >= 2:
            cancel_total += 1
            if records[1][2].get("cancel_evidence"):
                cancel_checked += 1
                errors.extend(check_cancel(order, records[1][2], records[1][1]))
            reasons[records[1][2].get("reason", "unknown")] += 1
            waits.append(records[1][1] - records[0][1])
            if records[1][2]["order"]["cancel_requested_at_ms"] != records[1][1]:
                errors.append("cancel_request_time")
        if len(records) >= 3:
            lifetimes.append(records[2][1] - records[0][1])
            latencies.append(records[2][1] - records[1][1])
            evidence = records[0][2].get("entry_evidence")
            if (
                evidence
                and records[2][2].get("reason") != "shutdown"
                and latencies[-1] < evidence["maker"]["paper_cancel_latency_ms"]
            ):
                errors.append("cancel_ack_too_early")
            if records[2][2]["order"]["cancel_requested_at_ms"] != records[1][1]:
                errors.append("cancel_ack_request_time")
        if any(records[i][1] < records[i - 1][1] for i in range(1, len(records))):
            errors.append("order_clock_regression")
        failures.extend({"order_id": oid, "check": name} for name in sorted(set(errors)))
    if maximum > 1:
        failures.append({"check": "overlapping_orders"})
    if any(fills.values()):
        failures.append({"check": "unexpected_phase1_fill_or_hedge_records"})
    recent = [(ts, p) for ts, p in samples if ts >= captured - 300_000]
    unfinished = [oid for oid, row in stored.items() if row[0] != "CANCELED"]
    complete = (
        bool(grouped)
        and checked == len(grouped)
        and cancel_checked == cancel_total
        and not unfinished
    )
    return {
        "status": "fail" if failures else "pass" if complete else "limited",
        "captured_at_ms": captured,
        "session_start_ms": start[1],
        "session_start_event_id": start[0],
        "cutoff_event_id": events[-1][0],
        "orders": len(grouped),
        "directions": dict(directions),
        "entry_evidence_checked": checked,
        "entry_evidence_missing": len(grouped) - checked,
        "cancel_evidence_checked": cancel_checked,
        "cancel_evidence_missing": cancel_total - cancel_checked,
        "max_simultaneous_orders": maximum,
        "unfinished_order_ids": sorted(active),
        "database_unfinished_order_ids": sorted(unfinished),
        "cancel_reasons": dict(reasons),
        "created_to_cancel_request_ms": distribution(waits),
        "cancel_ack_ms": distribution(latencies),
        "order_lifetime_ms": distribution(lifetimes),
        "confirmation_ticks": distribution(counts),
        "confirmation_ms": distribution(durations),
        "sampled_quotes": len(samples),
        "sampled_quotes_valid": sum(bool(p["quotes_valid"]) for _, p in samples),
        "recent_5min_samples": len(recent),
        "recent_5min_valid": sum(bool(p["quotes_valid"]) for _, p in recent),
        "database_execution_records": fills,
        "failures": failures,
        "limitations": [
            "Legacy orders without evidence cannot have their entry price/quote checks verified.",
            "Quote validity counts are samples, not per-tick or time-weighted availability.",
            "This validates Paper records only, not exchange fills, hedging or profitability.",
            "Confirmation/hysteresis summaries are checked; full tick replay is unavailable.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.output and args.output.resolve() == args.database.resolve():
        parser.error("Audit output must not overwrite the database")
    report = audit_session(args.database)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(dumps(report) + "\n", encoding="utf-8")
    print(dumps(report))
    if report["status"] == "fail":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
