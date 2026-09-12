"""Observed fill prices only; missing evidence is never replaced by a quote."""

from decimal import Decimal, InvalidOperation

D = Decimal


def fill_price(result, key):
    try:
        value = D(str(result[key]))
        return value if value.is_finite() and value > 0 else None
    except (KeyError, TypeError, InvalidOperation):
        return None


def update_metrics(t, multiplier):
    for stage in ("entry", "exit"):
        suffix = "open" if stage == "entry" else "close"
        b = fill_price(t.get("binance_" + suffix), "avgPrice")
        m = fill_price(t.get("mt5_" + suffix), "price")
        if b is not None and m is not None:
            t["actual_" + stage + "_spread"] = str(b - m if t["sell"] else m - b)
    actual = t.get("actual_entry_spread")
    signal = t.get("signal_entry_spread", t.get("entry_spread"))
    if actual is not None and signal is not None:
        t["entry_spread_loss"] = str(D(signal) - D(actual))
    if t["state"] == "CLOSED" and actual is not None and t.get("actual_exit_spread") is not None:
        t["gross_pnl"] = str(
            D(t["filled_qty"]) * multiplier * (D(actual) - D(t["actual_exit_spread"]))
        )
    elif t["state"] == "FAILED" and t.get("compensate_client_id"):
        b = fill_price(t.get("binance_open"), "avgPrice")
        x = fill_price(t.get("binance_close"), "avgPrice")
        if b is not None and x is not None:
            t["compensation_gross_pnl"] = str(
                D(t["filled_qty"]) * multiplier * (b - x if t["sell"] else x - b)
            )


def settlement_totals(trades):
    totals, settled, pending = {}, 0, 0
    for t in trades:
        if D(t.get("filled_qty", "0")) <= 0:
            continue
        evidence = t.get("settlement")
        if evidence is None:
            pending += 1
            continue
        settled += 1
        summary = evidence["summary"]
        for currency, value in (
            ("USDT", summary["binance_net"]),
            (evidence["mt5_currency"], summary["mt5_net"]),
        ):
            totals[currency] = totals.get(currency, D(0)) + D(value)
        # Binance commissions in other assets cannot be silently treated as USDT.
        for currency, fee in summary["binance_fees"].items():
            if currency != "USDT":
                totals[currency] = totals.get(currency, D(0)) - D(fee)
    return dict(settled_attempts=settled, unsettled_filled_attempts=pending, net_by_currency=totals)
