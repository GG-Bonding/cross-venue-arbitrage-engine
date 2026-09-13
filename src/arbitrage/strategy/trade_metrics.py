"""Observed fill prices only; missing evidence is never replaced by a quote."""

from decimal import Decimal, InvalidOperation

D = Decimal


def expected_net_pnl(t, cost, budget, multiplier):
    actual = t.get("actual_entry_spread")
    fees = {key: getattr(budget, key) for key in ("open_fees", "close_fees", "funding", "swap")}
    quantity = D(t.get("paired_qty", t.get("filled_qty", "0")))
    result = dict(gross_pnl=None, costs=fees, expected_net_pnl=None, estimated=True)
    if actual is None:
        return result
    if D(t.get("closed_qty", "0")) or t.get("compensations"):
        cash = observed_cash(t, multiplier)
        if cash is None:
            return result
        gross = cash - (quantity - D(t.get("closed_qty", "0"))) * multiplier * cost
    else:
        gross = quantity * multiplier * (D(actual) - cost)
    result["gross_pnl"] = gross
    if budget.quote_units_aligned and all(value is not None for value in fees.values()):
        result["expected_net_pnl"] = gross - sum(fees.values(), D(0))
    return result


def observed_cash(t, multiplier):
    """Local pair cash-flow attribution, including risk compensation and all MT5 tickets."""
    contract = t.get("mt5_contract_size")
    if contract is None:
        return None
    sign = D(1) if t["sell"] else D(-1)
    total = D(0)
    orders = [(t.get("binance_open"), sign)]
    orders.extend((a.get("result"), -sign) for a in t.get("close_attempts", []) if a.get("result"))
    orders.extend(
        (a.get("result"), sign if a["restore"] else -sign) for a in t.get("compensations", [])
    )
    for result, side in orders:
        if result is None:
            return None
        quantity = D(result["executedQty"])
        if not quantity:
            continue
        price = fill_price(result, "avgPrice")
        if price is None:
            return None
        total += side * quantity * multiplier * price
    for name, side in (("mt5_open_fills", -sign), ("mt5_close_fills", sign)):
        for result in t.get(name, []):
            price = fill_price(result, "price")
            if price is None:
                return None
            total += side * D(result["volume"]) * D(contract) * price
    return total


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
            D(t.get("paired_qty", t["filled_qty"]))
            * multiplier
            * (D(actual) - D(t["actual_exit_spread"]))
        )
        if "mt5_open_fills" in t:
            cash = observed_cash(t, multiplier)
            t["gross_pnl"] = str(cash) if cash is not None else None
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
