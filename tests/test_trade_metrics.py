from decimal import Decimal as D

import pytest

from arbitrage.config import ProfitBudget
from arbitrage.strategy.trade_metrics import expected_net_pnl, settlement_totals, update_metrics


def test_expected_net_requires_complete_explicit_budget():
    t = dict(actual_entry_spread="4.08", filled_qty="1", sell=True)
    assert expected_net_pnl(t, D("2.90"), ProfitBudget(), D(1))["expected_net_pnl"] is None
    budget = ProfitBudget(
        quote_units_aligned=True, open_fees="0.1", close_fees="0.15", funding="0.05", swap="0.05"
    )
    result = expected_net_pnl(t, D("2.90"), budget, D(1))
    assert result["gross_pnl"] == D("1.18")
    assert result["expected_net_pnl"] == D("0.83")
    assert result["estimated"]


@pytest.mark.parametrize("sell,entry,exit_,pnl", [(True, "4", "3", "2"), (False, "-4", "-3", "-2")])
def test_actual_paired_spreads_use_fills_and_quantity(sell, entry, exit_, pnl):
    t = dict(
        state="CLOSED",
        sell=sell,
        filled_qty="2",
        signal_entry_spread="5",
        binance_open={"avgPrice": "4417"},
        mt5_open={"price": "4413"},
        binance_close={"avgPrice": "4420"},
        mt5_close={"price": "4417"},
    )
    update_metrics(t, D(1))
    assert t["actual_entry_spread"] == entry
    assert t["actual_exit_spread"] == exit_
    assert t["gross_pnl"] == pnl
    assert D(t["entry_spread_loss"]) == D(5) - D(entry)


@pytest.mark.parametrize("missing", [None, {}, {"avgPrice": "0"}, {"avgPrice": "NaN"}])
def test_missing_fill_price_does_not_use_signal_or_quote(missing):
    t = dict(
        state="OPEN",
        sell=True,
        filled_qty="1",
        signal_entry_spread="5",
        binance_open=missing,
        mt5_open={"price": "4413"},
    )
    update_metrics(t, D(1))
    assert "actual_entry_spread" not in t


def test_total_includes_compensation_and_marks_unsettled_attempts():
    trades = [
        dict(
            state=state,
            filled_qty="1",
            settlement=dict(
                summary=dict(binance_net=pnl, mt5_net="0", binance_fees={"BNB": "0.1"}),
                mt5_currency="USD",
            ),
        )
        for state, pnl in [("CLOSED", "2"), ("FAILED", "-3")]
    ]
    trades.append(dict(state="REVIEW", filled_qty="1"))
    total = settlement_totals(trades)
    assert total["net_by_currency"] == {"USDT": D(-1), "USD": D(0), "BNB": D("-0.2")}
    assert total["settled_attempts"] == 2
    assert total["unsettled_filled_attempts"] == 1
