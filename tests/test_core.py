from decimal import Decimal as D

import pytest

from arbitrage.domain.enums import Direction
from arbitrage.domain.quote import Quote
from arbitrage.risk.quote_guard import QuoteGuard
from arbitrage.strategy.confirmation import EntryConfirmation
from arbitrage.strategy.spread import entry_spread, pending_spread


def quote(bid="4412.50", ask="4412.62", ts=1000, local=None):
    return Quote(D(bid), D(ask), None, None, ts, ts if local is None else local)


@pytest.mark.parametrize(
    "direction,bid,ask,expected",
    [
        (Direction.SHORT_BINANCE, "4416.90", "4416.98", "4.36"),
        (Direction.LONG_BINANCE, "4408.10", "4408.20", "4.40"),
    ],
)
def test_direction_spread(direction, bid, ask, expected):
    result = entry_spread(direction, quote(bid, ask), quote(), D("4.20"))
    assert result.raw_spread == D(expected)
    assert result.edge == D(expected) - D("4.20")


@pytest.mark.parametrize(
    "direction,price,expected",
    [
        (Direction.SHORT_BINANCE, "4416.98", "4.36"),
        (Direction.LONG_BINANCE, "4408.10", "4.40"),
    ],
)
def test_pending_uses_order_price(direction, price, expected):
    assert pending_spread(direction, D(price), quote()) == D(expected)


def test_confirmation_requires_both_ticks_and_duration():
    c = EntryConfirmation(3, 200)
    assert c.update(D("0"), 1000).state == "CONFIRMING"
    assert c.update(D("0.1"), 1200).state == "CONFIRMING"
    assert c.update(D("0.1"), 1201).state == "CONFIRMED"
    c.reset()
    for ts in (1000, 1001, 1002):
        assert c.update(D("0.1"), ts).state == "CONFIRMING"
    assert c.update(D("0.1"), 1200).state == "CONFIRMED"


def test_failure_stale_and_duplicate_ticks_reset_or_do_not_count():
    c = EntryConfirmation(3, 200)
    c.update(D("0.1"), 1000)
    assert c.update(D("0.1"), 1000).count == 1
    assert c.update(D("-0.01"), 1100).count == 0
    assert c.update(D("0.1"), 1200).count == 1
    assert c.update(D("0.1"), 1300, valid=False).state == "IDLE"
    assert c.update(D("0.1"), 1400).count == 1


@pytest.mark.parametrize(
    "b,m,now,reason",
    [
        (quote(), quote(), 1300, None),
        (quote(), quote(), 1301, "quote_stale"),
        (quote(ts=1000), quote(ts=1201), 1250, "quote_skew"),
        (quote(ts=1, local=1000), quote(ts=1, local=1000), 1000, "quote_stale"),
        (quote(ts=1001), quote(), 1000, "quote_future"),
    ],
)
def test_quote_guard(b, m, now, reason):
    guard = QuoteGuard(300, 200)
    assert guard.check(b, m, now) == reason
    if reason:
        assert guard.metrics.counters[reason + "_total"] == 1


@pytest.mark.parametrize("bid,ask", [("0", "1"), ("2", "1"), ("NaN", "2")])
def test_invalid_quote_rejected(bid, ask):
    with pytest.raises(ValueError):
        quote(bid, ask)
