from decimal import Decimal as D

import pytest
from test_core import quote
from test_execution import spec
from test_manual_orders import command, request

from arbitrage.config import Settings
from arbitrage.domain.specs import MT5Spec
from arbitrage.execution.live_venues import ExecutionUnknown, OrderRejected
from arbitrage.main import read_status
from arbitrage.persistence.sqlite_repository import SQLiteRepository
from arbitrage.strategy.live_orders import LiveOrderStrategy


class Binance:
    async def identity(self):
        return "test-binance"

    def __init__(self):
        self.sent = []
        self.amounts = {"LONG": D(0), "SHORT": D(0)}
        self.fill = D(1)
        self.error = None
        self.result = None
        self.canceled = 0

    async def initialize(self):
        pass

    async def inventory(self):
        return [{"positionSide": k, "positionAmt": str(v)} for k, v in self.amounts.items()]

    async def open_orders(self):
        return []

    async def submit(self, client_id, side, position_side, quantity, *, price=None):
        self.sent.append((side, position_side, quantity, price))
        if self.error:
            raise self.error
        filled = self.fill if price is not None else quantity
        self.amounts[position_side] += filled if price is not None else -filled
        self.result = dict(
            status="FILLED" if filled == quantity else "PARTIALLY_FILLED",
            executedQty=str(filled),
            orderId=len(self.sent),
            avgPrice="4400",
        )
        return self.result

    async def order(self, client_id):
        return self.result

    async def cancel(self, client_id):
        self.canceled += 1
        return {**self.result, "status": "CANCELED"}

    async def account(self):
        return dict(currency="USDT", equity="1000", available="900", profit="0")

    async def trades(self, order_id):
        return []


class MT5:
    async def identity(self):
        return "test-mt5"

    def __init__(self):
        self.sent, self.rows = [], []
        self.error = None

    async def initialize(self):
        pass

    async def positions(self):
        return self.rows.copy()

    async def send(self, tag, buy, lots, *, ticket=None):
        self.sent.append((buy, lots, ticket))
        if self.error:
            raise self.error
        if ticket:
            self.rows = [p for p in self.rows if p["ticket"] != ticket]
            order = ticket
        else:
            order = 100 + len(self.sent)
            self.rows.append(
                dict(
                    ticket=order,
                    identifier=order,
                    volume=str(lots),
                    type=0 if buy else 1,
                    magic=260911,
                )
            )
        return dict(order=order, deal=order, volume=str(lots), price="4400")

    async def account(self):
        return dict(currency="USD", equity="1000", available="900", profit="0")

    async def deals(self, identifier):
        return []


def engine(repo, monkeypatch):
    monkeypatch.setattr("arbitrage.strategy.live_orders.now_ms", lambda: 1200)
    monkeypatch.setenv("CONFIRM_LIVE_TRADING", "I_UNDERSTAND")
    monkeypatch.setenv("BINANCE_API_KEY", "test-only-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "test-only-secret")
    settings = Settings(
        mode="live", live={"enabled": True}, trading={"binance_underlying_per_qty": "1"}
    )
    return LiveOrderStrategy(
        settings,
        spec(),
        repo,
        Binance(),
        MT5(),
        MT5Spec("XAUUSD", D(100), D("0.01"), D(100), D("0.01")),
    )


async def trigger(e):
    for ts in (1000, 1100, 1200):
        await e.on_quotes(quote("4416.90", "4416.98", ts), quote(ts=ts), ts)
    if e.worker:
        await e.worker


async def test_live_no_manual_request_never_sends(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        await trigger(e)
        assert e.binance.sent == e.mt5.sent == []
        await e.shutdown(1400)


@pytest.mark.parametrize("direction", ["SHORT_BINANCE", "LONG_BINANCE"])
async def test_real_adapter_flow_opens_and_ticket_closes_both_sides(
    tmp_path, monkeypatch, direction
):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request(direction=direction, entry_threshold="-10", cancel_threshold="-11")
        await command(e, "create", r)
        await trigger(e)
        c = e.conditions[r["request_id"]]
        assert c.state == "OPEN"
        sell = direction == "SHORT_BINANCE"
        assert e.binance.sent[0][:3] == (
            "SELL" if sell else "BUY",
            "SHORT" if sell else "LONG",
            D(1),
        )
        assert e.mt5.sent[0] == (sell, D("0.01"), None)
        await command(e, "close", c.request_id, 1250)
        await e.worker
        assert c.state == "DONE" and c.last_result == "CLOSED"
        assert e.mt5.sent[1] == (not sell, D("0.01"), 101)
        assert all(v == 0 for v in e.binance.amounts.values()) and e.mt5.rows == []
        records = await repo.load_trades()
        assert records[0]["state"] == "CLOSED"
        status = read_status(tmp_path / "live.db")
        assert status["mode"] == "live" and status["unfinished_orders"] == []
        await e.shutdown(1400)


async def test_live_fifo_only_one_submission_per_confirmation(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        a, b = request(), request()
        await command(e, "create", a)
        await command(e, "create", b)
        await trigger(e)
        assert len(e.binance.sent) == 1
        assert e.conditions[a["request_id"]].state == "OPEN"
        assert e.conditions[b["request_id"]].state == "WAITING"
        await e.shutdown(1400)


async def test_partial_fill_unrepresentable_cancels_then_flattens(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        e.binance.fill = D("0.5")
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        assert e.binance.canceled == 1
        assert e.binance.sent[1] == ("BUY", "SHORT", D("0.5"), None)
        assert not e.mt5.sent
        assert e.conditions[r["request_id"]].state == "FAILED"
        assert e.binance.amounts["SHORT"] == 0
        await e.shutdown(1400)


@pytest.mark.parametrize("venue", ["binance", "mt5"])
async def test_unknown_execution_blocks_following_orders_and_survives_restart(
    tmp_path, monkeypatch, venue
):
    path = tmp_path / "live.db"
    async with SQLiteRepository(path) as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        getattr(e, venue).error = ExecutionUnknown("ambiguous acknowledgement")
        r = request()
        await command(e, "create", r)
        await trigger(e)
        assert e.state == "SAFE_MODE" and e.conditions[r["request_id"]].state == "REVIEW"
        assert len(e.binance.sent) == 1
        with pytest.raises(ValueError):
            await command(e, "create", request())
        with pytest.raises(ValueError):
            await command(e, "cancel", r["request_id"])
        await e.shutdown(1400)
    async with SQLiteRepository(path) as repo:
        restored = engine(repo, monkeypatch)
        await restored.start(1500)
        assert restored.state == "SAFE_MODE"
        await trigger(restored)
        assert not restored.binance.sent
        await restored.shutdown(1600)


async def test_definitive_hedge_precheck_rejection_flattens_binance(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        e.mt5.error = OrderRejected("precheck")
        r = request()
        await command(e, "create", r)
        await trigger(e)
        assert len(e.binance.sent) == 2 and e.binance.amounts["SHORT"] == 0
        assert e.conditions[r["request_id"]].state == "FAILED"
        await e.shutdown(1400)


async def test_external_positions_block_live_start(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        e.binance.amounts["LONG"] = D(1)
        await e.start(900)
        assert e.state == "SAFE_MODE"
        with pytest.raises(ValueError):
            await command(e, "create", request())
        await e.shutdown(1400)


async def test_paper_database_cannot_be_reused_for_live(tmp_path):
    async with SQLiteRepository(tmp_path / "db") as repo:
        await repo.bind_mode("paper")
        with pytest.raises(ValueError, match="数据库"):
            await repo.bind_mode("live")


async def test_bulk_close_runs_serially_and_repeat_requeues_after_close(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        a, b = request(repeat=True), request()
        await command(e, "create", a)
        await command(e, "create", b)
        await trigger(e)
        await trigger(e)
        assert [c.state for c in e.conditions.values()] == ["OPEN", "OPEN"]
        result = await command(e, "close_all", None, 1300)
        assert result["queued"] == 2
        await e.worker
        assert len(e.binance.sent) == 3
        assert e.conditions[b["request_id"]].state == "OPEN"
        await e.on_timer(1350)
        await e.worker
        assert len(e.binance.sent) == 4
        assert e.conditions[a["request_id"]].state == "WAITING"
        assert e.conditions[b["request_id"]].state == "DONE"
        await e.shutdown(1400)


async def test_conditional_exit_uses_crossed_quotes_and_rechecks_before_send(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request(exit_threshold="4.40")
        await command(e, "create", r)
        await trigger(e)
        c = e.conditions[r["request_id"]]
        # Entry maker spread 4.36 is below 4.40, but crossed exit spread 4.48 is not.
        await trigger(e)
        assert c.state == "OPEN" and not c.close_requested
        c.exit_threshold = D("5")
        await trigger(e)
        assert c.close_requested
        await e.on_timer(1200)
        # Before preflight completes the exit price deteriorates; do not execute queued target.
        e.binance_quote = quote("4419.90", "4419.98", 1200)
        await e.worker
        assert c.state == "OPEN" and len(e.binance.sent) == 1
        await e.shutdown(1400)


async def test_known_open_pair_reconciles_after_restart_without_reopening(tmp_path, monkeypatch):
    path = tmp_path / "live.db"
    async with SQLiteRepository(path) as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        await e.shutdown(1400)
        venues = e.binance, e.mt5
    async with SQLiteRepository(path) as repo:
        restored = engine(repo, monkeypatch)
        restored.binance, restored.mt5 = venues
        await restored.start(1500)
        assert restored.state != "SAFE_MODE"
        assert restored.conditions[r["request_id"]].state == "OPEN"
        await trigger(restored)
        assert len(restored.binance.sent) == 1
        await restored.shutdown(1600)


async def test_failed_second_close_leg_blocks_every_later_execution(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        e.mt5.error = ExecutionUnknown("close acknowledgement lost")
        await command(e, "close", r["request_id"], 1300)
        await e.worker
        assert e.state == "SAFE_MODE" and len(e.binance.sent) == 2
        with pytest.raises(ValueError):
            await command(e, "close", r["request_id"], 1350)
        await e.shutdown(1400)


async def test_representable_partial_fill_hedges_actual_not_requested_quantity(
    tmp_path, monkeypatch
):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        e.settings = e.base_settings = e.settings.model_copy(
            update={"live": e.settings.live.model_copy(update={"max_binance_qty": D(2)})}
        )
        await e.start(900)
        r = request(quantity="2")
        await command(e, "create", r)
        await trigger(e)
        assert e.binance.canceled == 1
        assert e.conditions[r["request_id"]].state == "OPEN"
        assert e.mt5.sent == [(True, D("0.01"), None)]
        t = next(iter(e.trades.values()))
        assert t["quantity"] == "2" and t["filled_qty"] == "1"
        await e.shutdown(1400)


async def test_settlement_requires_complete_volume_and_keeps_fee_currencies(tmp_path, monkeypatch):
    async with SQLiteRepository(tmp_path / "live.db") as repo:
        e = engine(repo, monkeypatch)
        await e.start(900)
        r = request()
        await command(e, "create", r)
        await trigger(e)
        await command(e, "close", r["request_id"], 1300)
        await e.worker
        t = next(iter(e.trades.values()))
        await e._accounts()
        assert "settlement" not in t

        async def trades(order_id):
            return [
                dict(
                    qty="1",
                    realizedPnl="0" if order_id == 1 else "2",
                    commission="0.1",
                    commissionAsset="USDT" if order_id == 1 else "BNB",
                )
            ]

        async def deals(identifier):
            return [
                dict(entry=i, volume="0.01", profit=str(i), commission="-0.1", swap="0", fee="0")
                for i in (0, 1)
            ]

        e.binance.trades, e.mt5.deals = trades, deals
        await e._accounts()
        summary = t["settlement"]["summary"]
        assert summary["binance_net"] == D("1.9")
        assert summary["binance_fees"] == {"USDT": D("0.1"), "BNB": D("0.1")}
        assert summary["mt5_net"] == D("0.8")
        await e.shutdown(1400)
