import hashlib
import hmac
from decimal import Decimal as D
from types import SimpleNamespace as NS
from urllib.parse import parse_qs

import pytest

from arbitrage.config import Settings
from arbitrage.execution.live_venues import (
    BinanceTrading,
    ExecutionUnknown,
    MT5Trading,
    OrderRejected,
)


async def test_binance_post_only_and_close_position_side():
    adapter = BinanceTrading(Settings(), None, key="test", secret="secret")
    calls = []

    async def request(method, path, **params):
        calls.append((method, path, params))
        return {"status": "FILLED"}

    adapter.request = request
    await adapter.submit_maker("open-id", "SELL", "SHORT", D(1), price=D(4000))
    await adapter.submit_market("close-id", "BUY", "SHORT", D(1))
    opening, closing = calls
    assert opening[2]["timeInForce"] == "GTX"
    assert opening[2]["type"] == "LIMIT"
    assert opening[2]["newOrderRespType"] == "ACK"
    assert opening[2]["price"] == "4000"
    assert closing[2]["newOrderRespType"] == "RESULT"
    assert "price" not in closing[2] and "timeInForce" not in closing[2]
    assert opening[2]["positionSide"] == closing[2]["positionSide"] == "SHORT"
    assert closing[2]["type"] == "MARKET" and "reduceOnly" not in closing[2]


@pytest.mark.parametrize("query_succeeds", [True, False])
@pytest.mark.parametrize("maker", [True, False])
async def test_uncertain_binance_post_only_queries_never_resends(query_succeeds, maker):
    adapter = BinanceTrading(Settings(), None, key="test", secret="secret")
    methods = []

    async def request(method, path, **params):
        methods.append(method)
        if method == "POST":
            raise ExecutionUnknown("network timeout")
        if not query_succeeds:
            raise OrderRejected("not found yet")
        assert params["origClientOrderId"] == "stable-id"
        return {"status": "FILLED", "executedQty": "1"}

    adapter.request = request
    submit = adapter.submit_maker if maker else adapter.submit_market
    kwargs = {"price": D(4000)} if maker else {}
    if query_succeeds:
        assert (await submit("stable-id", "SELL", "SHORT", D(1), **kwargs))["status"] == "FILLED"
    else:
        with pytest.raises(ExecutionUnknown):
            await submit("stable-id", "SELL", "SHORT", D(1), **kwargs)
    assert methods == ["POST", "GET"]


async def test_cancel_ack_uses_query_fill_not_delete_response():
    adapter = BinanceTrading(Settings(), None, key="test", secret="secret")

    async def request(method, path, **params):
        if method == "DELETE":
            raise OrderRejected("already filled")
        return {"status": "FILLED", "executedQty": "0.75"}

    adapter.request = request
    assert (await adapter.cancel("id"))["executedQty"] == "0.75"


@pytest.mark.parametrize("maker", [True, False])
async def test_definitive_submission_rejection_is_not_retried(maker):
    adapter = BinanceTrading(Settings(), None, key="test", secret="secret")
    methods = []

    async def reject(method, path, **params):
        methods.append(method)
        raise OrderRejected("definitive rejection")

    adapter.request = reject
    submit = adapter.submit_maker if maker else adapter.submit_market
    with pytest.raises(OrderRejected, match="definitive rejection"):
        await submit("id", "SELL", "SHORT", D(1), **({"price": D(4000)} if maker else {}))
    assert methods == ["POST"]


async def test_maker_requires_price():
    adapter = BinanceTrading(Settings(), None, key="test", secret="secret")
    with pytest.raises(TypeError):
        await adapter.submit_maker("id", "SELL", "SHORT", D(1))


async def test_missing_order_during_cancel_is_unknown_not_zero_fill():
    adapter = BinanceTrading(Settings(), None, key="test", secret="secret")

    async def request(*args, **kwargs):
        raise OrderRejected("order not found")

    adapter.request = request
    with pytest.raises(ExecutionUnknown):
        await adapter.cancel("id")


async def test_signed_request_and_sanitized_transport_error():
    class Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def json(self):
            return {"ok": True}

    class Session:
        def request(self, method, url, **kwargs):
            self.params = kwargs["params"]
            self.headers = kwargs["headers"]
            return Response()

    session = Session()
    adapter = BinanceTrading(Settings(), session, key="test-key", secret="test-secret")
    await adapter.request("GET", "/fapi/v1/order", symbol="XAUUSDT")
    unsigned, signature = session.params.rsplit("&signature=", 1)
    assert signature == hmac.new(b"test-secret", unsigned.encode(), hashlib.sha256).hexdigest()
    assert parse_qs(unsigned)["symbol"] == ["XAUUSDT"]
    assert session.headers["X-MBX-APIKEY"] == "test-key"

    async def broken_json(self):
        raise ValueError("secret-signed-url")

    Response.json = broken_json
    with pytest.raises(ExecutionUnknown) as error:
        await adapter.request("POST", "/fapi/v1/order")
    assert "secret-signed-url" not in str(error.value)


def mt5(monkeypatch):
    class API:
        ACCOUNT_MARGIN_MODE_RETAIL_HEDGING = 2
        ORDER_TYPE_BUY, ORDER_TYPE_SELL = 0, 1
        POSITION_TYPE_BUY = 0
        TRADE_ACTION_DEAL = 1
        ORDER_TIME_GTC = 0
        ORDER_FILLING_FOK = 0
        TRADE_RETCODE_DONE = 10009
        calls = []
        check_code = 0
        result_code = 10009

        def account_info(self):
            return NS(trade_allowed=True, trade_expert=True, margin_mode=2)

        def terminal_info(self):
            return NS(connected=True, trade_allowed=True, tradeapi_disabled=False)

        def symbol_info(self, symbol):
            return NS(filling_mode=1)

        def symbol_info_tick(self, symbol):
            return NS(time_msc=1200, ask=4400, bid=4399)

        def positions_get(self, **kwargs):
            return (NS(ticket=101, symbol="XAUUSD", magic=260911, volume=0.01, type=0),)

        def order_check(self, request):
            return NS(retcode=self.check_code)

        def order_send(self, request):
            self.calls.append(request)
            return NS(retcode=self.result_code, order=101, deal=102, volume=0.01, price=4400)

    async def call(function):
        return function()

    monkeypatch.setattr("arbitrage.execution.live_venues.now_ms", lambda: 1200)
    api = API()
    api.calls = []
    return MT5Trading(NS(api=api, settings=Settings(), _call=call))


async def test_mt5_open_fok_and_close_exact_ticket(monkeypatch):
    adapter = mt5(monkeypatch)
    await adapter.send("pair-tag", True, D("0.01"))
    await adapter.send("pair-tag", False, D("0.01"), ticket=101)
    opening, closing = adapter.api.calls
    assert opening["type_filling"] == adapter.api.ORDER_FILLING_FOK
    assert "position" not in opening
    assert closing["position"] == 101 and closing["type"] == adapter.api.ORDER_TYPE_SELL


async def test_mt5_precheck_rejection_never_sends(monkeypatch):
    adapter = mt5(monkeypatch)
    adapter.api.check_code = 10019
    with pytest.raises(OrderRejected):
        await adapter.send("tag", True, D("0.01"))
    assert adapter.api.calls == []


async def test_mt5_close_mismatch_never_sends(monkeypatch):
    adapter = mt5(monkeypatch)
    with pytest.raises(ExecutionUnknown):
        await adapter.send("tag", False, D("0.02"), ticket=101)
    assert adapter.api.calls == []


async def test_mt5_partial_or_timeout_never_retries(monkeypatch):
    adapter = mt5(monkeypatch)
    adapter.api.result_code = 10010
    with pytest.raises(ExecutionUnknown):
        await adapter.send("tag", True, D("0.01"))
    assert len(adapter.api.calls) == 1
