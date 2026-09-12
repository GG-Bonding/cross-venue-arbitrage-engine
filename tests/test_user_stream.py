import pytest

from arbitrage.execution.user_stream import OrderUpdates


@pytest.mark.parametrize("field,value", [("S", "BUY"), ("q", "2"), ("z", "NaN"), ("z", "2")])
def test_stream_rejects_identity_and_quantity_conflicts(field, value):
    from decimal import Decimal

    updates = OrderUpdates("BTCUSDT")
    updates.watch("owned", quantity=Decimal(1), S="SELL", ps="SHORT")
    payload = event("1", "FILLED")
    payload["o"][field] = value
    with pytest.raises(ValueError):
        updates.feed(payload)
    assert updates.get("owned") is None


def event(qty, status, stamp=1, client="owned"):
    return {
        "e": "ORDER_TRADE_UPDATE",
        "T": stamp,
        "o": {
            "s": "BTCUSDT",
            "c": client,
            "i": 10,
            "X": status,
            "z": qty,
            "ap": "100",
            "ps": "SHORT",
            "S": "SELL",
            "q": "1",
        },
    }


def test_user_updates_ignore_duplicates_out_of_order_and_unowned():
    updates = OrderUpdates("BTCUSDT")
    updates.watch("owned")
    assert updates.feed(event("0.5", "PARTIALLY_FILLED", 2))
    assert not updates.feed(event("0.5", "PARTIALLY_FILLED", 2))
    assert not updates.feed(event("0", "NEW", 1))
    assert not updates.feed(event("1", "FILLED", 3, "external"))
    assert updates.feed(event("1", "FILLED", 3))
    assert not updates.feed(event("1", "NEW", 4))
    assert updates.get("owned")["executedQty"] == "1"
    updates.forget("owned")
    assert updates.get("owned") is None


async def test_private_stream_transport_and_disconnect_use_rest_fallback():
    import asyncio
    from types import SimpleNamespace

    import aiohttp
    from aiohttp import web

    from arbitrage.config import Settings
    from arbitrage.execution.user_stream import run_user_stream

    calls = []
    stop = asyncio.Event()
    updates = OrderUpdates("BTCUSDT")
    updates.watch("owned")

    async def key(request):
        assert request.headers["X-MBX-APIKEY"] == "offline-key"
        calls.append(request.method)
        return web.json_response({"listenKey": "offline-listen-key"})

    async def stream(request):
        assert request.query["listenKey"] == "offline-listen-key"
        assert request.query["events"] == "ORDER_TRADE_UPDATE"
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_json(event("1", "FILLED"))
        await ws.close()
        return ws

    def receive(payload):
        updates.feed(payload)
        stop.set()

    app = web.Application()
    app.router.add_post("/fapi/v1/listenKey", key)
    app.router.add_get("/private/ws", stream)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        address = f"127.0.0.1:{runner.addresses[0][1]}"
        settings = Settings(
            market={
                "binance_rest_url": "http://" + address,
                "binance_ws_url": "ws://" + address + "/public/ws",
            }
        )
        async with aiohttp.ClientSession() as session:
            adapter = SimpleNamespace(settings=settings, session=session, key="offline-key")
            engine = SimpleNamespace(
                user_updates=updates, execution_wakeup=asyncio.Event(), on_user_event=receive
            )
            await asyncio.wait_for(run_user_stream(adapter, engine, stop), 2)
        assert calls == ["POST"]
        assert updates.get("owned")["status"] == "FILLED"
        assert not updates.connected
        assert engine.execution_wakeup.is_set()
    finally:
        await runner.cleanup()
