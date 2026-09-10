import asyncio
from contextlib import asynccontextmanager

import aiohttp
import pytest
from aiohttp import web
from test_market import exchange_info

from arbitrage.config import Settings
from arbitrage.market.binance_market import BinanceMarket


@asynccontextmanager
async def public_server():
    app = web.Application()
    requests = []

    async def info(request):
        requests.append((request.method, request.path))
        return web.json_response(exchange_info())

    async def stream(request):
        requests.append((request.method, request.path))
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        event = {
            "e": "bookTicker",
            "s": "XAUUSDT",
            "b": "4416.90",
            "a": "4416.98",
            "B": "1",
            "A": "2",
            "T": 1000,
            "u": 123,
        }
        await ws.send_json(event)
        await ws.send_json(event)  # Duplicate update must not be enqueued again.
        await ws.close()
        return ws

    app.router.add_get("/fapi/v1/exchangeInfo", info)
    app.router.add_get("/public/ws/xauusdt@bookTicker", stream)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        yield f"127.0.0.1:{port}", requests
    finally:
        await runner.cleanup()


async def test_public_http_websocket_duplicate_and_disconnect():
    async with public_server() as (address, requests):
        settings = Settings.model_validate(
            {
                "market": {
                    "binance_rest_url": f"http://{address}",
                    "binance_ws_url": f"ws://{address}/public/ws",
                }
            }
        )
        async with aiohttp.ClientSession() as session:
            market = BinanceMarket(settings, session)
            assert (await market.specification()).symbol == "XAUUSDT"
            queue = asyncio.Queue(maxsize=2)
            with pytest.raises(RuntimeError, match="disconnected"):
                await market.stream(queue, asyncio.Event())
            assert queue.qsize() == 1
            assert requests == [
                ("GET", "/fapi/v1/exchangeInfo"),
                ("GET", "/public/ws/xauusdt@bookTicker"),
            ]


async def test_websocket_handshake_has_a_deadline():
    class StalledSession:
        async def ws_connect(self, *args, **kwargs):
            await asyncio.Event().wait()

    settings = Settings.model_validate({"market": {"http_timeout_ms": 5}})
    market = BinanceMarket(settings, StalledSession())
    with pytest.raises(RuntimeError, match="cause=TimeoutError"):
        async with asyncio.timeout(1):
            await market.stream(asyncio.Queue(), asyncio.Event())


async def test_configured_proxy_is_used_for_rest_and_websocket(monkeypatch):
    async with public_server() as (address, _):
        settings = Settings.model_validate(
            {
                "market": {
                    "binance_rest_url": f"http://{address}",
                    "binance_ws_url": f"ws://{address}/public/ws",
                    "binance_proxy_url": "http://127.0.0.1:7890",
                }
            }
        )
        async with aiohttp.ClientSession() as session:
            seen = []
            original_get, original_ws = session.get, session.ws_connect

            def get(url, **kwargs):
                seen.append(("REST", kwargs.pop("proxy")))
                return original_get(url, **kwargs)

            def ws_connect(url, **kwargs):
                seen.append(("WS", kwargs.pop("proxy")))
                return original_ws(url, **kwargs)

            monkeypatch.setattr(session, "get", get)
            monkeypatch.setattr(session, "ws_connect", ws_connect)
            market = BinanceMarket(settings, session)
            await market.specification()
            with pytest.raises(RuntimeError, match="disconnected"):
                await market.stream(asyncio.Queue(), asyncio.Event())
            assert seen == [("REST", "http://127.0.0.1:7890"), ("WS", "http://127.0.0.1:7890")]
