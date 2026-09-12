"""Binance order updates; only explicitly watched client IDs are retained."""

import asyncio
import json
from decimal import Decimal
from time import monotonic_ns
from urllib.parse import urlencode, urlsplit, urlunsplit

import aiohttp

from arbitrage.observability import log_event

END = {"FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"}


class OrderUpdates:
    def __init__(self, symbol):
        self.symbol = symbol
        self.orders = {}
        self.expected = {}
        self.quantities = {}
        self.first_fill_ns = {}
        self.connected = False

    def watch(self, client_id, *, quantity=None, **expected):
        self.orders[client_id] = None
        self.expected[client_id] = expected
        self.quantities[client_id] = quantity

    def forget(self, client_id):
        self.orders.pop(client_id, None)
        self.expected.pop(client_id, None)
        self.quantities.pop(client_id, None)
        self.first_fill_ns.pop(client_id, None)

    def get(self, client_id):
        return self.orders.get(client_id)

    def feed(self, message):
        message = message.get("data", message)
        if message.get("e") != "ORDER_TRADE_UPDATE":
            return False
        o = message["o"]
        client = o["c"]
        if client not in self.orders or o["s"] != self.symbol:
            return False
        if any(o.get(k) != v for k, v in self.expected[client].items()):
            self.orders[client] = None
            raise ValueError("User stream order identity mismatch")
        qty = Decimal(o["z"])
        total = Decimal(o["q"])
        if (
            not total.is_finite()
            or total <= 0
            or not qty.is_finite()
            or qty < 0
            or qty > total
            or (self.quantities[client] is not None and total != self.quantities[client])
            or o["X"] not in END | {"NEW", "PARTIALLY_FILLED"}
            or (o["X"] == "FILLED" and qty != total)
        ):
            self.orders[client] = None
            raise ValueError("Invalid user stream cumulative quantity")
        result = dict(
            status=o["X"],
            executedQty=o["z"],
            orderId=o["i"],
            avgPrice=o["ap"],
            symbol=o["s"],
            clientOrderId=client,
            positionSide=o["ps"],
            side=o["S"],
            updateTime=int(message["T"]),
        )
        old = self.orders[client]
        if old:
            if qty < Decimal(old["executedQty"]) or result["updateTime"] < old["updateTime"]:
                return False
            if old["status"] in END:
                if qty > Decimal(old["executedQty"]):
                    self.orders[client] = None
                    raise ValueError("Conflicting terminal order event")
                return False
            if qty == Decimal(old["executedQty"]) and result["status"] == old["status"]:
                return False
        self.orders[client] = result
        if qty > 0:
            self.first_fill_ns.setdefault(client, monotonic_ns())
        return True


async def run_user_stream(adapter, engine, stop):
    settings, session = adapter.settings, adapter.session
    url = settings.market.binance_rest_url.rstrip("/") + "/fapi/v1/listenKey"

    async def listen_key(method):
        async with session.request(
            method,
            url,
            headers={"X-MBX-APIKEY": adapter.key},
            proxy=settings.market.binance_proxy_url,
        ) as response:
            if response.status != 200:
                raise RuntimeError("User stream listen key unavailable")
            return await response.json()

    async def keepalive():
        while True:
            await asyncio.sleep(1800)
            await listen_key("PUT")

    while not stop.is_set():
        try:
            key = (await listen_key("POST"))["listenKey"]
            parts = urlsplit(settings.market.binance_ws_url)
            ws_url = urlunsplit(
                (
                    parts.scheme,
                    parts.netloc,
                    "/private/ws",
                    urlencode(
                        {
                            "listenKey": key,
                            "events": "ORDER_TRADE_UPDATE",
                        }
                    ),
                    "",
                )
            )
            async with asyncio.timeout(settings.market.http_timeout_ms / 1000):
                connection = await session.ws_connect(
                    ws_url, heartbeat=20, proxy=settings.market.binance_proxy_url
                )
            async with connection as ws, asyncio.TaskGroup() as group:
                renewal = group.create_task(keepalive())
                engine.user_updates.connected = True
                engine.execution_wakeup.set()
                async for message in ws:
                    if message.type != aiohttp.WSMsgType.TEXT:
                        break
                    payload = json.loads(message.data)
                    if payload.get("e") == "listenKeyExpired":
                        break
                    engine.on_user_event(payload)
                renewal.cancel()
        except Exception as exc:
            # Never log URLs or listen keys, including nested transport exceptions.
            log_event("user_stream_disconnected", cause=type(exc).__name__)
        finally:
            engine.user_updates.connected = False
            engine.execution_wakeup.set()
        try:
            await asyncio.wait_for(stop.wait(), 1)
        except TimeoutError:
            pass
