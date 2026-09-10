import asyncio
import json
from decimal import Decimal, InvalidOperation

import aiohttp

from arbitrage.config import Settings
from arbitrage.domain.quote import Quote
from arbitrage.domain.specs import BinanceSpec
from arbitrage.observability import log_event, now_ms


def parse_exchange_info(data: dict, symbol: str) -> BinanceSpec:
    item = next((s for s in data.get("symbols", []) if s["symbol"] == symbol), None)
    if item is None or item.get("status") != "TRADING":
        raise ValueError(f"Binance symbol unavailable or not trading symbol={symbol}")
    if (
        item.get("contractType") not in {"PERPETUAL", "TRADIFI_PERPETUAL"}
        or item.get("quoteAsset") != "USDT"
    ):
        raise ValueError(f"Expected USDT perpetual contract symbol={symbol}")
    try:
        filters = {f["filterType"]: f for f in item["filters"]}
        price, lot = filters["PRICE_FILTER"], filters["LOT_SIZE"]
        return BinanceSpec(
            symbol,
            Decimal(price["tickSize"]),
            Decimal(lot["stepSize"]),
            Decimal(lot["minQty"]),
            Decimal(lot["maxQty"]),
            Decimal(filters["MIN_NOTIONAL"]["notional"]),
            int(item["quantityPrecision"]),
            int(item["pricePrecision"]),
            Decimal(price["minPrice"]),
            Decimal(price["maxPrice"]),
        )
    except (KeyError, TypeError, InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid Binance exchange filters symbol={symbol}") from exc


def parse_book_ticker(data: dict, symbol: str, local_ts: int) -> Quote:
    event = data.get("data", data)
    if event.get("s") != symbol or event.get("e") != "bookTicker" or event.get("st", 1) != 1:
        raise ValueError(f"Unexpected Binance bookTicker symbol={symbol} received={event.get('s')}")
    try:
        return Quote(
            Decimal(event["b"]),
            Decimal(event["a"]),
            Decimal(event["B"]),
            Decimal(event["A"]),
            int(event["T"]),
            local_ts,
        )
    except (KeyError, TypeError, InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid Binance bookTicker payload symbol={symbol}") from exc


class BinanceMarket:
    """Public GET + market WebSocket only; no signing keys or order endpoint."""

    def __init__(self, settings: Settings, session: aiohttp.ClientSession):
        self.settings = settings
        self.session = session

    async def specification(self) -> BinanceSpec:
        url = self.settings.market.binance_rest_url.rstrip("/") + "/fapi/v1/exchangeInfo"
        try:
            async with self.session.get(
                url, proxy=self.settings.market.binance_proxy_url
            ) as response:
                response.raise_for_status()
                data = await response.json()
            return parse_exchange_info(data, self.settings.symbol.binance)
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise RuntimeError(
                f"Binance exchangeInfo failed symbol={self.settings.symbol.binance}"
            ) from exc

    async def stream(self, queue: asyncio.Queue, stop: asyncio.Event) -> None:
        symbol = self.settings.symbol.binance
        url = self.settings.market.binance_ws_url.rstrip("/") + f"/{symbol.lower()}@bookTicker"
        last_update = -1
        try:
            # Bound the handshake explicitly; ClientTimeout.total does not bound a WS session.
            async with asyncio.timeout(self.settings.market.http_timeout_ms / 1000):
                connection = await self.session.ws_connect(
                    url, heartbeat=20, proxy=self.settings.market.binance_proxy_url
                )
            async with connection as ws:
                log_event("binance_connected", symbol=symbol)
                while not stop.is_set():
                    async with asyncio.timeout(self.settings.market.stream_timeout_ms / 1000):
                        message = await ws.receive()
                    if message.type != aiohttp.WSMsgType.TEXT:
                        raise RuntimeError(
                            f"Binance WebSocket disconnected symbol={symbol} type={message.type}"
                        )
                    local = now_ms()
                    data = json.loads(message.data)
                    event = data.get("data", data)
                    update = int(event["u"])
                    if update <= last_update:
                        log_event(
                            "binance_duplicate_or_out_of_order", symbol=symbol, update_id=update
                        )
                        continue
                    quote = parse_book_ticker(data, symbol, local)
                    last_update = update
                    queue.put_nowait(("binance", quote))
        except (aiohttp.ClientError, TimeoutError, asyncio.QueueFull, KeyError, ValueError) as exc:
            raise RuntimeError(
                f"Binance market stream failed symbol={symbol} cause={type(exc).__name__}"
            ) from exc
