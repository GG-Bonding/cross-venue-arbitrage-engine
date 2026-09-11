"""Real venue adapters. No order retries after an ambiguous response."""

import hashlib
import hmac
import os
from decimal import Decimal
from urllib.parse import urlencode

import aiohttp

from arbitrage.observability import now_ms

D = Decimal


class ExecutionUnknown(RuntimeError):
    """May have reached a venue: reconcile instead of resubmitting."""


class OrderRejected(RuntimeError):
    """A definitive exchange rejection; no execution occurred."""


class BinanceTrading:
    def __init__(self, settings, session, *, key=None, secret=None):
        self.settings, self.session = settings, session
        self.key = key or os.environ["BINANCE_API_KEY"]
        self.secret = (secret or os.environ["BINANCE_API_SECRET"]).encode()
        self.offset = 0

    async def request(self, method, path, **params):
        params.update(timestamp=now_ms() + self.offset, recvWindow=5000)
        query = urlencode(params)
        signature = hmac.new(self.secret, query.encode(), hashlib.sha256).hexdigest()
        url = self.settings.market.binance_rest_url.rstrip("/") + path
        try:
            async with self.session.request(
                method,
                url,
                params=query + "&signature=" + signature,
                headers={"X-MBX-APIKEY": self.key},
                proxy=self.settings.market.binance_proxy_url,
            ) as response:
                data = await response.json()
                if response.status >= 400:
                    code = data.get("code") if isinstance(data, dict) else None
                    # 5xx / timeout responses can conceal an accepted order.
                    if response.status >= 500 or code in {-1006, -1007}:
                        raise ExecutionUnknown(f"Binance uncertain response code={code}")
                    raise OrderRejected(f"Binance rejected code={code}")
                return data
        except (TimeoutError, aiohttp.ClientError, ValueError):
            # Do not include aiohttp URL exceptions: signed URLs contain credentials.
            raise ExecutionUnknown("Binance transport failed; query client order ID") from None

    async def initialize(self):
        url = self.settings.market.binance_rest_url.rstrip("/") + "/fapi/v1/time"
        async with self.session.get(url, proxy=self.settings.market.binance_proxy_url) as res:
            if res.status != 200:
                raise RuntimeError("Binance clock unavailable")
            self.offset = int((await res.json())["serverTime"]) - now_ms()
        mode = await self.request("GET", "/fapi/v1/positionSide/dual")
        if mode.get("dualSidePosition") is not True:
            raise ValueError("实盘要求 Binance 双向持仓模式，请在平台设置后重启")
        account = await self.request("GET", "/fapi/v3/account")
        if not account.get("canTrade"):
            raise ValueError("Binance 账户当前不允许交易")
        mode = await self.request("GET", "/fapi/v1/multiAssetsMargin")
        if mode.get("multiAssetsMargin") is not False:
            raise ValueError("当前实盘实现要求 Binance 单资产保证金模式")

    async def order(self, client_id):
        return await self.request(
            "GET",
            "/fapi/v1/order",
            symbol=self.settings.symbol.binance,
            origClientOrderId=client_id,
        )

    async def identity(self):
        return hashlib.sha256(self.key.encode()).hexdigest()

    async def submit(self, client_id, side, position_side, quantity, *, price=None):
        params = dict(
            symbol=self.settings.symbol.binance,
            side=side,
            positionSide=position_side,
            quantity=str(quantity),
            newClientOrderId=client_id,
            newOrderRespType="RESULT",
        )
        params["type"] = "LIMIT" if price is not None else "MARKET"
        if price is not None:
            params.update(price=str(price), timeInForce="GTX")
        try:
            return await self.request("POST", "/fapi/v1/order", **params)
        except ExecutionUnknown:
            # Exactly one POST. A missing query result is not proof it was rejected.
            try:
                return await self.order(client_id)
            except (ExecutionUnknown, OrderRejected):
                raise ExecutionUnknown(f"Binance order unresolved client_id={client_id}") from None

    async def cancel(self, client_id):
        try:
            await self.request(
                "DELETE",
                "/fapi/v1/order",
                symbol=self.settings.symbol.binance,
                origClientOrderId=client_id,
            )
        except (OrderRejected, ExecutionUnknown):
            pass  # A fill may win the cancel race; query authoritative cumulative fill.
        try:
            result = await self.order(client_id)
        except (OrderRejected, ExecutionUnknown):
            raise ExecutionUnknown(
                f"Binance cancellation unresolved client_id={client_id}"
            ) from None
        if result["status"] not in {
            "FILLED",
            "CANCELED",
            "EXPIRED",
            "EXPIRED_IN_MATCH",
            "REJECTED",
        }:
            raise ExecutionUnknown(f"Binance cancellation unresolved client_id={client_id}")
        return result

    async def inventory(self):
        return await self.request(
            "GET", "/fapi/v3/positionRisk", symbol=self.settings.symbol.binance
        )

    async def open_orders(self):
        return await self.request("GET", "/fapi/v1/openOrders", symbol=self.settings.symbol.binance)

    async def account(self):
        a = await self.request("GET", "/fapi/v3/account")
        return {
            "currency": "USDT",
            "equity": a["totalMarginBalance"],
            "available": a["availableBalance"],
            "profit": a["totalUnrealizedProfit"],
        }

    async def trades(self, order_id):
        rows = await self.request(
            "GET",
            "/fapi/v1/userTrades",
            symbol=self.settings.symbol.binance,
            orderId=order_id,
            limit=1000,
        )
        return [
            {
                k: r[k]
                for k in (
                    "id",
                    "orderId",
                    "qty",
                    "price",
                    "commission",
                    "commissionAsset",
                    "realizedPnl",
                )
            }
            for r in rows
        ]


class MT5Trading:
    def __init__(self, market):
        self.market, self.api, self.settings = market, market.api, market.settings

    async def initialize(self):
        await self.market._call(self._check)

    async def identity(self):
        def read():
            a = self.api.account_info()
            if a is None:
                raise ExecutionUnknown("MT5 identity unavailable")
            return hashlib.sha256(f"{a.server}:{a.login}".encode()).hexdigest()

        return await self.market._call(read)

    def _check(self):
        a, t = self.api.account_info(), self.api.terminal_info()
        if a is None or t is None or not t.connected or not t.trade_allowed or t.tradeapi_disabled:
            raise ValueError("MT5 未连接或终端禁止 Python 交易")
        if not a.trade_allowed or not a.trade_expert:
            raise ValueError("MT5 账户不允许自动交易")
        if a.margin_mode != self.api.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING:
            raise ValueError("MT5 实盘要求对冲账户，不能使用净额账户")
        info = self.api.symbol_info(self.settings.symbol.mt5)
        if info is None or not (info.filling_mode & 1):
            raise ValueError("MT5 品种必须支持 FOK，以避免不可控的部分对冲")

    async def positions(self):
        def read():
            rows = self.api.positions_get(symbol=self.settings.symbol.mt5)
            if rows is None:
                raise ExecutionUnknown("MT5 positions query failed")
            return [
                dict(
                    ticket=p.ticket,
                    identifier=p.identifier,
                    volume=str(p.volume),
                    type=p.type,
                    magic=p.magic,
                    comment=p.comment,
                    price_open=str(p.price_open),
                    profit=str(p.profit),
                    swap=str(p.swap),
                )
                for p in rows
            ]

        return await self.market._call(read)

    async def send(self, tag, buy, lots, *, ticket=None):
        return await self.market._call(lambda: self._send(tag, buy, lots, ticket))

    def _send(self, tag, buy, lots, ticket):
        try:
            self._check()
        except ValueError as exc:
            raise OrderRejected(str(exc)) from None
        tick = self.api.symbol_info_tick(self.settings.symbol.mt5)
        if tick is None:
            raise OrderRejected("MT5 quote unavailable before order submission")
        stamp = int(tick.time_msc) - self.settings.mt5.tick_time_offset_minutes * 60000
        if not 0 <= now_ms() - stamp <= self.settings.market.max_quote_age_ms:
            raise OrderRejected("MT5 quote stale before order submission")
        request = dict(
            action=self.api.TRADE_ACTION_DEAL,
            symbol=self.settings.symbol.mt5,
            volume=float(lots),
            type=self.api.ORDER_TYPE_BUY if buy else self.api.ORDER_TYPE_SELL,
            price=tick.ask if buy else tick.bid,
            deviation=self.settings.live.mt5_deviation_points,
            magic=self.settings.live.mt5_magic,
            comment=tag,
            type_time=self.api.ORDER_TIME_GTC,
            type_filling=self.api.ORDER_FILLING_FOK,
        )
        if ticket is not None:
            positions = self.api.positions_get(ticket=int(ticket))
            if positions is None or len(positions) != 1:
                raise ExecutionUnknown("MT5 close ticket unavailable")
            p = positions[0]
            if (
                p.symbol != self.settings.symbol.mt5
                or p.magic != self.settings.live.mt5_magic
                or D(str(p.volume)) != lots
                or (p.type == self.api.POSITION_TYPE_BUY) == buy
            ):
                raise ExecutionUnknown("MT5 close position mismatch")
            request["position"] = int(ticket)
        check = self.api.order_check(request)
        if check is None or check.retcode != 0:
            raise OrderRejected("MT5 order_check rejected")
        result = self.api.order_send(request)
        if result is None:
            raise ExecutionUnknown("MT5 order_send returned no result; do not retry")
        if result.retcode != self.api.TRADE_RETCODE_DONE:
            # Includes partial fills and timeouts. Never assume zero exposure.
            raise ExecutionUnknown(
                f"MT5 execution requires reconciliation retcode={result.retcode}"
            )
        if D(str(result.volume)) != lots:
            raise ExecutionUnknown("MT5 executed volume differs from requested volume")
        return dict(
            order=result.order, deal=result.deal, volume=str(result.volume), price=str(result.price)
        )

    async def account(self):
        def read():
            a = self.api.account_info()
            if a is None:
                raise ExecutionUnknown("MT5 account query failed")
            return dict(
                currency=a.currency,
                equity=str(a.equity),
                available=str(a.margin_free),
                profit=str(a.profit),
            )

        return await self.market._call(read)

    async def deals(self, identifier):
        def read():
            rows = self.api.history_deals_get(position=int(identifier))
            if rows is None:
                raise ExecutionUnknown("MT5 deal history unavailable")
            return [
                dict(
                    ticket=d.ticket,
                    volume=str(d.volume),
                    entry=d.entry,
                    profit=str(d.profit),
                    commission=str(d.commission),
                    swap=str(d.swap),
                    fee=str(d.fee),
                )
                for d in rows
            ]

        return await self.market._call(read)
