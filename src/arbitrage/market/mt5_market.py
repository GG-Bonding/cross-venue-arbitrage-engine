import asyncio
import importlib
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from functools import partial

from arbitrage.config import Settings
from arbitrage.domain.quote import Quote
from arbitrage.domain.specs import MT5Spec
from arbitrage.observability import log_event, now_ms


class MT5Market:
    """Every terminal call executes on one dedicated worker, including shutdown."""

    def __init__(self, settings: Settings, *, api=None, clock=now_ms):
        self.settings = settings
        self.api = api
        self.clock = clock
        self.worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mt5-market")
        self.spec: MT5Spec | None = None
        self.last_key: tuple | None = None

    async def _call(self, function, *args):
        return await asyncio.get_running_loop().run_in_executor(
            self.worker, partial(function, *args)
        )

    async def __aenter__(self):
        try:
            if self.api is None:
                try:
                    self.api = importlib.import_module("MetaTrader5")
                except ImportError as exc:
                    raise RuntimeError(
                        "MT5 package unavailable; install .[mt5] on Windows or use --demo"
                    ) from exc
            self.spec = await self._call(self._initialize)
        except BaseException:
            await self.close()
            raise
        return self

    def _initialize(self) -> MT5Spec:
        config = self.settings.mt5
        kwargs = {"timeout": config.initialize_timeout_ms}
        if config.terminal_path:
            kwargs["path"] = config.terminal_path
        if not self.api.initialize(**kwargs):
            raise RuntimeError(f"MT5 initialize failed error={self.api.last_error()}")
        symbol = self.settings.symbol.mt5
        if not self.api.symbol_select(symbol, True):
            raise RuntimeError(
                f"MT5 symbol_select failed symbol={symbol} error={self.api.last_error()}"
            )
        info = self.api.symbol_info(symbol)
        if info is None:
            raise RuntimeError(
                f"MT5 symbol_info failed symbol={symbol} error={self.api.last_error()}"
            )
        return MT5Spec(
            symbol,
            Decimal(str(info.trade_contract_size)),
            Decimal(str(info.volume_min)),
            Decimal(str(info.volume_max)),
            Decimal(str(info.volume_step)),
        )

    def _read(self) -> Quote | None:
        terminal = self.api.terminal_info()
        if terminal is None or not terminal.connected:
            raise RuntimeError(f"MT5 disconnected symbol={self.settings.symbol.mt5}")
        tick = self.api.symbol_info_tick(self.settings.symbol.mt5)
        if tick is None:
            raise RuntimeError(
                f"MT5 tick failed symbol={self.settings.symbol.mt5} error={self.api.last_error()}"
            )
        key = (tick.time_msc, tick.bid, tick.ask)
        if self.last_key is not None and (key == self.last_key or key[0] < self.last_key[0]):
            return None
        q = Quote(
            Decimal(str(tick.bid)),
            Decimal(str(tick.ask)),
            None,
            None,
            int(tick.time_msc),
            self.clock(),
        )
        self.last_key = key
        return q

    async def read_quote(self) -> Quote | None:
        return await self._call(self._read)

    async def stream(self, queue: asyncio.Queue, stop: asyncio.Event) -> None:
        log_event("mt5_connected", symbol=self.settings.symbol.mt5)
        while not stop.is_set():
            quote = await self.read_quote()
            if quote is not None:
                try:
                    queue.put_nowait(("mt5", quote))
                except asyncio.QueueFull as exc:
                    raise RuntimeError("MT5 quote queue overflow; stopping strategy") from exc
            try:
                await asyncio.wait_for(stop.wait(), self.settings.market.mt5_poll_ms / 1000)
            except TimeoutError:
                pass  # The configured polling deadline elapsed; not an API error.

    async def close(self) -> None:
        try:
            if self.api is not None:
                await self._call(self.api.shutdown)
        finally:
            await asyncio.to_thread(self.worker.shutdown, wait=True, cancel_futures=True)

    async def __aexit__(self, *_):
        await self.close()
