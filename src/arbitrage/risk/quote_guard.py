from arbitrage.domain.quote import Quote
from arbitrage.observability import Metrics


class QuoteGuard:
    def __init__(self, max_age_ms: int, max_skew_ms: int, metrics: Metrics | None = None):
        self.max_age_ms = max_age_ms
        self.max_skew_ms = max_skew_ms
        self.metrics = metrics if metrics is not None else Metrics()

    def check(self, binance: Quote | None, mt5: Quote | None, now: int) -> str | None:
        reason = None
        if binance is None or mt5 is None:
            reason = "quote_missing"
        else:
            for venue, q in (("binance", binance), ("mt5", mt5)):
                self.metrics.gauges[f"{venue}_quote_latency_ms"] = q.local_ts_ms - q.exchange_ts_ms
            skew = abs(binance.exchange_ts_ms - mt5.exchange_ts_ms)
            self.metrics.gauges["quote_skew_ms"] = skew
            times = [q.local_ts_ms for q in (binance, mt5)]
            times += [q.exchange_ts_ms for q in (binance, mt5)]
            if any(now < ts for ts in times):
                reason = "quote_future"
            elif any(now - ts > self.max_age_ms for ts in times):
                reason = "quote_stale"
            elif skew > self.max_skew_ms:
                reason = "quote_skew"
        if reason:
            self.metrics.counters[reason + "_total"] += 1
        return reason
