"""Per-pair MT5 tickets and durable incremental hedge intents."""

from decimal import Decimal as D
from uuid import uuid4

from arbitrage.domain.specs import round_step
from arbitrage.execution.live_venues import ExecutionUnknown
from arbitrage.strategy.trade_metrics import fill_price


def legs(t):
    if "mt5_legs" not in t:
        t["mt5_legs"] = []
        if t.get("mt5_ticket"):
            t["mt5_legs"].append(
                dict(
                    ticket=t["mt5_ticket"],
                    identifier=t["mt5_identifier"],
                    lots=t["lots"],
                    closed_lots="0",
                )
            )
    return t["mt5_legs"]


def remaining(t):
    return D(t.get("paired_qty", t["filled_qty"])) - D(t.get("closed_qty", "0"))


def cycle(t):
    return t["active_close"] if t.get("closing_maker") else t


def average(results, volume_key, price_key):
    qty = sum((D(r[volume_key]) for r in results), D(0))
    if not qty or any(fill_price(r, price_key) is None for r in results):
        return "0"
    return str(sum((D(r[volume_key]) * D(r[price_key]) for r in results), D(0)) / qty)


async def advance(e, t, result):
    book = cycle(t)
    filled = D(result["executedQty"])
    old = D(book.get("cumulative_filled", "0"))
    if not filled.is_finite() or filled < old or filled > D(book["quantity"]):
        raise ExecutionUnknown("Non-monotonic or invalid cumulative fill")
    book["cumulative_filled"] = str(filled)
    if not t.get("closing_maker"):
        t["filled_qty"] = str(filled)
        t["binance_open"] = result
    book["result"] = result
    processed = D(book.get("processed_filled", "0"))
    delta = filled - processed
    lots = round_step(
        delta * e.hedge.underlying_per_binance_qty / e.hedge.mt5.contract_size,
        e.hedge.mt5.volume_step,
    )
    if lots < e.hedge.mt5.volume_min or book.get("hedge_rejected"):
        return
    if lots > e.hedge.mt5.volume_max:
        raise ExecutionUnknown("Hedge increment exceeds MT5 maximum")
    quantity = lots * e.hedge.mt5.contract_size / e.hedge.underlying_per_binance_qty
    intent = dict(
        tag="ah" + uuid4().hex[:24], lots=str(lots), quantity=str(quantity), state="INTENT"
    )
    book.setdefault("hedge_intents", []).append(intent)
    t["state"] = "MT5_CLOSE_INTENT" if t.get("closing_maker") else "HEDGE_INTENT"
    await e._save(t, "live_incremental_hedge_intent")
    if t.get("closing_maker"):
        left = lots
        for leg in legs(t):
            available = D(leg["lots"]) - D(leg["closed_lots"])
            amount = min(left, available)
            if not amount:
                continue
            if amount < e.hedge.mt5.volume_min:
                raise ExecutionUnknown("Ticket close increment below minimum")
            part = dict(
                tag="ac" + uuid4().hex[:24], ticket=leg["ticket"], lots=str(amount), state="INTENT"
            )
            intent.setdefault("parts", []).append(part)
            await e._save(t, "live_ticket_close_intent")
            response = await e.mt5.send(part["tag"], not t["sell"], amount, ticket=leg["ticket"])
            positions = await e.mt5.positions()
            matches = [p for p in positions if p["ticket"] == leg["ticket"]]
            expected = available - amount
            if (expected and (len(matches) != 1 or D(matches[0]["volume"]) != expected)) or (
                not expected and matches
            ):
                raise ExecutionUnknown("MT5 incremental close position mismatch")
            part.update(state="DONE", result=response)
            leg["closed_lots"] = str(D(leg["closed_lots"]) + amount)
            t.setdefault("mt5_close_fills", []).append(response)
            delta_qty = amount * e.hedge.mt5.contract_size / e.hedge.underlying_per_binance_qty
            book["processed_filled"] = str(D(book.get("processed_filled", "0")) + delta_qty)
            t["closed_qty"] = str(D(t.get("closed_qty", "0")) + delta_qty)
            t["mt5_close"] = dict(price=average(t["mt5_close_fills"], "volume", "price"))
            left -= amount
            await e._save(t, "live_ticket_closed")
            if not left:
                break
        if left:
            raise ExecutionUnknown("Insufficient owned MT5 tickets")
        t["mt5_close"] = dict(price=average(t["mt5_close_fills"], "volume", "price"))
    else:
        stamp = e.user_updates.first_fill_ns.get(book["open_client_id"])
        if stamp is not None and "hedge_dispatch" not in t.get("timings_ns", {}):
            e._measure_dispatch(intent["tag"], t, "hedge_dispatch", stamp)
        response = await e.mt5.send(intent["tag"], t["sell"], lots)
        matches = [p for p in await e.mt5.positions() if p["ticket"] == response["order"]]
        if len(matches) != 1 or D(matches[0]["volume"]) != lots:
            raise ExecutionUnknown("MT5 incremental hedge position mismatch")
        p = matches[0]
        if p["magic"] != e.settings.live.mt5_magic or p["type"] != (0 if t["sell"] else 1):
            raise ExecutionUnknown("MT5 hedge ticket ownership mismatch")
        if any(leg["ticket"] == p["ticket"] for leg in legs(t)):
            raise ExecutionUnknown("MT5 hedge ticket already processed")
        legs(t).append(
            dict(ticket=p["ticket"], identifier=p["identifier"], lots=str(lots), closed_lots="0")
        )
        t.setdefault("mt5_open_fills", []).append(response)
        t["mt5_open"] = dict(price=average(t["mt5_open_fills"], "volume", "price"))
        t["mt5_ticket"], t["mt5_identifier"] = p["ticket"], p["identifier"]
        t["mt5_hedged_qty"] = str(processed + quantity)
        t["lots"] = str(sum((D(p["lots"]) for p in legs(t)), D(0)))
    book["processed_filled"] = str(processed + quantity)
    intent.update(state="DONE")
    await e._save(t, "live_incremental_hedge_done")
