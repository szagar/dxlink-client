"""Benchmark live DXLink + TT REST latencies across stock / index / futures.

Measures three things, end to end against the live TastyTrade feed:

  * one-time setup   — OAuth token, /api-quote-tokens, WS connect+handshake
  * chain data       — REST nested-chain fetch per underlying
  * streaming        — subscribe -> first event latency for price + greeks
                       (the latency that gates a BrokerSource resolution)

Requires TASTYTRADE_* creds in .env (see .env.example) and the tastytrade extra:

    uv sync --extra tastytrade
    uv run python examples/benchmark_latency.py

Run it from your production host (e.g. zmini1/zmini2) for location-accurate
numbers — latency depends on network distance to TT / dxfeed. Prints ms only;
never tokens.
"""

from __future__ import annotations

import asyncio
import statistics
import time

import httpx

from dxlink_client import DXLinkConnection, GreeksEvent, QuoteEvent, TradeEvent
from dxlink_client.config import TastytradeSettings
from dxlink_client.providers.tastytrade import TastytradeTokenProvider

REPS = 5            # samples per streaming metric
EVENT_TIMEOUT = 6.0  # give up waiting for a first event after this many seconds


def _ms(seconds: float) -> float:
    return round(seconds * 1000, 1)


def _mid(strikes: list) -> dict:
    return strikes[len(strikes) // 2]


async def _timed_get(http: httpx.AsyncClient, path: str, **kw) -> tuple[float, dict]:
    t = time.perf_counter()
    r = await http.get(path, **kw)
    r.raise_for_status()
    return time.perf_counter() - t, r.json()


async def _drain(conn: DXLinkConnection) -> None:
    while await conn.next_event(timeout=0.02) is not None:
        pass


async def _first_event_latency(
    conn: DXLinkConnection, sym: str, etype: str, *, need_delta: bool = False
) -> dict | None:
    """Isolated subscribe -> first-event latency: {min, med, n} in ms, or None."""
    samples: list[float] = []
    for _ in range(REPS):
        await conn.unsubscribe(etype, [sym])
        await asyncio.sleep(0.25)
        await _drain(conn)
        t0 = time.monotonic()
        await conn.subscribe(etype, [sym], force=True)
        while time.monotonic() - t0 < EVENT_TIMEOUT:
            ev = await conn.next_event(timeout=EVENT_TIMEOUT - (time.monotonic() - t0))
            if getattr(ev, "event_symbol", None) != sym:
                continue
            if etype == "Greeks" and isinstance(ev, GreeksEvent):
                if ev.delta is not None or not need_delta:
                    samples.append(time.monotonic() - t0)
                    break
            elif etype == "Quote" and isinstance(ev, QuoteEvent):
                samples.append(time.monotonic() - t0)
                break
            elif etype == "Trade" and isinstance(ev, TradeEvent):
                samples.append(time.monotonic() - t0)
                break
    await conn.unsubscribe(etype, [sym])
    if not samples:
        return None
    return {"min": _ms(min(samples)), "med": _ms(statistics.median(samples)), "n": len(samples)}


async def _discover(http: httpx.AsyncClient) -> dict:
    """Find a representative option streamer symbol + underlying per class, timing
    the chain REST calls."""
    out: dict = {}

    # stock — Quote on the underlying
    d, j = await _timed_get(http, "/option-chains/AAPL/nested")
    opt = _mid(j["data"]["items"][0]["expirations"][0]["strikes"]).get("call-streamer-symbol")
    out["stock(AAPL)"] = {"chain_ms": _ms(d), "ul": ("AAPL", "Quote"), "opt": opt}

    # index — Trade on the underlying (indices have no bid/ask)
    try:
        d, j = await _timed_get(http, "/option-chains/SPX/nested")
        opt = _mid(j["data"]["items"][0]["expirations"][0]["strikes"]).get("call-streamer-symbol")
        out["index(SPX)"] = {"chain_ms": _ms(d), "ul": ("SPX", "Trade"), "opt": opt}
    except httpx.HTTPError as e:
        print(f"  [index SPX chain unavailable: {e}]")

    # futures — front contract + futures-option chain
    d1, jf = await _timed_get(http, "/instruments/futures", params={"product-code[]": "ES"})
    fits = [i for i in jf["data"]["items"] if i.get("active")] or jf["data"]["items"]
    fits.sort(key=lambda i: i.get("expiration-date", "9999-99-99"))
    ul_fut = fits[0].get("streamer-symbol") or fits[0].get("symbol")
    d2, jo = await _timed_get(http, "/futures-option-chains/ES/nested")
    fopt = None
    for ch in jo["data"].get("option-chains", []):
        for e in sorted(ch.get("expirations", []), key=lambda e: e.get("expiration-date", "9")):
            if e.get("strikes"):
                fopt = _mid(e["strikes"]).get("call-streamer-symbol")
                break
        if fopt:
            break
    out["future(ES)"] = {"chain_ms": _ms(d1 + d2), "ul": (ul_fut, "Quote"), "opt": fopt}
    return out


async def main() -> None:
    settings = TastytradeSettings()
    if not (settings.client_id and settings.client_secret and settings.refresh_token):
        raise SystemExit("Set TASTYTRADE_CLIENT_ID/CLIENT_SECRET/REFRESH_TOKEN in .env first.")

    provider = TastytradeTokenProvider(settings)

    t = time.perf_counter()
    access = await provider._access()
    oauth_ms = _ms(time.perf_counter() - t)

    t = time.perf_counter()
    await provider.quote_token()
    aqt_ms = _ms(time.perf_counter() - t)

    conn = DXLinkConnection(provider)
    t = time.perf_counter()
    await conn.connect()
    connect_ms = _ms(time.perf_counter() - t)

    print("=== one-time setup ===")
    print(f"  OAuth /oauth/token     {oauth_ms} ms")
    print(f"  /api-quote-tokens      {aqt_ms} ms")
    print(f"  WS connect + handshake {connect_ms} ms")

    http = httpx.AsyncClient(
        base_url=settings.api_base,
        headers={"Authorization": f"Bearer {access}"},
        timeout=25.0,
    )
    async with http:
        targets = await _discover(http)

    print(f"\n=== latency ms (min/median over up to {REPS} samples) ===")
    print(f"{'underlying':14}{'chain REST':>12}{'ul price':>18}{'opt price':>14}{'opt greek':>14}")
    for cls, t in targets.items():
        ul_sym, ul_et = t["ul"]
        ul = await _first_event_latency(conn, ul_sym, ul_et)
        op = await _first_event_latency(conn, t["opt"], "Quote") if t["opt"] else None
        gk = (
            await _first_event_latency(conn, t["opt"], "Greeks", need_delta=True)
            if t["opt"]
            else None
        )

        def _cell(r: dict | None) -> str:
            return f"{r['min']}/{r['med']}" if r else "—"

        print(
            f"{cls:14}{t['chain_ms']:>12}{_cell(ul) + f' ({ul_et})':>18}"
            f"{_cell(op):>14}{_cell(gk):>14}"
        )

    await conn.close()
    await provider.aclose()


if __name__ == "__main__":
    asyncio.run(main())
