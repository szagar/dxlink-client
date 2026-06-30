"""Live TastyTrade integration tests (gated on TASTYTRADE_* creds in .env).

These confirm the functionality the dxfeed demo can't: the real OAuth2 exchange,
the *authenticated* handshake (UNAUTHORIZED -> AUTH -> AUTHORIZED), and Greeks
`delta` parsing from a real option chain. Skipped automatically when no creds are
configured. Secrets are never printed/asserted.

Run:  uv run pytest -m tastytrade --extra tastytrade
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from dxlink_client import DXLinkConnection, GreeksEvent, QuoteEvent, TradeEvent
from dxlink_client.config import TastytradeSettings
from dxlink_client.providers.tastytrade import TastytradeTokenProvider

pytestmark = pytest.mark.tastytrade

_settings = TastytradeSettings()
_no_creds = not (_settings.client_id and _settings.client_secret and _settings.refresh_token)
skip_no_creds = pytest.mark.skipif(_no_creds, reason="TASTYTRADE_* creds not set in .env")


# --------------------------------------------------------------------------- #
# REST discovery helpers — find real streamer symbols to stream
# --------------------------------------------------------------------------- #
async def _bearer_client(provider: TastytradeTokenProvider) -> httpx.AsyncClient:
    access = await provider._access()
    return httpx.AsyncClient(
        base_url=_settings.api_base,
        headers={"Authorization": f"Bearer {access}"},
        timeout=25.0,
    )


async def _front_future_streamer(http: httpx.AsyncClient, product: str = "ES") -> str | None:
    r = await http.get("/instruments/futures", params={"product-code[]": product})
    r.raise_for_status()
    items = r.json()["data"]["items"]
    items = [i for i in items if i.get("active")] or items
    items.sort(key=lambda i: i.get("expiration-date", "9999-99-99"))
    f = items[0]
    return f.get("streamer-symbol") or f.get("symbol")


async def _future_option_streamer(http: httpx.AsyncClient, product: str = "ES") -> str | None:
    r = await http.get(f"/futures-option-chains/{product}/nested")
    r.raise_for_status()
    chains = r.json()["data"].get("option-chains") or []
    for ch in chains:
        for exp in sorted(ch.get("expirations", []), key=lambda e: e.get("expiration-date", "9")):
            strikes = exp.get("strikes") or []
            if strikes:
                return strikes[len(strikes) // 2].get("call-streamer-symbol")
    return None


async def _collect_price_and_delta(conn: DXLinkConnection, sym: str, *, want_delta: bool, timeout: float):
    await conn.subscribe("Quote", [sym])
    await conn.subscribe("Trade", [sym])
    if want_delta:
        await conn.subscribe("Greeks", [sym])
    price: QuoteEvent | TradeEvent | None = None
    delta: float | None = None
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if price is not None and (delta is not None or not want_delta):
            break
        ev = await conn.next_event(timeout=deadline - asyncio.get_event_loop().time())
        if isinstance(ev, (QuoteEvent, TradeEvent)) and ev.event_symbol == sym and price is None:
            price = ev
        elif isinstance(ev, GreeksEvent) and ev.event_symbol == sym and ev.delta is not None:
            delta = ev.delta
    return price, delta


@skip_no_creds
async def test_oauth_mints_quote_token() -> None:
    provider = TastytradeTokenProvider(_settings)
    try:
        qt = await provider.quote_token()
        # never print/assert the token value itself
        assert qt.url.startswith("wss://")
        assert qt.token and len(qt.token) > 10
    finally:
        await provider.aclose()


@skip_no_creds
async def test_live_authenticated_handshake_streams_quote() -> None:
    # Exercises the AUTH path the demo server skips.
    provider = TastytradeTokenProvider(_settings)
    try:
        async with DXLinkConnection(provider) as conn:
            await conn.subscribe("Quote", ["SPY"])
            got = None
            for _ in range(80):  # up to ~8s
                ev = await conn.next_event(timeout=0.1)
                if isinstance(ev, QuoteEvent) and ev.event_symbol == "SPY":
                    got = ev
                    break
            if got is None:
                pytest.skip("no SPY quote (market closed / feed quiet)")
            assert got.bid_price and got.ask_price
    finally:
        await provider.aclose()


@skip_no_creds
async def test_live_greeks_delta_for_real_option() -> None:
    """The key BrokerSource path: a real option's delta off DXLink."""
    provider = TastytradeTokenProvider(_settings)
    try:
        access = await provider._access()
        async with httpx.AsyncClient(
            base_url=_settings.api_base,
            headers={"Authorization": f"Bearer {access}"},
            timeout=15.0,
        ) as http:
            resp = await http.get("/option-chains/SPY/nested")
            resp.raise_for_status()
            chain = resp.json()["data"]["items"][0]
            exp = chain["expirations"][0]                  # nearest expiration
            strikes = exp["strikes"]
            mid = strikes[len(strikes) // 2]               # ~ATM-ish
            streamer_symbol = mid["call-streamer-symbol"]

        async with DXLinkConnection(provider) as conn:
            greeks = await conn.collect_greeks([streamer_symbol], timeout=8.0)

        if not greeks:
            pytest.skip(f"no greeks streamed for {streamer_symbol} (market closed?)")
        g = greeks[streamer_symbol]
        assert isinstance(g, GreeksEvent)
        assert g.delta is not None and -1.0 <= g.delta <= 1.0   # a call delta
    finally:
        await provider.aclose()


@skip_no_creds
async def test_live_future_price() -> None:
    """A futures contract streams a price (no greeks expected)."""
    provider = TastytradeTokenProvider(_settings)
    try:
        async with await _bearer_client(provider) as http:
            sym = await _front_future_streamer(http, "ES")
        assert sym, "no front ES future streamer symbol from REST"
        async with DXLinkConnection(provider) as conn:
            price, _ = await _collect_price_and_delta(conn, sym, want_delta=False, timeout=10.0)
        if price is None:
            pytest.skip(f"no price for {sym} (market closed?)")
        val = getattr(price, "bid_price", None) or getattr(price, "price", None)
        assert val and val > 0  # a future has a positive price
    finally:
        await provider.aclose()


@skip_no_creds
async def test_live_future_option_price_and_greeks() -> None:
    """The core OF path: an option-on-future streams BOTH a price and a delta."""
    provider = TastytradeTokenProvider(_settings)
    try:
        async with await _bearer_client(provider) as http:
            sym = await _future_option_streamer(http, "ES")
        assert sym, "no ES futures-option streamer symbol from REST"
        async with DXLinkConnection(provider) as conn:
            price, delta = await _collect_price_and_delta(conn, sym, want_delta=True, timeout=12.0)
        if delta is None:
            pytest.skip(f"no greeks for {sym} (market closed?)")
        assert -1.0 <= delta <= 1.0                       # option-on-future delta
        if price is not None:                             # price is best-effort off-hours
            val = getattr(price, "bid_price", None) or getattr(price, "price", None)
            assert val and val > 0
    finally:
        await provider.aclose()
