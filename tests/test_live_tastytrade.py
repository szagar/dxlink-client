"""Live TastyTrade integration tests (gated on TASTYTRADE_* creds in .env).

These confirm the functionality the dxfeed demo can't: the real OAuth2 exchange,
the *authenticated* handshake (UNAUTHORIZED -> AUTH -> AUTHORIZED), and Greeks
`delta` parsing from a real option chain. Skipped automatically when no creds are
configured. Secrets are never printed/asserted.

Run:  uv run pytest -m tastytrade --extra tastytrade
"""

from __future__ import annotations

import httpx
import pytest

from dxlink_client import DXLinkConnection, GreeksEvent, QuoteEvent
from dxlink_client.config import TastytradeSettings
from dxlink_client.providers.tastytrade import TastytradeTokenProvider

pytestmark = pytest.mark.tastytrade

_settings = TastytradeSettings()
_no_creds = not (_settings.client_id and _settings.client_secret and _settings.refresh_token)
skip_no_creds = pytest.mark.skipif(_no_creds, reason="TASTYTRADE_* creds not set in .env")


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
