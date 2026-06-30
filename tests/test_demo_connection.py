"""Live integration test against the dxfeed demo server (no credentials).

Marked `network` — skip in offline CI with `-m "not network"`. Validates the
reactive handshake (F2) and eventFields-driven parse (F1) end-to-end: a real
Quote for AAPL must arrive within a few seconds.
"""

from __future__ import annotations

import pytest

from dxlink_client import AnonymousTokenProvider, DXLinkConnection, QuoteEvent


@pytest.mark.network
async def test_demo_handshake_and_quote() -> None:
    async with DXLinkConnection(AnonymousTokenProvider()) as conn:
        await conn.subscribe("Quote", ["AAPL"])
        got = None
        for _ in range(50):  # up to ~5s
            ev = await conn.next_event(timeout=0.1)
            if isinstance(ev, QuoteEvent) and ev.event_symbol == "AAPL":
                got = ev
                break
        assert got is not None, "no AAPL Quote from the demo server"
        # bid/ask should be sane positive numbers parsed from COMPACT data
        assert got.bid_price and got.ask_price and got.ask_price >= got.bid_price
