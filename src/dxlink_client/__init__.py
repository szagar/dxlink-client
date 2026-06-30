"""dxlink-client — a robust async dxLink (dxfeed) WebSocket market-data client.

Broker-agnostic: the connection speaks the dxLink protocol; the quote token (and
endpoint URL) are injected via a `TokenProvider`, configured from env / `.env`.
Works against the dxfeed demo server with zero credentials.

    from dxlink_client import DXLinkConnection, SettingsTokenProvider

    async with DXLinkConnection(SettingsTokenProvider()) as conn:
        await conn.subscribe("Quote", ["AAPL"])
        print(await conn.next_event(timeout=2))

Design / robustness rationale: docs/plans/dxlink-robust-approach.md (contract-resolver).
"""

from dxlink_client.config import DEMO_URL, DXLinkSettings
from dxlink_client.connection import DXLinkConnection
from dxlink_client.models import (
    ErrorEvent,
    GreeksEvent,
    QuoteEvent,
    QuoteToken,
    TradeEvent,
)
from dxlink_client.parser import EventParser
from dxlink_client.tokens import (
    AnonymousTokenProvider,
    SettingsTokenProvider,
    TokenProvider,
)

__version__ = "0.1.0"

__all__ = [
    "DEMO_URL",
    "DXLinkConnection",
    "DXLinkSettings",
    "ErrorEvent",
    "EventParser",
    "GreeksEvent",
    "QuoteEvent",
    "QuoteToken",
    "TradeEvent",
    "TokenProvider",
    "AnonymousTokenProvider",
    "SettingsTokenProvider",
]
