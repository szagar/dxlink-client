"""Token providers — how a `QuoteToken` is obtained, injected into the client.

The core client is broker-agnostic: it never logs into TastyTrade. A caller
supplies a `TokenProvider`. Two are built in:

* `AnonymousTokenProvider` — the demo server (no token).
* `SettingsTokenProvider` — reads URL/token from env / `.env`.

A TastyTrade provider (mint a token from a login via `/api-quote-tokens`) is a
deliberate non-goal of the core package — implement it where the broker client
lives and pass it in.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from dxlink_client.config import DEMO_URL, DXLinkSettings
from dxlink_client.models import QuoteToken


@runtime_checkable
class TokenProvider(Protocol):
    async def quote_token(self) -> QuoteToken: ...


class AnonymousTokenProvider:
    """Demo / anonymous access — no token."""

    def __init__(self, url: str = DEMO_URL) -> None:
        self._url = url

    async def quote_token(self) -> QuoteToken:
        return QuoteToken(url=self._url, token=None)


class SettingsTokenProvider:
    """Read the URL + (optional) token from environment / `.env`."""

    def __init__(self, settings: DXLinkSettings | None = None) -> None:
        self._settings = settings or DXLinkSettings()

    async def quote_token(self) -> QuoteToken:
        token = self._settings.token or None  # treat blank as anonymous
        return QuoteToken(url=self._settings.url, token=token)
