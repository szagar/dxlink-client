"""TastyTrade OAuth2 → DXLink quote-token provider.

Turns ``client_id`` + ``client_secret`` + ``refresh_token`` into a live
`QuoteToken` via the two-step exchange:

    POST {api_base}/oauth/token   grant_type=refresh_token  -> access_token (~15m)
    GET  {api_base}/api-quote-tokens  Bearer access_token   -> {token, dxlink-url}

The access token is cached for its lifetime and refreshed on demand; the refresh
token never expires. Requires the ``dxlink-client[tastytrade]`` extra (httpx).
"""

from __future__ import annotations

import time

import httpx

from dxlink_client.config import TastytradeSettings
from dxlink_client.models import QuoteToken

_ACCESS_TOKEN_SLACK_S = 30  # refresh this many seconds before expiry


class TastytradeTokenProvider:
    def __init__(
        self,
        settings: TastytradeSettings | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._s = settings or TastytradeSettings()
        missing = [
            name
            for name in ("client_id", "client_secret", "refresh_token")
            if not getattr(self._s, name)
        ]
        if missing:
            raise ValueError(
                "TastytradeTokenProvider needs "
                + ", ".join(f"TASTYTRADE_{m.upper()}" for m in missing)
                + " (set them in .env)"
            )
        self._client = http_client
        self._owns_client = http_client is None
        self._access_token: str | None = None
        self._access_expiry: float = 0.0

    async def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=15.0)
        return self._client

    async def _access(self) -> str:
        if self._access_token and time.monotonic() < self._access_expiry - _ACCESS_TOKEN_SLACK_S:
            return self._access_token
        client = await self._http()
        resp = await client.post(
            f"{self._s.api_base}/oauth/token",
            data={
                "grant_type": "refresh_token",
                "client_id": self._s.client_id,
                "client_secret": self._s.client_secret,
                "refresh_token": self._s.refresh_token,
            },
        )
        resp.raise_for_status()
        body = resp.json()
        self._access_token = body["access_token"]
        self._access_expiry = time.monotonic() + float(body.get("expires_in", 900))
        return self._access_token

    async def quote_token(self) -> QuoteToken:
        access = await self._access()
        client = await self._http()
        resp = await client.get(
            f"{self._s.api_base}/api-quote-tokens",
            headers={"Authorization": f"Bearer {access}"},
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        return QuoteToken(url=data["dxlink-url"], token=data["token"], expires_at=None)

    async def aclose(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None
