"""Offline test of the OAuth2 -> quote-token exchange (mocked httpx transport)."""

from __future__ import annotations

import httpx
import pytest

from dxlink_client.config import TastytradeSettings
from dxlink_client.providers.tastytrade import TastytradeTokenProvider

API = "https://api.tastytrade.com"


def _settings() -> TastytradeSettings:
    return TastytradeSettings(
        client_id="cid", client_secret="csec", refresh_token="rtok", api_base=API
    )


def _transport(calls: list[httpx.Request]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/oauth/token":
            return httpx.Response(200, json={"access_token": "AT123", "expires_in": 900})
        if request.url.path == "/api-quote-tokens":
            return httpx.Response(
                200,
                json={"data": {"token": "DXTOK", "dxlink-url": "wss://tt.example/dxlink", "level": "api"}},
            )
        return httpx.Response(404)

    return httpx.MockTransport(handler)


async def test_exchange_yields_quote_token() -> None:
    calls: list[httpx.Request] = []
    client = httpx.AsyncClient(transport=_transport(calls))
    provider = TastytradeTokenProvider(_settings(), http_client=client)

    qt = await provider.quote_token()

    assert qt.url == "wss://tt.example/dxlink"
    assert qt.token == "DXTOK"

    # 1) OAuth refresh_token grant with the creds; 2) Bearer access token to quote-tokens
    oauth, quote = calls
    assert oauth.url.path == "/oauth/token"
    body = oauth.content.decode()
    assert "grant_type=refresh_token" in body and "refresh_token=rtok" in body
    assert "client_id=cid" in body and "client_secret=csec" in body
    assert quote.headers["Authorization"] == "Bearer AT123"


async def test_access_token_is_cached_across_calls() -> None:
    calls: list[httpx.Request] = []
    client = httpx.AsyncClient(transport=_transport(calls))
    provider = TastytradeTokenProvider(_settings(), http_client=client)

    await provider.quote_token()
    await provider.quote_token()

    # one /oauth/token (cached), two /api-quote-tokens
    paths = [c.url.path for c in calls]
    assert paths.count("/oauth/token") == 1
    assert paths.count("/api-quote-tokens") == 2


def test_missing_credentials_raise() -> None:
    with pytest.raises(ValueError, match="TASTYTRADE_CLIENT_ID"):
        TastytradeTokenProvider(TastytradeSettings(api_base=API))
