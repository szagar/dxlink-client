"""Token-provider behavior (offline)."""

from __future__ import annotations

from dxlink_client import AnonymousTokenProvider, DXLinkSettings, SettingsTokenProvider


async def test_anonymous_provider_has_no_token() -> None:
    qt = await AnonymousTokenProvider("wss://demo.example/ws").quote_token()
    assert qt.url == "wss://demo.example/ws" and qt.token is None


async def test_settings_provider_passes_token_through() -> None:
    qt = await SettingsTokenProvider(
        DXLinkSettings(url="wss://tt.example/dxlink", token="DXTOK")
    ).quote_token()
    assert qt.url == "wss://tt.example/dxlink" and qt.token == "DXTOK"


async def test_settings_provider_blank_token_is_anonymous() -> None:
    # blank DXLINK_TOKEN must become None (anonymous), not the empty string
    qt = await SettingsTokenProvider(DXLinkSettings(token="")).quote_token()
    assert qt.token is None
