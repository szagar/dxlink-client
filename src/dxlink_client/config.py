"""Settings loaded from environment / `.env` (pydantic-settings).

Endpoint URL, quote token, and keepalive cadence come from the environment so
no credentials are ever hard-coded. Defaults target the dxfeed DEMO server, so a
fresh checkout streams with zero configuration.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict

DEMO_URL = "wss://demo.dxfeed.com/market-data/dxlink-ws"


class DXLinkSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DXLINK_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    url: str = DEMO_URL
    """dxLink WebSocket URL. Demo by default; the live TT URL comes from
    `/api-quote-tokens` (`dxlink-url`)."""

    token: str | None = None
    """Quote token. ``None``/blank → anonymous (demo). Live TT feed needs the
    `/api-quote-tokens` `token`."""

    keepalive_interval_s: int = 30
    """How often to send KEEPALIVE (server timeout is 60s)."""

    connect_timeout_s: float = 10.0
    """Handshake deadline (connect → first FEED_CONFIG)."""


class TastytradeSettings(BaseSettings):
    """OAuth2 credentials for `TastytradeTokenProvider` (env prefix TASTYTRADE_).

    These mint the DXLink token at runtime; the core client never reads them.
    """

    model_config = SettingsConfigDict(
        env_prefix="TASTYTRADE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    client_id: str | None = None
    client_secret: str | None = None
    refresh_token: str | None = None
    api_base: str = "https://api.tastytrade.com"
