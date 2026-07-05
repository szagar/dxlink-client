"""Offline tests for the connect-time hardening: typed AUTH errors + backoff.

Covers the client capability behind Fixes 1+2 of the MDS reconnect-hardening
plan (zts-massive docs/plans/mds-dxlink-reconnect-hardening.md):

* an ERROR frame during the handshake raises a typed `DXLinkAuthError`
  (classified retryable vs fatal) instead of an untyped timeout;
* `connect_with_backoff` retries transient errors with bounded exponential
  backoff, short-circuits fatal ones, and re-raises on exhaustion;
* `DXLinkConnection.connect_with_retry` recovers from an AUTH ERROR at
  startup and succeeds on the Nth attempt.

No network: `websockets.connect` is monkeypatched with a scripted fake WS.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from dxlink_client import (
    DXLinkAuthError,
    DXLinkConnection,
    QuoteToken,
    connect_with_backoff,
    is_retryable_error_type,
)
from dxlink_client import connection as connection_mod
from dxlink_client import retry as retry_mod


# ---- classification ---------------------------------------------------------
def test_error_type_classification() -> None:
    assert is_retryable_error_type("TIMEOUT")
    assert is_retryable_error_type("UNAUTHORIZED")  # transient under burst re-auth
    assert is_retryable_error_type("UNKNOWN")
    assert is_retryable_error_type(None)
    assert is_retryable_error_type("SOME_FUTURE_TYPE")  # unrecognized -> retryable
    assert not is_retryable_error_type("UNSUPPORTED_PROTOCOL")
    assert not is_retryable_error_type("INVALID_MESSAGE")
    assert not is_retryable_error_type("BAD_ACTION")


def test_auth_error_carries_type_and_retryability() -> None:
    err = DXLinkAuthError("unauthorized", "burst re-auth rejected")
    assert err.error_type == "UNAUTHORIZED"
    assert err.retryable
    assert isinstance(err, ConnectionError)  # existing except-clauses still catch it
    assert "UNAUTHORIZED" in str(err) and "burst re-auth rejected" in str(err)
    assert not DXLinkAuthError("UNSUPPORTED_PROTOCOL").retryable


# ---- connect_with_backoff ---------------------------------------------------
@pytest.fixture()
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    slept: list[float] = []

    async def _fast(delay: float) -> None:
        slept.append(delay)

    monkeypatch.setattr(retry_mod.asyncio, "sleep", _fast)
    return slept


async def test_backoff_retries_transient_then_succeeds(_no_sleep: list[float]) -> None:
    calls = 0

    async def connect() -> str:
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise DXLinkAuthError("TIMEOUT")
        return "session"

    assert await connect_with_backoff(connect, attempts=4) == "session"
    assert calls == 3


async def test_backoff_delays_are_exponential_and_capped(
    _no_sleep: list[float],
) -> None:
    async def connect() -> None:
        raise DXLinkAuthError("UNAUTHORIZED")

    with pytest.raises(DXLinkAuthError):
        await connect_with_backoff(
            connect, attempts=5, initial_delay_s=1.0, max_delay_s=4.0
        )
    assert _no_sleep == [1.0, 2.0, 4.0, 4.0]  # no sleep after the final attempt


async def test_backoff_fatal_error_short_circuits(_no_sleep: list[float]) -> None:
    calls = 0

    async def connect() -> None:
        nonlocal calls
        calls += 1
        raise DXLinkAuthError("UNSUPPORTED_PROTOCOL")

    with pytest.raises(DXLinkAuthError) as ei:
        await connect_with_backoff(connect, attempts=5)
    assert calls == 1 and not ei.value.retryable
    assert _no_sleep == []


async def test_backoff_exhaustion_reraises_last_error(_no_sleep: list[float]) -> None:
    calls = 0

    async def connect() -> None:
        nonlocal calls
        calls += 1
        raise ConnectionError("dial failed")  # OSError subclass -> default retry_on

    with pytest.raises(ConnectionError, match="dial failed"):
        await connect_with_backoff(connect, attempts=3)
    assert calls == 3


async def test_backoff_unclassified_error_propagates_immediately(
    _no_sleep: list[float],
) -> None:
    async def connect() -> None:
        raise ValueError("bug, not weather")

    with pytest.raises(ValueError):
        await connect_with_backoff(connect, attempts=5)
    assert _no_sleep == []


async def test_backoff_on_attempt_failed_callback(_no_sleep: list[float]) -> None:
    seen: list[tuple[int, int, str, float | None]] = []

    async def connect() -> None:
        raise DXLinkAuthError("TIMEOUT")

    with pytest.raises(DXLinkAuthError):
        await connect_with_backoff(
            connect,
            attempts=3,
            initial_delay_s=1.0,
            on_attempt_failed=lambda a, n, exc, d: seen.append(
                (a, n, type(exc).__name__, d)
            ),
        )
    # Every transient failure reported; the final one carries delay=None.
    assert seen == [
        (1, 3, "DXLinkAuthError", 1.0),
        (2, 3, "DXLinkAuthError", 2.0),
        (3, 3, "DXLinkAuthError", None),
    ]


# ---- DXLinkConnection: typed handshake failure ------------------------------
class _ScriptedWS:
    """Feeds pre-scripted frames to the receive loop; records what was sent."""

    def __init__(self, frames: list[dict]) -> None:
        self._frames = list(frames)
        self.sent: list[dict] = []
        self._closed = asyncio.Event()

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))

    async def close(self) -> None:
        self._closed.set()

    def __aiter__(self) -> "_ScriptedWS":
        return self

    async def __anext__(self) -> str:
        if self._frames:
            return json.dumps(self._frames.pop(0))
        await self._closed.wait()
        raise StopAsyncIteration


class _StubProvider:
    async def quote_token(self) -> QuoteToken:
        return QuoteToken(url="wss://tt/ws", token="T")


_HAPPY_HANDSHAKE = [
    {"type": "SETUP", "channel": 0},
    {"type": "AUTH_STATE", "channel": 0, "state": "UNAUTHORIZED"},
    {"type": "AUTH_STATE", "channel": 0, "state": "AUTHORIZED"},
    {"type": "CHANNEL_OPENED", "channel": 1},
    {"type": "FEED_CONFIG", "channel": 1},
]


def _patch_ws(
    monkeypatch: pytest.MonkeyPatch, scripts: list[list[dict]]
) -> list[_ScriptedWS]:
    """Each websockets.connect call consumes the next frame script."""
    made: list[_ScriptedWS] = []

    async def fake_connect(*_a: object, **_k: object) -> _ScriptedWS:
        ws = _ScriptedWS(scripts.pop(0))
        made.append(ws)
        return ws

    monkeypatch.setattr(connection_mod.websockets, "connect", fake_connect)
    return made


async def test_connect_raises_typed_auth_error_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The server answers SETUP with an ERROR frame — connect() must raise the
    # typed error promptly, not wait out the full connect timeout.
    _patch_ws(
        monkeypatch,
        [
            [
                {
                    "type": "ERROR",
                    "channel": 0,
                    "error": "UNAUTHORIZED",
                    "message": "auth rejected",
                }
            ]
        ],
    )
    conn = DXLinkConnection(_StubProvider(), connect_timeout_s=30.0)
    with pytest.raises(DXLinkAuthError) as ei:
        await asyncio.wait_for(conn.connect(), timeout=2.0)  # << connect_timeout_s
    assert ei.value.error_type == "UNAUTHORIZED" and ei.value.retryable
    assert conn._ws is None  # half-open socket abandoned cleanly


async def test_connect_with_retry_recovers_on_nth_attempt(
    monkeypatch: pytest.MonkeyPatch, _no_sleep: list[float]
) -> None:
    # AUTH ERROR at startup on the first two dials, clean handshake on the third.
    made = _patch_ws(
        monkeypatch,
        [
            [{"type": "ERROR", "channel": 0, "error": "UNAUTHORIZED"}],
            [{"type": "ERROR", "channel": 0, "error": "TIMEOUT"}],
            list(_HAPPY_HANDSHAKE),
        ],
    )
    conn = DXLinkConnection(_StubProvider())
    try:
        await conn.connect_with_retry(attempts=4, initial_delay_s=1.0)
        assert conn._ready.is_set()
        assert len(made) == 3
        # The failed sockets were closed before the next AUTH attempt.
        assert made[0]._closed.is_set() and made[1]._closed.is_set()
        assert _no_sleep == [1.0, 2.0]
    finally:
        await conn.close()


async def test_connect_with_retry_exhaustion_raises_typed(
    monkeypatch: pytest.MonkeyPatch, _no_sleep: list[float]
) -> None:
    _patch_ws(
        monkeypatch,
        [[{"type": "ERROR", "channel": 0, "error": "UNAUTHORIZED"}] for _ in range(3)],
    )
    conn = DXLinkConnection(_StubProvider())
    with pytest.raises(DXLinkAuthError) as ei:
        await conn.connect_with_retry(attempts=3, initial_delay_s=1.0)
    assert ei.value.error_type == "UNAUTHORIZED"


async def test_post_ready_error_frame_stays_an_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # After the handshake completes, ERROR frames keep their existing surface
    # (ErrorEvent on the queue) and must NOT poison the next connect().
    conn = DXLinkConnection(_StubProvider())
    conn._ready.set()
    await conn._dispatch({"type": "ERROR", "channel": 0, "error": "BAD_SYMBOL"})
    assert conn._handshake_error is None and not conn._handshake_failed.is_set()
