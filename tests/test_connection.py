"""Offline tests for the connection state machine + collect_greeks.

No network: we exercise `_dispatch` (the reactive handshake/parse, F1/F2) and the
public helpers by stubbing `_send` / `subscribe` / `unsubscribe` and feeding
frames or events directly.
"""

from __future__ import annotations

from dxlink_client import DXLinkConnection, GreeksEvent, QuoteEvent, QuoteToken
from dxlink_client.models import ErrorEvent


def _recorder(conn: DXLinkConnection) -> list[dict]:
    sent: list[dict] = []

    async def fake_send(obj: dict) -> None:
        sent.append(obj)

    conn._send = fake_send  # type: ignore[method-assign]
    return sent


# ---- handshake dispatch (F2) ------------------------------------------------
async def test_auth_state_authenticated_path_opens_channel() -> None:
    conn = DXLinkConnection()
    conn._token = QuoteToken(url="wss://tt/ws", token="T")
    sent = _recorder(conn)

    await conn._dispatch({"type": "AUTH_STATE", "state": "UNAUTHORIZED"})
    assert sent[-1] == {"type": "AUTH", "channel": 0, "token": "T"}

    await conn._dispatch({"type": "AUTH_STATE", "state": "AUTHORIZED"})
    assert sent[-1]["type"] == "CHANNEL_REQUEST" and sent[-1]["service"] == "FEED"


async def test_anonymous_path_skips_auth() -> None:
    conn = DXLinkConnection()
    conn._token = QuoteToken(url="wss://demo.dxfeed.com/ws", token=None)
    sent = _recorder(conn)

    await conn._dispatch({"type": "AUTH_STATE", "state": "UNAUTHORIZED"})
    # no token -> go straight to channel request (demo authorizes anonymously)
    assert sent[-1]["type"] == "CHANNEL_REQUEST"


async def test_channel_opened_triggers_feed_setup() -> None:
    conn = DXLinkConnection()
    sent = _recorder(conn)
    await conn._dispatch({"type": "CHANNEL_OPENED", "channel": 1})
    assert sent[-1]["type"] == "FEED_SETUP" and sent[-1]["acceptDataFormat"] == "COMPACT"


async def test_feed_config_sets_ready_and_updates_parser() -> None:
    conn = DXLinkConnection()
    _recorder(conn)
    # first FEED_CONFIG (no eventFields) marks ready
    await conn._dispatch({"type": "FEED_CONFIG", "channel": 1})
    assert conn._ready.is_set()
    # later FEED_CONFIG carries authoritative field order (F1)
    await conn._dispatch(
        {"type": "FEED_CONFIG", "eventFields":
            {"Quote": ["eventType", "eventSymbol", "bidPrice", "askPrice"]}}
    )
    await conn._dispatch({"type": "FEED_DATA", "data": ["Quote", ["Quote", "AAPL", 1.0, 2.0]]})
    ev = await conn.next_event(timeout=0.1)
    assert isinstance(ev, QuoteEvent) and ev.bid_price == 1.0 and ev.ask_price == 2.0
    assert conn.last_feed_at is not None  # F3 watchdog hook is populated


async def test_error_frame_is_surfaced(  # F4
) -> None:
    conn = DXLinkConnection()
    await conn._dispatch({"type": "ERROR", "error": "BAD_SYMBOL"})
    ev = await conn.next_event(timeout=0.1)
    assert isinstance(ev, ErrorEvent) and ev.error == "BAD_SYMBOL"


# ---- subscription bookkeeping ----------------------------------------------
async def test_subscribe_dedups_and_force_resends() -> None:
    conn = DXLinkConnection()
    sent = _recorder(conn)

    await conn.subscribe("Quote", ["X", "Y"])
    await conn.subscribe("Quote", ["Y", "Z"])          # Y already held -> only Z
    await conn.subscribe("Quote", ["Z"], force=True)   # resend despite being held

    adds = [[d["symbol"] for d in m["add"]] for m in sent]
    assert adds == [["X", "Y"], ["Z"], ["Z"]]


async def test_unsubscribe_only_sends_held_symbols() -> None:
    conn = DXLinkConnection()
    sent = _recorder(conn)
    await conn.subscribe("Greeks", ["A", "B"])
    sent.clear()
    await conn.unsubscribe("Greeks", ["B", "C"])  # C was never held
    assert sent == [{"type": "FEED_SUBSCRIPTION", "channel": 1,
                     "remove": [{"type": "Greeks", "symbol": "B"}]}]


# ---- collect_greeks (BrokerSource-critical) --------------------------------
async def test_collect_greeks_returns_only_non_nan_delta_and_unsubscribes() -> None:
    conn = DXLinkConnection()
    calls: list[tuple] = []

    async def fake_sub(t: str, s: list[str], **k: object) -> None:
        calls.append(("sub", t, tuple(s)))

    async def fake_unsub(t: str, s: list[str]) -> None:
        calls.append(("unsub", t, tuple(s)))

    conn.subscribe = fake_sub        # type: ignore[method-assign]
    conn.unsubscribe = fake_unsub    # type: ignore[method-assign]

    conn._events.put_nowait(GreeksEvent("A", delta=0.30))
    conn._events.put_nowait(GreeksEvent("B", delta=None))   # NaN delta -> ignored
    conn._events.put_nowait(QuoteEvent("A"))                # wrong type -> ignored

    out = await conn.collect_greeks(["A", "B"], timeout=0.2)

    assert set(out) == {"A"} and out["A"].delta == 0.30      # B never delivered usable delta
    assert ("sub", "Greeks", ("A", "B")) in calls
    assert ("unsub", "Greeks", ("A", "B")) in calls          # always tears down
