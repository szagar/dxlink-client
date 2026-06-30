"""DXLinkConnection — a reactive, single-loop dxLink WebSocket client.

Implements the robust-approach findings (see docs/plans/dxlink-robust-approach.md
in the contract-resolver repo):

* F1 — parse COMPACT data by the field order the server declares in FEED_CONFIG.
* F2 — drive the handshake reactively from ONE receive loop (tolerate interleaved
        KEEPALIVE / FEED_CONFIG / FEED_DATA) rather than a fixed recv sequence.
* F6 — `ping_interval=None`; send app-level KEEPALIVE on a timer.

Not yet implemented here (tracked in the spec): the per-subscription staleness
watchdog (F3) and reconnect/token-refresh (F5). They layer on top of this loop;
the connection exposes the hooks they need (`last_feed_at`, `subscribe(force=)`).
"""

from __future__ import annotations

import asyncio
import json
import time

import websockets

from dxlink_client.models import ErrorEvent, Event, GreeksEvent, QuoteToken
from dxlink_client.parser import EventParser
from dxlink_client.tokens import AnonymousTokenProvider, TokenProvider

_FEED_CHANNEL = 1

# Fields we ask the server to deliver per event type (it echoes the authoritative
# order back in FEED_CONFIG; the parser uses that, not this).
_ACCEPT_EVENT_FIELDS = {
    "Quote": ["eventType", "eventSymbol", "bidPrice", "askPrice", "bidSize", "askSize"],
    "Greeks": ["eventType", "eventSymbol", "price", "volatility", "delta", "gamma",
               "theta", "rho", "vega"],
    "Trade": ["eventType", "eventSymbol", "price", "size"],
}


class DXLinkConnection:
    def __init__(
        self,
        provider: TokenProvider | None = None,
        *,
        keepalive_interval_s: int = 30,
        connect_timeout_s: float = 10.0,
    ) -> None:
        self._provider = provider or AnonymousTokenProvider()
        self._keepalive_interval_s = keepalive_interval_s
        self._connect_timeout_s = connect_timeout_s

        self._ws: websockets.ClientConnection | None = None
        self._token: QuoteToken | None = None
        self._parser = EventParser()
        self._events: asyncio.Queue[Event | ErrorEvent] = asyncio.Queue()
        self._ready = asyncio.Event()
        self._authorized = asyncio.Event()
        self._channel_open = asyncio.Event()
        self._run_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._subscriptions: dict[str, set[str]] = {}  # eventType -> symbols
        self.last_feed_at: float | None = None  # monotonic; for the F3 watchdog

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    async def __aenter__(self) -> "DXLinkConnection":
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def connect(self) -> None:
        self._token = await self._provider.quote_token()
        # demo server expects a browser-style Origin; live TT does not need one.
        origin = "https://demo.dxfeed.com" if "demo.dxfeed.com" in self._token.url else None
        self._ws = await websockets.connect(
            self._token.url,
            ping_interval=None,  # F6 — DXLink owns liveness; WS pings cause 1006 under load
            origin=origin,  # type: ignore[arg-type]  # websockets uses an Origin NewType
            additional_headers={"User-Agent": "dxlink-client/0.1"},
        )
        self._run_task = asyncio.create_task(self._run())
        await self._send({"type": "SETUP", "channel": 0, "version": "0.1-py/0.1.0",
                          "keepaliveTimeout": 60, "acceptKeepaliveTimeout": 60})
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=self._connect_timeout_s)
        except asyncio.TimeoutError as e:
            await self.close()
            raise ConnectionError("DXLink handshake did not complete in time") from e
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    async def close(self) -> None:
        for task in (self._keepalive_task, self._run_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._keepalive_task = self._run_task = None
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._ready.clear()
        self._authorized.clear()
        self._channel_open.clear()

    # ------------------------------------------------------------------ #
    # subscriptions
    # ------------------------------------------------------------------ #
    async def subscribe(self, event_type: str, symbols: list[str], *, force: bool = False) -> None:
        held = self._subscriptions.setdefault(event_type, set())
        new = symbols if force else [s for s in symbols if s not in held]
        if not new:
            return
        await self._send({"type": "FEED_SUBSCRIPTION", "channel": _FEED_CHANNEL,
                          "add": [{"type": event_type, "symbol": s} for s in new]})
        held.update(new)

    async def unsubscribe(self, event_type: str, symbols: list[str]) -> None:
        held = self._subscriptions.get(event_type, set())
        drop = [s for s in symbols if s in held]
        if not drop:
            return
        await self._send({"type": "FEED_SUBSCRIPTION", "channel": _FEED_CHANNEL,
                          "remove": [{"type": event_type, "symbol": s} for s in drop]})
        held.difference_update(drop)

    async def next_event(self, timeout: float | None = None) -> Event | ErrorEvent | None:
        try:
            return await asyncio.wait_for(self._events.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def collect_greeks(
        self, symbols: list[str], *, timeout: float = 3.0
    ) -> dict[str, GreeksEvent]:
        """Subscribe, gather one usable (non-NaN delta) Greeks event per symbol,
        then unsubscribe. Missing symbols are simply absent — the caller decides
        whether that's a NoMatch. Bounded by `timeout` (first event ~150ms live)."""
        wanted = set(symbols)
        await self.subscribe("Greeks", symbols)
        out: dict[str, GreeksEvent] = {}
        deadline = time.monotonic() + timeout
        try:
            while wanted - out.keys():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                ev = await self.next_event(timeout=remaining)
                if isinstance(ev, GreeksEvent) and ev.event_symbol in wanted and ev.delta is not None:
                    out[ev.event_symbol] = ev
        finally:
            await self.unsubscribe("Greeks", symbols)
        return out

    # ------------------------------------------------------------------ #
    # receive loop (reactive handshake + dispatch) — F2
    # ------------------------------------------------------------------ #
    async def _run(self) -> None:
        assert self._ws is not None
        try:
            async for raw in self._ws:
                msg = json.loads(raw)
                await self._dispatch(msg)
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            return

    async def _dispatch(self, msg: dict) -> None:
        mtype = msg.get("type")
        if mtype == "FEED_DATA":
            self.last_feed_at = time.monotonic()
            for ev in self._parser.parse_feed_data(msg.get("data", [])):
                self._events.put_nowait(ev)
        elif mtype == "AUTH_STATE":
            state = msg.get("state")
            if state == "UNAUTHORIZED" and self._token and self._token.token:
                await self._send({"type": "AUTH", "channel": 0, "token": self._token.token})
            elif state == "UNAUTHORIZED":
                await self._open_channel()  # demo: anonymous → proceed
            elif state == "AUTHORIZED":
                self._authorized.set()
                await self._open_channel()
        elif mtype == "CHANNEL_OPENED":
            self._channel_open.set()
            await self._send({"type": "FEED_SETUP", "channel": _FEED_CHANNEL,
                              "acceptAggregationPeriod": 0.1, "acceptDataFormat": "COMPACT",
                              "acceptEventFields": _ACCEPT_EVENT_FIELDS})
        elif mtype == "FEED_CONFIG":
            # The first FEED_CONFIG (post FEED_SETUP) configures the channel and
            # carries NO eventFields — that's the "channel ready" signal. The
            # eventFields-bearing FEED_CONFIGs arrive later, per subscribed type;
            # the parser absorbs each as it comes (F1). So: ready on any
            # FEED_CONFIG; update field order whenever it's present.
            ef = msg.get("eventFields")
            if ef:
                self._parser.update_config(ef)  # F1 — authoritative field order
            self._ready.set()
        elif mtype == "ERROR":
            # F4 (partial): surface as a sentinel so callers/watchdog can react.
            self._events.put_nowait(ErrorEvent(str(msg.get("error", "unknown"))))

    async def _open_channel(self) -> None:
        await self._send({"type": "CHANNEL_REQUEST", "channel": _FEED_CHANNEL,
                          "service": "FEED", "parameters": {"contract": "AUTO"}})

    async def _keepalive_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._keepalive_interval_s)
                await self._send({"type": "KEEPALIVE", "channel": 0})
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            return

    async def _send(self, obj: dict) -> None:
        if self._ws is None:
            raise ConnectionError("not connected")
        await self._ws.send(json.dumps(obj))
