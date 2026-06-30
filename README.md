# dxlink-client

A robust async **dxLink (dxfeed) WebSocket** market-data client. Broker-agnostic:
the connection speaks the dxLink protocol; the **quote token + endpoint URL are
injected** (configured from env / `.env`), so the same client serves the
TastyTrade live feed and the dxfeed demo server. Two consumers share it — an MDS
streamer and a `contract-resolver` `BrokerSource` — without depending on each
other.

## Quick start (demo server, no credentials)

```bash
cp .env.example .env        # defaults already point at the dxfeed demo server
uv sync
uv run python examples/stream_demo.py
```

```python
from dxlink_client import DXLinkConnection, SettingsTokenProvider

async with DXLinkConnection(SettingsTokenProvider()) as conn:
    await conn.subscribe("Quote", ["AAPL"])
    print(await conn.next_event(timeout=2))

    # bounded greeks fetch for option streamer symbols (delta/premium selection)
    greeks = await conn.collect_greeks([".SPXW...C..."], timeout=3.0)
```

## Configuration (`.env`)

| Var | Meaning | Default |
|---|---|---|
| `DXLINK_URL` | dxLink WebSocket URL | dxfeed demo |
| `DXLINK_TOKEN` | quote token (`/api-quote-tokens`); blank = anonymous/demo | _(blank)_ |
| `DXLINK_KEEPALIVE_INTERVAL_S` | KEEPALIVE cadence (server timeout 60s) | `30` |

Credentials are **never** hard-coded — copy `.env.example` to `.env` (gitignored)
and fill in. For the live TastyTrade feed, set `DXLINK_URL`/`DXLINK_TOKEN` from a
`GET /api-quote-tokens` response (mint that token wherever your broker client
lives and inject it; the core package does not log in).

## Robustness

Built to the spec in `contract-resolver/docs/plans/dxlink-robust-approach.md`,
grounded in live protocol testing:

- **F1** — parse COMPACT data by the server's `FEED_CONFIG.eventFields`, not a
  fixed map. ✅ implemented
- **F2** — reactive single-loop handshake (tolerate interleaved
  `KEEPALIVE`/`FEED_CONFIG`). ✅ implemented
- **F6** — `ping_interval=None` + app-level KEEPALIVE timer. ✅ implemented
- **F3** — per-subscription staleness watchdog (resubscribe → reconnect). ◻ TODO
- **F4** — act on `ERROR` frames. ◻ partial (surfaced as a queue sentinel)
- **F5** — token refresh / reconnect. ◻ TODO

## Tests

```bash
uv run pytest -m "not network"   # offline: parser / field-order (F1)
uv run pytest -m network         # live: demo-server handshake + AAPL quote (F1+F2)
```

The demo server has **no option greeks**, so `Greeks`/`delta` parsing is covered
by the offline parser tests + a (gated) live TastyTrade smoke test, not the demo.

## Non-goals

Logging into a broker, minting tokens, business logic, or persistence. It speaks
dxLink and hands you parsed events; everything else is injected or lives above.
