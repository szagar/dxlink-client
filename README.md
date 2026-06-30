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
and fill in.

### Live TastyTrade feed (OAuth2)

Your TastyTrade `client_id` / `client_secret` / `refresh_token` are **not** the
DXLink token — they mint it via a two-step exchange. Put them in `.env` and use
the bundled provider (leave `DXLINK_URL`/`DXLINK_TOKEN` blank):

```
TASTYTRADE_CLIENT_ID=...
TASTYTRADE_CLIENT_SECRET=...
TASTYTRADE_REFRESH_TOKEN=...
```

```python
# needs the extra:  uv sync --extra tastytrade   (dxlink-client[tastytrade])
from dxlink_client import DXLinkConnection
from dxlink_client.providers.tastytrade import TastytradeTokenProvider

provider = TastytradeTokenProvider()                 # reads TASTYTRADE_* from .env
async with DXLinkConnection(provider) as conn:       # mints the token at connect
    await conn.subscribe("Quote", ["AAPL"])
    print(await conn.next_event(timeout=2))
```

The exchange: `client_id + client_secret + refresh_token` → `POST /oauth/token`
→ `access_token` (~15 min, auto-refreshed) → `GET /api-quote-tokens` →
`{ token, dxlink-url }` → injected into the connection. The `refresh_token` never
expires. The **core** package never logs in — this provider lives behind the
`[tastytrade]` extra (httpx).

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

## Latency

Measured live against TastyTrade with `examples/benchmark_latency.py` (numbers
are machine/location dependent — re-run on your production host):

| | latency |
|---|---|
| Cold start (OAuth + quote-token + WS handshake) | ~450–490 ms (one-time) |
| Chain data (REST nested chain) | ~105–155 ms |
| Price / greek, **warm** connection (subscribe → first event) | ~10–35 ms |

So a delta-selected resolution is ~150 ms (chain) + ~35 ms (greek) ≈ **~200 ms
warm**; cold adds the ~490 ms token+handshake — which is why repeated resolution
should keep the connection warm. First-event latency is the subscribe snapshot;
ongoing updates batch at the 100 ms `aggregationPeriod`. Indices stream the
underlying as `Trade` (no bid/ask); stocks/futures as `Quote`.

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
