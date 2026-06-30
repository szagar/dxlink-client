"""Stream a few quotes from the configured dxLink endpoint.

Reads DXLINK_URL / DXLINK_TOKEN from `.env` (defaults to the dxfeed demo server,
no credentials). Run:  uv run python examples/stream_demo.py
"""

import asyncio

from dxlink_client import DXLinkConnection, SettingsTokenProvider


async def main() -> None:
    async with DXLinkConnection(SettingsTokenProvider()) as conn:
        await conn.subscribe("Quote", ["AAPL"])
        await conn.subscribe("Trade", ["AAPL"])
        print("connected; streaming AAPL for ~4s...")
        deadline = asyncio.get_event_loop().time() + 4.0
        while asyncio.get_event_loop().time() < deadline:
            ev = await conn.next_event(timeout=deadline - asyncio.get_event_loop().time())
            if ev is not None:
                print(" ", ev)


if __name__ == "__main__":
    asyncio.run(main())
