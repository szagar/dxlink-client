"""Value types: the injected quote token + the parsed market-data events."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class QuoteToken:
    """What a `TokenProvider` returns: everything needed to dial dxLink.

    `token=None` means anonymous access (the dxfeed demo server).
    """

    url: str
    token: str | None = None
    expires_at: datetime | None = None


def _num(v: object) -> float | None:
    """Coerce a COMPACT field value to float, mapping NaN/None/blank → None."""
    if v is None or v == "" or v == "NaN":
        return None
    try:
        f = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


@dataclass(frozen=True)
class QuoteEvent:
    event_symbol: str
    bid_price: float | None = None
    ask_price: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None

    @classmethod
    def from_fields(cls, rec: dict[str, object]) -> "QuoteEvent":
        return cls(
            event_symbol=str(rec.get("eventSymbol", "")),
            bid_price=_num(rec.get("bidPrice")),
            ask_price=_num(rec.get("askPrice")),
            bid_size=_num(rec.get("bidSize")),
            ask_size=_num(rec.get("askSize")),
        )


@dataclass(frozen=True)
class GreeksEvent:
    event_symbol: str
    price: float | None = None
    volatility: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    rho: float | None = None
    vega: float | None = None

    @classmethod
    def from_fields(cls, rec: dict[str, object]) -> "GreeksEvent":
        return cls(
            event_symbol=str(rec.get("eventSymbol", "")),
            price=_num(rec.get("price")),
            volatility=_num(rec.get("volatility")),
            delta=_num(rec.get("delta")),
            gamma=_num(rec.get("gamma")),
            theta=_num(rec.get("theta")),
            rho=_num(rec.get("rho")),
            vega=_num(rec.get("vega")),
        )


@dataclass(frozen=True)
class TradeEvent:
    event_symbol: str
    price: float | None = None
    size: float | None = None

    @classmethod
    def from_fields(cls, rec: dict[str, object]) -> "TradeEvent":
        return cls(
            event_symbol=str(rec.get("eventSymbol", "")),
            price=_num(rec.get("price")),
            size=_num(rec.get("size")),
        )


@dataclass(frozen=True)
class ErrorEvent:
    """Server-sent ERROR, surfaced on the event stream (Finding F4)."""

    error: str


Event = QuoteEvent | GreeksEvent | TradeEvent

_BUILDERS: dict[str, Callable[[dict[str, object]], Event]] = {
    "Quote": QuoteEvent.from_fields,
    "Greeks": GreeksEvent.from_fields,
    "Trade": TradeEvent.from_fields,
}
