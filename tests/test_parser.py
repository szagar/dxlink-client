"""Pure (offline) tests for the eventFields-driven COMPACT parser — Finding F1."""

from __future__ import annotations

from dxlink_client import EventParser, GreeksEvent, QuoteEvent


def test_parses_quote_in_declared_field_order() -> None:
    p = EventParser()
    p.update_config({"Quote": ["eventType", "eventSymbol", "bidPrice", "askPrice", "bidSize", "askSize"]})
    events = p.parse_feed_data(["Quote", ["Quote", "AAPL", 285.88, 285.91, 400.0, 1000.0]])
    assert events == [QuoteEvent("AAPL", 285.88, 285.91, 400.0, 1000.0)]


def test_field_order_is_authoritative_not_hardcoded() -> None:
    # Server declares a DIFFERENT order — parser must follow it, not a fixed map.
    p = EventParser()
    p.update_config({"Quote": ["eventType", "eventSymbol", "askPrice", "bidPrice"]})
    (q,) = p.parse_feed_data(["Quote", ["Quote", "AAPL", 285.91, 285.88]])
    assert q.ask_price == 285.91 and q.bid_price == 285.88


def test_chunks_multiple_events_in_one_block() -> None:
    p = EventParser()
    p.update_config({"Trade": ["eventType", "eventSymbol", "price", "size"]})
    events = p.parse_feed_data(
        ["Trade", ["Trade", "AAPL", 285.91, 40.0, "Trade", "AAPL", 285.92, 5.0]]
    )
    assert [(e.price, e.size) for e in events] == [(285.91, 40.0), (285.92, 5.0)]


def test_parses_greeks_delta_in_declared_order() -> None:
    # delta is the BrokerSource-critical field — pin its positional extraction.
    p = EventParser()
    p.update_config(
        {"Greeks": ["eventType", "eventSymbol", "price", "volatility", "delta",
                    "gamma", "theta", "rho", "vega"]}
    )
    (g,) = p.parse_feed_data(
        ["Greeks", ["Greeks", ".SPY", 1.23, 0.15, 0.42, 0.01, -0.05, 0.2, 0.3]]
    )
    assert isinstance(g, GreeksEvent)
    assert g.event_symbol == ".SPY" and g.delta == 0.42 and g.volatility == 0.15


def test_nan_and_null_greeks_coerce_to_none() -> None:
    p = EventParser()
    p.update_config({"Greeks": ["eventType", "eventSymbol", "delta", "gamma"]})
    (g,) = p.parse_feed_data(["Greeks", ["Greeks", ".X", "NaN", None]])
    assert isinstance(g, GreeksEvent) and g.delta is None and g.gamma is None


def test_unknown_type_without_config_is_skipped() -> None:
    p = EventParser()  # no config absorbed yet
    assert p.parse_feed_data(["Quote", ["Quote", "AAPL", 1, 2]]) == []
