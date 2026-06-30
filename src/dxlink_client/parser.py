"""COMPACT FEED_DATA parser driven by the server's FEED_CONFIG.eventFields.

Finding F1 (see docs): the authoritative field order for each event type is the
one the server declares in FEED_CONFIG — NOT a hard-coded map. We capture it from
every FEED_CONFIG and parse each COMPACT row positionally against it. A row block
may carry several events concatenated (length = N * len(fields)).
"""

from __future__ import annotations

from collections.abc import Iterable

from dxlink_client.models import _BUILDERS, Event


class EventParser:
    def __init__(self) -> None:
        # eventType -> authoritative field order (from FEED_CONFIG.eventFields)
        self._fields: dict[str, list[str]] = {}

    def update_config(self, event_fields: dict[str, Iterable[str]]) -> None:
        """Absorb a FEED_CONFIG.eventFields mapping (last write wins per type)."""
        for event_type, fields in event_fields.items():
            self._fields[event_type] = list(fields)

    @property
    def known_types(self) -> set[str]:
        return set(self._fields)

    def parse_feed_data(self, data: list) -> list[Event]:
        """Parse a FEED_DATA `data` payload: [type, rowblock, type, rowblock, ...]."""
        events: list[Event] = []
        i = 0
        while i + 1 < len(data):
            event_type = data[i]
            block = data[i + 1]
            i += 2
            fields = self._fields.get(event_type)
            builder = _BUILDERS.get(event_type)
            if not fields or builder is None or not isinstance(block, list):
                continue
            k = len(fields)
            if k == 0:
                continue
            # One FEED_DATA block can pack multiple events: chunk by field count.
            for j in range(0, len(block) - k + 1, k):
                rec = dict(zip(fields, block[j : j + k]))
                events.append(builder(rec))
        return events
