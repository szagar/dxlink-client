"""Typed dxLink protocol errors + retryable-vs-fatal classification.

Per the dxLink spec (dxlink-specification/asyncapi.yml) an ``ERROR`` frame
carries one of ``UNSUPPORTED_PROTOCOL | TIMEOUT | UNAUTHORIZED |
INVALID_MESSAGE | BAD_ACTION | UNKNOWN``. Upstream AUTH endpoints are known
to reject transiently under burst re-auth (the 2026-06-23 MDS incident: the
same credential was rejected four times, then connected cleanly), so
``UNAUTHORIZED`` — and anything unrecognized — is classified retryable.
Only error types a retry of the identical handshake cannot fix (protocol /
client-frame defects) are fatal.
"""

from __future__ import annotations

#: Retrying the same handshake cannot succeed — version or client-frame
#: defects, not transient auth state.
FATAL_ERROR_TYPES: frozenset[str] = frozenset(
    {"UNSUPPORTED_PROTOCOL", "INVALID_MESSAGE", "BAD_ACTION"}
)

#: Observed-transient upstream rejections. Informational only — anything not
#: in FATAL_ERROR_TYPES (including unrecognized types) is treated retryable.
RETRYABLE_ERROR_TYPES: frozenset[str] = frozenset(
    {"TIMEOUT", "UNAUTHORIZED", "AUTH_FAILED", "UNKNOWN"}
)


def is_retryable_error_type(error_type: str | None) -> bool:
    """True unless the type is a known can't-fix-by-retrying protocol error."""
    return (error_type or "UNKNOWN").upper() not in FATAL_ERROR_TYPES


class DXLinkAuthError(ConnectionError):
    """A dxLink ``ERROR`` frame received during connect/handshake, typed.

    Carries the frame's ``error`` type so callers can branch retryable
    (``TIMEOUT`` / ``UNAUTHORIZED`` / ``UNKNOWN`` — transient burst re-auth
    rejections) vs fatal (``UNSUPPORTED_PROTOCOL`` etc.) instead of collapsing
    every handshake failure into an untyped, equally-terminal error.
    """

    def __init__(self, error_type: str | None = None, message: str = "") -> None:
        self.error_type = (error_type or "UNKNOWN").upper()
        detail = f": {message}" if message else ""
        super().__init__(f"DXLink handshake ERROR [{self.error_type}]{detail}")

    @property
    def retryable(self) -> bool:
        return is_retryable_error_type(self.error_type)
