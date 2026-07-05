"""connect-with-backoff — bounded, classified retry for dxLink connects.

The client capability behind Fixes 1+2 of the MDS reconnect-hardening plan
(zts-massive docs/plans/mds-dxlink-reconnect-hardening.md): a transient
upstream AUTH/connect error is retried in-process with real exponential
backoff — never delegated to a tight external crash-restart loop, which
self-perpetuates the upstream stale-session conflict (each crashed connection
leaves a half-open session server-side that rejects the next AUTH before the
stale one expires).

Callers pass a zero-arg coroutine factory that performs ONE full connect
attempt (mint/refresh the token, dial, handshake) so every retry runs with
fresh state. A ``DXLinkAuthError`` classified fatal short-circuits
immediately; retryable errors back off ``initial_delay_s → … → max_delay_s``
and the last error is re-raised once ``attempts`` is exhausted.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from dxlink_client.errors import DXLinkAuthError

T = TypeVar("T")

#: Called after each failed-but-retryable attempt: (attempt, attempts, error,
#: delay before the next attempt — None when this was the final attempt).
OnAttemptFailed = Callable[[int, int, BaseException, "float | None"], None]


async def connect_with_backoff(
    connect: Callable[[], Awaitable[T]],
    *,
    attempts: int = 8,
    initial_delay_s: float = 1.0,
    max_delay_s: float = 60.0,
    retry_on: tuple[type[BaseException], ...] = (TimeoutError, OSError),
    on_attempt_failed: OnAttemptFailed | None = None,
) -> T:
    """Run ``connect()`` with bounded exponential backoff on transient errors.

    * ``DXLinkAuthError`` — retried iff ``.retryable`` (a fatal type such as
      ``UNSUPPORTED_PROTOCOL`` re-raises immediately, no further attempts).
    * ``retry_on`` instances — retried. The default covers ``ConnectionError``
      / timeouts (``ConnectionError`` is an ``OSError`` subclass); pass
      ``(Exception,)`` to also retry token-mint failures etc.
    * Anything else propagates immediately.

    Once ``attempts`` is exhausted the last error is re-raised so the caller
    can fail loud (the exhaustion is the caller's CRITICAL, each transient
    attempt its WARNING via ``on_attempt_failed``).
    """
    attempts = max(1, attempts)
    delay = initial_delay_s
    for attempt in range(1, attempts + 1):
        try:
            return await connect()
        except Exception as exc:
            fatal_auth = isinstance(exc, DXLinkAuthError) and not exc.retryable
            transient = not fatal_auth and (
                isinstance(exc, DXLinkAuthError) or isinstance(exc, retry_on)
            )
            last_attempt = attempt >= attempts
            if transient and on_attempt_failed is not None:
                on_attempt_failed(
                    attempt, attempts, exc, None if last_attempt else delay
                )
            if not transient or last_attempt:
                raise
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay_s)
    raise AssertionError("unreachable")  # pragma: no cover
