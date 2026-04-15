"""Runtime bypass control for the caching middleware.

Two complementary mechanisms:

* :func:`bypass` / :func:`enabled` context managers for dynamic, per-call
  opt-out. Backed by :class:`contextvars.ContextVar`, so the bypass state
  is isolated per thread and per asyncio task by construction.
* :attr:`styxcache.CachePolicy.bypass_tools` for static, declarative
  opt-out keyed on ``f"{metadata.package}/{metadata.name}"``. Use this for
  tools that are intrinsically non-cacheable (random seeds, network
  access, system-state probes).

When either mechanism fires, :meth:`CachingRunner.start_execution` returns
the underlying base runner's execution unwrapped — the caching layer is
completely invisible for that call.
"""

from __future__ import annotations

import contextlib
import contextvars
from collections.abc import Iterator

_BYPASS: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "styxcache_bypass", default=False
)


@contextlib.contextmanager
def bypass() -> Iterator[None]:
    """Disable caching for all styxcache-wrapped calls in this context.

    Thread-safe and asyncio-safe: the bypass state is stored in a
    :class:`contextvars.ContextVar`, so concurrent threads and tasks do
    not leak state across each other.

    Nests correctly with :func:`enabled`.
    """
    token = _BYPASS.set(True)
    try:
        yield
    finally:
        _BYPASS.reset(token)


@contextlib.contextmanager
def enabled() -> Iterator[None]:
    """Re-enable caching within this context, overriding an outer bypass."""
    token = _BYPASS.set(False)
    try:
        yield
    finally:
        _BYPASS.reset(token)


def is_bypassed() -> bool:
    """Return True if the current context has bypass active."""
    return _BYPASS.get()
