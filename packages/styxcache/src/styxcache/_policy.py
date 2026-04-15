"""Cache policy configuration."""

from __future__ import annotations

import os
import typing
from dataclasses import dataclass, field

from ._hashing import ContentHasher, InputHasher

ImageDigestResolver = typing.Callable[[str], str]
"""Resolves a container image tag to an immutable digest string.

The returned digest is treated as opaque; only its equality matters for cache
keying. Implementations typically shell out to ``docker inspect`` / ``podman
inspect`` / ``singularity inspect``.
"""


def trust_tag(tag: str) -> str:
    """Trivial resolver that uses the tag string verbatim as the digest.

    Unsafe for floating tags such as ``latest``; use only when tags are
    pinned or for local development. Prefer a real resolver from
    :mod:`styxcache.backends`.
    """
    return f"tag:{tag}"


@dataclass
class CachePolicy:
    """Policy knobs for :class:`~styxcache.CachingRunner`.

    Attributes:
        hasher: Strategy used to fingerprint input files and directories.
        image_digest: Callable that resolves a container image tag to a
            digest string. Defaults to :func:`trust_tag`, which is unsafe
            for floating tags.
        env_allowlist: Names of environment variables whose values are
            folded into the cache key. Use this for variables that affect
            tool output determinism (e.g. ``OMP_NUM_THREADS``). Missing
            variables are encoded as the empty string.
        bypass_tools: Static denylist of tools that should never be cached,
            matched against ``f"{metadata.package}/{metadata.name}"`` (for
            example ``"ants/PrintHeader"``). Use for tools that are
            intrinsically non-deterministic (random seeds, network calls,
            system-state probes). Matched exactly — no globbing.
        extra: Arbitrary string-keyed fields added verbatim to the key
            payload. Useful for user-space invalidation bumps.
    """

    hasher: InputHasher = field(default_factory=ContentHasher)
    image_digest: ImageDigestResolver = trust_tag
    env_allowlist: tuple[str, ...] = ()
    bypass_tools: frozenset[str] = field(default_factory=frozenset)
    extra: dict[str, str] = field(default_factory=dict)

    def env_values(self) -> list[tuple[str, str]]:
        """Return sorted ``(name, value)`` pairs for the allowlisted env."""
        return sorted((name, os.environ.get(name, "")) for name in self.env_allowlist)
