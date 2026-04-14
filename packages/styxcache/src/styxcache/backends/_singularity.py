"""Singularity image digest resolution."""

from __future__ import annotations

import functools
import hashlib
import pathlib
import subprocess


@functools.lru_cache(maxsize=None)
def singularity_digest_resolver(tag: str) -> str:
    """Resolve a singularity image reference to a stable digest.

    Resolution strategy:

    1. If ``tag`` (after stripping a leading ``docker://`` prefix) points to an
       existing local file, return a SHA-256 of that SIF file's bytes.
    2. Otherwise try ``singularity inspect --json <tag>`` and hash the JSON
       output. This is as stable as singularity's metadata for the reference.
    3. Fall back to the tag string unchanged (with a ``tag:`` prefix) — unsafe
       for floating references; use only when the above strategies cannot
       apply.

    Raises:
        RuntimeError: If none of the strategies succeed.
    """
    if not tag:
        return "image:none"

    stripped = tag.removeprefix("docker://")
    local = pathlib.Path(stripped)
    if local.is_file():
        h = hashlib.sha256()
        with local.open("rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return f"singularity-sif:{h.hexdigest()}"

    try:
        result = subprocess.run(
            ["singularity", "inspect", "--json", tag],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError:
        return f"tag:{tag}"
    except subprocess.CalledProcessError:
        return f"tag:{tag}"

    h = hashlib.sha256(result.stdout.encode("utf-8"))
    return f"singularity-inspect:{h.hexdigest()}"
