"""Podman image digest resolution."""

from __future__ import annotations

import functools
import subprocess


@functools.lru_cache(maxsize=None)
def podman_digest_resolver(tag: str) -> str:
    """Resolve a podman image tag to its local image ID.

    Uses ``podman image inspect --format '{{.Id}}' <tag>``. The image must
    already be present locally.

    Raises:
        RuntimeError: If ``podman`` is not available or the image is not
            present locally.
    """
    if not tag:
        return "image:none"
    try:
        result = subprocess.run(
            ["podman", "image", "inspect", "--format", "{{.Id}}", tag],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as e:
        raise RuntimeError("podman CLI not found on PATH") from e
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"podman image inspect failed for {tag!r}: {e.stderr.strip()}"
        ) from e
    digest = result.stdout.strip()
    if not digest:
        raise RuntimeError(f"podman image inspect returned empty id for {tag!r}")
    return f"podman:{digest}"
