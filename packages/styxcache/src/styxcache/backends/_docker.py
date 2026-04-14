"""Docker image digest resolution."""

from __future__ import annotations

import functools
import subprocess


@functools.lru_cache(maxsize=None)
def docker_digest_resolver(tag: str) -> str:
    """Resolve a docker image tag to its local image ID.

    Uses ``docker image inspect --format '{{.Id}}' <tag>``. The image must
    already be present locally; this function does not pull. Callers that
    want auto-pull behaviour should wrap this resolver.

    Raises:
        RuntimeError: If ``docker`` is not available or the image is not
            present locally.
    """
    if not tag:
        return "image:none"
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{.Id}}", tag],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as e:
        raise RuntimeError("docker CLI not found on PATH") from e
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"docker image inspect failed for {tag!r}: {e.stderr.strip()}"
        ) from e
    digest = result.stdout.strip()
    if not digest:
        raise RuntimeError(f"docker image inspect returned empty id for {tag!r}")
    return f"docker:{digest}"
