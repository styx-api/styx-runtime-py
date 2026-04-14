"""Content hashing strategies for cache keys."""

from __future__ import annotations

import pathlib
import typing

import blake3


class InputHasher(typing.Protocol):
    """Produces a stable string fingerprint for a host path."""

    def hash_path(self, path: pathlib.Path) -> str:
        """Return a hex digest identifying the contents at ``path``.

        For files, hash the byte stream. For directories, hash the full
        recursive tree (sorted by relative path, including filenames).
        """
        ...


class ContentHasher:
    """Blake3-based content hasher.

    Files are hashed by streaming their contents. Directories are hashed by
    walking the tree in sorted order and folding each file's relative path
    and content digest into a single top-level digest.
    """

    def __init__(self, chunk_size: int = 1 << 20) -> None:
        """Create a ContentHasher.

        Args:
            chunk_size: Read buffer size for file streaming, in bytes.
        """
        self._chunk = chunk_size

    def hash_path(self, path: pathlib.Path) -> str:
        """Hash a file or directory tree."""
        if path.is_dir():
            return self._hash_dir(path)
        if path.is_file():
            return self._hash_file_hex(path)
        raise FileNotFoundError(path)

    def _hash_file_hex(self, path: pathlib.Path) -> str:
        h = blake3.blake3()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(self._chunk), b""):
                h.update(chunk)
        return h.hexdigest()

    def _hash_dir(self, root: pathlib.Path) -> str:
        h = blake3.blake3()
        for entry in sorted(root.rglob("*")):
            if not entry.is_file():
                continue
            rel = entry.relative_to(root).as_posix().encode("utf-8")
            h.update(len(rel).to_bytes(4, "big"))
            h.update(rel)
            # Nested file digest folded in as raw bytes.
            file_h = blake3.blake3()
            with entry.open("rb") as f:
                for chunk in iter(lambda: f.read(self._chunk), b""):
                    file_h.update(chunk)
            h.update(file_h.digest())
        return h.hexdigest()
