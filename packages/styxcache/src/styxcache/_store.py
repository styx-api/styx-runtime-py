"""Disk-backed cache store with atomic stage-and-rename commits."""

from __future__ import annotations

import os
import pathlib
import shutil
import uuid


class CacheStore:
    """Disk layout for cache entries.

    Entries live at ``<root>/<key[:2]>/<key>/`` and are considered valid only
    when they contain a ``.styxcache.done`` sentinel file.

    Writes are concurrency-safe: a miss writes into a per-run staging
    directory under ``<root>/.incoming/<uuid>/``, drops ``.styxcache.done``
    there on success, and atomically renames the whole staging directory
    into the final entry slot. If another writer committed the same key
    first, the loser discards its staging dir. Rename uses the same
    filesystem as the cache root, so the operation is atomic on POSIX and
    on Windows when the target does not exist.

    Cache eviction / GC is intentionally out of scope — the CI runner or
    deployment owner manages ``root`` lifetime. Stale ``.incoming`` dirs
    from crashed runs can be swept by periodic ``rm -rf`` without affecting
    committed entries.
    """

    DONE_MARKER = ".styxcache.done"
    STAGING_DIR = ".incoming"

    def __init__(self, root: pathlib.Path) -> None:
        """Create the store, ensuring ``root`` exists."""
        self.root = pathlib.Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / self.STAGING_DIR).mkdir(exist_ok=True)

    def entry_dir(self, key: str) -> pathlib.Path:
        """Directory for the committed cache entry identified by ``key``."""
        return self.root / key[:2] / key

    def is_hit(self, key: str) -> bool:
        """True if ``key`` is a committed cache entry."""
        return (self.entry_dir(key) / self.DONE_MARKER).exists()

    def stage(self) -> pathlib.Path:
        """Allocate a fresh staging directory for a miss-path run."""
        staging = self.root / self.STAGING_DIR / uuid.uuid4().hex
        staging.mkdir(parents=True, exist_ok=False)
        return staging

    def discard(self, staging: pathlib.Path) -> None:
        """Remove a staging directory (e.g. after a failed run or lost race)."""
        shutil.rmtree(staging, ignore_errors=True)

    def commit(self, staging: pathlib.Path, key: str) -> pathlib.Path:
        """Atomically move a populated staging dir into the entry slot.

        On success returns the final entry path. If another writer committed
        the same ``key`` first, the staging dir is discarded and the
        existing entry path is returned.
        """
        (staging / self.DONE_MARKER).write_text("1", encoding="utf-8")
        entry = self.entry_dir(key)
        entry.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.rename(staging, entry)
        except OSError:
            # Target exists (race) or cross-device rename failed. If a
            # committed entry is already present we accept it; otherwise
            # leave staging for inspection and re-raise.
            if self.is_hit(key):
                self.discard(staging)
                return entry
            raise
        return entry
