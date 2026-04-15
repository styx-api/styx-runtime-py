"""Caching middleware runner and execution."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import pathlib
import typing

from styxdefs import Execution, InputPathType, Metadata, OutputPathType, Runner

from ._bypass import is_bypassed
from ._policy import CachePolicy
from ._store import CacheStore

_KEY_VERSION = 1
_STDOUT_FILE = ".styxcache.stdout.gz"
_STDERR_FILE = ".styxcache.stderr.gz"


def _canon(p: str | os.PathLike[str]) -> str:
    """Canonical string form for host-path equality comparison."""
    return os.fspath(pathlib.Path(p))


class CachingRunner(Runner):
    """Runner middleware that content-addresses tool outputs.

    Wraps an existing :class:`~styxdefs.Runner`. On first invocation for a
    given input set the base runner executes and its output directory is
    redirected into a cache entry. Subsequent invocations with identical
    inputs skip the base runner entirely and reuse the cached entry.

    Inputs contributing to the cache key:

    * ``metadata.id`` / ``metadata.package`` / ``metadata.name`` — fingerprint
      the wrapper version.
    * The container image tag resolved through :attr:`CachePolicy.image_digest`.
    * The post-normalisation params dict, with any value matching a path
      passed to :meth:`Execution.input_file` substituted by its content hash.
    * Each ``input_file`` invocation's ``resolve_parent`` / ``mutable`` flags.
    * Environment variables named in :attr:`CachePolicy.env_allowlist`.
    * :attr:`CachePolicy.extra` user-space invalidation fields.

    Notes on ``mutable=True`` inputs: the key captures pre-run content, so a
    cache hit will skip re-mutating the host file. Pair this runner with a
    base that copies mutable inputs before mounting to avoid surprising
    callers that rely on in-place mutation.
    """

    def __init__(
        self,
        base: Runner,
        cache_dir: str | os.PathLike[str],
        policy: CachePolicy | None = None,
    ) -> None:
        """Create a CachingRunner.

        Args:
            base: The underlying runner that performs real execution.
            cache_dir: Root directory for persisted cache entries. Must be
                writable. Caller is responsible for lifecycle/GC.
            policy: Optional cache policy overrides.
        """
        self.base = base
        self.store = CacheStore(pathlib.Path(cache_dir))
        self.policy = policy or CachePolicy()

    def start_execution(self, metadata: Metadata) -> Execution:
        """Begin a cached execution, or bypass to the base runner.

        Caching is skipped — and the base runner's execution is returned
        unwrapped — when either:

        * :func:`styxcache.bypass` is active in the current context.
        * ``f"{metadata.package}/{metadata.name}"`` is in
          :attr:`CachePolicy.bypass_tools`.
        """
        if is_bypassed() or self._matches_bypass_tools(metadata):
            return self.base.start_execution(metadata)
        return _CachingExecution(
            base=self.base.start_execution(metadata),
            runner=self,
            metadata=metadata,
        )

    def _matches_bypass_tools(self, metadata: Metadata) -> bool:
        if not self.policy.bypass_tools:
            return False
        return f"{metadata.package}/{metadata.name}" in self.policy.bypass_tools


class _CachingExecution(Execution):
    """Execution wrapper that computes keys lazily and redirects outputs."""

    def __init__(
        self,
        base: Execution,
        runner: CachingRunner,
        metadata: Metadata,
    ) -> None:
        self._base = base
        self._runner = runner
        self._metadata = metadata
        self._params: dict[str, typing.Any] | None = None
        self._input_records: list[dict[str, typing.Any]] = []
        self._host_to_hash: dict[str, str] = {}
        self._key: str | None = None
        self._entry_dir: pathlib.Path | None = None
        self._staging_dir: pathlib.Path | None = None
        self._is_hit: bool = False

    # -- Execution protocol -------------------------------------------------

    def input_file(
        self,
        host_file: InputPathType,
        resolve_parent: bool = False,
        mutable: bool = False,
    ) -> str:
        local = self._base.input_file(host_file, resolve_parent, mutable)
        host_path = pathlib.Path(host_file)
        canon = _canon(host_path)

        if canon not in self._host_to_hash:
            target = host_path.parent if resolve_parent else host_path
            content_hash = self._runner.policy.hasher.hash_path(target)
            self._host_to_hash[canon] = content_hash

        self._input_records.append(
            {
                "host": canon,
                "hash": self._host_to_hash[canon],
                "resolve_parent": resolve_parent,
                "mutable": mutable,
            }
        )
        return local

    def output_file(self, local_file: str, optional: bool = False) -> OutputPathType:
        self._ensure_key_computed()
        assert self._entry_dir is not None
        return self._entry_dir / local_file

    def params(self, params: dict) -> dict:
        normalised = self._base.params(params)
        self._params = normalised
        return normalised

    def run(
        self,
        cargs: list[str],
        handle_stdout: typing.Callable[[str], None] | None = None,
        handle_stderr: typing.Callable[[str], None] | None = None,
    ) -> None:
        # If no output_file was called, compute the key here as a fallback so
        # the miss-path still redirects correctly.
        self._ensure_key_computed()
        assert self._key is not None
        assert self._entry_dir is not None

        if self._is_hit:
            # Bump mtime on hit so downstream GC scripts can evict LRU-style
            # (e.g. `find <cache_dir> -mtime +N -type d -exec rm -rf {} +`).
            # This clobbers the creation timestamp, which we've decided is
            # an acceptable trade for actionable "last used" information.
            try:
                os.utime(self._entry_dir, None)
            except OSError:
                # Filesystem refused the utime call (read-only mount,
                # permissions). Caching still works; GC just falls back to
                # FIFO by creation time.
                pass
            _replay_stream(self._entry_dir / _STDOUT_FILE, handle_stdout)
            _replay_stream(self._entry_dir / _STDERR_FILE, handle_stderr)
            return

        assert self._staging_dir is not None
        # Swap the base execution's output directory to the staging dir so the
        # container writes there; commit atomically on success.
        previous_output_dir = getattr(self._base, "output_dir", None)
        if previous_output_dir is None:
            raise RuntimeError(
                "CachingRunner requires the base execution to expose a "
                "mutable .output_dir attribute (all styx container and local "
                "runners do). Got base: "
                f"{type(self._base).__name__}"
            )

        # Tee handlers so we capture lines for later replay while still
        # forwarding to the caller's handlers in real time.
        stdout_lines: list[str] = []
        stderr_lines: list[str] = []

        def _tee_stdout(line: str) -> None:
            stdout_lines.append(line)
            if handle_stdout is not None:
                handle_stdout(line)

        def _tee_stderr(line: str) -> None:
            stderr_lines.append(line)
            if handle_stderr is not None:
                handle_stderr(line)

        try:
            self._base.output_dir = self._staging_dir  # type: ignore[attr-defined]
            try:
                self._base.run(cargs, _tee_stdout, _tee_stderr)
            except BaseException:
                self._runner.store.discard(self._staging_dir)
                raise
        finally:
            self._base.output_dir = previous_output_dir  # type: ignore[attr-defined]

        _persist_stream(self._staging_dir / _STDOUT_FILE, stdout_lines)
        _persist_stream(self._staging_dir / _STDERR_FILE, stderr_lines)
        self._runner.store.commit(self._staging_dir, self._key)

    # -- key computation ----------------------------------------------------

    def _ensure_key_computed(self) -> None:
        if self._key is not None:
            return

        payload = {
            "v": _KEY_VERSION,
            "metadata": {
                "id": self._metadata.id,
                "package": self._metadata.package,
                "name": self._metadata.name,
            },
            "image": self._runner.policy.image_digest(
                self._metadata.container_image_tag or ""
            ),
            "env": self._runner.policy.env_values(),
            "extra": dict(sorted(self._runner.policy.extra.items())),
            "inputs": [
                {
                    "hash": rec["hash"],
                    "resolve_parent": rec["resolve_parent"],
                    "mutable": rec["mutable"],
                }
                for rec in self._input_records
            ],
            "params": self._canonicalise_params(self._params),
        }
        blob = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=_json_default
        ).encode("utf-8")
        self._key = hashlib.sha256(blob).hexdigest()
        self._entry_dir = self._runner.store.entry_dir(self._key)
        self._is_hit = self._runner.store.is_hit(self._key)
        if not self._is_hit:
            self._staging_dir = self._runner.store.stage()

    def _canonicalise_params(self, value: typing.Any) -> typing.Any:  # noqa: ANN401
        """Recursively substitute known host paths with their content hashes."""
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, str):
            # Compare as canonical pathlib form to match the recorded keys.
            canon = _canon(value)
            if canon in self._host_to_hash:
                return f"__styx_input__:{self._host_to_hash[canon]}"
            return value
        if isinstance(value, pathlib.PurePath):
            canon = _canon(value)
            if canon in self._host_to_hash:
                return f"__styx_input__:{self._host_to_hash[canon]}"
            return canon
        if isinstance(value, dict):
            return {k: self._canonicalise_params(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._canonicalise_params(v) for v in value]
        # Unknown type — fall back to repr for stability.
        return f"__styx_repr__:{type(value).__name__}:{value!r}"


def _json_default(obj: typing.Any) -> typing.Any:  # noqa: ANN401
    if isinstance(obj, pathlib.PurePath):
        return os.fspath(obj)
    raise TypeError(f"Unserialisable for cache key: {type(obj).__name__}")


def _persist_stream(path: pathlib.Path, lines: list[str]) -> None:
    """Write captured lines to a gzipped file, one per line."""
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as f:
        for line in lines:
            f.write(line)
            f.write("\n")


def _replay_stream(
    path: pathlib.Path,
    handler: typing.Callable[[str], None] | None,
) -> None:
    """Read a gzipped stream file and feed each line to ``handler``.

    Missing files are tolerated — entries committed by older styxcache
    versions predate stream persistence and simply skip replay.
    """
    if handler is None or not path.exists():
        return
    with gzip.open(path, "rt", encoding="utf-8", newline="\n") as f:
        for line in f:
            handler(line.rstrip("\n"))
