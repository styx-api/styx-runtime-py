"""Tests for styxcache.CachingRunner."""

from __future__ import annotations

import pathlib
import sys
import typing

import pytest
from styxcache import CachePolicy, CachingRunner
from styxdefs import (
    Execution,
    InputPathType,
    LocalRunner,
    Metadata,
    OutputPathType,
    Runner,
)


class _FakeExecution(Execution):
    """Minimal Execution that records calls and writes dummy outputs."""

    def __init__(self, output_dir: pathlib.Path, metadata: Metadata) -> None:
        self.output_dir = output_dir
        self.metadata = metadata
        self.run_count = 0
        self._outputs: list[str] = []

    def input_file(
        self,
        host_file: InputPathType,
        resolve_parent: bool = False,
        mutable: bool = False,
    ) -> str:
        return f"/mnt/{pathlib.Path(host_file).name}"

    def output_file(
        self, local_file: str, optional: bool = False
    ) -> OutputPathType:
        self._outputs.append(local_file)
        return self.output_dir / local_file

    def params(self, params: dict) -> dict:
        return params

    def run(
        self,
        cargs: list[str],
        handle_stdout: typing.Callable[[str], None] | None = None,
        handle_stderr: typing.Callable[[str], None] | None = None,
    ) -> None:
        self.run_count += 1
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Produce a deterministic output the caller can read back.
        (self.output_dir / "result.txt").write_text("ran\n")


class _FakeRunner(Runner):
    """Runner with a shared execution counter for assertions."""

    def __init__(self, base_output: pathlib.Path) -> None:
        self.base_output = base_output
        self.total_runs = 0
        self.last_execution: _FakeExecution | None = None

    def start_execution(self, metadata: Metadata) -> Execution:
        exec_dir = self.base_output / f"run_{self.total_runs}"
        ex = _FakeExecution(exec_dir, metadata)
        # Wrap .run to count at the runner level.
        inner_run = ex.run

        def _counted_run(
            cargs: list[str],
            handle_stdout: typing.Callable[[str], None] | None = None,
            handle_stderr: typing.Callable[[str], None] | None = None,
        ) -> None:
            self.total_runs += 1
            inner_run(cargs, handle_stdout, handle_stderr)

        ex.run = _counted_run  # type: ignore[method-assign]
        self.last_execution = ex
        return ex


def _wrapper(
    runner: Runner,
    in_file: pathlib.Path,
    opts: dict[str, typing.Any],
) -> OutputPathType:
    """Mimics a niwrap-generated *_execute() flow."""
    metadata = Metadata(
        id="test-tool.v1",
        name="test-tool",
        package="testpkg",
        container_image_tag="example/test:1.0",
    )
    execution = runner.start_execution(metadata)
    params = {"in_file": in_file, **opts}
    execution.params(params)
    cargs = ["test-tool", execution.input_file(in_file)]
    out = execution.output_file("result.txt")
    execution.run(cargs)
    return out


def test_second_call_is_cache_hit(tmp_path: pathlib.Path) -> None:
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")
    base = _FakeRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    out1 = _wrapper(cache, in_file, {"flag": 1})
    out2 = _wrapper(cache, in_file, {"flag": 1})

    assert base.total_runs == 1
    assert out1 == out2
    assert pathlib.Path(out1).read_text() == "ran\n"


def test_different_content_invalidates(tmp_path: pathlib.Path) -> None:
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")
    base = _FakeRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    _wrapper(cache, in_file, {"flag": 1})
    in_file.write_text("beta")
    _wrapper(cache, in_file, {"flag": 1})

    assert base.total_runs == 2


def test_different_params_invalidate(tmp_path: pathlib.Path) -> None:
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")
    base = _FakeRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    _wrapper(cache, in_file, {"flag": 1})
    _wrapper(cache, in_file, {"flag": 2})

    assert base.total_runs == 2


def test_same_content_different_host_path_is_hit(tmp_path: pathlib.Path) -> None:
    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"
    a.write_text("same-bytes")
    b.write_text("same-bytes")
    base = _FakeRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    out_a = _wrapper(cache, a, {"flag": 1})
    out_b = _wrapper(cache, b, {"flag": 1})

    assert base.total_runs == 1
    assert out_a == out_b


def test_nested_params_with_input_paths(tmp_path: pathlib.Path) -> None:
    """Simulates ants_registration-style nested list-of-dicts params."""
    f1 = tmp_path / "fixed.nii"
    f1.write_text("fixed-content")
    f2 = tmp_path / "moving.nii"
    f2.write_text("moving-content")
    base = _FakeRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    def run_once() -> OutputPathType:
        metadata = Metadata(
            id="nested.v1",
            name="reg",
            package="testpkg",
            container_image_tag="example/reg:1",
        )
        execution = cache.start_execution(metadata)
        params = {
            "stages": [
                {
                    "metrics": [
                        {"fixed_image": f1, "moving_image": f2, "weight": 1.0}
                    ]
                }
            ],
            "scalar": "hello",
        }
        execution.params(params)
        cargs = [
            "reg",
            execution.input_file(f1),
            execution.input_file(f2),
        ]
        out = execution.output_file("out.mat")
        execution.run(cargs)
        return out

    run_once()
    run_once()
    assert base.total_runs == 1


def test_metadata_id_invalidates(tmp_path: pathlib.Path) -> None:
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")
    base = _FakeRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    def run_with_id(mid: str) -> None:
        md = Metadata(
            id=mid,
            name="test-tool",
            package="testpkg",
            container_image_tag="example/test:1.0",
        )
        ex = cache.start_execution(md)
        ex.params({"in_file": in_file})
        ex.input_file(in_file)
        ex.output_file("x.txt")
        ex.run(["x"])

    run_with_id("wrapper.v1")
    run_with_id("wrapper.v2")
    assert base.total_runs == 2


def test_output_paths_live_in_cache_dir(tmp_path: pathlib.Path) -> None:
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")
    cache_dir = tmp_path / "cache"
    base = _FakeRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=cache_dir)

    out = _wrapper(cache, in_file, {"flag": 1})
    assert pathlib.Path(out).is_relative_to(cache_dir)


def test_env_allowlist_invalidates(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")
    base = _FakeRunner(tmp_path / "base")
    cache = CachingRunner(
        base,
        cache_dir=tmp_path / "cache",
        policy=CachePolicy(env_allowlist=("TEST_VAR",)),
    )

    monkeypatch.setenv("TEST_VAR", "one")
    _wrapper(cache, in_file, {"flag": 1})
    monkeypatch.setenv("TEST_VAR", "two")
    _wrapper(cache, in_file, {"flag": 1})

    assert base.total_runs == 2


def test_failed_run_does_not_pollute_cache(tmp_path: pathlib.Path) -> None:
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")

    class _Crashing(_FakeRunner):
        def start_execution(self, metadata: Metadata) -> Execution:
            ex = super().start_execution(metadata)

            def _boom(
                cargs: list[str],
                handle_stdout: typing.Callable[[str], None] | None = None,
                handle_stderr: typing.Callable[[str], None] | None = None,
            ) -> None:
                self.total_runs += 1
                raise RuntimeError("tool crashed")

            ex.run = _boom  # type: ignore[method-assign]
            return ex

    base = _Crashing(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    with pytest.raises(RuntimeError, match="tool crashed"):
        _wrapper(cache, in_file, {"flag": 1})
    # Entry should not be committed; next run tries again.
    with pytest.raises(RuntimeError, match="tool crashed"):
        _wrapper(cache, in_file, {"flag": 1})
    assert base.total_runs == 2

    # Staging dirs from failures get cleaned up (no build-up).
    staging_root = tmp_path / "cache" / ".incoming"
    assert staging_root.exists()
    assert list(staging_root.iterdir()) == []


def test_store_commit_handles_race(tmp_path: pathlib.Path) -> None:
    """commit() onto an already-committed key discards staging cleanly."""
    from styxcache._store import CacheStore

    store = CacheStore(tmp_path / "cache")
    key = "a" * 64

    # Simulate another worker having already committed this key.
    winner_entry = store.entry_dir(key)
    winner_entry.mkdir(parents=True)
    (winner_entry / "out.txt").write_text("winner")
    (winner_entry / CacheStore.DONE_MARKER).write_text("1")

    # Our run allocates a staging dir, populates it, then tries to commit.
    loser_staging = store.stage()
    (loser_staging / "out.txt").write_text("loser")
    result = store.commit(loser_staging, key)

    assert result == winner_entry
    assert (winner_entry / "out.txt").read_text() == "winner"
    assert not loser_staging.exists()


def test_local_runner_integration(tmp_path: pathlib.Path) -> None:
    """End-to-end against the real styxdefs.LocalRunner.

    Confirms the .output_dir swap works on an Execution that wasn't designed
    with caching in mind, and that outputs actually land in the cache entry.
    """
    in_file = tmp_path / "input.txt"
    in_file.write_text("payload")

    base = LocalRunner(data_dir=tmp_path / "base_data")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")
    metadata = Metadata(
        id="echo.v1", name="echo", package="testpkg", container_image_tag=None
    )

    def run_once() -> OutputPathType:
        execution = cache.start_execution(metadata)
        execution.params({"in_file": in_file})
        execution.input_file(in_file)
        out = execution.output_file("out.txt")
        # Write a file in the subprocess's cwd, which LocalRunner sets to
        # our overridden output_dir.
        execution.run(
            [
                sys.executable,
                "-c",
                "open('out.txt', 'w').write('hello from tool')",
            ]
        )
        return out

    out1 = run_once()
    assert pathlib.Path(out1).read_text() == "hello from tool"
    assert pathlib.Path(out1).is_relative_to(tmp_path / "cache")

    # Second call: the subprocess-writing command is gone (would fail if it
    # were invoked in a dir without the expected state), so a hit is the only
    # way this can succeed. Prove it by swapping the command to one that
    # would fail if actually executed.
    execution = cache.start_execution(metadata)
    execution.params({"in_file": in_file})
    execution.input_file(in_file)
    out2 = execution.output_file("out.txt")
    execution.run([sys.executable, "-c", "import sys; sys.exit(1)"])
    assert out1 == out2
    assert pathlib.Path(out2).read_text() == "hello from tool"


def test_resolve_parent_hashes_directory_tree(tmp_path: pathlib.Path) -> None:
    parent = tmp_path / "bundle"
    parent.mkdir()
    (parent / "file.txt").write_text("one")
    base = _FakeRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    def run_once() -> None:
        md = Metadata(
            id="rp.v1",
            name="rp",
            package="testpkg",
            container_image_tag="example/rp:1",
        )
        ex = cache.start_execution(md)
        ex.params({"in_file": parent / "file.txt"})
        ex.input_file(parent / "file.txt", resolve_parent=True)
        ex.output_file("out.txt")
        ex.run(["rp"])

    run_once()
    run_once()
    assert base.total_runs == 1

    # Changing a sibling file in the parent dir should invalidate.
    (parent / "sibling.txt").write_text("new")
    run_once()
    assert base.total_runs == 2
