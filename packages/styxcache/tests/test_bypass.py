"""Tests for the bypass mechanisms (context manager + policy denylist)."""

from __future__ import annotations

import pathlib
import threading
import typing

import styxcache
from styxcache import CachePolicy, CachingRunner
from styxdefs import Execution, InputPathType, Metadata, OutputPathType, Runner


class _FakeExecution(Execution):
    def __init__(self, output_dir: pathlib.Path) -> None:
        self.output_dir = output_dir

    def input_file(
        self,
        host_file: InputPathType,
        resolve_parent: bool = False,
        mutable: bool = False,
    ) -> str:
        return str(host_file)

    def output_file(self, local_file: str, optional: bool = False) -> OutputPathType:
        return self.output_dir / local_file

    def params(self, params: dict) -> dict:
        return params

    def run(
        self,
        cargs: list[str],
        handle_stdout: typing.Callable[[str], None] | None = None,
        handle_stderr: typing.Callable[[str], None] | None = None,
    ) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "result.txt").write_text("ran\n")


class _CountingRunner(Runner):
    def __init__(self, base_output: pathlib.Path) -> None:
        self.base_output = base_output
        self.total_runs = 0
        self.started_executions: list[Execution] = []

    def start_execution(self, metadata: Metadata) -> Execution:
        ex = _FakeExecution(self.base_output / f"run_{len(self.started_executions)}")
        self.started_executions.append(ex)
        inner = ex.run

        def _counted(
            cargs: list[str],
            handle_stdout: typing.Callable[[str], None] | None = None,
            handle_stderr: typing.Callable[[str], None] | None = None,
        ) -> None:
            self.total_runs += 1
            inner(cargs, handle_stdout, handle_stderr)

        ex.run = _counted  # type: ignore[method-assign]
        return ex


def _invoke(runner: Runner, in_file: pathlib.Path, *, name: str = "tool") -> None:
    md = Metadata(
        id=f"{name}.v1", name=name, package="testpkg", container_image_tag=None
    )
    ex = runner.start_execution(md)
    ex.params({"in_file": in_file})
    ex.input_file(in_file)
    ex.output_file("out.txt")
    ex.run([name])


def test_bypass_context_manager_skips_cache(tmp_path: pathlib.Path) -> None:
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")
    base = _CountingRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    # Two bypassed calls both run the base — nothing is cached.
    with styxcache.bypass():
        _invoke(cache, in_file)
        _invoke(cache, in_file)
    assert base.total_runs == 2

    # Outside bypass, first call runs, second hits.
    _invoke(cache, in_file)
    _invoke(cache, in_file)
    assert base.total_runs == 3


def test_enabled_overrides_outer_bypass(tmp_path: pathlib.Path) -> None:
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")
    base = _CountingRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    with styxcache.bypass():
        # Outer bypass active.
        _invoke(cache, in_file)  # runs (bypassed)
        assert base.total_runs == 1
        with styxcache.enabled():
            _invoke(cache, in_file)  # runs (miss, will cache)
            _invoke(cache, in_file)  # hit
        assert base.total_runs == 2
        _invoke(cache, in_file)  # back in outer bypass; runs
    assert base.total_runs == 3


def test_bypass_tools_denylist_by_package_name(tmp_path: pathlib.Path) -> None:
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")
    base = _CountingRunner(tmp_path / "base")
    cache = CachingRunner(
        base,
        cache_dir=tmp_path / "cache",
        policy=CachePolicy(bypass_tools=frozenset({"testpkg/never_cache_me"})),
    )

    # Denylisted tool: runs every time.
    _invoke(cache, in_file, name="never_cache_me")
    _invoke(cache, in_file, name="never_cache_me")
    assert base.total_runs == 2

    # Other tools still cache.
    _invoke(cache, in_file, name="normal_tool")
    _invoke(cache, in_file, name="normal_tool")
    assert base.total_runs == 3  # second normal_tool was a hit


def test_bypass_returns_base_execution_unwrapped(tmp_path: pathlib.Path) -> None:
    """Bypass should return the base runner's execution, not a wrapper."""
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")
    base = _CountingRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    md = Metadata(id="x.v1", name="x", package="testpkg", container_image_tag=None)
    with styxcache.bypass():
        ex = cache.start_execution(md)
    # Must be the base execution itself — no caching state attached.
    assert ex is base.started_executions[-1]


def test_bypass_is_thread_local(tmp_path: pathlib.Path) -> None:
    """Bypass state in one thread must not leak into another."""
    in_file = tmp_path / "input.txt"
    in_file.write_text("alpha")
    base = _CountingRunner(tmp_path / "base")
    cache = CachingRunner(base, cache_dir=tmp_path / "cache")

    # Cache one entry first so the hit path exists.
    _invoke(cache, in_file)
    assert base.total_runs == 1

    thread_saw_bypass: dict[str, bool] = {}
    start = threading.Event()
    thread_b_saw = threading.Event()

    def thread_a() -> None:
        with styxcache.bypass():
            start.set()
            thread_b_saw.wait(timeout=2)
            thread_saw_bypass["a"] = styxcache.is_bypassed()

    def thread_b() -> None:
        start.wait(timeout=2)
        # Even though thread A has bypass active, we must not see it.
        thread_saw_bypass["b"] = styxcache.is_bypassed()
        thread_b_saw.set()

    ta = threading.Thread(target=thread_a)
    tb = threading.Thread(target=thread_b)
    ta.start()
    tb.start()
    ta.join()
    tb.join()

    assert thread_saw_bypass == {"a": True, "b": False}


def test_is_bypassed_reports_current_state() -> None:
    assert styxcache.is_bypassed() is False
    with styxcache.bypass():
        assert styxcache.is_bypassed() is True
        with styxcache.enabled():
            assert styxcache.is_bypassed() is False
        assert styxcache.is_bypassed() is True
    assert styxcache.is_bypassed() is False
