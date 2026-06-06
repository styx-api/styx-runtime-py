"""Smoke tests for styxkit."""

import importlib
import pathlib as pl

import pytest
import styxkit
from styxdefs import DryRunner, LocalRunner, get_global_runner


def test_use_local_registers_and_returns(tmp_path: pl.Path) -> None:
    runner = styxkit.use_local(data_dir=tmp_path)
    assert isinstance(runner, LocalRunner)
    assert get_global_runner() is runner


def test_use_dry_registers_and_returns() -> None:
    runner = styxkit.use_dry()
    assert isinstance(runner, DryRunner)
    assert get_global_runner() is runner


def test_use_docker_constructs_real_runner(tmp_path: pl.Path) -> None:
    styxdocker = pytest.importorskip("styxdocker")
    runner = styxkit.use_docker(data_dir=tmp_path)
    assert isinstance(runner, styxdocker.DockerRunner)
    assert get_global_runner() is runner


def test_use_graph_wraps_current_global_runner(tmp_path: pl.Path) -> None:
    styxgraph = pytest.importorskip("styxgraph")
    base = styxkit.use_local(data_dir=tmp_path)
    runner = styxkit.use_graph()
    assert isinstance(runner, styxgraph.GraphRunner)
    assert runner.base is base
    assert get_global_runner() is runner


def test_missing_backend_raises_friendly(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = importlib.import_module

    def fake_import(name: str, package: str | None = None) -> object:
        if name == "styxdocker":
            # A real missing-package import sets exc.name to the package.
            raise ModuleNotFoundError("No module named 'styxdocker'", name="styxdocker")
        return real_import(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    with pytest.raises(ModuleNotFoundError, match=r"styxkit\[docker\]"):
        styxkit.use_docker()


def test_transitive_import_error_is_not_masked(monkeypatch: pytest.MonkeyPatch) -> None:
    # An installed backend whose own (transitive) dependency is missing must
    # surface as-is, not be relabeled "install styxkit[docker]".
    def fake_import(name: str, package: str | None = None) -> object:
        raise ModuleNotFoundError(
            "No module named 'styxcontainer_common'", name="styxcontainer_common"
        )

    monkeypatch.setattr(importlib, "import_module", fake_import)
    with pytest.raises(ModuleNotFoundError) as excinfo:
        styxkit.use_docker()
    assert excinfo.value.name == "styxcontainer_common"
    assert "styxkit[docker]" not in str(excinfo.value)


def test_resolve_runner_passthrough() -> None:
    assert styxkit.resolve_runner("podman") == "podman"


def test_resolve_runner_auto_prefers_first_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(styxkit, "_available", lambda kind: kind == "podman")
    assert styxkit.resolve_runner("auto") == "podman"


def test_resolve_runner_auto_falls_back_to_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(styxkit, "_available", lambda kind: False)
    assert styxkit.resolve_runner("auto") == "local"


def test_use_auto_local_when_nothing_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(styxkit, "_available", lambda kind: False)
    runner = styxkit.use_auto()
    assert isinstance(runner, LocalRunner)
    assert get_global_runner() is runner


def test_use_auto_passes_detected_executable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(styxkit, "_available", lambda kind: kind == "singularity")

    def fake_which(exe: str) -> str | None:
        return "/usr/bin/apptainer" if exe == "apptainer" else None

    monkeypatch.setattr(styxkit.shutil, "which", fake_which)
    captured: dict[str, object] = {}

    def fake_use_singularity(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(styxkit, "use_singularity", fake_use_singularity)
    styxkit.use_auto()
    assert captured == {"singularity_executable": "apptainer"}
