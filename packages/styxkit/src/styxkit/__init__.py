"""Convenience helpers spanning the Styx runtime backends.

``styxdefs`` is the base contract; each backend (``styxdocker``, ``styxpodman``,
``styxsingularity``, ``styxgraph``) is an independent piece. ``styxkit`` sits on
top and offers one-call runner selection across whichever pieces are installed.

Backends are imported lazily, so ``styxkit`` itself only requires ``styxdefs``.
Calling a ``use_*()`` for a backend that is not installed raises a friendly
:class:`ModuleNotFoundError` naming the package (and extra) to install.
"""

from __future__ import annotations

import importlib
import importlib.util
import shutil
import typing

# Re-exported for convenience; the public surface is pinned by ``__all__`` below.
from styxdefs import (
    DryRunner,
    Execution,
    InputPathType,
    LocalRunner,
    Metadata,
    OutputPathType,
    Runner,
    StyxRuntimeError,
    StyxValidationError,
    get_global_runner,
    set_global_runner,
)

RunnerType = typing.Literal["local", "docker", "podman", "singularity"]
"""A container-runner kind selectable via :func:`resolve_runner` / :func:`use_auto`."""


class _Backend(typing.NamedTuple):
    """A lazily-loaded runner backend: its module, class, exe kwarg, and probes."""

    module: str
    cls: str
    exe_kwarg: str | None
    executables: tuple[str, ...]


# Constructor kwarg names mirror the real backend classes (field-tested in rbc).
_BACKENDS: dict[str, _Backend] = {
    "docker": _Backend("styxdocker", "DockerRunner", "docker_executable", ("docker",)),
    "podman": _Backend("styxpodman", "PodmanRunner", "podman_executable", ("podman",)),
    "singularity": _Backend(
        "styxsingularity",
        "SingularityRunner",
        "singularity_executable",
        ("apptainer", "singularity"),
    ),
    "graph": _Backend("styxgraph", "GraphRunner", None, ()),
}


def _runner_factory(kind: str) -> typing.Callable[..., Runner]:
    """Import a backend's runner class (a Runner factory), or raise a friendly error.

    Typed as ``Callable[..., Runner]`` rather than ``type[Runner]``: each backend
    constructor has its own keyword signature, none of which the base ``Runner``
    protocol declares, so the constructor kwargs are forwarded opaquely.
    """
    backend = _BACKENDS[kind]
    try:
        module = importlib.import_module(backend.module)
    except ModuleNotFoundError as exc:
        # Only translate the backend package being absent. A ModuleNotFoundError
        # naming something else means an *installed* backend failed to import a
        # transitive dependency - re-raise that as-is rather than mislabeling it
        # as "install styxkit[...]".
        if exc.name != backend.module:
            raise
        raise ModuleNotFoundError(
            f"The {kind!r} runner needs the {backend.module!r} package. "
            f'Install it with `pip install "styxkit[{kind}]"` '
            f"(or `pip install {backend.module}`)."
        ) from exc
    return typing.cast("typing.Callable[..., Runner]", getattr(module, backend.cls))


def _use(runner: Runner) -> Runner:
    """Register ``runner`` as the global runner and return it."""
    set_global_runner(runner)
    return runner


def use_local(**kwargs: typing.Any) -> Runner:
    """Register a ``LocalRunner`` as the global runner and return it."""
    return _use(LocalRunner(**kwargs))


def use_dry() -> Runner:
    """Register a ``DryRunner`` as the global runner and return it.

    ``DryRunner`` takes no configuration, so this helper accepts no arguments.
    """
    return _use(DryRunner())


def use_docker(**kwargs: typing.Any) -> Runner:
    """Register a ``DockerRunner`` as the global runner and return it.

    Requires the ``styxdocker`` package (``pip install "styxkit[docker]"``).
    """
    return _use(_runner_factory("docker")(**kwargs))


def use_podman(**kwargs: typing.Any) -> Runner:
    """Register a ``PodmanRunner`` as the global runner and return it.

    Requires the ``styxpodman`` package (``pip install "styxkit[podman]"``).
    """
    return _use(_runner_factory("podman")(**kwargs))


def use_singularity(**kwargs: typing.Any) -> Runner:
    """Register a ``SingularityRunner`` as the global runner and return it.

    Requires the ``styxsingularity`` package
    (``pip install "styxkit[singularity]"``).
    """
    return _use(_runner_factory("singularity")(**kwargs))


def use_graph(base: Runner | None = None, **kwargs: typing.Any) -> Runner:
    """Wrap a runner in a ``GraphRunner`` and register it as the global runner.

    Unlike the leaf runners, ``GraphRunner`` decorates a base runner. When
    ``base`` is omitted it wraps the current global runner, so ``use_graph()``
    starts recording a graph over whatever runner is already active.

    Requires the ``styxgraph`` package (``pip install "styxkit[graph]"``).
    """
    resolved_base = base if base is not None else get_global_runner()
    return _use(_runner_factory("graph")(resolved_base, **kwargs))


def _available(kind: str) -> bool:
    """True iff a backend is importable and one of its executables is on PATH."""
    backend = _BACKENDS[kind]
    if importlib.util.find_spec(backend.module) is None:
        return False
    return any(shutil.which(exe) for exe in backend.executables)


def resolve_runner(runner: RunnerType | typing.Literal["auto"] = "auto") -> RunnerType:
    """Resolve a runner selection, auto-detecting when ``runner == "auto"``.

    Auto prefers the first container backend that is both installed and has its
    executable on PATH (docker > podman > singularity), falling back to
    ``"local"``.

    Args:
        runner: An explicit runner kind, or ``"auto"`` to detect one.

    Returns:
        The resolved runner kind.
    """
    if runner != "auto":
        return runner
    for kind in ("docker", "podman", "singularity"):
        if _available(kind):
            return typing.cast(RunnerType, kind)
    return "local"


def use_auto(**kwargs: typing.Any) -> Runner:
    """Detect the best available runner, register it as global, and return it.

    Detection order is described in :func:`resolve_runner`. For a container
    runner, the detected executable (e.g. ``apptainer`` vs ``singularity``) is
    passed through unless the caller already supplied it. Extra keyword
    arguments are forwarded to the selected runner's constructor.
    """
    kind = resolve_runner("auto")
    if kind == "local":
        return use_local(**kwargs)
    backend = _BACKENDS[kind]
    if backend.exe_kwarg and backend.exe_kwarg not in kwargs:
        exe = next((e for e in backend.executables if shutil.which(e)), None)
        if exe is not None:
            kwargs[backend.exe_kwarg] = exe
    dispatch: dict[str, typing.Callable[..., Runner]] = {
        "docker": use_docker,
        "podman": use_podman,
        "singularity": use_singularity,
    }
    return dispatch[kind](**kwargs)


__all__ = [
    "DryRunner",
    "Execution",
    "InputPathType",
    "LocalRunner",
    "Metadata",
    "OutputPathType",
    "Runner",
    "RunnerType",
    "StyxRuntimeError",
    "StyxValidationError",
    "get_global_runner",
    "resolve_runner",
    "set_global_runner",
    "use_auto",
    "use_docker",
    "use_dry",
    "use_graph",
    "use_local",
    "use_podman",
    "use_singularity",
]
