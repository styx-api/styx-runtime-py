"""Global state for the Styx library."""

from .local_runner import LocalRunner
from .types import Runner

_STYX_GLOBAL_RUNNER: Runner | None = None


def get_global_runner() -> Runner:
    """Get the default runner."""
    global _STYX_GLOBAL_RUNNER
    if _STYX_GLOBAL_RUNNER is None:
        _STYX_GLOBAL_RUNNER = LocalRunner()
    return _STYX_GLOBAL_RUNNER


def set_global_runner(runner: Runner) -> None:
    """Set the default runner."""
    global _STYX_GLOBAL_RUNNER
    _STYX_GLOBAL_RUNNER = runner
