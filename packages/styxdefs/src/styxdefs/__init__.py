""".. include:: ../../README.md"""  # noqa: D415

from .dry_runner import (
    DryRunner,
)
from .global_state import (
    get_global_runner,
    set_global_runner,
)
from .local_runner import (
    LocalRunner,
)
from .types import (
    Execution,
    InputPathType,
    Metadata,
    OutputPathType,
    Runner,
    StyxRuntimeError,
    StyxValidationError,
)

__all__ = [
    "Execution",
    "InputPathType",
    "Metadata",
    "OutputPathType",
    "Runner",
    "LocalRunner",
    "DryRunner",
    "get_global_runner",
    "set_global_runner",
    "StyxRuntimeError",
    "StyxValidationError",
]
