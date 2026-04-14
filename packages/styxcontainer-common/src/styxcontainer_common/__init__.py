"""Shared base classes for Styx container runners.

Internal support package for ``styxdocker``, ``styxpodman``, and
``styxsingularity``. API stability is not guaranteed between minor
versions — depend on one of the concrete runner packages instead.
"""

import os

from ._errors import StyxContainerError
from ._execution import BaseContainerExecution
from ._runner import BaseContainerRunner

if os.name == "posix":
    HOST_UID: int | None = os.getuid()  # type: ignore[attr-defined]
else:
    HOST_UID = None

__all__ = [
    "HOST_UID",
    "BaseContainerExecution",
    "BaseContainerRunner",
    "StyxContainerError",
]
