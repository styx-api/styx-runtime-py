"""Shared base error for container runtimes."""

import shlex
from typing import ClassVar

from styxdefs import StyxRuntimeError


class StyxContainerError(StyxRuntimeError):
    """Base class for Styx container runtime errors.

    Subclasses set :attr:`runtime_name` to get a properly labelled message.
    """

    runtime_name: ClassVar[str] = "Container"

    def __init__(
        self,
        return_code: int | None = None,
        command_args: list[str] | None = None,
        runtime_args: list[str] | None = None,
        stdout_tail: list[str] | None = None,
        stderr_tail: list[str] | None = None,
    ) -> None:
        """Create a container runtime error."""
        message_extra = (
            f"- {self.runtime_name} args: {shlex.join(runtime_args)}"
            if runtime_args
            else None
        )
        super().__init__(
            return_code=return_code,
            command_args=command_args,
            message_extra=message_extra,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
        )
