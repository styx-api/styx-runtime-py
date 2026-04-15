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
        self.stdout_tail = stdout_tail
        self.stderr_tail = stderr_tail
        parts: list[str] = []
        if runtime_args:
            parts.append(f"- {self.runtime_name} args: {shlex.join(runtime_args)}")
        for label, tail in (("stdout", stdout_tail), ("stderr", stderr_tail)):
            if tail:
                parts.append(
                    f"- Last {len(tail)} {label} line(s):\n"
                    + "\n".join(f"    {line}" for line in tail)
                )
        super().__init__(
            return_code=return_code,
            command_args=command_args,
            message_extra="\n".join(parts) if parts else None,
        )
