"""Dry runner for debugging purposes."""

import pathlib
import typing

from .types import Execution, InputPathType, Metadata, OutputPathType, Runner


class DryRunner(Runner, Execution):
    """Dry runner for debugging purposes."""

    def __init__(self) -> None:
        """Create new dry runner."""
        self.last_cargs: list[str] | None = None
        self.last_metadata: Metadata | None = None
        self.last_params: dict | None = None

    def start_execution(self, metadata: Metadata) -> Execution:
        """Start execution."""
        self.last_metadata = metadata
        return self

    def input_file(
        self,
        host_file: InputPathType,
        resolve_parent: bool = False,
        mutable: bool = False,
    ) -> str:
        """Resolve input file."""
        return str(host_file)

    def output_file(self, local_file: str, optional: bool = False) -> OutputPathType:
        """Resolve output file."""
        return pathlib.Path(local_file)

    def params(self, params: dict) -> dict:
        """Process tool parameters."""
        self.last_params = params
        return params

    def run(
        self,
        cargs: list[str],
        handle_stdout: typing.Callable[[str], None] | None = None,
        handle_stderr: typing.Callable[[str], None] | None = None,
    ) -> None:
        """Execute command (in this dry runner this only captures the outputs)."""
        self.last_cargs = cargs
