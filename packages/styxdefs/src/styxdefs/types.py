"""Types for styx generated wrappers."""

import pathlib
import shlex
import typing

InputPathType = pathlib.Path | str
"""Input host file type."""
OutputPathType = pathlib.Path
"""Output host file type."""


class Execution(typing.Protocol):
    """Execution object used to execute commands.

    Created by `Runner.start_execution()`.
    """

    def input_file(
        self,
        host_file: InputPathType,
        resolve_parent: bool = False,
        mutable: bool = False,
    ) -> str:
        """Resolve host input files.

        Args:
            host_file: The input file path on the host system.
            resolve_parent: If True, resolve the parent directory of the input file.
            mutable: If True, the input file may be written to during execution.

        Returns:
            str: A local filepath.

        Note:
            Called (potentially multiple times) after
            `Runner.start_execution()` and before `Runner.run()`.
        """
        ...

    def output_file(self, local_file: str, optional: bool = False) -> OutputPathType:
        """Resolve local output files.

        Args:
            local_file: The local file path.
            optional: If True, the output file is optional.

        Returns:
            OutputPathType: A host filepath.

        Note:
            Called (potentially multiple times) after all
            `Runner.input_file()` calls.
        """
        ...

    def params(self, params: dict) -> dict:
        """Process tool parameters.

        This is called by wrappers once before command-line arguments are built.
        Parameters may be logged, cached, or modified.

        Args:
            params: Tool parameters.
        """
        ...

    def run(
        self,
        cargs: list[str],
        handle_stdout: typing.Callable[[str], None] | None = None,
        handle_stderr: typing.Callable[[str], None] | None = None,
    ) -> None:
        """Run the command.

        Args:
            cargs: A list of command arguments.
            handle_stdout: If defined the runner must forward stdout output to this\
                (may be called multiple times).
            handle_stderr: If defined the runner must forward stderr output to this\
                (may be called multiple times).

        Note:
            Called after all `Execution.input_file()`
            and `Execution.output_file()` calls.
        """
        ...


class Metadata(typing.NamedTuple):
    """Static tool metadata.

    This is structured static metadata that is known at compile time.
    Runners can use this to set up execution environments.
    """

    id: str
    """Unique identifier of the tool."""
    name: str
    """Name of the tool."""
    package: str
    """Name of the package that provides the tool."""
    citations: list[str] | None = None
    """List of references to cite when using the tool."""
    container_image_tag: str | None = None
    """Name of an image where the tool is installed and configured.
    Example: bids/mriqc.
    """


class Runner(typing.Protocol):
    """Runner object used to execute commands.

    Possible examples would be `LocalRunner`,
    `DockerRunner`, `DryRunner`, ...
    Used as a factory for `Execution` objects.
    """

    def start_execution(self, metadata: Metadata) -> Execution:
        """Start an execution.

        Args:
            metadata: Static tool metadata.

        Returns:
            Execution: An Execution object.

        Note:
            Called before any `Execution.input_file()` calls.
        """
        ...


class StyxRuntimeError(Exception):
    """Styx runtime error.

    Raised when a command reports a non-zero return code.

    Attributes:
        return_code: The return code of the failed command.
        command_args: The arguments of the failed command.
    """

    def __init__(
        self,
        return_code: int | None = None,
        command_args: list[str] | None = None,
        message_extra: str | None = None,
    ) -> None:
        """Initialize the error.

        Args:
            return_code: The return code of the failed command.
            command_args: The arguments of the failed command.
            message_extra: Additional error message.
        """
        self.return_code = return_code
        self.command_args = command_args

        if return_code is not None:
            message = f"Command failed with return code {return_code}."
        else:
            message = "Command failed."

        if command_args is not None:
            message += f"\n- Command args: {shlex.join(command_args)}"

        if message_extra is not None:
            message += f"\n{message_extra}"

        super().__init__(message)


class StyxValidationError(Exception):
    """Styx validation error.

    Raised when input validation fails for parameters.
    """

    def __init__(self, message: str) -> None:
        """Initialize the error.

        Args:
            message: The validation error message.
        """
        super().__init__(message)
