"""Shared base for container execution contexts."""

import logging
import pathlib as pl
import shlex
import typing
from abc import abstractmethod
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from subprocess import PIPE, Popen

from styxdefs import Execution, InputPathType, Metadata, OutputPathType


class BaseContainerExecution(Execution):
    """Shared execution implementation for container runtimes.

    Concrete subclasses implement :meth:`_build_command` and
    :meth:`_make_error`. They may also override
    :meth:`_make_run_script_content` to customise the bash script written
    into the output directory.
    """

    output_tail_lines: typing.ClassVar[int] = 40
    """Number of trailing stdout/stderr lines attached to failures.

    Captured from both streams because tools are inconsistent about which
    one they use for diagnostics (FSL/AFNI/ANTs mostly write to stderr;
    others put useful error context on stdout).
    """

    def __init__(
        self,
        logger: logging.Logger,
        output_dir: pl.Path,
        metadata: Metadata,
        container_tag: str,
        environ: dict[str, str],
    ) -> None:
        """Initialise shared execution state."""
        self.logger: logging.Logger = logger
        self.output_dir = output_dir
        self.metadata = metadata
        self.container_tag = container_tag
        self.environ = environ
        self.input_mounts: list[tuple[pl.Path, str, bool]] = []
        self.input_file_next_id = 0

    def input_file(
        self,
        host_file: InputPathType,
        resolve_parent: bool = False,
        mutable: bool = False,
    ) -> str:
        """Resolve an input file and register the host path for mounting."""
        _host_file = pl.Path(host_file)

        if resolve_parent:
            _host_file_parent = _host_file.parent
            if not _host_file_parent.is_dir():
                # If the container runtime gets passed a file to mount which
                # does not exist it will create a directory at this location.
                # This odd behaviour can lead to cryptic error messages
                # downstream, so we catch it early.
                raise FileNotFoundError(
                    f'Input folder not found: "{_host_file_parent}"'
                )

            local_file = (
                f"/styx_input/{self.input_file_next_id}/{_host_file_parent.name}"
            )
            resolved_file = f"{local_file}/{_host_file.name}"
            self.input_mounts.append((_host_file_parent, local_file, mutable))
        else:
            if not _host_file.exists():
                # See note above. We don't know if the 'file' here is a
                # directory or file so we can't assert further.
                raise FileNotFoundError(f'Input file not found: "{_host_file}"')

            resolved_file = local_file = (
                f"/styx_input/{self.input_file_next_id}/{_host_file.name}"
            )
            self.input_mounts.append((_host_file, local_file, mutable))

        self.input_file_next_id += 1
        return resolved_file

    def output_file(self, local_file: str, optional: bool = False) -> OutputPathType:
        """Resolve output file path on the host filesystem."""
        return self.output_dir / local_file

    def params(self, params: dict) -> dict:
        """Pass parameters through unchanged."""
        return params

    def run(
        self,
        cargs: list[str],
        handle_stdout: typing.Callable[[str], None] | None = None,
        handle_stderr: typing.Callable[[str], None] | None = None,
    ) -> None:
        """Execute the command in a container.

        Writes a bash run script into the output directory, delegates to
        :meth:`_build_command` for the full runtime invocation, and streams
        stdout/stderr through the supplied handlers (or the execution
        logger by default).
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

        run_script = self.output_dir / "run.sh"
        run_script.write_text(
            self._make_run_script_content(cargs),
            encoding="utf-8",
            newline="\n",
        )

        runtime_command = self._build_command(cargs)

        self.logger.debug(
            f"Running {self._runtime_label}: {shlex.join(runtime_command)}"
        )
        self.logger.debug(f"Running command: {shlex.join(cargs)}")

        # Tool output is quiet by default (debug). On non-zero exit we surface
        # the last N lines from both streams in the raised exception so users
        # don't need to rerun with DEBUG to diagnose failures. Child loggers
        # keep the stream identity in the LogRecord name (e.g. for filters or
        # a `%(name)s` formatter) without prefixing the line itself.
        stdout_logger = self.logger.getChild("stdout")
        stderr_logger = self.logger.getChild("stderr")
        user_stdout_handler = handle_stdout or (lambda line: stdout_logger.debug(line))
        user_stderr_handler = handle_stderr or (lambda line: stderr_logger.debug(line))
        stdout_tail: deque[str] = deque(maxlen=self.output_tail_lines)
        stderr_tail: deque[str] = deque(maxlen=self.output_tail_lines)

        def _stdout_handler(line: str) -> None:
            stdout_tail.append(line)
            user_stdout_handler(line)

        def _stderr_handler(line: str) -> None:
            stderr_tail.append(line)
            user_stderr_handler(line)

        time_start = datetime.now()
        with Popen(runtime_command, text=True, stdout=PIPE, stderr=PIPE) as process:
            with ThreadPoolExecutor(2) as pool:  # two threads to handle the streams
                exhaust = partial(pool.submit, partial(deque, maxlen=0))
                exhaust(_stdout_handler(line[:-1]) for line in process.stdout)  # type: ignore
                exhaust(_stderr_handler(line[:-1]) for line in process.stderr)  # type: ignore
        return_code = process.poll()
        time_end = datetime.now()
        self.logger.info(
            f"Executed {self.metadata.package} {self.metadata.name} "
            f"in {time_end - time_start}"
        )
        if return_code:
            raise self._make_error(
                return_code,
                cargs,
                runtime_command,
                list(stdout_tail),
                list(stderr_tail),
            )

    def _make_run_script_content(self, cargs: list[str]) -> str:
        """Return the contents of the run.sh script written into the output dir."""
        return f"#!/bin/bash\n{shlex.join(cargs)}\n"

    @property
    def _runtime_label(self) -> str:
        """Short runtime name used in debug log messages (e.g. ``"docker"``)."""
        return type(self).__name__.removeprefix("_").removesuffix("Execution").lower()

    @abstractmethod
    def _build_command(self, cargs: list[str]) -> list[str]:
        """Build the full runtime invocation (e.g. ``docker run ... tag ./run.sh``)."""

    @abstractmethod
    def _make_error(
        self,
        return_code: int,
        cargs: list[str],
        runtime_command: list[str],
        stdout_tail: list[str],
        stderr_tail: list[str],
    ) -> Exception:
        """Construct the runtime-specific exception raised on non-zero exit."""
