"""Simple local runner implementation."""

import logging
import os
import pathlib
import shlex
import typing
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from subprocess import PIPE, Popen

from .types import (
    Execution,
    InputPathType,
    Metadata,
    OutputPathType,
    Runner,
    StyxRuntimeError,
)


class _LocalExecution(Execution):
    """Local execution object."""

    def __init__(
        self,
        logger: logging.Logger,
        output_dir: pathlib.Path,
        metadata: Metadata,
        environ: dict[str, str] | None,
    ) -> None:
        """Initialize the execution."""
        self.logger: logging.Logger = logger
        self.output_dir: pathlib.Path = output_dir
        self.metadata: Metadata = metadata
        self.environ = environ

        while self.output_dir.exists():
            self.logger.warning(
                f"Output directory {self.output_dir} already exists. Trying another."
            )
            self.output_dir = self.output_dir.with_name(f"{self.output_dir.name}_1")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def input_file(
        self,
        host_file: InputPathType,
        resolve_parent: bool = False,
        mutable: bool = False,
    ) -> str:
        """Resolve host input files."""
        return str(pathlib.Path(host_file).absolute())

    def output_file(self, local_file: str, optional: bool = False) -> OutputPathType:
        """Resolve local output files."""
        return self.output_dir / local_file

    def params(self, params: dict) -> dict:
        """Process tool parameters."""
        return params

    def run(
        self,
        cargs: list[str],
        handle_stdout: typing.Callable[[str], None] | None = None,
        handle_stderr: typing.Callable[[str], None] | None = None,
    ) -> None:
        """Run the command."""
        self.logger.debug(f"Running command: {shlex.join(cargs)}")

        _stdout_handler = (
            handle_stdout if handle_stdout else lambda line: self.logger.info(line)
        )
        _stderr_handler = (
            handle_stderr if handle_stderr else lambda line: self.logger.error(line)
        )

        time_start = datetime.now()
        with Popen(
            cargs,
            text=True,
            stdout=PIPE,
            stderr=PIPE,
            cwd=self.output_dir,
            env=self.environ,
        ) as process:
            with ThreadPoolExecutor(2) as pool:  # two threads to handle the streams
                exhaust = partial(pool.submit, partial(deque, maxlen=0))
                exhaust(_stdout_handler(line[:-1]) for line in process.stdout)  # type: ignore
                exhaust(_stderr_handler(line[:-1]) for line in process.stderr)  # type: ignore
        return_code = process.poll()
        time_end = datetime.now()
        self.logger.info(f"Executed {self.metadata.name} in {time_end - time_start}")
        if return_code:
            raise StyxRuntimeError(return_code, cargs)


class LocalRunner(Runner):
    """Local runner implementation."""

    logger_name = "styx_local_runner"

    def __init__(
        self,
        data_dir: InputPathType | None = None,
        environ: dict[str, str] | None = None,
    ) -> None:
        """Initialize the runner."""
        self.data_dir = pathlib.Path(data_dir or "styx_tmp")
        self.uid = os.urandom(8).hex()
        self.execution_counter = 0
        self.environ = environ

        # Configure logger
        self.logger = logging.getLogger(self.logger_name)
        if not self.logger.hasHandlers():
            self.logger.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            formatter = logging.Formatter("[%(levelname).1s] %(message)s")
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def start_execution(self, metadata: Metadata) -> Execution:
        """Start a new execution."""
        output_dir = (
            self.data_dir / f"{self.uid}_{self.execution_counter}_{metadata.name}"
        )
        self.execution_counter += 1
        return _LocalExecution(
            logger=self.logger,
            output_dir=output_dir,
            metadata=metadata,
            environ=self.environ,
        )
