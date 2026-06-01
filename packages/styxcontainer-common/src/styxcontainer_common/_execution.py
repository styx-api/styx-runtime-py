"""Shared base for container execution contexts."""

import logging
import pathlib as pl
import shlex
import shutil
import stat
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
        # (host path, in-container path) bind mounts; all read-only. Mutable
        # inputs do not appear here - they are staged as writable copies inside
        # the output dir (see mutable_stages / input_file).
        self.input_mounts: list[tuple[pl.Path, str]] = []
        self.input_file_next_id = 0
        # Mutable inputs, keyed by absolute source path -> staged basename.
        # The writable copy lives inside the output dir (mounted read-write at
        # /styx_output), so input_file(mutable=True) and mutable_copy() resolve
        # to the same file. Copies are materialised at run time (the cache
        # middleware swaps output_dir before run, so an eager copy would land
        # in the wrong directory).
        self.mutable_stages: dict[str, str] = {}

    def input_file(
        self,
        host_file: InputPathType,
        *,
        resolve_parent: bool = False,
        mutable: bool = False,
    ) -> str:
        """Resolve an input file and register the host path for mounting.

        Mutable inputs are not bind-mounted from their original location.
        Instead a writable copy is staged inside the output directory and the
        in-container path of that copy is returned, so the tool edits the copy
        in place and the caller's original file is never touched.
        """
        if mutable:
            return f"/styx_output/{self._mutable_staged_name(host_file)}"

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
            self.input_mounts.append((_host_file_parent, local_file))
        else:
            if not _host_file.exists():
                # See note above. We don't know if the 'file' here is a
                # directory or file so we can't assert further.
                raise FileNotFoundError(f'Input file not found: "{_host_file}"')

            resolved_file = local_file = (
                f"/styx_input/{self.input_file_next_id}/{_host_file.name}"
            )
            self.input_mounts.append((_host_file, local_file))

        self.input_file_next_id += 1
        return resolved_file

    def output_file(self, local_file: str, *, optional: bool = False) -> OutputPathType:
        """Resolve output file path on the host filesystem."""
        return self.output_dir / local_file

    def mutable_copy(self, host_file: InputPathType) -> OutputPathType:
        """Return the host path of the writable copy staged for a mutable input.

        Pairs with ``input_file(host_file, mutable=True)``: both resolve to the
        same staged copy inside the output directory. The copy is materialised
        at run time, so the returned path is only populated once :meth:`run`
        has executed.
        """
        return self.output_dir / self._mutable_staged_name(host_file)

    def _mutable_staged_name(self, host_file: InputPathType) -> str:
        """Reserve (idempotently) the output-dir basename for a mutable input.

        Distinct sources that share a basename get a suffixed name so they
        never alias one file. Does not copy anything; copies happen in
        :meth:`_copy_mutable_inputs` once the output directory is final.
        """
        src = pl.Path(host_file).absolute()
        key = str(src)
        existing = self.mutable_stages.get(key)
        if existing is not None:
            return existing
        if not src.is_file():
            raise FileNotFoundError(f'Mutable input file not found: "{src}"')
        taken = set(self.mutable_stages.values())
        name = src.name
        counter = 1
        while name in taken:
            name = f"{src.stem}_{counter}{src.suffix}"
            counter += 1
        self.mutable_stages[key] = name
        return name

    def _copy_mutable_inputs(self) -> None:
        """Stage writable copies of mutable inputs into the output directory.

        Called at the start of :meth:`run`, when ``output_dir`` is final (the
        cache middleware may have swapped it). The copy is made owner-writable
        even when the source is read-only - the whole point is an editable copy
        the tool can modify in place.
        """
        for src, name in self.mutable_stages.items():
            dest = self.output_dir / name
            if dest.exists():
                continue
            shutil.copy2(src, dest)
            dest.chmod(dest.stat().st_mode | stat.S_IWUSR)

    def params(self, params: dict) -> dict:
        """Pass parameters through unchanged."""
        return params

    def run(
        self,
        cargs: list[str],
        *,
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
        self._copy_mutable_inputs()

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
                out, err = process.stdout, process.stderr
                exhaust(_stdout_handler(line.removesuffix("\n")) for line in out)  # type: ignore
                exhaust(_stderr_handler(line.removesuffix("\n")) for line in err)  # type: ignore
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
