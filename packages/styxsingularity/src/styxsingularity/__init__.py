""".. include:: ../../README.md"""  # noqa: D415

import logging
import os
import pathlib as pl
import shlex
from typing import ClassVar

from styxcontainer_common import (
    BaseContainerExecution,
    BaseContainerRunner,
    StyxContainerError,
)
from styxdefs import Execution, InputPathType, Metadata


def _singularity_mount(host_path: str, container_path: str, readonly: bool) -> str:
    """Construct Singularity mount argument.

    Args:
        host_path: Path on the host filesystem to mount.
        container_path: Path inside the container where the host path will be mounted.
        readonly: If True, mount as read-only; otherwise mount as read-write.

    Returns:
        Formatted mount string in the format "host:container[:ro]".

    Raises:
        ValueError: If host_path or container_path contains illegal characters
            (comma, backslash, or colon).
    """
    charset = set(host_path + container_path)
    if any(c in charset for c in r",\\:"):
        raise ValueError("Illegal characters in path")
    return f"{host_path}:{container_path}{':ro' if readonly else ''}"


class StyxSingularityError(StyxContainerError):
    """Styx Singularity runtime error."""

    runtime_name: ClassVar[str] = "Singularity"

    def __init__(
        self,
        return_code: int | None = None,
        command_args: list[str] | None = None,
        singularity_args: list[str] | None = None,
        stdout_tail: list[str] | None = None,
        stderr_tail: list[str] | None = None,
    ) -> None:
        """Create StyxSingularityError."""
        super().__init__(
            return_code=return_code,
            command_args=command_args,
            runtime_args=singularity_args,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
        )


class _SingularityExecution(BaseContainerExecution):
    """Singularity execution."""

    def __init__(
        self,
        logger: logging.Logger,
        output_dir: pl.Path,
        metadata: Metadata,
        container_tag: str,
        singularity_executable: str,
        singularity_extra_args: list[str],
        environ: dict[str, str],
    ) -> None:
        """Create SingularityExecution."""
        super().__init__(
            logger=logger,
            output_dir=output_dir,
            metadata=metadata,
            container_tag=container_tag,
            environ=environ,
        )
        self.singularity_executable = singularity_executable
        self.singularity_extra_args = singularity_extra_args

    def _make_run_script_content(self, cargs: list[str]) -> str:
        # Singularity does not set the working directory to /styx_output, so we
        # cd into it explicitly at the top of the script.
        return f"#!/bin/bash\ncd /styx_output\n{shlex.join(cargs)}\n"

    def _build_command(self, cargs: list[str]) -> list[str]:
        mounts: list[str] = []
        for host_file, local_file, mutable in self.input_mounts:
            mounts += [
                "--bind",
                _singularity_mount(
                    host_file.absolute().as_posix(), local_file, readonly=not mutable
                ),
            ]
        mounts += [
            "--bind",
            _singularity_mount(
                self.output_dir.absolute().as_posix(), "/styx_output", readonly=False
            ),
        ]

        environ_arg = ",".join(f"{k}={v}" for k, v in self.environ.items())

        return [
            self.singularity_executable,
            "exec",
            *self.singularity_extra_args,
            *mounts,
            *(["--env", environ_arg] if environ_arg else []),
            self.container_tag,
            "/bin/bash",
            "/styx_output/run.sh",
        ]

    def _make_error(
        self,
        return_code: int,
        cargs: list[str],
        runtime_command: list[str],
        stdout_tail: list[str],
        stderr_tail: list[str],
    ) -> Exception:
        return StyxSingularityError(
            return_code=return_code,
            command_args=cargs,
            singularity_args=runtime_command,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
        )


class SingularityRunner(BaseContainerRunner):
    """Singularity container runner.

    Not supported on Windows.
    """

    logger_name: ClassVar[str] = "styx_singularity_runner"

    def __init__(
        self,
        image_overrides: dict[str, str] | None = None,
        singularity_executable: str = "singularity",
        singularity_extra_args: list[str] | None = None,
        data_dir: InputPathType | None = None,
        environ: dict[str, str] | None = None,
    ) -> None:
        """Create a new SingularityRunner."""
        if os.name == "nt":
            raise ValueError("SingularityRunner is not supported on Windows")
        super().__init__(
            image_overrides=image_overrides, data_dir=data_dir, environ=environ
        )
        self.singularity_executable = singularity_executable
        self.singularity_extra_args = singularity_extra_args or [
            "--no-mount",
            "hostfs",
        ]

    def _transform_container_tag(self, container_tag: str) -> str:
        if not container_tag.startswith("docker://"):
            return f"docker://{container_tag}"
        return container_tag

    def _make_execution(
        self,
        output_dir: pl.Path,
        metadata: Metadata,
        container_tag: str,
    ) -> Execution:
        return _SingularityExecution(
            logger=self.logger,
            output_dir=output_dir,
            metadata=metadata,
            container_tag=container_tag,
            singularity_executable=self.singularity_executable,
            singularity_extra_args=self.singularity_extra_args,
            environ=self.environ,
        )
