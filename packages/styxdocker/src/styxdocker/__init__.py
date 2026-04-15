""".. include:: ../../README.md"""  # noqa: D415

import logging
import pathlib as pl
from typing import ClassVar

from styxcontainer_common import (
    HOST_UID,
    BaseContainerExecution,
    BaseContainerRunner,
    StyxContainerError,
)
from styxdefs import Execution, InputPathType, Metadata


def _docker_mount(host_path: str, container_path: str, readonly: bool) -> str:
    """Construct Docker mount argument."""
    host_path = host_path.replace('"', r"\"")
    container_path = container_path.replace('"', r"\"")
    host_path = host_path.replace("\\", "\\\\")
    container_path = container_path.replace("\\", "\\\\")
    readonly_str = ",readonly" if readonly else ""
    return f"type=bind,source={host_path},target={container_path}{readonly_str}"


class StyxDockerError(StyxContainerError):
    """Styx Docker runtime error."""

    runtime_name: ClassVar[str] = "Docker"

    def __init__(
        self,
        return_code: int | None = None,
        command_args: list[str] | None = None,
        docker_args: list[str] | None = None,
        stdout_tail: list[str] | None = None,
        stderr_tail: list[str] | None = None,
    ) -> None:
        """Create StyxDockerError."""
        super().__init__(
            return_code=return_code,
            command_args=command_args,
            runtime_args=docker_args,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
        )


class _DockerExecution(BaseContainerExecution):
    """Docker execution."""

    def __init__(
        self,
        logger: logging.Logger,
        output_dir: pl.Path,
        metadata: Metadata,
        container_tag: str,
        docker_executable: str,
        docker_extra_args: list[str],
        docker_user_id: int | None,
        environ: dict[str, str],
    ) -> None:
        """Create DockerExecution."""
        super().__init__(
            logger=logger,
            output_dir=output_dir,
            metadata=metadata,
            container_tag=container_tag,
            environ=environ,
        )
        self.docker_executable = docker_executable
        self.docker_extra_args = docker_extra_args
        self.docker_user_id = docker_user_id

    def _build_command(self, cargs: list[str]) -> list[str]:
        mounts: list[str] = []
        for host_file, local_file, mutable in self.input_mounts:
            mounts += [
                "--mount",
                _docker_mount(
                    host_file.absolute().as_posix(), local_file, readonly=not mutable
                ),
            ]
        mounts += [
            "--mount",
            _docker_mount(
                self.output_dir.absolute().as_posix(), "/styx_output", readonly=False
            ),
        ]

        environ_args: list[str] = []
        for key, value in self.environ.items():
            environ_args += ["--env", f"{key}={value}"]

        return [
            self.docker_executable,
            "run",
            *self.docker_extra_args,
            "--rm",
            "--name",
            f"styx_{self.output_dir.name}",
            *(
                ["-u", str(self.docker_user_id)]
                if self.docker_user_id is not None
                else []
            ),
            "-w",
            "/styx_output",
            *mounts,
            "--entrypoint",
            "/bin/bash",
            *environ_args,
            self.container_tag,
            "./run.sh",
        ]

    def _make_error(
        self,
        return_code: int,
        cargs: list[str],
        runtime_command: list[str],
        stdout_tail: list[str],
        stderr_tail: list[str],
    ) -> Exception:
        return StyxDockerError(
            return_code=return_code,
            command_args=cargs,
            docker_args=runtime_command,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
        )


class DockerRunner(BaseContainerRunner):
    """Docker runner."""

    logger_name: ClassVar[str] = "styx_docker_runner"

    def __init__(
        self,
        image_overrides: dict[str, str] | None = None,
        docker_executable: str = "docker",
        docker_extra_args: list[str] | None = None,
        docker_user_id: int | None = None,
        data_dir: InputPathType | None = None,
        environ: dict[str, str] | None = None,
    ) -> None:
        """Create a new DockerRunner."""
        super().__init__(
            image_overrides=image_overrides, data_dir=data_dir, environ=environ
        )
        self.docker_executable = docker_executable
        self.docker_extra_args = docker_extra_args or []
        self.docker_user_id = docker_user_id if docker_user_id is not None else HOST_UID

    def _make_execution(
        self,
        output_dir: pl.Path,
        metadata: Metadata,
        container_tag: str,
    ) -> Execution:
        return _DockerExecution(
            logger=self.logger,
            output_dir=output_dir,
            metadata=metadata,
            container_tag=container_tag,
            docker_executable=self.docker_executable,
            docker_extra_args=self.docker_extra_args,
            docker_user_id=self.docker_user_id,
            environ=self.environ,
        )
