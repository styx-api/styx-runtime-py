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


def _podman_mount(host_path: str, container_path: str, readonly: bool) -> str:
    """Construct Podman mount argument."""
    host_path = host_path.replace('"', r"\"")
    container_path = container_path.replace('"', r"\"")
    host_path = host_path.replace("\\", "\\\\")
    container_path = container_path.replace("\\", "\\\\")
    readonly_str = ",readonly" if readonly else ""
    return f"type=bind,source={host_path},target={container_path}{readonly_str}"


class StyxPodmanError(StyxContainerError):
    """Styx Podman runtime error."""

    runtime_name: ClassVar[str] = "Podman"

    def __init__(
        self,
        return_code: int | None = None,
        command_args: list[str] | None = None,
        podman_args: list[str] | None = None,
    ) -> None:
        """Create StyxPodmanError."""
        super().__init__(
            return_code=return_code,
            command_args=command_args,
            runtime_args=podman_args,
        )


class _PodmanExecution(BaseContainerExecution):
    """Podman execution."""

    def __init__(
        self,
        logger: logging.Logger,
        output_dir: pl.Path,
        metadata: Metadata,
        container_tag: str,
        podman_executable: str,
        podman_extra_args: list[str],
        podman_user_id: int | None,
        environ: dict[str, str],
    ) -> None:
        """Create PodmanExecution."""
        super().__init__(
            logger=logger,
            output_dir=output_dir,
            metadata=metadata,
            container_tag=container_tag,
            environ=environ,
        )
        self.podman_executable = podman_executable
        self.podman_extra_args = podman_extra_args
        self.podman_user_id = podman_user_id

    def _build_command(self, cargs: list[str]) -> list[str]:
        mounts: list[str] = []
        for host_file, local_file, mutable in self.input_mounts:
            mounts += [
                "--mount",
                _podman_mount(
                    host_file.absolute().as_posix(), local_file, readonly=not mutable
                ),
            ]
        mounts += [
            "--mount",
            _podman_mount(
                self.output_dir.absolute().as_posix(), "/styx_output", readonly=False
            ),
        ]

        environ_args: list[str] = []
        for key, value in self.environ.items():
            environ_args += ["--env", f"{key}={value}"]

        return [
            self.podman_executable,
            "run",
            *self.podman_extra_args,
            "--rm",
            "--name",
            f"styx_{self.output_dir.name}",
            *(
                ["-u", str(self.podman_user_id)]
                if self.podman_user_id is not None
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
    ) -> Exception:
        return StyxPodmanError(
            return_code=return_code,
            command_args=cargs,
            podman_args=runtime_command,
        )


class PodmanRunner(BaseContainerRunner):
    """Podman runner."""

    logger_name: ClassVar[str] = "styx_podman_runner"

    def __init__(
        self,
        image_overrides: dict[str, str] | None = None,
        podman_executable: str = "podman",
        podman_extra_args: list[str] | None = None,
        podman_user_id: int | None = None,
        data_dir: InputPathType | None = None,
        environ: dict[str, str] | None = None,
    ) -> None:
        """Create a new PodmanRunner."""
        super().__init__(
            image_overrides=image_overrides, data_dir=data_dir, environ=environ
        )
        self.podman_executable = podman_executable
        self.podman_extra_args = podman_extra_args or []
        self.podman_user_id = podman_user_id if podman_user_id is not None else HOST_UID

    def _transform_container_tag(self, container_tag: str) -> str:
        if container_tag.startswith("docker://"):
            return container_tag.replace("docker://", "docker.io/", 1)
        if not container_tag.startswith("docker.io/"):
            return f"docker.io/{container_tag}"
        return container_tag

    def _make_execution(
        self,
        output_dir: pl.Path,
        metadata: Metadata,
        container_tag: str,
    ) -> Execution:
        return _PodmanExecution(
            logger=self.logger,
            output_dir=output_dir,
            metadata=metadata,
            container_tag=container_tag,
            podman_executable=self.podman_executable,
            podman_extra_args=self.podman_extra_args,
            podman_user_id=self.podman_user_id,
            environ=self.environ,
        )
