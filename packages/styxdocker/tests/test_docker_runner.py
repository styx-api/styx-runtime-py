"""Smoke tests for styxdocker."""

import pathlib as pl

from styxdefs import Metadata
from styxdocker import DockerRunner, StyxDockerError, _DockerExecution


def _metadata(tag: str = "hello:latest") -> Metadata:
    return Metadata(
        id="fake.hello", name="hello", package="fake", container_image_tag=tag
    )


def test_runner_passes_through_tag(tmp_path: pl.Path) -> None:
    runner = DockerRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata("some/image:tag"))
    assert isinstance(exec_, _DockerExecution)
    assert exec_.container_tag == "some/image:tag"


def test_build_command_includes_user_id_flag(tmp_path: pl.Path) -> None:
    runner = DockerRunner(data_dir=tmp_path, docker_user_id=1337)
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _DockerExecution)
    cmd = exec_._build_command(["echo", "hi"])
    assert cmd[0] == "docker"
    assert cmd[1] == "run"
    assert "-u" in cmd and "1337" in cmd
    assert "--entrypoint" in cmd
    assert cmd[-1] == "./run.sh"


def test_build_command_omits_user_id_when_none(tmp_path: pl.Path) -> None:
    runner = DockerRunner(data_dir=tmp_path, docker_user_id=None)
    # Override HOST_UID default for cross-platform determinism.
    runner.docker_user_id = None
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _DockerExecution)
    cmd = exec_._build_command(["echo"])
    assert "-u" not in cmd


def test_build_command_passes_env_flags(tmp_path: pl.Path) -> None:
    runner = DockerRunner(data_dir=tmp_path, environ={"FOO": "bar"})
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _DockerExecution)
    cmd = exec_._build_command(["echo"])
    assert "--env" in cmd
    assert "FOO=bar" in cmd


def test_error_label(tmp_path: pl.Path) -> None:
    err = StyxDockerError(
        return_code=1, command_args=["x"], docker_args=["docker", "run"]
    )
    assert "Docker args:" in str(err)
