"""Smoke tests for styxpodman."""

import pathlib as pl

import pytest
from styxdefs import Metadata
from styxpodman import PodmanRunner, StyxPodmanError, _PodmanExecution


def _metadata(tag: str = "hello:latest") -> Metadata:
    return Metadata(
        id="fake.hello", name="hello", package="fake", container_image_tag=tag
    )


@pytest.mark.parametrize(
    ("input_tag", "expected"),
    [
        ("nginx:latest", "docker.io/nginx:latest"),
        ("docker://nginx:latest", "docker.io/nginx:latest"),
        ("docker.io/nginx:latest", "docker.io/nginx:latest"),
    ],
)
def test_container_tag_rewritten_to_docker_io(
    tmp_path: pl.Path, input_tag: str, expected: str
) -> None:
    runner = PodmanRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata(input_tag))
    assert isinstance(exec_, _PodmanExecution)
    assert exec_.container_tag == expected


def test_build_command_uses_podman_executable(tmp_path: pl.Path) -> None:
    runner = PodmanRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _PodmanExecution)
    cmd = exec_._build_command(["echo"])
    assert cmd[0] == "podman"
    assert cmd[1] == "run"


def test_error_label() -> None:
    err = StyxPodmanError(
        return_code=1, command_args=["x"], podman_args=["podman", "run"]
    )
    assert "Podman args:" in str(err)
