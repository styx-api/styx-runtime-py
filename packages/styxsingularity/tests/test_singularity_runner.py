"""Smoke tests for styxsingularity."""

import os
import pathlib as pl

import pytest
from styxdefs import Metadata

if os.name == "nt":
    pytest.skip("SingularityRunner not supported on Windows", allow_module_level=True)

from styxsingularity import (  # noqa: E402
    SingularityRunner,
    StyxSingularityError,
    _SingularityExecution,
)


def _metadata(tag: str = "hello:latest") -> Metadata:
    return Metadata(
        id="fake.hello", name="hello", package="fake", container_image_tag=tag
    )


@pytest.mark.parametrize(
    ("input_tag", "expected"),
    [
        ("hello:latest", "docker://hello:latest"),
        ("docker://hello:latest", "docker://hello:latest"),
    ],
)
def test_container_tag_gets_docker_prefix(
    tmp_path: pl.Path, input_tag: str, expected: str
) -> None:
    runner = SingularityRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata(input_tag))
    assert isinstance(exec_, _SingularityExecution)
    assert exec_.container_tag == expected


def test_run_script_cds_into_output_dir(tmp_path: pl.Path) -> None:
    runner = SingularityRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _SingularityExecution)
    script = exec_._make_run_script_content(["echo", "hi"])
    assert "cd /styx_output" in script


def test_build_command_uses_bind_flag(tmp_path: pl.Path) -> None:
    runner = SingularityRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _SingularityExecution)
    cmd = exec_._build_command(["echo"])
    assert cmd[0] == "singularity"
    assert cmd[1] == "exec"
    assert "--bind" in cmd
    assert "--mount" not in cmd


def test_error_label() -> None:
    err = StyxSingularityError(
        return_code=1, command_args=["x"], singularity_args=["singularity", "exec"]
    )
    assert "Singularity args:" in str(err)
