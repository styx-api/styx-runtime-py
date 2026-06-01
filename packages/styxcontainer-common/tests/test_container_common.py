"""Tests for styxcontainer_common."""

import pathlib as pl
import stat

import pytest
from styxcontainer_common import (
    BaseContainerExecution,
    BaseContainerRunner,
    StyxContainerError,
)
from styxdefs import Metadata


class _FakeExecution(BaseContainerExecution):
    def _build_command(self, cargs):
        return ["fake", *cargs]

    def _make_error(
        self, return_code, cargs, runtime_command, stdout_tail, stderr_tail
    ):
        return StyxContainerError(
            return_code=return_code,
            command_args=cargs,
            runtime_args=runtime_command,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
        )


class _FakeRunner(BaseContainerRunner):
    logger_name = "styx_fake_runner"

    def _make_execution(self, output_dir, metadata, container_tag):
        return _FakeExecution(
            logger=self.logger,
            output_dir=output_dir,
            metadata=metadata,
            container_tag=container_tag,
            environ=self.environ,
        )


def _metadata(tag: str = "hello:latest") -> Metadata:
    return Metadata(
        id="fake.hello",
        name="hello",
        package="fake",
        container_image_tag=tag,
    )


def test_runner_rejects_missing_tag() -> None:
    runner = _FakeRunner()
    with pytest.raises(ValueError):
        runner.start_execution(
            Metadata(id="x", name="x", package="x", container_image_tag=None)
        )


def test_runner_applies_image_override(tmp_path: pl.Path) -> None:
    runner = _FakeRunner(
        image_overrides={"hello:latest": "local/hello:dev"}, data_dir=tmp_path
    )
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _FakeExecution)
    assert exec_.container_tag == "local/hello:dev"


def test_input_file_resolves_and_increments(tmp_path: pl.Path) -> None:
    runner = _FakeRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _FakeExecution)

    f1 = tmp_path / "a.txt"
    f1.write_text("x")
    f2 = tmp_path / "b.txt"
    f2.write_text("y")

    assert exec_.input_file(f1).endswith("/0/a.txt")
    assert exec_.input_file(f2).endswith("/1/b.txt")
    assert len(exec_.input_mounts) == 2


def test_input_file_missing_raises(tmp_path: pl.Path) -> None:
    runner = _FakeRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata())
    with pytest.raises(FileNotFoundError):
        exec_.input_file(tmp_path / "missing")


def test_mutable_input_resolves_to_output_mount(tmp_path: pl.Path) -> None:
    runner = _FakeRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _FakeExecution)

    src = tmp_path / "scan.nii"
    src.write_text("orig")

    # cargs path points at the copy inside the read-write output mount...
    in_container = exec_.input_file(src, mutable=True)
    assert in_container == "/styx_output/scan.nii"
    # ...and the output handle resolves to that same copy on the host.
    assert exec_.mutable_copy(src) == exec_.output_dir / "scan.nii"
    # A mutable input is never bind-mounted from its original location.
    assert exec_.input_mounts == []


def test_mutable_input_missing_raises(tmp_path: pl.Path) -> None:
    runner = _FakeRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _FakeExecution)
    with pytest.raises(FileNotFoundError):
        exec_.input_file(tmp_path / "missing.nii", mutable=True)


def test_mutable_input_basename_collision_suffixed(tmp_path: pl.Path) -> None:
    runner = _FakeRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _FakeExecution)

    a = tmp_path / "a"
    a.mkdir()
    b = tmp_path / "b"
    b.mkdir()
    src_a = a / "scan.nii"
    src_a.write_text("a")
    src_b = b / "scan.nii"
    src_b.write_text("b")

    assert exec_.input_file(src_a, mutable=True) == "/styx_output/scan.nii"
    assert exec_.input_file(src_b, mutable=True) == "/styx_output/scan_1.nii"
    # Idempotent per source: the same source resolves to the same copy.
    assert exec_.mutable_copy(src_a) == exec_.output_dir / "scan.nii"
    assert exec_.mutable_copy(src_b) == exec_.output_dir / "scan_1.nii"


def test_copy_mutable_inputs_stages_writable_copy(tmp_path: pl.Path) -> None:
    runner = _FakeRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _FakeExecution)

    src = tmp_path / "scan.nii"
    src.write_text("orig")
    src.chmod(0o444)  # read-only source

    exec_.input_file(src, mutable=True)
    exec_.output_dir.mkdir(parents=True, exist_ok=True)
    exec_._copy_mutable_inputs()

    copy = exec_.output_dir / "scan.nii"
    assert copy.read_text() == "orig"
    # The copy is owner-writable even though the source was read-only.
    assert copy.stat().st_mode & stat.S_IWUSR
    # Editing the copy leaves the caller's original untouched.
    copy.write_text("edited")
    assert src.read_text() == "orig"


def test_copy_mutable_inputs_is_idempotent(tmp_path: pl.Path) -> None:
    runner = _FakeRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata())
    assert isinstance(exec_, _FakeExecution)

    src = tmp_path / "scan.nii"
    src.write_text("orig")

    exec_.mutable_copy(src)
    exec_.output_dir.mkdir(parents=True, exist_ok=True)
    exec_._copy_mutable_inputs()
    copy = exec_.output_dir / "scan.nii"
    copy.write_text("edited")
    # A second staging pass must not clobber an already-staged copy.
    exec_._copy_mutable_inputs()
    assert copy.read_text() == "edited"


def test_output_file(tmp_path: pl.Path) -> None:
    runner = _FakeRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata())
    out = exec_.output_file("result.nii.gz")
    assert pl.Path(out).name == "result.nii.gz"


def test_container_error_message_includes_runtime_label() -> None:
    class _DockerStyleError(StyxContainerError):
        runtime_name = "Docker"

    err = _DockerStyleError(
        return_code=1, command_args=["x"], runtime_args=["docker", "run"]
    )
    assert "Docker args:" in str(err)


def test_transform_container_tag_default_identity(tmp_path: pl.Path) -> None:
    runner = _FakeRunner(data_dir=tmp_path)
    exec_ = runner.start_execution(_metadata("some/image:tag"))
    assert isinstance(exec_, _FakeExecution)
    assert exec_.container_tag == "some/image:tag"


def test_logger_shared_across_runners(tmp_path: pl.Path) -> None:
    runner1 = _FakeRunner(data_dir=tmp_path)
    runner2 = _FakeRunner(data_dir=tmp_path)
    # Creating a second runner returns the same named logger — handlers
    # attached once and re-used on subsequent constructions.
    assert runner1.logger is runner2.logger
    assert runner1.logger.name == _FakeRunner.logger_name
