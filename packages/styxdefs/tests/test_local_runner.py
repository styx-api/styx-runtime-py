"""Test the local runner."""

import os
import pathlib
import stat

import styxdefs


def _execution(tmp_path: pathlib.Path) -> styxdefs.Execution:
    runner = styxdefs.LocalRunner(data_dir=tmp_path / "xyz")
    return runner.start_execution(styxdefs.Metadata(id="1", name="t", package="t"))


def test_local_runner(tmp_path: pathlib.Path) -> None:
    """Test the local runner."""
    runner = styxdefs.LocalRunner(data_dir=tmp_path / "xyz")

    x = runner.start_execution(
        styxdefs.Metadata(
            id="123",
            name="test",
            package="test",
        )
    )

    input_file = x.input_file("abc")
    output_file = x.output_file("def")
    if os.name == "posix":
        x.run(["ls"])

    assert pathlib.Path(input_file).name == "abc"
    assert output_file.is_relative_to(tmp_path / "xyz")
    assert output_file.name == "def"


def test_local_mutable_input_stages_writable_copy(tmp_path: pathlib.Path) -> None:
    """A mutable input is copied into the output dir, leaving the original."""
    x = _execution(tmp_path)
    src = tmp_path / "scan.nii"
    src.write_text("orig")
    src.chmod(0o444)  # read-only source

    # cargs get the copy's name relative to the (working-dir) output dir...
    assert x.input_file(src, mutable=True) == "scan.nii"
    # ...and the output handle is the absolute host path of that same copy.
    copy = x.mutable_copy(src)
    assert copy == x.output_dir / "scan.nii"  # type: ignore[attr-defined]

    x._copy_mutable_inputs()  # type: ignore[attr-defined]
    assert copy.read_text() == "orig"
    # Writable even though the source was read-only.
    assert copy.stat().st_mode & stat.S_IWUSR
    # Editing the copy leaves the caller's original untouched.
    copy.write_text("edited")
    assert src.read_text() == "orig"


def test_local_mutable_basename_collision_suffixed(tmp_path: pathlib.Path) -> None:
    """Distinct sources sharing a basename never alias one copy."""
    x = _execution(tmp_path)
    a = tmp_path / "a"
    a.mkdir()
    (a / "scan.nii").write_text("a")
    b = tmp_path / "b"
    b.mkdir()
    (b / "scan.nii").write_text("b")

    assert x.input_file(a / "scan.nii", mutable=True) == "scan.nii"
    assert x.input_file(b / "scan.nii", mutable=True) == "scan_1.nii"

    x._copy_mutable_inputs()  # type: ignore[attr-defined]
    assert (x.output_dir / "scan.nii").read_text() == "a"  # type: ignore[attr-defined]
    assert (x.output_dir / "scan_1.nii").read_text() == "b"  # type: ignore[attr-defined]
