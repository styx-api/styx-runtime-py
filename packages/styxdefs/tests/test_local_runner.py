"""Test the local runner."""

import os
import pathlib

import styxdefs


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
        x.run(["ls"], None, None)

    assert pathlib.Path(input_file).name == "abc"
    assert output_file.is_relative_to(tmp_path / "xyz")
    assert output_file.name == "def"
