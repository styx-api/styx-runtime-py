"""Test global state."""

import styxdefs


def test_global_runner() -> None:
    """Test the global runner."""
    runner = styxdefs.get_global_runner()
    assert hasattr(runner, "start_execution")
    assert isinstance(runner, styxdefs.LocalRunner)

    styxdefs.set_global_runner(styxdefs.LocalRunner(data_dir="xyz"))
    runner = styxdefs.get_global_runner()
    assert isinstance(runner, styxdefs.LocalRunner)
