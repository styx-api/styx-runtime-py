"""Tests for the types module."""

import styxdefs.types


def test_runtime_error() -> None:
    """Test the StyxRuntimeError class."""
    try:
        raise styxdefs.types.StyxRuntimeError(1, ["ls", "-l"])
    except styxdefs.types.StyxRuntimeError as e:
        assert e.return_code == 1
        assert e.command_args == ["ls", "-l"]
