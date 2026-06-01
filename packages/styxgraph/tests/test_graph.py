"""Tests for styxgraph dependency tracking, including mutable inputs."""

from __future__ import annotations

import pathlib
import typing

from styxdefs import Execution, InputPathType, Metadata, OutputPathType
from styxgraph import GraphRunner


class _FakeExecution(Execution):
    """Minimal Execution whose paths are rooted in a fixed output dir."""

    def __init__(self, output_dir: pathlib.Path, metadata: Metadata) -> None:
        self.output_dir = output_dir
        self.metadata = metadata

    def input_file(
        self,
        host_file: InputPathType,
        resolve_parent: bool = False,
        mutable: bool = False,
    ) -> str:
        return f"/mnt/{pathlib.Path(host_file).name}"

    def output_file(self, local_file: str, optional: bool = False) -> OutputPathType:
        return self.output_dir / local_file

    def mutable_copy(self, host_file: InputPathType) -> OutputPathType:
        return self.output_dir / pathlib.Path(host_file).name

    def params(self, params: dict) -> dict:
        return params

    def run(
        self,
        cargs: list[str],
        handle_stdout: typing.Callable[[str], None] | None = None,
        handle_stderr: typing.Callable[[str], None] | None = None,
    ) -> None:
        return None


class _FakeRunner:
    def __init__(self, output_dir: pathlib.Path) -> None:
        self.output_dir = output_dir

    def start_execution(self, metadata: Metadata) -> Execution:
        return _FakeExecution(self.output_dir, metadata)


def _metadata() -> Metadata:
    return Metadata(id="t.v1", name="tool", package="pkg")


def test_mutable_copy_records_input_and_output_edge(tmp_path: pathlib.Path) -> None:
    out_dir = tmp_path / "out"
    runner: GraphRunner = GraphRunner(_FakeRunner(out_dir))
    execution = runner.start_execution(_metadata())

    src = tmp_path / "scan.nii"
    src.write_text("orig")

    # Forwarded to the base and resolved to the staged copy's host path.
    result = execution.mutable_copy(src)
    assert result == out_dir / "scan.nii"

    execution.run(["tool"])

    node = runner.nodes[0]
    # A mutable input is both consumed and produced: it appears on both edges.
    assert pathlib.Path(src) in node.inputs
    assert out_dir / "scan.nii" in node.outputs


def test_mutable_copy_dependency_chains_to_consumer(tmp_path: pathlib.Path) -> None:
    out_dir = tmp_path / "out"
    runner: GraphRunner = GraphRunner(_FakeRunner(out_dir))

    # First tool mutates an input, surfacing the copy under the output root.
    producer = runner.start_execution(_metadata())
    src = tmp_path / "scan.nii"
    src.write_text("orig")
    mutated = producer.mutable_copy(src)
    producer.run(["edit", str(mutated)])

    # Second tool consumes that mutated copy.
    consumer = runner.start_execution(_metadata())
    consumer.input_file(mutated)
    consumer.output_file("result.txt")
    consumer.run(["use"])

    mermaid = runner.generate_mermaid()
    # The mutated-copy output of node 0 is an input of node 1: an edge exists.
    assert "0_pkg_tool" in mermaid
    assert "1_pkg_tool" in mermaid
    assert "0_pkg_tool --> 1_pkg_tool" in mermaid
