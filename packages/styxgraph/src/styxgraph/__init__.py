""".. include:: ../../README.md"""  # noqa: D415

import typing
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Generic, TypeVar

from styxdefs import Execution, InputPathType, Metadata, OutputPathType, Runner


@dataclass
class Node:
    """Represents a command execution node in the dependency graph."""

    execution_id: int
    package: str
    name: str
    inputs: list[Path]
    outputs: list[Path]

    @property
    def id(self) -> str:
        """Generate a unique identifier for the node."""
        return f"{self.execution_id}_{self.package}_{self.name}"

    @property
    def label(self) -> str:
        """Generate a display label for the node."""
        return f"{self.package}/{self.name}"


class GraphStyle(Enum):
    """Supported Mermaid graph styles."""

    TOP_DOWN = "TD"
    LEFT_RIGHT = "LR"
    BOTTOM_TOP = "BT"
    RIGHT_LEFT = "RL"


class DependencyResolver:
    """Handles the logic for resolving dependencies between nodes."""

    @staticmethod
    def is_dependent(input_path: Path, output_root: Path) -> bool:
        """Check if input_path is within the output root directory."""
        try:
            return input_path.absolute().is_relative_to(output_root.absolute())
        except ValueError:
            return False

    def build_dependencies(self, nodes: list[Node]) -> set[tuple[str, str]]:
        """Build all dependencies between nodes."""
        dependencies = set()

        # Create lookup for root output directories
        output_roots = {node.outputs[0]: node.id for node in nodes if node.outputs}

        # Check each node's inputs against all output roots
        for node in nodes:
            for input_path in node.inputs:
                for output_root, source_id in output_roots.items():
                    if (
                        self.is_dependent(input_path, output_root)
                        and source_id != node.id
                    ):
                        dependencies.add((source_id, node.id))

        return dependencies


class MermaidFormatter:
    """Handles the generation of Mermaid diagram syntax."""

    def __init__(self, style: GraphStyle = GraphStyle.TOP_DOWN) -> None:
        """Create MermaidFormatter."""
        self.style = style

    def format_node(self, node: Node) -> str:
        """Format a single node for Mermaid."""
        return f'  {node.id}["{node.label}"]'

    def format_edge(self, source: str, target: str) -> str:
        """Format a single edge for Mermaid."""
        return f"  {source} --> {target}"

    def generate_diagram(
        self, nodes: list[Node], dependencies: set[tuple[str, str]]
    ) -> str:
        """Generate complete Mermaid diagram."""
        lines = [f"graph {self.style.value}"]

        # Add nodes
        for node in nodes:
            lines.append(self.format_node(node))

        # Add edges
        for source, target in dependencies:
            lines.append(self.format_edge(source, target))

        return "\n".join(lines)


T = TypeVar("T", bound=Runner)


class _GraphExecution(Execution):
    """Wrapper execution that tracks file operations."""

    def __init__(
        self, base: Execution, graph_runner: "GraphRunner", metadata: Metadata
    ) -> None:
        self.base = base
        self.graph_runner = graph_runner
        self.metadata = metadata
        self.input_files: list[Path] = []
        self.output_files: list[Path] = []

    def input_file(
        self,
        host_file: InputPathType,
        resolve_parent: bool = False,
        mutable: bool = False,
    ) -> str:
        self.input_files.append(Path(host_file))
        return self.base.input_file(host_file, resolve_parent, mutable)

    def output_file(self, local_file: str, optional: bool = False) -> OutputPathType:
        output_file = self.base.output_file(local_file, optional)
        self.output_files.append(Path(output_file))
        return output_file

    def params(self, params: dict) -> dict:
        return self.base.params(params)

    def run(
        self,
        cargs: list[str],
        handle_stdout: typing.Callable[[str], None] | None = None,
        handle_stderr: typing.Callable[[str], None] | None = None,
    ) -> None:
        self.graph_runner.record_execution(
            self.metadata, self.input_files, self.output_files
        )
        return self.base.run(
            cargs, handle_stdout=handle_stdout, handle_stderr=handle_stderr
        )


class GraphRunner(Runner, Generic[T]):
    """Runner that builds and maintains a dependency graph."""

    def __init__(self, base: T, graph_style: GraphStyle = GraphStyle.TOP_DOWN) -> None:
        """Create GraphRunner."""
        self.base = base
        self.nodes: list[Node] = []
        self.dependency_resolver = DependencyResolver()
        self.mermaid_formatter = MermaidFormatter(graph_style)
        self._execution_counter = 0

    def start_execution(self, metadata: Metadata) -> Execution:
        """Start execution."""
        return _GraphExecution(self.base.start_execution(metadata), self, metadata)

    def _next_execution_id(self) -> int:
        self._execution_counter += 1
        return self._execution_counter - 1

    def record_execution(
        self,
        metadata: Metadata,
        input_files: list[Path],
        output_files: list[Path],
    ) -> None:
        """Record a command execution in the graph."""
        node = Node(
            execution_id=self._next_execution_id(),
            package=metadata.package,
            name=metadata.name,
            inputs=input_files,
            outputs=output_files,
        )
        self.nodes.append(node)

    def generate_mermaid(self) -> str:
        """Generate a Mermaid diagram of the dependency graph."""
        dependencies = self.dependency_resolver.build_dependencies(self.nodes)
        return self.mermaid_formatter.generate_diagram(self.nodes, dependencies)
