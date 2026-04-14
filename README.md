# styx-runtime-py

Python runtime and integrations for [Styx](https://github.com/styx-api/styx).
This monorepo consolidates the protocol definitions, the middleware, and the
container runners into a single `uv` workspace.

## Packages

| Package | Description |
| --- | --- |
| [`styxdefs`](packages/styxdefs) | Core types, protocol, and minimal runtime (local + dry runners). Zero runtime dependencies. |
| [`styxgraph`](packages/styxgraph) | Middleware runner that records executions as a Mermaid graph. |
| [`styxcontainer-common`](packages/styxcontainer-common) | Internal shared base classes for the container runners. |
| [`styxdocker`](packages/styxdocker) | Docker runner. |
| [`styxpodman`](packages/styxpodman) | Podman runner. |
| [`styxsingularity`](packages/styxsingularity) | Singularity/Apptainer runner. |

## Development

This is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/).
All tooling and CI runs against the whole workspace; individual packages are
published to PyPI independently.

```bash
# Install every workspace member into a shared venv.
uv sync --all-packages

# Run the full test matrix.
uv run pytest packages/

# Lint and format.
uv run ruff check
uv run ruff format --check
uv run mypy packages/
```

## Releasing

Per-package releases are driven by tags of the form `<package>-v<version>`.
For example, to ship `styxdocker` 0.6.4:

```bash
git tag styxdocker-v0.6.4
git push --tags
```

The `publish.yaml` workflow picks up the tag, builds only the matching
package, and pushes it to PyPI.
