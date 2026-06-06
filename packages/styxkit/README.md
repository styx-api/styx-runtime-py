# Styxkit - convenience helpers for the Styx runtime

`styxkit` is a small convenience layer on top of the Styx runtime. It provides
one-call runner selection across whichever backend packages you have installed,
so you do not have to import each runner class and wire up the global runner by
hand.

It sits at the top of the runtime stack:

- `styxdefs` is the base contract (the `Runner` protocol, `LocalRunner`,
  `DryRunner`, and `set_global_runner` / `get_global_runner`).
- Each backend (`styxdocker`, `styxpodman`, `styxsingularity`, `styxgraph`) is an
  independent package depending only on `styxdefs`.
- `styxkit` depends only on `styxdefs` and imports the backends **lazily**, so
  installing it does not pull in any container backend you do not want.

## Installation

```bash
pip install styxkit                 # base, plus styxdefs
pip install "styxkit[docker]"       # + the Docker backend
pip install "styxkit[all]"          # + every backend (docker, podman, singularity, graph)
```

Calling a `use_*()` for a backend you have not installed raises a friendly error
telling you exactly what to install.

## Usage

```python
import styxkit

# Pick a specific runner:
styxkit.use_docker()
styxkit.use_singularity(singularity_executable="apptainer")
styxkit.use_local()

# ...or let styxkit detect the best available container runtime,
# falling back to local when none is found:
runner = styxkit.use_auto()

# Each use_*() registers the runner as the global runner and returns it,
# so you can configure it without a get_global_runner() round-trip:
runner = styxkit.use_docker()
runner.data_dir = "/tmp/styx"
```

`use_auto()` prefers, in order, the first container backend that is both
installed and has its executable on `PATH`: Docker, then Podman, then
Singularity/Apptainer, otherwise the local runner.

## Available helpers

- `use_local`, `use_dry` - always available (from `styxdefs`).
- `use_docker`, `use_podman`, `use_singularity` - lazy, require the matching backend.
- `use_graph(base=None)` - wraps a runner in a graph recorder; defaults to the
  current global runner.
- `use_auto()` / `resolve_runner()` - detect and select a runner.
- The core `styxdefs` symbols (`Runner`, `Execution`, `Metadata`,
  `set_global_runner`, ...) are re-exported for convenience.

## License

`styxkit` is released under the MIT License. See the LICENSE file for details.
