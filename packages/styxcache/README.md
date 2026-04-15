# styxcache

Content-addressed caching middleware for [Styx](https://github.com/styx-api) runners.

Wraps any Styx `Runner` (docker, podman, singularity, local, ...) and persists
tool outputs in a content-addressed cache directory. Subsequent invocations with
identical inputs (file contents, parameters, wrapper version, container image
digest) skip execution and reuse the cached output tree directly.

## Usage

```python
from styxdefs import set_global_runner
from styxdocker import DockerRunner
from styxcache import CachingRunner, CachePolicy
from styxcache.backends import docker_digest_resolver

set_global_runner(
    CachingRunner(
        base=DockerRunner(data_dir="/tmp/styx"),
        cache_dir="/mnt/ci-cache/styx",
        policy=CachePolicy(image_digest=docker_digest_resolver),
    )
)

# Use any Styx wrapper as usual; repeated calls with the same inputs are cached.
```

## Opting out of caching

Two complementary mechanisms, useful for tools that are non-deterministic
or whose outputs you want to always refresh:

```python
import styxcache

# Dynamic, scoped to a block of code. Thread- and asyncio-safe.
with styxcache.bypass():
    result = flaky_tool(params)

# Nested re-enable, in case the outer scope is bypassed.
with styxcache.bypass():
    with styxcache.enabled():
        result = stable_tool(params)   # still cached
```

```python
# Static denylist, matched on f"{metadata.package}/{metadata.name}".
# Preferred for tools that are intrinsically non-cacheable.
policy = CachePolicy(
    bypass_tools=frozenset({"ants/PrintHeader"}),
)
```

## How the key is computed

```
sha256(
    metadata.id,
    metadata.package,
    metadata.name,
    image_digest,
    sorted(env_allowlist_values),
    params_dict_with_input_paths_replaced_by_content_hashes,
)
```

- `metadata.id` — fingerprints the wrapper version; changes when the wrapper changes.
- `image_digest` — resolves the container image tag to an immutable digest.
- `params_dict` is walked recursively; any value matching a path passed to
  `execution.input_file()` is replaced with the blake3 hash of that file (or
  directory tree, when `resolve_parent=True`).
- Input file hashing uses blake3 (fast, cryptographic, parallelizable).

## Output location

`execution.output_file("out.nii")` returns `cache_dir/<key>/out.nii` directly.
The cache directory *is* the output tree, so downstream consumers of the
returned paths read from the cache with no additional materialisation step.

## Cache purging

Each cache hit bumps the entry directory's `mtime` to the current time, so
its `mtime` reflects **last used**, not creation. This enables simple LRU
eviction with a one-liner on Unix:

```bash
# Delete cache entries not used in the last 30 days.
find /mnt/ci-cache/styx -mindepth 2 -maxdepth 2 -type d -mtime +30 \
    -exec rm -rf {} +

# Also remove empty shard directories left behind.
find /mnt/ci-cache/styx -mindepth 1 -maxdepth 1 -type d -empty -delete

# Sweep any staging dirs from crashed runs older than a day.
find /mnt/ci-cache/styx/.incoming -mindepth 1 -maxdepth 1 -type d -mtime +1 \
    -exec rm -rf {} +
```

The `-mindepth 2 -maxdepth 2` targets `<cache_dir>/<shard>/<key>/` exactly,
so you never accidentally match the cache root or a shard subdirectory.
Deleting an entry is always safe — the next call to that tool rebuilds it.

Wiping the entire cache is also always safe; it just forces a cold run.
