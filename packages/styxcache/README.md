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
