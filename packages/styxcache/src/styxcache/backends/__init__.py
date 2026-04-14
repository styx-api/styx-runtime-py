"""Container image digest resolvers for popular runtimes."""

from ._docker import docker_digest_resolver
from ._podman import podman_digest_resolver
from ._singularity import singularity_digest_resolver

__all__ = [
    "docker_digest_resolver",
    "podman_digest_resolver",
    "singularity_digest_resolver",
]
