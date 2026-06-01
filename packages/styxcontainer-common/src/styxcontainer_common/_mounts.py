"""Shared bind-mount argument construction for OCI runtimes."""


def oci_mount(host_path: str, container_path: str, readonly: bool) -> str:
    """Construct an OCI ``--mount`` argument.

    Docker and Podman accept the identical ``type=bind`` mount syntax, so they
    share this helper. Singularity uses a different ``host:container[:ro]`` form
    and builds its own.
    """
    host_path = host_path.replace('"', r"\"")
    container_path = container_path.replace('"', r"\"")
    host_path = host_path.replace("\\", "\\\\")
    container_path = container_path.replace("\\", "\\\\")
    readonly_str = ",readonly" if readonly else ""
    return f"type=bind,source={host_path},target={container_path}{readonly_str}"
