"""Shared base for container runners."""

import logging
import os
import pathlib as pl
from abc import abstractmethod
from typing import ClassVar

from styxdefs import Execution, InputPathType, Metadata, Runner


class BaseContainerRunner(Runner):
    """Shared runner implementation for container runtimes.

    Handles logging, output-directory layout, image overrides, environment
    variables, and the bookkeeping done in :meth:`start_execution`.
    Concrete subclasses provide the per-runtime execution class via
    :meth:`_make_execution` and may override
    :meth:`_transform_container_tag` to rewrite image tags.
    """

    logger_name: ClassVar[str] = "styx_container_runner"

    def __init__(
        self,
        image_overrides: dict[str, str] | None = None,
        data_dir: InputPathType | None = None,
        environ: dict[str, str] | None = None,
    ) -> None:
        """Initialise shared runner state."""
        self.data_dir = pl.Path(data_dir or "styx_tmp")
        self.uid = os.urandom(8).hex()
        self.execution_counter = 0
        self.image_overrides = image_overrides or {}
        self.environ = environ or {}
        self.logger = _configure_logger(self.logger_name)

    def start_execution(self, metadata: Metadata) -> Execution:
        """Start a new container execution for the given command metadata."""
        if metadata.container_image_tag is None:
            raise ValueError("No container image tag specified in metadata")
        container_tag = self.image_overrides.get(
            metadata.container_image_tag, metadata.container_image_tag
        )
        container_tag = self._transform_container_tag(container_tag)

        self.execution_counter += 1
        output_dir = (
            self.data_dir / f"{self.uid}_{self.execution_counter - 1}_{metadata.name}"
        )
        return self._make_execution(
            output_dir=output_dir, metadata=metadata, container_tag=container_tag
        )

    def _transform_container_tag(self, container_tag: str) -> str:
        """Rewrite the container image tag before passing it to the runtime."""
        return container_tag

    @abstractmethod
    def _make_execution(
        self,
        output_dir: pl.Path,
        metadata: Metadata,
        container_tag: str,
    ) -> Execution:
        """Construct the runtime-specific execution context."""


def _configure_logger(name: str) -> logging.Logger:
    """Return a debug-level stream logger, idempotently attached."""
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter("[%(levelname).1s] %(message)s"))
        logger.addHandler(ch)
    return logger
