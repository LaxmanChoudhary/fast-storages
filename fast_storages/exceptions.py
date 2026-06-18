"""
Common exception hierarchy for fast_storages.

Backends MUST translate provider-specific exceptions (botocore.exceptions.ClientError,
azure.core.exceptions.ResourceNotFoundError, etc.) into these so calling code never
needs to import or catch SDK-specific errors.
"""
from __future__ import annotations


class StorageError(Exception):
    """Base class for all storage-related errors."""


class StorageFileNotFoundError(StorageError):
    """Raised when an operation targets a file that does not exist."""

    def __init__(self, name: str, *, backend: str | None = None) -> None:
        self.name = name
        self.backend = backend
        msg = f"File not found: {name!r}"
        if backend:
            msg += f" (backend={backend!r})"
        super().__init__(msg)


class StoragePermissionError(StorageError):
    """Raised when the backend denies access (auth failure, bucket policy, etc.)."""

    def __init__(self, name: str, *, backend: str | None = None, detail: str | None = None) -> None:
        self.name = name
        self.backend = backend
        msg = f"Permission denied for: {name!r}"
        if backend:
            msg += f" (backend={backend!r})"
        if detail:
            msg += f" - {detail}"
        super().__init__(msg)


class StorageUnsupportedOperationError(StorageError):
    """
    Raised when a backend cannot support a requested operation at all.

    Example: local filesystem backend has no native concept of a signed URL with
    expiry; calling url(expires_in=...) on it should raise this rather than
    silently ignoring expires_in or returning something misleading.
    """

    def __init__(self, operation: str, *, backend: str | None = None, reason: str | None = None) -> None:
        self.operation = operation
        self.backend = backend
        msg = f"Operation not supported: {operation!r}"
        if backend:
            msg += f" (backend={backend!r})"
        if reason:
            msg += f" - {reason}"
        super().__init__(msg)


class StorageConfigError(StorageError):
    """Raised for invalid/missing backend configuration at construction time."""


class StorageConnectionError(StorageError):
    """Raised when the backend cannot reach the underlying service (network, DNS, timeout)."""

    def __init__(self, *, backend: str | None = None, detail: str | None = None) -> None:
        self.backend = backend
        msg = "Could not connect to storage backend"
        if backend:
            msg += f" {backend!r}"
        if detail:
            msg += f": {detail}"
        super().__init__(msg)
