"""
fast-storages: Django-style, loosely-coupled async file storage for FastAPI.

Public API surface -- everything below is what calling code should import.
Backends, the registry internals, etc. are implementation details accessible
under fast-storages.backends / fast-storages.registry but not re-exported
here to keep the top-level namespace small and stable.
"""
# Import triggers registration of bundled backends (local, s3, azure) into
# the registry via their @register_backend decorators. This import is
# cheap even when optional SDKs (aioboto3, azure-storage-blob) aren't
# installed, because each backend module only imports its SDK lazily inside
# __init__, not at module level.
from . import backends as _backends  # noqa: F401
from .base import SaveContent, Storage
from .config import BaseStorageSettings
from .dependencies import get_storage, get_storage_manager
from .exceptions import (
    StorageConfigError,
    StorageConnectionError,
    StorageError,
    StorageFileNotFoundError,
    StoragePermissionError,
    StorageUnsupportedOperationError,
)
from .files import FileMeta, UploadFileReader, guess_content_type, read_all
from .manager import DEFAULT_STORAGE_NAME, StorageManager
from .registry import build_storage, list_registered_backends, register_backend

__version__ = "0.1.0"

__all__ = [
    "Storage",
    "SaveContent",
    "StorageManager",
    "DEFAULT_STORAGE_NAME",
    "BaseStorageSettings",
    "get_storage",
    "get_storage_manager",
    "build_storage",
    "register_backend",
    "list_registered_backends",
    "UploadFileReader",
    "FileMeta",
    "guess_content_type",
    "read_all",
    "StorageError",
    "StorageFileNotFoundError",
    "StoragePermissionError",
    "StorageUnsupportedOperationError",
    "StorageConfigError",
    "StorageConnectionError",
]
