"""
StorageManager: holds one or more named, configured Storage instances for an
application -- analogous to Django's STORAGES setting, where "default" and
"media" (for example) can point at different backends simultaneously.

This is the object FastAPI's dependency layer (dependencies.py) reads from.
Typical usage is one StorageManager per application, built at startup and
attached to app.state.
"""
from __future__ import annotations

from typing import Any

from .base import Storage
from .config import BaseStorageSettings
from .exceptions import StorageConfigError
from .registry import build_storage

DEFAULT_STORAGE_NAME = "default"


class StorageManager:
    """
    Registry of configured Storage instances, keyed by name.

    Example
    -------
        manager = StorageManager()
        manager.add(
            "default",
            backend="local",
            config={"media_root": "/var/data/uploads", "media_url": "/uploads"},
        )
        manager.add(
            "avatars",
            backend="s3",
            config=S3StorageSettings(bucket="avatars-prod"),
        )

        storage = manager.get("default")
        avatars_storage = manager.get("avatars")
    """

    def __init__(self) -> None:
        self._storages: dict[str, Storage] = {}

    def add(
        self,
        name: str,
        *,
        backend: str,
        config: dict[str, Any] | BaseStorageSettings | None = None,
        **kwargs: Any,
    ) -> Storage:
        """Build and register a Storage instance under `name`."""
        if name in self._storages:
            raise StorageConfigError(f"A storage named {name!r} is already registered")
        instance = build_storage(backend, config, **kwargs)
        self._storages[name] = instance
        return instance

    def register_instance(self, name: str, storage: Storage) -> None:
        """Register an already-constructed Storage instance directly, bypassing build_storage."""
        if name in self._storages:
            raise StorageConfigError(f"A storage named {name!r} is already registered")
        self._storages[name] = storage

    def get(self, name: str = DEFAULT_STORAGE_NAME) -> Storage:
        try:
            return self._storages[name]
        except KeyError:
            known = ", ".join(sorted(self._storages)) or "(none configured)"
            raise StorageConfigError(f"No storage named {name!r} is configured (known: {known})") from None

    def __contains__(self, name: str) -> bool:
        return name in self._storages

    async def aclose_all(self) -> None:
        """Close every managed storage's resources. Call from app shutdown/lifespan."""
        for storage in self._storages.values():
            await storage.aclose()
