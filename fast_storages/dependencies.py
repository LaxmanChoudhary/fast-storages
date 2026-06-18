"""
FastAPI dependency-injection wiring.

The StorageManager for an app is expected to live on app.state.storage_manager
(set up once, typically in a lifespan handler -- see examples/app.py). Route
handlers get a configured Storage via Depends(get_storage()) or
Depends(get_storage("avatars")) for a non-default named storage.
"""
from __future__ import annotations

from typing import Callable

from fastapi import Request

from .base import Storage
from .manager import DEFAULT_STORAGE_NAME, StorageManager


def get_storage_manager(request: Request) -> StorageManager:
    """
    Low-level dependency: fetch the StorageManager itself off app.state.

    Most route handlers should use get_storage(...) instead; this is exposed
    for code that needs the manager directly (e.g. to access multiple named
    storages dynamically based on a request parameter).
    """
    manager = getattr(request.app.state, "storage_manager", None)
    if manager is None:
        raise RuntimeError(
            "No StorageManager found on app.state.storage_manager. "
            "Set it up in your app's lifespan handler before using storage dependencies."
        )
    return manager


def get_storage(name: str = DEFAULT_STORAGE_NAME) -> Callable[[Request], Storage]:
    """
    Build a FastAPI dependency that resolves to the named Storage instance.

    Example
    -------
        @app.post("/upload")
        async def upload(
            file: UploadFile,
            storage: Storage = Depends(get_storage()),          # "default"
            avatar_storage: Storage = Depends(get_storage("avatars")),
        ):
            ...
    """

    def _dependency(request: Request) -> Storage:
        manager = get_storage_manager(request)
        return manager.get(name)

    return _dependency
