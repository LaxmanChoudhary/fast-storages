"""
Local filesystem storage backend.

This is the "default" backend, analogous to Django's FileSystemStorage:
useful for development, tests, and single-server deployments. Files are
written under `base_path`; `url()` returns a path relative to `base_url`
(the caller is responsible for actually serving that directory, e.g. via
StaticFiles in FastAPI -- this backend does not run a web server).

base_url may be either a path ("/media") or a full origin
("https://cdn.example.com/media"). url() always returns base_url + name
regardless of which form was given. full_url() additionally requires
base_url to include a scheme+host -- if base_url is just a path, full_url()
raises StorageUnsupportedOperationError since this backend has no way to
know what domain it's served from.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, AsyncIterator, ClassVar
from urllib.parse import urlsplit

import aiofiles
import aiofiles.os

from ..base import DEFAULT_CHUNK_SIZE, SaveContent, Storage, UploadTo, resolve_upload_name
from ..config import BaseStorageSettings
from ..exceptions import (
    StorageConfigError,
    StorageFileNotFoundError,
    StoragePermissionError,
    StorageUnsupportedOperationError,
)
from ..files import FileMeta
from pydantic_settings import SettingsConfigDict


class LocalStorageSettings(BaseStorageSettings):
    """
    Env-driven config for LocalStorage.

    Reads FASTAPI_STORAGE_LOCAL_BASE_PATH / FASTAPI_STORAGE_LOCAL_BASE_URL
    by default. Field names match LocalStorage's constructor kwargs exactly.
    """

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="FASTAPI_STORAGE_LOCAL_",
        extra="ignore",
    )

    base_path: str
    base_url: str | None = None


def _resolve_safe_path(base_path: Path, name: str) -> Path:
    """
    Join `name` onto `base_path` and guarantee the result cannot escape
    `base_path` via "../" segments, absolute paths, or symlink tricks.

    Raises StoragePermissionError if the resolved path would land outside
    base_path -- this is a security boundary, not just a usability check.
    """
    if not name or name.strip() in ("", ".", ".."):
        raise StorageConfigError(f"Invalid storage name: {name!r}")

    # normalize separators, strip any leading slash so name is always
    # treated as relative to base_path regardless of how it was supplied
    cleaned = name.replace("\\", "/").lstrip("/")
    candidate = (base_path / cleaned).resolve()
    resolved_base = base_path.resolve()

    try:
        candidate.relative_to(resolved_base)
    except ValueError:
        raise StoragePermissionError(
            name, backend="local", detail="resolved path escapes base_path"
        ) from None

    return candidate


class LocalStorage(Storage):
    """
    Filesystem-backed Storage implementation.

    Parameters
    ----------
    base_path:
        Root directory files are stored under. Created if it doesn't exist.
    base_url:
        Optional URL prefix used by url()/full_url(). May be a path
        ("/media") or a full origin ("https://cdn.example.com/media"). If
        None, url() and full_url() raise StorageUnsupportedOperationError --
        there's no way to serve the files without knowing how they're
        exposed over HTTP. full_url() additionally requires a scheme+host.

    save() accepts upload_to (str prefix or callable) to compute the final
    stored path -- see resolve_upload_name() in base.py.
    """

    backend_name = "local"

    def __init__(self, base_path: str | os.PathLike[str], base_url: str | None = None) -> None:
        self.base_path = Path(base_path)
        self.base_url = base_url.rstrip("/") if base_url else None
        self.base_path.mkdir(parents=True, exist_ok=True)

    async def save(
        self,
        name: str,
        content: SaveContent,
        *,
        content_type: str | None = None,
        upload_to: UploadTo = None,
        context: dict[str, Any] | None = None,
    ) -> FileMeta:
        # content_type is accepted for interface compatibility but local
        # filesystem has no native metadata slot to put it in; silently
        # ignored, matching Django's FileSystemStorage behavior.
        resolved_name = resolve_upload_name(name, upload_to, context)
        path = _resolve_safe_path(self.base_path, resolved_name)
        path.parent.mkdir(parents=True, exist_ok=True)

        total_size = 0
        try:
            async with aiofiles.open(path, "wb") as f:
                if isinstance(content, bytes):
                    await f.write(content)
                    total_size = len(content)
                else:
                    async for chunk in content:
                        await f.write(chunk)
                        total_size += len(chunk)
        except PermissionError as exc:
            raise StoragePermissionError(resolved_name, backend="local", detail=str(exc)) from exc

        return FileMeta(
            name=name,
            key=resolved_name,
            size=total_size,
            content_type=content_type,
            backend=self.backend_name,
        )

    async def open(self, name: str, *, chunk_size: int = DEFAULT_CHUNK_SIZE) -> AsyncIterator[bytes]:
        path = _resolve_safe_path(self.base_path, name)
        if not path.is_file():
            raise StorageFileNotFoundError(name, backend="local")

        async def _generator() -> AsyncIterator[bytes]:
            try:
                async with aiofiles.open(path, "rb") as f:
                    while True:
                        chunk = await f.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk
            except PermissionError as exc:
                raise StoragePermissionError(name, backend="local", detail=str(exc)) from exc

        return _generator()

    async def delete(self, name: str) -> None:
        path = _resolve_safe_path(self.base_path, name)
        try:
            await aiofiles.os.remove(path)
        except FileNotFoundError:
            # idempotent delete, per Storage.delete contract
            return
        except PermissionError as exc:
            raise StoragePermissionError(name, backend="local", detail=str(exc)) from exc

    async def exists(self, name: str) -> bool:
        path = _resolve_safe_path(self.base_path, name)
        return await aiofiles.os.path.isfile(path)

    async def size(self, name: str) -> int:
        path = _resolve_safe_path(self.base_path, name)
        try:
            stat_result = await aiofiles.os.stat(path)
        except FileNotFoundError as exc:
            raise StorageFileNotFoundError(name, backend="local") from exc
        return stat_result.st_size

    async def url(self, name: str, *, expires_in: int | None = None) -> str:
        if self.base_url is None:
            raise StorageUnsupportedOperationError(
                "url", backend="local", reason="base_url was not configured"
            )
        if expires_in is not None:
            raise StorageUnsupportedOperationError(
                "url(expires_in=...)",
                backend="local",
                reason="local filesystem backend has no concept of expiring URLs",
            )
        cleaned = name.replace("\\", "/").lstrip("/")
        return f"{self.base_url}/{cleaned}"

    async def full_url(self, name: str, *, expires_in: int | None = None) -> str:
        relative = await self.url(name, expires_in=expires_in)
        parsed = urlsplit(self.base_url or "")
        if not parsed.scheme or not parsed.netloc:
            raise StorageUnsupportedOperationError(
                "full_url",
                backend="local",
                reason=(
                    "base_url has no scheme/host to build an absolute URI from "
                    f"(got {self.base_url!r}); configure base_url as a full origin, "
                    "e.g. 'https://cdn.example.com/media', to use full_url()."
                ),
            )
        return relative