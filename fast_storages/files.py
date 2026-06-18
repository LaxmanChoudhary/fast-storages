"""
Adapters between FastAPI/Starlette upload objects and the Storage.save()
SaveContent contract (bytes | AsyncIterable[bytes]).

starlette.datastructures.UploadFile exposes `.read(size)` / async iteration
via `__aiter__` in recent Starlette versions, but pinning behavior to
whatever Starlette version happens to be installed is fragile. This module
wraps it explicitly so the rest of the package -- and backends -- never deal
with UploadFile directly.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator, Optional

from .base import DEFAULT_CHUNK_SIZE


@dataclass(frozen=True)
class FileMeta:
    """Lightweight metadata describing a stored or to-be-stored file."""

    name: str
    size: int | None = None
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None  # ISO 8601; kept as str to avoid forcing a tz lib choice on callers


class UploadFileReader:
    """
    Wraps a Starlette/FastAPI UploadFile (or any object exposing an async
    `.read(size: int) -> bytes` method) as an AsyncIterator[bytes], so it can
    be passed directly to Storage.save().

    Example
    -------
        @app.post("/upload")
        async def upload(file: UploadFile, storage: Storage = Depends(get_storage)):
            reader = UploadFileReader(file)
            await storage.save(file.filename, reader, content_type=file.content_type)
    """

    def __init__(self, upload_file: "object", chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        self._upload_file = upload_file
        self._chunk_size = chunk_size

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[bytes]:
        read = getattr(self._upload_file, "read", None)
        if read is None:
            raise TypeError(
                f"{type(self._upload_file).__name__!r} has no .read() method; "
                "UploadFileReader requires an UploadFile-like object."
            )
        while True:
            chunk = await read(self._chunk_size)
            if not chunk:
                break
            yield chunk


async def read_all(content_iter: AsyncIterator[bytes]) -> bytes:
    """
    Drain an AsyncIterator[bytes] into a single bytes object.

    Useful for small files or in tests. Backends should generally avoid
    calling this internally for save() -- defeats the purpose of streaming --
    but it's handy for callers who explicitly want the full buffer (e.g. to
    compute a checksum before upload).
    """
    chunks: list[bytes] = []
    async for chunk in content_iter:
        chunks.append(chunk)
    return b"".join(chunks)


def guess_content_type(filename: str, fallback: Optional[str] = None) -> str | None:
    """Best-effort MIME type guess from filename extension."""
    import mimetypes

    guessed, _ = mimetypes.guess_type(filename)
    return guessed or fallback
