"""
Core storage abstraction.

Every backend (local, s3, azure, gcs, dropbox, ...) implements this interface.
Calling code (route handlers, services) depends ONLY on this module, never on
a concrete backend or a provider SDK. That's the whole point of the package.

Design notes
------------
- Overwrite-on-collision: save() writes to exactly the name given, overwriting
  any existing object at that name. No collision-avoidance / renaming logic
  lives in this version of the contract. (Easy to add later as an opt-in
  policy layered on top, without breaking this interface.)
- Streaming-first: save() accepts an async iterable of bytes (or plain bytes)
  so large uploads are not forced into memory, and open() returns an async
  iterator for the same reason. UploadFile from FastAPI satisfies the
  AsyncIterable[bytes] shape via its `.read()` chunking, see adapters in
  files.py.
- url() is intentionally allowed to raise StorageUnsupportedOperationError.
  Local filesystem has no native "URL" concept; don't make every backend
  pretend it does.
- All methods are coroutines. No sync/async dual API and no executor-wrapping
  baked into the interface -- if a future backend only has a sync SDK, that
  backend's implementation can wrap calls in run_in_executor internally, but
  callers never need to know that.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, AsyncIterable, AsyncIterator, Callable, Union

if TYPE_CHECKING:
    from .files import FileMeta

# What save() accepts. bytes for small/in-memory content, AsyncIterable[bytes]
# for streamed/large content (file uploads, generators reading from disk, etc).
SaveContent = Union[bytes, AsyncIterable[bytes]]

DEFAULT_CHUNK_SIZE = 64 * 1024  # 64 KiB, used by backends that need to chunk reads

# ---------------------------------------------------------------------------
# Upload-to helpers: allow callers to control where a file lands
# ---------------------------------------------------------------------------

# UploadTo controls how save() computes the final storage path from the
# caller-supplied `name`.
#
# • None          → use `name` as-is.
# • str           → treat as a directory prefix, e.g. "avatars" turns
#                    "photo.png" into "avatars/photo.png".
# • callable      → full custom logic; receives (name, context) and returns
#                    the resolved name.  `context` is an optional dict the
#                    caller can pass through save() for use in the callable
#                    (e.g. a user ID, a request object, ...).
UploadTo = Union[str, Callable[[str, "dict[str, Any] | None"], str], None]


def resolve_upload_name(
    name: str,
    upload_to: UploadTo = None,
    context: dict[str, Any] | None = None,
) -> str:
    """
    Apply an *upload_to* directive to *name* and return the resolved path.

    Parameters
    ----------
    name:
        The original filename supplied by the caller (e.g. ``"photo.png"``).
    upload_to:
        - ``None`` → return *name* unchanged.
        - ``str``  → treated as a directory prefix; the result is
          ``"{upload_to}/{name}"``, with any trailing slashes on the prefix
          normalised away.
        - callable → called as ``upload_to(name, context)``; must return the
          final storage-relative name as a ``str``.
    context:
        Arbitrary caller-supplied data forwarded to a callable *upload_to*.
        Ignored when *upload_to* is ``None`` or a ``str``.

    Returns
    -------
    str
        The resolved storage-relative path/key.
    """
    if upload_to is None:
        return name
    if isinstance(upload_to, str):
        prefix = upload_to.rstrip("/")
        return f"{prefix}/{name}"
    # callable
    return upload_to(name, context)


class Storage(ABC):
    """
    Abstract base class for all storage backends.

    Subclasses must implement every abstractmethod below. Each backend module
    should also expose a matching `*StorageSettings` (see config.py) used for
    env-driven configuration, but the constructor itself should accept plain
    kwargs so the backend can always be built directly without Settings.
    """

    #: Short, stable identifier for this backend, e.g. "local", "s3", "azure".
    #: Used in error messages and by the registry. Subclasses must set this.
    backend_name: str = "base"

    @abstractmethod
    async def save(
        self,
        name: str,
        content: SaveContent,
        *,
        content_type: str | None = None,
        upload_to: UploadTo = None,
        context: dict[str, Any] | None = None,
    ) -> "FileMeta":
        """
        Write `content` to `name`, overwriting any existing object at that path.

        Parameters
        ----------
        name:
            Storage-relative path/key, e.g. "avatars/user_42.png". Backends
            are responsible for any path normalization/sanitization.
        content:
            Raw bytes, or an async iterable of bytes chunks (e.g. an
            UploadFile wrapper). Backends should not assume the whole
            payload fits in memory.
        content_type:
            Optional MIME type. Backends that support storing metadata
            (S3, Azure, GCS) should persist it; backends that can't (plain
            local filesystem) may ignore it.
        upload_to:
            Controls how the final storage path is computed from *name*.
            ``None`` uses *name* as-is, a ``str`` is treated as a directory
            prefix, and a callable receives ``(name, context)`` and returns
            the resolved name. See :func:`resolve_upload_name`.
        context:
            Arbitrary caller-supplied data forwarded to a callable
            *upload_to*. Ignored when *upload_to* is ``None`` or a ``str``.

        Returns
        -------
        FileMeta
            A :class:`~fast_storages.files.FileMeta` instance describing the
            saved file, including the resolved storage key, size in bytes,
            content type, and backend identifier.

        Raises
        ------
        StoragePermissionError, StorageConnectionError, StorageError
        """
        raise NotImplementedError

    @abstractmethod
    async def open(self, name: str, *, chunk_size: int = DEFAULT_CHUNK_SIZE) -> AsyncIterator[bytes]:
        """
        Stream the content stored at `name`.

        Yields chunks of bytes. Backends should not load the entire object
        into memory before yielding the first chunk where the underlying
        SDK supports true streaming (S3/Azure/GCS range/stream reads); the
        local backend reads the file in `chunk_size` blocks.

        Raises
        ------
        StorageFileNotFoundError
            If `name` does not exist.
        StoragePermissionError, StorageConnectionError, StorageError
        """
        raise NotImplementedError

    @abstractmethod
    async def delete(self, name: str) -> None:
        """
        Delete the object at `name`.

        Implementations should be idempotent: deleting a name that does not
        exist should NOT raise StorageFileNotFoundError (mirrors Django's
        Storage.delete behavior, and avoids forcing callers to catch errors
        for what is usually a "make sure it's gone" intent).

        Raises
        ------
        StoragePermissionError, StorageConnectionError, StorageError
        """
        raise NotImplementedError

    @abstractmethod
    async def exists(self, name: str) -> bool:
        """
        Return True if an object is stored at `name`.

        Raises
        ------
        StoragePermissionError, StorageConnectionError, StorageError
            (but never StorageFileNotFoundError -- that's what this method
            answers, it shouldn't raise it)
        """
        raise NotImplementedError

    @abstractmethod
    async def size(self, name: str) -> int:
        """
        Return the size of the object at `name`, in bytes.

        Raises
        ------
        StorageFileNotFoundError
            If `name` does not exist.
        StoragePermissionError, StorageConnectionError, StorageError
        """
        raise NotImplementedError

    @abstractmethod
    async def url(self, name: str, *, expires_in: int | None = None) -> str:
        """
        Return a URL that can be used to access `name`.

        Parameters
        ----------
        expires_in:
            Seconds until a generated URL expires. Backends that only
            support permanent/public URLs (or none at all) should raise
            StorageUnsupportedOperationError if a caller asks for an
            expiring URL they can't provide, rather than silently ignoring
            the parameter.

        Raises
        ------
        StorageFileNotFoundError
            If `name` does not exist (backends MAY skip this check for
            performance and let it surface lazily when the URL is used;
            document the chosen behavior per backend).
        StorageUnsupportedOperationError
            If this backend cannot produce a URL at all, or cannot honor
            `expires_in`.
        StoragePermissionError, StorageConnectionError, StorageError
        """
        raise NotImplementedError

    @abstractmethod
    async def full_url(self, name: str, *, expires_in: int | None = None) -> str:
        """
        Return a fully-qualified (absolute) URL for `name`.

        Unlike :meth:`url`, which may return a relative path (e.g.
        ``/media/photo.png`` for the local backend), ``full_url()`` always
        returns a URL that includes scheme and host.

        For cloud backends (S3, Azure, GCS) whose URLs are inherently
        absolute, this typically just delegates to :meth:`url`. For the
        local backend, ``full_url()`` requires ``base_url`` to include a
        scheme+host and raises
        :class:`~fast_storages.exceptions.StorageUnsupportedOperationError`
        if it doesn't.

        Parameters
        ----------
        expires_in:
            Same semantics as :meth:`url`.

        Raises
        ------
        StorageUnsupportedOperationError
            If this backend cannot produce an absolute URL (e.g. the local
            backend when ``base_url`` is only a path, not a full origin).
        StorageFileNotFoundError
            If ``name`` does not exist (same caveats as :meth:`url`).
        StoragePermissionError, StorageConnectionError, StorageError
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Optional lifecycle hooks. Default no-ops; backends with connection
    # pools / SDK clients (S3, Azure) should override these.
    # ------------------------------------------------------------------

    async def aclose(self) -> None:
        """
        Release any held resources (HTTP sessions, client pools).

        Default is a no-op. Backends that open persistent clients (aioboto3
        sessions, azure aio BlobServiceClient) MUST override this. Intended
        to be called from an app shutdown hook / lifespan context manager.
        """
        return None

    async def __aenter__(self) -> "Storage":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        return f"<{type(self).__name__} backend_name={self.backend_name!r}>"
