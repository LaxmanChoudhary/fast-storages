"""
PostgreSQL Large Object storage backend.

Stores file content as PostgreSQL Large Objects (server-managed binary storage
in the ``pg_largeobject`` system catalog), with a companion metadata table that
maps logical file names to Large Object OIDs plus content type, size, and
timestamps.

Why Large Objects over ``bytea``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- Streaming reads/writes (fits the Storage ABC's ``AsyncIterable[bytes]``
  contract without buffering the entire file in memory).
- No base64-encoding or escaping overhead.
- Files up to 4 TB (vs ``bytea``'s ~1 GB practical limit).
- Chunked I/O avoids loading entire files into memory.

Driver support
~~~~~~~~~~~~~~
Two async PostgreSQL drivers are supported:

- **psycopg** (v3, async mode) + ``psycopg-pool`` — preferred default.
  Install with: ``pip install fast-storages[postgresql]``

- **asyncpg** — high-performance alternative.
  Install with: ``pip install fast-storages[postgresql-asyncpg]``

If both are installed, psycopg is used by default.  Pass ``driver="asyncpg"``
(or ``driver="psycopg"``) to force a specific driver.

Schema management
~~~~~~~~~~~~~~~~~
The backend expects a metadata table to exist (default name:
``storage_files``).  Two ways to create it:

1. **Alembic / SQLAlchemy migrations** (recommended): import
   :class:`~fast-storages.backends.postgresql_schema.StorageFileMixin`
   and include it in your declarative ``Base``.  The table is then managed
   by Alembic like any other model.

2. **Auto-creation** (simple setups / tests): pass ``create_table=True``
   to the constructor and the backend will run
   ``CREATE TABLE IF NOT EXISTS`` on first use.

Required DDL (for reference)::

    CREATE TABLE IF NOT EXISTS storage_files (
        name         TEXT PRIMARY KEY,
        loid         OID  NOT NULL,
        size         BIGINT NOT NULL DEFAULT 0,
        content_type TEXT,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );

Streaming caveat
~~~~~~~~~~~~~~~~
``open()`` holds a database connection (and transaction) for the entire
duration of the read stream — Large Object file descriptors are only valid
within a transaction.  If many files are streamed concurrently, consider
increasing ``pool_max_size``.
"""
from __future__ import annotations

import asyncio
import re
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, ClassVar
from urllib.parse import urlsplit

from pydantic_settings import SettingsConfigDict

from ..base import DEFAULT_CHUNK_SIZE, SaveContent, Storage, UploadTo, resolve_upload_name
from ..config import BaseStorageSettings
from ..exceptions import (
    StorageConfigError,
    StorageConnectionError,
    StorageError,
    StorageFileNotFoundError,
    StoragePermissionError,
    StorageUnsupportedOperationError,
)
from ..files import FileMeta

# PostgreSQL Large Object open-mode flags (from libpq headers).
_INV_READ = 0x40000
_INV_WRITE = 0x20000

# ---------------------------------------------------------------------------
# Table-name validation
# ---------------------------------------------------------------------------

_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_table_name(name: str) -> str:
    """Ensure *name* is a safe, optionally schema-qualified SQL identifier."""
    parts = name.split(".")
    if len(parts) > 2:
        raise StorageConfigError(
            f"Invalid table_name {name!r}: at most one dot (schema.table) is allowed."
        )
    for part in parts:
        if not _IDENT_RE.match(part):
            raise StorageConfigError(
                f"Invalid table_name {name!r}: each component must match "
                "[a-zA-Z_][a-zA-Z0-9_]*."
            )
    return name


# ---------------------------------------------------------------------------
# Driver exception mapping
# ---------------------------------------------------------------------------


def _wrap_pg_exception(
    exc: Exception,
    *,
    name: str | None = None,
) -> StorageError:
    """Best-effort mapping of a driver exception to a StorageError subclass."""
    msg = str(exc).lower()

    if "permission denied" in msg or "insufficient privilege" in msg:
        return StoragePermissionError(
            name or "<unknown>", backend="postgresql", detail=str(exc),
        )
    if any(kw in msg for kw in ("connection", "timeout", "refused", "reset", "closed", "ssl")):
        return StorageConnectionError(backend="postgresql", detail=str(exc))
    if "does not exist" in msg and "relation" in msg:
        return StorageConfigError(
            f"Metadata table does not exist. Run Alembic migrations (see "
            f"fast_storages.backends.postgresql_schema) or pass "
            f"create_table=True: {exc}"
        )
    return StorageError(f"PostgreSQL storage error: {exc}")


# ---------------------------------------------------------------------------
# Settings (pydantic-settings)
# ---------------------------------------------------------------------------


class PostgreSQLStorageSettings(BaseStorageSettings):
    """
    Env-driven config for :class:`PostgreSQLStorage`.

    Reads ``FASTAPI_STORAGE_POSTGRESQL_*`` environment variables.
    Field names match the constructor kwargs of :class:`PostgreSQLStorage`
    exactly, so ``to_kwargs()`` can be unpacked straight into the constructor.
    """

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="FASTAPI_STORAGE_POSTGRESQL_",
        extra="ignore",
    )

    dsn: str
    table_name: str = "storage_files"
    serve_url: str | None = None
    pool_min_size: int = 2
    pool_max_size: int = 10
    chunk_size: int = DEFAULT_CHUNK_SIZE
    driver: str | None = None
    create_table: bool = False


# ---------------------------------------------------------------------------
# Internal driver abstraction
# ---------------------------------------------------------------------------
#
# Both psycopg (v3) and asyncpg are supported.  Queries are written with
# PostgreSQL-native ``$1``/``$2``/... placeholders; the psycopg adapter
# converts them to ``%s`` on the fly.
#
# psycopg is used in **sync mode** with ``asyncio.to_thread`` so it works
# on any event loop (including Windows' ProactorEventLoop).  asyncpg is
# natively async.
# ---------------------------------------------------------------------------

_DOLLAR_RE = re.compile(r"\$\d+")


def _dollar_to_percent(query: str) -> str:
    """Convert ``$1, $2, …`` placeholders to ``%s`` for psycopg."""
    return _DOLLAR_RE.sub("%s", query)


class _DriverConnection(ABC):
    """Uniform async connection wrapper (internal)."""

    @abstractmethod
    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]: ...

    @abstractmethod
    async def fetchval(self, query: str, *args: Any) -> Any: ...

    @abstractmethod
    async def fetchone(self, query: str, *args: Any) -> tuple[Any, ...] | None: ...

    @abstractmethod
    async def execute(self, query: str, *args: Any) -> None: ...


class _Driver(ABC):
    """Uniform async pool wrapper (internal)."""

    @abstractmethod
    async def open(self, dsn: str, *, min_size: int, max_size: int) -> None: ...

    @abstractmethod
    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_DriverConnection]: ...

    @abstractmethod
    async def close(self) -> None: ...


# -- psycopg (v3, sync-in-thread) adapter ------------------------------------
#
# psycopg's AsyncConnection requires a SelectorEventLoop, which is NOT the
# default on Windows (ProactorEventLoop).  Instead of forcing callers to
# change their event-loop policy, we use the sync Connection / ConnectionPool
# and dispatch every blocking call through ``asyncio.to_thread``.
#
# Connections are opened with ``autocommit=True`` so we can manage
# transactions explicitly via BEGIN / COMMIT / ROLLBACK.
# -------------------------------------------------------------------------


class _PsycopgConnection(_DriverConnection):
    __slots__ = ("_conn",)

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        await asyncio.to_thread(self._conn.execute, "BEGIN")
        try:
            yield
            await asyncio.to_thread(self._conn.execute, "COMMIT")
        except BaseException:
            await asyncio.to_thread(self._conn.execute, "ROLLBACK")
            raise

    async def fetchval(self, query: str, *args: Any) -> Any:
        def _do() -> Any:
            cur = self._conn.execute(_dollar_to_percent(query), args or None)
            row = cur.fetchone()
            return row[0] if row else None

        return await asyncio.to_thread(_do)

    async def fetchone(self, query: str, *args: Any) -> tuple[Any, ...] | None:
        def _do() -> tuple[Any, ...] | None:
            cur = self._conn.execute(_dollar_to_percent(query), args or None)
            return cur.fetchone()

        return await asyncio.to_thread(_do)

    async def execute(self, query: str, *args: Any) -> None:
        await asyncio.to_thread(
            self._conn.execute, _dollar_to_percent(query), args or None,
        )


class _PsycopgDriver(_Driver):
    def __init__(self) -> None:
        self._pool: Any = None

    async def open(self, dsn: str, *, min_size: int = 2, max_size: int = 10) -> None:
        from psycopg_pool import ConnectionPool

        def _create() -> Any:
            return ConnectionPool(
                conninfo=dsn,
                min_size=min_size,
                max_size=max_size,
                kwargs={"autocommit": True},
            )

        self._pool = await asyncio.to_thread(_create)

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_DriverConnection]:
        conn = await asyncio.to_thread(self._pool.getconn)
        try:
            yield _PsycopgConnection(conn)
        finally:
            await asyncio.to_thread(self._pool.putconn, conn)

    async def close(self) -> None:
        if self._pool is not None:
            await asyncio.to_thread(self._pool.close)


# -- asyncpg adapter --------------------------------------------------------


class _AsyncpgConnection(_DriverConnection):
    __slots__ = ("_conn",)

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        async with self._conn.transaction():
            yield

    async def fetchval(self, query: str, *args: Any) -> Any:
        return await self._conn.fetchval(query, *args)

    async def fetchone(self, query: str, *args: Any) -> tuple[Any, ...] | None:
        row = await self._conn.fetchrow(query, *args)
        return tuple(row.values()) if row else None

    async def execute(self, query: str, *args: Any) -> None:
        await self._conn.execute(query, *args)


class _AsyncpgDriver(_Driver):
    def __init__(self) -> None:
        self._pool: Any = None

    async def open(self, dsn: str, *, min_size: int = 2, max_size: int = 10) -> None:
        import asyncpg

        self._pool = await asyncpg.create_pool(
            dsn, min_size=min_size, max_size=max_size,
        )

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_DriverConnection]:
        async with self._pool.acquire() as conn:
            yield _AsyncpgConnection(conn)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()


# -- factory -----------------------------------------------------------------


def _create_driver(driver_name: str | None = None) -> _Driver:
    """
    Instantiate the requested driver, or auto-detect the installed one.

    Raises :class:`ImportError` with install instructions when no suitable
    driver package is available.
    """
    if driver_name == "psycopg":
        try:
            import psycopg  # noqa: F401
            import psycopg_pool  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "psycopg driver requires psycopg and psycopg-pool. "
                "Install with: pip install fast-storages[postgresql]"
            ) from exc
        return _PsycopgDriver()

    if driver_name == "asyncpg":
        try:
            import asyncpg  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "asyncpg driver requires the asyncpg package. "
                "Install with: pip install fast-storages[postgresql-asyncpg]"
            ) from exc
        return _AsyncpgDriver()

    if driver_name is not None:
        raise StorageConfigError(
            f"Unknown PostgreSQL driver {driver_name!r}; "
            "accepted values are 'psycopg' and 'asyncpg'."
        )

    # Auto-detect: prefer psycopg, fall back to asyncpg.
    try:
        import psycopg  # noqa: F401
        import psycopg_pool  # noqa: F401

        return _PsycopgDriver()
    except ImportError:
        pass

    try:
        import asyncpg  # noqa: F401

        return _AsyncpgDriver()
    except ImportError:
        pass

    raise ImportError(
        "PostgreSQLStorage requires psycopg (v3) + psycopg-pool, or asyncpg. "
        "Install with:\n"
        "  pip install fast-storages[postgresql]          # psycopg (recommended)\n"
        "  pip install fast-storages[postgresql-asyncpg]  # asyncpg"
    )


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------


class PostgreSQLStorage(Storage):
    """
    PostgreSQL Large Object storage backend.

    Parameters
    ----------
    dsn:
        PostgreSQL connection string,
        e.g. ``"postgresql://user:pass@localhost:5432/mydb"``.
    table_name:
        Name of the metadata table (default ``"storage_files"``).
        May be schema-qualified (``"myschema.storage_files"``).
    base_url:
        Optional URL prefix for :meth:`url` / :meth:`full_url`.  Without it
        both methods raise
        :class:`~fast_storages.exceptions.StorageUnsupportedOperationError`.
    pool_min_size:
        Minimum connections in the pool (default 2).
    pool_max_size:
        Maximum connections in the pool (default 10).
    chunk_size:
        Default read/write chunk size for Large Object streaming
        (default 64 KiB).
    driver:
        ``"psycopg"`` or ``"asyncpg"``.  ``None`` (default) auto-detects
        whichever is installed, preferring psycopg.
    create_table:
        If ``True``, run ``CREATE TABLE IF NOT EXISTS`` on first use.
        For production, prefer Alembic migrations via
        :class:`~fast_storages.backends.postgresql_schema.StorageFileMixin`.
    """

    backend_name = "postgresql"

    def __init__(
        self,
        dsn: str,
        *,
        table_name: str = "storage_files",
        serve_url: str | None = None,
        pool_min_size: int = 2,
        pool_max_size: int = 10,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        driver: str | None = None,
        create_table: bool = False,
    ) -> None:
        self._dsn = dsn
        self._table_name = _validate_table_name(table_name)
        self.serve_url = serve_url.rstrip("/") if serve_url else None

        if not self.serve_url:
            import warnings
            warnings.warn(
                "PostgreSQL storage backend is configured without 'serve_url'. "
                "As a result, url() will not work.",
                UserWarning,
                stacklevel=2,
            )
        else:
            parsed = urlsplit(self.serve_url)
            if not parsed.scheme or not parsed.netloc:
                import warnings
                warnings.warn(
                    f"PostgreSQL storage backend configured 'serve_url' {self.serve_url!r} without a scheme and host. "
                    "As a result, url() will not work.",
                    UserWarning,
                    stacklevel=2,
                )
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._chunk_size = chunk_size
        self._create_table = create_table

        # Driver detection happens eagerly (matches S3/Azure pattern of
        # failing fast when the required SDK is missing).
        self._driver: _Driver = _create_driver(driver)
        self._pool_opened = False
        self._pool_lock = asyncio.Lock()

    # -- internal helpers ----------------------------------------------------

    async def _ensure_pool(self) -> None:
        """Lazily open the connection pool (and optionally create the table)."""
        if self._pool_opened:
            return
        async with self._pool_lock:
            if self._pool_opened:  # double-check after acquiring lock
                return
            try:
                await self._driver.open(
                    self._dsn,
                    min_size=self._pool_min_size,
                    max_size=self._pool_max_size,
                )
            except ImportError:
                raise
            except Exception as exc:
                raise StorageConnectionError(
                    backend="postgresql",
                    detail=f"Failed to create connection pool: {exc}",
                ) from exc

            if self._create_table:
                try:
                    async with self._driver.acquire() as conn:
                        async with conn.transaction():
                            await conn.execute(self._ddl())
                except StorageError:
                    raise
                except Exception as exc:
                    raise _wrap_pg_exception(exc) from exc

            self._pool_opened = True

    def _ddl(self) -> str:
        """Return the ``CREATE TABLE IF NOT EXISTS`` statement."""
        t = self._table_name
        return (
            f"CREATE TABLE IF NOT EXISTS {t} ("
            f"  name         TEXT PRIMARY KEY,"
            f"  loid         OID  NOT NULL,"
            f"  size         BIGINT NOT NULL DEFAULT 0,"
            f"  content_type TEXT,"
            f"  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),"
            f"  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()"
            f")"
        )

    # -- Storage ABC implementation ------------------------------------------

    async def save(
        self,
        name: str,
        content: SaveContent,
        *,
        content_type: str | None = None,
        upload_to: UploadTo = None,
        context: dict[str, Any] | None = None,
    ) -> FileMeta:
        resolved = resolve_upload_name(name, upload_to, context)
        if not resolved or resolved.strip() in ("", ".", ".."):
            raise StorageConfigError(f"Invalid storage name: {resolved!r}")

        await self._ensure_pool()
        t = self._table_name

        try:
            async with self._driver.acquire() as conn:
                async with conn.transaction():
                    # If the file already exists, unlink the old Large Object.
                    old_oid = await conn.fetchval(
                        f"SELECT loid FROM {t} WHERE name = $1", resolved,
                    )
                    if old_oid is not None:
                        await conn.execute("SELECT lo_unlink($1)", old_oid)
                        await conn.execute(
                            f"DELETE FROM {t} WHERE name = $1", resolved,
                        )

                    # Create a new Large Object and write content in chunks.
                    oid = await conn.fetchval("SELECT lo_create(0)")
                    fd = await conn.fetchval(
                        "SELECT lo_open($1, $2)", oid, _INV_WRITE,
                    )

                    total_size = 0
                    if isinstance(content, bytes):
                        mv = memoryview(content)
                        offset = 0
                        while offset < len(mv):
                            chunk = bytes(mv[offset : offset + self._chunk_size])
                            await conn.fetchval(
                                "SELECT lowrite($1, $2)", fd, chunk,
                            )
                            total_size += len(chunk)
                            offset += len(chunk)
                    else:
                        async for chunk in content:
                            await conn.fetchval(
                                "SELECT lowrite($1, $2)", fd, chunk,
                            )
                            total_size += len(chunk)

                    await conn.execute("SELECT lo_close($1)", fd)

                    # Upsert metadata row.
                    await conn.execute(
                        f"INSERT INTO {t}"
                        f"  (name, loid, size, content_type, created_at, updated_at)"
                        f" VALUES ($1, $2, $3, $4, now(), now())"
                        f" ON CONFLICT (name) DO UPDATE SET"
                        f"  loid = EXCLUDED.loid,"
                        f"  size = EXCLUDED.size,"
                        f"  content_type = EXCLUDED.content_type,"
                        f"  updated_at = now()",
                        resolved, oid, total_size, content_type,
                    )
        except StorageError:
            raise
        except Exception as exc:
            raise _wrap_pg_exception(exc, name=resolved) from exc

        return FileMeta(
            name=name,
            key=resolved,
            size=total_size,
            content_type=content_type,
            backend=self.backend_name,
        )

    async def open(
        self, name: str, *, chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> AsyncIterator[bytes]:
        await self._ensure_pool()
        t = self._table_name

        # Eagerly validate existence so callers get StorageFileNotFoundError
        # from open() itself, not lazily during iteration.
        try:
            async with self._driver.acquire() as conn:
                row = await conn.fetchone(
                    f"SELECT loid FROM {t} WHERE name = $1", name,
                )
        except StorageError:
            raise
        except Exception as exc:
            raise _wrap_pg_exception(exc, name=name) from exc

        if row is None:
            raise StorageFileNotFoundError(name, backend="postgresql")

        oid = row[0]
        driver = self._driver  # capture for the generator closure

        async def _stream() -> AsyncIterator[bytes]:
            try:
                async with driver.acquire() as sconn:
                    async with sconn.transaction():
                        fd = await sconn.fetchval(
                            "SELECT lo_open($1, $2)", oid, _INV_READ,
                        )
                        try:
                            while True:
                                data: bytes | None = await sconn.fetchval(
                                    "SELECT loread($1, $2)", fd, chunk_size,
                                )
                                if not data:
                                    break
                                yield data
                        finally:
                            await sconn.execute("SELECT lo_close($1)", fd)
            except StorageError:
                raise
            except Exception as exc:
                raise _wrap_pg_exception(exc, name=name) from exc

        return _stream()

    async def delete(self, name: str) -> None:
        await self._ensure_pool()
        t = self._table_name

        try:
            async with self._driver.acquire() as conn:
                async with conn.transaction():
                    oid = await conn.fetchval(
                        f"SELECT loid FROM {t} WHERE name = $1", name,
                    )
                    if oid is None:
                        return  # idempotent — matches Storage.delete contract
                    await conn.execute("SELECT lo_unlink($1)", oid)
                    await conn.execute(
                        f"DELETE FROM {t} WHERE name = $1", name,
                    )
        except StorageError:
            raise
        except Exception as exc:
            raise _wrap_pg_exception(exc, name=name) from exc

    async def exists(self, name: str) -> bool:
        await self._ensure_pool()
        t = self._table_name

        try:
            async with self._driver.acquire() as conn:
                result = await conn.fetchval(
                    f"SELECT 1 FROM {t} WHERE name = $1", name,
                )
        except StorageError:
            raise
        except Exception as exc:
            raise _wrap_pg_exception(exc, name=name) from exc

        return result is not None

    async def size(self, name: str) -> int:
        await self._ensure_pool()
        t = self._table_name

        try:
            async with self._driver.acquire() as conn:
                result = await conn.fetchval(
                    f"SELECT size FROM {t} WHERE name = $1", name,
                )
        except StorageError:
            raise
        except Exception as exc:
            raise _wrap_pg_exception(exc, name=name) from exc

        if result is None:
            raise StorageFileNotFoundError(name, backend="postgresql")
        return int(result)

    async def url(self, name: str, *, expires_in: int | None = None) -> str:
        if self.serve_url is None:
            raise StorageUnsupportedOperationError(
                "url",
                backend="postgresql",
                reason="serve_url was not configured",
            )
        if expires_in is not None:
            raise StorageUnsupportedOperationError(
                "url(expires_in=...)",
                backend="postgresql",
                reason="PostgreSQL backend has no concept of expiring URLs",
            )
        parsed = urlsplit(self.serve_url)
        if not parsed.scheme or not parsed.netloc:
            raise StorageUnsupportedOperationError(
                "url",
                backend="postgresql",
                reason=(
                    f"serve_url must be an absolute URL containing scheme and host (got {self.serve_url!r})."
                ),
            )
        cleaned = name.replace("\\", "/").lstrip("/")
        return f"{self.serve_url}/{cleaned}"

    async def aclose(self) -> None:
        """Close the connection pool and release resources."""
        if self._pool_opened:
            await self._driver.close()
            self._pool_opened = False
