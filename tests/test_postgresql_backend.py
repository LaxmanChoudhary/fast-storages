"""
Tests for the PostgreSQL Large Object storage backend.

Unit tests (registration, settings, URL behavior, table-name validation) run
without a database or driver packages.  Integration tests require a live
PostgreSQL instance; set the ``FASTAPI_STORAGE_POSTGRESQL_DSN`` environment
variable to enable them.

Run with::

    pytest tests/test_postgresql_backend.py -v
"""
from __future__ import annotations

import os

import pytest

import fast_storages as fs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _has_pg_driver() -> bool:
    """Return True if at least one async PG driver is importable."""
    try:
        import psycopg  # noqa: F401
        import psycopg_pool  # noqa: F401

        return True
    except ImportError:
        pass
    try:
        import asyncpg  # noqa: F401

        return True
    except ImportError:
        pass
    return False


_HAS_DRIVER = _has_pg_driver()
_SKIP_NO_DRIVER = "No PostgreSQL async driver (psycopg or asyncpg) installed"

_HAS_DSN = bool(os.environ.get("FASTAPI_STORAGE_POSTGRESQL_DSN"))
_SKIP_NO_DSN = "Set FASTAPI_STORAGE_POSTGRESQL_DSN to run PostgreSQL integration tests"


# ===================================================================
# Unit tests — no database or driver required
# ===================================================================


def test_postgresql_backend_registered():
    """The backend is discoverable via the registry."""
    assert "postgresql" in fs.list_registered_backends()


def test_postgresql_storage_settings_to_kwargs(monkeypatch):
    monkeypatch.setenv("FASTAPI_STORAGE_POSTGRESQL_DSN", "postgresql://u:p@localhost/testdb")
    monkeypatch.setenv("FASTAPI_STORAGE_POSTGRESQL_TABLE_NAME", "my_files")
    monkeypatch.setenv("FASTAPI_STORAGE_POSTGRESQL_SERVE_URL", "http://localhost:8000/files")

    from fast_storages.backends.postgresql import PostgreSQLStorageSettings

    settings = PostgreSQLStorageSettings()
    kwargs = settings.to_kwargs()

    assert kwargs["dsn"] == "postgresql://u:p@localhost/testdb"
    assert kwargs["table_name"] == "my_files"
    assert kwargs["serve_url"] == "http://localhost:8000/files"


def test_postgresql_storage_settings_defaults(monkeypatch):
    monkeypatch.setenv("FASTAPI_STORAGE_POSTGRESQL_DSN", "postgresql://localhost/db")

    from fast_storages.backends.postgresql import PostgreSQLStorageSettings

    settings = PostgreSQLStorageSettings()
    kwargs = settings.to_kwargs()

    assert kwargs["dsn"] == "postgresql://localhost/db"
    assert kwargs["table_name"] == "storage_files"
    # None values are dropped by to_kwargs()
    assert "serve_url" not in kwargs
    assert "driver" not in kwargs


def test_invalid_table_name_raises():
    """Table name validation fires before driver detection."""
    from fast_storages.backends.postgresql import _validate_table_name

    with pytest.raises(fs.StorageConfigError, match="Invalid table_name"):
        _validate_table_name("bad table!")

    with pytest.raises(fs.StorageConfigError, match="Invalid table_name"):
        _validate_table_name("a.b.c.d")

    with pytest.raises(fs.StorageConfigError, match="Invalid table_name"):
        _validate_table_name("123start")


def test_schema_qualified_table_name_accepted():
    from fast_storages.backends.postgresql import _validate_table_name

    assert _validate_table_name("myschema.storage_files") == "myschema.storage_files"
    assert _validate_table_name("storage_files") == "storage_files"
    assert _validate_table_name("_private.my_table") == "_private.my_table"


def test_unknown_driver_raises():
    """Requesting a non-existent driver name is a config error, not ImportError."""
    from fast_storages.backends.postgresql import _create_driver

    with pytest.raises(fs.StorageConfigError, match="Unknown PostgreSQL driver"):
        _create_driver("mysql")


# ===================================================================
# Unit tests — require a PG driver (construct PostgreSQLStorage)
# ===================================================================


@pytest.mark.skipif(not _HAS_DRIVER, reason=_SKIP_NO_DRIVER)
@pytest.mark.asyncio
async def test_postgresql_backend_warning_without_serve_url():
    from fast_storages.backends.postgresql import PostgreSQLStorage

    with pytest.warns(UserWarning, match="configured without 'serve_url'"):
        PostgreSQLStorage(dsn="postgresql://localhost/db")


@pytest.mark.skipif(not _HAS_DRIVER, reason=_SKIP_NO_DRIVER)
@pytest.mark.asyncio
async def test_postgresql_backend_warning_relative_serve_url():
    from fast_storages.backends.postgresql import PostgreSQLStorage

    with pytest.warns(UserWarning, match="without a scheme and host"):
        PostgreSQLStorage(dsn="postgresql://localhost/db", serve_url="/files")


@pytest.mark.skipif(not _HAS_DRIVER, reason=_SKIP_NO_DRIVER)
@pytest.mark.asyncio
async def test_url_without_serve_url_raises():
    from fast_storages.backends.postgresql import PostgreSQLStorage

    with pytest.warns(UserWarning):
        storage = PostgreSQLStorage(dsn="postgresql://localhost/db")
    with pytest.raises(fs.StorageUnsupportedOperationError):
        await storage.url("somefile.txt")


@pytest.mark.skipif(not _HAS_DRIVER, reason=_SKIP_NO_DRIVER)
@pytest.mark.asyncio
async def test_url_with_relative_serve_url_raises():
    from fast_storages.backends.postgresql import PostgreSQLStorage

    with pytest.warns(UserWarning):
        storage = PostgreSQLStorage(dsn="postgresql://localhost/db", serve_url="/files")
    with pytest.raises(fs.StorageUnsupportedOperationError):
        await storage.url("docs/readme.txt")


@pytest.mark.skipif(not _HAS_DRIVER, reason=_SKIP_NO_DRIVER)
@pytest.mark.asyncio
async def test_url_with_serve_url():
    from fast_storages.backends.postgresql import PostgreSQLStorage

    storage = PostgreSQLStorage(dsn="postgresql://localhost/db", serve_url="http://localhost:8000/files")
    assert await storage.url("docs/readme.txt") == "http://localhost:8000/files/docs/readme.txt"


@pytest.mark.skipif(not _HAS_DRIVER, reason=_SKIP_NO_DRIVER)
@pytest.mark.asyncio
async def test_url_with_expires_in_raises():
    from fast_storages.backends.postgresql import PostgreSQLStorage

    storage = PostgreSQLStorage(dsn="postgresql://localhost/db", serve_url="http://localhost:8000/files")
    with pytest.raises(fs.StorageUnsupportedOperationError):
        await storage.url("a.txt", expires_in=60)


@pytest.mark.skipif(not _HAS_DRIVER, reason=_SKIP_NO_DRIVER)
def test_repr():
    from fast_storages.backends.postgresql import PostgreSQLStorage

    with pytest.warns(UserWarning):
        storage = PostgreSQLStorage(dsn="postgresql://localhost/db")
    assert "postgresql" in repr(storage)


# ===================================================================
# Integration tests — require FASTAPI_STORAGE_POSTGRESQL_DSN + driver
# ===================================================================


@pytest.fixture
async def pg_storage():
    """Create a PostgreSQLStorage with an auto-created test table."""
    from fast_storages.backends.postgresql import PostgreSQLStorage

    dsn = os.environ["FASTAPI_STORAGE_POSTGRESQL_DSN"]
    storage = PostgreSQLStorage(
        dsn=dsn,
        table_name="test_storage_files",
        create_table=True,
    )

    yield storage

    # Clean up: drop the test table and close the pool.
    try:
        await storage._ensure_pool()
        async with storage._driver.acquire() as conn:
            async with conn.transaction():
                # Unlink any remaining large objects owned by test rows.
                rows: list[tuple[Any, ...]] = []
                row = await conn.fetchone(
                    "SELECT loid FROM test_storage_files",
                )
                while row is not None:
                    rows.append(row)
                    # fetchone only returns one row; use a different approach
                    break
                # Simpler: just drop the table; orphan LOs are harmless in tests
                await conn.execute("DROP TABLE IF EXISTS test_storage_files")
    except Exception:
        pass
    await storage.aclose()


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_save_and_open_bytes(pg_storage):
    await pg_storage.save("hello.txt", b"hello world")
    chunks = [chunk async for chunk in await pg_storage.open("hello.txt")]
    assert b"".join(chunks) == b"hello world"


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_save_overwrites(pg_storage):
    await pg_storage.save("a.txt", b"first")
    await pg_storage.save("a.txt", b"second")
    chunks = [chunk async for chunk in await pg_storage.open("a.txt")]
    assert b"".join(chunks) == b"second"


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_save_from_async_iterable(pg_storage):
    async def gen():
        yield b"chunk-1-"
        yield b"chunk-2"

    await pg_storage.save("streamed.txt", gen())
    chunks = [chunk async for chunk in await pg_storage.open("streamed.txt")]
    assert b"".join(chunks) == b"chunk-1-chunk-2"


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_exists(pg_storage):
    assert await pg_storage.exists("nope.txt") is False
    await pg_storage.save("yes.txt", b"x")
    assert await pg_storage.exists("yes.txt") is True


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_size(pg_storage):
    await pg_storage.save("sized.txt", b"12345")
    assert await pg_storage.size("sized.txt") == 5


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_size_missing_raises(pg_storage):
    with pytest.raises(fs.StorageFileNotFoundError):
        await pg_storage.size("missing.txt")


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_open_missing_raises(pg_storage):
    with pytest.raises(fs.StorageFileNotFoundError):
        await pg_storage.open("missing.txt")


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_delete_is_idempotent(pg_storage):
    await pg_storage.save("a.txt", b"x")
    await pg_storage.delete("a.txt")
    assert await pg_storage.exists("a.txt") is False
    await pg_storage.delete("a.txt")  # must not raise


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_save_with_content_type(pg_storage):
    await pg_storage.save("image.png", b"\x89PNG", content_type="image/png")
    assert await pg_storage.size("image.png") == 4


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_save_with_upload_to(pg_storage):
    result = await pg_storage.save("photo.jpg", b"data", upload_to="avatars")
    assert result == "avatars/photo.jpg"
    assert await pg_storage.exists("avatars/photo.jpg") is True


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_save_with_upload_to_callable(pg_storage):
    def namer(name: str, ctx: dict | None) -> str:
        user_id = ctx["user_id"] if ctx else "anon"
        return f"users/{user_id}/{name}"

    result = await pg_storage.save(
        "avatar.png", b"img", upload_to=namer, context={"user_id": "42"},
    )
    assert result == "users/42/avatar.png"
    assert await pg_storage.exists("users/42/avatar.png") is True


@pytest.mark.skipif(not (_HAS_DSN and _HAS_DRIVER), reason=_SKIP_NO_DSN)
@pytest.mark.asyncio
async def test_invalid_name_rejected(pg_storage):
    with pytest.raises(fs.StorageConfigError):
        await pg_storage.save("", b"x")
