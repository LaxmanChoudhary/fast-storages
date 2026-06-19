# fast-storages

[![PyPI version](https://img.shields.io/pypi/v/fast-storages.svg)](https://pypi.org/project/fast-storages/)
[![Python versions](https://img.shields.io/pypi/pyversions/fast-storages.svg)](https://pypi.org/project/fast-storages/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Django-style, loosely-coupled **async file storage** for FastAPI.

Pluggable backends behind one stable `Storage` contract — swap from local
filesystem to S3, Azure Blob, PostgreSQL, or your own custom backend without
changing a single line of application code.

## Features

- **One interface, many backends** — `save`, `open`, `delete`, `exists`, `size`, `url` work identically across all backends.
- **Streaming-first** — `save()` accepts `bytes` or `AsyncIterable[bytes]`; `open()` returns an `AsyncIterator[bytes]`. Large files never need to be buffered in memory.
- **Named storages** — configure multiple backends simultaneously (e.g. `"default"` → S3, `"avatars"` → local) just like Django's `STORAGES`.
- **FastAPI dependency injection** — `Depends(get_storage())` gives you a configured `Storage` in any route handler.
- **Env-driven configuration** — every backend ships a pydantic-settings `*StorageSettings` class that reads from environment variables.
- **Custom backends** — register your own backend with `@register_backend("name")` or use a dotted import path.

## Supported Backends

| Backend      | Status          | Install extra          |
|:-------------|:----------------|:-----------------------|
| **Local**    | ✅ Stable       | *(included)*           |
| **Azure Blob** | ✅ Stable     | `fast-storages[azure]` |
| **PostgreSQL** | ✅ Stable     | `fast-storages[postgresql]` or `fast-storages[postgresql-asyncpg]` |
| **S3**       | 🔧 Interface only | `fast-storages[s3]`  |

---

## Installation

```bash
# Core (includes local filesystem backend)
pip install fast-storages

# With a specific backend
pip install fast-storages[azure]
pip install fast-storages[s3]
pip install fast-storages[postgresql]         # psycopg driver (recommended)
pip install fast-storages[postgresql-asyncpg]  # asyncpg driver

# Everything
pip install fast-storages[all]
```

> **Requires Python 3.10+**

---

## Quick Start

### 1. Set up the StorageManager in your app's lifespan

```python
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from fast_storages import StorageManager

@asynccontextmanager
async def lifespan(app: FastAPI):
    manager = StorageManager()

    # Register a "default" storage backed by the local filesystem
    manager.add(
        "default",
        backend="local",
        config={"media_root": "./uploads", "media_url": "/files"},
    )

    app.state.storage_manager = manager
    yield
    await manager.aclose_all()

app = FastAPI(lifespan=lifespan)
```

### 2. Use `get_storage()` in your route handlers

```python
from fastapi import Depends, UploadFile
from fastapi.responses import StreamingResponse

from fast_storages import Storage, UploadFileReader, get_storage, guess_content_type

@app.post("/upload")
async def upload(file: UploadFile, storage: Storage = Depends(get_storage())):
    reader = UploadFileReader(file)
    result = await storage.save(
        file.filename or "unnamed",
        reader,
        content_type=file.content_type or guess_content_type(file.filename or ""),
    )
    return {
        "name": result.name,
        "key": result.key,
        "size": result.size,
        "content_type": result.content_type,
        "url": await storage.url(result.key),
    }

@app.get("/download/{name:path}")
async def download(name: str, storage: Storage = Depends(get_storage())):
    stream = await storage.open(name)
    return StreamingResponse(stream, media_type="application/octet-stream")

@app.delete("/files/{name:path}")
async def delete_file(name: str, storage: Storage = Depends(get_storage())):
    await storage.delete(name)
    return {"deleted": name}
```

That's it — your app now handles file uploads, downloads, and deletes through
the storage layer.

---

## Multiple Named Storages

Register as many backends as you need, each under a unique name:

```python
manager = StorageManager()

manager.add(
    "default",
    backend="azure",
    config={
        "connection_string": "DefaultEndpointsProtocol=https;...",
        "container": "uploads",
    },
)

manager.add(
    "avatars",
    backend="local",
    config={"media_root": "/data/avatars", "media_url": "/avatars"},
)

manager.add(
    "db",
    backend="postgresql",
    config={
        "dsn": "postgresql://user:pass@localhost:5432/mydb",
        "serve_url": "http://localhost:8000/db/download",
        "create_table": True,
    },
)
```

Then inject the specific storage by name:

```python
@app.post("/upload/avatar")
async def upload_avatar(
    file: UploadFile,
    storage: Storage = Depends(get_storage("avatars")),
):
    reader = UploadFileReader(file)
    result = await storage.save(file.filename, reader)
    return {"url": await storage.url(result.key)}
```

---

## Configuration

Every backend accepts config as **plain kwargs**, a **dict**, or a
**pydantic-settings** `*StorageSettings` instance. All three are equivalent:

```python
# 1. Plain kwargs via dict
manager.add("default", backend="local", config={"media_root": "/data", "media_url": "/media"})

# 2. Direct construction
from fast_storages.backends.local import LocalStorage
storage = LocalStorage(base_path="/data", base_url="/media")

# 3. Env-driven settings
from fast_storages.backends.local import LocalStorageSettings
settings = LocalStorageSettings()  # reads FASTAPI_STORAGE_LOCAL_BASE_PATH, etc.
manager.add("default", backend="local", config=settings)
```

### Environment Variable Prefixes

| Backend      | Prefix                           |
|:-------------|:---------------------------------|
| Local        | `FASTAPI_STORAGE_LOCAL_`         |
| Azure Blob   | `FASTAPI_STORAGE_AZURE_`        |
| S3           | `FASTAPI_STORAGE_S3_`           |
| PostgreSQL   | `FASTAPI_STORAGE_POSTGRESQL_`   |

### Backend Configuration Reference

<details>
<summary><strong>Local Filesystem</strong></summary>

| Parameter    | Type     | Required | Description                          |
|:-------------|:---------|:---------|:-------------------------------------|
| `media_root` | `str`    | ✅       | Root directory for stored files      |
| `media_url`  | `str`    | No       | URL prefix for `url()`               |

```python
manager.add("default", backend="local", config={
    "media_root": "./uploads",
    "media_url": "/files",
})
```
</details>

<details>
<summary><strong>Azure Blob Storage</strong></summary>

| Parameter            | Type   | Required | Description                              |
|:---------------------|:-------|:---------|:-----------------------------------------|
| `container`          | `str`  | ✅       | Blob container name                      |
| `connection_string`  | `str`  | ✅*      | Full Azure connection string             |
| `account_url`        | `str`  | ✅*      | Account URL (mutually exclusive with connection_string) |
| `account_key`        | `str`  | ✅*      | Account key (used with account_url)      |
| `public`             | `bool` | No       | If `True`, `url()` returns plain blob URL without SAS token (default: `False`) |
| `default_expires_in` | `int`  | No       | SAS token lifetime in seconds (default: `3600`) |
| `custom_url`         | `str`  | No       | Optional custom CDN or domain URL for `url()` |

\* Provide **either** `connection_string` **or** both `account_url` + `account_key`.

```python
manager.add("default", backend="azure", config={
    "connection_string": "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;",
    "container": "uploads",
    "custom_url": "https://cdn.example.com/uploads",
})
```
</details>

<details>
<summary><strong>PostgreSQL (Large Objects)</strong></summary>

| Parameter       | Type   | Required | Description                              |
|:----------------|:-------|:---------|:-----------------------------------------|
| `dsn`           | `str`  | ✅       | PostgreSQL connection string             |
| `table_name`    | `str`  | No       | Metadata table name (default: `storage_files`) |
| `serve_url`     | `str`  | No       | Complete absolute URL prefix for `url()` (e.g. `http://localhost:8000/db/download`) |
| `pool_min_size` | `int`  | No       | Minimum pool connections (default: `2`)  |
| `pool_max_size` | `int`  | No       | Maximum pool connections (default: `10`) |
| `chunk_size`    | `int`  | No       | Read/write chunk size in bytes (default: `65536`) |
| `driver`        | `str`  | No       | `"psycopg"` or `"asyncpg"` (auto-detected if `None`) |
| `create_table`  | `bool` | No       | Auto-create metadata table (default: `False`) |

```python
manager.add("db", backend="postgresql", config={
    "dsn": "postgresql://user:pass@localhost:5432/mydb",
    "table_name": "storage_files",
    "serve_url": "http://localhost:8000/db/download",
    "create_table": True,
})
```

### Serving Database Content

Since PostgreSQL Large Object storage files are kept in the database, the URL returned by `url()` is not automatically served. You must serve it yourself by creating a matching endpoint in your application.

Here is a complete FastAPI working example:

```python
from fastapi import FastAPI, Depends
from fastapi.responses import StreamingResponse
from fast_storages import Storage, get_storage

app = FastAPI()

# Matches the path portion of serve_url = "http://localhost:8000/db/download"
@app.get("/db/download/{name:path}")
async def db_download(name: str, storage: Storage = Depends(get_storage("db"))):
    """Stream a file from PostgreSQL Large Object storage."""
    stream = await storage.open(name)
    return StreamingResponse(stream, media_type="application/octet-stream")
```
</details>

**Schema management with Alembic:**

```python
from sqlalchemy.orm import DeclarativeBase
from fast_storages.backends.postgresql_schema import StorageFileMixin

class Base(DeclarativeBase):
    pass

class StorageFile(StorageFileMixin, Base):
    pass  # table is now tracked by Alembic
```
</details>

<details>
<summary><strong>S3 (Interface Only)</strong></summary>

| Parameter           | Type   | Required | Description                              |
|:--------------------|:-------|:---------|:-----------------------------------------|
| `bucket`            | `str`  | ✅       | S3 bucket name                           |
| `region_name`       | `str`  | No       | AWS region                               |
| `endpoint_url`      | `str`  | No       | Custom endpoint (MinIO, R2, etc.)        |
| `access_key_id`     | `str`  | No       | AWS access key (falls back to default credential chain) |
| `secret_access_key` | `str`  | No       | AWS secret key                           |
| `public`            | `bool` | No       | Return public URLs when `expires_in` is `None` (default: `False`) |
| `default_expires_in`| `int`  | No       | Presigned URL lifetime in seconds (default: `3600`) |

> **Note:** The S3 backend currently defines the interface only — method bodies
> are not yet implemented. Constructor and method signatures are finalized.

```python
manager.add("default", backend="s3", config={
    "bucket": "my-bucket",
    "region_name": "us-east-1",
})
```
</details>

---

## Storage API

Every backend implements the same `Storage` interface:

```python
class Storage(ABC):
    async def save(name, content, *, content_type=None, upload_to=None, context=None) -> FileMeta
    async def open(name, *, chunk_size=65536) -> AsyncIterator[bytes]
    async def delete(name) -> None
    async def exists(name) -> bool
    async def size(name) -> int
    async def url(name, *, expires_in=None) -> str
    async def aclose() -> None
```

| Method      | Description |
|:------------|:------------|
| `save()`    | Write content to `name`, overwriting if it already exists. Returns a `FileMeta` with the stored file's `key`, `name`, `size`, `content_type`, and `backend`. |
| `open()`    | Stream the content at `name` as an async iterator of bytes chunks. |
| `delete()`  | Delete the object at `name`. Idempotent — no error if it doesn't exist. |
| `exists()`  | Return `True` if an object exists at `name`. |
| `size()`    | Return the size of the object in bytes. |
| `url()`     | Return a URL for the stored file. May be relative or absolute depending on backend and configuration. |
| `aclose()`  | Release held resources (connection pools, HTTP sessions). Call from your app's shutdown handler. |

### `FileMeta` — the `save()` return object

`save()` returns a frozen `FileMeta` dataclass with everything you need about the stored file:

| Field          | Type             | Description |
|:---------------|:-----------------|:------------|
| `name`         | `str`            | The original filename as supplied by the caller |
| `key`          | `str`            | The resolved storage path/key (pass this to `open()`, `url()`, `delete()`, etc.) |
| `size`         | `int`            | Total bytes written |
| `content_type` | `str \| None`    | MIME type, if provided |
| `backend`      | `str \| None`    | Backend identifier (e.g. `"local"`, `"s3"`, `"azure"`, `"postgresql"`) |

```python
result = await storage.save("photo.png", data, content_type="image/png", upload_to="avatars")
result.name          # "photo.png"
result.key           # "avatars/photo.png"
result.size          # 102400
result.content_type  # "image/png"
result.backend       # "local"
```

All backends also support the async context manager protocol:

```python
async with LocalStorage(base_path="./uploads") as storage:
    await storage.save("hello.txt", b"Hello, world!")
```

---

## Upload Path Control (`upload_to`)

`save()` supports an `upload_to` parameter to control where a file is stored:

```python
# String prefix — prepends a directory
result = await storage.save("photo.png", content, upload_to="avatars")
result.key   # "avatars/photo.png"
result.name  # "photo.png"

# Callable — full custom logic
def user_upload_path(name: str, context: dict | None) -> str:
    user_id = context["user_id"]
    return f"users/{user_id}/{name}"

result = await storage.save(
    "photo.png", content,
    upload_to=user_upload_path,
    context={"user_id": 42},
)
result.key  # "users/42/photo.png"
```

---

## Exception Handling

All backends translate provider-specific errors into a common exception
hierarchy — your application code never needs to catch SDK-specific exceptions:

```
StorageError (base)
├── StorageFileNotFoundError
├── StoragePermissionError
├── StorageUnsupportedOperationError
├── StorageConfigError
└── StorageConnectionError
```

Map them to HTTP responses in FastAPI:

```python
from fast_storages import (
    StorageError,
    StorageFileNotFoundError,
    StoragePermissionError,
    StorageUnsupportedOperationError,
)

@app.exception_handler(StorageFileNotFoundError)
async def _not_found(request, exc):
    raise HTTPException(status_code=404, detail=str(exc))

@app.exception_handler(StoragePermissionError)
async def _forbidden(request, exc):
    raise HTTPException(status_code=403, detail=str(exc))

@app.exception_handler(StorageUnsupportedOperationError)
async def _not_implemented(request, exc):
    raise HTTPException(status_code=501, detail=str(exc))

@app.exception_handler(StorageError)
async def _storage_error(request, exc):
    raise HTTPException(status_code=502, detail=str(exc))
```

---

## Writing a Custom Backend

1. Subclass `Storage` and implement all abstract methods:

```python
from fast_storages import Storage, SaveContent
from fast_storages.base import UploadTo, resolve_upload_name

class MyStorage(Storage):
    backend_name = "my-backend"

    async def save(self, name, content, *, content_type=None, upload_to=None, context=None):
        resolved = resolve_upload_name(name, upload_to, context)
        # ... write content, track total_size ...
        return FileMeta(
            name=name, key=resolved, size=total_size,
            content_type=content_type, backend=self.backend_name,
        )

    async def open(self, name, *, chunk_size=65536):
        # ... return an AsyncIterator[bytes] ...

    async def delete(self, name):
        # ... idempotent delete ...

    async def exists(self, name):
        # ... return bool ...

    async def size(self, name):
        # ... return int ...

    async def url(self, name, *, expires_in=None):
        # ... return str ...
```

2. Register it (optional — enables use by short name):

```python
from fast_storages import register_backend

register_backend("my-backend")(MyStorage)
```

3. Or use the dotted import path directly:

```python
manager.add("default", backend="mypackage.backends.MyStorage", config={...})
```

---

## Utilities

| Function / Class      | Description |
|:----------------------|:------------|
| `UploadFileReader`    | Wraps a FastAPI `UploadFile` as `AsyncIterator[bytes]` for `save()` |
| `guess_content_type(filename)` | Best-effort MIME type guess from a filename |
| `read_all(iterator)`  | Drain an `AsyncIterator[bytes]` into a single `bytes` object |
| `FileMeta`            | Frozen dataclass returned by `save()` with `name`, `key`, `size`, `content_type`, `backend` |
| `build_storage(backend, config)` | Construct a `Storage` instance outside of `StorageManager` |
| `list_registered_backends()` | List all registered backend short names |

---

## License

[MIT](https://opensource.org/licenses/MIT)

## Links

- **PyPI:** https://pypi.org/project/fast-storages/
- **Repository:** https://github.com/LaxmanChoudhary/fast-storages
- **Changelog:** https://github.com/LaxmanChoudhary/fast-storages/releases
