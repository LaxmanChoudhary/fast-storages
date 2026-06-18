"""
Example FastAPI app demonstrating fast-storages wiring.

Run with: uvicorn examples.app:app --reload

Three named storages are configured:

  - "default"  → Azure Blob (Azurite dev emulator)
  - "avatars"  → Local filesystem
  - "db"       → PostgreSQL Large Objects

To test the PostgreSQL backend, make sure:
  1. psycopg + psycopg-pool are installed:
       pip install psycopg[binary] psycopg-pool
  2. A local PostgreSQL server is running with a database called "fast-storages"
     (or set PGQL_DSN below to your own connection string).
  3. The metadata table is auto-created on first request (create_table=True).
"""
from __future__ import annotations

import os
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from fast_storages import (
    Storage,
    StorageError,
    StorageFileNotFoundError,
    StorageManager,
    StoragePermissionError,
    StorageUnsupportedOperationError,
    UploadFileReader,
    get_storage,
    guess_content_type,
)

BASE_DIR = Path(__file__).parent
BASE_URL = "http://localhost:8000/files"

# PostgreSQL DSN — override with the PGQL_DSN env var if needed.
PGQL_DSN = os.environ.get(
    "PGQL_DSN",
    "postgresql://postgres:postgres@localhost:5432/fast-storage",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    manager = StorageManager()
    # manager.add(
    #     "default",
    #     backend="local",
    #     config={"base_path": BASE_DIR / "uploads", "base_url": "/files"},
    # )
    manager.add(
        "default",
        backend="azure",
        config={
            "connection_string": (
                "DefaultEndpointsProtocol=http;"
                "AccountName=devstoreaccount1;"
                "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
            ),
            "container": "uploads",

        },
    )
    # A second named storage, to demonstrate multi-storage support.
    manager.add(
        "avatars",
        backend="local",
        config={"base_path": "/tmp/storage_test/avatars", "base_url": "/avatars"},
    )
    # PostgreSQL Large Object storage.
    manager.add(
        "db",
        backend="postgresql",
        config={
            "dsn": PGQL_DSN,
            "table_name": "storage_files",
            "base_url": "/db/download",
            "create_table": True,
        },
    )
    app.state.storage_manager = manager
    yield
    await manager.aclose_all()


app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------------------
# Exception → HTTP mapping
# ---------------------------------------------------------------------------

@app.exception_handler(StorageFileNotFoundError)
async def _not_found_handler(request, exc: StorageFileNotFoundError):
    raise HTTPException(status_code=404, detail=str(exc))


@app.exception_handler(StoragePermissionError)
async def _permission_handler(request, exc: StoragePermissionError):
    raise HTTPException(status_code=403, detail=str(exc))


@app.exception_handler(StorageUnsupportedOperationError)
async def _unsupported_handler(request, exc: StorageUnsupportedOperationError):
    raise HTTPException(status_code=501, detail=str(exc))


@app.exception_handler(StorageError)
async def _generic_storage_error_handler(request, exc: StorageError):
    raise HTTPException(status_code=502, detail=str(exc))


# ---------------------------------------------------------------------------
# Azure (default) routes
# ---------------------------------------------------------------------------

@app.post("/upload")
async def upload(file: UploadFile, storage: Storage = Depends(get_storage())):
    content_type = file.content_type or guess_content_type(file.filename or "")
    reader = UploadFileReader(file)
    file_name = file.filename or "unnamed"
    full_path = "myspace/2026/"+file_name
    key = await storage.save(full_path, reader, content_type=content_type)
    return {
        "name": file_name,
        "size": await storage.size(key),
        "url": await storage.url(key),
    }


@app.post("/upload/avatar")
async def upload_avatar(file: UploadFile, storage: Storage = Depends(get_storage("avatars"))):
    reader = UploadFileReader(file)
    saved_name = await storage.save(file.filename or "unnamed", reader, content_type=file.content_type)
    return {"name": saved_name, "url": await storage.url(saved_name)}


@app.get("/download/{name}")
async def download(name: str, storage: Storage = Depends(get_storage())):
    stream = await storage.open(name)
    return StreamingResponse(stream, media_type="application/octet-stream")


@app.delete("/files/{name}")
async def delete_file(name: str, storage: Storage = Depends(get_storage())):
    await storage.delete(name)
    return {"deleted": name}


# ---------------------------------------------------------------------------
# PostgreSQL DB routes
# ---------------------------------------------------------------------------

@app.post("/db/upload")
async def db_upload(file: UploadFile, storage: Storage = Depends(get_storage("db"))):
    """Upload a file into PostgreSQL Large Object storage."""
    content_type = file.content_type or guess_content_type(file.filename or "")
    reader = UploadFileReader(file)
    file_name = file.filename or "unnamed"
    key = await storage.save(file_name, reader, content_type=content_type)
    return {
        "name": key,
        "size": await storage.size(key),
        "url": await storage.url(key),
    }


@app.post("/db/upload/{folder:path}")
async def db_upload_to_folder(
    folder: str,
    file: UploadFile,
    storage: Storage = Depends(get_storage("db")),
):
    """Upload a file into a specific folder path inside DB storage."""
    content_type = file.content_type or guess_content_type(file.filename or "")
    reader = UploadFileReader(file)
    file_name = file.filename or "unnamed"
    key = await storage.save(file_name, reader, content_type=content_type, upload_to=folder)
    return {
        "name": key,
        "size": await storage.size(key),
        "url": await storage.url(key),
    }


@app.get("/db/download/{name:path}")
async def db_download(name: str, storage: Storage = Depends(get_storage("db"))):
    """Stream a file from PostgreSQL Large Object storage."""
    stream = await storage.open(name)
    return StreamingResponse(stream, media_type="application/octet-stream")


@app.get("/db/exists/{name:path}")
async def db_exists(name: str, storage: Storage = Depends(get_storage("db"))):
    """Check if a file exists in DB storage."""
    found = await storage.exists(name)
    return {"name": name, "exists": found}


@app.delete("/db/files/{name:path}")
async def db_delete(name: str, storage: Storage = Depends(get_storage("db"))):
    """Delete a file from DB storage."""
    await storage.delete(name)
    return {"deleted": name}
