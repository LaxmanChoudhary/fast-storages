"""
Example FastAPI app demonstrating fast-storages wiring.

Run with: uvicorn examples.app:app --reload

Three named storages are configured:

  - "default"  → Local filesystem (media_root/media_url)
  - "azure"    → Azure Blob (with custom_url)
  - "db"       → PostgreSQL Large Objects (with serve_url)

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

from fastapi import Depends, FastAPI, HTTPException, UploadFile, BackgroundTasks
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles

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
UPLOAD_DIR = BASE_DIR / "uploads"

# Ensure the upload directory exists before mounting StaticFiles
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# PostgreSQL DSN — override with the PGQL_DSN env var if needed.
PGQL_DSN = os.environ.get(
    "PGQL_DSN",
    "postgresql://postgres:postgres@localhost:5432/fast-storage",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    manager = StorageManager()
    
    # 1. Local filesystem storage
    manager.add(
        "default",
        backend="local",
        config={
            "media_root": UPLOAD_DIR,
            "media_url": "/static/uploads",
        },
    )
    
    # 2. Azure Blob storage
    manager.add(
        "azure",
        backend="azure",
        config={
            "connection_string": (
                "DefaultEndpointsProtocol=http;"
                "AccountName=devstoreaccount1;"
                "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
            ),
            "container": "uploads",
            "custom_url": "http://localhost:8000/cdn/uploads",
        },
    )
    
    # 3. PostgreSQL Large Object storage
    manager.add(
        "db",
        backend="postgresql",
        config={
            "dsn": PGQL_DSN,
            "table_name": "storage_files",
            "serve_url": "http://localhost:8000/db/download",
            "create_table": True,
        },
    )
    app.state.storage_manager = manager
    yield
    await manager.aclose_all()


app = FastAPI(lifespan=lifespan)

# Mount FastAPI StaticFiles to handle serving uploaded local files statically
app.mount("/static/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")


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
# Local Storage Routes (default)
# ---------------------------------------------------------------------------

@app.post("/upload")
async def upload(file: UploadFile, storage: Storage = Depends(get_storage("default"))):
    """Upload a file to the local filesystem storage."""
    content_type = file.content_type or guess_content_type(file.filename or "")
    reader = UploadFileReader(file)
    file_name = file.filename or "unnamed"
    file_meta = await storage.save(file_name, reader, upload_to="test-upload", content_type=content_type)
    return {
        "name": file_meta.name,
        "size": file_meta.size,
        "key": file_meta.key,
        "url": await storage.url(file_meta.key),
    }


@app.delete("/files/{name:path}")
async def delete_file(name: str, storage: Storage = Depends(get_storage("default"))):
    """Delete a file from local filesystem storage."""
    await storage.delete(name)
    return {"deleted": name}


# ---------------------------------------------------------------------------
# Azure Blob Storage Routes
# ---------------------------------------------------------------------------

@app.post("/azure/upload")
async def azure_upload(file: UploadFile, storage: Storage = Depends(get_storage("azure"))):
    """Upload a file to Azure Blob storage."""
    content_type = file.content_type or guess_content_type(file.filename or "")
    reader = UploadFileReader(file)
    file_name = file.filename or "unnamed"
    file_meta = await storage.save(file_name, reader, content_type=content_type)
    return {
        "name": file_meta.name,
        "size": file_meta.size,
        "key": file_meta.key,
        "url": await storage.url(file_meta.key),
    }


# ---------------------------------------------------------------------------
# PostgreSQL DB Routes
# ---------------------------------------------------------------------------

@app.post("/db/upload")
async def db_upload(file: UploadFile, storage: Storage = Depends(get_storage("db"))):
    """Upload a file into PostgreSQL Large Object storage."""
    content_type = file.content_type or guess_content_type(file.filename or "")
    reader = UploadFileReader(file)
    file_name = file.filename or "unnamed"
    file_meta = await storage.save(file_name, reader, content_type=content_type)
    return {
        "name": file_meta.name,
        "size": file_meta.size,
        "key": file_meta.key,
        "url": await storage.url(file_meta.key),
    }


@app.get("/db/download/{name:path}")
async def db_download(name: str, storage: Storage = Depends(get_storage("db"))):
    """Stream a file from PostgreSQL Large Object storage."""
    stream = await storage.open(name)
    return StreamingResponse(stream, media_type="application/octet-stream")


@app.get("/db/file-response/{name:path}")
async def db_file_response(
    name: str,
    background_tasks: BackgroundTasks,
    storage: Storage = Depends(get_storage("db")),
):
    """
    Serve DB content using FastAPI's FileResponse.
    Since FileResponse requires a file path on disk, we stream the database
    content to a temporary file first, return the FileResponse, and register
    a background task to delete the temporary file after the response is sent.
    """
    temp_dir = BASE_DIR / "temp"
    temp_dir.mkdir(exist_ok=True)
    
    # Generate a safe local filename
    safe_name = name.replace("/", "_").replace("\\", "_")
    temp_file_path = temp_dir / safe_name
    
    # Write the DB content to the temporary file
    stream = await storage.open(name)
    with open(temp_file_path, "wb") as f:
        async for chunk in stream:
            f.write(chunk)
            
    # Clean up the file after the response has finished sending
    background_tasks.add_task(lambda path: path.unlink(missing_ok=True), temp_file_path)
    
    return FileResponse(
        path=temp_file_path,
        filename=name,
        media_type="application/octet-stream",
    )


@app.delete("/db/files/{name:path}")
async def db_delete(name: str, storage: Storage = Depends(get_storage("db"))):
    """Delete a file from DB storage."""
    await storage.delete(name)
    return {"deleted": name}
