"""
Test suite covering the Storage ABC contract via the local backend
(the only fully-implemented backend), plus registry and manager behavior.

Run with: pytest tests/ -v
"""
from __future__ import annotations

import pytest

import fast_storages as fs
from fast_storages.backends.local import LocalStorage, LocalStorageSettings


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(media_root=tmp_path, media_url="/media")


@pytest.mark.asyncio
async def test_save_and_open_bytes(storage):
    result = await storage.save("a.txt", b"hello")
    assert result.key == "a.txt"
    assert result.name == "a.txt"
    assert result.size == 5
    assert result.backend == "local"
    chunks = [chunk async for chunk in await storage.open("a.txt")]
    assert b"".join(chunks) == b"hello"


@pytest.mark.asyncio
async def test_save_overwrites_existing(storage):
    await storage.save("a.txt", b"first")
    result = await storage.save("a.txt", b"second")
    assert result.size == 6
    chunks = [chunk async for chunk in await storage.open("a.txt")]
    assert b"".join(chunks) == b"second"


@pytest.mark.asyncio
async def test_save_from_async_iterable(storage):
    async def gen():
        yield b"chunk-1-"
        yield b"chunk-2"

    result = await storage.save("streamed.txt", gen())
    assert result.key == "streamed.txt"
    assert result.size == 15
    chunks = [chunk async for chunk in await storage.open("streamed.txt")]
    assert b"".join(chunks) == b"chunk-1-chunk-2"


@pytest.mark.asyncio
async def test_save_returns_filemeta_with_content_type(storage):
    result = await storage.save("photo.png", b"\x89PNG", content_type="image/png")
    assert result.name == "photo.png"
    assert result.key == "photo.png"
    assert result.size == 4
    assert result.content_type == "image/png"
    assert result.backend == "local"


@pytest.mark.asyncio
async def test_save_with_upload_to_reflects_in_key(storage):
    result = await storage.save("photo.png", b"img", upload_to="avatars")
    assert result.name == "photo.png"
    assert result.key == "avatars/photo.png"
    assert result.size == 3


@pytest.mark.asyncio
async def test_exists(storage):
    assert await storage.exists("missing.txt") is False
    await storage.save("present.txt", b"x")
    assert await storage.exists("present.txt") is True


@pytest.mark.asyncio
async def test_size(storage):
    await storage.save("sized.txt", b"12345")
    assert await storage.size("sized.txt") == 5


@pytest.mark.asyncio
async def test_size_missing_raises(storage):
    with pytest.raises(fs.StorageFileNotFoundError):
        await storage.size("missing.txt")


@pytest.mark.asyncio
async def test_open_missing_raises(storage):
    with pytest.raises(fs.StorageFileNotFoundError):
        await storage.open("missing.txt")


@pytest.mark.asyncio
async def test_delete_is_idempotent(storage):
    await storage.save("a.txt", b"x")
    await storage.delete("a.txt")
    assert await storage.exists("a.txt") is False
    await storage.delete("a.txt")  # must not raise


@pytest.mark.asyncio
async def test_url_with_base_url(storage):
    await storage.save("dir/a.txt", b"x")
    assert await storage.url("dir/a.txt") == "/media/dir/a.txt"


@pytest.mark.asyncio
async def test_url_without_media_url_raises(tmp_path):
    storage_no_url = LocalStorage(media_root=tmp_path)
    with pytest.raises(fs.StorageUnsupportedOperationError):
        await storage_no_url.url("a.txt")


@pytest.mark.asyncio
async def test_url_with_expires_in_raises(storage):
    await storage.save("a.txt", b"x")
    with pytest.raises(fs.StorageUnsupportedOperationError):
        await storage.url("a.txt", expires_in=60)


@pytest.mark.asyncio
async def test_nested_directories_created_on_save(storage):
    await storage.save("deeply/nested/path/file.txt", b"x")
    assert await storage.exists("deeply/nested/path/file.txt") is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "malicious_name",
    [
        "../escape.txt",
        "../../etc/passwd",
        "a/../../escape.txt",
    ],
)
async def test_path_traversal_blocked(storage, malicious_name):
    with pytest.raises(fs.StoragePermissionError):
        await storage.save(malicious_name, b"pwned")


@pytest.mark.asyncio
async def test_invalid_name_rejected(storage):
    with pytest.raises(fs.StorageConfigError):
        await storage.save("", b"x")


def test_local_storage_settings_to_kwargs(tmp_path, monkeypatch):
    monkeypatch.setenv("FASTAPI_STORAGE_LOCAL_MEDIA_ROOT", str(tmp_path))
    monkeypatch.setenv("FASTAPI_STORAGE_LOCAL_MEDIA_URL", "/media")
    settings = LocalStorageSettings()
    kwargs = settings.to_kwargs()
    assert kwargs == {"media_root": str(tmp_path), "media_url": "/media"}


def test_build_storage_with_dict_config(tmp_path):
    storage = fs.build_storage("local", {"media_root": str(tmp_path), "media_url": "/m"})
    assert isinstance(storage, LocalStorage)
    assert storage.media_url == "/m"


def test_build_storage_with_settings_object(tmp_path, monkeypatch):
    monkeypatch.setenv("FASTAPI_STORAGE_LOCAL_MEDIA_ROOT", str(tmp_path))
    settings = LocalStorageSettings()
    storage = fs.build_storage("local", settings)
    assert isinstance(storage, LocalStorage)


def test_build_storage_unknown_backend_raises():
    with pytest.raises(fs.StorageConfigError):
        fs.build_storage("not-a-real-backend", {})


def test_build_storage_dotted_path(tmp_path):
    storage = fs.build_storage(
        "fast_storages.backends.local.LocalStorage", {"media_root": str(tmp_path)}
    )
    assert isinstance(storage, LocalStorage)


def test_s3_backend_registered_but_unusable_without_sdk():
    with pytest.raises(ImportError):
        fs.build_storage("s3", {"bucket": "test-bucket"})


def test_storage_manager_duplicate_name_raises(tmp_path):
    manager = fs.StorageManager()
    manager.add("default", backend="local", config={"media_root": str(tmp_path / "a")})
    with pytest.raises(fs.StorageConfigError):
        manager.add("default", backend="local", config={"media_root": str(tmp_path / "b")})


def test_storage_manager_unknown_name_raises(tmp_path):
    manager = fs.StorageManager()
    manager.add("default", backend="local", config={"media_root": str(tmp_path)})
    with pytest.raises(fs.StorageConfigError):
        manager.get("nonexistent")


def test_storage_manager_multiple_named_storages(tmp_path):
    manager = fs.StorageManager()
    manager.add("default", backend="local", config={"media_root": str(tmp_path / "a")})
    manager.add("avatars", backend="local", config={"media_root": str(tmp_path / "b")})
    assert manager.get("default").media_root != manager.get("avatars").media_root


@pytest.mark.asyncio
async def test_storage_manager_aclose_all(tmp_path):
    manager = fs.StorageManager()
    manager.add("default", backend="local", config={"media_root": str(tmp_path)})
    await manager.aclose_all()  # local backend's aclose is a no-op; must not raise
