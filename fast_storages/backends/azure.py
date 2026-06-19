"""
Azure Blob Storage backend.

STATUS: fully implemented, using azure-storage-blob's aio client. Install with:

    pip install fast-storages[azure]   # pulls in azure-storage-blob

Auth: exactly one of `connection_string` or (`account_url` + `account_key`)
must be supplied. Both paths end up with self._account_key populated when an
account key is known (connection strings normally embed AccountKey=...,
parsed out here) because generate_blob_sas() needs the raw key directly --
this is the one piece of friction worth flagging: SAS URL generation in this
SDK wants the key itself, not just an authenticated client, so a backend
built from a connection string that DOESN'T embed AccountKey (e.g. one using
a SAS token or AAD instead) can save/open/delete/exists/size fine but will
raise StorageUnsupportedOperationError from url()/full_url() if asked for an
expiring link, since there's no key available to sign with.

Other implementation notes
---------------------------
- One BlobServiceClient is created in __init__ and held for the backend's
  lifetime; aclose() closes it. Container/blob clients are cheap to create
  per-call (no separate connection), so they aren't cached.
- upload_blob's `data` param directly accepts AsyncIterable[bytes], so
  AsyncIterable content from save() is passed straight through with no
  buffering -- confirmed against the installed SDK version's signature.
- download_blob() returns a StorageStreamDownloader; iterating
  `async for chunk in downloader.chunks()` yields bytes chunks sized by the
  service/SDK, not by our chunk_size parameter -- Azure's chunking is not
  caller-controllable the way S3's iter_chunks(chunk_size) is, so chunk_size
  is accepted for interface compatibility but not actually honored here.
- delete_blob() raises ResourceNotFoundError on a missing blob (unlike S3's
  delete_object, which is silently idempotent), so that's caught explicitly
  to satisfy Storage.delete's idempotency contract.
- exists() uses the SDK's own BlobClient.exists(), no manual 404 handling
  needed.
"""
from __future__ import annotations

import re
from typing import Any, AsyncIterator, ClassVar
from urllib.parse import urlsplit

from ..base import DEFAULT_CHUNK_SIZE, SaveContent, Storage, UploadTo, resolve_upload_name
from ..config import BaseStorageSettings
from ..exceptions import (
    StorageConfigError,
    StorageConnectionError,
    StorageFileNotFoundError,
    StoragePermissionError,
    StorageUnsupportedOperationError,
)
from ..files import FileMeta
from pydantic_settings import SettingsConfigDict


class AzureStorageSettings(BaseStorageSettings):
    """
    Env-driven config for AzureStorage.

    Reads FASTAPI_STORAGE_AZURE_* by default. Field names match
    AzureStorage's constructor kwargs exactly. Exactly one of
    connection_string or (account_url + account_key) should be provided;
    that validation lives in AzureStorage.__init__ so it also applies when
    the backend is built directly from kwargs rather than from Settings.
    """

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="FASTAPI_STORAGE_AZURE_",
        extra="ignore",
    )

    container: str
    connection_string: str | None = None
    account_url: str | None = None
    account_key: str | None = None
    public: bool = False
    default_expires_in: int = 3600


def _extract_account_key_from_connection_string(connection_string: str) -> str | None:
    """
    Pull AccountKey=... out of a connection string, if present.

    Connection strings built around a SAS token or AAD credential won't have
    this, in which case the caller gets None back and SAS URL generation
    will not be available later (clearly surfaced via
    StorageUnsupportedOperationError, not a silent failure).
    """
    match = re.search(r"AccountKey=([^;]+)", connection_string)
    return match.group(1) if match else None


def _extract_account_name(connection_string: str | None, account_url: str | None) -> str | None:
    if connection_string:
        match = re.search(r"AccountName=([^;]+)", connection_string)
        if match:
            return match.group(1)
    if account_url:
        # https://{account}.blob.core.windows.net -> {account}
        host = urlsplit(account_url).netloc
        if host:
            return host.split(".")[0]
    return None


class AzureStorage(Storage):
    """
    Azure Blob Storage Storage implementation.

    Parameters
    ----------
    container:
        Blob container name. Not created automatically -- unlike the local
        backend's base_path, Azure containers typically aren't something you
        want auto-provisioned from app code, so a missing container surfaces
        as a StorageError (via ResourceNotFoundError) on first real use
        rather than being silently created.
    connection_string:
        Full Azure Storage connection string. Mutually exclusive with
        account_url/account_key.
    account_url, account_key:
        Account URL ("https://{account}.blob.core.windows.net") plus the
        account key. Mutually exclusive with connection_string.
    public:
        If True, url()/full_url() with expires_in=None return the plain
        blob URL with no SAS token (assumes the container/blob is
        configured for public read access in Azure). If False (default),
        a SAS token is always generated, using default_expires_in when no
        explicit expires_in is given.
    default_expires_in:
        Default SAS token lifetime in seconds, used when url()/full_url()
        is called without an explicit expires_in and public=False.
    """

    backend_name = "azure"

    def __init__(
        self,
        container: str,
        *,
        connection_string: str | None = None,
        account_url: str | None = None,
        account_key: str | None = None,
        public: bool = False,
        default_expires_in: int = 3600,
    ) -> None:
        try:
            from azure.storage.blob.aio import BlobServiceClient
        except ImportError as exc:
            raise ImportError(
                "AzureStorage requires azure-storage-blob. Install with: "
                "pip install fast-storages[azure]"
            ) from exc

        if not connection_string and not (account_url and account_key):
            raise StorageConfigError(
                "AzureStorage requires either connection_string, or both "
                "account_url and account_key."
            )
        if connection_string and (account_url or account_key):
            raise StorageConfigError(
                "AzureStorage accepts either connection_string OR "
                "account_url+account_key, not both."
            )

        self.container = container
        self.public = public
        self.default_expires_in = default_expires_in

        if connection_string:
            self._client = BlobServiceClient.from_connection_string(connection_string)
            self._account_key = _extract_account_key_from_connection_string(connection_string)
            self._account_name = _extract_account_name(connection_string, None)
        else:
            self._client = BlobServiceClient(account_url=account_url, credential=account_key)
            self._account_key = account_key
            self._account_name = _extract_account_name(None, account_url)

    def _blob_client(self, name: str) -> Any:
        container_client = self._client.get_container_client(self.container)
        return container_client.get_blob_client(name)

    async def save(
        self,
        name: str,
        content: SaveContent,
        *,
        content_type: str | None = None,
        upload_to: UploadTo = None,
        context: dict[str, Any] | None = None,
    ) -> FileMeta:
        from azure.core.exceptions import ClientAuthenticationError, HttpResponseError, ServiceRequestError
        from azure.storage.blob import ContentSettings

        resolved_name = resolve_upload_name(name, upload_to, context)
        blob_client = self._blob_client(resolved_name)
        content_settings = ContentSettings(content_type=content_type) if content_type else None

        # Track total bytes so we can report size in the returned FileMeta.
        total_size = 0
        if isinstance(content, bytes):
            total_size = len(content)
            upload_data: SaveContent = content
        else:
            # Wrap the async iterable to count bytes as they flow through.
            size_acc: list[int] = [0]

            async def _counting_iter() -> AsyncIterator[bytes]:
                async for chunk in content:
                    size_acc[0] += len(chunk)
                    yield chunk

            upload_data = _counting_iter()

        try:
            # overwrite=True matches this library's overwrite-on-collision
            # contract; upload_blob accepts bytes or AsyncIterable[bytes]
            # directly, so streamed content is passed through unbuffered.
            await blob_client.upload_blob(
                upload_data,
                overwrite=True,
                content_settings=content_settings,
            )
        except ClientAuthenticationError as exc:
            raise StoragePermissionError(resolved_name, backend="azure", detail=str(exc)) from exc
        except HttpResponseError as exc:
            if exc.status_code == 403:
                raise StoragePermissionError(resolved_name, backend="azure", detail=str(exc)) from exc
            raise
        except ServiceRequestError as exc:
            raise StorageConnectionError(backend="azure", detail=str(exc)) from exc

        if not isinstance(content, bytes):
            total_size = size_acc[0]

        return FileMeta(
            name=name,
            key=resolved_name,
            size=total_size,
            content_type=content_type,
            backend=self.backend_name,
        )

    async def open(self, name: str, *, chunk_size: int = DEFAULT_CHUNK_SIZE) -> AsyncIterator[bytes]:
        from azure.core.exceptions import ClientAuthenticationError, HttpResponseError, ResourceNotFoundError

        blob_client = self._blob_client(name)
        try:
            downloader = await blob_client.download_blob()
        except ResourceNotFoundError as exc:
            raise StorageFileNotFoundError(name, backend="azure") from exc
        except ClientAuthenticationError as exc:
            raise StoragePermissionError(name, backend="azure", detail=str(exc)) from exc
        except HttpResponseError as exc:
            if exc.status_code == 403:
                raise StoragePermissionError(name, backend="azure", detail=str(exc)) from exc
            raise

        # NOTE: chunk_size is accepted for Storage interface compatibility
        # but Azure's chunks() determines its own chunk boundaries; there is
        # no direct equivalent of S3's iter_chunks(chunk_size) here.
        async def _generator() -> AsyncIterator[bytes]:
            async for chunk in downloader.chunks():
                yield chunk

        return _generator()

    async def delete(self, name: str) -> None:
        from azure.core.exceptions import ClientAuthenticationError, HttpResponseError, ResourceNotFoundError

        blob_client = self._blob_client(name)
        try:
            await blob_client.delete_blob()
        except ResourceNotFoundError:
            # idempotent delete, per Storage.delete contract -- Azure raises
            # here where S3's delete_object would not.
            return
        except ClientAuthenticationError as exc:
            raise StoragePermissionError(name, backend="azure", detail=str(exc)) from exc
        except HttpResponseError as exc:
            if exc.status_code == 403:
                raise StoragePermissionError(name, backend="azure", detail=str(exc)) from exc
            raise

    async def exists(self, name: str) -> bool:
        from azure.core.exceptions import ClientAuthenticationError, HttpResponseError

        blob_client = self._blob_client(name)
        try:
            return await blob_client.exists()
        except ClientAuthenticationError as exc:
            raise StoragePermissionError(name, backend="azure", detail=str(exc)) from exc
        except HttpResponseError as exc:
            if exc.status_code == 403:
                raise StoragePermissionError(name, backend="azure", detail=str(exc)) from exc
            raise

    async def size(self, name: str) -> int:
        from azure.core.exceptions import ClientAuthenticationError, HttpResponseError, ResourceNotFoundError

        blob_client = self._blob_client(name)
        try:
            props = await blob_client.get_blob_properties()
        except ResourceNotFoundError as exc:
            raise StorageFileNotFoundError(name, backend="azure") from exc
        except ClientAuthenticationError as exc:
            raise StoragePermissionError(name, backend="azure", detail=str(exc)) from exc
        except HttpResponseError as exc:
            if exc.status_code == 403:
                raise StoragePermissionError(name, backend="azure", detail=str(exc)) from exc
            raise
        return props.size

    def _build_sas_url(self, name: str, expires_in: int) -> str:
        from datetime import datetime, timedelta, timezone

        from azure.storage.blob import BlobSasPermissions, generate_blob_sas

        if not self._account_key or not self._account_name:
            raise StorageUnsupportedOperationError(
                "url(expires_in=...)",
                backend="azure",
                reason=(
                    "no account key available to sign a SAS token (this backend was "
                    "constructed with a connection string/credential that doesn't "
                    "expose a raw account key)"
                ),
            )

        sas_token = generate_blob_sas(
            account_name=self._account_name,
            container_name=self.container,
            blob_name=name,
            account_key=self._account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(seconds=expires_in),
        )
        blob_client = self._blob_client(name)
        return f"{blob_client.url}?{sas_token}"

    async def url(self, name: str, *, expires_in: int | None = None) -> str:
        if expires_in is None and self.public:
            blob_client = self._blob_client(name)
            return blob_client.url
        return self._build_sas_url(name, expires_in or self.default_expires_in)

    async def full_url(self, name: str, *, expires_in: int | None = None) -> str:
        # Azure blob URLs (SAS or public) are already absolute, including
        # scheme and host, so full_url() is identical to url() here.
        return await self.url(name, expires_in=expires_in)

    async def aclose(self) -> None:
        await self._client.close()