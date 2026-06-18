from ..registry import register_backend
from .local import LocalStorage, LocalStorageSettings
from .s3 import S3Storage, S3StorageSettings
from .azure import AzureStorage, AzureStorageSettings
from .postgresql import PostgreSQLStorage, PostgreSQLStorageSettings

register_backend("local")(LocalStorage)
register_backend("s3")(S3Storage)
register_backend("azure")(AzureStorage)
register_backend("postgresql")(PostgreSQLStorage)

__all__ = [
    "LocalStorage",
    "LocalStorageSettings",
    "S3Storage",
    "S3StorageSettings",
    "AzureStorage",
    "AzureStorageSettings",
    "PostgreSQLStorage",
    "PostgreSQLStorageSettings",
]
