"""
Configuration pattern shared by all backends.

Design: each backend's constructor accepts plain kwargs (or a plain dict via
**dict). A companion `*StorageSettings` (pydantic-settings BaseSettings)
exists per backend purely as an optional, validated, env-var-driven way to
*produce* those kwargs. There are not two separate config code paths -- the
Settings model's only job is `.to_kwargs()`, which the backend constructor
also accepts directly.

This means all three of these are equivalent:

    LocalStorage(base_path="/var/data/media", base_url="/media")

    LocalStorage(**{"base_path": "/var/data/media", "base_url": "/media"})

    settings = LocalStorageSettings()  # reads FASTAPI_STORAGE_LOCAL_BASE_PATH etc from env
    LocalStorage(**settings.to_kwargs())
"""
from __future__ import annotations

from typing import Any, ClassVar

from pydantic import ConfigDict
from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseStorageSettings(BaseSettings):
    """
    Base class for per-backend settings.

    Subclasses set `env_prefix` via model_config, e.g.
    SettingsConfigDict(env_prefix="FASTAPI_STORAGE_S3_").

    Field names on the subclass should match the constructor kwarg names of
    the corresponding backend exactly, so `to_kwargs()` can hand them
    straight to `Backend(**kwargs)` with no translation layer to keep in
    sync by hand.
    """

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        extra="ignore",
        case_sensitive=False,
    )

    def to_kwargs(self) -> dict[str, Any]:
        """
        Return constructor kwargs for the matching backend, dropping unset
        optional fields (None) so backend constructors can use their own
        defaults rather than receiving an explicit None override.
        """
        return {k: v for k, v in self.model_dump().items() if v is not None}


class StorageConfigDict(ConfigDict):
    """Re-exported for backend modules that want a typed plain-dict config shape."""
