"""
Backend registry and factory.

Two ways a backend becomes usable by name:

1. Explicit registration via @register_backend("name") on the class. This is
   how backends shipped in this package (local, s3, azure, ...) register
   themselves, and how third-party backend packages can register into the
   same namespace without fast_storages needing to know about them ahead
   of time.

2. Dotted-path fallback: if a name isn't found in the registry, build()
   treats it as an importable path ("mypackage.backends.MyStorage") and
   imports it directly. This mirrors Django's STORAGES = {"BACKEND": "dotted.path"}
   pattern and means a backend never strictly needs to call register_backend
   at all -- registration is a convenience, not a requirement.

Either way, get_storage()/build() returns a configured Storage instance.
Config can be supplied as plain kwargs, a dict, or a BaseStorageSettings
instance (in which case .to_kwargs() is called automatically).
"""
from __future__ import annotations

import importlib
from typing import Any

from .base import Storage
from .config import BaseStorageSettings
from .exceptions import StorageConfigError

_REGISTRY: dict[str, type[Storage]] = {}


def register_backend(name: str) -> Any:
    """
    Class decorator that registers a Storage subclass under `name`.

    Example
    -------
        @register_backend("local")
        class LocalStorage(Storage):
            ...
    """

    def decorator(cls: type[Storage]) -> type[Storage]:
        if not issubclass(cls, Storage):
            raise TypeError(f"{cls!r} must subclass Storage to be registered")
        if name in _REGISTRY and _REGISTRY[name] is not cls:
            raise StorageConfigError(
                f"Backend name {name!r} is already registered to {_REGISTRY[name]!r}; "
                f"refusing to overwrite with {cls!r}."
            )
        _REGISTRY[name] = cls
        return cls

    return decorator


def _resolve_backend_class(backend: str) -> type[Storage]:
    """
    Resolve a backend identifier to a Storage subclass.

    `backend` is first looked up in the explicit registry (e.g. "local",
    "s3", "azure"). If not found, it's treated as a dotted import path
    ("package.module.ClassName") and imported directly.
    """
    if backend in _REGISTRY:
        return _REGISTRY[backend]

    if "." not in backend:
        known = ", ".join(sorted(_REGISTRY)) or "(none registered)"
        raise StorageConfigError(
            f"Unknown backend {backend!r}. Not a registered name (known: {known}) "
            "and not a dotted import path (no '.' found)."
        )

    module_path, _, class_name = backend.rpartition(".")
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise StorageConfigError(f"Could not import module {module_path!r} for backend {backend!r}") from exc

    try:
        cls = getattr(module, class_name)
    except AttributeError as exc:
        raise StorageConfigError(f"Module {module_path!r} has no attribute {class_name!r}") from exc

    if not (isinstance(cls, type) and issubclass(cls, Storage)):
        raise StorageConfigError(f"{backend!r} does not resolve to a Storage subclass")

    return cls


def build_storage(
    backend: str,
    config: dict[str, Any] | BaseStorageSettings | None = None,
    **kwargs: Any,
) -> Storage:
    """
    Construct a configured Storage instance.

    Parameters
    ----------
    backend:
        Either a registered short name ("local", "s3", "azure") or a dotted
        import path ("mypackage.backends.MyStorage").
    config:
        Optional dict of constructor kwargs, OR a BaseStorageSettings
        instance (its .to_kwargs() is called automatically).
    **kwargs:
        Additional/override constructor kwargs, merged on top of `config`.
        Lets callers do build_storage("s3", settings, bucket="override-bucket").

    Examples
    --------
        build_storage("local", {"base_path": "/data", "base_url": "/media"})

        settings = S3StorageSettings()  # from env
        build_storage("s3", settings)

        build_storage("mypackage.backends.MyStorage", {"option": "value"})
    """
    cls = _resolve_backend_class(backend)

    resolved_kwargs: dict[str, Any] = {}
    if isinstance(config, BaseStorageSettings):
        resolved_kwargs.update(config.to_kwargs())
    elif isinstance(config, dict):
        resolved_kwargs.update(config)
    elif config is not None:
        raise StorageConfigError(
            f"config must be a dict, a BaseStorageSettings instance, or None; got {type(config)!r}"
        )

    resolved_kwargs.update(kwargs)

    try:
        return cls(**resolved_kwargs)
    except TypeError as exc:
        raise StorageConfigError(
            f"Failed to construct backend {backend!r} ({cls.__name__}) with kwargs "
            f"{list(resolved_kwargs)}: {exc}"
        ) from exc


def list_registered_backends() -> list[str]:
    """Return the names of all currently registered backends."""
    return sorted(_REGISTRY)
