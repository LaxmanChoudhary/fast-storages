"""
SQLAlchemy model mixin for the PostgreSQL storage backend metadata table.

This module provides a declarative mixin that defines the ``storage_files``
table schema.  Import it into your application and combine it with your
SQLAlchemy ``Base`` so that Alembic (or any other migration tool) picks up
the table automatically.

Example
-------
::

    from sqlalchemy.orm import DeclarativeBase
    from fast_storages.backends.postgresql_schema import StorageFileMixin

    class Base(DeclarativeBase):
        pass

    class StorageFile(StorageFileMixin, Base):
        # Optionally override __tablename__, add extra columns, indexes, etc.
        pass

    # The ``storage_files`` table is now part of ``Base.metadata`` and will
    # be included in ``alembic revision --autogenerate``.

If you need a different table name (e.g. per-tenant isolation), override
``__tablename__`` on the concrete class::

    class TenantStorageFile(StorageFileMixin, Base):
        __tablename__ = "tenant_storage_files"

and pass the same name to ``PostgreSQLStorage(table_name="tenant_storage_files")``.

Notes
-----
- The mixin uses ``Column`` (not ``mapped_column``) for compatibility with
  both SQLAlchemy 1.4+ and 2.x.
- ``loid`` is stored as ``BIGINT`` rather than ``OID`` to avoid
  signed-overflow issues in some ORMs; PostgreSQL ``OID`` is a 32-bit
  unsigned integer that fits comfortably in a ``BIGINT``.
- ``created_at`` / ``updated_at`` use ``server_default=func.now()`` so the
  timestamps are set by the database.  The raw-SQL backend sets
  ``updated_at = now()`` explicitly in its upsert queries.
"""
from __future__ import annotations

from sqlalchemy import BigInteger, Column, DateTime, Text, func


class StorageFileMixin:
    """
    Declarative mixin for the PostgreSQL storage backend metadata table.

    Inherit from this mixin alongside your SQLAlchemy ``Base`` to include
    the storage metadata table in your Alembic migration graph.

    Attributes
    ----------
    name : str
        Logical file path / storage key (primary key).
    loid : int
        PostgreSQL Large Object OID.
    size : int
        File size in bytes.
    content_type : str | None
        MIME type (e.g. ``"image/png"``).
    created_at : datetime
        Row creation timestamp (database-side default).
    updated_at : datetime
        Last-update timestamp (database-side default on insert; set
        explicitly by backend SQL on update).
    """

    __tablename__ = "storage_files"

    name = Column(
        Text, primary_key=True,
        comment="Logical file path / storage key",
    )
    loid = Column(
        BigInteger, nullable=False,
        comment="PostgreSQL Large Object OID",
    )
    size = Column(
        BigInteger, nullable=False, default=0,
        comment="File size in bytes",
    )
    content_type = Column(
        Text, nullable=True,
        comment="MIME content type",
    )
    created_at = Column(
        DateTime(timezone=True), nullable=False,
        server_default=func.now(),
        comment="Row creation timestamp",
    )
    updated_at = Column(
        DateTime(timezone=True), nullable=False,
        server_default=func.now(),
        comment="Last update timestamp",
    )
