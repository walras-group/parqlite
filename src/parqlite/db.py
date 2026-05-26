from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from parqlite.duckdb_backend import DuckDBBackend
from parqlite.errors import SchemaError
from parqlite.iceberg import (
    KEYS_PROPERTY,
    RESERVED_PROPERTIES,
    VERSION_BY_PROPERTY,
    IcebergStore,
    parse_table_name,
)
from parqlite.io import to_arrow_table
from parqlite.partitioning import PartitionTransform, build_partition_spec
from parqlite.properties import TablePropertyKey, TablePropertyValue
from parqlite.schema import normalize_schema, schema_to_dict, to_iceberg_schema
from parqlite.snapshots import (
    ExpireSnapshotsResult,
    RemoveOrphanFilesResult,
    SnapshotRef,
    SnapshotSelector,
    TableSnapshot,
)
from parqlite.types import SchemaType


def connect(path: str | Path) -> DB:
    return DB(path)


class DB:
    def __init__(self, path: str | Path):
        self._store = IcebergStore(path)
        self._duckdb = DuckDBBackend(self._store)

    def create_namespace(self, name: str) -> None:
        self._store.create_namespace(name)

    def create_table(
        self,
        name: str,
        schema: Mapping[str, str | SchemaType],
        partition_by: list[str | PartitionTransform]
        | tuple[str | PartitionTransform, ...]
        | None = None,
        keys: list[str] | tuple[str, ...] | None = None,
        version_by: str | None = None,
        properties: Mapping[TablePropertyKey, TablePropertyValue] | None = None,
    ) -> None:
        parse_table_name(name)
        columns = normalize_schema(schema)
        column_names = {column.name for column in columns}
        _validate_reserved_metadata(column_names, keys, version_by)
        table_properties = _normalize_table_properties(properties)
        _validate_reserved_property_conflicts(table_properties)

        iceberg_schema = to_iceberg_schema(columns)
        partition_spec = build_partition_spec(iceberg_schema, partition_by)
        table_properties.update(_reserved_properties(keys, version_by))
        self._store.create_table(
            name,
            schema=iceberg_schema,
            partition_spec=partition_spec,
            properties=table_properties,
        )

    def append(self, name: str, data: Any) -> None:
        table = self._store.load_table(name)
        arrow_table = to_arrow_table(data, table.schema().as_arrow())
        self._store.append(name, arrow_table)

    def overwrite(self, name: str, data: Any) -> None:
        table = self._store.load_table(name)
        arrow_table = to_arrow_table(data, table.schema().as_arrow())
        self._store.overwrite(name, arrow_table)

    def sql(
        self,
        query: str,
        *,
        at: dict[str, SnapshotSelector] | None = None,
    ):
        return self._duckdb.sql(query, at=at)

    def open_ui(self) -> None:
        self._duckdb.open_ui()

    def tables(self) -> list[str]:
        return self._store.tables()

    def schema(self, name: str) -> dict[str, str]:
        return schema_to_dict(self._store.load_table(name).schema())

    def table_properties(self, table: str) -> dict[str, str]:
        return self._store.table_properties(table)

    def set_table_properties(
        self,
        table: str,
        properties: Mapping[TablePropertyKey, TablePropertyValue],
    ) -> dict[str, str]:
        normalized = _normalize_table_properties(properties)
        _validate_reserved_property_conflicts(normalized)
        return self._store.set_table_properties(table, normalized)

    def remove_table_properties(
        self,
        table: str,
        keys: Sequence[str],
    ) -> dict[str, str]:
        normalized = _validate_table_property_keys(keys)
        _validate_reserved_property_conflicts(normalized)
        return self._store.remove_table_properties(table, normalized)

    def drop_table(self, name: str) -> None:
        self._store.drop_table(name)

    def current_snapshot(self, table: str) -> TableSnapshot:
        return self._store.current_snapshot(table)

    def snapshots(self, table: str, limit: int | None = None) -> list[TableSnapshot]:
        return self._store.snapshots(table, limit=limit)

    def refs(self, table: str) -> list[SnapshotRef]:
        return self._store.refs(table)

    def create_tag(
        self,
        table: str,
        tag: str,
        at: SnapshotSelector | None = None,
    ) -> None:
        self._store.create_tag(table, tag, at=at)

    def delete_tag(self, table: str, tag: str) -> None:
        self._store.delete_tag(table, tag)

    def rollback_to(self, table: str, target: SnapshotSelector) -> None:
        self._store.rollback_to(table, target)

    def expire_snapshots(
        self,
        table: str,
        *,
        older_than: datetime | timedelta | None = None,
        snapshot_ids: Sequence[int] | None = None,
        retain_last: int | None = None,
    ) -> ExpireSnapshotsResult:
        return self._store.expire_snapshots(
            table,
            older_than=older_than,
            snapshot_ids=snapshot_ids,
            retain_last=retain_last,
        )

    def remove_orphan_files(
        self,
        table: str,
        *,
        older_than: datetime | timedelta | None = None,
        location: str | Path | None = None,
        dry_run: bool = False,
    ) -> RemoveOrphanFilesResult:
        return self._store.remove_orphan_files(
            table,
            older_than=older_than,
            location=location,
            dry_run=dry_run,
        )

    def close(self) -> None:
        self._duckdb.close()


def _validate_reserved_metadata(
    column_names: set[str],
    keys: list[str] | tuple[str, ...] | None,
    version_by: str | None,
) -> None:
    if keys is not None:
        seen: set[str] = set()
        for key in keys:
            if not isinstance(key, str) or not key:
                raise SchemaError("keys must be non-empty column names")
            if key in seen:
                raise SchemaError(f"duplicate key column: {key}")
            if key not in column_names:
                raise SchemaError(f"key column does not exist in schema: {key}")
            seen.add(key)

    if version_by is not None:
        if not isinstance(version_by, str) or not version_by:
            raise SchemaError("version_by must be a non-empty column name")
        if version_by not in column_names:
            raise SchemaError(
                f"version_by column does not exist in schema: {version_by}"
            )


def _normalize_table_properties(
    properties: Mapping[TablePropertyKey, TablePropertyValue] | None,
) -> dict[str, str]:
    if properties is None:
        return {}
    if not isinstance(properties, Mapping):
        raise SchemaError("properties must be a mapping")

    normalized: dict[str, str] = {}
    for key, value in properties.items():
        if not isinstance(key, str) or not key:
            raise SchemaError("table property keys must be non-empty strings")

        if isinstance(value, bool):
            normalized[key] = "true" if value else "false"
        elif isinstance(value, int):
            normalized[key] = str(value)
        elif isinstance(value, str):
            normalized[key] = value
        else:
            raise SchemaError(
                "table property values must be strings, integers, or booleans"
            )

    return normalized


def _validate_table_property_keys(keys: Sequence[str]) -> list[str]:
    if isinstance(keys, str | bytes):
        raise SchemaError("table property keys must be a sequence of strings")

    normalized: list[str] = []
    for key in keys:
        if not isinstance(key, str) or not key:
            raise SchemaError("table property keys must be non-empty strings")
        normalized.append(key)
    return normalized


def _validate_reserved_property_conflicts(
    properties: Mapping[str, object] | Sequence[str],
) -> None:
    keys = (
        set(properties.keys()) if isinstance(properties, Mapping) else set(properties)
    )
    conflicts = keys & RESERVED_PROPERTIES
    if conflicts:
        raise SchemaError(
            "reserved table properties cannot be modified by user: "
            + ", ".join(sorted(conflicts))
        )


def _reserved_properties(
    keys: list[str] | tuple[str, ...] | None,
    version_by: str | None,
) -> dict[str, str]:
    properties: dict[str, str] = {}
    if keys:
        properties[KEYS_PROPERTY] = ",".join(keys)
    if version_by:
        properties[VERSION_BY_PROPERTY] = version_by
    return properties
