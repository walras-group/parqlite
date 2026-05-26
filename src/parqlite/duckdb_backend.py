from __future__ import annotations

import subprocess
import tempfile
from collections.abc import Iterable
from pathlib import Path

import duckdb

from parqlite.errors import QueryBackendError, SnapshotError
from parqlite.iceberg import DEFAULT_NAMESPACE, IcebergStore, parse_table_name
from parqlite.snapshots import SnapshotSelector


class DuckDBBackend:
    def __init__(self, store: IcebergStore):
        self._store = store
        self._connection = duckdb.connect(":memory:")
        self._iceberg_extension_loaded = False
        self._registered: dict[str, str] = {}
        self._registered_sql: dict[str, str] = {}

    def sql(
        self,
        query: str,
        *,
        at: dict[str, SnapshotSelector] | None = None,
    ) -> duckdb.DuckDBPyRelation:
        if at is not None and not isinstance(at, dict):
            raise SnapshotError("sql(at=...) requires a table-name mapping")

        self._ensure_iceberg_extension()
        self._refresh_views(at or {})
        return self._connection.sql(query)

    def close(self) -> None:
        self._connection.close()

    def open_ui(self) -> None:
        init_sql = duckdb_ui_init_sql(self._store)

        with tempfile.TemporaryDirectory() as temp_dir:
            init_path = Path(temp_dir) / "parqlite-duckdb-ui-init.sql"
            init_path.write_text(init_sql, encoding="utf-8")

            try:
                result = subprocess.run(
                    ["duckdb", "-init", str(init_path), "-ui"],
                    check=False,
                )
            except FileNotFoundError as exc:
                raise QueryBackendError(
                    "DuckDB CLI is required to open the DuckDB UI. "
                    "Install duckdb and make sure it is available on PATH."
                ) from exc

        if result.returncode != 0:
            raise QueryBackendError(
                f"DuckDB UI exited unsuccessfully with status {result.returncode}."
            )

    def _ensure_iceberg_extension(self) -> None:
        if self._iceberg_extension_loaded:
            return

        try:
            self._connection.execute("install iceberg")
            self._connection.execute("load iceberg")
        except duckdb.Error as exc:
            raise QueryBackendError(
                "DuckDB Iceberg query backend is unavailable. "
                "Failed to install or load DuckDB's iceberg extension using "
                "DuckDB's default extension directory. "
                "DuckDB's default extension directory may be unwritable, or "
                "DuckDB may be unable to download or load the iceberg extension. "
                f"Original DuckDB error: {exc}"
            ) from exc

        self._iceberg_extension_loaded = True

    def _refresh_views(self, at: dict[str, SnapshotSelector]) -> None:
        current_metadata = {
            name: self._store.table_metadata_location(name)
            for name in self._store.tables()
        }

        for name in at:
            if not isinstance(name, str):
                raise SnapshotError("sql(at=...) table names must be strings")
            if name not in current_metadata:
                self._store.load_table(name)

        for name in list(self._registered):
            if name not in current_metadata:
                self._connection.execute(
                    f"drop view if exists {_qualified_view_name(name)}"
                )
                del self._registered[name]
                del self._registered_sql[name]

        for name, metadata_location in current_metadata.items():
            scan = f"iceberg_scan({_sql_string(metadata_location)})"
            if name in at:
                scan_options = self._store.duckdb_scan_options(name, at[name])
                scan = f"iceberg_scan({_sql_string(metadata_location)}, {scan_options})"

            view_sql = f"select * from {scan}"
            if self._registered_sql.get(name) == view_sql:
                continue

            _ensure_view_schema(self._connection, name)
            self._connection.execute(
                f"create or replace view {_qualified_view_name(name)} as {view_sql}"
            )
            self._registered[name] = metadata_location
            self._registered_sql[name] = view_sql


def duckdb_ui_init_sql(store: IcebergStore) -> str:
    lines = [
        "INSTALL iceberg;",
        "LOAD iceberg;",
    ]

    for name in store.tables():
        schema_sql = _create_view_schema_sql(name)
        if schema_sql is not None:
            lines.append(schema_sql)

        metadata_location = store.table_metadata_location(name)
        lines.append(
            "CREATE OR REPLACE VIEW "
            f"{_qualified_view_name(name)} AS "
            f"SELECT * FROM iceberg_scan({_sql_string(metadata_location)});"
        )

    return "\n".join(lines) + "\n"


def _ensure_view_schema(connection: duckdb.DuckDBPyConnection, name: str) -> None:
    schema_sql = _create_view_schema_sql(name)
    if schema_sql is None:
        return

    connection.execute(schema_sql)


def _create_view_schema_sql(name: str) -> str | None:
    namespace, _ = parse_table_name(name)
    if namespace == DEFAULT_NAMESPACE:
        return None

    return f"CREATE SCHEMA IF NOT EXISTS {_quoted_identifier(namespace)};"


def _qualified_view_name(name: str) -> str:
    namespace, table = parse_table_name(name)
    if namespace == DEFAULT_NAMESPACE:
        return _quoted_identifier(table)
    return ".".join(_quoted_identifiers([namespace, table]))


def _quoted_identifiers(identifiers: Iterable[str]) -> list[str]:
    return [_quoted_identifier(identifier) for identifier in identifiers]


def _quoted_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _sql_string(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
