from pathlib import Path

import duckdb
import pandas as pd
import pytest

from parquetdb import connect
from parquetdb.duckdb_backend import DuckDBBackend
from parquetdb.errors import QueryBackendError
from parquetdb.iceberg import IcebergStore


def test_sql_refreshes_view_when_metadata_location_changes(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long", "name": "string"})

    first_metadata = db._store.table_metadata_location("items")
    assert db.sql("select count(*) from items").fetchone()[0] == 0

    db.append("items", pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}))
    second_metadata = db._store.table_metadata_location("items")

    assert second_metadata != first_metadata
    assert db.sql("select id, name from items order by id").fetchall() == [
        (1, "a"),
        (2, "b"),
    ]
    assert db._duckdb._registered["items"] == second_metadata


def test_overwrite_replaces_visible_snapshot(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long", "name": "string"})
    db.append("items", pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}))
    assert db.sql("select id from items order by id").fetchall() == [(1,), (2,)]

    db.overwrite("items", pd.DataFrame({"id": [3], "name": ["c"]}))

    assert db.sql("select id, name from items").fetchall() == [(3, "c")]


def test_view_names_are_quoted(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("select", {"id": "long"})
    db.append("select", pd.DataFrame({"id": [1]}))

    assert db.sql('select count(*) from "select"').fetchone()[0] == 1


def test_drop_table_removes_duckdb_view(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})
    db.append("items", pd.DataFrame({"id": [1]}))
    assert db.sql("select count(*) from items").fetchone()[0] == 1

    db.drop_table("items")

    with pytest.raises(duckdb.CatalogException):
        db.sql("select count(*) from items")
    assert "items" not in db._duckdb._registered


def test_iceberg_extension_failure_raises_query_backend_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingConnection:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute(self, query: str) -> None:
            self.queries.append(query)
            if query == "install iceberg":
                raise duckdb.IOException("Permission denied")

        def close(self) -> None:
            pass

    connection = FailingConnection()
    monkeypatch.setattr(
        "parquetdb.duckdb_backend.duckdb.connect",
        lambda _: connection,
    )

    backend = DuckDBBackend(IcebergStore(tmp_path))

    with pytest.raises(QueryBackendError) as exc_info:
        backend.sql("select 1")

    message = str(exc_info.value)
    assert "Permission denied" in message
    assert "DuckDB's default extension directory" in message
    assert not any("set extension_directory" in query for query in connection.queries)
