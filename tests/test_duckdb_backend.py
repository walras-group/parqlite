import subprocess
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from parqlite import connect
from parqlite.duckdb_backend import DuckDBBackend, duckdb_ui_init_sql
from parqlite.errors import QueryBackendError
from parqlite.iceberg import IcebergStore


class FakeStore:
    def __init__(self, metadata_by_table: dict[str, str] | None = None):
        self.metadata_by_table = metadata_by_table or {}

    def tables(self) -> list[str]:
        return sorted(self.metadata_by_table)

    def table_metadata_location(self, name: str) -> str:
        return self.metadata_by_table[name]


def test_duckdb_ui_init_sql_loads_iceberg_with_no_tables() -> None:
    assert duckdb_ui_init_sql(FakeStore()) == "INSTALL iceberg;\nLOAD iceberg;\n"


def test_duckdb_ui_init_sql_registers_current_tables_as_views() -> None:
    sql = duckdb_ui_init_sql(
        FakeStore(
            {
                "select": "/tmp/has'quote/select/metadata.json",
                "binance.klines": "/tmp/binance/klines/metadata.json",
            }
        )
    )

    assert "INSTALL iceberg;" in sql
    assert "LOAD iceberg;" in sql
    assert (
        'CREATE OR REPLACE VIEW "select" AS '
        "SELECT * FROM iceberg_scan('/tmp/has''quote/select/metadata.json');"
    ) in sql
    assert 'CREATE SCHEMA IF NOT EXISTS "binance";' in sql
    assert (
        'CREATE OR REPLACE VIEW "binance"."klines" AS '
        "SELECT * FROM iceberg_scan('/tmp/binance/klines/metadata.json');"
    ) in sql


def test_duckdb_ui_launcher_runs_duckdb_with_generated_init_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_run(command: list[str], *, check: bool) -> subprocess.CompletedProcess:
        captured["command"] = command
        captured["check"] = check
        captured["sql"] = Path(command[2]).read_text(encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr("parqlite.duckdb_backend.subprocess.run", fake_run)

    backend = DuckDBBackend(FakeStore({"items": "/tmp/items/metadata.json"}))
    backend.open_ui()

    command = captured["command"]
    assert command[0] == "duckdb"
    assert command[1] == "-init"
    assert command[3] == "-ui"
    assert captured["check"] is False
    assert (
        'CREATE OR REPLACE VIEW "items" AS '
        "SELECT * FROM iceberg_scan('/tmp/items/metadata.json');"
    ) in captured["sql"]


def test_duckdb_ui_launcher_missing_cli_raises_query_backend_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(command: list[str], *, check: bool) -> subprocess.CompletedProcess:
        raise FileNotFoundError

    monkeypatch.setattr("parqlite.duckdb_backend.subprocess.run", fake_run)

    backend = DuckDBBackend(FakeStore())

    with pytest.raises(QueryBackendError, match="DuckDB CLI"):
        backend.open_ui()


def test_duckdb_ui_launcher_nonzero_exit_raises_query_backend_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(command: list[str], *, check: bool) -> subprocess.CompletedProcess:
        return subprocess.CompletedProcess(command, 7)

    monkeypatch.setattr("parqlite.duckdb_backend.subprocess.run", fake_run)

    backend = DuckDBBackend(FakeStore())

    with pytest.raises(QueryBackendError, match="status 7"):
        backend.open_ui()


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
        "parqlite.duckdb_backend.duckdb.connect",
        lambda _: connection,
    )

    backend = DuckDBBackend(IcebergStore(tmp_path))

    with pytest.raises(QueryBackendError) as exc_info:
        backend.sql("select 1")

    message = str(exc_info.value)
    assert "Permission denied" in message
    assert "DuckDB's default extension directory" in message
    assert not any("set extension_directory" in query for query in connection.queries)
