import pytest

from parqlite.cli import main


class FakeDB:
    def __init__(
        self,
        calls: list[tuple[str, str | None]],
        *,
        fail_open_ui: bool = False,
    ):
        self._calls = calls
        self._fail_open_ui = fail_open_ui

    def __enter__(self) -> "FakeDB":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def open_ui(self) -> None:
        self._calls.append(("open_ui", None))
        if self._fail_open_ui:
            raise RuntimeError("boom")

    def open_shell(self, query: str | None = None) -> None:
        self._calls.append(("open_shell", query))

    def close(self) -> None:
        self._calls.append(("close", None))


def test_main_dispatches_ui_subcommand(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str | None]] = []

    def fake_connect(path: str) -> FakeDB:
        calls.append(("connect", path))
        return FakeDB(calls)

    monkeypatch.setattr("parqlite.cli.connect", fake_connect)

    main(["ui", "./data"])

    assert calls == [
        ("connect", "./data"),
        ("open_ui", None),
        ("close", None),
    ]


def test_main_closes_db_when_open_ui_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str | None]] = []

    def fake_connect(path: str) -> FakeDB:
        calls.append(("connect", path))
        return FakeDB(calls, fail_open_ui=True)

    monkeypatch.setattr("parqlite.cli.connect", fake_connect)

    with pytest.raises(RuntimeError, match="boom"):
        main(["ui", "./data"])

    assert calls == [
        ("connect", "./data"),
        ("open_ui", None),
        ("close", None),
    ]


def test_main_tables_runs_duckdb_catalog_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str | None]] = []

    def fake_connect(path: str) -> FakeDB:
        calls.append(("connect", path))
        return FakeDB(calls)

    monkeypatch.setattr("parqlite.cli.connect", fake_connect)

    main(["tables", "./data"])

    assert calls == [
        ("connect", "./data"),
        (
            "open_shell",
            "SELECT\n"
            "    CASE\n"
            "        WHEN schema_name = 'main' THEN view_name\n"
            "        ELSE schema_name || '.' || view_name\n"
            '    END AS "Tables"\n'
            "FROM duckdb_views()\n"
            "WHERE database_name = 'memory'\n"
            "    AND NOT internal\n"
            'ORDER BY "Tables"',
        ),
        ("close", None),
    ]


def test_main_schema_runs_duckdb_describe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str | None]] = []

    def fake_connect(path: str) -> FakeDB:
        calls.append(("connect", path))
        return FakeDB(calls)

    monkeypatch.setattr("parqlite.cli.connect", fake_connect)

    main(["schema", "./data", "binance.funding_rates"])

    assert calls == [
        ("connect", "./data"),
        ("open_shell", 'DESCRIBE "binance"."funding_rates"'),
        ("close", None),
    ]


def test_main_sql_runs_query_in_duckdb_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str | None]] = []

    def fake_connect(path: str) -> FakeDB:
        calls.append(("connect", path))
        return FakeDB(calls)

    monkeypatch.setattr("parqlite.cli.connect", fake_connect)

    main(["sql", "./data", "select id, name from items"])

    assert calls == [
        ("connect", "./data"),
        ("open_shell", "select id, name from items"),
        ("close", None),
    ]


def test_main_sql_without_query_opens_interactive_duckdb_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str | None]] = []

    def fake_connect(path: str) -> FakeDB:
        calls.append(("connect", path))
        return FakeDB(calls)

    monkeypatch.setattr("parqlite.cli.connect", fake_connect)

    main(["sql", "./data"])

    assert calls == [
        ("connect", "./data"),
        ("open_shell", None),
        ("close", None),
    ]


def test_main_requires_subcommand() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main([])

    assert exc_info.value.code == 2
