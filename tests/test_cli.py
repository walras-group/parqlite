import pytest

from parqlite.cli import main


def test_main_dispatches_ui_subcommand(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str | None]] = []

    class FakeDB:
        def open_ui(self) -> None:
            calls.append(("open_ui", None))

        def close(self) -> None:
            calls.append(("close", None))

    def fake_connect(path: str) -> FakeDB:
        calls.append(("connect", path))
        return FakeDB()

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

    class FakeDB:
        def open_ui(self) -> None:
            calls.append(("open_ui", None))
            raise RuntimeError("boom")

        def close(self) -> None:
            calls.append(("close", None))

    def fake_connect(path: str) -> FakeDB:
        calls.append(("connect", path))
        return FakeDB()

    monkeypatch.setattr("parqlite.cli.connect", fake_connect)

    with pytest.raises(RuntimeError, match="boom"):
        main(["ui", "./data"])

    assert calls == [
        ("connect", "./data"),
        ("open_ui", None),
        ("close", None),
    ]


def test_main_requires_subcommand() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main([])

    assert exc_info.value.code == 2
