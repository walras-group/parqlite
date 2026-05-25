import pytest

from parquetdb.cli import main


def test_main_dispatches_ui_subcommand(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, str] = {}

    def fake_open_ui(path: str) -> None:
        calls["path"] = path

    monkeypatch.setattr("parquetdb.cli.open_ui", fake_open_ui)

    main(["ui", "./data"])

    assert calls == {"path": "./data"}


def test_main_requires_subcommand() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main([])

    assert exc_info.value.code == 2
