import pytest

from parquetdb.ui import main


def test_main_connects_to_path_and_opens_ui(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    class FakeDB:
        def open_ui(self) -> None:
            calls["opened"] = True

        def close(self) -> None:
            calls["closed"] = True

    def fake_connect(path: str) -> FakeDB:
        calls["path"] = path
        return FakeDB()

    monkeypatch.setattr("parquetdb.ui.connect", fake_connect)

    main(["./data"])

    assert calls == {
        "path": "./data",
        "opened": True,
        "closed": True,
    }


def test_main_requires_path() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main([])

    assert exc_info.value.code == 2
