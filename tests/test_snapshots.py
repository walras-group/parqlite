from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import time
from urllib.parse import unquote, urlparse

import pandas as pd
import pytest

from parqlite import as_of, connect, ref, snapshot_id
from parqlite.errors import OrphanFileError, SnapshotError


def test_snapshots_and_current_snapshot(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})

    db.append("items", pd.DataFrame({"id": [1]}))
    time.sleep(0.02)
    db.append("items", pd.DataFrame({"id": [2]}))

    snapshots = db.snapshots("items")
    current = db.current_snapshot("items")

    assert len(snapshots) == 2
    assert all(snapshot.manifest_list for snapshot in snapshots)
    assert all(snapshot.committed_at.tzinfo is not None for snapshot in snapshots)
    assert snapshots[-1] == current
    assert db.snapshots("items", limit=1) == [current]


def test_sql_time_travel_uses_table_mapping_selectors(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})

    db.append("items", pd.DataFrame({"id": [1]}))
    first = db.current_snapshot("items")
    time.sleep(0.02)
    db.append("items", pd.DataFrame({"id": [2]}))

    db.create_tag("items", "first", at=snapshot_id(first.snapshot_id))

    assert db.sql("select id from items order by id").fetchall() == [(1,), (2,)]
    assert db.sql(
        "select id from items order by id",
        at={"items": snapshot_id(first.snapshot_id)},
    ).fetchall() == [(1,)]
    assert db.sql(
        "select id from items order by id",
        at={"items": as_of(first.committed_at)},
    ).fetchall() == [(1,)]
    assert db.sql(
        "select id from items order by id", at={"items": ref("first")}
    ).fetchall() == [(1,)]

    with pytest.raises(SnapshotError, match="table-name mapping"):
        db.sql("select id from items", at=snapshot_id(first.snapshot_id))


def test_multi_table_sql_can_pin_tables_independently(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("left_items", {"id": "long"})
    db.create_table("right_items", {"id": "long"})

    db.append("left_items", pd.DataFrame({"id": [1]}))
    first_left = db.current_snapshot("left_items")
    db.append("right_items", pd.DataFrame({"id": [10]}))

    db.append("left_items", pd.DataFrame({"id": [2]}))
    db.append("right_items", pd.DataFrame({"id": [20]}))
    latest_right = db.current_snapshot("right_items")

    assert db.sql(
        """
            select
                (select count(*) from left_items) as left_count,
                (select max(id) from right_items) as right_max
            """,
        at={
            "left_items": snapshot_id(first_left.snapshot_id),
            "right_items": snapshot_id(latest_right.snapshot_id),
        },
    ).fetchall() == [(1, 20)]


def test_tags_protect_snapshots_from_expiration(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})

    db.append("items", pd.DataFrame({"id": [1]}))
    first = db.current_snapshot("items")
    db.create_tag("items", "stable", at=snapshot_id(first.snapshot_id))
    db.append("items", pd.DataFrame({"id": [2]}))

    result = db.expire_snapshots("items", older_than=_future_cutoff())

    refs = {snapshot_ref.name: snapshot_ref for snapshot_ref in db.refs("items")}
    assert refs["stable"].type == "TAG"
    assert refs["stable"].snapshot_id == first.snapshot_id
    assert first.snapshot_id not in result.expired_snapshot_ids
    assert first.snapshot_id in {
        snapshot.snapshot_id for snapshot in db.snapshots("items")
    }

    db.delete_tag("items", "stable")
    result = db.expire_snapshots("items", older_than=_future_cutoff())

    assert first.snapshot_id in result.expired_snapshot_ids
    assert first.snapshot_id not in {
        snapshot.snapshot_id for snapshot in db.snapshots("items")
    }


def test_expire_snapshots_retain_last_keeps_latest_snapshots(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})

    for value in range(12):
        db.append("items", pd.DataFrame({"id": [value]}))

    expected = [snapshot.snapshot_id for snapshot in db.snapshots("items")[-10:]]

    result = db.expire_snapshots("items", older_than=_future_cutoff(), retain_last=10)

    assert result.expired_snapshots_count == 2
    assert [snapshot.snapshot_id for snapshot in db.snapshots("items")] == expected


def test_expire_snapshots_retain_last_keeps_protected_refs(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})

    db.append("items", pd.DataFrame({"id": [1]}))
    first = db.current_snapshot("items")
    db.create_tag("items", "stable", at=snapshot_id(first.snapshot_id))
    db.append("items", pd.DataFrame({"id": [2]}))
    middle = db.current_snapshot("items")
    db.append("items", pd.DataFrame({"id": [3]}))
    latest = db.current_snapshot("items")

    result = db.expire_snapshots("items", older_than=_future_cutoff(), retain_last=1)

    remaining = {snapshot.snapshot_id for snapshot in db.snapshots("items")}
    assert result.expired_snapshot_ids == [middle.snapshot_id]
    assert remaining == {first.snapshot_id, latest.snapshot_id}


def test_expire_snapshots_uses_default_table_properties(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table(
        "items",
        {"id": "long"},
        properties={
            "history.expire.max-snapshot-age-ms": 0,
            "history.expire.min-snapshots-to-keep": 2,
        },
    )

    for value in range(4):
        db.append("items", pd.DataFrame({"id": [value]}))

    expected = [snapshot.snapshot_id for snapshot in db.snapshots("items")[-2:]]

    result = db.expire_snapshots("items")

    assert result.expired_snapshots_count == 2
    assert [snapshot.snapshot_id for snapshot in db.snapshots("items")] == expected


def test_expire_snapshots_accepts_snapshot_ids(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})

    db.append("items", pd.DataFrame({"id": [1]}))
    first = db.current_snapshot("items")
    db.append("items", pd.DataFrame({"id": [2]}))

    result = db.expire_snapshots("items", snapshot_ids=[first.snapshot_id])

    assert result.expired_snapshot_ids == [first.snapshot_id]
    assert first.snapshot_id not in {
        snapshot.snapshot_id for snapshot in db.snapshots("items")
    }


def test_expire_snapshots_keeps_branch_heads(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})

    db.append("items", pd.DataFrame({"id": [1]}))
    first = db.current_snapshot("items")
    db._store.load_table("items").manage_snapshots().create_branch(
        first.snapshot_id,
        "audit",
    ).commit()
    db.append("items", pd.DataFrame({"id": [2]}))

    result = db.expire_snapshots("items", older_than=_future_cutoff())

    assert first.snapshot_id not in result.expired_snapshot_ids
    assert first.snapshot_id in {
        snapshot.snapshot_id for snapshot in db.snapshots("items")
    }


def test_metadata_retention_properties_prune_tracked_metadata_log(
    tmp_path: Path,
) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})
    db.set_table_properties(
        "items",
        {
            "write.metadata.delete-after-commit.enabled": True,
            "write.metadata.previous-versions-max": 1,
        },
    )

    for value in range(4):
        db.append("items", pd.DataFrame({"id": [value]}))

    table = db._store.load_table("items")
    metadata_log = table.metadata.metadata_log

    assert len(metadata_log) <= 1
    assert Path(unquote(urlparse(table.metadata_location).path)).exists()
    assert all(
        Path(unquote(urlparse(entry.metadata_file).path)).exists()
        for entry in metadata_log
    )


def test_remove_orphan_files_dry_run_previews_without_deleting_files(
    tmp_path: Path,
) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})

    db.append("items", pd.DataFrame({"id": [1]}))
    db.overwrite("items", pd.DataFrame({"id": [2]}))
    db.expire_snapshots("items", older_than=_future_cutoff())

    result = db.remove_orphan_files(
        "items",
        older_than=_future_cutoff(),
        dry_run=True,
    )

    assert result.dry_run is True
    assert result.deleted_files_count == 0
    assert result.deleted_bytes == 0
    assert result.files
    assert any(orphan.path.endswith(".parquet") for orphan in result.files)
    assert all(Path(orphan.path).exists() for orphan in result.files)
    assert db.sql("select id from items").fetchall() == [(2,)]


def test_remove_orphan_files_deletes_dry_run_candidates(
    tmp_path: Path,
) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})

    db.append("items", pd.DataFrame({"id": [1]}))
    db.overwrite("items", pd.DataFrame({"id": [2]}))
    db.expire_snapshots("items", older_than=_future_cutoff())

    preview = db.remove_orphan_files(
        "items",
        older_than=_future_cutoff(),
        dry_run=True,
    )
    removed = db.remove_orphan_files("items", older_than=_future_cutoff())

    assert [orphan.path for orphan in removed.files] == [
        orphan.path for orphan in preview.files
    ]
    assert removed.dry_run is False
    assert removed.deleted_files_count == len(preview.files)
    assert removed.deleted_bytes == sum(orphan.size_bytes for orphan in preview.files)
    assert ".parquet" in removed.by_suffix
    assert all(not Path(orphan.path).exists() for orphan in removed.files)
    assert db.sql("select id from items").fetchall() == [(2,)]


def test_remove_orphan_files_default_window_skips_new_files(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})
    orphan = _table_root(db, "items") / "new.tmp"
    orphan.write_text("orphan", encoding="utf-8")

    preview = db.remove_orphan_files("items", dry_run=True)

    assert str(orphan) not in {file.path for file in preview.files}

    _make_old(orphan)

    preview = db.remove_orphan_files("items", dry_run=True)
    removed = db.remove_orphan_files("items")

    assert str(orphan) in {file.path for file in preview.files}
    assert str(orphan) in {file.path for file in removed.files}
    assert not orphan.exists()


def test_remove_orphan_files_location_is_limited_to_table_root(
    tmp_path: Path,
) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})
    table_root = _table_root(db, "items")
    metadata_orphan = table_root / "metadata" / "old.tmp"
    root_orphan = table_root / "old.tmp"
    metadata_orphan.write_text("metadata", encoding="utf-8")
    root_orphan.write_text("root", encoding="utf-8")
    _make_old(metadata_orphan)
    _make_old(root_orphan)

    preview = db.remove_orphan_files("items", location="metadata", dry_run=True)

    assert [file.path for file in preview.files] == [str(metadata_orphan)]

    with pytest.raises(OrphanFileError, match="under table location"):
        db.remove_orphan_files(
            "items",
            location=tmp_path / "outside",
            dry_run=True,
        )


def test_retain_last_must_be_valid(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})
    db.append("items", pd.DataFrame({"id": [1]}))

    with pytest.raises(SnapshotError, match="retain_last"):
        db.expire_snapshots("items", retain_last=-1)

    with pytest.raises(SnapshotError, match="retain_last"):
        db.expire_snapshots("items", retain_last=True)

    with pytest.raises(TypeError, match="retain_last"):
        db.remove_orphan_files("items", retain_last=1)  # type: ignore[call-arg]


def test_rollback_changes_current_snapshot(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})

    db.append("items", pd.DataFrame({"id": [1]}))
    first = db.current_snapshot("items")
    db.append("items", pd.DataFrame({"id": [2]}))

    db.rollback_to("items", snapshot_id(first.snapshot_id))

    assert db.current_snapshot("items").snapshot_id == first.snapshot_id
    assert db.sql("select id from items order by id").fetchall() == [(1,)]


def test_orphan_files_are_listed_then_removed_after_expiration(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})

    db.append("items", pd.DataFrame({"id": [1]}))
    db.overwrite("items", pd.DataFrame({"id": [2]}))
    db.expire_snapshots("items", older_than=_future_cutoff())

    preview = db.remove_orphan_files(
        "items",
        older_than=_future_cutoff(),
        dry_run=True,
    )

    assert preview.files
    assert any(orphan.path.endswith(".parquet") for orphan in preview.files)
    assert all(Path(orphan.path).exists() for orphan in preview.files)
    assert db.sql("select id from items").fetchall() == [(2,)]

    removed = db.remove_orphan_files("items", older_than=_future_cutoff())

    assert [orphan.path for orphan in removed.files] == [
        orphan.path for orphan in preview.files
    ]
    assert all(not Path(orphan.path).exists() for orphan in removed.files)
    assert (
        db.remove_orphan_files(
            "items",
            older_than=_future_cutoff(),
            dry_run=True,
        ).files
        == []
    )
    assert db.sql("select id from items").fetchall() == [(2,)]


def test_datetime_inputs_must_be_timezone_aware(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long"})
    db.append("items", pd.DataFrame({"id": [1]}))

    with pytest.raises(SnapshotError, match="timezone-aware"):
        as_of(datetime(2024, 1, 1))

    with pytest.raises(SnapshotError, match="timezone-aware"):
        db.expire_snapshots("items", older_than=datetime(2024, 1, 1))

    with pytest.raises(OrphanFileError, match="timezone-aware"):
        db.remove_orphan_files("items", older_than=datetime(2024, 1, 1))


def _future_cutoff() -> datetime:
    return datetime.now(timezone.utc) + timedelta(days=1)


def _table_root(db, table: str) -> Path:
    parsed = urlparse(db._store.load_table(table).location())
    return Path(unquote(parsed.path)).resolve()


def _make_old(path: Path) -> None:
    old_timestamp = (datetime.now(timezone.utc) - timedelta(days=4)).timestamp()
    os.utime(path, (old_timestamp, old_timestamp))
