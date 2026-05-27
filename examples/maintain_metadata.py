from __future__ import annotations

from datetime import timedelta

from parqlite import connect


DB_PATH = "./crypto"
TABLE_NAME = "binance.klines"
SNAPSHOT_RETENTION = timedelta(days=7)
ORPHAN_RETENTION = timedelta(days=3)
RETAIN_LAST_SNAPSHOTS = 10


def main() -> None:
    db = connect(DB_PATH)
    try:
        expired = db.expire_snapshots(
            TABLE_NAME,
            older_than=SNAPSHOT_RETENTION,
            retain_last=RETAIN_LAST_SNAPSHOTS,
        )
        print(
            "expired "
            f"{expired.expired_snapshots_count} snapshots: "
            f"{expired.expired_snapshot_ids}"
        )

        preview = db.remove_orphan_files(
            TABLE_NAME,
            older_than=ORPHAN_RETENTION,
            location="metadata",
            dry_run=True,
        )
        print(
            "metadata orphan preview: "
            f"{len(preview.files)} files, "
            f"{preview.deleted_bytes} bytes, "
            f"by suffix {preview.by_suffix}"
        )

        removed = db.remove_orphan_files(
            TABLE_NAME,
            older_than=ORPHAN_RETENTION,
            location="metadata",
        )
        print(
            "deleted metadata orphans: "
            f"{removed.deleted_files_count} files, "
            f"{removed.deleted_bytes} bytes"
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
