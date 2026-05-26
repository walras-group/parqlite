from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from parqlite.errors import SnapshotError


@dataclass(frozen=True, slots=True)
class _SnapshotIdSelector:
    id: int


@dataclass(frozen=True, slots=True)
class _AsOfSelector:
    timestamp: datetime


@dataclass(frozen=True, slots=True)
class _RefSelector:
    name: str


SnapshotSelector = _SnapshotIdSelector | _AsOfSelector | _RefSelector


@dataclass(frozen=True, slots=True)
class TableSnapshot:
    snapshot_id: int
    parent_id: int | None
    committed_at: datetime
    operation: str
    manifest_list: str
    summary: dict[str, str]


@dataclass(frozen=True, slots=True)
class SnapshotRef:
    name: str
    type: str
    snapshot_id: int


@dataclass(frozen=True, slots=True)
class OrphanFile:
    path: str
    size_bytes: int
    modified_at: datetime


@dataclass(frozen=True, slots=True)
class ExpireSnapshotsResult:
    expired_snapshot_ids: list[int]
    expired_snapshots_count: int


@dataclass(frozen=True, slots=True)
class RemoveOrphanFilesResult:
    files: list[OrphanFile]
    dry_run: bool
    deleted_files_count: int
    deleted_bytes: int
    by_suffix: dict[str, int]


def snapshot_id(id: int) -> SnapshotSelector:
    if not isinstance(id, int) or isinstance(id, bool):
        raise SnapshotError("snapshot_id selector requires an integer snapshot id")
    return _SnapshotIdSelector(id)


def as_of(timestamp: datetime) -> SnapshotSelector:
    if not isinstance(timestamp, datetime):
        raise SnapshotError("as_of selector requires a datetime")
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise SnapshotError("as_of selector requires a timezone-aware datetime")
    return _AsOfSelector(timestamp)


def ref(name: str) -> SnapshotSelector:
    if not isinstance(name, str) or not name:
        raise SnapshotError("ref selector requires a non-empty reference name")
    return _RefSelector(name)


__all__ = [
    "ExpireSnapshotsResult",
    "OrphanFile",
    "RemoveOrphanFilesResult",
    "SnapshotRef",
    "SnapshotSelector",
    "TableSnapshot",
    "as_of",
    "ref",
    "snapshot_id",
]
