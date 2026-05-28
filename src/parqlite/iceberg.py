from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from urllib.parse import unquote, urlparse

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError as IcebergNamespaceAlreadyExistsError,
    NamespaceNotEmptyError as IcebergNamespaceNotEmptyError,
    NoSuchTableError,
    NoSuchNamespaceError as IcebergNamespaceNotFoundError,
    TableAlreadyExistsError as IcebergTableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from pyiceberg.table import Table, TableProperties, UpsertResult
from pyiceberg.utils.properties import property_as_int

from parqlite.errors import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    OrphanFileError,
    SchemaError,
    SnapshotError,
    TableAlreadyExistsError,
    TableNotFoundError,
)
from parqlite.snapshots import (
    ExpireSnapshotsResult,
    OrphanFile,
    RemoveOrphanFilesResult,
    SnapshotRef,
    SnapshotSelector,
    TableSnapshot,
    _AsOfSelector,
    _RefSelector,
    _SnapshotIdSelector,
)


DEFAULT_NAMESPACE = "default"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KEYS_PROPERTY = "parqlite.keys"
VERSION_BY_PROPERTY = "parqlite.version_by"
RESERVED_PROPERTIES = frozenset({KEYS_PROPERTY, VERSION_BY_PROPERTY})
DEFAULT_ORPHAN_RETENTION = timedelta(days=3)


def validate_namespace_name(name: str) -> str:
    _validate_identifier(name, "namespace")
    return name


def parse_table_name(name: str) -> tuple[str, str]:
    if not isinstance(name, str):
        raise SchemaError(
            "table name must be a valid SQL identifier or namespace.table name"
        )

    parts = name.split(".")
    if len(parts) == 1:
        namespace = DEFAULT_NAMESPACE
        table = parts[0]
    elif len(parts) == 2:
        namespace, table = parts
    else:
        raise SchemaError(
            "table name must be a valid SQL identifier or namespace.table name"
        )

    _validate_identifier(namespace, "namespace")
    _validate_identifier(table, "table")
    return namespace, table


def _validate_identifier(value: str, label: str) -> None:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise SchemaError(
            f"{label} name must be a valid SQL identifier: letters, numbers, "
            "and underscores; it must not start with a number"
        )


def _table_name_from_identifier(identifier: tuple[str, ...]) -> str:
    namespace = ".".join(identifier[:-1])
    table = identifier[-1]
    if namespace == DEFAULT_NAMESPACE:
        return table
    return f"{namespace}.{table}"


def _namespace_name_from_identifier(identifier: str | tuple[str, ...]) -> str:
    if isinstance(identifier, str):
        return identifier
    return ".".join(identifier)


class IcebergStore:
    def __init__(self, root: str | Path):
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.warehouse = self.root / "warehouse"
        self.warehouse.mkdir(parents=True, exist_ok=True)
        self.catalog_path = self.root / "catalog.db"
        self.catalog = self._load_catalog()
        self.catalog.create_namespace_if_not_exists(DEFAULT_NAMESPACE)

    def create_namespace(self, name: str) -> None:
        namespace = validate_namespace_name(name)
        try:
            self.catalog.create_namespace(namespace)
        except IcebergNamespaceAlreadyExistsError as exc:
            raise NamespaceAlreadyExistsError(
                f"namespace already exists: {namespace}"
            ) from exc

    def list_namespaces(self) -> list[str]:
        return sorted(
            _namespace_name_from_identifier(identifier)
            for identifier in self.catalog.list_namespaces()
        )

    def drop_namespace(self, name: str) -> None:
        namespace = validate_namespace_name(name)
        if namespace == DEFAULT_NAMESPACE:
            raise SchemaError("default namespace cannot be dropped")

        try:
            self.catalog.drop_namespace(namespace)
        except IcebergNamespaceNotFoundError as exc:
            raise NamespaceNotFoundError(f"namespace not found: {namespace}") from exc
        except IcebergNamespaceNotEmptyError as exc:
            raise NamespaceNotEmptyError(
                f"namespace is not empty: {namespace}"
            ) from exc

    def create_table(
        self,
        name: str,
        schema: Schema,
        partition_spec,
        properties: Mapping[str, str],
    ) -> None:
        try:
            self.catalog.create_table(
                self._identifier(name),
                schema=schema,
                partition_spec=partition_spec,
                properties=properties,
            )
        except IcebergNamespaceNotFoundError as exc:
            namespace, _ = parse_table_name(name)
            raise NamespaceNotFoundError(f"namespace not found: {namespace}") from exc
        except IcebergTableAlreadyExistsError as exc:
            raise TableAlreadyExistsError(f"table already exists: {name}") from exc

    def load_table(self, name: str) -> Table:
        try:
            return self.catalog.load_table(self._identifier(name))
        except NoSuchTableError as exc:
            raise TableNotFoundError(f"table not found: {name}") from exc

    def append(self, name: str, table: pa.Table) -> None:
        iceberg_table = self.load_table(name)
        iceberg_table.append(table)

    def overwrite(self, name: str, table: pa.Table) -> None:
        iceberg_table = self.load_table(name)
        iceberg_table.overwrite(table)

    def upsert(self, name: str, table: pa.Table, join_cols: list[str]) -> UpsertResult:
        iceberg_table = self.load_table(name)
        return iceberg_table.upsert(table, join_cols=join_cols)

    def drop_table(self, name: str) -> None:
        try:
            self.catalog.drop_table(self._identifier(name))
        except NoSuchTableError as exc:
            raise TableNotFoundError(f"table not found: {name}") from exc

    def tables(self) -> list[str]:
        names: list[str] = []
        for namespace in self.catalog.list_namespaces():
            for identifier in self.catalog.list_tables(namespace):
                names.append(_table_name_from_identifier(identifier))
        return sorted(names)

    def table_exists(self, name: str) -> bool:
        return self.catalog.table_exists(self._identifier(name))

    def table_metadata_location(self, name: str) -> str:
        return self.load_table(name).metadata_location

    def table_properties(self, name: str) -> dict[str, str]:
        return dict(self.load_table(name).properties)

    def set_table_properties(
        self,
        name: str,
        properties: Mapping[str, str],
    ) -> dict[str, str]:
        if not properties:
            return self.table_properties(name)

        table = self.load_table(name)
        try:
            table.transaction().set_properties(properties).commit_transaction()
        except Exception as exc:
            raise SnapshotError(
                f"failed to set table properties for table {name!r}"
            ) from exc
        return self.table_properties(name)

    def remove_table_properties(
        self,
        name: str,
        keys: Sequence[str],
    ) -> dict[str, str]:
        if not keys:
            return self.table_properties(name)

        table = self.load_table(name)
        try:
            table.transaction().remove_properties(*keys).commit_transaction()
        except Exception as exc:
            raise SnapshotError(
                f"failed to remove table properties for table {name!r}"
            ) from exc
        return self.table_properties(name)

    def current_snapshot(self, name: str) -> TableSnapshot:
        table = self.load_table(name)
        current = table.current_snapshot()
        if current is None:
            raise SnapshotError(f"table has no snapshots: {name}")

        snapshots = self._table_snapshots(table)
        for snapshot in snapshots:
            if snapshot.snapshot_id == current.snapshot_id:
                return snapshot

        raise SnapshotError(
            f"current snapshot is missing from table metadata for table: {name}"
        )

    def snapshots(self, name: str, limit: int | None = None) -> list[TableSnapshot]:
        if limit is not None:
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
                raise SnapshotError("snapshot limit must be a non-negative integer")

        snapshots = self._table_snapshots(self.load_table(name))
        if limit is None:
            return snapshots
        if limit == 0:
            return []
        return snapshots[-limit:]

    def refs(self, name: str) -> list[SnapshotRef]:
        rows = self.load_table(name).inspect.refs().to_pylist()
        return [
            SnapshotRef(
                name=row["name"],
                type=row["type"],
                snapshot_id=row["snapshot_id"],
            )
            for row in rows
        ]

    def duckdb_scan_options(self, name: str, selector: SnapshotSelector) -> str:
        table = self.load_table(name)

        if isinstance(selector, _SnapshotIdSelector):
            self._snapshot_for_selector(table, name, selector)
            return f"snapshot_from_id={selector.id}"

        if isinstance(selector, _AsOfSelector):
            self._snapshot_for_selector(table, name, selector)
            return f"snapshot_from_timestamp={_timestamp_literal(selector.timestamp)}"

        if isinstance(selector, _RefSelector):
            snapshot = self._snapshot_for_selector(table, name, selector)
            return f"snapshot_from_id={snapshot.snapshot_id}"

        raise SnapshotError("invalid snapshot selector")

    def create_tag(
        self,
        name: str,
        tag: str,
        at: SnapshotSelector | None = None,
    ) -> None:
        if not isinstance(tag, str) or not tag:
            raise SnapshotError("tag name must be a non-empty string")

        table = self.load_table(name)
        if at is None:
            snapshot = table.current_snapshot()
            if snapshot is None:
                raise SnapshotError(f"table has no snapshots: {name}")
        else:
            snapshot = self._snapshot_for_selector(table, name, at)

        try:
            table.manage_snapshots().create_tag(snapshot.snapshot_id, tag).commit()
        except Exception as exc:
            raise SnapshotError(
                f"failed to create tag {tag!r} on table {name!r}"
            ) from exc

    def delete_tag(self, name: str, tag: str) -> None:
        if not isinstance(tag, str) or not tag:
            raise SnapshotError("tag name must be a non-empty string")

        refs = {snapshot_ref.name: snapshot_ref for snapshot_ref in self.refs(name)}
        if tag not in refs or refs[tag].type != "TAG":
            raise SnapshotError(f"tag not found for table {name!r}: {tag}")

        table = self.load_table(name)
        try:
            table.manage_snapshots().remove_tag(tag).commit()
        except Exception as exc:
            raise SnapshotError(
                f"failed to delete tag {tag!r} on table {name!r}"
            ) from exc

    def rollback_to(self, name: str, target: SnapshotSelector) -> None:
        table = self.load_table(name)

        try:
            if isinstance(target, _AsOfSelector):
                self._snapshot_for_selector(table, name, target)
                table.manage_snapshots().rollback_to_timestamp(
                    _datetime_to_millis(target.timestamp)
                ).commit()
                return

            snapshot = self._snapshot_for_selector(table, name, target)
            table.manage_snapshots().rollback_to_snapshot(snapshot.snapshot_id).commit()
        except SnapshotError:
            raise
        except Exception as exc:
            raise SnapshotError(f"failed to roll back table {name!r}") from exc

    def expire_snapshots(
        self,
        name: str,
        *,
        older_than: datetime | timedelta | None = None,
        snapshot_ids: Sequence[int] | None = None,
        retain_last: int | None = None,
    ) -> ExpireSnapshotsResult:
        retain_last = _validate_retain_last(retain_last)
        table = self.load_table(name)
        candidate_ids = self._snapshot_expiration_candidate_ids(
            table,
            older_than=older_than,
            snapshot_ids=snapshot_ids,
            retain_last=retain_last,
        )

        if not candidate_ids:
            return ExpireSnapshotsResult(
                expired_snapshot_ids=[],
                expired_snapshots_count=0,
            )

        try:
            table.maintenance.expire_snapshots().by_ids(candidate_ids).commit()
        except Exception as exc:
            raise SnapshotError(
                f"failed to expire snapshots for table {name!r}"
            ) from exc

        return ExpireSnapshotsResult(
            expired_snapshot_ids=candidate_ids,
            expired_snapshots_count=len(candidate_ids),
        )

    def remove_orphan_files(
        self,
        name: str,
        *,
        older_than: datetime | timedelta | None = None,
        location: str | Path | None = None,
        dry_run: bool = False,
    ) -> RemoveOrphanFilesResult:
        if not isinstance(dry_run, bool):
            raise OrphanFileError("dry_run must be a boolean")

        table = self.load_table(name)
        table_root = _location_to_path(table.location())
        scan_root = _orphan_scan_root(table_root, location)
        cutoff = _cutoff_datetime(
            older_than if older_than is not None else DEFAULT_ORPHAN_RETENTION,
            OrphanFileError,
        )
        orphans = self._orphan_files(table, scan_root=scan_root, cutoff=cutoff)
        by_suffix = _by_suffix(orphans)

        if dry_run:
            return RemoveOrphanFilesResult(
                files=orphans,
                dry_run=True,
                deleted_files_count=0,
                deleted_bytes=0,
                by_suffix=by_suffix,
            )

        deleted_bytes = 0
        for orphan in orphans:
            path = Path(orphan.path)
            try:
                path.relative_to(table_root)
            except ValueError as exc:
                raise OrphanFileError(
                    f"refusing to delete orphan outside table directory: {path}"
                ) from exc

            try:
                path.unlink()
            except OSError as exc:
                raise OrphanFileError(f"failed to delete orphan file: {path}") from exc
            deleted_bytes += orphan.size_bytes

        return RemoveOrphanFilesResult(
            files=orphans,
            dry_run=False,
            deleted_files_count=len(orphans),
            deleted_bytes=deleted_bytes,
            by_suffix=by_suffix,
        )

    def _load_catalog(self) -> SqlCatalog:
        return load_catalog(
            "parqlite",
            **{
                "type": "sql",
                "uri": f"sqlite:///{self.catalog_path.as_posix()}",
                "warehouse": self.warehouse.as_uri(),
            },
        )

    def _identifier(self, name: str) -> tuple[str, str]:
        return parse_table_name(name)

    def _table_snapshots(self, table: Table) -> list[TableSnapshot]:
        return [
            _snapshot_from_row(row) for row in table.inspect.snapshots().to_pylist()
        ]

    def _snapshot_for_selector(
        self,
        table: Table,
        table_name: str,
        selector: SnapshotSelector,
    ):
        if isinstance(selector, _SnapshotIdSelector):
            snapshot = table.snapshot_by_id(selector.id)
            if snapshot is None:
                raise SnapshotError(
                    f"snapshot not found for table {table_name!r}: {selector.id}"
                )
            return snapshot

        if isinstance(selector, _AsOfSelector):
            snapshot = table.snapshot_as_of_timestamp(
                _datetime_to_millis(selector.timestamp),
                inclusive=True,
            )
            if snapshot is None:
                raise SnapshotError(
                    f"no snapshot exists at or before {selector.timestamp!r} "
                    f"for table {table_name!r}"
                )
            return snapshot

        if isinstance(selector, _RefSelector):
            snapshot = table.snapshot_by_name(selector.name)
            if snapshot is None:
                raise SnapshotError(
                    f"snapshot ref not found for table {table_name!r}: {selector.name}"
                )
            return snapshot

        raise SnapshotError("invalid snapshot selector")

    def _protected_snapshot_ids(self, table: Table) -> set[int]:
        return {
            row["snapshot_id"]
            for row in table.inspect.refs().to_pylist()
            if row["snapshot_id"] is not None
        }

    def _current_ancestor_snapshot_ids(self, table: Table, limit: int) -> set[int]:
        if limit == 0:
            return set()

        current = table.current_snapshot()
        if current is None:
            return set()

        snapshots = {
            snapshot.snapshot_id: snapshot for snapshot in self._table_snapshots(table)
        }
        retained: set[int] = set()
        snapshot_id: int | None = current.snapshot_id
        while snapshot_id is not None and len(retained) < limit:
            snapshot = snapshots.get(snapshot_id)
            if snapshot is None:
                break
            retained.add(snapshot.snapshot_id)
            snapshot_id = snapshot.parent_id
        return retained

    def _snapshot_expiration_candidate_ids(
        self,
        table: Table,
        *,
        older_than: datetime | timedelta | None,
        snapshot_ids: Sequence[int] | None,
        retain_last: int | None,
    ) -> list[int]:
        snapshots = self._table_snapshots(table)
        snapshots_by_id = {snapshot.snapshot_id: snapshot for snapshot in snapshots}
        candidate_ids: set[int] = set()
        retained = self._protected_snapshot_ids(table)

        if older_than is None and snapshot_ids is None:
            max_age_ms = _table_property_as_non_negative_int(
                table.properties,
                TableProperties.MAX_SNAPSHOT_AGE_MS,
                TableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT,
            )
            min_snapshots_to_keep = _table_property_as_non_negative_int(
                table.properties,
                TableProperties.MIN_SNAPSHOTS_TO_KEEP,
                TableProperties.MIN_SNAPSHOTS_TO_KEEP_DEFAULT,
            )
            cutoff = datetime.now(timezone.utc) - timedelta(milliseconds=max_age_ms)
            retained.update(
                self._current_ancestor_snapshot_ids(table, min_snapshots_to_keep)
            )
            candidate_ids.update(
                snapshot.snapshot_id
                for snapshot in snapshots
                if snapshot.committed_at < cutoff
            )
        else:
            if older_than is not None:
                cutoff = _cutoff_datetime(older_than, SnapshotError)
                candidate_ids.update(
                    snapshot.snapshot_id
                    for snapshot in snapshots
                    if snapshot.committed_at < cutoff
                )

            if snapshot_ids is not None:
                ids = _validate_snapshot_ids(snapshot_ids)
                missing_ids = [
                    snapshot_id
                    for snapshot_id in ids
                    if snapshot_id not in snapshots_by_id
                ]
                if missing_ids:
                    raise SnapshotError(
                        "snapshot not found for expiration: "
                        + ", ".join(str(snapshot_id) for snapshot_id in missing_ids)
                    )
                candidate_ids.update(ids)

        if retain_last is not None:
            retained.update(self._current_ancestor_snapshot_ids(table, retain_last))

        candidate_ids.difference_update(retained)
        return [
            snapshot.snapshot_id
            for snapshot in snapshots
            if snapshot.snapshot_id in candidate_ids
        ]

    def _orphan_files(
        self,
        table: Table,
        *,
        scan_root: Path,
        cutoff: datetime,
    ) -> list[OrphanFile]:
        reachable = self._reachable_files(table)

        orphans: list[OrphanFile] = []
        for path in scan_root.rglob("*"):
            if not path.is_file():
                continue

            resolved = path.resolve()
            if resolved in reachable:
                continue

            stat = resolved.stat()
            modified_at = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
            if modified_at >= cutoff:
                continue

            orphans.append(
                OrphanFile(
                    path=str(resolved),
                    size_bytes=stat.st_size,
                    modified_at=modified_at,
                )
            )

        return sorted(orphans, key=lambda orphan: orphan.path)

    def _reachable_files(self, table: Table) -> set[Path]:
        paths: set[Path] = set()
        _add_location(paths, table.metadata_location)

        for row in table.inspect.metadata_log_entries().to_pylist():
            _add_location(paths, row["file"])

        for row in table.inspect.snapshots().to_pylist():
            _add_location(paths, row["manifest_list"])

        for row in table.inspect.all_manifests().to_pylist():
            _add_location(paths, row["path"])

        for row in table.inspect.all_files().to_pylist():
            _add_location(paths, row["file_path"])

        return paths


def _snapshot_from_row(row: dict) -> TableSnapshot:
    return TableSnapshot(
        snapshot_id=row["snapshot_id"],
        parent_id=row["parent_id"],
        committed_at=_aware_utc(row["committed_at"]),
        operation=row["operation"],
        manifest_list=row["manifest_list"],
        summary=dict(row["summary"] or {}),
    )


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _datetime_to_millis(value: datetime) -> int:
    return int(value.astimezone(timezone.utc).timestamp() * 1000)


def _timestamp_literal(value: datetime) -> str:
    timestamp = value.astimezone(timezone.utc).replace(tzinfo=None)
    escaped = timestamp.isoformat(sep=" ", timespec="microseconds").replace("'", "''")
    return f"TIMESTAMP '{escaped}'"


def _cutoff_datetime(
    value: datetime | timedelta,
    error_type: type[SnapshotError] | type[OrphanFileError],
) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise error_type("datetime inputs must be timezone-aware")
        return value.astimezone(timezone.utc)

    if isinstance(value, timedelta):
        if value.total_seconds() <= 0:
            raise error_type("timedelta inputs must be positive")
        return datetime.now(timezone.utc) - value

    raise error_type("older_than must be a timezone-aware datetime or timedelta")


def _table_property_as_non_negative_int(
    properties: dict[str, str],
    property_name: str,
    default: int,
) -> int:
    try:
        value = property_as_int(properties, property_name, default)
    except ValueError as exc:
        raise SnapshotError(str(exc)) from exc

    if value is None or value < 0:
        raise SnapshotError(f"table property {property_name} must be non-negative")
    return value


def _validate_retain_last(retain_last: int | None) -> int | None:
    if retain_last is None:
        return None

    if not isinstance(retain_last, int) or isinstance(retain_last, bool):
        raise SnapshotError("retain_last must be a non-negative integer")
    if retain_last < 0:
        raise SnapshotError("retain_last must be a non-negative integer")

    return retain_last


def _validate_snapshot_ids(snapshot_ids: Sequence[int]) -> list[int]:
    if isinstance(snapshot_ids, str | bytes):
        raise SnapshotError("snapshot_ids must be a sequence of integer snapshot ids")

    ids: list[int] = []
    for snapshot_id in snapshot_ids:
        if not isinstance(snapshot_id, int) or isinstance(snapshot_id, bool):
            raise SnapshotError(
                "snapshot_ids must be a sequence of integer snapshot ids"
            )
        ids.append(snapshot_id)
    return ids


def _add_location(paths: set[Path], location: str | None) -> None:
    if location is None:
        return
    paths.add(_location_to_path(location))


def _orphan_scan_root(table_root: Path, location: str | Path | None) -> Path:
    if location is None:
        return table_root

    if isinstance(location, Path):
        path = location.expanduser()
        if not path.is_absolute():
            path = table_root / path
        path = path.resolve()
    elif isinstance(location, str):
        parsed = urlparse(location)
        if parsed.scheme or Path(location).expanduser().is_absolute():
            path = _location_to_path(location)
        else:
            path = (table_root / location).resolve()
    else:
        raise OrphanFileError("location must be a path, file URI, or None")

    try:
        path.relative_to(table_root)
    except ValueError as exc:
        raise OrphanFileError(
            f"orphan file location must be under table location: {path}"
        ) from exc

    if not path.exists():
        raise OrphanFileError(f"orphan file location does not exist: {path}")
    if not path.is_dir():
        raise OrphanFileError(f"orphan file location must be a directory: {path}")

    return path


def _by_suffix(files: Sequence[OrphanFile]) -> dict[str, int]:
    by_suffix: dict[str, int] = {}
    for orphan in files:
        suffix = Path(orphan.path).suffix.lower()
        by_suffix[suffix] = by_suffix.get(suffix, 0) + 1
    return dict(sorted(by_suffix.items()))


def _location_to_path(location: str) -> Path:
    parsed = urlparse(location)
    if parsed.scheme == "file":
        if parsed.netloc not in ("", "localhost"):
            raise OrphanFileError(f"unsupported file location: {location}")
        return Path(unquote(parsed.path)).resolve()

    if parsed.scheme:
        raise OrphanFileError("only local file-backed table locations are supported")

    return Path(location).expanduser().resolve()
