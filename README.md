# parqlite

Local-first Python storage library backed by Iceberg metadata, Parquet data
files, and DuckDB SQL queries.

parqlite is useful when you want a small local analytical database with:

- Iceberg table metadata and snapshots
- Parquet data files
- DuckDB SQL reads
- explicit append and full-table overwrite writes
- snapshot time travel, tags, rollback, and maintenance helpers

v1 intentionally stays small. It does not implement `write`, `upsert`, `merge`,
`delete`, or schema evolution. `keys` and `version_by` are stored as reserved
table metadata for future deduplication features, but they do not enforce
uniqueness in v1.

## Install

This repo uses `uv`:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv sync
```

Run tests:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run pytest
```

## Quick Start

```python
import pandas as pd

from parqlite import connect, month, t

db = connect("./data")

db.create_table(
    "factor_values",
    schema={
        "factor": t.string,
        "date": t.date,
        "instrument_id": t.string,
        "value": t.double,
        "updated_at": t.timestamp,
    },
    partition_by=[month("date")],
    keys=["factor", "date", "instrument_id"],
    version_by="updated_at",
)

db.append(
    "factor_values",
    pd.DataFrame(
        {
            "factor": ["quality", "quality"],
            "date": pd.to_datetime(["2024-01-31", "2024-02-29"]).date,
            "instrument_id": ["000001", "000002"],
            "value": [1.2, 0.8],
            "updated_at": pd.to_datetime(
                ["2024-03-01 09:00:00", "2024-03-01 09:01:00"]
            ),
        }
    ),
)

rows = db.sql(
    """
    select factor, instrument_id, value
    from factor_values
    order by instrument_id
    """
).fetchall()
```

## Connect

`connect(path)` opens or creates a local parqlite directory.

```python
from parqlite import connect

db = connect("./data")
```

The directory contains a local SQL catalog database and an Iceberg warehouse:

```text
data/
  catalog.db
  warehouse/
```

Close the DuckDB connection when you are done:

```python
db.close()
```

Or use `DB` as a context manager:

```python
with connect("./data") as db:
    tables = db.tables()
```

## Schemas

Use `parqlite.types` helpers for schemas:

```python
from parqlite import t

schema = {
    "id": t.long,
    "active": t.boolean,
    "price": t.decimal(18, 4),
    "payload": t.binary,
    "fixed_hash": t.fixed(16),
    "created_at": t.timestamp,
}
```

Supported helpers:

- `t.boolean`
- `t.int`
- `t.long`
- `t.float`
- `t.double`
- `t.date`
- `t.time`
- `t.timestamp`
- `t.timestamptz`
- `t.string`
- `t.uuid`
- `t.binary`
- `t.decimal(precision, scale)`
- `t.fixed(length)`

Raw Iceberg type strings are also accepted:

```python
db.create_table("items", {"id": "long", "name": "string"})
```

Input data columns must match the table schema exactly, including column order.

## Create Tables

```python
from parqlite import bucket, day, hour, identity, month, truncate, year

db.create_table(
    "events",
    schema={
        "event_id": "long",
        "event_date": "date",
        "customer_id": "string",
        "event_type": "string",
    },
    partition_by=[
        month("event_date"),
        bucket("customer_id", 16),
        truncate("event_type", 8),
    ],
    keys=["event_id"],
    properties={
        "write.metadata.delete-after-commit.enabled": True,
        "write.metadata.previous-versions-max": 5,
    },
)
```

Pass `if_not_exists=True` to make creation a no-op when the table already
exists. Existing table schema, partitioning, and properties are not modified.

Partition helpers:

- `identity("column")`, or pass `"column"` directly in `partition_by`
- `year("date_column")`
- `month("date_column")`
- `day("date_column")`
- `hour("timestamp_column")`
- `bucket("column", num_buckets)`
- `truncate("column", width)`

`properties` accepts Iceberg table properties. Values may be strings, integers,
or booleans. Booleans and integers are stored as strings in Iceberg metadata.
The examples above are not the full Iceberg property list; they are common
maintenance-related properties. See the Apache Iceberg configuration docs for
the complete current table property reference:
https://iceberg.apache.org/docs/latest/configuration/#table-properties

parqlite exports `IcebergTablePropertyKey` as a `Literal[...]` type alias for
the official static Iceberg table property keys:

```python
from parqlite import IcebergTablePropertyKey, TablePropertyValue

props: dict[IcebergTablePropertyKey, TablePropertyValue] = {
    "history.expire.max-snapshot-age-ms": 7 * 24 * 60 * 60 * 1000,
    "history.expire.min-snapshots-to-keep": 2,
}
```

The `create_table(..., properties=...)` and `set_table_properties(...)` APIs
still accept general string keys. That is intentional because Iceberg also has
column-scoped keys such as `write.metadata.metrics.column.<column_name>` and
parqlite allows custom informational keys such as `custom.owner`.

`parqlite.keys` and `parqlite.version_by` are reserved. Set them through
`keys=` and `version_by=`, not through `properties=`.

## Write Data

`append(table, data)` appends records and creates a new Iceberg snapshot.

```python
import pandas as pd
import pyarrow as pa

db.append("items", pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}))
db.append("items", pa.table({"id": [3], "name": ["c"]}))
db.append("items", "./more_items.parquet")
db.append("items", "./more_items.csv")
db.append("items", "./more_items.jsonl")
```

Supported file inputs:

- `.parquet`
- `.csv`
- `.json`
- `.jsonl`
- `.ndjson`

`overwrite(table, data)` replaces the visible table contents with `data` and
creates a new snapshot:

```python
db.overwrite("items", pd.DataFrame({"id": [10], "name": ["replacement"]}))
```

## Query With SQL

`sql(query)` returns a DuckDB relation:

```python
relation = db.sql("select id, name from items order by id")

rows = relation.fetchall()
df = relation.df()
one = db.sql("select count(*) from items").fetchone()[0]
```

parqlite refreshes DuckDB views against the latest Iceberg `metadata.json` on
each SQL call.

## Table Introspection

```python
tables = db.tables()
exists = db.table_exists("items")
schema = db.schema("items")
properties = db.table_properties("items")
```

Drop a table:

```python
db.drop_table("items")
db.drop_table("items", if_exists=True)
```

## Namespaces

The `default` namespace always exists. Additional namespaces let you use table
names such as `binance.klines`.

```python
db.create_namespace("binance")
db.create_namespace("binance", if_not_exists=True)

namespaces = db.list_namespaces()

db.drop_namespace("binance")
db.drop_namespace("binance", if_exists=True)
```

`drop_namespace` only removes empty non-default namespaces.

## CLI

The CLI keeps the command shape `parqlite <command> <path> ...`:

```bash
parqlite ui ./data
parqlite tables ./data
parqlite schema ./data items
parqlite sql ./data
parqlite sql ./data "select * from items order by id"
```

`tables` runs a DuckDB catalog query after preloading the current parqlite
tables as views, then displays table names with DuckDB's native output.

`schema` runs DuckDB's native `DESCRIBE` command after preloading the current
parqlite tables as views.

`sql` starts DuckDB CLI with the current parqlite tables preloaded as views. If
you pass a query, DuckDB runs it and exits. Without a query, DuckDB opens an
interactive shell.

## Table Properties

Read, set, and remove Iceberg table properties:

```python
props = db.table_properties("items")

updated = db.set_table_properties(
    "items",
    {
        "history.expire.max-snapshot-age-ms": 7 * 24 * 60 * 60 * 1000,
        "history.expire.min-snapshots-to-keep": 2,
        "write.metadata.delete-after-commit.enabled": True,
        "write.metadata.previous-versions-max": 3,
        "custom.owner": "research",
    },
)

updated = db.remove_table_properties("items", ["custom.owner"])
```

Use metadata retention properties to let PyIceberg delete old tracked metadata
files after commits:

```python
db.set_table_properties(
    "items",
    {
        "write.metadata.delete-after-commit.enabled": True,
        "write.metadata.previous-versions-max": 1,
    },
)
```

This only applies to metadata files tracked by the table metadata log. Untracked
metadata files can still be removed by orphan cleanup.

## Snapshots

Inspect snapshots:

```python
current = db.current_snapshot("items")
snapshots = db.snapshots("items")
recent = db.snapshots("items", limit=5)

print(current.snapshot_id)
print(current.parent_id)
print(current.committed_at)
print(current.operation)
```

`current_snapshot` and `snapshots` return `TableSnapshot` objects with:

- `snapshot_id`
- `parent_id`
- `committed_at`
- `operation`
- `manifest_list`
- `summary`

## Time Travel

Use snapshot selectors from the top-level package:

```python
from parqlite import as_of, ref, snapshot_id

first = db.snapshots("items")[0]

rows_by_id = db.sql(
    "select * from items",
    at={"items": snapshot_id(first.snapshot_id)},
).fetchall()

rows_by_time = db.sql(
    "select * from items",
    at={"items": as_of(first.committed_at)},
).fetchall()
```

When a SQL query references more than one table, pass one selector per table
that should be pinned:

```python
db.sql(
    """
    select *
    from left_items l
    join right_items r on l.id = r.id
    """,
    at={
        "left_items": snapshot_id(left_snapshot_id),
        "right_items": snapshot_id(right_snapshot_id),
    },
)
```

Datetime selectors must be timezone-aware.

## Tags And Rollback

Create a tag for a snapshot:

```python
from parqlite import ref, snapshot_id

current = db.current_snapshot("items")
db.create_tag("items", "stable", at=snapshot_id(current.snapshot_id))
```

Read a tag:

```python
stable_rows = db.sql(
    "select * from items",
    at={"items": ref("stable")},
).fetchall()
```

List refs:

```python
for snapshot_ref in db.refs("items"):
    print(snapshot_ref.name, snapshot_ref.type, snapshot_ref.snapshot_id)
```

Delete a tag:

```python
db.delete_tag("items", "stable")
```

Rollback the table to a previous snapshot or time:

```python
db.rollback_to("items", snapshot_id(previous_snapshot_id))
db.rollback_to("items", as_of(previous_timestamp))
```

Rollback changes the current table snapshot and creates metadata changes through
PyIceberg.

## Snapshot Expiration

`expire_snapshots` removes old snapshots from table metadata using PyIceberg's
native maintenance transaction.

Expire snapshots older than a cutoff:

```python
from datetime import datetime, timezone

result = db.expire_snapshots(
    "items",
    older_than=datetime(2024, 1, 1, tzinfo=timezone.utc),
)

print(result.expired_snapshot_ids)
print(result.expired_snapshots_count)
```

Expire snapshots by ID:

```python
result = db.expire_snapshots("items", snapshot_ids=[old_snapshot_id])
```

Protect the most recent current-branch ancestor snapshots while expiring older
ones:

```python
from datetime import timedelta

result = db.expire_snapshots(
    "items",
    older_than=timedelta(days=30),
    retain_last=2,
)
```

If both `older_than` and `snapshot_ids` are omitted, parqlite uses Iceberg table
properties:

```python
db.set_table_properties(
    "items",
    {
        "history.expire.max-snapshot-age-ms": 5 * 24 * 60 * 60 * 1000,
        "history.expire.min-snapshots-to-keep": 1,
    },
)

result = db.expire_snapshots("items")
```

Important details:

- `retain_last` only protects recent current-branch ancestor snapshots.
- Tags and branch heads are protected by Iceberg and are not expired.
- Snapshot expiration updates metadata only. It does not delete orphan files by
  itself.

## Orphan File Cleanup

Use `remove_orphan_files` after snapshot expiration to preview and then delete
files that are no longer reachable from current table metadata.

Always preview first:

```python
preview = db.remove_orphan_files("items", dry_run=True)

for file in preview.files:
    print(file.path, file.size_bytes, file.modified_at)
```

Delete the same kind of candidates:

```python
result = db.remove_orphan_files("items")

print(result.deleted_files_count)
print(result.deleted_bytes)
print(result.by_suffix)
```

`remove_orphan_files` returns `RemoveOrphanFilesResult`:

- `files`: candidate or deleted `OrphanFile` entries
- `dry_run`: whether files were only previewed
- `deleted_files_count`
- `deleted_bytes`
- `by_suffix`: count by file suffix, such as `{".parquet": 4}`

By default, orphan cleanup only considers files older than 3 days. This safety
window avoids deleting files from recent or in-progress writes.

Use a custom cutoff:

```python
from datetime import timedelta

preview = db.remove_orphan_files(
    "items",
    older_than=timedelta(days=7),
    dry_run=True,
)
```

Limit cleanup to a table subdirectory:

```python
preview = db.remove_orphan_files(
    "items",
    location="metadata",
    dry_run=True,
)
```

`location` must be under the table root. parqlite currently supports local
file-backed tables only for orphan cleanup.

There is no `vacuum` or `optimize` API. Use the Iceberg-native sequence instead:

```python
db.expire_snapshots("items", older_than=timedelta(days=30), retain_last=2)
preview = db.remove_orphan_files("items", dry_run=True)
deleted = db.remove_orphan_files("items")
```

## Result Types

`ExpireSnapshotsResult`:

```python
result.expired_snapshot_ids
result.expired_snapshots_count
```

`RemoveOrphanFilesResult`:

```python
result.files
result.dry_run
result.deleted_files_count
result.deleted_bytes
result.by_suffix
```

`OrphanFile`:

```python
file.path
file.size_bytes
file.modified_at
```

## Examples

Create a local `./crypto` database directory, create the `binance` namespace,
and import the included Binance BTCUSDT one-minute klines CSV files into
`binance.klines`:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python examples/import_binance_klines.py
```

Create a local `./cn_market` database directory, recreate the `factor` table,
and insert deterministic sample factor values:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python examples/write_cn_market.py
```

Use `PARQLITE_EXAMPLE_ROWS` to run a smaller sample:

```bash
PARQLITE_EXAMPLE_ROWS=10000 UV_CACHE_DIR=/tmp/uv-cache uv run python examples/write_cn_market.py
```

Read the `factor` table and run sample analytical queries:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python examples/read_cn_market.py
```

Inspect snapshots and read a tagged snapshot:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python examples/read_version.py
```

## Storage And SQL Notes

parqlite uses PyIceberg for local catalog management, table creation, appends,
overwrites, drops, snapshots, table properties, and metadata commits. Table data
is stored as Parquet files under the local warehouse.

SQL reads use DuckDB's Iceberg extension. The first SQL query for a database may
need DuckDB to install the `iceberg` extension into DuckDB's default user
extension directory. If that directory is not writable, or DuckDB cannot
download or load the extension, parqlite raises a query backend error that
includes DuckDB's original error.
