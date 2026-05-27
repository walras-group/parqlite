---
name: parqlite
description: Use when an agent needs to create, query, maintain, or explain local-first analytical datasets with ParqLite, including its Python API, CLI, Iceberg-backed snapshots, DuckDB SQL reads, schema definitions, partitioning, time travel, tags, rollback, and cleanup workflows.
---

# ParqLite

Use this skill when a task involves ParqLite data stores or code that should read/write local analytical tables backed by Iceberg metadata, Parquet files, and DuckDB SQL.

## Start Here

- Prefer the Python API for application code, scripts, examples, and tests.
- Use the CLI for ad hoc inspection: `parqlite tables`, `parqlite schema`, `parqlite sql`, and `parqlite ui`.
- In this repo, run commands through `uv`: `UV_CACHE_DIR=/tmp/uv-cache uv run ...`.
- Close database connections with `db.close()` or use `with connect(path) as db:`.

```python
import pandas as pd

from parqlite import connect, month, t

with connect("./data") as db:
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
        if_not_exists=True,
    )

    db.append(
        "factor_values",
        pd.DataFrame(
            {
                "factor": ["quality"],
                "date": pd.to_datetime(["2024-01-31"]).date,
                "instrument_id": ["000001"],
                "value": [1.2],
                "updated_at": pd.to_datetime(["2024-03-01 09:00:00"]),
            }
        ),
    )

    rows = db.sql("select * from factor_values").fetchall()
```

## Core API

Import the public API from `parqlite`:

```python
from parqlite import (
    as_of,
    bucket,
    connect,
    day,
    hour,
    identity,
    month,
    ref,
    snapshot_id,
    t,
    truncate,
    year,
)
```

Common operations:

```python
db = connect("./data")
db.create_namespace("binance", if_not_exists=True)
db.list_namespaces()

db.create_table("items", {"id": t.long, "name": t.string}, if_not_exists=True)
db.tables()
db.table_exists("items")
db.schema("items")
db.table_properties("items")

db.append("items", dataframe_or_arrow_table_or_file_path)
db.overwrite("items", replacement_data)
db.sql("select id, name from items order by id").df()

db.drop_table("items", if_exists=True)
db.close()
```

## Schemas And Data

Use `parqlite.types` through the exported `t` alias:

- Scalars: `t.boolean`, `t.int`, `t.long`, `t.float`, `t.double`, `t.date`, `t.time`, `t.timestamp`, `t.timestamptz`, `t.string`, `t.uuid`, `t.binary`
- Parameterized: `t.decimal(precision, scale)`, `t.fixed(length)`
- Raw Iceberg type strings are accepted, but typed helpers are clearer.

Input data for `append` and `overwrite` can be a `pandas.DataFrame`, `pyarrow.Table`, or a path ending in `.parquet`, `.csv`, `.json`, `.jsonl`, or `.ndjson`.

Column names and column order must exactly match the table schema. ParqLite safely casts input to the table schema and raises schema/input errors if the data cannot be cast.

## Tables And Partitioning

Use partition helpers when creating tables:

```python
db.create_table(
    "events",
    schema={
        "event_id": t.long,
        "event_date": t.date,
        "customer_id": t.string,
        "event_type": t.string,
    },
    partition_by=[
        month("event_date"),
        bucket("customer_id", 16),
        truncate("event_type", 8),
    ],
    keys=["event_id"],
    if_not_exists=True,
)
```

Supported partition transforms: `identity("column")`, direct `"column"`, `year`, `month`, `day`, `hour`, `bucket(column, num_buckets)`, and `truncate(column, width)`.

`keys` and `version_by` are reserved metadata for future deduplication features. They do not enforce uniqueness in v1. Set them through `create_table(..., keys=..., version_by=...)`, not through table properties.

## SQL And CLI

`db.sql(query)` returns a DuckDB relation. Use `.fetchall()`, `.fetchone()`, or `.df()` depending on the task.

CLI shape:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run parqlite tables ./data
UV_CACHE_DIR=/tmp/uv-cache uv run parqlite schema ./data items
UV_CACHE_DIR=/tmp/uv-cache uv run parqlite sql ./data "select * from items"
UV_CACHE_DIR=/tmp/uv-cache uv run parqlite ui ./data
```

ParqLite refreshes DuckDB views against the latest Iceberg metadata on each SQL call. The first SQL query may require DuckDB to install/load its Iceberg extension.

## Snapshots, Time Travel, Tags

Inspect snapshots:

```python
current = db.current_snapshot("items")
history = db.snapshots("items", limit=5)
refs = db.refs("items")
```

Read historical data:

```python
first = db.snapshots("items")[0]
rows = db.sql(
    "select * from items",
    at={"items": snapshot_id(first.snapshot_id)},
).fetchall()
```

Use `as_of(aware_datetime)` for timestamp selectors. Datetimes must be timezone-aware.

Tag and rollback:

```python
db.create_tag("items", "stable", at=snapshot_id(current.snapshot_id))
stable = db.sql("select * from items", at={"items": ref("stable")}).df()
db.delete_tag("items", "stable")
db.rollback_to("items", snapshot_id(previous_snapshot_id))
```

## Maintenance

Set Iceberg table properties with `set_table_properties`; remove them with `remove_table_properties`.

For cleanup, use Iceberg-native maintenance in this order:

```python
from datetime import timedelta

db.expire_snapshots("items", older_than=timedelta(days=30), retain_last=2)
preview = db.remove_orphan_files("items", dry_run=True)
deleted = db.remove_orphan_files("items")
```

Always preview orphan cleanup with `dry_run=True` before deleting. By default, orphan cleanup only considers files older than 3 days.

## Constraints To Preserve

- ParqLite v1 supports explicit append and full-table overwrite writes.
- Do not implement or imply row-level `write`, `upsert`, `merge`, `delete`, schema evolution, `vacuum`, or `optimize` APIs unless the codebase has changed.
- `drop_namespace` only removes empty non-default namespaces.
- `default` namespace always exists.
- Keep local store paths explicit. A ParqLite root contains `catalog.db` and `warehouse/`.
