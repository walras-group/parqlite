from pathlib import Path
from typing import get_args

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parqlite import IcebergTablePropertyKey, connect, types as t
from parqlite.errors import (
    NamespaceAlreadyExistsError,
    NamespaceNotFoundError,
    SchemaError,
    SchemaMismatchError,
)
from parqlite.iceberg import KEYS_PROPERTY, VERSION_BY_PROPERTY
from parqlite.partitioning import month


def test_exports_iceberg_table_property_key_literal() -> None:
    assert "history.expire.max-snapshot-age-ms" in get_args(IcebergTablePropertyKey)
    assert "write.metadata.previous-versions-max" in get_args(IcebergTablePropertyKey)


def test_create_table_saves_reserved_metadata(tmp_path: Path) -> None:
    db = connect(tmp_path)

    db.create_table(
        "factor_values",
        schema={
            "factor": "string",
            "ts": "timestamp",
            "instrument_id": "string",
            "value": "double",
            "updated_at": "timestamp",
        },
        partition_by=[month("ts")],
        keys=["factor", "ts", "instrument_id"],
        version_by="updated_at",
        properties={
            "write.metadata.previous-versions-max": 3,
            "custom.enabled": True,
        },
    )

    table = db._store.load_table("factor_values")

    assert table.properties[KEYS_PROPERTY] == "factor,ts,instrument_id"
    assert table.properties[VERSION_BY_PROPERTY] == "updated_at"
    assert table.properties["write.metadata.previous-versions-max"] == "3"
    assert table.properties["custom.enabled"] == "true"
    assert str(table.spec().fields[0].transform) == "month"


def test_table_properties_can_be_updated_and_removed(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table(
        "items",
        {"id": "long"},
        properties={"write.metadata.previous-versions-max": 10},
    )

    updated = db.set_table_properties(
        "items",
        {
            "write.metadata.delete-after-commit.enabled": True,
            "custom.batch-size": 500,
        },
    )

    assert updated["write.metadata.previous-versions-max"] == "10"
    assert updated["write.metadata.delete-after-commit.enabled"] == "true"
    assert updated["custom.batch-size"] == "500"

    removed = db.remove_table_properties("items", ["custom.batch-size"])

    assert "custom.batch-size" not in removed
    assert removed["write.metadata.delete-after-commit.enabled"] == "true"


def test_reserved_table_properties_cannot_be_overridden(tmp_path: Path) -> None:
    db = connect(tmp_path)

    with pytest.raises(SchemaError, match="reserved table properties"):
        db.create_table("bad", {"id": "long"}, properties={KEYS_PROPERTY: "id"})

    db.create_table("items", {"id": "long"}, keys=["id"])

    with pytest.raises(SchemaError, match="reserved table properties"):
        db.set_table_properties("items", {VERSION_BY_PROPERTY: "id"})

    with pytest.raises(SchemaError, match="reserved table properties"):
        db.remove_table_properties("items", [KEYS_PROPERTY])


def test_create_table_accepts_schema_type_helpers(tmp_path: Path) -> None:
    db = connect(tmp_path)

    db.create_table(
        "items",
        schema={
            "id": t.long,
            "name": t.string,
            "price": t.decimal(10, 2),
            "blob": t.fixed(16),
        },
    )

    assert db.schema("items") == {
        "id": "long",
        "name": "string",
        "price": "decimal(10, 2)",
        "blob": "fixed(16)",
    }


def test_namespace_table_can_be_created_appended_and_queried(tmp_path: Path) -> None:
    db = connect(tmp_path)

    db.create_namespace("binance")
    db.create_table("binance.klines", {"opentime": "long", "symbol": "string"})
    db.append(
        "binance.klines",
        pd.DataFrame({"opentime": [1769904000000], "symbol": ["BTCUSDT"]}),
    )

    assert db.tables() == ["binance.klines"]
    assert db.schema("binance.klines") == {
        "opentime": "long",
        "symbol": "string",
    }
    assert db.sql("select symbol from binance.klines").fetchall() == [("BTCUSDT",)]

    reconnected = connect(tmp_path)

    assert reconnected.tables() == ["binance.klines"]
    assert reconnected.sql("select opentime from binance.klines").fetchall() == [
        (1769904000000,)
    ]


def test_create_table_requires_existing_namespace(tmp_path: Path) -> None:
    db = connect(tmp_path)

    with pytest.raises(NamespaceNotFoundError, match="namespace not found: binance"):
        db.create_table("binance.klines", {"opentime": "long"})


def test_create_namespace_rejects_existing_namespace(tmp_path: Path) -> None:
    db = connect(tmp_path)

    db.create_namespace("binance")

    with pytest.raises(NamespaceAlreadyExistsError, match="namespace already exists"):
        db.create_namespace("binance")


def test_table_and_namespace_names_must_be_valid_identifiers(tmp_path: Path) -> None:
    db = connect(tmp_path)

    invalid_table_names = [
        "",
        ".items",
        "binance.",
        "binance..klines",
        "a.b.c",
        "1binance.klines",
        "binance.1klines",
    ]
    for table_name in invalid_table_names:
        with pytest.raises(SchemaError):
            db.create_table(table_name, {"id": "long"})

    for namespace_name in ["", "a.b", "1binance"]:
        with pytest.raises(SchemaError):
            db.create_namespace(namespace_name)


def test_reserved_metadata_must_reference_schema_fields(tmp_path: Path) -> None:
    db = connect(tmp_path)

    with pytest.raises(SchemaError):
        db.create_table("bad_keys", {"id": "long"}, keys=["missing"])

    with pytest.raises(SchemaError):
        db.create_table("bad_version", {"id": "long"}, version_by="updated_at")

    with pytest.raises(SchemaError):
        db.create_table("duplicate_keys", {"id": "long"}, keys=["id", "id"])

    assert db.tables() == []


def test_append_does_not_deduplicate_reserved_keys(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table(
        "factor_values",
        schema={
            "factor": "string",
            "ts": "timestamp",
            "value": "double",
            "updated_at": "timestamp",
        },
        keys=["factor", "ts"],
        version_by="updated_at",
    )

    db.append(
        "factor_values",
        pd.DataFrame(
            {
                "factor": ["quality", "quality"],
                "ts": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "value": [1.0, 2.0],
                "updated_at": pd.to_datetime(["2024-01-01 01:00", "2024-01-01 02:00"]),
            }
        ),
    )

    assert db.sql("select count(*) from factor_values").fetchone()[0] == 2


def test_append_and_overwrite_from_pandas_and_arrow(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long", "name": "string"})

    db.append("items", pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}))
    assert db.sql("select count(*) from items").fetchone()[0] == 2

    db.overwrite("items", pa.table({"id": [3], "name": ["c"]}))

    assert db.sql("select id, name from items").fetchall() == [(3, "c")]


def test_append_from_files(tmp_path: Path) -> None:
    db = connect(tmp_path / "db")
    db.create_table("items", {"id": "long", "name": "string"})

    parquet_path = tmp_path / "items.parquet"
    csv_path = tmp_path / "items.csv"
    json_path = tmp_path / "items.jsonl"
    pq.write_table(pa.table({"id": [1], "name": ["parquet"]}), parquet_path)
    pd.DataFrame({"id": [2], "name": ["csv"]}).to_csv(csv_path, index=False)
    pd.DataFrame({"id": [3], "name": ["json"]}).to_json(
        json_path,
        orient="records",
        lines=True,
    )

    db.append("items", parquet_path)
    db.append("items", csv_path)
    db.append("items", json_path)

    assert db.sql("select id, name from items order by id").fetchall() == [
        (1, "parquet"),
        (2, "csv"),
        (3, "json"),
    ]


def test_append_rejects_missing_extra_reordered_and_incompatible_columns(
    tmp_path: Path,
) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"id": "long", "name": "string"})

    with pytest.raises(SchemaMismatchError, match="missing columns"):
        db.append("items", pd.DataFrame({"id": [1]}))

    with pytest.raises(SchemaMismatchError, match="extra columns"):
        db.append("items", pd.DataFrame({"id": [1], "name": ["a"], "extra": [1]}))

    with pytest.raises(SchemaMismatchError, match="column order"):
        db.append("items", pd.DataFrame({"name": ["a"], "id": [1]}))

    with pytest.raises(SchemaMismatchError):
        db.append("items", pd.DataFrame({"id": ["not an int"], "name": ["a"]}))


def test_sql_and_persistence(tmp_path: Path) -> None:
    db = connect(tmp_path)
    db.create_table("items", {"category": "string", "value": "double"})
    db.append(
        "items",
        pd.DataFrame({"category": ["a", "a", "b"], "value": [1.0, 2.0, 3.0]}),
    )

    assert db.sql(
        "select category, sum(value) from items group by category order by category"
    ).fetchall() == [("a", 3.0), ("b", 3.0)]

    reconnected = connect(tmp_path)

    assert reconnected.tables() == ["items"]
    assert reconnected.schema("items") == {"category": "string", "value": "double"}
    assert (
        reconnected.sql("select count(*) from items where value > 1").fetchone()[0] == 2
    )
