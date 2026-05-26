from parqlite.partitioning import (
    bucket,
    build_partition_spec,
    day,
    hour,
    month,
    truncate,
    year,
)
from parqlite.schema import normalize_schema, to_iceberg_schema


def test_partition_transforms_build_iceberg_spec() -> None:
    schema = to_iceberg_schema(
        normalize_schema(
            {
                "symbol": "string",
                "ts": "timestamp",
                "instrument_id": "string",
            }
        )
    )

    spec = build_partition_spec(
        schema,
        [
            "symbol",
            year("ts"),
            month("ts"),
            day("ts"),
            hour("ts"),
            bucket("instrument_id", 16),
            truncate("instrument_id", 3),
        ],
    )

    assert [field.name for field in spec.fields] == [
        "symbol",
        "ts_year",
        "ts_month",
        "ts_day",
        "ts_hour",
        "instrument_id_bucket_16",
        "instrument_id_trunc_3",
    ]
    assert [str(field.transform) for field in spec.fields] == [
        "identity",
        "year",
        "month",
        "day",
        "hour",
        "bucket[16]",
        "truncate[3]",
    ]
