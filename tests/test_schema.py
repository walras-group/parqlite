import pytest

from parquetdb import types as t
from parquetdb.errors import SchemaError
from parquetdb.schema import normalize_schema, to_iceberg_schema


def test_supports_v1_scalar_types() -> None:
    columns = normalize_schema(
        {
            "boolean_col": "boolean",
            "int_col": "int",
            "long_col": "long",
            "float_col": "float",
            "double_col": "double",
            "date_col": "date",
            "time_col": "time",
            "timestamp_col": "timestamp",
            "timestamptz_col": "timestamptz",
            "string_col": "string",
            "uuid_col": "uuid",
            "binary_col": "binary",
            "decimal_col": "decimal(10, 2)",
            "fixed_col": "fixed(16)",
        }
    )

    iceberg_schema = to_iceberg_schema(columns)

    assert iceberg_schema.column_names == [
        "boolean_col",
        "int_col",
        "long_col",
        "float_col",
        "double_col",
        "date_col",
        "time_col",
        "timestamp_col",
        "timestamptz_col",
        "string_col",
        "uuid_col",
        "binary_col",
        "decimal_col",
        "fixed_col",
    ]


def test_accepts_schema_type_helpers() -> None:
    columns = normalize_schema(
        {"name": t.string, "price": t.decimal(10, 2), "blob": t.fixed(16)}
    )

    assert [(column.name, column.type_text) for column in columns] == [
        ("name", "string"),
        ("price", "decimal(10, 2)"),
        ("blob", "fixed(16)"),
    ]


@pytest.mark.parametrize(
    "type_text",
    [
        "json",
        "decimal(0, 0)",
        "decimal(4, 5)",
        "decimal(39, 2)",
        "fixed(0)",
    ],
)
def test_rejects_unknown_and_invalid_types(type_text: str) -> None:
    with pytest.raises(SchemaError):
        normalize_schema({"bad": type_text})


def test_rejects_str_with_string_suggestion() -> None:
    with pytest.raises(SchemaError, match='did you mean "string"'):
        normalize_schema({"name": "str"})
