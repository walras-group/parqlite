from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import re

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimeType,
    TimestampType,
    TimestamptzType,
    UUIDType,
)

from parqlite.errors import SchemaError
from parqlite.types import SchemaType


@dataclass(frozen=True)
class Column:
    name: str
    type_text: str
    iceberg_type: IcebergType
    nullable: bool = True


_DECIMAL_RE = re.compile(r"^decimal\((\d+)\s*,\s*(\d+)\)$")
_FIXED_RE = re.compile(r"^fixed\((\d+)\)$")

_SCALAR_TYPES: dict[str, type[IcebergType]] = {
    "boolean": BooleanType,
    "int": IntegerType,
    "long": LongType,
    "float": FloatType,
    "double": DoubleType,
    "date": DateType,
    "time": TimeType,
    "timestamp": TimestampType,
    "timestamptz": TimestamptzType,
    "string": StringType,
    "uuid": UUIDType,
    "binary": BinaryType,
}


def normalize_schema(schema: Mapping[str, str | SchemaType]) -> tuple[Column, ...]:
    if not isinstance(schema, Mapping) or not schema:
        raise SchemaError(
            "schema must be a non-empty mapping of column names to schema types"
        )

    columns: list[Column] = []
    seen: set[str] = set()
    for name, type_text in schema.items():
        if not isinstance(name, str) or not name:
            raise SchemaError("column names must be non-empty strings")
        if name in seen:
            raise SchemaError(f"duplicate column name: {name}")
        type_text = _coerce_type_text(name, type_text)

        normalized_type_text = _normalize_type_text(type_text)
        columns.append(
            Column(name, normalized_type_text, parse_type(normalized_type_text))
        )
        seen.add(name)

    return tuple(columns)


def parse_type(type_text: str) -> IcebergType:
    normalized = _normalize_type_text(type_text)
    scalar_type = _SCALAR_TYPES.get(normalized)
    if scalar_type is not None:
        return scalar_type()
    if normalized == "str":
        raise SchemaError('unsupported type: str (did you mean "string"?)')

    decimal_match = _DECIMAL_RE.match(normalized)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        if precision < 1 or precision > 38:
            raise SchemaError("decimal precision must be between 1 and 38")
        if scale < 0 or scale > precision:
            raise SchemaError("decimal scale must be between 0 and precision")
        return DecimalType(precision, scale)

    fixed_match = _FIXED_RE.match(normalized)
    if fixed_match:
        length = int(fixed_match.group(1))
        if length < 1:
            raise SchemaError("fixed length must be positive")
        return FixedType(length)

    raise SchemaError(f"unsupported type: {type_text}")


def to_iceberg_schema(columns: tuple[Column, ...]) -> Schema:
    return Schema(
        *(
            NestedField(
                field_id=index,
                name=column.name,
                field_type=column.iceberg_type,
                required=not column.nullable,
            )
            for index, column in enumerate(columns, start=1)
        )
    )


def to_arrow_schema(columns: tuple[Column, ...]) -> pa.Schema:
    return to_iceberg_schema(columns).as_arrow()


def schema_to_dict(schema: Schema) -> dict[str, str]:
    return {
        field.name: iceberg_type_to_text(field.field_type) for field in schema.fields
    }


def iceberg_type_to_text(iceberg_type: IcebergType) -> str:
    if isinstance(iceberg_type, BooleanType):
        return "boolean"
    if isinstance(iceberg_type, IntegerType):
        return "int"
    if isinstance(iceberg_type, LongType):
        return "long"
    if isinstance(iceberg_type, FloatType):
        return "float"
    if isinstance(iceberg_type, DoubleType):
        return "double"
    if isinstance(iceberg_type, DateType):
        return "date"
    if isinstance(iceberg_type, TimeType):
        return "time"
    if isinstance(iceberg_type, TimestampType):
        return "timestamp"
    if isinstance(iceberg_type, TimestamptzType):
        return "timestamptz"
    if isinstance(iceberg_type, StringType):
        return "string"
    if isinstance(iceberg_type, UUIDType):
        return "uuid"
    if isinstance(iceberg_type, BinaryType):
        return "binary"
    if isinstance(iceberg_type, DecimalType):
        return f"decimal({iceberg_type.precision}, {iceberg_type.scale})"
    if isinstance(iceberg_type, FixedType):
        return f"fixed({len(iceberg_type)})"
    raise SchemaError(f"unsupported Iceberg type in table metadata: {iceberg_type}")


def _normalize_type_text(type_text: str) -> str:
    return type_text.strip().lower()


def _coerce_type_text(name: str, type_value: str | SchemaType) -> str:
    if isinstance(type_value, SchemaType):
        return type_value.type_text
    if isinstance(type_value, str):
        return type_value
    raise SchemaError(f"type for column {name!r} must be a string or SchemaType")
