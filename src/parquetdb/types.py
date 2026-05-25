from __future__ import annotations

import builtins
from dataclasses import dataclass

from parquetdb.errors import SchemaError


@dataclass(frozen=True)
class SchemaType:
    type_text: str

    def __str__(self) -> str:
        return self.type_text


boolean = SchemaType("boolean")
int = SchemaType("int")
long = SchemaType("long")
float = SchemaType("float")
double = SchemaType("double")
date = SchemaType("date")
time = SchemaType("time")
timestamp = SchemaType("timestamp")
timestamptz = SchemaType("timestamptz")
string = SchemaType("string")
uuid = SchemaType("uuid")
binary = SchemaType("binary")


def decimal(precision: builtins.int, scale: builtins.int) -> SchemaType:
    precision = _validate_integer("decimal precision", precision)
    scale = _validate_integer("decimal scale", scale)
    if precision < 1 or precision > 38:
        raise SchemaError("decimal precision must be between 1 and 38")
    if scale < 0 or scale > precision:
        raise SchemaError("decimal scale must be between 0 and precision")
    return SchemaType(f"decimal({precision}, {scale})")


def fixed(length: builtins.int) -> SchemaType:
    length = _validate_integer("fixed length", length)
    if length < 1:
        raise SchemaError("fixed length must be positive")
    return SchemaType(f"fixed({length})")


def _validate_integer(name: str, value: object) -> builtins.int:
    if type(value) is not builtins.int:
        raise SchemaError(f"{name} must be an integer")
    return value


__all__ = [
    "SchemaType",
    "binary",
    "boolean",
    "date",
    "decimal",
    "double",
    "fixed",
    "float",
    "int",
    "long",
    "string",
    "time",
    "timestamp",
    "timestamptz",
    "uuid",
]
