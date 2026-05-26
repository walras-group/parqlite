from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    YearTransform,
)

from parqlite.errors import PartitionError


TransformName = Literal[
    "identity", "year", "month", "day", "hour", "bucket", "truncate"
]


@dataclass(frozen=True)
class PartitionTransform:
    source: str
    transform: TransformName
    parameter: int | None = None

    @property
    def name(self) -> str:
        if self.transform == "identity":
            return self.source
        if self.transform == "bucket":
            return f"{self.source}_bucket_{self.parameter}"
        if self.transform == "truncate":
            return f"{self.source}_trunc_{self.parameter}"
        return f"{self.source}_{self.transform}"


def identity(source: str) -> PartitionTransform:
    return PartitionTransform(_validate_source(source), "identity")


def year(source: str) -> PartitionTransform:
    return PartitionTransform(_validate_source(source), "year")


def month(source: str) -> PartitionTransform:
    return PartitionTransform(_validate_source(source), "month")


def day(source: str) -> PartitionTransform:
    return PartitionTransform(_validate_source(source), "day")


def hour(source: str) -> PartitionTransform:
    return PartitionTransform(_validate_source(source), "hour")


def bucket(source: str, num_buckets: int) -> PartitionTransform:
    if not isinstance(num_buckets, int) or num_buckets < 1:
        raise PartitionError("bucket count must be a positive integer")
    return PartitionTransform(_validate_source(source), "bucket", num_buckets)


def truncate(source: str, width: int) -> PartitionTransform:
    if not isinstance(width, int) or width < 1:
        raise PartitionError("truncate width must be a positive integer")
    return PartitionTransform(_validate_source(source), "truncate", width)


def build_partition_spec(
    schema: Schema,
    partition_by: list[str | PartitionTransform]
    | tuple[str | PartitionTransform, ...]
    | None,
) -> PartitionSpec:
    if partition_by is None:
        return PartitionSpec()

    fields: list[PartitionField] = []
    for index, partition in enumerate(partition_by, start=1000):
        transform = identity(partition) if isinstance(partition, str) else partition
        if not isinstance(transform, PartitionTransform):
            raise PartitionError(
                "partition_by entries must be column names or partition transforms"
            )

        try:
            source_field = schema.find_field(transform.source)
        except ValueError as exc:
            raise PartitionError(
                f"partition source field does not exist: {transform.source}"
            ) from exc

        fields.append(
            PartitionField(
                source_id=source_field.field_id,
                field_id=index,
                transform=_to_iceberg_transform(transform),
                name=transform.name,
            )
        )

    return PartitionSpec(*fields)


def _to_iceberg_transform(transform: PartitionTransform) -> Transform:
    if transform.transform == "identity":
        return IdentityTransform()
    if transform.transform == "year":
        return YearTransform()
    if transform.transform == "month":
        return MonthTransform()
    if transform.transform == "day":
        return DayTransform()
    if transform.transform == "hour":
        return HourTransform()
    if transform.transform == "bucket":
        if transform.parameter is None:
            raise PartitionError("bucket transform requires a bucket count")
        return BucketTransform(transform.parameter)
    if transform.transform == "truncate":
        if transform.parameter is None:
            raise PartitionError("truncate transform requires a width")
        return TruncateTransform(transform.parameter)
    raise PartitionError(f"unsupported partition transform: {transform.transform}")


def _validate_source(source: str) -> str:
    if not isinstance(source, str) or not source:
        raise PartitionError("partition source must be a non-empty column name")
    return source
