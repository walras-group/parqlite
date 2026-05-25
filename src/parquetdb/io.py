from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as pq

from parquetdb.errors import InputDataError, SchemaMismatchError


def to_arrow_table(data: Any, target_schema: pa.Schema) -> pa.Table:
    if isinstance(data, pa.Table):
        table = data
    elif isinstance(data, pd.DataFrame):
        table = pa.Table.from_pandas(data, preserve_index=False)
    elif isinstance(data, str | Path):
        table = _read_file(Path(data))
    else:
        raise InputDataError(
            "data must be a pandas.DataFrame, pyarrow.Table, or a parquet/csv/json file path"
        )

    _validate_column_order(table, target_schema)
    try:
        return table.cast(target_schema, safe=True)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
        raise SchemaMismatchError(
            f"data cannot be safely cast to table schema: {exc}"
        ) from exc


def _read_file(path: Path) -> pa.Table:
    if not path.exists():
        raise InputDataError(f"input file does not exist: {path}")

    suffix = path.suffix.lower()
    try:
        if suffix == ".parquet":
            return pq.read_table(path)
        if suffix == ".csv":
            return pacsv.read_csv(path)
        if suffix in {".json", ".jsonl", ".ndjson"}:
            return _read_json(path)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OSError, ValueError) as exc:
        raise InputDataError(f"could not read input file {path}: {exc}") from exc

    raise InputDataError(
        "file input must have a .parquet, .csv, .json, .jsonl, or .ndjson suffix"
    )


def _read_json(path: Path) -> pa.Table:
    try:
        return pajson.read_json(path)
    except pa.ArrowInvalid:
        dataframe = pd.read_json(path)
        return pa.Table.from_pandas(dataframe, preserve_index=False)


def _validate_column_order(table: pa.Table, target_schema: pa.Schema) -> None:
    actual = table.column_names
    expected = target_schema.names
    if actual == expected:
        return

    missing = [column for column in expected if column not in actual]
    extra = [column for column in actual if column not in expected]
    if missing or extra:
        parts = []
        if missing:
            parts.append(f"missing columns: {', '.join(missing)}")
        if extra:
            parts.append(f"extra columns: {', '.join(extra)}")
        raise SchemaMismatchError("; ".join(parts))

    raise SchemaMismatchError(
        f"column order must match table schema: expected {expected}, got {actual}"
    )
