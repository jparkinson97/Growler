from __future__ import annotations

import base64
from typing import Iterable, Optional

import pyarrow as pa
import pyarrow.ipc as ipc


ALLOC_ID = "growler"


def encode_schema(schema: pa.Schema) -> str:
    return base64.b64encode(schema.serialize().to_pybytes()).decode("ascii")


def decode_schema(b64: str) -> pa.Schema:
    return ipc.read_schema(pa.BufferReader(base64.b64decode(b64)))


def encode_batch(batch: pa.RecordBatch) -> str:
    return base64.b64encode(batch.serialize().to_pybytes()).decode("ascii")


def decode_batch(b64: str, schema: pa.Schema) -> pa.RecordBatch:
    return ipc.read_record_batch(pa.BufferReader(base64.b64decode(b64)), schema)


def encode_block(schema: pa.Schema, batch: Optional[pa.RecordBatch]) -> dict:
    if batch is None:
        batch = pa.record_batch([pa.array([], type=f.type) for f in schema], schema=schema)
    return {
        "aId": ALLOC_ID,
        "schema": encode_schema(schema),
        "records": encode_batch(batch),
    }


def decode_block(block: dict) -> tuple[pa.Schema, pa.RecordBatch]:
    schema = decode_schema(block["schema"])
    batch = decode_batch(block["records"], schema)
    return schema, batch


def partitions_block_from_dicts(
    partition_columns: list[tuple[str, pa.DataType]],
    rows: Iterable[dict[str, str]],
) -> dict:
    schema = pa.schema([pa.field(name, dtype) for name, dtype in partition_columns])
    columns: dict[str, list] = {name: [] for name, _ in partition_columns}
    row_list = list(rows)
    for row in row_list:
        for name, dtype in partition_columns:
            columns[name].append(_coerce(row.get(name), dtype))
    if not partition_columns:
        if row_list:
            placeholder = pa.table({"_": pa.array([None] * len(row_list), type=pa.null())})
            batch = placeholder.drop(["_"]).to_batches()[0]
        else:
            batch = None
    else:
        arrays = [pa.array(columns[name], type=dtype) for name, dtype in partition_columns]
        batch = pa.record_batch(arrays, schema=schema) if row_list else None
    return encode_block(schema, batch)


def partitions_from_block(block: dict) -> list[dict[str, str]]:
    schema, batch = decode_block(block)
    out: list[dict[str, str]] = []
    if batch.num_rows == 0:
        return out
    cols = {name: batch.column(i).to_pylist() for i, name in enumerate(batch.schema.names)}
    for i in range(batch.num_rows):
        row = {name: _stringify(values[i]) for name, values in cols.items()}
        out.append(row)
    return out


def _coerce(value, dtype: pa.DataType):
    if value is None:
        return None
    if pa.types.is_string(dtype):
        return str(value)
    if pa.types.is_integer(dtype):
        return int(value)
    if pa.types.is_floating(dtype):
        return float(value)
    if pa.types.is_boolean(dtype):
        return bool(value)
    return value


def _stringify(v) -> str:
    if v is None:
        return ""
    return str(v)
