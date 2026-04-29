from __future__ import annotations

import json
from typing import Any, Optional

import pyarrow as pa
import pyarrow.compute as pc

from growler.errors import GrowlerError, TableNotFound
from growler.federation.constraints import ConstraintsEvaluator, parse_constraints
from growler.federation.serde import decode_schema, encode_block, encode_schema
from growler.federation.synthetic_partition import (
    SYNTHETIC_COLUMN,
    augment_table_with_value,
)
from growler.federation.wire import error_envelope, ok_envelope, req_type, split_properties
from growler.record_handler import RecordHandler


SOURCE_TYPE = "growler"


class RecordFederationLambda:
    def __init__(
        self,
        record: RecordHandler,
        catalog_name: str = "growler",
        source_type: str = SOURCE_TYPE,
    ):
        self.record = record
        self.catalog_name = catalog_name
        self.source_type = source_type

    def handle(self, event: dict) -> dict:
        try:
            rt = req_type(event)
            if rt == "PingRequest":
                return self._ping(event)
            if rt == "ReadRecordsRequest":
                return self._read_records(event)
            return error_envelope(f"Unknown request type: {rt}", "UnsupportedOperationException")
        except TableNotFound as e:
            return error_envelope(f"Table not found: {e.table}", "TableNotFoundException")
        except GrowlerError as e:
            return error_envelope(str(e), "InternalError")

    def lambda_handler(self, event: dict, context: Any = None) -> dict:
        return self.handle(event)

    def _ping(self, event: dict) -> dict:
        return ok_envelope(
            "PingResponse",
            catalogName=event.get("catalogName", self.catalog_name),
            queryId=event.get("queryId", ""),
            sourceType=self.source_type,
            capabilities=0,
            serDeVersion=2,
        )

    def _read_records(self, event: dict) -> dict:
        split = event.get("split") or {}
        properties = split_properties(split)
        if "s3_uri" not in properties or "table" not in properties:
            return error_envelope("Split missing required properties", "InvalidArgumentException")
        internal_split = _split_from_properties(properties)
        requested_schema_b64 = event.get("schema")
        requested_paths: Optional[list[str]] = None
        wants_synthetic = False
        if requested_schema_b64:
            requested_schema = decode_schema(requested_schema_b64)
            requested_paths = list(requested_schema.names)
            if SYNTHETIC_COLUMN in requested_paths:
                wants_synthetic = True
                requested_paths = [p for p in requested_paths if p != SYNTHETIC_COLUMN]
        constraints_json = event.get("constraints") or {}
        evaluator = ConstraintsEvaluator(parse_constraints(constraints_json))
        batches = list(self.record.read_split(internal_split, requested_paths=requested_paths))
        if batches:
            table = pa.Table.from_batches(batches)
            table = evaluator.filter_table(table)
            table = _cast_for_athena(table)
            if wants_synthetic:
                table = augment_table_with_value(table)
            combined = table.combine_chunks().to_batches()
            batch = combined[0] if combined else None
            schema = table.schema
        else:
            schema = pa.schema([])
            batch = None
        return ok_envelope(
            "ReadRecordsResponse",
            catalogName=event.get("catalogName", self.catalog_name),
            records=encode_block(schema, batch),
        )


def _split_from_properties(properties: dict[str, str]) -> dict:
    return {
        "table": properties["table"],
        "s3_uri": properties["s3_uri"],
        "column_mapping": json.loads(properties.get("column_mapping") or "{}"),
        "partition_values": json.loads(properties.get("partition_values") or "{}"),
        "row_count": int(properties.get("row_count", "0")),
    }


def _cast_for_athena(table: pa.Table) -> pa.Table:
    changed = False
    new_columns = []
    new_fields = []
    for i, field in enumerate(table.schema):
        column = table.column(i)
        if pa.types.is_timestamp(field.type):
            column = _timestamp_to_date64_ms(column)
            field = pa.field(field.name, pa.date64(), nullable=field.nullable)
            changed = True
        new_columns.append(column)
        new_fields.append(field)
    if not changed:
        return table
    return pa.table(new_columns, schema=pa.schema(new_fields))


def _timestamp_to_date64_ms(column):
    unit = column.type.unit
    as_int64 = column.cast(pa.int64())
    if unit == "s":
        ms = pc.multiply(as_int64, 1000)
    elif unit == "ms":
        ms = as_int64
    elif unit == "us":
        ms = pc.divide(as_int64, 1000)
    elif unit == "ns":
        ms = pc.divide(as_int64, 1_000_000)
    else:
        raise ValueError(f"Unsupported timestamp unit: {unit}")
    chunks = ms.chunks if isinstance(ms, pa.ChunkedArray) else [ms]
    return pa.chunked_array([c.view(pa.date64()) for c in chunks], type=pa.date64())
