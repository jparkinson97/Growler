from __future__ import annotations

from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from growler.errors import SchemaMismatch
from growler.pointer import Pointer, Schema
from growler.types import type_matches


def extract_leaf_paths(arrow_schema: pa.Schema) -> dict[str, pa.DataType]:
    out: dict[str, pa.DataType] = {}
    for field in arrow_schema:
        _walk_field(field, "", out)
    return out


def _walk_field(field: pa.Field, prefix: str, out: dict[str, pa.DataType]) -> None:
    path = field.name if not prefix else f"{prefix}.{field.name}"
    t = field.type
    if pa.types.is_struct(t):
        out[path] = t
        for i in range(t.num_fields):
            _walk_field(t.field(i), path, out)
        return
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        out[path] = t
        inner = t.value_type
        if pa.types.is_struct(inner):
            for i in range(inner.num_fields):
                _walk_field(inner.field(i), path, out)
        return
    if pa.types.is_map(t):
        out[path] = t
        inner = t.item_type
        if pa.types.is_struct(inner):
            for i in range(inner.num_fields):
                _walk_field(inner.field(i), path, out)
        return
    out[path] = t


def validate_file_schema(
    file_arrow_schema: pa.Schema,
    table_schema: Schema,
    file_column_mapping: Optional[dict[int, str]] = None,
) -> dict[int, str]:
    file_leaves = extract_leaf_paths(file_arrow_schema)
    mapping: dict[int, str] = {}
    for col in table_schema.columns:
        file_path = (
            file_column_mapping.get(col.id, col.path) if file_column_mapping else col.path
        )
        if file_path not in file_leaves:
            raise SchemaMismatch(
                "File missing column required by table schema. Writer must be upgraded.",
                missing=col.path,
                column_id=col.id,
            )
        if not type_matches(file_leaves[file_path], col.type):
            raise SchemaMismatch(
                "Type mismatch for column in file.",
                column_id=col.id,
                path=col.path,
                expected=col.type,
                got=str(file_leaves[file_path]),
            )
        mapping[col.id] = file_path
    return mapping


def reject_disallowed_encodings(parquet_file_schema) -> None:
    for i in range(len(parquet_file_schema.names)):
        col = parquet_file_schema.column(i)
        if col.physical_type == "INT96":
            raise SchemaMismatch(
                "INT96 timestamps are not allowed.",
                path=col.path,
            )


def extract_stats(pq_file: pq.ParquetFile, column_mapping: dict[int, str]) -> dict[int, dict]:
    stats: dict[int, dict] = {}
    md = pq_file.metadata
    schema = pq_file.schema_arrow
    top_leaf_paths = extract_leaf_paths(schema)
    id_to_top_col_index: dict[int, int] = {}
    names = [f.name for f in schema]
    for col_id, path in column_mapping.items():
        top = path.split(".", 1)[0]
        if top in names:
            id_to_top_col_index[col_id] = names.index(top)
    if md is None:
        return stats
    for col_id, col_index in id_to_top_col_index.items():
        path = column_mapping[col_id]
        if path not in top_leaf_paths:
            continue
        t = top_leaf_paths[path]
        if (
            pa.types.is_struct(t)
            or pa.types.is_list(t)
            or pa.types.is_large_list(t)
            or pa.types.is_map(t)
        ):
            continue
        agg_min = None
        agg_max = None
        nulls = 0
        found = False
        for rg_i in range(md.num_row_groups):
            rg = md.row_group(rg_i)
            for c_i in range(rg.num_columns):
                c = rg.column(c_i)
                if c.path_in_schema != path.replace(".", "."):
                    continue
                if c.statistics is None:
                    continue
                s = c.statistics
                found = True
                if s.has_min_max:
                    if agg_min is None or s.min < agg_min:
                        agg_min = s.min
                    if agg_max is None or s.max > agg_max:
                        agg_max = s.max
                if s.null_count is not None:
                    nulls += s.null_count
        if found:
            stats[col_id] = {"min": agg_min, "max": agg_max, "null_count": nulls}
    return stats


def read_parquet_metadata(s3_uri: str, filesystem=None) -> pq.ParquetFile:
    path = s3_uri.replace("s3://", "") if filesystem is not None else s3_uri
    return pq.ParquetFile(path, filesystem=filesystem)
