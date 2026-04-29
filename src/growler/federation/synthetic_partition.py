from __future__ import annotations

import pyarrow as pa


SYNTHETIC_COLUMN = "__athena_partition__"
SYNTHETIC_VALUE = "0"
SYNTHETIC_TYPE = pa.int32()


def needed(user_partition_columns: list[str]) -> bool:
    return not user_partition_columns


def augment_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema(list(schema) + [pa.field(SYNTHETIC_COLUMN, SYNTHETIC_TYPE)])


def augment_table_with_value(table: pa.Table) -> pa.Table:
    if SYNTHETIC_COLUMN in table.column_names:
        return table
    arr = pa.array([int(SYNTHETIC_VALUE)] * table.num_rows, type=SYNTHETIC_TYPE)
    return table.append_column(SYNTHETIC_COLUMN, arr)
