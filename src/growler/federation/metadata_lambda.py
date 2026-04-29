from __future__ import annotations

import json
from typing import Any, Optional
import uuid

import pyarrow as pa

from growler.errors import GrowlerError, TableNotFound
from growler.federation.constraints import ConstraintsEvaluator, parse_constraints
from growler.federation.serde import encode_schema, partitions_block_from_dicts
from growler.federation.synthetic_partition import (
    SYNTHETIC_COLUMN,
    SYNTHETIC_TYPE,
    SYNTHETIC_VALUE,
    augment_schema,
    needed,
)
from growler.federation.wire import (
    error_envelope,
    make_split,
    ok_envelope,
    req_type,
    table_name,
)
from growler.metadata_handler import MetadataHandler


SOURCE_TYPE = "growler"


class MetadataFederationLambda:
    def __init__(
        self,
        metadata: MetadataHandler,
        catalog_name: str = "growler",
        source_type: str = SOURCE_TYPE,
    ):
        self.metadata = metadata
        self.catalog_name = catalog_name
        self.source_type = source_type

    def handle(self, event: dict) -> dict:
        try:
            rt = req_type(event)
            if rt == "PingRequest":
                return self._ping(event)
            if rt == "ListSchemasRequest":
                return self._list_schemas(event)
            if rt == "ListTablesRequest":
                return self._list_tables(event)
            if rt == "GetTableRequest":
                return self._get_table(event)
            if rt == "GetTableLayoutRequest":
                return self._get_table_layout(event)
            if rt == "GetSplitsRequest":
                return self._get_splits(event)
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

    def _list_schemas(self, event: dict) -> dict:
        return ok_envelope(
            "ListSchemasResponse",
            catalogName=event.get("catalogName", self.catalog_name),
            schemas=self.metadata.do_list_schema_names(),
        )

    def _list_tables(self, event: dict) -> dict:
        schema = event.get("schemaName") or event.get("schema") or ""
        tables = [
            {"schemaName": schema, "tableName": t}
            for t in self.metadata.do_list_tables(schema)
        ]
        return ok_envelope(
            "ListTablesResponse",
            tables=tables,
            catalogName=event.get("catalogName", self.catalog_name),
        )

    def _get_table(self, event: dict) -> dict:
        schema_name, table = table_name(event.get("tableName") or {})
        info = self.metadata.do_get_table(schema_name, table)
        arrow_schema = info["arrow_schema"]
        partition_columns = info["partition_columns"]
        if needed(partition_columns):
            arrow_schema = augment_schema(arrow_schema)
            partition_columns = [SYNTHETIC_COLUMN]
        return ok_envelope(
            "GetTableResponse",
            catalogName=event.get("catalogName", self.catalog_name),
            tableName={"schemaName": schema_name, "tableName": table},
            schema=encode_schema(arrow_schema),
            partitionColumns=partition_columns,
        )

    def _get_table_layout(self, event: dict) -> dict:
        schema_name, table = table_name(event.get("tableName") or {})
        constraints_json = event.get("constraints") or {}
        parsed = parse_constraints(constraints_json)
        evaluator = ConstraintsEvaluator(parsed)
        predicate = evaluator.partition_predicate() if parsed else None
        parts = self.metadata.get_partitions(schema_name, table, predicate)
        info = self.metadata.do_get_table(schema_name, table)
        partition_columns = info["partition_columns"]
        if needed(partition_columns):
            partition_columns = [SYNTHETIC_COLUMN]
            partition_types = [SYNTHETIC_TYPE]
            rows = [{SYNTHETIC_COLUMN: SYNTHETIC_VALUE}]
        else:
            partition_types = _partition_types(info["arrow_schema"], partition_columns)
            rows = [p["partition_values"] for p in parts]
        block = partitions_block_from_dicts(list(zip(partition_columns, partition_types)), rows)
        return ok_envelope(
            "GetTableLayoutResponse",
            catalogName=event.get("catalogName", self.catalog_name),
            tableName={"schemaName": schema_name, "tableName": table},
            partitions=block,
        )

    def _get_splits(self, event: dict) -> dict:
        schema_name, table = table_name(event.get("tableName") or {})
        partitions_block = event.get("partitions") or None
        continuation_token = event.get("continuationToken")
        
        # 1. Grab the queryId from the event (used to organize spill files)
        query_id = event.get("queryId", "unknown-query")
        
        # 2. Get spill bucket and prefix (usually passed as Lambda Environment Variables)
        spill_bucket = "asf-custom-data-source"
        spill_prefix = "spill"

        from growler.federation.serde import partitions_from_block

        info = self.metadata.do_get_table(schema_name, table)
        synthetic = needed(info["partition_columns"])

        if partitions_block and not synthetic:
            partition_rows = partitions_from_block(partitions_block)
            eligible = set(_tuple_key(r) for r in partition_rows)
        else:
            eligible = None
            
        splits = self.metadata.do_get_splits(schema_name, table)
        if eligible is not None:
            splits = [s for s in splits if _tuple_key(s.get("partition_values", {})) in eligible]
            
        start = int(continuation_token) if continuation_token else 0
        page_size = 1000
        page = splits[start : start + page_size]
        next_token = str(start + page_size) if start + page_size < len(splits) else None
        
        # 3. Add the spill_location dict to make_split
        split_objs = []
        for s in page:
            split_id = str(uuid.uuid4())
            
            spill_location = {
                "@type": "S3SpillLocation",
                "bucket": spill_bucket,
                "key": f"{spill_prefix}/{query_id}/{split_id}",
                "directory": True
            }
            
            split_objs.append(
                make_split(
                    properties={
                        "s3_uri": s["s3_uri"],
                        "table": s["table"],
                        "column_mapping": json.dumps(s.get("column_mapping", {})),
                        "partition_values": json.dumps(
                            _augment_partition_values(s.get("partition_values", {}), synthetic)
                        ),
                        "row_count": str(s.get("row_count", 0)),
                    },
                    spill_location=spill_location
                )
            )

        return ok_envelope(
            "GetSplitsResponse",
            catalogName=event.get("catalogName", self.catalog_name),
            splits=split_objs,
            continuationToken=next_token,
        )

def _tuple_key(pvals: dict[str, str]) -> tuple:
    return tuple(sorted((k, str(v)) for k, v in pvals.items()))


def _augment_partition_values(pvals: dict, synthetic: bool) -> dict:
    if not synthetic:
        return pvals
    out = dict(pvals)
    out[SYNTHETIC_COLUMN] = SYNTHETIC_VALUE
    return out


def _partition_types(arrow_schema: pa.Schema, partition_columns: list[str]) -> list[pa.DataType]:
    types: list[pa.DataType] = []
    for p in partition_columns:
        try:
            t = arrow_schema.field(p).type
            if pa.types.is_date(t) or pa.types.is_timestamp(t) or pa.types.is_decimal(t):
                types.append(pa.string())
            else:
                types.append(pa.string() if not pa.types.is_primitive(t) else t)
        except KeyError:
            types.append(pa.string())
    return types
