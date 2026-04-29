from __future__ import annotations

from typing import Callable, Iterable, Optional

import pyarrow as pa

from growler.manifest import walk_leaves
from growler.pointer import Column, Pointer, Schema
from growler.storage import PointerStore
from growler.types import to_arrow


def build_arrow_schema(schema: Schema) -> pa.Schema:
    roots: dict = {}
    for c in schema.columns:
        parts = c.path.split(".")
        node = roots
        for i, part in enumerate(parts):
            if part not in node:
                node[part] = {"children": {}, "column": None}
            if i == len(parts) - 1:
                node[part]["column"] = c
            node = node[part]["children"]
    fields = [_emit_field(name, entry) for name, entry in roots.items()]
    return pa.schema(fields)


def _emit_field(name: str, entry: dict) -> pa.Field:
    from growler.types import _parse_map

    col: Column | None = entry["column"]
    children = entry["children"]
    if col is None:
        inner = [_emit_field(n, e) for n, e in children.items()]
        return pa.field(name, pa.struct(inner), nullable=True)
    t = col.type
    if t == "struct":
        inner = [_emit_field(n, e) for n, e in children.items()]
        return pa.field(name, pa.struct(inner), nullable=col.nullable)
    if t == "list<struct>":
        inner = [_emit_field(n, e) for n, e in children.items()]
        return pa.field(name, pa.list_(pa.struct(inner)), nullable=col.nullable)
    map_parsed = _parse_map(t)
    if map_parsed and map_parsed[1] == "struct":
        k_type = to_arrow(map_parsed[0])
        inner = [_emit_field(n, e) for n, e in children.items()]
        return pa.field(name, pa.map_(k_type, pa.struct(inner)), nullable=col.nullable)
    return pa.field(name, to_arrow(t), nullable=col.nullable)


PredicateFn = Callable[[dict[str, str]], bool]


def _always_true(_: dict[str, str]) -> bool:
    return True


class MetadataHandler:
    def __init__(
        self,
        store: PointerStore,
        catalog: Optional[dict[str, list[str]]] = None,
    ):
        self.store = store
        self.catalog = catalog or {}

    def do_list_schema_names(self) -> list[str]:
        return sorted(self.catalog.keys())

    def do_list_tables(self, schema_name: str) -> list[str]:
        return sorted(self.catalog.get(schema_name, []))

    def do_get_table(self, schema_name: str, table: str) -> dict:
        loaded = self.store.load(_qualified(schema_name, table))
        pointer = loaded.pointer
        arrow_schema = build_arrow_schema(pointer.schema)
        partition_paths = [pointer.schema.by_id(i).path for i in pointer.partition_spec]
        return {
            "schema": schema_name,
            "table": table,
            "arrow_schema": arrow_schema,
            "partition_columns": partition_paths,
            "version": pointer.version,
        }

    def get_partitions(
        self,
        schema_name: str,
        table: str,
        predicate: Optional[PredicateFn] = None,
    ) -> list[dict]:
        loaded = self.store.load(_qualified(schema_name, table))
        pointer = loaded.pointer
        pred = predicate or _always_true
        out: list[dict] = []
        for path, leaf in walk_leaves(pointer.manifest_tree):
            pvals = {pointer.schema.by_id(col_id).path: value for col_id, value in path}
            if not pred(pvals):
                continue
            out.append({"partition_values": pvals, "files": list(leaf.get("files", []))})
        return out

    def do_get_splits(
        self,
        schema_name: str,
        table: str,
        predicate: Optional[PredicateFn] = None,
    ) -> list[dict]:
        partitions = self.get_partitions(schema_name, table, predicate)
        splits: list[dict] = []
        for p in partitions:
            for f in p["files"]:
                splits.append(
                    {
                        "table": _qualified(schema_name, table),
                        "s3_uri": f["s3_uri"],
                        "partition_values": f.get("partition_values", p["partition_values"]),
                        "column_mapping": f.get("column_mapping", {}),
                        "row_count": f.get("row_count", 0),
                        "size_bytes": f.get("size_bytes", 0),
                        "stats": f.get("stats", {}),
                    }
                )
        return splits


def _qualified(schema_name: str, table: str) -> str:
    if schema_name:
        return f"{schema_name}/{table}"
    return table


def eq_predicate(**equalities: str) -> PredicateFn:
    want = {k: str(v) for k, v in equalities.items()}

    def _pred(pvals: dict[str, str]) -> bool:
        for k, v in want.items():
            if pvals.get(k) != v:
                return False
        return True

    return _pred
