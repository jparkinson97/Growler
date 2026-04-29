from __future__ import annotations

import re
from typing import Optional

import pyarrow as pa

from growler.errors import InvalidType


_DECIMAL_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")
_LIST_RE = re.compile(r"^list<(.+)>$")

_PRIMITIVES = {
    "bool",
    "int8",
    "int16",
    "int32",
    "int64",
    "float32",
    "float64",
    "string",
    "binary",
    "date",
    "timestamp_ms",
    "timestamp_us",
    "timestamp_ns",
}


def _parse_map(type_str: str) -> Optional[tuple[str, str]]:
    if not type_str.startswith("map<") or not type_str.endswith(">"):
        return None
    inner = type_str[4:-1]
    depth = 0
    for i, c in enumerate(inner):
        if c == "<":
            depth += 1
        elif c == ">":
            depth -= 1
        elif c == "," and depth == 0:
            return inner[:i].strip(), inner[i + 1 :].strip()
    return None


def is_valid_type(type_str: str) -> bool:
    if type_str in _PRIMITIVES:
        return True
    if _DECIMAL_RE.match(type_str):
        return True
    m = _LIST_RE.match(type_str)
    if m:
        inner = m.group(1)
        if inner == "struct":
            return True
        return is_valid_type(inner)
    if type_str == "struct":
        return True
    map_parsed = _parse_map(type_str)
    if map_parsed:
        k, v = map_parsed
        if not is_valid_type(k):
            return False
        if v == "struct":
            return True
        return is_valid_type(v)
    return False


def validate_type(type_str: str) -> str:
    if not is_valid_type(type_str):
        raise InvalidType(f"Unsupported type: {type_str!r}")
    return type_str


def to_arrow(type_str: str) -> pa.DataType:
    if type_str == "bool":
        return pa.bool_()
    if type_str == "int8":
        return pa.int8()
    if type_str == "int16":
        return pa.int16()
    if type_str == "int32":
        return pa.int32()
    if type_str == "int64":
        return pa.int64()
    if type_str == "float32":
        return pa.float32()
    if type_str == "float64":
        return pa.float64()
    if type_str == "string":
        return pa.string()
    if type_str == "binary":
        return pa.binary()
    if type_str == "date":
        return pa.date32()
    if type_str == "timestamp_ms":
        return pa.date64()
    if type_str == "timestamp_us":
        return pa.date64()
    if type_str == "timestamp_ns":
        return pa.date64()
    m = _DECIMAL_RE.match(type_str)
    if m:
        p, s = int(m.group(1)), int(m.group(2))
        return pa.decimal128(p, s)
    m = _LIST_RE.match(type_str)
    if m:
        inner = m.group(1)
        if inner == "struct":
            return pa.list_(pa.struct([]))
        return pa.list_(to_arrow(inner))
    if type_str == "struct":
        return pa.struct([])
    map_parsed = _parse_map(type_str)
    if map_parsed:
        k, v = map_parsed
        if v == "struct":
            return pa.map_(to_arrow(k), pa.struct([]))
        return pa.map_(to_arrow(k), to_arrow(v))
    raise InvalidType(f"Cannot map to Arrow: {type_str!r}")


def type_matches(pa_type: pa.DataType, type_str: str) -> bool:
    if type_str == "bool":
        return pa.types.is_boolean(pa_type)
    if type_str == "int8":
        return pa.types.is_int8(pa_type)
    if type_str == "int16":
        return pa.types.is_int16(pa_type)
    if type_str == "int32":
        return pa.types.is_int32(pa_type)
    if type_str == "int64":
        return pa.types.is_int64(pa_type)
    if type_str == "float32":
        return pa.types.is_float32(pa_type)
    if type_str == "float64":
        return pa.types.is_float64(pa_type)
    if type_str == "string":
        return pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type)
    if type_str == "binary":
        return pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type)
    if type_str == "date":
        return pa.types.is_date(pa_type)
    if type_str == "timestamp_ms":
        return pa.types.is_timestamp(pa_type) and pa_type.unit == "ms"
    if type_str == "timestamp_us":
        return pa.types.is_timestamp(pa_type) and pa_type.unit == "us"
    if type_str == "timestamp_ns":
        return pa.types.is_timestamp(pa_type) and pa_type.unit == "ns"
    m = _DECIMAL_RE.match(type_str)
    if m:
        p, s = int(m.group(1)), int(m.group(2))
        return pa.types.is_decimal(pa_type) and pa_type.precision == p and pa_type.scale == s
    m = _LIST_RE.match(type_str)
    if m:
        if not pa.types.is_list(pa_type) and not pa.types.is_large_list(pa_type):
            return False
        inner = m.group(1)
        if inner == "struct":
            return pa.types.is_struct(pa_type.value_type)
        return type_matches(pa_type.value_type, inner)
    if type_str == "struct":
        return pa.types.is_struct(pa_type)
    map_parsed = _parse_map(type_str)
    if map_parsed:
        if not pa.types.is_map(pa_type):
            return False
        k, v = map_parsed
        if not type_matches(pa_type.key_type, k):
            return False
        if v == "struct":
            return pa.types.is_struct(pa_type.item_type)
        return type_matches(pa_type.item_type, v)
    return False


def reject_int96_timestamps(arrow_schema: pa.Schema) -> None:
    pass
