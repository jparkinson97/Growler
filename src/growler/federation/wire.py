from __future__ import annotations

from typing import Any, Optional


REQUEST_TYPES = {
    "PingRequest",
    "ListSchemasRequest",
    "ListTablesRequest",
    "GetTableRequest",
    "GetTableLayoutRequest",
    "GetSplitsRequest",
    "ReadRecordsRequest",
}


def req_type(event: dict) -> str:
    t = event.get("@type") or ""
    if "." in t:
        t = t.rsplit(".", 1)[-1]
    return t


def ok_envelope(response_type: str, **fields: Any) -> dict:
    resp: dict[str, Any] = {"@type": response_type}
    resp.update(fields)
    return resp


def error_envelope(message: str, error_code: str = "InternalError") -> dict:
    return {
        "@type": "FederationException",
        "message": message,
        "errorCode": error_code,
    }


def table_name(name: dict) -> tuple[str, str]:
    return name.get("schemaName", ""), name.get("tableName", "")


def split_properties(split: dict) -> dict[str, str]:
    return dict(split.get("properties") or {})


def make_split(properties: dict[str, str], spill_location: Optional[dict] = None, encryption_key: Optional[dict] = None) -> dict:
    return {
        "spillLocation": spill_location,
        "encryptionKey": encryption_key,
        "properties": dict(properties),
    }
