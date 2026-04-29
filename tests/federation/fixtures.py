from __future__ import annotations

from typing import Optional


IDENTITY = {
    "@type": "FederatedIdentity",
    "id": "test",
    "principal": "test",
    "account": "000000000000",
    "configOptions": {},
    "properties": {},
}


def ping(catalog: str = "growler", query_id: str = "q1") -> dict:
    return {
        "@type": "PingRequest",
        "identity": IDENTITY,
        "catalogName": catalog,
        "queryId": query_id,
    }


def list_schemas(catalog: str = "growler") -> dict:
    return {
        "@type": "ListSchemasRequest",
        "identity": IDENTITY,
        "catalogName": catalog,
        "queryId": "q",
    }


def list_tables(schema: str, catalog: str = "growler") -> dict:
    return {
        "@type": "ListTablesRequest",
        "identity": IDENTITY,
        "catalogName": catalog,
        "schemaName": schema,
        "queryId": "q",
    }


def get_table(schema: str, table: str, catalog: str = "growler") -> dict:
    return {
        "@type": "GetTableRequest",
        "identity": IDENTITY,
        "catalogName": catalog,
        "tableName": {"schemaName": schema, "tableName": table},
        "queryId": "q",
    }


def get_table_layout(
    schema: str,
    table: str,
    catalog: str = "growler",
    constraints: Optional[dict] = None,
) -> dict:
    return {
        "@type": "GetTableLayoutRequest",
        "identity": IDENTITY,
        "catalogName": catalog,
        "tableName": {"schemaName": schema, "tableName": table},
        "constraints": constraints or {"@type": "Constraints", "summary": {}},
        "schema": "",
        "partitionCols": [],
        "queryId": "q",
    }


def get_splits(
    schema: str,
    table: str,
    partitions_block: dict,
    catalog: str = "growler",
    continuation_token: Optional[str] = None,
) -> dict:
    event = {
        "@type": "GetSplitsRequest",
        "identity": IDENTITY,
        "catalogName": catalog,
        "tableName": {"schemaName": schema, "tableName": table},
        "partitions": partitions_block,
        "partitionCols": [],
        "constraints": {"@type": "Constraints", "summary": {}},
        "queryId": "q",
    }
    if continuation_token is not None:
        event["continuationToken"] = continuation_token
    return event


def read_records(
    schema: str,
    table: str,
    split: dict,
    requested_schema_b64: str,
    catalog: str = "growler",
    constraints: Optional[dict] = None,
) -> dict:
    return {
        "@type": "ReadRecordsRequest",
        "identity": IDENTITY,
        "catalogName": catalog,
        "tableName": {"schemaName": schema, "tableName": table},
        "schema": requested_schema_b64,
        "split": split,
        "constraints": constraints or {"@type": "Constraints", "summary": {}},
        "maxBlockSize": 16777216,
        "maxInlineBlockSize": 5242880,
        "queryId": "q",
    }


def eq_value_set(values: list, column_type_hint: str = "string") -> dict:
    return {
        "@type": "EquatableValueSet",
        "whiteList": True,
        "nullAllowed": False,
        "valueBlock": {"rows": values},
    }


def range_value_set(
    low: Optional[object] = None,
    low_inclusive: bool = True,
    high: Optional[object] = None,
    high_inclusive: bool = True,
) -> dict:
    low_mark: dict = {}
    high_mark: dict = {}
    if low is not None:
        low_mark = {"value": low, "bound": "EXACTLY" if low_inclusive else "ABOVE"}
    if high is not None:
        high_mark = {"value": high, "bound": "EXACTLY" if high_inclusive else "BELOW"}
    return {
        "@type": "SortedRangeSet",
        "nullAllowed": False,
        "ranges": [{"low": low_mark, "high": high_mark}],
    }
