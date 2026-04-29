from __future__ import annotations

import json
import os
from typing import Any

from growler.federation.metadata_lambda import MetadataFederationLambda
from growler.federation.record_lambda import RecordFederationLambda
from growler.federation.wire import error_envelope, req_type
from growler.metadata_handler import MetadataHandler
from growler.record_handler import RecordHandler
from growler.storage import S3PointerStore


def _build() -> tuple[MetadataFederationLambda, RecordFederationLambda]:
    bucket = os.environ["GROWLER_BUCKET"]
    catalog_name = os.environ.get("GROWLER_CATALOG", "growler")
    catalog = json.loads(os.environ.get("GROWLER_CATALOG_JSON", "{}"))
    store = S3PointerStore(bucket=bucket)
    metadata = MetadataFederationLambda(
        metadata=MetadataHandler(store=store, catalog=catalog),
        catalog_name=catalog_name,
    )
    record = RecordFederationLambda(
        record=RecordHandler(store=store),
        catalog_name=catalog_name,
    )
    return metadata, record


_metadata, _record = _build()


def lambda_handler(event: dict, context: Any = None) -> dict:
    rt = req_type(event)
    if rt == "ReadRecordsRequest":
        return _record.lambda_handler(event, context)
    if rt in {
        "PingRequest",
        "ListSchemasRequest",
        "ListTablesRequest",
        "GetTableRequest",
        "GetTableLayoutRequest",
        "GetSplitsRequest",
    }:
        return _metadata.lambda_handler(event, context)
    return error_envelope(f"Unknown request type: {rt}", "UnsupportedOperationException")
