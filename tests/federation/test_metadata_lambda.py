from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from growler import MetadataHandler
from growler.federation import MetadataFederationLambda
from growler.federation.serde import decode_schema, partitions_from_block
from tests.federation import fixtures as fx


def _setup(handler, tmp_parquet_dir: Path) -> MetadataFederationLambda:
    handler.create_table(
        "default/events",
        [
            {"path": "id", "type": "int64", "nullable": False},
            {"path": "region", "type": "string", "nullable": False},
            {"path": "event_date", "type": "date", "nullable": False},
        ],
        partition_paths=["event_date", "region"],
    )
    for date, region, name in [
        ("2026-04-20", "us", "a"),
        ("2026-04-20", "eu", "b"),
        ("2026-04-21", "us", "c"),
    ]:
        p = tmp_parquet_dir / f"{name}.parquet"
        pq.write_table(
            pa.table(
                {
                    "id": pa.array([1], type=pa.int64()),
                    "region": [region],
                    "event_date": pa.array([date], type=pa.date32().__class__()) if False else pa.array([date]).cast(pa.date32(), safe=False),
                }
            ),
            p,
        )
        handler.add_files(
            "default/events",
            [{"s3_uri": str(p), "partition_values": {"event_date": date, "region": region}}],
        )
    md = MetadataHandler(handler.store, catalog={"default": ["events"]})
    return MetadataFederationLambda(md)


def test_ping(handler, tmp_parquet_dir):
    lam = _setup(handler, tmp_parquet_dir)
    resp = lam.handle(fx.ping())
    assert resp["@type"] == "PingResponse"
    assert resp["sourceType"] == "growler"


def test_list_schemas(handler, tmp_parquet_dir):
    lam = _setup(handler, tmp_parquet_dir)
    resp = lam.handle(fx.list_schemas())
    assert resp["@type"] == "ListSchemasResponse"
    assert resp["schemas"] == ["default"]


def test_list_tables(handler, tmp_parquet_dir):
    lam = _setup(handler, tmp_parquet_dir)
    resp = lam.handle(fx.list_tables("default"))
    assert resp["@type"] == "ListTablesResponse"
    names = [t["tableName"] for t in resp["tables"]]
    assert names == ["events"]


def test_get_table_returns_arrow_schema(handler, tmp_parquet_dir):
    lam = _setup(handler, tmp_parquet_dir)
    resp = lam.handle(fx.get_table("default", "events"))
    assert resp["@type"] == "GetTableResponse"
    assert resp["partitionColumns"] == ["event_date", "region"]
    schema = decode_schema(resp["schema"])
    assert "id" in schema.names
    assert schema.field("id").type == pa.int64()


def test_get_table_layout_returns_partitions_block(handler, tmp_parquet_dir):
    lam = _setup(handler, tmp_parquet_dir)
    resp = lam.handle(fx.get_table_layout("default", "events"))
    assert resp["@type"] == "GetTableLayoutResponse"
    rows = partitions_from_block(resp["partitions"])
    dates = {r["event_date"] for r in rows}
    regions = {r["region"] for r in rows}
    assert dates == {"2026-04-20", "2026-04-21"}
    assert regions == {"us", "eu"}


def test_get_table_layout_with_equatable_constraint(handler, tmp_parquet_dir):
    lam = _setup(handler, tmp_parquet_dir)
    event = fx.get_table_layout(
        "default",
        "events",
        constraints={
            "@type": "Constraints",
            "summary": {"event_date": fx.eq_value_set(["2026-04-20"])},
        },
    )
    resp = lam.handle(event)
    rows = partitions_from_block(resp["partitions"])
    assert all(r["event_date"] == "2026-04-20" for r in rows)
    assert len(rows) == 2


def test_get_splits_from_layout(handler, tmp_parquet_dir):
    lam = _setup(handler, tmp_parquet_dir)
    layout = lam.handle(fx.get_table_layout("default", "events"))
    resp = lam.handle(fx.get_splits("default", "events", layout["partitions"]))
    assert resp["@type"] == "GetSplitsResponse"
    assert len(resp["splits"]) == 3
    for s in resp["splits"]:
        assert "s3_uri" in s["properties"]
        assert s["properties"]["table"] == "default/events"


def test_non_partitioned_table_gets_synthetic_partition(handler, tmp_parquet_dir):
    from growler.federation.serde import partitions_from_block
    from growler.federation.synthetic_partition import SYNTHETIC_COLUMN, SYNTHETIC_VALUE

    handler.create_table(
        "default/nopart",
        [{"path": "id", "type": "int64", "nullable": False}],
        partition_paths=[],
    )
    md = MetadataHandler(handler.store, catalog={"default": ["nopart"]})
    lam = MetadataFederationLambda(md)

    get_table = lam.handle(fx.get_table("default", "nopart"))
    assert get_table["partitionColumns"] == [SYNTHETIC_COLUMN]

    layout = lam.handle(fx.get_table_layout("default", "nopart"))
    rows = partitions_from_block(layout["partitions"])
    assert rows == [{SYNTHETIC_COLUMN: SYNTHETIC_VALUE}]


def test_table_not_found(handler, tmp_parquet_dir):
    lam = _setup(handler, tmp_parquet_dir)
    resp = lam.handle(fx.get_table("default", "nope"))
    assert resp["@type"] == "FederationException"
    assert resp["errorCode"] == "TableNotFoundException"
