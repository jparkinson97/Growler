import pyarrow as pa

from growler.federation.serde import (
    decode_batch,
    decode_block,
    decode_schema,
    encode_batch,
    encode_block,
    encode_schema,
    partitions_block_from_dicts,
    partitions_from_block,
)


def test_schema_roundtrip():
    s = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("user", pa.struct([pa.field("name", pa.string())])),
        ]
    )
    b64 = encode_schema(s)
    assert isinstance(b64, str)
    assert decode_schema(b64) == s


def test_batch_roundtrip():
    s = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
    batch = pa.record_batch([pa.array([1, 2]), pa.array(["x", "y"])], schema=s)
    b64 = encode_batch(batch)
    back = decode_batch(b64, s)
    assert back.equals(batch)


def test_block_roundtrip():
    s = pa.schema([pa.field("a", pa.int64())])
    batch = pa.record_batch([pa.array([1, 2, 3])], schema=s)
    block = encode_block(s, batch)
    assert block["aId"]
    s2, b2 = decode_block(block)
    assert s2 == s
    assert b2.equals(batch)


def test_empty_block():
    s = pa.schema([pa.field("a", pa.int64())])
    block = encode_block(s, None)
    s2, b2 = decode_block(block)
    assert s2 == s
    assert b2.num_rows == 0


def test_partitions_block_no_columns_preserves_row_count():
    # Non-partitioned tables produce one implicit partition row with zero columns.
    block = partitions_block_from_dicts([], [{}])
    from growler.federation.serde import decode_block

    _, batch = decode_block(block)
    assert batch.num_rows == 1
    assert batch.num_columns == 0


def test_partitions_block_roundtrip():
    cols = [("event_date", pa.string()), ("region", pa.string())]
    rows = [
        {"event_date": "2026-04-20", "region": "us"},
        {"event_date": "2026-04-20", "region": "eu"},
        {"event_date": "2026-04-21", "region": "us"},
    ]
    block = partitions_block_from_dicts(cols, rows)
    out = partitions_from_block(block)
    assert out == rows
