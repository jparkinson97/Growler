import pyarrow as pa
import pytest

from growler.errors import InvalidType
from growler.federation.serde import decode_schema, encode_schema
from growler.types import is_valid_type, to_arrow, type_matches, validate_type


ALL_PRIMITIVES = [
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
]


def test_primitives_valid():
    for t in ALL_PRIMITIVES:
        assert is_valid_type(t)


def test_decimal_valid():
    assert is_valid_type("decimal(10,2)")
    assert is_valid_type("decimal(38, 0)")


def test_list_valid():
    assert is_valid_type("list<int64>")
    assert is_valid_type("list<string>")
    assert is_valid_type("list<struct>")


def test_invalid():
    assert not is_valid_type("uint64")
    assert not is_valid_type("timestamp")
    with pytest.raises(InvalidType):
        validate_type("bogus")


def test_to_arrow_primitives():
    assert to_arrow("int64") == pa.int64()
    assert to_arrow("string") == pa.string()
    assert to_arrow("timestamp_ms") == pa.date64()
    assert to_arrow("timestamp_us") == pa.date64()
    assert to_arrow("timestamp_ns") == pa.date64()


def test_to_arrow_decimal():
    assert to_arrow("decimal(10,2)") == pa.decimal128(10, 2)


def test_to_arrow_list():
    assert to_arrow("list<int64>") == pa.list_(pa.int64())


def test_type_matches_primitives():
    assert type_matches(pa.int64(), "int64")
    assert not type_matches(pa.int32(), "int64")
    assert type_matches(pa.string(), "string")
    assert type_matches(pa.large_string(), "string")


def test_type_matches_timestamp_unit():
    assert type_matches(pa.timestamp("us", tz="UTC"), "timestamp_us")
    assert not type_matches(pa.timestamp("ns", tz="UTC"), "timestamp_us")
    assert type_matches(pa.timestamp("ms"), "timestamp_ms")


def test_type_matches_decimal():
    assert type_matches(pa.decimal128(10, 2), "decimal(10,2)")
    assert not type_matches(pa.decimal128(10, 3), "decimal(10,2)")


def test_type_matches_list():
    assert type_matches(pa.list_(pa.int64()), "list<int64>")
    assert type_matches(pa.list_(pa.struct([pa.field("x", pa.int64())])), "list<struct>")


def test_map_valid():
    assert is_valid_type("map<string, string>")
    assert is_valid_type("map<string, int64>")
    assert is_valid_type("map<string, struct>")
    assert is_valid_type("map<int32, list<int32>>")


def test_to_arrow_map():
    assert to_arrow("map<string, string>") == pa.map_(pa.string(), pa.string())
    assert to_arrow("map<string, int64>") == pa.map_(pa.string(), pa.int64())


def test_type_matches_map():
    assert type_matches(pa.map_(pa.string(), pa.string()), "map<string, string>")
    assert not type_matches(pa.map_(pa.int32(), pa.string()), "map<string, string>")
    assert type_matches(
        pa.map_(pa.string(), pa.struct([pa.field("x", pa.int64())])),
        "map<string, struct>",
    )


ATHENA_FEDERATION_SUPPORTED = {
    (pa.bool_(),),
    (pa.int8(),),
    (pa.int16(),),
    (pa.int32(),),
    (pa.int64(),),
    (pa.float32(),),
    (pa.float64(),),
    (pa.string(),),
    (pa.binary(),),
    (pa.date32(),),
    (pa.date64(),),
}


def _is_athena_supported(t: pa.DataType) -> bool:
    if pa.types.is_decimal(t):
        return True
    if pa.types.is_list(t):
        return _is_athena_supported(t.value_type)
    if pa.types.is_struct(t):
        for f in t:
            if not _is_athena_supported(f.type):
                return False
        return True
    return (t,) in ATHENA_FEDERATION_SUPPORTED


@pytest.mark.parametrize("grammar_type", ALL_PRIMITIVES)
def test_to_arrow_primitive_is_athena_compatible(grammar_type):
    t = to_arrow(grammar_type)
    assert _is_athena_supported(t), (
        f"{grammar_type} → {t} is not in Athena Federation's supported Arrow type set"
    )


@pytest.mark.parametrize(
    "grammar_type",
    ["decimal(10,2)", "decimal(38,0)", "list<int64>", "list<string>", "list<struct>", "struct"],
)
def test_to_arrow_complex_is_athena_compatible(grammar_type):
    t = to_arrow(grammar_type)
    assert _is_athena_supported(t)


@pytest.mark.parametrize("grammar_type", ALL_PRIMITIVES + ["decimal(10,2)", "list<int64>"])
def test_schema_roundtrips_for_all_types(grammar_type):
    t = to_arrow(grammar_type)
    schema = pa.schema([pa.field("c", t)])
    b64 = encode_schema(schema)
    decoded = decode_schema(b64)
    assert decoded.field("c").type == t
