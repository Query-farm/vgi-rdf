from __future__ import annotations

import datetime as dt
import math
from decimal import Decimal
from typing import Any, Literal

import pyarrow as pa
import pytest

from rdf_worker.codec import ARROW_LITERAL, decode, encode


@pytest.mark.parametrize(
    ("data_type", "value"),
    [
        (pa.int8(), -128),
        (pa.int16(), 32767),
        (pa.int32(), -2147483648),
        (pa.int64(), -9223372036854775808),
        (pa.uint64(), 18446744073709551615),
        (pa.bool_(), True),
        (pa.bool_(), False),
        (pa.float32(), 1.25),
        (pa.float64(), -0.0),
        (pa.float64(), float("inf")),
        (pa.float64(), float("-inf")),
        (pa.string(), 'quote " slash \\ newline\n \0 snowman ☃'),
        (pa.string(), ""),
        (pa.binary(), b"\x00\xff"),
        (pa.binary(3), b"abc"),
        (pa.decimal128(38, 10), Decimal("1234567890123456789012345678.1234567890")),
        (pa.date32(), dt.date(2020, 2, 29)),
        (pa.timestamp("us"), 1234567890123456),
        (pa.timestamp("ns"), 1234567890123456789),
        (pa.timestamp("ns", "UTC"), 1234567890123456789),
        (pa.time64("ns"), 123456789),
        (pa.time64("us"), 86399999999),
        (pa.duration("ns"), 123456789),
        (pa.month_day_nano_interval(), (3, 2, 123456789)),
        (pa.list_(pa.int64()), [1, None, 3]),
        (pa.list_(pa.string()), []),
        (pa.struct([("name", pa.string()), ("values", pa.list_(pa.int64()))]), {"name": "a", "values": [None, 2]}),
        (pa.map_(pa.string(), pa.int64()), [("a", 1), ("b", None)]),
    ],
)
def test_round_trip(data_type: pa.DataType, value: Any) -> None:
    original = pa.scalar(value, type=data_type)
    term = encode(original)
    result = pa.array([decode(term, data_type)], type=data_type)[0]
    assert result.equals(original)
    if pa.types.is_floating(data_type) and value == 0:
        assert math.copysign(1, result.as_py()) == math.copysign(1, value)


@pytest.mark.parametrize("data_type", [pa.string(), pa.int64(), pa.timestamp("ns"), pa.list_(pa.int64())])
def test_null_omits_property(data_type: pa.DataType) -> None:
    assert encode(pa.scalar(None, type=data_type)) is None
    assert decode(None, data_type) is None


def test_nan_round_trip() -> None:
    assert math.isnan(decode(encode(pa.scalar(float("nan"))), pa.float64()))


def test_nested_nanosecond_precision() -> None:
    data_type = pa.struct([("when", pa.timestamp("ns"))])
    original = pa.array([{"when": 1234567890123456789}], type=data_type)[0]
    term = encode(original)
    assert term is not None and term.datatype == ARROW_LITERAL
    assert decode(term, data_type).equals(original)


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
@pytest.mark.parametrize("value", [2**63 - 1, -(2**63) + 1])
def test_extreme_timestamp_values(unit: Literal["s", "ms", "us", "ns"], value: int) -> None:
    data_type = pa.timestamp(unit)
    original = pa.scalar(value, type=data_type)
    assert pa.array([decode(encode(original), data_type)], type=data_type)[0].equals(original)
