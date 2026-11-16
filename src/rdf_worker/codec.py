"""Lossless Arrow cells as RDF literals, using XSD types wherever practical."""

from __future__ import annotations

import base64
import datetime as dt
import math
import re
from typing import Any

import pyarrow as pa
from pyoxigraph import Literal, NamedNode

NS = "urn:vgi:rdf:"
XSD = "http://www.w3.org/2001/XMLSchema#"
ARROW_LITERAL = NamedNode(NS + "arrowIPC")


def encode(value: pa.Scalar[Any]) -> Literal | None:
    """Encode one cell; SQL NULL is represented by an absent property."""
    if not value.is_valid:
        return None
    data_type = value.type
    if pa.types.is_timestamp(data_type):
        lexical = value.cast(pa.string()).as_py().replace(" ", "T")
        if re.match(r"^\d{4}-\d{2}-\d{2}T", lexical) is None:
            # Arrow uses an out-of-range display string for SQL infinities and
            # extreme epochs. Preserve their physical value instead of that text.
            return _arrow_literal(value)
        # Arrow renders UTC as +0000; RDF requires an ISO offset.
        if len(lexical) >= 5 and lexical[-5] in "+-" and lexical[-4:].isdigit():
            lexical = lexical[:-2] + ":" + lexical[-2:]
        try:
            if not pa.scalar(lexical).cast(data_type).equals(value):
                return _arrow_literal(value)
        except pa.ArrowInvalid:
            # Arrow cannot parse some representable instants at its range
            # boundaries, even though formatting those instants succeeds.
            return _arrow_literal(value)
        return Literal(lexical, datatype=NamedNode(XSD + "dateTime"))
    if pa.types.is_time(data_type):
        return Literal(value.cast(pa.string()).as_py(), datatype=NamedNode(XSD + "time"))
    item: Any = None
    if not (pa.types.is_nested(data_type) or pa.types.is_interval(data_type) or pa.types.is_duration(data_type)):
        try:
            item = value.as_py()
        except OverflowError, ValueError:
            return _arrow_literal(value)
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return Literal(item)
    if pa.types.is_boolean(data_type):
        return Literal(item)
    if pa.types.is_integer(data_type):
        return Literal(str(item), datatype=NamedNode(XSD + "integer"))
    if pa.types.is_decimal(data_type):
        return Literal(format(item, "f"), datatype=NamedNode(XSD + "decimal"))
    if pa.types.is_floating(data_type):
        lexical = (
            "NaN" if math.isnan(item) else "INF" if item == math.inf else "-INF" if item == -math.inf else repr(item)
        )
        return Literal(lexical, datatype=NamedNode(XSD + "double"))
    if pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type) or pa.types.is_fixed_size_binary(data_type):
        return Literal(base64.b64encode(item).decode(), datatype=NamedNode(XSD + "base64Binary"))
    if pa.types.is_date(data_type):
        return Literal(item.isoformat(), datatype=NamedNode(XSD + "date"))
    return _arrow_literal(value)


def _arrow_literal(value: pa.Scalar[Any]) -> Literal:
    sink = pa.BufferOutputStream()
    batch = pa.record_batch([pa.array([value], type=value.type)], names=["value"])
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return Literal(base64.b64encode(sink.getvalue().to_pybytes()).decode(), datatype=ARROW_LITERAL)


def decode(term: Literal | None, data_type: pa.DataType) -> Any:
    """Decode a cell into a value accepted by an array of the declared Arrow type."""
    if term is None:
        return None
    if term.datatype == ARROW_LITERAL:
        batch = pa.ipc.open_stream(base64.b64decode(term.value, validate=True)).read_next_batch()
        if batch.num_rows != 1 or batch.num_columns != 1 or batch.column(0).type != data_type:
            raise ValueError("Corrupt RDF Arrow literal: cell type does not match the table schema")
        return batch.column(0)[0]
    value = term.value
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return value
    if pa.types.is_boolean(data_type):
        return value in {"true", "1"}
    if pa.types.is_integer(data_type):
        return int(value)
    if pa.types.is_floating(data_type):
        return float(value)
    if pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type) or pa.types.is_fixed_size_binary(data_type):
        return base64.b64decode(value, validate=True)
    if pa.types.is_date(data_type):
        return dt.date.fromisoformat(value)
    if pa.types.is_time(data_type):
        # Preserve sub-microsecond time values with an integer Arrow cast.
        hours, minutes, seconds = value.split(":")
        whole, _, fraction = seconds.partition(".")
        nanos = ((int(hours) * 60 + int(minutes)) * 60 + int(whole)) * 1_000_000_000
        nanos += int(fraction.ljust(9, "0")) if fraction else 0
        scale = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1000, "ns": 1}[data_type.unit]
        return pa.scalar(nanos // scale, type=data_type)
    return pa.scalar(value).cast(data_type)


def pack_schema(schema: pa.Schema) -> str:
    """Encode a schema including DuckDB logical type and default metadata."""
    return base64.b64encode(schema.serialize().to_pybytes()).decode()


def unpack_schema(value: str) -> pa.Schema:
    """Decode a persisted Arrow schema."""
    return pa.ipc.read_schema(pa.py_buffer(base64.b64decode(value, validate=True)))


def identifier(value: str) -> str:
    """Quote a SQL identifier without interpreting embedded dots or quotes."""
    return '"' + value.replace('"', '""') + '"'
