"""Read-only, bounded SPARQL execution with lossless RDF term columns."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyoxigraph import BlankNode, Literal, NamedNode, Triple

from .codec import pack_schema, unpack_schema
from .limits import query_limits, setting

TERM_TYPE = pa.struct([(n, pa.string()) for n in ("kind", "value", "datatype", "language")])

# Skip only complete SPARQL lexical strings, IRIREFs and comments. Oxigraph
# subsequently parses the entire query. Reject escapes elsewhere rather than
# letting escaped keyword spellings bypass the network prohibition.
_LEXEMES = re.compile(
    r'''\#[^\r\n]*|"""(?:[^"\\]|\\[\s\S]|"(?!""))*"""|'''
    r"'''(?:[^'\\]|\\[\s\S]|'(?!''))*'''|"
    r""""(?:[^"\\\r\n]|\\[^\r\n])*"|'(?:[^'\\\r\n]|\\[^\r\n])*'|"""
    r"""<(?:[^\x00-\x20<>"{}|^`\\]|\\u[0-9A-Fa-f]{4}|\\U[0-9A-Fa-f]{8})*>|"""
    r"(?P<word>[A-Za-z_][A-Za-z_0-9]*)|(?P<escape>\\)|(?P<other>[\s\S])"
)


def validate_query(query: str) -> None:
    """Reject network federation and oversized query text before any execution."""
    if len(query.encode()) > setting("MAX_QUERY_BYTES", 262_144):
        raise ValueError("SPARQL query exceeds MAX_QUERY_BYTES")
    for token in _LEXEMES.finditer(query):
        if token.lastgroup == "escape":
            raise ValueError("SPARQL escapes are allowed only inside RDF strings and IRIs")
        if token.lastgroup == "word" and token.group().upper() == "SERVICE":
            raise ValueError("SPARQL SERVICE is disabled; queries cannot access remote endpoints")


def term_value(term: Any) -> dict[str, str | None] | None:
    """Keep IRIs, blank nodes, literal datatypes and language tags distinct."""
    if term is None:
        return None
    result: dict[str, str | None] = {"kind": None, "value": None, "datatype": None, "language": None}
    if isinstance(term, NamedNode):
        result.update(kind="iri", value=term.value)
    elif isinstance(term, BlankNode):
        result.update(kind="blank", value=term.value)
    elif isinstance(term, Literal):
        result.update(kind="literal", value=term.value, datatype=term.datatype.value, language=term.language)
    elif isinstance(term, Triple):
        result.update(kind="triple", value=str(term))
    else:
        raise ValueError("Unsupported RDF term")
    return result


def call_runner(request: dict[str, Any], *, timeout_ms: int) -> dict[str, Any]:
    """Run query work in a disposable process so deadlines can terminate evaluation."""
    environment = {k: v for k, v in os.environ.items() if k not in {"VGI_BEARER_TOKENS", "VGI_SIGNING_KEY"}}
    try:
        result = subprocess.run(
            [sys.executable, "-m", "rdf_worker.query_runner"],
            input=json.dumps({**request, "_deadline_ms": timeout_ms}),
            text=True,
            capture_output=True,
            timeout=timeout_ms / 1000,
            env=environment,
        )
    except subprocess.TimeoutExpired as error:
        raise TimeoutError(f"RDF query exceeded its {timeout_ms} ms deadline") from error
    if result.returncode != 0:
        raise ValueError("RDF query failed: " + result.stderr[-2048:].strip())
    return dict(json.loads(result.stdout))


def describe_query(query: str, timeout_ms: int = 30_000) -> pa.Schema:
    """Bind SELECT variables, ASK boolean, or CONSTRUCT/DESCRIBE triple columns."""
    validate_query(query)
    result = call_runner({"query": query, "describe": True}, timeout_ms=timeout_ms)
    return unpack_schema(result["schema"])


def execute_query(
    path: Path, query: str, schema: pa.Schema, *, max_rows: int, timeout_ms: int, union_default: bool
) -> Path:
    """Materialize one immutable query result, bounded in rows, bytes and wall time."""
    validate_query(query)
    query_limits(max_rows, timeout_ms)
    output = path.parent / (path.name + ".arrow")
    try:
        call_runner(
            {
                "path": str(path),
                "query": query,
                "schema": pack_schema(schema),
                "output": str(output),
                "max_rows": max_rows,
                "max_bytes": setting("MAX_RESULT_BYTES", 64 * 1024 * 1024),
                "union_default": union_default,
            },
            timeout_ms=timeout_ms,
        )
    except BaseException:
        output.unlink(missing_ok=True)
        raise
    return output
