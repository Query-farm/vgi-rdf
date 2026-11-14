"""Disposable query subprocess; stdout carries only its small JSON response."""

from __future__ import annotations

import json
import os
import sys
import threading
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
from pyoxigraph import NamedNode, QueryBoolean, QuerySolutions, Store

from .codec import pack_schema, unpack_schema
from .sparql import TERM_TYPE, term_value, validate_query
from .traversal import EDGE_SCHEMA, REACH_SCHEMA, traverse


def result_schema(result: Any) -> pa.Schema:
    """Describe an Oxigraph query without coercing RDF terms into SQL primitives."""
    if isinstance(result, QueryBoolean):
        return pa.schema([pa.field("answer", pa.bool_(), metadata={b"comment": b"Whether the ASK pattern matches."})])
    names = (
        [v.value for v in result.variables]
        if isinstance(result, QuerySolutions)
        else ["subject", "predicate", "object"]
    )
    if len({n.lower() for n in names}) != len(names):
        raise ValueError("SPARQL result variables must be distinct under SQL case folding")
    return pa.schema(
        [pa.field(n, TERM_TYPE, metadata={b"comment": b"RDF term: kind, value, datatype, language."}) for n in names]
    )


def result_rows(result: Any, schema: pa.Schema) -> Iterator[dict[str, Any]]:
    """Translate the result lazily, preserving unbound variables as SQL NULL."""
    if isinstance(result, QueryBoolean):
        yield {"answer": bool(result)}
    elif isinstance(result, QuerySolutions):
        for row in result:
            yield {name: term_value(row[name]) for name in schema.names}
    else:
        for triple in result:
            yield {
                "subject": term_value(triple.subject),
                "predicate": term_value(triple.predicate),
                "object": term_value(triple.object),
            }


def run(request: dict[str, Any]) -> dict[str, Any]:
    """Execute against an immutable snapshot and write bounded Arrow record batches."""
    if request.get("operation") == "table_scan":
        from .scans import run_scan

        return run_scan(request)
    store = Store() if request.get("describe") else Store.read_only(request["path"])
    iterator: Iterator[dict[str, Any]]
    if request.get("operation") == "quads":
        schema = unpack_schema(request["schema"])
        graph_filter = f"VALUES ?g {{ {NamedNode(request['graph_iri'])} }} " if request["graph_iri"] else ""
        result = store.query(
            "SELECT ?s ?p ?o ?g WHERE { " + graph_filter + "GRAPH ?g { ?s ?p ?o } } ORDER BY ?g ?s ?p ?o"
        )
        assert isinstance(result, QuerySolutions)
        iterator = (
            {name: str(row[key]) for name, key in zip(schema.names, ["s", "p", "o", "g"], strict=True)}
            for row in result
        )
    elif request.get("operation") in {"neighbors", "reachable"}:
        schema = EDGE_SCHEMA if request["operation"] == "neighbors" else REACH_SCHEMA
        iterator = traverse(store, request["options"], request["operation"], request["max_rows"])
    else:
        validate_query(request["query"])
        result = store.query(request["query"], use_default_graph_as_union=request.get("union_default", False))
        schema = result_schema(result)
        iterator = result_rows(result, schema)
    if request.get("describe"):
        return {"schema": pack_schema(schema)}
    expected = unpack_schema(request["schema"])
    if schema != expected:
        raise ValueError("SPARQL result schema changed after binding")
    count = 0
    with open(request["output"], "xb") as output, pa.ipc.new_file(output, schema) as writer:
        rows = []
        for row in iterator:
            count += 1
            if count > request["max_rows"]:
                raise ValueError("RDF result exceeds max_rows; narrow the query or increase the permitted limit")
            rows.append(row)
            if len(rows) == 1024:
                writer.write_batch(pa.RecordBatch.from_pylist(rows, schema=schema))
                rows.clear()
                if output.tell() > request["max_bytes"]:
                    raise ValueError("RDF result exceeds MAX_RESULT_BYTES")
        if rows:
            writer.write_batch(pa.RecordBatch.from_pylist(rows, schema=schema))
    if os.path.getsize(request["output"]) > request["max_bytes"]:
        raise ValueError("RDF result exceeds MAX_RESULT_BYTES")
    return {"rows": count}


def main() -> None:
    """Read one internal request, returning a concise error on failure."""
    watchdog: threading.Timer | None = None
    try:
        request = json.load(sys.stdin)
        if "_deadline_ms" in request:
            # This deadline survives death of the parent worker, which would
            # otherwise leave a cancelled native query running as an orphan.
            watchdog = threading.Timer(max(1, int(request["_deadline_ms"])) / 1000, lambda: os._exit(124))
            watchdog.daemon = True
            watchdog.start()
        print(json.dumps(run(request)))
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from None
    finally:
        if watchdog is not None:
            watchdog.cancel()


if __name__ == "__main__":
    main()
