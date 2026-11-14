"""Single-evaluation relational scans streamed through bounded Arrow spool files."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pyarrow as pa

from .codec import pack_schema, unpack_schema
from .limits import setting
from .sparql import call_runner
from .storage import Database, Snapshot


def prepare_scan(
    db: Database,
    token: bytes,
    schema_name: str,
    table_name: str,
    output_schema: pa.Schema,
    *,
    include_iri: bool = False,
) -> str:
    """Freeze and evaluate a scan once, bounding temporary storage and execution time."""
    name = db.freeze_scan(token)
    with db.scan_access(token, name) as snapshot:
        output = snapshot.path.parent / (name + ".arrow")
        try:
            call_runner(
                {
                    "operation": "table_scan",
                    "path": str(snapshot.path),
                    "output": str(output),
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "schema": pack_schema(output_schema),
                    "include_iri": include_iri,
                    "max_rows": setting("MAX_SCAN_ROWS", 10_000_000),
                    "max_bytes": setting("MAX_SCAN_BYTES", 1024 * 1024 * 1024),
                },
                timeout_ms=setting("SCAN_TIMEOUT_MS", 120_000),
            )
        except BaseException:
            output.unlink(missing_ok=True)
            raise
    return name


def read_scan(db: Database, token: bytes, name: str, index: int) -> pa.RecordBatch | None:
    """Read an already computed Arrow batch under the transaction's lifetime lock."""
    with (
        db.scan_access(token, name) as snapshot,
        pa.memory_map(str(snapshot.path.parent / (name + ".arrow")), "r") as source,
    ):
        reader = pa.ipc.open_file(source)
        return reader.get_batch(index) if index < reader.num_record_batches else None


def prepare_quads(db: Database, token: bytes, graph_iri: str, schema: pa.Schema) -> str:
    """Materialize quad inspection once with the same limits as relational scans."""
    name = db.freeze_scan(token)
    with db.scan_access(token, name) as snapshot:
        output = snapshot.path.parent / (name + ".arrow")
        try:
            call_runner(
                {
                    "operation": "quads",
                    "path": str(snapshot.path),
                    "output": str(output),
                    "graph_iri": graph_iri,
                    "schema": pack_schema(schema),
                    "max_rows": setting("MAX_SCAN_ROWS", 10_000_000),
                    "max_bytes": setting("MAX_SCAN_BYTES", 1024 * 1024 * 1024),
                },
                timeout_ms=setting("SCAN_TIMEOUT_MS", 120_000),
            )
        except BaseException:
            output.unlink(missing_ok=True)
            raise
    return name


def run_scan(request: dict[str, Any]) -> dict[str, Any]:
    """Materialize a typed scan inside the deadline-controlled query subprocess."""
    from .model import RelationalGraph

    schema = unpack_schema(request["schema"])
    snapshot = Snapshot(Path(request["path"]), writable=False)
    count = 0
    try:
        graph = RelationalGraph(snapshot)
        table = graph.require([request["schema_name"]], request["table_name"])
        source_schema = table.scan_schema if request["include_iri"] else schema
        with open(request["output"], "xb") as output, pa.ipc.new_file(output, schema) as writer:
            for batch, _ in graph.scan_batches(table, source_schema):
                count += batch.num_rows
                if count > request["max_rows"]:
                    raise ValueError("RDF table scan exceeds MAX_SCAN_ROWS")
                if request["include_iri"]:
                    iris = pa.array([table.row_node(i.as_py()).value for i in batch.column(table.rowid_name)])
                    batch = pa.RecordBatch.from_arrays([*batch.columns[:-1], iris], schema=schema)
                writer.write_batch(batch)
                if output.tell() > request["max_bytes"]:
                    raise ValueError("RDF table scan exceeds MAX_SCAN_BYTES")
        if os.path.getsize(request["output"]) > request["max_bytes"]:
            raise ValueError("RDF table scan exceeds MAX_SCAN_BYTES")
    finally:
        snapshot.close()
    return {"rows": count}
