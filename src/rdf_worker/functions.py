"""Arrow scan/write handlers and a public RDF quad inspection function."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import BindParams, OutputCollector, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi.table_in_out_function import TableInOutGenerator
from vgi.write_results import write_options, write_result_batch, write_result_schema
from vgi_rpc import ArrowSerializableDataclass

from .config import database_path, forced_read_only
from .limits import setting
from .model import RelationalGraph
from .scans import prepare_quads, prepare_scan, read_scan
from .storage import Database

BATCH_SIZE = 2048


def database(attachment: bytes | None, *, writing: bool = False) -> Database:
    """Resolve authenticated attachment bytes to a shared database directory."""
    if attachment is None:
        raise ValueError("Attach the rdf catalog before querying tables")
    info = json.loads(attachment)
    if writing and (info["read_only"] or forced_read_only()):
        raise ValueError("RDF catalog is attached read-only")
    return Database(database_path(info["path"]))


def _result_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: value if value.is_valid else None for key, value in row.items()}


@dataclass(frozen=True, kw_only=True)
class TableArgs:
    """Internal table address, passed structurally rather than as dotted SQL."""

    schema_name: Annotated[str, Arg(0, doc="Raw SQL schema name.")]
    table_name: Annotated[str, Arg(1, doc="Raw SQL table name.")]


@dataclass(kw_only=True)
class ScanState(ArrowSerializableDataclass):
    """Serializable cursor that works across pooled processes and HTTP requests."""

    after: int = 0
    snapshot_id: str = ""


@init_single_worker
class RdfScan(TableFunctionGenerator[TableArgs, ScanState]):
    """Scan projected RDF-backed rows with one bounded evaluation per statement."""

    class Meta:
        """Internal catalog function metadata."""

        name = "rdf_scan"
        description = "Internal projected scan of an RDF-backed SQL table"
        projection_pushdown = True
        filter_pushdown = False

    @classmethod
    def on_bind(cls, params: BindParams[TableArgs]) -> BindResponse:
        """Bind the exact persisted table schema including its hidden row identity."""
        db = database(params.attach_opaque_data)
        with db.access(params.transaction_opaque_data) as snapshot:
            table = RelationalGraph(snapshot).require([params.args.schema_name], params.args.table_name)
            return BindResponse(output_schema=table.scan_schema)

    @classmethod
    def initial_state(cls, params: ProcessParams[TableArgs]) -> ScanState:
        """Start immediately before the first row identifier."""
        db = database(params.attach_opaque_data)
        if params.transaction_opaque_data is None:
            raise ValueError("Table scans require a VGI transaction")
        return ScanState(
            snapshot_id=prepare_scan(
                db,
                params.transaction_opaque_data,
                params.args.schema_name,
                params.args.table_name,
                params.output_schema,
            )
        )

    @classmethod
    def process(cls, params: ProcessParams[TableArgs], state: ScanState, out: OutputCollector) -> None:
        """Read at most one Arrow batch from the transaction's RDF snapshot."""
        db = database(params.attach_opaque_data)
        assert params.transaction_opaque_data is not None
        batch = read_scan(db, params.transaction_opaque_data, state.snapshot_id, state.after)
        if batch is not None:
            out.emit(batch)
            state.after += 1
        else:
            out.finish()


class RdfWrite(TableInOutGenerator[TableArgs, None]):
    """Shared batch write implementation; subclasses select a SQL operation."""

    operation: ClassVar[str] = "insert"

    @classmethod
    def on_bind(cls, params: BindParams[TableArgs]) -> BindResponse:
        """Choose count, returned rows, or OLD/NEW changes as requested by VGI."""
        db = database(params.attach_opaque_data, writing=True)
        with db.access(params.transaction_opaque_data) as snapshot:
            table = RelationalGraph(snapshot).require([params.args.schema_name], params.args.table_name)
            return BindResponse(output_schema=write_result_schema(write_options(params).result_mode, table.data_schema))

    @classmethod
    def process(
        cls, params: ProcessParams[TableArgs], state: None, batch: pa.RecordBatch, out: OutputCollector
    ) -> None:
        """Validate and write a batch inside the host SQL transaction."""
        db = database(params.attach_opaque_data, writing=True)
        if params.transaction_opaque_data is None:
            raise ValueError("RDF writes require a VGI transaction")
        with db.access(params.transaction_opaque_data, write=True) as snapshot:
            if batch.nbytes > setting("MAX_WRITE_BATCH_BYTES", 64 * 1024 * 1024):
                raise ValueError("RDF write batch exceeds MAX_WRITE_BATCH_BYTES")
            graph = RelationalGraph(snapshot)
            table = graph.require([params.args.schema_name], params.args.table_name)
            old, new = graph.write(table, cls.operation, batch)
            # Arrow scalars preserve precision internally; the result helper accepts them too.
            old = [_result_row(row) for row in old]
            new = [_result_row(row) for row in new]
            out.emit(write_result_batch(write_options(params).result_mode, table.data_schema, old, new))


class RdfInsert(RdfWrite):
    """INSERT handler."""

    operation = "insert"

    class Meta:
        """Internal write metadata."""

        name = "rdf_insert"
        description = "Internal transactional RDF INSERT"


class RdfUpdate(RdfWrite):
    """UPDATE handler."""

    operation = "update"

    class Meta:
        """Internal write metadata."""

        name = "rdf_update"
        description = "Internal transactional RDF UPDATE"


class RdfDelete(RdfWrite):
    """DELETE handler."""

    operation = "delete"

    class Meta:
        """Internal write metadata."""

        name = "rdf_delete"
        description = "Internal transactional RDF DELETE"


@dataclass(frozen=True, kw_only=True)
class QuadArgs:
    """Optional graph selection for RDF inspection."""

    graph_iri: Annotated[str, Arg("graph_iri", default="", doc="Named graph IRI to inspect; empty reads all graphs.")]


@dataclass(kw_only=True)
class QuadState(ArrowSerializableDataclass):
    """Position in a deterministically ordered RDF dataset snapshot."""

    offset: int = 0
    snapshot_id: str = ""


@init_single_worker
class RdfQuads(TableFunctionGenerator[QuadArgs, QuadState]):
    """Expose the actual persisted RDF terms as N-Triples-formatted strings."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field(name, pa.string(), metadata={b"comment": description.encode()})
            for name, description in [
                ("subject", "RDF subject IRI in N-Triples syntax."),
                ("predicate", "RDF predicate IRI in N-Triples syntax."),
                ("object", "RDF object term including its datatype, in N-Triples syntax."),
                ("graph", "Named graph IRI in N-Triples syntax."),
            ]
        ]
    )

    class Meta:
        """Public RDF inspection metadata."""

        name = "rdf_quads"
        description = "Inspect the persisted RDF dataset as subject, predicate, object and graph terms"
        tags = {
            "vgi.title": "RDF Quads",
            "vgi.category": "rdf",
            "vgi.keywords": json.dumps(["rdf", "triples", "quads", "graph", "inspect", "storage"]),
            "vgi.doc_llm": (
                "Inspect the attached database's actual RDF quads. The optional named graph_iri argument "
                "restricts inspection to one named graph. Returns "
                "subject, predicate, object and graph as `VARCHAR` N-Triples terms. Includes the catalog"
                " graph and each table's row graph. SQL NULL cells have no triple; rowId triples "
                "distinguish duplicate and all-NULL rows. Use SQL filters and aggregation to inspect "
                "graphs. Read-only and transaction-consistent."
            ),
            "vgi.doc_md": (
                "## rdf_quads()\n\nUse `graph_iri := 'urn:vgi:rdf:catalog'` to select one graph, "
                "or omit it to read all graphs. Read the actual RDF storage as four `VARCHAR` columns: `subject`, "
                "`predicate`, `object`, `graph`. Terms use N-Triples syntax and literals retain "
                "datatype annotations. Catalog metadata lives in `<urn:vgi:rdf:catalog>`; table rows "
                "live in separate named graphs. NULL cells omit their property triple. This function "
                "does not mutate data."
            ),
            "vgi.result_columns_schema": json.dumps(
                [
                    {"name": name, "type": "VARCHAR", "description": desc}
                    for name, desc in [
                        ("subject", "RDF subject term."),
                        ("predicate", "RDF predicate term."),
                        ("object", "RDF object term with datatype."),
                        ("graph", "Named graph term."),
                    ]
                ]
            ),
            "vgi.example_queries": json.dumps(
                [
                    {
                        "description": "Count the main schema resource",
                        "sql": (
                            "SELECT count(*) AS main_schemas FROM rdf.main.rdf_quads() WHERE predicate = "
                            "'<urn:vgi:rdf:schemaName>' AND object = '\"main\"'"
                        ),
                    }
                ]
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "The main schema is stored in RDF",
                        "sql": (
                            "SELECT count(*) AS main_schemas FROM rdf.main.rdf_quads() WHERE predicate = "
                            "'<urn:vgi:rdf:schemaName>' AND object = '\"main\"'"
                        ),
                        "expected_result": [[1]],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT graph, count(*) AS triples FROM rdf.main.rdf_quads() GROUP BY graph ORDER BY graph",
                description="Count stored RDF statements by named graph",
            ),
            FunctionExample(
                sql="SELECT predicate, count(*) AS statements FROM "
                "rdf.main.rdf_quads(graph_iri := 'urn:vgi:rdf:catalog') GROUP BY predicate ORDER BY predicate",
                description="Count metadata statements by predicate",
            ),
            FunctionExample(
                sql="SELECT count(*) AS schemas FROM rdf.main.rdf_quads() WHERE predicate = '<urn:vgi:rdf:schemaName>'",
                description="Count RDF schema resources",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[QuadArgs]) -> BindResponse:
        """Bind the four RDF term columns."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[QuadArgs]) -> QuadState:
        """Begin at the first quad in the snapshot."""
        if params.transaction_opaque_data is None:
            raise ValueError("RDF inspection requires a VGI transaction")
        db = database(params.attach_opaque_data)
        return QuadState(
            snapshot_id=prepare_quads(db, params.transaction_opaque_data, params.args.graph_iri, params.output_schema)
        )

    @classmethod
    def process(cls, params: ProcessParams[QuadArgs], state: QuadState, out: OutputCollector) -> None:
        """Emit a bounded page; RDF term objects never escape the store context."""
        db = database(params.attach_opaque_data)
        assert params.transaction_opaque_data is not None
        batch = read_scan(db, params.transaction_opaque_data, state.snapshot_id, state.offset)
        if batch is None:
            out.finish()
            return
        state.offset += 1
        out.emit(batch)


INTERNAL_FUNCTIONS: list[type[Any]] = [RdfScan, RdfInsert, RdfUpdate, RdfDelete]

# Match the SDK's complete example list so each discovered query has a description.
RdfQuads.Meta.tags["vgi.example_queries"] = json.dumps(
    [{"sql": example.sql, "description": example.description} for example in RdfQuads.Meta.examples]
)
