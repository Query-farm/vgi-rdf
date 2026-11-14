"""Public graph queries and SQL-to-RDF identity discovery."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import BindParams, OutputCollector, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi_rpc import ArrowSerializableDataclass

from .codec import pack_schema
from .functions import RdfScan, ScanState, TableArgs, database
from .limits import query_limits, setting
from .model import RelationalGraph, Table, name_key
from .scans import prepare_scan
from .sparql import call_runner, describe_query, execute_query
from .traversal import EDGE_SCHEMA, REACH_SCHEMA, validate_traversal


@dataclass(frozen=True, kw_only=True)
class SparqlArgs:
    """A local read-only SPARQL query and its execution bounds."""

    query: Annotated[str, Arg(0, doc="Read-only SPARQL query text. Remote federation is disabled.")]
    max_rows: Annotated[
        int, Arg("max_rows", default=10000, ge=1, doc="Maximum result rows; exceeding it raises an error.")
    ]
    timeout_ms: Annotated[
        int, Arg("timeout_ms", default=30000, ge=1, doc="Wall-clock deadline including query subprocess startup.")
    ]
    union_default: Annotated[
        bool, Arg("union_default", default=False, doc="Use the union of stored graphs as the default graph.")
    ]


@dataclass(kw_only=True)
class QueryState(ArrowSerializableDataclass):
    """A durable result cursor; only its identifier and batch position cross RPCs."""

    snapshot_id: str = ""
    batch_index: int = 0


def read_spool(params: ProcessParams[Any], state: QueryState, out: OutputCollector) -> None:
    """Read one Arrow batch from a frozen graph result under its transaction lock."""
    assert params.transaction_opaque_data is not None
    db = database(params.attach_opaque_data)
    with (
        db.scan_access(params.transaction_opaque_data, state.snapshot_id) as snapshot,
        pa.memory_map(str(snapshot.path.parent / (state.snapshot_id + ".arrow")), "r") as source,
    ):
        reader = pa.ipc.open_file(source)
        if state.batch_index >= reader.num_record_batches:
            out.finish()
            return
        out.emit(reader.get_batch(state.batch_index))
        state.batch_index += 1


@init_single_worker
class RdfSparql(TableFunctionGenerator[SparqlArgs, QueryState]):
    """Execute read-only SPARQL with a hard deadline in an isolated subprocess."""

    class Meta:
        """Public query metadata."""

        name = "rdf_sparql"
        tags: ClassVar[dict[str, str]] = {}
        description = "Query RDF with bounded local SPARQL and join the results with SQL tables"
        examples = [
            FunctionExample(
                sql="SELECT answer FROM rdf.main.rdf_sparql('ASK { GRAPH <urn:vgi:rdf:catalog> { ?s ?p ?o } }')",
                description="Check whether the RDF catalog contains statements",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[SparqlArgs]) -> BindResponse:
        """Describe the result without reading or modifying application data."""
        query_limits(params.args.max_rows, params.args.timeout_ms)
        return BindResponse(output_schema=describe_query(params.args.query, params.args.timeout_ms))

    @classmethod
    def initial_state(cls, params: ProcessParams[SparqlArgs]) -> QueryState:
        """Freeze and evaluate once, preserving ordering and nondeterministic expressions."""
        if params.transaction_opaque_data is None:
            raise ValueError("RDF graph queries require a VGI transaction")
        db = database(params.attach_opaque_data)
        name = db.freeze_scan(params.transaction_opaque_data)
        with db.scan_access(params.transaction_opaque_data, name) as snapshot:
            execute_query(
                snapshot.path,
                params.args.query,
                params.output_schema,
                max_rows=params.args.max_rows,
                timeout_ms=params.args.timeout_ms,
                union_default=params.args.union_default,
            )
        return QueryState(snapshot_id=name)

    @classmethod
    def process(cls, params: ProcessParams[SparqlArgs], state: QueryState, out: OutputCollector) -> None:
        """Read one Arrow batch from the immutable, already bounded result."""
        read_spool(params, state, out)


@dataclass(frozen=True, kw_only=True)
class MappingArgs:
    """Optional schema selection for catalog identity discovery."""

    schema_name: Annotated[
        str, Arg("schema_name", default="", doc="SQL schema to inspect; empty includes all schemas.")
    ]


@init_single_worker
class RdfTables(TableFunctionGenerator[MappingArgs, ScanState]):
    """Discover stable table, row graph, column and relationship IRIs."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field(n, pa.string(), metadata={b"comment": d.encode()})
            for n, d in [
                ("schema_name", "SQL schema containing the table."),
                ("table_name", "SQL table name."),
                ("kind", "table, column, or foreign_key."),
                ("name", "Table, column, or constraint name."),
                ("iri", "Raw RDF resource or predicate IRI."),
                ("graph_iri", "Graph containing its RDF statements."),
                ("target_iri", "Foreign-key target table IRI; NULL for other kinds."),
            ]
        ]
    )

    class Meta:
        """Public identity discovery metadata."""

        name = "rdf_tables"
        description = "Discover RDF identifiers for SQL tables, columns and foreign-key relationships"
        examples = [
            FunctionExample(
                sql="SELECT count(*) AS objects FROM rdf.main.rdf_tables()",
                description="Count mapped SQL objects by RDF role",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[MappingArgs]) -> BindResponse:
        """Return the fixed mapping schema."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[MappingArgs]) -> ScanState:
        """Start before the single bounded catalog batch."""
        return ScanState()

    @classmethod
    def process(cls, params: ProcessParams[MappingArgs], state: ScanState, out: OutputCollector) -> None:
        """Expose mapping metadata without exposing opaque serialized table definitions."""
        if state.after:
            out.finish()
            return
        rows = []
        with database(params.attach_opaque_data).access(params.transaction_opaque_data) as snapshot:
            graph = RelationalGraph(snapshot)
            for table in sorted(graph.tables(), key=lambda t: (t.path, t.name)):
                if params.args.schema_name and name_key(params.args.schema_name) != name_key(table.path[0]):
                    continue
                base = {
                    "schema_name": table.path[0],
                    "table_name": table.name,
                    "graph_iri": table.graph.value,
                    "target_iri": None,
                }
                rows.append({**base, "kind": "table", "name": table.name, "iri": table.node.value})
                for field in table.schema:
                    rows.append(
                        {**base, "kind": "column", "name": field.name, "iri": table.predicate(field.name).value}
                    )
                for constraint in table.constraints:
                    fk = constraint.definition.foreign_key
                    if fk is not None:
                        target = graph.require(fk.referenced_table.schema_path, fk.referenced_table.name)
                        rows.append(
                            {
                                **base,
                                "kind": "foreign_key",
                                "name": constraint.definition.declared_name or constraint.ref.name,
                                "iri": table.relationship(constraint).value,
                                "graph_iri": "urn:vgi:rdf:relationships",
                                "target_iri": target.node.value,
                            }
                        )
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        state.after = 1


def iri_column(table: Table) -> str:
    """Pick a public RDF identity column without shadowing user columns."""
    name = "__rdf_iri"
    while name_key(name) in {name_key(n) for n in table.schema.names}:
        name += "_"
    return name


@init_single_worker
class RdfRows(RdfScan):
    """Read typed SQL rows with their stable RDF identity alongside each row."""

    class Meta:
        """Public row identity metadata."""

        name = "rdf_rows"
        tags: ClassVar[dict[str, str]] = {}
        description = "Read a SQL table with its stable RDF row IRI for graph joins"
        projection_pushdown = False
        filter_pushdown = False
        auto_apply_filters = False
        examples = [
            FunctionExample(
                sql="SELECT name, __rdf_iri FROM rdf.main.rdf_rows('main', 'people') ORDER BY name",
                description="Find RDF identities for an existing people table with a name column",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[TableArgs]) -> BindResponse:
        """Append a collision-free RDF IRI column to the table's ordinary data schema."""
        with database(params.attach_opaque_data).access(params.transaction_opaque_data) as snapshot:
            table = RelationalGraph(snapshot).require([params.args.schema_name], params.args.table_name)
            return BindResponse(
                output_schema=pa.schema(
                    [
                        *table.data_schema,
                        pa.field(
                            iri_column(table), pa.string(), metadata={b"comment": b"Stable RDF row resource IRI."}
                        ),
                    ]
                )
            )

    @classmethod
    def initial_state(cls, params: ProcessParams[TableArgs]) -> ScanState:
        """Freeze and evaluate typed rows with identities in one bounded scan."""
        if params.transaction_opaque_data is None:
            raise ValueError("RDF rows require a VGI transaction")
        db = database(params.attach_opaque_data)
        return ScanState(
            snapshot_id=prepare_scan(
                db,
                params.transaction_opaque_data,
                params.args.schema_name,
                params.args.table_name,
                params.output_schema,
                include_iri=True,
            )
        )


def graph_metadata(function: Any, documentation: str, *, executable: list[dict[str, Any]] | None = None) -> None:
    """Keep graph discovery documentation and executable examples together."""
    function.Meta.tags = {
        "vgi.title": function.Meta.name.replace("_", " ").title(),
        "vgi.category": "rdf",
        "vgi.keywords": json.dumps(["rdf", "graph", "sql", "sparql"]),
        "vgi.doc_llm": documentation,
        "vgi.doc_md": documentation + "\n\n### Operation\n\n"
        "The function reads the current attachment and participates in its SQL transaction. "
        "Use explicit SQL ordering when row order matters. A failure returns an error; "
        "retry the whole transaction after a serialization conflict. SQL examples are listed separately.",
        "vgi.example_queries": json.dumps(
            [{"sql": e.sql, "description": e.description} for e in function.Meta.examples]
        ),
    }
    if executable is not None:
        function.Meta.tags["vgi.executable_examples"] = json.dumps(executable)
    if hasattr(function, "FIXED_SCHEMA"):
        function.Meta.tags["vgi.result_columns_schema"] = json.dumps(
            [
                {
                    "name": f.name,
                    "type": "BIGINT" if pa.types.is_int64(f.type) else "VARCHAR",
                    "description": (f.metadata or {}).get(b"comment", b"").decode(),
                }
                for f in function.FIXED_SCHEMA
            ]
        )


graph_metadata(
    RdfSparql,
    "Execute local read-only SPARQL SELECT, ASK, CONSTRUCT or DESCRIBE. SELECT variables become SQL columns "
    "of `STRUCT(kind, value, datatype, language)`, preserving unbound values as NULL; use column.value for an IRI "
    "or literal lexical value. ASK returns answer `BOOLEAN`; graph results return subject, predicate, "
    "object term structs. "
    "SERVICE and remote access are disabled. Use GRAPH clauses, or union_default := true. "
    "Results are frozen once per statement. max_rows (default 10000) and timeout_ms (default 30000) are hard bounds; "
    "operator ceilings also apply. A limit error fails the query instead of returning incomplete results.",
    executable=[
        {"sql": RdfSparql.Meta.examples[0].sql, "description": "Verify the catalog exists", "expected_result": [[True]]}
    ],
)
graph_metadata(
    RdfTables,
    "Discover the mapping between SQL objects and RDF IRIs. Returns schema_name, table_name, kind (table, column, "
    "foreign_key), name, iri, graph_iri, target_iri. Use column IRIs in SPARQL and foreign-key predicates "
    "for traversal. "
    "target_iri is NULL except for foreign keys. An empty database has no mapped tables.",
    executable=[
        {
            "sql": "SELECT count(*) FROM rdf.main.rdf_tables()",
            "description": "A fresh attachment contains no user tables",
            "expected_result": [[0]],
        }
    ],
)
graph_metadata(
    RdfRows,
    "Read an existing SQL table with ordinary typed columns plus __rdf_iri containing its stable RDF row IRI. "
    "Supply raw schema and table names separately. If a user column already has that name, underscores are appended "
    "until it is unique. Identities survive updates but disappear on DELETE; replacement tables get new identities. "
    "Use these identifiers to join relational rows with graph queries. Requires a user-created table; the people "
    "example assumes CREATE TABLE people(name `VARCHAR`) has already run.",
)

GRAPH_FUNCTIONS: list[type[Any]] = [RdfSparql, RdfTables, RdfRows]


@dataclass(frozen=True, kw_only=True)
class NeighborArgs:
    """Indexed edge lookup with explicit graph, direction and resource bounds."""

    node: Annotated[str, Arg(0, doc="Starting node as a raw IRI or _:blank-node label.")]
    graph_iri: Annotated[
        str, Arg("graph_iri", default="urn:vgi:rdf:relationships", doc="Named graph; empty searches all graphs.")
    ]
    predicate_iri: Annotated[
        str, Arg("predicate_iri", default="", doc="Relationship predicate IRI; empty accepts every predicate.")
    ]
    direction: Annotated[
        str,
        Arg(
            "direction",
            default="out",
            choices=["out", "in", "both"],
            doc="Follow outgoing, incoming, or both directions.",
        ),
    ]
    max_rows: Annotated[
        int, Arg("max_rows", default=10000, ge=1, doc="Maximum returned rows; overflow fails explicitly.")
    ]
    timeout_ms: Annotated[
        int, Arg("timeout_ms", default=30000, ge=1, doc="Execution deadline including subprocess startup.")
    ]


@dataclass(frozen=True, kw_only=True)
class ReachArgs(NeighborArgs):
    """Neighborhood options plus a finite maximum traversal depth."""

    max_depth: Annotated[
        int, Arg("max_depth", default=8, ge=1, le=64, doc="Maximum number of edges to follow, from 1 to 64.")
    ]


def start_traversal(params: ProcessParams[Any], operation: str) -> QueryState:
    """Run a bounded traversal against a frozen statement snapshot."""
    if params.transaction_opaque_data is None:
        raise ValueError("RDF traversal requires a VGI transaction")
    db = database(params.attach_opaque_data)
    name = db.freeze_scan(params.transaction_opaque_data)
    with db.scan_access(params.transaction_opaque_data, name) as snapshot:
        output = snapshot.path.parent / (name + ".arrow")
        try:
            call_runner(
                {
                    "operation": operation,
                    "options": asdict(params.args),
                    "path": str(snapshot.path),
                    "output": str(output),
                    "schema": pack_schema(params.output_schema),
                    "max_rows": params.args.max_rows,
                    "max_bytes": setting("MAX_RESULT_BYTES", 64 * 1024 * 1024),
                },
                timeout_ms=params.args.timeout_ms,
            )
        except BaseException:
            output.unlink(missing_ok=True)
            raise
    return QueryState(snapshot_id=name)


@init_single_worker
class RdfNeighbors(TableFunctionGenerator[NeighborArgs, QueryState]):
    """Expose incident resource-to-resource edges without traversing literal values."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = EDGE_SCHEMA

    operation: ClassVar[str] = "neighbors"

    class Meta:
        """Public neighborhood metadata."""

        name = "rdf_neighbors"
        description = "Find outgoing or incoming RDF relationship edges for one node"
        examples = [
            FunctionExample(
                sql="SELECT count(*) AS edges FROM rdf.main.rdf_neighbors('urn:example:alice')",
                description="Find the relationships of an example entity",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[NeighborArgs]) -> BindResponse:
        """Bind the fixed result schema after validating query bounds and terms."""
        query_limits(params.args.max_rows, params.args.timeout_ms)
        validate_traversal(asdict(params.args))
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[NeighborArgs]) -> QueryState:
        """Run the indexed lookup in a deadline-controlled child process."""
        return start_traversal(params, cls.operation)

    @classmethod
    def process(cls, params: ProcessParams[NeighborArgs], state: QueryState, out: OutputCollector) -> None:
        """Read the immutable Arrow result using the common spool implementation."""
        read_spool(params, state, out)


@init_single_worker
class RdfReachable(TableFunctionGenerator[ReachArgs, QueryState]):
    """Compute bounded reachability, returning each node once at its minimum depth."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = REACH_SCHEMA

    operation: ClassVar[str] = "reachable"

    class Meta:
        """Public reachability metadata."""

        name = "rdf_reachable"
        description = "Traverse RDF relationships with cycle detection and bounded minimum-depth results"
        examples = [
            FunctionExample(
                sql="SELECT count(*) AS nodes FROM rdf.main.rdf_reachable('urn:example:alice', max_depth := 3)",
                description="Find entities up to three edges away from an example entity",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ReachArgs]) -> BindResponse:
        """Bind reachability arguments, including maximum depth."""
        query_limits(params.args.max_rows, params.args.timeout_ms)
        validate_traversal(asdict(params.args))
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ReachArgs]) -> QueryState:
        """Initialize the bounded traversal with its depth limit."""
        return start_traversal(params, cls.operation)

    @classmethod
    def process(cls, params: ProcessParams[ReachArgs], state: QueryState, out: OutputCollector) -> None:
        """Return a page of distinct reachable nodes."""
        read_spool(params, state, out)


graph_metadata(
    RdfNeighbors,
    "Find resource-to-resource edges adjacent to node, a raw IRI or _:blank-node label. "
    "The default graph_iri is urn:vgi:rdf:relationships, containing derived SQL foreign-key edges. "
    "Use another graph_iri for imported RDF or empty for all graphs. predicate_iri optionally restricts edges; "
    "direction is out, in, or both. Returns source, predicate, target, graph and neighbor identifiers. "
    "Literals are not traversed. A default-graph edge has graph NULL. Results have no implicit ordering. "
    "max_rows, timeout_ms and deployment ceilings fail explicitly rather than truncate results.",
    executable=[
        {
            "sql": "SELECT count(*) FROM rdf.main.rdf_neighbors('urn:example:absent')",
            "description": "An absent node has no neighbors",
            "expected_result": [[0]],
        }
    ],
)
graph_metadata(
    RdfReachable,
    "Breadth-first RDF traversal starting from node. Returns each reachable node once with depth equal to "
    "the minimum number of edges from the start, excluding the start itself even when cycles exist. "
    "max_depth defaults to 8 and cannot exceed 64. graph_iri, predicate_iri and direction have the same "
    "meaning as rdf_neighbors. The default graph contains SQL foreign-key relationships. "
    "Traversal has row, examined-edge and wall-time limits; exceeding a limit raises an error. "
    "Order results explicitly, for example ORDER BY depth, node.",
    executable=[
        {
            "sql": "SELECT count(*) FROM rdf.main.rdf_reachable('urn:example:absent', max_depth := 3)",
            "description": "An absent node has no reachable neighbors",
            "expected_result": [[0]],
        }
    ],
)
GRAPH_FUNCTIONS.extend([RdfNeighbors, RdfReachable])


RdfSparql.Meta.tags["vgi.result_dynamic_columns_md"] = """
SELECT results (one column per projected variable):

| Name | Type | Description |
| --- | --- | --- |
| variable | STRUCT(kind VARCHAR, value VARCHAR, datatype VARCHAR, language VARCHAR) | Term or NULL. |

ASK results:

| Name | Type | Description |
| --- | --- | --- |
| answer | BOOLEAN | Whether the pattern matches. |

CONSTRUCT and DESCRIBE results:

| Name | Type | Description |
| --- | --- | --- |
| subject | STRUCT(kind VARCHAR, value VARCHAR, datatype VARCHAR, language VARCHAR) | RDF triple subject. |
| predicate | STRUCT(kind VARCHAR, value VARCHAR, datatype VARCHAR, language VARCHAR) | RDF triple predicate. |
| object | STRUCT(kind VARCHAR, value VARCHAR, datatype VARCHAR, language VARCHAR) | RDF triple object. |
"""
RdfRows.Meta.tags["vgi.result_dynamic_columns_md"] = """
The table selected by schema_name and table_name determines the ordinary columns:

| Name | Type | Description |
| --- | --- | --- |
| name | VARCHAR | Example for a people(name VARCHAR) table; actual names and types come from the selected table. |
| __rdf_iri | VARCHAR | Stable RDF row IRI; underscores are appended if necessary to avoid name collisions. |
"""
