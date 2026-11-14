"""Relational tables represented entirely by RDF catalog and row graphs."""

from __future__ import annotations

import base64
import json
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, replace
from typing import Any, cast
from urllib.parse import quote

import duckdb
import pyarrow as pa
from pyoxigraph import Literal, NamedNode, Quad, QuerySolutions
from vgi.catalog import OnConflict
from vgi.catalog.constraints import (
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintInfo,
    ConstraintKind,
    ConstraintOperation,
    ConstraintsApplyRequest,
    ConstraintSnapshot,
    ConstraintValidation,
    ForeignKeyMatch,
    ReferentialAction,
    served_table_schema,
)
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.sql import column_expressions
from vgi.write_results import row_id_field

from .codec import NS, decode, encode, identifier, pack_schema, unpack_schema
from .limits import setting
from .storage import Snapshot

CATALOG = NamedNode(NS + "catalog")
DEFINITION = NamedNode(NS + "definition")
TABLE_KEY = NamedNode(NS + "tableKey")
SCHEMA_NAME = NamedNode(NS + "schemaName")
ROW_ID = NamedNode(NS + "rowId")
NEXT_ID = NamedNode(NS + "nextId")
RDF_TYPE = NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
RELATIONSHIPS = NamedNode(NS + "relationships")
KEY_INDEX = NamedNode(NS + "key-index")


def _scan_batch(schema: pa.Schema, values: list[list[Any]], count: int) -> pa.RecordBatch:
    if not schema:
        return pa.record_batch([pa.nulls(count)], names=["_count"]).select([])
    return pa.RecordBatch.from_arrays(
        [pa.array(v, type=f.type) for f, v in zip(schema, values, strict=True)],
        schema=schema,
    )


def rows_table(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.Table:
    """Build typed arrays without Arrow's NULL nested-scalar conversion bug."""
    return pa.Table.from_arrays(
        [
            pa.array([r[f.name] if r[f.name] is not None and r[f.name].is_valid else None for r in rows], type=f.type)
            for f in schema
        ],
        schema=schema,
    )


def name_key(name: str) -> str:
    """Match DuckDB's ASCII case-insensitive identifier policy."""
    return name.translate(str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"))


def _schema_node(name: str) -> NamedNode:
    return NamedNode(NS + "schema:" + quote(name_key(name), safe=""))


def _table_key(path: list[str], name: str) -> Literal:
    return Literal(json.dumps([*[name_key(p) for p in path], name_key(name)]))


@dataclass
class Table:
    """A persisted SQL table definition and its stable RDF identity."""

    node: NamedNode
    path: list[str]
    name: str
    schema: pa.Schema
    constraints: list[ConstraintInfo]
    revision: bytes
    comment: str | None = None
    tags: dict[str, str] | None = None

    @property
    def graph(self) -> NamedNode:
        """Name of the graph containing this table's rows."""
        return NamedNode(self.node.value + "/rows")

    @property
    def rowid_name(self) -> str:
        """Choose an internal row identifier that cannot shadow a user column."""
        name = "__rdf_rowid"
        while name_key(name) in {name_key(n) for n in self.schema.names}:
            name += "_"
        return name

    @property
    def scan_schema(self) -> pa.Schema:
        """Return user columns plus the hidden row identity required for writes."""
        return pa.schema([*self.data_schema, row_id_field(self.rowid_name)])

    @property
    def data_schema(self) -> pa.Schema:
        """Remove catalog-only expressions from table-function Arrow schemas."""
        keys = {b"vgi.column.default.v1", b"vgi.column.generated.v1", b"vgi.column.generation.v1"}
        return pa.schema(
            [f.with_metadata({k: v for k, v in (f.metadata or {}).items() if k not in keys}) for f in self.schema]
        )

    def predicate(self, column: str) -> NamedNode:
        """Return an injective, readable IRI for a column."""
        return NamedNode(self.node.value + "/column/" + quote(column, safe=""))

    def row_node(self, rowid: int) -> NamedNode:
        """Return a stable row resource, independent of any SQL key."""
        return NamedNode(self.node.value + f"/row/{rowid}")

    def relationship(self, constraint: ConstraintInfo) -> NamedNode:
        """Return the stable predicate for a foreign-key constraint."""
        return NamedNode(self.node.value + "/foreign-key/" + constraint.ref.name)

    def key_predicate(self, constraint: ConstraintInfo) -> NamedNode:
        """Return the RDF predicate for a SQL key's hash bucket index."""
        return NamedNode(self.node.value + "/key/" + constraint.ref.name)

    def to_json(self) -> str:
        """Serialize the definition as the object of an RDF metadata statement."""
        return json.dumps(
            {
                "path": self.path,
                "name": self.name,
                "schema": pack_schema(self.schema),
                "constraints": [base64.b64encode(c.serialize_to_bytes()).decode() for c in self.constraints],
                "revision": self.revision.hex(),
                "comment": self.comment,
                "tags": self.tags,
            }
        )

    @classmethod
    def from_json(cls, node: NamedNode, value: str) -> Table:
        """Read one persisted table definition."""
        obj = json.loads(value)
        return cls(
            node,
            obj["path"],
            obj["name"],
            unpack_schema(obj["schema"]),
            [ConstraintInfo.deserialize_from_bytes(base64.b64decode(c)) for c in obj["constraints"]],
            bytes.fromhex(obj["revision"]),
            obj["comment"],
            obj["tags"],
        )


class RelationalGraph:
    """Read and mutate tables within an already locked RDF snapshot."""

    def __init__(self, snapshot: Snapshot) -> None:
        """Use the caller's locked snapshot without retaining another store handle."""
        self.snapshot = snapshot

    def schemas(self) -> list[str]:
        """List user schemas in deterministic name order."""
        return sorted(
            cast(Literal, q.object).value
            for q in self.snapshot.store.quads_for_pattern(None, SCHEMA_NAME, None, CATALOG)
        )

    def schema_create(self, name: str, conflict: OnConflict = OnConflict.ERROR) -> None:
        """Create an RDF schema resource; preserve IF NOT EXISTS semantics."""
        existing = next((s for s in self.schemas() if name_key(s) == name_key(name)), None)
        if existing is not None:
            if conflict == OnConflict.IGNORE:
                return
            raise ValueError(f"Schema {name!r} already exists")
        if not name or "\0" in name:
            raise ValueError("Schema name must be nonempty without NUL")
        self.snapshot.store.add(Quad(_schema_node(name), SCHEMA_NAME, Literal(name), CATALOG))

    def tables(self) -> list[Table]:
        """List table definitions stored in the catalog graph."""
        return [
            Table.from_json(cast(NamedNode, q.subject), cast(Literal, q.object).value)
            for q in self.snapshot.store.quads_for_pattern(None, DEFINITION, None, CATALOG)
        ]

    def get(self, path: list[str], name: str) -> Table | None:
        """Resolve a table without flattening quoted schema/table names."""
        matches = list(self.snapshot.store.quads_for_pattern(None, TABLE_KEY, _table_key(path, name), CATALOG))
        if not matches:
            return None
        node = cast(NamedNode, matches[0].subject)
        value = next(self.snapshot.store.quads_for_pattern(node, DEFINITION, None, CATALOG)).object
        return Table.from_json(node, cast(Literal, value).value)

    def require(self, path: list[str], name: str) -> Table:
        """Resolve a table or report a clear missing-object error."""
        table = self.get(path, name)
        if table is None:
            raise ValueError(f"Table {path!r}.{name!r} does not exist")
        return table

    def create(
        self,
        path: list[str],
        name: str,
        schema: pa.Schema,
        definitions: list[ConstraintDefinition],
        conflict: OnConflict,
        comment: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> bool:
        """Create a table, checking every definition before storing any triples."""
        if len(path) != 1:
            raise ValueError("RDF tables use one SQL schema component")
        resolved = next((s for s in self.schemas() if name_key(s) == name_key(path[0])), None)
        if resolved is None:
            raise ValueError(f"Schema {path!r} does not exist")
        path = [resolved]
        old = self.get(path, name)
        if old is not None:
            if conflict == OnConflict.IGNORE:
                return False
            if conflict != OnConflict.REPLACE:
                raise ValueError(f"Table {name!r} already exists")
        if not name or "\0" in name or len(schema) == 0:
            raise ValueError("A table needs a valid name and at least one column")
        if len({name_key(f.name) for f in schema}) != len(schema):
            raise ValueError("Duplicate column names")
        for field in schema:
            if not field.name or "\0" in field.name or b"is_row_id" in (field.metadata or {}):
                raise ValueError("Invalid user column name or reserved row-id metadata")
            if column_expressions(field).generated is not None:
                raise ValueError("Generated columns are not supported; use a SELECT expression")
        table = Table(
            NamedNode(NS + "table:" + uuid.uuid4().hex), path, name, schema, [], uuid.uuid4().bytes, comment, tags or {}
        )
        table.constraints = self._build_constraints(table, definitions)
        table.schema = served_table_schema(schema, table.constraints)
        self.validate(table, [])  # Bind CHECKs and validate supported expression contexts even on empty tables.
        if old is not None:
            self.drop(path, old.name)
        self.snapshot.store.extend(
            [
                Quad(table.node, TABLE_KEY, _table_key(path, name), CATALOG),
                Quad(table.node, DEFINITION, Literal(table.to_json()), CATALOG),
                Quad(table.node, NEXT_ID, Literal(1), CATALOG),
            ]
        )
        return True

    def _build_constraints(self, table: Table, definitions: list[ConstraintDefinition]) -> list[ConstraintInfo]:
        schema, path, name = table.schema, table.path, table.name
        result: list[ConstraintInfo] = []
        for definition in definitions:
            definition.validate_schema(schema)
            if any(len(p) != 1 for p in definition.column_paths):
                raise ValueError("Constraints on nested fields are not supported")
            reference = None
            if definition.kind == ConstraintKind.FOREIGN_KEY:
                fk = definition.foreign_key
                assert fk is not None
                if (
                    fk.match != ForeignKeyMatch.SIMPLE
                    or fk.on_update not in {ReferentialAction.NO_ACTION, ReferentialAction.RESTRICT}
                    or fk.on_delete not in {ReferentialAction.NO_ACTION, ReferentialAction.RESTRICT}
                ):
                    raise ValueError("Foreign keys support MATCH SIMPLE and NO ACTION/RESTRICT only")
                if fk.referenced_table.schema_path == path and name_key(fk.referenced_table.name) == name_key(name):
                    raise ValueError("Self-referencing foreign keys are not supported")
                target = self.require(fk.referenced_table.schema_path, fk.referenced_table.name)
                reference = next(
                    (
                        c.ref
                        for c in target.constraints
                        if c.definition.kind in {ConstraintKind.PRIMARY_KEY, ConstraintKind.UNIQUE}
                        and c.definition.column_paths == fk.referenced_column_paths
                    ),
                    None,
                )
                if reference is None:
                    raise ValueError("Foreign key must reference a declared PRIMARY KEY or UNIQUE constraint")
                for local, remote in zip(definition.column_paths, fk.referenced_column_paths, strict=True):
                    if schema.field(local[0]).type != target.schema.field(remote[0]).type:
                        raise ValueError("Foreign key columns must have matching types")
            result.append(
                ConstraintInfo(
                    ref=CatalogObjectRef(kind="constraint", schema_path=path, name=uuid.uuid4().hex, owner_table=name),
                    definition=definition,
                    enforcement=ConstraintEnforcement.ENFORCED,
                    validation=ConstraintValidation.ALL_ROWS,
                    referenced_constraint=reference,
                )
            )
        return result

    def apply_constraints(self, request: ConstraintsApplyRequest) -> ConstraintSnapshot:
        """Apply canonical ADD/DROP/RENAME/REPLACE changes and validate existing rows."""
        table = self.require(request.table.schema_path, request.table.name)
        if request.expected_revision != table.revision:
            raise ValueError("Constraint revision conflict; refresh the table and retry")
        rows = self.rows(table)
        records = {c.ref.name: c for c in table.constraints}
        staged: dict[str, Table] = {}
        for change in request.constraint_changes():
            current = records.get(change.ref.name) if change.ref is not None else None
            if change.operation != ConstraintOperation.ADD and (current is None or current.ref != change.ref):
                raise ValueError("Unknown constraint identity")
            if change.operation in {ConstraintOperation.DROP, ConstraintOperation.REPLACE}:
                assert current is not None
                for other in self.tables():
                    matches = [c for c in other.constraints if c.referenced_constraint == current.ref]
                    if matches:
                        if not change.cascade:
                            raise ValueError("Cannot mutate a key referenced by a foreign key")
                        other = staged.get(other.node.value, other)
                        other.constraints = [c for c in other.constraints if c.referenced_constraint != current.ref]
                        other.revision = uuid.uuid4().bytes
                        staged[other.node.value] = other
                del records[current.ref.name]
            if change.operation in {ConstraintOperation.ADD, ConstraintOperation.REPLACE}:
                if (
                    change.enforcement != ConstraintEnforcement.ENFORCED
                    or change.validation != ConstraintValidation.ALL_ROWS
                ):
                    raise ValueError("Only ENFORCED constraints validated against ALL_ROWS are supported")
                assert change.definition is not None
                info = self._build_constraints(table, [change.definition])[0]
                if current is not None:
                    info = replace(info, ref=current.ref)
                records[info.ref.name] = info
            if change.operation == ConstraintOperation.RENAME:
                assert current is not None
                records[current.ref.name] = replace(
                    current, definition=replace(current.definition, declared_name=change.declared_name)
                )
        table.constraints = list(records.values())
        table.schema = served_table_schema(table.schema, table.constraints)
        table.revision = uuid.uuid4().bytes
        result = ConstraintSnapshot(
            table=request.table,
            definition_revision=table.revision,
            constraints=[c.serialize_to_bytes() for c in table.constraints],
        )
        self.validate(table, rows)
        staged[table.node.value] = table
        updates = []
        for item in staged.values():
            updates.append(f"DELETE WHERE {{ GRAPH {CATALOG} {{ {item.node} {DEFINITION} ?d }} }}")
            updates.append(
                f"INSERT DATA {{ GRAPH {CATALOG} {{ {item.node} {DEFINITION} {Literal(item.to_json())} }} }}"
            )
        self.snapshot.store.update("; ".join(updates))
        for item in staged.values():
            self.rebuild_relationships(item)
            self.rebuild_keys(item)
        return result

    def drop(self, path: list[str], name: str, *, ignore_missing: bool = False, cascade: bool = False) -> None:
        """Drop all table triples, rejecting live foreign-key dependencies."""
        table = self.get(path, name)
        if table is None:
            if ignore_missing:
                return
            raise ValueError(f"Table {name!r} does not exist")
        dependent = [
            other
            for other in self.tables()
            if any(
                c.referenced_constraint is not None
                and c.referenced_constraint.schema_path == table.path
                and c.referenced_constraint.owner_table == table.name
                for c in other.constraints
            )
        ]
        if dependent and not cascade:
            raise ValueError(f"Cannot drop {name!r}: foreign-key dependencies exist")
        updates: list[str] = []
        for other in dependent:
            # The dependent object is the foreign-key constraint, not its table.
            # DROP parent CASCADE must preserve the child's data.
            other.constraints = [
                c
                for c in other.constraints
                if not (
                    c.referenced_constraint is not None
                    and c.referenced_constraint.schema_path == table.path
                    and c.referenced_constraint.owner_table == table.name
                )
            ]
            other.revision = uuid.uuid4().bytes
            updates.append(f"DELETE WHERE {{ GRAPH {CATALOG} {{ {other.node} {DEFINITION} ?d }} }}")
            updates.append(
                f"INSERT DATA {{ GRAPH {CATALOG} {{ {other.node} {DEFINITION} {Literal(other.to_json())} }} }}"
            )
        updates.append(f"DROP SILENT GRAPH {table.graph}; DELETE WHERE {{ GRAPH {CATALOG} {{ {table.node} ?p ?o }} }}")
        self.snapshot.store.update("; ".join(updates))
        self.snapshot.store.update(
            f"DELETE {{ GRAPH {RELATIONSHIPS} {{ ?s ?p ?o }} }} WHERE {{ GRAPH {RELATIONSHIPS} {{ ?s ?p ?o }} "
            f"FILTER(STRSTARTS(STR(?s), {Literal(table.node.value + '/row/')})) }}"
        )
        for other in dependent:
            self.rebuild_relationships(other)
        self._clear_keys(table)

    def schema_drop(self, name: str, *, ignore_missing: bool, cascade: bool) -> None:
        """Drop a schema and, when requested, its dependent tables."""
        if name_key(name) == "main":
            raise ValueError("Cannot drop the main schema")
        if name_key(name) not in {name_key(s) for s in self.schemas()}:
            if ignore_missing:
                return
            raise ValueError(f"Schema {name!r} does not exist")
        tables = [t for t in self.tables() if name_key(t.path[0]) == name_key(name)]
        if tables and not cascade:
            raise ValueError("Schema is not empty; use CASCADE")
        for table in tables:
            self.drop(table.path, table.name, cascade=True, ignore_missing=True)
        self.snapshot.store.update(f"DELETE WHERE {{ GRAPH {CATALOG} {{ {_schema_node(name)} ?p ?o }} }}")

    def scan(
        self,
        table: Table,
        schema: pa.Schema | None = None,
        *,
        after: int = 0,
        limit: int | None = None,
        row_ids: set[int] | None = None,
    ) -> tuple[pa.RecordBatch, int]:
        """Collect a requested row subset for write validation and catalog operations."""
        schema = table.scan_schema if schema is None else schema
        batches = list(self.scan_batches(table, schema, after=after, limit=limit, row_ids=row_ids))
        if not batches:
            return _scan_batch(schema, [[] for _ in schema], 0), after
        if len(batches) == 1:
            return batches[0]
        combined = pa.Table.from_batches([batch for batch, _ in batches], schema=schema).combine_chunks()
        return combined.to_batches(max_chunksize=combined.num_rows)[0], batches[-1][1]

    def scan_batches(
        self,
        table: Table,
        schema: pa.Schema | None = None,
        *,
        after: int = 0,
        limit: int | None = None,
        row_ids: set[int] | None = None,
        batch_size: int = 2048,
    ) -> Iterator[tuple[pa.RecordBatch, int]]:
        """Evaluate one projected RDF query and decode bounded Arrow batches."""
        schema = table.scan_schema if schema is None else schema
        fields = [f for f in schema if f.name != table.rowid_name]
        names = list(dict.fromkeys(f.name for f in fields))
        optional = " ".join(f"OPTIONAL {{ ?row {table.predicate(n)} ?v{i} }}" for i, n in enumerate(names))
        variables = " ".join(f"?v{i}" for i in range(len(names)))
        selection = "" if row_ids is None else "VALUES ?id { " + " ".join(str(int(i)) for i in row_ids) + " } "
        query = (
            f"SELECT ?id {variables} WHERE {{ {selection} GRAPH {table.graph} {{ ?row {ROW_ID} ?id . "
            f"FILTER(?id > {after}) {optional} }} }} ORDER BY ?id"
        )
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        solutions = cast(QuerySolutions, self.snapshot.store.query(query))
        values: list[list[Any]] = [[] for _ in schema]
        last = after
        count = 0
        for row in solutions:
            last = int(cast(Literal, row["id"]).value)
            count += 1
            for i, field in enumerate(schema):
                if field.name == table.rowid_name:
                    values[i].append(last)
                else:
                    term = cast(Literal | None, row[f"v{names.index(field.name)}"])
                    values[i].append(decode(term, field.type))
            if count == batch_size:
                yield _scan_batch(schema, values, count), last
                values = [[] for _ in schema]
                count = 0
        if count:
            yield _scan_batch(schema, values, count), last

    def rows(self, table: Table, row_ids: set[int] | None = None) -> list[dict[str, Any]]:
        """Return cells as Arrow scalars so nanoseconds and nested types remain exact."""
        maximum_rows = setting("MAX_VALIDATION_ROWS", 250_000)
        maximum_bytes = setting("MAX_VALIDATION_BYTES", 256 * 1024 * 1024)
        result: list[dict[str, Any]] = []
        size = 0
        for batch, _ in self.scan_batches(table, row_ids=row_ids, limit=maximum_rows + 1):
            size += batch.nbytes
            if len(result) + batch.num_rows > maximum_rows or size > maximum_bytes:
                raise ValueError("RDF validation exceeds MAX_VALIDATION_ROWS or MAX_VALIDATION_BYTES")
            result.extend(
                {name: batch.column(i)[j] for i, name in enumerate(batch.schema.names)} for j in range(batch.num_rows)
            )
        return result

    def validate(self, table: Table, rows: list[dict[str, Any]]) -> None:
        """Enforce SQL constraints with an ephemeral expression engine, never relational persistence."""
        if not table.constraints:
            return
        with duckdb.connect(config={"enable_external_access": "false", "threads": "1"}) as connection:
            connection.register("candidate", rows_table(rows, table.data_schema))
            for constraint in table.constraints:
                definition = constraint.definition
                columns = [identifier(path[0]) for path in definition.column_paths]
                kind = definition.kind
                query = ""
                if kind in {ConstraintKind.NOT_NULL, ConstraintKind.PRIMARY_KEY}:
                    missing = " OR ".join(f"{c} IS NULL" for c in columns)
                    if connection.execute(f"SELECT 1 FROM candidate WHERE {missing} LIMIT 1").fetchone():
                        raise ValueError(f"NOT NULL constraint failed on {table.name}: {columns}")
                if kind in {ConstraintKind.PRIMARY_KEY, ConstraintKind.UNIQUE}:
                    nonnull = " AND ".join(f"{c} IS NOT NULL" for c in columns)
                    query = (
                        f"SELECT 1 FROM candidate WHERE {nonnull} GROUP BY {', '.join(columns)} "
                        "HAVING count(*) > 1 LIMIT 1"
                    )
                elif kind == ConstraintKind.CHECK:
                    expression = definition.check_expression
                    assert expression is not None
                    context = expression.evaluation_context
                    if context.collation != "binary" or context.timezone != "UTC":
                        raise ValueError("CHECK expressions currently require binary collation and UTC")
                    query = f"SELECT 1 FROM candidate WHERE ({expression.sql}) IS FALSE LIMIT 1"
                    statements = connection.extract_statements(query)
                    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
                        raise ValueError("CHECK must contain one scalar SQL expression")
                elif kind == ConstraintKind.FOREIGN_KEY:
                    fk = definition.foreign_key
                    assert fk is not None
                    target = self.require(fk.referenced_table.schema_path, fk.referenced_table.name)
                    connection.register("referenced", rows_table(self.rows(target), target.data_schema))
                    condition = " AND ".join(
                        f"c.{left} = p.{identifier(right[0])}"
                        for left, right in zip(columns, fk.referenced_column_paths, strict=True)
                    )
                    nonnull = " AND ".join(f"c.{c} IS NOT NULL" for c in columns)
                    query = (
                        f"SELECT 1 FROM candidate c WHERE {nonnull} "
                        f"AND NOT EXISTS (SELECT 1 FROM referenced p WHERE {condition}) LIMIT 1"
                    )
                if query and connection.execute(query).fetchone():
                    raise ValueError(f"{kind.value} constraint failed on {table.name}")

    def _validate_dependents(self, table: Table, candidates: list[dict[str, Any]]) -> None:
        for other in self.tables():
            for constraint in other.constraints:
                fk = constraint.definition.foreign_key
                if fk is None or constraint.referenced_constraint is None:
                    continue
                ref = constraint.referenced_constraint
                if ref.schema_path != table.path or ref.owner_table != table.name:
                    continue
                with duckdb.connect(config={"enable_external_access": "false", "threads": "1"}) as connection:
                    connection.register("parent", rows_table(candidates, table.data_schema))
                    connection.register("child", rows_table(self.rows(other), other.data_schema))
                    local = [identifier(p[0]) for p in constraint.definition.column_paths]
                    remote = [identifier(p[0]) for p in fk.referenced_column_paths]
                    condition = " AND ".join(f"c.{a} = p.{b}" for a, b in zip(local, remote, strict=True))
                    nonnull = " AND ".join(f"c.{a} IS NOT NULL" for a in local)
                    if connection.execute(
                        f"SELECT 1 FROM child c WHERE {nonnull} "
                        f"AND NOT EXISTS (SELECT 1 FROM parent p WHERE {condition}) LIMIT 1"
                    ).fetchone():
                        raise ValueError(f"FOREIGN_KEY constraint failed: {other.name} references {table.name}")

    def write(
        self, table: Table, operation: str, batch: pa.RecordBatch
    ) -> tuple[list[dict[str, Any] | None], list[dict[str, Any] | None]]:
        """Validate and apply an Arrow batch within the caller's private transaction."""
        if operation not in {"insert", "update", "delete"}:
            raise ValueError(f"Unknown write operation {operation}")
        dependent = operation != "insert" and any(
            c.referenced_constraint is not None
            and c.referenced_constraint.schema_path == table.path
            and c.referenced_constraint.owner_table == table.name
            for other in self.tables()
            for c in other.constraints
        )
        whole_table = dependent
        target_ids: set[int] = (
            set() if operation == "insert" else {int(v.as_py()) for v in batch.column(table.rowid_name)}
        )
        rows = self.rows(table, None if whole_table else target_ids) if whole_table or target_ids else []
        by_id = {int(r[table.rowid_name].as_py()): r for r in rows}
        next_id_term = next(self.snapshot.store.quads_for_pattern(table.node, NEXT_ID, None, CATALOG)).object
        next_id = int(cast(Literal, next_id_term).value)
        original_next = next_id
        old: list[dict[str, Any] | None] = []
        new: list[dict[str, Any] | None] = []
        changed: set[int] = set()
        after: dict[str, Any] | None
        for index in range(batch.num_rows):
            incoming = {name: batch.column(i)[index] for i, name in enumerate(batch.schema.names)}
            if operation == "insert":
                rowid = next_id
                next_id += 1
                before = None
                after = {f.name: incoming.get(f.name, pa.scalar(None, type=f.type)) for f in table.schema}
                after[table.rowid_name] = pa.scalar(rowid, type=pa.int64())
            else:
                rowid = int(incoming[table.rowid_name].as_py())
                if rowid in changed:
                    continue  # SQL joins can select the same UPDATE/DELETE target more than once.
                if rowid not in by_id:
                    raise ValueError(f"Write target rowid {rowid} does not exist")
                before = by_id[rowid]
                after = {**before, **incoming} if operation == "update" else None
            changed.add(rowid)
            if after is None:
                del by_id[rowid]
            else:
                # Coerce through the declared Arrow schema before accepting RPC values.
                normalized = rows_table([after], table.scan_schema).to_batches()[0]
                after = {f.name: normalized.column(i)[0] for i, f in enumerate(table.scan_schema)}
                by_id[rowid] = after
            old.append(before)
            new.append(after)
        candidates = list(by_id.values())
        if not whole_table:
            matches = self._key_matches(table, candidates) - changed
            if matches:
                candidates += self.rows(table, matches)
        self.validate(table, candidates)
        if operation != "insert":
            self._validate_dependents(table, list(by_id.values()))
        deletes = " ".join(str(table.row_node(rowid)) for rowid in changed)
        statements: list[str] = []
        if changed and operation != "insert":
            statements.append(
                f"DELETE {{ GRAPH {table.graph} {{ ?row ?p ?o }} }} "
                f"WHERE {{ VALUES ?row {{ {deletes} }} GRAPH {table.graph} {{ ?row ?p ?o }} }}"
            )
        triples: list[str] = []
        for after in new:
            if after is None:
                continue
            rowid = int(after[table.rowid_name].as_py())
            subject = table.row_node(rowid)
            triples.extend([f"{subject} {ROW_ID} {Literal(rowid)} .", f"{subject} {RDF_TYPE} {table.node} ."])
            for field in table.schema:
                value = encode(after[field.name])
                if value is not None:
                    triples.append(f"{subject} {table.predicate(field.name)} {value} .")
        if triples:
            statements.append(f"INSERT DATA {{ GRAPH {table.graph} {{ {' '.join(triples)} }} }}")
        if next_id != original_next:
            statements.append(f"DELETE WHERE {{ GRAPH {CATALOG} {{ {table.node} {NEXT_ID} ?n }} }}")
            statements.append(f"INSERT DATA {{ GRAPH {CATALOG} {{ {table.node} {NEXT_ID} {Literal(next_id)} }} }}")
        if statements:
            self.snapshot.store.update("; ".join(statements))
        if changed:
            self._write_relationships(table, [r for r in new if r is not None], changed)
            self._write_keys(table, [r for r in new if r is not None], changed)
        return old, new

    def _key_hashes(
        self, table: Table, rows: list[dict[str, Any]], constraint: ConstraintInfo
    ) -> list[tuple[int, int]]:
        """Compute SQL-consistent buckets; collisions are resolved by exact SQL validation."""
        if not rows:
            return []
        columns = [identifier(p[0]) for p in constraint.definition.column_paths]
        with duckdb.connect(config={"enable_external_access": "false", "threads": "1"}) as connection:
            connection.register("keys", rows_table(rows, table.scan_schema))
            nonnull = " AND ".join(f"{c} IS NOT NULL" for c in columns)
            return [
                (int(row), int(bucket))
                for row, bucket in connection.execute(
                    f"SELECT {identifier(table.rowid_name)}, hash({', '.join(columns)}) FROM keys WHERE {nonnull}"
                ).fetchall()
            ]

    def _key_matches(self, table: Table, rows: list[dict[str, Any]]) -> set[int]:
        result: set[int] = set()
        for constraint in table.constraints:
            if constraint.definition.kind not in {ConstraintKind.PRIMARY_KEY, ConstraintKind.UNIQUE}:
                continue
            for bucket in {h for _, h in self._key_hashes(table, rows, constraint)}:
                for quad in self.snapshot.store.quads_for_pattern(
                    None, table.key_predicate(constraint), Literal(bucket), KEY_INDEX
                ):
                    result.add(int(cast(NamedNode, quad.subject).value.rsplit("/", 1)[1]))
        return result

    def _write_keys(self, table: Table, rows: list[dict[str, Any]], changed: set[int]) -> None:
        updates = []
        if changed:
            subjects = " ".join(str(table.row_node(i)) for i in changed)
            updates.append(
                f"DELETE {{ GRAPH {KEY_INDEX} {{ ?s ?p ?o }} }} WHERE {{ VALUES ?s {{ {subjects} }} "
                f"GRAPH {KEY_INDEX} {{ ?s ?p ?o }} }}"
            )
        triples = [
            f"{table.row_node(row)} {table.key_predicate(c)} {Literal(bucket)} ."
            for c in table.constraints
            if c.definition.kind in {ConstraintKind.PRIMARY_KEY, ConstraintKind.UNIQUE}
            for row, bucket in self._key_hashes(table, rows, c)
        ]
        if triples:
            updates.append(f"INSERT DATA {{ GRAPH {KEY_INDEX} {{ {' '.join(triples)} }} }}")
        if updates:
            self.snapshot.store.update("; ".join(updates))

    def _clear_keys(self, table: Table) -> None:
        self.snapshot.store.update(
            f"DELETE {{ GRAPH {KEY_INDEX} {{ ?s ?p ?o }} }} WHERE {{ GRAPH {KEY_INDEX} {{ ?s ?p ?o }} "
            f"FILTER(STRSTARTS(STR(?s), {Literal(table.node.value + '/row/')})) }}"
        )

    def rebuild_keys(self, table: Table) -> None:
        """Rebuild derived key buckets after a constraint or hash-format migration."""
        self._clear_keys(table)
        if any(c.definition.kind in {ConstraintKind.PRIMARY_KEY, ConstraintKind.UNIQUE} for c in table.constraints):
            self._write_keys(table, self.rows(table), set())

    def _write_relationships(self, table: Table, rows: list[dict[str, Any]], changed: set[int]) -> None:
        """Maintain derived row-to-row edges using the same SQL equality as foreign keys."""
        updates = []
        if changed:
            subjects = " ".join(str(table.row_node(i)) for i in changed)
            updates.append(
                f"DELETE {{ GRAPH {RELATIONSHIPS} {{ ?s ?p ?o }} }} WHERE {{ VALUES ?s {{ {subjects} }} "
                f"GRAPH {RELATIONSHIPS} {{ ?s ?p ?o }} }}"
            )
        triples: list[str] = []
        if rows:
            for constraint in table.constraints:
                fk = constraint.definition.foreign_key
                if fk is None:
                    continue
                parent = self.require(fk.referenced_table.schema_path, fk.referenced_table.name)
                with duckdb.connect(config={"enable_external_access": "false", "threads": "1"}) as connection:
                    connection.register("child", rows_table(rows, table.scan_schema))
                    connection.register("parent", rows_table(self.rows(parent), parent.scan_schema))
                    condition = " AND ".join(
                        f"c.{identifier(a[0])} = p.{identifier(b[0])}"
                        for a, b in zip(constraint.definition.column_paths, fk.referenced_column_paths, strict=True)
                    )
                    pairs = connection.execute(
                        f"SELECT c.{identifier(table.rowid_name)}, p.{identifier(parent.rowid_name)} "
                        f"FROM child c JOIN parent p ON {condition}"
                    ).fetchall()
                    triples.extend(
                        f"{table.row_node(a)} {table.relationship(constraint)} {parent.row_node(b)} ." for a, b in pairs
                    )
        if triples:
            updates.append(f"INSERT DATA {{ GRAPH {RELATIONSHIPS} {{ {' '.join(triples)} }} }}")
        if updates:
            self.snapshot.store.update("; ".join(updates))

    def rebuild_relationships(self, table: Table) -> None:
        """Regenerate a table's derived foreign-key edges after a definition change."""
        self.snapshot.store.update(
            f"DELETE {{ GRAPH {RELATIONSHIPS} {{ ?s ?p ?o }} }} WHERE {{ GRAPH {RELATIONSHIPS} {{ ?s ?p ?o }} "
            f"FILTER(STRSTARTS(STR(?s), {Literal(table.node.value + '/row/')})) }}"
        )
        if any(c.definition.foreign_key is not None for c in table.constraints):
            self._write_relationships(table, self.rows(table), set())
