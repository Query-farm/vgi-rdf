"""Read-only consistency checks for SQL data and its derived RDF graphs."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from pyoxigraph import Literal, Quad

from .model import KEY_INDEX, RDF_TYPE, RELATIONSHIPS, ROW_ID, RelationalGraph
from .storage import Database, Snapshot


def check(database: Database) -> dict[str, Any]:
    """Verify rows, SQL constraints and rebuilt indexes against one committed snapshot."""
    token = database.begin()
    counts = {"tables": 0, "rows": 0}
    try:
        with database.access(token) as snapshot, TemporaryDirectory(prefix="vgi-rdf-check-") as temporary:
            graph = RelationalGraph(snapshot)
            tables = graph.tables()
            for table in tables:
                rows = graph.rows(table)
                graph.validate(table, rows)
                identities = {int(row[table.rowid_name].as_py()) for row in rows}
                if len(identities) != len(rows):
                    raise ValueError(f"Duplicate RDF cell or row identity in {table.name}")
                if any(
                    Quad(table.row_node(i), RDF_TYPE, table.node, table.graph) not in snapshot.store for i in identities
                ):
                    raise ValueError(f"Missing RDF row type in {table.name}")
                predicates = {ROW_ID, RDF_TYPE, *(table.predicate(field.name) for field in table.schema)}
                for quad in snapshot.store.quads_for_pattern(None, None, None, table.graph):
                    if quad.predicate not in predicates:
                        raise ValueError(f"Unknown row predicate in {table.name}")
                    identity = list(snapshot.store.quads_for_pattern(quad.subject, ROW_ID, None, table.graph))
                    if len(identity) != 1 or not isinstance(identity[0].object, Literal):
                        raise ValueError(f"Missing or duplicate RDF row identity in {table.name}")
                    row_id = int(identity[0].object.value)
                    if row_id not in identities or quad.subject != table.row_node(row_id):
                        raise ValueError(f"Invalid RDF row resource in {table.name}")
                    if quad.predicate == RDF_TYPE and quad.object != table.node:
                        raise ValueError(f"Invalid RDF row type in {table.name}")
                counts["tables"] += 1
                counts["rows"] += len(rows)
            copy_path = Path(temporary) / "copy"
            snapshot.store.backup(str(copy_path))
            copy = Snapshot(copy_path, writable=True)
            try:
                copy.store.remove_graph(KEY_INDEX)
                copy.store.remove_graph(RELATIONSHIPS)
                rebuilt = RelationalGraph(copy)
                for table in rebuilt.tables():
                    rebuilt.rebuild_keys(table)
                    rebuilt.rebuild_relationships(table)
                for name in [KEY_INDEX, RELATIONSHIPS]:
                    actual = set(snapshot.store.quads_for_pattern(None, None, None, name))
                    expected = set(copy.store.quads_for_pattern(None, None, None, name))
                    if actual != expected:
                        raise ValueError(f"Derived RDF graph is inconsistent: {name.value}")
            finally:
                copy.close()
        return {"status": "ok", **counts}
    finally:
        database.rollback(token)
