"""Run SQL, RDF joins, SPARQL paths and traversal against a disposable local database.

Run with: uv run python examples/graph_demo.py
All data is temporary; the script asserts its results before printing them.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from rdf_worker.graphs import import_rdf
from rdf_worker.storage import Database


def sql_literal(value: str) -> str:
    """Quote a SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def main() -> None:
    """Exercise graph features through a real stock DuckDB/VGI connection."""
    worker = Path(sys.executable).parent / ("vgi-rdf-worker.exe" if os.name == "nt" else "vgi-rdf-worker")
    with TemporaryDirectory(prefix="vgi-rdf-demo-") as temporary, duckdb.connect() as sql:
        path = Path(temporary) / "database"
        sql.execute("SET allow_extension_repositories='allowed'")
        sql.execute(
            "CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm'"
        )
        sql.execute("INSTALL vgi FROM query_farm")
        sql.execute("LOAD vgi FROM query_farm")
        sql.execute(f"ATTACH 'rdf' (TYPE vgi, LOCATION {sql_literal(str(worker))}, database {sql_literal(str(path))})")
        sql.execute("CREATE TABLE rdf.main.people(id INTEGER PRIMARY KEY, name VARCHAR)")
        sql.execute("CREATE TABLE rdf.main.orders(id INTEGER, buyer INTEGER REFERENCES rdf.main.people(id))")
        sql.execute("INSERT INTO rdf.main.people VALUES (1,'Ada'),(2,'Grace')")
        sql.execute("INSERT INTO rdf.main.orders VALUES (10,1),(11,1),(12,2)")
        relationships = "SELECT ?order ?buyer WHERE { GRAPH <urn:vgi:rdf:relationships> { ?order ?p ?buyer } }"
        counts = sql.execute(
            "SELECT p.name,count(*) AS orders FROM rdf.main.rdf_sparql(" + sql_literal(relationships) + ") g "
            "JOIN rdf.main.rdf_rows('main','people') p ON g.buyer.value=p.__rdf_iri GROUP BY p.name ORDER BY p.name"
        ).fetchall()
        assert counts == [("Ada", 2), ("Grace", 1)]
        print("SQL rows joined to RDF foreign-key edges:", json.dumps(counts))
        order_iri = sql.execute("SELECT __rdf_iri FROM rdf.main.rdf_rows('main','orders') WHERE id=10").fetchone()
        assert order_iri is not None
        edges = sql.execute("SELECT neighbor FROM rdf.main.rdf_neighbors(" + sql_literal(order_iri[0]) + ")").fetchall()
        assert len(edges) == 1
        import_rdf(
            Database(path),
            Path(__file__).with_name("network.ttl"),
            format_name="turtle",
            graph_iri="https://example.org/network",
        )
        reach = sql.execute(
            "SELECT node,depth FROM rdf.main.rdf_reachable('https://example.org/alice', "
            "graph_iri := 'https://example.org/network', max_depth := 3) ORDER BY depth,node"
        ).fetchall()
        assert reach == [
            ("https://example.org/bob", 1),
            ("https://example.org/dana", 1),
            ("https://example.org/carol", 2),
        ]
        print("Cycle-safe reachability and minimum distance:", json.dumps(reach))
        paths = (
            "PREFIX ex: <https://example.org/> SELECT ?person ?name WHERE { "
            "GRAPH ex:network { ex:alice ex:knows+ ?person . ?person ex:name ?name } } ORDER BY ?person"
        )
        names = sql.execute(
            "SELECT person.value, name.value, name.language FROM rdf.main.rdf_sparql(" + sql_literal(paths) + ")"
        ).fetchall()
        assert ("https://example.org/carol", "Carole", "fr") in names
        print("SPARQL property paths and language tags:", json.dumps(names))
        sql.execute("BEGIN")
        sql.execute("DELETE FROM rdf.main.orders WHERE buyer=1")
        sql.execute("ROLLBACK")
        assert sql.execute("SELECT count(*) FROM rdf.main.orders").fetchone() == (3,)
        sql.execute("DETACH rdf")
        print("All graph examples passed.")


if __name__ == "__main__":
    main()
