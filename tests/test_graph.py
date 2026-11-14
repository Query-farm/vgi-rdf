from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest
from pyoxigraph import Literal, NamedNode, Quad
from sql_support import connection, literal

from rdf_worker.meta import FOREIGN_KEY_EDGES_SQL, ROWS_WITH_INCOMING_EDGES_SQL, TABLE_GRAPH_SIZES_SQL
from rdf_worker.sparql import describe_query, execute_query, validate_query
from rdf_worker.storage import Database


def test_sparql_types_and_query_forms(sql: Any) -> None:
    assert sql.execute("SELECT answer FROM rdf.main.rdf_sparql('ASK {}')").fetchall() == [(True,)]
    query = 'SELECT ?iri ?label ?number ?unbound WHERE { VALUES (?iri ?label ?number) { (<urn:x> "bonjour"@fr 12) } }'
    row = sql.execute(f"SELECT * FROM rdf.main.rdf_sparql({literal(query)})").fetchone()
    assert row[0] == {"kind": "iri", "value": "urn:x", "datatype": None, "language": None}
    assert row[1]["value"] == "bonjour" and row[1]["language"] == "fr"
    assert row[2]["value"] == "12" and row[2]["datatype"].endswith("#integer")
    assert row[3] is None
    query = 'CONSTRUCT { <urn:x> <urn:p> "hello" } WHERE {}'
    assert sql.execute(f"SELECT subject.value, object.value FROM rdf.main.rdf_sparql({literal(query)})").fetchall() == [
        ("urn:x", "hello")
    ]


def test_composed_metadata_examples(sql: Any) -> None:
    assert sql.execute(TABLE_GRAPH_SIZES_SQL).fetchall() == []
    assert sql.execute(FOREIGN_KEY_EDGES_SQL).fetchall() == []
    sql.execute("CREATE TABLE rdf.main.people(id INTEGER PRIMARY KEY, name VARCHAR)")
    sql.execute("CREATE TABLE rdf.main.orders(id INTEGER, buyer INTEGER REFERENCES rdf.main.people(id))")
    sql.execute("INSERT INTO rdf.main.people VALUES (1,'Ada'),(2,'Grace'),(3,'Linus')")
    sql.execute("INSERT INTO rdf.main.orders VALUES (10,1),(11,1),(12,2),(13,NULL)")
    sizes = sql.execute(TABLE_GRAPH_SIZES_SQL).fetchall()
    assert [(schema, table) for schema, table, _ in sizes] == [("main", "orders"), ("main", "people")]
    assert all(statements > 0 for *_, statements in sizes)
    assert sql.execute(FOREIGN_KEY_EDGES_SQL).fetchall() == [("main", "orders", "people", 3)]
    assert sql.execute(ROWS_WITH_INCOMING_EDGES_SQL).fetchall() == [("Ada", 2), ("Grace", 1), ("Linus", 0)]


def test_sql_row_graph_join_and_fk_lifecycle(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.people(id INT PRIMARY KEY, name VARCHAR)")
    sql.execute("CREATE TABLE rdf.main.orders(id INT, buyer INT REFERENCES rdf.main.people(id))")
    sql.execute("INSERT INTO rdf.main.people VALUES (1,'Ada'), (2,'Grace')")
    sql.execute("INSERT INTO rdf.main.orders VALUES (10,1),(11,2),(12,NULL)")
    identities = dict(sql.execute("SELECT id,__rdf_iri FROM rdf.main.rdf_rows('main','people')").fetchall())
    mapping = sql.execute("SELECT iri,target_iri FROM rdf.main.rdf_tables() WHERE kind='foreign_key'").fetchone()
    assert mapping[0].startswith("urn:vgi:rdf:table:")
    query = "SELECT ?child ?parent WHERE { GRAPH <urn:vgi:rdf:relationships> { ?child ?p ?parent } }"
    result = sql.execute(
        f"SELECT p.name FROM rdf.main.rdf_sparql({literal(query)}) g "
        "JOIN rdf.main.rdf_rows('main','people') p ON g.parent.value=p.__rdf_iri ORDER BY name"
    ).fetchall()
    assert result == [("Ada",), ("Grace",)]
    sql.execute("UPDATE rdf.main.people SET name='Augusta' WHERE id=1")
    assert dict(sql.execute("SELECT id,__rdf_iri FROM rdf.main.rdf_rows('main','people')").fetchall()) == identities
    sql.execute("BEGIN")
    sql.execute("UPDATE rdf.main.orders SET buyer=2 WHERE id=10")
    assert sql.execute(
        f"SELECT count(*) FROM rdf.main.rdf_sparql({literal(query)}) WHERE parent.value={literal(identities[2])}"
    ).fetchone() == (2,)
    sql.execute("ROLLBACK")
    sql.execute("DELETE FROM rdf.main.orders WHERE id=11")
    assert sql.execute(f"SELECT parent.value FROM rdf.main.rdf_sparql({literal(query)})").fetchall() == [
        (identities[1],)
    ]
    sql.execute("DROP TABLE rdf.main.people CASCADE")
    assert sql.execute(f"SELECT count(*) FROM rdf.main.rdf_sparql({literal(query)})").fetchone() == (0,)
    assert sql.execute("SELECT count(*) FROM rdf.main.orders").fetchone() == (2,)


def test_sparql_multi_batch_and_result_bound(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.t AS SELECT i FROM range(2500) t(i)")
    query = "SELECT ?s WHERE { GRAPH ?g { ?s <urn:vgi:rdf:rowId> ?id } } ORDER BY ?s"
    assert sql.execute(f"SELECT count(*) FROM rdf.main.rdf_sparql({literal(query)})").fetchone() == (2500,)
    with pytest.raises(duckdb.Error, match="max_rows"):
        sql.execute(f"SELECT * FROM rdf.main.rdf_sparql({literal(query)}, max_rows := 10)").fetchall()
    assert sql.execute("SELECT count(*) FROM rdf.main.t").fetchone() == (2500,)


def test_sparql_local_graph_scope(sql: Any) -> None:
    assert sql.execute("SELECT answer FROM rdf.main.rdf_sparql('ASK { ?s ?p ?o }')").fetchone() == (False,)
    assert sql.execute(
        "SELECT answer FROM rdf.main.rdf_sparql('ASK { ?s ?p ?o }', union_default := true)"
    ).fetchone() == (True,)
    with pytest.raises(duckdb.Error):
        sql.execute("SELECT * FROM rdf.main.rdf_sparql('DELETE WHERE { ?s ?p ?o }')").fetchall()


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * WHERE { SERVICE <https://example.invalid/> {?s ?p ?o} }",
        "SELECT * WHERE { SeRvIcE SILENT ?endpoint {?s ?p ?o} }",
        r"SELECT * WHERE { S\u0045RVICE <https://example.invalid/> {?s ?p ?o} }",
        "SELECT * WHERE { FILTER(?x < 2) SERVICE <https://example.invalid/> {?s ?p ?o} }",
    ],
)
def test_sparql_blocks_network_lexically(query: str) -> None:
    with pytest.raises(ValueError):
        validate_query(query)


def test_sparql_allows_keyword_text_inside_terms() -> None:
    validate_query('SELECT ("SERVICE" AS ?x) (<urn:SERVICE> AS ?y) WHERE {} # SERVICE')
    validate_query("SELECT ('''long SERVICE string''' AS ?x) WHERE {}")


def test_query_deadline_and_byte_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = Database(tmp_path)
    token = db.begin()
    with db.access(token, write=True) as snapshot:
        snapshot.store.add(Quad(NamedNode("urn:x"), NamedNode("urn:p"), Literal("x" * 1000)))
    name = db.freeze_scan(token)
    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
    schema = describe_query(query)
    with db.scan_access(token, name) as snapshot:
        monkeypatch.setenv("VGI_RDF_MAX_RESULT_BYTES", "100")
        with pytest.raises(ValueError, match="MAX_RESULT_BYTES"):
            execute_query(snapshot.path, query, schema, max_rows=10, timeout_ms=30000, union_default=False)
        assert not (snapshot.path.parent / (name + ".arrow")).exists()
        monkeypatch.delenv("VGI_RDF_MAX_RESULT_BYTES")
        values = " ".join(str(i) for i in range(100))
        expensive = (
            "SELECT (COUNT(*) AS ?n) WHERE { " + " ".join(f"VALUES ?{v} {{ {values} }}" for v in "abcdef") + " }"
        )
        start = time.monotonic()
        with pytest.raises(TimeoutError, match="deadline"):
            execute_query(snapshot.path, expensive, pa.schema([]), max_rows=1, timeout_ms=1000, union_default=False)
        assert time.monotonic() - start < 5
    db.rollback(token)


def test_row_iri_collision(sql: Any) -> None:
    sql.execute('CREATE TABLE rdf.main.t("__rdf_iri" VARCHAR)')
    sql.execute("INSERT INTO rdf.main.t VALUES('user value')")
    row = sql.execute("SELECT __rdf_iri,__rdf_iri_ FROM rdf.main.rdf_rows('main','t')").fetchone()
    assert row[0] == "user value" and row[1].startswith("urn:vgi:rdf:table:")


def test_import_traversal_cycles_and_graph_scope(tmp_path: Path) -> None:
    from rdf_worker.graphs import drop_graph, import_rdf

    path = tmp_path / "db"
    database = Database(path)
    source = tmp_path / "network.ttl"
    source.write_text(
        "@prefix ex: <urn:example:> . ex:a ex:knows ex:b, ex:d . ex:b ex:knows ex:c . "
        'ex:c ex:knows ex:a . ex:a ex:name "Alice" .'
    )
    assert import_rdf(database, source, format_name="turtle", graph_iri="urn:network") == 5
    with connection(path) as c:
        assert c.execute(
            "SELECT neighbor FROM rdf.main.rdf_neighbors('urn:example:a', graph_iri := 'urn:network') ORDER BY neighbor"
        ).fetchall() == [("urn:example:b",), ("urn:example:d",)]
        assert c.execute(
            "SELECT neighbor FROM rdf.main.rdf_neighbors('urn:example:a', "
            "graph_iri := 'urn:network', direction := 'in')"
        ).fetchall() == [("urn:example:c",)]
        assert c.execute(
            "SELECT node,depth FROM rdf.main.rdf_reachable('urn:example:a', "
            "graph_iri := 'urn:network', max_depth := 3) ORDER BY depth,node"
        ).fetchall() == [("urn:example:b", 1), ("urn:example:d", 1), ("urn:example:c", 2)]
        assert c.execute(
            "SELECT node FROM rdf.main.rdf_reachable('urn:example:a', "
            "graph_iri := 'urn:network', max_depth := 1) ORDER BY node"
        ).fetchall() == [("urn:example:b",), ("urn:example:d",)]
        assert c.execute("SELECT count(*) FROM rdf.main.rdf_neighbors('urn:example:a')").fetchone() == (0,)
        with pytest.raises(duckdb.Error, match="max_rows"):
            c.execute(
                "SELECT * FROM rdf.main.rdf_reachable('urn:example:a', graph_iri := 'urn:network', max_rows := 1)"
            ).fetchall()
    with pytest.raises(ValueError, match="reserved"):
        drop_graph(database, "urn:vgi:rdf:catalog")
    drop_graph(database, "urn:network")
    with connection(path) as c:
        assert c.execute(
            "SELECT count(*) FROM rdf.main.rdf_neighbors('urn:example:a', graph_iri := 'urn:network')"
        ).fetchone() == (0,)


def test_import_is_atomic_and_protects_managed_graphs(tmp_path: Path) -> None:
    from rdf_worker.graphs import import_rdf

    database = Database(tmp_path / "db")
    source = tmp_path / "data.nq"
    source.write_text("<urn:a> <urn:p> <urn:b> <urn:external> .")
    assert import_rdf(database, source, format_name="nquads", graph_iri="") == 1
    source.write_text("<urn:a> <urn:p> <urn:c> <urn:external> .\n<urn:s> <urn:p> <urn:o> <urn:vgi:rdf:catalog> .")
    with pytest.raises(ValueError, match="reserved"):
        import_rdf(database, source, format_name="nquads", graph_iri="")
    with database.access() as snapshot:
        assert list(snapshot.store.quads_for_pattern(NamedNode("urn:a"), None, None, NamedNode("urn:external"))) == [
            Quad(NamedNode("urn:a"), NamedNode("urn:p"), NamedNode("urn:b"), NamedNode("urn:external"))
        ]
    source.write_text("this is not RDF")
    with pytest.raises(SyntaxError):
        import_rdf(database, source, format_name="turtle", graph_iri="urn:external", replace=True)
    with database.access() as snapshot:
        assert len(list(snapshot.store.quads_for_pattern(None, None, None, NamedNode("urn:external")))) == 1
