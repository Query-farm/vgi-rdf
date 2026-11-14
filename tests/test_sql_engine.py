from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest
from sql_support import connection


def test_native_crud_defaults_returning(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.t (id INT PRIMARY KEY, name VARCHAR NOT NULL, n INT DEFAULT 7)")
    assert sql.execute("INSERT INTO rdf.main.t (id,name) VALUES (1,'Ada'), (2,'Grace') RETURNING *").fetchall() == [
        (1, "Ada", 7),
        (2, "Grace", 7),
    ]
    assert sql.execute("UPDATE rdf.main.t SET n=n+1 WHERE id=1 RETURNING id,n").fetchall() == [(1, 8)]
    assert sql.execute("DELETE FROM rdf.main.t WHERE id=2").fetchall() == [(1,)]
    assert sql.execute("SELECT * FROM rdf.main.t").fetchall() == [(1, "Ada", 8)]
    sql.execute("DROP TABLE rdf.main.t")
    sql.execute("DROP TABLE IF EXISTS rdf.main.t")


def test_duplicate_and_all_null_rows(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.t (a INT, b VARCHAR)")
    sql.execute("INSERT INTO rdf.main.t VALUES (NULL,NULL),(NULL,NULL),(3,'x'),(3,'x')")
    assert sql.execute("SELECT count(*),count(a) FROM rdf.main.t").fetchall() == [(4, 2)]
    assert sql.execute("UPDATE rdf.main.t SET b=NULL WHERE a=3").fetchall() == [(2,)]
    assert sql.execute("DELETE FROM rdf.main.t WHERE a IS NULL").fetchall() == [(2,)]
    assert sql.execute("SELECT DISTINCT a,b FROM rdf.main.t").fetchall() == [(3, None)]


def test_multi_batch_and_empty_projection(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.t AS SELECT i::INTEGER AS id, i*2 AS n FROM range(6200) t(i)")
    assert sql.execute("SELECT count(*),sum(n) FROM rdf.main.t").fetchall() == [(6200, 6200 * 6199)]
    assert sql.execute("SELECT 42 FROM rdf.main.t LIMIT 3").fetchall() == [(42,)] * 3
    assert sql.execute("UPDATE rdf.main.t SET n=n+1 WHERE id%2=0").fetchall() == [(3100,)]
    assert sql.execute("DELETE FROM rdf.main.t WHERE id>=5000").fetchall() == [(1200,)]
    assert sql.execute("SELECT count(*) FROM rdf.main.t").fetchall() == [(5000,)]


def test_query_composition_join_aggregate_window(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.a AS SELECT i AS id, i%2 AS grp FROM range(8) t(i)")
    sql.execute("CREATE TABLE rdf.main.b AS SELECT 0 AS grp, 'even' AS label UNION ALL SELECT 1,'odd'")
    assert sql.execute(
        "SELECT b.label,sum(a.id) FROM rdf.main.a a JOIN rdf.main.b b USING(grp) GROUP BY b.label ORDER BY b.label"
    ).fetchall() == [("even", 12), ("odd", 16)]
    assert sql.execute(
        "SELECT id,row_number() OVER(ORDER BY id) FROM rdf.main.a WHERE id>5 ORDER BY id"
    ).fetchall() == [(6, 1), (7, 2)]


def test_ddl_and_dml_rollback(sql: Any) -> None:
    sql.execute("BEGIN")
    sql.execute("CREATE TABLE rdf.main.t (n INTEGER)")
    sql.execute("INSERT INTO rdf.main.t VALUES (1)")
    assert sql.execute("SELECT n FROM rdf.main.t").fetchall() == [(1,)]
    sql.execute("ROLLBACK")
    with pytest.raises(duckdb.Error, match="does not exist"):
        sql.execute("SELECT * FROM rdf.main.t")
    sql.execute("CREATE TABLE rdf.main.t (n INTEGER)")
    sql.execute("INSERT INTO rdf.main.t VALUES (1)")
    sql.execute("BEGIN")
    sql.execute("UPDATE rdf.main.t SET n=2")
    sql.execute("DROP TABLE rdf.main.t")
    sql.execute("ROLLBACK")
    assert sql.execute("SELECT * FROM rdf.main.t").fetchall() == [(1,)]


def test_failed_multi_batch_insert_is_atomic(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.t (n BIGINT PRIMARY KEY)")
    sql.execute("INSERT INTO rdf.main.t VALUES (6500)")
    with pytest.raises(duckdb.Error, match=r"PRIMARY_KEY|constraint|duplicate"):
        sql.execute("INSERT INTO rdf.main.t SELECT i FROM range(7000) t(i)")
    assert sql.execute("SELECT n FROM rdf.main.t").fetchall() == [(6500,)]


@pytest.mark.parametrize(
    ("definition", "bad"),
    [
        ("n INT NOT NULL", "NULL"),
        ("n INT PRIMARY KEY", "1"),
        ("n INT UNIQUE", "1"),
    ],
)
def test_constraints_reject_and_preserve_previous_data(sql: Any, definition: str, bad: str) -> None:
    sql.execute(f"CREATE TABLE rdf.main.t ({definition})")
    sql.execute("INSERT INTO rdf.main.t VALUES (1)")
    with pytest.raises(duckdb.Error, match=r"constraint|NOT NULL"):
        sql.execute(f"INSERT INTO rdf.main.t VALUES ({bad})")
    assert sql.execute("SELECT * FROM rdf.main.t").fetchall() == [(1,)]


def test_composite_unique_nulls_and_checks(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.t (a INT, b INT, UNIQUE(a,b))")
    sql.execute("INSERT INTO rdf.main.t VALUES (NULL,1),(NULL,1),(1,NULL),(1,NULL),(1,2)")
    with pytest.raises(duckdb.Error, match="UNIQUE"):
        sql.execute("INSERT INTO rdf.main.t VALUES (1,2)")
    assert sql.execute("SELECT count(*) FROM rdf.main.t").fetchone() == (5,)


def test_foreign_keys_parent_and_child_paths(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.parent (id INT PRIMARY KEY)")
    sql.execute("CREATE TABLE rdf.main.child (pid INT REFERENCES rdf.main.parent(id))")
    sql.execute("INSERT INTO rdf.main.parent VALUES (1),(2)")
    sql.execute("INSERT INTO rdf.main.child VALUES (1),(NULL)")
    for query in [
        "INSERT INTO rdf.main.child VALUES (9)",
        "UPDATE rdf.main.child SET pid=9",
        "DELETE FROM rdf.main.parent WHERE id=1",
        "UPDATE rdf.main.parent SET id=10 WHERE id=1",
        "DROP TABLE rdf.main.parent",
    ]:
        with pytest.raises(duckdb.Error, match=r"FOREIGN_KEY|foreign.key|dependenc"):
            sql.execute(query)
    assert sql.execute("DELETE FROM rdf.main.parent WHERE id=2").fetchone() == (1,)
    sql.execute("DELETE FROM rdf.main.child WHERE pid=1")
    sql.execute("DELETE FROM rdf.main.parent")


def test_if_not_exists_replace_and_ctas(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.t AS SELECT 1 AS n")
    sql.execute("CREATE TABLE IF NOT EXISTS rdf.main.t AS SELECT 2 AS n")
    assert sql.execute("SELECT * FROM rdf.main.t").fetchall() == [(1,)]
    sql.execute("CREATE OR REPLACE TABLE rdf.main.t AS SELECT 3 AS n")
    assert sql.execute("SELECT * FROM rdf.main.t").fetchall() == [(3,)]


def test_quoted_identifiers_schemas_and_internal_rowid_collision(sql: Any) -> None:
    sql.execute('CREATE SCHEMA rdf."odd.schema"')
    sql.execute('CREATE TABLE rdf."odd.schema"."a.b" ("x.y" INT, "quote""name" VARCHAR, __rdf_rowid BIGINT)')
    sql.execute('INSERT INTO rdf."odd.schema"."a.b" VALUES (1,\'☃\',99)')
    sql.execute('UPDATE rdf."odd.schema"."a.b" SET "x.y"=2')
    assert sql.execute('SELECT * FROM rdf."odd.schema"."a.b"').fetchall() == [(2, "☃", 99)]
    with pytest.raises(duckdb.Error, match=r"not empty|dependenc"):
        sql.execute('DROP SCHEMA rdf."odd.schema"')
    sql.execute('DROP SCHEMA rdf."odd.schema" CASCADE')


@pytest.mark.parametrize(
    ("sql_type", "expression"),
    [
        ("TINYINT", "-128"),
        ("UTINYINT", "255"),
        ("SMALLINT", "-32768"),
        ("USMALLINT", "65535"),
        ("INTEGER", "-2147483648"),
        ("UINTEGER", "4294967295"),
        ("BIGINT", "-9223372036854775808"),
        ("UBIGINT", "18446744073709551615"),
        ("HUGEINT", "99999999999999999999999999999999999999"),
        ("BOOLEAN", "true"),
        ("FLOAT", "1.25"),
        ("DOUBLE", "'NaN'"),
        ("DOUBLE", "'Infinity'"),
        ("DECIMAL(38,10)", "'1234567890123456789012345678.1234567890'"),
        ("VARCHAR", "'hello ☃'"),
        ("BLOB", "from_hex('00FF4142')"),
        ("DATE", "'2024-02-29'"),
        ("DATE", "'infinity'"),
        ("TIME", "'23:59:59.123456'"),
        ("TIMESTAMP", "'2024-01-02 03:04:05.123456'"),
        ("TIMESTAMP_NS", "'2024-01-02 03:04:05.123456789'"),
        ("TIMESTAMPTZ", "'2024-01-02 03:04:05.123456+00'"),
        ("INTERVAL", "INTERVAL '3 months 2 days 1 microsecond'"),
        ("UUID", "'12345678-1234-5678-1234-567812345678'"),
        ("INTEGER[]", "[1,NULL,3]"),
        ("INTEGER[3]", "[1,NULL,3]"),
        ("STRUCT(a INTEGER,b VARCHAR)", "{'a':4,'b':'text'}"),
        ("MAP(VARCHAR, INTEGER)", "map(['a','b'],[1,NULL])"),
    ],
)
def test_sql_type_roundtrip(sql: Any, sql_type: str, expression: str) -> None:
    sql.execute(f"CREATE TABLE rdf.main.t (v {sql_type})")
    sql.execute(f"INSERT INTO rdf.main.t VALUES (CAST({expression} AS {sql_type})), (NULL)")
    result = sql.execute(
        f"SELECT count(*) FROM rdf.main.t WHERE v IS NOT DISTINCT FROM CAST({expression} AS {sql_type})"
    ).fetchone()
    assert result == (1,)
    assert sql.execute("SELECT count(*) FROM rdf.main.t WHERE v IS NULL").fetchone() == (1,)


def test_persistent_reattach_and_read_only(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with connection(path) as c:
        c.execute("CREATE TABLE rdf.main.t (n INT)")
        c.execute("INSERT INTO rdf.main.t VALUES (42)")
    with connection(path, read_only=True) as c:
        assert c.execute("SELECT * FROM rdf.main.t").fetchall() == [(42,)]
        for query in [
            "INSERT INTO rdf.main.t VALUES(2)",
            "UPDATE rdf.main.t SET n=2",
            "DELETE FROM rdf.main.t",
            "DROP TABLE rdf.main.t",
            "CREATE TABLE rdf.main.x(n INT)",
        ]:
            with pytest.raises(duckdb.Error, match=r"read.only"):
                c.execute(query)


def test_two_connections_snapshot_and_conflict(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with connection(path) as a, connection(path) as b:
        a.execute("CREATE TABLE rdf.main.t(n INT)")
        a.execute("INSERT INTO rdf.main.t VALUES(1)")
        a.execute("BEGIN")
        b.execute("BEGIN")
        assert a.execute("SELECT * FROM rdf.main.t").fetchall() == [(1,)]
        assert b.execute("SELECT * FROM rdf.main.t").fetchall() == [(1,)]
        a.execute("UPDATE rdf.main.t SET n=2")
        a.execute("COMMIT")
        assert b.execute("SELECT * FROM rdf.main.t").fetchall() == [(1,)]
        b.execute("UPDATE rdf.main.t SET n=3")
        with pytest.raises(duckdb.Error, match="serialization conflict"):
            b.execute("COMMIT")
        assert a.execute("SELECT * FROM rdf.main.t").fetchall() == [(2,)]


def test_rdf_terms_are_visible(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.t(n INT)")
    sql.execute("INSERT INTO rdf.main.t VALUES (42),(NULL)")
    assert sql.execute(
        "SELECT count(*) FROM rdf.main.rdf_quads() WHERE predicate='<urn:vgi:rdf:rowId>'"
    ).fetchone() == (2,)
    assert sql.execute(
        "SELECT count(*) FROM rdf.main.rdf_quads() WHERE object='\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>'"
    ).fetchone() == (1,)


def test_scan_is_frozen_for_self_insert(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.t AS SELECT i AS n FROM range(4100) t(i)")
    assert sql.execute("INSERT INTO rdf.main.t SELECT n+4100 FROM rdf.main.t").fetchone() == (4100,)
    assert sql.execute("SELECT count(*),max(n) FROM rdf.main.t").fetchone() == (8200, 8199)


def test_stock_engine_limits_are_explicit(sql: Any) -> None:
    with pytest.raises(duckdb.NotImplementedException, match="canonical CHECK"):
        sql.execute("CREATE TABLE rdf.main.checked(n INT CHECK(n>0))")
    sql.execute("CREATE TABLE rdf.main.t(n INT)")
    sql.execute("INSERT INTO rdf.main.t VALUES(1)")
    with pytest.raises(duckdb.BinderException, match="GetDeleteReturningVirtualColumns"):
        sql.execute("DELETE FROM rdf.main.t RETURNING *")
    assert sql.execute("SELECT * FROM rdf.main.t").fetchall() == [(1,)]


def test_graph_filter(sql: Any) -> None:
    # The initial graph contains the main schema and the derived-index format marker.
    assert sql.execute("SELECT count(*) FROM rdf.main.rdf_quads(graph_iri := 'urn:vgi:rdf:catalog')").fetchone() == (2,)
    assert sql.execute("SELECT count(*) FROM rdf.main.rdf_quads(graph_iri := 'urn:missing')").fetchone() == (0,)


def test_drop_parent_cascade_preserves_child_table(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.p(id INT PRIMARY KEY)")
    sql.execute("CREATE TABLE rdf.main.c(id INT REFERENCES rdf.main.p(id))")
    sql.execute("INSERT INTO rdf.main.p VALUES(1)")
    sql.execute("INSERT INTO rdf.main.c VALUES(1)")
    sql.execute("DROP TABLE rdf.main.p CASCADE")
    assert sql.execute("SELECT * FROM rdf.main.c").fetchall() == [(1,)]
    sql.execute("INSERT INTO rdf.main.c VALUES(99)")
    assert sql.execute("SELECT id FROM rdf.main.c ORDER BY id").fetchall() == [(1,), (99,)]


def test_upsert_uses_persisted_keys(sql: Any) -> None:
    sql.execute("CREATE TABLE rdf.main.t(id INT PRIMARY KEY, n INT)")
    sql.execute("INSERT INTO rdf.main.t VALUES(1,10)")
    sql.execute("INSERT INTO rdf.main.t VALUES(1,99) ON CONFLICT DO NOTHING")
    assert sql.execute("SELECT * FROM rdf.main.t").fetchall() == [(1, 10)]
    sql.execute("INSERT INTO rdf.main.t VALUES(1,20) ON CONFLICT(id) DO UPDATE SET n=excluded.n")
    assert sql.execute("SELECT * FROM rdf.main.t").fetchall() == [(1, 20)]


@pytest.mark.parametrize("value", ["infinity", "-infinity"])
def test_infinite_timestamp_roundtrip(sql: Any, value: str) -> None:
    sql.execute("CREATE TABLE rdf.main.t(v TIMESTAMP)")
    sql.execute(f"INSERT INTO rdf.main.t VALUES ('{value}')")
    assert sql.execute(f"SELECT v = '{value}'::TIMESTAMP FROM rdf.main.t").fetchone() == (True,)
