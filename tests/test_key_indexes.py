from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest
from pyoxigraph import Literal, Quad
from sql_support import connection
from vgi.catalog import OnConflict
from vgi.catalog.constraints import ConstraintDefinition, ConstraintInfo, ConstraintKind

from rdf_worker.graphs import DATASET, MAPPING_VERSION, upgrade
from rdf_worker.model import CATALOG, KEY_INDEX, RelationalGraph, Table
from rdf_worker.storage import Database


@pytest.mark.parametrize(
    "type_name,first,duplicate",
    [
        ("DOUBLE", "'-0.0'::DOUBLE", "0.0"),
        ("DOUBLE", "'NaN'::DOUBLE", "'NaN'::DOUBLE"),
        ("DECIMAL(18,8)", "0.00000001", "0.00000001"),
        ("VARCHAR", "'café'", "'café'"),
        ("INTEGER[]", "[1,NULL,3]", "[1,NULL,3]"),
    ],
)
def test_key_indexes_preserve_sql_equality(sql: Any, type_name: str, first: str, duplicate: str) -> None:
    sql.execute(f"CREATE TABLE rdf.main.t(k {type_name} UNIQUE)")
    sql.execute(f"INSERT INTO rdf.main.t VALUES ({first})")
    with pytest.raises(duckdb.Error, match="UNIQUE"):
        sql.execute(f"INSERT INTO rdf.main.t VALUES ({duplicate})")
    assert sql.execute("SELECT count(*) FROM rdf.main.t").fetchone() == (1,)


def test_hash_collisions_use_exact_sql_validation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = Database(tmp_path)

    def constant_hash(
        self: RelationalGraph, table: Table, rows: list[dict[str, Any]], constraint: ConstraintInfo
    ) -> list[tuple[int, int]]:
        return [(int(r[table.rowid_name].as_py()), 1) for r in rows]

    monkeypatch.setattr(RelationalGraph, "_key_hashes", constant_hash)
    with db.access(write=True) as snapshot:
        graph = RelationalGraph(snapshot)
        graph.create(
            ["main"],
            "t",
            pa.schema([("k", pa.int64())]),
            [ConstraintDefinition(kind=ConstraintKind.UNIQUE, column_paths=[["k"]])],
            OnConflict.ERROR,
        )
        table = graph.require(["main"], "t")
        graph.write(table, "insert", pa.record_batch([pa.array([1, 2, 3])], names=["k"]))
        graph.write(table, "insert", pa.record_batch([pa.array([4, 5])], names=["k"]))
    with pytest.raises(ValueError, match="UNIQUE"), db.access(write=True) as snapshot:
        graph = RelationalGraph(snapshot)
        graph.write(graph.require(["main"], "t"), "insert", pa.record_batch([pa.array([4])], names=["k"]))
    with db.access() as snapshot:
        assert len(RelationalGraph(snapshot).rows(RelationalGraph(snapshot).require(["main"], "t"))) == 5


def test_upgrade_rebuilds_old_database_indexes(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with connection(path) as c:
        c.execute("CREATE TABLE rdf.main.t(k INT PRIMARY KEY)")
        c.execute("INSERT INTO rdf.main.t VALUES(7)")
    database = Database(path)
    with database.access(write=True) as snapshot:
        snapshot.store.remove_graph(KEY_INDEX)
        snapshot.store.remove(Quad(DATASET, MAPPING_VERSION, Literal(2), CATALOG))
        snapshot.store.add(Quad(DATASET, MAPPING_VERSION, Literal(1), CATALOG))
    with pytest.raises(ValueError, match="upgrade"):
        upgrade(database, read_only=True)
    assert upgrade(database)
    assert not upgrade(database)
    with connection(path) as c, pytest.raises(duckdb.Error, match="PRIMARY_KEY"):
        c.execute("INSERT INTO rdf.main.t VALUES(7)")
