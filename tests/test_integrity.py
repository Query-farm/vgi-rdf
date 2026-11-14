from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from sql_support import WORKER, connection

from rdf_worker.codec import pack_schema
from rdf_worker.integrity import check
from rdf_worker.model import KEY_INDEX
from rdf_worker.sparql import TERM_TYPE
from rdf_worker.storage import Database


def test_integrity_detects_missing_derived_index_without_changing_data(tmp_path: Path) -> None:
    path = tmp_path / "database"
    with connection(path) as sql:
        sql.execute("CREATE TABLE rdf.main.parent(id INT PRIMARY KEY)")
        sql.execute("CREATE TABLE rdf.main.child(id INT REFERENCES rdf.main.parent(id))")
        sql.execute("INSERT INTO rdf.main.parent VALUES(7)")
        sql.execute("INSERT INTO rdf.main.child VALUES(7)")
    database = Database(path)
    version = database.version()
    assert check(database) == {"status": "ok", "tables": 2, "rows": 2}
    assert database.version() == version
    with database.access(write=True) as snapshot:
        snapshot.store.remove(next(snapshot.store.quads_for_pattern(None, None, None, KEY_INDEX)))
    with pytest.raises(ValueError, match="inconsistent"):
        check(database)


def test_query_child_enforces_its_own_deadline(tmp_path: Path) -> None:
    database = Database(tmp_path / "db")
    token = database.begin()
    name = database.freeze_scan(token)
    values = " ".join(str(i) for i in range(100))
    query = "SELECT (COUNT(*) AS ?n) WHERE { " + " ".join(f"VALUES ?{v} {{ {values} }}" for v in "abcdef") + " }"
    schema = pa.schema([pa.field("n", TERM_TYPE, metadata={b"comment": b"RDF term: kind, value, datatype, language."})])
    with database.scan_access(token, name) as snapshot:
        result = subprocess.run(
            [sys.executable, "-m", "rdf_worker.query_runner"],
            input=json.dumps(
                {
                    "query": query,
                    "path": str(snapshot.path),
                    "output": str(tmp_path / "result.arrow"),
                    "schema": pack_schema(schema),
                    "max_rows": 1,
                    "max_bytes": 1024 * 1024,
                    "_deadline_ms": 100,
                }
            ),
            text=True,
            capture_output=True,
            timeout=5,
        )
        assert result.returncode == 124
    database.rollback(token)


def test_scan_limit_failure_leaves_committed_rows_intact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "db"
    with connection(path) as sql:
        sql.execute("CREATE TABLE rdf.main.t AS SELECT * FROM range(5)")
    monkeypatch.setenv("VGI_RDF_MAX_SCAN_ROWS", "2")
    # A distinct launch command prevents the SDK pool from reusing a process
    # whose environment was captured before the operator limit changed.
    with (
        connection(path, worker=WORKER + " --log-level WARNING") as sql,
        pytest.raises(duckdb.Error, match="MAX_SCAN_ROWS"),
    ):
        sql.execute("SELECT * FROM rdf.main.t").fetchall()
    monkeypatch.delenv("VGI_RDF_MAX_SCAN_ROWS")
    with connection(path) as sql:
        assert sql.execute("SELECT count(*) FROM rdf.main.t").fetchone() == (5,)
