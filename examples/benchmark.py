"""Measure a disposable RDF workload through stock DuckDB; emit JSON evidence.

Run with uv run python examples/benchmark.py --rows 100000 --output benchmark-results.json.
Timings include VGI transport, storage and commit costs; this is a workload sample, not an SLA.
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import duckdb
from graph_demo import sql_literal


def main() -> None:
    """Measure insert, scan, lookup, update, rollback, backup and reopen behavior."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 1 <= args.rows <= 10_000_000:
        parser.error("--rows must be between 1 and 10000000")
    worker = Path(sys.executable).parent / ("vgi-rdf-worker.exe" if sys.platform == "win32" else "vgi-rdf-worker")
    measurements: dict[str, Any] = {
        "rows": args.rows,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "engine": duckdb.__version__,
        "timings_seconds": {},
    }
    with TemporaryDirectory(prefix="vgi-rdf-benchmark-") as temporary, duckdb.connect() as sql:
        database = Path(temporary) / "data"
        sql.execute("SET allow_extension_repositories='allowed'")
        sql.execute(
            "CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm'"
        )
        sql.execute("INSTALL vgi FROM query_farm")
        sql.execute("LOAD vgi FROM query_farm")
        sql.execute(
            f"ATTACH 'rdf' (TYPE vgi, LOCATION {sql_literal(str(worker))}, database {sql_literal(str(database))})"
        )
        sql.execute("CREATE TABLE rdf.main.items(id BIGINT PRIMARY KEY, label VARCHAR, amount DOUBLE)")

        def run(name: str, query: str) -> list[tuple[Any, ...]]:
            start = time.perf_counter()
            result = sql.execute(query).fetchall()
            measurements["timings_seconds"][name] = round(time.perf_counter() - start, 6)
            return result

        assert run(
            "insert", f"INSERT INTO rdf.main.items SELECT i, 'item-'||i, i*0.5 FROM range({args.rows}) t(i)"
        ) == [(args.rows,)]
        assert run("aggregate_scan", "SELECT count(*),sum(id) FROM rdf.main.items") == [
            (args.rows, args.rows * (args.rows - 1) // 2)
        ]
        assert run("point_lookup", f"SELECT label FROM rdf.main.items WHERE id={args.rows - 1}") == [
            (f"item-{args.rows - 1}",)
        ]
        assert run("point_update", f"UPDATE rdf.main.items SET amount=amount+1 WHERE id={args.rows - 1}") == [(1,)]
        sql.execute("BEGIN")
        assert run("delete_then_rollback", f"DELETE FROM rdf.main.items WHERE id={args.rows - 1}") == [(1,)]
        sql.execute("ROLLBACK")
        assert sql.execute("SELECT count(*) FROM rdf.main.items").fetchone() == (args.rows,)
        from rdf_worker.admin import backup
        from rdf_worker.storage import Database

        start = time.perf_counter()
        backup(Database(database), Path(temporary) / "backup")
        measurements["timings_seconds"]["backup"] = round(time.perf_counter() - start, 6)
        measurements["storage_logical_bytes"] = sum(p.stat().st_size for p in database.rglob("*") if p.is_file())
        measurements["database_status"] = Database(database).status()
        sql.execute("DETACH rdf")
        sql.execute(
            f"ATTACH 'rdf' (TYPE vgi, LOCATION {sql_literal(str(worker))}, "
            f"database {sql_literal(str(Path(temporary) / 'backup'))})"
        )
        assert run("restore_scan", "SELECT count(*),sum(id) FROM rdf.main.items") == [
            (args.rows, args.rows * (args.rows - 1) // 2)
        ]
        sql.execute("DETACH rdf")
    measurements["status"] = "passed"
    rendered = json.dumps(measurements, indent=2)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
