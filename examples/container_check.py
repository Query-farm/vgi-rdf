"""Verify the built deployment image through SQL, then remove its disposable volume.

Run docker build -t vgi-rdf:local . followed by uv run python examples/container_check.py.
Requires Docker Compose and the project's stock DuckDB dependency.
"""

from __future__ import annotations

import json
import os
import secrets
import socket
import subprocess
import time
import uuid
from pathlib import Path

import duckdb
from graph_demo import sql_literal


def main() -> None:
    """Test authenticated SQL, graph queries, backup, restart and enforced read-only mode."""
    project = "vgi-rdf-test-" + uuid.uuid4().hex[:12]
    token = secrets.token_urlsafe(32)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    environment = {
        **os.environ,
        "VGI_BEARER_TOKENS": token + "=test",
        "VGI_SIGNING_KEY": secrets.token_hex(32),
        "VGI_RDF_PORT": str(port),
        "VGI_RDF_READ_ONLY": "false",
    }
    compose = ["docker", "compose", "-p", project, "-f", str(Path(__file__).parents[1] / "deploy/compose.yaml")]

    def run(*arguments: str) -> str:
        result = subprocess.run([*compose, *arguments], env=environment, capture_output=True, text=True, timeout=120)
        if result.returncode:
            raise RuntimeError(result.stderr.replace(token, "<redacted>"))
        return result.stdout

    def ready() -> None:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            container = run("ps", "-q", "rdf").strip()
            if container:
                health = subprocess.run(
                    ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                ).stdout.strip()
                if health == "healthy":
                    return
            time.sleep(0.2)
        raise RuntimeError("Container did not become healthy")

    def attach(sql: duckdb.DuckDBPyConnection, database: str = "/data/default") -> None:
        sql.execute("LOAD vgi FROM query_farm")
        sql.execute(
            f"ATTACH 'rdf' (TYPE vgi, LOCATION 'http://127.0.0.1:{port}', "
            f"bearer_token {sql_literal(token)}, database {sql_literal(database)}, read_only false)"
        )

    try:
        run("up", "--no-build", "-d")
        ready()
        with duckdb.connect() as sql:
            sql.execute("SET allow_extension_repositories='allowed'")
            sql.execute(
                "CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm "
                "WITH PREFIX 'https://duckdb-extensions.query.farm'"
            )
            sql.execute("INSTALL vgi FROM query_farm")
            sql.execute("LOAD vgi FROM query_farm")
            try:
                sql.execute(f"ATTACH 'rdf' (TYPE vgi, LOCATION 'http://127.0.0.1:{port}', bearer_token 'incorrect')")
            except duckdb.Error:
                pass
            else:
                raise AssertionError("Invalid credentials were accepted")
            attach(sql)
            sql.execute("CREATE TABLE rdf.main.people(id INT PRIMARY KEY, name VARCHAR)")
            sql.execute("CREATE TABLE rdf.main.orders(buyer INT REFERENCES rdf.main.people(id))")
            sql.execute("INSERT INTO rdf.main.people VALUES(1,'Ada')")
            sql.execute("INSERT INTO rdf.main.orders VALUES(1)")
            assert sql.execute(
                "SELECT answer FROM rdf.main.rdf_sparql('ASK { GRAPH <urn:vgi:rdf:relationships> { ?s ?p ?o } }')"
            ).fetchone() == (True,)
            sql.execute("DETACH rdf")
        report = json.loads(run("exec", "-T", "rdf", "vgi-rdf-admin", "/data/default", "check"))
        assert report == {"status": "ok", "tables": 2, "rows": 2}
        run("exec", "-T", "rdf", "vgi-rdf-admin", "/data/default", "backup", "/data/restore")
        run("restart", "rdf")
        ready()
        for database in ["/data/default", "/data/restore"]:
            with duckdb.connect() as sql:
                attach(sql, database)
                assert sql.execute("SELECT id,name FROM rdf.main.people").fetchall() == [(1, "Ada")]
                sql.execute("DETACH rdf")
        environment["VGI_RDF_READ_ONLY"] = "true"
        run("up", "--no-build", "-d", "--force-recreate")
        ready()
        with duckdb.connect() as sql:
            attach(sql)
            assert sql.execute("SELECT count(*) FROM rdf.main.orders").fetchone() == (1,)
            try:
                sql.execute("INSERT INTO rdf.main.people VALUES(2,'Grace')")
            except duckdb.Error as error:
                assert "read-only" in str(error)
            else:
                raise AssertionError("Client bypassed the server read-only policy")
            sql.execute("DETACH rdf")
        run("exec", "-T", "rdf", "vgi-rdf-admin", "/data/default", "reap", "--idle-seconds", "3600")
        run("exec", "-T", "rdf", "vgi-rdf-admin", "/data/default", "vacuum", "--receipts-older-than", "604800")
        print("Container SQL, authentication, graph, integrity, backup, restart and read-only checks passed.")
    finally:
        run("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
