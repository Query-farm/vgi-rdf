"""Shared fixtures: a worker client and the local mock API."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest
from sql_support import connection
from vgi.client import Client

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def client() -> Iterator[Client]:
    """A client that launches the worker as a subprocess, the way DuckDB does."""
    with Client([sys.executable, "-m", "rdf_worker.starter"]) as c:
        yield c


@pytest.fixture(scope="session")
def mock_api() -> Iterator[str]:
    """Start test/mock_api/server.py on a free port and return its base URL."""
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "test" / "mock_api" / "server.py")],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().strip()
    assert line.startswith("PORT:"), line
    yield f"http://127.0.0.1:{line.removeprefix('PORT:')}"
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture(scope="session", autouse=True)
def install_extension() -> None:
    with duckdb.connect() as c:
        c.execute("SET allow_extension_repositories='allowed'")
        c.execute(
            "CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm'"
        )
        c.execute("INSTALL vgi FROM query_farm")


@pytest.fixture
def sql(tmp_path: Path) -> Iterator[Any]:
    with connection(tmp_path / "rdf") as c:
        yield c
