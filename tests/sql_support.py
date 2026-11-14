from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
WORKER = str(Path(sys.executable).parent / ("vgi-rdf-worker.exe" if os.name == "nt" else "vgi-rdf-worker"))


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connection(path: Path, *, read_only: bool = False, worker: str = WORKER) -> Any:
    c = duckdb.connect(config={"threads": 1})
    c.execute("LOAD vgi FROM query_farm")
    c.execute(
        f"ATTACH 'rdf' (TYPE vgi, LOCATION {literal(worker)}, "
        f"database {literal(str(path))}, read_only {str(read_only).lower()})"
    )
    return c
