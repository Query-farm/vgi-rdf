from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from pyoxigraph import Literal, NamedNode, Quad, RdfFormat, Store

from rdf_worker.admin import backup
from rdf_worker.storage import Database


def test_independent_backup_and_rdf_export(tmp_path: Path) -> None:
    db = Database(tmp_path / "source")
    quad = Quad(NamedNode("urn:s"), NamedNode("urn:p"), Literal("hello"), NamedNode("urn:g"))
    with db.access(write=True) as snapshot:
        snapshot.store.add(quad)
    backup(db, tmp_path / "backup")
    with db.access(write=True) as snapshot:
        snapshot.store.remove(quad)
    copy = Database(tmp_path / "backup")
    with copy.access() as snapshot:
        assert quad in snapshot.store
    output = tmp_path / "out.nq"
    subprocess.run(
        [sys.executable, "-m", "rdf_worker.admin", str(copy.path), "export", str(output)], check=True, timeout=20
    )
    store = Store()
    store.load(path=output, format=RdfFormat.N_QUADS)
    assert quad in store
    with pytest.raises(FileExistsError):
        backup(db, tmp_path / "backup")


def test_admin_does_not_create_a_missing_database(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    result = subprocess.run(
        [sys.executable, "-m", "rdf_worker.admin", str(missing), "vacuum"], capture_output=True, text=True, timeout=20
    )
    assert result.returncode != 0
    assert "database does not exist" in result.stderr
    assert not missing.exists()
