from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest
from pyoxigraph import Literal, NamedNode, Quad, RdfFormat, Store
from vgi.catalog import OnConflict

from rdf_worker.model import RelationalGraph
from rdf_worker.storage import Database, TransactionConflictError


def create(db: Database, token: bytes | None, name: str = "items") -> None:
    with db.access(token, write=True) as snapshot:
        RelationalGraph(snapshot).create(["main"], name, pa.schema([("n", pa.int64())]), [], OnConflict.ERROR)


def names(db: Database, token: bytes | None = None) -> list[str]:
    with db.access(token) as snapshot:
        return sorted(t.name for t in RelationalGraph(snapshot).tables())


def test_commit_rollback_and_repeatable_reads(tmp_path: Path) -> None:
    db = Database(tmp_path)
    before = db.begin()
    writer = db.begin()
    create(db, writer)
    assert names(db, writer) == ["items"]
    assert names(db) == names(db, before) == []
    db.commit(writer)
    db.commit(writer)
    db.rollback(writer)
    assert names(db) == ["items"]
    assert names(db, before) == []
    db.rollback(before)
    db.rollback(before)


def test_write_conflict_never_loses_committed_data(tmp_path: Path) -> None:
    db = Database(tmp_path)
    a, b = db.begin(), db.begin()
    create(db, a, "a")
    create(db, b, "b")
    db.commit(a)
    with pytest.raises(TransactionConflictError, match="serialization conflict"):
        db.commit(b)
    db.rollback(b)
    assert names(db) == ["a"]


def test_failed_write_aborts_whole_transaction(tmp_path: Path) -> None:
    db = Database(tmp_path)
    token = db.begin()
    create(db, token)
    with pytest.raises(ValueError, match="already exists"):
        create(db, token)
    with pytest.raises(ValueError, match="aborted"):
        db.commit(token)
    db.rollback(token)
    assert names(db) == []


def test_process_independent_transaction_and_persistence(tmp_path: Path) -> None:
    db = Database(tmp_path)
    token = db.begin()
    create(db, token)
    subprocess.run(
        [
            sys.executable,
            "-c",
            "from rdf_worker.storage import Database; import sys; Database(sys.argv[1]).commit(sys.argv[2].encode())",
            str(tmp_path),
            token.decode(),
        ],
        check=True,
        timeout=20,
    )
    assert names(Database(tmp_path)) == ["items"]


def test_raw_dataset_is_valid_rdf_and_preserves_duplicate_null_rows(tmp_path: Path) -> None:
    db = Database(tmp_path)
    create(db, None)
    with db.access(write=True) as snapshot:
        graph = RelationalGraph(snapshot)
        table = graph.require(["main"], "items")
        graph.write(table, "insert", pa.record_batch([pa.array([None, None, 7, 7], type=pa.int64())], names=["n"]))
    with db.access() as snapshot:
        graph = RelationalGraph(snapshot)
        table = graph.require(["main"], "items")
        batch, _ = graph.scan(table)
        assert batch.column("n").to_pylist() == [None, None, 7, 7]
        assert batch.column(table.rowid_name).to_pylist() == [1, 2, 3, 4]
        export = snapshot.store.dump(format=RdfFormat.N_QUADS)
    independent = Store()
    independent.load(export, RdfFormat.N_QUADS)
    assert len(list(independent.quads_for_pattern(None, NamedNode("urn:vgi:rdf:rowId"), None, None))) == 4
    assert Quad(table.row_node(3), table.predicate("n"), Literal(7), table.graph) in independent


def test_standalone_failure_does_not_publish(tmp_path: Path) -> None:
    db = Database(tmp_path)
    with pytest.raises(RuntimeError, match="interrupt"), db.access(write=True) as snapshot:
        snapshot.store.add(Quad(NamedNode("urn:s"), NamedNode("urn:p"), Literal("partial")))
        raise RuntimeError("interrupt")
    with db.access() as snapshot:
        assert not list(snapshot.store.quads_for_pattern(NamedNode("urn:s"), None, None, None))


@pytest.mark.parametrize("token", [b"../CURRENT.json", b"", b"unknown", b"0" * 32])
def test_invalid_transaction_tokens(tmp_path: Path, token: bytes) -> None:
    with pytest.raises(ValueError, match="transaction"):
        Database(tmp_path).commit(token)


def test_vacuum_preserves_active_snapshots(tmp_path: Path) -> None:
    db = Database(tmp_path)
    create(db, None, "first")
    reader = db.begin()
    create(db, None, "second")
    assert db.vacuum() >= 1
    assert names(db, reader) == ["first"]
    assert names(db) == ["first", "second"]
    db.rollback(reader)
    assert db.vacuum() >= 1
    assert names(db) == ["first", "second"]


def test_scan_snapshots_and_cleanup(tmp_path: Path) -> None:
    db = Database(tmp_path)
    create(db, None)
    tx = db.begin()
    scan = db.freeze_scan(tx)
    create(db, tx, "later")
    with db.scan_access(tx, scan) as snapshot:
        assert [t.name for t in RelationalGraph(snapshot).tables()] == ["items"]
    db.commit(tx)
    assert not (tmp_path / "transactions" / tx.decode() / "scans").exists()


def test_recover_commit_after_receipt_write_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import rdf_worker.storage as storage

    db = Database(tmp_path)
    token = db.begin()
    create(db, token)
    original = storage._write_json

    def fail_receipt(path: Path, value: dict[str, object]) -> None:
        if path.name == "state.json" and value.get("status") == "committed":
            raise OSError("simulated receipt write failure")
        original(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(storage, "_write_json", fail_receipt)
        with pytest.raises(OSError, match="receipt"):
            db.commit(token)
    assert names(db) == ["items"]
    db.rollback(token)  # The commit already published; rollback must acknowledge that fact.
    db.commit(token)
    assert names(db) == ["items"]
