from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from pyoxigraph import Literal, NamedNode, Quad

from rdf_worker import storage
from rdf_worker.storage import Database

TRIPLE = Quad(NamedNode("urn:test:subject"), NamedNode("urn:test:predicate"), Literal("committed"))


def test_process_death_mid_write_cannot_commit(tmp_path: Path) -> None:
    db = Database(tmp_path)
    token = db.begin()
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os, sys
from pyoxigraph import NamedNode, Literal, Quad
from rdf_worker.storage import Database
db = Database(sys.argv[1])
with db.access(sys.argv[2].encode(), write=True) as snapshot:
    snapshot.store.add(Quad(NamedNode('urn:s'), NamedNode('urn:p'), Literal('partial')))
    snapshot.store.flush()
    os._exit(86)
""",
            str(tmp_path),
            token.decode(),
        ],
        timeout=20,
    )
    assert result.returncode == 86
    with pytest.raises(ValueError, match="aborted"):
        Database(tmp_path).commit(token)
    db.rollback(token)
    with db.access() as snapshot:
        assert not list(snapshot.store.quads_for_pattern(NamedNode("urn:s"), None, None, None))


@pytest.mark.parametrize("phase", ["moved", "published", "receipt"])
def test_process_death_during_commit_recovers_exactly_once(tmp_path: Path, phase: str) -> None:
    db = Database(tmp_path)
    token = db.begin()
    with db.access(token, write=True) as snapshot:
        snapshot.store.add(TRIPLE)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os, sys
from pathlib import Path
from rdf_worker import storage
phase = sys.argv[3]
original = storage.os.replace
def replace(source, destination):
    original(source, destination)
    target = Path(destination)
    if ((phase == 'moved' and target.parent.name == 'versions') or
        (phase == 'published' and target.name == 'CURRENT.json') or
        (phase == 'receipt' and target.name == 'state.json')):
        os._exit(86)
storage.os.replace = replace
storage.Database(sys.argv[1]).commit(sys.argv[2].encode())
""",
            str(tmp_path),
            token.decode(),
            phase,
        ],
        timeout=20,
    )
    assert result.returncode == 86
    reopened = Database(tmp_path)
    with reopened.access() as snapshot:
        assert (TRIPLE in snapshot.store) == (phase != "moved")
    # Maintenance must preserve an interrupted commit's private generation
    # and the ancestry needed to recognize publication exactly once.
    reopened.vacuum()
    reopened.commit(token)
    version = reopened.version()
    reopened.commit(token)
    reopened.rollback(token)
    assert reopened.version() == version
    with reopened.access() as snapshot:
        assert TRIPLE in snapshot.store


def test_disk_full_cannot_leave_failed_write_committable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = Database(tmp_path)
    token = db.begin()
    write_json = storage._write_json

    def fail_abort(path: Path, value: dict[str, object]) -> None:
        if value.get("status") == "aborted":
            raise OSError(28, "No space left on device")
        write_json(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(storage, "_write_json", fail_abort)
        with pytest.raises(OSError, match="space"), db.access(token, write=True) as snapshot:
            snapshot.store.add(TRIPLE)
            raise OSError(28, "No space left on device")
    with pytest.raises(ValueError, match="aborted"):
        db.commit(token)
    db.rollback(token)
    with db.access() as snapshot:
        assert TRIPLE not in snapshot.store


def test_idle_reaper_preserves_locked_and_recent_transactions(tmp_path: Path) -> None:
    db = Database(tmp_path)
    abandoned, recent, busy = db.begin(), db.begin(), db.begin()
    for token in [abandoned, busy]:
        path = tmp_path / "transactions" / token.decode() / "state.json"
        os.utime(path, (time.time() - 120, time.time() - 120))
    with db.access(busy):
        assert db.reap(60) == 1
    assert db.status()["transactions"] == {"aborted": 1, "active": 2}
    with pytest.raises(ValueError, match="aborted"), db.access(abandoned):
        pass
    with db.access(recent), db.access(busy):
        pass
    db.rollback(recent)
    db.rollback(busy)


def test_concurrent_processes_retry_without_losing_updates(tmp_path: Path) -> None:
    db = Database(tmp_path)
    with db.access(write=True) as snapshot:
        snapshot.store.add(Quad(NamedNode("urn:counter"), NamedNode("urn:value"), Literal(0)))
    script = """
import sys, time
from pyoxigraph import NamedNode, Literal, Quad
from rdf_worker.storage import Database, TransactionConflictError
db = Database(sys.argv[1])
for i in range(12):
    for attempt in range(200):
        token = db.begin()
        try:
            with db.access(token, write=True) as snapshot:
                old = next(snapshot.store.quads_for_pattern(
                    NamedNode('urn:counter'), NamedNode('urn:value'), None, None))
                value = int(old.object.value)
                snapshot.store.update('DELETE DATA { <urn:counter> <urn:value> ' + str(value)
                    + ' }; INSERT DATA { <urn:counter> <urn:value> ' + str(value+1) + ' }')
            db.commit(token)
            break
        except TransactionConflictError:
            db.rollback(token)
            time.sleep(0.001)
    else:
        raise RuntimeError('retry budget exhausted')
"""
    processes = [
        subprocess.Popen([sys.executable, "-c", script, str(tmp_path)], stderr=subprocess.PIPE) for _ in range(4)
    ]
    for process in processes:
        _, error = process.communicate(timeout=90)
        assert process.returncode == 0, error.decode()
    with db.access() as snapshot:
        value = next(snapshot.store.quads_for_pattern(NamedNode("urn:counter"), None, None, None)).object
        assert value == Literal(48)


def test_generation_pointer_rejects_directory_escape(tmp_path: Path) -> None:
    Database(tmp_path)
    (tmp_path / "CURRENT.json").write_text('{"format":1,"version":1,"generation":"../outside"}')
    with pytest.raises(ValueError, match="generation pointer"):
        Database(tmp_path)


def test_published_transaction_cannot_be_written_again(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = Database(tmp_path)
    token = db.begin()
    with db.access(token, write=True) as snapshot:
        snapshot.store.add(TRIPLE)
    write_json = storage._write_json

    def fail_receipt(path: Path, value: dict[str, object]) -> None:
        if value.get("status") == "committed":
            raise OSError("lost commit receipt")
        write_json(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(storage, "_write_json", fail_receipt)
        with pytest.raises(OSError, match="receipt"):
            db.commit(token)
    with pytest.raises(ValueError, match="already committed"), db.access(token, write=True):
        pass
    db.commit(token)
    with db.access() as snapshot:
        assert TRIPLE in snapshot.store


def test_vacuum_receipt_retention_keeps_active_snapshot_ancestry(tmp_path: Path) -> None:
    db = Database(tmp_path)
    reader = db.begin()
    first = db.begin()
    with db.access(first, write=True) as snapshot:
        snapshot.store.add(TRIPLE)
    db.commit(first)
    second = db.begin()
    with db.access(second, write=True) as snapshot:
        snapshot.store.add(Quad(NamedNode("urn:second"), NamedNode("urn:p"), Literal(2)))
    db.commit(second)
    receipt = tmp_path / "transactions" / first.decode() / "state.json"
    os.utime(receipt, (time.time() - 100, time.time() - 100))
    db.vacuum(receipt_seconds=60)
    assert not receipt.exists()
    with db.access(reader) as snapshot:
        assert TRIPLE not in snapshot.store
    db.commit(second)
    with pytest.raises(ValueError, match="Unknown"):
        db.commit(first)
    db.rollback(reader)
    db.vacuum()
    assert len(list((tmp_path / "versions").iterdir())) == 1
