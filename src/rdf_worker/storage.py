"""Durable Oxigraph snapshots shared by independent VGI worker processes.

Committed generations are immutable. Transactions lazily fork one using
Oxigraph's hard-link backup, mutate their private store, and publish it with
an atomic pointer replacement. A database-wide optimistic version check
prevents lost updates and write skew. No process-local transaction state is
required. All paths in transaction tokens are generated here, never supplied
by a caller.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from filelock import FileLock, Timeout
from pyoxigraph import Store


class TransactionConflictError(RuntimeError):
    """Another writer committed after this transaction's snapshot began."""


def _sync_directory(path: Path) -> None:
    if os.name == "nt":
        return  # Windows does not expose POSIX directory fsync.
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}")
    try:
        with temporary.open("x") as stream:
            json.dump(value, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = json.loads(path.read_text())
    return result


def _mkdir_durable(path: Path) -> None:
    """Create a directory tree and persist each new parent directory entry."""
    missing = []
    candidate = path
    while not candidate.exists():
        missing.append(candidate)
        candidate = candidate.parent
    path.mkdir(parents=True, exist_ok=True)
    for created in reversed(missing):
        _sync_directory(created.parent)


class Snapshot:
    """A bounded store handle; closing invalidates it before releasing locks."""

    def __init__(self, path: Path, *, writable: bool) -> None:
        """Open a private writable store or an immutable committed generation."""
        self.path = path
        self._store: Store | None = Store(str(path)) if writable else Store.read_only(str(path))
        self.writable = writable

    @property
    def store(self) -> Store:
        """Return the active RDF store, rejecting use after context exit."""
        if self._store is None:
            raise RuntimeError("RDF snapshot is closed")
        return self._store

    def close(self) -> None:
        """Flush writes before dropping Oxigraph's last owned store reference."""
        try:
            if self._store is not None and self.writable:
                self._store.flush()
        finally:
            self._store = None


class Database:
    """An RDF database directory with cross-process snapshot transactions."""

    def __init__(self, path: str | Path) -> None:
        """Initialize a database directory exactly once under an interprocess lock."""
        self.path = Path(path).expanduser().resolve()
        _mkdir_durable(self.path)
        self.lock = FileLock(self.path / "database.lock", timeout=60)
        with self.lock:
            (self.path / "versions").mkdir(exist_ok=True)
            (self.path / "transactions").mkdir(exist_ok=True)
            if not (self.path / "CURRENT.json").exists():
                generation = uuid.uuid4().hex
                store = Store(str(self.path / "versions" / generation))
                # Schema and table metadata are RDF too; main is never implicit.
                store.update(
                    "INSERT DATA { GRAPH <urn:vgi:rdf:catalog> { "
                    '<urn:vgi:rdf:schema:main> <urn:vgi:rdf:schemaName> "main" . '
                    "<urn:vgi:rdf:dataset> <urn:vgi:rdf:relationshipMappingVersion> 2 } }"
                )
                store.flush()
                del store
                _sync_directory(self.path / "versions")
                _write_json(self.path / "CURRENT.json", {"format": 1, "generation": generation, "version": 1})
            if self.current()["format"] != 1:
                raise ValueError("Unsupported vgi-rdf storage format")

    def current(self) -> dict[str, Any]:
        """Read the atomically published generation pointer."""
        current = _read_json(self.path / "CURRENT.json")
        if (
            current.get("format") != 1
            or not isinstance(current.get("version"), int)
            or current["version"] < 1
            or not isinstance(current.get("generation"), str)
            or re.fullmatch(r"[0-9a-f]{32}", current["generation"]) is None
        ):
            raise ValueError("Invalid or unsupported RDF generation pointer")
        return current

    def _transaction_path(self, token: bytes) -> Path:
        name = token.decode("ascii")
        if re.fullmatch(r"[0-9a-f]{32}", name) is None:
            raise ValueError("Invalid RDF transaction token")
        path = self.path / "transactions" / name
        if not path.is_dir():
            raise ValueError("Unknown RDF transaction")
        return path

    def begin(self) -> bytes:
        """Pin the current committed generation without copying it yet."""
        with self.lock:
            current = self.current()
            name = uuid.uuid4().hex
            path = self.path / "transactions" / name
            path.mkdir()
            _sync_directory(path.parent)
            _write_json(
                path / "state.json",
                {"base": current["generation"], "version": current["version"], "status": "active", "dirty": False},
            )
            return name.encode("ascii")

    @contextmanager
    def access(self, token: bytes | None = None, *, write: bool = False) -> Iterator[Snapshot]:
        """Open one RPC's snapshot, atomically committing standalone writes."""
        if token is None and write:
            token = self.begin()
            try:
                with self.access(token, write=True) as standalone_snapshot:
                    yield standalone_snapshot
                self.commit(token)
            except BaseException:
                self.rollback(token)
                raise
            return
        if token is None:
            with self.lock:
                current_snapshot = Snapshot(self.path / "versions" / self.current()["generation"], writable=False)
                try:
                    yield current_snapshot
                finally:
                    current_snapshot.close()
            return
        path = self._transaction_path(token)
        with FileLock(path / "transaction.lock", timeout=60):
            state = _read_json(path / "state.json")
            if state["status"] != "active":
                raise ValueError(f"RDF transaction is {state['status']}; ROLLBACK and retry")
            if state["dirty"]:
                with self.lock:
                    if self._published(path.name, state["base"]):
                        state["status"] = "committed"
                        _write_json(path / "state.json", state)
                        raise ValueError("RDF transaction is already committed; begin a new transaction")
            snapshot: Snapshot | None = None
            if write:
                # A crash or disk-full failure must never leave a partially
                # executed RPC eligible for commit. Persist intent before touching data.
                state["status"] = "writing"
                _write_json(path / "state.json", state)
            try:
                if write and not state["dirty"]:
                    shutil.rmtree(path / "data", ignore_errors=True)
                    original = Store.read_only(str(self.path / "versions" / state["base"]))
                    original.backup(str(path / "data"))
                    del original
                    state["dirty"] = True
                    _write_json(path / "state.json", state)
                source = path / "data" if state["dirty"] else self.path / "versions" / state["base"]
                snapshot = Snapshot(source, writable=write)
                yield snapshot
                snapshot.close()
                if write:
                    state["status"] = "active"
                    _write_json(path / "state.json", state)
                else:
                    os.utime(path / "state.json", None)
            except BaseException:
                if write:
                    state["status"] = "aborted"
                    _write_json(path / "state.json", state)
                raise
            finally:
                if snapshot is not None:
                    snapshot.close()

    def _published(self, name: str, base: str) -> bool:
        ancestor = self.current()["generation"]
        seen: set[str] = set()
        while ancestor != base:
            if ancestor in seen:
                raise ValueError("Cycle in RDF commit ancestry")
            seen.add(ancestor)
            if ancestor == name:
                return True
            parent = self.path / "versions" / ancestor / "parent.json"
            if not parent.exists():
                break
            ancestor = _read_json(parent)["generation"]
        return False

    def commit(self, token: bytes) -> None:
        """Publish a complete transaction, rejecting concurrent write conflicts."""
        path = self._transaction_path(token)
        with FileLock(path / "transaction.lock", timeout=60), self.lock:
            state = _read_json(path / "state.json")
            if state["status"] == "committed":
                shutil.rmtree(path / "scans", ignore_errors=True)
                return
            if state["status"] != "active":
                raise ValueError("Cannot commit an aborted RDF transaction")
            current = self.current()
            name = path.name
            # Recovery after pointer publication but before recording the receipt.
            if self._published(name, state["base"]):
                state["status"] = "committed"
                _write_json(path / "state.json", state)
                shutil.rmtree(path / "scans", ignore_errors=True)
                return
            if state["dirty"]:
                if current["generation"] != state["base"]:
                    state["status"] = "aborted"
                    _write_json(path / "state.json", state)
                    raise TransactionConflictError(
                        "RDF serialization conflict: another writer committed; retry transaction"
                    )
                destination = self.path / "versions" / name
                if (path / "data").exists():
                    _write_json(path / "data" / "parent.json", {"generation": state["base"]})
                    os.replace(path / "data", destination)
                    _sync_directory(destination.parent)
                    _sync_directory(path)
                _write_json(
                    self.path / "CURRENT.json", {"format": 1, "generation": name, "version": current["version"] + 1}
                )
            state["status"] = "committed"
            _write_json(path / "state.json", state)
            shutil.rmtree(path / "scans", ignore_errors=True)

    def rollback(self, token: bytes) -> None:
        """Discard private writes; repeated rollback and rollback after commit are safe."""
        path = self._transaction_path(token)
        with FileLock(path / "transaction.lock", timeout=60), self.lock:
            state = _read_json(path / "state.json")
            if state["status"] == "committed":
                shutil.rmtree(path / "scans", ignore_errors=True)
                return
            if self._published(path.name, state["base"]):
                state["status"] = "committed"
                _write_json(path / "state.json", state)
                shutil.rmtree(path / "scans", ignore_errors=True)
                return
            state["status"] = "aborted"
            _write_json(path / "state.json", state)
            shutil.rmtree(path / "data", ignore_errors=True)
            shutil.rmtree(path / "scans", ignore_errors=True)

    def version(self, token: bytes | None = None) -> int:
        """Return the committed catalog version or the transaction's pinned version."""
        if token is None:
            return int(self.current()["version"])
        return int(_read_json(self._transaction_path(token) / "state.json")["version"])

    def freeze_scan(self, token: bytes) -> str:
        """Pin a statement scan before its writes can affect its own input."""
        path = self._transaction_path(token)
        with self.access(token) as snapshot:
            scans = path / "scans"
            scans.mkdir(exist_ok=True)
            name = uuid.uuid4().hex
            snapshot.store.backup(str(scans / name))
            return name

    def vacuum(self, *, receipt_seconds: int | None = None) -> int:
        """Reclaim old RDF files while retaining active snapshots and commit ancestry."""
        if receipt_seconds is not None and receipt_seconds < 1:
            raise ValueError("receipt_seconds must be positive")
        removed = 0
        with self.lock:
            keep = {self.current()["generation"]}
            bases = set()
            for path in (self.path / "transactions").iterdir():
                state_path = path / "state.json"
                if not state_path.exists():
                    continue
                state = _read_json(state_path)
                if state["status"] in {"active", "writing"}:
                    keep.update([state["base"], path.name])
                    bases.add(state["base"])
                elif receipt_seconds is not None and time.time() - state_path.stat().st_mtime >= receipt_seconds:
                    try:
                        with FileLock(path / "transaction.lock", timeout=0):
                            shutil.rmtree(path)
                    except (Timeout, PermissionError):
                        pass  # Windows may defer deleting an open lock file until a later vacuum.
            ancestry = set()
            ancestor = self.current()["generation"]
            while bases:
                bases.discard(ancestor)
                if not bases:
                    break
                if ancestor in ancestry:
                    raise ValueError("Cycle in RDF commit ancestry")
                ancestry.add(ancestor)
                parent = self.path / "versions" / ancestor / "parent.json"
                if not parent.exists():
                    break
                ancestor = _read_json(parent)["generation"]
            for path in (self.path / "versions").iterdir():
                if path.name in keep:
                    continue
                deleted = False
                for child in path.iterdir():
                    if child.name == "parent.json" and path.name in ancestry:
                        continue  # Small durable receipts make ambiguous commits recoverable.
                    if child.is_dir():
                        shutil.rmtree(child)
                    else:
                        child.unlink()
                    deleted = True
                removed += int(deleted)
                if not any(path.iterdir()):
                    path.rmdir()
        return removed

    def reap(self, idle_seconds: int) -> int:
        """Expire idle transactions while skipping every currently locked operation."""
        if idle_seconds < 1:
            raise ValueError("idle_seconds must be positive")
        count = 0
        for path in (self.path / "transactions").iterdir():
            try:
                with FileLock(path / "transaction.lock", timeout=0), self.lock:
                    state_path = path / "state.json"
                    if not state_path.exists() or time.time() - state_path.stat().st_mtime < idle_seconds:
                        continue
                    state = _read_json(state_path)
                    if state["status"] not in {"active", "writing"}:
                        continue
                    # rollback uses reentrant file locks only when it owns the same
                    # lock object, so perform the recovery directly under these locks.
                    state["status"] = "committed" if self._published(path.name, state["base"]) else "aborted"
                    _write_json(state_path, state)
                    shutil.rmtree(path / "data", ignore_errors=True)
                    shutil.rmtree(path / "scans", ignore_errors=True)
                    count += 1
            except Timeout:
                continue
        return count

    def status(self) -> dict[str, Any]:
        """Return committed version, transaction counts and oldest idle age for monitoring."""
        with self.lock:
            current = self.current()
            counts: dict[str, int] = {}
            oldest = 0.0
            for path in (self.path / "transactions").glob("*/state.json"):
                state = _read_json(path)
                status = str(state["status"])
                counts[status] = counts.get(status, 0) + 1
                if status in {"active", "writing"}:
                    oldest = max(oldest, time.time() - path.stat().st_mtime)
            return {**current, "transactions": counts, "oldest_idle_seconds": round(oldest, 3)}

    @contextmanager
    def scan_access(self, token: bytes, name: str) -> Iterator[Snapshot]:
        """Read a frozen scan from any worker process."""
        if re.fullmatch(r"[0-9a-f]{32}", name) is None:
            raise ValueError("Invalid RDF scan token")
        path = self._transaction_path(token)
        with FileLock(path / "transaction.lock", timeout=60):
            state = _read_json(path / "state.json")
            if state["status"] != "active":
                raise ValueError("RDF scan transaction is no longer active")
            snapshot = Snapshot(path / "scans" / name, writable=False)
            try:
                yield snapshot
            finally:
                snapshot.close()
                os.utime(path / "state.json", None)
