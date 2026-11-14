"""Offline-friendly administration commands; separate from the Arrow worker channel."""

from __future__ import annotations

import argparse
import json
import shutil
import uuid
from pathlib import Path

from pyoxigraph import RdfFormat

from .graphs import FORMATS, drop_graph, import_rdf, upgrade
from .integrity import check
from .storage import Database, _mkdir_durable, _read_json, _sync_directory, _write_json


def backup(database: Database, destination: Path) -> None:
    """Create a consistent standalone backup in a new directory."""
    _mkdir_durable(destination.parent)
    destination.mkdir(exist_ok=False)
    try:
        generation = uuid.uuid4().hex
        (destination / "versions").mkdir()
        (destination / "transactions").mkdir()
        with database.access() as snapshot:
            snapshot.store.backup(str(destination / "versions" / generation))
        _sync_directory(destination / "versions" / generation)
        _sync_directory(destination / "versions")
        _write_json(destination / "CURRENT.json", {"format": 1, "generation": generation, "version": 1})
        _sync_directory(destination.parent)
    except BaseException:
        shutil.rmtree(destination)
        raise


def main() -> None:
    """Export RDF, back up a database, reclaim old generations, or recover an abandoned transaction."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path, help="Existing vgi-rdf database directory")
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export", help="Export the current dataset to a new N-Quads or TriG file")
    export.add_argument("output", type=Path)
    export.add_argument("--format", choices=["nquads", "trig"], default="nquads")
    copy = commands.add_parser("backup", help="Create a consistent independent database backup")
    copy.add_argument("destination", type=Path)
    vacuum = commands.add_parser("vacuum", help="Reclaim obsolete RDF files, preserving active snapshots")
    vacuum.add_argument(
        "--receipts-older-than", type=int, help="Also remove terminal transaction receipts older than this many seconds"
    )
    commands.add_parser("upgrade", help="Upgrade derived graph mappings in an existing database")
    commands.add_parser("status", help="Report versions and transaction counts as JSON")
    commands.add_parser("check", help="Verify rows, constraints and derived graph indexes without changing data")
    reaper = commands.add_parser("reap", help="Expire idle transactions, skipping operations currently in progress")
    reaper.add_argument("--idle-seconds", type=int, required=True, help="Idle transaction lease to enforce")
    ingest = commands.add_parser("import", help="Atomically import external RDF into user-owned named graphs")
    ingest.add_argument("input", type=Path)
    ingest.add_argument("--format", choices=list(FORMATS), required=True)
    ingest.add_argument("--graph", default="", help="Destination graph IRI; required for triple formats")
    ingest.add_argument("--replace", action="store_true", help="Replace the selected graph atomically")
    remove = commands.add_parser("drop-graph", help="Drop one external named graph")
    remove.add_argument("graph")
    commands.add_parser("transactions", help="List active transaction IDs for recovery")
    rollback = commands.add_parser("rollback", help="Roll back one explicitly selected abandoned transaction")
    rollback.add_argument("transaction", help="Transaction ID returned by the transactions command")
    args = parser.parse_args()
    if not (args.database / "CURRENT.json").exists():
        parser.error("database does not exist")
    database = Database(args.database)
    if args.command == "export":
        with args.output.open("xb") as output, database.access() as snapshot:
            snapshot.store.dump(output, RdfFormat.N_QUADS if args.format == "nquads" else RdfFormat.TRIG)
    elif args.command == "backup":
        backup(database, args.destination)
    elif args.command == "vacuum":
        print(json.dumps({"reclaimed_generations": database.vacuum(receipt_seconds=args.receipts_older_than)}))
    elif args.command == "upgrade":
        print(json.dumps({"upgraded": upgrade(database)}))
    elif args.command == "status":
        print(json.dumps(database.status()))
    elif args.command == "check":
        print(json.dumps(check(database)))
    elif args.command == "reap":
        print(json.dumps({"expired_transactions": database.reap(args.idle_seconds)}))
    elif args.command == "import":
        print(
            json.dumps(
                {
                    "statements_read": import_rdf(
                        database, args.input, format_name=args.format, graph_iri=args.graph, replace=args.replace
                    )
                }
            )
        )
    elif args.command == "drop-graph":
        drop_graph(database, args.graph)
    elif args.command == "transactions":
        active = []
        for path in sorted((database.path / "transactions").glob("*/state.json")):
            state = _read_json(path)
            if state["status"] in {"active", "writing"}:
                active.append({"transaction": path.parent.name, **state})
        print(json.dumps(active, indent=2))
    elif args.command == "rollback":
        database.rollback(args.transaction.encode("ascii"))


if __name__ == "__main__":
    main()
