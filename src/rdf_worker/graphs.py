"""Transactional external RDF import with protected SQL-managed graphs."""

from __future__ import annotations

from pathlib import Path

from pyoxigraph import DefaultGraph, Literal, NamedNode, Quad, RdfFormat, parse

from .codec import NS
from .limits import setting
from .model import CATALOG, RelationalGraph
from .storage import Database

FORMATS = {
    "turtle": RdfFormat.TURTLE,
    "ntriples": RdfFormat.N_TRIPLES,
    "trig": RdfFormat.TRIG,
    "nquads": RdfFormat.N_QUADS,
}
DATASET = NamedNode(NS + "dataset")
MAPPING_VERSION = NamedNode(NS + "relationshipMappingVersion")


def user_graph(iri: str) -> NamedNode:
    """Reject the reserved namespace used by SQL rows, definitions and derived edges."""
    value = NamedNode(iri)
    if iri.startswith(NS):
        raise ValueError("The urn:vgi:rdf: graph namespace is reserved for SQL-managed data")
    return value


def upgrade(database: Database, *, read_only: bool = False) -> bool:
    """Atomically derive relationship edges for databases created before graph support."""
    with database.access() as snapshot:
        marker = next(snapshot.store.quads_for_pattern(DATASET, MAPPING_VERSION, None, CATALOG), None)
        if marker is not None and marker.object != Literal(1):
            if marker.object != Literal(2):
                raise ValueError("Unsupported RDF relationship mapping version")
            return False
    if read_only:
        raise ValueError("Database needs graph mapping upgrade; run vgi-rdf-admin DATABASE upgrade first")
    with database.access(write=True) as snapshot:
        graph = RelationalGraph(snapshot)
        for table in graph.tables():
            graph.rebuild_relationships(table)
            graph.rebuild_keys(table)
        snapshot.store.update(f"DELETE WHERE {{ GRAPH {CATALOG} {{ {DATASET} {MAPPING_VERSION} ?v }} }}")
        snapshot.store.add(Quad(DATASET, MAPPING_VERSION, Literal(2), CATALOG))
    return True


def import_rdf(database: Database, path: Path, *, format_name: str, graph_iri: str, replace: bool = False) -> int:
    """Import a bounded RDF document atomically, preserving or explicitly selecting graphs."""
    if format_name not in FORMATS:
        raise ValueError("Supported import formats: turtle, ntriples, trig, nquads")
    target = user_graph(graph_iri) if graph_iri else None
    if format_name in {"turtle", "ntriples"} and target is None:
        raise ValueError("Triple formats require an explicit --graph IRI")
    if replace and target is None:
        raise ValueError("--replace requires an explicit --graph IRI")
    ceiling = setting("MAX_IMPORT_BYTES", 64 * 1024 * 1024)
    with path.open("rb") as source:
        content = source.read(ceiling + 1)
    if len(content) > ceiling:
        raise ValueError("RDF import exceeds MAX_IMPORT_BYTES")
    count = 0
    with database.access(write=True) as snapshot:
        if target is not None and replace:
            snapshot.store.remove_graph(target)
        batch = []
        for quad in parse(content, FORMATS[format_name], rename_blank_nodes=True):
            graph = target or quad.graph_name
            if not isinstance(graph, NamedNode):
                if isinstance(graph, DefaultGraph):
                    raise ValueError("Import requires named graphs; supply --graph for a default graph")
                raise ValueError("Blank-node graph names are not supported for managed imports")
            user_graph(graph.value)
            batch.append(Quad(quad.subject, quad.predicate, quad.object, graph))
            count += 1
            if len(batch) == 2048:
                snapshot.store.extend(batch)
                batch.clear()
        if batch:
            snapshot.store.extend(batch)
        if target is not None:
            snapshot.store.add_graph(target)
    return count


def drop_graph(database: Database, graph_iri: str) -> None:
    """Drop one external graph without allowing SQL metadata or row graph mutation."""
    graph = user_graph(graph_iri)
    with database.access(write=True) as snapshot:
        snapshot.store.remove_graph(graph)
