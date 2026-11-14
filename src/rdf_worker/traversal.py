"""Indexed RDF neighborhood scans and bounded breadth-first reachability."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
from pyoxigraph import BlankNode, DefaultGraph, NamedNode, Store

from .limits import setting

EDGE_SCHEMA = pa.schema(
    [
        pa.field(n, pa.string(), metadata={b"comment": d.encode()})
        for n, d in [
            ("source", "Edge subject: raw IRI or _:blank-node label."),
            ("predicate", "Raw relationship predicate IRI."),
            ("target", "Edge object: raw IRI or _:blank-node label."),
            ("graph", "Raw graph IRI or _:blank-node label; NULL for the default graph."),
            ("neighbor", "Adjacent node relative to the requested starting node."),
        ]
    ]
)
_REACH_FIELDS: list[pa.Field[Any]] = [
    pa.field("node", pa.string(), metadata={b"comment": b"Reachable node: raw IRI or _:blank-node label."}),
    pa.field("depth", pa.int64(), metadata={b"comment": b"Minimum number of edges from the starting node."}),
]
REACH_SCHEMA = pa.schema(_REACH_FIELDS)


def node(value: str) -> NamedNode | BlankNode:
    """Accept a raw IRI or an explicitly marked blank-node label."""
    return BlankNode(value[2:]) if value.startswith("_:") else NamedNode(value)


def label(value: Any) -> str | None:
    """Expose identities without confusing default graphs with named graphs."""
    if isinstance(value, DefaultGraph):
        return None
    return str(value) if isinstance(value, BlankNode) else str(value.value)


def validate_traversal(options: dict[str, Any]) -> None:
    """Validate names and bounded traversal options before opening a store."""
    node(options["node"])
    if options.get("graph_iri"):
        NamedNode(options["graph_iri"])
    if options.get("predicate_iri"):
        NamedNode(options["predicate_iri"])
    if options["direction"] not in {"out", "in", "both"}:
        raise ValueError("direction must be out, in or both")
    if not 1 <= options.get("max_depth", 1) <= 64:
        raise ValueError("max_depth must be between 1 and 64")


def neighbors(store: Store, options: dict[str, Any], counter: list[int]) -> Iterator[dict[str, Any]]:
    """Use Oxigraph's subject/object indexes and count every examined statement."""
    current = node(options["node"])
    graph = NamedNode(options["graph_iri"]) if options["graph_iri"] else None
    predicate = NamedNode(options["predicate_iri"]) if options["predicate_iri"] else None
    directions = ["out", "in"] if options["direction"] == "both" else [options["direction"]]
    ceiling = setting("MAX_TRAVERSAL_EDGES", 1_000_000)
    for direction in directions:
        quads = store.quads_for_pattern(
            current if direction == "out" else None, predicate, current if direction == "in" else None, graph
        )
        for quad in quads:
            counter[0] += 1
            if counter[0] > ceiling:
                raise ValueError("Traversal exceeds MAX_TRAVERSAL_EDGES")
            adjacent = quad.object if direction == "out" else quad.subject
            if not isinstance(adjacent, NamedNode | BlankNode):
                continue
            if direction == "in" and options["direction"] == "both" and quad.subject == quad.object:
                continue
            yield {
                "source": label(quad.subject),
                "predicate": quad.predicate.value,
                "target": label(quad.object),
                "graph": label(quad.graph_name),
                "neighbor": label(adjacent),
            }


def traverse(store: Store, options: dict[str, Any], operation: str, max_rows: int) -> Iterator[dict[str, Any]]:
    """Return adjacent edges or shortest unweighted distances within explicit bounds."""
    validate_traversal(options)
    counter = [0]
    if operation == "neighbors":
        yield from neighbors(store, options, counter)
        return
    visited = {options["node"]}
    queue = deque([(options["node"], 0)])
    while queue:
        current, depth = queue.popleft()
        if depth >= options["max_depth"]:
            continue
        for edge in neighbors(store, {**options, "node": current}, counter):
            adjacent = edge["neighbor"]
            if adjacent in visited:
                continue
            if len(visited) > max_rows:
                raise ValueError("Traversal result exceeds max_rows")
            visited.add(adjacent)
            yield {"node": adjacent, "depth": depth + 1}
            queue.append((adjacent, depth + 1))
