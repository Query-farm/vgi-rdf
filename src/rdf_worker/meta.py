"""Public discovery metadata for the RDF database worker."""

import json

CATALOG_NAME = "rdf"
# Catalog-level one-line description (VGI001): plain text, no Markdown, at most 300 characters.
CATALOG_COMMENT = (
    "Transactional SQL tables stored as RDF in an Oxigraph dataset, so the same data can be queried "
    "as ordinary SQL rows or with local SPARQL and graph traversal."
)
DESCRIPTION = (
    "Persistent SQL tables backed by an Oxigraph RDF dataset. Supports SELECT, INSERT, UPDATE, DELETE, "
    "CREATE/DROP TABLE, schemas, RETURNING, defaults, NOT NULL, primary/unique keys and immediate "
    "foreign keys. Native SQL runs in the host engine; data and table definitions persist as RDF. "
    "Transactions use immutable snapshots and reject concurrent writer conflicts. Attach with database "
    "set to a durable directory, or omit it for a temporary database. Query the same data as typed SQL rows "
    "or RDF terms using local read-only SPARQL and bounded graph traversal. Stable resource identities connect "
    "the two query models. Foreign keys produce transactional resource-to-resource RDF edges. "
    "The backend enforces CHECK constraints, but the tested SQL engines cannot send native CHECK definitions."
)
# Composed examples: each SQL string is defined once and shared by the schema-level list and the
# example list of the function that drives the query.
TABLE_GRAPH_SIZES_SQL = (
    "SELECT t.schema_name, t.table_name, count(q.subject) AS statements "
    "FROM rdf.main.rdf_tables() AS t LEFT JOIN rdf.main.rdf_quads() AS q ON q.graph = '<' || t.graph_iri || '>' "
    "WHERE t.kind = 'table' GROUP BY t.schema_name, t.table_name ORDER BY t.schema_name, t.table_name"
)
TABLE_GRAPH_SIZES_DESCRIPTION = (
    "Count the RDF statements stored in each SQL table's row graph by joining rdf_tables to rdf_quads"
)
FOREIGN_KEY_EDGES_SQL = (
    "SELECT fk.schema_name, fk.table_name AS referencing_table, target.table_name AS referenced_table, "
    "count(q.subject) AS edges FROM rdf.main.rdf_tables() AS fk "
    "JOIN rdf.main.rdf_tables() AS target ON target.kind = 'table' AND target.iri = fk.target_iri "
    "LEFT JOIN rdf.main.rdf_quads(graph_iri := 'urn:vgi:rdf:relationships') AS q ON q.predicate = '<' || fk.iri || '>' "
    "WHERE fk.kind = 'foreign_key' GROUP BY fk.schema_name, fk.table_name, target.table_name "
    "ORDER BY fk.schema_name, referencing_table, referenced_table"
)
FOREIGN_KEY_EDGES_DESCRIPTION = (
    "Count the row-to-row RDF edges each foreign key has materialized, naming the referencing and referenced "
    "tables, by joining rdf_tables to itself and to the relationships graph from rdf_quads"
)
ROWS_WITH_INCOMING_EDGES_SQL = (
    "SELECT p.name, count(g.source_row) AS incoming_references FROM rdf.main.rdf_rows('main', 'people') AS p "
    "LEFT JOIN rdf.main.rdf_sparql('SELECT ?source_row ?target_row WHERE { GRAPH <urn:vgi:rdf:relationships> "
    "{ ?source_row ?relationship ?target_row } }') AS g ON g.target_row.value = p.__rdf_iri "
    "GROUP BY p.name ORDER BY p.name"
)
ROWS_WITH_INCOMING_EDGES_DESCRIPTION = (
    "Count the foreign-key references pointing at each person, including people with none, by joining rdf_rows "
    "identities to SPARQL relationship edges; assumes people(id PRIMARY KEY, name) referenced by e.g. "
    "orders(id, buyer REFERENCES people(id))"
)

SCHEMA_TAGS = {
    "vgi.title": "RDF",
    "vgi.subtitle": "SQL tables, SPARQL and graph traversal",
    "vgi.doc_llm": DESCRIPTION,
    "vgi.doc_md": (
        "## SQL tables and RDF graphs\n\n"
        "Create application tables here and query them with ordinary SQL. "
        "Schema names and quoted identifiers retain their spelling.\n\n"
        "Query relationships with local read-only SPARQL or bounded graph traversal. "
        "Stable row IRIs connect ordinary typed SQL rows to RDF terms without losing literal datatypes "
        "or language tags. Foreign-key edges share SQL transaction boundaries; external named graphs "
        "can hold imported RDF. Choose SQL for tabular analysis and graph queries for paths and neighborhoods."
    ),
    "domain": "rdf",
    "vgi.keywords": json.dumps(["rdf", "sql", "tables", "transactions", "oxigraph"]),
    "vgi.categories": json.dumps(
        [{"name": "rdf", "title": "RDF", "description": "Inspect RDF storage and SQL tables."}]
    ),
    "vgi.example_queries": json.dumps(
        [
            {
                "description": "Count RDF statements by graph",
                "sql": "SELECT graph, count(*) AS statements FROM rdf.main.rdf_quads() GROUP BY graph ORDER BY graph",
            },
            {"description": TABLE_GRAPH_SIZES_DESCRIPTION, "sql": TABLE_GRAPH_SIZES_SQL},
            {"description": FOREIGN_KEY_EDGES_DESCRIPTION, "sql": FOREIGN_KEY_EDGES_SQL},
            {"description": ROWS_WITH_INCOMING_EDGES_DESCRIPTION, "sql": ROWS_WITH_INCOMING_EDGES_SQL},
        ]
    ),
}
CATALOG_TAGS = {
    **{key: value for key, value in SCHEMA_TAGS.items() if key not in {"vgi.categories", "vgi.example_queries"}},
    "vgi.doc_md": (
        "## RDF database worker\n\n" + DESCRIPTION + "\n\n"
        "SQL data and definitions live in an Oxigraph dataset. "
        "The database directory contains immutable committed generations and private transaction workspaces. "
        "Commit publishes a complete generation; rollback discards private writes."
    ),
    "vgi.title": "VGI RDF Database",
    "vgi.subtitle": "Transactional SQL tables stored as RDF graphs",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.source_url": "https://github.com/Query-farm/vgi-rdf",
    "vgi.copyright": "Copyright 2026 Query.Farm LLC",
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.agent_test_tasks": json.dumps(
        [
            {
                "name": "inspect_main_schema",
                "prompt": (
                    "Using the rdf worker, count RDF statements whose predicate is "
                    "<urn:vgi:rdf:schemaName> and whose object is the RDF string literal main. Return a "
                    "single count."
                ),
            }
        ]
    ),
}

_GRAPH_TASKS = [
    {
        "name": "query_catalog_sparql",
        "prompt": "Use rdf_sparql to ASK whether urn:vgi:rdf:catalog contains statements. Return answer.",
    },
    {
        "name": "discover_table_mapping",
        "prompt": "Count the SQL-to-RDF mappings returned by rdf_tables. Return one count.",
    },
    {
        "name": "inspect_neighbors",
        "prompt": "Count outgoing edges from urn:example:absent with rdf_neighbors. Return one count.",
    },
    {
        "name": "inspect_reachability",
        "prompt": "Count nodes reachable from urn:example:absent within three edges using rdf_reachable.",
    },
    {
        "name": "inspect_row_identity",
        "prompt": "Create rdf.main.agent_people(name VARCHAR), insert Ada, then use rdf_rows to return name "
        "and whether its __rdf_iri starts with urn:vgi:rdf:table:.",
    },
    {
        "name": "count_row_column_statements",
        "prompt": "Create rdf.main.agent_contacts(name VARCHAR, email VARCHAR, phone VARCHAR) and insert "
        "('Ada', 'ada@example.org', NULL) and ('Grace', NULL, NULL). Then, combining rdf_rows, rdf_quads and "
        "rdf_tables, count for each row the RDF statements whose subject is that row's IRI and whose predicate "
        "is one of the table's column IRIs. Return name and stored_columns (BIGINT), ordered by name.",
    },
]
CATALOG_TAGS["vgi.agent_test_tasks"] = json.dumps(json.loads(CATALOG_TAGS["vgi.agent_test_tasks"]) + _GRAPH_TASKS)
