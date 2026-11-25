# Changelog

## 0.3.0 (2026-11-25)

- Speaks VGI protocol 3.4: require vgi-python `>=0.55.2,<0.56`; uv.lock re-locked. This release
  needs Haybarn 2.0.1 (or stock DuckDB 2.0 with `INSTALL vgi FROM query_farm`): Haybarn 2.0.0
  loads a 3.3 `vgi` extension that cannot attach it.
- SQL tests run on haybarn-unittest 2.0.1rc3 (CI and Makefile).
- `serve` extra includes `vgi-python[http,oauth]`; the Docker image installs it, so the image runs in
  Orchard hosted mode (`VGI_ORCHARD_HOSTED=1` with VGI_JWT_ISSUER, VGI_JWT_AUDIENCE,
  VGI_JWT_JWKS_URI and VGI_SIGNING_KEY). The Docker HEALTHCHECK uses `HEAD /health`, which
  stays anonymous in hosted mode.

## 0.2.4 (2026-11-16)

- Bump vgi-python to 0.52.2.
- Requires Python 3.14 (CI, Docker image and tooling moved to 3.14).

## 0.2.3

- Docs link to upstream: the catalog documentation now links Oxigraph, pyoxigraph, W3C RDF 1.1 Concepts and SPARQL 1.1 Query (vgi-lint VGI183).

## 0.2.2

- Every catalog example now shows real output in a fresh session. The `rdf_neighbors` and `rdf_reachable` examples queried `urn:example:alice`, which nothing creates, so they always returned 0; the `rdf_tables` and `rdf_rows` examples needed user tables and returned no rows or failed. They are now executable examples that create a small people/orders dataset inside a transaction, run the query with its expected result, and roll back.
- `rdf_sparql` gains an inline SPARQL `VALUES` example that returns typed terms without stored data; it replaces the table-dependent composed examples in the schema-level list.
- Examples that intentionally return 0 (absent nodes, a fresh attachment's tables) say so in their descriptions.
- `rdf_tables`, `rdf_neighbors` and `rdf_reachable` keep one illustrative example that is honestly 0 in a fresh database; `rdf_rows` keeps its illustrative people example, which needs that table. Removed the `rdf_tables` VGI902 waiver, ran vgi-lint examples one at a time (the worker serves an attachment from one process), and added the catalog support contact and policy tags.

## 0.2.1

- Shortened the `rdf` catalog comment (and the bundled starter example catalog's) to one plain-text sentence; the full description stays in the catalog `vgi.doc_md` and `vgi.doc_llm` tags.

## 0.2.0

- Added local SPARQL SELECT/ASK/CONSTRUCT/DESCRIBE, typed RDF terms, mapping discovery, stable row identities, neighbors and bounded cycle-safe reachability.
- Added transactional RDF edges for SQL foreign keys and atomic external RDF import/drop with protected system graphs.
- Added derived RDF key indexes with exact collision checks and automatic migration of earlier database mappings.
- Replaced repeated scan queries with one bounded Arrow spool per statement; recorded a 100,000-row benchmark and before/after evidence.
- Hardened write-intent fencing, commit publication, process-death recovery and disk-full handling; added lease expiry, receipt retention, status and integrity auditing.
- Added authenticated HTTP deployment, confined database paths, server-enforced read-only mode, query deadlines/resource bounds and a constrained Docker Compose profile.
- Added runnable SQL/graph examples, crash/concurrency/security tests, Linux container tests, and a production-image SQL/backup/restart lifecycle check.
- Added composed catalog examples combining `rdf_tables`, `rdf_quads`, `rdf_rows` and `rdf_sparql` (per-table statement counts, foreign-key edges per relationship, incoming references per row), plus a composed agent task.

## 0.1.0

- Implemented native VGI SELECT, INSERT, UPDATE, DELETE, CREATE TABLE and DROP TABLE with Oxigraph RDF persistence.
- Added snapshot transactions, cross-process state, atomic commit, conflict detection, rollback and stable statement scans.
- Added SQL schemas, defaults, composite primary/unique keys, immediate foreign keys and canonical constraint enforcement.
- Preserved SQL NULLs, duplicate rows, nested values and temporal/decimal precision through an RDF cell codec.
- Added RDF inspection, dataset export, consistent backup, snapshot reclamation and transaction recovery commands.
- Added storage, codec, catalog, stock-DuckDB and Haybarn transport tests; retained starter regression tests on a separate example worker.
- Documented native engine limitations for CHECK, DELETE RETURNING and the HUGEINT Arrow mapping.
