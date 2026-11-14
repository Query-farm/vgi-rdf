# Changelog

## 0.2.0

- Added local SPARQL SELECT/ASK/CONSTRUCT/DESCRIBE, typed RDF terms, mapping discovery, stable row identities, neighbors and bounded cycle-safe reachability.
- Added transactional RDF edges for SQL foreign keys and atomic external RDF import/drop with protected system graphs.
- Added derived RDF key indexes with exact collision checks and automatic migration of earlier database mappings.
- Replaced repeated scan queries with one bounded Arrow spool per statement; recorded a 100,000-row benchmark and before/after evidence.
- Hardened write-intent fencing, commit publication, process-death recovery and disk-full handling; added lease expiry, receipt retention, status and integrity auditing.
- Added authenticated HTTP deployment, confined database paths, server-enforced read-only mode, query deadlines/resource bounds and a constrained Docker Compose profile.
- Added runnable SQL/graph examples, crash/concurrency/security tests, Linux container tests, and a production-image SQL/backup/restart lifecycle check.

## 0.1.0

- Implemented native VGI SELECT, INSERT, UPDATE, DELETE, CREATE TABLE and DROP TABLE with Oxigraph RDF persistence.
- Added snapshot transactions, cross-process state, atomic commit, conflict detection, rollback and stable statement scans.
- Added SQL schemas, defaults, composite primary/unique keys, immediate foreign keys and canonical constraint enforcement.
- Preserved SQL NULLs, duplicate rows, nested values and temporal/decimal precision through an RDF cell codec.
- Added RDF inspection, dataset export, consistent backup, snapshot reclamation and transaction recovery commands.
- Added storage, codec, catalog, stock-DuckDB and Haybarn transport tests; retained starter regression tests on a separate example worker.
- Documented native engine limitations for CHECK, DELETE RETURNING and the HUGEINT Arrow mapping.
