# vgi-rdf

A Python VGI worker that exposes **persistent SQL tables and graph queries over the same Oxigraph RDF dataset**. Use ordinary `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, and `DROP TABLE` from DuckDB or Haybarn, alongside SPARQL and graph traversal. Foreign keys become RDF edges that stay consistent with SQL transactions.

Version 0.2 targets a single host with local durable storage and authenticated clients in one trust domain. It includes crash recovery, bounded query execution, backup/restore, integrity checks and container deployment. The validated workload is 100,000 rows; capacity and latency for a different workload require measurement. This is not a distributed database or a service with an established uptime SLA.

## Run locally

Requires Python 3.13+ and [uv](https://docs.astral.sh/uv/). The lockfile pins the tested dependencies.

```sh
git clone https://github.com/Query-farm/vgi-rdf.git
cd vgi-rdf
make build
```

In **stock DuckDB 2.0.0**:

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm
    WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;

ATTACH 'rdf' AS rdf (
    TYPE vgi,
    LOCATION 'uv run --frozen vgi-rdf-worker',
    database 'data/example'
);

CREATE TABLE rdf.main.people (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    visits INTEGER DEFAULT 0
);
INSERT INTO rdf.main.people(id, name) VALUES (1, 'Ada'), (2, 'Grace');
SELECT * FROM rdf.main.people ORDER BY id;
UPDATE rdf.main.people SET visits = visits + 1 WHERE id = 1 RETURNING *;
DELETE FROM rdf.main.people WHERE id = 2;
DROP TABLE rdf.main.people;
DETACH rdf;
```

Run the SQL client from this checkout for this development launch. `database` is a **directory on the worker's machine**, not a Turtle file. Omitting it uses `VGI_RDF_DATABASE` if set; otherwise the worker creates an isolated temporary database and removes it on `DETACH`. Persistent directories survive worker restarts and reattachment.

In **Haybarn 2.0.0rc11**, install/load the extension with `INSTALL vgi FROM community; LOAD vgi;`, then use the same `ATTACH`. Run `FORCE INSTALL` from the appropriate repository if a stale installed extension speaks an older protocol.

Attach an existing store with `read_only true` to reject all table, schema, and row mutations. Worker stdout is reserved for Arrow IPC; diagnostics go to stderr.

## SQL behavior

| Surface | Behavior |
|---|---|
| `SELECT` | Projection is pushed into the scan; filtering, joins, grouping, aggregates, ordering, subqueries and windows run in the host. A scan evaluates once into a bounded Arrow spool, then emits batches of up to 2,048 rows. |
| `CREATE TABLE` | Typed columns, defaults, `NOT NULL`, primary keys, unique keys, immediate foreign keys, `IF NOT EXISTS`, `OR REPLACE`, and `AS SELECT`. |
| `INSERT` | Column lists, defaults, multiple values and `INSERT … SELECT`; duplicate rows remain distinct when no unique key prevents them. |
| `UPDATE` | SQL predicates and expressions; stable internal row identities allow updating duplicate rows without requiring a primary key. |
| `DELETE` | Deletes only matching row identities; omitted predicates delete all rows. |
| `RETURNING` | Insert/update returned rows, plus delete returned rows on a compatible engine. The worker also implements VGI OLD/NEW change results. |
| `DROP TABLE` | Transactional removal of metadata and rows; `IF EXISTS`, dependency rejection, and `CASCADE`. Cascading a referenced table removes dependent foreign-key constraints and preserves child tables. |
| Schemas | `CREATE SCHEMA`, `DROP SCHEMA`, `IF EXISTS`, `IF NOT EXISTS` and `CASCADE`; one schema component, including quoted names containing dots. `main` cannot be dropped. |
| Constraints | Composite keys; multiple NULLs allowed by UNIQUE; primary keys imply NOT NULL. Foreign keys use MATCH SIMPLE with immediate NO ACTION/RESTRICT enforcement on both child and parent writes. |

Identifiers follow DuckDB's ASCII case-insensitive matching while preserving original spelling. Names are never interpolated into RDF queries without term encoding. Schema/table names travel as separate components. A user column named `rowid` or `__rdf_rowid` does not collide with internal row identity.

The backend implements CHECK expressions and canonical constraint ADD/DROP/RENAME/REPLACE, validates existing rows before accepting a new constraint, and guards changes with a definition revision. SQL availability depends on the engine's VGI adapter; see the limitations below.

## Transactions and concurrency

```sql
BEGIN;
INSERT INTO rdf.main.people(id, name) VALUES (3, 'Lin');
UPDATE rdf.main.people SET visits = 10 WHERE id = 3;
ROLLBACK; -- neither write persists
```

Each transaction pins an immutable committed generation. Its first write creates a private Oxigraph backup using hard links to immutable RocksDB files where supported. Subsequent batches see their own writes. Commit flushes the store and atomically publishes a new generation pointer; rollback discards the private store. DDL participates in the same transaction.

Different worker processes share filesystem transaction records and locks. No connection or data state is held only in a Python global. Readers retain a consistent snapshot while other clients commit. Writers use a **database-wide optimistic conflict check**: if another writer has committed since the snapshot began, commit fails with a serialization conflict. Retry the whole transaction. This deliberately favors correctness over concurrent writer throughput.

Scans freeze a statement snapshot, including inside explicit transactions. `INSERT INTO t SELECT … FROM t` therefore does not keep discovering its own newly inserted rows. An error in any write batch aborts the transaction, so earlier batches of a failed statement cannot leak into committed data. Commit and rollback are idempotent, including recovery after publication interrupted the commit receipt write.

The directory layout is:

```text
CURRENT.json                 # atomically published generation and format version
versions/<generation>/       # immutable committed Oxigraph databases
transactions/<id>/state.json # persistent transaction state and commit receipts
transactions/<id>/data/      # private writable Oxigraph database, when needed
transactions/<id>/scans/     # statement snapshots, removed at commit/rollback
```

Use a local filesystem with reliable locking, atomic rename and sufficient free space for private generations and statement spools. macOS ARM64 and Linux ARM64 in Docker were tested locally; Windows CI is configured but has not been run in this checkout. Network filesystems, multi-host shared volumes and Windows durability are outside the validated deployment profile. POSIX directory entries and commit pointers are fsynced. Recovery tests kill processes at publication boundaries and inject disk-full failures; they do not simulate every hardware or power-loss fault.

## RDF representation and types

The catalog is the named graph `<urn:vgi:rdf:catalog>`. Every table has a stable resource and a separate row graph. Each row has a resource IRI and a numeric row-ID statement, even when every SQL column is NULL. Each non-NULL cell becomes a column-property statement. SQL NULL means the property is absent; duplicate SQL rows have different resource IRIs.

Primitive cells use RDF literals with XSD datatypes: strings, booleans, integers, decimal, double, date, time, dateTime and base64Binary. Decimal scale and nanosecond timestamp/time precision survive round trips. Nested/list/map/interval cells and values outside Python's datetime range use the private literal datatype `urn:vgi:rdf:arrowIPC`, containing a base64-encoded one-cell Arrow stream. The catalog retains the exact Arrow schema and logical-type metadata in RDF.

`urn:vgi:rdf:relationships` contains child-row → foreign-key predicate → parent-row edges, including composite keys. Row identities survive updates. SQL mutations, constraint changes, rollback and cascading table drops maintain these edges transactionally. `urn:vgi:rdf:key-index` stores derived primary/unique-key hash buckets as RDF; exact SQL equality checks resolve collisions. Indexes use DuckDB 2.0.0 equality/hash semantics, so that dependency is pinned. Mapping version 2 records these derived graphs in the catalog.

```sql
-- Inspect actual stored RDF terms, including their datatype annotations.
SELECT graph, predicate, count(*) AS statements
FROM rdf.main.rdf_quads()
GROUP BY graph, predicate ORDER BY graph, predicate;

-- Restrict inspection to catalog metadata.
SELECT subject, predicate, object
FROM rdf.main.rdf_quads(graph_iri := 'urn:vgi:rdf:catalog')
ORDER BY subject, predicate, object;
```

The inspection function returns N-Triples term strings. Direct writes to the underlying Oxigraph directories are outside the SQL constraint/transaction contract. This is an application-specific RDF mapping, not the W3C Direct Mapping vocabulary or an automatic import of arbitrary RDF into SQL tables.

## Graph features and examples

| Function | Result and use |
|---|---|
| `rdf_sparql(query, max_rows := 10000, timeout_ms := 30000, union_default := false)` | Local SPARQL SELECT, ASK, CONSTRUCT and DESCRIBE, including OPTIONAL, aggregates and property paths. SELECT variables are `STRUCT(kind, value, datatype, language)`; unbound terms are SQL NULL. ASK returns `answer BOOLEAN`; graph queries return subject/predicate/object term structs. |
| `rdf_tables(schema_name := '')` | Discover table, column and foreign-key IRIs, graph IRIs and target table IRIs. Empty schema selection includes all schemas. |
| `rdf_rows(schema_name, table_name)` | Ordinary typed SQL columns plus stable `__rdf_iri` for joining graph results to rows. Appends underscores if that name collides with a user column. |
| `rdf_neighbors(node, graph_iri := 'urn:vgi:rdf:relationships', predicate_iri := '', direction := 'out', max_rows := 10000, timeout_ms := 30000)` | Resource-to-resource edges: source, predicate, target, graph and neighbor identifiers. Direction is out, in or both. |
| `rdf_reachable(node, max_depth := 8, …)` | The same graph/direction/limit options as neighbors; returns distinct nodes and minimum depth, with cycle detection. Depth is bounded to 1–64; the start node is excluded. |
| `rdf_quads(graph_iri := '')` | Inspect stored named-graph quads as subject, predicate, object and graph term strings. |

SPARQL sees named graphs through `GRAPH`; use `union_default := true` to make their union the default graph. `SERVICE` is disabled and the query interface accepts no mutations. Term `kind` distinguishes `iri`, `blank`, `literal` and quoted `triple`; `value` is the lexical value, with datatype/language retained separately. Traversal accepts raw IRIs or `_:label` blank nodes and ignores literal-valued edges. Empty `graph_iri` searches all graphs; empty `predicate_iri` accepts all predicates.

For a fresh attachment, this example joins SQL rows to foreign-key edges through SPARQL:

```sql
CREATE TABLE rdf.main.people(id INTEGER PRIMARY KEY, name VARCHAR);
CREATE TABLE rdf.main.orders(id INTEGER, buyer INTEGER REFERENCES rdf.main.people(id));
INSERT INTO rdf.main.people VALUES (1, 'Ada'), (2, 'Grace');
INSERT INTO rdf.main.orders VALUES (10, 1), (11, 1), (12, 2);

SELECT p.name, count(*) AS orders
FROM rdf.main.rdf_sparql('
    SELECT ?buyer WHERE {
        GRAPH <urn:vgi:rdf:relationships> { ?order ?relationship ?buyer }
    }') AS g
JOIN rdf.main.rdf_rows('main', 'people') AS p ON g.buyer.value = p.__rdf_iri
GROUP BY p.name ORDER BY p.name;
-- Ada: 2, Grace: 1

SELECT kind, name, iri, graph_iri
FROM rdf.main.rdf_tables(schema_name := 'main') ORDER BY table_name, kind, name;

-- Edges materialized by each foreign key, with the tables it connects
SELECT fk.table_name AS referencing_table, target.table_name AS referenced_table, count(q.subject) AS edges
FROM rdf.main.rdf_tables() AS fk
JOIN rdf.main.rdf_tables() AS target ON target.kind = 'table' AND target.iri = fk.target_iri
LEFT JOIN rdf.main.rdf_quads(graph_iri := 'urn:vgi:rdf:relationships') AS q ON q.predicate = '<' || fk.iri || '>'
WHERE fk.kind = 'foreign_key'
GROUP BY fk.table_name, target.table_name ORDER BY referencing_table;
-- orders -> people: 3
```

Import external RDF using the admin tool, which publishes it atomically:

```sh
uv run vgi-rdf-admin ./data/example import examples/network.ttl \
    --format turtle --graph https://example.org/network
```

```sql
SELECT node, depth
FROM rdf.main.rdf_reachable('https://example.org/alice',
    graph_iri := 'https://example.org/network', max_depth := 3)
ORDER BY depth, node;
-- bob and dana at depth 1; carol at depth 2

SELECT person.value AS person, name.value AS name, name.language AS language
FROM rdf.main.rdf_sparql('
    PREFIX ex: <https://example.org/>
    SELECT ?person ?name WHERE {
        GRAPH ex:network { ex:alice ex:knows+ ?person . ?person ex:name ?name }
    }') ORDER BY person;
```

Turtle/N-Triples imports require a destination graph. TriG/N-Quads can retain named graphs or use `--graph` to select one destination; otherwise default/blank graph names are rejected. `--replace` replaces the selected graph in the same transaction. Blank nodes are renamed per import. Imports cannot modify `urn:vgi:rdf:` system graphs, and RDF/XML, JSON-LD, remote fetching and arbitrary SPARQL UPDATE are not enabled. Remove external data with `vgi-rdf-admin DATABASE drop-graph GRAPH_IRI`.

Run `make test-examples` for the complete, asserted [graph demo](examples/graph_demo.py), including SQL joins, relationship traversal, imported Turtle, language tags, property paths and rollback. It uses a disposable database.

## Deploy with authenticated HTTP

The [Compose configuration](deploy/compose.yaml) runs as UID 10001 with a persistent volume, read-only container root, 2 GiB memory/2 CPU limits, bounded logs and a loopback-only published port. The SDK's internal coordination database is directed to `/data/.vgi-runtime.sqlite`; application rows, definitions and derived indexes remain RDF. It requires an authentication token and stable signing key. Generate a private environment file once:

```sh
umask 077
python3 - <<'PY'
import secrets
from pathlib import Path
with Path('deploy/.env').open('x') as output:
    output.write('VGI_BEARER_TOKENS=' + secrets.token_urlsafe(32) + '=operator\n')
    output.write('VGI_SIGNING_KEY=' + secrets.token_hex(32) + '\n')
PY
docker compose --env-file deploy/.env -f deploy/compose.yaml up --build -d
curl -fsS http://127.0.0.1:8000/health
```

After loading VGI in the SQL client, use the token portion before `=operator`:

```sql
ATTACH 'rdf' (TYPE vgi, LOCATION 'http://127.0.0.1:8000',
    bearer_token 'YOUR_RANDOM_TOKEN', database '/data/default');
SELECT answer FROM rdf.main.rdf_sparql('ASK {}');
```

Use a client secret facility or inject credentials at runtime; avoid storing real tokens in SQL history. Keep `deploy/.env` private and retain the signing key across restarts. For remote clients, put a TLS reverse proxy in front of the loopback endpoint with request/concurrency limits. The SDK also supports JWT issuer/audience authentication. Non-loopback worker listeners refuse to start without authentication and `VGI_RDF_ROOT`.

`VGI_RDF_ROOT` restricts database paths, including resolved symlinks. It is a filesystem boundary, not per-user authorization: authenticated principals share access to databases under that root. Deploy separate instances/roots for different trust domains. `VGI_RDF_READ_ONLY=true` rejects SQL mutations even if the client requests a writable attachment; administrative commands remain privileged local operations. A read-only SQL service still needs writable transaction/spool storage.

`/health` checks process health, not database integrity or disk capacity. Monitor container restarts, error logs, disk utilization, `vgi-rdf-admin DATABASE status`, conflict rates and query latency. Back up before upgrading; run `upgrade` and `check` using the new image before serving a migrated database. Writable attachments automatically upgrade pre-v2 derived mappings; read-only attachments require an explicit upgrade first. Keep an untouched backup when rolling back to an older release.

Verify a built deployment with `make test-container`. It creates a disposable Compose project, checks rejected/accepted authentication, SQL/graph results, integrity, backup/restore, restart persistence and forced read-only access, then removes its own volume. Normal `docker compose down` preserves production data; `down --volumes` deletes it.

## Resource limits

All values below are positive integer environment settings with the `VGI_RDF_` prefix. Exceeding a limit raises an error; results are never silently truncated. SQL scans are spooled once, so a host `LIMIT` or WHERE predicate does not avoid scanning the source. Query deadlines apply to child evaluation, including child startup; snapshot preparation and lock waits are separate.

| Setting suffix | Default | Applies to |
|---|---:|---|
| `MAX_QUERY_ROWS` | 100000 | Ceiling for SPARQL/traversal `max_rows` (caller default 10000) |
| `QUERY_TIMEOUT_MS` | 30000 | Ceiling for graph query `timeout_ms` |
| `MAX_QUERY_BYTES` | 262144 | SPARQL query text |
| `MAX_RESULT_BYTES` | 67108864 | Graph result Arrow file |
| `MAX_TRAVERSAL_EDGES` | 1000000 | Examined traversal edges |
| `MAX_SCAN_ROWS` | 10000000 | Relational/quad scan rows |
| `MAX_SCAN_BYTES` | 1073741824 | Relational/quad scan Arrow file |
| `SCAN_TIMEOUT_MS` | 120000 | Relational/quad evaluation deadline |
| `MAX_WRITE_BATCH_BYTES` | 67108864 | Incoming SQL write batch |
| `MAX_VALIDATION_ROWS` | 250000 | Rows materialized by full constraint validation |
| `MAX_VALIDATION_BYTES` | 268435456 | Arrow bytes materialized by full constraint validation |
| `MAX_IMPORT_BYTES` | 67108864 | External RDF input file |

These are per-operation limits, not whole-service memory or storage quotas. Query planning, native indexes and Python values can use more memory than serialized Arrow results. Keep container/OS resource limits and provision disk for concurrent snapshots. Interrupted writes are fenced by a durable write-intent state; query children have independent deadlines if their parent dies.

## Maintenance

The separate admin executable writes ordinary output; it is not the Arrow worker.

```sh
# Consistent RDF export (refuses to overwrite an existing output file).
uv run vgi-rdf-admin ./data/example export ./example.nq
uv run vgi-rdf-admin ./data/example export ./example.trig --format trig

# Standalone, consistent backup into a new directory.
uv run vgi-rdf-admin ./data/example backup ./data/example-backup

# Reclaim obsolete generation files, preserving live transaction snapshots.
uv run vgi-rdf-admin ./data/example vacuum

# JSON operational status and a read-only consistency audit.
uv run vgi-rdf-admin ./data/example status
uv run vgi-rdf-admin ./data/example check

# Upgrade old derived relationship/key graphs atomically.
uv run vgi-rdf-admin ./data/example upgrade

# Inspect transactions left active after a client crash.
uv run vgi-rdf-admin ./data/example transactions
# Only after confirming that the selected client/transaction is abandoned:
uv run vgi-rdf-admin ./data/example rollback TRANSACTION_ID

# Optional policy: expire transactions idle for an hour, skipping locked operations.
uv run vgi-rdf-admin ./data/example reap --idle-seconds 3600
# Retain commit/rollback retry receipts for seven days while reclaiming old files.
uv run vgi-rdf-admin ./data/example vacuum --receipts-older-than 604800
```

Statement snapshots are cleaned up on commit/rollback. Schedule `vacuum` to reclaim obsolete generations. By default terminal transaction receipts remain indefinitely; `--receipts-older-than` explicitly bounds the idempotent retry window, after which old tokens are unknown. Ancestry needed for active snapshots or ambiguous commits is preserved. `reap` is an operator-selected idle lease: even a healthy client loses its transaction if it stays idle beyond that lease. Choose a lease longer than legitimate pauses and schedule it only if that contract fits your clients. Nothing expires automatically.

In Compose, prefix commands with `docker compose --env-file deploy/.env -f deploy/compose.yaml exec -T rdf` and use `/data/default`. Backups exclude uncommitted changes and can be attached using their directory as `database`. Copy backups to separate durable storage and periodically restore and run `check`; a backup on the same volume does not survive volume loss. Export is an interoperable RDF dataset, while `backup` produces a directly attachable database directory. `check` validates SQL constraints, row representation and rebuilt derived indexes without changing data, subject to the validation limits above.

## Supported scope and engine limits

- All six requested SQL operations are tested through real VGI engines. Primary/unique checks use RDF hash indexes and exact SQL comparisons. Foreign-key validation and parent updates/deletes can still materialize referenced/dependent tables. DDL constraint validation and integrity checks also need full bounded table reads.
- SQL predicates run in the host. The installed SDK's optional filter evaluator imports Haybarn, which conflicts with the stock DuckDB native library used for constraints; the worker deliberately does not load both. Point lookups still scan projected columns. High write concurrency, low-latency point serving and larger datasets need workload-specific capacity tests.
- Stock DuckDB 2.0.0 lacks `GetDeleteReturningVirtualColumns`, so `DELETE … RETURNING` is rejected by its binder. Plain DELETE works. The Haybarn 2.0.0rc11 SQL suite tests DELETE RETURNING successfully.
- Both tested engines reject native canonical `CHECK` definitions before calling the worker. CHECK enforcement and canonical constraint mutations are tested through the backend/catalog API, not claimed as passing native SQL CHECK support.
- The current VGI Arrow bridge exposes `HUGEINT` as `DECIMAL(38,0)`; values beyond 38 decimal digits cannot be transported by that mapping. The type tests exercise the supported range, not the entire 128-bit integer domain.
- Self-referencing foreign keys, cascading referential actions, deferred constraints, nested-field constraints, generated columns, SQL time travel, arbitrary ALTER COLUMN operations, views, user indexes, custom table layouts and arbitrary SPARQL mutations are not implemented. Unsupported operations fail explicitly.

## Measured workload

The [recorded benchmark](benchmark-results.json) uses stock DuckDB 2.0.0 and Python 3.14.4 on macOS ARM64, one client, 100,000 rows and a three-column table with a primary key. Timings include VGI transport and storage work: insert **17.51 s**, aggregate scan **1.34 s**, point lookup **2.03 s**, point update **2.74 s**, delete within a rolled-back transaction **2.02 s**, restored aggregate scan **1.35 s**. The backup was a local Oxigraph hard-link snapshot, not a remote copy. Reported logical storage bytes count hard-linked files more than once.

One-pass scan materialization reduced this workload's aggregate scan from 12.63 s to 1.34 s and point update from 27.98 s to 2.74 s; [earlier measurements](benchmarks/before-scan-optimization.json) are retained. These observations are not an SLA or a general throughput claim. Reproduce with `uv run python examples/benchmark.py --rows 100000 --output benchmark-results.json`. Concurrent correctness is separately tested with four writer processes and transaction retries.

## Tests and provenance

```sh
make check                 # formatting, static typing, docstrings, pytest, all SQL transports, metadata
make test-unit             # codec/storage/catalog tests and stock-DuckDB integration tests
make test-sql               # Haybarn 2.0.0rc11 SQLLogicTest suite over stdio
make test-sql TRANSPORT=launch
make test-sql TRANSPORT=http
make test-examples          # asserted SQL/graph demonstration
make test-container         # authenticated production-image lifecycle test
make lint-explain
```

Pytest includes real stock **DuckDB 2.0.0**, loading the signed VGI extension from `query_farm`. SQLLogicTest uses **Haybarn 2.0.0rc11**, loading VGI from `community`, over stdio, launch and HTTP. These are reported separately. Tests use disposable data, not the example directory. Engine/extension installation requires network access; data fixtures themselves are local.

Tests cover CRUD, CTAS, defaults, empty results, NULLs, duplicate rows, multiple batches, returned rows, joins, windows, type round trips, constraints, rollback, concurrent clients, worker/process restart, authenticated/read-only HTTP, path confinement, RDF export, scan stability, backup, vacuum and interrupted commit recovery. Graph tests cover SPARQL result forms/terms, property paths, identity joins, foreign-key edge maintenance, traversal cycles/limits, import rollback and protected graphs. Fault tests cover process death, disk-full receipt failure, hash collisions, stale leases, scan limits and integrity corruption. The original starter tests remain under a separate example worker; the product catalog does not expose the greeting/REST demonstrations.

`vgi-worker.json` is a portable stock-DuckDB verification contract. The additional verifier was run with:

```sh
uv run --with duckdb==2.0.0 python \
  /Users/rusty/.agents/skills/vgi-worker/scripts/verify_worker.py \
  --project . --output verification.json
```

Starter: [Query-farm/vgi-starter-python](https://github.com/Query-farm/vgi-starter-python), version 1.3.0, revision `9cca9105474d6e0052b8e343c8ea21f3d1a7503c`. Its baseline `make check` passed before adaptation. Resolved worker SDK: `vgi-python 0.51.1`; RDF engine: `pyoxigraph 0.5.13`. The starter SDK constraints and recipes are preserved. The metadata gate has scoped exemptions for the `rdf_rows` example's pre-existing table requirement and for the `rdf_tables` composed examples returning no rows on the linter's empty database; both are exercised by real SQL tests and the runnable demo. No public support/SLA policy is invented.

Implementation: [storage.py](src/rdf_worker/storage.py), [model.py](src/rdf_worker/model.py), [codec.py](src/rdf_worker/codec.py), [catalog.py](src/rdf_worker/catalog.py), [functions.py](src/rdf_worker/functions.py). `make template-diff` can inspect upstream starter changes before an SDK upgrade.

## Executed SQL notebook

See [notebooks](notebooks/README.md) for a portable, executed Haybarn walkthrough with charts and a separate parameter experiment.

## Notebook examples

[Two executed SQL-first notebooks](notebooks/README.md) explore bounded foreign-key
lineage and SQL/SPARQL revenue reconciliation, then transaction-consistent graph
edits with rollback, commit and read-only reopening. They use the public v0.2.0
release, synthetic owned data, vector figures and direct Open in Colab links.
