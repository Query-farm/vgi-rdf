# RDF notebook

[Follow foreign keys across query models](01_follow_foreign_keys_across_query_models.ipynb) builds an explicitly synthetic USD ledger, follows a line item's ownership chain with bounded RDF traversal, reconciles SQL and SPARQL totals, and verifies rollback. Every write goes to a fresh disposable local database.

Install [uv](https://docs.astral.sh/uv/). From this directory, or a complete copy of it:

```sh
uv sync --frozen
uv run --frozen python execute.py
uv run --frozen python execute.py --parameters '{"MAX_DEPTH":3}' --output-dir validation/depth-three
```

The default depth is 2. The experiment allows a third foreign-key edge, reaching the region; ledger rows and regional revenue remain unchanged. Run all cells after editing a parameter. Saved defaults include outputs, and the runner only replaces them after successful execution. Overrides require a separate destination; unknown parameters and source-directory destinations are rejected. Experiment copies include their branding, provenance, helper, runner and locked environment.

For interactive use, register this environment and select it in Jupyter:

```sh
uv run --frozen python -m ipykernel install --user --name rdf-notebooks --display-name 'Python (RDF notebook)'
```

The visible ATTACH runs `uvx --python 3.13 --from git+https://github.com/Query-farm/vgi-rdf@43d3f598295456555af632ddf008a370f3fcfd5e vgi-rdf-worker`. It installs the public immutable worker revision; it requires no local worker checkout or build. Dependencies, the published Haybarn 2.0.0rc11/VGI extension, and the Git source require internet access. Python is provisioned by uv. This launch was tested on macOS arm64; other platforms were not executed. To upgrade, change the literal revision and rerun defaults, the experiment and all checks. The notebook environment is locked separately from the worker's dependencies.

`WorkerSandbox` clears inherited `VGI_RDF_*` settings, gives the worker a new filesystem root, and restores the environment after closing the SQL connection. With no `database` option or inherited database path, the worker creates a temporary RDF store within that root and deletes it on detach. No existing RDF database is opened. Haybarn gets separate fresh temporary SQL secret storage with persistent secrets disabled; empty inventories are asserted before ATTACH and at cleanup. No SQL secrets are created or existing saved secrets read or changed.

The small `HaybarnSession` adapter keeps JupySQL on one native session for `SET VARIABLE` and `BEGIN`/`ROLLBACK`, with JupySQL autocommit disabled. Analytical SQL remains visible. Stateful or costly worker calls are materialized before local display queries. The rollback preview is read-only and deliberately not materialized into the host database while the RDF transaction is active.

Foreign-key edges point from child to parent. `rdf_reachable` returns distinct nodes at minimum depth and excludes the starting node; a depth bound is not proof of complete discovery. The SPARQL example follows three edges in this known four-table schema. Its one-region-per-line-item check would catch duplicate path multiplicities. SQL and RDF are two views of one store, so agreement is a consistency test, not independent revenue validation. Negative credits remain negative; amounts use decimal USD. The worker's existing exclusion of self-referencing foreign keys is respected.

Nine checks cover lineage equivalence, unique identities, path multiplicity, decimal totals, null/credit handling and rollback. Two charts are embedded and exported to ignored `validation/charts/`. All inputs are invented in visible SQL; [provenance](fixtures/provenance.json) records their origin. [STANDARD.md](STANDARD.md) describes presentation conventions.

Source validation: `make check` with the published Haybarn 2.0.0rc11 SQL test runner passed 153 pytest tests, 157 stdio / 157 Unix-launch / 122 HTTP SQL assertions, formatting, Ruff, mypy, pydoclint, the asserted graph demo and metadata 100/100 L2 (zero errors/warnings, two existing support-policy informational findings). The final metadata gate uses empty temporary SQL secret storage with persistent secrets disabled. Docker deployment was not run; image publication is manual-only. Source publication excluded runtime data and generated reports.

The notebook retains its executed immutable runtime revision. A later metadata-only correction on `main` adds the catalog's top-level `source_url`; the original revision has that URL only as a tag and reports VGI004 in current metadata validation. The correction does not change the demonstrated SQL, RDF, traversal or transaction functions. The validation above describes the corrected source, not the notebook's older runtime pin.
