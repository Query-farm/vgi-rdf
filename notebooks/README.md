# RDF notebook examples

Two independent, executed SQL-first notebooks use the public **v0.2.0** worker:

| Notebook | Question | Open in Colab |
| --- | --- | --- |
| [Follow foreign keys across query models](01_follow_foreign_keys_across_query_models.ipynb) | Does bounded reachability match the known SQL lineage, and do SPARQL paths preserve regional revenue? | [Run](https://colab.research.google.com/github/Query-farm/vgi-rdf/blob/main/notebooks/01_follow_foreign_keys_across_query_models.ipynb) |
| [Foreign-key edits move graph edges](02_foreign_key_edits_move_graph_edges.ipynb) | Do graph edges follow rollback and commit, and does the change survive a read-only reopen? | [Run](https://colab.research.google.com/github/Query-farm/vgi-rdf/blob/main/notebooks/02_foreign_key_edits_move_graph_edges.ipynb) |

Both notebooks create small, explicitly synthetic SQL datasets in their own temporary
directory. No account, API key or preexisting database is needed. The second uses
the same directory for its reopen, then deletes it at cleanup. The companion
isolates inherited RDF storage configuration and restores it afterward; SQL secrets
start empty. The visible `database` option identifies this owned store.

## Run

In Colab, use the link and **Run all**. The first setup cell installs only the base
[shared example tools](https://github.com/Query-farm/vgi-example-notebooks); it keeps
compatible installed plotting and kernel libraries. Hash-verified companions download
automatically, so cloning this repository is unnecessary. First execution needs network
access for the shared package, released worker and VGI extension. A local preloaded-kernel
compatibility test is distinct from a manual hosted-Colab execution.

Locally, from this directory with `uv` available:

```sh
uv sync --locked
uv run --locked vgi-notebooks --timeout 300
```

The `[runner]` dependencies are for local publication. Both visible ATTACH calls use
`uvx --python 3.13 --from git+https://github.com/Query-farm/vgi-rdf@v0.2.0 vgi-rdf-worker`.
The release owns its dependencies; no local worker build or transitive override list
is needed. A small transaction adapter keeps JupySQL cells on one Haybarn session,
so explicit BEGIN/ROLLBACK/COMMIT and session variables span cell boundaries.

## Experiments

Change the tagged parameter cell, restart and run all, or save a separate experiment:

```sh
uv run --locked vgi-notebooks 01_follow_foreign_keys_across_query_models.ipynb --parameters '{"MAX_DEPTH":3}' --output-dir artifacts/depth-three --timeout 300
uv run --locked vgi-notebooks 02_foreign_key_edits_move_graph_edges.ipynb --parameters '{"TRANSFER_TO":3}' --output-dir artifacts/transfer-cedar --timeout 300
```

The first discovers the extra region node while leaving all ledger amounts and
revenue totals fixed. The second transfers to Cedar instead of Briar while preserving
four tasks, three owner edges, all row identities and the unassigned task. Complete
SQL checks reconcile the returned nodes, stored RDF statements and typed rows.

## Interpretation and presentation

These examples compare two representations of the same synthetic data; agreement
is a consistency check, not independent source verification. Bounded reachability
excludes the start and reports minimum edge depth. NULL column values omit their
property triple but preserve row identity. No concurrent-writer, remote-storage,
cross-database transaction or backup-recovery claim is made. The second notebook
reads the preview into a dataframe: Haybarn cannot write its local tables and the
RDF database within the same transaction.

Figures render as SVG in Jupyter/Colab. Content-addressed SVG assets also support
GitHub's notebook renderer. GitHub does not honor editor cell-folding metadata,
so setup is concise and the release connection remains visible. The canonical
[notebook standard](https://github.com/Query-farm/vgi-example-notebooks/blob/main/docs/standard.md)
and runner live in the shared repository. `coverage.json` records all six named
public functions against the actual release catalog and identical current source.
