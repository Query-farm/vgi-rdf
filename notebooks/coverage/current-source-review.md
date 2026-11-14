# Catalog and source review

The public v0.2.0 tag and current source both resolve to 6ea6da05f7628ac4116bbbbdf6ea5bb96e51053d. Actual Haybarn attachment exports six named public functions, zero preexisting tables and zero views; all six are included. Dynamic user tables are created by the examples and do not add fixed worker catalog names.

The registry includes four internal scan/write handlers (rdf_scan, rdf_insert, rdf_update, rdf_delete). RdfCatalog.schema_contents explicitly marks them table_only; they are absent from the public function catalog and are invoked by SQL table operations. They are not excluded merely because they write. The notebooks exercise owned-store CREATE/INSERT/UPDATE, rollback/commit, and read-only reattachment.

Complete source registration lists are RdfQuads + GRAPH_FUNCTIONS + INTERNAL_FUNCTIONS in catalog.py. GRAPH_FUNCTIONS lists RdfSparql, RdfTables, RdfRows, RdfNeighbors and RdfReachable. No other public fixed names or authentication functions are exported. All source/build/dependency content is identical to the actual runtime release.

CI43615633297 succeeded at this exact revision, covering lint/unit/examples, stdio/launch/HTTP SQL and metadata jobs. Reused for notebook-only publication; no worker source or dependency changes.
