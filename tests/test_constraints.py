from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from vgi.catalog import OnConflict
from vgi.catalog.constraints import (
    ConstraintChange,
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintKind,
    ConstraintOperation,
    ConstraintsApplyRequest,
    ConstraintValidation,
)
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.sql import SqlExpression

from rdf_worker.catalog import RdfCatalog
from rdf_worker.model import RelationalGraph
from rdf_worker.storage import Database


def test_check_enforcement_and_sql_null_semantics(tmp_path: Path) -> None:
    db = Database(tmp_path)
    definition = ConstraintDefinition(
        kind=ConstraintKind.CHECK, check_expression=SqlExpression.scalar("n > 0", ["main"])
    )
    with db.access(write=True) as snapshot:
        graph = RelationalGraph(snapshot)
        graph.create(["main"], "t", pa.schema([("n", pa.int64())]), [definition], OnConflict.ERROR)
        graph.write(graph.require(["main"], "t"), "insert", pa.record_batch([pa.array([1, None])], names=["n"]))
    for operation, batch in [
        ("insert", pa.record_batch([pa.array([2, -1])], names=["n"])),
        ("update", pa.record_batch([pa.array([-2]), pa.array([1])], names=["n", "__rdf_rowid"])),
    ]:
        with pytest.raises(ValueError, match="CHECK"), db.access(write=True) as snapshot:
            graph = RelationalGraph(snapshot)
            graph.write(graph.require(["main"], "t"), operation, batch)
        with db.access() as snapshot:
            graph = RelationalGraph(snapshot)
            assert graph.scan(graph.require(["main"], "t"))[0].column("n").to_pylist() == [1, None]


def test_invalid_check_definition_is_rejected_at_create(tmp_path: Path) -> None:
    db = Database(tmp_path)
    definition = ConstraintDefinition(
        kind=ConstraintKind.CHECK, check_expression=SqlExpression.scalar("missing > 0", ["main"])
    )
    with pytest.raises(Exception, match="missing"), db.access(write=True) as snapshot:
        RelationalGraph(snapshot).create(["main"], "t", pa.schema([("n", pa.int64())]), [definition], OnConflict.ERROR)
    with db.access() as snapshot:
        assert RelationalGraph(snapshot).get(["main"], "t") is None


def test_constraint_mutation_checks_existing_rows_and_revisions(tmp_path: Path) -> None:
    catalog = RdfCatalog()
    attach = catalog.attach_opaque_data_for(name="rdf", options={"database": str(tmp_path)})
    db = Database(tmp_path)
    with db.access(write=True) as snapshot:
        graph = RelationalGraph(snapshot)
        graph.create(["main"], "t", pa.schema([("n", pa.int64())]), [], OnConflict.ERROR)
        graph.write(graph.require(["main"], "t"), "insert", pa.record_batch([pa.array([1, 1])], names=["n"]))
    with db.access() as snapshot:
        table = RelationalGraph(snapshot).require(["main"], "t")
    ref = CatalogObjectRef(kind="table", schema_path=["main"], name="t")
    unique = ConstraintChange(
        operation=ConstraintOperation.ADD,
        definition=ConstraintDefinition(kind=ConstraintKind.UNIQUE, column_paths=[["n"]]),
        enforcement=ConstraintEnforcement.ENFORCED,
        validation=ConstraintValidation.ALL_ROWS,
    )
    with pytest.raises(ValueError, match="UNIQUE"):
        catalog.table_constraints_apply(
            ConstraintsApplyRequest(
                attach_opaque_data=attach,
                table=ref,
                expected_revision=table.revision,
                changes=[unique.serialize_to_bytes()],
            )
        )
    notnull = ConstraintChange(
        operation=ConstraintOperation.ADD,
        definition=ConstraintDefinition(kind=ConstraintKind.NOT_NULL, column_paths=[["n"]]),
        enforcement=ConstraintEnforcement.ENFORCED,
        validation=ConstraintValidation.ALL_ROWS,
    )
    result = catalog.table_constraints_apply(
        ConstraintsApplyRequest(
            attach_opaque_data=attach,
            table=ref,
            expected_revision=table.revision,
            changes=[notnull.serialize_to_bytes()],
        )
    )
    with pytest.raises(ValueError, match="revision conflict"):
        catalog.table_constraints_apply(
            ConstraintsApplyRequest(
                attach_opaque_data=attach,
                table=ref,
                expected_revision=table.revision,
                changes=[notnull.serialize_to_bytes()],
            )
        )
    info = result.constraint_infos()[0]
    rename = ConstraintChange(operation=ConstraintOperation.RENAME, ref=info.ref, declared_name="required_n")
    renamed = catalog.table_constraints_apply(
        ConstraintsApplyRequest(
            attach_opaque_data=attach,
            table=ref,
            expected_revision=result.definition_revision,
            changes=[rename.serialize_to_bytes()],
        )
    )
    assert renamed.constraint_infos()[0].ref == info.ref
    assert renamed.constraint_infos()[0].definition.declared_name == "required_n"
    drop = ConstraintChange(operation=ConstraintOperation.DROP, ref=info.ref, cascade=False)
    catalog.table_constraints_apply(
        ConstraintsApplyRequest(
            attach_opaque_data=attach,
            table=ref,
            expected_revision=renamed.definition_revision,
            changes=[drop.serialize_to_bytes()],
        )
    )
    with db.access(write=True) as snapshot:
        graph = RelationalGraph(snapshot)
        table = graph.require(["main"], "t")
        assert table.schema.field("n").nullable
        graph.write(table, "insert", pa.record_batch([pa.array([None], type=pa.int64())], names=["n"]))
