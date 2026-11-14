"""VGI's native catalog, transaction, DDL and write dispatch contracts."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any

import pyarrow as pa
from vgi.catalog import (
    AttachOpaqueData,
    Catalog,
    CatalogAttachResult,
    OnConflict,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    Schema,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionOpaqueData,
)
from vgi.catalog.capabilities import (
    CONSTRAINT_CREATE,
    CONSTRAINT_ENFORCE,
    CONSTRAINT_MUTATE,
    CONSTRAINT_RECORDS_READ,
    ROW_SCALAR_EXPRESSION,
    SCHEMA_LIFECYCLE,
    TABLE_METADATA_CREATE,
    TRANSACTION_DDL,
    CatalogCapabilities,
)
from vgi.catalog.constraints import ConstraintsApplyRequest, ConstraintSnapshot, constraint_definitions
from vgi.catalog.dependencies import CatalogObjectRef

from .config import database_path, forced_read_only
from .functions import INTERNAL_FUNCTIONS, RdfQuads, database
from .graph_functions import GRAPH_FUNCTIONS
from .graphs import upgrade
from .meta import CATALOG_TAGS, DESCRIPTION, SCHEMA_TAGS
from .model import RelationalGraph, Table, name_key
from .storage import Database

CATALOG = Catalog(
    name="rdf",
    default_schema="main",
    comment=DESCRIPTION,
    tags=CATALOG_TAGS,
    schemas=[
        Schema(
            path=["main"],
            comment=DESCRIPTION,
            tags=SCHEMA_TAGS,
            functions=[RdfQuads, *GRAPH_FUNCTIONS, *INTERNAL_FUNCTIONS],
        )
    ],
)


def _table_info(table: Table) -> TableInfo:
    return TableInfo(
        name=table.name,
        schema_path=table.path,
        comment=table.comment,
        tags=table.tags or {},
        columns=SerializedSchema(pa.schema([*table.schema, table.scan_schema.field(-1)]).serialize().to_pybytes()),
        constraints=[c.serialize_to_bytes() for c in table.constraints],
        definition_revision=table.revision,
        write_result_modes=dict.fromkeys(["insert", "update", "delete"], "changes"),
    )


class RdfCatalog(ReadOnlyCatalogInterface):
    """Dynamic writable catalog; unrelated optional DDL keeps the SDK's explicit rejection."""

    catalog = CATALOG
    supports_transactions = True

    def attach_opaque_data_for(self, *, name: str, options: dict[str, Any]) -> AttachOpaqueData:
        """Create or open a shared store and encode its identity in the attachment."""
        location = options.get("database") or os.environ.get("VGI_RDF_DATABASE", "")
        temporary = not location
        if temporary:
            root = os.environ.get("VGI_RDF_ROOT")
            if root:
                Path(root).expanduser().mkdir(parents=True, exist_ok=True)
            location = tempfile.mkdtemp(prefix="vgi-rdf-", dir=Path(root).expanduser() if root else None)
        location = str(database_path(location))
        read_only = options.get("read_only", False)
        if not isinstance(read_only, bool):
            raise ValueError("read_only must be a boolean")
        read_only = read_only or forced_read_only()
        if read_only and not temporary and not (Path(location).expanduser() / "CURRENT.json").exists():
            raise ValueError("RDF read-only database does not exist")
        db = Database(location)
        upgrade(db, read_only=read_only)
        return AttachOpaqueData(
            json.dumps({"path": str(db.path), "temporary": temporary, "read_only": read_only}).encode()
        )

    def catalog_attach(self, **kwargs: Any) -> CatalogAttachResult:
        """Advertise a changing catalog and real multi-statement transactions."""
        result = super().catalog_attach(**kwargs)
        return replace(
            result,
            catalog_version_frozen=False,
            supports_catalog_contents=False,
            catalog_version=database(result.attach_opaque_data).version(),
        )

    def catalog_detach(self, *, attach_opaque_data: AttachOpaqueData) -> None:
        """Remove only automatically allocated temporary databases on DETACH."""
        info = json.loads(attach_opaque_data)
        if info["temporary"]:
            shutil.rmtree(info["path"], ignore_errors=True)

    def catalog_capabilities(self, *, attach_opaque_data: AttachOpaqueData) -> CatalogCapabilities:
        """Publish only implemented constraint, schema and transaction profiles."""
        return CatalogCapabilities(
            read=[CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION],
            ddl=[CONSTRAINT_CREATE, CONSTRAINT_MUTATE, SCHEMA_LIFECYCLE, TABLE_METADATA_CREATE],
            write=[CONSTRAINT_ENFORCE],
            transaction=[TRANSACTION_DDL],
        )

    def catalog_transaction_begin(self, *, attach_opaque_data: AttachOpaqueData) -> TransactionOpaqueData:
        """Pin a committed RDF snapshot."""
        return TransactionOpaqueData(database(attach_opaque_data).begin())

    def catalog_transaction_commit(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        """Commit durably, or report a retryable serialization conflict."""
        database(attach_opaque_data).commit(transaction_opaque_data)

    def catalog_transaction_rollback(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        """Discard the transaction's private RDF generation."""
        database(attach_opaque_data).rollback(transaction_opaque_data)

    def catalog_version(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None = None,
        **kwargs: Any,
    ) -> int:
        """Invalidate the host's catalog cache after committed changes."""
        return database(attach_opaque_data).version(transaction_opaque_data)

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None
    ) -> list[SchemaInfo]:
        """Discover schemas from RDF catalog statements."""
        with database(attach_opaque_data).access(transaction_opaque_data) as snapshot:
            return [
                SchemaInfo(
                    path=[name], comment=f"SQL tables in schema {name!r} and RDF inspection tools.", tags=SCHEMA_TAGS
                )
                for name in RelationalGraph(snapshot).schemas()
            ]

    def schema_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
    ) -> SchemaInfo | None:
        """Resolve one raw schema name with DuckDB identifier semantics."""
        return next(
            (
                s
                for s in self.schemas(
                    attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data
                )
                if [name_key(p) for p in path] == [name_key(p) for p in s.path]
            ),
            None,
        )

    def schema_create(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
        on_conflict: OnConflict = OnConflict.ERROR,
        **kwargs: Any,
    ) -> None:
        """Create a schema in the current transaction."""
        if len(path) != 1:
            raise ValueError("Only one SQL schema component is supported")
        if kwargs.get("options") or kwargs.get("comment") or kwargs.get("tags"):
            raise ValueError("Schema options and custom metadata are not supported")
        with database(attach_opaque_data, writing=True).access(transaction_opaque_data, write=True) as snapshot:
            RelationalGraph(snapshot).schema_create(path[0], on_conflict)

    def schema_drop(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: list[str],
        ignore_not_found: bool,
        cascade: bool,
    ) -> None:
        """Drop a schema, respecting dependencies and CASCADE."""
        if len(path) != 1:
            raise ValueError("Only one SQL schema component is supported")
        with database(attach_opaque_data, writing=True).access(transaction_opaque_data, write=True) as snapshot:
            RelationalGraph(snapshot).schema_drop(path[0], ignore_missing=ignore_not_found, cascade=cascade)

    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: list[str],
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Discover typed columns, stable constraints and write capabilities."""
        if at_unit or at_value:
            raise ValueError("SQL time travel is not supported")
        with database(attach_opaque_data).access(transaction_opaque_data) as snapshot:
            table = RelationalGraph(snapshot).get(schema_path, name)
            return _table_info(table) if table else None

    def schema_contents(self, **kwargs: Any) -> Any:
        """Merge dynamic table discovery with the SDK's function registry."""
        kind = kwargs["type"]
        if kind == SchemaObjectType.TABLE:
            with database(kwargs["attach_opaque_data"]).access(kwargs["transaction_opaque_data"]) as snapshot:
                return [
                    _table_info(t)
                    for t in RelationalGraph(snapshot).tables()
                    if [name_key(p) for p in t.path] == [name_key(p) for p in kwargs["path"]]
                ]
        contents = super().schema_contents(**kwargs)
        internal = {fn.Meta.name for fn in INTERNAL_FUNCTIONS}
        return [
            replace(info, visibility="table_only") if getattr(info, "name", "") in internal else info
            for info in contents
        ]

    def table_create(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: list[str],
        name: str,
        columns: SerializedSchema,
        on_conflict: OnConflict = OnConflict.ERROR,
        constraint_definitions: list[bytes],
        **kwargs: Any,
    ) -> bool:
        """Create SQL metadata and row graph in one RDF transaction."""
        if kwargs.get("options") is not None or kwargs.get("layout") is not None:
            raise ValueError("Table storage options and partition layouts are not supported")
        schema = pa.ipc.read_schema(pa.py_buffer(columns))
        definitions = _decode_definitions(constraint_definitions, schema_path, name)
        with database(attach_opaque_data, writing=True).access(transaction_opaque_data, write=True) as snapshot:
            return RelationalGraph(snapshot).create(
                schema_path, name, schema, definitions, on_conflict, kwargs.get("comment"), kwargs.get("tags")
            )

    def table_constraints_apply(self, request: ConstraintsApplyRequest) -> ConstraintSnapshot:
        """Mutate constraints atomically, validating the complete existing table."""
        with database(request.attach_opaque_data, writing=True).access(
            request.transaction_opaque_data, write=True
        ) as snapshot:
            return RelationalGraph(snapshot).apply_constraints(request)

    def table_drop(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: list[str],
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop a table's data and metadata transactionally."""
        with database(attach_opaque_data, writing=True).access(transaction_opaque_data, write=True) as snapshot:
            RelationalGraph(snapshot).drop(schema_path, name, ignore_missing=ignore_not_found, cascade=cascade)

    def _redirect(self, operation: str, **kwargs: Any) -> ScanFunctionResult:
        with database(kwargs["attach_opaque_data"], writing=operation != "scan").access(
            kwargs.get("transaction_opaque_data")
        ) as snapshot:
            table = RelationalGraph(snapshot).require(kwargs["schema_path"], kwargs["name"])
            return ScanFunctionResult(
                function_name=f"rdf_{operation}",
                schema_path=["main"],
                positional_arguments=[pa.scalar(table.path[0]), pa.scalar(table.name)],
                named_arguments={},
            )

    def table_scan_function_get(self, **kwargs: Any) -> ScanFunctionResult:
        """Route a table scan through the generic RDF scan handler."""
        return self._redirect("scan", **kwargs)

    def table_insert_function_get(self, **kwargs: Any) -> ScanFunctionResult:
        """Route SQL INSERT through the transactional batch handler."""
        return self._redirect("insert", **kwargs)

    def table_update_function_get(self, **kwargs: Any) -> ScanFunctionResult:
        """Route SQL UPDATE through the transactional batch handler."""
        return self._redirect("update", **kwargs)

    def table_delete_function_get(self, **kwargs: Any) -> ScanFunctionResult:
        """Route SQL DELETE through the transactional batch handler."""
        return self._redirect("delete", **kwargs)


def _decode_definitions(values: list[bytes], path: list[str], name: str) -> Any:
    return constraint_definitions(values, CatalogObjectRef(kind="table", schema_path=path, name=name))
