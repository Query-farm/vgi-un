"""Building blocks shared by the schemas: list scans, catalog tables and cursors."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
from vgi.catalog import Table
from vgi.catalog.constraints import constraint_record
from vgi.table_function import OutputCollector, ProcessParams, TableFunctionGenerator
from vgi_rpc import ArrowSerializableDataclass

from .common import Options, emit_rows, options_from

CHUNK_ROWS = 5000


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """No arguments."""


@dataclass(kw_only=True)
class OffsetState(ArrowSerializableDataclass):
    """Cursor over a list that is re-derived (from the per-process cache) on every call."""

    offset: int = 0


class ListScan(TableFunctionGenerator[NoArgs, OffsetState]):
    """Emit the rows of one catalog/list endpoint, ``CHUNK_ROWS`` per batch.

    Subclasses set ``FIXED_SCHEMA`` and implement :meth:`rows`.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Return every row of the list, as dicts keyed by column name."""
        raise NotImplementedError

    @classmethod
    def initial_state(cls, params: ProcessParams[NoArgs]) -> OffsetState:
        """Start at the first row."""
        return OffsetState()

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: OffsetState, out: OutputCollector) -> None:
        """Emit the next chunk; finish once the list is exhausted."""
        rows = cls.rows(options_from(params.attach_options))
        chunk = rows[state.offset : state.offset + CHUNK_ROWS]
        if not chunk:
            out.finish()
            return
        emit_rows(out, params.output_schema, chunk)
        state.offset += CHUNK_ROWS
        if state.offset >= len(rows):
            out.finish()


def catalog_table(fn: type[Any], name: str, schema_name: str, key: str | None, domain: str) -> Table:
    """Expose a parameterless scan as a catalog table carrying the function's metadata.

    ``key`` names a primary-key column the worker guarantees (non-null and unique in every row).
    """
    constraints: tuple[Any, ...] = ()
    if key is not None:
        constraints = (
            constraint_record(
                [schema_name],
                name,
                f"{key}_not_null",
                "NOT_NULL",
                [[key]],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
            ),
            constraint_record(
                [schema_name], name, "pk", "PRIMARY_KEY", [[key]], enforcement="ENFORCED", validation="ALL_ROWS"
            ),
        )
    return Table(
        name=name,
        function=fn,
        comment=fn.Meta.description,
        tags={
            **fn.Meta.tags,
            "domain": domain,
            "vgi.example_queries": json.dumps(
                [{"description": e.description, "sql": e.sql} for e in getattr(fn.Meta, "examples", [])]
            ),
        },
        constraints=constraints,
        column_comments={
            f.name: (f.metadata or {}).get(b"comment", b"").decode() for f in fn.FIXED_SCHEMA if f.metadata
        },
    )
