"""Assemble the schemas into the ``un`` catalog and run the worker.

One schema per source (``sdg``, ``uis``, ``heritage``, ``population``,
``refugees``, ``health``) plus ``main`` for the bundled M49 reference table.
Each source module exposes ``FUNCTIONS``, ``TABLES``, ``SCHEMA_COMMENT`` and
``SCHEMA_TAGS``.

Run it:

    vgi-un-worker                      # stdio: what DuckDB launches by default
    vgi-un-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    ATTACH 'un' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-un-worker');
"""

from __future__ import annotations

import json
from types import ModuleType
from typing import Annotated, Any

from vgi import Worker
from vgi.catalog import Catalog, Schema
from vgi.catalog.attach_option import AttachOption

from . import health, heritage, population, reference, refugees, sdg, uis
from .common import DEFAULT_USER_AGENT
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    MAIN_COMMENT,
    MAIN_DOC_LLM,
    MAIN_DOC_MD,
    SCHEMA_CATEGORIES,
    SOURCE_URL,
    keywords_array,
)
from .population import POPULATION_SECRET_TYPE

SOURCES: list[ModuleType] = [sdg, uis, heritage, population, refugees, health]


def _examples(functions: list[type[Any]], tables: list[Any]) -> list[dict[str, str]]:
    """Stamp each function's ``vgi.example_queries`` and return the schema-level list."""
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    out = [entry for entries in by_name.values() for entry in entries]
    for table in tables:
        out.extend(json.loads(table.tags.get("vgi.example_queries", "[]")))
    return out


def _schema(name: str, functions: list[type[Any]], tables: list[Any], comment: str, tags: dict[str, str]) -> Schema:
    used = {fn.Meta.tags["vgi.category"] for fn in functions} | {t.tags["vgi.category"] for t in tables}
    return Schema(
        path=[name],
        comment=comment,
        functions=functions,
        tables=tables,
        tags={
            **tags,
            "vgi.categories": json.dumps([c for c in SCHEMA_CATEGORIES if c["name"] in used]),
            "vgi.example_queries": json.dumps(_examples(functions, tables)),
        },
    )


MAIN_SCHEMA = _schema(
    "main",
    reference.FUNCTIONS,
    reference.TABLES,
    MAIN_COMMENT,
    {
        "vgi.title": "UN Reference Codes",
        "domain": "country codes",
        "vgi.keywords": keywords_array("m49, iso 3166, country codes, regions, ldc, sids"),
        "vgi.source_url": reference.M49_URL,
        "vgi.doc_llm": MAIN_DOC_LLM,
        "vgi.doc_md": MAIN_DOC_MD,
    },
)

SCHEMAS = [
    MAIN_SCHEMA,
    *(_schema(m.SCHEMA, m.FUNCTIONS, m.TABLES, m.SCHEMA_COMMENT, m.SCHEMA_TAGS) for m in SOURCES),
]


class UnWorker(Worker):
    """The worker process: the ``un`` catalog."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=SCHEMAS,
    )

    secret_types = [POPULATION_SECRET_TYPE]

    class AttachOptions:
        """Options for ``ATTACH 'un' (TYPE vgi, LOCATION '...', base_url '...', user_agent '...')``."""

        base_url: Annotated[
            str,
            AttachOption(
                desc=(
                    "Where requests go. Empty (default): each organisation's public API. An http(s) URL: a mirror "
                    "or proxy serving <base_url>/<schema>/<api path>. A local directory: recorded responses "
                    "(see test/fixtures), for offline use and tests."
                )
            ),
        ] = ""
        user_agent: Annotated[
            str,
            AttachOption(desc=f"User-Agent sent upstream (default {DEFAULT_USER_AGENT!r})."),
        ] = DEFAULT_USER_AGENT


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    UnWorker.main()


if __name__ == "__main__":
    main()
