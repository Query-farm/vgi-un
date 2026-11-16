"""``heritage`` schema: the UNESCO World Heritage List (keyless).

whc.unesco.org's own XML/XLS exports sit behind a browser challenge, so the
worker reads UNESCO's official open-data copy of the List (dataset ``whc001`` on
data.unesco.org, an Opendatasoft Explore v2.1 API). Category, danger status,
region, inscription year and country are pushed into an ODSQL ``where``.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .common import emit_rows, fetch_json, options_from, pushed_int_range, pushed_values, to_float, to_int
from .countries import iso2_to_iso3_map, iso3_to_iso2_map
from .meta import attribution, column, keywords_array, object_tags, result_columns

SCHEMA = "heritage"
DOMAIN = "cultural heritage"
SOURCE = "heritage"
HOME_URL = "https://whc.unesco.org/en/list/"
DATASET_URL = "https://data.unesco.org/explore/dataset/whc001/"
TERMS = (
    "UNESCO World Heritage Centre, [World Heritage List](https://data.unesco.org/explore/dataset/whc001/), "
    "[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). Fetched at query time; not bundled. "
    "Results you share are adaptations under the same licence."
)
PAGE_SIZE = 100
FIELDS = (
    "id_no,uuid,name_en,short_description_en,date_inscribed,secondary_dates,danger,date_end,danger_list,"
    "area_hectares,criteria_txt,category,states_names,iso_codes,region,region_code,transboundary,"
    "geo_point_2d,components_count"
)
CATEGORIES = ("Cultural", "Natural", "Mixed")

SITES_SCHEMA = pa.schema(
    [
        column(
            "site_id",
            pa.string(),
            "World Heritage id number, e.g. '20' (also in the whc.unesco.org URL).",
            nullable=False,
        ),
        column("name", pa.string(), "Site name (English).", nullable=False),
        column("category", pa.string(), "Cultural, Natural or Mixed."),
        column("criteria", pa.list_(pa.string()), "Selection criteria as roman numerals, e.g. ['i', 'iii', 'iv']."),
        column("year_inscribed", pa.int32(), "Year the site was first inscribed."),
        column("inscription_years", pa.list_(pa.int32()), "Year of inscription and of each extension."),
        column(
            "in_danger", pa.bool_(), "True when the site is on the List of World Heritage in Danger.", nullable=False
        ),
        column("danger_history", pa.string(), "Danger-list history as published, e.g. 'Y 2013' (years on the list)."),
        column("date_end", pa.date32(), "Date the site was removed from the List (delisted sites), else NULL."),
        column("area_ha", pa.float64(), "Property area in hectares."),
        column("states", pa.list_(pa.string()), "States Parties (country names)."),
        column("states_iso3", pa.list_(pa.string()), "States Parties as ISO 3166-1 alpha-3 codes."),
        column("states_iso2", pa.list_(pa.string()), "States Parties as ISO 3166-1 alpha-2 codes (as published)."),
        column("region", pa.string(), "UNESCO region, e.g. 'Europe and North America', 'Arab States'."),
        column("region_code", pa.string(), "UNESCO region code, e.g. 'EUR', 'ARB'."),
        column("transboundary", pa.bool_(), "True when the site spans several States Parties."),
        column("latitude", pa.float64(), "Latitude of the site's main point (WGS84)."),
        column("longitude", pa.float64(), "Longitude of the site's main point (WGS84)."),
        column("components_count", pa.int32(), "Number of component parts of a serial property."),
        column("short_description", pa.string(), "Short description (English)."),
        column("url", pa.string(), "The site's page on whc.unesco.org.", nullable=False),
        column("uuid", pa.string(), "UNESCO open-data record UUID."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class SitesArgs:
    """Arguments for ``sites(country := '')``."""

    country: Annotated[
        str,
        Arg(
            "country",
            default="",
            doc="Only sites of this State Party, ISO alpha-3 ('FRA'); default '' = all countries.",
            pattern=r"^([A-Za-z]{3})?$",
        ),
    ]


@dataclass(kw_only=True)
class OffsetState(ArrowSerializableDataclass):
    """Cursor: the next record offset, or -1 once done."""

    offset: int = 0


def _odsql_str(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def where_clause(country: str, filters: Any) -> str | None:
    """ODSQL ``where`` for the country argument and pushed-down predicates."""
    clauses: list[str] = []
    if country:
        iso2 = iso3_to_iso2_map().get(country.upper(), "--")
        clauses.append(f"iso_codes = {_odsql_str(iso2)}")
    cats = [str(c) for c in pushed_values(filters, "category") or [] if str(c) in CATEGORIES]
    if cats:
        clauses.append("(" + " or ".join(f"category = {_odsql_str(c)}" for c in sorted(cats)) + ")")
    danger = pushed_values(filters, "in_danger")
    if danger is not None and len(set(danger)) == 1:
        clauses.append(f"danger = {_odsql_str('True' if danger[0] else 'False')}")
    regions = pushed_values(filters, "region")
    if regions:
        clauses.append("(" + " or ".join(f"region = {_odsql_str(str(r))}" for r in sorted(map(str, regions))) + ")")
    lo, hi = pushed_int_range(filters, "year_inscribed")
    if lo is not None:
        clauses.append(f"date_inscribed >= date'{lo:04d}-01-01'")
    if hi is not None:
        clauses.append(f"date_inscribed <= date'{hi:04d}-12-31'")
    return " and ".join(clauses) if clauses else None


ROMAN = re.compile(r"\(([ivx]+)\)", re.IGNORECASE)


def _list(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if x is not None and str(x).strip()]
    return [p.strip() for p in str(raw).split(",") if p.strip()]


def _bool(raw: Any) -> bool | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in ("true", "1", "yes", "y")


def site_row(r: dict[str, Any]) -> dict[str, Any]:
    """Map one ODS record to an output row."""
    iso2 = [c.upper() for c in _list(r.get("iso_codes"))]
    to3 = iso2_to_iso3_map()
    point = r.get("geo_point_2d") or {}
    years = sorted({y for y in (to_int(x) for x in re.findall(r"\d{4}", str(r.get("secondary_dates") or ""))) if y})
    end = r.get("date_end")
    site_id = str(r.get("id_no"))
    return {
        "site_id": site_id,
        "name": r.get("name_en"),
        "category": r.get("category"),
        "criteria": [m.lower() for m in ROMAN.findall(str(r.get("criteria_txt") or ""))],
        "year_inscribed": to_int(str(r.get("date_inscribed") or "")[:4] or None),
        "inscription_years": years,
        "in_danger": bool(_bool(r.get("danger"))),
        "danger_history": r.get("danger_list") or None,
        "date_end": dt.date.fromisoformat(str(end)[:10]) if end and len(str(end)) >= 10 else None,
        "area_ha": to_float(r.get("area_hectares")),
        "states": _list(r.get("states_names")),
        "states_iso3": [to3.get(c, c) for c in iso2],
        "states_iso2": iso2,
        "region": r.get("region"),
        "region_code": r.get("region_code"),
        "transboundary": _bool(r.get("transboundary")),
        "latitude": to_float(point.get("lat")),
        "longitude": to_float(point.get("lon")),
        "components_count": to_int(r.get("components_count")),
        "short_description": r.get("short_description_en"),
        "url": f"https://whc.unesco.org/en/list/{site_id}",
        "uuid": r.get("uuid"),
    }


SITES_DOC_LLM = (
    "Every property on the UNESCO World Heritage List (about 1,250), one row per site: `site_id`, `name`, "
    "`category` ('Cultural', 'Natural', 'Mixed'), `criteria` (`VARCHAR[]` of roman numerals 'i'..'x'; i-vi are "
    "cultural, vii-x natural), `year_inscribed`, `in_danger` (on the List in Danger), `area_ha`, `states` / "
    "`states_iso3` (`VARCHAR[]`; transboundary sites list several), `region`, `latitude`, `longitude` and the "
    "whc.unesco.org `url`. Pass `country := 'FRA'` (ISO alpha-3) for one State Party's sites; WHERE filters "
    "on category, in_danger, region and year_inscribed are pushed into the API. For a per-country count, "
    "UNNEST(states_iso3)."
)


@init_single_worker
@bind_fixed_schema
class Sites(TableFunctionGenerator[SitesArgs, OffsetState]):
    """Stream World Heritage sites, 100 per request."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SITES_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "sites"
        description = (
            "UNESCO World Heritage List sites with category, criteria, inscription year, danger status and location"
        )
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="World Heritage Sites",
                category="reference",
                keywords="world heritage, unesco, heritage sites, in danger, cultural, natural, monuments",
                doc_llm=SITES_DOC_LLM,
                doc_md=(
                    "## sites(country := '')\n\nThe [World Heritage List](https://whc.unesco.org/en/list/), "
                    "read from UNESCO's open-data portal." + attribution(TERMS)
                ),
            ),
            "vgi.source_url": DATASET_URL,
            "vgi.result_columns_schema": result_columns(SITES_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT category, count(*) AS sites FROM un.heritage.sites() WHERE in_danger "
                    "GROUP BY category ORDER BY category"
                ),
                description="Sites on the List of World Heritage in Danger, by category",
            ),
            FunctionExample(
                sql=(
                    "SELECT name, year_inscribed, criteria FROM un.heritage.sites(country := 'SYR') "
                    "ORDER BY year_inscribed, name"
                ),
                description="World Heritage sites in Syria with their inscription year and criteria",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[SitesArgs]) -> OffsetState:
        """Start at the first record."""
        return OffsetState()

    @classmethod
    def process(cls, params: ProcessParams[SitesArgs], state: OffsetState, out: OutputCollector) -> None:
        """Fetch one page of records and emit it."""
        if state.offset < 0:
            out.finish()
            return
        query: list[tuple[str, str | int]] = [
            ("select", FIELDS),
            ("order_by", "id_no"),
            ("limit", PAGE_SIZE),
            ("offset", state.offset),
        ]
        where = where_clause(params.args.country, params.current_pushdown_filters)
        if where:
            query.append(("where", where))
        body = fetch_json(options_from(params.attach_options), SOURCE, "records", query)
        results = body.get("results") or []
        total = int(body.get("total_count") or 0)
        state.offset += PAGE_SIZE
        if state.offset >= total or not results:
            state.offset = -1
        if results:
            emit_rows(out, params.output_schema, [site_row(r) for r in results])
        else:
            out.finish()


FUNCTIONS: list[type[Any]] = [Sites]
TABLES: list[Any] = []

SCHEMA_COMMENT = "The UNESCO World Heritage List: inscribed cultural, natural and mixed properties."
SCHEMA_TAGS = {
    "vgi.title": "UNESCO World Heritage List",
    "domain": DOMAIN,
    "vgi.keywords": keywords_array("world heritage, unesco, heritage sites, monuments, in danger"),
    "vgi.source_url": HOME_URL,
    "vgi.doc_llm": (
        "The `heritage` schema lists UNESCO World Heritage properties, read from UNESCO's official open-data "
        "copy of the List. States Parties are given as ISO alpha-3 lists so sites join to every other "
        "country-level table in the catalog."
    ),
    "vgi.doc_md": (
        "## heritage\n\nThe [UNESCO World Heritage List](https://whc.unesco.org/en/list/)." + attribution(TERMS)
    ),
}
