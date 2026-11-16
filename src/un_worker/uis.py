"""``uis`` schema: the UNESCO Institute for Statistics data API (keyless).

API: https://api.uis.unesco.org/api/public/documentation/ . A single data request
may return at most 100,000 records (more is refused with HTTP 400), so
``uis.data`` plans its requests: indicators are batched by their published
record counts, and a refused request is split (indicators, then years, then
geo units) until every piece fits. A piece that cannot be split further fails
with a clear error; results are never silently truncated.
"""

from __future__ import annotations

import datetime as dt
import json
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

from .base import ListScan, catalog_table
from .common import (
    Options,
    UpstreamError,
    emit_rows,
    fetch_json,
    fetch_json_cached,
    merge_codes,
    options_from,
    pushed_int_range,
    pushed_values,
    split_codes,
    to_float,
    to_int,
)
from .countries import known_iso3
from .meta import attribution, column, keywords_array, object_tags, result_columns

SCHEMA = "uis"
DOMAIN = "education statistics"
SOURCE = "uis"
HOME_URL = "https://databrowser.uis.unesco.org/"
API_URL = "https://api.uis.unesco.org/api/public/documentation/"
TERMS = (
    "UNESCO Institute for Statistics, [UIS Data API](https://api.uis.unesco.org/api/public/documentation/), "
    "[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). Fetched at query time; not bundled. "
    "Results you share are adaptations under the same licence."
)
RECORD_CAP = 100_000
BATCH_TARGET = 90_000
MAX_CODES_PER_REQUEST = 50
FIRST_YEAR = 1970
NATIONAL_UNITS = 241
TOO_MUCH = re.compile(r"Too much data requested \((\d+) records\)")
CODES_PATTERN = r"^[^;]*$"


def _ddmmyyyy(raw: Any) -> dt.date | None:
    try:
        day, month, year = (int(p) for p in str(raw).split("/"))
        return dt.date(year, month, day)
    except ValueError, TypeError:
        return None


def _timestamp(raw: Any) -> dt.datetime | None:
    if not raw:
        return None
    try:
        return dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(dt.UTC)
    except ValueError:
        return None


def indicator_definitions(opts: Options) -> list[dict[str, Any]]:
    """Return the indicator definitions list (about 2 MB; cached for an hour per process)."""
    data: list[dict[str, Any]] = fetch_json_cached(
        opts, SOURCE, "definitions/indicators", [("disaggregations", "false"), ("glossaryTerms", "false")]
    )
    return data


def year_span(opts: Options) -> tuple[int, int]:
    """Earliest and latest year any indicator has data for."""
    lo, hi = FIRST_YEAR, FIRST_YEAR
    for i in indicator_definitions(opts):
        timeline = (i.get("dataAvailability") or {}).get("timeLine") or {}
        hi = max(hi, to_int(timeline.get("max")) or hi)
    return lo, hi


def default_version(opts: Options) -> str:
    """Return the current default (newest published) data version."""
    return str(fetch_json_cached(opts, SOURCE, "versions/default")["version"])


# ======================================================================== lists

INDICATORS_SCHEMA = pa.schema(
    [
        column(
            "indicator_code", pa.string(), "UIS indicator code, e.g. 'CR.1'; pass it to uis.data().", nullable=False
        ),
        column("name", pa.string(), "Indicator name."),
        column("theme", pa.string(), "EDUCATION, SCIENCE_TECHNOLOGY_INNOVATION, CULTURE or DEMOGRAPHIC_SOCIOECONOMIC."),
        column("last_data_update", pa.date32(), "Date of the latest data release for the indicator."),
        column("last_data_update_description", pa.string(), "Release name, e.g. 'November 2026 Data Release'."),
        column("total_record_count", pa.int64(), "Records published for the indicator (all countries and years)."),
        column("first_year", pa.int32(), "Earliest year with data."),
        column("last_year", pa.int32(), "Latest year with data."),
        column("geo_unit_types", pa.list_(pa.string()), "Geo unit types with data: NATIONAL and/or REGIONAL."),
    ]
)


@init_single_worker
@bind_fixed_schema
class IndicatorsScan(ListScan):
    """UIS indicator definitions."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = INDICATORS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "indicators_scan"
        description = (
            "The ~5,500 UNESCO UIS indicators (education, science, culture) with theme, coverage and record count"
        )
        tags = {
            **object_tags(
                title="UIS Indicators",
                category="catalogs",
                keywords="uis indicators, education indicators, literacy, enrolment, completion rate, science, culture",
                doc_llm=(
                    "Every UNESCO Institute for Statistics indicator: `indicator_code` (pass to `uis.data`), "
                    "`name`, `theme` (EDUCATION, SCIENCE_TECHNOLOGY_INNOVATION, CULTURE, "
                    "DEMOGRAPHIC_SOCIOECONOMIC), `last_data_update`, `total_record_count`, `first_year`, "
                    "`last_year` and `geo_unit_types`. Search with `name ILIKE '%completion%'`."
                ),
                doc_md="## indicators\n\nUIS indicator definitions." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(INDICATORS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT theme, count(*) AS indicators FROM un.uis.indicators GROUP BY theme ORDER BY theme",
                description="Number of UIS indicators per theme",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Rows from ``/definitions/indicators``."""
        out = []
        for i in indicator_definitions(opts):
            avail = i.get("dataAvailability") or {}
            timeline = avail.get("timeLine") or {}
            out.append(
                {
                    "indicator_code": i["indicatorCode"],
                    "name": i.get("name"),
                    "theme": i.get("theme"),
                    "last_data_update": _ddmmyyyy(i.get("lastDataUpdate")),
                    "last_data_update_description": i.get("lastDataUpdateDescription"),
                    "total_record_count": to_int(avail.get("totalRecordCount")),
                    "first_year": to_int(timeline.get("min")),
                    "last_year": to_int(timeline.get("max")),
                    "geo_unit_types": list((avail.get("geoUnits") or {}).get("types") or []),
                }
            )
        return out


GEO_SCHEMA = pa.schema(
    [
        column(
            "geo_unit",
            pa.string(),
            "Geo unit id: ISO alpha-3 for countries, 'GROUP: Name' for regions.",
            nullable=False,
        ),
        column("name", pa.string(), "Country or region name."),
        column("type", pa.string(), "NATIONAL or REGIONAL."),
        column("region_group", pa.string(), "Regional grouping scheme, e.g. 'SDG', 'WB', 'UIS'; NULL for countries."),
        column("country_iso3", pa.string(), "ISO alpha-3 code for national units listed in M49, else NULL."),
    ]
)


@init_single_worker
@bind_fixed_schema
class GeoUnitsScan(ListScan):
    """UIS countries and regions."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = GEO_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "geo_units_scan"
        description = "Countries and regional groupings that UIS publishes data for"
        tags = {
            **object_tags(
                title="UIS Geo Units",
                category="reference",
                keywords="uis countries, regions, geo units, sdg regions, world bank regions",
                doc_llm=(
                    "UIS geo units: `geo_unit` (ISO alpha-3 for countries, 'SDG: Sub-Saharan Africa'-style ids "
                    "for regions), `name`, `type` (NATIONAL, REGIONAL), `region_group` and `country_iso3`."
                ),
                doc_md="## geo_units\n\nUIS countries and regions." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(GEO_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT region_group, count(*) AS regions FROM un.uis.geo_units WHERE type = 'REGIONAL' "
                    "GROUP BY region_group ORDER BY regions DESC"
                ),
                description="Regional groupings and how many regions each defines",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Rows from ``/definitions/geounits``."""
        data = fetch_json_cached(opts, SOURCE, "definitions/geounits")
        return [
            {
                "geo_unit": g["id"],
                "name": g.get("name"),
                "type": g.get("type"),
                "region_group": g.get("regionGroup"),
                "country_iso3": known_iso3(g["id"]) if g.get("type") == "NATIONAL" else None,
            }
            for g in data
        ]


THEME_TYPE = pa.struct(
    [
        pa.field("theme", pa.string()),
        pa.field("last_update", pa.date32()),
        pa.field("description", pa.string()),
    ]
)
VERSIONS_SCHEMA = pa.schema(
    [
        column("version", pa.string(), "Data version id, e.g. '20261110-392b682a'.", nullable=False),
        column("publication_date", pa.timestamp("us", tz="UTC"), "When the version was published."),
        column("is_default", pa.bool_(), "True for the current default version.", nullable=False),
        column("themes", pa.list_(THEME_TYPE), "Per-theme release status: theme, last_update, description."),
    ]
)


@init_single_worker
@bind_fixed_schema
class VersionsScan(ListScan):
    """Published UIS data versions."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = VERSIONS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "versions_scan"
        description = "Published UIS data versions, newest first, with the default version flagged"
        tags = {
            **object_tags(
                title="UIS Data Versions",
                category="catalogs",
                keywords="uis versions, data releases, vintages, publication date",
                doc_llm=(
                    "UIS data versions (vintages): `version` (pass to `uis.data(version := ...)` to reproduce "
                    "an older release), `publication_date`, `is_default` and `themes` (list of structs with "
                    "theme, last_update, description)."
                ),
                doc_md="## versions\n\nPublished UIS data releases." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(VERSIONS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT version, publication_date FROM un.uis.versions WHERE is_default",
                description="The current default UIS data version",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Rows from ``/versions``."""
        default = default_version(opts)
        return [
            {
                "version": v["version"],
                "publication_date": _timestamp(v.get("publicationDate")),
                "is_default": v["version"] == default,
                "themes": [
                    {
                        "theme": t.get("theme"),
                        "last_update": _ddmmyyyy(t.get("lastUpdate")),
                        "description": t.get("description"),
                    }
                    for t in v.get("themeDataStatus") or []
                ],
            }
            for v in fetch_json_cached(opts, SOURCE, "versions")
        ]


# ========================================================================= data

FOOTNOTE_TYPE = pa.struct(
    [pa.field("type", pa.string()), pa.field("subtype", pa.string()), pa.field("value", pa.string())]
)
DATA_SCHEMA = pa.schema(
    [
        column("indicator_code", pa.string(), "UIS indicator code.", nullable=False),
        column("geo_unit", pa.string(), "Geo unit id (ISO alpha-3 for countries).", nullable=False),
        column("country_iso3", pa.string(), "ISO alpha-3 code for countries, NULL for regions."),
        column("year", pa.int32(), "Reference year.", nullable=False),
        column("value", pa.float64(), "Indicator value (units are part of the indicator definition)."),
        column("magnitude", pa.string(), "Magnitude flag, e.g. 'NIL' or 'NA', when published."),
        column("qualifier", pa.string(), "Qualifier flag, e.g. 'UIS_EST' (UIS estimate), when published."),
        column("footnotes", pa.list_(FOOTNOTE_TYPE), "Footnotes: list of (type, subtype, value)."),
        column("version", pa.string(), "Data version the value comes from.", nullable=False),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class DataArgs:
    """Arguments for ``uis.data(indicator := '', geo_unit := '', version := '')``."""

    indicator: Annotated[
        str,
        Arg(
            "indicator",
            default="",
            doc="Indicator code(s), comma-separated, e.g. 'CR.1' or 'CR.1,CR.2'.",
            pattern=CODES_PATTERN,
        ),
    ]
    geo_unit: Annotated[
        str,
        Arg(
            "geo_unit",
            default="",
            doc="Geo unit id(s), comma-separated: ISO alpha-3 ('BRA,USA') or region ids from uis.geo_units.",
            pattern=CODES_PATTERN,
        ),
    ]
    version: Annotated[
        str,
        Arg("version", default="", doc="Data version from uis.versions; default '' = the current default version."),
    ]


@dataclass(kw_only=True)
class PlanState(ArrowSerializableDataclass):
    """Cursor: the resolved version and the JSON list of request chunks still to fetch."""

    version: str = ""
    pending: str = ""


def plan_chunks(
    indicators: list[str], geos: list[str], start: int | None, end: int | None, counts: dict[str, int]
) -> list[dict[str, Any]]:
    """Batch indicators so each request's estimated record count stays under ``BATCH_TARGET``."""
    if not indicators:
        return [{"i": [], "g": geos, "s": start, "e": end}]
    share = 1.0 if not geos else min(1.0, 3.0 * len(geos) / NATIONAL_UNITS)
    chunks: list[dict[str, Any]] = []
    batch: list[str] = []
    estimate = 0.0
    for code in indicators:
        n = counts.get(code, 0) * share
        if batch and (estimate + n > BATCH_TARGET or len(batch) >= MAX_CODES_PER_REQUEST):
            chunks.append({"i": batch, "g": geos, "s": start, "e": end})
            batch, estimate = [], 0.0
        batch.append(code)
        estimate += n
    chunks.append({"i": batch, "g": geos, "s": start, "e": end})
    return chunks


def split_chunk(chunk: dict[str, Any], requested: int, years: tuple[int, int]) -> list[dict[str, Any]]:
    """Split a chunk the API refused as too large: indicators, then years, then geo units.

    ``years`` is the published year span, used when the chunk has no year bounds.
    """
    ind, geos = chunk["i"], chunk["g"]
    if len(ind) > 1:
        mid = len(ind) // 2
        return [{**chunk, "i": ind[:mid]}, {**chunk, "i": ind[mid:]}]
    start = chunk["s"] if chunk["s"] is not None else years[0]
    end = chunk["e"] if chunk["e"] is not None else years[1]
    if end > start:
        pieces = min(end - start + 1, max(2, -(-requested // BATCH_TARGET)))
        size = -(-(end - start + 1) // pieces)
        return [{**chunk, "s": y, "e": min(y + size - 1, end)} for y in range(start, end + 1, size)]
    if len(geos) > 1:
        mid = len(geos) // 2
        return [{**chunk, "g": geos[:mid]}, {**chunk, "g": geos[mid:]}]
    raise UpstreamError(
        f"UIS refuses this request: {requested} records for indicator {ind or 'all'}, geo unit {geos or 'all'}, "
        f"year {start}, over its {RECORD_CAP}-record limit, and it cannot be split further. Narrow it with "
        "indicator := or a WHERE filter on indicator_code."
    )


def chunk_params(chunk: dict[str, Any], version: str) -> list[tuple[str, str | int]]:
    """Query parameters of one data request."""
    params: list[tuple[str, str | int]] = [("indicator", c) for c in chunk["i"]]
    params += [("geoUnit", g) for g in chunk["g"]]
    if chunk["s"] is not None:
        params.append(("start", chunk["s"]))
    if chunk["e"] is not None:
        params.append(("end", chunk["e"]))
    params += [("footnotes", "true"), ("indicatorMetadata", "false"), ("version", version)]
    return params


def data_row(r: dict[str, Any], version: str) -> dict[str, Any]:
    """Map one API record to an output row."""
    geo = str(r.get("geoUnit"))
    return {
        "indicator_code": r.get("indicatorId"),
        "geo_unit": geo,
        "country_iso3": known_iso3(geo),
        "year": to_int(r.get("year")),
        "value": to_float(r.get("value")),
        "magnitude": r.get("magnitude"),
        "qualifier": r.get("qualifier"),
        "footnotes": [
            {"type": f.get("type"), "subtype": f.get("subtype"), "value": f.get("value")}
            for f in r.get("footnotes") or []
        ],
        "version": version,
    }


DATA_DOC_LLM = (
    "UNESCO Institute for Statistics observations, one row per indicator x geo unit x year. Give at least one "
    "of `indicator := 'CR.1'` (codes from `uis.indicators`, comma-separated for several) or `geo_unit := 'BRA'` "
    "(ISO alpha-3 or a region id from `uis.geo_units`); WHERE filters on `indicator_code`, `geo_unit`, "
    "`country_iso3` and `year` ranges are pushed into the API as well. `value` is `DOUBLE` (the unit is in the "
    "indicator name), `qualifier` flags estimates, `footnotes` lists sources. `version := ''` uses the current "
    "default data version. Large requests are split automatically to respect the API's 100,000-record limit; "
    "a request that cannot be split fails with an error rather than returning partial data."
)


@init_single_worker
@bind_fixed_schema
class Data(TableFunctionGenerator[DataArgs, PlanState]):
    """Stream UIS observations, one planned request per batch."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATA_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "data"
        description = "UNESCO UIS education, science and culture observations by indicator, country or region, and year"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="UIS Observations",
                category="observations",
                keywords="uis data, education data, completion rate, enrolment, literacy, out of school, unesco",
                doc_llm=DATA_DOC_LLM,
                doc_md=(
                    "## data(indicator := '', geo_unit := '', version := '')\n\nTidy UIS observations; "
                    "requests are planned to stay under the API's 100,000-record limit." + attribution(TERMS)
                ),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(DATA_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT geo_unit, year, round(value, 1) AS completion_rate FROM un.uis.data(indicator := 'CR.1') "
                    "WHERE country_iso3 IN ('BRA', 'USA') AND year BETWEEN 2015 AND 2020 ORDER BY geo_unit, year"
                ),
                description="Primary completion rate in Brazil and the United States, 2015-2020",
            ),
            FunctionExample(
                sql=(
                    "SELECT d.indicator_code, i.name, d.value FROM un.uis.data(geo_unit := 'BRA') d "
                    "JOIN un.uis.indicators i USING (indicator_code) WHERE d.year = 2020 ORDER BY d.indicator_code"
                ),
                description="Every UIS indicator value for Brazil in 2020, with indicator names",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[DataArgs]) -> PlanState:
        """Not planned yet; see `plan`."""
        return PlanState()

    @classmethod
    def plan(cls, params: ProcessParams[DataArgs]) -> PlanState:
        """Resolve the version and plan the requests (pushed filters are known by the first process call)."""
        a = params.args
        opts = options_from(params.attach_options)
        filters = params.current_pushdown_filters
        indicators = merge_codes(split_codes(a.indicator), pushed_values(filters, "indicator_code"))
        geos = merge_codes(
            split_codes(a.geo_unit),
            (pushed_values(filters, "geo_unit") or []) + (pushed_values(filters, "country_iso3") or []),
        )
        if not indicators and not geos:
            raise ValueError(
                "uis.data needs an indicator or a geo unit: pass indicator := 'CR.1' or geo_unit := 'BRA', or "
                "filter WHERE indicator_code = ... / country_iso3 = ... (find codes in un.uis.indicators)"
            )
        start, end = pushed_int_range(filters, "year")
        counts: dict[str, int] = {}
        if len(indicators) > 1:
            counts = {
                i["indicatorCode"]: int((i.get("dataAvailability") or {}).get("totalRecordCount") or 0)
                for i in indicator_definitions(opts)
            }
        version = a.version.strip() or default_version(opts)
        chunks = plan_chunks(sorted(set(indicators)), sorted(set(geos)), start, end, counts)
        return PlanState(version=version, pending=json.dumps(chunks))

    @classmethod
    def process(cls, params: ProcessParams[DataArgs], state: PlanState, out: OutputCollector) -> None:
        """Fetch chunks until one yields rows; split refused chunks; finish when none remain."""
        opts = options_from(params.attach_options)
        if not state.version:
            planned = cls.plan(params)
            state.version, state.pending = planned.version, planned.pending
        pending: list[dict[str, Any]] = json.loads(state.pending)
        while pending:
            chunk = pending.pop(0)
            try:
                body = fetch_json(opts, SOURCE, "data/indicators", chunk_params(chunk, state.version))
            except UpstreamError as err:
                match = TOO_MUCH.search(err.body or str(err))
                if err.status != 400 or not match:
                    raise
                pending[:0] = split_chunk(chunk, int(match.group(1)), year_span(opts))
                continue
            rows = [data_row(r, state.version) for r in body.get("records") or []]
            if rows:
                state.pending = json.dumps(pending)
                emit_rows(out, params.output_schema, rows)
                return
        state.pending = "[]"
        out.finish()


FUNCTIONS: list[type[Any]] = [Data]
TABLES = [
    catalog_table(IndicatorsScan, "indicators", SCHEMA, "indicator_code", DOMAIN),
    catalog_table(GeoUnitsScan, "geo_units", SCHEMA, "geo_unit", DOMAIN),
    catalog_table(VersionsScan, "versions", SCHEMA, "version", DOMAIN),
]

SCHEMA_COMMENT = (
    "UNESCO Institute for Statistics: education, science, culture and communication indicators and their "
    "observations by country or region and year."
)
SCHEMA_TAGS = {
    "vgi.title": "UNESCO Institute for Statistics",
    "domain": DOMAIN,
    "vgi.keywords": keywords_array("unesco, uis, education, literacy, enrolment, science, r&d, culture"),
    "vgi.source_url": HOME_URL,
    "vgi.doc_llm": (
        "The `uis` schema reads the UNESCO Institute for Statistics data API: about 5,500 indicators, mostly "
        "education (enrolment, completion, out-of-school, literacy, teachers, spending) plus science and "
        "technology, culture and demographic context. Find a code in the indicator list, then read observations "
        "by indicator and/or country; countries are ISO alpha-3."
    ),
    "vgi.doc_md": (
        "## uis\n\nThe [UNESCO Institute for Statistics](https://databrowser.uis.unesco.org/) data API."
        + attribution(TERMS)
    ),
}
