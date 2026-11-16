"""``sdg`` schema: the UN Statistics Division SDG Global Database API (keyless).

API: https://unstats.un.org/sdgapi/swagger/ . Observations come from
``/v1/sdg/Series/Data``, paged 1,000 at a time; the series code, area codes and a
bounded year range are pushed into the request.
"""

from __future__ import annotations

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
    emit_rows,
    fetch_json,
    fetch_json_cached,
    options_from,
    pushed_int_range,
    pushed_values,
    split_codes,
    to_float,
    to_int,
)
from .countries import iso3_for_m49, iso3_to_m49_map, normalize_m49
from .meta import attribution, column, keywords_array, object_tags, result_columns

SCHEMA = "sdg"
DOMAIN = "sustainable development"
SOURCE = "sdg"
SDG_URL = "https://unstats.un.org/sdgs/dataportal"
API_URL = "https://unstats.un.org/sdgapi/swagger/"
TERMS = (
    "UN Statistics Division, [SDG Global Database](https://unstats.un.org/sdgs/dataportal); "
    "free to copy and redistribute with citation under the "
    "[UNdata terms of use](https://data.un.org/Host.aspx?Content=UNdataUse)."
)
PAGE_SIZE = 1000
MAX_PUSHED_YEARS = 100
SERIES_PATTERN = r"^\s*[A-Za-z0-9_]+(\s*,\s*[A-Za-z0-9_]+)*\s*$"


def _str_list(raw: Any) -> list[str] | None:
    if raw is None:
        return None
    if isinstance(raw, list):
        return [str(x) for x in raw]
    return [str(raw)]


# ======================================================================== lists

GOALS_SCHEMA = pa.schema(
    [
        column("goal", pa.string(), "Goal number as text, '1' to '17'.", nullable=False),
        column("title", pa.string(), "Short goal title, e.g. 'End poverty in all its forms everywhere'."),
        column("description", pa.string(), "Longer description of the goal."),
    ]
)


@init_single_worker
@bind_fixed_schema
class GoalsScan(ListScan):
    """The 17 Sustainable Development Goals."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = GOALS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "goals_scan"
        description = "The 17 Sustainable Development Goals with their titles and descriptions"
        tags = {
            **object_tags(
                title="SDG Goals",
                category="catalogs",
                keywords="sdg, sustainable development goals, goals, agenda 2030, global goals",
                doc_llm=(
                    "The 17 Sustainable Development Goals from the UN SDG Global Database: `goal` ('1'..'17' as "
                    "text), `title` and `description`. Join to `sdg.targets` / `sdg.indicators` / `sdg.series` "
                    "on goal."
                ),
                doc_md="## goals\n\nThe 17 SDGs of the 2030 Agenda." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(GOALS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT goal, title FROM un.sdg.goals ORDER BY goal::INTEGER",
                description="All 17 goals in numeric order",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Goals from ``/v1/sdg/Goal/List``."""
        data = fetch_json_cached(opts, SOURCE, "v1/sdg/Goal/List", [("includechildren", "false")])
        return [{"goal": g["code"], "title": g.get("title"), "description": g.get("description")} for g in data]


TARGETS_SCHEMA = pa.schema(
    [
        column("target", pa.string(), "Target code, e.g. '1.1' or '17.a'.", nullable=False),
        column("goal", pa.string(), "Goal number the target belongs to.", nullable=False),
        column("title", pa.string(), "Target text."),
    ]
)


@init_single_worker
@bind_fixed_schema
class TargetsScan(ListScan):
    """The SDG targets."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TARGETS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "targets_scan"
        description = "The 169 SDG targets, each with its goal and wording"
        tags = {
            **object_tags(
                title="SDG Targets",
                category="catalogs",
                keywords="sdg targets, targets, goals, agenda 2030",
                doc_llm=(
                    "The 169 SDG targets: `target` code ('1.1', '17.a'), parent `goal` and `title` (the target "
                    "wording). Join to `sdg.indicators` on target."
                ),
                doc_md="## targets\n\nThe 169 targets under the 17 goals." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(TARGETS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT goal, count(*) AS targets FROM un.sdg.targets GROUP BY goal ORDER BY goal::INTEGER",
                description="Number of targets under each goal",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Targets from ``/v1/sdg/Target/List``."""
        data = fetch_json_cached(opts, SOURCE, "v1/sdg/Target/List", [("includechildren", "false")])
        return [{"target": t["code"], "goal": t["goal"], "title": t.get("title")} for t in data]


INDICATORS_SCHEMA = pa.schema(
    [
        column("indicator", pa.string(), "Indicator code, e.g. '1.1.1'.", nullable=False),
        column("goal", pa.string(), "Goal number.", nullable=False),
        column("target", pa.string(), "Target code, e.g. '1.1'.", nullable=False),
        column("description", pa.string(), "Indicator wording."),
        column("tier", pa.string(), "Tier classification ('1', '2', ...) as published."),
        column(
            "series_codes",
            pa.list_(pa.string()),
            "Codes of the data series measuring this indicator; pass one to sdg.data().",
        ),
    ]
)


@init_single_worker
@bind_fixed_schema
class IndicatorsScan(ListScan):
    """The SDG indicators and the series that measure them."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = INDICATORS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "indicators_scan"
        description = "The SDG indicators (1.1.1 ...) with their wording, tier and data series codes"
        tags = {
            **object_tags(
                title="SDG Indicators",
                category="catalogs",
                keywords="sdg indicators, indicator framework, tier, series codes",
                doc_llm=(
                    "The SDG global indicator framework: one row per `indicator` code ('1.1.1') with `goal`, "
                    "`target`, `description`, `tier` and `series_codes` (`VARCHAR[]`), the data series that "
                    "measure it. Data is published per series, so find a series here (or in `sdg.series`) "
                    "and pass it to `sdg.data(series)`."
                ),
                doc_md="## indicators\n\nThe SDG indicator framework and its data series." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(INDICATORS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT indicator, description, series_codes FROM un.sdg.indicators "
                    "WHERE target = '1.1' ORDER BY indicator"
                ),
                description="Indicators under target 1.1 and the series that measure them",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Return the indicators from ``/v1/sdg/Indicator/List``."""
        data = fetch_json_cached(opts, SOURCE, "v1/sdg/Indicator/List")
        return [
            {
                "indicator": i["code"],
                "goal": i["goal"],
                "target": i["target"],
                "description": i.get("description"),
                "tier": i.get("tier"),
                "series_codes": [s["code"] for s in i.get("series") or []],
            }
            for i in data
        ]


SERIES_SCHEMA = pa.schema(
    [
        column("series_code", pa.string(), "Series code, e.g. 'SI_POV_DAY1'; pass it to sdg.data().", nullable=False),
        column("description", pa.string(), "What the series measures, with its unit in parentheses."),
        column("goals", pa.list_(pa.string()), "Goals the series reports on."),
        column("targets", pa.list_(pa.string()), "Targets the series reports on."),
        column("indicators", pa.list_(pa.string()), "Indicator codes the series measures."),
        column("release", pa.string(), "Database release the series is from, e.g. '2026.Q3.G.01'."),
    ]
)


@init_single_worker
@bind_fixed_schema
class SeriesScan(ListScan):
    """The SDG data series."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SERIES_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "series_scan"
        description = "The 700+ SDG data series in the current release, with the indicators they measure"
        tags = {
            **object_tags(
                title="SDG Data Series",
                category="catalogs",
                keywords="sdg series, series codes, data series, search indicators",
                doc_llm=(
                    "Every data series of the current SDG Global Database release: `series_code` (pass to "
                    "`sdg.data`), `description` (includes the unit), and the `goals`, `targets` and "
                    "`indicators` (`VARCHAR[]`) it reports on. Search with `description ILIKE '%poverty%'` or "
                    "`list_contains(indicators, '1.1.1')`."
                ),
                doc_md="## series\n\nThe data series of the current release." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(SERIES_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT series_code, description FROM un.sdg.series "
                    "WHERE list_contains(indicators, '1.1.1') ORDER BY series_code"
                ),
                description="Series that measure indicator 1.1.1 (extreme poverty)",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Series from ``/v1/sdg/Series/List``."""
        data = fetch_json_cached(opts, SOURCE, "v1/sdg/Series/List", [("allreleases", "false")])
        return [
            {
                "series_code": s["code"],
                "description": s.get("description"),
                "goals": _str_list(s.get("goal")),
                "targets": _str_list(s.get("target")),
                "indicators": _str_list(s.get("indicator")),
                "release": s.get("release"),
            }
            for s in data
        ]


GEOAREAS_SCHEMA = pa.schema(
    [
        column(
            "area_code", pa.string(), "SDG area code as the API writes it (M49 without leading zeros).", nullable=False
        ),
        column("m49", pa.string(), "3-digit M49 code with leading zeros, e.g. '004'."),
        column("country_iso3", pa.string(), "ISO 3166-1 alpha-3 code; NULL for regions and groupings."),
        column("area_name", pa.string(), "Country, area or region name."),
    ]
)


@init_single_worker
@bind_fixed_schema
class GeoAreasScan(ListScan):
    """SDG geographic areas: countries, areas and regional groupings."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = GEOAREAS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "geoareas_scan"
        description = "Countries, areas and regional groupings the SDG database reports, with M49 and ISO codes"
        tags = {
            **object_tags(
                title="SDG Geographic Areas",
                category="reference",
                keywords="geoareas, countries, regions, m49, area codes, iso3",
                doc_llm=(
                    "The ~460 geographic areas of the SDG database: `area_code` (M49 numeric, as the API writes "
                    "it, e.g. '4'), `m49` ('004'), `country_iso3` (NULL for regions such as '1' World or '150' "
                    "Europe) and `area_name`."
                ),
                doc_md="## geoareas\n\nAreas reported in the SDG database." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(GEOAREAS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT count(*) FILTER (WHERE country_iso3 IS NOT NULL) AS countries, "
                    "count(*) FILTER (WHERE country_iso3 IS NULL) AS other_areas FROM un.sdg.geoareas"
                ),
                description="Countries versus regional groupings in the SDG area list",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Areas from ``/v1/sdg/GeoArea/List``."""
        data = fetch_json_cached(opts, SOURCE, "v1/sdg/GeoArea/List")
        return [
            {
                "area_code": a["geoAreaCode"],
                "m49": normalize_m49(a["geoAreaCode"]),
                "country_iso3": iso3_for_m49(a["geoAreaCode"]),
                "area_name": a.get("geoAreaName"),
            }
            for a in data
        ]


# ========================================================================= data

DATA_SCHEMA = pa.schema(
    [
        column("series_code", pa.string(), "Series code, e.g. 'SI_POV_DAY1'.", nullable=False),
        column("series_description", pa.string(), "What the series measures, with unit."),
        column("goal", pa.list_(pa.string()), "Goals the series reports on."),
        column("target", pa.list_(pa.string()), "Targets the series reports on."),
        column("indicator", pa.list_(pa.string()), "Indicator codes, e.g. ['1.1.1']."),
        column("area_code", pa.string(), "SDG area code (M49 without leading zeros, e.g. '356').", nullable=False),
        column("m49", pa.string(), "3-digit M49 code, e.g. '356'; NULL for non-numeric area codes."),
        column("country_iso3", pa.string(), "ISO 3166-1 alpha-3 code; NULL for regions and groupings."),
        column("area_name", pa.string(), "Country, area or region name."),
        column("year", pa.int32(), "Observation year (the start of the time period)."),
        column("time_detail", pa.string(), "Finer period detail when published, e.g. '2015-2017'."),
        column("value", pa.float64(), "Numeric value; NULL when the published value is not a number."),
        column("value_text", pa.string(), "The value exactly as published (may be '<1', 'NaN', 'N')."),
        column("value_type", pa.string(), "Published value type, e.g. 'Float'."),
        column("unit", pa.string(), "Units attribute code, e.g. 'PERCENT', 'NUMBER', 'CON_USD'."),
        column(
            "nature", pa.string(), "Nature of data code: C country, CA adjusted, E estimated, M modelled, G global."
        ),
        column("sex", pa.string(), "Sex dimension code (BOTHSEX, FEMALE, MALE); NULL when not disaggregated."),
        column("age", pa.string(), "Age dimension code, e.g. 'ALLAGE', '15-24'; NULL when not disaggregated."),
        column("location", pa.string(), "Urban/rural dimension (ALLAREA, URBAN, RURAL); NULL when absent."),
        column("reporting_type", pa.string(), "N national, G global (UN-estimated), R regional."),
        column("lower_bound", pa.float64(), "Lower confidence bound when published."),
        column("upper_bound", pa.float64(), "Upper confidence bound when published."),
        column("base_period", pa.string(), "Base period of prices or indices, e.g. '2021'."),
        column("source", pa.string(), "Data source named by the custodian agency."),
        column("footnotes", pa.list_(pa.string()), "Footnotes attached to the observation."),
        column("dimensions", pa.map_(pa.string(), pa.string()), "Every dimension of the observation, name -> code."),
        column("attributes", pa.map_(pa.string(), pa.string()), "Every attribute of the observation, name -> code."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class DataArgs:
    """Arguments for ``sdg.data(series)``."""

    series: Annotated[
        str,
        Arg(
            0,
            doc="Series code, e.g. 'SI_POV_DAY1', or a comma-separated list. Find codes in sdg.series.",
            pattern=SERIES_PATTERN,
        ),
    ]


@dataclass(kw_only=True)
class PageState(ArrowSerializableDataclass):
    """Cursor: the next page to request (1-based), or 0 once done."""

    page: int = 1


def area_codes(filters: Any) -> list[str]:
    """SDG area codes implied by pushed-down ``area_code`` / ``m49`` / ``country_iso3`` filters (a superset)."""
    codes: list[str] = []
    for v in pushed_values(filters, "area_code") or []:
        codes.append(str(v))
    for v in pushed_values(filters, "m49") or []:
        norm = normalize_m49(v)
        if norm:
            codes.append(str(int(norm)))
    iso_map = iso3_to_m49_map()
    for v in pushed_values(filters, "country_iso3") or []:
        m49 = iso_map.get(str(v).upper())
        if m49:
            codes.append(str(int(m49)))
        else:
            codes.append("__none__")  # an unknown ISO code matches nothing; keep the request narrow
    return sorted(set(codes))


def data_request(series: str, filters: Any, page: int) -> list[tuple[str, str | int]]:
    """Query parameters for one page of ``/v1/sdg/Series/Data``."""
    params: list[tuple[str, str | int]] = [("seriesCode", s.upper()) for s in split_codes(series)]
    params += [("areaCode", c) for c in area_codes(filters) if c != "__none__"]
    lo, hi = pushed_int_range(filters, "year")
    if lo is not None and hi is not None and 0 <= hi - lo < MAX_PUSHED_YEARS:
        params += [("timePeriod", y) for y in range(lo, hi + 1)]
    params += [("pageSize", PAGE_SIZE), ("page", page)]
    return params


def data_row(d: dict[str, Any]) -> dict[str, Any]:
    """Map one ``data`` element of the API response to an output row."""
    dims = d.get("dimensions") or {}
    attrs = d.get("attributes") or {}
    area = str(d.get("geoAreaCode"))
    return {
        "series_code": d.get("series"),
        "series_description": d.get("seriesDescription"),
        "goal": _str_list(d.get("goal")),
        "target": _str_list(d.get("target")),
        "indicator": _str_list(d.get("indicator")),
        "area_code": area,
        "m49": normalize_m49(area),
        "country_iso3": iso3_for_m49(area),
        "area_name": d.get("geoAreaName"),
        "year": to_int(d.get("timePeriodStart")),
        "time_detail": d.get("time_detail"),
        "value": to_float(d.get("value")),
        "value_text": None if d.get("value") is None else str(d.get("value")),
        "value_type": d.get("valueType"),
        "unit": attrs.get("Units"),
        "nature": attrs.get("Nature"),
        "sex": dims.get("Sex"),
        "age": dims.get("Age"),
        "location": dims.get("Location"),
        "reporting_type": dims.get("Reporting Type"),
        "lower_bound": to_float(d.get("lowerBound")),
        "upper_bound": to_float(d.get("upperBound")),
        "base_period": None if d.get("basePeriod") is None else str(d.get("basePeriod")),
        "source": d.get("source"),
        "footnotes": [str(f) for f in d.get("footnotes") or [] if f],
        "dimensions": [(str(k), None if v is None else str(v)) for k, v in dims.items()],
        "attributes": [(str(k), None if v is None else str(v)) for k, v in attrs.items()],
    }


DATA_DOC_LLM = (
    "Observations of one or more SDG data series from the UN SDG Global Database, one row per area x year x "
    "dimension combination. `series` (positional, required) is a series code such as 'SI_POV_DAY1' or a "
    "comma-separated list; find codes in `sdg.series` or `sdg.indicators.series_codes`. Filter with WHERE: "
    "`country_iso3 = 'IND'` (ISO alpha-3), `m49 = '356'` or `area_code = '356'`, and `year = 2022` / "
    "`year BETWEEN 2015 AND 2022`; these are pushed into the API request. Many series are disaggregated: "
    "`sex` ('BOTHSEX', 'FEMALE', 'MALE'), `age` ('ALLAGE', '15-24' ...), `location` ('ALLAREA', 'URBAN', "
    "'RURAL') and `reporting_type` ('N' national, 'G' UN global estimate); pick the totals (BOTHSEX, ALLAGE, "
    "ALLAREA) to avoid double counting, and see `dimensions` for any other breakdown. `value` is `DOUBLE` (NULL "
    "when the published text is not numeric; `value_text` keeps it), `unit` is the units code. Regions have "
    "country_iso3 NULL."
)


@init_single_worker
@bind_fixed_schema
class Data(TableFunctionGenerator[DataArgs, PageState]):
    """Stream SDG observations page by page."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATA_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "data"
        description = "SDG Global Database observations for one or more data series, by area, year and dimension"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="SDG Series Observations",
                category="observations",
                keywords="sdg data, observations, indicator values, country data, time series, poverty, sdg api",
                doc_llm=DATA_DOC_LLM,
                doc_md=(
                    "## data(series)\n\n"
                    "Tidy observations of SDG data series. `WHERE` filters on `country_iso3`, `m49`, "
                    "`area_code` and `year` are sent to the API; everything else is filtered locally."
                    + attribution(TERMS)
                ),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(DATA_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT year, value FROM un.sdg.data('SI_POV_DAY1') WHERE country_iso3 = 'IND' "
                    "AND sex = 'BOTHSEX' AND age = 'ALLAGE' AND location = 'ALLAREA' ORDER BY year"
                ),
                description="Share of India's population below the international poverty line, by year",
            ),
            FunctionExample(
                sql=(
                    "SELECT d.year, m.region_name, round(avg(d.value), 1) AS avg_poverty_pct "
                    "FROM un.sdg.data('SI_POV_DAY1') d JOIN un.main.m49_countries m ON m.iso_alpha3 = d.country_iso3 "
                    "WHERE d.country_iso3 IN ('IND', 'BGD') AND d.year BETWEEN 2004 AND 2022 "
                    "AND d.age = 'ALLAGE' AND d.location = 'ALLAREA' GROUP BY ALL ORDER BY ALL"
                ),
                description="Average poverty rate of India and Bangladesh by year, labelled with the M49 region",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[DataArgs]) -> PageState:
        """Start at page 1."""
        return PageState()

    @classmethod
    def process(cls, params: ProcessParams[DataArgs], state: PageState, out: OutputCollector) -> None:
        """Fetch one page and emit it; finish after the last page."""
        if state.page == 0:
            out.finish()
            return
        filters = params.current_pushdown_filters
        if "__none__" in area_codes(filters) and len(area_codes(filters)) == 1:
            out.finish()  # only unknown ISO codes were asked for
            return
        opts = options_from(params.attach_options)
        body = fetch_json(opts, SOURCE, "v1/sdg/Series/Data", data_request(params.args.series, filters, state.page))
        rows = [data_row(d) for d in body.get("data") or []]
        total_pages = int(body.get("totalPages") or 0)
        if rows:
            emit_rows(out, params.output_schema, rows)
        if state.page >= total_pages or not rows:
            state.page = 0
            if not rows:
                out.finish()
            return
        state.page += 1


FUNCTIONS: list[type[Any]] = [Data]
TABLES = [
    catalog_table(GoalsScan, "goals", SCHEMA, "goal", DOMAIN),
    catalog_table(TargetsScan, "targets", SCHEMA, "target", DOMAIN),
    catalog_table(IndicatorsScan, "indicators", SCHEMA, "indicator", DOMAIN),
    catalog_table(SeriesScan, "series", SCHEMA, "series_code", DOMAIN),
    catalog_table(GeoAreasScan, "geoareas", SCHEMA, "area_code", DOMAIN),
]

SCHEMA_COMMENT = (
    "The UN SDG Global Database: the 17 goals, their targets, indicators and data series, and series "
    "observations by country or region and year."
)
SCHEMA_TAGS = {
    "vgi.title": "UN SDG Global Database",
    "domain": DOMAIN,
    "vgi.keywords": keywords_array("sdg, sustainable development goals, indicators, series, unstats, m49"),
    "vgi.source_url": SDG_URL,
    "vgi.doc_llm": (
        "The `sdg` schema reads the UN Statistics Division SDG Global Database API. Catalog tables describe the "
        "framework (goals -> targets -> indicators -> data series); observations are published per data series, "
        "so look up a `series_code` and pass it to the observations function, filtering by `country_iso3` and "
        "`year`. Areas are UN M49 codes (countries and regional groupings)."
    ),
    "vgi.doc_md": (
        "## sdg\n\nThe [SDG Global Database](https://unstats.un.org/sdgs/dataportal): the indicator framework "
        "and country, regional and global observations." + attribution(TERMS)
    ),
}
