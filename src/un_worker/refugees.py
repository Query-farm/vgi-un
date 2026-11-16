"""``refugees`` schema: the UNHCR Refugee Population Statistics API (keyless).

API: https://api.unhcr.org/docs/refugee-statistics.html . Each endpoint is
reshaped into tidy rows, one per year x origin x asylum country x measure.
Origin/asylum ISO codes and the year range are pushed into the request
(``cf_type=ISO``); unfiltered origins/asylum countries are broken down with
``coo_all`` / ``coa_all`` rather than summed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .base import ListScan, NoArgs, catalog_table
from .common import (
    Options,
    emit_rows,
    fetch_json,
    fetch_json_cached,
    options_from,
    pushed_int_range,
    pushed_values,
    to_float,
)
from .meta import attribution, column, keywords_array, object_tags, result_columns

SCHEMA = "refugees"
DOMAIN = "forced displacement"
SOURCE = "refugees"
HOME_URL = "https://www.unhcr.org/refugee-statistics/"
API_URL = "https://api.unhcr.org/docs/refugee-statistics.html"
TERMS = (
    "UNHCR Refugee Population Statistics Database, [CC BY 4.0](https://www.unhcr.org/terms-use-datasets) "
    "(cite as 'UNHCR Refugee Population Statistics Database')."
)
PAGE_LIMIT = 1000


def _code(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return None if text in ("", "-") else text


BASE_FIELDS: list[pa.Field[Any]] = [
    column("year", pa.int32(), "Calendar year (end-year stock, or flows during the year).", nullable=False),
    column("origin_iso3", pa.string(), "Country of origin, ISO 3166-1 alpha-3 (NULL when unknown or stateless)."),
    column(
        "origin_code", pa.string(), "Country of origin, UNHCR 3-letter code (e.g. 'SYR'; differs from ISO for some)."
    ),
    column("origin_name", pa.string(), "Country of origin name as UNHCR writes it."),
    column("asylum_iso3", pa.string(), "Country of asylum or residence, ISO 3166-1 alpha-3."),
    column("asylum_code", pa.string(), "Country of asylum, UNHCR code (e.g. 'GFR' for Germany)."),
    column("asylum_name", pa.string(), "Country of asylum name as UNHCR writes it."),
]
MEASURE_FIELDS: list[pa.Field[Any]] = [
    column("measure", pa.string(), "What is counted; see the function description for the values.", nullable=False),
    column("value", pa.float64(), "Number of people (or applications / decisions).", nullable=False),
]


@dataclass(kw_only=True)
class PageState(ArrowSerializableDataclass):
    """Cursor: the next page (1-based), or 0 once done."""

    page: int = 1


def request_params(filters: Any, page: int) -> list[tuple[str, str | int]]:
    """Query parameters: ISO origin/asylum filters, year range and paging."""
    params: list[tuple[str, str | int]] = [("cf_type", "ISO"), ("limit", PAGE_LIMIT), ("page", page)]
    for col, key in (("origin_iso3", "coo"), ("asylum_iso3", "coa")):
        values = sorted({str(v).upper() for v in pushed_values(filters, col) or []})
        if values:
            params.append((key, ",".join(values)))
        else:
            params.append((f"{key}_all", "true"))
    lo, hi = pushed_int_range(filters, "year")
    if lo is not None:
        params.append(("yearFrom", lo))
    if hi is not None:
        params.append(("yearTo", hi))
    return params


def base_row(item: dict[str, Any]) -> dict[str, Any]:
    """Return the year and country columns of one API item."""
    return {
        "year": int(item["year"]),
        "origin_iso3": _code(item.get("coo_iso")),
        "origin_code": _code(item.get("coo")),
        "origin_name": _code(item.get("coo_name")),
        "asylum_iso3": _code(item.get("coa_iso")),
        "asylum_code": _code(item.get("coa")),
        "asylum_name": _code(item.get("coa_name")),
    }


class _UnhcrScan(TableFunctionGenerator[NoArgs, PageState]):
    """Shared paging over one UNHCR endpoint; subclasses name the endpoint, measures and dimensions."""

    FIXED_SCHEMA: ClassVar[pa.Schema]
    ENDPOINT: ClassVar[str]
    MEASURES: ClassVar[tuple[str, ...]]
    DIMENSIONS: ClassVar[dict[str, str]] = {}

    @classmethod
    def initial_state(cls, params: ProcessParams[NoArgs]) -> PageState:
        """Start at page 1."""
        return PageState()

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: PageState, out: OutputCollector) -> None:
        """Fetch one page, melt its measures into rows and emit them."""
        if state.page == 0:
            out.finish()
            return
        opts = options_from(params.attach_options)
        body = fetch_json(opts, SOURCE, cls.ENDPOINT, request_params(params.current_pushdown_filters, state.page))
        items = body.get("items") or []
        wanted = pushed_values(params.current_pushdown_filters, "measure")
        rows: list[dict[str, Any]] = []
        for item in items:
            base = base_row(item)
            for out_name, api_name in cls.DIMENSIONS.items():
                base[out_name] = _code(item.get(api_name))
            for measure in cls.MEASURES:
                if wanted is not None and measure not in wanted:
                    continue
                value = to_float(item.get(measure))
                if value is not None:  # '-' = not applicable; omitted
                    rows.append({**base, "measure": measure, "value": value})
        if rows:
            emit_rows(out, params.output_schema, rows)
        max_pages = int(body.get("maxPages") or 0)
        if not items or state.page >= max_pages:
            state.page = 0
            if not rows:
                out.finish()
            return
        state.page += 1


# ================================================================== population

POPULATION_MEASURES = (
    "refugees",
    "asylum_seekers",
    "returned_refugees",
    "idps",
    "returned_idps",
    "stateless",
    "ooc",
    "oip",
    "hst",
)
POPULATION_SCHEMA = pa.schema([*BASE_FIELDS, *MEASURE_FIELDS])


@init_single_worker
@bind_fixed_schema
class Population(_UnhcrScan):
    """Forcibly displaced and stateless populations."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = POPULATION_SCHEMA
    ENDPOINT = "population/"
    MEASURES = POPULATION_MEASURES

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "population_scan"
        description = (
            "UNHCR end-year populations: refugees, asylum-seekers, IDPs, stateless and others, by origin and asylum"
        )
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="UNHCR Displaced Populations",
                category="observations",
                keywords="refugees, asylum seekers, idps, stateless, displacement, unhcr, forcibly displaced",
                doc_llm=(
                    "UNHCR end-year population figures, one row per year x country of origin x country of asylum "
                    "x measure. `measure` is one of 'refugees', 'asylum_seekers', 'returned_refugees', 'idps' "
                    "(internally displaced), 'returned_idps', 'stateless', 'ooc' (others of concern), 'oip' "
                    "(other people in need of international protection) and 'hst' (host community); `value` is "
                    "the number of people. Filter with WHERE `origin_iso3 = 'SYR'`, `asylum_iso3 = 'DEU'` (ISO "
                    "alpha-3) and `year`; these are pushed into the API. Without an origin (or asylum) filter, "
                    "every origin (or asylum country) is returned separately, so sum `value` yourself for "
                    "totals. Cells UNHCR publishes as '-' (not applicable) are omitted."
                ),
                doc_md=(
                    "## populations\n\nEnd-year stocks of refugees, asylum-seekers, IDPs, stateless people and "
                    "others of concern, in long format." + attribution(TERMS)
                ),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(POPULATION_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT value AS refugees FROM un.refugees.populations WHERE origin_iso3 = 'SYR' "
                    "AND asylum_iso3 = 'DEU' AND year = 2023 AND measure = 'refugees'"
                ),
                description="Refugees from Syria living in Germany at the end of 2023",
            ),
            FunctionExample(
                sql=(
                    "SELECT asylum_name, value FROM un.refugees.populations WHERE origin_iso3 = 'SYR' "
                    "AND year = 2023 AND measure = 'refugees' ORDER BY value DESC LIMIT 5"
                ),
                description="The five countries hosting the most Syrian refugees in 2023",
            ),
        ]


# ======================================================== asylum applications

APPLICATIONS_DIMS = {
    "procedure_type": "procedure_type",
    "application_type": "app_type",
    "decision_level": "dec_level",
    "count_basis": "app_pc",
}
DIM_FIELDS = {
    "procedure_type": column("procedure_type", pa.string(), "G government, U UNHCR, J joint procedure."),
    "application_type": column("application_type", pa.string(), "Application type code, e.g. N new, A appeal."),
    "decision_level": column("decision_level", pa.string(), "Procedure stage, e.g. FI first instance, AR appeal."),
    "count_basis": column("count_basis", pa.string(), "P persons or C cases."),
}
APPLICATIONS_SCHEMA = pa.schema([*BASE_FIELDS, *(DIM_FIELDS[d] for d in APPLICATIONS_DIMS), *MEASURE_FIELDS])


@init_single_worker
@bind_fixed_schema
class AsylumApplications(_UnhcrScan):
    """Asylum applications lodged during the year."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = APPLICATIONS_SCHEMA
    ENDPOINT = "asylum-applications/"
    MEASURES = ("applied",)
    DIMENSIONS = APPLICATIONS_DIMS

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "asylum_applications_scan"
        description = "UNHCR asylum applications lodged each year, by origin, asylum country and procedure"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="UNHCR Asylum Applications",
                category="observations",
                keywords="asylum applications, asylum claims, asylum seekers, unhcr, refugee status determination",
                doc_llm=(
                    "Asylum applications lodged during each year, one row per year x origin x asylum country x "
                    "procedure_type x application_type x decision_level x count_basis; `measure` is always "
                    "'applied' and `value` the number of applications. `count_basis` 'P' counts persons, 'C' "
                    "cases. Filter by `origin_iso3`, `asylum_iso3` and `year` (pushed into the API)."
                ),
                doc_md="## asylum_applications\n\nAsylum claims lodged per year." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(APPLICATIONS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT decision_level, sum(value) AS applications FROM un.refugees.asylum_applications "
                    "WHERE origin_iso3 = 'SYR' AND asylum_iso3 = 'DEU' AND year = 2023 "
                    "GROUP BY decision_level ORDER BY decision_level"
                ),
                description="Syrian asylum applications in Germany in 2023 by procedure stage",
            ),
        ]


# ========================================================== asylum decisions

DECISIONS_DIMS = {"procedure_type": "procedure_type", "decision_level": "dec_level", "count_basis": "dec_pc"}
DECISIONS_SCHEMA = pa.schema([*BASE_FIELDS, *(DIM_FIELDS[d] for d in DECISIONS_DIMS), *MEASURE_FIELDS])


@init_single_worker
@bind_fixed_schema
class AsylumDecisions(_UnhcrScan):
    """Decisions taken on asylum applications during the year."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DECISIONS_SCHEMA
    ENDPOINT = "asylum-decisions/"
    MEASURES = ("dec_recognized", "dec_other", "dec_rejected", "dec_closed", "dec_total")
    DIMENSIONS = DECISIONS_DIMS

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "asylum_decisions_scan"
        description = "UNHCR decisions on asylum claims each year: recognized, other protection, rejected, closed"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="UNHCR Asylum Decisions",
                category="observations",
                keywords="asylum decisions, recognition rate, rejected, refugee status, complementary protection",
                doc_llm=(
                    "Decisions on asylum applications taken during each year, one row per year x origin x asylum "
                    "country x procedure_type x decision_level x count_basis x measure. `measure` is "
                    "'dec_recognized' (refugee status), 'dec_other' (complementary protection), 'dec_rejected', "
                    "'dec_closed' (otherwise closed) or 'dec_total'. A recognition rate is dec_recognized / "
                    "dec_total. Filter by `origin_iso3`, `asylum_iso3` and `year` (pushed into the API)."
                ),
                doc_md="## asylum_decisions\n\nOutcomes of asylum procedures per year." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(DECISIONS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT measure, sum(value) AS decisions FROM un.refugees.asylum_decisions "
                    "WHERE origin_iso3 = 'SYR' AND asylum_iso3 = 'DEU' AND year = 2023 "
                    "GROUP BY measure ORDER BY measure"
                ),
                description="Outcomes of Syrian asylum claims decided in Germany in 2023",
            ),
        ]


# ================================================================== solutions

SOLUTIONS_SCHEMA = pa.schema([*BASE_FIELDS, *MEASURE_FIELDS])


@init_single_worker
@bind_fixed_schema
class Solutions(_UnhcrScan):
    """Durable solutions: returns, resettlement and naturalisation."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SOLUTIONS_SCHEMA
    ENDPOINT = "solutions/"
    MEASURES = ("returned_refugees", "resettlement", "naturalisation", "returned_idps")

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "solutions_scan"
        description = "UNHCR durable solutions each year: refugee returns, resettlement, naturalisation, IDP returns"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="UNHCR Durable Solutions",
                category="observations",
                keywords="durable solutions, resettlement, returns, repatriation, naturalisation, unhcr",
                doc_llm=(
                    "Durable solutions during each year, one row per year x origin x asylum country x measure: "
                    "'returned_refugees' (returned from the asylum country), 'resettlement' (resettled to the "
                    "asylum country), 'naturalisation' and 'returned_idps'. Filter by `origin_iso3`, "
                    "`asylum_iso3` and `year` (pushed into the API)."
                ),
                doc_md="## solutions\n\nReturns, resettlement and naturalisation per year." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(SOLUTIONS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT asylum_name, value FROM un.refugees.solutions WHERE origin_iso3 = 'SYR' "
                    "AND year = 2023 AND measure = 'returned_refugees' ORDER BY value DESC LIMIT 5"
                ),
                description="Where Syrian refugees returning home in 2023 came back from",
            ),
        ]


# ================================================================== countries

COUNTRIES_SCHEMA = pa.schema(
    [
        column("unhcr_code", pa.string(), "UNHCR 3-letter country code, e.g. 'ALG'.", nullable=False),
        column("country_iso3", pa.string(), "ISO 3166-1 alpha-3 code, e.g. 'DZA'."),
        column("iso2", pa.string(), "ISO 3166-1 alpha-2 code."),
        column("name", pa.string(), "Short English name."),
        column("major_area", pa.string(), "UNHCR major area, e.g. 'Africa'."),
        column("region", pa.string(), "Region, e.g. 'Northern Africa'."),
    ]
)


@init_single_worker
@bind_fixed_schema
class CountriesScan(ListScan):
    """UNHCR country list with the UNHCR <-> ISO code mapping."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = COUNTRIES_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "countries_scan"
        description = "UNHCR's country list, mapping UNHCR 3-letter codes to ISO alpha-3 codes and regions"
        tags = {
            **object_tags(
                title="UNHCR Countries",
                category="reference",
                keywords="unhcr country codes, iso3, regions, country list",
                doc_llm=(
                    "UNHCR's country list: `unhcr_code` (UNHCR's own 3-letter code, e.g. 'ALG', 'GFR'), "
                    "`country_iso3` ('DZA', 'DEU'), `iso2`, `name`, `major_area` and `region`. The observation "
                    "functions already return both codes; use this to translate UNHCR codes found elsewhere."
                ),
                doc_md="## countries\n\nUNHCR <-> ISO country codes." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(COUNTRIES_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT unhcr_code, country_iso3, name FROM un.refugees.countries "
                    "WHERE unhcr_code <> country_iso3 ORDER BY unhcr_code LIMIT 10"
                ),
                description="Countries whose UNHCR code differs from the ISO code",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """All pages of ``/countries/``."""
        rows: list[dict[str, Any]] = []
        page = 1
        while True:
            body = fetch_json_cached(opts, SOURCE, "countries/", [("limit", PAGE_LIMIT), ("page", page)])
            for c in body.get("items") or []:
                rows.append(
                    {
                        "unhcr_code": c.get("code"),
                        "country_iso3": _code(c.get("iso")),
                        "iso2": _code(c.get("iso2")),
                        "name": c.get("name"),
                        "major_area": c.get("majorArea"),
                        "region": c.get("region"),
                    }
                )
            if page >= int(body.get("maxPages") or 0):
                return rows
            page += 1


FUNCTIONS: list[type[Any]] = []


def _fact_table(fn: type[Any], name: str) -> Any:
    # No declared keys: DuckDB only accepts ENFORCED constraints, and UNHCR rows can carry
    # origin/asylum codes outside its country list (see the waivers in vgi-lint.toml).
    return catalog_table(fn, name, SCHEMA, None, DOMAIN)


TABLES = [
    _fact_table(Population, "populations"),
    _fact_table(AsylumApplications, "asylum_applications"),
    _fact_table(AsylumDecisions, "asylum_decisions"),
    _fact_table(Solutions, "solutions"),
    catalog_table(CountriesScan, "countries", SCHEMA, "unhcr_code", DOMAIN),
]

SCHEMA_COMMENT = (
    "UNHCR refugee statistics: forcibly displaced and stateless populations, asylum applications and "
    "decisions, and durable solutions, by year, country of origin and country of asylum."
)
SCHEMA_TAGS = {
    "vgi.title": "UNHCR Refugee Statistics",
    "domain": DOMAIN,
    "vgi.keywords": keywords_array("refugees, asylum, idps, stateless, unhcr, displacement, resettlement"),
    "vgi.source_url": HOME_URL,
    "vgi.doc_llm": (
        "The `refugees` schema reads UNHCR's Refugee Population Statistics API. Every observation function is in "
        "long format (year, origin, asylum country, measure, value) and is a table: filter with WHERE on "
        "`origin_iso3`, `asylum_iso3` (ISO alpha-3) and `year`, which are pushed into the API. Unfiltered "
        "origins or asylum countries come back one row each (not summed). Values are people (or applications "
        "and decisions)."
    ),
    "vgi.doc_md": (
        "## refugees\n\n[UNHCR Refugee Population Statistics](https://www.unhcr.org/refugee-statistics/) in "
        "tidy long format." + attribution(TERMS)
    ),
}
