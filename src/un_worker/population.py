"""``population`` schema: the UN DESA World Population Prospects Data Portal API.

API: https://population.un.org/dataportalapi/index.html . The indicator and
location lists are keyless; the data endpoints require a bearer token, read
from a ``un_population`` DuckDB secret. The portal allows 5 requests per 10
seconds, so requests to it are spaced 2.1 s apart.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg, Secret
from vgi.catalog import SecretTypeSpec
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
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

SCHEMA = "population"
DOMAIN = "demography"
SOURCE = "population"
HOME_URL = "https://population.un.org/wpp/"
API_URL = "https://population.un.org/dataportalapi/index.html"
SECRET_TYPE = "un_population"
TERMS = (
    "United Nations, Department of Economic and Social Affairs, Population Division, "
    "[World Population Prospects / Data Portal](https://population.un.org/dataportal/), "
    "[CC BY 3.0 IGO](https://creativecommons.org/licenses/by/3.0/igo/)."
)
PAGE_SIZE = 100
FIRST_YEAR = 1950
LAST_YEAR = 2100

POPULATION_SECRET_TYPE = SecretTypeSpec(
    name=SECRET_TYPE,
    description="Bearer token for the UN DESA Population Division Data Portal API (population.data).",
    schema=pa.schema(
        [
            pa.field(
                "token",
                pa.string(),
                metadata={
                    "redact": "true",
                    "vgi_doc": (
                        "Data Portal API token (the 'GENERATE TOKEN' button on the API page), sent as a bearer token."
                    ),
                },
            )
        ],
        metadata={"vgi_doc_url": "https://github.com/Query-farm/vgi-un#secrets"},
    ),
)


def _paged(opts: Options, path: str) -> list[dict[str, Any]]:
    """Every row of a keyless paged list endpoint."""
    rows: list[dict[str, Any]] = []
    page = 1
    while True:
        body = fetch_json_cached(opts, SOURCE, path, [("pageNumber", page), ("pageSize", PAGE_SIZE)])
        rows.extend(body.get("data") or [])
        if page >= int(body.get("pages") or 0):
            return rows
        page += 1


# ======================================================================== lists

INDICATORS_SCHEMA = pa.schema(
    [
        column(
            "indicator_id",
            pa.int32(),
            "Numeric indicator id, e.g. 49 (total population); pass to population.data().",
            nullable=False,
        ),
        column("short_name", pa.string(), "Short code, e.g. 'TPopulation'."),
        column("name", pa.string(), "Indicator name with unit."),
        column("description", pa.string(), "What the indicator measures."),
        column("topic", pa.string(), "Topic, e.g. 'Population', 'Fertility', 'Mortality'."),
        column("source", pa.string(), "Source dataset, e.g. 'World Population Prospects 2024'."),
        column("source_year", pa.int32(), "Revision year of the source."),
        column("value_type", pa.string(), "Value type, e.g. 'number', 'percent', 'rate'."),
        column("unit", pa.string(), "Unit label, e.g. 'thousands', 'per cent'."),
        column("unit_scaling", pa.float64(), "Multiply value by this to get base units (e.g. 1000 for thousands)."),
        column("has_age", pa.bool_(), "True when the indicator is broken down by age."),
        column("has_sex", pa.bool_(), "True when the indicator is broken down by sex."),
        column("has_variant", pa.bool_(), "True when the indicator has projection variants."),
    ]
)


@init_single_worker
@bind_fixed_schema
class IndicatorsScan(ListScan):
    """WPP Data Portal indicators (keyless)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = INDICATORS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "indicators_scan"
        description = (
            "The UN Population Division Data Portal indicators (population, fertility, mortality, migration ...)"
        )
        tags = {
            **object_tags(
                title="WPP Indicators",
                category="catalogs",
                keywords="population indicators, fertility, mortality, life expectancy, migration, wpp",
                doc_llm=(
                    "The UN DESA Population Division Data Portal indicators (keyless): `indicator_id` (pass to "
                    "`population.data`), `short_name`, `name`, `description`, `topic`, `source`, `unit`, "
                    "`unit_scaling` and whether the indicator has age / sex / variant breakdowns."
                ),
                doc_md="## indicators\n\nData Portal indicators." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(INDICATORS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT topic, count(*) AS indicators FROM un.population.indicators GROUP BY topic ORDER BY topic",
                description="Data Portal indicators per topic",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Rows from ``/indicators``."""
        return [
            {
                "indicator_id": i["id"],
                "short_name": i.get("shortName"),
                "name": i.get("name"),
                "description": i.get("description"),
                "topic": i.get("topicName"),
                "source": i.get("sourceName"),
                "source_year": to_int(i.get("sourceYear")),
                "value_type": i.get("valueType"),
                "unit": i.get("unitLongLabel") or i.get("unitShortLabel"),
                "unit_scaling": to_float(i.get("unitScaling")),
                "has_age": i.get("dimAge"),
                "has_sex": i.get("dimSex"),
                "has_variant": i.get("dimVariant"),
            }
            for i in _paged(opts, "indicators")
        ]


LOCATIONS_SCHEMA = pa.schema(
    [
        column(
            "location_id", pa.int32(), "Location id (the M49 numeric code for countries, e.g. 356).", nullable=False
        ),
        column("name", pa.string(), "Location name."),
        column("country_iso3", pa.string(), "ISO alpha-3 code.", nullable=True),
        column("iso2", pa.string(), "ISO alpha-2 code."),
        column("latitude", pa.float64(), "Latitude of the location's centroid, in decimal degrees (WGS84)."),
        column("longitude", pa.float64(), "Longitude of the location's centroid, in decimal degrees (WGS84)."),
    ]
)


@init_single_worker
@bind_fixed_schema
class LocationsScan(ListScan):
    """WPP Data Portal locations (keyless)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = LOCATIONS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "locations_scan"
        description = "Countries and areas of the UN Population Division Data Portal with ids and ISO codes"
        tags = {
            **object_tags(
                title="WPP Locations",
                category="reference",
                keywords="wpp locations, countries, location id, m49, iso3",
                doc_llm=(
                    "Data Portal locations (keyless): `location_id` (the M49 number for countries), `name`, "
                    "`country_iso3`, `iso2`, `latitude`, `longitude`. population.data accepts ISO codes "
                    "directly, so this is mostly for browsing."
                ),
                doc_md="## locations\n\nData Portal locations." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(LOCATIONS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT location_id, name FROM un.population.locations "
                    "WHERE country_iso3 IN ('IND', 'NGA') ORDER BY name"
                ),
                description="Data Portal ids of India and Nigeria",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Rows from ``/locations``."""
        return [
            {
                "location_id": loc["id"],
                "name": loc.get("name"),
                "country_iso3": loc.get("iso3") or None,
                "iso2": loc.get("iso2") or None,
                "latitude": to_float(loc.get("latitude")),
                "longitude": to_float(loc.get("longitude")),
            }
            for loc in _paged(opts, "locations")
        ]


# ========================================================================= data

DATA_SCHEMA = pa.schema(
    [
        column("indicator_id", pa.int32(), "Indicator id.", nullable=False),
        column("indicator", pa.string(), "Indicator short name, e.g. 'TPopulation'."),
        column("indicator_name", pa.string(), "Indicator display name."),
        column("location_id", pa.int32(), "Location id (M49 number for countries).", nullable=False),
        column("location", pa.string(), "Location name."),
        column("country_iso3", pa.string(), "ISO alpha-3 code of the location (NULL for aggregates)."),
        column("year", pa.int32(), "Year (start of the period for multi-year periods)."),
        column("time_label", pa.string(), "Period as published, e.g. '2024' or '2020-2025'."),
        column("variant", pa.string(), "Projection variant, e.g. 'Median', 'Estimates', 'High', 'Low'."),
        column("sex", pa.string(), "Sex: 'Both sexes', 'Female' or 'Male'."),
        column("age_label", pa.string(), "Age group label, e.g. 'Total', '0-4', '15-49'."),
        column("age_start", pa.int32(), "First age of the group."),
        column("age_end", pa.int32(), "Last age of the group (NULL for open-ended groups)."),
        column("category", pa.string(), "Category dimension, e.g. 'All women' for family planning indicators."),
        column("estimate_type", pa.string(), "Estimate type as published."),
        column("estimate_method", pa.string(), "Estimate method as published (e.g. 'Interpolation')."),
        column("source", pa.string(), "Source dataset, e.g. 'World Population Prospects 2024'."),
        column("revision", pa.int32(), "Revision year of the source."),
        column("value", pa.float64(), "Value in the indicator's unit (see population.indicators.unit).", nullable=True),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class DataArgs:
    """Arguments for ``population.data(indicator, location := '', variants := '')``."""

    indicator: Annotated[
        str,
        Arg(
            0,
            doc="Indicator id(s) from population.indicators, e.g. '49' (total population) or '49,19'.",
            pattern=r"^\s*\d+(\s*,\s*\d+)*\s*$",
        ),
    ]
    location: Annotated[
        str,
        Arg(
            "location",
            default="",
            doc="Countries as ISO alpha-3 or location ids, comma-separated ('IND,NGA'); or filter WHERE country_iso3.",
        ),
    ]
    variants: Annotated[
        str,
        Arg(
            "variants",
            default="",
            doc="Projection variant id(s) or 'all'; default '' = the median variant (estimates + median projection).",
        ),
    ]


@dataclass(kw_only=True)
class PageState(ArrowSerializableDataclass):
    """Cursor: the next page (1-based), or 0 once done."""

    page: int = 1


def token_from(params: ProcessParams[Any]) -> str:
    """Return the bearer token from the ``un_population`` secret; fail clearly when it is missing."""
    for secret in params.secrets.of_type(SECRET_TYPE):
        value = secret.get("token")
        if value is not None and value.as_py():
            return str(value.as_py())
    raise ValueError(
        "population.data needs a UN Population Division Data Portal token: get one from "
        "https://population.un.org/dataportalapi/index.html ('GENERATE TOKEN'), then run "
        f"CREATE SECRET (TYPE {SECRET_TYPE}, TOKEN '<token>'). The indicator and location lists work without it."
    )


def location_ids(opts: Options, codes: list[str]) -> list[int]:
    """Location ids for ISO alpha-3 codes or numeric ids."""
    by_iso = {str(loc.get("iso3") or "").upper(): int(loc["id"]) for loc in _paged(opts, "locations")}
    ids: set[int] = set()
    unknown = []
    for code in codes:
        c = code.strip().upper()
        if c.isdigit():
            ids.add(int(c))
        elif c in by_iso:
            ids.add(by_iso[c])
        else:
            unknown.append(code)
    if unknown and not ids:
        raise ValueError(f"unknown location(s) {unknown}: use ISO alpha-3 codes or ids from un.population.locations")
    return sorted(ids)


def data_path(indicators: list[int], locations: list[int], start: int, end: int) -> str:
    """Path of a data request."""
    ind = ",".join(map(str, indicators))
    loc = ",".join(map(str, locations))
    return f"data/indicators/{ind}/locations/{loc}/start/{start}/end/{end}"


def data_row(d: dict[str, Any]) -> dict[str, Any]:
    """Map one ``MainDataSetFullDto`` to an output row."""
    label = str(d.get("timeLabel") or "")
    return {
        "indicator_id": d.get("indicatorId"),
        "indicator": d.get("indicator"),
        "indicator_name": d.get("indicatorDisplayName"),
        "location_id": d.get("locationId"),
        "location": d.get("location"),
        "country_iso3": known_iso3(d.get("iso3")),
        "year": to_int(label[:4]) if label[:4].isdigit() else None,
        "time_label": label or None,
        "variant": d.get("variant"),
        "sex": d.get("sex"),
        "age_label": d.get("ageLabel"),
        "age_start": to_int(d.get("ageStart")),
        "age_end": to_int(d.get("ageEnd")),
        "category": d.get("category"),
        "estimate_type": d.get("estimateType"),
        "estimate_method": d.get("estimateMethod"),
        "source": d.get("source"),
        "revision": to_int(d.get("revision")),
        "value": to_float(d.get("value")),
    }


@init_single_worker
class Data(TableFunctionGenerator[DataArgs, PageState]):
    """Stream WPP data for indicators x locations x years (needs a token)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATA_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "data"
        description = "UN World Population Prospects estimates and projections by indicator, country, year, sex and age"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="WPP Observations",
                category="observations",
                keywords="population data, world population prospects, projections, fertility, life expectancy",
                doc_llm=(
                    "World Population Prospects estimates (to 2023) and projections (to 2100) from the UN DESA "
                    "Population Division Data Portal. NEEDS A TOKEN: CREATE SECRET (TYPE un_population, TOKEN "
                    "'...'). `indicator` (positional) is an id from `population.indicators` (49 = total "
                    "population, thousands); countries come from `location := 'IND,NGA'` or WHERE "
                    "`country_iso3 IN (...)` (at least one is required); WHERE `year BETWEEN ...` narrows the "
                    "request (default 1950-2100). `variants := ''` gives the median variant. One row per "
                    "indicator x location x period x variant x sex x age group; `value` is in the indicator's "
                    "unit (often thousands)."
                ),
                doc_md=(
                    "## data(indicator, location := '', variants := '')\n\nWPP estimates and projections; "
                    "requires a `un_population` secret." + attribution(TERMS)
                ),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(DATA_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT year, value AS population_thousands FROM un.population.data('49') "
                    "WHERE country_iso3 = 'IND' AND year BETWEEN 2020 AND 2025 AND sex = 'Both sexes' ORDER BY year"
                ),
                description="India's total population (thousands), 2020-2025 (needs a un_population secret)",
            ),
        ]

    @classmethod
    def on_bind(
        cls,
        params: BindParams[DataArgs],
        *,
        un_population: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the output schema and ask DuckDB for the ``un_population`` secret."""
        return BindResponse(output_schema=DATA_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[DataArgs]) -> PageState:
        """Start at page 1."""
        return PageState()

    @classmethod
    def process(cls, params: ProcessParams[DataArgs], state: PageState, out: OutputCollector) -> None:
        """Fetch one page of data and emit it."""
        if state.page == 0:
            out.finish()
            return
        token = token_from(params)
        opts = options_from(params.attach_options)
        filters = params.current_pushdown_filters
        codes = merge_codes(split_codes(params.args.location), pushed_values(filters, "country_iso3"))
        codes = merge_codes(codes, pushed_values(filters, "location_id"))
        if not codes:
            raise ValueError(
                "population.data needs countries: pass location := 'IND' or filter WHERE country_iso3 = 'IND' "
                "(or IN (...)); see un.population.locations"
            )
        lo, hi = pushed_int_range(filters, "year")
        start = max(FIRST_YEAR, lo) if lo is not None else FIRST_YEAR
        end = min(LAST_YEAR, hi) if hi is not None else LAST_YEAR
        indicators = sorted({int(c) for c in split_codes(params.args.indicator)})
        query: list[tuple[str, str | int]] = [("pageNumber", state.page), ("pageSize", PAGE_SIZE)]
        if params.args.variants.strip():
            query.append(("variants", params.args.variants.strip()))
        try:
            body = fetch_json(
                opts,
                SOURCE,
                data_path(indicators, location_ids(opts, codes), start, end),
                query,
                headers={"Authorization": f"Bearer {token}"},
            )
        except UpstreamError as err:
            if err.status == 401:
                raise UpstreamError(
                    f"the Data Portal rejected the {SECRET_TYPE} token (HTTP 401); generate a new one at {API_URL}",
                    status=401,
                ) from err
            raise
        items = body if isinstance(body, list) else body.get("data") or []
        pages = 1 if isinstance(body, list) else int(body.get("pages") or 0)
        rows = [data_row(d) for d in items]
        if rows:
            emit_rows(out, params.output_schema, rows)
        if state.page >= pages or not rows:
            state.page = 0
            if not rows:
                out.finish()
            return
        state.page += 1


FUNCTIONS: list[type[Any]] = [Data]
TABLES = [
    catalog_table(IndicatorsScan, "indicators", SCHEMA, "indicator_id", DOMAIN),
    catalog_table(LocationsScan, "locations", SCHEMA, "location_id", DOMAIN),
]

SCHEMA_COMMENT = (
    "UN DESA World Population Prospects: population, fertility, mortality and migration estimates and "
    "projections to 2100. The indicator and location lists are keyless; data needs a un_population secret."
)
SCHEMA_TAGS = {
    "vgi.title": "UN World Population Prospects",
    "domain": DOMAIN,
    "vgi.keywords": keywords_array("population, world population prospects, wpp, demography, fertility, projections"),
    "vgi.source_url": HOME_URL,
    "vgi.doc_llm": (
        "The `population` schema reads the UN DESA Population Division Data Portal API. The indicator and "
        "location lists are keyless. The observations function needs a bearer token in a `un_population` "
        "DuckDB secret and at least one country (ISO alpha-3). The portal is rate limited (5 requests per 10 "
        "seconds), so keep requests narrow."
    ),
    "vgi.doc_md": (
        "## population\n\n[World Population Prospects](https://population.un.org/wpp/) via the Data Portal API."
        + attribution(TERMS)
    ),
}
