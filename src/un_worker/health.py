"""``health`` schema: the WHO Global Health Observatory OData API (keyless).

API: https://www.who.int/data/gho/info/gho-odata-api . Observations come from
``/api/{IndicatorCode}``; country (``SpatialDim``) and year (``TimeDim``) filters
are pushed into an OData ``$filter``. ``@odata.nextLink`` paging is followed via
``$skip``.
"""

from __future__ import annotations

import datetime as dt
import urllib.parse
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
    to_float,
    to_int,
)
from .meta import attribution, column, keywords_array, object_tags, result_columns

SCHEMA = "health"
DOMAIN = "global health"
SOURCE = "health"
HOME_URL = "https://www.who.int/data/gho"
API_URL = "https://www.who.int/data/gho/info/gho-odata-api"
TERMS = (
    "World Health Organization, Global Health Observatory; used under the "
    "[WHO terms and conditions for datasets](https://www.who.int/about/policies/publishing/data-policy/"
    "terms-and-conditions) (attribution 'WHO, <dataset>, <year>, <date of access>'). Fetched at query time; "
    "not redistributed by this worker."
)
CODE_PATTERN = r"^[A-Za-z0-9_.\-]+$"


# ======================================================================== lists

INDICATORS_SCHEMA = pa.schema(
    [
        column("indicator_code", pa.string(), "GHO indicator code, e.g. 'WHOSIS_000001'.", nullable=False),
        column("name", pa.string(), "Indicator name, e.g. 'Life expectancy at birth (years)'."),
    ]
)


@init_single_worker
@bind_fixed_schema
class IndicatorsScan(ListScan):
    """The GHO indicator list."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = INDICATORS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "indicators_scan"
        description = "The 3,000+ WHO Global Health Observatory indicators with their codes and names"
        tags = {
            **object_tags(
                title="WHO GHO Indicators",
                category="catalogs",
                keywords="who indicators, gho, health indicators, life expectancy, mortality, search",
                doc_llm=(
                    "Every WHO Global Health Observatory indicator: `indicator_code` (pass to `health.data`) "
                    "and `name`. Search with `name ILIKE '%life expectancy%'`."
                ),
                doc_md="## indicators\n\nGHO indicator codes and names." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(INDICATORS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT indicator_code, name FROM un.health.indicators "
                    "WHERE name ILIKE '%life expectancy%' ORDER BY indicator_code"
                ),
                description="Find the life expectancy indicators",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Return the indicators from ``/api/Indicator``."""
        body = fetch_json_cached(opts, SOURCE, "Indicator")
        return [{"indicator_code": i["IndicatorCode"], "name": i.get("IndicatorName")} for i in body["value"]]


COUNTRIES_SCHEMA = pa.schema(
    [
        column("country_iso3", pa.string(), "ISO 3166-1 alpha-3 code as used in SpatialDim.", nullable=False),
        column("name", pa.string(), "Country name."),
        column("who_region_code", pa.string(), "WHO region code: AFR, AMR, SEAR, EUR, EMR, WPR."),
        column("who_region", pa.string(), "WHO region name, e.g. 'South-East Asia'."),
    ]
)


@init_single_worker
@bind_fixed_schema
class CountriesScan(ListScan):
    """GHO countries and their WHO regions."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = COUNTRIES_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "countries_scan"
        description = "Countries in the WHO Global Health Observatory with their WHO region"
        tags = {
            **object_tags(
                title="WHO Countries And Regions",
                category="reference",
                keywords="who regions, countries, afr, amr, sear, eur, emr, wpr",
                doc_llm=(
                    "GHO country dimension values: `country_iso3`, `name`, `who_region_code` (AFR, AMR, SEAR, "
                    "EUR, EMR, WPR) and `who_region`. Use it to aggregate `health.data` by WHO region."
                ),
                doc_md="## countries\n\nCountries and WHO regions." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(COUNTRIES_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT who_region, count(*) AS countries FROM un.health.countries "
                    "GROUP BY who_region ORDER BY who_region"
                ),
                description="Number of countries in each WHO region",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Countries from ``/api/DIMENSION/COUNTRY/DimensionValues``."""
        body = fetch_json_cached(opts, SOURCE, "DIMENSION/COUNTRY/DimensionValues")
        return [
            {
                "country_iso3": c["Code"],
                "name": c.get("Title"),
                "who_region_code": c.get("ParentCode"),
                "who_region": c.get("ParentTitle"),
            }
            for c in body["value"]
        ]


DIMENSIONS_SCHEMA = pa.schema(
    [
        column("dimension", pa.string(), "Dimension code, e.g. 'SEX', 'AGEGROUP', 'COUNTRY'.", nullable=False),
        column("title", pa.string(), "Dimension title."),
    ]
)


@init_single_worker
@bind_fixed_schema
class DimensionsScan(ListScan):
    """GHO dimensions (the Dim1/Dim2/Dim3 types)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DIMENSIONS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "dimensions_scan"
        description = "GHO dimension types (SEX, AGEGROUP, ...) that observations are broken down by"
        tags = {
            **object_tags(
                title="WHO GHO Dimensions",
                category="catalogs",
                keywords="who dimensions, sex, age group, disaggregation, gho",
                doc_llm=(
                    "The GHO dimension types: `dimension` code (the values of `dim1_type`/`dim2_type`/"
                    "`dim3_type` in `health.data`, e.g. 'SEX', 'AGEGROUP') and `title`. List a dimension's "
                    "codes with `health.dimension_values('SEX')`."
                ),
                doc_md="## dimensions\n\nGHO disaggregation dimensions." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(DIMENSIONS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT dimension, title FROM un.health.dimensions WHERE title ILIKE '%age%' ORDER BY dimension",
                description="Dimensions about age",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Dimensions from ``/api/Dimension``."""
        body = fetch_json_cached(opts, SOURCE, "Dimension")
        return [{"dimension": d["Code"], "title": d.get("Title")} for d in body["value"]]


VALUES_SCHEMA = pa.schema(
    [
        column("dimension", pa.string(), "The dimension asked for.", nullable=False),
        column("code", pa.string(), "Value code, e.g. 'SEX_MLE'.", nullable=False),
        column("title", pa.string(), "Value title, e.g. 'Male'."),
        column("parent_dimension", pa.string(), "Parent dimension code, when hierarchical."),
        column("parent_code", pa.string(), "Parent value code."),
        column("parent_title", pa.string(), "Parent value title."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class DimensionArgs:
    """Arguments for ``dimension_values(dimension)``."""

    dimension: Annotated[
        str, Arg(0, doc="Dimension code from health.dimensions, e.g. 'SEX' or 'AGEGROUP'.", pattern=CODE_PATTERN)
    ]


@dataclass(kw_only=True)
class DoneState(ArrowSerializableDataclass):
    """Cursor: whether the single response was emitted."""

    done: bool = False


@init_single_worker
@bind_fixed_schema
class DimensionValues(TableFunctionGenerator[DimensionArgs, DoneState]):
    """Codes of one GHO dimension."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = VALUES_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "dimension_values"
        description = "The codes and titles of one GHO dimension, e.g. SEX -> SEX_MLE 'Male'"
        tags = {
            **object_tags(
                title="WHO GHO Dimension Values",
                category="catalogs",
                keywords="dimension values, codes, sex codes, age groups, gho",
                doc_llm=(
                    "List the codes of one GHO dimension: `dimension_values('SEX')` returns SEX_BTSX (both "
                    "sexes), SEX_FMLE, SEX_MLE with titles. Use it to decode `dim1`/`dim2`/`dim3` in "
                    "`health.data`. Columns: dimension, code, title, parent_dimension, parent_code, parent_title."
                ),
                doc_md="## dimension_values(dimension)\n\nCodes of one dimension." + attribution(TERMS),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(VALUES_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT code, title FROM un.health.dimension_values('SEX') ORDER BY code",
                description="Decode the SEX dimension",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[DimensionArgs]) -> DoneState:
        """Not yet emitted."""
        return DoneState()

    @classmethod
    def process(cls, params: ProcessParams[DimensionArgs], state: DoneState, out: OutputCollector) -> None:
        """Fetch and emit the dimension's values."""
        if state.done:
            out.finish()
            return
        dim = params.args.dimension.upper()
        body = fetch_json_cached(options_from(params.attach_options), SOURCE, f"DIMENSION/{dim}/DimensionValues")
        rows = [
            {
                "dimension": dim,
                "code": v["Code"],
                "title": v.get("Title"),
                "parent_dimension": v.get("ParentDimension") or None,
                "parent_code": v.get("ParentCode") or None,
                "parent_title": v.get("ParentTitle") or None,
            }
            for v in body.get("value") or []
        ]
        state.done = True
        if not rows:
            out.finish()
            return
        emit_rows(out, params.output_schema, rows)


# ========================================================================= data

DATA_SCHEMA = pa.schema(
    [
        column("indicator_code", pa.string(), "GHO indicator code.", nullable=False),
        column("country_iso3", pa.string(), "ISO alpha-3 code when the spatial dimension is a country, else NULL."),
        column("spatial_dim_type", pa.string(), "COUNTRY, REGION, WORLDBANKINCOMEGROUP, GLOBAL, ..."),
        column("spatial_dim", pa.string(), "Area code: ISO alpha-3 for countries, e.g. 'SEAR' for a region."),
        column("parent_location_code", pa.string(), "WHO region code of the country, e.g. 'SEAR'."),
        column("parent_location", pa.string(), "WHO region name."),
        column("year", pa.int32(), "Year, when the time dimension is a year (NULL otherwise)."),
        column("time_dim_type", pa.string(), "YEAR, or another period type."),
        column("period", pa.string(), "The period as published, e.g. '2019' or '2015-2019'."),
        column("period_begin", pa.date32(), "First day of the period."),
        column("period_end", pa.date32(), "Last day of the period."),
        column("sex", pa.string(), "Sex code (SEX_BTSX, SEX_FMLE, SEX_MLE) when one of the dimensions is SEX."),
        column("dim1_type", pa.string(), "First disaggregation dimension type, e.g. 'SEX'."),
        column("dim1", pa.string(), "First disaggregation code, e.g. 'SEX_MLE'."),
        column("dim2_type", pa.string(), "Second disaggregation dimension type."),
        column("dim2", pa.string(), "Second disaggregation code."),
        column("dim3_type", pa.string(), "Third disaggregation dimension type."),
        column("dim3", pa.string(), "Third disaggregation code."),
        column("data_source", pa.string(), "Data source dimension code, when published."),
        column("value", pa.float64(), "Numeric value (NumericValue)."),
        column("value_text", pa.string(), "Display value as published, e.g. '70.1 [68.8-71.5]'."),
        column("low", pa.float64(), "Lower uncertainty bound."),
        column("high", pa.float64(), "Upper uncertainty bound."),
        column("comments", pa.string(), "Published comment or footnote."),
        column("updated_at", pa.timestamp("us", tz="UTC"), "When WHO last updated the observation."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class DataArgs:
    """Arguments for ``health.data(indicator)``."""

    indicator: Annotated[
        str,
        Arg(0, doc="GHO indicator code from health.indicators, e.g. 'WHOSIS_000001'.", pattern=CODE_PATTERN),
    ]


@dataclass(kw_only=True)
class SkipState(ArrowSerializableDataclass):
    """Cursor: OData ``$skip`` of the next page, or -1 once done."""

    skip: int = 0


def _quote(code: str) -> str:
    return "'" + code.replace("'", "''") + "'"


def odata_filter(filters: Any) -> str | None:
    """OData ``$filter`` for pushed-down country and year predicates."""
    clauses: list[str] = []
    codes = sorted(
        {str(v).upper() for v in (pushed_values(filters, "country_iso3") or [])}
        | {str(v) for v in (pushed_values(filters, "spatial_dim") or [])}
    )
    if codes:
        clauses.append("(" + " or ".join(f"SpatialDim eq {_quote(c)}" for c in codes) + ")")
    lo, hi = pushed_int_range(filters, "year")
    if lo is not None:
        clauses.append(f"TimeDim ge {lo}")
    if hi is not None:
        clauses.append(f"TimeDim le {hi}")
    return " and ".join(clauses) if clauses else None


def _date(raw: Any) -> dt.date | None:
    if not raw:
        return None
    try:
        return dt.date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def _timestamp(raw: Any) -> dt.datetime | None:
    if not raw:
        return None
    try:
        return dt.datetime.fromisoformat(str(raw)).astimezone(dt.UTC)
    except ValueError:
        return None


def data_row(v: dict[str, Any]) -> dict[str, Any]:
    """Map one OData row to an output row."""
    dims = [(v.get(f"Dim{i}Type"), v.get(f"Dim{i}")) for i in (1, 2, 3)]
    sex = next((code for typ, code in dims if typ == "SEX"), None)
    is_country = v.get("SpatialDimType") == "COUNTRY"
    return {
        "indicator_code": v.get("IndicatorCode"),
        "country_iso3": v.get("SpatialDim") if is_country else None,
        "spatial_dim_type": v.get("SpatialDimType"),
        "spatial_dim": v.get("SpatialDim"),
        "parent_location_code": v.get("ParentLocationCode"),
        "parent_location": v.get("ParentLocation"),
        "year": to_int(v.get("TimeDim")) if v.get("TimeDimType") == "YEAR" else None,
        "time_dim_type": v.get("TimeDimType"),
        "period": v.get("TimeDimensionValue"),
        "period_begin": _date(v.get("TimeDimensionBegin")),
        "period_end": _date(v.get("TimeDimensionEnd")),
        "sex": sex,
        "dim1_type": dims[0][0],
        "dim1": dims[0][1],
        "dim2_type": dims[1][0],
        "dim2": dims[1][1],
        "dim3_type": dims[2][0],
        "dim3": dims[2][1],
        "data_source": v.get("DataSourceDim"),
        "value": to_float(v.get("NumericValue")),
        "value_text": None if v.get("Value") is None else str(v.get("Value")),
        "low": to_float(v.get("Low")),
        "high": to_float(v.get("High")),
        "comments": v.get("Comments"),
        "updated_at": _timestamp(v.get("Date")),
    }


def next_skip(body: dict[str, Any]) -> int:
    """``$skip`` from ``@odata.nextLink``, or -1 when there is no next page."""
    link = body.get("@odata.nextLink")
    if not link:
        return -1
    query = urllib.parse.parse_qs(urllib.parse.urlparse(str(link)).query)
    skip = query.get("$skip") or query.get("%24skip")
    return int(skip[0]) if skip else -1


@init_single_worker
@bind_fixed_schema
class Data(TableFunctionGenerator[DataArgs, SkipState]):
    """Stream one GHO indicator's observations."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATA_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "data"
        description = "WHO Global Health Observatory observations of one indicator, by country or region, year and sex"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="WHO GHO Observations",
                category="observations",
                keywords="who data, health data, life expectancy, mortality, gho odata, country health statistics",
                doc_llm=(
                    "Observations of one WHO Global Health Observatory indicator. `indicator` (positional, "
                    "required) is a code from `health.indicators`, e.g. 'WHOSIS_000001' (life expectancy at "
                    "birth). One row per area x period x disaggregation. Filter with WHERE `country_iso3 = 'IND'` "
                    "(or IN), `spatial_dim` for regions ('SEAR', 'GLOBAL') and `year` ranges; these become an "
                    "OData $filter. Countries have spatial_dim_type 'COUNTRY'; regions and income groups have "
                    "country_iso3 NULL. `sex` is SEX_BTSX (both sexes), SEX_FMLE or SEX_MLE when the indicator "
                    "is broken down by sex; other breakdowns are in dim1..dim3 (decode with "
                    "`health.dimension_values`). `value` is the numeric value with `low`/`high` bounds."
                ),
                doc_md=(
                    "## data(indicator)\n\nTidy observations of one GHO indicator; `WHERE` filters on "
                    "`country_iso3`, `spatial_dim` and `year` are sent to the API as an OData `$filter`."
                    + attribution(TERMS)
                ),
            ),
            "vgi.source_url": API_URL,
            "vgi.result_columns_schema": result_columns(DATA_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT year, sex, value FROM un.health.data('WHOSIS_000001') WHERE country_iso3 = 'IND' "
                    "AND year BETWEEN 2015 AND 2021 ORDER BY year, sex"
                ),
                description="Life expectancy at birth in India by sex, 2015-2021",
            ),
            FunctionExample(
                sql=(
                    "SELECT d.country_iso3, m.country_or_area, d.value FROM un.health.data('WHOSIS_000001') d "
                    "JOIN un.main.m49_countries m ON m.iso_alpha3 = d.country_iso3 "
                    "WHERE d.country_iso3 IN ('BGD', 'IND', 'PAK') AND d.year = 2019 AND d.sex = 'SEX_BTSX' "
                    "ORDER BY d.value DESC"
                ),
                description="Life expectancy in 2019 in three South Asian countries, with M49 names",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[DataArgs]) -> SkipState:
        """Start at the first row."""
        return SkipState()

    @classmethod
    def process(cls, params: ProcessParams[DataArgs], state: SkipState, out: OutputCollector) -> None:
        """Fetch one OData page and emit it."""
        if state.skip < 0:
            out.finish()
            return
        query: list[tuple[str, str | int]] = []
        flt = odata_filter(params.current_pushdown_filters)
        if flt:
            query.append(("$filter", flt))
        if state.skip:
            query.append(("$skip", state.skip))
        body = fetch_json(options_from(params.attach_options), SOURCE, params.args.indicator, query)
        rows = [data_row(v) for v in body.get("value") or []]
        state.skip = next_skip(body)
        if rows:
            emit_rows(out, params.output_schema, rows)
        elif state.skip < 0:
            out.finish()


FUNCTIONS: list[type[Any]] = [Data, DimensionValues]
TABLES = [
    catalog_table(IndicatorsScan, "indicators", SCHEMA, "indicator_code", DOMAIN),
    catalog_table(CountriesScan, "countries", SCHEMA, "country_iso3", DOMAIN),
    catalog_table(DimensionsScan, "dimensions", SCHEMA, "dimension", DOMAIN),
]

SCHEMA_COMMENT = (
    "WHO Global Health Observatory: about 3,000 health indicators and their observations by country, WHO "
    "region, year and sex."
)
SCHEMA_TAGS = {
    "vgi.title": "WHO Global Health Observatory",
    "domain": DOMAIN,
    "vgi.keywords": keywords_array("who, health, gho, life expectancy, mortality, disease, odata"),
    "vgi.source_url": HOME_URL,
    "vgi.doc_llm": (
        "The `health` schema reads the WHO Global Health Observatory OData API. Find an indicator code in the "
        "indicator list, then read its observations with the observations function, filtering by "
        "`country_iso3` and `year` (pushed into the API). Countries use ISO alpha-3 codes; regions use WHO "
        "region codes in `spatial_dim`."
    ),
    "vgi.doc_md": (
        "## health\n\nThe [WHO Global Health Observatory](https://www.who.int/data/gho)." + attribution(TERMS)
    ),
}
