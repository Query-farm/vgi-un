"""``main`` schema: the bundled UN M49 table and M49 <-> ISO alpha-3 converters (no network)."""

from __future__ import annotations

import json
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction
from vgi.table_function import bind_fixed_schema, init_single_worker

from .base import ListScan, catalog_table
from .common import Options
from .countries import iso3_for_m49, iso3_to_m49_map, m49_table
from .meta import attribution, column, object_tags, result_columns

M49_URL = "https://unstats.un.org/unsd/methodology/m49/overview/"
TERMS = (
    "United Nations Statistics Division, [Standard country or area codes for statistical use (M49)]"
    f"({M49_URL}), retrieved 2026-11-15; reused with citation under the "
    "[UNdata terms of use](https://data.un.org/Host.aspx?Content=UNdataUse)."
)

M49_SCHEMA = pa.schema(
    [
        column("m49", pa.string(), "3-digit M49 code with leading zeros, e.g. '004'.", nullable=False),
        column("country_or_area", pa.string(), "Country or area name (English).", nullable=False),
        column("iso_alpha2", pa.string(), "ISO 3166-1 alpha-2 code, e.g. 'AF' (NULL for a few areas)."),
        column("iso_alpha3", pa.string(), "ISO 3166-1 alpha-3 code, e.g. 'AFG' (NULL for a few areas)."),
        column("region_code", pa.string(), "M49 region code, e.g. '142' Asia (NULL for Antarctica)."),
        column("region_name", pa.string(), "Region: Africa, Americas, Asia, Europe or Oceania."),
        column("subregion_code", pa.string(), "M49 sub-region code, e.g. '034'."),
        column("subregion_name", pa.string(), "Sub-region name, e.g. 'Southern Asia'."),
        column("intermediate_region_code", pa.string(), "Intermediate region code, e.g. '014'; often NULL."),
        column("intermediate_region_name", pa.string(), "Intermediate region name, e.g. 'Eastern Africa'."),
        column("ldc", pa.bool_(), "True for Least Developed Countries.", nullable=False),
        column("lldc", pa.bool_(), "True for Land Locked Developing Countries.", nullable=False),
        column("sids", pa.bool_(), "True for Small Island Developing States.", nullable=False),
    ]
)


@init_single_worker
@bind_fixed_schema
class M49Scan(ListScan):
    """The bundled M49 overview table."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = M49_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "m49_scan"
        description = "UN M49 country and area codes with ISO alpha-2/alpha-3 codes, regions and LDC/LLDC/SIDS flags"
        tags = {
            **object_tags(
                title="UN M49 Country Codes",
                category="reference",
                keywords="m49, iso 3166, iso3, country codes, regions, ldc, sids, lldc, unsd",
                doc_llm=(
                    "The UN M49 standard (bundled, no network): one row per country or area (248) with `m49` "
                    "('004'), `country_or_area`, `iso_alpha2`, `iso_alpha3`, the region / sub-region / "
                    "intermediate region codes and names, and boolean `ldc`, `lldc`, `sids` flags. Join any "
                    "source's `country_iso3` to `iso_alpha3`, or SDG `m49` to `m49`, to group by region."
                ),
                doc_md="## m49_countries\n\nThe UN M49 country or area code standard." + attribution(TERMS),
            ),
            "vgi.source_url": M49_URL,
            "vgi.result_columns_schema": result_columns(M49_SCHEMA),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Least developed countries per region",
                        "sql": (
                            "SELECT region_name, count(*) AS ldcs FROM un.main.m49_countries WHERE ldc "
                            "GROUP BY region_name ORDER BY region_name"
                        ),
                        "expected_result": [["Africa", 32], ["Americas", 1], ["Asia", 8], ["Oceania", 3]],
                    },
                    {
                        "description": "Look up India by its ISO code",
                        "sql": (
                            "SELECT m49, country_or_area, subregion_name FROM un.main.m49_countries "
                            "WHERE iso_alpha3 = 'IND'"
                        ),
                        "expected_result": [["356", "India", "Southern Asia"]],
                    },
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT subregion_name, count(*) AS countries, count(*) FILTER (WHERE sids) AS sids "
                    "FROM un.main.m49_countries WHERE region_name = 'Oceania' "
                    "GROUP BY subregion_name ORDER BY subregion_name"
                ),
                description="Countries and small island developing states in each Oceania sub-region",
            ),
        ]

    @classmethod
    def rows(cls, opts: Options) -> list[dict[str, Any]]:
        """Rows of the bundled CSV."""
        return list(m49_table().to_pylist())


class IsoFromM49(ScalarFunction):
    """``iso3_from_m49(code)``."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "iso3_from_m49"
        description = "Convert a UN M49 country code ('4', '004' or '356') to its ISO 3166-1 alpha-3 code"
        tags = object_tags(
            title="ISO Alpha-3 From M49",
            category="reference",
            keywords="m49 to iso3, country code conversion, iso 3166, alpha-3",
            doc_llm=(
                "Return the ISO 3166-1 alpha-3 code for a numeric UN M49 country code given as text, with or "
                "without leading zeros: `iso3_from_m49('4')` and `iso3_from_m49('004')` both return 'AFG'. "
                "Region codes ('001' World, '150' Europe), unknown codes and NULL return NULL. Uses the bundled "
                "M49 table; no network."
            ),
            doc_md="## iso3_from_m49(code)\n\n`'356'` -> `'IND'`; regions and unknown codes give NULL.",
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT code, un.main.iso3_from_m49(code) AS iso3 "
                    "FROM (VALUES ('4'), ('356'), ('150')) t(code) ORDER BY code"
                ),
                description="Convert SDG-style M49 area codes to ISO alpha-3 (regions give NULL)",
            ),
        ]

    @classmethod
    def compute(
        cls,
        code: Annotated[pa.StringArray, Param(doc="M49 numeric code as text, e.g. '4' or '004'; NULL gives NULL.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Map each code through the bundled table."""
        return pa.array([iso3_for_m49(c) for c in code.to_pylist()], pa.string())


class M49FromIso(ScalarFunction):
    """``m49_from_iso3(code)``."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "m49_from_iso3"
        description = "Convert an ISO 3166-1 alpha-3 country code ('IND') to its 3-digit UN M49 code ('356')"
        tags = object_tags(
            title="M49 From ISO Alpha-3",
            category="reference",
            keywords="iso3 to m49, country code conversion, m49, numeric country code",
            doc_llm=(
                "Return the 3-digit UN M49 code (`VARCHAR` with leading zeros) for an ISO 3166-1 alpha-3 code, "
                "case-insensitive: `m49_from_iso3('afg')` returns '004'. Unknown codes and NULL return NULL. "
                "Uses the bundled M49 table; no network."
            ),
            doc_md="## m49_from_iso3(code)\n\n`'AFG'` -> `'004'`; unknown codes give NULL.",
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT iso3, un.main.m49_from_iso3(iso3) AS m49 "
                    "FROM (VALUES ('AFG'), ('bra'), ('XXX')) t(iso3) ORDER BY iso3"
                ),
                description="Convert ISO alpha-3 codes to M49 (unknown codes give NULL)",
            ),
        ]

    @classmethod
    def compute(
        cls,
        code: Annotated[pa.StringArray, Param(doc="ISO alpha-3 country code, e.g. 'IND'; NULL gives NULL.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Map each code through the bundled table."""
        mapping = iso3_to_m49_map()
        return pa.array([None if c is None else mapping.get(c.strip().upper()) for c in code.to_pylist()], pa.string())


FUNCTIONS: list[type[Any]] = [IsoFromM49, M49FromIso]
TABLES = [catalog_table(M49Scan, "m49_countries", "main", "m49", "country codes")]
