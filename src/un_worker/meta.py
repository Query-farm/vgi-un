"""Discovery metadata for the ``un`` catalog and helpers shared by every schema.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Catalog-level tags live in ``CATALOG_TAGS``; each schema module
(``sdg.py``, ``uis.py`` ...) carries its own schema docs; every function and
table carries ``object_tags(...)``.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "un"

SOURCE_URL = "https://github.com/Query-farm/vgi-un"

# Every schema uses the same three sections.
SCHEMA_CATEGORIES = [
    {
        "name": "catalogs",
        "title": "Indicator catalogs",
        "description": "Lists of the indicators, series, goals, versions and code lists a source publishes.",
    },
    {
        "name": "observations",
        "title": "Observations",
        "description": "Tidy data: one row per country or area, year, indicator and dimension combination.",
    },
    {
        "name": "reference",
        "title": "Reference",
        "description": "Country, area and site reference lists used to join sources together.",
    },
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def duckdb_type(t: pa.DataType) -> str:
    """DuckDB spelling of an Arrow type, for ``vgi.result_columns_schema``."""
    simple = {
        pa.string(): "VARCHAR",
        pa.int16(): "SMALLINT",
        pa.int32(): "INTEGER",
        pa.int64(): "BIGINT",
        pa.float64(): "DOUBLE",
        pa.bool_(): "BOOLEAN",
        pa.date32(): "DATE",
    }
    if t in simple:
        return simple[t]
    if pa.types.is_timestamp(t):
        return "TIMESTAMP WITH TIME ZONE" if t.tz else "TIMESTAMP"
    if pa.types.is_list(t):
        return f"{duckdb_type(t.value_type)}[]"
    if pa.types.is_map(t):
        return f"MAP({duckdb_type(t.key_type)}, {duckdb_type(t.item_type)})"
    if pa.types.is_struct(t):
        inner = ", ".join(f"{f.name} {duckdb_type(f.type)}" for f in t)
        return f"STRUCT({inner})"
    raise TypeError(f"no DuckDB name for {t}")


def result_columns(schema: pa.Schema) -> str:
    """``vgi.result_columns_schema`` JSON from an Arrow schema built with `column`."""
    out = [
        {
            "name": f.name,
            "type": duckdb_type(f.type),
            "description": (f.metadata or {}).get(b"comment", b"").decode(),
        }
        for f in schema
    ]
    return json.dumps(out, separators=(",", ":"))


def attribution(text: str) -> str:
    """Markdown attribution paragraph appended to schema and function docs."""
    return f"\n\n*Source and terms:* {text}"


CATALOG_COMMENT = (
    "Curated United Nations statistics as tidy country-year tables: SDG indicators, UNESCO education data, the "
    "World Heritage List, World Population Prospects, UNHCR refugee figures and WHO health indicators."
)

CATALOG_DOC_LLM = (
    "United Nations data, one schema per source, each read live from that organisation's own API: `sdg` (UN "
    "Statistics Division SDG Global Database: goals, targets, indicators, series, observations), `uis` "
    "(UNESCO Institute for Statistics: about 5,500 education, science, culture and communication indicators), "
    "`heritage` (UNESCO World Heritage List sites), `population` (UN DESA World Population Prospects; data "
    "needs a bearer token in a `un_population` secret), `refugees` (UNHCR forcibly displaced populations, "
    "asylum applications and decisions, durable solutions) and `health` (WHO Global Health Observatory). "
    "Observation tables are tidy: one row per area, year, indicator and dimension combination with `value` "
    "`DOUBLE`, `unit` and dimension columns. Countries are identified by `country_iso3` (ISO 3166-1 alpha-3, "
    "NULL for regions and aggregates) everywhere, plus each source's own area code (`m49` 3-digit text for "
    "SDG). `main.m49_countries` is the bundled UN M49 table mapping M49 codes to ISO alpha-2/alpha-3 and regions, and "
    "`main.iso3_from_m49` / `main.m49_from_iso3` convert codes. Filters on indicator/series codes, countries "
    "and year ranges are pushed into each API, so always filter observations by country and year where you "
    "can. Only `population` data needs a key; everything else is keyless."
)

CATALOG_DOC_MD = (
    "# United Nations data\n\n"
    "**Curated UN statistics as tidy `(country, year, indicator, value)` tables in DuckDB.** Each schema "
    "talks to one UN system portal's own JSON API and pushes indicator, country and year filters into it:\n\n"
    "| Schema | Source |\n|---|---|\n"
    "| `sdg` | [UN SDG Global Database](https://unstats.un.org/sdgs/dataportal) (UN Statistics Division) |\n"
    "| `uis` | [UNESCO Institute for Statistics](https://databrowser.uis.unesco.org/) data API |\n"
    "| `heritage` | [UNESCO World Heritage List](https://data.unesco.org/explore/dataset/whc001/) |\n"
    "| `population` | [UN DESA World Population Prospects](https://population.un.org/dataportal/) |\n"
    "| `refugees` | [UNHCR Refugee Population Statistics](https://www.unhcr.org/refugee-statistics/) |\n"
    "| `health` | [WHO Global Health Observatory](https://www.who.int/data/gho) |\n\n"
    "Countries are joined on ISO 3166-1 alpha-3 codes (`country_iso3`); the bundled "
    "[UN M49 standard](https://unstats.un.org/unsd/methodology/m49/overview/) maps M49 numeric codes. "
    "Data belongs to the publishing organisations and is used under their terms: UNdata terms (SDG, M49), "
    "CC BY-SA 4.0 (UIS, World Heritage), CC BY 3.0 IGO (WPP), CC BY 4.0 (UNHCR) and the WHO data terms. "
    "This worker is not affiliated with or endorsed by the United Nations."
)

MAIN_COMMENT = "The bundled UN M49 country and area code standard, with code conversion helpers."

MAIN_DOC_LLM = (
    "The `main` schema holds reference data shared by every source: the UN M49 standard (country or area "
    "name, 3-digit M49 code, ISO 3166-1 alpha-2 and alpha-3 codes, region / sub-region / intermediate region "
    "codes and names, and LDC / LLDC / SIDS flags), bundled with the worker so it needs no network. Use it "
    "to turn SDG M49 area codes into ISO alpha-3 codes, to group countries by region, or to filter "
    "least developed countries. M49 codes are `VARCHAR` with leading zeros ('004' Afghanistan)."
)

MAIN_DOC_MD = (
    "## main\n\n"
    "The [UN M49 standard](https://unstats.un.org/unsd/methodology/m49/overview/) of country and area codes "
    "for statistical use, bundled, plus scalar converters between M49 and ISO alpha-3 codes."
    + attribution("United Nations Statistics Division, UNdata terms of use (free reuse with citation).")
)

AGENT_TEST_TASKS = [
    {
        "name": "ldc_count_africa",
        "prompt": "Using the un worker, how many least developed countries (LDCs) are in Africa according to M49?",
    },
    {"name": "m49_for_india", "prompt": "Using the un worker, what is the UN M49 code of India (ISO code IND)?"},
    {
        "name": "sdg_goal_count",
        "prompt": "Using the un worker, how many Sustainable Development Goals does the SDG database list?",
    },
    {
        "name": "india_poverty_2022",
        "prompt": (
            "Using the un worker's SDG data, what share of India's population lived below the international "
            "poverty line in 2022 (series SI_POV_DAY1, all ages, both sexes, all areas)?"
        ),
    },
    {
        "name": "syrian_refugees_germany",
        "prompt": (
            "Using the un worker's UNHCR data, how many refugees from Syria were in Germany at the end of 2023?"
        ),
    },
    {
        "name": "who_life_expectancy",
        "prompt": (
            "Using the un worker's WHO data, what was life expectancy at birth for both sexes in India in "
            "2019 (indicator WHOSIS_000001)?"
        ),
    },
    {
        "name": "heritage_in_danger",
        "prompt": "Using the un worker, how many World Heritage sites are currently on the List in Danger?",
    },
    {
        "name": "uis_indicator_theme",
        "prompt": "Using the un worker, how many UNESCO UIS indicators belong to the CULTURE theme?",
    },
    {"name": "iso3_for_m49_code", "prompt": "Using the un worker, which ISO alpha-3 code does UN M49 code 4 map to?"},
    {"name": "sdg_goal1_targets", "prompt": "Using the un worker, how many targets does SDG goal 1 have?"},
    {
        "name": "sdg_indicator_series",
        "prompt": "Using the un worker, which SDG data series measure indicator 1.1.1?",
    },
    {
        "name": "sdg_poverty_series_code",
        "prompt": (
            "Using the un worker, what is the SDG series code for the proportion of the population below the "
            "international poverty line?"
        ),
    },
    {
        "name": "sdg_country_areas",
        "prompt": "Using the un worker, how many SDG geographic areas are countries with an ISO alpha-3 code?",
    },
    {
        "name": "uis_sdg_regions",
        "prompt": "Using the un worker, how many UIS regional geo units belong to the SDG region grouping?",
    },
    {"name": "uis_default_version", "prompt": "Using the un worker, what is the current default UIS data version?"},
    {
        "name": "uis_completion_brazil",
        "prompt": (
            "Using the un worker, what was the primary completion rate (UIS indicator CR.1) in Brazil in each "
            "year from 2015 to 2020?"
        ),
    },
    {"name": "heritage_syria", "prompt": "Using the un worker, how many World Heritage sites does Syria have?"},
    {
        "name": "wpp_fertility_indicators",
        "prompt": (
            "Using the un worker, how many World Population Prospects data portal indicators are about Fertility?"
        ),
    },
    {
        "name": "wpp_india_location",
        "prompt": "Using the un worker, what is India's location id in the WPP data portal?",
    },
    {
        "name": "wpp_india_population",
        "prompt": (
            "Using the un worker (a un_population secret is configured), what is India's total population for "
            "both sexes in 2025 according to World Population Prospects?"
        ),
    },
    {
        "name": "syria_germany_applications",
        "prompt": "Using the un worker, how many asylum applications did Syrians lodge in Germany in 2023?",
    },
    {
        "name": "syria_germany_recognized",
        "prompt": (
            "Using the un worker, how many Syrian asylum applicants were recognized as refugees in Germany in 2023?"
        ),
    },
    {
        "name": "syria_returns_2023",
        "prompt": "Using the un worker, from which country did the most Syrian refugees return home in 2023?",
    },
    {"name": "unhcr_code_algeria", "prompt": "Using the un worker, what is UNHCR's country code for Algeria?"},
    {
        "name": "who_life_expectancy_code",
        "prompt": "Using the un worker, which WHO GHO indicator code is life expectancy at birth?",
    },
    {
        "name": "who_sear_countries",
        "prompt": "Using the un worker, how many countries does the WHO GHO list in the South-East Asia region?",
    },
    {"name": "who_sex_codes", "prompt": "Using the un worker, what codes does the WHO GHO use for the SEX dimension?"},
    {"name": "who_age_dimensions", "prompt": "Using the un worker, which WHO GHO dimensions are about age?"},
]

CATALOG_TAGS = {
    "vgi.title": "United Nations Data",
    "vgi.subtitle": "UN SDG, UNESCO, World Heritage, World Population Prospects, UNHCR and WHO data as tidy tables.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "united nations, un, sdg, sustainable development goals, unesco, world heritage, population, "
        "refugees, unhcr, who, health, m49, iso 3166, country statistics, indicators"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    # Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
