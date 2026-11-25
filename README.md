<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# United Nations SDG, UNESCO, Population, Refugee and Health Statistics in DuckDB

> **vgi-un** · a [Query.Farm](https://query.farm) VGI worker

[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Curated United Nations data as tidy `(country, year, indicator, value)` tables. Each schema
talks to one UN-system portal's own JSON API, and pushes indicator, country and year filters
into it:

| Schema | Source | Key? |
|---|---|---|
| `sdg` | [UN SDG Global Database](https://unstats.un.org/sdgs/dataportal) (UN Statistics Division) | no |
| `uis` | [UNESCO Institute for Statistics](https://databrowser.uis.unesco.org/) data API | no |
| `heritage` | [UNESCO World Heritage List](https://whc.unesco.org/en/list/) (UNESCO open-data copy) | no |
| `population` | [UN DESA World Population Prospects](https://population.un.org/wpp/) Data Portal | data only: `un_population` secret |
| `refugees` | [UNHCR Refugee Population Statistics](https://www.unhcr.org/refugee-statistics/) | no |
| `health` | [WHO Global Health Observatory](https://www.who.int/data/gho) (OData) | no |
| `main` | bundled [UN M49](https://unstats.un.org/unsd/methodology/m49/overview/) country codes | — |

Countries join on **ISO 3166-1 alpha-3** (`country_iso3`) everywhere, plus each source's own
area code (`m49` 3-digit text for SDG, UNHCR codes, WPP location ids).

```sql
-- Extreme poverty in India over time (SDG 1.1.1)
SELECT year, value FROM un.sdg.data('SI_POV_DAY1')
WHERE country_iso3 = 'IND' AND sex = 'BOTHSEX' AND age = 'ALLAGE' AND location = 'ALLAREA'
ORDER BY year;

-- Where Syrian refugees lived at the end of 2023
SELECT asylum_name, value FROM un.refugees.populations
WHERE origin_iso3 = 'SYR' AND year = 2023 AND measure = 'refugees'
ORDER BY value DESC LIMIT 5;

-- Life expectancy by WHO region, joined through the bundled M49 table
SELECT m.region_name, round(avg(d.value), 1) AS life_expectancy
FROM un.health.data('WHOSIS_000001') d JOIN un.main.m49_countries m ON m.iso_alpha3 = d.country_iso3
WHERE d.year = 2019 AND d.sex = 'SEX_BTSX' GROUP BY ALL ORDER BY ALL;
```

## Install and attach

Requires [uv](https://docs.astral.sh/uv/) and DuckDB 2.0 (or [Haybarn](https://query.farm/products/haybarn) 2.0).

vgi-un 0.2.0 speaks VGI protocol 3.4, so it needs Haybarn 2.0.1 or newer (Haybarn 2.0.0 loads a 3.3 `vgi` extension that cannot attach it).

```sql
-- DuckDB 2.0
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;

ATTACH 'un' (TYPE vgi, LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-un@v0.2.0 vgi-un-worker');
```

From a checkout: `make build`, then
`ATTACH 'un' (TYPE vgi, LOCATION '/abs/path/to/vgi-un/.venv/bin/vgi-un-worker')`.

### ATTACH options

| Option | Default | Meaning |
|---|---|---|
| `base_url` | `''` (each organisation's public API) | An `http(s)://` mirror or proxy serving `<base_url>/<schema>/<api path>`, or a local directory of recorded responses (see `test/fixtures`), for offline use |
| `user_agent` | `vgi-un/0.1 (+https://query.farm)` | User-Agent sent upstream |

### Secrets

Only `population.data` needs a key. Generate a Data Portal token on
https://population.un.org/dataportalapi/index.html ("GENERATE TOKEN"), then:

```sql
CREATE SECRET (TYPE un_population, TOKEN '...');
SELECT year, value FROM un.population.data('49')
WHERE country_iso3 = 'IND' AND year BETWEEN 2020 AND 2030 AND sex = 'Both sexes';
```

Without it the function fails with a message naming the secret type. The WPP indicator and
location lists are keyless. (UN Comtrade is not included in this release.)

## Functions and tables

| Object | What it returns |
|---|---|
| `main.m49_countries` | The bundled M49 standard: M49, ISO alpha-2/alpha-3, region / sub-region / intermediate region, LDC / LLDC / SIDS |
| `main.iso3_from_m49(code)`, `main.m49_from_iso3(code)` | Code converters (scalars, no network) |
| `sdg.goals`, `sdg.targets`, `sdg.indicators`, `sdg.series`, `sdg.geoareas` | The SDG framework and area list |
| `sdg.data(series)` | Observations of one or more series, with sex/age/location/reporting-type columns plus `dimensions`/`attributes` maps |
| `uis.indicators`, `uis.geo_units`, `uis.versions` | UIS indicator definitions (with record counts), countries/regions, data versions |
| `uis.data(indicator := , geo_unit := , version := )` | Observations; at least one of indicator / geo unit (argument or `WHERE`) |
| `heritage.sites(country := )` | World Heritage properties: category, criteria list, inscription year, danger flag, area, ISO-3 States Parties, lat/lon, URL |
| `population.indicators`, `population.locations` | WPP Data Portal indicators and locations (keyless) |
| `population.data(indicator, location := , variants := )` | WPP estimates and projections (needs the secret) |
| `refugees.populations`, `refugees.asylum_applications`, `refugees.asylum_decisions`, `refugees.solutions` | UNHCR figures in long format: year, origin, asylum country, (procedure dimensions), `measure`, `value` |
| `refugees.countries` | UNHCR <-> ISO country codes |
| `health.indicators`, `health.countries`, `health.dimensions`, `health.dimension_values(dimension)` | GHO indicator, country/WHO-region and dimension code lists |
| `health.data(indicator)` | GHO observations by area, period, sex and other dimensions |

### Pushdown

`WHERE` filters (and the named arguments) are turned into upstream request parameters; every
pushed predicate is also re-applied locally, so results are exact.

| Function | Pushed into the API |
|---|---|
| `sdg.data` | series code (argument); `country_iso3` / `m49` / `area_code` `=` or `IN` -> `areaCode`; bounded `year` ranges (< 100 years) -> repeated `timePeriod` (the API's `timePeriodStart/End` did not filter correctly) |
| `uis.data` | `indicator_code`, `geo_unit`, `country_iso3` `=`/`IN`; `year` bounds -> `start`/`end`; data `version` |
| `heritage.sites` | `country` argument -> `iso_codes`; `category`, `in_danger`, `region`, `year_inscribed` bounds -> ODSQL `where` |
| `population.data` | indicator, `location` / `country_iso3` / `location_id`, `year` bounds -> path `start/end` |
| `refugees.*` | `origin_iso3`, `asylum_iso3` -> `coo`/`coa` (with `cf_type=ISO`); `year` bounds -> `yearFrom`/`yearTo` |
| `health.data` | `country_iso3` / `spatial_dim`, `year` bounds -> OData `$filter` |

Pagination cursors (page numbers, OData `$skip`, UIS request plans) live in the table
function's serialized state, so scans resume correctly over the HTTP transport.

### The UIS 100,000-record limit

The UIS API refuses any request over 100,000 records. `uis.data` batches indicators by their
published record counts, and when the API still refuses a request it splits it (indicator list,
then year range, then geo units) and retries the pieces. If a single indicator/geo unit/year
is still too large, the query fails with an error; it never returns truncated data.

## Data sources and terms

Upstream data belongs to the publishing organisations and comes with their own terms, separate
from this worker's code licence. Details, checks and checksums: [DATA-SOURCES.md](DATA-SOURCES.md).

- **SDG Global Database, M49** — UN Statistics Division, UNdata terms of use: free to copy and
  redistribute with citation. The M49 table is bundled.
- **UIS** and **World Heritage List** — CC BY-SA 4.0. Fetched at query time, never bundled;
  anything you publish from them is an adaptation under the same licence. The World Heritage
  data is read from UNESCO's open-data portal (dataset `whc001`) because whc.unesco.org's own
  exports block non-browser clients.
- **World Population Prospects** — UN DESA Population Division, CC BY 3.0 IGO.
- **UNHCR** — CC BY 4.0; cite "UNHCR Refugee Population Statistics Database".
- **WHO GHO** — WHO terms and conditions for datasets (attribution; public-health use; no
  resale). Fetched at query time, never bundled.

This worker is not affiliated with or endorsed by the United Nations or its agencies.

## Development

```bash
make check                 # ruff, mypy, pydoclint, pytest, SQL tests over stdio/launch/http, vgi-lint
make test-unit
make test-sql TRANSPORT=stdio
```

Tests and the metadata lint run offline: the worker reads `test/fixtures` through its ordinary
`base_url` option. Fixtures from redistributable sources (SDG, UNHCR, WPP lists) are real
responses recorded with `test/fixtures/record.py`; UIS, World Heritage, WHO and the token-gated
WPP data are hand-written by `test/fixtures/make_synthetic.py` — made-up values shaped like the
real APIs. A live check (opt-in, hits the real APIs): `make live-check`.

## Support

Open an issue on [GitHub](https://github.com/Query-farm/vgi-un/issues).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). Data terms are separate; see
above.
