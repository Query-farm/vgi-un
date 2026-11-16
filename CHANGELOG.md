# Changelog

## 0.1.0 (unreleased)

First release of vgi-un: curated UN data as tidy DuckDB tables.

- `sdg`: UN SDG Global Database goals, targets, indicators, series, areas and observations.
- `uis`: UNESCO UIS indicators, geo units, versions and observations (100,000-record limit handled).
- `heritage`: UNESCO World Heritage List sites.
- `population`: UN DESA WPP indicators/locations (keyless) and data (`un_population` secret).
- `refugees`: UNHCR populations, asylum applications/decisions, solutions, countries.
- `health`: WHO GHO indicators, countries, dimensions and observations.
- `main`: bundled UN M49 table and M49 <-> ISO alpha-3 converters.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
