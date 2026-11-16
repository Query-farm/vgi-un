# Data sources

Upstream data licences are separate from the code licence (0BSD). Every source below was
checked on 2026-11-15 with the User-Agent `vgi-un/0.1 (+https://query.farm)`, using
about 85 polite requests in total (exploration, fixture recording and one `make live-check`) (at most 1 per second, 2.2 s apart for the WPP
portal). This worker is not affiliated with or endorsed by the United Nations or any of its
agencies.

## Fetched at query time (never bundled)

| Schema | API | Licence / terms | Notes |
|---|---|---|---|
| `sdg` | UN Statistics Division SDG Global Database, `https://unstats.un.org/sdgapi/v1/sdg/...` ([swagger](https://unstats.un.org/sdgapi/swagger/)) | [UNdata terms of use](https://data.un.org/Host.aspx?Content=UNdataUse): "may be copied freely, duplicated and further distributed provided that UNdata is cited as the reference" | Keyless. `Series/Data` paged 1,000 per page. `timePeriodStart`/`timePeriodEnd` returned 0 rows for ranges that do have data, so year ranges are pushed as repeated `timePeriod=` values instead. |
| `uis` | UNESCO Institute for Statistics, `https://api.uis.unesco.org/api/public/...` ([docs](https://api.uis.unesco.org/api/public/documentation/)) | **CC BY-SA 4.0** (per the API's own OpenAPI description) | Keyless, no rate limit published. A data request over 100,000 records is refused with HTTP 400 `Too much data requested (N records)`; the worker plans and splits requests. Share-alike: not bundled; fixtures are synthetic. |
| `heritage` | UNESCO open-data portal, dataset `whc001` (World Heritage List), `https://data.unesco.org/api/explore/v2.1/catalog/datasets/whc001/records` | **CC BY-SA 4.0** (dataset metadata), publisher UNESCO | `whc.unesco.org/en/list/xml/` and `/en/syndication/` answer every non-browser client with a Cloudflare JavaScript challenge (HTTP 403), so the official open-data copy is used instead (1,273 records on 2026-11-15). Share-alike: not bundled; fixtures are synthetic. |
| `population` | UN DESA Population Division Data Portal, `https://population.un.org/dataportalapi/api/v1/...` ([swagger](https://population.un.org/dataportalapi/index.html)) | **CC BY 3.0 IGO** (swagger `info.license`) | `indicators`, `locations` are keyless. `data/...` endpoints return HTTP 401 without a bearer token (secret `un_population`). Limit: 5 requests / 10 s per IP. |
| `refugees` | UNHCR Refugee Population Statistics, `https://api.unhcr.org/population/v1/...` ([docs](https://api.unhcr.org/docs/refugee-statistics.html)) | **CC BY 4.0** ([UNHCR terms of use for datasets](https://www.unhcr.org/terms-use-datasets)); cite "UNHCR Refugee Population Statistics Database" | Keyless. `limit=1000` is honoured. `cf_type=ISO` makes `coo`/`coa` accept ISO alpha-3. |
| `health` | WHO Global Health Observatory OData, `https://ghoapi.azureedge.net/api/...` ([docs](https://www.who.int/data/gho/info/gho-odata-api)) | [WHO terms and conditions for datasets](https://www.who.int/about/policies/publishing/data-policy/terms-and-conditions) (attribution "WHO, title, year, date of access"; one WHO page states CC BY 4.0 plus a mandatory addition, the data-policy page limits use to public-health purposes and forbids resale) | Keyless. WHO announced this OData API would be deprecated "near the end of 2025"; it still answered on 2026-11-15. Because the terms are not a plain open licence, nothing is bundled and fixtures are synthetic. |

## Bundled in `src/un_worker/data/`

| File | Source | Licence | Retrieved | SHA-256 |
|---|---|---|---|---|
| `m49.csv` (248 rows, 23,776 bytes) | Parsed from the English table (`#downloadTableEN`) of https://unstats.un.org/unsd/methodology/m49/overview/ — UN Statistics Division, *Standard country or area codes for statistical use (M49)* | UNdata terms of use (free copying and redistribution with citation). A factual code list. | 2026-11-15 | `33b5c94506aced449ce9690ffe4e43c33e37da95b35bd5cb2eb5096b05ee2ec1` (source page `b9048114f6e7f2abda83bf03d4263c9d7cd1bd7230e3d0461025ee7839a7a1fb`) |

The LDC / LLDC / SIDS columns were blank or `x` in the source table and are stored as
`true`/`false`. Every column is text; Namibia's alpha-2 code is the string `NA`.

## Test fixtures (`test/fixtures/`)

Recorded with `test/fixtures/record.py` (2026-11-15; each source's `MANIFEST.tsv` lists the
requests) — redistributable licences only:

- `sdg/` — UN SDG Global Database (UNdata terms): goal, target, indicator, series and area
  lists, and `SI_POV_DAY1` observations for India and Bangladesh.
- `refugees/` — UNHCR (CC BY 4.0): population, asylum applications/decisions and solutions
  for Syrians in 2023, and the country list.
- `population/indicators`, `population/locations` — WPP Data Portal (CC BY 3.0 IGO).

Written by `test/fixtures/make_synthetic.py` — **made-up records**, shaped like the real
responses but not real data:

- `uis/` (CC BY-SA source), `heritage/` (CC BY-SA source), `health/` (WHO terms), and
  `population/data/...` (the token-gated endpoint could not be recorded without a token; its
  shape follows the swagger `MainDataSetFullDto` schema).
