"""Write the hand-written (synthetic) fixtures for the sources we may not redistribute.

    uv run python test/fixtures/make_synthetic.py

UIS and the World Heritage List are CC BY-SA 4.0, WHO data comes with its own
terms, and WPP data needs a token we do not have, so their fixtures are written
here: the JSON *shapes* follow each API's documentation and the live responses
we inspected, but every record, name and number below is made up for tests and
must not be read as real data. Codes (ISO, indicator codes) are real so joins work.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from un_worker.common import fixture_relpath

HERE = Path(__file__).resolve().parent
VERSION = "20261110-fixture"


def put(source: str, path: str, params: list[tuple[str, Any]], body: Any, status: int = 200) -> None:
    """Write one fixture where the worker will look for it."""
    target = HERE / fixture_relpath(source, path, params)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(body, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    status_file = target.with_suffix(".status")
    if status != 200:
        status_file.write_text(f"{status}\n")
    elif status_file.exists():
        status_file.unlink()


# ---------------------------------------------------------------------------- UIS


def uis() -> None:
    """UNESCO UIS: versions, definitions, data (incl. the 100,000-record refusal)."""
    themes = [
        {"theme": "EDUCATION", "lastUpdate": "12/11/2026", "description": "Fixture release"},
        {"theme": "CULTURE", "lastUpdate": "02/09/2026", "description": "Fixture release"},
    ]
    default = {"version": VERSION, "publicationDate": "2026-11-12T17:04:52.832Z", "themeDataStatus": themes}
    put("uis", "versions/default", [], default)
    put(
        "uis",
        "versions",
        [],
        [
            default,
            {"version": "20260507-fixture", "publicationDate": "2026-05-08T16:58:36.233Z", "themeDataStatus": themes},
        ],
    )

    def ind(code: str, name: str, theme: str, n: int, lo: int = 1970, hi: int = 2025) -> dict[str, Any]:
        return {
            "indicatorCode": code,
            "name": name,
            "theme": theme,
            "lastDataUpdate": "12/11/2026",
            "lastDataUpdateDescription": "Fixture release",
            "dataAvailability": {
                "totalRecordCount": n,
                "timeLine": {"min": lo, "max": hi},
                "geoUnits": {"types": ["NATIONAL"]},
            },
        }

    put(
        "uis",
        "definitions/indicators",
        [("disaggregations", "false"), ("glossaryTerms", "false")],
        [
            ind("CR.1", "Completion rate, primary education, both sexes (%)", "EDUCATION", 4200, 1990),
            ind("CR.2", "Completion rate, lower secondary education, both sexes (%)", "EDUCATION", 4100, 1990),
            ind("LR.AG15T99", "Adult literacy rate, population 15+ years, both sexes (%)", "EDUCATION", 3900),
            ind(
                "ROFST.1.CP", "Out-of-school rate for children of primary school age, both sexes (%)", "EDUCATION", 8800
            ),
            ind("XGDP.FSGOV", "Government expenditure on education as a percentage of GDP (%)", "EDUCATION", 6100),
            ind("GERD.GDP", "GERD as a percentage of GDP", "SCIENCE_TECHNOLOGY_INNOVATION", 2600, 1996),
            ind("CUL.HER.1", "Number of cultural heritage sites (fixture)", "CULTURE", 900, 2000),
            ind("CUL.HER.2", "Number of natural heritage sites (fixture)", "CULTURE", 900, 2000),
        ],
    )
    put(
        "uis",
        "definitions/geounits",
        [],
        [
            {"id": "BRA", "name": "Brazil", "type": "NATIONAL"},
            {"id": "IND", "name": "India", "type": "NATIONAL"},
            {"id": "USA", "name": "United States of America", "type": "NATIONAL"},
            {"id": "SDG: Sub-Saharan Africa", "name": "Sub-Saharan Africa", "type": "REGIONAL", "regionGroup": "SDG"},
            {"id": "SDG: World", "name": "World", "type": "REGIONAL", "regionGroup": "SDG"},
            {"id": "WB: High income", "name": "High income", "type": "REGIONAL", "regionGroup": "WB"},
        ],
    )

    def rec(code: str, geo: str, year: int, value: float | None, **extra: Any) -> dict[str, Any]:
        return {
            "indicatorId": code,
            "geoUnit": geo,
            "year": year,
            "value": value,
            "magnitude": extra.get("magnitude"),
            "qualifier": extra.get("qualifier"),
            "footnotes": extra.get("footnotes", []),
        }

    src = [{"type": "Source", "subtype": "Data sources", "value": "Fixture household survey"}]
    tail = [("footnotes", "true"), ("indicatorMetadata", "false"), ("version", VERSION)]
    cr1 = [rec("CR.1", "BRA", y, 92.0 + 0.7 * (y - 2015), footnotes=src) for y in range(2015, 2021)]
    cr1 += [rec("CR.1", "USA", y, 99.0 + 0.1 * (y - 2015), qualifier="UIS_EST") for y in range(2015, 2021)]
    put(
        "uis",
        "data/indicators",
        [("indicator", "CR.1"), ("geoUnit", "BRA"), ("geoUnit", "USA"), ("start", 2015), ("end", 2020), *tail],
        {"hints": [], "records": cr1, "indicatorMetadata": []},
    )
    bra = [
        rec("CR.1", "BRA", 2020, 96.3),
        rec("CR.2", "BRA", 2020, 88.4),
        rec("LR.AG15T99", "BRA", 2020, 93.2),
        rec("XGDP.FSGOV", "BRA", 2020, 5.8),
        rec("CUL.HER.1", "BRA", 2020, None, magnitude="NA"),
    ]
    put(
        "uis",
        "data/indicators",
        [("geoUnit", "BRA"), ("start", 2020), ("end", 2020), *tail],
        {"hints": [], "records": bra, "indicatorMetadata": []},
    )
    # geo_unit := 'IND' with no year: refused (210,000 records) and split into 3 year windows.
    too_much = (
        "Too much data requested ({n} records), please reduce the amount of records queried to less than "
        "100000 by using the available filters."
    )
    put(
        "uis",
        "data/indicators",
        [("geoUnit", "IND"), *tail],
        {"message": too_much.format(n=210000), "error": "Bad Request", "statusCode": 400},
        status=400,
    )
    for lo, hi, value in ((1970, 1988, 40.0), (1989, 2007, 60.0), (2008, 2025, 75.0)):
        put(
            "uis",
            "data/indicators",
            [("geoUnit", "IND"), ("start", lo), ("end", hi), *tail],
            {"hints": [], "records": [rec("LR.AG15T99", "IND", lo, value)], "indicatorMetadata": []},
        )
    # A single geo unit and year that is still too big cannot be split: a clear error.
    put(
        "uis",
        "data/indicators",
        [("geoUnit", "IND"), ("start", 1999), ("end", 1999), *tail],
        {"message": too_much.format(n=120000), "error": "Bad Request", "statusCode": 400},
        status=400,
    )


# ------------------------------------------------------------------------ heritage

STATES = [
    ("SY", "Syrian Arab Republic", "Arab States", "ARB", 34.8, 38.9),
    ("FR", "France", "Europe and North America", "EUR", 46.6, 2.4),
    ("IN", "India", "Asia and the Pacific", "APA", 22.9, 79.6),
    ("BR", "Brazil", "Latin America and the Caribbean", "LAC", -10.8, -52.9),
    ("KE", "Kenya", "Africa", "AFR", 0.2, 37.9),
]
CRITERIA = {"Cultural": "(ii)(iv)", "Natural": "(vii)(ix)", "Mixed": "(iii)(vii)"}


def heritage_sites() -> list[dict[str, Any]]:
    """120 made-up sites: deterministic category / danger / country patterns."""
    sites = []
    for n in range(1, 121):
        iso, state, region, region_code, lat, lon = STATES[n % len(STATES)]
        category = ("Cultural", "Cultural", "Natural", "Cultural", "Mixed")[n % 5]
        danger = n % 12 == 0 or (iso == "SY" and n % 3 == 0)
        transboundary = n == 61
        year = 1978 + n % 45
        sites.append(
            {
                "id_no": str(1000 + n),
                "uuid": f"00000000-0000-4000-8000-{n:012d}",
                "name_en": f"Fixture Site {n:03d}",
                "short_description_en": f"A made-up {category.lower()} property used by the vgi-un tests.",
                "date_inscribed": str(year),
                "secondary_dates": f"{year}, {year + 7}" if n % 10 == 0 else str(year),
                "danger": "True" if danger else "False",
                "date_end": None,
                "danger_list": f"Y {year + 20}" if danger else None,
                "area_hectares": float(n * 37 % 5000) + 0.5,
                "criteria_txt": CRITERIA[category],
                "category": category,
                "states_names": [state, "Kenya"] if transboundary else [state],
                "iso_codes": [iso, "KE"] if transboundary else [iso],
                "region": region,
                "region_code": region_code,
                "transboundary": "True" if transboundary else "False",
                "geo_point_2d": {"lon": lon + n / 100, "lat": lat + n / 200},
                "components_count": 1 + n % 3,
            }
        )
    return sites


def heritage() -> None:
    """ODS records responses for the queries the tests and examples run."""
    from un_worker.heritage import FIELDS, PAGE_SIZE

    sites = sorted(heritage_sites(), key=lambda s: s["id_no"])

    def pages(where: str | None, rows: list[dict[str, Any]]) -> None:
        for offset in range(0, max(len(rows), 1), PAGE_SIZE):
            params: list[tuple[str, Any]] = [
                ("select", FIELDS),
                ("order_by", "id_no"),
                ("limit", PAGE_SIZE),
                ("offset", offset),
            ]
            if where:
                params.append(("where", where))
            put("heritage", "records", params, {"total_count": len(rows), "results": rows[offset : offset + PAGE_SIZE]})

    pages(None, sites)
    pages('danger = "True"', [s for s in sites if s["danger"] == "True"])
    pages('iso_codes = "SY"', [s for s in sites if "SY" in s["iso_codes"]])
    pages('(category = "Natural")', [s for s in sites if s["category"] == "Natural"])


# -------------------------------------------------------------------------- health


def health() -> None:
    """WHO GHO OData responses."""
    ctx = "https://ghoapi.azureedge.net/api/$metadata"
    put(
        "health",
        "Indicator",
        [],
        {
            "@odata.context": ctx,
            "value": [
                {
                    "IndicatorCode": "WHOSIS_000001",
                    "IndicatorName": "Life expectancy at birth (years)",
                    "Language": "EN",
                },
                {
                    "IndicatorCode": "WHOSIS_000015",
                    "IndicatorName": "Life expectancy at age 60 (years)",
                    "Language": "EN",
                },
                {
                    "IndicatorCode": "WHOSIS_000002",
                    "IndicatorName": "Healthy life expectancy (HALE) at birth (years)",
                    "Language": "EN",
                },
                {
                    "IndicatorCode": "MDG_0000000001",
                    "IndicatorName": "Infant mortality rate (per 1000 live births)",
                    "Language": "EN",
                },
                {
                    "IndicatorCode": "NCD_BMI_30A",
                    "IndicatorName": "Prevalence of obesity among adults (%)",
                    "Language": "EN",
                },
            ],
        },
    )
    put(
        "health",
        "Dimension",
        [],
        {
            "@odata.context": ctx,
            "value": [
                {"Code": "AGEGROUP", "Title": "Age Group"},
                {"Code": "COUNTRY", "Title": "Country"},
                {"Code": "REGION", "Title": "WHO region"},
                {"Code": "SEX", "Title": "Sex"},
            ],
        },
    )
    put(
        "health",
        "DIMENSION/SEX/DimensionValues",
        [],
        {
            "@odata.context": ctx,
            "value": [
                {
                    "Code": "SEX_BTSX",
                    "Title": "Both sexes",
                    "ParentDimension": "",
                    "Dimension": "SEX",
                    "ParentCode": "",
                    "ParentTitle": "",
                },
                {
                    "Code": "SEX_FMLE",
                    "Title": "Female",
                    "ParentDimension": "",
                    "Dimension": "SEX",
                    "ParentCode": "",
                    "ParentTitle": "",
                },
                {
                    "Code": "SEX_MLE",
                    "Title": "Male",
                    "ParentDimension": "",
                    "Dimension": "SEX",
                    "ParentCode": "",
                    "ParentTitle": "",
                },
            ],
        },
    )
    countries = [
        ("BGD", "Bangladesh", "SEAR", "South-East Asia"),
        ("BRA", "Brazil", "AMR", "Americas"),
        ("IND", "India", "SEAR", "South-East Asia"),
        ("KEN", "Kenya", "AFR", "Africa"),
        ("PAK", "Pakistan", "EMR", "Eastern Mediterranean"),
        ("FRA", "France", "EUR", "Europe"),
    ]
    put(
        "health",
        "DIMENSION/COUNTRY/DimensionValues",
        [],
        {
            "@odata.context": ctx,
            "value": [
                {
                    "Code": c,
                    "Title": t,
                    "ParentDimension": "REGION",
                    "Dimension": "COUNTRY",
                    "ParentCode": rc,
                    "ParentTitle": rt,
                }
                for c, t, rc, rt in countries
            ],
        },
    )
    region = {c: (rc, rt) for c, _, rc, rt in countries}
    base = {"BGD": 71.0, "IND": 69.0, "PAK": 65.0}
    offset = {"SEX_BTSX": 0.0, "SEX_FMLE": 1.5, "SEX_MLE": -1.5}

    def obs(country: str, year: int, sex: str, n: int) -> dict[str, Any]:
        value = round(base[country] + 0.3 * (year - 2015) + offset[sex], 1)
        return {
            "Id": n,
            "IndicatorCode": "WHOSIS_000001",
            "SpatialDimType": "COUNTRY",
            "SpatialDim": country,
            "TimeDimType": "YEAR",
            "ParentLocationCode": region[country][0],
            "ParentLocation": region[country][1],
            "Dim1Type": "SEX",
            "TimeDim": year,
            "Dim1": sex,
            "Dim2Type": None,
            "Dim2": None,
            "Dim3Type": None,
            "Dim3": None,
            "DataSourceDimType": None,
            "DataSourceDim": None,
            "Value": f"{value} [{value - 1.2:.1f}-{value + 1.2:.1f}]",
            "NumericValue": value,
            "Low": round(value - 1.2, 1),
            "High": round(value + 1.2, 1),
            "Comments": None,
            "Date": "2026-10-01T16:11:13.08+02:00",
            "TimeDimensionValue": str(year),
            "TimeDimensionBegin": f"{year}-01-01T00:00:00+01:00",
            "TimeDimensionEnd": f"{year}-12-31T00:00:00+01:00",
        }

    india = [obs("IND", y, s, i) for i, (y, s) in enumerate((y, s) for y in range(2015, 2022) for s in offset)]
    put(
        "health",
        "WHOSIS_000001",
        [("$filter", "(SpatialDim eq 'IND') and TimeDim ge 2015 and TimeDim le 2021")],
        {"@odata.context": ctx, "value": india},
    )
    south_asia = [
        obs(c, 2019, s, 100 + i) for i, (c, s) in enumerate((c, s) for c in ("BGD", "IND", "PAK") for s in offset)
    ]
    put(
        "health",
        "WHOSIS_000001",
        [
            (
                "$filter",
                "(SpatialDim eq 'BGD' or SpatialDim eq 'IND' or SpatialDim eq 'PAK') "
                "and TimeDim ge 2019 and TimeDim le 2019",
            )
        ],
        {"@odata.context": ctx, "value": south_asia},
    )
    put(
        "health",
        "WHOSIS_000001",
        [("$filter", "(SpatialDim eq 'IND') and TimeDim ge 2019 and TimeDim le 2019")],
        {"@odata.context": ctx, "value": [r for r in india if r["TimeDim"] == 2019]},
    )
    # Paging: Pakistan 2019 split over two OData pages via @odata.nextLink.
    pak = [obs("PAK", 2019, s, 200 + i) for i, s in enumerate(offset)]
    flt = "(SpatialDim eq 'PAK') and TimeDim ge 2019 and TimeDim le 2019"
    put(
        "health",
        "WHOSIS_000001",
        [("$filter", "(SpatialDim eq 'PAK') and TimeDim ge 2019 and TimeDim le 2019")],
        {
            "@odata.context": ctx,
            "value": pak[:2],
            "@odata.nextLink": "https://ghoapi.azureedge.net/api/WHOSIS_000001?$filter="
            + flt.replace(" ", "%20")
            + "&$skip=2",
        },
    )
    put("health", "WHOSIS_000001", [("$filter", flt), ("$skip", 2)], {"@odata.context": ctx, "value": pak[2:]})


# ---------------------------------------------------------------------- population


def population() -> None:
    """WPP data (token-gated, so never recorded): shaped like MainDataSetFullDto."""
    rows = []
    for year in range(2020, 2026):
        for sex_id, sex, share in ((3, "Both sexes", 1.0), (2, "Female", 0.485), (1, "Male", 0.515)):
            rows.append(
                {
                    "locationId": 356,
                    "location": "India",
                    "iso3": "IND",
                    "iso2": "IN",
                    "locationTypeId": 4,
                    "indicatorId": 49,
                    "indicator": "TPopulation",
                    "indicatorDisplayName": "Total population",
                    "sourceId": 1,
                    "source": "World Population Prospects 2024 (fixture)",
                    "revision": 2024,
                    "variantId": 4 if year >= 2024 else 1,
                    "variant": "Median" if year >= 2024 else "Estimates",
                    "variantShortName": "Median" if year >= 2024 else "Estimates",
                    "variantLabel": "Median" if year >= 2024 else "Estimates",
                    "timeId": year - 1949,
                    "timeLabel": str(year),
                    "timeMid": f"{year}.5",
                    "categoryId": 0,
                    "category": "Not applicable",
                    "estimateTypeId": 0,
                    "estimateType": "Model-based Estimates",
                    "estimateMethodId": 0,
                    "estimateMethod": "Interpolation",
                    "sexId": sex_id,
                    "sex": sex,
                    "ageId": 188,
                    "ageLabel": "Total",
                    "ageStart": 0,
                    "ageEnd": None,
                    "ageMid": None,
                    "value": round((1_400_000 + 9_000 * (year - 2020)) * share, 1),
                }
            )
    put(
        "population",
        "data/indicators/49/locations/356/start/2020/end/2025",
        [("pageNumber", 1), ("pageSize", 100)],
        {
            "pageNumber": 1,
            "pageSize": 100,
            "previousPage": None,
            "nextPage": None,
            "pages": 1,
            "total": len(rows),
            "data": rows,
        },
    )


def main() -> None:
    """Write every synthetic fixture."""
    uis()
    heritage()
    health()
    population()


if __name__ == "__main__":
    main()
