"""Unit tests for request building, response parsing, planning and the HTTP layer."""

from __future__ import annotations

import http.server
import json
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from un_worker import common, health, heritage, refugees, sdg, uis
from un_worker.common import Options, UpstreamError, fixture_relpath, pushed_int_range, pushed_values
from un_worker.countries import iso2_to_iso3_map, iso3_for_m49, known_iso3, m49_table, normalize_m49

FIXTURES = Path(__file__).resolve().parent.parent / "test" / "fixtures"


@dataclass
class Bounds:
    """Stand-in for vgi's ColumnBounds."""

    min_value: Any = None
    min_inclusive: bool = True
    max_value: Any = None
    max_inclusive: bool = True


@dataclass
class FakeFilters:
    """Stand-in for PushdownFilters: equality, IN and bounds per column."""

    eq: dict[str, Any] = field(default_factory=dict)
    isin: dict[str, list[Any]] = field(default_factory=dict)
    bounds: dict[str, Bounds] = field(default_factory=dict)

    def get_column_constant(self, name: str) -> pa.Scalar[Any] | None:
        return pa.scalar(self.eq[name]) if name in self.eq else None

    def get_column_in_values(self, name: str) -> pa.Array[Any] | None:
        return pa.array(self.isin[name]) if name in self.isin else None

    def get_column_bounds(self, name: str) -> Bounds | None:
        return self.bounds.get(name)


def between(lo: int | None, hi: int | None, *, strict: bool = False) -> Bounds:
    return Bounds(
        min_value=None if lo is None else pa.scalar(lo, pa.int32()),
        min_inclusive=not strict,
        max_value=None if hi is None else pa.scalar(hi, pa.int32()),
        max_inclusive=not strict,
    )


# ------------------------------------------------------------------ common


def test_canonical_query_is_order_independent_but_keeps_repeats() -> None:
    a = common.canonical_query([("b", 1), ("a", "x"), ("a", "y")])
    b = common.canonical_query([("a", "x"), ("b", 1), ("a", "y")])
    assert a == b == "a=x&a=y&b=1"
    assert fixture_relpath("sdg", "/v1/sdg/Goal/List", [("q", "1")]).startswith("sdg/v1/sdg/Goal/List/")
    assert fixture_relpath("health", "WHOSIS_000001", [("$filter", "x eq 'y'")]).endswith(".json")


def test_pushdown_helpers() -> None:
    f = FakeFilters(eq={"year": 2020}, isin={"country_iso3": ["IND", None, "BGD"]})
    assert pushed_values(f, "country_iso3") == ["IND", "BGD"]
    assert pushed_int_range(f, "year") == (2020, 2020)
    g = FakeFilters(bounds={"year": between(2000, 2010, strict=True)})
    assert pushed_int_range(g, "year") == (2001, 2009)
    assert pushed_int_range(None, "year") == (None, None)
    assert pushed_values(FakeFilters(), "x") is None


def test_numbers() -> None:
    assert common.to_float("1,234.5") == 1234.5
    assert common.to_float("-") is None
    assert common.to_float(True) is None
    assert common.to_int(2015.0) == 2015


def test_options_modes() -> None:
    assert common.options_from({}).mode == "live"
    assert common.options_from({"base_url": "http://x"}).mode == "mirror"
    assert common.options_from({"base_url": "file:///tmp/f"}).base_url == "/tmp/f"
    assert common.options_from({"base_url": "/tmp/f"}).mode == "fixtures"


def test_missing_fixture_names_the_request(tmp_path: Path) -> None:
    with pytest.raises(UpstreamError, match="no recorded response"):
        common.fetch_json(Options(str(tmp_path), "ua"), "sdg", "v1/nothing")


# ----------------------------------------------------------------- countries


def test_m49_bundle() -> None:
    table = m49_table()
    assert table.num_rows == 248
    namibia = next(r for r in table.to_pylist() if r["country_or_area"] == "Namibia")
    assert namibia["iso_alpha2"] == "NA"  # not parsed as a missing value
    assert iso2_to_iso3_map()["NA"] == "NAM"
    assert normalize_m49("4") == "004"
    assert normalize_m49("1000") is None
    assert iso3_for_m49("4") == "AFG"
    assert iso3_for_m49("150") is None  # Europe, a region
    assert known_iso3("ind") == "IND"
    assert known_iso3("XXX") is None


# ----------------------------------------------------------------------- sdg


def test_sdg_request_pushes_areas_and_bounded_years() -> None:
    f = FakeFilters(isin={"country_iso3": ["IND", "BGD"]}, bounds={"year": between(2004, 2006)})
    params = sdg.data_request("si_pov_day1", f, 2)
    assert ("seriesCode", "SI_POV_DAY1") in params
    assert [v for k, v in params if k == "areaCode"] == ["356", "50"]
    assert [v for k, v in params if k == "timePeriod"] == [2004, 2005, 2006]
    assert ("page", 2) in params
    open_ended = sdg.data_request("X", FakeFilters(bounds={"year": between(2000, None)}), 1)
    assert not [k for k, _ in open_ended if k == "timePeriod"]


def test_sdg_unknown_iso_is_marked() -> None:
    assert sdg.area_codes(FakeFilters(eq={"country_iso3": "ZZZ"})) == ["__none__"]
    assert sdg.area_codes(FakeFilters(eq={"m49": "004"})) == ["4"]


def test_sdg_row_parsing() -> None:
    body = json.loads(
        (
            FIXTURES
            / fixture_relpath(
                "sdg",
                "v1/sdg/Series/Data",
                [("seriesCode", "SI_POV_DAY1"), ("areaCode", "356"), ("pageSize", 1000), ("page", 1)],
            )
        ).read_text()
    )
    row = sdg.data_row(body["data"][0])
    assert row["country_iso3"] == "IND" and row["m49"] == "356" and row["year"] == 1977
    assert row["value"] == 59.7 and row["unit"] == "PERCENT" and row["sex"] == "BOTHSEX"
    assert dict(row["dimensions"])["Reporting Type"] == "G"
    assert sdg.data_row({**body["data"][0], "value": "<1"})["value"] is None


# ----------------------------------------------------------------------- uis


def test_uis_plan_batches_by_record_count() -> None:
    counts = {"A": 60_000, "B": 40_000, "C": 10_000}
    chunks = uis.plan_chunks(["A", "B", "C"], [], None, None, counts)
    assert [c["i"] for c in chunks] == [["A"], ["B", "C"]]
    one = uis.plan_chunks(["A", "B", "C"], ["BRA"], 2000, 2010, counts)
    assert [c["i"] for c in one] == [["A", "B", "C"]] and one[0]["s"] == 2000
    assert uis.plan_chunks([], ["BRA"], None, None, {}) == [{"i": [], "g": ["BRA"], "s": None, "e": None}]


def test_uis_split_order_and_failure() -> None:
    two = uis.split_chunk({"i": ["A", "B"], "g": [], "s": None, "e": None}, 150_000, (1970, 2025))
    assert [c["i"] for c in two] == [["A"], ["B"]]
    years = uis.split_chunk({"i": [], "g": ["IND"], "s": None, "e": None}, 210_000, (1970, 2025))
    assert [(c["s"], c["e"]) for c in years] == [(1970, 1988), (1989, 2007), (2008, 2025)]
    geos = uis.split_chunk({"i": [], "g": ["IND", "USA"], "s": 2000, "e": 2000}, 150_000, (1970, 2025))
    assert [c["g"] for c in geos] == [["IND"], ["USA"]]
    with pytest.raises(UpstreamError, match="cannot be split further"):
        uis.split_chunk({"i": ["A"], "g": ["IND"], "s": 2000, "e": 2000}, 150_000, (1970, 2025))


def test_uis_too_much_message() -> None:
    match = uis.TOO_MUCH.search("Too much data requested (310712 records), please reduce")
    assert match and match.group(1) == "310712"


# -------------------------------------------------------------------- health


def test_health_odata_filter_and_paging() -> None:
    f = FakeFilters(isin={"country_iso3": ["pak", "BGD"]}, bounds={"year": between(2019, 2019)})
    assert (
        health.odata_filter(f) == "(SpatialDim eq 'BGD' or SpatialDim eq 'PAK') and TimeDim ge 2019 and TimeDim le 2019"
    )
    assert health.odata_filter(FakeFilters()) is None
    assert health.next_skip({"@odata.nextLink": "https://x/api/A?$filter=a%20eq%201&$skip=200"}) == 200
    assert health.next_skip({}) == -1


def test_health_row() -> None:
    row = health.data_row(
        {
            "IndicatorCode": "X",
            "SpatialDimType": "REGION",
            "SpatialDim": "SEAR",
            "TimeDimType": "YEAR",
            "TimeDim": 2019,
            "Dim1Type": "AGEGROUP",
            "Dim1": "AGE0-4",
            "Dim2Type": "SEX",
            "Dim2": "SEX_FMLE",
            "NumericValue": 1.5,
            "Date": "2026-10-01T16:11:13.08+02:00",
            "TimeDimensionBegin": "2019-01-01T00:00:00+01:00",
        }
    )
    assert row["country_iso3"] is None and row["sex"] == "SEX_FMLE" and row["year"] == 2019
    assert row["updated_at"].hour == 14 and str(row["period_begin"]) == "2019-01-01"


# ------------------------------------------------------------------ heritage


def test_heritage_where_and_row() -> None:
    f = FakeFilters(
        isin={"category": ["Natural", "Bogus"]}, eq={"in_danger": True}, bounds={"year_inscribed": between(1990, None)}
    )
    where = heritage.where_clause("fra", f)
    assert where == (
        'iso_codes = "FR" and (category = "Natural") and danger = "True" and date_inscribed >= date\'1990-01-01\''
    )
    assert heritage.where_clause("", None) is None
    row = heritage.site_row(
        {
            "id_no": "20",
            "name_en": "X",
            "criteria_txt": "(i)(ii)(iv)",
            "date_inscribed": "1979",
            "secondary_dates": "1979, 2011",
            "danger": "True",
            "iso_codes": ["sy", "lb"],
            "transboundary": "True",
            "geo_point_2d": {"lat": 1.5, "lon": 2.5},
        }
    )
    assert row["criteria"] == ["i", "ii", "iv"] and row["inscription_years"] == [1979, 2011]
    assert row["states_iso3"] == ["SYR", "LBN"] and row["in_danger"] and row["url"].endswith("/20")


# ------------------------------------------------------------------ refugees


def test_refugees_params() -> None:
    p = dict(refugees.request_params(FakeFilters(eq={"origin_iso3": "syr", "year": 2023}), 3))
    assert p["coo"] == "SYR" and p["coa_all"] == "true" and p["yearFrom"] == 2023 and p["page"] == 3
    assert p["cf_type"] == "ISO"
    assert refugees._code("-") is None


# ---------------------------------------------------------------- HTTP layer


class _Handler(http.server.BaseHTTPRequestHandler):
    calls: list[tuple[str, str | None]] = []

    def do_GET(self) -> None:
        _Handler.calls.append((self.path, self.headers.get("Authorization")))
        if len(_Handler.calls) == 1:
            self.send_response(429)
            self.send_header("Retry-After", "0")
            self.end_headers()
            return
        body = json.dumps({"ok": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:
        pass


@pytest.fixture
def mirror() -> Iterator[str]:
    _Handler.calls = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_mirror_retries_429_and_sends_headers(mirror: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("un_worker.common.time.sleep", lambda _s: None)
    body = common.fetch_json(
        Options(mirror, "ua-test"), "population", "data/x", [("b", 2), ("a", 1)], headers={"Authorization": "Bearer t"}
    )
    assert body == {"ok": True}
    assert len(_Handler.calls) == 2
    assert _Handler.calls[1] == ("/population/data/x?a=1&b=2", "Bearer t")
