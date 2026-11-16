"""The bundled UNSD M49 standard: country codes and the M49 <-> ISO 3166-1 mapping."""

from __future__ import annotations

import functools
import io
from importlib import resources

import pyarrow as pa
import pyarrow.csv as pacsv

BOOL_COLUMNS = ("ldc", "lldc", "sids")


@functools.cache
def m49_table() -> pa.Table:
    """Load the bundled M49 overview table (248 countries or areas), every code as VARCHAR."""
    raw = resources.files("un_worker").joinpath("data/m49.csv").read_bytes()
    # Every column is text: "NA" is Namibia's alpha-2 code, not a missing value.
    names = raw.split(b"\n", 1)[0].decode().strip().split(",")
    convert = pacsv.ConvertOptions(
        column_types=dict.fromkeys(names, pa.string()),
        null_values=[""],
        strings_can_be_null=True,
        quoted_strings_can_be_null=False,
    )
    table = pacsv.read_csv(io.BytesIO(raw), convert_options=convert)
    for name in BOOL_COLUMNS:
        idx = table.column_names.index(name)
        table = table.set_column(idx, name, pa.array([v == "true" for v in table[name].to_pylist()], pa.bool_()))
    return table


@functools.cache
def _rows() -> list[dict[str, str | bool | None]]:
    return m49_table().to_pylist()


@functools.cache
def m49_to_iso3_map() -> dict[str, str]:
    """``'004'`` → ``'AFG'``."""
    return {str(r["m49"]): str(r["iso_alpha3"]) for r in _rows() if r["iso_alpha3"]}


@functools.cache
def iso3_to_m49_map() -> dict[str, str]:
    """``'AFG'`` → ``'004'``."""
    return {v: k for k, v in m49_to_iso3_map().items()}


@functools.cache
def iso2_to_iso3_map() -> dict[str, str]:
    """``'AF'`` → ``'AFG'``."""
    return {str(r["iso_alpha2"]): str(r["iso_alpha3"]) for r in _rows() if r["iso_alpha2"] and r["iso_alpha3"]}


@functools.cache
def iso3_to_iso2_map() -> dict[str, str]:
    """``'AFG'`` → ``'AF'``."""
    return {v: k for k, v in iso2_to_iso3_map().items()}


def normalize_m49(code: str | int | None) -> str | None:
    """Zero-pad a numeric M49 code to three digits (``'4'`` → ``'004'``); non-numeric → None."""
    if code is None:
        return None
    text = str(code).strip()
    if not text.isdigit() or len(text) > 3:
        return None
    return text.zfill(3)


def iso3_for_m49(code: str | int | None) -> str | None:
    """ISO alpha-3 of a country-level M49 code, or None for regions and unknown codes."""
    norm = normalize_m49(code)
    return m49_to_iso3_map().get(norm) if norm else None


def known_iso3(code: str | None) -> str | None:
    """Return ``code`` upper-cased when it is an ISO alpha-3 code listed in M49, else None."""
    if not code:
        return None
    up = code.strip().upper()
    return up if up in iso3_to_m49_map() else None
