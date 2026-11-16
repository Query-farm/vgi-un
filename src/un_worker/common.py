"""Shared plumbing: ATTACH options, polite HTTP fetching, fixtures, memo cache and pushdown helpers.

Nothing here touches the network at import or ATTACH time. Requests happen only
while a query runs.

Every upstream request goes through :func:`fetch_json` with a *source* name
(``sdg``, ``uis``, ``heritage``, ``population``, ``refugees``, ``health``), a path
relative to that source's API root and a list of query parameters. The
``base_url`` ATTACH option redirects all of them:

* empty (default): each source's public API host (see ``HOSTS``);
* ``http(s)://mirror``: ``<mirror>/<source>/<path>?<query>`` (a proxy or mock);
* a local directory: recorded responses laid out by :func:`fixture_relpath`,
  which is how the tests and the metadata linter run offline.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
from vgi.table_function import OutputCollector

log = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "vgi-un/0.1 (+https://query.farm)"
TIMEOUT_SECONDS = 60.0
MAX_ATTEMPTS = 4
MEMO_TTL_SECONDS = 3600.0

HOSTS = {
    "sdg": "https://unstats.un.org/sdgapi",
    "uis": "https://api.uis.unesco.org/api/public",
    "heritage": "https://data.unesco.org/api/explore/v2.1/catalog/datasets/whc001",
    "population": "https://population.un.org/dataportalapi/api/v1",
    "refugees": "https://api.unhcr.org/population/v1",
    "health": "https://ghoapi.azureedge.net/api",
}

# Minimum seconds between two requests to one source from this process. The WPP
# Data Portal allows 5 requests per 10 seconds per IP; the others publish no limit.
MIN_INTERVAL = {"population": 2.1}
DEFAULT_INTERVAL = 0.25

Params = Sequence[tuple[str, str | int]]


class UpstreamError(RuntimeError):
    """An upstream request failed in a way a retry will not fix."""

    def __init__(self, message: str, *, status: int | None = None, body: str = "") -> None:
        """Keep the HTTP status and a body excerpt for callers that inspect them."""
        super().__init__(message)
        self.status = status
        self.body = body


@dataclass(frozen=True, slots=True)
class Options:
    """The ATTACH options, with defaults filled in."""

    base_url: str
    user_agent: str

    @property
    def mode(self) -> str:
        """``live``, ``mirror`` or ``fixtures``."""
        if not self.base_url:
            return "live"
        if self.base_url.startswith(("http://", "https://")):
            return "mirror"
        return "fixtures"


def _value(raw: Any) -> Any:
    return raw.as_py() if hasattr(raw, "as_py") else raw


def options_from(attach_options: Mapping[str, Any] | None) -> Options:
    """Build `Options` from ``params.attach_options`` (lower-cased names, defaults absent)."""
    attach_options = attach_options or {}
    base_url = str(_value(attach_options.get("base_url")) or "").strip()
    user_agent = str(_value(attach_options.get("user_agent")) or "").strip() or DEFAULT_USER_AGENT
    return Options(base_url=base_url.removeprefix("file://"), user_agent=user_agent)


def canonical_query(params: Params) -> str:
    """Encode query parameters in a stable order (repeated keys keep their relative order)."""
    items = sorted(((k, str(v)) for k, v in params), key=lambda kv: kv[0])
    return urllib.parse.urlencode(items, quote_via=urllib.parse.quote)


def fixture_relpath(source: str, path: str, params: Params) -> str:
    """Relative file name of a recorded response: ``<source>/<path>/<sha12 of the query>.json``."""
    clean = "/".join(
        "".join(c if c.isalnum() or c in "._-" else "_" for c in seg) for seg in path.strip("/").split("/") if seg
    )
    digest = hashlib.sha256(canonical_query(params).encode()).hexdigest()[:12]
    return f"{source}/{clean}/{digest}.json"


def request_url(opts: Options, source: str, path: str, params: Params) -> str:
    """Return the URL a request resolves to in live or mirror mode."""
    root = f"{opts.base_url.rstrip('/')}/{source}" if opts.mode == "mirror" else HOSTS[source]
    query = canonical_query(params)
    return f"{root}/{path.lstrip('/')}" + (f"?{query}" if query else "")


_last_request: dict[str, float] = {}
_throttle_lock = threading.Lock()


def _throttle(source: str) -> None:
    gap = MIN_INTERVAL.get(source, DEFAULT_INTERVAL)
    with _throttle_lock:
        now = time.monotonic()
        wait = _last_request.get(source, 0.0) + gap - now
        _last_request[source] = max(now, now + wait)
    if wait > 0:
        time.sleep(wait)


def _http_get(opts: Options, source: str, url: str, headers: Mapping[str, str]) -> bytes:
    base_headers = {"User-Agent": opts.user_agent, "Accept": "application/json", **headers}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        _throttle(source)
        req = urllib.request.Request(url, headers=base_headers)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
                return bytes(resp.read())
        except urllib.error.HTTPError as err:
            body = err.read(2000).decode("utf-8", errors="replace")
            retryable = err.code == 429 or err.code >= 500
            if not retryable or attempt == MAX_ATTEMPTS:
                raise UpstreamError(
                    f"GET {_redact(url)} failed with HTTP {err.code}: {body[:300]}", status=err.code, body=body
                ) from err
            retry_after = err.headers.get("Retry-After")
            delay = float(retry_after) if retry_after and retry_after.isdigit() else 1.0 * 2**attempt
            log.info("HTTP %s from %s; retrying in %.1fs", err.code, source, delay)
            time.sleep(min(delay, 60.0))
        except (urllib.error.URLError, TimeoutError) as err:
            if attempt == MAX_ATTEMPTS:
                reason = getattr(err, "reason", err)
                raise UpstreamError(f"GET {_redact(url)} failed: {reason}") from err
            time.sleep(1.0 * 2**attempt)
    raise AssertionError("unreachable")


def _redact(url: str) -> str:
    return url  # no credentials ever travel in URLs; tokens go in headers


def fetch_bytes(
    opts: Options, source: str, path: str, params: Params = (), *, headers: Mapping[str, str] | None = None
) -> bytes:
    """Fetch one upstream response body (or its recorded fixture)."""
    if opts.mode == "fixtures":
        rel = fixture_relpath(source, path, params)
        file = Path(opts.base_url).expanduser() / rel
        if not file.exists():
            raise UpstreamError(
                f"no recorded response for {source} {path}?{canonical_query(params)} (expected {file}); "
                "record it with test/fixtures/record.py or point base_url at the live API"
            )
        meta = file.with_suffix(".status")
        data = file.read_bytes()
        if meta.exists():
            status = int(meta.read_text().strip())
            body = data.decode("utf-8", errors="replace")
            raise UpstreamError(
                f"GET {source} {path} failed with HTTP {status}: {body[:300]}", status=status, body=body
            )
        return data
    return _http_get(opts, source, request_url(opts, source, path, params), headers or {})


def fetch_json(
    opts: Options, source: str, path: str, params: Params = (), *, headers: Mapping[str, str] | None = None
) -> Any:
    """Fetch and decode one JSON response (a UTF-8 BOM is tolerated)."""
    raw = fetch_bytes(opts, source, path, params, headers=headers)
    try:
        return json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise UpstreamError(f"{source} {path} did not return JSON: {raw[:200]!r}") from err


_memo: dict[tuple[str, str, str, str], tuple[float, Any]] = {}
_memo_lock = threading.Lock()


def fetch_json_cached(opts: Options, source: str, path: str, params: Params = ()) -> Any:
    """`fetch_json` with a per-process cache (one hour) for catalog/list endpoints."""
    key = (opts.base_url, source, path, canonical_query(params))
    now = time.monotonic()
    with _memo_lock:
        hit = _memo.get(key)
        if hit and now - hit[0] < MEMO_TTL_SECONDS:
            return hit[1]
    value = fetch_json(opts, source, path, params)
    with _memo_lock:
        _memo[key] = (now, value)
    return value


# ---------------------------------------------------------------- pushdown helpers


def pushed_values(filters: Any, column: str) -> list[Any] | None:
    """Values a column must equal (``=`` or ``IN``), or None when not constrained that way."""
    if filters is None:
        return None
    const = filters.get_column_constant(column)
    if const is not None and const.is_valid:
        return [const.as_py()]
    values = filters.get_column_in_values(column)
    if values is not None:
        return [v for v in values.to_pylist() if v is not None]
    return None


def pushed_int_range(filters: Any, column: str) -> tuple[int | None, int | None]:
    """Inclusive integer bounds on a column from ``=``, ``IN``, ``<``, ``<=``, ``>``, ``>=``, ``BETWEEN``."""
    if filters is None:
        return None, None
    values = pushed_values(filters, column)
    if values:
        ints = [int(v) for v in values]
        return min(ints), max(ints)
    bounds = filters.get_column_bounds(column)
    if bounds is None:
        return None, None
    lo = hi = None
    if bounds.min_value is not None and bounds.min_value.is_valid:
        lo = int(bounds.min_value.as_py()) + (0 if bounds.min_inclusive else 1)
    if bounds.max_value is not None and bounds.max_value.is_valid:
        hi = int(bounds.max_value.as_py()) - (0 if bounds.max_inclusive else 1)
    return lo, hi


def split_codes(text: str) -> list[str]:
    """Split ``'A, B ,C'`` into ``['A', 'B', 'C']`` (empty text gives ``[]``)."""
    return [p.strip() for p in text.split(",") if p.strip()]


def merge_codes(arg: Iterable[str], pushed: list[Any] | None) -> list[str]:
    """Combine codes from an argument with pushed-down ``=``/``IN`` values (argument wins)."""
    codes = [c for c in arg if c]
    if codes:
        return codes
    return [str(v) for v in pushed or [] if v is not None and str(v)]


# ---------------------------------------------------------------- batches


def emit_rows(out: OutputCollector, schema: pa.Schema, rows: list[dict[str, Any]]) -> None:
    """Emit dict rows as one batch in the bound (possibly projected) schema."""
    cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
    out.emit(pa.RecordBatch.from_pydict(cols, schema=schema))


def to_float(raw: Any) -> float | None:
    """Parse a number the APIs may send as a string; ``'-'``, ``''`` and junk become None."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip().replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def to_int(raw: Any) -> int | None:
    """Parse an integer-valued field (``2015``, ``2015.0`` or ``'2015'``)."""
    num = to_float(raw)
    return None if num is None else int(num)
