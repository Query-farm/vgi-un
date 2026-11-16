"""Record one live upstream response as a fixture (maintainer tool; never run by the tests).

    uv run python test/fixtures/record.py sdg v1/sdg/Series/Data seriesCode=SI_POV_DAY1 areaCode=356

Writes ``test/fixtures/<source>/<path>/<sha12>.json`` (the layout the worker reads when
``base_url`` is a directory) and appends the request to ``test/fixtures/<source>/MANIFEST.tsv``.
A non-2xx answer is stored too, with its status in a sibling ``.status`` file, so error
handling can be tested offline. Repeat ``key=value`` for repeated query parameters.

Only record sources whose licence allows redistribution (see DATA-SOURCES.md); the
CC BY-SA and WHO sources use hand-written fixtures instead.
"""

from __future__ import annotations

import datetime as dt
import sys
import urllib.error
import urllib.request
from pathlib import Path

from un_worker.common import DEFAULT_USER_AGENT, Options, canonical_query, fixture_relpath, request_url

HERE = Path(__file__).resolve().parent


def main(argv: list[str]) -> int:
    """Fetch and store one response."""
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    source, path, *pairs = argv
    params = [tuple(p.split("=", 1)) for p in pairs]
    opts = Options(base_url="", user_agent=DEFAULT_USER_AGENT)
    url = request_url(opts, source, path, params)  # type: ignore[arg-type]
    req = urllib.request.Request(url, headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"})
    status = 200
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read()
    except urllib.error.HTTPError as err:
        status, body = err.code, err.read()
    rel = fixture_relpath(source, path, params)  # type: ignore[arg-type]
    target = HERE / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    if status != 200:
        target.with_suffix(".status").write_text(f"{status}\n")
    with open(HERE / source / "MANIFEST.tsv", "a", encoding="utf-8") as manifest:
        query = canonical_query(params)  # type: ignore[arg-type]
        manifest.write(f"{rel}\t{status}\t{dt.date.today().isoformat()}\t{path}?{query}\n")
    print(f"{status} {len(body)} bytes -> {rel}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
