"""Hugging Face dataset fetch via the datasets-server rows API.

Stdlib-only (urllib): the repo deliberately carries no ``datasets`` /
``pandas`` / ``pyarrow`` dependency. Rows are cached under
``<data_root>/datasets/<safe_name>/`` with a ``meta.json`` pinning the
dataset's git revision, so a run manifest can record exactly which dataset
version produced its instances.

Offline discipline: loaders never fetch implicitly during a run — a missing
cache is a loud ``DatasetError`` telling the operator to run
``okg-bench fetch``.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .errors import DatasetError

ROWS_URL = "https://datasets-server.huggingface.co/rows"
DATASET_API_URL = "https://huggingface.co/api/datasets/"
PAGE = 100
_TIMEOUT_S = 60
_MAX_ATTEMPTS = 5
_RATE_LIMIT_BACKOFF_S = 20.0


def _safe(name: str) -> str:
    return name.replace("/", "__")


def cache_dir(data_root: Path, dataset: str) -> Path:
    return data_root / "datasets" / _safe(dataset)


def _get_json(url: str) -> Any:
    """GET with backoff. HTTP 429 (the datasets-server rate limit, hit
    while fetching a multi-config dataset) needs a far longer wait than a
    transient network error, and honors Retry-After when present."""
    req = urllib.request.Request(url, headers={"User-Agent": "okg-bench/1"})
    last_err: Exception | None = None
    for attempt in range(_MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_err = exc
            if exc.code != 429:
                time.sleep(2**attempt)
                continue
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            try:
                delay = float(retry_after) if retry_after else 0.0
            except ValueError:
                delay = 0.0
            time.sleep(max(delay, _RATE_LIMIT_BACKOFF_S * (attempt + 1)))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_err = exc
            time.sleep(2**attempt)
    raise DatasetError(
        f"GET {url} failed after {_MAX_ATTEMPTS} attempts: {last_err}"
    )


def dataset_revision(dataset: str) -> str:
    info = _get_json(DATASET_API_URL + dataset)
    sha = info.get("sha")
    if not sha:
        raise DatasetError(f"HF API returned no revision sha for {dataset!r}: {info}")
    return str(sha)


def cache_key(config: str, split: str) -> str:
    """Cache filename stem for one (config, split) pair.

    Datasets partition either way: SWE-QA has one config and a split per
    repo, CTIBench has five configs that ALL use split "test". Keying on
    the split alone made every CTIBench config overwrite the previous
    one's cache (caught by the echo dry run), so a non-default config is
    the key and the split is the key only under the default config."""
    return split if config in ("default", "", None) else config


def fetch_split(
    dataset: str, config: str, split: str, *, data_root: Path
) -> tuple[list[dict[str, Any]], str]:
    """Fetch every row of one split, cache it, and return (rows, revision)."""
    revision = dataset_revision(dataset)
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        qs = urllib.parse.urlencode(
            {"dataset": dataset, "config": config, "split": split,
             "offset": offset, "length": PAGE}
        )
        payload = _get_json(f"{ROWS_URL}?{qs}")
        if "rows" not in payload:
            raise DatasetError(
                f"datasets-server returned no rows for {dataset}/{config}/{split}"
                f" at offset {offset}: {payload}"
            )
        batch = [r["row"] for r in payload["rows"]]
        rows.extend(batch)
        total = payload.get("num_rows_total")
        if total is None:
            raise DatasetError(
                f"datasets-server response missing num_rows_total for {dataset}"
            )
        offset += len(batch)
        if offset >= int(total) or not batch:
            break
    if not rows:
        raise DatasetError(f"{dataset}/{config}/{split} fetched zero rows")
    out = cache_dir(data_root, dataset)
    out.mkdir(parents=True, exist_ok=True)
    key = cache_key(config, split)
    (out / f"{key}.json").write_text(
        json.dumps(rows, indent=None, sort_keys=True), encoding="utf-8"
    )
    meta_path = out / "meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    meta.setdefault("dataset", dataset)
    meta["revision"] = revision
    meta.setdefault("splits", {})[key] = {
        "rows": len(rows), "config": config, "split": split,
        "fetched_at": _now(),
    }
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
    return rows, revision


def load_cached_split(dataset: str, split: str, *, data_root: Path) -> list[dict[str, Any]]:
    path = cache_dir(data_root, dataset) / f"{split}.json"
    if not path.exists():
        raise DatasetError(
            f"dataset {dataset!r} split {split!r} is not cached at {path}; "
            f"run `okg-bench fetch --benchmark <name>` first (network required once)"
        )
    return json.loads(path.read_text(encoding="utf-8"))


def cached_meta(dataset: str, *, data_root: Path) -> dict[str, Any]:
    path = cache_dir(data_root, dataset) / "meta.json"
    if not path.exists():
        raise DatasetError(
            f"dataset {dataset!r} has no cache meta at {path}; run `okg-bench fetch` first"
        )
    return json.loads(path.read_text(encoding="utf-8"))


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
