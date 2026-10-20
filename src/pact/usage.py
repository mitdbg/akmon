"""One JSON line per invocation, so PACT can be measured without transcripts.

Default path ``.okg/pact-usage.jsonl`` under the repo root; ``PACT_USAGE_LOG``
overrides it and ``PACT_USAGE_LOG=off`` disables it. Never raises.
"""

from __future__ import annotations

import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pact import git

CLASS_BY_PREFIX = (
    ("evidence_", "stale_receipt"),
    ("unproven_", "missing_proof"),
    ("approval_", "approval"),
    ("tier_", "tier"),
    ("invalid_", "manifest_syntax"),
    ("duplicate_", "manifest_syntax"),
    ("task_requirement_unknown", "manifest_syntax"),
    ("requirement_without_task", "manifest_syntax"),
    ("unknown_change", "manifest_syntax"),
    ("change_id_mismatch", "manifest_syntax"),
)


def classify(token: str) -> str:
    for prefix, cls in CLASS_BY_PREFIX:
        if token.startswith(prefix):
            return cls
    return "other"


def _public_origin(url: str | None) -> str | None:
    """The origin without any userinfo (tokens live there in CI clones)."""
    if not url:
        return None
    if "://" in url:
        scheme, _, rest = url.partition("://")
        rest = rest.rsplit("@", 1)[-1]
        return f"{scheme}://{rest}"
    return url.rsplit("@", 1)[-1] if url.count("@") > 1 else url


def log_path(root: Path) -> Path | None:
    override = os.environ.get("PACT_USAGE_LOG")
    if override == "off":
        return None
    if override:
        return Path(override)
    return root / ".okg" / "pact-usage.jsonl"


def record(root: Path, *, verb: str, change_id: str | None, verdict: str, blocked_by: list[str], started: float, caller: str | None = None) -> None:
    path = log_path(root)
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "verb": verb,
            "change": change_id,
            "verdict": verdict,
            "blocked_by": list(blocked_by),
            "classes": sorted({classify(t) for t in blocked_by}),
            "duration_ms": int((time.monotonic() - started) * 1000),
            "head": git.head(root),
            "origin": _public_origin(git.origin_url(root)),
            "caller": caller or os.environ.get("PACT_CALLER") or ("ci" if os.environ.get("CI") else "local"),
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    except OSError:
        return


def report(paths: list[Path]) -> dict:
    """The study's measures, recomputed from usage lines."""
    rows: list[dict] = []
    for path in paths:
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    verdicts = Counter(r.get("verdict") for r in rows)
    verbs = Counter(r.get("verb") for r in rows)
    classes: Counter = Counter()
    tokens: Counter = Counter()
    for r in rows:
        for c in r.get("classes", []):
            classes[c] += 1
        for t in r.get("blocked_by", []):
            tokens[t] += 1
    durations = sorted(int(r.get("duration_ms") or 0) for r in rows)
    changes = {r.get("change") for r in rows if r.get("change")}
    callers = Counter(r.get("caller") for r in rows)
    refused = verdicts.get("refused", 0)
    return {
        "invocations": len(rows),
        "changes": len(changes),
        "verdicts": dict(verdicts),
        "refusal_rate": round(refused / len(rows), 4) if rows else None,
        "verbs": dict(verbs),
        "refusal_classes": dict(classes),
        "top_tokens": tokens.most_common(10),
        "duration_ms_median": durations[len(durations) // 2] if durations else None,
        "callers": dict(callers),
    }
