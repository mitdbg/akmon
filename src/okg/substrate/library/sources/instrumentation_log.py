"""Instrumentation-audit register source.

Turns the study's "what the graph cannot see" register into durable graph
nodes so readiness and review surfaces can query known telemetry blind spots.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)


__all__ = ["InstrumentationLogSource"]


_SLUG_RE = re.compile(r"[^a-z0-9]+")


@dataclass
class InstrumentationLogSource:
    """Emit one ``instrumentation_gap`` node per register row."""

    deployment: str
    repo: str
    register_path: Path | str

    name: str = "instrumentation_log"
    profile: str = "discovery_crawl"

    # Change-probe contract (#323): a single non-git register YAML, so the
    # O(content) floor over that one file is the right probe. The source already
    # hashes row content into its cursor; the probe hashes the file bytes so the
    # runner skips the parse on no-change.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.register_path = Path(self.register_path).expanduser().resolve()
        self.last_run_stats: dict[str, Any] = {}
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Path]]:
            path = Path(self.register_path)
            return [(str(path), path)] if path.is_file() else []

        return ContentHashProbe(
            content_items=_items,
            config={
                "repo": self.repo,
                "deployment": self.deployment,
                "register_path": str(self.register_path),
            },
            emit_targets=[sys.modules[__name__]],
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        del cursor, sync_scope
        rows = self._load_rows()
        facts: list[NodeFact] = []
        records: dict[str, RecordEmission] = {}
        content_hash = _content_hash(rows)
        for row in rows:
            fact = self._fact_for_row(row, content_hash=content_hash)
            facts.append(fact)
            record_key = fact.attrs["gap_id"]
            records[record_key] = RecordEmission(
                primary=RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=fact.source_record_id,
                    source_revision=fact.source_revision,
                )
            )

        self.last_run_stats = {
            "rows": len(rows),
            "open_gaps": sum(1 for fact in facts if fact.attrs["status"] == "open"),
            "closed_gaps": sum(1 for fact in facts if fact.attrs["status"] == "closed"),
            "register_path": str(self.register_path),
        }
        return SourceRun(
            facts=iter(facts),
            completed_scope=True,
            next_cursor={
                "run_id": run_id,
                "content_hash": content_hash,
                "row_count": len(rows),
            },
            run_mode=mode,
            record_set=RecordSet(records=records),
        )

    def _load_rows(self) -> list[dict[str, Any]]:
        if not self.register_path.is_file():
            raise SubstrateError(
                f"source {self.name!r}: register_path {self.register_path} not found",
                data={"adapter": self.name, "register_path": str(self.register_path)},
                fix_hint="point register_path at the instrumentation_log YAML fixture",
            )
        raw = fastyaml.safe_load(self.register_path.read_text(encoding="utf-8")) or {}
        source_report = _source_report(raw.get("source"))
        rows = raw.get("rows")
        if not isinstance(rows, list):
            raise SubstrateError(
                f"source {self.name!r}: register rows must be a list",
                data={"adapter": self.name, "register_path": str(self.register_path)},
            )
        return [
            _normalize_row(item, index=index, source_report=source_report)
            for index, item in enumerate(rows)
        ]

    def _fact_for_row(self, row: dict[str, Any], *, content_hash: str) -> NodeFact:
        field = row["field"]
        valid_from = row["valid_from"]
        valid_to = row.get("valid_to")
        gap_id = f"{self.deployment}:{_slug(field)}:{valid_from}"
        node_id = f"instrumentation-gap:{self.deployment}:{_slug(field)}:{valid_from}"
        status = "closed" if valid_to else "open"
        consequence = row["consequence"]
        attrs = {
            "gap_id": gap_id,
            "field": field,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "coverage": row["coverage"],
            "consequence": consequence,
            "status": status,
            "closed_at": valid_to if status == "closed" else None,
            "closed_by": row.get("closed_by"),
            "source_report": row.get("source_report"),
            "register_path": str(self.register_path),
            "text": _search_text(
                field=field,
                valid_from=valid_from,
                valid_to=valid_to,
                coverage=row["coverage"],
                consequence=consequence,
                status=status,
            ),
        }
        source_record_id = {
            "repo": self.repo,
            "deployment": self.deployment,
            "gap_id": gap_id,
            "field": field,
        }
        source_revision = {
            "content_hash": content_hash,
            "row_hash": hashlib.sha256(
                json.dumps(row, sort_keys=True).encode("utf-8")
            ).hexdigest(),
        }
        return NodeFact(
            node_id=node_id,
            subtype="instrumentation_gap",
            attrs=attrs,
            source_record_id=source_record_id,
            source_revision=source_revision,
        )


def _normalize_row(
    raw: Any,
    *,
    index: int,
    source_report: Optional[str] = None,
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise SubstrateError(
            "instrumentation_log row must be a mapping",
            data={"row_index": index},
        )
    required = ("field", "valid_from", "coverage", "consequence")
    missing = [key for key in required if raw.get(key) in (None, "")]
    if missing:
        raise SubstrateError(
            "instrumentation_log row missing required fields",
            data={"row_index": index, "missing": missing},
        )
    out: dict[str, Any] = {
        "field": str(raw["field"]),
        "valid_from": str(raw["valid_from"]),
        "coverage": str(raw["coverage"]),
        "consequence": str(raw["consequence"]),
    }
    for key in ("valid_to", "closed_by", "source_report"):
        value = raw.get(key)
        if value not in (None, ""):
            out[key] = str(value)
    if "source_report" not in out and source_report:
        out["source_report"] = source_report
    return out


def _source_report(raw: Any) -> Optional[str]:
    if not isinstance(raw, dict):
        return None
    value = raw.get("report_ref") or raw.get("report_path") or raw.get("study")
    return str(value) if value not in (None, "") else None


def _content_hash(rows: list[dict[str, Any]]) -> str:
    return hashlib.sha256(
        json.dumps(rows, sort_keys=True).encode("utf-8")
    ).hexdigest()


def _slug(value: str) -> str:
    slug = _SLUG_RE.sub("-", value.lower()).strip("-")
    return slug or "field"


def _search_text(
    *,
    field: str,
    valid_from: str,
    valid_to: Optional[str],
    coverage: str,
    consequence: str,
    status: str,
) -> str:
    window = f"{valid_from}..{valid_to}" if valid_to else f"{valid_from}..open"
    return (
        f"instrumentation gap {field} status {status} valid {window}; "
        f"coverage {coverage}; consequence {consequence}"
    )
