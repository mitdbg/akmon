"""Research branch disposition source.

This adapter projects a repo-local disposition register into the git graph
module. Branch enumeration, summary authoring, and destructive git operations
belong to the branch-disposition skill, not this source adapter.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from okg.substrate import fastyaml

from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)


TERMINAL_DISPOSITIONS = frozenset({
    "merge",
    "archive_with_summary",
    "kill",
})
NON_TERMINAL_DISPOSITIONS = frozenset({"undispositioned"})
SUPPORTED_DISPOSITIONS = TERMINAL_DISPOSITIONS | NON_TERMINAL_DISPOSITIONS


class BranchDispositionError(ValueError):
    """Raised when a branch disposition register violates the contract."""


def default_register_path(repo_root: Path | str) -> Path:
    """Return the repo-local disposition register path."""
    root = Path(repo_root).expanduser()
    return root / ".okg" / "research_branch_dispositions.yaml"


def slugify_branch_name(branch_name: str) -> str:
    """Make a branch name safe for deterministic graph ids."""
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", branch_name.strip())
    slug = re.sub(r"-+", "-", slug).strip("-")
    if not slug:
        raise BranchDispositionError("branch name cannot slugify to empty")
    return slug.lower()


def normalize_state(raw_state: Any) -> str:
    state = str(raw_state or "undispositioned").strip().lower()
    state = state.replace("-", "_")
    if state == "archive":
        state = "archive_with_summary"
    if state not in SUPPORTED_DISPOSITIONS:
        raise BranchDispositionError(
            f"unsupported branch disposition state {raw_state!r}; "
            f"expected one of {sorted(SUPPORTED_DISPOSITIONS)}"
        )
    return state


def _coerce_int(value: Any, *, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise BranchDispositionError(f"expected integer, got {value!r}") from exc


def _summary_body(raw_summary: Any) -> str:
    if isinstance(raw_summary, str):
        return raw_summary.strip()
    if isinstance(raw_summary, dict):
        for key in ("body", "summary", "text"):
            value = raw_summary.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _summary_title(raw_summary: Any, *, branch_name: str) -> str:
    if isinstance(raw_summary, dict):
        value = raw_summary.get("title")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return f"Archive summary for {branch_name}"


def _summary_findings(raw_summary: Any) -> list[str]:
    if isinstance(raw_summary, dict):
        raw_findings = raw_summary.get("findings") or []
        if isinstance(raw_findings, list):
            return [str(item).strip() for item in raw_findings if str(item).strip()]
    return []


def _summary_id(raw_summary: Any, *, repo: str, branch_name: str) -> str:
    if isinstance(raw_summary, dict):
        value = raw_summary.get("summary_id")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return f"branch-archive-summary:{repo}:{slugify_branch_name(branch_name)}"


def load_disposition_register(register_path: Path | str) -> list[dict[str, Any]]:
    path = Path(register_path).expanduser()
    if not path.exists():
        return []
    return _load_disposition_bytes(path.read_bytes(), path=path)


def _load_disposition_bytes(
    raw_bytes: bytes,
    *,
    path: Path,
) -> list[dict[str, Any]]:
    raw = fastyaml.safe_load(raw_bytes.decode("utf-8")) or {}
    if isinstance(raw, list):
        rows = raw
    elif isinstance(raw, dict):
        rows = raw.get("dispositions") or raw.get("branches") or []
    else:
        raise BranchDispositionError(
            f"branch disposition register {path} must be a list or mapping"
        )
    if not isinstance(rows, list):
        raise BranchDispositionError(
            f"branch disposition register {path} has non-list dispositions"
        )
    return [dict(row or {}) for row in rows]


def normalize_disposition(
    raw: dict[str, Any],
    *,
    repo: str,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    # ``now`` is retained for API compatibility only. Acquisition time is not
    # content and must never enter a fact derived from this register.
    del now
    branch_name = str(raw.get("branch_name") or raw.get("name") or "").strip()
    if not branch_name:
        raise BranchDispositionError("branch disposition requires branch_name")

    state = normalize_state(raw.get("state") or raw.get("disposition"))
    terminal = state in TERMINAL_DISPOSITIONS
    raw_summary = raw.get("summary") or {}
    summary_body = _summary_body(raw_summary)
    reason = str(raw.get("reason") or "").strip()
    if state == "archive_with_summary" and not summary_body:
        raise BranchDispositionError(
            f"branch {branch_name!r} cannot be archive_with_summary "
            "without a non-empty summary"
        )
    if state == "kill" and not reason:
        raise BranchDispositionError(
            f"branch {branch_name!r} cannot be kill without a reason"
        )

    normalized_repo = str(raw.get("repo") or repo).strip()
    base_branch = str(raw.get("base_branch") or "dev").strip()
    disposition_id = str(
        raw.get("disposition_id")
        or f"branch-disposition:{normalized_repo}:{slugify_branch_name(branch_name)}"
    )
    age_days = _coerce_int(raw.get("age_days"), default=0)
    edit_mass = _coerce_int(raw.get("edit_mass"), default=0)
    owner = str(raw.get("owner") or "").strip()
    pact_id = str(raw.get("pact_id") or "").strip()
    origin_session_id = str(raw.get("origin_session_id") or "").strip()
    decided_at = str(raw.get("decided_at") or raw.get("archived_at") or "").strip()
    warning = (
        str(raw.get("warning") or "").strip()
        if state == "undispositioned"
        else ""
    )
    checked_at = str(raw.get("checked_at") or "").strip()

    summary_id = ""
    summary_title = ""
    summary_findings: list[str] = []
    if state == "archive_with_summary":
        summary_id = _summary_id(
            raw_summary,
            repo=normalized_repo,
            branch_name=branch_name,
        )
        summary_title = _summary_title(raw_summary, branch_name=branch_name)
        summary_findings = _summary_findings(raw_summary)

    text_parts = [
        branch_name,
        state,
        reason,
        summary_title,
        summary_body,
        " ".join(summary_findings),
        pact_id,
        origin_session_id,
    ]
    return {
        "disposition_id": disposition_id,
        "repo": normalized_repo,
        "branch_name": branch_name,
        "base_branch": base_branch,
        "state": state,
        "terminal": terminal,
        "reason": reason,
        "summary_id": summary_id,
        "summary_title": summary_title,
        "summary_body": summary_body,
        "summary_findings": summary_findings,
        "origin_session_id": origin_session_id,
        "owner": owner,
        "pact_id": pact_id,
        "age_days": age_days,
        "edit_mass": edit_mass,
        "warning": warning,
        "decided_at": decided_at,
        "checked_at": checked_at,
        "text": "\n".join(part for part in text_parts if part),
    }


@dataclass
class BranchDispositionsSource:
    """Project the research-branch disposition register into graph facts."""

    repo: str
    register_path: Path | str

    name: str = "branch_dispositions"
    profile: str = "discovery_crawl"
    probe_short_circuit_safe = True
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    # Change-probe contract (#323): a single non-git disposition register YAML,
    # so the content-hash floor over that one file is the sound probe.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if isinstance(self.register_path, str):
            self.register_path = Path(self.register_path).expanduser()
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        def _items() -> list[tuple[str, Path]]:
            path = Path(self.register_path)
            return [(str(path), path)] if path.is_file() else []

        return ContentHashProbe(
            content_items=_items,
            config={"repo": self.repo, "register_path": str(self.register_path)},
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
            ),
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        del cursor, sync_scope
        register_path = Path(self.register_path)
        register_bytes = (
            register_path.read_bytes()
            if register_path.exists()
            else None
        )
        rows = [
            normalize_disposition(raw, repo=self.repo)
            for raw in (
                _load_disposition_bytes(register_bytes, path=register_path)
                if register_bytes is not None
                else []
            )
        ]
        del run_id
        register_hash = (
            hashlib.sha256(register_bytes).hexdigest()
            if register_bytes is not None
            else "missing"
        )
        revision = {"register_hash": register_hash}
        applied_item_hashes = (
            {str(register_path): register_hash}
            if register_bytes is not None
            else {}
        )
        facts: list[NodeFact | EdgeFact] = []
        records: dict[str, RecordEmission] = {}
        for row in rows:
            disposition_node = f"branch-disposition:{row['repo']}:{slugify_branch_name(row['branch_name'])}"
            disposition_record = {
                "repo": row["repo"],
                "branch_name": row["branch_name"],
                "disposition_id": row["disposition_id"],
            }
            disposition_attrs = {
                "disposition_id": row["disposition_id"],
                "repo": row["repo"],
                "branch_name": row["branch_name"],
                "base_branch": row["base_branch"],
                "state": row["state"],
                "terminal": row["terminal"],
                "reason": row["reason"],
                "summary_id": row["summary_id"],
                "origin_session_id": row["origin_session_id"],
                "owner": row["owner"],
                "pact_id": row["pact_id"],
                "age_days": row["age_days"],
                "edit_mass": row["edit_mass"],
                "warning": row["warning"],
                "decided_at": row["decided_at"],
                "text": row["text"],
            }
            if row["checked_at"]:
                disposition_attrs["checked_at"] = row["checked_at"]
            facts.append(NodeFact(
                node_id=disposition_node,
                subtype="branch_disposition",
                attrs=disposition_attrs,
                source_record_id=disposition_record,
                source_revision=revision,
            ))

            descendants: list[RecordNode] = []
            edges: list[EdgeKey] = []
            if row["state"] == "archive_with_summary":
                summary_node = row["summary_id"]
                summary_record = {
                    "repo": row["repo"],
                    "branch_name": row["branch_name"],
                    "summary_id": row["summary_id"],
                }
                summary_attrs = {
                    "summary_id": row["summary_id"],
                    "repo": row["repo"],
                    "branch_name": row["branch_name"],
                    "title": row["summary_title"],
                    "summary": row["summary_body"],
                    "findings": row["summary_findings"],
                    "origin_session_id": row["origin_session_id"],
                    "disposition_id": row["disposition_id"],
                    "archived_at": row["decided_at"],
                    "text": row["text"],
                }
                facts.append(NodeFact(
                    node_id=summary_node,
                    subtype="branch_archive_summary",
                    attrs=summary_attrs,
                    source_record_id=summary_record,
                    source_revision=revision,
                ))
                edge_attrs = {
                    "relation": "archive_summary",
                    "branch_name": row["branch_name"],
                }
                facts.append(EdgeFact(
                    src=disposition_node,
                    dst=summary_node,
                    edge_type="references",
                    attrs=edge_attrs,
                    source_record_id=summary_record,
                    source_revision=revision,
                ))
                descendants.append(RecordNode(
                    node_id=summary_node,
                    subtype="branch_archive_summary",
                    source_record_id=summary_record,
                    source_revision=revision,
                ))
                edges.append(EdgeKey(
                    src=disposition_node,
                    dst=summary_node,
                    edge_type="references",
                    attrs=edge_attrs,
                    source_record_id=summary_record,
                    source_revision=revision,
                ))

            records[row["disposition_id"]] = RecordEmission(
                primary=RecordNode(
                    node_id=disposition_node,
                    subtype="branch_disposition",
                    source_record_id=disposition_record,
                    source_revision=revision,
                ),
                descendants=descendants,
                edges=edges,
            )

        self.last_run_stats = {
            "register_path": str(register_path),
            "rows_loaded": len(rows),
            "archived_count": sum(
                1 for row in rows if row["state"] == "archive_with_summary"
            ),
            "terminal_count": sum(1 for row in rows if row["terminal"]),
            "undispositioned_count": sum(
                1 for row in rows if row["state"] == "undispositioned"
            ),
            "facts_emitted": len(facts),
        }
        completed_scope = mode in {"reconcile", "scope_complete"}
        return SourceRun(
            facts=iter(facts),
            completed_scope=completed_scope,
            next_cursor=revision,
            applied_raw_token=self.change_probe.token_from_item_hashes(
                applied_item_hashes
            ),
            applied_token_scope="global",
            applied_token_authority="exact",
            run_mode=mode,
            record_set=RecordSet(records=records) if completed_scope else None,
        )


__all__ = [
    "BranchDispositionError",
    "BranchDispositionsSource",
    "NON_TERMINAL_DISPOSITIONS",
    "SUPPORTED_DISPOSITIONS",
    "TERMINAL_DISPOSITIONS",
    "default_register_path",
    "load_disposition_register",
    "normalize_disposition",
    "normalize_state",
    "slugify_branch_name",
]
