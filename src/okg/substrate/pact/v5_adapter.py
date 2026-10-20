"""Read a PACT v5 manifest through the v4 model.

Every okg consumer of manifests (the graph projection source, corpus
hygiene, the review queue, the console views) is written against the v4
``PactManifest``. During the cutover both shapes live under ``pact/changes``;
this adapter lets the v4 readers see a v5 manifest as a conservative,
valid v4 view. It is read-only: nothing here writes, and the v5 ``pact check``
remains the only authority for a v5 change.
"""

from __future__ import annotations

from pathlib import PurePath
from typing import Any

V5_KIND_TO_V4 = {
    "executed_test": "executed_test",
    "formal_check": "formal_check",
    "published_generation": "published_generation",
    "design_review": "design_review",
    "manual": "manual",
}


def is_v5(raw: dict[str, Any]) -> bool:
    return raw.get("pact_version") == 5


def is_archived_path(source_path: PurePath) -> bool:
    """True when ``source_path`` sits under a ``pact/archive/`` directory.

    Matched on the adjacent pair so an unrelated directory merely named
    ``archive`` does not retire a live change.
    """
    parts = source_path.parts
    return any(
        parts[index] == "pact" and parts[index + 1] == "archive"
        for index in range(len(parts) - 1)
    )


#: The v4 authored `change.status` vocabulary has no terminal member: v4
#: itself projects every terminal lifecycle state (retired, superseded,
#: rejected) onto "verification" via `fsm.STATUS_PROJECTION`. So "terminal"
#: for an adapted v5 manifest means that same value, reached the same way.
_V4_TERMINAL_STATUS = "verification"


def _status(raw: dict[str, Any], *, archived: bool = False) -> str:
    # Derived, never authored: proven requirements move the change forward.
    proven = {
        row.get("requirement")
        for row in raw.get("evidence") or []
        if isinstance(row, dict) and row.get("status", "pass") == "pass" and row.get("head")
    }
    req_ids = [r.get("id") for r in raw.get("requirements") or [] if isinstance(r, dict)]
    if archived:
        # A manifest under pact/archive/ is finished work. Deriving its status
        # from evidence alone read a merged v5 change back as a fresh
        # "proposal", so the review surfaces showed settled changes as
        # in-flight (S6).
        return _V4_TERMINAL_STATUS
    if req_ids and all(r in proven for r in req_ids):
        return _V4_TERMINAL_STATUS
    if proven or raw.get("decisions"):
        return "implementation"
    return "proposal"


def to_v4(raw: dict[str, Any], *, archived: bool = False) -> dict[str, Any]:
    """Adapt a v5 manifest to the v4 shape.

    ``archived`` says the manifest was read from ``pact/archive/``; the caller
    knows the path, the manifest body does not.
    """
    evidence = []
    for index, row in enumerate(raw.get("evidence") or []):
        if not isinstance(row, dict):
            continue
        kind = V5_KIND_TO_V4.get(str(row.get("kind")), "manual")
        status = "pass" if row.get("status", "pass") == "pass" and row.get("head") else "fail"
        evidence.append(
            {
                "id": f"ev.v5.{index}",
                "kind": kind,
                "status": status,
                "refs": [row.get("requirement")],
                "artifact": row.get("artifact"),
                "command": row.get("command"),
                "summary": row.get("note"),
                "data": {
                    "pact_version": 5,
                    "head": row.get("head"),
                    "sha256": row.get("sha256"),
                    "attached_at": row.get("at"),
                },
            }
        )
    proven = {e["refs"][0] for e in evidence if e["status"] == "pass"}
    requirements = []
    for req in raw.get("requirements") or []:
        if not isinstance(req, dict):
            continue
        kinds = sorted({e["kind"] for e in evidence if e["refs"][0] == req.get("id") and e["status"] == "pass"})
        requirements.append(
            {
                "id": req.get("id"),
                "statement": req.get("statement") or "",
                # The policy mirrors the bound proof so a v4 reader sees a
                # done task as evidenced; an unproven requirement keeps the
                # v4 default and reads as planned.
                "evidence_policy": {"required": kinds or ["planned_test"]},
                "scenarios": [],
                "acceptance": [],
            }
        )
    tasks = []
    for task in raw.get("tasks") or []:
        if not isinstance(task, dict):
            continue
        req = task.get("requirement")
        tasks.append(
            {
                "id": task.get("id"),
                "title": str(task.get("verification") or task.get("id") or "")[:120],
                "status": "done" if req in proven else "todo",
                "verification": task.get("verification") or "",
                "requirements": [req] if req else [],
                "evidence": [e["id"] for e in evidence if e["refs"][0] == req and e["status"] == "pass"],
                "environment": [],
            }
        )
    why = str(raw.get("why") or "")
    rationale, _, root_cause = why.partition("\n\n")
    return {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {
            "id": raw.get("id"),
            "title": raw.get("title") or raw.get("id"),
            "status": _status(raw, archived=archived),
            "rationale": rationale or why or None,
            "root_cause": root_cause or None,
            "tier": raw.get("tier"),
            "affected_code": list(raw.get("affected_code") or []),
            "method_metadata": {
                "pact_version": 5,
                "archived": archived,
                "decisions": list(raw.get("decisions") or []),
            },
        },
        "requirements": requirements,
        "tasks": tasks,
        "dependencies": [],
        "test_plan": [],
        "formal": [],
        "waivers": [],
        "evidence": evidence,
    }
