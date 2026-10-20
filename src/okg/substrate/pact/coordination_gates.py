"""Coordination-backed PACT task gates.

The initial resolver reads an external state file so agents can prove
coordination progress without racing to edit pact.yaml. The same result shape
is intentionally graph-provider friendly: a future OKG read can supply the
same task/gate payload for a pinned generation.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import envelope


PASSING_STATUSES = {"ok", "pass", "passed", "satisfied", "true"}


def coordination_gate_state_path(repo_root: Path, change_id: str) -> Path:
    override = os.environ.get("OKG_PACT_COORDINATION_GATES_FILE")
    if override:
        return Path(override)
    return repo_root / ".pact" / "coordination-gates" / f"{change_id}.json"


def resolve_coordination_gates(repo_root: Path, change_id: str, task: Any) -> dict[str, Any]:
    required = list(getattr(task, "coordination_gates", []) or [])
    if not required:
        return envelope.component(
            ok=True,
            required=[],
            missing=[],
            details=[],
            source="none",
        )

    state_path = coordination_gate_state_path(repo_root, change_id)
    payload = _load_payload(state_path)
    task_payload = _task_payload(payload, task.id)
    actual_generation = _generation_id(payload, task_payload)
    expected_generation = getattr(task, "coordination_generation_id", None)

    details: list[dict[str, Any]] = []
    missing: list[str] = []
    gates = task_payload.get("gates", {}) if isinstance(task_payload, dict) else {}
    if not isinstance(gates, dict):
        gates = {}

    for gate in required:
        raw = gates.get(gate)
        detail = _gate_detail(
            gate,
            raw,
            state_path=state_path,
            expected_generation=expected_generation,
            actual_generation=actual_generation,
        )
        details.append(detail)
        if detail["status"] != "satisfied":
            missing.append(f"coordination_gate:{gate}")

    return envelope.component(
        ok=not missing,
        required=required,
        missing=missing,
        details=details,
        source="external_state",
        state_path=str(state_path),
        generation_id=actual_generation,
        expected_generation_id=expected_generation,
    )


def _load_payload(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SubstrateError(
            "invalid coordination gate state JSON",
            data={"path": str(path), "error": str(exc)},
        ) from exc
    if not isinstance(raw, dict):
        raise SubstrateError(
            "coordination gate state must be a JSON object",
            data={"path": str(path)},
        )
    return raw


def _task_payload(payload: dict[str, Any], task_id: str) -> dict[str, Any]:
    tasks = payload.get("tasks")
    if isinstance(tasks, dict):
        raw = tasks.get(task_id)
        if isinstance(raw, dict):
            return raw
    raw = payload.get(task_id)
    if isinstance(raw, dict):
        return raw
    return {}


def _generation_id(
    payload: dict[str, Any],
    task_payload: dict[str, Any],
) -> int | None:
    raw = task_payload.get("generation_id", payload.get("generation_id"))
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _gate_detail(
    gate: str,
    raw: Any,
    *,
    state_path: Path,
    expected_generation: int | None,
    actual_generation: int | None,
) -> dict[str, Any]:
    if expected_generation is not None and actual_generation != expected_generation:
        return {
            "gate": gate,
            "status": "stale_generation",
            "expected_generation_id": expected_generation,
            "generation_id": actual_generation,
            "source": str(state_path),
        }
    if raw is True:
        return {"gate": gate, "status": "satisfied", "source": str(state_path)}
    if raw is False or raw is None:
        return {"gate": gate, "status": "missing", "source": str(state_path)}
    if isinstance(raw, str):
        status = "satisfied" if raw.lower() in PASSING_STATUSES else raw
        return {"gate": gate, "status": status, "source": str(state_path)}
    if not isinstance(raw, dict):
        return {"gate": gate, "status": "invalid", "source": str(state_path)}

    status = str(raw.get("status") or "").lower()
    fresh = raw.get("fresh", True)
    if status in PASSING_STATUSES and fresh is not False:
        status = "satisfied"
    elif fresh is False:
        status = "stale"
    elif not status:
        status = "missing"
    detail = {
        "gate": gate,
        "status": status,
        "source": str(state_path),
    }
    for key in ("evidence_ref", "summary", "generation_id", "revision"):
        if key in raw:
            detail[key] = raw[key]
    return detail
