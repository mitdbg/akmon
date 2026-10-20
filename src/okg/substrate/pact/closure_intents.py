"""Durable local closure intents for ledger-unreachable PACT verbs.

Per `polish-pact-agent-ux` req.polish.deferred-closure: when the decision
ledger is unreachable, `gate task-done` and `evidence attach/ingest` must
not discard their coordination exhaust — they queue an intent here (under
the worktree-local `.okg/` boundary) and the next ledger-touching verb (or
`okg pact evidence sync`) replays it. This module is pure storage: queue,
list, mark-synced. Replay itself lives in operations (it needs the ledger
and manifest machinery).

Intent ids are deterministic over (change, kind, task, event_type,
payload), so re-queueing the same deferred action overwrites one file
instead of stacking, and a replayed intent moves to `synced/` so a second
sync finds nothing to write — one ledger record per intent.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

PENDING_DIRNAME = ".okg/pact-closure-intents/pending"
SYNCED_DIRNAME = ".okg/pact-closure-intents/synced"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def pending_dir(repo_root: Path) -> Path:
    return repo_root / PENDING_DIRNAME


def synced_dir(repo_root: Path) -> Path:
    return repo_root / SYNCED_DIRNAME


def intent_id(
    *,
    change_id: str,
    kind: str,
    task_id: str | None,
    event_type: str | None,
    payload: dict[str, Any] | None,
) -> str:
    digest = hashlib.sha256(
        json.dumps(
            {
                "change": change_id,
                "kind": kind,
                "task": task_id,
                "event_type": event_type,
                "payload": payload or {},
            },
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()[:16]
    return f"ci.{kind}.{digest}"


def queue_intent(
    repo_root: Path,
    *,
    change_id: str,
    kind: str,
    task_id: str | None = None,
    event_type: str | None = None,
    intent_digest: str | None = None,
    payload: dict[str, Any] | None = None,
    ledger_reason: str | None = None,
    ledger_authority: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record = {
        "intent_id": intent_id(
            change_id=change_id,
            kind=kind,
            task_id=task_id,
            event_type=event_type,
            payload=payload,
        ),
        "change": change_id,
        "kind": kind,
        "task": task_id,
        "event_type": event_type,
        "intent_digest": intent_digest,
        "payload": payload or {},
        "ledger_reason": ledger_reason,
        # The CLASSIFIED cause (and its documented fix) as the ledger reported
        # it, so a verb that replays or reports this intent derives the same
        # remedy the verb that queued it would have. A truncated reason string
        # alone forced every downstream reader to default.
        "ledger_authority": ledger_authority or {},
        "created_at": _now(),
    }
    directory = pending_dir(repo_root)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{record['intent_id']}.yaml"
    # Atomic write (tmp + rename): a torn intent file would poison replay.
    tmp = path.with_suffix(".yaml.tmp")
    tmp.write_text(yaml.safe_dump(record, sort_keys=False), encoding="utf-8")
    tmp.replace(path)
    record["path"] = str(path)
    return record


def pending_intents(
    repo_root: Path,
    change_id: str | None = None,
) -> list[dict[str, Any]]:
    directory = pending_dir(repo_root)
    if not directory.is_dir():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(directory.glob("ci.*.yaml")):
        try:
            record = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001 - a corrupt intent must not wedge the verb
            record = None
        if not isinstance(record, dict):
            # Surface the corruption instead of silently skipping it.
            records.append({
                "intent_id": path.stem,
                "change": None,
                "kind": "unreadable",
                "path": str(path),
                "error": "unreadable intent file",
            })
            continue
        if change_id is not None and record.get("change") != change_id:
            continue
        record["path"] = str(path)
        records.append(record)
    return records


def mark_synced(
    repo_root: Path,
    record: dict[str, Any],
    *,
    ledger_result: dict[str, Any] | None = None,
) -> None:
    source = Path(record["path"])
    directory = synced_dir(repo_root)
    directory.mkdir(parents=True, exist_ok=True)
    synced = {
        k: v for k, v in record.items() if k != "path"
    }
    synced["synced_at"] = _now()
    if ledger_result:
        synced["ledger_result"] = ledger_result
    (directory / source.name).write_text(
        yaml.safe_dump(synced, sort_keys=False), encoding="utf-8"
    )
    source.unlink(missing_ok=True)
