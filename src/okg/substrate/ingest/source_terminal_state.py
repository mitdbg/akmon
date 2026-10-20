"""Transactional source terminal-state evidence and persistence."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

import psycopg

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.runner import (
    persist_source_watermark,
    validate_adapter_source_progress,
)


def source_terminal_state_evidence(
    *,
    next_cursor: Mapping[str, Any] | None,
    observed_at: datetime | None,
    completed_scope: bool,
    deployment: str,
    source_progress: Mapping[str, str] | None,
    clear_replay_required: bool = False,
    replay_target: Mapping[str, Any] | None = None,
    expected_last_applied_rev: Mapping[str, Any] | None = None,
    watermark_compare_and_swap: bool = False,
    expected_bootstrap_ledger: Mapping[str, Any] | None = None,
    next_bootstrap_ledger: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    if observed_at is None:
        return None
    out = {
        "next_cursor": None if next_cursor is None else dict(next_cursor),
        "observed_at": observed_at.isoformat(),
        "completed_scope": bool(completed_scope),
        "deployment": deployment,
        "source_progress": {
            str(key): str(value)
            for key, value in (source_progress or {}).items()
        },
    }
    if clear_replay_required or isinstance(replay_target, Mapping):
        out["clear_replay_required"] = bool(clear_replay_required)
        out["replay_target"] = (
            dict(replay_target)
            if isinstance(replay_target, Mapping)
            else None
        )
    if watermark_compare_and_swap:
        out["watermark_compare_and_swap"] = True
        out["expected_last_applied_rev"] = dict(
            expected_last_applied_rev or {}
        )
        out["expected_bootstrap_ledger"] = (
            dict(expected_bootstrap_ledger)
            if expected_bootstrap_ledger is not None
            else None
        )
        if next_bootstrap_ledger is None:
            raise SubstrateError(
                "bootstrap CAS terminal state has no ledger transition",
                data={"deployment": deployment},
            )
        out["next_bootstrap_ledger"] = dict(next_bootstrap_ledger)
    return out


def persist_accepted_source_terminal_state(
    conn: psycopg.Connection,
    *,
    source: str,
    terminal_state: Mapping[str, Any] | None,
) -> None:
    """Commit terminal source state in the envelope-acceptance transaction."""
    if terminal_state is None:
        return
    raw_observed_at = terminal_state.get("observed_at")
    if not isinstance(raw_observed_at, str):
        raise SubstrateError(
            "source terminal state is missing observed_at",
            data={"source": source},
        )
    observed_at = datetime.fromisoformat(raw_observed_at)
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise SubstrateError(
            "source terminal state observed_at must be timezone-aware",
            data={"source": source, "observed_at": raw_observed_at},
        )
    raw_cursor = terminal_state.get("next_cursor")
    if raw_cursor is not None and not isinstance(raw_cursor, Mapping):
        raise SubstrateError(
            "source terminal state next_cursor must be a mapping",
            data={"source": source},
        )
    raw_progress = terminal_state.get("source_progress") or {}
    deployment = terminal_state.get("deployment")
    terminal_progress: dict[str, str] = {}
    if raw_progress:
        if not isinstance(deployment, str):
            raise SubstrateError(
                "source terminal progress evidence is malformed",
                data={"source": source},
            )
        terminal_progress = validate_adapter_source_progress(
            raw_progress,
            source_name=source,
        )
    persist_source_watermark(
        conn,
        source_name=source,
        deployment_name=(
            str(terminal_state["deployment"])
            if terminal_state.get("deployment")
            else None
        ),
        next_cursor=raw_cursor,
        last_reconcile=(
            observed_at
            if bool(terminal_state.get("completed_scope"))
            else None
        ),
        clear_replay_required=bool(
            terminal_state.get("clear_replay_required")
        ),
        replay_target_fingerprint=(
            str(replay_target.get("target_fingerprint"))
            if isinstance(
                (replay_target := terminal_state.get("replay_target")),
                Mapping,
            )
            and replay_target.get("target_fingerprint")
            else None
        ),
        replay_target_run_id=(
            str(replay_target.get("run_id"))
            if isinstance(replay_target, Mapping)
            and replay_target.get("run_id")
            else None
        ),
        expected_last_applied_rev=(
            terminal_state.get("expected_last_applied_rev")
            if isinstance(
                terminal_state.get("expected_last_applied_rev"),
                Mapping,
            )
            else None
        ),
        require_compare_and_swap=bool(
            terminal_state.get("watermark_compare_and_swap")
        ),
        expected_bootstrap_ledger=(
            terminal_state.get("expected_bootstrap_ledger")
            if isinstance(
                terminal_state.get("expected_bootstrap_ledger"),
                Mapping,
            )
            else None
        ),
        next_bootstrap_ledger=(
            terminal_state.get("next_bootstrap_ledger")
            if isinstance(
                terminal_state.get("next_bootstrap_ledger"),
                Mapping,
            )
            else None
        ),
    )
    if terminal_progress:
        assert isinstance(deployment, str)
        rows = [
            (deployment, source, key, value)
            for key, value in terminal_progress.items()
        ]
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO okg.source_progress
                  (deployment, source_name, key, value, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (deployment, source_name, key)
                  DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = EXCLUDED.updated_at
                """,
                rows,
            )
