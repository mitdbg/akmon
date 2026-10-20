"""Deterministic SQL dirty-rule execution for derived artifacts.

This is the first dirty-rule executor slice. It is deliberately
relation-backed and SQL-only: callers declare an independent scope
universe query plus a dirty-scope query. The executor persists a
dirty-rule run, records the derived dirty scopes, fails closed to the
full universe when the rule cannot prove completeness, and can run a
deterministic missed-dirty audit query.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate.catalog.invariants import sql_contains_writes
from okg.substrate.derived_artifacts.relations import (
    DirtyScope,
    RelationArtifactDefinition,
    enqueue_dirty_scope,
    register_jsonb_relation,
    stable_hash,
)
from okg.substrate.errors import SubstrateError


DEFAULT_DIRTY_RULE_TIMEOUT_MS = 5000


class DirtyRuleLoadError(SubstrateError):
    code = "derived_dirty_rule_load_error"


class DirtyRuleExecutionError(SubstrateError):
    code = "derived_dirty_rule_execution_error"


@dataclass(frozen=True)
class SqlDirtyRuleSpec:
    """One constrained SQL dirty rule for a relation artifact."""

    definition: RelationArtifactDefinition
    scope_universe_sql: str
    dirty_scope_sql: str
    missed_dirty_audit_sql: str | None = None
    timeout_ms: int = DEFAULT_DIRTY_RULE_TIMEOUT_MS
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DirtyRuleRunResult:
    """Durable result of one dirty-rule executor pass."""

    dirty_rule_run_id: UUID
    status: str
    fallback_kind: str
    target_generation_id: int
    total_scope_count: int
    dirty_scope_count: int
    fallback_scope_count: int
    missed_scope_count: int
    dirty_scope_keys: list[str]
    elapsed_ms: int


def execute_sql_dirty_rule(
    conn: psycopg.Connection[Any],
    spec: SqlDirtyRuleSpec,
    *,
    target_generation_id: int,
    catalog_version_id: int | None = None,
    deployment_name: str | None = None,
) -> DirtyRuleRunResult:
    """Derive and persist dirty scopes from a read-only SQL rule."""

    _validate_spec(spec)
    register_jsonb_relation(conn, spec.definition)
    scope_universe_hash = stable_hash(
        {
            "kind": "scope_universe_sql_v1",
            "sql": spec.scope_universe_sql,
        }
    )
    dirty_query_hash = stable_hash(
        {
            "kind": "dirty_scope_sql_v1",
            "sql": spec.dirty_scope_sql,
        }
    )
    audit_query_hash = (
        None if spec.missed_dirty_audit_sql is None
        else stable_hash(
            {
                "kind": "missed_dirty_audit_sql_v1",
                "sql": spec.missed_dirty_audit_sql,
            }
        )
    )
    dirty_rule_run_id = _start_dirty_rule_run(
        conn,
        spec,
        target_generation_id=target_generation_id,
        scope_universe_hash=scope_universe_hash,
        dirty_query_hash=dirty_query_hash,
        audit_query_hash=audit_query_hash,
    )

    started = time.monotonic()
    params = {
        "target_generation_id": target_generation_id,
        "catalog_version_id": catalog_version_id,
        "deployment_name": deployment_name or spec.definition.deployment_name,
        "definition_id": spec.definition.definition_id,
        "dirty_rule_hash": spec.definition.dirty_rule_hash,
    }
    metrics: dict[str, Any] = {
        "metadata": dict(spec.metadata),
        "timeout_ms": spec.timeout_ms,
    }
    try:
        universe_rows = _fetch_rows(
            conn,
            spec.scope_universe_sql,
            params,
            timeout_ms=spec.timeout_ms,
        )
        universe_keys = sorted(
            set(_scope_keys_from_rows(universe_rows, where="scope_universe_sql"))
        )
        universe_set = set(universe_keys)
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        _fail_dirty_rule_run(conn, dirty_rule_run_id, exc, elapsed_ms)
        raise DirtyRuleExecutionError(
            "scope_universe_sql failed; cannot derive a safe dirty set",
            data={
                "definition_id": spec.definition.definition_id,
                "target_generation_id": target_generation_id,
            },
        ) from exc

    fallback_kind = "none"
    fallback_reason: str | None = None
    dirty_events: list[DirtyScope]
    dirty_rows: list[dict[str, Any]] = []
    try:
        dirty_rows = _fetch_rows(
            conn,
            spec.dirty_scope_sql,
            params,
            timeout_ms=spec.timeout_ms,
        )
        dirty_events, incomplete_reason = _dirty_events_from_rows(
            dirty_rows,
            default_dependency_ref=dirty_query_hash,
            dirty_rule_run_id=dirty_rule_run_id,
        )
        out_of_universe = sorted(
            {event.scope_key for event in dirty_events} - universe_set
        )
        if incomplete_reason is not None:
            fallback_reason = incomplete_reason
        elif out_of_universe:
            fallback_reason = "dirty_scope_outside_scope_universe"
            metrics["out_of_universe_scope_keys"] = out_of_universe
    except Exception as exc:
        dirty_events = []
        fallback_reason = "dirty_scope_sql_error"
        metrics["dirty_scope_sql_error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }

    if fallback_reason is not None:
        fallback_kind = "full_scope_universe"
        dirty_events = [
            DirtyScope(
                scope_key=scope_key,
                dirty_reason="dirty_rule_fallback",
                dependency_kind="dirty_rule_fallback",
                dependency_ref=dirty_query_hash,
                dependency_hash=spec.definition.dirty_rule_hash,
                dirty_rule_run_id=dirty_rule_run_id,
            )
            for scope_key in universe_keys
        ]
        metrics["fallback_reason"] = fallback_reason

    dirty_scope_keys_before_audit = {
        event.scope_key for event in dirty_events
    }
    missed_scope_keys: list[str] = []
    if spec.missed_dirty_audit_sql is None:
        _record_dirty_rule_audit(
            conn,
            dirty_rule_run_id=dirty_rule_run_id,
            definition=spec.definition,
            target_generation_id=target_generation_id,
            status="skipped",
            expected_scope_count=len(universe_keys),
            observed_scope_count=len(dirty_scope_keys_before_audit),
            missed_scope_keys=[],
            metadata={"reason": "no_missed_dirty_audit_sql"},
        )
    elif fallback_kind != "none":
        _record_dirty_rule_audit(
            conn,
            dirty_rule_run_id=dirty_rule_run_id,
            definition=spec.definition,
            target_generation_id=target_generation_id,
            status="skipped",
            expected_scope_count=len(universe_keys),
            observed_scope_count=len(dirty_scope_keys_before_audit),
            missed_scope_keys=[],
            metadata={"reason": "full_scope_universe_fallback"},
        )
    else:
        audit_rows = _fetch_rows(
            conn,
            spec.missed_dirty_audit_sql,
            params,
            timeout_ms=spec.timeout_ms,
        )
        audit_scope_keys = set(
            _scope_keys_from_rows(audit_rows, where="missed_dirty_audit_sql")
        )
        missed_scope_keys = sorted(
            (audit_scope_keys & universe_set) - dirty_scope_keys_before_audit
        )
        for scope_key in missed_scope_keys:
            dirty_events.append(
                DirtyScope(
                    scope_key=scope_key,
                    dirty_reason="missed_dirty_audit",
                    dependency_kind="missed_dirty_audit",
                    dependency_ref=audit_query_hash or "missed_dirty_audit",
                    dependency_hash=spec.definition.dirty_rule_hash,
                    dirty_rule_run_id=dirty_rule_run_id,
                )
            )
        _record_dirty_rule_audit(
            conn,
            dirty_rule_run_id=dirty_rule_run_id,
            definition=spec.definition,
            target_generation_id=target_generation_id,
            status="failed" if missed_scope_keys else "passed",
            expected_scope_count=len(audit_scope_keys & universe_set),
            observed_scope_count=len(dirty_scope_keys_before_audit),
            missed_scope_keys=missed_scope_keys,
            metadata={
                "audit_row_count": len(audit_rows),
                "audit_out_of_universe_scope_keys": sorted(
                    audit_scope_keys - universe_set
                ),
            },
        )

    _persist_dirty_events(
        conn,
        spec.definition,
        target_generation_id=target_generation_id,
        dirty_events=dirty_events,
    )
    dirty_scope_keys = sorted({event.scope_key for event in dirty_events})
    elapsed_ms = int((time.monotonic() - started) * 1000)
    status = "audit_failed" if missed_scope_keys else "completed"
    metrics.update(
        {
            "scope_universe_row_count": len(universe_rows),
            "dirty_query_row_count": len(dirty_rows),
            "dirty_scope_keys_before_audit": sorted(
                dirty_scope_keys_before_audit
            ),
            "missed_scope_keys": missed_scope_keys,
        }
    )
    _complete_dirty_rule_run(
        conn,
        dirty_rule_run_id,
        status=status,
        fallback_kind=fallback_kind,
        total_scope_count=len(universe_keys),
        dirty_scope_count=len(dirty_scope_keys),
        fallback_scope_count=(
            len(universe_keys) if fallback_kind == "full_scope_universe" else 0
        ),
        missed_scope_count=len(missed_scope_keys),
        elapsed_ms=elapsed_ms,
        metrics=metrics,
    )
    return DirtyRuleRunResult(
        dirty_rule_run_id=dirty_rule_run_id,
        status=status,
        fallback_kind=fallback_kind,
        target_generation_id=target_generation_id,
        total_scope_count=len(universe_keys),
        dirty_scope_count=len(dirty_scope_keys),
        fallback_scope_count=(
            len(universe_keys) if fallback_kind == "full_scope_universe" else 0
        ),
        missed_scope_count=len(missed_scope_keys),
        dirty_scope_keys=dirty_scope_keys,
        elapsed_ms=elapsed_ms,
    )


def _validate_spec(spec: SqlDirtyRuleSpec) -> None:
    if spec.timeout_ms <= 0:
        raise DirtyRuleLoadError(
            "dirty-rule timeout_ms must be positive",
            data={"timeout_ms": spec.timeout_ms},
        )
    _reject_write_sql(
        spec.scope_universe_sql,
        field="scope_universe_sql",
    )
    _reject_write_sql(spec.dirty_scope_sql, field="dirty_scope_sql")
    if spec.missed_dirty_audit_sql is not None:
        _reject_write_sql(
            spec.missed_dirty_audit_sql,
            field="missed_dirty_audit_sql",
        )


def _reject_write_sql(sql: str, *, field: str) -> None:
    hit = sql_contains_writes(sql)
    if hit is not None:
        raise DirtyRuleLoadError(
            f"{field} contains write-shaped SQL token {hit!r}",
            data={"field": field, "token": hit},
            fix_hint="dirty-rule SQL must be read-only SELECT SQL",
        )


def _fetch_rows(
    conn: psycopg.Connection[Any],
    sql: str,
    params: Mapping[str, Any],
    *,
    timeout_ms: int,
) -> list[dict[str, Any]]:
    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
            cur.execute(sql, dict(params))
            return [dict(row) for row in cur.fetchall()]


def _scope_keys_from_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    where: str,
) -> list[str]:
    scope_keys: list[str] = []
    for row in rows:
        raw = row.get("scope_key")
        if raw is None:
            raise DirtyRuleExecutionError(
                f"{where} must return a non-null scope_key column",
                data={"where": where, "row": dict(row)},
            )
        scope_key = str(raw)
        if not scope_key:
            raise DirtyRuleExecutionError(
                f"{where} returned an empty scope_key",
                data={"where": where, "row": dict(row)},
            )
        scope_keys.append(scope_key)
    return scope_keys


def _dirty_events_from_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    default_dependency_ref: str,
    dirty_rule_run_id: UUID,
) -> tuple[list[DirtyScope], str | None]:
    events: list[DirtyScope] = []
    incomplete_reason: str | None = None
    for row in rows:
        complete_value = row.get(
            "dirty_complete",
            row.get("coverage_complete", True),
        )
        if not _is_truthy(complete_value):
            incomplete_reason = str(
                row.get("incomplete_reason")
                or "dirty_scope_coverage_incomplete"
            )
        scope_key = _scope_keys_from_rows([row], where="dirty_scope_sql")[0]
        events.append(
            DirtyScope(
                scope_key=scope_key,
                dirty_reason=str(row.get("dirty_reason") or "source_changed"),
                dependency_kind=str(row.get("dependency_kind") or "source_fact"),
                dependency_ref=str(
                    row.get("dependency_ref") or default_dependency_ref
                ),
                dependency_hash=(
                    None if row.get("dependency_hash") is None
                    else str(row.get("dependency_hash"))
                ),
                dirty_rule_run_id=dirty_rule_run_id,
            )
        )
    return events, incomplete_reason


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"true", "t", "1", "yes", "y"}


def _start_dirty_rule_run(
    conn: psycopg.Connection[Any],
    spec: SqlDirtyRuleSpec,
    *,
    target_generation_id: int,
    scope_universe_hash: str,
    dirty_query_hash: str,
    audit_query_hash: str | None,
) -> UUID:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.derived_dirty_rule_runs (
                  definition_id, target_generation_id, dirty_rule_hash,
                  scope_universe_hash, dirty_query_hash, audit_query_hash,
                  status, metrics
                ) VALUES (
                  %(definition_id)s, %(target_generation_id)s,
                  %(dirty_rule_hash)s, %(scope_universe_hash)s,
                  %(dirty_query_hash)s, %(audit_query_hash)s,
                  'running', %(metrics)s
                )
                RETURNING dirty_rule_run_id
                """,
                {
                    "definition_id": spec.definition.definition_id,
                    "target_generation_id": target_generation_id,
                    "dirty_rule_hash": spec.definition.dirty_rule_hash,
                    "scope_universe_hash": scope_universe_hash,
                    "dirty_query_hash": dirty_query_hash,
                    "audit_query_hash": audit_query_hash,
                    "metrics": Jsonb({"metadata": dict(spec.metadata)}),
                },
            )
            (dirty_rule_run_id,) = cur.fetchone()
            return dirty_rule_run_id


def _persist_dirty_events(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
    *,
    target_generation_id: int,
    dirty_events: Sequence[DirtyScope],
) -> None:
    for event in dirty_events:
        enqueue_dirty_scope(
            conn,
            definition,
            target_generation_id=target_generation_id,
            dirty_scope=event,
        )


def _record_dirty_rule_audit(
    conn: psycopg.Connection[Any],
    *,
    dirty_rule_run_id: UUID,
    definition: RelationArtifactDefinition,
    target_generation_id: int,
    status: str,
    expected_scope_count: int,
    observed_scope_count: int,
    missed_scope_keys: Sequence[str],
    metadata: Mapping[str, Any],
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.derived_dirty_rule_audits (
                  dirty_rule_run_id, definition_id, target_generation_id,
                  audit_kind, status, expected_scope_count,
                  observed_scope_count, missed_scope_count,
                  missed_scope_keys, metadata
                ) VALUES (
                  %(dirty_rule_run_id)s, %(definition_id)s,
                  %(target_generation_id)s, 'missed_dirty',
                  %(status)s, %(expected_scope_count)s,
                  %(observed_scope_count)s, %(missed_scope_count)s,
                  %(missed_scope_keys)s, %(metadata)s
                )
                """,
                {
                    "dirty_rule_run_id": dirty_rule_run_id,
                    "definition_id": definition.definition_id,
                    "target_generation_id": target_generation_id,
                    "status": status,
                    "expected_scope_count": expected_scope_count,
                    "observed_scope_count": observed_scope_count,
                    "missed_scope_count": len(missed_scope_keys),
                    "missed_scope_keys": Jsonb(list(missed_scope_keys)),
                    "metadata": Jsonb(dict(metadata)),
                },
            )


def _complete_dirty_rule_run(
    conn: psycopg.Connection[Any],
    dirty_rule_run_id: UUID,
    *,
    status: str,
    fallback_kind: str,
    total_scope_count: int,
    dirty_scope_count: int,
    fallback_scope_count: int,
    missed_scope_count: int,
    elapsed_ms: int,
    metrics: Mapping[str, Any],
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_dirty_rule_runs
                   SET status = %(status)s,
                       fallback_kind = %(fallback_kind)s,
                       total_scope_count = %(total_scope_count)s,
                       dirty_scope_count = %(dirty_scope_count)s,
                       fallback_scope_count = %(fallback_scope_count)s,
                       missed_scope_count = %(missed_scope_count)s,
                       elapsed_ms = %(elapsed_ms)s,
                       metrics = %(metrics)s,
                       completed_at = now()
                 WHERE dirty_rule_run_id = %(dirty_rule_run_id)s
                """,
                {
                    "dirty_rule_run_id": dirty_rule_run_id,
                    "status": status,
                    "fallback_kind": fallback_kind,
                    "total_scope_count": total_scope_count,
                    "dirty_scope_count": dirty_scope_count,
                    "fallback_scope_count": fallback_scope_count,
                    "missed_scope_count": missed_scope_count,
                    "elapsed_ms": elapsed_ms,
                    "metrics": Jsonb(dict(metrics)),
                },
            )


def _fail_dirty_rule_run(
    conn: psycopg.Connection[Any],
    dirty_rule_run_id: UUID,
    exc: Exception,
    elapsed_ms: int,
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_dirty_rule_runs
                   SET status = 'failed',
                       elapsed_ms = %(elapsed_ms)s,
                       error = %(error)s,
                       completed_at = now()
                 WHERE dirty_rule_run_id = %(dirty_rule_run_id)s
                """,
                {
                    "dirty_rule_run_id": dirty_rule_run_id,
                    "elapsed_ms": elapsed_ms,
                    "error": Jsonb(
                        {
                            "type": type(exc).__name__,
                            "message": str(exc),
                        }
                    ),
                },
            )
