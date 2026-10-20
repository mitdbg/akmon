"""Retention for the Nomos decision log.

okg.nomos_decisions is an audit log with no natural bound: even after the
volatile-key identity fix (issue #635) stopped per-publish re-minting, new
subjects, policies, and access patterns keep appending. This module is the
single retention path: prune rows older than a documented age bound, never
touching a row any other table still references.

The write path uses ON CONFLICT DO NOTHING, so pruning a decision that later
recurs is safe: the next occurrence re-inserts it with a fresh created_at
(no unrecorded-decision window).
"""

from __future__ import annotations

import os

import psycopg

from okg.substrate.nomos.core import NomosPolicyError

#: Default age bound for pruning, in days. Operators override via
#: OKG_NOMOS_DECISION_RETENTION_DAYS (a positive integer; anything else is a
#: loud configuration error, never a silent fallback). Effectively-keep-all
#: is expressed as a very large value, not a disable switch.
DEFAULT_NOMOS_DECISION_RETENTION_DAYS = 90

_RETENTION_ENV = "OKG_NOMOS_DECISION_RETENTION_DAYS"

# Every table that can hold a reference to okg.nomos_decisions. The five FK
# children use ON DELETE RESTRICT, but retention must not lean on the
# constraint (a RESTRICT hit aborts the whole delete); the two activation
# tables reference decisions WITHOUT a foreign key, so only this predicate
# protects them.
_REFERENCING_TABLES: tuple[tuple[str, str, str], ...] = (
    ("federation_connection_requests", "nomos_decision_deployment_name",
     "nomos_decision_id"),
    ("federation_peer_trust_pins", "nomos_decision_deployment_name",
     "nomos_decision_id"),
    ("federation_route_events", "nomos_decision_deployment_name",
     "nomos_decision_id"),
    ("poros_incorporation_events", "nomos_decision_deployment_name",
     "nomos_decision_id"),
    ("nomos_external_claims", "deployment_name", "nomos_decision_id"),
    ("nomos_policy_activations", "deployment_name", "activation_decision_id"),
    ("nomos_config_activations", "deployment_name", "activation_decision_id"),
    # External-action review is a separate lifecycle, but its authenticated
    # audit, decisions, claims, outcomes, and restore evidence still pin the
    # Nomos decision that authorized them.  Keep those decisions until every
    # child reference is gone; relying only on the schema FK would make a
    # retention batch fail and roll back all unrelated pruning work.
    ("external_action_auth_audits", "deployment_name", "nomos_decision_id"),
    ("external_action_decisions", "deployment_name", "nomos_decision_id"),
    ("external_action_transition_idempotency", "deployment_name", "nomos_decision_id"),
    ("external_action_executions", "deployment_name", "nomos_decision_id"),
    ("external_action_outcomes", "deployment_name", "nomos_decision_id"),
    ("external_action_events", "deployment_name", "nomos_decision_ref"),
    ("external_action_owner_unavailability_attestations", "deployment_name", "nomos_decision_id"),
    ("external_action_execution_epoch_leases", "deployment_name", "nomos_decision_id"),
    ("external_action_restore_attestations", "deployment_name", "nomos_decision_id"),
    ("external_action_restore_sessions", "deployment_name", "nomos_decision_id"),
)


def nomos_decision_retention_days() -> int:
    """Resolve the retention bound; misconfiguration is a hard error."""
    raw = os.getenv(_RETENTION_ENV)
    if raw is None or not raw.strip():
        return DEFAULT_NOMOS_DECISION_RETENTION_DAYS
    try:
        days = int(raw.strip())
    except ValueError as exc:
        raise NomosPolicyError(
            f"{_RETENTION_ENV} must be a positive integer number of days; "
            f"got {raw!r}"
        ) from exc
    if days <= 0:
        raise NomosPolicyError(
            f"{_RETENTION_ENV} must be a positive integer number of days; "
            f"got {days} (retention cannot be disabled silently; use a "
            "large value to effectively keep all)"
        )
    return days


def _unreferenced_predicate() -> str:
    return " AND ".join(
        (
            "NOT EXISTS (SELECT 1 FROM okg.{table} r "
            "WHERE r.{dep_col} = d.deployment_name "
            "AND r.{id_col} = d.decision_id)"
        ).format(table=table, dep_col=dep_col, id_col=id_col)
        for table, dep_col, id_col in _REFERENCING_TABLES
    )


def prune_nomos_decisions(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    max_age_days: int | None = None,
    dry_run: bool = False,
) -> int:
    """Delete decision rows older than the bound that nothing references.

    Returns the number of rows deleted (or that would be deleted, when
    ``dry_run``). Raises ``NomosPolicyError`` on a misconfigured bound; never
    silently skips.
    """
    if max_age_days is None:
        max_age_days = nomos_decision_retention_days()
    if not isinstance(max_age_days, int) or max_age_days <= 0:
        raise NomosPolicyError(
            "prune_nomos_decisions: max_age_days must be a positive "
            f"integer; got {max_age_days!r}"
        )
    age_predicate = (
        "d.deployment_name = %(deployment_name)s "
        "AND d.created_at < now() - make_interval(days => %(days)s)"
    )
    params = {"deployment_name": deployment_name, "days": max_age_days}
    if dry_run:
        sql = (
            "SELECT count(*) FROM okg.nomos_decisions d "
            f"WHERE {age_predicate} AND {_unreferenced_predicate()}"
        )
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return int(cur.fetchone()[0])
    sql = (
        "DELETE FROM okg.nomos_decisions d "
        f"WHERE {age_predicate} AND {_unreferenced_predicate()}"
    )
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.rowcount or 0)
