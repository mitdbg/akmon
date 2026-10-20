"""Legacy-SQL vs IR-check equivalence harness (consistency-checks-as-ir).

FROZEN — retirement blocked cross-repo; do NOT delete this module. It is the
SQL-vs-IR migration differ named by ``sql_gate.SQL_CHECK_FREEZE_POINTER`` and by
``deployment_lint``'s frozen-SQL findings, and it is imported by the cern-twiki
parity witnesses (``tests/substrate/consistency/test_equivalence_twiki.py`` and
``test_equivalence_twiki_structure.py``). It dies WITH the legacy SQL gate path,
whose deletion is gated on the cross-repo cern-twiki IR re-authoring (PACT
``consistency-checks-as-ir`` task ``retire-sql-path`` /
``consistency-deployment-currency`` task ``dep.twiki-ir-reauthor``, currently
blocked). ``consistency-single-engine`` (retirement-inventory item #2) owns this
deletion and freezes the module until that external gate opens — the three-way
differ (``consistency/equivalence_differ.py``) is the repo's LIVE equivalence
surface, and this SQL-vs-SQL harness exists only to prove parity before a
deployment's ``deterministic_sql`` check is retired. Delete this module (and its
twiki witnesses) only once ``FROZEN_SQL_CHECK_ALLOWLIST`` empties and no fleet
manifest declares a ``deterministic_sql`` check.

Fleet-migration tool: before a deployment's ``deterministic_sql`` check is
frozen and retired, its IR re-authoring must be proven to produce the SAME
finding set on the same generation. Both sides are EXECUTED SQL — the legacy
check runs through the real SqlGate engine (its verbatim ``candidate_sql``,
scope staging included) and its findings are read back from
``okg.observation_events``; the IR programs run through the canonical
``lower_query`` lowering on the same connection. The comparison is therefore
SQL-to-SQL, never interpreter-to-SQL.

A legacy check typically UNION ALLs several predicate arms into one statement;
its IR re-authoring is one program PER PREDICATE (the claim-model
decomposition). The harness unions the IR finding sets before comparing.

Findings are keyed by ``(input_scope, normalized_predicate, canonical
normalized_values)`` — the observation identity triple. The IR side's
``normalized_values`` and ``participant_roles`` are RECONSTRUCTED from the
program's ``normalized_value_columns`` / ``participant_roles`` attrs exactly as
``IrPostPublishCheck._emit`` persists them (see ``ir_findings_from_sql``), so the
diff compares the STORED observation the executor writes, not the raw SQL
projection. On top of the identity match, the compared columns default to
``predicate_key``, ``severity``, ``participant_roles`` and the reviewer-triage
columns ``payload`` and ``evidence``: a re-authoring that fires on the same
subjects but drops a participant role or the legacy issue_subtype /
precision_reason / review_action / evidence_refs context is a fidelity
divergence, surfaced as an ``AttributeMismatch``, not a silent pass. Differences
that are expected BY DESIGN are reported as ``explained``, not as divergence:

- ``finding_event='resolved'`` rows on the legacy side. Legacy checks re-emit
  resolutions through a ``resolved_existing`` arm reading
  ``okg.observation_current``; the IR executor derives resolutions ENGINE-SIDE
  (``ir_executor._resolutions``), so IR candidate programs never emit them.
  Reason: ``engine_side_resolutions``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row

from .ir_check import IrConsistencyCheckSpec
from .ir_executor import _normalized_values_for_row

FindingKey = tuple[str, str, str]

ENGINE_SIDE_RESOLUTIONS = "engine_side_resolutions"


def _canonical(value: Any) -> str:
    """Stable text form for jsonb-ish values (dict key order, numerics)."""

    def default(o: Any) -> Any:
        return str(o)

    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=default)


def finding_key(row: Mapping[str, Any]) -> FindingKey:
    return (
        str(row["input_scope"]),
        str(row["normalized_predicate"]),
        _canonical(row.get("normalized_values")),
    )


@dataclass(frozen=True)
class AttributeMismatch:
    """Same finding identity on both sides, but a compared column differs."""

    key: FindingKey
    column: str
    legacy_value: Any
    ir_value: Any


@dataclass(frozen=True)
class EquivalenceReport:
    matched: int
    legacy_only: tuple[dict[str, Any], ...]
    ir_only: tuple[dict[str, Any], ...]
    explained: tuple[tuple[dict[str, Any], str], ...]
    attribute_mismatches: tuple[AttributeMismatch, ...] = field(default=())

    @property
    def equivalent(self) -> bool:
        return not self.legacy_only and not self.ir_only and not self.attribute_mismatches

    def offending_input_scopes(self) -> dict[str, list[str]]:
        """The divergent observation identities, by side — the report a seeded
        divergence must surface."""
        return {
            "legacy_only": sorted(str(r["input_scope"]) for r in self.legacy_only),
            "ir_only": sorted(str(r["input_scope"]) for r in self.ir_only),
            "attribute_mismatch": sorted({m.key[0] for m in self.attribute_mismatches}),
        }


def legacy_findings_from_store(
    conn: psycopg.Connection[Any],
    *,
    check_id: str,
    target_generation_id: int,
) -> list[dict[str, Any]]:
    """The legacy engine run's finding rows, read back from the observation
    store (the engine executed the verbatim candidate_sql to produce them).

    The finding set active AS OF a generation is the currently-OPEN findings
    (read from ``observation_current``) plus the resolutions stamped at that
    generation. It is deliberately NOT ``observation_events WHERE
    target_generation_id = gen``: bounded-event-growth
    (consistency-framework-soundness, task.growth.events) makes an unchanged
    still-open finding stop re-stamping an event every generation, so a
    generation-filtered event read under-reports carried-forward open
    findings. ``observation_current`` is the durable active set and is the
    correct oracle for "what findings hold after this publish"."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT input_scope, predicate_key, normalized_predicate,
                   normalized_values, participant_roles,
                   'observed'::text AS finding_event,
                   severity, evidence, payload
              FROM okg.observation_current
             WHERE check_id = %(check_id)s
               AND finding_status = 'open'
               AND latest_generation_id <= %(generation)s
            UNION ALL
            SELECT input_scope, predicate_key, normalized_predicate,
                   normalized_values, participant_roles, finding_event,
                   severity, evidence, payload
              FROM okg.observation_events
             WHERE check_id = %(check_id)s
               AND target_generation_id = %(generation)s
               AND finding_event = 'resolved'
            """,
            {"check_id": check_id, "generation": target_generation_id},
        )
        return [dict(row) for row in cur.fetchall()]


def _stored_normalized_values(
    row: Mapping[str, Any], check_attrs: Mapping[str, Any], *, check_id: str
) -> dict[str, Any]:
    """The normalized_values IrPostPublishCheck._emit persists for this row:
    EXACTLY the declared ``normalized_value_columns``, computed by the
    executor's own ``_normalized_values_for_row`` so the two sides cannot
    drift. There is no projection-derived fallback on either side (#691; PACT
    consistency-finding-identity): an undeclared contract raises the same typed
    ``UndeclaredFindingIdentity`` refusal the executor raises, surfaced here
    rather than masked with a divergent identity."""
    return _normalized_values_for_row(
        row, check_attrs.get("normalized_value_columns"), check_id=check_id
    )


def _stored_participant_roles(
    row: Mapping[str, Any], check_attrs: Mapping[str, Any]
) -> dict[str, str]:
    """The participant_roles the executor persists: role -> str(row[column])."""
    return {
        str(role): str(row[column])
        for role, column in check_attrs["participant_roles"].items()
    }


def ir_findings_from_sql(
    conn: psycopg.Connection[Any],
    specs: Iterable[IrConsistencyCheckSpec],
) -> list[dict[str, Any]]:
    """Union of the IR re-authorings' finding rows — each program lowered to
    SQL and executed read-only on the same connection.

    Each row's ``normalized_values`` and ``participant_roles`` are RECONSTRUCTED
    from the program's ``normalized_value_columns`` / ``participant_roles`` attrs
    exactly as ``IrPostPublishCheck._emit`` persists them, so the harness diffs
    the STORED observation identity — not the raw SQL projection, which the
    executor does not store verbatim (it recomputes input_scope and keys
    normalized_values off the declared columns)."""
    rows: list[dict[str, Any]] = []
    for spec in specs:
        check_attrs = spec.program.attrs["check"]
        for raw in spec.run_rows(conn):
            row = dict(raw)
            row["normalized_values"] = _stored_normalized_values(
                row, check_attrs, check_id=spec.check_id
            )
            row["participant_roles"] = _stored_participant_roles(row, check_attrs)
            rows.append(row)
    return rows


def compare_finding_sets(
    legacy_rows: Sequence[Mapping[str, Any]],
    ir_rows: Sequence[Mapping[str, Any]],
    *,
    compare_columns: Sequence[str] = (
        "predicate_key", "severity", "participant_roles", "payload", "evidence",
    ),
) -> EquivalenceReport:
    """Diff the two finding sets by observation identity.

    ``compare_columns`` are diffed on every matched finding and default to the
    predicate identity (``predicate_key``, ``severity``, ``participant_roles``)
    PLUS the reviewer-triage columns (``payload``, ``evidence``) — the legacy
    candidate_sql projects those onto each finding and the IR re-authoring must
    reproduce them byte-for-byte, or the review agent-tool that reads
    ``evidence`` silently loses its context.
    Values are compared under :func:`_canonical` (dict-key-order- and
    numeric-stable jsonb), so key order is irrelevant.

    Duplicate identities within one side are a defect in that side's check and
    are surfaced as that side's `_only` rows (they cannot be matched 1:1).
    """
    legacy_by_key: dict[FindingKey, dict[str, Any]] = {}
    explained: list[tuple[dict[str, Any], str]] = []
    legacy_dupes: list[dict[str, Any]] = []
    for row in legacy_rows:
        if str(row.get("finding_event")) == "resolved":
            explained.append((dict(row), ENGINE_SIDE_RESOLUTIONS))
            continue
        key = finding_key(row)
        if key in legacy_by_key:
            legacy_dupes.append(dict(row))
            continue
        legacy_by_key[key] = dict(row)

    ir_by_key: dict[FindingKey, dict[str, Any]] = {}
    ir_dupes: list[dict[str, Any]] = []
    for row in ir_rows:
        key = finding_key(row)
        if key in ir_by_key:
            ir_dupes.append(dict(row))
            continue
        ir_by_key[key] = dict(row)

    matched = 0
    mismatches: list[AttributeMismatch] = []
    legacy_only = list(legacy_dupes)
    ir_only = list(ir_dupes)
    for key, legacy_row in legacy_by_key.items():
        ir_row = ir_by_key.pop(key, None)
        if ir_row is None:
            legacy_only.append(legacy_row)
            continue
        matched += 1
        for column in compare_columns:
            if _canonical(legacy_row.get(column)) != _canonical(ir_row.get(column)):
                mismatches.append(AttributeMismatch(
                    key=key,
                    column=column,
                    legacy_value=legacy_row.get(column),
                    ir_value=ir_row.get(column),
                ))
    ir_only.extend(ir_by_key.values())

    return EquivalenceReport(
        matched=matched,
        legacy_only=tuple(legacy_only),
        ir_only=tuple(ir_only),
        explained=tuple(explained),
        attribute_mismatches=tuple(mismatches),
    )


__all__ = [
    "ENGINE_SIDE_RESOLUTIONS",
    "AttributeMismatch",
    "EquivalenceReport",
    "compare_finding_sets",
    "finding_key",
    "ir_findings_from_sql",
    "legacy_findings_from_store",
]
