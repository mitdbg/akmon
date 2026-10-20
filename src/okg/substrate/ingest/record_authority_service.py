"""Service-side glue for the opt-in indexed record authority (issue #1325).

These helpers live outside ``ingest/service.py`` on purpose. That module has an
architecture line budget (``tests/architecture/test_ingest_planning_boundaries``
:: ``test_planning_and_service_line_budgets``), and the record-authority wiring
is a self-contained concern: resolving the per-source format, carrying the
private staged candidate out of a run result, parking it for a later review
decision, and sealing it once a decision accepts. Keeping it here lets
``service.py`` call four named operations instead of inlining their bodies.

Nothing here is reachable for a ``legacy_v1`` source: every entry point is a
no-op when there is no candidate.
"""

from __future__ import annotations

from typing import Any

import psycopg

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.record_authority import (
    PrivateAuthorityCandidate,
    ProductionAuthorityStore,
)

__all__ = [
    "PrivateAuthorityCandidate",
    "private_authority_candidate_from_result",
    "record_pending_authority_admission",
    "resolve_record_authority_format",
    "seal_bound_authority",
]


def resolve_record_authority_format(entry: Any) -> str:
    """Return the per-source storage format, defaulting to the legacy path.

    A registry entry predating the field, or a test double standing in for one,
    means ``legacy_v1`` -- the format every source uses today. The registry
    itself rejects any value that is neither ``legacy_v1`` nor ``indexed_v2``,
    so this default cannot smuggle an unknown format past validation.
    """

    return getattr(entry, "record_authority_format", "legacy_v1")


def private_authority_candidate_from_result(
    result: Any,
) -> PrivateAuthorityCandidate | None:
    """Carry the private staged candidate out of a run result.

    Unlike ``rejected_fact_count``, a missing value here is not a silent data
    loss: ``None`` means the legacy path, which is what every source uses.
    Reading it by attribute would break every in-memory RunResult double in the
    suite. A rename is caught loudly instead by
    ``test_run_result_declares_the_private_authority_field``, which asserts the
    real dataclass still declares the field.
    """

    return getattr(result, "private_authority_candidate", None)


def record_pending_authority_admission(
    conn: psycopg.Connection,
    *,
    admission_id: str,
    candidate: PrivateAuthorityCandidate | None,
) -> None:
    """Park a staged indexed-v2 candidate for the later review decision.

    A review-mode run cannot seal yet, so the private candidate is written to a
    transaction-visible handoff row that ``okg admission decide`` reads back and
    revalidates. The row is read back here as well: a conflicting handoff for
    the same admission means two runs raced, and accepting either one would
    bind the wrong authority.
    """

    if candidate is None:
        return
    conn.execute(
        """INSERT INTO okg.source_record_authority_pending_admissions
           (admission_id,authority_id,source,incarnation,attempt,
            expected_sha256,expected_records,expected_operations,
            expected_components)
           VALUES (%s::uuid,%s::uuid,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT(admission_id) DO NOTHING""",
        (admission_id, candidate.authority, candidate.source,
         candidate.incarnation, candidate.attempt,
         candidate.expected_sha256, candidate.expected_records,
         candidate.expected_operations, candidate.expected_components),
    )
    stored = conn.execute(
        """SELECT authority_id::text,source,incarnation,attempt,
                  expected_sha256,expected_records,
                  expected_operations,expected_components
           FROM okg.source_record_authority_pending_admissions
           WHERE admission_id=%s::uuid""",
        (admission_id,),
    ).fetchone()
    if stored != (
        candidate.authority, candidate.source, candidate.incarnation,
        candidate.attempt, candidate.expected_sha256,
        candidate.expected_records, candidate.expected_operations,
        candidate.expected_components,
    ):
        raise SubstrateError("conflicting private authority admission handoff")


def seal_bound_authority(
    conn: psycopg.Connection,
    candidate: PrivateAuthorityCandidate | None,
) -> None:
    """Seal a staged authority and publish its reference in this transaction.

    Called only from an accepting admission branch, so the seal, the public
    reference and the terminal admission state commit or roll back together.
    """

    if candidate is None:
        return
    ProductionAuthorityStore(conn).seal_and_bind(candidate)
