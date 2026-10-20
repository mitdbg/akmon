"""Zero-output admission verdicts for a source run that appended nothing.

Moved verbatim out of ``ingest.service`` (the service module carries a line
budget); the service re-exports these names so existing imports keep working.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _source_health_is_healthy(source_health_status: str) -> bool:
    return source_health_status in {
        "ok",
        "not_applicable",
        "skipped_optional",
    }


#: The history signals, in the order they are checked.
SOURCE_HISTORY_CURSOR = "cursor"
SOURCE_HISTORY_RECONCILE_TIME = "reconcile_time"
SOURCE_HISTORY_PUBLISHED_ADMISSION = "published_source_admission"


def _source_history_signal(
    conn: Any, *, source: str, deployment: str,
) -> str | None:
    """Which accepted, published history the source has, or None.

    Three signals count, checked in this order:

    * ``cursor``: a stored, non-empty ``last_applied_rev``.
    * ``reconcile_time``: a recorded ``last_reconcile_at``.
    * ``published_source_admission``: an admission envelope of this
      deployment, with producer kind ``source``, for this source, that
      reached ``published``.

    A watermark row alone is not enough: the runner's replay fence inserts a
    row with an empty ``last_applied_rev`` BEFORE a brand-new source's first
    read. Accepted terminal state is what writes a cursor or a reconcile
    time. The watermark lookup is keyed by source name alone, as the table
    is; the envelope lookup is scoped to the deployment as well.

    A published envelope counts because a reader that never claims a
    complete scope and keeps no cursor -- the reference-catalog readers
    before ``okg.deployment`` exported ``COMPLETED_SCOPE_RUN_MODES`` --
    publishes its first run without writing either, so its watermark row
    stays empty and every later unchanged run looked brand-new and was
    refused ``partial``. A deployment upgraded from such a reader carries
    exactly that state. An envelope that failed, was rejected, or was
    accepted but never published is not history: nothing it carried reached
    a generation. Every source-envelope writer sets ``run_id``, so the
    ``run_id IS NOT NULL`` term costs nothing and lets the lookup use the
    partial index ``admission_envelopes_source_run_idx``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_applied_rev <> '{}'::jsonb
              FROM okg.source_watermarks
             WHERE source = %s
               AND (
                 last_applied_rev <> '{}'::jsonb
                 OR last_reconcile_at IS NOT NULL
               )
            """,
            (source,),
        )
        row = cur.fetchone()
        if row is not None:
            return (
                SOURCE_HISTORY_CURSOR if row[0] else SOURCE_HISTORY_RECONCILE_TIME
            )
        cur.execute(
            """
            SELECT 1
              FROM okg.admission_envelopes
             WHERE source = %s
               AND run_id IS NOT NULL
               AND deployment_name = %s
               AND producer_kind = 'source'
               AND status = 'published'
             LIMIT 1
            """,
            (source, deployment),
        )
        if cur.fetchone() is not None:
            return SOURCE_HISTORY_PUBLISHED_ADMISSION
    return None


def _source_has_published_watermark(
    conn: Any, *, source: str, deployment: str,
) -> bool:
    """True when :func:`_source_history_signal` finds any history.

    The name predates the published-admission signal; the verdict key
    ``source_has_published_watermark`` keeps it because consumers read it.
    """
    return _source_history_signal(
        conn, source=source, deployment=deployment,
    ) is not None


def _source_history_kwargs(
    conn: Any, *, source: str, deployment: str, completed_scope: bool,
) -> dict[str, Any]:
    """The history arguments for :func:`_zero_output_admission_verdict`.

    A completed scope needs no history, so nothing is read for it.
    """
    signal = (
        None
        if completed_scope
        else _source_history_signal(conn, source=source, deployment=deployment)
    )
    return {
        "source_has_published_watermark": signal is not None,
        "source_history": signal,
    }


def _zero_output_rejection_reason(
    *,
    authority_reason: str | None,
    structural_allowed: bool,
    structural_kind: str | None,
) -> str:
    """Reason for a rejected zero-output run: the check that actually failed.

    Authority denials own the reason. Only when the STRUCTURAL verdict is the
    rejecter (``structural_allowed`` is False and authority raised no reason) is
    the structural kind (e.g. ``unclassified_empty`` / ``partial`` /
    ``facts_rejected``) surfaced, instead of the generic
    ``zero_output_not_authorized`` that otherwise blames authority for a
    structural verdict and misdirected #826's diagnosis. When authority denied a
    structurally-accepted run without a reason string, the structural kind is an
    ACCEPT label and must not leak as the rejection reason, so the generic
    default stands.
    """
    if authority_reason:
        return authority_reason
    if not structural_allowed and structural_kind:
        return structural_kind
    return "zero_output_not_authorized"


def _zero_output_admission_verdict(
    *,
    source: str,
    run_id: str,
    admission_mode: str,
    completed_scope: bool,
    source_health_status: str,
    run_mode: str,
    next_cursor: Mapping[str, Any] | None,
    rejected_fact_count: int = 0,
    prior_unpublished_admissions: list[str] | None = None,
    prior_unpublished_check_failed: bool = False,
    source_has_published_watermark: bool = False,
    source_history: str | None = None,
) -> dict[str, Any]:
    healthy = _source_health_is_healthy(source_health_status)
    unchanged = run_mode in {"no_change", "release_unchanged"}
    record_set = (
        next_cursor.get("record_set") if isinstance(next_cursor, Mapping) else None
    )
    summary = (
        next_cursor.get("record_set_summary")
        if isinstance(next_cursor, Mapping)
        else None
    )
    # A completed-scope authoritative pass carries its full declared coverage as
    # a record set — inline (`record_set`) or by external reference
    # (`record_set_summary` + `record_set_ref`). That record set is the same
    # authority signal whether it is empty or not, so it backs two accepted
    # zero-output outcomes:
    #   * empty  -> `authoritative_empty_snapshot` (the source genuinely has no
    #     records), and
    #   * non-empty with zero APPENDED output -> `authoritative_replay`: an
    #     idempotent re-sync whose facts all deduped to a semantic no-op against
    #     already-persisted rows. The interval-idempotence work made this the
    #     normal steady state for an unchanged reconcile; rejecting it blocked
    #     every such publish (#826).
    has_inline_record_set = isinstance(record_set, Mapping)
    has_external_record_set = isinstance(summary, Mapping) and bool(
        next_cursor and next_cursor.get("record_set_ref")
    )
    empty_inline = has_inline_record_set and not (record_set.get("records", record_set))
    empty_external = has_external_record_set and summary.get("record_count") == 0
    # `release_new` is the reference_catalog profile's whole-scope mode (the
    # runner's `_AUTHORITATIVE_REPLAY_MODE`), and a strict reference_catalog
    # source runs in it on a plain ingest (#2476). Its completed pass carries
    # the same full record set, so an all-deduped re-read is the same
    # idempotent replay; leaving it out rejected every forced replay of an
    # unchanged catalog as `unclassified_empty`.
    authoritative_snapshot = run_mode in {
        "reconcile",
        "scope_complete",
        "full_rebuild",
        "release_new",
    } and (has_inline_record_set or has_external_record_set)
    # Zero appended output only proves an idempotent replay if EVERY emitted
    # fact was accounted for. A rejected fact (NUL in an identity field, etc.)
    # is dropped with a warning but does NOT degrade health or scope, so an
    # all-rejected reconcile otherwise looks identical to a clean replay
    # (record set present, zero appended, healthy, complete). Accepting it would
    # advance the watermark past facts that never landed — a silent data loss.
    # A rejection makes the run not a clean pass, so it is never accepted and is
    # labelled `facts_rejected` so the operator sees why.
    rejected_facts = rejected_fact_count > 0
    # close-refused-publish-redrive-gap (#828): zero appended output is only
    # an idempotent no-op when the rows it deduped against reached a
    # PUBLISHED generation. Prior staged admissions of this source
    # (accepted/sweepable, refs attached, never swept) mean the dedupe basis
    # is unpublished branch-head state left by a refused publish; accepting
    # (as unchanged OR authoritative_*) would report SUCCESS while the
    # stranded work never re-emits. The commit gate's staged-source drive
    # publishes those admissions, after which the next zero-output run is
    # accepted normally -- the rejection is loud and terminating, not a
    # loop. A failed detection is its own rejection (fail-closed).
    unpublished_prior = bool(prior_unpublished_admissions)
    clean_basis = (
        healthy
        and not rejected_facts
        and not unpublished_prior
        and not prior_unpublished_check_failed
    )
    # #2074 (a), operator decision on #1859 (2026-09-22): a HEALTHY run that
    # did not finish its scan and produced nothing is an accepted no-op, not
    # a `partial` refusal -- but only for a source that already has accepted
    # terminal state. It proves nothing about deletion (the runner never
    # synthesizes retractions from an incomplete run and carries the prior
    # authority forward) and the source still reads as stale, because the
    # terminal state advances `last_reconcile` only for a completed scope. A
    # brand-new source stays refused: there is no prior state to keep, and an
    # accepted empty first run would hide a reader that reads nothing.
    incomplete_scope_noop = (
        clean_basis
        and not completed_scope
        and source_has_published_watermark
    )
    accepted = (
        clean_basis
        and completed_scope
        and (unchanged or authoritative_snapshot)
    ) or incomplete_scope_noop
    # Health, scope, rejected-fact, and unpublished-basis failures dominate
    # the label so a rejected run reports why it was rejected, never an
    # accept-looking `authoritative_replay` / `unchanged` (the kind is
    # surfaced as the rejection reason — #826).
    if not healthy:
        zero_output_kind = "failed_or_degraded"
    elif incomplete_scope_noop:
        zero_output_kind = "incomplete_scope_noop"
    elif not completed_scope and not source_has_published_watermark:
        # A brand-new source: nothing to keep, so an incomplete empty run
        # stays the `partial` refusal it always was.
        zero_output_kind = "partial"
    elif rejected_facts:
        zero_output_kind = "facts_rejected"
    elif prior_unpublished_check_failed:
        zero_output_kind = "prior_emission_check_unavailable"
    elif unpublished_prior:
        zero_output_kind = "prior_emission_unpublished"
    elif unchanged:
        zero_output_kind = "unchanged"
    elif authoritative_snapshot and (empty_inline or empty_external):
        zero_output_kind = "authoritative_empty_snapshot"
    elif authoritative_snapshot:
        zero_output_kind = "authoritative_replay"
    else:
        zero_output_kind = "unclassified_empty"
    producer_policy: dict[str, Any] = {
        "status": "matched" if accepted else "rejected",
        "matched_clause_ids": [],
        "failed_clause_ids": [],
        "record_counts": {"node": 0, "edge": 0},
        "reason": zero_output_kind,
        "source_health_status": source_health_status,
        "completed_scope": completed_scope,
        "run_mode": run_mode,
        "terminal_state_authorized": accepted,
    }
    if not completed_scope:
        producer_policy["source_has_published_watermark"] = bool(
            source_has_published_watermark
        )
        # Which history signal matched (see `_source_history_signal`), or
        # None. The key above keeps its name because consumers read it.
        producer_policy["source_history"] = source_history
    if incomplete_scope_noop:
        # The no-op changes nothing but the admission record. The caller
        # saves no terminal source state for it: not the reader's proposed
        # cursor, not its progress keys, not a reconcile time. A reader that
        # records its next position before it reads (Gmail's history id)
        # and then fails every fetch would otherwise skip that unread range
        # for good.
        producer_policy["keeps_source_state"] = True
    if unpublished_prior:
        # Name the blocking admissions so the refusal is actionable from
        # the verdict alone (the #828 lesson: evidence that never reaches
        # the operator is evidence that does not exist). The detection
        # caps the list, so the key says "sample" -- honest about
        # possible truncation.
        producer_policy["unpublished_prior_admissions_sample"] = list(
            prior_unpublished_admissions or []
        )
    return {
        "source": source,
        "run_id": run_id,
        "fact_ref_count": 0,
        "admission_mode": admission_mode,
        "producer_policy": producer_policy,
    }
