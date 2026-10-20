"""Merge-back orchestrator (worktree-fork-merge-back: staging-and-fast-forward).

Lands a worktree fork's changes onto the canonical ``default`` branch via STAGE -> GATE ->
LAND:

  1. STAGE -- create a fresh staging child of default's tip and merge the fork onto it
              (default's live head is never touched).
  2. GATE  -- publish the staging candidate, which runs the existing consistency gate over
              it. A narrowing/no_dangling/node-subtype violation BLOCKS here.
  3. LAND  -- on gate-pass (and default unchanged since STAGE), merge the GATED STAGING
              branch onto default's OWN head as a normal write, then publish default. On
              block, staging is abandoned and default is unchanged. If the LAND publish
              itself blocks, default is rolled back by writing the reverse edit onto its
              own head (``restore_branch_to_generation``), which, like the replay, keeps
              default's interval wide (issue #2862).

LANDING CHOICE -- why "replay" (merge staging into default) and not a fast-forward.
A fast-forward (the chronos-fast-forward-branch primitive, ``store.fast_forward``) is the
metadata-only alternative: it re-roots default's head ONTO the staging segment. That is
sound (proven) but has an interval-budget cost: default's head becomes a sub-interval of
the staging segment every merge-back, so default's interval collapses geometrically and
repeated fast-forwards EXHAUST the interval after ~5 cycles (which also seizes new forks --
``create_branch`` needs interval too). That is a DEPTH problem no allocation policy fixes
(a child interval cannot be wider than its parent).

Replaying instead applies the gated delta to default's OWN head via a normal write, which
does NOT split/shrink default's interval (only branch/checkpoint operations do, and a
publish only nibbles ~sqrt(W) like any publish). So repeated merge-backs scale indefinitely.
The cost is a copy of the (small) gated delta instead of a pointer move -- negligible for a
worktree's changes. The fast-forward primitive remains available for large deltas where the
copy is expensive; the long-term ideal is decoupling read-ancestry from interval-nesting in
chronos so a fast-forward keeps default wide (see the chronos-fast-forward-branch budget
finding).

``store.merge`` is the guarded entry (okg#150/#151): the LAND threads ``decider`` (a
named-human MergeDecision) to authorize landing on the protected target, and the merge
guard enforces the two-way / catalog-drift / deletion preconditions there. REMAINING #151
integration: compose the diff-transforms (promote-scoping / restamp / partition) at STAGE so
only the filtered, re-stamped subset is staged and replayed; the gate branch validates the
deployment's enriched graph while the replay staging branch remains source-only.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field, replace
from typing import Any, Iterable

from okg.substrate.merge_guard import MergeDecision

logger = logging.getLogger(__name__)

DEFAULT_BRANCH = "default"


@dataclass(frozen=True)
class MergeBackResult:
    status: str  # "merged" | "blocked"
    staging_branch: str
    generation_id: str | None = None
    blocked_reason: str | None = None
    #: okg#3112: the post-publish index rebuild after a landed merge.
    post_publish_index_rebuild: dict[str, Any] | None = field(default=None, compare=False)


@dataclass(frozen=True)
class _GateSideEffectSnapshot:
    deployment_name: str
    gate_branch: str
    node_fact_watermark: int
    edge_fact_watermark: int
    enricher_names: tuple[str, ...]
    enricher_progress_rows: tuple[tuple[Any, ...], ...]


def _merge_back_publisher_setup(
    dsn: str,
    deployment_name: str,
) -> tuple[Any, dict[str, Any]]:
    from okg.substrate.consistency import TransformConsistencyGate
    from okg.substrate.publisher.deployment_stack import (
        build_deployment_publisher_stack,
    )

    stack = build_deployment_publisher_stack(
        dsn=dsn,
        deployment_name=deployment_name,
    )
    overrides: dict[str, Any] = {}
    if not stack.consistency_gates:
        overrides["consistency_gates"] = [TransformConsistencyGate()]
    return stack, overrides


def merge_back(
    store: Any,
    fork_branch: str,
    *,
    dsn: str,
    base_generation_id: int | str,
    decider: MergeDecision | None = None,
    target: str = DEFAULT_BRANCH,
    deployment_name: str = "default",
    staging_branch: str | None = None,
) -> MergeBackResult:
    """Run STAGE -> GATE -> LAND, then (okg#3112) the post-publish index
    rebuild once the LAND publish's connection and locks are released."""
    result = _merge_back_locked(
        store,
        fork_branch,
        dsn=dsn,
        base_generation_id=base_generation_id,
        decider=decider,
        target=target,
        deployment_name=deployment_name,
        staging_branch=staging_branch,
    )
    if result.status != "merged":
        return result
    from okg.substrate.db.index_reclaim import rebuild_indexes_after_publish

    return replace(
        result,
        post_publish_index_rebuild=rebuild_indexes_after_publish(
            dsn,
            deployment=deployment_name,
            summary={"status": "published", "generation_id": result.generation_id},
        ),
    )


def _merge_back_locked(
    store: Any,
    fork_branch: str,
    *,
    dsn: str,
    base_generation_id: int | str,
    decider: MergeDecision | None = None,
    target: str = DEFAULT_BRANCH,
    deployment_name: str = "default",
    staging_branch: str | None = None,
) -> MergeBackResult:
    """Run STAGE -> GATE -> LAND for ``fork_branch`` into ``target``.

    ``base_generation_id`` is target's latest published generation (the fork base the staged
    candidate is gated against). Returns merged (with the new target generation) or blocked
    (staging abandoned, target unchanged).
    """
    from okg.substrate.merge_guard import MergeRefused
    from okg.substrate.publisher.factory import build_deployment_publisher
    from okg.substrate.db.connection import connect as db_connect

    staging = staging_branch or f"staging:{fork_branch}:{uuid.uuid4().hex[:8]}"
    gate_branch = f"{staging}:gate:{uuid.uuid4().hex[:8]}"
    gate_generation_id: str | None = None
    land_blocked_generation_id: str | None = None
    compose_completed = False
    publisher_stack, publisher_overrides = _merge_back_publisher_setup(
        dsn,
        deployment_name,
    )
    try:
        # STAGE: a fresh staging child of target's tip; compose the fork's promote-scoped,
        # re-stamped, accept-partitioned subset onto it (NOT a wholesale merge), routing the
        # quarantined subset to quarantine:<fork> atomically. staging is non-protected, so the
        # decider-boundary applies only at LAND.
        store.create_branch(
            staging,
            from_branch=target,
            metadata={"base_branch": target, "base_generation_id": base_generation_id},
        )
        try:
            _compose_and_stage(
                store, fork_branch, staging,
                target=target, fork_tip_gen=str(base_generation_id),
            )
            compose_completed = True
        except MergeRefused as exc:
            return MergeBackResult(
                status="blocked",
                staging_branch=staging,
                blocked_reason=(exc.data or {}).get("reason") or "stage_compose_refused",
            )
        # target's head AFTER all of merge_back's own branch creation (staging + any
        # quarantine) -- so the GUARD detects only a CONCURRENT writer, not our own setup.
        expected_head = store._branch_head_segment(target)

        # GATE runs the deployment's real publisher stack over a clone of the source-only
        # staging branch. Enrichers may write derived rows into the gate branch, but LAND
        # replays the untouched staging branch and lets the default publisher be the sole
        # deriver on the canonical branch.
        store.create_branch(
            gate_branch,
            from_branch=staging,
            metadata={
                "base_branch": target,
                "base_generation_id": base_generation_id,
                "merge_back_staging_branch": staging,
                "role": "merge_back_gate",
            },
        )

        # `idle_in_transaction_session_timeout=0` disables the bound for THIS
        # session only. The gate runs the deployment's real publisher stack on
        # this connection: `_snapshot_gate_side_effects` opens the transaction
        # and `gate_publisher.cycle(conn=conn)` then spends its time on
        # client-side extractor and enricher work (model calls for an LLM
        # enricher) between statements. The session is legitimately idle in
        # transaction for as long as a cycle takes, which exceeds the
        # 300-second default and would be killed with SQLSTATE 25P03, losing
        # the gate. Same reasoning as `enrichers/backfill.py`. The factory's
        # other protections (keepalives, connect_timeout) still apply, and
        # libpq takes the last `-c` for a setting, so this overrides the
        # factory's own value.
        with db_connect(
            dsn, options="-c idle_in_transaction_session_timeout=0",
        ) as conn:
            gate_publisher = build_deployment_publisher(
                dsn=dsn,
                deployment_name=deployment_name,
                command="okg merge-back gate",
                branch_name=gate_branch,
                stack=publisher_stack,
                publisher_overrides=publisher_overrides,
            )

            def run_gate_publish() -> dict[str, Any]:
                nonlocal gate_generation_id
                gate_side_effects = _snapshot_gate_side_effects(
                    conn,
                    deployment_name=deployment_name,
                    gate_branch=gate_branch,
                    enrichers=gate_publisher.enrichers,
                )
                out = gate_publisher.cycle(conn=conn)
                gate_generation_id = out.get("generation_id")
                _restore_gate_side_effects(conn, gate_side_effects)
                return out

            gate_mode = gate_publisher._resolved_coordination_mode()
            if gate_mode == "dbos":
                # The gate publisher commits before returning. Hold one
                # session-level publisher mutex count across the side-effect
                # cleanup so same-deployment publishers cannot advance
                # enricher_progress between the gate cycle and restore.
                with gate_publisher._publish_lock_on_connection(
                    conn,
                    mode=gate_mode,
                ):
                    gate_out = run_gate_publish()
            else:
                gate_out = run_gate_publish()
            if gate_out.get("status") != "published":
                return MergeBackResult(
                    status="blocked",
                    staging_branch=staging,
                    blocked_reason=(
                        gate_out.get("blocked_reason") or gate_out.get("status")
                    ),
                )

            locked_store = store.__class__.from_connection(
                conn,
                ensure_schema=False,
                interval_continuation_percent=(
                    getattr(store.context._backend, "continuation_percent", None)
                ),
            )
            locked = locked_store._lock_branch_write_session(target)
            try:
                # Also acquire the transaction-scoped lock used by ordinary branch writers.
                # The session-level lock above keeps the same key held across publisher
                # commits; this xact lock preserves the canonical write-lock check inside the
                # guarded critical section.
                locked_store._lock_branch_write(target)

                # GUARD: target must not have advanced since STAGE -- else the gated result
                # was validated against a stale base and the LAND re-gate could differ. Force
                # rebase while the branch-write lock prevents a writer from racing in after
                # this check.
                if locked_store._branch_head_segment(target) != expected_head:
                    return MergeBackResult(
                        status="blocked",
                        staging_branch=staging,
                        blocked_reason="target_advanced_rebase_required",
                    )

                pre_land_generation = locked_store.latest_generation(
                    deployment_name=deployment_name,
                    branch=target,
                    status="published",
                )
                pre_land_generation_id = (
                    getattr(pre_land_generation, "checkpoint_id", None)
                    or str(base_generation_id)
                )
                # Item 92a: a source write that changes only a ranked edge's
                # declaration is not in any checkpoint yet (the publisher
                # reuses the last generation), and the source has already
                # moved its watermark past it. Read those rows before landing
                # so a rollback to pre_land_generation_id puts them back.
                try:
                    pending_declarations = (
                        locked_store.edge_declarations_since_generation(
                            pre_land_generation_id, branch=target,
                        )
                    )
                except Exception as exc:  # noqa: BLE001 - block before landing
                    logger.error(
                        "merge-back: cannot read declarations written since %s "
                        "(%s); not landing, since a rollback could not put "
                        "them back",
                        pre_land_generation_id,
                        exc,
                    )
                    return MergeBackResult(
                        status="blocked",
                        staging_branch=staging,
                        blocked_reason="pending_declarations_unreadable",
                    )

                # LAND (replay): merge the SOURCE-ONLY staging branch onto target's OWN head
                # (a normal write -- does NOT shrink target's interval, so repeated
                # merge-backs never exhaust the budget) and publish target with the same
                # deployment stack used by GATE. `decider` is the named-human decision that
                # authorizes landing on the protected target (merge_guard decider-boundary);
                # the guard refuses without it before applying.
                try:
                    locked_store.merge(staging, target, decision=decider)
                except MergeRefused as exc:
                    return MergeBackResult(
                        status="blocked",
                        staging_branch=staging,
                        blocked_reason=(exc.data or {}).get("reason") or "merge_refused",
                    )

                land_out = build_deployment_publisher(
                    dsn=dsn,
                    deployment_name=deployment_name,
                    command="okg merge-back land",
                    branch_name=target,
                    stack=publisher_stack,
                    publisher_overrides=publisher_overrides,
                ).cycle(conn=conn)
                if land_out.get("status") != "published":
                    reason = land_out.get("blocked_reason") or "land_gate_blocked"
                    land_blocked_generation_id = land_out.get("generation_id")
                    rollback_incomplete = False
                    if land_blocked_generation_id is not None:
                        try:
                            locked_store.delete_generation(land_blocked_generation_id)
                        except Exception as exc:  # noqa: BLE001 - restore must still run
                            # Loud degrade, never silent: the branch restore
                            # below is the correctness-critical step and must
                            # proceed, but the result may not claim a clean
                            # rollback while the blocked generation checkpoint
                            # is stranded (the finally retry is best-effort).
                            rollback_incomplete = True
                            logger.warning(
                                "merge-back rollback: delete_generation(%s) "
                                "failed (%s); restoring the branch anyway -- "
                                "the blocked generation checkpoint is "
                                "stranded pending the cleanup retry",
                                land_blocked_generation_id,
                                exc,
                            )
                    locked_store.restore_branch_to_generation(
                        pre_land_generation_id,
                        branch=target,
                        deployment_name=deployment_name,
                    )
                    try:
                        locked_store.reapply_edge_declarations(
                            pending_declarations, branch=target,
                        )
                    except Exception as exc:  # noqa: BLE001 - report, never raise past the rollback
                        # The rollback itself completed; only putting back
                        # declarations written since the pre-land generation
                        # failed. Say so in the blocked result instead of
                        # raising out of a finished rollback.
                        logger.error(
                            "merge-back rollback: re-applying %d declarations "
                            "written since %s failed (%s); their sources must "
                            "rerun to write them again",
                            len(pending_declarations),
                            pre_land_generation_id,
                            exc,
                        )
                        return MergeBackResult(
                            status="blocked",
                            staging_branch=staging,
                            blocked_reason=(
                                f"land_gate_blocked_rolled_back:{reason}:"
                                "declarations_not_reapplied"
                            ),
                        )
                    if rollback_incomplete:
                        return MergeBackResult(
                            status="blocked",
                            staging_branch=staging,
                            blocked_reason=(
                                f"land_gate_blocked_rollback_incomplete:{reason}:"
                                f"stranded_generation={land_blocked_generation_id}"
                            ),
                        )
                    return MergeBackResult(
                        status="blocked",
                        staging_branch=staging,
                        blocked_reason=f"land_gate_blocked_rolled_back:{reason}",
                    )
                return MergeBackResult(
                    status="merged", staging_branch=staging,
                    generation_id=land_out.get("generation_id"),
                )
            finally:
                if locked:
                    try:
                        locked_store._unlock_branch_write_session(target)
                    except Exception:  # noqa: BLE001 - connection close releases it
                        pass
    finally:
        # The staging branch + its gate-publish generation are short-lived. On success the
        # landed rows live in target's own head; on a blocked/errored exit this abandons
        # staging so target is unchanged. delete_branch does not cascade checkpoints, so the
        # staging generation is deleted explicitly.
        if land_blocked_generation_id is not None:
            try:
                store.delete_generation(land_blocked_generation_id)
            except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                logger.warning(
                    "merge-back cleanup: retry delete_generation(%s) failed "
                    "(%s); the blocked generation checkpoint remains stranded",
                    land_blocked_generation_id,
                    exc,
                )
        if gate_generation_id is not None:
            try:
                store.delete_generation(gate_generation_id)
            except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                logger.warning(
                    "merge-back cleanup: delete_generation(%s) for the gate "
                    "publish failed (%s); the gate generation checkpoint "
                    "remains stranded and pins its segment from GC",
                    gate_generation_id,
                    exc,
                )
        try:
            store.delete_branch(gate_branch)
        except Exception:  # noqa: BLE001 - best-effort cleanup (e.g. gate never created)
            pass
        try:
            store.delete_branch(staging)
        except Exception:  # noqa: BLE001 - best-effort cleanup (e.g. staging never created)
            pass
        if not compose_completed:
            try:
                store.delete_branch(f"quarantine:{fork_branch}")
            except Exception:  # noqa: BLE001 - best-effort cleanup (e.g. never created)
                pass


def _snapshot_gate_side_effects(
    conn: Any,
    *,
    deployment_name: str,
    gate_branch: str,
    enrichers: Iterable[Any],
) -> _GateSideEffectSnapshot:
    node_watermark = _fact_watermark(conn, "okg.node_facts")
    edge_watermark = _fact_watermark(conn, "okg.edge_facts")
    enricher_names = tuple(
        str(getattr(enricher, "name", enricher.__class__.__name__))
        for enricher in enrichers
    )
    progress_rows: tuple[tuple[Any, ...], ...] = ()
    if enricher_names:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT deployment_name, enricher_name, fact_watermark, edge_watermark,
                           generation_id, catalog_version_id, last_run_at, last_status,
                           lifetime_edges_emitted, dormant_ok,
                           consecutive_failure_count
                      FROM okg.enricher_progress
                     WHERE deployment_name = %s
                       AND enricher_name = ANY(%s)
                     ORDER BY enricher_name
                    """,
                    (deployment_name, list(enricher_names)),
                )
                progress_rows = tuple(tuple(row) for row in cur.fetchall())
        except Exception:  # noqa: BLE001 - schema may be absent in narrow tests
            conn.rollback()
            progress_rows = ()
    return _GateSideEffectSnapshot(
        deployment_name=deployment_name,
        gate_branch=gate_branch,
        node_fact_watermark=node_watermark,
        edge_fact_watermark=edge_watermark,
        enricher_names=enricher_names,
        enricher_progress_rows=progress_rows,
    )


def _restore_gate_side_effects(
    conn: Any,
    snapshot: _GateSideEffectSnapshot,
) -> None:
    derived_source_predicate = """
        (
            f.source LIKE '_enricher:%%'
            OR f.source LIKE '_extractor:%%'
            OR f.source LIKE '_lens:%%'
            OR f.source LIKE 'derived_artifact:%%'
        )
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH gate_runs AS (
                    SELECT DISTINCT r.source, r.run_id
                      FROM okg.admission_envelopes e
                      JOIN okg.admission_record_refs r
                        ON r.admission_id = e.admission_id
                     WHERE e.deployment_name = %s
                       AND e.target_branch_name = %s
                       AND e.producer_kind IN ('enricher', 'extractor', 'lens')
                       AND r.run_id IS NOT NULL
                       AND r.ref_kind IN ('node_fact_range', 'edge_fact_range')
                )
                DELETE FROM okg.node_facts f
                 USING gate_runs g
                 WHERE f.fact_id > %s
                   AND f.source = g.source
                   AND f.run_id = g.run_id
                   AND {derived_source_predicate}
                """,
                (
                    snapshot.deployment_name,
                    snapshot.gate_branch,
                    snapshot.node_fact_watermark,
                ),
            )
            cur.execute(
                f"""
                WITH gate_ranges AS (
                    SELECT r.source, r.run_id, r.fact_id_min, r.fact_id_max
                      FROM okg.admission_envelopes e
                      JOIN okg.admission_record_refs r
                        ON r.admission_id = e.admission_id
                     WHERE e.deployment_name = %s
                       AND e.target_branch_name = %s
                       AND e.producer_kind IN ('enricher', 'extractor', 'lens')
                       AND r.run_id IS NOT NULL
                       AND r.ref_kind = 'edge_fact_range'
                       AND r.fact_id_min IS NOT NULL
                       AND r.fact_id_max IS NOT NULL
                )
                DELETE FROM okg.edge_facts f
                 USING gate_ranges g
                 WHERE f.fact_id > %s
                   AND f.source = g.source
                   AND f.run_id = g.run_id
                   AND f.fact_id BETWEEN g.fact_id_min AND g.fact_id_max
                   AND {derived_source_predicate}
                """,
                (
                    snapshot.deployment_name,
                    snapshot.gate_branch,
                    snapshot.edge_fact_watermark,
                ),
            )
            cur.execute(
                """
                DELETE FROM okg.admission_envelopes
                 WHERE deployment_name = %s
                   AND target_branch_name = %s
                   AND producer_kind IN ('enricher', 'extractor', 'lens')
                """,
                (snapshot.deployment_name, snapshot.gate_branch),
            )
            if snapshot.enricher_names:
                cur.execute(
                    """
                    DELETE FROM okg.enricher_progress
                     WHERE deployment_name = %s
                       AND enricher_name = ANY(%s)
                    """,
                    (
                        snapshot.deployment_name,
                        list(snapshot.enricher_names),
                    ),
                )
                if snapshot.enricher_progress_rows:
                    cur.executemany(
                        """
                        INSERT INTO okg.enricher_progress
                          (deployment_name, enricher_name, fact_watermark, edge_watermark,
                           generation_id, catalog_version_id, last_run_at, last_status,
                           lifetime_edges_emitted, dormant_ok,
                           consecutive_failure_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (deployment_name, enricher_name)
                          DO UPDATE SET
                            fact_watermark = EXCLUDED.fact_watermark,
                            edge_watermark = EXCLUDED.edge_watermark,
                            generation_id = EXCLUDED.generation_id,
                            catalog_version_id = EXCLUDED.catalog_version_id,
                            last_run_at = EXCLUDED.last_run_at,
                            last_status = EXCLUDED.last_status,
                            lifetime_edges_emitted = EXCLUDED.lifetime_edges_emitted,
                            dormant_ok = EXCLUDED.dormant_ok,
                            consecutive_failure_count =
                              EXCLUDED.consecutive_failure_count
                        """,
                        snapshot.enricher_progress_rows,
                    )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _fact_watermark(conn: Any, table: str) -> int:
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(max(fact_id), 0) FROM {table}")
            row = cur.fetchone()
        return int(row[0] or 0)
    except Exception:  # noqa: BLE001 - fact table may be absent in narrow tests
        conn.rollback()
        return 0


def _compose_and_stage(
    store: Any,
    fork_branch: str,
    staging: str,
    *,
    target: str,
    fork_tip_gen: str,
) -> str | None:
    """STAGE composition -- the load-bearing part of compose-stage-guards.

    Instead of merging the fork's full diff wholesale (which has no per-row transform seam),
    stage onto ``staging`` ONLY the promote-scoped, re-stamped, subtype-legal, non-conflicting
    subset of the fork's changes, and write the quarantined subset (conflicts + illegal rows)
    onto ``quarantine:<fork>`` ATOMICALLY with the staging apply. Returns the quarantine branch
    name if anything was quarantined (it persists for review, reaped with the fork by
    gc-forks), else None.

    Raises MergeRefused on a block: the two-way fallback (no shared base), an unmergeable
    (null/system/derived) source on a durable row, an unclassifiable row, or no applied catalog.
    """
    from types import SimpleNamespace

    from okg.substrate.merge_guard import MergeRefused
    from okg.substrate.merge_partition import partition_merge, subtype_legality_verdict
    from okg.substrate.merge_restamp import restamp_diff
    from okg.substrate.promote_filter import is_promote_marked

    backend = store.context._backend

    # (a) Fail closed on the two-way fallback: STAGE consults the backend preview directly
    # (it bypasses ChronosGraphStore.merge), so it MUST re-assert the no-shared-base guard.
    preview = backend.merge_preview(fork_branch, staging)
    if preview is None:
        raise MergeRefused(
            f"refusing merge {fork_branch!r}: no shared base with {target!r} (two-way fallback)",
            data={
                "reason": "two_way_fallback_no_shared_base",
                "source": fork_branch,
                "target": target,
            },
        )

    # (b) Promote-scope: only durable rows are eligible (deny-by-default drops scratch rows
    # AND deletions); preserve the changes/conflicts split the partition needs.
    changes = [c for c in (getattr(preview, "changes", None) or []) if is_promote_marked(c)]
    conflicts = [c for c in (getattr(preview, "conflicts", None) or []) if is_promote_marked(c)]

    # (c) Re-stamp provenance on the durable rows -- HARD-REFUSES the whole merge if any row
    # carries a null/system/derived source (batch deny-by-default, before partition; legality
    # is unaffected by the re-stamp, which rewrites only provenance).
    changes = restamp_diff(changes, worktree=fork_branch, fork_tip_gen=fork_tip_gen)
    conflicts = restamp_diff(conflicts, worktree=fork_branch, fork_tip_gen=fork_tip_gen)

    # (d) Partition by subtype legality against the active catalog: accepted (legal,
    # non-conflicting) lands; quarantined (every conflict + every illegal row) is set aside.
    cv = store._current_applied_catalog_version()
    if cv is None:
        raise MergeRefused(
            f"refusing merge {fork_branch!r}: no applied catalog to check subtype legality",
            data={"reason": "no_applied_catalog", "source": fork_branch, "target": target},
        )
    legal = frozenset(
        row["name"]
        for row in store.query(
            f"SELECT name FROM okg.subtypes WHERE catalog_version_id = {int(cv)}",
            branch=target,
        )
    )
    part = partition_merge(
        SimpleNamespace(changes=changes, conflicts=conflicts),
        is_legal=subtype_legality_verdict(legal),
    )

    # (e) Apply the accepted subset onto staging and the quarantined subset onto
    # quarantine:<fork> ATOMICALLY in ONE staging-session transaction. The quarantine branch
    # row is created through the backend, not the runtime wrapper, so it is part of this
    # transaction rather than an eager autocommit. The quarantine write is addressed via the
    # backend branch_id within that same txn -- a second checked-out session would open its
    # own root txn and commit staging early; one txn means a database rollback leaves NEITHER.
    quarantine = f"quarantine:{fork_branch}" if part.quarantined else None
    quarantine_created = False
    staging_session = store.context.checkout(staging)
    with staging_session.transaction():
        if quarantine is not None:
            backend.create_branch(
                quarantine,
                target,
                {
                    "base_branch": target,
                    "kind": "merge_quarantine",
                    "fork_branch": fork_branch,
                },
            )
            quarantine_created = True
        store.context._apply_merge_changes(staging_session, list(part.accepted))
        if quarantine is not None:
            _apply_upserts_to_branch(backend, quarantine, part.quarantined)
    if quarantine_created:
        store.context._metadata_epoch += 1
    return quarantine


def _apply_upserts_to_branch(backend: Any, branch_id: str, changes: Iterable[Any]) -> None:
    """Apply the add/modify rows of ``changes`` to ``branch_id`` via the backend, grouped by
    table -- used for the quarantine write inside the staging transaction so both branches
    commit atomically. Promote-scoping has already dropped deletions, so every change is an
    upsert."""
    by_table: dict[str, list[dict[str, Any]]] = {}
    order: list[str] = []
    for ch in changes:
        after = getattr(ch, "after", None)
        if after is None:
            continue
        if ch.table not in by_table:
            by_table[ch.table] = []
            order.append(ch.table)
        by_table[ch.table].append(after)
    for table in order:
        backend.upsert_rows(branch_id, table, by_table[table])


__all__ = ["MergeBackResult", "merge_back"]
