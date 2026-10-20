"""Session execution of IR consistency checks with lifecycle emission.

PACT ``consistency-checks-as-ir``, task.ir.session-executor
(req.ir.session-execution + req.ir.observations). An IR-authored check runs
post-publish through the publisher's pinned Chronos checkout session, reading
the graph surfaces directly — **no** ``okg.chronos_graph_*_current`` mirror,
**no** ``pg_temp`` scope staging. Offending rows flow into the existing
durable observation lifecycle (deterministic candidate/observation ids, bulk
event writes, open/resolved transitions), and the coverage row is certified BY
THE ENGINE from the derived scope (:mod:`.ir_locality`) — full runs are
universe-complete, scoped runs say so, and the scope mode/reason always ride
in the coverage payload and gate preflight.

:class:`IrPostPublishCheck` implements the same ``evaluate`` shape the
publisher's post-publish loop already drives for SQL checks, plus optional
delta context (``dirty_scope`` / ``previous_generation_id`` / ``branch_name``)
that the publisher passes when available — absent delta evidence the check
runs FULL with the reason recorded, never silently scoped.
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Any, Mapping, Sequence
from uuid import UUID

import psycopg

from okg.substrate.consistency.chronos_sql_mirror import (
    checkout_published_generation,
    release_generation_session,
)
from okg.substrate.consistency.gates import (
    ConsistencyPublishGateEvaluation,
    evaluate_consistency_publish_gate,
)
from okg.substrate.consistency.ir_check import IrConsistencyCheckSpec
from okg.substrate.consistency.ir_locality import (
    SCOPE_PARAM,
    derive_frontier_plan,
    scope_subjects_from_delta,
    scoped_program,
)
from okg.substrate.consistency.observations import (
    CandidateCoverageReport,
    ConsistencyCandidate,
    ConsistencyCandidateObservation,
    HistoricalCandidateIdentityUnverifiable,
    ObservationEvent,
    candidate_id_for,
    observation_id_for,
    record_candidate_coverage,
    record_candidate_observations_bulk,
    scope_complement_recipe,
    scope_identity,
    start_consistency_run,
)
from okg.substrate.consistency.sql_gate import _complete_run, _fail_run
from okg.substrate.errors import SubstrateError, UndeclaredFindingIdentity
from okg.substrate.transform import lower_query
from okg.substrate.transform.operators import to_chronos_sql

log = logging.getLogger(__name__)

GENERATION_POLICY = "chronos_checkpoint_v1"


class IrPostPublishCheck:
    """A loaded IR check, runnable by the publisher's post-publish loop."""

    #: publisher marker: pass dirty_scope/previous/branch kwargs when available.
    accepts_publish_delta = True

    def __init__(self, spec: IrConsistencyCheckSpec) -> None:
        self.spec = spec
        self.name = spec.name
        self.blocks_publish = False

    # ------------------------------------------------------------------ #
    # gate-protocol entry point                                          #
    # ------------------------------------------------------------------ #
    def evaluate(
        self,
        conn: psycopg.Connection[Any],
        *,
        target_generation_id: int | str,
        catalog_version_id: int,
        deployment_name: str,
        branch_name: str = "default",
        dirty_scope: tuple | None = None,
        previous_generation_id: int | str | None = None,
        scope_subject_ids: Sequence[str] | None = None,
        checkout_generation_id: int | str | None = None,
    ) -> ConsistencyPublishGateEvaluation:
        """Evaluate the check against one published generation.

        ``target_generation_id`` keys the run, coverage and gate records (the
        publisher's compat id). ``checkout_generation_id`` is the Chronos
        checkpoint id the check READS; when omitted, ``target_generation_id``
        is resolved to its published checkpoint (checkpoint id or compat id,
        through ``store.resolve_generation``). An id that does not resolve
        fails the run with :class:`PostPublishGenerationUnresolved`; the check
        never reads the live branch head instead.
        """
        from okg.substrate.graph_store import ChronosGraphStore

        spec = self.spec
        definition = spec.definition(deployment_name)
        check_attrs: Mapping[str, Any] = spec.program.attrs["check"]
        t0 = time.monotonic()

        claim_state_surfaces = spec.claim_state_surface_names()
        plan = derive_frontier_plan(
            spec.program,
            check_attrs=check_attrs,
            claim_state_surfaces=claim_state_surfaces,
        )
        compat_gen = _compat_generation_int(
            conn, target_generation_id,
            deployment_name=deployment_name, branch_name=branch_name,
        )
        run_id = start_consistency_run(
            conn,
            definition,
            target_generation_id=compat_gen,
            trigger_kind="post_publish",
            metrics={
                "gate_name": self.name,
                "gate_type": "transform_ir",
                "execution_phase": "post_publish",
                "blocks_publish": False,
                "program_checksum": spec.program.checksum(),
                "scope_plan": {"mode": plan.mode, "reason": plan.reason},
            },
        )
        read_generation_id = (
            checkout_generation_id
            if checkout_generation_id is not None
            else target_generation_id
        )
        session = None
        previous_session = None
        try:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            session = self._checkout(
                store,
                read_generation_id,
                deployment_name=deployment_name,
                branch_name=branch_name,
                # Both ids given: they must name the same generation.
                expected_target_generation_id=(
                    compat_gen if checkout_generation_id is not None else None
                ),
            )
            if claim_state_surfaces:
                # Explicit pin: a claim-state read is generation-scoped ONLY by
                # the checkout's interval visibility over the Chronos-REGISTERED
                # kept table. Verify each claim-state kept table is registered at
                # this checkout's backend BEFORE reading, so the pin actually
                # applies. An unregistered kept table would fall through the
                # session's table rewrite and read raw rows across ALL
                # generations (or a stale un-versioned copy) — refuse loudly
                # rather than emit findings from a silently-mixed claim plane.
                self._assert_claim_state_pinned(store, spec)
            if previous_generation_id is not None:
                try:
                    previous_session = store.checkout_generation(
                        previous_generation_id
                    )
                except Exception as exc:  # noqa: BLE001 - no previous checkpoint -> full-scan fallback below
                    # Behavior unchanged: previous_session stays None, so the
                    # delta step below has nothing to diff against and this run
                    # widens to a FULL scan. Record the class + message so "why
                    # did this run go full instead of scoped" is answerable
                    # rather than silently swallowed.
                    log.info(
                        "consistency check %s: previous-generation checkout "
                        "load failed for generation %s (%s: %s); falling back "
                        "to full scan (no previous session)",
                        self.name,
                        previous_generation_id,
                        type(exc).__name__,
                        exc,
                    )
                    previous_session = None

            subjects, scope_reason = scope_subjects_from_delta(
                dirty_scope,
                plan,
                session_current=session,
                session_previous=previous_session,
            )
            # Anti-entropy oracle: a would-be-scoped run widens to a full
            # sweep when a prior run of this check errored (its findings went
            # unverified) or the periodic cadence is due. This is the drift
            # backstop for scoped runs — see _antientropy_force_full. Only a
            # scoped run can be forced; a run already going full stays full.
            if subjects is not None:
                forced = self._antientropy_force_full(
                    conn, definition, target_generation_id=compat_gen
                )
                if forced is not None:
                    subjects, scope_reason = None, forced
            if scope_subject_ids:
                # An explicit operator scope (consistency-rerun --scope-subject)
                # is authoritative: it overrides the delta-derived set and the
                # anti-entropy widening above, honoring the "limit checks to
                # these subjects" contract.
                subjects = list(dict.fromkeys(scope_subject_ids))
                scope_reason = "explicit_scope_subject_ids"
            scoped = subjects is not None

            offenders = self._execute(session, subjects)
            emitted = self._emit(
                conn,
                definition,
                run_id=run_id,
                target_generation_id=compat_gen,
                check_attrs=check_attrs,
                offenders=offenders,
                scoped_subjects=subjects,
            )
            eligible = (
                len(subjects) if scoped
                else self._universe_count(session)
            )
            coverage_payload = {
                "runtime": "chronos_session_transform_ir",
                "program_checksum": spec.program.checksum(),
                "scope_mode": plan.mode if scoped else "full",
                "scope_plan_reason": plan.reason,
                "scope_reason": scope_reason,
                "scope_restricted": scoped,
                "scope_subject_count": len(subjects) if scoped else None,
                "resolved_count": emitted["resolved"],
                "observed_count": emitted["observed"],
            }
            if scoped:
                # IDENTITY, not just the count above. A count says how many
                # subjects were evaluated but never which, so the complement --
                # the UNEVALUATED subjects, which is what an operator needs to
                # judge whether the verdict is safe to act on -- was not
                # recoverable from the published generation. The complement is
                # now computable: the check's subject universe (whose size is
                # already persisted as eligible_input_count on a full run) minus
                # these ids.
                assert subjects is not None
                subject_identity = scope_identity("scope_subject", subjects)
                coverage_payload.update(subject_identity)
                # Suppressed (with the reason) when the id list is capped: the
                # subtraction would report evaluated subjects as unevaluated.
                coverage_payload["scope_complement_recipe"] = (
                    scope_complement_recipe(
                        "scope_subject",
                        subject_identity,
                        universe=(
                            "this check's subject universe at this generation"
                        ),
                    )
                )
            if claim_state_surfaces:
                # Auditable pin: WHICH claim-state surfaces were read and the
                # generation they were pinned to. A claim-state check is always
                # FULL (bucket locality is task.binding.differential), so this
                # rides alongside scope_mode='full' / scope_restricted=False.
                coverage_payload["claim_state_surfaces"] = sorted(claim_state_surfaces)
                coverage_payload["claim_state_pinned_generation"] = str(
                    read_generation_id
                )
            if not scoped:
                # Full sweep is the drift oracle: it saw every open finding, so
                # its missed/stale counts are meaningful. On a scoped run
                # offender_scopes covers only the evaluated subset, so these
                # numbers would be misleading — never persist them there. This
                # dict IS preflight["scope"] (same object), so mutating it here
                # threads the drift into both the coverage row and the gate.
                coverage_payload["sweep"] = True
                coverage_payload["missed_open"] = emitted["missed_open"]
                coverage_payload["stale_open"] = emitted["stale_open"]
            record_candidate_coverage(
                conn,
                definition,
                target_generation_id=compat_gen,
                run_id=run_id,
                report=CandidateCoverageReport(
                    universe_key=(
                        f"ir:{spec.check_id}:{target_generation_id}:"
                        + ("scoped" if scoped else "full")
                    ),
                    # Engine-certified: a scoped run is exact over its derived
                    # scope but NOT universe-complete — it must never claim so.
                    # scope_restricted is the write-boundary's independent
                    # check on coverage_complete (observations._validate_coverage).
                    coverage_basis="exact_postgres",
                    coverage_complete=not scoped,
                    scope_restricted=scoped,
                    scope_subject_count=len(subjects) if scoped else 0,
                    eligible_input_count=eligible,
                    candidate_count=len(offenders),
                    evaluator_invocation_count=0,
                    payload=coverage_payload,
                ),
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            evaluation = evaluate_consistency_publish_gate(
                conn,
                definition,
                target_generation_id=compat_gen,
                run_id=run_id,
                gate_name=self.name,
                elapsed_ms=elapsed_ms,
                preflight={
                    "catalog_version_id": catalog_version_id,
                    "execution_phase": "post_publish",
                    "blocks_publish": False,
                    "runtime": "chronos_session_transform_ir",
                    "offending_row_count": len(offenders),
                    "scope": coverage_payload,
                },
                blocks_publish=False,
            )
            _complete_run(conn, run_id, {
                "gate_name": self.name,
                "gate_type": "transform_ir",
                "gate_result": evaluation.result,
                "offending_row_count": len(offenders),
                "scope": coverage_payload,
            })
            return evaluation
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            _fail_run(conn, run_id, exc)
            return evaluate_consistency_publish_gate(
                conn,
                definition,
                target_generation_id=compat_gen,
                run_id=run_id,
                gate_name=self.name,
                elapsed_ms=elapsed_ms,
                preflight={
                    "catalog_version_id": catalog_version_id,
                    "execution_phase": "post_publish",
                    "blocks_publish": False,
                    "runtime": "chronos_session_transform_ir",
                },
                error={"type": type(exc).__name__, "message": str(exc)},
                blocks_publish=False,
            )
        finally:
            release_generation_session(previous_session)
            release_generation_session(session)

    # ------------------------------------------------------------------ #
    # anti-entropy oracle: force a scoped run to widen to a full sweep    #
    # ------------------------------------------------------------------ #
    def _antientropy_force_full(
        self,
        conn: psycopg.Connection[Any],
        definition: Any,
        *,
        target_generation_id: int,
    ) -> str | None:
        """Should this would-be-scoped run widen to a full sweep instead?

        Two backstops keep scoped drift from accumulating silently:

        * **After-error.** If the check's most recent PRIOR run errored
          (durable ``blocked_error`` row), its findings went unverified — the
          next run sweeps the whole universe to reconcile.
        * **Cadence.** Every ``OKG_ANTIENTROPY_SWEEP_EVERY`` (default 24)
          consecutive scoped runs, force one full sweep so stranded opens
          eventually resolve. ``<= 0`` disables the cadence. We count PRIOR
          coverage rows newest-first, tallying consecutive scoped ones and
          stopping at the first full sweep; only that unbroken scoped streak
          counts, so a full run resets the clock.

        Both queries order PRIOR runs by ``created_at`` (run recency) and do
        NOT filter on ``target_generation_id``: this run's own coverage/gate
        rows are not written until AFTER this decision, so every existing row
        is a prior run — and a ``target_generation_id < current`` filter would
        wrongly drop a prior run that SHARES this run's compat generation id
        (consecutive / no-op publishes resolve to the same compat integer,
        so the ordering column is not a reliable run sequence — created_at is).

        Returns the scope_reason to record if a sweep is forced, else None.
        """
        cid = definition.check_id
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT result
                  FROM okg.consistency_gate_results
                 WHERE check_id = %(cid)s
                 ORDER BY created_at DESC
                 LIMIT 1
                """,
                {"cid": cid},
            )
            row = cur.fetchone()
        if row is not None and row[0] == "blocked_error":
            return "antientropy_after_error"

        try:
            every = int(os.environ.get("OKG_ANTIENTROPY_SWEEP_EVERY", "24"))
        except (TypeError, ValueError):
            every = 24
        if every <= 0:
            return None  # cadence disabled

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload->>'scope_mode' AS mode
                  FROM okg.consistency_candidate_coverage
                 WHERE check_id = %(cid)s
                 ORDER BY created_at DESC
                """,
                {"cid": cid},
            )
            modes = [r[0] for r in cur.fetchall()]
        consecutive_scoped = 0
        for mode in modes:
            if mode is None or mode == "full":
                break  # a full sweep (or unlabeled run) resets the streak
            consecutive_scoped += 1
        if consecutive_scoped >= every:
            return "antientropy_cadence_sweep"
        return None

    # ------------------------------------------------------------------ #
    # explicit claim-state generation pin                                #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _assert_claim_state_pinned(store: Any, spec: IrConsistencyCheckSpec) -> None:
        """Verify every claim-state kept table the check reads is Chronos-
        registered at ``store``'s backend, so the pinned checkout session scopes
        it to the run's generation.

        Load-bearing: an unregistered kept table is left untouched by the
        session's table rewrite, so the read would return the physical rows
        across ALL generations (or a stale un-versioned plain table) — the exact
        silent generation-mixing this repo bans. Fail closed and loud instead.
        """
        required = spec.claim_state_binding_tables()
        if not required:
            return
        backend = getattr(getattr(store, "context", None), "_backend", None)
        refresh = getattr(backend, "refresh_registries", None)
        tables = getattr(backend, "tables", None)
        if backend is None or tables is None:
            raise SubstrateError(
                f"check {spec.check_id!r} reads claim-state surface table(s) "
                f"{sorted(required)} but the pinned checkout's backend does not "
                "expose Chronos table registration; the generation pin cannot be "
                "verified — refusing rather than risk a generation-mixed read.",
                data={"check_id": spec.check_id, "claim_state_tables": sorted(required)},
            )
        if callable(refresh):
            refresh()  # pick up tables registered by the just-run derivations
            tables = backend.tables
        missing = sorted(t for t in required if t not in tables)
        if missing:
            raise SubstrateError(
                f"check {spec.check_id!r} reads claim-state surface table(s) "
                f"{missing} that are NOT Chronos-registered at the pinned "
                "checkout; the checkout could not scope them to this generation "
                "and a read would silently mix generations. Register the claim-"
                "model derivations (claim_effective / claim_projection) in the "
                "deployment's publish cycle (task.binding.wire-deployment).",
                data={"check_id": spec.check_id, "missing_tables": missing},
            )

    # ------------------------------------------------------------------ #
    # execution                                                          #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _checkout(
        store: Any,
        generation_id: int | str,
        *,
        deployment_name: str | None = None,
        branch_name: str = "default",
        expected_target_generation_id: int | None = None,
    ):
        """Open the published checkpoint, or raise.

        A post-publish check runs after its generation is checkpointed, so an
        id that does not open a checkpoint is a caller bug (for example the
        publisher's compat clock id instead of the checkpoint id). Reading the
        branch head instead would judge later writes against this generation,
        so :class:`PostPublishGenerationUnresolved` is raised.
        """
        return checkout_published_generation(
            store,
            generation_id,
            purpose="IR post-publish consistency check",
            deployment_name=deployment_name,
            branch=branch_name,
            expected_target_generation_id=expected_target_generation_id,
        )

    def _execute(
        self,
        session: Any,
        subjects: set[str] | None,
    ) -> list[dict[str, Any]]:
        spec = self.spec
        plan = derive_frontier_plan(
            spec.program,
            check_attrs=spec.program.attrs["check"],
            claim_state_surfaces=spec.claim_state_surface_names(),
        )
        if subjects is not None:
            if not subjects:
                return []
            program = scoped_program(spec.program, plan)
            sql, params = lower_query(program, spec.registries())
            params[SCOPE_PARAM] = sorted(subjects)
        else:
            sql, params = lower_query(spec.program, spec.registries())
        return [dict(row) for row in session.query(to_chronos_sql(sql), params)]

    @staticmethod
    def _universe_count(session: Any) -> int:
        rows = list(session.query(
            "SELECT count(*) AS n FROM okg.graph_nodes", {}
        ))
        return int(rows[0]["n"]) if rows else 0

    # ------------------------------------------------------------------ #
    # observation lifecycle emission                                     #
    # ------------------------------------------------------------------ #
    def _emit(
        self,
        conn: psycopg.Connection[Any],
        definition: Any,
        *,
        run_id: UUID,
        target_generation_id: int,
        check_attrs: Mapping[str, Any],
        offenders: list[dict[str, Any]],
        scoped_subjects: set[str] | None,
    ) -> dict[str, int]:
        spec = self.spec
        subject_role = check_attrs.get("subject_role") or next(
            iter(check_attrs["participant_roles"])
        )
        predicate_key = str(check_attrs["predicate_key"])
        normalized_predicate = str(check_attrs["normalized_predicate"])
        severity = check_attrs.get("severity")
        value_columns = check_attrs.get("normalized_value_columns")
        scope_columns = check_attrs.get("finding_scope_columns")
        payload_column = check_attrs.get("payload_column")
        evidence_column = check_attrs.get("evidence_column")

        records: list[ConsistencyCandidateObservation] = []
        offender_scopes: set[str] = set()
        for row in offenders:
            roles = {
                str(role): str(row[column])
                for role, column in check_attrs["participant_roles"].items()
            }
            subject = roles[subject_role]
            input_scope = f"ir:{normalized_predicate}:{subject}"
            if scope_columns:
                # Per-item identity: a subject can hold SEVERAL findings of
                # this predicate (one per link/claim/element). The suffix keys
                # each finding so resolution stays per-item — without it, one
                # remaining bad item would hold every sibling finding open.
                input_scope += ":" + _finding_scope_digest(
                    row, scope_columns, check_id=spec.check_id
                )
            offender_scopes.add(input_scope)
            normalized_values = _normalized_values_for_row(
                row, value_columns, check_id=spec.check_id
            )
            payload = _mapping_column(
                row, payload_column, check_id=spec.check_id, attr="payload_column"
            )
            evidence = _mapping_column(
                row, evidence_column, check_id=spec.check_id, attr="evidence_column"
            )
            candidate = ConsistencyCandidate(
                input_scope=input_scope,
                blocking_rule_hash=spec.program.checksum(),
                participant_roles=roles,
                predicate_key=predicate_key,
                generation_policy=GENERATION_POLICY,
            )
            candidate_id = candidate_id_for(definition, candidate)
            observation_id = observation_id_for(
                definition,
                candidate_id=candidate_id,
                input_scope=input_scope,
                participant_roles=roles,
                normalized_predicate=normalized_predicate,
                normalized_values=normalized_values,
                source_revision_policy=GENERATION_POLICY,
            )
            records.append(ConsistencyCandidateObservation(
                candidate=candidate,
                candidate_id=candidate_id,
                event=ObservationEvent(
                    observation_id=observation_id,
                    candidate_id=candidate_id,
                    input_scope=input_scope,
                    predicate_key=predicate_key,
                    normalized_predicate=normalized_predicate,
                    normalized_values=normalized_values,
                    participant_roles=roles,
                    finding_event="observed",
                    severity=severity,
                    confidence=1.0,
                    confidence_type=spec.confidence_type,
                    evidence=(
                        evidence if evidence is not None
                        else {"columns": normalized_values}
                    ),
                    payload=payload or {},
                    coverage_complete=True,
                ),
            ))

        resolved, open_before = self._resolutions(
            conn,
            definition,
            check_attrs=check_attrs,
            offender_scopes=offender_scopes,
            scoped_subjects=scoped_subjects,
            subject_role=subject_role,
        )
        records.extend(resolved)
        if records:
            record_candidate_observations_bulk(
                conn,
                definition,
                target_generation_id=target_generation_id,
                run_id=run_id,
                records=records,
            )
        # Drift oracle (meaningful only on a full sweep, where open_before is
        # the whole open set): offenders that were NOT already open, and opens
        # that no longer offend. On a full run stale_open == len(resolved).
        return {
            "observed": len(offender_scopes),
            "resolved": len(resolved),
            "missed_open": len(offender_scopes - open_before),
            "stale_open": len(open_before - offender_scopes),
        }

    def _resolutions(
        self,
        conn: psycopg.Connection[Any],
        definition: Any,
        *,
        check_attrs: Mapping[str, Any],
        offender_scopes: set[str],
        scoped_subjects: set[str] | None,
        subject_role: str,
    ) -> tuple[list[ConsistencyCandidateObservation], set[str]]:
        """Open observations whose subject was evaluated and no longer offends.

        Full run: every open observation of this check was evaluated. Scoped
        run: only those whose subject is inside the derived scope — subjects
        outside the scope were not evaluated and MUST NOT be resolved.

        Scope-shape migration churn (known, one cycle): resolution matches by
        exact input_scope, so changing a check's finding_scope_columns makes
        every open observation's OLD scope miss the new offender set — still-
        real findings resolve and immediately re-observe under their new
        scopes in the same run. Net state is correct; finding continuity and
        that cycle's resolved_count are not. Expect it when migrating a check
        onto (or between) per-item scopes.

        Returns the resolution records AND the set of pre-sweep open
        input_scopes (every open row this run fetched), so the caller can
        compute the drift oracle (missed/stale) without a second query.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH latest_historical_candidates AS MATERIALIZED (
                  SELECT DISTINCT ON (candidate.candidate_id)
                         candidate.candidate_id,
                         candidate.blocking_rule_hash,
                         candidate.generation_policy
                    FROM okg.consistency_candidates candidate
                   WHERE candidate.check_id = %(check_id)s
                   ORDER BY candidate.candidate_id,
                            candidate.target_generation_id DESC,
                            candidate.created_at DESC
                )
                SELECT observation.observation_id, observation.candidate_id,
                       observation.input_scope, observation.predicate_key,
                       observation.normalized_predicate,
                       observation.normalized_values,
                       observation.participant_roles, observation.severity,
                       historical.blocking_rule_hash,
                       historical.generation_policy
                  FROM okg.observation_current observation
                  LEFT JOIN latest_historical_candidates historical
                    ON historical.candidate_id = observation.candidate_id
                 WHERE observation.check_id = %(check_id)s
                   AND observation.finding_status = 'open'
                """,
                {"check_id": definition.check_id},
            )
            rows = cur.fetchall()

        open_scopes_before: set[str] = {str(row[2]) for row in rows}
        out: list[ConsistencyCandidateObservation] = []
        for (observation_id, candidate_id, input_scope, predicate_key,
             normalized_predicate, normalized_values, participant_roles,
             severity, blocking_rule_hash, generation_policy) in rows:
            if input_scope in offender_scopes:
                continue  # still offending
            roles = dict(participant_roles or {})
            subject = roles.get(subject_role)
            if scoped_subjects is not None and subject not in scoped_subjects:
                continue  # not evaluated this run — must stay open
            if blocking_rule_hash is None or generation_policy is None:
                raise HistoricalCandidateIdentityUnverifiable(
                    "historical candidate identity unverifiable: no persisted "
                    f"candidate row for check {definition.check_id!r}, "
                    f"candidate {candidate_id!r}"
                )
            candidate = ConsistencyCandidate(
                input_scope=str(input_scope),
                blocking_rule_hash=str(blocking_rule_hash),
                participant_roles=roles,
                predicate_key=str(predicate_key),
                generation_policy=str(generation_policy),
            )
            out.append(ConsistencyCandidateObservation(
                candidate=candidate,
                candidate_id=str(candidate_id),
                event=ObservationEvent(
                    observation_id=str(observation_id),
                    candidate_id=str(candidate_id),
                    input_scope=str(input_scope),
                    predicate_key=str(predicate_key),
                    normalized_predicate=str(normalized_predicate),
                    normalized_values=dict(normalized_values or {}),
                    participant_roles=roles,
                    finding_event="resolved",
                    severity=severity,
                    confidence=1.0,
                    confidence_type=self.spec.confidence_type,
                    # the subject WAS fully evaluated this run (full mode, or
                    # inside the derived scope) — resolution is honest.
                    coverage_complete=True,
                ),
            ))
        return out, open_scopes_before


def _normalized_values_for_row(
    row: Mapping[str, Any],
    value_columns: Any,
    *,
    check_id: str,
) -> dict[str, Any]:
    """The stored finding identity payload: EXACTLY the declared columns.

    ``observation_id_for`` folds this dict, so the declaration IS the finding's
    identity closure. A finding's identity must be a pure function of (check,
    satisfying assignment), keyed off the check's declared
    ``normalized_value_columns`` and nothing else — the old projection-derived
    fallback (``sorted(row)`` / an alphabetical sweep of every projected column)
    is DELETED (#691; PACT consistency-finding-identity,
    req.identity.declared-or-refused), never conditionally bypassed. A volatile
    projected column (a timestamp, a rowcount) would otherwise churn
    ``observation_id`` every generation and mint phantom duplicate findings for
    one real problem.

    The declaration is enforced at load (``ir_check._validate_program``); the
    undeclared-closure raise here is the executor's backstop for
    directly-constructed specs that skipped the loader. It is a typed
    ``UndeclaredFindingIdentity`` (also a ``ValueError``) naming the check and
    the missing declaration, so the refusal carries a wire error code / fix_hint
    yet still satisfies the broad ``ValueError`` contract the finding-identity
    engine asserts on this path. A declared column the program does not project
    is an authoring bug that errors loudly — never a silent None.

    The equivalence harness (``equivalence._stored_normalized_values``) calls
    this same function, so the harness diffs exactly what the executor
    persists and the two sides cannot drift.
    """
    if not value_columns:
        raise UndeclaredFindingIdentity(
            f"check {check_id!r}: normalized_value_columns is undeclared; the "
            "executor refuses to key finding identity on the whole offender row "
            "(a volatile column would churn observation_id every generation and "
            "mint phantom duplicate findings). Declare normalized_value_columns.",
            data={"check_id": check_id},
            fix_hint=(
                "declare normalized_value_columns on this ir_consistency_checks "
                "entry (the closure that keys finding identity)"
            ),
        )
    values: dict[str, Any] = {}
    for column in value_columns:
        column = str(column)
        if column not in row:
            raise ValueError(
                f"check {check_id!r}: normalized_value_columns column "
                f"{column!r} is not in the program's offender row"
            )
        values[column] = _jsonable(row[column])
    return values


# Back-compat alias: the #691 refusal test suite
# (tests/substrate/consistency/test_ir_executor_identity.py) imports the helper
# under its original name. The dev-side finding-identity engine renamed it to
# ``_normalized_values_for_row``; both names refer to the one identity helper so
# neither the executor nor the equivalence harness can drift from it.
def _normalized_values(
    row: Mapping[str, Any],
    value_columns: Any,
    *,
    check_id: str,
) -> dict[str, Any]:
    return _normalized_values_for_row(
        row,
        value_columns,
        check_id=check_id,
    )


def _finding_scope_digest(
    row: Mapping[str, Any],
    scope_columns: Any,
    *,
    check_id: str,
) -> str:
    """Deterministic per-item scope suffix from the declared columns.

    A missing column is an authoring bug and errors loudly — a silently
    empty suffix would collapse distinct findings back onto one scope.
    Values are length-prefixed before joining so no value content can
    collide across column boundaries; a JSON null and an empty string
    deliberately coincide (both render as SQL-empty text).
    """
    parts: list[str] = []
    for column in scope_columns:
        column = str(column)
        if column not in row:
            raise ValueError(
                f"check {check_id!r}: finding_scope_columns column "
                f"{column!r} is not in the program's offender row"
            )
        value = row[column]
        parts.append("" if value is None else str(value))
    joined = "\x1f".join(f"{len(p)}:{p}" for p in parts)  # length-prefixed
    return hashlib.md5(joined.encode("utf-8"), usedforsecurity=False).hexdigest()


def _mapping_column(
    row: Mapping[str, Any],
    column: Any,
    *,
    check_id: str,
    attr: str,
) -> dict[str, Any] | None:
    """A program-projected jsonb-object column (triage payload/evidence).

    Undeclared -> None (caller keeps its default). Declared but missing or
    not a mapping -> loud error, never a silent default.
    """
    if column is None:
        return None
    column = str(column)
    if column not in row:
        raise ValueError(
            f"check {check_id!r}: {attr} column {column!r} is not in the "
            "program's offender row"
        )
    value = row[column]
    if not isinstance(value, Mapping):
        raise ValueError(
            f"check {check_id!r}: {attr} column {column!r} must be a jsonb "
            f"object; got {type(value).__name__}"
        )
    return dict(value)


def _compat_generation_int(
    conn: psycopg.Connection[Any],
    generation_id: int | str,
    *,
    deployment_name: str,
    branch_name: str,
) -> int:
    """derived_artifact_runs.target_generation_id is the compat INTEGER id.

    A Chronos checkpoint-id string resolves through the shared generation
    resolver's checkpoint metadata; a plain integer passes through.
    """
    if isinstance(generation_id, int):
        return generation_id
    text = str(generation_id)
    if text.isdigit():
        return int(text)
    from okg.substrate.generation_resolver import resolve_okg_generation

    resolved = resolve_okg_generation(
        conn,
        deployment_name=deployment_name,
        requested=text,
        branch=branch_name,
        status=None,
    )
    if resolved is None or resolved.target_generation_id is None:
        raise ValueError(
            f"cannot resolve a compat integer generation id for {generation_id!r}"
        )
    return int(resolved.target_generation_id)


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool, list, dict)):
        return value
    return str(value)


__all__ = ["IrPostPublishCheck", "GENERATION_POLICY"]
