"""The reuse pass as a durable step inside the publisher DBOS workflow (§9.6).

PACT ``consistency-resolution-aware-reuse``, task.reuse.scheduler
(req.reuse.protocol + req.reuse.single-mechanism). Normative semantics in
docs/develop/architecture/consistency.md §9.6 and
docs/develop/architecture/runtime.md.

The scheduler engine lives in
:mod:`okg.substrate.consistency.reuse_scheduler` (pure, deterministic). This
module makes it RIDE the runtime: it registers the pass as a named DBOS step (the
same idiom as :mod:`okg.substrate.runtime.publisher_workflow`), applies the commit
gate before the checkpoint, and executes the loud fallback ladder:

* fixed point → commit the certificates ATOMICALLY with the generation checkpoint
  (one transaction — the DBOS same-database boundary);
* discard (unknown dependency / envelope violation) → roll back the tentative
  certificates and execute the complete path (the force-full lever,
  :func:`run_dependency_baseline`);
* abort (operator failure) → roll back and leave prior state intact (no checkpoint,
  no certificate rows for the aborted generation).

The pass is deterministic from its inputs, so a worker killed between the
scheduler step and the checkpoint resumes to the SAME decisions (a byte-equal
:class:`ReusePassResult` digest) or discards cleanly — never a half-commit.

Opt-in
------
The publish-cycle wiring is opt-in per deployment: a ``consistency.
resolution_aware_reuse`` flag in ``deployment.yaml`` (consumed by the publisher
stack, :func:`okg.substrate.publisher.deployment_stack.load_resolution_aware_reuse_enabled`).
Only ``consistency-fixture`` opts in this wave; every other deployment's cycle is
unchanged (:func:`run_post_cycle_reuse_pass` returns a ``disabled`` marker).

Boundary (v1 scope, stated so it is not mistaken for more): the mock-DBOS
witnesses of this wave pass the connection + cone inputs to the step directly (the
established SDK test idiom — the mock ``@DBOS.step`` is a pass-through). Under a
LIVE DBOS singleton a step rebuilds its connection and cone from serializable
identity; that live-singleton wiring, and the generic (non-fixture) cone builder,
are ``task.reuse.live-proof``. This module builds the CLAIM-PLANE cone for the
fixture only.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from okg.substrate.consistency.reuse_certificates import (
    CertificateDependencies,
    ReuseCertificate,
    ReuseCertificateStore,
)
from okg.substrate.consistency.reuse_scheduler import (
    ReusePassResult,
    ReuseRungConfig,
    ReuseStep,
    commit_gate,
    run_reuse_pass,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.producer_authority import summarize_envelope_violations

__all__ = [
    "REUSE_PASS_STEP_NAME",
    "CLAIM_PRODUCER_STEP",
    "register_reuse_pass_step",
    "reuse_pass_step",
    "poisoned_producers_for_generation",
    "commit_maintenance_state",
    "run_reuse_pass_in_cycle",
    "run_post_cycle_reuse_pass",
    "build_claim_plane_cone",
    "build_dominance_skip_cone",
    "DominanceSkipCone",
]

log = logging.getLogger(__name__)

# Stable DBOS step name (operator tooling filters on it).
REUSE_PASS_STEP_NAME = "okg_reuse_pass"

# Module-level handle to the decorated step (populated by register_*).
_DECORATED: dict[str, Any] = {}


# --------------------------------------------------------------------------- #
# DBOS step registration (mirrors publisher_workflow.register_*).             #
# --------------------------------------------------------------------------- #
def register_reuse_pass_step(*, deployment: str) -> None:
    """Decorate the reuse pass as a DBOS step for ``deployment``.

    Called from the worker entry point after ``DBOS()`` is constructed but before
    ``DBOS.launch()``. Idempotent: clears the cache on entry so a fresh worker boot
    does not pick up a stale closure. The step body is the deterministic scheduler
    pass (:func:`run_reuse_pass`); the checkpoint commit is a distinct phase
    (:func:`commit_maintenance_state`), so a crash between them resumes or discards
    without a half-commit."""
    from dbos import DBOS

    _DECORATED.clear()

    @DBOS.step(name=REUSE_PASS_STEP_NAME, retries_allowed=False)
    def reuse_pass_step(conn: Any, **inputs: Any) -> ReusePassResult:
        """Run the scheduler pass for the bound deployment; return the result.

        The pass records each skip's certificate into ``conn``'s open transaction
        (the certificate store's ``record_skip``), so a caller that rolls back
        leaves no certificate rows and a caller that commits lands them atomically
        with the checkpoint."""
        inputs.setdefault("deployment", deployment)
        return run_reuse_pass(conn, **inputs)

    _DECORATED[REUSE_PASS_STEP_NAME] = reuse_pass_step
    log.info(
        "registered reuse pass step: deployment=%s name=%s",
        deployment, REUSE_PASS_STEP_NAME,
    )


def reuse_pass_step() -> Callable[..., ReusePassResult]:
    """Public accessor for the decorated reuse pass step."""
    if REUSE_PASS_STEP_NAME not in _DECORATED:
        raise RuntimeError(
            "reuse pass step not registered; call register_reuse_pass_step() "
            "before DBOS.launch()"
        )
    return _DECORATED[REUSE_PASS_STEP_NAME]


# --------------------------------------------------------------------------- #
# Envelope-violation poisoning set (per generation).                          #
# --------------------------------------------------------------------------- #
def poisoned_producers_for_generation(
    conn: Any, *, deployment: str, generation_id: int | str | None = None,
) -> frozenset[str]:
    """The producers whose out-of-envelope outputs poison reuse this generation.

    Reads ``okg.envelope_violations`` via
    :func:`summarize_envelope_violations`; any producer with a poisoning row is
    returned. A reached step whose producer is in this set discards the pass into
    complete execution (§9.6). Over-approximation is sound — a producer that
    violated its envelope cannot be trusted to reuse."""
    summary = summarize_envelope_violations(
        conn, deployment=deployment, generation_id=generation_id,
    )
    return frozenset(
        str(row["producer_id"])
        for row in summary.get("by_producer", [])
        if row.get("poisons_reuse")
    )


# --------------------------------------------------------------------------- #
# Phase 2 — commit the maintenance state atomically with the checkpoint.      #
# --------------------------------------------------------------------------- #
def commit_maintenance_state(
    conn: Any,
    result: ReusePassResult,
    *,
    checkpoint_writer: Callable[[Any], Any] | None = None,
) -> dict[str, Any]:
    """Commit the certificates atomically with the generation checkpoint (§9.6).

    Refuses (via :func:`commit_gate`) unless the pass reached a fixed point. The
    certificates are already recorded into ``conn``'s open transaction by the pass;
    ``checkpoint_writer(conn)`` writes the generation checkpoint in the SAME
    transaction, and the single ``conn.commit()`` lands both — so a crash before
    this returns leaves neither (never a half-commit)."""
    commit_gate(result)  # raises CommitBeforeFixedPoint unless at fixed point
    checkpoint = None
    if checkpoint_writer is not None:
        checkpoint = checkpoint_writer(conn)
    conn.commit()
    log.info(
        "reuse maintenance state committed: deployment=%s gen=%s skips=%d runs=%d "
        "digest=%s",
        result.deployment, result.generation_id,
        len(result.skips), len(result.runs), result.digest()[:12],
    )
    return {
        "status": "committed",
        "terminal": result.terminal,
        "pass_digest": result.digest(),
        "certificates_committed": len(result.skips),
        "checkpoint": checkpoint,
    }


# --------------------------------------------------------------------------- #
# The publish-cycle integration point — pass + gate + loud fallback ladder.   #
# --------------------------------------------------------------------------- #
def run_reuse_pass_in_cycle(
    conn: Any,
    *,
    deployment: str,
    generation_id: str,
    worklist_seed: Sequence[str],
    steps: Mapping[str, ReuseStep],
    checks: Sequence[Any] = (),
    prior_certificates: Mapping[str, ReuseCertificate] | None = None,
    # Per-step change STATEMENTS. Omitting a step is not "nothing changed": the
    # scheduler reads the omission as UNSTATED_CHANGE and refuses that step a
    # justified skip. Say ChangedStateTerms.nothing_changed() when nothing did.
    changed_terms: Mapping[str, Any] | None = None,
    current_facts: Mapping[str, CertificateDependencies] | None = None,
    prior_facts: Mapping[str, CertificateDependencies] | None = None,
    prior_generation_id: str | None = None,
    incumbent_selection: Mapping[str, Mapping[str, Any]] | None = None,
    dominance_inputs: Mapping[str, Any] | None = None,
    step_runner: Callable[[str], Sequence[str]] | None = None,
    cert_store: ReuseCertificateStore | None = None,
    poisoned_producers: frozenset[str] = frozenset(),
    # WHICH reuse rungs the pass may use (E3 mechanism ablation). A recorded part
    # of the run configuration, carried into the ReusePassResult; None = the
    # complete production ladder.
    rungs: ReuseRungConfig | None = None,
    checkpoint_writer: Callable[[Any], Any] | None = None,
    complete_executor: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Run the reuse pass to a fixed point and take the correct terminal action.

    The single publish-cycle integration point. Runs the pass (recording tentative
    certificates into ``conn``'s transaction), then:

    * fixed point → :func:`commit_maintenance_state` (certificates + checkpoint in
      one transaction);
    * discard (unknown dependency / envelope violation) → ``conn.rollback()`` drops
      the tentative certificates, then ``complete_executor()`` runs the force-full
      complete path (no certificates for the discarded pass);
    * abort (operator failure) → ``conn.rollback()`` and NO checkpoint (prior state
      intact — the aborted generation has no certificate rows).

    ``conn`` must be a real transaction-scoped connection (``autocommit=False``);
    the rollback on discard/abort is what makes "no certificates for the discarded
    /aborted pass" structural, not a promise. Returns a summary carrying the
    terminal state, the fallback cause, and the byte-stable pass digest."""
    result = run_reuse_pass(
        conn,
        deployment=deployment,
        generation_id=generation_id,
        worklist_seed=worklist_seed,
        steps=steps,
        checks=checks,
        prior_certificates=prior_certificates,
        changed_terms=changed_terms,
        current_facts=current_facts,
        prior_facts=prior_facts,
        prior_generation_id=prior_generation_id,
        incumbent_selection=incumbent_selection,
        dominance_inputs=dominance_inputs,
        step_runner=step_runner,
        cert_store=cert_store,
        poisoned_producers=poisoned_producers,
        rungs=rungs,
    )

    if result.reached_fixed_point:
        commit = commit_maintenance_state(
            conn, result, checkpoint_writer=checkpoint_writer,
        )
        return {
            "reuse_pass": "committed",
            "result": result,
            "terminal": result.terminal,
            "fallback_cause": None,
            "pass_digest": result.digest(),
            **commit,
        }

    if result.discarded:
        # Drop the tentative certificates — the discarded pass writes zero.
        conn.rollback()
        complete = None
        if complete_executor is not None:
            complete = complete_executor()
        log.warning(
            "reuse pass discarded into complete execution: deployment=%s gen=%s "
            "cause=%s failed_step=%s",
            deployment, generation_id, result.fallback_cause, result.failed_step,
        )
        return {
            "reuse_pass": "discarded",
            "result": result,
            "terminal": result.terminal,
            "fallback_cause": result.fallback_cause,
            "failed_step": result.failed_step,
            "pass_digest": result.digest(),
            "certificates_committed": 0,
            "complete_execution": complete,
        }

    # Aborted (operator failure): prior state intact, no checkpoint, no certs.
    conn.rollback()
    log.error(
        "reuse pass aborted (operator failure); prior state intact: "
        "deployment=%s gen=%s failed_step=%s",
        deployment, generation_id, result.failed_step,
    )
    return {
        "reuse_pass": "aborted",
        "result": result,
        "terminal": result.terminal,
        "fallback_cause": result.fallback_cause,
        "failed_step": result.failed_step,
        "pass_digest": result.digest(),
        "certificates_committed": 0,
    }


# --------------------------------------------------------------------------- #
# The opt-in seam — read the flag, build the fixture cone, run the pass.       #
# --------------------------------------------------------------------------- #
def run_post_cycle_reuse_pass(
    *,
    dsn: str,
    deployment: str,
    summary: Mapping[str, Any],
    deployment_dir: Any | None = None,
) -> dict[str, Any]:
    """Publisher-workflow hook: run the reuse pass after a published cycle, IFF
    the deployment opted in.

    Reads the opt-in flag through the publisher stack loader. When the deployment
    has NOT opted in, returns an explicit ``disabled`` marker (never a silent
    branch) and touches nothing — every non-fixture deployment's cycle is
    unchanged. When opted in, builds the claim-plane cone for the published
    generation and runs the pass, committing the certificates. Exceptions are
    isolated so a reuse-pass failure never turns a successful publish into a
    crash (the pass is post-publish maintenance state)."""
    from okg.substrate.publisher.deployment_stack import (
        load_resolution_aware_reuse_enabled,
    )

    if deployment_dir is None:
        from okg.substrate.deployments import find_deployment_dir
        deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None or not load_resolution_aware_reuse_enabled(
        deployment_dir
    ):
        return {"reuse_pass": "disabled", "deployment": deployment}

    generation_id = summary.get("compat_generation_id")
    if generation_id is None:
        generation_id = summary.get("generation_id")
    if summary.get("status") != "published" or generation_id is None:
        return {"reuse_pass": "skipped", "reason": "no_published_generation"}

    try:
        return _run_fixture_cone_pass(
            dsn=dsn, deployment=deployment, generation_id=str(generation_id),
        )
    except Exception as exc:  # noqa: BLE001 - post-publish maintenance is isolated
        log.exception(
            "reuse pass post-cycle hook failed: deployment=%s gen=%s",
            deployment, generation_id,
        )
        return {
            "reuse_pass": "error",
            "deployment": deployment,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _run_fixture_cone_pass(
    *, dsn: str, deployment: str, generation_id: str,
) -> dict[str, Any]:
    """Run the claim-plane cone pass for a published fixture generation.

    v1 (fixture-scoped): the cone's steps are the claim-producing step and the two
    claim derivations. This models the STEADY-STATE unchanged cycle — the prior
    generation's manifest is byte-identical to the current one (``prior_facts`` is
    the same deterministic snapshot) — so every cone step skips behind a DEGENERATE
    (unchanged-inputs carry-forward) certificate, the single skip mechanism, without
    perturbing the resolved state. The generic cone builder — changed-term derivation
    from the live delta, real prior certificates and manifests — is
    ``task.reuse.live-proof``.

    The change statement is EXPLICIT, not omitted. This v1 cone models the
    unchanged steady state, so it truthfully declares
    ``ChangedStateTerms.nothing_changed()`` per step. Its safety is therefore
    stated rather than incidental: previously the omission was read as an empty
    (permissive) statement, and this pass was only safe because the degenerate
    carry-forward rung fired first. When the generic cone builder makes
    ``prior_facts`` differ from ``current_facts``, this callsite already has to say
    what changed — and a wrong statement here is a visible edit, not a silence."""
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.consistency.possible_effects import ChangedStateTerms

    steps, seed, facts, incumbent = build_claim_plane_cone(deployment)
    # v1 fixture scope: the cycle genuinely changed no claim bucket and no graph
    # check finding. Declared per step, so the justified-skip rung sees a
    # STATEMENT; the degenerate rung below still decides first.
    nothing_changed = ChangedStateTerms.nothing_changed()
    changed_terms = {step_id: nothing_changed for step_id in steps}
    store = ReuseCertificateStore(deployment=deployment)
    with db_connect(dsn) as conn:  # autocommit=False -> one transaction
        poisoned = poisoned_producers_for_generation(
            conn, deployment=deployment, generation_id=generation_id,
        )
        return run_reuse_pass_in_cycle(
            conn,
            deployment=deployment,
            generation_id=generation_id,
            worklist_seed=seed,
            steps=steps,
            current_facts=facts,
            changed_terms=changed_terms,
            # the prior generation's manifest == this one (unchanged steady
            # state) -> each cone step skips behind a degenerate certificate.
            prior_facts=facts,
            incumbent_selection=incumbent,
            cert_store=store,
            poisoned_producers=poisoned,
        )


# The claim-plane cone step ids for the fixture (declared, not discovered — the
# generic discovery is live-proof).
CLAIM_SOURCE_STEP = "claim_source"
CLAIM_EFFECTIVE_STEP = "claim_effective"
CLAIM_PROJECTION_STEP = "claim_projection"


def build_claim_plane_cone(
    deployment: str,
) -> tuple[
    dict[str, ReuseStep],
    tuple[str, ...],
    dict[str, CertificateDependencies],
    dict[str, dict[str, Any]],
]:
    """Build the fixture's claim-plane cone: steps + seed + facts + incumbent.

    The cone is the claim-producing step feeding the two claim derivations
    (effective merge, then projection+selection). Each step carries a BOUNDED
    write envelope over the claim plane and a deterministic dependency-fact
    snapshot, so when the caller supplies ``prior_facts`` equal to these facts
    (the unchanged steady state) every cone step skips behind a DEGENERATE
    certificate (§9.5, the a2 rung). The incumbent is the carry-forward decision.
    Returns ``(steps, worklist_seed, current_facts, incumbent_selection)`` — the
    fixture-scoped v1 shape."""
    from okg.substrate.producer_authority import normalize_write_envelope

    claim_envelope = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim",
                    "clause_id": f"{deployment}:claim"}]},
        source_name=CLAIM_SOURCE_STEP,
    )
    steps = {
        CLAIM_SOURCE_STEP: ReuseStep(
            step_id=CLAIM_SOURCE_STEP, envelope=claim_envelope,
            producer_id=CLAIM_SOURCE_STEP,
        ),
        CLAIM_EFFECTIVE_STEP: ReuseStep(
            step_id=CLAIM_EFFECTIVE_STEP, envelope=claim_envelope,
            producer_id=CLAIM_EFFECTIVE_STEP,
        ),
        CLAIM_PROJECTION_STEP: ReuseStep(
            step_id=CLAIM_PROJECTION_STEP, envelope=claim_envelope,
            producer_id=CLAIM_PROJECTION_STEP,
        ),
    }
    facts = {
        step_id: _degenerate_facts(deployment, step_id) for step_id in steps
    }
    incumbent = {
        step_id: {"outcome": "carry_forward", "basis": "unchanged_inputs"}
        for step_id in steps
    }
    seed = (CLAIM_SOURCE_STEP, CLAIM_EFFECTIVE_STEP, CLAIM_PROJECTION_STEP)
    return steps, seed, facts, incumbent


def _degenerate_facts(deployment: str, step_id: str) -> CertificateDependencies:
    """A minimal, well-typed dependency-fact snapshot for a cone step.

    Every dependency class is present (a certificate never omits one, §9.5); the
    fingerprints are the deployment/step identity so two runs of the same pass
    build byte-identical certificates (the durability property)."""
    return CertificateDependencies(
        input_digests={step_id: f"{deployment}:{step_id}:inputs"},
        operator_versions={step_id: "v1"},
        model_prompt_keys={},
        envelope_fingerprint=f"{deployment}:{step_id}:envelope",
        constraint_set={},
        generator_versions={},
        policy_fingerprint=f"{deployment}:policy",
        precedence_fingerprint=f"{deployment}:precedence",
        competitor_bounds={},
        catalog_version_id=f"{deployment}:catalog",
    )


# The generic changed-inputs-skip producer step id (task.reuse.live-proof): the
# claim-producing step whose INPUT changed (a new crawl-class claim arrived) but
# whose every permitted output provably loses to the decisive incumbent, so the
# scheduler skips it behind a resolution_dominance certificate (§9.5 / §9.7).
CLAIM_PRODUCER_STEP = "claim_producer"


@dataclass(frozen=True)
class DominanceSkipCone:
    """A cone whose producer step SKIPS behind a bounded-dominance certificate.

    The CENTREPIECE changed-inputs skip (task.reuse.live-proof): the producer
    step's input changed (a crawl-class claim arrived for a bucket the decisive
    incumbent already wins), but its declared envelope constrains every permitted
    output to a source class whose authority loses to the incumbent's decisive
    ``declared_source_authority`` basis. Feed the fields to
    :func:`run_reuse_pass_in_cycle`; the pass records a ``resolution_dominance``
    certificate over :attr:`covered_bucket_row_key` — the bucket the differential
    run therefore never extracts the loser into, and the exact bucket the
    equivalence differ's ``CertifiedDifferences`` licenses (§9.7)."""

    steps: dict[str, "ReuseStep"]
    worklist_seed: tuple[str, ...]
    current_facts: dict[str, CertificateDependencies]
    incumbent_selection: dict[str, dict[str, Any]]
    changed_terms: dict[str, Any]
    dominance_inputs: dict[str, Any]
    covered_bucket_row_key: str


def build_dominance_skip_cone(
    deployment: str,
    *,
    predicate: str,
    normalized_key: str,
    incumbent_authority: int,
    incumbent_basis: str,
    competitor_source_classes: Sequence[str],
    competitor_authority_max: int,
    producer_step: str = CLAIM_PRODUCER_STEP,
    incumbent_outcome: str = "selected",
) -> "DominanceSkipCone":
    """Build the bounded-dominance changed-inputs-skip cone for one bucket (§9.5).

    The producer step declares a claim write envelope over ``(predicate,
    normalized_key)`` bounded to ``competitor_source_classes`` with a closed
    source-authority ceiling ``competitor_authority_max``. Its input changed
    (``changed_terms`` overlaps the incumbent bucket), so set-disjointness refuses;
    the bounded-dominance rung then proves every permitted output loses to the
    decisive incumbent (basis ``incumbent_basis`` — must be
    ``declared_source_authority`` — winning authority ``incumbent_authority``,
    which must strictly exceed the competitor ceiling) and the pass skips behind a
    ``resolution_dominance`` certificate.

    Returns a :class:`DominanceSkipCone`; the caller runs
    :func:`run_reuse_pass_in_cycle` with its fields and licenses the differential
    run's never-extracted loser through
    :attr:`DominanceSkipCone.covered_bucket_row_key`."""
    from okg.substrate.consistency.possible_effects import ChangedStateTerms
    from okg.substrate.consistency.reuse_scheduler import (
        DominanceInput,
        IncumbentDecision,
        ReuseStep,
        derive_competitor_bound,
    )
    from okg.substrate.derived_artifacts.claim_projection_derivation import (
        bucket_row_key,
    )
    from okg.substrate.producer_authority import normalize_write_envelope

    bucket = (predicate, normalized_key)
    # A finite claim write envelope constrained to the competitor source classes /
    # authority ceiling (§9.3). value_vocabularies declares the closed source-class
    # and source-authority vocabularies the dominance rung reads.
    envelope = normalize_write_envelope(
        {
            "nodes": [{"subtype": "consistency_claim",
                       "clause_id": f"{deployment}:{producer_step}"}],
            "claim_keys": [normalized_key],
            "value_vocabularies": {
                "source_class": list(competitor_source_classes),
                "source_authority": [str(competitor_authority_max)],
            },
        },
        source_name=producer_step,
    )
    steps = {
        producer_step: ReuseStep(
            step_id=producer_step, envelope=envelope, producer_id=producer_step,
            claim_predicates=(predicate,),
        )
    }
    # The changed input overlaps the incumbent bucket -> set-disjointness refuses,
    # so the bounded-dominance rung is the only path to a skip.
    changed_terms = {
        producer_step: ChangedStateTerms(buckets=frozenset({bucket})),
    }
    incumbent = IncumbentDecision(
        bucket=bucket, outcome=incumbent_outcome, basis=incumbent_basis,
        authority=incumbent_authority,
    )
    # The competitor-best bound is DERIVED FROM the declared envelope (§9.3), never
    # passed around it: the closed source-authority vocabulary's max is the ceiling.
    competitor_bound = derive_competitor_bound(envelope)
    if competitor_bound is None:  # pragma: no cover - the envelope above bounds it
        raise SubstrateError(
            "build_dominance_skip_cone: the producer envelope does not provably "
            "bound the output source authority; dominance is unprovable (§9.5)",
            data={"producer_step": producer_step},
        )
    dominance_inputs = {
        producer_step: DominanceInput(
            incumbents={bucket: incumbent}, competitor_bound=competitor_bound,
        )
    }
    # The recorded dependency facts pin the incumbent + competitor bound on the
    # competitor_bounds class, so a change to either invalidates the certificate.
    facts = {
        producer_step: CertificateDependencies(
            input_digests={producer_step: f"{deployment}:{producer_step}:inputs"},
            operator_versions={producer_step: "v1"},
            model_prompt_keys={},
            envelope_fingerprint=f"{deployment}:{producer_step}:envelope",
            constraint_set={},
            generator_versions={},
            policy_fingerprint=f"{deployment}:policy",
            precedence_fingerprint=f"{deployment}:precedence",
            competitor_bounds={
                "incumbent_authority": incumbent_authority,
                "incumbent_basis": incumbent_basis,
                "competitor_source_classes": sorted(competitor_source_classes),
                "competitor_authority_max": competitor_authority_max,
            },
            catalog_version_id=f"{deployment}:catalog",
        )
    }
    incumbent_selection = {
        producer_step: {"outcome": incumbent_outcome, "basis": incumbent_basis},
    }
    return DominanceSkipCone(
        steps=steps,
        worklist_seed=(producer_step,),
        current_facts=facts,
        incumbent_selection=incumbent_selection,
        changed_terms=changed_terms,
        dominance_inputs=dominance_inputs,
        covered_bucket_row_key=bucket_row_key(predicate, normalized_key),
    )
