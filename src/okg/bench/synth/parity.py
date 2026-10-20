"""Replay a sealed history through three execution modes and grade exact parity.

PACT ``concord-exact-parity``. RUNNER side of the purity boundary (publishing IS
the system under test), built on the two children that landed before it:
``synth-update-histories`` (the seeded history + the per-step sealed gold + the
recorded fact sets) and ``concord-complete-binding`` (the corpus → scheduler
vocabulary map). It invents no scheduling semantics: the decision ladder, the
certificate mechanism, and the commit gate are used exactly as they stand.

The three modes, charged on ONE history
---------------------------------------
``complete``          every reached step RUNS. The worklist is seeded with every
                      document step; the change statement says a first publish
                      plants every bucket the corpus writes, so the ladder
                      refuses every skip. This is the REFERENCE.
``dependency_only``   the worklist is seeded with the documents whose bytes
                      changed (dependency pruning) and every reached step RUNS;
                      no certificate is consulted and none is written. A document
                      the pruning never reached carries its prior result forward
                      — which is the only reuse dependency pruning gets.
``differential``      the worklist is seeded with every document step and the
                      scheduler decides: prior certificates and the prior
                      generation's dependency snapshot are supplied, the change
                      statement is derived TRUTHFULLY from the facts, and the
                      bounded-dominance inputs are derived from the run's OWN
                      prior published state.

Every mode consumes the SAME recorded fact set per update. Extraction ran once,
in ``history_replay``; here a fact only ever comes out of ``step_NN.json``
(:func:`RecordedUpdate.facts`), so a parity verdict can never be an extraction
artifact. :func:`consumed_digest` is the witness.

The truthful change statement
-----------------------------
``skip_is_justifiable`` approves a skip when the change is disjoint from the
step's reach, so an empty statement approves EVERYTHING (the binding measured
this as finding F1; since PR #972 the only way to spell it is the explicit
``ChangedStateTerms.nothing_changed()``, silence being a distinct type that
refuses). The statement therefore has to be true, and it has
to be true *relative to the state the executor actually holds*:
:func:`changed_buckets` diffs the incoming recorded fact set against the fact
list the executor LAST PUBLISHED, not against the previous update's recorded set.
A differential run that skipped a step is holding a state without that step's
output; measuring the change against the recorded set instead would understate it
and hand the ladder a lie. On the FIRST update there is no prior state at all, so
:func:`build_update_cone` states every bucket. On a later truthful no-delta update,
it uses ``ChangedStateTerms.nothing_changed()`` explicitly rather than the
ambiguous empty constructor.

Certificate keying — the binding's open F3, settled
---------------------------------------------------
A publish mints its generation id at the END of a cycle, so the pass is labelled
with a deterministic CANDIDATE id derived from the corpus digest. At zero skips
that was invisible. Here certificates must survive from one update to the next,
so the keying is settled explicitly: **certificates stay keyed by the candidate
label they were written under, and the driver keeps the candidate → published
ledger** (:class:`ModeState.bindings`). The next update loads its predecessor's
certificates with ``ReuseCertificateStore.for_generation(conn, prior_candidate)``.

Two alternatives were rejected. Re-keying the rows to the published generation id
after the publish would need an UPDATE against an append-only store (§9.5).
Labelling the pass with the PRIOR published generation id would make the pass
claim to maintain a generation it is not maintaining, and would collide the moment
an update is absorbed with no new generation. The candidate label carries the
update index, so two updates can never collide on the store's
``(deployment, generation_id, step_id)`` key.

Absorbed updates
----------------
A skip does not extend the worklist — only a RUN's observed dependents do. So an
update in which EVERY document step skips never reaches the producer, assembles
nothing, and publishes nothing: it is ABSORBED. The executor's resolved state is
the pin it already held, which is the correct answer when nothing it could publish
would differ. The certificates still commit (the pass's maintenance state is
real), and the ledger row records ``published=False`` with the carried pin.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from okg.bench.synth.governed import (
    CorpusCone,
    build_corpus_cone,
    document_step_id,
    partition_recorded_facts,
)
from okg.bench.synth.history import History, history_from_json, verify_chain
from okg.bench.synth.history_replay import (
    HISTORY_FILENAME,
    RECORDED_FACTS_DIRNAME,
    REPLAY_FILENAME,
    accepted_state,
    compare_step,
    deserialize_facts,
    facts_digest,
    materialize_base,
    materialize_step,
    state_diff,
)
from okg.bench.synth.runner import (
    SMOKE_DEPLOYMENT,
    RunnerError,
    detection_state,
    publish_fact_list,
    resolutions_at,
)
from okg.substrate.runtime.reuse_pass_workflow import (
    CLAIM_EFFECTIVE_STEP,
    CLAIM_PRODUCER_STEP,
    CLAIM_PROJECTION_STEP,
    CLAIM_SOURCE_STEP,
)

PARITY_SCHEMA = "okg.synth.exact-parity.v1"

MODE_COMPLETE = "complete"
MODE_DEPENDENCY = "dependency_only"
MODE_DIFFERENTIAL = "differential"
MODES: tuple[str, ...] = (MODE_COMPLETE, MODE_DEPENDENCY, MODE_DIFFERENTIAL)

# The three whole-corpus derivations, discovered as observed dependents.
DERIVATION_STEPS: tuple[str, ...] = (
    CLAIM_PRODUCER_STEP, CLAIM_EFFECTIVE_STEP, CLAIM_PROJECTION_STEP,
)

_CLAIM_SUBTYPE = "consistency_claim"


class ParityReplayError(RunnerError):
    """A parity replay could not be executed, or a per-update contract broke."""


# --------------------------------------------------------------------------- #
# The sealed artifact set, rehydrated                                          #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RecordedUpdate:
    """One update's RECORDED fact set — the only source of facts any executor
    reads. ``digest`` is over the serialized rows, so two executors that
    consumed the same file provably consumed the same bytes."""

    index: int
    rows: tuple[Mapping[str, Any], ...]
    digest: str

    def facts(self) -> list[Any]:
        return deserialize_facts(list(self.rows))


@dataclass(frozen=True)
class ParityHistory:
    """The self-contained replay artifact directory, rehydrated and verified."""

    history: History
    updates: tuple[RecordedUpdate, ...]
    replay: Mapping[str, Any]

    def gold_step(self, index: int) -> Any:
        return self.history.steps[index]


def load_parity_history(run_dir: Path) -> ParityHistory:
    """Rehydrate a ``history_replay`` artifact directory and verify its chain.

    Reads ONLY the files that directory holds: the sealed history, the recorded
    fact sets, and the direct-path replay report. A broken gold chain is a loud
    refusal — replaying against tampered gold would grade nothing."""
    run_dir = Path(run_dir)
    sealed = json.loads((run_dir / HISTORY_FILENAME).read_text(encoding="utf-8"))
    history = history_from_json(sealed)
    broken = verify_chain(history)
    if broken:
        raise ParityReplayError(
            f"refusing to replay a history whose gold chain is broken: {broken}"
        )
    facts_dir = run_dir / RECORDED_FACTS_DIRNAME
    updates: list[RecordedUpdate] = []
    for step in history.steps:
        path = facts_dir / f"step_{step.index:02d}.json"
        if not path.is_file():
            raise ParityReplayError(
                f"update {step.index}: no recorded fact set at {path}. Every "
                "executor replays the RECORDED facts; a missing one would force "
                "a re-extraction and make the parity verdict attributable to "
                "extraction drift."
            )
        rows = tuple(json.loads(path.read_text(encoding="utf-8")))
        updates.append(
            RecordedUpdate(index=step.index, rows=rows, digest=facts_digest(list(rows)))
        )
    replay = json.loads((run_dir / REPLAY_FILENAME).read_text(encoding="utf-8"))
    return ParityHistory(
        history=history, updates=tuple(updates), replay=replay,
    )


def consumed_digest(update: RecordedUpdate, facts: Sequence[Any]) -> str:
    """The digest of the facts an executor ACTUALLY deserialized.

    Equal to ``update.digest`` iff the executor consumed the recorded bytes and
    nothing else — the witness that keeps a parity verdict attributable to reuse
    (req.parity.per-update, the replay-determinism acceptance)."""
    from okg.bench.synth.history_replay import serialize_facts

    return facts_digest(serialize_facts(list(facts)))


# --------------------------------------------------------------------------- #
# The truthful change statement                                                #
# --------------------------------------------------------------------------- #
def bucket_label(predicate: str, key: str) -> str:
    """Gold's bucket spelling: ``predicate:normalized_key``."""
    return f"{predicate}:{key}"


def split_bucket_label(label: str) -> tuple[str, str]:
    predicate, _, key = label.partition(":")
    return predicate, key


def claim_signatures(facts: Sequence[Any]) -> dict[tuple[str, str], str]:
    """bucket -> a canonical signature over every claim the fact list puts in it.

    The signature covers the claim id AND every attribute, so any change a claim
    could make to a bucket's resolution (a new value, a new declared authority, a
    claim appearing or vanishing) moves it. Over-approximation is sound here:
    naming a bucket that did not really change costs work, never correctness."""
    grouped: dict[tuple[str, str], list[str]] = {}
    for fact in facts:
        if getattr(fact, "subtype", "") != _CLAIM_SUBTYPE:
            continue
        bucket = (str(fact.attrs["predicate"]), str(fact.attrs["normalized_key"]))
        grouped.setdefault(bucket, []).append(
            json.dumps([fact.node_id, dict(fact.attrs)], sort_keys=True)
        )
    return {
        bucket: json.dumps(sorted(rows), sort_keys=True)
        for bucket, rows in grouped.items()
    }


def changed_buckets(
    held: Mapping[tuple[str, str], str], incoming: Mapping[tuple[str, str], str]
) -> frozenset[tuple[str, str]]:
    """The buckets the incoming fact set changes RELATIVE TO the state held."""
    return frozenset(
        bucket
        for bucket in set(held) | set(incoming)
        if held.get(bucket) != incoming.get(bucket)
    )


# --------------------------------------------------------------------------- #
# The per-update ledger row                                                    #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LedgerRow:
    """One update's charged work in one execution mode (req.parity.ledger)."""

    mode: str
    update: int
    steps_reached: int
    steps_run: int
    skips_by_cause: Mapping[str, int]
    run_causes: Mapping[str, int]
    certificates_written: int
    certificates_invalidated: int
    invalidated_classes: tuple[str, ...]
    terminal: str
    fallback_cause: str | None
    published: bool
    generation_id: str
    cone_steps: int
    # The denominator: cone steps gold says a sound mechanism could have skipped.
    skips_achievable: int
    # Certificate-backed skips (the SKIP_CAUSES sense) inside that set.
    skips_achieved: int
    # Cone steps this mode did not RUN at all — the mode-comparable saving, so a
    # mode that saves by never reaching a step (dependency pruning) is charged
    # for it too rather than reading a definitional zero.
    unrun_achievable: int

    @property
    def steps_skipped(self) -> int:
        return sum(self.skips_by_cause.values())

    @property
    def achieved_ratio(self) -> float | None:
        if not self.skips_achievable:
            return None
        return self.skips_achieved / self.skips_achievable

    @property
    def unrun_ratio(self) -> float | None:
        if not self.skips_achievable:
            return None
        return self.unrun_achievable / self.skips_achievable

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "update": self.update,
            "steps_reached": self.steps_reached,
            "steps_run": self.steps_run,
            "steps_skipped": self.steps_skipped,
            "skips_by_cause": dict(self.skips_by_cause),
            "run_causes": dict(self.run_causes),
            "certificates_written": self.certificates_written,
            "certificates_invalidated": self.certificates_invalidated,
            "invalidated_classes": list(self.invalidated_classes),
            "terminal": self.terminal,
            "fallback_cause": self.fallback_cause,
            "published": self.published,
            "generation_id": self.generation_id,
            "cone_steps": self.cone_steps,
            "skips_achievable": self.skips_achievable,
            "skips_achieved": self.skips_achieved,
            "unrun_achievable": self.unrun_achievable,
        }


@dataclass(frozen=True)
class UpdateExecution:
    """One update executed in one mode: its ledger row, its pin, and its state."""

    mode: str
    index: int
    ledger: LedgerRow
    generation_id: str
    compat_generation_id: str
    published: bool
    consumed_digest: str
    published_claim_ids: frozenset[str]
    accepted: Mapping[str, Mapping[str, Any]]
    realized_diff: Mapping[str, Mapping[str, Any]]
    snapshot: Any                                  # ResolvedStateSnapshot
    covered_buckets: frozenset[str]
    certificate_digests: tuple[str, ...]
    skip_outcomes: tuple[tuple[str, str], ...]     # (step_id, cause)
    run_steps: tuple[str, ...]
    candidate_generation_id: str
    stated_buckets: int
    withholding_steps: tuple[str, ...]
    # The PINNED read this update already performed, carried out so a consumer
    # can score the funnel and the checker's work surface per update without a
    # second database read (req.e2e.capstone). Defaulted so the dataclass stays
    # constructible by the safety witnesses that build one by hand; a consumer
    # that needs them refuses loudly on ``None`` rather than scoring an empty
    # state as a clean one.
    detection: Any = None                          # PipelineState at ``generation_id``
    resolutions: tuple[Any, ...] = ()              # BucketResolution rows at the pin
    #: Every SKIP outcome of this update, paired to the certificate that licensed
    #: it — the pairing ``skip_outcomes`` and ``certificate_digests`` each keep
    #: half of. Recorded, never re-derived; defaulted so the safety witnesses that
    #: build an execution by hand stay constructible.
    skip_records: tuple[Mapping[str, Any], ...] = ()


@dataclass
class ModeState:
    """What one mode's replay carries between updates."""

    published_facts: list[Any] = field(default_factory=list)
    published_ids: frozenset[str] = frozenset()
    # doc -> the claim slice this executor's published state holds for it.
    carried: dict[str, list[Any]] = field(default_factory=dict)
    # docs whose source step has run at least once (so a prior result EXISTS).
    has_prior_result: set[str] = field(default_factory=set)
    prior_facts: dict[str, Any] = field(default_factory=dict)
    prior_candidate: str | None = None
    prior_doc_digests: dict[str, str] = field(default_factory=dict)
    generation_id: str = ""
    compat_generation_id: str = ""
    accepted: dict[str, dict[str, Any]] = field(default_factory=dict)
    # candidate label -> the generation it turned out to maintain (F3's ledger).
    bindings: list[dict[str, Any]] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# The cone for one update                                                      #
# --------------------------------------------------------------------------- #
def build_update_cone(
    *,
    corpus_dir: Path,
    deployment_dir: Path,
    policies: Sequence[str],
    checks: Sequence[Any],
    update: RecordedUpdate,
    facts: Sequence[Any],
    state: ModeState,
    mode: str,
    deployment: str = SMOKE_DEPLOYMENT,
) -> tuple[CorpusCone, frozenset[tuple[str, str]]]:
    """The reached-step cone for one update, with the mode's change statement.

    Returns ``(cone, stated)``.

    ``complete`` and ``dependency_only`` DECLARE the whole corpus changed — the
    binding's first-publish statement. That is an over-approximation, which is
    always sound (it costs work, never correctness), and it is what makes those
    two modes what they claim to be: every reached step is refused a skip and
    RUNS, so ``dependency_only``'s only saving is the pruned worklist seed and
    ``complete``'s is nothing at all.

    ``differential`` gets the TRUTHFUL statement: the buckets this fact set
    changes relative to the fact list the executor LAST PUBLISHED. Measuring
    against the previous update's recorded set instead would understate the
    change for a run that had skipped a step, and hand the ladder a lie.

    With NO prior published state, every bucket is stated above. For a later
    truthful no-delta update, use the explicit ``nothing_changed()`` spelling;
    a bare empty constructor is ambiguous and refused before scheduling."""
    import dataclasses

    from okg.substrate.consistency.possible_effects import ChangedStateTerms

    cone = build_corpus_cone(
        corpus_dir,
        deployment=deployment,
        deployment_dir=Path(deployment_dir),
        policies=tuple(policies),
        phase=f"u{update.index:02d}",
        checks=checks,
        recorded_facts=list(facts),
        declare_source_vocabularies=True,
    )
    if mode in (MODE_COMPLETE, MODE_DEPENDENCY) or not state.published_facts:
        return cone, frozenset(cone.buckets)

    stated = changed_buckets(
        claim_signatures(state.published_facts), claim_signatures(facts)
    )
    if not stated and not state.generation_id:  # pragma: no cover - guarded above
        raise ParityReplayError(
            f"update {update.index}: an EMPTY change statement with no prior "
            "published state would approve a skip for every step and keep a "
            "result that does not exist (concord-complete-binding finding F1)"
        )
    terms = (
        ChangedStateTerms.nothing_changed()
        if not stated
        else ChangedStateTerms(buckets=stated)
    )
    changed_document_steps = {
        document_step_id(doc)
        for doc in cone.docs
        if state.prior_doc_digests.get(doc) != cone.doc_digests[doc]
    }
    changed_terms = {
        step: (
            current
            if step in changed_document_steps and current.unknown
            else terms
        )
        for step, current in cone.changed_terms.items()
    }
    return (
        dataclasses.replace(cone, changed_terms=changed_terms),
        stated,
    )


# --------------------------------------------------------------------------- #
# Bounded-dominance inputs, derived from the executor's OWN prior state        #
# --------------------------------------------------------------------------- #
def dominance_inputs_for(
    *,
    cone: CorpusCone,
    prior_resolutions: Sequence[Any],
    prior_facts_by_claim: Mapping[str, Mapping[str, str]],
) -> dict[str, Any]:
    """Per-document-step dominance inputs, conservative-or-refuse.

    An incumbent is offered for a bucket ONLY when the executor's own prior
    published state recorded a decisive winner whose basis is
    ``declared_source_authority`` AND that winner's claim carries a readable
    declared authority. A bucket the analysis cannot vouch for is simply omitted,
    which makes the rung refuse and the step run — running is never wrong.

    The competitor bound comes from the step's DECLARED envelope
    (:func:`...reuse_scheduler.derive_competitor_bound`), never from observed
    output; a step whose envelope does not bound its output authority gets no
    dominance input at all."""
    from okg.substrate.consistency.claim_model.precedence import (
        SIGNAL_SOURCE_AUTHORITY,
    )
    from okg.substrate.consistency.reuse_scheduler import (
        DominanceInput,
        IncumbentDecision,
        derive_competitor_bound,
    )

    incumbents: dict[tuple[str, str], Any] = {}
    for res in prior_resolutions:
        if res.basis != SIGNAL_SOURCE_AUTHORITY or not res.winner_claim_id:
            continue
        winner = prior_facts_by_claim.get(str(res.winner_claim_id))
        if winner is None:
            continue
        raw = winner.get("source_authority")
        if raw is None:
            # The winner declares no authority: the projection ranks it 0, and a
            # zero ceiling can never strictly dominate. Omit rather than assume.
            continue
        try:
            authority = int(raw)
        except (TypeError, ValueError):
            continue
        bucket = (str(res.predicate), str(res.bucket_key))
        incumbents[bucket] = IncumbentDecision(
            bucket=bucket, outcome=str(res.outcome),
            basis=str(res.basis), authority=authority,
        )
    if not incumbents:
        return {}

    out: dict[str, Any] = {}
    for step_id, step in cone.steps.items():
        if not step_id.startswith(f"{CLAIM_SOURCE_STEP}:"):
            continue
        bound = derive_competitor_bound(step.envelope)
        if bound is None:
            continue
        reach = {
            bucket for bucket in incumbents
            if bucket[1] in set(step.envelope.claim_keys)
            and bucket[0] in set(step.claim_predicates)
        }
        if not reach:
            continue
        out[step_id] = DominanceInput(
            incumbents={b: incumbents[b] for b in reach}, competitor_bound=bound,
        )
    return out


# --------------------------------------------------------------------------- #
# Gold's denominator: which reached steps a sound mechanism could have skipped  #
# --------------------------------------------------------------------------- #
def gold_skippable_steps(
    cone: CorpusCone, gold_step: Any, *, checks: Sequence[Any] = ()
) -> frozenset[str]:
    """The reached steps gold says a sound mechanism could have skipped.

    Gold labels BUCKETS: ``changed`` (the accepted answer moved),
    ``touched_but_dominated`` (inputs moved, the answer did not — the skippable
    shape), ``untouched_inputs``. A STEP is gold-skippable at this update iff
    none of the buckets its declared envelope can write is one whose accepted
    answer moved. That is the denominator the ledger reports against: a mechanism
    that skips nothing reads a visible zero."""
    from okg.substrate.consistency.possible_effects import (
        RunAlways,
        compile_possible_effects,
    )

    decisive = {
        split_bucket_label(label) for label in gold_step.expected_diff["changed"]
    }
    out: set[str] = set()
    for step_id, step in cone.steps.items():
        effects = compile_possible_effects(
            step.envelope, checks=checks,
            claim_predicates=step.claim_predicates,
            producer_id=step.resolved_producer_id(),
        )
        if isinstance(effects, RunAlways):
            continue
        if any(
            effects.bucket_reach.touches(predicate, key)
            for predicate, key in decisive
        ):
            continue
        out.add(step_id)
    return frozenset(out)


# --------------------------------------------------------------------------- #
# The certificate licence for a raw-extraction shortfall (§9.7)                #
# --------------------------------------------------------------------------- #
def certificate_licence(
    *,
    cone: CorpusCone,
    skips: Sequence[Any],
    withholding_steps: Sequence[str],
    checks: Sequence[Any] = (),
    record: list[dict[str, Any]] | None = None,
) -> tuple[frozenset[str], tuple[str, ...]]:
    """The buckets this update's VALID certificates cover, and their digests.

    ``CertifiedDifferences.covered_buckets`` is documented as "the bucket
    row-keys the valid certificates' effects reach" — so the licence is read off
    the SKIPPED steps' declared possible-effects bucket reach, never off
    bookkeeping about what went missing. Only the steps that actually WITHHELD
    output are cited: a skip that kept a byte-identical result licenses nothing
    and does not need to.

    A skipped step whose reach is a predicate wildcard or the all-buckets
    sentinel is a LOUD refusal: an unbounded licence would certify divergences
    nothing proved harmless.

    ``record``, when given, is appended one row per SKIP OUTCOME — before the
    withholding filter, so it covers every skip and not only the licensing ones.
    This loop is the last place the pairing exists: the returned digests are a
    sorted, de-duplicated flat tuple and ``UpdateExecution.skip_outcomes`` keeps
    only ``(step_id, cause)``, so which certificate licensed which workflow step
    is destroyed at the boundary. Recording it here changes nothing that is
    returned — both existing outputs keep their exact shape."""
    from okg.substrate.consistency.possible_effects import (
        RunAlways,
        compile_possible_effects,
    )
    from okg.substrate.derived_artifacts.claim_projection_derivation import (
        bucket_row_key,
    )

    withholding = set(withholding_steps)
    covered: set[str] = set()
    digests: list[str] = []
    for outcome in skips:
        if record is not None:
            record.append({
                "workflow_step_id": str(outcome.step_id),
                "cause": str(outcome.cause),
                "certificate_digest": str(outcome.certificate_digest or ""),
                "licensing": outcome.step_id in withholding,
            })
        if outcome.step_id not in withholding:
            continue
        step = cone.steps[outcome.step_id]
        effects = compile_possible_effects(
            step.envelope, checks=checks,
            claim_predicates=step.claim_predicates,
            producer_id=step.resolved_producer_id(),
        )
        if isinstance(effects, RunAlways) or effects.bucket_reach.all_buckets or (
            effects.bucket_reach.predicates
        ):
            raise ParityReplayError(
                f"step {outcome.step_id!r} skipped while withholding output, but "
                "its possible-effects reach is unbounded (wildcard or "
                "all-buckets). A certificate licence built from it would license "
                "divergences nothing proved harmless — refusing."
            )
        covered.update(
            bucket_row_key(predicate, key)
            for predicate, key in effects.bucket_reach.concrete
        )
        if outcome.certificate_digest:
            digests.append(outcome.certificate_digest)
    return frozenset(covered), tuple(sorted(set(digests)))


# --------------------------------------------------------------------------- #
# One update, one mode                                                         #
# --------------------------------------------------------------------------- #
def run_update(
    *,
    dsn: str,
    deployment_dir: Path,
    corpus_dir: Path,
    mode: str,
    update: RecordedUpdate,
    gold_step: Any,
    policies: Sequence[str],
    checks: Sequence[Any],
    state: ModeState,
    deployment: str = SMOKE_DEPLOYMENT,
    mutation: "Mutation | None" = None,
    rungs: Any = None,
) -> UpdateExecution:
    """Execute ONE update in ONE mode: build the cone, run the scheduler pass to
    a fixed point, publish behind the commit gate, and charge the ledger."""
    import psycopg

    from okg.substrate.consistency.reuse_certificates import ReuseCertificateStore
    from okg.substrate.consistency.reuse_scheduler import (
        ACTION_SKIP,
        TERMINAL_FIXED_POINT,
    )
    from okg.substrate.runtime.reuse_pass_workflow import run_reuse_pass_in_cycle

    if mode not in MODES:
        raise ParityReplayError(f"unknown execution mode {mode!r}")

    facts = update.facts()
    digest = consumed_digest(update, facts)
    if digest != update.digest:
        raise ParityReplayError(
            f"update {update.index}: the deserialized fact set does not "
            "round-trip to the recorded digest; the executors would not be "
            "consuming the same bytes"
        )
    cone, stated = build_update_cone(
        corpus_dir=corpus_dir, deployment_dir=deployment_dir, policies=policies,
        checks=checks, update=update, facts=facts, state=state, mode=mode,
        deployment=deployment,
    )
    claims_by_doc, other_facts = partition_recorded_facts(facts, cone.docs)

    # ---- the worklist seed + what the ladder is allowed to consult ---------- #
    if mode == MODE_DEPENDENCY:
        # Dependency pruning: only the documents whose bytes moved are reached.
        # A NEW document has no prior digest, so it is reached too.
        seed = tuple(
            document_step_id(doc) for doc in cone.docs
            if state.prior_doc_digests.get(doc) != cone.doc_digests[doc]
        )
        if not seed:
            raise ParityReplayError(
                f"update {update.index}: dependency pruning reached NO document — "
                "the history declares a real touch at every update, so an empty "
                "pruned seed means the digests were not carried forward"
            )
        prior_certificates: dict[str, Any] = {}
        prior_facts: dict[str, Any] = {}
        dominance: dict[str, Any] = {}
    elif mode == MODE_COMPLETE:
        seed = cone.worklist_seed
        prior_certificates = {}
        prior_facts = {}
        dominance = {}
    else:
        seed = cone.worklist_seed
        prior_facts = dict(state.prior_facts)
        prior_certificates = _load_prior_certificates(
            dsn, deployment=deployment, candidate=state.prior_candidate,
        )
        dominance = _dominance_for(
            dsn, cone=cone, state=state, deployment=deployment,
        )

    changed_terms = dict(cone.changed_terms)
    incumbent = dict(cone.incumbent_selection)
    poisoned: frozenset[str] = frozenset()
    if mutation is not None:
        (
            cone, changed_terms, prior_facts, prior_certificates, dominance,
            poisoned, state,
        ) = mutation.apply(
            dsn=dsn, cone=cone, changed_terms=changed_terms,
            prior_facts=prior_facts, prior_certificates=prior_certificates,
            dominance=dominance, poisoned=poisoned, state=state,
            deployment=deployment,
        )
        if mutation.kind == MUTATION_CORRUPT_CERTIFICATE:
            # Re-read the store AFTER the row was tampered with: the digest check
            # must refuse it here, which is the whole point of the mutation.
            prior_certificates = _load_prior_certificates(
                dsn, deployment=deployment, candidate=state.prior_candidate,
            )

    # ---- the step runner: real work, with carry-forward on a skip ----------- #
    executed: list[str] = []
    materialized: dict[str, list[Any]] = {}
    assembled: list[Any] = []

    def _assemble() -> list[Any]:
        out = list(other_facts)
        for doc in cone.docs:
            if doc in materialized:
                out.extend(materialized[doc])
                continue
            if doc in state.carried:
                out.extend(state.carried[doc])
                continue
            if doc in state.has_prior_result:
                raise ParityReplayError(
                    f"update {update.index}: document step {document_step_id(doc)!r} "
                    "was not run, and the prior result it would carry forward is "
                    "GONE. Reuse is impossible, not silently assumed — refusing "
                    "to publish a state that quietly drops the step's output."
                )
            # Never run in this executor: its prior result is genuinely EMPTY.
        return out

    def _runner(step_id: str) -> Sequence[str]:
        executed.append(step_id)
        if step_id.startswith(f"{CLAIM_SOURCE_STEP}:"):
            doc = step_id[len(CLAIM_SOURCE_STEP) + 1:]
            materialized[doc] = list(claims_by_doc[doc])
            return (CLAIM_PRODUCER_STEP,)
        if step_id == CLAIM_PRODUCER_STEP:
            assembled.clear()
            assembled.extend(_assemble())
            return (CLAIM_EFFECTIVE_STEP,)
        if step_id == CLAIM_EFFECTIVE_STEP:
            return (CLAIM_PROJECTION_STEP,)
        if step_id == CLAIM_PROJECTION_STEP:
            return ()
        raise ParityReplayError(
            f"reached step {step_id!r} has no mapped benchmark work"
        )

    published: dict[str, Any] = {}

    def _checkpoint(_conn: Any) -> str:
        if not assembled:
            # ABSORBED: no document step ran, so the producer was never reached
            # and there is nothing new to publish. The pin the executor already
            # holds IS its answer for this update.
            if not state.generation_id:
                raise ParityReplayError(
                    f"update {update.index}: the pass absorbed the update with no "
                    "prior generation to carry forward — a first update can never "
                    "be absorbed"
                )
            return state.generation_id
        out, ids = publish_fact_list(
            dsn, Path(deployment_dir), list(assembled), state.published_ids
        )
        published["out"] = out
        published["ids"] = ids
        return str(out["generation_id"])

    store = ReuseCertificateStore(deployment=deployment)
    conn = psycopg.connect(dsn)
    try:
        summary = run_reuse_pass_in_cycle(
            conn,
            deployment=deployment,
            generation_id=cone.candidate_generation_id,
            worklist_seed=seed,
            steps=cone.steps,
            checks=checks,
            prior_certificates=prior_certificates or None,
            current_facts=cone.current_facts,
            prior_facts=prior_facts or None,
            prior_generation_id=state.prior_candidate,
            changed_terms=changed_terms,
            incumbent_selection=incumbent,
            dominance_inputs=dominance or None,
            step_runner=_runner,
            cert_store=store,
            poisoned_producers=poisoned,
            rungs=rungs,
            checkpoint_writer=_checkpoint,
        )
    finally:
        conn.close()

    result = summary["result"]
    if summary.get("reuse_pass") != "committed":
        raise ParityReplayError(
            f"update {update.index} [{mode}]: the reuse pass did not commit — "
            f"terminal={summary.get('terminal')!r} "
            f"cause={summary.get('fallback_cause')!r} "
            f"failed_step={summary.get('failed_step')!r} "
            f"detail={dict(result.detail)!r}. No generation was published "
            "(the commit gate is upstream of the publish)."
        )
    if result.terminal != TERMINAL_FIXED_POINT:  # pragma: no cover - defensive
        raise ParityReplayError(f"pass committed off a fixed point: {result.terminal}")

    # ---- what the executor now holds --------------------------------------- #
    did_publish = "out" in published
    if did_publish:
        state.published_facts = list(assembled)
        state.published_ids = published["ids"]
        state.generation_id = str(published["out"]["generation_id"])
        state.compat_generation_id = str(published["out"]["compat_generation_id"])
        for doc, slice_ in materialized.items():
            state.carried[doc] = list(slice_)
            state.has_prior_result.add(doc)
    state.prior_facts = dict(cone.current_facts)
    state.prior_doc_digests = dict(cone.doc_digests)
    state.bindings.append({
        "update": update.index,
        "candidate_generation_id": cone.candidate_generation_id,
        "prior_candidate_generation_id": state.prior_candidate,
        "published_generation_id": (
            state.generation_id if did_publish else None
        ),
        "carried_generation_id": None if did_publish else state.generation_id,
    })
    state.prior_candidate = cone.candidate_generation_id

    # ---- the accepted state + gold agreement ------------------------------- #
    pin = state.generation_id
    detection = detection_state(dsn, pin)
    resolutions = resolutions_at(dsn, pin, detection.claims)
    accepted = accepted_state(resolutions)
    realized = state_diff(state.accepted, accepted)
    state.accepted = dict(accepted)

    snapshot = _snapshot(
        dsn, deployment=deployment, generation_id=state.compat_generation_id,
        chronos_generation_id=pin,
    )

    # ---- the certificate licence for whatever this skip withheld ----------- #
    withholding = [
        document_step_id(doc)
        for doc in cone.docs
        if doc not in materialized
        and _slice_signature(state.carried.get(doc, ()))
        != _slice_signature(claims_by_doc[doc])
    ]
    skips = [o for o in result.outcomes if o.action == ACTION_SKIP]
    skip_records: list[dict[str, Any]] = []
    covered, digests = certificate_licence(
        cone=cone, skips=skips, withholding_steps=withholding, checks=checks,
        record=skip_records,
    )

    gold_skippable = gold_skippable_steps(cone, gold_step, checks=checks)
    ran = {o.step_id for o in result.runs}
    skipped = {o.step_id for o in result.skips}
    invalidated = [
        cls
        for o in result.outcomes
        for cls in o.detail.get("prior_certificate_invalidated", ())
    ]
    ledger = LedgerRow(
        mode=mode,
        update=update.index,
        steps_reached=len(result.outcomes),
        steps_run=len(result.runs),
        skips_by_cause=dict(Counter(o.cause for o in result.skips)),
        run_causes=dict(Counter(o.cause for o in result.runs)),
        certificates_written=int(summary.get("certificates_committed") or 0),
        certificates_invalidated=sum(
            1 for o in result.outcomes
            if o.detail.get("prior_certificate_invalidated")
        ),
        invalidated_classes=tuple(sorted(set(invalidated))),
        terminal=result.terminal,
        fallback_cause=result.fallback_cause,
        published=did_publish,
        generation_id=pin,
        cone_steps=len(cone.steps),
        skips_achievable=len(gold_skippable),
        skips_achieved=len(gold_skippable & skipped),
        unrun_achievable=len(gold_skippable - ran),
    )
    return UpdateExecution(
        mode=mode,
        index=update.index,
        ledger=ledger,
        generation_id=pin,
        compat_generation_id=state.compat_generation_id,
        published=did_publish,
        consumed_digest=digest,
        published_claim_ids=state.published_ids,
        accepted=accepted,
        realized_diff=realized,
        snapshot=snapshot,
        covered_buckets=covered,
        certificate_digests=digests,
        skip_outcomes=tuple(sorted((o.step_id, o.cause) for o in result.skips)),
        run_steps=tuple(executed),
        candidate_generation_id=cone.candidate_generation_id,
        stated_buckets=len(stated),
        withholding_steps=tuple(sorted(withholding)),
        detection=detection,
        resolutions=tuple(resolutions),
        skip_records=tuple(
            sorted(skip_records, key=lambda entry: entry["workflow_step_id"])
        ),
    )


def _slice_signature(facts: Sequence[Any]) -> str:
    """A document slice's identity in the fields that reach the resolved state.

    Deliberately excludes ``source_revision`` (the document sha): a document
    re-rendered with no claim delta carries a new sha but writes the identical
    claims, and counting that as a withheld output would build a certificate
    licence for a skip that withheld nothing."""
    return json.dumps(
        sorted(
            [fact.node_id, fact.subtype, dict(fact.attrs), fact.op]
            for fact in facts
        ),
        sort_keys=True,
    )


def _snapshot(
    dsn: str, *, deployment: str, generation_id: str, chronos_generation_id: str
) -> Any:
    import psycopg

    from okg.substrate.consistency.equivalence_differ import ResolvedStateSnapshot

    with psycopg.connect(dsn, autocommit=True) as conn:
        return ResolvedStateSnapshot.extract(
            conn, deployment=deployment, generation_id=generation_id,
            chronos_generation_id=chronos_generation_id,
        )


def _load_prior_certificates(
    dsn: str, *, deployment: str, candidate: str | None
) -> dict[str, Any]:
    """The prior update's certificates, keyed by the CANDIDATE label (F3).

    A publish mints its generation id at the END of a cycle, so the pass labels
    itself with a deterministic candidate id; certificates therefore live under
    the candidate label and the driver's candidate → published ledger is what
    joins them to a real generation. Reading them back here — and the ladder
    revalidating them — is the witness that the keying works across updates."""
    import psycopg

    from okg.substrate.consistency.reuse_certificates import ReuseCertificateStore

    if not candidate:
        return {}
    store = ReuseCertificateStore(deployment=deployment)
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = store.for_generation(conn, candidate)
    return {cert.step_id: cert for cert in rows}


def _dominance_for(
    dsn: str, *, cone: CorpusCone, state: ModeState, deployment: str
) -> dict[str, Any]:
    if not state.generation_id:
        return {}
    detection = detection_state(dsn, state.generation_id)
    resolutions = resolutions_at(dsn, state.generation_id, detection.claims)
    by_claim = {
        fact.node_id: dict(fact.attrs)
        for fact in state.published_facts
        if getattr(fact, "subtype", "") == _CLAIM_SUBTYPE
    }
    return dominance_inputs_for(
        cone=cone, prior_resolutions=resolutions, prior_facts_by_claim=by_claim,
    )


# --------------------------------------------------------------------------- #
# A whole history, one mode                                                    #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ModeRun:
    """One execution mode replayed over a whole history."""

    mode: str
    executions: tuple[UpdateExecution, ...]
    bindings: tuple[Mapping[str, Any], ...]

    def ledger(self) -> tuple[LedgerRow, ...]:
        return tuple(e.ledger for e in self.executions)

    def totals(self) -> dict[str, Any]:
        rows = self.ledger()
        causes: Counter[str] = Counter()
        for row in rows:
            causes.update(row.skips_by_cause)
        return {
            "mode": self.mode,
            "updates": len(rows),
            "steps_reached": sum(r.steps_reached for r in rows),
            "steps_run": sum(r.steps_run for r in rows),
            "steps_skipped": sum(r.steps_skipped for r in rows),
            "skips_by_cause": dict(sorted(causes.items())),
            "certificates_written": sum(r.certificates_written for r in rows),
            "certificates_invalidated": sum(
                r.certificates_invalidated for r in rows
            ),
            "publishes": sum(1 for r in rows if r.published),
            "skips_achieved": sum(r.skips_achieved for r in rows),
            "skips_achievable": sum(r.skips_achievable for r in rows),
            "unrun_achievable": sum(r.unrun_achievable for r in rows),
        }


def replay_mode(
    *,
    dsn: str,
    deployment_dir: Path,
    corpus_dir: Path,
    parity: ParityHistory,
    mode: str,
    policies: Sequence[str],
    checks: Sequence[Any],
    deployment: str = SMOKE_DEPLOYMENT,
    mutations: Mapping[int, "Mutation"] | None = None,
    on_update: Callable[[UpdateExecution], None] | None = None,
    limit: int | None = None,
    rungs: Any = None,
) -> ModeRun:
    """Replay the whole history in ONE mode against ONE database.

    The corpus is re-materialized per update from the sealed history (the same
    bytes the direct-path replay wrote), so the document digests the certificates
    stand on are real file digests and not a restatement.

    ``limit`` stops after that many updates. A history's state is built strictly
    in order, so a prefix is a real replay of a shorter history — never a jump to
    an update whose predecessors did not run.
    ``rungs`` (concord-reuse-defect-isolation, req.rdi.isolate) selects WHICH
    reuse shortcuts the pass may take. ``None`` is the shipped ladder and leaves
    every existing result byte-identical. Disabling one shortcut at a time over a
    SEALED history is how a divergence between execution modes is attributed to a
    mechanism rather than merely observed: the switch is a first-class recorded
    input to the pass, so an arm is graded on the engine's own cause tally and
    never on the flag that was set.
    """
    corpus_dir = Path(corpus_dir)
    materialize_base(parity.history, corpus_dir)
    state = ModeState()
    executions: list[UpdateExecution] = []
    for update in parity.updates[: limit if limit is not None else len(parity.updates)]:
        materialize_step(parity.history, update.index, corpus_dir)
        execution = run_update(
            dsn=dsn, deployment_dir=Path(deployment_dir), corpus_dir=corpus_dir,
            mode=mode, update=update,
            gold_step=parity.gold_step(update.index),
            policies=policies, checks=checks, state=state,
            deployment=deployment,
            mutation=(mutations or {}).get(update.index),
            rungs=rungs,
        )
        executions.append(execution)
        if on_update is not None:
            on_update(execution)
    return ModeRun(
        mode=mode, executions=tuple(executions),
        bindings=tuple(state.bindings),
    )


# --------------------------------------------------------------------------- #
# Grading                                                                      #
# --------------------------------------------------------------------------- #
VERDICT_EQUAL = "EQUAL"
VERDICT_CERTIFIED = "EQUAL-with-certified-differences"
VERDICT_MISMATCH = "MISMATCH"


@dataclass(frozen=True)
class UpdateVerdict:
    """The three-way agreement for one update: differential, reference, gold."""

    update: int
    verdict: str
    certified_objects: tuple[str, ...]
    covered_buckets: tuple[str, ...]
    certificate_digests: tuple[str, ...]
    gold_problems: tuple[str, ...]
    reference_gold_problems: tuple[str, ...]
    same_facts: bool
    reference_generation: str
    differential_generation: str
    withheld_claims: tuple[str, ...]

    @property
    def agrees(self) -> bool:
        return (
            self.verdict in (VERDICT_EQUAL, VERDICT_CERTIFIED)
            and not self.gold_problems
            and not self.reference_gold_problems
            and self.same_facts
        )


def grade_update(
    *,
    gold_step: Any,
    reference: UpdateExecution,
    differential: UpdateExecution,
) -> UpdateVerdict:
    """Grade ONE update three ways: differential vs reference, and each against
    gold's expected accepted-result diff.

    The differential-vs-reference verdict uses the live proof's own vocabulary
    (:func:`...equivalence_differ.diff_resolved_states`): EQUAL, or EQUAL with a
    distinct certified-differences facet when every divergence is attributable to
    a certificate-covered bucket AND the resolved DECISION is byte-identical.
    A plain-EQUAL verdict is never reported where a certified difference exists —
    the facet is what distinguishes them."""
    from okg.substrate.consistency.equivalence_differ import (
        VERDICT_EQUAL as DIFFER_EQUAL,
        CertifiedDifferences,
        diff_resolved_states,
    )

    licence = (
        CertifiedDifferences(
            covered_buckets=differential.covered_buckets,
            certificate_digests=differential.certificate_digests,
        )
        if differential.covered_buckets else None
    )
    outcome = diff_resolved_states(
        reference.snapshot, differential.snapshot, certified_differences=licence,
    )
    if outcome.verdict != DIFFER_EQUAL:
        verdict = VERDICT_MISMATCH
    elif outcome.is_certified_equal:
        verdict = VERDICT_CERTIFIED
    else:
        verdict = VERDICT_EQUAL

    withheld = tuple(sorted(
        set(reference.published_claim_ids) - set(differential.published_claim_ids)
    ))
    return UpdateVerdict(
        update=differential.index,
        verdict=verdict,
        certified_objects=tuple(sorted({
            str(d.get("object")) for d in outcome.certified_differences
            if d.get("object")
        })),
        covered_buckets=tuple(sorted(differential.covered_buckets)),
        certificate_digests=differential.certificate_digests,
        gold_problems=tuple(compare_step(
            index=differential.index,
            gold_state=gold_step.expected_state,
            gold_diff=gold_step.expected_diff,
            realized_state=differential.accepted,
            realized_diff=differential.realized_diff,
        )),
        reference_gold_problems=tuple(compare_step(
            index=reference.index,
            gold_state=gold_step.expected_state,
            gold_diff=gold_step.expected_diff,
            realized_state=reference.accepted,
            realized_diff=reference.realized_diff,
        )),
        same_facts=reference.consumed_digest == differential.consumed_digest,
        reference_generation=reference.generation_id,
        differential_generation=differential.generation_id,
        withheld_claims=withheld,
    )


def render_verdict_table(verdicts: Sequence[UpdateVerdict]) -> str:
    lines = [
        f"{'upd':>3}  {'verdict':<34} {'facts':<6} {'withheld':>8}  "
        f"{'covered buckets':<16} gold"
    ]
    for verdict in verdicts:
        lines.append(
            f"{verdict.update:>3}  {verdict.verdict:<34} "
            f"{'same' if verdict.same_facts else 'DIFFER':<6} "
            f"{len(verdict.withheld_claims):>8}  "
            f"{len(verdict.covered_buckets):<16} "
            f"{'ok' if not (verdict.gold_problems or verdict.reference_gold_problems) else 'PROBLEM'}"
        )
    return "\n".join(lines)


def render_ledger_table(runs: Sequence[ModeRun]) -> str:
    lines = [
        f"{'mode':<16} {'upd':>4} {'reached':>8} {'run':>6} {'skip':>5} "
        f"{'certs':>6} {'inval':>6} {'pub':>4} {'skip/able':>12} "
        f"{'unrun/able':>12}"
    ]
    for run in runs:
        totals = run.totals()
        lines.append(
            f"{totals['mode']:<16} {totals['updates']:>4} "
            f"{totals['steps_reached']:>8} {totals['steps_run']:>6} "
            f"{totals['steps_skipped']:>5} {totals['certificates_written']:>6} "
            f"{totals['certificates_invalidated']:>6} {totals['publishes']:>4} "
            f"{totals['skips_achieved']:>5}/{totals['skips_achievable']:<6} "
            f"{totals['unrun_achievable']:>5}/{totals['skips_achievable']:<6}"
        )
        for cause, count in sorted(totals["skips_by_cause"].items()):
            lines.append(f"{'':<16}   by cause: {cause:<40} {count:>5}")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# The safety mutations (req.parity.safety)                                     #
# --------------------------------------------------------------------------- #
MUTATION_DEPENDENCY_FACT = "dependency_fact_mutated"
MUTATION_ENVELOPE_VIOLATION = "effects_envelope_violated"
MUTATION_PRIOR_RESULT_DELETED = "prior_result_deleted"
MUTATION_CORRUPT_CERTIFICATE = "certificate_row_corrupted"
MUTATION_VACUOUS_CHANGE = "vacuous_change_statement"

MUTATIONS: tuple[str, ...] = (
    MUTATION_DEPENDENCY_FACT,
    MUTATION_ENVELOPE_VIOLATION,
    MUTATION_PRIOR_RESULT_DELETED,
    MUTATION_CORRUPT_CERTIFICATE,
    MUTATION_VACUOUS_CHANGE,
)


@dataclass
class Mutation:
    """One broken assumption, applied to ONE update's scheduler inputs.

    ``kind`` names which assumption; ``target_step`` is filled in when the
    mutation is applied so the witness can name the step it broke. Mutations
    never touch the scheduler — they move the inputs the scheduler is entitled to
    trust, which is exactly what the safety trial is about."""

    kind: str
    target_step: str = ""
    detail: dict[str, Any] = field(default_factory=dict)

    def apply(
        self,
        *,
        dsn: str,
        cone: CorpusCone,
        changed_terms: dict[str, Any],
        prior_facts: dict[str, Any],
        prior_certificates: dict[str, Any],
        dominance: dict[str, Any],
        poisoned: frozenset[str],
        state: ModeState,
        deployment: str,
    ) -> tuple[
        CorpusCone, dict[str, Any], dict[str, Any], dict[str, Any],
        dict[str, Any], frozenset[str], ModeState,
    ]:
        import dataclasses

        from okg.substrate.consistency.possible_effects import ChangedStateTerms

        victim = self._victim(cone, prior_certificates)
        self.target_step = victim

        if self.kind == MUTATION_DEPENDENCY_FACT:
            # A dependency-fact class moves UNDER a valid certificate: the
            # deployment's constraint set is re-authored. The certificate must
            # invalidate on `constraint_set`; the truthful change statement for a
            # re-authored check is "every bucket the check reads could move", so
            # the ladder must then RUN the step.
            facts = dict(cone.current_facts)
            for step_id, snapshot in facts.items():
                facts[step_id] = dataclasses.replace(
                    snapshot,
                    constraint_set={
                        **dict(snapshot.constraint_set),
                        "mutated_check": "re-authored-by-the-safety-trial",
                    },
                )
            cone = dataclasses.replace(cone, current_facts=facts)
            everything = ChangedStateTerms(buckets=frozenset(cone.buckets))
            changed_terms = {step: everything for step in cone.steps}
            self.detail["mutated_class"] = "constraint_set"

        elif self.kind == MUTATION_ENVELOPE_VIOLATION:
            # The step's DECLARED effects envelope is violated by its actual
            # output: a poisoning row for its producer. The pass must DISCARD
            # into complete execution, never approve a justified skip.
            poisoned = frozenset({cone.steps[victim].resolved_producer_id()})
            self.detail["poisoned_producer"] = cone.steps[
                victim
            ].resolved_producer_id()

        elif self.kind == MUTATION_PRIOR_RESULT_DELETED:
            # The recorded prior RESULT the skip would keep is gone. The
            # certificate is untouched and still validates, so the scheduler
            # still approves the skip — reuse must be refused by the executor
            # that knows the result is missing.
            doc = victim[len(CLAIM_SOURCE_STEP) + 1:]
            self.detail["deleted_doc"] = doc
            self.detail["had_prior_result"] = doc in state.has_prior_result
            state.carried.pop(doc, None)

        elif self.kind == MUTATION_CORRUPT_CERTIFICATE:
            # A stored certificate row is corrupted after write. Reading it back
            # must be REFUSED by the store's digest check.
            self.detail["corrupted_step"] = victim
            _corrupt_certificate_row(
                dsn, deployment=deployment,
                generation_id=str(state.prior_candidate or ""), step_id=victim,
            )

        elif self.kind == MUTATION_VACUOUS_CHANGE:
            # The caller UNDERSTATES the change: it asserts, falsely, that
            # nothing changed. Until PR #972 this mutation was spelled as a bare
            # ChangedStateTerms() and stood for BOTH readings of an empty
            # statement — a false assertion and a plain omission. Those are now
            # different types and the bare constructor is refused, so the
            # mutation is the one that is still expressible: an explicit
            # nothing_changed() the caller is accountable for. The omission
            # reading is no longer a mutation at all, because the ladder refuses
            # it on its own (REFUSE_UNSTATED_CHANGE) with no witness needed.
            changed_terms = {
                step: ChangedStateTerms.nothing_changed() for step in cone.steps
            }
            self.detail["stated_buckets"] = 0

        else:  # pragma: no cover - the taxonomy is closed
            raise ParityReplayError(f"unknown safety mutation {self.kind!r}")

        return (
            cone, changed_terms, prior_facts, prior_certificates, dominance,
            poisoned, state,
        )

    @staticmethod
    def _victim(cone: CorpusCone, prior_certificates: Mapping[str, Any]) -> str:
        """The document step the mutation breaks: one that HAS a prior
        certificate (so "under a valid certificate" is literally true), or the
        first document step when none does."""
        candidates = sorted(
            step for step in prior_certificates
            if step.startswith(f"{CLAIM_SOURCE_STEP}:")
        )
        if candidates:
            return candidates[0]
        return cone.worklist_seed[0]


def _corrupt_certificate_row(
    dsn: str, *, deployment: str, generation_id: str, step_id: str
) -> None:
    """Overwrite a stored certificate's comparison, leaving its digest column.

    The store recomputes the digest on read and REFUSES a row whose content was
    altered after write (§9.5) — this is what makes that refusal falsifiable."""
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE okg.reuse_certificates "
            "   SET comparison = jsonb_set("
            "         comparison, '{incumbent_outcome}', "
            "         '\"TAMPERED-BY-THE-SAFETY-TRIAL\"'::jsonb) "
            " WHERE deployment_name = %s AND generation_id = %s AND step_id = %s",
            (deployment, generation_id, step_id),
        )
        if cur.rowcount != 1:
            raise ParityReplayError(
                f"the corruption mutation matched {cur.rowcount} certificate rows "
                f"for ({deployment}, {generation_id}, {step_id}) — a mutation that "
                "changes nothing proves nothing"
            )


__all__ = [
    "PARITY_SCHEMA",
    "MODES",
    "MODE_COMPLETE",
    "MODE_DEPENDENCY",
    "MODE_DIFFERENTIAL",
    "MUTATIONS",
    "MUTATION_CORRUPT_CERTIFICATE",
    "MUTATION_DEPENDENCY_FACT",
    "MUTATION_ENVELOPE_VIOLATION",
    "MUTATION_PRIOR_RESULT_DELETED",
    "MUTATION_VACUOUS_CHANGE",
    "VERDICT_CERTIFIED",
    "VERDICT_EQUAL",
    "VERDICT_MISMATCH",
    "DERIVATION_STEPS",
    "LedgerRow",
    "ModeRun",
    "ModeState",
    "Mutation",
    "ParityHistory",
    "ParityReplayError",
    "RecordedUpdate",
    "UpdateExecution",
    "UpdateVerdict",
    "bucket_label",
    "build_update_cone",
    "certificate_licence",
    "changed_buckets",
    "claim_signatures",
    "consumed_digest",
    "dominance_inputs_for",
    "gold_skippable_steps",
    "grade_update",
    "load_parity_history",
    "render_ledger_table",
    "render_verdict_table",
    "replay_mode",
    "run_update",
    "split_bucket_label",
]
