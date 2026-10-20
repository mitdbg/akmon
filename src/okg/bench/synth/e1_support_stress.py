"""E1 — read-set completeness stress for the exact reuse engine.

PACT ``concord-disclosure-budget``, task.batch.e1-harness
(req.batch.support-theorem). Method as reviewed in
``designs/batch-scheduling-research.md`` §E1 and design v8
(``concord-invariant-engine/designs/invariant-engine-design.md`` §5): read sets
are DERIVED declarations, textual overlap is not a read set, and the whole
skip chain is subjected to a standing mutation experiment — dependencies are
deleted on purpose and every proposed skip is compared against full processing.

What this module is
-------------------
The E1 stress harness, in two halves that share one grading rule.

* The PURE half (this module's world, checks, and arms): a seeded in-memory
  corpus with hand-authored counterexample checks in the six reviewed classes
  — absence, alias, relationship-closure, global aggregate, policy-pointer,
  dynamic-consumer — plus a plain local check, driven through the REAL exact
  engine (:func:`okg.substrate.consistency.reuse_scheduler.run_reuse_pass`;
  the decision logic is not stubbed or re-implemented). Rung coverage,
  stated precisely (review finding EWD-E1-2): the pure half exercises the
  certificate rungs (valid-prior and degenerate unchanged-inputs — the
  rungs a support-index omission can corrupt), the ``effects_overlap`` run
  path, and, through the fallback probes, ``run_always`` and
  ``unstated_change``; the ``skip_justified`` and bounded-dominance rungs
  are exercised on the live smoke path by the standing parity witnesses,
  not here. Full processing is an independent gold evaluator that never
  consults the engine; every proposed skip is graded against it.
* The SMOKE half (:class:`SupportOmissionMutation`): the same omission model
  applied to the ``synthetic-corpus-smoke`` deployment's parity replay
  (:mod:`okg.bench.synth.parity`) — the declared per-step dependency snapshot
  stops seeing a document's changed input, the ladder approves the skip it is
  entitled to approve, and the three-way parity grade must CATCH the stale
  result as a mismatch. The truthful arm of that replay is the existing
  parity witness; this mutation is E1's induced-omission arm on the live path.

The grading rule (both halves): a proposed skip is UNSAFE iff full processing
of the post-change state yields a result different from the recorded result the
skip kept. Zero tolerance under a declared-complete index: one unsafe skip in a
``complete`` arm refutes the completeness mechanism (the E1 falsifier). Under a
deliberately mutated index, unsafe skips are EXPECTED — and every one must be
caught by the harness, which is what proves the harness can catch them. A
lexical (textual-mention) index that passes the ordinary cases but fails a
counterexample is REJECTED, never accepted (§E1 falsification clause).

Scope: unsafe-skip count, skip precision/recall, fallback rate, and
support-set size are computed here, on the pure half. The remaining two §E1
metrics — runtime saved and checker mutation detection — are WALL-CLOCK and
LIVE-PATH quantities: they are measured on the smoke half by
:mod:`okg.bench.synth.e1_runtime_saved` (task.batch.support-index-theorem)
and attached to an :class:`E1Report` through
:meth:`E1Report.with_smoke_metrics`. A pure arm therefore carries the honest
:func:`pure_arm_runtime_saved_marker` — which names where the measurement IS
taken — never a fabricated pure-world runtime and never a zero.

Preregistration
---------------
:class:`E1Manifest` freezes the zero-tolerance rules, the primary metric, the
seed set, the omission rates, the aggregation rule, and the uncertainty
statement BEFORE any arm runs; every report binds the manifest digest. The
arms are deterministic functions of ``(manifest, seed)``: exact counts, no
sampling — so the uncertainty statement is "deterministic exhaustive counts;
across-seed spread reported as min/max", and thresholds cannot drift after
outcomes are seen.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Mapping, Sequence

from okg.bench.synth.prf import SynthPrf
from okg.substrate.consistency.possible_effects import (
    ChangedStateTerms,
    ChangeStatement,
)
from okg.substrate.consistency.reuse_certificates import (
    CertificateDependencies,
    ReuseCertificate,
)
from okg.substrate.consistency.reuse_scheduler import (
    ACTION_SKIP,
    CAUSE_RUN_ALWAYS,
    CAUSE_UNSTATED_CHANGE,
    TERMINAL_FIXED_POINT,
    ReuseStep,
    run_reuse_pass,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.producer_authority import normalize_write_envelope

__all__ = [
    "CHECK_CLASSES",
    "CLASS_ABSENCE",
    "CLASS_ALIAS",
    "CLASS_AGGREGATE",
    "CLASS_CLOSURE",
    "CLASS_DYNAMIC",
    "CLASS_LOCAL",
    "CLASS_POLICY_POINTER",
    "COUNTEREXAMPLE_CLASSES",
    "DECL_COMPLETE",
    "DECL_LEXICAL",
    "E1_FINDING_PREDICATE",
    "E1ArmResult",
    "E1Manifest",
    "E1Report",
    "E1StressError",
    "MUTATION_DECLARED_SUPPORT_OMITTED",
    "MemoryCertificateStore",
    "OmissionPlan",
    "PREREGISTERED_MANIFEST_DIGEST",
    "RUNTIME_SAVED_MEASURED_BY",
    "RUNTIME_SAVED_NOT_MEASURED_HERE",
    "SupportCheck",
    "SupportOmissionMutation",
    "SupportWorld",
    "WorldState",
    "build_checks",
    "build_manifest",
    "build_world",
    "pure_arm_runtime_saved_marker",
    "run_arm",
    "run_drop_one_sweep",
    "run_report",
    "term_value",
]


class E1StressError(SubstrateError):
    """The E1 harness refused: a mis-built arm proves nothing."""


# --------------------------------------------------------------------------- #
# The preregistered manifest.                                                  #
# --------------------------------------------------------------------------- #
#: The reviewed omission rates (§E1: "seeded omission rates of 1, 5, 10, and
#: 20 percent").
OMISSION_RATES: tuple[float, ...] = (0.01, 0.05, 0.10, 0.20)

#: The default seed set. Three seeds: the pure arms are deterministic and
#: exhaustive per seed, so seeds vary the corpus shape, not sampling noise.
DEFAULT_SEEDS: tuple[int, ...] = (11, 23, 37)

#: The PREREGISTERED manifest digest (review finding EWD-E1-1): the digest
#: of ``build_manifest()`` with its default configuration, pinned here so an
#: outcome-driven edit to seeds, rates, rules, or thresholds cannot pass
#: silently — changing the configuration means changing this constant in the
#: same reviewable diff. The witness asserts the equality.
PREREGISTERED_MANIFEST_DIGEST = (
    "253d51af40f5268cc57250ded34403c189a9bb6796f8607462a54401ea16bace"
)

#: The status a wall-clock-free arm reports for runtime saved. It is neither
#: "deferred" (the metric IS delivered — on the smoke half) nor a zero (a zero
#: would claim the engine saved nothing). It names where the measurement is
#: taken. The vocabulary lives here rather than in
#: :mod:`okg.bench.synth.e1_runtime_saved` so this harness can state its own
#: honest marker without importing the live-path machinery.
RUNTIME_SAVED_NOT_MEASURED_HERE = "not_measured_in_this_arm"

#: Where a wall-clock-free arm points a reader.
RUNTIME_SAVED_MEASURED_BY = (
    "okg.bench.synth.e1_runtime_saved.measure_runtime_saved, over a "
    "MODE_COMPLETE and a MODE_DIFFERENTIAL measure_replay of the same "
    "sealed synthetic-corpus-smoke history"
)


def pure_arm_runtime_saved_marker() -> dict[str, str]:
    """The honest runtime-saved marker for an arm with no wall clock.

    The E1 pure world is an in-memory deterministic model: a "runtime"
    measured over it would describe this Python process, not the engine, and
    would be quoted as engine evidence the moment it entered a report. So the
    arm states the absence positively and names the measurement's real
    home."""
    return {
        "measured_by": RUNTIME_SAVED_MEASURED_BY,
        "reason": (
            "the E1 pure world has no wall clock and no database, so a "
            "runtime measured here would describe this process rather than "
            "the engine; runtime saved is measured on the smoke half"
        ),
        "status": RUNTIME_SAVED_NOT_MEASURED_HERE,
    }


@dataclass(frozen=True)
class E1Manifest:
    """The preregistered E1 run manifest — frozen before outcomes exist.

    Built by :func:`build_manifest` from configuration only. Its
    :meth:`digest` binds every report row; a report whose manifest digest
    differs from the preregistered one is a different experiment, not a rerun.
    """

    seeds: tuple[int, ...]
    omission_rates: tuple[float, ...]
    updates_per_seed: int
    zero_tolerance: Mapping[str, str]
    primary_metric: str
    comparator: str
    aggregation: str
    uncertainty: str
    threshold: str
    #: Metrics §E1 names that this experiment does NOT yet deliver, with the
    #: task that owns each. Empty since task.batch.support-index-theorem
    #: landed: every §E1 metric now has a delivered definition.
    deferred_metrics: Mapping[str, str]
    #: Metrics §E1 names that ARE delivered, each with the definition its
    #: number is computed under. A delivered metric moves out of
    #: ``deferred_metrics`` and into here in the same reviewable diff that
    #: re-pins :data:`PREREGISTERED_MANIFEST_DIGEST`, so a metric can never
    #: quietly change from "not measured" to "measured as whatever came out".
    delivered_metrics: Mapping[str, str]
    data_binding: Mapping[str, str]
    schema: str = "okg.synth.e1-support-stress.v2"

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "seeds": list(self.seeds),
            "omission_rates": list(self.omission_rates),
            "updates_per_seed": self.updates_per_seed,
            "zero_tolerance": dict(sorted(self.zero_tolerance.items())),
            "primary_metric": self.primary_metric,
            "comparator": self.comparator,
            "aggregation": self.aggregation,
            "uncertainty": self.uncertainty,
            "threshold": self.threshold,
            "deferred_metrics": dict(sorted(self.deferred_metrics.items())),
            "delivered_metrics": dict(sorted(self.delivered_metrics.items())),
            "data_binding": dict(sorted(self.data_binding.items())),
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_canonical_dict(), sort_keys=True, separators=(",", ":")
        )

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


def build_manifest(
    *,
    seeds: Sequence[int] = DEFAULT_SEEDS,
    omission_rates: Sequence[float] = OMISSION_RATES,
    updates_per_seed: int = 12,
) -> E1Manifest:
    """The one manifest constructor. Configuration in, preregistration out.

    Deliberately takes NO result object of any kind, so a manifest cannot be
    shaped by outcomes; the digest depends on configuration alone."""
    if updates_per_seed < len(INJECTION_OPS) + 1:
        raise E1StressError(
            "E1 needs the counterexample-injection block plus at least one "
            "seeded update per trajectory",
            data={"updates_per_seed": updates_per_seed,
                  "injection": len(INJECTION_OPS)},
        )
    return E1Manifest(
        seeds=tuple(int(s) for s in seeds),
        omission_rates=tuple(float(r) for r in omission_rates),
        updates_per_seed=int(updates_per_seed),
        zero_tolerance={
            "unsafe_skip_under_complete_index": (
                "one proposed skip whose full-processing result differs from "
                "the kept result, in any declared-complete arm, refutes the "
                "completeness mechanism (E1 falsifier)"
            ),
            "counterexample_not_caught_under_lexical_index": (
                "a lexical index passing ordinary cases but producing no "
                "caught unsafe skip for one of the six counterexample classes "
                "means the class was not exercised; the arm is invalid, and a "
                "lexical index is never accepted"
            ),
            "undetected_unsafe_outcome": (
                "grading is exhaustive against the independent gold evaluator "
                "for every step at every update; an unsafe skip that grading "
                "cannot see is structurally impossible in the pure half and a "
                "mismatch-graded defect in the smoke half"
            ),
        },
        primary_metric="unsafe_skip_count",
        comparator="declared-complete arm on the same (seed, updates)",
        aggregation=(
            "per-arm exact sums over all updates and steps of one seed; "
            "across seeds reported per seed plus min/max spread"
        ),
        uncertainty=(
            "deterministic exhaustive counts — no sampling; across-seed "
            "spread is reported as min/max, never as a confidence interval"
        ),
        threshold="unsafe_skip_count == 0 in every declared-complete arm",
        deferred_metrics={},
        delivered_metrics={
            "runtime_saved": (
                "complete-mode realized MINUS differential-mode realized, "
                "over two measure_replay runs of the SAME sealed "
                "synthetic-corpus-smoke history, in the measurement "
                "adapter's declared units (nanoseconds for duration, "
                "executor_steps for cost); the setup-charged cycle-0 "
                "interval is EXCLUDED from both sums and reported "
                "separately per arm; any UNKNOWN component makes the "
                "difference UNKNOWN with a reason and the metric "
                "partially_known — never zero-filled. Pure arms carry a "
                "not_measured_in_this_arm marker naming the smoke "
                "measurement; they never report a pure-world runtime"
            ),
            "checker_mutation_detection": (
                "on the same live replay: the certificate-row corruption "
                "mutation (caught iff the certificate store's own "
                "recomputed-digest refusal reaches the caller) and a "
                "dependency-fact mutation (caught iff the certificate "
                "invalidates on the moved dependency class AND the victim "
                "step re-runs); a mutation that did not demonstrably change "
                "anything is refused rather than graded, and a not-caught "
                "row is reported as a finding, never dropped"
            ),
        },
        data_binding={
            "pure_world": (
                "the mutation grid (counterexample classes, drop-one, "
                "omission rates, fallback probes) runs on the seeded "
                "in-memory world, driving the real exact engine"
            ),
            "smoke_deployment": (
                "the synthetic-corpus-smoke live path carries the core "
                "soundness comparison: the truthful parity prefix and ONE "
                "induced support-omission per run (SupportOmissionMutation); "
                "it also carries the two wall-clock/live-path metrics — "
                "runtime saved (a MODE_COMPLETE and a MODE_DIFFERENTIAL "
                "measure_replay of one sealed history) and checker mutation "
                "detection (certificate-row corruption plus a "
                "dependency-fact mutation). The full omission-rate and "
                "drop-one GRID at smoke scale remains out of scope and is "
                "not claimed by any metric here: the grid runs on the pure "
                "world only"
            ),
            "rung_coverage": (
                "the pure half exercises the certificate rungs "
                "(certificate_valid / certificate_degenerate_unchanged), "
                "the effects_overlap run path, and — via the fallback "
                "probes — run_always and unstated_change; the "
                "skip_justified and bounded-dominance rungs are exercised "
                "on the live smoke path by the standing parity witnesses, "
                "not here"
            ),
        },
    )


# --------------------------------------------------------------------------- #
# The world: a small typed corpus with a term vocabulary.                      #
# --------------------------------------------------------------------------- #
#: The term vocabulary. A TERM is one addressable unit of check-visible state;
#: support sets, change sets, and certificate ``input_digests`` are all keyed
#: by terms, so an omission is one dropped key — never a fuzzy notion.
TERM_ABSENT = "∅"  # the recorded value of a term that addresses nothing


def _entity_term(entity_id: str, attr: str) -> str:
    return f"entity:{entity_id}:{attr}"


def _alias_term(alias_id: str) -> str:
    return f"alias:{alias_id}"


def _rel_term(src: str) -> str:
    return f"rel:{src}"


def _members_term(kind: str) -> str:
    return f"members:{kind}"


def _policy_term(policy_id: str) -> str:
    return f"policy:{policy_id}"


@dataclass(frozen=True)
class WorldState:
    """One immutable snapshot of the pure corpus.

    ``entities`` maps entity id -> attributes (every entity carries ``kind``);
    ``aliases`` maps alias id -> target entity id; ``relations`` maps a source
    entity to its sorted out-neighbours; ``policies`` maps policy id -> a
    JSON-shaped body (the policy-pointer and dynamic-consumer checks read
    through it)."""

    entities: Mapping[str, Mapping[str, str]]
    aliases: Mapping[str, str]
    relations: Mapping[str, tuple[str, ...]]
    policies: Mapping[str, Mapping[str, str]]

    def members(self, kind: str) -> tuple[str, ...]:
        return tuple(sorted(
            e for e, attrs in self.entities.items() if attrs.get("kind") == kind
        ))


def term_value(state: WorldState, term: str) -> str:
    """The canonical recorded value of one term in one state.

    Total: a term addressing nothing yields :data:`TERM_ABSENT` (absence is a
    value — a term that appears must CHANGE the value, which is what makes
    membership terms sound for absence-style checks, design v8 §4(b))."""
    kind, _, rest = term.partition(":")
    if kind == "entity":
        entity_id, _, attr = rest.partition(":")
        attrs = state.entities.get(entity_id)
        if attrs is None or attr not in attrs:
            return TERM_ABSENT
        return str(attrs[attr])
    if kind == "alias":
        return state.aliases.get(rest, TERM_ABSENT)
    if kind == "rel":
        return json.dumps(list(state.relations.get(rest, ())))
    if kind == "members":
        return json.dumps(list(state.members(rest)))
    if kind == "policy":
        body = state.policies.get(rest)
        if body is None:
            return TERM_ABSENT
        return json.dumps(dict(sorted(body.items())))
    raise E1StressError(
        "unknown term kind — the term vocabulary is closed",
        data={"term": term},
    )


def _term_digest(state: WorldState, term: str) -> str:
    return hashlib.sha256(
        f"{term}\x1f{term_value(state, term)}".encode("utf-8")
    ).hexdigest()


def changed_terms_between(
    pre: WorldState, post: WorldState, terms: Sequence[str]
) -> frozenset[str]:
    """The terms (from ``terms``) whose recorded value differs pre -> post."""
    return frozenset(
        t for t in terms if term_value(pre, t) != term_value(post, t)
    )


# --------------------------------------------------------------------------- #
# The checks: six reviewed counterexample classes plus a plain local one.      #
# --------------------------------------------------------------------------- #
CLASS_LOCAL = "local"
CLASS_ABSENCE = "absence"
CLASS_ALIAS = "alias"
CLASS_CLOSURE = "relationship_closure"
CLASS_AGGREGATE = "global_aggregate"
CLASS_POLICY_POINTER = "policy_pointer"
CLASS_DYNAMIC = "dynamic_consumer"

#: The six classes the review named as counterexamples to a lexical index.
COUNTEREXAMPLE_CLASSES: tuple[str, ...] = (
    CLASS_ABSENCE,
    CLASS_ALIAS,
    CLASS_CLOSURE,
    CLASS_AGGREGATE,
    CLASS_POLICY_POINTER,
    CLASS_DYNAMIC,
)
CHECK_CLASSES: tuple[str, ...] = (CLASS_LOCAL, *COUNTEREXAMPLE_CLASSES)

#: The predicate every check-step's finding bucket lives under; the step's
#: declared envelope reaches exactly its own ``(predicate, check_id)`` bucket,
#: which is what lets the effect-bound rung reason about "this check's inputs
#: moved" through the engine's real bucket vocabulary.
E1_FINDING_PREDICATE = "e1_finding"


@dataclass(frozen=True)
class SupportCheck:
    """One check: its class, its gold semantics, and its lexical strawman.

    ``evaluate`` is FULL PROCESSING for this check — an independent function of
    the world state that never consults the engine, a declaration, or a prior
    result. ``gold_support`` is the complete semantic read set of ``evaluate``
    on the given state (state-dependent for the dynamic-consumer class).
    ``lexical_support`` is the textual-mention strawman the review rejected:
    the terms a mention index would find in the check's text. Both support
    functions return TERM sets; the harness turns them into certificate
    ``input_digests``."""

    check_id: str
    check_class: str
    evaluate: Callable[[WorldState], str]
    gold_support: Callable[[WorldState], frozenset[str]]
    lexical_support: Callable[[WorldState], frozenset[str]]

    def __post_init__(self) -> None:
        if self.check_class not in CHECK_CLASSES:
            raise E1StressError(
                "unknown check class — the counterexample taxonomy is closed",
                data={"check_id": self.check_id, "got": self.check_class,
                      "known": list(CHECK_CLASSES)},
            )


@dataclass(frozen=True)
class SupportWorld:
    """The seeded pure corpus: an initial state and its update trajectory.

    ``states[0]`` is the initial state; ``states[i+1]`` is the state after
    update ``i``. ``operations[i]`` names what update ``i`` did (the report
    keys per-operation findings off it). Deterministic from the seed."""

    seed: int
    states: tuple[WorldState, ...]
    operations: tuple[str, ...]


# -- world construction ------------------------------------------------------ #
_OP_KINDS: tuple[str, ...] = (
    "bump_local_val",          # ordinary attribute change
    "flip_absence_flag",       # a doc's flag flips (absence check input)
    "insert_flagged_doc",      # NEW entity appears — the absence counterexample
    "delete_flagged_doc",      # entity DISAPPEARS — absence via deletion
    "repoint_alias",           # alias target moves
    "repoint_ptr_policy",      # the POINTER itself moves (policy indirection)
    "bump_alias_target_val",   # the entity BEHIND the alias changes
    "add_second_hop_edge",     # remote edge appears — the closure counterexample
    "remove_second_hop_edge",  # remote edge DISAPPEARS — neighbour-closure loss
    "bump_second_hop_val",     # a 2-hop neighbour's value changes
    "insert_person",           # membership change — the aggregate counterexample
    "bump_pointed_threshold",  # the fact a policy POINTS TO changes
    "repoint_audit_policy",    # the dynamic consumer's subject moves
    "bump_new_audit_target",   # the NEW subject changes after the repoint
    "duplicate_doc_entity",    # same content, new id — the duplication probe
)

#: The guaranteed counterexample-injection suffix (§E1: the counterexamples
#: are INJECTED, deliberately — never left to a shuffle). Every trajectory
#: ends with this block in this order, so each of the six classes is exercised
#: on every seed: a new flagged doc (absence), the value behind the alias
#: (alias), a two-hop neighbour (closure), a new member (aggregate), the
#: pointed-to fact (policy-pointer), a repoint followed by a change to the
#: NEW subject (dynamic-consumer), and an entity duplication.
INJECTION_OPS: tuple[str, ...] = (
    "insert_flagged_doc",
    "delete_flagged_doc",       # absence via DELETION, not only growth
    "bump_alias_target_val",
    "bump_second_hop_val",
    "remove_second_hop_edge",   # the §E1 neighbour-closure LOSS case
    "insert_person",
    "bump_pointed_threshold",
    "repoint_audit_policy",
    "bump_new_audit_target",
    "duplicate_doc_entity",
)


def build_world(seed: int, *, updates: int) -> SupportWorld:
    """The deterministic ``seed -> world + update trajectory`` map.

    The trajectory is a seeded operation prefix (variety across seeds)
    followed by the fixed :data:`INJECTION_OPS` counterexample block, so every
    counterexample class is exercised on every seed. ``updates`` must leave
    room for the injection block. Entity/alias/edge shapes vary with the seed
    so the arms have a spread rather than one draw."""
    if updates < len(INJECTION_OPS) + 1:
        raise E1StressError(
            "the trajectory must fit the counterexample-injection block plus "
            "at least one seeded update",
            data={"updates": updates, "injection": len(INJECTION_OPS)},
        )
    prf = SynthPrf(f"e1-support-stress:{seed}")
    n_docs = 3 + prf.randint(0, 2, "n_docs")
    n_people = 4 + prf.randint(0, 2, "n_people")

    entities: dict[str, dict[str, str]] = {}
    for i in range(n_docs):
        entities[f"doc{i}"] = {
            "kind": "doc", "flag": "0", "val": str(10 + i),
        }
    for i in range(n_people):
        entities[f"p{i}"] = {"kind": "person", "val": str(i + 1)}

    aliases = {"handbook": "doc0", "lead": "p0"}
    # a chain p0 -> p1 -> p2 (2-hop closure reads p1 and p2 through p0)
    relations: dict[str, tuple[str, ...]] = {"p0": ("p1",), "p1": ("p2",)}
    policies = {
        "ptr": {"target_term": _entity_term("doc1", "val")},
        "audit": {"target": "p1", "threshold": "3"},
    }
    state = WorldState(
        entities={k: dict(v) for k, v in entities.items()},
        aliases=dict(aliases),
        relations=dict(relations),
        policies={k: dict(v) for k, v in policies.items()},
    )

    prefix_len = updates - len(INJECTION_OPS)
    ops = [
        prf.choice(_OP_KINDS, "op_prefix", str(i)) for i in range(prefix_len)
    ]
    ops.extend(INJECTION_OPS)

    states = [state]
    for index, op in enumerate(ops):
        states.append(_apply_operation(states[-1], op, prf, index))
    return SupportWorld(seed=seed, states=tuple(states), operations=tuple(ops))


def _apply_operation(
    state: WorldState, op: str, prf: SynthPrf, index: int
) -> WorldState:
    entities = {k: dict(v) for k, v in state.entities.items()}
    aliases = dict(state.aliases)
    relations = {k: tuple(v) for k, v in state.relations.items()}
    policies = {k: dict(v) for k, v in state.policies.items()}
    tag = str(index)

    if op == "bump_local_val":
        victim = prf.choice(sorted(entities), "bump_local", tag)
        entities[victim]["val"] = str(int(entities[victim].get("val", "0")) + 1)
    elif op == "flip_absence_flag":
        docs = [e for e, a in entities.items() if a.get("kind") == "doc"]
        victim = prf.choice(sorted(docs), "flip_flag", tag)
        entities[victim]["flag"] = "1" if entities[victim]["flag"] == "0" else "0"
    elif op == "insert_flagged_doc":
        new_id = f"doc-new-{index}"
        entities[new_id] = {"kind": "doc", "flag": "1", "val": "99"}
    elif op == "delete_flagged_doc":
        # absence via DELETION: a flagged doc disappears, so the absence
        # check's result shrinks though no surviving fact changed. Prefer an
        # inserted doc (guaranteed flagged), else any flagged doc, else any
        # non-anchor doc — deletion must actually happen.
        docs = sorted(e for e, a in entities.items() if a.get("kind") == "doc")
        flagged = [d for d in docs if entities[d].get("flag") == "1"]
        preferred = [d for d in flagged if d.startswith("doc-new-")]
        candidates = preferred or flagged or [d for d in docs if d != "doc0"]
        if candidates:
            victim = prf.choice(candidates, "delete_doc", tag)
            del entities[victim]
    elif op == "repoint_alias":
        docs = sorted(e for e, a in entities.items() if a.get("kind") == "doc")
        current = aliases["handbook"]
        candidates = [d for d in docs if d != current] or docs
        aliases["handbook"] = prf.choice(candidates, "repoint_alias", tag)
    elif op == "bump_alias_target_val":
        target = aliases["handbook"]
        entities[target]["val"] = str(int(entities[target].get("val", "0")) + 1)
    elif op == "repoint_ptr_policy":
        # the POINTER moves: the policy body changes (caught by any index
        # watching the policy term); the interesting failure is the NEXT
        # pointee change, which only the re-derived pointee term catches.
        docs = sorted(e for e, a in entities.items() if a.get("kind") == "doc")
        current = policies["ptr"]["target_term"]
        candidates = [
            _entity_term(d, "val") for d in docs
            if _entity_term(d, "val") != current
        ]
        if candidates:
            policies["ptr"]["target_term"] = prf.choice(
                candidates, "repoint_ptr", tag
            )
    elif op == "remove_second_hop_edge":
        # the §E1 neighbour-closure LOSS: a remote edge disappears, so the
        # two-hop neighbourhood shrinks though the surviving facts are
        # untouched. When no second-hop edge survives to remove, one is
        # ADDED instead (a closure change either way — never a silent no-op).
        first_hops = list(relations.get("p0", ()))
        removable = [
            (hop, dst) for hop in first_hops
            for dst in relations.get(hop, ())
        ]
        if removable:
            hop, dst = prf.choice(sorted(removable), "remove_second", tag)
            relations[hop] = tuple(d for d in relations[hop] if d != dst)
        elif first_hops:
            hop = prf.choice(sorted(first_hops), "readd_second_src", tag)
            people = sorted(
                e for e, a in entities.items() if a.get("kind") == "person"
            )
            candidates = [p for p in people if p != hop]
            if candidates:
                relations[hop] = tuple(sorted({
                    *relations.get(hop, ()),
                    prf.choice(candidates, "readd_second_dst", tag),
                }))
    elif op == "add_second_hop_edge":
        people = sorted(e for e, a in entities.items() if a.get("kind") == "person")
        first_hops = relations.get("p0", ())
        if first_hops:
            hop = prf.choice(list(first_hops), "second_hop_src", tag)
            existing = set(relations.get(hop, ()))
            candidates = [p for p in people if p not in existing and p != hop]
            if candidates:
                new_dst = prf.choice(candidates, "second_hop_dst", tag)
                relations[hop] = tuple(sorted({*existing, new_dst}))
    elif op == "bump_second_hop_val":
        second = set()
        for hop in relations.get("p0", ()):
            second.update(relations.get(hop, ()))
        if second:
            victim = prf.choice(sorted(second), "bump_second", tag)
            entities[victim]["val"] = str(int(entities[victim].get("val", "0")) + 1)
    elif op == "insert_person":
        new_id = f"p-new-{index}"
        entities[new_id] = {"kind": "person", "val": str(7 + index)}
    elif op == "bump_pointed_threshold":
        target_term = policies["ptr"]["target_term"]
        _, _, rest = target_term.partition(":")
        entity_id, _, attr = rest.partition(":")
        if entity_id in entities:
            entities[entity_id][attr] = str(
                int(entities[entity_id].get(attr, "0")) + 1
            )
    elif op == "repoint_audit_policy":
        people = sorted(e for e, a in entities.items() if a.get("kind") == "person")
        current = policies["audit"]["target"]
        # prefer a subject the initial (lexically indexed) target is not, so
        # the follow-up change to the NEW subject is the genuine
        # dynamic-consumer counterexample rather than a lexically watched one.
        candidates = (
            [p for p in people if p not in (current, "p1")]
            or [p for p in people if p != current]
            or people
        )
        policies["audit"]["target"] = prf.choice(candidates, "repoint_audit", tag)
    elif op == "bump_new_audit_target":
        target = policies["audit"]["target"]
        if target in entities:
            entities[target]["val"] = str(int(entities[target].get("val", "0")) + 1)
    elif op == "duplicate_doc_entity":
        docs = sorted(e for e, a in entities.items() if a.get("kind") == "doc")
        victim = prf.choice(docs, "duplicate", tag)
        entities[f"{victim}-dup-{index}"] = dict(entities[victim])
    else:  # pragma: no cover - the taxonomy is closed
        raise E1StressError("unknown world operation", data={"op": op})

    return WorldState(
        entities=entities, aliases=aliases, relations=relations,
        policies=policies,
    )


# -- check construction ------------------------------------------------------ #
def build_checks(initial: WorldState) -> tuple[SupportCheck, ...]:
    """The seven checks over one world. ``initial`` pins the LEXICAL index:
    the strawman is built once, from the entities the check text mentions at
    index time, and never refreshed — which is exactly the failure mode the
    counterexamples exist to expose (design v8 §5: textual overlap is not a
    read set)."""

    # 1. local: reads one entity attribute; the ordinary case a lexical index
    #    gets right (its text literally names the entity and attribute).
    local_entity = "doc0"

    def local_eval(state: WorldState) -> str:
        return term_value(state, _entity_term(local_entity, "val"))

    def local_support(state: WorldState) -> frozenset[str]:
        return frozenset({_entity_term(local_entity, "val")})

    # 2. absence: "no flagged doc exists". Gold support carries the doc
    #    membership term (a NEW doc changes it though it appears in no prior
    #    read). The lexical strawman lists only the flags of the docs that
    #    existed at index time.
    def absence_eval(state: WorldState) -> str:
        flagged = sorted(
            d for d in state.members("doc")
            if state.entities[d].get("flag") == "1"
        )
        return json.dumps(flagged)

    def absence_gold(state: WorldState) -> frozenset[str]:
        return frozenset(
            {_members_term("doc")}
            | {_entity_term(d, "flag") for d in state.members("doc")}
        )

    _absence_lexical = frozenset(
        _entity_term(d, "flag") for d in initial.members("doc")
    )

    # 3. alias: reads the value BEHIND an alias. Gold support carries the alias
    #    term and the current target's value term; the lexical strawman
    #    mentions only the alias name.
    def alias_eval(state: WorldState) -> str:
        target = state.aliases.get("handbook", "")
        return term_value(state, _entity_term(target, "val"))

    def alias_gold(state: WorldState) -> frozenset[str]:
        target = state.aliases.get("handbook", "")
        return frozenset(
            {_alias_term("handbook"), _entity_term(target, "val")}
        )

    _alias_lexical = frozenset({_alias_term("handbook")})

    # 4. relationship closure: reads values two hops out from p0. Gold support
    #    carries BOTH hop adjacency terms and every reached value; the lexical
    #    strawman stops after the first hop.
    def closure_neighbourhood(state: WorldState) -> tuple[frozenset[str], frozenset[str]]:
        first = frozenset(state.relations.get("p0", ()))
        second = frozenset(
            dst for hop in first for dst in state.relations.get(hop, ())
        )
        return first, second

    def closure_eval(state: WorldState) -> str:
        first, second = closure_neighbourhood(state)
        # member-id:value pairs, so a MEMBERSHIP change (an edge removed —
        # neighbour-closure loss) changes the result even when the surviving
        # values happen to coincide.
        return json.dumps(sorted(
            f"{m}:{term_value(state, _entity_term(m, 'val'))}"
            for m in first | second
        ))

    def closure_gold(state: WorldState) -> frozenset[str]:
        first, second = closure_neighbourhood(state)
        return frozenset(
            {_rel_term("p0")}
            | {_rel_term(hop) for hop in first}
            | {_entity_term(m, "val") for m in first | second}
        )

    _closure_first_initial = frozenset(initial.relations.get("p0", ()))
    _closure_lexical = frozenset(
        {_rel_term("p0")}
        | {_entity_term(m, "val") for m in _closure_first_initial}
    )

    # 5. global aggregate: sum of person values. Gold support carries the
    #    person membership term and every member's value; the lexical strawman
    #    names the single person its text mentions.
    def aggregate_eval(state: WorldState) -> str:
        return str(sum(
            int(state.entities[p].get("val", "0"))
            for p in state.members("person")
        ))

    def aggregate_gold(state: WorldState) -> frozenset[str]:
        return frozenset(
            {_members_term("person")}
            | {_entity_term(p, "val") for p in state.members("person")}
        )

    _aggregate_lexical = frozenset({_entity_term("p0", "val")})

    # 6. policy pointer: reads whatever fact the policy names. Gold support
    #    carries the policy body and the pointed-to term; the lexical strawman
    #    sees only the policy (its text never names the pointee).
    def pointer_eval(state: WorldState) -> str:
        body = state.policies.get("ptr", {})
        target_term = body.get("target_term", "")
        pointed = term_value(state, target_term) if target_term else TERM_ABSENT
        return json.dumps({"target": target_term, "value": pointed})

    def pointer_gold(state: WorldState) -> frozenset[str]:
        body = state.policies.get("ptr", {})
        out = {_policy_term("ptr")}
        if body.get("target_term"):
            out.add(str(body["target_term"]))
        return frozenset(out)

    _pointer_lexical = frozenset({_policy_term("ptr")})

    # 7. dynamic consumer: the audited subject is chosen BY DATA. Gold support
    #    is re-derived per state (the current subject's value); the lexical
    #    strawman was built once against the initial subject and never
    #    refreshed — after a repoint it still watches the old subject.
    def dynamic_eval(state: WorldState) -> str:
        body = state.policies.get("audit", {})
        target = body.get("target", "")
        value = term_value(state, _entity_term(target, "val"))
        threshold = body.get("threshold", "0")
        breach = (
            value != TERM_ABSENT and int(value) > int(threshold)
        )
        # the audited VALUE is part of the finding, so any change to the
        # current subject's value changes the result — the dynamic-consumer
        # counterexample is decidable, not threshold-lucky.
        return json.dumps({"target": target, "value": value, "breach": breach})

    def dynamic_gold(state: WorldState) -> frozenset[str]:
        body = state.policies.get("audit", {})
        out = {_policy_term("audit")}
        target = body.get("target", "")
        if target:
            out.add(_entity_term(target, "val"))
        return frozenset(out)

    _dynamic_initial_target = initial.policies["audit"]["target"]
    _dynamic_lexical = frozenset({
        _policy_term("audit"),
        _entity_term(_dynamic_initial_target, "val"),
    })

    def _static(terms: frozenset[str]) -> Callable[[WorldState], frozenset[str]]:
        return lambda _state, _t=terms: _t

    return (
        SupportCheck(
            check_id="check.local.doc0-val", check_class=CLASS_LOCAL,
            evaluate=local_eval, gold_support=local_support,
            lexical_support=local_support,
        ),
        SupportCheck(
            check_id="check.absence.no-flagged-doc", check_class=CLASS_ABSENCE,
            evaluate=absence_eval, gold_support=absence_gold,
            lexical_support=_static(_absence_lexical),
        ),
        SupportCheck(
            check_id="check.alias.handbook-val", check_class=CLASS_ALIAS,
            evaluate=alias_eval, gold_support=alias_gold,
            lexical_support=_static(_alias_lexical),
        ),
        SupportCheck(
            check_id="check.closure.p0-two-hop", check_class=CLASS_CLOSURE,
            evaluate=closure_eval, gold_support=closure_gold,
            lexical_support=_static(_closure_lexical),
        ),
        SupportCheck(
            check_id="check.aggregate.person-sum", check_class=CLASS_AGGREGATE,
            evaluate=aggregate_eval, gold_support=aggregate_gold,
            lexical_support=_static(_aggregate_lexical),
        ),
        SupportCheck(
            check_id="check.pointer.ptr-policy", check_class=CLASS_POLICY_POINTER,
            evaluate=pointer_eval, gold_support=pointer_gold,
            lexical_support=_static(_pointer_lexical),
        ),
        SupportCheck(
            check_id="check.dynamic.audit-policy", check_class=CLASS_DYNAMIC,
            evaluate=dynamic_eval, gold_support=dynamic_gold,
            lexical_support=_static(_dynamic_lexical),
        ),
    )


# --------------------------------------------------------------------------- #
# Declarations: how a support index becomes engine inputs.                     #
# --------------------------------------------------------------------------- #
DECL_COMPLETE = "complete"   # gold support, re-derived per pass
DECL_LEXICAL = "lexical"     # the rejected textual-mention strawman, static


@dataclass(frozen=True)
class OmissionPlan:
    """Which declared dependencies an arm drops, and how.

    ``drop_exact`` names ``(check_id, term)`` pairs to drop wherever they
    appear. ``drop_rate`` drops each declared ``(check_id, term)`` pair with
    the given seeded probability (the §E1 omission-rate arms). Both compose;
    an empty plan drops nothing."""

    drop_exact: frozenset[tuple[str, str]] = frozenset()
    drop_rate: float = 0.0
    rate_seed: int = 0

    def apply(
        self, check_id: str, terms: frozenset[str]
    ) -> frozenset[str]:
        kept = set(terms)
        for cid, term in self.drop_exact:
            if cid == check_id:
                kept.discard(term)
        if self.drop_rate > 0.0:
            prf = SynthPrf(f"e1-omission:{self.rate_seed}")
            kept = {
                t for t in kept
                if prf.uniform("drop", check_id, t) >= self.drop_rate
            }
        return frozenset(kept)


def declared_support(
    check: SupportCheck,
    state: WorldState,
    *,
    index_kind: str,
    omission: OmissionPlan,
) -> frozenset[str]:
    """The support set the DECLARATION offers the engine for one check.

    ``complete`` re-derives the gold support on the given state; ``lexical``
    returns the frozen mention-index terms. The omission plan then deletes
    from whichever base was chosen — deletion, never widening, so a mutated
    index only ever under-approximates (the E1 direction of interest)."""
    if index_kind == DECL_COMPLETE:
        base = check.gold_support(state)
    elif index_kind == DECL_LEXICAL:
        base = check.lexical_support(state)
    else:
        raise E1StressError(
            "unknown declaration index kind",
            data={"got": index_kind, "known": [DECL_COMPLETE, DECL_LEXICAL]},
        )
    return omission.apply(check.check_id, base)


# --------------------------------------------------------------------------- #
# Driving the REAL engine.                                                     #
# --------------------------------------------------------------------------- #
class MemoryCertificateStore:
    """An in-memory stand-in for the certificate store's ``record_skip`` seam.

    The pure half has no database; the ENGINE under test is the scheduler and
    the certificate validation logic, both of which run unmodified. This store
    keeps the recorded certificates so the next pass can validate them through
    the real :func:`validate_certificate` path. Type refusals mirror the real
    store's — a skip without a :class:`ReuseCertificate` is refused."""

    def __init__(self, *, deployment: str) -> None:
        if not deployment:
            raise E1StressError("MemoryCertificateStore requires a deployment")
        self.deployment = deployment
        self.recorded: list[ReuseCertificate] = []

    def record_skip(self, conn: Any, certificate: ReuseCertificate) -> str:
        if not isinstance(certificate, ReuseCertificate):
            raise E1StressError(
                "record_skip requires a ReuseCertificate",
                data={"got_type": type(certificate).__name__},
            )
        if certificate.deployment != self.deployment:
            raise E1StressError(
                "certificate deployment does not match the store",
                data={"store": self.deployment,
                      "certificate": certificate.deployment},
            )
        self.recorded.append(certificate)
        return certificate.digest


_SCALARS = {
    "policy_fingerprint": "e1:policy",
    "precedence_fingerprint": "e1:precedence",
    "catalog_version_id": "e1:catalog",
}


def _step_envelope(check: SupportCheck, *, unbounded: bool = False) -> Any:
    declared: dict[str, Any] = {
        "nodes": [{"subtype": "e1_finding",
                   "clause_id": f"e1:{check.check_id}"}],
        "claim_keys": [check.check_id],
    }
    if unbounded:
        # the fallback probe: an unbounded dimension has NO possible-effects
        # summary, so the effect rung answers RunAlways (§9.4) whenever the
        # certificate rungs cannot skip.
        declared["unbounded"] = ["claim_keys"]
    return normalize_write_envelope(declared, source_name=check.check_id)


def _step_facts(
    check: SupportCheck, state: WorldState, support: frozenset[str],
    envelope_fp: str,
) -> CertificateDependencies:
    return CertificateDependencies(
        input_digests={t: _term_digest(state, t) for t in sorted(support)},
        operator_versions={check.check_id: "v1"},
        model_prompt_keys={},
        envelope_fingerprint=envelope_fp,
        constraint_set={check.check_id: "e1:v1"},
        generator_versions={},
        competitor_bounds={},
        **_SCALARS,
    )


@dataclass
class _EngineState:
    """Cross-pass engine memory: carried results, recorded reads, prior certs.

    ``recorded_support`` is each step's read set AS RECORDED AT ITS LAST RUN —
    the object a certificate actually names (design v8 §4). Validation
    re-digests exactly those terms against the current state; it never
    re-derives the read set without running (that would be circular: deriving
    the current reads IS running the query). A NEW fact can therefore reach a
    step only through a membership/adjacency/policy term the recorded read set
    carries — which is the mechanism E1 exists to stress."""

    carried: dict[str, str] = field(default_factory=dict)
    recorded_support: dict[str, frozenset[str]] = field(default_factory=dict)
    prior_facts: dict[str, CertificateDependencies] = field(default_factory=dict)
    prior_certs: dict[str, ReuseCertificate] = field(default_factory=dict)


@dataclass(frozen=True)
class StepGrade:
    """One step's graded outcome at one update."""

    check_id: str
    check_class: str
    action: str            # skip | run
    cause: str
    unsafe: bool           # skip that kept a result full processing contradicts
    gold_skippable: bool   # full processing says the result would not change
    support_size: int
    #: The certificate digest the engine's outcome carried. A SKIP always has
    #: one — :class:`...reuse_scheduler.StepOutcome` refuses a skip without a
    #: digest — and a RUN never does. Recorded here so the certificate-only
    #: skip path can be witnessed over what the harness actually produced,
    #: rather than only over the scheduler's own type invariant.
    certificate_digest: str = ""


@dataclass(frozen=True)
class UpdateGrade:
    """All step grades for one update, plus the pass terminal state."""

    index: int
    operation: str
    terminal: str
    steps: tuple[StepGrade, ...]


# --------------------------------------------------------------------------- #
# The arm runner.                                                              #
# --------------------------------------------------------------------------- #
#: Run causes counted as FALLBACK: the engine ran not because a stated change
#: overlapped, but because a premise could not be established at all.
FALLBACK_RUN_CAUSES: frozenset[str] = frozenset({
    CAUSE_RUN_ALWAYS, CAUSE_UNSTATED_CHANGE,
})


@dataclass(frozen=True)
class E1ArmResult:
    """One arm's exact counts, per the preregistered metric definitions.

    ``unsafe_by_class`` keys every check class that produced at least one
    unsafe skip. ``skip_precision`` is safe skips over proposed skips;
    ``skip_recall`` is safe skips over gold-skippable step-updates; both are
    ``None`` (never a defaulted 1.0 or 0.0) when their denominator is zero.
    ``runtime_saved`` carries :func:`pure_arm_runtime_saved_marker` — this
    arm has no wall clock, so it names where the measurement is taken rather
    than reporting a number about this process."""

    arm: str
    seed: int
    index_kind: str
    omission: str
    #: which fallback probes were armed ("none" outside the fallback arm).
    probes: str
    updates: int
    steps_graded: int
    skips_proposed: int
    unsafe_skips: int
    safe_skips: int
    gold_skippable: int
    runs: int
    fallback_runs: int
    run_cause_tally: Mapping[str, int]
    skip_cause_tally: Mapping[str, int]
    unsafe_by_class: Mapping[str, int]
    support_size_min: int
    support_size_mean: float
    support_size_max: int

    @property
    def skip_precision(self) -> float | None:
        if self.skips_proposed == 0:
            return None
        return self.safe_skips / self.skips_proposed

    @property
    def skip_recall(self) -> float | None:
        if self.gold_skippable == 0:
            return None
        return self.safe_skips / self.gold_skippable

    @property
    def fallback_rate(self) -> float | None:
        if self.steps_graded == 0:
            return None
        return self.fallback_runs / self.steps_graded

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "arm": self.arm,
            "seed": self.seed,
            "index_kind": self.index_kind,
            "omission": self.omission,
            "probes": self.probes,
            "updates": self.updates,
            "steps_graded": self.steps_graded,
            "skips_proposed": self.skips_proposed,
            "unsafe_skips": self.unsafe_skips,
            "safe_skips": self.safe_skips,
            "gold_skippable": self.gold_skippable,
            "runs": self.runs,
            "fallback_runs": self.fallback_runs,
            "skip_precision": self.skip_precision,
            "skip_recall": self.skip_recall,
            "fallback_rate": self.fallback_rate,
            "run_cause_tally": dict(sorted(self.run_cause_tally.items())),
            "skip_cause_tally": dict(sorted(self.skip_cause_tally.items())),
            "unsafe_by_class": dict(sorted(self.unsafe_by_class.items())),
            "support_size_min": self.support_size_min,
            "support_size_mean": self.support_size_mean,
            "support_size_max": self.support_size_max,
            "runtime_saved": pure_arm_runtime_saved_marker(),
        }


def run_arm(
    world: SupportWorld,
    checks: Sequence[SupportCheck],
    *,
    arm: str,
    index_kind: str,
    omission: OmissionPlan | None = None,
    collect: list[UpdateGrade] | None = None,
    #: fallback probes (review finding EWD-E1-3): checks whose ENVELOPE is
    #: declared unbounded (the effect rung answers RunAlways when the
    #: certificate rungs cannot skip), and checks whose change STATEMENT is
    #: withheld (the ladder reads UNSTATED and refuses). They make the
    #: fallback causes reachable, so the fallback-rate instrument can move.
    unbounded_envelope: frozenset[str] = frozenset(),
    omit_statement: frozenset[str] = frozenset(),
) -> E1ArmResult:
    """Run one arm: the whole update trajectory through the REAL engine, every
    step graded against full processing at every update.

    The first state is published in full (every step runs; there is nothing to
    reuse), which seeds the carried results, the prior dependency snapshots,
    and the prior certificates. Each subsequent update then drives
    :func:`run_reuse_pass` with declaration-derived inputs and grades every
    outcome. Deterministic from ``(world, checks, index_kind, omission)``."""
    omission = omission or OmissionPlan()
    checks = tuple(checks)
    if not checks:
        raise E1StressError("run_arm needs at least one check")

    unknown_probe = (unbounded_envelope | omit_statement) - {
        c.check_id for c in checks
    }
    if unknown_probe:
        raise E1StressError(
            "a fallback probe names an unknown check — a probe that touches "
            "nothing proves nothing",
            data={"unknown": sorted(unknown_probe)},
        )
    envelopes = {
        c.check_id: _step_envelope(
            c, unbounded=c.check_id in unbounded_envelope
        )
        for c in checks
    }
    envelope_fps = {
        cid: hashlib.sha256(
            json.dumps(env.summary(), sort_keys=True).encode("utf-8")
        ).hexdigest()
        for cid, env in envelopes.items()
    }
    steps = {
        c.check_id: ReuseStep(
            step_id=c.check_id, envelope=envelopes[c.check_id],
            producer_id=c.check_id,
            claim_predicates=(E1_FINDING_PREDICATE,),
        )
        for c in checks
    }

    engine = _EngineState()
    initial = world.states[0]
    for check in checks:
        support = declared_support(
            check, initial, index_kind=index_kind, omission=omission
        )
        engine.carried[check.check_id] = check.evaluate(initial)
        engine.recorded_support[check.check_id] = support
        engine.prior_facts[check.check_id] = _step_facts(
            check, initial, support, envelope_fps[check.check_id]
        )

    counters = {
        "steps": 0, "skips": 0, "unsafe": 0, "safe": 0,
        "gold_skippable": 0, "runs": 0, "fallback": 0,
    }
    run_causes: dict[str, int] = {}
    skip_causes: dict[str, int] = {}
    unsafe_by_class: dict[str, int] = {}
    support_sizes: list[int] = []

    for update_index, post in enumerate(world.states[1:]):
        pre = world.states[update_index]
        current_facts: dict[str, CertificateDependencies] = {}
        changed_stmts: dict[str, ChangeStatement] = {}
        for check in checks:
            # Validation re-digests the READS THE LAST RUN RECORDED — never a
            # freshly derived read set (deriving the current reads IS running
            # the query; a validator that could do it for free would make the
            # whole certificate mechanism pointless and the absence
            # counterexample invisible).
            recorded = engine.recorded_support[check.check_id]
            support_sizes.append(len(recorded))
            current_facts[check.check_id] = _step_facts(
                check, post, recorded, envelope_fps[check.check_id]
            )
            # The declaration-projected change statement: the index maps
            # changed terms to the checks whose RECORDED reads carry them. A
            # change the index cannot see produces a truthful-to-the-index
            # nothing_changed — which is exactly how an incomplete index
            # licenses an unsafe skip.
            if check.check_id in omit_statement:
                # the probe: no statement at all — the ladder must read
                # UNSTATED (a distinct type), never an empty permission.
                pass
            elif changed_terms_between(pre, post, sorted(recorded)):
                changed_stmts[check.check_id] = ChangedStateTerms(
                    buckets=frozenset(
                        {(E1_FINDING_PREDICATE, check.check_id)}
                    ),
                )
            else:
                changed_stmts[check.check_id] = (
                    ChangedStateTerms.nothing_changed()
                )

        store = MemoryCertificateStore(deployment="e1-pure")
        result = run_reuse_pass(
            None,
            deployment="e1-pure",
            generation_id=f"u{update_index:02d}",
            worklist_seed=tuple(c.check_id for c in checks),
            steps=steps,
            prior_certificates=dict(engine.prior_certs),
            changed_terms=changed_stmts,
            current_facts=current_facts,
            prior_facts=dict(engine.prior_facts),
            prior_generation_id=f"u{update_index - 1:02d}",
            cert_store=store,
        )
        if result.terminal != TERMINAL_FIXED_POINT:
            raise E1StressError(
                "the E1 pure pass must reach a fixed point — a discard or "
                "abort here is a harness defect, not a finding",
                data={"terminal": result.terminal,
                      "cause": result.fallback_cause},
            )

        recorded_certs = {c.step_id: c for c in store.recorded}
        grades: list[StepGrade] = []
        by_id = {c.check_id: c for c in checks}
        for outcome in result.outcomes:
            check = by_id[outcome.step_id]
            truth = check.evaluate(post)
            gold_skippable = truth == engine.carried[check.check_id]
            counters["steps"] += 1
            if gold_skippable:
                counters["gold_skippable"] += 1
            if outcome.action == ACTION_SKIP:
                counters["skips"] += 1
                skip_causes[outcome.cause] = skip_causes.get(outcome.cause, 0) + 1
                unsafe = not gold_skippable
                if unsafe:
                    counters["unsafe"] += 1
                    unsafe_by_class[check.check_class] = (
                        unsafe_by_class.get(check.check_class, 0) + 1
                    )
                else:
                    counters["safe"] += 1
                # the skip keeps the carried result — that is what a skip IS.
            else:
                counters["runs"] += 1
                run_causes[outcome.cause] = run_causes.get(outcome.cause, 0) + 1
                if outcome.cause in FALLBACK_RUN_CAUSES:
                    counters["fallback"] += 1
                engine.carried[check.check_id] = truth
                # a RUN re-derives and re-records the step's read set on the
                # state it just evaluated — the only moment a read set may be
                # recorded (design v8 §5: derived, never declared free-hand).
                rerecorded = declared_support(
                    check, post, index_kind=index_kind, omission=omission
                )
                engine.recorded_support[check.check_id] = rerecorded
                engine.prior_facts[check.check_id] = _step_facts(
                    check, post, rerecorded, envelope_fps[check.check_id]
                )
                unsafe = False
            if outcome.action == ACTION_SKIP:
                # a skip advances the snapshot label without changing content.
                engine.prior_facts[check.check_id] = (
                    current_facts[check.check_id]
                )
            grades.append(StepGrade(
                check_id=check.check_id, check_class=check.check_class,
                action=outcome.action, cause=outcome.cause, unsafe=unsafe,
                gold_skippable=gold_skippable,
                support_size=len(current_facts[check.check_id].input_digests),
                certificate_digest=outcome.certificate_digest or "",
            ))

        # skips carry their newly recorded certificates into the next pass.
        engine.prior_certs = recorded_certs
        if collect is not None:
            collect.append(UpdateGrade(
                index=update_index,
                operation=world.operations[update_index],
                terminal=result.terminal,
                steps=tuple(grades),
            ))

    if not support_sizes:
        raise E1StressError("run_arm graded no updates", data={"arm": arm})
    probe_parts = []
    if unbounded_envelope:
        probe_parts.append("unbounded:" + ",".join(sorted(unbounded_envelope)))
    if omit_statement:
        probe_parts.append("unstated:" + ",".join(sorted(omit_statement)))
    return E1ArmResult(
        arm=arm, seed=world.seed, index_kind=index_kind,
        omission=_omission_label(omission),
        probes="+".join(probe_parts) or "none",
        updates=len(world.states) - 1,
        steps_graded=counters["steps"],
        skips_proposed=counters["skips"],
        unsafe_skips=counters["unsafe"],
        safe_skips=counters["safe"],
        gold_skippable=counters["gold_skippable"],
        runs=counters["runs"],
        fallback_runs=counters["fallback"],
        run_cause_tally=dict(run_causes),
        skip_cause_tally=dict(skip_causes),
        unsafe_by_class=dict(unsafe_by_class),
        support_size_min=min(support_sizes),
        support_size_mean=sum(support_sizes) / len(support_sizes),
        support_size_max=max(support_sizes),
    )


def _omission_label(omission: OmissionPlan) -> str:
    if not omission.drop_exact and omission.drop_rate == 0.0:
        return "none"
    parts = []
    if omission.drop_exact:
        parts.append(f"exact:{len(omission.drop_exact)}")
    if omission.drop_rate:
        parts.append(f"rate:{omission.drop_rate:g}:seed:{omission.rate_seed}")
    return "+".join(parts)


# --------------------------------------------------------------------------- #
# The drop-one sweep: remove one dependency at a time.                         #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DropOneFinding:
    """One single-dependency omission's graded consequence over the whole run.

    ``consequence`` (review finding EWD-E1: labelled by what the TERM did,
    not by a run-count proxy):

    * ``unsafe_skip_detected`` — the omission licensed at least one skip
      full processing contradicts, and the harness caught it;
    * ``still_ran`` — the dropped term's value CHANGED somewhere on the
      trajectory, yet no unsafe skip resulted: the change was caught
      through another declared term (or the result was unaffected);
    * ``benign`` — the dropped term never changed value on this trajectory,
      so the omission could not have hidden anything here."""

    check_id: str
    term: str
    consequence: str
    unsafe_skips: int
    seed: int = 0


CONSEQUENCE_UNSAFE = "unsafe_skip_detected"
CONSEQUENCE_RAN = "still_ran"
CONSEQUENCE_BENIGN = "benign"


def run_drop_one_sweep(
    world: SupportWorld, checks: Sequence[SupportCheck]
) -> tuple[DropOneFinding, ...]:
    """Remove ONE declared dependency at a time from the complete index and
    rerun the whole trajectory per omission — the §E1 "remove one dependency
    at a time" arm, exhaustive over every ``(check, term)`` the complete
    index declares anywhere on the trajectory."""
    pairs: set[tuple[str, str]] = set()
    for state in world.states:
        for check in checks:
            for term in check.gold_support(state):
                pairs.add((check.check_id, term))

    findings: list[DropOneFinding] = []
    baseline = run_arm(
        world, checks, arm="drop-one-baseline", index_kind=DECL_COMPLETE,
    )
    if baseline.unsafe_skips:
        raise E1StressError(
            "the drop-one baseline (complete index) has unsafe skips — the "
            "sweep cannot attribute omissions on a refuted baseline",
            data={"unsafe_skips": baseline.unsafe_skips},
        )
    changed_terms: set[str] = set()
    for pre, post in zip(world.states, world.states[1:]):
        for _cid, term in pairs:
            if term_value(pre, term) != term_value(post, term):
                changed_terms.add(term)
    for check_id, term in sorted(pairs):
        result = run_arm(
            world, checks,
            arm=f"drop-one:{check_id}:{term}",
            index_kind=DECL_COMPLETE,
            omission=OmissionPlan(drop_exact=frozenset({(check_id, term)})),
        )
        if result.unsafe_skips:
            consequence = CONSEQUENCE_UNSAFE
        elif term in changed_terms:
            consequence = CONSEQUENCE_RAN
        else:
            consequence = CONSEQUENCE_BENIGN
        findings.append(DropOneFinding(
            check_id=check_id, term=term, consequence=consequence,
            unsafe_skips=result.unsafe_skips, seed=world.seed,
        ))
    return tuple(findings)


# --------------------------------------------------------------------------- #
# The report.                                                                  #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class E1Report:
    """The whole pure-half E1 result set for one manifest.

    ``verdict`` is derived, never authored: ``pass`` iff every declared-
    complete arm has zero unsafe skips AND every counterexample class produced
    at least one caught unsafe skip under the lexical index AND the drop-one
    sweep found at least one detected unsafe omission (the harness is proven
    able to catch what it exists to catch). Anything else is the named
    failure.

    The two SMOKE-half metrics are ATTACHED, never recomputed here:
    ``runtime_saved`` is an
    :class:`okg.bench.synth.e1_runtime_saved.RuntimeSaved` and
    ``checker_mutations`` are
    :class:`...e1_runtime_saved.MutationDetection` rows, both produced by a
    live replay and handed in through :meth:`with_smoke_metrics`. A report
    with none attached says so by name — it does not report a zero saving or
    an empty detection set as if they had been measured. A report WITH them
    attached is graded on them too: an undetected checker mutation refutes
    the run."""

    manifest_digest: str
    arms: tuple[E1ArmResult, ...]
    drop_one: tuple[DropOneFinding, ...]
    #: Duck-typed on purpose (``to_canonical_dict`` + ``status``): the pure
    #: half must stay importable without the live-path machinery.
    runtime_saved: Any | None = None
    checker_mutations: tuple[Any, ...] = ()

    def with_smoke_metrics(
        self,
        *,
        runtime_saved: Any,
        checker_mutations: Sequence[Any],
    ) -> "E1Report":
        """Attach the smoke half's measured metrics to this pure-half report.

        Refuses an empty mutation set: attaching "no mutations" would turn
        the checker-mutation-detection surface from unmeasured into
        vacuously-clean, which is exactly the substitution this metric
        exists to prevent."""
        rows = tuple(checker_mutations)
        if runtime_saved is None:
            raise E1StressError(
                "with_smoke_metrics needs a measured runtime-saved result; "
                "attach nothing rather than attaching None, so the report "
                "keeps its honest not-attached marker"
            )
        if not rows:
            raise E1StressError(
                "with_smoke_metrics needs at least one checker-mutation row; "
                "an empty detection set would read as a clean sweep that was "
                "never run"
            )
        return replace(
            self, runtime_saved=runtime_saved, checker_mutations=rows,
        )

    def undetected_mutations(self) -> tuple[Any, ...]:
        return tuple(m for m in self.checker_mutations if not m.detected)

    def complete_arms(self) -> tuple[E1ArmResult, ...]:
        return tuple(
            a for a in self.arms
            if a.index_kind == DECL_COMPLETE and a.omission == "none"
            and a.probes == "none"
        )

    def lexical_arms(self) -> tuple[E1ArmResult, ...]:
        return tuple(a for a in self.arms if a.index_kind == DECL_LEXICAL)

    def lexical_classes_caught(self) -> frozenset[str]:
        caught: set[str] = set()
        for a in self.lexical_arms():
            caught.update(a.unsafe_by_class)
        return frozenset(caught)

    @property
    def verdict(self) -> str:
        complete = self.complete_arms()
        if not complete:
            return "invalid:no_complete_arm"
        if any(a.unsafe_skips for a in complete):
            return "refuted:unsafe_skip_under_complete_index"
        missing = set(COUNTEREXAMPLE_CLASSES) - self.lexical_classes_caught()
        if missing:
            return (
                "invalid:counterexample_class_not_exercised:"
                + ",".join(sorted(missing))
            )
        if not any(
            f.consequence == CONSEQUENCE_UNSAFE for f in self.drop_one
        ):
            return "invalid:drop_one_sweep_detected_nothing"
        # Attached smoke metrics are GRADED, not merely reported: a checker
        # mutation the live path did not catch refutes the run. Vacuous when
        # nothing is attached, and `with_smoke_metrics` refuses an empty set,
        # so this can never pass by having measured nothing.
        undetected = self.undetected_mutations()
        if undetected:
            return (
                "refuted:checker_mutation_not_detected:"
                + ",".join(sorted({m.mutation_kind for m in undetected}))
            )
        return "pass"

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema": "okg.synth.e1-support-stress-report.v2",
            "manifest_digest": self.manifest_digest,
            "verdict": self.verdict,
            "runtime_saved": (
                self.runtime_saved.to_canonical_dict()
                if self.runtime_saved is not None
                else {
                    "status": RUNTIME_SAVED_NOT_MEASURED_HERE,
                    "reason": (
                        "no smoke-half measurement is attached to this "
                        "report; runtime saved is a wall-clock quantity and "
                        "is never inferred from the pure arms"
                    ),
                    "measured_by": RUNTIME_SAVED_MEASURED_BY,
                }
            ),
            "checker_mutation_detection": (
                {
                    "status": "measured",
                    "rows": [
                        m.to_canonical_dict() for m in self.checker_mutations
                    ],
                }
                if self.checker_mutations
                else {
                    "status": "not_measured_in_this_report",
                    "reason": (
                        "checker mutations run on the live smoke path; an "
                        "empty row set here means the arm was not run, not "
                        "that nothing escaped detection"
                    ),
                    "measured_by": (
                        "okg.bench.synth.e1_runtime_saved."
                        "classify_certificate_corruption and "
                        "classify_dependency_fact_mutation"
                    ),
                }
            ),
            "arms": [a.to_canonical_dict() for a in self.arms],
            "drop_one": [
                {
                    "check_id": f.check_id, "term": f.term,
                    "consequence": f.consequence,
                    "unsafe_skips": f.unsafe_skips,
                    "seed": f.seed,
                }
                for f in self.drop_one
            ],
            "lexical_index_disposition": (
                "rejected: textual overlap is not a read set (design v8 §5); "
                "every counterexample class must produce a caught unsafe skip"
            ),
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_canonical_dict(), sort_keys=True, separators=(",", ":")
        )


def run_report(manifest: E1Manifest) -> E1Report:
    """Run the full preregistered pure-half arm grid for one manifest.

    Per seed: the declared-complete arm, the lexical arm, the four omission-
    rate arms over the complete index, the fallback-probe arm (unbounded
    envelope on the local check, withheld statement on the alias check —
    the causes that make the fallback-rate instrument movable), and the
    exhaustive drop-one sweep."""
    arms: list[E1ArmResult] = []
    drop_one: list[DropOneFinding] = []
    for seed in manifest.seeds:
        world = build_world(seed, updates=manifest.updates_per_seed)
        checks = build_checks(world.states[0])
        arms.append(run_arm(
            world, checks, arm=f"complete:seed{seed}",
            index_kind=DECL_COMPLETE,
        ))
        arms.append(run_arm(
            world, checks, arm=f"lexical:seed{seed}",
            index_kind=DECL_LEXICAL,
        ))
        for rate in manifest.omission_rates:
            arms.append(run_arm(
                world, checks,
                arm=f"omit-{rate:g}:seed{seed}",
                index_kind=DECL_COMPLETE,
                omission=OmissionPlan(drop_rate=rate, rate_seed=seed),
            ))
        arms.append(run_arm(
            world, checks, arm=f"fallback:seed{seed}",
            index_kind=DECL_COMPLETE,
            unbounded_envelope=frozenset({"check.local.doc0-val"}),
            omit_statement=frozenset({"check.alias.handbook-val"}),
        ))
        drop_one.extend(run_drop_one_sweep(world, checks))
    return E1Report(
        manifest_digest=manifest.digest(),
        arms=tuple(arms),
        drop_one=tuple(drop_one),
    )


# --------------------------------------------------------------------------- #
# The SMOKE half: the omission mutation on the live parity replay.             #
# --------------------------------------------------------------------------- #
MUTATION_DECLARED_SUPPORT_OMITTED = "declared_support_omitted"


class SupportOmissionMutation:
    """E1's induced-omission arm on the ``synthetic-corpus-smoke`` live path.

    Duck-typed for :func:`okg.bench.synth.parity.run_update`'s ``mutation``
    seam (the same shape as :class:`okg.bench.synth.parity.Mutation`): the
    victim document step's CURRENT dependency snapshot is replaced with its
    prior generation's — the declared support index stops seeing the input
    that actually changed. The change statement and the corpus stay truthful;
    only the SUPPORT DECLARATION lies, which is exactly E1's subject. The
    ladder is then entitled to approve an unchanged-inputs skip, the executor
    keeps a stale result, and the three-way parity grade must go MISMATCH —
    the harness catching the unsafe skip is the witness."""

    kind = MUTATION_DECLARED_SUPPORT_OMITTED

    def __init__(self, *, prefer_step: str = "") -> None:
        #: the step to hide the omission under, when the caller knows which
        #: document's change matters (e.g. one whose accepted answer moves —
        #: hiding a change that changes nothing would prove nothing). Must be
        #: a step whose declared inputs actually moved this update; refused
        #: loudly otherwise.
        self.prefer_step = prefer_step
        self.target_step: str = ""
        self.detail: dict[str, Any] = {}

    def apply(
        self,
        *,
        dsn: str,
        cone: Any,
        changed_terms: dict[str, Any],
        prior_facts: dict[str, Any],
        prior_certificates: dict[str, Any],
        dominance: dict[str, Any],
        poisoned: frozenset[str],
        state: Any,
        deployment: str,
    ) -> tuple[Any, ...]:
        victims = sorted(
            step for step in cone.steps
            if step in prior_facts
            and step in cone.current_facts
            and prior_facts[step].input_digests
            != cone.current_facts[step].input_digests
        )
        if not victims:
            raise E1StressError(
                "SupportOmissionMutation found no step whose declared inputs "
                "moved this update — an omission that hides nothing proves "
                "nothing; mutate an update with a real document delta",
                data={"steps": sorted(cone.steps)},
            )
        if self.prefer_step:
            if self.prefer_step not in victims:
                raise E1StressError(
                    "SupportOmissionMutation: the preferred step's declared "
                    "inputs did not move this update — hiding nothing proves "
                    "nothing",
                    data={"prefer_step": self.prefer_step,
                          "victims": victims},
                )
            victim = self.prefer_step
        else:
            victim = victims[0]
        self.target_step = victim
        before = dict(cone.current_facts[victim].input_digests)
        after = dict(prior_facts[victim].input_digests)
        self.detail = {
            "victim": victim,
            "hidden_input_keys": sorted(
                k for k in set(before) | set(after)
                if before.get(k) != after.get(k)
            ),
        }
        facts = dict(cone.current_facts)
        facts[victim] = prior_facts[victim]
        cone = replace(cone, current_facts=facts)
        return (
            cone, changed_terms, prior_facts, prior_certificates, dominance,
            poisoned, state,
        )
