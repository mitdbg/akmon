"""The possible-effects compiler over declared write envelopes (spec §9.4).

PACT ``consistency-resolution-aware-reuse``, task.reuse.effects-compiler
(req.reuse.sound-effects). Normative semantics in
docs/develop/architecture/consistency.md §9.4 (possible-effects summaries) and the
ratified notation map (``consistency-differential-spec/designs/notation-map.md``,
row: ``PossibleEffects`` / ``compile_possible_effects``; adjudication row 16).

What this module is
-------------------
The differential scheduler reuses a result only when a *sound* analysis proves
no permitted output of the step could change the resolved state. This module
compiles that analysis input — a :class:`PossibleEffects` summary — from
**declarations only** (a producer's :class:`~okg.substrate.producer_authority.WriteEnvelope`
propagated through the constraint read surfaces of the deployment's IR checks
into the claim buckets and selection terms it could touch). It never reads
observed output: an inferred envelope is the banned silent fallback (§9.3), and a
possible-effects summary derived from what a step *did* emit would be circular.

Three objects, and nothing else:

1. :class:`PossibleEffects` — the conservative bound on what one producer step
   could change: the check ids whose read surfaces its envelope can touch (graph
   surfaces by node-subtype / edge-triple overlap, claim-state surfaces by the
   claim-typed envelope dimensions), the claim buckets it can touch, and the
   selection terms those buckets imply. Canonical-JSON serializable with a stable
   :meth:`~PossibleEffects.digest` (the reuse certificate records it).

2. :func:`compile_possible_effects` — envelope + check specs → a
   :class:`PossibleEffects`, OR the :class:`RunAlways` sentinel when the envelope
   marks ANY dimension unbounded (no summary exists — the step runs, §9.4).

3. :func:`skip_is_justifiable` — the conservative helper the scheduler consumes:
   it approves a skip ONLY when the summary's touched sets are provably disjoint
   from every violation / candidate / selection term the changed inputs could
   involve; anything else (overlap, :class:`RunAlways`, an unknown term) refuses
   with a typed reason.

Soundness contract (req.reuse.sound-effects):

* **Over-approximation is permitted and costs only work; omission is unsound.**
  Where the key space is not enumerable from the envelope the bucket dimension
  is a declared-conservative bound (a predicate-wildcard, or the all-buckets
  sentinel) — never a guess that narrows it. A check read surface this analyzer
  cannot classify makes the check conservatively touched, never silently ignored.
* The obligation is adversarially witnessed: removing a real effect from a
  summary must make the reuse decision detectably wrong under the equivalence
  harness (``tests/substrate/consistency/test_possible_effects.py::test_omitted_effect_is_caught_by_harness``).

Boundary (v1, stated so it is not mistaken for more): the disjointness in
:func:`skip_is_justifiable` is **set-disjointness over (check ids, claim
buckets)** — there are no scoring bounds here (the incumbent-worst-vs-competitor-
best comparison and its persisted certificate are the certificate store's and the
scheduler's layer, siblings of this task). This module answers only "could this
step touch anything the change touched"; it never decides "would the touched
outcome actually beat the incumbent".

Same discipline as its claim-model siblings: **pure** — DB-free, no LLM, no wall
clock, deterministic sorted output, inputs never mutated. The compiler reads only
in-memory declarations (a :class:`WriteEnvelope`, loaded
:class:`~okg.substrate.consistency.ir_check.IrConsistencyCheckSpec` objects); the
adversarial witness that proves the guard load-bearing runs against the live
equivalence harness in the test module.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from okg.substrate.consistency.ir_check import IrConsistencyCheckSpec
from okg.substrate.consistency.claim_model.views import CLAIM_SUBTYPE
from okg.substrate.errors import SubstrateError
from okg.substrate.producer_authority import WriteEnvelope
from okg.substrate.transform import OpKind, TransformProgram

# --------------------------------------------------------------------------- #
# Surface vocabulary — the substrate Chronos surfaces a deployment cannot      #
# rebind (ir_check.registries refuses a deployment surface that shadows one).  #
# Recognition is by surface NAME for the graph plane and by the kept TABLE a   #
# binding reads for the claim plane (ir_check.claim_state_binding_tables),     #
# never by a deployment-chosen alias — mirroring ir_locality's discipline.     #
# --------------------------------------------------------------------------- #
NODE_SURFACE = "nodes_live"
EDGE_SURFACE = "edges_live"

# A claim is materialized as this graph node subtype (claim_effective_derivation
# .CLAIM_SUBTYPE); an envelope whose node signature includes it writes claims and
# therefore reaches the claim plane, even if it declares no claim_keys.
CLAIM_NODE_SUBTYPES: frozenset[str] = frozenset({CLAIM_SUBTYPE})

# Typed skip-refusal / approval reasons (SkipJudgment.reason). Named constants so
# the scheduler and the tests match on tokens, never on prose.
SKIP_APPROVED = "approved"
REFUSE_RUN_ALWAYS = "run_always"
REFUSE_CHECK_OVERLAP = "check_overlap"
REFUSE_BUCKET_OVERLAP = "bucket_overlap"
REFUSE_UNKNOWN_TERM = "unknown_term"
REFUSE_UNSTATED_CHANGE = "unstated_change"


def _canonical_json(obj: Any) -> str:
    """sha-stable canonical JSON: sorted keys, compact separators.

    The single serialization discipline for the module — every digest rests on
    it (the same idiom as ``equivalence_differ._canonical_json``)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# --------------------------------------------------------------------------- #
# Per-check read-surface reach (the input the compiler classifies).           #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CheckSurfaceReach:
    """Which read surfaces one IR check's program touches. Pure derivation.

    ``reads_nodes_live`` / ``reads_edges_live`` are the graph planes; the
    node-subtype / edge-triple overlap a graph-writing envelope tests against.
    ``claim_state_tables`` are the claim-state kept tables the check reads
    (``okg.claim_effective`` / ``okg.claim_projection``); a claim-writing
    envelope reaches these via the claim plane. ``unclassified_surfaces`` are
    deployment relations this analyzer cannot place on either plane — a check
    reading one is conservatively touched by any producer write (over-
    approximation, never a silent skip)."""

    check_id: str
    reads_nodes_live: bool
    reads_edges_live: bool
    claim_state_tables: frozenset[str]
    unclassified_surfaces: frozenset[str]

    @property
    def reads_claim_state(self) -> bool:
        return bool(self.claim_state_tables)


def _from_surface_names(program: TransformProgram) -> list[str]:
    """Every FROM-op surface name across the program's blocks, in order.

    Reads the same FROM ops ``ir_locality.derive_frontier_plan`` reads (so the
    two analyses classify from one source of truth — the assertion in
    ``test_reach_agrees_with_ir_locality_on_graph_surface_check`` rests on this)."""
    surfaces: list[str] = []
    for block in program.blocks:
        for binding in block.bindings:
            op = binding.op
            if op.kind is OpKind.FROM:
                surfaces.append(str(op.args.get("surface")))
    return surfaces


def check_surface_reach(spec: IrConsistencyCheckSpec) -> CheckSurfaceReach:
    """Classify one check's read surfaces onto the graph / claim / unknown planes.

    Recognition mirrors the rest of the engine: the graph surfaces by their
    substrate names (``nodes_live`` / ``edges_live``), the claim plane by the
    kept table each declared surface binding reads
    (:meth:`IrConsistencyCheckSpec.claim_state_surface_names`), and anything else
    as unclassified (conservatively touched). Pure."""
    claim_surface_names = spec.claim_state_surface_names()
    claim_tables = spec.claim_state_binding_tables()
    reads_nodes = False
    reads_edges = False
    unclassified: set[str] = set()
    for surface_name in _from_surface_names(spec.program):
        if surface_name == NODE_SURFACE:
            reads_nodes = True
        elif surface_name == EDGE_SURFACE:
            reads_edges = True
        elif surface_name in claim_surface_names:
            # a claim-state surface — carried on claim_tables, not unclassified.
            continue
        else:
            unclassified.add(surface_name)
    return CheckSurfaceReach(
        check_id=spec.check_id,
        reads_nodes_live=reads_nodes,
        reads_edges_live=reads_edges,
        claim_state_tables=claim_tables,
        unclassified_surfaces=frozenset(unclassified),
    )


# --------------------------------------------------------------------------- #
# The claim-bucket reach (a conservative bound over (predicate, key) buckets). #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class BucketReach:
    """The claim buckets a step can touch — a conservative bound (§9.4).

    A claim bucket is ``(predicate, normalized_key)``. The bound is a union of
    three shapes, most-to-least precise:

    * ``concrete`` — enumerated ``(predicate, key)`` buckets (the key space was
      enumerable from the envelope's claim_keys × the producer's predicates);
    * ``predicates`` — predicate-wildcards: every key under a named predicate
      (the predicate is known but the key space is not enumerable);
    * ``all_buckets`` — the top sentinel: any predicate, any key (the producer
      writes claims but not even the predicate is bounded).

    Over-approximation is sound; omission is not. A concrete changed bucket
    ``(p, k)`` overlaps this reach iff ``all_buckets``, or ``p`` is a wildcard,
    or ``(p, k)`` is an enumerated member — :meth:`touches`."""

    all_buckets: bool = False
    predicates: frozenset[str] = frozenset()
    concrete: frozenset[tuple[str, str]] = frozenset()

    @property
    def is_empty(self) -> bool:
        return not (self.all_buckets or self.predicates or self.concrete)

    def touches(self, predicate: str, key: str) -> bool:
        if self.all_buckets:
            return True
        if predicate in self.predicates:
            return True
        return (predicate, key) in self.concrete

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "all_buckets": self.all_buckets,
            "predicates": sorted(self.predicates),
            "concrete": sorted([list(b) for b in self.concrete]),
        }


def _derive_bucket_reach(
    envelope: WriteEnvelope,
    claim_predicates: frozenset[str],
    *,
    writes_claims: bool,
) -> BucketReach:
    """Derive the claim-bucket bound from the envelope's claim dimensions.

    (Called only for a finite envelope — an ``unbounded`` ``claim_keys`` short-
    circuits to :class:`RunAlways` upstream.) ``envelope.claim_keys`` are the
    enumerable normalized keys; ``claim_predicates`` are the predicates the
    producer writes claims under (a declaration the scheduler supplies, never
    inferred from output). The value vocabularies bound the VALUE axis, not the
    bucket identity ``(predicate, key)``, so they do not narrow this reach in
    v1 — they ride on :class:`PossibleEffects` as recorded provenance."""
    if not writes_claims:
        return BucketReach()
    if not claim_predicates:
        # writes claims but the predicate is not bounded -> every bucket.
        return BucketReach(all_buckets=True)
    keys = tuple(dict.fromkeys(str(k) for k in envelope.claim_keys))
    if keys:
        # enumerable key space -> concrete (predicate, key) buckets.
        concrete = frozenset(
            (predicate, key) for predicate in claim_predicates for key in keys
        )
        return BucketReach(concrete=concrete)
    # predicates known, key space not enumerable -> predicate-wildcards.
    return BucketReach(predicates=frozenset(claim_predicates))


# --------------------------------------------------------------------------- #
# The possible-effects summary + the run-always sentinel.                     #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PossibleEffects:
    """The conservative bound on what one producer step could change (§9.4).

    ``check_ids`` (union of ``graph_check_ids`` and ``claim_state_check_ids``) is
    the recorded "check ids whose read surfaces this envelope can touch". The two
    subsets differ in how :func:`skip_is_justifiable` compares them:

    * ``graph_check_ids`` are COARSE disjointness terms (a graph or unclassified
      check whose findings could change; the envelope does not carry the graph
      node ids to scope them further);
    * ``claim_state_check_ids`` are recorded but NOT compared coarsely — a
      claim-state check's findings are per-bucket, so ``bucket_reach`` is the
      sound and precise term for them. Comparing the bare check id would refuse
      every claim-plane skip (a claim-state check reads all buckets); the bucket
      reach is exactly the finer term that lets a genuinely disjoint skip pass.

    ``bucket_reach`` bounds the claim buckets (violations + candidates) it can
    touch; :meth:`selection_reach` is the selection terms those buckets imply
    (v1: competing-candidate exposure per bucket == the same bucket reach — a
    touched bucket's selected repair ``r*`` could change). The provenance fields
    (node subtypes, edge triples, claim predicates, value vocabularies, the
    nondeterministic classification) are recorded for the certificate and report;
    they are not disjointness terms. Canonical-JSON serializable with a stable
    :meth:`digest`."""

    producer_id: str
    graph_check_ids: frozenset[str]
    claim_state_check_ids: frozenset[str]
    bucket_reach: BucketReach
    node_subtypes: frozenset[str] = frozenset()
    edge_triples: frozenset[tuple[str, str, str]] = frozenset()
    claim_predicates: frozenset[str] = frozenset()
    value_vocabularies: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    nondeterministic: bool = False

    @property
    def check_ids(self) -> frozenset[str]:
        """Every touched check — the graph-surface and the claim-state checks."""
        return self.graph_check_ids | self.claim_state_check_ids

    def selection_reach(self) -> BucketReach:
        """The selection terms the touched buckets imply (competing-candidate
        exposure per bucket). v1 has no scoring bounds, so a touched bucket's
        selection ``r*`` could change iff the bucket is touched — the reach is
        the bucket reach itself."""
        return self.bucket_reach

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "kind": "possible_effects",
            "producer_id": self.producer_id,
            "check_ids": sorted(self.check_ids),
            "graph_check_ids": sorted(self.graph_check_ids),
            "claim_state_check_ids": sorted(self.claim_state_check_ids),
            "bucket_reach": self.bucket_reach.to_canonical_dict(),
            "node_subtypes": sorted(self.node_subtypes),
            "edge_triples": sorted([list(t) for t in self.edge_triples]),
            "claim_predicates": sorted(self.claim_predicates),
            "value_vocabularies": {
                k: list(v) for k, v in sorted(self.value_vocabularies.items())
            },
            "nondeterministic": self.nondeterministic,
        }

    def canonical_json(self) -> str:
        return _canonical_json(self.to_canonical_dict())

    def digest(self) -> str:
        """A stable sha256 over the canonical form (the certificate records it)."""
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RunAlways:
    """The §9.4 sentinel: an envelope with an unbounded dimension has NO summary,
    so the step always runs and can never be certified skippable.

    A distinct type, never ``None`` — a caller that receives it must run the step;
    it cannot be mistaken for "no effects". Carries the unbounded dimensions and
    a typed reason for the recorded outcome. Canonical-JSON serializable with a
    stable :meth:`digest` so the certificate layer records the run-always cause
    the same way it records a summary digest."""

    producer_id: str
    unbounded_dimensions: tuple[str, ...]
    reason: str = "unbounded_envelope"

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "kind": "run_always",
            "producer_id": self.producer_id,
            "unbounded_dimensions": list(self.unbounded_dimensions),
            "reason": self.reason,
        }

    def canonical_json(self) -> str:
        return _canonical_json(self.to_canonical_dict())

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# The compiler.                                                               #
# --------------------------------------------------------------------------- #
def compile_possible_effects(
    envelope: WriteEnvelope,
    *,
    checks: Iterable[IrConsistencyCheckSpec],
    claim_predicates: Iterable[str] = (),
    producer_id: str = "",
    claim_node_subtypes: frozenset[str] = CLAIM_NODE_SUBTYPES,
) -> PossibleEffects | RunAlways:
    """Compile a producer's possible-effects summary from declarations (§9.4).

    ``envelope`` is the producer's declared :class:`WriteEnvelope`; ``checks`` are
    the deployment's loaded IR checks whose read surfaces the envelope is tested
    against; ``claim_predicates`` are the predicates the producer writes claims
    under (a declaration — the scheduler supplies it; empty means the predicate
    is not bounded). ``claim_node_subtypes`` recognizes claim-typed node
    signatures (default: the substrate's one claim subtype).

    Returns :class:`RunAlways` when the envelope marks ANY dimension unbounded
    (no summary exists — the step runs, §9.4); otherwise a :class:`PossibleEffects`
    whose reach is a conservative over-approximation of what the step could
    change. Compilation reads only declarations; it never consults observed
    output. Pure."""
    if envelope.unbounded:
        return RunAlways(
            producer_id=producer_id,
            unbounded_dimensions=tuple(sorted(envelope.unbounded)),
        )

    claim_predicates = frozenset(str(p) for p in claim_predicates)
    node_subtypes = frozenset(
        str(n["subtype"]) for n in (envelope.signature.get("nodes") or [])
    )
    edge_triples = frozenset(
        (str(e["src_subtype"]), str(e["edge_type"]), str(e["dst_subtype"]))
        for e in (envelope.signature.get("edges") or [])
    )
    writes_nodes = bool(node_subtypes)
    writes_edges = bool(edge_triples)
    writes_claims = (
        bool(claim_predicates)
        or bool(envelope.claim_keys)
        or bool(node_subtypes & claim_node_subtypes)
    )
    writes_anything = writes_nodes or writes_edges or writes_claims

    bucket_reach = _derive_bucket_reach(
        envelope, claim_predicates, writes_claims=writes_claims
    )

    graph_check_ids: set[str] = set()
    claim_state_check_ids: set[str] = set()
    for spec in checks:
        reach = check_surface_reach(spec)
        # An unclassified read surface could be reached by any producer write:
        # conservatively touched (coarse), never silently ignored.
        if reach.unclassified_surfaces and writes_anything:
            graph_check_ids.add(spec.check_id)
        if reach.reads_nodes_live and writes_nodes:
            graph_check_ids.add(spec.check_id)
        if reach.reads_edges_live and writes_edges:
            graph_check_ids.add(spec.check_id)
        if reach.reads_claim_state and writes_claims:
            claim_state_check_ids.add(spec.check_id)

    return PossibleEffects(
        producer_id=producer_id,
        graph_check_ids=frozenset(graph_check_ids),
        claim_state_check_ids=frozenset(claim_state_check_ids),
        bucket_reach=bucket_reach,
        node_subtypes=node_subtypes,
        edge_triples=edge_triples,
        claim_predicates=claim_predicates,
        value_vocabularies={
            k: tuple(v) for k, v in envelope.value_vocabularies.items()
        },
        nondeterministic=envelope.nondeterministic,
    )


# --------------------------------------------------------------------------- #
# The scheduler-facing skip helper.                                           #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class UnstatedChange:
    """The ABSENCE of a change statement — "I did not tell you what changed".

    A distinct TYPE, never a value of :class:`ChangedStateTerms`, because the two
    readings of an empty statement are opposite: "nothing changed" is permissive
    (reuse everything) and "the change was not stated" is restrictive (reuse
    nothing from this rung). Conflating them gave the ladder a SECOND, unearned
    skip mechanism alongside certificates: a first publish handed an empty
    statement approved a justified skip for every reached step, keeping prior
    results that did not exist.

    :func:`skip_is_justifiable` refuses on this type with
    :data:`REFUSE_UNSTATED_CHANGE`. It can never compare equal to a
    :class:`ChangedStateTerms` (different types), so no set operation, ``==``, or
    ``or``-style default can silently promote silence into permission. It is the
    fail-closed default everywhere a change statement may be omitted."""

    reason: str = "no change statement was supplied for this step"


#: The one canonical "nothing was stated" value. Prefer it over constructing
#: :class:`UnstatedChange` so refusal details read identically everywhere.
UNSTATED_CHANGE = UnstatedChange()


@dataclass(frozen=True)
class ChangedStateTerms:
    """The violation / candidate / selection terms the changed inputs could
    involve — what :func:`skip_is_justifiable` tests a summary's reach against.

    ``graph_check_ids`` are graph-surface (coarse) check terms whose findings the
    change could alter. ``buckets`` are the concrete ``(predicate, key)`` claim
    buckets the change could alter — each one subsumes that bucket's resolution,
    its selected repair, AND every claim-state check finding scoped to it (they
    move together iff the bucket changes), which is why the summary's per-bucket
    reach is the sound term to compare, not a coarse claim-state check id.
    ``unknown`` marks a changed dependency that could NOT be resolved to a check
    or bucket (an unknown dependency, §9.6) — it forces a refusal; a skip is
    never approved over a term the analysis could not place.

    ``declares_nothing_changed`` is the EXPLICIT permissive statement: the caller
    asserts it looked and there was no change. It is the only way to spell an
    empty-but-permissive statement, and it must be spelled — a bare
    ``ChangedStateTerms()`` is REFUSED at construction, because an all-empty
    statement is ambiguous between that assertion and simple silence. Silence is
    :data:`UNSTATED_CHANGE`, a different type that refuses. Use
    :meth:`nothing_changed` for the permissive one."""

    graph_check_ids: frozenset[str] = frozenset()
    buckets: frozenset[tuple[str, str]] = frozenset()
    unknown: bool = False
    unknown_reason: str = ""
    declares_nothing_changed: bool = False

    def __post_init__(self) -> None:
        states_something = bool(
            self.graph_check_ids or self.buckets or self.unknown
        )
        if self.declares_nothing_changed and states_something:
            raise SubstrateError(
                "ChangedStateTerms cannot declare 'nothing changed' AND name "
                "changed terms — the statement contradicts itself",
                data={
                    "graph_check_ids": sorted(self.graph_check_ids),
                    "buckets": sorted(self.buckets),
                    "unknown": self.unknown,
                },
                fix_hint=(
                    "drop declares_nothing_changed, or drop the named terms"
                ),
            )
        if not states_something and not self.declares_nothing_changed:
            raise SubstrateError(
                "an all-empty ChangedStateTerms is ambiguous: it could mean "
                "'nothing changed' (permissive — a skip is licensed) or 'the "
                "change was not stated' (restrictive — no skip licence). The "
                "scheduler must not guess; say which one you mean.",
                fix_hint=(
                    "ChangedStateTerms.nothing_changed() to assert nothing "
                    "changed, or UNSTATED_CHANGE to say you did not state it"
                ),
            )

    @classmethod
    def nothing_changed(cls) -> "ChangedStateTerms":
        """The EXPLICIT permissive statement: the caller asserts no term changed.

        Keeps the steady-state case its reuse — a summary is trivially disjoint
        from an empty changed set, so :func:`skip_is_justifiable` approves. This
        is a claim the caller makes and is accountable for, not the default."""
        return cls(declares_nothing_changed=True)


#: What a caller may hand the ladder: a STATEMENT about the change, or the typed
#: admission that none was made. Every seam that can omit the statement is typed
#: with this union so the omission has a name instead of a permissive default.
ChangeStatement = ChangedStateTerms | UnstatedChange


@dataclass(frozen=True)
class SkipJudgment:
    """The typed verdict of :func:`skip_is_justifiable`.

    ``approved`` is the decision; ``reason`` is one of the ``SKIP_*`` / ``REFUSE_*``
    tokens (never prose the scheduler must parse). A refusal names what forced it:
    ``overlapping_checks`` for a graph-check overlap, ``overlapping_buckets`` for a
    claim-bucket overlap, ``detail`` for the run-always / unknown-term causes."""

    approved: bool
    reason: str
    overlapping_checks: tuple[str, ...] = ()
    overlapping_buckets: tuple[tuple[str, str], ...] = ()
    detail: str = ""

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "approved": self.approved,
            "reason": self.reason,
            "overlapping_checks": list(self.overlapping_checks),
            "overlapping_buckets": [list(b) for b in self.overlapping_buckets],
            "detail": self.detail,
        }


def skip_is_justifiable(
    effects: PossibleEffects | RunAlways,
    *,
    changed_state_terms: ChangeStatement,
) -> SkipJudgment:
    """Approve a skip ONLY when the summary is provably disjoint from the change.

    The conservative helper the differential scheduler consumes (v1: pure
    set-disjointness over (check ids, claim buckets) — no scoring bounds; the
    incumbent-worst-vs-competitor-best comparison is the certificate store's and
    scheduler's layer, not this module). Refuses — with a typed reason — on any of:

    * :class:`RunAlways` (an unbounded envelope can never be certified skippable);
    * :class:`UnstatedChange` — no change statement was made at all. Silence is
      not the empty set: the caller may simply not have derived the change, and
      approving on it would make omission a skip mechanism that no certificate
      earned. Only :meth:`ChangedStateTerms.nothing_changed` asserts emptiness;
    * an unknown changed term (a dependency the analysis could not place, §9.6);
    * a graph-check overlap (a touched graph/unclassified check the change also
      touches);
    * a claim-bucket overlap (a changed bucket the summary's reach can touch,
      accounting for the predicate-wildcard and all-buckets bounds).

    Approves only when none of these holds. Refusal is always sound (running a
    step is never wrong); the load-bearing property is that a summary with a real
    effect OMITTED must not let an unsound skip through — witnessed by
    ``test_omitted_effect_is_caught_by_harness``. Pure."""
    if isinstance(effects, RunAlways):
        return SkipJudgment(
            approved=False,
            reason=REFUSE_RUN_ALWAYS,
            detail=(
                f"envelope unbounded on {list(effects.unbounded_dimensions)} — "
                "no possible-effects summary exists; the step runs (§9.4)"
            ),
        )
    if isinstance(changed_state_terms, UnstatedChange):
        return SkipJudgment(
            approved=False,
            reason=REFUSE_UNSTATED_CHANGE,
            detail=(
                f"{changed_state_terms.reason} — an unstated change is not "
                "'nothing changed'; a skip needs a statement (say "
                "ChangedStateTerms.nothing_changed() if nothing did)"
            ),
        )
    if changed_state_terms.unknown:
        return SkipJudgment(
            approved=False,
            reason=REFUSE_UNKNOWN_TERM,
            detail=(
                changed_state_terms.unknown_reason
                or "a changed dependency could not be resolved to a check or bucket"
            ),
        )
    check_overlap = tuple(
        sorted(effects.graph_check_ids & changed_state_terms.graph_check_ids)
    )
    if check_overlap:
        return SkipJudgment(
            approved=False,
            reason=REFUSE_CHECK_OVERLAP,
            overlapping_checks=check_overlap,
        )
    bucket_overlap = tuple(sorted(
        (predicate, key)
        for (predicate, key) in changed_state_terms.buckets
        if effects.bucket_reach.touches(predicate, key)
    ))
    if bucket_overlap:
        return SkipJudgment(
            approved=False,
            reason=REFUSE_BUCKET_OVERLAP,
            overlapping_buckets=bucket_overlap,
        )
    return SkipJudgment(approved=True, reason=SKIP_APPROVED)


__all__ = [
    "NODE_SURFACE",
    "EDGE_SURFACE",
    "CLAIM_NODE_SUBTYPES",
    "SKIP_APPROVED",
    "REFUSE_RUN_ALWAYS",
    "REFUSE_CHECK_OVERLAP",
    "REFUSE_BUCKET_OVERLAP",
    "REFUSE_UNKNOWN_TERM",
    "REFUSE_UNSTATED_CHANGE",
    "UNSTATED_CHANGE",
    "BucketReach",
    "ChangeStatement",
    "ChangedStateTerms",
    "UnstatedChange",
    "CheckSurfaceReach",
    "PossibleEffects",
    "RunAlways",
    "SkipJudgment",
    "check_surface_reach",
    "compile_possible_effects",
    "skip_is_justifiable",
]
