"""Layered corruption planner over the ground-truth world.

Three layers with distinct semantics (design §4):
- ``kg_fork``: divergent belief — the fork changes a fact; some renderings
  show the fork, the rest the truth.
- ``render``: transcription — the graph is right, one rendering garbles it.
- ``post_render``: direct text damage applied after rendering (the #754
  operator library's territory; the runner routes it through the operator
  plan/apply contract).

Every corruption row carries its detectability partition, assigned by a
pure per-class function of (class, params, realized visibility,
counterpart placement, backup-check availability):
- ``cross_doc``: a clean counterpart rendering exists in a different
  document.
- ``internal_only``: no clean counterpart, but a declared backup check
  (functional-dependency or functional-predicate policy) can fire.
- ``world_knowledge``: implausible in isolation — a domain-knowledgeable
  judge can flag it without any counterpart. Scored in its own
  denominator, never in cross-doc recall.
- ``undetectable_by_construction``: no counterpart AND no backup check —
  in particular, structural ghost references at k=1 are undetectable in
  v1 because the referent-inventory detector lane does not exist yet.

Conformance is fail-closed everywhere: a class whose eligible population
(after excluding facts already claimed by another class) cannot satisfy
its requested count raises — the planner never silently under-delivers.

Plausible-in-isolation (design §5): KG-fork numeric perturbations are
sampled inside the world's own empirical range for the fact kind; a
perturbation landing outside it (only via ``allow_implausible``) is
auto-labeled ``world_knowledge``, never ``cross_doc``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Mapping

from okg.bench.synth.config import ClassSpec, SynthConfig
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.worldschema import Fact, GroundTruthWorld, WorldError


class CorruptionError(ValueError):
    """Typed rejection: the requested corruption is inexpressible."""


PARTITION_CROSS_DOC = "cross_doc"
PARTITION_INTERNAL_ONLY = "internal_only"
PARTITION_WORLD_KNOWLEDGE = "world_knowledge"
PARTITION_UNDETECTABLE = "undetectable_by_construction"

LAYER_KG_FORK = "kg_fork"
LAYER_RENDER = "render"
LAYER_POST_RENDER = "post_render"

# Which classes have a counterpart-free backup check that does NOT derive
# from the world's own distributions AND exists in the v1 smoke deployment
# (functional predicate policy or declared functional dependency). The
# structural referent-inventory lane does NOT exist yet, so the structural
# classes honestly carry False (review finding M4): at k=1 they are
# undetectable_by_construction, not internal_only.
CLASS_BACKUP_CHECK = {
    "value_perturbation": False,
    "entity_swap": False,
    "cardinality_violation": True,
    "polarity_flip": False,
    "derived_value": True,
    "digit_transposition": False,
    "unit_conversion_error": False,
    "label_value_mismatch": False,
    "broken_reference": False,
    "dangling_entity": False,
    # The Σ-coverage classes (synth-constraint-coverage). incompatible_status_pair
    # is detected by an incompatible_pair policy that needs a surviving clean
    # counterpart (no counterpart-free backup check), like polarity_flip.
    # linked_reference and ambiguous_subject are detected by a relational
    # policy that fires on the corrupted claim + its partner / the subject key
    # alone (a counterpart-free backup check exists).
    "incompatible_status_pair": False,
    "linked_reference": True,
    "ambiguous_subject": True,
}

# Which Σ constraint KIND each class's corruption trips (design: recall is
# reported per kind, never pooled). Every legacy class produces the functional
# shape (one value per key); the three coverage classes each trip one of the
# relational kinds. The gold manifest carries this per corruption so the
# scorecard can report detection recall per kind rather than let a kind that
# never fires hide behind one that always does.
KIND_FUNCTIONAL = "functional"
KIND_INCOMPATIBLE_PAIR = "incompatible_pair"
KIND_LINKED_PAIR = "linked_pair"
KIND_UNRESOLVED_KEY = "unresolved_key"
CONSTRAINT_KINDS = (
    KIND_FUNCTIONAL,
    KIND_INCOMPATIBLE_PAIR,
    KIND_LINKED_PAIR,
    KIND_UNRESOLVED_KEY,
)
CLASS_CONSTRAINT_KIND = {
    "incompatible_status_pair": KIND_INCOMPATIBLE_PAIR,
    "linked_reference": KIND_LINKED_PAIR,
    "ambiguous_subject": KIND_UNRESOLVED_KEY,
}


def constraint_kind_for(corruption_class: str) -> str:
    """The Σ constraint kind a class trips (functional unless a coverage class)."""
    return CLASS_CONSTRAINT_KIND.get(corruption_class, KIND_FUNCTIONAL)


def entity_slug(full_name: str) -> str:
    """The H1 entity slug the deterministic claim grammar derives for a patient.

    Byte-compatible with ``claim_corpus_source._slug`` over the H1 title
    ``"Patient {full_name}"`` — a divergent copy would zero the link, so the
    core-purity boundary forbids importing the grammar's ``_slug`` and instead
    the copies are pinned equal by ``test_world::test_slug_copies_are_byte_equal``
    (the same discipline as ``goldseal._entity_slug``). A linked_pair corruption
    rewrites a record_reference value to this slug so the claim links (link_key =
    the source value) to the SAME patient's claims, whose normalized_key IS this
    slug."""
    return re.sub(r"[^a-z0-9]+", "_", f"patient {full_name}".lower()).strip("_")


@dataclass(frozen=True)
class CorruptionRow:
    """One planned corruption — the gold row before rendering resolves spans."""

    corruption_id: str
    corruption_class: str
    layer: str
    fact_id: str
    clean_value: str
    corrupted_value: str
    visibility_corrupted: int
    partition: str
    params: Mapping[str, str]


@dataclass(frozen=True)
class TwinRow:
    """A benign hard-negative twin: truthful variation that must NOT flag.

    Constructed by the RENDERER from realized variation sites (design §5 —
    hard negatives are generative rendering knobs), not planned here.
    """

    twin_id: str
    family: str
    fact_id: str
    detail: Mapping[str, str]


@dataclass(frozen=True)
class CorruptionPlan:
    rows: tuple[CorruptionRow, ...]


def _numeric_facts(world: GroundTruthWorld, prefix: str) -> list[Fact]:
    out = []
    for fact in world.facts:
        if not fact.kind.startswith(prefix) or fact.derived_from is not None:
            continue
        try:
            float(fact.value)
        except ValueError:
            continue
        out.append(fact)
    return out


def _partition_for(
    visibility: int, k_for_fact: int, *, in_range: bool, backup_check: bool
) -> str:
    """The pure partition-assignment function (design §5).

    ``visibility`` is the REALIZED corrupted-rendering count the class
    emits (review finding L9 — never the config value a class ignores).
    ``backup_check`` says whether a counterpart-free detector exists that
    does NOT derive from the world's own distributions. An in-range
    corruption with no counterpart and no backup check is
    ``undetectable_by_construction`` — claiming internal detectability
    there would be circular, because the only range table available is the
    one the planner sampled from.
    """
    if not in_range:
        return PARTITION_WORLD_KNOWLEDGE
    if visibility >= k_for_fact:
        return PARTITION_INTERNAL_ONLY if backup_check else PARTITION_UNDETECTABLE
    return PARTITION_CROSS_DOC


def _perturb_in_range(
    value: float, lo: float, hi: float, magnitude: float, prf: SynthPrf, *path: str
) -> float:
    """A relative perturbation clamped inside [lo, hi] and != value."""
    if hi <= lo:
        raise CorruptionError(f"degenerate range [{lo}, {hi}] for perturbation")
    delta = max(abs(value) * magnitude, (hi - lo) * 0.05)
    sign = 1.0 if prf.uniform(*path, "sign") >= 0.5 else -1.0
    candidate = value + sign * delta
    if candidate > hi or candidate < lo:
        candidate = value - sign * delta
    candidate = min(max(candidate, lo), hi)
    if abs(candidate - value) < 1e-9:
        candidate = hi if value < hi else lo
    return candidate


class Planner:
    """Builds a :class:`CorruptionPlan` from (world, config, prf)."""

    def __init__(self, world: GroundTruthWorld, config: SynthConfig, prf: SynthPrf) -> None:
        self._world = world
        self._config = config
        self._prf = prf
        self._k = {}
        self._claimed: set[str] = set()
        self._patient_by_id = {p.patient_id: p for p in world.patients}
        k_min = config.redundancy.renderings_per_fact_min
        k_max = config.redundancy.renderings_per_fact_max
        for fact in world.facts:
            self._k[fact.fact_id] = prf.randint(k_min, k_max, "redundancy", fact.fact_id)

    def renderings_for(self, fact_id: str) -> int:
        return self._k[fact_id]

    def _pick(self, candidates: list[Fact], spec: ClassSpec, cls: str) -> list[Fact]:
        """PRF-shuffled selection with optional hub placement bias.

        Candidates already claimed by an earlier class are excluded BEFORE
        the count check (review finding H1): one corruption per fact keeps
        gold unambiguous without silently shrinking any class's delivery.
        ``placement=hub`` prefers facts of patients with the most facts —
        hub corruptions have more counterparts and should be easier; the
        parameter asks whether detection exploits redundancy.
        """
        available = [f for f in candidates if f.fact_id not in self._claimed]
        if len(available) < spec.count:
            raise CorruptionError(
                f"{cls}: requested count={spec.count} exceeds eligible "
                f"population {len(available)} (after cross-class exclusion; "
                f"conformance fails, never silently under-delivers)"
            )
        shuffled = self._prf.shuffled(available, "pick", cls)
        if spec.placement == "hub":
            per_patient: dict[str, int] = {}
            for fact in self._world.facts:
                per_patient[fact.patient_id] = per_patient.get(fact.patient_id, 0) + 1
            shuffled = sorted(
                shuffled, key=lambda f: (-per_patient.get(f.patient_id, 0), f.fact_id)
            )
        picked = shuffled[: spec.count]
        self._claimed.update(f.fact_id for f in picked)
        return picked

    # -- class implementations --------------------------------------------

    def _plan_value_perturbation(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        candidates = _numeric_facts(self._world, "observation:")
        if not candidates:
            raise CorruptionError("value_perturbation: world has no numeric observations")
        for fact in self._pick(candidates, spec, "value_perturbation"):
            lo, hi = self._world.kind_range(fact.kind)
            value = fact.quantity()
            if spec.allow_implausible:
                corrupted = hi + max(1.0, (hi - lo))  # deliberately out of range
                in_range = False
            else:
                corrupted = _perturb_in_range(
                    value, lo, hi, spec.magnitude, self._prf, "perturb", fact.fact_id
                )
                in_range = True
            yield CorruptionRow(
                corruption_id=f"corr:value_perturbation:{fact.fact_id}",
                corruption_class="value_perturbation",
                layer=LAYER_KG_FORK,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value=f"{corrupted:.1f}",
                visibility_corrupted=spec.visibility_corrupted,
                partition=_partition_for(
                    spec.visibility_corrupted,
                    self._k[fact.fact_id],
                    in_range=in_range,
                    backup_check=False,
                ),
                params={"magnitude": str(spec.magnitude), "range": f"{lo}..{hi}"},
            )

    def _plan_entity_swap(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        if spec.swap_distance == "unrelated":
            raise CorruptionError(
                "entity_swap: swap_distance=unrelated is inexpressible in v1 "
                "(the world carries no cross-category formulary, and labeling "
                "a plausible sibling swap world_knowledge would misstate "
                "detectability - review finding M5; deferred)"
            )
        med_facts = [f for f in self._world.facts if f.kind.startswith("medication:")]
        if len({f.value for f in med_facts}) < 2:
            raise CorruptionError("entity_swap: world has fewer than two distinct medications")
        displays = sorted({f.value for f in med_facts})
        for fact in self._pick(med_facts, spec, "entity_swap"):
            alternatives = [d for d in displays if d != fact.value]
            swap = self._prf.choice(alternatives, "swap", fact.fact_id)
            yield CorruptionRow(
                corruption_id=f"corr:entity_swap:{fact.fact_id}",
                corruption_class="entity_swap",
                layer=LAYER_KG_FORK,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value=swap,
                visibility_corrupted=spec.visibility_corrupted,
                partition=_partition_for(
                    spec.visibility_corrupted,
                    self._k[fact.fact_id],
                    in_range=True,
                    backup_check=False,
                ),
                params={"swap_distance": spec.swap_distance},
            )

    def _plan_cardinality_violation(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        provider_facts = [f for f in self._world.facts if f.kind == "care_provider"]
        if not provider_facts:
            raise CorruptionError("cardinality_violation: world has no care_provider facts")
        names = sorted({p.name for p in self._world.practitioners})
        if len(names) < 2:
            raise CorruptionError("cardinality_violation: needs at least two practitioners")
        for fact in self._pick(provider_facts, spec, "cardinality"):
            other = self._prf.choice([n for n in names if n != fact.value], "alt", fact.fact_id)
            yield CorruptionRow(
                corruption_id=f"corr:cardinality_violation:{fact.fact_id}",
                corruption_class="cardinality_violation",
                layer=LAYER_KG_FORK,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value=other,
                visibility_corrupted=spec.visibility_corrupted,
                partition=_partition_for(
                    spec.visibility_corrupted,
                    self._k[fact.fact_id],
                    in_range=True,
                    backup_check=True,
                ),
                params={"functional_predicate": "care_provider"},
            )

    def _plan_polarity_flip(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        allergy_facts = [f for f in self._world.facts if f.kind.startswith("allergy_status:")]
        if not allergy_facts:
            raise CorruptionError("polarity_flip: world has no allergy facts")
        for fact in self._pick(allergy_facts, spec, "polarity"):
            yield CorruptionRow(
                corruption_id=f"corr:polarity_flip:{fact.fact_id}",
                corruption_class="polarity_flip",
                layer=LAYER_KG_FORK,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value="denied" if fact.value == "present" else "present",
                visibility_corrupted=spec.visibility_corrupted,
                partition=_partition_for(
                    spec.visibility_corrupted,
                    self._k[fact.fact_id],
                    in_range=True,
                    backup_check=False,
                ),
                params={},
            )

    def _plan_derived_value(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        derived = [f for f in self._world.facts if f.derived_from is not None]
        if not derived:
            raise CorruptionError(
                "derived_value: world declares no functional dependencies (inexpressible)"
            )
        for fact in self._pick(derived, spec, "derived_value"):
            value = fact.quantity()
            corrupted = value * (1.0 + max(spec.magnitude, 0.2))
            yield CorruptionRow(
                corruption_id=f"corr:derived_value:{fact.fact_id}",
                corruption_class="derived_value",
                layer=LAYER_KG_FORK,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value=f"{corrupted:.1f}",
                visibility_corrupted=spec.visibility_corrupted,
                # breaking an FD is detectable by the derived check even with
                # no counterpart rendering: internal_only at worst.
                partition=(
                    PARTITION_CROSS_DOC
                    if spec.visibility_corrupted < self._k[fact.fact_id]
                    else PARTITION_INTERNAL_ONLY
                ),
                # The input fact ids travel in gold so the funnel's expressibility
                # predicate can ask "did the declared FD inputs survive extraction"
                # from the manifest alone, never re-deriving the world.
                params={
                    "dependency": fact.derived_from[0] if fact.derived_from else "",
                    "input_fact_ids": (
                        ",".join(fact.derived_from[1]) if fact.derived_from else ""
                    ),
                },
            )

    def _plan_digit_transposition(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        candidates = [
            f
            for f in _numeric_facts(self._world, "observation:")
            if sum(c.isdigit() for c in f.value) >= 2
        ]
        if not candidates:
            raise CorruptionError("digit_transposition: no multi-digit numeric facts")
        for fact in self._pick(candidates, spec, "digit_transposition"):
            digits = list(fact.value)
            idx = [i for i, c in enumerate(digits) if c.isdigit()]
            a, b = idx[0], idx[1]
            digits[a], digits[b] = digits[b], digits[a]
            corrupted = "".join(digits)
            if corrupted == fact.value:
                corrupted = fact.value[::-1]
            lo, hi = self._world.kind_range(fact.kind)
            try:
                in_range = lo <= float(corrupted) <= hi
            except ValueError:
                in_range = False
            yield CorruptionRow(
                corruption_id=f"corr:digit_transposition:{fact.fact_id}",
                corruption_class="digit_transposition",
                layer=LAYER_RENDER,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value=str(corrupted),
                visibility_corrupted=1,  # transcription garbles exactly one rendering
                partition=_partition_for(
                    1, self._k[fact.fact_id], in_range=in_range, backup_check=False
                ),
                params={},
            )

    def _plan_unit_conversion_error(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        weights = [
            f for f in self._world.facts if f.kind == "observation:body_weight"
        ]
        if not weights:
            raise CorruptionError("unit_conversion_error: no body_weight observations")
        for fact in self._pick(weights, spec, "unit_conversion"):
            # kg rendered as if converted to lb but keeping the kg label:
            wrong = fact.quantity() * 2.20462
            yield CorruptionRow(
                corruption_id=f"corr:unit_conversion_error:{fact.fact_id}",
                corruption_class="unit_conversion_error",
                layer=LAYER_RENDER,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value=f"{wrong:.1f}",
                visibility_corrupted=1,
                partition=PARTITION_CROSS_DOC
                if self._k[fact.fact_id] > 1
                else PARTITION_WORLD_KNOWLEDGE,
                params={"mode": "kg_as_lb"},
            )

    def _plan_label_value_mismatch(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        by_patient: dict[str, list[Fact]] = {}
        for fact in _numeric_facts(self._world, "observation:"):
            by_patient.setdefault(fact.patient_id, []).append(fact)
        # candidate = the target fact (first unclaimed numeric observation of
        # a patient with a donor available); _pick enforces conformance and
        # cross-class exclusion uniformly (review finding M6).
        candidates: list[Fact] = []
        donors: dict[str, Fact] = {}
        for patient_id in sorted(by_patient):
            facts = sorted(by_patient[patient_id], key=lambda f: f.fact_id)
            unclaimed = [f for f in facts if f.fact_id not in self._claimed]
            if len(unclaimed) >= 2:
                candidates.append(unclaimed[0])
                donors[unclaimed[0].fact_id] = unclaimed[1]
        if not candidates:
            raise CorruptionError("label_value_mismatch: needs >=2 numeric observations")
        for target in self._pick(candidates, spec, "label_value"):
            donor = donors[target.fact_id]
            yield CorruptionRow(
                corruption_id=f"corr:label_value_mismatch:{target.fact_id}",
                corruption_class="label_value_mismatch",
                layer=LAYER_RENDER,
                fact_id=target.fact_id,
                clean_value=target.value,
                corrupted_value=donor.value,
                visibility_corrupted=1,
                partition=PARTITION_CROSS_DOC
                if self._k[target.fact_id] > 1
                else PARTITION_UNDETECTABLE,
                params={"donor_fact": donor.fact_id},
            )

    def _plan_broken_reference(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        birthdates = [f for f in self._world.facts if f.kind == "birthdate"]
        if not birthdates:
            raise CorruptionError("broken_reference: world has no birthdate facts")
        for fact in self._pick(birthdates, spec, "broken_ref"):
            ghost = f"encounter-{self._prf.token('ghost', fact.patient_id, length=8)}"
            yield CorruptionRow(
                corruption_id=f"corr:broken_reference:{fact.patient_id}",
                corruption_class="broken_reference",
                layer=LAYER_RENDER,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value=ghost,
                visibility_corrupted=1,
                # the ghost referent diverges from the true value rendered in
                # counterpart documents (review finding M4): k>1 is genuinely
                # cross-doc; a lone rendering is undetectable in v1 (the
                # referent-inventory lane does not exist).
                partition=PARTITION_CROSS_DOC
                if self._k[fact.fact_id] > 1
                else PARTITION_UNDETECTABLE,
                params={"referent_kind": "encounter"},
            )

    def _plan_dangling_entity(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        med_facts = [f for f in self._world.facts if f.kind.startswith("medication:")]
        if not med_facts:
            raise CorruptionError("dangling_entity: world has no medications")
        for fact in self._pick(med_facts, spec, "dangling"):
            ghost_drug = f"Zenvorix {self._prf.token('drug', fact.fact_id, length=4)}"
            yield CorruptionRow(
                corruption_id=f"corr:dangling_entity:{fact.fact_id}",
                corruption_class="dangling_entity",
                layer=LAYER_RENDER,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value=ghost_drug,
                visibility_corrupted=1,
                partition=PARTITION_CROSS_DOC
                if self._k[fact.fact_id] > 1
                else PARTITION_UNDETECTABLE,
                params={"referent_kind": "medication"},
            )

    # -- Σ-coverage classes (synth-constraint-coverage) -------------------
    #
    # These three trip the relational constraint kinds the legacy taxonomy
    # never exercised. Each records, in ``params``, the constraint KIND it
    # targets and the value-level facts the deployment's policy needs
    # (predicates are pack-derived, so the runner resolves them from the trace,
    # not here — the planner stays pack-free). The policy itself is minted at
    # the runner's ingest seam, exactly as the functional policies are.

    def _plan_incompatible_status_pair(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        """A condition rendered both 'active' and 'resolved' in one bucket — the
        pair an incompatible_pair policy forbids (a status cannot be both). Needs
        a surviving clean 'active' counterpart, so requires k>=2."""
        candidates = [
            f
            for f in self._world.facts
            if f.kind.startswith("condition_status:")
            and f.value == "active"
            and self._k[f.fact_id] >= 2
        ]
        if not candidates:
            raise CorruptionError(
                "incompatible_status_pair: world has no condition_status fact "
                "with value 'active' rendered at least twice (k>=2)"
            )
        for fact in self._pick(candidates, spec, "incompatible_status_pair"):
            yield CorruptionRow(
                corruption_id=f"corr:incompatible_status_pair:{fact.fact_id}",
                corruption_class="incompatible_status_pair",
                layer=LAYER_KG_FORK,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value="resolved",
                visibility_corrupted=spec.visibility_corrupted,
                partition=_partition_for(
                    spec.visibility_corrupted,
                    self._k[fact.fact_id],
                    in_range=True,
                    backup_check=False,
                ),
                params={
                    "constraint_kind": KIND_INCOMPATIBLE_PAIR,
                    "value_a": "active",
                    "value_b": "resolved",
                },
            )

    def _plan_linked_reference(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        """Rewrite a patient's record_reference value to that patient's OWN entity
        key, so the record_reference claim links (link_key = the source value) to
        the SAME patient's birthdate claim — the pairing a linked_pair policy
        forbids. The clean value is the chart's record id, so a clean
        record_reference never self-links; only this rewrite to the entity key
        does, which keeps the clean corpus free of a linked-pair false positive.

        CROSS-BUCKET: the two member claims sit in DIFFERENT (predicate, entity)
        buckets (record_reference vs birthdate) — the only cross-bucket kind.

        Same-PATIENT, deliberately: a linked finding needs its source and target
        claims to share an OVERLAPPING scope, and a synth claim's only
        applicability axis is its heading path, which always carries the patient's
        H1 ("Patient X > Patient Details"). Two different patients' claims have
        disjoint heading paths, and the engine treats disjoint scopes as
        NOT-comparable (never a finding), so a cross-patient link cannot fire in
        this corpus — verified against derive_linked_findings + scope_relation.
        The rewrite therefore targets the patient's own birthdate; the surface
        ("Filed under record reference <slug>.") is what survives prose extraction
        where the old age_years surface did not."""
        birth_by_patient = {
            f.patient_id: f for f in self._world.facts if f.kind == "birthdate"
        }
        candidates = [
            f
            for f in self._world.facts
            if f.kind == "record_reference" and f.patient_id in birth_by_patient
        ]
        if not candidates:
            raise CorruptionError(
                "linked_reference: world has no record_reference fact whose "
                "patient also has a birthdate (needed for the cross-bucket link)"
            )
        for fact in self._pick(candidates, spec, "linked_reference"):
            patient = self._patient_by_id[fact.patient_id]
            entity = entity_slug(patient.full_name)
            target = birth_by_patient[fact.patient_id]
            yield CorruptionRow(
                corruption_id=f"corr:linked_reference:{fact.fact_id}",
                corruption_class="linked_reference",
                layer=LAYER_KG_FORK,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value=entity,
                visibility_corrupted=spec.visibility_corrupted,
                # The linked policy fires on the corrupted source claim + the
                # target claim with no counterpart of the source needed: an
                # internal_only backup check (the policy itself).
                partition=PARTITION_INTERNAL_ONLY,
                params={
                    "constraint_kind": KIND_LINKED_PAIR,
                    "target_fact_id": target.fact_id,
                    "violates_value": target.value,
                    "link_entity": entity,
                },
            )

    def _plan_ambiguous_subject(self, spec: ClassSpec) -> Iterable[CorruptionRow]:
        """A perturbed observation on a patient whose SUBJECT KEY the deployment
        declares unresolved: what would be a functional conflict is instead
        NOT-COMPARABLE, because the engine may not bucket an ambiguous key.
        Requires k>=2 so the bucket has the two members the ambiguity applies
        to."""
        candidates = [
            f
            for f in _numeric_facts(self._world, "observation:")
            if self._k[f.fact_id] >= 2
        ]
        if not candidates:
            raise CorruptionError(
                "ambiguous_subject: world has no numeric observation rendered at "
                "least twice (k>=2) to seat an unresolved-key bucket"
            )
        for fact in self._pick(candidates, spec, "ambiguous_subject"):
            patient = self._patient_by_id[fact.patient_id]
            entity = entity_slug(patient.full_name)
            lo, hi = self._world.kind_range(fact.kind)
            corrupted = _perturb_in_range(
                fact.quantity(), lo, hi, spec.magnitude, self._prf, "ambig", fact.fact_id
            )
            yield CorruptionRow(
                corruption_id=f"corr:ambiguous_subject:{fact.fact_id}",
                corruption_class="ambiguous_subject",
                layer=LAYER_KG_FORK,
                fact_id=fact.fact_id,
                clean_value=fact.value,
                corrupted_value=f"{corrupted:.1f}",
                visibility_corrupted=spec.visibility_corrupted,
                partition=_partition_for(
                    spec.visibility_corrupted,
                    self._k[fact.fact_id],
                    in_range=True,
                    backup_check=False,
                ),
                params={
                    "constraint_kind": KIND_UNRESOLVED_KEY,
                    "unresolved_entity": entity,
                },
            )

    _CLASS_PLANNERS = {
        "value_perturbation": _plan_value_perturbation,
        "entity_swap": _plan_entity_swap,
        "cardinality_violation": _plan_cardinality_violation,
        "polarity_flip": _plan_polarity_flip,
        "derived_value": _plan_derived_value,
        "digit_transposition": _plan_digit_transposition,
        "unit_conversion_error": _plan_unit_conversion_error,
        "label_value_mismatch": _plan_label_value_mismatch,
        "broken_reference": _plan_broken_reference,
        "dangling_entity": _plan_dangling_entity,
        "incompatible_status_pair": _plan_incompatible_status_pair,
        "linked_reference": _plan_linked_reference,
        "ambiguous_subject": _plan_ambiguous_subject,
    }

    REGISTRY: tuple[str, ...] = tuple(sorted(_CLASS_PLANNERS))

    def plan(self) -> CorruptionPlan:
        rows: list[CorruptionRow] = []
        self._claimed = set()
        delivered: dict[str, int] = {}
        for name in sorted(self._config.corruption.classes):
            spec = self._config.corruption.classes[name]
            if name not in self._CLASS_PLANNERS:
                raise CorruptionError(
                    f"unknown corruption class {name!r}; registry: {self.REGISTRY}"
                )
            if spec.count == 0:
                continue
            produced = list(self._CLASS_PLANNERS[name](self, spec))
            delivered[name] = len(produced)
            rows.extend(produced)
        for name, spec in sorted(self._config.corruption.classes.items()):
            if spec.count and delivered.get(name, 0) != spec.count:
                raise CorruptionError(
                    f"{name}: delivered {delivered.get(name, 0)} of requested "
                    f"{spec.count} corruptions (conformance fails, never "
                    f"silently under-delivers)"
                )
        return CorruptionPlan(rows=tuple(rows))

    def eligibility_report(self) -> dict[str, bool]:
        """Which registry classes the loaded world can express (for docs/CLI)."""
        report = {}
        for name, planner in self._CLASS_PLANNERS.items():
            saved = set(self._claimed)
            try:
                list(planner(self, ClassSpec(count=1)))
                report[name] = True
            except (CorruptionError, WorldError):
                report[name] = False
            finally:
                self._claimed = saved
        return report
