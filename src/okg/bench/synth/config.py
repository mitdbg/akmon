"""Three-tier corpus config: strict schema, typed rejections, sealed digest.

Tier 1 (headline swept axes) and tier 2 (realism defaults) are the schema
below. Tier 3 — determinism, generation/detection independence, leakage
gates, the fresh-seed world rule, the gold schema — is deliberately NOT
configurable: there are no keys for it, and unknown keys are errors
(``extra="forbid"``), so attempting to set tier-3 behavior fails loudly.

Cross-field expressibility is validated here when it is config-internal
(majority-wrong needs redundancy support); world-dependent expressibility
(derived-value corruption needs declared functional dependencies) is
enforced fail-closed by the corruption planner against the loaded world.
"""

from __future__ import annotations

import hashlib
import json
from importlib import resources
from pathlib import Path
from typing import ClassVar, Any, Mapping

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

CONFIG_SCHEMA_VERSION = "okg.synth.config.v1"

_PRESET_PACKAGE = "okg.bench.synth.presets"

# Precedence signal names — the CLOSED admittable vocabulary for a precedence
# arm, byte-equal to the engine's five DECISIVE signal classes
# (okg.substrate.consistency.claim_model.precedence._DECISIVE). Defined as
# literals here to keep this CORE module substrate-free (test_world
# ::test_core_imports_no_substrate forbids an okg.substrate import); a no-DB
# witness (test_precedence_arm) pins these to the engine's _DECISIVE set so the
# two can never drift.
SIGNAL_OPERATOR_PRECEDENT = "operator_precedent"
SIGNAL_EXPLICIT_SUPERSESSION = "explicit_supersession"
SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE = "claim_local_effective_date"
SIGNAL_DECLARED_SOURCE_AUTHORITY = "declared_source_authority"
SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT = "dependency_adjusted_support"
ADMITTABLE_SIGNALS = frozenset({
    SIGNAL_OPERATOR_PRECEDENT,
    SIGNAL_EXPLICIT_SUPERSESSION,
    SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE,
    SIGNAL_DECLARED_SOURCE_AUTHORITY,
    SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT,
})

# Where an admitted signal ENTERS the pipeline. Only ingest_stamp is valid in
# v1: the signal is applied to the EXTRACTED claim at ingest, post-extraction,
# so the precedence measurement is isolated from extraction noise (a claim lost
# at extraction has nothing to stamp, keeping the funnel consistent). The
# document surface renders the signal into the document text and requires the
# extractor to carry it; it is a DECLARED DEFERRAL, not an omission.
SURFACE_INGEST_STAMP = "ingest_stamp"
SURFACE_DOCUMENT = "document"

# The four Σ constraint kinds a run may require to fire, byte-equal to the
# engine's policy kinds (claim_projection_derivation._POLICY_KINDS) and to
# corruption.CONSTRAINT_KINDS. Literals here to keep this CORE module
# substrate-free and free of a corruption import (corruption imports config);
# a no-DB witness pins them to corruption.CONSTRAINT_KINDS so the two can't drift.
CONSTRAINT_KIND_VOCABULARY = frozenset(
    {"functional", "incompatible_pair", "linked_pair", "unresolved_key"}
)

# The CLOSED corruption-class vocabulary an error-cost table may price, byte-equal
# to corruption.CLASS_BACKUP_CHECK's keys. Literals here for the same reason as
# CONSTRAINT_KIND_VOCABULARY above (corruption imports config, so config cannot
# import corruption); a no-DB witness pins the two sets equal. Pricing a class name
# that does not exist is a typo that would silently price nothing, so it is rejected
# at config load.
CORRUPTION_CLASS_VOCABULARY = frozenset({
    "value_perturbation",
    "entity_swap",
    "cardinality_violation",
    "polarity_flip",
    "derived_value",
    "digit_transposition",
    "unit_conversion_error",
    "label_value_mismatch",
    "broken_reference",
    "dangling_entity",
    "incompatible_status_pair",
    "linked_reference",
    "ambiguous_subject",
})

# The five benchmark ACTION classes that carry a dollar price. Named once here so
# the config schema, the sealed manifest, and the scorecard's spent-action ledger
# cannot disagree about what a run pays for.
ACTION_PARSE = "parse"
ACTION_EXTRACT_DOCUMENT = "extract_document"
ACTION_CHECK = "check"
ACTION_CORRECTION = "correction"
ACTION_PUBLISH = "publish"
ACTION_CLASSES = (
    ACTION_PARSE,
    ACTION_EXTRACT_DOCUMENT,
    ACTION_CHECK,
    ACTION_CORRECTION,
    ACTION_PUBLISH,
)

# How an INFINITE error cost is written down in a sealed manifest.
#
# JSON has no Infinity literal. Python's json module emits the bare token
# ``Infinity`` by default, which is NOT valid JSON: a conformant parser rejects
# it, so a manifest carrying one could not be re-read (or re-digested) outside
# Python, and ``manifest_digest`` would be a hash of bytes no other reader
# accepts. The config therefore never stores an infinite float at all — a
# mandatory class is NAMED in ``mandatory_classes`` — and the sealed table writes
# its cost as this string sentinel, so the manifest stays standards-valid JSON and
# the sha256 round-trips through any parser. ``error_cost == MANDATORY_COST`` and
# ``mandatory is True`` are the same statement in the sealed table, by
# construction.
MANDATORY_COST = "inf"


class ConfigError(ValueError):
    """Typed rejection: the config is invalid or inexpressible as requested."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RedundancyAxis(_StrictModel):
    """Tier 1 — how much restatement cross-doc detection gets to work with."""

    renderings_per_fact_min: int = Field(ge=1, default=1)
    renderings_per_fact_max: int = Field(ge=1, default=3)

    @model_validator(mode="after")
    def _ordered(self) -> "RedundancyAxis":
        if self.renderings_per_fact_max < self.renderings_per_fact_min:
            raise ConfigError(
                "redundancy: renderings_per_fact_max < renderings_per_fact_min"
            )
        return self


class ClassSpec(_StrictModel):
    """Per-class corruption request: rate + difficulty parameters."""

    count: int = Field(ge=0, default=0)
    magnitude: float = Field(gt=0.0, le=1.0, default=0.15)
    swap_distance: str = Field(default="sibling", pattern="^(sibling|unrelated)$")
    visibility_corrupted: int = Field(ge=1, default=1)
    placement: str = Field(default="uniform", pattern="^(uniform|hub)$")
    allow_implausible: bool = False


class CorruptionAxis(_StrictModel):
    """Tier 1 — prevalence, subtlety, visibility span (per class)."""

    classes: Mapping[str, ClassSpec] = Field(default_factory=dict)


class BenignVariationAxis(_StrictModel):
    """Tier 1 — truthful variation the pipeline must NOT flag.

    v1 ships exactly ONE wired family: unit-equivalent restatement, applied
    at render time at this rate; the rendered variation sites ARE the
    hard-negative twin gold. The rounding / alias / typo families are
    deferred (design §10) and deliberately have NO keys here — an
    unimplemented knob that changes config_digest without changing the
    corpus is silent theater (review finding H2), and extra="forbid"
    rejects them loudly instead.
    """

    unit_rescale_rate: float = Field(ge=0.0, le=1.0, default=0.2)


class ScaleAxis(_StrictModel):
    """Tier 1 — corpus size. ``patients`` deterministically subsets the
    frozen world (fail-closed when it exceeds the world's population);
    ``max_facts_per_patient`` caps the FHIR->KG mapping."""

    patients: int = Field(ge=1, default=8)
    max_facts_per_patient: int = Field(ge=1, default=40)


class RealismDefaults(_StrictModel):
    """Tier 2 — hand-set in v1; a measured calibration can upgrade these
    defaults later without any schema change. The typo-noise knob is
    deferred with the other unimplemented realism families (no key)."""

    # referral_letter carries every fact kind, which is what lifts realized
    # redundancy to k>=3 and makes a corroboration majority expressible for
    # more than one predicate (see the pack).
    genres: tuple[str, ...] = (
        "discharge_summary", "medication_list", "clinic_note", "referral_letter",
    )
    slot_shuffle: bool = True
    # How a fact is written down. "structured" emits `Label: value` lines;
    # "prose" emits narrative sentences.
    #
    # This is a MODE rather than a replacement because the two lanes have
    # different extractors. The LLM-free lane reads documents with the
    # deterministic label parser (claim_corpus_source.parse_document_claims,
    # which matches line by line), so a prose corpus would extract ZERO claims
    # there -- no claims, no buckets, no findings, and every recall metric
    # would report a clean-looking zero denominator instead of failing. The
    # incompatible pairing is rejected at load; see _validate_render_mode.
    render_mode: str = Field(default="structured", pattern="^(structured|prose)$")
    # Declares the extractor the lane will use, so the incompatible pairing is
    # caught at CONFIG time rather than surfacing as an empty corpus.
    extractor: str = Field(default="deterministic_labels",
                           pattern="^(deterministic_labels|llm)$")

    @model_validator(mode="after")
    def _validate_render_mode(self) -> "RealismDefaults":
        """Reject prose + the label parser LOUDLY.

        Silence is the dangerous outcome here: that pairing yields a corpus
        the extractor cannot read at all, so the run completes and reports
        zero-denominator recall rather than an error. Failing at config load
        keeps the mistake un-runnable.
        """
        if self.render_mode == "prose" and self.extractor == "deterministic_labels":
            raise ConfigError(
                "render_mode 'prose' is incompatible with extractor "
                "'deterministic_labels': that extractor matches 'Label: value' "
                "line by line, so a narrative corpus yields NO claims and every "
                "recall metric would report a clean zero denominator instead of "
                "failing. Use extractor 'llm' for prose, or keep render_mode "
                "'structured' for the LLM-free lane."
            )
        return self


class PrecedenceArm(_StrictModel):
    """Tier 1 — the precedence signals this arm ADMITS, and where each enters.

    ``admits`` is the set of DECISIVE precedence signals the arm studies, drawn
    from the closed vocabulary :data:`ADMITTABLE_SIGNALS` (the engine's five
    decisive classes). The DEFAULT — ``admits`` empty — is today's
    corroboration-only lane, byte-for-byte: no signal is stamped, the scope
    condition asserts every ranking attr is absent, and the deterministic lane's
    measurements are unchanged.

    ``surface`` names, per admitted signal, WHERE it enters the pipeline. Only
    ``ingest_stamp`` is valid in v1 (:data:`SURFACE_INGEST_STAMP`); ``document``
    is a declared deferral and is rejected LOUDLY with the reason. An admitted
    signal with no explicit surface defaults to ``ingest_stamp``
    (:meth:`resolved_surface`).
    """

    admits: tuple[str, ...] = ()
    surface: Mapping[str, str] = Field(default_factory=dict)
    # How many DISTINCT buckets to seat per arm role (task.complete.capstone). The
    # default 1 is today's single-axis arm, byte-identical. The LLM capstone raises
    # it so each decisive basis is proven on SEVERAL buckets: an imperfect extractor
    # can drop a stamped target's claim, so one bucket per basis lets a basis zero
    # out by draw; N buckets per basis make P(all N lost) negligible, and the gate
    # requires each basis PRESENT (not every target hit). Bounded by the corpus's
    # supply of one_vs_one / majority buckets (fail-closed if it runs short).
    role_redundancy: int = Field(ge=1, default=1)
    # The POISONING rate r (synth-authority-poisoning). Over the ELIGIBLE conflicted
    # buckets left after the arm's roles are seated — a conflicted bucket carrying a
    # corrupted claim and a clean counterpart whose corrupted value is NOT already a
    # strict majority (so absent authority the corrupted value would not win) — a
    # PRF-chosen fraction r has ``declared_source_authority`` stamped HIGH on the
    # CORRUPTED claim, so the engine settles the bucket canonically on the corrupted
    # value: a FLIP (a mis-repair the poison caused). r is an ADVERSARIAL axis
    # ORTHOGONAL to ``admits``: the scope guard admits the authority attr on exactly
    # the poisoned claims (never elsewhere), the poisoned buckets are sealed in gold,
    # and the scorecard reports the flip rate while keeping poisoned buckets OUT of
    # every other repair denominator. The DEFAULT 0.0 poisons nothing, so no bucket
    # is stamped; ONLY the config_digest moves (the field is part of the canonical
    # dump), exactly as role_redundancy and the provenance axis did.
    poison_rate: float = Field(ge=0.0, le=1.0, default=0.0)
    #: The rescue axis (concord-system-stress): the fraction of
    #: corrupted-MAJORITY buckets that get a CORRECT authority stamp.
    #: Complement of poison_rate, read against it as a curve.
    rescue_rate: float = Field(ge=0.0, le=1.0, default=0.0)

    @model_validator(mode="after")
    def _validate(self) -> "PrecedenceArm":
        seen: set[str] = set()
        for signal in self.admits:
            if signal not in ADMITTABLE_SIGNALS:
                raise ConfigError(
                    f"precedence_arm.admits: unknown signal {signal!r}; the "
                    f"closed vocabulary is {sorted(ADMITTABLE_SIGNALS)}"
                )
            if signal in seen:
                raise ConfigError(
                    f"precedence_arm.admits: duplicate signal {signal!r}"
                )
            seen.add(signal)
        for signal, surface in self.surface.items():
            if signal not in seen:
                raise ConfigError(
                    f"precedence_arm.surface names {signal!r}, which the arm does "
                    f"not admit (admits={sorted(seen)})"
                )
            if surface == SURFACE_DOCUMENT:
                raise ConfigError(
                    f"precedence_arm.surface[{signal!r}]='document' is a DECLARED "
                    "DEFERRAL in v1: the document surface renders the signal into "
                    "the document text and needs prose templates that render "
                    "signals traceably plus an extension of the LLM extraction "
                    "contract beyond (attribute, value, quote); it becomes "
                    "worthwhile only after synth-stage-attribution can attribute "
                    "the extra extraction losses it introduces. Use "
                    f"{SURFACE_INGEST_STAMP!r}."
                )
            if surface != SURFACE_INGEST_STAMP:
                raise ConfigError(
                    f"precedence_arm.surface[{signal!r}]={surface!r} is invalid; "
                    f"only {SURFACE_INGEST_STAMP!r} is valid in v1"
                )
        return self

    def resolved_surface(self) -> dict[str, str]:
        """Every admitted signal → its surface (default ``ingest_stamp``)."""
        return {
            signal: self.surface.get(signal, SURFACE_INGEST_STAMP)
            for signal in sorted(self.admits)
        }


class ProvenanceAxis(_StrictModel):
    """Tier 1 — the provenance-group / majority-flavor axis
    (synth-provenance-diversity).

    ``enabled`` declares the run STAMPS a ``provenance_group`` attr onto the
    corrupted-rendering claims of every majority-visibility site at the ingest
    seam (never rendered into a document — the corpus stays byte-identical
    between flavors). The engine reads no such attr today, so a stamped and an
    unstamped publish of the same corpus yield row-wise identical findings and
    resolutions; the attr is a DECLARED AXIS the per-arm scope condition admits,
    NOT a ranking signal.

    ``flavor_split`` assigns COPY vs INDEPENDENT provenance flavor to the sorted
    majority sites. ``alternate`` (the default) gives a single run BOTH flavors
    with a balanced per-flavor denominator; ``copy`` / ``independent`` force one
    flavor for a single-flavor witness. The mode literals are byte-equal to
    provenance.SPLIT_MODES (a no-DB witness pins them so the two cannot drift).

    The DEFAULT — ``enabled`` False — keeps every existing preset's ingest
    byte-for-byte unchanged; ONLY the config_digest moves (the axis is part of
    the canonical dump), exactly as surviving_conflict_floor and precedence_arm
    did.
    """

    enabled: bool = False
    flavor_split: str = Field(
        default="alternate", pattern="^(alternate|copy|independent)$"
    )


class ActionPrices(_StrictModel):
    """Tier 1 — the dollar price of each benchmark ACTION class.

    The paper's objective charges for work as well as for error: a result's cost
    is the remaining error cost plus the price of the actions taken to get there.
    The five classes are the run's real work surfaces — parsing a document,
    extracting from a document, evaluating a check, applying an operator
    correction, publishing a generation.

    The DEFAULT is every price 0.0: a run that declares no prices spends nothing,
    so ``spent_action_cost`` reads a total of 0 and every existing preset's
    measurements are byte-for-byte today's. Only the config_digest moves (the
    prices are part of the canonical dump), exactly as surviving_conflict_floor
    and precedence_arm did.
    """

    parse: float = Field(ge=0.0, default=0.0)
    extract_document: float = Field(ge=0.0, default=0.0)
    check: float = Field(ge=0.0, default=0.0)
    correction: float = Field(ge=0.0, default=0.0)
    publish: float = Field(ge=0.0, default=0.0)

    def as_table(self) -> dict[str, float]:
        """The price table, keyed by ACTION_CLASSES, sorted — the sealed shape."""
        prices = {
            ACTION_PARSE: self.parse,
            ACTION_EXTRACT_DOCUMENT: self.extract_document,
            ACTION_CHECK: self.check,
            ACTION_CORRECTION: self.correction,
            ACTION_PUBLISH: self.publish,
        }
        return {name: float(prices[name]) for name in sorted(ACTION_CLASSES)}


class CostModel(_StrictModel):
    """Tier 1 — the run's ECONOMICS: per-class error costs and action prices.

    ``error_costs`` prices a corruption CLASS in dollars; a class with no entry
    costs ``default_error_cost``. ``mandatory_classes`` names the classes whose
    error cost is INFINITE — the paper's MANDATORY violations, the ones whose
    survival disqualifies a result outright. Infinity is expressed by NAMING the
    class rather than by storing a float, because JSON cannot carry an Infinity
    literal (see :data:`MANDATORY_COST`); a class named mandatory may therefore
    not also carry a finite dollar amount, which is rejected as ambiguous.

    The DEFAULT — every class at 1.0, nothing mandatory, every price 0.0 — is the
    IDENTITY: remaining error cost then equals the count of unrepaired
    corruptions and spent action cost is 0, so today's count metrics are
    reproduced exactly and only the config_digest moves.
    """

    error_costs: Mapping[str, float] = Field(default_factory=dict)
    default_error_cost: float = Field(ge=0.0, default=1.0)
    mandatory_classes: tuple[str, ...] = ()
    action_prices: ActionPrices = ActionPrices()

    @model_validator(mode="after")
    def _validate(self) -> "CostModel":
        for name, dollars in self.error_costs.items():
            if name not in CORRUPTION_CLASS_VOCABULARY:
                raise ConfigError(
                    f"cost.error_costs: unknown corruption class {name!r}; the "
                    f"closed vocabulary is {sorted(CORRUPTION_CLASS_VOCABULARY)}"
                )
            if dollars < 0.0:
                raise ConfigError(
                    f"cost.error_costs[{name!r}]={dollars} is negative; an error "
                    "cost is a loss in dollars, never a gain"
                )
        seen: set[str] = set()
        for name in self.mandatory_classes:
            if name not in CORRUPTION_CLASS_VOCABULARY:
                raise ConfigError(
                    f"cost.mandatory_classes: unknown corruption class {name!r}; "
                    f"the closed vocabulary is {sorted(CORRUPTION_CLASS_VOCABULARY)}"
                )
            if name in seen:
                raise ConfigError(
                    f"cost.mandatory_classes: duplicate class {name!r}"
                )
            seen.add(name)
            if name in self.error_costs:
                raise ConfigError(
                    f"cost: class {name!r} is declared MANDATORY (error cost "
                    f"infinite) AND priced at {self.error_costs[name]} dollars. A "
                    "violation cannot cost both infinity and a finite amount; drop "
                    "one of the two declarations."
                )
        return self

    def is_mandatory(self, corruption_class: str) -> bool:
        return corruption_class in self.mandatory_classes

    def error_cost_of(self, corruption_class: str) -> float | None:
        """The class's dollar error cost, or ``None`` when it is INFINITE."""
        if self.is_mandatory(corruption_class):
            return None
        return float(self.error_costs.get(corruption_class, self.default_error_cost))

    def sealed_error_costs(self) -> dict[str, dict[str, Any]]:
        """The full per-class error-cost table, over the CLOSED vocabulary.

        Every class gets a row (default-derived ones included) so a run's
        economics are auditable from the manifest alone rather than only for the
        classes that happened to be planted. Infinity is the string sentinel.
        """
        table: dict[str, dict[str, Any]] = {}
        for name in sorted(CORRUPTION_CLASS_VOCABULARY):
            mandatory = self.is_mandatory(name)
            table[name] = {
                "error_cost": MANDATORY_COST if mandatory else self.error_cost_of(name),
                "mandatory": mandatory,
            }
        return table


class SynthConfig(_StrictModel):
    schema_version: str = CONFIG_SCHEMA_VERSION
    preset_name: str = "custom"
    seed: str = Field(min_length=1)
    world_path: str
    redundancy: RedundancyAxis = RedundancyAxis()
    corruption: CorruptionAxis = CorruptionAxis()
    benign_variation: BenignVariationAxis = BenignVariationAxis()
    scale: ScaleAxis = ScaleAxis()
    realism: RealismDefaults = RealismDefaults()
    clean_fp_ceiling: int = Field(ge=0, default=0)
    # The declared statistical floor on SURVIVING conflicts (req.attrib.llm-lane):
    # the count of injected corruptions that reached the engine as an answerable
    # conflict (funnel states {not_detected, detected_only, mis_repaired, repaired}
    # within the cross_doc / internal_only partitions). Default 0 means NOT
    # asserted — the LLM-free lane never sizes a floor. A prose preset declares a
    # positive floor, and the runner asserts the REALIZED count meets it after
    # artifacts are written; an under-powered run fails loudly rather than
    # reporting on a tiny denominator. Adding this field moves every
    # config_digest (it is part of the canonical dump), which is why the smoke
    # golden regenerates with the change — only its digest fields move, no score.
    surviving_conflict_floor: int = Field(ge=0, default=0)
    # The precedence arm (synth-precedence-arms): which decisive signals this run
    # ADMITS and stamps at ingest. Default empty = corroboration-only, so scores
    # in the default arm are byte-for-byte unchanged; ONLY the config_digest moves
    # (the arm is part of the canonical dump), which is why the smoke golden
    # regenerates with the change — exactly as surviving_conflict_floor did.
    precedence_arm: PrecedenceArm = PrecedenceArm()
    # The Σ constraint kinds this run REQUIRES to fire (synth-constraint-coverage).
    # Default empty = not asserted, so smoke and the LLM presets are unchanged
    # except their config_digest moves (the field is part of the canonical dump,
    # like surviving_conflict_floor). A coverage preset lists all four kinds and
    # the runner FAILS if any listed kind has a zero detection denominator — a
    # kind the corpus was meant to exercise but did not, which pooling would hide.
    require_constraint_kinds: tuple[str, ...] = ()
    # The declared floor on REALIZED-MAJORITY-PRESERVED sites (task.complete.capstone):
    # the count of majority-visibility corruptions whose post-extraction vote still
    # kept the planned corrupted majority (corrupted votes > clean votes in the gold
    # bucket). The capstone gate asserts mis_repaired > 0 over exactly these sites and
    # that their count meets this floor — a mis-repair metric read over a tiny or
    # extraction-inverted denominator proves nothing. Default 0 = not asserted, so
    # every non-capstone preset is unchanged except its config_digest (the field is
    # part of the canonical dump, like surviving_conflict_floor).
    majority_wrong_floor: int = Field(ge=0, default=0)
    # The provenance-group / majority-flavor axis (synth-provenance-diversity).
    # Default disabled = no ingest stamp, so scores in every existing preset are
    # byte-for-byte unchanged; ONLY the config_digest moves (the axis is part of
    # the canonical dump), exactly as surviving_conflict_floor and precedence_arm
    # did. A flavor preset enables it, and the scorecard then reports mis_repaired
    # per flavor against its own preserved-majority denominator.
    provenance: ProvenanceAxis = ProvenanceAxis()
    # The run's ECONOMICS (synth-cost-weighted-gold): per-corruption-class dollar
    # error costs, the MANDATORY classes (infinite cost), and the per-action-class
    # dollar prices. Sealed into the manifest so a re-score cannot silently
    # re-price a recorded run. The DEFAULT is the IDENTITY — every error cost 1,
    # nothing mandatory, every price 0 — under which remaining error cost equals
    # the count of unrepaired corruptions and spent action cost is 0, so every
    # existing preset's count metrics are reproduced exactly and ONLY the
    # config_digest moves (the cost model is part of the canonical dump), exactly
    # as surviving_conflict_floor, precedence_arm and provenance did.
    cost: CostModel = CostModel()
    materialize_pdf: bool = False

    @model_validator(mode="after")
    def _expressible(self) -> "SynthConfig":
        if self.schema_version != CONFIG_SCHEMA_VERSION:
            raise ConfigError(
                f"config schema {self.schema_version!r} != {CONFIG_SCHEMA_VERSION!r}"
            )
        seen_kinds: set[str] = set()
        for kind in self.require_constraint_kinds:
            if kind not in CONSTRAINT_KIND_VOCABULARY:
                raise ConfigError(
                    f"require_constraint_kinds: unknown kind {kind!r}; the closed "
                    f"vocabulary is {sorted(CONSTRAINT_KIND_VOCABULARY)}"
                )
            if kind in seen_kinds:
                raise ConfigError(
                    f"require_constraint_kinds: duplicate kind {kind!r}"
                )
            seen_kinds.add(kind)
        k_max = self.redundancy.renderings_per_fact_max
        for name, spec in self.corruption.classes.items():
            if not spec.count:
                continue
            if spec.visibility_corrupted > k_max:
                raise ConfigError(
                    f"class {name!r}: visibility_corrupted={spec.visibility_corrupted} "
                    f"exceeds renderings_per_fact_max={k_max} (inexpressible)"
                )
            if spec.visibility_corrupted > 1 and k_max < 3:
                raise ConfigError(
                    f"class {name!r}: majority-wrong visibility requires "
                    f"renderings_per_fact_max >= 3, got {k_max} (inexpressible)"
                )
        return self

    # -- digest ------------------------------------------------------------

    #: Axes added AFTER corpora were sealed. An axis that is OFF does not change
    #: the corpus, so it must not change the corpus's identity: including it
    #: unconditionally would move every config, manifest, history and chain
    #: digest in the repository and silently invalidate every sealed row and
    #: golden that pins one. Omitted only at its default; a run that actually
    #: USES the axis digests differently, which is correct — that IS a different
    #: corpus. Adding an axis here is a deliberate compatibility decision, not a
    #: convenience: a NEW axis whose default changes the generated corpus must
    #: NOT be listed, because then the digest would be lying.
    _DIGEST_OMIT_AT_DEFAULT: ClassVar[tuple[tuple[str, str, Any], ...]] = (
        ("precedence_arm", "rescue_rate", 0.0),
    )

    def canonical_json_bytes(self) -> bytes:
        payload = self.model_dump(mode="json")
        for block, field_name, default in self._DIGEST_OMIT_AT_DEFAULT:
            section = payload.get(block)
            if isinstance(section, dict) and section.get(field_name) == default:
                section.pop(field_name, None)
        return json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")

    def config_digest(self) -> str:
        return hashlib.sha256(self.canonical_json_bytes()).hexdigest()


def load_config(source: Mapping[str, Any] | str | Path) -> SynthConfig:
    """Load and validate a config from a mapping or YAML path."""
    if isinstance(source, (str, Path)):
        raw = yaml.safe_load(Path(source).read_text(encoding="utf-8"))
    else:
        raw = dict(source)
    if not isinstance(raw, Mapping):
        raise ConfigError(f"config must be a mapping, got {type(raw).__name__}")
    try:
        return SynthConfig.model_validate(raw)
    except ConfigError:
        raise
    except Exception as exc:  # pydantic ValidationError → typed rejection
        raise ConfigError(f"invalid synth config: {exc}") from exc


def load_preset(name: str) -> SynthConfig:
    """Load a named in-repo preset (smoke / default / easy / hard)."""
    try:
        text = (resources.files(_PRESET_PACKAGE) / f"{name}.yaml").read_text(
            encoding="utf-8"
        )
    except FileNotFoundError as exc:
        raise ConfigError(f"unknown preset {name!r}") from exc
    config = load_config(yaml.safe_load(text))
    if config.preset_name != name:
        raise ConfigError(
            f"preset file {name}.yaml declares preset_name={config.preset_name!r}"
        )
    return config
