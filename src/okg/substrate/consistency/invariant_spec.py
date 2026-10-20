"""The invariant specification: schema, digests, loader, and load-time checks.

PACT ``concord-spec-views-contract`` (slice 1 of the ``concord-invariant-engine``
program), task.svc.implement (req.svc.serialization + req.svc.views).
Normative design: ``designs/spec-schema.md`` v2, implementing the approved
program design v8 §2, §3.1, §4 (read-set objects), and §10.5. This slice is
the serialized contract and its loader ONLY — no scheduler, no policy
runtime, no proposal outbox, no extraction pipeline (slices 2–3).

What this module is
-------------------
1. The typed spec objects (defaults, corpora + extractors, views,
   invariants) and their **canonical JSON** digests: keys sorted, UTF-8,
   integers and strings only — floats and nulls REFUSE (rates are integer
   basis points; absent means absent). Every addressable object carries
   ``version`` and gets a sha256 over its canonical rendering; the spec
   digest is the digest of the object digests in canonical order. A digest
   change anywhere is a spec change everywhere it is referenced.
2. ``load_invariant_spec`` — the fail-closed loader. Unknown field, missing
   field, dangling reference, or a failed check refuses with a NAMED refusal
   code; nothing defaults silently. The §10.5 deployment defaults apply only
   through the explicit ``defaults`` block, which materialization copies
   into each object — filling ONLY fields the author omitted: an explicit
   ``0`` is an author's value, not an absence (review finding EWD-49), so
   time-bound-only and passes-only bounded policies are both expressible.
3. The load-time checks, in the REVIEWED order (classification runs before
   lazy admissibility, which consumes it — review finding EWD-52):
   structural → ``derivation_cycle`` (including the view→``attaches`` edge,
   review finding EWD-51) → read-set derivation and classification →
   ``lazy_inadmissible`` → ``strict_over_advisory`` (warning) →
   ``envelope_exceeds_authority``.
4. The certificate SHAPE (design §5 of the slice doc): what a certificate
   may store. ``read_collections`` entries are legal only for evaluations
   classified ``instance_certifiable`` — the join demotion is enforced by
   the constructor's type check, not by review (finding EWD-54).
   Invalidation keys on ``invariant_digest``; ``spec_digest`` is carried as
   a diagnostic only (finding EWD-53).

Read sets are DERIVED, never declared (design v8 §5): a view's or a formal
invariant's read set comes from its Transform-IR program through the same
analysis the engine already uses (:func:`...possible_effects.check_surface_reach`
for surface reach, :func:`...ir_locality.derive_frontier_plan` for the
selection-versus-join classification). ``fixed_inputs`` never adds reads —
it is a coincidence claim checked against the derived set.

One deliberate expressibility point: a *free-form* generation step — one
that consumes no declared view — is a real thing the parent design §3.1
says the specification must be able to say ("it runs every pass, and the
specification says so"). It is spelled ``query: free-form://<label>`` on a
``role: generation`` view entry: no program, no ``query_digest``,
classification ``whole_graph``, and ``runs_every_pass: true`` in the load
report. Likewise a judged invariant with NO ``context_view`` is legal and
unmaintainable (design v8 §5): recorded ``runs_every_pass``, never guessed
at.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from okg.substrate.consistency.ir_locality import derive_frontier_plan
from okg.substrate.errors import SubstrateError
from okg.substrate.transform import TransformProgram

__all__ = [
    "AUTHORITY_ADVISORY",
    "AUTHORITY_AUTHORITATIVE",
    "AUTHORITY_CLASSES",
    "AUTHORITY_OWNED",
    "CLASSIFICATION_INSTANCE",
    "CLASSIFICATION_KIND_LEVEL",
    "CLASSIFICATION_WHOLE_GRAPH",
    "CLASSIFICATIONS",
    "FREE_FORM_PREFIX",
    "MODES",
    "REFUSAL_BOUNDED_CLOCKLESS",
    "REFUSAL_DANGLING_REFERENCE",
    "REFUSAL_DERIVATION_CYCLE",
    "REFUSAL_ENVELOPE_EXCEEDS_AUTHORITY",
    "REFUSAL_FIXED_INPUT_OUTSIDE_READ_SET",
    "REFUSAL_FIXED_INPUT_TRACE_MISMATCH",
    "REFUSAL_LAZY_INADMISSIBLE",
    "REFUSAL_MISSING_FIELD",
    "REFUSAL_QUERY_DIGEST_MISMATCH",
    "REFUSAL_UNKNOWN_FIELD",
    "WARNING_STRICT_OVER_ADVISORY",
    "CorpusDecl",
    "ExtractorDecl",
    "InvariantDecl",
    "JudgeDecl",
    "LoadReport",
    "LoadedSpec",
    "PolicyDecl",
    "ReadKinds",
    "RepairsDecl",
    "SpecCertificate",
    "SpecDefaults",
    "SpecLoadRefused",
    "ViewDecl",
    "canonical_json",
    "check_fixed_inputs_coincide",
    "classify_program",
    "load_invariant_spec",
    "object_digest",
    "read_kinds_of_program",
    "spec_to_canonical_dict",
]


# --------------------------------------------------------------------------- #
# Refusal and warning codes — named, closed.                                   #
# --------------------------------------------------------------------------- #
REFUSAL_UNKNOWN_FIELD = "unknown_field"
REFUSAL_MISSING_FIELD = "missing_field"
REFUSAL_DANGLING_REFERENCE = "dangling_reference"
REFUSAL_QUERY_DIGEST_MISMATCH = "query_digest_mismatch"
REFUSAL_DERIVATION_CYCLE = "derivation_cycle"
REFUSAL_LAZY_INADMISSIBLE = "lazy_inadmissible"
REFUSAL_ENVELOPE_EXCEEDS_AUTHORITY = "envelope_exceeds_authority"
REFUSAL_BOUNDED_CLOCKLESS = "bounded_clockless"
REFUSAL_FIXED_INPUT_OUTSIDE_READ_SET = "fixed_input_outside_read_set"
WARNING_STRICT_OVER_ADVISORY = "strict_over_advisory"

AUTHORITY_OWNED = "owned"
AUTHORITY_ADVISORY = "advisory"
AUTHORITY_AUTHORITATIVE = "authoritative"
AUTHORITY_CLASSES = (
    AUTHORITY_OWNED, AUTHORITY_ADVISORY, AUTHORITY_AUTHORITATIVE,
)

MODE_STRICT = "strict"
MODE_BOUNDED = "bounded"
MODE_EVENTUAL = "eventual"
MODE_LAZY = "lazy"
MODES = (MODE_STRICT, MODE_BOUNDED, MODE_EVENTUAL, MODE_LAZY)

TIER_FORMAL = "formal"
TIER_JUDGED = "judged"

ROLE_GENERATION = "generation"
ROLE_JUDGED_CONTEXT = "judged-context"

#: Read-set classification (slice doc §4.3), recorded per view/invariant.
CLASSIFICATION_INSTANCE = "instance_certifiable"
CLASSIFICATION_KIND_LEVEL = "kind_level_only"
CLASSIFICATION_WHOLE_GRAPH = "whole_graph"
CLASSIFICATIONS = (
    CLASSIFICATION_INSTANCE, CLASSIFICATION_KIND_LEVEL,
    CLASSIFICATION_WHOLE_GRAPH,
)

#: The free-form generation sentinel (see the module docstring).
FREE_FORM_PREFIX = "free-form://"

#: The deployment-surface naming convention through which a spec program
#: reads an ANNOTATION KIND: a FROM surface named ``annotation:<kind>``. The
#: kind-level derivation graph and the lazy revelation bound are computed
#: over these derived kinds — reading is derived from the query text, never
#: declared beside it (design v8 §5).
ANNOTATION_SURFACE_PREFIX = "annotation:"

#: The kind-level token for "reads the graph plane" (nodes/edges/claim
#: state). Deliberately one coarse kind in this slice: the scheduler slice
#: may refine it; a coarser kind only ever ADDS conservative edges.
GRAPH_KIND = "graph"

#: The top element for read-kind sets: a whole-graph reader reads everything.
TOP = "⊤"


class SpecLoadRefused(SubstrateError):
    """A spec failed the fail-closed load. ``data['code']`` is the named
    refusal code; ``data['where']`` names the object."""


def _refuse(code: str, where: str, message: str, **extra: Any) -> None:
    raise SpecLoadRefused(
        f"invariant spec load refused [{code}] at {where}: {message}",
        data={"code": code, "where": where, **extra},
    )


# --------------------------------------------------------------------------- #
# Canonical JSON — integers and strings only; floats and nulls refuse.        #
# --------------------------------------------------------------------------- #
def _check_canonical_value(value: Any, where: str) -> None:
    if value is None:
        _refuse(
            REFUSAL_MISSING_FIELD, where,
            "null is not a value; absent means absent (no nulls in the "
            "canonical form)",
        )
    if isinstance(value, bool):
        _refuse(
            REFUSAL_UNKNOWN_FIELD, where,
            "booleans are not admitted to the canonical form — the closure "
            "is integers and strings (review finding EWD-SVC-7)",
        )
    if isinstance(value, float):
        _refuse(
            REFUSAL_UNKNOWN_FIELD, where,
            "floats are not admitted to the canonical form; express rates "
            "as integer basis points",
        )
    if isinstance(value, Mapping):
        for k, v in value.items():
            _check_canonical_value(v, f"{where}.{k}")
    elif isinstance(value, (list, tuple)):
        for i, v in enumerate(value):
            _check_canonical_value(v, f"{where}[{i}]")


def canonical_json(value: Any, *, where: str = "$") -> str:
    """The single canonical rendering every digest rests on."""
    _check_canonical_value(value, where)
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def object_digest(value: Any, *, where: str = "$") -> str:
    return hashlib.sha256(
        canonical_json(value, where=where).encode("utf-8")
    ).hexdigest()


# --------------------------------------------------------------------------- #
# The typed spec objects.                                                      #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SpecDefaults:
    """The §10.5 deployment defaults, all fields explicit in the spec."""

    bounded_k: int
    bounded_time_bound_days: int
    eventual_slice_basis_points: int
    repair_attempts_r: int
    repair_created_subjects_per_pass: int
    corpus_authority: str
    lazy_escalation: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "bounded_k": self.bounded_k,
            "bounded_time_bound_days": self.bounded_time_bound_days,
            "eventual_slice_basis_points": self.eventual_slice_basis_points,
            "repair_attempts_r": self.repair_attempts_r,
            "repair_created_subjects_per_pass":
                self.repair_created_subjects_per_pass,
            "corpus_authority": self.corpus_authority,
            "lazy_escalation": self.lazy_escalation,
        }


@dataclass(frozen=True)
class ExtractorDecl:
    id: str
    version: int
    #: annotation kinds; ``None`` = the envelope is UNBOUNDED — legal, only
    #: expensive: the corpus's work is never deferral-eligible (design v8
    #: §6) and the load report records that consequence.
    output_envelope: tuple[str, ...] | None

    def to_canonical_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"id": self.id, "version": self.version}
        if self.output_envelope is not None:
            out["output_envelope"] = list(self.output_envelope)
        return out


@dataclass(frozen=True)
class CorpusDecl:
    id: str
    version: int
    kind: str                      # documents | graph-region
    authority: str
    digest_algorithm: str
    extractors: tuple[ExtractorDecl, ...]

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "id": self.id, "version": self.version, "kind": self.kind,
            "authority": self.authority,
            "digest_algorithm": self.digest_algorithm,
            "extractors": [e.to_canonical_dict() for e in self.extractors],
        }


@dataclass(frozen=True)
class ViewDecl:
    id: str
    version: int
    query: str                     # ir://... program ref, or free-form://...
    query_digest: str              # "" only for free-form
    role: str                      # generation | judged-context
    renders: str                   # generation only; "" otherwise
    attaches: tuple[str, ...]      # annotation kinds generation attaches
    fixed_inputs: tuple[str, ...]
    #: resolved program; None for free-form.
    program: TransformProgram | None = field(compare=False)

    @property
    def free_form(self) -> bool:
        return self.query.startswith(FREE_FORM_PREFIX)

    def to_canonical_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "id": self.id, "version": self.version, "query": self.query,
            "role": self.role,
        }
        if self.query_digest:
            out["query_digest"] = self.query_digest
        if self.renders:
            out["renders"] = self.renders
        if self.attaches:
            out["attaches"] = list(self.attaches)
        if self.fixed_inputs:
            out["fixed_inputs"] = list(self.fixed_inputs)
        return out


@dataclass(frozen=True)
class PolicyDecl:
    mode: str
    #: bounded only. ``0`` disables that clock — an AUTHOR's value; at most
    #: one of ``k`` / ``time_bound_days`` may be 0 (refusal
    #: ``bounded_clockless`` otherwise). An omitted field is filled from
    #: the defaults block during materialization, BEFORE construction —
    #: the ``-1`` default below is only the never-observed placeholder for
    #: the non-bounded modes, which do not carry clocks at all.
    k: int = -1
    time_bound_days: int = -1
    escalation: str = ""           # lazy only

    def to_canonical_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"mode": self.mode}
        if self.mode == MODE_BOUNDED:
            out["k"] = self.k
            out["time_bound_days"] = self.time_bound_days
        if self.mode == MODE_LAZY:
            out["escalation"] = self.escalation
        return out


@dataclass(frozen=True)
class RepairsDecl:
    envelope: tuple[str, ...]      # location patterns; empty = report-only
    hints: tuple[str, ...]
    attempts_r: int

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "envelope": list(self.envelope), "hints": list(self.hints),
            "attempts_r": self.attempts_r,
        }


@dataclass(frozen=True)
class JudgeDecl:
    model: str
    prompt_template: str           # <ref@version> — digested with the spec
    settings: str                  # pinned settings ref

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "model": self.model, "prompt_template": self.prompt_template,
            "settings": self.settings,
        }


@dataclass(frozen=True)
class InvariantDecl:
    id: str
    version: int
    tier: str
    scope: str                     # corpus://<id> (this slice's scope shape)
    policy: PolicyDecl
    witness_key: tuple[str, ...]
    repairs: RepairsDecl
    # formal tier:
    query: str = ""
    query_digest: str = ""
    # judged tier:
    text: str = ""
    context_view: str = ""         # optional; absent = unmaintainable
    judge: JudgeDecl | None = None
    witness_parser: str = ""       # parser://... ; "" = whole-subject only
    program: TransformProgram | None = field(default=None, compare=False)

    def to_canonical_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "id": self.id, "version": self.version, "tier": self.tier,
            "scope": self.scope,
            "policy": self.policy.to_canonical_dict(),
            "witness_key": list(self.witness_key),
            "repairs": self.repairs.to_canonical_dict(),
        }
        prop: dict[str, Any] = {}
        if self.tier == TIER_FORMAL:
            prop["query"] = self.query
            prop["query_digest"] = self.query_digest
        else:
            prop["text"] = self.text
            if self.context_view:
                prop["context_view"] = self.context_view
            assert self.judge is not None
            prop["judge"] = self.judge.to_canonical_dict()
            if self.witness_parser:
                prop["witness_parser"] = self.witness_parser
        out["property"] = prop
        return out


@dataclass(frozen=True)
class ReadKinds:
    """The kind-level read set derived from one program (never declared).

    ``annotation_kinds`` come from ``annotation:<kind>`` FROM surfaces;
    ``graph`` is true when the program reads any graph or claim-plane
    surface (one coarse kind in this slice); ``top`` marks an underivable /
    whole-graph read set — it absorbs everything."""

    annotation_kinds: frozenset[str] = frozenset()
    graph: bool = False
    top: bool = False

    def kinds(self) -> frozenset[str]:
        if self.top:
            return frozenset({TOP})
        out = set(self.annotation_kinds)
        if self.graph:
            out.add(GRAPH_KIND)
        return frozenset(out)


def read_kinds_of_program(program: TransformProgram) -> ReadKinds:
    """Derive the kind-level read set from the program's FROM surfaces.

    The same source of truth the engine's own analyses read (a program's
    surfaces), applied at kind level: ``annotation:<kind>`` surfaces are
    annotation-kind reads; every other surface is a graph-plane read. A
    program whose surfaces cannot be enumerated derives ``top``."""
    annotation: set[str] = set()
    graph = False
    try:
        blocks = program.blocks
    except Exception:  # pragma: no cover - defensive
        return ReadKinds(top=True)
    for block in blocks:
        for binding in block.bindings:
            op = binding.op
            if op.kind.value != "from":
                continue
            surface = str(op.args.get("surface", ""))
            if not surface:
                return ReadKinds(top=True)
            if surface.startswith(ANNOTATION_SURFACE_PREFIX):
                annotation.add(surface[len(ANNOTATION_SURFACE_PREFIX):])
            else:
                graph = True
    return ReadKinds(annotation_kinds=frozenset(annotation), graph=graph)


# --------------------------------------------------------------------------- #
# Classification: selections versus joins, from the existing analysis.        #
# --------------------------------------------------------------------------- #
#: FULL-frontier reasons that mean the read set is UNBOUNDED (runs every
#: pass), versus merely join-defined (reached at kind level means run).
#: Default-deny: an unmapped reason classifies WHOLE_GRAPH — failing toward
#: more work, never toward an unearned certificate.
_WHOLE_GRAPH_REASONS = (
    "aggregation_over_other_rows",
    "search_op_reads_whole_index",
    "limit_changes_result_set",
    "no_from_surface",
)
_KIND_LEVEL_REASON_PREFIXES = (
    "non_local_join",
    "rank_over_other_rows",
    "multiple_where_bindings",
    "base_surface_not_nodes_live",
    "claim_state_surface_bucket_locality_pending",
)


def classify_program(program: TransformProgram | None) -> str:
    """``instance_certifiable`` | ``kind_level_only`` | ``whole_graph``.

    Derived through :func:`derive_frontier_plan` — the engine's own
    selection-versus-join analysis; this module adds only the mapping onto
    the certificate vocabulary. No program (free-form) is ``whole_graph``.

    Only ``subject_local`` earns ``instance_certifiable``. The
    ``node_with_incident_edges`` frontier mode is DEMOTED to
    ``kind_level_only`` here (review finding EWD-SVC-4): the frontier
    analysis classifies by FROM surfaces alone and never verifies that the
    node×edge join predicate is endpoint incidence, so a value join over
    ``edges_live`` — a join-defined enumeration with no maintainable
    membership version (design v8 §4) — would otherwise be granted instance
    certificates. Until an incidence-verified analysis exists, reached
    means run for such programs; the demotion costs work, never
    soundness."""
    if program is None:
        return CLASSIFICATION_WHOLE_GRAPH
    # the subject-column derivation reads the program's own check attrs —
    # the same source the engine's loader hands the analysis.
    check_attrs = dict(program.attrs.get("check", {}) or {})
    plan = derive_frontier_plan(program, check_attrs=check_attrs)
    if plan.mode == "subject_local":
        return CLASSIFICATION_INSTANCE
    if plan.mode == "node_with_incident_edges":
        return CLASSIFICATION_KIND_LEVEL
    reason = plan.reason
    if reason in _WHOLE_GRAPH_REASONS:
        return CLASSIFICATION_WHOLE_GRAPH
    if any(reason.startswith(p) for p in _KIND_LEVEL_REASON_PREFIXES):
        return CLASSIFICATION_KIND_LEVEL
    # subject-column underivable and every future reason: fail toward the
    # most conservative class (more work is never wrong).
    return CLASSIFICATION_WHOLE_GRAPH


# --------------------------------------------------------------------------- #
# The load report + the loaded spec.                                          #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LoadReport:
    """What the load recorded (never silently): warnings and consequences."""

    warnings: tuple[dict[str, Any], ...]
    classification: Mapping[str, str]          # object id -> classification
    runs_every_pass: tuple[str, ...]           # object ids
    unbounded_extractors: tuple[str, ...]      # "corpus/extractor" labels

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "warnings": [dict(w) for w in self.warnings],
            "classification": dict(sorted(self.classification.items())),
            "runs_every_pass": list(self.runs_every_pass),
            "unbounded_extractors": list(self.unbounded_extractors),
        }


@dataclass(frozen=True)
class LoadedSpec:
    """The loaded, checked, digest-carrying specification."""

    spec_version: int
    deployment: str
    defaults: SpecDefaults
    corpora: tuple[CorpusDecl, ...]
    views: tuple[ViewDecl, ...]
    invariants: tuple[InvariantDecl, ...]
    object_digests: Mapping[str, str]          # object id -> sha256
    spec_digest: str
    report: LoadReport

    def corpus(self, corpus_id: str) -> CorpusDecl:
        for c in self.corpora:
            if c.id == corpus_id:
                return c
        raise SpecLoadRefused(
            f"unknown corpus {corpus_id!r}",
            data={"code": REFUSAL_DANGLING_REFERENCE, "where": corpus_id},
        )

    def view(self, view_id: str) -> ViewDecl:
        for v in self.views:
            if v.id == view_id:
                return v
        raise SpecLoadRefused(
            f"unknown view {view_id!r}",
            data={"code": REFUSAL_DANGLING_REFERENCE, "where": view_id},
        )

    def invariant(self, invariant_id: str) -> InvariantDecl:
        for i in self.invariants:
            if i.id == invariant_id:
                return i
        raise SpecLoadRefused(
            f"unknown invariant {invariant_id!r}",
            data={"code": REFUSAL_DANGLING_REFERENCE, "where": invariant_id},
        )

    def invariant_digest(self, invariant_id: str) -> str:
        return self.object_digests[f"invariant:{invariant_id}"]


def spec_to_canonical_dict(spec: LoadedSpec) -> dict[str, Any]:
    """The serialized form: parse(serialize(spec)) reloads byte-stably."""
    return {
        "spec_version": spec.spec_version,
        "deployment": spec.deployment,
        "defaults": spec.defaults.to_canonical_dict(),
        "corpora": [c.to_canonical_dict() for c in spec.corpora],
        "views": [v.to_canonical_dict() for v in spec.views],
        "invariants": [i.to_canonical_dict() for i in spec.invariants],
    }


# --------------------------------------------------------------------------- #
# Parsing helpers (fail-closed).                                              #
# --------------------------------------------------------------------------- #
#: Kind names the vocabulary reserves (review finding EWD-SVC-9): an
#: annotation kind that collides with the ⊤ sentinel, the coarse graph
#: kind, or the doc-kind namespace would silently merge with (or vanish
#: from) the derivation graph.
_RESERVED_KIND_PREFIXES = ("doc:",)
_RESERVED_KIND_NAMES = (TOP, GRAPH_KIND)


def _check_kind_name(kind: str, where: str) -> None:
    if kind in _RESERVED_KIND_NAMES or any(
        kind.startswith(p) for p in _RESERVED_KIND_PREFIXES
    ):
        _refuse(REFUSAL_UNKNOWN_FIELD, where,
                f"annotation kind {kind!r} collides with a reserved name of "
                "the kind vocabulary",
                reserved=[*_RESERVED_KIND_NAMES, *_RESERVED_KIND_PREFIXES])


def _require_mapping(raw: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        _refuse(REFUSAL_MISSING_FIELD, where,
                f"expected a mapping, got {type(raw).__name__}")
    return raw


def _take(
    raw: Mapping[str, Any], where: str, *,
    required: Sequence[str], optional: Sequence[str] = (),
) -> dict[str, Any]:
    unknown = sorted(set(raw) - set(required) - set(optional))
    if unknown:
        _refuse(REFUSAL_UNKNOWN_FIELD, where,
                f"unknown field(s) {unknown}", unknown=unknown)
    for key in required:
        if key not in raw:
            _refuse(REFUSAL_MISSING_FIELD, where,
                    f"missing required field {key!r}", missing=key)
    return dict(raw)


def _str_field(raw: Mapping[str, Any], key: str, where: str) -> str:
    value = raw[key]
    if not isinstance(value, str) or not value:
        _refuse(REFUSAL_MISSING_FIELD, f"{where}.{key}",
                "expected a non-empty string")
    return value


def _int_field(raw: Mapping[str, Any], key: str, where: str) -> int:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, int):
        _refuse(REFUSAL_MISSING_FIELD, f"{where}.{key}",
                "expected an integer (no floats, no strings)")
    return value


def _str_list(raw: Any, where: str) -> tuple[str, ...]:
    if not isinstance(raw, (list, tuple)):
        _refuse(REFUSAL_MISSING_FIELD, where, "expected a list of strings")
    out = []
    for i, v in enumerate(raw):
        if not isinstance(v, str) or not v:
            _refuse(REFUSAL_MISSING_FIELD, f"{where}[{i}]",
                    "expected a non-empty string")
        out.append(v)
    return tuple(out)


def _enum(value: str, allowed: Sequence[str], where: str) -> str:
    if value not in allowed:
        _refuse(REFUSAL_MISSING_FIELD, where,
                f"expected one of {list(allowed)}, got {value!r}")
    return value


# --------------------------------------------------------------------------- #
# The loader.                                                                  #
# --------------------------------------------------------------------------- #
def load_invariant_spec(
    raw: Mapping[str, Any],
    *,
    program_resolver: Callable[[str], TransformProgram],
    parser_registry: Any = None,
) -> LoadedSpec:
    """Load, check, materialize, and digest one specification. Fail-closed.

    ``program_resolver`` maps an ``ir://`` reference to its
    :class:`TransformProgram` (raising ``KeyError`` for an unknown ref —
    surfaced as ``dangling_reference``). ``parser_registry`` (when given)
    must expose ``has(ref) -> bool`` for ``parser://`` references
    (:class:`okg.substrate.consistency.violation_identity.WitnessParserRegistry`);
    a judged invariant naming a parser the registry lacks is a
    ``dangling_reference``."""
    top = _take(
        _require_mapping(raw, "$"), "$",
        required=("spec_version", "deployment", "defaults", "corpora",
                  "views", "invariants"),
    )
    spec_version = _int_field(top, "spec_version", "$")
    deployment = _str_field(top, "deployment", "$")

    defaults = _parse_defaults(_require_mapping(top["defaults"], "$.defaults"))
    corpora = tuple(
        _parse_corpus(_require_mapping(c, f"$.corpora[{i}]"),
                      f"$.corpora[{i}]", defaults)
        for i, c in enumerate(_as_list(top["corpora"], "$.corpora"))
    )
    _refuse_duplicate_ids([c.id for c in corpora], "$.corpora")
    views = tuple(
        _parse_view(_require_mapping(v, f"$.views[{i}]"), f"$.views[{i}]",
                    program_resolver)
        for i, v in enumerate(_as_list(top["views"], "$.views"))
    )
    _refuse_duplicate_ids([v.id for v in views], "$.views")
    invariants = tuple(
        _parse_invariant(_require_mapping(x, f"$.invariants[{i}]"),
                         f"$.invariants[{i}]", defaults, program_resolver)
        for i, x in enumerate(_as_list(top["invariants"], "$.invariants"))
    )
    _refuse_duplicate_ids([x.id for x in invariants], "$.invariants")

    # ---- structural cross-references (dangling_reference) ------------------ #
    corpus_ids = {c.id for c in corpora}
    view_by_id = {v.id: v for v in views}
    for inv in invariants:
        scope_corpus = _scope_corpus(inv.scope, f"invariant:{inv.id}")
        if scope_corpus not in corpus_ids:
            _refuse(REFUSAL_DANGLING_REFERENCE, f"invariant:{inv.id}",
                    f"scope names unknown corpus {scope_corpus!r}")
        if inv.tier == TIER_JUDGED and inv.context_view:
            ctx = view_by_id.get(inv.context_view)
            if ctx is None:
                _refuse(REFUSAL_DANGLING_REFERENCE, f"invariant:{inv.id}",
                        f"context_view names unknown view "
                        f"{inv.context_view!r}")
            if ctx.role != ROLE_JUDGED_CONTEXT:
                _refuse(REFUSAL_DANGLING_REFERENCE, f"invariant:{inv.id}",
                        f"context_view {inv.context_view!r} has role "
                        f"{ctx.role!r}, not {ROLE_JUDGED_CONTEXT!r}")
        if inv.tier == TIER_JUDGED and inv.witness_parser:
            if parser_registry is None or not parser_registry.has(
                inv.witness_parser
            ):
                _refuse(REFUSAL_DANGLING_REFERENCE, f"invariant:{inv.id}",
                        f"witness_parser {inv.witness_parser!r} is not "
                        "registered")
        if inv.tier == TIER_FORMAL and inv.witness_key and inv.program is not None:
            columns = _projection_columns(inv.program)
            if not columns:
                # fail-closed, not fail-open (review finding EWD-SVC-6): a
                # property query whose output columns cannot be derived
                # cannot validate ANY witness key.
                _refuse(REFUSAL_DANGLING_REFERENCE, f"invariant:{inv.id}",
                        "the property query's output columns are "
                        "underivable, so the declared witness_key cannot "
                        "be validated")
            missing = sorted(set(inv.witness_key) - columns)
            if missing:
                _refuse(REFUSAL_DANGLING_REFERENCE, f"invariant:{inv.id}",
                        f"witness_key column(s) {missing} are not output "
                        "columns of the property query",
                        columns=sorted(columns))
    for view in views:
        if view.role == ROLE_GENERATION and not view.renders:
            _refuse(REFUSAL_MISSING_FIELD, f"view:{view.id}",
                    "a generation view must declare renders")
        if view.role == ROLE_JUDGED_CONTEXT and view.renders:
            _refuse(REFUSAL_UNKNOWN_FIELD, f"view:{view.id}",
                    "a judged-context view renders nothing")
        if view.role == ROLE_JUDGED_CONTEXT and view.attaches:
            _refuse(REFUSAL_UNKNOWN_FIELD, f"view:{view.id}",
                    "attaches is a generation edge; a judged-context view "
                    "attaches nothing (review finding EWD-SVC-10)")
        if view.renders and not view.renders.startswith("doc://"):
            _refuse(REFUSAL_MISSING_FIELD, f"view:{view.id}",
                    "renders must be a doc:// location")
        if view.renders:
            corpus_id = view.renders[len("doc://"):].split("/", 1)[0]
            if corpus_id not in corpus_ids:
                _refuse(REFUSAL_DANGLING_REFERENCE, f"view:{view.id}",
                        f"renders names unknown corpus {corpus_id!r}")

    # ---- fixed_inputs coincidence claim (never adds reads) ----------------- #
    for view in views:
        if not view.fixed_inputs:
            continue
        kinds = read_kinds_of_program(view.program).kinds() if (
            view.program is not None
        ) else frozenset({TOP})
        for fixed in view.fixed_inputs:
            kind = _location_kind(fixed, f"view:{view.id}")
            if TOP not in kinds and kind not in kinds:
                _refuse(REFUSAL_FIXED_INPUT_OUTSIDE_READ_SET,
                        f"view:{view.id}",
                        f"fixed input {fixed!r} (kind {kind!r}) is outside "
                        "the view query's derived read set — a fixed input "
                        "never adds reads",
                        derived_kinds=sorted(kinds))

    # ---- check 2: derivation_cycle ---------------------------------------- #
    _check_derivation_cycle(corpora, views, invariants)

    # ---- check 3: classification (recorded; consumed by the lazy check) ---- #
    classification: dict[str, str] = {}
    runs_every_pass: list[str] = []
    for view in views:
        cls = classify_program(view.program)
        classification[f"view:{view.id}"] = cls
        if view.free_form or cls == CLASSIFICATION_WHOLE_GRAPH:
            runs_every_pass.append(f"view:{view.id}")
    for inv in invariants:
        if inv.tier == TIER_FORMAL:
            cls = classify_program(inv.program)
        elif inv.context_view:
            # a judged invariant reads through its context view: same
            # binding, same derivation (req.svc.views).
            cls = classification[f"view:{inv.context_view}"]
        else:
            cls = CLASSIFICATION_WHOLE_GRAPH
            runs_every_pass.append(f"invariant:{inv.id}")
        classification[f"invariant:{inv.id}"] = cls
        if (
            cls == CLASSIFICATION_WHOLE_GRAPH
            and f"invariant:{inv.id}" not in runs_every_pass
        ):
            runs_every_pass.append(f"invariant:{inv.id}")

    # ---- check 4: lazy_inadmissible --------------------------------------- #
    _check_lazy_admissibility(corpora, views, invariants, classification)

    # ---- check 5: strict_over_advisory (warning) --------------------------- #
    warnings: list[dict[str, Any]] = []
    corpus_by_id = {c.id: c for c in corpora}
    for inv in invariants:
        if inv.policy.mode != MODE_STRICT:
            continue
        scope_corpus = corpus_by_id[_scope_corpus(inv.scope, inv.id)]
        if scope_corpus.authority == AUTHORITY_ADVISORY:
            warnings.append({
                "code": WARNING_STRICT_OVER_ADVISORY,
                "where": f"invariant:{inv.id}",
                "detail": (
                    f"strict over advisory corpus {scope_corpus.id!r} hands "
                    "a publication veto to the foreign document owner "
                    "(design v8 §3.3)"
                ),
            })

    # ---- check 6: envelope_exceeds_authority ------------------------------- #
    for inv in invariants:
        for pattern in inv.repairs.envelope:
            _check_envelope_authority(pattern, inv, corpus_by_id)

    # ---- unbounded extractors (recorded, not refused) ---------------------- #
    unbounded: list[str] = []
    for corpus in corpora:
        for extractor in corpus.extractors:
            if extractor.output_envelope is None:
                unbounded.append(f"{corpus.id}/{extractor.id}")

    report = LoadReport(
        warnings=tuple(warnings),
        classification=dict(classification),
        runs_every_pass=tuple(sorted(set(runs_every_pass))),
        unbounded_extractors=tuple(sorted(unbounded)),
    )

    # ---- digests ----------------------------------------------------------- #
    digests: dict[str, str] = {
        "defaults": object_digest(defaults.to_canonical_dict(),
                                  where="defaults"),
    }
    for corpus in corpora:
        digests[f"corpus:{corpus.id}"] = object_digest(
            corpus.to_canonical_dict(), where=f"corpus:{corpus.id}")
    for view in views:
        digests[f"view:{view.id}"] = object_digest(
            view.to_canonical_dict(), where=f"view:{view.id}")
    for inv in invariants:
        payload = inv.to_canonical_dict()
        # the invariant digest transitively covers its judged context view's
        # digest and judge pins (slice doc §5): editing the context view
        # invalidates exactly the invariants that read through it.
        if inv.tier == TIER_JUDGED and inv.context_view:
            payload["context_view_digest"] = digests[
                f"view:{inv.context_view}"
            ]
        digests[f"invariant:{inv.id}"] = object_digest(
            payload, where=f"invariant:{inv.id}")
    spec_digest = object_digest(
        {
            "spec_version": spec_version,
            "deployment": deployment,
            "objects": dict(sorted(digests.items())),
        },
        where="spec",
    )

    return LoadedSpec(
        spec_version=spec_version,
        deployment=deployment,
        defaults=defaults,
        corpora=corpora,
        views=views,
        invariants=invariants,
        object_digests=digests,
        spec_digest=spec_digest,
        report=report,
    )


def _as_list(raw: Any, where: str) -> list[Any]:
    if not isinstance(raw, (list, tuple)):
        _refuse(REFUSAL_MISSING_FIELD, where, "expected a list")
    return list(raw)


def _refuse_duplicate_ids(ids: Sequence[str], where: str) -> None:
    seen: set[str] = set()
    for object_id in ids:
        if object_id in seen:
            _refuse(REFUSAL_DANGLING_REFERENCE, where,
                    f"duplicate object id {object_id!r}")
        seen.add(object_id)


def _parse_defaults(raw: Mapping[str, Any]) -> SpecDefaults:
    where = "$.defaults"
    fields = _take(raw, where, required=(
        "bounded_k", "bounded_time_bound_days", "eventual_slice_basis_points",
        "repair_attempts_r", "repair_created_subjects_per_pass",
        "corpus_authority", "lazy_escalation",
    ))
    bounded_k = _int_field(fields, "bounded_k", where)
    time_bound = _int_field(fields, "bounded_time_bound_days", where)
    if bounded_k == 0 and time_bound == 0:
        _refuse(REFUSAL_BOUNDED_CLOCKLESS, where,
                "bounded_k and bounded_time_bound_days may not both be 0 — "
                "a bounded policy with no clock is unbounded")
    return SpecDefaults(
        bounded_k=bounded_k,
        bounded_time_bound_days=time_bound,
        eventual_slice_basis_points=_int_field(
            fields, "eventual_slice_basis_points", where),
        repair_attempts_r=_int_field(fields, "repair_attempts_r", where),
        repair_created_subjects_per_pass=_int_field(
            fields, "repair_created_subjects_per_pass", where),
        corpus_authority=_enum(
            _str_field(fields, "corpus_authority", where), AUTHORITY_CLASSES,
            f"{where}.corpus_authority"),
        lazy_escalation=_enum(
            _str_field(fields, "lazy_escalation", where),
            (MODE_EVENTUAL, MODE_BOUNDED, MODE_STRICT),
            f"{where}.lazy_escalation"),
    )


def _parse_corpus(
    raw: Mapping[str, Any], where: str, defaults: SpecDefaults
) -> CorpusDecl:
    fields = _take(raw, where,
                   required=("id", "version", "kind", "digest_algorithm",
                             "extractors"),
                   optional=("authority",))
    extractors = []
    for i, e in enumerate(_as_list(fields["extractors"],
                                   f"{where}.extractors")):
        e_where = f"{where}.extractors[{i}]"
        e_fields = _take(_require_mapping(e, e_where), e_where,
                         required=("id", "version"),
                         optional=("output_envelope",))
        envelope = (
            _str_list(e_fields["output_envelope"],
                      f"{e_where}.output_envelope")
            if "output_envelope" in e_fields else None
        )
        for kind in (envelope or ()):
            _check_kind_name(kind, f"{e_where}.output_envelope")
        extractors.append(ExtractorDecl(
            id=_str_field(e_fields, "id", e_where),
            version=_int_field(e_fields, "version", e_where),
            output_envelope=envelope,
        ))
    # authority is explicit or the DEFAULT (advisory unless the defaults
    # block says otherwise); owned/authoritative are never inferred.
    authority = (
        _enum(_str_field(fields, "authority", where), AUTHORITY_CLASSES,
              f"{where}.authority")
        if "authority" in fields else defaults.corpus_authority
    )
    return CorpusDecl(
        id=_str_field(fields, "id", where),
        version=_int_field(fields, "version", where),
        kind=_enum(_str_field(fields, "kind", where),
                   ("documents", "graph-region"), f"{where}.kind"),
        authority=authority,
        digest_algorithm=_enum(
            _str_field(fields, "digest_algorithm", where), ("sha256",),
            f"{where}.digest_algorithm"),
        extractors=tuple(extractors),
    )


def _parse_view(
    raw: Mapping[str, Any], where: str,
    program_resolver: Callable[[str], TransformProgram],
) -> ViewDecl:
    fields = _take(raw, where,
                   required=("id", "version", "query", "role"),
                   optional=("query_digest", "renders", "attaches",
                             "fixed_inputs"))
    query = _str_field(fields, "query", where)
    role = _enum(_str_field(fields, "role", where),
                 (ROLE_GENERATION, ROLE_JUDGED_CONTEXT), f"{where}.role")
    program: TransformProgram | None = None
    query_digest = ""
    if query.startswith(FREE_FORM_PREFIX):
        # free-form generation: no program, no digest — and it must SAY it is
        # a generation (a free-form judged context would be unbounded reads
        # posing as a view).
        if role != ROLE_GENERATION:
            _refuse(REFUSAL_MISSING_FIELD, where,
                    "free-form is only expressible for generation views")
        if "query_digest" in fields:
            _refuse(REFUSAL_UNKNOWN_FIELD, where,
                    "a free-form generation has no query to digest")
    else:
        if "query_digest" not in fields:
            _refuse(REFUSAL_MISSING_FIELD, where,
                    "query_digest is authored into the digested spec "
                    "(slice doc §2.2) — it cannot be omitted")
        query_digest = _str_field(fields, "query_digest", where)
        program = _resolve_program(query, query_digest, where,
                                   program_resolver)
    return ViewDecl(
        id=_str_field(fields, "id", where),
        version=_int_field(fields, "version", where),
        query=query,
        query_digest=query_digest,
        role=role,
        renders=(
            _str_field(fields, "renders", where) if "renders" in fields
            else ""
        ),
        attaches=_checked_attaches(fields, where),
        fixed_inputs=(
            _str_list(fields["fixed_inputs"], f"{where}.fixed_inputs")
            if "fixed_inputs" in fields else ()
        ),
        program=program,
    )


def _checked_attaches(
    fields: Mapping[str, Any], where: str
) -> tuple[str, ...]:
    if "attaches" not in fields:
        return ()
    attaches = _str_list(fields["attaches"], f"{where}.attaches")
    for kind in attaches:
        _check_kind_name(kind, f"{where}.attaches")
    return attaches


def _resolve_program(
    query: str, query_digest: str, where: str,
    program_resolver: Callable[[str], TransformProgram],
) -> TransformProgram:
    try:
        program = program_resolver(query)
    except KeyError:
        _refuse(REFUSAL_DANGLING_REFERENCE, where,
                f"query {query!r} does not resolve to a program")
    actual = f"sha256:{program.checksum()}"
    if actual != query_digest:
        _refuse(REFUSAL_QUERY_DIGEST_MISMATCH, where,
                "the authored query_digest does not match the referenced "
                "program — an in-place program edit requires a spec edit "
                "(slice doc §2.2)",
                authored=query_digest, actual=actual)
    return program


def _parse_invariant(
    raw: Mapping[str, Any], where: str, defaults: SpecDefaults,
    program_resolver: Callable[[str], TransformProgram],
) -> InvariantDecl:
    fields = _take(raw, where,
                   required=("id", "version", "tier", "scope", "policy",
                             "property", "repairs"),
                   optional=("witness_key",))
    tier = _enum(_str_field(fields, "tier", where), (TIER_FORMAL, TIER_JUDGED),
                 f"{where}.tier")
    policy = _parse_policy(
        _require_mapping(fields["policy"], f"{where}.policy"),
        f"{where}.policy", defaults)
    repairs = _parse_repairs(
        _require_mapping(fields["repairs"], f"{where}.repairs"),
        f"{where}.repairs", defaults)
    prop = _require_mapping(fields["property"], f"{where}.property")
    witness_key = (
        _str_list(fields["witness_key"], f"{where}.witness_key")
        if "witness_key" in fields else ()
    )

    query = ""
    query_digest = ""
    text = ""
    context_view = ""
    judge: JudgeDecl | None = None
    witness_parser = ""
    program: TransformProgram | None = None
    if tier == TIER_FORMAL:
        p_fields = _take(prop, f"{where}.property",
                         required=("query", "query_digest"))
        query = _str_field(p_fields, "query", f"{where}.property")
        query_digest = _str_field(p_fields, "query_digest",
                                  f"{where}.property")
        program = _resolve_program(query, query_digest, f"{where}.property",
                                   program_resolver)
        if not witness_key:
            _refuse(REFUSAL_MISSING_FIELD, where,
                    "a formal invariant declares its witness-key columns")
    else:
        p_fields = _take(prop, f"{where}.property",
                         required=("text", "judge"),
                         optional=("context_view", "witness_parser"))
        text = _str_field(p_fields, "text", f"{where}.property")
        j_where = f"{where}.property.judge"
        j_fields = _take(_require_mapping(p_fields["judge"], j_where),
                         j_where,
                         required=("model", "prompt_template", "settings"))
        judge = JudgeDecl(
            model=_str_field(j_fields, "model", j_where),
            prompt_template=_str_field(j_fields, "prompt_template", j_where),
            settings=_str_field(j_fields, "settings", j_where),
        )
        if "context_view" in p_fields:
            context_view = _str_field(p_fields, "context_view",
                                      f"{where}.property")
        if "witness_parser" in p_fields:
            witness_parser = _str_field(p_fields, "witness_parser",
                                        f"{where}.property")

    return InvariantDecl(
        id=_str_field(fields, "id", where),
        version=_int_field(fields, "version", where),
        tier=tier,
        scope=_str_field(fields, "scope", where),
        policy=policy,
        witness_key=witness_key,
        repairs=repairs,
        query=query,
        query_digest=query_digest,
        text=text,
        context_view=context_view,
        judge=judge,
        witness_parser=witness_parser,
        program=program,
    )


def _parse_policy(
    raw: Mapping[str, Any], where: str, defaults: SpecDefaults
) -> PolicyDecl:
    fields = _take(raw, where, required=("mode",),
                   optional=("k", "time_bound_days", "escalation"))
    mode = _enum(_str_field(fields, "mode", where), MODES, f"{where}.mode")
    if mode != MODE_BOUNDED and ("k" in fields or "time_bound_days" in fields):
        _refuse(REFUSAL_UNKNOWN_FIELD, where,
                "k / time_bound_days are bounded-mode fields")
    if mode != MODE_LAZY and "escalation" in fields:
        _refuse(REFUSAL_UNKNOWN_FIELD, where,
                "escalation is a lazy-mode field")
    if mode == MODE_BOUNDED:
        # materialization fills ONLY omitted fields; an explicit 0 is the
        # author's value (disables that clock), never an absence.
        k = (_int_field(fields, "k", where) if "k" in fields
             else defaults.bounded_k)
        time_bound = (
            _int_field(fields, "time_bound_days", where)
            if "time_bound_days" in fields
            else defaults.bounded_time_bound_days
        )
        if k == 0 and time_bound == 0:
            _refuse(REFUSAL_BOUNDED_CLOCKLESS, where,
                    "k and time_bound_days may not both be 0 — a bounded "
                    "policy with no clock is unbounded")
        return PolicyDecl(mode=mode, k=k, time_bound_days=time_bound)
    if mode == MODE_LAZY:
        escalation = (
            _str_field(fields, "escalation", where)
            if "escalation" in fields else defaults.lazy_escalation
        )
        return PolicyDecl(mode=mode, escalation=_enum(
            escalation, (MODE_EVENTUAL, MODE_BOUNDED, MODE_STRICT),
            f"{where}.escalation"))
    return PolicyDecl(mode=mode)


def _parse_repairs(
    raw: Mapping[str, Any], where: str, defaults: SpecDefaults
) -> RepairsDecl:
    # the repairs block is REQUIRED even when empty (envelope: [] =
    # report-only) — its absence is a missing_field at the invariant.
    fields = _take(raw, where, required=("envelope",),
                   optional=("hints", "attempts_r"))
    envelope = _str_list(fields["envelope"], f"{where}.envelope")
    for i, pattern in enumerate(envelope):
        _location_kind(pattern, f"{where}.envelope[{i}]")  # grammar check
    return RepairsDecl(
        envelope=envelope,
        hints=(
            _str_list(fields["hints"], f"{where}.hints")
            if "hints" in fields else ()
        ),
        attempts_r=(
            _int_field(fields, "attempts_r", where)
            if "attempts_r" in fields else defaults.repair_attempts_r
        ),
    )


# --------------------------------------------------------------------------- #
# Locations: the pattern grammar shared by envelopes and fixed inputs.        #
# --------------------------------------------------------------------------- #
def _location_kind(pattern: str, where: str) -> str:
    """The KIND a location pattern writes/reads, for the kind-level graphs.

    ``annotation://<corpus>/<doc-pattern>/<kind>`` → the annotation kind;
    ``doc://<corpus>/...`` → the corpus's document kind (``doc:<corpus>``);
    ``graph://...`` → :data:`GRAPH_KIND`. Anything else refuses."""
    if pattern.startswith("annotation://"):
        parts = pattern[len("annotation://"):].split("/")
        if len(parts) < 3 or not parts[-1]:
            _refuse(REFUSAL_MISSING_FIELD, where,
                    f"annotation location {pattern!r} must be "
                    "annotation://<corpus>/<doc-pattern>/<kind>")
        return parts[-1]
    if pattern.startswith("doc://"):
        corpus = pattern[len("doc://"):].split("/", 1)[0]
        if not corpus:
            _refuse(REFUSAL_MISSING_FIELD, where,
                    f"doc location {pattern!r} names no corpus")
        return f"doc:{corpus}"
    if pattern.startswith("graph://"):
        return GRAPH_KIND
    _refuse(REFUSAL_MISSING_FIELD, where,
            f"unknown location scheme in {pattern!r} (annotation:// | "
            "doc:// | graph://)")
    raise AssertionError("unreachable")


def _location_corpus(pattern: str) -> str | None:
    if pattern.startswith("annotation://"):
        return pattern[len("annotation://"):].split("/", 1)[0]
    if pattern.startswith("doc://"):
        return pattern[len("doc://"):].split("/", 1)[0]
    return None


def _scope_corpus(scope: str, where: str) -> str:
    if not scope.startswith("corpus://"):
        _refuse(REFUSAL_MISSING_FIELD, where,
                f"scope {scope!r} must be corpus://<id> in this slice")
    return scope[len("corpus://"):]


def _check_envelope_authority(
    pattern: str, inv: InvariantDecl, corpus_by_id: Mapping[str, CorpusDecl]
) -> None:
    """§2.3: document-editing patterns require an OWNED corpus. graph://
    locations are always grantable (precedence-inverted repairs over
    authoritative sources land on the graph).

    Recorded decision (review finding EWD-SVC-5): in this slice,
    ``annotation://`` envelopes follow their CORPUS's authority class —
    annotation rewrites over advisory or authoritative corpora are refused
    even though extracted annotations are deployment-owned artifacts. This
    is deliberately the conservative reading: an annotation edit that
    diverges from its source document manufactures extraction-relative
    "consistency" without the document changing. Admitting deployment-owned
    annotation envelopes over foreign corpora would need its own reviewed
    semantics (what re-extraction does to a hand-edited annotation), which
    slice 2+ may propose; until then the narrow rule stands."""
    corpus_id = _location_corpus(pattern)
    if corpus_id is None:  # graph:// — always grantable
        return
    corpus = corpus_by_id.get(corpus_id)
    if corpus is None:
        _refuse(REFUSAL_DANGLING_REFERENCE, f"invariant:{inv.id}",
                f"repair envelope {pattern!r} names unknown corpus "
                f"{corpus_id!r}")
    if corpus.authority == AUTHORITY_ADVISORY:
        _refuse(REFUSAL_ENVELOPE_EXCEEDS_AUTHORITY, f"invariant:{inv.id}",
                f"envelope {pattern!r} edits documents in ADVISORY corpus "
                f"{corpus_id!r}; the engine may only propose there "
                "(design v8 §3.3)")
    if corpus.authority == AUTHORITY_AUTHORITATIVE:
        _refuse(REFUSAL_ENVELOPE_EXCEEDS_AUTHORITY, f"invariant:{inv.id}",
                f"envelope {pattern!r} edits AUTHORITATIVE documents in "
                f"{corpus_id!r}; repair the graph or derived documents "
                "instead (design v8 §3.3)")


# --------------------------------------------------------------------------- #
# Check 2: the kind-level derivation graph and its acyclicity.                 #
# --------------------------------------------------------------------------- #
def _derivation_edges(
    corpora: Sequence[CorpusDecl],
    views: Sequence[ViewDecl],
    invariants: Sequence[InvariantDecl],
) -> dict[str, set[str]]:
    """Nodes are kinds (``doc:<corpus>``, annotation kinds, ``graph``) and
    views (``view:<id>``). Edges follow the slice doc §4.2: extractor
    outputs, view reads, view render targets, and — the reviewed edge — a
    view's generation ``attaches``."""
    edges: dict[str, set[str]] = {}

    def edge(src: str, dst: str) -> None:
        edges.setdefault(src, set()).add(dst)

    for corpus in corpora:
        doc_kind = f"doc:{corpus.id}"
        for extractor in corpus.extractors:
            for kind in (extractor.output_envelope or ()):
                edge(doc_kind, kind)     # extractor: document -> annotation
    for view in views:
        node = f"view:{view.id}"
        kinds = (
            read_kinds_of_program(view.program).kinds()
            if view.program is not None else frozenset()
        )
        for kind in kinds:
            if kind != TOP:
                edge(kind, node)         # read: kind -> view
        if view.renders:
            corpus_id = view.renders[len("doc://"):].split("/", 1)[0]
            edge(node, f"doc:{corpus_id}")   # render: view -> document kind
        for kind in view.attaches:
            edge(node, kind)             # THE reviewed edge: view -> attaches
    # invariants are pure readers in this graph: they derive nothing, so
    # they add no edges — their read kinds matter to the lazy bound, not to
    # acyclicity.
    _ = invariants
    return edges


def _check_derivation_cycle(
    corpora: Sequence[CorpusDecl],
    views: Sequence[ViewDecl],
    invariants: Sequence[InvariantDecl],
) -> None:
    edges = _derivation_edges(corpora, views, invariants)
    # iterative DFS three-color cycle detection, deterministic order.
    WHITE, GREY, BLACK = 0, 1, 2
    color: dict[str, int] = {}
    for start in sorted(edges):
        if color.get(start, WHITE) != WHITE:
            continue
        stack: list[tuple[str, list[str]]] = [
            (start, sorted(edges.get(start, ())))
        ]
        color[start] = GREY
        path = [start]
        while stack:
            node, todo = stack[-1]
            if todo:
                nxt = todo.pop(0)
                state = color.get(nxt, WHITE)
                if state == GREY:
                    cycle = path[path.index(nxt):] + [nxt]
                    _refuse(REFUSAL_DERIVATION_CYCLE, "derivation-graph",
                            "the declared derivation graph has a cycle "
                            "(design v8 §8 T2's checked premise)",
                            cycle=cycle)
                if state == WHITE:
                    color[nxt] = GREY
                    stack.append((nxt, sorted(edges.get(nxt, ()))))
                    path.append(nxt)
            else:
                color[node] = BLACK
                stack.pop()
                path.pop()


def _forward_closure(
    seeds: frozenset[str], edges: Mapping[str, set[str]]
) -> frozenset[str]:
    out = set(seeds)
    frontier = list(seeds)
    while frontier:
        node = frontier.pop()
        for nxt in edges.get(node, ()):
            if nxt not in out:
                out.add(nxt)
                frontier.append(nxt)
    return out


# --------------------------------------------------------------------------- #
# Check 4: lazy admissibility (consumes the classification).                   #
# --------------------------------------------------------------------------- #
def _invariant_read_kinds(
    inv: InvariantDecl, views: Sequence[ViewDecl],
    classification: Mapping[str, str],
) -> frozenset[str]:
    if classification[f"invariant:{inv.id}"] == CLASSIFICATION_WHOLE_GRAPH:
        return frozenset({TOP})
    if inv.tier == TIER_FORMAL and inv.program is not None:
        return read_kinds_of_program(inv.program).kinds()
    if inv.context_view:
        for view in views:
            if view.id == inv.context_view and view.program is not None:
                # the judge reads the context view's rows AND the SUBJECT
                # documents themselves — the prompt includes the scoped
                # text, so the scope corpus's document kind is a read
                # (review finding EWD-SVC-3).
                return read_kinds_of_program(view.program).kinds() | {
                    f"doc:{_scope_corpus(inv.scope, inv.id)}"
                }
    return frozenset({TOP})


def _check_lazy_admissibility(
    corpora: Sequence[CorpusDecl],
    views: Sequence[ViewDecl],
    invariants: Sequence[InvariantDecl],
    classification: Mapping[str, str],
) -> None:
    strict_reads: set[str] = set()
    for inv in invariants:
        if inv.policy.mode != MODE_STRICT:
            continue
        kinds = _invariant_read_kinds(inv, views, classification)
        strict_reads |= set(kinds)
    edges = _derivation_edges(corpora, views, invariants)
    # UNKNOWABLE-bound sources (review findings EWD-SVC-1/-2): a view whose
    # read set cannot be enumerated (free-form, or an underivable program)
    # consumes ANY written kind, and a corpus with an UNBOUNDED extractor
    # can emit any kind from its documents. Design v8 §6: an unbounded or
    # undeclared envelope makes the intersection unknowable, so the action
    # is not deferral-eligible — refuse, never truncate the bound.
    unknown_reader_views = sorted(
        view.id for view in views
        if view.program is None or read_kinds_of_program(view.program).top
    )
    unbounded_doc_kinds = frozenset(
        f"doc:{corpus.id}"
        for corpus in corpora
        for extractor in corpus.extractors
        if extractor.output_envelope is None
    )
    for inv in invariants:
        if inv.policy.mode != MODE_LAZY:
            continue
        write_kinds = frozenset(
            _location_kind(p, f"invariant:{inv.id}")
            for p in inv.repairs.envelope
        )
        if not write_kinds:
            continue  # an empty envelope reveals nothing
        if strict_reads and unknown_reader_views:
            _refuse(REFUSAL_LAZY_INADMISSIBLE, f"invariant:{inv.id}",
                    "a view with an unenumerable read set consumes any "
                    "written kind, so the revelation bound is unknowable "
                    "while strict invariants exist (design v8 §6)",
                    unknown_reader_views=unknown_reader_views)
        bound = _forward_closure(write_kinds, edges)
        if strict_reads and (bound & unbounded_doc_kinds):
            _refuse(REFUSAL_LAZY_INADMISSIBLE, f"invariant:{inv.id}",
                    "the revelation bound reaches documents whose extractor "
                    "output envelope is UNBOUNDED — extraction may emit any "
                    "kind, so the bound is unknowable (design v8 §6)",
                    unbounded_docs=sorted(bound & unbounded_doc_kinds))
        if TOP in strict_reads:
            _refuse(REFUSAL_LAZY_INADMISSIBLE, f"invariant:{inv.id}",
                    "a strict invariant's read set is underivable (⊤), so "
                    "any nonempty revelation bound may reach it "
                    "(design v8 §2.2)",
                    revelation_bound=sorted(bound))
        overlap = sorted(bound & strict_reads)
        if overlap:
            _refuse(REFUSAL_LAZY_INADMISSIBLE, f"invariant:{inv.id}",
                    "the transitive revelation bound reaches a strict "
                    "invariant's read set (design v8 §2.2/§6)",
                    overlap=overlap)


# --------------------------------------------------------------------------- #
# Check 3 helper: witness-key column validation input.                         #
# --------------------------------------------------------------------------- #
def _projection_columns(program: TransformProgram) -> frozenset[str]:
    columns: set[str] = set()
    for block in program.blocks:
        for binding in block.bindings:
            op = binding.op
            if op.kind.value != "select":
                continue
            project = op.args.get("project") or {}
            if isinstance(project, Mapping):
                columns.update(str(k) for k in project)
    return frozenset(columns)


# --------------------------------------------------------------------------- #
# The generation-time fixed-input coincidence check (slice doc §2.2).          #
# --------------------------------------------------------------------------- #
REFUSAL_FIXED_INPUT_TRACE_MISMATCH = "fixed_input_trace_mismatch"


def check_fixed_inputs_coincide(
    view: ViewDecl, recorded_trace: Sequence[str]
) -> None:
    """Refuse when a view's declared fixed list and the recorded generation
    trace disagree. ``fixed_inputs`` is a COINCIDENCE CLAIM against the
    view's rows — it never adds reads (that is checked at load); here the
    claim is checked against what generation actually recorded. Order-free:
    the claim is about the set of inputs, not their visitation order."""
    if not view.fixed_inputs:
        return
    declared = frozenset(view.fixed_inputs)
    recorded = frozenset(str(t) for t in recorded_trace)
    if declared != recorded:
        _refuse(REFUSAL_FIXED_INPUT_TRACE_MISMATCH, f"view:{view.id}",
                "the declared fixed-input list and the recorded generation "
                "trace disagree — the coincidence claim failed",
                declared_only=sorted(declared - recorded),
                recorded_only=sorted(recorded - declared))


# --------------------------------------------------------------------------- #
# §5: what certificates may store (contract only).                             #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SpecCertificate:
    """The certificate SHAPE this slice owns; T1's formal objects and the
    scheduler slice both consume it. Not the pass — the contract.

    ``read_collections`` entries — ``(collection id, membership version)``
    pairs — are legal ONLY for evaluations classified
    ``instance_certifiable``: a join-defined enumeration has no maintainable
    membership version (design v8 §4), so the constructor refuses — the
    demotion is enforced by type, not by review. Invalidation keys on
    ``invariant_digest``; ``spec_digest`` is a DIAGNOSTIC — keying on it
    would kill every certificate on any unrelated spec edit."""

    invariant_id: str
    invariant_version: int
    subject: str
    classification: str
    read_facts: tuple[str, ...]
    read_collections: tuple[tuple[str, str], ...]
    result_digest: str
    pass_stamp: str
    invariant_digest: str
    spec_digest: str

    def __post_init__(self) -> None:
        if self.classification not in CLASSIFICATIONS:
            raise SpecLoadRefused(
                "SpecCertificate.classification must be a member of the "
                "closed classification vocabulary",
                data={"got": self.classification,
                      "known": list(CLASSIFICATIONS)},
            )
        if (
            self.read_collections
            and self.classification != CLASSIFICATION_INSTANCE
        ):
            raise SpecLoadRefused(
                "read_collections are legal only for instance_certifiable "
                "evaluations — a join-defined enumeration has no "
                "maintainable membership version (design v8 §4; the "
                "demotion is enforced by type)",
                data={"classification": self.classification,
                      "read_collections": [
                          list(rc) for rc in self.read_collections
                      ]},
            )
        for name in ("invariant_id", "subject", "result_digest",
                     "pass_stamp", "invariant_digest", "spec_digest"):
            if not getattr(self, name):
                raise SpecLoadRefused(
                    f"SpecCertificate.{name} must be non-empty",
                    data={"field": name},
                )

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "evaluation": {
                "invariant_id": self.invariant_id,
                "invariant_version": self.invariant_version,
                "subject": self.subject,
            },
            "classification": self.classification,
            "read_facts": list(self.read_facts),
            "read_collections": [list(rc) for rc in self.read_collections],
            "result_digest": self.result_digest,
            "pass_stamp": self.pass_stamp,
            "invariant_digest": self.invariant_digest,
            "spec_digest": self.spec_digest,
        }
