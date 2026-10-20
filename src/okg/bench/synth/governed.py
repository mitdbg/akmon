"""Drive a benchmark corpus through the SCHEDULER-GOVERNED publish path.

RUNNER side of the purity boundary (design §3): publishing IS the system under
test, so this module may import substrate. It is the BINDING PACT
``concord-complete-binding`` asks for, and nothing more — a vocabulary map from
what the benchmark produces onto what
:mod:`okg.substrate.consistency.reuse_scheduler` already consumes. It invents no
scheduling semantics: the decision ladder, the certificate mechanism, and the
commit gate are used exactly as they stand.

The two publish paths, side by side
-----------------------------------
The DIRECT path (:func:`okg.bench.synth.runner.ingest_and_publish`) extracts a
corpus in one call and publishes the fact list. Nothing decides what work to do;
everything runs because nothing is asked.

The GOVERNED path here reaches the same publish through the update protocol
(§9.6). The corpus is mapped onto the scheduler's step vocabulary:

* one **source step** per rendered document (``claim_source:<doc>``) — the unit
  whose input is one document;
* the **producer step** (``claim_producer``) — extraction, which assembles the
  publishable fact list out of the source steps' claims;
* the **effective merge** (``claim_effective``) and the **projection**
  (``claim_projection``, where the deployment's checks raise findings and the
  bucket resolutions are decided) — the two claim derivations, at their existing
  step ids.

A CHECK is deliberately not a step: in the scheduler's own vocabulary a check is
a READ SURFACE the possible-effects compiler maps envelopes onto, and a versioned
fact the certificate records. The deployment's real IR checks are passed through
at that existing kind — into ``compile_possible_effects`` and into each step's
``constraint_set`` dependency class — so a re-authored check invalidates every
certificate that stood on it.

The worklist is SEEDED with the document steps only. A RUN's observed dependents
extend it (documents → producer → effective merge → projection), so the reached
set is discovered by execution rather than declared, exactly as §9.6 specifies.
At the fixed point :func:`...reuse_pass_workflow.run_reuse_pass_in_cycle` applies
the commit gate and only then does the checkpoint writer publish — the publish is
DOWNSTREAM of the gate, so a pass that discards or aborts produces no generation.

What a benchmark corpus could not supply (and what the binding supplies instead)
-------------------------------------------------------------------------------
Three things the scheduler assumes, that a benchmark corpus does not carry. Each
is supplied HERE, in the binding, without touching the scheduler:

1. **Changed state terms.** ``skip_is_justifiable`` approves a skip when the
   change is disjoint from the step's reach — so an explicit
   ``ChangedStateTerms.nothing_changed()`` approves everything. On a FIRST
   publish that would skip every step behind a fresh certificate with no prior
   result to reuse: unsound by construction. (Saying NOTHING is a different
   thing since PR #972 — ``UNSTATED_CHANGE`` is a distinct type the ladder
   refuses — so the unsound case is a false assertion, not a silence.)
   A first publish changes every bucket the corpus writes (absent → present), so
   :func:`build_corpus_cone` states exactly that, and every step is refused a
   skip with cause ``effects_overlap`` and RUNS. The zero-skip property is thus a
   consequence of a truthful change statement, never of a configuration knob.

2. **A declared write envelope per step.** A rendered document has no producer
   manifest. For the DETERMINISTIC lane the envelope is derived by applying the
   extractor's own declared parse rule to the step's INPUT (the document) — a
   static analysis of the input, never of observed output (§9.3) — and the
   document's digest is recorded as that step's input dependency, so a document
   change invalidates the certificate that the derivation justified. For the LLM
   lane no such derivation is sound (a model may emit any key), so the envelope
   declares ``claim_keys`` unbounded and the step can never be certified
   skippable (§9.4).

3. **A generation id, before the generation exists.** The pass is labelled with
   the generation it maintains, but a publish mints its generation id at the END
   of the cycle. The binding labels the pass with a deterministic CANDIDATE id
   derived from the corpus + policy digests and records the
   candidate → published binding on the result. On a first publish this is
   invisible (zero skips ⇒ zero certificate rows).

Two more boundaries, stated rather than papered over: the envelope-violation
POISONING set is per PUBLISHED generation, so a pass that runs BEFORE the publish
has none to consult (:func:`governed_publish`); and a real publish cycle manages
its own connections, so the generation checkpoint is GATED BY the certificate
commit rather than written inside the same transaction (§9.6's same-transaction
shape holds for a single-statement checkpoint, which a publisher is not).

Replay
------
``concord-exact-parity`` replays a recorded history where extraction runs ONCE
per update. :func:`governed_publish` accepts ``recorded_facts``: the worklist
shape is unchanged (each document source step owns its recorded slice, via
:func:`partition_recorded_facts`) but nothing is re-extracted and the producer
publishes the recorded list VERBATIM — so a later parity verdict is attributable
to reuse and never to extraction drift.

Parity
------
:func:`resolved_state_rows` reads the four row families the PACT names —
accepted claims, findings, resolutions, winners — at one pinned generation, and
:func:`compare_resolved_states` compares two pins ROW-WISE, failing loudly on the
first differing row. A comparison over an empty state is itself a failure: the
non-empty guard refuses before comparing.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from okg.bench.synth.runner import (
    SMOKE_DEPLOYMENT,
    RunnerError,
    _canonicalize_claim_fact,
    apply_arm_stamps,
    apply_provenance_stamps,
    detection_state,
    effective_claims_at,
    publish_fact_list,
    resolutions_at,
)

# The claim-plane step ids come from the substrate module that owns them — the
# binding maps ONTO the existing vocabulary, it does not mint a parallel one.
from okg.substrate.runtime.reuse_pass_workflow import (
    CLAIM_EFFECTIVE_STEP,
    CLAIM_PRODUCER_STEP,
    CLAIM_PROJECTION_STEP,
    CLAIM_SOURCE_STEP,
)

GOVERNED_SCHEMA = "okg.synth.governed-publish.v1"

# The claim node subtype every benchmark claim is materialized as.
_CLAIM_SUBTYPE = "consistency_claim"

# The deterministic extractor's declared identity (recorded as the step's
# operator version, so a change to the extraction rule invalidates certificates).
DETERMINISTIC_EXTRACTOR = "claim_corpus_source@parse_document_claims.v1"
LLM_EXTRACTOR = "llm_extractor@nondeterministic"

# The step families the projection covers, declared so the report can say which
# benchmark artifact each scheduler step stands for.
STEP_ROLES: dict[str, str] = {
    CLAIM_SOURCE_STEP: "one rendered document (the source step's input)",
    CLAIM_PRODUCER_STEP: "extraction — assembles the publishable fact list",
    CLAIM_EFFECTIVE_STEP: "the effective-merge claim derivation",
    CLAIM_PROJECTION_STEP: "checks + bucket resolution (the projection derivation)",
}


class GovernedRunError(RunnerError):
    """The governed path refused: the pass did not commit, or the binding could
    not map a benchmark artifact onto the scheduler's vocabulary."""


class ParityError(RunnerError):
    """A row-wise parity comparison failed, or refused an empty comparison."""


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def document_step_id(doc: str) -> str:
    """The source step id for one rendered document."""
    return f"{CLAIM_SOURCE_STEP}:{doc}"


# --------------------------------------------------------------------------- #
# The cone: benchmark artifacts mapped onto the scheduler's vocabulary.        #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CorpusCone:
    """One corpus phase mapped onto the reached-step vocabulary (§9.6).

    ``worklist_seed`` is the document steps ONLY: the producer, the effective
    merge, and the projection are DISCOVERED as observed dependents when their
    predecessors run, so the reached set grows by execution. ``changed_terms``
    states what a first publish changes (every bucket the corpus writes), which
    is what forces every reached step to run."""

    phase: str
    deployment: str
    docs: tuple[str, ...]
    doc_digests: Mapping[str, str]
    doc_claim_keys: Mapping[str, tuple[str, ...]]
    predicates: tuple[str, ...]
    buckets: frozenset[tuple[str, str]]
    steps: Mapping[str, Any]                    # step_id -> ReuseStep
    worklist_seed: tuple[str, ...]
    current_facts: Mapping[str, Any]            # step_id -> CertificateDependencies
    changed_terms: Mapping[str, Any]            # step_id -> ChangedStateTerms
    incumbent_selection: Mapping[str, Mapping[str, str]]
    corpus_digest: str
    candidate_generation_id: str
    dependents: Mapping[str, tuple[str, ...]]

    def reached_step_count(self) -> int:
        """Every step the worklist can reach: the seed plus its dependents."""
        return len(self.steps)


def _document_texts(corpus_dir: Path) -> dict[str, str]:
    root = Path(corpus_dir).resolve()
    if not root.is_dir():
        raise GovernedRunError(f"governed path: corpus {root} is not a directory")
    docs = sorted(p.relative_to(root).as_posix() for p in root.rglob("*.md"))
    if not docs:
        raise GovernedRunError(
            f"governed path: corpus {root} contains no .md documents — there is "
            "no source step to seed the worklist with"
        )
    return {doc: (root / doc).read_text(encoding="utf-8") for doc in docs}


def _document_claim_facts(doc: str, text: str) -> list[Any]:
    """This document's claim facts under the DECLARED extraction rule.

    The same ``parse_document_claims`` rule + the same unit canonicalization the
    direct path's ``CanonicalizingClaimSource`` applies, so the per-document
    composition of these lists is byte-identical to the direct path's one-shot
    extraction (the property :func:`assert_fact_parity` witnesses)."""
    from okg.bench.injection.claim_corpus_source import (
        parse_document_claims,
    )

    digest = _sha256(text)
    return [
        _canonicalize_claim_fact(fact)
        for fact in parse_document_claims(doc, text, digest=digest)
    ]


def partition_recorded_facts(
    facts: Sequence[Any], docs: Sequence[str]
) -> tuple[dict[str, list[Any]], list[Any]]:
    """Split a RECORDED fact set into per-document claims + everything else.

    The replay seam ``concord-exact-parity`` needs: a history step records ONE
    fact set (``history_replay``'s ``recorded-facts/step_NN.json``) and every
    executor replays it verbatim, so the governed path must not re-extract. The
    document source steps still do real work — each one owns the recorded claims
    that came from its document, keyed by the fact's own recorded path — while
    the producer step publishes the recorded list unchanged.

    Returns ``(claims_by_doc, other)``. Raises when a recorded claim names a
    document the cone does not carry: a claim with nowhere to belong would make
    a per-document skip silently drop it."""
    known = set(docs)
    by_doc: dict[str, list[Any]] = {doc: [] for doc in docs}
    other: list[Any] = []
    orphans: list[str] = []
    for fact in facts:
        if getattr(fact, "subtype", "") != _CLAIM_SUBTYPE:
            other.append(fact)
            continue
        path = str(
            dict(getattr(fact, "source_record_id", {}) or {}).get("path")
            or dict(getattr(fact, "attrs", {}) or {}).get("source_id")
            or ""
        )
        if path not in known:
            orphans.append(f"{fact.node_id} (path={path!r})")
            continue
        by_doc[path].append(fact)
    if orphans:
        raise GovernedRunError(
            "governed path: recorded claims name documents the cone does not "
            f"carry — a claim with no source step would be dropped: {orphans[:5]}"
        )
    return by_doc, other


def _policy_facts(policies: Sequence[str]) -> list[Any]:
    """The functional predicate policies, in the extractor's own order.

    ``ClaimCorpusSource`` emits these BEFORE any document's claims; the binding
    mints the identical rows so the assembled fact list matches the direct path
    element for element."""
    from okg.substrate.library.sources.base import NodeFact

    return [
        NodeFact(
            f"policy:functional:{predicate}",
            "predicate_policy",
            {
                "enabled": "true",
                "policy_id": f"functional_{predicate}",
                "policy_kind": "functional",
                "predicate": predicate,
            },
            {"id": f"policy:functional:{predicate}"},
            {"v": "1"},
        )
        for predicate in sorted(policies)
    ]


def source_vocabularies(facts: Sequence[Any]) -> dict[str, list[str]]:
    """The closed ``source_class`` / ``source_authority`` vocabularies a document
    step's permitted outputs carry, derived from the step's INPUT.

    The bounded-dominance rung (§9.5) reads these off the declared envelope
    (:func:`...reuse_scheduler.derive_competitor_bound`): the source-authority
    vocabulary's MAX is the competitor-best ceiling, and an envelope that
    declares none refuses dominance outright. Derived the same way the claim
    keys are — by applying the extraction rule to this document — so it is a
    static analysis of the input, never of observed output (§9.3).

    A claim carrying no ``source_authority`` attribute is ranked ZERO by the
    projection itself (``claim_projection_derivation.py``:
    ``COALESCE((attrs->>'source_authority')::bigint, 0)``), so ``"0"`` is that
    claim's DECLARED authority under the engine's own reading — not a default
    substituted for a value the analysis failed to find. A non-integer authority
    is a loud refusal: the vocabulary is omitted entirely, so dominance cannot be
    claimed over a step whose authority the analysis could not read."""
    classes = sorted(
        {str(f.attrs["source_class"]) for f in facts if f.attrs.get("source_class")}
    )
    authorities: set[int] = set()
    for fact in facts:
        raw = fact.attrs.get("source_authority")
        if raw is None:
            authorities.add(0)          # the projection's own COALESCE reading
            continue
        try:
            authorities.add(int(raw))
        except (TypeError, ValueError):
            return {}                   # unreadable -> declare nothing, refuse
    declared: dict[str, list[str]] = {}
    if classes:
        declared["source_class"] = classes
    if authorities:
        declared["source_authority"] = [str(a) for a in sorted(authorities)]
    return declared


def _envelope_for_document(
    *, deployment: str, doc: str, claim_keys: Sequence[str], nondeterministic: bool,
    vocabularies: Mapping[str, Sequence[str]] | None = None,
) -> Any:
    """The source step's declared write surface.

    DETERMINISTIC lane: the claim keys are derived by applying the extractor's
    declared parse rule to the step's INPUT (this document). That is a static
    analysis of the input, not of observed output (§9.3) — and the document's
    digest is the step's recorded input dependency, so a document edit
    invalidates any certificate the derivation justified.

    LLM lane: no key derivation is sound (a model may emit any key), so
    ``claim_keys`` is declared UNBOUNDED and the step can never be certified
    skippable (§9.4 — the compiler returns ``RunAlways``).

    ``vocabularies`` (opt-in, :func:`source_vocabularies`) declares the closed
    value vocabularies the bounded-dominance rung reads. They bound the VALUE
    axis only — ``_derive_bucket_reach`` ignores them — so declaring them never
    narrows the step's bucket reach, it only makes a dominance argument
    expressible."""
    from okg.substrate.producer_authority import normalize_write_envelope

    declared: dict[str, Any] = {
        "nodes": [
            {"subtype": _CLAIM_SUBTYPE, "clause_id": f"{deployment}:{doc}"}
        ],
        "nondeterministic": nondeterministic,
    }
    if nondeterministic:
        declared["unbounded"] = ["claim_keys"]
    else:
        declared["claim_keys"] = list(dict.fromkeys(claim_keys))
    if vocabularies:
        declared["value_vocabularies"] = {
            k: list(v) for k, v in sorted(vocabularies.items())
        }
    return normalize_write_envelope(declared, source_name=document_step_id(doc))


def _envelope_for_derivation(
    *, deployment: str, step_id: str, nondeterministic: bool
) -> Any:
    """A whole-corpus step's declared write surface.

    The producer, the effective merge, and the projection write claims under the
    deployment's predicates but over a key space no declaration enumerates — so
    the envelope declares the claim plane WITHOUT a key list and the compiler's
    bucket reach becomes the predicate-wildcard bound (every key under the
    declared predicates). Bounded, so the ladder still reaches the effects rung;
    conservative, so nothing is omitted (§9.4)."""
    from okg.substrate.producer_authority import normalize_write_envelope

    return normalize_write_envelope(
        {
            "nodes": [
                {"subtype": _CLAIM_SUBTYPE, "clause_id": f"{deployment}:{step_id}"}
            ],
            "nondeterministic": nondeterministic,
        },
        source_name=step_id,
    )


def _envelope_fingerprint(envelope: Any) -> str:
    return _sha256(_canonical(envelope.summary()))


def _constraint_set(checks: Sequence[Any]) -> dict[str, str]:
    """check id -> the IR program's content checksum.

    The deployment's own check identity, so a re-authored check invalidates every
    certificate that stood on it."""
    return {str(spec.check_id): str(spec.program.checksum()) for spec in checks}


def _changed_terms_for(buckets, *, what: str) -> Any:
    """The change statement for a step whose changed inputs resolved to
    ``buckets`` — which may be EMPTY.

    concord-measurement-integrity, req.mi.statable-update. An empty bucket set
    is NOT "nothing changed". The input DID change; the analysis simply could
    not place the change on any check or bucket — which is exactly what
    ``unknown`` spells, and it forces a refusal rather than licensing a skip.

    On structured corpora every changed document renders labelled facts and this
    branch never fires. On prose a document can contribute no checkable claim at
    all (a model extracted nothing that maps to a bucket), and the previous code
    built a bare ``ChangedStateTerms(buckets=frozenset())`` — an all-empty
    statement the engine REFUSES as ambiguous between "nothing changed"
    (permissive, licences a skip) and "not stated" (restrictive). The engine's
    refusal is correct and is untouched; this is the caller learning to say
    which one it means. Guessing the permissive reading here would licence an
    unsound skip over a document whose content nobody looked at.
    """
    from okg.substrate.consistency.possible_effects import ChangedStateTerms

    placed = frozenset(buckets)
    if placed:
        return ChangedStateTerms(buckets=placed)
    return ChangedStateTerms(
        unknown=True,
        unknown_reason=(
            f"{what} changed but its extraction resolved to no claim bucket; "
            "the change could not be placed, so no skip is licensed over it"
        ),
    )


def build_corpus_cone(
    corpus_dir: Path,
    *,
    deployment: str,
    deployment_dir: Path,
    policies: Sequence[str],
    phase: str,
    checks: Sequence[Any] = (),
    extra_policy_facts: Sequence[Any] = (),
    arm_plan: Mapping[str, Any] | None = None,
    provenance_plan: Mapping[str, Any] | None = None,
    nondeterministic: bool = False,
    recorded_facts: Sequence[Any] | None = None,
    declare_source_vocabularies: bool = False,
) -> CorpusCone:
    """Map one rendered corpus onto the scheduler's reached-step vocabulary.

    Returns the cone the pass consumes: the steps and their declared envelopes,
    the worklist seed (the document steps), the per-step dependency-fact
    snapshots the certificate mechanism records, and the CHANGED STATE TERMS a
    first publish carries (every bucket the corpus writes goes absent → present).
    Pure with respect to the database — it reads the corpus and the deployment's
    declarations only.

    ``recorded_facts`` switches the cone to REPLAY: the per-document buckets and
    claim keys come from the recorded fact set (partitioned by each fact's own
    recorded path) rather than from re-parsing the corpus, and the recorded set's
    digest joins the whole-corpus steps' input dependencies — so a certificate
    written under replay pins the exact fact set that was replayed.

    ``declare_source_vocabularies`` additionally declares each document step's
    closed source-class / source-authority vocabularies (:func:`source_vocabularies`),
    which is what makes the bounded-dominance rung expressible for a document
    step. Off by default: it moves every document envelope's fingerprint, so a
    caller opts in for a whole run rather than per step."""
    from okg.substrate.catalog.manifest_identity import manifest_fingerprint_for
    from okg.substrate.consistency.reuse_certificates import CertificateDependencies
    from okg.substrate.consistency.reuse_scheduler import ReuseStep

    texts = _document_texts(corpus_dir)
    docs = tuple(sorted(texts))
    doc_digests = {doc: _sha256(text) for doc, text in texts.items()}

    doc_claim_keys: dict[str, tuple[str, ...]] = {}
    doc_buckets: dict[str, frozenset[tuple[str, str]]] = {}
    doc_vocabularies: dict[str, dict[str, list[str]]] = {}
    predicates: set[str] = set(policies)
    buckets: set[tuple[str, str]] = set()
    replay_claims: dict[str, list[Any]] | None = None
    if recorded_facts is not None:
        replay_claims, _other = partition_recorded_facts(recorded_facts, docs)
    for doc in docs:
        facts = (
            replay_claims[doc] if replay_claims is not None
            else _document_claim_facts(doc, texts[doc])
        )
        pairs = {
            (str(f.attrs["predicate"]), str(f.attrs["normalized_key"]))
            for f in facts
        }
        doc_claim_keys[doc] = tuple(sorted({key for _p, key in pairs}))
        doc_buckets[doc] = frozenset(pairs)
        doc_vocabularies[doc] = (
            source_vocabularies(facts) if declare_source_vocabularies else {}
        )
        predicates.update(p for p, _k in pairs)
        buckets.update(pairs)
    if not buckets:
        raise GovernedRunError(
            f"governed path: corpus {corpus_dir} yielded no claim buckets — a "
            "cone with nothing to change cannot state what a first publish "
            "changes"
        )

    manifest_fp, _blocks = manifest_fingerprint_for(deployment_dir)
    catalog_version_id = manifest_fp or f"{deployment}:unfingerprinted-manifest"
    policy_fp = _sha256(
        _canonical(
            {
                "functional": sorted(policies),
                "extra": sorted(
                    (f.node_id, dict(f.attrs)) for f in extra_policy_facts
                ),
            }
        )
    )
    precedence_fp = _sha256(
        _canonical(
            {
                "arm_plan": arm_plan or {},
                "provenance_plan": provenance_plan or {},
            }
        )
    )
    constraint_set = _constraint_set(checks)
    corpus_digest = _sha256(_canonical([[d, doc_digests[d]] for d in docs]))
    operator = LLM_EXTRACTOR if nondeterministic else DETERMINISTIC_EXTRACTOR

    steps: dict[str, Any] = {}
    current_facts: dict[str, Any] = {}
    changed_terms: dict[str, Any] = {}
    incumbent_selection: dict[str, dict[str, str]] = {}

    def _facts_for(step_id: str, envelope: Any, inputs: Mapping[str, str]) -> Any:
        return CertificateDependencies(
            input_digests=dict(inputs),
            operator_versions={step_id: operator},
            # The deterministic lane makes no model call: a MEASURED empty, not
            # an uninstrumented gap. The LLM lane's outcome keys would ride here.
            model_prompt_keys={},
            envelope_fingerprint=_envelope_fingerprint(envelope),
            constraint_set=dict(constraint_set),
            generator_versions={},
            policy_fingerprint=policy_fp,
            precedence_fingerprint=precedence_fp,
            competitor_bounds={},
            catalog_version_id=str(catalog_version_id),
        )

    # (1) documents -> source steps. Each one's input is its own document.
    for doc in docs:
        step_id = document_step_id(doc)
        envelope = _envelope_for_document(
            deployment=deployment, doc=doc, claim_keys=doc_claim_keys[doc],
            nondeterministic=nondeterministic,
            vocabularies=doc_vocabularies[doc],
        )
        steps[step_id] = ReuseStep(
            step_id=step_id,
            envelope=envelope,
            producer_id=CLAIM_SOURCE_STEP,
            claim_predicates=tuple(sorted({p for p, _k in doc_buckets[doc]})),
        )
        current_facts[step_id] = _facts_for(
            step_id, envelope, {doc: doc_digests[doc]}
        )
        # A first publish plants every bucket this document writes.
        changed_terms[step_id] = _changed_terms_for(
            doc_buckets[doc], what=f"document {doc!r}"
        )
        incumbent_selection[step_id] = {
            "outcome": "first_publish", "basis": "no_prior_result",
        }

    # (2) extraction + the two claim derivations -> the whole-corpus steps.
    corpus_inputs = dict(doc_digests)
    if recorded_facts is not None:
        # Under replay the producer's input is the RECORDED set, not the corpus:
        # pin it so a certificate can never stand on a different fact set.
        corpus_inputs["recorded_facts"] = _sha256(
            _canonical(
                [
                    [f.node_id, f.subtype, dict(f.attrs), f.op]
                    for f in recorded_facts
                ]
            )
        )
    for step_id in (CLAIM_PRODUCER_STEP, CLAIM_EFFECTIVE_STEP, CLAIM_PROJECTION_STEP):
        envelope = _envelope_for_derivation(
            deployment=deployment, step_id=step_id,
            nondeterministic=nondeterministic,
        )
        steps[step_id] = ReuseStep(
            step_id=step_id,
            envelope=envelope,
            producer_id=step_id,
            claim_predicates=tuple(sorted(predicates)),
        )
        current_facts[step_id] = _facts_for(step_id, envelope, corpus_inputs)
        changed_terms[step_id] = _changed_terms_for(
            buckets, what=f"derivation {step_id!r}"
        )
        incumbent_selection[step_id] = {
            "outcome": "first_publish", "basis": "no_prior_result",
        }

    dependents = {
        **{document_step_id(doc): (CLAIM_PRODUCER_STEP,) for doc in docs},
        CLAIM_PRODUCER_STEP: (CLAIM_EFFECTIVE_STEP,),
        CLAIM_EFFECTIVE_STEP: (CLAIM_PROJECTION_STEP,),
        CLAIM_PROJECTION_STEP: (),
    }
    candidate = f"{deployment}:{phase}:{corpus_digest[:16]}"
    return CorpusCone(
        phase=phase,
        deployment=deployment,
        docs=docs,
        doc_digests=doc_digests,
        doc_claim_keys=doc_claim_keys,
        predicates=tuple(sorted(predicates)),
        buckets=frozenset(buckets),
        steps=steps,
        worklist_seed=tuple(document_step_id(doc) for doc in docs),
        current_facts=current_facts,
        changed_terms=changed_terms,
        incumbent_selection=incumbent_selection,
        corpus_digest=corpus_digest,
        candidate_generation_id=candidate,
        dependents=dependents,
    )


# --------------------------------------------------------------------------- #
# The governed publish.                                                        #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GovernedPublish:
    """One corpus phase published through the scheduler-governed path."""

    phase: str
    deployment: str
    candidate_generation_id: str
    generation_id: str
    compat_generation_id: str
    publish: Mapping[str, Any]
    claim_ids: frozenset[str]
    facts: tuple[Any, ...]
    reuse_summary: Mapping[str, Any]
    executed: tuple[str, ...]
    reached: tuple[str, ...]
    worklist_seed: tuple[str, ...]

    @property
    def result(self) -> Any:
        return self.reuse_summary["result"]

    def accounting(self) -> dict[str, Any]:
        """The run/skip accounting the zero-skip acceptance is read from."""
        result = self.result
        return {
            "terminal": result.terminal,
            "reached": len(result.outcomes),
            "runs": len(result.runs),
            "skips": len(result.skips),
            "run_causes": sorted({o.cause for o in result.runs}),
            "skip_causes": sorted({o.cause for o in result.skips}),
            "certificates_committed": self.reuse_summary.get(
                "certificates_committed"
            ),
            "pass_digest": self.reuse_summary.get("pass_digest"),
        }


def governed_publish(
    dsn: str,
    corpus_dir: Path,
    deployment_dir: Path,
    policies: Sequence[str],
    previous_ids: frozenset[str] = frozenset(),
    *,
    phase: str,
    deployment: str = SMOKE_DEPLOYMENT,
    checks: Sequence[Any] = (),
    extra_policy_facts: Sequence[Any] = (),
    arm_plan: Mapping[str, Any] | None = None,
    admits: Sequence[str] = (),
    provenance_plan: Mapping[str, Any] | None = None,
    nondeterministic: bool = False,
    recorded_facts: Sequence[Any] | None = None,
    cone: CorpusCone | None = None,
) -> GovernedPublish:
    """Publish one rendered corpus THROUGH the scheduler-governed path (§9.6).

    The reached-step worklist is seeded with the document steps and processes to
    a fixed point; every RUN executes the real benchmark work for its step and
    returns its observed dependents, which extend the worklist. At the fixed
    point the commit gate authorizes the checkpoint, and only then does the
    checkpoint writer run the REAL publish cycle — so a pass that discards or
    aborts produces no generation at all.

    Reuse is DISABLED here (complete mode): no prior certificates and no prior
    dependency snapshot are supplied, and the changed-state terms state what a
    first publish changes, so the ladder refuses every skip and runs every
    reached step.

    ``recorded_facts`` switches the run to REPLAY, the mode
    ``concord-exact-parity`` needs: the worklist shape is unchanged (each
    document source step owns the recorded claims that came from its document),
    but nothing is re-extracted and the producer publishes the recorded list
    VERBATIM. Without this, two executors replaying one update would extract
    separately and any later parity verdict could be provider drift rather than a
    maintenance defect.

    Boundary, stated so it is not mistaken for more: the envelope-violation
    POISONING set (``poisoned_producers``) is per PUBLISHED generation, and this
    pass runs BEFORE the generation exists — so a pre-publish pass has no
    poisoning row to consult and the set is empty here. Wiring it needs the prior
    generation's violations, which is the consumer's (``concord-exact-parity``)
    scope, not this binding's."""
    import psycopg

    from okg.substrate.consistency.reuse_certificates import ReuseCertificateStore
    from okg.substrate.runtime.reuse_pass_workflow import run_reuse_pass_in_cycle

    if deployment != SMOKE_DEPLOYMENT:
        raise GovernedRunError(
            f"governed path: publish_fact_list applies facts under "
            f"{SMOKE_DEPLOYMENT!r}, so a governed publish for {deployment!r} "
            "would label its certificates for one deployment and its facts for "
            "another. Bind the benchmark deployment, or lift the deployment name "
            "out of publish_fact_list first."
        )
    if cone is None:
        cone = build_corpus_cone(
            corpus_dir,
            deployment=deployment,
            deployment_dir=deployment_dir,
            policies=policies,
            phase=phase,
            checks=checks,
            extra_policy_facts=extra_policy_facts,
            arm_plan=arm_plan,
            provenance_plan=provenance_plan,
            nondeterministic=nondeterministic,
            recorded_facts=recorded_facts,
        )

    texts = _document_texts(corpus_dir)
    replay_claims: dict[str, list[Any]] | None = None
    if recorded_facts is not None:
        replay_claims, _other = partition_recorded_facts(
            recorded_facts, cone.docs
        )
    extracted: dict[str, list[Any]] = {}
    assembled: list[Any] = []
    executed: list[str] = []

    def _step_runner(step_id: str) -> Sequence[str]:
        """Execute one reached step for real; return its observed dependents."""
        executed.append(step_id)
        if step_id.startswith(f"{CLAIM_SOURCE_STEP}:"):
            doc = step_id[len(CLAIM_SOURCE_STEP) + 1:]
            extracted[doc] = (
                list(replay_claims[doc]) if replay_claims is not None
                else _document_claim_facts(doc, texts[doc])
            )
            return (CLAIM_PRODUCER_STEP,)
        if step_id == CLAIM_PRODUCER_STEP:
            missing = [d for d in cone.docs if d not in extracted]
            if missing:
                raise GovernedRunError(
                    "governed path: the producer step reached before every "
                    f"document source step ran (missing {missing})"
                )
            assembled.clear()
            if recorded_facts is not None:
                # REPLAY: publish the recorded list VERBATIM. The per-document
                # steps partitioned it (so a later per-document skip has a slice
                # to carry forward), but the bytes that publish are the recorded
                # ones — extraction never runs twice on one update.
                assembled.extend(list(recorded_facts))
                return (CLAIM_EFFECTIVE_STEP,)
            facts: list[Any] = _policy_facts(policies)
            for doc in cone.docs:
                facts.extend(extracted[doc])
            facts = apply_provenance_stamps(facts, provenance_plan)
            facts = apply_arm_stamps(
                facts, arm_plan, admits,
                provenance_admitted=bool((provenance_plan or {}).get("enabled")),
            )
            assembled.extend(list(facts) + list(extra_policy_facts))
            return (CLAIM_EFFECTIVE_STEP,)
        if step_id == CLAIM_EFFECTIVE_STEP:
            return (CLAIM_PROJECTION_STEP,)
        if step_id == CLAIM_PROJECTION_STEP:
            return ()
        raise GovernedRunError(
            f"governed path: reached step {step_id!r} has no mapped benchmark "
            "work — the vocabulary map is incomplete"
        )

    published: dict[str, Any] = {}

    def _checkpoint_writer(_conn: Any) -> str:
        """The REAL publish, downstream of the commit gate.

        Reached only after :func:`...reuse_scheduler.commit_gate` accepted the
        pass, so the fixed point is what authorizes a generation to exist. The
        publish cycle manages its own connections (a publisher is not a single
        statement on the caller's transaction), so the generation checkpoint is
        GATED BY the certificate commit rather than atomic with it — the one
        place a real publish cannot honor the §9.6 same-transaction shape."""
        if not assembled:
            raise GovernedRunError(
                "governed path: the pass reached a fixed point with no assembled "
                "fact list — the producer step never ran"
            )
        out, ids = publish_fact_list(
            dsn, Path(deployment_dir), list(assembled), previous_ids
        )
        published["out"] = out
        published["ids"] = ids
        return str(out["generation_id"])

    store = ReuseCertificateStore(deployment=deployment)
    conn = psycopg.connect(dsn)  # autocommit=False -> one transaction
    try:
        summary = run_reuse_pass_in_cycle(
            conn,
            deployment=deployment,
            generation_id=cone.candidate_generation_id,
            worklist_seed=cone.worklist_seed,
            steps=cone.steps,
            checks=checks,
            current_facts=cone.current_facts,
            changed_terms=cone.changed_terms,
            incumbent_selection=cone.incumbent_selection,
            step_runner=_step_runner,
            cert_store=store,
            checkpoint_writer=_checkpoint_writer,
        )
    finally:
        conn.close()

    if summary.get("reuse_pass") != "committed":
        raise GovernedRunError(
            "governed path: the reuse pass did not commit — "
            f"terminal={summary.get('terminal')!r} "
            f"cause={summary.get('fallback_cause')!r} "
            f"failed_step={summary.get('failed_step')!r}. No generation was "
            "published (the commit gate is upstream of the publish)."
        )
    out = published["out"]
    return GovernedPublish(
        phase=phase,
        deployment=deployment,
        candidate_generation_id=cone.candidate_generation_id,
        generation_id=str(out["generation_id"]),
        compat_generation_id=str(out["compat_generation_id"]),
        publish=out,
        claim_ids=published["ids"],
        facts=tuple(assembled),
        reuse_summary=summary,
        executed=tuple(executed),
        reached=tuple(sorted(o.step_id for o in summary["result"].outcomes)),
        worklist_seed=cone.worklist_seed,
    )


def assert_zero_skips(
    publish: GovernedPublish, *, expected_reached: int | None = None
) -> dict[str, Any]:
    """A FIRST publish must run every reached step — zero skips (§9.6).

    A first publish has no prior result, so a skip would keep nothing: it is
    unsound by construction. Read from the run/skip accounting, and the run
    CAUSE is checked too — every run must be an ``effects_overlap`` refusal (the
    change genuinely reaches the step), never ``run_always``, which would make
    the zero-skip reading vacuous (an unbounded envelope can never be skipped, so
    it proves nothing about the ladder)."""
    from okg.substrate.consistency.reuse_scheduler import (
        CAUSE_EFFECTS_OVERLAP,
        TERMINAL_FIXED_POINT,
    )

    account = publish.accounting()
    if account["terminal"] != TERMINAL_FIXED_POINT:
        raise GovernedRunError(
            f"first publish did not reach a fixed point: {account}"
        )
    if account["skips"]:
        raise GovernedRunError(
            "first publish SKIPPED a reached step — a first publish has no "
            f"prior result, so every skip is unsound: {account}"
        )
    if not account["runs"]:
        raise GovernedRunError(f"first publish ran nothing: {account}")
    if expected_reached is not None and account["reached"] != expected_reached:
        raise GovernedRunError(
            f"first publish reached {account['reached']} steps, expected "
            f"{expected_reached} — the worklist did not process the whole cone: "
            f"{account}"
        )
    if account["run_causes"] != [CAUSE_EFFECTS_OVERLAP]:
        raise GovernedRunError(
            "first publish runs must be effects-overlap refusals (a truthful "
            "changed-state statement), never run_always (an unbounded envelope "
            f"proves nothing about the ladder): {account}"
        )
    return account


# --------------------------------------------------------------------------- #
# The two-phase governed run over an ALREADY-RENDERED corpus.                  #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GovernedBenchmark:
    """A benchmark corpus published through the governed path, end to end."""

    clean: GovernedPublish
    corrupted: GovernedPublish
    scorecard: Mapping[str, Any]
    routed: frozenset[str]
    checks: tuple[Any, ...]

    def generation_ids(self) -> list[str]:
        return [self.clean.generation_id, self.corrupted.generation_id]


def run_governed_benchmark(
    *,
    dsn: str,
    deployment_dir: Path,
    run_dir: Path,
    config: Any,
    deployment: str = SMOKE_DEPLOYMENT,
) -> GovernedBenchmark:
    """Publish an already-rendered benchmark corpus through the governed path.

    ``run_dir`` is a completed benchmark artifact directory (it holds
    ``corpus_clean/``, ``corpus_corrupted/`` and the sealed manifest), so the
    governed run consumes the SAME BYTES the direct run did — the parity claim is
    about the two publish paths, never about two renderings.

    Both phases go through :func:`governed_publish`; the corrupted phase is
    scored with the existing :func:`...runner.build_synth_scorecard`, proving the
    governed generation is a first-class published generation the scorer reads at
    a pin."""
    from okg.bench.synth.goldseal import load_manifest
    from okg.bench.synth.runner import (
        MANIFEST_FILENAME,
        build_synth_scorecard,
        constraint_policy_facts,
        finding_kinds,
        functional_policies,
        routed_buckets,
    )
    from okg.substrate.consistency.ir_check import load_ir_consistency_checks

    if getattr(config.realism, "extractor", "deterministic") == "llm":
        raise GovernedRunError(
            "governed path: the LLM lane is OUT OF SCOPE for this binding — its "
            "per-document envelope cannot be derived from the extraction rule, "
            "so the cone would declare claim_keys unbounded and every step would "
            "run for a reason that proves nothing about the ladder. Bind the "
            "deterministic lane; the LLM lane needs its own declared envelope."
        )

    run_dir = Path(run_dir)
    manifest = load_manifest(run_dir / MANIFEST_FILENAME)
    extra_policies, excluded = constraint_policy_facts(manifest)
    policies = functional_policies(manifest, exclude=excluded)
    arm_plan = manifest.get("precedence_arm", {}).get("plan")
    admits = tuple(config.precedence_arm.admits)
    provenance_plan = manifest.get("provenance")
    checks = tuple(load_ir_consistency_checks(Path(deployment_dir)))

    clean = governed_publish(
        dsn, run_dir / "corpus_clean", deployment_dir, policies,
        phase="clean", deployment=deployment, checks=checks,
        extra_policy_facts=extra_policies, arm_plan=arm_plan, admits=admits,
        provenance_plan=provenance_plan,
    )
    corrupted = governed_publish(
        dsn, run_dir / "corpus_corrupted", deployment_dir, policies,
        clean.claim_ids, phase="corrupted", deployment=deployment, checks=checks,
        extra_policy_facts=extra_policies, arm_plan=arm_plan, admits=admits,
        provenance_plan=provenance_plan,
    )

    clean_state = detection_state(dsn, clean.generation_id)
    corrupted_state = detection_state(dsn, corrupted.generation_id)
    routed = routed_buckets(dsn)
    resolutions = resolutions_at(
        dsn, corrupted.generation_id, corrupted_state.claims
    )
    kinds = finding_kinds(dsn, corrupted.generation_id)
    core = build_synth_scorecard(
        manifest=manifest,
        clean_state=clean_state,
        corrupted_state=corrupted_state,
        routed=routed,
        config=config,
        resolutions=resolutions,
        finding_kinds=kinds,
    )
    return GovernedBenchmark(
        clean=clean, corrupted=corrupted, scorecard=core, routed=routed,
        checks=checks,
    )


# --------------------------------------------------------------------------- #
# Row-wise parity between two pins.                                            #
# --------------------------------------------------------------------------- #
# The four row families the PACT names. Each maps a stable row key to a
# canonical row value, so a comparison names the FIRST differing row rather than
# reporting that two blobs differ.
FAMILY_ACCEPTED_CLAIMS = "accepted_claims"
FAMILY_FINDINGS = "findings"
FAMILY_RESOLUTIONS = "resolutions"
FAMILY_WINNERS = "winners"
PARITY_FAMILIES: tuple[str, ...] = (
    FAMILY_ACCEPTED_CLAIMS, FAMILY_FINDINGS, FAMILY_RESOLUTIONS, FAMILY_WINNERS,
)


@dataclass(frozen=True)
class ResolvedStateRows:
    """The four row families at ONE pinned generation."""

    label: str
    dsn: str
    generation_id: str
    families: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    def row_counts(self) -> dict[str, int]:
        return {name: len(self.families.get(name, {})) for name in PARITY_FAMILIES}

    def total_rows(self) -> int:
        return sum(self.row_counts().values())


def resolved_state_rows(
    dsn: str, generation_id: Any, *, label: str
) -> ResolvedStateRows:
    """Read the four row families at one pinned generation.

    Every read is PINNED (a chronos checkout): an unpinned SELECT on the
    versioned claim tables returns no rows, which would make an empty comparison
    look like agreement."""
    state = detection_state(dsn, generation_id)
    resolutions = resolutions_at(dsn, generation_id, state.claims)
    accepted = effective_claims_at(dsn, generation_id)

    accepted_rows = {
        claim_id: {"value": row["value"], "provenance": row["provenance"]}
        for claim_id, row in accepted.items()
    }
    finding_rows = {
        str(f.finding_id): {
            "channel": f.channel,
            "bucket_key": f.bucket_key,
            "claim_ids": sorted(f.claim_ids),
        }
        for f in state.findings
    }
    # Keyed by the FULL bucket (``predicate:key``): ``bucket_key`` alone is the
    # entity, so two predicates over one entity would collide into one row and a
    # divergence could hide inside the collision.
    resolution_rows = {
        str(res.bucket): {
            "predicate": res.predicate,
            "bucket_key": res.bucket_key,
            "outcome": res.outcome,
            "basis": res.basis,
            "ranked_claim_ids": list(res.ranked_claim_ids),
        }
        for res in resolutions
    }
    winner_rows = {
        str(res.bucket): {
            "winner_claim_id": res.winner_claim_id,
            "winner_value": res.winner_value,
        }
        for res in resolutions
        if res.winner_claim_id
    }
    return ResolvedStateRows(
        label=label,
        dsn=dsn,
        generation_id=str(generation_id),
        families={
            FAMILY_ACCEPTED_CLAIMS: accepted_rows,
            FAMILY_FINDINGS: finding_rows,
            FAMILY_RESOLUTIONS: resolution_rows,
            FAMILY_WINNERS: winner_rows,
        },
    )


def assert_non_empty(
    rows: ResolvedStateRows, *, minimum: Mapping[str, int] | None = None
) -> dict[str, int]:
    """Refuse a comparison over a trivial state — the non-empty guard.

    A silent pass over an empty comparison is itself a failure (req.bind.parity),
    so this runs BEFORE any comparison and names the empty family."""
    floor = {name: 1 for name in PARITY_FAMILIES}
    floor.update(dict(minimum or {}))
    counts = rows.row_counts()
    thin = sorted(
        name for name in PARITY_FAMILIES if counts[name] < floor[name]
    )
    if thin:
        raise ParityError(
            f"parity refused: the {rows.label!r} state at generation "
            f"{rows.generation_id} is trivial — families {thin} hold fewer rows "
            f"than the declared floor (counts={counts}, floor={floor}). A pass "
            "over an empty comparison would prove nothing."
        )
    return counts


def compare_resolved_states(
    left: ResolvedStateRows,
    right: ResolvedStateRows,
    *,
    minimum: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Compare two pins ROW-WISE; fail loudly on the FIRST differing row.

    The non-empty guard runs on BOTH sides first. Then, family by family and row
    key by row key, the first divergence raises :class:`ParityError` naming the
    family, the row key, and both values — never "the states differ"."""
    left_counts = assert_non_empty(left, minimum=minimum)
    right_counts = assert_non_empty(right, minimum=minimum)

    for family in PARITY_FAMILIES:
        lhs = dict(left.families.get(family, {}))
        rhs = dict(right.families.get(family, {}))
        for key in sorted(set(lhs) | set(rhs)):
            if key not in lhs:
                raise ParityError(
                    f"parity FAILED on the first differing row: family "
                    f"{family!r} row {key!r} is present in {right.label!r} "
                    f"(gen {right.generation_id}) and ABSENT from {left.label!r} "
                    f"(gen {left.generation_id}); value={rhs[key]!r}"
                )
            if key not in rhs:
                raise ParityError(
                    f"parity FAILED on the first differing row: family "
                    f"{family!r} row {key!r} is present in {left.label!r} "
                    f"(gen {left.generation_id}) and ABSENT from {right.label!r} "
                    f"(gen {right.generation_id}); value={lhs[key]!r}"
                )
            if lhs[key] != rhs[key]:
                raise ParityError(
                    f"parity FAILED on the first differing row: family "
                    f"{family!r} row {key!r} — {left.label}={lhs[key]!r} vs "
                    f"{right.label}={rhs[key]!r}"
                )
    return {
        "compared": True,
        "left": {"label": left.label, "generation_id": left.generation_id,
                 "counts": left_counts},
        "right": {"label": right.label, "generation_id": right.generation_id,
                  "counts": right_counts},
        "rows_compared": left.total_rows(),
    }


# --------------------------------------------------------------------------- #
# Scorecard parity (generation-identifier normalized).                         #
# --------------------------------------------------------------------------- #
GENERATION_PLACEHOLDER = "<generation-id>"


def normalize_scorecard(
    core: Mapping[str, Any], *, generation_ids: Sequence[Any]
) -> str:
    """Canonical scorecard JSON with every generation identifier neutralized.

    Today's ``okg.synth.scorecard.v1`` core is generation-FREE by construction
    (generation ids live only in ``run-binding.json``), so the substitution is
    normally a no-op — :func:`assert_scorecard_parity` asserts that it was, so a
    future core that started carrying a pin cannot make a byte comparison pass by
    comparing the same pin to itself."""
    text = json.dumps(core, sort_keys=True, separators=(",", ":"))
    for gen in sorted({str(g) for g in generation_ids if str(g)}, key=len,
                      reverse=True):
        text = text.replace(gen, GENERATION_PLACEHOLDER)
    return text


def assert_scorecard_parity(
    direct_core: Mapping[str, Any],
    governed_core: Mapping[str, Any],
    *,
    generation_ids: Sequence[Any],
) -> dict[str, Any]:
    """Byte-compare two scorecards after normalizing generation identifiers.

    Refuses a vacuous comparison: the normalized bytes must be non-trivial and
    must still carry the measured blocks (a scorecard normalized down to nothing
    would compare equal for the wrong reason)."""
    direct_text = normalize_scorecard(direct_core, generation_ids=generation_ids)
    governed_text = normalize_scorecard(governed_core, generation_ids=generation_ids)
    for name, text in (("direct", direct_text), ("governed", governed_text)):
        for required in ("\"funnel\"", "\"per_class\"", "\"extraction\""):
            if required not in text:
                raise ParityError(
                    f"scorecard parity refused: the {name} scorecard lost "
                    f"{required} under generation normalization — the byte "
                    "comparison would be vacuous"
                )
    if direct_text != governed_text:
        keys = sorted(set(direct_core) | set(governed_core))
        first = next(
            (k for k in keys if direct_core.get(k) != governed_core.get(k)), None
        )
        raise ParityError(
            "scorecard parity FAILED after generation normalization; first "
            f"differing top-level block={first!r}: "
            f"direct={direct_core.get(first)!r} vs "
            f"governed={governed_core.get(first)!r}"
        )
    return {
        "bytes_compared": len(direct_text),
        "generation_ids_normalized": sorted({str(g) for g in generation_ids}),
        "placeholder_substitutions": direct_text.count(GENERATION_PLACEHOLDER),
    }


def assert_fact_parity(
    governed_facts: Sequence[Any], direct_facts: Sequence[Any]
) -> int:
    """The governed path's assembled fact list equals the direct path's.

    Not the parity requirement itself (that is the resolved state), but the
    property ``concord-exact-parity`` will rest on: both executors must consume
    the SAME fact set, so a later verdict is attributable to reuse and never to
    extraction drift. Names the first differing element."""
    if len(governed_facts) != len(direct_facts):
        raise ParityError(
            f"fact parity FAILED: governed assembled {len(governed_facts)} "
            f"facts, direct extracted {len(direct_facts)}"
        )
    if not governed_facts:
        raise ParityError("fact parity refused: both fact lists are empty")
    for index, (lhs, rhs) in enumerate(zip(governed_facts, direct_facts)):
        left = (lhs.node_id, lhs.subtype, dict(lhs.attrs), lhs.op)
        right = (rhs.node_id, rhs.subtype, dict(rhs.attrs), rhs.op)
        if left != right:
            raise ParityError(
                f"fact parity FAILED at index {index}: governed={left!r} vs "
                f"direct={right!r}"
            )
    return len(governed_facts)


__all__ = [
    "GOVERNED_SCHEMA",
    "DETERMINISTIC_EXTRACTOR",
    "LLM_EXTRACTOR",
    "STEP_ROLES",
    "PARITY_FAMILIES",
    "FAMILY_ACCEPTED_CLAIMS",
    "FAMILY_FINDINGS",
    "FAMILY_RESOLUTIONS",
    "FAMILY_WINNERS",
    "GENERATION_PLACEHOLDER",
    "CorpusCone",
    "GovernedBenchmark",
    "GovernedPublish",
    "GovernedRunError",
    "ParityError",
    "ResolvedStateRows",
    "assert_fact_parity",
    "assert_non_empty",
    "assert_scorecard_parity",
    "assert_zero_skips",
    "build_corpus_cone",
    "compare_resolved_states",
    "document_step_id",
    "governed_publish",
    "run_governed_benchmark",
    "normalize_scorecard",
    "partition_recorded_facts",
    "resolved_state_rows",
    "source_vocabularies",
]
