"""Seeded UPDATE HISTORIES: ordered document deltas with per-step sealed gold.

CORE module (substrate-free, like ``arms`` and ``provenance``): a history is a
pure function of ``(config, seed, world, manifest, base corpus)``. It computes,
for each update, the documents the delta touches, their exact bytes, the live
corruption set, and — the reference answer — the accepted-result state the
engine MUST produce, together with the diff against the previous step and a
reason class per bucket. Every artifact is digest-chained to its predecessor,
so a history is tamper-evident end to end.

Why this exists. The benchmark's unit of measurement was one frozen corpus:
plant, publish, score. Concord's unit of measurement is one UPDATE — after each
change, redo only what is affected and return exactly what a full redo returns.
Grading that needs the reference answer PER STEP, and the reference answer must
not be produced by the system under test. So the expected accepted-result diff
is computed HERE, from the world and a gold-side restatement of the precedence
order, and the direct-path replay witness is what catches the restatement and
the engine drifting apart.

Determinism has TWO halves and only the first lives here:

* GOLD-SIDE (this module): the same ``(config, seed, world)`` yields
  byte-identical histories; every random choice goes through the keyed PRF
  namespaced by update index.
* REPLAY-SIDE (:mod:`okg.bench.synth.history_replay`): extraction runs ONCE per
  update and the fact set is RECORDED; every executor replaying that update
  consumes the recorded facts verbatim. Without it, a history replayed through a
  nondeterministic extractor yields two different claim sets and downstream
  parity reads provider drift as a maintenance defect.

The delta taxonomy covers the four shapes the hand-authored live reuse proof
(``tests/substrate/consistency/test_reuse_live_proof.py``) needed, plus the two
that compose the error funnel over a history:

``fresh_conflict``     a new conflicting claim arrives into a unanimous bucket
``bucket_increment``   a HIGHER-precedence claim into an already-conflicted one
``reaffirmation``      a document is revised with NO claim delta (carry-forward)
``dominated_claim``    a claim that LOSES to the incumbent (the skippable shape)
``plant_error``        a sealed corruption site becomes live
``unplant_error``      a live corruption site is corrected away
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from okg.bench.synth.config import (
    SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE,
    SIGNAL_DECLARED_SOURCE_AUTHORITY,
    SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT,
    SIGNAL_EXPLICIT_SUPERSESSION,
    SIGNAL_OPERATOR_PRECEDENT,
    SynthConfig,
)
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.worldschema import GroundTruthWorld

HISTORY_SCHEMA = "okg.synth.history.v1"
HISTORY_STEP_SCHEMA = "okg.synth.history-step.v1"

# -- the delta taxonomy ----------------------------------------------------
DELTA_FRESH_CONFLICT = "fresh_conflict"
DELTA_BUCKET_INCREMENT = "bucket_increment"
DELTA_REAFFIRMATION = "reaffirmation"
DELTA_DOMINATED_CLAIM = "dominated_claim"
DELTA_PLANT_ERROR = "plant_error"
DELTA_UNPLANT_ERROR = "unplant_error"

DELTA_KINDS = (
    DELTA_FRESH_CONFLICT,
    DELTA_BUCKET_INCREMENT,
    DELTA_REAFFIRMATION,
    DELTA_DOMINATED_CLAIM,
    DELTA_PLANT_ERROR,
    DELTA_UNPLANT_ERROR,
)

# The four shapes the live reuse proof hand-authored, mapped to the taxonomy
# label a generated history carries. A history that does not realize all four is
# not a substitute for the hand-authored series, so the generator asserts it.
LIVE_PROOF_SHAPES = {
    DELTA_FRESH_CONFLICT: "fresh-conflict",
    DELTA_BUCKET_INCREMENT: "incremental-higher-authority",
    DELTA_REAFFIRMATION: "unchanged-no-claim-delta",
    DELTA_DOMINATED_CLAIM: "changed-inputs-skip",
}

# -- reason classes on the expected diff -----------------------------------
# The three answers to "why is this bucket in the state it is". A sound reuse
# mechanism could have skipped exactly the first two; the third is work it had
# to do. That is the work ledger's denominator.
REASON_UNTOUCHED = "untouched_inputs"
REASON_DOMINATED = "touched_but_dominated"
REASON_DECISIVE = "touched_and_decisive"

# -- annotation kinds (the review side-car's locators) ----------------------
# Recorded WHERE THE OFFSET IS ALREADY KNOWN — inside the placer that writes the
# bytes — never re-derived by searching a document for its own text. A span
# searched for after the fact is ambiguous the moment a surface repeats, and the
# placer already shifts every later offset by ``len(post) - len(pre)``, so the
# arithmetic exists here and nowhere else.
ANNOTATION_CORRUPTION = "corruption"
ANNOTATION_CLAIM = "claim"
#: The update index a BASE-corpus annotation carries: the corpus before update 0.
BASE_UPDATE_INDEX = -1


class HistoryError(ValueError):
    """The requested history cannot be realized against this corpus."""


# --------------------------------------------------------------------------- #
# The GOLD-SIDE precedence model.                                              #
#                                                                              #
# A deliberate INDEPENDENT RESTATEMENT of the engine's ``≻`` order — the same   #
# discipline ``computed_support`` uses against the SQL surface it audits. It    #
# must not import the engine: a reference answer produced by the system under   #
# test cannot falsify that system. The direct-path replay witness is what keeps #
# the restatement honest — when the two disagree, the witness names the bucket. #
# --------------------------------------------------------------------------- #
SIG_PRECEDENT = SIGNAL_OPERATOR_PRECEDENT
SIG_SUPERSESSION = SIGNAL_EXPLICIT_SUPERSESSION
SIG_EFFECTIVE_DATE = SIGNAL_CLAIM_LOCAL_EFFECTIVE_DATE
SIG_SOURCE_AUTHORITY = SIGNAL_DECLARED_SOURCE_AUTHORITY
SIG_SUPPORT = SIGNAL_DEPENDENCY_ADJUSTED_SUPPORT
SIG_DOCUMENT_RECENCY = "document_recency"
SIG_MAINTENANCE = "maintenance_heuristic"

# The ≻ order, strongest first. PARAMETERIZED on purpose: mutating it is how the
# skippable-step labelling is proven falsifiable (a claim that is dominated under
# the declared order becomes decisive under a transposed one).
GOLD_PRECEDENCE_ORDER = (
    SIG_PRECEDENT,
    SIG_SUPERSESSION,
    SIG_EFFECTIVE_DATE,
    SIG_SOURCE_AUTHORITY,
    SIG_SUPPORT,
    SIG_DOCUMENT_RECENCY,
    SIG_MAINTENANCE,
)
# Only these SETTLE a winner; recency and maintenance rank but never settle.
GOLD_DECISIVE = frozenset(
    {SIG_PRECEDENT, SIG_SUPERSESSION, SIG_EFFECTIVE_DATE, SIG_SOURCE_AUTHORITY, SIG_SUPPORT}
)
BASIS_TIE = "tie"
BASIS_SOLE = "sole_claim"
OUTCOME_CANONICAL = "canonical"
OUTCOME_CONTESTED = "contested"


@dataclass(frozen=True)
class GoldClaim:
    """One claim as GOLD knows it, before the engine ever sees the corpus."""

    claim_id: str
    bucket: str
    source_id: str  # the document — the corroboration unit
    value: str
    source_class: str = "wiki"
    is_superseded: bool = False
    effective_date: str = ""
    source_authority: int = 0
    document_recency: str = ""
    maintenance_rank: int = 0


def gold_canonicalize_value(value: str) -> str:
    """Unit canonicalization, restated gold-side.

    MUST equal ``runner.canonicalize_value``: the corpus renders a benign
    unit-equivalent surface ('82000 g') that the ingest seam folds back to the
    canonical one ('82.0 kg'), so a gold model that skipped the fold would see a
    two-value bucket where the engine sees a unanimous one. Restated rather than
    imported because the runner is the substrate-facing half; the parity is
    asserted by a witness over the real rendered surfaces, never assumed.
    """
    match = re.match(r"^(?P<num>\d+(?:\.\d+)?) (?P<unit>g|mm)$", value)
    if match is None:
        return value
    unit, factor = {"g": ("kg", 0.001), "mm": ("cm", 0.1)}[match.group("unit")]
    return f"{float(match.group('num')) * factor:.1f} {unit}"


def _signal_value(claim: GoldClaim, signal: str, support: int) -> Any:
    if signal == SIG_PRECEDENT:
        return 0  # no operator precedent participates in a history (declared)
    if signal == SIG_SUPERSESSION:
        return 0 if claim.is_superseded else 1
    if signal == SIG_EFFECTIVE_DATE:
        return claim.effective_date
    if signal == SIG_SOURCE_AUTHORITY:
        return claim.source_authority
    if signal == SIG_SUPPORT:
        return support
    if signal == SIG_DOCUMENT_RECENCY:
        return claim.document_recency
    if signal == SIG_MAINTENANCE:
        return claim.maintenance_rank
    raise HistoryError(f"unknown precedence signal {signal!r}")


def gold_support(claims: Sequence[GoldClaim]) -> dict[str, int]:
    """claim_id -> corroboration: DISTINCT source documents asserting its value.

    Restated from the engine's rule (distinct ``source_id`` per (bucket, value)).
    Copies do not count once: N mirrored documents outrank one correct original,
    and the benchmark must predict that, not wish it away.

    SCOPE. This restates the rule for a corpus that declares no ORIGIN. The
    engine counts distinct DECLARED origin (``provenance_group``) and treats an
    undeclared claim as its own origin — its document identity — so the two rules
    coincide exactly while nothing is declared, which is the case for every
    history a ``HistoryConfig`` can currently generate (it stamps ranking signals
    only). A history that starts stamping ``provenance_group`` must switch this
    to count declared origins, or its expected diff will over-count corroboration
    for mirrored claims and the direct-path replay will fail naming the bucket.
    """
    sources: dict[tuple[str, str], set[str]] = {}
    for claim in claims:
        sources.setdefault((claim.bucket, claim.value), set()).add(claim.source_id)
    return {c.claim_id: len(sources[(c.bucket, c.value)]) for c in claims}


def gold_resolve_bucket(
    claims: Sequence[GoldClaim],
    *,
    order: Sequence[str] = GOLD_PRECEDENCE_ORDER,
) -> dict[str, Any]:
    """Resolve ONE bucket the way the engine's ``resolve_bucket`` must.

    The runner-up is the best claim asserting a DIFFERENT value (claims sharing
    the leader's value corroborate rather than compete). Canonical only when the
    first signal separating leader from runner-up is DECISIVE; a lead on a
    ranking-only signal, or a full tie, is CONTESTED.
    """
    if not claims:
        raise HistoryError("gold_resolve_bucket: empty bucket")
    support = gold_support(claims)

    def key(claim: GoldClaim) -> tuple:
        return tuple(_signal_value(claim, sig, support[claim.claim_id]) for sig in order)

    ranked = sorted(claims, key=lambda c: (key(c), c.claim_id), reverse=True)
    leader = ranked[0]
    rival = next((c for c in ranked[1:] if c.value != leader.value), None)
    if rival is None:
        return {
            "outcome": OUTCOME_CANONICAL,
            "winner_value": leader.value,
            "winner_claim_id": leader.claim_id,
            "basis": BASIS_SOLE,
        }
    top, runner = key(leader), key(rival)
    for index, (a, b) in enumerate(zip(top, runner)):
        if a != b:
            basis = order[index]
            if basis in GOLD_DECISIVE:
                return {
                    "outcome": OUTCOME_CANONICAL,
                    "winner_value": leader.value,
                    "winner_claim_id": leader.claim_id,
                    "basis": basis,
                }
            return {
                "outcome": OUTCOME_CONTESTED,
                "winner_value": None,
                "winner_claim_id": "",
                "basis": basis,
            }
    return {
        "outcome": OUTCOME_CONTESTED,
        "winner_value": None,
        "winner_claim_id": "",
        "basis": BASIS_TIE,
    }


def predict_accepted_state(
    claims: Sequence[GoldClaim],
    *,
    order: Sequence[str] = GOLD_PRECEDENCE_ORDER,
) -> dict[str, dict[str, Any]]:
    """The ACCEPTED STATE gold expects: bucket -> resolution cell.

    Only CONFLICTED buckets appear — the engine writes a ``bucket_resolution``
    row per conflicted bucket and none for a unanimous one, so a unanimous bucket
    is ABSENT from the accepted state rather than present-and-unanimous. A
    bucket's appearance and disappearance are therefore themselves diffs.
    """
    by_bucket: dict[str, list[GoldClaim]] = {}
    for claim in claims:
        by_bucket.setdefault(claim.bucket, []).append(claim)
    out: dict[str, dict[str, Any]] = {}
    for bucket in sorted(by_bucket):
        members = by_bucket[bucket]
        if len({c.value for c in members}) < 2:
            continue  # unanimous: no conflict, no resolution row
        cell = gold_resolve_bucket(members, order=order)
        out[bucket] = {
            "outcome": cell["outcome"],
            "winner_value": cell["winner_value"],
            "basis": cell["basis"],
        }
    return out


# --------------------------------------------------------------------------- #
# History configuration + artifacts                                            #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class HistoryConfig:
    """The history's own declaration — deliberately NOT part of SynthConfig.

    Keeping it separate is what lets a history be authored without moving every
    existing preset's ``config_digest`` (and every pinned golden with it). The
    history's comparability key is :meth:`digest`, which folds in the synth
    config digest it was generated against.
    """

    updates: int = 22
    arm_buckets: int = 4
    unplant_after: int = 3
    admits: tuple[str, ...] = (SIG_SOURCE_AUTHORITY,)
    precedence_order: tuple[str, ...] = GOLD_PRECEDENCE_ORDER
    fresh_authority: int = 9
    increment_authority: int = 20
    # Declared draw weights over the taxonomy. A uniform draw over whatever is
    # feasible makes ``reaffirmation`` — the only always-feasible kind — the
    # plurality of a long history, which starves the shapes the ledger needs a
    # denominator from. Weights are part of the history digest, so a re-weighted
    # history is a different history rather than a quiet re-roll.
    kind_weights: tuple[tuple[str, int], ...] = (
        (DELTA_FRESH_CONFLICT, 2),
        (DELTA_BUCKET_INCREMENT, 2),
        (DELTA_DOMINATED_CLAIM, 3),
        (DELTA_REAFFIRMATION, 1),
        (DELTA_PLANT_ERROR, 3),
        (DELTA_UNPLANT_ERROR, 2),
    )

    def as_json(self) -> dict[str, Any]:
        return {
            "updates": self.updates,
            "arm_buckets": self.arm_buckets,
            "unplant_after": self.unplant_after,
            "admits": list(self.admits),
            "precedence_order": list(self.precedence_order),
            "fresh_authority": self.fresh_authority,
            "increment_authority": self.increment_authority,
            "kind_weights": [list(pair) for pair in self.kind_weights],
        }

    def digest(self, config_digest: str) -> str:
        payload = {"history": self.as_json(), "config_digest": config_digest}
        return hashlib.sha256(_canonical(payload)).hexdigest()


@dataclass(frozen=True)
class UpdateDelta:
    """One delta from the declared taxonomy."""

    kind: str
    label: str
    bucket: str = ""
    corruption_id: str = ""
    detail: Mapping[str, str] = field(default_factory=dict)

    def as_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "label": self.label,
            "bucket": self.bucket,
            "corruption_id": self.corruption_id,
            "detail": {k: self.detail[k] for k in sorted(self.detail)},
        }


@dataclass(frozen=True)
class DocumentAnnotation:
    """One located span in one document's bytes AT ONE UPDATE.

    Deliberately NOT part of :meth:`HistoryStep.sealed_payload`: adding a field
    there would move every ``step_digest`` and every ``chain_digest`` after it,
    and unbind the committed capstone row from its own history. Annotations ride
    beside the sealed history (``history_to_json`` carries them, ``as_json`` does
    not) and are bound TO the chain by the renderer, which re-digests the bytes
    each span points into before it draws a single mark.
    """

    doc: str
    update_index: int
    start: int
    end: int
    kind: str
    claim_id: str = ""
    corruption_id: str = ""

    def as_json(self) -> dict[str, Any]:
        return {
            "doc": self.doc,
            "update_index": self.update_index,
            "start": self.start,
            "end": self.end,
            "kind": self.kind,
            "claim_id": self.claim_id,
            "corruption_id": self.corruption_id,
        }


def annotation_from_json(payload: Mapping[str, Any]) -> DocumentAnnotation:
    return DocumentAnnotation(
        doc=str(payload["doc"]),
        update_index=int(payload["update_index"]),
        start=int(payload["start"]),
        end=int(payload["end"]),
        kind=str(payload["kind"]),
        claim_id=str(payload.get("claim_id", "")),
        corruption_id=str(payload.get("corruption_id", "")),
    )


def _annotation_sort_key(entry: DocumentAnnotation) -> tuple:
    return (
        entry.update_index,
        entry.doc,
        entry.start,
        entry.end,
        entry.kind,
        entry.claim_id,
        entry.corruption_id,
    )


@dataclass(frozen=True)
class HistoryStep:
    """One update: its delta, the documents it touches, and the gold it seals."""

    index: int
    delta: UpdateDelta
    touched_docs: tuple[str, ...]
    doc_texts: Mapping[str, str]  # touched documents only, post-update bytes
    corpus_digests: Mapping[str, str]  # the WHOLE corpus after the update
    live_corruptions: tuple[str, ...]
    stamps: Mapping[str, Mapping[str, str]]
    expected_state: Mapping[str, Mapping[str, Any]]
    expected_diff: Mapping[str, Any]
    step_digest: str = ""
    prev_chain_digest: str = ""
    chain_digest: str = ""

    def sealed_payload(self) -> dict[str, Any]:
        """Everything the step's digest covers (the chain field excluded)."""
        return {
            "schema": HISTORY_STEP_SCHEMA,
            "index": self.index,
            "delta": self.delta.as_json(),
            "touched_docs": list(self.touched_docs),
            "touched_digests": {
                doc: _sha256(self.doc_texts[doc].encode("utf-8"))
                for doc in sorted(self.doc_texts)
            },
            "corpus_digests": {k: self.corpus_digests[k] for k in sorted(self.corpus_digests)},
            "live_corruptions": list(self.live_corruptions),
            "stamps": {
                claim: {a: self.stamps[claim][a] for a in sorted(self.stamps[claim])}
                for claim in sorted(self.stamps)
            },
            "expected_state": {
                b: dict(self.expected_state[b]) for b in sorted(self.expected_state)
            },
            "expected_diff": json.loads(json.dumps(self.expected_diff, sort_keys=True)),
        }


@dataclass(frozen=True)
class History:
    """A byte-reproducible sequence of updates with chained per-step gold."""

    schema: str
    config_digest: str
    history_digest: str
    manifest_digest: str
    world_digest: str
    admits: tuple[str, ...]
    precedence_order: tuple[str, ...]
    base_docs: Mapping[str, str]
    steps: tuple[HistoryStep, ...]
    chain_seed: str = ""
    #: Located spans in the bytes above, recorded by the placer. NOT sealed —
    #: :meth:`as_json` deliberately omits them, so no digest in this module moves
    #: when they are present. See :class:`DocumentAnnotation`.
    annotations: tuple[DocumentAnnotation, ...] = ()

    def as_json(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "config_digest": self.config_digest,
            "history_digest": self.history_digest,
            "chain_seed": self.chain_seed,
            "manifest_digest": self.manifest_digest,
            "world_digest": self.world_digest,
            "admits": list(self.admits),
            "precedence_order": list(self.precedence_order),
            "base_corpus_digests": {
                doc: _sha256(self.base_docs[doc].encode("utf-8"))
                for doc in sorted(self.base_docs)
            },
            "steps": [
                {
                    **step.sealed_payload(),
                    "step_digest": step.step_digest,
                    "prev_chain_digest": step.prev_chain_digest,
                    "chain_digest": step.chain_digest,
                }
                for step in self.steps
            ],
        }

    def json_bytes(self) -> bytes:
        return _canonical(self.as_json())

    def corpus_at(self, index: int) -> dict[str, str]:
        """The FULL corpus after update ``index`` (-1 = the base corpus)."""
        docs = dict(self.base_docs)
        for step in self.steps:
            if step.index > index:
                break
            docs.update(step.doc_texts)
        return docs

    def touched_buckets_at(self, index: int) -> frozenset[str]:
        step = self.steps[index]
        return frozenset(step.expected_diff["touched_buckets"])


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def verify_chain(history: History) -> list[str]:
    """Every step whose sealed gold no longer matches the chain (loud, named).

    A history is tamper-evident end to end: altering ANY intermediate artifact —
    an expected winner, a touched-document set, a live corruption — re-digests
    that step and every step after it.
    """
    broken: list[str] = []
    prev = history.chain_seed
    for step in history.steps:
        expected_step = _sha256(_canonical(step.sealed_payload()))
        expected_chain = _sha256(f"{prev}:{expected_step}".encode("utf-8"))
        if step.step_digest != expected_step:
            broken.append(
                f"step {step.index}: sealed gold altered "
                f"(digest {step.step_digest[:12]} != recomputed {expected_step[:12]})"
            )
        elif step.prev_chain_digest != prev:
            broken.append(
                f"step {step.index}: chain broken - carries prev "
                f"{step.prev_chain_digest[:12]}, the chain reached {prev[:12]}"
            )
        elif step.chain_digest != expected_chain:
            broken.append(f"step {step.index}: chain digest does not close")
        # The chain advances on the RECOMPUTED digest, never the stored one:
        # carrying the stored value forward would let a tampered step re-anchor
        # the chain at its own forgery and every later step would still verify —
        # a chain that stops at the first break is not tamper-evident end to end.
        prev = expected_chain
    return broken


# --------------------------------------------------------------------------- #
# Corpus surgery — the exact bytes each delta produces                         #
# --------------------------------------------------------------------------- #
_NUM_UNIT_RE = re.compile(r"^(?P<num>\d+(?:\.\d+)?) (?P<unit>[A-Za-z%/0-9]+)$")


def _slug(text: str) -> str:
    """The one slug rule (equals claim_corpus_source._slug / arms._slug)."""
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _h1_title(text: str) -> str:
    match = re.match(r"^#\s+(?P<title>.+?)\s*$", text.splitlines()[0])
    if match is None:
        raise HistoryError("base document has no H1 title to key the entity on")
    return match.group("title")


def _section_at(text: str, offset: int) -> str:
    """The H2 section the byte at ``offset`` sits under.

    Replaces a scan for a ``Label: value`` line (:func:`_section_of`), which
    only ever worked on STRUCTURED rendering: a prose document writes "Her A1c
    was 6.0 %." and carries no such line, so a history could not be built over
    prose at all — and the three-mode maintenance comparison, which is this
    system's headline claim, was therefore confined to documents a parser
    cannot misread.

    The scope requirement the label scan existed for is unchanged and is why
    this reads the BYTES rather than a template: a claim's scope IS its heading
    path, the engine treats disjoint scopes as compatible, and an update
    document that invented its own heading would be silently scope-suppressed
    out of the bucket it is meant to join. What changes is only HOW the section
    is located — by the rendering's own sealed offset instead of by a surface
    pattern. The renderer already recorded where it put the value; re-deriving
    that from the output was the mistake.
    """
    if offset < 0 or offset > len(text):
        raise HistoryError(
            f"claim offset {offset} is outside the document ({len(text)} bytes)"
        )
    section = ""
    cursor = 0
    for line in text.splitlines(keepends=True):
        if cursor > offset:
            break
        stripped = line.strip()
        if stripped.startswith("## "):
            section = stripped[3:].strip()
        cursor += len(line)
    return section


def _section_of(text: str, label: str) -> str:
    """The H2 section a ``Label: value`` line sits under, read from the bytes.

    Read rather than looked up in the template pack: the claim's SCOPE is its
    heading path, and the engine treats disjoint scopes as compatible, so an
    update document that invented its own heading would be silently
    scope-suppressed out of the very bucket it is meant to join.
    """
    section = ""
    for line in text.splitlines():
        if line.startswith("## "):
            section = line[3:].strip()
        elif line.startswith(f"{label}: "):
            return section
    raise HistoryError(f"document does not render label {label!r}")


def _update_doc_text(title: str, section: str, label: str, surface: str, note: str) -> str:
    """One update document. The note is BODY TEXT, never a heading: a heading
    would open a new scope and disjoint scopes never conflict."""
    text, _start, _end = _update_doc_text_placed(title, section, label, surface, note)
    return text


def _update_doc_text_placed(
    title: str, section: str, label: str, surface: str, note: str
) -> tuple[str, int, int]:
    """:func:`_update_doc_text`, with the surface value's span in the bytes.

    The document is SYNTHESIZED here, so the offset is a property of the
    construction: the surface starts where the prefix ends. Assembled once, from
    the same pieces, so the text and the span can never disagree.
    """
    prefix = (
        f"# {title}\n\n"
        f"**Interim Update.** {note}\n\n"
        f"## {section}\n\n"
        f"{label}: "
    )
    start = len(prefix)
    return f"{prefix}{surface}\n\n", start, start + len(surface)


def _alternate_value(base: str, factor: float) -> str:
    match = _NUM_UNIT_RE.match(base)
    if match is None:
        raise HistoryError(
            f"cannot derive an alternate for non-numeric surface {base!r}; the "
            "history's arm buckets are restricted to numeric-with-unit values so "
            "a rival value is a real measurement disagreement, not a mangled string"
        )
    raw = match.group("num")
    decimals = len(raw.split(".")[1]) if "." in raw else 0
    return f"{float(raw) * factor:.{decimals}f} {match.group('unit')}"


def _apply_sites(clean_text: str, sites: Sequence[Mapping[str, Any]]) -> str:
    """Re-render one document with exactly the LIVE corruption sites applied.

    Applied from the sealed clean spans in DESCENDING order so earlier offsets
    stay valid, and every replacement is verified against the sealed ``pre_text``
    — a silently-missed edit would put gold and the corpus out of step.
    """
    text, _placed, _carried = _apply_sites_recorded(clean_text, sites, ())
    return text


def _apply_sites_recorded(
    clean_text: str,
    sites: Sequence[Mapping[str, Any]],
    clean_spans: Sequence[tuple[int, int, str]],
) -> tuple[str, list[tuple[int, int, str]], list[tuple[int, int, str]]]:
    """:func:`_apply_sites`, recording where each span LANDED.

    ``clean_spans`` are ``(start, end, claim_id)`` in the CLEAN document's
    coordinates — the sealed trace's own offsets. Returns the corrupted text,
    the corruption placements as ``(start, end, corruption_id)``, and the carried
    claim spans in the corrupted document's coordinates.

    The arithmetic is the one this function already performs. Substituting at
    ``start`` moves every offset at or after ``end`` by
    ``len(post_text) - len(pre_text)``; because sites are applied in DESCENDING
    order, every placement recorded so far sits above the current site and shifts
    with it. A claim span the site sits INSIDE becomes the site's own extent —
    the corrupted surface IS that claim's surface — so the claim keeps a span
    rather than losing one. Nothing here searches the document for its own text.
    """
    text = clean_text
    placed: list[list[Any]] = []
    carried: list[list[Any]] = [[int(a), int(b), cid] for a, b, cid in clean_spans]
    for site in sorted(sites, key=lambda s: -int(s["clean_span"][0])):
        start, end = int(site["clean_span"][0]), int(site["clean_span"][1])
        if text[start:end] != site["pre_text"]:
            raise HistoryError(
                f"corruption {site['corruption_id']}: clean span {start}:{end} of "
                f"{site['doc']} holds {text[start:end]!r}, sealed gold says "
                f"{site['pre_text']!r}"
            )
        text = text[:start] + site["post_text"] + text[end:]
        landed_end = start + len(site["post_text"])
        delta = landed_end - end
        for entry in placed:  # every earlier placement is ABOVE this site
            entry[0] += delta
            entry[1] += delta
        for entry in carried:
            if entry[0] >= end:
                entry[0] += delta
                entry[1] += delta
            elif entry[0] >= start and entry[1] <= end:
                entry[0], entry[1] = start, landed_end
        placed.append([start, landed_end, str(site["corruption_id"])])
    return (
        text,
        [(int(a), int(b), str(c)) for a, b, c in placed],
        [(int(a), int(b), str(c)) for a, b, c in carried],
    )


def base_claim_spans(manifest: Mapping[str, Any]) -> dict[str, list[tuple[int, int, str]]]:
    """doc -> the CLEAN surface span of every base claim rendering.

    A verbatim join of two sealed records that already carry the offsets: the
    manifest's trace (``clean_start`` / ``clean_end`` per rendering) and the claim
    id :func:`_base_claim_rows` mints for that same row, in the same order. No
    offset is computed here and none is searched for.
    """
    rows = _base_claim_rows(manifest)
    trace = manifest["trace"]
    if len(rows) != len(trace):  # pragma: no cover - defensive
        raise HistoryError(
            f"{len(rows)} base claim row(s) against {len(trace)} trace row(s); the "
            "claim ids and the sealed offsets are no longer the same list"
        )
    out: dict[str, list[tuple[int, int, str]]] = {}
    for row, traced in zip(rows, trace):
        out.setdefault(str(traced["doc"]), []).append(
            (int(traced["clean_start"]), int(traced["clean_end"]), str(row["claim_id"]))
        )
    return {doc: sorted(out[doc]) for doc in sorted(out)}


# --------------------------------------------------------------------------- #
# The gold claim population                                                    #
# --------------------------------------------------------------------------- #
def _base_claim_rows(
    manifest: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """One row per BASE rendering: claim id, bucket, doc, clean surface.

    Occurrence is assigned in trace order within ``(doc, predicate, entity)`` —
    the same counting ``claim_corpus_source`` does line by line — so a fact
    rendered twice in one document keys distinctly.
    """
    patients = manifest["patients"]
    clean_by_site: dict[tuple[str, str], str] = {}
    for site in manifest["gold"]["corruptions"]:
        clean_by_site[(site["fact_id"], site["doc"])] = site["pre_text"]
    occ: dict[tuple[str, str, str], int] = {}
    rows: list[dict[str, Any]] = []
    for row in manifest["trace"]:
        predicate = _slug(row["predicate_label"])
        entity = patients[row["fact_id"].split(":")[1]]["entity"]
        doc = row["doc"]
        key = (doc, predicate, entity)
        occurrence = occ.get(key, 0)
        occ[key] = occurrence + 1
        # The trace records the CORRUPTED surface; the clean one is the site's
        # sealed pre_text where a site touched this rendering, the trace surface
        # everywhere else.
        clean_surface = clean_by_site.get((row["fact_id"], doc), row["surface_value"])
        rows.append(
            {
                "clean_start": int(row["clean_start"]),
                "claim_id": f"claim:{doc}:{predicate}:{entity}:{occurrence}",
                "bucket": f"{predicate}:{entity}",
                "doc": doc,
                "fact_id": row["fact_id"],
                "label": row["predicate_label"],
                "clean_surface": clean_surface,
                "variation": row.get("variation", ""),
            }
        )
    return rows


def _gold_claims(
    base_rows: Sequence[Mapping[str, Any]],
    live_sites: Mapping[str, Mapping[str, Any]],
    update_claims: Sequence[Mapping[str, Any]],
    stamps: Mapping[str, Mapping[str, str]],
) -> list[GoldClaim]:
    """The full gold claim population at one step, post-canonicalization."""
    live_by_rendering = {
        (site["fact_id"], site["doc"]): site for site in live_sites.values()
    }
    out: list[GoldClaim] = []
    for row in base_rows:
        site = live_by_rendering.get((row["fact_id"], row["doc"]))
        surface = site["post_text"] if site is not None else row["clean_surface"]
        out.append(
            _stamped(
                GoldClaim(
                    claim_id=row["claim_id"],
                    bucket=row["bucket"],
                    source_id=row["doc"],
                    value=gold_canonicalize_value(surface),
                ),
                stamps.get(row["claim_id"], {}),
            )
        )
    for row in update_claims:
        out.append(
            _stamped(
                GoldClaim(
                    claim_id=row["claim_id"],
                    bucket=row["bucket"],
                    source_id=row["doc"],
                    value=gold_canonicalize_value(row["value"]),
                ),
                stamps.get(row["claim_id"], {}),
            )
        )
    return out


def _stamped(claim: GoldClaim, stamp: Mapping[str, str]) -> GoldClaim:
    from dataclasses import replace as _replace

    if not stamp:
        return claim
    updates: dict[str, Any] = {}
    if "source_authority" in stamp:
        updates["source_authority"] = int(stamp["source_authority"])
    if "is_superseded" in stamp:
        updates["is_superseded"] = stamp["is_superseded"] == "true"
    if "effective_date" in stamp:
        updates["effective_date"] = stamp["effective_date"]
    return _replace(claim, **updates)


def _bucket_claim_ids(claims: Sequence[GoldClaim]) -> dict[str, dict[str, str]]:
    """bucket -> {claim_id: value} — the INPUT fingerprint a diff reasons over."""
    out: dict[str, dict[str, str]] = {}
    for claim in claims:
        cell = out.setdefault(claim.bucket, {})
        cell[claim.claim_id] = (
            f"{claim.value}|a={claim.source_authority}|s={int(claim.is_superseded)}"
            f"|d={claim.effective_date}"
        )
    return out


def _expected_diff(
    *,
    before_state: Mapping[str, Mapping[str, Any]],
    after_state: Mapping[str, Mapping[str, Any]],
    before_inputs: Mapping[str, Mapping[str, str]],
    after_inputs: Mapping[str, Mapping[str, str]],
) -> dict[str, Any]:
    """Which buckets MUST change, which MUST NOT, and WHY — the reference answer.

    Reason classes are the maintenance vocabulary: ``untouched_inputs`` (nothing
    this bucket depends on moved), ``touched_but_dominated`` (inputs moved, the
    accepted answer did not — the skippable shape), ``touched_and_decisive``
    (inputs moved and the answer moved with them).
    """
    touched = sorted(
        bucket
        for bucket in set(before_inputs) | set(after_inputs)
        if before_inputs.get(bucket) != after_inputs.get(bucket)
    )
    touched_set = set(touched)
    changed: dict[str, Any] = {}
    unchanged: dict[str, Any] = {}
    for bucket in sorted(set(before_state) | set(after_state) | touched_set):
        before = before_state.get(bucket)
        after = after_state.get(bucket)
        if before != after:
            changed[bucket] = {
                "before": dict(before) if before is not None else None,
                "after": dict(after) if after is not None else None,
                "reason": REASON_DECISIVE,
            }
        else:
            unchanged[bucket] = {
                "state": dict(after) if after is not None else None,
                "reason": REASON_DOMINATED if bucket in touched_set else REASON_UNTOUCHED,
            }
    # A bucket whose inputs did not move CANNOT have moved: corroboration is
    # bucket-local and no other signal crosses buckets. Asserted rather than
    # trusted — if it ever fails, the gold model has a cross-bucket coupling it
    # does not admit to, and every "untouched" label below it is worthless.
    for bucket, cell in changed.items():
        if bucket not in touched_set:
            raise HistoryError(
                f"gold model incoherent: bucket {bucket} changed "
                f"{cell['before']} -> {cell['after']} with UNTOUCHED inputs"
            )
    return {
        "touched_buckets": touched,
        "changed": changed,
        "unchanged": unchanged,
        "skippable": sorted(
            b for b, cell in unchanged.items() if cell["reason"] == REASON_DOMINATED
        ),
    }


# --------------------------------------------------------------------------- #
# The generator                                                                #
# --------------------------------------------------------------------------- #
def _arm_candidates(
    base_rows: Sequence[Mapping[str, Any]],
    corruption_buckets: frozenset[str],
) -> list[dict[str, Any]]:
    """Buckets an update may legitimately target.

    Disjoint from EVERY sealed corruption bucket (a history update landing in a
    corruption's bucket would move that corruption's realized vote and the funnel
    would read an operator's update as an extraction effect), unanimous at base,
    rendered in >= 2 documents, numeric-with-unit, and carrying no benign
    variation.
    """
    by_bucket: dict[str, list[Mapping[str, Any]]] = {}
    for row in base_rows:
        by_bucket.setdefault(row["bucket"], []).append(row)
    out: list[dict[str, Any]] = []
    for bucket in sorted(by_bucket):
        if bucket in corruption_buckets:
            continue
        rows = by_bucket[bucket]
        if len(rows) < 2 or len({r["doc"] for r in rows}) != len(rows):
            continue
        if any(r["variation"] for r in rows):
            continue
        values = {gold_canonicalize_value(r["clean_surface"]) for r in rows}
        if len(values) != 1:
            continue
        (value,) = values
        if _NUM_UNIT_RE.match(value) is None:
            continue
        out.append(
            {
                "bucket": bucket,
                "label": rows[0]["label"],
                "base_value": value,
                "docs": sorted(r["doc"] for r in rows),
                # where each rendering sits in ITS document, so the update's
                # heading scope is read from the sealed offset rather than
                # re-derived from a surface pattern that only structured
                # rendering produces.
                "starts": {r["doc"]: int(r["clean_start"]) for r in rows},
                "fact_id": rows[0]["fact_id"],
            }
        )
    return out


def _plant_order(
    manifest: Mapping[str, Any], base_rows: Sequence[Mapping[str, Any]]
) -> list[str]:
    """The order sites are planted in: the most gradable first.

    A cross-document site whose fact renders three times leaves a clean MAJORITY
    when one rendering is corrupted, so the engine can settle it on the clean
    value and the corruption reaches ``repaired`` — the state the
    no-spontaneous-regression property is about. Sites with no surviving
    counterpart can only ever read as an extraction or expressibility outcome, so
    they are planted later. Deterministic, declared, and derived from sealed gold.
    """
    renderings: dict[str, int] = {}
    for row in base_rows:
        renderings[row["fact_id"]] = renderings.get(row["fact_id"], 0) + 1
    rank = {"cross_doc": 0, "internal_only": 1, "world_knowledge": 2}

    def sort_key(site: Mapping[str, Any]) -> tuple:
        return (
            rank.get(site["partition"], 3),
            -renderings.get(site["fact_id"], 0),
            site["corruption_id"],
        )

    return [s["corruption_id"] for s in sorted(manifest["gold"]["corruptions"], key=sort_key)]


def build_history(
    *,
    config: SynthConfig,
    world: GroundTruthWorld,
    manifest: Mapping[str, Any],
    base_docs: Mapping[str, str],
    history_config: HistoryConfig = HistoryConfig(),
) -> History:
    """Generate the seeded history. Pure; the ONLY entropy is the keyed PRF.

    ``base_docs`` is the CLEAN rendering: a history starts from a corpus with no
    planted error, and every corruption enters through a ``plant_error`` delta,
    so each one carries a real trajectory (planted at k, repaired at k+1,
    corrected away at m) instead of being present-since-forever.
    """
    for site in manifest["gold"]["corruptions"]:
        kind = dict(site.get("params", {})).get("constraint_kind")
        if kind and kind != "functional":
            raise HistoryError(
                f"corruption {site['corruption_id']} declares constraint kind "
                f"{kind!r}; the gold-side precedence model covers the FUNCTIONAL "
                "Σ kind only (a relational kind re-keys its bucket, so a "
                "history's expected accepted-state diff would be silently wrong "
                "for it). Generate the history from a functional-kind preset."
            )
    prf = SynthPrf(config.seed)
    base_rows = _base_claim_rows(manifest)
    sites_by_id = {s["corruption_id"]: s for s in manifest["gold"]["corruptions"]}
    corruption_buckets = frozenset(
        f"{_slug(next(r['label'] for r in base_rows if r['fact_id'] == s['fact_id']))}:"
        f"{manifest['patients'][s['fact_id'].split(':')[1]]['entity']}"
        for s in sites_by_id.values()
    )
    candidates = _arm_candidates(base_rows, corruption_buckets)
    if len(candidates) < history_config.arm_buckets:
        raise HistoryError(
            f"history needs {history_config.arm_buckets} arm buckets, the corpus "
            f"offers {len(candidates)}"
        )
    arms = prf.shuffled(candidates, "history", "arms")[: history_config.arm_buckets]
    plant_queue = _plant_order(manifest, base_rows)

    docs = dict(base_docs)
    # The annotation ledger. ``current`` holds each document's located spans in
    # ITS CURRENT BYTES; every touch rewrites the touched document's entry from
    # the placer that produced those bytes, and the rewritten entry is appended
    # to the sink under that update's index. A document nobody touched keeps the
    # entry it already had, which is what the renderer carries forward.
    clean_spans = base_claim_spans(manifest)
    annotations: list[DocumentAnnotation] = []
    current: dict[str, list[DocumentAnnotation]] = {}
    for doc in sorted(base_docs):
        current[doc] = [
            DocumentAnnotation(
                doc=doc,
                update_index=BASE_UPDATE_INDEX,
                start=start,
                end=end,
                kind=ANNOTATION_CLAIM,
                claim_id=claim_id,
            )
            for start, end, claim_id in clean_spans.get(doc, ())
        ]
        annotations.extend(current[doc])

    live: dict[str, Mapping[str, Any]] = {}
    planted_at: dict[str, int] = {}
    stamps: dict[str, dict[str, str]] = {}
    update_claims: list[dict[str, Any]] = []
    update_notes: dict[str, str] = {}  # update doc -> its current note text
    arm_state: dict[str, str] = {arm["bucket"]: "unused" for arm in arms}
    arm_by_bucket = {arm["bucket"]: arm for arm in arms}
    arm_values: dict[str, dict[str, str]] = {}

    state = predict_accepted_state(
        _gold_claims(base_rows, live, update_claims, stamps),
        order=history_config.precedence_order,
    )
    inputs = _bucket_claim_ids(_gold_claims(base_rows, live, update_claims, stamps))
    steps: list[HistoryStep] = []
    # The chain is anchored in the three artifacts a history is a function of:
    # the synth config, the history declaration, and the SEALED MANIFEST (world,
    # corpus, corruption roster). Swapping any of them under an unchanged step
    # list would otherwise leave the chain closing over a corpus the gold was
    # never computed against.
    chain_seed = _sha256(
        ":".join(
            (
                config.config_digest(),
                history_config.digest(config.config_digest()),
                str(manifest.get("manifest_digest", "")),
            )
        ).encode("utf-8")
    )
    prev_chain = chain_seed

    for index in range(history_config.updates):
        kind, target = _choose(
            prf=prf,
            index=index,
            arms=arms,
            arm_state=arm_state,
            plant_queue=plant_queue,
            planted_at=planted_at,
            live=live,
            update_notes=update_notes,
            history_config=history_config,
        )
        touched: dict[str, str] = {}
        # doc -> its located spans in the bytes this update writes for it.
        placed_now: dict[str, list[DocumentAnnotation]] = {}
        delta_detail: dict[str, str] = {}
        bucket = ""
        corruption_id = ""

        if kind in (DELTA_FRESH_CONFLICT, DELTA_BUCKET_INCREMENT, DELTA_DOMINATED_CLAIM):
            arm = arm_by_bucket[target]
            bucket = arm["bucket"]
            base_doc_text = base_docs[arm["docs"][0]]
            title = _h1_title(base_doc_text)
            section = _section_at(base_doc_text, arm["starts"][arm["docs"][0]])
            values = arm_values.setdefault(
                bucket,
                {
                    "base": arm["base_value"],
                    "fresh": _alternate_value(arm["base_value"], 1.15),
                    "increment": _alternate_value(arm["base_value"], 0.85),
                },
            )
            role = {
                DELTA_FRESH_CONFLICT: "fresh",
                DELTA_BUCKET_INCREMENT: "increment",
                DELTA_DOMINATED_CLAIM: "dominated",
            }[kind]
            surface = values["base"] if kind == DELTA_DOMINATED_CLAIM else values[role]
            doc = f"update_{index:02d}_{role}.md"
            note = (
                f"Interim record filed at update {index} restating "
                f"{arm['label'].lower()} for this patient."
            )
            text, value_start, value_end = _update_doc_text_placed(
                title, section, arm["label"], surface, note
            )
            touched[doc] = text
            update_notes[doc] = note
            predicate, entity = bucket.split(":", 1)
            claim_id = f"claim:{doc}:{predicate}:{entity}:0"
            placed_now[doc] = [
                DocumentAnnotation(
                    doc=doc,
                    update_index=index,
                    start=value_start,
                    end=value_end,
                    kind=ANNOTATION_CLAIM,
                    claim_id=claim_id,
                )
            ]
            update_claims.append(
                {"claim_id": claim_id, "bucket": bucket, "doc": doc, "value": surface}
            )
            if kind == DELTA_FRESH_CONFLICT:
                stamps[claim_id] = {
                    "source_authority": str(history_config.fresh_authority)
                }
                arm_state[bucket] = "fresh"
            elif kind == DELTA_BUCKET_INCREMENT:
                stamps[claim_id] = {
                    "source_authority": str(history_config.increment_authority)
                }
                arm_state[bucket] = "increment"
            else:
                arm_state[bucket] = "dominated"
            delta_detail = {"doc": doc, "value": surface, "role": role}

        elif kind == DELTA_REAFFIRMATION:
            doc = target
            note = (
                f"Reaffirmed without change at update {index}; the recorded "
                "measurement stands as filed."
            )
            head, _, tail = docs[doc].partition("**Interim Update.** ")
            old_note, _, rest = tail.partition("\n")
            touched[doc] = f"{head}**Interim Update.** {note}\n{rest}"
            update_notes[doc] = note
            delta_detail = {"doc": doc, "revision": "note_only"}
            # The note is the FIRST body line of an update document, so every
            # span in it sits after the rewrite and moves by the note's own
            # length change. The shift is the edit this branch just performed.
            shift = len(note) - len(old_note)
            placed_now[doc] = [
                DocumentAnnotation(
                    doc=doc,
                    update_index=index,
                    start=entry.start + shift,
                    end=entry.end + shift,
                    kind=entry.kind,
                    claim_id=entry.claim_id,
                    corruption_id=entry.corruption_id,
                )
                for entry in current.get(doc, ())
            ]

        elif kind == DELTA_PLANT_ERROR:
            corruption_id = target
            site = sites_by_id[corruption_id]
            live[corruption_id] = site
            planted_at[corruption_id] = index
            doc = site["doc"]
            touched[doc], placed_now[doc] = _place_live_sites(
                base_text=base_docs[doc],
                sites=[s for s in live.values() if s["doc"] == doc],
                clean_spans=clean_spans.get(doc, ()),
                doc=doc,
                index=index,
            )
            bucket = _corruption_bucket(site, base_rows, manifest)
            delta_detail = {
                "doc": doc,
                "corruption_class": site["corruption_class"],
                "partition": site["partition"],
            }

        elif kind == DELTA_UNPLANT_ERROR:
            corruption_id = target
            site = sites_by_id[corruption_id]
            del live[corruption_id]
            doc = site["doc"]
            touched[doc], placed_now[doc] = _place_live_sites(
                base_text=base_docs[doc],
                sites=[s for s in live.values() if s["doc"] == doc],
                clean_spans=clean_spans.get(doc, ()),
                doc=doc,
                index=index,
            )
            bucket = _corruption_bucket(site, base_rows, manifest)
            delta_detail = {"doc": doc, "corruption_class": site["corruption_class"]}
        else:  # pragma: no cover - _choose only returns declared kinds
            raise HistoryError(f"unknown delta kind {kind!r}")

        # Only DECLARED touches, and only REAL ones: a document re-rendered to
        # identical bytes is not a touch, and a silent touch is a gold defect.
        touched = {doc: text for doc, text in touched.items() if docs.get(doc) != text}
        if not touched:
            raise HistoryError(
                f"update {index} ({kind}) re-rendered its documents to IDENTICAL "
                "bytes; a declared-but-identical touch would make the touched-set "
                "contract vacuous"
            )
        docs.update(touched)
        # Only the documents this update REALLY wrote get a new annotation set;
        # a declared-but-identical touch was dropped above and must not restate
        # spans that did not move.
        for doc_name in sorted(touched):
            entries = placed_now.get(doc_name, [])
            current[doc_name] = entries
            annotations.extend(entries)

        claims = _gold_claims(base_rows, live, update_claims, stamps)
        new_state = predict_accepted_state(
            claims, order=history_config.precedence_order
        )
        new_inputs = _bucket_claim_ids(claims)
        diff = _expected_diff(
            before_state=state,
            after_state=new_state,
            before_inputs=inputs,
            after_inputs=new_inputs,
        )
        step = HistoryStep(
            index=index,
            delta=UpdateDelta(
                kind=kind,
                label=LIVE_PROOF_SHAPES.get(kind, kind),
                bucket=bucket,
                corruption_id=corruption_id,
                detail=delta_detail,
            ),
            touched_docs=tuple(sorted(touched)),
            doc_texts=dict(touched),
            corpus_digests={
                doc: _sha256(text.encode("utf-8")) for doc, text in sorted(docs.items())
            },
            live_corruptions=tuple(sorted(live)),
            stamps={k: dict(v) for k, v in stamps.items()},
            expected_state=new_state,
            expected_diff=diff,
        )
        step_digest = _sha256(_canonical(step.sealed_payload()))
        chain = _sha256(f"{prev_chain}:{step_digest}".encode("utf-8"))
        from dataclasses import replace as _replace

        step = _replace(
            step,
            step_digest=step_digest,
            prev_chain_digest=prev_chain,
            chain_digest=chain,
        )
        steps.append(step)
        prev_chain = chain
        state, inputs = new_state, new_inputs

    realized = {step.delta.kind for step in steps}
    missing = [k for k in LIVE_PROOF_SHAPES if k not in realized]
    if missing:
        raise HistoryError(
            f"generated history realizes only {sorted(realized)}; the four "
            f"live-proof shapes are mandatory and {missing} never occurred. A "
            "history that skips a shape cannot stand in for the hand-authored "
            "four-update series."
        )
    return History(
        schema=HISTORY_SCHEMA,
        config_digest=config.config_digest(),
        history_digest=history_config.digest(config.config_digest()),
        manifest_digest=str(manifest.get("manifest_digest", "")),
        world_digest=_sha256(world.to_json_bytes()),
        admits=tuple(history_config.admits),
        precedence_order=tuple(history_config.precedence_order),
        base_docs=dict(base_docs),
        steps=tuple(steps),
        chain_seed=chain_seed,
        annotations=tuple(sorted(annotations, key=_annotation_sort_key)),
    )


def _place_live_sites(
    *,
    base_text: str,
    sites: Sequence[Mapping[str, Any]],
    clean_spans: Sequence[tuple[int, int, str]],
    doc: str,
    index: int,
) -> tuple[str, list[DocumentAnnotation]]:
    """Re-render one document at its live site set, keeping every landed span."""
    text, placed, carried = _apply_sites_recorded(base_text, sites, clean_spans)
    entries = [
        DocumentAnnotation(
            doc=doc,
            update_index=index,
            start=start,
            end=end,
            kind=ANNOTATION_CORRUPTION,
            corruption_id=corruption_id,
        )
        for start, end, corruption_id in placed
    ]
    entries.extend(
        DocumentAnnotation(
            doc=doc,
            update_index=index,
            start=start,
            end=end,
            kind=ANNOTATION_CLAIM,
            claim_id=claim_id,
        )
        for start, end, claim_id in carried
    )
    return text, sorted(entries, key=_annotation_sort_key)


def _corruption_bucket(
    site: Mapping[str, Any],
    base_rows: Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
) -> str:
    predicate = _slug(
        next(r["label"] for r in base_rows if r["fact_id"] == site["fact_id"])
    )
    entity = manifest["patients"][site["fact_id"].split(":")[1]]["entity"]
    return f"{predicate}:{entity}"


def _choose(
    *,
    prf: SynthPrf,
    index: int,
    arms: Sequence[Mapping[str, Any]],
    arm_state: Mapping[str, str],
    plant_queue: Sequence[str],
    planted_at: Mapping[str, int],
    live: Mapping[str, Any],
    update_notes: Mapping[str, str],
    history_config: HistoryConfig,
) -> tuple[str, str]:
    """The (kind, target) for one update — every choice PRF-keyed by index.

    The first four updates are a DECLARED prelude in the live proof's own order
    (fresh conflict, higher-precedence increment, no-claim-delta reaffirmation,
    dominated claim) so the four shapes are guaranteed rather than hoped for; the
    remainder is PRF-drawn from whatever is feasible at that point.
    """
    first_arm = arms[0]["bucket"]
    prelude = {
        0: (DELTA_FRESH_CONFLICT, first_arm),
        1: (DELTA_BUCKET_INCREMENT, first_arm),
        3: (DELTA_DOMINATED_CLAIM, first_arm),
    }
    if index in prelude:
        return prelude[index]
    if index == 2:
        return DELTA_REAFFIRMATION, sorted(update_notes)[0]

    feasible: dict[str, list[str]] = {}
    unused = sorted(b for b, s in arm_state.items() if s == "unused")
    if unused:
        feasible[DELTA_FRESH_CONFLICT] = unused
    fresh = sorted(b for b, s in arm_state.items() if s == "fresh")
    if fresh:
        feasible[DELTA_BUCKET_INCREMENT] = fresh
    # A dominated claim is REPEATABLE on a bucket that already has an incumbent:
    # each one is another loser arriving under the active precedence order, which
    # is exactly the step a sound reuse mechanism must be able to skip. Capping it
    # at one per bucket would leave the skippable denominator at 1.
    incumbent = sorted(b for b, s in arm_state.items() if s in ("increment", "dominated"))
    if incumbent:
        feasible[DELTA_DOMINATED_CLAIM] = incumbent
    unplanted = [c for c in plant_queue if c not in planted_at]
    if unplanted:
        feasible[DELTA_PLANT_ERROR] = [unplanted[0]]
    settled = sorted(
        c for c in live if index - planted_at[c] >= history_config.unplant_after
    )
    if settled:
        feasible[DELTA_UNPLANT_ERROR] = settled
    if update_notes:
        feasible[DELTA_REAFFIRMATION] = sorted(update_notes)
    if not feasible:  # pragma: no cover - the corpus always affords something
        raise HistoryError(f"update {index}: no feasible delta remains")
    weights = dict(history_config.kind_weights)
    pool = [
        kind for kind in sorted(feasible) for _ in range(max(1, weights.get(kind, 1)))
    ]
    kind = prf.choice(pool, "history", "kind", str(index))
    target = prf.choice(feasible[kind], "history", "target", str(index))
    return kind, target


def history_from_json(payload: Mapping[str, Any]) -> History:
    """Rehydrate a sealed history (the replay reads gold from disk, never RAM)."""
    if payload.get("schema") != HISTORY_SCHEMA:
        raise HistoryError(f"history schema {payload.get('schema')!r} != {HISTORY_SCHEMA!r}")
    if "base_docs" not in payload or any("doc_texts" not in s for s in payload["steps"]):
        raise HistoryError(
            "history payload carries digests but not document bytes; write it with "
            "history_to_json (the sealed view alone cannot be replayed)"
        )
    steps = tuple(
        HistoryStep(
            index=int(raw["index"]),
            delta=UpdateDelta(
                kind=raw["delta"]["kind"],
                label=raw["delta"]["label"],
                bucket=raw["delta"]["bucket"],
                corruption_id=raw["delta"]["corruption_id"],
                detail=dict(raw["delta"]["detail"]),
            ),
            touched_docs=tuple(raw["touched_docs"]),
            doc_texts=dict(raw["doc_texts"]),
            corpus_digests=dict(raw["corpus_digests"]),
            live_corruptions=tuple(raw["live_corruptions"]),
            stamps={k: dict(v) for k, v in raw["stamps"].items()},
            expected_state=raw["expected_state"],
            expected_diff=raw["expected_diff"],
            step_digest=raw["step_digest"],
            prev_chain_digest=raw["prev_chain_digest"],
            chain_digest=raw["chain_digest"],
        )
        for raw in payload["steps"]
    )
    return History(
        schema=payload["schema"],
        config_digest=payload["config_digest"],
        history_digest=payload["history_digest"],
        manifest_digest=payload["manifest_digest"],
        world_digest=payload["world_digest"],
        admits=tuple(payload["admits"]),
        precedence_order=tuple(payload["precedence_order"]),
        base_docs=dict(payload["base_docs"]),
        steps=steps,
        chain_seed=payload["chain_seed"],
        annotations=tuple(
            annotation_from_json(entry) for entry in payload.get("annotations", ())
        ),
    )


def history_to_json(history: History) -> dict[str, Any]:
    """The on-disk form: the sealed view PLUS the document bytes it replays.

    ``annotations`` ride here and NOT in :meth:`History.as_json`, which is what
    ``history_digest`` and every ``step_digest`` are computed over. Writing them
    beside the sealed view is the whole reason no digest in this module moved.
    """
    payload = history.as_json()
    payload["base_docs"] = dict(history.base_docs)
    payload["annotations"] = [entry.as_json() for entry in history.annotations]
    for raw, step in zip(payload["steps"], history.steps):
        raw["doc_texts"] = dict(step.doc_texts)
    return payload


def taxonomy_census(history: History) -> dict[str, int]:
    counts = {kind: 0 for kind in DELTA_KINDS}
    for step in history.steps:
        counts[step.delta.kind] += 1
    return counts


def corruption_trajectory_scaffold(history: History) -> dict[str, list[int]]:
    """corruption_id -> the update indices at which it is LIVE (from gold alone)."""
    out: dict[str, list[int]] = {}
    for step in history.steps:
        for corruption_id in step.live_corruptions:
            out.setdefault(corruption_id, []).append(step.index)
    return {k: out[k] for k in sorted(out)}


def unsanctioned_history_surfaces(history: History) -> dict[str, str]:
    """The per-step surfaces the leakage scan must find CLEAN.

    Corpus bytes are sanctioned (they ARE the corpus). What must never carry a
    gold token is anything the engine reads that is not corpus-derived, so the
    scan is offered the history's own declaration: the delta program's labels,
    the stamps, and the reason classes. Sealed gold (fact ids, corruption sites)
    lives in the manifest, is never handed to the engine, and is deliberately not
    scanned here — scanning gold against itself would fail by construction and
    teach nothing.
    """
    out: dict[str, str] = {}
    for step in history.steps:
        out[f"history_step_{step.index:02d}_stamps"] = json.dumps(
            {k: dict(v) for k, v in step.stamps.items()}, sort_keys=True
        )
        out[f"history_step_{step.index:02d}_delta_kind"] = json.dumps(
            {"kind": step.delta.kind, "label": step.delta.label}, sort_keys=True
        )
    return out


__all__ = [
    "ANNOTATION_CLAIM",
    "ANNOTATION_CORRUPTION",
    "BASE_UPDATE_INDEX",
    "BASIS_SOLE",
    "BASIS_TIE",
    "DELTA_BUCKET_INCREMENT",
    "DELTA_DOMINATED_CLAIM",
    "DELTA_FRESH_CONFLICT",
    "DELTA_KINDS",
    "DELTA_PLANT_ERROR",
    "DELTA_REAFFIRMATION",
    "DELTA_UNPLANT_ERROR",
    "GOLD_DECISIVE",
    "GOLD_PRECEDENCE_ORDER",
    "HISTORY_SCHEMA",
    "LIVE_PROOF_SHAPES",
    "OUTCOME_CANONICAL",
    "OUTCOME_CONTESTED",
    "REASON_DECISIVE",
    "REASON_DOMINATED",
    "REASON_UNTOUCHED",
    "DocumentAnnotation",
    "GoldClaim",
    "History",
    "HistoryConfig",
    "HistoryError",
    "HistoryStep",
    "UpdateDelta",
    "annotation_from_json",
    "base_claim_spans",
    "build_history",
    "corruption_trajectory_scaffold",
    "gold_canonicalize_value",
    "gold_resolve_bucket",
    "gold_support",
    "history_from_json",
    "history_to_json",
    "predict_accepted_state",
    "taxonomy_census",
    "unsanctioned_history_surfaces",
    "verify_chain",
]
