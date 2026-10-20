"""task.attrib.llm-wire witnesses (lane-free, no DB, no model call).

Every witness here exercises the LLM lane's contract WITHOUT a network call or a
Postgres publish, so the failure modes the real run must never silently absorb
are checked on every CI run:

- routing: extract_facts routes on the lane; the deterministic path is untouched;
- canonicalization parity: an LLM-lane unit twin canonicalizes to the same value
  the deterministic lane produces, so it cannot flag as a false positive;
- quote granularity: a quote ambiguous across two sections (or non-verbatim)
  drops THAT fact and records a fact-scope ledger row while the document's other
  facts land; a quote repeated within one section is scoped without complaint;
  with no failure sink the ambiguity still surfaces loudly (never silently
  scoped);
- per-document failure semantics: a document-fatal cause (call failure after
  bounded retries, unparseable output, missing H1) loses the WHOLE document as a
  document-scope ledger row while the run continues; a per-fact quote drop does
  not, and never triggers a document retry;
- the golden diff: the regenerated golden differs from the pre-field one only in
  the DECLARED new fields (digests + the survival block + the FP-split reshape);
- re-score: rebuilding the scorecard from the recorded artifacts alone
  reproduces it byte-for-byte;
- the floor's realized count is exactly the funnel's conditional denominator.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.synth.config import load_config, load_preset
from okg.bench.synth.corruption import Planner
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.goldseal import build_manifest, write_manifest
from okg.bench.synth.llm_extract import (
    LlmExtractionError,
    LlmExtractor,
    QuoteScopeError,
)
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import render_corpus
from okg.bench.synth.resolution import (
    BucketResolution,
    corruption_bucket_map,
    funnel,
    synth_slug,
)
from okg.bench.synth.runner import (
    ENGINE_READS_FILENAME,
    EXTRACTION_FAILURES_FILENAME,
    MANIFEST_FILENAME,
    LlmLane,
    _canonicalize_claim_fact,
    _write_engine_reads,
    _write_llm_artifacts,
    build_synth_scorecard,
    canonicalize_value,
    extract_facts,
    rescore_from_artifacts,
    synth_survival_equivalent,
)
from okg.bench.synth.runner import _subset_world  # noqa: E402
from okg.bench.injection.scoring import FindingView, PipelineState

_SYNTH = Path(__file__).resolve().parents[3] / "src/okg/bench/synth"
_TESTS = Path(__file__).resolve().parent

POLICIES = ("body_weight", "body_height")


# --------------------------------------------------------------------------- #
# a network-free stub extractor                                               #
# --------------------------------------------------------------------------- #
def _stub_extractor(monkeypatch, responder) -> LlmExtractor:
    """An LlmExtractor whose _complete is a pure function of the document text,
    so the lane never touches the network."""
    ex = LlmExtractor(labels=("Body Weight", "Body Height", "Reading"))
    monkeypatch.setattr(ex, "_complete", responder)
    return ex


def _write(corpus: Path, name: str, text: str) -> None:
    corpus.mkdir(parents=True, exist_ok=True)
    (corpus / name).write_text(text, encoding="utf-8")


# --------------------------------------------------------------------------- #
# routing                                                                     #
# --------------------------------------------------------------------------- #
def test_extract_facts_routes_on_the_lane(tmp_path, monkeypatch) -> None:
    """lane=None reads with the deterministic label parser; an LlmLane reads
    with the extractor. Same corpus, two extractors, distinct claims — the knob
    the runner had validated but never read is now the routing decision."""
    corpus = tmp_path / "corpus"
    _write(
        corpus,
        "note.md",
        "# Patient Ada Lovelace\n\n## Measurements\n\nBody Weight: 77.2 kg\n",
    )

    deterministic = extract_facts(corpus, POLICIES, lane=None)
    claims = [f for f in deterministic if f.subtype == "consistency_claim"]
    assert len(claims) == 1
    assert claims[0].attrs["predicate"] == "body_weight"
    assert claims[0].attrs["value"] == "77.2 kg"

    # the LLM lane returns what its extractor says — here a DIFFERENT attribute,
    # proving the deterministic parser was NOT used.
    def responder(_text: str) -> str:
        return '[{"attribute": "Body Height", "value": "164 cm", "quote": "Body Weight: 77.2 kg"}]'

    lane = LlmLane(extractor=_stub_extractor(monkeypatch, responder))
    routed = extract_facts(corpus, POLICIES, lane=lane, phase="clean")
    routed_claims = [f for f in routed if f.subtype == "consistency_claim"]
    assert len(routed_claims) == 1
    assert routed_claims[0].attrs["predicate"] == "body_height"
    # policies flow through the LLM lane identically to the deterministic one
    assert {f.subtype for f in routed} == {"predicate_policy", "consistency_claim"}
    assert sum(1 for f in routed if f.subtype == "predicate_policy") == len(POLICIES)


# --------------------------------------------------------------------------- #
# canonicalization parity (twins do not flag in the LLM lane)                  #
# --------------------------------------------------------------------------- #
def test_llm_lane_canonicalizes_a_unit_twin(tmp_path, monkeypatch) -> None:
    """The LLM lane canonicalizes '82000 g' to '82.0 kg' via the SAME helper the
    deterministic lane uses, so a benign unit twin lands on the same bucket value
    as its kg sibling and cannot become a contested false positive."""
    corpus = tmp_path / "corpus"
    _write(
        corpus,
        "note.md",
        "# Patient Ada Lovelace\n\n## Measurements\n\nBody Weight: 82000 g\n",
    )

    def responder(_text: str) -> str:
        return '[{"attribute": "Body Weight", "value": "82000 g", "quote": "Body Weight: 82000 g"}]'

    lane = LlmLane(extractor=_stub_extractor(monkeypatch, responder))
    facts = [
        f
        for f in extract_facts(corpus, POLICIES, lane=lane, phase="clean")
        if f.subtype == "consistency_claim"
    ]
    assert facts[0].attrs["value"] == "82.0 kg", "LLM lane must canonicalize units"

    # parity is by construction: the shared helper maps both surfaces to one
    # value, so the twin and its sibling never disagree.
    assert canonicalize_value("82000 g") == "82.0 kg"
    assert synth_survival_equivalent("82000 g", "82.0 kg")


def test_canonicalize_helper_is_the_shared_parity_mechanism() -> None:
    """Both lanes route claims through _canonicalize_claim_fact; a non-claim
    fact passes through untouched."""
    from okg.substrate.library.sources.base import NodeFact

    claim = NodeFact(
        "claim:x", "consistency_claim",
        {"predicate": "body_weight", "value": "82000 g"}, {}, {},
    )
    assert _canonicalize_claim_fact(claim).attrs["value"] == "82.0 kg"

    policy = NodeFact("policy:x", "predicate_policy", {"enabled": "true"}, {}, {})
    assert _canonicalize_claim_fact(policy) is policy


# --------------------------------------------------------------------------- #
# quote disambiguation                                                        #
# --------------------------------------------------------------------------- #
_TWO_SECTION_DOC = (
    "# Patient Test\n\n## Alpha\n\nReading: 5 units\n\n## Beta\n\nReading: 5 units\n"
)
_ONE_SECTION_DOC = (
    "# Patient Test\n\n## Alpha\n\nReading: 5 units\nReading: 5 units\n"
)


def test_quote_in_two_sections_without_a_sink_raises() -> None:
    """A quote occurring twice across two sections cannot be scoped. The
    judgment is unchanged — it is never silently keyed to the first section — so
    with no per-fact sink the guard surfaces loudly as a QuoteScopeError."""
    ex = LlmExtractor(labels=("Reading",))
    resp = '[{"attribute": "Reading", "value": "5 units", "quote": "Reading: 5 units"}]'
    with pytest.raises(QuoteScopeError, match="occurs 2 times across 2 sections"):
        list(ex._claims_from_response("note.md", _TWO_SECTION_DOC, resp))


def test_ambiguous_quote_drops_the_fact_and_records_it_with_a_sink() -> None:
    """The per-fact granularity at the extractor seam: given a failure sink, an
    ambiguous quote drops THAT fact (no claim) and appends one row naming the
    quote, while a grounded fact in the same response still lands. The guard's
    judgment is unchanged; only its blast radius is the fact, not the document."""
    ex = LlmExtractor(labels=("Reading", "Body Weight"))
    doc = (
        "# Patient Test\n\n## Alpha\n\nBody Weight: 70.0 kg\nReading: 5 units\n\n"
        "## Beta\n\nReading: 5 units\n"
    )
    resp = json.dumps(
        [
            {"attribute": "Body Weight", "value": "70.0 kg",
             "quote": "Body Weight: 70.0 kg"},
            {"attribute": "Reading", "value": "5 units", "quote": "Reading: 5 units"},
        ]
    )
    fact_failures: list[dict[str, str]] = []
    facts = list(
        ex._claims_from_response("note.md", doc, resp, fact_failures=fact_failures)
    )
    # the grounded fact lands; the ambiguous one does not
    assert [f.attrs["predicate"] for f in facts] == ["body_weight"]
    # exactly one recorded drop, naming the ambiguous quote and its cause
    assert len(fact_failures) == 1
    assert fact_failures[0]["quote"] == "Reading: 5 units"
    assert fact_failures[0]["attribute"] == "Reading"
    assert "occurs 2 times across 2 sections" in fact_failures[0]["cause"]


def test_quote_repeated_within_one_section_is_scoped_not_rejected() -> None:
    """The same quote twice in ONE section is unambiguous (identical scope): it
    is scoped without complaint, and each repeat consumes the next occurrence."""
    ex = LlmExtractor(labels=("Reading",))
    resp = (
        '[{"attribute": "Reading", "value": "5 units", "quote": "Reading: 5 units"},'
        '{"attribute": "Reading", "value": "5 units", "quote": "Reading: 5 units"}]'
    )
    facts = list(ex._claims_from_response("note.md", _ONE_SECTION_DOC, resp))
    assert len(facts) == 2
    assert {f.node_id for f in facts} == {
        "claim:note.md:reading:patient_test:0",
        "claim:note.md:reading:patient_test:1",
    }
    assert all("Alpha" in f.attrs["scope"] for f in facts)


# --------------------------------------------------------------------------- #
# the linked_pair surface survives prose extraction (error-injection redesign) #
# --------------------------------------------------------------------------- #
def test_record_reference_slug_round_trips_through_extraction() -> None:
    """The record_reference surface is extractable IN PRINCIPLE: a corrupted
    "Filed under record reference <slug>." sentence, given the closed vocabulary
    that carries "Record Reference", round-trips the entity-key VALUE verbatim
    through _claims_from_response.

    This is the whole reason the linked_pair corruption moved off age_years: its
    corrupted surface was "The patient is <slug> of age." — semantic nonsense the
    extractor skipped, so on prose the planted sites survived 0/2 and linked_pair
    fired zero findings. A slug is at home in a record-reference frame, so its
    value quotes verbatim and the claim (predicate record_reference, value = the
    entity key the linked policy keys on) actually lands."""
    from okg.bench.synth.corruption import entity_slug

    full_name = "Taryn906 Crooks415"
    slug = entity_slug(full_name)  # == the H1-title entity the buckets key on
    doc = (
        f"# Patient {full_name}\n\n"
        "**Clinic Note.** Follow-up visit note.\n\n"
        "## Patient Details\n\n"
        f"Filed under record reference {slug}.\n"
    )
    # the CLOSED vocabulary the extractor is constrained to; "Record Reference"
    # slugs to the record_reference predicate the gold buckets on.
    ex = LlmExtractor(labels=("Record Reference", "Age Years"))
    response = json.dumps(
        [{"attribute": "Record Reference", "value": slug, "quote": slug}]
    )
    facts = list(ex._claims_from_response("clinic_note.md", doc, response))

    assert len(facts) == 1, "the record-reference sentence must yield one claim"
    (claim,) = facts
    assert claim.subtype == "consistency_claim"
    assert claim.attrs["predicate"] == "record_reference"
    # the VALUE the linked policy keys on survived VERBATIM — this is the slug
    # equal to the target claim's normalized_key, so the link actually forms.
    assert claim.attrs["value"] == slug
    assert claim.attrs["normalized_key"] == slug  # H1 entity == the planted value
    # scope is the canonical section, so it overlaps the birthdate claim's scope
    assert claim.attrs["scope"] == f"Patient {full_name} > Patient Details"


# --------------------------------------------------------------------------- #
# per-document failure semantics                                              #
# --------------------------------------------------------------------------- #
def test_failed_document_is_named_and_the_run_continues(tmp_path, monkeypatch) -> None:
    """A document whose extraction call still fails after bounded retries is
    named in the failure record and its claims never land — but the OTHER
    documents publish and the lane returns. Silent skip and whole-run abort are
    both rejected; its corruption sites fall to missed_at_extraction downstream."""
    corpus = tmp_path / "corpus"
    _write(corpus, "a.md", "# Patient Ann\n\n## M\n\nBody Weight: 70.0 kg\n")
    _write(corpus, "b.md", "# Patient Bob\n\n## M\n\nBody Weight: 80.0 kg\n")
    _write(corpus, "c.md", "# Patient Cora\n\n## M\n\nBody Weight: 90.0 kg\n")

    calls: list[str] = []

    def responder(text: str) -> str:
        calls.append(text)
        if "Patient Bob" in text:
            raise LlmExtractionError("stub: flaky provider on Bob")
        weight = text.split("Body Weight: ")[1].split("\n")[0]
        return f'[{{"attribute": "Body Weight", "value": "{weight}", "quote": "Body Weight: {weight}"}}]'

    lane = LlmLane(extractor=_stub_extractor(monkeypatch, responder), backoff_base_s=0.0)
    lane.sleep = lambda _s: None
    facts = extract_facts(corpus, POLICIES, lane=lane, phase="corrupted")

    claim_docs = {f.attrs["source_id"] for f in facts if f.subtype == "consistency_claim"}
    assert claim_docs == {"a.md", "c.md"}, "failed doc's claims must not land"

    failures = lane.all_failures()
    assert len(failures) == 1
    assert failures[0]["scope"] == "document"  # the whole document was lost
    assert failures[0]["doc"] == "b.md"
    assert failures[0]["attempts"] == 3  # 1 try + 2 retries
    assert "flaky provider on Bob" in failures[0]["cause"]
    # bounded retry actually retried the failing doc 3 times
    assert sum(1 for t in calls if "Patient Bob" in t) == 3
    assert len(lane.responses["corrupted"]) == 2


def test_ambiguous_quote_loses_the_fact_not_the_document(tmp_path, monkeypatch) -> None:
    """WITNESS (1): a single document with one ambiguous-quote fact and N
    grounded facts yields N claims plus one FACT-scope failure row naming the
    quote — and the old whole-document loss is shown GONE. Before this change an
    ambiguous quote raised inside claim generation, the per-document retry
    treated it as a document failure, and all N grounded facts were thrown away
    with it; now only the one fact drops."""
    corpus = tmp_path / "corpus"
    doc = (
        "# Patient Test\n\n"
        "## Alpha\n\nBody Weight: 70.0 kg\nReading: 5 units\n\n"
        "## Beta\n\nBody Height: 160 cm\nReading: 5 units\n"
    )
    _write(corpus, "note.md", doc)

    def responder(_text: str) -> str:
        return json.dumps(
            [
                {"attribute": "Body Weight", "value": "70.0 kg",
                 "quote": "Body Weight: 70.0 kg"},
                {"attribute": "Body Height", "value": "160 cm",
                 "quote": "Body Height: 160 cm"},
                # "Reading: 5 units" sits in BOTH sections — ambiguous, un-scopable
                {"attribute": "Reading", "value": "5 units",
                 "quote": "Reading: 5 units"},
            ]
        )

    lane = LlmLane(extractor=_stub_extractor(monkeypatch, responder), backoff_base_s=0.0)
    lane.sleep = lambda _s: None
    facts = extract_facts(corpus, POLICIES, lane=lane, phase="corrupted")

    claims = [f for f in facts if f.subtype == "consistency_claim"]
    # the N grounded facts LAND; the ambiguous one is absent
    assert {f.attrs["predicate"] for f in claims} == {"body_weight", "body_height"}
    assert all(f.attrs["predicate"] != "reading" for f in claims)

    failures = lane.all_failures()
    # exactly one FACT-scope drop, naming the offending quote — no document loss
    assert len(failures) == 1
    assert failures[0]["scope"] == "fact"
    assert failures[0]["doc"] == "note.md"
    assert failures[0]["quote"] == "Reading: 5 units"
    assert "occurs 2 times across 2 sections" in failures[0]["cause"]
    # OLD behavior gone: the document SUCCEEDED (its response was recorded), it is
    # NOT a document-scope failure, and it was extracted in a single attempt (a
    # per-fact drop is deterministic — it must not trigger a document retry).
    assert [r["doc"] for r in lane.responses["corrupted"]] == ["note.md"]
    assert lane.responses["corrupted"][0]["attempts"] == 1
    assert all(row["scope"] != "document" for row in failures)


@pytest.mark.parametrize(
    "bad_doc, bad_response, cause_fragment",
    [
        # unparseable output — no JSON array in the model's answer
        ("# Patient Bad\n\n## M\n\nBody Weight: 80.0 kg\n",
         "I could not find any facts.", "no JSON array"),
        # missing H1 — a document that can never key a bucket
        ("no title here\n\nBody Weight: 80.0 kg\n",
         '[{"attribute": "Body Weight", "value": "80.0 kg", '
         '"quote": "Body Weight: 80.0 kg"}]', "no H1"),
    ],
)
def test_document_fatal_causes_are_document_scope_rows(
    tmp_path, monkeypatch, bad_doc, bad_response, cause_fragment
) -> None:
    """WITNESS (2): a document-fatal cause (unparseable output, missing H1)
    still loses the WHOLE document — a document-scope row, retried to exhaustion,
    with no claims landing — distinct from the per-fact drop above. A good
    document alongside it publishes normally."""
    corpus = tmp_path / "corpus"
    _write(corpus, "good.md", "# Patient Ann\n\n## M\n\nBody Weight: 70.0 kg\n")
    _write(corpus, "bad.md", bad_doc)

    def responder(text: str) -> str:
        if "Body Weight: 80.0 kg" in text:
            return bad_response
        return '[{"attribute": "Body Weight", "value": "70.0 kg", "quote": "Body Weight: 70.0 kg"}]'

    lane = LlmLane(extractor=_stub_extractor(monkeypatch, responder), backoff_base_s=0.0)
    lane.sleep = lambda _s: None
    facts = extract_facts(corpus, POLICIES, lane=lane, phase="corrupted")

    claim_docs = {f.attrs["source_id"] for f in facts if f.subtype == "consistency_claim"}
    assert claim_docs == {"good.md"}, "the fatal document loses ALL its facts"

    failures = lane.all_failures()
    assert len(failures) == 1
    assert failures[0]["scope"] == "document"
    assert failures[0]["doc"] == "bad.md"
    assert failures[0]["attempts"] == 3  # document-fatal → retried to exhaustion
    assert cause_fragment in failures[0]["cause"]


def test_fact_scope_row_lands_in_the_extraction_failures_artifact(
    tmp_path, monkeypatch
) -> None:
    """WITNESS (3): the per-fact drop reaches extraction-failures.json with the
    distinct row kind. The ledger carries both scopes under one schema, so the
    concentrated-loss diagnosis (which patient lost which single fact) survives
    into the recorded artifact rather than living only in memory."""
    corpus = tmp_path / "corpus"
    doc = (
        "# Patient Test\n\n## Alpha\n\nBody Weight: 70.0 kg\nReading: 5 units\n\n"
        "## Beta\n\nReading: 5 units\n"
    )
    _write(corpus, "note.md", doc)

    def responder(_text: str) -> str:
        return json.dumps(
            [
                {"attribute": "Body Weight", "value": "70.0 kg",
                 "quote": "Body Weight: 70.0 kg"},
                {"attribute": "Reading", "value": "5 units",
                 "quote": "Reading: 5 units"},
            ]
        )

    lane = LlmLane(extractor=_stub_extractor(monkeypatch, responder), backoff_base_s=0.0)
    lane.sleep = lambda _s: None
    extract_facts(corpus, POLICIES, lane=lane, phase="corrupted")

    _write_llm_artifacts(tmp_path, lane)
    payload = json.loads((tmp_path / EXTRACTION_FAILURES_FILENAME).read_text())
    assert payload["schema"] == "okg.synth.extraction-failures.v1"
    fact_rows = [row for row in payload["failures"] if row["scope"] == "fact"]
    assert len(fact_rows) == 1
    assert fact_rows[0]["doc"] == "note.md"
    assert fact_rows[0]["quote"] == "Reading: 5 units"
    assert fact_rows[0]["phase"] == "corrupted"
    assert "occurs 2 times across 2 sections" in fact_rows[0]["cause"]


def test_auth_error_blocks_the_run_never_a_document_failure(tmp_path, monkeypatch) -> None:
    """An auth/credit refusal is BLOCKED, never swallowed into a per-document
    failure — treating an expired key as 'every document failed' would fake a
    plausible under-floor result."""
    from okg.bench.synth.runner import _AuthError

    corpus = tmp_path / "corpus"
    _write(corpus, "a.md", "# Patient Ann\n\n## M\n\nBody Weight: 70.0 kg\n")

    def responder(_text: str) -> str:
        raise RuntimeError("AuthenticationError: no auth credentials found (401)")

    lane = LlmLane(extractor=_stub_extractor(monkeypatch, responder), backoff_base_s=0.0)
    lane.sleep = lambda _s: None
    with pytest.raises(_AuthError, match="BLOCKED"):
        extract_facts(corpus, POLICIES, lane=lane, phase="clean")


# --------------------------------------------------------------------------- #
# the golden diff: adding the config field moves ONLY the digest fields        #
# --------------------------------------------------------------------------- #
def test_golden_differs_only_in_declared_fields() -> None:
    """The LLM-free lane is provably untouched: the regenerated smoke golden
    differs from the committed (pre-field) one only in the DECLARED new fields —
    config_digest and the config-derived manifest_digest (the
    surviving_conflict_floor change) plus the two req.attrib.survival blocks
    (per_class_survival, added; corrupted_unexpected_findings, reshaped from a
    flat list into the extractor/engine split) plus per_kind_detection and the
    extraction COUNT (the error-injection redesign's record_reference fact adds
    losslessly-recovered claims to the deterministic corpus). No OTHER score block
    moved, so the deterministic lane's MEASUREMENTS are unchanged. If the golden
    has not been regenerated yet this witness is vacuous by design; regenerate it
    with
    the fresh-DB e2e run and it becomes the real proof."""
    prefield = json.loads((_TESTS / "golden_smoke_scorecard.prefield.json").read_text())
    live = json.loads((_TESTS / "golden_smoke_scorecard.json").read_text())

    changed = {
        key for key in set(prefield) | set(live) if prefield.get(key) != live.get(key)
    }
    # config_digest MUST move (surviving_conflict_floor is part of the canonical
    # dump); the manifest embeds the config, so manifest_digest moves with it.
    # per_class_survival is the new survival block; corrupted_unexpected_findings
    # changed shape (list -> {extractor_attributed, engine_attributed}). Nothing
    # else — every other measurement block is byte-identical.
    declared = {
        "config_digest",
        "manifest_digest",
        "per_class_survival",
        "corrupted_unexpected_findings",
        # synth-constraint-coverage added the per-kind detection block, and
        # synth-perkind-honesty RESHAPED it: by_kind now carries {end_to_end,
        # conditional} per kind, DERIVED from the funnel states rather than a raw
        # finding-on-bucket count. The smoke corpus exercises only the functional
        # kind and its extraction is lossless, so conditional == end_to_end there;
        # the block still moves (its shape changed), but the funnel block does NOT
        # — smoke has no unresolved_key site whose detection the one predicate
        # flips. No OTHER measurement block moves.
        "per_kind_detection",
        # the error-injection redesign gives every patient a record_reference
        # fact (the surface the linked_pair corruption rewrites so its value
        # survives prose extraction). It renders in the smoke corpus's genres, so
        # the deterministic lane extracts those extra claims: extraction's
        # denominator/recovered rise together (every one is recovered losslessly).
        # It is NOT corrupted in smoke, so no finding/partition/repair block moves
        # — only the claim COUNT does, and the world/trace change moves the digest.
        "extraction",
        # synth-provenance-diversity adds the per-flavor majority mis-repair block.
        # smoke declares no provenance flavor (the axis is disabled by default), so
        # both flavor cells read zero — the block is a new always-present key, not a
        # moved measurement. The config gains the provenance axis (moving the digest,
        # already declared above) but the smoke ingest is byte-for-byte unchanged.
        "majority_flavor",
        # synth-authority-poisoning adds the flip-rate block. smoke declares
        # poison_rate 0 (the default), so it poisons nothing and the block reads a
        # poisoned count and flip rate of zero — a new always-present key, not a moved
        # measurement, exactly like majority_flavor. The config gains poison_rate
        # (moving the digest, already declared above) but the smoke ingest is
        # byte-for-byte unchanged.
        "poisoning",
        # synth-cost-weighted-gold adds the ECONOMICS: qualification (a loud
        # top-level verdict plus its audit block), remaining error cost split by
        # funnel stage, the spent-action ledger, and the sealed cost tables
        # surfaced as provenance. smoke declares the IDENTITY cost model (every
        # error cost 1, nothing mandatory, every action price 0), so the new
        # blocks are today's counts re-expressed — remaining cost equals the
        # unrepaired-corruption count and spent cost is 0. New always-present
        # keys, not moved measurements, exactly as majority_flavor and poisoning
        # were. The config gains the cost model (moving the digest, already
        # declared above); the smoke ingest is byte-for-byte unchanged, which
        # test_cost::test_golden_smoke_identity_is_byte_for_byte_todays_counts
        # pins against the pre-change golden.
        "qualified",
        "qualification",
        "remaining_error_cost",
        "spent_action_cost",
        "cost_model",
    }
    assert changed <= declared, (
        f"an UNDECLARED score block changed, not just the declared fields: "
        f"{sorted(changed - declared)}"
    )
    # the digest moved for the documented reason: the smoke config now carries
    # surviving_conflict_floor.
    assert load_preset("smoke").config_digest() != prefield["config_digest"]
    # the survival block is present and reshaped FP split landed
    assert "per_class_survival" in live
    assert set(live["corrupted_unexpected_findings"]) == {
        "extractor_attributed",
        "engine_attributed",
    }


# --------------------------------------------------------------------------- #
# an offline smoke manifest for the re-score + floor witnesses                 #
# --------------------------------------------------------------------------- #
def _offline_smoke_manifest():
    world_dir = _SYNTH / "world_smoke"
    prov = json.loads((world_dir / "provenance.json").read_text())
    world = world_from_synthea_dir(
        world_dir, reference_date=prov["reference_date"], provenance=prov
    )
    config = load_preset("smoke")
    world = _subset_world(world, config.scale.patients)
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    return build_manifest(config=config, world=world, plan=plan, rendered=rendered), config


def _full_extraction(manifest) -> dict[str, str]:
    """Every trace row present as an occurrence-0 claim (a lossless extraction),
    valued at its rendered surface — enough to drive a non-trivial funnel."""
    patients = manifest["patients"]
    claims: dict[str, str] = {}
    for row in manifest["trace"]:
        entity = patients[row["fact_id"].split(":")[1]]["entity"]
        pred = synth_slug(row["predicate_label"])
        claims[f"claim:{row['doc']}:{pred}:{entity}:0"] = row["surface_value"]
    return claims


# --------------------------------------------------------------------------- #
# re-score from artifacts reproduces every block                              #
# --------------------------------------------------------------------------- #
def test_rescore_from_artifacts_reproduces_the_scorecard(tmp_path) -> None:
    """Given the sealed manifest and the pinned engine-side snapshot, re-score
    rebuilds every scorecard block byte-for-byte — no model call, no live read."""
    manifest, config = _offline_smoke_manifest()
    claims = _full_extraction(manifest)

    # a fabricated but self-consistent engine surface that exercises findings,
    # candidates and a resolution row through the serializer.
    a_bucket = next(iter(corruption_bucket_map(manifest).values()))

    def _bucket_of(cid: str) -> str:
        parts = cid.split(":")
        return f"{parts[2]}:{parts[3]}"

    members = frozenset(cid for cid in claims if _bucket_of(cid) == a_bucket)
    findings = (FindingView(finding_id="f1", channel="definite", bucket_key=a_bucket),)
    corrupted_state = PipelineState(
        claims=claims, candidates=frozenset({members}), findings=findings
    )
    clean_state = PipelineState(claims=claims, candidates=frozenset(), findings=())
    routed = frozenset({a_bucket})
    predicate, key = a_bucket.split(":", 1)
    resolutions = (
        BucketResolution(
            predicate=predicate, bucket_key=key, outcome="contested", basis="tie",
            winner_claim_id="", ranked_claim_ids=tuple(sorted(members)), winner_value=None,
        ),
    )

    core = build_synth_scorecard(
        manifest=manifest, clean_state=clean_state, corrupted_state=corrupted_state,
        routed=routed, config=config, resolutions=resolutions,
    )

    write_manifest(manifest, tmp_path / MANIFEST_FILENAME)
    _write_engine_reads(
        tmp_path, clean_state=clean_state, corrupted_state=corrupted_state,
        routed=routed, resolutions=resolutions,
    )
    assert (tmp_path / ENGINE_READS_FILENAME).exists()

    rescored = rescore_from_artifacts(tmp_path)
    assert rescored == core, "re-score must reproduce the scorecard from artifacts alone"


# --------------------------------------------------------------------------- #
# the floor's realized count == the funnel conditional denominator            #
# --------------------------------------------------------------------------- #
def test_realized_floor_count_is_the_funnel_conditional_denominator() -> None:
    """The runner asserts the floor against the funnel's conditional-detection
    denominator; this pins that number to the survivor states within the
    cross_doc / internal_only partitions, so 'realized surviving conflicts' and
    the asserted quantity are the same thing."""
    manifest, _config = _offline_smoke_manifest()
    claims = _full_extraction(manifest)
    stages = funnel(
        manifest=manifest, extracted_claims=claims, resolutions=(),
        findings=(), equivalent=synth_survival_equivalent,
    )
    survivor = {"not_detected", "detected_only", "mis_repaired", "repaired"}
    conditional_partitions = {"cross_doc", "internal_only"}
    realized = sum(
        1
        for row in stages["rows"]
        if row["partition"] in conditional_partitions and row["state"] in survivor
    )
    # NON-VACUITY FIRST: 0 == 0 is the failure mode this identity would
    # otherwise report as agreement, and it is exactly what a run whose corpus
    # produced no surviving conflict would show — the identity holds and says
    # nothing.
    assert realized > 0, "no surviving conflict: the identity would hold at zero"
    assert realized == stages["detection_recall"]["conditional"]["denominator"]
    # the conditional denominator conditions on something: it cannot exceed the
    # corruption set it is drawn from.
    assert realized <= stages["total"]


def test_prose_preset_declares_a_positive_floor() -> None:
    assert load_preset("prose_llm").surviving_conflict_floor == 30
    # the field is strict: a negative floor is a typed rejection
    from okg.bench.synth.config import ConfigError

    with pytest.raises(ConfigError):
        load_config({"seed": "x", "world_path": "world_smoke", "surviving_conflict_floor": -1})
