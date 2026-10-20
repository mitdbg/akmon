"""LLM extraction: the shape contract and the ways it must fail LOUDLY.

Every failure here has the same worst case — returning zero facts without
raising. That would shrink the recall denominator and report a clean-looking
score for a broken run, so each is an error rather than an empty result.

These witnesses use a stub response and make NO network calls: the failure
modes are exactly what a live run must never silently absorb, so they need to
be exercised on every CI run rather than only when credentials exist.
"""

from __future__ import annotations

import pytest

from okg.bench.synth import llm_extract
from okg.bench.synth.llm_extract import (
    LlmExtractionError,
    LlmExtractor,
    QuoteScopeError,
    pack_labels,
)

DOC = """# Patient Ada Lovelace

## Measurements

Body Weight: 77.2 kg
Body Height: 164 cm
"""

LABELS = ("Body Weight", "Body Height", "Care Provider")


class _Msg:
    def __init__(self, content, reasoning=""):
        self.content = content
        self.reasoning_content = reasoning


class _Choice:
    def __init__(self, content, finish, reasoning=""):
        self.message = _Msg(content, reasoning)
        self.finish_reason = finish


class _Resp:
    def __init__(self, content, finish="stop", reasoning=""):
        self.choices = [_Choice(content, finish, reasoning)]
        self.usage = None


def _extractor(monkeypatch, response):
    ex = LlmExtractor(labels=LABELS)
    monkeypatch.setattr(
        ex, "_complete",
        lambda text: llm_extract._content(response, max_tokens=ex.max_tokens),
    )
    return ex


# --------------------------------------------------------------------------- #
# the shape contract                                                           #
# --------------------------------------------------------------------------- #
def test_claims_match_the_deterministic_contract(monkeypatch) -> None:
    """Buckets form on (predicate, normalized_key) and the gold keys
    corruptions the same way, so a free-form attribute would key a bucket the
    gold cannot find and recall would fall silently."""
    body = (
        '[{"attribute": "Body Weight", "value": "77.2 kg", '
        '"quote": "Body Weight: 77.2 kg"}]'
    )
    ex = _extractor(monkeypatch, _Resp(body))
    facts = list(ex.claims("note.md", DOC))

    assert len(facts) == 1
    attrs = facts[0].attrs
    assert attrs["predicate"] == "body_weight"        # slug of the LABEL
    assert attrs["normalized_key"] == "patient_ada_lovelace"  # H1 entity
    assert attrs["value"] == "77.2 kg"
    assert attrs["scope_state"] == "known"
    assert attrs["source_id"] == "note.md"
    # scope comes from the quote's OFFSET, so it is the real section path
    assert "Measurements" in attrs["scope"]
    assert facts[0].node_id == "claim:note.md:body_weight:patient_ada_lovelace:0"


def test_repeated_predicate_gets_distinct_occurrence_ids(monkeypatch) -> None:
    """Two claims on one predicate are the CONFLICT case; colliding node_ids
    would collapse them and delete the conflict."""
    body = (
        '[{"attribute": "Body Weight", "value": "77.2 kg", '
        '"quote": "Body Weight: 77.2 kg"},'
        '{"attribute": "Body Weight", "value": "164 cm", '
        '"quote": "Body Height: 164 cm"}]'
    )
    ex = _extractor(monkeypatch, _Resp(body))
    ids = [f.node_id for f in ex.claims("note.md", DOC)]
    assert len(ids) == len(set(ids)) == 2


# --------------------------------------------------------------------------- #
# the loud failures                                                            #
# --------------------------------------------------------------------------- #
def test_truncated_mid_answer_names_the_token_ceiling(monkeypatch) -> None:
    """The defect this caught in validation: at max_tokens=4000 two documents
    truncated mid-array, and the error said 'no JSON array in model output' --
    pointing at a malformed model rather than a short budget."""
    partial = '[\n  {"attribute": "Body Weight", "value": "77.2 kg", "quo'
    with pytest.raises(LlmExtractionError, match="token ceiling mid-answer"):
        _extractor(monkeypatch, _Resp(partial, finish="length"))._complete(DOC)


def test_budget_exhausted_before_answering_names_the_token_ceiling(
    monkeypatch,
) -> None:
    """The other exhaustion mode: content empty, answer stranded in
    reasoning_content, and NO exception from the client."""
    with pytest.raises(LlmExtractionError, match="token ceiling before answering"):
        _extractor(
            monkeypatch, _Resp("", finish="length", reasoning="x" * 500)
        )._complete(DOC)


def test_empty_content_is_never_scored_as_no_facts(monkeypatch) -> None:
    with pytest.raises(LlmExtractionError, match="EMPTY content"):
        _extractor(monkeypatch, _Resp("", finish="stop"))._complete(DOC)


def test_unparseable_output_raises(monkeypatch) -> None:
    with pytest.raises(LlmExtractionError, match="no JSON array"):
        list(_extractor(monkeypatch, _Resp("I could not find any facts."))
             .claims("note.md", DOC))


def test_a_quote_that_is_not_verbatim_drops_the_fact(monkeypatch) -> None:
    """Grounding: the quote must appear character-for-character. A value the
    model altered or invented has no locatable scope, so accepting it would put
    a fabricated claim into the graph — the FACT is dropped and recorded, never
    the document. Passing no failure sink surfaces the same judgment loudly
    (never a silent drop)."""
    body = (
        '[{"attribute": "Body Weight", "value": "80 kg", '
        '"quote": "Body Weight: 80 kg"}]'
    )
    # with a sink: the fact drops and a per-fact row names the offending quote
    failures: list[dict[str, str]] = []
    facts = list(
        _extractor(monkeypatch, _Resp(body)).claims(
            "note.md", DOC, fact_failures=failures
        )
    )
    assert facts == [], "a non-verbatim quote must not yield a claim"
    assert len(failures) == 1
    assert failures[0]["quote"] == "Body Weight: 80 kg"
    assert failures[0]["attribute"] == "Body Weight"
    assert "not a verbatim" in failures[0]["cause"]

    # the judgment is a per-FACT QuoteScopeError, and without a sink it re-raises
    with pytest.raises(QuoteScopeError, match="not a verbatim"):
        list(_extractor(monkeypatch, _Resp(body)).claims("note.md", DOC))


def test_off_vocabulary_attributes_are_dropped_but_COUNTED(monkeypatch) -> None:
    """Dropping is correct — the attribute cannot key a gold bucket — but a
    silent drop reads as 'the model found fewer facts' rather than 'it named
    them differently'."""
    body = (
        '[{"attribute": "Body Weight", "value": "77.2 kg", '
        '"quote": "Body Weight: 77.2 kg"},'
        '{"attribute": "Mood", "value": "cheerful", "quote": "Body Height"}]'
    )
    ex = _extractor(monkeypatch, _Resp(body))
    facts = list(ex.claims("note.md", DOC))
    assert len(facts) == 1
    assert ex.cost.off_vocabulary == 1


def test_missing_h1_raises_before_any_model_call(monkeypatch) -> None:
    """The H1 guard must fire BEFORE the completion call: a doc that can never
    yield claims must not cost a model call. This regressed once — claims()
    fetched the response first, so this test made a REAL OpenRouter call
    (401 in CI, silent spend anywhere with credentials) — so the mock now
    asserts the network is never touched, not merely that the guard fires."""
    ex = LlmExtractor(labels=LABELS)

    def _no_network(text: str) -> str:
        raise AssertionError("claims() called the model before the H1 guard")

    monkeypatch.setattr(ex, "_complete", _no_network)
    with pytest.raises(LlmExtractionError, match="no H1"):
        list(ex.claims("note.md", "no title here"))


# --------------------------------------------------------------------------- #
# vocabulary + cost                                                            #
# --------------------------------------------------------------------------- #
def test_pack_labels_expands_prefix_rules() -> None:
    """Prefix kinds must expand to the SAME label _label_for builds, or the
    model's choices slug to predicates the gold never uses."""
    pack = {"labels": {"care_provider": "Care Provider",
                       "medication:": "Medication"}}
    labels = pack_labels(pack, {"care_provider", "medication:metformin"})
    assert "Care Provider" in labels
    assert "Medication Metformin" in labels


def test_cost_reports_reasoning_share() -> None:
    """A run must be costable from a small one; reasoning share is the term
    that makes a naive estimate wrong (measured 66-80% of completion tokens)."""
    cost = llm_extract.ExtractionCost(
        calls=2, prompt_tokens=100, completion_tokens=1000,
        reasoning_tokens=800, cost_usd=0.02,
    )
    assert cost.as_dict()["reasoning_share"] == 0.8
