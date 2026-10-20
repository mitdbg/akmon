"""PACT ``concord-measurement-integrity`` — req.mi.statable-update.

On prose, a changed document can contribute ZERO claim buckets: the model
extracted nothing that maps to a checkable claim. The cone builder used to hand
the engine a bare all-empty change statement, which the engine REFUSES as
ambiguous — it could mean "nothing changed" (permissive, a skip is licensed) or
"the change was not stated" (restrictive, nothing is licensed), and guessing the
permissive reading would licence an unsound skip over a document nobody looked
at.

The engine's refusal is correct and is NOT weakened here. The defect was in the
caller, which assumed every changed document contributes a bucket — true on
structured corpora, false on prose, and it made realistic maintenance
measurement impossible.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import okg.bench.synth.parity as parity
from okg.bench.synth.governed import CorpusCone, _changed_terms_for
from okg.substrate.consistency.possible_effects import ChangedStateTerms
from okg.substrate.errors import SubstrateError


def test_a_document_with_buckets_is_unchanged_from_before() -> None:
    """The existing path must not move: this fix is additive."""
    terms = _changed_terms_for(
        frozenset({("body_weight", "p1"), ("body_height", "p1")}),
        what="document 'note.md'",
    )
    assert terms.buckets == {("body_weight", "p1"), ("body_height", "p1")}
    assert terms.unknown is False
    assert terms.declares_nothing_changed is False


def test_a_zero_bucket_document_is_unknown_not_nothing_changed() -> None:
    """THE fix. A document that changed and resolved to no bucket is a change
    that could not be PLACED — not an assertion that nothing changed."""
    terms = _changed_terms_for(frozenset(), what="document 'clinic_note.md'")
    assert terms.unknown is True
    assert terms.declares_nothing_changed is False, (
        "a zero-bucket document must never spell the PERMISSIVE statement — "
        "that would licence a skip over content nobody placed"
    )
    assert not terms.buckets
    assert "clinic_note.md" in terms.unknown_reason, (
        "the refusal must name the document, or a reader cannot act on it"
    )


def test_the_statement_the_fix_produces_is_accepted_by_the_engine(
    monkeypatch,
) -> None:
    """A later replay must carry the document's restrictive statement through
    the global fact-set rewrite rather than silently licensing every skip."""
    restrictive = _changed_terms_for(
        frozenset(), what="document 'clinic_note.md'"
    )
    cone = CorpusCone(
        phase="u01",
        deployment="proof",
        docs=("clinic_note.md",),
        doc_digests={"clinic_note.md": "new"},
        doc_claim_keys={},
        predicates=(),
        buckets=frozenset(),
        steps={"claim_source:clinic_note.md": object()},
        worklist_seed=("claim_source:clinic_note.md",),
        current_facts={},
        changed_terms={"claim_source:clinic_note.md": restrictive},
        incumbent_selection={},
        corpus_digest="proof",
        candidate_generation_id="candidate",
        dependents={},
    )
    monkeypatch.setattr(parity, "build_corpus_cone", lambda *args, **kwargs: cone)
    state = parity.ModeState(
        published_facts=[object()],
        prior_doc_digests={"clinic_note.md": "old"},
        generation_id="prior",
    )
    updated, stated = parity.build_update_cone(
        corpus_dir=Path("unused"),
        deployment_dir=Path("unused"),
        policies=(),
        checks=(),
        update=parity.RecordedUpdate(index=1, rows=(), digest="proof"),
        facts=[object()],
        state=state,
        mode=parity.MODE_DIFFERENTIAL,
        deployment="proof",
    )
    terms = updated.changed_terms["claim_source:clinic_note.md"]
    assert not stated
    assert terms.unknown is True
    assert terms.declares_nothing_changed is False


def test_the_engine_still_refuses_a_bare_all_empty_statement() -> None:
    """NON-VACUITY. The fix is in the caller; the guard that caught it must
    still fire, or a future caller could reintroduce the ambiguity."""
    with pytest.raises(SubstrateError, match="ambiguous"):
        ChangedStateTerms()
    with pytest.raises(SubstrateError, match="ambiguous"):
        ChangedStateTerms(buckets=frozenset())


def test_the_permissive_statement_still_has_its_own_explicit_spelling() -> None:
    """A caller that really did look and find nothing has a way to say so, and
    it is a DIFFERENT statement from the one above."""
    permissive = ChangedStateTerms.nothing_changed()
    assert permissive.declares_nothing_changed is True
    assert permissive.unknown is False
    restrictive = _changed_terms_for(frozenset(), what="document 'x.md'")
    assert restrictive != permissive, (
        "the two readings must remain distinguishable objects"
    )
