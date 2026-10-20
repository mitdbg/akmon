"""Refusal DISPOSITION and refusal SHAPE for semantic search (#797).

Two things are proven here, both without a database — deliberately. A
`build_server` test is unnecessary because both properties are pure
functions of an exception or of a readiness measurement, so neither needs a
corpus:

  * `assert_rankable` REFUSES rather than ranking, and distinguishes the three
    provenance states (matching / known-different / unknown).
  * a corpus refusal is converted into a TYPED operator refusal carrying
    `fallback_method` and a `capability` block, matching the shape the sibling
    `semantic_search_unavailable` refusal already uses. An agent that cannot
    parse a refusal reads it as "the graph has nothing".

The behaviour against real published rows is proven in
`test_semantic_coverage_disclosure.py` and `test_semantic_model_identity.py`.
"""
from __future__ import annotations

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.semantic_readiness import (
    SEMANTIC_CORPUS_REFUSAL_CODES,
    UNKNOWN_PROVENANCE,
    SemanticReadiness,
    assert_rankable,
    probe_semantic_readiness,
    resolve_node_segment,
    semantic_corpus_refusal_error,
)


QUERY_MODEL = "hash:2560"


def _readiness(**kw) -> SemanticReadiness:
    base = {"embedded": 0, "unembedded": 0, "models": {}}
    base.update(kw)
    return SemanticReadiness(**base)  # type: ignore[arg-type]


# ---------- disposition ----------

def test_zero_coverage_refuses_rather_than_reporting_an_empty_page() -> None:
    """An unembedded corpus is a refusal, not zero results.

    This is the DEFAULT state of every in-repo deployment, so returning an
    empty page here teaches an analyst that the graph lacks content it holds.
    """
    with pytest.raises(SubstrateError) as exc:
        assert_rankable(
            _readiness(embedded=0, unembedded=42),
            query_model_id=QUERY_MODEL,
        )
    assert exc.value.code == "semantic_index_empty"
    # The count is REPORTED, so the operator can see the corpus has text.
    assert exc.value.data["unembedded_nodes"] == 42
    # And the hint names the thing that populates vectors.
    assert "okg_embed_batch" in (exc.value.fix_hint or "")


def test_matching_model_is_rankable() -> None:
    """The control: a corpus written by the querying model ranks normally.

    Without this, every other assertion here could hold against code that
    refuses unconditionally.
    """
    assert_rankable(
        _readiness(embedded=5, models={QUERY_MODEL: 5}),
        query_model_id=QUERY_MODEL,
    ) is None


def test_known_different_model_is_refused_and_named() -> None:
    with pytest.raises(SubstrateError) as exc:
        assert_rankable(
            _readiness(
                embedded=7,
                models={"sentence-transformers:bge-large-en-v1.5": 7},
            ),
            query_model_id=QUERY_MODEL,
        )
    assert exc.value.code == "embedding_model_skew"
    # Both sides named, with row counts: the operator learns WHICH embedder to
    # configure, not merely that something mismatched.
    assert exc.value.data["query_model_id"] == QUERY_MODEL
    assert exc.value.data["corpus_models"] == {
        "sentence-transformers:bge-large-en-v1.5": 7,
    }
    assert "bge-large-en-v1.5" in str(exc.value)
    assert "7 rows" in str(exc.value)


def test_unknown_provenance_is_refused_by_default_not_read_as_a_match() -> None:
    """NULL `embedding_model` is a THIRD state, never folded into "matches".

    The column is nullable on rows embedded before it existed, so a row can
    carry a real vector and an unknown provenance. Treating that as agreement
    would report a verified model having verified nothing, on exactly the
    legacy rows most likely to carry a stale one.
    """
    readiness = _readiness(embedded=3, models={UNKNOWN_PROVENANCE: 3})
    with pytest.raises(SubstrateError) as exc:
        assert_rankable(readiness, query_model_id=QUERY_MODEL)
    assert exc.value.code == "embedding_model_skew"
    assert exc.value.data["unknown_provenance_rows"] == 3
    assert "unknown" in str(exc.value)

    # The opt-in ADMITS them (it does not merely mute a message).
    assert_rankable(
        readiness,
        query_model_id=QUERY_MODEL,
        allow_unverified_provenance=True,
    )


def test_optin_admits_unknown_but_not_a_known_different_model() -> None:
    """"I accept unverified provenance" is not "I accept a model I know is wrong".

    Without this, the escape hatch would be a blanket disable of the check.
    """
    with pytest.raises(SubstrateError) as exc:
        assert_rankable(
            _readiness(
                embedded=9,
                models={
                    UNKNOWN_PROVENANCE: 4,
                    "sentence-transformers:gte-Qwen2-7B-instruct": 5,
                },
            ),
            query_model_id=QUERY_MODEL,
            allow_unverified_provenance=True,
        )
    assert exc.value.code == "embedding_model_skew"
    assert "gte-Qwen2-7B-instruct" in str(exc.value)


def test_a_mixed_corpus_containing_the_query_model_still_refuses() -> None:
    """Partial agreement is not agreement.

    A corpus where SOME rows match cannot be ranked as a whole: the distances
    of the non-matching rows are noise, and they compete for rank with the
    real ones.
    """
    with pytest.raises(SubstrateError) as exc:
        assert_rankable(
            _readiness(
                embedded=10,
                models={QUERY_MODEL: 6, "sentence-transformers:other": 4},
            ),
            query_model_id=QUERY_MODEL,
        )
    assert exc.value.code == "embedding_model_skew"


def test_scoped_zero_coverage_names_the_scope() -> None:
    """Under a profile the refusal says the SCOPE holds no vectors.

    Whole-graph phrasing would send the operator looking for a global problem
    when the ranked subtypes are the empty ones.
    """
    with pytest.raises(SubstrateError) as exc:
        assert_rankable(
            _readiness(embedded=0, unembedded=3),
            query_model_id=QUERY_MODEL,
            scoped_subtypes=["chunk", "entity"],
        )
    assert exc.value.code == "semantic_index_empty"
    assert exc.value.data["scoped_subtypes"] == ["chunk", "entity"]
    assert "search profile" in str(exc.value)


# ---------- unmeasurable is a refusal, not a null ----------

class _RaisingConn:
    """A connection whose cursor raises, standing in for an unresolvable pin."""

    def cursor(self, *a, **k):  # noqa: D102
        import psycopg
        raise psycopg.OperationalError("segment registry unavailable")


def test_unmeasurable_coverage_refuses_rather_than_ranking() -> None:
    """"Coverage could not be verified" must not look like "coverage is fine".

    Ranking with a null disclosure is the fail-open the requirement forbids:
    an absent or null measurement reads as a healthy corpus. The state is
    INDUCED here (the segment lookup raises) so this proves the DISPOSITION;
    it is not a claim that a healthy deployment reaches it — see
    `test_a_missing_lexical_index_does_not_deny_a_working_search` in the
    coverage suite for that direction.
    """
    with pytest.raises(SubstrateError) as exc:
        resolve_node_segment(_RaisingConn())  # type: ignore[arg-type]
    assert exc.value.code == "semantic_coverage_unmeasurable"
    assert "Refusing rather than ranking" in str(exc.value)

    with pytest.raises(SubstrateError) as exc2:
        probe_semantic_readiness(
            _RaisingConn(), object(),  # type: ignore[arg-type]
        )
    assert exc2.value.code == "semantic_coverage_unmeasurable"


def test_an_unregistered_segment_refuses_rather_than_counting_zero() -> None:
    """A missing segment must not read as "no vectors".

    If an unresolvable segment returned 0 it would surface as
    `semantic_index_empty` — telling the operator to run the embed
    workflow when the real problem is that the graph could not be located.
    """
    class _EmptyConn:
        def cursor(self, *a, **k):
            class _C:
                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    return False

                def execute(self, *a, **k):
                    return None

                def fetchone(self):
                    return None
            return _C()

    with pytest.raises(SubstrateError) as exc:
        resolve_node_segment(_EmptyConn())  # type: ignore[arg-type]
    assert exc.value.code == "semantic_coverage_unmeasurable"


# ---------- the MCP surface gets a TYPED refusal ----------

@pytest.mark.parametrize("code", sorted(SEMANTIC_CORPUS_REFUSAL_CODES))
def test_every_corpus_refusal_becomes_a_typed_operator_refusal(code) -> None:
    """An agent must be able to READ the refusal, not just receive it."""
    exc = SubstrateError("nope", data={"embedded_nodes": 0}, fix_hint="do X")
    exc.code = code
    error = semantic_corpus_refusal_error(exc, method="semantic")
    assert error is not None, f"{code} must be classified as a corpus refusal"
    assert error["code"] == code
    # The same shape the sibling `semantic_search_unavailable` refusal uses.
    assert error["fallback_method"] == "lexical"
    assert error["capability"]["method"] == "semantic"
    assert error["capability"]["refusal_reason"] == code
    assert error["fix_hint"] == "do X"
    # Measured fields are forwarded, so the agent learns how much coverage.
    assert error["capability"]["embedded_nodes"] == 0


def test_the_provenance_optin_is_refused_on_a_non_semantic_method() -> None:
    """The flag is never accepted-and-discarded, on any surface.

    A caller who passes it on a lexical search believes provenance checking was
    loosened. Silently returning a normal page teaches them nothing about which
    surface honoured it -- the argument-surface form of absence dressed as
    agreement. `search()` owns this refusal so the CLI and the MCP tool cannot
    diverge; the MCP tool therefore forwards the flag UNCONDITIONALLY rather
    than gating on `method == "semantic"`.

    Pure argument validation, so it needs no connection or session.
    """
    from okg.substrate.mcp.search import search

    for method in ("lexical", "alias"):
        with pytest.raises(ValueError, match="only to method='semantic'"):
            search(
                None, None, "q",  # type: ignore[arg-type]
                method=method, allow_unverified_provenance=True,
            )
    # The default stays silent, so an ordinary lexical call is unaffected.
    with pytest.raises(Exception) as exc:
        search(
            None, None, "q",  # type: ignore[arg-type]
            method="lexical", allow_unverified_provenance=False,
        )
    assert "only to method='semantic'" not in str(exc.value)


def test_an_unrelated_error_is_not_converted_into_a_capability_report() -> None:
    """Fail-open guard: only the known corpus refusals are converted.

    Blanket conversion would turn a genuine bug into a reassuring
    "semantic unavailable, use lexical" message.
    """
    exc = SubstrateError("something else broke")
    exc.code = "narrowing_violation"
    assert semantic_corpus_refusal_error(exc, method="semantic") is None
    # And a non-semantic method never routes here at all.
    skew = SubstrateError("skew")
    skew.code = "embedding_model_skew"
    assert semantic_corpus_refusal_error(skew, method="lexical") is None
