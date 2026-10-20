"""One spec parser, one identity, and an identity that separates models (#797).

These are ALGEBRAIC proofs, not example checks, and that is deliberate. The
defect they guard is that the previous derivation —
``getattr(embedder, "model_id", f"{type(embedder).__name__}:{dim}")`` —
occurred in exactly ONE place and was still wrong: neither shipped embedder
declared ``model_id``, so the fallback always fired and every
sentence-transformer at the substrate width collapsed to the single string
``SentenceTransformerEmbedder:2560``. A textual "occurs once" check was green
against that code. INJECTIVITY is the property that matters, and only a test
over the enumerated spec set can express it.

No database: these are pure functions over specs and embedder objects, so the
`fast` lane (`not postgres`) picks the file up with no lane registration.
"""
from __future__ import annotations

import pytest

from okg.substrate.embeddings import (
    HashEmbedder,
    assert_embedder_matches_substrate_width,
    embedder_model_id,
    model_id_for_spec,
)
from okg.substrate.embeddings.identity import EMBEDDING_DIMENSION
from okg.substrate.errors import SubstrateError


#: Every spec shape the substrate accepts, with TWO distinct
#: sentence-transformer models — the pair the old class-derived identity
#: could not tell apart, and the reason this list is not just ["hash"].
ACCEPTED_SPECS = (
    "hash",
    "sentence-transformers:all-mpnet-base-v2",
    "sentence-transformers:bge-large-en-v1.5",
    "sentence-transformers:gte-Qwen2-7B-instruct",
)


def test_identities_are_pairwise_distinct_across_accepted_specs() -> None:
    """Injectivity over the accepted spec set, not over one lucky pair."""
    ids = {spec: model_id_for_spec(spec) for spec in ACCEPTED_SPECS}
    assert len(set(ids.values())) == len(ACCEPTED_SPECS), (
        "embedder identities collide, so semantic search cannot tell two "
        f"models apart: {ids}"
    )
    # The specific collision that motivated this contract: two different
    # sentence-transformer models at the same (substrate) width. Asserted
    # explicitly so a future refactor back to a class-derived id fails HERE
    # with a legible message rather than only in the aggregate above.
    st_specs = [s for s in ACCEPTED_SPECS if s.startswith("sentence-")]
    assert len(st_specs) >= 2, "the fixture must contain the colliding case"
    assert len({model_id_for_spec(s) for s in st_specs}) == len(st_specs)
    # And the identity must not be the class name, which is what collided.
    assert all(
        "SentenceTransformerEmbedder" not in ids[s] for s in st_specs
    ), f"identity is class-derived and therefore not injective: {ids}"


def test_spec_only_and_instance_derivations_agree_for_every_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`model_id_for_spec(s)` == `embedder_model_id(build_embedder(s))`.

    The spec-only path exists so an unembedded or skewed corpus can be refused
    BEFORE a sentence-transformer is loaded from disk. If the two derivations
    could disagree, a probe done before construction would be meaningless — so
    agreement is a tested property, not an assumption.

    EVERY accepted spec is exercised, including the sentence-transformer ones.
    Those cannot download weights in a unit test, so only the MODEL LOAD is
    stubbed out — `build_embedder`, the `model_id` property, and
    `embedder_model_id` all run for real. Stubbing the load rather than skipping
    the spec matters: `model_id` is where a class-derived (colliding) identity
    would reappear, and an arm that never executes cannot catch that.
    """
    from okg.substrate.embeddings import sentence_transformers as st_mod
    from okg.substrate.embeddings.identity import build_embedder

    def _no_download(self, model_name="all-MiniLM-L6-v2", *, device=None):
        # Mirrors what the real __init__ leaves behind, minus the disk load.
        self._model_name = model_name
        self._model = None
        self._dimension = EMBEDDING_DIMENSION

    monkeypatch.setattr(
        st_mod.SentenceTransformerEmbedder, "__init__", _no_download,
    )

    for spec in ACCEPTED_SPECS:
        built = build_embedder(spec)
        assert model_id_for_spec(spec) == embedder_model_id(built), spec

    # ...and the identities are still pairwise distinct when derived from the
    # INSTANCES rather than the specs, which is the direction the writer uses.
    instance_ids = {
        embedder_model_id(build_embedder(spec)) for spec in ACCEPTED_SPECS
    }
    assert len(instance_ids) == len(ACCEPTED_SPECS)
    assert embedder_model_id(HashEmbedder()) == f"hash:{EMBEDDING_DIMENSION}"


def test_embedder_without_declared_identity_raises() -> None:
    """No class-name fallback: an unattributable embedder is refused.

    A fallback is what made the old derivation non-injective, so the absence of
    one is the fix. An embedder that cannot say which model it is cannot be
    checked against the corpus, and ranking anyway is the silent-wrong-answer
    this contract exists to prevent.
    """
    class Anonymous:
        dimension = EMBEDDING_DIMENSION

        def embed(self, text: str) -> list[float]:  # pragma: no cover
            return [0.0] * EMBEDDING_DIMENSION

    with pytest.raises(SubstrateError) as exc:
        embedder_model_id(Anonymous())
    assert exc.value.code == "embedder_identity_undeclared"
    # The message must not offer the class name as an identity.
    assert "Anonymous" in str(exc.value)
    assert "model_id" in (exc.value.fix_hint or "")


@pytest.mark.parametrize(
    "bad_spec",
    ["", "   ", "nope", "hash:2560", "sentence-transformers:", None, 7],
)
def test_invalid_specs_are_refused_with_a_registered_code(bad_spec) -> None:
    with pytest.raises(SubstrateError) as exc:
        model_id_for_spec(bad_spec)  # type: ignore[arg-type]
    assert exc.value.code == "embedder_spec_invalid"


def test_dimension_guard_compares_against_the_substrate_width() -> None:
    """The guard checks the COLUMN, not the embedder against itself.

    The guard this replaced was `len(embedder.embed(q)) != embedder.dimension`
    — a tautology over one object that cannot fail for any correctly
    implemented embedder, and that said nothing about whether the vector could
    be compared to the stored ones. Extracted as a pure function precisely so
    this is provable without a session or a pinned generation.
    """
    # A correctly implemented embedder at the WRONG width: the old guard passed
    # this (its output length equals its own declared dimension); the corrected
    # one refuses it.
    narrow = HashEmbedder(dimension=384)
    assert len(narrow.embed("x")) == narrow.dimension, (
        "precondition: the old tautological guard would pass this embedder"
    )
    with pytest.raises(SubstrateError) as exc:
        assert_embedder_matches_substrate_width(narrow)
    assert exc.value.code == "embedder_dim_mismatch"
    # Both widths are named, so an operator learns which to change.
    assert exc.value.data["embedder_dimension"] == 384
    assert exc.value.data["substrate_dimension"] == EMBEDDING_DIMENSION

    # And the matching case passes.
    assert_embedder_matches_substrate_width(HashEmbedder())


def test_the_substrate_width_has_one_definition() -> None:
    """The runtime module RE-EXPORTS the width; it does not hold a copy.

    Two copies of one schema fact is the same drift class this module exists to
    end, and it also meant the read side had to import a schema constant from a
    DBOS workflow module.
    """
    from okg.substrate.embeddings import identity
    from okg.substrate.runtime import embed_workflow

    assert embed_workflow.EMBEDDING_DIMENSION is identity.EMBEDDING_DIMENSION


def test_the_spec_parser_has_one_implementation() -> None:
    """Both former parsers delegate to the shared one.

    Asserted structurally rather than textually: the check is that calling
    either former entry point produces an embedder whose identity the shared
    derivation recognises, and that both raise the SAME registered code on the
    same bad spec. A grep for the old expression would break on reformatting
    and would also match the PACT that quotes it.
    """
    from okg.substrate.cli.main import _build_embedder
    from okg.substrate.runtime.embed_workflow import _build_embedder_from_spec

    for entry in (_build_embedder, _build_embedder_from_spec):
        built = entry("hash")
        assert embedder_model_id(built) == f"hash:{EMBEDDING_DIMENSION}"
        with pytest.raises(SubstrateError) as exc:
            entry("definitely-not-a-spec")
        assert exc.value.code == "embedder_spec_invalid"
