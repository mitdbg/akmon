"""SentenceTransformerEmbedder — gated on the optional dep."""
from __future__ import annotations

import pytest

from okg.substrate.embeddings import SentenceTransformerEmbedder
from okg.substrate.errors import EmbedderModelMissing


def _has_sentence_transformers() -> bool:
    try:
        import sentence_transformers  # noqa: F401
        return True
    except ImportError:
        return False


def test_missing_dep_raises_with_fix_hint() -> None:
    """When sentence-transformers isn't installed, the
    constructor SHALL raise EmbedderModelMissing with a
    pip-install fix hint."""
    if _has_sentence_transformers():
        pytest.skip(
            "sentence-transformers is installed; the missing-dep "
            "branch can't be exercised in this environment"
        )
    with pytest.raises(EmbedderModelMissing) as exc_info:
        SentenceTransformerEmbedder()
    assert "pip install sentence-transformers" in exc_info.value.fix_hint


@pytest.mark.skipif(
    not _has_sentence_transformers(),
    reason="sentence-transformers not installed",
)
def test_default_model_dimension_is_384() -> None:
    e = SentenceTransformerEmbedder()
    assert e.dimension == 384


@pytest.mark.skipif(
    not _has_sentence_transformers(),
    reason="sentence-transformers not installed",
)
def test_embed_returns_unit_norm_floats() -> None:
    import math

    e = SentenceTransformerEmbedder()
    vec = e.embed("hello world")
    assert len(vec) == e.dimension
    norm = math.sqrt(sum(v * v for v in vec))
    # normalize_embeddings=True → unit norm
    assert abs(norm - 1.0) < 1e-3


@pytest.mark.skipif(
    not _has_sentence_transformers(),
    reason="sentence-transformers not installed",
)
def test_embed_batch_returns_n_lists() -> None:
    e = SentenceTransformerEmbedder()
    out = e.embed_batch(["a", "b", "c"])
    assert len(out) == 3
    for row in out:
        assert len(row) == e.dimension
