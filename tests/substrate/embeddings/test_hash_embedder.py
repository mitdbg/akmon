from __future__ import annotations

import math

import pytest

from okg.substrate.embeddings.base import HashEmbedder


def test_hash_embedder_embed_batch_matches_individual_embeddings() -> None:
    embedder = HashEmbedder(dimension=8)

    batch = embedder.embed_batch(["alpha", "beta"])

    assert batch == [embedder.embed("alpha"), embedder.embed("beta")]
    assert all(math.isclose(sum(value * value for value in vec), 1.0) for vec in batch)


def test_hash_embedder_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="dimension must be positive"):
        HashEmbedder(dimension=0)

    with pytest.raises(ValueError, match="must not be None"):
        HashEmbedder(dimension=8).embed(None)  # type: ignore[arg-type]
