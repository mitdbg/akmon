"""Embedder Protocol + HashEmbedder default."""
from __future__ import annotations

import hashlib
import math
from typing import Protocol, Sequence, runtime_checkable


DEFAULT_DIMENSION = 2560  # matches nodes_live.embedding vector(2560)


@runtime_checkable
class Embedder(Protocol):
    """Contract every substrate embedder honors."""

    @property
    def dimension(self) -> int:
        raise NotImplementedError

    @property
    def model_id(self) -> str:
        """Identity recorded in `nodes_live.embedding_model`.

        MUST be unique per MODEL, not per class. Semantic search compares
        this against the identity stored on the vectors it ranks, so an
        identity shared by two different models (as a class-name-derived one
        would be) makes that check pass while ranking noise. An embedder that
        does not declare one is refused rather than defaulted; see
        `okg.substrate.embeddings.identity.embedder_model_id`.
        """
        raise NotImplementedError

    def embed(self, text: str) -> list[float]:
        """Return a unit-norm vector of length `dimension`."""
        raise NotImplementedError

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        """Embed a list of texts. Default implementations may loop
        over `embed`; batching is an optional optimization."""
        raise NotImplementedError


class HashEmbedder:
    """Deterministic hash-based pseudo-embedding.

    Uses SHA-256 over the text and unpacks bytes into floats in
    [-1, 1], then L2-normalizes. Same text always produces the
    same vector (round-trip safe). Different texts produce
    different vectors with very high probability.

    Purpose: tests and CI where a real sentence-transformer is
    overkill or unavailable. Production deployments MUST replace
    this with a real embedder.
    """

    def __init__(self, dimension: int = DEFAULT_DIMENSION) -> None:
        if dimension <= 0:
            raise ValueError(
                f"HashEmbedder dimension must be positive; got {dimension}"
            )
        self._dimension = dimension

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def model_id(self) -> str:
        # The dimension is part of the identity because two HashEmbedders at
        # different widths produce incomparable vectors.
        return f"hash:{self._dimension}"

    def embed(self, text: str) -> list[float]:
        """Hash-based deterministic vector."""
        if text is None:
            raise ValueError("HashEmbedder.embed: text must not be None")
        # Stretch the hash over the requested dimension by
        # repeatedly feeding (text, counter) into SHA-256 until we
        # have enough bytes.
        needed = self._dimension * 2  # 2 bytes per float
        buf = bytearray()
        counter = 0
        while len(buf) < needed:
            h = hashlib.sha256(f"{text}|{counter}".encode("utf-8")).digest()
            buf.extend(h)
            counter += 1
        # Unpack as unsigned 16-bit values → map to [-1, 1].
        vec = [
            (int.from_bytes(buf[i:i + 2], "big") / 32767.5) - 1.0
            for i in range(0, needed, 2)
        ][: self._dimension]
        # L2-normalize so cosine distance behaves like dot product.
        # The probability of SHA-256 unpacking to an all-zero vector
        # at dimension >= 128 is effectively 0, so `norm == 0` is
        # unreachable in practice. Assert to document the precondition
        # — if it ever fires, something about the entropy source
        # changed, and silently returning a bogus unit vector is the
        # wrong answer.
        norm = math.sqrt(sum(v * v for v in vec))
        if norm <= 0:
            raise RuntimeError(
                "HashEmbedder.embed produced all-zero vector for "
                f"text={text!r}; hash source broken"
            )
        return [v / norm for v in vec]

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        return [self.embed(t) for t in texts]
