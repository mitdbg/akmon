"""SentenceTransformerEmbedder — optional adapter backed by
the `sentence-transformers` library.

Per `add-embedding-worker` (2026-04-26). Conforms to the
existing `Embedder` Protocol; lazy-imports the optional
dependency so the substrate package stays light when no
deployment uses semantic embeddings.

The library default is `all-MiniLM-L6-v2` (384-dim). It does NOT match
the substrate's fixed 2560-dimensional column. Embedding execution-plan
preflight therefore refuses it before runtime registration or graph writes.
The repository currently ships no supported 2560-dimensional production
model recipe; deployments must provide and verify a compatible backend.
"""
from __future__ import annotations

from typing import Optional, Sequence

from okg.substrate.errors import EmbedderModelMissing
from okg.substrate.support.env_provenance import (
    annotate_optional_dependency_error,
)


__all__ = ["SentenceTransformerEmbedder"]


class SentenceTransformerEmbedder:
    """Wraps a `sentence-transformers` model behind the
    Embedder Protocol."""

    name: str = "sentence-transformers"

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        *,
        device: Optional[str] = None,
    ) -> None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as e:
            # Issue #639: name the interpreter that lacked the package so
            # a uv-tool-shim run is distinguishable from a missing install.
            message, fix_hint, provenance = (
                annotate_optional_dependency_error(
                    "sentence_transformers is not installed",
                    (
                        "pip install sentence-transformers "
                        "(or `pip install -e .[embeddings]`)"
                    ),
                    extras=("embeddings",),
                )
            )
            raise EmbedderModelMissing(
                message,
                data={"model_name": model_name, **provenance},
                fix_hint=fix_hint,
            ) from e
        self._model_name = model_name
        self._model = SentenceTransformer(model_name, device=device)
        dim = self._model.get_sentence_embedding_dimension()
        if dim is None:
            raise EmbedderModelMissing(
                f"sentence-transformers model {model_name!r} "
                f"reported no embedding dimension",
                data={"model_name": model_name},
                fix_hint="pick a model with a fixed-size embedding",
            )
        self._dimension = int(dim)

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def model_id(self) -> str:
        """Identity recorded in `nodes_live.embedding_model`.

        Carries the MODEL NAME, not the class name. A class-derived identity
        (`SentenceTransformerEmbedder:2560`) is shared by every model at the
        substrate width, so a corpus re-embedded under a different model would
        compare EQUAL to the old query embedder and rank noise silently. See
        `okg.substrate.embeddings.identity`.
        """
        return f"sentence-transformers:{self._model_name}"

    def embed(self, text: str) -> list[float]:
        if text is None:
            raise ValueError(
                "SentenceTransformerEmbedder.embed: text must "
                "not be None"
            )
        vec = self._model.encode(
            [text],
            batch_size=1,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return [float(v) for v in vec[0]]

    def embed_batch(
        self, texts: Sequence[str],
    ) -> list[list[float]]:
        if not texts:
            return []
        vecs = self._model.encode(
            list(texts),
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return [[float(v) for v in row] for row in vecs]
