"""Embedder Protocol + default implementations (M4.2/M4.3).

The substrate defers actual embedding model choice to the
deployment; the Protocol is the contract every plug-in honors:

    class Embedder(Protocol):
        @property
        def dimension(self) -> int: ...
        def embed(self, text: str) -> list[float]: ...
        def embed_batch(self, texts: list[str]) -> list[list[float]]: ...

Two implementations ship:

  - HashEmbedder: deterministic pseudo-embedding suitable for
    tests and CI. Same text -> same vector. Dimension configurable;
    default 2560 to match `nodes_live.embedding vector(2560)`.
    Production deployments must NOT use this.

  - SentenceTransformerEmbedder: thin wrapper over
    sentence-transformers. Only constructed when the optional
    dependency is available; raises at construction if not.

Production embedders must match the active schema width.
"""
from okg.substrate.embeddings.base import (
    DEFAULT_DIMENSION,
    Embedder,
    HashEmbedder,
)
from okg.substrate.embeddings.config import (
    EMBEDDING_CONFIG_CONTRACT_VERSION,
    EMBEDDING_PROBE_TEXT,
    EmbeddingConfig,
    EmbeddingConfigDeprecationWarning,
    EmbeddingConfigError,
    EmbeddingExecutionPlan,
    build_embedding_execution_plan,
    parse_embedding_config,
)
from okg.substrate.embeddings.identity import (
    ACCEPTED_EMBEDDER_SPECS,
    EMBEDDING_DIMENSION,
    assert_embedder_matches_substrate_width,
    build_embedder,
    embedder_model_id,
    model_id_for_spec,
)
from okg.substrate.embeddings.sentence_transformers import (
    SentenceTransformerEmbedder,
)


__all__ = [
    "ACCEPTED_EMBEDDER_SPECS",
    "EMBEDDING_CONFIG_CONTRACT_VERSION",
    "EMBEDDING_PROBE_TEXT",
    "DEFAULT_DIMENSION",
    "EMBEDDING_DIMENSION",
    "Embedder",
    "EmbeddingConfig",
    "EmbeddingConfigDeprecationWarning",
    "EmbeddingConfigError",
    "EmbeddingExecutionPlan",
    "HashEmbedder",
    "SentenceTransformerEmbedder",
    "assert_embedder_matches_substrate_width",
    "build_embedding_execution_plan",
    "build_embedder",
    "embedder_model_id",
    "model_id_for_spec",
    "parse_embedding_config",
]
