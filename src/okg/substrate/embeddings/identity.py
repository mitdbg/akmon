"""One spec parser, one model identity, one substrate width.

Per `reachable-semantic-search` (issue #797 item 5). Three facts about
embedders were duplicated or unsound before this module existed, and all
three are the same drift class: two copies of one fact that can disagree.

1. THE SPEC PARSER existed twice — `cli.main._build_embedder` and
   `runtime.embed_workflow._build_embedder_from_spec`, whose docstring
   admitted it "mirrors" the other. The WRITER of embeddings parsed specs
   with one copy and the READER with the other, so one copy changing its
   default model would make the same spec string mean two different
   embedders. :func:`build_embedder` is now the only implementation.

2. THE MODEL IDENTITY was derived inline as
   ``getattr(embedder, "model_id", f"{type(embedder).__name__}:{dim}")``
   and was NOT INJECTIVE. Neither shipped embedder declared ``model_id``,
   so the fallback always fired and EVERY sentence-transformer at the
   substrate width collapsed to the single string
   ``SentenceTransformerEmbedder:2560``. Two different retrieval models
   were recorded as the same model, which is exactly the state a
   cross-model check must detect. :func:`embedder_model_id` refuses an
   embedder that declares no identity rather than naming it after its
   class: the class-name fallback IS the collision, so keeping it as a
   default would preserve the defect for every future embedder, and an
   embedder that cannot say what it is cannot be checked against a corpus.

3. THE SUBSTRATE WIDTH existed as both ``DEFAULT_DIMENSION`` here and
   ``EMBEDDING_DIMENSION`` in the DBOS runtime module. :data:`EMBEDDING_DIMENSION`
   is now defined once, in the embeddings package, and re-exported by the
   runtime module — so the read side never imports a schema constant
   from a workflow module.

Injectivity, not single-siting, is the property that matters. A single
call site that returns colliding identities is no better than two call
sites that agree. `tests/substrate/test_embedder_identity.py` asserts
injectivity over the enumerated accepted spec set and asserts that the
spec-only and instance-based derivations agree, which no textual
single-siting check could express.
"""
from __future__ import annotations

import re
import threading
from typing import Any

from okg.substrate.embeddings.base import DEFAULT_DIMENSION
from okg.substrate.errors import SubstrateError


__all__ = [
    "EMBEDDING_DIMENSION",
    "ACCEPTED_EMBEDDER_SPECS",
    "HASH_WIDTH_PREFIX",
    "OPENROUTER_PREFIX",
    "SENTENCE_TRANSFORMERS_PREFIX",
    "build_embedder",
    "fixed_width_for_model_id",
    "is_paid_model_id",
    "native_dimension_for_model_id",
    "parse_openrouter_spec",
    "embedder_model_id",
    "model_id_for_spec",
    "assert_embedder_matches_substrate_width",
    "sentence_transformer_width",
]


#: Width of `nodes_live.embedding vector(2560)`. THE definition; the DBOS
#: runtime module re-exports this rather than holding a second copy.
EMBEDDING_DIMENSION = DEFAULT_DIMENSION

#: ``hash@<dims>``: the hash test embedder at a chosen width. A vector-set
#: backend only (PACT vector-set-pgvector-widths); plain ``hash`` stays
#: ``hash:2560``, so existing set ids do not change.
HASH_WIDTH_PREFIX = "hash@"

SENTENCE_TRANSFORMERS_PREFIX = "sentence-transformers:"

#: An OpenAI-compatible embeddings endpoint reached through OpenRouter, as
#: ``openrouter:<model>@<dims>`` (PACT openrouter-embeddings-hnsw). The model
#: returns ``<dims>``-wide vectors; the backend zero-pads them to
#: :data:`EMBEDDING_DIMENSION`, which leaves every cosine distance unchanged.
OPENROUTER_PREFIX = "openrouter:"

#: The spec grammar, for error messages and for the injectivity proof to
#: enumerate. `sentence-transformers:<model>` and `openrouter:<model>@<dims>`
#: are families, not single specs, so the proof supplies concrete names.
ACCEPTED_EMBEDDER_SPECS = (
    "hash",
    f"{SENTENCE_TRANSFORMERS_PREFIX}<model>",
    f"{OPENROUTER_PREFIX}<model>@<dims>",
    f"{HASH_WIDTH_PREFIX}<dims>",
)

#: The characters an OpenRouter model name may carry. Narrow on purpose: the
#: identity is written into an index predicate, so it must never need quoting
#: beyond a plain SQL string literal.
_OPENROUTER_MODEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:-]*$")


def _invalid_spec(spec: object, detail: str) -> SubstrateError:
    err = SubstrateError(
        f"unknown embedder spec: {spec!r} — {detail}",
        fix_hint=(
            "use 'hash', 'sentence-transformers:<model>' "
            "(e.g. 'sentence-transformers:all-mpnet-base-v2') or "
            "'openrouter:<model>@<dims>' "
            "(e.g. 'openrouter:google/gemini-embedding-2@768')"
        ),
    )
    err.code = "embedder_spec_invalid"
    return err


def model_id_for_spec(spec: str, *, vector_set: bool = False) -> str:
    """The recorded model identity for a spec, WITHOUT building the embedder.

    ``vector_set=True`` reads the spec as a vector-set backend (PACT
    vector-set-pgvector-widths): ``hash@<dims>`` is accepted and an
    OpenRouter width is not capped at :data:`EMBEDDING_DIMENSION`, because a
    set stores each model at its own width. The set's configuration refuses
    a width above pgvector's limit for its storage type and index kind. The
    default reads it for the node column, unchanged.

    This exists because `SentenceTransformerEmbedder.__init__` loads the
    model from disk, and `reachable-semantic-search` requires refusing an
    unembedded or model-skewed corpus BEFORE paying that cost. The identity
    is a pure function of the spec, so the probe never needs the instance.

    Agreement with :func:`embedder_model_id` is a tested property, not an
    assumption — two derivations that could disagree would recreate the
    skew this module exists to detect.
    """
    if not isinstance(spec, str) or not spec.strip():
        raise _invalid_spec(spec, "spec must be a non-empty string")
    spec = spec.strip()
    if spec == "hash":
        return f"hash:{EMBEDDING_DIMENSION}"
    if spec.startswith(HASH_WIDTH_PREFIX):
        return f"hash:{_parse_hash_width(spec, vector_set=vector_set)}"
    if spec.startswith(SENTENCE_TRANSFORMERS_PREFIX):
        model_name = spec[len(SENTENCE_TRANSFORMERS_PREFIX):].strip()
        if not model_name:
            raise _invalid_spec(
                spec, "sentence-transformers requires a model name",
            )
        # The model NAME, not the class name: two different models must be
        # two different identities. Note the stated residual — a model
        # republished in place under the same name yields the same identity
        # and different vectors; recording an upstream revision is out of
        # scope for issue #797 and is a known limit, not an oversight.
        return f"{SENTENCE_TRANSFORMERS_PREFIX}{model_name}"
    if spec.startswith(OPENROUTER_PREFIX):
        model_name, dims = parse_openrouter_spec(spec, vector_set=vector_set)
        # Model AND width: the same model at two widths writes vectors that
        # cannot be compared, so they must be two identities.
        return f"{OPENROUTER_PREFIX}{model_name}@{dims}"
    raise _invalid_spec(spec, "unrecognised spec prefix")


def _parse_hash_width(spec: str, *, vector_set: bool) -> int:
    """``hash@<dims>`` -> ``dims``, or refuse. A vector-set backend only."""
    if not vector_set:
        raise _invalid_spec(
            spec, "'hash@<dims>' is a vector-set backend (embeddings.sets); "
            f"the single backend form uses 'hash' ({EMBEDDING_DIMENSION} wide)",
        )
    dims_text = spec[len(HASH_WIDTH_PREFIX):].strip()
    if not dims_text.isdigit() or int(dims_text) < 1:
        raise _invalid_spec(
            spec, f"hash dims {dims_text!r} is not a positive integer",
        )
    return int(dims_text)


def parse_openrouter_spec(spec: str, *, vector_set: bool = False) -> tuple[str, int]:
    """``openrouter:<model>@<dims>`` -> ``(model, dims)``, or refuse.

    ``dims`` is the width the model is asked for. For the node column it
    must lie in ``1..EMBEDDING_DIMENSION`` because the backend pads up to the
    column width and never truncates. For a vector set (``vector_set=True``)
    it must be at least 1; the set's configuration refuses a width above
    pgvector's limit for its storage type and index kind.
    """
    if not isinstance(spec, str) or not spec.strip().startswith(OPENROUTER_PREFIX):
        raise _invalid_spec(spec, "not an openrouter spec")
    rest = spec.strip()[len(OPENROUTER_PREFIX):]
    model_name, sep, dims_text = rest.rpartition("@")
    model_name = model_name.strip()
    dims_text = dims_text.strip()
    if not sep or not model_name or not dims_text:
        raise _invalid_spec(
            spec, "openrouter requires '<model>@<dims>', e.g. "
            "'openrouter:google/gemini-embedding-2@768'",
        )
    if not _OPENROUTER_MODEL_RE.match(model_name):
        raise _invalid_spec(
            spec, f"openrouter model name {model_name!r} has characters "
            "outside [A-Za-z0-9._/:-]",
        )
    if not dims_text.isdigit():
        raise _invalid_spec(spec, f"openrouter dims {dims_text!r} is not an integer")
    dims = int(dims_text)
    if vector_set:
        if dims < 1:
            raise _invalid_spec(spec, f"openrouter dims must be at least 1; got {dims}")
    elif not 1 <= dims <= EMBEDDING_DIMENSION:
        raise _invalid_spec(
            spec, f"openrouter dims must be in 1..{EMBEDDING_DIMENSION}; got {dims}",
        )
    return model_name, dims


def is_paid_model_id(model_id: str | None) -> bool:
    """Whether vectors for this identity cost money per call.

    True for the ``openrouter:`` family. A paid backend is embedded only by
    ``okg db embed-backfill``, which caps and records spend; the DBOS embed
    worker refuses it.
    """
    return isinstance(model_id, str) and model_id.startswith(OPENROUTER_PREFIX)


def native_dimension_for_model_id(model_id: str) -> int | None:
    """The width a model actually returns, when it is narrower than the column.

    ``None`` for every identity whose vectors fill the column (``hash``,
    sentence-transformers): those have no narrower native width to index.
    """
    if isinstance(model_id, str) and model_id.startswith(OPENROUTER_PREFIX):
        return parse_openrouter_spec(model_id)[1]
    return None


def fixed_width_for_model_id(model_id: str) -> int | None:
    """The width a vector set's model writes, when its identity names it.

    ``hash:<dims>`` and ``openrouter:<model>@<dims>`` carry their width;
    a sentence-transformers identity does not (``None``): its width is the
    loaded model's (:func:`sentence_transformer_width`).
    """
    if not isinstance(model_id, str):
        return None
    if model_id.startswith("hash:"):
        dims = model_id[len("hash:"):]
        return int(dims) if dims.isdigit() else None
    if model_id.startswith(OPENROUTER_PREFIX):
        return parse_openrouter_spec(model_id, vector_set=True)[1]
    return None


_SENTENCE_TRANSFORMERS: dict[str, Any] = {}
_SENTENCE_TRANSFORMERS_LOCK = threading.Lock()


def _sentence_transformer(model_name: str) -> Any:
    """One loaded model per name per process, shared by the width lookup and
    the vector-set backend, so a set's model is read from disk once."""
    with _SENTENCE_TRANSFORMERS_LOCK:
        embedder = _SENTENCE_TRANSFORMERS.get(model_name)
        if embedder is None:
            from okg.substrate.embeddings.sentence_transformers import (
                SentenceTransformerEmbedder,
            )

            embedder = SentenceTransformerEmbedder(model_name=model_name)
            _SENTENCE_TRANSFORMERS[model_name] = embedder
        return embedder


def sentence_transformer_width(model_name: str) -> int:
    """The width a sentence-transformers model returns, from the model itself.

    Loads the model (once per process). A vector set of this backend stores
    vectors at this width (PACT vector-set-pgvector-widths).
    """
    return int(_sentence_transformer(model_name).dimension)


def reset_sentence_transformer_cache() -> None:
    """Forget every loaded model (tests that install a stand-in model)."""
    with _SENTENCE_TRANSFORMERS_LOCK:
        _SENTENCE_TRANSFORMERS.clear()


def build_embedder(
    spec: str, *, query: bool = False, provider: str | None = None,
    vector_set: bool = False,
) -> Any:
    """Parse an embedder spec into an Embedder. The ONLY implementation.

    Recognized:
      - ``hash``
      - ``sentence-transformers:<model_name>``
      - ``openrouter:<model>@<dims>``

    ``query=True`` builds the search-path client of a paid backend: a short
    timeout and two attempts, so one slow request cannot stall a search (the
    backfill keeps its own retry policy). ``provider`` pins an OpenRouter
    provider with no fallback; it is part of a vector set's identity.

    ``vector_set=True`` builds a vector set's backend (PACT
    vector-set-pgvector-widths): every backend returns vectors at its own
    width, with no zero-padding to :data:`EMBEDDING_DIMENSION`; a
    sentence-transformers model is loaded once per process and shared.
    The default builds the node column's backend, unchanged.
    """
    if provider is not None and not str(spec).strip().startswith(OPENROUTER_PREFIX):
        raise _invalid_spec(spec, "a provider pin applies only to an openrouter backend")
    if not isinstance(spec, str) or not spec.strip():
        raise _invalid_spec(spec, "spec must be a non-empty string")
    spec = spec.strip()
    # Validate before importing so a bad spec fails the same way whether or
    # not the optional dependency is installed.
    model_id = model_id_for_spec(spec, vector_set=vector_set)
    if spec == "hash" or spec.startswith(HASH_WIDTH_PREFIX):
        from okg.substrate.embeddings.base import HashEmbedder
        embedder = HashEmbedder(dimension=int(model_id[len("hash:"):]))
    elif spec.startswith(OPENROUTER_PREFIX):
        from okg.substrate.embeddings.openrouter import (
            QUERY_MAX_ATTEMPTS,
            QUERY_TIMEOUT_SECONDS,
            OpenRouterEmbedder,
        )

        model_name, dims = parse_openrouter_spec(spec, vector_set=vector_set)
        client: dict[str, Any] = (
            {"timeout_seconds": QUERY_TIMEOUT_SECONDS, "max_attempts": QUERY_MAX_ATTEMPTS}
            if query else {}
        )
        embedder = OpenRouterEmbedder(
            model=model_name, native_dimension=dims, provider=provider,
            pad_to_column=not vector_set, **client,
        )
    elif vector_set:
        embedder = _sentence_transformer(
            spec[len(SENTENCE_TRANSFORMERS_PREFIX):].strip(),
        )
    else:
        from okg.substrate.embeddings.sentence_transformers import (
            SentenceTransformerEmbedder,
        )
        model_name = spec[len(SENTENCE_TRANSFORMERS_PREFIX):].strip()
        embedder = SentenceTransformerEmbedder(model_name=model_name)
    # Fail loudly here rather than letting the two derivations drift apart
    # silently. This is cheap and it is the invariant the corpus probe rests
    # on: the identity computed from the spec must be the identity the
    # instance reports, or a probe done before construction is meaningless.
    built = embedder_model_id(embedder)
    if built != model_id:
        raise SubstrateError(
            f"embedder identity disagreement for spec {spec!r}: "
            f"model_id_for_spec gave {model_id!r} but the constructed "
            f"embedder reports {built!r}",
            data={"spec": spec, "from_spec": model_id, "from_instance": built},
            fix_hint=(
                "the spec-only and instance identity derivations must agree; "
                "fix the embedder's model_id property"
            ),
        )
    return embedder


def embedder_model_id(embedder: Any) -> str:
    """The identity recorded in `embedding_model` for this embedder.

    Refuses an embedder that declares no ``model_id``. There is deliberately
    NO class-name fallback: ``f"{type(embedder).__name__}:{dimension}"`` is
    not injective — every sentence-transformer at the substrate width
    collapses to one string — and a non-injective identity makes a
    cross-model check report a verified model having verified nothing.
    """
    model_id = getattr(embedder, "model_id", None)
    if not isinstance(model_id, str) or not model_id.strip():
        err = SubstrateError(
            f"embedder {type(embedder).__name__} declares no model_id, so "
            "the vectors it produces cannot be attributed to a model. "
            "Semantic search compares the query embedder's identity against "
            "the identity recorded on the stored vectors; an embedder "
            "without one cannot be checked.",
            data={"embedder_type": type(embedder).__name__},
            fix_hint=(
                "give the embedder a `model_id` property that is unique per "
                "model (not per class), e.g. "
                "'sentence-transformers:all-mpnet-base-v2'"
            ),
        )
        err.code = "embedder_identity_undeclared"
        raise err
    return model_id.strip()


def assert_embedder_matches_substrate_width(embedder: Any) -> None:
    """Check the embedder against the COLUMN width, not against itself.

    The guard this replaces compared ``len(embedder.embed(q))`` against
    ``embedder.dimension`` — a tautology over one object that cannot
    fail for any correctly implemented embedder and that says nothing about
    whether the vector fits the corpus. Extracted as a pure function so it
    is provable without a session or a pinned generation.
    """
    dimension = getattr(embedder, "dimension", None)
    if not isinstance(dimension, int):
        raise _invalid_spec(
            type(embedder).__name__, "embedder declares no integer dimension",
        )
    if dimension != EMBEDDING_DIMENSION:
        err = SubstrateError(
            f"embedder {type(embedder).__name__} produces "
            f"{dimension}-dimensional vectors but the substrate's "
            f"embedding column is {EMBEDDING_DIMENSION}-dimensional, so its "
            "vectors cannot be compared against the stored ones",
            data={
                "embedder_dimension": dimension,
                "substrate_dimension": EMBEDDING_DIMENSION,
            },
            fix_hint=(
                f"use an embedder whose dimension is {EMBEDDING_DIMENSION}"
            ),
        )
        err.code = "embedder_dim_mismatch"
        raise err
