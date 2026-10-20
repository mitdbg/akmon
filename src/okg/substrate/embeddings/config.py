"""Fail-closed deployment embedding configuration and execution plans."""

from __future__ import annotations

import hashlib
import json
import math
import re
import warnings
from collections.abc import Callable, Mapping, Sequence
import dataclasses
from dataclasses import dataclass
from typing import Any

from okg.substrate.embeddings.identity import (
    EMBEDDING_DIMENSION,
    OPENROUTER_PREFIX,
    SENTENCE_TRANSFORMERS_PREFIX,
    assert_embedder_matches_substrate_width,
    build_embedder,
    embedder_model_id,
    fixed_width_for_model_id,
    is_paid_model_id,
    model_id_for_spec,
    parse_openrouter_spec,
    sentence_transformer_width,
)
from okg.substrate.errors import SubstrateError


__all__ = [
    "DEFAULT_SET_NAME",
    "DOCUMENT_FORMAT_VERSION",
    "EMBEDDING_CONFIG_CONTRACT_VERSION",
    "EMBEDDING_PROBE_TEXT",
    "DocumentTemplate",
    "EmbeddingConfig",
    "EmbeddingConfigDeprecationWarning",
    "EmbeddingConfigError",
    "EmbeddingExecutionPlan",
    "VectorIndexConfig",
    "VectorSetConfig",
    "build_embedding_execution_plan",
    "build_vector_set_plan",
    "parse_embedding_config",
    "set_width_refusal",
]


EMBEDDING_CONFIG_CONTRACT_VERSION = 1
EMBEDDING_PROBE_TEXT = "okg embedding capability probe v1"
_MISSING = object()
_BLOCK_KEYS = frozenset({"enabled", "backend", "reason", "search_set", "sets"})

#: The set an old single ``backend:`` block reads as (PACT
#: openrouter-embeddings-hnsw, design v3.2).
DEFAULT_SET_NAME = "main"

#: The version of the fixed document format (``Label: value`` header lines,
#: a blank line, the body, cut at ``max_chars``). Part of every set's
#: identity: a new format makes new vectors.
DOCUMENT_FORMAT_VERSION = 1

_SET_KEYS = frozenset({
    "backend", "subtypes", "max_chars", "document_prefix", "query_prefix",
    "provider", "document", "index", "storage",
})
#: pgvector types a set stores its vectors as; halfvec (16-bit floats) is
#: the default, vector (32-bit floats) the full-precision choice.
STORAGE_TYPES = ("halfvec", "vector")
DEFAULT_STORAGE = "halfvec"
_DOCUMENT_KEYS = frozenset({"default", "types"})
_TEMPLATE_KEYS = frozenset({"header", "body"})
_INDEX_KEYS = frozenset({"kind", "build", "search"})
_BUILD_KEYS = frozenset({"m", "ef_construction"})
_SEARCH_KEYS = frozenset({"ef_search", "max_scan_tuples"})
_INDEX_KINDS = ("hnsw", "none")

_SET_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_ATTR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_SUBTYPE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:-]{0,127}$")
_PROVIDER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

#: Bounds, inclusive. pgvector's HNSW limits: m 2..100, ef_construction
#: 4..1000 (and at least 2*m), ef_search 1..1000; HNSW indexes up to 4000
#: dimensions of halfvec and 2000 of vector, and either type stores up to
#: 16000. A set's width is its model's own width, bounded only by these
#: pgvector limits (PACT vector-set-pgvector-widths); the 2560 of the node
#: column does not apply.
MAX_CHARS_RANGE = (1, 200_000)
PREFIX_MAX_CHARS = 1_000
MAX_HEADER_FIELDS = 16
HNSW_M_RANGE = (2, 100)
HNSW_EF_CONSTRUCTION_RANGE = (4, 1000)
HNSW_EF_SEARCH_RANGE = (1, 1000)
HNSW_MAX_SCAN_TUPLES_RANGE = (1, 100_000_000)
HNSW_MAX_DIMENSIONS = 4000
HALFVEC_MAX_DIMENSIONS = 16000
VECTOR_HNSW_MAX_DIMENSIONS = 2000
VECTOR_MAX_DIMENSIONS = 16000
#: pgvector's limits per storage type: (most dimensions stored, most under
#: an HNSW index).
STORAGE_LIMITS = {
    "halfvec": (HALFVEC_MAX_DIMENSIONS, HNSW_MAX_DIMENSIONS),
    "vector": (VECTOR_MAX_DIMENSIONS, VECTOR_HNSW_MAX_DIMENSIONS),
}

#: The only body attribute in this version: the node's own ``text``, whose
#: read rule (a row that hides ``text`` is never ranked) already exists.
DOCUMENT_BODY_ATTRS = ("text",)

#: Header pseudo-field that renders the node's subtype as ``Type: <subtype>``.
SUBTYPE_FIELD = "subtype"


class EmbeddingConfigError(ValueError):
    """A deployment embedding block is malformed or contradictory."""


def set_width_refusal(
    width: int, kind: str, storage: str = DEFAULT_STORAGE,
) -> str | None:
    """Why pgvector cannot hold a set of this width, index kind and storage
    type, or None.

    The limits are pgvector's: either type stores up to 16000 dimensions
    (index kind ``none``: exact search); an HNSW index takes up to 4000 of
    halfvec and 2000 of vector.
    """
    stored, indexed = STORAGE_LIMITS[storage]
    if width > stored:
        return (
            f"pgvector's {storage} type stores at most {stored} "
            f"dimensions; this set's model returns {width}"
        )
    if kind == "hnsw" and width > indexed:
        return (
            f"pgvector's {storage} HNSW index takes at most {indexed} "
            f"dimensions; this set's model returns {width} (index kind "
            f"'none' searches exactly up to {stored})"
        )
    return None


class EmbeddingConfigDeprecationWarning(UserWarning):
    """The legacy singular ``embedding`` manifest key was admitted."""


@dataclass(frozen=True)
class DocumentTemplate:
    """Which node fields one subtype's embedded document is built from."""

    header: tuple[str, ...] = (SUBTYPE_FIELD, "title")
    body: str = "text"

    def as_json(self) -> dict[str, Any]:
        return {"header": list(self.header), "body": self.body}


@dataclass(frozen=True)
class VectorIndexConfig:
    """A set's index: ``hnsw`` with its build and search settings, or none."""

    kind: str = "hnsw"
    m: int = 16
    ef_construction: int = 64
    ef_search: int = 100
    max_scan_tuples: int = 20_000

    def build_json(self) -> dict[str, Any]:
        """The settings that shape the built index (its name carries a hash)."""
        if self.kind == "none":
            return {"kind": "none"}
        return {"kind": self.kind, "m": self.m, "ef_construction": self.ef_construction}


@dataclass(frozen=True)
class VectorSetConfig:
    """One named vector set: a model, its document rules and its index.

    ``identity`` holds only what changes the vectors themselves; subtypes and
    templates are left out because vectors are keyed by the hash of the
    document they embed, so a template change makes new documents (and only
    those need vectors).
    """

    name: str
    backend: str
    subtypes: tuple[str, ...] | None = None  # None = every subtype with text
    max_chars: int = 24_000
    document_prefix: str = ""
    query_prefix: str = ""
    provider: str | None = None
    default_template: DocumentTemplate = DocumentTemplate()
    type_templates: tuple[tuple[str, DocumentTemplate], ...] = ()
    index: VectorIndexConfig = VectorIndexConfig()
    #: The pgvector type the vectors are stored as (``storage:``); part of
    #: the identity only when it is not the default, so every set registered
    #: before the choice existed keeps its id.
    storage: str = DEFAULT_STORAGE
    #: The width when it is already known without the model: the registered
    #: row's, or the node column's for the old single ``backend:`` form.
    #: Not compared: it never changes what the set is.
    resolved_width: int | None = dataclasses.field(default=None, compare=False)

    def __post_init__(self) -> None:
        # The type and its column are written into SQL: only the two known.
        if self.storage not in STORAGE_TYPES:
            raise EmbeddingConfigError(
                f"vector set {self.name!r} storage must be one of "
                f"{list(STORAGE_TYPES)}; got {self.storage!r}"
            )

    @property
    def model_id(self) -> str:
        return model_id_for_spec(self.backend, vector_set=True)

    @property
    def width(self) -> int:
        """The model's own width (PACT vector-set-pgvector-widths): named by
        a ``hash@<dims>`` or ``openrouter:<model>@<dims>`` backend, and read
        from the loaded model for sentence-transformers."""
        if self.resolved_width is not None:
            return int(self.resolved_width)
        fixed = fixed_width_for_model_id(self.model_id)
        if fixed is not None:
            return int(fixed)
        return sentence_transformer_width(
            self.backend.strip()[len(SENTENCE_TRANSFORMERS_PREFIX):].strip()
        )

    def identity_key(self) -> dict[str, Any]:
        """The identity without its width. The model identity already fixes
        the width, so two sets with equal keys have equal identities; this
        form compares sets without loading a sentence-transformers model."""
        key = {
            "model_id": self.model_id,
            "max_chars": self.max_chars,
            "document_prefix": self.document_prefix,
            "query_prefix": self.query_prefix,
            "provider": self.provider,
            "document_format_version": DOCUMENT_FORMAT_VERSION,
        }
        if self.storage != DEFAULT_STORAGE:
            key["storage"] = self.storage
        return key

    @property
    def vector_column(self) -> str:
        """The ``okg.text_embeddings`` column holding this set's vectors."""
        return "embedding" if self.storage == "halfvec" else "embedding_f32"

    @property
    def vector_type(self) -> str:
        """The pgvector type of one of this set's vectors, e.g. ``halfvec(768)``."""
        return f"{self.storage}({int(self.width)})"

    @property
    def cosine_opclass(self) -> str:
        """The HNSW operator class for cosine distance on this set's type."""
        return f"{self.storage}_cosine_ops"

    def identity(self) -> dict[str, Any]:
        """The canonical identity: model, width, cut, prefixes, provider,
        document format version and, when not halfvec, storage. Nothing
        else."""
        return {**self.identity_key(), "width": self.width}

    @property
    def set_id(self) -> str:
        """``es_`` plus 24 hex digits of the identity's sha256. Hex only, so
        it can be inlined in an index predicate and an index name."""
        return "es_" + _canonical_sha256(self.identity())[:24]

    def template_for(self, subtype: str) -> DocumentTemplate:
        for name, template in self.type_templates:
            if name == subtype:
                return template
        return self.default_template

    def covers(self, subtype: str) -> bool:
        return self.subtypes is None or subtype in self.subtypes

    def documents_json(self) -> dict[str, Any]:
        """The document rules as stored for the graph writer."""
        return {
            "subtypes": None if self.subtypes is None else list(self.subtypes),
            "default": self.default_template.as_json(),
            "types": {name: t.as_json() for name, t in self.type_templates},
        }


@dataclass(frozen=True)
class EmbeddingConfig:
    """The one normalized meaning of a deployment's embedding block.

    ``backend`` is the search set's backend: every reader of the old node
    columns keeps reading it. ``sets`` is empty for the old single
    ``backend:`` form; :attr:`vector_sets` then reads it as one set
    :data:`DEFAULT_SET_NAME` with the default settings.
    """

    enabled: bool = False
    backend: str | None = None
    reason: str | None = None
    sets: tuple[VectorSetConfig, ...] = ()
    search_set: str | None = None

    @property
    def vector_sets(self) -> tuple[VectorSetConfig, ...]:
        """Every configured set; empty when embeddings are off."""
        if not self.enabled:
            return ()
        if self.sets:
            return self.sets
        if self.backend is None:
            return ()
        # The single form's backend writes the 2560-wide node column, whose
        # plan refuses any other width, so a sentence-transformers set of
        # this form is that wide without loading the model.
        return (VectorSetConfig(
            name=DEFAULT_SET_NAME, backend=self.backend,
            resolved_width=(
                EMBEDDING_DIMENSION
                if self.backend.strip().startswith(SENTENCE_TRANSFORMERS_PREFIX)
                else None
            ),
        ),)

    @property
    def search_vector_set(self) -> VectorSetConfig | None:
        sets = self.vector_sets
        if not sets:
            return None
        wanted = self.search_set or DEFAULT_SET_NAME
        for item in sets:
            if item.name == wanted:
                return item
        raise EmbeddingConfigError(
            f"embedding search_set {wanted!r} names no configured set"
        )


@dataclass(frozen=True)
class EmbeddingExecutionPlan:
    """A process-local backend whose actual output capability was verified."""

    config: EmbeddingConfig
    embedder: Any | None
    model_id: str | None
    declared_dimension: int | None
    observed_dimension: int | None
    fingerprint: str
    #: Whether the plan's vectors fit the node column ``nodes_live.embedding
    #: vector(2560)``. False for a plan at a vector set's own width (PACT
    #: vector-set-pgvector-widths): the node-column worker and the alias
    #: embedding backend then do not use it.
    node_column: bool = True

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    @property
    def can_embed(self) -> bool:
        """False for a disabled plan and for a paid backend's unprobed plan."""
        return self.enabled and self.embedder is not None

    @property
    def dimension(self) -> int:
        """Expose the verified width when a plan is used as an Embedder."""
        if self.observed_dimension is None:
            if self.enabled:
                raise EmbeddingConfigError(
                    f"the plan for paid backend {self.model_id!r} was built "
                    "without a backend (paid_backend='absent'), so it has no "
                    "verified vector dimension"
                )
            raise EmbeddingConfigError(
                "disabled embedding execution plan has no vector dimension"
            )
        return self.observed_dimension

    def _require_backend(self) -> None:
        if not self.enabled:
            raise EmbeddingConfigError(
                "disabled embedding execution plan cannot embed text"
            )
        if self.embedder is None:
            raise EmbeddingConfigError(
                f"the plan for paid backend {self.model_id!r} was built without "
                "a backend (no probe call), so it cannot embed; embed with "
                "`okg db embed-backfill`"
            )

    def embed(self, text: str) -> list[float]:
        """Delegate through the backend already bound by capability preflight."""
        self._require_backend()
        return self._checked(self.embedder.embed(text))

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        """One backend batch call, every vector checked like :meth:`embed`.

        The backend's own ``embed_batch`` is used, so a backend that batches
        (the OpenRouter one sends many texts per request) is not reduced to
        one call per text here. The count must match the input count.
        """
        self._require_backend()
        texts = list(texts)
        vectors = list(self.embedder.embed_batch(texts))
        if len(vectors) != len(texts):
            err = SubstrateError(
                f"embedding backend {self.model_id!r} returned {len(vectors)} "
                f"vectors for {len(texts)} texts",
                data={
                    "model_id": self.model_id,
                    "texts": len(texts),
                    "vectors": len(vectors),
                },
                fix_hint="fix the backend so embed_batch returns one vector per text",
            )
            err.code = "embedder_output_invalid"
            raise err
        return [self._checked(vector) for vector in vectors]

    def _checked(self, vector: Any) -> list[float]:
        # A set plan's mismatch names the set's width, not the node column's.
        set_width = None if self.node_column else self.dimension
        if isinstance(vector, (str, bytes)) or not isinstance(vector, Sequence):
            raise _dimension_mismatch(
                model_id=str(self.model_id),
                declared=self.declared_dimension,
                observed="non-vector",
                set_width=set_width,
            )
        if len(vector) != self.dimension:
            raise _dimension_mismatch(
                model_id=str(self.model_id),
                declared=self.declared_dimension,
                observed=len(vector),
                set_width=set_width,
            )
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            for value in vector
        ):
            err = SubstrateError(
                f"embedding backend {self.model_id!r} returned non-finite "
                "or non-numeric vector data after capability preflight",
                data={"model_id": self.model_id},
                fix_hint="fix the backend so every output element is finite numeric data",
            )
            err.code = "embedder_output_invalid"
            raise err
        return [float(value) for value in vector]


def _parse_block(value: object, *, key: str) -> EmbeddingConfig:
    if not isinstance(value, Mapping):
        raise EmbeddingConfigError(f"deployment.yaml key {key!r} must be a mapping")
    unknown = sorted(str(name) for name in set(value) - _BLOCK_KEYS)
    if unknown:
        raise EmbeddingConfigError(
            f"deployment.yaml key {key!r} has unknown embedding key(s): "
            + ", ".join(unknown)
        )

    enabled = value.get("enabled", False)
    if not isinstance(enabled, bool):
        raise EmbeddingConfigError(
            f"deployment.yaml key {key!r}.enabled must be a boolean"
        )

    backend_raw = value.get("backend")
    if backend_raw is not None and not isinstance(backend_raw, str):
        raise EmbeddingConfigError(
            f"deployment.yaml key {key!r}.backend must be a string"
        )
    backend = backend_raw.strip() if isinstance(backend_raw, str) else None
    if backend_raw is not None and not backend:
        raise EmbeddingConfigError(
            f"deployment.yaml key {key!r}.backend must not be empty"
        )
    if backend is not None:
        try:
            model_id_for_spec(backend)
        except Exception as exc:  # normalize parser errors at manifest admission
            raise EmbeddingConfigError(
                f"deployment.yaml key {key!r}.backend is invalid: {exc}"
            ) from exc

    reason = value.get("reason")
    if reason is not None and not isinstance(reason, str):
        raise EmbeddingConfigError(
            f"deployment.yaml key {key!r}.reason must be a string"
        )

    sets_raw = value.get("sets", _MISSING)
    search_raw = value.get("search_set", _MISSING)
    if sets_raw is _MISSING:
        if search_raw is not _MISSING:
            raise EmbeddingConfigError(
                f"deployment.yaml key {key!r}.search_set needs a 'sets' block"
            )
        if enabled and backend is None:
            raise EmbeddingConfigError(
                f"deployment.yaml key {key!r} enables embeddings but does not "
                "name an explicit backend"
            )
        return EmbeddingConfig(enabled=enabled, backend=backend, reason=reason)

    if backend_raw is not None:
        raise EmbeddingConfigError(
            f"deployment.yaml key {key!r} has both 'backend' and 'sets'; name "
            "the backend inside each set"
        )
    sets = _parse_sets(sets_raw, key=key)
    if search_raw is _MISSING:
        if len(sets) != 1:
            raise EmbeddingConfigError(
                f"deployment.yaml key {key!r} declares {len(sets)} sets; name "
                "the one search reads with 'search_set'"
            )
        search_set = sets[0].name
    else:
        if not isinstance(search_raw, str) or not search_raw.strip():
            raise EmbeddingConfigError(
                f"deployment.yaml key {key!r}.search_set must be a set name"
            )
        search_set = search_raw.strip()
        if search_set not in {item.name for item in sets}:
            raise EmbeddingConfigError(
                f"deployment.yaml key {key!r}.search_set {search_set!r} names "
                "no set in 'sets'"
            )
    ids: dict[str, str] = {}
    for item in sets:
        # The identity key, not the set id: equal keys mean equal ids, and
        # the key needs no sentence-transformers model loaded.
        identity_key = _canonical_sha256(item.identity_key())
        other = ids.setdefault(identity_key, item.name)
        if other != item.name:
            raise EmbeddingConfigError(
                f"deployment.yaml key {key!r}: sets {other!r} and {item.name!r} "
                "have the same identity (model, width, storage, max_chars, "
                "prefixes, provider); keep one"
            )
    search = next(item for item in sets if item.name == search_set)
    return EmbeddingConfig(
        enabled=enabled,
        backend=search.backend,
        reason=reason,
        sets=sets,
        search_set=search_set,
    )


def _refuse_non_string_keys(value: Mapping[Any, Any], where: str) -> None:
    bad = sorted(repr(name) for name in value if not isinstance(name, str))
    if bad:
        raise EmbeddingConfigError(
            f"deployment.yaml {where} has non-string key(s): " + ", ".join(bad)
        )


def _refuse_unknown(value: Mapping[str, Any], allowed: frozenset[str], where: str) -> None:
    unknown = sorted(str(name) for name in set(value) - allowed)
    if unknown:
        raise EmbeddingConfigError(
            f"deployment.yaml {where} has unknown key(s): " + ", ".join(unknown)
        )


def _int_in_range(
    value: object, bounds: tuple[int, int], where: str,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EmbeddingConfigError(f"deployment.yaml {where} must be an integer")
    low, high = bounds
    if not low <= value <= high:
        raise EmbeddingConfigError(
            f"deployment.yaml {where} must be in {low}..{high}; got {value}"
        )
    return value


def header_field_denied(name: str) -> str | None:
    """Why a header field can never be shown, or None.

    Ids, hashes and timestamps are not meaning, and system attributes are
    governance state: a header names none of them.
    """
    from okg.substrate.nomos.read_masking import GOVERNANCE_KEYS
    from okg.substrate.searchable_text import PRODUCER_TEXT_ATTR

    lowered = name.lower()
    if lowered == "id" or lowered.endswith("_id"):
        return "an id"
    if lowered.endswith("_hash") or lowered == "hash":
        return "a hash"
    if lowered.endswith("_at"):
        return "a timestamp"
    if name in GOVERNANCE_KEYS or lowered.startswith(("okg_", "nomos_")):
        return "a system attribute"
    if name == PRODUCER_TEXT_ATTR or name in DOCUMENT_BODY_ATTRS:
        return "the document body"
    return None


def _parse_template(value: object, where: str) -> DocumentTemplate:
    if not isinstance(value, Mapping):
        raise EmbeddingConfigError(f"deployment.yaml {where} must be a mapping")
    _refuse_unknown(value, _TEMPLATE_KEYS, where)
    header_raw = value.get("header", list(DocumentTemplate().header))
    if not isinstance(header_raw, list) or not all(
        isinstance(item, str) for item in header_raw
    ):
        raise EmbeddingConfigError(
            f"deployment.yaml {where}.header must be a list of attribute names"
        )
    if len(header_raw) > MAX_HEADER_FIELDS:
        raise EmbeddingConfigError(
            f"deployment.yaml {where}.header has {len(header_raw)} fields; at "
            f"most {MAX_HEADER_FIELDS}"
        )
    seen: set[str] = set()
    for field in header_raw:
        if not _ATTR_NAME_RE.match(field):
            raise EmbeddingConfigError(
                f"deployment.yaml {where}.header field {field!r} is not an "
                "attribute name"
            )
        if field in seen:
            raise EmbeddingConfigError(
                f"deployment.yaml {where}.header names {field!r} twice"
            )
        seen.add(field)
        if field == SUBTYPE_FIELD:
            continue
        denied = header_field_denied(field)
        if denied is not None:
            raise EmbeddingConfigError(
                f"deployment.yaml {where}.header field {field!r} is {denied}; "
                "headers name node attributes only (no ids, hashes, "
                "timestamps or system attributes)"
            )
    body = value.get("body", DocumentTemplate().body)
    if body not in DOCUMENT_BODY_ATTRS:
        raise EmbeddingConfigError(
            f"deployment.yaml {where}.body must be one of "
            f"{list(DOCUMENT_BODY_ATTRS)}; got {body!r}"
        )
    return DocumentTemplate(header=tuple(header_raw), body=str(body))


def _parse_index(
    value: object, where: str, *, width: int | None, storage: str = DEFAULT_STORAGE,
) -> VectorIndexConfig:
    """``width`` None: a sentence-transformers set, whose width is known
    only once its model is loaded; :func:`build_vector_set_plan` and ``okg
    migrate`` apply the same limits then."""
    if not isinstance(value, Mapping):
        raise EmbeddingConfigError(f"deployment.yaml {where} must be a mapping")
    _refuse_unknown(value, _INDEX_KEYS, where)
    kind = value.get("kind", "hnsw")
    if kind not in _INDEX_KINDS:
        raise EmbeddingConfigError(
            f"deployment.yaml {where}.kind must be one of {list(_INDEX_KINDS)}; "
            f"got {kind!r}"
        )
    refusal = None if width is None else set_width_refusal(width, str(kind), storage)
    if refusal is not None:
        raise EmbeddingConfigError(f"deployment.yaml {where}: {refusal}")
    if kind == "none":
        extra = sorted(name for name in ("build", "search") if name in value)
        if extra:
            raise EmbeddingConfigError(
                f"deployment.yaml {where}: kind 'none' takes no "
                + ", ".join(extra)
            )
        return VectorIndexConfig(kind="none")
    defaults = VectorIndexConfig()
    build = value.get("build", {})
    if not isinstance(build, Mapping):
        raise EmbeddingConfigError(f"deployment.yaml {where}.build must be a mapping")
    _refuse_unknown(build, _BUILD_KEYS, f"{where}.build")
    search = value.get("search", {})
    if not isinstance(search, Mapping):
        raise EmbeddingConfigError(f"deployment.yaml {where}.search must be a mapping")
    _refuse_unknown(search, _SEARCH_KEYS, f"{where}.search")
    m = _int_in_range(build.get("m", defaults.m), HNSW_M_RANGE, f"{where}.build.m")
    ef_construction = _int_in_range(
        build.get("ef_construction", defaults.ef_construction),
        HNSW_EF_CONSTRUCTION_RANGE,
        f"{where}.build.ef_construction",
    )
    if ef_construction < 2 * m:
        raise EmbeddingConfigError(
            f"deployment.yaml {where}.build.ef_construction must be at least "
            f"2*m ({2 * m}); got {ef_construction}"
        )
    ef_search = _int_in_range(
        search.get("ef_search", defaults.ef_search),
        HNSW_EF_SEARCH_RANGE,
        f"{where}.search.ef_search",
    )
    max_scan_tuples = _int_in_range(
        search.get("max_scan_tuples", defaults.max_scan_tuples),
        HNSW_MAX_SCAN_TUPLES_RANGE,
        f"{where}.search.max_scan_tuples",
    )
    return VectorIndexConfig(
        kind="hnsw", m=m, ef_construction=ef_construction,
        ef_search=ef_search, max_scan_tuples=max_scan_tuples,
    )


def _parse_set(name: str, value: object, *, key: str) -> VectorSetConfig:
    where = f"key {key!r}.sets.{name}"
    if not isinstance(value, Mapping):
        raise EmbeddingConfigError(f"deployment.yaml {where} must be a mapping")
    _refuse_unknown(value, _SET_KEYS, where)
    backend_raw = value.get("backend")
    if not isinstance(backend_raw, str) or not backend_raw.strip():
        raise EmbeddingConfigError(
            f"deployment.yaml {where}.backend must name a backend"
        )
    backend = backend_raw.strip()
    try:
        model_id = model_id_for_spec(backend, vector_set=True)
    except Exception as exc:
        raise EmbeddingConfigError(
            f"deployment.yaml {where}.backend is invalid: {exc}"
        ) from exc

    subtypes_raw = value.get("subtypes", "all")
    if subtypes_raw == "all":
        subtypes: tuple[str, ...] | None = None
    elif isinstance(subtypes_raw, list) and subtypes_raw and all(
        isinstance(item, str) and _SUBTYPE_NAME_RE.match(item)
        for item in subtypes_raw
    ):
        if len(set(subtypes_raw)) != len(subtypes_raw):
            raise EmbeddingConfigError(
                f"deployment.yaml {where}.subtypes names a subtype twice"
            )
        subtypes = tuple(sorted(subtypes_raw))
    else:
        raise EmbeddingConfigError(
            f"deployment.yaml {where}.subtypes must be 'all' or a non-empty "
            "list of subtype names"
        )

    max_chars = _int_in_range(
        value.get("max_chars", VectorSetConfig.max_chars),
        MAX_CHARS_RANGE,
        f"{where}.max_chars",
    )
    prefixes: dict[str, str] = {}
    for prefix_key in ("document_prefix", "query_prefix"):
        raw = value.get(prefix_key, "")
        if not isinstance(raw, str):
            raise EmbeddingConfigError(
                f"deployment.yaml {where}.{prefix_key} must be a string"
            )
        if len(raw) > PREFIX_MAX_CHARS:
            raise EmbeddingConfigError(
                f"deployment.yaml {where}.{prefix_key} has {len(raw)} "
                f"characters; at most {PREFIX_MAX_CHARS}"
            )
        prefixes[prefix_key] = raw

    provider = value.get("provider")
    if provider is not None:
        if not isinstance(provider, str) or not _PROVIDER_RE.match(provider):
            raise EmbeddingConfigError(
                f"deployment.yaml {where}.provider must be a provider name"
            )
        if not model_id.startswith(OPENROUTER_PREFIX):
            raise EmbeddingConfigError(
                f"deployment.yaml {where}.provider applies only to an "
                f"openrouter backend; {backend!r} has no provider"
            )

    document = value.get("document", {})
    if not isinstance(document, Mapping):
        raise EmbeddingConfigError(f"deployment.yaml {where}.document must be a mapping")
    _refuse_unknown(document, _DOCUMENT_KEYS, f"{where}.document")
    default_template = (
        _parse_template(document["default"], f"{where}.document.default")
        if "default" in document else DocumentTemplate()
    )
    types_raw = document.get("types", {})
    if not isinstance(types_raw, Mapping):
        raise EmbeddingConfigError(
            f"deployment.yaml {where}.document.types must be a mapping"
        )
    type_templates: list[tuple[str, DocumentTemplate]] = []
    _refuse_non_string_keys(types_raw, f"{where}.document.types")
    for subtype in sorted(types_raw):
        if not _SUBTYPE_NAME_RE.match(subtype):
            raise EmbeddingConfigError(
                f"deployment.yaml {where}.document.types key {subtype!r} is "
                "not a subtype name"
            )
        if subtypes is not None and subtype not in subtypes:
            raise EmbeddingConfigError(
                f"deployment.yaml {where}.document.types names {subtype!r}, "
                "which the set's subtypes leave out"
            )
        type_templates.append((
            subtype,
            _parse_template(types_raw[subtype], f"{where}.document.types.{subtype}"),
        ))

    storage = value.get("storage", DEFAULT_STORAGE)
    if storage not in STORAGE_TYPES:
        raise EmbeddingConfigError(
            f"deployment.yaml {where}.storage must be one of "
            f"{list(STORAGE_TYPES)}; got {storage!r}"
        )
    index = _parse_index(
        value.get("index", {}), f"{where}.index",
        width=fixed_width_for_model_id(model_id), storage=str(storage),
    )
    return VectorSetConfig(
        name=name,
        backend=backend,
        subtypes=subtypes,
        max_chars=max_chars,
        document_prefix=prefixes["document_prefix"],
        query_prefix=prefixes["query_prefix"],
        provider=provider,
        default_template=default_template,
        type_templates=tuple(type_templates),
        index=index,
        storage=str(storage),
    )


def _parse_sets(value: object, *, key: str) -> tuple[VectorSetConfig, ...]:
    if not isinstance(value, Mapping) or not value:
        raise EmbeddingConfigError(
            f"deployment.yaml key {key!r}.sets must be a non-empty mapping of "
            "set name to set"
        )
    out = []
    _refuse_non_string_keys(value, f"key {key!r}.sets")
    for name in sorted(value):
        if not _SET_NAME_RE.match(name):
            raise EmbeddingConfigError(
                f"deployment.yaml key {key!r}.sets name {name!r} must match "
                "[a-z][a-z0-9_]{0,31}"
            )
        out.append(_parse_set(name, value[name], key=key))
    return tuple(out)


def _canonical_sha256(payload: object) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def parse_embedding_config(
    *,
    singular: object = _MISSING,
    plural: object = _MISSING,
) -> EmbeddingConfig:
    """Normalize the canonical/legacy blocks according to the closed table."""

    if singular is _MISSING and plural is _MISSING:
        return EmbeddingConfig()

    legacy = (
        _parse_block(singular, key="embedding") if singular is not _MISSING else None
    )
    canonical = (
        _parse_block(plural, key="embeddings") if plural is not _MISSING else None
    )
    if legacy is not None and canonical is not None and legacy != canonical:
        raise EmbeddingConfigError(
            "deployment.yaml keys 'embedding' and 'embeddings' conflict; "
            "remove the legacy singular block and keep one canonical value"
        )
    if legacy is not None:
        warnings.warn(
            "deployment.yaml key 'embedding' is deprecated; migrate to the "
            "canonical 'embeddings' spelling",
            EmbeddingConfigDeprecationWarning,
            stacklevel=2,
        )
    return canonical if canonical is not None else legacy  # type: ignore[return-value]


def _fingerprint(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _dimension_mismatch(
    *,
    model_id: str,
    declared: object,
    observed: object,
    set_width: int | None = None,
    set_name: str | None = None,
) -> SubstrateError:
    if set_width is not None:
        which = "the vector set" if set_name is None else f"vector set {set_name!r}"
        err = SubstrateError(
            f"embedding backend capability does not match {which}, which is "
            f"{set_width} wide: model={model_id!r} "
            f"declared={declared!r} observed={observed!r}",
            data={
                "model_id": model_id,
                "declared_dimension": declared,
                "observed_dimension": observed,
                "set": set_name,
                "width": set_width,
            },
            fix_hint="use the set's own backend, which returns vectors of the set's width",
        )
        err.code = "embedder_dim_mismatch"
        return err
    err = SubstrateError(
        "embedding backend capability does not match the substrate's fixed "
        f"{EMBEDDING_DIMENSION}-dimensional column: model={model_id!r} "
        f"declared={declared!r} observed={observed!r}",
        data={
            "model_id": model_id,
            "declared_dimension": declared,
            "observed_dimension": observed,
            "substrate_dimension": EMBEDDING_DIMENSION,
        },
        fix_hint=(
            f"use a backend that declares and actually returns "
            f"{EMBEDDING_DIMENSION}-dimensional vectors"
        ),
    )
    err.code = "embedder_dim_mismatch"
    return err


def build_embedding_execution_plan(
    config: EmbeddingConfig,
    *,
    builder: Callable[[str], Any] = build_embedder,
    probe_text: str = EMBEDDING_PROBE_TEXT,
    paid_backend: str = "probe",
) -> EmbeddingExecutionPlan:
    """Construct and probe a backend before any runtime or graph side effect.

    ``paid_backend`` decides what happens for a paid backend (``openrouter:``)
    and is ignored for a free one, which is always built and probed:

    * ``"probe"``: build it and make the one probe call, as for any backend
      (``okg db embed-backfill``, whose ledger records that call).
    * ``"no_probe"``: build it but make no probe call; the width is the
      backend's declared one and every returned vector is still checked. For
      semantic search (the MCP server and ``okg search``), whose only paid
      call is then the query's own embedding.
    * ``"absent"``: build no backend at all. For processes that never embed
      with a paid backend (the runtime worker, the publisher stack); the
      plan's ``can_embed`` is False and embedding through it raises.
    """
    if paid_backend not in ("probe", "no_probe", "absent"):
        raise ValueError(
            f"paid_backend must be 'probe', 'no_probe' or 'absent'; got {paid_backend!r}"
        )

    if not config.enabled:
        return EmbeddingExecutionPlan(
            config=config,
            embedder=None,
            model_id=None,
            declared_dimension=None,
            observed_dimension=None,
            fingerprint=_fingerprint(
                {
                    "contract_version": EMBEDDING_CONFIG_CONTRACT_VERSION,
                    "enabled": False,
                }
            ),
        )
    if config.backend is None:  # defensive for direct dataclass construction
        raise EmbeddingConfigError(
            "enabled embedding configuration requires an explicit backend"
        )
    # The `sets` form: the plan is the search set's. When its vectors cannot
    # fill the 2560-wide node column (a width other than 2560 that is not
    # an OpenRouter width the node-column backend pads), the plan works at
    # the set's own width and the node-column users skip it (PACT
    # vector-set-pgvector-widths). Otherwise it keeps the node column's
    # shape, so every existing deployment's plan is unchanged.
    search = config.search_vector_set if config.sets else None
    if search is not None and not _fits_node_column(search):
        return build_vector_set_plan(
            search, builder=builder, probe_text=probe_text,
            paid_backend=paid_backend, config=config,
        )
    plural = search is not None
    paid = is_paid_model_id(model_id_for_spec(config.backend, vector_set=plural))
    if paid and paid_backend == "absent":
        spec_model_id = model_id_for_spec(config.backend, vector_set=plural)
        return EmbeddingExecutionPlan(
            config=config,
            embedder=None,
            model_id=spec_model_id,
            declared_dimension=EMBEDDING_DIMENSION,
            observed_dimension=None,
            fingerprint=_fingerprint(
                {
                    "contract_version": EMBEDDING_CONFIG_CONTRACT_VERSION,
                    "backend": config.backend,
                    "model_id": spec_model_id,
                    "probe": "not_run_paid_backend",
                }
            ),
        )

    if builder is build_embedder:
        # The search set's provider pin rides with its backend; a no-probe
        # paid plan is the search path (MCP server, `okg search`), so it gets
        # the short query client (5 s, 2 attempts). A `sets` form backend
        # other than OpenRouter is built as the set's backend: at 2560 wide
        # it returns the node column's vectors, and a sentence-transformers
        # model is then loaded once for the set's width and this plan.
        embedder = build_embedder(
            config.backend,
            query=bool(paid and paid_backend == "no_probe"),
            provider=None if search is None else search.provider,
            vector_set=plural and not paid,
        )
    else:
        embedder = builder(config.backend)
    model_id = embedder_model_id(embedder)
    declared = getattr(embedder, "dimension", None)
    assert_embedder_matches_substrate_width(embedder)
    if paid and paid_backend == "no_probe":
        return EmbeddingExecutionPlan(
            config=config,
            embedder=embedder,
            model_id=model_id,
            declared_dimension=declared,
            observed_dimension=declared,
            fingerprint=_fingerprint(
                {
                    "contract_version": EMBEDDING_CONFIG_CONTRACT_VERSION,
                    "backend": config.backend,
                    "model_id": model_id,
                    "declared_dimension": declared,
                    "probe": "skipped_paid_backend",
                }
            ),
        )
    vector = embedder.embed(probe_text)
    if isinstance(vector, (str, bytes)) or not isinstance(vector, Sequence):
        raise _dimension_mismatch(
            model_id=model_id,
            declared=declared,
            observed="non-vector",
        )
    observed = len(vector)
    if observed != declared or observed != EMBEDDING_DIMENSION:
        raise _dimension_mismatch(
            model_id=model_id,
            declared=declared,
            observed=observed,
        )
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        for value in vector
    ):
        err = SubstrateError(
            f"embedding backend {model_id!r} returned a non-finite or "
            "non-numeric capability-probe vector",
            data={"model_id": model_id, "observed_dimension": observed},
            fix_hint="fix the backend so every output element is finite numeric data",
        )
        err.code = "embedder_output_invalid"
        raise err

    return EmbeddingExecutionPlan(
        config=config,
        embedder=embedder,
        model_id=model_id,
        declared_dimension=declared,
        observed_dimension=observed,
        fingerprint=_fingerprint(
            {
                "contract_version": EMBEDDING_CONFIG_CONTRACT_VERSION,
                "backend": config.backend,
                "model_id": model_id,
                "declared_dimension": declared,
                "observed_dimension": observed,
            }
        ),
    )


def node_column_ann_model_id(config: EmbeddingConfig) -> str | None:
    """The model whose node-column HNSW index (`okg db semantic-index`)
    applies, or None. A `sets` form search set wider than the node column
    has no node-column index (PACT vector-set-pgvector-widths); its own
    index is the set's."""
    if not config.enabled or not config.backend:
        return None
    if not config.sets:
        return model_id_for_spec(config.backend)
    search = config.search_vector_set
    model_id = search.model_id
    if (
        model_id.startswith(OPENROUTER_PREFIX)
        and parse_openrouter_spec(model_id, vector_set=True)[1] > EMBEDDING_DIMENSION
    ):
        return None
    return model_id


def _fits_node_column(vector_set: VectorSetConfig) -> bool:
    """Whether a `sets` form search set's vectors fill the node column: an
    OpenRouter width the node-column backend pads (1..2560), or a width of
    exactly 2560."""
    if vector_set.model_id.startswith(OPENROUTER_PREFIX):
        dims = parse_openrouter_spec(vector_set.model_id, vector_set=True)[1]
        return dims <= EMBEDDING_DIMENSION
    return vector_set.width == EMBEDDING_DIMENSION


def build_vector_set_plan(
    vector_set: VectorSetConfig,
    *,
    builder: Callable[[str], Any] | None = None,
    probe_text: str = EMBEDDING_PROBE_TEXT,
    paid_backend: str = "probe",
    config: EmbeddingConfig | None = None,
) -> EmbeddingExecutionPlan:
    """A plan at one vector set's own width (PACT vector-set-pgvector-widths).

    The backend returns the model's own vectors, with no zero-padding, and
    every vector must have the set's width. A width pgvector cannot hold for
    the set's index kind refuses here, before any backend is built: that is
    where a sentence-transformers set first learns its width. ``builder``
    replaces the backend constructor (tests); ``paid_backend`` is as for
    :func:`build_embedding_execution_plan`. The plan's ``node_column`` is
    False: the node-column worker and the alias embedding backend do not
    use it.
    """
    if paid_backend not in ("probe", "no_probe", "absent"):
        raise ValueError(
            f"paid_backend must be 'probe', 'no_probe' or 'absent'; got {paid_backend!r}"
        )
    if config is None:
        config = EmbeddingConfig(
            enabled=True, backend=vector_set.backend, sets=(vector_set,),
            search_set=vector_set.name,
        )
    model_id = vector_set.model_id
    paid = is_paid_model_id(model_id)
    width = vector_set.width
    refusal = set_width_refusal(width, vector_set.index.kind, vector_set.storage)
    if refusal is not None:
        raise EmbeddingConfigError(f"vector set {vector_set.name!r}: {refusal}")
    base = {
        "contract_version": EMBEDDING_CONFIG_CONTRACT_VERSION,
        "backend": vector_set.backend,
        "model_id": model_id,
        "shape": "vector_set",
        "width": width,
    }
    if paid and paid_backend == "absent":
        return EmbeddingExecutionPlan(
            config=config, embedder=None, model_id=model_id,
            declared_dimension=width, observed_dimension=None,
            fingerprint=_fingerprint({**base, "probe": "not_run_paid_backend"}),
            node_column=False,
        )
    if builder is None or builder is build_embedder:
        embedder = build_embedder(
            vector_set.backend,
            query=bool(paid and paid_backend == "no_probe"),
            provider=vector_set.provider,
            vector_set=True,
        )
    else:
        embedder = builder(vector_set.backend)
    built_model_id = embedder_model_id(embedder)
    if built_model_id != model_id:
        err = SubstrateError(
            f"the backend built for vector set {vector_set.name!r} is "
            f"{built_model_id!r}, but the set holds {model_id!r} vectors",
            data={"query_model_id": built_model_id, "set_model_id": model_id},
            fix_hint="build the set's own backend",
        )
        err.code = "embedding_model_skew"
        raise err
    declared = getattr(embedder, "dimension", None)

    def mismatch(observed: object) -> SubstrateError:
        return _dimension_mismatch(
            model_id=built_model_id, declared=declared, observed=observed,
            set_width=width, set_name=vector_set.name,
        )

    if isinstance(declared, bool) or declared != width:
        raise mismatch(declared)
    if paid and paid_backend == "no_probe":
        return EmbeddingExecutionPlan(
            config=config, embedder=embedder, model_id=built_model_id,
            declared_dimension=declared, observed_dimension=declared,
            fingerprint=_fingerprint({
                **base, "declared_dimension": declared,
                "probe": "skipped_paid_backend",
            }),
            node_column=False,
        )
    vector = embedder.embed(probe_text)
    if isinstance(vector, (str, bytes)) or not isinstance(vector, Sequence):
        raise mismatch("non-vector")
    observed = len(vector)
    if observed != width:
        raise mismatch(observed)
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        for value in vector
    ):
        err = SubstrateError(
            f"embedding backend {built_model_id!r} returned a non-finite or "
            "non-numeric capability-probe vector",
            data={"model_id": built_model_id, "observed_dimension": observed},
            fix_hint="fix the backend so every output element is finite numeric data",
        )
        err.code = "embedder_output_invalid"
        raise err
    return EmbeddingExecutionPlan(
        config=config, embedder=embedder, model_id=built_model_id,
        declared_dimension=declared, observed_dimension=observed,
        fingerprint=_fingerprint({
            **base, "declared_dimension": declared, "observed_dimension": observed,
        }),
        node_column=False,
    )
