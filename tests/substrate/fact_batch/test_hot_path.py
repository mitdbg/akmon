"""Contract pins for the single normalize-and-hash path in ``FactChunk.create``.

``FactChunk.create`` used to freeze every fact and hash the chunk, then hand
the result to the public dataclass constructor, whose ``__post_init__``
re-froze every fact and recomputed the same digest to verify it. Collapsing
that to one pass is only safe if the canonical bytes and every refusal survive
unchanged, so this module pins:

* the exact canonical digest for a fixed fixture (nested mappings, non-ASCII,
  floats, lists, tuples, bools, ``None``) -- the values were computed on the
  two-pass implementation and must never move;
* that the public constructor still verifies a caller-supplied digest;
* that ``create`` still raises the same refusals for ordinals, chunk sizes,
  forbidden keys, non-finite numbers, and non-serializable values;
* that ``from_dict`` round-trips to a chunk equal to ``create``'s.
"""

from __future__ import annotations

import uuid
from types import MappingProxyType

import pytest

from okg.substrate.fact_batch import (
    MAX_FACTS_PER_CHUNK,
    MAX_MARKERS_PER_CHUNK,
    FactBatchAuthorityError,
    FactBatchFieldError,
    FactBatchIntegrityError,
    FactChunk,
    canonical_chunk_digest,
    canonical_marker_digest,
    validate_chunk_authority,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact, ProgressMarker
from okg.substrate.producer_authority import ProducerProfile

GOLDEN_RUN_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
GOLDEN_ORDINAL = 7

# Computed on the two-pass implementation (origin/dev at a55095a7755d) before
# the single normalize+hash change. A move here is a wire-format break.
GOLDEN_CHUNK_DIGEST = (
    "sha256:56382cb1f777125d7283ed57b871f6a5d893ffa341ae61e0c4aa3e8cde9d106d"
)
GOLDEN_MARKER_DIGEST = (
    "sha256:bc7295edcf3b16150dd4e65a3b901a505ce3b8f5905dc5fefc0c0eeec56dd926"
)


def golden_facts() -> list[NodeFact | EdgeFact]:
    """A fixture that exercises every canonicalization branch."""

    return [
        NodeFact(
            node_id="cms:claim:00000001",
            subtype="ClaimLine",
            attrs={
                "zeta": "last key sorts last",
                "alpha": None,
                "title": "Claim — résumé ünïcode 日本語",
                "amount": 1234.5,
                "negative": -0.125,
                "flags": {"denied": False, "review": True, "count": 3},
                "codes": ["A01", "B22", "C03"],
                "tuple_values": ("x", "y"),
                "nested": {
                    "provider": {
                        "npi": "1000000001",
                        "geo": [42.3601, -71.0942],
                        "aliases": ("Clínica", "Nacional"),
                    }
                },
            },
            source_record_id={"file": "part-000.csv", "row": 1},
            source_revision={"sha": "deadbeef", "mtime": 1725000000},
        ),
        EdgeFact(
            src="cms:claim:00000001",
            dst="cms:provider:000042",
            edge_type="billed_by",
            attrs={
                "weight": 0.75,
                "window": {"start": "2026-01-01", "end": "2026-03-31"},
                "tags": ["primary", "verified"],
                "empty_map": {},
                "empty_list": [],
            },
            provenance="explicit",
            confidence=0.95,
            source_record_id={"file": "part-000.csv", "row": 2},
            source_revision={"sha": "cafebabe", "mtime": 1725000001},
        ),
        NodeFact(
            node_id="cms:provider:000042",
            subtype="Provider",
            attrs={"name": "Clínica Nacional"},
            source_record_id={},
            source_revision={},
            op="r",
            ownership_mode="substitute",
        ),
    ]


def golden_markers() -> list[ProgressMarker]:
    return [
        ProgressMarker("record:1", "fingerprint-1"),
        ProgressMarker("record:2", "fingerprint-2"),
    ]


def _golden_chunk() -> FactChunk:
    return FactChunk.create(
        run_id=GOLDEN_RUN_ID,
        ordinal=GOLDEN_ORDINAL,
        facts=golden_facts(),
        progress_markers=golden_markers(),
    )


def test_create_digest_matches_pinned_canonical_bytes() -> None:
    assert _golden_chunk().digest == GOLDEN_CHUNK_DIGEST


def test_canonical_chunk_digest_helper_agrees_with_create() -> None:
    assert (
        canonical_chunk_digest(
            run_id=GOLDEN_RUN_ID,
            ordinal=GOLDEN_ORDINAL,
            facts=golden_facts(),
            progress_markers=golden_markers(),
        )
        == GOLDEN_CHUNK_DIGEST
    )
    assert canonical_marker_digest(golden_markers()) == GOLDEN_MARKER_DIGEST


def test_digest_is_stable_across_frozen_and_raw_inputs() -> None:
    """Re-normalizing an already-frozen chunk must not move the digest."""

    chunk = _golden_chunk()
    assert (
        canonical_chunk_digest(
            run_id=chunk.run_id,
            ordinal=chunk.ordinal,
            facts=chunk.facts,
            progress_markers=chunk.progress_markers,
        )
        == chunk.digest
    )


def test_create_freezes_nested_attrs() -> None:
    chunk = _golden_chunk()
    node = chunk.facts[0]
    assert isinstance(node.attrs, MappingProxyType)
    assert isinstance(node.attrs["nested"], MappingProxyType)
    assert isinstance(node.attrs["nested"]["provider"], MappingProxyType)
    assert node.attrs["codes"] == ("A01", "B22", "C03")
    assert node.attrs["nested"]["provider"]["geo"] == (42.3601, -71.0942)
    with pytest.raises(TypeError):
        node.attrs["nested"]["provider"]["npi"] = "tampered"
    assert chunk.facts[2].op == "R"


def test_public_constructor_still_verifies_caller_digest() -> None:
    chunk = _golden_chunk()
    rebuilt = FactChunk(
        run_id=chunk.run_id,
        ordinal=chunk.ordinal,
        facts=chunk.facts,
        progress_markers=chunk.progress_markers,
        digest=chunk.digest,
    )
    assert rebuilt == chunk
    with pytest.raises(FactBatchIntegrityError, match="chunk digest mismatch"):
        FactChunk(
            run_id=chunk.run_id,
            ordinal=chunk.ordinal,
            facts=chunk.facts,
            progress_markers=chunk.progress_markers,
            digest="sha256:" + "0" * 64,
        )


def test_from_dict_roundtrip_equals_create() -> None:
    chunk = _golden_chunk()
    assert FactChunk.from_dict(chunk.to_dict()) == chunk


@pytest.mark.parametrize("ordinal", [-1, True, 1.0, "3"])
def test_create_refuses_bad_ordinal(ordinal: object) -> None:
    with pytest.raises(FactBatchFieldError, match="ordinal must be non-negative"):
        FactChunk.create(run_id=GOLDEN_RUN_ID, ordinal=ordinal)  # type: ignore[arg-type]


def test_create_refuses_oversize_fact_and_marker_counts() -> None:
    fact = NodeFact(
        node_id="n",
        subtype="S",
        attrs={},
        source_record_id={},
        source_revision={},
    )
    with pytest.raises(FactBatchFieldError, match="MAX_FACTS_PER_CHUNK"):
        FactChunk.create(
            run_id=GOLDEN_RUN_ID,
            ordinal=0,
            facts=[fact] * (MAX_FACTS_PER_CHUNK + 1),
        )
    marker = ProgressMarker("record:1", "fingerprint-1")
    with pytest.raises(FactBatchFieldError, match="MAX_MARKERS_PER_CHUNK"):
        FactChunk.create(
            run_id=GOLDEN_RUN_ID,
            ordinal=0,
            progress_markers=[marker] * (MAX_MARKERS_PER_CHUNK + 1),
        )


@pytest.mark.parametrize(
    ("attrs", "message"),
    [
        ({"api_key": "sk-live"}, "forbidden control/secret field"),
        ({"nested": {"deep": [{"secret": "x"}]}}, "forbidden control/secret field"),
        ({"score": float("nan")}, "non-finite number"),
        ({"nested": {"score": float("inf")}}, "non-finite number"),
        ({"callback": len}, "callback/client object"),
        ({"blob": {1, 2}}, "non-serializable set"),
        ({"outer": {2: "non-string key"}}, "non-string key"),
    ],
)
def test_create_still_refuses_unsafe_fact_attrs(
    attrs: dict[str, object], message: str
) -> None:
    fact = NodeFact(
        node_id="n",
        subtype="S",
        attrs=attrs,
        source_record_id={},
        source_revision={},
    )
    with pytest.raises(FactBatchFieldError, match=message):
        FactChunk.create(run_id=GOLDEN_RUN_ID, ordinal=0, facts=[fact])


def test_create_allows_control_plane_names_in_fact_data() -> None:
    """Fact DATA keeps ``command``/``client``; only the control surface bans them."""

    fact = NodeFact(
        node_id="n",
        subtype="S",
        attrs={"command": "pytest -q", "client": "ci-agent"},
        source_record_id={},
        source_revision={},
    )
    chunk = FactChunk.create(run_id=GOLDEN_RUN_ID, ordinal=0, facts=[fact])
    assert chunk.facts[0].attrs["command"] == "pytest -q"


def test_create_refuses_non_finite_edge_confidence() -> None:
    edge = EdgeFact(
        src="a",
        dst="b",
        edge_type="e",
        confidence=float("nan"),
    )
    with pytest.raises(FactBatchFieldError, match="edge confidence must be finite"):
        FactChunk.create(run_id=GOLDEN_RUN_ID, ordinal=0, facts=[edge])


def test_create_refuses_foreign_fact_and_marker_types() -> None:
    with pytest.raises(FactBatchFieldError, match="must be NodeFact or EdgeFact"):
        FactChunk.create(run_id=GOLDEN_RUN_ID, ordinal=0, facts=[object()])  # type: ignore[list-item]
    with pytest.raises(FactBatchFieldError, match="must contain ProgressMarker"):
        FactChunk.create(
            run_id=GOLDEN_RUN_ID,
            ordinal=0,
            progress_markers=[object()],  # type: ignore[list-item]
        )


# Digests recorded on origin/dev (a55095a7755d) for facts the freezer never
# saw. ``canonical_chunk_digest`` is public, and an unfrozen fact can carry
# non-string mapping keys, which the old code coerced with ``str(key)`` before
# sorting -- so "10" sorted before "2". These pin that ordering.
UNFROZEN_INT_KEY_DIGEST = (
    "sha256:f11586b45eb55c0d90210f0b4b1dd70792b7c0fa16c5cac617c5f1f3a057a0f7"
)
UNFROZEN_MIXED_KEY_DIGEST = (
    "sha256:239d127175b42fe4b3390cd8285fb49552c2d2bcbb73a44613f9a10498c0d70b"
)


def _profile(allowed_record_kinds: tuple[str, ...]) -> ProducerProfile:
    return ProducerProfile(
        deployment_name="test",
        producer_id="source:test",
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={"repo": "okg"},
        allowed_record_kinds=allowed_record_kinds,
        output_signature={"nodes": [], "edges": []},
        policy_clauses=(),
        output_scope_summary={},
        source_ref={"source": "test"},
        catalog_version_id=7,
        profile_fingerprint="sha256:" + "a" * 64,
        status="active",
    )


def test_fact_refusal_precedes_ordinal_and_size_refusal() -> None:
    """A chunk wrong in two ways still reports the fact first, as before.

    ``create`` froze every fact before reaching the constructor, so a bad fact
    was always the refusal a caller saw. Moving the ordinal and size checks
    into ``create`` must not reorder that.
    """

    bad_fact = NodeFact(
        node_id="n",
        subtype="S",
        attrs={"api_key": "sk-live"},
        source_record_id={},
        source_revision={},
    )
    with pytest.raises(FactBatchFieldError, match="forbidden control/secret field"):
        FactChunk.create(run_id=GOLDEN_RUN_ID, ordinal=-1, facts=[bad_fact])
    with pytest.raises(FactBatchFieldError, match="must be NodeFact or EdgeFact"):
        FactChunk.create(run_id=GOLDEN_RUN_ID, ordinal="x", facts=[object()])  # type: ignore[arg-type,list-item]
    with pytest.raises(FactBatchFieldError, match="forbidden control/secret field"):
        FactChunk.create(
            run_id=GOLDEN_RUN_ID,
            ordinal=0,
            facts=[bad_fact] * (MAX_FACTS_PER_CHUNK + 1),
        )


@pytest.mark.parametrize(
    ("attrs", "expected"),
    [
        ({"m": {2: "a", 10: "b"}}, UNFROZEN_INT_KEY_DIGEST),
        ({"m": {2: "a", "b": "c"}}, UNFROZEN_MIXED_KEY_DIGEST),
    ],
)
def test_unfrozen_non_string_keys_keep_their_digest(
    attrs: dict[str, object], expected: str
) -> None:
    fact = NodeFact(
        node_id="n",
        subtype="S",
        attrs=attrs,
        source_record_id={},
        source_revision={},
    )
    assert (
        canonical_chunk_digest(
            run_id=GOLDEN_RUN_ID, ordinal=0, facts=[fact], progress_markers=[]
        )
        == expected
    )


def test_validate_chunk_authority_without_signature_still_refuses() -> None:
    """Skipping the signature payload must not skip an authority refusal."""

    profile = _profile(("node",))
    edge_chunk = FactChunk.create(
        run_id=GOLDEN_RUN_ID,
        ordinal=0,
        facts=[EdgeFact(src="a", dst="b", edge_type="e")],
    )
    with pytest.raises(FactBatchAuthorityError, match="not allowed to emit edge"):
        validate_chunk_authority(
            edge_chunk,
            profile=profile,
            retractions_allowed=False,
            validate_signature=False,
        )
    retraction_chunk = FactChunk.create(
        run_id=GOLDEN_RUN_ID,
        ordinal=0,
        facts=[
            NodeFact(
                node_id="n",
                subtype="S",
                attrs={},
                source_record_id={},
                source_revision={},
                op="R",
            )
        ],
    )
    with pytest.raises(FactBatchAuthorityError, match="retraction requires"):
        validate_chunk_authority(
            retraction_chunk,
            profile=profile,
            retractions_allowed=False,
            validate_signature=False,
        )


def test_validate_chunk_authority_without_signature_admits_allowed_facts() -> None:
    validate_chunk_authority(
        _golden_chunk(),
        profile=_profile(("node", "edge")),
        retractions_allowed=True,
        validate_signature=False,
    )
