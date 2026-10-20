"""Edge-identity projection: what counts as the same edge, and what refuses.

These are pure predicates over one plan's inputs, so they need no database and
run in the per-PR fast lane. The database-backed consequences — a merge that
actually deduplicates and applies — live in test_merge_dedup_provenance.py.
"""

from __future__ import annotations

from okg.substrate.entity_merge import (
    PROVENANCE_ATTR_KEYS,
    _candidate_record_identity,
    _edge_identity,
    _provenance_variance,
    _survivor_lineage_narrowing,
    _union_values,
)
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    RECORD_DETAIL_KEY,
    RECORD_DETAIL_TRUNCATED,
    normalize_source_lineage,
)


def _lineage(*, source_id: str = "extracted-source", record: str = "chunk-1") -> dict:
    return normalize_source_lineage(
        None,
        source_id=source_id,
        source_record_id={"chunk": record},
        source_revision={"run_id": "run-a"},
    )


def _edge(**overrides) -> dict:
    row = {
        "src": "paper:cooper",
        "dst": "concept:signal-region",
        "edge_type": "mentions",
        "attrs": {"grounding": "section-2", LINEAGE_ATTR: _lineage()},
        "provenance": "extracted_model",
        "confidence": 0.91,
        "source": "extracted-source",
        "source_record_id": {"chunk": "chunk-1"},
        "source_revision": {"run_id": "run-a", "generation_id": 0},
    }
    row.update(overrides)
    return row


SURVIVOR_CANDIDATE = "concept:Signal Region"
RETIRED_CANDIDATE = "concept:signal-region"
OTHER_CANDIDATE = "concept:signal region alternate"


def _candidate_edge(
    node_id: str,
    *,
    chunk_node_id: str = "paper:chunk-1",
    enricher: str = "cooper_physics_extraction",
    span: int = 12,
    policy_fingerprint: str = "policy-a",
    confidence: float = 0.91,
) -> dict:
    record = {
        "chunk_node_id": chunk_node_id,
        "enricher": enricher,
        "node_id": node_id,
        "span": span,
    }
    return _edge(
        source_record_id=record,
        confidence=confidence,
        attrs={
            "grounding": "section-2",
            LINEAGE_ATTR: normalize_source_lineage(
                None,
                source_id="extracted-source",
                source_record_id=record,
                source_revision={"run_id": "run-a"},
                policy_fingerprint=policy_fingerprint,
            ),
        },
    )


def test_run_bookkeeping_does_not_decide_sameness() -> None:
    """The defect in issue #1042: two runs over the same chunk never agreed."""
    first = _edge()
    second = _edge(
        source_revision={"run_id": "run-b", "generation_id": 0},
        attrs={
            "grounding": "section-2",
            LINEAGE_ATTR: normalize_source_lineage(
                None,
                source_id="extracted-source",
                source_record_id={"chunk": "chunk-1"},
                source_revision={"run_id": "run-b"},
            ),
        },
    )
    first_identity, first_reason = _edge_identity(first)
    second_identity, second_reason = _edge_identity(second)
    assert (first_reason, second_reason) == (None, None)
    assert first_identity == second_identity


def test_meaning_still_decides_sameness() -> None:
    identity, _ = _edge_identity(_edge())
    for changed in (
        {"confidence": 0.5},
        {"provenance": "explicit"},
        {"source": "curated-source"},
        {"source_record_id": {"chunk": "chunk-2"}},
        {"attrs": {"grounding": "section-9", LINEAGE_ATTR: _lineage()}},
    ):
        other, reason = _edge_identity(_edge(**changed))
        assert reason is None
        assert other != identity, changed


def test_candidate_node_bookkeeping_normalizes_for_only_the_explicit_pair() -> None:
    survivor = _candidate_edge(SURVIVOR_CANDIDATE)
    retired = _candidate_edge(RETIRED_CANDIDATE)
    raw_survivor, _ = _edge_identity(survivor)
    raw_retired, _ = _edge_identity(retired)
    assert raw_survivor != raw_retired

    projected_survivor, survivor_reason = _edge_identity(
        survivor,
        survivor_node_id=SURVIVOR_CANDIDATE,
        retired_node_id=RETIRED_CANDIDATE,
    )
    projected_retired, retired_reason = _edge_identity(
        retired,
        survivor_node_id=SURVIVOR_CANDIDATE,
        retired_node_id=RETIRED_CANDIDATE,
    )
    assert (survivor_reason, retired_reason) == (None, None)
    assert projected_survivor == projected_retired
    assert projected_retired["source_record_id"]["node_id"] == (SURVIVOR_CANDIDATE)
    assert projected_retired["lineage"] == projected_survivor["lineage"]

    unrelated, _ = _edge_identity(
        _candidate_edge(OTHER_CANDIDATE),
        survivor_node_id=SURVIVOR_CANDIDATE,
        retired_node_id=RETIRED_CANDIDATE,
    )
    assert unrelated != projected_survivor
    assert unrelated["source_record_id"]["node_id"] == OTHER_CANDIDATE


def test_candidate_record_projection_requires_exact_extraction_shape() -> None:
    generic = {"node_id": RETIRED_CANDIDATE}
    assert (
        _candidate_record_identity(
            generic,
            survivor_node_id=SURVIVOR_CANDIDATE,
            retired_node_id=RETIRED_CANDIDATE,
        )
        == generic
    )

    nested = {
        "chunk_node_id": "paper:chunk-1",
        "enricher": "cooper_physics_extraction",
        "node_id": RETIRED_CANDIDATE,
        "owner": {"node_id": RETIRED_CANDIDATE},
    }
    assert _candidate_record_identity(
        nested,
        survivor_node_id=SURVIVOR_CANDIDATE,
        retired_node_id=RETIRED_CANDIDATE,
    ) == {
        "chunk_node_id": "paper:chunk-1",
        "enricher": "cooper_physics_extraction",
        "node_id": SURVIVOR_CANDIDATE,
        "owner": {"node_id": RETIRED_CANDIDATE},
    }

    generic_survivor, _ = _edge_identity(
        _edge(source_record_id={"node_id": SURVIVOR_CANDIDATE}),
        survivor_node_id=SURVIVOR_CANDIDATE,
        retired_node_id=RETIRED_CANDIDATE,
    )
    generic_retired, _ = _edge_identity(
        _edge(source_record_id={"node_id": RETIRED_CANDIDATE}),
        survivor_node_id=SURVIVOR_CANDIDATE,
        retired_node_id=RETIRED_CANDIDATE,
    )
    assert generic_survivor != generic_retired

    generic_lineage_survivor, _ = _edge_identity(
        _edge(
            source_record_id={"node_id": SURVIVOR_CANDIDATE},
            attrs={
                "grounding": "section-2",
                LINEAGE_ATTR: normalize_source_lineage(
                    None,
                    source_id="extracted-source",
                    source_record_id={"node_id": SURVIVOR_CANDIDATE},
                    source_revision={"run_id": "run-a"},
                ),
            },
        ),
        survivor_node_id=SURVIVOR_CANDIDATE,
        retired_node_id=RETIRED_CANDIDATE,
    )
    generic_lineage_retired, _ = _edge_identity(
        _edge(
            source_record_id={"node_id": RETIRED_CANDIDATE},
            attrs={
                "grounding": "section-2",
                LINEAGE_ATTR: normalize_source_lineage(
                    None,
                    source_id="extracted-source",
                    source_record_id={"node_id": RETIRED_CANDIDATE},
                    source_revision={"run_id": "run-a"},
                ),
            },
        ),
        survivor_node_id=SURVIVOR_CANDIDATE,
        retired_node_id=RETIRED_CANDIDATE,
    )
    assert generic_lineage_survivor != generic_lineage_retired


def test_candidate_record_projection_preserves_every_other_identity_field() -> None:
    identity, _ = _edge_identity(
        _candidate_edge(RETIRED_CANDIDATE),
        survivor_node_id=SURVIVOR_CANDIDATE,
        retired_node_id=RETIRED_CANDIDATE,
    )
    for changed in (
        {"chunk_node_id": "paper:chunk-2"},
        {"enricher": "other_enricher"},
        {"span": 99},
        {"policy_fingerprint": "policy-b"},
    ):
        other, reason = _edge_identity(
            _candidate_edge(RETIRED_CANDIDATE, **changed),
            survivor_node_id=SURVIVOR_CANDIDATE,
            retired_node_id=RETIRED_CANDIDATE,
        )
        assert reason is None
        assert other != identity, changed


def test_candidate_projection_keeps_confidence_identity_bearing() -> None:
    survivor, _ = _edge_identity(
        _candidate_edge(SURVIVOR_CANDIDATE, confidence=0.91),
        survivor_node_id=SURVIVOR_CANDIDATE,
        retired_node_id=RETIRED_CANDIDATE,
    )
    retired, reason = _edge_identity(
        _candidate_edge(RETIRED_CANDIDATE, confidence=0.90),
        survivor_node_id=SURVIVOR_CANDIDATE,
        retired_node_id=RETIRED_CANDIDATE,
    )
    assert reason is None
    assert retired != survivor


def test_differing_lineage_source_record_or_fingerprint_blocks_dedup() -> None:
    """Retiring one of these would narrow what a read policy can see."""
    identity, _ = _edge_identity(_edge())
    for lineage in (
        _lineage(source_id="other-source"),
        _lineage(record="chunk-2"),
        normalize_source_lineage(
            None,
            source_id="extracted-source",
            source_record_id={"chunk": "chunk-1"},
            source_revision={"run_id": "run-a"},
            policy_fingerprint="policy-v2",
        ),
    ):
        other, reason = _edge_identity(
            _edge(attrs={"grounding": "section-2", LINEAGE_ATTR: lineage})
        )
        assert reason is None
        assert other != identity


def test_a_declared_provenance_key_is_projected_out_of_identity() -> None:
    """A deployment can name its own per-run attribute key without every
    cross-run pair blocking; the set is a constant, not an inline literal."""
    assert "nomos_source_lineage" in PROVENANCE_ATTR_KEYS
    identity, _ = _edge_identity(_edge())
    assert LINEAGE_ATTR not in identity["attrs"]
    assert "grounding" in identity["attrs"]


def test_malformed_lineage_refuses_instead_of_agreeing_on_emptiness() -> None:
    """Two unknown lineages must not read as two matching lineages.

    `source_ids_from_lineage` returns an empty tuple for a malformed envelope,
    so comparing the projections alone would make two unknowns compare equal.
    """
    malformed = {"schema": "okg.nomos.source_lineage.v1", "status": "malformed"}
    identity, reason = _edge_identity(
        _edge(attrs={"grounding": "section-2", LINEAGE_ATTR: malformed})
    )
    assert reason == "lineage_malformed"


def test_truncated_lineage_refuses() -> None:
    """Two envelopes can share every retained record and differ in the rest."""
    truncated = dict(_lineage())
    truncated[RECORD_DETAIL_KEY] = RECORD_DETAIL_TRUNCATED
    _, reason = _edge_identity(
        _edge(attrs={"grounding": "section-2", LINEAGE_ATTR: truncated})
    )
    assert reason == "lineage_truncated"


def test_absent_lineage_compares_equal_to_absent_lineage() -> None:
    """A deployment that stamps no lineage has no read policy to narrow."""
    bare = {"grounding": "section-2"}
    first, first_reason = _edge_identity(_edge(attrs=dict(bare)))
    second, second_reason = _edge_identity(_edge(attrs=dict(bare)))
    assert (first_reason, second_reason) == (None, None)
    assert first == second
    present, _ = _edge_identity(_edge())
    assert present != first


def test_provenance_variance_names_what_is_not_carried_over() -> None:
    surviving = _edge()
    retiring = _edge(source_revision={"run_id": "run-b", "generation_id": 0})
    variance = _provenance_variance(surviving, retiring)
    assert variance["source_revision"]["kept"] == {
        "run_id": "run-a",
        "generation_id": 0,
    }
    assert variance["source_revision"]["not_carried_over"] == {
        "run_id": "run-b",
        "generation_id": 0,
    }


def test_provenance_variance_is_empty_when_nothing_differs() -> None:
    assert _provenance_variance(_edge(), _edge()) == {}


def _survivor(lineage) -> dict:
    return {
        "node_id": "concept:vv",
        "source": "concept-source",
        "source_record_id": {"chunk": "chunk-1"},
        "source_revision": {"run_id": "run-a"},
        "attrs": {"name": "VV", LINEAGE_ATTR: lineage},
    }


def test_survivor_write_that_reproduces_its_own_lineage_is_allowed() -> None:
    lineage = normalize_source_lineage(
        None,
        source_id="concept-source",
        source_record_id={"chunk": "chunk-1"},
        source_revision={"run_id": "run-a"},
    )
    assert _survivor_lineage_narrowing(_survivor(lineage)) is None


def test_survivor_write_that_would_drop_a_source_record_is_refused() -> None:
    """Merging citations must not shrink the survivor's provenance as a side
    effect. The write path rebuilds a node's lineage from the batch source, so a
    survivor carrying several records would come back carrying one."""
    accumulated = normalize_source_lineage(
        None,
        source_id="concept-source",
        source_record_id={"chunk": "chunk-1"},
        source_revision={"run_id": "run-a"},
    )
    # A second record on the same source — what a derived node accumulates.
    accumulated["sources"][0]["source_record_ids"].append({"chunk": "chunk-2"})
    accumulated["sources"][0]["source_records"].append(
        {"source_record_id": {"chunk": "chunk-2"}}
    )
    refusal = _survivor_lineage_narrowing(_survivor(accumulated))
    assert refusal is not None
    assert refusal["kind"] == "survivor_lineage_narrowing"
    assert refusal["lost_source_record_refs"]


def test_union_order_does_not_depend_on_which_side_was_read_first() -> None:
    """`plan_digest` is compared across processes, so the union has to be
    reproducible rather than merely correct as a set."""
    left = ["chunk:b", "chunk:a"]
    right = ["chunk:c", "chunk:a"]
    assert _union_values(left, right) == _union_values(right, left)
    assert _union_values(left, right) == ["chunk:a", "chunk:b", "chunk:c"]


def test_union_deduplicates_dicts_by_value_not_by_object() -> None:
    entry = {"chunk": "chunk-1", "span": 2}
    assert _union_values([entry], [dict(entry)]) == [entry]
