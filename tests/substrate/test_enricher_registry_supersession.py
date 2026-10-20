from __future__ import annotations

from okg.substrate.derived_artifacts.change_frontier import (
    invalidations_from_projection_deltas,
)
from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.enrichers.governance import (
    ENRICHER_GOVERNANCE_SCHEMA,
    ENRICHER_GOVERNANCE_STATUS_AVAILABLE,
    ENRICHER_GOVERNANCE_STATUS_UNAVAILABLE,
    ENRICHER_SUPERSESSION_REASON,
    apply_edge_count_projection_delta,
    count_projected_edges_by_type,
    enricher_governance_summary,
    enricher_projection_parity_oracle,
    merge_enricher_projection_deltas,
    record_enricher_governance_failure,
    reproject_frozen_edge_facts,
    supersede_enricher_edge_fact,
    validate_enricher_judgment_registration,
)
from okg.substrate.publisher import Publisher


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *args, **kwargs):
        return None


class _Conn:
    def cursor(self):
        return _Cursor()


def _edge(dedupe_key: str, src: str, edge_type: str, dst: str) -> dict:
    return {
        "dedupe_key": dedupe_key,
        "src_id": src,
        "edge_type": edge_type,
        "dst_id": dst,
        "op": "I",
        "source": "_enricher:judgment_linker",
        "source_record_id": {"enricher": "judgment_linker"},
    }


def test_enricher_registry_supersession_projection_parity_and_delta() -> None:
    producer_calls = 0

    def _producer_output() -> list[dict]:
        nonlocal producer_calls
        producer_calls += 1
        return [
            _edge("edge:a", "node:a", "mentions", "node:b"),
            _edge("edge:b", "node:c", "mentions", "node:d"),
        ]

    frozen_edge_facts = _producer_output()
    frozen = {"edge_facts": frozen_edge_facts}
    kept = reproject_frozen_edge_facts(frozen)

    assert producer_calls == 1
    assert (
        enricher_projection_parity_oracle()
        .diff(
            kept,
            frozen,
            checkout=None,
        )
        .ok
    )

    prior_counts = count_projected_edges_by_type(kept)
    outcome = supersede_enricher_edge_fact(
        frozen_edge_facts,
        target_dedupe_key="edge:a",
        enricher_name="judgment_linker",
        generation_id=77,
        decision_id="decision:override-a",
    )

    assert all(value[0] != "node:a" for value in outcome.projected_edges.values())
    assert outcome.projection_deltas["source"] == "enricher_supersession"
    assert outcome.projection_deltas["edge_count_delta"] == {"mentions": -1}
    retracted_edge_id = str(outcome.projection_deltas["edge_id"])

    invalidations = invalidations_from_projection_deltas(
        outcome.projection_deltas,
    )
    assert [
        (item.dependency_kind, item.dependency_ref, item.dirty_reason)
        for item in invalidations
    ] == [
        ("graph_edge", retracted_edge_id, ENRICHER_SUPERSESSION_REASON),
        ("decision", "decision:override-a", ENRICHER_SUPERSESSION_REASON),
    ]

    incrementally_maintained = apply_edge_count_projection_delta(
        prior_counts,
        outcome.projection_deltas,
    )
    from_scratch = count_projected_edges_by_type(outcome.projected_edges)
    assert incrementally_maintained == from_scratch == {"mentions": 1}
    assert (
        enricher_projection_parity_oracle()
        .diff(
            outcome.projected_edges,
            {
                "edge_facts": frozen_edge_facts,
                "superseded_dedupe_keys": ["edge:a"],
            },
            checkout=None,
        )
        .ok
    )


def test_enricher_registry_supersession_reprojection_applies_retractions_by_fact_order() -> (
    None
):
    projected = reproject_frozen_edge_facts(
        [
            {
                **_edge("edge:a:R", "node:a", "mentions", "node:b"),
                "op": "R",
                "fact_id": 2,
            },
            {
                **_edge("edge:a:I", "node:a", "mentions", "node:b"),
                "fact_id": 1,
            },
        ]
    )

    assert projected == {}
    assert (
        enricher_projection_parity_oracle()
        .diff(
            projected,
            {
                "edge_facts": [
                    {
                        **_edge("edge:a:R", "node:a", "mentions", "node:b"),
                        "op": "R",
                        "fact_id": 2,
                    },
                    {
                        **_edge("edge:a:I", "node:a", "mentions", "node:b"),
                        "fact_id": 1,
                    },
                ],
            },
            checkout=None,
        )
        .ok
    )


def test_enricher_registry_supersession_registers_judgment_row() -> None:
    class _JudgmentEnricher:
        name = "judgment_linker"
        reads_subtypes = ("pact_change",)
        reads_edge_types = ()
        requires_narrowings = ()

    row = validate_enricher_judgment_registration(_JudgmentEnricher())

    assert row["derivation_id"] == "_enricher:judgment_linker"
    assert row["determinism"] == "judgment"
    assert row["oracle"] == "ProjectionParityOracle"
    assert row["override"] is True
    assert row["materializations"] == [
        {"kind": "graph_edges", "storage_table": "okg.graph_edges"}
    ]
    assert "graph_node" in row["declared_deps"]


def test_enricher_registry_supersession_deltas_merge_into_checkpoint_receipt() -> None:
    outcome = supersede_enricher_edge_fact(
        [
            _edge("edge:a", "node:a", "mentions", "node:b"),
            _edge("edge:b", "node:c", "mentions", "node:d"),
        ],
        target_dedupe_key="edge:a",
        enricher_name="judgment_linker",
        generation_id=77,
        decision_id="decision:override-a",
    )

    receipt = merge_enricher_projection_deltas(
        {"edges_retracted": 0, "noop": False},
        outcome.projection_deltas,
    )

    assert receipt["edge_count_delta"] == {"mentions": -1}
    assert receipt["enricher_projection_deltas"] == [outcome.projection_deltas]
    assert [
        (item.dependency_kind, item.dependency_ref)
        for item in invalidations_from_projection_deltas(receipt)
    ] == [
        ("graph_edge", str(outcome.projection_deltas["edge_id"])),
        ("decision", "decision:override-a"),
    ]


def test_enricher_registry_supersession_publisher_checkpoint_receipts() -> None:
    record = record_enricher_governance_failure(
        enricher_name="judgment_linker",
        generation_id=77,
        failure_reason="failed:RuntimeError",
        registry_row={"derivation_id": "_enricher:judgment_linker"},
    )
    outcome = supersede_enricher_edge_fact(
        [_edge("edge:a", "node:a", "mentions", "node:b")],
        target_dedupe_key="edge:a",
        enricher_name="judgment_linker",
        generation_id=77,
    )

    governance, projection_deltas = Publisher._enricher_checkpoint_receipts(
        enricher_runs=[{"name": "judgment_linker", "governance_record": record}],
        projection_deltas={"edges_retracted": 0, "noop": False},
        projection_delta_hints=(outcome.projection_deltas,),
    )

    assert governance["records"] == [record]
    assert projection_deltas["enricher_projection_deltas"] == [
        outcome.projection_deltas
    ]
    assert [
        (item.dependency_kind, item.dependency_ref)
        for item in invalidations_from_projection_deltas(projection_deltas)
    ] == [("graph_edge", str(outcome.projection_deltas["edge_id"]))]


def test_enricher_registry_supersession_failure_record_is_loud() -> None:
    record = record_enricher_governance_failure(
        enricher_name="judgment_linker",
        generation_id=77,
        failure_reason="failed:RuntimeError",
    )

    assert record["schema"] == ENRICHER_GOVERNANCE_SCHEMA
    assert record["governance_status"] == ENRICHER_GOVERNANCE_STATUS_UNAVAILABLE
    assert record["fallback_reason"] == "enricher_failed"
    assert record["output_state"] == "unavailable"
    assert record["publish_behavior"] == "record_and_continue"
    assert "edge_count" not in record


def test_enricher_registry_supersession_governance_summary_is_durable_shape() -> None:
    record = record_enricher_governance_failure(
        enricher_name="judgment_linker",
        generation_id=77,
        failure_reason="failed:RuntimeError",
        registry_row={"derivation_id": "_enricher:judgment_linker"},
    )
    summary = enricher_governance_summary(
        [{"name": "judgment_linker", "governance_record": record}]
    )

    assert summary["schema"] == ENRICHER_GOVERNANCE_SCHEMA
    assert summary["record_count"] == 1
    assert summary["status_counts"] == {ENRICHER_GOVERNANCE_STATUS_UNAVAILABLE: 1}
    assert summary["records"] == [record]


def test_enricher_registry_supersession_publisher_records_governance(
    monkeypatch,
) -> None:
    class _GoodEnricher:
        name = "good"
        reads_subtypes = ("pact_change",)
        reads_edge_types = ()
        requires_narrowings = ()
        # Explicit minimal envelope: the boot guard demands a declared
        # write surface; this fixture's behavior under test is
        # governance/lineage, not admission.
        write_envelope = {
            "edges": [{"src_subtype": "pact_change", "edge_type": "references", "dst_subtype": "pact_change"}],
            "nondeterministic": False,
        }

        def enrich(self, conn, *, generation_id: int, incremental=None):
            return EnrichResult(self.name, n_edges_emitted=1)

    class _FailingEnricher:
        name = "bad"
        reads_subtypes = ("pact_change",)
        reads_edge_types = ()
        requires_narrowings = ()
        # Explicit minimal envelope: the boot guard demands a declared
        # write surface; this fixture's behavior under test is
        # governance/lineage, not admission.
        write_envelope = {
            "edges": [{"src_subtype": "pact_change", "edge_type": "references", "dst_subtype": "pact_change"}],
            "nondeterministic": False,
        }

        def enrich(self, conn, *, generation_id: int, incremental=None):
            raise RuntimeError("boom")

    publisher = Publisher(
        "postgres://unused",
        "test",
        enrichers=[_GoodEnricher(), _FailingEnricher()],
    )
    admitted_failure_reasons: list[str | None] = []

    monkeypatch.setattr(
        "okg.substrate.publisher.publisher.persist_transform_program",
        lambda conn, program: None,
    )
    monkeypatch.setattr(
        publisher,
        "_load_enricher_progress_rows",
        lambda conn, names: {name: (0, 0, "never", None, None) for name in names},
    )
    monkeypatch.setattr(
        publisher,
        "_load_enricher_profile_fingerprints",
        lambda conn, producer_ids: {},
    )
    monkeypatch.setattr(publisher, "_current_fact_watermarks", lambda conn: (0, 0))
    monkeypatch.setattr(publisher, "_current_catalog_version_id", lambda conn: 7)
    monkeypatch.setattr(
        publisher,
        "_query_dirty_from_chronos_current_segment_by_type",
        lambda conn, *, branch_name, previous=None, timing_sink=None, timing_prefix=None: (
            {"pact_change": ["pact:1"]},
            {},
            True,
            None,
            {},
            None,
        ),
    )
    monkeypatch.setattr(
        publisher,
        "_query_dirty_from_chronos_branch",
        lambda conn, reads_subtypes, *, branch_name: ["pact:1"],
    )
    monkeypatch.setattr(
        publisher,
        "_expected_enricher_profile_fingerprint",
        lambda conn, *, enricher, producer_id, catalog_version_id: f"fp:{producer_id}",
    )
    monkeypatch.setattr(
        publisher,
        "_materialize_enricher_profile",
        lambda *args, **kwargs: "fp",
    )
    monkeypatch.setattr(
        publisher,
        "_admit_enricher_outputs",
        lambda *args, **kwargs: admitted_failure_reasons.append(
            kwargs.get("failure_reason")
        ),
    )
    monkeypatch.setattr(
        publisher,
        "_advance_enricher_progress",
        lambda *args, **kwargs: None,
    )

    runs = publisher._run_enrichers(
        _Conn(),
        generation_id=77,
        catalog_version_id=7,
    )

    good, bad = runs[1], runs[2]
    assert good["status"] == "ok"
    assert good["governance_status"] == ENRICHER_GOVERNANCE_STATUS_AVAILABLE
    assert good["governance_record"]["materialization"] == {
        "kind": "edge_facts",
        "projection": "project_new_edges",
    }
    assert good["lineage_envelope"]["materialization"] == {
        "kind": "edge_facts",
        "projection": "project_new_edges",
    }

    assert bad["status"] == "failed"
    assert bad["governance_status"] == ENRICHER_GOVERNANCE_STATUS_UNAVAILABLE
    assert bad["governance_record"]["fallback_reason"] == "enricher_failed"
    assert bad["governance_record"]["publish_behavior"] == "record_and_continue"
    assert admitted_failure_reasons == [None, "failed:RuntimeError"]
