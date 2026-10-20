"""Published-generation proof for extraction-enricher snapshot replacement.

Issue #1135 is a graph-state defect, not a call-count defect: a prompt change
causes the LLM outcome key to change and deliberately re-reads every input, but
the old extraction used to remain live beside the new one.  These tests use the
real Publisher and Chronos graph so a passing unit-level INSERT is not mistaken
for convergence.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.enrichers import _fact_emitters
from okg.substrate.library.enrichers._fact_emitters import FactEmitOutcome
from okg.substrate.library.enrichers.extraction import claim_extractor as claim_module
from okg.substrate.library.enrichers.extraction.base import make_snapshot_fact_key
from okg.substrate.library.enrichers.extraction.claim_extractor import ClaimExtractor
from okg.substrate.library.enrichers.extraction.concept_extractor import (
    ConceptExtractor,
)
from okg.substrate.library.enrichers.extraction.evidence_extractor import (
    EvidenceExtractor,
)
from okg.substrate.library.enrichers.extraction.hypothesis_extractor import (
    HypothesisExtractor,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.publisher.enricher_loader import (
    EnricherLoadError,
    validate_enricher_envelopes,
)
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


class _MutableLLM:
    model = "issue-1135-fixture"

    def __init__(self, response: str) -> None:
        self.response = response
        self.truncated = False
        self.error: Exception | None = None

    @property
    def last_call_stats(self) -> dict[str, Any]:
        return {"cost_usd": 0.0, "truncated": self.truncated}

    def complete(self, system: str, user: str, **_: Any) -> str:
        del system, user
        if self.error is not None:
            raise self.error
        return self.response


class _CapturingClaimExtractor(ClaimExtractor):
    def __init__(self, **kwargs: Any) -> None:
        self.results = []
        super().__init__(**kwargs)

    def enrich(self, conn, *, generation_id: int, **kwargs):
        result = super().enrich(conn, generation_id=generation_id, **kwargs)
        self.results.append(result)
        return result


_TWO_CLAIMS = """[
  {"text": "Alpha is measurable", "kind": "empirical", "confidence": 0.9},
  {"text": "Beta exceeds alpha", "kind": "comparative", "confidence": 0.8}
]"""

_ONE_REVISED_CLAIM = """[
  {"text": "Gamma supersedes both", "kind": "theoretical", "confidence": 0.95}
]"""


def _seed_extraction_graph(
    conn: psycopg.Connection,
    *,
    deployment: str,
) -> None:
    catalog_version_id = seed_catalog(
        conn,
        subtypes=[
            "paragraph",
            "theorem",
            "table",
            "claim",
            "extraction_dropped",
        ],
        narrowings=[
            ("paragraph", "contains", "claim"),
            ("theorem", "states", "claim"),
            ("table", "states", "claim"),
        ],
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.producer_profiles
              (deployment_name, producer_id, producer_kind, trust_label,
               admission_mode, catalog_version_id, profile_fingerprint, status)
            VALUES (%s, 'issue-1135-source', 'source', 'curated',
                    'fast_track', %s, 'fixture:issue-1135-source', 'active')
            """,
            (deployment, catalog_version_id),
        )

    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(
                "paragraph:issue-1135",
                "paragraph",
                {"text": "Alpha, beta, and gamma are compared."},
                {"id": "paragraph:issue-1135"},
                {"v": 1},
            )
        ],
        source="issue-1135-source",
    )

    # Pre-fix deployments can already contain multiple live answers from this
    # producer.  The first complete rerun is the executable repair path: source
    # ownership, not new metadata, brings legacy rows into the replacement set.
    store.apply_facts(
        [
            NodeFact(
                "claim:legacy-duplicate-a",
                "claim",
                {"text": "Legacy duplicate", "kind": "empirical"},
                {
                    "legacy": "a",
                    "derived_from": ["paragraph:issue-1135"],
                    "derived_support": {
                        "schema": "okg.derived_support.v1",
                        "producer": "_enricher:claim_extractor",
                        "members": ["paragraph:issue-1135"],
                        "policy": "all_members",
                    },
                },
                {"prompt": "old-a"},
            ),
            NodeFact(
                "claim:legacy-duplicate-b",
                "claim",
                {"text": "Legacy duplicate", "kind": "empirical"},
                {
                    "legacy": "b",
                    "derived_from": ["paragraph:issue-1135"],
                    "derived_support": {
                        "schema": "okg.derived_support.v1",
                        "producer": "_enricher:claim_extractor",
                        "members": ["paragraph:issue-1135"],
                        "policy": "all_members",
                    },
                },
                {"prompt": "old-b"},
            ),
            EdgeFact(
                "paragraph:issue-1135",
                "claim:legacy-duplicate-a",
                "contains",
                provenance="extracted_model",
                source_record_id={"legacy": "a"},
                source_revision={"prompt": "old-a"},
            ),
            EdgeFact(
                "paragraph:issue-1135",
                "claim:legacy-duplicate-b",
                "contains",
                provenance="extracted_model",
                source_record_id={"legacy": "b"},
                source_revision={"prompt": "old-b"},
            ),
        ],
        source="_enricher:claim_extractor",
    )


def _producer_graph_rows(
    dsn: str,
    *,
    generation_id: int,
    producer_id: str = "_enricher:claim_extractor",
) -> tuple[list[str], list[tuple[str, str, str]]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout_generation(generation_id)
        nodes = [
            str(row["node_id"])
            for row in graph.query(
                "SELECT node_id FROM okg.graph_nodes "
                "WHERE source = :source ORDER BY node_id",
                {"source": producer_id},
            )
        ]
        edges = [
            (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
            for row in graph.query(
                "SELECT src, edge_type, dst FROM okg.graph_edges "
                "WHERE source = :source ORDER BY src, edge_type, dst",
                {"source": producer_id},
            )
        ]
    return nodes, edges


def _producer_fact_counts(dsn: str) -> tuple[int, int]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.node_facts WHERE source = %s",
            ("_enricher:claim_extractor",),
        )
        node_count = int(cur.fetchone()[0])
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts WHERE source = %s",
            ("_enricher:claim_extractor",),
        )
        edge_count = int(cur.fetchone()[0])
    return node_count, edge_count


def test_snapshot_fact_identity_is_structured_and_injective() -> None:
    left = make_snapshot_fact_key(
        fact_kind="edge",
        producer_id="_enricher:claim_extractor",
        op="R",
        graph_identity=("a|b", "c", "d"),
    )
    right = make_snapshot_fact_key(
        fact_kind="edge",
        producer_id="_enricher:claim_extractor",
        op="R",
        graph_identity=("a", "b|c", "d"),
    )
    assert left != right


def test_shipped_extractors_explicitly_declare_complete_snapshots() -> None:
    assert {
        extractor.snapshot_replacement_mode
        for extractor in (
            ClaimExtractor,
            ConceptExtractor,
            EvidenceExtractor,
            HypothesisExtractor,
        )
    } == {"complete_producer"}


def test_duplicate_enricher_producer_identity_is_rejected_at_boot() -> None:
    first = ClaimExtractor(
        confidence_floor=0.6,
        llm=_MutableLLM(_TWO_CLAIMS),
        input_quarantine_max_attempts=3,
    )
    second = ClaimExtractor(
        confidence_floor=0.9,
        llm=_MutableLLM(_ONE_REVISED_CLAIM),
        input_quarantine_max_attempts=3,
    )
    with pytest.raises(
        EnricherLoadError,
        match="duplicate enricher producer identity",
    ):
        validate_enricher_envelopes([first, second])


def test_published_reruns_replace_instead_of_growing(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    deployment = "issue-1135-published-replacement"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_extraction_graph(conn, deployment=deployment)

    llm = _MutableLLM(_TWO_CLAIMS)
    extractor = _CapturingClaimExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    publisher = Publisher(clean_runtime, deployment, enrichers=[extractor], catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))

    repaired = publisher.cycle()
    assert repaired["status"] == "published", repaired
    repaired_rows = _producer_graph_rows(
        clean_runtime,
        generation_id=repaired["generation_id"],
    )
    repaired_nodes, repaired_edges = repaired_rows
    assert len(repaired_nodes) == 2
    assert len(repaired_edges) == 2
    assert not any("legacy-duplicate" in node_id for node_id in repaired_nodes)
    assert extractor.results[-1].output_lineage == {
        "schema": "okg.enricher_snapshot_replacement.v1",
        "producer_id": "_enricher:claim_extractor",
        "complete": True,
        "prompt_hashes": extractor.results[-1].output_lineage["prompt_hashes"],
        "candidate_count": 2,
        "quarantined_count": 0,
        "quarantined_item_refs": [],
        "nodes_retracted": 2,
        "edges_retracted": 2,
        "nodes_retained": 0,
        "edges_retained": 0,
        "node_assertions_inserted": 2,
        "edge_assertions_inserted": 2,
    }

    unchanged = publisher.cycle(force_enrichers=True)
    assert unchanged["status"] == "published", unchanged
    unchanged_rows = _producer_graph_rows(
        clean_runtime,
        generation_id=unchanged["generation_id"],
    )
    assert unchanged_rows == repaired_rows
    assert extractor.results[-1].output_lineage["nodes_retracted"] == 2
    assert extractor.results[-1].output_lineage["node_assertions_inserted"] == 2
    facts_before_incomplete = _producer_fact_counts(clean_runtime)

    # Malformed output is not an authoritative empty snapshot.  The previous
    # two rows remain live and the run is observable as incomplete.
    llm.response = "not valid JSON"
    malformed = publisher.cycle(force_enrichers=True)
    assert malformed["status"] == "published", malformed
    assert (
        _producer_graph_rows(
            clean_runtime,
            generation_id=malformed["generation_id"],
        )
        == repaired_rows
    )
    malformed_run = next(
        run for run in malformed["enricher_runs"] if run["name"] == "claim_extractor"
    )
    assert malformed_run["status"] == "partial"
    assert extractor.results[-1].output_lineage["complete"] is False
    assert "failure_reason" in extractor.results[-1].output_lineage
    assert _producer_fact_counts(clean_runtime) == facts_before_incomplete

    llm.response = '{"text": "wrong top-level shape"}'
    wrong_shape = publisher.cycle(force_enrichers=True)
    assert wrong_shape["status"] == "published", wrong_shape
    assert (
        _producer_graph_rows(
            clean_runtime,
            generation_id=wrong_shape["generation_id"],
        )
        == repaired_rows
    )
    assert _producer_fact_counts(clean_runtime) == facts_before_incomplete

    # Missing client, invalid rows, provider failures, and truncation are four
    # other ways an empty-looking response could otherwise erase good state.
    extractor.llm = None
    no_client = publisher.cycle(force_enrichers=True)
    assert no_client["status"] == "published", no_client
    assert (
        _producer_graph_rows(
            clean_runtime,
            generation_id=no_client["generation_id"],
        )
        == repaired_rows
    )
    assert _producer_fact_counts(clean_runtime) == facts_before_incomplete
    extractor.llm = llm

    llm.response = '[{"text": "bad", "kind": "not-a-kind", "confidence": 0.9}]'
    invalid = publisher.cycle(force_enrichers=True)
    assert invalid["status"] == "published", invalid
    assert (
        _producer_graph_rows(
            clean_runtime,
            generation_id=invalid["generation_id"],
        )
        == repaired_rows
    )
    assert _producer_fact_counts(clean_runtime) == facts_before_incomplete

    llm.error = RuntimeError("provider unavailable")
    provider_error = publisher.cycle(force_enrichers=True)
    assert provider_error["status"] == "published", provider_error
    assert (
        _producer_graph_rows(
            clean_runtime,
            generation_id=provider_error["generation_id"],
        )
        == repaired_rows
    )
    assert _producer_fact_counts(clean_runtime) == facts_before_incomplete
    llm.error = None

    llm.response = _ONE_REVISED_CLAIM
    llm.truncated = True
    truncated = publisher.cycle(force_enrichers=True)
    assert truncated["status"] == "published", truncated
    assert (
        _producer_graph_rows(
            clean_runtime,
            generation_id=truncated["generation_id"],
        )
        == repaired_rows
    )
    assert _producer_fact_counts(clean_runtime) == facts_before_incomplete
    llm.truncated = False

    # A prompt change deliberately changes the OutcomeKey and re-reads the
    # input.  It now replaces the whole producer snapshot, including the
    # second old claim that is absent from the new (smaller) answer.
    monkeypatch.setattr(
        claim_module,
        "_SYSTEM_PROMPT",
        claim_module._SYSTEM_PROMPT + "\nRevision: emit only the strongest claim.",
    )
    llm.response = _ONE_REVISED_CLAIM
    revised = publisher.cycle(force_enrichers=True)
    assert revised["status"] == "published", revised
    revised_nodes, revised_edges = _producer_graph_rows(
        clean_runtime,
        generation_id=revised["generation_id"],
    )
    assert len(revised_nodes) == 1
    assert len(revised_edges) == 1
    assert set(revised_nodes).isdisjoint(repaired_nodes)
    assert (
        "superseded prior extraction snapshot (nodes_retracted=2, "
        "edges_retracted=2, nodes_retained=0, edges_retained=0, "
        "node_assertions_inserted=1, "
        "edge_assertions_inserted=1"
    ) in caplog.text
    assert extractor.results[-1].output_lineage["prompt_hashes"]

    # A valid empty array is authoritative and retracts the last output.
    llm.response = "[]"
    emptied = publisher.cycle(force_enrichers=True)
    assert emptied["status"] == "published", emptied
    assert _producer_graph_rows(
        clean_runtime,
        generation_id=emptied["generation_id"],
    ) == ([], [])


def test_soft_fact_rejection_rolls_back_the_whole_replacement(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    deployment = "issue-1135-atomic-rejection"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_extraction_graph(conn, deployment=deployment)

    llm = _MutableLLM(_TWO_CLAIMS)
    extractor = _CapturingClaimExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    publisher = Publisher(
        clean_runtime,
        deployment,
        enrichers=[extractor],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )
    first = publisher.cycle()
    assert first["status"] == "published", first
    before = _producer_graph_rows(
        clean_runtime,
        generation_id=first["generation_id"],
    )
    facts_before = _producer_fact_counts(clean_runtime)

    real_emit_node_fact = _fact_emitters.emit_node_fact
    rejection_mode = {"value": "rejected"}

    def reject_new_assertion(conn, **kwargs):
        if kwargs.get("op") == "I":
            if rejection_mode["value"] == "rejected":
                return FactEmitOutcome(
                    emitted=False,
                    rejected_fields=("node_id",),
                )
            if rejection_mode["value"] == "held":
                return FactEmitOutcome(
                    emitted=False,
                    held=True,
                    held_reason="fixture policy hold",
                )
            raise RuntimeError("fixture assertion failure")
        return real_emit_node_fact(conn, **kwargs)

    monkeypatch.setattr(_fact_emitters, "emit_node_fact", reject_new_assertion)
    llm.response = _ONE_REVISED_CLAIM
    second = publisher.cycle(force_enrichers=True)
    assert second["status"] == "published", second
    assert (
        _producer_graph_rows(
            clean_runtime,
            generation_id=second["generation_id"],
        )
        == before
    )
    rejected_run = next(
        run for run in second["enricher_runs"] if run["name"] == "claim_extractor"
    )
    assert rejected_run["status"] == "partial"
    assert extractor.results[-1].rejected_fact_count == 1
    assert _producer_fact_counts(clean_runtime) == facts_before

    rejection_mode["value"] = "held"
    held = publisher.cycle(force_enrichers=True)
    assert held["status"] == "published", held
    assert (
        _producer_graph_rows(
            clean_runtime,
            generation_id=held["generation_id"],
        )
        == before
    )
    assert extractor.results[-1].rejected_fact_count == 1
    assert _producer_fact_counts(clean_runtime) == facts_before

    rejection_mode["value"] = "raised"
    raised = publisher.cycle(force_enrichers=True)
    assert raised["status"] == "published", raised
    assert (
        _producer_graph_rows(
            clean_runtime,
            generation_id=raised["generation_id"],
        )
        == before
    )
    assert extractor.results[-1].rejected_fact_count == 0
    assert _producer_fact_counts(clean_runtime) == facts_before
    assert "complete snapshot emission rolled back" in caplog.text


def test_shared_output_survives_one_input_vanishing_then_converges_to_empty(
    clean_runtime: str,
) -> None:
    deployment = "issue-1135-shared-output"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog_version_id = seed_catalog(
            conn,
            subtypes=["paper", "paper_section", "concept", "extraction_dropped"],
            narrowings=[
                ("paper", "defines", "concept"),
                ("paper", "uses", "concept"),
                ("paper", "mentions", "concept"),
                ("concept", "refines", "concept"),
            ],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.producer_profiles
                  (deployment_name, producer_id, producer_kind, trust_label,
                   admission_mode, catalog_version_id,
                   profile_fingerprint, status)
                VALUES (%s, 'issue-1135-source', 'source', 'curated',
                        'fast_track', %s,
                        'fixture:issue-1135-shared-source', 'active')
                """,
                (deployment, catalog_version_id),
            )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "paper:issue-1135-first",
                    "paper",
                    {"title": "First paper about shared concepts"},
                    {"id": "paper:issue-1135-first"},
                    {"v": 1},
                ),
                NodeFact(
                    "paper:issue-1135-second",
                    "paper",
                    {"title": "Second paper about shared concepts"},
                    {"id": "paper:issue-1135-second"},
                    {"v": 1},
                ),
            ],
            source="issue-1135-source",
        )

    publisher = Publisher(
        clean_runtime,
        deployment,
        enrichers=[
            ConceptExtractor(
                confidence_floor=0.6,
                llm=_MutableLLM(
                    """[
                      {"name": "shared identity", "relationship": "defines",
                       "description": "One concept emitted by both papers",
                       "confidence": 0.9}
                    ]"""
                ),
                input_quarantine_max_attempts=3,
            )
        ],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )
    first = publisher.cycle()
    assert first["status"] == "published", first
    first_nodes, first_edges = _producer_graph_rows(
        clean_runtime,
        generation_id=first["generation_id"],
        producer_id="_enricher:concept_extractor",
    )
    # Both papers emit the same slug-based concept identity, but each owns its
    # own defines edge. Structured whole-snapshot replacement keeps the sharing.
    assert first_nodes == ["concept:shared-identity"]
    assert len(first_edges) == 2

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "paper:issue-1135-second",
                    "paper",
                    {},
                    {"id": "paper:issue-1135-second"},
                    {"v": 2},
                    op="R",
                )
            ],
            source="issue-1135-source",
        )

    one_input = publisher.cycle()
    assert one_input["status"] == "published", one_input
    one_input_nodes, one_input_edges = _producer_graph_rows(
        clean_runtime,
        generation_id=one_input["generation_id"],
        producer_id="_enricher:concept_extractor",
    )
    assert one_input_nodes == first_nodes
    assert len(one_input_edges) == 1
    assert one_input_edges[0][0] == "paper:issue-1135-first"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "paper:issue-1135-first",
                    "paper",
                    {},
                    {"id": "paper:issue-1135-first"},
                    {"v": 2},
                    op="R",
                )
            ],
            source="issue-1135-source",
        )

    no_inputs = publisher.cycle()
    assert no_inputs["status"] == "published", no_inputs
    assert _producer_graph_rows(
        clean_runtime,
        generation_id=no_inputs["generation_id"],
        producer_id="_enricher:concept_extractor",
    ) == ([], [])


def test_entity_resolved_owned_node_is_retained_without_a_new_assertion(
    clean_runtime: str,
) -> None:
    deployment = "issue-1135-resolved-retention"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_extraction_graph(conn, deployment=deployment)

    class ExistingCanonicalResolution:
        def begin_batch(self) -> None:
            return None

        def enabled_for(self, subtype: str) -> bool:
            return subtype == "claim"

        def resolve(self, *_args, **_kwargs):
            return SimpleNamespace(
                canonical_node_id="claim:legacy-duplicate-a",
                materialize=False,
            )

    extractor = _CapturingClaimExtractor(
        confidence_floor=0.6,
        llm=_MutableLLM(
            '[{"text": "Resolved current claim", "kind": "empirical", '
            '"confidence": 0.9}]'
        ),
        input_quarantine_max_attempts=3,
    )
    extractor._entity_resolution = ExistingCanonicalResolution()
    published = Publisher(
        clean_runtime,
        deployment,
        enrichers=[extractor],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert published["status"] == "published", published
    nodes, edges = _producer_graph_rows(
        clean_runtime,
        generation_id=published["generation_id"],
    )
    assert nodes == ["claim:legacy-duplicate-a"]
    assert edges == [
        (
            "paragraph:issue-1135",
            "contains",
            "claim:legacy-duplicate-a",
        )
    ]
    lineage = extractor.results[-1].output_lineage
    assert lineage["nodes_retracted"] == 1
    assert lineage["nodes_retained"] == 1
    assert lineage["node_assertions_inserted"] == 0


def test_replay_miss_preserves_the_last_published_snapshot(
    clean_runtime: str,
) -> None:
    deployment = "issue-1135-replay-miss"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_extraction_graph(conn, deployment=deployment)

    llm = _MutableLLM(_TWO_CLAIMS)
    first = Publisher(
        clean_runtime,
        deployment,
        enrichers=[
            ClaimExtractor(
                confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
            )
        ],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert first["status"] == "published", first
    before = _producer_graph_rows(
        clean_runtime,
        generation_id=first["generation_id"],
    )
    facts_before = _producer_fact_counts(clean_runtime)

    replay_extractor = _CapturingClaimExtractor(
        confidence_floor=0.6,
        llm=llm,
        model_outcomes_max_rows=10,
        model_outcomes_deployment=deployment,
        replay_outcomes=True,
        input_quarantine_max_attempts=3,
    )
    replayed = Publisher(
        clean_runtime,
        deployment,
        enrichers=[replay_extractor],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle(force_enrichers=True)
    assert replayed["status"] == "published", replayed
    assert (
        _producer_graph_rows(
            clean_runtime,
            generation_id=replayed["generation_id"],
        )
        == before
    )
    assert _producer_fact_counts(clean_runtime) == facts_before
    assert replay_extractor.results[-1].incomplete is True
    assert replay_extractor.results[-1].n_input_failures == 1
    assert replay_extractor.results[-1].output_lineage["complete"] is False


@pytest.mark.parametrize("replacement", ["new", "reuse", "same-text", "failure", "empty", "missing-crop"])
def test_superseded_caption_retraction(clean_runtime: str, replacement: str) -> None:
    from tests.captioning.test_visual_caption_enricher import (
        _MODEL, _ENRICHER_SOURCE, _StubCaptionSvc, _stage_figure, _seed_nodes, _checkpoint,
    )
    from okg.substrate.library.enrichers.visual_caption_enricher import (
        VisualCaptionEnricher, _caption_id, _dedupe,
    )

    visual = "visual:supersession"
    svc = _StubCaptionSvc(
        fixed_text="replacement" if replacement != "same-text" else "obsolete",
        fail_on={visual} if replacement == "failure" else set(),
        empty_on={visual} if replacement == "empty" else set(),
    )
    enricher = VisualCaptionEnricher(model=_MODEL, caption_service=svc)
    old_id = _caption_id(visual, "obsolete")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        attrs = _stage_figure(conn, visual_id=visual, doc_id="doc:1", context="new", png_seed=3)
        signature = enricher._vision_signature(
            content_sha256=attrs["content_sha256"],
            render_sha256=attrs["render_sha256"], context="new",
        )
        if replacement == "missing-crop":
            attrs["crop_blob_ref"] = "absent"
        rows = [(visual, "visual_artifact", attrs)]
        for node_id, text, sig in [
            (old_id, "obsolete", "old-signature"),
            ("chunk:foreign", "foreign", "old-signature"),
            ("chunk:unrelated", "unrelated", "old-signature"),
        ]:
            rows.append((node_id, "document_chunk", {
                "chunk_id": node_id, "text": text, "chunk_source": "vision_caption",
                "visual_ref": "visual:other" if node_id == "chunk:unrelated" else visual,
                "vision_signature": sig,
            }))
        if replacement == "reuse":
            rows.append(("chunk:current", "document_chunk", {
                "chunk_id": "chunk:current", "text": "replacement", "chunk_source": "vision_caption",
                "visual_ref": visual, "vision_signature": signature,
            }))
        _seed_nodes(conn, rows)
        graph = ChronosGraphStore.from_connection(conn).checkout()
        graph.execute(
            "UPDATE okg.graph_nodes SET source = :source WHERE node_id != 'chunk:foreign'",
            {"source": _ENRICHER_SOURCE},
        )
        _checkpoint(conn, 1)
        result = enricher.enrich(conn, generation_id=1)
        with conn.cursor() as cur:
            cur.execute("SELECT node_id, dedupe_key FROM okg.node_facts WHERE op = 'R'")
            retractions = cur.fetchall()
        expected = [(old_id, _dedupe("node", old_id, "old-signature"))] if replacement in {"new", "reuse"} else []
        assert retractions == expected
        # The count is visible on the result, not only in the fact log: a mass
        # supersession must be readable from the enricher's own receipt.
        assert result.n_retracted == len(expected)
        assert result.incomplete == (replacement in {"failure", "empty", "missing-crop"})
        if replacement == "reuse":
            assert svc.calls == []


def test_superseded_caption_stops_scoring_after_publish(clean_runtime: str) -> None:
    from tests.captioning.test_visual_caption_enricher import (
        _MODEL, _StubCaptionSvc, _stage_figure, _seed_nodes,
    )
    from okg.substrate.graph_store import DEFAULT_BRANCH, NODE_TABLE
    from okg.substrate.library.enrichers.visual_caption_enricher import VisualCaptionEnricher
    from okg.substrate.mcp import open_session, search
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    svc = _StubCaptionSvc(fixed_text="obsoletequasar")
    enricher = VisualCaptionEnricher(model=_MODEL, caption_service=svc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        attrs = _stage_figure(conn, visual_id="visual:search", doc_id="doc:1", context="old", png_seed=2)
        _seed_nodes(conn, [
            ("doc:1", "document", {"title": "Figure document"}),
            ("visual:search", "visual_artifact", attrs),
        ])
    publisher = Publisher(
        clean_runtime, "caption-retraction", enrichers=[enricher],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "caption-retraction"),
    )
    first = publisher.cycle()
    assert first["status"] == "published", first
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="caption-retraction")
        hits = search(conn, session, "obsoletequasar", method="lexical")["results"]
        assert len(hits) == 1
        old_id = hits[0]["node_id"]
        attrs["caption_context"] = "new"
        _seed_nodes(conn, [("visual:search", "visual_artifact", attrs)])
    svc._fixed_text = "currentnebula"
    second = publisher.cycle()
    assert second["status"] == "published", second
    # The publish receipt is the count, not just the search result: exactly one
    # caption node left the graph, and so did BOTH of its edges (derived_from
    # the visual, member_of the document). Asserting the edge count here pins
    # the enricher's own edge retraction -- under the default `block`
    # dangling-edge policy a node retracted without its edges would either
    # block this publish or depend on the publisher's orphan sweep to save it.
    assert second["nodes_retracted"] == 1, second
    assert second["edges_retracted"] == 2, second
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="caption-retraction")
        assert search(conn, session, "obsoletequasar", method="lexical")["results"] == []
        current = search(conn, session, "currentnebula", method="lexical")["results"]
        assert len(current) == 1
        current_id = current[0]["node_id"]
        assert current_id != old_id
        graph = ChronosGraphStore.from_connection(conn).checkout()
        assert graph.query(
            "SELECT edge_id FROM okg.graph_edges WHERE src = :src",
            {"src": old_id},
        ) == []
        assert len(graph.query(
            "SELECT edge_id FROM okg.graph_edges WHERE src = :src",
            {"src": current_id},
        )) == 2

    # Retraction-only cycle: the enricher reuses the live caption (zero VLM
    # calls, zero edges emitted) and its ONLY output is a retraction. That path
    # feeds the wired-but-silent liveness accounting and the publisher's
    # zero-delta short-circuits, so prove it publishes rather than assuming it.
    stale_id = "chunk:staleorphan"
    calls_after_second = list(svc.calls)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        session = store.checkout(DEFAULT_BRANCH)
        live = session.query(
            "SELECT node_id, subtype, attrs, source, source_record_id, "
            "source_revision FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": current_id},
        )
        assert len(live) == 1, live
        row = live[0]
        # Clone the live caption's producer + support declaration so the stale
        # node is a genuine prior output of THIS enricher, not a fixture row
        # the derived-node cleanup would tombstone on its own.
        stale_attrs = dict(row["attrs"])
        stale_attrs.update({
            "chunk_id": stale_id,
            "text": "staleorphan",
            "vision_signature": "superseded-signature",
        })
        with session.transaction():
            session.upsert_rows(NODE_TABLE, [{
                "node_id": stale_id,
                "subtype": row["subtype"],
                "attrs": stale_attrs,
                "text_hash": None,
                "embedding": None,
                "embedding_model": None,
                "source": row["source"],
                "source_record_id": dict(row["source_record_id"] or {}),
                "source_revision": dict(row["source_revision"] or {}),
                "run_id": None,
                "observed_at": None,
            }])
    third = publisher.cycle()
    assert third["status"] == "published", third
    # Zero VLM calls: the live caption matched the predicted signature, so the
    # cycle reused it and emitted no edges at all.
    assert svc.calls == calls_after_second
    assert third["edges_added"] == 0, third
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT node_id FROM okg.node_facts WHERE op = 'R' "
                "ORDER BY node_id"
            )
            assert [r[0] for r in cur.fetchall()] == [old_id, stale_id]
        graph = ChronosGraphStore.from_connection(conn).checkout()
        # The stale caption is gone from the published graph and the live one
        # survived -- the retraction landed through the publish even though the
        # enricher emitted nothing else this cycle.
        assert graph.query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": stale_id},
        ) == []
        assert len(graph.query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": current_id},
        )) == 1
