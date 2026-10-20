"""Issue #714: bounded item retries release a held publisher watermark safely."""

from __future__ import annotations

import json
import threading
from typing import Any

import psycopg
import pytest

from okg.substrate.errors import PublisherLockHeld
from okg.substrate.cli.main import main
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.enrichers import _fact_emitters
from okg.substrate.library.enrichers._fact_emitters import FactEmitOutcome
from okg.substrate.library.enrichers.extraction import base as extraction_base
from okg.substrate.library.enrichers.extraction.claim_extractor import ClaimExtractor
from okg.substrate.library.enrichers.extraction.base import (
    IncompleteExtractionResponse,
)
from okg.substrate.llm.fanout import ModelCallJob
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from okg.views.execution.errors import LLMCallFailed
from tests.substrate.extraction.test_enricher_snapshot_replacement import (
    _CapturingClaimExtractor,
    _seed_extraction_graph,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


_GOOD_POISON = '[{"text":"Poison prior claim","kind":"empirical","confidence":0.9}]'
_GOOD_HEALTHY = '[{"text":"Healthy prior claim","kind":"empirical","confidence":0.9}]'
_REVISED_HEALTHY = (
    '[{"text":"Healthy revised claim","kind":"theoretical","confidence":0.95}]'
)


class _PerItemLLM:
    model = "issue-714-fixture"

    def __init__(self) -> None:
        self.responses = {
            "paragraph:issue-1135": _GOOD_POISON,
            "paragraph:issue-714-healthy": _GOOD_HEALTHY,
        }
        self.calls: list[str] = []
        self.error: BaseException | None = None
        self.truncated = False

    @property
    def last_call_stats(self) -> dict[str, Any]:
        return {"cost_usd": 0.0, "truncated": self.truncated}

    def complete(self, _system: str, user: str, **_kwargs: Any) -> str:
        if self.error is not None:
            raise self.error
        item_ref = next(ref for ref in self.responses if f"Source id: {ref}\n" in user)
        self.calls.append(item_ref)
        return self.responses[item_ref]


def _seed_two_inputs(conn: psycopg.Connection, deployment: str) -> None:
    _seed_extraction_graph(conn, deployment=deployment)
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(
                "paragraph:issue-714-healthy",
                "paragraph",
                {"text": "A healthy sibling input."},
                {"id": "paragraph:issue-714-healthy"},
                {"v": 1},
            )
        ],
        source="issue-1135-source",
    )


def _claim_entity_resolution(deployment: str) -> dict[str, Any]:
    return {
        "deployment_name": deployment,
        "branch_name": "default",
        "algorithm_version": "unicode-escape-v1",
        "policies": {
            "claim": {
                "name_field": "text",
                "id_prefix": "claim",
                "identity_fields": ["claim_id"],
                "alias_first": False,
                "live_exact": False,
            }
        },
    }


def _progress(conn: psycopg.Connection, deployment: str) -> tuple[int, int, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fact_watermark, edge_watermark, last_status
              FROM okg.enricher_progress
             WHERE deployment_name = %s
               AND enricher_name = 'claim_extractor'
            """,
            (deployment,),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0]), int(row[1]), str(row[2])


def _quarantines(conn: psycopg.Connection, deployment: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT item_ref, input_signature, status, attempt_count,
                   attempt_limit, last_failure_class, last_error
              FROM okg.enricher_input_quarantines
             WHERE deployment_name = %s
             ORDER BY item_ref, created_at, input_signature
            """,
            (deployment,),
        )
        return [
            {
                "item_ref": str(row[0]),
                "signature": str(row[1]),
                "status": str(row[2]),
                "attempt_count": int(row[3]),
                "attempt_limit": int(row[4]),
                "failure_class": row[5],
                "error": row[6],
            }
            for row in cur.fetchall()
        ]


def _identity_claim_exists(
    conn: psycopg.Connection,
    deployment: str,
    normalized_key: str,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM okg.entity_identity_claims "
            "WHERE deployment_name = %s AND normalized_key = %s)",
            (deployment, normalized_key),
        )
        return bool(cur.fetchone()[0])


def _producer_rows(dsn: str, generation_id: int) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        return [
            dict(row)
            for row in graph.query(
                """
                SELECT node_id, attrs, source_record_id
                  FROM okg.graph_nodes
                 WHERE source = '_enricher:claim_extractor'
                 ORDER BY node_id
                """
            )
        ]


def _producer_edges(dsn: str, generation_id: int) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        return [
            dict(row)
            for row in graph.query(
                """
                SELECT src, edge_type, dst, attrs, source_record_id
                  FROM okg.graph_edges
                 WHERE source = '_enricher:claim_extractor'
                 ORDER BY src, edge_type, dst
                """
            )
        ]


def test_real_publish_quarantines_poison_retains_output_and_advances(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    deployment = "issue-714-real-publish"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_two_inputs(conn, deployment)

    llm = _PerItemLLM()
    extractor = _CapturingClaimExtractor(
        confidence_floor=0.6,
        llm=llm,
        input_quarantine_max_attempts=2,
        entity_resolution=_claim_entity_resolution(deployment),
    )
    publisher = Publisher(clean_runtime, deployment, enrichers=[extractor], catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))

    baseline = publisher.cycle()
    assert baseline["status"] == "published", baseline
    baseline_rows = _producer_rows(clean_runtime, baseline["generation_id"])
    poison_prior = next(
        row
        for row in baseline_rows
        if row["source_record_id"]["snapshot_input_ref"] == "paragraph:issue-1135"
    )
    healthy_prior = next(
        row
        for row in baseline_rows
        if row["source_record_id"]["snapshot_input_ref"]
        == "paragraph:issue-714-healthy"
    )
    assert poison_prior["attrs"]["text"] == "Poison prior claim"
    poison_prior_edge = next(
        row
        for row in _producer_edges(clean_runtime, baseline["generation_id"])
        if row["source_record_id"]["snapshot_input_ref"] == "paragraph:issue-1135"
    )

    llm.responses["paragraph:issue-1135"] = "{"  # stable malformed response
    llm.responses["paragraph:issue-714-healthy"] = _REVISED_HEALTHY

    held = publisher.cycle(force_enrichers=True)
    assert held["status"] == "published", held
    held_result = extractor.results[-1]
    assert held_result.incomplete is True
    assert held_result.n_quarantined == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        held_progress = _progress(conn, deployment)
        rows = _quarantines(conn, deployment)
    assert held_progress[2].startswith("failed:")
    poison_retry = next(
        row for row in rows if row["item_ref"] == "paragraph:issue-1135"
    )
    assert (poison_retry["status"], poison_retry["attempt_count"]) == (
        "retrying",
        1,
    )

    terminal = publisher.cycle()
    assert terminal["status"] == "published", terminal
    terminal_result = extractor.results[-1]
    assert terminal_result.incomplete is False
    assert terminal_result.n_quarantined == 1
    assert terminal_result.output_lineage["quarantined_item_refs"] == [
        "paragraph:issue-1135"
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        terminal_progress = _progress(conn, deployment)
        rows = _quarantines(conn, deployment)
    assert terminal_progress[2] == "success"
    assert terminal_progress[:2] > held_progress[:2]
    active = next(row for row in rows if row["status"] == "active")
    assert active["item_ref"] == "paragraph:issue-1135"
    assert active["attempt_count"] == active["attempt_limit"] == 2
    assert active["failure_class"] == "incomplete_extraction_response"

    terminal_rows = _producer_rows(clean_runtime, terminal["generation_id"])
    poison_after = next(
        row
        for row in terminal_rows
        if row["source_record_id"]["snapshot_input_ref"] == "paragraph:issue-1135"
    )
    healthy_after = next(
        row
        for row in terminal_rows
        if row["source_record_id"]["snapshot_input_ref"]
        == "paragraph:issue-714-healthy"
    )
    assert poison_after["node_id"] == poison_prior["node_id"]
    assert poison_after["attrs"] == poison_prior["attrs"]
    poison_after_edge = next(
        row
        for row in _producer_edges(clean_runtime, terminal["generation_id"])
        if row["source_record_id"]["snapshot_input_ref"] == "paragraph:issue-1135"
    )
    assert poison_after_edge == poison_prior_edge
    assert healthy_after["node_id"] != healthy_prior["node_id"]
    assert healthy_after["attrs"]["text"] == "Healthy revised claim"

    calls_before = list(llm.calls)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "paragraph:issue-714-healthy",
                    "paragraph",
                    {"text": "A changed healthy sibling input."},
                    {"id": "paragraph:issue-714-healthy"},
                    {"v": 2},
                )
            ],
            source="issue-1135-source",
        )
    filtered = publisher.cycle()
    assert filtered["status"] == "published", filtered
    new_calls = llm.calls[len(calls_before) :]
    assert new_calls == ["paragraph:issue-714-healthy"]
    assert extractor.results[-1].n_quarantined == 1

    exit_code = main(
        [
            "doctor",
            "--check",
            "enricher-liveness",
            "--deployment",
            deployment,
            "--dsn",
            clean_runtime,
            "--json",
        ]
    )
    output = capsys.readouterr().out.strip()
    payload = json.loads(output)
    assert exit_code == 1
    assert payload["quarantine_count"] == 1
    finding = next(
        row
        for row in payload["findings"]
        if row["reason"] == "enricher_input_quarantined"
    )
    assert finding["item_ref"] == "paragraph:issue-1135"
    assert finding["attempt_count"] == finding["attempt_limit"] == 2

    # A behavior-affecting input change starts a fresh signature budget. The
    # old active record remains visible throughout the new signature's retry
    # and is superseded only after that changed input succeeds.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "paragraph:issue-1135",
                    "paragraph",
                    {"text": "Poison input changed and may now recover."},
                    {"id": "paragraph:issue-1135"},
                    {"v": 2},
                )
            ],
            source="issue-1135-source",
        )
    changed_retry = publisher.cycle(force_enrichers=True)
    assert changed_retry["status"] == "published", changed_retry
    assert extractor.results[-1].incomplete is True
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        poison_history = [
            row
            for row in _quarantines(conn, deployment)
            if row["item_ref"] == "paragraph:issue-1135"
        ]
    assert sorted(row["status"] for row in poison_history) == [
        "active",
        "retrying",
    ]
    assert (
        next(row for row in poison_history if row["status"] == "retrying")[
            "attempt_count"
        ]
        == 1
    )

    llm.responses["paragraph:issue-1135"] = (
        '[{"text":"Recovered changed claim","kind":"empirical","confidence":0.99}]'
    )
    recovered = publisher.cycle()
    assert recovered["status"] == "published", recovered
    assert extractor.results[-1].incomplete is False
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        poison_history = [
            row
            for row in _quarantines(conn, deployment)
            if row["item_ref"] == "paragraph:issue-1135"
        ]
    assert sorted(row["status"] for row in poison_history) == [
        "resolved",
        "superseded",
    ]


def test_repeated_systemic_provider_failure_never_consumes_item_budget(
    clean_runtime: str,
) -> None:
    deployment = "issue-714-systemic-hold"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_two_inputs(conn, deployment)

    llm = _PerItemLLM()
    llm.error = RuntimeError("provider route unavailable: 503")
    publisher = Publisher(
        clean_runtime,
        deployment,
        enrichers=[
            ClaimExtractor(
                confidence_floor=0.6,
                llm=llm,
                input_quarantine_max_attempts=2,
            )
        ],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )
    for _ in range(3):
        result = publisher.cycle(force_enrichers=True)
        assert result["status"] == "published", result
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            assert _progress(conn, deployment)[2].startswith("failed:")
            assert _quarantines(conn, deployment) == []


def test_repeated_stats_truncation_never_consumes_item_budget(
    clean_runtime: str,
) -> None:
    deployment = "issue-913-truncation-hold"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_two_inputs(conn, deployment)

    llm = _PerItemLLM()
    extractor = _CapturingClaimExtractor(
        confidence_floor=0.6,
        llm=llm,
        input_quarantine_max_attempts=1,
    )
    publisher = Publisher(clean_runtime, deployment, enrichers=[extractor], catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))
    baseline = publisher.cycle()
    assert baseline["status"] == "published", baseline
    baseline_rows = _producer_rows(clean_runtime, baseline["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        baseline_quarantines = _quarantines(conn, deployment)
    assert all(
        row["status"] == "resolved" and row["attempt_count"] == 0
        for row in baseline_quarantines
    )

    llm.truncated = True
    for _ in range(3):
        held = publisher.cycle(force_enrichers=True)
        assert held["status"] == "published", held
        result = extractor.results[-1]
        assert result.incomplete is True
        assert result.n_input_failures == 2
        assert result.n_quarantined == 0
        assert "truncat" in str(result.error).lower()
        assert _producer_rows(clean_runtime, held["generation_id"]) == baseline_rows
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            assert _progress(conn, deployment)[2].startswith("failed:")
            assert _quarantines(conn, deployment) == baseline_quarantines


def test_item_attributed_candidate_rejection_quarantines_and_publishes(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "issue-714-candidate-rejection"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_two_inputs(conn, deployment)

    llm = _PerItemLLM()
    extractor = _CapturingClaimExtractor(
        confidence_floor=0.6,
        llm=llm,
        input_quarantine_max_attempts=1,
    )
    publisher = Publisher(clean_runtime, deployment, enrichers=[extractor], catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))
    baseline = publisher.cycle()
    assert baseline["status"] == "published", baseline
    poison_prior = next(
        row
        for row in _producer_rows(clean_runtime, baseline["generation_id"])
        if row["source_record_id"].get("snapshot_input_ref") == "paragraph:issue-1135"
    )

    real_emit_node_fact = _fact_emitters.emit_node_fact

    def reject_poison_assertion(conn, **kwargs):
        source_record_id = dict(kwargs.get("source_record_id") or {})
        if (
            kwargs.get("op") == "I"
            and source_record_id.get("snapshot_input_ref") == "paragraph:issue-1135"
        ):
            return FactEmitOutcome(
                emitted=False,
                rejected_fields=("node_id",),
            )
        return real_emit_node_fact(conn, **kwargs)

    monkeypatch.setattr(
        _fact_emitters,
        "emit_node_fact",
        reject_poison_assertion,
    )
    llm.responses["paragraph:issue-1135"] = (
        '[{"text":"Rejected poison replacement","kind":"empirical","confidence":0.99}]'
    )
    llm.responses["paragraph:issue-714-healthy"] = _REVISED_HEALTHY

    terminal = publisher.cycle(force_enrichers=True)
    assert terminal["status"] == "published", terminal
    assert extractor.results[-1].incomplete is False
    assert extractor.results[-1].n_quarantined == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _progress(conn, deployment)[2] == "success"
        active = next(
            row for row in _quarantines(conn, deployment) if row["status"] == "active"
        )
    assert active["item_ref"] == "paragraph:issue-1135"
    assert active["failure_class"] == "item_snapshot_emission_rejected"
    assert active["attempt_count"] == active["attempt_limit"] == 1

    terminal_rows = _producer_rows(clean_runtime, terminal["generation_id"])
    poison_after = next(
        row
        for row in terminal_rows
        if row["source_record_id"].get("snapshot_input_ref") == "paragraph:issue-1135"
    )
    assert poison_after == poison_prior
    assert any(row["attrs"]["text"] == "Healthy revised claim" for row in terminal_rows)


def test_first_upgrade_quarantine_retains_untagged_legacy_rows_fail_safe(
    clean_runtime: str,
) -> None:
    deployment = "issue-714-legacy-retention"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_two_inputs(conn, deployment)

    llm = _PerItemLLM()
    llm.responses["paragraph:issue-1135"] = "{"
    extractor = _CapturingClaimExtractor(
        confidence_floor=0.6,
        llm=llm,
        input_quarantine_max_attempts=1,
    )
    published = Publisher(
        clean_runtime,
        deployment,
        enrichers=[extractor],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert published["status"] == "published", published
    assert extractor.results[-1].n_quarantined == 1
    rows = _producer_rows(clean_runtime, published["generation_id"])
    by_id = {row["node_id"]: row for row in rows}
    assert {
        "claim:legacy-duplicate-a",
        "claim:legacy-duplicate-b",
    } <= set(by_id)
    assert (
        "snapshot_input_ref"
        not in by_id["claim:legacy-duplicate-a"]["source_record_id"]
    )
    assert any(
        row["source_record_id"].get("snapshot_input_ref")
        == "paragraph:issue-714-healthy"
        for row in rows
    )


def test_terminal_quarantine_and_progress_roll_back_at_publisher_cut(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "issue-714-transaction-cut"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_two_inputs(conn, deployment)

    llm = _PerItemLLM()
    extractor = _CapturingClaimExtractor(
        confidence_floor=0.6,
        llm=llm,
        input_quarantine_max_attempts=2,
        entity_resolution=_claim_entity_resolution(deployment),
    )
    publisher = Publisher(clean_runtime, deployment, enrichers=[extractor], catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))
    assert publisher.cycle()["status"] == "published"
    llm.responses["paragraph:issue-1135"] = "{"
    first_failure = publisher.cycle(force_enrichers=True)
    assert first_failure["status"] == "published", first_failure
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        progress_before = _progress(conn, deployment)
        rows_before = _quarantines(conn, deployment)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.node_facts "
                "WHERE source = '_enricher:claim_extractor'"
            )
            node_facts_before = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FROM okg.edge_facts "
                "WHERE source = '_enricher:claim_extractor'"
            )
            edge_facts_before = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FROM okg.entity_identity_claims "
                "WHERE deployment_name = %s",
                (deployment,),
            )
            identity_claims_before = int(cur.fetchone()[0])
        assert not _identity_claim_exists(
            conn,
            deployment,
            "healthy revised claim",
        )
    poison_before = next(
        row for row in rows_before if row["item_ref"] == "paragraph:issue-1135"
    )
    assert poison_before["status"] == "retrying"
    assert poison_before["attempt_count"] == 1
    llm.responses["paragraph:issue-714-healthy"] = _REVISED_HEALTHY

    real_advance = publisher._advance_enricher_progress

    def fail_after_snapshot(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("fault after terminal snapshot before progress commit")

    monkeypatch.setattr(publisher, "_advance_enricher_progress", fail_after_snapshot)
    with pytest.raises(RuntimeError, match="fault after terminal snapshot"):
        publisher.cycle(force_enrichers=True)
    monkeypatch.setattr(publisher, "_advance_enricher_progress", real_advance)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _progress(conn, deployment) == progress_before
        rows_after = _quarantines(conn, deployment)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.node_facts "
                "WHERE source = '_enricher:claim_extractor'"
            )
            assert int(cur.fetchone()[0]) == node_facts_before
            cur.execute(
                "SELECT count(*) FROM okg.edge_facts "
                "WHERE source = '_enricher:claim_extractor'"
            )
            assert int(cur.fetchone()[0]) == edge_facts_before
            cur.execute(
                "SELECT count(*) FROM okg.entity_identity_claims "
                "WHERE deployment_name = %s",
                (deployment,),
            )
            assert int(cur.fetchone()[0]) == identity_claims_before
        assert not _identity_claim_exists(
            conn,
            deployment,
            "healthy revised claim",
        )
    poison_after = next(
        row for row in rows_after if row["item_ref"] == "paragraph:issue-1135"
    )
    assert poison_after["status"] == "retrying"
    assert poison_after["attempt_count"] == 1

    terminal = publisher.cycle(force_enrichers=True)
    assert terminal["status"] == "published", terminal
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _progress(conn, deployment)[2] == "success"
        active = next(
            row
            for row in _quarantines(conn, deployment)
            if row["item_ref"] == "paragraph:issue-1135"
        )
    assert active["status"] == "active"
    assert active["attempt_count"] == 2
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _identity_claim_exists(
            conn,
            deployment,
            "healthy revised claim",
        )


@pytest.mark.parametrize(
    "cut",
    [
        "after_reservation",
        "after_completion",
        "after_output_admission",
        "before_commit",
    ],
)
def test_publisher_transaction_fault_cuts_do_not_split_state(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    cut: str,
) -> None:
    deployment = f"issue-714-fault-{cut.replace('_', '-')}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_two_inputs(conn, deployment)

    llm = _PerItemLLM()
    extractor = _CapturingClaimExtractor(
        confidence_floor=0.6,
        llm=llm,
        input_quarantine_max_attempts=2,
        entity_resolution=_claim_entity_resolution(deployment),
    )
    publisher = Publisher(clean_runtime, deployment, enrichers=[extractor], catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))
    assert publisher.cycle()["status"] == "published"
    llm.responses["paragraph:issue-1135"] = "{"
    assert publisher.cycle(force_enrichers=True)["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        progress_before = _progress(conn, deployment)
        quarantines_before = _quarantines(conn, deployment)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.node_facts "
                "WHERE source = '_enricher:claim_extractor'"
            )
            node_facts_before = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FROM okg.edge_facts "
                "WHERE source = '_enricher:claim_extractor'"
            )
            edge_facts_before = int(cur.fetchone()[0])
        assert not _identity_claim_exists(
            conn,
            deployment,
            "healthy revised claim",
        )
    llm.responses["paragraph:issue-714-healthy"] = _REVISED_HEALTHY

    if cut == "after_reservation":
        real_claim_inputs = extraction_base.claim_inputs

        def fail_after_reservation(*args: Any, **kwargs: Any):
            real_claim_inputs(*args, **kwargs)
            raise RuntimeError("fault after input reservation")

        monkeypatch.setattr(
            extraction_base,
            "claim_inputs",
            fail_after_reservation,
        )
        result = publisher.cycle(force_enrichers=True)
        assert result["status"] == "published", result
    elif cut == "after_completion":
        real_complete_many = extractor._complete_many

        def fail_after_completion(*args: Any, **kwargs: Any):
            real_complete_many(*args, **kwargs)
            raise RuntimeError("fault after provider completion")

        monkeypatch.setattr(extractor, "_complete_many", fail_after_completion)
        result = publisher.cycle(force_enrichers=True)
        assert result["status"] == "published", result
    elif cut == "after_output_admission":
        real_admit = publisher._admit_enricher_outputs

        def fail_after_output_admission(
            conn: psycopg.Connection,
            *args: Any,
            **kwargs: Any,
        ):
            real_admit(conn, *args, **kwargs)
            # Force the no-lineage/no-relational-endpoint trust fallback that
            # is otherwise data-shape dependent.  It must query Chronos while
            # the retry transaction is open without committing it.
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT min(fact_id), max(fact_id) FROM okg.edge_facts "
                    "WHERE source = '_enricher:claim_extractor'"
                )
                fact_id_min, fact_id_max = cur.fetchone()
            assert fact_id_min is not None and fact_id_max is not None
            Publisher._chronos_endpoint_source_ids_for_trust_check(
                conn,
                branch_name=publisher.branch_name,
                source="_enricher:claim_extractor",
                fact_id_min=int(fact_id_min),
                fact_id_max=int(fact_id_max),
            )
            raise RuntimeError("fault after output admission")

        monkeypatch.setattr(
            publisher,
            "_admit_enricher_outputs",
            fail_after_output_admission,
        )
        with pytest.raises(RuntimeError, match="after output admission"):
            publisher.cycle(force_enrichers=True)
    else:
        real_finish = extractor._finish_publisher_transaction

        def fail_before_commit(conn, *, commit: bool) -> None:
            if commit:
                raise RuntimeError("fault immediately before publisher commit")
            real_finish(conn, commit=False)

        monkeypatch.setattr(
            extractor,
            "_finish_publisher_transaction",
            fail_before_commit,
        )
        with pytest.raises(RuntimeError, match="immediately before publisher commit"):
            publisher.cycle(force_enrichers=True)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        progress_after = _progress(conn, deployment)
        assert progress_after[:2] == progress_before[:2]
        assert _quarantines(conn, deployment) == quarantines_before
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.node_facts "
                "WHERE source = '_enricher:claim_extractor'"
            )
            assert int(cur.fetchone()[0]) == node_facts_before
            cur.execute(
                "SELECT count(*) FROM okg.edge_facts "
                "WHERE source = '_enricher:claim_extractor'"
            )
            assert int(cur.fetchone()[0]) == edge_facts_before
        assert not _identity_claim_exists(
            conn,
            deployment,
            "healthy revised claim",
        )


def test_model_backed_extractor_requires_explicit_positive_retry_limit() -> None:
    llm = _PerItemLLM()
    with pytest.raises(ValueError, match="input_quarantine_max_attempts"):
        ClaimExtractor(confidence_floor=0.6, llm=llm)
    for invalid in (0, -1, True, "3"):
        with pytest.raises(ValueError, match="input_quarantine_max_attempts"):
            ClaimExtractor(
                confidence_floor=0.6,
                llm=llm,
                input_quarantine_max_attempts=invalid,
            )
    with pytest.raises(ValueError, match="input_quarantine_max_attempts"):
        ClaimExtractor(confidence_floor=0.6, llm_canary=True)
    no_model = ClaimExtractor(confidence_floor=0.6)
    assert no_model.llm is None
    assert no_model.input_quarantine_max_attempts is None


def test_duplicate_item_signature_dispatches_and_consumes_once(
    clean_runtime: str,
) -> None:
    deployment = "issue-714-duplicate-signature"
    llm = _PerItemLLM()
    extractor = ClaimExtractor(
        confidence_floor=0.6,
        llm=llm,
        input_quarantine_max_attempts=2,
    )
    job = ModelCallJob(
        system="system",
        user="Source id: paragraph:issue-1135\nbody",
        typed_input={"source_node_id": "paragraph:issue-1135", "body": "body"},
        item_ref="paragraph:issue-1135",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        extractor._run_conn = conn
        extractor._run_deployment_name = deployment
        extractor._run_branch_name = "default"
        extractor._run_generation_id = 7
        outcomes = extractor._complete_many(conn, [job, job], generation_id=7)
        assert [outcome.content for outcome in outcomes] == [
            _GOOD_POISON,
            _GOOD_POISON,
        ]
        assert llm.calls == ["paragraph:issue-1135"]
        failure = IncompleteExtractionResponse("duplicate malformed response")
        extractor._record_input_failure("paragraph:issue-1135", failure)
        extractor._record_input_failure("paragraph:issue-1135", failure)
        extractor._finish_publisher_transaction(conn, commit=True)

        rows = _quarantines(conn, deployment)
    assert len(rows) == 1
    assert rows[0]["attempt_count"] == 1
    assert rows[0]["status"] == "retrying"


def test_retry_episode_pins_limit_across_lowering_and_raising(
    clean_runtime: str,
) -> None:
    job = ModelCallJob(
        system="system",
        user="Source id: paragraph:issue-1135\nbody",
        typed_input={"source_node_id": "paragraph:issue-1135", "body": "body"},
        item_ref="paragraph:issue-1135",
    )

    def fail_once(deployment: str, configured_limit: int) -> dict[str, Any]:
        extractor = ClaimExtractor(
            confidence_floor=0.6,
            llm=_PerItemLLM(),
            input_quarantine_max_attempts=configured_limit,
        )
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            extractor._run_conn = conn
            extractor._run_deployment_name = deployment
            extractor._run_branch_name = "default"
            extractor._run_generation_id = 7
            outcomes = extractor._complete_many(conn, [job], generation_id=7)
            assert outcomes[0].content == _GOOD_POISON
            extractor._record_input_failure(
                job.item_ref,
                IncompleteExtractionResponse("same deterministic failure"),
            )
            extractor._finish_publisher_transaction(conn, commit=True)
            rows = _quarantines(conn, deployment)
        assert len(rows) == 1
        return rows[0]

    lowering = "issue-714-limit-lowering"
    assert fail_once(lowering, 3)["attempt_count"] == 1
    assert fail_once(lowering, 3)["attempt_count"] == 2
    lowered = fail_once(lowering, 1)
    assert (
        lowered["status"],
        lowered["attempt_count"],
        lowered["attempt_limit"],
    ) == ("active", 3, 3)

    raising = "issue-714-limit-raising"
    first = fail_once(raising, 2)
    assert (first["attempt_count"], first["attempt_limit"]) == (1, 2)
    raised = fail_once(raising, 5)
    assert (
        raised["status"],
        raised["attempt_count"],
        raised["attempt_limit"],
    ) == ("active", 2, 2)


def test_pinned_limit_does_not_leak_to_new_item_in_same_batch(
    clean_runtime: str,
) -> None:
    deployment = "issue-714-limit-per-item"
    old_job = ModelCallJob(
        system="system",
        user="Source id: paragraph:issue-1135\nbody",
        typed_input={"source_node_id": "paragraph:issue-1135", "body": "body"},
        item_ref="paragraph:issue-1135",
    )
    new_job = ModelCallJob(
        system="system",
        user="Source id: paragraph:issue-714-healthy\nbody",
        typed_input={
            "source_node_id": "paragraph:issue-714-healthy",
            "body": "body",
        },
        item_ref="paragraph:issue-714-healthy",
    )

    first = ClaimExtractor(
        confidence_floor=0.6,
        llm=_PerItemLLM(),
        input_quarantine_max_attempts=2,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first._run_conn = conn
        first._run_deployment_name = deployment
        first._run_branch_name = "default"
        first._run_generation_id = 7
        first._complete_many(conn, [old_job], generation_id=7)
        first._record_input_failure(
            old_job.item_ref,
            IncompleteExtractionResponse("first old-item failure"),
        )
        first._finish_publisher_transaction(conn, commit=True)

    next_cycle = ClaimExtractor(
        confidence_floor=0.6,
        llm=_PerItemLLM(),
        input_quarantine_max_attempts=5,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        next_cycle._run_conn = conn
        next_cycle._run_deployment_name = deployment
        next_cycle._run_branch_name = "default"
        next_cycle._run_generation_id = 8
        next_cycle._complete_many(conn, [old_job, new_job], generation_id=8)
        assert next_cycle._input_claims[old_job.item_ref].attempt_limit == 2
        assert next_cycle._input_claims[new_job.item_ref].attempt_limit == 5
        next_cycle._finish_publisher_transaction(conn, commit=False)


def test_replay_store_miss_is_read_once_before_input_claim(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "issue-714-replay-read-order"
    extractor = ClaimExtractor(
        confidence_floor=0.6,
        llm=_PerItemLLM(),
        model_outcomes_max_rows=10,
        model_outcomes_deployment=deployment,
        replay_outcomes=True,
        input_quarantine_max_attempts=2,
    )
    job = ModelCallJob(
        system="system",
        user="Source id: paragraph:issue-1135\nbody",
        typed_input={"source_node_id": "paragraph:issue-1135", "body": "body"},
        item_ref="paragraph:issue-1135",
        fragment_id="claim:paragraph:issue-1135",
    )
    events: list[str] = []
    reservation_started = False
    real_claim_inputs = extraction_base.claim_inputs

    def ordered_get(_conn: psycopg.Connection, _key: Any):
        assert not reservation_started, "outcome store read occurred after reservation"
        events.append("store_get")
        return None

    def ordered_claim(*args: Any, **kwargs: Any):
        nonlocal reservation_started
        reservation_started = True
        events.append("claim")
        return real_claim_inputs(*args, **kwargs)

    assert extractor.outcomes.store is not None
    monkeypatch.setattr(extractor.outcomes.store, "get", ordered_get)
    monkeypatch.setattr(extraction_base, "claim_inputs", ordered_claim)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        extractor._run_conn = conn
        extractor._run_deployment_name = deployment
        extractor._run_branch_name = "default"
        extractor._run_generation_id = 7
        outcomes = extractor._complete_many(conn, [job], generation_id=7)
        extractor._finish_publisher_transaction(conn, commit=True)

    assert events == ["store_get", "claim"]
    assert outcomes[0].replay_excluded is True
    assert len(extractor.outcomes.exclusions) == 1
    assert extractor.llm.calls == []


def test_concurrent_publisher_is_excluded_before_item_claim_or_dispatch(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "issue-714-concurrent-claim"
    entered_provider = threading.Event()
    release_provider = threading.Event()
    holder_result: dict[str, Any] = {}
    claim_calls = 0
    real_claim_inputs = extraction_base.claim_inputs

    class BlockingLLM(_PerItemLLM):
        def complete(self, *args: Any, **kwargs: Any) -> str:
            entered_provider.set()
            assert release_provider.wait(timeout=5.0)
            return super().complete(*args, **kwargs)

    def count_claims(*args: Any, **kwargs: Any):
        nonlocal claim_calls
        claim_calls += 1
        return real_claim_inputs(*args, **kwargs)

    monkeypatch.setattr(extraction_base, "claim_inputs", count_claims)
    monkeypatch.setenv("OKG_DBOS_PUBLISH_LOCK_WAIT_SECONDS", "0")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_extraction_graph(conn, deployment=deployment)

    holder_llm = BlockingLLM()
    holder = Publisher(
        clean_runtime,
        deployment,
        enrichers=[
            _CapturingClaimExtractor(
                confidence_floor=0.6,
                llm=holder_llm,
                input_quarantine_max_attempts=2,
            )
        ],
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )
    contender_llm = _PerItemLLM()
    contender = Publisher(
        clean_runtime,
        deployment,
        enrichers=[
            _CapturingClaimExtractor(
                confidence_floor=0.6,
                llm=contender_llm,
                input_quarantine_max_attempts=2,
            )
        ],
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )

    def run_holder() -> None:
        try:
            holder_result["result"] = holder.cycle()
        except Exception as exc:  # pragma: no cover - asserted below
            holder_result["error"] = exc

    thread = threading.Thread(target=run_holder)
    thread.start()
    try:
        assert entered_provider.wait(timeout=5.0), holder_result.get("error")
        with pytest.raises(PublisherLockHeld):
            contender.cycle()
    finally:
        release_provider.set()
        thread.join(timeout=10.0)

    assert not thread.is_alive()
    assert "error" not in holder_result, holder_result.get("error")
    assert holder_result["result"]["status"] == "published"
    assert claim_calls == 1
    assert contender_llm.calls == []


def test_provider_rejection_taxonomy_is_closed_to_explicit_stable_codes() -> None:
    class TypedProviderError(RuntimeError):
        def __init__(self, code: str) -> None:
            self.code = code
            super().__init__(f"typed provider rejection: {code}")

    for code in (
        "context_length_exceeded",
        "content_policy_rejected",
        "invalid_prompt_input",
    ):
        provider_error = TypedProviderError(code)
        client_error = LLMCallFailed("trusted client boundary")
        client_error.__cause__ = provider_error
        assert (
            ClaimExtractor._deterministic_failure_class(client_error)
            == f"provider_input_rejected:{code}"
        )

    direct_client_error = LLMCallFailed("trusted client rejection")
    direct_client_error.code = "invalid_prompt_input"
    assert (
        ClaimExtractor._deterministic_failure_class(direct_client_error)
        == "provider_input_rejected:invalid_prompt_input"
    )

    ambiguous = RuntimeError("400 context length exceeded")
    ambiguous.status_code = 400
    assert ClaimExtractor._deterministic_failure_class(ambiguous) is None
    internal_collision = TypedProviderError("context_length_exceeded")
    assert ClaimExtractor._deterministic_failure_class(internal_collision) is None
    client_error = LLMCallFailed("trusted client boundary")
    client_error.__cause__ = TypedProviderError("bad_request")
    assert ClaimExtractor._deterministic_failure_class(client_error) is None
