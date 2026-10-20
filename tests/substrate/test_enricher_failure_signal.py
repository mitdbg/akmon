"""Issue #839: failed enrichers are first-class publish/doctor signals."""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


class _RecoveringEnricher:
    name = "recovering_enricher"
    publisher_skip_when_clean = False
    dormant_ok = True
    reads_subtypes = ("concept",)
    reads_edge_types = ()
    emits_edge_types = ()
    requires_narrowings = ()
    write_envelope = {
        "nodes": [{"subtype": "concept"}],
        "nondeterministic": False,
    }
    declares_node_provenance = True

    def __init__(self) -> None:
        self.fail = True
        self.generation_ids: list[int] = []
        self.working_head_reads = 0

    def enrich(self, conn, *, generation_id, incremental=None):
        del incremental
        self.generation_ids.append(generation_id)
        if self.fail:
            # A real database exception aborts PostgreSQL's transaction until
            # its enclosing savepoint rolls back. This proves the publisher can
            # still persist failure proof and complete its non-gating publish.
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM okg.issue_839_missing_relation")

        checkout = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout_generation(generation_id)
        self.working_head_reads = int(
            checkout.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        return EnrichResult(self.name, n_edges_emitted=0)


def _run_record(result: dict[str, object]) -> dict[str, object]:
    return next(
        row
        for row in result["enricher_runs"]  # type: ignore[index]
        if row["name"] == _RecoveringEnricher.name
    )


def _progress(conn) -> tuple[int, int, int, str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fact_watermark, edge_watermark, generation_id,
                   last_status, consecutive_failure_count
              FROM okg.enricher_progress
             WHERE deployment_name = 'test-pub'
               AND enricher_name = 'recovering_enricher'
            """
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0]), int(row[1]), int(row[2]), str(row[3]), int(row[4])


def _doctor_payload(clean_runtime: str, capsys) -> tuple[int, dict[str, object]]:
    exit_code = main([
        "doctor",
        "--check", "enricher-liveness",
        "--deployment", "test-pub",
        "--dsn", clean_runtime,
        "--json",
    ])
    captured = capsys.readouterr()
    output = captured.out.strip() or captured.err.strip()
    return exit_code, json.loads(output)


def test_failed_enricher_is_counted_in_publish_and_doctor_until_recovery(
    clean_runtime: str,
    capsys,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["concept"], narrowings=[])

    enricher = _RecoveringEnricher()
    publisher = Publisher(clean_runtime, "test-pub", enrichers=[enricher], catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))

    first = publisher.cycle()
    first_run = _run_record(first)
    assert first["status"] == "published"
    assert first_run["status"] == "failed"
    assert first_run["exception_class"] == "UndefinedTable"
    assert first_run["consecutive_failure_count"] == 1
    assert int(first_run["compat_generation_id"]) > 0

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first_progress = _progress(conn)
    assert first_progress[2:] == (
        first_run["compat_generation_id"],
        "failed:UndefinedTable",
        1,
    )

    exit_code, payload = _doctor_payload(clean_runtime, capsys)
    assert exit_code == 1
    assert payload["ok"] is False
    assert payload["failed_count"] == 1
    finding = payload["findings"][0]
    assert finding["enricher_name"] == enricher.name
    assert finding["exception_class"] == "UndefinedTable"
    assert finding["consecutive_failure_count"] == 1
    assert finding["compat_generation_id"] == first_run["compat_generation_id"]

    second = publisher.cycle()
    second_run = _run_record(second)
    assert second["status"] == "published"
    assert second_run["consecutive_failure_count"] == 2
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        second_progress = _progress(conn)
    assert second_progress[:2] == first_progress[:2]
    assert second_progress[2:] == (
        second_run["compat_generation_id"],
        "failed:UndefinedTable",
        2,
    )

    enricher.fail = False
    recovered = publisher.cycle()
    recovered_run = _run_record(recovered)
    assert recovered["status"] == "published"
    assert recovered_run["status"] == "ok"
    assert recovered_run["consecutive_failure_count"] == 0
    assert int(recovered_run["compat_generation_id"]) > 0
    assert enricher.generation_ids == [0, 0, 0]
    assert enricher.working_head_reads == 0

    exit_code, payload = _doctor_payload(clean_runtime, capsys)
    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["failed_count"] == 0
    assert payload["silent_count"] == 0


def test_enricher_liveness_doctor_query_error_is_not_green(
    clean_runtime: str,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import doctor as doctor_module

    def fail_query(*_args, **_kwargs):
        raise RuntimeError("simulated progress query failure")

    monkeypatch.setattr(doctor_module, "_enricher_failure_findings", fail_query)

    exit_code, payload = _doctor_payload(clean_runtime, capsys)

    assert exit_code == 1
    assert "simulated progress query failure" in payload["error"]["message"]


def test_enricher_liveness_doctor_names_active_input_quarantine(
    clean_runtime: str,
    capsys,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.enricher_input_quarantines
              (deployment_name, branch_name, enricher_name, item_ref,
               input_signature, operator_version, model_id, prompt_hash,
               input_hash, status, attempt_count, attempt_limit,
               last_failure_class, last_error, first_failed_at,
               last_failed_at, quarantined_at)
            VALUES
              ('test-pub', 'default', 'claim_extractor', 'paragraph:poison',
               'signature-714', 'v1', 'model', 'prompt', 'input', 'active',
               2, 2, 'incomplete_extraction_response', 'malformed JSON',
               now() - interval '1 minute', now(), now())
            """
        )

    exit_code, payload = _doctor_payload(clean_runtime, capsys)
    assert exit_code == 1
    assert payload["quarantine_count"] == 1
    finding = payload["findings"][0]
    assert finding["reason"] == "enricher_input_quarantined"
    assert finding["deployment_name"] == "test-pub"
    assert finding["branch_name"] == "default"
    assert finding["enricher_name"] == "claim_extractor"
    assert finding["item_ref"] == "paragraph:poison"
    assert finding["input_signature"] == "signature-714"
    assert finding["attempt_count"] == finding["attempt_limit"] == 2
    assert finding["error"] == "malformed JSON"
    assert finding["first_failed_at"]
    assert finding["last_failed_at"]
