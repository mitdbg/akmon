"""Regression: an enricher run that did not fully process its inputs
(a per-item LLM call failed) must NOT advance its watermark.

Companion to
``tests/substrate/extraction/test_extractor_llm_failure_watermark.py``
(which proves the base extractor marks such a run
``EnrichResult(incomplete=True)``). This file proves the PUBLISHER half
of the contract:

1. ``_enricher_run_is_complete`` gates the watermark advance on the
   ``incomplete`` flag (not merely ``result is not None``).
2. Driving the real ``_run_enrichers`` with an incomplete result calls
   ``_advance_enricher_progress(success=False)`` (watermark held) while
   still admitting the GOOD outputs (``failure_reason=None``).
3. ``_should_skip_clean_enricher`` re-runs the enricher next cycle
   whenever the held ``last_status`` is not ``"success"`` (so the failed
   item is reprocessed rather than skipped).
4. (DB) ``_advance_enricher_progress(success=False)`` leaves a
   pre-seeded ``okg.enricher_progress.fact_watermark`` un-advanced.

Pre-fix, the extractor swallowed the failure and the publisher advanced
the watermark PAST the failed item, permanently and silently losing its
typed nodes (a P0 data-loss bug).
"""
from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.enrichers.base import EnrichResult, IncrementalContext
from okg.substrate.publisher import Publisher


# ---------------------------------------------------------------------------
# Mock psycopg primitives for the no-DB seam tests
# ---------------------------------------------------------------------------


class _Cursor:
    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_a: Any) -> None:
        return None

    def execute(self, *_a: Any, **_k: Any) -> None:
        return None


class _Conn:
    def cursor(self) -> _Cursor:
        return _Cursor()


# ---------------------------------------------------------------------------
# 1. The watermark-advance predicate
# ---------------------------------------------------------------------------


def test_run_is_complete_predicate() -> None:
    complete = EnrichResult("e", n_edges_emitted=3)
    incomplete = EnrichResult(
        "e", n_edges_emitted=3, incomplete=True, n_input_failures=1,
    )

    # A clean run advances; an incomplete run and a raised run (None) do
    # NOT (they hold the watermark for retry).
    assert Publisher._enricher_run_is_complete(complete) is True
    assert Publisher._enricher_run_is_complete(incomplete) is False
    assert Publisher._enricher_run_is_complete(None) is False


# ---------------------------------------------------------------------------
# 2. _run_enrichers maps incomplete -> hold watermark, keep good outputs
# ---------------------------------------------------------------------------


class _ResultEnricher:
    """Enricher that returns a caller-supplied EnrichResult."""

    publisher_skip_when_clean = False
    reads_subtypes = ()
    reads_edge_types = ()
    requires_narrowings = ()
    # Explicit minimal envelope: the boot guard demands a declared
    # write surface; this fixture's behavior under test is watermark
    # accounting, not admission.
    write_envelope = {
        "nodes": [{"subtype": "concept"}],
        "nondeterministic": False,
    }
    # The node surface above exists only to satisfy the write-envelope boot
    # guard; this fixture emits no node facts at all, so the provenance
    # declaration is vacuously satisfied. Behavior under test is watermark
    # accounting, not admission.
    declares_node_provenance = True

    def __init__(self, name: str, result: EnrichResult) -> None:
        self.name = name
        self._result = result

    def enrich(self, conn: Any, *, generation_id: int, incremental=None):
        return self._result


def _wire_publisher_stubs(monkeypatch, publisher: Publisher) -> None:
    """Stub out every DB-touching helper _run_enrichers calls so it runs
    against a mock connection (mirrors test_enricher_registry_lineage)."""
    monkeypatch.setattr(
        "okg.substrate.publisher.publisher.persist_transform_program",
        lambda conn, program: None,
    )
    monkeypatch.setattr(
        publisher, "_load_enricher_progress_rows",
        lambda conn, names: {n: (5, 6, "success", None, 7) for n in names},
    )
    monkeypatch.setattr(
        publisher, "_load_enricher_profile_fingerprints",
        lambda conn, producer_ids: {p: f"fp:{p}" for p in producer_ids},
    )
    monkeypatch.setattr(
        publisher, "_current_fact_watermarks", lambda conn: (5, 6),
    )
    monkeypatch.setattr(
        publisher, "_current_catalog_version_id", lambda conn: 7,
    )
    monkeypatch.setattr(
        publisher, "_query_dirty_from_chronos_current_segment_by_type",
        lambda conn, *, branch_name, previous=None, timing_sink=None,
        timing_prefix=None: ({}, {}, True, None, {}, None),
    )
    monkeypatch.setattr(
        publisher, "_expected_enricher_profile_fingerprint",
        lambda conn, *, enricher, producer_id, catalog_version_id: (
            f"fp:_enricher:{enricher.name}"
        ),
    )
    monkeypatch.setattr(
        publisher, "_materialize_enricher_profile",
        lambda *a, **k: "fp",
    )


def test_incomplete_result_holds_watermark_but_admits_good_outputs(
    monkeypatch,
) -> None:
    result = EnrichResult(
        "partial_ex", n_edges_emitted=2, incomplete=True,
        n_input_failures=1,
        error="1 input LLM call(s) failed (chunk:s2: LLMCallFailed: boom); "
              "watermark held so unprocessed inputs retry next cycle",
    )
    publisher = Publisher(
        "postgres://unused", "test",
        enrichers=[_ResultEnricher("partial_ex", result)],
    )
    _wire_publisher_stubs(monkeypatch, publisher)

    advance_calls: list[dict[str, Any]] = []
    admit_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        publisher, "_advance_enricher_progress",
        lambda conn, ename, **kw: advance_calls.append({"ename": ename, **kw}),
    )
    monkeypatch.setattr(
        publisher, "_admit_enricher_outputs",
        lambda conn, **kw: admit_calls.append(kw),
    )

    runs = publisher._run_enrichers(
        _Conn(), generation_id=99, catalog_version_id=7,
    )

    # Watermark held: the advance is called with success=False and a
    # progress status that (a) records the hold and (b) is a VALID
    # enricher_progress.last_status. The DB constrains last_status to
    # ('never','success') OR LIKE 'failed:%', so the held status MUST be
    # 'failed:'-prefixed or the advance would CheckViolation and abort
    # the whole publish (worse than the original silent-loss bug).
    assert len(advance_calls) == 1
    assert advance_calls[0]["success"] is False
    assert advance_calls[0]["failure_repr"] == "failed:incomplete_inputs"
    assert advance_calls[0]["failure_repr"].startswith("failed:")

    # Good outputs STILL admit this cycle: an incomplete-but-returned run
    # passes failure_reason=None to admission (only a raised enricher
    # fails its output range).
    assert len(admit_calls) == 1
    assert admit_calls[0]["failure_reason"] is None

    run = next(r for r in runs if r["name"] == "partial_ex")
    assert run["status"] == "partial"
    assert "LLMCallFailed: boom" in run["reason"]


def test_complete_result_advances_watermark(monkeypatch) -> None:
    result = EnrichResult("clean_ex", n_edges_emitted=4)  # incomplete=False
    publisher = Publisher(
        "postgres://unused", "test",
        enrichers=[_ResultEnricher("clean_ex", result)],
    )
    _wire_publisher_stubs(monkeypatch, publisher)

    advance_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        publisher, "_advance_enricher_progress",
        lambda conn, ename, **kw: advance_calls.append({"ename": ename, **kw}),
    )
    monkeypatch.setattr(
        publisher, "_admit_enricher_outputs", lambda conn, **kw: None,
    )

    runs = publisher._run_enrichers(
        _Conn(), generation_id=99, catalog_version_id=7,
    )

    assert len(advance_calls) == 1
    assert advance_calls[0]["success"] is True
    assert advance_calls[0]["failure_repr"] is None
    run = next(r for r in runs if r["name"] == "clean_ex")
    assert run["status"] == "ok"


# ---------------------------------------------------------------------------
# 3. A held (non-success) status re-runs the enricher next cycle
# ---------------------------------------------------------------------------


class _SkippableEnricher:
    name = "skippable"
    publisher_skip_when_clean = True
    reads_subtypes = ()
    reads_edge_types = ()
    requires_narrowings = ()
    # Explicit minimal envelope (see _ResultEnricher).
    write_envelope = {
        "nodes": [{"subtype": "concept"}],
        "nondeterministic": False,
    }
    # The node surface above exists only to satisfy the write-envelope boot
    # guard; this fixture emits no node facts at all, so the provenance
    # declaration is vacuously satisfied. Behavior under test is watermark
    # accounting, not admission.
    declares_node_provenance = True


def _ctx(last_status: str) -> IncrementalContext:
    return IncrementalContext(
        prev_fact_watermark=100,
        prev_edge_watermark=50,
        dirty_node_ids=frozenset(),
        is_first_run=False,
        last_status=last_status,
        prev_catalog_version_id=7,
        current_catalog_version_id=7,
    )


def test_incomplete_status_forces_rerun_success_status_skips() -> None:
    publisher = Publisher("postgres://unused", "test")
    enricher = _SkippableEnricher()

    # The canonical held status the publisher writes on an incomplete run
    # ("failed:incomplete_inputs", within the enricher_progress_last_status_chk
    # CHECK) is not "success" => the enricher is NOT skipped, so the failed
    # item is reprocessed next cycle.
    assert publisher._should_skip_clean_enricher(
        _Conn(), enricher, _ctx("failed:incomplete_inputs"),
        catalog_version_id=7,
    ) is False

    # Control: an identical clean context WITH last_status="success" DOES
    # skip — proving it is specifically the held status that forces the
    # retry, not some other difference.
    assert publisher._should_skip_clean_enricher(
        _Conn(), enricher, _ctx("success"), catalog_version_id=7,
    ) is True


# ---------------------------------------------------------------------------
# 4. (DB) _advance_enricher_progress(success=False) holds the watermark
# ---------------------------------------------------------------------------


def _read_progress(conn, ename: str) -> tuple[int, int, str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fact_watermark, edge_watermark, last_status, generation_id
              FROM okg.enricher_progress
             WHERE deployment_name = %s AND enricher_name = %s
            """,
            ("test", ename),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0]), int(row[1]), str(row[2]), int(row[3])


@pytest.mark.postgres
def test_advance_progress_success_false_holds_watermark_db(
    clean_runtime: str,
) -> None:
    publisher = Publisher("postgres://unused", "test")
    ename = "partial_ex"

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.enricher_progress
                  (deployment_name, enricher_name, fact_watermark, edge_watermark,
                   generation_id, catalog_version_id, last_run_at, last_status)
                VALUES ('test', %s, 100, 50, 1, 1, now(), 'success')
                """,
                (ename,),
            )

        # Incomplete run: watermark must NOT move; status records the hold.
        # Use the exact held status the publisher writes — it must satisfy
        # enricher_progress_last_status_chk (LIKE 'failed:%') or this
        # INSERT raises CheckViolation in production.
        publisher._advance_enricher_progress(
            conn, ename,
            success=False,
            failure_repr="failed:incomplete_inputs",
            generation_id=2,
            catalog_version_id=1,
        )
        fw, ew, status, gen = _read_progress(conn, ename)
        assert (fw, ew) == (100, 50), "watermark advanced past a failed item"
        assert status == "failed:incomplete_inputs"
        assert status.startswith("failed:")  # DB CHECK: last_status LIKE 'failed:%'
        assert gen == 2  # generation/last_run advance; the watermark does not

        # A later fully-complete run DOES advance the watermark.
        publisher._advance_enricher_progress(
            conn, ename,
            success=True,
            failure_repr=None,
            generation_id=3,
            catalog_version_id=1,
            current_fact_watermark=200,
            current_edge_watermark=150,
        )
        fw, ew, status, gen = _read_progress(conn, ename)
        assert (fw, ew) == (200, 150)
        assert status == "success"
