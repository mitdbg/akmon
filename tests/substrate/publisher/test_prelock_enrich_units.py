"""Database-free checks for issue 2941 fix 1 (pre-lock enricher work)."""

from __future__ import annotations

import uuid

from okg.substrate.publisher.prelock_enrich import (
    PrelockEnrichState,
    differing_keys,
    report_counters,
)
from okg.substrate.publisher.publisher import _publisher_slowest_phase


def test_slowest_phase_ignores_the_passes_before_the_lock() -> None:
    """The lock-headroom fix hint must name a phase inside the hold. On the
    real okg-workspace run it named stage.prelock_enrich (854 s, outside the
    lock) instead of the slowest phase under it."""
    flat = {
        "stage.prelock_enrich": 853_647.0,
        "stage.prelock_enrich.rss_after_mb": 11_189.1,
        "stage.prelock_enrich.chunk_lock_hold_ms_total": 999_999.0,
        "stage.attribute_hash_prelock": 400_000.0,
        "stage.attribute_effective_view_materialize": 338_600.0,
        "stage.enrichers": 316_000.0,
        "stage.total": 2_431_700.0,
        "stage.locked_connection": 26.9,
    }
    assert _publisher_slowest_phase(flat) == {
        "name": "stage.attribute_effective_view_materialize",
        "elapsed_ms": 338_600.0,
    }
    # The in-lock rechecks stay candidates.
    assert _publisher_slowest_phase({
        "stage.prelock_enrich_recheck": 5.0,
        "stage.enrichers": 1.0,
    }) == {"name": "stage.prelock_enrich_recheck", "elapsed_ms": 5.0}


def test_seed_dirty_scope_only_for_the_state_it_was_read_under() -> None:
    state = PrelockEnrichState(report={}, previous_checkpoint_id="gen-1")
    state.graph = {"segment_signature": {"segment_id": 4, "nodes": {"rows": 3}}}
    scope = ({"code_symbol": ["a"]}, {}, True)
    state.dirty_scope = scope

    assert state.seed_dirty_scope(
        segment_signature={"segment_id": 4, "nodes": {"rows": 4}},
        previous_checkpoint_id="gen-1",
    ) == (False, None)
    assert state.report["dirty_scope"]["seed_refused"] == (
        "segment_signature_changed"
    )
    assert state.seed_dirty_scope(
        segment_signature=state.graph["segment_signature"],
        previous_checkpoint_id="gen-0",
    ) == (False, None)
    assert state.report["dirty_scope"]["seed_refused"] == "predecessor_changed"
    assert state.seed_dirty_scope(
        segment_signature=state.graph["segment_signature"],
        previous_checkpoint_id="gen-1",
    ) == (True, scope)

    # A scope that came from the caller's hint is never seeded (the cycle
    # already holds it), and nothing is seeded after release().
    state.dirty_scope_from_hint = True
    assert state.seed_dirty_scope(
        segment_signature=state.graph["segment_signature"],
        previous_checkpoint_id="gen-1",
    ) == (False, None)
    state.dirty_scope_from_hint = False
    state.release()
    assert state.seed_dirty_scope(
        segment_signature=state.graph["segment_signature"],
        previous_checkpoint_id="gen-1",
    ) == (False, None)


def test_drop_discards_every_plan_and_reports_the_first_reason() -> None:
    state = PrelockEnrichState(report={})
    run = uuid.uuid4()
    state.plans = {"code_reference": object()}
    state.prepared = {("_enricher:code_reference", run): {0: object()}}
    state.enrichers_usable = state.derived_usable = True

    assert state.plan_for("code_reference") is not None
    assert state.prepared_for("_enricher:code_reference", run) is not None
    state.drop("inputs_changed_before_enrichers", changed=["segment_signature"])
    state.drop("later_reason")
    assert state.plan_for("code_reference") is None
    assert state.prepared_for("_enricher:code_reference", run) is None
    assert state.report["fallback"] == {
        "reason": "inputs_changed_before_enrichers",
        "changed": ["segment_signature"],
    }


def test_differing_keys_and_counters() -> None:
    assert differing_keys({"a": 1, "b": 2}, {"a": 1, "b": 3, "c": 0}) == [
        "b", "c",
    ]
    counters = report_counters({
        "planned": True,
        "rss_before_mb": 100.0,
        "rss_after_mb": 350.5,
        "rss_delta_mb": 250.5,
        "dirty_scope": {"seeded": 1, "elapsed_ms": 12.0},
        "enrichers": {
            "code_reference": {"planned": True, "used": True, "rows": 7,
                               "derived_chunks": 2},
            "test_symbol": {"planned": True, "used": False, "rows": 0},
            "declarative": {"planned": False},
        },
        "derived": {"chunks_reused": 1, "chunks_recomputed": 1,
                    "chunk_lock_hold_ms_max": 4.0},
    })
    assert counters["prelock_enrich.planned"] == 1.0
    assert counters["prelock_enrich.fallback"] == 0.0
    assert counters["prelock_enrich.enrichers_planned"] == 2.0
    assert counters["prelock_enrich.enrichers_used"] == 1.0
    assert counters["prelock_enrich.planned_rows"] == 7.0
    assert counters["prelock_enrich.derived_chunks_prepared"] == 2.0
    assert counters["prelock_enrich.chunks_recomputed"] == 1.0
    assert counters["prelock_enrich.rss_delta_mb"] == 250.5
    assert report_counters({}) == {}


def test_derived_chunk_steps_under_the_lock_are_summed_into_the_counters() -> None:
    """Issue #2941: each chunk's write-transaction entries are summed, so a
    real publish shows where the derived apply's lock hold goes. A count
    stays a count; a configured threshold is not summed."""
    from types import SimpleNamespace

    state = PrelockEnrichState(report={})
    chunk = {"prepared.reused": 1.0, "prepared.lock_hold": 10.0,
             "transaction.stamp_read_class": 2.5,
             "transaction.edge_declarers": 4.0, "prelock.prepare_total": 99.0,
             "transaction.edge_declarers.handoffs": 3.0,
             "transaction.edit_edges.splice.mixed_single_row_threshold": 8.0}
    state.note_derived_result(SimpleNamespace(primitive_results=[
        SimpleNamespace(timings_ms=chunk),
        SimpleNamespace(timings_ms={**chunk, "transaction.stamp_read_class": 1.5}),
        # Not a prepared chunk: not counted.
        SimpleNamespace(timings_ms={"transaction.stamp_read_class": 50.0}),
    ]))
    counters = report_counters(state.report)
    assert counters["prelock_enrich.chunk_lock_hold_ms_total"] == 20.0
    assert counters["prelock_enrich.chunk_sum.transaction.stamp_read_class"] == 4.0
    assert counters["prelock_enrich.chunk_sum.transaction.edge_declarers"] == 8.0
    assert counters["prelock_enrich.chunk_sum.transaction.edge_declarers.handoffs"] == 6.0
    assert not any("threshold" in key for key in counters)
    assert not any("prepare_total" in key for key in counters)
