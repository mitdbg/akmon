"""Enforcement tests for the publish/admission commit protocol.

PACT ``specify-publish-commit-protocol`` (#695/#653/#708) specifies the
order in which publisher and admission effects become durable as six
named properties.  Each test here induces the failure the property
forbids and asserts the invariant holds; every test fails on the
pre-fix code.

Property map (see ``reports/design-review.md`` for the ratified
rulings):

* P1 -- envelope accept commits before any chronos default-branch
  effect becomes durable, or the effect is idempotent and reconciled.
* P2 -- interval mint+sync+reconcile complete in one transaction before
  ``checkpoint_generation``; a reconcile failure refuses the checkpoint.
* P3 -- phase events are flushed on failure paths, not only on
  ``cycle_complete``.
* P4 -- a watermark advances only over proven-complete inputs, bounded
  by the enricher-phase-start positions, never to a live max that jumps
  a sibling enricher's just-emitted facts.
* P5 -- a missing checkpoint status is a loud error, never
  ``'published'``.
* P6 -- the publisher noop-reuse probe's input closure includes
  enricher retry state, and a sanctioned force lever bypasses the skip.

The DB-fixture scenarios (marked ``postgres``) require the runtime
schema on 127.0.0.1:5433 with ``OKG_TRUST_AS_EPHEMERAL=1``.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.enrichers._fact_emitters import emit_edge_fact
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.publisher import publisher as publisher_mod
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


# ---------------------------------------------------------------------------
# Shared mock psycopg primitives (mirror
# tests/substrate/test_enricher_input_failure_holds_watermark.py).
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


# ===========================================================================
# P4 -- watermark bounded by enricher-phase-start positions
# ===========================================================================


class _ResultEnricher:
    """Enricher returning a caller-supplied EnrichResult (no DB)."""

    publisher_skip_when_clean = False
    reads_subtypes = ()
    reads_edge_types = ()
    requires_narrowings = ()
    write_envelope = {
        "nodes": [{"subtype": "concept"}],
        "nondeterministic": False,
    }
    # The node surface above exists only to satisfy the write-envelope boot
    # guard; this fixture emits no node facts at all, so the provenance
    # declaration is vacuously satisfied. Behavior under test is publish
    # commit/watermark accounting, not admission.
    declares_node_provenance = True

    def __init__(self, name: str, result: EnrichResult) -> None:
        self.name = name
        self._result = result

    def enrich(self, conn: Any, *, generation_id: int, incremental=None):
        return self._result


# Phase-start bounds the publisher captures once, before any enricher
# runs (publisher.py `_run_enrichers`, the `_current_fact_watermarks`
# read).  P4 requires the SUCCESS advance to be pinned to exactly these.
_PHASE_START_FACT_WM = 5
_PHASE_START_EDGE_WM = 6


def _wire_publisher_stubs(monkeypatch, publisher: Publisher) -> None:
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
    # Phase-start bounds: this is the ONE read the P4 success advance must
    # be pinned to.  A pre-fix run reads live max at advance time instead.
    monkeypatch.setattr(
        publisher, "_current_fact_watermarks",
        lambda conn: (_PHASE_START_FACT_WM, _PHASE_START_EDGE_WM),
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


def test_p4_success_advance_is_bounded_by_phase_start(monkeypatch) -> None:
    """P4: a complete enricher's watermark advance is pinned to the
    enricher-phase-start bounds, not read from the live max at advance
    time.

    Pre-fix, the success call site passed no ``current_*_watermark``, so
    ``_advance_enricher_progress`` fell through to
    ``self._current_fact_watermarks(conn)`` READ AT ADVANCE TIME -- which
    on a real cycle includes a sibling enricher's just-emitted facts,
    silently skipping them next cycle.  This test asserts the success
    advance carries the captured phase-start bounds so the sibling's
    facts remain in next cycle's delta.
    """
    result = EnrichResult("clean_ex", n_edges_emitted=4)  # complete
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

    publisher._run_enrichers(
        _Conn(), generation_id=99, catalog_version_id=7,
    )

    assert len(advance_calls) == 1
    call = advance_calls[0]
    assert call["success"] is True
    # The load-bearing P4 assertion: the success advance is bounded by the
    # phase-start read, so `_advance_enricher_progress` never reads the
    # live max (which would jump a sibling's same-cycle emissions).
    assert call.get("current_fact_watermark") == _PHASE_START_FACT_WM, (
        "P4: success advance must pass the enricher-phase-start fact "
        "watermark; a None here lets _advance_enricher_progress read the "
        "live max and skip a sibling enricher's just-emitted facts"
    )
    assert call.get("current_edge_watermark") == _PHASE_START_EDGE_WM, (
        "P4: success advance must pass the enricher-phase-start edge "
        "watermark, not the live max"
    )


class _P4EdgeEmitter:
    name = "p4_edge_emitter"
    publisher_skip_when_clean = True
    reads_subtypes = ("p4_source",)
    reads_edge_types = ()
    emits_edge_types = ("p4_links",)
    requires_narrowings = (("p4_source", "p4_links", "p4_target"),)
    nondeterministic = False
    write_envelope = {
        "edges": [
            {
                "src_subtype": "p4_source",
                "edge_type": "p4_links",
                "dst_subtype": "p4_target",
            },
        ],
        "nondeterministic": False,
    }

    def __init__(self) -> None:
        self.calls = 0

    def enrich(self, conn, *, generation_id, incremental=None):
        self.calls += 1
        should_emit = (
            incremental is not None and "p4:source" in incremental.dirty_node_ids
        )
        if not should_emit:
            return EnrichResult(self.name, n_edges_emitted=0)
        outcome = emit_edge_fact(
            conn,
            enricher_name=self.name,
            src="p4:source",
            dst="p4:target",
            edge_type="p4_links",
            attrs={},
            provenance="derived_deterministic",
            confidence=1.0,
            dedupe_key=f"p4-edge:{generation_id}",
            source_record_id={"fixture": "p4-edge"},
            source_revision={"generation_id": str(generation_id)},
            run_id=str(uuid.uuid4()),
            observed_at=datetime.now(timezone.utc),
        )
        return EnrichResult(
            self.name,
            n_edges_emitted=int(outcome.inserted),
            rejected_fact_count=int(not outcome.emitted),
            incomplete=not outcome.emitted,
        )


class _P4EdgeConsumer:
    name = "p4_edge_consumer"
    publisher_skip_when_clean = True
    reads_subtypes = ()
    reads_edge_types = ("p4_links",)
    emits_edge_types = ()
    emits_typed_subtype = "p4_target"
    requires_narrowings = ()
    declares_node_provenance = True
    nondeterministic = False
    write_envelope = {
        "nodes": [{"subtype": "p4_target"}],
        "nondeterministic": False,
    }

    def __init__(self) -> None:
        self.contexts = []

    def enrich(self, conn, *, generation_id, incremental=None):
        del conn, generation_id
        self.contexts.append(incremental)
        return EnrichResult(self.name, n_edges_emitted=0)


@pytest.mark.postgres
def test_p4_real_publish_leaves_sibling_edge_beyond_consumer_watermark(
    clean_runtime: str,
    record_property,
) -> None:
    """A real published generation leaves same-phase sibling output unread.

    The emitter runs before the consumer. Both receive the phase-start dirty
    scope, so the consumer cannot see the emitter's new edge until the next
    cycle. Its durable watermark must therefore remain below that edge fact.
    """
    deployment = "p4-real-watermark"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["p4_source", "p4_target"],
            narrowings=[("p4_source", "p4_links", "p4_target")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "p4:source",
                    "p4_source",
                    {},
                    {"fixture": "p4-source"},
                    {"revision": 1},
                ),
                NodeFact(
                    "p4:target",
                    "p4_target",
                    {},
                    {"fixture": "p4-target"},
                    {"revision": 1},
                ),
            ],
            source="p4_fixture",
        )

    emitter = _P4EdgeEmitter()
    consumer = _P4EdgeConsumer()
    publisher = Publisher(
        clean_runtime,
        deployment,
        enrichers=[emitter, consumer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )
    first = publisher.cycle()
    assert first["status"] == "published"
    assert first["edges_added"] == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT max(fact_id) FROM okg.edge_facts "
            "WHERE source = '_enricher:p4_edge_emitter'",
        )
        emitted_edge_fact_id = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT edge_watermark, last_status
              FROM okg.enricher_progress
             WHERE deployment_name = %s
               AND enricher_name = %s
            """,
            (deployment, consumer.name),
        )
        consumer_watermark, consumer_status = cur.fetchone()

    assert consumer_status == "success"
    assert int(consumer_watermark) < emitted_edge_fact_id
    assert not consumer.contexts[-1].dirty_edge_ids
    record_property("generation_id", first["generation_id"])
    record_property("generation_status", first["status"])
    record_property("edges_added", first["edges_added"])
    record_property("consumer_edge_watermark", int(consumer_watermark))
    record_property("sibling_edge_fact_id", emitted_edge_fact_id)


# ===========================================================================
# P5 -- a missing checkpoint status is a loud error, never 'published'
# ===========================================================================


class _StatusRowCursor:
    """Cursor that returns one checkpoint-metadata row for the
    ``_chronos_branch_interval_checkpoints`` SELECT in
    ``_load_generation_feedback``'s non-integer generation branch."""

    def __init__(self, metadata: dict[str, Any]) -> None:
        self._metadata = metadata

    def __enter__(self) -> "_StatusRowCursor":
        return self

    def __exit__(self, *_a: Any) -> None:
        return None

    def execute(self, *_a: Any, **_k: Any) -> None:
        return None

    def fetchone(self) -> tuple[Any, ...]:
        return (
            "default",
            1,
            "2026-08-29T00:00:00+00:00",
            self._metadata,
        )


class _StatusRowConn:
    def __init__(self, metadata: dict[str, Any]) -> None:
        self._metadata = metadata

    def cursor(self) -> _StatusRowCursor:
        return _StatusRowCursor(self._metadata)


def test_p5_missing_checkpoint_status_raises() -> None:
    """P5: a checkpoint whose metadata omits ``status`` is a loud error.

    Pre-fix, ``_load_generation_feedback`` returned
    ``str(metadata.get("status") or "published")`` -- a benign default
    that reports an un-attested generation as successfully published.
    """
    conn = _StatusRowConn(
        {"deployment_name": "test", "branch": "default"}
    )  # no "status"
    with pytest.raises(SubstrateError) as excinfo:
        publisher_mod_load_generation_feedback(
            conn, deployment_name="test", generation_id="generation:abc",
        )
    # The refusal names the missing attestation, not a generic error.
    assert "status" in str(excinfo.value).lower()


def test_p5_present_status_is_returned() -> None:
    """Control: an explicit status is returned verbatim (no default)."""
    conn = _StatusRowConn(
        {
            "deployment_name": "test",
            "branch": "default",
            "status": "blocked",
        }
    )
    feedback = publisher_mod_load_generation_feedback(
        conn, deployment_name="test", generation_id="generation:abc",
    )
    assert feedback["status"] == "blocked"


def publisher_mod_load_generation_feedback(conn, **kw):
    from okg.substrate.admission import _load_generation_feedback

    return _load_generation_feedback(conn, **kw)


# ===========================================================================
# P3 -- phase events are flushed on a mid-cycle failure path
# ===========================================================================


def test_p3_phase_events_flushed_on_failure(monkeypatch) -> None:
    """P3: phase events accumulated before a mid-cycle exception are
    flushed by the failure path, so a failed cycle's timeline is
    inspectable.

    Pre-fix, the batch lived only in the cycle's local ``timings_ms`` and
    was flushed exclusively on ``cycle_complete``; a mid-cycle raise
    discarded it.
    """
    emitted: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        publisher_mod, "_emit_phase_event",
        lambda name, payload: emitted.append((name, payload)),
    )
    # Force batch mode (the production default) regardless of env.
    monkeypatch.setenv("OKG_PUBLISH_PHASE_EVENTS", "batch")

    timings_ms: dict[str, Any] = {}
    # Two mid-cycle phase events accumulate in the batch (not flushed --
    # neither is "cycle_complete").
    publisher_mod._emit_phase_event_with_timing(
        timings_ms, "reserve_complete", {"generation_id": 1},
    )
    publisher_mod._emit_phase_event_with_timing(
        timings_ms, "phase_a_complete", {"converged": True},
    )
    assert emitted == [], "batched events must not emit before flush"

    # Simulate a mid-cycle raise wrapped by the failure-flush guard.
    failure_propagated = False
    try:
        with publisher_mod._phase_event_failure_flush(timings_ms):
            raise RuntimeError("mid-cycle boom")
    except RuntimeError:
        # Expected injected failure; assertions below verify the flush.
        failure_propagated = True

    # The batch was flushed on the failure path.
    assert failure_propagated, "the simulated mid-cycle failure must propagate"
    assert len(emitted) == 1, "the batch flush emits exactly one event"
    name, payload = emitted[0]
    assert name == publisher_mod._PHASE_EVENT_BATCH_EVENT_KEY
    names_in_batch = [e["name"] for e in payload["events"]]
    assert names_in_batch == ["reserve_complete", "phase_a_complete"]


def test_p3_failure_flush_noop_after_cycle_complete(monkeypatch) -> None:
    """Control: once the batch flushed on cycle_complete, the failure
    guard is a no-op (no double-emit)."""
    emitted: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        publisher_mod, "_emit_phase_event",
        lambda name, payload: emitted.append((name, payload)),
    )
    monkeypatch.setenv("OKG_PUBLISH_PHASE_EVENTS", "batch")

    timings_ms: dict[str, Any] = {}
    publisher_mod._emit_phase_event_with_timing(
        timings_ms, "reserve_complete", {"generation_id": 1},
    )
    # cycle_complete flushes the batch.
    publisher_mod._emit_phase_event_with_timing(
        timings_ms, "cycle_complete", {"status": "published"},
    )
    assert len(emitted) == 1  # the batch flush

    # A subsequent failure guard finds no pending batch and emits nothing.
    with publisher_mod._phase_event_failure_flush(timings_ms):
        pass
    assert len(emitted) == 1, "no double-flush after cycle_complete"


# ===========================================================================
# P6 -- the noop-reuse probe's input closure includes enricher retry state
# ===========================================================================


class _NamedEnricher:
    reads_subtypes = ()
    reads_edge_types = ()
    requires_narrowings = ()
    write_envelope = {
        "nodes": [{"subtype": "concept"}],
        "nondeterministic": False,
    }
    # The node surface above exists only to satisfy the write-envelope boot
    # guard; this fixture emits no node facts at all, so the provenance
    # declaration is vacuously satisfied. Behavior under test is publish
    # commit/watermark accounting, not admission.
    declares_node_provenance = True

    def __init__(self, name: str) -> None:
        self.name = name


@pytest.mark.postgres
def test_p6_held_enricher_retry_invalidates_reuse(clean_runtime: str) -> None:
    """P6 closure: an enricher held at a non-success status (a #679
    retry) is reported as pending by the publisher's reuse-closure
    predicate, so the pre-enrichment noop-reuse probe cannot skip the
    enricher stage and starve the retry on a zero-delta deployment.

    This is the publisher-side analogue of the ingest-side
    ``_stack_enrichers_have_pending_work`` check; before this fix the
    publisher probe omitted enricher retry state entirely.
    """
    import psycopg

    publisher = Publisher(
        "postgres://unused", "test",
        enrichers=[_NamedEnricher("e1"), _NamedEnricher("e2")],
    )
    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.enricher_progress
                  (deployment_name, enricher_name, fact_watermark, edge_watermark,
                   generation_id, catalog_version_id, last_run_at, last_status)
                VALUES
                  ('test', 'e1', 100, 50, 1, 1, now(), 'success'),
                  ('test', 'e2', 0, 0, 1, 1, now(), 'failed:incomplete_inputs')
                """,
            )

        # e2 is held at a non-success status => pending; the predicate
        # returns its name so the probe invalidates reuse.
        assert publisher._enricher_retry_pending_name(conn) == "e2"

        # Once e2 succeeds, no enricher is pending => reuse is allowed.
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.enricher_progress
                   SET last_status = 'success'
                 WHERE deployment_name = 'test' AND enricher_name = 'e2'
                """,
            )
        assert publisher._enricher_retry_pending_name(conn) is None


@pytest.mark.postgres
def test_p6_never_run_enricher_is_pending(clean_runtime: str) -> None:
    """A wired enricher with NO progress row has never run and must run
    at least once -- it is pending, not reusable."""
    import psycopg

    publisher = Publisher(
        "postgres://unused", "test",
        enrichers=[_NamedEnricher("fresh")],
    )
    with psycopg.connect(clean_runtime) as conn:
        assert publisher._enricher_retry_pending_name(conn) == "fresh"


# ===========================================================================
# P2 -- reconcile completes (in one transaction) before checkpoint, or the
#       checkpoint is refused
# ===========================================================================
#
# NOTE ON "fails-on-pre-fix": #663 (dbb3992cd) already reordered ordinal
# mint + interval sync + reconcile into one transaction BEFORE
# checkpoint_generation, without any regression test. Per the ratified
# design (edit #9), this child lands the SPECIFICATION, the invariant
# name, and the regression PIN -- these tests therefore pass on current
# dev by design and FAIL only if the ordering is reverted (reconcile moved
# after the checkpoint) or the reconcile failure is swallowed. The
# #653 stranding no longer reproduces on dev.


def test_p2_reconcile_precedes_checkpoint_in_one_transaction() -> None:
    """P2 ordering pin: in ``_cycle_chronos_native`` the generation ordinal
    and the overlay-range sync run inside the publication transaction that
    appears BEFORE ``store.checkpoint_generation(...)``.

    Reverting to the pre-#663 order (checkpoint first) reintroduces the
    #653 stranding -- a durably-published generation with no read
    authority -- and breaks this pin. (The interval sync and reconcile this
    pinned are retired with the attribute copies, PACT
    lane-s-storage-drop-copies.)
    """
    import inspect

    src = inspect.getsource(publisher_mod.Publisher._cycle_chronos_native)
    txn_open = src.index("_open_publication_transaction(_publication_scope)")
    ordinal_call = src.index("assigned_ordinal = assign_generation_ordinal(")
    sync_call = src.index("sync_overlay_ranges_for_generation(\n")
    checkpoint_call = src.index("store.checkpoint_generation(")

    assert txn_open < ordinal_call < sync_call < checkpoint_call, (
        "the ordinal and the range sync must run inside the publication "
        "transaction, before the checkpoint"
    )
    assert "sync_effective_interval_view_for_generation" not in src
    assert "_reconcile_effective_interval_view" not in src


# ===========================================================================
# P1 -- envelope accept commits before any chronos default-branch effect
#       becomes durable (idempotent + reconciled)
# ===========================================================================


def test_p1_no_unaccounted_durable_effect_on_envelope_failure() -> None:
    """P1: an envelope-transaction failure after the chronos write path
    must leave no partial durable write. The runtime fault test in
    ``test_admission_workflows`` proves rollback; this source pin prevents the
    split-connection hazard from being reintroduced."""
    import inspect

    from okg.substrate.runtime.admission_workflows import (
        _emit_review_payload_facts,
    )

    src = inspect.getsource(_emit_review_payload_facts)
    assert "autocommit=False" in src
    assert "psycopg.connect(graph_dsn" not in src


# ===========================================================================
# P6 -- the sanctioned force lever (--force-enrichers) drives a full cycle
# ===========================================================================


def test_p6_cycle_forwards_force_enrichers(monkeypatch) -> None:
    """``Publisher.cycle(force_enrichers=True)`` forwards the flag to the
    cycle implementation.

    Pre-fix, ``cycle`` had no such parameter (``TypeError``): there was no
    CLI-reachable way to drive a full cycle on a zero-delta deployment.
    """
    publisher = Publisher("postgres://unused", "test")
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        publisher, "_cycle_chronos_native",
        lambda **kw: captured.update(kw) or {"generation_id": "g"},
    )
    publisher.cycle(force_enrichers=True)
    assert captured.get("force_enrichers") is True

    captured.clear()
    publisher.cycle()  # default is the normal skip behavior
    assert captured.get("force_enrichers") is False


def test_p6_force_enrichers_records_operator_forced_invalidation() -> None:
    """P6 pin: when ``force_enrichers`` is set the cycle records an
    ``operator_forced`` noop-reuse invalidation (bypassing the skip),
    and does so BEFORE the ordinary reuse checks so the force always
    wins."""
    import inspect

    src = inspect.getsource(publisher_mod.Publisher._cycle_chronos_native)
    assert "if force_enrichers:" in src
    forced_at = src.index("if force_enrichers:")
    operator_forced_at = src.index('"operator_forced"')
    no_prev_at = src.index('"no_previous_generation"')
    # The force check is the FIRST arm of the invalidation chain, so it
    # short-circuits every ordinary reuse check.
    assert forced_at < operator_forced_at < no_prev_at


def test_p6_cli_run_parses_force_enrichers() -> None:
    """``okg run --once --force-enrichers`` parses to
    ``args.force_enrichers is True`` (the CLI path that drives the
    cycle, which did not exist pre-fix)."""
    from okg.substrate.cli.main import _build_parser

    parser = _build_parser()
    args = parser.parse_args(
        ["run", "--once", "--inline", "--force-enrichers"],
    )
    assert args.force_enrichers is True

    args_default = parser.parse_args(["run", "--once", "--inline"])
    assert args_default.force_enrichers is False

    ingest_args = parser.parse_args(
        ["ingest", "--deployment", "test", "--force-enrichers"],
    )
    assert ingest_args.force_enrichers is True
