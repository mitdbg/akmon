"""Regression (#707): a zero-emission enricher must be visibly FLAGGED,
not rendered healthy.

Before this fix, ``okg.enricher_progress`` stored only watermarks +
``last_status``, so a cycle that ran and emitted ZERO edges was
byte-identical to a productive one. The console health view mapped
``last_status='success'`` straight to ``"healthy"`` and no doctor check
existed — a deployment ran a broken enricher for days at zero LLM calls
while every surface reported green.

The fix records a **lifetime emitted count** on ``enricher_progress``
(maintained by ``_advance_enricher_progress`` from each cycle's
``EnrichResult.n_edges_emitted``). Two consumers read it:

1. ``enricher_diagnostics._health_status`` — a wired enricher
   (``last_status='success'``) with zero lifetime emissions and no
   declared ``dormant_ok`` escape renders ``"wired-but-silent"``, not
   ``"healthy"``.
2. ``cli.doctor._enricher_liveness_findings`` — the same predicate as a
   doctor check finding.

Point (i) of the audit design (``pact/changes/remediate-audit-2026-07/
design.md``): "A wired component declares its expected liveness (or an
explicit ``dormant-ok``)". A genuinely-quiet enricher escapes the flag
by declaring ``dormant_ok`` (persisted on the progress row).

An INCOMPLETE run (#679: ``EnrichResult(incomplete=True)`` →
``last_status='failed:incomplete_inputs'``) is *failing*, not silent —
those paths are unchanged and must not regress.
"""
from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.console_views.enricher_diagnostics import (
    _health_status,
    _progress_rows,
)
from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.publisher import Publisher


# ---------------------------------------------------------------------------
# 1. Pure-Python reproduction of the #707 blind spot in _health_status
# ---------------------------------------------------------------------------


def test_health_status_flags_wired_but_silent_enricher() -> None:
    """A success row with zero lifetime emissions is NOT healthy."""
    silent = {
        "last_status": "success",
        "lifetime_edges_emitted": 0,
        "dormant_ok": False,
    }
    # Pre-fix this returned "healthy" (last_status=='success' → healthy),
    # which is exactly the #707 blind spot.
    assert _health_status(silent, None) == "wired-but-silent"


def test_health_status_productive_enricher_is_healthy() -> None:
    productive = {
        "last_status": "success",
        "lifetime_edges_emitted": 42,
        "dormant_ok": False,
    }
    assert _health_status(productive, None) == "healthy"


def test_health_status_dormant_ok_escape_is_healthy() -> None:
    """The declared dormant-ok escape (design point (i)) suppresses the
    flag for a legitimately-quiet enricher."""
    dormant = {
        "last_status": "success",
        "lifetime_edges_emitted": 0,
        "dormant_ok": True,
    }
    assert _health_status(dormant, None) == "healthy"


def test_health_status_incomplete_is_still_failing_not_silent() -> None:
    """A #679 incomplete run is failing, not silent — do not regress it."""
    incomplete = {
        "last_status": "failed:incomplete_inputs",
        "lifetime_edges_emitted": 0,
        "dormant_ok": False,
    }
    assert _health_status(incomplete, None) == "failing"


def test_health_status_never_run_is_unknown() -> None:
    never = {
        "last_status": "never",
        "lifetime_edges_emitted": 0,
        "dormant_ok": False,
    }
    assert _health_status(never, None) == "unknown"


# ---------------------------------------------------------------------------
# 2. No-DB seam: _run_enrichers feeds n_edges_emitted into the advance
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


class _ResultEnricher:
    publisher_skip_when_clean = False
    reads_subtypes = ()
    reads_edge_types = ()
    requires_narrowings = ()
    # Dev's `consistency-output-envelopes` contract (enricher_loader
    # `derive_write_envelope` / `validate_enricher_envelopes`) refuses an
    # enricher that declares no write surface. Declare a typed-node emit so
    # the envelope derives (nodes-only) WITHOUT a requires_narrowings triple
    # — a triple would make `validate_enricher_narrowings` open a DB
    # connection to the stub's dummy DSN at Publisher construction. The
    # liveness assertions below (emitted_count forwarding) are unaffected.
    emits_typed_subtype = "thing"
    # Boot-time contract (PACT bind-derived-node-provenance): a
    # node-emitting enricher must declare it supplies provenance.
    declares_node_provenance = True

    def __init__(self, name: str, result: EnrichResult) -> None:
        self.name = name
        self._result = result

    def enrich(self, conn: Any, *, generation_id: int, incremental=None):
        return self._result


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


def _capture_advance(monkeypatch, publisher: Publisher) -> list[dict[str, Any]]:
    advance_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        publisher, "_advance_enricher_progress",
        lambda conn, ename, **kw: advance_calls.append({"ename": ename, **kw}),
    )
    monkeypatch.setattr(
        publisher, "_admit_enricher_outputs", lambda conn, **kw: None,
    )
    return advance_calls


def test_zero_emission_run_advances_with_emitted_count_zero(
    monkeypatch,
) -> None:
    """The #707 broken enricher: returns n_edges_emitted=0 every cycle.
    The publisher must forward that 0 into the lifetime accumulator so a
    perpetually-silent enricher never accrues a lifetime emission."""
    result = EnrichResult("silent_ex", n_edges_emitted=0)
    publisher = Publisher(
        "postgres://unused", "test",
        enrichers=[_ResultEnricher("silent_ex", result)],
    )
    _wire_publisher_stubs(monkeypatch, publisher)
    advance_calls = _capture_advance(monkeypatch, publisher)

    publisher._run_enrichers(_Conn(), generation_id=99, catalog_version_id=7)

    assert len(advance_calls) == 1
    assert advance_calls[0]["success"] is True
    assert advance_calls[0]["emitted_count"] == 0


def test_productive_run_forwards_edges_emitted(monkeypatch) -> None:
    result = EnrichResult("busy_ex", n_edges_emitted=7)
    publisher = Publisher(
        "postgres://unused", "test",
        enrichers=[_ResultEnricher("busy_ex", result)],
    )
    _wire_publisher_stubs(monkeypatch, publisher)
    advance_calls = _capture_advance(monkeypatch, publisher)

    publisher._run_enrichers(_Conn(), generation_id=99, catalog_version_id=7)

    assert len(advance_calls) == 1
    assert advance_calls[0]["emitted_count"] == 7


# ---------------------------------------------------------------------------
# 3. (DB) Lifetime accumulation + the doctor liveness check
# ---------------------------------------------------------------------------


def _read_lifetime(conn, ename: str) -> tuple[int, str, bool]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT lifetime_edges_emitted, last_status, dormant_ok
              FROM okg.enricher_progress
             WHERE deployment_name = %s AND enricher_name = %s
            """,
            ("test", ename),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0]), str(row[1]), bool(row[2])


@pytest.mark.postgres
def test_advance_accumulates_lifetime_and_doctor_flags_zero(
    clean_runtime: str,
) -> None:
    from okg.substrate.cli.doctor import _enricher_liveness_findings

    publisher = Publisher("postgres://unused", "test")
    silent = "silent_ex"
    busy = "busy_ex"

    with psycopg.connect(clean_runtime) as conn:
        # A broken enricher: three successful cycles, zero edges each.
        for gen in (1, 2, 3):
            publisher._advance_enricher_progress(
                conn, silent,
                success=True, failure_repr=None,
                generation_id=gen, catalog_version_id=1,
                current_fact_watermark=10 * gen,
                current_edge_watermark=5 * gen,
                emitted_count=0,
            )
        lifetime, status, dormant = _read_lifetime(conn, silent)
        assert lifetime == 0
        assert status == "success"
        assert dormant is False

        # A productive enricher accumulates across cycles.
        publisher._advance_enricher_progress(
            conn, busy,
            success=True, failure_repr=None,
            generation_id=1, catalog_version_id=1,
            current_fact_watermark=10, current_edge_watermark=5,
            emitted_count=3,
        )
        publisher._advance_enricher_progress(
            conn, busy,
            success=True, failure_repr=None,
            generation_id=2, catalog_version_id=1,
            current_fact_watermark=20, current_edge_watermark=10,
            emitted_count=4,
        )
        busy_lifetime, _, _ = _read_lifetime(conn, busy)
        assert busy_lifetime == 7

        # Doctor: the silent enricher is flagged; the busy one is not.
        findings = _enricher_liveness_findings(conn, "test")
        flagged = {f["enricher_name"] for f in findings}
        assert silent in flagged
        assert busy not in flagged

        # Diagnostics: the silent enricher renders wired-but-silent.
        rows = _progress_rows(conn, "test")
        assert _health_status(rows[silent], None) == "wired-but-silent"
        assert _health_status(rows[busy], None) == "healthy"


@pytest.mark.postgres
def test_dormant_ok_escape_suppresses_doctor_flag(
    clean_runtime: str,
) -> None:
    from okg.substrate.cli.doctor import _enricher_liveness_findings

    publisher = Publisher("postgres://unused", "test")
    ename = "dormant_ex"

    with psycopg.connect(clean_runtime) as conn:
        publisher._advance_enricher_progress(
            conn, ename,
            success=True, failure_repr=None,
            generation_id=1, catalog_version_id=1,
            current_fact_watermark=10, current_edge_watermark=5,
            emitted_count=0,
            dormant_ok=True,
        )
        lifetime, status, dormant = _read_lifetime(conn, ename)
        assert lifetime == 0
        assert dormant is True

        findings = _enricher_liveness_findings(conn, "test")
        assert ename not in {f["enricher_name"] for f in findings}

        rows = _progress_rows(conn, "test")
        assert _health_status(rows[ename], None) == "healthy"
