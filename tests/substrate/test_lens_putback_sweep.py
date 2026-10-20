"""Tests for sweep_stale_putback_requests.

Covers both sweep policies (`sweep_to_failed` default, `auto_commit`
opt-in), idempotency across re-runs, and the cutoff/age boundary. The
fixtures lean on the same `_publish_editable_view` helper that
`test_lens_putback.py` uses for setup; this file only exercises the
sweep entry point.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.lenses import (
    LensPutbackDelta,
    commit_lens_overlay,
    prepare_lens_putback,
    register_lens_definition,
    sweep_stale_putback_requests,
)

# Reuse the helpers from the existing test module — they construct the
# view / relation / generation fixtures needed before any putback can
# happen.
from tests.substrate.test_lens_putback import (
    _generation_id,
    _insert_generation,
    _publish_editable_view,
)


pytestmark = pytest.mark.postgres


def _make_prepared_request(
    conn: psycopg.Connection,
    *,
    suffix: str,
    actor_id: str,
    idempotency_key: str,
):
    """Set up a fresh editable view + register lens + prepare one delta.

    Returns ``(lens, prepared_request)``. The prepared request's
    ``prepared_at`` is whatever Postgres ``now()`` was at INSERT time.
    """
    generation_id = _generation_id()
    _insert_generation(conn, generation_id)
    _view, lens, published_view = _publish_editable_view(
        conn,
        suffix=suffix,
        generation_id=generation_id,
    )
    register_lens_definition(conn, lens)
    prepared = prepare_lens_putback(
        conn,
        lens,
        base_view_manifest_id=published_view.manifest_id,
        target_generation_id=generation_id,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        deltas=[
            LensPutbackDelta(
                slot_name="editable_publications",
                scope_key="publication",
                row_key="publication:afi-1",
                field_path=("title",),
                expected_value="Old title",
                new_value="New title",
                source_authority="test.publications",
                source_record_id={"node_id": "publication:afi-1"},
                expected_source_revision={"revision": "rev-1"},
            )
        ],
    )
    assert prepared.status == "prepared"
    return lens, prepared


def _age_prepared_at(
    conn: psycopg.Connection, request_id, *, delta: timedelta
) -> None:
    """Backdate a prepared request so the sweeper sees it as stale."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.lens_putback_requests
               SET prepared_at = prepared_at - %(delta)s
             WHERE request_id = %(request_id)s
            """,
            {"delta": delta, "request_id": request_id},
        )
    conn.commit()


def _status(
    conn: psycopg.Connection, request_id
) -> tuple[str, dict | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, error
              FROM okg.lens_putback_requests
             WHERE request_id = %(request_id)s
            """,
            {"request_id": request_id},
        )
        row = cur.fetchone()
    return row[0], (dict(row[1]) if row[1] is not None else None)


def test_sweep_to_failed_transitions_stale_prepared(
    clean_runtime: str,
) -> None:
    """A 2-hour-old prepared row is swept to 'failed' under the
    default policy."""
    suffix = uuid4().hex
    with psycopg.connect(clean_runtime) as conn:
        _lens, prepared = _make_prepared_request(
            conn,
            suffix=suffix,
            actor_id="operator:sweep-test",
            idempotency_key="sweep-default-1",
        )
        _age_prepared_at(conn, prepared.request_id, delta=timedelta(hours=2))

        now = datetime.now(timezone.utc)
        result = sweep_stale_putback_requests(
            conn,
            deployment="test",
            now=now,
            stale_after=timedelta(hours=1),
            policy="sweep_to_failed",
        )

        assert result == {
            "considered": 1,
            "swept": 1,
            "committed": 0,
            "skipped": 0,
        }
        status, error = _status(conn, prepared.request_id)
        assert status == "failed"
        assert error is not None
        assert error["type"] == "stale_prepared_request"
        assert error["stale_after_seconds"] == 3600


def test_sweep_leaves_recent_prepared_alone(
    clean_runtime: str,
) -> None:
    """A 30-minute-old prepared row is NOT swept when stale_after=1h."""
    suffix = uuid4().hex
    with psycopg.connect(clean_runtime) as conn:
        _lens, prepared = _make_prepared_request(
            conn,
            suffix=suffix,
            actor_id="operator:sweep-test",
            idempotency_key="sweep-recent-1",
        )
        _age_prepared_at(
            conn, prepared.request_id, delta=timedelta(minutes=30)
        )

        now = datetime.now(timezone.utc)
        result = sweep_stale_putback_requests(
            conn,
            deployment="test",
            now=now,
            stale_after=timedelta(hours=1),
            policy="sweep_to_failed",
        )

        assert result["swept"] == 0
        status, _ = _status(conn, prepared.request_id)
        assert status == "prepared"


def test_sweep_skips_terminal_rows(clean_runtime: str) -> None:
    """Rows already in committed_overlay / failed are not re-touched."""
    suffix = uuid4().hex
    with psycopg.connect(clean_runtime) as conn:
        _lens, prepared = _make_prepared_request(
            conn,
            suffix=suffix,
            actor_id="operator:sweep-test",
            idempotency_key="sweep-committed-1",
        )
        commit_lens_overlay(conn, request_id=prepared.request_id)
        conn.commit()
        status_before, _ = _status(conn, prepared.request_id)
        assert status_before == "committed_overlay"

        # Backdate prepared_at (was set during prepare, before commit)
        # so it would be eligible by age — but the WHERE clause filters
        # on status='prepared', so committed rows are skipped.
        _age_prepared_at(
            conn, prepared.request_id, delta=timedelta(hours=2)
        )

        now = datetime.now(timezone.utc)
        result = sweep_stale_putback_requests(
            conn,
            deployment="test",
            now=now,
            stale_after=timedelta(hours=1),
            policy="sweep_to_failed",
        )

        assert result["swept"] == 0
        status_after, _ = _status(conn, prepared.request_id)
        assert status_after == "committed_overlay"


def test_sweep_is_idempotent(clean_runtime: str) -> None:
    """Running the sweep twice on the same DB state produces zero work
    on the second invocation."""
    suffix = uuid4().hex
    with psycopg.connect(clean_runtime) as conn:
        _lens, prepared = _make_prepared_request(
            conn,
            suffix=suffix,
            actor_id="operator:sweep-test",
            idempotency_key="sweep-idem-1",
        )
        _age_prepared_at(conn, prepared.request_id, delta=timedelta(hours=2))

        now = datetime.now(timezone.utc)
        first = sweep_stale_putback_requests(
            conn,
            deployment="test",
            now=now,
            stale_after=timedelta(hours=1),
            policy="sweep_to_failed",
        )
        second = sweep_stale_putback_requests(
            conn,
            deployment="test",
            now=now,
            stale_after=timedelta(hours=1),
            policy="sweep_to_failed",
        )

        assert first["swept"] == 1
        assert second["swept"] == 0
        assert second["considered"] == 0


def test_sweep_auto_commit_applies_overlay(clean_runtime: str) -> None:
    """Under the auto_commit policy, a stale prepared row is committed
    to the overlay log (not transitioned to failed)."""
    suffix = uuid4().hex
    with psycopg.connect(clean_runtime) as conn:
        _lens, prepared = _make_prepared_request(
            conn,
            suffix=suffix,
            actor_id="operator:sweep-test",
            idempotency_key="sweep-auto-commit-1",
        )
        _age_prepared_at(conn, prepared.request_id, delta=timedelta(hours=2))

        now = datetime.now(timezone.utc)
        result = sweep_stale_putback_requests(
            conn,
            deployment="test",
            now=now,
            stale_after=timedelta(hours=1),
            policy="auto_commit",
        )

        assert result == {
            "considered": 1,
            "swept": 0,
            "committed": 1,
            "skipped": 0,
        }
        status, _ = _status(conn, prepared.request_id)
        assert status == "committed_overlay"

        # The overlay log got the delta.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM okg.lens_overlay_log
                 WHERE request_id = %(request_id)s
                """,
                {"request_id": prepared.request_id},
            )
            (overlay_count,) = cur.fetchone()
        assert overlay_count == 1


def test_sweep_scoped_to_deployment(clean_runtime: str) -> None:
    """The sweep only touches rows whose deployment_name matches."""
    suffix = uuid4().hex
    with psycopg.connect(clean_runtime) as conn:
        _lens, prepared = _make_prepared_request(
            conn,
            suffix=suffix,
            actor_id="operator:sweep-test",
            idempotency_key="sweep-scoped-1",
        )
        _age_prepared_at(conn, prepared.request_id, delta=timedelta(hours=2))

        now = datetime.now(timezone.utc)
        # Wrong deployment name → no sweep.
        result = sweep_stale_putback_requests(
            conn,
            deployment="not-this-deployment",
            now=now,
            stale_after=timedelta(hours=1),
            policy="sweep_to_failed",
        )

        assert result["swept"] == 0
        status, _ = _status(conn, prepared.request_id)
        assert status == "prepared"


def test_sweep_unknown_policy_raises() -> None:
    """An invalid policy name raises ValueError."""
    # No DB needed — the check is at the top of the function.
    import psycopg  # noqa: F401  (annotation only)

    with pytest.raises(ValueError, match="unknown sweep policy"):
        # Pass None for conn — we expect the policy check to raise
        # before any SQL runs. If implementation changes, this test
        # will catch that.
        sweep_stale_putback_requests(
            None,  # type: ignore[arg-type]
            deployment="test",
            now=datetime.now(timezone.utc),
            stale_after=timedelta(hours=1),
            policy="not_a_real_policy",  # type: ignore[arg-type]
        )
