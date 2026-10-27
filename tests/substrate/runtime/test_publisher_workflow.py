"""Tests for the publisher workflow + schedule (migrate-substrate-
runtime-to-dbos Phase 1).

Three layers of test:

  1. Pure-logic: coordination_mode argument validation + resolution.
  2. Registration: register_publisher_workflow / register_publisher_schedule
     install into the mock DBOS in a way that the scheduled enqueue
     can find the workflow.
  3. Live: deferred — needs a real DBOS singleton, covered by the
     Phase 1.4 cutover-gate live test (per the proposal).
"""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Json
from okg.substrate.publisher.repository_provenance import RepositoryProvenance


@pytest.fixture(autouse=True)
def _mock_dbos_has_no_operational_transactions(monkeypatch):
    """The mock DBOS has no ``sql_session``, so the publisher's typed-outcome
    transactions (publisher-starvation-4039) cannot run under it. Their
    recording, replay and reader are covered against a real DBOS in
    ``test_publisher_cycle_overdue.py``; here they are no-ops."""
    from types import SimpleNamespace

    from okg.substrate.runtime import workflow_operational_outcomes as outcomes

    monkeypatch.setattr(
        outcomes,
        "define_operational_transactions",
        lambda *, participant: SimpleNamespace(
            register_execution_tx=lambda: {},
            record_outcome_tx=lambda _outcome: {},
        ),
    )


# ---------------------------------------------------------------------------
# coordination_mode resolution
# ---------------------------------------------------------------------------


def _make_publisher_stub(coordination_mode):
    """Construct a Publisher with the narrowings validator stubbed
    out. Used by mode-resolution tests that don't need a real
    catalog / deployment stack."""
    import unittest.mock

    from okg.substrate.publisher.publisher import Publisher

    with unittest.mock.patch(
        "okg.substrate.publisher.enricher_loader.validate_enricher_narrowings",
        return_value=None,
    ):
        return Publisher(
            dsn="postgres://stub",
            deployment_name="test",
            coordination_mode=coordination_mode,
        )


def test_resolved_coordination_mode_explicit_dbos_passes_through():
    pub = _make_publisher_stub("dbos")
    assert pub._resolved_coordination_mode() == "dbos"


def test_resolved_coordination_mode_rejects_explicit_lease():
    with pytest.raises(ValueError, match="coordination_mode"):
        _make_publisher_stub("lease")


def test_resolved_coordination_mode_auto_resolves_to_dbos_on_error(
    monkeypatch,
):
    """When manifest resolution fails (e.g. running outside a
    deployment context), 'auto' still resolves to DBOS. The legacy
    lease path is retired."""
    pub = _make_publisher_stub("auto")
    # Force resolve_deployment to raise.
    import okg.substrate.deployments as dep
    monkeypatch.setattr(
        dep, "resolve_deployment",
        lambda name: (_ for _ in ()).throw(
            dep.DeploymentResolutionError(
                "test: forced failure"
            )
        ),
        raising=True,
    )
    assert pub._resolved_coordination_mode() == "dbos"


# ---------------------------------------------------------------------------
# coordination_mode argument validation
# ---------------------------------------------------------------------------


def test_publisher_coordination_mode_default_is_auto():
    """The default stays 'auto' for older callers, but now resolves
    to the DBOS coordination path."""
    import inspect

    from okg.substrate.publisher.publisher import Publisher

    sig = inspect.signature(Publisher.__init__)
    assert sig.parameters["coordination_mode"].default == "auto"


def test_publisher_coordination_mode_rejects_bad_value(monkeypatch):
    """Validation catches typos at construction so they don't surface
    deep inside a cycle."""
    from okg.substrate.publisher.publisher import Publisher

    # Avoid the full deployment-stack construction by patching the
    # narrowings validator (which is the only side effect of __init__
    # beyond attribute assignment).
    monkeypatch.setattr(
        "okg.substrate.publisher.enricher_loader."
        "validate_enricher_narrowings",
        lambda *a, **kw: None,
        raising=False,
    )

    with pytest.raises(ValueError, match="coordination_mode"):
        Publisher(
            dsn="postgres://stub",
            deployment_name="test",
            coordination_mode="lEaSe",  # case-sensitive
        )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def test_register_publisher_workflow_caches_handle():
    """The registration function must populate the module-level cache
    so the scheduled enqueue can find the workflow by name."""
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.publisher_workflow import (
            PUBLISHER_WORKFLOW_NAME,
            _DECORATED,
            publisher_cycle_workflow,
            register_publisher_workflow,
        )

        register_publisher_workflow(deployment="test")

        assert PUBLISHER_WORKFLOW_NAME in _DECORATED
        # Public accessor finds the decorated workflow.
        wf = publisher_cycle_workflow()
        assert callable(wf)


def test_register_publisher_workflow_clears_stale_cache():
    """A second register call (e.g. worker restart in the same Python
    process) must clear the prior cache so it doesn't carry a stale
    closure."""
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.publisher_workflow import (
            _DECORATED,
            register_publisher_workflow,
        )

        register_publisher_workflow(deployment="alpha")
        first = _DECORATED[next(iter(_DECORATED))]

        register_publisher_workflow(deployment="beta")
        second = _DECORATED[next(iter(_DECORATED))]

        # Different deployment → different closure object.
        assert first is not second


def test_publisher_cycle_workflow_accessor_raises_when_not_registered():
    """Calling the accessor before register_publisher_workflow has
    run is a programmer error and must raise."""
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.publisher_workflow import (
            _DECORATED,
            publisher_cycle_workflow,
        )
        _DECORATED.clear()

        with pytest.raises(RuntimeError, match="not registered"):
            publisher_cycle_workflow()


def test_publisher_admission_feedback_failure_is_structured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Post-publish feedback failure must not crash a successful cycle."""
    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.runtime import publisher_workflow

    monkeypatch.setattr(
        publisher_workflow,
        "_run_one_cycle",
        lambda *, deployment, branch_name="default", force_enrichers=False: {
            "generation_id": "gen-1",
            "status": "published",
        },
    )
    monkeypatch.setattr(
        publisher_workflow,
        "_apply_admission_feedback",
        lambda *, deployment, summary: (_ for _ in ()).throw(
            RuntimeError("dbos teardown race")
        ),
    )

    with mock_dbos_context():
        publisher_workflow.register_publisher_workflow(deployment="test")
        summary = publisher_workflow.publisher_cycle_workflow()()

    assert summary["status"] == "published"
    assert summary["admission_feedback"]["updated"] == 0
    assert summary["admission_feedback"]["skipped"] == (
        "admission_feedback_failed"
    )
    assert "dbos teardown race" in summary["admission_feedback"]["error"]


def test_the_outcome_is_recorded_after_the_admission_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The outcome's time is the publisher's freshness clock, so it is the end
    of the whole cycle, feedback included (publisher-starvation-4039)."""
    from types import SimpleNamespace

    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.runtime import publisher_workflow
    from okg.substrate.runtime import workflow_operational_outcomes as outcomes

    order: list[str] = []
    monkeypatch.setattr(
        outcomes, "define_operational_transactions",
        lambda *, participant: SimpleNamespace(
            register_execution_tx=lambda: order.append("register"),
            record_outcome_tx=lambda _o: order.append("outcome"),
        ),
    )
    monkeypatch.setattr(
        publisher_workflow, "_run_one_cycle",
        lambda *, deployment, branch_name="default", force_enrichers=False: (
            order.append("cycle") or {"status": "published", "generation_id": "g"}
        ),
    )
    monkeypatch.setattr(
        publisher_workflow, "_apply_admission_feedback_safely",
        lambda *, deployment, summary: order.append("feedback") or {"updated": 0},
    )

    with mock_dbos_context():
        publisher_workflow.register_publisher_workflow(deployment="test")
        publisher_workflow.publisher_cycle_workflow()()

    assert order == ["register", "cycle", "feedback", "outcome"]


def test_publisher_workflow_forwards_force_enrichers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P6: the runtime-enabled DBOS workflow preserves operator force."""
    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.runtime import publisher_workflow

    seen: dict[str, object] = {}

    def fake_run_one_cycle(*, deployment, branch_name, force_enrichers):
        seen.update(
            deployment=deployment,
            branch_name=branch_name,
            force_enrichers=force_enrichers,
        )
        return {"generation_id": "gen-1", "status": "published"}

    monkeypatch.setattr(
        publisher_workflow,
        "_run_one_cycle",
        fake_run_one_cycle,
    )
    monkeypatch.setattr(
        publisher_workflow,
        "_apply_admission_feedback_safely",
        lambda *, deployment, summary: {"updated": 0},
    )

    with mock_dbos_context():
        publisher_workflow.register_publisher_workflow(deployment="test")
        publisher_workflow.publisher_cycle_workflow()(
            "feature",
            True,
        )

    assert seen == {
        "deployment": "test",
        "branch_name": "feature",
        "force_enrichers": True,
    }


def test_publisher_enqueue_serializes_force_enrichers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P6: the enqueued payload carries the one-shot force bit."""
    from types import SimpleNamespace

    from okg.substrate.runtime import admin
    from okg.substrate.runtime.dbos_enqueue import DBOSWorkflowEnqueueResult

    captured: dict[str, object] = {}

    class FakeClient:
        def list_workflows(self, *, workflow_ids):
            assert workflow_ids == ["wf-force"]
            return [
                SimpleNamespace(
                    status="SUCCESS",
                    output={"status": "published"},
                    error=None,
                ),
            ]

    def fake_enqueue(*, args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return DBOSWorkflowEnqueueResult(workflow_id="wf-force")

    monkeypatch.setattr(admin, "_build_client", lambda **_kwargs: FakeClient())
    monkeypatch.setattr(
        admin, "registered_app_version", lambda _dsn: "app-version-1",
    )
    monkeypatch.setattr(admin, "enqueue_deployment_workflow", fake_enqueue)

    result = admin.client_enqueue_publisher_cycle(
        deployment="test",
        dsn="postgres://unused",
        branch_name="feature",
        force_enrichers=True,
        poll_interval_seconds=0,
    )

    assert result.status == "SUCCESS"
    assert captured["args"] == ("feature", True)


def test_publisher_cycle_yields_to_active_source_sync_at_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A queued publisher must not grab broad publish work over source-sync."""
    from okg.substrate.runtime import publisher_workflow
    import okg.substrate.deployments as deployments
    import okg.substrate.publisher.deployment_stack as stack_mod

    monkeypatch.setattr(
        deployments,
        "resolve_deployment",
        lambda _deployment: object(),
    )
    monkeypatch.setattr(
        deployments,
        "require_postgres_dsn",
        lambda _manifest, *, deployment: "postgres://stub",
    )
    monkeypatch.setattr(
        publisher_workflow,
        "_active_source_sync_workflow_id_for_publisher",
        lambda *, deployment, dsn: "wf-source-sync-active",
    )
    monkeypatch.setattr(
        stack_mod,
        "build_deployment_publisher_stack",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("publisher stack should not be built")
        ),
    )

    summary = publisher_workflow._run_one_cycle(
        deployment="test",
        branch_name="default",
    )

    assert summary == {
        "deployment": "test",
        "branch_name": "default",
        "status": "skipped",
        "publish_skipped": True,
        "skipped": "source_sync_active",
        "blocking_workflow_id": "wf-source-sync-active",
    }
    assert publisher_workflow._apply_admission_feedback(
        deployment="test",
        summary=summary,
    ) == {
        "deployment": "test",
        "status": "skipped",
        "updated": 0,
        "skipped": "no_generation_feedback_needed",
    }


def test_publisher_cycle_yields_if_source_sync_appears_before_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The workflow converts the lock-level priority yield into a skip."""
    from contextlib import contextmanager

    from okg.substrate.errors import PublisherLeaseHeld
    from okg.substrate.runtime import publisher_workflow
    import okg.substrate.deployments as deployments
    import okg.substrate.publisher.deployment_stack as stack_mod
    import okg.substrate.runtime.profiling_bridge as profiling_bridge

    class _Stack:
        deployment_name = "test"
        deployment_dir = tmp_path

        def publisher_kwargs(self):
            return {}

    class _Publisher:
        def __init__(self, *args, **kwargs):
            self.kwargs = kwargs

        def cycle(self, *, force_enrichers=False):
            assert force_enrichers is False
            raise PublisherLeaseHeld(
                "source-sync active",
                data={
                    "reason": "source_sync_active",
                    "blocking_workflow_id": "wf-source-sync-race",
                },
            )

    @contextmanager
    def _profile_context(**_kwargs):
        class _Profile:
            generation_id = None

        yield _Profile()

    monkeypatch.setattr(
        deployments,
        "resolve_deployment",
        lambda _deployment: object(),
    )
    monkeypatch.setattr(
        deployments,
        "require_postgres_dsn",
        lambda _manifest, *, deployment: "postgres://stub",
    )
    monkeypatch.setattr(
        publisher_workflow,
        "_active_source_sync_workflow_id_for_publisher",
        lambda *, deployment, dsn: None,
    )
    monkeypatch.setattr(
        stack_mod,
        "build_deployment_publisher_stack",
        lambda **_kwargs: _Stack(),
    )
    monkeypatch.setattr(
        stack_mod,
        "publisher_kwargs_from_env",
        lambda: {},
    )
    monkeypatch.setattr(
        stack_mod,
        "build_runtime_catalog_identity",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.repository_provenance.tracking_repository_provenance",
        lambda _path: RepositoryProvenance.exact(
            core_revision="1" * 40,
            deployment_revision="2" * 40,
            deployment_tree_sha="3" * 40,
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.publisher.Publisher",
        _Publisher,
    )
    monkeypatch.setattr(
        profiling_bridge,
        "workflow_profiling_context",
        _profile_context,
    )

    summary = publisher_workflow._run_one_cycle(
        deployment="test",
        branch_name="default",
    )

    assert summary["status"] == "skipped"
    assert summary["publish_skipped"] is True
    assert summary["skipped"] == "source_sync_active"
    assert summary["blocking_workflow_id"] == "wf-source-sync-race"


def test_publisher_lock_releases_if_source_sync_appears_after_acquire():
    """The advisory-lock guard releases if source-sync wins after try-lock."""
    from okg.substrate.errors import PublisherLeaseHeld
    from okg.substrate.publisher.publisher import Publisher

    probes = iter([None, "wf-source-sync-after-lock"])
    queries = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, query, params=None):
            queries.append(str(query))
            self._row = (True,)

        def fetchone(self):
            return self._row

    class _Conn:
        def cursor(self):
            return _Cursor()

    pub = Publisher(
        dsn="postgres://unused",
        deployment_name="test-source-sync-priority",
        coordination_mode="dbos",
        source_sync_priority_probe=lambda: next(probes),
    )

    with pytest.raises(PublisherLeaseHeld) as exc:
        pub._acquire_dbos_publish_lock(_Conn())

    assert exc.value.data["reason"] == "source_sync_active"
    assert (
        exc.value.data["blocking_workflow_id"]
        == "wf-source-sync-after-lock"
    )
    assert any("pg_advisory_unlock" in query for query in queries)


def test_publisher_lock_yields_to_source_sync_admission_intent(monkeypatch):
    """A pending source-sync enqueue is enough to make publisher yield."""
    from okg.substrate.errors import PublisherLeaseHeld
    from okg.substrate.publisher.publisher import Publisher

    monkeypatch.setattr(
        "okg.substrate.publisher.publisher."
        "source_sync_admission_intent_workflow_id_on_connection",
        lambda **_kwargs: "source-sync-admission-intent",
    )

    class _Conn:
        def cursor(self):
            raise AssertionError("publisher should yield before try-lock")

    pub = Publisher(
        dsn="postgres://unused",
        deployment_name="test-source-sync-intent",
        coordination_mode="dbos",
    )

    with pytest.raises(PublisherLeaseHeld) as exc:
        pub._acquire_dbos_publish_lock(_Conn())

    assert exc.value.data["reason"] == "source_sync_active"
    assert (
        exc.value.data["blocking_workflow_id"]
        == "source-sync-admission-intent"
    )


def test_publisher_execution_yields_to_source_sync_admission_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Workflow-start probe checks source-sync admission intent before build."""
    from okg.substrate.runtime import publisher_workflow
    import okg.substrate.deployments as deployments
    import okg.substrate.publisher.deployment_stack as stack_mod

    monkeypatch.setattr(
        deployments,
        "resolve_deployment",
        lambda _deployment: object(),
    )
    monkeypatch.setattr(
        deployments,
        "require_postgres_dsn",
        lambda _manifest, *, deployment: "postgres://stub",
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.source_sync_priority."
        "source_sync_admission_intent_workflow_id",
        lambda **_kwargs: "source-sync-admission-intent",
    )
    monkeypatch.setattr(
        stack_mod,
        "build_deployment_publisher_stack",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("publisher stack should not be built")
        ),
    )

    summary = publisher_workflow._run_one_cycle(
        deployment="test",
        branch_name="default",
    )

    assert summary["status"] == "skipped"
    assert summary["publish_skipped"] is True
    assert summary["skipped"] == "source_sync_active"
    assert summary["blocking_workflow_id"] == "source-sync-admission-intent"


# ---------------------------------------------------------------------------
# Schedule registration
# ---------------------------------------------------------------------------


def test_register_publisher_schedule_caches_handle():
    """The scheduled enqueue must be decorated and cached so
    apply_publisher_schedule can pass it to DBOS.apply_schedules."""
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.publisher_schedule import (
            PUBLISHER_SCHEDULE_ID,
            _DECORATED,
            register_publisher_schedule,
        )

        register_publisher_schedule(
            deployment="test",
            dsn="postgres://stub",
        )
        assert PUBLISHER_SCHEDULE_ID in _DECORATED
        assert callable(_DECORATED[PUBLISHER_SCHEDULE_ID])


def test_apply_publisher_schedule_requires_register_first():
    """apply_publisher_schedule must error loudly if register hasn't
    run, rather than silently registering with the default cron."""
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.publisher_schedule import (
            _DECORATED,
            apply_publisher_schedule,
        )
        _DECORATED.clear()

        with pytest.raises(RuntimeError, match="before"):
            apply_publisher_schedule(schedule_overrides={})


# ---------------------------------------------------------------------------
# Scheduled enqueue surfaces enqueue calls on the mock DBOSClient
# ---------------------------------------------------------------------------


def test_scheduled_enqueue_invokes_dbos_client(monkeypatch: pytest.MonkeyPatch):
    """When the scheduled workflow fires, it must call
    DBOSClient.start_workflow_in_queue with the deployment's
    partition queue name + the publisher workflow name."""
    from contextlib import contextmanager
    from datetime import datetime, timezone

    from okg.deployment.testing import mock_dbos_context

    @contextmanager
    def permit_fake_enqueue(**_kwargs):
        yield

    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.promotion_enqueue_permit",
        permit_fake_enqueue,
    )

    with mock_dbos_context() as mock:
        from okg.substrate.runtime.publisher_schedule import (
            _DECORATED, register_publisher_schedule,
        )

        register_publisher_schedule(
            deployment="myapp", dsn="postgres://stub",
        )
        schedule_fn = _DECORATED[next(iter(_DECORATED))]

        # Drive the scheduled enqueue manually.
        result = schedule_fn(datetime.now(timezone.utc), None)

        # The mock DBOSClient should have recorded the enqueue.
        mock.assert_workflow_enqueued(
            "okg_publisher_cycle",
            queue_name="okg-myapp",
        )
        # The handle returned is the mock's pattern.
        assert result.startswith("mock-wf-")


def test_scheduled_enqueue_skips_when_source_sync_active(monkeypatch):
    """Publisher cron must not contend with an active source-sync publish."""
    from datetime import datetime, timezone

    from okg.deployment.testing import mock_dbos_context

    calls = []

    def fake_active_workflow_id_for_name(**kwargs):
        calls.append(kwargs)
        if (
            kwargs["queue_name"] == "okg-myapp-source-sync"
            and kwargs["workflow_name"] == "okg_source_sync"
        ):
            return "source-sync-active"
        return None

    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.active_workflow_id_for_name",
        fake_active_workflow_id_for_name,
    )

    with mock_dbos_context() as mock:
        from okg.substrate.runtime import publisher_schedule
        from okg.substrate.runtime.publisher_schedule import (
            _DECORATED, PublisherFreshness, register_publisher_schedule,
        )

        # The stub DSN has no database. A publisher that published a minute
        # ago is not overdue, so the tick yields as before; the overdue
        # counterpart is in test_publisher_cycle_overdue.py.
        now = datetime.now(timezone.utc)
        monkeypatch.setattr(
            publisher_schedule,
            "read_publisher_freshness",
            lambda **_kw: PublisherFreshness(
                deployment="myapp", branch_name="default", bound_seconds=600.0,
                last_cycle_at=now, last_generation_at=now, checked_at=now,
            ),
        )
        monkeypatch.setattr(
            publisher_schedule,
            "source_sync_workflow_row",
            lambda **_kw: ("PENDING", 5.0),
        )
        register_publisher_schedule(
            deployment="myapp", dsn="postgres://stub",
        )
        schedule_fn = _DECORATED[next(iter(_DECORATED))]

        result = schedule_fn(datetime.now(timezone.utc), None)

        assert result == "source-sync-active"
        assert mock.enqueued == []

    assert calls[0]["queue_name"] == "okg-myapp"
    assert calls[0]["workflow_name"] == "okg_publisher_cycle"
    assert calls[1]["queue_name"] == "okg-myapp-source-sync"
    assert calls[1]["workflow_name"] == "okg_source_sync"


@pytest.mark.postgres
def test_publisher_workflow_applies_blocked_admission_feedback(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The DBOS publisher workflow feedback step marks blocked batches."""
    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.admission import (
        attach_fact_range_refs_for_run,
        ensure_envelope,
        transition_envelope,
    )
    from okg.substrate.runtime import publisher_workflow

    run_id = str(uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES
                  ('blocked:node', 'document', '{}'::jsonb,
                   'blocked_source', '{}'::jsonb, '{}'::jsonb,
                   now(), %s, %s::uuid, 'I')
                """,
                (f"blocked_source:{run_id}", run_id),
            )
            generation_id = _insert_generation(
                cur,
                deployment_name="blocked-dep",
                status="blocked",
                blocked_reason={"gate": "fixture_gate"},
            )
        with conn.transaction():
            envelope = ensure_envelope(
                conn,
                deployment_name="blocked-dep",
                producer_id="blocked_source",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="pending",
                idempotency_key="blocked-key",
                source="blocked_source",
                run_id=run_id,
                check_summary={"admission_policy": {"declared": True}},
            )
            attach_fact_range_refs_for_run(
                conn,
                admission_id=envelope.admission_id,
                source="blocked_source",
                run_id=run_id,
            )
            transition_envelope(
                conn,
                admission_id=envelope.admission_id,
                status="accepted",
            )

    import okg.substrate.deployments as deployments

    monkeypatch.setattr(deployments, "resolve_deployment", lambda _d: object())
    monkeypatch.setattr(
        deployments,
        "require_postgres_dsn",
        lambda _manifest, *, deployment: clean_runtime,
    )
    monkeypatch.setattr(
        publisher_workflow,
        "_run_one_cycle",
            lambda *, deployment, branch_name="default", force_enrichers=False: {
            "generation_id": generation_id,
            "status": "blocked",
        },
    )

    with mock_dbos_context():
        publisher_workflow.register_publisher_workflow(
            deployment="blocked-dep",
        )
        summary = publisher_workflow.publisher_cycle_workflow()()

    assert summary["admission_feedback"]["updated"] == 1
    assert summary["admission_feedback"]["status"] == "blocked"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, target_generation_ref, verdict, workflow_ref
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (envelope.admission_id,),
        )
        status, target_generation_ref, verdict, workflow_ref = cur.fetchone()
        cur.execute(
            """
            SELECT count(*)
              FROM okg.admission_events
             WHERE admission_id = %s::uuid
               AND event_type = 'blocked'
            """,
            (envelope.admission_id,),
        )
        blocked_events = int(cur.fetchone()[0])

    assert status == "blocked"
    assert target_generation_ref == str(generation_id)
    assert verdict["blocked_reason"]["gate"] == "fixture_gate"
    assert workflow_ref["workflow_name"] == "okg_publisher_cycle"
    assert workflow_ref["workflow_id"] == "mock-wf-1"
    assert blocked_events == 1


def _insert_generation(
    cur,
    *,
    deployment_name: str,
    status: str,
    blocked_reason: dict | None = None,
) -> int:
    cur.execute(
        """
        INSERT INTO okg.graph_generations
          (deployment_name, status, fact_watermark, edge_fact_watermark,
           catalog_version_id, published_at, blocked_reason,
           correlation_id)
        VALUES
          (%s, %s, 0, 0,
           (SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'),
           now(), %s::jsonb, %s)
        RETURNING generation_id
        """,
        (
            deployment_name,
            status,
            Json(blocked_reason) if blocked_reason is not None else None,
            f"publisher-workflow-test-{uuid4()}",
        ),
    )
    return int(cur.fetchone()[0])
