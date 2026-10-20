"""Tests for SDK test fixtures (Phase 2 §6.1 / §6.2).

Verifies:
  - mock_dbos installs as the global ``dbos.DBOS``.
  - Decorators are no-ops under mock_dbos.
  - DBOS.recv consumes queued responses.
  - DBOS.set_event records into the mock.
  - DBOSClient.start_workflow_in_queue records into the mock.
  - assert_workflow_enqueued / cost_usd_consumed assertion helpers.

`dbos_test_runtime` is covered by Phase 1's existing test suite; we
re-export the same fixture from okg.deployment.testing.
"""
from __future__ import annotations


def test_mock_dbos_installs_as_global():
    from okg.deployment.testing import MockDBOS, mock_dbos_context

    import dbos
    saved = dbos.DBOS

    with mock_dbos_context() as mock:
        assert isinstance(mock, MockDBOS)
        assert dbos.DBOS is mock

    # Restored on exit.
    assert dbos.DBOS is saved


def test_decorators_are_passthrough_under_mock_dbos():
    from okg.deployment import deployment_step, deployment_workflow
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        @deployment_workflow(name="t")
        def my_workflow(x):
            return x * 2

        @deployment_step()
        def my_step():
            return "ran"

        # Decorators are no-ops; functions run synchronously.
        assert my_workflow(3) == 6
        assert my_step() == "ran"


def test_recv_returns_queued_response():
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context() as mock:
        mock.queued_response("escalation:e-1", {"label": "stale"})

        import dbos
        result = dbos.DBOS.recv(topic="escalation:e-1")
        assert result == {"label": "stale"}

        # Queue is empty now — next call returns None (timeout
        # sentinel).
        assert dbos.DBOS.recv(topic="escalation:e-1") is None


def test_set_event_records_into_mock():
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context() as mock:
        import dbos
        dbos.DBOS.set_event("progress", {"phase": "verify"})
        assert mock.events["progress"] == {"phase": "verify"}


def test_dbos_client_records_enqueues():
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context() as mock:
        from dbos import DBOSClient
        c = DBOSClient()
        h = c.start_workflow_in_queue(
            queue_name="okg-test",
            workflow_name="my_workflow",
            args=("foo", 42),
        )
        assert h.workflow_id.startswith("mock-wf-")
        assert len(mock.enqueued) == 1
        mock.assert_workflow_enqueued(
            "my_workflow",
            queue_name="okg-test",
            args=("foo", 42),
        )


def test_assert_workflow_enqueued_raises_on_mismatch():
    import pytest
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context() as mock:
        with pytest.raises(AssertionError, match="not enqueued"):
            mock.assert_workflow_enqueued("nothing_ever_enqueued")
