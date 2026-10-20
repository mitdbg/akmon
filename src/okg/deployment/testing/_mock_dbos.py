"""`mock_dbos` — narrow-scope test fixture for SDK unit tests.

Phase 0 C4 fixed the scope of this fixture explicitly:

  * **In scope**: SDK call-graph + SQL assertions. The fixture lets a
    test verify "this workflow calls emit_node_fact + emit_edge_fact +
    request_escalation in this order" and "the queued operator
    response from mock_dbos.queued_response is consumed".

  * **Out of scope**: durability. The fixture does NOT simulate
    workflow recovery, step caching, or partition queues. Tests that
    depend on those use :func:`okg.deployment.testing.dbos_test_runtime`
    (a real DBOS singleton).

The shim works by monkey-patching ``dbos.DBOS`` symbols with no-ops
and a small payload queue. Decorators (``deployment_workflow`` etc.)
that reach into ``dbos.DBOS.workflow()`` see the mock and produce
pass-through callables.
"""
from __future__ import annotations

from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator


__all__ = ["MockDBOS", "mock_dbos_context"]


@dataclass
class _RecvQueue:
    """Per-topic FIFO of queued responses."""

    payloads: deque = field(default_factory=deque)


class MockDBOS:
    """Test-time stand-in for the DBOS singleton.

    Records workflow enqueues + events so tests can assert on them;
    serves queued responses for ``DBOS.recv`` so ``request_escalation``
    returns the value the test programmed.
    """

    def __init__(self) -> None:
        # Workflows enqueued via DBOSClient.start_workflow_in_queue
        # during the test.
        self.enqueued: list[dict[str, Any]] = []
        # Events published via DBOS.set_event.
        self.events: dict[str, Any] = {}
        # Queued recv() responses per topic.
        self._recv_queues: dict[str, _RecvQueue] = defaultdict(_RecvQueue)
        # The current workflow_id the test is running as.
        self.workflow_id: str | None = "mock-wf-1"
        self.step_id: str | None = None

    # -------------------------------------------------------
    # SDK assertion helpers
    # -------------------------------------------------------

    def queued_response(self, topic: str, payload: Any) -> None:
        """Programmatically queue a recv() response.

        The next call to ``DBOS.recv(topic=topic, ...)`` returns
        ``payload``. Tests use this to drive ``request_escalation``
        without standing up an operator-response loop.
        """
        self._recv_queues[topic].payloads.append(payload)

    def assert_workflow_enqueued(
        self,
        name: str,
        *,
        queue_name: str | None = None,
        args: tuple | None = None,
    ) -> None:
        """Assert a workflow was enqueued during the test.

        Parameters
        ----------
        name : str
            Workflow name (passed to ``start_workflow_in_queue``).
        queue_name : str | None
            Optional: also assert the queue name.
        args : tuple | None
            Optional: also assert the args tuple.
        """
        for e in self.enqueued:
            if e["workflow_name"] != name:
                continue
            if queue_name is not None and e["queue_name"] != queue_name:
                continue
            if args is not None and tuple(e["args"]) != tuple(args):
                continue
            return
        raise AssertionError(
            f"workflow {name!r} not enqueued; enqueues: {self.enqueued}"
        )

    def cost_usd_consumed(self, scope: str) -> float:
        """Read total consumed USD for a scope from the live ledger.

        This goes through the actual Postgres connection the caller
        bound via :func:`bind_test_context`, so it's a real
        durability check — distinct from the mock's in-memory state.
        """
        from okg.deployment._internal.context import current_context

        ctx = current_context()
        with ctx.conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(sum(usd_consumed), 0) "
                "FROM okg.deployment_cost_ledger "
                "WHERE scope = %s",
                (scope,),
            )
            row = cur.fetchone()
        return float(row[0]) if row else 0.0

    # -------------------------------------------------------
    # DBOS-shaped API surface (used by SDK decorators / helpers)
    # -------------------------------------------------------

    def workflow(
        self, name: str | None = None,
        *,
        max_recovery_attempts: int = 50,
    ) -> Callable:
        def _decorator(func):
            wf_name = name or func.__qualname__
            func._okg_workflow_name = wf_name  # type: ignore[attr-defined]
            return func
        return _decorator

    def step(
        self, name: str | None = None,
        *,
        retries_allowed: bool = False,
        max_attempts: int = 3,
        interval_seconds: float = 1.0,
        backoff_rate: float = 2.0,
    ) -> Callable:
        def _decorator(func):
            return func
        return _decorator

    def transaction(
        self, name: str | None = None,
        *,
        isolation_level: str = "SERIALIZABLE",
    ) -> Callable:
        def _decorator(func):
            return func
        return _decorator

    def scheduled(self, cron: str) -> Callable:
        def _decorator(func):
            return func
        return _decorator

    def set_event(self, key: str, value: Any) -> None:
        self.events[key] = value

    def recv(
        self,
        *,
        topic: str,
        timeout_seconds: float | None = None,
    ) -> Any:
        """Pop the next queued payload for ``topic`` or return None
        (the documented "timeout fired" sentinel)."""
        queue = self._recv_queues[topic]
        if queue.payloads:
            return queue.payloads.popleft()
        return None


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


@contextmanager
def mock_dbos_context() -> Iterator[MockDBOS]:
    """Install a MockDBOS as the global ``dbos.DBOS`` for the duration
    of the with-block.

    The patch is best-effort: if the ``dbos`` package isn't installed,
    the context manager still yields a MockDBOS but no real DBOS
    methods are intercepted (the SDK uses lazy ``from dbos import
    DBOS`` so calls will simply not find the package). Tests that
    don't touch DBOS see the same MockDBOS regardless.
    """
    mock = MockDBOS()

    try:
        import dbos as dbos_module
    except ImportError:
        # No DBOS installed — yield the mock; SDK helpers that try to
        # import dbos will see ImportError and fall back to the
        # contextvar-driven paths.
        yield mock
        return

    saved = dbos_module.DBOS
    saved_client = getattr(dbos_module, "DBOSClient", None)

    # Patch DBOS to the mock.
    dbos_module.DBOS = mock

    class _MockDBOSClient:
        """Records enqueue calls on the active mock.

        Supports both the current DBOSClient.enqueue(options, *args)
        API and the legacy start_workflow_in_queue(queue_name=,
        workflow_name=, args=) shape for tests that haven't migrated.
        """

        def __init__(self, *_a, **_kw) -> None:
            pass

        def enqueue(self, options, *args, **_kwargs):
            mock.enqueued.append(
                {
                    "queue_name": options["queue_name"],
                    "workflow_name": options["workflow_name"],
                    "priority": options.get("priority"),
                    "args": tuple(args),
                }
            )

            class _Handle:
                workflow_id = (
                    f"mock-wf-{len(mock.enqueued)}"
                )

            return _Handle()

        def start_workflow_in_queue(
            self,
            *,
            queue_name: str,
            workflow_name: str,
            args: tuple = (),
        ):
            # Back-compat shim — real DBOSClient does not have this
            # method. Forwards to enqueue() so the mock matches both
            # call styles.
            return self.enqueue(
                {
                    "queue_name": queue_name,
                    "workflow_name": workflow_name,
                },
                *args,
            )

        def send(self, *_a, **_kw):
            return None

        def get_event(self, *, workflow_id, key, timeout_seconds=None):
            return mock.events.get(key)

    dbos_module.DBOSClient = _MockDBOSClient

    try:
        yield mock
    finally:
        dbos_module.DBOS = saved
        if saved_client is not None:
            dbos_module.DBOSClient = saved_client
