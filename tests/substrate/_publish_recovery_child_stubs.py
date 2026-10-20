"""Installers the compensation tests run inside real source children.

``tests/substrate/test_publish_recovery.py`` drives a source to a finalizer
rejection and checks what compensation leaves behind. The source runs in a
``spawn`` child, so the stubs and spies it needs are installed there by these
functions (see ``tests/pytest/source_child_patches.py``) and report through
the record file.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from tests.pytest.source_child_patches import record


class _FinalizerDeny:
    effect = "deny"
    reason_code = "nomos.test.finalizer_denied"

    def to_dict(self) -> dict[str, str]:
        return {"effect": self.effect, "reason_code": self.reason_code}


def finalizer_denial(rejection: str = "deny") -> None:
    """The finalizer's Nomos boundary denies the run, or raises."""
    import okg.substrate.nomos as nomos

    def _stub(*_args: Any, **_kwargs: Any) -> Any:
        if rejection == "boundary_error":
            raise RuntimeError("nomos boundary unavailable")
        return SimpleNamespace(allowed=False, decision=_FinalizerDeny())

    nomos.authorize_source_run_outcome_with_active_policy = _stub


def _fail_the_rejection_write() -> None:
    import okg.substrate.ingest.service as ingest_service

    real = ingest_service.transition_envelope

    def _fail(conn: Any, **kwargs: Any) -> Any:
        if kwargs.get("status") == "rejected":
            record("forced_failure", status=str(kwargs.get("status")))
            raise RuntimeError("forced finalizer failure after compensation")
        return real(conn, **kwargs)

    ingest_service.transition_envelope = _fail


def rollback_after_compensation() -> None:
    """#2152: deny, compensate in the finalizer, then roll it back.

    The outer handler's second compensation (no fence) is suppressed so the
    test reads what the in-transaction compensation left.
    """
    import okg.substrate.ingest.service as ingest_service

    finalizer_denial("deny")
    real_retract = ingest_service._retract_chronos_run

    def _record_retract(retract_dsn: str, **kwargs: Any) -> None:
        held = kwargs.get("graph_conn") is not None
        record(
            "retract",
            held=held,
            **{k: str(v) for k, v in kwargs.items() if k != "graph_conn"},
        )
        if not held:
            return None
        real_retract(retract_dsn, **kwargs)

    ingest_service._retract_chronos_run = _record_retract
    _fail_the_rejection_write()


def _spy_real_run_outcome() -> None:
    import okg.substrate.nomos as nomos

    real = nomos.authorize_source_run_outcome_with_active_policy

    def _spy(*args: Any, **kwargs: Any) -> Any:
        outcome = real(*args, **kwargs)
        record(
            "outcome",
            allowed=outcome is not None and bool(outcome.allowed),
            reason=None if outcome is None else outcome.decision.reason_code,
        )
        return outcome

    nomos.authorize_source_run_outcome_with_active_policy = _spy


def real_denial_rollback() -> None:
    """#2244 case 2: observe the fenced retraction, then force a rollback."""
    import okg.substrate.ingest.service as ingest_service
    from okg.substrate.graph_store import ChronosGraphStore
    from tests.substrate.test_publish_recovery import (
        _graph_counts_on,
        _receipt_rows,
        _receipt_rows_on,
        _run_graph_counts,
    )

    _spy_real_run_outcome()
    real_retract = ingest_service._retract_chronos_run

    def _observe(retract_dsn: str, **kwargs: Any) -> None:
        if kwargs.get("graph_conn") is None:
            return None
        call = {k: str(v) for k, v in kwargs.items() if k != "graph_conn"}
        call["rows_before"] = list(_run_graph_counts(
            retract_dsn, source=call["source"], run_id=call["run_id"],
        ))
        call["receipts_before"] = [list(r) for r in _receipt_rows(retract_dsn)]
        call["retract_returned"] = False
        try:
            real_retract(retract_dsn, **kwargs)
            call["retract_returned"] = True
            graph_conn = kwargs["graph_conn"]
            call["rows_in_txn"] = list(_graph_counts_on(
                ChronosGraphStore.from_connection(
                    graph_conn, ensure_schema=False, autocommit=False,
                ),
                source=call["source"],
                run_id=call["run_id"],
            ))
            call["receipts_in_txn"] = [list(r) for r in _receipt_rows_on(graph_conn)]
        finally:
            record("fenced", **call)

    ingest_service._retract_chronos_run = _observe
    _fail_the_rejection_write()


def real_denial_suppressed() -> None:
    """#2244 case 1 setup: a real denial whose compensations are suppressed."""
    import okg.substrate.ingest.service as ingest_service

    _spy_real_run_outcome()

    def _suppress(_dsn: str, **kwargs: Any) -> None:
        record(
            "suppressed",
            **{k: str(v) for k, v in kwargs.items() if k != "graph_conn"},
        )

    ingest_service._retract_chronos_run = _suppress
