from __future__ import annotations

import pytest

from okg.substrate.runtime_dlq import (
    _MAX_FAILURE_ROWS,
    _MAX_SUPERSESSION_ROWS,
    RuntimeDLQAuthorityUnavailable,
    _digest,
    _responsibility,
    _workflow_row,
    current_runtime_dlq,
    failure_snapshot_digest,
)
from okg.substrate.source_sync import SourceSyncJob
from okg.substrate.source_sync_enqueue import (
    _serialize_source_sync_dbos_args,
    _source_sync_workflow_args,
)


class _Cursor:
    def __init__(
        self,
        rows,
        current_version="v2",
        workflow_columns=None,
        acknowledgement_rows=None,
        acknowledgement_table_exists=False,
    ):
        self.rows = rows
        self.current_version = current_version
        self.workflow_columns = workflow_columns or (
            "workflow_uuid",
            "name",
            "status",
            "application_version",
            "queue_name",
            "inputs",
            "serialization",
            "error",
            "output",
            "updated_at",
        )
        self.acknowledgement_rows = acknowledgement_rows or []
        self.acknowledgement_table_exists = acknowledgement_table_exists
        self.result = []
        self.queries: list[str] = []
        self.fetched_ids: list[str] | None = None

    def execute(self, query, params=None):
        self.queries.append(str(query))
        if "to_regclass('dbos.workflow_status')" in query:
            self.result = [("dbos.workflow_status",)]
        elif "table_name = 'workflow_status'" in query:
            self.result = [
                (column,)
                for column in self.workflow_columns
            ]
        elif "FROM dbos.workflow_status" in query:
            if "status = 'SUCCESS'" in query:
                self.result = [row for row in self.rows if row[2] == "SUCCESS"]
                return
            failures = [row for row in self.rows if row[2] in {
                "ERROR",
                "FAILED",
                "MAX_RECOVERY_ATTEMPTS",
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
            }]
            if "GROUP BY" in query:
                # The census: a bounded GROUP BY over (name, version).
                bounded = failures[:_MAX_FAILURE_ROWS + 1]
                groups: dict[tuple, int] = {}
                for row in bounded:
                    key = (row[1], self._version(row))
                    groups[key] = groups.get(key, 0) + 1
                self.result = [(*key, count) for key, count in groups.items()]
                return
            # The fetch: only the rows whose content reaches the result.
            excluded_names = set(params[0]) if params else set()
            historical = set(params[1]) if params and len(params) > 1 else set()
            self.fetched_ids = [
                row[0] for row in failures
                if (row[1] or "") not in excluded_names
                and (self._version(row) is None or self._version(row) not in historical)
            ]
            self.result = [row for row in failures if row[0] in set(self.fetched_ids)]
        elif "to_regclass('dbos.application_versions')" in query:
            self.result = [("dbos.application_versions",)]
        elif "table_name = 'application_versions'" in query:
            self.result = [
                ("version_name",),
                ("version_timestamp",),
                ("created_at",),
            ]
        elif "FROM dbos.application_versions" in query:
            self.result = [(self.current_version,)]
        elif "to_regclass('okg.runtime_dlq_acknowledgement_events')" in query:
            self.result = [
                (
                    "okg.runtime_dlq_acknowledgement_events"
                    if self.acknowledgement_table_exists
                    else None,
                )
            ]
        elif "FROM okg.runtime_dlq_acknowledgement_events" in query:
            self.result = self.acknowledgement_rows
        else:
            raise AssertionError(query)

    def _version(self, row):
        # A table without the column reads NULL for it, as the SQL does.
        if "application_version" not in self.workflow_columns:
            return None
        return row[3]

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return list(self.result)


def _row(workflow_id, status, *, version="v2", source="git", run_mode="scope_complete", supersedes=None, scope=None):
    args = [
        [source], run_mode, True, "closure",
        ([{"scope_key": scope}] if scope else []), True, True, False, [], supersedes,
    ]
    return (
        workflow_id,
        "okg_source_sync",
        status,
        version,
        "okg-okg-ci-source-sync",
        args,
        None,
        "boom",
        None,
        100,
    )


def _serialized_row(
    workflow_id: str,
    status: str,
    *,
    supersedes: str | None = None,
) -> tuple:
    job = SourceSyncJob(
        deployment="okg-ci",
        sources=("git",),
        run_mode="scope_complete",
        supersedes_workflow_id=supersedes,
    )
    inputs, serialization = _serialize_source_sync_dbos_args(
        args=_source_sync_workflow_args(job),
        serialization=None,
    )
    assert serialization == "py_pickle"
    return (
        workflow_id,
        "okg_source_sync",
        status,
        "v2",
        "okg-okg-ci-source-sync",
        inputs,
        serialization,
        "boom",
        None,
        100,
    )


def _acknowledgement(failure: tuple) -> tuple:
    view = _workflow_row(failure)
    responsibility = _responsibility(view)
    witness = {
        "kind": "source_sync",
        "replacement_workflow_id": "replacement-1",
        "responsibility": responsibility,
        "watermark": {
            "source": "git",
            "field": "last_reconcile_at",
            "observed_at": "2026-08-09T00:00:00+00:00",
        },
        "published_generation": {
            "generation_id": "generation-1",
            "published_at": "2026-08-09T00:00:00+00:00",
        },
        "live_convergence": {
            "schema": "okg.runtime_dlq.live_source_convergence.v1",
            "state": "ready",
            "authority": [
                "dbos.workflow_status.output",
                "okg.source_progress_or_watermark",
                "okg.graph_generations",
            ],
            "workflow_id": "replacement-1",
            "watermark_digest": _digest({
                "source": "git",
                "field": "last_reconcile_at",
                "observed_at": "2026-08-09T00:00:00+00:00",
            }),
            "generation_id": "generation-1",
        },
        "request_readiness_digest": _digest({"ok": True}),
    }
    operation_receipt_digest = _digest({"receipt": "production-fence"})
    entry = {
        "workflow_id": view["workflow_id"],
        "workflow_name": view["name"],
        "application_version": view["application_version"],
        "failure_snapshot_digest": failure_snapshot_digest(view),
        "error_digest": _digest(view["error"]),
        "responsibility": responsibility,
        "witness": witness,
    }
    plan = {
        "schema": "okg.runtime_dlq_acknowledgement_plan.v1",
        "deployment": "okg-ci",
        "actor": "operator@example.com",
        "reason": "recovery verified",
        "candidate_revision": "candidate-sha",
        "operation_receipt_digest": operation_receipt_digest,
        "runtime_fence": {
            "promotion_id": "promotion-1",
            "status": "fenced",
            "candidate_revision": "candidate-sha",
        },
        "current_application_version": view["application_version"],
        "entries": [entry],
    }
    return (
        "okg-ci",
        view["workflow_id"],
        view["name"],
        view["application_version"],
        failure_snapshot_digest(view),
        _digest(view["error"]),
        responsibility,
        "operator@example.com",
        "recovery verified",
        "candidate-sha",
        operation_receipt_digest,
        witness,
        plan,
        _digest(plan),
        "2026-08-09T00:00:00+00:00",
    )


def test_current_dlq_requires_explicit_matching_supersession() -> None:
    failure = _row("failed-1", "ERROR")
    unrelated = _row("success-unrelated", "SUCCESS", source="pact", supersedes="failed-1")
    implicit = _row("success-implicit", "SUCCESS")
    assert current_runtime_dlq(_Cursor([failure, unrelated, implicit]))["current_count"] == 1

    matching = _row("success-matching", "SUCCESS", supersedes="failed-1")
    result = current_runtime_dlq(_Cursor([failure, unrelated, implicit, matching]))
    assert result["current_count"] == 0
    assert result["historical_count"] == 1


def test_current_dlq_reads_success_payloads_without_success_outputs() -> None:
    failure = _row("failed-1", "ERROR")
    matching = _row("success-matching", "SUCCESS", supersedes="failed-1")
    cur = _Cursor([failure, matching])

    result = current_runtime_dlq(cur)

    assert result["current_count"] == 0
    success_query = next(
        query for query in cur.queries
        if "status = 'SUCCESS'" in query
    )
    assert "NULL::text AS error" in success_query
    assert "NULL::text AS output" in success_query
    assert f"LIMIT {_MAX_SUPERSESSION_ROWS + 1}" in success_query


def test_current_dlq_bounds_terminal_failure_history() -> None:
    cur = _Cursor([_row("failed-1", "ERROR")])

    current_runtime_dlq(cur)

    failure_query = next(
        query for query in cur.queries
        if "FROM dbos.workflow_status" in query and "status IN" in query
    )
    assert f"LIMIT {_MAX_FAILURE_ROWS + 1}" in failure_query


def test_current_dlq_fails_loud_when_terminal_failure_bound_is_exceeded() -> None:
    rows = [_row(f"failed-{index}", "ERROR") for index in range(_MAX_FAILURE_ROWS + 1)]

    with pytest.raises(
        RuntimeDLQAuthorityUnavailable,
        match="terminal failure history exceeds",
    ):
        current_runtime_dlq(_Cursor(rows))


def test_current_dlq_skips_success_history_when_no_source_failure_is_current() -> None:
    cur = _Cursor([_row("failed-old", "ERROR", version="v1")])

    result = current_runtime_dlq(cur)

    assert result["current_count"] == 0
    assert not any("status = 'SUCCESS'" in query for query in cur.queries)


def test_current_dlq_keeps_scope_or_run_mode_mismatch_current() -> None:
    failure = _row("failed-1", "ERROR", scope="repo-a")
    wrong_scope = _row(
        "success-1", "SUCCESS", supersedes="failed-1", scope="repo-b"
    )
    wrong_mode = _row(
        "success-2", "SUCCESS", supersedes="failed-1", scope="repo-a", run_mode="cursor"
    )
    assert current_runtime_dlq(_Cursor([failure, wrong_scope, wrong_mode]))["current_count"] == 1


def test_current_dlq_treats_other_application_version_as_history() -> None:
    result = current_runtime_dlq(_Cursor([_row("failed-old", "ERROR", version="v1")]))
    assert result["current_count"] == 0
    assert result["historical_count"] == 1


def test_current_dlq_fails_closed_for_unknown_application_version() -> None:
    result = current_runtime_dlq(_Cursor([_row("failed-unknown", "ERROR", version=None)]))
    assert result["current_count"] == 1
    assert result["historical_count"] == 0


def test_current_dlq_fails_closed_for_blank_application_version() -> None:
    result = current_runtime_dlq(
        _Cursor([_row("failed-blank", "ERROR", version="  ")])
    )

    assert result["current_count"] == 1
    assert result["current_failures"][0]["application_version"] is None


def test_current_dlq_decodes_native_dbos_inputs_and_supersedes_id() -> None:
    failure = _serialized_row("failed-native", "ERROR")
    success = _serialized_row(
        "success-native",
        "SUCCESS",
        supersedes="failed-native",
    )

    result = current_runtime_dlq(_Cursor([failure, success]))

    assert result["current_count"] == 0
    assert result["historical_count"] == 1


def test_current_dlq_never_counts_cancelled_hygiene() -> None:
    result = current_runtime_dlq(_Cursor([_row("cancelled-1", "CANCELLED")]))

    assert result["current_count"] == 0
    assert result["historical_count"] == 0


def test_current_dlq_fails_closed_with_minimal_legacy_columns() -> None:
    failure = (
        "failed-minimal",
        "okg_publisher_cycle",
        "ERROR",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    result = current_runtime_dlq(
        _Cursor(
            [failure],
            workflow_columns=("workflow_uuid", "name", "status"),
        )
    )

    assert result["current_count"] == 1
    assert result["current_failures"][0]["workflow_id"] == "failed-minimal"


def test_current_dlq_excludes_workflows_delegated_to_other_health_gates() -> None:
    result = current_runtime_dlq(
        _Cursor([
            _row("source-sync-failure", "ERROR"),
            (
                "doctor-failure",
                "doctor_workflow",
                "ERROR",
                "v2",
                "_dbos_internal_queue",
                [],
                None,
                "doctor failed",
                None,
                100,
            ),
            (
                "generic-failure",
                "generic_workflow",
                "ERROR",
                "v2",
                "_dbos_internal_queue",
                [],
                None,
                "generic failed",
                None,
                100,
            ),
        ]),
        ignored_workflow_names={"okg_source_sync", "doctor_workflow"},
    )

    assert result["current_count"] == 1
    assert result["ignored_count"] == 2
    assert result["ignored_workflow_names"] == [
        "doctor_workflow",
        "okg_source_sync",
    ]
    assert result["current_failures"][0]["workflow_id"] == "generic-failure"


def test_current_dlq_fails_closed_when_acknowledgement_authority_is_absent() -> None:
    result = current_runtime_dlq(_Cursor([_row("failed-1", "ERROR")]))

    assert result["current_count"] == 1
    assert result["acknowledged_count"] == 0
    assert result["acknowledgement_authority"]["available"] is False


def test_current_dlq_separates_exact_append_only_acknowledgement() -> None:
    failure = _row("failed-1", "ERROR")
    result = current_runtime_dlq(
        _Cursor(
            [failure],
            acknowledgement_rows=[_acknowledgement(failure)],
            acknowledgement_table_exists=True,
        )
    )

    assert result["current_count"] == 0
    assert result["terminal_current_version_count"] == 1
    assert result["acknowledged_count"] == 1
    assert result["acknowledged_failures"][0]["workflow_id"] == "failed-1"
    assert result["acknowledgement_authority"]["available"] is True


def test_current_dlq_rejects_cross_deployment_acknowledgement() -> None:
    failure = _row("failed-1", "ERROR")
    acknowledgement = list(_acknowledgement(failure))
    acknowledgement[0] = "other-deployment"
    plan = dict(acknowledgement[12])
    plan["deployment"] = "other-deployment"
    acknowledgement[12] = plan
    acknowledgement[13] = _digest(plan)

    result = current_runtime_dlq(
        _Cursor(
            [failure],
            acknowledgement_rows=[tuple(acknowledgement)],
            acknowledgement_table_exists=True,
        )
    )

    assert result["current_count"] == 1
    assert result["acknowledged_count"] == 0
    assert result["invalid_acknowledgements"][0]["workflow_id"] == "failed-1"


def test_current_dlq_rejects_acknowledgement_after_failure_snapshot_drift() -> None:
    failure = _row("failed-1", "ERROR")
    acknowledgement = _acknowledgement(failure)
    changed = (*failure[:7], "different error", *failure[8:])

    result = current_runtime_dlq(
        _Cursor(
            [changed],
            acknowledgement_rows=[acknowledgement],
            acknowledgement_table_exists=True,
        )
    )

    assert result["current_count"] == 1
    assert result["acknowledged_count"] == 0
    assert result["invalid_acknowledgements"][0]["workflow_id"] == "failed-1"


def test_current_dlq_rejects_acknowledgement_after_raw_input_drift() -> None:
    failure = _row("failed-1", "ERROR")
    acknowledgement = _acknowledgement(failure)
    changed_args = list(failure[5])
    changed_args[2] = False
    changed = (*failure[:5], changed_args, *failure[6:])

    assert _responsibility(_workflow_row(changed)) == _responsibility(
        _workflow_row(failure)
    )
    result = current_runtime_dlq(
        _Cursor(
            [changed],
            acknowledgement_rows=[acknowledgement],
            acknowledgement_table_exists=True,
        )
    )

    assert result["current_count"] == 1
    assert result["acknowledged_count"] == 0


# --- issue #2202: count what is only counted; decode only what is served ----
#
# Full readiness ignores `okg_source_sync` (a dedicated authority checks it),
# yet the dead-letter check used to fetch every one of its failure rows with
# inputs, error and output and unpickle every input -- 12,605 decodes, 5.5 s of
# a 12.3 s check on okg-workspace -- only to add one to `ignored_count`. The
# same held for failures from another application version, which only feed
# `historical_count`. These tests count the decodes.


@pytest.fixture
def _decodes(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    from okg.substrate.source_sync_enqueue import (
        source_sync_payload_from_dbos_inputs as original,
    )

    seen: list[str] = []

    def counting(**kwargs):
        seen.append(str(kwargs.get("inputs"))[:16])
        return original(**kwargs)

    monkeypatch.setattr(
        "okg.substrate.runtime_dlq.source_sync_payload_from_dbos_inputs",
        counting,
    )
    return seen


def _versioned(row: tuple, version: str | None) -> tuple:
    return (*row[:3], version, *row[4:])


def _generic(workflow_id: str, *, version: str | None = "v2") -> tuple:
    return (
        workflow_id,
        "generic_workflow",
        "ERROR",
        version,
        "_dbos_internal_queue",
        [],
        None,
        "generic failed",
        None,
        100,
    )


def test_ignored_workflow_failures_are_counted_in_sql_not_decoded(
    _decodes: list[str],
) -> None:
    ignored_rows = [
        _serialized_row(f"source-{index}", "ERROR") for index in range(50)
    ]
    cur = _Cursor([*ignored_rows, _generic("generic-1")])

    result = current_runtime_dlq(cur, ignored_workflow_names={"okg_source_sync"})

    assert _decodes == []
    assert cur.fetched_ids == ["generic-1"]
    assert result["current_count"] == 1
    assert result["ignored_count"] == 50
    assert result["raw_terminal_count"] == 51
    assert result["terminal_current_version_count"] == 51
    assert result["historical_count"] == 0
    assert result["current_failures"][0]["workflow_id"] == "generic-1"


def test_readiness_probe_decodes_no_delegated_workflow_row(
    _decodes: list[str],
) -> None:
    """The one read readiness makes, with readiness's own ignored set."""
    from okg.substrate.runtime_dlq import probe_current_runtime_dlq
    from okg.substrate.runtime_queue_health import (
        DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES,
    )

    rows = [_serialized_row(f"source-{index}", "ERROR") for index in range(20)]
    result, unknown = probe_current_runtime_dlq(
        _Cursor(rows),
        ignored_workflow_names=set(
            DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES
        ),
    )

    assert unknown is None
    assert _decodes == []
    assert result["current_count"] == 0
    assert result["ignored_count"] == 20


def test_other_version_failures_are_counted_in_sql_not_decoded(
    _decodes: list[str],
) -> None:
    old = [
        _versioned(_serialized_row(f"old-{index}", "ERROR"), "v1")
        for index in range(10)
    ]
    current = _serialized_row("current-1", "ERROR")
    cur = _Cursor([*old, current])

    result = current_runtime_dlq(cur)

    # Only the current failure is decoded: its responsibility is served.
    assert len(_decodes) == 1
    assert cur.fetched_ids == ["current-1"]
    assert result["historical_count"] == 10
    assert result["raw_terminal_count"] == 11
    assert result["terminal_current_version_count"] == 1
    assert result["current_count"] == 1
    assert result["current_failures"][0]["responsibility"]["sources"] == ["git"]


def test_the_bound_still_counts_rows_it_no_longer_fetches() -> None:
    rows = [_row(f"failed-{index}", "ERROR") for index in range(_MAX_FAILURE_ROWS + 1)]

    with pytest.raises(
        RuntimeDLQAuthorityUnavailable,
        match="terminal failure history exceeds",
    ):
        current_runtime_dlq(
            _Cursor(rows), ignored_workflow_names={"okg_source_sync"}
        )


def test_every_figure_is_kept_for_a_mixed_history() -> None:
    """Ignored, historical, superseded, acknowledged and current failures in
    one history: every count and list the function returns."""
    acknowledged = _row("failed-acked", "ERROR")
    rows = [
        _row("failed-current", "ERROR"),
        _row("failed-superseded", "ERROR", source="docs"),
        _row("success-1", "SUCCESS", source="docs", supersedes="failed-superseded"),
        _row("failed-old", "ERROR", version="v1"),
        _versioned(_generic("generic-old"), "v1"),
        _generic("generic-current"),
        _generic("generic-null-version", version=None),
        (
            "doctor-failure", "doctor_workflow", "ERROR", "v2",
            "_dbos_internal_queue", [], None, "doctor failed", None, 100,
        ),
        (
            "doctor-old", "doctor_workflow", "ERROR", "v1",
            "_dbos_internal_queue", [], None, "doctor failed", None, 100,
        ),
        acknowledged,
        _row("cancelled-1", "CANCELLED"),
    ]
    cur = _Cursor(
        rows,
        acknowledgement_rows=[_acknowledgement(acknowledged)],
        acknowledgement_table_exists=True,
    )

    result = current_runtime_dlq(cur, ignored_workflow_names={"doctor_workflow"})

    assert result["raw_terminal_count"] == 9
    assert result["historical_count"] == 4  # 3 other-version + 1 superseded
    assert result["superseded_count"] == 1
    assert result["ignored_count"] == 1
    assert result["terminal_current_version_count"] == 6
    assert result["acknowledged_count"] == 1
    assert result["acknowledged_failures"][0]["workflow_id"] == "failed-acked"
    assert result["invalid_acknowledgements"] == []
    assert [f["workflow_id"] for f in result["current_failures"]] == [
        "failed-current",
        "generic-current",
        "generic-null-version",
    ]
    assert result["current_count"] == 3
    assert result["ignored_workflow_names"] == ["doctor_workflow"]
    assert result["current_application_version"] == "v2"
