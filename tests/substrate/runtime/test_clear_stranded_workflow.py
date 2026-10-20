"""Regressions for internal issues 1257 and 1254.

Both issues are the same observable failure with different causes: exactly one
row in ``dbos.workflow_status`` sits in a non-final state on a
``concurrency=1`` deployment queue forever, so publishing stops while every
read-side check stays green, and the operator has no scoped way to clear it.

  * #1257 — a publisher cycle that raised stayed ``PENDING`` and held the only
    slot. The automatic stale-PENDING sweep spares any workflow that already
    started, so nothing ever reaped it.
  * #1254 — a scheduled workflow left ``ENQUEUED`` by the previous application
    version blocked worker readiness. The refusal named the row and offered no
    action.

These tests use an in-process psycopg fake rather than a live DBOS schema, so
they carry no Postgres marker and run in the ordinary fast lane. The fake
refuses any statement it was not written for, so a change to the production
SQL surfaces here instead of passing silently, and
``test_deadline_sweep_reads_columns_dbos_actually_defines`` pins every column
name the sweep touches against the real DBOS schema definition, which is the
one thing a fake cannot vouch for.
"""
from __future__ import annotations

import argparse

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.runtime import workflow_cancel


DEPLOYMENT = "cooper"
PARTITION_QUEUE = f"okg-{DEPLOYMENT}"
SOURCE_SYNC_QUEUE = f"okg-{DEPLOYMENT}-source-sync"
DSN = "postgres://stub/stub"

_COLUMNS = (
    "name",
    "status",
    "queue_name",
    "application_version",
    "recovery_attempts",
    "started_at_epoch_ms",
    "workflow_deadline_epoch_ms",
    "deduplication_id",
    "updated_at",
    "completed_at",
)


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "name": "okg_publisher_cycle",
        "status": "PENDING",
        "queue_name": PARTITION_QUEUE,
        "application_version": "okg-runtime-v2",
        "recovery_attempts": 0,
        "started_at_epoch_ms": None,
        "workflow_deadline_epoch_ms": None,
        "deduplication_id": "publisher:cooper:cycle:1",
        "updated_at": 0,
        "completed_at": None,
    }
    unknown = set(overrides) - set(_COLUMNS)
    assert not unknown, f"unknown workflow_status column(s): {sorted(unknown)}"
    row.update(overrides)
    return row


class _FakeCursor:
    """Interprets exactly the statements ``workflow_cancel`` issues."""

    def __init__(self, store: dict[str, dict[str, object]], *, schema: bool):
        self.store = store
        self.schema = schema
        self._result: list[tuple] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, sql: str, params: tuple = ()) -> None:
        normalized = " ".join(sql.split()).upper()
        self._result = []
        if normalized.startswith("SELECT TO_REGCLASS"):
            self._result = [
                ("dbos.workflow_status",) if self.schema else (None,)
            ]
            return
        if normalized.startswith(
            "SELECT NAME, STATUS, QUEUE_NAME, APPLICATION_VERSION, "
            "RECOVERY_ATTEMPTS"
        ):
            row = self.store.get(params[0])
            if row is not None:
                self._result = [(
                    row["name"],
                    row["status"],
                    row["queue_name"],
                    row["application_version"],
                    row["recovery_attempts"],
                )]
            return
        if normalized.startswith("SELECT STATUS FROM DBOS.WORKFLOW_STATUS"):
            row = self.store.get(params[0])
            if row is not None:
                self._result = [(row["status"],)]
            return
        if normalized.startswith("SELECT WORKFLOW_UUID, NAME, QUEUE_NAME"):
            # The fake filters in Python below, so pin the predicates the
            # production statement must carry. Without this the "spares the
            # rest" assertions would pass because the fake narrows, not
            # because the SQL does.
            for predicate in (
                "STATUS = 'PENDING'",
                "QUEUE_NAME = ANY(%S)",
                "WORKFLOW_DEADLINE_EPOCH_MS IS NOT NULL",
            ):
                assert predicate in normalized, (
                    f"the deadline sweep no longer narrows on {predicate}: "
                    f"{sql}"
                )
            queues = set(params[0])
            self._result = [
                (
                    workflow_id,
                    row["name"],
                    row["queue_name"],
                    row["application_version"],
                    row["started_at_epoch_ms"],
                    row["workflow_deadline_epoch_ms"],
                )
                for workflow_id, row in self.store.items()
                if row["status"] == "PENDING"
                and row["queue_name"] in queues
                and row["workflow_deadline_epoch_ms"] is not None
            ]
            return
        if normalized.startswith("UPDATE DBOS.WORKFLOW_STATUS"):
            assert "DEDUPLICATION_ID = NULL" in normalized, (
                "a cancel that leaves deduplication_id set keeps the dedup "
                "bucket claimed by a workflow that is no longer running"
            )
            if normalized.endswith(
                "RETURNING WORKFLOW_UUID, NAME, QUEUE_NAME, STATUS"
            ):
                status, updated_at, completed_at, workflow_id, expected = params
                row = self.store.get(workflow_id)
                if row is None or row["status"] != expected:
                    return
                row["status"] = status
                row["updated_at"] = updated_at
                row["completed_at"] = completed_at
                row["deduplication_id"] = None
                self._result = [(
                    workflow_id, row["name"], row["queue_name"], status,
                )]
                return
            if normalized.endswith("RETURNING WORKFLOW_UUID"):
                status, updated_at, completed_at, workflow_id = params
                row = self.store.get(workflow_id)
                if row is None or row["status"] != "PENDING":
                    return
                row["status"] = status
                row["updated_at"] = updated_at
                row["completed_at"] = completed_at
                row["deduplication_id"] = None
                self._result = [(workflow_id,)]
                return
        raise AssertionError(f"fake psycopg saw an unexpected statement: {sql}")

    def fetchone(self) -> tuple | None:
        return self._result[0] if self._result else None

    def fetchall(self) -> list[tuple]:
        return list(self._result)


class _FakeConnection:
    def __init__(self, store: "_WorkflowStore", *, schema: bool):
        self.store = store
        self.schema = schema

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self.store, schema=self.schema)

    def commit(self) -> None:
        # Counted on the store, which outlives the connection, so a test can
        # assert the write was actually committed rather than only staged.
        self.store.commits += 1


class _WorkflowStore(dict):
    """The fake ``dbos.workflow_status`` table, plus a schema-present flag.

    ``schema_present`` and ``commits`` are class-level defaults only: writing
    either binds an instance attribute, and the fixture builds a fresh store
    per test, so nothing leaks between tests.
    """

    schema_present = True
    commits = 0


@pytest.fixture
def workflow_store(monkeypatch: pytest.MonkeyPatch) -> _WorkflowStore:
    store = _WorkflowStore()

    def _fake_connect(_dsn: str, **_kwargs: object) -> _FakeConnection:
        return _FakeConnection(store, schema=store.schema_present)

    # `workflow_cancel` reaches libpq through the substrate connection
    # factory, so `db_connect` is the seam it actually calls.
    monkeypatch.setattr(workflow_cancel, "db_connect", _fake_connect)
    return store


# ---------------------------------------------------------------------------
# issue #1257 — a raising cycle must not hold the slot forever
# ---------------------------------------------------------------------------


def test_started_pending_past_its_deadline_is_selected(
    record_property,
) -> None:
    """The bound reads the deadline DBOS recorded, so a started row qualifies.

    The existing stale-PENDING sweep skips any row with a start marker, which
    is exactly why the raising cycle in #1257 held the slot for as long as the
    operator let it.
    """
    record_property("issue", "1257")
    expired = workflow_cancel.deadline_expired_pending_workflows(
        [
            (
                "wf-raised",
                "okg_publisher_cycle",
                PARTITION_QUEUE,
                "okg-runtime-v2",
                1_000,          # started, so the stale sweep spares it
                2_000,          # deadline already passed
            ),
        ],
        now_ms=5_000,
    )

    assert [row["workflow_uuid"] for row in expired] == ["wf-raised"]
    assert expired[0]["overdue_seconds"] == pytest.approx(3.0)


@pytest.mark.parametrize(
    ("deadline_ms", "why"),
    [
        (None, "no deadline was ever recorded for this workflow"),
        (9_000, "the recorded deadline is still in the future"),
        (5_000, "the deadline is exactly now, so it has not passed"),
    ],
)
def test_pending_rows_without_an_expired_deadline_are_left_alone(
    deadline_ms: int | None, why: str, record_property,
) -> None:
    """The bound never guesses a deadline for a workflow that has none."""
    record_property("issue", "1257")
    expired = workflow_cancel.deadline_expired_pending_workflows(
        [
            {
                "workflow_uuid": "wf-live",
                "name": "okg_source_sync",
                "queue_name": SOURCE_SYNC_QUEUE,
                "application_version": "okg-runtime-v2",
                "started_at_epoch_ms": 1_000,
                "workflow_deadline_epoch_ms": deadline_ms,
            },
        ],
        now_ms=5_000,
    )

    assert expired == (), why


def test_deadline_sweep_cancels_the_wedging_cycle_and_spares_the_rest(
    workflow_store, record_property,
) -> None:
    record_property("issue", "1257")
    workflow_store["wf-raised"] = _row(
        started_at_epoch_ms=1_000, workflow_deadline_epoch_ms=2_000,
    )
    workflow_store["wf-running"] = _row(
        started_at_epoch_ms=4_900, workflow_deadline_epoch_ms=9_000,
    )
    workflow_store["wf-no-deadline"] = _row(started_at_epoch_ms=1_000)
    workflow_store["wf-other-deployment"] = _row(
        queue_name="okg-somewhere-else", workflow_deadline_epoch_ms=2_000,
    )
    workflow_store["wf-finished"] = _row(
        status="SUCCESS", workflow_deadline_epoch_ms=2_000,
    )

    report = workflow_cancel.cleanup_deadline_expired_pending_workflows(
        dsn=DSN, deployment=DEPLOYMENT, now_ms=5_000,
    )

    assert report["cancelled_count"] == 1
    assert [row["workflow_uuid"] for row in report["cancelled"]] == [
        "wf-raised",
    ]
    assert workflow_store["wf-raised"]["status"] == "CANCELLED"
    assert workflow_store["wf-running"]["status"] == "PENDING"
    assert workflow_store["wf-no-deadline"]["status"] == "PENDING"
    assert workflow_store["wf-other-deployment"]["status"] == "PENDING"
    assert workflow_store["wf-finished"]["status"] == "SUCCESS"


def test_deadline_sweep_runs_at_worker_start_and_in_maintenance(
    monkeypatch: pytest.MonkeyPatch, record_property,
) -> None:
    """The bound has to be wired in, not merely defined.

    Both automatic callers are asserted because a sweep that exists and never
    runs leaves #1257 exactly as reported.
    """
    record_property("issue", "1257")
    from okg.substrate.runtime import services, worker

    calls: list[str] = []

    def fake_sweep(*, dsn: str, deployment: str, **_kwargs: object) -> dict:
        assert dsn == DSN
        assert deployment == DEPLOYMENT
        calls.append("swept")
        return {"cancelled_count": 0, "cancelled": []}

    monkeypatch.setattr(
        workflow_cancel,
        "cleanup_deadline_expired_pending_workflows",
        fake_sweep,
    )
    monkeypatch.setattr(
        services, "cleanup_stale_pending_workflows",
        lambda **_kwargs: {"cancelled_count": 0},
    )
    monkeypatch.setattr(
        services, "cleanup_stale_enqueued_workflows",
        lambda **_kwargs: {"cancelled_count": 0},
    )

    worker._cleanup_stale_runtime_wakeups(
        dsn=DSN, deployment=DEPLOYMENT, phase="pre-launch",
    )
    assert calls == ["swept"], "worker start does not run the deadline sweep"

    report = services._run_maintenance_once(
        dsn=DSN, deployment=DEPLOYMENT, stale_after_seconds=300,
    )
    assert calls == ["swept", "swept"], (
        "worker maintenance does not run the deadline sweep"
    )
    assert "deadline_expired" in report


def test_publisher_cycles_actually_carry_a_deadline(record_property) -> None:
    """Without a workflow timeout the #1257 bound would never fire.

    DBOS fills ``workflow_deadline_epoch_ms`` on dequeue only when the row
    carries a ``workflow_timeout_ms``, which comes from the timeout the
    publisher schedule declares. Drop that timeout and the sweep still passes
    every test above while cancelling nothing in production, so pin it here.
    """
    record_property("issue", "1257")
    from okg.substrate.runtime import publisher_schedule

    assert publisher_schedule.PUBLISHER_WORKFLOW_TIMEOUT_SECONDS, (
        "the publisher schedule declares no workflow timeout, so DBOS records "
        "no deadline and the deadline sweep can never release a wedged slot"
    )

    # Read the module's own factory call rather than re-running it with
    # arguments this test chose, which would prove only that the test agrees
    # with itself.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(publisher_schedule))
    passed = [
        keyword.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "make_enqueue_schedule"
        for keyword in node.keywords
        if keyword.arg == "workflow_timeout_seconds"
    ]
    assert len(passed) == 1, (
        "the publisher schedule no longer passes exactly one "
        "workflow_timeout_seconds to its enqueue factory"
    )
    assert getattr(passed[0], "id", None) == "PUBLISHER_WORKFLOW_TIMEOUT_SECONDS"


def test_deadline_sweep_reads_columns_dbos_actually_defines(
    record_property,
) -> None:
    """Pin the column names against DBOS's own schema, not against the fake."""
    record_property("issue", "1257")
    pytest.importorskip("dbos")
    from dbos._schemas.system_database import SystemSchema

    defined = {
        column.name for column in SystemSchema.workflow_status.columns
    }
    required = {
        "workflow_uuid",
        "name",
        "queue_name",
        "status",
        "application_version",
        "started_at_epoch_ms",
        "workflow_deadline_epoch_ms",
        "recovery_attempts",
        "updated_at",
        # The two columns the cancel newly writes. A DBOS bump that dropped
        # either would otherwise fail only in production.
        "deduplication_id",
        "completed_at",
    }
    assert required <= defined, sorted(required - defined)


# ---------------------------------------------------------------------------
# issue #1254 — a stranded scheduled workflow needs a scoped remedy
# ---------------------------------------------------------------------------


def test_verb_clears_a_workflow_stranded_by_a_version_cutover(
    workflow_store, record_property,
) -> None:
    record_property("issue", "1254")
    workflow_store["wf-stranded"] = _row(
        name="okg_runtime_gates_reevaluate",
        status="ENQUEUED",
        application_version=None,
        started_at_epoch_ms=None,
    )

    result = workflow_cancel.cancel_deployment_workflow(
        dsn=DSN, deployment=DEPLOYMENT, workflow_id="wf-stranded",
    )

    assert result.previous_status == "ENQUEUED"
    assert result.status == "CANCELLED"
    assert result.name == "okg_runtime_gates_reevaluate"
    assert result.queue_name == PARTITION_QUEUE
    # It was waiting, not running, so no concurrency slot was freed. The row
    # blocked worker readiness, which is what cancelling it resolves.
    assert result.queue_slot_released is False
    assert workflow_store["wf-stranded"]["status"] == "CANCELLED"
    assert workflow_store.commits == 1, "the cancellation was never committed"
    # The dedup key must not stay claimed by a cancelled row, or the next
    # enqueue in the same bucket collides with a workflow that is not running.
    assert workflow_store["wf-stranded"]["deduplication_id"] is None


def test_cutover_refusal_names_the_command_that_clears_each_blocker(
    record_property,
) -> None:
    """#1254: the refusal used to name the row and offer no way out."""
    record_property("issue", "1254")
    from okg.substrate.runtime.services import RuntimeCutoverBlocked

    error = RuntimeCutoverBlocked(
        deployment=DEPLOYMENT,
        live_version="okg-runtime-v3",
        blocker_rows=[
            (
                "1f041aec-feac-4cac-8569-64f779d2f389",
                "okg_runtime_gates_reevaluate",
                PARTITION_QUEUE,
                "ENQUEUED",
                None,
                None,
            ),
        ],
    )

    expected = (
        "uv run okg runtime cancel-workflow --deployment cooper "
        "1f041aec-feac-4cac-8569-64f779d2f389"
    )
    assert error.blockers[0]["remedy_command"] == expected
    assert expected in str(error)
    assert expected in str(error.to_dict())


# ---------------------------------------------------------------------------
# the verb refuses every out-of-scope target instead of writing
# ---------------------------------------------------------------------------


def test_verb_refuses_a_workflow_that_is_already_final(
    workflow_store, record_property,
) -> None:
    record_property("issue", "1257")
    workflow_store["wf-done"] = _row(status="SUCCESS")

    with pytest.raises(SubstrateError) as excinfo:
        workflow_cancel.cancel_deployment_workflow(
            dsn=DSN, deployment=DEPLOYMENT, workflow_id="wf-done",
        )

    assert excinfo.value.code == "workflow_already_terminal"
    assert workflow_store["wf-done"]["status"] == "SUCCESS"


def test_verb_refuses_a_workflow_on_another_deployments_queue(
    workflow_store, record_property,
) -> None:
    record_property("issue", "1257")
    workflow_store["wf-elsewhere"] = _row(queue_name="okg-other")

    with pytest.raises(SubstrateError) as excinfo:
        workflow_cancel.cancel_deployment_workflow(
            dsn=DSN, deployment=DEPLOYMENT, workflow_id="wf-elsewhere",
        )

    assert excinfo.value.code == "workflow_outside_deployment"
    assert workflow_store["wf-elsewhere"]["status"] == "PENDING"


@pytest.mark.parametrize(
    ("case", "queue_name", "status"),
    [
        # The cutover refusal names blockers with no queue filter, so both of
        # these shapes can appear in the message that advertises this verb.
        # Refusing them here would make the advertised remedy a dead end.
        ("dbos internal queue", "_dbos_internal_queue", "ENQUEUED"),
        ("no queue at all", None, "ENQUEUED"),
    ],
)
def test_verb_clears_a_cutover_blocker_that_holds_no_slot(
    workflow_store, case: str, queue_name: str | None, status: str,
    record_property,
) -> None:
    record_property("issue", "1254")
    workflow_store["wf-blocker"] = _row(
        name="doctor_workflow", queue_name=queue_name, status=status,
    )

    result = workflow_cancel.cancel_deployment_workflow(
        dsn=DSN, deployment=DEPLOYMENT, workflow_id="wf-blocker",
    )

    assert result.status == "CANCELLED", case
    assert workflow_store["wf-blocker"]["status"] == "CANCELLED"
    # It held no bounded-concurrency slot, and the result must say so rather
    # than claim capacity was freed.
    assert result.queue_slot_released is False, case


def test_slot_release_is_reported_only_for_a_running_row_on_a_bounded_queue(
    workflow_store, record_property,
) -> None:
    record_property("issue", "1257")
    workflow_store["wf-holding"] = _row(status="PENDING")
    workflow_store["wf-waiting"] = _row(status="ENQUEUED")

    holding = workflow_cancel.cancel_deployment_workflow(
        dsn=DSN, deployment=DEPLOYMENT, workflow_id="wf-holding",
    )
    waiting = workflow_cancel.cancel_deployment_workflow(
        dsn=DSN, deployment=DEPLOYMENT, workflow_id="wf-waiting",
    )

    assert holding.queue_slot_released is True
    # DBOS counts a bounded queue as occupied by its PENDING rows only.
    assert waiting.queue_slot_released is False


def test_deadline_sweep_does_not_reach_the_dbos_internal_queue(
    workflow_store, record_property,
) -> None:
    """The internal queue has no bounded concurrency, and services.py already
    sweeps it; widening the verb's scope must not widen the sweep's."""
    record_property("issue", "1257")
    workflow_store["wf-internal"] = _row(
        queue_name="_dbos_internal_queue",
        started_at_epoch_ms=1_000,
        workflow_deadline_epoch_ms=2_000,
    )

    report = workflow_cancel.cleanup_deadline_expired_pending_workflows(
        dsn=DSN, deployment=DEPLOYMENT, now_ms=5_000,
    )

    assert report["cancelled_count"] == 0
    assert workflow_store["wf-internal"]["status"] == "PENDING"


def test_deadline_candidate_row_missing_a_field_raises(record_property) -> None:
    """A malformed row must not resolve to a record whose update matches
    nothing, which the sweep would then read as a clean outcome."""
    record_property("issue", "1257")
    with pytest.raises(ValueError, match="missing required field"):
        workflow_cancel.deadline_expired_pending_workflows(
            [{"name": "okg_publisher_cycle", "workflow_deadline_epoch_ms": 1}],
            now_ms=5_000,
        )


def test_verb_refuses_an_unknown_workflow(
    workflow_store, record_property,
) -> None:
    record_property("issue", "1257")

    with pytest.raises(SubstrateError) as excinfo:
        workflow_cancel.cancel_deployment_workflow(
            dsn=DSN, deployment=DEPLOYMENT, workflow_id="wf-missing",
        )

    assert excinfo.value.code == "workflow_not_found"


def test_verb_refuses_when_the_dbos_schema_is_absent(
    workflow_store, record_property,
) -> None:
    record_property("issue", "1254")
    workflow_store.schema_present = False

    with pytest.raises(SubstrateError) as excinfo:
        workflow_cancel.cancel_deployment_workflow(
            dsn=DSN, deployment=DEPLOYMENT, workflow_id="wf-anything",
        )

    assert excinfo.value.code == "dbos_schema_absent"


def test_verb_reports_a_concurrent_transition_rather_than_a_false_clear(
    workflow_store, monkeypatch: pytest.MonkeyPatch, record_property,
) -> None:
    """A zero-row update must never read as a successful clear."""
    record_property("issue", "1257")
    workflow_store["wf-racing"] = _row(status="PENDING")

    original_execute = _FakeCursor.execute

    def racing_execute(self, sql, params=()):  # type: ignore[no-untyped-def]
        if " ".join(sql.split()).upper().startswith("UPDATE"):
            workflow_store["wf-racing"]["status"] = "SUCCESS"
        return original_execute(self, sql, params)

    monkeypatch.setattr(_FakeCursor, "execute", racing_execute)

    with pytest.raises(SubstrateError) as excinfo:
        workflow_cancel.cancel_deployment_workflow(
            dsn=DSN, deployment=DEPLOYMENT, workflow_id="wf-racing",
        )

    assert excinfo.value.code == "workflow_status_changed"
    assert excinfo.value.data["current_status"] == "SUCCESS"


def test_every_refusal_code_is_in_the_documented_registry(
    record_property,
) -> None:
    """These codes are attached per-instance, so the class-walking parity
    test in tests/substrate/test_error_codes_parity.py cannot see them.
    Without this check the documented error registry would silently
    under-report the verb's surface."""
    record_property("issue", "1257")
    import ast
    import inspect
    from pathlib import Path

    import yaml

    tree = ast.parse(inspect.getsource(workflow_cancel))
    raised = {
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "_refuse"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    }
    assert raised, "no refusal codes found; the extractor is out of date"

    repo_root = Path(workflow_cancel.__file__).resolve().parents[4]
    registry = yaml.safe_load(
        (repo_root / "docs/reference/error_codes.yaml").read_text(
            encoding="utf-8",
        )
    )
    documented = {entry["code"] for entry in registry["codes"]}
    assert raised <= documented, sorted(raised - documented)


# ---------------------------------------------------------------------------
# the operator reaches all of this through one documented verb
# ---------------------------------------------------------------------------


def test_cli_exposes_cancel_workflow_and_dispatches_to_the_remedy(
    monkeypatch: pytest.MonkeyPatch, capsys, record_property,
) -> None:
    record_property("issue", "1254")
    from okg.substrate.cli import runtime as cli_runtime
    from okg.substrate.cli.main import _build_parser

    parser = _build_parser()
    args = parser.parse_args(
        [
            "runtime", "cancel-workflow",
            "--deployment", DEPLOYMENT,
            "1f041aec-feac-4cac-8569-64f779d2f389",
        ]
    )
    assert args.rt_subcmd == "cancel-workflow"
    assert args.workflow_id == "1f041aec-feac-4cac-8569-64f779d2f389"

    seen: dict[str, str] = {}

    def fake_cancel(*, dsn: str, deployment: str, workflow_id: str):
        seen.update(
            dsn=dsn, deployment=deployment, workflow_id=workflow_id,
        )
        return workflow_cancel.WorkflowCancelResult(
            deployment=deployment,
            workflow_id=workflow_id,
            name="okg_publisher_cycle",
            queue_name=PARTITION_QUEUE,
            previous_status="PENDING",
            status="CANCELLED",
            application_version="okg-runtime-v2",
            recovery_attempts=2,
            queue_slot_released=True,
        )

    monkeypatch.setattr(
        workflow_cancel, "cancel_deployment_workflow", fake_cancel,
    )

    manifest = argparse.Namespace(
        runtime=argparse.Namespace(enabled=True),
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _name: manifest,
    )

    args.dsn = DSN
    exit_code = cli_runtime._cmd_runtime(args, as_json=True)

    assert exit_code == 0
    assert seen == {
        "dsn": DSN,
        "deployment": DEPLOYMENT,
        "workflow_id": "1f041aec-feac-4cac-8569-64f779d2f389",
    }
    payload = capsys.readouterr().out
    assert '"status": "CANCELLED"' in payload
    assert '"queue_slot_released": true' in payload


# ---------------------------------------------------------------------------
# issue #2502 — `okg run --once` cancels only a row no worker has taken
# ---------------------------------------------------------------------------


def test_expected_status_refuses_a_row_a_worker_already_started(
    workflow_store,
) -> None:
    """A PENDING (running) cycle is never cancelled by the ENQUEUED-only
    cancel; nothing is written."""
    workflow_store["wf-running"] = _row(status="PENDING")

    with pytest.raises(SubstrateError) as excinfo:
        workflow_cancel.cancel_deployment_workflow(
            dsn=DSN,
            deployment=DEPLOYMENT,
            workflow_id="wf-running",
            expected_status="ENQUEUED",
        )

    assert excinfo.value.code == "workflow_status_changed"
    assert excinfo.value.data["observed_status"] == "ENQUEUED"
    assert excinfo.value.data["current_status"] == "PENDING"
    assert workflow_store["wf-running"]["status"] == "PENDING"
    assert workflow_store["wf-running"]["deduplication_id"] is not None
    assert workflow_store.commits == 0


def test_expected_status_refuses_a_dequeue_between_read_and_write(
    workflow_store, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A worker that dequeues after the read still wins: the write is
    conditioned on ENQUEUED and matches no row."""
    workflow_store["wf-racing"] = _row(status="ENQUEUED")
    original_execute = _FakeCursor.execute

    def dequeue_at_update(self, sql, params=()):  # type: ignore[no-untyped-def]
        if " ".join(sql.split()).upper().startswith("UPDATE"):
            workflow_store["wf-racing"]["status"] = "PENDING"
        return original_execute(self, sql, params)

    monkeypatch.setattr(_FakeCursor, "execute", dequeue_at_update)

    with pytest.raises(SubstrateError) as excinfo:
        workflow_cancel.cancel_deployment_workflow(
            dsn=DSN,
            deployment=DEPLOYMENT,
            workflow_id="wf-racing",
            expected_status="ENQUEUED",
        )

    assert excinfo.value.code == "workflow_status_changed"
    assert workflow_store["wf-racing"]["status"] == "PENDING"


def test_expected_status_cancels_a_row_still_enqueued(workflow_store) -> None:
    workflow_store["wf-waiting"] = _row(status="ENQUEUED")

    result = workflow_cancel.cancel_deployment_workflow(
        dsn=DSN,
        deployment=DEPLOYMENT,
        workflow_id="wf-waiting",
        expected_status="ENQUEUED",
    )

    assert result.previous_status == "ENQUEUED"
    assert workflow_store["wf-waiting"]["status"] == "CANCELLED"
    assert workflow_store.commits == 1


def _drive_run_once_against_store(
    monkeypatch: pytest.MonkeyPatch,
    store: _WorkflowStore,
    *,
    after_poll,
    dequeue_timeout_seconds: float,
):
    """Run ``client_enqueue_publisher_cycle`` with the real cancel.

    The fake client reads the row status from the same fake table the
    real ``cancel_deployment_workflow`` writes, and ``after_poll`` runs
    after each poll so a test can move the row the way a worker would
    between the poll and the cancel.
    """
    from types import SimpleNamespace

    from okg.substrate.runtime import admin
    from okg.substrate.runtime.dbos_enqueue import DBOSWorkflowEnqueueResult

    store["wf-2502"] = _row(status="ENQUEUED")
    polls = {"n": 0}

    class Client:
        def list_workflows(self, *, workflow_ids=None, **_kwargs):
            if workflow_ids is None:
                return []  # idle queue: nothing else is running
            polls["n"] += 1
            status = store["wf-2502"]["status"]
            after_poll(polls["n"])
            return [SimpleNamespace(
                status=status,
                output={"status": "published"} if status == "SUCCESS" else None,
                error=None,
            )]

    monkeypatch.setattr(admin, "_build_client", lambda **_kw: Client())
    monkeypatch.setattr(admin, "registered_app_version", lambda _dsn: "v2")
    monkeypatch.setattr(
        admin,
        "enqueue_deployment_workflow",
        lambda **_kw: DBOSWorkflowEnqueueResult(workflow_id="wf-2502"),
    )
    return admin.client_enqueue_publisher_cycle(
        deployment=DEPLOYMENT,
        dsn=DSN,
        poll_interval_seconds=0.01,
        dequeue_timeout_seconds=dequeue_timeout_seconds,
    )


def test_run_once_never_cancels_a_cycle_a_worker_took_after_the_poll(
    workflow_store, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review blocker on #2502: the poll sees ENQUEUED, then a worker
    starts the cycle before the cancel reads. The running cycle must not
    be cancelled; the call keeps waiting and reports its real result."""

    def worker(poll: int) -> None:
        row = workflow_store["wf-2502"]
        if poll == 1:
            row["status"] = "PENDING"  # dequeued right after the poll
        elif poll >= 3:
            row["status"] = "SUCCESS"

    result = _drive_run_once_against_store(
        monkeypatch, workflow_store, after_poll=worker,
        dequeue_timeout_seconds=0.0,
    )

    assert result.status == "SUCCESS"
    assert workflow_store.commits == 0, "a running cycle was cancelled"


def test_run_once_cancels_a_row_no_worker_takes(
    workflow_store, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admin

    with pytest.raises(admin.PublisherCycleNotDequeued):
        _drive_run_once_against_store(
            monkeypatch, workflow_store, after_poll=lambda _poll: None,
            dequeue_timeout_seconds=0.05,
        )

    assert workflow_store["wf-2502"]["status"] == "CANCELLED"
    assert workflow_store["wf-2502"]["deduplication_id"] is None
    assert workflow_store.commits == 1
