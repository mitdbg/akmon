"""`okg run --once` DBOS-path outcome reporting (issue #1311).

The DBOS branch of ``_cmd_run`` used to print the enqueued workflow's DBOS
state as ``status=`` next to ``published_status=`` from the cycle summary,
and exit 0 whenever the workflow state was SUCCESS. A workflow reaches
SUCCESS whenever the cycle function returned -- including when it published
nothing -- so a publisher that never published read as healthy.

These tests pin the two properties that failed during the 2026-08-23
incident:

  1. a cycle that did not publish is never labelled a success, and
  2. a failing enqueued cycle is reported as failed with a non-zero exit.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import okg.substrate.cli.main as cli


def _args(**kwargs) -> SimpleNamespace:
    defaults = {
        "once": True,
        "apply": True,
        "inline": False,
        "deployment": "demo",
        "dsn": None,
        "branch": None,
        "force_enrichers": False,
        "reconcile_count_stamp": False,
        "include_llm_invariants": False,
        "llm_invariant": None,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _enqueue_result(**kwargs) -> SimpleNamespace:
    defaults = {
        "workflow_id": "wf-1311",
        "status": "SUCCESS",
        "summary": None,
        "error": None,
        "elapsed_seconds": 2.0,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


@pytest.fixture
def dbos_run(monkeypatch: pytest.MonkeyPatch):
    """Drive `_cmd_run` down the DBOS path with a canned enqueue result."""
    import okg.substrate.runtime.admin as runtime_admin
    from okg.substrate.db import readiness as db_readiness

    monkeypatch.setattr(
        cli,
        "_resolve_run_context",
        lambda _args: ("postgres://test", "demo", None, True),
    )
    # Issue #2128 put one database-readiness answer in front of every
    # graph command, so `_cmd_run` now probes the DSN before it enqueues
    # anything. Nothing listens behind `postgres://test`, and these cases
    # are about outcome REPORTING, not readiness, so the probe is answered
    # with a fully migrated database. The gate is not stubbed away for
    # free: `test_the_readiness_gate_runs_before_the_enqueue` below fails
    # if it stops being called, and the refusal itself is pinned against
    # a real server in `tests/substrate/test_migrate.py`.
    def _ready(dsn, *, surface, deployment=None, roles=()):
        return db_readiness.DatabaseReadiness(
            schema_present=True, schema_applied=True, ledger_present=True
        )

    monkeypatch.setattr(db_readiness, "require_database_ready", _ready)

    calls: list[dict] = []

    def _run(result, *, args=None, as_json=True):
        def fake_enqueue(**kwargs):
            calls.append(kwargs)
            return result

        monkeypatch.setattr(
            runtime_admin,
            "client_enqueue_publisher_cycle",
            fake_enqueue,
        )
        rc = cli._cmd_run(args or _args(), as_json=as_json)
        return rc, calls

    return _run


def _payload(capsys: pytest.CaptureFixture[str]) -> dict:
    return json.loads(capsys.readouterr().out)


# ---------------------------------------------------------------------------
# 1. a failing enqueued cycle is never reported as SUCCESS
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "workflow_status",
    ["ERROR", "CANCELLED", "MAX_RECOVERY_ATTEMPTS_EXCEEDED"],
)
def test_failed_cycle_is_not_reported_as_success(
    dbos_run, capsys: pytest.CaptureFixture[str], workflow_status: str,
) -> None:
    rc, _ = dbos_run(
        _enqueue_result(
            status=workflow_status,
            error="CatalogOwnershipMismatch: foreign catalog owner",
        )
    )
    payload = _payload(capsys)

    assert rc == 1
    assert payload["outcome"] == "failed"
    assert payload["published"] is False
    assert payload["workflow_status"] == workflow_status
    # The publish outcome must not be reported using the DBOS workflow
    # vocabulary -- that conflation is the bug.
    assert payload["outcome"] != "SUCCESS"
    assert "CatalogOwnershipMismatch" in payload["error"]


def test_failed_cycle_human_output_says_failed_not_success(
    dbos_run, capsys: pytest.CaptureFixture[str],
) -> None:
    rc, _ = dbos_run(
        _enqueue_result(status="ERROR", error="CatalogOwnershipMismatch"),
        as_json=False,
    )
    out = capsys.readouterr().out

    assert rc == 1
    assert "outcome=failed" in out
    assert "SUCCESS" not in out.splitlines()[0]
    assert "nothing was published by this command" in out
    assert "okg runtime debug-workflow wf-1311 --deployment demo" in out


# ---------------------------------------------------------------------------
# 2. workflow SUCCESS + no publish is labelled `skipped`, not success
# ---------------------------------------------------------------------------


def test_workflow_success_with_skipped_cycle_is_labelled_skipped(
    dbos_run, capsys: pytest.CaptureFixture[str],
) -> None:
    """The exact incident shape: SUCCESS / skipped in 2s with no generation."""
    rc, _ = dbos_run(
        _enqueue_result(
            status="SUCCESS",
            summary={
                "deployment": "demo",
                "status": "skipped",
                "publish_skipped": True,
                "skipped": "source_sync_active",
                "generation_id": None,
            },
        )
    )
    payload = _payload(capsys)

    assert rc == 0  # a genuine no-op is still a legitimate outcome
    assert payload["outcome"] == "skipped"
    assert payload["exit_code"] == 0
    assert payload["published"] is False
    assert payload["workflow_status"] == "SUCCESS"
    assert payload["skip_reason"] == "source_sync_active"
    assert payload["generation_id"] is None
    assert "not a successful publish" in payload["outcome_detail"]
    # `status` used to hold the DBOS workflow state and read as the publish
    # result. It is gone; callers must read `outcome` or `workflow_status`.
    assert "status" not in payload


def test_workflow_success_without_summary_is_unknown_not_success(
    dbos_run, capsys: pytest.CaptureFixture[str],
) -> None:
    rc, _ = dbos_run(_enqueue_result(status="SUCCESS", summary=None))
    payload = _payload(capsys)

    assert rc == 1
    assert payload["exit_code"] == 1
    assert payload["outcome"] == "unknown"
    assert payload["published"] is False


def test_timed_out_poll_is_labelled_not_awaited(
    dbos_run, capsys: pytest.CaptureFixture[str],
) -> None:
    rc, _ = dbos_run(_enqueue_result(status="PENDING", elapsed_seconds=600.0))
    payload = _payload(capsys)

    assert rc == 1
    assert payload["outcome"] == "not_awaited"
    assert payload["published"] is False
    assert "outcome is unknown" in payload["outcome_detail"]


def test_real_publish_is_reported_as_published(
    dbos_run, capsys: pytest.CaptureFixture[str],
) -> None:
    rc, _ = dbos_run(
        _enqueue_result(
            status="SUCCESS",
            summary={
                "status": "published",
                "generation_id": "okg:demo:gen:42",
                "nodes_added": 17,
            },
            elapsed_seconds=1503.0,
        )
    )
    payload = _payload(capsys)

    assert rc == 0
    assert payload["outcome"] == "published"
    assert payload["published"] is True
    assert payload["generation_id"] == "okg:demo:gen:42"


# ---------------------------------------------------------------------------
# 3. flag plumbing on the DBOS path
# ---------------------------------------------------------------------------


def test_force_enrichers_and_branch_reach_the_enqueued_cycle(
    dbos_run, capsys: pytest.CaptureFixture[str],
) -> None:
    rc, calls = dbos_run(
        _enqueue_result(
            status="SUCCESS",
            summary={"status": "published", "generation_id": "okg:demo:gen:9"},
        ),
        args=_args(force_enrichers=True, branch="experiment"),
    )
    payload = _payload(capsys)

    assert rc == 0
    assert calls == [{
        "deployment": "demo",
        "dsn": "postgres://test",
        "branch_name": "experiment",
        "force_enrichers": True,
        # #2502: only the CLI bounds the wait for a worker to take the row.
        "dequeue_timeout_seconds": 30.0,
    }]
    # Echoed back so the operator can see what the enqueued cycle ran with.
    assert payload["force_enrichers"] is True
    assert payload["branch_name"] == "experiment"


@pytest.mark.parametrize(
    ("attr", "value", "flag"),
    [
        ("reconcile_count_stamp", True, "--reconcile-count-stamp"),
        ("include_llm_invariants", True, "--include-llm-invariants"),
        ("llm_invariant", "some-judge", "--llm-invariant"),
    ],
)
def test_inline_only_flags_are_refused_not_silently_dropped(
    dbos_run,
    capsys: pytest.CaptureFixture[str],
    attr: str,
    value: object,
    flag: str,
) -> None:
    rc, calls = dbos_run(
        _enqueue_result(),
        args=_args(**{attr: value}),
    )
    captured = capsys.readouterr()
    error = json.loads(
        (captured.err or captured.out).strip().splitlines()[-1]
    )["error"]

    assert rc == 1
    assert calls == []  # refused before anything was enqueued
    assert flag in error["message"]
    assert "--inline" in error["fix_hint"]


def test_force_enrichers_on_a_skipped_cycle_explains_the_no_op(
    dbos_run, capsys: pytest.CaptureFixture[str],
) -> None:
    """The reported symptom: `--force-enrichers` "does nothing" in 2s."""
    rc, _ = dbos_run(
        _enqueue_result(
            status="SUCCESS",
            summary={"status": "skipped", "generation_id": None},
        ),
        args=_args(force_enrichers=True),
    )
    payload = _payload(capsys)

    assert rc == 0
    assert payload["outcome"] == "skipped"
    assert payload["force_enrichers"] is True
    assert "--force-enrichers was passed to the enqueued cycle" in (
        payload["hint"]
    )
    assert "--inline --force-enrichers" in payload["hint"]


def test_no_force_enrichers_hint_when_the_cycle_published(
    dbos_run, capsys: pytest.CaptureFixture[str],
) -> None:
    rc, _ = dbos_run(
        _enqueue_result(
            status="SUCCESS",
            summary={"status": "published", "generation_id": "okg:demo:gen:3"},
        ),
        args=_args(force_enrichers=True),
    )
    payload = _payload(capsys)

    assert rc == 0
    assert payload["hint"] is None


# ---------------------------------------------------------------------------
# 4. a Phase-B-gate-blocked cycle is `blocked`, never `published`
#
# The generation id is minted BEFORE the Phase B gate runs, and
# `Publisher._cycle_chronos_native` sets
# ``checkpoint_status = "blocked" if blocked_reason else "published"``
# afterwards. So a gate-blocked cycle returns a workflow SUCCESS *and* a
# generation_id *and* ``status: "blocked"``. Classifying on the id alone
# reports `published` for a generation that was never exposed -- a silent
# fallback, and a regression against the pre-#1311 output which at least
# printed `published_status=blocked`.
# ---------------------------------------------------------------------------


def _blocked_summary(**overrides) -> dict:
    summary = {
        "deployment_name": "demo",
        "generation_id": "okg:demo:gen:77",
        "status": "blocked",
        "phase_b_blocked": True,
        "blocked_reason": {
            "invariant": "cross_source_alias_agreement",
            "detail": "2 conflicting aliases",
        },
    }
    summary.update(overrides)
    return summary


def test_gate_blocked_cycle_is_not_reported_as_published(
    dbos_run, capsys: pytest.CaptureFixture[str],
) -> None:
    rc, _ = dbos_run(
        _enqueue_result(status="SUCCESS", summary=_blocked_summary())
    )
    payload = _payload(capsys)

    # 3 is the blocked code on both `okg run` paths (#2346); 1 stays the
    # code for a failed or unknown outcome.
    assert rc == 3
    assert payload["exit_code"] == 3
    assert payload["outcome"] == "blocked"
    assert payload["published"] is False
    assert payload["workflow_status"] == "SUCCESS"
    # The id is still reported -- it is how the operator inspects the
    # blocked generation -- but it does not make the cycle a success.
    assert payload["generation_id"] == "okg:demo:gen:77"
    assert payload["publish_status"] == "blocked"
    assert payload["blocked_reason"] == {
        "invariant": "cross_source_alias_agreement",
        "detail": "2 conflicting aliases",
    }
    assert "blocked" in payload["outcome_detail"]


@pytest.mark.parametrize(
    "summary",
    [
        # status alone
        _blocked_summary(blocked_reason=None, phase_b_blocked=False),
        # blocked_reason alone, status not yet updated
        _blocked_summary(status="published", phase_b_blocked=False),
        # phase_b_blocked alone
        _blocked_summary(status="published", blocked_reason=None),
    ],
    ids=["status_only", "blocked_reason_only", "phase_b_blocked_only"],
)
def test_any_blocked_signal_defeats_the_generation_id(
    dbos_run, capsys: pytest.CaptureFixture[str], summary: dict,
) -> None:
    """Any one of the three blocked signals is enough to refuse `published`."""
    rc, _ = dbos_run(_enqueue_result(status="SUCCESS", summary=summary))
    payload = _payload(capsys)

    assert rc != 0
    assert payload["outcome"] == "blocked"
    assert payload["published"] is False


def test_blocked_cycle_human_output_does_not_say_published(
    dbos_run, capsys: pytest.CaptureFixture[str],
) -> None:
    rc, _ = dbos_run(
        _enqueue_result(status="SUCCESS", summary=_blocked_summary()),
        as_json=False,
    )
    out = capsys.readouterr().out

    assert rc != 0
    assert "outcome=blocked" in out
    assert "outcome=published" not in out
    assert "nothing was published by this command" in out


# ---------------------------------------------------------------------------
# 5. a failing cycle summary is `failed`, never `skipped`
#
# `skipped` exits 0 because a deliberate no-op is a legitimate outcome. A
# cycle that reported `status: "fail"` is not a no-op, and must not borrow
# the no-op's exit code.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("publish_status", ["fail", "failed", "error"])
def test_failing_cycle_summary_is_not_reported_as_skipped(
    dbos_run, capsys: pytest.CaptureFixture[str], publish_status: str,
) -> None:
    rc, _ = dbos_run(
        _enqueue_result(
            status="SUCCESS",
            summary={
                "deployment_name": "demo",
                "status": publish_status,
                "generation_id": None,
                "error": "psycopg.OperationalError: too many connections",
            },
        )
    )
    payload = _payload(capsys)

    assert rc != 0
    assert payload["outcome"] == "failed"
    assert payload["published"] is False
    assert payload["publish_status"] == publish_status


def test_failing_cycle_with_a_generation_id_is_still_failed(
    dbos_run, capsys: pytest.CaptureFixture[str],
) -> None:
    rc, _ = dbos_run(
        _enqueue_result(
            status="SUCCESS",
            summary={
                "status": "fail",
                "generation_id": "okg:demo:gen:78",
            },
        )
    )
    payload = _payload(capsys)

    assert rc != 0
    assert payload["outcome"] == "failed"
    assert payload["published"] is False


# ---------------------------------------------------------------------------
# The readiness gate the fixture above stubs out is really there (issue #2128)
# ---------------------------------------------------------------------------


def test_the_readiness_gate_runs_before_the_enqueue(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """An unready database is refused, and no cycle is ever enqueued.

    Every case above answers the readiness probe with a migrated
    database, which is only honest while the probe still exists and
    still runs FIRST. Deleting the gate from `_cmd_run`, or moving it
    after the enqueue, fails here: the refusal disappears, or
    `client_enqueue_publisher_cycle` is reached against a database that
    has no `okg` schema.
    """
    import okg.substrate.runtime.admin as runtime_admin
    from okg.substrate.db import readiness as db_readiness
    from okg.substrate.errors import SubstrateError

    monkeypatch.setattr(
        cli,
        "_resolve_run_context",
        lambda _args: ("postgres://test", "demo", None, True),
    )

    def _refuse(dsn, *, surface, deployment=None, roles=()):
        err = SubstrateError(
            f"{surface}: this database is not migrated",
            fix_hint="run `okg migrate --deployment demo --apply` once",
        )
        err.code = "database_not_migrated"
        raise err

    monkeypatch.setattr(db_readiness, "require_database_ready", _refuse)

    def _never(**kwargs):
        raise AssertionError(
            "the readiness gate must refuse before any cycle is enqueued"
        )

    monkeypatch.setattr(
        runtime_admin, "client_enqueue_publisher_cycle", _never
    )

    rc = cli._cmd_run(_args(), as_json=True)
    assert rc != 0
    # The error envelope goes to stderr; stdout stays clean so a caller
    # parsing `--json` output never reads a refusal as a result.
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["error"]["code"] == "database_not_migrated"
    assert "okg migrate" in payload["error"]["fix_hint"]


# ---------------------------------------------------------------------------
# Issue #2502: no worker took the cycle -> named refusal, non-zero exit
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("as_json", [True, False])
def test_undequeued_cycle_refusal_keeps_its_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    as_json: bool,
) -> None:
    """The typed refusal reaches the operator with its own code and hint.

    Before the fix every enqueue exception was rewrapped as a generic
    ``substrate_error`` ("DBOS-path publisher enqueue failed"), which
    would hide the named reason and the worker remedy.
    """
    import okg.substrate.runtime.admin as runtime_admin
    from okg.substrate.db import readiness as db_readiness

    monkeypatch.setattr(
        cli,
        "_resolve_run_context",
        lambda _args: ("postgres://test", "demo", None, True),
    )
    monkeypatch.setattr(
        db_readiness,
        "require_database_ready",
        lambda dsn, **kwargs: db_readiness.DatabaseReadiness(
            schema_present=True, schema_applied=True, ledger_present=True
        ),
    )

    def _not_dequeued(**_kwargs):
        raise runtime_admin.PublisherCycleNotDequeued(
            "no worker took publisher cycle 'wf-2502' off queue 'okg-demo'",
            data={
                "deployment": "demo",
                "workflow_id": "wf-2502",
                "queue_name": "okg-demo",
                "dequeue_timeout_seconds": 30.0,
                "cancelled": True,
            },
            fix_hint=(
                "start the deployment's worker with `okg runtime worker "
                "--deployment demo` and retry, or rerun with --inline"
            ),
        )

    monkeypatch.setattr(
        runtime_admin, "client_enqueue_publisher_cycle", _not_dequeued
    )

    rc = cli._cmd_run(_args(), as_json=as_json)

    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    if as_json:
        error = json.loads(captured.err)["error"]
        assert error["code"] == "publisher_cycle_not_dequeued"
        assert error["data"]["queue_name"] == "okg-demo"
        assert "okg runtime worker" in error["fix_hint"]
        assert "--inline" in error["fix_hint"]
    else:
        assert "error [publisher_cycle_not_dequeued]" in captured.err
        assert "okg runtime worker" in captured.err
