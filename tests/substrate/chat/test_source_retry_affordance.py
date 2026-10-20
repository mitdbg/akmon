"""The failed-document retry affordance (PACT chat-source-admin).

``task.chat-source-admin.retry-affordance``. A2rchi's admin pages have a
retry button for documents that failed to ingest. Its home in chat has
to drive the recovery path that can ACTUALLY clear a DLQ row, and has to
report honestly when it did not.

The trap this module exists to close: ``okg ingest --enqueue`` carries no
supersession flag, so it can advance the watermark and add rows while the
DLQ row stays current forever. An affordance built on it would report
success while the failure persists. The first test below proves that
against ``runtime_dlq._explicitly_supersedes`` itself, so the claim is
checked against the real predicate rather than restated.

Precondition for the live arm: ``--enqueue`` requires ``runtime.enabled``
on the deployment.
"""

from __future__ import annotations

import json
import subprocess
from argparse import Namespace
from typing import Any

import pytest

from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.chat.source_admin import (
    RESYNC_COMMAND_HINT,
    SHARED_STATUS_FIELDS,
    RetryObservation,
    retry_plan,
    retry_verdict,
    shared_status_fields,
    status_field_diff,
)
from okg.substrate.runtime_dlq import _explicitly_supersedes


DEPLOYMENT = "okg-workspace"
SOURCE = "papers"
FAILED_WORKFLOW_ID = "wf-failed-0001"


def _workflow_row(
    *,
    workflow_id: str,
    status: str,
    supersedes: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "deployment": DEPLOYMENT,
        "sources": [SOURCE],
        "events": [],
        "run_mode": "scope_complete",
    }
    if supersedes is not None:
        payload["supersedes_workflow_id"] = supersedes
    return {
        "workflow_id": workflow_id,
        "name": "okg_source_sync",
        "status": status,
        "application_version": "v1",
        "queue_name": f"okg-{DEPLOYMENT}-source-sync",
        "args": [],
        "payload": payload,
    }


def _dlq(*failures: str) -> dict[str, Any]:
    return {
        "current_count": len(failures),
        "current_failures": [
            {
                "workflow_id": workflow_id,
                "workflow_name": "okg_source_sync",
                "status": "ERROR",
                "application_version": "v1",
                "responsibility": {
                    "deployment": DEPLOYMENT,
                    "sources": [SOURCE],
                    "workflow_type": "okg_source_sync",
                    "application_version": "v1",
                    "run_mode": "scope_complete",
                    "scope": [],
                },
            }
            for workflow_id in failures
        ],
    }


# ---------------------------------------------------------------------------
# Only supersession clears a DLQ row — checked against the real predicate
# ---------------------------------------------------------------------------

def test_ingest_enqueue_cannot_clear_a_dlq_row() -> None:
    """A success with no ``supersedes_workflow_id`` supersedes nothing.

    ``okg ingest --enqueue`` produces exactly this shape. Asserted
    against ``runtime_dlq._explicitly_supersedes`` — the function the
    DLQ gate actually consults — so the claim cannot drift from it.
    """
    failure = _workflow_row(
        workflow_id=FAILED_WORKFLOW_ID, status="ERROR",
    )
    plain_success = _workflow_row(workflow_id="wf-later", status="SUCCESS")
    assert _explicitly_supersedes(plain_success, failure) is False

    superseding = _workflow_row(
        workflow_id="wf-retry",
        status="SUCCESS",
        supersedes=FAILED_WORKFLOW_ID,
    )
    assert _explicitly_supersedes(superseding, failure) is True


def test_supersession_must_name_the_exact_failed_workflow() -> None:
    failure = _workflow_row(
        workflow_id=FAILED_WORKFLOW_ID, status="ERROR",
    )
    wrong = _workflow_row(
        workflow_id="wf-retry", status="SUCCESS", supersedes="wf-something-else",
    )
    assert _explicitly_supersedes(wrong, failure) is False


# ---------------------------------------------------------------------------
# The command chain
# ---------------------------------------------------------------------------

def test_retry_drives_only_the_existing_recovery_command() -> None:
    plan = retry_plan(
        deployment=DEPLOYMENT,
        source=SOURCE,
        dlq_payload=_dlq(FAILED_WORKFLOW_ID),
    )
    assert plan.applicable is True
    assert plan.failed_workflow_ids == (FAILED_WORKFLOW_ID,)
    assert plan.commands == (
        (
            "okg", "jobs", "enqueue-source",
            "--deployment", DEPLOYMENT,
            "--source", SOURCE,
            "--supersedes-workflow-id", FAILED_WORKFLOW_ID,
            "--apply",
        ),
    )
    rendered = plan.rendered[0]
    # THE retry path, and nothing else. No chat-local retry queue, no
    # chat-local failure record: the failed workflow id is read from the
    # DLQ authority's own payload.
    assert "--supersedes-workflow-id" in rendered
    assert " ingest " not in f" {rendered} "


def test_workflow_id_is_read_from_the_dlq_payload_not_invented() -> None:
    plan = retry_plan(
        deployment=DEPLOYMENT,
        source=SOURCE,
        dlq_payload=_dlq("wf-a", "wf-b"),
    )
    assert plan.failed_workflow_ids == ("wf-a", "wf-b")
    assert len(plan.commands) == 2
    assert {
        argv[argv.index("--supersedes-workflow-id") + 1]
        for argv in plan.commands
    } == {"wf-a", "wf-b"}


def test_a_source_with_no_current_dlq_row_gets_the_resync_hint() -> None:
    """Re-sync and retry are different affordances, and stay named apart."""
    plan = retry_plan(
        deployment=DEPLOYMENT, source=SOURCE, dlq_payload=_dlq(),
    )
    assert plan.applicable is False
    assert plan.commands == ()
    assert RESYNC_COMMAND_HINT in (plan.refusal or "")
    assert "cannot clear a DLQ row" in (plan.refusal or "")


def test_a_failure_for_another_source_is_not_this_source_s_retry() -> None:
    payload = _dlq(FAILED_WORKFLOW_ID)
    payload["current_failures"][0]["responsibility"]["sources"] = ["notes"]
    plan = retry_plan(
        deployment=DEPLOYMENT, source=SOURCE, dlq_payload=payload,
    )
    assert plan.applicable is False


def test_a_failure_carrying_no_workflow_id_refuses_to_plan() -> None:
    payload = _dlq(FAILED_WORKFLOW_ID)
    payload["current_failures"][0]["workflow_id"] = ""
    plan = retry_plan(
        deployment=DEPLOYMENT, source=SOURCE, dlq_payload=payload,
    )
    assert plan.applicable is False
    assert "no workflow_id" in (plan.refusal or "")


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------

def _plan() -> Any:
    return retry_plan(
        deployment=DEPLOYMENT,
        source=SOURCE,
        dlq_payload=_dlq(FAILED_WORKFLOW_ID),
    )


def test_dlq_row_still_current_is_not_succeeded_regardless_of_row_delta() -> None:
    """Rows landed, the watermark moved, and it still did not succeed.

    This is the exact shape ``okg ingest --enqueue`` produces: real
    substrate change, failure untouched. Reporting it as success is the
    silent fallback the umbrella forbids program-wide.
    """
    verdict = retry_verdict(
        plan=_plan(),
        before=RetryObservation(
            dlq_payload=_dlq(FAILED_WORKFLOW_ID),
            watermark="2026-08-04T00:00:00+00:00",
            queue_state={"current_count": 1},
            row_count=1000,
        ),
        after=RetryObservation(
            dlq_payload=_dlq(FAILED_WORKFLOW_ID),
            watermark="2026-08-04T00:10:00+00:00",
            queue_state={"current_count": 1},
            row_count=1250,
        ),
    )
    assert verdict.succeeded is False
    assert verdict.dlq_row_still_current is True
    assert verdict.still_current_workflow_ids == (FAILED_WORKFLOW_ID,)
    assert any("still in current_failures" in r for r in verdict.reasons)


def test_effect_free_retry_is_not_succeeded() -> None:
    """Watermark, queue state, and row count all unchanged: no effect.

    The DLQ row did leave ``current_failures`` here, so arm 1 passes —
    and the verdict is still NOT succeeded, because a success message
    with no substrate change is a silent fallback.
    """
    unchanged = dict(
        watermark="2026-08-04T00:00:00+00:00",
        queue_state={"current_count": 0},
        row_count=1000,
    )
    verdict = retry_verdict(
        plan=_plan(),
        before=RetryObservation(dlq_payload=_dlq(), **unchanged),
        after=RetryObservation(dlq_payload=_dlq(), **unchanged),
    )
    assert verdict.succeeded is False
    assert verdict.effect_free is True
    assert verdict.dlq_row_still_current is False
    assert any("all unchanged" in r for r in verdict.reasons)


def test_cleared_dlq_row_with_substrate_change_succeeds() -> None:
    verdict = retry_verdict(
        plan=_plan(),
        before=RetryObservation(
            dlq_payload=_dlq(FAILED_WORKFLOW_ID),
            watermark="2026-08-04T00:00:00+00:00",
            queue_state={"current_count": 1},
            row_count=1000,
        ),
        after=RetryObservation(
            dlq_payload=_dlq(),
            watermark="2026-08-04T00:10:00+00:00",
            queue_state={"current_count": 0},
            row_count=1250,
        ),
    )
    assert verdict.succeeded is True
    assert verdict.reasons == ()


def test_unobserved_signals_are_an_unproven_effect_not_a_success() -> None:
    """An unmeasured effect is not a proven one.

    Two failed reads must not compare equal and read as "unchanged", and
    an unrunnable comparison must not be skipped into a pass. The DLQ row
    cleared here — the retry's own condition — and the verdict is STILL
    not succeeded, because nothing measured the substrate change.
    """
    verdict = retry_verdict(
        plan=_plan(),
        before=RetryObservation(dlq_payload=_dlq(FAILED_WORKFLOW_ID)),
        after=RetryObservation(dlq_payload=_dlq()),
    )
    assert verdict.succeeded is False
    assert verdict.effect_free is False
    assert set(verdict.unobserved_signals) == {
        "watermark", "queue_state", "row_count",
    }
    assert any("effect unproven" in r for r in verdict.reasons)


def test_a_failed_recovery_command_is_not_a_retry() -> None:
    """A chain that exited non-zero never ran the recovery.

    Judging DLQ state after a command that failed to launch would credit
    or blame the retry for something it did not do.
    """
    verdict = retry_verdict(
        plan=_plan(),
        before=RetryObservation(dlq_payload=_dlq(FAILED_WORKFLOW_ID)),
        after=RetryObservation(dlq_payload=_dlq()),
        command_results=[{
            "command": "okg jobs enqueue-source ...",
            "returncode": 2,
            "stderr": "requires --apply under OKG_AGENT=1",
        }],
    )
    assert verdict.succeeded is False
    assert verdict.failed_commands == ("okg jobs enqueue-source ...",)
    assert any("recovery command itself failed" in r for r in verdict.reasons)


def test_no_plan_means_no_success_report() -> None:
    plan = retry_plan(
        deployment=DEPLOYMENT, source=SOURCE, dlq_payload=_dlq(),
    )
    verdict = retry_verdict(
        plan=plan,
        before=RetryObservation(),
        after=RetryObservation(),
    )
    assert verdict.succeeded is False
    assert any("no retry ran" in r for r in verdict.reasons)


# ---------------------------------------------------------------------------
# One status vocabulary (design.md §5)
# ---------------------------------------------------------------------------

def _dogfood_payload() -> dict[str, Any]:
    return {
        "schema": "okg.dogfood.status.v1",
        "deployment": DEPLOYMENT,
        "trust_level": "ready",
        "read_authority": {
            "ok": True,
            "generation_id": "gen:2026-08-04T00:00:00Z/42",
            "generation_status": "published",
        },
        "source_freshness": {"ok": True, "stale": [], "missing": []},
        "queues": {"substrate": {"depth": 0}, "source_sync": {}},
        "doctor": {"ok": True, "failed_checks": []},
        "warnings": [
            {
                "id": "sources.sync_liveness",
                "status": "warning",
                "details": {"dlq_count": 3},
            },
        ],
    }


#: The shared fields, transcribed BY HAND off the `_dogfood_payload`
#: literal above. An independent left-hand side: comparing
#: `shared_status_fields(p)` to itself would pass even if the function
#: returned all-None, which is what an earlier version of this test did.
EXPECTED_SHARED_STATUS = {
    "generation_id": "gen:2026-08-04T00:00:00Z/42",
    "trust_level": "ready",
    "source_freshness": {"ok": True, "stale": [], "missing": []},
    "queues": {"substrate": {"depth": 0}, "source_sync": {}},
    "dlq_count": 3,
    "doctor": {"ok": True, "failed_checks": []},
}


def test_chat_status_uses_the_same_field_names_and_values() -> None:
    payload = _dogfood_payload()
    rendered = shared_status_fields(payload)
    assert set(rendered) == set(SHARED_STATUS_FIELDS)
    # Against the hand-transcribed expectation, not against itself.
    assert rendered == EXPECTED_SHARED_STATUS
    assert status_field_diff(EXPECTED_SHARED_STATUS, payload) == {}


def test_the_status_diff_catches_a_gutted_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-vacuity: gut `shared_status_fields` and the diff must fire.

    The diff's right-hand side is computed by the very function under
    test, so without this arm a projection that returned nothing useful
    would still compare equal to itself and the test would pass.
    """
    from okg.substrate.chat import source_admin as mod

    monkeypatch.setattr(
        mod,
        "shared_status_fields",
        lambda payload, dlq_payload=None: dict.fromkeys(
            SHARED_STATUS_FIELDS
        ),
    )
    diff = mod.status_field_diff(EXPECTED_SHARED_STATUS, _dogfood_payload())
    assert set(diff) == set(SHARED_STATUS_FIELDS)
    assert all(row["reason"] == "values disagree" for row in diff.values())


def test_a_field_chat_omits_is_a_disagreement_not_a_pass() -> None:
    payload = _dogfood_payload()
    rendered = shared_status_fields(payload)
    rendered.pop("trust_level")
    diff = status_field_diff(rendered, payload)
    assert set(diff) == {"trust_level"}
    assert diff["trust_level"]["chat"] == "<absent>"


def test_a_renamed_or_restated_field_shows_in_the_diff() -> None:
    payload = _dogfood_payload()
    rendered = shared_status_fields(payload)
    rendered["trust_level"] = "healthy"
    diff = status_field_diff(rendered, payload)
    assert set(diff) == {"trust_level"}
    assert diff["trust_level"]["dogfood"] == "ready"


def test_absent_dlq_count_is_unavailable_never_zero() -> None:
    """A zero would read as "no dead letters"."""
    payload = _dogfood_payload()
    payload["warnings"] = []
    rendered = shared_status_fields(payload)
    assert rendered["dlq_count"] is None


def test_explicit_dlq_payload_is_the_dlq_authority() -> None:
    payload = _dogfood_payload()
    rendered = shared_status_fields(
        payload, dlq_payload=_dlq(FAILED_WORKFLOW_ID),
    )
    assert rendered["dlq_count"] == 1


@pytest.mark.parametrize("field", SHARED_STATUS_FIELDS)
def test_every_shared_field_is_extracted_not_recomputed(field: str) -> None:
    """Chat renders FROM the dogfood payload, so agreement is structural.

    Mutating the payload must move the chat-rendered value with it; a
    field chat computed independently would not follow.
    """
    payload = _dogfood_payload()
    baseline = shared_status_fields(payload)
    if field == "generation_id":
        payload["read_authority"]["generation_id"] = "gen:changed"
    elif field == "dlq_count":
        payload["warnings"][0]["details"]["dlq_count"] = 99
    else:
        payload[field] = {"mutated": True}
    assert shared_status_fields(payload)[field] != baseline[field]


# ---------------------------------------------------------------------------
# `okg chat source-retry` — the operator-facing affordance
# ---------------------------------------------------------------------------

class _Resolution:
    dsn = "postgresql://stub/stub"
    deployment = DEPLOYMENT
    authority = "stub"
    target = "stub"


def _completed(argv: Any) -> Any:
    return subprocess.CompletedProcess(
        args=list(argv), returncode=0, stdout="{}", stderr="",
    )


def _cli_args(**overrides: Any) -> Any:
    base: dict[str, Any] = {
        "deployment": DEPLOYMENT,
        "source": SOURCE,
        "apply": True,
        "dsn": None,
    }
    base.update(overrides)
    return Namespace(**base)


@pytest.fixture
def stub_cli(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Wire `okg chat source-retry` to injected DLQ reads.

    The DB reads are the only thing stubbed. The command chain, the
    verdict, and the exit code are the real ones.
    """
    from okg.substrate.cli import chat as chat_cli

    state: dict[str, Any] = {
        "dlq": [_dlq(FAILED_WORKFLOW_ID), _dlq(FAILED_WORKFLOW_ID)],
        # (watermark, row_count, last_run_id) before and after.
        "observations": [("t0", 100, "run-a"), ("t1", 140, "run-b")],
        "ran": [],
    }

    def _fake_dlq(_dsn: str) -> Any:
        queue = state["dlq"]
        return queue.pop(0) if len(queue) > 1 else queue[0]

    def _fake_observe(_dsn: str, _dep: str, _src: str) -> Any:
        queue = state["observations"]
        return queue.pop(0) if len(queue) > 1 else queue[0]

    monkeypatch.setattr(chat_cli, "_read_dlq", _fake_dlq)
    monkeypatch.setattr(chat_cli, "_observe_source", _fake_observe)
    monkeypatch.setattr(
        "okg.substrate.cli.dsn_authority.resolve_read_dsn",
        lambda *a, **k: _Resolution(),
    )
    return state


def test_cli_source_retry_exits_non_zero_when_the_row_is_still_current(
    stub_cli: dict[str, Any], capsys: pytest.CaptureFixture[str],
) -> None:
    """The affordance reports NOT succeeded, and the exit code says so."""
    from okg.substrate.cli.chat import _cmd_source_retry

    def _runner(argv: Any) -> Any:
        stub_cli["ran"].append(list(argv))
        return _completed(argv)

    code = _cmd_source_retry(_cli_args(), as_json=True, runner=_runner)
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "not_succeeded"
    assert payload["ok"] is False
    assert payload["dlq_row_still_current"] is True
    # Only the existing recovery command was invoked, through the
    # RUNNING interpreter rather than a bare `okg` from PATH. No
    # chat-local retry queue, and no `okg ingest` anywhere in the chain.
    assert stub_cli["ran"] == [[
        *running_okg_argv(),
        "jobs", "enqueue-source",
        "--deployment", DEPLOYMENT,
        "--source", SOURCE,
        "--supersedes-workflow-id", FAILED_WORKFLOW_ID,
        "--apply",
    ]]


def test_cli_source_retry_succeeds_when_the_row_clears(
    stub_cli: dict[str, Any], capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.cli.chat import _cmd_source_retry

    stub_cli["dlq"] = [_dlq(FAILED_WORKFLOW_ID), _dlq()]

    def _runner(argv: Any) -> Any:
        stub_cli["ran"].append(list(argv))
        return _completed(argv)

    code = _cmd_source_retry(_cli_args(), as_json=True, runner=_runner)
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "succeeded"
    assert payload["ok"] is True


def test_cli_source_retry_without_apply_is_not_reported_as_ok(
    stub_cli: dict[str, Any], capsys: pytest.CaptureFixture[str],
) -> None:
    """A plan is not a retry.

    Without ``--apply`` the command prints what it WOULD run and nothing
    else; ``ok`` stays false so a caller cannot read a dry run as a
    cleared DLQ.
    """
    from okg.substrate.cli.chat import _cmd_source_retry

    code = _cmd_source_retry(
        _cli_args(apply=False), as_json=True, runner=_completed,
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "planned"
    assert payload["ok"] is False
    assert payload["commands"] == [
        f"{' '.join(running_okg_argv())} jobs enqueue-source "
        f"--deployment {DEPLOYMENT} --source {SOURCE} "
        f"--supersedes-workflow-id {FAILED_WORKFLOW_ID} --apply"
    ]
    assert stub_cli["ran"] == []


def test_cli_source_retry_refuses_a_source_with_no_current_failure(
    stub_cli: dict[str, Any], capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.cli.chat import _cmd_source_retry

    stub_cli["dlq"] = [_dlq()]
    code = _cmd_source_retry(
        _cli_args(), as_json=True, runner=_completed,
    )
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "not_applicable"
    assert RESYNC_COMMAND_HINT in payload["refusal"]
