"""No-database check: a failed source enqueue names the child's cause (#2480).

`okg deployment apply` runs `okg jobs enqueue-source --json` per source. The
child reports its refusal as an `error` object on stdout, so the apply's
message `source supervision enqueue failed for <source>` used to stop there
and the operator never saw why.
"""
from __future__ import annotations

import json

from okg.substrate.deployment_apply import ApplyCommandResult, _source_enqueue_failure


def test_enqueue_failure_carries_the_json_error_from_stdout() -> None:
    result = ApplyCommandResult(
        1,
        stdout=json.dumps({"error": {
            "message": (
                "DBOS-path source-sync enqueue failed: relation "
                "\"dbos.workflow_status\" does not exist"
            ),
            "fix_hint": "run okg migrate",
        }}),
        stderr="",
    )
    error = _source_enqueue_failure("code_data_flow", result)
    assert str(error) == (
        "source supervision enqueue failed for code_data_flow: DBOS-path "
        "source-sync enqueue failed: relation \"dbos.workflow_status\" does "
        "not exist"
    )
    assert error.fix_hint == "run okg migrate"
    assert error.data["returncode"] == 1


def test_enqueue_failure_falls_to_the_last_stderr_line_and_masks_a_password() -> None:
    result = ApplyCommandResult(
        2,
        stdout="",
        stderr=(
            "Traceback (most recent call last):\n"
            "psycopg.OperationalError: connection to "
            "postgresql://okg:hunter2-secret@db/okg failed\n"
        ),
    )
    error = _source_enqueue_failure("docs", result)
    assert str(error).startswith(
        "source supervision enqueue failed for docs: psycopg.OperationalError"
    )
    assert "hunter2-secret" not in str(error)
    assert "hunter2-secret" not in error.data["stderr"]


def test_enqueue_failure_with_no_output_says_so() -> None:
    error = _source_enqueue_failure("docs", ApplyCommandResult(9, "", ""))
    assert str(error) == (
        "source supervision enqueue failed for docs: exit 9 with no error output"
    )


def test_enqueue_failure_masks_a_password_in_the_childs_fix_hint() -> None:
    result = ApplyCommandResult(1, stdout=json.dumps({"error": {
        "message": "enqueue refused",
        "fix_hint": "retry with --dsn postgresql://okg:hunter2-secret@db/okg",
    }}))
    error = _source_enqueue_failure("docs", result)
    assert "hunter2-secret" not in (error.fix_hint or "")
    assert error.fix_hint.startswith("retry with --dsn")
