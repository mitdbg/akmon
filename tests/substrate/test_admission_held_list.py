"""The review inbox's held-admission read (issue #1752).

Split from test_admission.py so the blocking per-PR substrate-db lane runs it.
"""
from __future__ import annotations

import json
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.admission import ensure_envelope
from okg.substrate.cli.main import main


pytestmark = pytest.mark.postgres


def test_admission_cli_held_lists_every_mode_that_waits_for_a_decision(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Issue #1752: the review inbox reads `--status pending --held`.

    Quarantine-mode runs now finalize `pending` like review-mode runs, so
    the held read must return both and nothing in a non-held mode.
    """
    ids: dict[str, str] = {}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            for mode in ("review", "quarantine", "direct"):
                ids[mode] = ensure_envelope(
                    conn,
                    deployment_name="test_cli_held",
                    producer_id=f"{mode}_source",
                    producer_kind="source",
                    intent="source_sync",
                    admission_mode=mode,
                    status="pending",
                    idempotency_key=f"held-{mode}",
                    source=f"{mode}_source",
                    run_id=str(uuid4()),
                ).admission_id

    rc = main([
        "--json",
        "admission",
        "list",
        "--dsn",
        clean_runtime,
        "--deployment",
        "test_cli_held",
        "--status",
        "pending",
        "--held",
    ])
    assert rc == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["count"] == 2
    assert {row["admission_id"] for row in listed["admissions"]} == {
        ids["review"],
        ids["quarantine"],
    }

    # `--held` and `--mode` are exclusive: the held set is the filter.
    with pytest.raises(SystemExit):
        main([
            "admission", "list", "--dsn", clean_runtime,
            "--held", "--mode", "review",
        ])
