"""The retry chain carries --apply because the CLI gate demands it.

Moved out of ``test_source_retry_affordance.py`` unchanged apart from the
``slow`` marker (issue #2301, PR #2413): it runs one okg CLI child, about
one second, and every row of this file is owned by a blocking lane.

``jobs enqueue-source`` is refused without ``--apply`` under
``OKG_AGENT=1``. The check runs the CLI's own gate instead of restating
it, through the same child argv the retry affordance uses.
"""

from __future__ import annotations

import os
import subprocess
from typing import Any

from okg.substrate.chat.source_admin import retry_plan
from okg.substrate.cli.child_argv import running_okg_argv

DEPLOYMENT = "okg-workspace"
SOURCE = "papers"
FAILED_WORKFLOW_ID = "wf-failed-0001"


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


def test_the_chain_carries_apply_because_the_gate_demands_it() -> None:
    """Without the flag the affordance fails where an agent-driven chat runs.

    The failure would surface as "DLQ row still current" -- a correct
    verdict for the wrong reason.
    """
    plan = retry_plan(
        deployment=DEPLOYMENT,
        source=SOURCE,
        dlq_payload=_dlq(FAILED_WORKFLOW_ID),
        okg_argv_prefix=tuple(running_okg_argv()),
    )
    assert "--apply" in plan.commands[0]

    prefix_len = len(running_okg_argv())
    env = {**os.environ, "OKG_AGENT": "1"}
    without_apply = [
        arg for arg in plan.commands[0][prefix_len:] if arg != "--apply"
    ]
    refused = subprocess.run(
        [*running_okg_argv(), *without_apply],
        capture_output=True, text=True, check=False, env=env, timeout=120,
    )
    assert refused.returncode != 0
    assert "requires --apply" in refused.stdout + refused.stderr
