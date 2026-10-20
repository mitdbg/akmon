"""Receipt-backed runtime fence and drain used before candidate checkout."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
from typing import Any

from okg.substrate.runtime.admin import client_drain
from okg.substrate.runtime.promotion_fence import (
    acquire_promotion_fence,
    release_promotion_fence,
)


SCHEMA = "okg.deployment_runtime_quiescence.v1"


def quiesce_runtime(
    *,
    deployment: str,
    dsn: str,
    owner: str,
    candidate_revision: str,
    lifecycle_plan_digest: str,
    lifecycle_contract_digest: str,
    max_wait_seconds: float,
    poll_interval_seconds: float = 2.0,
    stable_zero_seconds: float = 31.0,
    bootstrap_quiescence: bool = False,
) -> dict[str, Any]:
    """Fence new work, remove schedules, and wait for every queue to drain."""
    metadata = {
        "schema": SCHEMA,
        "deployment": deployment,
        "candidate_revision": candidate_revision,
        "lifecycle_plan_digest": lifecycle_plan_digest,
        "lifecycle_contract_digest": lifecycle_contract_digest,
        "bootstrap_quiescence": bootstrap_quiescence,
    }
    fence = acquire_promotion_fence(
        deployment=deployment,
        dsn=dsn,
        promotion_id=owner,
        metadata=metadata,
    )
    try:
        drain = client_drain(
            deployment=deployment,
            dsn=dsn,
            max_wait_seconds=max_wait_seconds,
            poll_interval_seconds=poll_interval_seconds,
            stable_zero_seconds=stable_zero_seconds,
        )
    except Exception as drain_exc:  # noqa: BLE001
        payload: dict[str, Any] = {
            "schema": SCHEMA,
            "ok": False,
            "status": "drain_error",
            "deployment": deployment,
            "owner": owner,
            "candidate_revision": candidate_revision,
            "lifecycle_plan_digest": lifecycle_plan_digest,
            "lifecycle_contract_digest": lifecycle_contract_digest,
            "bootstrap_quiescence": bootstrap_quiescence,
            "fence": fence,
            "drain_error": {
                "type": type(drain_exc).__name__,
                "message": str(drain_exc),
            },
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            payload["pre_mutation_reopen"] = release_promotion_fence(
                deployment=deployment,
                dsn=dsn,
                promotion_id=owner,
                schedule_disposition="restore",
            )
        except Exception as reopen_exc:  # noqa: BLE001
            payload["pre_mutation_reopen_error"] = {
                "type": type(reopen_exc).__name__,
                "message": str(reopen_exc),
                "fence_retained": True,
            }
        return payload
    payload: dict[str, Any] = {
        "schema": SCHEMA,
        "ok": drain.drained,
        "status": "quiesced" if drain.drained else "drain_failed",
        "deployment": deployment,
        "owner": owner,
        "candidate_revision": candidate_revision,
        "lifecycle_plan_digest": lifecycle_plan_digest,
        "lifecycle_contract_digest": lifecycle_contract_digest,
        "bootstrap_quiescence": bootstrap_quiescence,
        "fence": fence,
        "drain": {
            "deployment": drain.deployment,
            "drained": drain.drained,
            "in_flight_at_timeout": drain.in_flight_at_timeout,
            "elapsed_seconds": drain.elapsed_seconds,
            "pending_workflows": drain.pending_workflows,
        },
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    if not drain.drained:
        try:
            payload["pre_mutation_reopen"] = release_promotion_fence(
                deployment=deployment,
                dsn=dsn,
                promotion_id=owner,
                schedule_disposition="restore",
            )
        except Exception as reopen_exc:  # noqa: BLE001
            payload["pre_mutation_reopen_error"] = {
                "type": type(reopen_exc).__name__,
                "message": str(reopen_exc),
                "fence_retained": True,
            }
    return payload


def write_receipt(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--deployment", required=True)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--lifecycle-plan-digest", required=True)
    parser.add_argument("--lifecycle-contract-digest", required=True)
    parser.add_argument("--max-wait-seconds", type=float, default=600.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=2.0)
    parser.add_argument("--stable-zero-seconds", type=float, default=31.0)
    parser.add_argument("--bootstrap-quiescence", action="store_true")
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    payload = quiesce_runtime(
        deployment=args.deployment,
        dsn=args.dsn,
        owner=args.owner,
        candidate_revision=args.candidate_revision,
        lifecycle_plan_digest=args.lifecycle_plan_digest,
        lifecycle_contract_digest=args.lifecycle_contract_digest,
        max_wait_seconds=args.max_wait_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        stable_zero_seconds=args.stable_zero_seconds,
        bootstrap_quiescence=args.bootstrap_quiescence,
    )
    if args.receipt is not None:
        write_receipt(args.receipt, payload)
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
