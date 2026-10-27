"""One fail-closed authority for post-deployment okg-ci operational closure."""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.errors import SubstrateError


LIVE_PROOF_SCHEMA = "okg.deployment.live_proof.v1"
CommandRunner = Callable[[Sequence[str]], tuple[int, str, str]]
#: The generation-bound read the proof runs. It is okg-ci deployment code, so a
#: checkout without that deployment (the public copy) cannot run the proof.
QUERY_PROBE_REPO_PATH = "deployments/okg-ci/smoke_probe.py"


def query_probe_path(repo_root: Path | None = None) -> Path:
    """Where the query probe lives in this checkout (or in ``repo_root``)."""
    root = repo_root if repo_root is not None else Path(__file__).resolve().parents[3]
    return root / QUERY_PROBE_REPO_PATH


def require_query_probe(repo_root: Path | None = None) -> Path:
    """Return the query probe, or refuse by name when the checkout lacks it.

    Without this check the proof would run every other read and then record
    only ``query.unreadable``, which does not say that the probe is missing.
    """
    path = query_probe_path(repo_root)
    if not path.is_file():
        raise SubstrateError(
            "deployment prove-live needs the okg-ci query probe, which this "
            "checkout does not have",
            data={"path": str(path)},
            fix_hint=(
                f"run prove-live from a checkout that carries {QUERY_PROBE_REPO_PATH}"
            ),
        )
    return path


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _default_runner(argv: Sequence[str]) -> tuple[int, str, str]:
    proc = subprocess.run(list(argv), capture_output=True, text=True, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def _read_json_command(
    runner: CommandRunner,
    argv: Sequence[str],
    *,
    failures: list[str],
    code: str,
) -> dict[str, Any]:
    try:
        rc, stdout, _stderr = runner(argv)
        payload = json.loads(stdout)
    except Exception:  # noqa: BLE001 - every unreadable authority fails closed.
        failures.append(f"{code}.unreadable")
        return {}
    if rc != 0 or not isinstance(payload, dict):
        failures.append(f"{code}.failed")
        return payload if isinstance(payload, dict) else {}
    return payload


def _required_int(value: Any, *, code: str, failures: list[str]) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        failures.append(code)
        return None
    return value


def _generation(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("generation_id") or payload.get("checkpoint_id")
    return str(value) if value else None


def _release_revision(payload: Mapping[str, Any]) -> str | None:
    direct = payload.get("code_revision") or payload.get("release_revision") or payload.get("revision")
    if direct:
        return str(direct)
    current = payload.get("current")
    if isinstance(current, Mapping):
        value = current.get("code_revision") or current.get("revision")
        return str(value) if value else None
    code = payload.get("code")
    if isinstance(code, Mapping) and code.get("revision"):
        return str(code["revision"])
    return None


def _release_kind(payload: Mapping[str, Any]) -> str | None:
    """The declared release kind the report carries, if any (#1962)."""
    direct = payload.get("release_kind")
    if direct:
        return str(direct)
    current = payload.get("current")
    if isinstance(current, Mapping) and current.get("release_kind"):
        return str(current["release_kind"])
    return None


def _release_binding(
    release: Mapping[str, Any],
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Whether the proof is bound to a repository revision, and why not.

    #1962. `release_bound` is a bare boolean, and before this it was False
    for a standalone deployment in exactly the same way it is False for a
    deployment whose running code does not match the receipt. Those are
    different facts: the first cannot be fixed and the second must be. The
    check stays False either way -- a live proof genuinely cannot bind a
    revision that the deployment records none of, and reporting True would
    assert a binding that does not exist -- but the reason is now named.
    """
    from okg.substrate.deployment_release import STANDALONE_RELEASE_KIND

    observed = _release_revision(release)
    kind = _release_kind(release)
    expected = receipt.get("candidate_revision")
    expected = str(expected) if expected else None
    if observed:
        bound = observed == expected
        reason = (
            "revision_matches_receipt" if bound
            else "revision_differs_from_receipt"
        )
    elif kind == STANDALONE_RELEASE_KIND:
        bound = False
        reason = "standalone_no_repository_revision"
    else:
        bound = False
        reason = "revision_unavailable"
    return {
        "bound": bound,
        "reason": reason,
        "release_kind": kind,
        "observed_revision": observed,
        "expected_revision": expected,
    }


def _source_watermark_rows(
    freshness: Mapping[str, Any],
) -> tuple[bool, list[dict[str, Any]]]:
    """Require named, fresh, timestamped authority for every declared source."""
    groups = freshness.get("groups")
    max_age = freshness.get("max_source_age_seconds")
    if (
        not isinstance(groups, Mapping)
        or not groups
        or isinstance(max_age, bool)
        or not isinstance(max_age, (int, float))
        or max_age <= 0
    ):
        return False, []
    observed_now = _now()
    watermarks: list[dict[str, Any]] = []
    for group_name, group in groups.items():
        if not isinstance(group, Mapping) or group.get("ok") is not True:
            return False, watermarks
        sources = group.get("sources")
        if not isinstance(sources, list) or not sources:
            return False, watermarks
        for source in sources:
            if not isinstance(source, Mapping):
                return False, watermarks
            name = str(source.get("source") or "").strip()
            observed_at = source.get("last_reconcile_at") or source.get(
                "last_progress_at"
            )
            try:
                observed_time = datetime.fromisoformat(
                    str(observed_at).replace("Z", "+00:00")
                )
                age_seconds = (observed_now - observed_time).total_seconds()
                timestamp_valid = (
                    observed_time.tzinfo is not None
                    and -300.0 <= age_seconds <= float(max_age)
                )
            except (TypeError, ValueError):
                timestamp_valid = False
            if (
                not name
                or source.get("status") != "fresh"
                or not timestamp_valid
            ):
                return False, watermarks
            watermarks.append(
                {
                    "group": str(group_name),
                    "source": name,
                    "observed_at": str(observed_at),
                    "age_seconds": age_seconds,
                    "freshness_kind": str(
                        source.get("freshness_kind") or "source_watermark"
                    ),
                }
            )
    return bool(watermarks), watermarks


def _phase_row(phase_results: Sequence[Any], phase: str) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in phase_results
        if isinstance(row, Mapping) and row.get("phase") == phase
    ]
    return rows[0] if rows else {}


def _predecessor_generation(
    phase_results: Sequence[Any], *, failures: list[str]
) -> tuple[dict[str, Any], str]:
    """Return the pre-upgrade published head and the phase that recorded it.

    ``okg up``'s ``preflight`` phase records the published head before any
    phase mutates the database (``predecessor_generation``: the generation
    id, its checkpoint counts and its pinned counts). Until okg#3112 S3 the
    preflight's effective-attribute recovery classification carried these
    fields; that classification went with the attribute copies.

    The record is REQUIRED, never assumed. A preflight row without one
    records ``generations.predecessor_unrecorded``; a record the preflight
    could not read (``status: unavailable``) or a deployment with nothing
    published has no generation id, so ``predecessor_bound`` fails closed.
    """
    record = _phase_row(phase_results, "preflight").get("predecessor_generation")
    if not isinstance(record, Mapping):
        failures.append("generations.predecessor_unrecorded")
        return {}, "preflight"
    return dict(record), "preflight"


def _load_schema() -> dict[str, Any]:
    raw = resources.files("okg.substrate").joinpath(
        "contracts/deployment_live_proof_v1.json"
    ).read_text(encoding="utf-8")
    return json.loads(raw)


def validate_live_proof(payload: Mapping[str, Any]) -> list[str]:
    return sorted(
        error.message
        for error in Draft202012Validator(_load_schema()).iter_errors(dict(payload))
    )


def _operation_receipt(
    path: Path,
    *,
    max_age_seconds: float,
    failures: list[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        failures.append("operation.receipt_unreadable")
        return {}, {}
    if not isinstance(receipt, dict):
        failures.append("operation.receipt_not_object")
        return {}, {}
    checks = {
        "schema": receipt.get("schema") == "okg.okg_ci_autoupdate_operation.v2",
        "status": receipt.get("status") == "succeeded" and receipt.get("exit_code") == 0,
        "worker": receipt.get("worker_state") == "active",
        "candidate": bool(receipt.get("candidate_revision")),
        "plan": bool(receipt.get("plan_digest")),
        "contract": bool(receipt.get("lifecycle_contract_digest")),
        "activation_pending": receipt.get("baseline_disposition") == "pending",
    }
    for name, ok in checks.items():
        if not ok:
            failures.append(f"operation.{name}")
    try:
        updated = datetime.fromisoformat(str(receipt["updated_at"]).replace("Z", "+00:00"))
        raw_age = (_now() - updated).total_seconds()
        if raw_age < -300.0:
            failures.append("operation.future")
        age = max(0.0, raw_age)
    except (KeyError, TypeError, ValueError):
        age = max_age_seconds + 1
        failures.append("operation.updated_at")
    if age > max_age_seconds:
        failures.append("operation.stale")
    try:
        state = json.loads(Path(str(receipt["state_file"])).read_text(encoding="utf-8"))
    except (KeyError, OSError, ValueError):
        failures.append("operation.state_unreadable")
        state = {}
    if not isinstance(state, dict) or state.get("completed") is not True:
        failures.append("operation.state_incomplete")
        state = state if isinstance(state, dict) else {}
    return receipt, state


def collect_live_deployment_proof(
    *,
    deployment: str,
    profile: str,
    operation_receipt: Path,
    dsn: str | None = None,
    max_receipt_age_seconds: float = 3600.0,
    max_observation_seconds: float = 120.0,
    runner: CommandRunner | None = None,
) -> dict[str, Any]:
    """Collect all terminal authorities in one generation-stable observation."""
    runner = runner or _default_runner
    failures: list[str] = []
    started_at = _now()
    started = time.monotonic()
    receipt, state = _operation_receipt(
        operation_receipt,
        max_age_seconds=max_receipt_age_seconds,
        failures=failures,
    )
    # Issue #2301: the running okg with -P, not the venv console script.
    okg = running_okg_argv()
    dsn_args = ["--dsn", dsn] if dsn else []
    ready_cmd = [
        *okg, "deployment", "ready", deployment, "--checks", profile,
        *dsn_args, "--json",
    ]
    readiness_before = _read_json_command(
        runner, ready_cmd, failures=failures, code="readiness.before"
    )
    generation = readiness_before.get("generation")
    generation = dict(generation) if isinstance(generation, Mapping) else {}
    successor = _generation(generation)
    release = _read_json_command(
        runner,
        [*okg, "deployment", "release", deployment, "--process-role", "autoupdate", *dsn_args, "--json"],
        failures=failures,
        code="release",
    )
    metrics = _read_json_command(
        runner,
        [*okg, "metrics", "--qa-snapshot", "--deployment", deployment, "--generation", str(successor or ""), *dsn_args, "--json"],
        failures=failures,
        code="metrics",
    )
    current_dlq = _read_json_command(
        runner,
        [*okg, "db", "current-runtime-dlq", "--deployment", deployment, *dsn_args, "--json"],
        failures=failures,
        code="queues.current_dlq",
    )
    query = _read_json_command(
        runner,
        [
            sys.executable,
            str(query_probe_path()),
            "--deployment", deployment,
            *(["--dsn", dsn] if dsn else []),
            "--generation-id", str(successor or ""),
            "--json",
        ],
        failures=failures,
        code="query",
    )
    readiness_after = _read_json_command(
        runner, ready_cmd, failures=failures, code="readiness.after"
    )

    phase_results = state.get("phase_results") or []
    predecessor, predecessor_phase = _predecessor_generation(
        phase_results, failures=failures
    )
    predecessor_id = predecessor.get("generation_id")
    integrity = generation.get("count_integrity")
    integrity = dict(integrity) if isinstance(integrity, Mapping) else {}
    checkpoint = integrity.get("checkpoint")
    checkpoint = dict(checkpoint) if isinstance(checkpoint, Mapping) else {}
    pinned = integrity.get("pinned")
    pinned = dict(pinned) if isinstance(pinned, Mapping) else {}
    node_count = _required_int(
        checkpoint.get("node_count"), code="generation.node_count", failures=failures
    )
    edge_count = _required_int(
        checkpoint.get("edge_count"), code="generation.edge_count", failures=failures
    )
    queues = readiness_before.get("queues")
    queues = dict(queues) if isinstance(queues, Mapping) else {}
    active_depth = _required_int(
        queues.get("active_queue_depth"), code="queues.active_depth", failures=failures
    )
    current_count = _required_int(
        current_dlq.get("current_count"), code="queues.current_count", failures=failures
    )
    historical_count = _required_int(
        current_dlq.get("historical_count"), code="queues.historical_count", failures=failures
    )
    freshness = readiness_before.get("source_freshness")
    freshness = dict(freshness) if isinstance(freshness, Mapping) else {}
    source_watermarks_ok, source_watermarks = _source_watermark_rows(freshness)
    liveness = readiness_before.get("source_sync_liveness")
    liveness = dict(liveness) if isinstance(liveness, Mapping) else {}
    source_sync = readiness_before.get("source_sync_status")
    source_sync = dict(source_sync) if isinstance(source_sync, Mapping) else {}
    convergence = source_sync.get("convergence")
    convergence = dict(convergence) if isinstance(convergence, Mapping) else {}
    doctor = readiness_before.get("doctor")
    doctor = dict(doctor) if isinstance(doctor, Mapping) else {}
    mcp = readiness_before.get("mcp")
    mcp = dict(mcp) if isinstance(mcp, Mapping) else {}
    after_generation = _generation(
        readiness_after.get("generation")
        if isinstance(readiness_after.get("generation"), Mapping) else {}
    )
    release_binding = _release_binding(release, receipt)
    checks = {
        "receipt_valid": not any(code.startswith("operation.") for code in failures),
        "release_bound": release_binding["bound"],
        "predecessor_bound": bool(predecessor_id)
        and isinstance(predecessor.get("checkpoint_counts"), Mapping)
        and isinstance(predecessor.get("pinned_counts"), Mapping),
        "successor_published": bool(successor) and generation.get("status") == "published",
        "successor_advanced": bool(predecessor_id) and bool(successor)
        and str(predecessor_id) != str(successor),
        "successor_counts_exact": integrity.get("ok") is True
        and checkpoint == pinned and isinstance(node_count, int) and isinstance(edge_count, int)
        and node_count + edge_count > 0,
        "metrics_bound": str(metrics.get("generation_id") or "") == str(successor or "")
        and isinstance(metrics.get("nodes_live_delta"), int)
        and isinstance(metrics.get("edges_live_delta"), int),
        "runtime_fresh": liveness.get("ok") is True
        and not liveness.get("missing_producers") and not liveness.get("stale_producers"),
        "sources_fresh": freshness.get("ok") is True
        and not freshness.get("missing") and not freshness.get("stale"),
        "source_watermarks": source_watermarks_ok,
        "source_convergence": convergence.get("ok") is True
        and convergence.get("state") == "ready",
        "queue_empty": active_depth == 0,
        "current_dlq_empty": current_count == 0 and isinstance(historical_count, int),
        "nomos": isinstance(readiness_before.get("nomos_audit"), Mapping)
        and readiness_before["nomos_audit"].get("ok") is True
        and isinstance(readiness_before.get("nomos_runtime"), Mapping)
        and readiness_before["nomos_runtime"].get("ok") is True,
        "doctor": doctor.get("ok") is True,
        "mcp_bound": mcp.get("ok") is True
        and str(mcp.get("pinned_generation_id") or mcp.get("generation_id") or "") == str(successor or ""),
        "query_bound": query.get("ok") is True
        and str(query.get("pinned_generation_id") or "") == str(successor or "")
        and isinstance(query.get("node_count"), int) and query.get("node_count", 0) > 0,
        "readiness_stable": readiness_before.get("ok") is True
        and readiness_after.get("ok") is True and successor == after_generation,
    }
    failures.extend(name for name, passed in checks.items() if not passed)
    duration = time.monotonic() - started
    if duration > max_observation_seconds:
        failures.append("observation.deadline")
    failures = sorted(set(failures))
    command = (
        f"okg deployment prove-live {deployment} --checks {profile} "
        f"--operation-receipt {operation_receipt} --json"
    )
    evidence_status = "pass" if not failures else "fail"
    evidence = [
        {
            "kind": kind,
            "status": evidence_status,
            "refs": ["req.live-operational-closure", "task.verify-post-merge-okg-ci"],
            "generation_id": successor,
            "summary": (
                "deployment live proof passed" if not failures
                else "deployment live proof failed: " + ", ".join(failures)
            ),
            "command": command,
            "data": {"nodes_live": node_count, "edges_live": edge_count},
        }
        for kind in ("graph_report", "published_generation")
    ]
    payload: dict[str, Any] = {
        "schema": LIVE_PROOF_SCHEMA,
        "ok": not failures,
        "deployment": deployment,
        "profile": profile,
        "operation": receipt,
        "observation": {
            "started_at": started_at.isoformat(),
            "completed_at": _now().isoformat(),
            "duration_seconds": round(duration, 6),
            "max_seconds": max_observation_seconds,
        },
        "release": release,
        # #1962: why `checks.release_bound` came out the way it did. A
        # standalone deployment records no repository revision by
        # declaration, which a bare False could not say.
        "release_binding": release_binding,
        "generations": {
            "predecessor": predecessor,
            # The phase that recorded the predecessor: always `preflight`.
            "predecessor_phase": predecessor_phase,
            "successor": {**generation, "checkpoint_counts": checkpoint, "pinned_counts": pinned},
        },
        "metrics": metrics,
        "runtime": liveness,
        "sources": {
            "freshness": freshness,
            "watermarks": source_watermarks,
            "convergence": convergence,
        },
        "queues": {"runtime": queues, "current_dlq": current_dlq},
        "nomos": {"audit": readiness_before.get("nomos_audit"), "runtime": readiness_before.get("nomos_runtime")},
        "doctor": doctor,
        "readiness": {"before": readiness_before, "after": readiness_after},
        "mcp": mcp,
        "query": query,
        "checks": checks,
        "failures": failures,
        "evidence": evidence,
    }
    schema_errors = validate_live_proof(payload)
    if schema_errors:
        payload["ok"] = False
        payload["failures"] = sorted(set([*failures, "proof.schema_invalid"]))
        for row in payload["evidence"]:
            row["status"] = "fail"
            row["summary"] = "deployment live proof schema validation failed"
            row["data"]["schema_errors"] = schema_errors
    return payload


def proof_digest(payload: Mapping[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(
        json.dumps(dict(payload), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "LIVE_PROOF_SCHEMA",
    "collect_live_deployment_proof",
    "query_probe_path",
    "require_query_probe",
    "proof_digest",
    "validate_live_proof",
]
