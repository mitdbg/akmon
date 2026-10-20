from __future__ import annotations


def patch_deployment_apply_ready(monkeypatch) -> None:
    """Stub deployment-apply health for tests focused on other live checks."""

    def _apply_health(**kwargs):
        return {
            "ok": True,
            "status": "ready",
            "managed": True,
            "deployment": kwargs.get("deployment"),
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_apply.deployment_apply_health",
        _apply_health,
    )


def patch_runtime_worker_ready(monkeypatch) -> None:
    """Stub exact-one-worker proof for tests focused on other dogfood gates."""

    def _runtime_worker_process_audit(deployment: str):
        return {
            "ok": True,
            "status": "single_executor",
            "deployment": deployment,
            "root_count": 1,
            "process_count": 1,
            "split_executor": False,
            "roots": [{"pid": 101, "working_directory": "<test-root>"}],
            "processes": [{"pid": 101, "root_pid": 101}],
        }

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        _runtime_worker_process_audit,
    )


def patch_nomos_ready(monkeypatch) -> None:
    """Stub policy/apply/worker slices for tests focused on other gates."""

    def _audit_status(**kwargs):
        return {
            "schema": "okg.nomos.deployment_policy_audit.v1",
            "ok": True,
            "status": "pass",
            "deployment": kwargs.get("deployment"),
            "required_by_profile": True,
            "findings": [],
            "warnings": [],
        }

    def _active_state_status(**kwargs):
        return {
            "schema": "okg.nomos.runtime_active_state.v1",
            "ok": True,
            "status": "pass",
            "deployment": kwargs.get("deployment"),
            "effective_mode": "enforce",
            "blocks_actions": True,
            "blocking_required": True,
            "readiness_profile": kwargs.get("readiness_profile"),
            "mode_source": "manifest",
            "rollout_exception": None,
            "findings": [],
        }

    monkeypatch.setattr(
        "okg.substrate.cli.deployment._ready_nomos_audit_status",
        _audit_status,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.deployment._ready_nomos_active_state_status",
        _active_state_status,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._ready_nomos_active_state_status",
        _active_state_status,
    )
    patch_deployment_apply_ready(monkeypatch)
    patch_runtime_worker_ready(monkeypatch)
