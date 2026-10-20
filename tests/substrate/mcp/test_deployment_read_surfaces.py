from __future__ import annotations

import json
from pathlib import Path

import jsonschema

from okg.substrate.contracts.deployment_product.projections import (
    ReadPolicySummary,
    build_readback_projection_authority,
    schema_catalog,
)
from okg.substrate.mcp.deployment_read_surfaces import (
    deployment_read_resource,
    deployment_state_root,
    register_deployment_read_resources,
)


def test_mcp_resource_returns_typed_readback_for_the_existing_operation(tmp_path: Path) -> None:
    state_file = tmp_path / "upgrade-state.json"
    state_file.write_text(
        json.dumps(
            {
                "deployment": "acme-prod",
                "operation_id": "up-existing-operation",
                "lifecycle_plan_admission": {"plan_digest": "sha256:" + "1" * 64},
                "active_phase": None,
                "failed_phase": "readiness",
                "next_phase": "readiness",
                "status": "blocked",
                "completed": False,
                "phase_statuses": {"preflight": "completed", "readiness": "blocked"},
            }
        ),
        encoding="utf-8",
    )

    resource = deployment_read_resource(state_file)
    contents = resource["contents"]

    assert resource["schema_version"] == "okg.deployment_product.read_status/v2"
    assert resource["uri"].endswith("/operations/up-existing-operation")
    assert contents["state"] == "blocked"
    assert contents["blockers"] == [
        {"code": "operation-blocked", "phase": "readiness", "retryable": False}
    ]
    assert contents["next_actions"][0]["arguments"]["operation_id"] == contents["operation_id"]
    jsonschema.validate(contents, schema_catalog()["deployment_read_status"])


def test_registered_mcp_resources_are_profile_explicit_and_load_authority(
    tmp_path: Path,
) -> None:
    registered: dict[str, object] = {}
    principals: list[str] = []

    class FakeMcp:
        def resource(self, uri: str, **_metadata):
            def decorate(fn):
                registered[uri] = fn
                return fn
            return decorate

    for profile in ("dev", "prod"):
        state_file = tmp_path / f"acme-{profile}.json"
        state_file.write_text(json.dumps({
            "deployment": "acme",
            "operation_id": f"up-{profile}",
            "lifecycle_plan_admission": {"plan_digest": "sha256:" + "6" * 64},
            "active_phase": None,
            "completed": False,
            "phase_statuses": {},
        }), encoding="utf-8")
        authority = build_readback_projection_authority(
            operation_id=f"up-{profile}",
            deployment_id="acme",
            plan_digest="sha256:" + "6" * 64,
            policy=ReadPolicySummary(
                audience_ids=("operator",),
                tenant_ids=(f"tenant-{profile}",),
                visible_fields=("summary",),
                masked_fields=("contact-email",),
                decision_ids=(f"policy-{profile}",),
            ),
        )
        state_file.with_suffix(".readback.json").write_text(
            authority.model_dump_json(), encoding="utf-8"
        )

    resources = register_deployment_read_resources(
        FakeMcp(),
        deployment_name="acme",
        state_root=tmp_path,
        require_principal=principals.append,
    )
    assert set(resources) == {"dev", "prod"}
    assert set(registered) == {
        "okg://deployment/acme/read-status/dev",
        "okg://deployment/acme/read-status/prod",
    }
    prod = json.loads(resources["prod"]())
    assert prod["contents"]["operation_id"] == "up-prod"
    assert prod["contents"]["policy"]["masked_fields"] == ["contact-email"]
    assert principals == ["deployment-read-status"]


class _RecordingMcp:
    """Stands in for FastMCP: records each registered resource URI."""

    def __init__(self) -> None:
        self.registered: dict[str, object] = {}

    def resource(self, uri: str, **_metadata):
        def decorate(fn):
            self.registered[uri] = fn
            return fn
        return decorate


def _write_lifecycle_record(state_file: Path, *, operation_id: str) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps({
        "deployment": "acme",
        "operation_id": operation_id,
        "lifecycle_plan_admission": {"plan_digest": "sha256:" + "7" * 64},
        "active_phase": None,
        "completed": True,
        "status": "completed",
        "phase_statuses": {"preflight": "completed"},
    }), encoding="utf-8")


def test_a_profile_without_a_lifecycle_record_is_not_listed(tmp_path: Path) -> None:
    """Only the profile `okg deployment up` left a record for is served.

    A listed resource that can only answer "no such file" is what Codex read
    first on a fresh install, and gave up on.
    """
    _write_lifecycle_record(tmp_path / "acme-dev.json", operation_id="up-dev")
    mcp = _RecordingMcp()

    resources = register_deployment_read_resources(
        mcp,
        deployment_name="acme",
        state_root=tmp_path,
        require_principal=lambda _name: None,
    )

    assert set(resources) == {"dev"}
    assert set(mcp.registered) == {"okg://deployment/acme/read-status/dev"}
    assert json.loads(resources["dev"]())["contents"]["operation_id"] == "up-dev"


def test_no_state_root_lists_no_read_status_resource(tmp_path: Path) -> None:
    """A deployment with no repository to hold `.okg/up` lists nothing."""
    mcp = _RecordingMcp()

    resources = register_deployment_read_resources(
        mcp,
        deployment_name="acme",
        state_root=None,
        require_principal=lambda _name: None,
    )

    assert resources == {}
    assert mcp.registered == {}


def test_state_root_is_the_deployment_repository_not_the_working_directory(
    tmp_path: Path, monkeypatch,
) -> None:
    """The record is found from the deployment's repository, from any cwd.

    `okg deployment up` writes `.okg/up/<dep>-<profile>.json` in the directory
    it runs from, which is the repository root that holds the deployment. The
    agent launcher starts the server from `/`, so a cwd-relative lookup never
    found the record.
    """
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    deployment_dir = repo / "deployments" / "acme"
    deployment_dir.mkdir(parents=True)
    _write_lifecycle_record(
        repo / ".okg" / "up" / "acme-prod.json", operation_id="up-prod",
    )
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    state_root = deployment_state_root(deployment_dir)

    assert state_root == (repo / ".okg" / "up").resolve()
    mcp = _RecordingMcp()
    resources = register_deployment_read_resources(
        mcp,
        deployment_name="acme",
        state_root=state_root,
        require_principal=lambda _name: None,
    )
    assert set(mcp.registered) == {"okg://deployment/acme/read-status/prod"}
    assert json.loads(resources["prod"]())["contents"]["operation_id"] == "up-prod"


def test_a_git_worktree_file_marks_the_repository_root(tmp_path: Path) -> None:
    """A linked worktree has a `.git` FILE; it is still the repository root."""
    repo = tmp_path / "worktree"
    repo.mkdir()
    (repo / ".git").write_text("gitdir: /elsewhere/.git/worktrees/x\n")
    deployment_dir = repo / "deployments" / "acme"
    deployment_dir.mkdir(parents=True)

    assert deployment_state_root(deployment_dir) == (repo / ".okg" / "up").resolve()


def test_a_deployment_outside_any_repository_has_no_state_root(
    tmp_path: Path,
) -> None:
    deployment_dir = tmp_path / "installed" / "acme"
    deployment_dir.mkdir(parents=True)

    assert deployment_state_root(deployment_dir) is None
    assert deployment_state_root(None) is None
