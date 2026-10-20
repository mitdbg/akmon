from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.pact.coordination_operations import (
    SetupPlan,
    build_setup_plan,
    client_shapes_for_routes,
)
from okg.substrate.pact.project_settings import (
    CoordinationPolicy,
    coordination_policy,
    coordination_capacity_digest,
    coordination_policy_digest,
    coordination_policy_digests,
    project_settings_digest,
)

from tests.private_policy import private_policy_path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _policy_document() -> dict[str, object]:
    return {
        "coordination": {
            "agent_transport": "restricted_ssh_stdio",
            "operator_transport": "viz_operator_token",
            "lease_ttl_seconds": {
                "chief": 3600,
                "sprint": 3600,
                "pact": 3600,
                "delegation": 3600,
            },
            "execution": {
                "lane_capacity": {"critical": 2, "light": 6},
                "named_resource_capacity": {"database": 1, "shared_storage": 2},
                "profiles": {
                    "default-light": {"lane": "light", "resources": {}},
                    "database-critical": {
                        "lane": "critical",
                        "resources": {"database": 1, "shared_storage": 1},
                    },
                },
                "helper_budgets": {
                    "chief": {"profile": "default-light", "max_active": 2},
                    "sprint": {"profile": "default-light", "max_active": 2},
                    "pact": {"profile": "database-critical", "max_active": 2},
                },
            },
            "client_routes": {
                "local-claude": "opaque:local-claude",
                "local-codex": "opaque:local-codex",
                "remote-a": "opaque:remote-a",
                "remote-b": "opaque:remote-b",
            },
            "action_registry": {
                "read": "allow",
                "merge": "require_approval",
            },
            "human_involvement_profiles": {
                "exception_only": {"overrides": {}},
            },
            "default_profile": "exception_only",
            "escalation_route": {
                "hops": ["worker", "coordinator", "chief", "operator"],
            },
            "decision_suppression": {
                "digest_fields": [
                    "assignment_id",
                    "assignment_generation",
                    "principal",
                    "assignment_session_id",
                    "mandate",
                    "action_digest",
                    "resource_digest",
                    "scope_digest",
                    "human_policy_digest",
                ],
                "terminal_statuses": [
                    "granted",
                    "denied",
                    "revoked",
                    "consumed",
                ],
            },
        }
    }


def _write_policy(root: Path, document: dict[str, object] | None = None) -> Path:
    path = root / "pact" / "project.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        yaml.safe_dump(document or _policy_document(), sort_keys=False),
        encoding="utf-8",
    )
    return path


def test_current_repo_coordination_policy_is_tracked_and_typed() -> None:
    private_policy_path(".okg/coordination.yaml")
    policy = coordination_policy(REPO_ROOT)

    assert isinstance(policy, CoordinationPolicy)
    assert policy.agent_transport == "restricted_ssh_stdio"
    assert policy.operator_transport == "viz_operator_token"
    assert policy.lease_ttl_seconds.to_json() == {
        "chief": 3600,
        "sprint": 3600,
        "pact": 3600,
        "delegation": 3600,
    }
    assert dict(policy.execution.lane_capacity) == {"critical": 2, "light": 6}
    assert dict(policy.execution.named_resource_capacity) == {
        "database": 1,
        "shared_storage": 2,
    }
    assert policy.profile("default-light").to_json() == {
        "lane": "light",
        "resources": {},
    }
    assert policy.profile("database-critical").to_json() == {
        "lane": "critical",
        "resources": {"database": 1, "shared_storage": 1},
    }
    # The remote route names are this deployment's own data; the test checks
    # their shape, not the names, so the source carries no machine name.
    assert policy.client_route_names[:2] == ("local-claude", "local-codex")
    assert len(policy.remote_client_route_names) == 2
    assert dict(policy.client_routes) == {
        name: f"opaque:{name}" for name in policy.client_route_names
    }
    assert {
        kind: budget.to_json()
        for kind, budget in policy.execution.helper_budgets.items()
    } == {
        "chief": {"profile": "default-light", "max_active": 2},
        "sprint": {"profile": "default-light", "max_active": 2},
        "pact": {"profile": "database-critical", "max_active": 2},
    }
    assert policy.source == "pact/project.yaml"
    assert policy.policy_digest.startswith("sha256:")
    assert policy.project_digest.startswith("sha256:")
    assert policy.capacity_digest.startswith("sha256:")
    assert policy.to_json()["policy_digest"] == policy.policy_digest
    assert policy.to_json()["capacity_digest"] == policy.capacity_digest
    assert policy.to_json()["human_policy_digest"] == policy.human_policy_digest


def test_digests_are_deterministic_and_exposed_for_writer_binding(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write_policy(root)

    first = coordination_policy(root)
    second = coordination_policy(root)

    assert first.policy_digest == second.policy_digest
    assert first.project_digest == second.project_digest
    assert first.capacity_digest == second.capacity_digest
    assert coordination_policy_digest(root) == first.policy_digest
    assert project_settings_digest(root) == first.project_digest
    assert coordination_capacity_digest(root) == first.capacity_digest
    assert coordination_policy_digests(root) == {
        "policy_digest": first.policy_digest,
        "human_policy_digest": first.human_policy_digest,
        "project_digest": first.project_digest,
        "capacity_digest": first.capacity_digest,
    }


def test_semantically_identical_yaml_key_order_has_same_digest(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    first = _write_policy(root)
    before = coordination_policy(root)

    first.write_text(
        yaml.safe_dump(
            {
                "coordination": {
                    "execution": {
                        "helper_budgets": {
                            "pact": {
                                "max_active": 2,
                                "profile": "database-critical",
                            },
                            "sprint": {
                                "max_active": 2,
                                "profile": "default-light",
                            },
                            "chief": {
                                "max_active": 2,
                                "profile": "default-light",
                            },
                        },
                        "profiles": {
                            "database-critical": {
                                "resources": {"shared_storage": 1, "database": 1},
                                "lane": "critical",
                            },
                            "default-light": {"resources": {}, "lane": "light"},
                        },
                        "named_resource_capacity": {
                            "shared_storage": 2,
                            "database": 1,
                        },
                        "lane_capacity": {"light": 6, "critical": 2},
                    },
                    "lease_ttl_seconds": {
                        "delegation": 3600,
                        "pact": 3600,
                        "sprint": 3600,
                        "chief": 3600,
                    },
                    "operator_transport": "viz_operator_token",
                    "agent_transport": "restricted_ssh_stdio",
                    "client_routes": {
                        "local-claude": "opaque:local-claude",
                        "local-codex": "opaque:local-codex",
                        "remote-a": "opaque:remote-a",
                        "remote-b": "opaque:remote-b",
                    },
                    "action_registry": {
                        "read": "allow",
                        "merge": "require_approval",
                    },
                    "human_involvement_profiles": {
                        "exception_only": {"overrides": {}},
                    },
                    "default_profile": "exception_only",
                    "escalation_route": {
                        "hops": ["worker", "coordinator", "chief", "operator"],
                    },
                    "decision_suppression": {
                        "digest_fields": [
                            "assignment_id",
                            "assignment_generation",
                            "principal",
                            "assignment_session_id",
                            "mandate",
                            "action_digest",
                            "resource_digest",
                            "scope_digest",
                            "human_policy_digest",
                        ],
                        "terminal_statuses": [
                            "granted",
                            "denied",
                            "revoked",
                            "consumed",
                        ],
                    },
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    after = coordination_policy(root)
    assert after.policy_digest == before.policy_digest
    assert after.project_digest == before.project_digest
    assert after.capacity_digest == before.capacity_digest


def test_tracked_policy_edit_changes_both_digests(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    path = _write_policy(root)
    before = coordination_policy(root)

    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["coordination"]["lease_ttl_seconds"]["pact"] = 3601
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    after = coordination_policy(root)
    assert after.policy_digest != before.policy_digest
    assert after.project_digest != before.project_digest
    assert after.capacity_digest == before.capacity_digest


def test_capacity_only_edit_preserves_assignment_and_project_digests(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    path = _write_policy(root)
    before = coordination_policy(root)

    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["coordination"]["execution"]["lane_capacity"]["light"] = 0
    # A reduced capacity may be below a profile's demand; it is still a
    # valid policy and only affects admission of future work.
    document["coordination"]["execution"]["named_resource_capacity"]["database"] = 0
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    after = coordination_policy(root)
    assert after.policy_digest == before.policy_digest
    assert after.project_digest == before.project_digest
    assert after.capacity_digest != before.capacity_digest
    assert after.execution.lane_capacity["light"] == 0
    assert after.execution.named_resource_capacity["database"] == 0


def test_human_policy_edit_has_its_own_digest_without_refencing_assignments(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    path = _write_policy(root)
    before = coordination_policy(root)

    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["coordination"]["action_registry"]["read"] = "notify"
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    after = coordination_policy(root)
    assert after.policy_digest == before.policy_digest
    assert after.project_digest == before.project_digest
    assert after.human_policy_digest != before.human_policy_digest


@pytest.mark.parametrize(
    "missing",
    [
        "action_registry",
        "human_involvement_profiles",
        "default_profile",
        "escalation_route",
        "decision_suppression",
    ],
)
def test_missing_human_policy_keys_fail_closed(tmp_path: Path, missing: str) -> None:
    root = tmp_path / "repo"
    document = _policy_document()
    del document["coordination"][missing]
    _write_policy(root, document)

    with pytest.raises(SubstrateError, match="human-policy field"):
        coordination_policy(root)


def test_local_overlay_and_environment_do_not_override_tracked_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "repo"
    _write_policy(root)
    local = root / ".okg" / "pact-local.yaml"
    local.parent.mkdir(parents=True)
    local.write_text(
        yaml.safe_dump(
            {
                "coordination": {
                    "agent_transport": "spoofed",
                    "operator_transport": "spoofed",
                    "lease_ttl_seconds": {"chief": 1},
                    "execution": {"spoofed": True},
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_AGENT_TRANSPORT", "spoofed")
    monkeypatch.setenv("OKG_OPERATOR_TRANSPORT", "spoofed")
    monkeypatch.setenv("OKG_COORDINATION_LEASE_TTL_SECONDS", "1")

    policy = coordination_policy(root)
    assert policy.agent_transport == "restricted_ssh_stdio"
    assert policy.operator_transport == "viz_operator_token"
    assert policy.lease_ttl_seconds.chief == 3600
    assert policy.execution.lane_capacity["critical"] == 2


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("agent_transport", "other", "unknown transport"),
        ("operator_transport", "other", "unknown transport"),
        ("agent_transport", True, "selected transport"),
        ("operator_transport", None, "selected transport"),
        ("lease_ttl_seconds", [], "must be a mapping"),
    ],
)
def test_unknown_or_malformed_policy_settings_refuse(
    tmp_path: Path, path: str, value: object, message: str
) -> None:
    root = tmp_path / "repo"
    document = _policy_document()
    document["coordination"][path] = value  # type: ignore[index]
    _write_policy(root, document)

    with pytest.raises(SubstrateError, match=message):
        coordination_policy(root)


@pytest.mark.parametrize(
    ("kind", "value", "message"),
    [
        ("missing", None, "missing required field"),
        ("chief", True, "positive integer"),
        ("sprint", 0, "positive integer"),
        ("pact", -1, "positive integer"),
        ("delegation", 1.0, "positive integer"),
    ],
)
def test_lease_kinds_are_closed_and_positive(
    tmp_path: Path, kind: str, value: object, message: str
) -> None:
    root = tmp_path / "repo"
    document = _policy_document()
    leases = document["coordination"]["lease_ttl_seconds"]  # type: ignore[index]
    if kind == "missing":
        del leases["delegation"]  # type: ignore[index]
    else:
        leases[kind] = value  # type: ignore[index]
    _write_policy(root, document)

    with pytest.raises(SubstrateError, match=message):
        coordination_policy(root)


def test_extra_fields_and_invalid_shapes_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    document = _policy_document()
    document["coordination"]["unexpected"] = True  # type: ignore[index]
    _write_policy(root, document)
    with pytest.raises(SubstrateError, match="unknown field"):
        coordination_policy(root)

    root = tmp_path / "other-repo"
    _write_policy(root, {"coordination": []})
    with pytest.raises(SubstrateError, match="must be a mapping"):
        coordination_policy(root)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("execution", [], "coordination.execution must be a mapping"),
        (
            "execution.lane_capacity",
            {"critical": 2},
            "missing required field",
        ),
        (
            "execution.lane_capacity",
            {"critical": True, "light": 6},
            "non-negative integer capacity",
        ),
        (
            "execution.named_resource_capacity",
            {" ": 1},
            "non-blank strings",
        ),
        (
            "execution.profiles",
            {"default-light": {"lane": "urgent", "resources": {}}},
            "unknown execution lane",
        ),
        (
            "execution.profiles",
            {"default-light": {"lane": "light", "resources": {"cache": 1}}},
            "unknown resource",
        ),
        (
            "execution.profiles",
            {"default-light": {"lane": "light", "resources": {"database": 0}}},
            "positive integer resource demand",
        ),
        (
            "execution.profiles",
            {" ": {"lane": "light", "resources": {}}},
            "non-blank strings",
        ),
        (
            "execution.helper_budgets",
            {"chief": {"profile": "default-light", "max_active": 2}},
            "missing required field",
        ),
        (
            "execution.helper_budgets.chief",
            {"profile": "missing", "max_active": 2},
            "unknown execution profile",
        ),
        (
            "execution.helper_budgets.sprint",
            {"profile": "default-light", "max_active": True},
            "non-negative integer capacity",
        ),
    ],
)
def test_execution_policy_malformed_settings_refuse(
    tmp_path: Path, path: str, value: object, message: str
) -> None:
    root = tmp_path / "repo"
    document = _policy_document()
    target: object = document["coordination"]
    parts = path.split(".")
    for part in parts[:-1]:
        target = target[part]  # type: ignore[index]
    target[parts[-1]] = value  # type: ignore[index]
    _write_policy(root, document)

    with pytest.raises(SubstrateError, match=message):
        coordination_policy(root)


def test_missing_tracked_policy_refuses(tmp_path: Path) -> None:
    with pytest.raises(SubstrateError, match="coordination must be a mapping"):
        coordination_policy(tmp_path / "repo-without-policy")


def _routes_document(routes: dict[str, str]) -> dict[str, object]:
    document = _policy_document()
    document["coordination"]["client_routes"] = routes  # type: ignore[index]
    return document


def test_remote_route_names_come_from_the_deployment_policy(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    routes = {
        "local-claude": "opaque:local-claude",
        "local-codex": "opaque:local-codex",
        "node-west": "opaque:node-west",
        "node-east": "opaque:node-east",
    }
    _write_policy(root, _routes_document(routes))

    policy = coordination_policy(root)
    assert policy.remote_client_route_names == ("node-east", "node-west")
    assert policy.client_route_names == (
        "local-claude",
        "local-codex",
        "node-east",
        "node-west",
    )
    shapes = client_shapes_for_routes(policy.client_routes)
    assert [(s.name, s.agent, s.host_ref, s.remote, s.route_ref) for s in shapes] == [
        ("local-claude", "claude-code", "logical:local", False, "opaque:local-claude"),
        ("local-codex", "codex", "logical:local", False, "opaque:local-codex"),
        ("node-east", "codex", "logical:node-east", True, "opaque:node-east"),
        ("node-west", "codex", "logical:node-west", True, "opaque:node-west"),
    ]

    names = policy.client_route_names
    probe = {
        "deployment_exists": True,
        "deployment_ready": True,
        "ledger_empty": True,
        "operator_control_available": True,
        "identity_ready": True,
        "git_ready": True,
        "okg_ready": True,
        "host_reachability": {name: True for name in names},
        "capacity": {"ok": True},
        "configuration_collisions": [],
        "credential_references": {name: "${OKG_AGENT_SSH_GRANT}" for name in names},
        "git_binding": {
            "branch": "coord/chief/g1",
            "base_commit": "sha256:git-base",
            "head_commit": "sha256:git-head",
            "worktree": "/ignored/local/worktree",
        },
        "pact_binding": {
            "change_id": "agent-coordination-operations",
            "task_id": "task.coord-operations.setup-start",
            "intent_digest": "sha256:" + "1" * 64,
        },
        "graph_binding": {"generation": "generation:42", "branch": "coord/chief/g1"},
        "runtime_binding": {
            "route_ref": "opaque:local-codex",
            "config_digest": "sha256:" + "2" * 64,
        },
    }
    plan = build_setup_plan(root, "okg-workspace", probe=probe)
    assert plan.checks["client_routes"] is True
    assert plan.checks["hosts"] is True
    assert "client_routes_unavailable" not in plan.blocked_by
    assert [client.name for client in plan.clients] == list(policy.client_route_names)
    assert SetupPlan.from_json(plan.to_json(), repo_root=root) == plan

    # A saved plan cannot drop or reshape a configured remote route.
    dropped = plan.to_json()
    dropped["clients"] = dropped["clients"][:3]
    with pytest.raises(ValueError, match="four closed shapes"):
        SetupPlan.from_json(dropped, repo_root=root)
    reshaped = plan.to_json()
    reshaped["clients"][2]["remote"] = False
    with pytest.raises(ValueError, match="closed order"):
        SetupPlan.from_json(reshaped, repo_root=root)


@pytest.mark.parametrize(
    ("routes", "message"),
    [
        (
            {"local-claude": "opaque:local-claude", "local-codex": "opaque:local-codex"},
            r"exactly 2 remote routes \(missing local: none; remote routes named: 0\)",
        ),
        (
            {
                "local-claude": "opaque:local-claude",
                "node-a": "opaque:node-a",
                "node-b": "opaque:node-b",
            },
            r"missing local: local-codex",
        ),
        (
            {
                "local-claude": "opaque:local-claude",
                "local-codex": "opaque:local-codex",
                "node-a": "opaque:node-a",
                "node-b": "opaque:node-b",
                "node-c": "opaque:node-c",
            },
            r"remote routes named: 3",
        ),
        (
            {
                "local-claude": "opaque:local-claude",
                "local-codex": "opaque:local-codex",
                "local-extra": "opaque:local-extra",
                "node-a": "opaque:node-a",
            },
            r"reserved local- prefix",
        ),
        (
            {
                "local-claude": "opaque:local-claude",
                "local-codex": "opaque:local-codex",
                "Node-A": "opaque:Node-A",
                "node-b": "opaque:node-b",
            },
            r"one lower-case token",
        ),
    ],
    ids=["no-remote", "missing-local", "three-remote", "local-prefix", "upper-case"],
)
def test_unconfigured_or_malformed_remote_routes_fail_loudly(
    tmp_path: Path, routes: dict[str, str], message: str
) -> None:
    root = tmp_path / "repo"
    _write_policy(root, _routes_document(routes))

    with pytest.raises(SubstrateError, match=message) as raised:
        coordination_policy(root)
    assert raised.value.data["field"] == "coordination.client_routes"

    # The planner names no remote route of its own when policy lacks them.
    plan = build_setup_plan(root, "okg-workspace", probe={})
    assert "client_routes_unavailable" in plan.blocked_by
    assert [client.name for client in plan.clients] == ["local-claude", "local-codex"]


NEUTRAL_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "coordination" / "neutral_coordination.yaml"
TRACKED_RUNTIME_POLICY = REPO_ROOT / ".okg" / "coordination.yaml"
NEUTRAL_REMOTE_ROUTES = ("remote-a", "remote-b")


def _neutralized(document: dict[str, object]) -> dict[str, object]:
    """Rename the tracked remote routes to the fixture's neutral names.

    The tracked policy's `deployment_fleet` block is left out: it names this
    repository's private deployments, is not route policy, and the fixture
    ships in the public copy, which names no fleet (#2823).
    """

    document.pop("deployment_fleet", None)
    routes = document["coordination"]["client_routes"]  # type: ignore[index]
    remote = sorted(name for name in routes if not name.startswith("local-"))
    assert len(remote) == len(NEUTRAL_REMOTE_ROUTES)
    names = dict(zip(remote, NEUTRAL_REMOTE_ROUTES))
    document["coordination"]["client_routes"] = {  # type: ignore[index]
        names.get(name, name): (
            f"opaque:{names[name]}" if name in names and ref == f"opaque:{name}" else ref
        )
        for name, ref in routes.items()
    }
    return document


def _assert_fixture_matches_tracked_policy(tracked: Path, fixture: Path) -> None:
    expected = _neutralized(yaml.safe_load(tracked.read_text(encoding="utf-8")))
    actual = yaml.safe_load(fixture.read_text(encoding="utf-8"))
    assert actual == expected, (
        f"{fixture.relative_to(REPO_ROOT) if fixture.is_relative_to(REPO_ROOT) else fixture}"
        " has drifted from .okg/coordination.yaml; copy the tracked policy again"
        " and rename its remote routes to remote-a and remote-b"
    )


def test_neutral_route_fixture_matches_tracked_policy() -> None:
    """The viz setup tests' neutral policy must equal the tracked one, renamed."""

    if not TRACKED_RUNTIME_POLICY.is_file():
        pytest.skip(".okg/coordination.yaml is not in this checkout (public mirror)")
    _assert_fixture_matches_tracked_policy(TRACKED_RUNTIME_POLICY, NEUTRAL_FIXTURE)


def test_neutral_route_fixture_drift_is_detected(tmp_path: Path) -> None:
    if not TRACKED_RUNTIME_POLICY.is_file():
        pytest.skip(".okg/coordination.yaml is not in this checkout (public mirror)")
    document = yaml.safe_load(NEUTRAL_FIXTURE.read_text(encoding="utf-8"))
    document["coordination"]["lease_ttl_seconds"]["chief"] += 1
    drifted = tmp_path / "neutral_coordination.yaml"
    drifted.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    with pytest.raises(AssertionError, match="has drifted"):
        _assert_fixture_matches_tracked_policy(TRACKED_RUNTIME_POLICY, drifted)


def test_client_shapes_refuse_unconfigured_remote_routes() -> None:
    with pytest.raises(ValueError, match="exactly 2 configured remote routes"):
        client_shapes_for_routes(
            {"local-claude": "opaque:local-claude", "local-codex": "opaque:local-codex"}
        )
