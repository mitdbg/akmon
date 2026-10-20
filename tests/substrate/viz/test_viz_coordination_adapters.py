"""Hermetic checks for the production setup-composition boundary."""

from __future__ import annotations

import atexit
import hashlib
import json
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from okg.substrate.library.agent_runtimes.coordination_client import (
    ClientRegistry,
    ClientRoute,
    ClientWorkspace,
)
from okg.viz import coordination_adapters as adapters
from tests.substrate.viz.neutral_routes import (
    REMOTE_ROUTES,
    ROUTE_NAMES,
    use_neutral_route_policy,
)


DEPLOYMENT = "okg-workspace"
CHANGE_ID = "agent-coordination-operations"
TASK_ID = "task.coord-operations.setup-composition"
DSN = "postgresql://operator:secret@db.invalid/okg"


def _synthetic_chief_repository() -> Path:
    """A throwaway chief repository carrying the one PACT record these tests bind.

    The setup authority names a change and task in the chief's OWN
    repository, and the adapter reads that record from there. These tests
    therefore build the repository they need instead of binding to this
    repository's PACT records, which do not ship in the public copy.
    """

    root = Path(tempfile.mkdtemp(prefix="okg-coord-chief-")).resolve() / "chief"
    atexit.register(shutil.rmtree, root.parent, True)
    manifest = root / "pact" / "changes" / CHANGE_ID / "pact.yaml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        yaml.safe_dump(
            {
                "pact_version": 5,
                "id": CHANGE_ID,
                "title": "Coordination setup fixture",
                "tier": 1,
                "why": "Fixture record for the setup-composition tests.",
                "affected_code": [],
                "requirements": [
                    {"id": "req.fixture.setup", "statement": "Setup binds."}
                ],
                "tasks": [
                    {
                        "id": TASK_ID,
                        "requirement": "req.fixture.setup",
                        "verification": "These tests.",
                    }
                ],
                "evidence": [],
                "decisions": [],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    git = [
        "git", "-C", str(root),
        "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid",
        # Import-time setup: no signing prompt or global hook may interfere.
        "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null",
    ]
    subprocess.run([*git, "init", "-q", "-b", "main"], check=True)
    subprocess.run([*git, "add", "pact"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "fixture"], check=True)
    return root


ROOT = _synthetic_chief_repository()
REMOTE_A, REMOTE_B = REMOTE_ROUTES


@pytest.fixture(autouse=True)
def _neutral_route_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    use_neutral_route_policy(monkeypatch, ROOT)


def _authority_payload(*, route: str = "local-codex", **overrides: object) -> dict:
    payload: dict[str, object] = {
        "schema": adapters.SETUP_AUTHORITY_SCHEMA,
        "deployment": DEPLOYMENT,
        "pact": {"change_id": CHANGE_ID, "task_id": TASK_ID},
        "chief": {
            "project_id": "project-okg",
            "repo_path": str(ROOT),
            "worktree_path": str(ROOT.parent / ".tmp-coordination-worktree"),
            "git_branch": "codex/agent-coordination-protocols-redesign",
            "graph_base_branch": "main",
            "authenticated_host": "127.0.0.1",
            "ssh_auth_fingerprint": "sha256:" + "1" * 64,
            "source_address": "127.0.0.1",
            "target_address": "127.0.0.1",
            "target_port": 22,
            "route": route,
            "grant_id": "grant-chief",
        },
    }
    for key, value in overrides.items():
        payload[key] = value
    return payload


def _write_authority(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "setup-authority.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)
    return path


def _client_registry() -> ClientRegistry:
    routes = {
        name: ClientRoute(
            route=name,
            host="local" if name.startswith("local-") else f"{name}.internal",
            user="okg",
            identity_file="/etc/okg/coordination/id_ed25519",
            known_hosts_file="/etc/okg/coordination/known_hosts",
        )
        for name in ROUTE_NAMES
    }
    workspaces = {
        "grant-chief": ClientWorkspace("grant-chief", "local-codex"),
        **{
            f"grant-{name}": ClientWorkspace(f"grant-{name}", name)
            for name in ROUTE_NAMES
            if name != "local-codex"
        },
    }
    return ClientRegistry(routes=routes, workspaces=workspaces)


def _client_registry_with_fingerprint(fingerprint: str) -> dict:
    base = _client_registry()
    return {
        "routes": {
            name: {
                "route": route.route,
                "host": route.host,
                "user": route.user,
                "identity_file": route.identity_file,
                "known_hosts_file": route.known_hosts_file,
                "port": route.port,
                "ssh_auth_fingerprint": fingerprint,
            }
            for name, route in base.routes.items()
        },
        "workspaces": {
            grant: {"grant_id": workspace.grant_id, "route": workspace.route}
            for grant, workspace in base.workspaces.items()
        },
    }


def _server_registry(
    *,
    deployment_name: str = DEPLOYMENT,
    repo_root: Path = ROOT,
    expected_git_commit: str = "a" * 40,
    dsn: str = DSN,
) -> dict:
    return {
        "routes": {
            name: {
                "route": name,
                "deployment_name": deployment_name,
                "repo_root": str(repo_root),
                "expected_git_commit": expected_git_commit,
                "dsn": dsn,
            }
            for name in ROUTE_NAMES
        }
    }


@dataclass
class _InjectedProbes:
    """Read-only probe seam used to keep adapter tests database-free."""

    calls: list[str]
    authority_digest: str | None = None

    def _call(self, name: str, value: dict) -> dict:
        self.calls.append(name)
        return value

    def git(self):
        return self._call(
            "git",
            {
                "git_ready": True,
                "git_binding": {
                    "branch": "codex/agent-coordination-protocols-redesign",
                    "base_commit": "a" * 40,
                    "head_commit": "a" * 40,
                    "worktree": "opaque:worktree",
                },
            },
        )

    def pact(self):
        return self._call(
            "pact",
            {
                "pact_binding": {
                    "change_id": CHANGE_ID,
                    "task_id": TASK_ID,
                    "intent_digest": "sha256:" + "b" * 64,
                }
            },
        )

    def deployment(self):
        return self._call(
            "deployment", {"deployment_exists": True, "deployment_ready": True}
        )

    def graph(self):
        return self._call(
            "graph",
            {
                "okg_ready": True,
                "graph_binding": {"generation": "generation-1", "branch": "main"},
            },
        )

    def native_control(self):
        return self._call(
            "native_control",
            {
                "ledger_empty": True,
                "operator_control_available": True,
                "identity_ready": True,
            },
        )

    def capacity(self):
        return self._call(
            "capacity",
            {"capacity": {"ok": True, "capacity_digest": "sha256:" + "c" * 64}},
        )

    def runtime(self):
        return self._call(
            "runtime",
            {
                "credential_references": {
                    name: "${OKG_AGENT_SSH_GRANT}"
                    for name in ROUTE_NAMES
                },
                "runtime_binding": {
                    "route_ref": "opaque:local-codex",
                    "config_digest": "sha256:" + "d" * 64,
                },
                "setup_authority_digest": self.authority_digest,
            },
        )

    def route(self):
        return self._call(
            "route",
            {
                "local_only": True,
                "host_reachability": {
                    name: True for name in ROUTE_NAMES
                },
            },
        )


def test_load_setup_authority_is_closed_and_binds_tracked_pact(tmp_path: Path):
    path = _write_authority(tmp_path, _authority_payload())

    authority = adapters.load_setup_authority(
        path,
        expected_deployment=DEPLOYMENT,
        repo_root=ROOT,
    )

    assert authority.deployment == DEPLOYMENT
    assert authority.route == "local-codex"
    assert authority.grant_id == "grant-chief"
    assert authority.credential_id == "ssh-auth:sha256:" + "1" * 64
    assert authority.config_digest.startswith("sha256:")
    assert str(ROOT) not in authority.config_digest


@pytest.mark.parametrize(
    "branch",
    ["feature/valid-target", "release-2026.08"],
)
def test_load_setup_authority_accepts_valid_git_refs(tmp_path: Path, branch: str):
    payload = _authority_payload()
    payload["chief"]["git_branch"] = branch
    path = _write_authority(tmp_path, payload)

    authority = adapters.load_setup_authority(
        path,
        expected_deployment=DEPLOYMENT,
        repo_root=ROOT,
    )

    assert authority.git_branch == branch


@pytest.mark.parametrize(
    "branch",
    [
        "feature..target",
        "feature/",
        "feature.",
        "feature.lock",
        "feature//target",
        "feature@{target}",
    ],
)
def test_invalid_git_ref_is_rejected_before_bundle_construction(
    tmp_path: Path, branch: str
):
    payload = _authority_payload()
    payload["chief"]["git_branch"] = branch
    path = _write_authority(tmp_path, payload)

    with pytest.raises(adapters.ProductionSetupUnavailable) as loaded:
        adapters.load_setup_authority(
            path,
            expected_deployment=DEPLOYMENT,
            repo_root=ROOT,
        )
    assert str(loaded.value) == "setup authority has invalid chief.git_branch"

    with pytest.raises(adapters.ProductionSetupUnavailable) as built:
        adapters.build_production_setup_adapters(
            dsn=DSN,
            deployment_name=DEPLOYMENT,
            repo_root=ROOT,
            config_path=path,
            client_registry_path=tmp_path / "registry.json",
            client_registry_loader=lambda path, *, expected_uid: _client_registry(),
            server_registry_loader=lambda: _server_registry(),
            setup_probes=_InjectedProbes([]),
        )
    assert str(built.value) == "setup authority has invalid chief.git_branch"


def test_invalid_graph_base_ref_is_rejected_at_authority_load(tmp_path: Path):
    payload = _authority_payload()
    payload["chief"]["graph_base_branch"] = "main..archive"
    path = _write_authority(tmp_path, payload)

    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        adapters.load_setup_authority(
            path,
            expected_deployment=DEPLOYMENT,
            repo_root=ROOT,
        )
    assert str(raised.value) == "setup authority has invalid chief.graph_base_branch"


@pytest.mark.parametrize(
    "worktree_path",
    [ROOT / ".git" / "coordination-worktree", ROOT / "source-tree"],
)
def test_setup_authority_rejects_worktree_inside_repository(
    tmp_path: Path, worktree_path: Path
):
    payload = _authority_payload()
    payload["chief"]["worktree_path"] = str(worktree_path)
    path = _write_authority(tmp_path, payload)

    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        adapters.load_setup_authority(
            path,
            expected_deployment=DEPLOYMENT,
            repo_root=ROOT,
        )
    assert str(raised.value) == "setup repository/worktree paths overlap"


def test_setup_authority_rejects_repository_inside_worktree(tmp_path: Path):
    payload = _authority_payload()
    payload["chief"]["worktree_path"] = str(ROOT.parent)
    path = _write_authority(tmp_path, payload)

    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        adapters.load_setup_authority(
            path,
            expected_deployment=DEPLOYMENT,
            repo_root=ROOT,
        )
    assert str(raised.value) == "setup repository/worktree paths overlap"


@pytest.mark.parametrize(
    "mutation", ["missing", "mode", "symlink", "extra", "secret", "oversized"]
)
def test_load_setup_authority_fails_closed(tmp_path: Path, mutation: str):
    path = _write_authority(tmp_path, _authority_payload())
    if mutation == "missing":
        path.unlink()
    elif mutation == "mode":
        path.chmod(0o640)
    elif mutation == "symlink":
        target = tmp_path / "real.json"
        target.write_bytes(path.read_bytes())
        target.chmod(0o600)
        path.unlink()
        path.symlink_to(target)
    elif mutation == "extra":
        path = _write_authority(tmp_path, {**_authority_payload(), "unexpected": True})
    elif mutation == "secret":
        path = _write_authority(tmp_path, {**_authority_payload(), "secret": "inline"})
    else:
        path.write_bytes(b"{" + b" " * adapters.MAX_SETUP_AUTHORITY_BYTES + b"}")
        path.chmod(0o600)

    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        adapters.load_setup_authority(
            path, expected_deployment=DEPLOYMENT, repo_root=ROOT
        )
    assert str(raised.value) == "setup authority has unexpected fields" or mutation in {
        "missing",
        "mode",
        "symlink",
        "secret",
        "oversized",
    }
    assert str(ROOT) not in str(raised.value)


def test_build_bundle_uses_injected_readers_and_keeps_fragments_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    config_path = _write_authority(tmp_path, _authority_payload())
    probes = _InjectedProbes([])
    before_probe_files = set(tmp_path.iterdir())

    def load_registry(path, *, expected_uid):
        assert expected_uid == 0
        return _client_registry()

    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=load_registry,
        server_registry_loader=lambda: _server_registry(),
        setup_probes=probes,
    )

    assert isinstance(bundle, adapters.ProductionSetupAdapters)
    assert bundle.setup_probes is probes
    fragments = {
        name: getattr(bundle.setup_probes, name)()
        for name in ("git", "pact", "runtime", "route")
    }
    encoded = json.dumps(fragments, sort_keys=True)
    assert DSN not in encoded
    assert "secret" not in encoded
    assert str(ROOT) not in encoded
    assert str(config_path) not in encoded
    assert "db.invalid" not in encoded
    assert probes.calls == ["git", "pact", "runtime", "route"]
    assert set(tmp_path.iterdir()) == before_probe_files


def test_route_probe_reports_bounded_reachability_without_host_leaks(tmp_path: Path):
    config_path = _write_authority(tmp_path, _authority_payload())
    calls: list[tuple[str, int]] = []

    def load_registry(path, *, expected_uid):
        assert expected_uid == 0
        return _client_registry()

    def checker(host: str, port: int) -> bool:
        calls.append((host, port))
        return host != f"{REMOTE_B}.internal"

    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=load_registry,
        server_registry_loader=lambda: _server_registry(),
        reachability_checker=checker,
    )
    fragment = bundle.setup_probes.route()

    assert fragment["host_reachability"] == {
        "local-claude": True,
        "local-codex": True,
        REMOTE_A: True,
        REMOTE_B: False,
    }
    assert calls == [
        ("local", 22),
        ("local", 22),
        (f"{REMOTE_A}.internal", 22),
        (f"{REMOTE_B}.internal", 22),
    ]
    encoded = json.dumps(fragment)
    assert f"{REMOTE_B}.internal" not in encoded
    assert DSN not in encoded


def test_git_probe_binds_target_branch_and_reports_redacted_collisions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    payload = _authority_payload()
    worktree = tmp_path / "target-worktree"
    worktree.mkdir()
    (worktree / "existing.txt").write_text("collision", encoding="utf-8")
    payload["chief"]["worktree_path"] = str(worktree)
    payload["chief"]["git_branch"] = "branch-target"
    config_path = _write_authority(tmp_path, payload)

    class Probe:
        def __init__(self, **_kwargs):
            pass

        def capture_snapshot(self):
            return {
                "head_sha": "a" * 40,
                "tree_sha": "b" * 40,
                "branch": "branch-current",
                "dirty_manifest": {},
            }

    class Git:
        def branch_exists(self, _repo, branch):
            assert branch == "branch-target"
            return True

    class Graph:
        @classmethod
        def from_store(cls, store):
            return cls(store)

        def __init__(self, _store):
            pass

        def branch_exists(self, deployment, branch):
            assert deployment == DEPLOYMENT
            assert branch == "branch-target"
            return True

    class Store:
        def close(self):
            pass

    monkeypatch.setattr(adapters, "GitScopeProbe", Probe)
    monkeypatch.setattr(adapters, "ProductionGitWorkspaceAdapter", Git)
    monkeypatch.setattr(adapters, "ProductionGraphWorkspaceAdapter", Graph)
    monkeypatch.setattr(adapters, "_connect_graph_store", lambda _dsn: Store())
    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=lambda path, *, expected_uid: _client_registry(),
        server_registry_loader=lambda: _server_registry(),
        setup_probes=None,
    )

    fragment = bundle.setup_probes.git()
    assert fragment["git_binding"]["branch"] == "branch-target"
    assert fragment["git_binding"]["base_commit"] == "a" * 40
    assert fragment["configuration_collisions"] == [
        "git_target_branch_exists",
        "git_target_worktree_exists",
        "graph_target_branch_exists",
    ]
    encoded = json.dumps(fragment)
    assert str(worktree) not in encoded
    assert "existing.txt" not in encoded

    # The Git probe is the sole owner of this aggregate field.  Composing its
    # redacted fragment with the other read-only authorities must therefore
    # produce a readable, explicitly blocked plan rather than an authority
    # disagreement.
    from okg.substrate.pact.coordination_operations import build_setup_plan
    from okg.viz import coordination_services

    probes = _InjectedProbes([])
    snapshot = {
        name: getattr(probes, name)()
        for name in coordination_services._SETUP_PROBE_NAMES
    }
    snapshot["git"] = fragment
    state = coordination_services._compose_setup_state(snapshot)
    plan = build_setup_plan(ROOT, DEPLOYMENT, probe=state)
    assert plan.read_ok is True
    assert plan.ready is False
    assert plan.checks["configuration_collision_free"] is False
    assert "configuration_collision" in plan.blocked_by
    plan_encoded = json.dumps(plan.to_json())
    assert str(worktree) not in plan_encoded
    assert "existing.txt" not in plan_encoded


def test_chief_and_pair_bind_deterministically_to_plan_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    config_path = _write_authority(tmp_path, _authority_payload())

    def load_registry(path, *, expected_uid):
        assert expected_uid == 0
        return _client_registry()

    native = adapters._load_native_module(DEPLOYMENT)
    monkeypatch.setattr(adapters, "_load_native_module", lambda _name: native)
    probes = _InjectedProbes([])
    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=load_registry,
        server_registry_loader=lambda: _server_registry(),
        setup_probes=probes,
    )
    probes.authority_digest = adapters.load_setup_authority(
        config_path,
        expected_deployment=DEPLOYMENT,
        repo_root=ROOT,
    ).config_digest
    snapshot = {
        "git": bundle.setup_probes.git(),
        "graph": bundle.setup_probes.graph(),
        "runtime": bundle.setup_probes.runtime(),
    }
    plan = {"schema": "setup-plan", "plan_digest": "sha256:" + "e" * 64}
    first = bundle.chief_resolver(
        plan=plan, snapshot=snapshot, deployment_name=DEPLOYMENT
    )
    second = bundle.chief_resolver(
        plan=plan, snapshot=snapshot, deployment_name=DEPLOYMENT
    )
    assert first == second
    assert first.assignment_kind == "chief"
    assert first.assignment_generation == 1
    assert first.assignment_id == second.assignment_id
    assert first.credential_id == "ssh-auth:sha256:" + "1" * 64
    assert first.source_address == "127.0.0.1"
    assert first.source_port is None
    assert first.target_address == "127.0.0.1"
    assert first.target_port == 22
    assert "renew" in first.allowed_event_types

    changed = bundle.chief_resolver(
        plan={**plan, "plan_digest": "sha256:" + "f" * 64},
        snapshot=snapshot,
        deployment_name=DEPLOYMENT,
    )
    assert changed.assignment_id != first.assignment_id
    pair = bundle.pair_request_factory(
        chief_request=first,
        snapshot=snapshot,
        deployment_name=DEPLOYMENT,
    )
    assert pair.assignment_id == first.assignment_id
    assert pair.provision_attempt_id == adapters._provision_attempt_id(first)
    assert pair.authenticated_host == "127.0.0.1"
    assert pair.mcp_route == "local-codex"
    changed_pair = bundle.pair_request_factory(
        chief_request=changed,
        snapshot=snapshot,
        deployment_name=DEPLOYMENT,
    )
    assert changed_pair.provision_attempt_id != pair.provision_attempt_id

    class Store:
        close_calls = 0

        def close(self):
            self.close_calls += 1

    store = Store()
    monkeypatch.setattr(adapters, "_connect_graph_store", lambda _dsn: store)
    lifecycle = bundle.lifecycle_factory(pair)
    lifecycle.close()
    lifecycle.close()
    assert store.close_calls == 1

    with pytest.raises(adapters.ProductionSetupUnavailable):
        bundle.chief_resolver(plan=plan, snapshot=snapshot, deployment_name="other")


def test_production_lifecycle_surfaces_sanitized_close_failure():
    class Store:
        def close(self):
            raise RuntimeError("dsn=postgresql://secret@host/db")

    lifecycle = adapters.ProductionSetupLifecycle(Store())
    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        lifecycle.close()
    assert str(raised.value) == "setup graph handle could not close"
    assert "postgresql" not in str(raised.value)


def test_factories_refuse_physical_registry_rotation_after_plan_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    config_path = _write_authority(tmp_path, _authority_payload())
    native = adapters._load_native_module(DEPLOYMENT)
    monkeypatch.setattr(adapters, "_load_native_module", lambda _name: native)
    probes = _InjectedProbes([])
    server = _server_registry()
    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=lambda path, *, expected_uid: _client_registry(),
        server_registry_loader=lambda: server,
        setup_probes=probes,
    )
    probes.authority_digest = adapters.load_setup_authority(
        config_path,
        expected_deployment=DEPLOYMENT,
        repo_root=ROOT,
    ).config_digest
    snapshot = {
        "git": bundle.setup_probes.git(),
        "graph": bundle.setup_probes.graph(),
        "runtime": bundle.setup_probes.runtime(),
    }
    plan = {"schema": "setup-plan", "plan_digest": "sha256:" + "e" * 64}
    chief = bundle.chief_resolver(
        plan=plan, snapshot=snapshot, deployment_name=DEPLOYMENT
    )
    pair = bundle.pair_request_factory(
        chief_request=chief, snapshot=snapshot, deployment_name=DEPLOYMENT
    )

    class Store:
        def close(self):
            pass

    monkeypatch.setattr(adapters, "_connect_graph_store", lambda _dsn: Store())
    lifecycle = bundle.lifecycle_factory(pair)
    server["routes"][REMOTE_A]["expected_git_commit"] = "b" * 40

    with pytest.raises(adapters.ProductionSetupUnavailable) as chief_raised:
        bundle.chief_resolver(plan=plan, snapshot=snapshot, deployment_name=DEPLOYMENT)
    assert str(chief_raised.value) == "operator server route Git binding is stale"
    with pytest.raises(adapters.ProductionSetupUnavailable) as pair_raised:
        bundle.pair_request_factory(
            chief_request=chief,
            snapshot=snapshot,
            deployment_name=DEPLOYMENT,
        )
    assert str(pair_raised.value) == "operator server route Git binding is stale"
    with pytest.raises(adapters.ProductionSetupUnavailable) as lifecycle_raised:
        lifecycle.stage_or_adopt(pair)
    assert str(lifecycle_raised.value) == "operator server route Git binding is stale"


@pytest.mark.parametrize("mutation", ["symlink", "unwritable", "git_head"])
def test_final_stage_fence_rejects_worktree_or_source_rotation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
):
    worktree_parent = tmp_path / "worktree-parent"
    worktree_parent.mkdir()
    payload = _authority_payload()
    payload["chief"]["worktree_path"] = str(worktree_parent / "target")
    config_path = _write_authority(tmp_path, payload)
    native = adapters._load_native_module(DEPLOYMENT)
    monkeypatch.setattr(adapters, "_load_native_module", lambda _name: native)
    probes = _InjectedProbes([])
    server = _server_registry()
    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=lambda path, *, expected_uid: _client_registry(),
        server_registry_loader=lambda: server,
        setup_probes=probes,
    )
    probes.authority_digest = adapters.load_setup_authority(
        config_path,
        expected_deployment=DEPLOYMENT,
        repo_root=ROOT,
    ).config_digest
    snapshot = {
        "git": bundle.setup_probes.git(),
        "graph": bundle.setup_probes.graph(),
        "runtime": bundle.setup_probes.runtime(),
    }
    plan = {"schema": "setup-plan", "plan_digest": "sha256:" + "e" * 64}
    chief = bundle.chief_resolver(
        plan=plan, snapshot=snapshot, deployment_name=DEPLOYMENT
    )
    pair = bundle.pair_request_factory(
        chief_request=chief, snapshot=snapshot, deployment_name=DEPLOYMENT
    )

    class Store:
        def close(self):
            pass

    monkeypatch.setattr(adapters, "_connect_graph_store", lambda _dsn: Store())
    lifecycle = bundle.lifecycle_factory(pair)
    stage_calls: list[object] = []

    def stage_or_adopt(_self, request):
        stage_calls.append(request)
        return object()

    monkeypatch.setattr(
        adapters.PairedWorkspaceLifecycle, "stage_or_adopt", stage_or_adopt
    )
    replacement = tmp_path / "replacement-parent"
    replacement.mkdir()
    moved = tmp_path / "moved-parent"
    try:
        if mutation == "symlink":
            worktree_parent.rename(moved)
            worktree_parent.symlink_to(replacement, target_is_directory=True)
        elif mutation == "unwritable":
            worktree_parent.chmod(0o500)
        else:
            monkeypatch.setattr(
                adapters, "_read_source_git_head", lambda _repo: "b" * 40
            )
        with pytest.raises(adapters.ProductionSetupUnavailable):
            lifecycle.stage_or_adopt(pair)
        assert stage_calls == []
    finally:
        if mutation == "symlink":
            worktree_parent.unlink()
            moved.rename(worktree_parent)
        else:
            worktree_parent.chmod(0o700)


def test_route_mismatch_is_rejected_before_bundle_installation(
    tmp_path: Path,
):
    config_path = _write_authority(
        tmp_path,
        _authority_payload(route="local-unconfigured"),
    )

    with pytest.raises(adapters.ProductionSetupUnavailable):
        adapters.build_production_setup_adapters(
            dsn=DSN,
            deployment_name=DEPLOYMENT,
            repo_root=ROOT,
            config_path=config_path,
            client_registry_path=tmp_path / "registry.json",
            client_registry_loader=lambda path, *, expected_uid: _client_registry(),
            server_registry_loader=lambda: _server_registry(),
            setup_probes=_InjectedProbes([]),
        )


def test_four_logical_routes_must_resolve_to_four_distinct_physical_routes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    config_path = _write_authority(tmp_path, _authority_payload())
    policy = adapters.coordination_policy(ROOT)
    duplicate_route = {
        name: "opaque:local-codex" for name in ROUTE_NAMES
    }
    monkeypatch.setattr(
        adapters,
        "coordination_policy",
        lambda _root: replace(policy, client_routes=duplicate_route),
    )

    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        adapters.build_production_setup_adapters(
            dsn=DSN,
            deployment_name=DEPLOYMENT,
            repo_root=ROOT,
            config_path=config_path,
            client_registry_path=tmp_path / "registry.json",
            client_registry_loader=lambda path, *, expected_uid: _client_registry(),
            server_registry_loader=lambda: _server_registry(),
            setup_probes=_InjectedProbes([]),
        )
    assert str(raised.value) == "tracked client routes are not physically distinct"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("deployment_name", "other-deployment"),
        ("repo_root", "/var/empty/not-the-trusted-checkout"),
    ],
)
def test_every_forced_command_route_binds_exact_deployment_and_repo(
    tmp_path: Path, field: str, value: str
):
    config_path = _write_authority(tmp_path, _authority_payload())
    server = _server_registry()
    server["routes"][REMOTE_A][field] = value

    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        adapters.build_production_setup_adapters(
            dsn=DSN,
            deployment_name=DEPLOYMENT,
            repo_root=ROOT,
            config_path=config_path,
            client_registry_path=tmp_path / "registry.json",
            client_registry_loader=lambda path, *, expected_uid: _client_registry(),
            server_registry_loader=lambda: server,
            setup_probes=_InjectedProbes([]),
        )
    assert str(ROOT) not in str(raised.value)
    assert "/var/empty" not in str(raised.value)


def test_selected_forced_command_route_must_bind_service_database(
    tmp_path: Path,
):
    config_path = _write_authority(tmp_path, _authority_payload())
    server = _server_registry()
    server["routes"][REMOTE_A]["dsn"] = (
        "postgresql://route:other-secret@other.invalid/other-ledger"
    )

    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        adapters.build_production_setup_adapters(
            dsn=DSN,
            deployment_name=DEPLOYMENT,
            repo_root=ROOT,
            config_path=config_path,
            client_registry_path=tmp_path / "registry.json",
            client_registry_loader=lambda path, *, expected_uid: _client_registry(),
            server_registry_loader=lambda: server,
            setup_probes=_InjectedProbes([]),
        )
    assert "other.invalid" not in str(raised.value)
    assert "other-secret" not in str(raised.value)


def test_selected_forced_command_route_accepts_equivalent_database_dsn_forms(
    tmp_path: Path,
):
    config_path = _write_authority(tmp_path, _authority_payload())
    equivalent = "host=db.invalid port=5432 dbname=okg user=route password=different"
    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=lambda path, *, expected_uid: _client_registry(),
        server_registry_loader=lambda: _server_registry(dsn=equivalent),
        setup_probes=_InjectedProbes([]),
    )

    assert isinstance(bundle, adapters.ProductionSetupAdapters)


def test_forced_command_route_commit_is_bound_to_current_setup_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    config_path = _write_authority(tmp_path, _authority_payload())
    server = _server_registry(expected_git_commit="b" * 40)

    class Probe:
        def __init__(self, **_kwargs):
            pass

        def capture_snapshot(self):
            return {
                "head_sha": "a" * 40,
                "tree_sha": "b" * 40,
                "branch": "branch-current",
                "dirty_manifest": {},
            }

    class Git:
        def branch_exists(self, _repo, _branch):
            return False

    class Graph:
        @classmethod
        def from_store(cls, store):
            return cls(store)

        def __init__(self, _store):
            pass

        def branch_exists(self, _deployment, _branch):
            return False

    class Store:
        def close(self):
            pass

    monkeypatch.setattr(adapters, "GitScopeProbe", Probe)
    monkeypatch.setattr(adapters, "ProductionGitWorkspaceAdapter", Git)
    monkeypatch.setattr(adapters, "ProductionGraphWorkspaceAdapter", Graph)
    monkeypatch.setattr(adapters, "_connect_graph_store", lambda _dsn: Store())
    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=lambda path, *, expected_uid: _client_registry(),
        server_registry_loader=lambda: server,
        setup_probes=None,
    )

    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        bundle.setup_probes.git()
    assert str(ROOT) not in str(raised.value)


def test_client_identity_fingerprint_matches_native_server_evidence_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    identity_file = tmp_path / "id_ed25519"
    identity_file.write_text("private-key-placeholder", encoding="utf-8")
    identity_file.chmod(0o600)
    public_line = "ssh-ed25519 QUJD comment-that-is-not-hashed"
    monkeypatch.setattr(
        adapters.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=public_line),
    )

    observed = adapters._derive_identity_fingerprint(identity_file)
    expected_line = b"publickey ssh-ed25519 QUJD"
    expected = "sha256:" + hashlib.sha256(expected_line).hexdigest()

    assert observed == expected


@pytest.mark.parametrize("route", ["local-codex", REMOTE_A])
def test_selected_client_route_identity_fingerprint_must_match_authority(
    tmp_path: Path, route: str
):
    config_path = _write_authority(tmp_path, _authority_payload())
    registry = _client_registry_with_fingerprint("sha256:" + "2" * 64)
    registry["routes"][route]["ssh_auth_fingerprint"] = "sha256:" + "3" * 64

    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        adapters.build_production_setup_adapters(
            dsn=DSN,
            deployment_name=DEPLOYMENT,
            repo_root=ROOT,
            config_path=config_path,
            client_registry_path=tmp_path / "registry.json",
            client_registry_loader=lambda path, *, expected_uid: registry,
            server_registry_loader=lambda: _server_registry(),
            setup_probes=_InjectedProbes([]),
        )
    assert "333333" not in str(raised.value)


def test_selected_client_identity_fingerprints_accept_matching_registry(
    tmp_path: Path,
):
    config_path = _write_authority(tmp_path, _authority_payload())
    fingerprint = "sha256:" + "1" * 64
    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=lambda path, *, expected_uid: (
            _client_registry_with_fingerprint(fingerprint)
        ),
        server_registry_loader=lambda: _server_registry(),
        setup_probes=_InjectedProbes([]),
    )

    assert isinstance(bundle, adapters.ProductionSetupAdapters)


def test_worktree_destination_preflight_allows_absent_target(tmp_path: Path):
    adapters._preflight_worktree_destination(
        tmp_path / "missing" / "target", repo_root=ROOT
    )


def test_worktree_destination_preflight_rejects_unwritable_boundary(tmp_path: Path):
    boundary = tmp_path / "blocked"
    boundary.mkdir()
    boundary.chmod(0o500)
    try:
        with pytest.raises(adapters.ProductionSetupUnavailable):
            adapters._preflight_worktree_destination(boundary / "target")
    finally:
        boundary.chmod(0o700)


def test_worktree_destination_preflight_rejects_unwritable_existing_target(
    tmp_path: Path,
):
    target = tmp_path / "target"
    target.mkdir()
    target.chmod(0o500)
    try:
        with pytest.raises(adapters.ProductionSetupUnavailable):
            adapters._preflight_worktree_destination(target)
    finally:
        target.chmod(0o700)


def test_worktree_destination_preflight_rejects_symlinked_parent(tmp_path: Path):
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)

    with pytest.raises(adapters.ProductionSetupUnavailable):
        adapters._preflight_worktree_destination(link / "target")


def test_runtime_plan_digest_covers_physical_client_and_server_routes(
    tmp_path: Path,
):
    config_path = _write_authority(tmp_path, _authority_payload())
    client = _client_registry()
    server = _server_registry()
    client.routes["unrelated"] = ClientRoute(
        route="unrelated",
        host="other.internal",
        user="other",
        identity_file="/etc/other/id_ed25519",
        known_hosts_file="/etc/other/known_hosts",
    )
    client.workspaces["grant-unrelated"] = ClientWorkspace(
        "grant-unrelated", "unrelated"
    )
    server["routes"]["unrelated"] = {
        "route": "unrelated",
        "deployment_name": "other-deployment",
        "repo_root": "/var/other-repo",
        "expected_git_commit": "b" * 40,
        "dsn": "postgresql://other:secret@other.invalid/other",
    }

    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=lambda path, *, expected_uid: client,
        server_registry_loader=lambda: server,
        setup_probes=None,
    )
    first = bundle.setup_probes.runtime()
    first_digest = first["runtime_binding"]["config_digest"]

    client.routes["unrelated"] = ClientRoute(
        route="unrelated",
        host="drifted-other.internal",
        user="drifted",
        identity_file="/etc/drifted/id_ed25519",
        known_hosts_file="/etc/drifted/known_hosts",
    )
    server["routes"]["unrelated"].update(
        {
            "deployment_name": "drifted-deployment",
            "repo_root": "/var/drifted-repo",
            "expected_git_commit": "c" * 40,
            "dsn": "postgresql://drifted:secret@drifted.invalid/drifted",
        }
    )
    unrelated = bundle.setup_probes.runtime()
    assert unrelated["runtime_binding"]["config_digest"] == first_digest

    client.routes[REMOTE_B] = ClientRoute(
        route=REMOTE_B,
        host="changed.internal",
        user="okg",
        identity_file="/etc/okg/coordination/id_ed25519",
        known_hosts_file="/etc/okg/coordination/known_hosts",
    )
    server["routes"][REMOTE_A]["expected_git_commit"] = "b" * 40
    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        bundle.setup_probes.runtime()
    assert str(raised.value) == "operator route registry changed; restart is required"


def test_registry_rotation_between_git_and_runtime_requires_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    config_path = _write_authority(tmp_path, _authority_payload())
    server = _server_registry()

    class Probe:
        def __init__(self, **_kwargs):
            pass

        def capture_snapshot(self):
            return {
                "head_sha": "a" * 40,
                "tree_sha": "b" * 40,
                "branch": "branch-current",
                "dirty_manifest": {},
            }

    class Git:
        def branch_exists(self, _repo, _branch):
            return False

    class Graph:
        @classmethod
        def from_store(cls, store):
            return cls(store)

        def __init__(self, _store):
            pass

        def branch_exists(self, _deployment, _branch):
            return False

    class Store:
        def close(self):
            pass

    monkeypatch.setattr(adapters, "GitScopeProbe", Probe)
    monkeypatch.setattr(adapters, "ProductionGitWorkspaceAdapter", Git)
    monkeypatch.setattr(adapters, "ProductionGraphWorkspaceAdapter", Graph)
    monkeypatch.setattr(adapters, "_connect_graph_store", lambda _dsn: Store())
    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=lambda path, *, expected_uid: _client_registry(),
        server_registry_loader=lambda: server,
        setup_probes=None,
    )
    bundle.setup_probes.git()
    server["routes"][REMOTE_A]["expected_git_commit"] = "b" * 40

    with pytest.raises(adapters.ProductionSetupUnavailable) as raised:
        bundle.setup_probes.runtime()
    assert str(raised.value) == "operator route registry changed; restart is required"


def test_authority_digest_is_pinned_for_chief_and_pair_factories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    config_path = _write_authority(tmp_path, _authority_payload())
    native = adapters._load_native_module(DEPLOYMENT)
    monkeypatch.setattr(adapters, "_load_native_module", lambda _name: native)
    probes = _InjectedProbes([])
    bundle = adapters.build_production_setup_adapters(
        dsn=DSN,
        deployment_name=DEPLOYMENT,
        repo_root=ROOT,
        config_path=config_path,
        client_registry_path=tmp_path / "registry.json",
        client_registry_loader=lambda path, *, expected_uid: _client_registry(),
        server_registry_loader=lambda: _server_registry(),
        setup_probes=probes,
    )
    probes.authority_digest = adapters.load_setup_authority(
        config_path,
        expected_deployment=DEPLOYMENT,
        repo_root=ROOT,
    ).config_digest
    snapshot = {
        "git": bundle.setup_probes.git(),
        "graph": bundle.setup_probes.graph(),
        "runtime": bundle.setup_probes.runtime(),
    }
    plan = {"schema": "setup-plan", "plan_digest": "sha256:" + "e" * 64}
    with pytest.raises(adapters.ProductionSetupUnavailable):
        bundle.chief_resolver(
            plan=plan,
            snapshot={"git": snapshot["git"], "graph": snapshot["graph"]},
            deployment_name=DEPLOYMENT,
        )
    chief = bundle.chief_resolver(
        plan=plan, snapshot=snapshot, deployment_name=DEPLOYMENT
    )

    replacement = _authority_payload()
    replacement["chief"]["project_id"] = "project-replaced"
    config_path.write_text(json.dumps(replacement), encoding="utf-8")
    config_path.chmod(0o600)

    with pytest.raises(adapters.ProductionSetupUnavailable):
        bundle.chief_resolver(plan=plan, snapshot=snapshot, deployment_name=DEPLOYMENT)
    with pytest.raises(adapters.ProductionSetupUnavailable):
        bundle.pair_request_factory(
            chief_request=chief,
            snapshot=snapshot,
            deployment_name=DEPLOYMENT,
        )
