"""A deployment declares how strictly it is judged: ``readiness.profile`` (#3958).

PACT `readiness-profile-composed-declaration-3958`, part A of two. This change
only *declares* the profile and carries it through both load paths; it changes
no verdict. Part B makes ``okg dogfood status`` read it.

* A1  sealed composition: the exact composed ``readiness`` block per environment
* A2  a definition may declare ``profile`` and nothing environment-owned
* A3  verdict guard: ``okg dogfood status`` is the same with the declaration
* A4  lint: an invalid profile and an authored ``environment_policy`` are blockers
"""
from __future__ import annotations

import argparse
import copy
import json
import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.catalog.manifest_identity import (
    MANIFEST_FINGERPRINT_IN,
    manifest_fingerprint_for,
)
from okg.substrate.cli import dogfood as dogfood_cli
from okg.substrate.cli.deployment_readiness import (
    _ready_source_optimization_enforcement,
    _ready_source_optimization_latency_budget_enforcement,
)
from okg.substrate.deployment_lint import lint_deployment
from okg.substrate.deployments import DeploymentConfigError, resolve_deployment
from okg.substrate.deployments.environment import load_environment
from okg.substrate.source_sync_convergence import (
    readiness_source_groups_for_manifest,
)
from tests.mirror_scope import private_path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOYMENTS = REPO_ROOT / "deployments"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "deployment_environments"

CI_STAGING_SOURCE_GROUPS = {
    "ci": {
        "repo": ["git-history", "pact"],
        "test_artifacts": ["test-reports"],
        "dependencies": ["package-manifests"],
    }
}


def _sealed_values(tmp_path: Path) -> dict[str, str]:
    """Fake values for every secret reference the shipped descriptors name."""
    return {
        "OKG_STATE_ROOT": str(tmp_path / "state"),
        "OKG_WORKSPACE_SYNC_ROOT": str(REPO_ROOT),
        "OKG_WORKSPACE_STAGING_DSN": (
            "postgresql://postgres@127.0.0.1:55435/okg_workspace_staging"
        ),
        "OKG_WORKSPACE_PRODUCTION_DSN": (
            "postgresql://postgres@127.0.0.1:55434/okg_workspace"
        ),
        "OKG_CI_STAGING_SYNC_ROOT": str(tmp_path / "okg-ci-staging-sync"),
        "OKG_CI_PRODUCTION_SYNC_ROOT": str(tmp_path / "okg-ci-production-sync"),
        "OKG_CI_STAGING_STATE_ROOT": str(tmp_path / "okg-ci-staging-state"),
        "OKG_CI_PRODUCTION_STATE_ROOT": str(tmp_path / "okg-ci-production-state"),
        "OKG_CI_DEV_DSN": "postgresql://postgres@127.0.0.1:55437/okg_ci_dev",
        "OKG_CI_DSN": "postgresql://postgres@127.0.0.1:55436/okg_ci",
    }


def _enter_sealed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    environment_id: str,
    graph_deployment: str,
) -> None:
    """Set the three variables a sealed process is launched with."""
    descriptor = private_path(f"ops/environments/{environment_id}/environment.yaml")
    for name, value in _sealed_values(tmp_path).items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", str(descriptor))
    monkeypatch.setenv("OKG_RELEASE_ROOT", str(tmp_path / "release"))
    monkeypatch.setenv("OKG_DEPLOYMENT", graph_deployment)


def _sealed_readiness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    environment_id: str,
    graph_deployment: str,
) -> dict[str, Any]:
    _enter_sealed(monkeypatch, tmp_path, environment_id, graph_deployment)
    return dict(resolve_deployment().effective_raw["readiness"])


# --- A1: composition ---------------------------------------------------------


@pytest.mark.parametrize(
    ("environment_id", "graph_deployment", "expected"),
    [
        (
            "okg-workspace-production",
            "okg-workspace",
            {"profile": "dogfood", "environment_policy": "dogfood"},
        ),
        (
            "okg-workspace-staging",
            "okg-workspace-staging",
            {"profile": "dogfood", "environment_policy": "dogfood"},
        ),
        (
            "okg-ci-staging",
            "okg-ci-dev",
            {
                "profile": "ci",
                "environment_policy": "ci",
                "override_authority": "staging_fail_closed",
                "source_groups": CI_STAGING_SOURCE_GROUPS,
            },
        ),
        (
            "okg-ci-production",
            "okg-ci",
            {"profile": "ci", "environment_policy": "ci"},
        ),
    ],
)
def test_sealed_composition_yields_the_exact_readiness_block(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    environment_id: str,
    graph_deployment: str,
    expected: dict[str, Any],
) -> None:
    observed = _sealed_readiness(monkeypatch, tmp_path, environment_id, graph_deployment)
    assert observed == expected


# --- A2: a definition may declare the profile and nothing environment-owned --


def _fixture_with_definition_readiness(tmp_path: Path, readiness: Any) -> Path:
    environments = tmp_path / "environments"
    shutil.copytree(FIXTURES, environments)
    definition = environments / "shared-definition" / "definition.yaml"
    raw = yaml.safe_load(definition.read_text(encoding="utf-8"))
    raw["readiness"] = readiness
    definition.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return environments / "fixture-staging" / "environment.yaml"


def _load_fixture(descriptor: Path, tmp_path: Path):
    return load_environment(
        descriptor,
        environ={
            "TEST_STATE_ROOT": str(tmp_path / "state"),
            "TEST_STAGING_DSN": (
                "postgresql://postgres@127.0.0.1:55435/fixture_staging"
            ),
        },
    )


@pytest.mark.parametrize(
    "readiness",
    [
        {"override_authority": "staging_fail_closed"},
        {"source_groups": {"ci": {"repo": ["git-history"]}}},
        {"environment_policy": "dogfood"},
        {"profile": "ci", "environment_policy": "dogfood"},
        {"source_optimization": {"enforcement": "warn"}},
    ],
)
def test_a_definition_cannot_declare_environment_owned_readiness_keys(
    tmp_path: Path,
    readiness: dict[str, Any],
) -> None:
    descriptor = _fixture_with_definition_readiness(tmp_path, readiness)
    with pytest.raises(DeploymentConfigError, match="readiness declares"):
        _load_fixture(descriptor, tmp_path)


def test_a_definition_readiness_that_is_not_a_mapping_is_refused(
    tmp_path: Path,
) -> None:
    descriptor = _fixture_with_definition_readiness(tmp_path, "dogfood")
    with pytest.raises(DeploymentConfigError, match="readiness must be a mapping"):
        _load_fixture(descriptor, tmp_path)


def test_a_definition_that_declares_only_a_profile_loads_and_composes(
    tmp_path: Path,
) -> None:
    descriptor = _fixture_with_definition_readiness(tmp_path, {"profile": "standard"})
    environment = _load_fixture(descriptor, tmp_path)
    assert environment.manifest.effective_raw["readiness"] == {
        "profile": "standard",
        "environment_policy": "dogfood",
    }


def test_an_unknown_profile_value_still_loads_so_lint_can_report_it(
    tmp_path: Path,
) -> None:
    # Refusing the value at load would also stop MCP and ingest from starting.
    descriptor = _fixture_with_definition_readiness(tmp_path, {"profile": "general"})
    environment = _load_fixture(descriptor, tmp_path)
    assert environment.manifest.effective_raw["readiness"]["profile"] == "general"


def test_a_definition_without_readiness_composes_only_the_environment_policy(
    tmp_path: Path,
) -> None:
    environment = _load_fixture(
        FIXTURES / "fixture-staging" / "environment.yaml", tmp_path
    )
    assert environment.manifest.effective_raw["readiness"] == {
        "environment_policy": "dogfood"
    }


# --- A3: no verdict change ---------------------------------------------------

WORKSPACE = "okg-workspace"
GENERATION_ID = "okg:okg-workspace:branch:default:gen:20261021T000000000000Z:abc123456789"
RECORDED_ONE_WORKER = {
    "ok": True,
    "status": "single_executor",
    "process_count": 1,
    "root_count": 1,
    "roots": [{"pid": 4242, "ppid": 1, "command": "okg runtime worker"}],
    "processes": [],
    "split_executor": False,
}


def _recorded_live() -> dict[str, Any]:
    """A recorded healthy dogfood live result (the probes' output shape)."""
    return {
        "checks": [],
        "blockers": [],
        "warnings": [],
        "unknowns": [],
        "generation": {"generation_id": GENERATION_ID, "status": "published"},
        "mcp": {"ok": True, "pinned_generation_id": GENERATION_ID},
        "queues": {
            "active_queue_depth": 0,
            "dlq_count": 0,
            "dlq_status": "measured",
            "max_active_queue_depth": 0,
            "ok": True,
            "queue_depth": {"embed": 0, "enrich": 0, "ingest": 0, "publish": 0},
            "queue_health_measured": True,
            "status": "ready",
        },
        "code_drift": {"ok": True, "status": "current", "commits_behind": 0, "problems": []},
        "source_freshness": {},
        "doctor": {"ok": None, "status": "skipped"},
    }


def _run_workspace_status(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    deployment_dir: Path,
    stub_environment: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run the real status command; return (live-check kwargs, payload)."""
    seen: dict[str, Any] = {}
    if stub_environment:
        monkeypatch.setattr(
            dogfood_cli,
            "_deployment_ready_env_status",
            lambda _args, **_kw: (
                {"ok": True, "deployment_dir": str(deployment_dir)},
                "postgres://okg@127.0.0.1:1/okg_test_never_dialled",
            ),
        )

    def _live_checks(**kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return _recorded_live()

    monkeypatch.setattr(dogfood_cli, "_deployment_ready_live_checks", _live_checks)
    monkeypatch.setattr(
        dogfood_cli, "_plan_stale_source_sync_work_recovery", lambda **_kw: None
    )
    monkeypatch.setattr(
        dogfood_cli, "_attach_nomos_active_state", lambda live, **_kw: dict(live)
    )
    monkeypatch.setattr(
        dogfood_cli, "_attach_worker_code_identity", lambda live, **_kw: dict(live)
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_apply.deployment_apply_health",
        lambda **_kw: {"ok": True, "managed": True, "status": "ready"},
    )
    monkeypatch.setattr(
        dogfood_cli,
        "_runtime_worker_process_audit",
        lambda _deployment: copy.deepcopy(RECORDED_ONE_WORKER),
    )
    args = argparse.Namespace(
        deployment=WORKSPACE,
        dogfood_subcmd="status",
        details=False,
        skip_doctor=True,
        show_values=False,
        max_active_queue_depth=0,
        max_source_age_seconds=None,
        max_producer_age_seconds=None,
    )
    dogfood_cli._cmd_dogfood_status(args, as_json=True)
    payload = json.loads(capsys.readouterr().out)
    return seen, payload


def _comparable(kwargs: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    """Drop only what names the load path.

    That is the DSN, the ``environment`` block, and the one recovery command
    the real environment block recommends (the plain run stubs that block).
    """
    # Part B (#3958) adds `readiness_profile`, which says which load path
    # resolved the profile (`composed`), so it names the load path too.
    comparable = {
        key: copy.deepcopy(value)
        for key, value in payload.items()
        if key not in {"environment", "readiness_profile"}
    }
    comparable["recovery_commands"].pop("environment_recommended", None)
    return {
        "kwargs": {key: value for key, value in kwargs.items() if key != "dsn"},
        "payload": comparable,
    }


def test_the_workspace_manifests_declare_dogfood() -> None:
    plain = yaml.safe_load((DEPLOYMENTS / WORKSPACE / "deployment.yaml").read_text())
    sealed = yaml.safe_load((DEPLOYMENTS / WORKSPACE / "definition.yaml").read_text())
    assert plain["readiness"]["profile"] == "dogfood"
    assert sealed["readiness"] == {"profile": "dogfood"}


def test_dogfood_status_judges_the_workspace_as_dogfood_on_both_load_paths(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """The live checks get the dogfood profile and 1 h limit, plain or sealed.

    This passes on origin/dev too: declaring a profile must change nothing the
    status command does until part B reads it.
    """
    deployment_dir = DEPLOYMENTS / WORKSPACE
    plain_kwargs, plain_payload = _run_workspace_status(
        monkeypatch, capsys, deployment_dir=deployment_dir, stub_environment=True,
    )
    monkeypatch.undo()

    _enter_sealed(monkeypatch, tmp_path, "okg-workspace-production", WORKSPACE)
    sealed_kwargs, sealed_payload = _run_workspace_status(
        monkeypatch, capsys, deployment_dir=deployment_dir, stub_environment=False,
    )

    for kwargs in (plain_kwargs, sealed_kwargs):
        assert kwargs["readiness_profile"] == "dogfood"
        assert kwargs["max_source_age_seconds"] == 3600
        assert kwargs["deployment"] == WORKSPACE
    assert sealed_payload["environment"]["authority"]["mode"] == "sealed_environment"
    assert plain_payload["trust_level"] == sealed_payload["trust_level"] == "ready"
    plain = _comparable(plain_kwargs, plain_payload)
    sealed = _comparable(sealed_kwargs, sealed_payload)
    assert sealed["kwargs"] == plain["kwargs"]
    assert set(sealed["payload"]) == set(plain["payload"])
    for key in sorted(plain["payload"]):
        assert sealed["payload"][key] == plain["payload"][key], key


@pytest.mark.parametrize(
    ("environment_id", "graph_deployment", "profile"),
    [
        ("okg-workspace-production", "okg-workspace", "dogfood"),
        ("okg-ci-staging", "okg-ci-dev", "ci"),
    ],
)
def test_every_readiness_reader_ignores_the_declared_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    environment_id: str,
    graph_deployment: str,
    profile: str,
) -> None:
    """The readers of the composed block give the same answer without the new keys."""
    _enter_sealed(monkeypatch, tmp_path, environment_id, graph_deployment)
    composed = dict(resolve_deployment().effective_raw)
    assert composed["readiness"]["profile"] == profile
    before = copy.deepcopy(composed)
    for key in ("profile", "environment_policy"):
        before["readiness"].pop(key)

    for name in ("dogfood", "ci"):
        assert readiness_source_groups_for_manifest(
            composed, [name]
        ) == readiness_source_groups_for_manifest(before, [name])
    assert _ready_source_optimization_enforcement(
        composed
    ) == _ready_source_optimization_enforcement(before)
    assert _ready_source_optimization_latency_budget_enforcement(
        composed
    ) == _ready_source_optimization_latency_budget_enforcement(before)


def test_readiness_stays_outside_the_publish_fingerprint() -> None:
    assert "readiness" not in MANIFEST_FINGERPRINT_IN
    _, blocks = manifest_fingerprint_for(DEPLOYMENTS / WORKSPACE)
    assert "readiness" not in blocks


# --- A4: lint ----------------------------------------------------------------


def _lint_codes(
    tmp_path: Path,
    *,
    manifest_readiness: Any = None,
    definition_readiness: Any = None,
) -> dict[str, dict[str, Any]]:
    root = tmp_path / "deployments"
    directory = root / "lint-subject"
    directory.mkdir(parents=True)
    manifest: dict[str, Any] = {"name": "lint-subject"}
    if manifest_readiness is not None:
        manifest["readiness"] = manifest_readiness
    (directory / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    if definition_readiness is not None:
        (directory / "definition.yaml").write_text(
            yaml.safe_dump({
                "api_version": "okg.deployment-definition/v1",
                "id": "lint-subject",
                "readiness": definition_readiness,
            })
        )
    report = lint_deployment("lint-subject", deployments_root=root, env={})
    return {
        finding["code"]: finding
        for finding in report["findings"]
        if finding["code"].startswith("deployment.readiness.")
    }


@pytest.mark.parametrize("value", ["dogfod", "general", "", None, ["ci"]])
def test_lint_blocks_a_profile_outside_the_closed_set(
    tmp_path: Path, value: Any
) -> None:
    findings = _lint_codes(tmp_path, manifest_readiness={"profile": value})
    finding = findings["deployment.readiness.profile_invalid"]
    assert finding["severity"] == "blocker"
    assert finding["location"]["pointer"] == "/readiness/profile"
    assert "dogfood | ci | standard" == finding["expected"]


def test_lint_names_standard_for_general(tmp_path: Path) -> None:
    finding = _lint_codes(tmp_path, manifest_readiness={"profile": "general"})[
        "deployment.readiness.profile_invalid"
    ]
    assert "use 'standard'" in finding["message"]
    assert "smoke level" in finding["message"]
    other = _lint_codes(tmp_path / "other", manifest_readiness={"profile": "dogfod"})
    assert "use 'standard'" not in other["deployment.readiness.profile_invalid"]["message"]


@pytest.mark.parametrize("profile", ["dogfood", "ci", "standard"])
def test_lint_accepts_every_declarable_profile(tmp_path: Path, profile: str) -> None:
    assert _lint_codes(tmp_path, manifest_readiness={"profile": profile}) == {}
    # Part B (#3958) reports an undeclared profile, but a profile declared in
    # the sibling definition.yaml counts as declared.
    assert _lint_codes(tmp_path / "d", definition_readiness={"profile": profile}) == {}


def test_lint_blocks_an_authored_environment_policy(tmp_path: Path) -> None:
    findings = _lint_codes(
        tmp_path,
        manifest_readiness={"profile": "ci", "environment_policy": "dogfood"},
    )
    assert findings["deployment.readiness.environment_policy_authored"]["severity"] == "blocker"
    assert "deployment.readiness.profile_invalid" not in findings

    in_definition = _lint_codes(
        tmp_path / "d", definition_readiness={"environment_policy": "ci"}
    )
    finding = in_definition["deployment.readiness.environment_policy_authored"]
    assert finding["location"]["path"].endswith("definition.yaml")


def test_lint_reports_a_bad_profile_in_the_definition(tmp_path: Path) -> None:
    findings = _lint_codes(tmp_path, definition_readiness={"profile": "dogfod"})
    assert findings["deployment.readiness.profile_invalid"]["location"]["path"].endswith(
        "definition.yaml"
    )


@pytest.mark.parametrize("deployment", ["okg-workspace", "okg-ci", "okg-ci-dev"])
def test_the_declared_manifests_raise_neither_readiness_code(deployment: str) -> None:
    # Each declares its profile in deployment.yaml and in definition.yaml, so
    # this covers six files; lint reads both.
    from okg.substrate.deployments.readiness_profile import (
        readiness_profile_problem,
    )

    expected = "dogfood" if deployment == "okg-workspace" else "ci"
    for filename in ("deployment.yaml", "definition.yaml"):
        raw = yaml.safe_load((DEPLOYMENTS / deployment / filename).read_text())
        assert raw["readiness"]["profile"] == expected
        assert readiness_profile_problem(raw["readiness"]["profile"]) is None
    report = lint_deployment(deployment, deployments_root=DEPLOYMENTS, env={})
    assert [
        finding["code"]
        for finding in report["findings"]
        if finding["code"].startswith("deployment.readiness.")
    ] == []


def test_readiness_profile_problem_text() -> None:
    from okg.substrate.deployments.readiness_profile import (
        READINESS_PROFILES,
        readiness_profile_problem,
    )

    assert READINESS_PROFILES == ("dogfood", "ci", "standard")
    assert readiness_profile_problem("standard") is None
    assert "use 'standard'" in readiness_profile_problem("general")
    assert "'dogfod'" in readiness_profile_problem("dogfod")
