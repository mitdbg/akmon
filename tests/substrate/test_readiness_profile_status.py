"""`okg dogfood status` judges a deployment by its declared readiness profile (#3958).

PACT `deployment-readiness-profile-declared-3958`, part B of two. Part A
(`readiness-profile-composed-declaration-3958`) declared the profile; this part
reads it. The tests here need no database; the PostgreSQL end-to-end cases are
in ``tests/substrate/cli/test_readiness_profile_standard_db.py``.

* T1  the resolver, every precedence row
* T2  okg-workspace, plain path: the payload equals the recorded pre-change one
* T2b the same under the okg-workspace-production and -staging descriptors
* T3  trust: dogfood unchanged for every drift state; drift alone is no red
      under standard
* T6  a profile problem runs no live checks and is red by name; the
      environment readiness gate fails closed on a conflict
* T7  the doctor runs only on request
* T8  every printed command names the resolved profile and parses
* T9  dogfood checks the union of agent-context and baseline sources
* T10 the undeclared-profile lint, the template and the Helm values
* banner: an undeclared runtime under standard names no dead-letter queue
"""
from __future__ import annotations

import argparse
import contextlib
import copy
import io
import json
import shlex
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.cli import dogfood as dogfood_cli
from tests.mirror_scope import private_path
from tests.substrate.test_readiness_profile_declaration import (
    RECORDED_ONE_WORKER,
    _enter_sealed,
    _recorded_live,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOYMENTS = REPO_ROOT / "deployments"
WORKSPACE = "okg-workspace"
SNAPSHOT = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "readiness_profile"
    / "okg_workspace_dogfood_status.json"
)
#: Which graph deployment each recorded run judges.
RUNS = {
    "plain": WORKSPACE,
    "okg-workspace-production": WORKSPACE,
    "okg-workspace-staging": "okg-workspace-staging",
}


# --- the recorded okg-workspace run ------------------------------------------
#
# These helpers use only names that exist before this change, so the same
# harness produced the committed snapshot on the base commit (see the PACT
# evidence `workspace-snapshot-recording.log`).


def _pin_okg_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    """Print commands as `okg ...` whatever checkout runs the test."""
    import okg.substrate.support.user_hints as user_hints

    original = user_hints.okg_prefix
    for module in list(sys.modules.values()):
        if getattr(module, "okg_prefix", None) is original:
            monkeypatch.setattr(module, "okg_prefix", lambda: "okg")


def _normalized(value: Any) -> Any:
    if isinstance(value, Path):
        value = str(value)
    if isinstance(value, dict):
        return {key: _normalized(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalized(item) for item in value]
    if isinstance(value, str):
        return value.replace(str(REPO_ROOT), "<repo>")
    return value


def workspace_status_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    which: str,
    *,
    deployment_dir: Path | None = None,
    live: dict[str, Any] | None = None,
    subcommand: str = "status",
    process_audit: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run the real status command over a recorded live result.

    ``which`` is ``plain`` (the environment block stubbed) or an environment
    id (the three variables a sealed process is launched with are set).
    Returns ``(live-check keyword arguments, payload)``.
    """
    deployment = RUNS.get(which, WORKSPACE)
    directory = deployment_dir or private_path("deployments/okg-workspace")
    seen: dict[str, Any] = {}
    if which == "plain":
        monkeypatch.setattr(
            dogfood_cli,
            "_deployment_ready_env_status",
            lambda _args, **_kw: (
                {"ok": True, "deployment_dir": str(directory)},
                "postgres://okg@127.0.0.1:1/okg_test_never_dialled",
            ),
        )
    else:
        _enter_sealed(monkeypatch, tmp_path, which, deployment)
    _pin_okg_prefix(monkeypatch)

    def _live_checks(**kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return copy.deepcopy(live) if live is not None else _recorded_live()

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
        lambda _deployment: copy.deepcopy(process_audit or RECORDED_ONE_WORKER),
    )
    args = argparse.Namespace(
        deployment=deployment,
        dogfood_subcmd=subcommand,
        details=False,
        skip_doctor=False,
        show_values=False,
        max_active_queue_depth=0,
        max_source_age_seconds=None,
        max_producer_age_seconds=None,
    )
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        dogfood_cli.cmd_dogfood(args, as_json=True)
    return seen, json.loads(out.getvalue())


def comparable(kwargs: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    """Drop what names the load path or the checkout, and nothing else.

    The DSN, the ``environment`` block, the one recovery command the real
    environment block recommends, and this change's own ``readiness_profile``
    block. Paths are made relative to the repository root.
    """
    body = {
        key: copy.deepcopy(value)
        for key, value in payload.items()
        if key not in {"environment", "readiness_profile"}
    }
    body.get("recovery_commands", {}).pop("environment_recommended", None)
    return _normalized({
        "kwargs": {key: value for key, value in kwargs.items() if key != "dsn"},
        "payload": body,
    })


def record_snapshot(path: Path, tmp_root: Path) -> None:
    """Write the snapshot from the checkout on ``sys.path`` (base commit)."""
    recorded: dict[str, Any] = {}
    for which in RUNS:
        with pytest.MonkeyPatch.context() as monkeypatch:
            run_root = tmp_root / which
            run_root.mkdir(parents=True, exist_ok=True)
            kwargs, payload = workspace_status_run(monkeypatch, run_root, which)
            recorded[which] = comparable(kwargs, payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(recorded, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _snapshot() -> dict[str, Any]:
    return json.loads(SNAPSHOT.read_text(encoding="utf-8"))


# --- T1: the resolver ---------------------------------------------------------


def _resolve(readiness: Any, *, composed: bool) -> dict[str, Any]:
    from okg.substrate.deployments.readiness_profile import (
        declared_readiness_profile,
    )

    manifest = {} if readiness is None else {"readiness": readiness}
    return declared_readiness_profile(manifest, composed=composed)


@pytest.mark.parametrize(
    ("readiness", "composed", "name", "source"),
    [
        ({"profile": "dogfood"}, False, "dogfood", "declared"),
        ({"profile": "ci"}, False, "ci", "declared"),
        ({"profile": "standard"}, False, "standard", "declared"),
        (None, False, "standard", "default"),
        ({}, False, "standard", "default"),
        ({"source_groups": {}}, False, "standard", "default"),
        # The composed rows: M valid and E equal; E alone, including standard.
        ({"profile": "dogfood", "environment_policy": "dogfood"}, True, "dogfood", "declared"),
        ({"environment_policy": "dogfood"}, True, "dogfood", "environment"),
        ({"environment_policy": "ci"}, True, "ci", "environment"),
        ({"environment_policy": "standard"}, True, "standard", "environment"),
        # A policy outside the choosing set neither chooses nor conflicts.
        ({"environment_policy": "org-private"}, True, "standard", "default"),
        ({"profile": "ci", "environment_policy": "org-private"}, True, "ci", "declared"),
    ],
)
def test_the_resolver_chooses_by_one_precedence_rule(
    readiness: Any, composed: bool, name: str, source: str
) -> None:
    resolved = _resolve(readiness, composed=composed)
    assert resolved["problem"] is None
    assert (resolved["name"], resolved["source"]) == (name, source)


@pytest.mark.parametrize(
    ("readiness", "composed", "code"),
    [
        (
            {"profile": "dogfood", "environment_policy": "standard"},
            True,
            "deployment.readiness.profile_conflict",
        ),
        (
            {"profile": "standard", "environment_policy": "dogfood"},
            True,
            "deployment.readiness.profile_conflict",
        ),
        ({"profile": "general"}, False, "deployment.readiness.profile_invalid"),
        ({"profile": "dogfod"}, True, "deployment.readiness.profile_invalid"),
        ({"profile": None}, False, "deployment.readiness.profile_invalid"),
        (
            {"profile": "general", "environment_policy": "dogfood"},
            True,
            "deployment.readiness.profile_invalid",
        ),
        (
            {"environment_policy": "dogfood"},
            False,
            "deployment.readiness.environment_policy_authored",
        ),
        (
            {"profile": "ci", "environment_policy": "ci"},
            False,
            "deployment.readiness.environment_policy_authored",
        ),
    ],
)
def test_a_profile_that_cannot_be_resolved_is_a_named_problem(
    readiness: Any, composed: bool, code: str
) -> None:
    resolved = _resolve(readiness, composed=composed)
    assert resolved["name"] is None
    assert resolved["source"] is None
    assert resolved["problem"]["code"] == code
    assert resolved["problem"]["field"]


def test_general_is_invalid_and_the_message_names_standard() -> None:
    message = _resolve({"profile": "general"}, composed=False)["problem"]["message"]
    assert "use 'standard'" in message


def test_the_resolver_echoes_both_values() -> None:
    resolved = _resolve(
        {"profile": "dogfood", "environment_policy": "standard"}, composed=True
    )
    assert resolved["manifest_value"] == "dogfood"
    assert resolved["environment_policy"] == "standard"


def test_the_profile_age_limits_match_the_cli_constants() -> None:
    from okg.substrate.cli import deployment as deployment_cli
    from okg.substrate.deployments.readiness_profile import MAX_SOURCE_AGE_SECONDS

    assert MAX_SOURCE_AGE_SECONDS == {
        "dogfood": deployment_cli._READY_DOGFOOD_MAX_SOURCE_AGE_SECONDS,
        "ci": deployment_cli._READY_CI_MAX_SOURCE_AGE_SECONDS,
        "standard": deployment_cli._READY_DEFAULT_MAX_SOURCE_AGE_SECONDS,
    }
    assert MAX_SOURCE_AGE_SECONDS["standard"] == 14 * 24 * 3600


# --- T2 / T2b: okg-workspace is judged exactly as before ---------------------


def test_the_workspace_payload_equals_the_recorded_one_on_the_plain_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    kwargs, payload = workspace_status_run(monkeypatch, tmp_path, "plain")

    assert kwargs["readiness_profile"] == "dogfood"
    assert kwargs["max_source_age_seconds"] == 3600
    assert payload["readiness_profile"]["name"] == "dogfood"
    assert payload["readiness_profile"]["source"] == "declared"
    assert payload["readiness_profile"]["composed"] is False
    observed = comparable(kwargs, payload)
    expected = _snapshot()["plain"]
    assert observed["kwargs"] == expected["kwargs"]
    assert set(observed["payload"]) == set(expected["payload"])
    for key in sorted(expected["payload"]):
        assert observed["payload"][key] == expected["payload"][key], key


@pytest.mark.parametrize("environment_id", ["okg-workspace-production", "okg-workspace-staging"])
def test_the_workspace_payload_equals_the_recorded_one_on_the_sealed_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    environment_id: str,
) -> None:
    from okg.substrate.deployments import resolve_deployment

    kwargs, payload = workspace_status_run(monkeypatch, tmp_path, environment_id)

    assert resolve_deployment().effective_raw["readiness"] == {
        "profile": "dogfood",
        "environment_policy": "dogfood",
    }
    assert kwargs["readiness_profile"] == "dogfood"
    assert kwargs["max_source_age_seconds"] == 3600
    resolution = payload["readiness_profile"]
    assert (resolution["name"], resolution["source"]) == ("dogfood", "declared")
    assert resolution["composed"] is True
    assert resolution["environment_policy"] == "dogfood"
    observed = comparable(kwargs, payload)
    expected = _snapshot()[environment_id]
    assert observed["kwargs"] == expected["kwargs"]
    assert set(observed["payload"]) == set(expected["payload"])
    for key in sorted(expected["payload"]):
        assert observed["payload"][key] == expected["payload"][key], key


def _workspace_copy(tmp_path: Path, *, readiness: Any) -> Path:
    """okg-workspace's manifest in a temporary directory, readiness replaced."""
    source = private_path("deployments/okg-workspace/deployment.yaml")
    target = tmp_path / "deployments" / WORKSPACE
    target.mkdir(parents=True)
    raw = yaml.safe_load(source.read_text(encoding="utf-8"))
    if readiness is None:
        raw.pop("readiness", None)
    else:
        raw["readiness"] = readiness
    (target / "deployment.yaml").write_text(yaml.safe_dump(raw), encoding="utf-8")
    return target


def test_the_workspace_without_its_declaration_is_standard_and_warned(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    directory = _workspace_copy(tmp_path, readiness=None)

    kwargs, payload = workspace_status_run(
        monkeypatch, tmp_path, "plain", deployment_dir=directory
    )

    assert kwargs["readiness_profile"] == "standard"
    assert kwargs["max_source_age_seconds"] == 14 * 24 * 3600
    assert payload["readiness_profile"]["source"] == "default"
    warnings = {row["id"]: row for row in payload["warnings"]}
    defaulted = warnings["readiness.profile_defaulted"]
    assert "sync_root" in defaulted["message"]
    assert "nomos.deployment_posture.deployment_class" in defaulted["message"]
    # A warning: the defaulted profile changes nothing about trust.
    assert payload["trust_level"] == "ready"


def test_a_declared_standard_profile_is_not_warned_as_defaulted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    directory = _workspace_copy(tmp_path, readiness={"profile": "standard"})

    _kwargs, payload = workspace_status_run(
        monkeypatch, tmp_path, "plain", deployment_dir=directory
    )

    assert payload["readiness_profile"]["source"] == "declared"
    assert "readiness.profile_defaulted" not in {
        row["id"] for row in payload["warnings"]
    }


# --- T3: trust ----------------------------------------------------------------


def _drift_live(state: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """A healthy live result with one code-drift state, and its check row."""
    live = _recorded_live()
    drift = {
        "ok": {"ok": True, "status": "current", "commits_behind": 0},
        "behind": {"ok": False, "status": "behind", "commits_behind": 3},
        "misconfigured": {"ok": False, "status": "misconfigured", "commits_behind": None},
        "error": {"ok": False, "status": "error", "commits_behind": None},
    }[state]
    live["code_drift"] = drift
    return live, drift


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("ok", "ready"),
        ("behind", "stale_context"),
        ("misconfigured", "unavailable"),
        ("error", "unavailable"),
    ],
)
def test_dogfood_trust_is_unchanged_for_every_drift_state(
    state: str, expected: str
) -> None:
    live, drift = _drift_live(state)
    blockers = [] if state == "ok" else [{
        "id": "deployment.code_drift",
        "status": "blocker",
        "ok": False,
        "details": drift,
    }]
    for profile_kwargs in ({}, {"readiness_profile": "dogfood"}):
        assert dogfood_cli._trust_level(
            environment={"ok": True},
            resolved_dsn=True,
            live=live,
            blockers=list(blockers),
            unknowns=[],
            **profile_kwargs,
        ) == expected
        # The rule reads the drift block itself under dogfood, so a payload
        # without the blocker row is judged the same way.
        assert dogfood_cli._trust_level(
            environment={"ok": True},
            resolved_dsn=True,
            live=live,
            blockers=[],
            unknowns=[],
            **profile_kwargs,
        ) == expected


@pytest.mark.parametrize("profile", ["standard", "ci"])
@pytest.mark.parametrize("state", ["behind", "misconfigured", "error"])
def test_drift_alone_is_a_warning_and_never_moves_trust_outside_dogfood(
    profile: str, state: str
) -> None:
    live, _drift = _drift_live(state)
    assert dogfood_cli._trust_level(
        environment={"ok": True},
        resolved_dsn=True,
        live=live,
        blockers=[],
        unknowns=[],
        readiness_profile=profile,
    ) == "ready"


def test_a_real_outage_stays_red_under_standard() -> None:
    live = _recorded_live()
    for blocker_id in (
        "database.connectivity",
        "generation.latest_published",
        "mcp.graph_read",
        "deployment.apply",
        "database.identity",
        "sources.registry.unavailable",
    ):
        assert dogfood_cli._trust_level(
            environment={"ok": True},
            resolved_dsn=True,
            live=live,
            blockers=[{"id": blocker_id, "status": "blocker", "ok": False}],
            unknowns=[],
            readiness_profile="standard",
        ) == "unavailable", blocker_id
    unpublished = _recorded_live()
    unpublished["generation"] = {"status": "missing"}
    assert dogfood_cli._trust_level(
        environment={"ok": True},
        resolved_dsn=True,
        live=unpublished,
        blockers=[],
        unknowns=[],
        readiness_profile="standard",
    ) == "unavailable"


# --- T6: a profile problem ----------------------------------------------------


def _problem_dir(tmp_path: Path, readiness: Any) -> Path:
    directory = tmp_path / "problem-demo"
    directory.mkdir()
    (directory / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "problem-demo", "readiness": readiness}),
        encoding="utf-8",
    )
    return directory


def _status_without_live_checks(
    monkeypatch: pytest.MonkeyPatch,
    directory: Path,
) -> dict[str, Any]:
    called: list[str] = []

    def _no_live_checks(**_kwargs: Any) -> dict[str, Any]:
        called.append("live")
        raise AssertionError("a profile problem must run no live checks")

    monkeypatch.setattr(
        dogfood_cli,
        "_deployment_ready_env_status",
        lambda _args, **_kw: (
            {"ok": True, "deployment_dir": str(directory)},
            "postgres://okg@127.0.0.1:1/okg_test_never_dialled",
        ),
    )
    monkeypatch.setattr(dogfood_cli, "_deployment_ready_live_checks", _no_live_checks)
    args = argparse.Namespace(
        deployment=directory.name,
        dogfood_subcmd="status",
        details=False,
        skip_doctor=False,
        show_values=False,
        max_active_queue_depth=0,
        max_source_age_seconds=None,
        max_producer_age_seconds=None,
    )
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = dogfood_cli.cmd_dogfood(args, as_json=True)
    assert code == 1
    assert called == []
    return json.loads(out.getvalue())


def _assert_problem_payload(payload: dict[str, Any], *, code: str, field: str) -> None:
    from okg.substrate.chat.health_banner import read_health

    assert payload["trust_level"] == "unavailable"
    assert payload["ok"] is False
    assert [row["id"] for row in payload["blockers"]] == [
        "readiness.profile.unavailable"
    ]
    blocker = payload["blockers"][0]
    assert blocker["details"]["code"] == code
    assert blocker["details"]["field"] == field
    assert payload["next_action"]["code"] == "fix_readiness_profile"
    assert payload["next_action"]["field"] == field
    assert "--checks" not in payload["next_action"]["command"]
    for name, command in payload["recovery_commands"].items():
        assert "--checks" not in str(command), name
        assert "service render" not in str(command), name
    reading = read_health(payload)
    assert reading.state == "unavailable"
    assert reading.severity == "error"
    assert "published generation" in reading.conditions
    assert "source freshness (not reported)" in reading.conditions


@pytest.mark.parametrize(
    ("readiness", "code", "field"),
    [
        ({"profile": "general"}, "deployment.readiness.profile_invalid", "readiness.profile"),
        ({"profile": "dogfod"}, "deployment.readiness.profile_invalid", "readiness.profile"),
        (
            {"profile": "standard", "environment_policy": "standard"},
            "deployment.readiness.environment_policy_authored",
            "readiness.environment_policy",
        ),
    ],
)
def test_an_unresolvable_profile_runs_no_checks_and_is_red_by_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    readiness: dict[str, Any],
    code: str,
    field: str,
) -> None:
    directory = _problem_dir(tmp_path, readiness)

    payload = _status_without_live_checks(monkeypatch, directory)

    _assert_problem_payload(payload, code=code, field=field)
    files = payload["blockers"][0]["details"]["files"]
    assert files == [str(directory / "deployment.yaml")]


def test_a_manifest_that_does_not_parse_is_a_named_problem(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    directory = tmp_path / "problem-demo"
    directory.mkdir()
    (directory / "deployment.yaml").write_text("readiness: [unclosed\n", encoding="utf-8")

    payload = _status_without_live_checks(monkeypatch, directory)

    _assert_problem_payload(
        payload,
        code="deployment.readiness.profile_unreadable",
        field="readiness.profile",
    )


def test_a_conflict_on_the_sealed_path_names_both_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from okg.substrate.cli import deployment as deployment_cli

    directory = _problem_dir(tmp_path, {"profile": "dogfood"})
    descriptor = tmp_path / "environment.yaml"
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", str(descriptor))
    monkeypatch.setattr(
        deployment_cli,
        "_ready_manifest_and_origin",
        lambda _dir: (
            {"readiness": {"profile": "dogfood", "environment_policy": "standard"}},
            True,
        ),
    )

    payload = _status_without_live_checks(monkeypatch, directory)

    _assert_problem_payload(
        payload,
        code="deployment.readiness.profile_conflict",
        field="readiness.profile",
    )
    message = payload["blockers"][0]["message"]
    assert "'dogfood'" in message and "'standard'" in message
    assert payload["blockers"][0]["details"]["files"] == [
        str(directory / "definition.yaml"),
        str(descriptor),
    ]


def _environment_readiness_stubs(monkeypatch: pytest.MonkeyPatch) -> Any:
    from okg.substrate import environment_readiness as env

    ok_payload = {"ok": True, "status": "ready"}
    for name in (
        "environment_release_status",
        "environment_process_authority_status",
        "environment_service_installation_status",
        "environment_release_handoff_status",
        "_probe_pact_prerequisites",
        "_probe_webhook_owner",
        "_load_mcp_registration",
    ):
        monkeypatch.setattr(env, name, lambda *_a, **_k: dict(ok_payload))
    monkeypatch.setattr(
        env, "_probe_ui_http", lambda *_a, **_k: {**ok_payload, "generation_id": "gen:1"}
    )
    monkeypatch.setattr(
        env,
        "_probe_mcp_registration",
        lambda *_a, **_k: {
            **ok_payload,
            "release_authority_ok": True,
            "inspect_ok": True,
            "search_ok": True,
            "generation_aligned": True,
        },
    )
    monkeypatch.setattr(
        env,
        "_probe_database_readiness",
        lambda *_a, **_k: {
            "generation": {"ok": True, "status": "published", "generation_id": "gen:1"},
            "nomos_read_path": dict(ok_payload),
        },
    )
    monkeypatch.setattr(
        env,
        "_probe_operational_readiness",
        lambda *_a, **_k: {
            "checks": [{
                "id": "database.migrations", "status": "pass", "ok": True,
                "message": "migrated",
            }],
            "blockers": [], "warnings": [], "unknowns": [],
        },
    )
    return env


@pytest.mark.parametrize(
    ("definition_profile", "blocked"),
    [("standard", True), ("dogfood", False)],
)
def test_the_environment_readiness_gate_fails_closed_on_a_conflict(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    definition_profile: str,
    blocked: bool,
) -> None:
    from tests.substrate.test_readiness_profile_declaration import (
        _fixture_with_definition_readiness,
        _load_fixture,
    )

    descriptor = _fixture_with_definition_readiness(
        tmp_path, {"profile": definition_profile}
    )
    environment = _load_fixture(descriptor, tmp_path)
    env = _environment_readiness_stubs(monkeypatch)

    report = env.environment_readiness_report(environment, {})

    check_ids = [row["id"] for row in report["checks"]]
    if blocked:
        assert report["ok"] is False
        assert report["status"] == "blocked"
        blocker = {row["id"]: row for row in report["blockers"]}["readiness.profile"]
        assert "deployment.readiness.profile_conflict" in blocker["message"]
        assert blocker["details"]["problem"]["files"][1] == str(
            environment.descriptor_path
        )
    else:
        assert "readiness.profile" not in check_ids
        assert report["ok"] is True


# --- T7: the doctor runs only on request ---------------------------------------


@pytest.mark.parametrize(
    ("subcommand", "skip_doctor"),
    [("status", True), ("doctor", False)],
)
def test_the_doctor_runs_only_on_request_under_standard(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    subcommand: str,
    skip_doctor: bool,
) -> None:
    directory = _workspace_copy(tmp_path, readiness={"profile": "standard"})

    kwargs, _payload = workspace_status_run(
        monkeypatch, tmp_path, "plain", deployment_dir=directory, subcommand=subcommand
    )

    assert kwargs["readiness_profile"] == "standard"
    assert kwargs["skip_doctor"] is skip_doctor


# --- T8: printed commands -----------------------------------------------------


def _parse_okg_command(command: str) -> argparse.Namespace:
    from okg.substrate.cli.main import _build_parser

    words = shlex.split(command)
    if words[:3] == ["env", "-u", "OKG_DSN"]:
        words = words[3:]
    if words[:2] == ["uv", "run"]:
        words = words[2:]
    assert words[0] == "okg", command
    return _build_parser().parse_args(words[1:])


def _ready_commands(commands: dict[str, Any]) -> dict[str, str]:
    return {
        name: str(command)
        for name, command in commands.items()
        if isinstance(command, str) and " deployment ready " in f" {command} "
    }


@pytest.mark.parametrize("profile", ["dogfood", "ci", "standard"])
def test_every_printed_ready_command_names_the_resolved_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    profile: str,
) -> None:
    from okg.substrate.cli.deployment_readiness import (
        _ready_source_sync_recovery_commands,
    )

    directory = _workspace_copy(tmp_path, readiness={"profile": profile})

    _kwargs, payload = workspace_status_run(
        monkeypatch, tmp_path, "plain", deployment_dir=directory
    )
    commands = dict(payload["recovery_commands"])
    commands.update(
        _ready_source_sync_recovery_commands(
            deployment=WORKSPACE,
            max_producer_age_seconds=900,
            readiness_profile=profile,
        )
    )
    ready = _ready_commands(commands)
    assert "dogfood_ready_gate" in ready
    assert "check_ready" in ready
    for name, command in ready.items():
        args = _parse_okg_command(command)
        assert args.profile == profile, (name, command)
    renders = {name for name in commands if name.startswith("service_render_")}
    if profile == "dogfood":
        assert renders == {
            "service_render_launchd",
            "service_render_systemd",
            "service_render_docker_compose",
        }
    else:
        assert renders == set()


def _status_without_a_resolved_profile(
    monkeypatch: pytest.MonkeyPatch,
    env_status: Any,
) -> dict[str, Any]:
    def _no_live_checks(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("no live checks without an environment")

    monkeypatch.setattr(dogfood_cli, "_deployment_ready_env_status", env_status)
    monkeypatch.setattr(dogfood_cli, "_deployment_ready_live_checks", _no_live_checks)
    args = argparse.Namespace(
        deployment="probe-demo",
        dogfood_subcmd="status",
        details=False,
        skip_doctor=False,
        show_values=False,
        max_active_queue_depth=0,
        max_source_age_seconds=None,
        max_producer_age_seconds=None,
    )
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        assert dogfood_cli.cmd_dogfood(args, as_json=True) == 1
    return json.loads(out.getvalue())


def _raise_environment(_args: Any, **_kwargs: Any) -> Any:
    raise ValueError("deployment environment could not be read")


@pytest.mark.parametrize(
    "env_status",
    [
        _raise_environment,
        lambda _args, **_kw: ({"ok": False, "unresolved": ["OKG_DSN"]}, None),
    ],
    ids=["environment-raises", "environment-not-resolved"],
)
def test_no_printed_command_names_a_profile_when_none_was_resolved(
    monkeypatch: pytest.MonkeyPatch,
    env_status: Any,
) -> None:
    payload = _status_without_a_resolved_profile(monkeypatch, env_status)

    assert payload["trust_level"] == "unavailable"
    commands = payload["recovery_commands"]
    assert "dogfood_status" in commands
    for name, command in commands.items():
        assert "--checks" not in str(command), name
        assert "service render" not in str(command), name
    assert "--checks" not in str(payload["next_action"].get("command"))


def test_a_name_the_cli_refuses_gets_no_checks_flag() -> None:
    from okg.substrate.cli.deployment_readiness import (
        _ready_source_sync_recovery_commands,
    )

    commands = _ready_source_sync_recovery_commands(
        deployment="probe-demo",
        max_producer_age_seconds=900,
        readiness_profile="org-private",
    )
    assert "--checks" not in commands["check_ready"]
    _parse_okg_command(commands["check_ready"])


@pytest.mark.parametrize("command", ["deployment ready", "deployment up", "up"])
def test_checks_accepts_standard_everywhere_it_is_offered(command: str) -> None:
    from okg.substrate.cli.main import _build_parser

    words = [*command.split(), "probe-demo", "--checks", "standard"]
    args = _build_parser().parse_args(words)
    # `deployment ready` keeps the old `profile` dest; the up commands use
    # `readiness_profile` (their `profile` is the database target).
    dest = "profile" if command == "deployment ready" else "readiness_profile"
    assert getattr(args, dest) == "standard"


# --- T9: dogfood checks the union -----------------------------------------------


def _registry_entry(name: str, *, baseline: bool) -> dict[str, Any]:
    return {
        "module": "static_demo.source",
        "class": "StaticDemoSource",
        "ownership_id": f"union-demo.{name}",
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["path"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "required_for_baseline": baseline,
    }


def _union_dir(
    tmp_path: Path,
    *,
    sources: dict[str, bool],
    profile: str | None = None,
    workspace_modules: bool = False,
) -> Path:
    from okg.substrate.cli import deployment as deployment_cli

    directory = tmp_path / "union-demo"
    directory.mkdir()
    manifest: dict[str, Any] = {
        "name": "union-demo",
        "source_registry": "source_registry.yaml",
    }
    if workspace_modules:
        manifest["modules"] = sorted(deployment_cli._READY_WORKSPACE_GRAPH_MODULES)
    if profile is not None:
        manifest["readiness"] = {"profile": profile}
    (directory / "deployment.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")
    (directory / "source_registry.yaml").write_text(
        yaml.safe_dump({
            "sources": {
                name: _registry_entry(name, baseline=baseline)
                for name, baseline in sources.items()
            },
        }),
        encoding="utf-8",
    )
    return directory


def test_the_workspace_dogfood_groups_are_unchanged_and_not_empty() -> None:
    from okg.substrate.cli import deployment as deployment_cli

    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood",
        deployment=WORKSPACE,
        deployment_dir=private_path("deployments/okg-workspace"),
    )
    assert groups == deployment_cli._READY_AGENT_CONTEXT_SOURCE_GROUPS
    assert all(groups.values())


def test_backup_verify_style_dogfood_keeps_a_non_workspace_baseline_group(
    tmp_path: Path,
) -> None:
    from okg.substrate.cli import deployment as deployment_cli

    directory = _union_dir(tmp_path, sources={"docs": True, "notes": False})

    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood", deployment="union-demo", deployment_dir=directory
    )

    assert groups == {"declared_baseline": ("docs",)}


def test_a_module_detected_deployment_gets_both_groups(tmp_path: Path) -> None:
    from okg.substrate.cli import deployment as deployment_cli

    directory = _union_dir(
        tmp_path,
        sources={"code-structure": False, "docs": True},
        workspace_modules=True,
    )

    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood", deployment="union-demo", deployment_dir=directory
    )

    assert groups == {
        "code": ("code-structure",),
        "declared_baseline": ("docs",),
    }


def test_standard_checks_only_the_baseline_sources(tmp_path: Path) -> None:
    from okg.substrate.cli import deployment as deployment_cli

    directory = _union_dir(
        tmp_path, sources={"code-structure": False, "docs": True}
    )

    assert deployment_cli._ready_source_groups_for_profile(
        "standard", deployment="union-demo", deployment_dir=directory
    ) == {"declared_baseline": ("docs",)}


def test_standard_raises_on_a_registry_that_does_not_load(tmp_path: Path) -> None:
    from okg.substrate.cli import deployment as deployment_cli
    from okg.substrate.cli.deployment_readiness import SourceRegistryUnloadable

    directory = _union_dir(tmp_path, sources={"docs": True})
    (directory / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": {"docs": {"module": "x"}}}), encoding="utf-8"
    )

    with pytest.raises(SourceRegistryUnloadable):
        deployment_cli._ready_source_groups_for_profile(
            "standard", deployment="union-demo", deployment_dir=directory
        )
    # The dogfood union keeps the agent-context list instead (#3986).
    assert deployment_cli._ready_source_groups_for_profile(
        "dogfood", deployment="union-demo", deployment_dir=directory
    ) == deployment_cli._READY_AGENT_CONTEXT_SOURCE_GROUPS


@pytest.mark.parametrize(
    ("profile", "reported"),
    [("dogfood", True), (None, False), ("standard", False)],
)
def test_undeclared_agent_context_sources_are_reported_only_for_a_dogfood_manifest(
    tmp_path: Path,
    profile: str | None,
    reported: bool,
) -> None:
    from okg.substrate.cli import deployment as deployment_cli
    from okg.substrate.cli.deployment_readiness import (
        _ready_undeclared_agent_context_sources,
    )

    directory = _union_dir(
        tmp_path,
        sources={"code-structure": False},
        profile=profile,
        workspace_modules=True,
    )
    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood", deployment="union-demo", deployment_dir=directory
    )

    undeclared = _ready_undeclared_agent_context_sources(
        "dogfood",
        deployment="union-demo",
        deployment_dir=directory,
        source_groups=groups,
    )

    if reported:
        assert "openspec" in undeclared and "code-structure" not in undeclared
    else:
        assert undeclared == []


# --- T10: lint, template, Helm --------------------------------------------------


def _lint_findings(tmp_path: Path, manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    from okg.substrate.deployment_lint import lint_deployment

    root = tmp_path / "deployments"
    directory = root / "lint-subject"
    directory.mkdir(parents=True)
    (directory / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "lint-subject", **manifest}), encoding="utf-8"
    )
    report = lint_deployment("lint-subject", deployments_root=root, env={})
    return {
        finding["code"]: finding
        for finding in report["findings"]
        if finding["code"].startswith("deployment.readiness.")
    }


@pytest.mark.parametrize(
    ("manifest", "severity", "signal"),
    [
        ({}, "info", None),
        ({"sync_root": "/srv/checkout"}, "warning", "sync_root"),
        (
            {"nomos": {"deployment_posture": {"deployment_class": "dogfood_workspace"}}},
            "warning",
            "nomos.deployment_posture.deployment_class",
        ),
        (
            {"nomos": {"deployment_posture": {"readiness_profiles": ["dogfood"]}}},
            "warning",
            "nomos.deployment_posture.readiness_profiles",
        ),
    ],
)
def test_lint_reports_an_undeclared_profile(
    tmp_path: Path,
    manifest: dict[str, Any],
    severity: str,
    signal: str | None,
) -> None:
    findings = _lint_findings(tmp_path, manifest)
    finding = findings["deployment.readiness.profile_undeclared"]
    assert finding["severity"] == severity
    assert "judges this deployment as standard" in finding["message"]
    if signal is not None:
        assert signal in finding["message"]


@pytest.mark.parametrize(
    ("definition_readiness", "reported"),
    [({"profile": "dogfood"}, False), (None, True)],
)
def test_lint_counts_a_profile_declared_in_the_definition(
    tmp_path: Path,
    definition_readiness: Any,
    reported: bool,
) -> None:
    from okg.substrate.deployment_lint import lint_deployment

    root = tmp_path / "deployments"
    directory = root / "lint-subject"
    directory.mkdir(parents=True)
    (directory / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "lint-subject", "sync_root": "/srv/checkout"}),
        encoding="utf-8",
    )
    definition: dict[str, Any] = {
        "api_version": "okg.deployment-definition/v1",
        "id": "lint-subject",
    }
    if definition_readiness is not None:
        definition["readiness"] = definition_readiness
    (directory / "definition.yaml").write_text(
        yaml.safe_dump(definition), encoding="utf-8"
    )

    report = lint_deployment("lint-subject", deployments_root=root, env={})

    codes = {finding["code"] for finding in report["findings"]}
    assert ("deployment.readiness.profile_undeclared" in codes) is reported


def test_lint_is_quiet_about_a_declared_profile(tmp_path: Path) -> None:
    assert _lint_findings(
        tmp_path, {"readiness": {"profile": "standard"}, "sync_root": "/srv/checkout"}
    ) == {}


def test_the_workspace_graph_template_declares_standard() -> None:
    template = (
        REPO_ROOT
        / "src/okg/substrate/library/profiles/workspace-graph/deployment-defaults.yaml"
    )
    raw = yaml.safe_load(template.read_text(encoding="utf-8"))
    assert raw["readiness"] == {"profile": "standard"}


def test_the_helm_values_accept_standard() -> None:
    schema = json.loads(
        (REPO_ROOT / "ops/kubernetes/okg/values.schema.json").read_text(encoding="utf-8")
    )
    enum = schema["properties"]["okg"]["properties"]["readinessProfile"]["enum"]
    from okg.substrate.cli.deployment import READY_CHECKS_CHOICES

    assert tuple(enum) == READY_CHECKS_CHOICES


# --- runtime under standard, and the banner ------------------------------------

NO_WORKER = {"ok": False, "status": "not_observed", "roots": [], "split_executor": False}


def _runtime_dir(
    tmp_path: Path, runtime: Any, *, profile: str = "standard"
) -> Path:
    directory = _workspace_copy(tmp_path, readiness={"profile": profile})
    raw = yaml.safe_load((directory / "deployment.yaml").read_text(encoding="utf-8"))
    if runtime is None:
        raw.pop("runtime", None)
    else:
        raw["runtime"] = runtime
    (directory / "deployment.yaml").write_text(yaml.safe_dump(raw), encoding="utf-8")
    return directory


def test_an_undeclared_runtime_under_standard_reads_as_disabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from okg.substrate.chat.health_banner import read_health

    directory = _runtime_dir(tmp_path, None)

    _kwargs, payload = workspace_status_run(
        monkeypatch, tmp_path, "plain", deployment_dir=directory,
        process_audit=NO_WORKER,
    )

    assert payload["runtime_config"]["status"] == "not_declared"
    assert payload["runtime_worker"]["status"] == "not_declared_default_disabled"
    assert payload["trust_level"] == "ready"
    assert payload["blockers"] == []
    assert "runtime.enabled_not_declared" in {row["id"] for row in payload["warnings"]}
    assert read_health(payload).state == "healthy"


def test_a_worker_seen_while_the_runtime_is_undeclared_is_stale_context(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from okg.substrate.chat.health_banner import read_health

    directory = _runtime_dir(tmp_path, None)

    _kwargs, payload = workspace_status_run(
        monkeypatch, tmp_path, "plain", deployment_dir=directory,
        process_audit=RECORDED_ONE_WORKER,
    )

    blockers = {row["id"]: row for row in payload["blockers"]}
    assert set(blockers) == {"runtime.undeclared_but_running"}
    assert "not declared" in blockers["runtime.undeclared_but_running"]["message"]
    assert "runtime.enabled is false" not in blockers[
        "runtime.undeclared_but_running"
    ]["message"]
    assert payload["trust_level"] == "stale_context"
    reading = read_health(payload)
    assert reading.state == "degraded"
    assert "dead-letter queue (not reported)" not in reading.conditions


@pytest.mark.parametrize(
    ("profile", "runtime"),
    [
        ("dogfood", None),
        ("ci", None),
        ("standard", {"enabled": "maybe"}),
        ("standard", {"enabled": True}),
    ],
)
def test_the_worker_requirement_stays_where_the_runtime_is_not_read_as_off(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    profile: str,
    runtime: Any,
) -> None:
    directory = _runtime_dir(tmp_path, runtime, profile=profile)

    _kwargs, payload = workspace_status_run(
        monkeypatch, tmp_path, "plain", deployment_dir=directory,
        process_audit=NO_WORKER,
    )

    assert payload["runtime_worker"]["status"] not in {
        "disabled_by_config",
        "not_declared_default_disabled",
    }
    assert "runtime.worker_processes" in {row["id"] for row in payload["blockers"]}
    assert payload["trust_level"] == "unavailable"


def test_an_undeclared_runtime_names_no_dead_letter_queue_in_the_banner() -> None:
    from okg.substrate.chat.health_banner import read_health

    payload = {
        "trust_level": "stale_context",
        "read_authority": {"generation_id": "gen:1"},
        "source_freshness": {"ok": False},
        "doctor": {"ok": None, "status": "skipped"},
        "runtime_worker": {"status": "not_declared_default_disabled"},
        "blockers": [],
        "warnings": [],
    }
    assert "dead-letter queue (not reported)" not in read_health(payload).conditions
    payload["runtime_worker"] = {"status": "unknown"}
    assert "dead-letter queue (not reported)" in read_health(payload).conditions
