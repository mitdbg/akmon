"""Review fixes for the supervised UI read token (issue #3278).

F2: the readiness probe must prove the UI port belongs to the supervised
process BEFORE it sends the token, so a foreign listener never receives it.
F1: a sensitive deployment with the UI on and no ``ui.auth_token_env`` is
refused before the UI is installed or started. R1: the sensitivity is read
from the directory the environment pins, the one the running UI resolves.
R2: only install, start and restart refuse; render, stop, status and logs work.
F6: the previous-release rebuild reads ``--auth-token-env`` from the installed
command, like ``--host`` and ``--port``, instead of copying the current one.
"""
from __future__ import annotations

import http.server
import subprocess
import sys
import threading
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import okg.substrate.environment_readiness as environment_readiness
import okg.substrate.environment_services as environment_services
from okg.substrate.deployments import DeploymentConfigError
from okg.substrate.environment_services import render_environment_services
from tests.mirror_scope import is_public_copy, private_path
from tests.substrate.test_environment_services import (
    _environment_fixture,
    _load,
)
from tests.substrate.test_environment_ui_auth import (
    _UI_TOKEN_ENV,
    _UI_TOKEN_VALUE,
    _declare_ui_token,
    _ui_unit,
)


def test_readiness_ui_probe_never_sends_the_token_to_a_foreign_listener(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A different process on the UI port must not receive the credential."""
    received: list[dict[str, str]] = []

    class Foreign(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib handler name
            received.append({k.lower(): v for k, v in self.headers.items()})
            body = b'{"ok": true, "ready": true}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: Any) -> None:
            return None

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Foreign)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    # The supervised UI "pid" is a real process, but not the listener.
    sleeper = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        root, values = _environment_fixture(tmp_path)
        _declare_ui_token(root, "fixture-staging")
        loaded = _load(root, "fixture-staging", values)
        environment = replace(
            loaded, ui=replace(loaded.ui, port=server.server_address[1])
        )
        monkeypatch.setenv(_UI_TOKEN_ENV, _UI_TOKEN_VALUE)
        processes = {
            "services": {
                environment.ui.service: {
                    "ok": True,
                    "authority": {
                        "pid": sleeper.pid,
                        "process_start_identity": "whatever",
                    },
                }
            }
        }

        status = environment_readiness._probe_ui_http(
            environment, processes=processes, timeout_seconds=2
        )

        assert status["ok"] is False
        assert status["status"] == "foreign_or_unready"
        assert status["owner_matches"] is False
        assert received == [], "the foreign listener was contacted"
        assert _UI_TOKEN_VALUE not in repr(status)
    finally:
        sleeper.kill()
        sleeper.wait()
        server.shutdown()
        server.server_close()


def test_readiness_ui_probe_with_no_supervised_process_sends_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    _declare_ui_token(root, "fixture-staging")
    environment = _load(root, "fixture-staging", values)
    monkeypatch.setenv(_UI_TOKEN_ENV, _UI_TOKEN_VALUE)

    def never(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("no request may be sent without an owner proof")

    monkeypatch.setattr(environment_readiness, "open_validated_url", never)
    status = environment_readiness._probe_ui_http(environment, timeout_seconds=1)
    assert status["status"] == "foreign_or_unready"
    assert status["ok"] is False


def _write_pinned_manifest(root: Path, nomos_block: str) -> Path:
    """Write ``deployment.yaml`` into the directory the fixture environment's
    descriptor pins (its definition directory) and return that directory."""
    pinned = root / "shared-definition"
    (pinned / "deployment.yaml").write_text(nomos_block, encoding="utf-8")
    return pinned


def _isolate_name_lookup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Make a lookup of the deployment BY NAME find nothing, as it does from an
    operator's shell that does not hold the environment's pinned directory."""
    import okg.substrate.nomos.runtime_enforcement as enforcement

    empty = tmp_path / "no-deployments-here"
    empty.mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(empty))
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    enforcement._clear_nomos_enforcement_mode_cache()


def test_sensitivity_is_read_from_the_environments_pinned_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The check follows the manifest the running UI resolves, not a lookup of
    the deployment name from the operator's shell (review finding R1)."""
    import okg.substrate.nomos.runtime_enforcement as enforcement

    root, values = _environment_fixture(tmp_path)
    _isolate_name_lookup(monkeypatch, tmp_path)
    pinned = _write_pinned_manifest(
        root, "nomos:\n  runtime_enforcement: enforce\n"
    )
    environment = _load(root, "fixture-staging", values)
    assert environment.manifest.deployment_dir == pinned.resolve()

    # A lookup by name, as the old plan-time check did, sees nothing.
    assert (
        enforcement.deployment_requires_read_auth(environment.graph_deployment_name)
        is False
    )

    with pytest.raises(DeploymentConfigError) as refused:
        environment_services.require_ui_token_for_sensitive_deployment(environment)
    message = str(refused.value)
    assert "ui.auth_token_env" in message
    assert "credential_refs" in message
    assert environment.ui.service in message

    # With the token declared the same deployment passes and renders it.
    _declare_ui_token(root, "fixture-staging")
    declared = _load(root, "fixture-staging", values)
    environment_services.require_ui_token_for_sensitive_deployment(declared)
    assert f"--auth-token-env {_UI_TOKEN_ENV}" in _ui_unit(declared, values)


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_audit_and_enforce_manifests_refuse_a_tokenless_ui(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    root, values = _environment_fixture(tmp_path)
    _isolate_name_lookup(monkeypatch, tmp_path)
    _write_pinned_manifest(root, f"nomos:\n  runtime_enforcement: {mode}\n")
    environment = _load(root, "fixture-staging", values)
    with pytest.raises(DeploymentConfigError, match="ui.auth_token_env"):
        environment_services.require_ui_token_for_sensitive_deployment(environment)


def test_non_sensitive_manifest_without_a_ui_token_is_allowed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    _isolate_name_lookup(monkeypatch, tmp_path)
    _write_pinned_manifest(root, "nomos:\n  runtime_enforcement: 'off'\n")
    environment = _load(root, "fixture-staging", values)
    environment_services.require_ui_token_for_sensitive_deployment(environment)
    assert "--auth-token-env" not in _ui_unit(environment, values)


def test_unreadable_pinned_manifest_refuses_a_tokenless_ui(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A manifest the running UI could not parse makes the mode unknown; the
    UI would refuse to start, so the install must refuse too."""
    root, values = _environment_fixture(tmp_path)
    _isolate_name_lookup(monkeypatch, tmp_path)
    _write_pinned_manifest(root, "nomos: [unclosed\n")
    environment = _load(root, "fixture-staging", values)
    with pytest.raises(DeploymentConfigError, match="cannot tell whether"):
        environment_services.require_ui_token_for_sensitive_deployment(environment)


def test_only_install_and_start_actions_refuse_a_tokenless_sensitive_ui(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rendering and the stop, status and logs actions keep working on a
    sensitive environment with no token, so an operator can inspect and stop
    it (review finding R2). Install, start and restart refuse."""
    root, values = _environment_fixture(tmp_path)
    _isolate_name_lookup(monkeypatch, tmp_path)
    _write_pinned_manifest(root, "nomos:\n  runtime_enforcement: enforce\n")
    environment = _load(root, "fixture-staging", values)

    # Rendering the plan is allowed.
    plan = render_environment_services(
        environment, adapter="systemd", systemd_scope="user", environ=values
    )

    ran: list[Sequence[str]] = []

    def runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        ran.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    for action in ("install", "start", "restart"):
        with pytest.raises(DeploymentConfigError, match="ui.auth_token_env"):
            environment_services.execute_environment_service_action(
                plan, action=action, environment=environment, runner=runner
            )
    with pytest.raises(DeploymentConfigError, match="ui.auth_token_env"):
        environment_services.install_environment_services(environment, plan)
    assert ran == [], "a refused action must not run any command"

    for action in ("stop", "status", "logs"):
        try:
            environment_services.execute_environment_service_action(
                plan, action=action, environment=environment, runner=runner
            )
        except DeploymentConfigError as other:
            # Another precondition may stop the action in this bare fixture;
            # the token requirement must not be the reason.
            assert "ui.auth_token_env" not in str(other), action


@pytest.mark.parametrize(
    ("action", "refused"),
    [
        ("render", False),
        ("status", False),
        ("stop", False),
        ("logs", False),
        ("install", True),
        ("start", True),
        ("restart", True),
    ],
)
def test_service_cli_refuses_only_install_and_start_without_a_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    action: str,
    refused: bool,
) -> None:
    from okg.substrate.cli.main import main

    root, values = _environment_fixture(tmp_path)
    _isolate_name_lookup(monkeypatch, tmp_path)
    _write_pinned_manifest(root, "nomos:\n  runtime_enforcement: enforce\n")
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    rc = main([
        "--json", "environment", "service", action, "fixture-staging",
        "--environments-root", str(root), "--adapter", "systemd",
    ])
    captured = capsys.readouterr()
    output = captured.out + captured.err
    if refused:
        assert rc == 2, output
        assert "ui.auth_token_env" in output
    else:
        assert "ui.auth_token_env" not in output, output
        if action == "render":
            assert rc == 0, output


_FIXTURE_DB_PASSWORD = "test"  # a database password nothing connects with
_ENVIRONMENTS_DIR = Path(__file__).resolve().parents[2] / "ops" / "environments"


def _shipped_environment_ids() -> list[str]:
    """Every environment the development repository ships, read from disk.

    ``ops/environments`` is private to the development repository and does not
    ship, so in the public copy this is empty. In the development repository a
    missing directory is an error, never a quiet skip. Reading the ids keeps
    private environment names out of this file.
    """
    if not _ENVIRONMENTS_DIR.is_dir():
        if is_public_copy():
            return []
        raise FileNotFoundError(
            f"{_ENVIRONMENTS_DIR} is missing in the development repository"
        )
    return sorted(
        path.name
        for path in _ENVIRONMENTS_DIR.iterdir()
        if (path / "environment.yaml").is_file()
    )


def _environment_params(*, skip_prefix: str | None = None) -> list[Any]:
    """The shipped ids as test parameters; one skipped parameter in the public copy."""
    ids = [
        name
        for name in _shipped_environment_ids()
        if skip_prefix is None or not name.startswith(skip_prefix)
    ]
    if ids:
        return ids
    return [
        pytest.param(
            "none",
            marks=pytest.mark.skip(
                reason="private environment descriptors are not in the public copy"
            ),
        )
    ]


def _shipped_values(tmp_path: Path, descriptor: Path) -> dict[str, str]:
    """Environment values that let one shipped descriptor load: every
    ``${NAME}`` placeholder becomes a path and every ``env:NAME`` database
    secret becomes a DSN."""
    import re

    text = descriptor.read_text(encoding="utf-8")
    values = {
        name: str(tmp_path / name.lower())
        for name in re.findall(r"\$\{([A-Z0-9_]+)\}", text)
    }
    import yaml

    database = yaml.safe_load(text)["database"]
    secret = str(database["dsn_secret_ref"]).removeprefix("env:")
    values[secret] = (
        f"postgresql://postgres:{_FIXTURE_DB_PASSWORD}@127.0.0.1:{database['host_port']}"
        f"/{database['name']}"
    )
    return values


def test_every_shipped_environment_is_covered() -> None:
    """Reading the ids from disk must not silently drop a shipped environment."""
    private_path("ops/environments")
    found = _shipped_environment_ids()
    assert len(found) == 6, (
        f"expected exactly six shipped environments (a production and a staging "
        f"one for each of three deployments), found {len(found)}: {found}"
    )


@pytest.mark.parametrize("environment_id", _environment_params())
def test_install_time_and_running_ui_agree_on_sensitivity_for_shipped_environments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    environment_id: str,
) -> None:
    """The install-time answer equals the answer the running UI computes through
    its launcher-pinned descriptor, for every shipped environment, and no
    shipped environment is left sensitive without a token (review finding R1).
    """
    import okg.substrate.nomos.runtime_enforcement as enforcement
    from okg.substrate.deployments.environment import load_environment

    descriptor = private_path(f"ops/environments/{environment_id}/environment.yaml")
    values = _shipped_values(tmp_path, descriptor)
    environment = load_environment(descriptor, environ=values)
    name = environment.graph_deployment_name

    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    # Run the "operator's shell" lookup with nothing the pinned dir provides.
    empty = tmp_path / "empty-deployments"
    empty.mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(empty))
    enforcement._clear_nomos_enforcement_mode_cache()
    install_time = enforcement.deployment_requires_read_auth(
        name, deployment_dir=environment.manifest.deployment_dir
    )

    # The running UI: the launcher pins the descriptor and release root, and
    # the viz resolves the deployment by name through them.
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", str(descriptor))
    monkeypatch.setenv("OKG_RELEASE_ROOT", str(tmp_path / "release"))
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
    enforcement._clear_nomos_enforcement_mode_cache()
    running_ui = enforcement.deployment_requires_read_auth(name)

    assert install_time == running_ui, (
        f"{environment_id}: install-time sensitivity {install_time} but the "
        f"running UI computes {running_ui}"
    )
    if install_time and environment.ui.enabled:
        assert environment.ui.auth_token_env, (
            f"{environment_id} is sensitive with its UI on but names no token"
        )
        environment_services.require_ui_token_for_sensitive_deployment(environment)
        # Remove the token line: both the plan-time check and the UI refuse.
        tokenless = replace(environment, ui=replace(environment.ui, auth_token_env=None))
        with pytest.raises(DeploymentConfigError, match="ui.auth_token_env"):
            environment_services.require_ui_token_for_sensitive_deployment(tokenless)
        from okg.viz.auth import require_read_auth_for_sensitive_deployment
        from okg.substrate.errors import VizReadAuthRequired

        with pytest.raises(VizReadAuthRequired):
            require_read_auth_for_sensitive_deployment(
                name, None, surface="viz"
            )
    if environment_id.startswith("okg-workspace"):
        assert install_time is True


@pytest.mark.parametrize("predecessor_has_token", [False, True])
def test_previous_release_rebuild_reads_the_token_from_the_installed_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    predecessor_has_token: bool,
) -> None:
    root, values = _environment_fixture(tmp_path)
    _declare_ui_token(root, "fixture-staging")
    current = _load(root, "fixture-staging", values)
    assert current.ui.auth_token_env == _UI_TOKEN_ENV
    predecessor = replace(
        current,
        ui=replace(
            current.ui,
            auth_token_env=_UI_TOKEN_ENV if predecessor_has_token else None,
        ),
    )
    generation = environment_services._service_generation_topology(
        predecessor, definition_digest=predecessor.definition_digest
    )
    pointer = {
        "data_root": str(current.runtime.data_root),
        "tier": predecessor.tier,
        "graph_deployment_name": predecessor.graph_deployment_name,
        "descriptor_digest": predecessor.descriptor_digest,
        "definition_digest": predecessor.definition_digest,
        "database": {
            "name": predecessor.database.name,
            "dbos_identity": predecessor.database.dbos_identity,
        },
    }
    monkeypatch.setattr(
        environment_services, "load_active_release", lambda _path: pointer
    )
    monkeypatch.setattr(
        environment_services,
        "_previous_manager_binding",
        lambda *_a, **_k: {"service_topology": generation},
    )

    rebuilt = environment_services._installed_historical_environment(
        current, current_release={}
    )

    expected = _UI_TOKEN_ENV if predecessor_has_token else None
    assert rebuilt.ui.auth_token_env == expected


@pytest.mark.parametrize(
    "environment_id",
    _environment_params(skip_prefix="okg-ci"),
)
def test_shipped_ui_environments_name_the_token_variable_and_no_value(
    environment_id: str,
) -> None:
    import yaml

    descriptor = private_path(f"ops/environments/{environment_id}/environment.yaml")
    raw = yaml.safe_load(descriptor.read_text(encoding="utf-8"))
    assert raw["ui"]["auth_token_env"] == "OKG_UI_TOKEN"
    assert raw["runtime"]["credential_refs"][raw["ui"]["service"]] == [
        "env:OKG_UI_TOKEN"
    ]
