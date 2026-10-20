"""Unit tests for `_resolve_deployment_dsn`.

Per `one-db-per-deployment` (2026-04-26): manifest's
`postgres.dsn` is the only source of truth for
deployment-scoped DSNs. OKG_DSN is consumed only via
explicit `${OKG_DSN}` interpolation. The previous silent
env-var fallback is removed.
"""
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import _resolve_deployment_dsn, main
from okg.substrate.errors import SubstrateError
from tests.nomos_helpers import patch_nomos_ready


def _captured_error(capsys) -> dict:
    captured = capsys.readouterr()
    raw = (captured.err or captured.out).strip().splitlines()[-1]
    return json.loads(raw)


def _write_dep(tmp_path: Path, *, name: str, manifest: dict) -> Path:
    d = tmp_path / name
    d.mkdir()
    (d / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    return d


@pytest.fixture
def patched_finder(tmp_path, monkeypatch):
    fake = {}

    def _find(name):
        return fake.get(name)

    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        _find,
    )
    return fake


@pytest.fixture(autouse=True)
def restore_okg_env_after_test():
    tracked = {
        key: os.environ.get(key)
        for key in (
            "OKG_DSN",
            "OKG_LOCAL_ENV",
            "OKG_ENABLE_LOCAL_ENV_FOR_TESTS",
        )
    }
    yield
    for key, value in tracked.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def test_dsn_flag_wins(tmp_path, patched_finder, monkeypatch):
    monkeypatch.setenv("OKG_DSN", "postgres://env/wrong")
    d = _write_dep(
        tmp_path, name="x",
        manifest={
            "name": "x",
            "postgres": {"dsn": "postgres://manifest/correct"},
        },
    )
    patched_finder["x"] = d
    args = argparse.Namespace(deployment="x", dsn="postgres://flag/winner")
    assert _resolve_deployment_dsn(args) == "postgres://flag/winner"


def test_manifest_field_used_no_env_consultation(
    tmp_path, patched_finder, monkeypatch,
):
    """When the manifest declares a literal DSN, OKG_DSN is
    NOT consulted — even if it's set."""
    monkeypatch.setenv("OKG_DSN", "postgres://env/wrong")
    d = _write_dep(
        tmp_path, name="y",
        manifest={
            "name": "y",
            "postgres": {"dsn": "postgres://manifest/correct"},
        },
    )
    patched_finder["y"] = d
    args = argparse.Namespace(deployment="y", dsn=None)
    assert (
        _resolve_deployment_dsn(args) == "postgres://manifest/correct"
    )


def test_okg_dsn_interpolation_works(
    tmp_path, patched_finder, monkeypatch,
):
    monkeypatch.setenv("OKG_DSN", "postgres://x@h/dep_a")
    d = _write_dep(
        tmp_path, name="z",
        manifest={
            "name": "z",
            "postgres": {"dsn": "${OKG_DSN}"},
        },
    )
    patched_finder["z"] = d
    args = argparse.Namespace(deployment="z", dsn=None)
    assert _resolve_deployment_dsn(args) == "postgres://x@h/dep_a"


def test_empty_okg_dsn_does_not_resolve_manifest_interpolation(
    tmp_path, patched_finder, monkeypatch,
):
    monkeypatch.setenv("OKG_DSN", "")
    profile = tmp_path / "local.env"
    profile.write_text("OKG_DSN=postgres://profile/correct\n")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    d = _write_dep(
        tmp_path, name="empty-env",
        manifest={
            "name": "empty-env",
            "postgres": {"dsn": "${OKG_DSN}"},
        },
    )
    patched_finder["empty-env"] = d
    args = argparse.Namespace(deployment="empty-env", dsn=None)
    assert _resolve_deployment_dsn(args) == "postgres://profile/correct"


def test_missing_manifest_field_errors_loudly_even_with_env(
    tmp_path, patched_finder, monkeypatch,
):
    """The hard-deprecation: even with OKG_DSN set, a
    manifest without `postgres.dsn` MUST raise."""
    monkeypatch.setenv("OKG_DSN", "postgres://env/whatever")
    d = _write_dep(tmp_path, name="w", manifest={"name": "w"})
    patched_finder["w"] = d
    args = argparse.Namespace(deployment="w", dsn=None)
    with pytest.raises(SubstrateError) as exc:
        _resolve_deployment_dsn(args)
    assert "postgres.dsn" in str(exc.value) or "postgres.dsn" in (
        exc.value.fix_hint or ""
    )


def test_unresolved_interpolation_errors(
    tmp_path, patched_finder, monkeypatch,
):
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("DEP_A_DSN", raising=False)
    d = _write_dep(
        tmp_path, name="u",
        manifest={
            "name": "u",
            "postgres": {"dsn": "${DEP_A_DSN}"},
        },
    )
    patched_finder["u"] = d
    args = argparse.Namespace(deployment="u", dsn=None)
    with pytest.raises(SubstrateError) as exc:
        _resolve_deployment_dsn(args)
    assert "DEP_A_DSN" in str(exc.value)


def test_missing_deployment_arg_errors(monkeypatch):
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    args = argparse.Namespace(deployment=None, dsn=None)
    with pytest.raises(SubstrateError) as exc:
        _resolve_deployment_dsn(args)
    assert "deployment" in str(exc.value).lower()


def test_missing_deployment_dir_errors(patched_finder, monkeypatch):
    monkeypatch.delenv("OKG_DSN", raising=False)
    args = argparse.Namespace(deployment="ghost", dsn=None)
    with pytest.raises(SubstrateError) as exc:
        _resolve_deployment_dsn(args)
    assert "ghost" in str(exc.value)


def test_deployment_env_reports_missing_manifest_env(
    tmp_path,
    patched_finder,
    monkeypatch,
    capsys,
):
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setenv("OKG_LOCAL_ENV", str(tmp_path / "missing.env"))
    d = _write_dep(
        tmp_path, name="dep",
        manifest={"name": "dep", "postgres": {"dsn": "${OKG_DSN}"}},
    )
    patched_finder["dep"] = d

    rc = main(["deployment", "env", "dep", "--json"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["ok"] is False
    assert payload["postgres"]["required_env"] == ["OKG_DSN"]
    assert payload["environment"] == [
        {
            "name": "OKG_DSN",
            "required": True,
            "set": False,
            "source": "missing",
            "value": None,
        }
    ]
    assert "deployment env dep --write-local" in (
        payload["recommended_commands"][0]
    )


def test_deployment_env_recommends_worktree_isolated_dsn(
    tmp_path,
    patched_finder,
    monkeypatch,
    capsys,
):
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setenv("OKG_LOCAL_ENV", str(tmp_path / "missing.env"))
    d = _write_dep(
        tmp_path,
        name="okg-workspace",
        manifest={
            "name": "okg-workspace",
            "postgres": {"dsn": "${OKG_DSN}"},
        },
    )
    patched_finder["okg-workspace"] = d

    rc = main([
        "deployment", "env", "okg-workspace", "--json", "--show-values",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip())
    suggested = payload["postgres"]["suggested_isolated_dsn"]
    assert "okg_workspace_" in suggested
    assert suggested != "postgres://postgres:okg@127.0.0.1:5433/okg"
    assert suggested in payload["recommended_commands"][0]


def test_deployment_env_uses_main_checkout_env_from_cold_worktree(
    tmp_path,
    patched_finder,
    monkeypatch,
    capsys,
):
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setenv("OKG_LOCAL_ENV", str(tmp_path / "missing.env"))
    monkeypatch.setenv("OKG_ENABLE_LOCAL_ENV_FOR_TESTS", "1")
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.worktree_main_local_env_dsn",
        lambda: "postgres://postgres:okg@127.0.0.1:55434/okg_workspace",
    )
    d = _write_dep(
        tmp_path,
        name="okg-workspace",
        manifest={
            "name": "okg-workspace",
            "postgres": {
                "dsn": "${OKG_DSN}",
                "database": "okg_workspace",
            },
        },
    )
    patched_finder["okg-workspace"] = d

    rc = main([
        "deployment", "env", "okg-workspace", "--json", "--show-values",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["ok"] is True
    assert payload["unresolved"] == []
    assert payload["environment"] == [
        {
            "name": "OKG_DSN",
            "required": True,
            "set": True,
            "source": "worktree_main_local_env",
            "value": "postgres://postgres:okg@127.0.0.1:55434/okg_workspace",
        }
    ]
    assert payload["postgres"]["resolved"] is True
    assert payload["authority"]["mode"] == "worktree_main_local_env"
    assert payload["target"]["identity"] == "ok"


def test_deployment_env_write_local_initializes_profile(
    tmp_path,
    patched_finder,
    monkeypatch,
    capsys,
):
    monkeypatch.delenv("OKG_DSN", raising=False)
    profile = tmp_path / ".okg" / "local.env"
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    d = _write_dep(
        tmp_path, name="dep",
        manifest={"name": "dep", "postgres": {"dsn": "${OKG_DSN}"}},
    )
    patched_finder["dep"] = d

    rc = main([
        "deployment", "env", "dep", "--json",
        "--write-local",
        "--dsn", "postgres://profile/correct",
    ])

    assert rc == 0
    assert "OKG_DSN=postgres://profile/correct" in profile.read_text()
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["ok"] is True
    assert payload["environment"][0]["source"] == "local_profile"
    assert payload["postgres"]["resolved"] is True


def test_self_hosted_write_local_targets_main_checkout_profile(
    tmp_path,
    patched_finder,
    monkeypatch,
    capsys,
):
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_LOCAL_ENV", raising=False)
    main_profile = tmp_path / "main" / ".okg" / "local.env"
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.worktree_main_local_env_path",
        lambda: main_profile,
    )
    d = _write_dep(
        tmp_path,
        name="okg-workspace",
        manifest={
            "name": "okg-workspace",
            "postgres": {
                "dsn": "${OKG_DSN}",
                "database": "okg_workspace",
                "self_hosted_authoritative": True,
            },
        },
    )
    patched_finder["okg-workspace"] = d

    rc = main([
        "deployment", "env", "okg-workspace", "--json",
        "--write-local",
        "--dsn", "postgres://profile/correct/okg_workspace",
    ])

    assert rc == 0
    assert "OKG_DSN=postgres://profile/correct/okg_workspace" in (
        main_profile.read_text()
    )
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["local_env_path"] == str(main_profile)


def test_local_env_profile_resolves_manifest_dsn(
    tmp_path,
    patched_finder,
    monkeypatch,
    capsys,
):
    monkeypatch.delenv("OKG_DSN", raising=False)
    profile = tmp_path / "local.env"
    profile.write_text("OKG_DSN=postgres://profile/correct\n")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    d = _write_dep(
        tmp_path, name="dep",
        manifest={"name": "dep", "postgres": {"dsn": "${OKG_DSN}"}},
    )
    patched_finder["dep"] = d
    calls = {}

    class _Result:
        statements_executed = 0
        extension_versions = {}
        idempotent_second_run = True
        converged_views = ()

    def _fake_migrate(dsn, *, deployment_name=None):
        calls["dsn"] = dsn
        calls["deployment_name"] = deployment_name
        return _Result()

    monkeypatch.setattr("okg.substrate.db.migrate.migrate", _fake_migrate)
    rc = main([
        "--json", "migrate", "--deployment", "dep", "--apply",
    ])

    assert rc == 0
    assert calls == {
        "dsn": "postgres://profile/correct",
        "deployment_name": "dep",
    }
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["idempotent_second_run"] is True


def test_process_env_wins_over_local_env_profile(
    tmp_path,
    patched_finder,
    monkeypatch,
):
    monkeypatch.setenv("OKG_DSN", "postgres://process/correct")
    profile = tmp_path / "local.env"
    profile.write_text("OKG_DSN=postgres://profile/wrong\n")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    d = _write_dep(
        tmp_path, name="dep",
        manifest={"name": "dep", "postgres": {"dsn": "${OKG_DSN}"}},
    )
    patched_finder["dep"] = d
    calls = {}

    class _Result:
        statements_executed = 0
        extension_versions = {}
        idempotent_second_run = True
        converged_views = ()

    def _fake_migrate(dsn, *, deployment_name=None):
        calls["dsn"] = dsn
        return _Result()

    monkeypatch.setattr("okg.substrate.db.migrate.migrate", _fake_migrate)
    rc = main([
        "--json", "migrate", "--deployment", "dep", "--apply",
    ])

    assert rc == 0
    assert calls["dsn"] == "postgres://process/correct"


def test_dogfood_ready_profile_uses_local_env_when_process_env_conflicts(
    tmp_path,
    patched_finder,
    monkeypatch,
    capsys,
):
    patch_nomos_ready(monkeypatch)
    monkeypatch.setenv("OKG_DSN", "postgres://process/wrong")
    profile = tmp_path / "local.env"
    profile.write_text("OKG_DSN=postgres://profile/correct\n")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    d = _write_dep(
        tmp_path, name="okg-workspace",
        manifest={
            "name": "okg-workspace",
            "postgres": {"dsn": "${OKG_DSN}"},
        },
    )
    patched_finder["okg-workspace"] = d
    calls = {}

    def _fake_live_checks(**kwargs):
        calls.update(kwargs)
        return {
            "checks": [],
            "blockers": [],
            "warnings": [],
            "generation": {
                "generation_id": "okg:okg-workspace:gen:test",
                "status": "published",
            },
            "source_freshness": {"ok": True, "missing": [], "stale": []},
            "source_sync_status": {"ok": True, "status": "healthy"},
            "mcp": {"ok": True},
        }

    monkeypatch.setattr(
        "okg.substrate.cli.deployment._deployment_ready_live_checks",
        _fake_live_checks,
    )

    rc = main([
        "deployment", "ready", "okg-workspace",
        "--checks", "dogfood",
        "--json",
        "--show-values",
    ])

    assert rc == 0
    assert calls["dsn"] == "postgres://profile/correct"
    payload = json.loads(capsys.readouterr().out.strip())
    authority = payload["environment"]["authority"]
    assert authority["mode"] == "dogfood_local_profile"
    assert authority["conflict_count"] == 1
    assert authority["conflicts"][0] == {
        "name": "OKG_DSN",
        "process_env_ignored": True,
        "process_value": "postgres://process/wrong",
        "local_profile_value": "postgres://profile/correct",
        "reason": "dogfood_local_profile_authority",
    }
    assert payload["environment"]["environment"][0]["source"] == "local_profile"
    assert any(
        check["id"] == "env.authority"
        and check["status"] == "pass"
        for check in payload["checks"]
    )


def test_local_env_profile_does_not_load_test_db_keys(
    tmp_path,
    monkeypatch,
):
    from okg.substrate.local_env import load_local_env

    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_TEST_DSN", raising=False)
    monkeypatch.delenv("OKG_TEST_DSN_TEMPLATE", raising=False)
    profile = tmp_path / "local.env"
    profile.write_text(
        "OKG_DSN=postgres://profile/live\n"
        "OKG_TEST_DSN=postgres://profile/test\n"
        "OKG_TEST_DSN_TEMPLATE=postgres://profile/test_{workerid}\n"
    )

    result = load_local_env(path=profile)

    assert result.loaded == {"OKG_DSN": "postgres://profile/live"}
    assert "OKG_TEST_DSN" not in os.environ
    assert "OKG_TEST_DSN_TEMPLATE" not in os.environ
    assert result.skipped["OKG_TEST_DSN"] == (
        "test_db_key_not_loaded_from_local_profile"
    )


def test_local_env_profile_does_not_load_empty_values(
    tmp_path,
    monkeypatch,
):
    from okg.substrate.local_env import load_local_env

    monkeypatch.delenv("OKG_DSN", raising=False)
    profile = tmp_path / "local.env"
    profile.write_text("OKG_DSN=\n")

    result = load_local_env(path=profile)

    assert result.loaded == {}
    assert "OKG_DSN" not in os.environ
    assert result.skipped["OKG_DSN"] == "empty_value"


def test_local_env_profile_allows_chronos_interval_override(
    tmp_path,
    monkeypatch,
):
    from okg.substrate.local_env import load_local_env

    monkeypatch.delenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", raising=False)
    profile = tmp_path / "local.env"
    profile.write_text("CHRONOS_INTERVAL_CONTINUATION_PERCENT=95\n")

    result = load_local_env(path=profile)

    assert result.loaded == {"CHRONOS_INTERVAL_CONTINUATION_PERCENT": "95"}
    assert os.environ["CHRONOS_INTERVAL_CONTINUATION_PERCENT"] == "95"


def test_migrate_deployment_uses_manifest_dsn(
    tmp_path,
    patched_finder,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("OKG_DSN", "postgres://env/wrong")
    d = _write_dep(
        tmp_path, name="dep",
        manifest={
            "name": "dep",
            "postgres": {"dsn": "postgres://manifest/correct"},
        },
    )
    patched_finder["dep"] = d
    calls = {}

    class _Result:
        statements_executed = 0
        extension_versions = {}
        idempotent_second_run = True
        converged_views = ()

    def _fake_migrate(dsn, *, deployment_name=None):
        calls["dsn"] = dsn
        calls["deployment_name"] = deployment_name
        return _Result()

    monkeypatch.setattr("okg.substrate.db.migrate.migrate", _fake_migrate)
    rc = main([
        "--json", "migrate", "--deployment", "dep", "--apply",
    ])

    assert rc == 0
    assert calls == {
        "dsn": "postgres://manifest/correct",
        "deployment_name": "dep",
    }
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["idempotent_second_run"] is True


def _write_corrupt_dep(tmp_path: Path, *, name: str) -> Path:
    """A deployment dir whose deployment.yaml exists but cannot be parsed."""
    d = tmp_path / name
    d.mkdir()
    # Unbalanced flow mapping -> fastyaml.safe_load raises.
    (d / "deployment.yaml").write_text(
        "name: " + name + "\n"
        "postgres:\n"
        "  database: okg_workspace\n"
        "  dsn: postgres://u:p@h:5433/okg_workspace\n"
        "broken: {unclosed: [1, 2, 3\n"
    )
    return d


def test_corrupt_manifest_classifies_target_unresolved_not_undeclared(
    tmp_path, patched_finder, monkeypatch,
):
    """A deployment whose deployment.yaml PINS postgres.database but is CORRUPT
    must NOT be classified `undeclared` (and silently accepted). The fail-closed
    contract: an unreadable manifest -> target identity `unresolved`."""
    from okg.substrate.deployment_authority import (
        _read_manifest,
        check_publish_provenance,
        resolve_deployment_dsn_authority,
    )

    monkeypatch.delenv("OKG_DSN", raising=False)
    d = _write_corrupt_dep(tmp_path, name="okg-workspace")
    patched_finder["okg-workspace"] = d

    # The manifest exists but is unreadable.
    raw, readable = _read_manifest("okg-workspace")
    assert raw is None
    assert readable is False

    # Explicit-DSN authority path: corrupt manifest -> unresolved, not undeclared.
    resolution = resolve_deployment_dsn_authority(
        "okg-workspace",
        explicit_dsn="postgres://u:p@h:5433/okg_workspace",
    )
    assert resolution["target"]["identity"] == "unresolved"

    # Publish-provenance path also resolves the target as unresolved.
    provenance = check_publish_provenance(
        "okg-workspace",
        "postgres://u:p@h:5433/okg_workspace",
    )
    assert provenance["target"]["identity"] == "unresolved"


def test_corrupt_manifest_refuses_publish_provenance(
    tmp_path, patched_finder, monkeypatch,
):
    """check_publish_provenance on an UNREADABLE manifest must return
    allowed=False / mode manifest_unreadable, NOT the fail-open
    allowed:True sync_root_undeclared that a missing sync_root would give."""
    from okg.substrate.deployment_authority import check_publish_provenance

    monkeypatch.delenv("OKG_DSN", raising=False)
    d = _write_corrupt_dep(tmp_path, name="okg-workspace")
    patched_finder["okg-workspace"] = d

    provenance = check_publish_provenance(
        "okg-workspace",
        "postgres://u:p@h:5433/okg_workspace",
    )
    assert provenance["allowed"] is False
    assert provenance["mode"] == "manifest_unreadable"


def test_readable_manifest_without_database_stays_undeclared(
    tmp_path, patched_finder, monkeypatch,
):
    """Regression guard: a READABLE manifest that declares no database is a
    legitimate operator state and must stay identity `undeclared` (accepted),
    NOT get swept into the fail-closed `unresolved`."""
    from okg.substrate.deployment_authority import resolve_deployment_dsn_authority

    monkeypatch.delenv("OKG_DSN", raising=False)
    d = _write_dep(
        tmp_path, name="dep",
        manifest={"name": "dep", "postgres": {"dsn": "postgres://u:p@h/dep"}},
    )
    patched_finder["dep"] = d

    resolution = resolve_deployment_dsn_authority(
        "dep", explicit_dsn="postgres://u:p@h:5433/dep_some_db",
    )
    assert resolution["target"]["identity"] == "undeclared"


def test_readable_manifest_without_sync_root_allows_with_warning(
    tmp_path, patched_finder, monkeypatch,
):
    """Regression guard: a READABLE manifest that declares no sync_root keeps
    its allowed:True sync_root_undeclared behavior - unchanged by the fix."""
    from okg.substrate.deployment_authority import check_publish_provenance

    monkeypatch.delenv("OKG_DSN", raising=False)
    d = _write_dep(
        tmp_path, name="dep",
        manifest={"name": "dep", "postgres": {"dsn": "postgres://u:p@h/dep"}},
    )
    patched_finder["dep"] = d

    provenance = check_publish_provenance(
        "dep", "postgres://u:p@h:5433/dep",
    )
    assert provenance["allowed"] is True
    assert provenance["mode"] == "sync_root_undeclared"


def test_sealed_publish_provenance_uses_composed_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from okg.substrate.deployment_authority import _read_manifest

    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", "/sealed/environment.yaml")
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda deployment: SimpleNamespace(
            # A composed manifest carries its directory; the provenance guard
            # needs it to resolve sync_root the way the loader does.
            deployment_dir=Path("/sealed"),
            effective_raw={
                "name": deployment,
                "sync_root": "/srv/staging-sync",
                "postgres": {"database": "staging"},
            }
        ),
    )

    raw, readable = _read_manifest("demo-staging")

    assert readable is True
    assert raw["sync_root"] == "/srv/staging-sync"


def test_sealed_publish_provenance_binds_sync_root_to_release_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from okg.substrate.deployment_authority import check_publish_provenance

    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", "/sealed/environment.yaml")
    monkeypatch.setenv("OKG_RELEASE_ARTIFACT_ID", "sha256:artifact")
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda deployment: SimpleNamespace(
            # A composed manifest carries its directory; the provenance guard
            # needs it to resolve sync_root the way the loader does.
            deployment_dir=Path("/sealed"),
            effective_raw={
                "name": deployment,
                "sync_root": "/srv/staging-sync",
                "postgres": {"database": "staging"},
            }
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority.verify_worker_release_authority",
        lambda **_kwargs: {
            "sync_root": "/srv/staging-sync",
            "code_revision": "candidate-sha",
            "artifact_id": "sha256:artifact",
        },
    )

    def fake_run(command, **kwargs):
        assert command == ["git", "rev-parse", "--show-toplevel", "HEAD"]
        assert kwargs["cwd"] == "/srv/staging-sync"
        return SimpleNamespace(
            stdout="/srv/staging-sync\ncandidate-sha\n",
        )

    monkeypatch.setattr("subprocess.run", fake_run)

    provenance = check_publish_provenance(
        "demo-staging",
        "postgres://u:p@127.0.0.1:55437/staging",
    )

    assert provenance["allowed"] is True
    assert provenance["mode"] == "sync_root"
    assert provenance["checkout_revision"] == "candidate-sha"
    assert provenance["sealed_artifact_id"] == "sha256:artifact"


def test_sealed_publish_provenance_refuses_stale_sync_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from okg.substrate.deployment_authority import check_publish_provenance

    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", "/sealed/environment.yaml")
    monkeypatch.setenv("OKG_RELEASE_ARTIFACT_ID", "sha256:artifact")
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda deployment: SimpleNamespace(
            # A composed manifest carries its directory; the provenance guard
            # needs it to resolve sync_root the way the loader does.
            deployment_dir=Path("/sealed"),
            effective_raw={
                "name": deployment,
                "sync_root": "/srv/staging-sync",
                "postgres": {"database": "staging"},
            }
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority.verify_worker_release_authority",
        lambda **_kwargs: {
            "sync_root": "/srv/staging-sync",
            "code_revision": "candidate-sha",
            "artifact_id": "sha256:artifact",
        },
    )
    monkeypatch.setattr(
        "subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(
            stdout="/srv/staging-sync\nstale-sha\n",
        ),
    )

    provenance = check_publish_provenance(
        "demo-staging",
        "postgres://u:p@127.0.0.1:55437/staging",
    )

    assert provenance["allowed"] is False
    assert provenance["mode"] == "sealed_sync_root_revision_mismatch"


def test_explicit_dsn_on_wrong_host_refused_against_literal_manifest(
    tmp_path, patched_finder, monkeypatch,
):
    """The 2026-06-10 incident shape via the explicit-DSN path: the manifest
    pins a LITERAL DSN on the canonical host, but an operator passes an
    explicit DSN with the RIGHT database name on the WRONG host. The
    (host, port, database) triple refuses it instead of accepting it `ok`."""
    from okg.substrate.deployment_authority import resolve_deployment_dsn_authority

    monkeypatch.delenv("OKG_DSN", raising=False)
    d = _write_dep(
        tmp_path, name="okg-workspace",
        manifest={
            "name": "okg-workspace",
            "postgres": {
                "dsn": "postgres://u:p@canonical.example:55434/okg_workspace",
                "database": "okg_workspace",
            },
        },
    )
    patched_finder["okg-workspace"] = d

    resolution = resolve_deployment_dsn_authority(
        "okg-workspace",
        explicit_dsn="postgres://u:p@fork.example:5433/okg_workspace",
    )
    assert resolution["dsn"] is None
    assert resolution["target"]["identity"] == "mismatch"
    assert resolution["next_action"] == "fix_deployment_database_identity"


def test_explicit_dsn_loopback_alias_accepted_against_literal_manifest(
    tmp_path, patched_finder, monkeypatch,
):
    """A benign loopback alias is NOT a false mismatch: the manifest pins
    127.0.0.1 but the explicit DSN says localhost (same server, same port,
    same database) -> accepted `ok`."""
    from okg.substrate.deployment_authority import resolve_deployment_dsn_authority

    monkeypatch.delenv("OKG_DSN", raising=False)
    d = _write_dep(
        tmp_path, name="okg-workspace",
        manifest={
            "name": "okg-workspace",
            "postgres": {
                "dsn": "postgres://u:p@127.0.0.1:55434/okg_workspace",
                "database": "okg_workspace",
            },
        },
    )
    patched_finder["okg-workspace"] = d

    resolution = resolve_deployment_dsn_authority(
        "okg-workspace",
        explicit_dsn="postgres://u:p@localhost:55434/okg_workspace",
    )
    assert resolution["dsn"] == "postgres://u:p@localhost:55434/okg_workspace"
    assert resolution["target"]["identity"] == "ok"


def test_explicit_dsn_interpolated_manifest_stays_name_only(
    tmp_path, patched_finder, monkeypatch,
):
    """Regression guard: an INTERPOLATED manifest (${OKG_DSN}) declares no
    host/port, so the explicit-DSN path stays name-only - a matching database
    name on any host is accepted `ok`, unchanged behavior."""
    from okg.substrate.deployment_authority import resolve_deployment_dsn_authority

    monkeypatch.delenv("OKG_DSN", raising=False)
    d = _write_dep(
        tmp_path, name="okg-workspace",
        manifest={
            "name": "okg-workspace",
            "postgres": {
                "dsn": "${OKG_DSN}",
                "database": "okg_workspace",
            },
        },
    )
    patched_finder["okg-workspace"] = d

    resolution = resolve_deployment_dsn_authority(
        "okg-workspace",
        explicit_dsn="postgres://u:p@any.example:5433/okg_workspace",
    )
    assert resolution["dsn"] == "postgres://u:p@any.example:5433/okg_workspace"
    assert resolution["target"]["identity"] == "ok"


# ---------------------------------------------------------------------------
# self_hosted_authoritative manifest flag (replaces the hardcoded
# `deployment == "okg-workspace"` local-profile-authority determination).
# ---------------------------------------------------------------------------


def test_self_hosted_authoritative_true_when_flag_set(
    tmp_path, patched_finder, monkeypatch,
):
    """`deployment_self_hosted_authoritative` returns True when the manifest's
    postgres block sets `self_hosted_authoritative: true`."""
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
    )

    d = _write_dep(
        tmp_path, name="flagged",
        manifest={
            "name": "flagged",
            "postgres": {
                "dsn": "${OKG_DSN}",
                "self_hosted_authoritative": True,
            },
        },
    )
    patched_finder["flagged"] = d
    assert deployment_self_hosted_authoritative("flagged") is True


def test_self_hosted_authoritative_false_when_flag_absent(
    tmp_path, patched_finder, monkeypatch,
):
    """Missing key -> False (the default process-env posture)."""
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
    )

    d = _write_dep(
        tmp_path, name="plain",
        manifest={"name": "plain", "postgres": {"dsn": "${OKG_DSN}"}},
    )
    patched_finder["plain"] = d
    assert deployment_self_hosted_authoritative("plain") is False


def test_self_hosted_authoritative_false_when_manifest_absent(
    tmp_path, patched_finder, monkeypatch,
):
    """No deployment dir / manifest -> False, never a silent True."""
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
    )

    # patched_finder returns None for unknown names.
    assert deployment_self_hosted_authoritative("does-not-exist") is False


def test_self_hosted_authoritative_false_when_manifest_unreadable(
    tmp_path, patched_finder, monkeypatch,
):
    """A corrupt manifest fails closed: False, not a permissive True."""
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
    )

    d = _write_corrupt_dep(tmp_path, name="corrupt")
    patched_finder["corrupt"] = d
    assert deployment_self_hosted_authoritative("corrupt") is False


def test_self_hosted_authoritative_false_when_flag_not_true(
    tmp_path, patched_finder, monkeypatch,
):
    """Only a literal `true` opts in: a truthy-but-not-True value is False."""
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
    )

    d = _write_dep(
        tmp_path, name="stringy",
        manifest={
            "name": "stringy",
            "postgres": {
                "dsn": "${OKG_DSN}",
                "self_hosted_authoritative": "yes",
            },
        },
    )
    patched_finder["stringy"] = d
    assert deployment_self_hosted_authoritative("stringy") is False


def test_flagged_deployment_local_env_wins_over_process_env(
    tmp_path, patched_finder, monkeypatch,
):
    """A deployment whose manifest sets `self_hosted_authoritative: true` gets
    local-profile-authoritative behavior: `.okg/local.env` wins over a stale
    process-env $OKG_DSN, exactly the authority sites' wiring
    (local_profile_authoritative=deployment_self_hosted_authoritative(name))."""
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
        resolve_deployment_dsn_authority,
    )

    monkeypatch.setenv("OKG_DSN", "postgres://process/stale")
    monkeypatch.setenv("OKG_ENABLE_LOCAL_ENV_FOR_TESTS", "1")
    profile = tmp_path / "local.env"
    profile.write_text("OKG_DSN=postgres://profile/correct\n")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    d = _write_dep(
        tmp_path, name="flagged",
        manifest={
            "name": "flagged",
            "postgres": {
                "dsn": "${OKG_DSN}",
                "self_hosted_authoritative": True,
            },
        },
    )
    patched_finder["flagged"] = d

    resolution = resolve_deployment_dsn_authority(
        "flagged",
        local_profile_authoritative=deployment_self_hosted_authoritative(
            "flagged"
        ),
    )
    assert resolution["dsn"] == "postgres://profile/correct"
    assert resolution["authority"]["mode"] == "dogfood_local_profile"
    assert resolution["authority"]["conflict_count"] == 1


def test_unflagged_deployment_process_env_wins(
    tmp_path, patched_finder, monkeypatch,
):
    """A deployment WITHOUT the flag keeps the default process-env path: a
    stale process-env $OKG_DSN beats local.env."""
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
        resolve_deployment_dsn_authority,
    )

    monkeypatch.setenv("OKG_DSN", "postgres://process/correct")
    monkeypatch.setenv("OKG_ENABLE_LOCAL_ENV_FOR_TESTS", "1")
    profile = tmp_path / "local.env"
    profile.write_text("OKG_DSN=postgres://profile/ignored\n")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    d = _write_dep(
        tmp_path, name="plain",
        manifest={"name": "plain", "postgres": {"dsn": "${OKG_DSN}"}},
    )
    patched_finder["plain"] = d

    resolution = resolve_deployment_dsn_authority(
        "plain",
        local_profile_authoritative=deployment_self_hosted_authoritative(
            "plain"
        ),
    )
    assert resolution["dsn"] == "postgres://process/correct"
    assert resolution["authority"]["mode"] == "deployment_manifest"


def test_deployment_env_json_never_prints_a_password_by_default(
    tmp_path,
    patched_finder,
    monkeypatch,
    capsys,
):
    """`deployment env --json` leaked the suggested DSN password (#2213).

    `suggested_isolated_dsn` was redacted while `recommended_commands`
    embedded the same value verbatim, in a `--dsn ...` argument and an
    `export ...` line. The whole payload is scanned here, not just the two
    known keys, so a new key cannot reopen the hole quietly. The second
    producer, behind `deployment ready --json` and `okg dogfood`, is
    covered by `test_deployment_ready_env_status_never_prints_a_password`
    below; both build their commands in one helper.
    """
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setenv("OKG_LOCAL_ENV", str(tmp_path / "missing.env"))
    d = _write_dep(
        tmp_path,
        name="leaky",
        manifest={"name": "leaky", "postgres": {"dsn": "${OKG_DSN}"}},
    )
    patched_finder["leaky"] = d

    rc = main(["deployment", "env", "leaky", "--json"])

    assert rc == 0
    raw = capsys.readouterr().out.strip()
    payload = json.loads(raw)
    commands = payload["recommended_commands"]
    assert commands, payload
    assert any("--dsn" in command for command in commands), commands
    # Any userinfo password that is not the redaction marker is a leak.
    leak = re.search(r"://[^:/@\s\"]*:(?!\*\*\*)[^@\s\"]+@", raw)
    assert leak is None, leak.group(0) if leak else raw
    assert "postgres://postgres:***@" in "\n".join(commands), commands


def test_deployment_env_json_show_values_still_prints_a_usable_dsn(
    tmp_path,
    patched_finder,
    monkeypatch,
    capsys,
):
    """`--show-values` is the documented opt-in, and it stays symmetric.

    The recommended command must carry the same value the payload's
    `suggested_isolated_dsn` field carries, so a copy-paste repair works
    when the operator asked for the secret.
    """
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setenv("OKG_LOCAL_ENV", str(tmp_path / "missing.env"))
    d = _write_dep(
        tmp_path,
        name="shown",
        manifest={"name": "shown", "postgres": {"dsn": "${OKG_DSN}"}},
    )
    patched_finder["shown"] = d

    rc = main(["deployment", "env", "shown", "--json", "--show-values"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip())
    suggested = payload["postgres"]["suggested_isolated_dsn"]
    assert "***" not in suggested
    assert suggested in "\n".join(payload["recommended_commands"])


def _ready_env_args(deployment: str) -> argparse.Namespace:
    return argparse.Namespace(
        deployment=deployment,
        dsn=None,
        local_env=None,
        profile=None,
    )


def test_deployment_ready_env_status_never_prints_a_password(
    tmp_path,
    patched_finder,
    monkeypatch,
):
    """The second `recommended_commands` producer is gated too (#2213).

    `_deployment_ready_env_status` backs `okg deployment ready --json` and
    `okg dogfood`. It carried a byte-for-byte copy of the leaking
    `deployment env` code, so the fix there left the class open. Both now
    call `_recommended_dsn_commands`, and this scans the whole ready
    payload the way the `deployment env` guard scans its own.
    """
    from okg.substrate.cli.deployment import _deployment_ready_env_status

    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    monkeypatch.setenv("OKG_LOCAL_ENV", str(tmp_path / "missing.env"))
    d = _write_dep(
        tmp_path,
        name="readyleak",
        manifest={"name": "readyleak", "postgres": {"dsn": "${OKG_DSN}"}},
    )
    patched_finder["readyleak"] = d

    payload, _resolved = _deployment_ready_env_status(
        _ready_env_args("readyleak"),
        show_values=False,
    )

    raw = json.dumps(payload)
    commands = payload["recommended_commands"]
    assert commands, payload
    assert any("--dsn" in command for command in commands), commands
    leak = re.search(r"://[^:/@\s\"]*:(?!\*\*\*)[^@\s\"]+@", raw)
    assert leak is None, leak.group(0) if leak else raw
    assert "postgres://postgres:***@" in "\n".join(commands), commands


def test_deployment_ready_env_status_show_values_stays_symmetric(
    tmp_path,
    patched_finder,
    monkeypatch,
):
    """`--show-values` keeps the ready command copy-pastable."""
    from okg.substrate.cli.deployment import _deployment_ready_env_status

    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    monkeypatch.setenv("OKG_LOCAL_ENV", str(tmp_path / "missing.env"))
    d = _write_dep(
        tmp_path,
        name="readyshown",
        manifest={"name": "readyshown", "postgres": {"dsn": "${OKG_DSN}"}},
    )
    patched_finder["readyshown"] = d

    payload, _resolved = _deployment_ready_env_status(
        _ready_env_args("readyshown"),
        show_values=True,
    )

    suggested = payload["postgres"]["suggested_isolated_dsn"]
    assert "***" not in suggested
    assert suggested in "\n".join(payload["recommended_commands"])
