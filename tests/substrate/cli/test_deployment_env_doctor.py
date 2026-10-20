from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import okg.substrate.cli.main as cli


def _write_env_deployment(root: Path, name: str = "demo") -> Path:
    deployment_dir = root / name
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n",
        encoding="utf-8",
    )
    (deployment_dir / "deployment.yaml").write_text(
        "name: demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    return deployment_dir


def test_env_doctor_redacts_reports_winner_and_does_not_mutate_env(
    tmp_path: Path,
):
    from okg.substrate.deployment_env_doctor import (
        ENV_DOCTOR_SCHEMA,
        build_env_doctor_report,
    )

    deployment_dir = _write_env_deployment(tmp_path)
    local_env = tmp_path / "local.env"
    local_env.write_text(
        "OKG_DSN=postgres://local:localpass@localhost:5432/okg_demo\n"
        "API_TOKEN=sentinel-token\n",
        encoding="utf-8",
    )
    before = dict(os.environ)
    process_env = {
        "OKG_DSN": "postgres://proc:processpass@localhost:5432/wrong_db",
    }

    report = build_env_doctor_report(
        deployment_dir,
        local_env_path=local_env,
        env=process_env,
        for_command="read",
        include_worktree_main_fallback=False,
    )

    assert dict(os.environ) == before
    assert report["schema"] == ENV_DOCTOR_SCHEMA
    assert report["command_risk"] == "read"
    assert report["authority"]["mode"] == "deployment_manifest"
    assert report["environment"][0]["source"] == "process_env"
    assert report["postgres"]["target"]["database"] == "wrong_db"
    assert report["ok"] is True
    codes = {finding["code"] for finding in report["findings"]}
    assert "deployment.env.authority_conflict" in codes
    assert "deployment.env.dsn_target_mismatch" in codes

    dumped = json.dumps(report)
    assert "processpass" not in dumped
    assert "localpass" not in dumped
    assert "sentinel-token" not in dumped
    assert "postgres://proc:processpass" not in dumped
    assert "postgres://<redacted>@localhost:5432/wrong_db" in dumped
    assert report["local_env"]["skipped"]["OKG_DSN"] == "process_env_wins"
    assert report["local_env"]["skipped"]["API_TOKEN"] == "not_an_okg_key"
    assert report["authority"]["authority_order"] == [
        "deployment_manifest",
        "process_env",
        "local_env",
    ]
    assert "authority_order" not in report
    assert "local_env_loaded" not in report
    assert "local_env_skipped" not in report


def test_env_doctor_classifies_write_and_runtime_conflicts_as_blockers(
    tmp_path: Path,
):
    from okg.substrate.deployment_env_doctor import (
        build_env_doctor_report,
        classify_for_command,
    )

    deployment_dir = _write_env_deployment(tmp_path)
    local_env = tmp_path / "local.env"
    local_env.write_text(
        "OKG_DSN=postgres://local:localpass@localhost:5432/okg_demo\n",
        encoding="utf-8",
    )
    process_env = {
        "OKG_DSN": "postgres://proc:processpass@localhost:5432/wrong_db",
    }

    write_report = build_env_doctor_report(
        deployment_dir,
        local_env_path=local_env,
        env=process_env,
        for_command="ingest",
        include_worktree_main_fallback=False,
    )
    runtime_report = build_env_doctor_report(
        deployment_dir,
        local_env_path=local_env,
        env=process_env,
        for_command="runtime worker",
        include_worktree_main_fallback=False,
    )

    assert classify_for_command("ingest") == "write"
    assert classify_for_command("runtime worker") == "runtime"
    assert write_report["ok"] is False
    assert runtime_report["ok"] is False
    assert {
        finding["code"]
        for finding in write_report["findings"]
        if finding["severity"] == "blocker"
    } == {
        "deployment.env.authority_conflict",
        "deployment.env.dsn_target_mismatch",
    }


def test_env_doctor_dogfood_local_authority_and_show_values(tmp_path: Path):
    from okg.substrate.deployment_env_doctor import build_env_doctor_report

    deployment_dir = _write_env_deployment(tmp_path)
    local_env = tmp_path / "local.env"
    local_env.write_text(
        "OKG_DSN=postgres://local:localpass@localhost:5432/okg_demo\n",
        encoding="utf-8",
    )
    process_env = {
        "OKG_DSN": "postgres://proc:processpass@localhost:5432/wrong_db",
    }

    report = build_env_doctor_report(
        deployment_dir,
        local_env_path=local_env,
        env=process_env,
        for_command="write",
        local_profile_authoritative=True,
        include_worktree_main_fallback=False,
        show_values=True,
    )

    assert report["ok"] is True
    assert report["authority"]["mode"] == "dogfood_local_profile"
    assert report["environment"][0]["source"] == "local_env"
    assert report["postgres"]["target"]["database"] == "okg_demo"
    assert "localpass" in json.dumps(report)
    assert report["local_env"]["loaded"] == ["OKG_DSN"]


def _write_flagged_env_deployment(root: Path, name: str = "selfhosted") -> Path:
    """A manifest that opts into self_hosted_authoritative via the new flag."""
    deployment_dir = root / name
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n", encoding="utf-8",
    )
    (deployment_dir / "deployment.yaml").write_text(
        f"name: {name}\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "  self_hosted_authoritative: true\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    return deployment_dir


def test_env_doctor_flagged_manifest_local_profile_is_authoritative_by_default(
    tmp_path: Path,
):
    """When `local_profile_authoritative` is None (no explicit caller override),
    the env doctor now consults the manifest's self_hosted_authoritative flag.
    A flagged manifest -> local.env wins over a stale process env."""
    from okg.substrate.deployment_env_doctor import build_env_doctor_report

    deployment_dir = _write_flagged_env_deployment(tmp_path)
    local_env = tmp_path / "local.env"
    local_env.write_text(
        "OKG_DSN=postgres://local:localpass@localhost:5432/okg_demo\n",
        encoding="utf-8",
    )
    process_env = {
        "OKG_DSN": "postgres://proc:processpass@localhost:5432/wrong_db",
    }

    report = build_env_doctor_report(
        deployment_dir,
        local_env_path=local_env,
        env=process_env,
        for_command="write",
        include_worktree_main_fallback=False,
    )

    assert report["authority"]["mode"] == "dogfood_local_profile"
    assert report["environment"][0]["source"] == "local_env"
    assert report["postgres"]["target"]["database"] == "okg_demo"


def test_env_doctor_explicit_override_wins_over_manifest_flag(tmp_path: Path):
    """Explicit-override-wins is preserved: a flagged manifest with an explicit
    `local_profile_authoritative=False` falls back to the process-env path."""
    from okg.substrate.deployment_env_doctor import build_env_doctor_report

    deployment_dir = _write_flagged_env_deployment(tmp_path)
    local_env = tmp_path / "local.env"
    local_env.write_text(
        "OKG_DSN=postgres://local:localpass@localhost:5432/okg_demo\n",
        encoding="utf-8",
    )
    process_env = {
        "OKG_DSN": "postgres://proc:processpass@localhost:5432/okg_demo",
    }

    report = build_env_doctor_report(
        deployment_dir,
        local_env_path=local_env,
        env=process_env,
        for_command="read",
        local_profile_authoritative=False,
        include_worktree_main_fallback=False,
    )

    assert report["authority"]["mode"] == "deployment_manifest"
    assert report["environment"][0]["source"] == "process_env"


def test_env_doctor_missing_required_var_is_blocker(tmp_path: Path):
    from okg.substrate.deployment_env_doctor import build_env_doctor_report

    deployment_dir = _write_env_deployment(tmp_path)
    report = build_env_doctor_report(
        deployment_dir,
        env={},
        for_command="read",
        include_worktree_main_fallback=False,
    )

    assert report["ok"] is False
    assert report["authority"]["missing"] == ["OKG_DSN"]
    assert report["postgres"]["resolved"] is False
    assert any(
        finding["code"] == "deployment.env.required_missing"
        and finding["severity"] == "blocker"
        for finding in report["findings"]
    )
    assert report["command_contract"]["local_env_mutation"] == (
        "not_performed"
    )
    assert "--dsn " in report["recommended_commands"][1]


def test_env_doctor_explicit_dsn_overrides_missing_manifest_env(
    tmp_path: Path,
):
    from okg.substrate.deployment_env_doctor import build_env_doctor_report

    deployment_dir = _write_env_deployment(tmp_path)

    report = build_env_doctor_report(
        deployment_dir,
        explicit_dsn="postgres://explicit:pw@localhost:5432/okg_demo",
        env={},
        for_command="read",
        include_worktree_main_fallback=False,
    )

    assert report["ok"] is True
    assert report["authority"]["mode"] == "explicit"
    assert report["postgres"]["resolved"] is True
    assert {
        finding["code"] for finding in report["findings"]
    }.isdisjoint({"deployment.env.required_missing"})


def test_env_doctor_cli_nested_json_dispatch(tmp_path: Path, capsys):
    from okg.substrate.deployment_env_doctor import ENV_DOCTOR_SCHEMA

    deployment_dir = _write_env_deployment(tmp_path)

    rc = cli.main([
        "deployment",
        "env",
        "doctor",
        str(deployment_dir),
        "--dsn",
        "postgres://explicit:pw@localhost:5432/okg_demo",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == ENV_DOCTOR_SCHEMA
    assert payload["authority"]["mode"] == "explicit"


@pytest.mark.parametrize(
    "argv",
    [
        [
            "deployment",
            "env",
            "doctor",
            "{deployment}",
            "--dsn",
            "postgres://explicit:pw@localhost:5432/okg_demo",
            "--json",
        ],
        [
            "deployment",
            "env",
            "doctor",
            "--dsn",
            "postgres://explicit:pw@localhost:5432/okg_demo",
            "{deployment}",
            "--json",
        ],
        [
            "deployment",
            "env",
            "--dsn",
            "postgres://explicit:pw@localhost:5432/okg_demo",
            "doctor",
            "{deployment}",
            "--json",
        ],
    ],
)
def test_env_doctor_cli_preserves_option_orderings(
    argv: list[str],
    tmp_path: Path,
    capsys,
):
    from okg.substrate.deployment_env_doctor import ENV_DOCTOR_SCHEMA

    deployment_dir = _write_env_deployment(tmp_path)
    resolved_argv = [
        str(deployment_dir) if item == "{deployment}" else item
        for item in argv
    ]

    rc = cli.main(resolved_argv)

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == ENV_DOCTOR_SCHEMA
    assert payload["authority"]["mode"] == "explicit"


def test_deployment_env_doctor_keyword_is_reserved(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    _write_env_deployment(tmp_path, name="doctor")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setenv(
        "OKG_DSN",
        "postgres://process:pw@localhost:5432/okg_demo",
    )

    rc = cli.main(["deployment", "env", "doctor", "--json"])

    assert rc == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert "requires a deployment name" in payload["error"]["message"]


def test_deployment_env_rejects_extra_positionals(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    _write_env_deployment(tmp_path, name="demo")
    _write_env_deployment(tmp_path, name="extra")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))

    rc = cli.main(["deployment", "env", "demo", "extra", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().err)
    assert "exactly one deployment argument" in payload["error"]["message"]


def test_env_doctor_honors_okg_local_env_from_supplied_env(tmp_path: Path):
    from okg.substrate.deployment_env_doctor import build_env_doctor_report

    deployment_dir = _write_env_deployment(tmp_path)
    local_env = tmp_path / "mapped-local.env"
    local_env.write_text(
        "OKG_DSN=postgres://local:localpass@localhost:5432/okg_demo\n",
        encoding="utf-8",
    )

    report = build_env_doctor_report(
        deployment_dir,
        env={"OKG_LOCAL_ENV": str(local_env)},
        include_worktree_main_fallback=False,
    )

    assert report["ok"] is True
    assert report["local_env_path"] == str(local_env)
    assert report["environment"][0]["source"] == "local_env"


def test_env_doctor_treats_empty_process_dsn_as_absent(tmp_path: Path):
    from okg.substrate.deployment_env_doctor import build_env_doctor_report

    deployment_dir = _write_env_deployment(tmp_path)
    local_env = tmp_path / "local.env"
    local_env.write_text(
        "OKG_DSN=postgres://local:localpass@localhost:5432/okg_demo\n",
        encoding="utf-8",
    )

    report = build_env_doctor_report(
        deployment_dir,
        local_env_path=local_env,
        env={"OKG_DSN": ""},
        include_worktree_main_fallback=False,
    )

    assert report["ok"] is True
    assert report["environment"][0]["source"] == "local_env"
    assert report["local_env"]["loaded"] == ["OKG_DSN"]
    assert "OKG_DSN" not in report["local_env"]["skipped"]
