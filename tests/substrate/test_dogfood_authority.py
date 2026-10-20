from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.deployment_authority import resolve_deployment_dsn_authority
from okg.substrate.dogfood_authority import read_authority, write_authority

# The tracked okg-workspace compose descriptor, where the renderer writes it.
TRACKED_DESCRIPTOR = Path(
    "deployments/okg-workspace/docker-compose.okg-workspace.dogfood.yaml"
)


@pytest.fixture(autouse=True)
def _no_live_docker(monkeypatch):
    """Keep these tests hermetic: never shell out to a real Docker daemon.

    The compose identity guard probes Docker; the default here reports the
    daemon as unavailable, and the guard tests override this stub.
    """
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._docker_inspect_labels",
        lambda *, kind, name: {
            "probe": "docker_unavailable", "present": None, "labels": {},
        },
    )


def _write_manifest(root: Path, *, self_hosted: bool = True) -> Path:
    deployment_dir = root / "deployments" / "okg-workspace"
    deployment_dir.mkdir(parents=True)
    path = deployment_dir / "deployment.yaml"
    path.write_text(
        yaml.safe_dump({
            "name": "okg-workspace",
            "postgres": {
                "dsn": "${OKG_DSN}",
                "database": "okg_workspace",
                "self_hosted_authoritative": self_hosted,
            },
        }),
        encoding="utf-8",
    )
    return path


def test_authority_record_is_atomic_and_protected(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "authority.json"
    monkeypatch.setenv("OKG_DOGFOOD_AUTHORITY_PATH", str(path))

    written = write_authority(
        "okg-workspace",
        dsn="postgres://postgres:secret@127.0.0.1:55434/okg_workspace",
        database="okg_workspace",
        host="127.0.0.1",
        port=55434,
        runtime_instance={"container_name": "postgres", "compose_project": "okg"},
        descriptor_sha256="abc123",
    )

    assert written == path
    assert os.stat(path).st_mode & 0o777 == 0o600
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema"] == "okg.dogfood.authority.v1"
    assert payload["deployment"] == "okg-workspace"
    assert payload["runtime_instance"]["container_name"] == "postgres"
    loaded, error = read_authority("okg-workspace")
    assert error is None
    assert loaded is not None
    assert loaded["database"] == "okg_workspace"


def test_cold_worktree_resolves_runtime_authority_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_manifest(tmp_path)
    monkeypatch.setenv("OKG_DOGFOOD_AUTHORITY_PATH", str(tmp_path / "authority.json"))
    monkeypatch.setenv("OKG_LOCAL_ENV", str(tmp_path / "missing.env"))
    monkeypatch.setenv("OKG_ENABLE_LOCAL_ENV_FOR_TESTS", "1")
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: tmp_path / "deployments" / "okg-workspace",
    )
    write_authority(
        "okg-workspace",
        dsn="postgres://postgres:secret@127.0.0.1:55434/okg_workspace",
        database="okg_workspace",
        host="127.0.0.1",
        port=55434,
        runtime_instance={"container_name": "postgres", "compose_project": "okg"},
        descriptor_sha256="abc123",
    )

    result = resolve_deployment_dsn_authority("okg-workspace")

    assert result["dsn"].endswith("/okg_workspace")
    assert result["authority"]["mode"] == "dogfood_runtime_record"
    assert result["target"]["identity"] == "ok"


def test_self_hosted_profile_refuses_same_db_on_reused_port(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_manifest(tmp_path)
    authority_file = tmp_path / "authority.json"
    profile = tmp_path / "local.env"
    profile.write_text(
        "OKG_DSN=postgres://postgres:secret@127.0.0.1:5433/okg_workspace\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DOGFOOD_AUTHORITY_PATH", str(authority_file))
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    monkeypatch.setenv("OKG_ENABLE_LOCAL_ENV_FOR_TESTS", "1")
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: tmp_path / "deployments" / "okg-workspace",
    )
    write_authority(
        "okg-workspace",
        dsn="postgres://postgres:secret@127.0.0.1:55434/okg_workspace",
        database="okg_workspace",
        host="127.0.0.1",
        port=55434,
        runtime_instance={"container_name": "postgres", "compose_project": "okg"},
        descriptor_sha256="abc123",
    )

    result = resolve_deployment_dsn_authority("okg-workspace")

    assert result["dsn"] is None
    assert "active dogfood authority" in result["reason"]
    assert result["target"]["identity"] == "mismatch"


def test_compose_refuses_conflicting_database_identity(
    tmp_path: Path,
    capsys,
) -> None:
    _write_manifest(tmp_path)

    rc = main([
        "dogfood", "service", "render",
        "--deployment", "okg-workspace",
        "--adapter", "docker-compose",
        "--repo-root", str(tmp_path),
        "--postgres-db", "okg_workspace_from_another_worktree",
        "--json",
    ])

    assert rc == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.err or captured.out)
    assert "database identity mismatch" in str(payload)
    assert "okg_workspace" in str(payload)


def test_checked_in_compose_descriptor_requires_password_environment() -> None:
    descriptor = Path(__file__).parents[2] / TRACKED_DESCRIPTOR
    text = descriptor.read_text(encoding="utf-8")

    assert "${OKG_DOGFOOD_PG_PASSWORD:?" in text
    assert ":okg@" not in text


def test_installed_authority_uses_rendered_compose_project(tmp_path, monkeypatch, capsys):
    from okg.substrate.cli.dogfood import _publish_dogfood_authority

    monkeypatch.setenv("OKG_DOGFOOD_AUTHORITY_PATH", str(tmp_path / "authority.json"))
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.worktree_main_local_env_path",
        lambda: tmp_path / "local.env",
    )
    assert main([
        "dogfood", "service", "render", "--deployment", "fixture",
        "--adapter", "docker-compose", "--repo-root", str(tmp_path),
        "--postgres-db", "okg_fixture", "--json",
    ]) == 0
    rendered = json.loads(capsys.readouterr().out)
    published = _publish_dogfood_authority(
        deployment="fixture", repo_root=tmp_path, rendered=rendered,
    )
    project = yaml.safe_load(rendered["content"])["name"]
    assert published["runtime_instance"]["compose_project"] == project
    saved = json.loads((tmp_path / "authority.json").read_text())
    assert saved["runtime_instance"]["compose_project"] == project


def test_checked_in_compose_descriptor_matches_the_renderer(capsys) -> None:
    """The tracked descriptor is generated output; drift is a defect.

    It differs from `dogfood service render` in exactly one way by design: the
    credential is an environment placeholder and carries explaining comments.
    Render with that placeholder as the password and the files must match line
    for line, comments excluded -- otherwise the file in git (the one from
    incident #1602) keeps a stale Compose project label after the renderer
    moves on.
    """
    repo_root = Path(__file__).parents[2]
    descriptor = repo_root / TRACKED_DESCRIPTOR
    placeholder = (
        "${OKG_DOGFOOD_PG_PASSWORD:?set OKG_DOGFOOD_PG_PASSWORD in the environment}"
    )

    assert main([
        "dogfood", "service", "render",
        "--deployment", "okg-workspace",
        "--adapter", "docker-compose",
        "--repo-root", str(repo_root),
        "--postgres-password", placeholder,
        "--json",
    ]) == 0
    rendered = json.loads(capsys.readouterr().out)["content"]

    tracked = [
        line
        for line in descriptor.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("#")
    ]
    assert tracked == rendered.splitlines()


def test_checked_in_compose_descriptor_carries_no_absolute_path() -> None:
    """A tracked descriptor must not encode the checkout that rendered it."""
    descriptor = Path(__file__).parents[2] / TRACKED_DESCRIPTOR
    text = descriptor.read_text(encoding="utf-8")

    assert "/Users/" not in text
    assert "/home/" not in text.replace("/home/postgres/pgdata", "")
    assert "worktrees" not in text
