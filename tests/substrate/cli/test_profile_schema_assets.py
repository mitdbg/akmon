"""Distribution-owned schema assets for profile-driven installs."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
import zipfile
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

import okg.substrate.cli.main as cli
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.profile import ProfileError, load_profile
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    planned_writes,
    scaffold_deployment,
)


_ABSENT = object()
_PACKAGE_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "external_profile_package"
)


def _write_profile(
    root: Path,
    *,
    schemas_ref: object = _ABSENT,
) -> Path:
    profile_dir = root / "fixture"
    profile_dir.mkdir(parents=True)
    profile: dict[str, object] = {
        "name": "fixture",
        "version": "1.0.0",
        "classification": "fixture",
        "init_questions": [],
        "modules": "modules.yaml",
    }
    if schemas_ref is not _ABSENT:
        profile["schemas"] = schemas_ref
    (profile_dir / "profile.yaml").write_text(
        yaml.safe_dump(profile, sort_keys=False),
        encoding="utf-8",
    )
    (profile_dir / "modules.yaml").write_text(
        "modules: []\n",
        encoding="utf-8",
    )
    return profile_dir


def _build_plan(profile_dir: Path, tmp_path: Path, *, name: str = "demo"):
    return build_plan(
        profile=load_profile(profile_dir / "profile.yaml"),
        deployment_name=name,
        deployments_root=tmp_path / "deployments",
        raw_answers={},
    )


def test_profile_contract_accepts_optional_schemas_and_absent_key(
    tmp_path: Path,
) -> None:
    with_schemas = _write_profile(tmp_path / "with", schemas_ref="schemas/")
    (with_schemas / "schemas").mkdir()
    loaded = load_profile(with_schemas / "profile.yaml")

    assert loaded.schemas_dir == (with_schemas / "schemas").resolve()

    without_schemas = _write_profile(tmp_path / "without")
    plan = _build_plan(without_schemas, tmp_path / "without-plan")
    assert plan.profile.schemas_dir is None
    assert plan.schemas == []
    assert [
        write.path.name
        for write in planned_writes(plan)
        if write.path.parent.name == "schemas"
    ] == ["demo_base.yaml"]


def test_absent_schemas_preserves_legacy_asset_destination_behavior(
    tmp_path: Path,
) -> None:
    profile_dir = _write_profile(tmp_path / "profile")
    plan = replace(
        _build_plan(profile_dir, tmp_path / "plan"),
        dashboards=[("Foo.yaml", "upper\n"), ("foo.yaml", "lower\n")],
    )

    writes = planned_writes(plan)

    assert [
        write.path.name
        for write in writes
        if write.path.parent.name == "dashboards"
    ] == ["Foo.yaml", "foo.yaml"]


@pytest.mark.parametrize(
    "reference",
    ["", "/tmp/outside", "../outside", "schemas/../schemas", r"C:\\outside"],
)
def test_profile_contract_rejects_unsafe_schema_references(
    tmp_path: Path,
    reference: str,
) -> None:
    profile_dir = _write_profile(tmp_path, schemas_ref=reference)

    with pytest.raises(ProfileError) as exc_info:
        load_profile(profile_dir / "profile.yaml")

    assert exc_info.value.data["key"] == "schemas"


def test_profile_contract_rejects_schema_root_symlink_escape(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    profile_dir = _write_profile(tmp_path / "profile", schemas_ref="schemas/")
    (profile_dir / "schemas").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ProfileError) as exc_info:
        load_profile(profile_dir / "profile.yaml")

    assert exc_info.value.data["reason"] == "path_escape"


@pytest.mark.parametrize("shape", ["missing", "file"])
def test_plan_rejects_missing_or_non_directory_schema_root(
    tmp_path: Path,
    shape: str,
) -> None:
    profile_dir = _write_profile(tmp_path, schemas_ref="schemas/")
    if shape == "file":
        (profile_dir / "schemas").write_text("not a directory", encoding="utf-8")

    with pytest.raises(ProfileError) as exc_info:
        _build_plan(profile_dir, tmp_path)

    assert exc_info.value.data["reason"] == (
        "missing_directory" if shape == "missing" else "not_directory"
    )


@pytest.mark.parametrize("link_kind", ["file", "directory"])
def test_plan_rejects_nested_schema_symlinks(
    tmp_path: Path,
    link_kind: str,
) -> None:
    profile_dir = _write_profile(tmp_path / "profile", schemas_ref="schemas/")
    schemas = profile_dir / "schemas"
    schemas.mkdir()
    outside = tmp_path / "outside"
    if link_kind == "file":
        outside.write_bytes(b"outside\n")
        (schemas / "escape.yaml").symlink_to(outside)
    else:
        outside.mkdir()
        (outside / "escape.yaml").write_bytes(b"outside\n")
        (schemas / "escape").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ProfileError) as exc_info:
        _build_plan(profile_dir, tmp_path)

    assert exc_info.value.data["reason"] == "symlink_entry"
    assert not (tmp_path / "deployments" / "demo").exists()


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="mkfifo unavailable")
def test_plan_rejects_special_schema_entries(tmp_path: Path) -> None:
    profile_dir = _write_profile(tmp_path, schemas_ref="schemas/")
    schemas = profile_dir / "schemas"
    schemas.mkdir()
    os.mkfifo(schemas / "not-a-file")

    with pytest.raises(ProfileError) as exc_info:
        _build_plan(profile_dir, tmp_path)

    assert exc_info.value.data["reason"] == "special_entry"


def test_schema_assets_preserve_nested_paths_and_exact_bytes(
    tmp_path: Path,
) -> None:
    profile_dir = _write_profile(tmp_path, schemas_ref="schemas/")
    schemas = profile_dir / "schemas"
    bridges = schemas / "bridges"
    bridges.mkdir(parents=True)
    domain_bytes = b"id: https://example.test/domain\r\nclasses: {}\r\n"
    bridge_bytes = b"id: https://example.test/bridge\nclasses: {}\n"
    (schemas / "domain.yaml").write_bytes(domain_bytes)
    (bridges / "external.yaml").write_bytes(bridge_bytes)

    plan = _build_plan(profile_dir, tmp_path)
    result = scaffold_deployment(plan)

    assert (result.deployment_dir / "schemas" / "domain.yaml").read_bytes() == (
        domain_bytes
    )
    assert (
        result.deployment_dir / "schemas" / "bridges" / "external.yaml"
    ).read_bytes() == bridge_bytes
    assert {
        path.relative_to(result.deployment_dir).as_posix()
        for path in result.files_written
    } >= {
        "schemas/domain.yaml",
        "schemas/bridges/external.yaml",
    }


@pytest.mark.parametrize(
    "collision_path",
    ["collision_base.yaml", "collision_base.yaml/child.yaml"],
)
def test_schema_collision_refuses_before_fresh_or_forced_target_mutation(
    tmp_path: Path,
    collision_path: str,
) -> None:
    profile_dir = _write_profile(tmp_path / "fresh", schemas_ref="schemas/")
    schemas = profile_dir / "schemas"
    schemas.mkdir()
    collision_asset = schemas / collision_path
    collision_asset.parent.mkdir(parents=True, exist_ok=True)
    collision_asset.write_bytes(b"profile-owned\n")

    with pytest.raises(ProfileError) as fresh_error:
        _build_plan(profile_dir, tmp_path / "fresh-plan", name="collision")

    assert fresh_error.value.data["reason"] == "destination_collision"
    assert not (tmp_path / "fresh-plan" / "deployments" / "collision").exists()

    safe_profile = _write_profile(tmp_path / "forced")
    safe_plan = _build_plan(safe_profile, tmp_path / "forced-plan", name="collision")
    colliding_plan = replace(
        safe_plan,
        schemas=[(collision_path, b"profile-owned\n")],
    )
    colliding_plan.deployment_dir.mkdir(parents=True)
    sentinel = colliding_plan.deployment_dir / "operator-data.txt"
    sentinel.write_text("preserve me", encoding="utf-8")

    with pytest.raises(ProfileError) as dry_run_error:
        planned_writes(colliding_plan)

    assert dry_run_error.value.data["reason"] == "destination_collision"
    assert sentinel.read_text(encoding="utf-8") == "preserve me"

    with pytest.raises(ProfileError) as forced_error:
        scaffold_deployment(colliding_plan, force=True)

    assert forced_error.value.data["reason"] == "destination_collision"
    assert sentinel.read_text(encoding="utf-8") == "preserve me"


def _build_and_extract_fixture_wheel(tmp_path: Path) -> Path:
    package_root = tmp_path / "package-source"
    shutil.copytree(_PACKAGE_FIXTURE, package_root)
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    build_script = textwrap.dedent(
        """\
        import setuptools.build_meta
        import sys
        setuptools.build_meta.build_wheel(sys.argv[1])
        """
    )
    build = subprocess.run(
        [sys.executable, "-c", build_script, str(dist_dir)],
        cwd=package_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert build.returncode == 0, build.stdout + build.stderr
    wheels = list(dist_dir.glob("*.whl"))
    assert len(wheels) == 1

    site_packages = tmp_path / "site-packages"
    with zipfile.ZipFile(wheels[0]) as wheel:
        wheel.extractall(site_packages)
    profiles_root = site_packages / "external_profile_fixture" / "profiles"
    assert (profiles_root / "external-schema" / "profile.yaml").is_file()
    assert (
        profiles_root
        / "external-schema"
        / "schemas"
        / "bridges"
        / "external_bridge.yaml"
    ).is_file()
    return profiles_root


def test_packaged_external_profile_schema_is_present_at_first_catalog_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profiles_root = _build_and_extract_fixture_wheel(tmp_path)
    deployments_root = tmp_path / "deployments"
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profiles_root))

    from okg.substrate.cli import provision
    from okg.substrate.db import migrate as db_migrate

    monkeypatch.setattr(
        db_migrate,
        "ensure_required_extensions",
        lambda _dsn: {"installed": [], "already_present": []},
    )
    monkeypatch.setattr(db_migrate, "migrate", lambda *_args, **_kwargs: None)
    # The fixture profile declares no sources; this test is about schema
    # assets, so plan one runnable source and let the faked `okg ingest` run
    # (issue #2331 refuses a registry with no sources at all).
    from okg.substrate.cli import init as init_mod

    monkeypatch.setattr(
        init_mod,
        "bare_ingest_selection",
        lambda _deployment_dir: {
            "runnable": ["fixture-source"],
            "awaiting_setup": [],
        },
    )
    monkeypatch.setattr(
        provision,
        "_provision_publish_proof",
        lambda *_args, **_kwargs: {
            "ok": True,
            "generation_id": "fixture-generation",
        },
    )

    empty_modules = tmp_path / "empty-modules"
    empty_cross_links = tmp_path / "empty-cross-links"
    empty_modules.mkdir()
    empty_cross_links.mkdir()
    calls: list[str] = []

    real_run = subprocess.run

    def fake_subprocess_run(argv, **_kwargs):
        command = list(argv)
        if command == ["git", "rev-parse", "--show-toplevel"]:
            # The scaffold writer asks whether the target is inside a Git
            # repository, which decides the `release:` block it writes
            # (issue #2127). Read-only, and not one of the staged external
            # effects this fake is sequencing, so it runs for real.
            return real_run(argv, **_kwargs)
        if "catalog" in command and "load" in command:
            assert calls == []
            deployment = Path(command[command.index("--deployment") + 1])
            composed = compose_catalog(
                deployment,
                modules_root=empty_modules,
                cross_links_root=empty_cross_links,
            )
            assert "external_record" in composed.subtypes
            assert [path.name for path in composed.bridge_paths] == [
                "external_bridge.yaml"
            ]
            assert (
                deployment / "schemas" / "external_domain.yaml"
            ).read_bytes() == (
                profiles_root
                / "external-schema"
                / "schemas"
                / "external_domain.yaml"
            ).read_bytes()
            calls.append("catalog")
        elif "ingest" in command:
            calls.append("ingest")
        elif "run" in command and "--once" in command:
            calls.append("publish")
        else:  # pragma: no cover - an unexpected external effect is a failure
            raise AssertionError(f"unexpected subprocess: {command}")
        stdout = "{}\n"
        if "run" in command and "--once" in command:
            # init reads the version its own run reports (#2422).
            stdout = (
                '{"run_summary": {"outcome": "published", '
                '"generation_id": "fixture-generation"}}\n'
            )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(subprocess, "run", fake_subprocess_run)

    argv = [
        "install",
        "--profile",
        "external-schema",
        "--deployment-name",
        "external-install",
        "--deployments-root",
        str(deployments_root),
        "--postgres-dsn",
        "postgresql://fixture:fixture@127.0.0.1:59999/fixture",
        "--non-interactive",
        "--json",
    ]
    assert "--no-publish" not in argv

    rc = cli.main(argv)

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["publish"]["ok"] is True
    assert calls == ["catalog", "ingest", "publish"]
    assert (
        deployments_root
        / "external-install"
        / "schemas"
        / "bridges"
        / "external_bridge.yaml"
    ).is_file()
