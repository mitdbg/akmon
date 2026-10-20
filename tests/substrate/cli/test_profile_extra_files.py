"""A profile's `files:` slot installs deployment files where okg reads them.

A deployment such as comp-ops ships files that `okg migrate` and the runtime
read from the deployment directory: `views.sql` (deployment SQL views),
`aliases.json`, `extractors.yaml`, the MCP instructions, and others read by
fixed name. Before the `files:` slot a profile had no way to deliver them, so
every profile install needed a hand copy afterwards (#1185). These tests
install a profile into a temporary deployments root and read each file back
through the code that consumes it.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.profile import ProfileError, load_profile
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    planned_writes,
    scaffold_deployment,
)

DEPLOYMENT = "extra-files-probe"

VIEWS_SQL = (
    "CREATE OR REPLACE VIEW okg.v_extra_files_probe_one AS SELECT 1 AS one;\n"
)
ALIASES_JSON = '{"aliases": {}}\n'
EXTRACTORS_YAML = "# no backends: an empty mapping is a valid chain\n{}\n"
MCP_INSTRUCTIONS = "# Probe instructions\n\nAnswer from the graph.\n"
LIVE_AUTHORITY = "profiles: {}\n"


def _write_profile(root: Path, *, files: object, extra: dict[str, str]) -> Path:
    profile_dir = root / "bundles" / "probe"
    profile_dir.mkdir(parents=True)
    for rel, text in extra.items():
        path = profile_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    (profile_dir / "deployment-defaults.yaml").write_text(
        yaml.safe_dump({
            "aliases": "aliases.json",
            "mcp_instructions": "docs/mcp-instructions.md",
        }),
        encoding="utf-8",
    )
    body = {
        "name": "probe",
        "version": "0.1.0",
        "init_questions": [],
        "deployment_defaults": "deployment-defaults.yaml",
        "files": files,
    }
    (profile_dir / "profile.yaml").write_text(
        yaml.safe_dump(body), encoding="utf-8",
    )
    return profile_dir / "profile.yaml"


def _full_profile(root: Path) -> Path:
    extra = {
        "views.sql": VIEWS_SQL,
        "aliases.json": ALIASES_JSON,
        "extractors.yaml": EXTRACTORS_YAML,
        "docs/mcp-instructions.md": MCP_INSTRUCTIONS,
        "live_authority_profiles.yaml": LIVE_AUTHORITY,
    }
    return _write_profile(root, files=sorted(extra), extra=extra)


@pytest.fixture
def isolated_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    deployments_root = tmp_path / "deployments"
    deployments_root.mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    return deployments_root


def _install(profile_path: Path, deployments_root: Path) -> Path:
    plan = build_plan(
        profile=load_profile(profile_path),
        deployment_name=DEPLOYMENT,
        deployments_root=deployments_root,
        raw_answers={},
    )
    return scaffold_deployment(plan).deployment_dir


def test_profile_files_land_where_migrate_and_runtime_read_them(
    tmp_path: Path, isolated_roots: Path,
) -> None:
    from okg.substrate.db import migrate as migrate_module
    from okg.substrate.deployment_contract import _instruction_paths
    from okg.substrate.deployments import DeploymentManifest
    from okg.substrate.library.extraction.registry import load_extractors

    deployment_dir = _install(_full_profile(tmp_path), isolated_roots)

    # Every listed file is installed byte-for-byte at its relative path.
    assert (deployment_dir / "views.sql").read_text() == VIEWS_SQL
    assert (deployment_dir / "aliases.json").read_text() == ALIASES_JSON
    assert (deployment_dir / "extractors.yaml").read_text() == EXTRACTORS_YAML
    assert (
        deployment_dir / "docs" / "mcp-instructions.md"
    ).read_text() == MCP_INSTRUCTIONS
    assert (
        deployment_dir / "live_authority_profiles.yaml"
    ).read_text() == LIVE_AUTHORITY

    # `okg migrate --deployment <name>` applies the installed views.sql.
    sql, path = migrate_module._load_deployment_views(DEPLOYMENT)
    assert path == deployment_dir / "views.sql"
    assert sql == VIEWS_SQL

    # The runtime manifest resolves the keys the defaults declared onto the
    # installed files (strict load: a declared path must exist).
    manifest = DeploymentManifest.from_yaml(deployment_dir / "deployment.yaml")
    assert manifest.aliases == deployment_dir / "aliases.json"
    assert manifest.mcp_instructions == deployment_dir / "docs" / "mcp-instructions.md"
    assert deployment_dir / "docs" / "mcp-instructions.md" in _instruction_paths(
        yaml.safe_load((deployment_dir / "deployment.yaml").read_text()),
        deployment_dir,
    )

    # The extractor chain reads <deployment dir>/extractors.yaml.
    load_extractors(deployment_dir)


def test_dry_run_lists_profile_files(tmp_path: Path, isolated_roots: Path) -> None:
    plan = build_plan(
        profile=load_profile(_full_profile(tmp_path)),
        deployment_name=DEPLOYMENT,
        deployments_root=isolated_roots,
        raw_answers={},
    )
    written = {
        w.path.relative_to(plan.deployment_dir).as_posix() for w in planned_writes(plan)
    }
    assert {
        "views.sql",
        "aliases.json",
        "extractors.yaml",
        "docs/mcp-instructions.md",
        "live_authority_profiles.yaml",
    } <= written


def test_missing_views_resolves_to_no_deployment_sql(
    tmp_path: Path, isolated_roots: Path,
) -> None:
    from okg.substrate.db import migrate as migrate_module

    assert migrate_module._load_deployment_views("no-such-deployment") == (None, None)


def test_missing_listed_file_is_refused(tmp_path: Path, isolated_roots: Path) -> None:
    profile_path = _write_profile(
        tmp_path, files=["views.sql"], extra={},
    )
    with pytest.raises(ProfileError) as raised:
        _install(profile_path, isolated_roots)
    assert raised.value.data["reason"] == "missing_file"
    assert not (isolated_roots / DEPLOYMENT).exists()


@pytest.mark.parametrize(
    "entry, reason",
    [
        ("../outside.sql", "unsafe_path"),
        ("/etc/hosts", "unsafe_path"),
        ("docs\\x.md", "unsafe_path"),
    ],
)
def test_escaping_entry_is_refused(
    tmp_path: Path, entry: str, reason: str,
) -> None:
    profile_path = _write_profile(tmp_path, files=[entry], extra={})
    with pytest.raises(ProfileError) as raised:
        load_profile(profile_path)
    assert raised.value.data["key"] == "files"
    assert raised.value.data["reason"] == reason


def test_symlink_escape_is_refused(tmp_path: Path) -> None:
    outside = tmp_path / "outside.sql"
    outside.write_text("SELECT 1;\n", encoding="utf-8")
    profile_path = _write_profile(tmp_path, files=["views.sql"], extra={})
    (profile_path.parent / "views.sql").symlink_to(outside)
    with pytest.raises(ProfileError) as raised:
        load_profile(profile_path)
    assert raised.value.data["reason"] == "path_escape"


@pytest.mark.parametrize(
    "entries",
    [
        ["docs/README.md", "docs/readme.md"],
        ["views.sql", "./views.sql"],
        # NFC and NFD spellings of the same name.
        ["docs/caf\u00e9.md", "docs/cafe\u0301.md"],
    ],
)
def test_duplicate_entry_is_refused(tmp_path: Path, entries: list[str]) -> None:
    profile_path = _write_profile(tmp_path, files=entries, extra={})
    with pytest.raises(ProfileError) as raised:
        load_profile(profile_path)
    assert raised.value.data["reason"] == "duplicate"


def test_non_list_is_refused(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path, files="views.sql", extra={})
    with pytest.raises(ProfileError) as raised:
        load_profile(profile_path)
    assert raised.value.data["key"] == "files"


def test_nul_character_is_a_profile_error(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path, files=["views\x00.sql"], extra={})
    with pytest.raises(ProfileError) as raised:
        load_profile(profile_path)
    assert raised.value.data["reason"] == "unsafe_path"


@pytest.mark.parametrize(
    "entry",
    [
        # Deployment-local Python the runtime imports.
        "sources/evil.py",
        "enrichers/evil.py",
        "workers/evil.py",
        "agent_tools/evil.py",
        "attestors/evil.py",
        "views/evil.py",
        # The checked `schemas:` slot, and the generated files.
        "schemas/injected.yaml",
        "schemas",
        "deployment.yaml",
        "Deployment.yaml",
        "source_registry.yaml",
        "invariants.yaml",
        "skills/extra.md",
        # Version-control hooks and any executable.
        ".git/hooks/post-checkout",
        "evil.py",
        "docs/evil.py",
        "docs/run.sh",
        "docs/.hidden.md",
        # Allow-listed names are exact.
        "VIEWS.sql",
        "Docs/notes.md",
    ],
)
def test_entry_outside_the_allow_list_is_refused(
    tmp_path: Path, entry: str,
) -> None:
    profile_path = _write_profile(tmp_path, files=[entry], extra={})
    with pytest.raises(ProfileError) as raised:
        load_profile(profile_path)
    assert raised.value.data["key"] == "files"
    assert raised.value.data["reason"] == "not_allowed"
    assert "views.sql" in str(raised.value)


def test_allow_list_lives_in_one_place() -> None:
    from okg.substrate.catalog.profile import PROFILE_FILES_ALLOWED_ROOT

    assert PROFILE_FILES_ALLOWED_ROOT == {
        "views.sql",
        "aliases.json",
        "extractors.yaml",
        "live_authority_profiles.yaml",
        "identifier_patterns.yaml",
        "linker_config.yaml",
        "mcp_instructions.md",
    }


def test_chained_symlink_escape_is_refused(tmp_path: Path) -> None:
    outside = tmp_path / "outside.sql"
    outside.write_text("SELECT 1;\n", encoding="utf-8")
    profile_path = _write_profile(tmp_path, files=["views.sql"], extra={})
    (profile_path.parent / "hop.sql").symlink_to(outside)
    (profile_path.parent / "views.sql").symlink_to(profile_path.parent / "hop.sql")
    with pytest.raises(ProfileError) as raised:
        load_profile(profile_path)
    assert raised.value.data["reason"] == "path_escape"


def test_directory_symlink_escape_is_refused(tmp_path: Path) -> None:
    outside = tmp_path / "outside-docs"
    outside.mkdir()
    (outside / "notes.md").write_text("# outside\n", encoding="utf-8")
    profile_path = _write_profile(tmp_path, files=["docs/notes.md"], extra={})
    (profile_path.parent / "docs").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ProfileError) as raised:
        load_profile(profile_path)
    assert raised.value.data["reason"] == "path_escape"


def test_symlink_inside_the_profile_installs_its_bytes(
    tmp_path: Path, isolated_roots: Path,
) -> None:
    profile_path = _write_profile(
        tmp_path, files=["views.sql"], extra={"sql/real.sql": VIEWS_SQL},
    )
    (profile_path.parent / "views.sql").symlink_to(
        profile_path.parent / "sql" / "real.sql",
    )
    deployment_dir = _install(profile_path, isolated_roots)
    installed = deployment_dir / "views.sql"
    assert not installed.is_symlink()
    assert installed.read_text() == VIEWS_SQL


def _plan_with_forced_files(
    tmp_path: Path, deployments_root: Path, entries: list[str],
):
    """A plan whose `files:` bypass the loader's allow-list.

    The allow-list makes a collision with a generated file unreachable from
    `profile.yaml`; the scaffold still refuses one, and this proves it.
    """
    import dataclasses

    extra = {"views.sql": VIEWS_SQL}
    profile = load_profile(
        _write_profile(tmp_path, files=["views.sql"], extra=extra),
    )
    source = profile.profile_dir / "views.sql"
    profile = dataclasses.replace(
        profile, extra_files=tuple((entry, source) for entry in entries),
    )
    return build_plan(
        profile=profile,
        deployment_name=DEPLOYMENT,
        deployments_root=deployments_root,
        raw_answers={},
    )


@pytest.mark.parametrize(
    "entry",
    [
        "deployment.yaml",
        "Deployment.yaml",
        "source_registry.yaml",
        "schemas",
        "schemas/extra_files_probe_base.yaml",
    ],
)
def test_scaffold_refuses_a_file_that_collides_with_a_generated_one(
    tmp_path: Path, isolated_roots: Path, entry: str,
) -> None:
    from okg.substrate.deployment_bootstrap.profile_init import _schema_identifier

    assert _schema_identifier(DEPLOYMENT) == "extra_files_probe"
    plan = _plan_with_forced_files(tmp_path, isolated_roots, [entry])
    with pytest.raises(ProfileError) as raised:
        scaffold_deployment(plan)
    assert raised.value.data["key"] == "files"
    assert raised.value.data["reason"] == "destination_collision"
    assert not (isolated_roots / DEPLOYMENT).exists()


def test_files_do_not_change_how_skills_are_judged(
    tmp_path: Path, isolated_roots: Path,
) -> None:
    """Skills and dashboards stay exempt from the collision check whether or
    not the profile lists `files:`."""
    import dataclasses

    plan = build_plan(
        profile=load_profile(_full_profile(tmp_path)),
        deployment_name=DEPLOYMENT,
        deployments_root=isolated_roots,
        raw_answers={},
    )
    plan = dataclasses.replace(
        plan, skills=[("README.md", "a\n"), ("readme.md", "b\n")],
    )
    written = {w.path.relative_to(plan.deployment_dir).as_posix() for w in planned_writes(plan)}
    assert {"skills/README.md", "skills/readme.md", "views.sql"} <= written


def test_migrate_reads_views_of_a_deployment_outside_this_checkout(
    isolated_roots: Path,
) -> None:
    """`okg migrate --deployment <name>` resolves the deployment like every
    other command, not only this checkout's `deployments/<name>/`."""
    from okg.substrate.db import migrate as migrate_module

    deployment_dir = isolated_roots / "outside-probe"
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text(
        "name: outside-probe\n", encoding="utf-8",
    )
    (deployment_dir / "views.sql").write_text(VIEWS_SQL, encoding="utf-8")

    sql, path = migrate_module._load_deployment_views("outside-probe")
    assert path == deployment_dir / "views.sql"
    assert sql == VIEWS_SQL


def test_profile_explain_lists_files(tmp_path: Path) -> None:
    from okg.substrate.deployment_profile_explain import resolve_profile_explain

    explanation = resolve_profile_explain(_full_profile(tmp_path)).to_dict()
    packs = {pack["kind"]: pack for pack in explanation["asset_packs"]}
    assert packs["files"]["files"] == [
        "aliases.json",
        "docs/mcp-instructions.md",
        "extractors.yaml",
        "live_authority_profiles.yaml",
        "views.sql",
    ]
