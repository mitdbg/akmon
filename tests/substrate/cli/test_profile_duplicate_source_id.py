"""Two source-default files that declare one source id are refused (#2070).

`_build_source_registry_yaml` keyed the rendered `source_registry.yaml` by
source id and merged each file with `dict.update`. When two files in one
profile's `source-defaults/` declared the same id, the file that sorted last
replaced the other: `okg init` and the packaged `okg install` (both render
through `build_plan` + `planned_writes`) dropped a source and said nothing.
Archi hit it when an operator enabled `cmssw_releases_frozen.yaml.example`
by renaming it and kept the default `cmssw_releases.yaml`.

There is no precedence contract between files of one directory, so the
repeat is ambiguous: the render refuses with `source_default_duplicate_id`,
naming the id and both files, before any file is written. An optional
`.yaml.example` beside its default is not read at all, which is the
documented way to ship an alternative mode.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import okg.substrate.cli.main as cli
from okg.substrate.catalog.profile import ProfileError, load_profile
from okg.substrate.deployment_bootstrap.profile_init import (
    DuplicateSourceIdError,
    build_plan,
    planned_writes,
    scaffold_deployment,
)

LIVE = (
    "cmssw_releases:\n"
    "  class: okg.substrate.library.sources.git.GitSource\n"
    "  params:\n"
    "    mode: live\n"
)
FROZEN = (
    "cmssw_releases:\n"
    "  class: okg.substrate.library.sources.git.GitSource\n"
    "  params:\n"
    "    mode: frozen\n"
)
UNWRAPPED = (
    "class: okg.substrate.library.sources.git.GitSource\n"
    "params:\n"
    "  mode: live\n"
)
OTHER = (
    "twiki:\n"
    "  class: okg.substrate.library.sources.git.GitSource\n"
    "  params:\n"
    "    mode: live\n"
)


def _write_profile(root: Path, files: dict[str, str]) -> Path:
    """Write a minimal profile whose `source-defaults/` holds `files`."""
    profile_dir = root / "dup_probe"
    (profile_dir / "source-defaults").mkdir(parents=True)
    for name, body in files.items():
        (profile_dir / "source-defaults" / name).write_text(
            body, encoding="utf-8",
        )
    profile_path = profile_dir / "profile.yaml"
    profile_path.write_text(
        "name: dup_probe\n"
        "version: 1.0.0\n"
        "source_defaults: source-defaults\n",
        encoding="utf-8",
    )
    return profile_path


def _plan(profile_path: Path, tmp_path: Path):
    return build_plan(
        profile=load_profile(profile_path),
        deployment_name="dup_probe_dep",
        deployments_root=tmp_path / "deployments",
        raw_answers={},
    )


def _registry(profile_path: Path, tmp_path: Path) -> dict:
    result = scaffold_deployment(_plan(profile_path, tmp_path))
    registry = result.deployment_dir / "source_registry.yaml"
    return yaml.safe_load(registry.read_text(encoding="utf-8"))["sources"]


def test_two_files_declaring_one_id_are_refused_naming_both(
    tmp_path: Path,
) -> None:
    profile_path = _write_profile(tmp_path, {
        "cmssw_releases.yaml": LIVE,
        "cmssw_releases_frozen.yaml": FROZEN,
    })
    plan = _plan(profile_path, tmp_path)
    with pytest.raises(DuplicateSourceIdError) as caught:
        scaffold_deployment(plan)
    err = caught.value
    assert isinstance(err, ProfileError)
    assert err.code == "source_default_duplicate_id"
    directory = profile_path.parent / "source-defaults"
    first = str(directory / "cmssw_releases.yaml")
    second = str(directory / "cmssw_releases_frozen.yaml")
    assert err.data["source_id"] == "cmssw_releases"
    assert err.data["files"] == [first, second]
    assert first in str(err) and second in str(err)
    # Refused before anything is written: no deployment directory at all.
    assert not plan.deployment_dir.exists()


def test_planned_writes_refuses_too(tmp_path: Path) -> None:
    """`planned_writes` is what the dry-run plan lists; it raises the same
    error as the write itself. The command-level dry run is pinned below."""
    profile_path = _write_profile(tmp_path, {
        "cmssw_releases.yaml": LIVE,
        "cmssw_releases_frozen.yaml": FROZEN,
    })
    with pytest.raises(DuplicateSourceIdError):
        planned_writes(_plan(profile_path, tmp_path))


def _install_argv(root: Path, name: str, *extra: str) -> list[str]:
    return [
        "install", "--profile", "dup_probe",
        "--deployment-name", name,
        "--deployments-root", str(root),
        "--non-interactive", "--no-publish", *extra,
    ]


def test_okg_install_dry_run_json_emits_the_typed_refusal(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Through `cli.main`: `okg install --dry-run --json` used to crash
    with a DuplicateSourceIdError traceback, because the dry-run plan
    called `planned_writes` outside any error handler. It now emits the
    same envelope and exit code as the real `--scaffold-only` run."""
    profile_path = _write_profile(tmp_path / "profiles", {
        "cmssw_releases.yaml": LIVE,
        "cmssw_releases_frozen.yaml": FROZEN,
    })
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_path.parent.parent))
    root = tmp_path / "deployments"

    rc = cli.main(_install_argv(root, "dry_json", "--dry-run", "--json"))
    captured = capsys.readouterr()
    assert captured.out == ""
    envelope = json.loads(captured.err)
    error = envelope["error"]
    assert error["code"] == "source_default_duplicate_id"
    assert error["data"]["source_id"] == "cmssw_releases"
    assert [Path(f).name for f in error["data"]["files"]] == [
        "cmssw_releases.yaml", "cmssw_releases_frozen.yaml",
    ]
    assert "cmssw_releases_frozen.yaml" in error["fix_hint"]
    # A dry run writes nothing, not even the deployments root.
    assert not root.exists()

    real_rc = cli.main(
        _install_argv(root, "real_json", "--scaffold-only", "--json"),
    )
    real = json.loads(capsys.readouterr().err)
    assert rc == real_rc == 1
    assert real["error"]["code"] == error["code"]
    assert not (root / "real_json").exists()


def test_okg_install_dry_run_text_emits_the_typed_refusal(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    profile_path = _write_profile(tmp_path / "profiles", {
        "cmssw_releases.yaml": LIVE,
        "cmssw_releases_frozen.yaml": FROZEN,
    })
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_path.parent.parent))
    root = tmp_path / "deployments"

    rc = cli.main(_install_argv(root, "dry_text", "--dry-run"))
    captured = capsys.readouterr()
    assert rc == 1
    assert "Traceback" not in captured.err
    assert "[source_default_duplicate_id]" in captured.err
    assert "cmssw_releases_frozen.yaml" in captured.err
    assert not root.exists()


def _academic_researcher_override(
    tmp_path: Path, monkeypatch, *, duplicate: bool,
):
    """Copy academic-researcher under a temp `OKG_PROFILES_DIR`, optionally adding
    `dblp_frozen.yaml`, a second file that declares the `dblp` id.

    The profile CLI module itself is always loaded from the packaged
    profile, so only a check that resolves the profile the way the
    scaffold child does can see the copy.
    """
    import shutil

    from okg.substrate.cli.profile_cli_loader import get_profile_cli

    researcher_cli = get_profile_cli("academic-researcher").install
    shipped = Path(researcher_cli.__file__).resolve().parent.parent
    profiles = tmp_path / "profiles"
    copy = profiles / "academic-researcher"
    shutil.copytree(
        shipped, copy, ignore=shutil.ignore_patterns("__pycache__"),
    )
    if duplicate:
        source_defaults = copy / "source-defaults"
        shutil.copyfile(
            source_defaults / "dblp.yaml",
            source_defaults / "dblp_frozen.yaml",
        )
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profiles))
    return researcher_cli


def _record_docker(researcher_cli, monkeypatch) -> list[str]:
    """Stub every Docker entry point of the academic-researcher install and
    record each call. Nothing here starts or probes a real daemon."""
    import subprocess

    calls: list[str] = []

    def _docker_state() -> dict:
        calls.append("docker_state")
        return {
            "docker_on_path": True, "daemon_reachable": True,
            "image_present": True, "image_build_needed": False,
        }

    def _provision(*_args, **_kwargs) -> int:
        calls.append("provision_postgres_and_scaffold")
        return 1

    real_run = subprocess.run

    def _run(argv, *args, **kwargs):
        if argv and Path(str(argv[0])).name == "docker":
            calls.append("subprocess:docker")
            raise AssertionError(f"docker invoked: {argv!r}")
        return real_run(argv, *args, **kwargs)

    monkeypatch.setattr(researcher_cli, "_docker_state", _docker_state)
    monkeypatch.setattr(researcher_cli, "_provision_postgres_and_scaffold", _provision)
    monkeypatch.setattr(researcher_cli.subprocess, "run", _run)
    return calls


def _assert_dblp_refusal(captured) -> None:
    error = json.loads(captured.err)["error"]
    assert error["code"] == "source_default_duplicate_id"
    assert error["data"]["source_id"] == "dblp"
    assert [Path(f).name for f in error["data"]["files"]] == [
        "dblp.yaml", "dblp_frozen.yaml",
    ]
    # The refusal names the OVERRIDE copy, the files the child renders.
    assert all("profiles/academic-researcher" in f for f in error["data"]["files"])


@pytest.mark.parametrize("dry_run", [True, False], ids=["dry_run", "real"])
def test_academic_researcher_refuses_an_override_duplicate_before_docker(
    tmp_path: Path, monkeypatch, capsys, dry_run: bool,
) -> None:
    """The academic-researcher installer provisions Docker Postgres before its
    scaffold child renders, and the child resolves the profile through
    `OKG_PROFILES_DIR`. The pre-Docker check used to read the PACKAGED
    source-defaults, so a duplicate in an override passed it and was
    refused only after Docker had started. It now resolves the profile
    the way the child does, and refuses before any Docker call, on the
    dry run and on the real install alike."""
    researcher_cli = _academic_researcher_override(tmp_path, monkeypatch, duplicate=True)
    docker_calls = _record_docker(researcher_cli, monkeypatch)
    root = tmp_path / "deployments"
    argv = [
        "install", "--profile", "academic-researcher", "--name", "dupdblp",
        "--deployments-root", str(root), "--no-claude-config",
    ]
    if dry_run:
        argv.append("--dry-run")
    args = cli._build_parser(["install"]).parse_args(argv)

    rc = researcher_cli._cmd_install(args, as_json=True)
    captured = capsys.readouterr()
    assert rc == 1
    assert "Traceback" not in captured.err
    _assert_dblp_refusal(captured)
    assert docker_calls == []
    assert not (root / "dupdblp").exists()


def test_academic_researcher_refresh_dry_run_refuses_without_a_traceback(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """`--claude-config-only --dry-run` still takes the dry-run branch,
    which probes Docker and lists the sources. It used to skip the check,
    probe Docker, then crash with an uncaught DuplicateSourceIdError."""
    researcher_cli = _academic_researcher_override(tmp_path, monkeypatch, duplicate=True)
    docker_calls = _record_docker(researcher_cli, monkeypatch)
    args = cli._build_parser(["install"]).parse_args([
        "install", "--profile", "academic-researcher", "--name", "dupdblp",
        "--deployments-root", str(tmp_path / "deployments"),
        "--claude-config-only", "--dry-run", "--no-claude-config",
    ])

    rc = researcher_cli._cmd_install(args, as_json=True)
    captured = capsys.readouterr()
    assert rc == 1
    assert "Traceback" not in captured.err
    _assert_dblp_refusal(captured)
    assert docker_calls == []


def _reference_source_defaults(directory: Path) -> list[tuple[str, dict]]:
    """The loader academic-researcher shipped before issue #2070, kept here as the
    reference: sorted files, the one wrapping key, dict bodies only."""
    out: dict[str, dict] = {}
    for path in sorted(directory.glob("*.yaml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict) or len(raw) != 1:
            continue
        (source_id, body), = raw.items()
        if isinstance(body, dict):
            out[source_id] = body
    return list(out.items())


def test_academic_researcher_dry_run_without_a_duplicate_still_plans(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """The shipped academic-researcher source-defaults hold no repeat, so the
    shared check leaves every entry, its body and its order unchanged,
    and the dry-run plan lists the same sources in the same order."""
    from okg.substrate.cli.profile_cli_loader import get_profile_cli

    monkeypatch.delenv("OKG_PROFILES_DIR", raising=False)
    researcher_cli = get_profile_cli("academic-researcher").install
    monkeypatch.setattr(researcher_cli, "_docker_state", lambda: {
        "docker_on_path": True, "daemon_reachable": True,
        "image_present": True, "image_build_needed": False,
    })
    shipped = Path(researcher_cli.__file__).resolve().parent.parent / "source-defaults"
    reference = _reference_source_defaults(shipped)
    assert reference, "the shipped profile declares sources"
    loaded = researcher_cli._load_source_defaults(researcher_cli._find_repo_root())
    assert list(loaded.items()) == reference

    args = cli._build_parser(["install"]).parse_args([
        "install", "--profile", "academic-researcher", "--name", "cleanpi",
        "--deployments-root", str(tmp_path / "deployments"),
        "--dry-run", "--no-claude-config",
    ])
    rc = researcher_cli._cmd_install(args, as_json=True)
    plan = json.loads(capsys.readouterr().out)
    assert rc == 0, plan
    assert plan["status"] == "dry_run_ok"
    ordered = sorted(reference)
    expected = [
        {
            "source_id": source_id,
            "needs_connector": source_id in researcher_cli._CONNECTOR_SOURCE_IDS,
        }
        for enabled in (True, False)
        for source_id, body in ordered
        if bool(body.get("enabled_by_default", False)) is enabled
    ]
    assert plan["sources"]["total"] == len(reference)
    assert plan["sources"]["requires_setup_approval"] == expected


def test_a_stem_id_colliding_with_a_wrapped_id_is_refused(
    tmp_path: Path,
) -> None:
    """A file with no wrapping key takes its stem as the id; a second file
    that wraps its entry under that same id is the same ambiguity."""
    profile_path = _write_profile(tmp_path, {
        "cmssw_releases.yaml": UNWRAPPED,
        "zz_frozen.yaml": FROZEN,
    })
    with pytest.raises(DuplicateSourceIdError) as caught:
        scaffold_deployment(_plan(profile_path, tmp_path))
    assert caught.value.data["source_id"] == "cmssw_releases"
    assert [Path(f).name for f in caught.value.data["files"]] == [
        "cmssw_releases.yaml", "zz_frozen.yaml",
    ]


def test_distinct_ids_are_both_installed(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path, {
        "cmssw_releases.yaml": LIVE,
        "twiki.yaml": OTHER,
    })
    sources = _registry(profile_path, tmp_path)
    assert sorted(sources) == ["cmssw_releases", "twiki"]
    assert sources["cmssw_releases"]["params"]["mode"] == "live"


def test_an_example_beside_its_default_is_not_read(tmp_path: Path) -> None:
    """The shipped alternative stays inert until renamed; only then does
    the refusal apply."""
    profile_path = _write_profile(tmp_path, {
        "cmssw_releases.yaml": LIVE,
        "cmssw_releases_frozen.yaml.example": FROZEN,
    })
    sources = _registry(profile_path, tmp_path)
    assert list(sources) == ["cmssw_releases"]
    assert sources["cmssw_releases"]["params"]["mode"] == "live"


def test_okg_init_emits_the_typed_refusal(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Through the CLI dispatcher: non-zero exit, the typed code in the
    JSON envelope, and no deployment directory."""
    profile_path = _write_profile(tmp_path / "profiles", {
        "cmssw_releases.yaml": LIVE,
        "cmssw_releases_frozen.yaml": FROZEN,
    })
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_path.parent.parent))
    deployments_root = tmp_path / "deployments"
    args = SimpleNamespace(
        command="init",
        profile_name="dup_probe",
        template=None,
        deployment_name="dup_cli",
        deployments_root=str(deployments_root),
        force=False, yes=False, no_publish=True, non_interactive=True,
        target=None, repo_root=None, openspec_root=None,
        claude_projects_root=None, notes_root=None, since=None,
    )
    rc = cli._cmd_init(args, as_json=True)
    assert rc != 0
    captured = capsys.readouterr()
    envelope = json.loads((captured.err or captured.out).strip().splitlines()[-1])
    assert "source_default_duplicate_id" in json.dumps(envelope)
    assert "cmssw_releases_frozen.yaml" in json.dumps(envelope)
    assert not (deployments_root / "dup_cli").exists()


def test_academic_researcher_loads_source_defaults_outside_a_checkout(
    tmp_path: Path, monkeypatch,
) -> None:
    """Issue #2331: outside a source checkout `_find_repo_root` is None
    (#2313), and the pre-Docker source-default check crashed on
    `chdir(None)` before the install wrote anything. It now resolves the
    profile from the working directory the scaffold child inherits."""
    from okg.substrate.cli.profile_cli_loader import get_profile_cli

    monkeypatch.delenv("OKG_PROFILES_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    researcher_cli = get_profile_cli("academic-researcher").install
    assert researcher_cli._find_repo_root() is None
    shipped = Path(researcher_cli.__file__).resolve().parent.parent / "source-defaults"
    reference = _reference_source_defaults(shipped)
    assert reference, "the shipped profile declares sources"
    assert list(researcher_cli._load_source_defaults(None).items()) == reference
