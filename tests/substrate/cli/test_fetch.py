"""Tests for `okg fetch-deployments` + the single profile resolver
`discover_profile` (per `separate-framework-and-deployments`
Phase 3 + `deployment-fetch-resolver`).

Strategy: stand up a tiny git repo in tmp, exercise fetch
against `file://`, then exercise the resolver against the
populated cache. There is exactly ONE profile resolver
(`discover_profile`); the divergent `resolve_profile` /
`list_available_profiles` pair was deleted.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.catalog.profile import (
    ProfileError, ProfileFetchError,
)
from okg.substrate.cli import fetch
from okg.substrate.deployment_bootstrap.profile_init import (
    discover_profile,
)


# ---------- helpers ----------


def _init_git_repo(root: Path, files: dict[str, str]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    for relpath, content in files.items():
        p = root / relpath
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
    subprocess.run(["git", "-C", str(root), "init",
                    "--quiet", "--initial-branch=main"],
                   check=True)
    subprocess.run(["git", "-C", str(root), "config",
                    "user.email", "test@example.com"],
                   check=True)
    subprocess.run(["git", "-C", str(root), "config",
                    "user.name", "Test"], check=True)
    subprocess.run(["git", "-C", str(root), "add", "-A"],
                   check=True)
    subprocess.run(["git", "-C", str(root), "commit", "--quiet",
                    "-m", "initial"], check=True)


# ---------- _slugify_repo_url ----------


def test_slugify_ssh_url():
    assert fetch._slugify_repo_url(
        "git@github.com:example-org/example-deployments.git"
    ) == "example-org-example-deployments"


def test_slugify_https_url():
    assert fetch._slugify_repo_url(
        "https://github.com/example-org/example-deployments.git"
    ) == "example-org-example-deployments"


def test_slugify_file_url():
    slug = fetch._slugify_repo_url(
        "file:///tmp/okg-deployments",
    )
    assert "okg-deployments" in slug


# ---------- fetch_or_update ----------


def test_fetch_or_update_clones_first_time(tmp_path):
    src = tmp_path / "src-repo"
    _init_git_repo(src, {
        "cms/profile.yaml": "name: cms\n",
        "cms/README.md": "# cms\n",
    })
    target = tmp_path / "cache" / "src-repo"
    result = fetch.fetch_or_update(
        f"file://{src}", target=target,
    )
    assert result == target
    assert (target / ".git").is_dir()
    assert (target / "cms" / "profile.yaml").is_file()


def test_fetch_or_update_pulls_existing_clone(tmp_path):
    src = tmp_path / "src-repo"
    _init_git_repo(src, {"a.txt": "first\n"})
    target = tmp_path / "cache" / "src-repo"
    # First fetch — clones.
    fetch.fetch_or_update(f"file://{src}", target=target)
    # Update src.
    (src / "a.txt").write_text("second\n")
    subprocess.run(["git", "-C", str(src), "add", "a.txt"], check=True)
    subprocess.run(["git", "-C", str(src), "commit", "--quiet",
                    "-m", "update"], check=True)
    # Second fetch — pulls.
    fetch.fetch_or_update(f"file://{src}", target=target)
    assert (target / "a.txt").read_text() == "second\n"


# ---------- list_profiles ----------


def test_list_profiles_finds_dirs_with_profile_yaml(tmp_path):
    root = tmp_path / "cache"
    (root / "cms").mkdir(parents=True)
    (root / "cms" / "profile.yaml").write_text("name: cms\n")
    (root / "af_pubs").mkdir(parents=True)
    (root / "af_pubs" / "profile.yaml").write_text("name: af_pubs\n")
    (root / "no_profile_yet").mkdir(parents=True)
    profiles = fetch.list_profiles(root)
    assert profiles == ["af_pubs", "cms"]


def test_list_profiles_returns_empty_when_cache_missing(tmp_path):
    assert fetch.list_profiles(tmp_path / "nope") == []


# ---------- cmd_fetch_deployments ----------


def test_cmd_fetch_deployments_clones_and_lists(
    tmp_path, monkeypatch, capsys,
):
    src = tmp_path / "src-repo"
    _init_git_repo(src, {
        "cms/profile.yaml": "name: cms\n",
        "af_pubs/profile.yaml": "name: af_pubs\n",
    })
    cache_home = tmp_path / "cache-home"
    monkeypatch.setenv("OKG_CACHE_HOME", str(cache_home))

    args = argparse.Namespace(
        repo=f"file://{src}",
        branch="main",
        source_name=None,
        list=False,
    )
    rc = fetch.cmd_fetch_deployments(args, as_json=True)
    assert rc == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["status"] == "ok"
    assert set(payload["profiles"]) == {"cms", "af_pubs"}


def test_cmd_fetch_deployments_list_mode_skips_fetch(
    tmp_path, monkeypatch, capsys,
):
    src = tmp_path / "src-repo"
    _init_git_repo(src, {"cms/profile.yaml": "name: cms\n"})
    cache_home = tmp_path / "cache-home"
    monkeypatch.setenv("OKG_CACHE_HOME", str(cache_home))

    # First populate the cache.
    args = argparse.Namespace(
        repo=f"file://{src}", branch="main",
        source_name=None, list=False,
    )
    fetch.cmd_fetch_deployments(args, as_json=True)
    capsys.readouterr()  # clear

    # Now run --list — should not re-fetch.
    args_list = argparse.Namespace(
        repo=f"file://{src}", branch="main",
        source_name=None, list=True,
    )
    rc = fetch.cmd_fetch_deployments(args_list, as_json=True)
    assert rc == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["profiles"] == ["cms"]


# ---------- discover_profile: the single resolver ----------


def _isolate_resolver_roots(monkeypatch, tmp_path):
    """Point every non-cache resolver root at empty tmp dirs so a
    cache-cascade test can't be satisfied by a real builtin/cwd
    profile of the same name (and vice versa)."""
    monkeypatch.setenv("OKG_CACHE_HOME", str(tmp_path / "cache"))
    # Empty OKG_PROFILES_DIR so the env-root cascade step misses.
    empty_profiles = tmp_path / "no-profiles"
    empty_profiles.mkdir()
    monkeypatch.setenv("OKG_PROFILES_DIR", str(empty_profiles))
    # Run from a dir with no ./profiles/ so the cwd step misses.
    monkeypatch.chdir(tmp_path)


def test_discover_profile_finds_builtin(monkeypatch, tmp_path):
    monkeypatch.setenv("OKG_CACHE_HOME", str(tmp_path / "cache"))
    # academic-researcher is a real built-in shipped with the substrate.
    profile = discover_profile("academic-researcher")
    assert profile.name == "academic-researcher"


def test_discover_profile_finds_cached(tmp_path, monkeypatch):
    """The single resolver consults the fetch cache as its last
    cascade step. With OKG_CACHE_HOME set, `_cache_dir()` returns it
    directly (no "okg" suffix); the deployments-cache lives at
    `<cache>/deployments/<source>/<name>/`."""
    _isolate_resolver_roots(monkeypatch, tmp_path)
    cache_home = tmp_path / "cache"
    source_dir = cache_home / "deployments" / "mitdbg-okg-deployments"
    cms_dir = source_dir / "fetched-reference-cms"
    cms_dir.mkdir(parents=True)
    (cms_dir / "profile.yaml").write_text(
        "name: fetched-reference-cms\nversion: 0.1.0\n",
    )

    profile = discover_profile("fetched-reference-cms")
    assert profile.name == "fetched-reference-cms"
    assert profile.path == cms_dir / "profile.yaml"


def test_discover_profile_iterates_all_cached_sources(
    tmp_path, monkeypatch,
):
    """A profile fetched from any per-source subdir resolves — the
    cascade iterates every `<cache>/deployments/<source>/`."""
    _isolate_resolver_roots(monkeypatch, tmp_path)
    deployments = tmp_path / "cache" / "deployments"
    # Decoy source without the target; real source with it.
    (deployments / "decoy-source").mkdir(parents=True)
    target_src = deployments / "second-source"
    prof = target_src / "only-here"
    prof.mkdir(parents=True)
    (prof / "profile.yaml").write_text(
        "name: only-here\nversion: 0.1.0\n",
    )

    profile = discover_profile("only-here")
    assert profile.name == "only-here"


def test_discover_profile_loud_when_cache_unpopulated(
    tmp_path, monkeypatch,
):
    """A missing profile with an EMPTY fetch cache fails loudly with
    ProfileFetchError naming the unconfigured-external-repo
    condition — not a silent None and not a misleading plain
    profile_invalid."""
    _isolate_resolver_roots(monkeypatch, tmp_path)
    with pytest.raises(ProfileFetchError) as excinfo:
        discover_profile("definitely-not-a-profile")
    err = excinfo.value
    assert err.code == "profile_fetch_failed"
    assert err.data["condition"] == "external-repo-unconfigured"
    # The actionable fetch hint is surfaced.
    assert "fetch-deployments" in (err.fix_hint or "")


def test_discover_profile_plain_notfound_when_cache_populated(
    tmp_path, monkeypatch,
):
    """When the cache IS populated but the name is genuinely absent,
    the error is the plain ProfileError (profile_invalid), not the
    unconfigured-repo condition."""
    _isolate_resolver_roots(monkeypatch, tmp_path)
    src = tmp_path / "cache" / "deployments" / "mitdbg-okg-deployments"
    other = src / "some-other-profile"
    other.mkdir(parents=True)
    (other / "profile.yaml").write_text(
        "name: some-other-profile\nversion: 0.1.0\n",
    )
    with pytest.raises(ProfileError) as excinfo:
        discover_profile("definitely-not-a-profile")
    # ProfileFetchError is a sibling, not a subclass — assert we got
    # the plain not-found, not the fetch-failed condition.
    assert not isinstance(excinfo.value, ProfileFetchError)
    assert excinfo.value.code == "profile_invalid"


# ---------- doc-truth: the documented fetch-then-init path works ----------


_DOC_PROFILE_YAML = """\
name: doc-fetch-probe
version: 0.1.0
description: in-test reference profile for the documented fetch path
init_questions:
  - id: deployment_name
    prompt: deployment name
    type: slug
    required: true
modules: modules.yaml
"""

_DOC_MODULES_YAML = "modules:\n  - reasoning\n"


def test_documented_fetch_then_init_path_executes(
    tmp_path, monkeypatch, capsys,
):
    """deployments/README.md tells operators to run
    `okg fetch-deployments` then `okg init --profile <name>`. This
    smoke asserts that exact path executes end-to-end: fetch
    populates the cache, init resolves the cached profile and
    scaffolds — with no unrecognized-argument and no profile_invalid
    error. Guards the README/cli-surface doc-truth claim."""
    import okg.substrate.cli.main as cli

    # Isolate every resolver root so resolution can ONLY succeed via
    # the fetch cache the documented `okg fetch-deployments` fills.
    cache_home = tmp_path / "cache"
    monkeypatch.setenv("OKG_CACHE_HOME", str(cache_home))
    empty_profiles = tmp_path / "no-profiles"
    empty_profiles.mkdir()
    monkeypatch.setenv("OKG_PROFILES_DIR", str(empty_profiles))
    monkeypatch.chdir(tmp_path)

    # Build a tiny reference-deployments repo and fetch it exactly as
    # `okg fetch-deployments` would (same command entry point).
    src = tmp_path / "ref-deployments"
    _init_git_repo(src, {
        "doc-fetch-probe/profile.yaml": _DOC_PROFILE_YAML,
        "doc-fetch-probe/modules.yaml": _DOC_MODULES_YAML,
    })
    fetch_args = argparse.Namespace(
        repo=f"file://{src}", branch="main",
        source_name="mitdbg-okg-deployments", list=False,
    )
    rc = fetch.cmd_fetch_deployments(fetch_args, as_json=True)
    assert rc == 0
    capsys.readouterr()  # clear fetch output

    # Now the documented `okg init --profile doc-fetch-probe`.
    init_args = SimpleNamespace(
        command="init",
        profile_name="doc-fetch-probe",
        template=None,
        deployment_name="doc_probe_dep",
        deployments_root=str(tmp_path / "deployments"),
        force=False,
        yes=False,
        no_publish=True,
        non_interactive=True,
        target=None, repo_root=None, openspec_root=None,
        claude_projects_root=None, notes_root=None, since=None,
    )
    rc = cli._cmd_init(init_args, as_json=True)
    out = capsys.readouterr().out

    # The documented path executed: profile RESOLVED from the cache
    # and scaffolded. No profile_invalid / profile_fetch_failed.
    assert rc == 0, out
    assert "profile_invalid" not in out
    assert "profile_fetch_failed" not in out
    assert "unrecognized arguments" not in out
    payload = json.loads(out)
    assert payload["profile"]["name"] == "doc-fetch-probe"
    dep_dir = tmp_path / "deployments" / "doc_probe_dep"
    assert (dep_dir / "deployment.yaml").is_file()
