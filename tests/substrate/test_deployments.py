"""Unit tests for `okg.substrate.deployments` — the canonical
deployment loader. Per `unify-deployment-loader` (2026-04-27).

These tests exercise the discovery cascade, sys.path injection,
and manifest parsing in isolation — no DB required.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
import yaml


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _write_manifest(
    deployment_dir: Path, *, name: str | None = None,
    schema_dir: str = "schemas",
    source_registry: str = "source_registry.yaml",
    extra: dict | None = None,
) -> Path:
    """Write a minimal valid deployment.yaml. Required dirs/files
    are created (empty) so the manifest's path-existence checks
    pass.
    """
    deployment_dir.mkdir(parents=True, exist_ok=True)
    (deployment_dir / schema_dir).mkdir(exist_ok=True)
    (deployment_dir / source_registry).touch()

    manifest = {
        "name": name or deployment_dir.name,
        "description": "test",
        "schema_dir": schema_dir,
        "source_registry": source_registry,
    }
    if extra:
        manifest.update(extra)
    p = deployment_dir / "deployment.yaml"
    p.write_text(yaml.safe_dump(manifest))
    return p


@pytest.fixture
def synth_root(tmp_path: Path):
    """A synthetic deployments root with a single 'demo'
    deployment inside.
    """
    root = tmp_path / "deployments"
    _write_manifest(root / "demo")
    return root


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch):
    """Strip env vars + restore sys.path between tests so cascade
    behavior is observable.
    """
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    monkeypatch.delenv("PZ_OKG_DEPLOYMENT", raising=False)
    saved = list(sys.path)
    try:
        yield
    finally:
        sys.path[:] = saved


# ---------------------------------------------------------------------------
# Discovery cascade
# ---------------------------------------------------------------------------

class TestFindDeploymentsRoot:

    def test_explicit_override_wins(self, synth_root, monkeypatch):
        from okg.substrate.deployments import find_deployments_root

        # Even if env points elsewhere, override wins.
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", "/no/such/path")
        # And also: env points to a bad path, but override resolves
        # — should NOT raise on env because override is checked
        # first.
        result = find_deployments_root(
            deployment_dir_override=str(synth_root),
        )
        assert result == synth_root

    def test_env_var_resolves_when_override_absent(
        self, synth_root, monkeypatch,
    ):
        from okg.substrate.deployments import find_deployments_root

        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(synth_root))
        assert find_deployments_root() == synth_root

    def test_bad_override_raises_loudly(self, monkeypatch):
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            find_deployments_root,
        )

        with pytest.raises(DeploymentResolutionError) as excinfo:
            find_deployments_root(
                deployment_dir_override="/no/such/path",
            )
        assert "/no/such/path" in str(excinfo.value)

    def test_bad_env_var_raises_loudly(self, monkeypatch):
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            find_deployments_root,
        )

        monkeypatch.setenv(
            "OKG_DEPLOYMENTS_DIR", "/definitely/not/here",
        )
        with pytest.raises(DeploymentResolutionError) as excinfo:
            find_deployments_root()
        assert "/definitely/not/here" in str(excinfo.value)
        assert "OKG_DEPLOYMENTS_DIR" in str(excinfo.value)

    def test_repo_root_walk_finds_local_deployments(
        self, tmp_path, monkeypatch,
    ):
        """When run from inside a repo (pyproject.toml sentinel),
        the cascade resolves to that repo's deployments/ even
        without env var.
        """
        from okg.substrate.deployments import find_deployments_root

        repo = tmp_path / "fake_repo"
        repo.mkdir()
        (repo / "pyproject.toml").touch()
        (repo / "deployments").mkdir()
        # cwd to inside the repo's source tree
        nested = repo / "src" / "pkg"
        nested.mkdir(parents=True)
        monkeypatch.chdir(nested)

        result = find_deployments_root()
        assert result == repo / "deployments"

    def test_in_tree_fallback_when_no_repo_no_env(
        self, tmp_path, monkeypatch,
    ):
        """Outside any repo and with no env, returns the substrate
        package's own in-tree deployments/ if it exists.
        """
        from okg.substrate.deployments import find_deployments_root

        # cwd somewhere that has no pyproject / no deployments
        empty = tmp_path / "empty"
        empty.mkdir()
        monkeypatch.chdir(empty)

        result = find_deployments_root()
        # In this repo's own working tree, the in-tree fallback
        # IS the okg repo's deployments/. Just assert it's a dir
        # named "deployments" — content may vary.
        assert result is None or result.name == "deployments"


# ---------------------------------------------------------------------------
# find_deployment_dir
# ---------------------------------------------------------------------------

class TestFindDeploymentDir:

    def test_existing_deployment_returns_path(
        self, synth_root, monkeypatch,
    ):
        from okg.substrate.deployments import find_deployment_dir

        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(synth_root))
        result = find_deployment_dir("demo")
        assert result == synth_root / "demo"

    def test_missing_deployment_returns_none(
        self, synth_root, monkeypatch,
    ):
        from okg.substrate.deployments import find_deployment_dir

        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(synth_root))
        assert find_deployment_dir("nonexistent") is None

    def test_explicit_override_kwarg(self, synth_root):
        from okg.substrate.deployments import find_deployment_dir

        result = find_deployment_dir(
            "demo", deployment_dir_override=str(synth_root),
        )
        assert result == synth_root / "demo"


# ---------------------------------------------------------------------------
# XDG paths (per `separate-framework-and-deployments`)
# ---------------------------------------------------------------------------


class TestXdgPaths:

    def test_user_data_dir_uses_okg_user_home_override(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import _user_data_dir
        monkeypatch.setenv("OKG_USER_HOME", str(tmp_path))
        assert _user_data_dir() == tmp_path

    def test_user_data_dir_uses_xdg_data_home(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import _user_data_dir
        monkeypatch.delenv("OKG_USER_HOME", raising=False)
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))
        assert _user_data_dir() == tmp_path / "okg"

    def test_user_data_dir_falls_back_to_home(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import _user_data_dir
        monkeypatch.delenv("OKG_USER_HOME", raising=False)
        monkeypatch.delenv("XDG_DATA_HOME", raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))
        assert _user_data_dir() == tmp_path / ".local/share/okg"

    def test_cache_dir_uses_okg_cache_home_override(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import _cache_dir
        monkeypatch.setenv("OKG_CACHE_HOME", str(tmp_path))
        assert _cache_dir() == tmp_path

    def test_cache_dir_uses_xdg_cache_home(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import _cache_dir
        monkeypatch.delenv("OKG_CACHE_HOME", raising=False)
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        assert _cache_dir() == tmp_path / "okg"

    def test_user_state_deployments_root(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import (
            user_state_deployments_root,
        )
        monkeypatch.setenv("OKG_USER_HOME", str(tmp_path))
        assert (
            user_state_deployments_root()
            == tmp_path / "deployments"
        )

    def test_default_deployment_target(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import (
            default_deployment_target,
        )
        monkeypatch.setenv("OKG_USER_HOME", str(tmp_path))
        assert (
            default_deployment_target("einstein")
            == tmp_path / "deployments" / "einstein"
        )

    def test_user_state_chat_instances_root_is_not_deployments_root(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import (
            user_state_chat_instances_root,
            user_state_deployments_root,
        )
        monkeypatch.setenv("OKG_USER_HOME", str(tmp_path))
        assert user_state_chat_instances_root() == tmp_path / "chat-instances"
        assert user_state_chat_instances_root() != user_state_deployments_root()


class TestMultiRootCascade:

    def test_user_state_takes_precedence_over_in_repo(
        self, tmp_path, monkeypatch,
    ):
        """When a deployment exists in BOTH user-state AND
        in-repo locations, the user-state one wins (per the
        cascade order)."""
        from okg.substrate.deployments import find_deployment_dir

        # User-state location.
        user_home = tmp_path / "user-home"
        _write_manifest(user_home / "deployments" / "twin")
        # In-repo location (simulated via OKG_DEPLOYMENTS_DIR
        # would short-circuit; use cwd-walk by chdir to a tmp
        # repo).
        repo_root = tmp_path / "repo"
        _write_manifest(repo_root / "deployments" / "twin")
        (repo_root / "pyproject.toml").write_text("[project]\n")

        monkeypatch.setenv("OKG_USER_HOME", str(user_home))
        monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
        monkeypatch.chdir(repo_root)
        result = find_deployment_dir("twin")
        # User-state wins.
        assert result == (
            user_home / "deployments" / "twin"
        )

    def test_falls_through_to_in_repo_when_user_state_missing(
        self, tmp_path, monkeypatch,
    ):
        """If user-state doesn't have the deployment, fall
        through to in-repo."""
        from okg.substrate.deployments import find_deployment_dir

        # User-state exists but doesn't have the deployment.
        user_home = tmp_path / "user-home"
        (user_home / "deployments").mkdir(parents=True)
        # In-repo has it.
        repo_root = tmp_path / "repo"
        _write_manifest(repo_root / "deployments" / "only-in-repo")
        (repo_root / "pyproject.toml").write_text("[project]\n")

        monkeypatch.setenv("OKG_USER_HOME", str(user_home))
        monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
        monkeypatch.chdir(repo_root)
        result = find_deployment_dir("only-in-repo")
        assert result == (
            repo_root / "deployments" / "only-in-repo"
        )

    def test_skips_user_state_directory_without_manifest(
        self, tmp_path, monkeypatch,
    ):
        """A record-only higher-priority directory cannot shadow the repo."""
        from okg.substrate.deployments import find_deployment_dir

        user_home = tmp_path / "user-home"
        (user_home / "deployments" / "twin").mkdir(parents=True)
        repo_root = tmp_path / "repo"
        _write_manifest(repo_root / "deployments" / "twin")
        (repo_root / "pyproject.toml").write_text("[project]\n")

        monkeypatch.setenv("OKG_USER_HOME", str(user_home))
        monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
        monkeypatch.chdir(repo_root)

        assert find_deployment_dir("twin") == repo_root / "deployments" / "twin"

    def test_resolution_error_lists_checked_roots(
        self, tmp_path, monkeypatch,
    ):
        """A failed lookup reports the roots it actually inspected."""
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            resolve_deployment,
        )

        user_home = tmp_path / "user-home"
        (user_home / "deployments" / "twin").mkdir(parents=True)
        repo_root = tmp_path / "repo"
        (repo_root / "deployments").mkdir(parents=True)
        (repo_root / "pyproject.toml").write_text("[project]\n")

        monkeypatch.setenv("OKG_USER_HOME", str(user_home))
        monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
        monkeypatch.chdir(repo_root)

        with pytest.raises(DeploymentResolutionError) as caught:
            resolve_deployment("twin")
        message = str(caught.value)
        assert str(user_home / "deployments" / "twin" / "deployment.yaml") in message
        assert str(repo_root / "deployments" / "twin" / "deployment.yaml") in message

    def test_skips_symlinked_manifest(self, tmp_path, monkeypatch):
        """Discovery does not follow a manifest symlink from a lookup root."""
        from okg.substrate.deployments import find_deployment_dir

        user_home = tmp_path / "user-home"
        stray = user_home / "deployments" / "twin"
        stray.mkdir(parents=True)
        target = tmp_path / "outside-deployment.yaml"
        target.write_text("name: twin\n")
        (stray / "deployment.yaml").symlink_to(target)
        repo_root = tmp_path / "repo"
        _write_manifest(repo_root / "deployments" / "twin")
        (repo_root / "pyproject.toml").write_text("[project]\n")

        monkeypatch.setenv("OKG_USER_HOME", str(user_home))
        monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
        monkeypatch.chdir(repo_root)

        assert find_deployment_dir("twin") == repo_root / "deployments" / "twin"


# ---------------------------------------------------------------------------
# inject_deployment_path (idempotent)
# ---------------------------------------------------------------------------

class TestInjectDeploymentPath:

    def test_first_call_inserts_at_index_0(self, tmp_path):
        from okg.substrate.deployments import inject_deployment_path

        d = tmp_path / "dep"
        d.mkdir()
        inject_deployment_path(d)
        assert sys.path[0] == str(d)

    def test_second_call_is_idempotent(self, tmp_path):
        from okg.substrate.deployments import inject_deployment_path

        d = tmp_path / "dep"
        d.mkdir()
        inject_deployment_path(d)
        inject_deployment_path(d)
        # Exactly one entry, at index 0
        assert sys.path.count(str(d)) == 1
        assert sys.path[0] == str(d)

    def test_existing_path_moves_to_index_0(self, tmp_path):
        from okg.substrate.deployments import inject_deployment_path

        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        inject_deployment_path(first)
        inject_deployment_path(second)
        inject_deployment_path(first)

        assert sys.path.count(str(first.resolve())) == 1
        assert sys.path[0] == str(first.resolve())

    def test_switching_deployments_evicts_local_package_cache(
        self,
        tmp_path,
    ):
        from okg.substrate.deployments import inject_deployment_path

        first = tmp_path / "first"
        second = tmp_path / "second"
        for deployment, value in ((first, "first"), (second, "second")):
            package = deployment / "enrichers"
            package.mkdir(parents=True)
            (package / "__init__.py").write_text(
                f"DEPLOYMENT = {value!r}\n",
            )

        try:
            inject_deployment_path(first)
            assert importlib.import_module("enrichers").DEPLOYMENT == "first"
            inject_deployment_path(second)
            assert (
                importlib.import_module("enrichers").DEPLOYMENT
                == "second"
            )
        finally:
            for name in list(sys.modules):
                if name == "enrichers" or name.startswith("enrichers."):
                    sys.modules.pop(name, None)


# ---------------------------------------------------------------------------
# resolve_deployment (full path: discovery + manifest + sys.path)
# ---------------------------------------------------------------------------

class TestResolveDeployment:

    def test_named_deployment_via_env_var(
        self, synth_root, monkeypatch,
    ):
        from okg.substrate.deployments import resolve_deployment

        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(synth_root))
        manifest = resolve_deployment("demo")
        assert manifest.name == "demo"
        assert manifest.deployment_dir == synth_root / "demo"
        # sys.path injected
        assert sys.path[0] == str(synth_root / "demo")

    def test_named_deployment_via_override(self, synth_root):
        from okg.substrate.deployments import resolve_deployment

        manifest = resolve_deployment(
            "demo", deployment_dir_override=str(synth_root),
        )
        assert manifest.name == "demo"

    def test_named_deployment_falls_through_to_user_state(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import resolve_deployment

        repo_root = tmp_path / "repo-deployments"
        repo_root.mkdir()
        user_home = tmp_path / "user-home"
        user_deployment = user_home / "deployments" / "external"
        _write_manifest(user_deployment)
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(repo_root))
        monkeypatch.setenv("OKG_USER_HOME", str(user_home))

        manifest = resolve_deployment("external")

        assert manifest.name == "external"
        assert manifest.deployment_dir == user_deployment

    def test_missing_named_deployment_raises(
        self, synth_root, monkeypatch,
    ):
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            resolve_deployment,
        )

        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(synth_root))
        with pytest.raises(DeploymentResolutionError) as excinfo:
            resolve_deployment("nope")
        assert "nope" in str(excinfo.value)

    def test_auto_detect_single_deployment(
        self, synth_root, monkeypatch,
    ):
        """When the deployments root has exactly one deployment
        and no name is given, auto-detect picks it.
        """
        from okg.substrate.deployments import resolve_deployment

        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(synth_root))
        manifest = resolve_deployment()
        assert manifest.name == "demo"

    def test_auto_detect_multiple_raises(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            resolve_deployment,
        )

        root = tmp_path / "deployments"
        _write_manifest(root / "alpha")
        _write_manifest(root / "beta")
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

        with pytest.raises(DeploymentResolutionError) as excinfo:
            resolve_deployment()
        msg = str(excinfo.value)
        assert "alpha" in msg and "beta" in msg

    def test_auto_detect_zero_raises(self, tmp_path, monkeypatch):
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            resolve_deployment,
        )

        empty_root = tmp_path / "deployments"
        empty_root.mkdir()
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(empty_root))

        with pytest.raises(DeploymentResolutionError) as excinfo:
            resolve_deployment()
        assert "No deployments found" in str(excinfo.value)

    def test_okg_deployment_env_falls_through_to_name(
        self, synth_root, monkeypatch,
    ):
        """`OKG_DEPLOYMENT` is the canonical env var: name=None resolves
        through it with no deprecation warning."""
        import warnings

        from okg.substrate.deployments import resolve_deployment

        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(synth_root))
        monkeypatch.setenv("OKG_DEPLOYMENT", "demo")
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            manifest = resolve_deployment()  # name=None, falls to env
        assert manifest.name == "demo"

    def test_pz_okg_deployment_env_falls_through_to_name_with_deprecation(
        self, synth_root, monkeypatch,
    ):
        """The deprecated `PZ_OKG_DEPLOYMENT` alias still resolves but emits a
        DeprecationWarning so callers migrate to OKG_DEPLOYMENT."""
        from okg.substrate.deployments import resolve_deployment

        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(synth_root))
        monkeypatch.setenv("PZ_OKG_DEPLOYMENT", "demo")
        with pytest.warns(DeprecationWarning, match="PZ_OKG_DEPLOYMENT"):
            manifest = resolve_deployment()  # name=None, falls to env
        assert manifest.name == "demo"

    def test_okg_deployment_env_wins_over_pz_alias(
        self, synth_root, monkeypatch,
    ):
        """When both are set, the canonical OKG_DEPLOYMENT wins and the
        deprecated alias is not consulted (no warning)."""
        import warnings

        from okg.substrate.deployments import resolve_deployment

        _write_manifest(synth_root / "other")
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(synth_root))
        monkeypatch.setenv("OKG_DEPLOYMENT", "demo")
        monkeypatch.setenv("PZ_OKG_DEPLOYMENT", "other")
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            manifest = resolve_deployment()  # name=None, falls to env
        assert manifest.name == "demo"

    def test_path_shaped_name_resolves(self, synth_root):
        """`name` may be an absolute path to deployment.yaml or
        its containing dir. Common for downstream-repo callers.
        """
        from okg.substrate.deployments import resolve_deployment

        # Pass the directory itself as the "name"
        manifest = resolve_deployment(str(synth_root / "demo"))
        assert manifest.name == "demo"

    def test_resolve_is_idempotent_on_syspath(
        self, synth_root, monkeypatch,
    ):
        from okg.substrate.deployments import resolve_deployment

        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(synth_root))
        resolve_deployment("demo")
        resolve_deployment("demo")
        assert sys.path.count(str(synth_root / "demo")) == 1

    def test_workspace_graph_canonical_deployments(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            find_deployment_dir,
            resolve_deployment,
        )

        root = tmp_path / "deployments"
        _write_manifest(root / "okg-workspace")
        _write_manifest(root / "okg-workspace-qa")
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

        assert find_deployment_dir("okg-workspace") == root / "okg-workspace"
        assert (
            find_deployment_dir("okg-workspace-qa")
            == root / "okg-workspace-qa"
        )
        assert find_deployment_dir("okg-workspace") == root / "okg-workspace"

        workspace = resolve_deployment("okg-workspace")
        workspace_qa = resolve_deployment("okg-workspace-qa")

        assert workspace.name == "okg-workspace"
        assert workspace.deployment_dir == root / "okg-workspace"
        assert workspace_qa.name == "okg-workspace-qa"
        assert workspace_qa.deployment_dir == root / "okg-workspace-qa"

        retired_name = "local" + "-project"
        retired_qa_name = retired_name + "-qa"
        assert find_deployment_dir(retired_name) is None
        assert find_deployment_dir(retired_qa_name) is None
        with pytest.raises(DeploymentResolutionError):
            resolve_deployment(retired_name)


# ---------------------------------------------------------------------------
# Manifest placeholder substitution
# ---------------------------------------------------------------------------

class TestManifestPlaceholders:

    def test_manifest_dir_placeholder_substitutes(
        self, tmp_path,
    ):
        from okg.substrate.deployments import DeploymentManifest

        dep = tmp_path / "demo"
        dep.mkdir()
        (dep / "schemas").mkdir()
        (dep / "src.yaml").touch()
        manifest_path = dep / "deployment.yaml"
        manifest_path.write_text(yaml.safe_dump({
            "name": "demo",
            "description": "test",
            "schema_dir": "${manifest_dir}/schemas",
            "source_registry": "${manifest_dir}/src.yaml",
        }))
        m = DeploymentManifest.from_yaml(manifest_path)
        # Substitution produces an absolute path; the manifest
        # then resolves relative to deployment_dir, which double-
        # roots — but the path should resolve to an existing dir.
        assert m.schema_dir.exists()
        assert m.source_registry.exists()

    def test_repo_root_placeholder_substitutes_for_repo_deployment(
        self, tmp_path,
    ):
        from okg.substrate.deployments import DeploymentManifest

        dep = tmp_path / "repo" / "deployments" / "demo"
        dep.mkdir(parents=True)
        (dep / "schemas").mkdir()
        (dep / "src.yaml").touch()
        manifest_path = dep / "deployment.yaml"
        manifest_path.write_text(yaml.safe_dump({
            "name": "demo",
            "description": "test",
            "schema_dir": "schemas",
            "source_registry": "src.yaml",
            "modules_config": {
                "git_graph": {
                    "repo_root": "${repo_root}",
                    "deployment_dir": "${deployment_dir}",
                }
            },
        }))

        m = DeploymentManifest.from_yaml(manifest_path)

        assert m.modules_config["git_graph"]["repo_root"] == str(tmp_path / "repo")
        assert m.modules_config["git_graph"]["deployment_dir"] == str(dep)

    def test_env_var_placeholder_passes_through_unchanged(
        self, tmp_path,
    ):
        """`${VAR}` placeholders other than `${manifest_dir}` are
        env-var references that consumers (e.g. the DSN resolver in
        `cli/main.py`) expand at use time. The parser leaves them
        in place so downstream env-var expansion still has something
        to expand."""
        from okg.substrate.deployments import DeploymentManifest

        dep = tmp_path / "demo"
        dep.mkdir()
        (dep / "schemas").mkdir()
        (dep / "src.yaml").touch()
        manifest_path = dep / "deployment.yaml"
        manifest_path.write_text(yaml.safe_dump({
            "name": "demo",
            "description": "${user_home}/foo",
            "schema_dir": "schemas",
            "source_registry": "src.yaml",
            "postgres": {"dsn": "${OKG_DSN}"},
        }))
        m = DeploymentManifest.from_yaml(manifest_path)
        # Description preserved verbatim; consumer expands later.
        assert m.description == "${user_home}/foo"

    def test_agent_tools_are_preserved_raw(self, tmp_path):
        from okg.substrate.deployments import DeploymentManifest

        dep = tmp_path / "demo"
        dep.mkdir()
        (dep / "schemas").mkdir()
        (dep / "src.yaml").touch()
        manifest_path = dep / "deployment.yaml"
        manifest_path.write_text(yaml.safe_dump({
            "name": "demo",
            "description": "test",
            "schema_dir": "schemas",
            "source_registry": "src.yaml",
            "agent_tools": {
                "demo_echo": {
                    "module": "tools",
                    "function": "echo",
                    "boundary": "external_read",
                    "description": "Echo input.",
                }
            },
        }))

        m = DeploymentManifest.from_yaml(manifest_path)

        assert m.agent_tools == {
            "demo_echo": {
                "module": "tools",
                "function": "echo",
                "boundary": "external_read",
                "description": "Echo input.",
            }
        }

    def test_record_prefixes_are_preserved_raw(self, tmp_path):
        from okg.substrate.deployments import DeploymentManifest

        manifest_path = _write_manifest(
            tmp_path / "demo",
            extra={
                "record_prefixes": [
                    {
                        "prefix": "fit:",
                        "target_subtype": "hep_fit",
                        "owner": "deployment",
                    },
                ],
            },
        )

        m = DeploymentManifest.from_yaml(manifest_path)

        assert m.record_prefixes == (
            {
                "prefix": "fit:",
                "target_subtype": "hep_fit",
                "owner": "deployment",
            },
        )

    def test_session_start_context_is_preserved_raw(self, tmp_path):
        from okg.substrate.deployments import DeploymentManifest

        manifest_path = _write_manifest(
            tmp_path / "demo",
            extra={
                "session_start_context": {
                    "slices": {
                        "recent_records": {
                            "sql": "SELECT node_id FROM okg.v_nodes LIMIT 5",
                            "limit": 5,
                        },
                    },
                },
            },
        )

        m = DeploymentManifest.from_yaml(manifest_path)

        assert m.session_start_context == {
            "slices": {
                "recent_records": {
                    "sql": "SELECT node_id FROM okg.v_nodes LIMIT 5",
                    "limit": 5,
                },
            },
        }

    def test_agent_tools_list_is_refused_at_load(
        self, tmp_path,
    ):
        """`agent_tools: []` is malformed (tools are a mapping by name).

        It used to load as the raw list so the MCP server could refuse it
        later; the key table (#2571) now refuses it at load, in both loader
        modes, instead of letting `or {}` hide it.
        """
        from okg.substrate.deployments import (
            DeploymentKeyTypeError,
            DeploymentManifest,
        )

        dep = tmp_path / "demo"
        dep.mkdir()
        (dep / "schemas").mkdir()
        (dep / "src.yaml").touch()
        manifest_path = dep / "deployment.yaml"
        manifest_path.write_text(yaml.safe_dump({
            "name": "demo",
            "description": "test",
            "schema_dir": "schemas",
            "source_registry": "src.yaml",
            "agent_tools": [],
        }))

        with pytest.raises(DeploymentKeyTypeError, match="agent_tools must be a mapping"):
            DeploymentManifest.from_yaml(manifest_path)
        with pytest.raises(DeploymentKeyTypeError, match="agent_tools must be a mapping"):
            DeploymentManifest.from_yaml_lenient(manifest_path)


class TestManifestCacheEnvAwareness:
    """`DeploymentManifest.from_yaml`'s `(path, mtime)`-keyed cache is
    env-AWARE: it must not serve a manifest whose env-interpolated
    `postgres.dsn` was resolved under a different environment. Guards the
    DSG lab deployment's order-dependent flake (2026-07-07) where a sibling test
    resolved the manifest with `DSG_OKG_DSN` unset (caching
    `postgres_dsn=None`), and a later resolve with it set was served the
    stale None."""

    def test_reresolves_dsn_when_referenced_env_var_changes(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import DeploymentManifest

        monkeypatch.delenv("DEMO_OKG_DSN", raising=False)
        manifest_path = _write_manifest(
            tmp_path / "demo",
            extra={"postgres": {"dsn": "${DEMO_OKG_DSN}"}},
        )

        # Var UNSET: the DSN is unresolvable, so postgres_dsn is None —
        # and this None gets cached under (path, mtime).
        first = DeploymentManifest.from_yaml(manifest_path)
        assert first.postgres_dsn is None

        # SAME file (identical path + mtime), now with the var SET. The
        # cache must rebuild rather than serve the stale None.
        dsn = "postgres://postgres:okg@localhost:5433/okg_demo"
        monkeypatch.setenv("DEMO_OKG_DSN", dsn)
        second = DeploymentManifest.from_yaml(manifest_path)
        assert second.postgres_dsn == dsn

        # Invalidation works both directions: back to unset -> None again.
        monkeypatch.delenv("DEMO_OKG_DSN", raising=False)
        third = DeploymentManifest.from_yaml(manifest_path)
        assert third.postgres_dsn is None

    def test_cache_hits_when_env_unchanged(self, tmp_path, monkeypatch):
        """The common case stays a hit: a second resolve under an
        unchanged environment returns the identical cached object, so the
        env-awareness check does not defeat the cache."""
        from okg.substrate.deployments import DeploymentManifest

        dsn = "postgres://postgres:okg@localhost:5433/okg_demo"
        monkeypatch.setenv("DEMO_OKG_DSN", dsn)
        manifest_path = _write_manifest(
            tmp_path / "demo",
            extra={"postgres": {"dsn": "${DEMO_OKG_DSN}"}},
        )

        first = DeploymentManifest.from_yaml(manifest_path)
        second = DeploymentManifest.from_yaml(manifest_path)
        assert second is first
        assert first.postgres_dsn == dsn

    def test_literal_dsn_manifest_unaffected_by_env_change(
        self, tmp_path, monkeypatch,
    ):
        """A manifest with no `${VAR}` reference has an empty env
        signature, so it stays a pure (path, mtime) hit — an unrelated
        env-var change never forces a rebuild."""
        from okg.substrate.deployments import DeploymentManifest

        manifest_path = _write_manifest(
            tmp_path / "demo",
            extra={"postgres": {"dsn": "postgres://literal/dsn"}},
        )

        first = DeploymentManifest.from_yaml(manifest_path)
        monkeypatch.setenv("SOMETHING_UNRELATED", "x")
        second = DeploymentManifest.from_yaml(manifest_path)
        assert second is first
        assert first.postgres_dsn == "postgres://literal/dsn"


# ---------------------------------------------------------------------------
# A deployment given by folder stays reachable by its name (issue #2951)
# ---------------------------------------------------------------------------

def _outside_deployment(tmp_path: Path, monkeypatch) -> Path:
    """A deployment in a folder no deployments root contains.

    The quickstart put the deployment here: any folder, named differently
    from the deployment.
    """
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)
    folder = tmp_path / "quickstart" / "deployment"
    _write_manifest(folder, name="outside_dep")
    (folder / "source_registry.yaml").write_text(
        "sources:\n"
        "  test-source:\n"
        "    module: okg.substrate.library.sources.git_graph_files\n"
        "    class: GitFilesSource\n"
        "    ownership_id: outside_dep.test_source\n"
        "    params:\n"
        "      repo: not-a-real-repo\n"
        "      repo_root: /tmp/no-such-repo-for-binding-test\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [repo, path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n",
        encoding="utf-8",
    )
    return folder


class TestFolderResolvedDeploymentBinding:

    def test_binding_reaches_the_folder_only_inside_its_scope(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            bind_resolved_deployment,
            deployment_binding_scope,
            find_deployment_dir,
            resolve_deployment,
        )

        folder = _outside_deployment(tmp_path, monkeypatch)
        assert find_deployment_dir("outside_dep") is None
        with deployment_binding_scope():
            assert bind_resolved_deployment("outside_dep", folder) is True
            assert find_deployment_dir("outside_dep") == folder.resolve()
            assert resolve_deployment("outside_dep").deployment_dir == folder.resolve()
            # An explicit root still means that root.
            other = tmp_path / "other-root"
            other.mkdir()
            assert find_deployment_dir(
                "outside_dep", deployment_dir_override=str(other),
            ) is None
        # The binding ends with the command: nothing is remembered.
        assert find_deployment_dir("outside_dep") is None
        with pytest.raises(DeploymentResolutionError):
            resolve_deployment("outside_dep")

    def test_binding_skips_a_folder_without_a_manifest(self, tmp_path, monkeypatch):
        from okg.substrate.deployments import (
            bind_resolved_deployment,
            deployment_binding_scope,
            find_deployment_dir,
        )

        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
        monkeypatch.chdir(tmp_path)
        with deployment_binding_scope():
            assert bind_resolved_deployment("x_no_manifest", tmp_path) is False
            assert find_deployment_dir("x_no_manifest") is None

    def test_ingest_by_folder_looks_the_name_up_in_the_same_folder(
        self, tmp_path, monkeypatch, capsys,
    ):
        """Issue #2951: `okg ingest --deployment <folder>` failed halfway.

        Ingest resolved the folder, switched to the manifest's name, and
        then looked that name up again (runtime routing first), which
        searched only the deployments roots and reported the deployment
        "not found". Every later lookup must reach the folder given.
        """
        import okg.substrate.cli.main as cli
        import okg.substrate.ingest.service as service
        from okg.substrate.deployments import find_deployment_dir

        folder = _outside_deployment(tmp_path, monkeypatch)
        seen: dict = {}

        class _Outcome:
            report = {"ok": True}
            any_failed = False

        def fake_run_ingest(options):
            seen["deployment"] = options.deployment
            seen["deployment_dir"] = options.deployment_dir
            seen["by_name"] = find_deployment_dir(options.deployment)
            return _Outcome()

        monkeypatch.setattr(service, "run_ingest", fake_run_ingest)
        rc = cli.main([
            "--json", "ingest", "--deployment", str(folder),
            "--dsn", "postgresql://nobody@127.0.0.1:1/unused",
            "--no-publish",
        ])

        out = capsys.readouterr()
        assert rc == 0, out.out + out.err
        assert seen == {
            "deployment": "outside_dep",
            "deployment_dir": folder.resolve(),
            "by_name": folder.resolve(),
        }
        assert find_deployment_dir("outside_dep") is None

    def test_run_by_folder_binds_the_name_to_that_folder(
        self, tmp_path, monkeypatch,
    ):
        """`okg run --once --deployment <folder>` resolves through the same binding.

        `_resolve_run_context` resolves the folder and switches to the
        manifest name; the rest of the publish cycle looks that name up.
        """
        import argparse

        import okg.substrate.cli.main as cli
        from okg.substrate.deployments import (
            deployment_binding_scope,
            find_deployment_dir,
        )

        folder = _outside_deployment(tmp_path, monkeypatch)
        args = argparse.Namespace(
            deployment=str(folder),
            dsn="postgresql://nobody@127.0.0.1:1/unused",
            once=True,
        )
        with deployment_binding_scope():
            _dsn, name, dep_dir, _runtime = cli._resolve_run_context(args)
            by_name = find_deployment_dir(name)
        assert name == "outside_dep"
        assert dep_dir == folder.resolve()
        assert by_name == folder.resolve()
        assert find_deployment_dir("outside_dep") is None

    def test_canonical_reference_turns_a_folder_into_the_manifest_name(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import (
            canonical_deployment_reference,
            deployment_binding_scope,
            find_deployment_dir,
        )

        folder = _outside_deployment(tmp_path, monkeypatch)
        with deployment_binding_scope():
            assert canonical_deployment_reference(str(folder)) == (
                "outside_dep", folder.resolve(),
            )
            assert canonical_deployment_reference(
                str(folder / "deployment.yaml"),
            ) == ("outside_dep", folder.resolve())
            assert find_deployment_dir("outside_dep") == folder.resolve()
        # A plain name is left for the caller's ordinary lookup.
        assert canonical_deployment_reference("outside_dep") == (
            "outside_dep", None,
        )
        assert find_deployment_dir("outside_dep") is None

    def test_canonical_reference_refuses_a_path_with_no_manifest(
        self, tmp_path, monkeypatch,
    ):
        """A path that names no manifest is refused, never passed on as a name."""
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            canonical_deployment_reference,
        )

        monkeypatch.chdir(tmp_path)
        with pytest.raises(DeploymentResolutionError, match="not found"):
            canonical_deployment_reference(str(tmp_path / "no-such-deployment"))

    def test_provision_by_folder_uses_the_manifest_name_for_every_step(
        self, tmp_path, monkeypatch, capsys,
    ):
        """`okg provision --deployment <folder>` refused its publish with
        `catalog_writer_scope_conflict`: provision passed the raw folder path
        to migrate, catalog ownership, the catalog load, Nomos seeding, and
        ingest, so source progress and admission rows were written under the
        path while the publish's catalog identity carried the manifest name.
        Every step must see the manifest name, and the name must reach the
        folder given even though no deployments root contains it."""
        import okg.substrate.cli.provision as provision
        import okg.substrate.db.migrate as db_migrate
        import okg.substrate.ingest.service as service
        import okg.substrate.runtime.source_sync_priority as priority
        from contextlib import nullcontext

        import okg.substrate.cli.main as cli
        from okg.substrate.deployments import find_deployment_dir

        folder = _outside_deployment(tmp_path, monkeypatch)
        seen: dict = {}

        class _Stop(Exception):
            pass

        class _Migrated:
            statements_executed = 0

        def fake_migrate(dsn, *, deployment_name=None, **_kw):
            seen["migrate"] = deployment_name
            return _Migrated()

        def fake_claim(deployment, *, deployment_dir, dsn):
            seen["ownership"] = (deployment, Path(deployment_dir))
            return None

        def fake_catalog(deployment, *, dsn):
            seen["catalog"] = deployment
            seen["catalog_by_name"] = find_deployment_dir(deployment)
            return {"catalog_version_id": 1, "subtype_count": 0}

        def fake_baseline(deployment, deployment_dir, *, dsn):
            seen["nomos"] = deployment
            return None

        def fake_guard(*, dsn, deployment, deployment_dir, **_kw):
            seen["guard"] = (deployment, Path(deployment_dir))
            return nullcontext()

        def fake_run_ingest(options):
            seen["ingest"] = (options.deployment, Path(options.deployment_dir))
            raise _Stop()

        monkeypatch.setattr(
            db_migrate, "ensure_required_extensions",
            lambda dsn: {"created": [], "already_present": []},
        )
        monkeypatch.setattr(db_migrate, "migrate", fake_migrate)
        monkeypatch.setattr(
            provision, "_claim_rebound_staging_catalog_ownership", fake_claim,
        )
        monkeypatch.setattr(provision, "_apply_catalog", fake_catalog)
        monkeypatch.setattr(provision, "_seed_nomos_baseline", fake_baseline)
        monkeypatch.setattr(priority, "ingest_run_guard", fake_guard)
        monkeypatch.setattr(service, "run_ingest", fake_run_ingest)

        with pytest.raises(_Stop):
            cli.main([
                "--json", "provision", "--deployment", str(folder),
                "--dsn", "postgresql://nobody@127.0.0.1:1/unused",
            ])

        resolved = folder.resolve()
        assert seen == {
            "migrate": "outside_dep",
            "ownership": ("outside_dep", resolved),
            "catalog": "outside_dep",
            "catalog_by_name": resolved,
            "nomos": "outside_dep",
            "guard": ("outside_dep", resolved),
            "ingest": ("outside_dep", resolved),
        }
        # The binding ends with the command.
        assert find_deployment_dir("outside_dep") is None

    def test_provision_refuses_a_path_with_no_manifest(
        self, tmp_path, monkeypatch, capsys,
    ):
        import json

        import okg.substrate.cli.main as cli

        monkeypatch.chdir(tmp_path)
        missing = tmp_path / "no-such-deployment"
        rc = cli.main([
            "--json", "provision", "--deployment", str(missing),
            "--dsn", "postgresql://nobody@127.0.0.1:1/unused",
        ])
        captured = capsys.readouterr()
        assert rc == 1
        text = (captured.out or captured.err).strip().splitlines()[-1]
        error = json.loads(text)["error"]
        assert error["message"] == (
            f"Deployment manifest not found at path {str(missing)!r}"
        ), error
        assert error["data"] == {"deployment": str(missing)}

    def test_an_explicit_root_beats_the_binding_in_resolve_deployment(
        self, tmp_path, monkeypatch,
    ):
        from okg.substrate.deployments import (
            bind_resolved_deployment,
            deployment_binding_scope,
            resolve_deployment,
        )

        folder = _outside_deployment(tmp_path, monkeypatch)
        root = tmp_path / "explicit-root"
        _write_manifest(root / "outside_dep", name="outside_dep")
        with deployment_binding_scope():
            bind_resolved_deployment("outside_dep", folder)
            assert resolve_deployment("outside_dep").deployment_dir == folder.resolve()
            explicit = resolve_deployment(
                "outside_dep", deployment_dir_override=str(root),
            )
        assert explicit.deployment_dir == (root / "outside_dep").resolve()

    def test_binding_outside_a_scope_binds_nothing(self, tmp_path, monkeypatch):
        """Without a command scope there is nothing to end the binding with."""
        from okg.substrate.deployments import (
            bind_resolved_deployment,
            find_deployment_dir,
        )

        folder = _outside_deployment(tmp_path, monkeypatch)
        assert bind_resolved_deployment("outside_dep", folder) is False
        assert find_deployment_dir("outside_dep") is None

    def test_two_commands_on_two_threads_keep_their_own_bindings(
        self, tmp_path, monkeypatch,
    ):
        """Concurrent commands in one process must not see or drop each other's
        binding: A binds the name to one folder, B binds it to another while A
        is still running, and A finishing must leave B's binding in place."""
        import threading

        from okg.substrate.deployments import (
            bind_resolved_deployment,
            deployment_binding_scope,
            find_deployment_dir,
        )

        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
        monkeypatch.chdir(tmp_path)
        one = tmp_path / "one"
        two = tmp_path / "two"
        _write_manifest(one, name="shared_dep")
        _write_manifest(two, name="shared_dep")
        a_bound, b_bound, a_done = (
            threading.Event(), threading.Event(), threading.Event(),
        )
        seen: dict = {}

        def command_a():
            with deployment_binding_scope():
                bind_resolved_deployment("shared_dep", one)
                a_bound.set()
                assert b_bound.wait(10)
                seen["a"] = find_deployment_dir("shared_dep")
            a_done.set()

        def command_b():
            assert a_bound.wait(10)
            with deployment_binding_scope():
                bind_resolved_deployment("shared_dep", two)
                b_bound.set()
                assert a_done.wait(10)
                seen["b_after_a"] = find_deployment_dir("shared_dep")

        threads = [
            threading.Thread(target=command_a),
            threading.Thread(target=command_b),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(20)
        assert seen == {"a": one.resolve(), "b_after_a": two.resolve()}
        assert find_deployment_dir("shared_dep") is None


class TestManifestNotNamedDeploymentYaml:
    """A `*.yaml` manifest not named deployment.yaml cannot be tied to its
    folder; provision must refuse it rather than use its name to reach some
    other deployment."""

    @staticmethod
    def _custom_manifest(tmp_path, monkeypatch) -> Path:
        folder = _outside_deployment(tmp_path, monkeypatch)
        custom = folder / "custom.yaml"
        (folder / "deployment.yaml").rename(custom)
        return custom

    def test_provision_reference_refuses_it(self, tmp_path, monkeypatch):
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            canonical_deployment_reference,
            deployment_binding_scope,
        )

        custom = self._custom_manifest(tmp_path, monkeypatch)
        with deployment_binding_scope():
            with pytest.raises(DeploymentResolutionError, match="deployment.yaml"):
                canonical_deployment_reference(str(custom))
