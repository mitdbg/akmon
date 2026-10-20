from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import threading
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Mapping

import pytest

import okg.substrate.environment_services as environment_services
from okg.substrate.deployments.environment import (
    DeploymentEnvironment,
    load_environment,
)
from okg.substrate.deployments import DeploymentConfigError
from okg.substrate.errors import SealedReleaseInvalid
from okg.substrate.release_activation import (
    active_release_pointer_digest,
    activate_environment_release,
    environment_release_status,
    launcher_descriptor_path,
    load_active_release,
)
from okg.substrate.sealed_release import (
    SealedRelease,
    SealedReleaseInputs,
    dependency_lock_digest,
    prepare_sealed_release,
    prepare_environment_release,
    release_inputs_from_tree,
    tree_digest,
    verify_sealed_release,
)
from okg.substrate._sealed_release_integrity import executable_tree_digest


DependencySync = Callable[[Path, Path, Mapping[str, str]], None]


@pytest.fixture(autouse=True)
def _restore_tmp_permissions(tmp_path: Path):
    yield
    for path in [tmp_path, *tmp_path.rglob("*")]:
        if path.is_symlink():
            continue
        try:
            path.chmod(stat.S_IMODE(path.stat().st_mode) | 0o700)
        except OSError:
            # Best-effort fixture cleanup; the test failure remains primary.
            pass


def _sha(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode()).hexdigest()}"


def _git(repo: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _fixture_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "source"
    db = repo / "src" / "okg" / "substrate" / "db"
    (db / "migrations").mkdir(parents=True)
    (repo / "skills").mkdir()
    files = {
        ".python-version": "3.12\n",
        "pyproject.toml": (
            '[project]\nname = "sealed-fixture"\nversion = "0.1.0"\n'
            'requires-python = ">=3.12"\n\n[tool.uv]\npackage = false\n'
        ),
        "uv.lock": (
            'version = 1\nrevision = 2\nrequires-python = ">=3.12"\n\n'
            '[[package]]\nname = "sealed-fixture"\nversion = "0.1.0"\n'
            'source = { virtual = "." }\n'
        ),
        "src/okg/substrate/db/schema.sql": "CREATE SCHEMA okg;\n",
        "src/okg/substrate/db/views.sql": "SELECT 1;\n",
        "src/okg/substrate/db/migrations/0001_fixture.sql": "SELECT 1;\n",
        "src/okg/substrate/library/ontologies/fixture/schema.yaml": (
            "id: https://example.invalid/fixture\n"
            "name: fixture\n"
            "classes:\n"
            "  FixtureNode:\n"
            "    annotations:\n"
            "      archetype: Entity\n"
        ),
        "skills/fixture.md": "# fixture\n",
        "app.txt": "committed\n",
        "ops/definitions/shared-definition/definition.yaml": (
            "api_version: okg.deployment-definition/v1\n"
            "id: shared-fixture\n"
            "description: Sealed release fixture\n"
            "schema_dir: schemas\n"
            "source_registry: source_registry.yaml\n"
            "modules:\n  - fixture\n"
            "nomos: {}\n"
            "viz:\n  enabled: true\n"
        ),
        "ops/definitions/shared-definition/source_registry.yaml": "sources: []\n",
        "ops/definitions/shared-definition/schemas/base.yaml": (
            "subtypes: []\nedge_types: []\nnarrowings: []\n"
        ),
        "ops/environments/fixture-production/environment.yaml": (
            "api_version: okg.environment/v1\n"
            "id: fixture-production\n"
            "tier: production\n"
            "graph_deployment_name: fixture-graph\n"
            "definition:\n"
            "  path: ../../definitions/shared-definition/definition.yaml\n"
            "database:\n"
            "  dsn_secret_ref: env:TEST_PRODUCTION_DSN\n"
            "  name: fixture_production\n"
            "  container: fixture-production-postgres\n"
            "  volume: fixture-production-postgres-data\n"
            "  host_port: 55434\n"
            "  dbos_identity: fixture-production:dbos\n"
            "release:\n"
            "  mode: sealed\n"
            "  artifact_store: ${TEST_STATE_ROOT}/artifacts\n"
            "  active_pointer: ${TEST_STATE_ROOT}/fixture-production/active.json\n"
            "  previous_pointer: ${TEST_STATE_ROOT}/fixture-production/previous.json\n"
            "runtime:\n"
            "  worker_service: org.okg.runtime-worker.fixture-production\n"
            "  state_root: ${TEST_STATE_ROOT}/fixture-production\n"
            "  local_env_file: ${TEST_STATE_ROOT}/fixture-production/local.env\n"
            "  log_root: ${TEST_STATE_ROOT}/fixture-production/logs\n"
            "  config:\n    enabled: true\n"
            "ui:\n"
            "  service: org.okg.ui.fixture-production\n"
            "  host: 127.0.0.1\n"
            "  port: 18770\n"
            "webhook:\n"
            "  service: org.okg.webhook.fixture-production\n"
            "  host: 127.0.0.1\n"
            "  port: 8088\n"
            "mcp:\n"
            "  transport: stdio\n"
            "  registration_id: fixture-production\n"
            "  launcher_environment: fixture-production\n"
            "readiness:\n"
            "  policy: dogfood\n"
            "  evidence_ref:\n"
            "    store: runtime_state\n"
            "    path: readiness/current.json\n"
            "    max_age_seconds: 900\n"
        ),
        "ops/environments/fixture-production/postgres-container-owner.yaml": (
            "api_version: okg.postgres-container-owner/v1\nuid: 1000\ngid: 1000\n"
        ),
        "ops/environments/fixture-production/postgres-capacity.yaml": (
            "api_version: okg.postgres-capacity/v1\n"
            "max_worker_processes: 32\n"
            "max_parallel_workers: 8\n"
            "timescaledb_max_background_workers: 8\n"
            "timescaledb_job_worker_reserve: 4\n"
        ),
    }
    for relative, content in files.items():
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test User")
    return repo, _commit(repo, "initial")


def _resolver(candidate: Path, commit: str) -> SealedReleaseInputs:
    return release_inputs_from_tree(
        candidate,
        code_revision=commit,
        catalog_fingerprint=_sha("catalog"),
        definition_digest=_sha("definition"),
        skills_path=candidate / "skills",
    )


def test_sealed_input_identity_binds_distinct_core_and_deployment_repositories(
    tmp_path: Path,
) -> None:
    core_repo, core_revision = _fixture_repo(tmp_path / "core")
    deployment_repo = tmp_path / "deployment"
    (deployment_repo / "ops/deployments/sample").mkdir(parents=True)
    (deployment_repo / "ops/deployments/sample/deployment.yaml").write_text(
        "name: sample\n", encoding="utf-8"
    )
    (deployment_repo / "unrelated-root.txt").write_text(
        "included in the full tree\n", encoding="utf-8"
    )
    _git(deployment_repo, "init", "-q")
    _git(deployment_repo, "config", "user.email", "test@example.com")
    _git(deployment_repo, "config", "user.name", "Test User")
    deployment_revision = _commit(deployment_repo, "deployment")
    deployment_tree_sha = _git(deployment_repo, "rev-parse", "HEAD^{tree}")

    def distinct_inputs(candidate: Path, commit: str) -> SealedReleaseInputs:
        return release_inputs_from_tree(
            candidate,
            code_revision=core_revision,
            catalog_fingerprint=_sha("catalog"),
            definition_digest=_sha("definition"),
            deployment_revision=deployment_revision,
            deployment_tree_sha=deployment_tree_sha,
            deployment_layout="split",
            deployment_repo_path=".okg-deployment-repository",
        )

    release = prepare_sealed_release(
        source_repo=core_repo,
        revision=core_revision,
        artifact_store=tmp_path / "artifacts",
        input_resolver=distinct_inputs,
        dependency_sync=_fake_sync("distinct", []),
        deployment_source_repo=deployment_repo,
        deployment_revision=deployment_revision,
    )

    assert release.inputs.code_revision == core_revision
    assert release.inputs.deployment_revision == deployment_revision
    assert release.inputs.deployment_tree_sha == _git(
        deployment_repo, "rev-parse", "HEAD^{tree}"
    )
    assert release.inputs.code_revision != release.inputs.deployment_revision
    (deployment_repo / "unrelated-root.txt").write_text(
        "changed full-tree input\n", encoding="utf-8"
    )
    changed_deployment_revision = _commit(deployment_repo, "change unrelated root")
    changed_tree = _git(deployment_repo, "rev-parse", "HEAD^{tree}")

    def changed_inputs(candidate: Path, _commit: str) -> SealedReleaseInputs:
        return release_inputs_from_tree(
            candidate,
            code_revision=core_revision,
            catalog_fingerprint=_sha("catalog"),
            definition_digest=_sha("definition"),
            deployment_revision=changed_deployment_revision,
            deployment_tree_sha=changed_tree,
            deployment_layout="split",
            deployment_repo_path=".okg-deployment-repository",
        )

    changed_release = prepare_sealed_release(
        source_repo=core_repo,
        revision=core_revision,
        artifact_store=tmp_path / "artifacts",
        input_resolver=changed_inputs,
        dependency_sync=_fake_sync("changed", []),
        deployment_source_repo=deployment_repo,
        deployment_revision=changed_deployment_revision,
    )
    assert changed_release.inputs.input_id != release.inputs.input_id
    assert changed_release.inputs.deployment_tree_sha != release.inputs.deployment_tree_sha
    core_repo.rename(tmp_path / "core-moved")
    deployment_repo.rename(tmp_path / "deployment-moved")
    verified = verify_sealed_release(release.artifact_root)
    assert verified.inputs == release.inputs


def test_prepare_environment_release_uses_core_bundle_and_nested_deployment(
    tmp_path: Path,
) -> None:
    fixture_repo, _ = _fixture_repo(tmp_path / "fixture")
    deployment_repo = tmp_path / "deployment-only"
    shutil.copytree(fixture_repo / "ops", deployment_repo / "ops")
    (deployment_repo / "unrelated-root.txt").write_text(
        "full deployment tree input\n", encoding="utf-8"
    )
    core_repo = Path(__file__).resolve().parents[2]
    core_revision = _git(core_repo, "rev-parse", "HEAD")
    definition = deployment_repo / "ops/definitions/shared-definition/definition.yaml"
    definition.write_text(
        definition.read_text(encoding="utf-8").replace(
            "modules:\n  - fixture\n",
            "modules:\n  - person\n",
        ),
        encoding="utf-8",
    )
    descriptor = deployment_repo / "ops/environments/fixture-production/environment.yaml"
    descriptor.write_text(
        descriptor.read_text(encoding="utf-8").replace(
            "release:\n  mode: sealed\n",
            "release:\n"
            "  mode: sealed\n"
            "  pin:\n"
            f"    code_repository: {core_repo}\n"
            f"    code_revision: {core_revision}\n",
        ),
        encoding="utf-8",
    )
    _git(deployment_repo, "init", "-q")
    _git(deployment_repo, "config", "user.email", "test@example.com")
    _git(deployment_repo, "config", "user.name", "Test User")
    deployment_revision = _commit(deployment_repo, "deployment only")
    values = _environment_values(tmp_path / "state")
    environment = load_environment(
        descriptor,
        environ=values,
    )
    sync_calls: list[Path] = []
    base_sync = _fake_sync("split-production", sync_calls)

    def assert_split_sync(
        repo_root: Path, venv_root: Path, environ: Mapping[str, str]
    ) -> None:
        assert (repo_root / "pyproject.toml").is_file()
        nested = repo_root / ".okg-deployment-repository"
        assert (nested / "ops/environments/fixture-production/environment.yaml").is_file()
        assert not (nested / "src").exists()
        assert not (nested / "pyproject.toml").exists()
        base_sync(repo_root, venv_root, environ)

    release = prepare_environment_release(
        environment=environment,
        source_repo=deployment_repo,
        revision=deployment_revision,
        dependency_sync=assert_split_sync,
        environ=values,
    )

    assert release.inputs.code_revision == core_revision
    assert release.inputs.deployment_revision == deployment_revision
    assert release.inputs.deployment_tree_sha == _git(
        deployment_repo, "rev-parse", "HEAD^{tree}"
    )
    assert release.inputs.deployment_layout == "split"
    assert release.inputs.deployment_repo_path == ".okg-deployment-repository"
    assert release.deployment_repo_root == (
        release.repo_root / ".okg-deployment-repository"
    )
    activate_environment_release(
        environment=environment,
        release=release,
        source_repo=deployment_repo,
        activated_by="split-repository-test",
        environ=values,
    )
    active = load_active_release(environment.release.active_pointer)
    assert active is not None
    assert active["descriptor_relative"].startswith(
        ".okg-deployment-repository/"
    )


def test_split_release_refuses_reserved_core_namespace_collision(
    tmp_path: Path,
) -> None:
    core_repo, _ = _fixture_repo(tmp_path / "core")
    reserved = core_repo / ".okg-deployment-repository"
    reserved.mkdir()
    (reserved / "collision.txt").write_text("occupied\n", encoding="utf-8")
    core_revision = _commit(core_repo, "reserved collision")
    deployment_repo, deployment_revision = _fixture_repo(tmp_path / "deployment")
    calls: list[Path] = []

    with pytest.raises(SealedReleaseInvalid, match="reserved deployment snapshot"):
        prepare_sealed_release(
            source_repo=core_repo,
            revision=core_revision,
            artifact_store=tmp_path / "artifacts",
            input_resolver=_resolver,
            dependency_sync=_fake_sync("must-not-run", calls),
            deployment_source_repo=deployment_repo,
            deployment_revision=deployment_revision,
        )

    assert calls == []


def _fake_sync(payload: str, calls: list[Path]) -> DependencySync:
    def sync(
        repo_root: Path,
        venv_root: Path,
        environ: Mapping[str, str],
    ) -> None:
        calls.append(repo_root)
        assert not (repo_root / ".git").exists()
        assert environ["PYTHONDONTWRITEBYTECODE"] == "1"
        assert environ["UV_LINK_MODE"] == "copy"
        bin_root = venv_root / "bin"
        bin_root.mkdir(parents=True)
        interpreter = bin_root / "python3"
        interpreter.write_text(f"runtime:{payload}\n", encoding="utf-8")
        interpreter.chmod(0o755)
        (venv_root / "installed.txt").write_text(payload, encoding="utf-8")
        package = venv_root / "lib/python3.12/site-packages/okg/__init__.py"
        package.parent.mkdir(parents=True)
        package.write_text("__version__ = 'fixture'\n", encoding="utf-8")

    return sync


def _started_by_launcher(
    monkeypatch: pytest.MonkeyPatch,
    interpreter: Path,
    state_root: Path,
) -> str:
    """Make this process look as the launcher starts a sealed worker (#2861)."""
    prefix = str(state_root / "service-pycache" / "launch-test")
    real = sys.flags
    flags = {
        name: getattr(real, name)
        for name in dir(real)
        if not name.startswith("_") and not callable(getattr(real, name))
    }
    monkeypatch.setattr(
        sys,
        "orig_argv",
        [str(interpreter), "-I", "-B", "-X", f"pycache_prefix={prefix}",
         "-m", "okg.substrate.runtime.worker"],
    )
    monkeypatch.setattr(sys, "pycache_prefix", prefix)
    monkeypatch.setattr(
        sys, "flags", SimpleNamespace(**{**flags, "dont_write_bytecode": 1})
    )
    return prefix


def _prepare(
    repo: Path,
    commit: str,
    store: Path,
    sync: DependencySync | None,
    *,
    expected_inputs: SealedReleaseInputs | None = None,
) -> SealedRelease:
    return prepare_sealed_release(
        source_repo=repo,
        revision=commit,
        artifact_store=store,
        input_resolver=_resolver,
        dependency_sync=sync,
        expected_inputs=expected_inputs,
        environ=dict(os.environ),
    )


def _environment_values(state_root: Path) -> dict[str, str]:
    return {
        **dict(os.environ),
        "TEST_STATE_ROOT": str(state_root),
        "TEST_PRODUCTION_DSN": (
            "postgresql://postgres:test@127.0.0.1:55434/fixture_production"
        ),
    }


def _prepare_environment_fixture(
    repo: Path,
    commit: str,
    state_root: Path,
    *,
    payload: str = "runtime",
) -> tuple[DeploymentEnvironment, SealedRelease, dict[str, str]]:
    values = _environment_values(state_root)
    environment = load_environment(
        repo / "ops/environments/fixture-production/environment.yaml",
        environ=values,
    )
    release = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=commit,
        dependency_sync=_fake_sync(payload, []),
        environ=values,
    )
    return environment, release, values


def test_artifact_builds_exact_detached_revision_without_mutating_checkout(
    tmp_path: Path,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    (repo / "app.txt").write_text("dirty checkout\n", encoding="utf-8")
    calls: list[Path] = []

    release = _prepare(
        repo,
        commit,
        tmp_path / "artifacts",
        _fake_sync("one", calls),
    )

    assert len(calls) == 1
    assert release.inputs.code_revision == commit
    assert release.repo_root.joinpath("app.txt").read_text() == "committed\n"
    assert (repo / "app.txt").read_text() == "dirty checkout\n"
    assert _git(repo, "rev-parse", "HEAD") == commit
    assert _git(repo, "status", "--porcelain") == "M app.txt"
    assert release.artifact_root.name == release.artifact_id.removeprefix("sha256:")
    assert not (release.repo_root / ".git").exists()
    assert release.interpreter.is_file()
    assert stat.S_IMODE(release.artifact_root.stat().st_mode) & 0o222 == 0
    metadata = (release.artifact_root / "release.json").read_text()
    assert str(repo) not in metadata
    assert verify_sealed_release(release.artifact_root) == release


def test_artifact_identity_includes_full_tree_and_is_cross_store_deterministic(
    tmp_path: Path,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    first = _prepare(repo, commit, tmp_path / "one", _fake_sync("same", []))
    second = _prepare(repo, commit, tmp_path / "two", _fake_sync("same", []))
    changed = _prepare(repo, commit, tmp_path / "three", _fake_sync("changed", []))

    assert first.inputs == second.inputs == changed.inputs
    assert first.executable_tree_digest == second.executable_tree_digest
    assert first.artifact_id == second.artifact_id
    assert changed.executable_tree_digest != first.executable_tree_digest
    assert changed.artifact_id != first.artifact_id


def test_artifact_reuse_never_runs_install_again_and_recovers_missing_index(
    tmp_path: Path,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    store = tmp_path / "artifacts"
    calls: list[Path] = []
    first = _prepare(repo, commit, store, _fake_sync("one", calls))

    def forbidden_sync(*_args) -> None:
        raise AssertionError("dependency installation reran")

    second = _prepare(repo, commit, store, forbidden_sync)
    index = store / ".inputs" / f"{first.inputs.input_id[7:]}.json"
    index.unlink()
    third = _prepare(repo, commit, store, forbidden_sync)

    assert len(calls) == 1
    assert second.reused is True
    assert third.reused is True
    assert second.artifact_id == third.artifact_id == first.artifact_id
    assert index.is_file()


@pytest.mark.parametrize(
    ("relative", "field"),
    [
        ("uv.lock", "dependency_lock_digest"),
        (
            "src/okg/substrate/db/migrations/0001_fixture.sql",
            "schema_checksum",
        ),
        ("skills/fixture.md", "skill_bundle_digest"),
    ],
)
def test_artifact_declared_input_changes_are_reflected_in_identity(
    tmp_path: Path,
    relative: str,
    field: str,
) -> None:
    repo, first_commit = _fixture_repo(tmp_path)
    store = tmp_path / "artifacts"
    first = _prepare(repo, first_commit, store, _fake_sync("same", []))
    with (repo / relative).open("a", encoding="utf-8") as handle:
        handle.write("# changed\n")
    second_commit = _commit(repo, f"change {field}")

    second = _prepare(repo, second_commit, store, _fake_sync("same", []))

    assert getattr(first.inputs, field) != getattr(second.inputs, field)
    assert first.artifact_id != second.artifact_id


def test_artifact_expected_inputs_refuse_drift_before_install(tmp_path: Path) -> None:
    repo, first_commit = _fixture_repo(tmp_path)
    approved = _prepare(
        repo,
        first_commit,
        tmp_path / "approved",
        _fake_sync("one", []),
    )
    with (repo / "uv.lock").open("a", encoding="utf-8") as handle:
        handle.write("# drift\n")
    changed_commit = _commit(repo, "lock drift")
    calls: list[Path] = []

    with pytest.raises(SealedReleaseInvalid, match="approved identity") as caught:
        _prepare(
            repo,
            changed_commit,
            tmp_path / "candidate",
            _fake_sync("two", calls),
            expected_inputs=approved.inputs,
        )

    assert calls == []
    assert caught.value.data["expected"] == approved.inputs.to_dict()


def test_artifact_tampering_fails_full_tree_verification(tmp_path: Path) -> None:
    repo, commit = _fixture_repo(tmp_path)
    release = _prepare(
        repo,
        commit,
        tmp_path / "artifacts",
        _fake_sync("one", []),
    )
    target = release.repo_root / "app.txt"
    target.chmod(stat.S_IMODE(target.stat().st_mode) | stat.S_IWUSR)
    target.write_text("tampered\n", encoding="utf-8")

    with pytest.raises(SealedReleaseInvalid, match="executable tree mismatch"):
        verify_sealed_release(release.artifact_root)


def test_artifact_corrupt_input_index_fails_without_reinstall(tmp_path: Path) -> None:
    repo, commit = _fixture_repo(tmp_path)
    store = tmp_path / "artifacts"
    release = _prepare(repo, commit, store, _fake_sync("one", []))
    index = store / ".inputs" / f"{release.inputs.input_id[7:]}.json"
    raw = json.loads(index.read_text(encoding="utf-8"))
    raw["artifact_id"] = f"sha256:{'0' * 64}"
    index.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(SealedReleaseInvalid, match="metadata is missing"):
        _prepare(
            repo,
            commit,
            store,
            lambda *_args: pytest.fail("dependency installation reran"),
        )


def test_artifact_install_failure_removes_partial_bundle_and_worktree(
    tmp_path: Path,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    store = tmp_path / "artifacts"

    def fail_sync(*_args) -> None:
        raise RuntimeError("install failed")

    with pytest.raises(SealedReleaseInvalid, match="preparation failed"):
        _prepare(repo, commit, store, fail_sync)

    leftovers = [
        path.name
        for path in store.iterdir()
        if path.name.startswith((".preparing-", ".sealed-worktree-"))
    ]
    assert leftovers == []
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


def test_artifact_wrong_resolver_revision_is_refused_before_install(
    tmp_path: Path,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    calls: list[Path] = []

    def wrong_revision(candidate: Path, _commit: str) -> SealedReleaseInputs:
        resolved = _resolver(candidate, commit)
        return SealedReleaseInputs(
            code_revision="d" * 40,
            dependency_lock_digest=resolved.dependency_lock_digest,
            schema_checksum=resolved.schema_checksum,
            catalog_fingerprint=resolved.catalog_fingerprint,
            definition_digest=resolved.definition_digest,
            skill_bundle_digest=resolved.skill_bundle_digest,
        )

    with pytest.raises(SealedReleaseInvalid, match="wrong revision"):
        prepare_sealed_release(
            source_repo=repo,
            revision=commit,
            artifact_store=tmp_path / "artifacts",
            input_resolver=wrong_revision,
            dependency_sync=_fake_sync("one", calls),
        )

    assert calls == []


@pytest.mark.parametrize(
    "field,malformed",
    [
        ("code_revision", "z" * 40),
        ("deployment_revision", "A" * 40),
        ("deployment_revision", "1" * 41),
        ("deployment_tree_sha", "1" * 63),
        ("deployment_tree_sha", " " + "1" * 39),
    ],
)
def test_sealed_release_refuses_malformed_git_object_ids_before_install(
    tmp_path: Path,
    field: str,
    malformed: str,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    calls: list[Path] = []

    def malformed_inputs(candidate: Path, resolved_commit: str) -> SealedReleaseInputs:
        return replace(_resolver(candidate, resolved_commit), **{field: malformed})

    with pytest.raises(SealedReleaseInvalid, match="lowercase full Git object id"):
        prepare_sealed_release(
            source_repo=repo,
            revision=commit,
            artifact_store=tmp_path / "artifacts",
            input_resolver=malformed_inputs,
            dependency_sync=_fake_sync("must-not-run", calls),
        )

    assert calls == []

def test_artifact_tree_digest_covers_modes_symlinks_and_node_modules(
    tmp_path: Path,
) -> None:
    root = tmp_path / "tree"
    (root / "node_modules").mkdir(parents=True)
    target = root / "target"
    target.write_text("one\n", encoding="utf-8")
    dependency = root / "node_modules" / "runtime.js"
    dependency.write_text("one\n", encoding="utf-8")
    link = root / "current"
    link.symlink_to("target")
    initial = tree_digest(root)

    dependency.write_text("two\n", encoding="utf-8")
    dependency_changed = tree_digest(root)
    dependency.chmod(0o755)
    mode_changed = tree_digest(root)
    link.unlink()
    link.symlink_to("node_modules/runtime.js")
    link_changed = tree_digest(root)

    assert len({initial, dependency_changed, mode_changed, link_changed}) == 4


def test_executable_digest_ignores_only_deployment_live_profile_output(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "artifact"
    (artifact / "repo/unrelated/_live").mkdir(parents=True)
    (artifact / "repo/unrelated/_live/state.json").write_text(
        "one\n",
        encoding="utf-8",
    )
    (artifact / "venv").mkdir()
    initial = executable_tree_digest(artifact)

    (artifact / "repo/profiles/okg-ci-dev/_live").mkdir(parents=True)
    (artifact / "repo/profiles/okg-ci-dev/_live/publish.json").write_text(
        "runtime output\n",
        encoding="utf-8",
    )
    assert executable_tree_digest(artifact) == initial

    (artifact / "repo/profiles/okg-ci-dev/profile.yaml").write_text(
        "name: okg-ci-dev\n",
        encoding="utf-8",
    )
    assert executable_tree_digest(artifact) != initial

    changed_profile = executable_tree_digest(artifact)
    (artifact / "repo/profiles/okg-ci-dev/profile.yaml").write_text(
        "name: changed\n",
        encoding="utf-8",
    )
    assert executable_tree_digest(artifact) != changed_profile

    changed_profile = executable_tree_digest(artifact)
    (artifact / "repo/unrelated/_live/state.json").write_text(
        "two\n",
        encoding="utf-8",
    )
    assert executable_tree_digest(artifact) != changed_profile


def test_artifact_dependency_digest_uses_declared_root_locks_only(
    tmp_path: Path,
) -> None:
    repo, _commit_id = _fixture_repo(tmp_path)
    initial = dependency_lock_digest(repo)
    archived = repo / "archive" / "retired"
    archived.mkdir(parents=True)
    (archived / "package-lock.json").write_text("archived\n", encoding="utf-8")

    assert dependency_lock_digest(repo) == initial

    (repo / "package-lock.json").write_text("active\n", encoding="utf-8")
    assert dependency_lock_digest(repo) != initial


def test_artifact_escaping_symlink_is_refused(tmp_path: Path) -> None:
    repo, _commit_id = _fixture_repo(tmp_path)
    (repo / "escape").symlink_to("../../outside")
    commit = _commit(repo, "escaping symlink")

    with pytest.raises(SealedReleaseInvalid, match="escaping symlink"):
        _prepare(
            repo,
            commit,
            tmp_path / "artifacts",
            _fake_sync("one", []),
        )


def test_artifact_default_frozen_runtime_refuses_missing_producer_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    for inherited_authority in (
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
        "PYTHONHOME",
        "PYTHONPATH",
        "OKG_SEALED_RUNTIME_PROOF_ROOT",
        "OKG_SEALED_RUNTIME_PROOF_DIGEST",
    ):
        monkeypatch.delenv(inherited_authority, raising=False)

    with pytest.raises(SealedReleaseInvalid) as refusal:
        _prepare(repo, commit, tmp_path / "artifacts", None)

    assert refusal.value.data == {"code": "sealed_release_runtime_authority_missing"}


def test_launcher_activation_rotates_previous_and_reuses_same_release(
    tmp_path: Path,
) -> None:
    repo, first_commit = _fixture_repo(tmp_path)
    state_root = tmp_path / "state"
    environment, first, values = _prepare_environment_fixture(
        repo,
        first_commit,
        state_root,
        payload="first",
    )

    initial = activate_environment_release(
        environment=environment,
        release=first,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    assert initial.changed is True
    assert not environment.release.previous_pointer.exists()
    active = load_active_release(environment.release.active_pointer)
    assert active["artifact_id"] == first.artifact_id
    assert first.inputs.postgres_capacity_digest == (
        environment.postgres_capacity_digest
    )
    assert active["postgres_capacity"] == {
        "schema": "okg.postgres-capacity/v1",
        "digest": environment.postgres_capacity_digest,
        "max_worker_processes": 32,
        "max_parallel_workers": 8,
        "timescaledb_max_background_workers": 8,
        "timescaledb_job_worker_reserve": 4,
    }

    (repo / "app.txt").write_text("second\n", encoding="utf-8")
    second_commit = _commit(repo, "second release")
    second = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("second", []),
        environ=values,
    )
    changed = activate_environment_release(
        environment=environment,
        release=second,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    previous = load_active_release(environment.release.previous_pointer)
    assert changed.changed is True
    assert previous is not None
    assert previous["artifact_id"] == first.artifact_id

    unchanged = activate_environment_release(
        environment=environment,
        release=second,
        source_repo=repo,
        activated_by="another-test",
        environ=values,
    )
    assert unchanged.changed is False
    assert load_active_release(environment.release.previous_pointer) == previous
    status = environment_release_status(environment, environ=values)
    assert status["ok"] is True
    assert status["artifact_id"] == second.artifact_id
    assert status["previous_artifact_id"] == first.artifact_id
    first_pointer_definition = previous["definition_digest"]
    assert status["previous_definition_digest"] == first_pointer_definition
    assert status["previous_pointer_digest"] == active_release_pointer_digest(previous)
    assert status["ancestry_status"] == "bound"
    active = load_active_release(environment.release.active_pointer)
    assert active is not None
    assert active["predecessor"] == {
        "schema": "okg.release_predecessor/v1",
        "artifact_id": first.artifact_id,
        "definition_digest": first_pointer_definition,
        "pointer_digest": active_release_pointer_digest(previous),
    }


def test_activation_refuses_third_rotation_until_full_current_runtime_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.environment_services as environment_services

    repo, first_commit = _fixture_repo(tmp_path)
    environment, first, values = _prepare_environment_fixture(
        repo,
        first_commit,
        tmp_path / "state",
        payload="first",
    )
    activate_environment_release(
        environment=environment,
        release=first,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    (repo / "app.txt").write_text("second\n", encoding="utf-8")
    second_commit = _commit(repo, "second release")
    second = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("second", []),
        environ=values,
    )
    activate_environment_release(
        environment=environment,
        release=second,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    active_before = load_active_release(environment.release.active_pointer)
    previous_before = load_active_release(environment.release.previous_pointer)
    assert active_before is not None and previous_before is not None

    marker = environment.runtime.state_root / "supervisor/install.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("{}\n", encoding="utf-8")
    marker.chmod(0o600)
    observed: list[str] = []

    def healthy_process_without_service_handoff(_environment, *, release_status):
        observed.append(str(release_status["artifact_id"]))
        return {"ok": True, "status": "healthy"}

    monkeypatch.setattr(
        environment_services,
        "environment_process_authority_status",
        healthy_process_without_service_handoff,
    )
    (repo / "app.txt").write_text("third\n", encoding="utf-8")
    third_commit = _commit(repo, "third release")
    third = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=third_commit,
        dependency_sync=_fake_sync("third", []),
        environ=values,
    )

    with pytest.raises(SealedReleaseInvalid) as refusal:
        activate_environment_release(
            environment=environment,
            release=third,
            source_repo=repo,
            activated_by="test",
            environ=values,
        )

    assert refusal.value.data["code"] == "release_handoff_incomplete"
    assert observed == [second.artifact_id]
    assert load_active_release(environment.release.active_pointer) == active_before
    assert load_active_release(environment.release.previous_pointer) == previous_before

    rollback = activate_environment_release(
        environment=environment,
        release=first,
        source_repo=repo,
        activated_by="rollback-test",
        environ=values,
    )
    assert rollback.changed is True
    assert load_active_release(environment.release.active_pointer)[
        "artifact_id"
    ] == first.artifact_id


def test_activation_refuses_third_rotation_when_receipts_are_absent_but_manager_is_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, first_commit = _fixture_repo(tmp_path)
    environment, first, values = _prepare_environment_fixture(
        repo,
        first_commit,
        tmp_path / "state",
        payload="first",
    )
    activate_environment_release(
        environment=environment,
        release=first,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    (repo / "app.txt").write_text("second\n", encoding="utf-8")
    second_commit = _commit(repo, "second release")
    second = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("second", []),
        environ=values,
    )
    activate_environment_release(
        environment=environment,
        release=second,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    active_before = environment.release.active_pointer.read_bytes()
    previous_before = environment.release.previous_pointer.read_bytes()
    assert not (environment.runtime.state_root / "supervisor").exists()
    assert not (
        environment.runtime.state_root / "worker-authority.json"
    ).exists()
    assert not (environment.runtime.state_root / "service-authority").exists()

    observed_manager_targets: list[tuple[str, str, str | None]] = []

    def manager_status(_environment, *, service_id, plan=None):
        assert plan is not None
        adapter = str(plan["adapter"])
        scope = plan.get("systemd_scope")
        observed_manager_targets.append((service_id, adapter, scope))
        running = (
            service_id == environment.runtime.worker_service
            and adapter == "systemd"
            and scope == "system"
        )
        return {
            "ok": True,
            "status": "running" if running else "stopped",
            "pid": 424242 if running else None,
        }

    monkeypatch.setattr(environment_services.sys, "platform", "linux")
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        manager_status,
    )
    (repo / "app.txt").write_text("third\n", encoding="utf-8")
    third_commit = _commit(repo, "third release")
    third = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=third_commit,
        dependency_sync=_fake_sync("third", []),
        environ=values,
    )

    with pytest.raises(SealedReleaseInvalid) as refusal:
        activate_environment_release(
            environment=environment,
            release=third,
            source_repo=repo,
            activated_by="test",
            environ=values,
        )

    assert refusal.value.data["code"] == "release_handoff_incomplete"
    assert refusal.value.data["manager_census_status"] == "running"
    assert any(
        service_id == environment.runtime.worker_service
        and adapter == "systemd"
        and scope == "system"
        for service_id, adapter, scope in observed_manager_targets
    )
    assert environment.release.active_pointer.read_bytes() == active_before
    assert environment.release.previous_pointer.read_bytes() == previous_before


def test_activation_refuses_non_root_rotation_when_previous_pointer_is_missing(
    tmp_path: Path,
) -> None:
    import okg.substrate.release_activation as activation

    repo, first_commit = _fixture_repo(tmp_path)
    environment, first, values = _prepare_environment_fixture(
        repo,
        first_commit,
        tmp_path / "state",
        payload="first",
    )
    activate_environment_release(
        environment=environment,
        release=first,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    (repo / "app.txt").write_text("second\n", encoding="utf-8")
    second_commit = _commit(repo, "second release")
    second = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("second", []),
        environ=values,
    )
    activate_environment_release(
        environment=environment,
        release=second,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    active_before = environment.release.active_pointer.read_bytes()
    environment.release.previous_pointer.unlink()
    status = environment_release_status(environment, environ=values)
    assert status["ok"] is False
    assert status["status"] == "invalid"
    with pytest.raises(SealedReleaseInvalid, match="predecessor pointer is missing"):
        activate_environment_release(
            environment=environment,
            release=second,
            source_repo=repo,
            activated_by="same-release-test",
            environ=values,
        )
    assert environment.release.active_pointer.read_bytes() == active_before
    assert not environment.release.previous_pointer.exists()

    (repo / "app.txt").write_text("third\n", encoding="utf-8")
    third_commit = _commit(repo, "third release")
    third = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=third_commit,
        dependency_sync=_fake_sync("third", []),
        environ=values,
    )
    with pytest.raises(SealedReleaseInvalid, match="predecessor pointer is missing"):
        activate_environment_release(
            environment=environment,
            release=third,
            source_repo=repo,
            activated_by="test",
            environ=values,
        )

    assert environment.release.active_pointer.read_bytes() == active_before
    assert not environment.release.previous_pointer.exists()
    assert not activation._release_activation_transaction_path(environment).exists()


def test_split_release_activation_status_and_rotation_preserve_staging_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A staging-sealed release remains valid through production reads and rotation."""
    repo, first_commit = _fixture_repo(tmp_path)
    definitions = repo / "ops" / "definitions"
    production_definition = definitions / "shared-definition"
    staging_definition = definitions / "staging-definition"
    shutil.copytree(production_definition, staging_definition)
    staging_definition_file = staging_definition / "definition.yaml"
    staging_definition_file.write_text(
        staging_definition_file.read_text(encoding="utf-8")
        .replace(
            "id: shared-fixture\n",
            "id: shared-fixture-staging\n",
        )
        .replace(
            "nomos: {}\n",
            "nomos:\n"
            "  deployment_posture:\n"
            "    export_policy: no_export\n"
            "    live_call_default: deny\n"
            "    allowed_exportability:\n"
            "      - no_export\n",
        ),
        encoding="utf-8",
    )
    staging_descriptor = repo / "ops" / "environments" / "fixture-staging"
    staging_descriptor.mkdir(parents=True)
    production_descriptor = (
        repo / "ops" / "environments" / "fixture-production" / "environment.yaml"
    )
    descriptor_text = production_descriptor.read_text(encoding="utf-8")
    descriptor_text = (
        descriptor_text.replace("fixture-production", "fixture-staging")
        .replace("fixture_production", "fixture_staging")
        .replace("tier: production", "tier: staging")
        .replace(
            "graph_deployment_name: fixture-graph",
            "graph_deployment_name: fixture-graph-staging",
        )
        .replace("TEST_PRODUCTION_DSN", "TEST_STAGING_DSN")
        .replace("55434", "55435")
        .replace(
            "../../definitions/shared-definition/",
            "../../definitions/staging-definition/",
        )
    )
    (staging_descriptor / "environment.yaml").write_text(
        descriptor_text, encoding="utf-8"
    )
    for sidecar in ("postgres-container-owner.yaml", "postgres-capacity.yaml"):
        shutil.copy2(
            repo / "ops" / "environments" / "fixture-production" / sidecar,
            staging_descriptor / sidecar,
        )
    first_commit = _commit(repo, "add split staging descriptor")

    state_root = tmp_path / "state"
    values = _environment_values(state_root)
    values["TEST_STAGING_DSN"] = (
        "postgresql://postgres:test@127.0.0.1:55435/fixture_staging"
    )
    production = load_environment(
        repo / "ops/environments/fixture-production/environment.yaml",
        environ=values,
    )
    staging = load_environment(
        staging_descriptor / "environment.yaml",
        environ=values,
    )
    first = prepare_environment_release(
        environment=staging,
        source_repo=repo,
        revision=first_commit,
        dependency_sync=_fake_sync("first", []),
        environ=values,
    )
    activate_environment_release(
        environment=production,
        release=first,
        source_repo=repo,
        activated_by="promotion",
        environ=values,
        staging_environment=staging,
        approved_definition_digest=staging.definition_digest,
    )
    active = load_active_release(production.release.active_pointer)
    assert active is not None
    assert active["definition_digest"] == production.definition_digest
    assert active["release_definition_digest"] == staging.definition_digest
    status = environment_release_status(production, environ=values)
    assert status["ok"] is True
    assert status["artifact_id"] == first.artifact_id
    from okg.substrate.runtime.release_authority import (
        verify_worker_release_authority,
    )
    from okg.substrate.runtime.release_launcher import (
        build_exec_environment,
        verify_launch,
    )

    launched = build_exec_environment(
        verify_launch(launcher_descriptor_path(production)),
        environ=values,
    )
    _started_by_launcher(
        monkeypatch, first.interpreter, production.runtime.state_root
    )
    authority = verify_worker_release_authority(
        deployment=production.graph_deployment_name,
        dsn=values["TEST_PRODUCTION_DSN"],
        environ=launched,
        executable=first.interpreter,
        package_file=(first.venv_root / "lib/python3.12/site-packages/okg/__init__.py"),
        isolated=True,
    )
    assert authority is not None
    assert authority["artifact_id"] == first.artifact_id

    (repo / "app.txt").write_text("second\n", encoding="utf-8")
    second_commit = _commit(repo, "second staged release")
    second = prepare_environment_release(
        environment=staging,
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("second", []),
        environ=values,
    )
    activate_environment_release(
        environment=production,
        release=second,
        source_repo=repo,
        activated_by="promotion",
        environ=values,
        staging_environment=staging,
        approved_definition_digest=staging.definition_digest,
    )
    status = environment_release_status(production, environ=values)
    assert status["ok"] is True
    assert status["artifact_id"] == second.artifact_id
    assert status["previous_artifact_id"] == first.artifact_id


def test_release_status_rejects_postgres_capacity_pointer_drift(
    tmp_path: Path,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    environment, release, values = _prepare_environment_fixture(
        repo,
        commit,
        tmp_path / "state",
    )
    activate_environment_release(
        environment=environment,
        release=release,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    active = load_active_release(environment.release.active_pointer)
    assert active is not None
    active["postgres_capacity"]["max_worker_processes"] = 999
    environment.release.active_pointer.write_text(
        json.dumps(active),
        encoding="utf-8",
    )

    status = environment_release_status(environment, environ=values)

    assert status["ok"] is False
    assert status["status"] == "invalid"
    assert "does not match the environment" in status["message"]


def test_launcher_activation_crosses_owner_and_capacity_declaration_transition(
    tmp_path: Path,
) -> None:
    """A new controller may rotate a deployed pre-sidecar v1 artifact.

    The owner and capacity declarations are sealed sidecars that the installed
    v1 parser ignores but includes in the immutable artifact. Pre-sidecar
    descriptors remain invalid for current use and are admitted only as
    immutable history.
    """
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import catalog_identity_for
    from okg.substrate.release_activation import _active_payload

    repo, _initial = _fixture_repo(tmp_path)
    descriptor_relative = Path("ops/environments/fixture-production/environment.yaml")
    descriptor = repo / descriptor_relative
    state_root = tmp_path / "state"
    values = _environment_values(state_root)

    owner_path = descriptor.parent / "postgres-container-owner.yaml"
    owner_text = owner_path.read_text(encoding="utf-8")
    capacity_path = descriptor.parent / "postgres-capacity.yaml"
    capacity_text = capacity_path.read_text(encoding="utf-8")
    owner_path.unlink()
    capacity_path.unlink()
    legacy_commit = _commit(repo, "legacy descriptor without sidecars")

    with pytest.raises(
        DeploymentConfigError,
        match="requires sealed Postgres container owner declaration",
    ):
        load_environment(descriptor, environ=values)
    legacy_environment = load_environment(
        descriptor,
        environ=values,
        _allow_legacy_container_owner=True,
    )
    assert legacy_environment.database.container_uid is None
    assert legacy_environment.database.container_gid is None
    assert legacy_environment.postgres_capacity_digest is None

    def legacy_inputs(candidate_repo: Path, commit: str) -> SealedReleaseInputs:
        candidate = load_environment(
            candidate_repo / descriptor_relative,
            environ=values,
            _allow_legacy_container_owner=True,
        )
        composed = compose_catalog(
            candidate.manifest,
            modules_root=(candidate_repo / "src/okg/substrate/library/ontologies"),
        )
        catalog = catalog_identity_for(
            composed=composed,
            deployment_dir=candidate.manifest.deployment_dir,
            command="test legacy sealed transition",
        )
        return release_inputs_from_tree(
            candidate_repo,
            code_revision=commit,
            catalog_fingerprint=catalog.catalog_fingerprint,
            definition_digest=candidate.definition_digest,
            skills_path=candidate.manifest.skills_dir,
        )

    legacy_release = prepare_sealed_release(
        source_repo=repo,
        revision=legacy_commit,
        artifact_store=legacy_environment.release.artifact_store,
        input_resolver=legacy_inputs,
        dependency_sync=_fake_sync("legacy", []),
        environ=values,
    )
    active_payload = _active_payload(
        environment=legacy_environment,
        release=legacy_release,
        descriptor_relative=descriptor_relative,
        descriptor_path=legacy_release.repo_root / descriptor_relative,
        activated_by="legacy-controller",
    )
    active_path = legacy_environment.release.active_pointer
    active_path.parent.mkdir(parents=True, exist_ok=True)
    active_path.write_text(json.dumps(active_payload), encoding="utf-8")
    active_path.chmod(0o600)
    assert "postgres_capacity" not in active_payload

    owner_path.write_text(owner_text, encoding="utf-8")
    capacity_path.write_text(capacity_text, encoding="utf-8")
    current_commit = _commit(repo, "declare postgres owner and capacity")
    current_environment = load_environment(descriptor, environ=values)
    current_release = prepare_environment_release(
        environment=current_environment,
        source_repo=repo,
        revision=current_commit,
        dependency_sync=_fake_sync("current", []),
        environ=values,
    )

    # The new controller can still report the exact ownerless active artifact
    # while preparing its replacement. This bridge validates immutable release
    # authority only; service rendering from an ownerless environment remains
    # forbidden.
    status_before = environment_release_status(current_environment, environ=values)
    assert status_before["ok"] is True
    assert status_before["artifact_id"] == legacy_release.artifact_id

    result = activate_environment_release(
        environment=current_environment,
        release=current_release,
        source_repo=repo,
        activated_by="new-controller",
        environ=values,
    )

    assert result.changed is True
    previous = load_active_release(current_environment.release.previous_pointer)
    assert previous is not None
    assert previous["artifact_id"] == legacy_release.artifact_id
    assert "postgres_capacity" not in previous
    status = environment_release_status(current_environment, environ=values)
    assert status["ok"] is True
    assert status["previous_artifact_id"] == legacy_release.artifact_id


def test_environment_release_rejects_dirty_postgres_capacity_sidecar(
    tmp_path: Path,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    values = _environment_values(tmp_path / "state")
    capacity_path = repo / "ops/environments/fixture-production/postgres-capacity.yaml"
    capacity_path.write_text(
        capacity_path.read_text(encoding="utf-8").replace(
            "max_worker_processes: 32",
            "max_worker_processes: 40",
        ),
        encoding="utf-8",
    )
    dirty_environment = load_environment(
        repo / "ops/environments/fixture-production/environment.yaml",
        environ=values,
    )

    with pytest.raises(
        SealedReleaseInvalid,
        match="postgres_capacity_digest",
    ):
        prepare_environment_release(
            environment=dirty_environment,
            source_repo=repo,
            revision=commit,
            dependency_sync=_fake_sync("dirty-capacity", []),
            environ=values,
        )


def test_launcher_activation_rotates_across_definition_update(
    tmp_path: Path,
) -> None:
    repo, first_commit = _fixture_repo(tmp_path)
    state_root = tmp_path / "state"
    environment, first, values = _prepare_environment_fixture(
        repo,
        first_commit,
        state_root,
        payload="first",
    )
    activate_environment_release(
        environment=environment,
        release=first,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )

    registry = repo / "ops/definitions/shared-definition/source_registry.yaml"
    registry.write_text("sources: []\n# policy metadata changed\n", encoding="utf-8")
    second_commit = _commit(repo, "update definition asset")
    updated_environment = load_environment(
        repo / "ops/environments/fixture-production/environment.yaml",
        environ=values,
    )
    second = prepare_environment_release(
        environment=updated_environment,
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("second", []),
        environ=values,
    )

    result = activate_environment_release(
        environment=updated_environment,
        release=second,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )

    assert result.changed is True
    status = environment_release_status(updated_environment, environ=values)
    assert status["ok"] is True
    assert status["artifact_id"] == second.artifact_id
    assert status["previous_artifact_id"] == first.artifact_id
    assert status["definition_digest"] == updated_environment.definition_digest
    assert status["previous_definition_digest"] == environment.definition_digest
    assert status["previous_definition_digest"] != status["definition_digest"]
    previous_authority = environment_services._previous_release_authority_status(
        updated_environment,
        current_release=status,
    )
    assert previous_authority["artifact_id"] == first.artifact_id
    assert previous_authority["definition_digest"] == environment.definition_digest
    assert previous_authority["definition_digest"] != (
        updated_environment.definition_digest
    )


def test_launcher_activation_readers_never_observe_partial_pointer(
    tmp_path: Path,
) -> None:
    repo, first_commit = _fixture_repo(tmp_path)
    environment, first, values = _prepare_environment_fixture(
        repo,
        first_commit,
        tmp_path / "state",
        payload="first",
    )
    activate_environment_release(
        environment=environment,
        release=first,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    (repo / "app.txt").write_text("second\n", encoding="utf-8")
    second_commit = _commit(repo, "second release")
    second = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("second", []),
        environ=values,
    )
    stopped = threading.Event()
    observed: list[str] = []
    failures: list[Exception] = []

    def read_active() -> None:
        while not stopped.is_set():
            try:
                active = load_active_release(environment.release.active_pointer)
                assert active is not None
                observed.append(active["artifact_id"])
            except Exception as exc:  # noqa: BLE001 - captured for assertion.
                failures.append(exc)
                stopped.set()

    reader = threading.Thread(target=read_active)
    reader.start()
    try:
        for release in (second, first, second, first, second):
            activate_environment_release(
                environment=environment,
                release=release,
                source_repo=repo,
                activated_by="test",
                environ=values,
            )
    finally:
        stopped.set()
        reader.join(timeout=5)

    assert not reader.is_alive()
    assert not failures
    assert observed
    assert set(observed) <= {first.artifact_id, second.artifact_id}


def test_launcher_refuses_corrupt_current_and_previous_authority(
    tmp_path: Path,
) -> None:
    repo, first_commit = _fixture_repo(tmp_path)
    environment, first, values = _prepare_environment_fixture(
        repo,
        first_commit,
        tmp_path / "state",
        payload="first",
    )
    activate_environment_release(
        environment=environment,
        release=first,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    first_pointer = load_active_release(environment.release.active_pointer)
    assert first_pointer is not None
    (repo / "app.txt").write_text("second\n", encoding="utf-8")
    second_commit = _commit(repo, "second release")
    second = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("second", []),
        environ=values,
    )

    corrupt = {**first_pointer, "artifact_root": str(tmp_path / "foreign")}
    environment.release.active_pointer.write_text(
        json.dumps(corrupt),
        encoding="utf-8",
    )
    with pytest.raises(SealedReleaseInvalid, match="artifact store"):
        activate_environment_release(
            environment=environment,
            release=second,
            source_repo=repo,
            activated_by="test",
            environ=values,
        )
    assert not environment.release.previous_pointer.exists()

    environment.release.active_pointer.write_text(
        json.dumps(first_pointer),
        encoding="utf-8",
    )
    activate_environment_release(
        environment=environment,
        release=second,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    previous = load_active_release(environment.release.previous_pointer)
    assert previous is not None
    previous["artifact_root"] = str(tmp_path / "foreign")
    environment.release.previous_pointer.write_text(
        json.dumps(previous),
        encoding="utf-8",
    )
    status = environment_release_status(environment, environ=values)
    assert status["ok"] is False
    assert status["status"] == "invalid"
    assert "artifact store" in status["message"]

    active = load_active_release(environment.release.active_pointer)
    assert active is not None
    environment.release.previous_pointer.write_text(
        json.dumps(active),
        encoding="utf-8",
    )
    status = environment_release_status(environment, environ=values)
    assert status["ok"] is False
    assert status["status"] == "invalid"
    assert "predecessor binding" in status["message"]


def test_launcher_activation_recovers_after_active_replace_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.release_activation as activation

    repo, first_commit = _fixture_repo(tmp_path)
    environment, first, values = _prepare_environment_fixture(
        repo,
        first_commit,
        tmp_path / "state",
        payload="first",
    )
    activate_environment_release(
        environment=environment,
        release=first,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    (repo / "app.txt").write_text("second\n", encoding="utf-8")
    second_commit = _commit(repo, "second release")
    second = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("second", []),
        environ=values,
    )
    original_write = activation._atomic_write_json
    failed = False

    def fail_active_once(path: Path, payload: Mapping[str, object]) -> None:
        nonlocal failed
        if path == environment.release.active_pointer and not failed:
            failed = True
            raise OSError("simulated active-pointer replace failure")
        original_write(path, payload)

    monkeypatch.setattr(activation, "_atomic_write_json", fail_active_once)
    with pytest.raises(OSError, match="simulated"):
        activate_environment_release(
            environment=environment,
            release=second,
            source_repo=repo,
            activated_by="test",
            environ=values,
        )
    assert (
        load_active_release(environment.release.active_pointer)["artifact_id"]
        == first.artifact_id
    )
    assert (
        load_active_release(environment.release.previous_pointer)["artifact_id"]
        == first.artifact_id
    )

    monkeypatch.setattr(activation, "_atomic_write_json", original_write)
    activate_environment_release(
        environment=environment,
        release=second,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    assert (
        load_active_release(environment.release.active_pointer)["artifact_id"]
        == second.artifact_id
    )
    assert (
        load_active_release(environment.release.previous_pointer)["artifact_id"]
        == first.artifact_id
    )
    assert not activation._release_activation_transaction_path(environment).exists()


def test_launcher_activation_recovers_non_root_rotation_and_refuses_contradiction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.release_activation as activation

    repo, first_commit = _fixture_repo(tmp_path)
    environment, first, values = _prepare_environment_fixture(
        repo,
        first_commit,
        tmp_path / "state",
        payload="first",
    )
    activate_environment_release(
        environment=environment,
        release=first,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    (repo / "app.txt").write_text("second\n", encoding="utf-8")
    second_commit = _commit(repo, "second release")
    second = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("second", []),
        environ=values,
    )
    activate_environment_release(
        environment=environment,
        release=second,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    first_pointer = load_active_release(environment.release.previous_pointer)
    second_pointer = load_active_release(environment.release.active_pointer)
    assert first_pointer is not None and second_pointer is not None

    manager_censuses: list[tuple[str, ...]] = []

    def stopped_manager_census(_environment, *, service_ids):
        manager_censuses.append(tuple(service_ids))
        return {"ok": True, "status": "stopped", "process_present": False}

    monkeypatch.setattr(
        environment_services,
        "environment_manager_process_census",
        stopped_manager_census,
    )
    (repo / "app.txt").write_text("third\n", encoding="utf-8")
    third_commit = _commit(repo, "third release")
    third = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=third_commit,
        dependency_sync=_fake_sync("third", []),
        environ=values,
    )
    original_write = activation._atomic_write_json
    failed = False

    def fail_third_active_once(path: Path, payload: Mapping[str, object]) -> None:
        nonlocal failed
        if (
            path == environment.release.active_pointer
            and payload.get("artifact_id") == third.artifact_id
            and not failed
        ):
            failed = True
            raise OSError("simulated non-root active-pointer replace failure")
        original_write(path, payload)

    monkeypatch.setattr(activation, "_atomic_write_json", fail_third_active_once)
    with pytest.raises(OSError, match="simulated non-root"):
        activate_environment_release(
            environment=environment,
            release=third,
            source_repo=repo,
            activated_by="test",
            environ=values,
        )

    transaction_path = activation._release_activation_transaction_path(environment)
    transaction = json.loads(transaction_path.read_text(encoding="utf-8"))
    assert stat.S_IMODE(transaction_path.stat().st_mode) == 0o600
    assert load_active_release(environment.release.active_pointer) == second_pointer
    assert load_active_release(environment.release.previous_pointer) == second_pointer
    assert environment_release_status(environment, environ=values)["status"] == "invalid"

    original_write(
        environment.release.previous_pointer,
        transaction["active_after"],
    )
    bytes_before_refusal = {
        path: path.read_bytes()
        for path in (
            environment.release.active_pointer,
            environment.release.previous_pointer,
            transaction_path,
        )
    }
    with pytest.raises(SealedReleaseInvalid) as contradiction:
        activate_environment_release(
            environment=environment,
            release=third,
            source_repo=repo,
            activated_by="test",
            environ=values,
        )
    assert contradiction.value.data["code"] == (
        "release_activation_transaction_invalid"
    )
    assert {
        path: path.read_bytes() for path in bytes_before_refusal
    } == bytes_before_refusal

    original_write(environment.release.previous_pointer, second_pointer)
    monkeypatch.setattr(activation, "_atomic_write_json", original_write)
    result = activate_environment_release(
        environment=environment,
        release=third,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    assert result.changed is True
    assert load_active_release(environment.release.active_pointer)[
        "artifact_id"
    ] == third.artifact_id
    assert load_active_release(environment.release.previous_pointer) == second_pointer
    assert not transaction_path.exists()
    assert manager_censuses


@pytest.mark.parametrize(
    ("interruption", "expected_retry_changed"),
    (("journal_only", True), ("pointers_replaced", False)),
)
def test_launcher_activation_recovers_other_exact_non_root_transaction_states(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interruption: str,
    expected_retry_changed: bool,
) -> None:
    import okg.substrate.release_activation as activation

    repo, first_commit = _fixture_repo(tmp_path)
    environment, first, values = _prepare_environment_fixture(
        repo,
        first_commit,
        tmp_path / "state",
        payload="first",
    )
    activate_environment_release(
        environment=environment,
        release=first,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    (repo / "app.txt").write_text("second\n", encoding="utf-8")
    second_commit = _commit(repo, "second release")
    second = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("second", []),
        environ=values,
    )
    activate_environment_release(
        environment=environment,
        release=second,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    first_pointer = load_active_release(environment.release.previous_pointer)
    second_pointer = load_active_release(environment.release.active_pointer)
    assert first_pointer is not None and second_pointer is not None

    monkeypatch.setattr(
        environment_services,
        "environment_manager_process_census",
        lambda _environment, *, service_ids: {
            "ok": True,
            "status": "stopped",
            "process_present": False,
            "service_ids": list(service_ids),
        },
    )
    (repo / "app.txt").write_text("third\n", encoding="utf-8")
    third_commit = _commit(repo, "third release")
    third = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=third_commit,
        dependency_sync=_fake_sync("third", []),
        environ=values,
    )
    transaction_path = activation._release_activation_transaction_path(environment)
    original_write = activation._atomic_write_json
    original_unlink = activation._durable_unlink
    interrupted = False

    def interrupt_previous_replace(
        path: Path,
        payload: Mapping[str, object],
    ) -> None:
        nonlocal interrupted
        if path == environment.release.previous_pointer and not interrupted:
            interrupted = True
            raise OSError("simulated previous-pointer replace failure")
        original_write(path, payload)

    def interrupt_transaction_cleanup(path: Path) -> None:
        nonlocal interrupted
        if path == transaction_path and not interrupted:
            interrupted = True
            raise OSError("simulated transaction cleanup failure")
        original_unlink(path)

    if interruption == "journal_only":
        monkeypatch.setattr(
            activation,
            "_atomic_write_json",
            interrupt_previous_replace,
        )
    else:
        monkeypatch.setattr(
            activation,
            "_durable_unlink",
            interrupt_transaction_cleanup,
        )
    with pytest.raises(OSError, match="simulated"):
        activate_environment_release(
            environment=environment,
            release=third,
            source_repo=repo,
            activated_by="test",
            environ=values,
        )
    assert transaction_path.exists()
    if interruption == "journal_only":
        assert load_active_release(environment.release.active_pointer) == second_pointer
        assert load_active_release(environment.release.previous_pointer) == first_pointer
    else:
        assert load_active_release(environment.release.active_pointer)[
            "artifact_id"
        ] == third.artifact_id
        assert load_active_release(environment.release.previous_pointer) == second_pointer

    monkeypatch.setattr(activation, "_atomic_write_json", original_write)
    monkeypatch.setattr(activation, "_durable_unlink", original_unlink)
    retried = activate_environment_release(
        environment=environment,
        release=third,
        source_repo=repo,
        activated_by="retry-test",
        environ=values,
    )
    assert retried.changed is expected_retry_changed
    assert load_active_release(environment.release.active_pointer)[
        "artifact_id"
    ] == third.artifact_id
    assert load_active_release(environment.release.previous_pointer) == second_pointer
    assert not transaction_path.exists()


def test_launcher_is_stdlib_only_and_verifies_in_isolated_python(
    tmp_path: Path,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    environment, release, values = _prepare_environment_fixture(
        repo,
        commit,
        tmp_path / "state",
    )
    activate_environment_release(
        environment=environment,
        release=release,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    launcher = Path(__file__).resolve().parents[2] / (
        "src/okg/substrate/runtime/release_launcher.py"
    )
    source = launcher.read_text(encoding="utf-8")
    assert "from okg" not in source
    assert "import okg" not in source

    proc = subprocess.run(
        [
            sys.executable,
            "-I",
            str(launcher),
            "--descriptor",
            str(launcher_descriptor_path(environment)),
            "--verify-only",
            "--json",
        ],
        env=values,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["ok"] is True
    assert payload["environment_id"] == environment.id
    assert payload["artifact_id"] == release.artifact_id
    assert Path(payload["interpreter"]).is_relative_to(release.artifact_root)


def test_launcher_accepts_runtime_output_only_in_deployment_live_profile(
    tmp_path: Path,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    environment, release, values = _prepare_environment_fixture(
        repo,
        commit,
        tmp_path / "state",
    )
    activate_environment_release(
        environment=environment,
        release=release,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    live_root = release.repo_root / "profiles/fixture/_live"
    repo_mode = stat.S_IMODE(release.repo_root.stat().st_mode)
    release.repo_root.chmod(repo_mode | stat.S_IWUSR)
    live_root.mkdir(parents=True)
    release.repo_root.chmod(repo_mode)
    (live_root / "publish.json").write_text("runtime output\n", encoding="utf-8")
    launcher = Path(__file__).resolve().parents[2] / (
        "src/okg/substrate/runtime/release_launcher.py"
    )

    proc = subprocess.run(
        [
            sys.executable,
            "-I",
            str(launcher),
            "--descriptor",
            str(launcher_descriptor_path(environment)),
            "--verify-only",
            "--json",
        ],
        env=values,
        check=False,
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["ok"] is True

    (live_root / "publish.json").unlink()
    live_root.rmdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    live_root.symlink_to(outside, target_is_directory=True)
    refused = subprocess.run(
        [
            sys.executable,
            "-I",
            str(launcher),
            "--descriptor",
            str(launcher_descriptor_path(environment)),
            "--verify-only",
            "--json",
        ],
        env=values,
        check=False,
        capture_output=True,
        text=True,
    )
    assert refused.returncode == 2
    assert "symlink escapes" in refused.stderr


def test_launcher_exec_environment_removes_checkout_fallbacks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime.release_launcher import (
        _execute_plan,
        build_exec_environment,
        verify_launch,
    )

    repo, commit = _fixture_repo(tmp_path)
    environment, release, values = _prepare_environment_fixture(
        repo,
        commit,
        tmp_path / "state",
    )
    activate_environment_release(
        environment=environment,
        release=release,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    plan = verify_launch(launcher_descriptor_path(environment))
    dirty = {
        **values,
        "OKG_DEPLOYMENTS_DIR": "/foreign/deployments",
        "OKG_ENVIRONMENTS_DIR": "/foreign/environments",
        "OKG_LOCAL_ENV": "/foreign/local.env",
        "OKG_SERVICE_ID": "org.okg.ui.fixture-production",
        "INVOCATION_ID": "0123456789abcdef0123456789abcdef",
        "OKG_SERVICE_LIFECYCLE_NONCE": "launchd-wrapper-nonce-0123456789",
        "PYTHONHOME": "/foreign/python",
        "PYTHONINSPECT": "1",
        "PYTHONPATH": "/foreign/package",
        "PYTHONUSERBASE": "/foreign/user-site",
        "VIRTUAL_ENV": "/foreign/venv",
    }

    launched = build_exec_environment(plan, environ=dirty)

    for removed in (
        "OKG_DEPLOYMENTS_DIR",
        "OKG_ENVIRONMENTS_DIR",
        "OKG_LOCAL_ENV",
        "PYTHONHOME",
        "PYTHONINSPECT",
        "PYTHONPATH",
        "PYTHONUSERBASE",
        "VIRTUAL_ENV",
    ):
        assert removed not in launched
    assert launched["OKG_RELEASE_ROOT"] == str(release.artifact_root)
    assert launched["OKG_ENVIRONMENT_ID"] == environment.id
    assert launched["OKG_DEPLOYMENT"] == environment.graph_deployment_name
    assert launched["OKG_DSN"] == values["TEST_PRODUCTION_DSN"]
    assert launched["OKG_PROFILE_DIR"] == str(
        environment.runtime.data_root / "profiles"
    )
    assert launched["PYTHONNOUSERSITE"] == "1"
    assert launched["INVOCATION_ID"] == dirty["INVOCATION_ID"]
    assert launched["OKG_SERVICE_LIFECYCLE_NONCE"] == dirty[
        "OKG_SERVICE_LIFECYCLE_NONCE"
    ]

    captured: dict[str, object] = {}

    class ExecIntercept(Exception):
        pass

    def intercept_execve(
        executable: str,
        arguments: list[str],
        exec_environ: dict[str, str],
    ) -> None:
        captured.update(
            executable=executable,
            arguments=arguments,
            environ=exec_environ,
        )
        raise ExecIntercept

    monkeypatch.setattr(os, "chdir", lambda _path: None)
    monkeypatch.setattr(os, "execve", intercept_execve)
    with pytest.raises(ExecIntercept):
        _execute_plan(
            plan,
            command=("-c", "print('sealed')"),
            environ=dirty,
        )
    receipt = (
        environment.runtime.state_root
        / "service-authority/org.okg.ui.fixture-production.json"
    )
    receipt_payload = json.loads(receipt.read_text(encoding="utf-8"))
    pycache_prefix = receipt_payload["pycache_prefix"]
    assert Path(pycache_prefix).parent == (
        environment.runtime.state_root / "service-pycache"
    )
    assert captured["arguments"] == [
        plan["interpreter"],
        "-I",
        "-B",
        "-X",
        f"pycache_prefix={pycache_prefix}",
        "-c",
        "print('sealed')",
    ]
    assert receipt_payload["arguments"] == ["-I", "-c", "print('sealed')"]
    assert receipt_payload["artifact_id"] == release.artifact_id
    assert receipt_payload["interpreter"] == plan["interpreter"]
    assert receipt_payload["isolated"] is True
    assert receipt_payload["systemd_invocation_id"] == dirty["INVOCATION_ID"]
    assert receipt_payload["lifecycle_nonce"] == dirty[
        "OKG_SERVICE_LIFECYCLE_NONCE"
    ]
    assert stat.S_IMODE(receipt.stat().st_mode) == 0o600


def test_launcher_sealed_process_resolves_only_active_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        deployment_loads_module,
        find_deployment_dir,
        resolve_deployment,
    )
    from okg.substrate.runtime.release_launcher import (
        build_exec_environment,
        verify_launch,
    )

    repo, commit = _fixture_repo(tmp_path)
    environment, release, values = _prepare_environment_fixture(
        repo,
        commit,
        tmp_path / "state",
    )
    activate_environment_release(
        environment=environment,
        release=release,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    launched = build_exec_environment(
        verify_launch(launcher_descriptor_path(environment)),
        environ=values,
    )
    for key, value in launched.items():
        monkeypatch.setenv(key, value)

    manifest = resolve_deployment(environment.graph_deployment_name)

    assert manifest.name == environment.graph_deployment_name
    assert manifest.deployment_dir.is_relative_to(release.repo_root)
    assert find_deployment_dir(environment.graph_deployment_name) == (
        manifest.deployment_dir
    )
    assert deployment_loads_module(
        environment.graph_deployment_name,
        "fixture",
    )
    with pytest.raises(DeploymentResolutionError, match="outside its active"):
        resolve_deployment("foreign-deployment")


def test_launcher_worker_authority_proves_all_runtime_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime.release_authority import (
        verify_worker_release_authority,
    )
    from okg.substrate.runtime.release_launcher import (
        build_exec_environment,
        verify_launch,
    )

    repo, commit = _fixture_repo(tmp_path)
    environment, release, values = _prepare_environment_fixture(
        repo,
        commit,
        tmp_path / "state",
    )
    activate_environment_release(
        environment=environment,
        release=release,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    launched = build_exec_environment(
        verify_launch(launcher_descriptor_path(environment)),
        environ=values,
    )
    package = release.venv_root / "lib/python3.12/site-packages/okg/__init__.py"
    prefix = _started_by_launcher(
        monkeypatch, release.interpreter, environment.runtime.state_root
    )

    with pytest.raises(SealedReleaseInvalid, match="isolated Python mode"):
        verify_worker_release_authority(
            deployment=environment.graph_deployment_name,
            dsn=values["TEST_PRODUCTION_DSN"],
            environ=launched,
            executable=release.interpreter,
            package_file=package,
            isolated=False,
        )
    authority = verify_worker_release_authority(
        deployment=environment.graph_deployment_name,
        dsn=values["TEST_PRODUCTION_DSN"],
        environ=launched,
        executable=release.interpreter,
        package_file=package,
        isolated=True,
    )

    assert authority is not None
    assert authority["artifact_id"] == release.artifact_id
    assert authority["environment_id"] == environment.id
    assert authority["arguments"] == ["-I", "-m", "okg.substrate.runtime.worker"]
    assert authority["pycache_prefix"] == prefix
    assert authority["database_identity"] == environment.database.dbos_identity
    assert Path(authority["definition_root"]).is_relative_to(release.repo_root)
    with pytest.raises(SealedReleaseInvalid, match="okg.__file__"):
        verify_worker_release_authority(
            deployment=environment.graph_deployment_name,
            dsn=values["TEST_PRODUCTION_DSN"],
            environ=launched,
            executable=release.interpreter,
            package_file=repo / "app.txt",
            isolated=True,
        )


def test_launcher_refuses_definition_root_outside_artifact(
    tmp_path: Path,
) -> None:
    repo, _commit_id = _fixture_repo(tmp_path)
    external = tmp_path / "external-definition"
    (external / "schemas").mkdir(parents=True)
    (external / "definition.yaml").write_text(
        "api_version: okg.deployment-definition/v1\n"
        "id: external-fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n",
        encoding="utf-8",
    )
    (external / "source_registry.yaml").write_text(
        "sources: []\n",
        encoding="utf-8",
    )
    descriptor = repo / "ops/environments/fixture-production/environment.yaml"
    descriptor.write_text(
        descriptor.read_text(encoding="utf-8").replace(
            "../../definitions/shared-definition/definition.yaml",
            str(external / "definition.yaml"),
        ),
        encoding="utf-8",
    )
    commit = _commit(repo, "external definition")
    values = _environment_values(tmp_path / "state")
    environment = load_environment(descriptor, environ=values)

    with pytest.raises(SealedReleaseInvalid, match="definition.*outside"):
        prepare_environment_release(
            environment=environment,
            source_repo=repo,
            revision=commit,
            dependency_sync=_fake_sync("outside", []),
            environ=values,
        )


def test_launcher_refuses_tampered_artifact_before_candidate_import(
    tmp_path: Path,
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    environment, release, values = _prepare_environment_fixture(
        repo,
        commit,
        tmp_path / "state",
    )
    activate_environment_release(
        environment=environment,
        release=release,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    target = release.repo_root / "app.txt"
    target.chmod(stat.S_IMODE(target.stat().st_mode) | stat.S_IWUSR)
    target.write_text("tampered\n", encoding="utf-8")
    launcher = Path(__file__).resolve().parents[2] / (
        "src/okg/substrate/runtime/release_launcher.py"
    )

    proc = subprocess.run(
        [
            sys.executable,
            "-I",
            str(launcher),
            "--descriptor",
            str(launcher_descriptor_path(environment)),
            "--verify-only",
            "--json",
        ],
        env=values,
        check=False,
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 2
    error = json.loads(proc.stderr)["error"]
    assert error["code"] == "sealed_release_invalid"
    assert "tree digest mismatch" in error["message"]


# --- #1983: a sealed release refuses a null revision, never writes "None" ---


def _issue1983_environment(tmp_path: Path, pin: dict[str, object]):
    from dataclasses import dataclass, field
    from types import SimpleNamespace

    @dataclass(frozen=True)
    class _Manifest:
        deployment_dir: Path
        release: dict = field(default_factory=dict)

    deployment_dir = tmp_path / "deployment"
    deployment_dir.mkdir()
    return SimpleNamespace(
        release=SimpleNamespace(pin=pin),
        manifest=_Manifest(deployment_dir=deployment_dir),
    )


def _issue1983_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*") if path.is_file())


@pytest.mark.parametrize("null_revision", [None, "", "   "])
def test_issue1983_sealed_release_refuses_null_code_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    null_revision: object,
) -> None:
    from types import SimpleNamespace

    import okg.substrate.deployment_release as deployment_release
    import okg.substrate.deployment_upgrade as deployment_upgrade
    from okg.substrate.sealed_release import SealedReleaseRevisionMissing

    environment = _issue1983_environment(
        tmp_path, {"code_repository": str(tmp_path / "core")}
    )
    monkeypatch.setattr(
        deployment_release,
        "current_release_artifact",
        lambda _manifest: SimpleNamespace(code_revision=null_revision),
    )

    def forbidden(*args, **kwargs):
        raise AssertionError(("repository authority resolved", args, kwargs))

    monkeypatch.setattr(deployment_upgrade, "_resolve_repo_authorities", forbidden)
    before = _issue1983_files(tmp_path)

    with pytest.raises(SealedReleaseRevisionMissing) as excinfo:
        prepare_environment_release(
            environment=environment,
            source_repo=tmp_path / "deployment",
            revision="a" * 40,
            environ={},
        )

    error = excinfo.value.to_json()["error"]
    assert error["code"] == "sealed_release_revision_missing"
    assert isinstance(excinfo.value, SealedReleaseInvalid)
    assert error["data"] == {"field": "code_revision", "observed": null_revision}
    after = _issue1983_files(tmp_path)
    assert after == before
    for path in after:
        assert "None" not in path.read_text(encoding="utf-8", errors="replace")


@pytest.mark.parametrize("null_revision", [None, ""])
def test_issue1983_sealed_release_refuses_null_revision_argument(
    tmp_path: Path,
    null_revision: object,
) -> None:
    from okg.substrate.sealed_release import SealedReleaseRevisionMissing

    environment = _issue1983_environment(tmp_path, {})
    with pytest.raises(SealedReleaseRevisionMissing) as excinfo:
        prepare_environment_release(
            environment=environment,
            source_repo=tmp_path / "deployment",
            revision=null_revision,  # type: ignore[arg-type]
            environ={},
        )
    assert excinfo.value.to_json()["error"]["data"]["field"] == "revision"
