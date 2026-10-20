"""Per-machine ``sync_root`` declaration (issue #1014).

A deployment shared by more than one developer could not declare ``sync_root``:
the value is a machine-specific absolute path and the only place to put it is a
tracked, shipped ``deployment.yaml``. These tests pin the three states the
resolver distinguishes -- undeclared / declared / unresolved -- and prove every
consumer agrees, because the bug class this change guards against is the
declared-but-unresolvable case being read as "operator declared none", which is
a missing signal wearing a satisfied one's clothes.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from okg.substrate.deployments import DeploymentManifest
from okg.substrate.deployments.sync_root import (
    SOURCE_LOCAL_PROFILE,
    SOURCE_MANIFEST_LITERAL,
    SOURCE_PROCESS_ENV,
    STATE_DECLARED,
    STATE_UNDECLARED,
    STATE_UNRESOLVED,
    require_usable_source_configuration_authority,
    resolve_declared_sync_root,
)
from okg.substrate.errors import (
    SourceConfigurationAuthorityInvalid,
    SubstrateError,
)
from okg.substrate.runtime.deployment_drift import check_deployment_code_drift

VAR = "OKG_FIXTURE_SYNC_ROOT"
REPO_ROOT = Path(__file__).resolve().parents[3]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def checkout(tmp_path: Path) -> Path:
    """A real git checkout with an ``origin/dev`` ref, so the drift probe has
    something measurable to point at."""
    root = tmp_path / "sync-root"
    root.mkdir()
    subprocess.run(
        ["git", "-c", "init.defaultBranch=dev", "init", str(root)],
        check=True, capture_output=True,
    )
    _git(root, "config", "user.email", "t@example.invalid")
    _git(root, "config", "user.name", "T")
    _git(root, "config", "commit.gpgsign", "false")
    (root / "f.txt").write_text("base\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-m", "base")
    _git(root, "update-ref", "refs/remotes/origin/dev", "HEAD")
    return root.resolve()


@pytest.fixture
def deployment_dir(tmp_path: Path) -> Path:
    """A ``deployments/<name>/`` directory, so ``${repo_root}`` resolves the
    way the real layout makes it resolve."""
    d = tmp_path / "repo" / "deployments" / "fixture"
    d.mkdir(parents=True)
    return d


def write_manifest(deployment_dir: Path, sync_root: str | None) -> Path:
    body = "name: fixture\n"
    if sync_root is not None:
        body += f"sync_root: {sync_root}\n"
    path = deployment_dir / "deployment.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def raw_of(deployment_dir: Path) -> dict:
    from okg.substrate import fastyaml

    return fastyaml.safe_load(
        (deployment_dir / "deployment.yaml").read_text(encoding="utf-8")
    )


def write_local_env(root: Path, **values: str) -> Path:
    """Write a ``.okg/local.env`` and point the resolver at it.

    The resolver honours the same pytest guard as ``load_local_env_for_cli``:
    a developer machine's profile must not change unit-test outcomes unless the
    test names the file explicitly.
    """
    path = root / ".okg" / "local.env"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(f"{k}={v}\n" for k, v in values.items()), encoding="utf-8",
    )
    return path


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch):
    """No ambient value for the fixture variable, and no ambient profile."""
    monkeypatch.delenv(VAR, raising=False)
    monkeypatch.delenv("OKG_LOCAL_ENV", raising=False)
    monkeypatch.delenv("OKG_ENABLE_LOCAL_ENV_FOR_TESTS", raising=False)
    from okg.substrate.deployments import sync_root as sr

    sr._PROFILE_CACHE.clear()
    sr._PATH_CACHE.clear()
    yield
    sr._PROFILE_CACHE.clear()
    sr._PATH_CACHE.clear()


# ---------------------------------------------------------------------------
# resolver
# ---------------------------------------------------------------------------


def test_source_configuration_authority_canonicalizes_one_directory(
    tmp_path: Path,
) -> None:
    target = tmp_path / "authoritative"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)

    assert (
        require_usable_source_configuration_authority(
            None,
            command="ingest",
        )
        is None
    )
    assert (
        require_usable_source_configuration_authority(
            alias,
            command="ingest",
        )
        == target.resolve()
    )


@pytest.mark.parametrize("shape", ["missing", "file", "broken_symlink"])
def test_source_configuration_authority_refuses_invalid_path_shapes(
    tmp_path: Path,
    shape: str,
) -> None:
    configured = tmp_path / shape
    if shape == "file":
        configured.write_text("not a directory\n", encoding="utf-8")
    elif shape == "broken_symlink":
        configured.symlink_to(tmp_path / "absent", target_is_directory=True)

    with pytest.raises(SourceConfigurationAuthorityInvalid) as exc:
        require_usable_source_configuration_authority(
            configured,
            command="ingest",
        )

    assert exc.value.code == "source_configuration_authority_invalid"
    assert exc.value.data["configured_path"] == str(configured)
    assert "--inline" not in (exc.value.fix_hint or "")


def test_source_configuration_authority_refuses_resolution_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured = tmp_path / "permission-denied"
    real_resolve = Path.resolve

    def deny_resolve(self: Path, strict: bool = False) -> Path:
        if self == configured and strict:
            raise PermissionError("fixture denies traversal")
        return real_resolve(self, strict=strict)

    monkeypatch.setattr(Path, "resolve", deny_resolve)
    with pytest.raises(
        SourceConfigurationAuthorityInvalid,
        match="configured source root is not usable",
    ) as exc:
        require_usable_source_configuration_authority(
            configured,
            command="source sync",
        )
    assert "PermissionError" in exc.value.data["reason"]


def test_source_configuration_authority_refuses_unknown_home_user() -> None:
    configured = "~okg-user-that-does-not-exist/source"

    with pytest.raises(
        SourceConfigurationAuthorityInvalid,
        match="configured source root is not usable",
    ) as exc:
        require_usable_source_configuration_authority(
            configured,
            command="source sync",
        )

    assert exc.value.code == "source_configuration_authority_invalid"
    assert exc.value.data["configured_path"] == configured
    assert exc.value.data["reason"].startswith("RuntimeError:")
    assert "declared sync_root authority" in (exc.value.fix_hint or "")


def test_source_configuration_authority_refuses_embedded_nul() -> None:
    configured = "/tmp/source\0root"

    with pytest.raises(
        SourceConfigurationAuthorityInvalid,
        match="configured source root is not usable",
    ) as exc:
        require_usable_source_configuration_authority(
            configured,
            command="source sync",
        )

    assert exc.value.code == "source_configuration_authority_invalid"
    assert exc.value.data["configured_path"] == configured
    assert exc.value.data["reason"].startswith("ValueError:")
    assert "declared sync_root authority" in (exc.value.fix_hint or "")


@pytest.mark.parametrize(
    ("configured", "reason_type"),
    [
        ("~okg-user-that-does-not-exist/source", "RuntimeError:"),
        ("/tmp/source\0root", "ValueError:"),
    ],
)
def test_manifest_resolution_defers_exceptional_path_to_typed_authority_guard(
    configured: str,
    reason_type: str,
    tmp_path: Path,
) -> None:
    resolution = resolve_declared_sync_root(
        {"sync_root": configured},
        deployment_dir=tmp_path,
    )

    assert resolution.state == STATE_DECLARED
    assert resolution.path is not None
    with pytest.raises(SourceConfigurationAuthorityInvalid) as exc:
        require_usable_source_configuration_authority(
            resolution.path,
            command="ingest",
        )

    assert exc.value.code == "source_configuration_authority_invalid"
    assert exc.value.data["reason"].startswith(reason_type)
    assert "declared sync_root authority" in (exc.value.fix_hint or "")


def test_source_configuration_authority_refuses_relative_path() -> None:
    with pytest.raises(SourceConfigurationAuthorityInvalid) as exc:
        require_usable_source_configuration_authority(
            Path("relative/source-root"),
            command="source sync",
        )

    assert exc.value.data == {
        "configured_path": "relative/source-root",
        "reason": "relative_path_not_authoritative",
    }


def test_source_configuration_authority_refuses_unreadable_directory(
    tmp_path: Path,
) -> None:
    configured = tmp_path / "unreadable"
    configured.mkdir()
    configured.chmod(0)
    try:
        with pytest.raises(SourceConfigurationAuthorityInvalid) as exc:
            require_usable_source_configuration_authority(
                configured,
                command="source sync",
            )
        assert (
            exc.value.data["reason"]
            == "directory_not_readable_or_searchable"
        )
    finally:
        configured.chmod(0o700)


def test_sealed_source_configuration_refusal_names_launcher_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_RELEASE_ARTIFACT_ID", "artifact-fixture")
    with pytest.raises(SourceConfigurationAuthorityInvalid) as exc:
        require_usable_source_configuration_authority(
            tmp_path / "missing",
            command="source sync",
        )
    assert "environment descriptor or launcher" in (exc.value.fix_hint or "")
    assert ".okg/local.env" not in (exc.value.fix_hint or "")


def test_sealed_source_configuration_accepts_existing_non_git_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "sealed-source-root"
    source_root.mkdir()
    monkeypatch.setenv("OKG_RELEASE_ARTIFACT_ID", "artifact-fixture")

    assert require_usable_source_configuration_authority(
        source_root,
        command="source sync",
    ) == source_root.resolve()


def test_resolver_reports_undeclared_when_no_key(deployment_dir: Path) -> None:
    write_manifest(deployment_dir, None)
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.state == STATE_UNDECLARED
    assert r.path is None and r.missing == ()


def test_resolver_literal_path_reports_manifest_literal(
    deployment_dir: Path, checkout: Path,
) -> None:
    write_manifest(deployment_dir, str(checkout))
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.state == STATE_DECLARED
    assert r.path == checkout
    assert r.source == SOURCE_MANIFEST_LITERAL


def test_resolver_expands_variable_from_process_env(
    deployment_dir: Path, checkout: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_manifest(deployment_dir, "${" + VAR + "}")
    monkeypatch.setenv(VAR, str(checkout))
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.state == STATE_DECLARED
    assert r.path == checkout
    assert r.source == SOURCE_PROCESS_ENV


def test_resolver_expands_variable_from_local_profile(
    deployment_dir: Path, checkout: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = write_local_env(tmp_path, **{VAR: str(checkout)})
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    write_manifest(deployment_dir, "${" + VAR + "}")
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.state == STATE_DECLARED
    assert r.path == checkout
    assert r.source == SOURCE_LOCAL_PROFILE


def test_resolver_process_env_beats_local_profile(
    deployment_dir: Path, checkout: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Process-first is what lets a supervisor override a stale file at the
    sync root; the rejected local-profile-first rule would invert that."""
    stale = tmp_path / "stale"
    stale.mkdir()
    profile = write_local_env(tmp_path, **{VAR: str(stale)})
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    monkeypatch.setenv(VAR, str(checkout))
    write_manifest(deployment_dir, "${" + VAR + "}")
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.path == checkout
    assert r.source == SOURCE_PROCESS_ENV


def test_resolver_attributes_an_auto_loaded_profile_value_to_the_profile(
    deployment_dir: Path, checkout: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Most CLI entry points copy `.okg/local.env` into `os.environ` before
    dispatch, so a value that originated in a gitignored file arrives looking
    like a shell export. Attribution names the least-tracked source that could
    have supplied it, not the most flattering one."""
    profile = write_local_env(tmp_path, **{VAR: str(checkout)})
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    monkeypatch.setenv(VAR, str(checkout))  # what load_local_env_for_cli does
    write_manifest(deployment_dir, "${" + VAR + "}")
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.path == checkout
    assert r.source == SOURCE_LOCAL_PROFILE


def test_resolver_local_profile_applies_the_okg_key_filter(
    deployment_dir: Path, checkout: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A local profile contributes the same key set here as everywhere else:
    ``load_local_env`` admits ``OKG_``-prefixed keys only. Two ingestion rules
    for one file would make the laxer one a bypass."""
    profile = write_local_env(tmp_path, COOPER_SYNC_ROOT=str(checkout))
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    write_manifest(deployment_dir, "${COOPER_SYNC_ROOT}")
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.state == STATE_UNRESOLVED
    assert r.missing == ("COOPER_SYNC_ROOT",)


def test_resolver_reads_the_deployments_profile_not_the_cwds(
    deployment_dir: Path, checkout: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The start directory is the DEPLOYMENT's, never the process's working
    directory. If a consumer resolved against its own cwd it would compute a
    different sync root than one that resolved against the deployment -- two
    Tier W authority values inside one process, which is the disagreement this
    module exists to remove."""
    from okg.substrate.deployments import sync_root as sr

    # A profile beside the deployment's own checkout says `checkout`; the
    # process's working directory has a different one.
    deployment_checkout = deployment_dir.parent.parent
    (deployment_checkout / ".git").mkdir(parents=True, exist_ok=True)
    write_local_env(deployment_checkout, **{VAR: str(checkout)})

    cwd_checkout = tmp_path / "elsewhere"
    (cwd_checkout / ".git").mkdir(parents=True)
    write_local_env(cwd_checkout, **{VAR: str(tmp_path / "wrong")})

    monkeypatch.setenv("OKG_ENABLE_LOCAL_ENV_FOR_TESTS", "1")
    monkeypatch.chdir(cwd_checkout)
    sr._PATH_CACHE.clear()
    write_manifest(deployment_dir, "${" + VAR + "}")

    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    sr._PATH_CACHE.clear()
    assert r.path == checkout
    assert r.path != (tmp_path / "wrong")


def test_resolver_refuses_rather_than_guessing_a_deployment_dir(
    deployment_dir: Path,
) -> None:
    """Falling back to `Path.cwd()` would silently redefine `${repo_root}` and
    the profile location as "wherever this process happens to run"."""
    from okg.substrate.deployments.sync_root import SyncRootResolverError

    write_manifest(deployment_dir, "${" + VAR + "}")
    with pytest.raises(SyncRootResolverError):
        resolve_declared_sync_root(raw_of(deployment_dir), deployment_dir=None)

    # An undeclared sync_root needs no directory, so it still answers.
    assert resolve_declared_sync_root(
        {"name": "x"}, deployment_dir=None,
    ).state == STATE_UNDECLARED


def test_resolver_names_a_malformed_profile_instead_of_blaming_the_variable(
    deployment_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A profile that fails to parse contributes nothing. Reporting that as
    'the variable is unset' sends the operator to add a line that is already
    there and throws away the file and line the parser knew."""
    profile = tmp_path / ".okg" / "local.env"
    profile.parent.mkdir(parents=True)
    profile.write_text(f"{VAR}\n", encoding="utf-8")  # no '=' -> ValueError
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    write_manifest(deployment_dir, "${" + VAR + "}")

    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.state == STATE_UNRESOLVED
    assert "could not be parsed" in (r.detail or "")
    assert "local.env" in (r.detail or "")


def test_every_authority_surface_calls_the_refusal_helper() -> None:
    """The helper only protects the surfaces that call it. These four each
    read `manifest.sync_root` as authority -- as a worker working directory or
    as a source-registry root override -- where `None` silently means "some
    default root"."""
    import inspect

    from okg.substrate import deployment_apply
    from okg.substrate.cli import provision
    from okg.substrate.runtime import source_sync_workflow

    for module, func in (
        (deployment_apply, "desired_deployment_state"),
        (provision, "_cmd_provision"),
        (source_sync_workflow, "_resolve_context_cached"),
    ):
        source = inspect.getsource(getattr(module, func))
        assert "require_resolved_sync_root(" in source, (
            f"{module.__name__}.{func} reads sync_root as authority but does "
            "not refuse an unresolved one"
        )

    from okg.substrate.cli import main as cli_main

    assert "require_resolved_sync_root(" in inspect.getsource(cli_main)


def test_the_refusal_helper_does_not_read_its_own_signal_fail_open() -> None:
    """`getattr(manifest, 'sync_root_unresolved', ())` would make a
    manifest-like object without the field indistinguishable from one with
    nothing unresolved, silently turning every refusal into a no-op."""
    from types import SimpleNamespace

    from okg.substrate.deployments.sync_root import require_resolved_sync_root

    with pytest.raises(AttributeError):
        require_resolved_sync_root(SimpleNamespace(), command="ingest")


def test_the_cached_profile_mapping_cannot_be_mutated(
    tmp_path: Path, checkout: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cached mapping is shared by every later resolve in the process."""
    from okg.substrate.deployments.sync_root import local_profile_values

    profile = write_local_env(tmp_path, **{VAR: str(checkout)})
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    values = local_profile_values(tmp_path)
    assert values[VAR] == str(checkout)
    with pytest.raises(TypeError):
        values["OKG_INJECTED"] = "/somewhere"  # type: ignore[index]


def test_resolver_unset_variable_is_unresolved_not_undeclared(
    deployment_dir: Path,
) -> None:
    """The whole point of the change: a declared-but-unresolvable sync root is
    its own state. Collapsing it into 'undeclared' turns a missing signal into
    a satisfied one."""
    write_manifest(deployment_dir, "${" + VAR + "}")
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.state == STATE_UNRESOLVED
    assert r.state != STATE_UNDECLARED
    assert r.missing == (VAR,)
    assert r.path is None
    assert VAR in (r.detail or "")


def test_resolver_refuses_secret_like_variable(deployment_dir: Path) -> None:
    write_manifest(deployment_dir, "${OKG_FIXTURE_DSN}")
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.state == STATE_UNRESOLVED
    assert "secret-like" in (r.detail or "")


def test_resolver_refuses_secret_like_variable_even_when_set(
    deployment_dir: Path, checkout: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The refusal is on the NAME, so a status report can never echo a
    credential -- being set must not make it resolvable."""
    monkeypatch.setenv("OKG_FIXTURE_TOKEN", str(checkout))
    write_manifest(deployment_dir, "${OKG_FIXTURE_TOKEN}")
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.state == STATE_UNRESOLVED


def test_resolver_normalizes_relative_and_tilde(
    deployment_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_manifest(deployment_dir, "../..")
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.path == deployment_dir.parent.parent.resolve()

    home = deployment_dir.parent.parent / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    write_manifest(deployment_dir, "~/checkouts/x")
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.path == (home / "checkouts" / "x").resolve()


# ---------------------------------------------------------------------------
# anchors
# ---------------------------------------------------------------------------


def test_anchors_resolve_and_are_never_reported_as_env_vars(
    deployment_dir: Path,
) -> None:
    """``${repo_root}`` is a path anchor the loader has always expanded. The
    guard and doctor read the manifest without the loader, so without anchor
    expansion here they would report a missing environment variable named
    ``repo_root`` -- a refusal on a manifest that is entirely valid."""
    write_manifest(deployment_dir, "${repo_root}")
    r = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    assert r.state == STATE_DECLARED
    assert r.missing == ()
    assert r.path == deployment_dir.parent.parent.resolve()


def test_anchors_are_idempotent_for_already_substituted_input(
    deployment_dir: Path, checkout: Path,
) -> None:
    """The typed loader substitutes anchors before ``_build_from_raw``; the
    guard does not. Running substitution on an already-substituted value must
    be a no-op, not a corruption."""
    write_manifest(deployment_dir, str(checkout))
    once = resolve_declared_sync_root(
        raw_of(deployment_dir), deployment_dir=deployment_dir,
    )
    twice = resolve_declared_sync_root(
        {"sync_root": once.path_str}, deployment_dir=deployment_dir,
    )
    assert once.path == twice.path


# ---------------------------------------------------------------------------
# consumers
# ---------------------------------------------------------------------------


def _point_at(monkeypatch: pytest.MonkeyPatch, deployment_dir: Path) -> None:
    """Make the guard/doctor read the fixture deployment through the real
    ``_read_manifest_with_dir`` path."""
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _name: deployment_dir,
    )


def _guard(deployment_dir: Path, *, cwd: Path) -> dict:
    from okg.substrate.deployment_authority import check_publish_provenance

    return check_publish_provenance("fixture", None, repo_start=cwd)


def test_consumers_agree_on_a_resolved_variable(
    deployment_dir: Path, checkout: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_manifest(deployment_dir, "${" + VAR + "}")
    monkeypatch.setenv(VAR, str(checkout))
    _point_at(monkeypatch, deployment_dir)

    manifest = DeploymentManifest.from_yaml_lenient(
        deployment_dir / "deployment.yaml"
    )
    assert manifest.sync_root == checkout

    verdict = _guard(deployment_dir, cwd=checkout)
    assert verdict["sync_root"] == str(checkout)
    assert verdict["mode"] == "sync_root" and verdict["allowed"] is True

    drift = check_deployment_code_drift(deployment="fixture")
    assert drift.sync_root == str(checkout)
    assert drift.status != "misconfigured", drift.problems


def test_consumers_agree_the_producing_checkout_is_foreign(
    deployment_dir: Path, checkout: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resolved `${VAR}` must still refuse a foreign checkout -- per-machine
    declaration relaxes WHERE the value comes from, never WHAT it enforces."""
    other = tmp_path / "other"
    other.mkdir()
    subprocess.run(
        ["git", "-c", "init.defaultBranch=dev", "init", str(other)],
        check=True, capture_output=True,
    )
    _git(other, "config", "user.email", "t@example.invalid")
    _git(other, "config", "user.name", "T")
    _git(other, "config", "commit.gpgsign", "false")
    (other / "f.txt").write_text("other\n", encoding="utf-8")
    _git(other, "add", "-A")
    _git(other, "commit", "-m", "other")
    write_manifest(deployment_dir, "${" + VAR + "}")
    monkeypatch.setenv(VAR, str(checkout))
    _point_at(monkeypatch, deployment_dir)

    verdict = _guard(deployment_dir, cwd=other)
    assert verdict["allowed"] is False
    assert verdict["mode"] == "foreign_checkout"


def test_consumers_agree_a_literal_manifest_is_unchanged(
    deployment_dir: Path, checkout: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backward-compatibility pin: nothing that works today changes."""
    write_manifest(deployment_dir, str(checkout))
    _point_at(monkeypatch, deployment_dir)

    manifest = DeploymentManifest.from_yaml_lenient(
        deployment_dir / "deployment.yaml"
    )
    assert manifest.sync_root == checkout
    assert manifest.sync_root_unresolved == ()
    assert _guard(deployment_dir, cwd=checkout)["mode"] == "sync_root"
    drift = check_deployment_code_drift(deployment="fixture")
    assert drift.status == "ok", drift.problems
    assert drift.sync_root_source == SOURCE_MANIFEST_LITERAL


def test_consumers_agree_on_the_repo_root_anchor(
    deployment_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_manifest(deployment_dir, "${repo_root}")
    _point_at(monkeypatch, deployment_dir)
    expected = str(deployment_dir.parent.parent.resolve())

    manifest = DeploymentManifest.from_yaml_lenient(
        deployment_dir / "deployment.yaml"
    )
    assert str(manifest.sync_root) == expected
    assert _guard(deployment_dir, cwd=deployment_dir)["sync_root"] == expected
    drift = check_deployment_code_drift(deployment="fixture")
    assert drift.sync_root == expected


# ---------------------------------------------------------------------------
# unresolved fails closed
# ---------------------------------------------------------------------------


def test_guard_refuses_unresolved_and_never_calls_it_undeclared(
    deployment_dir: Path, checkout: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_manifest(deployment_dir, "${" + VAR + "}")
    _point_at(monkeypatch, deployment_dir)
    verdict = _guard(deployment_dir, cwd=checkout)
    assert verdict["allowed"] is False
    assert verdict["mode"] == "sync_root_unresolved"
    assert verdict["mode"] != "sync_root_undeclared"
    assert verdict["unresolved_variables"] == [VAR]


def test_drift_reports_unresolved_naming_the_variable(
    deployment_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_manifest(deployment_dir, "${" + VAR + "}")
    _point_at(monkeypatch, deployment_dir)
    drift = check_deployment_code_drift(deployment="fixture")
    assert drift.status == "misconfigured"
    joined = " ".join(drift.problems)
    assert VAR in joined
    assert "declares no sync_root" not in joined


def _full_manifest(deployment_dir: Path, sync_root: str) -> Path:
    """A manifest complete enough for the STRICT loader."""
    (deployment_dir / "schema").mkdir(exist_ok=True)
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n", encoding="utf-8",
    )
    path = deployment_dir / "deployment.yaml"
    path.write_text(
        "name: fixture\nschema_dir: schema\n"
        "source_registry: source_registry.yaml\n"
        f"sync_root: {sync_root}\n",
        encoding="utf-8",
    )
    return path


def test_strict_loader_carries_unresolved_instead_of_raising(
    deployment_dir: Path,
) -> None:
    """`from_yaml` is the single door for every read-only verb (`okg search`,
    `okg trace`, `okg doctor`). A manifest that ships inside an installed
    package would make all of them die on a machine that never set the
    variable -- a regression for exactly the consumers this field exists to
    serve. The declaration is carried; the refusal lives on the paths that USE
    it as authority."""
    path = _full_manifest(deployment_dir, "${" + VAR + "}")
    manifest = DeploymentManifest.from_yaml(path)
    assert manifest.sync_root is None
    assert manifest.sync_root_unresolved == (VAR,)


def test_write_paths_refuse_an_unresolved_declaration(
    deployment_dir: Path,
    tmp_path: Path,
) -> None:
    """...and the refusal is real: a surface that would otherwise read
    `sync_root` as None and silently fall back to a default repo root."""
    from okg.substrate.deployments.sync_root import require_resolved_sync_root

    path = _full_manifest(deployment_dir, "${" + VAR + "}")
    manifest = DeploymentManifest.from_yaml(path)
    with pytest.raises(SubstrateError) as exc:
        require_resolved_sync_root(manifest, command="ingest")
    assert VAR in json.dumps(getattr(exc.value, "data", {}) or {})

    # A resolved or absent declaration passes through untouched.
    resolved_deployment_dir = tmp_path / "resolved" / "deployments" / "fixture"
    resolved_deployment_dir.mkdir(parents=True)
    require_resolved_sync_root(
        DeploymentManifest.from_yaml(
            _full_manifest(resolved_deployment_dir, "/tmp/x")
        ),
        command="ingest",
    )


def test_undeclared_still_allows_with_a_warning(
    deployment_dir: Path, checkout: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Historical behaviour pin: an operator who declares nothing keeps the
    allow-with-warning verdict. Only the DECLARED-but-unresolvable case is new,
    so nothing that works today starts refusing."""
    write_manifest(deployment_dir, None)
    _point_at(monkeypatch, deployment_dir)
    verdict = _guard(deployment_dir, cwd=checkout)
    assert verdict["allowed"] is True
    assert verdict["mode"] == "sync_root_undeclared"


# ---------------------------------------------------------------------------
# lenient loader + deployment apply
# ---------------------------------------------------------------------------


def test_lenient_loader_records_unresolved_without_raising(
    deployment_dir: Path,
) -> None:
    """``from_yaml_lenient`` serves callers that never read ``sync_root``
    (config-block reads), so it must not raise -- but it must carry the signal
    so a caller that DOES read the field can fail closed."""
    write_manifest(deployment_dir, "${" + VAR + "}")
    manifest = DeploymentManifest.from_yaml_lenient(
        deployment_dir / "deployment.yaml"
    )
    assert manifest.sync_root is None
    assert manifest.sync_root_unresolved == (VAR,)


def test_apply_refuses_unresolved_instead_of_deriving_a_checkout(
    deployment_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the refusal, ``sync_root=None`` reaches
    ``_runtime_working_directory``, which derives the worker's working
    directory from whatever checkout the process runs in and folds it into
    ``desired_state_digest`` -- an unresolved declaration silently becoming a
    worker pinned to the wrong tree."""
    from okg.substrate import deployment_apply

    (deployment_dir / "schema").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n", encoding="utf-8",
    )
    (deployment_dir / "deployment.yaml").write_text(
        "name: fixture\nschema_dir: schema\n"
        "source_registry: source_registry.yaml\n"
        "sync_root: ${" + VAR + "}\n",
        encoding="utf-8",
    )
    with pytest.raises(SubstrateError) as exc:
        deployment_apply.desired_deployment_state(
            deployment="fixture",
            deployment_dir=deployment_dir,
            dsn="postgres://unused/deliberately",
        )
    assert VAR in json.dumps(getattr(exc.value, "data", {}) or {})
    assert "sync_root" in str(exc.value)


# ---------------------------------------------------------------------------
# source reporting + the supervised worker's channel
# ---------------------------------------------------------------------------


def test_provenance_and_drift_payloads_carry_the_source(
    deployment_dir: Path, checkout: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `local_profile` value moves no manifest fingerprint and shows in no
    diff, so which source supplied Tier W authority has to be readable."""
    profile = write_local_env(tmp_path, **{VAR: str(checkout)})
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    write_manifest(deployment_dir, "${" + VAR + "}")
    _point_at(monkeypatch, deployment_dir)

    assert _guard(deployment_dir, cwd=checkout)["sync_root_source"] == (
        SOURCE_LOCAL_PROFILE
    )
    drift = check_deployment_code_drift(deployment="fixture")
    assert drift.to_dict()["sync_root_source"] == SOURCE_LOCAL_PROFILE


def test_worker_channel_resolves_from_the_deployment_checkouts_local_env(
    checkout: Path, tmp_path: Path,
) -> None:
    """The supervised worker inherits no operator shell, so its channel is the
    deployment checkout's own ``.okg/local.env`` -- the same file that already
    carries ``OKG_DSN``. Run as a real subprocess with a SCRUBBED environment
    and a working directory somewhere else entirely, which proves both that the
    channel works and that it does not depend on where the process runs."""
    deployment_dir = checkout / "deployments" / "fixture"
    deployment_dir.mkdir(parents=True)
    write_manifest(deployment_dir, "${" + VAR + "}")
    write_local_env(checkout, **{VAR: str(checkout)})

    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()

    script = textwrap.dedent(
        f"""
        import json
        from pathlib import Path
        from okg.substrate import fastyaml
        from okg.substrate.deployments.sync_root import (
            resolve_declared_sync_root,
        )

        d = Path({str(deployment_dir)!r})
        raw = fastyaml.safe_load(
            (d / "deployment.yaml").read_text(encoding="utf-8")
        )
        r = resolve_declared_sync_root(raw, deployment_dir=d)
        print(json.dumps({{"path": r.path_str, "source": r.source}}))
        """
    )
    # Scrub exactly what this test is about -- every OKG_* variable, so no
    # inherited value can supply the sync root -- and keep the rest of the
    # environment so the child can still import okg. Handing the child a
    # hand-built PYTHONPATH instead is what broke on CI, where `sys.executable`
    # is the toolcache interpreter rather than a venv python.
    env = {
        k: v for k, v in os.environ.items()
        if not k.startswith("OKG_") and k != "PYTEST_CURRENT_TEST"
    }
    assert not [k for k in env if k.startswith("OKG_")]
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(elsewhere), env=env, capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload["path"] == str(checkout)
    assert payload["source"] == SOURCE_LOCAL_PROFILE


def test_the_source_field_reaches_the_doctor_payload(
    deployment_dir: Path, checkout: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reported field with no reader is decoration. This pins that
    ``okg doctor --check code-drift --json`` and ``okg dogfood status --json``
    (which passes the whole code_drift mapping through) actually carry it."""
    write_manifest(deployment_dir, str(checkout))
    _point_at(monkeypatch, deployment_dir)
    payload = {
        "check": "code-drift",
        **check_deployment_code_drift(deployment="fixture").to_dict(),
    }
    assert payload["sync_root_source"] == SOURCE_MANIFEST_LITERAL
    # Serializable: the doctor emits this dict as JSON.
    assert json.loads(json.dumps(payload))["sync_root_source"] == (
        SOURCE_MANIFEST_LITERAL
    )


# ---------------------------------------------------------------------------
# identity + cache
# ---------------------------------------------------------------------------


def test_fingerprint_is_identical_under_different_resolved_values(
    deployment_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """REGRESSION PIN. ``sync_root`` is behaviour-bearing for
    ``manifest_fingerprint``, so a fingerprint computed over a RESOLVED path
    would differ per machine and manufacture ownership-drift refusals. This
    passes today and must keep passing."""
    from okg.substrate.catalog.manifest_identity import manifest_fingerprint_for

    write_manifest(deployment_dir, "${" + VAR + "}")
    monkeypatch.setenv(VAR, "/machine/a")
    fp_a, blocks_a = manifest_fingerprint_for(deployment_dir)
    monkeypatch.setenv(VAR, "/machine/b")
    fp_b, blocks_b = manifest_fingerprint_for(deployment_dir)

    assert fp_a == fp_b
    assert blocks_a["sync_root"] == blocks_b["sync_root"]


def test_fingerprint_differs_for_a_literal_path(deployment_dir: Path) -> None:
    """Non-vacuity for the pin above: the fingerprint DOES see this field, so
    the equality result is a property of hashing the literal, not of the field
    being ignored."""
    from okg.substrate.catalog.manifest_identity import manifest_fingerprint_for

    write_manifest(deployment_dir, "${" + VAR + "}")
    templated, _ = manifest_fingerprint_for(deployment_dir)
    write_manifest(deployment_dir, "/machine/a")
    literal, _ = manifest_fingerprint_for(deployment_dir)
    assert templated != literal


def test_manifest_cache_invalidates_when_the_local_profile_changes(
    deployment_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The manifest cache is keyed by (path, mtime) plus an env signature. If
    that signature snapshots ``os.environ`` only, a sync_root resolved from
    ``.okg/local.env`` survives an edit to that file and the deployment keeps
    publishing from the previous checkout."""
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    (deployment_dir / "schema").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n", encoding="utf-8",
    )
    (deployment_dir / "deployment.yaml").write_text(
        "name: fixture\nschema_dir: schema\n"
        "source_registry: source_registry.yaml\n"
        "sync_root: ${" + VAR + "}\n",
        encoding="utf-8",
    )
    profile = write_local_env(tmp_path, **{VAR: str(first)})
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))

    manifest_path = deployment_dir / "deployment.yaml"
    assert DeploymentManifest.from_yaml(manifest_path).sync_root == first

    write_local_env(tmp_path, **{VAR: str(second)})
    # Pin the mtime explicitly: filesystem timestamp granularity, not the
    # cache, would otherwise decide whether this test is meaningful.
    stat = profile.stat()
    os.utime(profile, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

    assert DeploymentManifest.from_yaml(manifest_path).sync_root == second


# ---------------------------------------------------------------------------
# guidance
# ---------------------------------------------------------------------------


def test_guidance_for_an_undeclared_sync_root_names_the_per_machine_route(
    deployment_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The old text said only "Declare sync_root in deployment.yaml", and
    following it literally is what puts a developer's home directory into a
    shared, shipped manifest."""
    write_manifest(deployment_dir, None)
    _point_at(monkeypatch, deployment_dir)
    joined = " ".join(check_deployment_code_drift(deployment="fixture").problems)
    assert "${" in joined
    assert ".okg/local.env" in joined


# ---------------------------------------------------------------------------
# exhaustive verdict enumeration (see pact design.md, "Formal verification")
# ---------------------------------------------------------------------------


def test_publish_provenance_verdict_is_total_and_never_allows_unresolved(
    ) -> None:
    """``publish_provenance_verdict`` classifies over a FINITE input domain, so
    it is checked by enumerating the whole cross-product rather than by picking
    cases. ``identity`` is enumerated as the full domain ``resolved_target``
    can emit -- ok / isolated / mismatch / undeclared / unresolved -- plus
    ``None``, because the function reads ``target.get("identity")`` from a
    caller-supplied dict and must classify a missing key too.

    Two properties: no input yields an undefined verdict, and no input whose
    sync root is unresolved is ever allowed. The second is what this change
    introduces, and enumerating it is what proves there is no combination of
    identity / file_derived / readability that routes an unresolved declaration
    back into the allow-with-warning branch.

    Scope of the claim: the path-equality branch touches the filesystem
    (``Path.resolve()``), so the checkout axis is enumerated as three
    REPRESENTATIVE relations -- unknown, equal, different -- not as every
    string. Every purely-classifying branch is covered exhaustively.
    """
    from okg.substrate.deployment_authority import publish_provenance_verdict

    from okg.substrate.deployment_authority import IDENTITY_VALUES

    # Read the domain from the producer's own declaration, so adding an
    # identity widens this enumeration instead of silently escaping it.
    identities = (*sorted(IDENTITY_VALUES), None)
    sync_roots = (None, "/repo/main")
    checkouts = (None, "/repo/main", "/repo/worktree")
    unresolveds = ((), ("OKG_X_SYNC_ROOT",))

    seen = 0
    for identity in identities:
        for file_derived in (True, False):
            for readable in (True, False):
                for sync_root in sync_roots:
                    for checkout in checkouts:
                        for unresolved in unresolveds:
                            verdict = publish_provenance_verdict(
                                sync_root=sync_root,
                                checkout=checkout,
                                target={"identity": identity},
                                file_derived=file_derived,
                                manifest_readable=readable,
                                sync_root_unresolved=unresolved,
                            )
                            seen += 1
                            assert isinstance(verdict.get("allowed"), bool)
                            assert verdict.get("mode")
                            if (
                                unresolved
                                and identity != "isolated"
                                and file_derived
                                and readable
                            ):
                                assert verdict["allowed"] is False
                                assert verdict["mode"] == "sync_root_unresolved"
    assert seen == (
        len(identities) * 2 * 2 * len(sync_roots)
        * len(checkouts) * len(unresolveds)
    )
    assert seen == 288


def test_no_identity_can_escape_the_declared_domain() -> None:
    """The enumeration above reads its domain from `IDENTITY_VALUES`, so an
    identity that escapes that declaration would silently shrink what the
    proof covers.

    The guarantee is enforced in PRODUCTION, not by a test that pattern-matches
    the syntax `resolved_target` happens to use today: `resolved_target` checks
    its own output against the declaration and refuses. A test that scraped the
    AST for `identity = "..."` would miss a value introduced by a conditional
    expression, an annotated assignment, or a named constant -- and would go on
    passing while the proof quietly stopped being exhaustive.
    """
    from okg.substrate.deployment_authority import (
        IDENTITY_VALUES,
        _resolved_target,
        resolved_target,
    )

    # The wrapper is the enforcement point, and it is not a no-op: feed it an
    # implementation that emits an undeclared identity and it refuses.
    monkey = pytest.MonkeyPatch()
    try:
        monkey.setattr(
            "okg.substrate.deployment_authority._resolved_target",
            lambda *a, **k: {"identity": "sixth"},
        )
        with pytest.raises(SubstrateError) as exc:
            resolved_target(None, deployment="d")
        assert "not in the declared domain" in str(exc.value)
    finally:
        monkey.undo()

    # And every identity the real implementation produces IS declared.
    produced = {
        _resolved_target(dsn, deployment="d", **kwargs)["identity"]
        for dsn, kwargs in (
            (None, {}),
            ("postgres://h/db", {"manifest_readable": False}),
            ("postgres://h/db", {}),
            ("postgres://h/db", {"expected_database": "db"}),
            ("postgres://h/db_abc", {"expected_database": "db"}),
            ("postgres://h/other", {"expected_database": "db"}),
        )
    }
    assert produced <= IDENTITY_VALUES
    assert produced == IDENTITY_VALUES, (
        f"IDENTITY_VALUES declares {sorted(IDENTITY_VALUES - produced)} that "
        "these inputs no longer produce; either the domain shrank or this "
        "test stopped covering it"
    )


def test_verdicts_are_unchanged_for_every_resolved_or_absent_input() -> None:
    """Backward-compatibility enumeration: for every input whose sync root is
    NOT unresolved, the verdict is byte-identical to what the function returned
    before this change. The new branch is additive, not a re-routing."""
    from okg.substrate.deployment_authority import publish_provenance_verdict

    expected = {
        # (identity, file_derived, readable, sync_root, checkout) -> mode
        ("isolated", True, True, None, None): "isolated_clone",
        ("ok", False, True, None, None): "event_plane",
        ("ok", True, False, "/repo/main", "/repo/main"): "manifest_unreadable",
        ("ok", True, True, None, "/repo/main"): "sync_root_undeclared",
        ("ok", True, True, "/repo/main", None): "checkout_unknown",
        ("ok", True, True, "/repo/main", "/repo/main"): "sync_root",
        ("ok", True, True, "/repo/main", "/repo/wt"): "foreign_checkout",
    }
    for (identity, fd, readable, sr, co), mode in expected.items():
        verdict = publish_provenance_verdict(
            sync_root=sr,
            checkout=co,
            target={"identity": identity},
            file_derived=fd,
            manifest_readable=readable,
        )
        assert verdict["mode"] == mode, (identity, fd, readable, sr, co)
