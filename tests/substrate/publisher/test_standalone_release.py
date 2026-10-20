"""`release.kind: standalone` -- the declaration that waives Git provenance.

#1955. An ordinary `okg install` writes its deployment to the documented XDG
user-state root, which is never a Git working tree, so the publisher's
repository-provenance step refused every first publish. The fix is an explicit
deployment declaration, not a fallback, and these tests pin each edge of it:

* the declaration is read and validated from the deployment, and a typo is
  refused rather than read as "no declaration";
* it waives provenance only when the deployment really is outside every
  repository;
* a deployment inside a repository keeps the ordinary tracking path and all
  of its refusals, declaration or not;
* combining it with a packaged or sealed release authority is refused.

The real publish these decisions guard is proven end to end against Postgres in
tests/substrate/e2e/test_publish_outside_git.py.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

from .._deployment_repo import GIT_ENV, HERMETIC_GIT

from okg.substrate.deployment_release import (
    ReleaseKindInvalid,
    StandaloneReleaseConflict,
    declared_release_kind,
    standalone_release_declared,
)
from okg.substrate.deployments import DeploymentManifest
from okg.substrate.publisher.repository_provenance import (
    RepositoryProvenance,
    deployment_is_outside_repository,
)


def _write_deployment(root: Path, *, release: str = "") -> Path:
    """A minimal deployment the STRICT manifest loader accepts.

    `schema_dir` and `source_registry` are the two keys `from_yaml` requires,
    and it validates that declared paths exist -- so both are created. The
    publish path already parses the manifest strictly (via
    `tracking_repository_provenance`), so reading the declaration the same way
    adds no new tolerance.
    """
    root.mkdir(parents=True, exist_ok=True)
    (root / "schema").mkdir(exist_ok=True)
    (root / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    (root / "deployment.yaml").write_text(
        "name: sample\n"
        "schema_dir: schema\n"
        "source_registry: source_registry.yaml\n"
        f"{release}",
        encoding="utf-8",
    )
    return root


# ---------------------------------------------------------------- declaration


@pytest.mark.parametrize(
    ("release", "expected"),
    [
        ("", None),
        ("release:\n  channel: dev\n", None),
        ("release:\n  channel: dev\n  kind: standalone\n", "standalone"),
        ("release:\n  channel: dev\n  kind: packaged\n", "packaged"),
    ],
)
def test_declared_release_kind_reads_the_manifest(
    tmp_path: Path, release: str, expected: str | None,
) -> None:
    root = _write_deployment(tmp_path / "dep", release=release)
    manifest = DeploymentManifest.from_yaml(root / "deployment.yaml")
    assert declared_release_kind(manifest) == expected


def test_unknown_release_kind_is_refused_not_ignored(tmp_path: Path) -> None:
    """A typo must not read as "no declaration" and fail later as a Git error."""
    root = _write_deployment(
        tmp_path / "dep", release="release:\n  channel: dev\n  kind: standalon\n",
    )
    manifest = DeploymentManifest.from_yaml(root / "deployment.yaml")

    with pytest.raises(ReleaseKindInvalid) as raised:
        declared_release_kind(manifest)

    error = raised.value
    assert error.code == "release_kind_invalid"
    assert error.data["observed"] == "standalon"
    assert error.data["allowed"] == ["distribution", "packaged", "standalone"]
    assert "standalone" in (error.fix_hint or "")


def test_directory_without_a_manifest_declares_nothing(tmp_path: Path) -> None:
    empty = tmp_path / "no-manifest"
    empty.mkdir()
    assert standalone_release_declared(empty) is False


# ------------------------------------------------------------- repository probe


def _git(root: Path, *args: str) -> None:
    """Run git hermetically: no host config, identity from the environment.

    `HERMETIC_GIT` / `GIT_ENV` are the repository's established pattern
    (tests/substrate/_deployment_repo.py). Both halves matter on CI. A bare
    `git commit` in a temp repository exits 128 on a runner with no global
    `user.name` / `user.email` -- it passed locally only because a developer
    machine has them -- and a host global config can otherwise attach hooks,
    a gpg signing key or a filesystem monitor to a fixture commit.
    """
    subprocess.run(
        [*HERMETIC_GIT, *args],
        cwd=root,
        env={**os.environ, **GIT_ENV},
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _init_repo(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    _git(root, "init", "-q", "-b", "main")
    return root


def test_repository_probe_distinguishes_inside_from_outside(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    assert deployment_is_outside_repository(outside) is True

    inside = _init_repo(tmp_path / "inside")
    nested = inside / "deployments" / "sample"
    nested.mkdir(parents=True)
    assert deployment_is_outside_repository(inside) is False
    assert deployment_is_outside_repository(nested) is False


@pytest.mark.parametrize(
    "variable",
    ["GIT_DIR", "GIT_COMMON_DIR", "GIT_CEILING_DIRECTORIES"],
)
def test_repository_probe_ignores_the_ambient_git_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variable: str,
) -> None:
    """The answer depends on the directory, never on the caller's environment.

    `GIT_DIR` pointing elsewhere makes `git rev-parse --show-toplevel` report
    "not a git repository" from inside a real checkout. Honouring that would
    waive the uncommitted-tree refusal for a deployment that declares
    standalone and really is inside a repository.

    Only the three variables that demonstrably change git's answer here are
    exercised behaviourally; each was mutation-checked by un-stripping it,
    which fails this test. `GIT_CEILING_DIRECTORIES` must name a REAL
    ancestor -- git ignores a ceiling that is not on the path, so pointing it
    at a nonexistent directory would prove nothing.
    `GIT_WORK_TREE` and `GIT_DISCOVERY_ACROSS_FILESYSTEM` are stripped too but
    cannot be made to bite from a temp directory (the latter needs a real
    filesystem boundary), so the test below checks the environment directly
    rather than pretending a passing behavioural arm proved something.
    """
    inside = _init_repo(tmp_path / "inside")
    nested = inside / "deployments" / "sample"
    nested.mkdir(parents=True)
    values = {
        "GIT_DIR": str(tmp_path / "nonexistent" / "x.git"),
        "GIT_COMMON_DIR": str(tmp_path / "nonexistent" / "x.git"),
        # A real ancestor of `nested`, so discovery would stop before `inside`.
        "GIT_CEILING_DIRECTORIES": str(inside / "deployments"),
    }
    monkeypatch.setenv(variable, values[variable])

    assert deployment_is_outside_repository(nested) is False


def test_repository_probe_builds_a_neutral_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every discovery override is removed and the locale is pinned.

    Checked on the environment actually handed to `git`, so it holds for the
    two variables whose effect cannot be reproduced from a temp directory.
    """
    import subprocess as _subprocess

    stripped = (
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_CEILING_DIRECTORIES",
        "GIT_DISCOVERY_ACROSS_FILESYSTEM",
        "GIT_OBJECT_DIRECTORY",
    )
    for variable in stripped:
        monkeypatch.setenv(variable, "set-by-the-caller")
    monkeypatch.setenv("LANG", "de_DE.UTF-8")

    seen: dict[str, str] = {}
    real_run = _subprocess.run

    def _spy(*args: Any, **kwargs: Any) -> Any:
        seen.update(kwargs.get("env") or {})
        return real_run(*args, **kwargs)

    monkeypatch.setattr(
        "okg.substrate.publisher.repository_provenance.subprocess.run", _spy,
    )
    outside = tmp_path / "outside"
    outside.mkdir()
    assert deployment_is_outside_repository(outside) is True

    assert seen, "the probe never invoked git"
    leaked = [variable for variable in stripped if variable in seen]
    assert not leaked, leaked
    assert seen["LC_ALL"] == "C"
    assert seen["LANGUAGE"] == ""


@pytest.mark.parametrize("locale", ["de_DE.UTF-8", "fr_FR.UTF-8"])
def test_repository_probe_reads_git_in_a_non_english_shell(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, locale: str,
) -> None:
    """The "outside" answer is read out of a translated git message.

    Under a German locale git says "Kein Git-Repository". Without a pinned
    locale the probe would fail to recognise it, refuse, and blame a broken
    Git environment -- making the whole standalone feature unusable in a
    non-English shell.
    """
    outside = tmp_path / "outside"
    outside.mkdir()
    for variable in ("LANG", "LC_ALL", "LC_MESSAGES", "LANGUAGE"):
        monkeypatch.setenv(variable, locale)

    assert deployment_is_outside_repository(outside) is True
    assert deployment_is_outside_repository(_init_repo(tmp_path / "in")) is False


def test_repository_probe_refuses_an_inconclusive_answer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """It fails closed: no answer is never read as "outside"."""
    import subprocess as _subprocess

    from okg.substrate.errors import SubstrateError

    def _broken(*_args: Any, **_kwargs: Any) -> Any:
        return _subprocess.CompletedProcess(
            args=[], returncode=128, stdout="",
            stderr="fatal: detected dubious ownership in repository",
        )

    monkeypatch.setattr(
        "okg.substrate.publisher.repository_provenance.subprocess.run", _broken,
    )
    with pytest.raises(SubstrateError) as raised:
        deployment_is_outside_repository(tmp_path)
    assert raised.value.data["exit_code"] == 128
    assert "dubious ownership" in raised.value.data["reason"]


# ------------------------------------------------------------- publisher fork


def _build(
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    sealed: dict[str, str] | None = None,
    packaged: Any | None = None,
    outside_repository: bool = True,
) -> dict[str, Any]:
    """Drive the factory's provenance fork with every other boundary stubbed.

    Returns the kwargs the factory handed to ``Publisher``, so a test can read
    the provenance decision directly.
    """
    from okg.substrate.publisher import factory as publisher_factory
    from okg.substrate.publisher import publisher as publisher_module
    from okg.substrate.publisher.deployment_stack import (
        _clear_deployment_stack_cache,
        build_deployment_publisher_stack,
    )

    _clear_deployment_stack_cache()
    stack = build_deployment_publisher_stack(
        dsn="postgresql://unused",
        deployment_name="sample",
        deployment_dir=deployment_dir,
    )

    captured: dict[str, Any] = {}

    class _Publisher:
        def __init__(self, _dsn: str, _name: str, **kwargs: Any) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(publisher_module, "Publisher", _Publisher)
    monkeypatch.setattr(
        publisher_factory._deployment_stack,
        "build_runtime_catalog_identity",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority.verify_worker_release_authority",
        lambda **_kwargs: sealed,
    )
    monkeypatch.setattr(
        "okg.substrate.distributions.packaged_release.current_verified_release",
        lambda _dir=None: packaged,
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.repository_provenance."
        "deployment_is_outside_repository",
        lambda _dir: outside_repository,
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.repository_provenance."
        "tracking_repository_provenance",
        lambda _dir: _TRACKED,
    )

    publisher_factory.build_deployment_publisher(
        dsn="postgresql://unused",
        deployment_name="sample",
        command="test",
        deployment_dir=deployment_dir,
        stack=stack,
    )
    return captured


# A provenance value only the tracking path can produce, so a test can tell
# "the tracking path ran" from "the declaration short-circuited it".
_TRACKED = RepositoryProvenance.exact(
    core_revision="a" * 40,
    deployment_revision="b" * 40,
    deployment_tree_sha="c" * 40,
)

_STANDALONE = "release:\n  channel: dev\n  kind: standalone\n"


def test_declared_standalone_outside_a_repository_records_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _write_deployment(tmp_path / "dep", release=_STANDALONE)
    kwargs = _build(root, monkeypatch, outside_repository=True)

    assert kwargs["repository_provenance"] == RepositoryProvenance.unavailable()
    # Same shape the packaged branch records, and not a packaged publication.
    assert "packaged_release" not in kwargs


def test_undeclared_deployment_outside_a_repository_still_tracks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the declaration nothing is waived -- Git absence is not consent.

    The tracking path is what raises `deployment release authority is
    unavailable` for a non-Git deployment; reaching it is the refusal.
    """
    root = _write_deployment(tmp_path / "dep", release="release:\n  channel: dev\n")
    kwargs = _build(root, monkeypatch, outside_repository=True)

    assert kwargs["repository_provenance"] == _TRACKED


def test_declared_standalone_inside_a_repository_still_tracks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The declaration cannot silence drift refusals in a real checkout."""
    root = _write_deployment(tmp_path / "dep", release=_STANDALONE)
    kwargs = _build(root, monkeypatch, outside_repository=False)

    assert kwargs["repository_provenance"] == _TRACKED


def test_standalone_with_sealed_worker_authority_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _write_deployment(tmp_path / "dep", release=_STANDALONE)
    sealed = {
        "code_revision": "d" * 40,
        "deployment_revision": "e" * 40,
        "deployment_tree_sha": "f" * 40,
    }

    with pytest.raises(StandaloneReleaseConflict) as raised:
        _build(root, monkeypatch, sealed=sealed)

    error = raised.value
    assert error.code == "standalone_release_conflict"
    assert error.data["conflicting_authority"] == "sealed_worker"
    assert error.data["declared_kind"] == "standalone"


def test_standalone_with_packaged_release_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _write_deployment(tmp_path / "dep", release=_STANDALONE)

    with pytest.raises(StandaloneReleaseConflict) as raised:
        _build(root, monkeypatch, packaged=object())

    error = raised.value
    assert error.code == "standalone_release_conflict"
    assert error.data["conflicting_authority"] == "packaged"


# ------------------------------------------------------------------ fix hints


def test_declared_standalone_outside_git_resolves_a_release_artifact(
    tmp_path: Path,
) -> None:
    """#1962: the serving surfaces resolve instead of refusing.

    `okg rt worker`, `okg mcp-serve` and `okg deployment release` all reach
    `current_release_artifact` through `build_release_report`. Before #1962 it
    had no standalone branch, so each refused with "deployment release
    authority is unavailable" and a deployment that could publish could not be
    served.

    The artifact it returns says WHY it carries no revision. `code_revision`
    alone would be ambiguous -- a packaged artifact carries None too -- so
    `release_kind` records the declaration and
    `records_repository_revision` is the question a consumer asks.
    """
    from okg.substrate.deployment_release import (
        STANDALONE_RELEASE_KIND,
        current_release_artifact,
    )

    root = _write_deployment(tmp_path / "dep", release=_STANDALONE)
    assert deployment_is_outside_repository(root) is True

    artifact = current_release_artifact(
        DeploymentManifest.from_yaml(root / "deployment.yaml")
    )

    assert artifact.code_revision is None
    assert artifact.release_kind == STANDALONE_RELEASE_KIND
    assert artifact.records_repository_revision is False
    # The repository half is waived; the rest is a real measurement, so
    # schema-skew and catalog-drift detection keep working.
    assert artifact.schema_checksum
    assert artifact.packaged_release is None
    # The declaration travels in the serialized report a reader sees.
    assert artifact.to_dict()["release_kind"] == STANDALONE_RELEASE_KIND


def test_declared_standalone_inside_git_still_resolves_a_real_revision(
    tmp_path: Path,
) -> None:
    """The branch is bounded by the DIRECTORY, not by the declaration alone.

    This is the interaction the #1959 review flagged. A deployment that
    declares standalone but sits inside a repository must keep the tracking
    path, because `tracking_repository_provenance` feeds `code_revision`
    straight into `RepositoryProvenance.exact`, which rejects a null. If the
    standalone branch keyed off the declaration alone, that path would break
    on every in-repository standalone deployment.
    """
    from okg.substrate.deployment_release import current_release_artifact
    from okg.substrate.errors import SubstrateError

    root = _init_repo(tmp_path / "repo")
    dep = _write_deployment(root / "deployments" / "sample", release=_STANDALONE)
    assert deployment_is_outside_repository(dep) is False

    manifest = DeploymentManifest.from_yaml(dep / "deployment.yaml")
    try:
        artifact = current_release_artifact(manifest)
    except SubstrateError as error:
        # Whatever the tracking path decides, it must NOT be the
        # missing-repository refusal the standalone branch exists to remove,
        # and it must not have been waived by the declaration.
        assert "deployment release authority is unavailable" not in str(error)
        assert error.data.get("operation") != "resolve_deployment_repository"
    else:
        assert artifact.release_kind is None
        assert artifact.records_repository_revision is True
        assert len(str(artifact.code_revision)) == 40


def test_tracking_provenance_refuses_an_artifact_with_no_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard on the interaction above, from the other side.

    If any caller ever routes a revisionless artifact into the tracking path,
    `RepositoryProvenance.exact` would receive the string "None" and refuse
    with a message about Git object-id formatting that names neither the
    deployment nor the cause. It must refuse with the cause instead.
    """
    from okg.substrate.deployment_release import ReleaseArtifact
    from okg.substrate.errors import SubstrateError
    from okg.substrate.publisher.repository_provenance import (
        tracking_repository_provenance,
    )

    root = _init_repo(tmp_path / "repo")
    dep = _write_deployment(root / "deployments" / "sample")
    _git(root, "add", "-A")
    _git(root, "commit", "-m", "deployment")
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_repo_authorities",
        lambda *_args, **_kwargs: (root, root),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release.current_release_artifact",
        lambda _manifest: ReleaseArtifact.standalone(
            schema_checksum="sha256:deadbeef",
            catalog_fingerprint=None,
            skill_bundle_hash=None,
        ),
    )

    with pytest.raises(SubstrateError) as raised:
        tracking_repository_provenance(dep)

    assert "repository provenance requires a repository revision" in str(raised.value)
    assert raised.value.data["operation"] == "resolve_core_revision"
    assert raised.value.data["release_kind"] == "standalone"
    # Not the object-id formatting message the unguarded path produced.
    assert "lowercase full Git object ids" not in str(raised.value)


def test_refusal_hint_for_a_declaring_deployment_names_the_git_disagreement(
    tmp_path: Path,
) -> None:
    """#1962 narrowed what this refusal can mean for a declaring deployment.

    A declared standalone deployment that really is outside every repository
    is now served, so reaching the deployment-authority refusal while
    declaring standalone means the two Git questions disagreed -- usually a
    stale `GIT_DIR`. The hint must say that, and must still never tell a
    deployment to apply a declaration it already carries.
    """
    from okg.substrate.deployment_release import _deployment_repository_fix_hint

    hint = _deployment_repository_fix_hint(
        "sample",
        {"operation": "resolve_deployment_repository"},
        declared_kind="standalone",
    )

    assert "GIT_DIR" in hint
    assert "declare that in its deployment.yaml" not in hint


def test_refusal_hint_names_the_declaration_when_nothing_declares_it(
    tmp_path: Path,
) -> None:
    from okg.substrate.deployment_release import current_release_artifact
    from okg.substrate.errors import SubstrateError

    root = _write_deployment(
        tmp_path / "dep", release="release:\n  channel: dev\n",
    )

    with pytest.raises(SubstrateError) as raised:
        current_release_artifact(DeploymentManifest.from_yaml(root / "deployment.yaml"))

    assert "release.kind: standalone" in (raised.value.fix_hint or "")
