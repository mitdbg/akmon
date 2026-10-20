from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.deployment_release import ReleaseArtifact
from okg.substrate.publisher.repository_provenance import (
    RepositoryProvenance,
    tracking_repository_provenance,
)


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _repo(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "repo"
    deployment = root / "deployments" / "sample"
    deployment.mkdir(parents=True)
    (deployment / "deployment.yaml").write_text("name: sample\n", encoding="utf-8")
    (root / "outside.txt").write_text("full-tree input\n", encoding="utf-8")
    _git(root, "init")
    _git(root, "config", "user.email", "tests@example.test")
    _git(root, "config", "user.name", "Tests")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture")
    return root, deployment


def _artifact(code_revision: str) -> ReleaseArtifact:
    """The real release artifact, not a duck-typed stand-in.

    #1962 gave `tracking_repository_provenance` a guard that reads
    `ReleaseArtifact.records_repository_revision`. A `SimpleNamespace` stub
    carrying only `code_revision` answered the old call and raised
    `AttributeError` on the new one, so these stubs are the real type.
    """
    return ReleaseArtifact(
        code_revision=code_revision,
        schema_checksum=None,
        catalog_fingerprint=None,
        skill_bundle_hash=None,
    )


def test_tracking_provenance_uses_full_repository_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, deployment = _repo(tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployments.DeploymentManifest.from_yaml",
        lambda _path: SimpleNamespace(release={}),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_repo_authorities",
        lambda *_args, **_kwargs: (root, root),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release.current_release_artifact",
        lambda _manifest: _artifact(_git(root, "rev-parse", "HEAD")),
    )

    provenance = tracking_repository_provenance(deployment)

    assert provenance == RepositoryProvenance.exact(
        core_revision=_git(root, "rev-parse", "HEAD"),
        deployment_revision=_git(root, "rev-parse", "HEAD"),
        deployment_tree_sha=_git(root, "rev-parse", "HEAD^{tree}"),
    )
    assert provenance.deployment_tree_sha != _git(
        root,
        "rev-parse",
        "HEAD:deployments/sample",
    )


@pytest.mark.parametrize("tracked", [False, True])
def test_tracking_provenance_refuses_dirty_path_outside_deployment_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tracked: bool,
) -> None:
    root, deployment = _repo(tmp_path)
    outside = root / "outside-input.yaml"
    outside.write_text("before\n", encoding="utf-8")
    if tracked:
        _git(root, "add", str(outside))
        _git(root, "commit", "-m", "track outside input")
        outside.write_text("after\n", encoding="utf-8")
    monkeypatch.setattr(
        "okg.substrate.deployments.DeploymentManifest.from_yaml",
        lambda _path: SimpleNamespace(release={}),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_repo_authorities",
        lambda *_args, **_kwargs: (root, root),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release.current_release_artifact",
        lambda _manifest: _artifact(_git(root, "rev-parse", "HEAD")),
    )

    with pytest.raises(SubstrateError, match="not committed") as error:
        tracking_repository_provenance(deployment)

    # #1791: the refusal names its remedy and the retry, without `git add -A`.
    hint = error.value.fix_hint or ""
    assert "commit or gitignore every path listed in error.data.paths" in hint
    assert str(root.resolve()) in hint
    assert "okg ingest" in hint
    assert "git add -A" not in hint
    assert error.value.data["repository"] == str(root.resolve())


_CHECKOUT_GITIGNORE = Path(__file__).resolve().parents[3] / ".gitignore"


def test_tracking_provenance_ignores_coverage_worker_data_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3718: pytest-cov with xdist leaves `.coverage.<host>.pid<N>.X<rand>` in
    the checkout until the session combines it. The repository's own
    `.gitignore` must ignore it, or every publish from that checkout is refused
    as uncommitted input. Any other untracked path is still refused."""
    root, deployment = _repo(tmp_path)
    (root / ".gitignore").write_text(
        _CHECKOUT_GITIGNORE.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    _git(root, "add", ".gitignore")
    _git(root, "commit", "-m", "checkout gitignore")
    coverage_data = root / ".coverage.host.pid1.Xabc123"
    coverage_data.write_bytes(b"coverage data")
    monkeypatch.setattr(
        "okg.substrate.deployments.DeploymentManifest.from_yaml",
        lambda _path: SimpleNamespace(release={}),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_repo_authorities",
        lambda *_args, **_kwargs: (root, root),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release.current_release_artifact",
        lambda _manifest: _artifact(_git(root, "rev-parse", "HEAD")),
    )

    assert _git(root, "check-ignore", coverage_data.name) == coverage_data.name
    provenance = tracking_repository_provenance(deployment)
    assert provenance.deployment_revision == _git(root, "rev-parse", "HEAD")

    stray = root / ".coverage-stray"
    stray.write_text("not coverage data\n", encoding="utf-8")
    with pytest.raises(SubstrateError, match="not committed") as error:
        tracking_repository_provenance(deployment)
    assert error.value.data["paths"] == ["?? .coverage-stray"]


@pytest.mark.parametrize("tracked", [False, True])
def test_tracking_provenance_refuses_dirty_split_code_repository(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tracked: bool,
) -> None:
    deployment_root, deployment = _repo(tmp_path / "deployment")
    code_root, _ = _repo(tmp_path / "code")
    code_input = code_root / "runtime-input.py"
    code_input.write_text("before\n", encoding="utf-8")
    if tracked:
        _git(code_root, "add", str(code_input))
        _git(code_root, "commit", "-m", "track runtime input")
        code_input.write_text("after\n", encoding="utf-8")
    monkeypatch.setattr(
        "okg.substrate.deployments.DeploymentManifest.from_yaml",
        lambda _path: SimpleNamespace(release={}),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_repo_authorities",
        lambda *_args, **_kwargs: (code_root, deployment_root),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release.current_release_artifact",
        lambda _manifest: _artifact(_git(code_root, "rev-parse", "HEAD")),
    )

    with pytest.raises(SubstrateError, match="not committed") as error:
        tracking_repository_provenance(deployment)

    assert error.value.data["repository"] == str(code_root.resolve())
    assert str(code_root.resolve()) in (error.value.fix_hint or "")


def test_legacy_and_mismatched_provenance_never_match_exact() -> None:
    expected = RepositoryProvenance.exact(
        core_revision="1" * 40,
        deployment_revision="2" * 40,
        deployment_tree_sha="3" * 40,
    )
    mismatch = RepositoryProvenance.exact(
        core_revision="1" * 40,
        deployment_revision="2" * 40,
        deployment_tree_sha="4" * 40,
    )

    assert RepositoryProvenance.from_metadata(None).status == "unavailable"
    assert not RepositoryProvenance.from_metadata(None).matches_exact(expected)
    assert not mismatch.matches_exact(expected)
    assert expected.matches_exact(expected)


@pytest.mark.parametrize(
    "malformed",
    [
        "z" * 40,
        "A" * 40,
        "1" * 39,
        "1" * 41,
        "1" * 63,
        "1" * 65,
        " " + "1" * 39,
    ],
)
def test_malformed_git_object_ids_are_unavailable_and_never_exact(
    malformed: str,
) -> None:
    metadata = {
        "status": "exact",
        "core_revision": malformed,
        "deployment_revision": "2" * 40,
        "deployment_tree_sha": "3" * 40,
    }

    with pytest.raises(ValueError, match="lowercase full Git object ids"):
        RepositoryProvenance.exact(
            core_revision=malformed,
            deployment_revision="2" * 40,
            deployment_tree_sha="3" * 40,
        )
    projected = RepositoryProvenance.from_metadata(metadata)
    assert projected == RepositoryProvenance.unavailable()
    assert not projected.matches_exact(
        RepositoryProvenance.exact(
            core_revision="1" * 40,
            deployment_revision="2" * 40,
            deployment_tree_sha="3" * 40,
        )
    )


def test_factory_uses_sealed_authority_and_rejects_provenance_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publisher import factory
    from okg.substrate.publisher import publisher as publisher_module

    deployment = tmp_path / "deployment"
    deployment.mkdir()
    stack = SimpleNamespace(
        deployment_name="sample",
        deployment_dir=deployment,
        publisher_kwargs=lambda: {},
    )
    captured: dict[str, object] = {}

    class FakePublisher:
        def __init__(self, _dsn: str, _name: str, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(
        factory._deployment_stack,
        "build_runtime_catalog_identity",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority.verify_worker_release_authority",
        lambda **_kwargs: {
            "code_revision": "1" * 40,
            "deployment_revision": "2" * 40,
            "deployment_tree_sha": "3" * 40,
        },
    )
    monkeypatch.setattr(publisher_module, "Publisher", FakePublisher)

    factory.build_deployment_publisher(
        dsn="postgresql://unused",
        deployment_name="sample",
        command="test",
        deployment_dir=deployment,
        stack=stack,
    )

    assert captured["repository_provenance"] == RepositoryProvenance.exact(
        core_revision="1" * 40,
        deployment_revision="2" * 40,
        deployment_tree_sha="3" * 40,
    )
    with pytest.raises(ValueError, match="cannot replace deployment authority"):
        factory.build_deployment_publisher(
            dsn="postgresql://unused",
            deployment_name="sample",
            command="test",
            deployment_dir=deployment,
            stack=stack,
            publisher_overrides={
                "repository_provenance": RepositoryProvenance.unavailable()
            },
        )
