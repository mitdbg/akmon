"""okg running from an installed wheel names itself by that wheel.

Decision "wheel A" (2026-10-04): a deployment in its own Git repository, with
okg installed from a wheel and no okg checkout, publishes with provenance
status `distribution`. The deployment side keeps exact Git ids; the okg side
is the wheel's verified identity, and the commit it claims to be built from
is recorded as a claim.
"""
from __future__ import annotations

import base64
import hashlib
import json
import subprocess
from importlib.metadata import PathDistribution
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.deployment_release import (
    DISTRIBUTION_RELEASE_KIND,
    DatabaseReleaseState,
    ReleaseArtifact,
    ReleaseContract,
    evaluate_release_contract,
)
from okg.substrate.distributions.installed_framework import (
    InstalledFramework,
    InstalledFrameworkModified,
    installed_framework_distribution,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.publisher.repository_provenance import (
    RepositoryProvenance,
    tracking_repository_provenance,
)
from okg.substrate.runtime_dirs import mark_okg_runtime_ancestor, mark_runtime_dir

REVISION = "a" * 40
TREE = "b" * 40


def _record_hash(data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
    return "sha256=" + digest.decode()


def _site(
    tmp_path: Path,
    *,
    build_info: dict | None = None,
    extra_files: dict[str, bytes] | None = None,
) -> tuple[Path, PathDistribution]:
    """A fake site-packages holding an `okg` wheel install with a RECORD."""
    site = tmp_path / "site"
    files = {
        "okg/__init__.py": b"",
        "okg/substrate/__init__.py": b"",
        "okg/substrate/deployment_upgrade.py": b"# planner\n",
        **(extra_files or {}),
    }
    if build_info is not None:
        files["okg/_build_info.json"] = json.dumps(build_info).encode()
    record_lines = []
    for relative, data in files.items():
        path = site / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        record_lines.append(f"{relative},{_record_hash(data)},{len(data)}")
    dist_info = site / "okg-0.1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text("Metadata-Version: 2.1\nName: okg\nVersion: 0.1.0\n")
    record_lines += [
        "okg/substrate/__pycache__/x.cpython-312.pyc,,",
        "okg-0.1.0.dist-info/METADATA,,",
        "okg-0.1.0.dist-info/RECORD,,",
        "../../../bin/okg,,",
    ]
    (dist_info / "RECORD").write_text("\n".join(record_lines) + "\n")
    return site, PathDistribution(dist_info)


def _detect(site: Path, dist: PathDistribution) -> InstalledFramework | None:
    return installed_framework_distribution(
        module_path=site / "okg/substrate/deployment_upgrade.py",
        distribution=dist,
    )


BUILD_INFO = {"source_revision": REVISION, "source_tree": TREE, "source_dirty": False}


def test_a_wheel_install_is_identified_and_its_build_claim_recorded(tmp_path: Path) -> None:
    site, dist = _site(tmp_path, build_info=BUILD_INFO)

    identity = _detect(site, dist)

    assert identity is not None
    assert identity.version == "0.1.0"
    assert identity.payload_digest.startswith("sha256:")
    assert identity.file_count == 4  # the .pyc and dist-info rows are not payload
    assert (identity.source_revision, identity.source_tree, identity.source_dirty) == (
        REVISION, TREE, False,
    )


def test_two_installs_of_one_wheel_have_one_digest(tmp_path: Path) -> None:
    first = _detect(*_site(tmp_path / "a", build_info=BUILD_INFO))
    second = _detect(*_site(tmp_path / "b", build_info=BUILD_INFO))

    assert first is not None and second is not None
    assert first.payload_digest == second.payload_digest


def test_an_edited_installed_file_refuses(tmp_path: Path) -> None:
    site, dist = _site(tmp_path, build_info=BUILD_INFO)
    (site / "okg/substrate/deployment_upgrade.py").write_text("# edited\n")

    with pytest.raises(InstalledFrameworkModified) as exc:
        _detect(site, dist)

    assert exc.value.data["modified"][0]["path"] == "okg/substrate/deployment_upgrade.py"


def test_a_file_the_record_does_not_list_refuses(tmp_path: Path) -> None:
    """An added module changes behaviour without changing the digest:
    okg discovers source modules by listing directories."""
    site, dist = _site(tmp_path, build_info=BUILD_INFO)
    (site / "okg/substrate/planted_adapter.py").write_text("PLANTED = True\n")
    (site / "okg/substrate/__pycache__").mkdir()
    (site / "okg/substrate/__pycache__/x.cpython-312.pyc").write_bytes(b"\0")

    with pytest.raises(InstalledFrameworkModified) as exc:
        _detect(site, dist)

    assert exc.value.data["modified"] == [
        {"path": "okg/substrate/planted_adapter.py", "reason": "not listed in RECORD"},
    ]


SIBLING_PACKAGES = {
    "chronos_core/__init__.py": b"",
    "chronos_core/branching/_common.py": b"# branching\n",
    "pact/__init__.py": b"",
}


def test_every_package_the_wheel_installs_is_payload(tmp_path: Path) -> None:
    """okg's wheel also ships chronos_core/ and pact/, and okg imports them."""
    site, dist = _site(tmp_path, build_info=BUILD_INFO, extra_files=SIBLING_PACKAGES)

    identity = _detect(site, dist)
    okg_only = _detect(*_site(tmp_path / "okg-only", build_info=BUILD_INFO))

    assert identity is not None and okg_only is not None
    assert identity.file_count == 4 + len(SIBLING_PACKAGES)
    assert identity.payload_digest != okg_only.payload_digest


def test_an_edited_sibling_package_file_refuses(tmp_path: Path) -> None:
    site, dist = _site(tmp_path, build_info=BUILD_INFO, extra_files=SIBLING_PACKAGES)
    (site / "chronos_core/branching/_common.py").write_text("# edited\n")

    with pytest.raises(InstalledFrameworkModified) as exc:
        _detect(site, dist)

    assert exc.value.data["modified"] == [
        {"path": "chronos_core/branching/_common.py", "reason": "hash differs from RECORD"},
    ]


def test_a_file_planted_in_a_sibling_package_refuses(tmp_path: Path) -> None:
    site, dist = _site(tmp_path, build_info=BUILD_INFO, extra_files=SIBLING_PACKAGES)
    (site / "pact/planted.py").write_text("PLANTED = True\n")

    with pytest.raises(InstalledFrameworkModified) as exc:
        _detect(site, dist)

    assert exc.value.data["modified"] == [
        {"path": "pact/planted.py", "reason": "not listed in RECORD"},
    ]


def test_a_payload_row_without_a_hash_refuses(tmp_path: Path) -> None:
    site, dist = _site(tmp_path, build_info=BUILD_INFO)
    record = site / "okg-0.1.0.dist-info/RECORD"
    record.write_text(record.read_text().replace(
        "okg/__init__.py,sha256=", "okg/__init__.py,md5=",
    ))

    with pytest.raises(InstalledFrameworkModified) as exc:
        _detect(site, dist)

    assert exc.value.data["modified"][0]["reason"] == "RECORD has no sha256 for it"


def test_a_module_the_record_does_not_list_is_not_a_wheel_install(tmp_path: Path) -> None:
    """An editable install or a source checkout keeps the Git path."""
    site, dist = _site(tmp_path, build_info=BUILD_INFO)
    elsewhere = tmp_path / "checkout/src/okg/substrate/deployment_upgrade.py"
    elsewhere.parent.mkdir(parents=True)
    elsewhere.write_text("# planner\n")

    assert installed_framework_distribution(
        module_path=elsewhere, distribution=dist,
    ) is None


def test_a_wheel_without_build_info_records_an_unknown_build(tmp_path: Path) -> None:
    identity = _detect(*_site(tmp_path))

    assert identity is not None
    assert identity.source_revision is None and identity.source_dirty is None


def test_distribution_provenance_round_trips_and_rejects_malformed_identity(
    tmp_path: Path,
) -> None:
    identity = _detect(*_site(tmp_path, build_info=BUILD_INFO))
    provenance = RepositoryProvenance.distribution(
        core_distribution=identity.to_dict(),
        deployment_revision=REVISION,
        deployment_tree_sha=TREE,
    )

    assert RepositoryProvenance.from_metadata(provenance.to_dict()) == provenance
    assert provenance.core_revision is None
    assert not provenance.matches_exact(provenance)
    broken = {**provenance.to_dict(), "core_distribution": {"payload_digest": "nope"}}
    assert RepositoryProvenance.from_metadata(broken) == RepositoryProvenance.unavailable()


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True,
    ).stdout.strip()


def _deployment_repo(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "deployment-repo"
    deployment = root / "deployments" / "sample"
    deployment.mkdir(parents=True)
    (deployment / "deployment.yaml").write_text("name: sample\n", encoding="utf-8")
    _git(root, "init")
    _git(root, "config", "user.email", "tests@example.test")
    _git(root, "config", "user.name", "Tests")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture")
    return root, deployment


def _distribution_artifact(identity: InstalledFramework) -> ReleaseArtifact:
    return ReleaseArtifact(
        code_revision=None,
        schema_checksum=None,
        catalog_fingerprint=None,
        skill_bundle_hash=None,
        release_kind=DISTRIBUTION_RELEASE_KIND,
        framework_distribution=identity.to_dict(),
    )


def test_tracking_provenance_records_the_wheel_and_the_deployment_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = _detect(*_site(tmp_path, build_info=BUILD_INFO))
    root, deployment = _deployment_repo(tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployments.DeploymentManifest.from_yaml",
        lambda _path: SimpleNamespace(release={}),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release.current_release_artifact",
        lambda _manifest: _distribution_artifact(identity),
    )

    def _no_framework_checkout(*_a, **_kw):  # noqa: ANN202
        raise AssertionError("the distribution path must not look for an okg checkout")

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_repo_authorities",
        _no_framework_checkout,
    )

    provenance = tracking_repository_provenance(deployment)

    assert provenance.status == "distribution"
    assert provenance.deployment_revision == _git(root, "rev-parse", "HEAD")
    assert provenance.deployment_tree_sha == _git(root, "rev-parse", "HEAD^{tree}")
    assert provenance.core_distribution["payload_digest"] == identity.payload_digest

    (root / "untracked.txt").write_text("x")
    with pytest.raises(SubstrateError, match="not committed"):
        tracking_repository_provenance(deployment)


def _prod(pin: ReleaseArtifact, current: ReleaseArtifact) -> list[dict]:
    report = evaluate_release_contract(
        deployment="sample",
        contract=ReleaseContract(channel="prod", pin=pin),
        current=current,
        database=DatabaseReleaseState(
            ledger_exists=True, schema_checksum=None, schema_ok=True,
            pending_migrations=(), blocking_pending_migrations=(),
            checksum_mismatches=(), checksum_compatibilities=(),
        ),
        process_role="worker",
    )
    return report["problems"]


def test_prod_accepts_a_pinned_clean_wheel_and_refuses_the_rest(tmp_path: Path) -> None:
    clean = _detect(*_site(tmp_path / "clean", build_info=BUILD_INFO))
    unknown = _detect(*_site(tmp_path / "unknown"))
    dirty = _detect(*_site(
        tmp_path / "dirty", build_info={**BUILD_INFO, "source_dirty": True},
    ))
    pin = ReleaseArtifact.from_mapping({
        "release_kind": DISTRIBUTION_RELEASE_KIND,
        "framework_payload_digest": clean.payload_digest,
    })

    assert _prod(pin, _distribution_artifact(clean)) == []
    # A wheel with no build info is a different payload as well as unverified.
    assert {p["kind"] for p in _prod(pin, _distribution_artifact(unknown))} >= {
        "distribution_build_unverified",
    }
    assert {p["kind"] for p in _prod(pin, _distribution_artifact(dirty))} >= {
        "distribution_build_unverified", "release_pin_mismatch",
    }
    unpinned = ReleaseArtifact.from_mapping({"release_kind": DISTRIBUTION_RELEASE_KIND})
    assert "distribution_release_pin_missing_digest" in {
        p["kind"] for p in _prod(unpinned, _distribution_artifact(clean))
    }


def test_a_distribution_pin_cannot_also_pin_a_git_revision() -> None:
    with pytest.raises(ValueError, match="cannot also pin"):
        ReleaseArtifact.from_mapping({
            "release_kind": DISTRIBUTION_RELEASE_KIND,
            "framework_payload_digest": "sha256:" + "0" * 64,
            "code_revision": REVISION,
        })
    with pytest.raises(ValueError, match="release_kind: distribution"):
        ReleaseArtifact.from_mapping({"framework_payload_digest": "sha256:" + "0" * 64})


def test_runtime_dirs_ignore_themselves(tmp_path: Path) -> None:
    profiles = mark_runtime_dir(tmp_path / "profiles")
    mark_okg_runtime_ancestor(tmp_path / ".okg" / "up" / "dep-dev.json")
    mark_okg_runtime_ancestor(tmp_path / "chosen" / "state.json")

    assert (profiles / ".gitignore").read_text() == "*\n"
    assert (tmp_path / ".okg" / "up" / ".gitignore").read_text() == "*\n"
    assert not (tmp_path / "chosen").exists()


def _release_manifest(tmp_path: Path, release: dict):
    import yaml

    from okg.substrate.deployments import DeploymentManifest

    deployment = tmp_path / "dep"
    deployment.mkdir()
    (deployment / "schemas").mkdir()
    (deployment / "source_registry.yaml").write_text("sources: []\n")
    (deployment / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "dep", "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "postgres": {"dsn": "postgres://user@host/db"},
        "release": release,
    }))
    return DeploymentManifest.from_yaml(deployment / "deployment.yaml")


@pytest.fixture
def release_stubs(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        lambda _manifest: object(),
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.ownership.catalog_identity_for",
        lambda **kwargs: SimpleNamespace(catalog_fingerprint="catalog"),
    )
    monkeypatch.setattr(
        "okg.substrate.db.management.current_known_migrations",
        lambda: [SimpleNamespace(checksum="schema")],
    )
    monkeypatch.setattr(
        "okg.substrate.distributions.packaged_release.current_verified_release",
        lambda _dir: None,
    )


def test_git_authority_wins_over_a_wheel_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release_stubs,
) -> None:
    """A sealed release installs okg non-editably INSIDE the framework
    repository; it must keep its pinned Git revision (review finding)."""
    from okg.substrate.deployment_release import current_release_artifact

    identity = _detect(*_site(tmp_path / "wheel", build_info=BUILD_INFO))
    monkeypatch.setattr(
        "okg.substrate.distributions.installed_framework.installed_framework_distribution",
        lambda: identity,
    )
    root = tmp_path / "framework"
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_repo_authorities",
        lambda *_a, **_kw: (root, root),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release._git_revision", lambda _root: REVISION,
    )
    planner = root / "src/okg/substrate/deployment_upgrade.py"
    planner.parent.mkdir(parents=True)
    planner.write_text("# planner\n")
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._installed_framework_matches_revision",
        lambda *_a: SimpleNamespace(matched=True),
    )

    artifact = current_release_artifact(
        _release_manifest(tmp_path, {"channel": "dev"}),
    )

    assert artifact.release_kind is None
    assert artifact.code_revision == REVISION


def test_the_wheel_names_okg_when_git_finds_no_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release_stubs,
) -> None:
    from okg.substrate.deployment_release import current_release_artifact
    from okg.substrate.deployment_upgrade import RevisionAuthorityError

    identity = _detect(*_site(tmp_path / "wheel", build_info=BUILD_INFO))
    found = {"identity": identity}
    monkeypatch.setattr(
        "okg.substrate.distributions.installed_framework.installed_framework_distribution",
        lambda: found["identity"],
    )

    def _no_checkout(*_a, **_kw):  # noqa: ANN202
        raise RevisionAuthorityError(
            authority_role="framework", repository=None,
            requested_revision=None, resolved_revision=None,
            operation="discover_framework_repository", reason="no checkout",
        )

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_repo_authorities", _no_checkout,
    )
    manifest = _release_manifest(tmp_path, {"channel": "dev"})

    artifact = current_release_artifact(manifest)
    assert artifact.release_kind == DISTRIBUTION_RELEASE_KIND
    assert artifact.framework_distribution["payload_digest"] == identity.payload_digest

    found["identity"] = None  # an editable install: today's refusal stands
    with pytest.raises(SubstrateError, match="framework release authority is unavailable"):
        current_release_artifact(manifest)
