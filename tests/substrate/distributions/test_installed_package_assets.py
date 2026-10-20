"""An installed distribution package resolves every bundle asset (issue #1793).

The Archi wheel carries its profile bundle, playbooks, schema slices, skill
triggers and chat instructions inside the package. The authoring checkout
holds some of those as symlinks; an installed copy must hold ordinary files
and resolve every reference without the checkout. These tests install an
Archi-shaped fixture wheel into a temporary prefix and drive the verifier
through the pass and every refusal class. A consumer verifies its own real
wheel in its own CI.
"""

from __future__ import annotations

import base64
import hashlib
import io
import shutil
import sys
import zipfile
from pathlib import Path

import pytest

from okg.substrate.distributions import (
    DistributionPackageError,
    InstalledPackageAssets,
    verify_installed_package_assets,
)

PACKAGE = "archi_fixture"
INFO = "archi_fixture-3.0.0a1.dist-info"
BUNDLE = f"{PACKAGE}/bundles/cern-team"

PLAYBOOKS = {
    "README.md": "# Playbooks\n\nIndex of the shipped skills.\n",
    "evidence_retrieval.md": "---\nname: evidence_retrieval\n---\nSearch then cite.\n",
    "site_profile.md": "---\nname: site_profile\n---\nDescribe a site.\n",
    "chat-system-prompt.md": "You answer from graph evidence only.\n",
    "skill-triggers.yaml": (
        "version: 1\n"
        "intents:\n"
        "  evidence:\n"
        "    skill: evidence_retrieval\n"
        "    subtypes: ['*']\n"
        "  site:\n"
        "    skill: site_profile\n"
        "    subtypes: [site]\n"
    ),
}


def _members() -> dict[str, bytes]:
    members: dict[str, str] = {
        f"{PACKAGE}/__init__.py": "# inert fixture\n",
        f"{PACKAGE}/paths.py": "def profiles_dir():\n    return None\n",
        f"{PACKAGE}/schemas/operations.yaml": "id: operations\n",
        f"{BUNDLE}/profile.yaml": (
            "name: cern-team\n"
            "version: 0.1.0\n"
            "classification: reusable\n"
            "modules: modules.yaml\n"
            "source_defaults: source-defaults/\n"
            "invariants: invariants.yaml\n"
            "deployment_defaults: deployment-defaults.yaml\n"
            "skills: skills/\n"
            "schemas: schemas/\n"
        ),
        f"{BUNDLE}/modules.yaml": "modules:\n  - document_starter\n",
        f"{BUNDLE}/invariants.yaml": "invariants: []\n",
        f"{BUNDLE}/deployment-defaults.yaml": (
            "runtime:\n  enabled: false\n"
            "skills_dir: skills\n"
            "skill_triggers: skills/skill-triggers.yaml\n"
            "chat:\n  enabled: true\n  preset:\n"
            "    system_prompt_ref: skills/chat-system-prompt.md\n"
        ),
        f"{BUNDLE}/source-defaults/github_repo.yaml": "source: github_repo\n",
        f"{BUNDLE}/source-defaults/jira.yaml.example": "source: jira\n",
        f"{BUNDLE}/schemas/operations.yaml": "id: operations\n",
        f"{BUNDLE}/schemas/bridges/sources.yaml": "id: bridge\n",
        f"{INFO}/WHEEL": (
            "Wheel-Version: 1.0\nGenerator: hermetic-test\n"
            "Root-Is-Purelib: true\nTag: py3-none-any\n"
        ),
        f"{INFO}/METADATA": "Metadata-Version: 2.1\nName: archi-fixture\nVersion: 3.0.0a1\n",
    }
    for name, text in PLAYBOOKS.items():
        members[f"{BUNDLE}/skills/{name}"] = text
        if name != "chat-system-prompt.md":
            # The package-level copy of the authoring symlink target.
            members[f"{PACKAGE}/skills/{name}"] = text
    return {name: text.encode() for name, text in members.items()}


def _wheel_bytes(members: dict[str, bytes]) -> bytes:
    rows = []
    for name, payload in members.items():
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(payload).digest())
            .rstrip(b"=")
            .decode()
        )
        rows.append(f"{name},sha256={digest},{len(payload)}")
    record_name = f"{INFO}/RECORD"
    members = dict(members)
    members[record_name] = ("\n".join(rows) + f"\n{record_name},,\n").encode()
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as wheel:
        for name, payload in members.items():
            info = zipfile.ZipInfo(name)
            info.create_system = 3
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (0o100644 & 0xFFFF) << 16
            wheel.writestr(info, payload)
    return out.getvalue()


def _install(tmp_path: Path, members: dict[str, bytes] | None = None) -> Path:
    """Install the fixture wheel into a fresh prefix.

    Extraction is the installer here: a pure wheel's installed layout IS its
    archive layout, and dev's ``test_installed_wheel.py`` treats extraction as
    fixture preparation for the same reason. The real pinned wheel below goes
    through ``uv pip install --target`` instead.
    """
    wheel = tmp_path / "fixture.whl"
    wheel.write_bytes(_wheel_bytes(members or _members()))
    prefix = tmp_path / "prefix"
    with zipfile.ZipFile(wheel) as archive:
        archive.extractall(prefix)
    return prefix


def _verify(prefix: Path, **kwargs) -> InstalledPackageAssets:
    return verify_installed_package_assets(prefix, package=PACKAGE, **kwargs)


def test_fixture_install_resolves_every_asset_with_stated_counts(tmp_path: Path):
    prefix = _install(tmp_path)
    authoring = tmp_path / "authoring-checkout"

    report = _verify(prefix, authoring_roots=[authoring])

    assert report.package == PACKAGE
    assert report.profile == "okg.installed-package-assets/v1"
    # 11 package and bundle files, 5 bundle playbook members, 4 package twins;
    # the dist-info directory sits beside the package and is not an asset.
    assert report.files == 20
    assert report.text_files_scanned == 20
    assert len(report.bundles) == 1
    bundle = report.bundles[0]
    assert bundle.bundle == "cern-team"
    assert bundle.profile_refs == 6
    assert bundle.playbooks == 4
    assert bundle.schema_slices == 2
    assert bundle.skill_triggers == 2
    assert bundle.instruction_refs == 3
    # Four playbooks plus the triggers file have package-level twins.
    assert report.shared_playbooks == 4


def test_relocated_copy_resolves_identically(tmp_path: Path):
    """A copied prefix (no installer metadata, no checkout) is the same
    installation: the verifier reads layout, never origin."""
    prefix = _install(tmp_path)
    first = _verify(prefix)
    relocated = tmp_path / "elsewhere" / "copy"
    shutil.copytree(prefix, relocated)
    shutil.rmtree(prefix)

    second = verify_installed_package_assets(relocated, package=PACKAGE)

    assert second.bundles == first.bundles
    assert second.files == first.files
    assert second.installed_root == str(relocated)


def test_dangling_symlink_is_refused_as_a_link(tmp_path: Path):
    """A playbook left as an unresolved authoring symlink is refused for
    being a link, before anything decides whether it is missing."""
    prefix = _install(tmp_path)
    target = prefix / BUNDLE / "skills" / "site_profile.md"
    target.unlink()
    target.symlink_to(tmp_path / "authoring-checkout" / "playbooks" / "site_profile.md")

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix)
    assert error.value.code == "installed_asset_symlink"
    assert error.value.logical_path == "bundles/cern-team/skills/site_profile.md"


def test_resolving_symlink_is_refused_too(tmp_path: Path):
    prefix = _install(tmp_path)
    target = prefix / BUNDLE / "skills" / "site_profile.md"
    target.unlink()
    target.symlink_to(prefix / PACKAGE / "skills" / "site_profile.md")

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix)
    assert error.value.code == "installed_asset_symlink"


def test_missing_playbook_named_by_triggers_is_refused(tmp_path: Path):
    prefix = _install(tmp_path)
    (prefix / BUNDLE / "skills" / "site_profile.md").unlink()

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix)
    assert error.value.code == "installed_asset_missing"
    assert error.value.logical_path == "bundles/cern-team/skills/site_profile.md"
    assert "intents.site.skill" in str(error.value)


@pytest.mark.parametrize(
    "asset",
    ["modules.yaml", "invariants.yaml", "deployment-defaults.yaml", "schemas"],
)
def test_missing_profile_reference_is_refused(tmp_path: Path, asset: str):
    prefix = _install(tmp_path)
    target = prefix / BUNDLE / asset
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix)
    assert error.value.code in {
        "installed_asset_missing", "installed_asset_profile_invalid",
    }


def test_missing_chat_instructions_are_refused(tmp_path: Path):
    prefix = _install(tmp_path)
    (prefix / BUNDLE / "skills" / "chat-system-prompt.md").unlink()

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix)
    assert error.value.code == "installed_asset_missing"
    assert "chat.preset.system_prompt_ref" in str(error.value)


def test_authoring_checkout_path_in_an_asset_is_refused(tmp_path: Path):
    prefix = _install(tmp_path)
    authoring = tmp_path / "authoring-checkout"
    playbook = prefix / BUNDLE / "skills" / "evidence_retrieval.md"
    playbook.write_text(playbook.read_text() + f"\nSee {authoring}/playbooks\n")

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix, authoring_roots=[authoring])
    assert error.value.code == "installed_asset_checkout_path"
    assert error.value.logical_path == "bundles/cern-team/skills/evidence_retrieval.md"
    assert str(authoring) in str(error.value)


def test_home_directory_path_is_refused_without_a_declared_root(tmp_path: Path):
    prefix = _install(tmp_path)
    schema = prefix / BUNDLE / "schemas" / "operations.yaml"
    schema.write_text("id: operations\nsource: /Users/someone/archi/schemas\n")

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix)
    assert error.value.code == "installed_asset_checkout_path"


def test_diverged_playbook_copy_is_refused(tmp_path: Path):
    """The package-level playbook is the materialised authoring symlink; a
    bundle copy that differs means one of them is stale."""
    prefix = _install(tmp_path)
    (prefix / PACKAGE / "skills" / "site_profile.md").write_text("stale\n")

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix)
    assert error.value.code == "installed_asset_playbook_diverged"


def test_profile_reference_escaping_the_bundle_is_refused(tmp_path: Path):
    members = _members()
    members[f"{BUNDLE}/profile.yaml"] = members[f"{BUNDLE}/profile.yaml"].replace(
        b"invariants: invariants.yaml", b"invariants: ../../paths.py",
    )
    prefix = _install(tmp_path, members)

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix)
    assert error.value.code == "installed_asset_outside_bundle"


def test_package_without_bundles_is_refused(tmp_path: Path):
    members = {
        name: data for name, data in _members().items()
        if not name.startswith(f"{PACKAGE}/bundles/")
    }
    prefix = _install(tmp_path, members)

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix)
    assert error.value.code == "installed_asset_missing"


def test_verifier_never_imports_the_package(tmp_path: Path):
    members = _members()
    members[f"{PACKAGE}/__init__.py"] = b"raise RuntimeError('imported by the verifier')\n"
    prefix = _install(tmp_path, members)

    _verify(prefix)

    assert PACKAGE not in sys.modules


@pytest.mark.parametrize("surface_yaml,field", [
    (
        "agent_surface:\n  served: [search]\n  instructions: skills/agent.md\n",
        "agent_surface.instructions",
    ),
    (
        "agent_surface:\n  served: [search]\n  projections:\n    chat:\n"
        "      tools: [search]\n      system_prompt_ref: skills/agent.md\n",
        "agent_surface.projections.chat.system_prompt_ref",
    ),
])
def test_missing_agent_surface_asset_is_refused(
    tmp_path: Path, surface_yaml: str, field: str,
):
    """Issue #2067 moved the instruction paths into `agent_surface`; a bundle
    default naming one must be checked like the top-level keys it replaces."""
    members = _members()
    defaults = f"{BUNDLE}/deployment-defaults.yaml"
    members[defaults] = members[defaults] + surface_yaml.encode()
    prefix = _install(tmp_path, members)

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix)
    assert error.value.code == "installed_asset_missing"
    assert field in str(error.value)


def _members_with_profile_files() -> dict[str, bytes]:
    members = _members()
    profile = members[f"{BUNDLE}/profile.yaml"].decode()
    members[f"{BUNDLE}/profile.yaml"] = (
        profile + "files:\n  - views.sql\n  - docs/mcp-instructions.md\n"
    ).encode()
    members[f"{BUNDLE}/views.sql"] = b"SELECT 1;\n"
    members[f"{BUNDLE}/docs/mcp-instructions.md"] = b"# Instructions\n"
    return members


def test_profile_files_entries_are_verified(tmp_path: Path):
    """Each `files:` entry the profile lists counts as a resolved reference."""
    report = _verify(_install(tmp_path, _members_with_profile_files()))
    assert report.bundles[0].profile_refs == 8


def test_missing_profile_files_entry_is_refused(tmp_path: Path):
    prefix = _install(tmp_path, _members_with_profile_files())
    (prefix / BUNDLE / "views.sql").unlink()

    with pytest.raises(DistributionPackageError) as error:
        _verify(prefix)
    assert error.value.code == "installed_asset_missing"
    assert error.value.logical_path == "bundles/cern-team/views.sql"
    assert "files" in str(error.value)


def test_package_module_file_never_silently_replaces_a_rendered_file():
    """The packaged installer refuses a module file whose path the profile
    already rendered, instead of overwriting the profile's bytes."""
    from okg.substrate.distributions.install_execution import _merge_module_files
    from okg.substrate.errors import SubstrateError

    result = {"views.sql": b"profile bytes\n"}
    _merge_module_files(result, {"modules/archi/ops/schema.yaml": b"id: x\n"})
    assert result["modules/archi/ops/schema.yaml"] == b"id: x\n"

    rendered = {"modules/archi/ops/schema.yaml": b"profile bytes\n"}
    with pytest.raises(SubstrateError) as error:
        _merge_module_files(rendered, {"modules/archi/ops/schema.yaml": b"id: x\n"})
    assert error.value.data["reason"] == "profile_file_overlaps_package_module"
    assert rendered["modules/archi/ops/schema.yaml"] == b"profile bytes\n"
