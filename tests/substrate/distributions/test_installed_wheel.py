"""Pinned archive integrity and explicitly scoped installed-file comparison."""

from __future__ import annotations

import hashlib
from importlib.machinery import PathFinder
import json
import os
import pathlib
import shutil
import sys
import tomllib
import zipfile
from dataclasses import FrozenInstanceError

import pytest

from okg.substrate.distributions.installed_wheel import (
    _directory_names,
    verify_installed_wheel_payload,
)
from okg.substrate.distributions.package import DistributionPackageError
from tests.substrate.distributions.test_distribution_package import _wheel_bytes


INFO = "archi_fixture-3.0.0a1.dist-info"


def _pin(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


@pytest.fixture
def installation(tmp_path):
    wheel = tmp_path / "fixture.whl"
    data = _wheel_bytes()
    wheel.write_bytes(data)
    root = tmp_path / "site-packages"
    # Extraction is only fixture preparation, never the verification path.
    with zipfile.ZipFile(wheel) as archive:
        archive.extractall(root)
    (root / INFO / "INSTALLER").write_text("pip\n")
    (root / INFO / "REQUESTED").write_bytes(b"")
    (root / INFO / "direct_url.json").write_text(
        json.dumps(
            {
                "url": "file:///retained/fixture.whl",
                "archive_info": {"hash": _pin(data)},
            }
        )
    )
    return wheel, _pin(data), root


def _verify(installation):
    wheel, pin, root = installation
    return verify_installed_wheel_payload(
        wheel, expected_wheel_digest=pin, installed_root=root
    )


def test_verifies_owned_files_and_preserves_relocation_and_siblings(
    installation, tmp_path
):
    first = _verify(installation)
    wheel, pin, root = installation
    assert first.distribution == "archi-fixture"
    assert first.version == "3.0.0a1"
    assert len(first.files) == 6
    assert len(first.excluded_installer_metadata) == 4
    assert first.wheel_digest == pin
    assert first.profile == "okg.installed-wheel-files/v1"
    assert not any("RECORD" in item.path for item in first.files)
    with pytest.raises(FrozenInstanceError):
        first.wheel_digest = "modified"
    (root / "unrelated.py").write_text("raise RuntimeError('must not execute')")
    relocated = tmp_path / "relocated"
    shutil.copytree(root, relocated)
    assert (
        verify_installed_wheel_payload(
            wheel, expected_wheel_digest=pin, installed_root=relocated
        )
        == first
    )


@pytest.mark.parametrize("pin", [None, "", "0" * 64, "sha256:" + "A" * 64])
def test_pin_is_required_before_filesystem_access(installation, pin):
    wheel, _, root = installation
    with pytest.raises(DistributionPackageError) as error:
        verify_installed_wheel_payload(
            wheel, expected_wheel_digest=pin, installed_root=root
        )
    assert error.value.code == "installed_wheel_pin_invalid"


def test_substituted_same_version_archive_cannot_supply_its_own_pin(installation):
    wheel, _, _ = installation
    wheel.write_bytes(_wheel_bytes(extra_member="archi/changed.py"))
    with pytest.raises(DistributionPackageError) as error:
        _verify(installation)
    assert error.value.code == "installed_wheel_pin_mismatch"


@pytest.mark.parametrize(
    "mutation", ["modify", "delete", "extra", "cache", "directory", "symlink", "fifo"]
)
def test_installed_payload_tampering_refuses(installation, mutation):
    _, _, root = installation
    target = root / "archi/sources/monit.py"
    if mutation == "modify":
        target.write_bytes(b"x" * target.stat().st_size)
    elif mutation == "delete":
        target.unlink()
    elif mutation == "extra":
        (target.parent / "extra.py").write_text("raise RuntimeError('unverified')")
    elif mutation == "cache":
        (target.parent / "monit.pyc").write_bytes(b"cache")
    elif mutation == "directory":
        (target.parent / "extra").mkdir()
    elif mutation == "symlink":
        target.unlink()
        target.symlink_to(root / "archi/__init__.py")
    else:
        target.unlink()
        os.mkfifo(target)
    with pytest.raises(DistributionPackageError):
        _verify(installation)


@pytest.mark.parametrize(
    "extra", ["archi.py", "archi.so", "ARCHI", "archi_fixture-4.0.dist-info"]
)
def test_conflicting_top_level_names_refuse(installation, extra):
    _, _, root = installation
    if extra == "ARCHI":
        # Exercise case-sensitive and case-insensitive filesystems alike.
        (root / "archi").rename(root / "temporary")
        (root / "temporary").rename(root / extra)
    else:
        (root / extra).write_text("unverified")
    with pytest.raises(DistributionPackageError) as error:
        _verify(installation)
    assert error.value.code == "installed_wheel_extra"


def test_installed_record_never_authorizes_modified_bytes(installation):
    _, _, root = installation
    target = root / "archi/sources/monit.py"
    target.write_bytes(b"changed")
    (root / INFO / "RECORD").write_text("archi/sources/monit.py,sha256:anything,7\n")
    with pytest.raises(DistributionPackageError):
        _verify(installation)


def test_installer_metadata_is_excluded_not_trusted(installation):
    first = _verify(installation)
    _, _, root = installation
    (root / INFO / "RECORD").write_text("../../../bin/archi,,\n")
    (root / INFO / "direct_url.json").write_text(
        json.dumps(
            {
                "url": "file:///not-followed.whl",
                "archive_info": {"hash": "not-authority"},
            }
        )
    )
    assert _verify(installation) == first


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("RECORD", "malformed"),
        ("RECORD", '"unterminated quote'),
        ("REQUESTED", "not-empty"),
        ("INSTALLER", "unexpected\ncontent"),
        ("direct_url.json", "[]"),
        ("direct_url.json", '{"url":"file:///checkout","dir_info":{"editable":true}}'),
    ],
)
def test_invalid_installer_metadata_refuses(installation, name, value):
    _, _, root = installation
    (root / INFO / name).write_text(value)
    with pytest.raises(DistributionPackageError):
        _verify(installation)


@pytest.mark.parametrize(
    "extra",
    [
        "archi_fixture-3.0.0a1.data/purelib/archi/x.py",
        "archi/../escape.py",
        "archi/__pycache__/x.pyc",
        "archi/hook.pth",
        "namespace/module.py",
        INFO + "/INSTALLER",
        "archi/Sources/other.py",
    ],
)
def test_unsupported_or_unsafe_wheel_layout_refuses_before_installed_reads(
    installation, extra
):
    wheel, _, root = installation
    data = _wheel_bytes(extra_member=extra)
    wheel.write_bytes(data)
    with pytest.raises(DistributionPackageError) as error:
        verify_installed_wheel_payload(
            wheel, expected_wheel_digest=_pin(data), installed_root=root / "absent"
        )
    assert error.value.code != "installed_wheel_path_unsafe"


@pytest.mark.parametrize("malformation", ["symlink", "directory", "encrypted"])
def test_reuses_archive_member_safety(installation, malformation):
    wheel, _, root = installation
    data = _wheel_bytes(invalid_member=malformation)
    wheel.write_bytes(data)
    with pytest.raises(DistributionPackageError):
        verify_installed_wheel_payload(
            wheel, expected_wheel_digest=_pin(data), installed_root=root
        )


@pytest.mark.parametrize("target", ["wheel", "root", "package", "ancestor"])
def test_symlink_input_paths_refuse(installation, tmp_path, target):
    wheel, pin, root = installation
    if target == "wheel":
        alias = tmp_path / "alias.whl"
        alias.symlink_to(wheel)
        wheel = alias
    elif target == "root":
        alias = tmp_path / "alias"
        alias.symlink_to(root, target_is_directory=True)
        root = alias
    elif target == "ancestor":
        alias = tmp_path / "alias"
        alias.symlink_to(tmp_path, target_is_directory=True)
        root = alias / root.name
    else:
        package = root / "archi"
        moved = tmp_path / "moved"
        package.rename(moved)
        package.symlink_to(moved, target_is_directory=True)
    with pytest.raises(DistributionPackageError):
        verify_installed_wheel_payload(
            wheel, expected_wheel_digest=pin, installed_root=root
        )


@pytest.mark.parametrize("target", ["wheel", "root"])
def test_parent_components_are_not_normalized_past_symlinks(installation, target):
    wheel, pin, root = installation
    if target == "wheel":
        wheel = wheel.parent / "uninspected" / ".." / wheel.name
    else:
        root = root / "uninspected" / ".."
    with pytest.raises(DistributionPackageError) as error:
        verify_installed_wheel_payload(
            wheel, expected_wheel_digest=pin, installed_root=root
        )
    assert error.value.code == "installed_wheel_path_unsafe"


def test_explicit_root_success_does_not_claim_import_origin(
    installation, tmp_path, monkeypatch
):
    shadow = tmp_path / "shadow"
    shadow.mkdir()
    (shadow / "archi.py").write_text(
        "raise RuntimeError('shadow must not be imported')"
    )
    monkeypatch.syspath_prepend(str(shadow))
    before = sys.modules.get("archi")
    assert PathFinder.find_spec("archi", [str(shadow)]).origin == str(
        shadow / "archi.py"
    )
    result = _verify(installation)
    assert sys.modules.get("archi") is before
    assert result.profile == "okg.installed-wheel-files/v1"
    assert not hasattr(result, "runtime_verified")
    assert not hasattr(result, "core_revision")


def test_wheel_inventory_record_hashes_are_checked(installation):
    wheel, _, root = installation
    data = _wheel_bytes(record_mutation=True)
    wheel.write_bytes(data)
    with pytest.raises(DistributionPackageError) as error:
        verify_installed_wheel_payload(
            wheel, expected_wheel_digest=_pin(data), installed_root=root
        )
    assert error.value.code == "zip_integrity_failed"


def test_directory_limit_stops_iteration_before_collecting_all_entries(
    tmp_path, monkeypatch
):
    for number in range(10):
        (tmp_path / str(number)).write_bytes(b"")
    original = os.scandir
    visited = []

    class ObservedScan:
        def __init__(self, fd):
            self.scan = original(fd)

        def __enter__(self):
            return self

        def __iter__(self):
            for entry in self.scan:
                visited.append(entry.name)
                yield entry

        def __exit__(self, *_):
            self.scan.close()

    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        monkeypatch.setattr(os, "scandir", ObservedScan)
        with pytest.raises(DistributionPackageError) as error:
            _directory_names(fd, limit=2)
        assert error.value.code == "installed_wheel_limit"
        assert len(visited) == 3
    finally:
        os.close(fd)


def test_deep_wheel_refuses_before_recursive_installed_walk(installation):
    wheel, _, root = installation
    data = _wheel_bytes(extra_member="archi/" + "a/" * 120 + "deep.py")
    wheel.write_bytes(data)
    with zipfile.ZipFile(wheel) as archive:
        archive.extractall(root)
    with pytest.raises(DistributionPackageError) as error:
        verify_installed_wheel_payload(
            wheel, expected_wheel_digest=_pin(data), installed_root=root
        )
    assert error.value.code == "installed_wheel_layout_unsupported"


def _rewrite_zip_mode(wheel, member, mode, platform=3, dos_attrs=0):
    import io
    with zipfile.ZipFile(wheel) as original:
        rows = [(item, original.read(item.filename)) for item in original.infolist()]
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        for item, body in rows:
            if item.filename == member:
                item.external_attr = mode << 16 | dos_attrs
                item.create_system = platform
            archive.writestr(item, body)
    wheel.write_bytes(data.getvalue())
    return _pin(data.getvalue())


def test_hatchling_permission_only_generated_metadata_is_supported(installation):
    wheel, _, root = installation
    for member in ("METADATA", "WHEEL", "RECORD"):
        pin = _rewrite_zip_mode(wheel, INFO + "/" + member, 0o644)
    result = verify_installed_wheel_payload(wheel, expected_wheel_digest=pin, installed_root=root)
    assert result.distribution == "archi-fixture"


@pytest.mark.parametrize("member,mode,platform,dos_attrs", [
    ("archi/__init__.py", 0o644, 3, 0),
    (INFO + "/METADATA", 0o755, 3, 0),
    (INFO + "/METADATA", 0o600, 3, 0),
    (INFO + "/METADATA", 0o644, 0, 0),
    (INFO + "/METADATA", 0o644, 3, 0x10),
    (INFO + "/METADATA", 0o120644, 3, 0),
    (INFO + "/METADATA", 0o10644, 3, 0),
    (INFO + "/METADATA", 0, 3, 0),
])
def test_permission_only_exception_does_not_authorize_other_members_or_types(installation, member, mode, platform, dos_attrs):
    wheel, _, root = installation
    pin = _rewrite_zip_mode(wheel, member, mode, platform, dos_attrs)
    with pytest.raises(DistributionPackageError) as refused:
        verify_installed_wheel_payload(wheel, expected_wheel_digest=pin, installed_root=root)
    assert refused.value.code == "zip_member_unsafe"


# --- issue #1922: a used checkout must not build an uninstallable wheel ------


def test_pyc_refusal_names_the_offending_payload_file(tmp_path):
    """The refusal has to say WHICH file, or an operator cannot act on it.

    `verify_installed_wheel_payload` refuses any payload carrying a byte
    cache. Before #1922 it reported only `wheel`, so an install that failed
    because a stale `__pycache__` was swept in gave nothing to delete.
    """
    data = _wheel_bytes(extra_member="archi/__pycache__/x.pyc")
    wheel = tmp_path / "fixture.whl"
    wheel.write_bytes(data)
    with pytest.raises(DistributionPackageError) as refused:
        verify_installed_wheel_payload(
            wheel,
            expected_wheel_digest=_pin(data),
            installed_root=tmp_path / "absent",
        )
    assert refused.value.code == "installed_wheel_layout_unsupported"
    assert refused.value.logical_path == "archi/__pycache__/x.pyc"
    assert "archi/__pycache__/x.pyc" in str(refused.value)


def test_repo_packaging_declares_the_byte_cache_exclusion():
    """`pyproject.toml` must declare the exclusion at all (stdlib only).

    This is the floor: it needs nothing but `tomllib`, so it still runs when
    the setuptools-driven test below skips.
    """
    root = pathlib.Path(__file__).resolve().parents[3]
    with open(root / "pyproject.toml", "rb") as handle:
        config = tomllib.load(handle)
    excluded = config["tool"]["setuptools"]["exclude-package-data"]["*"]
    assert "*.pyc" in excluded
    assert "*.pyo" in excluded
    assert any("__pycache__" in pattern for pattern in excluded)
    assert config["tool"]["setuptools"]["include-package-data"] is True


def test_repo_packaging_excludes_byte_caches_from_package_data():
    """okg's own packaging config must drop `__pycache__` from the wheel.

    `include-package-data = true` plus MANIFEST.in's
    `recursive-include src/okg/substrate/library/profiles/academic-researcher *`
    sweeps every file under that non-package directory into the wheel. On a
    checkout that has imported `academic-researcher/cli/`, that includes a gitignored
    `__pycache__`, and the built wheel is then refused at install. This drives
    setuptools' own exclusion matcher over the real `pyproject.toml`.

    KNOWN BRITTLE: `setuptools.config.pyprojecttoml.apply_configuration` and
    `build_py.exclude_data_files` are setuptools internals, and setuptools is
    unpinned in `pyproject.toml` (83.0.0 in `uv.lock`). A setuptools release
    that moves either one skips this test rather than reddening the lane;
    `test_repo_packaging_declares_the_byte_cache_exclusion` above keeps the
    declaration itself covered, and the real `uv build` in
    `pact/changes/s15-1922-install-defects/evidence/wheel-build-proof.txt` is
    the end-to-end proof. Rewrite this against whatever replaces them.
    """
    pyprojecttoml = pytest.importorskip(
        "setuptools.config.pyprojecttoml",
        reason="setuptools internal apply_configuration moved or was removed",
    )
    apply_configuration = getattr(pyprojecttoml, "apply_configuration", None)
    if apply_configuration is None:
        pytest.skip("setuptools.config.pyprojecttoml.apply_configuration is gone")
    from setuptools.command.build_py import build_py
    from setuptools.dist import Distribution

    if not hasattr(build_py, "exclude_data_files"):
        pytest.skip("setuptools build_py.exclude_data_files is gone")

    root = pathlib.Path(__file__).resolve().parents[3]
    distribution = apply_configuration(
        Distribution({"src_root": str(root)}),
        root / "pyproject.toml",
        ignore_option_errors=False,
    )
    assert distribution.include_package_data is True
    command = build_py(distribution)
    command.finalize_options()

    package = "okg.substrate.library.profiles"
    source_dir = "src/okg/substrate/library/profiles"
    cache = source_dir + "/academic-researcher/cli/__pycache__/install.cpython-312.pyc"
    shipped = [
        source_dir + "/academic-researcher/compose.yaml",
        source_dir + "/academic-researcher/skills/pi/SKILL.md",
        source_dir + "/academic-researcher/initdb/00-extensions.sql",
    ]
    kept = command.exclude_data_files(package, source_dir, [cache, *shipped])

    assert cache not in kept
    assert kept == shipped
