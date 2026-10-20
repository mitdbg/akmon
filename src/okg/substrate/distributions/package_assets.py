"""Resolve every bundle asset an installed distribution package carries.

An external distribution such as Archi ships its profile bundles inside the
wheel: ``<package>/bundles/<name>/profile.yaml`` plus the playbooks, schema
slices, skill triggers and assistant instructions that profile names. The
authoring checkout may hold some of those as symlinks; the wheel must hold
ordinary files, and an installed or copied package must resolve every one
of them without reaching back into the checkout.

:func:`verify_installed_package_assets` answers exactly that question for an
installed root. It never imports the package: the tree is walked through
directory descriptors with ``O_NOFOLLOW`` (any link, dangling or not, is a
refusal), every ``profile.yaml`` is parsed by the same loader ``okg install``
uses, and every reference the loader or the deployment defaults produce must
land on a regular file or directory inside the bundle. Text assets are also
searched for absolute authoring paths. Byte integrity against a release pin
is a different question and stays with :mod:`.installed_wheel`.
"""

from __future__ import annotations

import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable, Sequence

from okg.substrate import fastyaml
from okg.substrate.catalog.profile import Profile, ProfileError, load_profile

from okg.substrate.agent_surface_paths import agent_surface_path_values

from .package import LIMITS, DistributionPackageError

__all__ = [
    "INSTALLED_PACKAGE_ASSETS_PROFILE",
    "InstalledBundleAssets",
    "InstalledPackageAssets",
    "verify_installed_package_assets",
]

INSTALLED_PACKAGE_ASSETS_PROFILE = "okg.installed-package-assets/v1"

#: Suffixes whose members are read as UTF-8 text and searched for absolute
#: authoring paths. Anything else is opaque: counted, never decoded.
_TEXT_SUFFIXES = frozenset({
    ".md", ".yaml", ".yml", ".py", ".txt", ".example", ".json", ".toml",
})

#: Generic home-directory prefixes. An authoring checkout lives under one of
#: these on every supported platform; a distribution asset never should.
_GENERIC_CHECKOUT_PATH = re.compile(r"(?<![\w./-])/(?:Users|home)/[^\s'\"`)>\]]+")

#: Deployment-defaults keys resolved relative to the bundle directory (with
#: ``chat.preset.system_prompt_ref``, the assistant instructions ``okg chat
#: sync`` materialises). The installer copies ``skills/`` verbatim into the
#: deployment, so a bundle-relative reference is exactly what the installed
#: deployment sees.
_DEFAULTS_PATH_KEYS = ("skills_dir", "skill_triggers", "mcp_instructions")

_PROFILE_REFS = (
    ("modules", "modules_file"),
    ("source_defaults", "source_defaults_dir"),
    ("invariants", "invariants_file"),
    ("dashboards", "dashboards_dir"),
    ("skills", "skills_dir"),
    ("schemas", "schemas_dir"),
    ("deployment_defaults", "deployment_defaults_file"),
)


@dataclass(frozen=True)
class InstalledBundleAssets:
    """What resolved inside one installed bundle."""

    bundle: str
    profile_refs: int
    playbooks: int
    schema_slices: int
    skill_triggers: int
    instruction_refs: int


@dataclass(frozen=True)
class InstalledPackageAssets:
    """Counts stated by :func:`verify_installed_package_assets`."""

    package: str
    installed_root: str
    files: int
    text_files_scanned: int
    #: Files under a bundle's skills directory (playbooks and the triggers
    #: file alike) that also exist at the package level, byte-compared.
    shared_playbooks: int
    bundles: tuple[InstalledBundleAssets, ...]
    profile: str = INSTALLED_PACKAGE_ASSETS_PROFILE


def _refuse(code: str, path: str, reason: str) -> None:
    raise DistributionPackageError(code, path, reason)


def _open_dir(name: str, *, dir_fd: int | None) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        return os.open(name, flags, dir_fd=dir_fd)
    except OSError as exc:
        _refuse("installed_asset_unreadable", name, f"cannot open directory: {exc}")
    raise AssertionError("unreachable")


def _read_regular(parent_fd: int, name: str, logical: str, *, limit: int) -> bytes:
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_fd)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            _refuse("installed_asset_not_regular", logical, "expected a regular file")
        if info.st_size > limit:
            _refuse("installed_asset_limit", logical, "file exceeds supported size")
        data = bytearray()
        while chunk := os.read(fd, min(1024 * 1024, limit + 1 - len(data))):
            data.extend(chunk)
            if len(data) > limit:
                _refuse("installed_asset_limit", logical, "file grew beyond size limit")
        return bytes(data)
    finally:
        os.close(fd)


def _walk(package_fd: int, *, limit_files: int, limit_bytes: int) -> dict[str, bytes]:
    """Every regular file under the package, keyed by posix-relative path.

    Links are refused wherever they appear; a dangling link is therefore
    refused for being a link, never mistaken for a missing file. The caller
    owns ``package_fd``; every directory opened below is closed here.
    """
    files: dict[str, bytes] = {}
    _walk_dir(package_fd, "", files, limit_files=limit_files, limit_bytes=limit_bytes)
    return files


def _walk_dir(
    fd: int,
    prefix: str,
    files: dict[str, bytes],
    *,
    limit_files: int,
    limit_bytes: int,
) -> None:
    with os.scandir(fd) as entries:
        names = sorted(entry.name for entry in entries)
    for name in names:
        logical = f"{prefix}{name}"
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode):
            _refuse(
                "installed_asset_symlink", logical,
                "installed package contains a symbolic link",
            )
        if stat.S_ISDIR(info.st_mode):
            child = _open_dir(name, dir_fd=fd)
            try:
                _walk_dir(
                    child, f"{logical}/", files,
                    limit_files=limit_files, limit_bytes=limit_bytes,
                )
            finally:
                os.close(child)
            continue
        if not stat.S_ISREG(info.st_mode):
            _refuse(
                "installed_asset_not_regular", logical,
                "installed package contains a special file",
            )
        if len(files) >= limit_files:
            _refuse("installed_asset_limit", logical, "too many installed files")
        files[logical] = _read_regular(fd, name, logical, limit=limit_bytes)


def _rel(package_dir: Path, target: Path, *, key: str, bundle: str) -> str:
    """Package-relative logical path of a bundle reference, or a refusal.

    ``load_profile`` resolves references against the bundle directory; one
    that climbs out of it (``../../paths.py``) is refused even when it still
    lands inside the package, because the installer copies bundle assets by
    bundle-relative path.
    """
    bundle_dir = (package_dir / "bundles" / bundle).resolve()
    resolved = target.resolve()
    try:
        resolved.relative_to(bundle_dir)
        return resolved.relative_to(package_dir.resolve()).as_posix()
    except ValueError:
        _refuse(
            "installed_asset_outside_bundle", f"bundles/{bundle}/{key}",
            f"`{key}` resolves outside the installed bundle",
        )
    raise AssertionError("unreachable")


def _present(files: dict[str, bytes], logical: str) -> bool:
    return logical in files or any(name.startswith(logical + "/") for name in files)


def _require(files: dict[str, bytes], logical: str, *, key: str, bundle: str) -> None:
    if not _present(files, logical):
        _refuse(
            "installed_asset_missing", logical,
            f"bundle `{bundle}` references `{key}` but no installed file "
            f"or directory resolves there",
        )


def _load_yaml(files: dict[str, bytes], logical: str) -> object:
    try:
        return fastyaml.safe_load(files[logical].decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - refused with the parse error named
        _refuse("installed_asset_invalid", logical, f"unreadable YAML: {exc}")
    raise AssertionError("unreachable")


def _under(files: dict[str, bytes], logical: str, suffix: str) -> list[str]:
    prefix = logical + "/"
    return sorted(
        name for name in files
        if name.startswith(prefix) and name.endswith(suffix)
    )


def _scan_text(
    files: dict[str, bytes], *, authoring_roots: Sequence[str],
) -> int:
    scanned = 0
    for logical, data in sorted(files.items()):
        if PurePosixPath(logical).suffix not in _TEXT_SUFFIXES:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            _refuse("installed_asset_invalid", logical, "text asset is not UTF-8")
        scanned += 1
        for root in authoring_roots:
            if root and root in text:
                _refuse(
                    "installed_asset_checkout_path", logical,
                    f"asset names the authoring root `{root}`",
                )
        match = _GENERIC_CHECKOUT_PATH.search(text)
        if match:
            _refuse(
                "installed_asset_checkout_path", logical,
                f"asset names an absolute home-directory path `{match.group(0)}`",
            )
    return scanned


def _verify_bundle(
    files: dict[str, bytes],
    *,
    package_dir: Path,
    bundle: str,
) -> tuple[InstalledBundleAssets, int]:
    bundle_dir = package_dir / "bundles" / bundle
    try:
        profile: Profile = load_profile(bundle_dir / "profile.yaml")
    except ProfileError as exc:
        _refuse(
            "installed_asset_profile_invalid", f"bundles/{bundle}/profile.yaml",
            str(exc),
        )
        raise AssertionError("unreachable")

    profile_refs = 0
    resolved: dict[str, str] = {}
    for key, attr in _PROFILE_REFS:
        target = getattr(profile, attr)
        if target is None:
            continue
        logical = _rel(package_dir, target, key=key, bundle=bundle)
        _require(files, logical, key=key, bundle=bundle)
        resolved[key] = logical
        profile_refs += 1

    # `files:` entries are installed byte-for-byte into the deployment, so
    # each must be an ordinary file inside this bundle, never a directory.
    for rel, source in profile.extra_files:
        key = f"files[{rel}]"
        logical = _rel(package_dir, source, key=key, bundle=bundle)
        if logical not in files:
            _refuse(
                "installed_asset_missing", logical,
                f"bundle `{bundle}` lists `{rel}` under `files` but no "
                "installed file resolves there",
            )
        profile_refs += 1

    playbooks = 0
    playbook_names: list[str] = []
    if "skills" in resolved:
        playbook_names = _under(files, resolved["skills"], ".md")
        playbooks = len(playbook_names)
    schema_slices = 0
    if "schemas" in resolved:
        schema_slices = len(_under(files, resolved["schemas"], ".yaml"))

    instruction_refs = 0
    triggers_logical: str | None = None
    skills_logical = resolved.get("skills")
    if "deployment_defaults" in resolved:
        defaults = _load_yaml(files, resolved["deployment_defaults"])
        if not isinstance(defaults, dict):
            _refuse(
                "installed_asset_invalid", resolved["deployment_defaults"],
                "deployment defaults must be a mapping",
            )
            raise AssertionError("unreachable")
        refs: list[tuple[str, object]] = [
            (key, defaults.get(key)) for key in _DEFAULTS_PATH_KEYS
        ]
        # The same paths when a bundle declares them in the agent_surface
        # block that replaces the top-level keys (issue #2067).
        refs.extend(
            (".".join(field_path), value)
            for field_path, value in agent_surface_path_values(defaults)
        )
        chat = defaults.get("chat")
        preset = chat.get("preset") if isinstance(chat, dict) else None
        if isinstance(preset, dict):
            refs.append((
                "chat.preset.system_prompt_ref", preset.get("system_prompt_ref"),
            ))
        for key, value in refs:
            if value is None:
                continue
            if not isinstance(value, str) or not value.strip():
                _refuse(
                    "installed_asset_invalid", resolved["deployment_defaults"],
                    f"`{key}` must be a non-empty relative path",
                )
                raise AssertionError("unreachable")
            logical = _rel(
                package_dir, bundle_dir / value.strip(), key=key, bundle=bundle,
            )
            _require(files, logical, key=key, bundle=bundle)
            instruction_refs += 1
            if key == "skill_triggers":
                triggers_logical = logical
            elif key == "skills_dir":
                skills_logical = logical

    skill_triggers = 0
    if triggers_logical is not None:
        triggers = _load_yaml(files, triggers_logical)
        intents = triggers.get("intents") if isinstance(triggers, dict) else None
        if not isinstance(intents, dict):
            _refuse(
                "installed_asset_invalid", triggers_logical,
                "skill triggers must declare an `intents` mapping",
            )
            raise AssertionError("unreachable")
        for intent, spec in sorted(intents.items()):
            skill = spec.get("skill") if isinstance(spec, dict) else None
            if not isinstance(skill, str) or not skill.strip():
                _refuse(
                    "installed_asset_invalid", triggers_logical,
                    f"intent `{intent}` names no skill",
                )
                raise AssertionError("unreachable")
            if skills_logical is None:
                _refuse(
                    "installed_asset_missing", triggers_logical,
                    f"intent `{intent}` names skill `{skill}` but the bundle "
                    "declares no skills directory",
                )
                raise AssertionError("unreachable")
            _require(
                files, f"{skills_logical}/{skill.strip()}.md",
                key=f"intents.{intent}.skill", bundle=bundle,
            )
            skill_triggers += 1

    # A playbook the package also ships at its top level (the materialised
    # copy of an authoring symlink) must be the same bytes in both places.
    shared = 0
    if "skills" in resolved:
        prefix = resolved["skills"] + "/"
        for logical in _under(files, resolved["skills"], ""):
            twin = f"skills/{logical[len(prefix):]}"
            if twin in files:
                if files[twin] != files[logical]:
                    _refuse(
                        "installed_asset_playbook_diverged", logical,
                        f"bundle playbook differs from the package copy `{twin}`",
                    )
                shared += 1

    return (
        InstalledBundleAssets(
            bundle=bundle,
            profile_refs=profile_refs,
            playbooks=playbooks,
            schema_slices=schema_slices,
            skill_triggers=skill_triggers,
            instruction_refs=instruction_refs,
        ),
        shared,
    )


def verify_installed_package_assets(
    installed_root: Path | str,
    *,
    package: str,
    authoring_roots: Iterable[Path | str] = (),
) -> InstalledPackageAssets:
    """Refuse unless every bundle asset resolves inside the installed package.

    ``installed_root`` is the directory that contains ``<package>/`` (a
    ``site-packages`` or a ``--target`` prefix). ``authoring_roots`` are
    absolute paths of the checkout the package was built from; any text asset
    naming one of them is refused, as is any asset naming a home-directory
    path. Refusals are :class:`DistributionPackageError` with an
    ``installed_asset_*`` code. Nothing in the package is imported.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", package or ""):
        _refuse("installed_asset_package_invalid", str(package), "not an importable name")
    root = Path(installed_root)
    package_dir = root / package
    if not package_dir.is_dir() or package_dir.is_symlink():
        _refuse(
            "installed_asset_missing", package,
            f"no installed package directory under {root}",
        )
    root_fd = _open_dir(str(root), dir_fd=None)
    try:
        package_fd = _open_dir(package, dir_fd=root_fd)
    finally:
        os.close(root_fd)
    try:
        files = _walk(
            package_fd,
            limit_files=int(LIMITS["archive_members"]),
            limit_bytes=int(LIMITS["selected_asset_bytes_each"]),
        )
    finally:
        os.close(package_fd)

    roots = [str(Path(r)) for r in authoring_roots if str(r).strip()]
    text_scanned = _scan_text(files, authoring_roots=roots)

    bundle_names = sorted({
        name.split("/", 2)[1]
        for name in files
        if name.startswith("bundles/") and name.count("/") >= 2
        and name.endswith("/profile.yaml") and name.split("/", 2)[2] == "profile.yaml"
    })
    if not bundle_names:
        _refuse(
            "installed_asset_missing", f"{package}/bundles",
            "installed package carries no `bundles/<name>/profile.yaml`",
        )
    bundles: list[InstalledBundleAssets] = []
    shared_total = 0
    for bundle in bundle_names:
        record, shared = _verify_bundle(files, package_dir=package_dir, bundle=bundle)
        bundles.append(record)
        shared_total += shared

    return InstalledPackageAssets(
        package=package,
        installed_root=str(root),
        files=len(files),
        text_files_scanned=text_scanned,
        shared_playbooks=shared_total,
        bundles=tuple(bundles),
    )
