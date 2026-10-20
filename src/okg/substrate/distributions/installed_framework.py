"""Identity of an okg that runs from an installed wheel, not a Git checkout.

A deployment kept in its own Git repository, with okg installed by
`pip install okg-<version>.whl`, could not publish: release authority looked
for the okg Git checkout the running code came from, found none, and refused
(`discover_framework_repository`). External deployments worked around it by
cloning okg's private `.git` into their images (archi-crab-okg
`docker/runtime/Dockerfile:26-58`).

:func:`installed_framework_distribution` answers instead from the installed
distribution itself (Jason's decision "wheel A", 2026-10-04):

* It applies only when the running okg module is a file the `okg`
  distribution's RECORD lists. An editable install, a source checkout, or a
  module the RECORD does not list returns ``None``, and the caller keeps
  today's Git path and its refusals (fail closed, never a fallback).
* Every installed file the RECORD lists with a hash is re-hashed. One that
  differs refuses the run (`installed_framework_modified`): the identity
  recorded for a generation must describe the code that produced it.
* ``payload_digest`` is a sha256 over the sorted ``path<TAB>sha256`` lines of
  that verified RECORD. Every install of one wheel has the same digest, so a
  prod pin can name it.
* A file under ``okg/`` the RECORD does not list, or lists without a sha256,
  refuses the same way. The same holds for every other top-level package the
  wheel installs (``chronos_core/``, ``pact/``): okg imports them, so they are
  part of the payload and of ``payload_digest``.
* Limit, the same one the Git-checkout proof has: byte-compiled caches
  (``__pycache__/*.pyc``) are not checked. Python may run a cache file whose
  header matches its source, so an account that can write into
  site-packages can still change behaviour; that account can already replace
  okg outright.
* ``source_revision`` / ``source_tree`` / ``source_dirty`` come from
  ``okg/_build_info.json``, written by the build (setup.py). They are the
  builder's CLAIM about which commit it built, recorded as a claim and never
  as verified Git authority; a wheel built before this file existed records
  them as null.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any, Mapping

from okg.substrate.errors import SubstrateError

DISTRIBUTION_NAME = "okg"
BUILD_INFO_RELATIVE = "okg/_build_info.json"


class InstalledFrameworkModified(SubstrateError):
    """An installed okg file no longer matches the wheel's RECORD."""

    code = "installed_framework_modified"


@dataclass(frozen=True)
class InstalledFramework:
    name: str
    version: str
    payload_digest: str
    file_count: int
    source_revision: str | None
    source_tree: str | None
    source_dirty: bool | None
    direct_url_commit: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "payload_digest": self.payload_digest,
            "file_count": self.file_count,
            "source_revision": self.source_revision,
            "source_tree": self.source_tree,
            "source_dirty": self.source_dirty,
            "direct_url_commit": self.direct_url_commit,
        }

    @classmethod
    def from_mapping(cls, raw: Any) -> "InstalledFramework":
        """Parse a recorded identity; raise ValueError on any malformed field."""
        if not isinstance(raw, Mapping):
            raise ValueError("installed framework identity must be a mapping")
        digest = raw.get("payload_digest")
        if not isinstance(digest, str) or not _is_sha256_ref(digest):
            raise ValueError("payload_digest must be sha256:<64 hex>")
        for key in ("name", "version"):
            if not isinstance(raw.get(key), str) or not raw.get(key):
                raise ValueError(f"{key} must be a non-empty string")
        count = raw.get("file_count")
        if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
            raise ValueError("file_count must be a positive integer")
        revision = raw.get("source_revision")
        if revision is not None and not _is_git_id(revision):
            raise ValueError("source_revision must be a full Git object id or null")
        tree = raw.get("source_tree")
        if tree is not None and not _is_git_id(tree):
            raise ValueError("source_tree must be a full Git object id or null")
        dirty = raw.get("source_dirty")
        if dirty is not None and not isinstance(dirty, bool):
            raise ValueError("source_dirty must be a boolean or null")
        direct = raw.get("direct_url_commit")
        if direct is not None and not isinstance(direct, str):
            raise ValueError("direct_url_commit must be a string or null")
        return cls(
            name=str(raw["name"]),
            version=str(raw["version"]),
            payload_digest=digest,
            file_count=count,
            source_revision=revision,
            source_tree=tree,
            source_dirty=dirty,
            direct_url_commit=direct,
        )


def _is_git_id(value: Any) -> bool:
    return isinstance(value, str) and len(value) in (40, 64) and all(
        c in "0123456789abcdef" for c in value
    )


def _is_sha256_ref(value: str) -> bool:
    prefix, _, hexdigest = value.partition(":")
    return prefix == "sha256" and len(hexdigest) == 64 and all(
        c in "0123456789abcdef" for c in hexdigest
    )


def _module_path() -> Path:
    # The planner module is the file release authority already keys on
    # (deployment_upgrade.CODE_REPO_MODULE_PATH); use the same anchor.
    from okg.substrate import deployment_upgrade

    return Path(deployment_upgrade.__file__).resolve()


def installed_framework_distribution(
    *,
    module_path: Path | None = None,
    distribution: Any | None = None,
) -> InstalledFramework | None:
    """The verified identity of the installed okg, or None when not a wheel.

    ``module_path`` and ``distribution`` exist for tests.
    """
    anchor = (module_path or _module_path()).resolve()
    try:
        dist = distribution or metadata.distribution(DISTRIBUTION_NAME)
    except metadata.PackageNotFoundError:
        return None
    files = dist.files
    if not files:
        return None
    listed = {}
    for entry in files:
        try:
            listed[Path(str(dist.locate_file(entry))).resolve()] = entry
        except (OSError, ValueError):
            continue
    if anchor not in listed:
        # Editable installs list a .pth or a finder, never the module file
        # itself; a source checkout is not listed at all.
        return None

    packages = _payload_packages(files)
    lines: list[str] = []
    modified: list[dict[str, str]] = []
    for path, entry in sorted(listed.items(), key=lambda item: str(item[1])):
        relative = str(entry).replace("\\", "/")
        if relative.split("/", 1)[0] not in packages or relative.endswith(".pyc"):
            continue
        expected = _record_hash_hex(entry.hash) if entry.hash is not None else None
        if expected is None:
            # A payload file the RECORD does not hash cannot be verified, so
            # it is never silently left out of the digest.
            modified.append({"path": relative, "reason": "RECORD has no sha256 for it"})
            continue
        try:
            observed = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as exc:
            modified.append({"path": relative, "reason": f"unreadable: {exc}"})
            continue
        if observed != expected:
            modified.append({"path": relative, "reason": "hash differs from RECORD"})
            continue
        lines.append(f"{relative}\t{expected}\n")
    # A file under a payload package that the RECORD does not list at all
    # would change behaviour without changing the digest: okg discovers source
    # modules by listing directories. Byte-compiled caches are the one
    # exception; see the module docstring.
    for package in sorted(packages):
        package_root = Path(str(dist.locate_file(package))).resolve()
        if not package_root.is_dir():
            continue
        for candidate in sorted(package_root.rglob("*")):
            if not candidate.is_file() or candidate.suffix == ".pyc":
                continue
            if candidate.resolve() not in listed:
                modified.append({
                    "path": candidate.relative_to(package_root.parent).as_posix(),
                    "reason": "not listed in RECORD",
                })
    if modified:
        raise InstalledFrameworkModified(
            "installed framework is modified",
            data={
                "distribution": DISTRIBUTION_NAME,
                "version": dist.version,
                "modified": modified[:20],
                "modified_count": len(modified),
            },
            fix_hint=(
                "reinstall okg from its wheel (`pip install --force-reinstall "
                "okg-<version>.whl`) so the installed files match the wheel's "
                "RECORD, then re-run the failed command"
            ),
        )
    if not lines:
        return None
    payload_digest = "sha256:" + hashlib.sha256("".join(lines).encode()).hexdigest()
    build_info = _read_build_info(dist)
    return InstalledFramework(
        name=DISTRIBUTION_NAME,
        version=str(dist.version),
        payload_digest=payload_digest,
        file_count=len(lines),
        source_revision=build_info.get("source_revision"),
        source_tree=build_info.get("source_tree"),
        source_dirty=build_info.get("source_dirty"),
        direct_url_commit=_direct_url_commit(dist),
    )


def _payload_packages(files: Any) -> frozenset[str]:
    """Every top-level package or module the wheel's RECORD installs.

    okg's wheel ships `chronos_core/` and `pact/` beside `okg/`, and okg
    imports both, so all of them are verified, not only `okg/`. Metadata
    (`*.dist-info`, `*.data`) and files outside site-packages (`../bin/okg`)
    are not payload.
    """
    packages: set[str] = set()
    for entry in files:
        relative = str(entry).replace("\\", "/")
        head, sep, _ = relative.partition("/")
        if head in ("", "..", ".") or head.endswith((".dist-info", ".data")):
            continue
        if sep or head.endswith(".py"):
            packages.add(head)
    return frozenset(packages)


def _record_hash_hex(file_hash: Any) -> str | None:
    if getattr(file_hash, "mode", None) != "sha256":
        return None
    value = str(getattr(file_hash, "value", ""))
    padded = value + "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(padded).hex()
    except (ValueError, TypeError):
        return None


def _read_build_info(dist: Any) -> dict[str, Any]:
    try:
        raw = json.loads(Path(str(dist.locate_file(BUILD_INFO_RELATIVE))).read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    info: dict[str, Any] = {}
    for key in ("source_revision", "source_tree"):
        info[key] = raw.get(key) if _is_git_id(raw.get(key)) else None
    dirty = raw.get("source_dirty")
    info["source_dirty"] = dirty if isinstance(dirty, bool) else None
    return info


def _direct_url_commit(dist: Any) -> str | None:
    try:
        text = dist.read_text("direct_url.json")
    except (OSError, ValueError):
        return None
    if not text:
        return None
    try:
        raw = json.loads(text)
    except ValueError:
        return None
    commit = (raw.get("vcs_info") or {}).get("commit_id") if isinstance(raw, dict) else None
    return commit if isinstance(commit, str) else None
