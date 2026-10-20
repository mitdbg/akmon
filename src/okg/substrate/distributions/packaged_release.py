"""Explicit packaged identity; never a substitute for Git object identifiers.

The host interpreter, dependencies and startup hooks are trusted prerequisites.
Payload verification establishes current files, not a durable execution sandbox.
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from importlib.machinery import BuiltinImporter, FrozenImporter, PathFinder, SourceFileLoader, FileFinder, SOURCE_SUFFIXES, ExtensionFileLoader, EXTENSION_SUFFIXES
from pathlib import Path
from types import ModuleType
from typing import Callable, Mapping

from okg.substrate.errors import SubstrateError
from .installed_wheel import VerifiedWheelPayload, verify_installed_wheel_payload

SCHEMA = "okg.packaged-release/v1"
_FIELDS = frozenset({"schema", "framework_wheel", "consumer_wheel", "configuration", "install_lock", "translation"})


def _refuse(reason: str) -> None:
    raise SubstrateError("packaged release verification refused", data={"reason": reason})


@dataclass(frozen=True)
class PackagedReleaseIdentity:
    framework_wheel: str
    consumer_wheel: str
    configuration: str
    install_lock: str
    translation: str

    def __post_init__(self) -> None:
        for value in (self.framework_wheel, self.consumer_wheel, self.configuration, self.install_lock):
            if not isinstance(value, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
                _refuse("invalid_digest")
        if not isinstance(self.translation, str) or re.fullmatch(r"[a-z][a-z0-9._-]*/v[1-9][0-9]*", self.translation) is None:
            _refuse("invalid_translation")

    def to_dict(self) -> dict[str, str]:
        return {"schema": SCHEMA, **vars(self)}

    @classmethod
    def from_metadata(cls, value: object) -> "PackagedReleaseIdentity":
        if not isinstance(value, Mapping) or set(value) != _FIELDS or value.get("schema") != SCHEMA:
            _refuse("malformed_packaged_metadata")
        return cls(**{key: value[key] for key in _FIELDS - {"schema"}})

    def require_same(self, existing: "PackagedReleaseIdentity") -> None:
        if self != existing:
            _refuse("release_or_configuration_drift")


_FILE_HOOK = FileFinder.path_hook(
    (ExtensionFileLoader, EXTENSION_SUFFIXES), (SourceFileLoader, SOURCE_SUFFIXES),
)


class _SixMovesFinder:
    """Confine the trusted six dependency's lazy importer to its own namespace."""
    def __init__(self, delegate):
        self._delegate = delegate

    def find_spec(self, fullname, path=None, target=None):
        if fullname == "six.moves" or fullname.startswith("six.moves."):
            return self._delegate.find_spec(fullname, path, target)
        return None


_SIX_FINDER: _SixMovesFinder | None = None


def configure_packaged_imports() -> None:
    """Launcher-owned path loading policy, applied before consumer imports.

    Ignore no unexpected hook silently: callers must start with a trusted fresh
    interpreter. Thereafter only these loader objects are supported.
    """
    global _SIX_FINDER
    # Dependencies/interpreter are the explicit trusted bootstrap boundary.
    # Import six before fixing the policy: its lazy moves importer otherwise
    # appears during extraction. Never let that delegate resolve owned packages.
    import six
    if (six.__version__ != "1.17.0" or type(six._importer) is not six._SixMetaPathImporter
            or six._importer.name != "six"):
        _refuse("unsupported_six_dependency")
    _SIX_FINDER = _SixMovesFinder(six._importer)
    sys.meta_path[:] = [BuiltinImporter, FrozenImporter, PathFinder, _SIX_FINDER]
    sys.path_hooks[:] = [_FILE_HOOK]
    sys.path_importer_cache.clear()


def _require_import_policy() -> None:
    def identical(actual, expected):
        return len(actual) == len(expected) and all(a is b for a, b in zip(actual, expected))
    expected_loaders = [
        *((suffix, ExtensionFileLoader) for suffix in EXTENSION_SUFFIXES),
        *((suffix, SourceFileLoader) for suffix in SOURCE_SUFFIXES),
    ]
    def supported_loaders(value):
        if type(value) is not FileFinder or len(value._loaders) != len(expected_loaders):
            return False
        return all(type(item) is tuple and len(item) == 2
                   and type(item[0]) is str and item[0] == suffix and item[1] is loader
                   for item, (suffix, loader) in zip(value._loaders, expected_loaders))
    base = [BuiltinImporter, FrozenImporter, PathFinder]
    supported = identical(sys.meta_path, base)
    if _SIX_FINDER is not None and identical(sys.meta_path, [*base, _SIX_FINDER]):
        six = sys.modules.get("six")
        supported = (six is not None and _SIX_FINDER._delegate is six._importer
                     and type(six._importer) is six._SixMetaPathImporter)
    if (not supported
            or not identical(sys.path_hooks, [_FILE_HOOK])
            or any(value is not None and not supported_loaders(value)
                   for value in sys.path_importer_cache.values())):
        _refuse("unsupported_import_finder")


def verify_loaded_package(
    package: str,
    *,
    wheel_path: Path,
    expected_wheel_digest: str,
    installed_root: Path,
    modules: Mapping[str, ModuleType] | None = None,
) -> VerifiedWheelPayload:
    """Compare the wheel and all already loaded package modules without importing.

    Require an ordinary source-file package rooted in this installation. A
    matching distribution elsewhere on disk does not establish runtime identity.
    Call before external code; subsequent loader entry points must recheck.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", package):
        _refuse("unsupported_package_name")
    _require_import_policy()
    loaded = sys.modules if modules is None else modules
    top = loaded.get(package)
    if top is None:
        _refuse("package_not_loaded")
    payload = verify_installed_wheel_payload(
        wheel_path, expected_wheel_digest=expected_wheel_digest,
        installed_root=installed_root,
    )
    root = installed_root.absolute()
    inventory = {str(root / entry.path) for entry in payload.files}
    expected_package = root / package
    for name, module in tuple(loaded.items()):
        if name != package and not name.startswith(package + "."):
            continue
        spec = getattr(module, "__spec__", None)
        origin = getattr(spec, "origin", None)
        filename = getattr(module, "__file__", None)
        if type(origin) is not str or type(filename) is not str or origin != filename or origin not in inventory:
            _refuse("import_origin_mismatch")
        path = Path(origin)
        named_path = root.joinpath(*name.split("."))
        expected_origins = {named_path / "__init__.py"}
        if name != package:
            expected_origins.add(named_path.with_suffix(".py"))
        if path not in expected_origins or path.suffix != ".py" or not path.is_relative_to(expected_package):
            _refuse("unsupported_module_origin")
        if (type(spec.loader) is not SourceFileLoader
                or any(type(value) is not str for value in (spec.loader.name, spec.name, spec.loader.path))
                or spec.loader.name != name or spec.name != name
                or spec.loader.path != origin):
            _refuse("unsupported_module_loader")
        search = getattr(spec, "submodule_search_locations", None)
        runtime_search = getattr(module, "__path__", None)
        if search is not None:
            expected = [str(path.parent)]
            if (any(type(item) is not str for item in (*search, *(runtime_search or ())))
                    or list(search) != expected or list(runtime_search or ()) != expected):
                _refuse("package_search_path_mismatch")
        elif runtime_search is not None:
            _refuse("package_search_path_mismatch")
    return payload


def verify_prospective_package(
    package: str,
    *,
    wheel_path: Path,
    expected_wheel_digest: str,
    installed_root: Path,
) -> VerifiedWheelPayload:
    """Resolve an unloaded top-level package without executing its initializer.

    Use the runtime search path, not a caller-selected clean root that could hide
    shadowing. This standard path-loader contract excludes custom import hooks.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", package):
        _refuse("unsupported_package_name")
    if package in sys.modules:
        return verify_loaded_package(
            package, wheel_path=wheel_path,
            expected_wheel_digest=expected_wheel_digest, installed_root=installed_root,
        )
    _require_import_policy()
    spec = PathFinder.find_spec(package, sys.path)
    if spec is None or type(spec.loader) is not SourceFileLoader:
        _refuse("unsupported_module_loader")
    expected = str(installed_root.absolute() / package / "__init__.py")
    if (any(type(value) is not str for value in (spec.name, spec.origin, spec.loader.name, spec.loader.path))
            or any(type(value) is not str for value in (spec.submodule_search_locations or ()))
            or spec.name != package or spec.origin != expected
            or spec.loader.name != package or spec.loader.path != expected
            or list(spec.submodule_search_locations or ()) != [str(Path(expected).parent)]):
        _refuse("import_origin_mismatch")
    payload = verify_installed_wheel_payload(
        wheel_path, expected_wheel_digest=expected_wheel_digest,
        installed_root=installed_root,
    )
    if package + "/__init__.py" not in {entry.path for entry in payload.files}:
        _refuse("import_origin_mismatch")
    return payload


def verify_plan_binding(identity: PackagedReleaseIdentity, plan: object, lock: object) -> None:
    """Check transitive plan/bundle identity, not merely two matching strings."""
    from okg.substrate.contracts.deployment_product.lock import (
        canonical_json_bytes, sha256_digest,
    )
    if not isinstance(plan, Mapping) or not isinstance(lock, Mapping):
        _refuse("malformed_install_documents")
    from .install_plan import INSTALL_PLAN_SCHEMA, INSTALL_LOCK_SCHEMA
    if plan.get("schema") != INSTALL_PLAN_SCHEMA or lock.get("schema") != INSTALL_LOCK_SCHEMA:
        _refuse("malformed_install_documents")
    plan_digest = sha256_digest(canonical_json_bytes({k: v for k, v in plan.items() if k != "plan_digest"}))
    lock_digest = sha256_digest(canonical_json_bytes({k: v for k, v in lock.items() if k != "lock_digest"}))
    if (plan.get("plan_digest") != plan_digest or lock.get("plan_digest") != plan_digest
            or lock.get("lock_digest") != lock_digest or identity.install_lock != lock_digest):
        _refuse("install_plan_lock_drift")
    bundle = plan.get("bundle")
    if not isinstance(bundle, Mapping) or bundle.get("bundle_id") != lock.get("bundle_id"):
        _refuse("install_bundle_drift")


@dataclass(frozen=True)
class WheelInput:
    package: str
    wheel_path: Path
    digest: str
    installed_root: Path


@dataclass(frozen=True)
class _VerifiedRelease:
    deployment_dir: Path
    identity: PackagedReleaseIdentity
    revalidate: Callable[[], None]


# Process-local context is established only by the verification scope below.
# Serialized checkpoint metadata cannot activate packaged execution.
from contextlib import contextmanager  # noqa: E402
from contextvars import ContextVar  # noqa: E402

_ACTIVE_RELEASE: ContextVar[_VerifiedRelease | None] = ContextVar("packaged_release", default=None)


def revalidate_active_release() -> _VerifiedRelease | None:
    """Recheck an already admitted context at an extension invocation boundary."""
    active = _ACTIVE_RELEASE.get()
    if active is not None:
        active.revalidate()
    return active


def current_verified_release(deployment_dir: Path | None) -> _VerifiedRelease | None:
    active = _ACTIVE_RELEASE.get()
    if active is not None and deployment_dir is None:
        _refuse("explicit_packaged_deployment_required")
    if active is not None and active.deployment_dir != deployment_dir.absolute():
        _refuse("packaged_deployment_mismatch")
    if active is not None:
        active.revalidate()
    return active


def verified_deployment_directory(name: str | None) -> Path | None:
    """Resolve only the manifest identity admitted by the active package scope."""
    active = _ACTIVE_RELEASE.get()
    if active is None:
        return None
    active.revalidate()
    from okg.substrate.deployments import DeploymentManifest
    manifest = DeploymentManifest.from_yaml(active.deployment_dir / "deployment.yaml")
    if name not in {manifest.name, str(active.deployment_dir), str(active.deployment_dir / "deployment.yaml")}:
        _refuse("packaged_deployment_name_mismatch")
    return active.deployment_dir


@contextmanager
def verified_release_scope(
    *,
    deployment_dir: Path,
    identity: PackagedReleaseIdentity,
    framework: WheelInput,
    consumer: WheelInput,
    configuration_files: tuple[str, ...],
    plan: Mapping[str, object],
    lock: Mapping[str, object],
):
    """Verify caller-pinned inputs before enabling this process's packaged path.

    Configuration is the finalized, explicitly selected immutable input set, not
    mutable runtime state. The installer must retain and recheck that selection
    on every invocation. This scope does not grant filesystem write protection.
    """
    from .installed_wheel import _read_file
    from .package import _open_anchor
    from .package import LIMITS
    from okg.substrate.contracts.deployment_product.lock import canonical_json_bytes, sha256_digest
    import hashlib
    import os

    def revalidate() -> None:
        if (framework.package != "okg" or consumer.package == "okg"
                or framework.digest != identity.framework_wheel
                or consumer.digest != identity.consumer_wheel):
            _refuse("wheel_identity_mismatch")
        verify_plan_binding(identity, plan, lock)
        for item in (framework, consumer):
            verify_prospective_package(
                item.package, wheel_path=item.wheel_path,
                expected_wheel_digest=item.digest, installed_root=item.installed_root,
            )
        if not configuration_files or len(set(configuration_files)) != len(configuration_files):
            _refuse("invalid_configuration_inventory")
        if not {"deployment.yaml", "source_registry.yaml"}.issubset(configuration_files):
            _refuse("incomplete_configuration_inventory")
        # Packaged configuration is an immutable tree; runtime state belongs
        # outside it. No caller-selected subset can hide added configuration.
        observed = set()
        pending = [deployment_dir]
        entries_seen = 0
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    entries_seen += 1
                    if entry.is_symlink():
                        _refuse("invalid_configuration_path")
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        observed.add(str(Path(entry.path).relative_to(deployment_dir)))
                    else:
                        _refuse("invalid_configuration_path")
                    if entries_seen > LIMITS["wheel_entries"]:
                        _refuse("configuration_inventory_limit")
        if observed != set(configuration_files):
            _refuse("incomplete_configuration_inventory")
        inventory = {}
        for name in sorted(configuration_files):
            relative = Path(name)
            if relative.is_absolute() or ".." in relative.parts or not relative.parts:
                _refuse("invalid_configuration_path")
            path = deployment_dir / relative
            parent = _open_anchor(path.parent, directory=True)
            try:
                content = _read_file(parent, path.name, limit=LIMITS["object_bytes_each"])
            finally:
                os.close(parent)
            inventory[name] = "sha256:" + hashlib.sha256(content).hexdigest()
        if sha256_digest(canonical_json_bytes(inventory)) != identity.configuration:
            _refuse("configuration_drift")
    revalidate()
    verified = _VerifiedRelease(deployment_dir.absolute(), identity, revalidate)
    token = _ACTIVE_RELEASE.set(verified)
    try:
        yield verified
    finally:
        _ACTIVE_RELEASE.reset(token)
