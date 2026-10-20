"""Profile-CLI discovery for the substrate CLI dispatcher.

Per `retire-pi-prefix-commands`: the substrate's `cli/main.py` is
profile-agnostic. Profiles that ship CLI verbs put them under
`library/profiles/<name>/cli/__init__.py`. At parser-build time
this loader walks that directory and merges each profile's
`register_subparsers` + `DISPATCH` exports into the substrate's
top-level CLI.

Hyphenated profile directory names (e.g. `academic-researcher`) aren't
valid Python module names, so we load via
`importlib.util.spec_from_file_location` rather than `import`.
"""
from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Protocol


_PROFILE_ROOT = (
    Path(__file__).resolve().parent.parent / "library" / "profiles"
)


class ProfileCliError(RuntimeError):
    """Raised when profile-CLI discovery fails (missing export,
    verb collision, …).
    """


def _profile_dirs() -> list[Path]:
    if not _PROFILE_ROOT.is_dir():
        return []
    return sorted(p for p in _PROFILE_ROOT.iterdir() if p.is_dir())


def _load_profile_cli(profile_dir: Path) -> ModuleType | None:
    """Load `<profile>/cli/__init__.py` if present; return None
    if the profile ships no CLI.

    We pass `submodule_search_locations` so the loaded module is
    treated as a package — that lets the profile's `cli/__init__.py`
    use relative imports (`from . import wizards`) against
    sibling files in the same directory. Without this the
    hyphenated profile dir name would block ordinary Python
    package resolution.
    """
    from okg.substrate.deployments.admission import require_active_deployment

    require_active_deployment(profile_dir)
    cli_dir = profile_dir / "cli"
    init_py = cli_dir / "__init__.py"
    if not init_py.is_file():
        return None
    # Synthetic module name. Use a profile_cli prefix + the
    # underscored profile name so the loaded module is uniquely
    # identifiable in sys.modules during tests + traces.
    safe_name = profile_dir.name.replace("-", "_")
    module_name = f"_okg_profile_cli_{safe_name}"
    spec = importlib.util.spec_from_file_location(
        module_name,
        init_py,
        submodule_search_locations=[str(cli_dir)],
    )
    if spec is None or spec.loader is None:
        raise ProfileCliError(
            f"failed to build import spec for "
            f"{init_py} (profile={profile_dir.name})"
        )
    module = importlib.util.module_from_spec(spec)
    # Register under sys.modules so submodule loads
    # (`from . import wizards`) find their package context.
    import sys
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:  # noqa: BLE001 — module-import failure must roll back the sys.modules registration before re-raising so a retry isn't blocked.
        sys.modules.pop(module_name, None)
        raise
    return module


def discover_profile_clis() -> dict[str, ModuleType]:
    """Return `{profile-name: cli-module}` for every profile that
    ships a `cli/__init__.py`. Profiles without a CLI are omitted.
    """
    out: dict[str, ModuleType] = {}
    from okg.substrate.deployments.admission import archived_deployment

    for profile_dir in _profile_dirs():
        if archived_deployment(profile_dir) is not None:
            continue
        mod = _load_profile_cli(profile_dir)
        if mod is not None:
            out[profile_dir.name] = mod
    return out


def get_profile_cli(name: str) -> ModuleType:
    """Fetch one profile's CLI module by directory name. Raises
    `ProfileCliError` if the profile has no CLI.
    """
    profile_dir = _PROFILE_ROOT / name
    if not profile_dir.is_dir():
        raise ProfileCliError(
            f"profile not found: {profile_dir}"
        )
    mod = _load_profile_cli(profile_dir)
    if mod is None:
        raise ProfileCliError(
            f"profile {name!r} ships no cli/__init__.py"
        )
    return mod


def register_all_profile_clis(
    sub: argparse._SubParsersAction,
    *,
    substrate_verbs: set[str] | None = None,
) -> dict[str, Callable[..., int]]:
    """Discover every profile CLI, register their subparsers on
    `sub`, and return the merged DISPATCH dict.

    `substrate_verbs` (optional): the set of verb names already
    registered by the substrate. A profile that tries to register
    one of these is refused with a `ProfileCliError`.
    """
    dispatch: dict[str, Callable[..., int]] = {}
    seen: dict[str, str] = {}  # verb -> source-profile-name
    substrate_owned = substrate_verbs or set()
    for name, mod in discover_profile_clis().items():
        if not hasattr(mod, "register_subparsers"):
            raise ProfileCliError(
                f"profile {name!r}: cli/__init__.py missing "
                f"`register_subparsers(sub)` export"
            )
        if not hasattr(mod, "DISPATCH"):
            raise ProfileCliError(
                f"profile {name!r}: cli/__init__.py missing "
                f"`DISPATCH` dict export"
            )
        # Validate dispatch keys before registering subparsers so
        # argparse state stays clean on conflict.
        for verb in mod.DISPATCH.keys():
            if verb in substrate_owned:
                raise ProfileCliError(
                    f"profile {name!r} registers verb "
                    f"{verb!r}, which is already shipped by the "
                    f"substrate itself; rename or remove"
                )
            if verb in seen:
                raise ProfileCliError(
                    f"verb {verb!r} registered by both "
                    f"{seen[verb]!r} and {name!r}; profiles "
                    f"cannot collide"
                )
            seen[verb] = name
        mod.register_subparsers(sub)
        dispatch.update(mod.DISPATCH)
    return dispatch


def discover_install_extensions() -> dict[str, "InstallExtension"]:
    """Profiles that can serve the substrate's `install` verb.

    Per PACT `define-newcomer-entry-path`
    (task.entry.promote-install-to-substrate). `install` is the one
    documented way in, so the substrate must own the verb — but
    `retire-pi-prefix-commands` deliberately removed every academic-researcher
    reference from the substrate dispatcher, and naming a profile here
    would put it straight back.

    So the substrate asks: which profiles can be installed? A profile opts
    in by exporting `INSTALL_EXTENSION` from its `cli/__init__.py`. The
    substrate registers one `install` verb, merges each profile's arguments
    onto it, and routes `--profile` to the matching handler — without ever
    naming a profile in substrate code.
    """
    out: dict[str, InstallExtension] = {}
    for name, mod in discover_profile_clis().items():
        ext = getattr(mod, "INSTALL_EXTENSION", None)
        if ext is None:
            continue
        if not callable(getattr(ext, "add_arguments", None)):
            raise ProfileCliError(
                f"profile {name!r}: INSTALL_EXTENSION missing a callable "
                "`add_arguments(parser)`"
            )
        if not callable(getattr(ext, "run", None)):
            raise ProfileCliError(
                f"profile {name!r}: INSTALL_EXTENSION missing a callable "
                "`run(args)`"
            )
        out[name] = ext
    return out


class InstallExtension(Protocol):
    """What a profile exports to be installable via `okg install`."""

    def add_arguments(self, parser: Any) -> None:
        """Contribute this profile's flags to the shared install parser."""

    def run(self, args: Any) -> int:
        """Perform the install. Returns a process exit code."""


__all__ = [
    "InstallExtension",
    "ProfileCliError",
    "discover_install_extensions",
    "discover_profile_clis",
    "get_profile_cli",
    "register_all_profile_clis",
]
