"""Profile-driven deployment scaffolder for `okg init --profile`.

Reads a `<profiles_root>/<name>/profile.yaml`, gathers init
answers (supplied via the CLI or interactively), and writes a
working deployment under `deployments/<deployment_name>/`. After
scaffold, the caller invokes:

    1. `okg migrate` (schema migration)
    2. `okg catalog load` (compose + apply catalog)
    3. `okg run --once` (first publish)

This module owns *only* the scaffolding step. The CLI orchestrates
the migrate / catalog-load / first-publish steps via existing
substrate primitives.

The scaffolded deployment.yaml carries a top-level `profile:`
block recording profile name, version, init timestamp, and the
operator's non-secret answers. `okg doctor` (Layer 2A §5)
consults this block for profile-comparison diagnostics; the
runtime ignores it.

Per design.md: variable interpolation is single-pass; no
recursion. Source-default YAML files are copied verbatim into
`source_registry.yaml` after the substitution pass.

Discovery cascade for `discover_profile(name)`:
  1. Explicit `profiles_root` override (tests).
  2. `OKG_PROFILES_DIR` env var.
  3. `./profiles/` relative to cwd (operator-local; typically
     gitignored — operator drops their own profile dirs here).
  4. `src/okg/substrate/library/profiles/` shipped with the
     substrate package itself (the canonical home for
     substrate-provided profiles like `academic-researcher`,
     `workspace-graph`, `_test`).
  5. The fetched-deployment cache under
     `${OKG_CACHE_HOME:-${XDG_CACHE_HOME:-$HOME/.cache}/okg}/
     deployments/<source>/<name>/profile.yaml`, populated by
     `okg fetch-deployments`. This is the single resolver that
     also consults the fetch cache; the previously-divergent
     `resolve_profile` was deleted (per `deployment-fetch-resolver`).
"""
from __future__ import annotations

import logging
import os
import re
import shutil
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml
from okg.substrate import fastyaml

from okg.substrate.catalog.profile import (
    Profile,
    ProfileError,
    ProfileFetchError,
    interpolate,
    load_profile,
    load_profile_modules,
    validate_answer,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.sources.redaction import redact_mapping
from okg.substrate.sources.registry import (
    SourceRegistryUnknownKeyError,
    admit_source_registry_document,
    validate_legacy_entry_keys,
)


logger = logging.getLogger(__name__)


class DuplicateSourceIdError(ProfileError):
    """Two source-default files in one profile declare the same source id.

    `_build_source_registry_yaml` keys the rendered registry by source id.
    Before issue #2070 it merged with `dict.update`, so the file that sorted
    last replaced the other one and the scaffold (`okg init`, the packaged
    `okg install`) dropped a source without a word. There is no override or
    precedence contract between files of one `source-defaults/` directory,
    so a repeated id is ambiguous and refused before anything is written.
    `data` names the id and both files.
    """

    code = "source_default_duplicate_id"


class ProfileRetiredError(ProfileError):
    """The requested profile name was removed from the shipped library.

    Raised by `discover_profile` before any directory is searched, so a
    retired name fails the same way whether or not a stale copy still
    sits in `./profiles/`, `OKG_PROFILES_DIR`, or the fetch cache. There
    are no compatibility aliases: the operator names the replacement
    explicitly. `data` carries `profile_name`, `replacement` (None when
    nothing replaces it) and `reason`.
    """

    code = "profile_retired"


@dataclass(frozen=True)
class RetiredProfile:
    """One retired profile name and what, if anything, replaces it."""

    replacement: Optional[str]
    reason: str


#: Profile names removed from the library, checked by `discover_profile`
#: before its search. Add a row when a profile is removed; never reuse a
#: retired name for a new profile.
RETIRED_PROFILES: Mapping[str, RetiredProfile] = {
    "git-code": RetiredProfile(
        replacement="workspace-graph",
        reason=(
            "git-code shipped the same source defaults as workspace-graph "
            "and was removed on 2026-09-23"
        ),
    ),
    "systems-research": RetiredProfile(
        replacement=None,
        reason=(
            "systems-research and its systems_research ontology module "
            "were removed on 2026-09-23"
        ),
    ),
    "academic-pi": RetiredProfile(
        replacement="academic-researcher",
        reason=(
            "academic-pi was renamed academic-researcher and merged with "
            "cs-researcher on 2026-09-23"
        ),
    ),
    "cs-researcher": RetiredProfile(
        replacement="academic-researcher",
        reason=(
            "cs-researcher was merged into academic-researcher, whose module "
            "set is a superset of it, on 2026-09-23"
        ),
    ),
}


def _refuse_retired_profile(name: str) -> None:
    retired = RETIRED_PROFILES.get(name)
    if retired is None:
        return
    if retired.replacement is not None:
        message = (
            f"profile {name!r} was retired; use "
            f"--profile {retired.replacement} instead"
        )
        fix_hint = f"re-run with --profile {retired.replacement}"
    else:
        message = f"profile {name!r} was retired and has no replacement"
        fix_hint = (
            "choose another profile listed in docs/use/profiles/index.md, "
            "or keep your own copy under a new name in OKG_PROFILES_DIR"
        )
    raise ProfileRetiredError(
        message,
        data={
            "profile_name": name,
            "replacement": retired.replacement,
            "reason": retired.reason,
        },
        fix_hint=fix_hint,
    )


__all__ = [
    "DuplicateSourceIdError",
    "ProfileRetiredError",
    "RETIRED_PROFILES",
    "RetiredProfile",
    "PlannedWrite",
    "ScaffoldPlan",
    "ScaffoldResult",
    "build_plan",
    "discover_profile",
    "planned_writes",
    "redacted_init_answers",
    "scaffold_deployment",
    "refuse_unknown_source_default_keys",
    "source_default_entries",
]


@dataclass(frozen=True)
class ScaffoldPlan:
    """Result of gathering inputs before any file is written.

    `deployment_dir` is where files will land.
    `answers` is the validated map keyed by init-question id.
    `modules` is the resolved module list from the profile.
    `source_defaults` maps source name (filename stem) to the
    interpolated YAML body (still as a string).
    """

    profile: Profile
    deployment_name: str
    deployment_dir: Path
    answers: dict[str, Any]
    modules: list[str]
    source_defaults: dict[str, str]
    schemas: list[tuple[str, bytes]] = field(default_factory=list)
    dashboards: list[tuple[str, str]] = field(default_factory=list)
    invariants_text: Optional[str] = None
    skills: list[tuple[str, str]] = field(default_factory=list)
    deployment_defaults: dict[str, Any] = field(default_factory=dict)
    #: The profile's `files:` entries as (deployment-relative POSIX path,
    #: bytes). Installed verbatim at that path in the deployment.
    files: list[tuple[str, bytes]] = field(default_factory=list)
    #: `strict_v1` when the profile asked for strict source admission, else
    #: None. `_build_source_registry_yaml` renders it as the registry's
    #: top-level `admission_contract:`.
    source_admission_contract: Optional[str] = None


@dataclass(frozen=True)
class PlannedWrite:
    """One file the scaffold will create, with its content and mode.

    `planned_writes` returns these and `scaffold_deployment` writes
    exactly them, so `okg install --dry-run` can report the paths a
    real install would create without a second, drift-prone list of
    "files we think we write".

    Generated configuration uses `text`; byte-verbatim profile assets use
    `data`. Exactly one content representation must be present.
    """

    path: Path
    text: Optional[str] = None
    mode: Optional[int] = None
    data: Optional[bytes] = None

    def __post_init__(self) -> None:
        if (self.text is None) == (self.data is None):
            raise ValueError("PlannedWrite requires exactly one of text or data")


@dataclass(frozen=True)
class ScaffoldResult:
    """What the scaffold wrote.

    `files_written` is the absolute paths created (deployment.yaml,
    source_registry.yaml, schemas/<x>, invariants.yaml,
    dashboards/<x>.yaml).
    Used by the CLI to report stats.
    """
    deployment_dir: Path
    files_written: list[Path]


def discover_profile(
    name: str,
    *,
    profiles_root: Optional[Path] = None,
) -> Profile:
    """Resolve a profile by name.

    This is the single profile resolver. Discovery cascade:
      1. Explicit `profiles_root` override (tests).
      2. `OKG_PROFILES_DIR` env var.
      3. `./profiles/` relative to cwd (operator-local; typically
         gitignored).
      4. `src/okg/substrate/library/profiles/` shipped with the
         substrate package (canonical home).
      5. The fetched-deployment cache populated by
         `okg fetch-deployments`, under
         `<cache>/deployments/<source>/<name>/`. Each per-source
         subdir is one fetched repo (e.g.
         `mitdbg-okg-deployments/`); the cascade iterates them all.

    The selected directory must contain `<name>/profile.yaml`.

    On a complete miss the failure is *loud and structured*: if the
    fetch cache is absent or empty (the operator never ran
    `okg fetch-deployments`, or it failed) the not-found is raised as
    :class:`ProfileFetchError` (`profile_fetch_failed`) naming the
    `external-repo-unconfigured` condition and carrying the fetch
    stderr hint, distinct from a plain `profile_invalid`. No silent
    `None`, no auto-fetch that swallows failures.

    A name in :data:`RETIRED_PROFILES` is refused with
    :class:`ProfileRetiredError` (`profile_retired`) before any search.
    """
    import os

    from okg.substrate.deployments import _cached_deployments_root

    _refuse_retired_profile(name)

    candidates: list[Path] = []
    if profiles_root is not None:
        candidates.append(Path(profiles_root))
    env = os.environ.get("OKG_PROFILES_DIR")
    if env:
        candidates.append(Path(env))
    cwd = Path.cwd() / "profiles"
    if cwd not in candidates:
        candidates.append(cwd)

    # In-tree fallback: src/okg/substrate/library/profiles/ ships
    # alongside this module. The library lives at
    # `okg.substrate.library.profiles`; derive the dir from this
    # file's path.
    here = Path(__file__).resolve()
    library_profiles = (
        here.parent.parent / "library" / "profiles"
    )
    if library_profiles.is_dir() and library_profiles not in candidates:
        candidates.append(library_profiles)

    # Cache cascade step: each fetched repo is a per-source subdir
    # under `<cache>/deployments/`. A profile name resolves against
    # `<source>/<name>/profile.yaml`. Iterate every per-source subdir
    # so a name fetched from any configured repo resolves. Honors
    # OKG_CACHE_HOME / XDG_CACHE_HOME via `_cached_deployments_root`.
    cache_root = _cached_deployments_root()
    cache_searched: list[Path] = []
    cache_populated = False
    if cache_root.is_dir():
        for source_dir in sorted(cache_root.iterdir()):
            if not source_dir.is_dir() or source_dir.name.startswith("."):
                continue
            cache_populated = True
            cache_searched.append(source_dir)
            candidate = source_dir / name
            if (candidate / "profile.yaml").is_file():
                candidates.append(source_dir)

    for root in candidates:
        profile_path = root / name / "profile.yaml"
        if profile_path.is_file():
            from okg.substrate.deployments.admission import require_active_deployment

            require_active_deployment(profile_path.parent)
            return load_profile(profile_path)

    searched_all = [str(c) for c in candidates] + [
        str(c) for c in cache_searched if c not in candidates
    ]

    # Loud distinction: if the fetch cache was never populated, the
    # external reference-deployments repo is unconfigured/unreachable
    # — surface that as the actionable condition rather than a bare
    # "profile not found", which misleads the operator into editing a
    # local profile that was never the issue.
    if not cache_populated:
        raise ProfileFetchError(
            f"profile {name!r} not found locally and the external "
            f"reference-deployments cache is empty: run "
            f"`okg fetch-deployments` first, or check repo access",
            data={
                "profile_name": name,
                "condition": "external-repo-unconfigured",
                "searched": searched_all,
                "cache_root": str(cache_root),
            },
            fix_hint=(
                "run `okg fetch-deployments` to populate "
                f"{cache_root} from the repository you name with --repo "
                "(or OKG_DEPLOYMENTS_REPO), then "
                "re-run with --profile " + name
            ),
        )

    raise ProfileError(
        f"profile {name!r} not found in any of: "
        + ", ".join(searched_all),
        data={
            "profile_name": name,
            "searched": searched_all,
        },
        fix_hint=(
            "create profiles/<name>/profile.yaml under cwd, set "
            "OKG_PROFILES_DIR, run `okg fetch-deployments`, or use "
            "one of the substrate-shipped profile names"
        ),
    )


def installable_library_profiles() -> tuple[Profile, ...]:
    """The profiles shipped with the substrate that a user can install.

    Every directory under ``library/profiles/`` with a ``profile.yaml``,
    except fixture-classified ones (``_test``), sorted by name. This is the
    list ``okg install --help`` prints (issue #3414): it used to name only
    the profiles that ship their own installer, so a reader never learned
    that ``workspace-graph`` -- or the default -- existed. A profile that
    fails to load raises: the help must not quietly drop it.
    """
    from okg.substrate.deployments.admission import archived_deployment

    library_profiles = Path(__file__).resolve().parent.parent / "library" / "profiles"
    if not library_profiles.is_dir():
        return ()
    profiles: list[Profile] = []
    for profile_dir in sorted(library_profiles.iterdir()):
        profile_path = profile_dir / "profile.yaml"
        if not profile_path.is_file() or archived_deployment(profile_dir):
            continue
        profile = load_profile(profile_path)
        if profile.classification == "fixture":
            continue
        profiles.append(profile)
    return tuple(profiles)


def _chmod_quietly(path: Path, mode: int) -> None:
    """Best-effort `chmod`. POSIX permission bits are meaningless on
    some filesystems (Windows, certain network mounts); a failure to
    tighten perms must never abort an install. Logs at debug so the
    rare miss is diagnosable without alarming the operator.
    """
    if os.name != "posix":
        return
    try:
        os.chmod(path, mode)
    except OSError as e:  # pragma: no cover - platform-dependent
        logger.debug("could not chmod %s to %o: %s", path, mode, e)


def planned_writes(plan: ScaffoldPlan) -> list[PlannedWrite]:
    """Every file `scaffold_deployment` would create, in write order.

    Pure: renders each file's content but touches no directory and
    creates no file. `scaffold_deployment` consumes this list, so the
    plan `okg install --dry-run` prints is produced by the same code
    that does the writing and cannot drift from it.
    """
    from okg.substrate.deployments.admission import require_active_deployment
    require_active_deployment(plan.profile.path.parent)
    require_active_deployment(plan.deployment_dir)
    target = plan.deployment_dir
    writes: list[PlannedWrite] = []

    schema_ident = _schema_identifier(plan.deployment_name)
    base_schema_write = PlannedWrite(
        path=target / "schemas" / f"{schema_ident}_base.yaml",
        text=_build_base_schema_yaml(plan.deployment_name),
    )
    writes.append(base_schema_write)
    schema_writes = [base_schema_write]

    # Distribution-owned schemas are copied as bytes, with their profile-
    # relative paths preserved. They are part of this same scaffold write set,
    # so every schema exists before the caller starts the first catalog load.
    for rel, data in plan.schemas:
        schema_write = PlannedWrite(
            path=target / "schemas" / rel,
            data=data,
        )
        writes.append(schema_write)
        schema_writes.append(schema_write)

    # 1. deployment.yaml — modules + profile reference block.
    #    0o600: carries the DSN + password.
    writes.append(PlannedWrite(
        path=target / "deployment.yaml",
        text=_build_deployment_yaml(plan),
        mode=0o600,
    ))

    # 2. source_registry.yaml — interpolated source defaults.
    #    0o600: carries source params / config.
    writes.append(PlannedWrite(
        path=target / "source_registry.yaml",
        text=_build_source_registry_yaml(plan),
        mode=0o600,
    ))

    # 3. invariants.yaml — copied verbatim if profile shipped one.
    if plan.invariants_text is not None:
        writes.append(PlannedWrite(
            path=target / "invariants.yaml",
            text=plan.invariants_text,
        ))

    # 4. dashboards/ — copied verbatim.
    for rel, text in plan.dashboards:
        writes.append(PlannedWrite(
            path=target / "dashboards" / rel, text=text,
        ))

    # 5. skills/ — copied verbatim if the profile shipped any.
    # Each skill is a markdown file (with YAML frontmatter declaring
    # `name:` + `description:`) that an MCP-aware agent loads to
    # compose graph operators for typed lab-management queries.
    # Added per `add-academic-pi-skills`. Operators can edit per
    # deployment without affecting the substrate-library originals.
    for rel, text in plan.skills:
        writes.append(PlannedWrite(
            path=target / "skills" / rel, text=text,
        ))

    # 6. `files:` -- profile-listed deployment files (views.sql,
    # aliases.json, extractors.yaml, MCP instructions, ...), byte-verbatim,
    # at the same relative path. `okg migrate` and the runtime read these
    # from the deployment directory by fixed name or by a deployment.yaml key.
    file_writes = [
        PlannedWrite(path=target / rel, data=data) for rel, data in plan.files
    ]
    writes.extend(file_writes)

    # This portability policy belongs to the new `schemas:` contract. Keep
    # legacy dashboard and skill destinations untouched when the key is absent.
    if plan.schemas:
        _validate_planned_write_paths(schema_writes, target=target)
    # A listed file may not replace, or sit under, any other file the
    # scaffold writes: the generated deployment.yaml and registry must win.
    # Only the listed files are judged; dashboards and skills keep their
    # historical exemption, so adding `files:` cannot change their verdict.
    if file_writes:
        _validate_planned_write_paths(
            writes, target=target, slot="files", only=file_writes,
        )
    return writes


def _portable_destination_key(path: Path) -> tuple[str, ...]:
    """Return collision parts that are stable across common filesystems."""
    return tuple(
        unicodedata.normalize("NFC", part).casefold()
        for part in path.parts
    )


def _colliding_destination(
    key: tuple[str, ...],
    seen: dict[tuple[str, ...], Path],
) -> Optional[Path]:
    """Find an equal or file-versus-descendant destination collision."""
    for previous_key, previous_path in seen.items():
        shared = min(len(key), len(previous_key))
        if key[:shared] == previous_key[:shared]:
            return previous_path
    return None


def _validate_planned_write_paths(
    writes: list[PlannedWrite],
    *,
    target: Path,
    slot: str = "schemas",
    only: Optional[list[PlannedWrite]] = None,
) -> None:
    """Refuse escaping or duplicate destinations before target mutation.

    With `only`, the other writes are recorded as occupied destinations but
    never judged against each other; each write in `only` is judged against
    all of them and against the earlier writes in `only`.
    """
    seen: dict[tuple[str, ...], Path] = {}
    if only is not None:
        judged = {id(write) for write in only}
        for write in writes:
            if id(write) not in judged:
                seen[_portable_destination_key(
                    write.path.relative_to(target)
                )] = write.path
        writes = list(only)
    for write in writes:
        try:
            relative = write.path.relative_to(target)
        except ValueError as exc:
            raise ProfileError(
                f"profile asset destination escapes deployment: {write.path}",
                data={
                    "key": slot,
                    "destination": str(write.path),
                    "reason": "destination_escape",
                },
            ) from exc
        if relative.is_absolute() or ".." in relative.parts:
            raise ProfileError(
                f"profile asset destination escapes deployment: {write.path}",
                data={
                    "key": slot,
                    "destination": str(write.path),
                    "reason": "destination_escape",
                },
            )
        key = _portable_destination_key(relative)
        previous = _colliding_destination(key, seen)
        if previous is not None:
            raise ProfileError(
                "profile assets have an ambiguous overwrite destination: "
                f"{write.path}",
                data={
                    "key": slot,
                    "destination": str(write.path),
                    "collides_with": str(previous),
                    "reason": "destination_collision",
                },
                fix_hint=(
                    "rename the profile schema so every file has a unique "
                    "destination and does not replace the generated base schema"
                    if slot == "schemas" else
                    "rename or move the listed profile file so it does not "
                    "replace a file the scaffold generates"
                ),
            )
        seen[key] = write.path


def scaffold_deployment(
    plan: ScaffoldPlan,
    *,
    force: bool = False,
) -> ScaffoldResult:
    """Write the deployment directory from `plan`.

    Refuses to clobber an existing `deployment_dir` unless
    `force=True`. The CLI couples `force` with `--yes` so the
    operator must double-acknowledge a destructive overwrite.
    """
    from okg.substrate.deployments.admission import require_active_deployment

    require_active_deployment(plan.profile.path.parent)
    require_active_deployment(plan.deployment_dir)
    target = plan.deployment_dir
    # Render and validate the complete write set before inspecting, deleting,
    # or creating the target. An invalid profile must not leave a fresh
    # directory behind or erase an existing deployment under `--force --yes`.
    writes = planned_writes(plan)
    if target.exists() and any(target.iterdir()):
        if not force:
            raise SubstrateError(
                f"deployment directory {target} already exists "
                f"(and is non-empty); init refuses to overwrite",
                data={
                    "deployment_dir": str(target),
                    "deployment_name": plan.deployment_name,
                },
                fix_hint=(
                    "pick a different --deployment-name, or pass "
                    "--force --yes to overwrite the existing files"
                ),
            )
        # `--force` removes the directory tree before scaffolding
        # so a leftover stale file from a previous init doesn't
        # confuse the new state. Drop the dir entirely; the caller
        # has already double-acknowledged with --yes.
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    # The deployment dir holds the Postgres DSN (with password) and
    # source configuration. Restrict it to the owner so another local
    # user / process can't read the credential out of a world-readable
    # (umask-default 0755/0644) scaffold. Per
    # `harden-okg-network-and-secrets`.
    _chmod_quietly(target, 0o700)
    # `DeploymentManifest.from_yaml` requires declared required paths to
    # exist. Profiles compose ontology modules from the substrate library,
    # so a fresh profile scaffold needs no deployment-native subtypes — but
    # the directory must survive transport: git does not track empty
    # directories, so a committed-then-cloned scaffold with a bare
    # `schemas/` resurrects the "points at a missing path" failure of
    # issue #636. Write a minimal deployment-native base schema (the
    # okg_workspace_base.yaml placeholder shape, `classes: {}`) so the
    # directory always carries a file and catalog composition has a base
    # schema to load. Per `fix-init-scaffold-provability`.
    (target / "schemas").mkdir(exist_ok=True)

    written: list[Path] = []
    for write in writes:
        write.path.parent.mkdir(parents=True, exist_ok=True)
        if write.data is not None:
            write.path.write_bytes(write.data)
        else:
            assert write.text is not None
            write.path.write_text(write.text, encoding="utf-8")
        if write.mode is not None:
            _chmod_quietly(write.path, write.mode)
        written.append(write.path)

    return ScaffoldResult(deployment_dir=target, files_written=written)


def _schema_identifier(deployment_name: str) -> str:
    """LinkML-safe identifier for a deployment name (`my-app` -> `my_app`)."""
    ident = re.sub(r"[^0-9A-Za-z]+", "_", deployment_name).strip("_")
    return ident or "deployment"


def _build_base_schema_yaml(deployment_name: str) -> str:
    """Minimal deployment-native base schema (placeholder, `classes: {}`).

    Same shape as deployments/okg-workspace/schemas/okg_workspace_base.yaml:
    profiles compose all real subtypes from library modules; this file
    exists so the scaffold survives git/tar transport (issue #636 residual)
    and so ontology loading always has a deployment base schema.
    """
    ident = _schema_identifier(deployment_name)
    return (
        f"# {deployment_name} has no deployment-native node subtypes; the\n"
        "# opted-in profile modules contribute every subtype. This file\n"
        "# exists so the scaffolded schemas/ directory survives transport\n"
        "# (git does not track empty directories) and ontology loading has\n"
        "# a base schema to load. Generated by `okg init`.\n"
        "\n"
        f"id: https://palimpzest.mit.edu/okg/deployments/{ident}\n"
        f"name: {ident}\n"
        f"title: {deployment_name} deployment ontology (placeholder)\n"
        "description: >-\n"
        f"  {deployment_name} declares no native subtypes; this base schema\n"
        "  only declares the deployment id and prefixes. All actual\n"
        "  subtypes are contributed by the opted-in profile modules.\n"
        "\n"
        "prefixes:\n"
        "  linkml: https://w3id.org/linkml/\n"
        "  okg: https://palimpzest.mit.edu/okg/\n"
        f"  {ident}: https://palimpzest.mit.edu/okg/deployments/{ident}/\n"
        "\n"
        "default_range: string\n"
        "\n"
        "imports:\n"
        "  - linkml:types\n"
        "\n"
        "classes: {}\n"
    )


def _load_profile_schema_assets(profile: Profile) -> list[tuple[str, bytes]]:
    """Load a profile's closed schema tree as byte-verbatim assets."""
    schema_dir = profile.schemas_dir
    if schema_dir is None:
        return []
    if not schema_dir.exists():
        raise ProfileError(
            f"profile {profile.name!r}: schemas directory not found: "
            f"{schema_dir}",
            data={
                "profile_path": str(profile.path),
                "schemas_dir": str(schema_dir),
                "key": "schemas",
                "reason": "missing_directory",
            },
            fix_hint=(
                "create the schemas directory or remove `schemas:` from the "
                "profile"
            ),
        )
    if not schema_dir.is_dir():
        raise ProfileError(
            f"profile {profile.name!r}: schemas path is not a directory: "
            f"{schema_dir}",
            data={
                "profile_path": str(profile.path),
                "schemas_dir": str(schema_dir),
                "key": "schemas",
                "reason": "not_directory",
            },
            fix_hint="point `schemas:` at a real directory inside the profile",
        )

    assets: list[tuple[str, bytes]] = []
    for asset_path in sorted(schema_dir.rglob("*")):
        relative = asset_path.relative_to(schema_dir)
        if asset_path.is_symlink():
            raise ProfileError(
                f"profile {profile.name!r}: schema assets may not be "
                f"symlinks: {asset_path}",
                data={
                    "profile_path": str(profile.path),
                    "schemas_dir": str(schema_dir),
                    "asset_path": str(asset_path),
                    "relative_path": relative.as_posix(),
                    "key": "schemas",
                    "reason": "symlink_entry",
                },
                fix_hint=(
                    "replace the symlink with a regular file or real directory "
                    "inside the profile package"
                ),
            )
        if asset_path.is_dir():
            continue
        if not asset_path.is_file():
            raise ProfileError(
                f"profile {profile.name!r}: unsupported schema asset entry: "
                f"{asset_path}",
                data={
                    "profile_path": str(profile.path),
                    "schemas_dir": str(schema_dir),
                    "asset_path": str(asset_path),
                    "relative_path": relative.as_posix(),
                    "key": "schemas",
                    "reason": "special_entry",
                },
                fix_hint=(
                    "remove the special entry; schema packages may contain "
                    "only real directories and regular files"
                ),
            )
        assets.append((relative.as_posix(), asset_path.read_bytes()))
    return assets


def _validate_schema_asset_destinations(
    *,
    deployment_name: str,
    schemas: list[tuple[str, bytes]],
) -> None:
    """Catch schema collisions while building the plan, before CLI writes."""
    generated = Path(f"{_schema_identifier(deployment_name)}_base.yaml")
    seen = {_portable_destination_key(generated): generated}
    for relative_text, _ in schemas:
        relative = Path(relative_text)
        if relative.is_absolute() or ".." in relative.parts:
            raise ProfileError(
                f"unsafe schema asset destination {relative_text!r}",
                data={
                    "key": "schemas",
                    "relative_path": relative_text,
                    "reason": "destination_escape",
                },
            )
        key = _portable_destination_key(relative)
        previous = _colliding_destination(key, seen)
        if previous is not None:
            raise ProfileError(
                "profile schema collides with another scaffolded schema: "
                f"{relative_text}",
                data={
                    "key": "schemas",
                    "relative_path": relative_text,
                    "collides_with": previous.as_posix(),
                    "reason": "destination_collision",
                },
                fix_hint=(
                    "rename the profile schema so it does not replace the "
                    "generated deployment base schema"
                ),
            )
        seen[key] = relative


def build_plan(
    *,
    profile: Profile,
    deployment_name: str,
    deployments_root: Path,
    raw_answers: dict[str, Any],
) -> ScaffoldPlan:
    """Validate raw answers, load profile-referenced files,
    interpolate source defaults, and return a frozen ScaffoldPlan.

    `raw_answers` is whatever the CLI gathered from flags +
    interactive prompts; it may include extra keys (ignored) or
    skip optional questions (default substituted).
    """
    from okg.substrate.deployments.admission import require_active_deployment
    require_active_deployment(profile.path.parent)
    # Validate every declared question against its type. Required
    # questions without an answer raise ProfileError loudly.
    validated: dict[str, Any] = {}
    for q in profile.init_questions:
        raw = raw_answers.get(q.id)
        validated[q.id] = validate_answer(q, raw)
    # Preserve `deployment_name` as a canonical answer so it can
    # be referenced via ${deployment_name} in source defaults
    # (e.g. default DSN derivation per design.md).
    validated.setdefault("deployment_name", deployment_name)
    # Same idea for `HOME`: per
    # `fix-academic-pi-source-templating-defaults` Phase 1, source
    # defaults like dblp's `cache_root: ${HOME}/.cache/okg/...`
    # need an interpolation-safe home-dir reference. The init
    # answer baking happens once at scaffold time, so caching
    # behavior is reproducible across re-runs from the same user.
    validated.setdefault("HOME", str(Path.home()))

    modules = load_profile_modules(profile)

    schemas = _load_profile_schema_assets(profile)
    _validate_schema_asset_destinations(
        deployment_name=deployment_name,
        schemas=schemas,
    )

    source_defaults: dict[str, str] = {}
    if (
        profile.source_defaults_dir is not None
        and profile.source_defaults_dir.is_dir()
    ):
        for src_path in sorted(profile.source_defaults_dir.glob("*.yaml")):
            raw_text = src_path.read_text(encoding="utf-8")
            interpolated = interpolate(
                raw_text, validated, keep_runtime=True,
            )
            source_defaults[src_path.stem] = interpolated

    invariants_text: Optional[str] = None
    if (
        profile.invariants_file is not None
        and profile.invariants_file.is_file()
    ):
        invariants_text = profile.invariants_file.read_text(
            encoding="utf-8",
        )

    dashboards: list[tuple[str, str]] = []
    if (
        profile.dashboards_dir is not None
        and profile.dashboards_dir.is_dir()
    ):
        for dash_path in sorted(profile.dashboards_dir.rglob("*")):
            if not dash_path.is_file():
                continue
            rel = dash_path.relative_to(profile.dashboards_dir)
            text = dash_path.read_text(encoding="utf-8")
            dashboards.append((str(rel), text))

    skills: list[tuple[str, str]] = []
    if (
        profile.skills_dir is not None
        and profile.skills_dir.is_dir()
    ):
        for skill_path in sorted(profile.skills_dir.rglob("*")):
            if not skill_path.is_file():
                continue
            rel = skill_path.relative_to(profile.skills_dir)
            text = skill_path.read_text(encoding="utf-8")
            skills.append((str(rel), text))

    files: list[tuple[str, bytes]] = []
    for rel, source in profile.extra_files:
        if not source.is_file():
            raise ProfileError(
                f"profile {profile.name!r}: `files` entry {rel!r} is not "
                f"a file: {source}",
                data={
                    "profile_path": str(profile.path),
                    "key": "files",
                    "reference": rel,
                    "source": str(source),
                    "reason": "missing_file",
                },
                fix_hint=(
                    "create the file inside the profile, or remove it "
                    "from `files:`"
                ),
            )
        files.append((rel, source.read_bytes()))

    deployment_defaults: dict[str, Any] = {}
    if profile.deployment_defaults_file is not None:
        if not profile.deployment_defaults_file.is_file():
            raise ProfileError(
                f"profile {profile.name!r}: deployment defaults file "
                f"not found: {profile.deployment_defaults_file}",
                data={
                    "profile_path": str(profile.path),
                    "deployment_defaults_file": str(
                        profile.deployment_defaults_file
                    ),
                    "key": "deployment_defaults",
                },
                fix_hint=(
                    "create the deployment defaults YAML file or remove "
                    "`deployment_defaults:` from the profile"
                ),
            )
        raw_defaults = profile.deployment_defaults_file.read_text(
            encoding="utf-8",
        )
        interpolated_defaults = interpolate(raw_defaults, validated)
        parsed_defaults = fastyaml.safe_load(interpolated_defaults) or {}
        if not isinstance(parsed_defaults, dict):
            raise ProfileError(
                f"{profile.deployment_defaults_file}: top-level must "
                f"be a mapping",
                data={
                    "profile_path": str(profile.path),
                    "deployment_defaults_file": str(
                        profile.deployment_defaults_file
                    ),
                    "key": "deployment_defaults",
                },
                fix_hint=(
                    "make deployment-defaults.yaml a mapping of "
                    "deployment.yaml top-level keys"
                ),
            )
        deployment_defaults = parsed_defaults

    return ScaffoldPlan(
        profile=profile,
        deployment_name=deployment_name,
        deployment_dir=(deployments_root / deployment_name).resolve(),
        answers=validated,
        modules=modules,
        source_defaults=source_defaults,
        schemas=schemas,
        dashboards=dashboards,
        invariants_text=invariants_text,
        skills=skills,
        source_admission_contract=profile.source_admission_contract,
        deployment_defaults=deployment_defaults,
        files=files,
    )


# ---------- internals ----------


def _build_deployment_yaml(plan: ScaffoldPlan) -> str:
    """Render `deployment.yaml` body.

    The runtime ignores the `profile:` block; it carries the
    profile name + version + redacted answers for `okg doctor`'s
    profile-aware diagnostics (Layer 2A §5).
    """
    profile = plan.profile
    now = datetime.now(tz=timezone.utc).isoformat(timespec="seconds")

    # Render answers with simple defaults; respect path_list as a
    # list. The runtime never reads this; we just want the YAML
    # to be human-readable when an operator opens deployment.yaml.
    profile_block = {
        "name": profile.name,
        "version": profile.version,
        "classification": profile.classification,
        "description": profile.description,
        "initialized_at": now,
        "init_answers": redacted_init_answers(profile, plan.answers),
        "components": _profile_component_provenance(profile),
    }

    body: dict[str, Any] = {
        "name": plan.deployment_name,
        "description": (
            profile.description.strip()
            if profile.description
            else f"Generated from profile {profile.name!r}"
        ),
        "profile": profile_block,
        "modules": list(plan.modules),
        "source_registry": "source_registry.yaml",
        # Per dogfood F-8: `okg doctor` (default mode) calls
        # `DeploymentManifest.from_yaml` which requires schema_dir.
        # The substrate's `okg deployment bootstrap` flow already
        # writes this key; the profile_init writer was missing it,
        # so profile-scaffolded deployments crashed on the default
        # doctor run. Convention is `schemas`; `scaffold_deployment`
        # creates the empty dir for profiles that do not ship local
        # schema files.
        "schema_dir": "schemas",
    }
    _merge_deployment_defaults(body, plan.deployment_defaults)
    # If the operator answered a `postgres_dsn` question, the
    # CLI surfaces it in deployment.yaml's postgres.dsn block
    # (matching existing deployments). Profiles that don't declare
    # this question leave the block out; the operator runs
    # `okg migrate --dsn ...` with their own DSN.
    dsn = plan.answers.get("postgres_dsn")
    if dsn:
        body["postgres"] = {"dsn": dsn}

    # Issue #2127. The installer knows where it is putting the deployment, so
    # it -- not the operator with a text editor -- declares the release shape
    # that fits. The two keys are decided SEPARATELY, because only one of
    # them depends on where the deployment sits:
    #
    #   channel: dev     ALWAYS, and not tied to the location probe. Two
    #                    distinct things go wrong without it, and both apply
    #                    inside a working tree as much as outside one:
    #                      - `okg deployment release <name>` loads the
    #                        contract unconditionally and refuses a manifest
    #                        with no `release` mapping -- "deployment
    #                        release.channel is required".
    #                      - `okg mcp-serve` and `okg runtime worker` go
    #                        through `enforce_release_contract_if_declared`,
    #                        which returns early when `release` is absent. A
    #                        deployment with no block is therefore never
    #                        checked for schema skew at all: the gate is not
    #                        passed, it is skipped. Writing the channel turns
    #                        it on.
    #                    `dev` is the no-pin channel, which is the truth for
    #                    a freshly scaffolded deployment; `prod` would
    #                    additionally demand a complete `release.pin`.
    #   kind: standalone ONLY outside every Git repository. Read by the
    #                    publisher, and by `current_release_artifact`, as the
    #                    DECLARATION that no repository authority backs this
    #                    deployment. Declaring it inside a checkout would be
    #                    false and would drop the checkout-tracking path with
    #                    every drift refusal that comes with it.
    #
    # `kind` waives nothing by itself: the standalone branch in
    # `current_release_artifact` re-runs the publisher own probe before it
    # waives Git provenance.
    #
    # Written after `_merge_deployment_defaults`. A profile that ships its own
    # `release` block keeps every key it set -- but a PARTIAL block is
    # COMPLETED rather than kept verbatim, because a block with no channel is
    # a file every contract-loading command refuses. That is the original bug
    # arriving by another route, and `deployment_lint` now flags it too.
    body["release"] = _install_release_block(
        plan.deployment_dir, declared=body.get("release"),
    )

    return yaml.safe_dump(body, sort_keys=False)


def _install_release_block(
    deployment_dir: Path, *, declared: Any = None,
) -> Any:
    """The `release:` block a scaffolded deployment.yaml carries.

    `channel` is unconditional and independent of where the deployment sits.
    Without it `okg deployment release` refuses outright, and the commands
    that enforce the contract lazily -- `okg mcp-serve`, `okg runtime
    worker` -- skip it entirely rather than pass it.

    `kind: standalone` is added only when the target is outside every Git
    working tree, because the declaration is false anywhere else.

    `declared` is whatever `_merge_deployment_defaults` left in the manifest.
    A profile that declared its own block keeps every key it set; only a
    MISSING channel is filled in. Keeping a channel-less block verbatim --
    exactly the `release: {kind: standalone}` shape the old fix hint told
    operators to hand-write -- would ship a manifest that every
    contract-loading command refuses. A declared value that is not a mapping
    is returned untouched: this writer cannot repair it, and
    `deployment_lint` reports it.

    The probe needs an existing directory to run `git rev-parse` in, and the
    scaffold target does not exist yet (nor does the deployments root, under
    `--dry-run`, which promises to create nothing). It therefore runs in the
    nearest EXISTING ancestor. That gives the same answer: repository
    membership is decided by the nearest ancestor holding a `.git`, and no
    `.git` can live inside a directory that does not exist.

    A Git environment that cannot answer the question raises out of
    :func:`deployment_is_outside_repository` rather than defaulting either
    way. The install path already needs a working `git` -- its own first
    publish resolves repository provenance through the same probe -- so a
    broken environment is reported here instead of being written into the
    manifest as a guess.
    """
    from okg.substrate.deployment_release import STANDALONE_RELEASE_KIND

    if declared is not None and not isinstance(declared, dict):
        return declared
    if declared:
        completed: dict[str, Any] = {"channel": "dev"}
        completed.update(declared)
        if not completed.get("channel"):
            completed["channel"] = "dev"
        return completed
    block: dict[str, Any] = {"channel": "dev"}
    if _scaffold_target_is_outside_every_repository(deployment_dir):
        block["kind"] = STANDALONE_RELEASE_KIND
    return block


def _scaffold_target_is_outside_every_repository(deployment_dir: Path) -> bool:
    """Whether the scaffold target sits outside every Git working tree."""
    from okg.substrate.publisher.repository_provenance import (
        deployment_is_outside_repository,
    )

    probe = Path(deployment_dir)
    while not probe.is_dir():
        parent = probe.parent
        if parent == probe:
            return False
        probe = parent
    return bool(deployment_is_outside_repository(probe))


def redacted_init_answers(
    profile: Profile,
    answers: dict[str, Any],
) -> dict[str, Any]:
    """Return a diagnostic/provenance copy of init answers without secrets."""
    redacted: dict[str, Any] = {}
    non_secret_types = {"path", "path_list", "slug", "integer", "orcid"}
    for key, value in answers.items():
        question = profile.question(str(key))
        # Issue #2277: an unset answer stays null. The redactor stringifies
        # its input, so None used to be stored as the string "None", which
        # YAML and every template reader take as a real value.
        if value is None or (
            question is not None and question.type in non_secret_types
        ):
            redacted[str(key)] = value
            continue
        redacted[str(key)] = redact_mapping({str(key): value})[str(key)]
    return redacted


def _profile_component_provenance(profile: Profile) -> dict[str, Any]:
    from okg.substrate.deployment_profile_explain import resolve_profile_explain

    explanation = resolve_profile_explain(profile).to_dict()
    return {
        "modules": explanation["modules"],
        "source_packs": explanation["source_packs"],
        "asset_packs": explanation["asset_packs"],
        "override_points": explanation["override_points"],
    }


def _merge_deployment_defaults(
    body: dict[str, Any],
    defaults: dict[str, Any],
) -> None:
    """Merge profile-owned defaults into deployment.yaml."""
    reserved = {
        "name",
        "description",
        "profile",
        "modules",
        "source_registry",
        "schema_dir",
        "postgres",
    }
    collisions = sorted(set(defaults) & reserved)
    if collisions:
        raise ProfileError(
            "deployment_defaults may not set scaffolder-owned keys: "
            + ", ".join(collisions),
            data={"reserved_keys": collisions},
            fix_hint=(
                "remove reserved keys from deployment-defaults.yaml; "
                "the profile scaffolder writes them"
            ),
        )
    body.update(defaults)


def source_entries_from_default(stem: str, text: str) -> dict[str, Any]:
    """Read one source-default file as ``{source_id: entry}``.

    A file that wraps its entry under a single source id (e.g.
    `bib_references: {class: ..., ...}`) contributes that id; a file that
    is the source entry directly (e.g. `class: ..., config: ...`) is named
    by its filename stem. The packaged installer reads credential
    declarations through this same rule, so it sees the sources the
    rendered registry will hold.
    """
    parsed = fastyaml.safe_load(text) or {}
    if isinstance(parsed, dict) and len(parsed) == 1 and not any(
        k in {"class", "profile", "config", "kind"}
        for k in parsed.keys()
    ):
        return dict(parsed)
    return {stem: parsed}


def _build_source_registry_yaml(plan: ScaffoldPlan) -> str:
    """Compose `source_registry.yaml` from interpolated source
    defaults.

    Each source default is one YAML doc; this function concatenates
    them into a single mapping under top-level `sources:`. If a
    source default itself is a mapping with a single key, that key
    is used as the source id; otherwise the filename stem is used.
    """
    sources_block = source_default_entries(
        plan.profile, plan.source_defaults,
    )

    out: dict[str, Any] = {}
    # A profile that declares nothing renders exactly what it rendered
    # before: `sources:` alone, admitted as legacy.
    if plan.source_admission_contract is not None:
        out["admission_contract"] = plan.source_admission_contract
    out["sources"] = sources_block
    rendered = yaml.safe_dump(out, sort_keys=False)
    if plan.source_admission_contract is not None:
        _refuse_inadmissible_strict_registry(plan, rendered)
    else:
        refuse_unknown_source_default_keys(
            plan.profile,
            sources_block,
            registry_path=plan.deployment_dir / "source_registry.yaml",
        )
    return rendered


def refuse_unknown_source_default_keys(
    profile: Profile,
    sources_block: Mapping[str, Any],
    *,
    registry_path: Path | None = None,
    source_files: Mapping[str, Path] | None = None,
) -> None:
    """Refuse at scaffold time a source default the legacy loader refuses.

    The error names the source-default file that declares the source when
    `source_files` maps its id, else the registry the scaffold would write.

    A legacy registry refuses an entry key it does not read
    (`source_registry_unknown_key`) when it loads. Without this check a
    profile that ships such a key scaffolds, `okg init` and the packaged
    installer exit 0, and the refusal arrives at the first ingest. A profile
    that declares `source_admission_contract` gets the full strict admission
    instead (`_refuse_inadmissible_strict_registry`).
    """
    for source_id, entry in sources_block.items():
        if not isinstance(entry, Mapping):
            continue
        source_file = (source_files or {}).get(str(source_id))
        error_path = source_file or registry_path
        if error_path is None:
            raise ValueError(
                "refuse_unknown_source_default_keys needs registry_path or "
                f"a source_files entry for {source_id!r}"
            )
        try:
            validate_legacy_entry_keys(str(source_id), entry, error_path)
        except SourceRegistryUnknownKeyError as exc:
            raise ProfileError(
                f"profile {profile.name!r} ships source default "
                f"{source_id!r} with a key the source registry does not "
                f"read: {exc}",
                data={
                    "profile_path": str(profile.path),
                    "code": exc.code,
                    "source": str(source_id),
                    "key": exc.key,
                    "suggestion": exc.suggestion,
                    **(
                        {"source_default_file": str(source_file)}
                        if source_file is not None else {}
                    ),
                    **(
                        {"registry_path": str(registry_path)}
                        if registry_path is not None else {}
                    ),
                },
                fix_hint=(
                    "fix the key in the profile's source-defaults file: "
                    "rename it to the suggested key, or move a reader "
                    "setting under `params:`"
                ),
            ) from exc


def source_default_entries(
    profile: Profile,
    source_defaults: Mapping[str, str],
) -> dict[str, Any]:
    """Map every source id the source-default files declare to its entry.

    `source_defaults` maps a file stem to that file's YAML text, in file
    order. A file that wraps its entry under one key (`bib_references:
    {class: ..., ...}`) declares that key as its id; any other file is the
    entry itself and declares its stem.

    Two files that declare one id are refused with DuplicateSourceIdError,
    never merged. This is the ONE duplicate check: the generic scaffold
    and the academic-researcher installer both call it, so neither can drop a
    source that the other refuses (issue #2070).
    """
    sources_block: dict[str, Any] = {}
    # Source id -> the stem of the file that declared it.
    declared_by: dict[str, str] = {}
    for stem, text in source_defaults.items():
        # One id rule, shared with the packaged installer's credential
        # reader: `source_entries_from_default`.
        entries = source_entries_from_default(stem, text)
        for source_id, entry in entries.items():
            if source_id in declared_by:
                _refuse_duplicate_source_id(
                    profile, str(source_id), declared_by[source_id], stem,
                )
            declared_by[source_id] = stem
            sources_block[source_id] = entry
    return sources_block


def _refuse_duplicate_source_id(
    profile: Profile,
    source_id: str,
    first_stem: str,
    second_stem: str,
) -> None:
    """Raise DuplicateSourceIdError naming the id and both files."""
    directory = profile.source_defaults_dir
    files = [
        str(directory / f"{stem}.yaml") if directory is not None
        else f"{stem}.yaml"
        for stem in (first_stem, second_stem)
    ]
    raise DuplicateSourceIdError(
        f"profile {profile.name!r}: source id {source_id!r} is "
        f"declared by two source-default files, {files[0]} and "
        f"{files[1]}; one would silently replace the other",
        data={
            "profile_path": str(profile.path),
            "key": "source_defaults",
            "source_id": source_id,
            "files": files,
        },
        fix_hint=(
            f"keep exactly one source-default file per source id; "
            f"{files[0]} and {files[1]} both declare {source_id}: remove "
            f"or rename the file you do not want (an alternative mode "
            f"replaces the default, it is not added beside it), or give "
            f"the second source a distinct id"
        ),
    )


def _refuse_inadmissible_strict_registry(
    plan: ScaffoldPlan,
    rendered: str,
) -> None:
    """Refuse at scaffold time a strict registry that cannot be admitted.

    Without this, a profile that declares `strict_v1` but ships a source
    default the strict contract rejects still scaffolds, `okg init` and the
    packaged installer exit 0, and the refusal surfaces only at the first
    ingest -- long after the operator stopped watching. The admitter that
    runs here is the same one the runtime runs, so the two cannot disagree.
    """
    registry_path = plan.deployment_dir / "source_registry.yaml"
    try:
        admit_source_registry_document(
            yaml.safe_load(rendered),
            registry_path=registry_path,
        )
    except (ValueError, KeyError) as exc:
        raise ProfileError(
            f"profile {plan.profile.name!r} declares "
            f"source_admission_contract: {plan.source_admission_contract}, "
            f"but its source defaults do not satisfy that contract: {exc}",
            data={
                "profile_path": str(plan.profile.path),
                "key": "source_admission_contract",
                "declared": plan.source_admission_contract,
                "registry_path": str(registry_path),
                "sources": sorted(plan.source_defaults),
                "admission_error": str(exc),
            },
            fix_hint=(
                "fix the source default the message names, or remove "
                "`source_admission_contract` from the profile to keep legacy "
                "admission"
            ),
        ) from exc
