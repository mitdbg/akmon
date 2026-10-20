"""Profile schema + loader for `okg init`.

A *profile* is a turnkey-deployment recipe: it names a list of
ontology modules to load, the init questions the operator must
answer at scaffold time, default source-registry entries (whose
configs reference init answers via `${var}` interpolation), an
invariants preset, and a dashboards directory. The CLI's
`okg init --profile <name>` reads the profile, gathers answers,
and writes a working deployment to `deployments/<name>/`.

Profile layout on disk:

    profiles/<name>/
        profile.yaml            # this file's schema
        modules.yaml            # list of module names
        source-defaults/        # one YAML per source-registry entry
            <source>.yaml
        invariants.yaml         # copied verbatim into the deployment
        dashboards/             # copied verbatim into the deployment
        schemas/                # optional deployment-native schemas
            bridges/            # optional deployment bridge narrowings
        deployment-defaults.yaml # optional deployment.yaml defaults
        <extra files>           # optional; listed under `files:`

The schema (informally):

    name: string                # required
    version: string             # required (semver-style)
    description: string         # optional
    init_questions:             # required (may be empty list)
      - id: string              # snake_case identifier
        prompt: string          # human-readable prompt
        type: slug|path|path_list|orcid|string|integer
        required: bool          # default false
        default: any            # optional
        description: string     # optional
    modules: string             # path to a YAML file containing a
                                # top-level `modules:` list
    source_defaults: string     # dir within profile, may be absent
    invariants: string          # path within profile
    dashboards: string          # dir within profile
    schemas: string             # dir within profile, copied verbatim
    deployment_defaults: string # optional deployment.yaml fragment
    files:                      # optional list of profile-relative
      - string                  # files, each installed byte-for-byte
                                # at the SAME relative path in the
                                # deployment. ALLOW-LIST only
                                # (`PROFILE_FILES_ALLOWED_ROOT`):
                                # views.sql, aliases.json,
                                # extractors.yaml,
                                # live_authority_profiles.yaml,
                                # identifier_patterns.yaml,
                                # linker_config.yaml,
                                # mcp_instructions.md, and *.md/*.txt
                                # under docs/ -- the deployment-root
                                # files `okg migrate` and the runtime
                                # read by fixed name or by a
                                # deployment.yaml key. Anything else
                                # (Python, schemas/, .git/) is
                                # refused, as is a path that leaves
                                # the profile or a missing file.
    source_admission_contract:  # optional; only `strict_v1` (#1915).
      string                    # Present: the scaffolded
                                # source_registry.yaml declares that
                                # contract at its top level, so the
                                # deployment's sources are admitted
                                # strictly. Absent: the registry is
                                # rendered and admitted exactly as
                                # before. Any other value, including
                                # `legacy_v1`, is refused here.

`load_profile` raises `ProfileError` (subclass of `SubstrateError`,
declared locally to avoid bloating `errors.py`) on schema
violations with file:line context. The loader does NOT read
referenced files (modules.yaml, source-defaults/, etc.) — the
CLI handles those when scaffolding.

`interpolate(value, answers)` does single-pass `${var}` text
substitution from init answers into source-default config
strings. Undefined references raise `ProfileError`.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Optional

import yaml
from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError


#: The only `source_admission_contract` a profile may declare (#1915). Spelled
#: here rather than imported from `okg.substrate.sources.registry`, so the
#: profile loader -- which `distributions/package_assets.py` uses to verify a
#: package WITHOUT importing it -- takes no dependency on the ingest stack of
#: its own. That does not yet shed the stack: importing `okg.substrate.catalog`
#: pulls `catalog.reconcile`, and with it the registry, either way. The test
#: `tests/substrate/cli/test_profile_strict_source_admission.py` pins this
#: equal to `registry.STRICT_ADMISSION_CONTRACT`, so the two cannot drift.
_STRICT_ADMISSION_CONTRACT = "strict_v1"


__all__ = [
    "InitQuestion",
    "Profile",
    "ProfileError",
    "ProfileFetchError",
    "ProfileSourceDefault",
    "INIT_QUESTION_TYPES",
    "PROFILE_FILES_ALLOWED_ROOT",
    "PROFILE_CLASSIFICATIONS",
    "load_profile",
    "load_profile_modules",
    "interpolate",
    "validate_answer",
]


class ProfileError(SubstrateError):
    """Raised when a `profiles/<name>/profile.yaml` violates the
    profile schema, or when init-answer interpolation references
    an undefined variable.

    Declared locally rather than in `errors.py` because the
    `profile_invalid` code is per-instance: `data` carries
    `{profile_path, key?, question_id?, variable?}` so the CLI
    emits a structured envelope. Per the per-instance pattern
    used by other deployment-bootstrap errors in the substrate.
    """

    code = "profile_invalid"


class ProfileFetchError(SubstrateError):
    """Raised when a profile can't be resolved because the external
    reference-deployments repo is unconfigured or unreachable.

    Distinct from :class:`ProfileError` (`profile_invalid`, which means
    "the profile name was searched everywhere and not found"): this
    `profile_fetch_failed` code means the fetch cache is empty *and* the
    operator never ran `okg fetch-deployments` (or it failed), so the
    cache cascade could not even be consulted. `data` carries the
    `condition` (`"external-repo-unconfigured"`) plus the fetch hint so
    the CLI can surface an actionable, structured envelope rather than a
    silent None or a misleading bare not-found.
    """

    code = "profile_fetch_failed"


# Permitted `init_questions[].type` values.
#
# - `slug`     — kebab/snake-case identifier (dir / db name).
# - `path`     — single filesystem path; tilde-expansion at
#                interpolation time.
# - `path_list`— comma-separated list of paths.
# - `orcid`    — ORCID identifier (0000-0000-0000-000X).
# - `string`   — free-form text.
# - `integer`  — integer; CLI parses + validates.
INIT_QUESTION_TYPES: tuple[str, ...] = (
    "slug",
    "path",
    "path_list",
    "orcid",
    "string",
    "integer",
)


PROFILE_CLASSIFICATIONS: tuple[str, ...] = (
    "reusable",
    "demo",
    "example",
    "fixture",
)


@dataclass(frozen=True)
class InitQuestion:
    """One entry under `init_questions:`."""

    id: str
    prompt: str
    type: str
    required: bool = False
    default: Any = None
    description: Optional[str] = None


@dataclass(frozen=True)
class ProfileSourceDefault:
    """One source-default YAML file under `source_defaults/`.

    `name` is the filename stem (e.g. `bib_references` for
    `source-defaults/bib_references.yaml`). `raw_text` is the
    file contents before any `${var}` interpolation.
    """

    name: str
    path: Path
    raw_text: str


@dataclass(frozen=True)
class Profile:
    """Parsed `profile.yaml`.

    `path` is the absolute path of the manifest file. `profile_dir`
    is `path.parent` — the directory whose `source-defaults/`,
    `invariants.yaml`, etc. are siblings.

    `modules_file`, `source_defaults_dir`, `invariants_file`,
    `dashboards_dir`, `skills_dir`, `schemas_dir` are resolved Path objects
    pointing at the sibling files / dirs referenced from the
    profile. Each may or may not exist on disk; the CLI checks
    at scaffold time.

    `skills_dir` carries the per-profile agent skills bundle
    (markdown files with frontmatter — `name:`, `description:`).
    Scaffolded deployments copy the bundle to
    `deployments/<name>/skills/` so any MCP-aware agent loaded
    against the deployment can read them. Used by the
    academic-pi profile per `add-academic-pi-skills`.
    """

    name: str
    version: str
    path: Path
    classification: str = "example"
    description: Optional[str] = None
    init_questions: tuple[InitQuestion, ...] = ()
    modules_file: Optional[Path] = None
    source_defaults_dir: Optional[Path] = None
    invariants_file: Optional[Path] = None
    dashboards_dir: Optional[Path] = None
    skills_dir: Optional[Path] = None
    schemas_dir: Optional[Path] = None
    deployment_defaults_file: Optional[Path] = None
    #: `files:` entries as (profile-relative POSIX path, resolved source
    #: path). Each is installed at the same relative path in the deployment.
    extra_files: tuple[tuple[str, Path], ...] = ()
    #: `strict_v1` when the profile asks its scaffolded
    #: `source_registry.yaml` to declare that admission contract, else None.
    source_admission_contract: Optional[str] = None
    module_metadata: dict[str, Any] = field(default_factory=dict)
    source_pack_metadata: dict[str, Any] = field(default_factory=dict)
    override_points: tuple[str, ...] = ()

    @property
    def profile_dir(self) -> Path:
        return self.path.parent

    def question(self, qid: str) -> Optional[InitQuestion]:
        for q in self.init_questions:
            if q.id == qid:
                return q
        return None


# ---------- public API ----------


def load_profile(path: Path | str) -> Profile:
    """Load and validate a `profile.yaml` at `path`.

    Raises `ProfileError` on schema violations. The error
    message names the profile path AND the offending key.
    Does NOT touch referenced files — only validates the
    profile's own structure.
    """
    path = Path(path)
    if not path.is_file():
        raise ProfileError(
            f"profile not found: {path}",
            data={"profile_path": str(path)},
            fix_hint=(
                "create the profile at this path, or pass a different "
                "--profile <name> (profiles live under `profiles/<name>/"
                "profile.yaml`)"
            ),
        )
    try:
        with path.open() as f:
            raw = fastyaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ProfileError(
            f"{path}: YAML parse error: {e}",
            data={"profile_path": str(path)},
            fix_hint="fix the YAML syntax in the profile",
        ) from e

    if not isinstance(raw, dict):
        raise ProfileError(
            f"{path}: top-level must be a mapping; got {type(raw).__name__}",
            data={"profile_path": str(path)},
            fix_hint="the profile must start with `name: ...`, etc.",
        )

    # Required top-level keys.
    for key in ("name", "version"):
        if key not in raw:
            raise ProfileError(
                f"{path}: missing required key {key!r}",
                data={"profile_path": str(path), "key": key},
                fix_hint=(f"add `{key}: ...` at the top of the profile"),
            )

    name = raw["name"]
    if not isinstance(name, str) or not name.strip():
        raise ProfileError(
            f"{path}: `name` must be a non-empty string",
            data={"profile_path": str(path), "key": "name"},
        )

    version = raw["version"]
    if not isinstance(version, str) or not version.strip():
        raise ProfileError(
            f"{path}: `version` must be a non-empty string",
            data={"profile_path": str(path), "key": "version"},
            fix_hint="use a semver-style string like '1.0.0'",
        )

    description = raw.get("description")
    if description is not None and not isinstance(description, str):
        raise ProfileError(
            f"{path}: `description` must be a string",
            data={"profile_path": str(path), "key": "description"},
        )

    classification = raw.get("classification", "example")
    if not isinstance(classification, str) or not classification.strip():
        raise ProfileError(
            f"{path}: `classification` must be a non-empty string",
            data={"profile_path": str(path), "key": "classification"},
            fix_hint=(f"use one of: {', '.join(PROFILE_CLASSIFICATIONS)}"),
        )
    classification = classification.strip()
    if classification not in PROFILE_CLASSIFICATIONS:
        raise ProfileError(
            f"{path}: unsupported profile classification {classification!r}",
            data={
                "profile_path": str(path),
                "key": "classification",
                "classification": classification,
            },
            fix_hint=(f"use one of: {', '.join(PROFILE_CLASSIFICATIONS)}"),
        )

    module_metadata = _parse_optional_mapping(
        raw.get("module_metadata"),
        path=path,
        key="module_metadata",
    )
    source_pack_metadata = _parse_optional_mapping(
        raw.get("source_pack_metadata"),
        path=path,
        key="source_pack_metadata",
    )
    override_points = tuple(
        _parse_optional_string_list(
            raw.get("override_points"),
            path=path,
            key="override_points",
        )
    )

    questions = _parse_init_questions(
        raw.get("init_questions"),
        path=path,
    )

    profile_dir = path.parent

    modules_ref = raw.get("modules")
    modules_file: Optional[Path] = None
    if modules_ref is not None:
        if not isinstance(modules_ref, str) or not modules_ref.strip():
            raise ProfileError(
                f"{path}: `modules` must be a non-empty string when "
                f"present (path to a YAML file with a top-level "
                f"`modules:` list)",
                data={"profile_path": str(path), "key": "modules"},
            )
        modules_file = (profile_dir / modules_ref.strip()).resolve()

    source_defaults_ref = raw.get("source_defaults")
    source_defaults_dir: Optional[Path] = None
    if source_defaults_ref is not None:
        if not isinstance(source_defaults_ref, str) or not source_defaults_ref.strip():
            raise ProfileError(
                f"{path}: `source_defaults` must be a non-empty "
                f"string when present (relative dir within the "
                f"profile)",
                data={
                    "profile_path": str(path),
                    "key": "source_defaults",
                },
            )
        source_defaults_dir = (profile_dir / source_defaults_ref.strip()).resolve()

    invariants_ref = raw.get("invariants")
    invariants_file: Optional[Path] = None
    if invariants_ref is not None:
        if not isinstance(invariants_ref, str) or not invariants_ref.strip():
            raise ProfileError(
                f"{path}: `invariants` must be a non-empty string "
                f"when present (relative path within the profile)",
                data={
                    "profile_path": str(path),
                    "key": "invariants",
                },
            )
        invariants_file = (profile_dir / invariants_ref.strip()).resolve()

    dashboards_ref = raw.get("dashboards")
    dashboards_dir: Optional[Path] = None
    if dashboards_ref is not None:
        if not isinstance(dashboards_ref, str) or not dashboards_ref.strip():
            raise ProfileError(
                f"{path}: `dashboards` must be a non-empty string "
                f"when present (relative dir within the profile)",
                data={
                    "profile_path": str(path),
                    "key": "dashboards",
                },
            )
        dashboards_dir = (profile_dir / dashboards_ref.strip()).resolve()

    skills_ref = raw.get("skills")
    skills_dir: Optional[Path] = None
    if skills_ref is not None:
        if not isinstance(skills_ref, str) or not skills_ref.strip():
            raise ProfileError(
                f"{path}: `skills` must be a non-empty string "
                f"when present (relative dir within the profile)",
                data={
                    "profile_path": str(path),
                    "key": "skills",
                },
            )
        skills_dir = (profile_dir / skills_ref.strip()).resolve()

    schemas_ref = raw.get("schemas")
    schemas_dir: Optional[Path] = None
    if schemas_ref is not None:
        if not isinstance(schemas_ref, str) or not schemas_ref.strip():
            raise ProfileError(
                f"{path}: `schemas` must be a non-empty string "
                f"when present (relative dir within the profile)",
                data={
                    "profile_path": str(path),
                    "key": "schemas",
                },
            )
        schemas_dir = _resolve_contained_schemas_dir(
            profile_dir=profile_dir,
            profile_path=path,
            reference=schemas_ref.strip(),
        )

    deployment_defaults_ref = raw.get("deployment_defaults")
    deployment_defaults_file: Optional[Path] = None
    if deployment_defaults_ref is not None:
        if (
            not isinstance(deployment_defaults_ref, str)
            or not deployment_defaults_ref.strip()
        ):
            raise ProfileError(
                f"{path}: `deployment_defaults` must be a non-empty "
                f"string when present (relative file within the "
                f"profile)",
                data={
                    "profile_path": str(path),
                    "key": "deployment_defaults",
                },
            )
        deployment_defaults_file = (
            profile_dir / deployment_defaults_ref.strip()
        ).resolve()

    extra_files = _parse_extra_files(
        raw.get("files"),
        profile_dir=profile_dir,
        profile_path=path,
    )

    source_admission_contract: Optional[str] = None
    if "source_admission_contract" in raw:
        declared = raw.get("source_admission_contract")
        if declared != _STRICT_ADMISSION_CONTRACT:
            # Refused, never defaulted: a package that believes it asked for
            # strict admission must not be handed the legacy contract
            # quietly. `legacy_v1` is refused for the same reason the registry
            # admitter refuses it as an explicit marker -- legacy admission is
            # what the absence of the key means, not a value to select.
            raise ProfileError(
                f"{path}: `source_admission_contract` must be "
                f"{_STRICT_ADMISSION_CONTRACT!r} when present, got "
                f"{declared!r}",
                data={
                    "profile_path": str(path),
                    "key": "source_admission_contract",
                    "declared": declared,
                    "supported": [_STRICT_ADMISSION_CONTRACT],
                },
                fix_hint=(
                    "set `source_admission_contract: "
                    f"{_STRICT_ADMISSION_CONTRACT}`, or remove the key to keep "
                    "the legacy admission the profile already renders"
                ),
            )
        source_admission_contract = _STRICT_ADMISSION_CONTRACT

    return Profile(
        name=name.strip(),
        version=version.strip(),
        path=path.resolve(),
        classification=classification,
        description=(description.strip() if description else None),
        init_questions=tuple(questions),
        modules_file=modules_file,
        source_defaults_dir=source_defaults_dir,
        invariants_file=invariants_file,
        dashboards_dir=dashboards_dir,
        skills_dir=skills_dir,
        schemas_dir=schemas_dir,
        deployment_defaults_file=deployment_defaults_file,
        extra_files=extra_files,
        source_admission_contract=source_admission_contract,
        module_metadata=module_metadata,
        source_pack_metadata=source_pack_metadata,
        override_points=override_points,
    )


#: The only files a profile's `files:` list may install (lead decision on
#: #2787, fail-closed). These are the deployment-root configuration files the
#: runtime and `okg migrate` read by fixed name; `mcp_instructions.md` is the
#: default MCP instructions name. Anything else -- deployment-local Python
#: (`sources/`, `enrichers/`, `workers/`, ...), `schemas/` (which has its own
#: checked slot), `.git/`, or any executable -- is refused at load, so a
#: third-party bundle cannot plant code the runtime imports.
PROFILE_FILES_ALLOWED_ROOT = frozenset({
    "views.sql",
    "aliases.json",
    "extractors.yaml",
    "live_authority_profiles.yaml",
    "identifier_patterns.yaml",
    "linker_config.yaml",
    "mcp_instructions.md",
})
#: Plus a `docs/` subtree of plain text, e.g. the file a manifest's
#: `mcp_instructions: docs/mcp-instructions.md` names.
PROFILE_FILES_DOCS_DIR = "docs"
PROFILE_FILES_DOCS_SUFFIXES = frozenset({".md", ".txt"})


def _profile_file_allowed(posix: str) -> bool:
    """True when `posix` is an allow-listed `files:` destination."""
    parts = posix.split("/")
    if len(parts) == 1:
        return posix in PROFILE_FILES_ALLOWED_ROOT
    if parts[0] != PROFILE_FILES_DOCS_DIR:
        return False
    if any(not part or part.startswith(".") for part in parts[1:]):
        return False
    return PurePosixPath(parts[-1]).suffix in PROFILE_FILES_DOCS_SUFFIXES


def profile_files_allowed_text() -> str:
    """The allowed `files:` set, in words, for errors and docs."""
    suffixes = " or ".join(f"*{s}" for s in sorted(PROFILE_FILES_DOCS_SUFFIXES))
    return (
        ", ".join(sorted(PROFILE_FILES_ALLOWED_ROOT))
        + f", and {suffixes} files under {PROFILE_FILES_DOCS_DIR}/"
    )


def _parse_extra_files(
    raw: Any,
    *,
    profile_dir: Path,
    profile_path: Path,
) -> tuple[tuple[str, Path], ...]:
    """Parse `files:` into (relative path, resolved source) pairs.

    Each entry is a profile-relative file path, installed at the same relative
    path in the deployment. Only allow-listed names are accepted
    (`PROFILE_FILES_ALLOWED_ROOT` plus a `docs/` text subtree). The same
    containment rule as `schemas` applies: absolute paths, parent traversal
    and symlink escapes are refused, because the destination is derived from
    the reference. Existence is checked when the plan is built, like the other
    referenced files.
    """
    if raw is None:
        return ()
    if not isinstance(raw, list) or not all(
        isinstance(item, str) and item.strip() for item in raw
    ):
        raise ProfileError(
            f"{profile_path}: `files` must be a list of non-empty strings "
            f"(relative file paths within the profile)",
            data={"profile_path": str(profile_path), "key": "files"},
            fix_hint="list each file by its path inside the profile, "
            "such as `files: [views.sql, aliases.json]`",
        )
    profile_root = profile_dir.resolve()
    parsed: list[tuple[str, Path]] = []
    seen: dict[str, str] = {}
    for item in raw:
        reference = item.strip()
        if "\x00" in reference:
            raise _extra_file_error(
                profile_path, reference, "contains a NUL character", "unsafe_path",
            )
        relative = Path(reference)
        windows_relative = PureWindowsPath(reference)
        if (
            relative.is_absolute()
            or windows_relative.is_absolute()
            or ".." in relative.parts
            or ".." in windows_relative.parts
            or "\\" in reference
        ):
            raise _extra_file_error(
                profile_path, reference, "must stay inside the profile",
                "unsafe_path",
            )
        posix = relative.as_posix()
        if not _profile_file_allowed(posix):
            raise _extra_file_error(
                profile_path, reference, "is not an allowed profile file",
                "not_allowed", allowed=profile_files_allowed_text(),
            )
        resolved = (profile_root / relative).resolve()
        if not resolved.is_relative_to(profile_root):
            raise _extra_file_error(
                profile_path, reference, "resolves outside the profile",
                "path_escape", resolved_path=str(resolved),
            )
        portable = unicodedata.normalize("NFC", posix).casefold()
        if portable in seen:
            raise _extra_file_error(
                profile_path, reference, "is listed more than once",
                "duplicate", duplicate_of=seen[portable],
            )
        seen[portable] = reference
        parsed.append((posix, resolved))
    return tuple(parsed)


def _extra_file_error(
    profile_path: Path,
    reference: str,
    message: str,
    reason: str,
    **extra: Any,
) -> ProfileError:
    return ProfileError(
        f"{profile_path}: `files` entry {reference!r} {message}; allowed: "
        f"{profile_files_allowed_text()}",
        data={
            "profile_path": str(profile_path),
            "key": "files",
            "reference": reference,
            "reason": reason,
            **extra,
        },
        fix_hint=(
            "list only allowed files that live inside the profile: "
            + profile_files_allowed_text()
        ),
    )


def _resolve_contained_schemas_dir(
    *,
    profile_dir: Path,
    profile_path: Path,
    reference: str,
) -> Path:
    """Resolve a schema asset reference without leaving its profile.

    Profile schemas are distribution-owned package data. Absolute paths,
    parent traversal, and symlink escapes would turn the profile into a file
    copy primitive over operator-owned paths, so reject those shapes before
    the scaffolder inspects or writes anything.
    """
    relative = Path(reference)
    windows_relative = PureWindowsPath(reference)
    if (
        relative.is_absolute()
        or windows_relative.is_absolute()
        or ".." in relative.parts
        or ".." in windows_relative.parts
    ):
        raise ProfileError(
            f"{profile_path}: `schemas` must stay inside the profile; "
            f"unsafe path {reference!r}",
            data={
                "profile_path": str(profile_path),
                "key": "schemas",
                "reference": reference,
                "reason": "unsafe_path",
            },
            fix_hint=(
                "use a relative directory below the profile, such as "
                "`schemas: schemas/`"
            ),
        )

    profile_root = profile_dir.resolve()
    resolved = (profile_root / relative).resolve()
    if not resolved.is_relative_to(profile_root):
        raise ProfileError(
            f"{profile_path}: `schemas` resolves outside the profile: "
            f"{reference!r}",
            data={
                "profile_path": str(profile_path),
                "key": "schemas",
                "reference": reference,
                "resolved_path": str(resolved),
                "reason": "path_escape",
            },
            fix_hint=(
                "replace the escaping path or symlink with a real directory "
                "inside the profile"
            ),
        )
    return resolved


def load_profile_modules(profile: Profile) -> list[str]:
    """Load the profile's `modules.yaml` and return the list of
    module names. Raises `ProfileError` when the file is missing
    or malformed.

    Returns an empty list when the profile declared no
    `modules:` reference (e.g. the test profile with two
    in-process modules supplied directly — see test fixture).
    """
    if profile.modules_file is None:
        return []
    if not profile.modules_file.is_file():
        raise ProfileError(
            f"profile {profile.name!r}: modules file not found: {profile.modules_file}",
            data={
                "profile_path": str(profile.path),
                "modules_file": str(profile.modules_file),
                "key": "modules",
            },
            fix_hint=(
                "create the modules file with a top-level `modules:` "
                "list of module names, or drop the `modules:` "
                "reference from the profile"
            ),
        )
    try:
        with profile.modules_file.open() as f:
            raw = fastyaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ProfileError(
            f"{profile.modules_file}: YAML parse error: {e}",
            data={
                "profile_path": str(profile.path),
                "modules_file": str(profile.modules_file),
            },
            fix_hint="fix the YAML syntax in the modules file",
        ) from e
    if not isinstance(raw, dict):
        raise ProfileError(
            f"{profile.modules_file}: top-level must be a mapping "
            f"with a `modules:` list",
            data={"profile_path": str(profile.path)},
        )
    modules = raw.get("modules")
    if modules is None:
        return []
    if not isinstance(modules, list) or not all(
        isinstance(m, str) and m.strip() for m in modules
    ):
        raise ProfileError(
            f"{profile.modules_file}: `modules` must be a list of non-empty strings",
            data={"profile_path": str(profile.path), "key": "modules"},
        )
    return [m.strip() for m in modules]


# ---------- variable interpolation ----------

# Single-pass `${var}` substitution. Variable names must match
# snake_case identifier conventions to keep the matcher narrow
# (avoids accidentally consuming `${OKG_VAR}` env-style refs;
# those happen elsewhere in the substrate). Per design.md:
# "single-pass: init answers feed into config strings before
# they're written to disk. No recursive interpolation, no
# template engine."
_VAR_PATTERN = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def interpolate(value: str, answers: dict[str, Any]) -> str:
    """Single-pass `${var}` substitution from `answers` into
    `value`.

    Undefined references raise `ProfileError` (loud failure
    rather than silent leave-as-is, per
    `history/openspec/project.md`'s no-silent-fallbacks discipline).

    The substitution does NOT recurse: if an answer's value
    itself contains `${...}`, the resulting interpolated string
    keeps the placeholder verbatim. Profiles that need richer
    templating ship Python helpers (per design.md).

    List-typed answers (e.g. `path_list`) are joined with `, `
    when interpolated into a string context. Bool-typed answers
    serialize as `"true"` / `"false"`.
    """

    def replace_match(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in answers:
            raise ProfileError(
                f"undefined variable reference {match.group(0)!r} in "
                f"profile source default (no init answer named "
                f"{key!r})",
                data={"variable": key},
                fix_hint=(
                    f"add an init_question with id {key!r} to the "
                    f"profile, or remove the reference from the "
                    f"source default"
                ),
            )
        v = answers[key]
        if v is None:
            return ""
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (list, tuple)):
            return ", ".join(str(item) for item in v)
        return str(v)

    return _VAR_PATTERN.sub(replace_match, value)


# ---------- answer validation ----------


_SLUG_PATTERN = re.compile(r"^[a-z][a-z0-9_\-]*$")
# ORCID = 16 digits in 4-4-4-4 groups, last char may be `X` per
# the ORCID checksum spec (mod 11-2). We accept lowercase `x`
# too and uppercase it at storage time.
_ORCID_PATTERN = re.compile(r"^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9Xx]$")


def validate_answer(question: InitQuestion, raw: Any) -> Any:
    """Coerce + validate a raw answer against `question.type`.

    Returns the (possibly-coerced) Python value to store in
    `init_answers`. Raises `ProfileError` on invalid input with
    a fix-hint naming the question id and the expected type.

    Empty / None on a required question without a default
    raises. Empty / None on an optional question returns the
    default (or None).
    """
    is_empty = raw is None or (isinstance(raw, str) and raw.strip() == "")
    if is_empty:
        if question.required and question.default is None:
            raise ProfileError(
                f"init question {question.id!r} is required but "
                f"no answer was supplied (and no default)",
                data={
                    "question_id": question.id,
                    "type": question.type,
                },
                fix_hint=(
                    f"supply --{question.id.replace('_', '-')}=<value> "
                    f"on the command line, or answer the prompt"
                ),
            )
        return question.default

    t = question.type
    if t == "slug":
        if not isinstance(raw, str) or not _SLUG_PATTERN.match(raw):
            raise ProfileError(
                f"init question {question.id!r}: value {raw!r} is "
                f"not a valid slug (must match `^[a-z][a-z0-9_-]*$`)",
                data={
                    "question_id": question.id,
                    "type": t,
                    "value": str(raw),
                },
                fix_hint=(
                    "use lowercase letters, digits, hyphens, "
                    "underscores; start with a letter"
                ),
            )
        return raw
    if t == "string":
        return str(raw)
    if t == "path":
        return str(raw)
    if t == "path_list":
        if isinstance(raw, list):
            return [str(p).strip() for p in raw if str(p).strip()]
        # Accept comma-separated string from CLI/prompt.
        if isinstance(raw, str):
            return [p.strip() for p in raw.split(",") if p.strip()]
        raise ProfileError(
            f"init question {question.id!r}: path_list expects a "
            f"comma-separated string or a list; got {type(raw).__name__}",
            data={"question_id": question.id, "type": t},
        )
    if t == "orcid":
        if not isinstance(raw, str) or not _ORCID_PATTERN.match(raw):
            raise ProfileError(
                f"init question {question.id!r}: value {raw!r} is "
                f"not a valid ORCID (expected NNNN-NNNN-NNNN-NNNN, "
                f"last digit may be X)",
                data={
                    "question_id": question.id,
                    "type": t,
                    "value": str(raw),
                },
            )
        return raw.upper()
    if t == "integer":
        try:
            return int(raw)
        except (TypeError, ValueError) as e:
            raise ProfileError(
                f"init question {question.id!r}: value {raw!r} is not a valid integer",
                data={
                    "question_id": question.id,
                    "type": t,
                    "value": str(raw),
                },
            ) from e
    raise ProfileError(
        f"init question {question.id!r}: unknown type {t!r} "
        f"(supported: {', '.join(INIT_QUESTION_TYPES)})",
        data={"question_id": question.id, "type": t},
    )


# ---------- internal parsers ----------


def _parse_init_questions(
    raw: Any,
    *,
    path: Path,
) -> list[InitQuestion]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ProfileError(
            f"{path}: `init_questions` must be a list",
            data={"profile_path": str(path), "key": "init_questions"},
        )
    seen_ids: set[str] = set()
    out: list[InitQuestion] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise ProfileError(
                f"{path}: `init_questions[{i}]` must be a mapping",
                data={
                    "profile_path": str(path),
                    "key": f"init_questions[{i}]",
                },
            )
        qid = entry.get("id")
        if not isinstance(qid, str) or not qid.strip():
            raise ProfileError(
                f"{path}: `init_questions[{i}].id` is required and "
                f"must be a non-empty string",
                data={
                    "profile_path": str(path),
                    "key": f"init_questions[{i}].id",
                },
            )
        qid = qid.strip()
        if qid in seen_ids:
            raise ProfileError(
                f"{path}: duplicate `init_questions[].id`: {qid!r}",
                data={
                    "profile_path": str(path),
                    "key": "init_questions",
                    "duplicate_id": qid,
                },
                fix_hint=(
                    "init question ids form a flat namespace; "
                    "rename the second occurrence"
                ),
            )
        seen_ids.add(qid)

        qtype = entry.get("type", "string")
        if qtype not in INIT_QUESTION_TYPES:
            raise ProfileError(
                f"{path}: `init_questions[{i}].type` {qtype!r} is "
                f"not one of {', '.join(INIT_QUESTION_TYPES)}",
                data={
                    "profile_path": str(path),
                    "key": f"init_questions[{i}].type",
                    "value": str(qtype),
                },
            )

        prompt = entry.get("prompt", qid)
        if not isinstance(prompt, str) or not prompt.strip():
            raise ProfileError(
                f"{path}: `init_questions[{i}].prompt` must be a "
                f"non-empty string when present",
                data={
                    "profile_path": str(path),
                    "key": f"init_questions[{i}].prompt",
                },
            )

        required = entry.get("required", False)
        if not isinstance(required, bool):
            raise ProfileError(
                f"{path}: `init_questions[{i}].required` must be a bool",
                data={
                    "profile_path": str(path),
                    "key": f"init_questions[{i}].required",
                },
            )

        description = entry.get("description")
        if description is not None and not isinstance(description, str):
            raise ProfileError(
                f"{path}: `init_questions[{i}].description` must be a string",
                data={
                    "profile_path": str(path),
                    "key": f"init_questions[{i}].description",
                },
            )

        out.append(
            InitQuestion(
                id=qid,
                prompt=prompt.strip(),
                type=qtype,
                required=required,
                default=entry.get("default"),
                description=(description.strip() if description else None),
            )
        )
    return out


def _parse_optional_mapping(
    raw: Any,
    *,
    path: Path,
    key: str,
) -> dict[str, Any]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ProfileError(
            f"{path}: `{key}` must be a mapping when present",
            data={"profile_path": str(path), "key": key},
        )
    return dict(raw)


def _parse_optional_string_list(
    raw: Any,
    *,
    path: Path,
    key: str,
) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list) or not all(
        isinstance(item, str) and item.strip() for item in raw
    ):
        raise ProfileError(
            f"{path}: `{key}` must be a list of non-empty strings when present",
            data={"profile_path": str(path), "key": key},
        )
    return [item.strip() for item in raw]
