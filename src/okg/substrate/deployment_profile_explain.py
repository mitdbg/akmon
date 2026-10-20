"""Pure profile explanation for built-in deployment profiles.

This module is intentionally read-only. It resolves a profile manifest and its
referenced module/source/default assets into a JSON-serializable explanation
that a CLI can render without scaffolding a deployment or consulting runtime
secrets.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from okg.substrate import fastyaml
from okg.substrate.catalog.module_manifest import (
    ManifestError,
    load_manifest,
)
from okg.substrate.catalog.modules_compose import load_modules_root
from okg.substrate.catalog.profile import (
    PROFILE_CLASSIFICATIONS,
    InitQuestion,
    Profile,
    load_profile,
    load_profile_modules,
)
from okg.substrate.deployment_bootstrap.profile_init import discover_profile
from okg.substrate.sources.redaction import (
    _key_names_secret,
    redact_mapping,
    redact_text,
)


BUILTIN_V1_PROFILE_NAMES: tuple[str, ...] = (
    "academic-researcher",
    "minimal",
    "workspace-graph",
)

_VAR_PATTERN = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


@dataclass(frozen=True)
class ExplainedModule:
    name: str
    required: bool
    origin: str
    override_point: str
    version: str | None = None
    description: str | None = None
    provenance: str = "profile.modules"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "required": self.required,
            "origin": self.origin,
            "override_point": self.override_point,
            "version": self.version,
            "description": self.description,
            "provenance": self.provenance,
        }


@dataclass(frozen=True)
class ExplainedSourcePack:
    name: str
    required: bool
    origin: str
    override_point: str
    version: str
    module: str | None = None
    class_name: str | None = None
    enabled_by_default: bool = True
    placeholders: tuple[str, ...] = ()
    config: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "required": self.required,
            "origin": self.origin,
            "override_point": self.override_point,
            "version": self.version,
            "module": self.module,
            "class": self.class_name,
            "enabled_by_default": self.enabled_by_default,
            "placeholders": list(self.placeholders),
            "config": self.config,
        }


@dataclass(frozen=True)
class ExplainedAssetPack:
    kind: str
    required: bool
    origin: str
    override_point: str
    files: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "required": self.required,
            "origin": self.origin,
            "override_point": self.override_point,
            "files": list(self.files),
        }


@dataclass(frozen=True)
class ProfileExplain:
    name: str
    version: str
    classification: str
    description: str | None
    profile_path: str
    classification_flags: dict[str, bool]
    init_questions: tuple[dict[str, Any], ...]
    modules: tuple[ExplainedModule, ...]
    source_packs: tuple[ExplainedSourcePack, ...]
    asset_packs: tuple[ExplainedAssetPack, ...]
    runtime_defaults: dict[str, Any]
    override_points: tuple[dict[str, str], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "classification": self.classification,
            "classification_flags": dict(self.classification_flags),
            "description": self.description,
            "profile_path": self.profile_path,
            "init_questions": list(self.init_questions),
            "modules": [module.to_dict() for module in self.modules],
            "source_packs": [
                source_pack.to_dict() for source_pack in self.source_packs
            ],
            "asset_packs": [asset_pack.to_dict() for asset_pack in self.asset_packs],
            "runtime_defaults": dict(self.runtime_defaults),
            "override_points": list(self.override_points),
        }

    def to_text(self) -> str:
        required_modules = [m.name for m in self.modules if m.required]
        optional_modules = [m.name for m in self.modules if not m.required]
        required_sources = [s.name for s in self.source_packs if s.required]
        optional_sources = [s.name for s in self.source_packs if not s.required]
        lines = [
            f"profile: {self.name} ({self.version})",
            f"classification: {self.classification}",
        ]
        if self.description:
            first_line = self.description.strip().splitlines()[0]
            lines.append(f"description: {first_line}")
        lines.extend(
            [
                "modules:",
                f"  required: {', '.join(required_modules) or '-'}",
                f"  optional: {', '.join(optional_modules) or '-'}",
                "source packs:",
                f"  required: {', '.join(required_sources) or '-'}",
                f"  optional: {', '.join(optional_sources) or '-'}",
                "asset packs:",
            ]
        )
        for asset in self.asset_packs:
            lines.append(f"  {asset.kind}: {asset.origin} ({len(asset.files)} files)")
        lines.append("override points:")
        for point in self.override_points:
            lines.append(f"  {point['name']}: {point['path']}")
        return "\n".join(lines)


def resolve_profile_explain(
    profile: str | Path | Profile,
) -> ProfileExplain:
    """Resolve a profile into a no-write explanation.

    `profile` may be a built-in/custom profile name, a path to a
    `profile.yaml`, a path to a profile directory, or an already-loaded
    `Profile`. The resolver only reads profile-library files.
    """
    loaded = _coerce_profile(profile)
    modules = _explain_modules(loaded)
    source_packs = _explain_source_packs(loaded)
    asset_packs = _explain_asset_packs(loaded)
    runtime_defaults = _load_runtime_defaults(loaded)
    return ProfileExplain(
        name=loaded.name,
        version=loaded.version,
        classification=loaded.classification,
        classification_flags={
            name: loaded.classification == name for name in PROFILE_CLASSIFICATIONS
        },
        description=loaded.description,
        profile_path=str(loaded.path),
        init_questions=tuple(
            _explain_init_question(question) for question in loaded.init_questions
        ),
        modules=tuple(modules),
        source_packs=tuple(source_packs),
        asset_packs=tuple(asset_packs),
        runtime_defaults=runtime_defaults,
        override_points=tuple(_override_points(loaded)),
    )


def profile_explain_as_dict(
    profile: str | Path | Profile,
) -> dict[str, Any]:
    """Convenience wrapper for CLI JSON output."""
    return resolve_profile_explain(profile).to_dict()


def profile_explain_as_text(profile: str | Path | Profile) -> str:
    """Convenience wrapper for CLI text output."""
    return resolve_profile_explain(profile).to_text()


def _coerce_profile(profile: str | Path | Profile) -> Profile:
    if isinstance(profile, Profile):
        return profile
    candidate = Path(profile)
    if candidate.is_dir():
        return load_profile(candidate / "profile.yaml")
    if candidate.is_file():
        return load_profile(candidate)
    return discover_profile(str(profile))


def _explain_init_question(question: InitQuestion) -> dict[str, Any]:
    default = _redact_init_default(question)
    return {
        "id": question.id,
        "prompt": question.prompt,
        "type": question.type,
        "required": question.required,
        "default": default,
        "description": question.description,
        "override_point": f"init_answers.{question.id}",
    }


def _redact_init_default(question: InitQuestion) -> Any:
    default = question.default
    redacted = redact_mapping({question.id: default}).get(question.id)
    if redacted == str(default):
        return default
    return redacted


def _explain_modules(profile: Profile) -> list[ExplainedModule]:
    module_names = load_profile_modules(profile)
    ordered: list[ExplainedModule] = []
    seen: set[str] = set()
    for name in module_names:
        seen.add(name)
        ordered.append(
            _module_info(
                profile,
                name,
                required=_metadata_required(
                    profile.module_metadata,
                    name,
                    default=True,
                ),
                provenance="profile.modules",
            )
        )
    for name in _metadata_name_list(profile.module_metadata, "optional"):
        if name in seen:
            continue
        seen.add(name)
        ordered.append(
            _module_info(
                profile,
                name,
                required=False,
                provenance="profile.module_metadata.optional",
            )
        )
    return ordered


def _module_info(
    profile: Profile,
    name: str,
    *,
    required: bool,
    provenance: str,
) -> ExplainedModule:
    modules_file = profile.modules_file or profile.path
    origin = str(modules_file)
    version: str | None = None
    description: str | None = None
    manifest_path = load_modules_root() / name / "module.yaml"
    if manifest_path.is_file():
        try:
            manifest = load_manifest(manifest_path)
        except ManifestError:
            manifest = None
        if manifest is not None:
            origin = str(manifest.path)
            version = manifest.version
            description = manifest.description
    return ExplainedModule(
        name=name,
        required=required,
        origin=origin,
        version=version,
        description=description,
        override_point=f"deployment.yaml:modules[{name}]",
        provenance=provenance,
    )


def _explain_source_packs(profile: Profile) -> list[ExplainedSourcePack]:
    source_dir = profile.source_defaults_dir
    if source_dir is None or not source_dir.is_dir():
        return []
    source_packs: list[ExplainedSourcePack] = []
    for path in sorted(source_dir.glob("*.yaml")):
        raw_text = path.read_text(encoding="utf-8")
        raw = _load_yaml_mapping(raw_text, path=path)
        if len(raw) == 1:
            name, config = next(iter(raw.items()))
            source_name = str(name)
            source_config = config if isinstance(config, dict) else {}
        else:
            source_name = path.stem
            source_config = raw
        enabled = bool(source_config.get("enabled_by_default", True))
        required = _metadata_required(
            profile.source_pack_metadata,
            source_name,
            default=enabled,
        )
        source_packs.append(
            ExplainedSourcePack(
                name=source_name,
                required=required,
                origin=str(path),
                override_point=f"source_registry.yaml:sources.{source_name}",
                version=str(
                    _metadata_value(
                        profile.source_pack_metadata,
                        source_name,
                        "version",
                        default=profile.version,
                    )
                ),
                module=_optional_str(source_config.get("module")),
                class_name=_optional_str(source_config.get("class")),
                enabled_by_default=enabled,
                placeholders=tuple(sorted(set(_VAR_PATTERN.findall(raw_text)))),
                config=_redact_value(source_config),
            )
        )
    return source_packs


def _explain_asset_packs(profile: Profile) -> list[ExplainedAssetPack]:
    packs: list[ExplainedAssetPack] = []
    if profile.invariants_file is not None and profile.invariants_file.is_file():
        packs.append(
            ExplainedAssetPack(
                kind="invariants",
                required=True,
                origin=str(profile.invariants_file),
                override_point="invariants.yaml",
                files=(profile.invariants_file.name,),
            )
        )
    if profile.dashboards_dir is not None and profile.dashboards_dir.is_dir():
        packs.append(
            ExplainedAssetPack(
                kind="dashboards",
                required=False,
                origin=str(profile.dashboards_dir),
                override_point="dashboards/",
                files=tuple(_relative_yaml_files(profile.dashboards_dir)),
            )
        )
    if profile.skills_dir is not None and profile.skills_dir.is_dir():
        packs.append(
            ExplainedAssetPack(
                kind="skills",
                required=False,
                origin=str(profile.skills_dir),
                override_point="skills/",
                files=tuple(_relative_files(profile.skills_dir)),
            )
        )
    if profile.extra_files:
        # `files:` -- installed at the same relative path in the deployment.
        packs.append(
            ExplainedAssetPack(
                kind="files",
                required=True,
                origin=str(profile.profile_dir),
                override_point="<deployment dir>/<same relative path>",
                files=tuple(rel for rel, _source in profile.extra_files),
            )
        )
    return packs


def _load_runtime_defaults(profile: Profile) -> dict[str, Any]:
    path = profile.deployment_defaults_file
    if path is None or not path.is_file():
        return {}
    raw_text = path.read_text(encoding="utf-8")
    raw = _load_yaml_mapping(raw_text, path=path)
    return _redact_value(raw)


def _load_yaml_mapping(raw_text: str, *, path: Path) -> dict[str, Any]:
    try:
        raw = fastyaml.safe_load(raw_text) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"{path}: YAML parse error: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: top-level must be a mapping")
    return raw


def _relative_yaml_files(root: Path) -> list[str]:
    return [
        str(path.relative_to(root))
        for path in sorted(root.rglob("*.yaml"))
        if path.is_file()
    ]


def _relative_files(root: Path) -> list[str]:
    return [
        str(path.relative_to(root))
        for path in sorted(root.rglob("*"))
        if path.is_file()
    ]


def _override_points(profile: Profile) -> list[dict[str, str]]:
    points = [
        {
            "name": "init_answers",
            "path": "profile.init_answers",
            "description": "Values supplied at scaffold time.",
        },
        {
            "name": "modules",
            "path": "deployment.yaml:modules",
            "description": "Add or remove ontology modules.",
        },
        {
            "name": "source_defaults",
            "path": "source_registry.yaml:sources",
            "description": "Override source registry entries.",
        },
        {
            "name": "invariants",
            "path": "invariants.yaml",
            "description": "Tune health gates and profile checks.",
        },
        {
            "name": "dashboards",
            "path": "dashboards/",
            "description": "Add, remove, or edit read-side dashboards.",
        },
        {
            "name": "runtime_defaults",
            "path": "deployment.yaml",
            "description": "Override runtime defaults such as viz/enrichers.",
        },
    ]
    seen = {point["name"] for point in points}
    for name in profile.override_points:
        if name in seen:
            continue
        points.append(
            {
                "name": name,
                "path": name,
                "description": "Profile-declared override point.",
            }
        )
        seen.add(name)
    return points


def _metadata_required(
    metadata: dict[str, Any],
    name: str,
    *,
    default: bool,
) -> bool:
    value = _metadata_value(metadata, name, "required", default=None)
    if isinstance(value, bool):
        return value
    if name in _metadata_name_list(metadata, "required"):
        return True
    if name in _metadata_name_list(metadata, "optional"):
        return False
    return default


def _metadata_value(
    metadata: dict[str, Any],
    name: str,
    key: str,
    *,
    default: Any,
) -> Any:
    entry = metadata.get(name)
    if isinstance(entry, dict) and key in entry:
        return entry[key]
    return default


def _metadata_name_list(metadata: dict[str, Any], key: str) -> list[str]:
    value = metadata.get(key)
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item).strip()]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if _key_names_secret(key_text):
                redacted[key_text] = "<redacted>"
            else:
                redacted[key_text] = _redact_value(item)
        return redacted
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    if isinstance(value, str):
        if "://" not in value and "=" not in value and ":" not in value:
            return value
        return redact_text(value)
    return value
