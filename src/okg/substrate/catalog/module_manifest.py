"""Module manifest schema + loader for substrate-library ontologies.

Each ontology module under
`src/okg/substrate/library/ontologies/<name>/` ships a
`module.yaml` declaring what it provides (subtypes, edge_types,
sources, enrichers, invariants, dashboards, intra-module
cross-link rule files) and how it extends sibling modules
(per-target-module blocks naming cross-link rule files that
compose the source module's subtypes with the target module's
subtypes).

The composition resolver
(`okg.substrate.catalog.composition.resolve`) consumes parsed
manifests; the legacy `library/cross_links/` directory was removed
in `add-module-manifests-and-composition`.

Schema (informally):

    name: string                    # required, matches dir name
    version: string                 # required (semver-style)
    description: string             # optional
    provides:                       # required (may be empty mapping)
      subtypes_file: string         # default "schema.yaml"
      edge_types: [string]
      sources: [{class, profile?}]
      enrichers: [{class}]
      invariants_preset: string     # path within module
      dashboards: string            # path within module
      intra_cross_link_rules: [string]   # paths within module
    extends:                        # optional
      <target-module-name>:
        description: string
        cross_link_rules: [string]  # paths within this module

Errors raised at load time (each ManifestError) carry the
manifest file path AND a short fix_hint. The loader does NOT
read referenced rule files — that's the resolver's job, and
deferring lets a manifest validate even when a rule path is
broken (the resolver surfaces that with its own diagnostic).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml
from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError


__all__ = [
    "ManifestError",
    "ModuleManifest",
    "ModuleProvides",
    "ModuleSource",
    "ModuleEnricher",
    "ExtendsBlock",
    "load_manifest",
    "discover_manifests",
]


class ManifestError(SubstrateError):
    """Raised when a `module.yaml` violates the manifest schema.

    `data` carries `{manifest_path, key?}` so the CLI can render
    a structured error envelope. Stringifying the exception
    yields a human-readable message naming the file.
    """
    code = "module_manifest_invalid"


@dataclass(frozen=True)
class ModuleSource:
    """One entry under `provides.sources:`."""
    class_path: str
    profile: Optional[str] = None
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ModuleEnricher:
    """One entry under `provides.enrichers:`."""
    class_path: str
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ModuleProvides:
    """The `provides:` block of a module manifest."""

    # Path within the module dir (default: `schema.yaml`).
    subtypes_file: str = "schema.yaml"
    edge_types: tuple[str, ...] = ()
    sources: tuple[ModuleSource, ...] = ()
    enrichers: tuple[ModuleEnricher, ...] = ()
    invariants_preset: Optional[str] = None
    dashboards: Optional[str] = None
    intra_cross_link_rules: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExtendsBlock:
    """One per-target-module block under `extends:`.

    `cross_link_rules` is a tuple of relative paths inside the
    extending module's directory. The resolver loads each as a
    standard CrossLinkRule YAML file (same schema the deleted
    legacy `library/cross_links/` directory used). `description`
    is human-only — captured for surfacing via diagnostics but
    has no runtime semantics.
    """

    description: Optional[str] = None
    cross_link_rules: tuple[str, ...] = ()


@dataclass(frozen=True)
class ModuleManifest:
    """Parsed `module.yaml` manifest.

    `path` is the absolute path to the manifest file (used by
    the resolver to resolve `cross_link_rules` relative paths).
    `module_dir` is `path.parent` — the directory whose
    `schema.yaml` and rule YAMLs are siblings of the manifest.
    """

    name: str
    version: str
    path: Path
    description: Optional[str] = None
    provides: ModuleProvides = field(default_factory=ModuleProvides)
    extends: dict[str, ExtendsBlock] = field(default_factory=dict)

    @property
    def module_dir(self) -> Path:
        return self.path.parent


# ---------- loader ----------


def load_manifest(path: Path | str) -> ModuleManifest:
    """Load and validate a `module.yaml` at `path`.

    Raises `ManifestError` on schema violations. The error
    message names the manifest path AND the offending key.
    Does NOT touch referenced files (rule YAMLs, schema.yaml,
    invariants presets) — only validates the manifest's own
    structure.
    """
    path = Path(path)
    if not path.is_file():
        raise ManifestError(
            f"module manifest not found: {path}",
            data={"manifest_path": str(path)},
            fix_hint=(
                "create the manifest at this path, or remove the "
                "module from the deployment's modules: list"
            ),
        )
    try:
        with path.open() as f:
            raw = fastyaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ManifestError(
            f"{path}: YAML parse error: {e}",
            data={"manifest_path": str(path)},
            fix_hint="fix the YAML syntax in the manifest",
        ) from e

    if not isinstance(raw, dict):
        raise ManifestError(
            f"{path}: top-level must be a mapping; got "
            f"{type(raw).__name__}",
            data={"manifest_path": str(path)},
            fix_hint="the manifest must start with `name: ...`, etc.",
        )

    # Required top-level keys.
    for key in ("name", "version", "provides"):
        if key not in raw:
            raise ManifestError(
                f"{path}: missing required key {key!r}",
                data={"manifest_path": str(path), "key": key},
                fix_hint=(
                    f"add `{key}: ...` at the top of the manifest"
                ),
            )

    name = raw["name"]
    if not isinstance(name, str) or not name.strip():
        raise ManifestError(
            f"{path}: `name` must be a non-empty string",
            data={"manifest_path": str(path), "key": "name"},
        )

    version = raw["version"]
    if not isinstance(version, str) or not version.strip():
        raise ManifestError(
            f"{path}: `version` must be a non-empty string",
            data={"manifest_path": str(path), "key": "version"},
            fix_hint="use a semver-style string like '1.0.0'",
        )

    description = raw.get("description")
    if description is not None and not isinstance(description, str):
        raise ManifestError(
            f"{path}: `description` must be a string",
            data={"manifest_path": str(path), "key": "description"},
        )

    provides = _parse_provides(raw["provides"], path=path)
    extends = _parse_extends(raw.get("extends"), path=path)

    return ModuleManifest(
        name=name.strip(),
        version=version.strip(),
        path=path.resolve(),
        description=(description.strip() if description else None),
        provides=provides,
        extends=extends,
    )


def discover_manifests(root: Path) -> dict[str, Path]:
    """Walk `root/<module_name>/module.yaml` and return a mapping
    from declared `name` to manifest path.

    Skips directories without a `module.yaml`. Raises
    `ManifestError` on name collisions (two manifests declare the
    same `name:`).
    """
    out: dict[str, Path] = {}
    if not root.is_dir():
        return out
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        manifest_path = entry / "module.yaml"
        if not manifest_path.is_file():
            continue
        manifest = load_manifest(manifest_path)
        if manifest.name in out:
            raise ManifestError(
                f"duplicate module name {manifest.name!r}: "
                f"{out[manifest.name]} and {manifest_path}",
                data={
                    "manifest_path": str(manifest_path),
                    "key": "name",
                    "first": str(out[manifest.name]),
                },
                fix_hint=(
                    "module names are a flat namespace; rename "
                    "one of the colliding manifests"
                ),
            )
        out[manifest.name] = manifest_path
    return out


# ---------- internal parsers ----------


def _parse_provides(
    raw: Any, *, path: Path,
) -> ModuleProvides:
    if raw is None:
        return ModuleProvides()
    if not isinstance(raw, dict):
        raise ManifestError(
            f"{path}: `provides` must be a mapping; got "
            f"{type(raw).__name__}",
            data={"manifest_path": str(path), "key": "provides"},
        )

    subtypes_file = raw.get("subtypes_file", "schema.yaml")
    if not isinstance(subtypes_file, str) or not subtypes_file.strip():
        raise ManifestError(
            f"{path}: `provides.subtypes_file` must be a non-empty "
            f"string when present",
            data={
                "manifest_path": str(path),
                "key": "provides.subtypes_file",
            },
        )

    edge_types = _parse_string_list(
        raw.get("edge_types"),
        path=path,
        key="provides.edge_types",
    )
    sources = _parse_sources(
        raw.get("sources"), path=path,
    )
    enrichers = _parse_enrichers(
        raw.get("enrichers"), path=path,
    )

    invariants_preset = raw.get("invariants_preset")
    if invariants_preset is not None and (
        not isinstance(invariants_preset, str)
        or not invariants_preset.strip()
    ):
        raise ManifestError(
            f"{path}: `provides.invariants_preset` must be a "
            f"non-empty string when present",
            data={
                "manifest_path": str(path),
                "key": "provides.invariants_preset",
            },
        )

    dashboards = raw.get("dashboards")
    if dashboards is not None and (
        not isinstance(dashboards, str) or not dashboards.strip()
    ):
        raise ManifestError(
            f"{path}: `provides.dashboards` must be a non-empty "
            f"string when present",
            data={
                "manifest_path": str(path),
                "key": "provides.dashboards",
            },
        )

    intra_rules = _parse_string_list(
        raw.get("intra_cross_link_rules"),
        path=path,
        key="provides.intra_cross_link_rules",
    )

    return ModuleProvides(
        subtypes_file=subtypes_file.strip(),
        edge_types=tuple(edge_types),
        sources=tuple(sources),
        enrichers=tuple(enrichers),
        invariants_preset=(
            invariants_preset.strip() if invariants_preset else None
        ),
        dashboards=dashboards.strip() if dashboards else None,
        intra_cross_link_rules=tuple(intra_rules),
    )


def _parse_extends(
    raw: Any, *, path: Path,
) -> dict[str, ExtendsBlock]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ManifestError(
            f"{path}: `extends` must be a mapping of "
            f"target-module-name to block; got {type(raw).__name__}",
            data={"manifest_path": str(path), "key": "extends"},
        )
    out: dict[str, ExtendsBlock] = {}
    for target, block in raw.items():
        if not isinstance(target, str) or not target.strip():
            raise ManifestError(
                f"{path}: `extends` keys must be non-empty "
                f"strings; got {target!r}",
                data={
                    "manifest_path": str(path),
                    "key": "extends",
                },
            )
        target = target.strip()
        if block is None:
            out[target] = ExtendsBlock()
            continue
        if not isinstance(block, dict):
            raise ManifestError(
                f"{path}: `extends.{target}` must be a mapping; "
                f"got {type(block).__name__}",
                data={
                    "manifest_path": str(path),
                    "key": f"extends.{target}",
                },
            )
        description = block.get("description")
        if description is not None and not isinstance(description, str):
            raise ManifestError(
                f"{path}: `extends.{target}.description` must "
                f"be a string",
                data={
                    "manifest_path": str(path),
                    "key": f"extends.{target}.description",
                },
            )
        rules = _parse_string_list(
            block.get("cross_link_rules"),
            path=path,
            key=f"extends.{target}.cross_link_rules",
        )
        out[target] = ExtendsBlock(
            description=description.strip() if description else None,
            cross_link_rules=tuple(rules),
        )
    return out


def _parse_string_list(
    raw: Any, *, path: Path, key: str,
) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list) or not all(
        isinstance(s, str) and s.strip() for s in raw
    ):
        raise ManifestError(
            f"{path}: `{key}` must be a list of non-empty strings",
            data={"manifest_path": str(path), "key": key},
        )
    return [s.strip() for s in raw]


def _parse_sources(
    raw: Any, *, path: Path,
) -> list[ModuleSource]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ManifestError(
            f"{path}: `provides.sources` must be a list",
            data={
                "manifest_path": str(path),
                "key": "provides.sources",
            },
        )
    out: list[ModuleSource] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise ManifestError(
                f"{path}: `provides.sources[{i}]` must be a "
                f"mapping",
                data={
                    "manifest_path": str(path),
                    "key": f"provides.sources[{i}]",
                },
            )
        class_path = entry.get("class")
        if not isinstance(class_path, str) or not class_path.strip():
            raise ManifestError(
                f"{path}: `provides.sources[{i}].class` is "
                f"required and must be a non-empty string",
                data={
                    "manifest_path": str(path),
                    "key": f"provides.sources[{i}].class",
                },
            )
        profile = entry.get("profile")
        if profile is not None and (
            not isinstance(profile, str) or not profile.strip()
        ):
            raise ManifestError(
                f"{path}: `provides.sources[{i}].profile` must "
                f"be a non-empty string when present",
                data={
                    "manifest_path": str(path),
                    "key": f"provides.sources[{i}].profile",
                },
            )
        extras = {
            k: v for k, v in entry.items()
            if k not in ("class", "profile")
        }
        out.append(ModuleSource(
            class_path=class_path.strip(),
            profile=profile.strip() if profile else None,
            extras=extras,
        ))
    return out


def _parse_enrichers(
    raw: Any, *, path: Path,
) -> list[ModuleEnricher]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ManifestError(
            f"{path}: `provides.enrichers` must be a list",
            data={
                "manifest_path": str(path),
                "key": "provides.enrichers",
            },
        )
    out: list[ModuleEnricher] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise ManifestError(
                f"{path}: `provides.enrichers[{i}]` must be a "
                f"mapping",
                data={
                    "manifest_path": str(path),
                    "key": f"provides.enrichers[{i}]",
                },
            )
        class_path = entry.get("class")
        if not isinstance(class_path, str) or not class_path.strip():
            raise ManifestError(
                f"{path}: `provides.enrichers[{i}].class` is "
                f"required and must be a non-empty string",
                data={
                    "manifest_path": str(path),
                    "key": f"provides.enrichers[{i}].class",
                },
            )
        extras = {k: v for k, v in entry.items() if k != "class"}
        out.append(ModuleEnricher(
            class_path=class_path.strip(),
            extras=extras,
        ))
    return out
