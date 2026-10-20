"""Library-reference resolver (M5b.6.1).

Resolves `okg.<kind>.<name>` references to concrete resources
shipped in this package. Kinds:

  invariants / linker_routes / templates
    → Path to a YAML file (or directory, for templates).
  modules
    → a Python import target `okg.substrate.library.sources.<name>`

Missing names raise `LibraryRefMissing` whose error data lists
the available alternatives under the same kind, so the catalog
loader can surface "did-you-mean" to the author.

Per `unify-ontology-systems` (2026-04-26): the legacy
`okg.ontology.*` and `okg.narrowings.*` resolver branches
were removed. All ontology declaration now lives in
`library/ontologies/<name>/schema.yaml` and is loaded via
the deployment's `modules:` list, not via dotted refs.
"""
from __future__ import annotations
from okg.substrate import fastyaml

from pathlib import Path

from okg.substrate.errors import SubstrateError


__all__ = [
    "LibraryRefMissing",
    "KINDS",
    "available_refs",
    "resolve",
    "parse_ref",
]


class LibraryRefMissing(SubstrateError):
    code = "library_ref_missing"


# Supported ref kinds. `templates` is a directory; the rest are
# YAML files.
KINDS: tuple[str, ...] = (
    "invariants",
    "linker_routes",
    "modules",
    "templates",
)


_LIBRARY_ROOT = Path(__file__).parent


def parse_ref(ref: str) -> tuple[str, str]:
    """Split `okg.<kind>.<name>` into (kind, name).

    Raises ValueError if the string isn't shaped like a library ref.
    """
    if not ref.startswith("okg."):
        raise ValueError(
            f"library ref must start with 'okg.'; got {ref!r}"
        )
    parts = ref.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"library ref must be 'okg.<kind>.<name>'; got {ref!r}"
        )
    _, kind, name = parts
    if kind not in KINDS:
        raise ValueError(
            f"unknown library kind {kind!r}; valid kinds: "
            f"{sorted(KINDS)}"
        )
    return kind, name


def _stems_in(dir_path: Path, suffix: str) -> list[str]:
    if not dir_path.exists():
        return []
    return sorted(
        p.stem for p in dir_path.iterdir()
        if p.is_file() and p.suffix == suffix
    )


def _dirnames_in(dir_path: Path) -> list[str]:
    if not dir_path.exists():
        return []
    return sorted(p.name for p in dir_path.iterdir() if p.is_dir())


def _template_names_in(dir_path: Path) -> list[str]:
    if not dir_path.exists():
        return []
    return sorted(
        p.name for p in dir_path.iterdir()
        if p.is_dir()
        and not p.name.startswith("__")
        and any(child.is_file() for child in p.iterdir())
    )


def available_refs(kind: str) -> list[str]:
    """Return the list of valid names for a kind. Used by the
    error payload for LibraryRefMissing."""
    if kind not in KINDS:
        raise ValueError(f"unknown kind {kind!r}")
    base = _LIBRARY_ROOT / kind
    if kind == "modules":
        # Modules are python subpackages under library/sources/.
        return _dirnames_in(_LIBRARY_ROOT / "sources") + sorted(
            p.stem for p in (_LIBRARY_ROOT / "sources").glob("*.py")
            if not p.stem.startswith("_") and p.stem != "__init__"
        )
    if kind == "templates":
        return _template_names_in(base)
    return _stems_in(base, ".yaml")


def resolve(ref: str) -> Path | str:
    """Resolve a `okg.<kind>.<name>` string to a concrete handle.

    Returns:
      - for yaml kinds: the `Path` to the .yaml file
      - for templates: the `Path` to the template directory
      - for modules: a Python import-dotted-path string like
        'okg.substrate.library.sources.jira'

    Raises LibraryRefMissing on unknown name. Raises
    SubstrateError on malformed ref string.
    """
    try:
        kind, name = parse_ref(ref)
    except ValueError as e:
        raise SubstrateError(
            f"malformed library ref: {e}",
            data={"ref": ref},
        ) from None

    alternatives = available_refs(kind)
    resolved_name = name
    if resolved_name not in alternatives:
        # For templates, honor `aliases:` in each template's
        # `template_meta.yaml` so renamed templates keep working
        # under their old name for back-compat. Per
        # `retire-pi-prefix-commands` Phase 4.3: `okg-workspace`
        # was renamed to `codebase-index`; the alias preserves
        # the prior CLI invocation.
        if kind == "templates":
            resolved_name = _template_alias_target(name, alternatives)
        if resolved_name is None or resolved_name not in alternatives:
            raise LibraryRefMissing(
                f"library ref {ref!r} not found; available "
                f"{kind} refs: {alternatives}",
                data={
                    "ref": ref,
                    "kind": kind,
                    "name": name,
                    "available": alternatives,
                },
                fix_hint=(
                    "use one of: "
                    + ", ".join(f"okg.{kind}.{n}" for n in alternatives[:10])
                    + ("..." if len(alternatives) > 10 else "")
                ),
            )

    if kind == "modules":
        return f"okg.substrate.library.sources.{resolved_name}"
    if kind == "templates":
        return _LIBRARY_ROOT / "templates" / resolved_name
    return _LIBRARY_ROOT / kind / f"{resolved_name}.yaml"


def _template_alias_target(
    requested: str, alternatives: list[str],
) -> str | None:
    """Look for a template whose `template_meta.yaml` lists
    `requested` in its `aliases:` list; return the canonical name
    if found.
    """
    import yaml  # local import: only needed on the alias path.
    for alt in alternatives:
        meta_path = _LIBRARY_ROOT / "templates" / alt / "template_meta.yaml"
        if not meta_path.is_file():
            continue
        try:
            meta = fastyaml.safe_load(meta_path.read_text()) or {}
        except yaml.YAMLError:
            continue
        aliases = meta.get("aliases") or []
        if requested in aliases:
            return alt
    return None
