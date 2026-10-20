"""Persona registry — canonical seeds + deployment-registered.

Per `add-substrate-pre-join-mcp-framework` D4 / S-A7-R6 / S-A7-R7:
personas are first-class substrate-library data, shipped as one
YAML per persona under this directory. Each YAML declares the
attrs required by the `personas.Persona` subtype schema.

The substrate's MCP server bootstrap calls `load_seed_personas()`
to enumerate canonical seeds, and `load_deployment_personas(path)`
to enumerate deployment-registered ones from
`deployment.yaml`'s `personas_dir:`.

The loader resolves `system_prompt_ref` against the repo root
(for substrate seeds the path is `src/okg/substrate/library/personas/prompts/<slug>.md`)
and computes the prompt's sha256 so the Persona NodeFact carries
`system_prompt_content_hash` at emit time. This is the freshness
mechanism described in proposal.md R2: the substrate re-hashes on
every `review_as` call and emits an updated Persona NodeFact when
the file changes.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

import yaml
from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError


__all__ = [
    "PersonaSeed",
    "PersonaLoadError",
    "PersonaSlugCollision",
    "load_seed_personas",
    "load_deployment_personas",
    "compute_prompt_hash",
    "iter_persona_yaml_files",
    "SEEDS_DIR",
]


SEEDS_DIR = Path(__file__).resolve().parent
_REQUIRED_FIELDS = (
    "slug",
    "name",
    "description",
    "system_prompt_ref",
    "pre_join_tool_for_input",
    "default_request_type",
)


class PersonaLoadError(SubstrateError):
    """Raised when a persona YAML can't be parsed or validated."""

    code = "persona_load_error"


class PersonaSlugCollision(SubstrateError):
    """Raised when a deployment-registered persona slug collides with
    a substrate-shipped canonical slug. Per S-A7-R7 the substrate
    fails loud at MCP server bootstrap."""

    code = "persona_slug_collision"


@dataclass(frozen=True)
class PersonaSeed:
    """One persona row, ready to emit as a NodeFact at MCP bootstrap."""

    slug: str
    name: str
    description: str
    system_prompt_ref: str
    pre_join_tool_for_input: str
    default_request_type: str
    origin: str  # 'substrate' or 'deployment:<deployment_name>'
    source_path: Path
    system_prompt_content_hash: Optional[str] = None
    lifecycle_stage: Optional[str] = None

    @property
    def persona_id(self) -> str:
        slug = self.slug
        return slug if slug.startswith("persona:") else f"persona:{slug}"

    def as_attrs(self) -> dict[str, object]:
        attrs: dict[str, object] = {
            "persona_id": self.persona_id,
            "slug": self.slug,
            "name": self.name,
            "description": self.description,
            "system_prompt_ref": self.system_prompt_ref,
            "pre_join_tool_for_input": self.pre_join_tool_for_input,
            "default_request_type": self.default_request_type,
            "label": self.name,
        }
        if self.system_prompt_content_hash is not None:
            attrs["system_prompt_content_hash"] = (
                self.system_prompt_content_hash
            )
        if self.lifecycle_stage:
            attrs["lifecycle_stage"] = self.lifecycle_stage
        return attrs


def iter_persona_yaml_files(directory: Path) -> Iterator[Path]:
    """Yield every persona YAML in ``directory`` in sorted order.
    The ``prompts/`` subdirectory + dotfiles + ``__init__.py`` are
    skipped. Each YAML must be a top-level mapping; broken files
    raise PersonaLoadError so the operator can't silently ship a
    typo'd persona.
    """
    if not directory.is_dir():
        return
    for path in sorted(directory.glob("*.yaml")):
        if path.name.startswith("_") or path.name.startswith("."):
            continue
        yield path
    for path in sorted(directory.glob("*.yml")):
        if path.name.startswith("_") or path.name.startswith("."):
            continue
        yield path


def compute_prompt_hash(path: Path) -> str:
    """sha256 of a file's bytes. Used at emit time so the substrate
    knows whether the prompt content changed between catalog loads
    or `review_as` invocations (per R2 in the proposal)."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_one(path: Path, *, origin: str, repo_root: Path) -> PersonaSeed:
    try:
        data = fastyaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as e:
        raise PersonaLoadError(
            f"persona YAML at {path} could not be parsed: {e}",
            data={"path": str(path)},
        ) from e
    if not isinstance(data, dict):
        raise PersonaLoadError(
            f"persona YAML at {path} must be a top-level mapping",
            data={"path": str(path)},
        )
    missing = [k for k in _REQUIRED_FIELDS if not data.get(k)]
    if missing:
        raise PersonaLoadError(
            f"persona YAML at {path} missing required field(s): "
            f"{', '.join(missing)}",
            data={"path": str(path), "missing": missing},
            fix_hint="add the missing key(s); see "
            "src/okg/substrate/library/ontologies/personas/schema.yaml "
            "Persona attrs.",
        )

    slug = str(data["slug"]).strip()
    system_prompt_ref = str(data["system_prompt_ref"]).strip()

    # Resolve the prompt path. Substrate seeds use repo-relative
    # paths (e.g. `src/okg/substrate/library/personas/prompts/<slug>.md`);
    # deployment seeds may use absolute paths or repo-relative paths.
    prompt_path = Path(system_prompt_ref)
    if not prompt_path.is_absolute():
        prompt_path = (repo_root / system_prompt_ref).resolve()
    content_hash: Optional[str] = None
    if prompt_path.is_file():
        content_hash = compute_prompt_hash(prompt_path)
    # We don't fail-load on a missing prompt file at registry-load
    # time — `review_as` raises persona_prompt_missing when the call
    # tries to read it. The bootstrap path emits the Persona node
    # with a null content_hash and the operator log surfaces the
    # missing path via a debug message.

    return PersonaSeed(
        slug=slug,
        name=str(data["name"]).strip(),
        description=str(data["description"]).strip(),
        system_prompt_ref=system_prompt_ref,
        pre_join_tool_for_input=str(data["pre_join_tool_for_input"]).strip(),
        default_request_type=str(data["default_request_type"]).strip(),
        origin=origin,
        source_path=path,
        system_prompt_content_hash=content_hash,
        lifecycle_stage=(
            str(data["lifecycle_stage"]).strip()
            if data.get("lifecycle_stage") else None
        ),
    )


def load_seed_personas(*, repo_root: Optional[Path] = None) -> list[PersonaSeed]:
    """Load the substrate library's canonical persona seeds.

    repo_root defaults to the repo root inferred from this module's
    location; tests may pass a hermetic path.
    """
    if repo_root is None:
        # SEEDS_DIR is .../src/okg/substrate/library/personas
        # repo_root is .../  (4 parents up)
        repo_root = SEEDS_DIR.parents[4]
    out: list[PersonaSeed] = []
    for path in iter_persona_yaml_files(SEEDS_DIR):
        out.append(_parse_one(path, origin="substrate", repo_root=repo_root))
    return out


def load_deployment_personas(
    directory: Path,
    *,
    deployment_name: str,
    canonical_slugs: Optional[set[str]] = None,
    repo_root: Optional[Path] = None,
) -> list[PersonaSeed]:
    """Load personas from a deployment-registered directory.

    Raises PersonaSlugCollision if any deployment-registered slug
    matches a canonical one. Canonical slugs are supplied by the
    caller so tests can vary the canonical set.
    """
    if repo_root is None:
        repo_root = SEEDS_DIR.parents[4]
    out: list[PersonaSeed] = []
    canonical = canonical_slugs or set()
    for path in iter_persona_yaml_files(directory):
        seed = _parse_one(
            path,
            origin=f"deployment:{deployment_name}",
            repo_root=repo_root,
        )
        if seed.slug in canonical:
            raise PersonaSlugCollision(
                f"deployment persona at {path} uses canonical slug "
                f"{seed.slug!r}; pick a distinct slug",
                data={"slug": seed.slug, "path": str(path)},
            )
        out.append(seed)
    return out
