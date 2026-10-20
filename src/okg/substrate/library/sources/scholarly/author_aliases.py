"""AuthorAliasesSource — operator-curated author canonicalization.

Reads a YAML alias file at the deployment-configured path and emits
substrate alias-table rows (as `same_as` EdgeFacts between Person
nodes). Coexists with `IdentityAliasSource` (structured-source IDs
from DBLP / OpenAlex / ORCID); their alias contributions union.

Per `add-substrate-bibliographic-sources` (Phase 4 of impl-A4) and
umbrella D6 (no fuzzy LLM matching; all alias entries are
operator-curated).

YAML shape (a list of alias groups):

    - canonical: "Morgan Vale"
      aliases:
        - "M. Vale"
        - "Vale, M."
        - "M. R. Vale"
      external_ids:
        dblp: "v/MorganVale"
        orcid: "0000-0001-2345-6789"

The source emits `same_as` EdgeFacts pointing each alias' Person
node to the canonical Person node. It does NOT emit Person nodes;
the deployment SHALL run Person-emitting sources (BibReferences,
DBLP, etc.) before this one. Aliases referencing Persons that do
not exist in the active catalog will be rejected by the substrate
narrowing trigger — this is intentional ("fail loudly").
"""
from __future__ import annotations
from okg.substrate import fastyaml

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError

from ._common import slugify
from okg.substrate.library.sources.base import (
    EdgeFact,
    SourceRun,
)


__all__ = ["AuthorAliasesSource"]


def _person_node_id(name: str) -> str:
    return f"person:{slugify(name)}"


def _external_id_node_id(provider: str, value: str) -> str:
    """Stable node id for an external-id alias target.

    External ids (DBLP key, ORCID, OpenAlex ID) are represented as
    Person nodes with provider-prefixed slugs so other sources that
    emit by the same external id collide on node_id and dedupe in
    place. Matches the existing scholarly identity-alias convention.
    """
    return f"person:{provider}:{slugify(value)}"


@dataclass
class AuthorAliasesSource:
    aliases_yaml_path: str | Path | None = None
    target_subtypes: list[str] | tuple[str, ...] = ("person",)
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    name: str = "author_aliases"
    profile: str = "discovery_crawl"

    # Change-probe contract (#323): a single operator-curated alias YAML on
    # local disk, so the content-hash floor over that one file is the sound
    # probe. Every emitted `same_as` edge attr (canonical_name, alias_value,
    # alias_kind, group index) is derived purely from the YAML bytes; nothing
    # `now()`-derived reaches an emitted attr, so a content hash is sound. A
    # missing file at probe time RAISES (file_content_items / _load_yaml) ->
    # run_probe forces a full read rather than a silent no-change skip.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if not self.aliases_yaml_path:
            raise SubstrateError(
                "AuthorAliasesSource: aliases_yaml_path is required",
                fix_hint=(
                    "Set `aliases_yaml_path: <path>` in the deployment's "
                    "source_registry.yaml entry."
                ),
            )
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        from okg.substrate.library.sources.scholarly import _common

        def _items() -> list[tuple[str, Path]]:
            path = Path(self.aliases_yaml_path).expanduser()
            return [(str(path), path)] if path.is_file() else []

        return ContentHashProbe(
            content_items=_items,
            config={"aliases_yaml_path": str(self.aliases_yaml_path)},
            # `_common.slugify` shapes the emitted person: node ids, so fold it
            # into emit_version: a normalization change with unchanged YAML must
            # bust the token (else a false-unchanged after that code change).
            emit_targets=[sys.modules[__name__], _common],
        )

    def _load_yaml(self) -> list[dict[str, Any]]:
        import yaml

        path = Path(self.aliases_yaml_path).expanduser()
        if not path.is_file():
            raise SubstrateError(
                f"AuthorAliasesSource: aliases_yaml_path {str(path)!r} "
                f"does not resolve to a file",
                fix_hint="Verify the path; supports `~` expansion.",
                data={"aliases_yaml_path": str(path)},
            )
        try:
            data = fastyaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            raise SubstrateError(
                f"AuthorAliasesSource: malformed YAML at {str(path)!r}: "
                f"{exc}",
                fix_hint="Lint the YAML file before retrying.",
                data={"aliases_yaml_path": str(path), "yaml_error": str(exc)},
            ) from exc
        if not isinstance(data, list):
            raise SubstrateError(
                f"AuthorAliasesSource: aliases YAML at {str(path)!r} must "
                f"be a list of alias-group entries, got "
                f"{type(data).__name__}",
                fix_hint="Wrap entries in a top-level YAML list (`- ...`).",
            )
        return data

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        entries = self._load_yaml()
        stats: dict[str, Any] = {
            "groups": 0,
            "alias_edges": 0,
            "external_id_edges": 0,
        }

        def _facts() -> Iterator[EdgeFact]:
            emitted: set[tuple[str, str, str]] = set()
            for idx, group in enumerate(entries):
                if not isinstance(group, dict):
                    raise SubstrateError(
                        f"AuthorAliasesSource: entry {idx} is not a dict "
                        f"(got {type(group).__name__})",
                        fix_hint="Each YAML entry must be a mapping.",
                    )
                canonical = (group.get("canonical") or "").strip()
                if not canonical:
                    raise SubstrateError(
                        f"AuthorAliasesSource: entry {idx} is missing "
                        f"`canonical` field",
                        fix_hint="Every alias group requires a canonical name.",
                        data={"entry_index": idx},
                    )
                stats["groups"] += 1
                canonical_id = _person_node_id(canonical)

                # Alias name variants → same_as canonical
                aliases = group.get("aliases") or []
                if not isinstance(aliases, list):
                    raise SubstrateError(
                        f"AuthorAliasesSource: entry {idx} `aliases` must "
                        f"be a list",
                        fix_hint="Use YAML list syntax for aliases.",
                    )
                for alias_name in aliases:
                    alias_name = str(alias_name).strip()
                    if not alias_name:
                        continue
                    alias_id = _person_node_id(alias_name)
                    if alias_id == canonical_id:
                        continue
                    edge_key = (alias_id, canonical_id, "same_as")
                    if edge_key in emitted:
                        continue
                    emitted.add(edge_key)
                    stats["alias_edges"] += 1
                    yield EdgeFact(
                        src=alias_id,
                        dst=canonical_id,
                        edge_type="same_as",
                        attrs={
                            "source_provider": "author_aliases",
                            "alias_kind": "name_variant",
                            "alias_value": alias_name,
                            "canonical_name": canonical,
                        },
                        source_record_id={
                            "source": "author_aliases",
                            "group_index": idx,
                            "alias": alias_id,
                            "canonical": canonical_id,
                        },
                        source_revision={"run_id": run_id},
                    )

                # External-id variants → same_as canonical
                external_ids = group.get("external_ids") or {}
                if not isinstance(external_ids, dict):
                    raise SubstrateError(
                        f"AuthorAliasesSource: entry {idx} `external_ids` "
                        f"must be a mapping",
                        fix_hint="Use YAML mapping syntax for external_ids.",
                    )
                for provider, value in external_ids.items():
                    value = str(value).strip()
                    if not value:
                        continue
                    ext_id = _external_id_node_id(provider, value)
                    if ext_id == canonical_id:
                        continue
                    edge_key = (ext_id, canonical_id, "same_as")
                    if edge_key in emitted:
                        continue
                    emitted.add(edge_key)
                    stats["external_id_edges"] += 1
                    yield EdgeFact(
                        src=ext_id,
                        dst=canonical_id,
                        edge_type="same_as",
                        attrs={
                            "source_provider": "author_aliases",
                            "alias_kind": "external_id",
                            "alias_provider": provider,
                            "alias_value": value,
                            "canonical_name": canonical,
                        },
                        source_record_id={
                            "source": "author_aliases",
                            "group_index": idx,
                            "provider": provider,
                            "value": value,
                            "canonical": canonical_id,
                        },
                        source_revision={"run_id": run_id},
                    )
            self.last_run_stats = stats

        return SourceRun(
            facts=_facts(),
            completed_scope=mode in {"scope_complete", "reconcile"},
            run_mode=mode,
        )
