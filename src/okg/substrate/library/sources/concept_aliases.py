"""Operator-curated Concept alias source.

Per `extend-substrate-concepts-as-universal-hub`: Concept
canonicalization is operator-curated, never inferred. The
operator authors a YAML file declaring `canonical → aliases[]`
mappings; this source reads the file and populates the
substrate's `okg.aliases` table via the existing
`add_alias()` primitive. No fuzzy-merge, no LLM-judged
auto-aliasing.

YAML shape::

    aliases:
      - canonical: bitemporal-data
        aliases:
          - bitemporal-records
          - bitemporal-storage
      - canonical: happens-before-ordering
        aliases:
          - happens-before
          - causal-precedence

Each entry's `canonical` is the `Concept.name` of an
already-landed Concept row (operator-authored seed or
A2 extractor-emitted). Each alias string in `aliases:` is
written to `okg.aliases` mapping to the canonical's
`node_id`.

**Operator workflow for Concept canonicalization (the
contract spelled by R-A3-1):**

1. Each newly-emitted Concept lands with its own
   `node_id` and provenance (operator seed or A2's
   `concept_extractor`).
2. A7's discovery brief surfaces candidate alias pairs
   for operator review.
3. The operator edits `concept_aliases.yaml` to declare
   one slug as canonical and the others as aliases.
4. On next publish cycle, this source re-runs and the
   substrate alias table absorbs the new canonical →
   aliases mapping.

No substrate-side fuzzy-merge, no naming-heuristic
collapse. The operator owns canonicalization decisions;
the substrate executes them.

**Path is deployment config.** This source hardcodes no
default path. Deployments configure `aliases_path:` (and
`dsn:`) in their `source_registry.yaml`. Per umbrella D11,
no deployment-specific paths appear in this substrate-library
file; each deployment supplies its own location.

**Edge preservation across aliasing.** Aliasing is
read-side resolution via `okg.aliases`, not edge
rewriting in `edges_live`. Historical edges keep their
original `dst` node_id; MCP traversal treats alias and
canonical as the same canonical Concept via the alias
table. See design.md D-A3-3 for the bitemporal-ledger
rationale.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Optional

import psycopg
from okg.substrate import fastyaml

from okg.substrate.catalog.aliases import (
    active_catalog_version_id,
    add_alias,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import SourceRun
from okg.substrate.db.connection import connect as db_connect


__all__ = [
    "ConceptAliasesSource",
    "ConceptAliasesPathMissing",
    "ConceptAliasesParseError",
    "ConceptAliasesNoDsn",
    "parse_aliases_file",
]


class ConceptAliasesPathMissing(SubstrateError):
    code = "concept_aliases_path_missing"


class ConceptAliasesParseError(SubstrateError):
    code = "concept_aliases_parse_error"


class ConceptAliasesNoDsn(SubstrateError):
    code = "concept_aliases_no_dsn"


def parse_aliases_file(path: Path) -> list[dict[str, Any]]:
    """Read and validate the operator-curated aliases YAML.

    Returns a list of dicts with keys
    ``{canonical: str, aliases: list[str]}``. Raises
    ``ConceptAliasesParseError`` on malformed structure,
    duplicate canonical entries, or alias slugs that
    overlap canonical slugs in the same file.
    """
    if not path.is_file():
        raise ConceptAliasesPathMissing(
            f"concept aliases file not found: {path}",
            data={"path": str(path)},
            fix_hint=(
                "create the file at the configured aliases_path "
                "(operator-curated YAML, shape documented in "
                "concept_aliases.py module docstring) or remove "
                "the source's registration from "
                "source_registry.yaml"
            ),
        )

    with path.open() as f:
        doc = fastyaml.safe_load(f) or {}

    if not isinstance(doc, dict) or "aliases" not in doc:
        raise ConceptAliasesParseError(
            f"{path}: missing top-level 'aliases:' key",
            data={"path": str(path)},
            fix_hint=(
                "wrap entries under a top-level `aliases:` list"
            ),
        )

    entries_raw = doc["aliases"]
    if not isinstance(entries_raw, list):
        raise ConceptAliasesParseError(
            f"{path}: 'aliases:' must be a list",
            data={"path": str(path)},
        )

    out: list[dict[str, Any]] = []
    canonicals_seen: set[str] = set()
    alias_slugs_seen: dict[str, str] = {}

    for i, entry in enumerate(entries_raw):
        if not isinstance(entry, dict):
            raise ConceptAliasesParseError(
                f"{path}: aliases[{i}] must be a mapping",
                data={"path": str(path), "index": i},
            )
        canonical = entry.get("canonical")
        aliases = entry.get("aliases")
        if not isinstance(canonical, str) or not canonical:
            raise ConceptAliasesParseError(
                f"{path}: aliases[{i}] missing string 'canonical'",
                data={"path": str(path), "index": i},
            )
        if (
            not isinstance(aliases, list)
            or not aliases
            or not all(isinstance(a, str) and a for a in aliases)
        ):
            raise ConceptAliasesParseError(
                f"{path}: aliases[{i}] 'aliases:' must be a non-empty "
                f"list of strings",
                data={
                    "path": str(path),
                    "index": i,
                    "canonical": canonical,
                },
            )
        if canonical in canonicals_seen:
            raise ConceptAliasesParseError(
                f"{path}: duplicate canonical {canonical!r}",
                data={
                    "path": str(path),
                    "canonical": canonical,
                },
            )
        canonicals_seen.add(canonical)

        for a in aliases:
            if a == canonical:
                raise ConceptAliasesParseError(
                    f"{path}: alias {a!r} equals its own canonical",
                    data={
                        "path": str(path),
                        "canonical": canonical,
                        "alias": a,
                    },
                )
            if a in alias_slugs_seen:
                raise ConceptAliasesParseError(
                    f"{path}: alias {a!r} appears under both "
                    f"canonical {alias_slugs_seen[a]!r} and "
                    f"canonical {canonical!r}",
                    data={
                        "path": str(path),
                        "alias": a,
                        "canonicals": [
                            alias_slugs_seen[a], canonical,
                        ],
                    },
                )
            alias_slugs_seen[a] = canonical

        out.append({"canonical": canonical, "aliases": list(aliases)})

    return out


class ConceptAliasesSource:
    """SourceAdapter that pushes operator-curated Concept
    aliases into ``okg.aliases``.

    Unlike NodeFact-emitting sources, this source writes
    directly to the alias table via
    ``okg.substrate.catalog.aliases.add_alias`` because
    emitting a NodeFact for each alias slug would create
    duplicate Concept rows. Per umbrella D11 the path is
    deployment config; the DSN is supplied alongside.

    Configuration (in ``source_registry.yaml``)::

        sources:
          concept_aliases:
            class: okg.substrate.library.sources.concept_aliases.ConceptAliasesSource
            params:
              aliases_path: /absolute/path/to/concept_aliases.yaml
              dsn: ${OKG_DSN}

    The source's ``run()`` returns an empty ``SourceRun``
    (no facts emitted) but as a side effect upserts one
    ``okg.aliases`` row per `(alias, canonical_concept_node_id)`
    pair. Missing canonical Concepts (no live row with
    matching ``name``) are reported as a fatal error so the
    operator notices typos before extracted Concepts
    accumulate edges under unintended slugs.
    """

    name: str = "concept_aliases"
    # `reference_catalog` profile: the source is a
    # full-snapshot read of an operator-curated file. The
    # source declares the full alias set; deletions in the
    # YAML between runs retract the corresponding alias
    # rows via the substrate's source-cutover machinery
    # (see Risks / R-A3-2).
    profile: str = "reference_catalog"

    # Incremental-source contract (issue #323). The emitted state (the
    # `(alias -> canonical_node_id)` rows written via `add_alias`) is a function
    # of TWO inputs, so a single-input probe would be a forbidden
    # false-unchanged:
    #   1. the operator-curated YAML file (local bytes) -> ContentHashProbe;
    #   2. the live-graph resolution `canonical slug -> Concept node_id` (a DB
    #      read) -> MutableApiProbe. A canonical that was missing at run N (the
    #      run raised) and LANDS at run N+1 must re-fire even though the YAML is
    #      byte-identical; a canonical whose node_id moves (or disappears) must
    #      too. A YAML-only content hash would silently skip those.
    # The sound probe is therefore a UnionProbe of the two (changed iff either
    # is). The DB watermark `version_fn` raises on a missing DSN / unreachable
    # DB / malformed YAML -> the run_probe boundary maps it to changed=true (a
    # full read), never a silent no-change skip. The membership digest is
    # delete-sensitive: a removed `(slug, node_id)` pair drops out of the sorted
    # set and busts the token.
    change_probe_kind = "union"

    def __init__(
        self,
        *,
        aliases_path: str,
        dsn: Optional[str] = None,
    ) -> None:
        if not aliases_path:
            raise ConceptAliasesPathMissing(
                "ConceptAliasesSource requires aliases_path",
                fix_hint=(
                    "set `params.aliases_path: <absolute path>` "
                    "in source_registry.yaml"
                ),
            )
        # Honor `~` and `${HOME}`/env-var expansion the way other
        # substrate sources do (e.g. AuthorAliasesSource via
        # `Path.expanduser()`). Without this, registry strings using
        # user-home or env-var prefixes fail the `.is_file()` check
        # even when the file exists.
        from os.path import expanduser, expandvars
        self._aliases_path = Path(expanduser(expandvars(aliases_path)))
        # Per dogfood-canary DQ audit (2026-05-19): also expand env
        # vars in `dsn:`. The substrate's source_registry placeholder
        # substitution only handles `${repo_root}` /
        # `${deployment_dir}` / `${manifest_dir}` /
        # `${source_registry_dir}` — not arbitrary env vars. Without
        # this, `dsn: ${OKG_DSN}` arrived as the literal string
        # and psycopg.connect raised `missing "="` on the bogus DSN.
        self._dsn = expandvars(dsn) if dsn else dsn
        # Incremental-source contract (#323): built at the END of __init__ so
        # `_aliases_path` / `_dsn` are populated. See `change_probe_kind` above.
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.mutable_api_probe import (
            MutableApiProbe,
        )
        from okg.substrate.library.sources.union_probe import UnionProbe

        # Input 1: the operator-curated YAML file (local bytes). A single file,
        # so the content-hash floor over it is the sound, mtime-immune,
        # delete-sensitive probe for the curated declaration.
        def _yaml_items() -> list[tuple[str, Path]]:
            return (
                [(str(self._aliases_path), self._aliases_path)]
                if self._aliases_path.is_file()
                else []
            )

        yaml_probe = ContentHashProbe(
            content_items=_yaml_items,
            config={"aliases_path": str(self._aliases_path)},
            emit_targets=[sys.modules[__name__]],
        )

        # Input 2: the live-graph resolution `canonical slug -> Concept node_id`.
        # The alias rows written depend on which canonicals resolve and to which
        # node_ids, so a watermark over that bounded resolution closes the
        # YAML-unchanged-but-Concept-landed gap. A missing DSN / DB error / bad
        # YAML raises out of `version_fn` -> run_probe forces a full read.
        resolution_probe = MutableApiProbe(
            version_fn=self._canonical_resolution_watermark,
            config={"aliases_path": str(self._aliases_path)},
            emit_targets=[
                sys.modules[__name__],
                _apply_aliases,
            ],
            content_kind="version_token",
        )

        return UnionProbe([yaml_probe, resolution_probe])

    def _canonical_resolution_watermark(self) -> str:
        """Membership digest of the live `(canonical_slug, node_id)` resolution.

        Reads the same `subtype='concept'` slug/name resolution that
        ``_apply_aliases`` performs, over exactly the canonicals declared in the
        current YAML, and returns a sorted, membership-sensitive hash. Advances
        when a declared canonical lands, moves node_id, or disappears -- the
        emission-determining graph state the YAML content cannot see.

        Raises (no DSN / unreachable DB / malformed YAML / missing file) so the
        ``run_probe`` boundary maps the failure to ``changed=True`` (a full
        read), never a silent no-change skip.
        """
        import hashlib

        if self._dsn is None:
            raise ConceptAliasesNoDsn(
                "ConceptAliasesSource change-probe requires a dsn to resolve "
                "canonical Concepts",
                fix_hint=(
                    "supply `params.dsn: ${OKG_DSN}` (or another "
                    "deployment-resolved DSN) in source_registry.yaml"
                ),
            )

        # parse_aliases_file raises on a missing file / malformed YAML -> the
        # probe error propagates (changed=true), which also covers the case
        # where the YAML became readable/unreadable between cycles.
        entries = parse_aliases_file(self._aliases_path)
        canonicals = sorted({e["canonical"] for e in entries})

        from okg.substrate.graph_store import ChronosGraphStore

        with db_connect(self._dsn, autocommit=True) as conn:
            # The emitted alias rows are stamped with the active catalog version
            # (add_alias -> catalog_version_id) and the read-side resolver filters
            # `WHERE catalog_version_id = max(applied cv)`, so a `catalog load
            # --apply` bump (cv N->N+1) makes prior-cv alias rows invisible and
            # the source MUST re-stamp them at the new cv. cv is therefore an
            # emission-determining input; fold it into the token so a bump busts
            # it even when the YAML + the concept resolution are byte-identical
            # (otherwise the gate would silently skip and the aliases would
            # vanish from resolution until an unrelated change forces a re-read).
            cv = active_catalog_version_id(conn)
            graph = ChronosGraphStore.from_connection(
                conn, ensure_schema=False
            ).checkout()
            pairs: list[tuple[str, str]] = []
            if canonicals:
                rows = graph.query(
                    "SELECT coalesce(attrs->>'slug', attrs->>'name') AS key, "
                    "       node_id "
                    "  FROM okg.graph_nodes "
                    " WHERE subtype = 'concept' "
                    "   AND (attrs->>'slug' = ANY(:canonicals) "
                    "        OR attrs->>'name' = ANY(:canonicals))",
                    {"canonicals": canonicals},
                )
                pairs = sorted(
                    (str(row["key"]), str(row["node_id"])) for row in rows
                )

        digest = hashlib.sha256()
        digest.update(f"cv={cv}".encode("utf-8"))
        # Encode membership (count) so a dropped/added resolution is caught even
        # if the remaining per-pair shas are unchanged.
        digest.update(f"n={len(pairs)}".encode("utf-8"))
        for key, node_id in pairs:
            digest.update(b"\x00")
            digest.update(key.encode("utf-8"))
            digest.update(b"\x00")
            digest.update(node_id.encode("utf-8"))
        return digest.hexdigest()

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        sync_scope: Optional[Mapping[str, Any]] = None,
    ) -> SourceRun:
        """Parse the YAML and write alias rows.

        Returns an empty ``SourceRun`` (no NodeFact / EdgeFact
        emission). The alias-table writes are a side effect
        applied directly to the substrate DB via
        ``add_alias()``.

        Idempotent: re-running with an unchanged YAML produces
        zero new alias rows (ON CONFLICT DO NOTHING in
        ``add_alias``). Removed YAML entries are NOT
        automatically retracted by this source; operator
        invokes ``okg alias remove`` for explicit deletion.
        See R-A3-2 of design.md.
        """
        if self._dsn is None:
            raise ConceptAliasesNoDsn(
                "ConceptAliasesSource requires a dsn to apply aliases",
                fix_hint=(
                    "supply `params.dsn: ${OKG_DSN}` (or another "
                    "deployment-resolved DSN) in source_registry.yaml"
                ),
            )

        entries = parse_aliases_file(self._aliases_path)

        with db_connect(self._dsn, autocommit=True) as conn:
            _apply_aliases(conn, entries=entries)

        return SourceRun(facts=[])


def _apply_aliases(
    conn: psycopg.Connection,
    *,
    entries: list[dict[str, Any]],
) -> dict[str, Any]:
    """Resolve each canonical name to a live Concept node_id
    and insert one alias row per declared alias slug.

    Fails loudly if any canonical name has no matching live
    Concept row (typo, or the operator declared an alias
    before the canonical landed). Per the no-silent-fallback
    discipline.
    """
    cv = active_catalog_version_id(conn)
    missing_canonicals: list[str] = []
    name_to_node_id: dict[str, str] = {}
    # Per dogfood-canary DQ audit (2026-05-19): match on `slug`
    # (kebab-case identity) not `name` (human-readable). The
    # operator's concept_aliases.yaml uses slugs (per file
    # docstring), and the canonical Concept node attrs include
    # both. Falling back to `name` keeps back-compat with any
    # operator who happens to have entered the human form.
    #
    # Chronos cutover: "does this canonical Concept exist now" is a
    # current-default-branch graph read (the legacy query used
    # `valid_to_gen IS NULL` with no generation pin), so it resolves
    # through a Chronos checkout over okg.graph_nodes; the visibility
    # predicate is dropped (the checkout session injects it). The
    # okg.aliases writes below stay on the raw connection -- aliases
    # is a catalog table, not a Chronos-versioned graph table.
    from okg.substrate.graph_store import ChronosGraphStore
    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False
    ).checkout()
    canonicals = [e["canonical"] for e in entries]
    if canonicals:
        rows = graph.query(
            "SELECT coalesce(attrs->>'slug', attrs->>'name') AS key, "
            "       node_id "
            "  FROM okg.graph_nodes "
            " WHERE subtype = 'concept' "
            "   AND (attrs->>'slug' = ANY(:canonicals) "
            "        OR attrs->>'name' = ANY(:canonicals))",
            {"canonicals": canonicals},
        )
        for row in rows:
            name_to_node_id[row["key"]] = row["node_id"]

    for entry in entries:
        if entry["canonical"] not in name_to_node_id:
            missing_canonicals.append(entry["canonical"])

    if missing_canonicals:
        raise SubstrateError(
            f"ConceptAliasesSource: {len(missing_canonicals)} "
            f"canonical Concept name(s) have no live row: "
            f"{missing_canonicals!r}",
            data={
                "missing_canonicals": missing_canonicals,
                "catalog_version_id": cv,
            },
            fix_hint=(
                "Each canonical name in concept_aliases.yaml must "
                "match a Concept node already in nodes_live (from "
                "seed.yaml or A2's concept_extractor). Land the "
                "Concept first, then re-run."
            ),
        )

    inserted = 0
    skipped = 0
    for entry in entries:
        canonical_node_id = name_to_node_id[entry["canonical"]]
        for alias in entry["aliases"]:
            result = add_alias(
                conn,
                alias=alias,
                canonical=canonical_node_id,
            )
            if result.get("inserted"):
                inserted += 1
            else:
                skipped += 1

    return {
        "catalog_version_id": cv,
        "alias_rows_inserted": inserted,
        "alias_rows_already_present": skipped,
    }
