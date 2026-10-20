"""MITRE CWE catalog source adapter — frozen local cwec XML.

Profile: `reference_catalog`.

Ingests the operator-downloaded CWE catalog (`cwec_v*.xml`, or the
`.xml.zip` archive MITRE publishes) with NO live network calls. The
catalog XML is namespace-versioned (`http://cwe.mitre.org/cwe-6`,
`cwe-7`, ...), so the parser is namespace-aware: it takes whatever
namespace the root element declares instead of pinning one release.

Emits `cwe` nodes (cwe_id like `CWE-79`, name, abstraction, first
Description text, created_at from the catalog's `Date` attribute when
present). Node ids use the shared shape in `_cti_snapshot.cwe_node_id`
so nvd_feeds' `has_weakness` edges resolve onto these nodes.

Per `okg-corpus-cti` (req.cti2.weakness) the catalog's OWN structure is
ingested too — cti-rcm's gold is frequently the parent or a sibling of
the CWE a description most obviously suggests, so a flat weakness list
cannot express the answer unit:

  * `cwe -child_of-> cwe` for every `Related_Weakness` with
    `Nature="ChildOf"` (1,318 rows in cwec v4.x);
  * `cwe -related_to-> cwe` for every OTHER nature (PeerOf,
    CanPrecede, Requires, CanAlsoBe, ...) — a distinct edge type, so a
    sibling relation can never be mistaken for an ancestor during a
    hierarchy walk. The per-nature histogram is reported in
    `last_run_stats`;
  * `cwe -has_member-> cwe` for every `Category/Relationships/
    Has_Member` row (4,260 in cwec v4.20), directed category ->
    member. A THIRD edge type, distinct from both of the above: a
    category groups entries by theme, so reading a membership as
    ancestry would answer an rcm walk with an unrelated cluster. The
    member is usually a weakness (4,152) but a category may also
    contain a category (108), which is why one narrowing covers both.
    The asserting `View_ID` rides on the edge;
  * `applicable_platforms` and `consequence_scopes` as structured
    (multivalued) attributes on the weakness.

A `Related_Weakness` or `Has_Member` pointing at a CWE id the catalog
does not define is SKIPPED AND COUNTED — never a stub node (it would
sit in the candidate pool the benchmark scores) and never a dangling
edge (it would block the publish), per `okg-corpus-design`. The
unresolved ids stay on the citing entry as
`unresolved_related_weaknesses` / `unresolved_members`.

An entry NO declaration in the catalog relates to another entry — no
Related_Weakness of its own, no Has_Member of its own, and nobody
naming it — carries `no_relations_reason`, stamped from what the
catalog states: `retired_entry` (`Status="Deprecated"`) or
`no_relationships_declared` (a live entry the catalog gives no
relations, e.g. an empty CERT/SEI mapping category). The stamp reads
the DECLARATIONS, never the emitted edge count: stamping from emission
would forgive exactly the parsing regression the attribute exists to
expose. Absence of the attribute therefore means "the catalog declared
a relation here", so an isolated node without a stamp is a lost edge,
not upstream emptiness. This is distinct from
`unresolved_related_weaknesses` / `unresolved_members`: "everything I
cited dangles" is a different fact from "I cited nothing", and only
the second is benign.

Category membership is ingested BECAUSE the categories themselves are:
NVD tags CVEs with legacy category ids (CWE-16, CWE-310), so category
nodes must exist for `has_weakness` to resolve, and a category node
with no way to reach its members is an isolated node the corpus
contract refuses.

NOT ingested, and deliberately so: `View` entries, Content_History,
demonstrative examples, detection methods and mitigation prose. None is
an answer unit of a scoreable CTIBench config; every one is recoverable
from the released catalog if a later child needs it.

Per `okg-bench-ctibench` (2026-07-27) and `okg-corpus-cti` (2026-07-28).
"""
from __future__ import annotations

import hashlib
import io
import re
import xml.etree.ElementTree as ET  # nosec B405 — parse goes through defusedxml
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._cti_snapshot import (
    cwe_node_id,
    normalize_timestamp,
    read_snapshot_bytes,
    record_set_from_facts,
)
from okg.substrate.library.sources._safe_xml import safe_xml_fromstring
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)


__all__ = ["CweCatalogSource"]


_TAG_RE = re.compile(r"^\{(?P<ns>.*)\}(?P<local>.*)$")

#: The one Related_Weakness nature that is an ANCESTOR relation. Every
#: other nature becomes `related_to` — never `child_of` — so a walk up
#: the hierarchy cannot drift sideways into a peer.
_CHILD_OF_NATURE = "ChildOf"

#: `Applicable_Platforms` children, in the order the catalog declares
#: them. The element name is kept as the entry's prefix so "Language:PHP"
#: and "Technology:Web Server" stay distinguishable.
_PLATFORM_TAGS = (
    "Language",
    "Operating_System",
    "Architecture",
    "Technology",
)

#: At or above this many Weakness elements, resolving ZERO ChildOf
#: relations FAILS the run (`okg-corpus-design`: "zero resolved
#: references in an input large enough to contain them fails the
#: build"). The released catalog carries 1,318 ChildOf rows.
_HIERARCHY_MIN_WEAKNESSES = 50

#: The same rule for membership: at or above this many Category
#: elements, resolving ZERO Has_Member rows FAILS the run. The released
#: catalog carries 422 categories, 368 of which declare members.
_MEMBERSHIP_MIN_CATEGORIES = 10


@dataclass
class CweCatalogSource:
    """CWE catalog adapter over one frozen local XML snapshot.

    `snapshot_path` is the downloaded catalog: a `.xml` file, or a
    `.zip` archive containing exactly one `.xml` member (the shape
    MITRE distributes). There is no live mode.
    """

    name: str = "cwe_catalog"
    profile: str = "reference_catalog"
    snapshot_path: Optional[Path | str] = None

    #: Counts from the most recent run: nodes, hierarchy edges per
    #: nature, and the skipped-dangling roster.
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    # Incremental-source contract (issue #323): one frozen local file
    # -> the content-hash floor over its bytes.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()
        # The run consumes exactly the hashed bytes of the one snapshot
        # file, so a matching token proves identical emission.
        self.probe_short_circuit_safe = True

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        def _items() -> list[tuple[str, Path]]:
            if self.snapshot_path is None:
                return []
            path = Path(self.snapshot_path)
            return [(str(path), path)] if path.is_file() else []

        return ContentHashProbe(
            content_items=_items,
            config={
                "snapshot_path": (
                    str(self.snapshot_path)
                    if self.snapshot_path is not None else None
                ),
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
            ),
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        if self.snapshot_path is None:
            raise SubstrateError(
                f"source {self.name!r}: pass snapshot_path",
                fix_hint="snapshot_path=Path('.../cwec_v4.13.xml') "
                         "(or the .xml.zip archive)",
            )
        path = Path(self.snapshot_path)
        raw = read_snapshot_bytes(path, adapter=self.name)
        xml_bytes = self._extract_xml_bytes(raw, path=path)
        facts = self._normalize_catalog(xml_bytes, path=path)

        completed_scope = mode in {"reconcile", "scope_complete", "release_new"}
        record_set = (
            record_set_from_facts(facts) if completed_scope else None
        )
        applied_raw_token = self.change_probe.token_from_item_hashes({
            str(path): hashlib.sha256(raw).hexdigest(),
        })

        def _stream() -> Iterator[NodeFact | EdgeFact]:
            yield from facts

        return SourceRun(
            facts=_stream(),
            completed_scope=completed_scope,
            applied_raw_token=applied_raw_token,
            applied_token_scope="global",
            applied_token_authority="exact",
            run_mode=mode,
            record_set=record_set,
        )

    # ---------- container + parse ----------

    def _extract_xml_bytes(self, raw: bytes, *, path: Path) -> bytes:
        """Unwrap the MITRE `.zip` distribution when given one.

        Container choice follows the file suffix, not sniffing: a
        `.zip` that isn't a readable archive with exactly one XML
        member is a corrupt snapshot and raises.
        """
        if path.suffix != ".zip":
            return raw
        try:
            archive = zipfile.ZipFile(io.BytesIO(raw))
        except zipfile.BadZipFile as exc:
            raise SubstrateError(
                f"source {self.name!r}: {path} has a .zip suffix but is "
                f"not a valid zip archive",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="re-download the CWE catalog archive",
            ) from exc
        with archive:
            members = [
                m for m in archive.namelist() if m.endswith(".xml")
            ]
            if len(members) != 1:
                raise SubstrateError(
                    f"source {self.name!r}: {path} must contain exactly "
                    f"one .xml member; found {len(members)}",
                    data={
                        "adapter": self.name,
                        "path": str(path),
                        "members": members,
                    },
                    fix_hint="use the official cwec .xml.zip archive",
                )
            return archive.read(members[0])

    def _normalize_catalog(
        self, xml_bytes: bytes, *, path: Path,
    ) -> list[NodeFact | EdgeFact]:
        """Parse the catalog in two phases.

        Phase 1 collects every catalog entry id (weaknesses AND
        categories, which NVD still tags CVEs with). Phase 2 emits
        nodes and hierarchy edges — a Related_Weakness can only be
        resolved against the COMPLETE id set, and an unresolvable one
        must be skipped and counted rather than stubbed.
        """
        try:
            root = safe_xml_fromstring(xml_bytes)
        except (ET.ParseError, ValueError) as exc:
            # defusedxml's entity/DTD refusals subclass ValueError;
            # both cases mean the snapshot is not a trustworthy
            # catalog file.
            raise SubstrateError(
                f"source {self.name!r}: {path} is not parseable "
                f"catalog XML",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="re-download the CWE catalog snapshot",
            ) from exc

        match = _TAG_RE.match(root.tag)
        ns_uri, local = (
            (match.group("ns"), match.group("local"))
            if match else ("", root.tag)
        )
        if local != "Weakness_Catalog":
            raise SubstrateError(
                f"source {self.name!r}: {path} root element is "
                f"{root.tag!r}, expected a CWE Weakness_Catalog",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="point snapshot_path at the MITRE cwec XML",
            )

        def q(tag: str) -> str:
            return f"{{{ns_uri}}}{tag}" if ns_uri else tag

        catalog_version = root.get("Version")
        catalog_date = root.get("Date")
        created_at = (
            normalize_timestamp(
                catalog_date,
                field="Date", context="Weakness_Catalog",
                adapter=self.name,
            )
            if catalog_date else None
        )
        source_revision: dict[str, Any] = {"catalog": "cwe"}
        if catalog_version:
            source_revision["catalog_version"] = catalog_version
        if catalog_date:
            source_revision["catalog_date"] = catalog_date

        weaknesses = root.findall(f"{q('Weaknesses')}/{q('Weakness')}")
        if not weaknesses:
            raise SubstrateError(
                f"source {self.name!r}: {path} contains zero Weakness "
                f"elements",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="check the download is the full cwec catalog, "
                         "not a fragment",
            )

        # Categories are referenceable catalog entries too: NVD still tags
        # CVEs with legacy category ids (observed: CWE-16, CWE-310 on the
        # 2026-07-27 recent feed), so omitting them leaves has_weakness
        # edges dangling and blocks the corpus publish. Views stay out —
        # nothing external references view ids as weaknesses.
        categories = root.findall(f"{q('Categories')}/{q('Category')}")

        # --- phase 1: every catalog id, before any edge is resolved ---
        entries: list[tuple[Any, str, str, str]] = []
        for weakness in weaknesses:
            entries.append((
                weakness,
                self._entry_id(weakness, kind="Weakness", path=path),
                self._entry_name(weakness, kind="Weakness", path=path),
                "weakness",
            ))
        for category in categories:
            entries.append((
                category,
                self._entry_id(category, kind="Category", path=path),
                self._entry_name(category, kind="Category", path=path),
                "category",
            ))
        known_ids = {cwe_id for _el, cwe_id, _name, _kind in entries}

        # --- phase 1b: what the catalog DECLARES about each entry ---
        # Read straight off the XML, before any id is resolved and
        # independent of the emission path below. This is what makes the
        # `no_relations_reason` stamp non-circular: stamping from the
        # emitted edge count would forgive precisely the parsing
        # regression the stamp exists to expose.
        declared_relations, cited_ids = self._declaration_census(entries, q)

        # --- phase 2: nodes + hierarchy edges ---
        facts: list[NodeFact | EdgeFact] = []
        edges: list[EdgeFact] = []
        emitted_edges: set[tuple[str, str, str]] = set()
        natures: dict[str, int] = {}
        edges_by_type: dict[str, int] = {}
        unresolved_total = 0
        self_referential = 0
        collapsed = 0
        related_rows = 0
        platform_entries = 0
        consequence_blocks = 0
        member_rows = 0
        member_unresolved_total = 0
        member_self_referential = 0
        member_collapsed = 0
        categories_with_members = 0
        no_relations_reasons: dict[str, int] = {}

        for element, cwe_id, name, kind in entries:
            text_el = element.find(
                q("Description") if kind == "weakness" else q("Summary")
            )
            description = (
                " ".join("".join(text_el.itertext()).split())
                if text_el is not None else None
            )
            attrs: dict[str, Any] = {
                "cwe_id": cwe_id,
                "name": name,
                "abstraction": (
                    element.get("Abstraction") if kind == "weakness" else None
                ),
                "kind": kind,
                "description": description,
            }
            if kind == "weakness":
                platforms = self._applicable_platforms(element, q)
                scopes = self._consequence_scopes(element, q)
                platform_entries += len(platforms)
                consequence_blocks += len(
                    element.findall(
                        f"{q('Common_Consequences')}/{q('Consequence')}"
                    )
                )
                attrs["applicable_platforms"] = platforms
                attrs["consequence_scopes"] = scopes
                node_edges, unresolved, stats = self._related_weaknesses(
                    element,
                    q,
                    cwe_id=cwe_id,
                    known_ids=known_ids,
                    emitted=emitted_edges,
                    source_revision=source_revision,
                    path=path,
                )
                edges.extend(node_edges)
                # The unresolvable ids stay on the citing weakness —
                # never as a stub node, never as a dangling edge.
                attrs["unresolved_related_weaknesses"] = unresolved
                unresolved_total += len(unresolved)
                related_rows += stats["rows"]
                self_referential += stats["self_referential"]
                collapsed += stats["collapsed"]
                for nature, count in stats["natures"].items():
                    natures[nature] = natures.get(nature, 0) + count
                for edge in node_edges:
                    edges_by_type[edge.edge_type] = (
                        edges_by_type.get(edge.edge_type, 0) + 1
                    )
            else:
                # A category's members. Emitted for the same reason the
                # category node itself is: NVD tags CVEs with category
                # ids, and a category no walk can descend from is an
                # isolated node.
                member_edges, unresolved_members, stats = (
                    self._category_members(
                        element,
                        q,
                        cwe_id=cwe_id,
                        known_ids=known_ids,
                        emitted=emitted_edges,
                        source_revision=source_revision,
                        path=path,
                    )
                )
                edges.extend(member_edges)
                attrs["unresolved_members"] = unresolved_members
                member_unresolved_total += len(unresolved_members)
                member_rows += stats["rows"]
                member_self_referential += stats["self_referential"]
                member_collapsed += stats["collapsed"]
                if stats["rows"]:
                    categories_with_members += 1
                for edge in member_edges:
                    edges_by_type[edge.edge_type] = (
                        edges_by_type.get(edge.edge_type, 0) + 1
                    )
            # The catalog knows WHY an entry has no relations, so it says
            # so here rather than leaving a downstream isolation probe to
            # infer it. Stamped only when NO declaration in the catalog
            # relates this entry to another one — neither its own rows
            # nor anyone else's naming it. Absence of this attribute
            # therefore means "the catalog DID declare a relation", so a
            # regression that drops an edge leaves an isolated node with
            # no stamp and the probe fires.
            reason = self._no_relations_reason(
                element,
                cwe_id=cwe_id,
                declared_relations=declared_relations,
                cited_ids=cited_ids,
            )
            if reason is not None:
                attrs["no_relations_reason"] = reason
                no_relations_reasons[reason] = (
                    no_relations_reasons.get(reason, 0) + 1
                )
            # created_at comes from the catalog-level Date attribute:
            # per-weakness creation dates live in Content_History,
            # which the snapshot contract does not require. Omit —
            # never fabricate — when the catalog has no Date.
            if created_at is not None:
                attrs["created_at"] = created_at
            facts.append(NodeFact(
                node_id=cwe_node_id(cwe_id),
                subtype="cwe",
                attrs=attrs,
                source_record_id={"cwe": cwe_id},
                source_revision=source_revision,
                op="I",
            ))

        if (
            len(weaknesses) >= _HIERARCHY_MIN_WEAKNESSES
            and not edges_by_type.get("child_of")
        ):
            raise SubstrateError(
                f"source {self.name!r}: {path} carries {len(weaknesses)} "
                f"weaknesses but resolved ZERO ChildOf relations "
                f"({related_rows} Related_Weakness row(s), "
                f"{unresolved_total} unresolved)",
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "weaknesses": len(weaknesses),
                    "related_weakness_rows": related_rows,
                    "related_weakness_unresolved": unresolved_total,
                    "natures": natures,
                },
                fix_hint="check the download is the full cwec catalog "
                         "(Related_Weaknesses included), not a fragment",
            )

        if (
            len(categories) >= _MEMBERSHIP_MIN_CATEGORIES
            and not edges_by_type.get("has_member")
        ):
            raise SubstrateError(
                f"source {self.name!r}: {path} carries {len(categories)} "
                f"categories but resolved ZERO Has_Member rows "
                f"({member_rows} row(s), {member_unresolved_total} "
                f"unresolved) — every category node would be isolated",
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "categories": len(categories),
                    "has_member_rows": member_rows,
                    "has_member_unresolved": member_unresolved_total,
                },
                fix_hint="check the download is the full cwec catalog "
                         "(Categories/Relationships included), not a "
                         "fragment",
            )

        facts.extend(edges)
        self.last_run_stats = {
            "weaknesses_emitted": len(weaknesses),
            "categories_emitted": len(categories),
            "nodes_emitted": len(entries),
            "related_weakness_rows": related_rows,
            "related_weakness_natures": natures,
            "edges_by_type": edges_by_type,
            "related_weakness_unresolved": unresolved_total,
            "related_weakness_self_referential": self_referential,
            "related_weakness_collapsed_duplicates": collapsed,
            "has_member_rows": member_rows,
            "categories_with_members": categories_with_members,
            "has_member_unresolved": member_unresolved_total,
            "has_member_self_referential": member_self_referential,
            "has_member_collapsed_duplicates": member_collapsed,
            "no_relations_reasons": no_relations_reasons,
            "applicable_platform_entries": platform_entries,
            "consequence_blocks": consequence_blocks,
        }
        return facts

    # ---------- element readers ----------

    def _entry_id(self, element: Any, *, kind: str, path: Path) -> str:
        raw_id = element.get("ID")
        if not raw_id:
            raise SubstrateError(
                f"source {self.name!r}: {path} has a {kind} "
                f"element missing its ID or Name attribute",
                data={"adapter": self.name, "path": str(path), "id": raw_id},
                fix_hint="re-download the CWE catalog snapshot",
            )
        return f"CWE-{raw_id}"

    def _entry_name(self, element: Any, *, kind: str, path: Path) -> str:
        name = element.get("Name")
        if not name:
            raise SubstrateError(
                f"source {self.name!r}: {path} has a {kind} "
                f"element missing its ID or Name attribute",
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "id": element.get("ID"),
                    "name": name,
                },
                fix_hint="re-download the CWE catalog snapshot",
            )
        return name

    @staticmethod
    def _applicable_platforms(element: Any, q: Any) -> list[str]:
        """`<Language Name="PHP"/>` -> `Language:PHP`, order preserved.

        A platform declared by Class rather than Name (`Not
        Language-Specific`) keeps the class string: it is what the
        catalog asserts, and dropping it would silently shrink the
        944 released entries.
        """
        out: list[str] = []
        block = element.find(q("Applicable_Platforms"))
        if block is None:
            return out
        for tag in _PLATFORM_TAGS:
            for entry in block.findall(q(tag)):
                value = entry.get("Name") or entry.get("Class")
                if value:
                    out.append(f"{tag}:{value}")
        return out

    @staticmethod
    def _consequence_scopes(element: Any, q: Any) -> list[str]:
        """Distinct `Common_Consequences//Scope` values, order preserved."""
        seen: dict[str, None] = {}
        for scope in element.findall(
            f"{q('Common_Consequences')}/{q('Consequence')}/{q('Scope')}"
        ):
            text = " ".join("".join(scope.itertext()).split())
            if text:
                seen.setdefault(text)
        return list(seen)

    def _declaration_census(
        self, entries: list[tuple[Any, str, str, str]], q: Any,
    ) -> tuple[dict[str, int], set[str]]:
        """Count, per entry, the relations the CATALOG declares.

        Returns `(declared_relations, cited_ids)`: how many rows each
        entry declares that name a DIFFERENT entry, and the set of ids
        some other entry's row names. Self-references are excluded from
        both — an entry that cites only itself is related to nothing,
        and counting it would hide that.

        Read from the XML attributes alone. Nothing here consults
        `known_ids`, the emitted edges, or the de-duplication set, so a
        resolution or emission regression cannot change the answer.
        """
        declared_relations: dict[str, int] = {}
        cited_ids: set[str] = set()
        for element, cwe_id, _name, _kind in entries:
            declared = 0
            rows = list(element.findall(
                f"{q('Related_Weaknesses')}/{q('Related_Weakness')}"
            )) + list(element.findall(
                f"{q('Relationships')}/{q('Has_Member')}"
            ))
            for row in rows:
                raw_target = row.get("CWE_ID")
                if not raw_target:
                    # Malformed; the emission pass raises on it by name.
                    continue
                target = f"CWE-{raw_target}"
                if target == cwe_id:
                    continue
                declared += 1
                cited_ids.add(target)
            declared_relations[cwe_id] = declared
        return declared_relations, cited_ids

    @staticmethod
    def _no_relations_reason(
        element: Any,
        *,
        cwe_id: str,
        declared_relations: dict[str, int],
        cited_ids: set[str],
    ) -> Optional[str]:
        """Why this entry has no relations, or `None` if it has some.

        `None` means the catalog DID declare a relation touching the
        entry — so if such a node still lands isolated, an edge was lost
        between declaration and emission, and no stamp forgives it.

        The two reasons are both catalog-stated, never inferred:

        * `retired_entry` — `Status="Deprecated"`. MITRE keeps the id
          for stability and strips its relationships; in cwec v4.20 all
          60 such entries are also name-prefixed `DEPRECATED:`.
        * `no_relationships_declared` — a live entry the catalog simply
          gives no relations, e.g. the CERT/SEI mapping categories that
          ship with an empty membership.
        """
        if declared_relations.get(cwe_id) or cwe_id in cited_ids:
            return None
        if element.get("Status") == "Deprecated":
            return "retired_entry"
        return "no_relationships_declared"

    def _category_members(
        self,
        element: Any,
        q: Any,
        *,
        cwe_id: str,
        known_ids: set[str],
        emitted: set[tuple[str, str, str]],
        source_revision: dict[str, Any],
        path: Path,
    ) -> tuple[list[EdgeFact], list[str], dict[str, Any]]:
        """`has_member`, directed category -> member.

        A THIRD edge type on purpose. A category is a thematic grouping,
        not a level of the hierarchy: `CWE-310 Cryptographic Issues`
        contains weaknesses that are not its descendants, so a walk that
        read membership as ancestry would answer an rcm question with an
        unrelated cluster. Held to the Related_Weakness contract — a
        member id the catalog does not define is skipped and counted,
        and its id is returned for the citing category's
        `unresolved_members`.
        """
        out: list[EdgeFact] = []
        unresolved: dict[str, None] = {}
        rows = 0
        self_referential = 0
        collapsed = 0
        src = cwe_node_id(cwe_id)
        for member in element.findall(
            f"{q('Relationships')}/{q('Has_Member')}"
        ):
            raw_target = member.get("CWE_ID")
            if not raw_target:
                raise SubstrateError(
                    f"source {self.name!r}: {path} has a Has_Member on "
                    f"{cwe_id} missing its CWE_ID attribute",
                    data={
                        "adapter": self.name,
                        "path": str(path),
                        "cwe": cwe_id,
                        "target": raw_target,
                    },
                    fix_hint="re-download the CWE catalog snapshot",
                )
            rows += 1
            target = f"CWE-{raw_target}"
            if target == cwe_id:
                self_referential += 1
                continue
            if target not in known_ids:
                unresolved.setdefault(target)
                continue
            key = (src, "has_member", cwe_node_id(target))
            if key in emitted:
                # The same membership declared under several views is
                # ONE edge; the collapse is counted, never inflated.
                collapsed += 1
                continue
            emitted.add(key)
            # The asserting view rides on the edge: membership is
            # view-scoped in the catalog, and dropping the view would
            # make two different groupings indistinguishable.
            attrs = {}
            view_id = member.get("View_ID")
            if view_id:
                attrs["view_id"] = f"CWE-VIEW-{view_id}"
            out.append(EdgeFact(
                src=src,
                dst=cwe_node_id(target),
                edge_type="has_member",
                attrs=attrs,
                source_record_id={"cwe": cwe_id, "member": target},
                source_revision=source_revision,
                op="I",
            ))
        return out, list(unresolved), {
            "rows": rows,
            "self_referential": self_referential,
            "collapsed": collapsed,
        }

    def _related_weaknesses(
        self,
        element: Any,
        q: Any,
        *,
        cwe_id: str,
        known_ids: set[str],
        emitted: set[tuple[str, str, str]],
        source_revision: dict[str, Any],
        path: Path,
    ) -> tuple[list[EdgeFact], list[str], dict[str, Any]]:
        """`child_of` for Nature=ChildOf, `related_to` for every other.

        The split is the point: cti-rcm walks ancestors, and a PeerOf
        row read as a parent would answer with a sibling. A row whose
        CWE_ID is not in the catalog is skipped and counted; its id is
        returned for the citing weakness's
        `unresolved_related_weaknesses`.
        """
        out: list[EdgeFact] = []
        unresolved: dict[str, None] = {}
        natures: dict[str, int] = {}
        rows = 0
        self_referential = 0
        collapsed = 0
        src = cwe_node_id(cwe_id)
        for related in element.findall(
            f"{q('Related_Weaknesses')}/{q('Related_Weakness')}"
        ):
            nature = related.get("Nature")
            raw_target = related.get("CWE_ID")
            if not nature or not raw_target:
                raise SubstrateError(
                    f"source {self.name!r}: {path} has a Related_Weakness "
                    f"on {cwe_id} missing its Nature or CWE_ID attribute",
                    data={
                        "adapter": self.name,
                        "path": str(path),
                        "cwe": cwe_id,
                        "nature": nature,
                        "target": raw_target,
                    },
                    fix_hint="re-download the CWE catalog snapshot",
                )
            rows += 1
            natures[nature] = natures.get(nature, 0) + 1
            target = f"CWE-{raw_target}"
            if target == cwe_id:
                self_referential += 1
                continue
            if target not in known_ids:
                unresolved.setdefault(target)
                continue
            edge_type = (
                "child_of" if nature == _CHILD_OF_NATURE else "related_to"
            )
            key = (src, edge_type, cwe_node_id(target))
            if key in emitted:
                # The same pair is declared under several views (1000,
                # 1003, 699); the graph edge is ONE edge, and the
                # collapse is counted rather than inflating the count.
                collapsed += 1
                continue
            emitted.add(key)
            out.append(EdgeFact(
                src=src,
                dst=cwe_node_id(target),
                edge_type=edge_type,
                attrs={"nature": nature},
                source_record_id={"cwe": cwe_id, "related": target},
                source_revision=source_revision,
                op="I",
            ))
        return out, list(unresolved), {
            "rows": rows,
            "natures": natures,
            "self_referential": self_referential,
            "collapsed": collapsed,
        }
