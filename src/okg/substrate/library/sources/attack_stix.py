"""MITRE ATT&CK STIX source adapter — frozen local bundle JSON.

Profile: `reference_catalog`.

Ingests an operator-downloaded ATT&CK STIX 2.x bundle (the
`enterprise-attack.json` / `mobile-attack.json` / `ics-attack.json`
artifacts from the attack-stix-data repository) with NO live network
calls. Non-bundle JSON raises — half-parsing an arbitrary STIX object
list would silently drop the tactic linkage.

Emits, per `okg-corpus-cti` (req.cti2.attack) — the bundle is a GRAPH,
so the adapter reads the relationship objects, not just the entity
lists:

  * `attack_technique` nodes from `attack-pattern` objects
    (technique_id like T1548 / T1548.002 from the mitre external
    reference, name, platforms, is_subtechnique, detection from
    x_mitre_detection, description, created_at from `created`);
  * `attack_tactic` nodes from `x-mitre-tactic` objects (tactic_id
    TAxxxx, name, shortname);
  * `attack_group` nodes from `intrusion-set` (Gxxxx),
    `attack_software` from `malware` AND `tool` (Sxxxx — ATT&CK
    relates the two identically, so they are ONE subtype carrying
    `software_type`), `attack_mitigation` from `course-of-action`
    (Mxxxx), `attack_campaign` from `campaign` (Cxxxx),
    `attack_detection_strategy` from `x-mitre-detection-strategy`
    (DETxxxx — the object modern ATT&CK makes the SOURCE of every
    `detects` relationship), `attack_analytic` from
    `x-mitre-analytic` (ANxxxx — the detection logic and telemetry
    prose ATT&CK moved off the technique when it retired
    `x_mitre_detection`), and `attack_data_component` from
    `x-mitre-data-component` (keyed by the uuid half of its STIX id;
    see `_stix_uuid`). Groups, software and campaigns carry their
    released `aliases` — the names threat-report prose actually uses,
    and cti-ate's only way in;
  * `attack_technique part_of attack_tactic` edges via each
    technique's `kill_chain_phases`, matched to tactic
    `x_mitre_shortname`;
  * the relationship-derived edges, one per declared triple:
    `attack_group|attack_software|attack_campaign -uses->
    attack_technique`, `attack_group|attack_campaign -uses->
    attack_software`, `attack_mitigation -mitigates->
    attack_technique`, `attack_detection_strategy -detects->
    attack_technique` (and `attack_data_component -detects->
    attack_technique`, the pre-detection-strategy shape, kept declared
    so an older bundle still parses), `attack_technique
    -subtechnique_of-> attack_technique`, `attack_campaign
    -attributed_to-> attack_group`;
  * the detection layer, which ATT&CK ships as EMBEDDED refs rather
    than relationship objects (see `_EMBEDDED_LINKS`):
    `attack_detection_strategy -contains-> attack_analytic` from
    `x_mitre_analytic_refs`, and `attack_analytic -references->
    attack_data_component` from each analytic's
    `x_mitre_log_source_references`. Reading only the relationship
    objects would leave every data component isolated, because in a
    current bundle nothing else touches one.

A relationship of an emitted type — or an embedded ref — whose resolved
endpoint pair matches NO declared triple RAISES, naming the observed
triple: an unanticipated pair is an unmodeled part of the ATT&CK graph,
and dropping it silently is exactly the failure this adapter was
rewritten to remove. Relationship types outside the emitted set are
counted by type in `last_run_stats`, never half-emitted.

Objects with `revoked: true` or `x_mitre_deprecated: true` are skipped
and COUNTED, for EVERY entity type. `revoked-by` relationships are
consumed ONLY as an exclusion signal (their source object is filtered
too) and are never emitted as edges. A relationship with a filtered or
absent endpoint — or an embedded ref with one — is dropped AND COUNTED,
never a stub node (it would enter the candidate pool the benchmark
scores) and never a dangling edge (it would block the publish), per
`okg-corpus-design`. A bundle whose live-technique count is zero raises
with the filtered count, and a bundle carrying at least
`_RELATIONSHIP_MIN_OBJECTS` relationships of an emitted type — or
embedded refs of one link kind — that resolves NONE of them raises, so
"no matches" can never masquerade as "none exist".

An ATT&CK node the released bundle relates to NOTHING carries
`no_relations_reason`, stamped from a census of the bundle's own
declarations (`_relation_census`) taken BEFORE any node is emitted and
independent of the emission passes:

  * `unreferenced_in_bundle` — no relationship object of an emitted type,
    no embedded ref, and no kill-chain phase in the bundle names this
    object. Measured on enterprise-attack 2026-07-27: 8 of 106
    `x-mitre-data-component` objects, which no analytic's
    `x_mitre_log_source_references` cites;
  * `referenced_only_by_excluded_objects` — every declaration naming it
    pairs it with an object that is revoked, deprecated, superseded by a
    `revoked-by`, or of a STIX type this adapter does not model, so the
    edge drops with its endpoint rather than being lost.

The attribute is OMITTED whenever the bundle declares at least one live
relation touching the object. Absence therefore means "the bundle DID
declare a relation here", so a node that lands isolated without a stamp
is a parsing regression and an isolation probe fires on it. The census
never reads the emitted edge list: stamping from emission is circular and
forgives exactly the regression the stamp exists to expose. It is
deliberately CONSERVATIVE — any mention at all counts as a declaration —
because an over-broad stamp forgives an isolate while a missing one only
costs a false alarm.

The name and slot are shared with `cwe_catalog.no_relations_reason` on
purpose: it is the same question ("why does this node relate to nothing,
per the upstream artifact"), so one isolation probe predicate covers both
sources. The value vocabularies are disjoint, so a value always names
which artifact stated it. It REPLACES the hardcoded
`AND n.subtype <> 'attack_data_component'` exemption the cti corpus
contract used to carry — a subtype-wide allow forgives a
regression-stranded data component exactly as readily as an upstream one.

Per `okg-bench-ctibench` (2026-07-27) and `okg-corpus-cti` (2026-07-28).
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._cti_snapshot import (
    load_json,
    normalize_timestamp,
    read_snapshot_bytes,
    record_set_from_facts,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)


__all__ = ["AttackStixSource"]


# The kill-chain / external-reference namespaces the three ATT&CK
# domains stamp on their objects.
_ATTACK_SOURCE_NAMES = frozenset({
    "mitre-attack",
    "mitre-mobile-attack",
    "mitre-ics-attack",
})
_TECHNIQUE_ID_RE = re.compile(r"^T\d{4}(?:\.\d{3})?$")
_TACTIC_ID_RE = re.compile(r"^TA\d{4}$")
_GROUP_ID_RE = re.compile(r"^G\d{4}$")
_SOFTWARE_ID_RE = re.compile(r"^S\d{4}$")
_MITIGATION_ID_RE = re.compile(r"^M\d{4}$")
_CAMPAIGN_ID_RE = re.compile(r"^C\d{4}$")
#: Detection strategies DO carry a mitre external id (DET0001..), unlike
#: data components: all 699 in the released enterprise bundle publish a
#: unique one, so the natural key is the ATT&CK id, not the STIX uuid.
_DETECTION_STRATEGY_ID_RE = re.compile(r"^DET\d{4}$")
_ANALYTIC_ID_RE = re.compile(r"^AN\d{4}$")

#: Description bound. Raised from 2,000 to cover the full released text
#: of every ATT&CK object (cti-ate matches report prose against
#: technique prose, and the old bound cut the longest descriptions
#: mid-sentence). Truncations are counted in `last_run_stats`, so a
#: bundle that ever exceeds the bound says so instead of losing text
#: silently.
_DESCRIPTION_MAX_CHARS = 20000

#: At or above this many relationship objects of an emitted type,
#: resolving ZERO of them FAILS the run (`okg-corpus-design`:
#: "zero resolved references in an input large enough to contain them
#: fails the build"). The real enterprise bundle carries 21,025.
_RELATIONSHIP_MIN_OBJECTS = 10


@dataclass(frozen=True)
class _EntitySpec:
    """How one STIX object type becomes one node subtype."""

    subtype: str
    id_attr: str
    #: `None` for x-mitre-data-component, which ATT&CK ships with NO
    #: external id — its natural key is the STIX uuid, recorded as
    #: such rather than invented.
    id_pattern: Optional[re.Pattern[str]]
    kind: str
    aliases: bool = False
    platforms: bool = False
    software_type: bool = False


#: The entity object types this adapter promotes to nodes. malware and
#: tool BOTH become `attack_software`: ATT&CK relates them identically
#: and cti-mcq does not distinguish them, so the distinction is kept as
#: the `software_type` attribute rather than as two subtypes.
_ENTITY_SPECS: dict[str, _EntitySpec] = {
    "intrusion-set": _EntitySpec(
        "attack_group", "group_id", _GROUP_ID_RE, "group", aliases=True,
    ),
    "malware": _EntitySpec(
        "attack_software", "software_id", _SOFTWARE_ID_RE, "software",
        aliases=True, platforms=True, software_type=True,
    ),
    "tool": _EntitySpec(
        "attack_software", "software_id", _SOFTWARE_ID_RE, "software",
        aliases=True, platforms=True, software_type=True,
    ),
    "course-of-action": _EntitySpec(
        "attack_mitigation", "mitigation_id", _MITIGATION_ID_RE,
        "mitigation",
    ),
    "campaign": _EntitySpec(
        "attack_campaign", "campaign_id", _CAMPAIGN_ID_RE, "campaign",
        aliases=True,
    ),
    "x-mitre-detection-strategy": _EntitySpec(
        "attack_detection_strategy", "detection_strategy_id",
        _DETECTION_STRATEGY_ID_RE, "detection strategy",
    ),
    "x-mitre-analytic": _EntitySpec(
        "attack_analytic", "analytic_id", _ANALYTIC_ID_RE, "analytic",
        platforms=True,
    ),
    "x-mitre-data-component": _EntitySpec(
        "attack_data_component", "data_component_id", None,
        "data component",
    ),
}

#: STIX relationship_type -> emitted edge_type. Every other
#: relationship_type is counted, not emitted; `revoked-by` is consumed
#: separately as an exclusion signal.
_RELATIONSHIP_EDGE_TYPES: dict[str, str] = {
    "uses": "uses",
    "mitigates": "mitigates",
    "detects": "detects",
    "subtechnique-of": "subtechnique_of",
    "attributed-to": "attributed_to",
}

@dataclass(frozen=True)
class _EmbeddedLink:
    """One structural link ATT&CK ships as an EMBEDDED ref rather than a
    relationship object.

    The detection layer is wired this way: a detection strategy names
    its analytics in `x_mitre_analytic_refs`, and each analytic names
    the data components its log sources come from in
    `x_mitre_log_source_references`. Nothing in the bundle restates
    either link as a `relationship`, so reading only the relationship
    objects leaves every data component isolated. These refs are
    published structure, so following them is ingestion, not inference.
    """

    src_stix_type: str
    edge_type: str
    #: Property on the source object holding the refs.
    property_name: str
    #: Key inside each entry holding the ref, for a list of objects;
    #: `None` when the property is a plain list of ref strings.
    ref_key: Optional[str]
    #: source_record_id keys, so an embedded edge traces back to the two
    #: STIX objects that asserted it (there is no relationship id).
    src_record_key: str
    dst_record_key: str


_EMBEDDED_LINKS: tuple[_EmbeddedLink, ...] = (
    _EmbeddedLink(
        "x-mitre-detection-strategy", "contains", "x_mitre_analytic_refs",
        None, "detection_strategy", "analytic",
    ),
    _EmbeddedLink(
        "x-mitre-analytic", "references", "x_mitre_log_source_references",
        "x_mitre_data_component_ref", "analytic", "data_component",
    ),
)

#: Exactly the triples the cti ontology module narrows. A resolved
#: relationship OR embedded ref outside this set raises rather than
#: being dropped.
_DECLARED_TRIPLES: frozenset[tuple[str, str, str]] = frozenset({
    ("attack_group", "uses", "attack_technique"),
    ("attack_software", "uses", "attack_technique"),
    ("attack_group", "uses", "attack_software"),
    ("attack_campaign", "uses", "attack_technique"),
    ("attack_campaign", "uses", "attack_software"),
    ("attack_mitigation", "mitigates", "attack_technique"),
    # Modern ATT&CK (spec 3.3.0) moved detection onto a dedicated
    # x-mitre-detection-strategy object: in the released enterprise
    # bundle ALL 697 `detects` relationships start there and NONE start
    # at a data component. The data-component shape is kept declared —
    # not deleted — so a pre-detection-strategy bundle still parses
    # instead of failing the endpoint-pair guard.
    ("attack_detection_strategy", "detects", "attack_technique"),
    ("attack_data_component", "detects", "attack_technique"),
    ("attack_technique", "subtechnique_of", "attack_technique"),
    ("attack_campaign", "attributed_to", "attack_group"),
    # The detection layer beneath a strategy, carried as embedded refs.
    ("attack_detection_strategy", "contains", "attack_analytic"),
    ("attack_analytic", "references", "attack_data_component"),
})

_ALIAS_KEYS = ("aliases", "x_mitre_aliases")

#: STIX object types this adapter promotes to nodes. An endpoint outside
#: this set can never become an edge, so a declaration pointing at one is
#: not a LIVE declaration for the purposes of `_relation_census`.
_MODELLED_STIX_TYPES: frozenset[str] = frozenset(
    {"attack-pattern", "x-mitre-tactic"} | set(_ENTITY_SPECS)
)


@dataclass(frozen=True)
class _RelationCensus:
    """What the RELEASED BUNDLE declares about each object, per STIX id.

    Built in one pass over the raw objects before any node is emitted, so
    it cannot be influenced by the emission passes. `declared` counts
    every mention of an id in a relationship of an emitted type, an
    embedded ref, or a kill-chain phase; `live` counts only the mentions
    whose OTHER participant is an object this adapter both models and does
    not filter — i.e. the mentions that can become an edge.
    """

    declared: dict[str, int]
    live: dict[str, int]

    def no_relations_reason(self, stix_id: str) -> Optional[str]:
        """Why the bundle relates this object to nothing, or `None`.

        `None` means the bundle DECLARED at least one live relation
        touching it, so a node that still lands isolated lost an edge
        between declaration and emission and no stamp forgives it.
        """
        if not self.declared.get(stix_id):
            return "unreferenced_in_bundle"
        if not self.live.get(stix_id):
            return "referenced_only_by_excluded_objects"
        return None


@dataclass(frozen=True)
class _Endpoint:
    """One resolved relationship endpoint.

    `state` is `ingested` (a node this run emits), `filtered` (a
    revoked/deprecated object), `foreign` (a live object of a type this
    adapter does not model) or `absent` (an id the bundle never
    defines).
    """

    state: str
    subtype: str
    node_id: Optional[str] = None


@dataclass
class AttackStixSource:
    """ATT&CK STIX bundle adapter over one frozen local JSON snapshot.

    `snapshot_path` is the downloaded STIX 2.x bundle. There is no
    live mode.
    """

    name: str = "attack_stix"
    profile: str = "reference_catalog"
    snapshot_path: Optional[Path | str] = None

    #: Counts from the most recent run: nodes per subtype, edges per
    #: declared triple, the filtered-object roster, and every dropped
    #: relationship. An empty result can never masquerade as a clean
    #: one because the reason is always in here.
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
                fix_hint="snapshot_path=Path('.../enterprise-attack.json')",
            )
        path = Path(self.snapshot_path)
        raw = read_snapshot_bytes(path, adapter=self.name)
        doc = load_json(raw, path=path, adapter=self.name)
        facts = self._normalize_bundle(doc, path=path)

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

    # ---------- bundle parsing ----------

    def _normalize_bundle(
        self, doc: Any, *, path: Path,
    ) -> list[NodeFact | EdgeFact]:
        """Parse the whole bundle: entities first, then relationships.

        One walk buckets the objects by STIX type (the enterprise
        bundle is ~40k objects, so every later lookup is a dict hit,
        never a rescan).
        """
        if (
            not isinstance(doc, dict)
            or doc.get("type") != "bundle"
            or not isinstance(doc.get("objects"), list)
        ):
            raise SubstrateError(
                f"source {self.name!r}: {path} is not a STIX bundle "
                f"({{'type': 'bundle', 'objects': [...]}})",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="point snapshot_path at an ATT&CK STIX 2.x "
                         "bundle download",
            )
        objects = doc["objects"]
        source_revision = {
            "catalog": "attack",
            "bundle_id": doc.get("id"),
        }

        by_type: dict[str, list[dict[str, Any]]] = {}
        object_type_by_id: dict[str, str] = {}
        for obj in objects:
            if not isinstance(obj, dict):
                continue
            obj_type = obj.get("type")
            if not isinstance(obj_type, str):
                continue
            by_type.setdefault(obj_type, []).append(obj)
            stix_id = obj.get("id")
            if isinstance(stix_id, str) and stix_id:
                object_type_by_id[stix_id] = obj_type

        relationships = by_type.get("relationship", [])
        # `revoked-by` is an EXCLUSION SIGNAL only: its source object is
        # superseded, so it is filtered exactly like `revoked: true`,
        # and the relationship itself never becomes an edge.
        revoked_by_sources: set[str] = set()
        for rel in relationships:
            if rel.get("relationship_type") != "revoked-by":
                continue
            src_ref, _dst_ref = self._relationship_refs(rel, path=path)
            revoked_by_sources.add(src_ref)

        # What the bundle DECLARES about each object. Read here, off the
        # raw objects, before a single node is emitted — that is what makes
        # the `no_relations_reason` stamp non-circular.
        census = self._relation_census(
            by_type, revoked_by_sources=revoked_by_sources,
        )
        no_relations_reasons: dict[str, int] = {}
        no_relations_by_subtype: dict[str, dict[str, int]] = {}

        def _stamp(
            obj: dict[str, Any], attrs: dict[str, Any], subtype: str,
        ) -> None:
            stix_id = obj.get("id")
            if not isinstance(stix_id, str) or not stix_id:
                return
            reason = census.no_relations_reason(stix_id)
            if reason is None:
                return
            attrs["no_relations_reason"] = reason
            no_relations_reasons[reason] = (
                no_relations_reasons.get(reason, 0) + 1
            )
            per_subtype = no_relations_by_subtype.setdefault(subtype, {})
            per_subtype[reason] = per_subtype.get(reason, 0) + 1

        state = _BundleState(revoked_by_sources=revoked_by_sources)
        facts: list[NodeFact | EdgeFact] = []

        # Pass 1 — tactics, so techniques' kill-chain phases resolve.
        tactic_ids_by_shortname: dict[str, str] = {}
        for obj in by_type.get("x-mitre-tactic", []):
            if self._filter(obj, state, "x-mitre-tactic"):
                continue
            tactic_id = self._mitre_external_id(
                obj, pattern=_TACTIC_ID_RE, kind="tactic", path=path,
            )
            name = self._required_str(obj, "name", context=tactic_id, path=path)
            shortname = self._required_str(
                obj, "x_mitre_shortname", context=tactic_id, path=path,
            )
            node_id = f"attack_tactic:mitre:{tactic_id}"
            tactic_ids_by_shortname[shortname] = node_id
            state.register(obj, subtype="attack_tactic", node_id=node_id)
            tactic_attrs: dict[str, Any] = {
                "tactic_id": tactic_id,
                "name": name,
                "shortname": shortname,
                "created_at": normalize_timestamp(
                    obj.get("created"),
                    field="created", context=tactic_id,
                    adapter=self.name,
                ),
            }
            _stamp(obj, tactic_attrs, "attack_tactic")
            facts.append(NodeFact(
                node_id=node_id,
                subtype="attack_tactic",
                attrs=tactic_attrs,
                source_record_id={"tactic": tactic_id},
                source_revision=source_revision,
                op="I",
            ))

        # Pass 2 — techniques + part_of edges.
        part_of_edges = 0
        for obj in by_type.get("attack-pattern", []):
            if self._filter(obj, state, "attack-pattern"):
                continue
            technique_id = self._mitre_external_id(
                obj, pattern=_TECHNIQUE_ID_RE, kind="technique", path=path,
            )
            name = self._required_str(
                obj, "name", context=technique_id, path=path,
            )
            platforms = obj.get("x_mitre_platforms") or []
            is_subtechnique = bool(
                obj.get("x_mitre_is_subtechnique", "." in technique_id)
            )
            node_id = f"attack_technique:mitre:{technique_id}"
            state.register(obj, subtype="attack_technique", node_id=node_id)
            technique_attrs: dict[str, Any] = {
                "technique_id": technique_id,
                "name": name,
                # Comma-joined like the github adapter's labels:
                # attrs stay flat scalars.
                "platforms": ",".join(str(p) for p in platforms),
                "is_subtechnique": is_subtechnique,
                # STIX description/detection are optional; absent
                # means honest null (the cvss-null pattern), never a
                # fabricated empty string.
                "description": self._bounded_text(
                    obj.get("description"), state,
                ),
                "detection": self._bounded_text(
                    obj.get("x_mitre_detection"), state,
                ),
                "created_at": normalize_timestamp(
                    obj.get("created"),
                    field="created", context=technique_id,
                    adapter=self.name,
                ),
            }
            _stamp(obj, technique_attrs, "attack_technique")
            facts.append(NodeFact(
                node_id=node_id,
                subtype="attack_technique",
                attrs=technique_attrs,
                source_record_id={"technique": technique_id},
                source_revision=source_revision,
                op="I",
            ))
            for edge in self._part_of_edges(
                obj,
                technique_id=technique_id,
                technique_node_id=node_id,
                tactic_ids_by_shortname=tactic_ids_by_shortname,
                source_revision=source_revision,
                path=path,
            ):
                part_of_edges += 1
                facts.append(edge)

        if state.nodes_by_subtype.get("attack_technique", 0) == 0:
            raise SubstrateError(
                f"source {self.name!r}: {path} contains zero live "
                f"attack-pattern objects "
                f"({state.filtered_total} revoked/deprecated object(s) "
                f"skipped)",
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "skipped": state.filtered_total,
                    "filtered_by_type": dict(state.filtered_by_type),
                },
                fix_hint="check the bundle is a full ATT&CK domain "
                         "download, not a fragment",
            )

        # Pass 3 — the entity types the relationship graph points at.
        for stix_type, spec in _ENTITY_SPECS.items():
            for obj in by_type.get(stix_type, []):
                if self._filter(obj, state, stix_type):
                    continue
                node = self._entity_node(
                    obj,
                    spec=spec,
                    stix_type=stix_type,
                    state=state,
                    source_revision=source_revision,
                    path=path,
                )
                _stamp(obj, node.attrs, spec.subtype)
                facts.append(node)

        # Pass 4 — the links ATT&CK ships as embedded refs. Runs after
        # every entity pass so both endpoints are already registered.
        facts.extend(self._embedded_edges(
            by_type,
            state=state,
            object_type_by_id=object_type_by_id,
            source_revision=source_revision,
            path=path,
        ))

        # Pass 5 — the relationship objects themselves.
        facts.extend(self._relationship_edges(
            relationships,
            state=state,
            object_type_by_id=object_type_by_id,
            source_revision=source_revision,
            path=path,
        ))

        self.last_run_stats = {
            "objects_total": len(objects),
            "nodes_emitted": sum(state.nodes_by_subtype.values()),
            "nodes_by_subtype": dict(state.nodes_by_subtype),
            "filtered_objects_total": state.filtered_total,
            "filtered_objects_by_type": dict(state.filtered_by_type),
            "revoked_by_exclusions": len(revoked_by_sources),
            "descriptions_truncated": state.truncated_descriptions,
            "part_of_edges": part_of_edges,
            "relationship_objects": len(relationships),
            "relationship_objects_by_type": dict(state.relationships_by_type),
            "relationship_candidates": state.relationship_candidates,
            "relationships_dropped_filtered_endpoint": (
                state.dropped_filtered_endpoint
            ),
            "relationships_dropped_absent_endpoint": (
                state.dropped_absent_endpoint
            ),
            "relationships_duplicate_skipped": state.duplicate_relationships,
            "relationship_edges": state.relationship_edges,
            "embedded_ref_candidates": state.embedded_ref_candidates,
            "embedded_refs_malformed": state.embedded_refs_malformed,
            "embedded_refs_dropped_filtered_endpoint": (
                state.embedded_dropped_filtered_endpoint
            ),
            "embedded_refs_dropped_absent_endpoint": (
                state.embedded_dropped_absent_endpoint
            ),
            "embedded_refs_duplicate_skipped": (
                state.embedded_duplicate_skipped
            ),
            "embedded_ref_edges": state.embedded_edges,
            # Covers BOTH the relationship-derived and the embedded-ref
            # triples: one roster the contract floors read.
            "edges_by_triple": {
                f"{src} {edge_type} {dst}": count
                for (src, edge_type, dst), count
                in sorted(state.edges_by_triple.items())
            },
            # WHY each stamped node relates to nothing, per the bundle.
            # A stamp nobody counts is a stamp nobody can audit for
            # discrimination against the connected complement.
            "no_relations_reasons": dict(sorted(no_relations_reasons.items())),
            "no_relations_by_subtype": {
                subtype: dict(sorted(reasons.items()))
                for subtype, reasons in sorted(no_relations_by_subtype.items())
            },
        }
        return facts

    # ---------- declaration census ----------

    def _relation_census(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        *,
        revoked_by_sources: set[str],
    ) -> _RelationCensus:
        """Count what the BUNDLE declares about each object.

        Reads the three ways ATT&CK states a relation this adapter emits —
        relationship objects of an emitted type, the embedded refs of
        `_EMBEDDED_LINKS`, and a technique's ATT&CK kill-chain phases —
        and never the emitted edge list. Deliberately generous: any
        mention counts as a declaration, and an id is `live` only when the
        object at the other end is one this adapter both models and does
        not filter. Over-counting declarations can only cost a false
        alarm; under-counting them would forgive a stranded node.
        """
        live_modelled: set[str] = set()
        for stix_type in _MODELLED_STIX_TYPES:
            for obj in by_type.get(stix_type, []):
                stix_id = obj.get("id")
                if not isinstance(stix_id, str) or not stix_id:
                    continue
                if self._is_skipped(obj) or stix_id in revoked_by_sources:
                    continue
                live_modelled.add(stix_id)

        declared: dict[str, int] = {}
        live: dict[str, int] = {}

        def _pair(left: Any, right: Any) -> None:
            if not (isinstance(left, str) and left):
                return
            if not (isinstance(right, str) and right):
                return
            declared[left] = declared.get(left, 0) + 1
            declared[right] = declared.get(right, 0) + 1
            if right in live_modelled:
                live[left] = live.get(left, 0) + 1
            if left in live_modelled:
                live[right] = live.get(right, 0) + 1

        for rel in by_type.get("relationship", []):
            if rel.get("relationship_type") not in _RELATIONSHIP_EDGE_TYPES:
                # `revoked-by` and the x-mitre housekeeping types never
                # become edges, so they declare nothing.
                continue
            _pair(rel.get("source_ref"), rel.get("target_ref"))

        for link in _EMBEDDED_LINKS:
            for obj in by_type.get(link.src_stix_type, []):
                for entry in obj.get(link.property_name) or []:
                    ref = entry if link.ref_key is None else (
                        entry.get(link.ref_key)
                        if isinstance(entry, dict) else None
                    )
                    _pair(obj.get("id"), ref)

        tactic_stix_by_shortname: dict[str, str] = {}
        for obj in by_type.get("x-mitre-tactic", []):
            shortname = obj.get("x_mitre_shortname")
            stix_id = obj.get("id")
            if (
                isinstance(shortname, str) and shortname
                and isinstance(stix_id, str) and stix_id
            ):
                tactic_stix_by_shortname.setdefault(shortname, stix_id)
        for obj in by_type.get("attack-pattern", []):
            for phase in obj.get("kill_chain_phases") or []:
                if not isinstance(phase, dict):
                    continue
                if phase.get("kill_chain_name") not in _ATTACK_SOURCE_NAMES:
                    continue
                _pair(
                    obj.get("id"),
                    tactic_stix_by_shortname.get(str(phase.get("phase_name"))),
                )

        return _RelationCensus(declared=declared, live=live)

    # ---------- entity emission ----------

    def _entity_node(
        self,
        obj: dict[str, Any],
        *,
        spec: _EntitySpec,
        stix_type: str,
        state: "_BundleState",
        source_revision: dict[str, Any],
        path: Path,
    ) -> NodeFact:
        if spec.id_pattern is not None:
            entity_id = self._mitre_external_id(
                obj, pattern=spec.id_pattern, kind=spec.kind, path=path,
            )
        else:
            entity_id = self._stix_uuid(obj, kind=spec.kind, path=path)
        name = self._required_str(obj, "name", context=entity_id, path=path)
        node_id = f"{spec.subtype}:mitre:{entity_id}"
        attrs: dict[str, Any] = {
            spec.id_attr: entity_id,
            "name": name,
        }
        if spec.software_type:
            # malware vs tool: kept as a value because ATT&CK relates
            # them identically, so splitting the subtype would split
            # the `uses` triples for no answerable difference.
            attrs["software_type"] = stix_type
        if spec.aliases:
            # The names report prose uses. Verbatim and ordered as
            # released — ATT&CK's first alias is the object's own name.
            attrs["aliases"] = self._aliases(obj)
        if spec.platforms:
            attrs["platforms"] = ",".join(
                str(p) for p in (obj.get("x_mitre_platforms") or [])
            )
        attrs["description"] = self._bounded_text(obj.get("description"), state)
        attrs["created_at"] = normalize_timestamp(
            obj.get("created"),
            field="created", context=entity_id, adapter=self.name,
        )
        state.register(obj, subtype=spec.subtype, node_id=node_id)
        return NodeFact(
            node_id=node_id,
            subtype=spec.subtype,
            attrs=attrs,
            source_record_id={spec.subtype: entity_id},
            source_revision=source_revision,
            op="I",
        )

    @staticmethod
    def _aliases(obj: dict[str, Any]) -> list[str]:
        """Released aliases, de-duplicated, order preserved.

        intrusion-set/campaign publish `aliases`; malware/tool publish
        `x_mitre_aliases`. Both keys are read so one attribute answers
        "what else is this called" for every subtype that has an
        answer.
        """
        seen: dict[str, None] = {}
        for key in _ALIAS_KEYS:
            for alias in obj.get(key) or []:
                if isinstance(alias, str) and alias:
                    seen.setdefault(alias)
        return list(seen)

    def _bounded_text(
        self, value: Any, state: "_BundleState",
    ) -> Optional[str]:
        if not value:
            return None
        text = str(value)
        if len(text) > _DESCRIPTION_MAX_CHARS:
            state.truncated_descriptions += 1
            return text[:_DESCRIPTION_MAX_CHARS]
        return text

    def _filter(
        self, obj: dict[str, Any], state: "_BundleState", stix_type: str,
    ) -> bool:
        """Filter revoked/deprecated/superseded objects, and count them."""
        stix_id = obj.get("id")
        superseded = (
            isinstance(stix_id, str) and stix_id in state.revoked_by_sources
        )
        if not (self._is_skipped(obj) or superseded):
            return False
        state.filtered_total += 1
        state.filtered_by_type[stix_type] = (
            state.filtered_by_type.get(stix_type, 0) + 1
        )
        if isinstance(stix_id, str):
            state.filtered_ids.add(stix_id)
        return True

    # ---------- embedded-ref emission ----------

    def _embedded_edges(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        *,
        state: "_BundleState",
        object_type_by_id: dict[str, str],
        source_revision: dict[str, Any],
        path: Path,
    ) -> list[EdgeFact]:
        """Edges from the refs ATT&CK embeds in the objects themselves.

        Held to exactly the contract the relationship pass is held to: a
        ref whose target was filtered or that the bundle never defines
        is skipped AND COUNTED (never a stub node, never a dangling
        edge), a resolved pair outside `_DECLARED_TRIPLES` RAISES, and a
        link kind with enough refs to contain a match that resolves NONE
        of them fails the run.
        """
        edges: list[EdgeFact] = []
        emitted: set[tuple[str, str, str]] = set()
        for link in _EMBEDDED_LINKS:
            candidates = 0
            resolved = 0
            for obj in by_type.get(link.src_stix_type, []):
                src_stix_id = str(obj.get("id"))
                hit = state.resolved.get(src_stix_id)
                if hit is None:
                    # Source object filtered (revoked/deprecated/
                    # superseded): already counted by `_filter`, and its
                    # embedded refs go with it rather than dangling.
                    continue
                src_subtype, src_node_id = hit
                for ref in self._embedded_refs(obj, link, state):
                    candidates += 1
                    state.embedded_ref_candidates += 1
                    dst = state.endpoint(ref, object_type_by_id)
                    if dst.state == "filtered":
                        state.embedded_dropped_filtered_endpoint += 1
                        continue
                    if dst.state == "absent":
                        state.embedded_dropped_absent_endpoint += 1
                        continue
                    triple = (src_subtype, link.edge_type, dst.subtype)
                    if triple not in _DECLARED_TRIPLES:
                        raise SubstrateError(
                            f"source {self.name!r}: {path} carries object "
                            f"{src_stix_id!r} whose embedded "
                            f"{link.property_name} ref {ref!r} resolves to "
                            f"the endpoint pair {src_subtype} "
                            f"-{link.edge_type}-> {dst.subtype}, which "
                            f"matches no declared triple",
                            data={
                                "adapter": self.name,
                                "path": str(path),
                                "source_object": src_stix_id,
                                "property": link.property_name,
                                "ref": ref,
                                "src_subtype": src_subtype,
                                "dst_subtype": dst.subtype,
                                "declared": sorted(
                                    " ".join(t) for t in _DECLARED_TRIPLES
                                ),
                            },
                            fix_hint="model the pair: add the subtype/"
                                     "narrowing to the cti module and this "
                                     "adapter's _DECLARED_TRIPLES, or drop "
                                     "the embedded link deliberately",
                        )
                    assert dst.node_id is not None
                    key = (src_node_id, link.edge_type, dst.node_id)
                    if key in emitted:
                        # One analytic names the same data component in
                        # several log sources; the graph edge is ONE edge.
                        state.embedded_duplicate_skipped += 1
                        continue
                    emitted.add(key)
                    resolved += 1
                    state.embedded_edges += 1
                    state.edges_by_triple[triple] = (
                        state.edges_by_triple.get(triple, 0) + 1
                    )
                    edges.append(EdgeFact(
                        src=src_node_id,
                        dst=dst.node_id,
                        edge_type=link.edge_type,
                        attrs={},
                        source_record_id={
                            link.src_record_key: src_stix_id,
                            link.dst_record_key: ref,
                        },
                        source_revision=source_revision,
                        op="I",
                    ))
            if candidates >= _RELATIONSHIP_MIN_OBJECTS and resolved == 0:
                raise SubstrateError(
                    f"source {self.name!r}: {path} carries {candidates} "
                    f"{link.src_stix_type} {link.property_name} ref(s) but "
                    f"resolved ZERO of them "
                    f"({state.embedded_dropped_filtered_endpoint} filtered, "
                    f"{state.embedded_dropped_absent_endpoint} absent)",
                    data={
                        "adapter": self.name,
                        "path": str(path),
                        "src_stix_type": link.src_stix_type,
                        "property": link.property_name,
                        "candidates": candidates,
                    },
                    fix_hint="the bundle is a fragment: download the "
                             "complete ATT&CK domain bundle (detection "
                             "strategies, analytics and data components "
                             "ship together)",
                )
        return edges

    @staticmethod
    def _embedded_refs(
        obj: dict[str, Any], link: _EmbeddedLink, state: "_BundleState",
    ) -> Iterator[str]:
        """The ref strings one embedded property holds.

        An entry carrying no usable ref is counted as malformed rather
        than passed over in silence.
        """
        for entry in obj.get(link.property_name) or []:
            ref = entry if link.ref_key is None else (
                entry.get(link.ref_key) if isinstance(entry, dict) else None
            )
            if isinstance(ref, str) and ref:
                yield ref
            else:
                state.embedded_refs_malformed += 1

    # ---------- relationship emission ----------

    def _relationship_edges(
        self,
        relationships: list[dict[str, Any]],
        *,
        state: "_BundleState",
        object_type_by_id: dict[str, str],
        source_revision: dict[str, Any],
        path: Path,
    ) -> list[EdgeFact]:
        edges: list[EdgeFact] = []
        emitted: set[tuple[str, str, str]] = set()
        for rel in relationships:
            rel_type = str(rel.get("relationship_type"))
            state.relationships_by_type[rel_type] = (
                state.relationships_by_type.get(rel_type, 0) + 1
            )
            edge_type = _RELATIONSHIP_EDGE_TYPES.get(rel_type)
            if edge_type is None:
                # Not an emitted relationship type (revoked-by,
                # x-mitre-* housekeeping). Counted above, never
                # half-emitted.
                continue
            state.relationship_candidates += 1
            src_ref, dst_ref = self._relationship_refs(rel, path=path)
            src = state.endpoint(src_ref, object_type_by_id)
            dst = state.endpoint(dst_ref, object_type_by_id)
            if src.state == "filtered" or dst.state == "filtered":
                state.dropped_filtered_endpoint += 1
                continue
            if src.state == "absent" or dst.state == "absent":
                # An id the bundle never defines: skipped AND counted,
                # never a stub node and never a dangling edge.
                state.dropped_absent_endpoint += 1
                continue
            triple = (src.subtype, edge_type, dst.subtype)
            if triple not in _DECLARED_TRIPLES:
                raise SubstrateError(
                    f"source {self.name!r}: {path} carries relationship "
                    f"{rel.get('id')!r} whose endpoint pair "
                    f"{src.subtype} -{edge_type}-> {dst.subtype} matches "
                    f"no declared triple",
                    data={
                        "adapter": self.name,
                        "path": str(path),
                        "relationship": rel.get("id"),
                        "relationship_type": rel_type,
                        "src_subtype": src.subtype,
                        "dst_subtype": dst.subtype,
                        "declared": sorted(
                            " ".join(t) for t in _DECLARED_TRIPLES
                        ),
                    },
                    fix_hint="model the pair: add the subtype/narrowing "
                             "to the cti module and this adapter's "
                             "_DECLARED_TRIPLES, or exclude the "
                             "relationship type deliberately",
                )
            assert src.node_id is not None and dst.node_id is not None
            key = (src.node_id, edge_type, dst.node_id)
            if key in emitted:
                # ATT&CK publishes one relationship object per citation,
                # so the same pair recurs; the graph edge is one edge.
                state.duplicate_relationships += 1
                continue
            emitted.add(key)
            state.relationship_edges += 1
            state.edges_by_triple[triple] = (
                state.edges_by_triple.get(triple, 0) + 1
            )
            edges.append(EdgeFact(
                src=src.node_id,
                dst=dst.node_id,
                edge_type=edge_type,
                attrs={},
                source_record_id={"relationship": rel.get("id")},
                source_revision=source_revision,
                op="I",
            ))

        if (
            state.relationship_candidates >= _RELATIONSHIP_MIN_OBJECTS
            and not edges
        ):
            raise SubstrateError(
                f"source {self.name!r}: {path} carries "
                f"{state.relationship_candidates} relationship object(s) of "
                f"an emitted type but resolved ZERO of them "
                f"({state.dropped_filtered_endpoint} filtered endpoint(s), "
                f"{state.dropped_absent_endpoint} absent endpoint(s))",
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "relationship_candidates": state.relationship_candidates,
                    "dropped_filtered_endpoint": (
                        state.dropped_filtered_endpoint
                    ),
                    "dropped_absent_endpoint": state.dropped_absent_endpoint,
                },
                fix_hint="the bundle is a fragment: download the complete "
                         "ATT&CK domain bundle (entities + relationships)",
            )
        return edges

    def _relationship_refs(
        self, rel: dict[str, Any], *, path: Path,
    ) -> tuple[str, str]:
        src_ref = rel.get("source_ref")
        dst_ref = rel.get("target_ref")
        if (
            not isinstance(src_ref, str) or not src_ref
            or not isinstance(dst_ref, str) or not dst_ref
        ):
            raise SubstrateError(
                f"source {self.name!r}: relationship {rel.get('id')!r} in "
                f"{path} is missing source_ref/target_ref",
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "relationship": rel.get("id"),
                    "relationship_type": rel.get("relationship_type"),
                },
                fix_hint="re-download the bundle; STIX relationships "
                         "always carry both endpoint refs",
            )
        return src_ref, dst_ref

    def _part_of_edges(
        self,
        obj: dict[str, Any],
        *,
        technique_id: str,
        technique_node_id: str,
        tactic_ids_by_shortname: dict[str, str],
        source_revision: dict[str, Any],
        path: Path,
    ) -> Iterator[EdgeFact]:
        seen: set[str] = set()
        for phase in obj.get("kill_chain_phases") or []:
            if not isinstance(phase, dict):
                continue
            if phase.get("kill_chain_name") not in _ATTACK_SOURCE_NAMES:
                # Foreign kill chains (e.g. lockheed-martin-cyber-kill-
                # chain) are out of the ATT&CK tactic vocabulary by
                # definition — not our edges to emit.
                continue
            phase_name = phase.get("phase_name")
            dst = tactic_ids_by_shortname.get(str(phase_name))
            if dst is None:
                # An in-vocabulary phase with no matching tactic means
                # the snapshot is missing part of the domain (tactics
                # ship in the same bundle) — refuse the partial graph.
                raise SubstrateError(
                    f"source {self.name!r}: technique {technique_id} in "
                    f"{path} references kill-chain phase {phase_name!r} "
                    f"but the bundle defines no tactic with that "
                    f"shortname",
                    data={
                        "adapter": self.name,
                        "path": str(path),
                        "technique": technique_id,
                        "phase_name": phase_name,
                    },
                    fix_hint="download the complete ATT&CK domain "
                             "bundle (tactics included)",
                )
            if dst in seen:
                continue
            seen.add(dst)
            yield EdgeFact(
                src=technique_node_id,
                dst=dst,
                edge_type="part_of",
                attrs={},
                source_record_id={
                    "technique": technique_id,
                    "phase": str(phase_name),
                },
                source_revision=source_revision,
                op="I",
            )

    # ---------- field extraction ----------

    @staticmethod
    def _is_skipped(obj: dict[str, Any]) -> bool:
        return bool(obj.get("revoked")) or bool(obj.get("x_mitre_deprecated"))

    def _stix_uuid(
        self, obj: dict[str, Any], *, kind: str, path: Path,
    ) -> str:
        """The uuid half of a STIX id, for objects with no ATT&CK id.

        x-mitre-data-component objects carried no external_id when this
        adapter was written; recent releases DO stamp one (DC0084), but
        the uuid is the one key present in every bundle vintage, so it
        stays the natural key — recorded as what it is rather than
        back-derived from the name (two data sources publish components
        of the same name). Switching the key to the DCxxxx id would
        rewrite every attack_data_component node id, so it is a pact
        decision, not a parse detail.
        """
        stix_id = obj.get("id")
        if not isinstance(stix_id, str) or "--" not in stix_id:
            raise SubstrateError(
                f"source {self.name!r}: {kind} object {stix_id!r} in "
                f"{path} has no usable STIX id",
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "stix_id": stix_id,
                    "kind": kind,
                },
                fix_hint="re-download the bundle; STIX objects always "
                         "carry a '<type>--<uuid>' id",
            )
        return stix_id.split("--", 1)[1]

    def _mitre_external_id(
        self,
        obj: dict[str, Any],
        *,
        pattern: re.Pattern[str],
        kind: str,
        path: Path,
    ) -> str:
        """The ATT&CK id (T1548.002 / TA0004 / G0016) from
        external_references.

        A live ATT&CK object without its mitre external id is
        unaddressable by the benchmark's answer key — malformed, so
        raise rather than skip.
        """
        observed: list[str] = []
        for ref in obj.get("external_references") or []:
            if not isinstance(ref, dict):
                continue
            if ref.get("source_name") not in _ATTACK_SOURCE_NAMES:
                continue
            external_id = ref.get("external_id")
            if not isinstance(external_id, str):
                continue
            observed.append(external_id)
            if pattern.match(external_id):
                return external_id
        raise SubstrateError(
            f"source {self.name!r}: {kind} object {obj.get('id')!r} in "
            f"{path} has no mitre external_id matching "
            f"{pattern.pattern!r} (observed: {observed or 'none'})",
            data={
                "adapter": self.name,
                "path": str(path),
                "stix_id": obj.get("id"),
                "kind": kind,
                "observed_external_ids": observed,
            },
            fix_hint="re-download the bundle; ATT&CK objects always "
                     "carry their mitre external reference",
        )

    def _required_str(
        self,
        obj: dict[str, Any],
        field_name: str,
        *,
        context: str,
        path: Path,
    ) -> str:
        value = obj.get(field_name)
        if not isinstance(value, str) or not value:
            raise SubstrateError(
                f"source {self.name!r}: {context} in {path} is missing "
                f"required field {field_name!r}",
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "context": context,
                    "field": field_name,
                },
                fix_hint="re-download the bundle",
            )
        return value


@dataclass
class _BundleState:
    """Per-run bookkeeping: what was emitted, filtered, and dropped.

    Every count here reaches `last_run_stats`, so an empty or shrunken
    result always carries its own explanation.
    """

    revoked_by_sources: set[str]
    resolved: dict[str, tuple[str, str]] = field(default_factory=dict)
    filtered_ids: set[str] = field(default_factory=set)
    nodes_by_subtype: dict[str, int] = field(default_factory=dict)
    filtered_by_type: dict[str, int] = field(default_factory=dict)
    relationships_by_type: dict[str, int] = field(default_factory=dict)
    edges_by_triple: dict[tuple[str, str, str], int] = field(
        default_factory=dict,
    )
    filtered_total: int = 0
    truncated_descriptions: int = 0
    relationship_candidates: int = 0
    dropped_filtered_endpoint: int = 0
    dropped_absent_endpoint: int = 0
    duplicate_relationships: int = 0
    relationship_edges: int = 0
    embedded_ref_candidates: int = 0
    embedded_refs_malformed: int = 0
    embedded_dropped_filtered_endpoint: int = 0
    embedded_dropped_absent_endpoint: int = 0
    embedded_duplicate_skipped: int = 0
    embedded_edges: int = 0

    def register(
        self, obj: dict[str, Any], *, subtype: str, node_id: str,
    ) -> None:
        stix_id = obj.get("id")
        if isinstance(stix_id, str) and stix_id:
            self.resolved[stix_id] = (subtype, node_id)
        self.nodes_by_subtype[subtype] = (
            self.nodes_by_subtype.get(subtype, 0) + 1
        )

    def endpoint(
        self, ref: str, object_type_by_id: dict[str, str],
    ) -> _Endpoint:
        hit = self.resolved.get(ref)
        if hit is not None:
            return _Endpoint("ingested", hit[0], hit[1])
        if ref in self.filtered_ids or ref in self.revoked_by_sources:
            return _Endpoint("filtered", "filtered")
        stix_type = object_type_by_id.get(ref)
        if stix_type is not None:
            # A live object of a type this adapter does not model. It is
            # NOT dropped quietly: a relationship touching it fails the
            # declared-triple check by name.
            return _Endpoint("foreign", f"stix:{stix_type}")
        return _Endpoint("absent", "absent")
