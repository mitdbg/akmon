"""EDGAR/XBRL filing source adapter — frozen local filing document sets.

Profile: `reference_catalog`.

The financial vertical's ingestion. Consumes a `docsets_root` directory
of per-filing XBRL document sets — one subdirectory per filing holding
`<doc_kind>.xml` files (`schema.xml`, `instance.xml`, `calculation.xml`,
`presentation.xml`, ...) — the exact layout
`FinAuditingAdapter.export_docsets` writes when re-splitting the
FinAuditing dataset rows, and equally the layout an operator produces
by unpacking filings fetched from EDGAR. NO live network calls.

Emits per filing:
- one `filing` node (filing_id = the directory slug, doc kinds present,
  per-filing parse stats);
- `xbrl_concept` nodes for concepts declared in the schema document
  (xsd:element name + targetNamespace) plus concepts referenced by
  facts, calculation / presentation / definition arcs and context
  members (marked `declared: false` — standard-taxonomy concepts such
  as us-gaap ones are referenced, not re-declared, by filer extensions),
  carrying the label linkbase's standard label as `label` and its
  documentation text as `documentation`;
- `xbrl_fact` nodes from the instance document (concept, context ref,
  unit, value);
- `xbrl_context` nodes from the instance document's contexts (entity
  identifier / scheme, period_type, typed instant / start / end dates);
- `xbrl_statement` nodes, one per presentation-linkbase extended link
  role (the filing's statements);
- edges: `filing -contains-> xbrl_fact`,
  `xbrl_fact -reports-> xbrl_concept`,
  `xbrl_fact -in_context-> xbrl_context` (exactly one per fact),
  `filing -contains-> xbrl_context`,
  `xbrl_context -has_member-> xbrl_concept` (explicit dimension
  members),
  `filing -contains-> xbrl_statement`,
  `xbrl_statement -presents-> xbrl_concept` (`order` preserved),
  `xbrl_concept -presentation_parent_of-> xbrl_concept` (parent->child),
  `xbrl_concept -dim_related_to-> xbrl_concept` (definition arcs, the
  XBRL `arcrole` carried as an EDGE ATTRIBUTE — ONE edge type, not one
  per arcrole, because the arcrole vocabulary is open to filer
  extension and a per-arcrole model would reject an unanticipated
  arcrole at insert as a narrowing_violation), and calculation-arc
  edges `xbrl_concept -calc_sums_to-> xbrl_concept` (src = the
  component concept, dst = the concept it sums into, `weight` on the
  edge).

Labels are ATTRIBUTES, never nodes: a label names a concept and is
never an endpoint a question traverses through, so promoting it would
add one degree-1 node per concept per role and inflate the isolated
fraction without creating a single new path. A concept no ingested
label linkbase names carries NO `label` key — never an empty string
and never the element name substituted, which would be
default-value-as-fallback and would make the prose->concept join
untestable.

Fail-loud contract: an empty/missing `docsets_root`, a filing dir with
no instance document, and unparseable XML all raise `SubstrateError`.
Individual concepts/facts/arcs/locators/contexts lacking required
identifying fields are skipped WITH per-filing counts (stamped on the
filing node and kept in `last_run_stats`); zero total facts across all
filings raises — a run that parsed everything into nothing is a broken
snapshot, not a degradation to accept.

Per `okg-bench-domain-adapters` / req.dom.finauditing (2026-07-27) and
`okg-corpus-xbrl` / req.xbrl.linkbases + req.xbrl.context (2026-07-28).
"""
from __future__ import annotations

import hashlib
import re
import xml.etree.ElementTree as ET  # nosec B405 — parse goes through defusedxml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._cti_snapshot import (
    read_snapshot_bytes,
    record_set_from_facts,
)
from okg.substrate.library.sources._safe_xml import safe_xml_fromstring
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)


__all__ = ["EdgarXbrlSource"]


_TAG_RE = re.compile(r"^\{(?P<ns>.*)\}(?P<local>.*)$")

_XSD_NS = "http://www.w3.org/2001/XMLSchema"
_XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
_XLINK_NS = "http://www.w3.org/1999/xlink"
_XBRLI_NS = "http://www.xbrl.org/2003/instance"
_LINKBASE_NS = "http://www.xbrl.org/2003/linkbase"

#: XBRL 2.1 label roles (the specification's label-element section).
#: Only these two become concept attributes: the standard label is the concept's name in prose (the
#: FinSM join), the documentation label is its definition. Terse,
#: verbose, total and period-start/end labels are presentation
#: variants of the same name — counted as ignored, not folded into
#: `label`, so the attribute means exactly one role.
_STANDARD_LABEL_ROLE = "http://www.xbrl.org/2003/role/label"
_DOCUMENTATION_LABEL_ROLE = "http://www.xbrl.org/2003/role/documentation"
_LABEL_ROLE_ATTRS = {
    _STANDARD_LABEL_ROLE: "label",
    _DOCUMENTATION_LABEL_ROLE: "documentation",
}

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _split_tag(tag: str) -> tuple[str, str]:
    match = _TAG_RE.match(tag)
    return (match.group("ns"), match.group("local")) if match else ("", tag)


def _local_name(element: ET.Element) -> str:
    return _split_tag(element.tag)[1]


def _element_text(element: ET.Element) -> str:
    return "".join(element.itertext()).strip()


def _float_or_none(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


#: Doc kinds this source knows how to read. Anything else in a docset is
#: COUNTED as unrecognized rather than silently ignored — a document the
#: source cannot see is indistinguishable from one that is not there.
_KNOWN_DOC_KINDS = {
    "schema", "instance", "calculation", "presentation", "label",
    "definition",
}


def _canonical_doc_kind(stem: str) -> str:
    """Normalise a docset filename stem to a canonical doc kind.

    FinAuditing's markers yield `calculation_linkbase`, `presentation_linkbase`,
    `label_linkbase`, `definition_linkbase`; unpacked EDGAR filings and this
    source's own contract use the bare kind. Before this normalisation the
    source looked up `documents["calculation"]`, never matched
    `calculation_linkbase`, and emitted ZERO calculation arcs from 25 filings
    that all had a parseable calculation linkbase — a silent no-op that left
    the graph without the calculation relationships FinMR's task asks about.
    """
    kind = stem.strip().lower()
    if kind.endswith("_linkbase"):
        kind = kind[: -len("_linkbase")]
    return kind


_XMLNS_RE = re.compile(r'xmlns:([A-Za-z_][\w.-]*)\s*=\s*"([^"]+)"')


def _collect_prefix_namespaces(paths: list[Path | None]) -> dict[str, str]:
    """prefix -> namespace URI, read from the filing's own xmlns
    declarations. Text-scanned rather than parsed: the declarations are
    needed even when the document itself is truncated mid-tag, which is
    exactly the case for ~20% of dataset-exported documents."""
    prefix_ns: dict[str, str] = {}
    for path in paths:
        if path is None or not path.is_file():
            continue
        try:
            head = path.read_text(errors="replace")[:8192]
        except OSError:
            continue
        for prefix, uri in _XMLNS_RE.findall(head):
            prefix_ns.setdefault(prefix, uri)
    return prefix_ns


def concept_node_id(namespace: str, name: str) -> str:
    """Node id for an XBRL concept, e.g.
    `xbrl_concept:http://fasb.org/us-gaap/2023#Assets`. Concepts are
    identified by (namespace, name) — the same concept referenced from
    two filings resolves to ONE node."""
    return f"xbrl_concept:{namespace}#{name}"


def statement_node_id(filing_slug: str, role_uri: str) -> str:
    """Node id for a presentation extended link role (a statement).

    Identity is (filing, role URI): the role URI is the filer's own,
    and the same role in two filings is two statements, one per filing.
    The URI is digested because it is long and carries characters that
    make a node id unreadable, and it stays on the node as `role_uri`.
    """
    digest = hashlib.sha256(role_uri.encode("utf-8")).hexdigest()[:16]
    return f"xbrl_statement:{filing_slug}:{digest}"


def context_node_id(filing_slug: str, context_ref: str) -> str:
    """Node id for an instance context. Context ids are unique only
    WITHIN an instance document, so the filing slug is part of the
    identity — two filings' `C_0000320193_20230101_20231231` are two
    contexts, not one."""
    return f"xbrl_context:{filing_slug}:{context_ref}"


@dataclass
class _ConceptPool:
    """Run-scoped registry of the `xbrl_concept` nodes emitted so far.

    A concept is identified by (namespace, name), so the same concept
    referenced from two filings is ONE node. That is why labels are
    applied through the pool rather than at construction: a filing's
    label linkbase must be able to name a concept some other document
    already created, and a label read before its concept exists must
    still land when it is created.
    """

    source_revision: dict[str, Any]
    #: node id -> the emitted NodeFact (whole run).
    index: dict[str, NodeFact] = field(default_factory=dict)
    #: node id -> {"label"|"documentation": text}. First writer wins, so
    #: the run is deterministic under the sorted filing order.
    labels: dict[str, dict[str, str]] = field(default_factory=dict)
    #: node id -> slug of the filing whose label linkbase carried it,
    #: so a label naming a concept NO document ever created is charged
    #: to a filing rather than vanishing.
    label_origin: dict[str, str] = field(default_factory=dict)
    #: concepts first emitted during the filing currently being read.
    new: list[NodeFact] = field(default_factory=list)

    def start_filing(self) -> None:
        self.new = []

    def ensure(
        self, namespace: str, name: str, *, declared: bool
    ) -> str:
        """Return the concept's node id, emitting the node on first sight."""
        node_id = concept_node_id(namespace, name)
        existing = self.index.get(node_id)
        if existing is not None:
            return node_id
        attrs: dict[str, Any] = {
            "concept_id": node_id,
            "name": name,
            "namespace": namespace,
            "declared": declared,
        }
        # A label already read for this concept is applied at creation;
        # a concept with no label carries no `label` key at all.
        attrs.update(self.labels.get(node_id, {}))
        node = NodeFact(
            node_id=node_id,
            subtype="xbrl_concept",
            attrs=attrs,
            source_record_id={"concept": node_id},
            source_revision=self.source_revision,
            op="I",
        )
        self.index[node_id] = node
        self.new.append(node)
        return node_id

    def record_label(
        self, node_id: str, attr: str, text: str, *, slug: str
    ) -> None:
        bucket = self.labels.setdefault(node_id, {})
        if attr in bucket:
            return
        bucket[attr] = text
        self.label_origin.setdefault(node_id, slug)
        node = self.index.get(node_id)
        if node is not None:
            # NodeFact is frozen; its attrs dict is not. The node has
            # not been yielded yet — run() collects every filing's facts
            # before streaming them.
            node.attrs.setdefault(attr, text)


@dataclass
class EdgarXbrlSource:
    """EDGAR/XBRL adapter over one frozen docsets directory.

    `docsets_root` holds one subdirectory per filing:
    `<filing_slug>/<doc_kind>.xml`. `repo` is an optional corpus label
    (e.g. the benchmark/dataset the docsets were re-split from, or an
    EDGAR accession scope) recorded in every fact's source_revision.
    There is no live mode.
    """

    name: str = "edgar_xbrl"
    profile: str = "reference_catalog"
    repo: Optional[str] = None
    docsets_root: Optional[Path | str] = None
    #: "raise" (default — a real filing that will not parse is corrupt) or
    #: "skip_counted" (a dataset export whose documents are capped upstream:
    #: skip the document, count it, never pretend it parsed).
    truncated_documents: str = "raise"

    # Incremental-source contract (issue #323): frozen local files ->
    # the content-hash floor over the CURRENT directory expansion — an
    # edited, added, or deleted document busts the token.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()
        # The run consumes exactly the bytes the probe hashes (complete
        # immutable local files), so a matching token proves the emitted
        # facts would be identical — short-circuit is safe.
        self.probe_short_circuit_safe = True
        #: per-filing skip/emit counts from the most recent run.
        self.last_run_stats: dict[str, dict[str, int]] = {}

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        def _items() -> list[tuple[str, Path]]:
            return [(str(p), p) for p in self._document_files()]

        return ContentHashProbe(
            content_items=_items,
            config={
                "repo": self.repo,
                "docsets_root": (
                    str(self.docsets_root)
                    if self.docsets_root is not None else None
                ),
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
            ),
        )

    def _document_files(self) -> list[Path]:
        if self.docsets_root is None:
            return []
        root = Path(self.docsets_root)
        if not root.is_dir():
            return []
        return sorted(
            p for p in root.glob("*/*.xml") if p.is_file()
        )

    def _filing_dirs(self) -> list[Path]:
        root = Path(self.docsets_root)
        if not root.is_dir():
            raise SubstrateError(
                f"source {self.name!r}: docsets_root {root} is not a "
                f"directory",
                data={"adapter": self.name, "docsets_root": str(root)},
                fix_hint="export the filing document sets first "
                         "(FinAuditingAdapter.export_docsets), or point "
                         "docsets_root at the unpacked EDGAR filings",
            )
        dirs = sorted(p for p in root.iterdir() if p.is_dir())
        if not dirs:
            raise SubstrateError(
                f"source {self.name!r}: docsets_root {root} contains no "
                f"filing directories",
                data={"adapter": self.name, "docsets_root": str(root)},
                fix_hint="each filing is one subdirectory of "
                         "<doc_kind>.xml files; an empty corpus is a "
                         "broken snapshot, not an ingestable one",
            )
        return dirs

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        if self.docsets_root is None:
            raise SubstrateError(
                f"source {self.name!r}: pass docsets_root",
                fix_hint="docsets_root=Path('.../docsets') as exported by "
                         "FinAuditingAdapter.export_docsets",
            )
        raw_by_file: dict[str, str] = {}
        facts: list[NodeFact | EdgeFact] = []
        stats: dict[str, dict[str, int]] = {}
        filing_nodes: dict[str, NodeFact] = {}
        pool = _ConceptPool(source_revision={"corpus": self.repo or "docsets"})
        total_facts = 0
        for filing_dir in self._filing_dirs():
            filing_facts, filing_stats, filing_node = self._normalize_filing(
                filing_dir, pool=pool, raw_by_file=raw_by_file,
            )
            facts.extend(filing_facts)
            stats[filing_dir.name] = filing_stats
            filing_nodes[filing_dir.name] = filing_node
            total_facts += filing_stats["facts_emitted"]
        self._reconcile_labels(pool, stats=stats, filing_nodes=filing_nodes)
        if total_facts == 0:
            raise SubstrateError(
                f"source {self.name!r}: zero XBRL facts across all "
                f"{len(stats)} filings in {self.docsets_root}",
                data={"adapter": self.name, "stats": stats},
                fix_hint="the instance documents parsed but yielded no "
                         "facts — the snapshot is malformed; re-export it",
            )
        self.last_run_stats = stats

        completed_scope = mode in {"reconcile", "scope_complete", "release_new"}
        record_set = record_set_from_facts(facts) if completed_scope else None
        applied_raw_token = self.change_probe.token_from_item_hashes({
            path: hashlib.sha256(raw.encode("utf-8")).hexdigest()
            for path, raw in raw_by_file.items()
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

    @staticmethod
    def _reconcile_labels(
        pool: _ConceptPool,
        *,
        stats: dict[str, dict[str, int]],
        filing_nodes: dict[str, NodeFact],
    ) -> None:
        """Charge every label that named a concept NO document in the run
        ever created back to the filing whose label linkbase carried it.

        Such a label is NOT promoted into a concept node: a concept that
        exists only because something named it is a degree-0 node that
        inflates the isolated fraction without creating a path. It is
        counted instead, so an unread schema shows up as a number rather
        than as a quietly thinner label coverage.
        """
        for node_id, slug in pool.label_origin.items():
            if node_id in pool.index:
                continue
            stats[slug]["labels_unmatched"] += 1
            filing_nodes[slug].attrs["labels_unmatched"] += 1

    # ---------- per-filing normalization ----------

    def _normalize_filing(
        self,
        filing_dir: Path,
        *,
        pool: _ConceptPool,
        raw_by_file: dict[str, str],
    ) -> tuple[list[NodeFact | EdgeFact], dict[str, int], NodeFact]:
        slug = filing_dir.name
        pool.start_filing()
        documents = {
            _canonical_doc_kind(p.stem): p
            for p in sorted(filing_dir.glob("*.xml")) if p.is_file()
        }
        unrecognized = sorted(set(documents) - _KNOWN_DOC_KINDS)
        if "instance" not in documents:
            raise SubstrateError(
                f"source {self.name!r}: filing {slug!r} has no instance "
                f"document ({filing_dir}/instance.xml)",
                data={
                    "adapter": self.name,
                    "filing": slug,
                    "doc_kinds": sorted(documents),
                },
                fix_hint="a filing without its instance document has no "
                         "facts to ingest; re-export the docset",
            )
        filing_id = f"filing:edgar:{slug}"
        source_revision = pool.source_revision
        stats = {
            "facts_emitted": 0,
            "facts_skipped": 0,
            #: fact candidates skipped because their contextRef named no
            #: context element in the instance — the reason breakdown for
            #: part of `facts_skipped`, never a dangling in_context edge.
            "facts_no_context": 0,
            "concepts_declared": 0,
            "concepts_skipped": 0,
            "calc_arcs_emitted": 0,
            "calc_arcs_skipped": 0,
            "calc_locators_unresolved": 0,
            "contexts_emitted": 0,
            "contexts_skipped": 0,
            "context_periods_unparseable": 0,
            "context_members_emitted": 0,
            "context_members_skipped": 0,
            #: typed (non-explicit) dimension members, which this source
            #: does not model — counted so the omission is visible.
            "context_typed_members_unread": 0,
            "statements_emitted": 0,
            "statement_links_skipped": 0,
            "presents_emitted": 0,
            "presentation_arcs_emitted": 0,
            "presentation_arcs_skipped": 0,
            "presentation_locators_unresolved": 0,
            "definition_arcs_emitted": 0,
            "definition_arcs_skipped": 0,
            "definition_locators_unresolved": 0,
            "labels_parsed": 0,
            "labels_skipped": 0,
            "label_arcs_skipped": 0,
            "label_roles_ignored": 0,
            "label_locators_unresolved": 0,
            #: labels that resolved to a concept id no document in the RUN
            #: created; filled in by _reconcile_labels after every filing.
            "labels_unmatched": 0,
            # Documents skipped because they are unparseable AND the source
            # runs in skip_counted mode (a dataset export whose embedded
            # documents are capped upstream). Stamped on the filing node so
            # a thin corpus is never mistaken for a complete one.
            "documents_truncated": 0,
            # ...and WHICH kind was truncated. The total alone cannot say
            # whether the filing lost its instance or merely its label
            # linkbase, and `structurally_stranded` turns on exactly that
            # distinction — see the filing node below.
            "documents_truncated_schema": 0,
            "documents_truncated_instance": 0,
            "documents_truncated_calculation": 0,
            "documents_truncated_presentation": 0,
            "documents_truncated_definition": 0,
            "documents_truncated_label": 0,
            #: docset files whose kind this source cannot read (see
            #: _KNOWN_DOC_KINDS) — counted, never silently dropped.
            "documents_unrecognized": len(unrecognized),
        }

        # Prefix->URI declarations come from the filing's own documents, so
        # an arc's "us-gaap_Foo" locator and a context member's
        # "us-gaap:FooMember" QName resolve onto the SAME concept nodes the
        # instance facts reference. Instance first: its declarations are
        # the ones the facts themselves are written against.
        prefix_ns = _collect_prefix_namespaces([
            documents.get("instance"), documents.get("schema"),
            documents.get("calculation"), documents.get("presentation"),
            documents.get("definition"), documents.get("label"),
        ])

        fragment_map: dict[str, tuple[str, str]] = {}
        role_definitions: dict[str, str] = {}
        declared_concepts: list[tuple[str, str]] = []
        if "schema" in documents:
            fragment_map, role_definitions, declared_concepts = self._parse_schema(
                documents["schema"], slug=slug, stats=stats, pool=pool,
                raw_by_file=raw_by_file,
            )

        # The label linkbase is read BEFORE the documents that create most
        # concepts, so a concept gets its name at construction; labels for
        # concepts another filing already created are applied through the
        # pool, and ones nothing ever creates are counted (_reconcile_labels).
        if "label" in documents:
            self._parse_label(
                documents["label"], slug=slug, stats=stats,
                fragment_map=fragment_map, prefix_ns=prefix_ns, pool=pool,
                raw_by_file=raw_by_file,
            )

        context_nodes, fact_nodes, instance_edges = self._parse_instance(
            documents["instance"], slug=slug, filing_id=filing_id,
            stats=stats, pool=pool, prefix_ns=prefix_ns,
            source_revision=source_revision, raw_by_file=raw_by_file,
        )

        calc_edges: list[EdgeFact] = []
        if "calculation" in documents:
            calc_edges = self._parse_calculation(
                documents["calculation"], slug=slug, stats=stats,
                fragment_map=fragment_map, prefix_ns=prefix_ns, pool=pool,
                source_revision=source_revision, raw_by_file=raw_by_file,
            )

        statement_nodes: list[NodeFact] = []
        presentation_edges: list[EdgeFact] = []
        if "presentation" in documents:
            statement_nodes, presentation_edges = self._parse_presentation(
                documents["presentation"], slug=slug, filing_id=filing_id,
                stats=stats, fragment_map=fragment_map, prefix_ns=prefix_ns,
                role_definitions=role_definitions, pool=pool,
                source_revision=source_revision, raw_by_file=raw_by_file,
            )

        definition_edges: list[EdgeFact] = []
        if "definition" in documents:
            definition_edges = self._parse_definition(
                documents["definition"], slug=slug, stats=stats,
                fragment_map=fragment_map, prefix_ns=prefix_ns, pool=pool,
                source_revision=source_revision, raw_by_file=raw_by_file,
            )

        # Any remaining document goes into the applied-token identity even
        # though it is not projected — the token must cover the bytes the
        # corpus mounts.
        for path in documents.values():
            if str(path) not in raw_by_file:
                raw_by_file[str(path)] = self._read_document(path)

        # Exactly three documents in a docset can anchor the FILING node:
        # the instance (contains xbrl_fact / xbrl_context), the presentation
        # linkbase (contains xbrl_statement) and the filer's own schema
        # (declares xbrl_concept). The calculation, definition and label
        # linkbases relate concepts to each other and never touch the
        # filing. So a filing is structurally stranded exactly when all
        # three of those are unusable IN THE INPUT — measured: 5 of 25 in
        # the FinAuditing export, whose instance and presentation documents
        # both hit the dataset's 32,767-byte cap and whose schemas declare
        # only imports and role types, no `xsd:element`.
        #
        # Read from the input, NOT from the fact list. The predecessor
        # predicate was `documents_truncated and not statement_nodes and
        # not context_nodes`: two of its three conjuncts asked whether
        # emission produced anything, so a parsing regression inside
        # `_parse_instance` on a filing that happened to have ANY truncated
        # document (a capped label linkbase would do) flipped the stamp to
        # true and forgave itself. These three conjuncts each name a
        # document and a fact the document itself carries.
        instance_unusable = (
            "instance" not in documents
            or bool(stats["documents_truncated_instance"])
        )
        presentation_unusable = (
            "presentation" not in documents
            or bool(stats["documents_truncated_presentation"])
        )
        # An unusable schema declares nothing, so this covers "schema
        # absent", "schema truncated" and "schema present but declaring no
        # extension element" — the last being the state the second real
        # build found, which falsified the `declares` remedy for these 5.
        schema_declares_nothing = not declared_concepts
        structurally_stranded = (
            instance_unusable
            and presentation_unusable
            and schema_declares_nothing
        )
        filing_node = NodeFact(
            node_id=filing_id,
            subtype="filing",
            attrs={
                "filing_id": slug,
                "doc_kinds": ",".join(sorted(documents)),
                # This flag is what lets the corpus contract distinguish
                # "isolated because upstream truncated it" from "isolated
                # because we failed to read it", instead of tolerating a
                # blanket isolated-filing allowance.
                "structurally_stranded": structurally_stranded,
                # Which of the three filing-anchoring documents was
                # unusable, so `structurally_stranded=true` can be audited
                # against the input rather than taken on trust.
                "instance_unusable": instance_unusable,
                "presentation_unusable": presentation_unusable,
                "schema_declares_no_concepts": schema_declares_nothing,
                "fact_count": stats["facts_emitted"],
                "facts_skipped": stats["facts_skipped"],
                "facts_no_context": stats["facts_no_context"],
                "concepts_skipped": stats["concepts_skipped"],
                "calc_arcs_skipped": stats["calc_arcs_skipped"],
                "calc_locators_unresolved": stats["calc_locators_unresolved"],
                "context_count": stats["contexts_emitted"],
                "contexts_skipped": stats["contexts_skipped"],
                "context_members_emitted": stats["context_members_emitted"],
                "context_members_skipped": stats["context_members_skipped"],
                "statement_count": stats["statements_emitted"],
                "presentation_arcs_skipped":
                    stats["presentation_arcs_skipped"],
                "presentation_locators_unresolved":
                    stats["presentation_locators_unresolved"],
                "definition_arcs_skipped": stats["definition_arcs_skipped"],
                "definition_locators_unresolved":
                    stats["definition_locators_unresolved"],
                "labels_parsed": stats["labels_parsed"],
                "labels_unmatched": stats["labels_unmatched"],
                "label_locators_unresolved":
                    stats["label_locators_unresolved"],
                "documents_truncated": stats["documents_truncated"],
                "documents_unrecognized": stats["documents_unrecognized"],
            },
            source_record_id={"filing": slug},
            source_revision=source_revision,
            op="I",
        )
        # `filing -declares-> xbrl_concept` for the filing's OWN extension
        # concepts. This is what anchors a filing whose instance AND
        # presentation linkbase are both truncated: the schema is the
        # smallest document in a docset and survives the dataset's 32,767-byte
        # cap when the others do not (measured: all 5 filings left isolated by
        # the first real build ship a parseable schema). Declaring a concept
        # is an assertion the filer's schema makes, so this adds no fact the
        # filing does not have.
        declares_edges = [
            EdgeFact(
                src=filing_id,
                dst=concept_node_id(namespace, name),
                edge_type="declares",
                attrs={},
                source_record_id={"filing": slug, "declares": name},
                source_revision=source_revision,
                op="I",
            )
            for namespace, name in declared_concepts
        ]
        stats["concepts_declared_edges"] = len(declares_edges)
        facts: list[NodeFact | EdgeFact] = [
            filing_node,
            *pool.new,
            *statement_nodes,
            *context_nodes,
            *fact_nodes,
            *instance_edges,
            *calc_edges,
            *presentation_edges,
            *definition_edges,
            *declares_edges,
        ]
        return facts, stats, filing_node

    # ---------- document parsing ----------

    def _read_document(self, path: Path) -> str:
        raw = read_snapshot_bytes(path, adapter=self.name)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SubstrateError(
                f"source {self.name!r}: {path} is not UTF-8 XML",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="re-export the filing document set",
            ) from exc

    def _parse_xml(
        self, path: Path, *, raw_by_file: dict[str, str]
    ) -> ET.Element | None:
        """Parse one document. Returns None for an unparseable document when
        ``truncated_documents='skip_counted'``.

        Why the mode exists: a real EDGAR filing that will not parse is
        corrupt and must fail loudly (the default). But a benchmark corpus
        whose documents are embedded in dataset rows can carry documents the
        DATASET truncated — FinAuditing caps each embedded document at 32,767
        bytes, so 1,203 of 6,157 documents end mid-tag (verified
        2026-07-27). Refusing the whole corpus over an upstream cap would
        make the vertical unbuildable; silently treating truncated XML as
        empty would be a fabrication. The middle path is to skip the
        document and COUNT it, with the count stamped on the filing node and
        in last_run_stats. Truncation is PER DOCUMENT: a filing whose
        instance is capped still contributes its statement structure and
        concept relationships from the five linkbases, which is what keeps
        it from being an isolated node."""
        text = self._read_document(path)
        raw_by_file[str(path)] = text
        try:
            return safe_xml_fromstring(text)
        except (ET.ParseError, ValueError) as exc:
            # defusedxml's entity/DTD refusals subclass ValueError; both
            # cases mean the document is not trustworthy filing XML.
            if self.truncated_documents == "skip_counted":
                return None
            raise SubstrateError(
                f"source {self.name!r}: {path} is not parseable XML",
                data={"adapter": self.name, "path": str(path)},
                fix_hint=(
                    "re-export the filing document set, or set "
                    "truncated_documents='skip_counted' when the corpus is "
                    "a dataset export whose documents are capped upstream"
                ),
            ) from exc

    def _parse_schema(
        self,
        path: Path,
        *,
        slug: str,
        stats: dict[str, int],
        pool: _ConceptPool,
        raw_by_file: dict[str, str],
    ) -> tuple[dict[str, tuple[str, str]], dict[str, str], list[tuple[str, str]]]:
        """Declared concepts + the fragment map + role URI -> human
        definition (the `roleType`/`definition` pairs a presentation
        linkbase's extended link roles are named by) + the (ns, name) pairs
        this filing's own schema DECLARES.

        The declared list exists because the first real build left 5 of 25
        filings isolated: those whose instance AND presentation linkbase are
        both truncated by the dataset's 32,767-byte cap. All 5 still ship a
        parseable schema, and "this filing declares this extension concept"
        is a fact the filer's schema asserts — so anchoring them on it adds
        no fact they do not have (okg-corpus-xbrl req.xbrl.linkbases (4))."""
        root = self._parse_xml(path, raw_by_file=raw_by_file)
        if root is None:
            stats["documents_truncated"] += 1
            stats["documents_truncated_schema"] += 1
            return {}, {}, []
        if _local_name(root) != "schema":
            raise SubstrateError(
                f"source {self.name!r}: {path} root element is {root.tag!r}, "
                f"expected an XML Schema `schema` document",
                data={"adapter": self.name, "filing": slug, "path": str(path)},
                fix_hint="the schema.xml doc kind must hold the filer's "
                         "xsd; re-export the docset",
            )
        target_ns = root.get("targetNamespace") or ""
        fragment_map: dict[str, tuple[str, str]] = {}
        declared: list[tuple[str, str]] = []
        for element in root.findall(f"{{{_XSD_NS}}}element"):
            concept_name = element.get("name")
            if not concept_name:
                stats["concepts_skipped"] += 1
                continue
            stats["concepts_declared"] += 1
            key = (target_ns, concept_name)
            fragment_map[concept_name] = key
            element_id = element.get("id")
            if element_id:
                fragment_map[element_id] = key
            pool.ensure(target_ns, concept_name, declared=True)
            declared.append(key)
        role_definitions: dict[str, str] = {}
        for role_type in root.iter():
            if _local_name(role_type) != "roleType":
                continue
            role_uri = role_type.get("roleURI")
            if not role_uri:
                continue
            for child in role_type:
                if _local_name(child) != "definition":
                    continue
                text = _element_text(child)
                if text:
                    role_definitions.setdefault(role_uri, text)
        return fragment_map, role_definitions, declared

    # ---------- instance: contexts and facts ----------

    def _parse_instance(
        self,
        path: Path,
        *,
        slug: str,
        filing_id: str,
        stats: dict[str, int],
        pool: _ConceptPool,
        prefix_ns: dict[str, str],
        source_revision: dict[str, Any],
        raw_by_file: dict[str, str],
    ) -> tuple[list[NodeFact], list[NodeFact], list[EdgeFact]]:
        root = self._parse_xml(path, raw_by_file=raw_by_file)
        if root is None:
            stats["documents_truncated"] += 1
            stats["documents_truncated_instance"] += 1
            return [], [], []
        context_nodes: list[NodeFact] = []
        edges: list[EdgeFact] = []
        #: contextRef -> xbrl_context node id, for THIS instance.
        contexts: dict[str, str] = {}
        for child in root:
            if _local_name(child) != "context":
                continue
            node, member_edges = self._parse_context(
                child, slug=slug, stats=stats, pool=pool,
                prefix_ns=prefix_ns, source_revision=source_revision,
            )
            if node is None:
                continue
            contexts[str(node.attrs["context_ref"])] = node.node_id
            context_nodes.append(node)
            edges.append(
                EdgeFact(
                    src=filing_id, dst=node.node_id,
                    edge_type="contains",
                    attrs={},
                    source_record_id={
                        "filing": slug,
                        "context": node.attrs["context_ref"],
                    },
                    source_revision=source_revision,
                    op="I",
                )
            )
            edges.extend(member_edges)

        fact_nodes: list[NodeFact] = []
        fact_index = 0
        for child in root:
            namespace, local = _split_tag(child.tag)
            if namespace in (_XBRLI_NS, _LINKBASE_NS) or local in (
                "context", "unit", "schemaRef",
            ):
                continue  # structural instance machinery, not a fact
            context_ref = child.get("contextRef")
            value = (child.text or "").strip()
            is_nil = child.get(f"{{{_XSI_NS}}}nil") == "true"
            if not context_ref or not value or is_nil:
                # A fact candidate missing its identifying fields
                # (concept context / value) is skipped and counted —
                # never silently defaulted.
                stats["facts_skipped"] += 1
                continue
            context_id = contexts.get(context_ref)
            if context_id is None:
                # The contextRef names no context element in this
                # instance: the period the value is reported for is
                # unknowable. Skip and count rather than emit a fact
                # whose in_context edge would dangle.
                stats["facts_skipped"] += 1
                stats["facts_no_context"] += 1
                continue
            concept_id = pool.ensure(namespace, local, declared=False)
            fact_id = f"xbrl_fact:{slug}:{fact_index:04d}"
            fact_index += 1
            stats["facts_emitted"] += 1
            fact_nodes.append(
                NodeFact(
                    node_id=fact_id,
                    subtype="xbrl_fact",
                    attrs={
                        "fact_id": fact_id,
                        "concept_name": local,
                        "concept_ns": namespace,
                        # Kept for provenance; the traversable handle is
                        # the in_context edge below.
                        "context_ref": context_ref,
                        "unit": child.get("unitRef"),
                        "value": value,
                    },
                    source_record_id={"filing": slug, "fact": fact_id},
                    source_revision=source_revision,
                    op="I",
                )
            )
            edges.append(
                EdgeFact(
                    src=filing_id, dst=fact_id,
                    edge_type="contains",
                    attrs={},
                    source_record_id={"filing": slug, "fact": fact_id},
                    source_revision=source_revision,
                    op="I",
                )
            )
            edges.append(
                EdgeFact(
                    src=fact_id, dst=concept_id,
                    edge_type="reports",
                    attrs={},
                    source_record_id={"filing": slug, "fact": fact_id},
                    source_revision=source_revision,
                    op="I",
                )
            )
            edges.append(
                EdgeFact(
                    src=fact_id, dst=context_id,
                    edge_type="in_context",
                    attrs={},
                    source_record_id={"filing": slug, "fact": fact_id},
                    source_revision=source_revision,
                    op="I",
                )
            )
        return context_nodes, fact_nodes, edges

    def _parse_context(
        self,
        element: ET.Element,
        *,
        slug: str,
        stats: dict[str, int],
        pool: _ConceptPool,
        prefix_ns: dict[str, str],
        source_revision: dict[str, Any],
    ) -> tuple[Optional[NodeFact], list[EdgeFact]]:
        context_ref = element.get("id")
        if not context_ref:
            stats["contexts_skipped"] += 1
            return None, []
        node_id = context_node_id(slug, context_ref)
        attrs: dict[str, Any] = {
            "context_id": node_id,
            "context_ref": context_ref,
            "filing_id": slug,
        }
        entity = _first_child(element, "entity")
        identifier = (
            _first_child(entity, "identifier") if entity is not None else None
        )
        if identifier is not None:
            text = _element_text(identifier)
            if text:
                attrs["entity_identifier"] = text
            scheme = identifier.get("scheme")
            if scheme:
                attrs["entity_scheme"] = scheme
        period = _first_child(element, "period")
        if period is not None:
            instant = _child_date(period, "instant")
            start = _child_date(period, "startDate")
            end = _child_date(period, "endDate")
            if instant is not None:
                attrs["period_type"] = "instant"
                attrs["instant"] = instant
            elif start is not None and end is not None:
                attrs["period_type"] = "duration"
                attrs["start_date"] = start
                attrs["end_date"] = end
            else:
                # A period whose dates do not parse as ISO dates leaves
                # NO period_type: an unknown period is absent, never
                # guessed at from a half-read element.
                stats["context_periods_unparseable"] += 1
        stats["contexts_emitted"] += 1
        node = NodeFact(
            node_id=node_id,
            subtype="xbrl_context",
            attrs=attrs,
            source_record_id={"filing": slug, "context": context_ref},
            source_revision=source_revision,
            op="I",
        )
        member_edges: list[EdgeFact] = []
        for member in element.iter():
            local = _local_name(member)
            if local == "typedMember":
                stats["context_typed_members_unread"] += 1
                continue
            if local != "explicitMember":
                continue
            resolved = self._resolve_qname(_element_text(member), prefix_ns)
            if resolved is None:
                stats["context_members_skipped"] += 1
                continue
            member_id = pool.ensure(*resolved, declared=False)
            edge_attrs: dict[str, Any] = {}
            dimension = member.get("dimension")
            if dimension:
                edge_attrs["dimension"] = dimension
            stats["context_members_emitted"] += 1
            member_edges.append(
                EdgeFact(
                    src=node_id, dst=member_id,
                    edge_type="has_member",
                    attrs=edge_attrs,
                    source_record_id={
                        "filing": slug,
                        "context": context_ref,
                        "member": member_id,
                    },
                    source_revision=source_revision,
                    op="I",
                )
            )
        return node, member_edges

    # ---------- linkbases ----------

    @staticmethod
    def _resolve_prefixed_fragment(
        fragment: str, prefix_ns: dict[str, str]
    ) -> tuple[str, str] | None:
        """Resolve a locator fragment like ``us-gaap_IncomeTaxExpenseBenefit``
        onto (namespace, concept name) using the filing's own prefix->URI
        declarations, so an arc lands on the SAME concept node the facts
        reference. Returns None when the prefix is undeclared — an
        unresolvable locator stays skipped and counted."""
        if "_" not in fragment:
            return None
        prefix, _, name = fragment.partition("_")
        namespace = prefix_ns.get(prefix)
        if not namespace or not name:
            return None
        return namespace, name

    @staticmethod
    def _resolve_qname(
        qname: str, prefix_ns: dict[str, str]
    ) -> tuple[str, str] | None:
        """Resolve ``us-gaap:RetailSegmentMember`` onto (namespace, name).

        Instance QNames use ``:``; linkbase locator fragments use ``_``.
        Returns None for an unprefixed or undeclared prefix — the member
        stays skipped and counted."""
        prefix, sep, name = qname.partition(":")
        if not sep or not name:
            return None
        namespace = prefix_ns.get(prefix)
        if not namespace:
            return None
        return namespace, name

    def _locator_map(
        self,
        scope: ET.Element,
        *,
        stats: dict[str, int],
        unresolved_counter: str,
        fragment_map: dict[str, tuple[str, str]],
        prefix_ns: dict[str, str],
        pool: _ConceptPool,
        create: bool,
    ) -> dict[str, str]:
        """xlink:label -> concept node id for every `loc` under `scope`.

        Resolution is the calculation linkbase's, unchanged: the filer's
        own schema fragment first, then the prefixed-fragment fallback
        that lands a standard-taxonomy locator
        (".../us-gaap-2021-01-31.xsd#us-gaap_Assets") on the same node
        the instance facts reference. An unresolvable locator is skipped
        AND COUNTED under `unresolved_counter`.

        `create=True` emits the endpoint concept (declared: false) so the
        arc lands on a node instead of dangling. `create=False` — the
        label linkbase — resolves without minting anything: a concept
        that exists only because something named it is a degree-0 node.
        """
        out: dict[str, str] = {}
        for loc in scope.iter():
            if _local_name(loc) != "loc":
                continue
            label = loc.get(f"{{{_XLINK_NS}}}label")
            href = loc.get(f"{{{_XLINK_NS}}}href") or ""
            fragment = href.split("#", 1)[1] if "#" in href else ""
            resolved = fragment_map.get(fragment)
            if resolved is None:
                resolved = self._resolve_prefixed_fragment(fragment, prefix_ns)
            if not label or resolved is None:
                stats[unresolved_counter] += 1
                continue
            if create:
                out[label] = pool.ensure(*resolved, declared=False)
            else:
                out[label] = concept_node_id(*resolved)
        return out

    def _parse_calculation(
        self,
        path: Path,
        *,
        slug: str,
        stats: dict[str, int],
        fragment_map: dict[str, tuple[str, str]],
        prefix_ns: dict[str, str],
        pool: _ConceptPool,
        source_revision: dict[str, Any],
        raw_by_file: dict[str, str],
    ) -> list[EdgeFact]:
        root = self._parse_xml(path, raw_by_file=raw_by_file)
        if root is None:
            stats["documents_truncated"] += 1
            stats["documents_truncated_calculation"] += 1
            return []
        label_to_concept = self._locator_map(
            root, stats=stats,
            unresolved_counter="calc_locators_unresolved",
            fragment_map=fragment_map, prefix_ns=prefix_ns, pool=pool,
            create=True,
        )
        edges: list[EdgeFact] = []
        for arc in root.iter():
            if _local_name(arc) != "calculationArc":
                continue
            src_label = arc.get(f"{{{_XLINK_NS}}}from")
            dst_label = arc.get(f"{{{_XLINK_NS}}}to")
            weight = _float_or_none(arc.get("weight"))
            total = label_to_concept.get(src_label or "")
            component = label_to_concept.get(dst_label or "")
            if total is None or component is None or weight is None:
                stats["calc_arcs_skipped"] += 1
                continue
            stats["calc_arcs_emitted"] += 1
            # Arc direction in the linkbase is total -> component; the
            # edge reads "component sums into total".
            edges.append(
                EdgeFact(
                    src=component, dst=total,
                    edge_type="calc_sums_to",
                    attrs={"weight": weight},
                    source_record_id={
                        "filing": slug,
                        "arc": f"{component}->{total}",
                    },
                    source_revision=source_revision,
                    op="I",
                )
            )
        return edges

    def _parse_presentation(
        self,
        path: Path,
        *,
        slug: str,
        filing_id: str,
        stats: dict[str, int],
        fragment_map: dict[str, tuple[str, str]],
        prefix_ns: dict[str, str],
        role_definitions: dict[str, str],
        pool: _ConceptPool,
        source_revision: dict[str, Any],
        raw_by_file: dict[str, str],
    ) -> tuple[list[NodeFact], list[EdgeFact]]:
        """One `xbrl_statement` per extended link role, its `presents`
        edges, and the intra-statement parent->child tree.

        Direction of `presentation_parent_of` is parent->child, matching
        the existing `calc_sums_to` total->component orientation, so the
        two relationship families read the same way round.
        """
        root = self._parse_xml(path, raw_by_file=raw_by_file)
        if root is None:
            stats["documents_truncated"] += 1
            stats["documents_truncated_presentation"] += 1
            return [], []
        statements: dict[str, NodeFact] = {}
        edges: list[EdgeFact] = []
        for link in root.iter():
            if _local_name(link) != "presentationLink":
                continue
            role = link.get(f"{{{_XLINK_NS}}}role")
            if not role:
                # An extended link with no role has no statement identity.
                stats["statement_links_skipped"] += 1
                continue
            node_id = statement_node_id(slug, role)
            if node_id not in statements:
                attrs: dict[str, Any] = {
                    "statement_id": node_id,
                    "role_uri": role,
                    "filing_id": slug,
                }
                definition = role_definitions.get(role)
                if definition:
                    # Absent when the filer's schema is truncated or does
                    # not declare the roleType: no key, never the URI
                    # restated as if it were a human name.
                    attrs["definition"] = definition
                statements[node_id] = NodeFact(
                    node_id=node_id,
                    subtype="xbrl_statement",
                    attrs=attrs,
                    source_record_id={"filing": slug, "statement": role},
                    source_revision=source_revision,
                    op="I",
                )
                stats["statements_emitted"] += 1
                edges.append(
                    EdgeFact(
                        src=filing_id, dst=node_id,
                        edge_type="contains",
                        attrs={},
                        source_record_id={"filing": slug, "statement": role},
                        source_revision=source_revision,
                        op="I",
                    )
                )
            locators = self._locator_map(
                link, stats=stats,
                unresolved_counter="presentation_locators_unresolved",
                fragment_map=fragment_map, prefix_ns=prefix_ns, pool=pool,
                create=True,
            )
            #: concept node id -> its order within this statement (None
            #: for a root, which is no arc's child — an absent order, not
            #: a zero standing in for one).
            presented: dict[str, Optional[float]] = {}
            for arc in link:
                if _local_name(arc) != "presentationArc":
                    continue
                parent = locators.get(arc.get(f"{{{_XLINK_NS}}}from") or "")
                child = locators.get(arc.get(f"{{{_XLINK_NS}}}to") or "")
                if parent is None or child is None:
                    stats["presentation_arcs_skipped"] += 1
                    continue
                order = _float_or_none(arc.get("order"))
                stats["presentation_arcs_emitted"] += 1
                arc_attrs: dict[str, Any] = {"role": role}
                if order is not None:
                    arc_attrs["order"] = order
                edges.append(
                    EdgeFact(
                        src=parent, dst=child,
                        edge_type="presentation_parent_of",
                        attrs=arc_attrs,
                        source_record_id={
                            "filing": slug,
                            "role": role,
                            "arc": f"{parent}->{child}",
                        },
                        source_revision=source_revision,
                        op="I",
                    )
                )
                presented.setdefault(parent, None)
                if presented.get(child) is None:
                    presented[child] = order
            for concept_id, order in presented.items():
                present_attrs: dict[str, Any] = {}
                if order is not None:
                    present_attrs["order"] = order
                stats["presents_emitted"] += 1
                edges.append(
                    EdgeFact(
                        src=node_id, dst=concept_id,
                        edge_type="presents",
                        attrs=present_attrs,
                        source_record_id={
                            "filing": slug,
                            "statement": role,
                            "concept": concept_id,
                        },
                        source_revision=source_revision,
                        op="I",
                    )
                )
        return list(statements.values()), edges

    def _parse_definition(
        self,
        path: Path,
        *,
        slug: str,
        stats: dict[str, int],
        fragment_map: dict[str, tuple[str, str]],
        prefix_ns: dict[str, str],
        pool: _ConceptPool,
        source_revision: dict[str, Any],
        raw_by_file: dict[str, str],
    ) -> list[EdgeFact]:
        """Dimensional arcs become ONE edge type carrying the arcrole.

        Not one edge type per arcrole: the arcrole vocabulary is open to
        filer extension, so a per-arcrole model would meet an
        unanticipated arcrole with a narrowing_violation at insert and
        fail the publish rather than ingest the relationship. An arc
        with no arcrole has no kind and is skipped AND COUNTED — the
        arcrole is never defaulted to a common one.
        """
        root = self._parse_xml(path, raw_by_file=raw_by_file)
        if root is None:
            stats["documents_truncated"] += 1
            stats["documents_truncated_definition"] += 1
            return []
        edges: list[EdgeFact] = []
        for link in root.iter():
            if _local_name(link) != "definitionLink":
                continue
            role = link.get(f"{{{_XLINK_NS}}}role")
            locators = self._locator_map(
                link, stats=stats,
                unresolved_counter="definition_locators_unresolved",
                fragment_map=fragment_map, prefix_ns=prefix_ns, pool=pool,
                create=True,
            )
            for arc in link:
                if _local_name(arc) != "definitionArc":
                    continue
                arcrole = arc.get(f"{{{_XLINK_NS}}}arcrole")
                src = locators.get(arc.get(f"{{{_XLINK_NS}}}from") or "")
                dst = locators.get(arc.get(f"{{{_XLINK_NS}}}to") or "")
                if not arcrole or src is None or dst is None:
                    stats["definition_arcs_skipped"] += 1
                    continue
                attrs: dict[str, Any] = {"arcrole": arcrole}
                if role:
                    attrs["role"] = role
                order = _float_or_none(arc.get("order"))
                if order is not None:
                    attrs["order"] = order
                stats["definition_arcs_emitted"] += 1
                edges.append(
                    EdgeFact(
                        src=src, dst=dst,
                        edge_type="dim_related_to",
                        attrs=attrs,
                        source_record_id={
                            "filing": slug,
                            "arcrole": arcrole,
                            "arc": f"{src}->{dst}",
                        },
                        source_revision=source_revision,
                        op="I",
                    )
                )
        return edges

    def _parse_label(
        self,
        path: Path,
        *,
        slug: str,
        stats: dict[str, int],
        fragment_map: dict[str, tuple[str, str]],
        prefix_ns: dict[str, str],
        pool: _ConceptPool,
        raw_by_file: dict[str, str],
    ) -> None:
        """Standard-role labels become the `label` attribute on the
        concept and documentation-role text the `documentation`
        attribute. Nothing here emits a node."""
        root = self._parse_xml(path, raw_by_file=raw_by_file)
        if root is None:
            stats["documents_truncated"] += 1
            stats["documents_truncated_label"] += 1
            return
        for link in root.iter():
            if _local_name(link) != "labelLink":
                continue
            locators = self._locator_map(
                link, stats=stats,
                unresolved_counter="label_locators_unresolved",
                fragment_map=fragment_map, prefix_ns=prefix_ns, pool=pool,
                create=False,
            )
            texts: dict[str, list[tuple[str, str]]] = {}
            for element in link:
                if _local_name(element) != "label":
                    continue
                key = element.get(f"{{{_XLINK_NS}}}label")
                # XBRL 2.1 §5.2.2.2.2 gives the label element's
                # xlink:role the standard label role when the attribute
                # is absent. That is the specification's declared
                # default for the attribute, not a value invented for
                # missing data.
                role = element.get(f"{{{_XLINK_NS}}}role") \
                    or _STANDARD_LABEL_ROLE
                text = _element_text(element)
                if not key or not text:
                    # An empty label is not a name. Counted, never
                    # stored as "".
                    stats["labels_skipped"] += 1
                    continue
                texts.setdefault(key, []).append((role, text))
            for arc in link:
                if _local_name(arc) != "labelArc":
                    continue
                concept_id = locators.get(arc.get(f"{{{_XLINK_NS}}}from") or "")
                entries = texts.get(arc.get(f"{{{_XLINK_NS}}}to") or "")
                if concept_id is None or not entries:
                    stats["label_arcs_skipped"] += 1
                    continue
                for role, text in entries:
                    attr = _LABEL_ROLE_ATTRS.get(role)
                    if attr is None:
                        # terse / verbose / total / period labels: real
                        # roles this source does not model. Counted so
                        # the omission is visible.
                        stats["label_roles_ignored"] += 1
                        continue
                    pool.record_label(concept_id, attr, text, slug=slug)
                    stats["labels_parsed"] += 1


def _first_child(parent: ET.Element, local: str) -> Optional[ET.Element]:
    """First direct child with this local name, whatever its namespace
    version — matching on the local name keeps a filing written against
    a different xbrli namespace revision readable."""
    for child in parent:
        if _local_name(child) == local:
            return child
    return None


def _child_date(parent: ET.Element, local: str) -> Optional[str]:
    """An ISO date from a period child, or None. A value that is not an
    ISO date is NOT coerced — the attribute stays absent and the caller
    counts the context as having an unparseable period."""
    child = _first_child(parent, local)
    if child is None:
        return None
    text = _element_text(child)
    return text if _ISO_DATE_RE.match(text) else None
