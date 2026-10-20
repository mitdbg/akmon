"""Spec-driven corpus builder: ONE provisioning path, one spec per vertical.

Every okg-bench benchmark that has an okg arm resolves to exactly one OKG
deployment definition. Two shapes predate this module — the repo-history
corpus (``corpus.py``, git+GitHub at a machine-checkable cutoff) and the
CTI snapshot catalog (``corpus_cti.py``) — and each was written as its own
builder. That does not scale: a source adapter plus an ontology module is
NOT a deployment, and every new vertical needed a bespoke scaffold that
was never written, so the okg arm silently had nowhere to run for five of
the ten benchmarks.

Here a vertical is DATA: ontology modules, source-registry entries,
expected node families, and the snapshot artifacts it requires. The build
path is shared — scaffold, catalog, ingest, publish, verify families,
write the freeze manifest — so adding a vertical is a spec, not a
codepath. ``corpus_cti`` now delegates here (one provisioning
implementation, not two).

Fail-loud contract: a spec whose expected families do not publish rows, a
snapshot missing a required artifact, an unknown vertical, or a blocked
publish all raise. A benchmark with NO okg corpus is a stated verdict
(``NO_CORPUS``) with its reason, never an unexplained gap.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .contract import CorpusContract, Probe, TripleClause
from .contract import enforce as enforce_contract
from .contract import evaluate as evaluate_contract
from .errors import CorpusBuildError
from .manifest import now_utc
from .model import dump_json

# --- nomos posture ------------------------------------------------------
# The per-source privacy audit runs even without runtime enforcement, and
# the class table (nomos/deployment_audit.py) constrains every field once
# a class is chosen: public_reference_demo REQUIRES public_allowed export,
# live_call deny, no_secrets, and public-only sensitivity/classification/
# exportability. Benchmark corpora are public reference data by
# construction, so that is the honest class for all of them.
_PUBLIC_POSTURE: dict[str, Any] = {
    "deployment_class": "public_reference_demo",
    "read_principals": ["human_operator", "agent", "okg_service", "mcp_client"],
    "write_principals": ["human_operator", "okg_service", "source_adapter"],
    "mcp_write_default": "review",
    "export_policy": "public_allowed",
    "secret_handling": "no_secrets",
    "live_call_default": "deny",
    "allowed_source_sensitivities": ["public"],
    "allowed_data_classifications": ["public"],
    "allowed_exportability": ["public"],
    "privacy_obligations": ["audit"],
    "readiness_profiles": ["dogfood"],
}

_PUBLIC_SOURCE_DEFAULTS: dict[str, Any] = {
    "store_raw": True,
    "sensitivity": "public",
    "data_classification": "public",
    "credential_ref_policy": "none",
    "live_call_allowed": False,
    "exportability": "public",
    "retention": "normal",
    "provenance": "declared",
    "privacy_obligations": ["audit"],
}


@dataclass(frozen=True)
class SourceSpec:
    """One source-registry entry, minus the paths resolved at build time."""

    name: str
    module: str
    cls: str
    source_class: str
    record_identity_kind: str
    record_identity_fields: list[str]
    #: params(snapshot_dir, deployment_dir, deployment_name) -> params dict
    params: Callable[[Path, Path, str], dict[str, Any]]
    predecessor: str | None = None
    source_revision_kind: str = "content_hash"
    #: params(spec) -> extra params, merged over `params`. The three-argument
    #: signature above cannot see build-level state; a source that must be
    #: told the snapshot DATE or the dsn reads it from the spec here rather
    #: than inferring it from the machine. Used by doc-corpus.
    spec_params: Callable[[Any], dict[str, Any]] | None = None


@dataclass(frozen=True)
class VerticalSpec:
    """A complete OKG deployment definition for one benchmark corpus."""

    name: str
    modules: list[str]
    sources: list[SourceSpec]
    #: node subtypes that MUST publish nonzero rows. Retained as the
    #: floor beneath the contract; the CONTRACT is what decides the build.
    expected_families: list[str]
    #: human description of the artifacts the snapshot dir must hold
    required_artifacts: list[tuple[str, Callable[[list[str]], bool]]]
    description: str
    #: True when the snapshot dir must be a git worktree (doc_corpus needs
    #: a file-walking predecessor for its source_file nodes)
    git_init_snapshot: bool = False
    notes: str = ""
    #: What makes this corpus USABLE, not merely populated. An empty
    #: contract is a not-yet-designed vertical: build() refuses it rather
    #: than falling back to the node-count gate this program replaced.
    contract: CorpusContract = field(default_factory=CorpusContract)
    #: extra files written verbatim into the deployment dir at scaffold
    #: time, relative path -> contents. doc-corpus needs an extractors.yaml;
    #: without one its source has no parser or chunker chain at all.
    deployment_files: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class NoCorpus:
    """A benchmark with no okg-arm corpus, and the reason why."""

    reason: str


def _has(*fragments: str) -> Callable[[list[str]], bool]:
    def check(names: list[str]) -> bool:
        low = [n.lower() for n in names]
        return any(all(f in n for f in fragments) for n in low)

    return check


def _suffix(*suffixes: str) -> Callable[[list[str]], bool]:
    def check(names: list[str]) -> bool:
        return any(n.lower().endswith(suffixes) for n in names)

    return check


# --- the verticals ------------------------------------------------------

#: Contracts are stated per vertical, next to the spec they bind. Floors are
#: measured against the source (see each child PACT); the first real build
#: records observed counts so the floors can be tightened.
_CTI_CONTRACT = CorpusContract(
    # attack_campaign was in zero_isolated_subtypes but NOT here, so "zero
    # isolated campaigns" was vacuously true of a corpus with no campaigns:
    # dropping all 56 campaign nodes and their 1,344 incident edges satisfied
    # every clause. A connectivity rule needs an existence rule under it.
    subtypes=("cve", "cwe", "attack_technique", "attack_tactic",
              "attack_group", "attack_software", "attack_mitigation",
              "attack_detection_strategy", "attack_analytic",
              "attack_data_component", "attack_campaign"),
    triples=(
        TripleClause("cve", "has_weakness", "cwe", 3200),
        TripleClause("attack_technique", "part_of", "attack_tactic", 850),
        # Floor 1000, not the 1,318 ChildOf ROWS the catalog carries: graph
        # edge identity is (src, edge_type, dst), so the same parent/child
        # pair declared under two CWE views collapses to ONE edge. Tightened
        # from the first real build's observed count, which is the only
        # number that distinguishes collapse from a parsing regression.
        TripleClause("cwe", "child_of", "cwe", 1000,
                     note="1,318 ChildOf rows collapse by (src,type,dst)"),
        TripleClause("cwe", "related_to", "cwe", 250,
                     note="observed 284; non-ChildOf natures (PeerOf, "
                          "CanPrecede, ...). Raised from 100 to the floor "
                          "req.cti2.contract actually declares"),
        # Membership is the ONLY way a walk descends into a category, and
        # categories exist because NVD tags CVEs with legacy category ids
        # (CWE-16, CWE-310). Without this clause 420 category nodes sat
        # isolated and the corpus was over its ceiling. 4,260 rows resolve to
        # 4,260 edges (0 unresolved, 0 duplicate, 0 self-referential); 368 of
        # 422 categories declare members, and 108 of the rows target another
        # category rather than a weakness.
        TripleClause("cwe", "has_member", "cwe", 4000,
                     note="observed 4,260 — distinct from child_of: a category "
                          "is a thematic grouping, not a level of the "
                          "hierarchy, so a walk cannot confuse the two"),
        # Floors below are MEASURED against the released bundle by a dry
        # parse (2026-07-28), not taken from the paper's object counts —
        # those count objects, while edges collapse by (src,type,dst) and
        # revoked/deprecated objects drop out first. Observed, in order:
        # 1,448 / 475 / 4,546 / 11,211 / 697.
        TripleClause("attack_mitigation", "mitigates", "attack_technique", 1400,
                     note="observed 1,448 (44 live mitigations; 224 deprecated); "
                          "raised from 1200 to the declared floor"),
        TripleClause("attack_technique", "subtechnique_of", "attack_technique",
                     470, note="observed 475. Restored to 470: the drop to 400 "
                               "rode along with the `detects` subtype fix and "
                               "no measurement ever justified it — observed "
                               "moved 477 -> 475, still above the old floor"),
        TripleClause("attack_group", "uses", "attack_technique", 3800,
                     note="observed 4,546 — the uses family splits by "
                          "endpoint pair, so each pair carries its own floor "
                          "and a regression in one cannot hide in the total"),
        TripleClause("attack_software", "uses", "attack_technique", 9500,
                     note="observed 11,211"),
        # The remaining THREE `uses`/`attributed_to` pairs. req.cti2.contract
        # named all five uses triples and the campaign attribution; the code
        # gated only two, leaving 2,489 published edges — a sixth of the
        # corpus — under no clause at all. Each pair carries its own floor for
        # the reason stated above: a regression in one must not hide in a
        # total. Floors from the same dry parse.
        TripleClause("attack_campaign", "uses", "attack_technique", 1050,
                     note="observed 1,146"),
        TripleClause("attack_group", "uses", "attack_software", 1050,
                     note="observed 1,145"),
        TripleClause("attack_campaign", "uses", "attack_software", 150,
                     note="observed 172"),
        TripleClause("attack_campaign", "attributed_to", "attack_group", 20,
                     note="observed 26 — the smallest ATT&CK triple, and the "
                          "only path from a campaign to its actor"),
        # `detects` originates from x-mitre-detection-strategy, NOT from
        # data components: measured, all 697 detects relationships in this
        # release come from the 699 DET#### strategy objects and ZERO from
        # data components. The earlier design assumed the opposite.
        TripleClause("attack_detection_strategy", "detects",
                     "attack_technique", 600, note="observed 697"),
        # The analytic layer, from EMBEDDED refs (x_mitre_analytic_refs and
        # x_mitre_log_source_references) rather than relationship objects.
        # This is the only path the bundle states from a technique to a data
        # component now that x_mitre_detection is gone from techniques.
        TripleClause("attack_detection_strategy", "contains",
                     "attack_analytic", 1700,
                     note="observed 1,745 — NOT 1,758: 13 live analytics "
                          "belong to 2 deprecated strategies, so the parent "
                          "edge drops with the parent"),
        TripleClause("attack_analytic", "references",
                     "attack_data_component", 4000, note="observed 4,170"),
    ),
    # SECONDARY guard only. The real isolation gate is the negative probe
    # below; this catches a blow-up in the SANCTIONED population (an NVD feed
    # change tripling unmapped CVEs) that the probe would wave through because
    # every one of them is individually explained. Observed 0.113.
    max_isolated_fraction=0.12,
    # Every ATT&CK subtype must be fully connected EXCEPT attack_data_component:
    # 8 of 106 are referenced by no analytic in the released bundle, so the
    # bundle states no path to them and inventing one would be fabrication.
    # cve and cwe are exempt HERE because their isolation is adjudicated
    # per-node by the negative probe below, which is strictly stronger than a
    # subtype-wide allow.
    zero_isolated_subtypes=("attack_technique", "attack_tactic",
                            "attack_group", "attack_mitigation",
                            "attack_detection_strategy", "attack_analytic",
                            "attack_software", "attack_campaign"),
    probes=(
        # An aggregate ceiling passes whether the isolates are the ones the
        # sources genuinely strand or the ones a parsing regression dropped.
        # It cannot tell them apart, so it is not the gate. Each source stamps
        # WHY it stranded a node, from what the upstream input DECLARES and
        # never from whether an edge came out (stamping from emission is
        # circular and forgives exactly the regression this catches):
        #   cwe.no_relations_reason    retired_entry | no_relationships_declared
        #   cve.weakness_mapping_status nvd_cwe_noinfo | nvd_cwe_other | not_stated
        #   ATT&CK no_relations_reason  unreferenced_in_bundle
        #                             | referenced_only_by_excluded_objects
        # Measured on the 2026-07-27 snapshot, the stamp DISCRIMINATES rather
        # than blanket-forgives: 991 isolated CVEs stamped vs 2,701 connected
        # unstamped; 70 isolated CWEs stamped (60 retired, 10 declaring no
        # relations at all) vs 1,321 connected unstamped; 8 isolated data
        # components stamped vs 4,360 connected ATT&CK objects unstamped; zero
        # stamped-but-connected in any of them. An exemption predicate keyed
        # on a field that is absent everywhere would forgive everything —
        # measure it on the complement before trusting it.
        #
        # There is NO subtype-wide allow left. The probe carried
        # `AND n.subtype <> 'attack_data_component'` — a blanket allow of the
        # exact shape this per-node rule is supposed to replace — because
        # attack_stix emitted no stranding stamp. Adversarial review caught
        # it; the source now runs a relation census over the raw bundle
        # BEFORE emitting, so those 8 are adjudicated node by node like
        # everything else.
        Probe(
            "no node is isolated for an UNSTATED reason",
            "SELECT n.subtype, n.node_id FROM okg.graph_nodes n "
            "WHERE NOT EXISTS (SELECT 1 FROM okg.graph_edges e "
            "  WHERE e.src = n.node_id OR e.dst = n.node_id) "
            "AND n.attrs->>'no_relations_reason' IS NULL "
            "AND n.attrs->>'weakness_mapping_status' IS NULL",
            expect_empty=True,
            note="a source must SAY why it stranded a node; an unstamped "
                 "isolate is a parsing regression, not upstream emptiness",
        ),
        Probe(
            "rcm: a CVE reaches its weakness",
            "SELECT 1 FROM okg.graph_edges e JOIN okg.graph_nodes s "
            "ON s.node_id = e.src JOIN okg.graph_nodes d ON d.node_id = e.dst "
            "WHERE s.subtype='cve' AND e.edge_type='has_weakness' "
            "AND d.subtype='cwe' LIMIT 1",
        ),
        Probe(
            "vsp: CVSS metrics are queryable, not just a vector string",
            "SELECT 1 FROM okg.graph_nodes WHERE subtype='cve' "
            "AND attrs ? 'cvss_attack_vector' LIMIT 1",
        ),
    ),
    notes="PACT okg-corpus-cti / req.cti2.contract",
)

CTI = VerticalSpec(
    name="cti",
    modules=["cti"],
    contract=_CTI_CONTRACT,
    expected_families=["cve", "cwe", "attack_technique"],
    description="NVD CVE feeds + MITRE CWE catalog + MITRE ATT&CK STIX bundle",
    required_artifacts=[
        ("NVD feed (nvd*.json[.gz])", _has("nvd", ".json")),
        ("CWE catalog (*.xml / cwec*.xml.zip)", _suffix(".xml", ".xml.zip")),
        ("ATT&CK bundle (*attack*.json)", _has("attack", ".json")),
    ],
    sources=[
        SourceSpec(
            name="nvd-feeds",
            module="okg.substrate.library.sources.nvd_feeds",
            cls="NvdFeedsSource",
            source_class="reference_catalog",
            record_identity_kind="domain_key",
            record_identity_fields=["cve_id"],
            params=lambda snap, dep, name: {
                # NvdFeedsSource takes no `repo`: CVE ids are globally
                # unique, so its node ids are not corpus-namespaced.
                "snapshot_glob": f"{snap}/nvd*.json*",
            },
        ),
        SourceSpec(
            name="cwe-catalog",
            module="okg.substrate.library.sources.cwe_catalog",
            cls="CweCatalogSource",
            source_class="reference_catalog",
            record_identity_kind="domain_key",
            record_identity_fields=["cwe_id"],
            params=lambda snap, dep, name: {
                "snapshot_path": _one(snap, ("cwec", ".xml"), "CWE catalog"),
            },
        ),
        SourceSpec(
            name="attack-stix",
            module="okg.substrate.library.sources.attack_stix",
            cls="AttackStixSource",
            source_class="reference_catalog",
            record_identity_kind="domain_key",
            record_identity_fields=["technique_id"],
            params=lambda snap, dep, name: {
                "snapshot_path": _one(snap, ("attack", ".json"), "ATT&CK bundle"),
            },
        ),
    ],
)

# PACT okg-corpus-xbrl / req.xbrl.contract.
_XBRL_CONTRACT = CorpusContract(
    subtypes=(
        "filing", "xbrl_concept", "xbrl_fact", "xbrl_statement", "xbrl_context",
    ),
    triples=(
        TripleClause("filing", "contains", "xbrl_fact", 134),
        TripleClause("xbrl_fact", "reports", "xbrl_concept", 134),
        TripleClause("xbrl_concept", "calc_sums_to", "xbrl_concept", 118),
        TripleClause("xbrl_fact", "in_context", "xbrl_context", 134,
                     note="exactly one context per fact node, so the floor "
                          "tracks the fact floor"),
        # These five were floored at 1 (and 7) because the counts were "not
        # derivable before the first real build". TWO real builds have since
        # run, so the umbrella's tightening rule applies and the placeholder
        # floors are replaced by measured ones. A floor of 1 catches total
        # absence and nothing else: the presentation linkbase could regress
        # from 463 edges to 2 and still pass. Unlike doc-corpus, whose
        # content is an open operator decision, xbrl's snapshot is fixed and
        # measured, so it has no excuse for a placeholder.
        TripleClause("filing", "contains", "xbrl_context", 68,
                     note="observed 76; >=1 per filing with a parseable "
                          "instance, and 18 of the 25 filings are truncated "
                          "mid-instance"),
        TripleClause("filing", "contains", "xbrl_statement", 18,
                     note="observed 20; >=1 per filing with a parseable "
                          "presentation linkbase"),
        TripleClause("xbrl_statement", "presents", "xbrl_concept", 420,
                     note="observed 463"),
        TripleClause("xbrl_concept", "presentation_parent_of", "xbrl_concept",
                     185, note="observed 206"),
        TripleClause("xbrl_concept", "dim_related_to", "xbrl_concept", 115,
                     note="observed 127"),
        # NOTE: `filing -declares-> xbrl_concept` IS emitted by the source
        # (a filer's schema declaring an extension concept is a real fact),
        # but it is NOT a contract clause: measured against this dataset
        # export, not one of the 25 schemas declares an xsd:element — they
        # carry only imports and role types — so a min>=1 clause would be
        # permanently unsatisfiable here. It fires for unpacked EDGAR
        # filings, which do declare extensions.
    ),
    max_isolated_fraction=0.01,
    # NOT zero_isolated_subtypes=("filing",): the first real build proved
    # that unsatisfiable. 5 of 25 filings have neither a parseable instance
    # nor a parseable presentation linkbase (the dataset's 32,767-byte cap),
    # and their schemas declare no extension concepts, so NO source-carried
    # structure reaches them — anchoring them would mean inventing a fact.
    # The honest clause is the negative probe below: no filing may be
    # isolated for a reason other than upstream truncation. That refuses a
    # parsing regression while admitting a documented, per-filing-flagged
    # upstream limit, instead of a blanket "some isolated filings are fine".
    probes=(
        Probe(
            "no filing is isolated for any reason but upstream truncation",
            "SELECT 1 FROM okg.graph_nodes n WHERE n.subtype = 'filing' "
            "AND NOT EXISTS (SELECT 1 FROM okg.graph_edges e "
            "  WHERE e.src = n.node_id OR e.dst = n.node_id) "
            "AND coalesce((n.attrs->>'structurally_stranded')::boolean, false) "
            "IS NOT TRUE LIMIT 1",
            expect_empty=True,
            note="an isolated filing must carry structurally_stranded=true, "
                 "i.e. no parseable instance AND no parseable presentation",
        ),
        Probe(
            "FinSM: a labelled concept reaches its statement and filing",
            "SELECT 1 FROM okg.graph_edges pe "
            "JOIN okg.graph_nodes st ON st.node_id = pe.src "
            "JOIN okg.graph_nodes c ON c.node_id = pe.dst "
            "JOIN okg.graph_edges fe ON fe.dst = st.node_id "
            "JOIN okg.graph_nodes f ON f.node_id = fe.src "
            "WHERE pe.edge_type='presents' AND st.subtype='xbrl_statement' "
            "AND c.subtype='xbrl_concept' AND c.attrs ? 'label' "
            "AND fe.edge_type='contains' AND f.subtype='filing' LIMIT 1",
            note="the prose->concept join FinSM scores",
        ),
        Probe(
            "FinRE: a concept pair returns a relationship carrying its kind",
            "SELECT 1 FROM okg.graph_edges e "
            "JOIN okg.graph_nodes s ON s.node_id = e.src "
            "JOIN okg.graph_nodes d ON d.node_id = e.dst "
            "WHERE s.subtype='xbrl_concept' AND d.subtype='xbrl_concept' "
            "AND e.edge_type IN "
            "('calc_sums_to','presentation_parent_of','dim_related_to') LIMIT 1",
        ),
        Probe(
            "FinMR: a fact is addressable by concept AND period, with components",
            "SELECT 1 FROM okg.graph_edges rf "
            "JOIN okg.graph_nodes fact ON fact.node_id = rf.src "
            "JOIN okg.graph_nodes c ON c.node_id = rf.dst "
            "JOIN okg.graph_edges ce ON ce.src = c.node_id "
            "  AND ce.edge_type='calc_sums_to' "
            "JOIN okg.graph_edges ctx ON ctx.src = fact.node_id "
            "  AND ctx.edge_type='in_context' "
            "JOIN okg.graph_nodes cx ON cx.node_id = ctx.dst "
            "WHERE rf.edge_type='reports' AND fact.subtype='xbrl_fact' "
            "AND c.subtype='xbrl_concept' AND cx.subtype='xbrl_context' "
            "AND cx.attrs ? 'period_type' LIMIT 1",
            note="a period, not an opaque context id",
        ),
    ),
    notes=(
        "PACT okg-corpus-xbrl / req.xbrl.contract. Floors 134/134/118 are the "
        "counts the requirement states. Two clauses of that requirement are "
        "NOT expressible here and are proven in pytest: the label-coverage "
        "floor over fact-referenced concepts (an attribute-fraction clause), "
        "and the gold boundary that no attr may mark WHICH concept or "
        "relationship is the error."
    ),
)

XBRL = VerticalSpec(
    name="xbrl",
    contract=_XBRL_CONTRACT,
    modules=["xbrl"],
    expected_families=["filing", "xbrl_concept", "xbrl_fact"],
    description=(
        "SEC EDGAR/XBRL filing document sets — the financial vertical. "
        "Consumes the docsets FinAuditingAdapter.export_docsets writes, and "
        "equally filings unpacked from EDGAR."
    ),
    required_artifacts=[("docsets/<filing>/<kind>.xml", _has("docsets"))],
    sources=[
        SourceSpec(
            name="edgar-xbrl",
            module="okg.substrate.library.sources.edgar_xbrl",
            cls="EdgarXbrlSource",
            source_class="reference_catalog",
            record_identity_kind="domain_key",
            record_identity_fields=["filing_id"],
            params=lambda snap, dep, name: {
                "repo": name,
                "docsets_root": str(snap / "docsets"),
                # FinAuditing embeds each XBRL document in a dataset row and
                # caps it at 32,767 bytes, so ~20% end mid-tag (1,203 of
                # 6,157 verified 2026-07-27). Skip-and-count those rather
                # than refuse the corpus over an upstream cap; the count is
                # stamped on the filing node and in last_run_stats.
                "truncated_documents": "skip_counted",
            },
        ),
    ],
)

# PACT okg-corpus-literature / req.lit.contract.
_LITERATURE_CONTRACT = CorpusContract(
    subtypes=("paper", "abstract_sentence"),
    triples=(
        TripleClause("paper", "contains", "abstract_sentence", 5183,
                     note="one edge per sentence in the snapshot; the "
                          "per-sentence total is a snapshot quantity this "
                          "schema cannot express, so the floor is SciFact's "
                          "5,183 abstracts at >=1 sentence each"),
        TripleClause("abstract_sentence", "mentions", "concept", 1,
                     note="req.lit.structure's derived concept relation"),
        TripleClause("paper", "shares_concept", "paper", 1,
                     note="req.lit.structure's cross-paper relation — the one "
                          "literature shape that exercises Reconcile"),
    ),
    #: req.lit.contract leaves the number to the vertical. Zero is the honest
    #: ceiling here: a paper contains sentences, a sentence is contained and
    #: mentions concepts, a concept is mentioned — every declared subtype is
    #: connected by construction, and req.lit.sentence already fails loudly on
    #: a paper with an empty abstract.
    max_isolated_fraction=0.0,
    zero_isolated_subtypes=("paper", "abstract_sentence"),
    probes=(
        Probe(
            "SciFact gold unit: a sentence is addressable by (doc_id, index)",
            "SELECT 1 FROM okg.graph_edges e "
            "JOIN okg.graph_nodes p ON p.node_id = e.src "
            "JOIN okg.graph_nodes s ON s.node_id = e.dst "
            "WHERE p.subtype='paper' AND e.edge_type='contains' "
            "AND s.subtype='abstract_sentence' "
            "AND s.attrs ? 'sentence_index' AND s.attrs ? 'doc_id' LIMIT 1",
            note="gold is a (doc_id, sentence_index) pair, not a paper",
        ),
    ),
    notes=(
        "PACT okg-corpus-literature / req.lit.contract. Gold exclusion "
        "(claims files never enter the corpus or the snapshot dir) is a "
        "property of the INPUTS, not of the published generation, so it is "
        "proven by the snapshot inventory and pytest rather than a probe."
    ),
)

LITERATURE = VerticalSpec(
    name="literature",
    contract=_LITERATURE_CONTRACT,
    modules=["literature"],
    expected_families=["paper"],
    description=(
        "Paper-abstract corpus — the scientific-literature vertical "
        "(SciFact's 5,183 abstracts in miniature)."
    ),
    required_artifacts=[("corpus.jsonl", _has("corpus.jsonl"))],
    sources=[
        SourceSpec(
            name="paper-abstracts",
            module="okg.substrate.library.sources.paper_abstracts",
            cls="PaperAbstractsSource",
            source_class="reference_catalog",
            record_identity_kind="domain_key",
            record_identity_fields=["doc_id"],
            params=lambda snap, dep, name: {
                "repo": name,
                "corpus_jsonl": str(snap / "corpus.jsonl"),
            },
        ),
    ],
)

# PACT okg-corpus-legal / req.legal.contract.
_LEGAL_CONTRACT = CorpusContract(
    subtypes=("statute_passage", "statute_section", "jurisdiction"),
    triples=(
        # Floors TIGHTENED from the first real build (umbrella rule), which
        # ran over the canonical slice: 28,576 released Missouri rows
        # Range-fetched from the 3.36 GB pool. Observed 2026-07-28:
        # part_of 28,576 / section part_of 29,021 / in_jurisdiction 28,576 /
        # cross_references 23,943. Floors sit at ~85% of observed to absorb
        # upstream churn. A DIFFERENT slice (another state, a smaller cut)
        # needs its own floors re-derived from its first build — these are
        # keyed to this snapshot, not to statute corpora in general.
        TripleClause("statute_passage", "part_of", "statute_section", 24000,
                     note="one per passage whose released path is non-empty "
                          "(observed 28,576 of 28,576 — no section orphans)"),
        TripleClause("statute_section", "part_of", "statute_section", 24000,
                     note="one per non-root section (observed 29,021)"),
        TripleClause("statute_passage", "in_jurisdiction", "jurisdiction", 24000,
                     note="exactly the passage count (observed 28,576)"),
        TripleClause("statute_section", "in_jurisdiction", "jurisdiction", 1,
                     note="one per root section; the root count is a snapshot "
                          "quantity, so the floor is >0"),
        TripleClause("statute_passage", "cross_references", "statute_passage",
                     15000,
                     note="req.legal.crossref: zero resolved references in a "
                          "slice large enough to contain them fails the build "
                          "(observed 23,943 real statutory cross-citations)"),
    ),
    #: req.legal.contract: an isolated-node ceiling of ZERO — every passage
    #: reaches at least its jurisdiction, so any isolated node is a defect.
    max_isolated_fraction=0.0,
    zero_isolated_subtypes=(
        "statute_passage", "statute_section", "jurisdiction",
    ),
    probes=(
        Probe(
            "housing_qa gold unit: statute_idx -> passage -> section -> state",
            "SELECT 1 FROM okg.graph_nodes p "
            "JOIN okg.graph_edges pe ON pe.src = p.node_id "
            "  AND pe.edge_type='part_of' "
            "JOIN okg.graph_nodes sec ON sec.node_id = pe.dst "
            "  AND sec.subtype='statute_section' "
            "JOIN okg.graph_edges je ON je.src = p.node_id "
            "  AND je.edge_type='in_jurisdiction' "
            "JOIN okg.graph_nodes j ON j.node_id = je.dst "
            "  AND j.subtype='jurisdiction' "
            "WHERE p.subtype='statute_passage' AND p.attrs ? 'statute_idx' "
            "LIMIT 1",
            note="a passage the gold can name, with its scoping section and "
                 "its state reachable as structure rather than as a string",
        ),
    ),
    notes=(
        "PACT okg-corpus-legal / req.legal.contract. The SEPARATE 2% "
        "section-orphan ceiling (passages with an empty released path, "
        "reported with their statute_idx values) is not expressible as a "
        "clause here — this schema has an isolation ceiling but no "
        "per-edge-type orphan ceiling — so it is folded into the part_of "
        "floor above and reported in full by the build. Gold exclusion "
        "(questions / questions_aux never enter the snapshot) is an input "
        "property, proven by the snapshot inventory."
    ),
)

LEGAL = VerticalSpec(
    name="legal",
    contract=_LEGAL_CONTRACT,
    modules=["legal"],
    expected_families=["statute_passage"],
    description=(
        "State-statute passage pool — the legal/gov vertical. The released "
        "pool is 3.36 GB; the pilot corpus is one state's slice."
    ),
    required_artifacts=[("statutes*.tsv", _has("statutes", ".tsv"))],
    sources=[
        SourceSpec(
            name="statute-corpus",
            module="okg.substrate.library.sources.statute_corpus",
            cls="StatuteCorpusSource",
            source_class="reference_catalog",
            record_identity_kind="domain_key",
            record_identity_fields=["idx"],
            params=lambda snap, dep, name: {
                "repo": name,
                "statutes_tsv": _one(snap, ("statutes", ".tsv"), "statute TSV"),
                # The released reglab/housing_qa pool separates path
                # segments with "|", not ">" (verified against real rows:
                # "2021 Missouri Revised Statutes|Title I - …|Chapter 1 - …").
                # The source refuses rather than flattening when no row
                # splits, so this must match the actual artifact.
                "path_separator": "|",
            },
        ),
    ],
)

# PACT okg-corpus-ops / req.ops.contract.
#
# PROVENANCE, RESTATED 2026-07-29. Every clause below is now met from PUBLISHED
# artifacts: the Apache-2.0 ITBench clone at pinned ref 7c68548 (scenarios,
# application library, Prometheus rule templates) and the Apache-2.0
# `ibm-research/ITBench-Lite` telemetry release (35 SRE scenario snapshots,
# 1,980 files / 1.576 GB, complete). The CC-BY-NC ITBench-Trajectories
# dependency this contract used to carry is GONE, and so are the two synthetic
# stand-in runs that stood for it.
#
# THREE CLAUSES WERE REMOVED, and their removal is a finding, not a
# convenience. Each was stated against ITBench-Trajectories — three recorded
# AGENT ATTEMPTS per scenario — and ITBench-Lite is a DIFFERENT ARTIFACT: one
# telemetry snapshot per scenario, no agent, no trajectory. Softening their
# floors would have counted something else under their names.
#
#   `ops_scenario -recorded_by-> telemetry_run` @105 -> restated to 34. 105
#   meant "35 scenarios x 3 runs". The release publishes 35 snapshots, of
#   which 34 name a scenario the 66-index tree defines; Scenario-9 is
#   published with NO index, so it carries a telemetry_run and no
#   recorded_by edge. A floor of 35 would be permanently unmeetable for a
#   reason unrelated to the data, and one of 34 that quietly excluded it is
#   where a later regression losing a real scenario would hide — so the node
#   itself carries `scenario_indexed=false` and the run report names it.
#
#   `telemetry_run -observed-> ops_alert` @105 -> DELETED, premise falsified.
#   The recorded and defined alert vocabularies are DISJOINT: re-measured on
#   the complete release, 18 distinct observed alertnames against 9 distinct
#   defined names (17 rule nodes), intersection EMPTY. Not a rename — every
#   revision of `prometheusrules.j2` in the public repo's history defines the
#   `High*` names, including `e9248312`, the last commit before the
#   2025-12-15 capture, and the thresholds differ. The observation family is
#   `alert_firing`, and there is deliberately NO edge from it to a
#   definition. The finding is not erased by the deletion: it is stamped in
#   the ops schema, in `_snapshot_facts`, and in the disjointness probe
#   below, which FAILS if anyone reconciles the two vocabularies.
#
#   `telemetry_signal -about-> ops_service` @105 -> DELETED with its subtype.
#   `telemetry_signal` indexes a TRAJECTORY run's observability artifacts;
#   the corpus no longer ingests trajectories, so the subtype publishes zero
#   rows. Keeping it in `subtypes` would have failed the build for an
#   artifact the corpus deliberately does not carry, and keeping the triple
#   at a nonzero floor would have been unmeetable. The source's trajectory
#   path, the ontology class and its narrowing all remain and stay under
#   test — this is a statement about THIS corpus's inputs, not a retraction.
#
# Two clauses replace them, over the artifact that actually exists.
_OPS_CONTRACT = CorpusContract(
    subtypes=(
        "ops_application", "ops_service", "ops_alert", "ops_scenario",
        "telemetry_run", "alert_firing",
    ),
    triples=(
        # ---- the application topology, from the Apache-2.0 clone ----
        TripleClause(
            "ops_scenario", "deploys", "ops_application", 66,
            note="observed 66 — one per scenario index; 66 indexes at pinned "
                 "ref 7c68548 (64 sre + 2 finops), each declaring exactly one "
                 "application. Exact rather than slack: the count is fixed by "
                 "the pinned tree, so any headroom would only hide a lost "
                 "scenario",
        ),
        TripleClause(
            "ops_application", "runs", "ops_service", 21,
            note="observed 23 — 21 opentelemetry-demo workloads read from the "
                 "telemetry's own k8s_objects_raw.tsv dump (Deployment / "
                 "StatefulSet / DaemonSet, unioned across all 35 snapshots so "
                 "no scenario's injected deletion shrinks its own roster) plus "
                 "2 book-info from the helm reconstruction, which has no "
                 "published telemetry. Floor 21, raised from 14: the "
                 "reconstruction-only path yields exactly 14, so any floor "
                 "above it refuses a silent reversion to the 12-component "
                 "helm guess, and 21 leaves headroom for chart drift without "
                 "forgiving that reversion",
        ),
        TripleClause(
            "ops_application", "defines", "ops_alert", 17,
            note="observed 17: 10 otel-demo + 7 book-info. The shared "
                 "common/prometheusrules.j2 renders per application, and its "
                 "otel-demo rendering is byte-identical to that "
                 "application's own FailedPodsDetected rule, so it merges "
                 "rather than double-counting",
        ),
        TripleClause(
            "ops_scenario", "raises", "ops_alert", 24,
            note="observed 24. 30 of 66 scenarios fire an alert (33 entries, 7 "
                 "distinct names); 13 entries resolve to a definition and "
                 "fan out to 24 edges because HighRequestErrorRate and "
                 "HighRequestLatency are each defined in two rule groups. "
                 "The other 20 entries name kube-prometheus-stack rules "
                 "(KubePodNotReady x13, KubePodCrashLooping x3, "
                 "KubeContainerWaiting x3, KubeHpaMaxedOut x1) that the "
                 "ITBench tree does not define anywhere; they are reported "
                 "on the scenario node's unresolved_alert_names, never "
                 "stubbed",
        ),
        TripleClause(
            "ops_alert", "targets", "ops_service", 5,
            note="observed 5. Only POSITIVE label matchers resolve a target: "
                 "otel-demo's two frontend golden-signal rules select "
                 "frontend|frontend-proxy (4 edges) and NoDatabaseOperations "
                 "selects accounting (1). The other 14 of 17 rules select on "
                 "namespace alone or by exclusion, and carry "
                 "targets_resolved=false plus target_selector_kind saying "
                 "which. `accounting` is why the roster is built from "
                 "WORKLOAD kinds and not from Service objects: it is a Kafka "
                 "consumer with no inbound Service, and a Service-based "
                 "roster would silently break the one targets edge that "
                 "resolves",
        ),
        # ---- the published telemetry, from ibm-research/ITBench-Lite ----
        TripleClause(
            "ops_scenario", "recorded_by", "telemetry_run", 34,
            note="observed 34 of 35 published snapshots. Exact, not slack: "
                 "the 35th is Scenario-9, published with no scenario index, "
                 "and it is EMITTED with scenario_indexed=false rather than "
                 "subtracted quietly. Restated from 105, which counted "
                 "ITBench-Trajectories agent attempts — a different artifact "
                 "this corpus does not ingest",
        ),
        TripleClause(
            "telemetry_run", "observed", "alert_firing", 1000,
            note="observed 1,146 distinct firings, keyed (scenario, "
                 "alertname, activeAt). The alerts directory is a ~1/min "
                 "poll, so 16,128 raw entries across 1,872 files dedup to "
                 "1,146; counting files would inflate the corpus by two "
                 "orders of magnitude. Floor 1,000 leaves headroom for poll "
                 "jitter between release versions while still refusing a "
                 "parser that lost a whole scenario's alerts (the largest "
                 "single snapshot contributes far less than 146)",
        ),
        TripleClause(
            "alert_firing", "targets", "ops_service", 350,
            note="observed 390 — the firings whose service label names a "
                 "workload of the application. The other 756 are stamped, not "
                 "dropped: 484 service_outside_topology (overwhelmingly "
                 "monitoring infrastructure — prometheus-kube-prometheus-"
                 "kubelet x429, -prometheus x38, kube-state-metrics x17) and "
                 "272 no_service_label (cluster-scoped alerts, about no "
                 "service by construction). Floor 350: this is the ONLY "
                 "clause that observes a symptom reaching a component, so it "
                 "carries the least slack of the telemetry clauses",
        ),
        # NOTE: `ops_service -depends_on-> ops_service` is NOT a clause, and
        # its absence is deliberate. The PACT assumed the environment block
        # would declare inter-application relations. Measured: it declares
        # none, and neither does the application library — the ITBench tree
        # carries no inter-service dependency anywhere. A min>=1 clause would
        # therefore be permanently unsatisfiable, and satisfying it would mean
        # INVENTING structure, which the umbrella forbids. The relation is
        # real but must be DERIVED from parent/child service pairs in the
        # recorded trace spans, which per the umbrella's ordering rule comes
        # after the source-carried structure lands — and needs the
        # Trajectories release. It is added, marked derived, in the enricher
        # follow-up.
    ),
    # SECONDARY guard only. Observed 0.000 (0 isolated of 1,289) — the sole
    # admissible isolate this module ever had was a telemetry_signal whose
    # subject did not resolve, and this corpus builds no telemetry_signal. The
    # ceiling is kept at 0.01 rather than 0.0 so that it stays a BLOW-UP guard
    # and not a duplicate of the per-subtype rules below, which are the real
    # gate along with the unstated-reason probe. The previous 0.05 was sized
    # for the 103-node stand-in build and is far too loose for 1,289 nodes:
    # it would have forgiven 64 isolates.
    max_isolated_fraction=0.01,
    # EVERY subtype the corpus publishes is required connected, and every one
    # of them is also in `subtypes` above, so `unbacked_isolation_rules()` is
    # empty — "zero isolated X" is vacuously true of a corpus holding no X,
    # and the cti attack_campaign defect was exactly that gap.
    zero_isolated_subtypes=(
        "ops_application", "ops_service", "ops_alert", "ops_scenario",
        "telemetry_run", "alert_firing",
    ),
    probes=(
        Probe(
            "no node is isolated for an UNSTATED reason",
            "SELECT n.subtype, n.node_id FROM okg.graph_nodes n "
            "WHERE NOT EXISTS (SELECT 1 FROM okg.graph_edges e "
            "  WHERE e.src = n.node_id OR e.dst = n.node_id) "
            "AND NOT (n.subtype = 'telemetry_signal' "
            "         AND coalesce(n.attrs->>'no_subject_reason', '') "
            "             IN ('artifact_empty', 'no_subject_declared'))",
            expect_empty=True,
            note="REWRITTEN after adversarial review. The old form keyed on "
                 "`subject_resolved`, which the source computed as "
                 "bool(subject) — the SAME expression that gated the `about` "
                 "edge — so every isolated signal was necessarily exempt and "
                 "the complement was empty (2 of 2 measured). It forgave "
                 "precisely the subject-parsing regression this note claimed "
                 "it caught. `no_subject_reason` is derived from the parsed "
                 "artifact and OMITTED when the artifact named a resolvable "
                 "service, so an isolate with no stamp is a lost edge. "
                 "`declared_subject_unknown` is deliberately NOT exempt: an "
                 "unresolvable name is a finding, and the source raises when "
                 "a run resolves none of a floor's worth. The coalesce is "
                 "load-bearing — without it NULL IN (...) is NULL, "
                 "NOT(true AND NULL) is NULL, and WHERE drops the row, so a "
                 "deleted stamp would exempt again. The telemetry_signal arm "
                 "is currently INERT — this corpus ingests no trajectories, "
                 "so 0 signals exist and the exemption forgives nothing. It "
                 "is retained for the trajectory path, not counted as work",
        ),
        Probe(
            "positive: a scenario reaches its application, its full service "
            "topology, and its alerts",
            "SELECT 1 FROM okg.graph_nodes sc "
            "JOIN okg.graph_edges de ON de.src = sc.node_id "
            "  AND de.edge_type='deploys' "
            "JOIN okg.graph_nodes app ON app.node_id = de.dst "
            "  AND app.subtype='ops_application' "
            "JOIN okg.graph_edges re ON re.src = app.node_id "
            "  AND re.edge_type='runs' "
            "JOIN okg.graph_nodes svc ON svc.node_id = re.dst "
            "  AND svc.subtype='ops_service' "
            "JOIN okg.graph_edges ra ON ra.src = sc.node_id "
            "  AND ra.edge_type='raises' "
            "JOIN okg.graph_nodes al ON al.node_id = ra.dst "
            "  AND al.subtype='ops_alert' "
            "WHERE sc.subtype='ops_scenario' LIMIT 1",
            note="two hops from the scenario to the component vocabulary an "
                 "answer is graded against. A corpus that hides that "
                 "vocabulary makes the task unsolvable — a defect, not "
                 "safety",
        ),
        Probe(
            "negative: no gold-bearing attribute reached the generation",
            "SELECT 1 FROM okg.graph_nodes WHERE attrs ?| array["
            "'disruptions','solutions','injection','injected','fault_kind',"
            "'gold_solution','solution_command','misconfigured_key',"
            "'misconfigured_value','description_text','symptom'] LIMIT 1",
            expect_empty=True,
            note="the injection's fault kind, its misconfigured key/value, "
                 "every gold solution command, and the scenario's prose "
                 "description stay out. NOTE ops_alert.description is the "
                 "RULE's annotation, keyed by application and identical "
                 "across every scenario using it — hence the distinct key "
                 "names above",
        ),
        Probe(
            "negative: no alert DEFINITION was stubbed for an undefined name",
            "SELECT 1 FROM okg.graph_nodes n WHERE n.subtype='ops_alert' "
            "AND coalesce(n.attrs->>'expr', '') = '' LIMIT 1",
            expect_empty=True,
            note="20 of the 33 upstream alert entries name a rule the "
                 "ITBench tree does not define. They are reported on the "
                 "scenario's unresolved_alert_names, never turned into an "
                 "expr-less node that would sit in the retrieval candidate "
                 "pool the benchmark scores. DELIBERATELY SCOPED TO "
                 "ops_alert and NOT widened to alert_firing: for a "
                 "definition an empty expr means a stub, while for a firing "
                 "an expr of any kind is the defect. The two subtypes need "
                 "OPPOSITE assertions, which is why the next probe exists "
                 "rather than this one covering both",
        ),
        Probe(
            "negative: a firing is an OBSERVATION and never acquires a "
            "definition's expr",
            "SELECT n.node_id FROM okg.graph_nodes n "
            "WHERE n.subtype='alert_firing' "
            "AND (coalesce(n.attrs->>'alert_name', '') = '' "
            "     OR coalesce(n.attrs->>'active_at', '') = '' "
            "     OR n.attrs ? 'expr') LIMIT 1",
            expect_empty=True,
            note="THE VOCABULARY DISJOINTNESS WRITTEN AS A GATE — do not "
                 "delete this as redundant with the probe above; they assert "
                 "opposite things. Re-measured on the complete release: 18 "
                 "distinct RECORDED alertnames, 9 distinct DEFINED names, "
                 "intersection EMPTY, and the skew is not a rename (every "
                 "revision of prometheusrules.j2 defines the High* names, "
                 "and where the prose matches the THRESHOLD does not). So no "
                 "firing can be joined to the rule that produced it, and the "
                 "corpus states no such join: there is no narrowing between "
                 "the subtypes, so an EDGE would be refused at insert, and "
                 "this probe covers the other route — copying the rule body "
                 "onto the firing. A future attempt to helpfully reconcile "
                 "the two vocabularies fails here instead of quietly "
                 "asserting a correspondence neither artifact states. The "
                 "two positive conjuncts are the firing's identity: "
                 "alert_name and active_at are what its node id is keyed on, "
                 "so an empty one means two firings collapsed into one. "
                 "Measured: 0 empty of 1,146 on both",
        ),
        Probe(
            "negative: a firing's no_target_reason agrees with the edges "
            "that exist, in BOTH directions",
            "SELECT n.node_id FROM okg.graph_nodes n "
            "WHERE n.subtype='alert_firing' "
            "AND (n.attrs ? 'no_target_reason') IS DISTINCT FROM "
            "NOT EXISTS (SELECT 1 FROM okg.graph_edges e "
            "  WHERE e.src = n.node_id AND e.edge_type='targets') LIMIT 1",
            expect_empty=True,
            note="A firing always carries its `observed` edge, so it is "
                 "never ISOLATED — the isolation probe above cannot see a "
                 "lost `targets` edge on a firing, and without this clause "
                 "only the 350 floor would. This is the biconditional: "
                 "stamped exactly when untargeted. Left-to-right catches a "
                 "lost edge (untargeted and unstamped); right-to-left "
                 "catches a stamp that forgives a firing the source did "
                 "relate. `no_target_reason` is read from the PARSED FIRING "
                 "and never from whether the edge came out, so the "
                 "biconditional is a real constraint and not a tautology. "
                 "Measured: 756 stamped == 756 untargeted, and the "
                 "complement is 390 targeted-and-unstamped — non-empty, "
                 "which is what makes it discriminating",
        ),
        Probe(
            "positive: a recorded symptom reaches the component vocabulary "
            "an answer is graded against",
            "SELECT 1 FROM okg.graph_nodes sc "
            "JOIN okg.graph_edges rb ON rb.src = sc.node_id "
            "  AND rb.edge_type='recorded_by' "
            "JOIN okg.graph_nodes run ON run.node_id = rb.dst "
            "  AND run.subtype='telemetry_run' "
            "JOIN okg.graph_edges ob ON ob.src = run.node_id "
            "  AND ob.edge_type='observed' "
            "JOIN okg.graph_nodes fi ON fi.node_id = ob.dst "
            "  AND fi.subtype='alert_firing' "
            "JOIN okg.graph_edges tg ON tg.src = fi.node_id "
            "  AND tg.edge_type='targets' "
            "JOIN okg.graph_nodes svc ON svc.node_id = tg.dst "
            "  AND svc.subtype='ops_service' "
            "WHERE sc.subtype='ops_scenario' LIMIT 1",
            note="the benchmark's actual question shape (a): given the "
                 "alerts that fired, which component is faulty. The "
                 "definition-side positive probe above traverses what the "
                 "library DECLARES; this one traverses what the telemetry "
                 "OBSERVED, and the two paths are disjoint by construction "
                 "since no edge joins a firing to a definition. Without it "
                 "the whole telemetry subgraph would be counted but never "
                 "shown to be reachable from a scenario",
        ),
        Probe(
            "negative: targets_resolved agrees with the edges that exist",
            "SELECT 1 FROM okg.graph_nodes n WHERE n.subtype='ops_alert' "
            "AND (n.attrs->>'targets_resolved')::boolean IS DISTINCT FROM "
            "EXISTS (SELECT 1 FROM okg.graph_edges e "
            "  WHERE e.src = n.node_id AND e.edge_type='targets') LIMIT 1",
            expect_empty=True,
            note="a source-stamped flag that disagreed with the graph would "
                 "let an unresolved alert read as resolved — absence dressed "
                 "as agreement, the defect class this whole PACT exists to "
                 "remove",
        ),
        Probe(
            "negative: every service of an application has the same "
            "attribute shape as its peers",
            "SELECT 1 FROM ("
            "  SELECT n.attrs->>'application_id' AS app, "
            "         count(DISTINCT ("
            "           SELECT string_agg(k, ',' ORDER BY k) "
            "           FROM jsonb_object_keys(n.attrs) k)) AS shapes "
            "  FROM okg.graph_nodes n WHERE n.subtype='ops_service' "
            "  GROUP BY 1) s WHERE s.shapes > 1 LIMIT 1",
            expect_empty=True,
            note="the umbrella's gold rule for this vertical is 'nothing "
                 "distinguishes the injected component from its peers'. "
                 "This is that rule as a query: no attr may be emitted only "
                 "for some services of an application",
        ),
    ),
    notes=(
        "PACT okg-corpus-ops / req.ops.contract. Every floor is MEASURED on "
        "the real build, never guessed: 66 / 23 / 17 / 24 / 5 from the "
        "Apache-2.0 clone at pinned ref 7c68548, and 34 / 1,146 / 390 from "
        "the complete Apache-2.0 ibm-research/ITBench-Lite telemetry slice "
        "(1,980 files, 1.576 GB, 0 fetch failures). Floors sit at or below "
        "each observed value: 66/17/24/5/34 are exact because the pinned "
        "input fixes them and headroom would only hide a loss; 21/1000/350 "
        "carry slack, each justified in its own note by naming the "
        "regression the floor must still refuse. Two clauses of the "
        "requirement are NOT expressible as a probe and are proven in pytest "
        "(tests/substrate/test_ops_telemetry_source.py): STRIPPED-GOLD "
        "EQUIVALENCE compares TWO builds, and its non-vacuity proof plants a "
        "leak the substring scan cannot see. Three further pairs prove the "
        "no_target_reason stamp DISCRIMINATES — for each of its three "
        "values, a mutation that drops the stamp and one that forces it, "
        "both red."
    ),
)

OPS = VerticalSpec(
    name="ops",
    contract=_OPS_CONTRACT,
    modules=["ops"],
    # All six published families, telemetry included. `telemetry_run` was
    # deliberately absent while the recorded telemetry was believed to need
    # the operator-supplied CC-BY-NC release; ITBench-Lite is Apache-2.0 and
    # published, so its absence would now be an unstated gap rather than a
    # stated one. `telemetry_signal` is NOT here: it indexes trajectory runs,
    # which this corpus does not ingest.
    expected_families=[
        "ops_application", "ops_service", "ops_alert", "ops_scenario",
        "telemetry_run", "alert_firing",
    ],
    description=(
        "ITBench SRE scenario definitions resolved through the application "
        "library (components + Prometheus alert definitions), plus the "
        "published ITBench-Lite telemetry — the computing-operations "
        "vertical. Gold is excluded by MARKING, not absence: the corpus must "
        "hold the component vocabulary an answer is graded against, and does "
        "so through a catalog built from the library and from a workload "
        "roster UNIONED across every scenario, so it is byte-identical no "
        "matter which scenario is read."
    ),
    required_artifacts=[
        ("scenarios/<n>.json", _has("scenarios")),
        ("library/library/indexes/applications/<n>.json", _has("library")),
        ("telemetry/snapshots/<domain>/<version>/Scenario-<n>/",
         _has("telemetry", "scenario-")),
    ],
    sources=[
        SourceSpec(
            name="ops-telemetry",
            module="okg.substrate.library.sources.ops_telemetry",
            cls="OpsTelemetrySource",
            source_class="reference_catalog",
            record_identity_kind="domain_key",
            record_identity_fields=["scenario_number"],
            # Each telemetry root is passed ONLY when the snapshot holds one.
            # The topology and alert families build from the clone alone, and
            # passing a non-existent directory raises rather than silently
            # producing an empty telemetry layer. `telemetry` is the
            # published ITBench-Lite release; `trajectories` is the recorded
            # agent-attempt layout, which this snapshot no longer carries but
            # the source still reads.
            params=lambda snap, dep, name: {
                "repo": name,
                "scenarios_root": str(snap / "scenarios"),
                "library_root": str(snap / "library"),
                **({"telemetry_root": str(snap / "telemetry")}
                   if (snap / "telemetry").is_dir() else {}),
                **({"trajectories_root": str(snap / "trajectories")}
                   if (snap / "trajectories").is_dir() else {}),
            },
        ),
    ],
)

# The doc-corpus deployment's extractor chain. Deliberately the SMALLEST
# chain that turns prose into passages: markdown + plain text parsers, a
# heading-aware chunker for markdown, a window chunker for everything else.
#
# No NER backend. Entity mentions are extra nodes with no role in FreshQA's
# answer unit — an answer is attributed to a PASSAGE — and every one of them
# would enter the retrieval candidate pool the benchmark scores. No code or
# PDF parsers either: the snapshot is documents, and a parser that fires on
# something the snapshot does not contain cannot be exercised, so it would
# ship unproven.
_DOC_CORPUS_EXTRACTORS = """\
# okg-bench doc-corpus (FreshQA) extractor chain.
# PACT okg-corpus-doc-corpus / req.fresh.passage.
parser:
  - format: markdown
    class: okg.substrate.library.extraction.parse.markdown.MarkdownParser
  - format: text
    class: okg.substrate.library.extraction.parse.text.TextParser

chunker:
  - format: markdown
    class: okg.substrate.library.extraction.chunk.markdown_heading.MarkdownHeadingChunker
    config: {min_tokens: 40, max_tokens: 600}
  - format: default
    class: okg.substrate.library.extraction.chunk.window.WindowChunker
    config: {max_chars: 1200, overlap: 150}

ner: []
"""

# PACT okg-corpus-repo-history / req.repo.contract.
PROBE_FILE_TO_MAINTAINER_RATIONALE = """
SELECT f.node_id AS source_file, pr.node_id AS pull_request,
       c.node_id AS comment
  FROM okg.graph_nodes f
  JOIN okg.graph_edges ca ON ca.dst = f.node_id AND ca.edge_type = 'affects'
  JOIN okg.graph_nodes cm ON cm.node_id = ca.src AND cm.subtype = 'git_commit'
  JOIN okg.graph_edges m  ON m.dst = cm.node_id AND m.edge_type = 'merged_as'
  JOIN okg.graph_nodes pr ON pr.node_id = m.src AND pr.subtype = 'pull_request'
  JOIN okg.graph_edges hc ON hc.src = pr.node_id AND hc.edge_type = 'has_comment'
  JOIN okg.graph_nodes c  ON c.node_id = hc.dst AND c.subtype = 'forge_comment'
 WHERE f.subtype = 'source_file'
 LIMIT 1
"""

#: SWE-QA question shape (b), stated exactly as req.repo.contract words it:
#: "from an issue, reach the commits that touched the files its discussion
#: names". The issue reaches its discussion through a DERIVED reference edge,
#: and the file naming is SOURCE-CARRIED — a review comment is anchored to a
#: path by the payload, so the join is `forge_comment.path = source_file.path`
#: rather than a text guess. Five hops:
#: issue -references-> pull_request -has_comment-> forge_comment(path)
#:       ~ source_file <-affects- git_commit
#: MEASURED: non-empty (e.g. issue #970 -> PR #895 -> review comment
#: rc:1860781 -> requests/adapters.py -> commit 796d3225…).
PROBE_ISSUE_DISCUSSION_TO_COMMITS = """
SELECT i.node_id AS issue, pr.node_id AS pull_request, c.node_id AS comment,
       f.node_id AS file, cm.node_id AS touching_commit
  FROM okg.graph_nodes i
  JOIN okg.graph_edges r  ON r.src = i.node_id AND r.edge_type = 'references'
  JOIN okg.graph_nodes pr ON pr.node_id = r.dst AND pr.subtype = 'pull_request'
  JOIN okg.graph_edges hc ON hc.src = pr.node_id AND hc.edge_type = 'has_comment'
  JOIN okg.graph_nodes c  ON c.node_id = hc.dst AND c.subtype = 'forge_comment'
  JOIN okg.graph_nodes f  ON f.subtype = 'source_file'
                         AND f.attrs->>'path' = c.attrs->>'path'
  JOIN okg.graph_edges af ON af.dst = f.node_id AND af.edge_type = 'affects'
  JOIN okg.graph_nodes cm ON cm.node_id = af.src AND cm.subtype = 'git_commit'
 WHERE i.subtype = 'issue'
 LIMIT 1
"""

#: Negative probe: the gold PR is absent from EVERY forge subtype, comments
#: included. Before this PACT the leakage audit keyed only on
#: issue/pull_request, so a gold-thread comment would have passed an audit
#: that reports PASS. `parent_number` is the attr that closes it.
#: MEASURED: 0 rows.
PROBE_GOLD_ABSENT = """
SELECT node_id FROM okg.graph_nodes
 WHERE subtype IN ('issue', 'pull_request', 'forge_comment')
   AND ((attrs->>'number') = '2317' OR (attrs->>'parent_number') = '2317')
"""


_REPO_HISTORY_CONTRACT = CorpusContract(
    subtypes=(
        "git_commit",
        "source_file",
        "issue",
        "pull_request",
        "forge_comment",
    ),
    triples=(
        # -- structure that already existed; floors from the same run ------
        TripleClause(
            "git_commit", "affects", "source_file", minimum=7000,
            note="MEASURED 7182 over 3689 commits; a floor under it catches "
                 "a partial history walk",
        ),
        TripleClause(
            "git_commit", "derives_from", "git_commit", minimum=4500,
            note="MEASURED 4580 (the commit DAG)",
        ),
        TripleClause(
            "repo", "contains", "issue", minimum=1200,
            note="MEASURED 1251 pre-cutoff issues",
        ),
        TripleClause(
            "repo", "contains", "pull_request", minimum=1000,
            note="MEASURED 1039 pre-cutoff pull requests",
        ),
        TripleClause(
            "source_file", "contains", "code_symbol", minimum=850,
            note="MEASURED 888; binds the code half to the file half",
        ),
        # -- the joins this PACT adds -------------------------------------
        TripleClause(
            "issue", "has_comment", "forge_comment", minimum=7700,
            note="MEASURED 8089 issue-thread comments. Source-carried from "
                 "the /issues/comments stream, filtered PER COMMENT against "
                 "the cutoff",
        ),
        TripleClause(
            "pull_request", "has_comment", "forge_comment", minimum=4500,
            note="MEASURED 4726 PR-thread comments (conversation + review). "
                 "12815 forge_comment nodes total, 316 of them review "
                 "comments carrying a `path` anchor",
        ),
        TripleClause(
            "pull_request", "merged_as", "git_commit", minimum=590,
            note="MEASURED 624, ALL from ForgeMergeAttributionLinker reading "
                 "git's own merge-commit subjects. The payload's "
                 "merge_commit_sha contributed ZERO: all 714 pre-cutoff "
                 "merged PRs report GitHub's ephemeral TEST-MERGE sha, none "
                 "of which is a commit in this repository — dropped with a "
                 "count, never dangled",
        ),
        TripleClause(
            "git_commit", "references", "pull_request", minimum=610,
            note="MEASURED 646 derived from #N / GH-N / same-repo URL forms "
                 "in commit messages",
        ),
        TripleClause(
            "pull_request", "references", "issue", minimum=300,
            note="MEASURED 316 derived from PR titles and bodies",
        ),
        TripleClause(
            "git_commit", "references", "issue", minimum=105,
            note="MEASURED 114",
        ),
        TripleClause(
            "issue", "references", "pull_request", minimum=24,
            note="MEASURED 26 — the smallest reference triple, pinned "
                 "separately so it cannot vanish behind the larger ones",
        ),
    ),
    #: MEASURED 0 isolated nodes out of 23249 (0.000). Pinned tight rather
    #: than at a comfortable default: this vertical's whole defect was a
    #: subgraph that was present but unreachable.
    max_isolated_fraction=0.005,
    zero_isolated_subtypes=(
        "forge_comment", "pull_request", "issue", "git_commit", "source_file",
    ),
    probes=(
        Probe(
            name="file -> merged PR -> maintainer comment",
            sql=PROBE_FILE_TO_MAINTAINER_RATIONALE,
            note="SWE-QA shape (a), the headline traversal. Impossible on "
                 "the pre-PACT graph: no merged_as edge and no comment node "
                 "existed, so the forge subgraph was an island. MEASURED "
                 "non-empty",
        ),
        Probe(
            name="issue -> discussion -> files it names -> commits",
            sql=PROBE_ISSUE_DISCUSSION_TO_COMMITS,
            note="SWE-QA shape (b). Exercises a derived reference edge AND a "
                 "source-carried review-comment path anchor in one traversal, "
                 "so it fails if either half regresses. MEASURED non-empty",
        ),
        Probe(
            name="gold PR absent from every forge subtype",
            sql=PROBE_GOLD_ABSENT,
            expect_empty=True,
            note="Extends the gold boundary to comments: the gold PR's whole "
                 "thread must be absent, not just its PR node. MEASURED 0 rows",
        ),
    ),
    notes=(
        "repo-history. ANSWER UNIT: a rationale grounded in what a maintainer "
        "said or did. PROVENANCE REAL — live GitHub REST API for psf/requests "
        "via the builder's existing `gh` auth path, plus the surgical checkout "
        "at base_commit. Floors read off generation "
        "okg:bench-rh-psf__requests-2317:...gen:20260728T172820854019Z:a2b126c7becc "
        "(23249 nodes / 38716 edges / 0 isolated) and pinned per-triple per "
        "the umbrella's tightening rule."
    ),
)

REPO_HISTORY = VerticalSpec(
    name="repo-history",
    contract=_REPO_HISTORY_CONTRACT,
    modules=["forge", "code"],
    expected_families=["git_commit", "source_file", "issue", "pull_request",
                       "forge_comment"],
    description=(
        "Engineering history for SWE-QA and SWE-bench Verified. UNLIKE the "
        "other six this vertical is NOT built by the spec-driven domain "
        "builder: it is built per SWE-bench instance by `okg-bench corpus "
        "build` (CorpusSpec), because its corpus is scoped to one repo at "
        "one cutoff. The spec exists so the health audit can resolve a "
        "contract for the `bench-<instance>` deployments it produces."
    ),
    required_artifacts=[],
    sources=[],
)


#: The file extensions the doc-corpus vertical PARSES into passages. One
#: constant, two consumers: the DocCorpusSource `include_globs` in the
#: DOC_CORPUS spec, and the isolation probe below. Stating the scope twice
#: is how the probe and the source drift apart, and a drifted probe
#: forgives exactly the files a parsing regression stranded.
DOC_CORPUS_SCOPE_EXTENSIONS: tuple[str, ...] = ("md", "markdown", "txt")


def _doc_corpus_scope_regex() -> str:
    return r"\.(" + "|".join(DOC_CORPUS_SCOPE_EXTENSIONS) + r")$"


def _doc_corpus_unparsed_in_scope_sql() -> str:
    """Every file the corpus CLAIMS to parse must have become a Document.

    This replaces a `zero_isolated_subtypes=("source_file",)` clause that
    MEASUREMENT showed to be vacuous. GitFilesSource emits
    `source_file -member_of-> git_branch` for every tracked file, so no
    source_file is ever isolated whether doc_corpus parsed it or not: the
    clause could not fail, and it read as proof that every file became a
    passage while proving nothing. Measured on the 2026-07-28 build, all 7
    source_files (6 parsed documents + the snapshot's own provenance.json)
    are connected and isolated=0.

    The predicate that actually binds is `derived_from`, and the exemption
    is keyed on what the INPUT declares — the file's own name against the
    scope the source is configured to parse — never on whether an edge came
    out, which would be circular.
    """
    return (
        "SELECT n.node_id, n.attrs->>'path' AS path FROM okg.graph_nodes n "
        "WHERE n.subtype = 'source_file' "
        f"AND lower(n.attrs->>'path') ~ '{_doc_corpus_scope_regex()}' "
        "AND NOT EXISTS (SELECT 1 FROM okg.graph_edges e "
        "  WHERE e.dst = n.node_id AND e.edge_type = 'derived_from')"
    )


_DOC_CORPUS_CONTRACT = CorpusContract(
    subtypes=("document", "document_chunk", "source_file"),
    triples=(
        # FLOORS OF 1, AND THEY CANNOT BE ANYTHING ELSE YET. Every sibling
        # vertical states its floor from a measured source dump; this one
        # has no dump — which documents constitute the snapshot is the open
        # operator decision this child does not get to make, so a larger
        # floor would be a number with nothing behind it. What binds
        # instead is per-node and content-independent: the zero-isolated
        # clause and the isolation probe below require EVERY document to
        # reach a chunk, EVERY chunk to reach its document, and every
        # in-scope file to reach a document. A snapshot of any size that
        # drops one passage fails those. Both floors TIGHTEN to the
        # observed counts of the operator's first real snapshot (the cti
        # child's rule).
        TripleClause("document", "contains", "document_chunk", 1,
                     note="observed 12 over 6 documents on the two-date "
                          "stand-in build (2026-07-28); NOT a floor — the "
                          "operator's content decides the real one"),
        # Provenance: a passage traces to the file it was parsed from. This
        # is the narrowing the doc_snapshot module declares and the reason
        # git_files is in the vertical at all. Without it the source_file
        # nodes are a detached file listing sitting beside the graph.
        TripleClause("document", "derived_from", "source_file", 1,
                     note="observed 6 (one per document) on the same build; "
                          "the predecessor linkage req.fresh.passage names"),
    ),
    # MEASURED 0.000 on both dated builds (26 nodes, 37 edges, 0 isolated),
    # and it is content-independent rather than a fact about the throwaway
    # set: doc_corpus connects every document to its chunks and its file,
    # and git_files connects every tracked file to the branch, so nothing in
    # this vertical's shape can strand a node at any corpus size. Keep 0.0 —
    # but see the note below on why 0.0 is WEAK for source_file.
    max_isolated_fraction=0.0,
    # A document that reaches no chunk was not parsed into passages; a chunk
    # that reaches no document is unattributable. Both are defects at any
    # corpus size, and both are real clauses here.
    #
    # source_file is DELIBERATELY NOT in this tuple, and the draft that put
    # it there was wrong. MEASURED: GitFilesSource emits
    # `source_file -member_of-> git_branch` for EVERY tracked file, so a
    # source_file is never isolated whether doc_corpus parsed it or not —
    # all 7 in the build are connected, including the snapshot's own
    # provenance.json which no parser touches. The clause could not fail. It
    # would have read as "every file became a passage" while proving
    # nothing: a check that reports verified having verified nothing. The
    # probe below is what actually binds that property.
    zero_isolated_subtypes=("document", "document_chunk"),
    probes=(
        Probe(
            "every file the corpus claims to parse became a Document",
            _doc_corpus_unparsed_in_scope_sql(),
            expect_empty=True,
            note="MEASURED on the 2026-07-28 build, the predicate "
                 "DISCRIMINATES: 6 in-scope source_files, all with a "
                 "derived_from Document; 1 out-of-scope source_file (the "
                 "snapshot's own provenance.json), with none. Zero in-scope "
                 "without, zero out-of-scope with. An exemption true of "
                 "every node would forgive everything — measure it on the "
                 "complement before trusting it (the cti child's rule).",
        ),
        Probe(
            "a chunk is an attributable, dated passage — not a filename",
            "SELECT 1 FROM okg.graph_edges e "
            "JOIN okg.graph_nodes d ON d.node_id = e.src "
            "JOIN okg.graph_nodes c ON c.node_id = e.dst "
            "WHERE d.subtype='document' AND e.edge_type='contains' "
            "AND c.subtype='document_chunk' AND c.attrs ? 'text' "
            "AND c.attrs ? 'snapshot_date' LIMIT 1",
            note="the precondition for matching a FreshQA answer to a passage",
        ),
        Probe(
            "every document and chunk carries the snapshot date (req.fresh.passage)",
            "SELECT 1 FROM okg.graph_nodes "
            "WHERE subtype IN ('document','document_chunk') "
            "AND NOT (attrs ? 'snapshot_date') LIMIT 1",
            expect_empty=True,
            note="a corpus that is one dated observation among several must "
                 "carry its date as a queryable attribute, not only in the "
                 "freeze manifest",
        ),
        Probe(
            "the snapshot states exactly ONE date",
            "SELECT 1 FROM (SELECT DISTINCT attrs->>'snapshot_date' AS d "
            "  FROM okg.graph_nodes "
            " WHERE subtype IN ('document','document_chunk')) u "
            "HAVING count(*) <> 1",
            expect_empty=True,
            note="two dates in one generation means the build mixed "
                 "observations and 'as of when' has no answer; zero dates "
                 "means it is not a dated snapshot at all",
        ),
        Probe(
            "every chunk carries the positional key (req.fresh.comparable)",
            "SELECT 1 FROM okg.graph_nodes WHERE subtype='document_chunk' "
            "AND NOT (attrs ? 'doc_path' AND attrs ? 'chunk_ordinal') LIMIT 1",
            expect_empty=True,
            note="without (doc_path, chunk_ordinal) a passage cannot be "
                 "recognised in the NEXT snapshot, and the freshness "
                 "measurement compares two unrelated graphs",
        ),
        Probe(
            "the positional key is carried on the occurrence, not only the node",
            "SELECT 1 FROM okg.graph_edges e "
            "JOIN okg.graph_nodes c ON c.node_id = e.src "
            "WHERE e.edge_type='member_of' AND c.subtype='document_chunk' "
            "AND e.attrs ? 'doc_path' AND e.attrs ? 'chunk_ordinal' LIMIT 1",
            note="chunk nodes deduplicate by CONTENT, so one text appearing "
                 "in two documents is ONE node and the node-level key names "
                 "only one of its occurrences; the member_of edge is where "
                 "each occurrence's own key lives",
        ),
    ),
    notes=(
        "PACT okg-corpus-doc-corpus / req.fresh.contract. WHICH CLAUSES "
        "CANNOT BE FLOORED UNTIL THE OPERATOR CHOOSES THE CONTENT: exactly "
        "the two triple minimums (`document -contains-> document_chunk` and "
        "`document -derived_from-> source_file`), both sitting at 1. They "
        "are properties of the document SET, which is an open operator "
        "decision, and a number invented ahead of it would be a fact about "
        "the throwaway set this vertical was proven on rather than about "
        "the vertical. They tighten to the first real snapshot's observed "
        "counts (the cti child's rule). EVERYTHING ELSE IS ALREADY BINDING "
        "AND CONTENT-INDEPENDENT: zero isolated documents and chunks, the "
        "0.0 isolation ceiling, every in-scope file becoming a Document, "
        "the dated-passage shape, exactly one stated date, and the "
        "positional key on both node and occurrence.\n\n"
        "The ANSWER-COVERAGE half of the usability probe — a sample of the "
        "run's ANSWERABLE QUESTIONS matching at least one chunk containing "
        "their answer text — cannot be a graph-only probe: the answers live "
        "in the FreshQA sheet, not in the corpus. It is implemented in "
        "corpus_snapshot.answer_coverage / enforce_answer_coverage and is "
        "covered by tests, but it is NOT YET WIRED into the freshqa adapter "
        "— it has no production caller today. Stated plainly because the "
        "earlier wording here claimed in the present tense that it 'runs "
        "adapter-side at snapshot-acceptance time', which was false and is "
        "the kind of claim this program exists to stop making.\n\n"
        "The consequence, so it is not discovered later: until it is wired, "
        "a snapshot whose chunks contain none of the answers would be "
        "ACCEPTED and produce silent zero-score runs — the exact failure the "
        "requirement forbids. Wiring is part of the still-open "
        "task.fresh.contract, and it is open for a reason: "
        "`minimum_fraction` has NO default (the bar depends on the document "
        "set nobody has chosen yet, and a default would silently BE that "
        "operator decision), so the gate cannot be switched on before the "
        "corpus-choice decision record exists."
    ),
)

DOC_CORPUS = VerticalSpec(
    name="doc-corpus",
    contract=_DOC_CORPUS_CONTRACT,
    # extraction owns Document/DocumentChunk and the contains/member_of
    # narrowings; git_graph owns SourceFile; doc_snapshot states the
    # cross-module `document derived_from source_file` narrowing that neither
    # of them can. Before this the vertical declared `extraction` alone, so
    # source_file — its own only expected family — was an UNDECLARED subtype.
    modules=["extraction", "git_graph", "doc_snapshot"],
    expected_families=["source_file", "document", "document_chunk"],
    description=(
        "Dated document snapshot — FreshQA's freshness corpus. git_files "
        "supplies the source_file nodes doc_corpus's predecessor invariant "
        "requires; doc_corpus parses each file into the dated, attributable "
        "passages the benchmark's answers are cited to; committing the "
        "frozen dump makes the snapshot content-addressed for free."
    ),
    required_artifacts=[("any document set", bool)],
    git_init_snapshot=True,
    deployment_files={"extractors.yaml": _DOC_CORPUS_EXTRACTORS},
    notes=(
        "OPEN OPERATOR DECISION: which documents constitute the dated "
        "snapshot (a Wikipedia dump slice, a curated document set, or "
        "reuse of another corpus with freshness as the only variable). "
        "The shape is defined; the content is the operator's call. The "
        "contract above is the bar any of the three must clear."
    ),
    sources=[
        SourceSpec(
            name="snapshot-git-files",
            module="okg.substrate.library.sources.git_graph_files",
            cls="GitFilesSource",
            source_class="discovery_crawl",
            record_identity_kind="scoped_locator",
            record_identity_fields=["repo", "path"],
            params=lambda snap, dep, name: {
                "repo": name,
                "repo_root": str(snap),
                "include_untracked": False,
            },
        ),
        SourceSpec(
            name="doc-corpus",
            module="okg.substrate.library.sources.doc_corpus",
            cls="DocCorpusSource",
            source_class="discovery_crawl",
            record_identity_kind="scoped_locator",
            record_identity_fields=["repo", "path"],
            # Runs AFTER snapshot-git-files. The adapter refuses to start
            # until the predecessor has reconciled, because a `derived_from`
            # edge whose source_file endpoint does not exist is a dangling
            # edge that blocks the publish.
            predecessor="snapshot-git-files",
            params=lambda snap, dep, name: {
                "repo": name,
                "repo_root": str(snap),
                # The chain lives in the deployment dir (deployment_files).
                "extractors_dir": str(dep),
                "include_globs": ["**/*.md", "**/*.markdown", "**/*.txt"],
                "exclude_globs": ["**/.git/**"],
                "predecessor_source": f"{name}.snapshot-git-files",
            },
            # The snapshot DATE is a build-level fact, and the dsn is needed
            # to check the predecessor's source_file endpoints exist. Neither
            # is visible to the (snap, dep, name) signature.
            spec_params=lambda spec: {
                "snapshot_date": spec.snapshot_date,
                "dsn": spec.dsn,
            },
        ),
    ],
)

#: Verticals that back a live benchmark. DOC_CORPUS is deliberately ABSENT:
#: FreshQA was cut, and it was doc-corpus's only consumer. A vertical with no
#: benchmark is not part of "one OKG per benchmark", so auditing it would
#: report a permanent NOT BUILT that no work could ever clear — a red that
#: means "unused", not "broken", and a gate nobody can act on gets ignored.
#: The DOC_CORPUS spec, its contract and its tests are RETAINED above: the
#: dated-passage shape, the positional comparability key and the
#: answer-coverage gate are general document-snapshot machinery that a
#: replacement benchmark can adopt without rebuilding. Re-add it here the
#: moment a benchmark maps to it.
_STARK_PRIME_CONTRACT = CorpusContract(
    subtypes=(
        "stark_prime_anatomy",
        "stark_prime_biological_process",
        "stark_prime_cellular_component",
        "stark_prime_disease",
        "stark_prime_drug",
        "stark_prime_effect_phenotype",
        "stark_prime_exposure",
        "stark_prime_gene_protein",
        "stark_prime_molecular_function",
        "stark_prime_pathway",
    ),
    triples=(
        TripleClause("stark_prime_drug", "synergistic_interaction",
                     "stark_prime_drug", 2672628),
        TripleClause("stark_prime_gene_protein", "expression_present",
                     "stark_prime_anatomy", 1518203),
        TripleClause("stark_prime_anatomy", "expression_present",
                     "stark_prime_gene_protein", 1518203),
        TripleClause("stark_prime_gene_protein", "ppi",
                     "stark_prime_gene_protein", 642150),
        TripleClause("stark_prime_disease", "phenotype_present",
                     "stark_prime_effect_phenotype", 150317),
        TripleClause("stark_prime_effect_phenotype", "phenotype_present",
                     "stark_prime_disease", 150317),
        TripleClause("stark_prime_gene_protein", "interacts_with",
                     "stark_prime_biological_process", 144805),
        TripleClause("stark_prime_biological_process", "interacts_with",
                     "stark_prime_gene_protein", 144805),
        TripleClause("stark_prime_biological_process", "parent_child",
                     "stark_prime_biological_process", 105772),
        TripleClause("stark_prime_gene_protein", "interacts_with",
                     "stark_prime_cellular_component", 83402),
        TripleClause("stark_prime_cellular_component", "interacts_with",
                     "stark_prime_gene_protein", 83402),
        TripleClause("stark_prime_disease", "associated_with",
                     "stark_prime_gene_protein", 80411),
        TripleClause("stark_prime_gene_protein", "associated_with",
                     "stark_prime_disease", 80411),
        TripleClause("stark_prime_gene_protein", "interacts_with",
                     "stark_prime_molecular_function", 69530),
        TripleClause("stark_prime_molecular_function", "interacts_with",
                     "stark_prime_gene_protein", 69530),
        TripleClause("stark_prime_drug", "side_effect",
                     "stark_prime_effect_phenotype", 64784),
        TripleClause("stark_prime_effect_phenotype", "side_effect",
                     "stark_prime_drug", 64784),
        TripleClause("stark_prime_disease", "parent_child",
                     "stark_prime_disease", 64388),
        TripleClause("stark_prime_gene_protein", "interacts_with",
                     "stark_prime_pathway", 42646),
        TripleClause("stark_prime_pathway", "interacts_with",
                     "stark_prime_gene_protein", 42646),
        TripleClause("stark_prime_effect_phenotype", "parent_child",
                     "stark_prime_effect_phenotype", 37472),
        TripleClause("stark_prime_disease", "contraindication",
                     "stark_prime_drug", 30675),
        TripleClause("stark_prime_drug", "contraindication",
                     "stark_prime_disease", 30675),
        TripleClause("stark_prime_anatomy", "parent_child",
                     "stark_prime_anatomy", 28064),
        TripleClause("stark_prime_molecular_function", "parent_child",
                     "stark_prime_molecular_function", 27148),
        TripleClause("stark_prime_gene_protein", "expression_absent",
                     "stark_prime_anatomy", 19887),
        TripleClause("stark_prime_anatomy", "expression_absent",
                     "stark_prime_gene_protein", 19887),
        TripleClause("stark_prime_gene_protein", "target",
                     "stark_prime_drug", 16380),
        TripleClause("stark_prime_drug", "target",
                     "stark_prime_gene_protein", 16380),
        TripleClause("stark_prime_cellular_component", "parent_child",
                     "stark_prime_cellular_component", 9690),
        TripleClause("stark_prime_disease", "indication",
                     "stark_prime_drug", 9388),
        TripleClause("stark_prime_drug", "indication",
                     "stark_prime_disease", 9388),
        TripleClause("stark_prime_gene_protein", "enzyme",
                     "stark_prime_drug", 5317),
        TripleClause("stark_prime_drug", "enzyme",
                     "stark_prime_gene_protein", 5317),
        TripleClause("stark_prime_pathway", "parent_child",
                     "stark_prime_pathway", 5070),
        TripleClause("stark_prime_exposure", "parent_child",
                     "stark_prime_exposure", 4140),
        TripleClause("stark_prime_gene_protein", "associated_with",
                     "stark_prime_effect_phenotype", 3330),
        TripleClause("stark_prime_effect_phenotype", "associated_with",
                     "stark_prime_gene_protein", 3330),
        TripleClause("stark_prime_gene_protein", "transporter",
                     "stark_prime_drug", 3092),
        TripleClause("stark_prime_drug", "transporter",
                     "stark_prime_gene_protein", 3092),
        TripleClause("stark_prime_disease", "off_label_use",
                     "stark_prime_drug", 2568),
        TripleClause("stark_prime_drug", "off_label_use",
                     "stark_prime_disease", 2568),
        TripleClause("stark_prime_disease", "linked_to",
                     "stark_prime_exposure", 2304),
        TripleClause("stark_prime_exposure", "linked_to",
                     "stark_prime_disease", 2304),
        TripleClause("stark_prime_biological_process", "interacts_with",
                     "stark_prime_exposure", 1625),
        TripleClause("stark_prime_exposure", "interacts_with",
                     "stark_prime_biological_process", 1625),
        TripleClause("stark_prime_gene_protein", "interacts_with",
                     "stark_prime_exposure", 1212),
        TripleClause("stark_prime_exposure", "interacts_with",
                     "stark_prime_gene_protein", 1212),
        TripleClause("stark_prime_disease", "phenotype_absent",
                     "stark_prime_effect_phenotype", 1193),
        TripleClause("stark_prime_effect_phenotype", "phenotype_absent",
                     "stark_prime_disease", 1193),
        TripleClause("stark_prime_gene_protein", "carrier",
                     "stark_prime_drug", 864),
        TripleClause("stark_prime_drug", "carrier",
                     "stark_prime_gene_protein", 864),
        TripleClause("stark_prime_molecular_function", "interacts_with",
                     "stark_prime_exposure", 45),
        TripleClause("stark_prime_exposure", "interacts_with",
                     "stark_prime_molecular_function", 45),
        TripleClause("stark_prime_cellular_component", "interacts_with",
                     "stark_prime_exposure", 10),
        TripleClause("stark_prime_exposure", "interacts_with",
                     "stark_prime_cellular_component", 10),
    ),
    # Observed 0.000 (0 of 129,375). Not a blow-up guard like cti's 0.12:
    # the release states minimum degree 2, so any isolate at all is a lost
    # edge row and the source refuses to emit one.
    max_isolated_fraction=0.0,
    # All 10, and all 10 are required subtypes above, so no connectivity
    # rule stands without an existence rule under it.
    zero_isolated_subtypes=(
        "stark_prime_anatomy",
        "stark_prime_biological_process",
        "stark_prime_cellular_component",
        "stark_prime_disease",
        "stark_prime_drug",
        "stark_prime_effect_phenotype",
        "stark_prime_exposure",
        "stark_prime_gene_protein",
        "stark_prime_molecular_function",
        "stark_prime_pathway",
    ),
    probes=(
        Probe(
            "negative: no edge type the release does not state",
            "SELECT 1 FROM okg.graph_edges WHERE edge_type NOT IN "
            "('ppi','carrier','enzyme','target','transporter',"
            "'contraindication','indication','off_label_use',"
            "'synergistic_interaction','associated_with','parent_child',"
            "'phenotype_absent','phenotype_present','side_effect',"
            "'interacts_with','linked_to','expression_present',"
            "'expression_absent') LIMIT 1",
            expect_empty=True,
            note="THE GOLD GATE. The promoted-from testbed source emitted "
                 "`query -targets-> entity` edges built straight from "
                 "`answer_ids`, stamped source_relation stark.gold_answer, "
                 "behind a flag defaulting ON. `targets` is not one of the "
                 "18 relation types `edge_type_dict.pkl` names, so this "
                 "clause fails the build if that path is ever restored — "
                 "and it fails for ANY invented relation, not only that "
                 "one, because the vocabulary is closed by the release",
        ),
        Probe(
            "negative: no node carries a QA-derived attribute",
            "SELECT 1 FROM okg.graph_nodes WHERE attrs ?| array["
            "'query','query_id','answer_ids','split','gold_answer',"
            "'source_relation','targets'] LIMIT 1",
            expect_empty=True,
            note="the other route the gold could take: not an edge but an "
                 "attribute. `split` is included because knowing WHICH "
                 "questions an entity answers in test is leakage even "
                 "without the answer itself",
        ),
        Probe(
            "negative: every node id is derivable from its entity index",
            "SELECT 1 FROM okg.graph_nodes WHERE node_id IS DISTINCT FROM "
            "'stark_prime:entity:' || (attrs->>'entity_index') LIMIT 1",
            expect_empty=True,
            note="THE ANSWER UNIT AS A GATE. STaRK's gold is a set of "
                 "entity INDICES; if a node id cannot be formed from one "
                 "without a lookup the corpus cannot be scored against the "
                 "benchmark at all. Stated as a biconditional over every "
                 "node rather than as a spot check, because one "
                 "differently-keyed subtype would silently zero that "
                 "subtype's recall",
        ),
        Probe(
            "negative: no entity is stamped as stranded",
            "SELECT 1 FROM okg.graph_nodes WHERE attrs ? "
            "'no_relations_reason' LIMIT 1",
            expect_empty=True,
            note="`no_relations_reason` is emitted ONLY by a bounded "
                 "(`max_entities`) build. Its presence here means a bounded "
                 "corpus was published against the full release's floors — "
                 "which the floors would also catch, but this says WHY "
                 "instead of reporting 56 near-misses",
        ),
        Probe(
            "positive: the benchmark's conjunctive shape — a drug reached "
            "from both a gene and a disease",
            "SELECT 1 FROM okg.graph_nodes d "
            "JOIN okg.graph_edges te ON te.src = d.node_id "
            "  AND te.edge_type = 'target' "
            "JOIN okg.graph_nodes g ON g.node_id = te.dst "
            "  AND g.subtype = 'stark_prime_gene_protein' "
            "JOIN okg.graph_edges ie ON ie.src = d.node_id "
            "  AND ie.edge_type = 'indication' "
            "JOIN okg.graph_nodes dis ON dis.node_id = ie.dst "
            "  AND dis.subtype = 'stark_prime_disease' "
            "WHERE d.subtype = 'stark_prime_drug' LIMIT 1",
            note="STaRK-Prime questions are conjunctive — 'which drug "
                 "targets X and is indicated for Y'. Measured: 1,255 of "
                 "7,957 drugs satisfy both legs (e.g. entity 14014 "
                 "Flunisolide -> gene 509, disease 33675). A corpus that "
                 "published the entities but lost either relation would "
                 "pass every subtype clause and make the benchmark "
                 "unanswerable",
        ),
        Probe(
            "positive: the semi-structured half — an entity carries the "
            "release's own prose, not just a name",
            "SELECT 1 FROM okg.graph_nodes WHERE "
            "subtype = 'stark_prime_disease' "
            "AND length(attrs->>'text') > 200 LIMIT 1",
            note="STaRK is SEMI-structured retrieval: relations plus node "
                 "text. Measured text length by type, and it is not "
                 "uniform — all 17,080 diseases exceed 200 chars (median "
                 "890), as do 2,448 of 2,516 pathways (median 1,017), "
                 "17,776 of 27,671 genes and 5,114 of 7,957 drugs; the "
                 "other SIX types carry no `details` block at all in the "
                 "release and their text is the name alone (median 13-41 "
                 "chars). The probe is therefore scoped to disease rather "
                 "than written corpus-wide, where it would be "
                 "unsatisfiable for reasons that are upstream and not a "
                 "defect",
        ),
    ),
    notes=(
        "PACT okg-bench-graph-benchmarks / req.stark.corpus. Floors are "
        "EXACT counts from the real full build (129,375 entities / "
        "8,100,498 edge rows / 56 triples / 0 isolated, 40.9 s), not the "
        "paper's Table 2: the paper's 129,375 / 8,100,498 happen to agree "
        "with the tensors, but its 8,100,498 'relations' double-counts — "
        "the release is perfectly symmetric and carries 4,050,249 "
        "undirected relations. Both directions are emitted because both "
        "are what `edge_index.pt` states. The QA half is refused twice "
        "over, by `StarkPrimeSource._refuse_gold_artifacts` and by the "
        "adapter's `refuse_gold_in_snapshot`; those are INPUT properties, "
        "proven by the snapshot inventory and pytest, while the two "
        "negative probes above cover the OUTPUT."
    ),
)

STARK_PRIME = VerticalSpec(
    name="stark-prime",
    contract=_STARK_PRIME_CONTRACT,
    modules=["stark_prime"],
    expected_families=[
        "stark_prime_anatomy",
        "stark_prime_biological_process",
        "stark_prime_cellular_component",
        "stark_prime_disease",
        "stark_prime_drug",
        "stark_prime_effect_phenotype",
        "stark_prime_exposure",
        "stark_prime_gene_protein",
        "stark_prime_molecular_function",
        "stark_prime_pathway",
    ],
    description=(
        "The STaRK-Prime semi-structured knowledge base — the biomedical "
        "retrieval vertical. 129,375 PrimeKG-derived entities across 10 "
        "types and 8,100,498 typed relation rows across 18 relation types, "
        "with the release's own node text. The QA table is NEVER ingested "
        "and never mounted: it carries answer_ids for every row including "
        "test, and there is no answer-free copy in the release."
    ),
    required_artifacts=[
        ("skb/prime/processed/node_info.pkl", _has("node_info.pkl")),
        ("skb/prime/processed/edge_index.pt", _has("edge_index.pt")),
        ("skb/prime/processed/node_type_dict.pkl", _has("node_type_dict.pkl")),
        ("skb/prime/processed/edge_type_dict.pkl", _has("edge_type_dict.pkl")),
        # The per-node and per-edge TYPE VECTORS. These two were missing from
        # the first draft of this list, so `inventory()` passed a snapshot the
        # source then refused with its own error — an artifact gate that
        # under-declares what the source reads reports "artifacts present" for
        # a build that cannot run. The dicts above map ids to names; these map
        # each node and edge to its id, and without them no subtype resolves.
        ("skb/prime/processed/node_types.pt", _has("node_types.pt")),
        ("skb/prime/processed/edge_types.pt", _has("edge_types.pt")),
    ],
    sources=[
        SourceSpec(
            name="stark-prime",
            module="okg.substrate.library.sources.stark_prime",
            cls="StarkPrimeSource",
            source_class="reference_catalog",
            record_identity_kind="domain_key",
            record_identity_fields=["entity_index"],
            params=lambda snap, dep, name: {
                # No `repo`: STaRK entity indices are the benchmark's own
                # global answer space, so namespacing node ids by
                # deployment would break the property that a gold answer
                # id resolves to a node id by formatting alone.
                "processed_dir": str(snap / "skb" / "prime" / "processed"),
                "snapshot_root": str(snap),
            },
        ),
    ],
)

PHANTOMWIKI_ARTICLE_RELATIONS: tuple[str, ...] = (
    "mother", "father", "son", "daughter", "sister", "brother",
    "wife", "husband", "friend",
)


def _phantomwiki_admitted_relations_sql() -> str:
    admitted = ", ".join(f"'{r}'" for r in PHANTOMWIKI_ARTICLE_RELATIONS)
    return (
        "SELECT DISTINCT e.edge_type FROM okg.graph_edges e "
        "JOIN okg.graph_nodes s ON s.node_id = e.src AND s.subtype = 'person' "
        "JOIN okg.graph_nodes d ON d.node_id = e.dst AND d.subtype = 'person' "
        f"WHERE e.edge_type NOT IN ({admitted})"
    )


# PACT okg-bench-graph-benchmarks / req.pw.corpus.
_PHANTOMWIKI_CONTRACT = CorpusContract(
    subtypes=(
        "person", "person_attribute", "document", "document_chunk",
        "source_file",
    ),
    triples=(
        # ---- the typed half, MEASURED on the 100,628-person build ----
        #
        # Floors sit at ~88% of observed. They carry more slack than cti's
        # exact clauses for one reason and it is not caution: the universe
        # is SAMPLED, so a rerun at a different seed moves every count by a
        # few percent while the size stays fixed. A floor tight enough to
        # pin one seed would refuse a legitimate regeneration, which is the
        # operation this whole benchmark exists to make cheap.
        #
        # THE FLOORS ALSO ENFORCE THE SIZE REQUIREMENT, which is why they
        # are stated per relation rather than as one total. The largest
        # RELEASED split is 5,030 articles and states about 3,296 mother
        # sentences; a corpus rebuilt from it would miss the mother floor
        # by a factor of eighteen. So "the corpus exceeds a context window"
        # is not a note in a manifest — it is a clause that fails.
        TripleClause("person", "mother", "person", 58000,
                     note="observed 65,968 — one per person whose article "
                          "names a mother; equal to `father` by "
                          "construction, since the generator pairs parents"),
        TripleClause("person", "father", "person", 58000,
                     note="observed 65,968"),
        TripleClause("person", "daughter", "person", 58000,
                     note="observed 66,612 — the inverse of mother+father "
                          "restricted to female children, stated "
                          "independently on the PARENT's article"),
        TripleClause("person", "son", "person", 57000,
                     note="observed 65,324"),
        TripleClause("person", "sister", "person", 51000,
                     note="observed 58,593"),
        TripleClause("person", "brother", "person", 50000,
                     note="observed 57,201"),
        TripleClause("person", "wife", "person", 28000,
                     note="observed 32,660 — exactly equal to `husband`, "
                          "because both spouses' articles state the pair. "
                          "Floored SEPARATELY from husband on purpose: if "
                          "one direction stops parsing, a combined floor "
                          "would still be met by the other"),
        TripleClause("person", "husband", "person", 28000,
                     note="observed 32,660"),
        TripleClause("person", "friend", "person", 265000,
                     note="observed 300,944. Friendship is the only "
                          "relation that crosses family trees, so it is "
                          "what makes 2,000 trees ONE universe rather than "
                          "2,000 disconnected ones — a regression here "
                          "would leave the graph shattered while every "
                          "family clause still passed"),
        TripleClause("person", "has_attribute", "person_attribute",
                     355000,
                     note="observed 402,512 = exactly 4 x 100,628. The "
                          "article template always states date of birth, "
                          "occupation, hobby and gender, so this is the one "
                          "clause with no sampling variance at a fixed "
                          "universe size; the slack absorbs a size change, "
                          "not a seed change"),
        # ---- the prose half, floors derived from the INPUT ----
        #
        # NOT measured on a published generation, and marked so rather than
        # dressed up. One Document per in-scope article file is a property
        # of doc_corpus's own contract (`every file the corpus claims to
        # parse became a Document`), so 100,628 documents is determined by
        # the snapshot; the CHUNK count depends on the chunker's token
        # bands and is not derivable ahead of a build. Both floors tighten
        # to observed on the first real publish (the cti child's rule).
        TripleClause("document", "derived_from", "source_file", 88000,
                     note="one per parsed article; 100,628 articles in the "
                          "snapshot. Derived from the input, not a build"),
        TripleClause("document", "contains", "document_chunk", 88000,
                     note=">=1 chunk per document, which doc-corpus's "
                          "zero-isolated clause already requires. Derived "
                          "from the input; tightens to observed"),
    ),
    # MEASURED 0.000 on the source run (0 isolated of 153,842). Kept at 0.0
    # rather than a comfortable default: a person always states four
    # attributes and a document always reaches a chunk, so nothing in this
    # vertical's shape can strand a node at any universe size, and any
    # isolate at all is a parsing regression.
    max_isolated_fraction=0.0,
    # `person` is here, and it is DELIBERATELY THE WEAK CLAUSE — the
    # attribute edges make it hard to fail, so it catches an attribute
    # regression and nothing about kinship. The KINSHIP isolation rule is
    # the first probe below, which is the one that binds. `person_attribute`
    # is the mirror: a value node nothing points at is vocabulary the
    # corpus invented. Both are in `subtypes` above, so neither is a
    # connectivity rule with no existence rule under it.
    zero_isolated_subtypes=(
        "person", "person_attribute", "document", "document_chunk",
    ),
    probes=(
        # ---------------- the isolation rule that binds ----------------
        Probe(
            "no person is kinship-isolated for an UNSTATED reason",
            "SELECT n.node_id FROM okg.graph_nodes n "
            "WHERE n.subtype = 'person' "
            "AND n.attrs->>'no_relations_reason' IS NULL "
            "AND NOT EXISTS (SELECT 1 FROM okg.graph_edges e "
            "  WHERE (e.src = n.node_id OR e.dst = n.node_id) "
            "    AND e.edge_type <> 'has_attribute')",
            expect_empty=True,
            note="`zero isolated person` cannot see a lost kinship edge: "
                 "four has_attribute edges keep every person connected "
                 "whether the Family section parsed or not. This is the "
                 "clause that does. The stamp is written from the ARTICLE "
                 "(zero relation sentences), never from whether an edge "
                 "came out — a stamp read off emission is true exactly "
                 "when the node is isolated and forgives the regression. "
                 "MEASURED: the exemption's population is ZERO on this "
                 "build (0 of 100,628 people state no relation), so it "
                 "forgives nothing today and the probe reduces to `no "
                 "person is kinship-isolated`, complement 100,628. The "
                 "exemption is retained because a smaller or sparser "
                 "universe can produce a hermit and the corpus must then "
                 "SAY so rather than fail; it is exercised by "
                 "tests/substrate/test_phantom_wiki_source.py, which "
                 "proves the stamp discriminates by planting one hermit "
                 "among three connected people and showing both a "
                 "dropped-stamp and a forced-stamp mutation red",
        ),
        # ---------------- the derived-rule boundary ----------------
        Probe(
            "negative: no composed kinship relation entered the graph",
            _phantomwiki_admitted_relations_sql(),
            expect_empty=True,
            note="THE PACT'S NEGATIVE PROBE. PhantomWiki's questions are "
                 "phrased in `great_granddaughter`, `female_second_cousin`, "
                 "`niece`, `mother_in_law`; its articles state only the "
                 "nine relations above; and its released `facts.pl` ships "
                 "the CLAUSES that compose one from the other. Admitting "
                 "them would hand the graph arm the reasoning schema the "
                 "prose arm must infer, which is the treatment under test "
                 "rather than a property of the corpus. Two mechanisms "
                 "enforce it and they are independent: no narrowing exists "
                 "for any composed edge type (so the insert is refused), "
                 "and this probe fails if one ever does. MEASURED on the "
                 "source run: 67 clauses over 52 predicate heads refused, "
                 "against 785,548 ground facts corroborated — both sides "
                 "non-empty, which is what makes the split a split. "
                 "`parent` and `sibling` are NOT admitted either, and that "
                 "is deliberate: they are ground/base predicates in the KB "
                 "and they DO appear in questions, but no article states "
                 "them",
        ),
        Probe(
            "negative: no node carries an answer, a trace, or a hop count",
            "SELECT node_id FROM okg.graph_nodes WHERE attrs ?| array["
            "'answer','answers','solution_traces','intermediate_answers',"
            "'prolog','prolog_query','query','template','difficulty',"
            "'is_aggregation_question'] LIMIT 1",
            expect_empty=True,
            note="the questions never enter the corpus at all, so this is "
                 "a guard against a future source that ingests them "
                 "'for evaluation convenience'. `prolog.query` is the "
                 "solution PROGRAM and is strictly worse to leak than the "
                 "answer; `template` reveals the hop structure and "
                 "`difficulty` the hop count",
        ),
        # ---------------- provenance as graph state ----------------
        Probe(
            "every person carries the full generation triple",
            "SELECT node_id FROM okg.graph_nodes WHERE subtype = 'person' "
            "AND NOT (attrs ? 'universe_seed' AND attrs ? 'universe_id' "
            "  AND attrs ? 'universe_family_trees' "
            "  AND attrs ? 'universe_max_family_tree_size' "
            "  AND attrs ? 'universe_question_depth') LIMIT 1",
            expect_empty=True,
            note="a universe that cannot be regenerated from its recorded "
                 "parameters is not a corpus. Carried per NODE, not on a "
                 "universe-root node: a root would need a membership edge "
                 "to every person to avoid sitting isolated, and that hub "
                 "would make `zero isolated person` true of a graph with "
                 "no kinship edges at all",
        ),
        Probe(
            "the generation states exactly ONE universe",
            "SELECT 1 FROM (SELECT DISTINCT attrs->>'universe_id' AS u "
            "  FROM okg.graph_nodes WHERE subtype = 'person') x "
            "HAVING count(*) <> 1",
            expect_empty=True,
            note="size splits at a FIXED SEED are NESTED, not independent: "
                 "size_50 and size_5000 at seed 1 begin with the same "
                 "people and identical family sections. Two universe ids "
                 "in one generation means the build merged two worlds and "
                 "every count above is the sum of two overlapping "
                 "populations",
        ),
        # ---------------- the answer unit is reachable ----------------
        Probe(
            "positive: a composed relation is REACHABLE from base edges",
            "SELECT root.node_id, e3.dst FROM okg.graph_edges e1 "
            "JOIN okg.graph_nodes root ON root.node_id = e1.src "
            "  AND root.subtype = 'person' "
            "JOIN okg.graph_edges e2 ON e2.src = e1.dst "
            "  AND e2.edge_type IN ('son','daughter') "
            "JOIN okg.graph_edges e3 ON e3.src = e2.dst "
            "  AND e3.edge_type = 'daughter' "
            "WHERE e1.edge_type IN ('son','daughter') LIMIT 1",
            note="THE PACT'S POSITIVE PROBE, and the counterpart of the "
                 "negative one: `great_granddaughter` must be UNSTATED and "
                 "still DERIVABLE. Three hops of the base relations reach "
                 "it. A corpus that made the composition impossible would "
                 "not be safe, it would be unsolvable — the questions "
                 "would have no answer in the graph and the okg arm would "
                 "score zero for a reason that has nothing to do with the "
                 "treatment",
        ),
        Probe(
            "positive: a question enters through an attribute and reaches a "
            "person with kin",
            "SELECT a.node_id, p.node_id FROM okg.graph_nodes a "
            "JOIN okg.graph_edges ha ON ha.dst = a.node_id "
            "  AND ha.edge_type = 'has_attribute' "
            "JOIN okg.graph_nodes p ON p.node_id = ha.src "
            "  AND p.subtype = 'person' "
            "JOIN okg.graph_edges k ON k.src = p.node_id "
            "  AND k.edge_type <> 'has_attribute' "
            "WHERE a.subtype = 'person_attribute' "
            "AND a.attrs->>'kind' = 'occupation' LIMIT 1",
            note="half the question templates start `the person whose "
                 "occupation is ...`, so the attribute node is the entry "
                 "point and not decoration. Attribute VALUES are shared "
                 "across people for exactly this reason: inlining them on "
                 "the person would turn the entry hop into a scan",
        ),
        Probe(
            "positive: gender is queryable, which is what turns a "
            "grandchild walk into a granddaughter answer",
            "SELECT 1 FROM okg.graph_nodes a "
            "JOIN okg.graph_edges ha ON ha.dst = a.node_id "
            "  AND ha.edge_type = 'has_attribute' "
            "WHERE a.subtype = 'person_attribute' "
            "AND a.attrs->>'kind' = 'gender' LIMIT 1",
            note="every composed relation in the question vocabulary that "
                 "carries a gender (granddaughter, female_cousin, niece, "
                 "mother_in_law) bottoms out here",
        ),
        # ---------------- the prose half ----------------
        Probe(
            "positive: a passage traces to the article file it was parsed "
            "from",
            "SELECT 1 FROM okg.graph_edges c "
            "JOIN okg.graph_nodes d ON d.node_id = c.src "
            "  AND d.subtype = 'document' "
            "JOIN okg.graph_nodes ch ON ch.node_id = c.dst "
            "  AND ch.subtype = 'document_chunk' "
            "JOIN okg.graph_edges df ON df.src = d.node_id "
            "  AND df.edge_type = 'derived_from' "
            "JOIN okg.graph_nodes f ON f.node_id = df.dst "
            "  AND f.subtype = 'source_file' "
            "WHERE c.edge_type = 'contains' AND ch.attrs ? 'text' LIMIT 1",
            note="the prose arm reads files; the okg arm must be able to "
                 "say WHICH file a passage came from, or the two arms "
                 "cannot be compared on the same evidence",
        ),
    ),
    notes=(
        "PACT okg-bench-graph-benchmarks / req.pw.corpus. GENERATED, not "
        "downloaded, and the reason is a measurement: the largest RELEASED "
        "split is 2,614,872 bytes over 5,030 articles (~0.65M tokens) and "
        "fits inside a long context window, which would suppress the effect "
        "the A/B measures. The build measured here is "
        "`pw-generate --num-family-trees 2000 --max-family-tree-size 50 "
        "--question-depth 20 --num-questions-per-type 1 --seed 1`: 100,628 "
        "articles / 50,314,583 bytes / ~12.6M tokens — 19x the released "
        "split, 12.6x a 1M-token window, ~63x a 200K one. Source emission "
        "on that instance: 153,842 nodes (100,628 person, 53,214 "
        "person_attribute), 1,148,442 edges, 0 isolated, 0 dangling, 0 "
        "unresolved relation targets.\n\n"
        "WHICH FLOORS ARE MEASURED AND WHICH ARE NOT. The ten "
        "person/attribute triples are read off that source run. The two "
        "document triples are derived from the INPUT (one Document per "
        "in-scope article file) because no publish has run — publishing "
        "needs this file — and they tighten to observed on the first real "
        "generation.\n\n"
        "SEED-KEYED, NOT UNIVERSAL. Every floor here is keyed to the "
        "generation triple above. A different triple needs its own floors "
        "re-derived from its own first build, exactly as the legal "
        "vertical's floors are keyed to the Missouri slice. What travels "
        "across triples is the SHAPE: the relation vocabulary, the "
        "isolation rules, the probes, and the refusal.\n\n"
        "TWO CLAUSES OF THE REQUIREMENT ARE NOT EXPRESSIBLE AS A PROBE and "
        "are proven in pytest (tests/substrate/test_phantom_wiki_source.py). "
        "The DERIVED-RULE REFUSAL is a property of the INPUT, not of the "
        "published generation: the probe above can only show that no "
        "composed edge exists, which is also true of a source that never "
        "read the KB at all. The count of what was refused — 67 clauses "
        "over 52 heads, against 785,548 ground facts corroborated — lives "
        "in the source's run stats, and the test plants a "
        "`great_granddaughter` clause and asserts both the refusal count "
        "and its absence from the emitted stream, then deletes the clause "
        "and asserts the count goes to zero. Second, KB CORROBORATION: "
        "every ground fact the released KB states must be entailed by what "
        "the articles said, or the two arms are reading different worlds. "
        "That compares two artifacts and cannot be a query over one graph."
    ),
)

PHANTOMWIKI = VerticalSpec(
    name="phantomwiki",
    contract=_PHANTOMWIKI_CONTRACT,
    # `kinship` owns Person / PersonAttribute and the ten narrowings the
    # article vocabulary needs; `extraction` + `git_graph` + `doc_snapshot`
    # are doc-corpus's own three, reused rather than reimplemented.
    modules=["kinship", "extraction", "git_graph", "doc_snapshot"],
    expected_families=[
        "person", "person_attribute", "document", "document_chunk",
        "source_file",
    ],
    description=(
        "A GENERATED fictional universe — the contamination-free vertical. "
        "PhantomWiki emits one biographical article per person plus a "
        "Prolog knowledge base of the same world; regenerate at a new seed "
        "and the world has never existed. The prose half is parsed by "
        "doc_corpus into dated, attributable passages; the typed half is "
        "read off the SAME article sentences into person and kinship nodes, "
        "so the two arms see one world in two encodings — which is exactly "
        "the treatment the A/B measures. The knowledge base is read and "
        "NOTHING from it is emitted: its clauses define the composed "
        "vocabulary the questions are phrased in, and are refused with a "
        "count."
    ),
    required_artifacts=[
        ("articles/<Name>.txt", _has("articles", ".txt")),
    ],
    # doc_corpus needs a file-walking predecessor for its source_file nodes.
    git_init_snapshot=True,
    deployment_files={"extractors.yaml": _DOC_CORPUS_EXTRACTORS},
    notes=(
        "TWO FILES ARE CALLED provenance, AND THEY ARE NOT THE SAME FILE. "
        "The snapshot's `provenance.json` is the corpus-design artifact "
        "declaration, `{\"artifacts\": {\"articles\": \"real\"}}`, read by "
        "`declared_provenance`. The GENERATION triple lives in the adapter's "
        "data root as `<data_root>/phantomwiki/instance/generation.json` and "
        "is a different schema entirely; it was named `provenance.json` in "
        "the first draft and renamed precisely because two files of one name "
        "and two schemas is how a build ends up reading the wrong one.\n\n"
        "A VOCABULARY GAP WORTH YOUR DECISION. `declared_provenance` admits "
        "`real` (the released artifact) or `stand-in` (an operator-supplied "
        "substitute), and `enforce` refuses anything but `real`. A GENERATED "
        "PhantomWiki universe is neither, on a literal reading. It is "
        "proposed as `real`, and the argument is that the generator IS the "
        "released artifact — MIT-licensed, shipped on PyPI, deterministic "
        "from a recorded seed — so a universe built from it is more "
        "reproducible than any downloaded file, whereas calling it "
        "`stand-in` would permanently block a corpus that substitutes for "
        "nothing. If you disagree the vocabulary needs a third term rather "
        "than this vertical needing a waiver.\n\n"
        "SNAPSHOT LAYOUT IS PART OF THE DESIGN, not a convention. The "
        "arm-visible snapshot holds `articles/<Name>.txt` and nothing else. "
        "The Prolog KB (`facts.pl`) and the generator's per-article "
        "`<Name>_facts.txt` sidecars live OUTSIDE it, passed to the source "
        "through `extra_params`, and the adapter's "
        "`refuse_gold_in_snapshot` rejects a snapshot containing either. "
        "Neither is gold — both hold the same facts the articles state — "
        "but a snapshot carrying them gives the PROSE arm the structured "
        "encoding that IS the treatment, and the contrast is destroyed just "
        "as thoroughly as by a leaked answer. The source refuses at "
        "construction if `kb_facts_path` resolves inside `articles_dir`.\n\n"
        "GENERATION IS AN OPERATOR STEP, deliberately not automated here. "
        "It needs SWI-Prolog on the host (`brew install swi-prolog`; "
        "verified 10.0.2) and the `phantom-wiki` PyPI package, neither of "
        "which belongs in this repo's dependency set for a corpus that is "
        "built once per seed. `okg-bench corpus build-domain --vertical "
        "phantomwiki` consumes the generated instance; it does not produce "
        "one.\n\n"
        "MEASURED GENERATION COST, because it decides what is feasible and "
        "the brief's suggested parameters do not survive contact with it. "
        "The CORPUS half is cheap: 100,628 articles and a 37 MB `facts.pl` "
        "were written in about 90 seconds. The QUESTION half runs one "
        "Prolog query per sampled question over the WHOLE universe and is "
        "roughly QUADRATIC in universe size. End-to-end wall time at "
        "`--question-depth 20 --num-questions-per-type 1 --seed 1`, "
        "measured on this host:\n"
        "    201 people 7s / 403 9s / 806 4s / 1,612 6s / 5,039 11s /\n"
        "    20,129 104s / 100,628 did NOT finish in 37 min of CPU\n"
        "5,039 -> 20,129 is 4x the people and 9.5x the time. At 5,039 "
        "people `--num-questions-per-type 3` had not finished in 50 minutes "
        "while `1` finished in 11 seconds.\n\n"
        "DEPTH 10 IS THE TRACTABLE POINT, and it costs nothing in corpus "
        "terms. The SAME command at `--question-depth 10` finished the full "
        "100,628-person instance in 362 SECONDS, and the article corpus it "
        "produced is BYTE-IDENTICAL to the depth-20 one (100,628 files, "
        "same sha256 over the ordered set) — question depth is a QA-budget "
        "knob and does not perturb the world. Since depth is part of the "
        "recorded triple, generating at depth 10 is a stated corpus "
        "parameter rather than a compromise, provided every arm shares it. "
        "The recommended command is therefore depth 10, and it is what the "
        "adapter's `fetch` refusal prints. The depth-10 and depth-20 "
        "instances at seed 1 / 2000 trees / size 50 share a `facts.pl` "
        "sha256 as well as an article digest, so every count above was "
        "measured on one and transfers to the other exactly."
    ),
    sources=[
        SourceSpec(
            name="snapshot-git-files",
            module="okg.substrate.library.sources.git_graph_files",
            cls="GitFilesSource",
            source_class="discovery_crawl",
            record_identity_kind="scoped_locator",
            record_identity_fields=["repo", "path"],
            params=lambda snap, dep, name: {
                "repo": name,
                "repo_root": str(snap),
                "include_untracked": False,
            },
        ),
        SourceSpec(
            name="doc-corpus",
            module="okg.substrate.library.sources.doc_corpus",
            cls="DocCorpusSource",
            source_class="discovery_crawl",
            record_identity_kind="scoped_locator",
            record_identity_fields=["repo", "path"],
            predecessor="snapshot-git-files",
            params=lambda snap, dep, name: {
                "repo": name,
                "repo_root": str(snap),
                "extractors_dir": str(dep),
                "include_globs": ["articles/*.txt"],
                # Belt and braces with refuse_gold_in_snapshot: even if a
                # sidecar reaches the snapshot, it is not parsed into a
                # passage, so the prose corpus stays prose.
                "exclude_globs": ["**/.git/**", "**/*_facts.txt"],
                # DocCorpusSource caps discovery at 5,000 files by default —
                # a sane guard against a crawl wandering into a whole disk.
                # Here the size IS the point: the contract floors require a
                # universe far larger than any context window, so the default
                # would refuse the only corpus that satisfies them. Raised
                # deliberately and bounded, not removed: a phantomwiki
                # universe is generated to a recorded size, so a crawl finding
                # more than this means the snapshot is not what it claims.
                "max_files": 250_000,
                "predecessor_source": f"{name}.snapshot-git-files",
            },
            spec_params=lambda spec: {
                "snapshot_date": spec.snapshot_date,
                "dsn": spec.dsn,
            },
        ),
        SourceSpec(
            name="phantom-wiki",
            module="okg.substrate.library.sources.phantom_wiki",
            cls="PhantomWikiSource",
            source_class="reference_catalog",
            record_identity_kind="domain_key",
            record_identity_fields=["name"],
            # `kb_facts_path` and the generation triple are NOT derivable
            # from (snapshot, deployment, name) — the KB deliberately sits
            # outside the snapshot and the triple is a fact about the
            # generation run — so they come through
            # DomainCorpusSpec.extra_params["phantom-wiki"]. The source
            # refuses rather than defaulting when any is absent.
            params=lambda snap, dep, name: {
                "repo": name,
                "articles_dir": str(snap / "articles"),
            },
        ),
    ],
)

VERTICALS: dict[str, VerticalSpec] = {
    v.name: v
    for v in (CTI, XBRL, LITERATURE, LEGAL, OPS, REPO_HISTORY,
              STARK_PRIME, PHANTOMWIKI)
}

#: Retained but unconsumed — see the note above.
UNCONSUMED_VERTICALS: dict[str, VerticalSpec] = {DOC_CORPUS.name: DOC_CORPUS}

#: benchmark -> vertical name, or NoCorpus with the reason. This mapping is
#: the machine-readable answer to "what OKG does this benchmark read?".
BENCHMARK_CORPORA: dict[str, str | NoCorpus] = {
    "swebench-verified": "repo-history",
    "swe-qa": "repo-history",
    "ctibench": "cti",
    "finauditing": "xbrl",
    "scifact": "literature",
    "housing-qa": "legal",
    "itbench": "ops",
    "phantomwiki": "phantomwiki",
    "stark-prime": "stark-prime",
    "gitgoodbench": NoCorpus(
        "instances carry no cutoff, so no point-in-time corpus can be "
        "bounded from them; a corpus recipe must be defined first"
    ),
    "regminer": NoCorpus(
        "format donor only — the RegMiner4APR benchmark repo carries no "
        "license, so its data is never fetched or ingested"
    ),
}

#: The vertical name built by corpus.py's per-instance path, not this module.
#: Named distinctly from the REPO_HISTORY VerticalSpec above — this used to
#: rebind that name to a bare string, which was harmless only because nothing
#: imported it and VERTICALS is assembled earlier in the module.
REPO_HISTORY_VERTICAL_NAME = "repo-history"


def corpus_for_benchmark(benchmark: str) -> str | NoCorpus:
    try:
        return BENCHMARK_CORPORA[benchmark]
    except KeyError:
        raise CorpusBuildError(
            f"benchmark {benchmark!r} has no recorded corpus definition; "
            f"known: {sorted(BENCHMARK_CORPORA)}"
        ) from None


def _one(snapshot_dir: Path, fragments: tuple[str, ...], label: str) -> str:
    """Resolve exactly one snapshot artifact for a single-file source."""
    hits = [
        p for p in sorted(snapshot_dir.rglob("*"))
        if p.is_file() and all(f in p.name.lower() for f in fragments)
    ]
    if len(hits) != 1:
        raise CorpusBuildError(
            f"snapshot must contain exactly one {label} matching {fragments}; "
            f"found {[str(h) for h in hits]}"
        )
    return str(hits[0])


# --- build --------------------------------------------------------------


@dataclass(frozen=True)
class DomainCorpusSpec:
    vertical: str
    snapshot_dir: Path
    snapshot_date: str
    deployment_name: str
    dsn: str
    corpus_root: Path
    extra_params: dict[str, dict[str, Any]] = field(default_factory=dict)
    # REMOVED: `provenance: str = "real"`. It was a caller-supplied default
    # that no caller ever supplied, so every corpus claimed released-artifact
    # provenance regardless of what it was built from. Provenance is now read
    # from the snapshot's own provenance.json by `declared_provenance`, and a
    # snapshot that does not declare it is refused.
    #: corpus.json of the snapshot this build SUPERSEDES, for verticals whose
    #: corpus is one dated observation among several (doc-corpus). When set,
    #: the build must be able to RELATE itself to that predecessor or it
    #: fails; it never quietly publishes an incomparable corpus.
    supersedes: Path | None = None


#: Filename a snapshot must carry declaring where each artifact came from.
PROVENANCE_FILENAME = "provenance.json"


@dataclass
class SnapshotProvenance:
    """What a snapshot SAYS about the origin of each of its artifacts."""

    #: artifact root -> "real" | "stand-in"
    by_artifact: dict[str, str]
    #: "real" only when EVERY artifact is real.
    verdict: str
    note: str = ""


def declared_provenance(snapshot_dir: Path) -> SnapshotProvenance:
    """Read the snapshot's provenance declaration, or refuse the build.

    This is deliberately fail-closed and it replaces a default of "real".
    `DomainCorpusSpec.provenance` defaulted to "real" and no caller ever set
    it, so EVERY corpus reported released-artifact provenance no matter what
    it was built from — the ops corpus over two synthetic runs included. The
    stand-in machinery was therefore unreachable in production, and the test
    that covered it passed only because it constructed a report directly.

    Absence of a declaration is not evidence of real inputs. A snapshot with
    no provenance.json is REFUSED, so the honest claim has to be written down
    by whoever assembled the snapshot rather than inferred from silence.
    """
    path = snapshot_dir / PROVENANCE_FILENAME
    if not path.is_file():
        raise CorpusBuildError(
            f"snapshot {snapshot_dir} carries no {PROVENANCE_FILENAME}. Every "
            "artifact root must declare 'real' (the released artifact) or "
            "'stand-in' (operator-supplied substitute). A missing declaration "
            "is refused rather than assumed real — a corpus built on "
            "substitutes that reports released provenance is the defect this "
            "gate exists to prevent (PACT okg-corpus-design / req.design.audit)."
        )
    try:
        declared = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise CorpusBuildError(f"{path} is not valid JSON: {exc}") from exc
    by_artifact = declared.get("artifacts")
    if not isinstance(by_artifact, dict) or not by_artifact:
        raise CorpusBuildError(
            f"{path} declares no artifacts; expected "
            '{"artifacts": {"<root>": "real"|"stand-in"}}'
        )
    # THREE terms, not two. `generated` was added for PhantomWiki, whose
    # corpus is produced by the released MIT-licensed GENERATOR from a
    # recorded seed rather than downloaded. Neither existing term fits:
    # "real" would collapse two different claims — a downloaded release and a
    # locally produced universe — into one verdict, and "stand-in" would
    # permanently mark unsatisfied a corpus that substitutes for nothing.
    # Overloading a verdict to mean two things is the defect this whole
    # program keeps finding; the fix is vocabulary, not a wider bucket.
    #
    # `generated` COUNTS AS SATISFYING the contract, but only because
    # determinism is what earns it: the snapshot must record the generation
    # parameters, and re-running them must reproduce the corpus. A generated
    # artifact whose parameters are not recorded is not reproducible and
    # should be declared "stand-in".
    allowed = {"real", "stand-in", "generated"}
    bad = {k: v for k, v in by_artifact.items() if v not in allowed}
    if bad:
        raise CorpusBuildError(
            f"{path} declares unknown provenance values {bad}; "
            f"each artifact must be one of {sorted(allowed)}"
        )
    standin = sorted(k for k, v in by_artifact.items() if v == "stand-in")
    generated = sorted(k for k, v in by_artifact.items() if v == "generated")
    if standin:
        verdict, note = "stand-in", f"stand-in artifacts: {', '.join(standin)}"
    elif generated:
        verdict = "generated"
        note = (f"generated by the released generator from a recorded seed: "
                f"{', '.join(generated)}")
    else:
        verdict, note = "real", "every artifact is the released one"
    return SnapshotProvenance(
        by_artifact=dict(by_artifact), verdict=verdict, note=note,
    )


def build(spec: DomainCorpusSpec) -> Path:
    """Build one vertical end-to-end. Returns the corpus.json path."""
    vertical = VERTICALS.get(spec.vertical)
    if vertical is None:
        raise CorpusBuildError(
            f"unknown vertical {spec.vertical!r}; known: {sorted(VERTICALS)}"
        )
    snapshot = inventory(spec.snapshot_dir, vertical)
    provenance = declared_provenance(spec.snapshot_dir)
    spec.corpus_root.mkdir(parents=True, exist_ok=True)
    corpus_json = spec.corpus_root / "corpus.json"
    if corpus_json.exists():
        raise CorpusBuildError(
            f"corpus.json already exists at {corpus_json}; corpora are "
            "immutable — build into a fresh corpus_root"
        )
    if vertical.git_init_snapshot:
        _ensure_git_snapshot(spec.snapshot_dir)
    deployment_dir = write_deployment(spec, vertical)
    generation_id, families = provision_and_publish(
        spec.dsn, spec.deployment_name, deployment_dir,
        command=f"okg-bench corpus build-domain --vertical {vertical.name}",
    )
    missing = [f for f in vertical.expected_families if families.get(f, 0) == 0]
    if missing:
        raise CorpusBuildError(
            f"{vertical.name} corpus published ZERO nodes for families "
            f"{missing} (counts: {families}); a hollow corpus never ships"
        )
    # THE gate: node counts are the floor, the contract is the decision.
    if not vertical.contract.is_binding():
        raise CorpusBuildError(
            f"vertical {vertical.name!r} has no binding corpus contract — it "
            "declares no edge triple and no zero-isolated rule, so an edgeless "
            "node bag would pass. Design the contract before building "
            "(PACT okg-corpus-design / req.design.contract)."
        )
    report = evaluate_contract(
        dsn=spec.dsn, generation_id=generation_id,
        contract=vertical.contract, provenance=provenance.verdict,
    )
    enforce_contract(report, vertical=vertical.name)
    # Lineage, for a corpus that is one dated observation among several. A
    # successor that cannot be related to its recorded predecessor raises
    # HERE, before corpus.json exists, naming both snapshot identities —
    # never a quietly incomparable pair of graphs.
    supersedes: dict[str, Any] | None = None
    if spec.supersedes is not None:
        from .corpus_snapshot import relate_to_predecessor

        supersedes = relate_to_predecessor(
            predecessor_corpus_json=Path(spec.supersedes),
            vertical=vertical.name,
            snapshot_date=spec.snapshot_date,
            dsn=spec.dsn,
            generation_id=generation_id,
            deployment_name=spec.deployment_name,
        ).as_dict()
    dump_json(
        corpus_json,
        {
            "vertical": vertical.name,
            "deployment_name": spec.deployment_name,
            "deployment_dir": str(deployment_dir),
            "dsn": spec.dsn,
            "generation_id": generation_id,
            "family_counts": families,
            "provenance": provenance.verdict,
            "provenance_by_artifact": provenance.by_artifact,
            "contract_report": {
                "ok": report.ok,
                "connected_fraction": round(report.connected_fraction(), 4),
                "total_nodes": report.total_nodes,
                "total_edges": report.total_edges,
                "triples": report.triple_counts,
                "isolated_by_subtype": report.isolated_by_subtype,
                "clauses": [
                    {"kind": r.kind, "clause": r.clause, "ok": r.ok,
                     "observed": r.observed, "required": r.required}
                    for r in report.results
                ],
            },
            "snapshot_dir": str(spec.snapshot_dir),
            "snapshot_date": spec.snapshot_date,
            "supersedes": supersedes,
            "snapshot": snapshot,
            "corpus_root": str(spec.corpus_root),
            "built_at": now_utc(),
        },
    )
    return corpus_json


def inventory(snapshot_dir: Path, vertical: VerticalSpec) -> dict[str, Any]:
    """Freeze manifest: sha256 of every snapshot file, plus the required
    artifact check for this vertical."""
    if not snapshot_dir.is_dir():
        raise CorpusBuildError(f"snapshot dir missing: {snapshot_dir}")
    files: dict[str, dict[str, Any]] = {}
    for path in sorted(p for p in snapshot_dir.rglob("*") if p.is_file()):
        if ".git" in path.relative_to(snapshot_dir).parts:
            continue  # git metadata is not corpus content
        files[str(path.relative_to(snapshot_dir))] = {
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "bytes": path.stat().st_size,
        }
    if not files:
        raise CorpusBuildError(f"snapshot dir {snapshot_dir} is empty")
    names = list(files)
    absent = [label for label, check in vertical.required_artifacts
              if not check(names)]
    if absent:
        raise CorpusBuildError(
            f"snapshot dir {snapshot_dir} lacks {vertical.name} artifacts: "
            f"{absent}; present: {names[:10]}"
        )
    return {"files": files, "file_count": len(files)}


def _ensure_git_snapshot(snapshot_dir: Path) -> None:
    """doc_corpus's predecessor needs source_file nodes from a git worktree.
    Initialise + commit the frozen dump when it is not already one.

    The toplevel must BE the snapshot dir, not merely contain it. `rev-parse
    --show-toplevel` succeeds from any directory nested inside a checkout, so
    treating "it succeeded" as "this dir is a git snapshot" silently hands
    GitFilesSource the ENCLOSING repository: a 7-file snapshot placed under
    .scratch/ published 8,393 source_file nodes — the whole okg repo — and
    `document -derived_from-> source_file` came out at ZERO, because the two
    sources computed `file:<repo>:<path>` against different roots. Every
    subtype clause and the expected-families check passed on that corpus;
    only a triple clause caught it. Refuse instead, and say which repo
    swallowed the snapshot.
    """
    inside = subprocess.run(
        ["git", "-C", str(snapshot_dir), "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=False,
    )
    if inside.returncode == 0 and inside.stdout.strip():
        toplevel = Path(inside.stdout.strip()).resolve()
        if toplevel == snapshot_dir.resolve():
            return
        raise CorpusBuildError(
            f"snapshot dir {snapshot_dir} is NESTED inside the git checkout "
            f"at {toplevel} rather than being its own repository. The file "
            "source would walk the enclosing repo and emit its files as the "
            "snapshot's, while the document source keys paths against the "
            "snapshot — the two disagree silently and the corpus publishes "
            "thousands of unrelated files with zero derived_from edges. "
            "Build the snapshot outside any checkout."
        )
    for argv in (
        ["git", "-C", str(snapshot_dir), "init", "-q", "-b", "main"],
        ["git", "-C", str(snapshot_dir), "add", "-A"],
        ["git", "-C", str(snapshot_dir), "-c", "user.name=okg-bench",
         "-c", "user.email=okg-bench@localhost",
         "commit", "-q", "-m", "frozen snapshot"],
    ):
        done = subprocess.run(argv, capture_output=True, text=True, check=False)
        if done.returncode != 0:
            raise CorpusBuildError(
                f"could not git-initialise the snapshot dir {snapshot_dir}: "
                f"{' '.join(argv)} -> {done.stderr.strip()}"
            )


def write_deployment(
    spec: DomainCorpusSpec | Any, vertical: VerticalSpec
) -> Path:
    """Scaffold the deployment dir: manifest + registry + nomos posture."""
    import yaml

    deployment_dir = spec.corpus_root / "deployments" / spec.deployment_name
    if deployment_dir.exists():
        raise CorpusBuildError(f"deployment dir already exists: {deployment_dir}")
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    manifest = {
        "name": spec.deployment_name,
        "description": (
            f"okg-bench {vertical.name} corpus ({vertical.description}), "
            f"snapshot {spec.snapshot_date} from {spec.snapshot_dir}"
        ),
        "modules": list(vertical.modules),
        "source_registry": "source_registry.yaml",
        "schema_dir": "schemas",
        "nomos": {
            "deployment_posture": dict(_PUBLIC_POSTURE),
            "source_policy_defaults": {
                "by_source_class": {
                    source_class: dict(_PUBLIC_SOURCE_DEFAULTS)
                    for source_class in sorted(
                        {s.source_class for s in vertical.sources}
                    )
                }
            },
        },
        "postgres": {"dsn": spec.dsn},
    }
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8"
    )
    snapshot_dir = Path(spec.snapshot_dir)
    sources: dict[str, Any] = {}
    for relative, contents in sorted(vertical.deployment_files.items()):
        target = deployment_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(contents, encoding="utf-8")
    for source in vertical.sources:
        params = source.params(
            snapshot_dir, deployment_dir, spec.deployment_name
        )
        if source.spec_params is not None:
            params.update(source.spec_params(spec))
        params.update(getattr(spec, "extra_params", {}).get(source.name, {}))
        entry: dict[str, Any] = {
            "module": source.module,
            "class": source.cls,
            "ownership_id": f"{spec.deployment_name}.{source.name}",
            "source_class": source.source_class,
            "record_identity_kind": source.record_identity_kind,
            "record_identity_fields": list(source.record_identity_fields),
            "source_revision_kind": source.source_revision_kind,
            "deletion_semantics": "missing_from_completed_scope",
            "publication_mode": "published_generation",
            "params": params,
        }
        if source.predecessor:
            entry["predecessor_source"] = (
                f"{spec.deployment_name}.{source.predecessor}"
            )
        sources[source.name] = entry
    (deployment_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": sources}, sort_keys=False), encoding="utf-8"
    )
    return deployment_dir


def provision_and_publish(
    dsn: str, deployment_name: str, deployment_dir: Path, *, command: str
) -> tuple[str, dict[str, int]]:
    """migrate -> catalog -> ingest -> publish, in process. Shared by every
    vertical AND by corpus_cti (one provisioning implementation)."""
    from okg.substrate.cli.provision import _apply_catalog, _load_main_helpers
    from okg.substrate.db.migrate import ensure_required_extensions, migrate
    from okg.substrate.ingest.service import IngestOptions, _publish, run_ingest

    from .corpus import _assert_ingest_clean, _published_generation

    _load_main_helpers()
    ensure_required_extensions(dsn)
    migrate(dsn, deployment_name=deployment_name)
    _apply_catalog(deployment_dir, dsn=dsn)
    options = IngestOptions(
        dsn=dsn,
        deployment=deployment_name,
        deployment_dir=deployment_dir,
        run_mode="scope_complete",
        publish=False,
        command=command,
    )
    result = run_ingest(options)
    _assert_ingest_clean(result)
    generation_id = _published_generation(_publish(options, "final"))
    return generation_id, family_counts(dsn, generation_id)


def family_counts(dsn: str, generation_id: str) -> dict[str, int]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        graph = store.checkout_generation(generation_id)
        rows = graph.query(
            "SELECT subtype, count(*) AS n FROM okg.graph_nodes GROUP BY subtype"
        )
        return {row["subtype"]: int(row["n"]) for row in rows}
    finally:
        store.close()
