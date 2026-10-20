"""req.design.contract + req.design.audit: the gate that replaced counting nodes.

The gate these tests cover exists because `expected_families nonzero` passed
a corpus of 5,183 papers with ZERO edges. Every case here is therefore about
a property a node count cannot see.
"""

from __future__ import annotations

import pytest

from okg.bench.ab import contract as C
from okg.bench.ab.corpus_domain import VERTICALS
from okg.bench.ab.errors import CorpusBuildError


def _report(**kw) -> C.ContractReport:
    r = C.ContractReport()
    r.node_counts = kw.get("nodes", {})
    r.triple_counts = kw.get("triples", {})
    r.isolated_by_subtype = kw.get("isolated", {})
    r.total_nodes = sum(r.node_counts.values())
    r.total_edges = sum(r.triple_counts.values())
    r.connected_nodes = r.total_nodes - sum(r.isolated_by_subtype.values())
    r.provenance = kw.get("provenance", "real")
    return r


def test_a_node_bag_cannot_satisfy_a_binding_contract():
    """The literature corpus as built — 5,183 papers, zero edges — must FAIL
    the contract it would have passed under the old node-count gate."""
    contract = C.CorpusContract(
        subtypes=("paper", "abstract_sentence"),
        triples=(C.TripleClause("paper", "contains", "abstract_sentence", 1),),
        max_isolated_fraction=0.05,
    )
    report = _report(nodes={"paper": 5183}, isolated={"paper": 5183})
    C._score(report, contract)
    assert not report.ok
    kinds = {(r.kind, r.ok) for r in report.results}
    assert ("triple", False) in kinds, "zero edges must fail a triple clause"
    assert ("subtype", False) in kinds, "missing sentence subtype must fail"
    assert ("isolation", False) in kinds
    # ...and the old gate would have been satisfied: nodes were published.
    assert report.node_counts["paper"] > 0


def test_contract_clauses_fail_loudly():
    """PACT anchor: every unmet clause names itself and its observed value,
    and enforce() refuses the build."""
    contract = C.CorpusContract(
        subtypes=("cve",),
        triples=(C.TripleClause("cve", "has_weakness", "cwe", 3200),),
        max_isolated_fraction=0.10,
        zero_isolated_subtypes=("cwe",),
    )
    report = _report(
        nodes={"cve": 3692, "cwe": 1391},
        triples={"cve -has_weakness-> cwe": 3296},
        isolated={"cwe": 1191, "cve": 991},
    )
    C._score(report, contract)
    assert not report.ok
    failed = {r.clause for r in report.failures}
    assert "zero isolated cwe" in failed
    assert "isolated fraction" in failed
    # The satisfied triple is reported as satisfied, with its count.
    triple = next(r for r in report.results if r.kind == "triple")
    assert triple.ok and triple.observed == 3296

    with pytest.raises(CorpusBuildError, match="does not meet its contract") as exc:
        C.enforce(report, vertical="cti")
    text = str(exc.value)
    assert "zero isolated cwe" in text and "1191" in text


def test_non_binding_contract_is_itself_a_defect():
    """A contract an edgeless bag could satisfy must not be accepted."""
    weak = C.CorpusContract(subtypes=("paper",))
    assert not weak.is_binding()
    report = _report(nodes={"paper": 10}, isolated={"paper": 10})
    C._score(report, weak)
    assert not report.ok
    assert any(r.clause == "binds connectivity" for r in report.failures)

    binding = C.CorpusContract(
        subtypes=("paper",),
        triples=(C.TripleClause("paper", "contains", "sentence", 1),),
    )
    assert binding.is_binding()


def test_stand_in_build_never_counts_as_satisfied():
    """ops builds over an operator-supplied stand-in; a stand-in is reported
    as such, never as a met contract."""
    contract = C.CorpusContract(
        triples=(C.TripleClause("a", "r", "b", 1),),
    )
    report = _report(nodes={"a": 1, "b": 1}, triples={"a -r-> b": 5},
                     provenance="stand-in")
    C._score(report, contract)
    assert report.ok, "clauses themselves pass"
    with pytest.raises(CorpusBuildError, match="stand-in"):
        C.enforce(report, vertical="ops")


def test_provenance_is_declared_by_the_snapshot_never_defaulted(tmp_path):
    """Regression, and the reason the test above was VACUOUS in production.

    `DomainCorpusSpec.provenance` defaulted to "real" and no caller ever set
    it, so every corpus reported released-artifact provenance no matter what
    it was built from — including the ops corpus over two synthetic runs. The
    stand-in path above was unreachable outside this file: it passed because
    it hands `_report` the verdict directly.

    Provenance is now READ from the snapshot, and a snapshot that declares
    nothing is refused rather than assumed real.
    """
    from okg.bench.ab.corpus_domain import PROVENANCE_FILENAME, declared_provenance

    # Absence is not evidence of real inputs.
    with pytest.raises(CorpusBuildError, match=PROVENANCE_FILENAME):
        declared_provenance(tmp_path)

    (tmp_path / PROVENANCE_FILENAME).write_text(
        '{"artifacts": {"scenarios": "real", "trajectories": "stand-in"}}'
    )
    got = declared_provenance(tmp_path)
    assert got.verdict == "stand-in", "one stand-in artifact taints the corpus"
    assert "trajectories" in got.note

    (tmp_path / PROVENANCE_FILENAME).write_text(
        '{"artifacts": {"scenarios": "real", "library": "real"}}'
    )
    assert declared_provenance(tmp_path).verdict == "real"

    # A typo must not read as a claim. "Real" is not "real"; anything
    # unrecognised is refused rather than silently treated as a stand-in
    # (which would be quietly lenient) or as real (quietly false).
    (tmp_path / PROVENANCE_FILENAME).write_text(
        '{"artifacts": {"scenarios": "Real"}}'
    )
    with pytest.raises(CorpusBuildError, match="unknown provenance"):
        declared_provenance(tmp_path)

    (tmp_path / PROVENANCE_FILENAME).write_text('{"artifacts": {}}')
    with pytest.raises(CorpusBuildError, match="declares no artifacts"):
        declared_provenance(tmp_path)

    # `generated` is a THIRD term, added for PhantomWiki, whose corpus comes
    # from the release's own generator rather than a download. It satisfies
    # the contract but stays distinct from `real`: folding it in would make
    # "downloaded the release" and "ran the generator" the same claim, and
    # overloading a verdict to mean two things is the defect this program
    # keeps finding.
    (tmp_path / PROVENANCE_FILENAME).write_text(
        '{"artifacts": {"universe": "generated"}}'
    )
    got = declared_provenance(tmp_path)
    assert got.verdict == "generated"
    assert "recorded seed" in got.note

    # ...and one stand-in still taints a generated corpus.
    (tmp_path / PROVENANCE_FILENAME).write_text(
        '{"artifacts": {"universe": "generated", "extra": "stand-in"}}'
    )
    assert declared_provenance(tmp_path).verdict == "stand-in"

    # enforce() accepts generated, refuses stand-in.
    ok = _report(nodes={"a": 1, "b": 1}, triples={"a -r-> b": 5},
                 provenance="generated")
    contract = C.CorpusContract(triples=(C.TripleClause("a", "r", "b", 1),))
    C._score(ok, contract)
    C.enforce(ok, vertical="phantom-wiki")  # must not raise


def test_health_audit_aggregates():
    """PACT anchor: the rendered report states what the corpus IS."""
    contract = C.CorpusContract(
        subtypes=("cve",),
        triples=(C.TripleClause("cve", "has_weakness", "cwe", 10),),
        max_isolated_fraction=0.5,
    )
    report = _report(
        nodes={"cve": 100, "cwe": 40},
        triples={"cve -has_weakness-> cwe": 90},
        isolated={"cwe": 5},
    )
    C._score(report, contract)
    text = report.render()
    assert "nodes=140" in text and "edges=90" in text
    assert "connected=135" in text
    assert "cve -has_weakness-> cwe" in text
    assert report.connected_fraction() == pytest.approx(135 / 140)
    assert report.ok


def test_uncontracted_corpus_resolves_or_fails_never_skips():
    """Regression: the first audit-health silently EXCLUDED a corpus whose
    `vertical` key was missing, so a CTI corpus already proven to fail four
    clauses was reported as merely 'no contract declared' and left out of
    the failure count. Resolution falls back to the deployment name, and an
    unresolvable corpus is a failure, never an exemption."""
    from okg.bench.ab.cli import _resolve_vertical

    name, vertical = _resolve_vertical({"vertical": "cti"})
    assert name == "cti" and vertical is not None
    # Corpora built before the spec-driven builder carry no vertical key.
    name, vertical = _resolve_vertical({"deployment_name": "bench-cti"})
    assert name == "cti" and vertical is not None, (
        "deployment-name fallback must resolve a pre-refactor corpus"
    )
    # Genuinely uncontracted: resolves to nothing, and the caller fails it.
    name, vertical = _resolve_vertical(
        {"deployment_name": "bench-psf__requests-2317"}
    )
    assert vertical is None


def test_literature_contract():
    """PACT anchor (req.lit.contract): the contract must reject the corpus as
    it was BUILT (5,183 papers, zero edges) and accept it as REBUILT — the
    real numbers from the 2026-07-28 build over SciFact's 5,183 abstracts."""
    contract = VERTICALS["literature"].contract
    triples = {c.label: c.minimum for c in contract.triples}
    assert "paper -contains-> abstract_sentence" in triples
    assert "abstract_sentence -mentions-> concept" in triples
    assert "paper -shares_concept-> paper" in triples
    assert "abstract_sentence" in contract.subtypes, "the gold unit is a subtype"
    assert contract.probes, "the gold-unit probe is what makes this binding"

    before = _report(nodes={"paper": 5183}, isolated={"paper": 5183})
    C._score(before, contract)
    assert not before.ok, "the flat corpus must fail"

    after = _report(
        nodes={"paper": 5183, "abstract_sentence": 45952, "concept": 16385},
        triples={
            "paper -contains-> abstract_sentence": 45952,
            "abstract_sentence -mentions-> concept": 229737,
            "paper -shares_concept-> paper": 61342,
        },
        isolated={},
    )
    C._score(after, contract)
    assert after.ok, [r.line() for r in after.failures]
    assert after.connected_fraction() == 1.0


def test_legal_contract():
    """PACT anchor (req.legal.contract): rejects the flat 60-node stand-in,
    accepts the real 28,576-row Missouri slice built 2026-07-28."""
    contract = VERTICALS["legal"].contract
    triples = {c.label: c.minimum for c in contract.triples}
    assert "statute_passage -part_of-> statute_section" in triples
    assert "statute_passage -cross_references-> statute_passage" in triples
    assert "statute_passage -in_jurisdiction-> jurisdiction" in triples
    assert contract.max_isolated_fraction == 0.0, "every passage must be placed"

    before = _report(nodes={"statute_passage": 60},
                     isolated={"statute_passage": 60})
    C._score(before, contract)
    assert not before.ok

    after = _report(
        nodes={"statute_passage": 28576, "statute_section": 29022,
               "jurisdiction": 1},
        triples={
            "statute_passage -part_of-> statute_section": 28576,
            "statute_section -part_of-> statute_section": 29021,
            "statute_passage -in_jurisdiction-> jurisdiction": 28576,
            "statute_section -in_jurisdiction-> jurisdiction": 1,
            "statute_passage -cross_references-> statute_passage": 23943,
        },
        isolated={},
    )
    C._score(after, contract)
    assert after.ok, [r.line() for r in after.failures]


def test_xbrl_contract():
    """PACT anchor (req.xbrl.contract). The isolated-filing rule is a NEGATIVE
    PROBE, not a zero-tolerance clause: 5 of 25 filings have neither a
    parseable instance nor presentation linkbase (upstream 32,767-byte cap)
    and their schemas declare no extension concepts, so no source-carried
    structure reaches them. Demanding zero would be unsatisfiable; allowing
    'some' would hide a parsing regression."""
    contract = VERTICALS["xbrl"].contract
    assert "filing" not in contract.zero_isolated_subtypes
    stranded = [p for p in contract.probes if p.expect_empty]
    assert stranded, "the stranded-filing rule must be a negative probe"
    assert "structurally_stranded" in stranded[0].sql

    triples = {c.label: c.minimum for c in contract.triples}
    for label in (
        "filing -contains-> xbrl_statement",
        "xbrl_fact -in_context-> xbrl_context",
        "xbrl_concept -dim_related_to-> xbrl_concept",
    ):
        assert label in triples, f"{label} is what the linkbases buy"
    # `declares` is emitted by the source but is NOT a clause: measured
    # against this export, no schema declares an xsd:element.
    assert "filing -declares-> xbrl_concept" not in triples

    after = _report(
        nodes={"filing": 25, "xbrl_concept": 289, "xbrl_fact": 134,
               "xbrl_statement": 20, "xbrl_context": 76},
        triples={
            "filing -contains-> xbrl_fact": 134,
            "xbrl_fact -reports-> xbrl_concept": 134,
            "xbrl_concept -calc_sums_to-> xbrl_concept": 118,
            "xbrl_fact -in_context-> xbrl_context": 134,
            "filing -contains-> xbrl_context": 76,
            "filing -contains-> xbrl_statement": 20,
            "xbrl_statement -presents-> xbrl_concept": 463,
            "xbrl_concept -presentation_parent_of-> xbrl_concept": 206,
            "xbrl_concept -dim_related_to-> xbrl_concept": 127,
        },
        isolated={"filing": 5},
    )
    C._score(after, contract)
    assert after.ok, [r.line() for r in after.failures]


def test_cti_contract():
    """PACT anchor (req.cti2.contract). Isolation is adjudicated PER NODE by a
    negative probe over source-written stamps, not by an aggregate fraction.

    An aggregate ceiling cannot distinguish the isolates the sources genuinely
    strand from the ones a parsing regression dropped — it passes either way.
    So each source stamps WHY it stranded a node, from what the upstream input
    DECLARES rather than from whether an edge came out, and the probe fails on
    any isolate carrying no stamp.

    The stamp must DISCRIMINATE. Measured on the 2026-07-27 snapshot: 991
    isolated CVEs stamped against 2,701 connected CVEs unstamped, 70 isolated
    CWEs stamped against 1,321 connected unstamped, and nothing stamped-but-
    connected in either subtype. The first draft of this gate keyed the CVE
    exemption on `cwe_ids IS NULL` — an attribute nvd_feeds never writes, so it
    was true of all 3,692 CVEs and would have forgiven every isolate including
    regression-stranded ones. Hence the rule: measure an exemption predicate on
    the COMPLEMENT before trusting it.
    """
    contract = VERTICALS["cti"].contract
    negative = [p for p in contract.probes if p.expect_empty]
    assert negative, "isolation must be adjudicated by a negative probe"
    sql = negative[0].sql
    assert "no_relations_reason" in sql and "weakness_mapping_status" in sql, (
        "the probe must key on the SOURCE-written stranding stamps"
    )
    # No blanket escape hatch. Forgiving a whole `kind` or matching the name
    # prose would excuse a regression-stranded node just as readily as an
    # upstream-empty one.
    assert "kind" not in sql and "ILIKE" not in sql.upper()
    # NO subtype-wide exemption at all. The probe carried
    # `AND n.subtype <> 'attack_data_component'` — a blanket allow of exactly
    # the shape this per-node rule replaces — because attack_stix emitted no
    # stranding stamp, so its 8 unreferenced components had nothing to be
    # adjudicated by. Adversarial review caught the rollup claiming all 1,069
    # isolates were stamped when 8 were not. The source now censuses the raw
    # bundle before emitting, so every isolate is judged node by node.
    assert sql.count("n.subtype <> ") == 0, (
        "a subtype-wide allow forgives a regression-stranded node as readily "
        "as an upstream-empty one"
    )
    assert "attack_data_component" not in sql

    # cve and cwe are NOT subtype-wide allows: the probe adjudicates them node
    # by node, which is strictly stronger.
    assert "cve" not in contract.zero_isolated_subtypes
    assert "cwe" not in contract.zero_isolated_subtypes

    triples = {c.label: c.minimum for c in contract.triples}
    # Category membership is what makes a category reachable at all; without it
    # 420 category nodes sat isolated and the corpus was over its ceiling.
    assert "cwe -has_member-> cwe" in triples
    # The floor is stated in EDGES. 1,318 ChildOf rows collapse to 1,160 edges
    # because 158 re-declare the same pair under views 1000/1003/699, so a
    # row-count floor of 1,300 could never be met by a correct build.
    assert triples["cwe -child_of-> cwe"] <= 1160

    # All FIVE `uses` pairs plus campaign attribution carry their own floor.
    # Adversarial review found four published triples — 2,489 edges, a sixth
    # of the corpus — under no clause at all, because the requirement named
    # them and the code gated only two.
    for label in ("attack_campaign -uses-> attack_technique",
                  "attack_group -uses-> attack_software",
                  "attack_campaign -uses-> attack_software",
                  "attack_campaign -attributed_to-> attack_group"):
        assert label in triples, f"{label} is published but ungated"

    after = _report(
        nodes={"cve": 3692, "cwe": 1391, "attack_technique": 697,
               "attack_tactic": 15, "attack_group": 174,
               "attack_software": 821, "attack_mitigation": 44,
               "attack_detection_strategy": 697, "attack_analytic": 1758,
               "attack_data_component": 106, "attack_campaign": 56},
        triples={
            "cve -has_weakness-> cwe": 3296,
            "attack_technique -part_of-> attack_tactic": 872,
            "cwe -child_of-> cwe": 1160,
            "cwe -has_member-> cwe": 4260,
            "cwe -related_to-> cwe": 284,
            "attack_mitigation -mitigates-> attack_technique": 1448,
            "attack_technique -subtechnique_of-> attack_technique": 475,
            "attack_group -uses-> attack_technique": 4546,
            "attack_software -uses-> attack_technique": 11211,
            "attack_campaign -uses-> attack_technique": 1146,
            "attack_group -uses-> attack_software": 1145,
            "attack_campaign -uses-> attack_software": 172,
            "attack_campaign -attributed_to-> attack_group": 26,
            "attack_detection_strategy -detects-> attack_technique": 697,
            "attack_detection_strategy -contains-> attack_analytic": 1745,
            "attack_analytic -references-> attack_data_component": 4170,
        },
        isolated={"cve": 991, "cwe": 70, "attack_data_component": 8},
    )
    C._score(after, contract)
    assert after.ok, [r.line() for r in after.failures]

    # THE REGRESSION THIS EXISTS TO CATCH: deleting every campaign node and
    # its 1,344 incident edges used to pass — "zero isolated attack_campaign"
    # is vacuously true of a corpus with no campaigns, and the isolated
    # fraction went DOWN. It must now fail.
    gutted = _report(
        nodes={k: v for k, v in after.node_counts.items()
               if k != "attack_campaign"},
        triples={k: v for k, v in after.triple_counts.items()
                 if "attack_campaign" not in k},
        isolated=dict(after.isolated_by_subtype),
    )
    C._score(gutted, contract)
    assert not gutted.ok, "a corpus with no campaigns must not pass"
    failed = {r.clause for r in gutted.failures}
    assert "attack_campaign" in failed, "the existence clause must fire"


#: The ops build as MEASURED on 2026-07-29 over the pinned Apache-2.0 clone
#: (7c68548) plus the complete Apache-2.0 ibm-research/ITBench-Lite slice.
#: 1,289 nodes / 1,705 edges / 0 isolated.
_OPS_OBSERVED_NODES = {
    "ops_application": 2, "ops_service": 23, "ops_alert": 17,
    "ops_scenario": 66, "telemetry_run": 35, "alert_firing": 1146,
}
_OPS_OBSERVED_TRIPLES = {
    "ops_scenario -deploys-> ops_application": 66,
    "ops_application -runs-> ops_service": 23,
    "ops_application -defines-> ops_alert": 17,
    "ops_scenario -raises-> ops_alert": 24,
    "ops_alert -targets-> ops_service": 5,
    "ops_scenario -recorded_by-> telemetry_run": 34,
    "telemetry_run -observed-> alert_firing": 1146,
    "alert_firing -targets-> ops_service": 390,
}


def test_ops_contract():
    """PACT anchor (req.ops.contract). The contract must reject the ops corpus
    AS BUILT (4 nodes, 2 edges) and accept the real build.

    RESTATED 2026-07-29. The three clauses stated against the CC-BY-NC
    ITBench-Trajectories release are gone, and their removal is a measurement,
    not a softening — see the block comment above `_OPS_CONTRACT`. In short:
    ITBench-Lite is a DIFFERENT ARTIFACT (one telemetry snapshot per scenario,
    no agent trajectory), the recorded and defined alert vocabularies are
    disjoint so `telemetry_run -observed-> ops_alert` has a falsified premise,
    and `telemetry_signal` indexes trajectory runs this corpus no longer
    ingests. Two clauses over the artifact that DOES exist replace them.
    """
    contract = VERTICALS["ops"].contract
    triples = {c.label: c.minimum for c in contract.triples}

    # Every floor sits at or below the value the real build observed. A floor
    # ABOVE observed is an unmeetable clause; a floor below the regression it
    # must refuse is a clause that measures nothing.
    for label, observed in _OPS_OBSERVED_TRIPLES.items():
        assert label in triples, f"{label} published but under no clause"
        assert triples[label] <= observed, (
            f"{label} floor {triples[label]} exceeds observed {observed}"
        )
    assert set(triples) == set(_OPS_OBSERVED_TRIPLES), (
        "a clause over a triple the build does not publish is unmeetable"
    )
    # The exactly-pinned ones carry NO slack: the pinned input fixes them, so
    # headroom could only hide a loss.
    for label in ("ops_scenario -deploys-> ops_application",
                  "ops_application -defines-> ops_alert",
                  "ops_scenario -raises-> ops_alert",
                  "ops_alert -targets-> ops_service",
                  "ops_scenario -recorded_by-> telemetry_run"):
        assert triples[label] == _OPS_OBSERVED_TRIPLES[label], label
    # ...and `runs` must still refuse a silent reversion from the k8s workload
    # roster (23) to the helm reconstruction (14), which is the whole reason
    # the floor moved off 14.
    assert triples["ops_application -runs-> ops_service"] > 14

    # depends_on is absent: the ITBench tree declares no inter-service
    # relation anywhere, so a clause would be unsatisfiable and meeting
    # it would mean inventing structure.
    assert "ops_service -depends_on-> ops_service" not in triples
    # NO clause and NO narrowing joins a firing to a definition. The
    # vocabularies are disjoint (18 recorded alertnames vs 9 defined,
    # intersection empty), so such an edge would assert a correspondence
    # neither artifact states.
    assert "alert_firing -instance_of-> ops_alert" not in triples
    assert not [t for t in triples if "alert_firing" in t and "ops_alert" in t]

    # Every zero-isolated subtype is also a required subtype — "zero isolated
    # X" is vacuously true of a corpus holding no X (the cti attack_campaign
    # defect).
    assert contract.unbacked_isolation_rules() == ()

    # Isolation is adjudicated per-node, as in cti and xbrl. Asserting an
    # attribute NAME appears in probe SQL proves nothing about emission; the
    # discrimination tests live in tests/substrate/test_ops_telemetry_source.py,
    # where the complement is constructed and six mutations go red.
    iso = next(p for p in contract.probes
               if p.expect_empty and "no_subject_reason" in p.sql)
    # The stamp is NOT the old `subject_resolved`, which the source computed as
    # bool(subject) — the same expression that gated the `about` edge, so every
    # isolated signal was exempt by construction.
    assert "subject_resolved" not in iso.sql
    # An unresolvable service NAME is a finding, not an exemption.
    assert "declared_subject_unknown" not in iso.sql
    # Without the coalesce, NULL IN (...) is NULL and the row is dropped by
    # WHERE — a deleted stamp would exempt every isolate again.
    assert "coalesce" in iso.sql
    # alert_firing has NO isolation exemption at all: 0 of 1,146 are isolated.
    assert "alert_firing" not in iso.sql

    # The two firing probes assert OPPOSITE things about `expr` and must both
    # exist: for a DEFINITION an empty expr is the defect, for an OBSERVATION
    # an expr of any kind is.
    stub = next(p for p in contract.probes
                if "no alert DEFINITION was stubbed" in p.name)
    assert "ops_alert" in stub.sql and "alert_firing" not in stub.sql
    shape = next(p for p in contract.probes
                 if "never acquires a definition's expr" in p.name)
    assert "alert_firing" in shape.sql
    for token in ("alert_name", "active_at", "attrs ? 'expr'"):
        assert token in shape.sql, token
    # The stamp biconditional: a firing always carries its `observed` edge, so
    # it is never isolated and the isolation probe cannot see a lost `targets`
    # edge on it. Without this clause only the 350 floor would.
    stamp = next(p for p in contract.probes
                 if "no_target_reason agrees" in p.name)
    assert "IS DISTINCT FROM" in stamp.sql and "no_target_reason" in stamp.sql

    before = _report(nodes={"ops_scenario": 2, "telemetry_run": 2},
                     triples={"ops_scenario -recorded_by-> "
                              "telemetry_run": 2})
    C._score(before, contract)
    assert not before.ok, "the 4-node corpus must fail"

    after = _report(
        nodes=dict(_OPS_OBSERVED_NODES),
        triples=dict(_OPS_OBSERVED_TRIPLES),
        isolated={},
    )
    C._score(after, contract)
    assert after.ok, [r.line() for r in after.failures]

    # A build over stand-in inputs is NEVER reported as satisfying the
    # contract, even when every clause is met.
    standin = _report(
        nodes=dict(_OPS_OBSERVED_NODES),
        triples=dict(_OPS_OBSERVED_TRIPLES),
        isolated={}, provenance="stand-in",
    )
    C._score(standin, contract)
    assert not standin.failures, "the stand-in arm must fail on PROVENANCE"
    with pytest.raises(CorpusBuildError, match="stand-in"):
        C.enforce(standin, vertical="ops")

    # Each telemetry clause must FAIL on the pre-rebuild corpus, so none of
    # them is passing on slack alone.
    for label in ("telemetry_run -observed-> alert_firing",
                  "alert_firing -targets-> ops_service",
                  "ops_application -runs-> ops_service"):
        regressed = _report(
            nodes=dict(_OPS_OBSERVED_NODES),
            triples={**_OPS_OBSERVED_TRIPLES, label: 0},
            isolated={},
        )
        C._score(regressed, contract)
        assert label in {r.clause for r in regressed.failures}, label


def test_repo_history_contract() -> None:
    """The repo-history contract binds the two SWE-QA traversals, not a node
    count. PACT okg-corpus-repo-history / req.repo.contract."""
    contract = VERTICALS["repo-history"].contract

    assert contract.is_binding()
    for subtype in ("git_commit", "source_file", "issue", "pull_request",
                    "forge_comment"):
        assert subtype in contract.subtypes

    labels = {c.label for c in contract.triples}
    # The three joins that turned two islands into one graph.
    assert "issue -has_comment-> forge_comment" in labels
    assert "pull_request -has_comment-> forge_comment" in labels
    assert "pull_request -merged_as-> git_commit" in labels
    # ...and the derived reference edges, pinned per triple so the smallest
    # one cannot vanish behind the largest.
    reference_labels = {label for label in labels if "-references->" in label}
    assert len(reference_labels) >= 3, reference_labels

    # Every floor is stated from a measurement, and says so.
    for clause in contract.triples:
        assert clause.minimum > 0, clause.label
        assert "MEASURED" in clause.note, (
            f"{clause.label} floor is not traceable to a measurement"
        )

    # Two positive answer-unit probes plus the gold-absence negative.
    positive = [p for p in contract.probes if not p.expect_empty]
    negative = [p for p in contract.probes if p.expect_empty]
    assert len(positive) >= 2, "req.repo.contract requires TWO answer-unit probes"
    assert len(negative) >= 1

    # Probe 1 must actually traverse file -> commit -> merged PR -> comment.
    # A probe touching one subtype would pass on the island graph this PACT
    # exists to fix, which is exactly how the old node-count gate went green.
    for token in ("source_file", "affects", "merged_as", "has_comment",
                  "forge_comment"):
        assert token in positive[0].sql, (
            f"answer-unit probe does not traverse {token}"
        )
    # Probe 2 must start at an issue and reach commits through the discussion.
    for token in ("references", "has_comment", "forge_comment", "affects",
                  "git_commit"):
        assert token in positive[1].sql, (
            f"discussion probe does not traverse {token}"
        )

    # The negative probe must cover forge_comment, not just the PR node:
    # keying only on issue/pull_request is what let a gold-thread comment
    # through an audit that reported PASS.
    assert "forge_comment" in negative[0].sql
    assert "parent_number" in negative[0].sql

    # A comment is only reachable if it is attached; an isolated one is a
    # retrieval candidate the benchmark scores but no traversal finds.
    assert "forge_comment" in contract.zero_isolated_subtypes
    assert contract.max_isolated_fraction <= 0.01


def test_doc_corpus_contract():
    """PACT anchor (req.fresh.contract). This is the only vertical whose
    CONTENT is still an open operator decision, so its contract has to bind
    without any snapshot-derived number, and it has to say WHICH numbers it
    is therefore leaving at 1 rather than quietly shipping a floor that
    describes a throwaway document set.

    Measured against the two-date stand-in build (2026-07-28), identical in
    both dated generations: 26 nodes / 37 edges / 100% connected /
    0 isolated. document 6, document_chunk 12, source_file 7, git_branch 1.
    contains 12, member_of 12, derived_from 6, source_file member_of
    git_branch 7.
    """
    # doc-corpus moved to UNCONSUMED_VERTICALS when FreshQA was cut. Its
    # contract is still asserted here so the machinery cannot rot while it
    # waits for a consumer — an unused vertical that quietly stops working is
    # worse than a deleted one, because adopting it later would look free.
    from okg.bench.ab.corpus_domain import UNCONSUMED_VERTICALS

    contract = UNCONSUMED_VERTICALS["doc-corpus"].contract
    triples = {c.label: c.minimum for c in contract.triples}
    assert "document -contains-> document_chunk" in triples
    assert "document -derived_from-> source_file" in triples, (
        "a passage that cannot be traced to its file is a filename, not "
        "provenance"
    )
    # The floors are 1 ON PURPOSE and the notes must say so, or a later
    # reader mistakes an unstateable floor for a lazy one.
    assert set(triples.values()) == {1}
    assert "cannot be floored" in contract.notes.lower()

    # source_file must NOT be a zero-isolated subtype. git_files emits
    # `source_file -member_of-> git_branch` for every tracked file, so that
    # clause can never fail — it would report "every file became a passage"
    # having verified nothing.
    assert "document" in contract.zero_isolated_subtypes
    assert "document_chunk" in contract.zero_isolated_subtypes
    assert "source_file" not in contract.zero_isolated_subtypes, (
        "a source_file is never isolated (git_branch membership), so the "
        "clause would be vacuous; the derived_from probe binds it instead"
    )
    negative = [p for p in contract.probes if p.expect_empty]
    parsed = [p for p in negative if "became a Document" in p.name]
    assert parsed, "the in-scope-file rule must be a negative probe"
    sql = parsed[0].sql
    assert "derived_from" in sql and "attrs->>'path'" in sql, (
        "the exemption must key on what the INPUT declares (the file name), "
        "never on whether an edge came out"
    )

    # A file listing — the vertical as it shipped BEFORE this PACT, with
    # git_files wired in and doc_corpus not — must fail.
    before = _report(
        nodes={"source_file": 7, "git_branch": 1},
        triples={"source_file -member_of-> git_branch": 7},
    )
    C._score(before, contract)
    assert not before.ok, "a file listing is not a passage corpus"
    failed = {r.clause for r in before.failures}
    assert "document" in failed and "document_chunk" in failed
    assert "document -contains-> document_chunk" in failed

    # The corpus as BUILT, at either date.
    after = _report(
        nodes={"document": 6, "document_chunk": 12, "source_file": 7,
               "git_branch": 1},
        triples={
            "document -contains-> document_chunk": 12,
            "document_chunk -member_of-> document": 12,
            "document -derived_from-> source_file": 6,
            "source_file -member_of-> git_branch": 7,
        },
        isolated={},
    )
    C._score(after, contract)
    assert after.ok, [r.line() for r in after.failures]
    assert after.connected_fraction() == 1.0


def test_doc_corpus_answer_coverage_rejects_an_unusable_snapshot():
    """PACT anchor (req.fresh.contract), the half that cannot be a graph
    probe: the answers live in the FreshQA sheet, not in the corpus.

    A snapshot that contains none of the run's answers still publishes,
    still satisfies every structural clause, and still scores zero — and
    that zero is indistinguishable from a model failure. It must be
    REJECTED, with its numbers named.
    """
    from okg.bench.ab.corpus_snapshot import (
        answer_coverage,
        enforce_answer_coverage,
    )

    texts = [
        "Argentina won the most recent FIFA World Cup final.",
        "The Secretary-General of the United Nations is Antonio Guterres.",
    ]
    questions = [
        ("q1", ["Argentina"]),
        ("q2", ["Antonio Guterres", "Guterres"]),
        ("q3", ["Kiribati"]),
    ]
    covered = answer_coverage(
        texts=texts, questions=questions, minimum_fraction=0.6
    )
    assert covered.covered == 2 and covered.sampled == 3
    assert covered.ok
    enforce_answer_coverage(covered, snapshot_date="2026-07-28")

    # Same sample, a bar the snapshot cannot clear -> rejected, loudly.
    strict = answer_coverage(
        texts=texts, questions=questions, minimum_fraction=0.9
    )
    assert not strict.ok
    with pytest.raises(CorpusBuildError, match="not usable as a FreshQA corpus") as exc:
        enforce_answer_coverage(strict, snapshot_date="2026-07-28")
    text = str(exc.value)
    assert "2/3" in text and "q3" in text, "the probe result must be named"

    # An empty sample proves nothing and must not pass for a clean run.
    empty = answer_coverage(texts=texts, questions=[], minimum_fraction=0.5)
    with pytest.raises(CorpusBuildError, match="EMPTY answer-coverage sample"):
        enforce_answer_coverage(empty, snapshot_date="2026-07-28")

    # No default bar anywhere: a zero bar accepts every snapshot.
    with pytest.raises(CorpusBuildError, match="minimum_fraction"):
        answer_coverage(texts=texts, questions=questions, minimum_fraction=0.0)


def test_doc_corpus_snapshots_relate_or_fail_loudly():
    """PACT anchor (req.fresh.comparable). A successor snapshot that cannot
    be related to its recorded predecessor must raise with BOTH snapshot
    identities named — never quietly publish a pair of graphs that share no
    passage, which would score as a freshness measurement while measuring
    nothing."""
    import json

    from okg.bench.ab.corpus_snapshot import relate_to_predecessor

    def _prior(tmp_path, **overrides):
        record = {
            "vertical": "doc-corpus",
            "deployment_name": "bench-doc-corpus-a",
            "snapshot_date": "2026-01-15",
            "generation_id": "gen-a",
            "dsn": "postgres://unused",
        }
        record.update(overrides)
        path = tmp_path / f"corpus-{abs(hash(json.dumps(record, sort_keys=True)))}.json"
        path.write_text(json.dumps(record))
        return path

    import tempfile
    with tempfile.TemporaryDirectory() as raw:
        from pathlib import Path as _P
        tmp = _P(raw)
        common = dict(
            vertical="doc-corpus", snapshot_date="2026-07-28",
            dsn="postgres://unused", generation_id="gen-b",
            deployment_name="bench-doc-corpus-b",
        )
        # Each refusal names both identities and never reaches the graph.
        for prior, pattern in (
            (_prior(tmp, vertical="cti"), "different verticals"),
            (_prior(tmp, snapshot_date=None), "records no snapshot_date"),
            (_prior(tmp, snapshot_date="2027-01-01"), "strictly LATER"),
            (_prior(tmp, dsn=""), "no dsn or generation_id"),
        ):
            with pytest.raises(CorpusBuildError, match=pattern) as exc:
                relate_to_predecessor(predecessor_corpus_json=prior, **common)
            text = str(exc.value)
            assert "predecessor(" in text and "successor(" in text, (
                "a refusal that does not name both snapshots cannot be acted on"
            )
        with pytest.raises(CorpusBuildError, match="not found"):
            relate_to_predecessor(
                predecessor_corpus_json=tmp / "nope.json", **common
            )


def test_a_zero_isolated_rule_needs_an_existence_rule_under_it():
    """"Zero isolated X" is VACUOUSLY TRUE of a corpus holding no X.

    Found by adversarial review in the cti contract: attack_campaign sat in
    zero_isolated_subtypes but not in subtypes, so deleting all 56 campaign
    nodes and their 1,344 incident edges satisfied every clause — the
    isolated fraction would have DROPPED. Same family as `all()` over an
    empty collection.
    """
    vacuous = C.CorpusContract(
        subtypes=("paper",),
        triples=(C.TripleClause("paper", "contains", "sentence", 1),),
        zero_isolated_subtypes=("paper", "ghost"),
    )
    assert vacuous.unbacked_isolation_rules() == ("ghost",)
    # A corpus with no `ghost` at all satisfies "zero isolated ghost"...
    report = _report(nodes={"paper": 10, "sentence": 10},
                     triples={"paper -contains-> sentence": 10})
    C._score(report, vacuous)
    ghost = next(r for r in report.results if r.clause == "zero isolated ghost")
    assert ghost.ok, "the vacuous clause does pass — that is the whole problem"
    # ...so the contract itself must be rejected.
    assert not report.ok
    assert any(
        r.clause == "isolation rules are backed by existence"
        for r in report.failures
    )

    backed = C.CorpusContract(
        subtypes=("paper", "ghost"),
        triples=(C.TripleClause("paper", "contains", "sentence", 1),),
        zero_isolated_subtypes=("paper", "ghost"),
    )
    assert backed.unbacked_isolation_rules() == ()

    # And no shipped vertical may carry one.
    for name, vertical in VERTICALS.items():
        assert vertical.contract.unbacked_isolation_rules() == (), (
            f"vertical {name} can satisfy a connectivity rule by deleting "
            f"the subtype: {vertical.contract.unbacked_isolation_rules()}"
        )


def test_per_instance_provenance_is_derived_not_asserted(tmp_path):
    """The per-instance builder writes no snapshot, so it declares its own
    provenance — and that declaration must be able to come out `stand-in`.

    A flat `"provenance": "real"` here would reintroduce the exact defect
    removed from the domain builder: a claim that cannot come out any other
    way is not evidence. The instance DEFINITION is the artifact that varies
    (`--offline-fixture` authors it locally); the forge payload and checkout
    are unconditionally live, so they are stated as real and this test pins
    that reasoning rather than the literal.
    """
    from okg.bench.ab.corpus import CorpusSpec, _declare_provenance

    def spec(source: str) -> CorpusSpec:
        return CorpusSpec(
            repo="psf/requests", base_commit="deadbeef",
            cutoff="2014-11-01T02:20:16Z", gold_pr_number=None,
            deployment_name="bench-x", dsn="postgres:///x",
            corpus_root=tmp_path, instance_source=source,
        )

    released = _declare_provenance(spec("dataset"))
    assert released["provenance"] == "real"
    assert set(released["provenance_by_artifact"]) == {
        "github_api", "checkout", "instance_definition"
    }

    # A locally-authored instance definition taints the corpus, and the note
    # names WHICH artifact — matching declared_provenance's one-taints-all.
    local = _declare_provenance(spec("offline-fixture:/tmp/pilot.json"))
    assert local["provenance"] == "stand-in"
    assert local["provenance_by_artifact"]["instance_definition"] == "stand-in"
    assert "offline-fixture" in local["provenance_note"]

    # An UNSET origin must refuse, not fall back to the released value. The
    # field defaulted to "dataset" and a direct-constructor build script never
    # set it, so the audited corpus got `real` for free — the defaulting
    # defect this field exists to remove, one level down.
    with pytest.raises(CorpusBuildError, match="instance_source is unset"):
        _declare_provenance(spec(""))


def test_a_vertical_with_no_corpus_is_reported_not_silently_omitted(capsys):
    """req.design.audit: the rollup must name what it did NOT cover.

    A path-driven audit can only see verticals that produced a corpus.json —
    but a build whose contract is unmet RAISES before writing one, so the
    corpora most worth reporting are exactly the ones that leave nothing to
    audit. Omitting them makes an incomplete run look like a clean sweep,
    which is the program-level form of absence dressed as agreement.
    """
    import argparse

    from okg.bench.ab.cli import _cmd_corpus_audit_health

    rc = _cmd_corpus_audit_health(argparse.Namespace(corpus=[]))
    out = capsys.readouterr().out
    assert rc == 1, "auditing nothing must not exit clean"
    for name in VERTICALS:
        assert name in out, f"{name} was silently omitted from the rollup"
    assert out.count("NOT BUILT") == len(VERTICALS)
    assert f"{len(VERTICALS)} corpus/corpora do not meet their contract" in out


def test_stark_prime_contract():
    """PACT anchor (req.stark.corpus): rejects the entity-bag corpus the
    promoted-from testbed source would have produced, accepts the real
    2023-12-29 release build, and refuses the gold path.

    The `before` case is not a strawman: it is what publishing the SKB's
    entities with the relation stream dropped looks like, which is exactly
    the failure mode `expected_families nonzero` could not see."""
    contract = VERTICALS["stark-prime"].contract
    triples = {c.label: c.minimum for c in contract.triples}

    # composition is the release's, entire
    assert len(contract.triples) == 56
    assert len(contract.subtypes) == 10
    assert sum(c.minimum for c in contract.triples) == 8100498, (
        "the 56 floors must reconcile to the release's edge-row count, or "
        "some published family is under no clause"
    )
    assert set(contract.zero_isolated_subtypes) == set(contract.subtypes)
    assert contract.unbacked_isolation_rules() == ()

    # the release is symmetric, so every clause has an equal mirror
    for clause in contract.triples:
        mirror = next(
            (c for c in contract.triples
             if c.src == clause.dst and c.edge_type == clause.edge_type
             and c.dst == clause.src),
            None,
        )
        assert mirror is not None, clause.label
        assert mirror.minimum == clause.minimum, clause.label

    # an entity bag with no relations must FAIL
    before = _report(
        nodes={s: 1 for s in contract.subtypes},
        isolated={s: 1 for s in contract.subtypes},
    )
    C._score(before, contract)
    assert not before.ok
    kinds = {(r.kind, r.ok) for r in before.results}
    assert ("triple", False) in kinds
    assert ("isolation", False) in kinds

    # the real build must PASS
    after = _report(nodes={
            "stark_prime_anatomy": 14035,
            "stark_prime_biological_process": 28642,
            "stark_prime_cellular_component": 4176,
            "stark_prime_disease": 17080,
            "stark_prime_drug": 7957,
            "stark_prime_effect_phenotype": 15311,
            "stark_prime_exposure": 818,
            "stark_prime_gene_protein": 27671,
            "stark_prime_molecular_function": 11169,
            "stark_prime_pathway": 2516,
        }, triples=triples, isolated={})
    C._score(after, contract)
    assert after.ok, [r.line() for r in after.failures]
    assert after.total_nodes == 129375
    assert after.total_edges == 8100498
    assert after.connected_fraction() == 1.0

    # the gold gate is a probe, and it names the relation that would carry
    # the gold back in
    gold_probe = next(
        p for p in contract.probes if "gold" in p.name.lower()
        or "edge type the release does not state" in p.name
    )
    assert gold_probe.expect_empty
    assert "targets" in gold_probe.note


def test_phantomwiki_contract_is_binding():
    """The PhantomWiki contract constrains connectivity, backs every
    isolation rule with an existence rule, and refuses the composed
    vocabulary — checked as PROPERTIES of the contract object, which is the
    only part that can be tested without a database.
    """
    from okg.bench.ab.corpus_domain import VERTICALS

    spec = VERTICALS["phantomwiki"]
    contract = spec.contract

    assert contract.is_binding()
    # "zero isolated X" is vacuously true of a corpus holding no X
    assert contract.unbacked_isolation_rules() == ()

    triples = {(t.src, t.edge_type, t.dst) for t in contract.triples}
    # every relation an article states carries its OWN floor, so a
    # regression in one cannot hide inside the total of another
    for relation in (
        "mother", "father", "son", "daughter", "sister", "brother",
        "wife", "husband", "friend",
    ):
        assert ("person", relation, "person") in triples, relation

    # ...and nothing composed has a clause, because nothing composed has a
    # narrowing. `parent` and `sibling` are in this list on purpose: they
    # are base predicates in the released KB and they DO appear in
    # questions, but no article states them.
    for composed in (
        "grandmother", "granddaughter", "great_granddaughter", "niece",
        "nephew", "cousin", "female_cousin", "aunt", "uncle",
        "mother_in_law", "sister_in_law", "parent", "sibling", "child",
    ):
        assert not [t for t in triples if t[1] == composed], composed

    # the negative probe's admitted list must AGREE with the floors, or the
    # probe forgives a relation the contract requires (or vice versa)
    negative = next(
        p for p in contract.probes
        if p.name.startswith("negative: no composed kinship")
    )
    assert negative.expect_empty
    for relation in (
        "mother", "father", "son", "daughter", "sister", "brother",
        "wife", "husband", "friend",
    ):
        assert f"'{relation}'" in negative.sql, relation
    for composed in ("great_granddaughter", "niece", "cousin", "parent"):
        assert f"'{composed}'" not in negative.sql, composed

    # every floor states the observed count it was derived from, so a
    # reader can tell a measured floor from an invented one
    for clause in contract.triples:
        assert clause.note, clause.label
        assert "observed" in clause.note or "input" in clause.note.lower()

    # the isolation ceiling is 0.0 and the kinship rule is a PROBE, because
    # the subtype-level rule cannot see a lost kinship edge
    assert contract.max_isolated_fraction == 0.0
    assert any(
        "kinship-isolated" in p.name for p in contract.probes
    ), "the clause that actually binds kinship connectivity is missing"


def test_every_vertical_declares_a_binding_contract():
    """No vertical may ship without a contract — build() refuses one, and a
    missing contract is exactly how the node-bag corpora shipped."""
    for name, vertical in VERTICALS.items():
        assert vertical.contract.is_binding(), (
            f"vertical {name} has no binding contract"
        )
        assert vertical.contract.probes, (
            f"vertical {name} declares no answer-unit probe"
        )


def test_audit_counts_only_what_it_prints_and_accepts_generated(tmp_path, capsys,
                                                                monkeypatch):
    """The roll-up's failure TOTAL must be explained by the lines it printed,
    and `generated` must satisfy exactly where the build gate says it does.

    Found by running the eight-vertical roll-up: phantomwiki reported every
    clause [ok], provenance `generated` -- which is what that corpus IS, a
    universe produced by the release's own generator from a recorded seed --
    and the audit still counted it as an unmet contract. It printed nothing
    to say so, so the footer read "2 corpus/corpora do not meet their
    contract" above exactly ONE [FAIL] line.

    Two defects, one cause. The audit open-coded `!= "real"` instead of using
    the predicate the build gate enforces, so the two could disagree; and it
    incremented a counter with no corresponding output, so the disagreement
    was invisible rather than merely wrong. Both are asserted here: the
    accepted set is shared, and the total equals the printed [FAIL] count.
    """
    import argparse
    import json
    import re
    from pathlib import Path

    from okg.bench.ab import contract as contract_mod
    from okg.bench.ab.cli import _cmd_corpus_audit_health
    from okg.bench.ab.contract import SATISFYING_PROVENANCE, ContractReport

    assert "generated" in SATISFYING_PROVENANCE, (
        "a generated corpus substitutes for nothing; refusing it would "
        "permanently block a benchmark whose corpus is meant to be generated"
    )

    def corpus(vertical: str, provenance: str) -> Path:
        path = tmp_path / f"{vertical}-{provenance}.json"
        path.write_text(json.dumps({
            "vertical": vertical,
            "deployment_name": f"bench-{vertical}",
            "dsn": "postgres://unused",
            "generation_id": f"gen-{vertical}",
            "provenance": provenance,
        }), encoding="utf-8")
        return path

    # Every clause passes; provenance is the only variable under test.
    def fake_evaluate(*, dsn, generation_id, contract, provenance):
        return ContractReport(provenance=provenance)

    monkeypatch.setattr(contract_mod, "evaluate", fake_evaluate)

    good, bad = "phantomwiki", "literature"
    rc = _cmd_corpus_audit_health(
        argparse.Namespace(
            corpus=[corpus(good, "generated"), corpus(bad, "stand-in")]
        )
    )
    out = capsys.readouterr().out

    # The generated corpus is not among the failures...
    assert f"{good}\n  [FAIL]" not in out, "a passing generated corpus must not fail"
    # ...and the stand-in one is, WITH a line naming the reason.
    assert "[FAIL] provenance: 'stand-in'" in out, out

    # The invariant the defect broke: the footer is accounted for by output.
    printed = out.count("[FAIL]")
    reported = int(re.search(r"^(\d+) corpus/corpora do not meet", out,
                             re.M).group(1))
    assert reported == printed, (
        f"footer claims {reported} failures but printed {printed} [FAIL] "
        f"lines; an unexplained count is worse than a wrong one:\n{out}"
    )
    assert rc == 1
