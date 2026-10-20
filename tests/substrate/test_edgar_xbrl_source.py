"""EdgarXbrlSource + xbrl ontology module (okg-bench-domain-adapters,
req.dom.finauditing; okg-corpus-xbrl, req.xbrl.linkbases +
req.xbrl.context).

Synthetic docset fixtures only — the adapter has NO live network path,
so every test writes local `<filing_slug>/<doc_kind>.xml` files and
asserts on the emitted NodeFact/EdgeFact stream: schema declarations
become xbrl_concept nodes, instance facts become xbrl_fact nodes with
contains/reports/in_context edges, instance contexts become
xbrl_context nodes, calculation-linkbase arcs become calc_sums_to
edges, presentation link roles become xbrl_statement nodes with
presents / presentation_parent_of edges, definition arcs become
dim_related_to edges carrying their arcrole, and label-linkbase labels
become ATTRIBUTES on the concept. Malformed XML, an empty docsets root,
a filing without an instance document, and zero total facts all raise
loudly.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.edgar_xbrl import (
    EdgarXbrlSource,
    context_node_id,
    statement_node_id,
)


_EXT_NS = "http://example.com/ext"
_GAAP_NS = "http://fasb.org/us-gaap/2023"

_SCHEMA_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            targetNamespace="{_EXT_NS}">
  <xsd:element name="Assets" id="ext_Assets"/>
  <xsd:element name="AssetsCurrent" id="ext_AssetsCurrent"/>
  <xsd:element name="AssetsNoncurrent" id="ext_AssetsNoncurrent"/>
</xsd:schema>
"""

_INSTANCE_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:ext="{_EXT_NS}"
            xmlns:us-gaap="{_GAAP_NS}">
  <xbrli:context id="c1"><xbrli:entity/></xbrli:context>
  <xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
  <ext:Assets contextRef="c1" unitRef="usd">100</ext:Assets>
  <ext:AssetsCurrent contextRef="c1" unitRef="usd">60</ext:AssetsCurrent>
  <ext:AssetsNoncurrent contextRef="c1" unitRef="usd">40</ext:AssetsNoncurrent>
  <us-gaap:Liabilities contextRef="c1" unitRef="usd">50</us-gaap:Liabilities>
</xbrli:xbrl>
"""

_CALCULATION_XML = """<?xml version="1.0" encoding="UTF-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase"
               xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:calculationLink xlink:role="http://example.com/role/balance">
    <link:loc xlink:label="loc_assets" xlink:href="schema.xsd#ext_Assets"/>
    <link:loc xlink:label="loc_current"
              xlink:href="schema.xsd#ext_AssetsCurrent"/>
    <link:calculationArc xlink:from="loc_assets" xlink:to="loc_current"
                         weight="1.0"/>
  </link:calculationLink>
</link:linkbase>
"""


# ---------- a docset carrying all six document kinds ----------

_BALANCE_ROLE = "http://example.com/role/balance"
_INCOME_ROLE = "http://example.com/role/income"
_HYPERCUBE_ARCROLE = "http://xbrl.org/int/dim/arcrole/hypercube-dimension"
#: An arcrole the source has NO branch for — the arcrole vocabulary is
#: open to filer extension, so the edge must carry it rather than the
#: model having one edge type per known arcrole.
_CUSTOM_ARCROLE = "http://example.com/arcrole/relates-oddly"

_SCHEMA_FULL_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:link="http://www.xbrl.org/2003/linkbase"
            targetNamespace="{_EXT_NS}">
  <xsd:annotation>
    <xsd:appinfo>
      <link:roleType roleURI="{_BALANCE_ROLE}" id="BalanceRole">
        <link:definition>0000003 - Statement - Balance Sheets</link:definition>
        <link:usedOn>link:presentationLink</link:usedOn>
      </link:roleType>
      <link:roleType roleURI="{_INCOME_ROLE}" id="IncomeRole">
        <link:definition>0000005 - Statement - Income</link:definition>
      </link:roleType>
    </xsd:appinfo>
  </xsd:annotation>
  <xsd:element name="Assets" id="ext_Assets"/>
  <xsd:element name="AssetsCurrent" id="ext_AssetsCurrent"/>
  <xsd:element name="AssetsNoncurrent" id="ext_AssetsNoncurrent"/>
  <xsd:element name="SegmentTable" id="ext_SegmentTable"/>
  <xsd:element name="SegmentAxis" id="ext_SegmentAxis"/>
  <xsd:element name="RetailMember" id="ext_RetailMember"/>
</xsd:schema>
"""

_INSTANCE_FULL_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
            xmlns:ext="{_EXT_NS}"
            xmlns:us-gaap="{_GAAP_NS}">
  <xbrli:context id="c_instant">
    <xbrli:entity>
      <xbrli:identifier scheme="http://www.sec.gov/CIK"
        >0000320193</xbrli:identifier>
    </xbrli:entity>
    <xbrli:period><xbrli:instant>2023-12-31</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:context id="c_duration">
    <xbrli:entity>
      <xbrli:identifier scheme="http://www.sec.gov/CIK"
        >0000320193</xbrli:identifier>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2023-01-01</xbrli:startDate>
      <xbrli:endDate>2023-12-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:context id="c_segment">
    <xbrli:entity>
      <xbrli:identifier scheme="http://www.sec.gov/CIK"
        >0000320193</xbrli:identifier>
      <xbrli:segment>
        <xbrldi:explicitMember dimension="ext:SegmentAxis"
          >ext:RetailMember</xbrldi:explicitMember>
      </xbrli:segment>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2023-01-01</xbrli:startDate>
      <xbrli:endDate>2023-12-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:context id="c_noperiod">
    <xbrli:entity><xbrli:identifier>0000320193</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>forever</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
  <ext:Assets contextRef="c_instant" unitRef="usd">100</ext:Assets>
  <ext:AssetsCurrent contextRef="c_instant" unitRef="usd">60</ext:AssetsCurrent>
  <ext:AssetsNoncurrent contextRef="c_duration"
                        unitRef="usd">40</ext:AssetsNoncurrent>
  <us-gaap:Liabilities contextRef="c_segment"
                       unitRef="usd">50</us-gaap:Liabilities>
  <ext:Assets contextRef="c_noperiod" unitRef="usd">99</ext:Assets>
  <ext:Assets contextRef="c_never_declared" unitRef="usd">7</ext:Assets>
</xbrli:xbrl>
"""

_LABEL_XML = """<?xml version="1.0" encoding="UTF-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase"
               xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:labelLink xlink:role="http://www.xbrl.org/2003/role/link">
    <link:loc xlink:label="loc_assets" xlink:href="schema.xsd#ext_Assets"/>
    <link:loc xlink:label="loc_current"
              xlink:href="schema.xsd#ext_AssetsCurrent"/>
    <link:loc xlink:label="loc_liab"
              xlink:href="us-gaap-2023.xsd#us-gaap_Liabilities"/>
    <link:loc xlink:label="loc_gone" xlink:href="schema.xsd#no_such_id"/>
    <link:label xlink:label="lab_assets"
                xlink:role="http://www.xbrl.org/2003/role/label"
                >Total assets</link:label>
    <link:label xlink:label="doc_assets"
                xlink:role="http://www.xbrl.org/2003/role/documentation"
                >Sum of the carrying amounts of all assets.</link:label>
    <link:label xlink:label="terse_assets"
                xlink:role="http://www.xbrl.org/2003/role/terseLabel"
                >Assets</link:label>
    <link:label xlink:label="lab_current"
                xlink:role="http://www.xbrl.org/2003/role/label"
                ></link:label>
    <link:label xlink:label="lab_liab">Liabilities, total</link:label>
    <link:labelArc xlink:from="loc_assets" xlink:to="lab_assets"/>
    <link:labelArc xlink:from="loc_assets" xlink:to="doc_assets"/>
    <link:labelArc xlink:from="loc_assets" xlink:to="terse_assets"/>
    <link:labelArc xlink:from="loc_current" xlink:to="lab_current"/>
    <link:labelArc xlink:from="loc_liab" xlink:to="lab_liab"/>
    <link:labelArc xlink:from="loc_gone" xlink:to="lab_assets"/>
  </link:labelLink>
</link:linkbase>
"""

_PRESENTATION_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase"
               xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:presentationLink xlink:role="{_BALANCE_ROLE}">
    <link:loc xlink:label="loc_assets" xlink:href="schema.xsd#ext_Assets"/>
    <link:loc xlink:label="loc_current"
              xlink:href="schema.xsd#ext_AssetsCurrent"/>
    <link:loc xlink:label="loc_noncurrent"
              xlink:href="schema.xsd#ext_AssetsNoncurrent"/>
    <link:loc xlink:label="loc_gone" xlink:href="schema.xsd#no_such_id"/>
    <link:presentationArc xlink:from="loc_assets" xlink:to="loc_current"
                          order="1.0"/>
    <link:presentationArc xlink:from="loc_assets" xlink:to="loc_noncurrent"
                          order="2.0"/>
    <link:presentationArc xlink:from="loc_assets" xlink:to="loc_gone"
                          order="3.0"/>
  </link:presentationLink>
  <link:presentationLink xlink:role="{_INCOME_ROLE}">
    <link:loc xlink:label="loc_liab"
              xlink:href="us-gaap-2023.xsd#us-gaap_Liabilities"/>
    <link:loc xlink:label="loc_assets" xlink:href="schema.xsd#ext_Assets"/>
    <link:presentationArc xlink:from="loc_liab" xlink:to="loc_assets"
                          order="1.0"/>
  </link:presentationLink>
</link:linkbase>
"""

_DEFINITION_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase"
               xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:definitionLink xlink:role="{_BALANCE_ROLE}">
    <link:loc xlink:label="loc_table" xlink:href="schema.xsd#ext_SegmentTable"/>
    <link:loc xlink:label="loc_axis" xlink:href="schema.xsd#ext_SegmentAxis"/>
    <link:loc xlink:label="loc_member"
              xlink:href="schema.xsd#ext_RetailMember"/>
    <link:definitionArc xlink:arcrole="{_HYPERCUBE_ARCROLE}"
                        xlink:from="loc_table" xlink:to="loc_axis"
                        order="1.0"/>
    <link:definitionArc xlink:arcrole="{_CUSTOM_ARCROLE}"
                        xlink:from="loc_axis" xlink:to="loc_member"/>
    <link:definitionArc xlink:from="loc_table" xlink:to="loc_member"/>
  </link:definitionLink>
</link:linkbase>
"""

_FULL_DOCS = {
    "schema": _SCHEMA_FULL_XML,
    "instance": _INSTANCE_FULL_XML,
    "calculation": _CALCULATION_XML,
    "presentation": _PRESENTATION_XML,
    "label": _LABEL_XML,
    "definition": _DEFINITION_XML,
}


def _write_docset(
    root: Path, slug: str, *, docs: dict[str, str] | None = None
) -> Path:
    docs = docs if docs is not None else {
        "schema": _SCHEMA_XML,
        "instance": _INSTANCE_XML,
        "calculation": _CALCULATION_XML,
    }
    filing_dir = root / slug
    filing_dir.mkdir(parents=True)
    for kind, text in docs.items():
        (filing_dir / f"{kind}.xml").write_text(text, encoding="utf-8")
    return filing_dir


def _run(root: Path, *, truncated_documents: str = "raise"):
    source = EdgarXbrlSource(
        docsets_root=root, truncated_documents=truncated_documents
    )
    run = source.run("rid", mode="scope_complete")
    facts = list(run.facts)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    return source, run, nodes, edges


def test_linkbase_doc_kinds_are_canonicalised():
    """FinAuditing markers yield '<kind>_linkbase' stems; the source's lookups
    use the bare kind. Before normalisation `documents["calculation"]` never
    matched `calculation_linkbase`, so 25 filings with parseable calculation
    linkbases emitted ZERO calc arcs — a silent no-op."""
    from okg.substrate.library.sources.edgar_xbrl import _canonical_doc_kind

    assert _canonical_doc_kind("calculation_linkbase") == "calculation"
    assert _canonical_doc_kind("Presentation_Linkbase") == "presentation"
    assert _canonical_doc_kind("label_linkbase") == "label"
    assert _canonical_doc_kind("instance") == "instance"
    assert _canonical_doc_kind("schema") == "schema"


def test_truncated_documents_raise_by_default_and_count_when_opted_in(tmp_path):
    """A real EDGAR filing that will not parse is corrupt -> raise. A dataset
    export whose documents are capped upstream (FinAuditing truncates each
    embedded document at 32,767 bytes; 1,203 of 6,157 end mid-tag) is
    skip-and-COUNTED, never silently treated as empty."""
    from okg.substrate.errors import SubstrateError

    root = tmp_path / "docsets" / "filing-1"
    root.mkdir(parents=True)
    (root / "instance.xml").write_text(
        '<?xml version="1.0"?>\n<xbrl xmlns="http://www.xbrl.org/2003/instance">'
        '\n  <us-gaap:Assets contextRef="c1" unitRef="usd">1'  # mid-tag cut
    )
    strict = EdgarXbrlSource(repo="r", docsets_root=tmp_path / "docsets")
    with pytest.raises(SubstrateError, match="not parseable XML"):
        list(strict.run("rid", mode="scope_complete").facts)

    lenient = EdgarXbrlSource(
        repo="r", docsets_root=tmp_path / "docsets",
        truncated_documents="skip_counted",
    )
    with pytest.raises(SubstrateError, match="zero"):
        # Every document truncated => nothing emitted => still loud, because
        # a corpus that parsed into nothing is broken, not degraded.
        list(lenient.run("rid", mode="scope_complete").facts)


def test_xbrl_docset_parses_to_filing_graph(tmp_path):
    root = tmp_path / "docsets"
    _write_docset(root, "filing-a")
    source, run, nodes, edges = _run(root)

    filings = [n for n in nodes if n.subtype == "filing"]
    concepts = {
        n.attrs["name"]: n for n in nodes if n.subtype == "xbrl_concept"
    }
    facts = [n for n in nodes if n.subtype == "xbrl_fact"]

    # One filing node with the doc kinds and parse stats stamped on.
    assert len(filings) == 1
    filing = filings[0]
    assert filing.node_id == "filing:edgar:filing-a"
    assert filing.attrs["filing_id"] == "filing-a"
    assert filing.attrs["doc_kinds"] == "calculation,instance,schema"
    assert filing.attrs["fact_count"] == 4
    assert filing.attrs["facts_skipped"] == 0

    # 3 schema-declared concepts + 1 fact-referenced standard concept.
    assert set(concepts) == {
        "Assets", "AssetsCurrent", "AssetsNoncurrent", "Liabilities",
    }
    assert concepts["Assets"].node_id == f"xbrl_concept:{_EXT_NS}#Assets"
    assert concepts["Assets"].attrs["declared"] is True
    assert concepts["Assets"].attrs["namespace"] == _EXT_NS
    assert concepts["Liabilities"].attrs["declared"] is False
    assert concepts["Liabilities"].attrs["namespace"] == _GAAP_NS

    # 4 facts, in document order, carrying concept/context/unit/value.
    assert [f.node_id for f in facts] == [
        f"xbrl_fact:filing-a:{i:04d}" for i in range(4)
    ]
    assets_fact = facts[0]
    assert assets_fact.attrs["concept_name"] == "Assets"
    assert assets_fact.attrs["concept_ns"] == _EXT_NS
    assert assets_fact.attrs["context_ref"] == "c1"
    assert assets_fact.attrs["unit"] == "usd"
    assert assets_fact.attrs["value"] == "100"

    # Edge shapes: filing contains each fact and each context, each fact
    # reports its concept, and the calculation arc becomes one
    # calc_sums_to edge.
    triples = sorted((e.src, e.edge_type, e.dst) for e in edges)
    contains = [t for t in triples if t[1] == "contains"]
    reports = [t for t in triples if t[1] == "reports"]
    calc = [e for e in edges if e.edge_type == "calc_sums_to"]
    assert len(contains) == 5  # 4 facts + the instance's one context
    assert all(src == "filing:edgar:filing-a" for src, _, _ in contains)
    assert len(reports) == 4
    assert (
        "xbrl_fact:filing-a:0003",
        "reports",
        f"xbrl_concept:{_GAAP_NS}#Liabilities",
    ) in triples
    assert len(calc) == 1
    assert calc[0].src == f"xbrl_concept:{_EXT_NS}#AssetsCurrent"
    assert calc[0].dst == f"xbrl_concept:{_EXT_NS}#Assets"
    assert calc[0].attrs == {"weight": 1.0}

    # Snapshot-run conventions: full record set + exact applied token.
    assert run.record_set is not None
    assert "filing:edgar:filing-a" in run.record_set.records
    assert run.applied_raw_token is not None
    assert run.applied_token_authority == "exact"
    assert source.last_run_stats["filing-a"]["calc_arcs_emitted"] == 1


def test_concepts_dedupe_across_filings(tmp_path):
    root = tmp_path / "docsets"
    _write_docset(root, "filing-a")
    _write_docset(root, "filing-b")
    _, _, nodes, edges = _run(root)
    concept_ids = [n.node_id for n in nodes if n.subtype == "xbrl_concept"]
    # Same (namespace, name) across filings resolves to ONE node.
    assert len(concept_ids) == len(set(concept_ids)) == 4
    assert len([n for n in nodes if n.subtype == "filing"]) == 2
    assert len([n for n in nodes if n.subtype == "xbrl_fact"]) == 8


def test_skipped_facts_are_counted_not_defaulted(tmp_path):
    root = tmp_path / "docsets"
    instance = _INSTANCE_XML.replace(
        '<ext:Assets contextRef="c1" unitRef="usd">100</ext:Assets>',
        '<ext:Assets contextRef="c1" unitRef="usd"></ext:Assets>',
    )
    _write_docset(
        root, "filing-a",
        docs={"schema": _SCHEMA_XML, "instance": instance},
    )
    source, _, nodes, _ = _run(root)
    filing = next(n for n in nodes if n.subtype == "filing")
    assert filing.attrs["fact_count"] == 3
    assert filing.attrs["facts_skipped"] == 1
    assert source.last_run_stats["filing-a"]["facts_skipped"] == 1


def test_malformed_xml_raises(tmp_path):
    root = tmp_path / "docsets"
    _write_docset(
        root, "filing-a",
        docs={"instance": "<xbrl><unclosed>"},
    )
    with pytest.raises(SubstrateError, match="not parseable XML"):
        _run(root)


def test_empty_docsets_root_raises(tmp_path):
    root = tmp_path / "docsets"
    root.mkdir()
    with pytest.raises(SubstrateError, match="no filing directories"):
        _run(root)
    with pytest.raises(SubstrateError, match="not a directory"):
        _run(tmp_path / "nowhere")
    with pytest.raises(SubstrateError, match="pass docsets_root"):
        EdgarXbrlSource().run("rid", mode="scope_complete")


def test_missing_instance_document_raises(tmp_path):
    root = tmp_path / "docsets"
    _write_docset(
        root, "filing-a",
        docs={"schema": _SCHEMA_XML, "calculation": _CALCULATION_XML},
    )
    with pytest.raises(SubstrateError, match="no instance document"):
        _run(root)


def test_zero_total_facts_raises(tmp_path):
    root = tmp_path / "docsets"
    instance = (
        '<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance">'
        '<xbrli:context id="c1"/></xbrli:xbrl>'
    )
    _write_docset(
        root, "filing-a",
        docs={"schema": _SCHEMA_XML, "instance": instance},
    )
    with pytest.raises(SubstrateError, match="zero XBRL facts"):
        _run(root)


def test_wrong_schema_root_raises(tmp_path):
    root = tmp_path / "docsets"
    _write_docset(
        root, "filing-a",
        docs={"schema": "<NotASchema/>", "instance": _INSTANCE_XML},
    )
    with pytest.raises(SubstrateError, match="expected an XML Schema"):
        _run(root)


def test_unresolved_calc_arc_is_counted(tmp_path):
    root = tmp_path / "docsets"
    calculation = _CALCULATION_XML.replace(
        "schema.xsd#ext_AssetsCurrent", "schema.xsd#no_such_id"
    )
    _write_docset(
        root, "filing-a",
        docs={
            "schema": _SCHEMA_XML,
            "instance": _INSTANCE_XML,
            "calculation": calculation,
        },
    )
    source, _, nodes, edges = _run(root)
    assert [e for e in edges if e.edge_type == "calc_sums_to"] == []
    filing = next(n for n in nodes if n.subtype == "filing")
    assert filing.attrs["calc_arcs_skipped"] == 1
    assert source.last_run_stats["filing-a"]["calc_arcs_skipped"] == 1


# ---------- okg-corpus-xbrl / req.xbrl.linkbases ----------


def test_label_presentation_definition_linkbases(tmp_path):
    """The three linkbases the source used to read into NOTHING.

    Labels land as ATTRIBUTES on the concept (a concept no linkbase
    names carries NO `label` key); the presentation linkbase's extended
    link roles become xbrl_statement nodes with ordered `presents` edges
    and a parent->child tree; definition arcs become ONE edge type
    carrying the arcrole, including an arcrole the code has no branch
    for. Unresolvable locators are skipped AND COUNTED.
    """
    root = tmp_path / "docsets"
    _write_docset(root, "filing-a", docs=_FULL_DOCS)
    source, _, nodes, edges = _run(root)
    concepts = {
        n.attrs["name"]: n for n in nodes if n.subtype == "xbrl_concept"
    }
    stats = source.last_run_stats["filing-a"]

    # (1) LABEL -> attributes, never nodes.
    assert "xbrl_label" not in {n.subtype for n in nodes}
    assert concepts["Assets"].attrs["label"] == "Total assets"
    assert concepts["Assets"].attrs["documentation"] == (
        "Sum of the carrying amounts of all assets."
    )
    # xlink:role absent on the label element => the XBRL 2.1 standard
    # label role, applied to a concept only the INSTANCE creates (the
    # label is read before the instance and lands at construction).
    assert concepts["Liabilities"].attrs["label"] == "Liabilities, total"
    # A concept the label linkbase does not name carries NO label key —
    # not "", not the element name substituted.
    assert "label" not in concepts["AssetsNoncurrent"].attrs
    # An EMPTY label element is not a name: skipped, and the concept
    # keeps no key.
    assert "label" not in concepts["AssetsCurrent"].attrs
    assert stats["labels_parsed"] == 3
    assert stats["labels_skipped"] == 1
    assert stats["label_roles_ignored"] == 1  # terseLabel: real, unmodelled
    assert stats["label_locators_unresolved"] == 1
    assert stats["label_arcs_skipped"] == 2
    assert stats["labels_unmatched"] == 0

    # (2) PRESENTATION -> statements, presents, parent->child tree.
    statements = {
        n.attrs["role_uri"]: n for n in nodes if n.subtype == "xbrl_statement"
    }
    assert set(statements) == {_BALANCE_ROLE, _INCOME_ROLE}
    balance = statements[_BALANCE_ROLE]
    assert balance.node_id == statement_node_id("filing-a", _BALANCE_ROLE)
    assert balance.attrs["definition"] == (
        "0000003 - Statement - Balance Sheets"
    )
    assert balance.attrs["filing_id"] == "filing-a"
    assert (
        "filing:edgar:filing-a", "contains", balance.node_id
    ) in {(e.src, e.edge_type, e.dst) for e in edges}

    presents = [e for e in edges if e.edge_type == "presents"]
    assert len(presents) == 5  # 3 in the balance role, 2 in the income role
    by_concept = {
        (e.src, e.dst): e.attrs for e in presents
    }
    assert by_concept[
        (balance.node_id, f"xbrl_concept:{_EXT_NS}#AssetsCurrent")
    ] == {"order": 1.0}
    assert by_concept[
        (balance.node_id, f"xbrl_concept:{_EXT_NS}#AssetsNoncurrent")
    ] == {"order": 2.0}
    # A statement ROOT is no arc's child, so it has no order — an absent
    # key, never a 0.0 standing in for one.
    assert by_concept[
        (balance.node_id, f"xbrl_concept:{_EXT_NS}#Assets")
    ] == {}

    tree = [e for e in edges if e.edge_type == "presentation_parent_of"]
    assert len(tree) == 3
    # parent -> child, matching calc_sums_to's total -> component sense.
    assert (
        f"xbrl_concept:{_EXT_NS}#Assets",
        f"xbrl_concept:{_EXT_NS}#AssetsCurrent",
    ) in {(e.src, e.dst) for e in tree}
    assert next(
        e.attrs for e in tree
        if e.dst == f"xbrl_concept:{_EXT_NS}#AssetsCurrent"
    ) == {"role": _BALANCE_ROLE, "order": 1.0}
    assert stats["presentation_locators_unresolved"] == 1
    assert stats["presentation_arcs_skipped"] == 1
    assert stats["statements_emitted"] == 2

    # (3) DEFINITION -> ONE edge type carrying the arcrole.
    dim = [e for e in edges if e.edge_type == "dim_related_to"]
    assert {e.attrs["arcrole"] for e in dim} == {
        _HYPERCUBE_ARCROLE, _CUSTOM_ARCROLE,
    }
    hypercube = next(e for e in dim if e.attrs["arcrole"] == _HYPERCUBE_ARCROLE)
    assert hypercube.src == f"xbrl_concept:{_EXT_NS}#SegmentTable"
    assert hypercube.dst == f"xbrl_concept:{_EXT_NS}#SegmentAxis"
    assert hypercube.attrs["role"] == _BALANCE_ROLE
    # An arc with NO arcrole has no kind: skipped and counted, never
    # defaulted to a common arcrole.
    assert stats["definition_arcs_emitted"] == 2
    assert stats["definition_arcs_skipped"] == 1


def test_truncated_linkbase_is_counted_not_fatal(tmp_path):
    """A linkbase capped mid-tag by the dataset export is skipped and
    COUNTED. The filing keeps its facts and its other documents'
    structure — which is the whole point of per-document truncation
    accounting: an unreadable instance no longer means an isolated
    filing."""
    docs = dict(_FULL_DOCS)
    docs["presentation"] = _PRESENTATION_XML[:220]  # ends mid-tag
    root = tmp_path / "docsets"
    _write_docset(root, "filing-a", docs=docs)

    with pytest.raises(SubstrateError, match="not parseable XML"):
        _run(root)  # a real EDGAR filing that will not parse is corrupt

    source, _, nodes, edges = _run(root, truncated_documents="skip_counted")
    stats = source.last_run_stats["filing-a"]
    assert stats["documents_truncated"] == 1
    assert [n for n in nodes if n.subtype == "xbrl_statement"] == []
    assert stats["statements_emitted"] == 0
    # The rest of the docset still lands.
    assert stats["facts_emitted"] == 5
    assert [e for e in edges if e.edge_type == "dim_related_to"]
    filing = next(n for n in nodes if n.subtype == "filing")
    assert filing.attrs["documents_truncated"] == 1


# ---------- okg-corpus-xbrl / req.xbrl.context ----------


def test_contexts_are_nodes_and_bind_every_fact(tmp_path):
    """`context_ref` used to be a dangling string, so "the value for
    period X" was a string match over ids like
    C_0000320193_20230101_20231231. Contexts are nodes now, every
    emitted fact carries EXACTLY ONE in_context edge, and a fact whose
    contextRef names no context element is skipped and counted rather
    than emitted with a dangling reference."""
    root = tmp_path / "docsets"
    _write_docset(root, "filing-a", docs=_FULL_DOCS)
    source, _, nodes, edges = _run(root)
    stats = source.last_run_stats["filing-a"]
    contexts = {
        n.attrs["context_ref"]: n for n in nodes if n.subtype == "xbrl_context"
    }
    facts = [n for n in nodes if n.subtype == "xbrl_fact"]

    assert set(contexts) == {
        "c_instant", "c_duration", "c_segment", "c_noperiod",
    }
    instant = contexts["c_instant"]
    assert instant.node_id == context_node_id("filing-a", "c_instant")
    assert instant.attrs["entity_identifier"] == "0000320193"
    assert instant.attrs["entity_scheme"] == "http://www.sec.gov/CIK"
    assert instant.attrs["period_type"] == "instant"
    assert instant.attrs["instant"] == "2023-12-31"
    assert "start_date" not in instant.attrs

    duration = contexts["c_duration"]
    assert duration.attrs["period_type"] == "duration"
    assert duration.attrs["start_date"] == "2023-01-01"
    assert duration.attrs["end_date"] == "2023-12-31"
    assert "instant" not in duration.attrs

    # A period whose dates do not parse leaves NO period_type — an
    # unknown period is absent, never guessed at.
    noperiod = contexts["c_noperiod"]
    assert "period_type" not in noperiod.attrs
    assert "instant" not in noperiod.attrs
    assert stats["context_periods_unparseable"] == 1

    # EXACTLY one in_context edge per fact node.
    in_context = [e for e in edges if e.edge_type == "in_context"]
    assert len(in_context) == len(facts) == 5
    assert len({e.src for e in in_context}) == len(facts)
    assert {e.src for e in in_context} == {f.node_id for f in facts}
    assert all(
        e.dst in {c.node_id for c in contexts.values()} for e in in_context
    )

    # The fact whose contextRef names no context element is skipped and
    # counted — never emitted with a dangling reference.
    assert stats["facts_no_context"] == 1
    assert stats["facts_skipped"] == 1
    assert "c_never_declared" not in {f.attrs["context_ref"] for f in facts}
    filing = next(n for n in nodes if n.subtype == "filing")
    assert filing.attrs["facts_no_context"] == 1
    assert filing.attrs["context_count"] == 4

    # filing -contains-> xbrl_context, one per context.
    triples = {(e.src, e.edge_type, e.dst) for e in edges}
    for context in contexts.values():
        assert (
            "filing:edgar:filing-a", "contains", context.node_id
        ) in triples

    # One has_member edge per explicit member element parsed.
    members = [e for e in edges if e.edge_type == "has_member"]
    assert len(members) == 1
    assert members[0].src == contexts["c_segment"].node_id
    assert members[0].dst == f"xbrl_concept:{_EXT_NS}#RetailMember"
    assert members[0].attrs == {"dimension": "ext:SegmentAxis"}
    assert stats["context_members_emitted"] == 1


# ---------- structural stranding: stamped from the DOCUMENTS ----------


#: a parseable filer schema that declares NO extension element — only an
#: import and a role type. The state the second real build found in all 5
#: isolated FinAuditing filings, which is what falsified the `declares`
#: remedy for them.
_SCHEMA_NO_ELEMENTS_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:link="http://www.xbrl.org/2003/linkbase"
            targetNamespace="{_EXT_NS}">
  <xsd:import namespace="{_GAAP_NS}" schemaLocation="us-gaap-2023.xsd"/>
  <xsd:annotation>
    <xsd:appinfo>
      <link:roleType roleURI="{_BALANCE_ROLE}" id="BalanceSheet">
        <link:definition>Balance Sheet</link:definition>
      </link:roleType>
    </xsd:appinfo>
  </xsd:annotation>
</xsd:schema>
"""

#: cut mid-tag, exactly like a document past FinAuditing's 32,767-byte cap
_TRUNCATED_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance">\n'
    '  <xbrli:context id="c1'
)

#: an instance that PARSES and yields no context and no fact: the whole
#: point of this fixture is that the input is intact while emission is
#: empty, which is where the old emission-derived predicate went wrong.
_INSTANCE_NO_CONTEXTS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance">
  <xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
</xbrli:xbrl>
"""


def test_structural_stranding_is_read_from_the_documents_not_the_facts(
    tmp_path,
):
    """`structurally_stranded` DISCRIMINATES, measured on the complement.

    Its predecessor was `documents_truncated and not statement_nodes and
    not context_nodes` — two of three conjuncts asking whether EMISSION
    produced anything. On a filing that happened to carry any truncated
    document (a capped label linkbase would do), a parsing regression
    inside `_parse_instance` flipped the flag to true and forgave itself,
    and the corpus contract's isolated-filing probe exempts on exactly
    this flag.

    `filing-label-capped` is the witness: one truncated document, zero
    contexts, zero statements — true under the old predicate, false under
    the new one, and connected in fact.
    """
    root = tmp_path / "docsets"
    _write_docset(root, "filing-intact", docs=_FULL_DOCS)
    # No parseable instance, no parseable presentation, and a schema that
    # declares nothing: none of the three filing-anchoring documents can
    # carry an edge, so this filing is stranded BY THE INPUT.
    _write_docset(root, "filing-capped", docs={
        "schema": _SCHEMA_NO_ELEMENTS_XML,
        "instance": _TRUNCATED_XML,
        "presentation": _TRUNCATED_XML,
        "label": _LABEL_XML,
    })
    # One truncated document, an instance that parses to nothing, and no
    # presentation linkbase at all — but a schema that DECLARES concepts,
    # so the filing is anchored and must not be stamped.
    _write_docset(root, "filing-label-capped", docs={
        "schema": _SCHEMA_XML,
        "instance": _INSTANCE_NO_CONTEXTS_XML,
        "label": _TRUNCATED_XML,
    })

    _source, _run_out, nodes, edges = _run(
        root, truncated_documents="skip_counted"
    )
    filings = {
        n.attrs["filing_id"]: n for n in nodes if n.subtype == "filing"
    }
    assert set(filings) == {
        "filing-intact", "filing-capped", "filing-label-capped",
    }

    assert {
        slug: node.attrs["structurally_stranded"]
        for slug, node in filings.items()
    } == {
        "filing-intact": False,
        "filing-capped": True,
        "filing-label-capped": False,
    }
    # The witness: emission produced no context and no statement for it,
    # and one of its documents was truncated — the three inputs the OLD
    # predicate read — yet its instance parsed, so it is not stranded.
    witness = filings["filing-label-capped"]
    assert witness.attrs["documents_truncated"] == 1
    assert witness.attrs["context_count"] == 0
    assert witness.attrs["statement_count"] == 0
    assert witness.attrs["instance_unusable"] is False
    assert witness.attrs["presentation_unusable"] is True
    assert witness.attrs["schema_declares_no_concepts"] is False
    # ...and the stranded filing says WHICH inputs were unusable, so the
    # flag can be audited rather than taken on trust.
    capped = filings["filing-capped"]
    assert capped.attrs["instance_unusable"] is True
    assert capped.attrs["presentation_unusable"] is True
    assert capped.attrs["schema_declares_no_concepts"] is True

    # THE COMPLEMENT. Stamped exactly when isolated; two connected filings
    # carry `false`, so the equality is discriminating rather than a
    # blanket allowance for every filing.
    connected = {e.src for e in edges} | {e.dst for e in edges}
    isolated = {
        slug for slug, node in filings.items() if node.node_id not in connected
    }
    stranded = {
        slug for slug, node in filings.items()
        if node.attrs["structurally_stranded"]
    }
    assert stranded == isolated == {"filing-capped"}
    assert set(filings) - stranded == {
        "filing-intact", "filing-label-capped",
    }


# ---------- xbrl module composition ----------


def test_xbrl_module_composes(tmp_path):
    """A deployment manifest listing module 'xbrl' composes a catalog
    admitting exactly the shapes the source emits (same harness as
    test_cti_sources.test_cti_module_composes)."""
    deployment = tmp_path / "scratch"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {"name": "scratch", "schema_dir": "schemas", "modules": ["xbrl"]}
        )
    )
    (deployment / "schemas").mkdir()

    composed = compose_catalog(deployment)

    for subtype in (
        "filing", "xbrl_concept", "xbrl_fact", "xbrl_statement",
        "xbrl_context",
    ):
        assert subtype in composed.subtypes, sorted(composed.subtypes)
        assert composed.subtype_origin[subtype] == "xbrl"

    # Exactly the narrowings the source emits, asserted as a SET rather
    # than as a count. A count can only report that two numbers differ;
    # it cannot NAME the triple that appeared or vanished, which is the
    # one thing the reader needs. (This assertion previously counted to
    # ten and went red when `filing declares xbrl_concept` was added —
    # reporting `11 == 10` and nothing else.) One dimensional edge type
    # rather than one per arcrole is part of what is pinned here: a
    # per-arcrole split would reject an unanticipated filer arcrole at
    # insert as a narrowing_violation and fail the publish.
    expected = {
        ("filing", "contains", "xbrl_fact"),
        ("filing", "contains", "xbrl_context"),
        ("filing", "contains", "xbrl_statement"),
        # The anchor for a filing whose instance AND presentation
        # linkbase are both truncated: its schema still declares the
        # extension concepts it defines.
        ("filing", "declares", "xbrl_concept"),
        ("xbrl_fact", "reports", "xbrl_concept"),
        ("xbrl_fact", "in_context", "xbrl_context"),
        ("xbrl_context", "has_member", "xbrl_concept"),
        ("xbrl_statement", "presents", "xbrl_concept"),
        ("xbrl_concept", "calc_sums_to", "xbrl_concept"),
        ("xbrl_concept", "presentation_parent_of", "xbrl_concept"),
        ("xbrl_concept", "dim_related_to", "xbrl_concept"),
    }
    assert expected <= composed.narrowing_triples()
    xbrl_rows = [r for r in composed.narrowings if r.origin == "xbrl"]
    assert {
        (r.parent_subtype, r.edge_archetype, r.child_subtype)
        for r in xbrl_rows
    } == expected
    # And no duplicate rows hiding behind the set comparison.
    assert len(xbrl_rows) == len(expected)
