"""CTI snapshot sources + cti ontology module (okg-bench-ctibench,
task.cti.sources; okg-corpus-cti, task.cti2.attack + task.cti2.weakness).

Synthetic snapshot fixtures only — the adapters have NO live network
path, so every test writes local files and asserts on the emitted
NodeFact/EdgeFact stream:

- NVD: the SAME CVE encoded in the legacy 1.1 feed shape and the 2.0
  feed shape parses to IDENTICAL node attrs + has_weakness edges;
  .json.gz handling; unknown-shape and zero-item snapshots raise; the
  v3.1 vector decomposes into eight individually queryable metrics that
  reassemble to the released string, while a v2-only or unscored CVE
  leaves those keys ABSENT.
- CWE: catalog XML parses namespace-aware (.xml and .zip); malformed
  and empty catalogs raise; node ids match the dst ids of nvd_feeds'
  has_weakness edges (the cross-source contract); Related_Weakness
  becomes child_of (ChildOf) or related_to (every other nature), a
  Category's Has_Member rows become has_member (a third edge type, so a
  thematic grouping can never read as ancestry), a relation or
  membership pointing outside the catalog is skipped AND counted, and
  an entry NO declaration relates to another entry carries
  no_relations_reason — stamped from the declarations, never from the
  emitted edges, so a lost edge leaves an isolated node unstamped and a
  negative isolation probe fires on it.
- ATT&CK: bundle parses techniques + sub-techniques + tactics with
  part_of edges, the seven entity types the graph points at (including
  x-mitre-detection-strategy, the source of `detects` in every current
  release, and the x-mitre-analytic layer beneath it), the ten
  relationship-derived triples, and the two triples ATT&CK ships as
  EMBEDDED refs rather than relationship objects; revoked/deprecated
  objects are skipped and counted, a relationship or embedded ref
  touching one is dropped and counted, an unresolved ref is counted
  rather than stubbed, a pair matching no declared triple RAISES from
  either path, and non-bundle JSON raises.
- The cti ontology module composes: a deployment manifest listing
  module 'cti' yields a catalog with the eleven subtypes and exactly
  the seventeen narrowings the sources emit — the narrowing set is
  asserted equal to the adapter's own declared triples, so the two can
  never drift apart.
"""
from __future__ import annotations

import gzip
import json
import zipfile
from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.attack_stix import (
    _DECLARED_TRIPLES,
    AttackStixSource,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.cwe_catalog import CweCatalogSource
from okg.substrate.library.sources.nvd_feeds import (
    _CVSS_BASE_METRICS,
    NvdFeedsSource,
)


# ---------- NVD fixtures ----------
#
# One real-shaped CVE (Log4Shell), encoded once per feed shape. The
# timestamps intentionally use each shape's native encoding of the
# same instant (1.1 minute-precision `Z`; 2.0 naive-UTC with
# fractional seconds) so the identical-parse test exercises timestamp
# normalization instead of relying on byte-equal fixture strings.

_DESCRIPTION = (
    "Apache Log4j2 JNDI features used in configuration do not protect "
    "against attacker controlled LDAP endpoints."
)
_VECTOR = "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"

#: The nine attributes the CVSS decomposition owns (req.cti2.weakness).
_CVSS_METRIC_ATTRS = (
    "cvss_attack_vector",
    "cvss_attack_complexity",
    "cvss_privileges_required",
    "cvss_user_interaction",
    "cvss_scope",
    "cvss_confidentiality_impact",
    "cvss_integrity_impact",
    "cvss_availability_impact",
    "cvss_base_severity",
)


def _nvd_11_doc() -> dict:
    return {
        "CVE_data_type": "CVE",
        "CVE_Items": [
            {
                "cve": {
                    "CVE_data_meta": {"ID": "CVE-2021-44228"},
                    "problemtype": {
                        "problemtype_data": [
                            {
                                "description": [
                                    {"lang": "en", "value": "CWE-502"},
                                    {"lang": "en", "value": "CWE-20"},
                                ]
                            }
                        ]
                    },
                    "description": {
                        "description_data": [
                            {"lang": "en", "value": _DESCRIPTION},
                        ]
                    },
                },
                "impact": {
                    "baseMetricV3": {
                        "cvssV3": {
                            "version": "3.1",
                            "vectorString": _VECTOR,
                            "baseScore": 10.0,
                            "baseSeverity": "CRITICAL",
                        }
                    }
                },
                "publishedDate": "2021-12-10T10:15Z",
                "lastModifiedDate": "2023-11-07T03:39Z",
            }
        ],
    }


def _nvd_20_doc() -> dict:
    return {
        "resultsPerPage": 1,
        "vulnerabilities": [
            {
                "cve": {
                    "id": "CVE-2021-44228",
                    "published": "2021-12-10T10:15:00.000",
                    "lastModified": "2023-11-07T03:39:00.000",
                    "descriptions": [
                        {"lang": "en", "value": _DESCRIPTION},
                    ],
                    "metrics": {
                        "cvssMetricV31": [
                            {
                                "type": "Primary",
                                "cvssData": {
                                    "version": "3.1",
                                    "vectorString": _VECTOR,
                                    "baseScore": 10.0,
                                    "baseSeverity": "CRITICAL",
                                },
                            }
                        ]
                    },
                    "weaknesses": [
                        {
                            "source": "nvd@example.gov",
                            "type": "Primary",
                            "description": [
                                {"lang": "en", "value": "CWE-502"},
                                {"lang": "en", "value": "CWE-20"},
                            ],
                        }
                    ],
                }
            }
        ],
    }


def _run_nvd(tmp_path: Path, name: str, doc: dict):
    feed_dir = tmp_path / name
    feed_dir.mkdir()
    (feed_dir / "feed.json").write_text(json.dumps(doc))
    source = NvdFeedsSource(snapshot_glob=str(feed_dir / "*.json*"))
    run = source.run("rid", mode="scope_complete")
    facts = list(run.facts)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    return run, nodes, edges


def _nvd_source(tmp_path: Path, name: str, doc: dict) -> NvdFeedsSource:
    """The adapter itself, for tests that assert on last_run_stats."""
    feed_dir = tmp_path / name
    feed_dir.mkdir()
    (feed_dir / "feed.json").write_text(json.dumps(doc))
    return NvdFeedsSource(snapshot_glob=str(feed_dir / "*.json*"))


def test_nvd_11_and_20_shapes_parse_identically(tmp_path):
    _, nodes_11, edges_11 = _run_nvd(tmp_path, "v11", _nvd_11_doc())
    _, nodes_20, edges_20 = _run_nvd(tmp_path, "v20", _nvd_20_doc())

    assert len(nodes_11) == len(nodes_20) == 1
    node_11, node_20 = nodes_11[0], nodes_20[0]
    assert node_11.node_id == node_20.node_id == "cve:nvd:CVE-2021-44228"
    assert node_11.subtype == node_20.subtype == "cve"
    assert node_11.attrs == node_20.attrs
    assert node_11.attrs["cve_id"] == "CVE-2021-44228"
    assert node_11.attrs["description"] == _DESCRIPTION
    assert node_11.attrs["cvss_version"] == "3.1"
    assert node_11.attrs["cvss_vector"] == _VECTOR
    assert node_11.attrs["cvss_base_score"] == 10.0
    # Normalized to the same UTC instant despite different feed
    # encodings.
    assert node_11.attrs["published_at"] == "2021-12-10T10:15:00+00:00"
    assert node_11.attrs["modified_at"] == "2023-11-07T03:39:00+00:00"
    assert node_11.attrs["created_at"] == node_11.attrs["published_at"]

    triples_11 = [(e.src, e.edge_type, e.dst) for e in edges_11]
    triples_20 = [(e.src, e.edge_type, e.dst) for e in edges_20]
    assert triples_11 == triples_20 == [
        ("cve:nvd:CVE-2021-44228", "has_weakness", "cwe:mitre:CWE-502"),
        ("cve:nvd:CVE-2021-44228", "has_weakness", "cwe:mitre:CWE-20"),
    ]


def test_nvd_gz_feed_parses(tmp_path):
    feed_dir = tmp_path / "gz"
    feed_dir.mkdir()
    (feed_dir / "nvdcve-1.1-2021.json.gz").write_bytes(
        gzip.compress(json.dumps(_nvd_11_doc()).encode("utf-8"))
    )
    source = NvdFeedsSource(snapshot_glob=str(feed_dir / "*.json*"))
    run = source.run("rid", mode="scope_complete")
    nodes = [f for f in run.facts if isinstance(f, NodeFact)]
    assert [n.node_id for n in nodes] == ["cve:nvd:CVE-2021-44228"]
    # scope-complete snapshot runs declare their full record set and an
    # exact applied token (github-adapter convention).
    assert run.record_set is not None
    assert "cve:nvd:CVE-2021-44228" in run.record_set.records
    assert run.applied_raw_token is not None
    assert run.applied_token_authority == "exact"


def test_nvd_corrupt_gz_raises(tmp_path):
    feed_dir = tmp_path / "badgz"
    feed_dir.mkdir()
    (feed_dir / "feed.json.gz").write_bytes(b"not gzip at all")
    source = NvdFeedsSource(snapshot_glob=str(feed_dir / "*.json*"))
    with pytest.raises(SubstrateError, match="not valid gzip"):
        source.run("rid", mode="scope_complete")


def test_nvd_unknown_shape_raises(tmp_path):
    feed_dir = tmp_path / "unknown"
    feed_dir.mkdir()
    (feed_dir / "feed.json").write_text(
        json.dumps({"totally": "unrelated"})
    )
    source = NvdFeedsSource(snapshot_glob=str(feed_dir / "*.json*"))
    with pytest.raises(SubstrateError, match="neither an NVD 1.1"):
        source.run("rid", mode="scope_complete")


def test_nvd_zero_items_raises(tmp_path):
    feed_dir = tmp_path / "empty"
    feed_dir.mkdir()
    (feed_dir / "feed.json").write_text(json.dumps({"CVE_Items": []}))
    source = NvdFeedsSource(snapshot_glob=str(feed_dir / "*.json*"))
    with pytest.raises(SubstrateError, match="zero CVE entries"):
        source.run("rid", mode="scope_complete")


def test_nvd_no_matched_files_raises(tmp_path):
    source = NvdFeedsSource(snapshot_glob=str(tmp_path / "nowhere" / "*.json"))
    with pytest.raises(SubstrateError, match="matched no files"):
        source.run("rid", mode="scope_complete")


def test_nvd_weakness_mapping_status_keeps_the_three_statements_apart(tmp_path):
    """The CVE-side twin of cwe's no_relations_reason.

    NVD makes three DIFFERENT statements about a CVE with no CWE, and
    they are not interchangeable: no information, an analyst judgment
    that the weakness is outside CWE, and silence. Each gets its own
    value; a CVE NVD did map carries no status at all, so an isolated
    CVE without one lost its edge.
    """
    def _with_weakness(descriptions):
        doc = _nvd_20_doc()
        cve = doc["vulnerabilities"][0]["cve"]
        if descriptions is None:
            cve.pop("weaknesses", None)
        else:
            cve["weaknesses"] = [{
                "source": "nvd@example.gov",
                "type": "Primary",
                "description": descriptions,
            }]
        return doc

    def _run(name, doc):
        source = _nvd_source(tmp_path, name, doc)
        facts = list(source.run("rid", mode="scope_complete").facts)
        return (
            source,
            [f for f in facts if isinstance(f, NodeFact)],
            [f for f in facts if isinstance(f, EdgeFact)],
        )

    # Mapped: no status key at all, and the edge is there.
    _, nodes, edges = _run("mapped", _nvd_20_doc())
    assert "weakness_mapping_status" not in nodes[0].attrs
    assert edges

    # NVD says it has no information.
    source, nodes, edges = _run(
        "noinfo",
        _with_weakness([{"lang": "en", "value": "NVD-CWE-noinfo"}]),
    )
    assert nodes[0].attrs["weakness_mapping_status"] == "nvd_cwe_noinfo"
    assert edges == []
    assert source.last_run_stats["cves_nvd_cwe_noinfo"] == 1

    # NVD says an analyst placed the weakness OUTSIDE CWE. A different
    # claim from "no information", so a different value.
    source, nodes, edges = _run(
        "other",
        _with_weakness([{"lang": "en", "value": "NVD-CWE-Other"}]),
    )
    assert nodes[0].attrs["weakness_mapping_status"] == "nvd_cwe_other"
    assert edges == []
    assert source.last_run_stats["cves_nvd_cwe_other"] == 1

    # NVD says nothing either way — no weakness block at all. This is
    # the case all 991 isolated CVEs in the real feed are in.
    source, nodes, edges = _run("silent", _with_weakness(None))
    assert nodes[0].attrs["weakness_mapping_status"] == "not_stated"
    assert edges == []
    assert source.last_run_stats["cves_not_stated"] == 1

    # A marker alongside a REAL id does not make the CVE unmapped: 14
    # CVEs in the released feed look like this.
    source, nodes, edges = _run(
        "mixed",
        _with_weakness([
            {"lang": "en", "value": "CWE-79"},
            {"lang": "en", "value": "NVD-CWE-noinfo"},
        ]),
    )
    assert "weakness_mapping_status" not in nodes[0].attrs
    assert [e.dst for e in edges] == ["cwe:mitre:CWE-79"]
    assert source.last_run_stats["cves_with_both_cwe_ref_and_marker"] == 1


def test_nvd_unknown_weakness_marker_raises(tmp_path):
    """An unread marker must never mint a status.

    A stamp is only worth anything if it always names a KNOWN upstream
    statement — silently treating an unrecognized value as "no mapping"
    would forgive an isolated CVE for a reason nobody has read.
    """
    doc = _nvd_20_doc()
    doc["vulnerabilities"][0]["cve"]["weaknesses"] = [{
        "source": "nvd@example.gov",
        "type": "Primary",
        "description": [{"lang": "en", "value": "Insufficient Information"}],
    }]
    path = tmp_path / "unknown-marker.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(
        SubstrateError,
        match=r"neither a CWE id nor a known NVD no-mapping marker",
    ):
        NvdFeedsSource(
            snapshot_glob=str(tmp_path / "unknown-marker.json"),
        ).run("rid", mode="scope_complete")


def test_nvd_unscored_cve_emits_null_cvss_and_no_edges(tmp_path):
    """No CVSS + only NVD's no-mapping CWE marker: honest nulls, zero
    edges — never a defaulted score (default-as-fallback is a P0)."""
    doc = _nvd_11_doc()
    item = doc["CVE_Items"][0]
    item["impact"] = {}
    item["cve"]["problemtype"]["problemtype_data"][0]["description"] = [
        {"lang": "en", "value": "NVD-CWE-noinfo"},
    ]
    _, nodes, edges = _run_nvd(tmp_path, "unscored", doc)
    assert nodes[0].attrs["cvss_version"] is None
    assert nodes[0].attrs["cvss_vector"] is None
    assert nodes[0].attrs["cvss_base_score"] is None
    # The v3 metric keys are ABSENT, not null: a null key would answer
    # a presence probe with "yes" while carrying no metric.
    for attr in _CVSS_METRIC_ATTRS:
        assert attr not in nodes[0].attrs
    assert edges == []


def test_metric_attr_list_tracks_the_source_decomposition() -> None:
    """The nine names this file asserts presence and ABSENCE on are
    exactly the ones nvd_feeds decomposes. Without this, a metric added
    to (or dropped from) `_CVSS_BASE_METRICS` would silently go
    unasserted -- the absence checks would keep passing while covering
    one attribute fewer than the source owns."""
    assert _CVSS_METRIC_ATTRS == tuple(
        attr for _abbrev, attr, _values in _CVSS_BASE_METRICS
    ) + ("cvss_base_severity",)


# ---------- CWE catalog ----------

_CWE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Weakness_Catalog xmlns="http://cwe.mitre.org/cwe-7"
                  Name="CWE" Version="4.13" Date="2023-10-26">
  <Weaknesses>
    <Weakness ID="79"
              Name="Improper Neutralization of Input During Web Page Generation ('Cross-site Scripting')"
              Abstraction="Base">
      <Description>The product does not neutralize user-controllable
        input before it is placed in output.</Description>
      <Related_Weaknesses>
        <Related_Weakness Nature="ChildOf" CWE_ID="74" View_ID="1000"
                          Ordinal="Primary"/>
        <Related_Weakness Nature="ChildOf" CWE_ID="74" View_ID="1003"/>
        <Related_Weakness Nature="PeerOf" CWE_ID="502" View_ID="1000"/>
        <Related_Weakness Nature="ChildOf" CWE_ID="9999" View_ID="1000"/>
      </Related_Weaknesses>
      <Applicable_Platforms>
        <Language Class="Not Language-Specific" Prevalence="Undetermined"/>
        <Technology Name="Web Server" Prevalence="Undetermined"/>
      </Applicable_Platforms>
      <Common_Consequences>
        <Consequence>
          <Scope>Confidentiality</Scope>
          <Scope>Integrity</Scope>
          <Impact>Read Application Data</Impact>
        </Consequence>
        <Consequence>
          <Scope>Confidentiality</Scope>
          <Impact>Bypass Protection Mechanism</Impact>
        </Consequence>
      </Common_Consequences>
    </Weakness>
    <Weakness ID="74"
              Name="Improper Neutralization of Special Elements in Output"
              Abstraction="Class">
      <Description>The product constructs a command using
        externally-influenced input.</Description>
    </Weakness>
    <Weakness ID="502" Name="Deserialization of Untrusted Data"
              Abstraction="Base">
      <Description>The product deserializes untrusted data.</Description>
    </Weakness>
    <Weakness ID="1004" Name="Sensitive Cookie Without HttpOnly Flag"
              Abstraction="Variant"/>
    <Weakness ID="132" Name="DEPRECATED: Miscalculated Null Termination"
              Abstraction="Variant" Status="Deprecated"/>
  </Weaknesses>
  <Categories>
    <Category ID="310" Name="Cryptographic Issues" Status="Obsolete">
      <Summary>Weaknesses in this category are related to the
        use of cryptography.</Summary>
      <Relationships>
        <Has_Member CWE_ID="79" View_ID="1000"/>
        <Has_Member CWE_ID="79" View_ID="699"/>
        <Has_Member CWE_ID="1004" View_ID="1000"/>
        <Has_Member CWE_ID="16" View_ID="1000"/>
        <Has_Member CWE_ID="6666" View_ID="1000"/>
      </Relationships>
    </Category>
    <Category ID="16" Name="Configuration" Status="Obsolete">
      <Summary>Weaknesses in this category are introduced during
        configuration.</Summary>
    </Category>
    <Category ID="1155" Status="Stable"
              Name="SEI CERT C Coding Standard - Guidelines 01. Preprocessor (PRE)">
      <Summary>A mapping shell the catalog ships with no members.</Summary>
    </Category>
  </Categories>
</Weakness_Catalog>
"""


def test_cwe_xml_parses(tmp_path):
    path = tmp_path / "cwec_v4.13.xml"
    path.write_text(_CWE_XML)
    source = CweCatalogSource(snapshot_path=path)
    run = source.run("rid", mode="scope_complete")
    nodes = [f for f in run.facts if isinstance(f, NodeFact)]

    assert [n.subtype for n in nodes] == ["cwe"] * 8
    by_id = {n.attrs["cwe_id"]: n for n in nodes}
    assert set(by_id) == {
        "CWE-79", "CWE-74", "CWE-502", "CWE-1004", "CWE-132",
        "CWE-310", "CWE-16", "CWE-1155",
    }
    # Categories are referenceable entries NVD still tags CVEs with
    # (legacy ids like CWE-310); omitting them dangles has_weakness edges.
    category = by_id["CWE-310"]
    assert category.attrs["kind"] == "category"
    assert category.attrs["abstraction"] is None
    assert category.attrs["description"].startswith("Weaknesses in this")
    assert by_id["CWE-79"].attrs["kind"] == "weakness"

    full = by_id["CWE-79"]
    assert full.node_id == "cwe:mitre:CWE-79"
    assert full.attrs["name"].startswith("Improper Neutralization")
    assert full.attrs["abstraction"] == "Base"
    # Whitespace-normalized first Description text.
    assert full.attrs["description"] == (
        "The product does not neutralize user-controllable input "
        "before it is placed in output."
    )
    # created_at promoted from the catalog Date attribute.
    assert full.attrs["created_at"] == "2023-10-26T00:00:00+00:00"
    # Description-less weakness: honest absence, no fabricated text.
    assert by_id["CWE-1004"].attrs["description"] is None
    assert run.record_set is not None
    assert set(run.record_set.records) == {
        "cwe:mitre:CWE-79", "cwe:mitre:CWE-74", "cwe:mitre:CWE-502",
        "cwe:mitre:CWE-1004", "cwe:mitre:CWE-310", "cwe:mitre:CWE-16",
        "cwe:mitre:CWE-132", "cwe:mitre:CWE-1155",
    }


def test_cwe_node_ids_match_nvd_edge_dst(tmp_path):
    """The cross-source contract: has_weakness edge dst ids land on
    the exact node ids the CWE catalog emits."""
    _, _, edges = _run_nvd(tmp_path, "xsrc", _nvd_11_doc())
    path = tmp_path / "cwec.xml"
    path.write_text(_CWE_XML)
    cwe_nodes = {
        f.node_id
        for f in CweCatalogSource(snapshot_path=path)
        .run("rid", mode="scope_complete").facts
        if isinstance(f, NodeFact)
    }
    assert "cwe:mitre:CWE-502" in {e.dst for e in edges}
    assert "cwe:mitre:CWE-502" in cwe_nodes


def test_cwe_zip_parses(tmp_path):
    path = tmp_path / "cwec_v4.13.xml.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("cwec_v4.13.xml", _CWE_XML)
    nodes = [
        f
        for f in CweCatalogSource(snapshot_path=path)
        .run("rid", mode="scope_complete").facts
        if isinstance(f, NodeFact)
    ]
    assert len(nodes) == 8  # 5 weaknesses + 3 categories


def test_cwe_malformed_xml_raises(tmp_path):
    path = tmp_path / "cwec.xml"
    path.write_text("<Weakness_Catalog><unclosed>")
    with pytest.raises(SubstrateError, match="not parseable"):
        CweCatalogSource(snapshot_path=path).run("rid", mode="scope_complete")


def test_cwe_wrong_root_raises(tmp_path):
    path = tmp_path / "other.xml"
    path.write_text("<NotACatalog/>")
    with pytest.raises(SubstrateError, match="Weakness_Catalog"):
        CweCatalogSource(snapshot_path=path).run("rid", mode="scope_complete")


def test_cwe_zero_weaknesses_raises(tmp_path):
    path = tmp_path / "cwec.xml"
    path.write_text(
        '<Weakness_Catalog xmlns="http://cwe.mitre.org/cwe-7" '
        'Date="2023-10-26"><Weaknesses/></Weakness_Catalog>'
    )
    with pytest.raises(SubstrateError, match="zero Weakness"):
        CweCatalogSource(snapshot_path=path).run("rid", mode="scope_complete")


# ---------- CWE hierarchy + CVSS metrics (req.cti2.weakness) ----------


def _run_cwe(tmp_path: Path, xml: str, name: str = "cwec.xml"):
    """Returns the SOURCE (for last_run_stats) plus the fact split."""
    path = tmp_path / name
    path.write_text(xml)
    source = CweCatalogSource(snapshot_path=path)
    facts = list(source.run("rid", mode="scope_complete").facts)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    return source, nodes, edges


def test_cwe_hierarchy_and_cvss_metrics(tmp_path):
    """req.cti2.weakness: the CWE hierarchy and the eight CVSS base
    metrics become queryable.

    ChildOf becomes child_of and every other nature becomes related_to
    (never the reverse); a relation pointing outside the catalog is
    skipped AND counted; a v3.1 vector decomposes into eight attributes
    that reassemble to the released string; a v2-only CVE and an
    unscored CVE leave all nine ABSENT.
    """
    source, nodes, edges = _run_cwe(tmp_path, _CWE_XML)
    by_id = {n.attrs["cwe_id"]: n for n in nodes}
    triples = {(e.src, e.edge_type, e.dst) for e in edges}

    # --- hierarchy, with the two natures kept apart ---
    assert ("cwe:mitre:CWE-79", "child_of", "cwe:mitre:CWE-74") in triples
    assert ("cwe:mitre:CWE-79", "related_to", "cwe:mitre:CWE-502") in triples
    # A PeerOf must NEVER read as an ancestor, in either direction.
    assert ("cwe:mitre:CWE-79", "child_of", "cwe:mitre:CWE-502") not in triples
    assert ("cwe:mitre:CWE-74", "child_of", "cwe:mitre:CWE-79") not in triples
    peer = next(
        e for e in edges
        if e.edge_type == "related_to" and e.dst == "cwe:mitre:CWE-502"
    )
    assert peer.attrs["nature"] == "PeerOf"

    # --- the dangling relation: skipped, counted, never a stub ---
    assert "cwe:mitre:CWE-9999" not in {n.node_id for n in nodes}
    assert "cwe:mitre:CWE-9999" not in {e.dst for e in edges}
    assert by_id["CWE-79"].attrs["unresolved_related_weaknesses"] == [
        "CWE-9999",
    ]
    stats = source.last_run_stats
    assert stats["related_weakness_unresolved"] == 1
    assert stats["related_weakness_natures"] == {"ChildOf": 3, "PeerOf": 1}
    assert stats["edges_by_type"] == {
        "child_of": 1, "related_to": 1, "has_member": 3,
    }
    # The same pair declared under two views is ONE edge, and the
    # collapse is counted rather than silently inflating the hierarchy.
    assert stats["related_weakness_collapsed_duplicates"] == 1

    # --- structured attributes ---
    assert by_id["CWE-79"].attrs["applicable_platforms"] == [
        "Language:Not Language-Specific", "Technology:Web Server",
    ]
    assert by_id["CWE-79"].attrs["consequence_scopes"] == [
        "Confidentiality", "Integrity",
    ]
    assert by_id["CWE-74"].attrs["applicable_platforms"] == []
    assert stats["consequence_blocks"] == 2

    # --- CVSS decomposition, and its round trip ---
    _, cve_nodes, _ = _run_nvd(tmp_path, "vsp", _nvd_11_doc())
    attrs = cve_nodes[0].attrs
    assert attrs["cvss_attack_vector"] == "NETWORK"
    assert attrs["cvss_attack_complexity"] == "LOW"
    assert attrs["cvss_privileges_required"] == "NONE"
    assert attrs["cvss_user_interaction"] == "NONE"
    assert attrs["cvss_scope"] == "CHANGED"
    assert attrs["cvss_confidentiality_impact"] == "HIGH"
    assert attrs["cvss_integrity_impact"] == "HIGH"
    assert attrs["cvss_availability_impact"] == "HIGH"
    assert attrs["cvss_base_severity"] == "CRITICAL"
    # The stored metrics reassemble to the RELEASED string — the source
    # raises on any CVE where they do not.
    assert _reassemble(attrs) == attrs["cvss_vector"] == _VECTOR

    # --- v2-only: all nine ABSENT, never defaulted ---
    v2_doc = _nvd_11_doc()
    v2_doc["CVE_Items"][0]["impact"] = {
        "baseMetricV2": {
            "cvssV2": {
                "version": "2.0",
                "vectorString": "AV:N/AC:L/Au:N/C:P/I:P/A:P",
                "baseScore": 7.5,
            },
            "severity": "HIGH",
        }
    }
    _, v2_nodes, _ = _run_nvd(tmp_path, "v2only", v2_doc)
    assert v2_nodes[0].attrs["cvss_version"] == "2.0"
    assert v2_nodes[0].attrs["cvss_vector"] == "AV:N/AC:L/Au:N/C:P/I:P/A:P"
    for attr in _CVSS_METRIC_ATTRS:
        assert attr not in v2_nodes[0].attrs

    # --- unscored: same, and the census says which case it was ---
    unscored_doc = _nvd_11_doc()
    unscored_doc["CVE_Items"][0]["impact"] = {}
    nvd = _nvd_source(tmp_path, "unscored-census", unscored_doc)
    unscored_nodes = [
        f for f in nvd.run("rid", mode="scope_complete").facts
        if isinstance(f, NodeFact)
    ]
    for attr in _CVSS_METRIC_ATTRS:
        assert attr not in unscored_nodes[0].attrs
    assert nvd.last_run_stats["cves_unscored"] == 1
    assert "cves_decomposed_v3.1" not in nvd.last_run_stats


def _reassemble(attrs: dict) -> str:
    """Rebuild the vector string from the eight stored metrics."""
    codes = {
        "NETWORK": "N", "ADJACENT_NETWORK": "A", "LOCAL": "L",
        "PHYSICAL": "P", "LOW": "L", "HIGH": "H", "NONE": "N",
        "REQUIRED": "R", "UNCHANGED": "U", "CHANGED": "C",
    }
    order = (
        ("AV", "cvss_attack_vector"),
        ("AC", "cvss_attack_complexity"),
        ("PR", "cvss_privileges_required"),
        ("UI", "cvss_user_interaction"),
        ("S", "cvss_scope"),
        ("C", "cvss_confidentiality_impact"),
        ("I", "cvss_integrity_impact"),
        ("A", "cvss_availability_impact"),
    )
    return "/".join(
        [f"CVSS:{attrs['cvss_version']}"]
        + [f"{abbrev}:{codes[attrs[attr]]}" for abbrev, attr in order]
    )


def test_cwe_dangling_only_hierarchy_keeps_nodes_and_counts(tmp_path):
    """Every Related_Weakness dangling: zero edges, zero stubs, and a
    count that says so."""
    xml = _CWE_XML.replace('CWE_ID="74"', 'CWE_ID="8888"').replace(
        'CWE_ID="502" View_ID="1000"', 'CWE_ID="7777" View_ID="1000"',
    )
    source, nodes, edges = _run_cwe(tmp_path, xml, name="dangling.xml")
    # Hierarchy only: category membership is a separate roster and is
    # unaffected by a dangling Related_Weakness.
    assert [
        e for e in edges if e.edge_type in {"child_of", "related_to"}
    ] == []
    assert {n.attrs["cwe_id"] for n in nodes} == {
        "CWE-79", "CWE-74", "CWE-502", "CWE-1004", "CWE-132",
        "CWE-310", "CWE-16", "CWE-1155",
    }
    # Three distinct unresolvable ids (the two ChildOf rows naming
    # CWE-8888 under different views collapse to one).
    assert source.last_run_stats["related_weakness_unresolved"] == 3
    citing = next(n for n in nodes if n.attrs["cwe_id"] == "CWE-79")
    assert citing.attrs["unresolved_related_weaknesses"] == [
        "CWE-8888", "CWE-7777", "CWE-9999",
    ]
    # "Everything I cited dangles" is NOT "I cited nothing": CWE-79
    # declared three relations, so it gets NO reason stamp even though
    # none of them resolved. That is the whole point of the split — a
    # node stranded by a resolution failure must stay unforgiven, and an
    # isolation probe demanding a stated reason will fire on it.
    assert "no_relations_reason" not in citing.attrs


def test_cwe_category_membership_emits_has_member(tmp_path):
    """A Category reaches the entries it groups.

    Category nodes exist because NVD tags CVEs with legacy category ids;
    without membership they are isolated nodes the corpus contract
    refuses. `has_member` is a THIRD edge type so a hierarchy walk can
    never read a thematic grouping as ancestry.
    """
    source, nodes, edges = _run_cwe(tmp_path, _CWE_XML, name="members.xml")
    by_id = {n.attrs["cwe_id"]: n for n in nodes}
    members = {
        (e.src, e.dst) for e in edges if e.edge_type == "has_member"
    }
    assert members == {
        ("cwe:mitre:CWE-310", "cwe:mitre:CWE-79"),
        ("cwe:mitre:CWE-310", "cwe:mitre:CWE-1004"),
        # A category may group another CATEGORY, which is why one
        # narrowing covers both member kinds.
        ("cwe:mitre:CWE-310", "cwe:mitre:CWE-16"),
    }
    # Direction is category -> member, never the reverse, and never
    # confusable with the hierarchy.
    assert ("cwe:mitre:CWE-79", "cwe:mitre:CWE-310") not in members
    assert "has_member" not in {
        e.edge_type for e in edges
        if (e.src, e.dst) == ("cwe:mitre:CWE-79", "cwe:mitre:CWE-74")
    }
    # The asserting view rides on the edge.
    member_edge = next(
        e for e in edges
        if e.edge_type == "has_member" and e.dst == "cwe:mitre:CWE-1004"
    )
    assert member_edge.attrs["view_id"] == "CWE-VIEW-1000"
    assert member_edge.source_record_id == {
        "cwe": "CWE-310", "member": "CWE-1004",
    }

    # A member the catalog does not define: skipped, counted, never a
    # stub node and never a dangling edge.
    assert "cwe:mitre:CWE-6666" not in {n.node_id for n in nodes}
    assert "cwe:mitre:CWE-6666" not in {e.dst for e in edges}
    assert by_id["CWE-310"].attrs["unresolved_members"] == ["CWE-6666"]
    assert by_id["CWE-16"].attrs["unresolved_members"] == []

    stats = source.last_run_stats
    assert stats["has_member_rows"] == 5
    assert stats["categories_with_members"] == 1
    assert stats["has_member_unresolved"] == 1
    assert stats["has_member_self_referential"] == 0
    # The same membership under two views is ONE edge, counted.
    assert stats["has_member_collapsed_duplicates"] == 1


def test_cwe_no_relations_reason_is_stamped_from_declarations(tmp_path):
    """The source states WHY an entry has no relations.

    An aggregate isolation ceiling passes whether the isolates are the
    ones the catalog genuinely strands or the ones a parsing regression
    dropped. Stamping the reason at the source lets a probe demand that
    every isolated node have a STATED one. The stamp reads the parsed
    declarations, never the emitted edges — reading emission would
    forgive exactly the regression this exists to catch.
    """
    source, nodes, _ = _run_cwe(tmp_path, _CWE_XML, name="reasons.xml")
    by_id = {n.attrs["cwe_id"]: n for n in nodes}

    # Retired: the catalog says Status="Deprecated" and strips the
    # entry's relationships, keeping the id for stability.
    assert by_id["CWE-132"].attrs["no_relations_reason"] == "retired_entry"
    # Live, but the catalog declares nothing for it — the CERT/SEI
    # mapping shells.
    assert by_id["CWE-1155"].attrs["no_relations_reason"] == (
        "no_relationships_declared"
    )

    # ABSENT wherever the catalog DOES declare a relation, in either
    # direction. This is what makes a missing stamp meaningful: an
    # isolated node without one lost an edge after declaration.
    for cwe_id in ("CWE-79", "CWE-310"):        # declare their own rows
        assert "no_relations_reason" not in by_id[cwe_id].attrs
    for cwe_id in ("CWE-74", "CWE-502", "CWE-1004", "CWE-16"):
        # Declare nothing themselves, but another entry names them —
        # by Related_Weakness (74, 502) or by Has_Member (1004, 16).
        assert "no_relations_reason" not in by_id[cwe_id].attrs

    assert source.last_run_stats["no_relations_reasons"] == {
        "retired_entry": 1,
        "no_relationships_declared": 1,
    }


def test_cwe_reason_stamp_survives_a_lost_edge(tmp_path):
    """The stamp must not be derivable from the emitted edges.

    Simulated regression: the ONLY declaration touching CWE-1004 is
    CWE-310's membership, and it is dropped from the emitted edges by
    dangling the citing category's target. CWE-1004 still declares
    nothing and is still cited by nobody... but CWE-310 now declares a
    dangling row, so IT stays unstamped and the probe fires on it.
    """
    xml = _CWE_XML.replace(
        '<Has_Member CWE_ID="1004" View_ID="1000"/>',
        '<Has_Member CWE_ID="4444" View_ID="1000"/>',
    )
    source, nodes, edges = _run_cwe(tmp_path, xml, name="lost-edge.xml")
    by_id = {n.attrs["cwe_id"]: n for n in nodes}

    # CWE-1004 is now genuinely unreferenced by any declaration, so the
    # catalog's own statement is what it says: nothing declares it.
    assert "cwe:mitre:CWE-1004" not in {e.dst for e in edges}
    assert by_id["CWE-1004"].attrs["no_relations_reason"] == (
        "no_relationships_declared"
    )
    # The citing category declared a relation that did not resolve, so
    # it carries the unresolved roster and NO reason stamp.
    assert by_id["CWE-310"].attrs["unresolved_members"] == [
        "CWE-4444", "CWE-6666",
    ]
    assert "no_relations_reason" not in by_id["CWE-310"].attrs
    assert source.last_run_stats["has_member_unresolved"] == 2


def test_cwe_categories_with_zero_resolved_members_raises(tmp_path):
    """Categories present but not one membership resolves: refused with
    the counts, because every category node would land isolated."""
    members = "\n".join(
        f'      <Category ID="{900 + n}" Name="Filler {n}">'
        f"<Summary>Filler.</Summary>"
        f'<Relationships><Has_Member CWE_ID="5{n:03d}" View_ID="1000"/>'
        f"</Relationships></Category>"
        for n in range(12)
    )
    xml = _CWE_XML.replace("  </Categories>", f"{members}\n  </Categories>")
    # Strip the one category whose members DO resolve.
    xml = xml.replace('<Has_Member CWE_ID="79" View_ID="1000"/>', "")
    xml = xml.replace('<Has_Member CWE_ID="79" View_ID="699"/>', "")
    xml = xml.replace('<Has_Member CWE_ID="1004" View_ID="1000"/>', "")
    xml = xml.replace('<Has_Member CWE_ID="16" View_ID="1000"/>', "")
    path = tmp_path / "no-members.xml"
    path.write_text(xml)
    with pytest.raises(
        SubstrateError, match=r"resolved ZERO Has_Member rows",
    ):
        CweCatalogSource(snapshot_path=path).run("rid", mode="scope_complete")


def test_cwe_has_member_without_target_raises(tmp_path):
    xml = _CWE_XML.replace(
        '<Has_Member CWE_ID="79" View_ID="1000"/>',
        '<Has_Member View_ID="1000"/>',
    )
    path = tmp_path / "no-member-id.xml"
    path.write_text(xml)
    with pytest.raises(SubstrateError, match="missing its CWE_ID"):
        CweCatalogSource(snapshot_path=path).run("rid", mode="scope_complete")


def test_cwe_related_weakness_without_nature_raises(tmp_path):
    xml = _CWE_XML.replace('Nature="PeerOf" ', "")
    path = tmp_path / "no-nature.xml"
    path.write_text(xml)
    with pytest.raises(SubstrateError, match="missing its Nature or CWE_ID"):
        CweCatalogSource(snapshot_path=path).run("rid", mode="scope_complete")


def test_nvd_lossy_cvss_vector_fails_naming_the_cve(tmp_path):
    """A vector that cannot round-trip fails the ingest with the CVE
    named — never a partially-decomposed node."""
    reordered = _nvd_11_doc()
    reordered["CVE_Items"][0]["impact"]["baseMetricV3"]["cvssV3"][
        "vectorString"
    ] = "CVSS:3.1/AC:L/AV:N/PR:N/UI:N/S:C/C:H/I:H/A:H"
    with pytest.raises(SubstrateError, match=r"CVE-2021-44228.*expected AV"):
        _run_nvd(tmp_path, "reordered", reordered)

    extra = _nvd_11_doc()
    extra["CVE_Items"][0]["impact"]["baseMetricV3"]["cvssV3"][
        "vectorString"
    ] = _VECTOR + "/E:H"
    with pytest.raises(
        SubstrateError, match=r"CVE-2021-44228.*exactly the 8 base metrics",
    ):
        _run_nvd(tmp_path, "extra", extra)


def test_nvd_v30_vector_decomposes(tmp_path):
    doc = _nvd_11_doc()
    block = doc["CVE_Items"][0]["impact"]["baseMetricV3"]["cvssV3"]
    block["version"] = "3.0"
    block["vectorString"] = "CVSS:3.0/AV:L/AC:H/PR:L/UI:R/S:U/C:L/I:N/A:N"
    nvd = _nvd_source(tmp_path, "v30", doc)
    nodes = [
        f for f in nvd.run("rid", mode="scope_complete").facts
        if isinstance(f, NodeFact)
    ]
    assert nodes[0].attrs["cvss_attack_vector"] == "LOCAL"
    assert nodes[0].attrs["cvss_user_interaction"] == "REQUIRED"
    assert nodes[0].attrs["cvss_availability_impact"] == "NONE"
    assert nvd.last_run_stats["cves_decomposed_v3.0"] == 1


# ---------- ATT&CK STIX ----------


def _attack_bundle() -> dict:
    return {
        "type": "bundle",
        "id": "bundle--0001",
        "objects": [
            {
                "type": "x-mitre-tactic",
                "id": "x-mitre-tactic--0004",
                "name": "Privilege Escalation",
                "x_mitre_shortname": "privilege-escalation",
                "created": "2018-10-17T00:14:20.652Z",
                "external_references": [
                    {"source_name": "mitre-attack", "external_id": "TA0004"},
                ],
            },
            {
                "type": "attack-pattern",
                "id": "attack-pattern--1548",
                "name": "Abuse Elevation Control Mechanism",
                "description": "D" * 5000,
                "created": "2020-01-30T13:58:14.373Z",
                "x_mitre_platforms": ["Linux", "macOS", "Windows"],
                "x_mitre_is_subtechnique": False,
                "kill_chain_phases": [
                    {
                        "kill_chain_name": "mitre-attack",
                        "phase_name": "privilege-escalation",
                    },
                ],
                "external_references": [
                    {"source_name": "mitre-attack", "external_id": "T1548"},
                ],
            },
            {
                "type": "attack-pattern",
                "id": "attack-pattern--1548-002",
                "name": "Bypass User Account Control",
                "description": "UAC bypass.",
                "created": "2020-01-30T14:11:41.212Z",
                "x_mitre_platforms": ["Windows"],
                "x_mitre_is_subtechnique": True,
                "kill_chain_phases": [
                    {
                        "kill_chain_name": "mitre-attack",
                        "phase_name": "privilege-escalation",
                    },
                ],
                "external_references": [
                    {
                        "source_name": "mitre-attack",
                        "external_id": "T1548.002",
                    },
                ],
            },
            {
                "type": "attack-pattern",
                "id": "attack-pattern--revoked",
                "name": "Old Revoked Technique",
                "revoked": True,
                "created": "2017-05-31T21:30:26.496Z",
                "external_references": [
                    {"source_name": "mitre-attack", "external_id": "T1088"},
                ],
            },
            {
                # revoked-by is an EXCLUSION SIGNAL, never an edge.
                "type": "relationship",
                "id": "relationship--revocation",
                "relationship_type": "revoked-by",
                "source_ref": "attack-pattern--revoked",
                "target_ref": "attack-pattern--1548",
            },
        ],
    }


def _run_attack(tmp_path: Path, doc: dict, name: str = "enterprise-attack"):
    """Returns the SOURCE (for last_run_stats) plus the fact split."""
    path = tmp_path / f"{name}.json"
    path.write_text(json.dumps(doc))
    source = AttackStixSource(snapshot_path=path)
    facts = list(source.run("rid", mode="scope_complete").facts)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    return source, nodes, edges


def test_attack_bundle_parses_techniques_tactics_and_part_of(tmp_path):
    _, nodes, edges = _run_attack(tmp_path, _attack_bundle())

    techniques = {
        n.attrs["technique_id"]: n
        for n in nodes if n.subtype == "attack_technique"
    }
    tactics = {
        n.attrs["tactic_id"]: n
        for n in nodes if n.subtype == "attack_tactic"
    }
    # The revoked T1088 was skipped.
    assert set(techniques) == {"T1548", "T1548.002"}
    assert set(tactics) == {"TA0004"}

    parent = techniques["T1548"]
    assert parent.node_id == "attack_technique:mitre:T1548"
    assert parent.attrs["is_subtechnique"] is False
    assert parent.attrs["platforms"] == "Linux,macOS,Windows"
    # The 2,000-char bound is gone: cti-ate matches report prose against
    # the FULL released description.
    assert len(parent.attrs["description"]) == 5000
    assert parent.attrs["created_at"] == "2020-01-30T13:58:14.373000+00:00"

    sub = techniques["T1548.002"]
    assert sub.attrs["is_subtechnique"] is True

    tactic = tactics["TA0004"]
    assert tactic.node_id == "attack_tactic:mitre:TA0004"
    assert tactic.attrs["shortname"] == "privilege-escalation"

    assert sorted((e.src, e.edge_type, e.dst) for e in edges) == [
        (
            "attack_technique:mitre:T1548",
            "part_of",
            "attack_tactic:mitre:TA0004",
        ),
        (
            "attack_technique:mitre:T1548.002",
            "part_of",
            "attack_tactic:mitre:TA0004",
        ),
    ]


def test_attack_non_bundle_raises(tmp_path):
    path = tmp_path / "objects.json"
    path.write_text(json.dumps({"objects": []}))
    with pytest.raises(SubstrateError, match="not a STIX bundle"):
        AttackStixSource(snapshot_path=path).run("rid", mode="scope_complete")


def test_attack_all_revoked_raises_with_skip_count(tmp_path):
    doc = _attack_bundle()
    doc["objects"] = [
        obj for obj in doc["objects"]
        if obj.get("id") in ("x-mitre-tactic--0004", "attack-pattern--revoked")
    ]
    path = tmp_path / "revoked-only.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(SubstrateError, match=r"zero live.*1 revoked"):
        AttackStixSource(snapshot_path=path).run("rid", mode="scope_complete")


def test_attack_phase_without_tactic_raises(tmp_path):
    doc = _attack_bundle()
    doc["objects"] = [
        obj for obj in doc["objects"]
        if obj.get("type") != "x-mitre-tactic"
    ]
    path = tmp_path / "no-tactics.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(SubstrateError, match="no tactic with that"):
        AttackStixSource(snapshot_path=path).run("rid", mode="scope_complete")


# ---------- ATT&CK as a graph (okg-corpus-cti, req.cti2.attack) ----------


def _entity(obj_type: str, stix_id: str, name: str, external_id: str, **extra):
    obj = {
        "type": obj_type,
        "id": stix_id,
        "name": name,
        "created": "2021-01-01T00:00:00.000Z",
        "external_references": [
            {"source_name": "mitre-attack", "external_id": external_id},
        ],
    }
    obj.update(extra)
    return obj


def _relationship(rel_id: str, rel_type: str, src: str, dst: str) -> dict:
    return {
        "type": "relationship",
        "id": rel_id,
        "relationship_type": rel_type,
        "source_ref": src,
        "target_ref": dst,
    }


def _attack_graph_bundle() -> dict:
    """The technique/tactic bundle plus every entity type the
    relationship graph points at, and every declared triple."""
    doc = _attack_bundle()
    doc["objects"].extend([
        _entity(
            "intrusion-set", "intrusion-set--apt29", "APT29", "G0016",
            aliases=["APT29", "Cozy Bear", "Midnight Blizzard"],
            description="A Russian state-sponsored group.",
        ),
        _entity(
            "intrusion-set", "intrusion-set--gone", "Retired Group", "G0999",
            revoked=True,
        ),
        _entity(
            "intrusion-set", "intrusion-set--superseded",
            "Superseded Group", "G0998",
        ),
        _entity(
            "malware", "malware--mimikatz", "Mimikatz", "S0002",
            x_mitre_aliases=["Mimikatz", "mimikatz.exe"],
            x_mitre_platforms=["Windows"],
        ),
        _entity(
            "tool", "tool--wce", "Windows Credential Editor", "S0005",
            x_mitre_platforms=["Windows"],
        ),
        _entity(
            "course-of-action", "course-of-action--m1013",
            "Application Developer Guidance", "M1013",
        ),
        _entity(
            "course-of-action", "course-of-action--legacy",
            "Deprecated Mitigation", "M1999",
            x_mitre_deprecated=True,
        ),
        _entity(
            "campaign", "campaign--c0001", "Frankenstein", "C0001",
            aliases=["Frankenstein"],
        ),
        {
            # Data components are keyed by the uuid half of the STIX id
            # (the one key every bundle vintage carries).
            "type": "x-mitre-data-component",
            "id": "x-mitre-data-component--0001-proc",
            "name": "Process Creation",
            "created": "2021-01-01T00:00:00.000Z",
            "description": "Birth of a new running process.",
        },
        _entity(
            # Modern ATT&CK's detection entity: it DOES carry a mitre
            # external id, and it — not the data component — is the
            # source of `detects` in every current release. It ships no
            # description; the prose lives on the analytics it
            # references through x_mitre_analytic_refs.
            "x-mitre-detection-strategy",
            "x-mitre-detection-strategy--det0237",
            "Detection Strategy for Abuse Elevation Control Mechanism",
            "DET0237",
            x_mitre_analytic_refs=[
                "x-mitre-analytic--an0110",
                # A ref the bundle never defines: skipped AND counted,
                # never a stub node.
                "x-mitre-analytic--nowhere",
            ],
        ),
        _entity(
            # The detection logic itself. ATT&CK links it to its
            # strategy and to its data components with EMBEDDED refs, so
            # reading only the relationship objects would leave the data
            # component isolated.
            "x-mitre-analytic", "x-mitre-analytic--an0110",
            "Analytic 0110", "AN0110",
            description="Monitor auditd for unusual kinit activity.",
            x_mitre_platforms=["Linux"],
            x_mitre_log_source_references=[
                {
                    "x_mitre_data_component_ref":
                        "x-mitre-data-component--0001-proc",
                    "name": "auditd:SYSCALL",
                    "channel": "socket/connect",
                },
                {
                    # The SAME component named by a second log source:
                    # one graph edge, the repeat counted.
                    "x_mitre_data_component_ref":
                        "x-mitre-data-component--0001-proc",
                    "name": "linux:syslog",
                    "channel": "kinit",
                },
                {
                    # An entry carrying no ref at all: counted malformed.
                    "name": "WinEventLog:Security",
                    "channel": "EventCode=4768",
                },
            ],
        ),
        _relationship(
            "relationship--ds-detects-t", "detects",
            "x-mitre-detection-strategy--det0237", "attack-pattern--1548-002",
        ),
        _relationship(
            "relationship--g-uses-t", "uses",
            "intrusion-set--apt29", "attack-pattern--1548",
        ),
        _relationship(
            # ATT&CK publishes one relationship object per citation, so
            # the same pair recurs; the graph edge is ONE edge.
            "relationship--g-uses-t-again", "uses",
            "intrusion-set--apt29", "attack-pattern--1548",
        ),
        _relationship(
            "relationship--s-uses-t", "uses",
            "malware--mimikatz", "attack-pattern--1548-002",
        ),
        _relationship(
            "relationship--g-uses-s", "uses",
            "intrusion-set--apt29", "malware--mimikatz",
        ),
        _relationship(
            "relationship--c-uses-t", "uses",
            "campaign--c0001", "attack-pattern--1548",
        ),
        _relationship(
            "relationship--c-uses-s", "uses",
            "campaign--c0001", "tool--wce",
        ),
        _relationship(
            "relationship--m-mitigates-t", "mitigates",
            "course-of-action--m1013", "attack-pattern--1548",
        ),
        _relationship(
            "relationship--d-detects-t", "detects",
            "x-mitre-data-component--0001-proc", "attack-pattern--1548",
        ),
        _relationship(
            "relationship--sub", "subtechnique-of",
            "attack-pattern--1548-002", "attack-pattern--1548",
        ),
        _relationship(
            "relationship--attrib", "attributed-to",
            "campaign--c0001", "intrusion-set--apt29",
        ),
        _relationship(
            # Endpoint revoked: dropped AND counted.
            "relationship--revoked-endpoint", "uses",
            "intrusion-set--gone", "attack-pattern--1548",
        ),
        _relationship(
            # Endpoint the bundle never defines: dropped AND counted.
            "relationship--absent-endpoint", "uses",
            "intrusion-set--apt29", "attack-pattern--nowhere",
        ),
        _relationship(
            # Exclusion signal only: G0998 is superseded, so it emits no
            # node and never appears as an endpoint.
            "relationship--supersede", "revoked-by",
            "intrusion-set--superseded", "intrusion-set--apt29",
        ),
        _relationship(
            # A relationship type this adapter does not emit: counted by
            # type, never half-emitted.
            "relationship--related", "related-to",
            "intrusion-set--apt29", "campaign--c0001",
        ),
    ])
    return doc


def test_attack_entities_and_relationships(tmp_path):
    """req.cti2.attack: the bundle is a GRAPH.

    Every new entity subtype lands with its aliases, every one of the
    ten declared triples is emitted in the right direction, revoked and
    superseded objects stay out, and relationships touching a filtered
    or absent endpoint are dropped AND counted.
    """
    source, nodes, edges = _run_attack(tmp_path, _attack_graph_bundle())
    by_subtype: dict[str, list[NodeFact]] = {}
    for node in nodes:
        by_subtype.setdefault(node.subtype, []).append(node)

    # --- entities ---
    group = by_subtype["attack_group"][0]
    assert [n.attrs["group_id"] for n in by_subtype["attack_group"]] == ["G0016"]
    assert group.node_id == "attack_group:mitre:G0016"
    # Aliases verbatim: report prose names groups this way, and cti-ate
    # has no other way in.
    assert group.attrs["aliases"] == [
        "APT29", "Cozy Bear", "Midnight Blizzard",
    ]

    software = {n.attrs["software_id"]: n for n in by_subtype["attack_software"]}
    assert set(software) == {"S0002", "S0005"}
    # malware and tool are ONE subtype carrying which one it was.
    assert software["S0002"].attrs["software_type"] == "malware"
    assert software["S0005"].attrs["software_type"] == "tool"
    assert software["S0002"].attrs["aliases"] == ["Mimikatz", "mimikatz.exe"]
    assert software["S0002"].attrs["platforms"] == "Windows"

    assert [
        n.attrs["mitigation_id"] for n in by_subtype["attack_mitigation"]
    ] == ["M1013"]
    campaign = by_subtype["attack_campaign"][0]
    assert campaign.attrs["campaign_id"] == "C0001"
    assert campaign.attrs["aliases"] == ["Frankenstein"]
    component = by_subtype["attack_data_component"][0]
    assert component.node_id == "attack_data_component:mitre:0001-proc"
    assert component.attrs["name"] == "Process Creation"
    strategy = by_subtype["attack_detection_strategy"][0]
    assert strategy.node_id == "attack_detection_strategy:mitre:DET0237"
    assert strategy.attrs["detection_strategy_id"] == "DET0237"

    # --- the ten declared triples, by direction ---
    triples = {
        (
            e.src.split(":")[0], e.edge_type, e.dst.split(":")[0],
        )
        for e in edges
    }
    assert triples == {
        ("attack_technique", "part_of", "attack_tactic"),
        ("attack_group", "uses", "attack_technique"),
        ("attack_software", "uses", "attack_technique"),
        ("attack_group", "uses", "attack_software"),
        ("attack_campaign", "uses", "attack_technique"),
        ("attack_campaign", "uses", "attack_software"),
        ("attack_mitigation", "mitigates", "attack_technique"),
        ("attack_data_component", "detects", "attack_technique"),
        ("attack_detection_strategy", "detects", "attack_technique"),
        ("attack_technique", "subtechnique_of", "attack_technique"),
        ("attack_campaign", "attributed_to", "attack_group"),
        ("attack_detection_strategy", "contains", "attack_analytic"),
        ("attack_analytic", "references", "attack_data_component"),
    }
    edge_keys = {(e.src, e.edge_type, e.dst) for e in edges}
    assert (
        "attack_technique:mitre:T1548.002",
        "subtechnique_of",
        "attack_technique:mitre:T1548",
    ) in edge_keys
    # ... and NOT the reverse direction.
    assert (
        "attack_technique:mitre:T1548",
        "subtechnique_of",
        "attack_technique:mitre:T1548.002",
    ) not in edge_keys

    # --- exclusions, every one of them counted ---
    revoked_group_ids = {"attack_group:mitre:G0999", "attack_group:mitre:G0998"}
    assert revoked_group_ids & {n.node_id for n in nodes} == set()
    assert "attack_mitigation:mitre:M1999" not in {n.node_id for n in nodes}
    stats = source.last_run_stats
    assert stats["filtered_objects_by_type"] == {
        "attack-pattern": 1,        # the revoked technique
        "intrusion-set": 2,         # revoked + superseded
        "course-of-action": 1,      # deprecated
    }
    assert stats["filtered_objects_total"] == 4
    assert stats["revoked_by_exclusions"] == 2
    assert stats["relationships_dropped_filtered_endpoint"] == 1
    assert stats["relationships_dropped_absent_endpoint"] == 1
    assert stats["relationships_duplicate_skipped"] == 1
    # revoked-by is consumed as an exclusion signal, NEVER as an edge.
    assert "revoked_by" not in {e.edge_type for e in edges}
    assert stats["relationship_objects_by_type"]["revoked-by"] == 2
    assert stats["relationship_objects_by_type"]["related-to"] == 1
    assert stats["edges_by_triple"] == {
        "attack_analytic references attack_data_component": 1,
        "attack_campaign attributed_to attack_group": 1,
        "attack_campaign uses attack_software": 1,
        "attack_campaign uses attack_technique": 1,
        "attack_data_component detects attack_technique": 1,
        "attack_detection_strategy contains attack_analytic": 1,
        "attack_detection_strategy detects attack_technique": 1,
        "attack_group uses attack_software": 1,
        "attack_group uses attack_technique": 1,
        "attack_mitigation mitigates attack_technique": 1,
        "attack_software uses attack_technique": 1,
        "attack_technique subtechnique_of attack_technique": 1,
    }
    assert stats["nodes_by_subtype"] == {
        "attack_tactic": 1,
        "attack_technique": 2,
        "attack_group": 1,
        "attack_software": 2,
        "attack_mitigation": 1,
        "attack_campaign": 1,
        "attack_detection_strategy": 1,
        "attack_analytic": 1,
        "attack_data_component": 1,
    }
    # Embedded refs get the relationship pass's contract: an unresolved
    # ref and a ref-less log-source entry are both counted, and the
    # component named twice becomes ONE edge.
    assert stats["embedded_ref_candidates"] == 4
    assert stats["embedded_refs_dropped_absent_endpoint"] == 1
    assert stats["embedded_refs_dropped_filtered_endpoint"] == 0
    assert stats["embedded_refs_duplicate_skipped"] == 1
    assert stats["embedded_refs_malformed"] == 1
    assert stats["embedded_ref_edges"] == 2
    # The two edge families are counted apart even though they share one
    # edges_by_triple roster: ten relationship-derived, two embedded.
    assert stats["relationship_edges"] == 10


def test_attack_technique_carries_detection_text(tmp_path):
    doc = _attack_graph_bundle()
    for obj in doc["objects"]:
        if obj.get("id") == "attack-pattern--1548":
            obj["x_mitre_detection"] = "Monitor for unusual UAC prompts."
    _, nodes, _ = _run_attack(tmp_path, doc, name="detection")
    technique = next(
        n for n in nodes if n.attrs.get("technique_id") == "T1548"
    )
    assert technique.attrs["detection"] == "Monitor for unusual UAC prompts."
    sub = next(n for n in nodes if n.attrs.get("technique_id") == "T1548.002")
    # Absent detection is an honest null, never a fabricated string.
    assert sub.attrs["detection"] is None


def test_attack_detection_strategy_node_and_detects_edge(tmp_path):
    """req.cti2.attack, corrected against the released bundle: `detects`
    starts at an x-mitre-detection-strategy, so that object is a node
    and the edge lands on the technique it names."""
    source, nodes, edges = _run_attack(
        tmp_path, _attack_graph_bundle(), name="detection-strategy",
    )
    strategy = next(
        n for n in nodes if n.subtype == "attack_detection_strategy"
    )
    # Keyed by its ATT&CK id, like every sibling that publishes one —
    # NOT by the STIX uuid the data component falls back to.
    assert strategy.node_id == "attack_detection_strategy:mitre:DET0237"
    assert strategy.attrs["detection_strategy_id"] == "DET0237"
    assert strategy.attrs["name"] == (
        "Detection Strategy for Abuse Elevation Control Mechanism"
    )
    # ATT&CK ships no description on these: honest null, not "".
    assert strategy.attrs["description"] is None
    assert strategy.attrs["created_at"] is not None
    assert strategy.source_record_id == {
        "attack_detection_strategy": "DET0237",
    }

    assert (
        "attack_detection_strategy:mitre:DET0237",
        "detects",
        "attack_technique:mitre:T1548.002",
    ) in {(e.src, e.edge_type, e.dst) for e in edges}
    assert source.last_run_stats["edges_by_triple"][
        "attack_detection_strategy detects attack_technique"
    ] == 1


def test_attack_analytic_layer_connects_data_components(tmp_path):
    """The detection layer ATT&CK ships as EMBEDDED refs.

    A strategy contains its analytics, an analytic references the data
    components its log sources come from, and those two hops are the
    only thing that keeps a data component reachable in a current
    bundle.
    """
    source, nodes, edges = _run_attack(
        tmp_path, _attack_graph_bundle(), name="analytics",
    )
    analytic = next(n for n in nodes if n.subtype == "attack_analytic")
    assert analytic.node_id == "attack_analytic:mitre:AN0110"
    assert analytic.attrs["analytic_id"] == "AN0110"
    assert analytic.attrs["platforms"] == "Linux"
    # The detection prose that used to live on the technique as
    # x_mitre_detection.
    assert analytic.attrs["description"] == (
        "Monitor auditd for unusual kinit activity."
    )

    edge_keys = {(e.src, e.edge_type, e.dst) for e in edges}
    assert (
        "attack_detection_strategy:mitre:DET0237",
        "contains",
        "attack_analytic:mitre:AN0110",
    ) in edge_keys
    assert (
        "attack_analytic:mitre:AN0110",
        "references",
        "attack_data_component:mitre:0001-proc",
    ) in edge_keys

    # The whole point: the data component is no longer isolated, and it
    # is reached through the analytic rather than by a `detects` edge
    # this release does not publish.
    touched = {e.src for e in edges} | {e.dst for e in edges}
    assert "attack_data_component:mitre:0001-proc" in touched

    # An embedded edge traces back to the two STIX objects that asserted
    # it — there is no relationship object to name.
    contains_edge = next(e for e in edges if e.edge_type == "contains")
    assert contains_edge.source_record_id == {
        "detection_strategy": "x-mitre-detection-strategy--det0237",
        "analytic": "x-mitre-analytic--an0110",
    }


def _data_component(stix_id: str, name: str) -> dict:
    return {
        "type": "x-mitre-data-component",
        "id": stix_id,
        "name": name,
        "created": "2021-01-01T00:00:00.000Z",
        "description": f"{name} telemetry.",
    }


def _stranding_bundle() -> dict:
    """The graph bundle plus the two ways a data component legitimately
    strands: nothing names it, and only an EXCLUDED object names it."""
    doc = _attack_graph_bundle()
    doc["objects"].extend([
        # (a) no relationship, no embedded ref, no kill-chain phase names
        #     it. 8 of 106 in enterprise-attack 2026-07-27 are in this
        #     state.
        _data_component("x-mitre-data-component--0002-file", "File Access"),
        # (b) named ONLY by a deprecated analytic, so the edge drops with
        #     its endpoint rather than being lost.
        _data_component("x-mitre-data-component--0003-net", "Network Flow"),
        _entity(
            "x-mitre-analytic", "x-mitre-analytic--an0999",
            "Retired Analytic", "AN0999",
            x_mitre_deprecated=True,
            x_mitre_log_source_references=[
                {"x_mitre_data_component_ref":
                    "x-mitre-data-component--0003-net"},
            ],
        ),
    ])
    return doc


def test_attack_stamps_why_a_node_relates_to_nothing(tmp_path):
    """req.cti2.contract: an isolate must carry a source-written reason.

    The cti corpus contract used to forgive isolated data components with
    a hardcoded `AND n.subtype <> 'attack_data_component'` — a
    subtype-wide allow that exempts a regression-stranded component
    exactly as readily as an upstream one, in the same probe whose own
    comment calls the per-node rule "strictly stronger than a subtype-wide
    allow". Measured on the 2026-07-28 cti generation, all 8 isolated
    data components carried NEITHER `no_relations_reason` NOR
    `weakness_mapping_status`.

    The stamp is read from a census of the BUNDLE's declarations taken
    before any node is emitted, so a regression that drops an edge leaves
    an unstamped isolate and the probe fires. Measured on the complement
    below: stamped exactly when isolated, over every ATT&CK subtype.
    """
    source, nodes, edges = _run_attack(
        tmp_path, _stranding_bundle(), name="stranding",
    )
    by_node_id = {n.node_id: n for n in nodes}

    assert by_node_id[
        "attack_data_component:mitre:0002-file"
    ].attrs["no_relations_reason"] == "unreferenced_in_bundle"
    assert by_node_id[
        "attack_data_component:mitre:0003-net"
    ].attrs["no_relations_reason"] == "referenced_only_by_excluded_objects"
    # The two states are NOT the same claim: "nobody named it" and "only a
    # deprecated analytic named it" are different facts about the bundle,
    # and collapsing them would destroy the distinction before a consumer
    # can use it.
    assert source.last_run_stats["no_relations_reasons"] == {
        "referenced_only_by_excluded_objects": 1,
        "unreferenced_in_bundle": 1,
    }
    assert source.last_run_stats["no_relations_by_subtype"] == {
        "attack_data_component": {
            "referenced_only_by_excluded_objects": 1,
            "unreferenced_in_bundle": 1,
        },
    }

    # THE COMPLEMENT, over every emitted node. An unstamped isolate is a
    # parsing regression; a stamped-but-connected node would mean the
    # stamp forgives something the bundle did in fact relate.
    connected = {e.src for e in edges} | {e.dst for e in edges}
    isolated = {n.node_id for n in nodes if n.node_id not in connected}
    stamped = {
        n.node_id for n in nodes if "no_relations_reason" in n.attrs
    }
    assert stamped == isolated == {
        "attack_data_component:mitre:0002-file",
        "attack_data_component:mitre:0003-net",
    }
    # ...and the complement is large and non-empty: every other node,
    # including the data component an analytic DOES reference, is
    # connected and unstamped.
    assert len(nodes) - len(stamped) >= 10
    assert "no_relations_reason" not in by_node_id[
        "attack_data_component:mitre:0001-proc"
    ].attrs


def test_attack_stamp_survives_a_lost_embedded_ref(tmp_path, monkeypatch):
    """The stamp must not be derivable from the emitted edges.

    Simulated regression: the embedded-ref emission pass is suppressed
    entirely while the BUNDLE is left intact, so the analytic layer's
    edges vanish without a single declaration changing. The data
    component the analytic still references, and the analytic the
    strategy still names, both land isolated and both stay UNSTAMPED —
    so an isolation probe fires on them. A stamp derived from emission
    would have marked all four isolates "legitimately stranded" and
    forgiven the regression.
    """
    doc = _stranding_bundle()
    # Without this, 0001-proc keeps a `detects` relationship edge and
    # would not be isolated at all; the analytic ref becomes its only
    # DECLARATION, which is what the census must still see.
    doc["objects"] = [
        obj for obj in doc["objects"]
        if obj.get("id") != "relationship--d-detects-t"
    ]
    monkeypatch.setattr(
        AttackStixSource, "_embedded_edges",
        lambda self, by_type, **kw: [],
    )
    _source, nodes, edges = _run_attack(tmp_path, doc, name="lost-ref")

    connected = {e.src for e in edges} | {e.dst for e in edges}
    isolated = {n.node_id for n in nodes if n.node_id not in connected}
    stamped = {n.node_id for n in nodes if "no_relations_reason" in n.attrs}
    # The two upstream-stranded components are still the ONLY stamped
    # nodes: the census read the bundle, not the fact list.
    assert stamped == {
        "attack_data_component:mitre:0002-file",
        "attack_data_component:mitre:0003-net",
    }
    # The regression's victims are isolated AND unstamped, which is the
    # state a per-node isolation probe refuses.
    assert isolated - stamped == {
        "attack_data_component:mitre:0001-proc",
        "attack_analytic:mitre:AN0110",
    }


def test_attack_unmodeled_embedded_ref_pair_raises(tmp_path):
    """An embedded ref is held to the SAME declared-triple guard as a
    relationship object: an unmodeled pair fails loudly instead of
    resolving to something the ontology cannot express."""
    doc = _attack_graph_bundle()
    for obj in doc["objects"]:
        if obj.get("id") == "x-mitre-detection-strategy--det0237":
            obj["x_mitre_analytic_refs"] = ["campaign--c0001"]
    path = tmp_path / "unmodeled-embedded.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(
        SubstrateError,
        match=r"attack_detection_strategy -contains-> attack_campaign, "
              r"which matches no declared triple",
    ):
        AttackStixSource(snapshot_path=path).run("rid", mode="scope_complete")


def test_attack_revoked_detection_strategy_is_filtered_and_counted(tmp_path):
    """The revoked/deprecated exclusion covers the new subtype too: the
    node stays out, its `detects` edge is dropped, and both are
    counted."""
    doc = _attack_graph_bundle()
    for obj in doc["objects"]:
        if obj.get("id") == "x-mitre-detection-strategy--det0237":
            obj["x_mitre_deprecated"] = True
    source, nodes, edges = _run_attack(tmp_path, doc, name="ds-deprecated")
    assert "attack_detection_strategy" not in {n.subtype for n in nodes}
    assert "attack_detection_strategy detects attack_technique" not in (
        source.last_run_stats["edges_by_triple"]
    )
    assert source.last_run_stats["filtered_objects_by_type"][
        "x-mitre-detection-strategy"
    ] == 1
    # The data-component `detects` still resolves, so the triple stays
    # supported for a pre-detection-strategy bundle.
    assert source.last_run_stats["edges_by_triple"][
        "attack_data_component detects attack_technique"
    ] == 1
    assert source.last_run_stats[
        "relationships_dropped_filtered_endpoint"
    ] == 2
    assert edges


def test_attack_unknown_detects_pair_raises(tmp_path):
    """The guard still fails loudly on an unmodeled `detects` pair —
    this is the check that caught the detection-strategy gap on the real
    bundle, and adding the subtype must not weaken it."""
    doc = _attack_graph_bundle()
    doc["objects"].append(_relationship(
        "relationship--unmodeled-detects", "detects",
        "campaign--c0001", "attack-pattern--1548",
    ))
    path = tmp_path / "unmodeled-detects.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(
        SubstrateError,
        match=r"attack_campaign -detects-> attack_technique matches no "
              r"declared triple",
    ):
        AttackStixSource(snapshot_path=path).run("rid", mode="scope_complete")


def test_attack_unknown_uses_pair_raises(tmp_path):
    """A `uses` whose endpoint pair matches no declared triple must FAIL
    LOUDLY — silently dropping it is the defect this adapter was
    rewritten to remove."""
    doc = _attack_graph_bundle()
    doc["objects"].append(_relationship(
        "relationship--unmodeled", "uses",
        "course-of-action--m1013", "attack-pattern--1548",
    ))
    path = tmp_path / "unmodeled.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(
        SubstrateError,
        match=r"attack_mitigation -uses-> attack_technique matches no "
              r"declared triple",
    ):
        AttackStixSource(snapshot_path=path).run("rid", mode="scope_complete")


def test_attack_relationship_without_endpoints_raises(tmp_path):
    doc = _attack_graph_bundle()
    doc["objects"].append({
        "type": "relationship",
        "id": "relationship--headless",
        "relationship_type": "uses",
    })
    path = tmp_path / "headless.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(SubstrateError, match="missing source_ref/target_ref"):
        AttackStixSource(snapshot_path=path).run("rid", mode="scope_complete")


def test_attack_all_relationships_dropped_raises(tmp_path):
    """Every relationship endpoint revoked: zero edges is refused with
    the drop counts, so "no matches" cannot masquerade as "none
    exist"."""
    doc = _attack_graph_bundle()
    revoked = {
        "intrusion-set--apt29", "malware--mimikatz", "tool--wce",
        "campaign--c0001", "course-of-action--m1013",
        "x-mitre-data-component--0001-proc", "attack-pattern--1548-002",
        "x-mitre-detection-strategy--det0237",
    }
    for obj in doc["objects"]:
        if obj.get("id") in revoked:
            obj["revoked"] = True
    path = tmp_path / "all-dropped.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(SubstrateError, match=r"resolved ZERO of them"):
        AttackStixSource(snapshot_path=path).run("rid", mode="scope_complete")


# ---------- cti module composition ----------


def test_cti_module_composes(tmp_path):
    """A deployment manifest listing module 'cti' composes a catalog
    admitting exactly the shapes the three sources emit (same harness
    as test_catalog_consistency_module)."""
    deployment = tmp_path / "scratch"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {"name": "scratch", "schema_dir": "schemas", "modules": ["cti"]}
        )
    )
    (deployment / "schemas").mkdir()

    composed = compose_catalog(deployment)

    for subtype in (
        "cve", "cwe", "attack_technique", "attack_tactic", "attack_group",
        "attack_software", "attack_mitigation", "attack_campaign",
        "attack_detection_strategy", "attack_analytic",
        "attack_data_component",
    ):
        assert subtype in composed.subtypes, sorted(composed.subtypes)
        assert composed.subtype_origin[subtype] == "cti"

    triples = composed.narrowing_triples()
    assert set(triples) >= {
        ("cve", "has_weakness", "cwe"),
        ("cwe", "child_of", "cwe"),
        ("cwe", "related_to", "cwe"),
        ("cwe", "has_member", "cwe"),
        ("attack_technique", "part_of", "attack_tactic"),
        ("attack_technique", "subtechnique_of", "attack_technique"),
        ("attack_group", "uses", "attack_technique"),
        ("attack_group", "uses", "attack_software"),
        ("attack_software", "uses", "attack_technique"),
        ("attack_campaign", "uses", "attack_technique"),
        ("attack_campaign", "uses", "attack_software"),
        ("attack_campaign", "attributed_to", "attack_group"),
        ("attack_mitigation", "mitigates", "attack_technique"),
        ("attack_data_component", "detects", "attack_technique"),
        ("attack_detection_strategy", "detects", "attack_technique"),
        ("attack_detection_strategy", "contains", "attack_analytic"),
        ("attack_analytic", "references", "attack_data_component"),
    }
    # Exactly the seventeen narrowings the sources emit — no stray
    # triples, which is what lets attack_stix raise on an unmodeled pair.
    cti_rows = [r for r in composed.narrowings if r.origin == "cti"]
    assert len(cti_rows) == 17

    # The ATT&CK narrowings are EXACTLY the adapter's declared triples
    # (plus the kill-chain-derived part_of, which comes from a technique
    # field rather than a relationship object). Asserted as equality so
    # a triple added to one side and not the other fails here instead of
    # at insert time on a real bundle.
    assert {t for t in triples if t[0].startswith("attack_")} == (
        set(_DECLARED_TRIPLES)
        | {("attack_technique", "part_of", "attack_tactic")}
    )

    # created_at promotion annotations harvested into subtype metadata.
    assert composed.subtype_metadata["cve"]["created_at_from"] == "published_at"
    assert composed.subtype_metadata["cwe"]["created_at_from"] == "created_at"
    assert (
        composed.subtype_metadata["attack_technique"]["created_at_from"]
        == "created_at"
    )
