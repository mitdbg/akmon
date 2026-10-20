"""Source adapter extension tests for add-substrate-reasoning-module.

Verifies that the substrate openspec source adapter walks the new
content kinds (openspec/hypotheses/, openspec/observations/,
openspec/insights/), emits the right NodeSpecs + EdgeSpecs, handles
unknown schema_version values loudly, and emits the cross-link
`Hypothesis supported_by AcceptanceCriterion` edges when a change
declares `hypotheses_tested:` in frontmatter.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from okg.substrate.library.sources._openspec_emit import OpenSpecSource

_FIXTURE = Path(__file__).parent / "fixtures" / "openspec_tree"


@pytest.fixture
def openspec_root(tmp_path: Path) -> Path:
    """Copy the fixture tree into tmp_path so tests can mutate it
    without affecting the checked-in fixture.
    """
    root = tmp_path / "openspec"
    shutil.copytree(_FIXTURE, root)
    return root


def _run(openspec_root: Path) -> OpenSpecSource:
    src = OpenSpecSource(
        openspec_root=openspec_root,
        repo="okg-test",
        include_archived=True,
    )
    list(src.nodes())
    list(src.edges())
    return src


def test_emits_hypothesis_node(openspec_root: Path) -> None:
    src = _run(openspec_root)
    hyps = [n for n in src._emit_nodes or () if n.subtype == "hypothesis"]
    assert any(n.node_id == "hyp:test-hyp-1" for n in hyps), (
        "expected hyp:test-hyp-1 node to be emitted"
    )
    node = next(n for n in hyps if n.node_id == "hyp:test-hyp-1")
    assert node.attrs["hypothesis_id"] == "test-hyp-1"
    assert node.attrs["statement"] == "Test hypothesis number one"
    assert node.attrs["status"] == "open"
    assert node.attrs["source_subtype"] == "openspec_hypothesis"
    assert node.attrs["source_path"].endswith("/hypotheses/test-hyp-1/")
    assert node.attrs["frontmatter_schema_version"] == 1
    assert node.attrs["domain"] == "testing"
    assert node.attrs["confidence"] == 0.5
    assert "okg-substrate" in node.attrs.get("related_projects", "")
    assert (
        "Body of the test hypothesis" in node.attrs.get("text", "")
        or "body of the test hypothesis" in node.attrs.get("text", "").lower()
    )


def test_emits_observation_node(openspec_root: Path) -> None:
    src = _run(openspec_root)
    obs = [n for n in src._emit_nodes or () if n.subtype == "observation"]
    assert any(n.node_id == "obs:test-obs-1" for n in obs)
    node = next(n for n in obs if n.node_id == "obs:test-obs-1")
    assert node.attrs["source_kind"] == "paper"
    assert node.attrs["source_ref"] == "10.5555/test-doi"
    assert node.attrs["measured_value"] == "42"
    assert node.attrs["observed_at"] == "2026-05-17"


def test_emits_insight_node(openspec_root: Path) -> None:
    src = _run(openspec_root)
    insights = [n for n in src._emit_nodes or () if n.subtype == "insight"]
    assert any(n.node_id == "insight:test-insight-1" for n in insights)
    node = next(n for n in insights if n.node_id == "insight:test-insight-1")
    assert "test-connection-1" in node.attrs.get("synthesizes_connection_refs", "")
    assert "test-connection-2" in node.attrs.get("synthesizes_connection_refs", "")


def test_emits_hypothesis_cites_principle_edges(
    openspec_root: Path,
) -> None:
    src = _run(openspec_root)
    edges = src._emit_edges or []
    cites = [
        e
        for e in edges
        if e.src == "hyp:test-hyp-1"
        and e.edge_type == "cites"
        and e.dst == "princ:test-principle-1"
    ]
    assert cites, "expected hyp:test-hyp-1 cites princ:test-principle-1"


def test_emits_principle_predicts_hypothesis_edges(
    openspec_root: Path,
) -> None:
    src = _run(openspec_root)
    edges = src._emit_edges or []
    predicts = [
        e
        for e in edges
        if e.src == "princ:test-principle-2"
        and e.edge_type == "predicts"
        and e.dst == "hyp:test-hyp-1"
    ]
    assert predicts, "expected princ:test-principle-2 predicts hyp:test-hyp-1"


def test_emits_observation_supports_hypothesis_edges(
    openspec_root: Path,
) -> None:
    src = _run(openspec_root)
    edges = src._emit_edges or []
    supports = [
        e
        for e in edges
        if e.src == "hyp:test-hyp-1"
        and e.dst == "obs:test-obs-1"
        and e.edge_type == "supported_by"
        and e.attrs.get("reason") == "supports_hypotheses_frontmatter"
    ]
    assert supports, (
        "expected hyp:test-hyp-1 supported_by obs:test-obs-1 "
        "(from observation's supports_hypotheses frontmatter)"
    )


def test_emits_observation_contradicted_by_edges(
    openspec_root: Path,
) -> None:
    src = _run(openspec_root)
    edges = src._emit_edges or []
    contradicts = [
        e
        for e in edges
        if e.src == "hyp:test-hyp-2"
        and e.dst == "obs:test-obs-1"
        and e.edge_type == "contradicted_by"
    ]
    assert contradicts, "expected hyp:test-hyp-2 contradicted_by obs:test-obs-1"


def test_emits_observation_instantiates_principle_edges(
    openspec_root: Path,
) -> None:
    src = _run(openspec_root)
    edges = src._emit_edges or []
    instantiates = [
        e
        for e in edges
        if e.src == "obs:test-obs-1"
        and e.dst == "princ:test-principle-1"
        and e.edge_type == "instantiates"
    ]
    assert instantiates, "expected obs:test-obs-1 instantiates princ:test-principle-1"


def test_emits_insight_synthesis_edges(openspec_root: Path) -> None:
    src = _run(openspec_root)
    edges = src._emit_edges or []
    synth = [
        e
        for e in edges
        if e.src == "insight:test-insight-1"
        and e.edge_type == "references"
        and e.dst.startswith("connect:")
    ]
    assert len(synth) == 2, (
        f"expected 2 synthesizes edges from insight:test-insight-1, got {len(synth)}"
    )


def test_emits_insight_cites_observation_edges(
    openspec_root: Path,
) -> None:
    src = _run(openspec_root)
    edges = src._emit_edges or []
    cites = [
        e
        for e in edges
        if e.src == "insight:test-insight-1"
        and e.edge_type == "cites"
        and e.dst == "obs:test-obs-1"
    ]
    assert cites


def test_emits_hypotheses_tested_cross_link(
    openspec_root: Path,
) -> None:
    """Per Requirement: AcceptanceCriterion to Hypothesis Cross-Link.

    The fixture change has 3 acceptance criteria + frontmatter
    `hypotheses_tested: [test-hyp-1]`; expect 3 `supported_by` edges
    from `hyp:test-hyp-1` to each criterion.
    """
    src = _run(openspec_root)
    edges = src._emit_edges or []
    crit_links = [
        e
        for e in edges
        if e.src == "hyp:test-hyp-1"
        and e.edge_type == "supported_by"
        and e.dst.startswith("crit:test-change-1:")
        and e.attrs.get("reason") == "hypotheses_tested_frontmatter"
    ]
    assert len(crit_links) == 3, (
        f"expected 3 Hypothesis supported_by AcceptanceCriterion edges "
        f"from test-change-1's hypotheses_tested frontmatter; got "
        f"{len(crit_links)}: {[e.dst for e in crit_links]}"
    )


def test_unknown_schema_version_is_rejected_loudly(
    openspec_root: Path,
) -> None:
    """Per Requirement: Frontmatter Schema Version Loud Rejection."""
    src = _run(openspec_root)
    # The bad-schema-version file SHALL NOT be emitted as a node
    bad_node_ids = [
        n.node_id
        for n in src._emit_nodes or ()
        if n.node_id == "hyp:bad-schema-version"
    ]
    assert not bad_node_ids, (
        "hyp:bad-schema-version SHALL NOT be emitted (schema_version=99)"
    )
    # The file SHALL be recorded in degraded_files
    degraded = [p for p in src._degraded_files if "bad-schema-version" in p]
    assert degraded, (
        "bad-schema-version file SHALL be in _degraded_files for operator visibility"
    )


def test_idempotent_re_emission(openspec_root: Path) -> None:
    """A second run with the same content state SHALL skip files
    whose content hash matches the prior run.
    """
    src1 = _run(openspec_root)
    state = src1.get_run_state()
    src2 = OpenSpecSource(
        openspec_root=openspec_root,
        repo="okg-test",
        include_archived=True,
        state=state,
    )
    list(src2.nodes())
    list(src2.edges())
    # Second run with identical state should emit zero new nodes
    # (all files' hashes match prior run).
    assert not src2._emit_nodes, (
        "second run with identical content SHALL emit zero nodes; "
        f"got {len(src2._emit_nodes or ())}"
    )


def test_absent_reasoning_dirs_is_noop(tmp_path: Path) -> None:
    """Backward compat: if openspec/hypotheses/ etc. don't exist,
    the source adapter SHALL run cleanly without emitting reasoning
    nodes.
    """
    bare = tmp_path / "bare-openspec"
    (bare / "changes").mkdir(parents=True)
    src = OpenSpecSource(
        openspec_root=bare,
        repo="okg-test",
    )
    list(src.nodes())
    list(src.edges())
    reasoning_nodes = [
        n
        for n in src._emit_nodes or ()
        if n.subtype in {"hypothesis", "observation", "insight"}
    ]
    assert not reasoning_nodes, "absent reasoning dirs SHALL emit zero reasoning nodes"
