"""Tests for the engram_runs source family (Layer 3A).

Coverage:

  * Schema-shape: parent_subtype refinements resolve via Layer 1A
    closure when `reasoning` is loaded.
  * Each source's ingest produces the expected typed nodes against
    the `engram_run_minimal` fixture.
  * `evaluates` edge keyed on (run_id, agent_id, experiment_id).
  * `refuted_by_experiment_ref` stamped on FailedApproach after
    agent_summary + experiment_result both ingested.
  * Conditional emission: scholarly-absent suppresses paper-
    citation refs with stats counter increment.
  * Conditional emission: bookkeeping-absent suppresses Project
    materialization with stats counter increment.
  * Configure-time gate: reasoning-absent raises
    `EngramSourceConfigError`.
  * Re-ingest idempotency: second run on unchanged input produces
    identical fact stream (same node ids, content hashes).
  * Malformed JSON: per-file YELLOW finding + skip without
    blocking sibling files.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_apply import (
    _detect_refinement_cycles,
)
from okg.substrate.catalog.modules_compose import (
    ComposedCatalog,
    _extract_subtypes,
)
from okg.substrate.library.sources.engram_runs import (
    EngramAgentSummarySource,
    EngramCandidateSolutionSource,
    EngramExperimentResultSource,
    EngramFinalResultSource,
    EngramResearchDigestSource,
)
from okg.substrate.library.sources.engram_runs._common import (
    EngramSourceConfigError,
)


# ---------- fixture paths ----------


FIXTURE_RUNS_DIR = (
    Path(__file__).resolve().parents[2]
    / "fixtures" / "engram_run_minimal" / "runs"
)

ENGRAM_SCHEMA = (
    Path(__file__).resolve().parents[4]
    / "src/okg/substrate/library/ontologies/engram_runs/schema.yaml"
)

REASONING_SCHEMA = (
    Path(__file__).resolve().parents[4]
    / "src/okg/substrate/library/ontologies/reasoning/schema.yaml"
)

EXTRACTION_SCHEMA = (
    Path(__file__).resolve().parents[4]
    / "src/okg/substrate/library/ontologies/extraction/schema.yaml"
)


MODULES_FULL = ("reasoning", "scholarly", "bookkeeping", "engram_runs")
MODULES_REASONING_ONLY = ("reasoning", "engram_runs")
MODULES_NO_BOOKKEEPING = ("reasoning", "scholarly", "engram_runs")
MODULES_NO_SCHOLARLY = ("reasoning", "bookkeeping", "engram_runs")


def _collect(source) -> tuple[dict[str, object], list, dict]:
    """Run a source against the fixture and split facts into
    (nodes_by_id, edges, stats).
    """
    run = source.run("test-run-id", mode="scope_complete")
    nodes: dict[str, object] = {}
    edges: list = []
    for f in run.facts:
        if hasattr(f, "node_id"):
            nodes[f.node_id] = f
        elif hasattr(f, "edge_type"):
            edges.append(f)
    return nodes, edges, source.last_run_stats


# ---------- schema-shape: refinements via parent_subtype ----------


def test_schema_subtypes_refine_reasoning_via_is_a() -> None:
    """LinkML `is_a:` on EmpiricalClaim / FailedApproach /
    ExperimentResult resolves to the snake-cased reasoning
    parents (per Layer 1A).
    """
    composed = ComposedCatalog(deployment_name="test")
    _extract_subtypes(REASONING_SCHEMA, "reasoning", composed)
    _extract_subtypes(EXTRACTION_SCHEMA, "extraction", composed)
    _extract_subtypes(ENGRAM_SCHEMA, "engram_runs", composed)
    # Subtype set contains the seven engram_runs subtypes.
    for s in (
        "research_digest", "experiment_result", "candidate_solution",
        "final_result", "agent_summary", "empirical_claim",
        "failed_approach",
    ):
        assert s in composed.subtypes
    # parent_subtype map carries the three refinements.
    assert composed.subtype_parent["experiment_result"] == "evidence"
    assert composed.subtype_parent["empirical_claim"] == "claim"
    assert composed.subtype_parent["failed_approach"] == "hypothesis"
    # Their reasoning / extraction parents are present (so the
    # DB-level FK on parent_subtype will resolve at apply time).
    assert "evidence" in composed.subtypes
    assert "claim" in composed.subtypes
    assert "hypothesis" in composed.subtypes


def test_refinement_chain_is_acyclic() -> None:
    """The full closure (engram_runs subtypes + reasoning parents +
    extraction parents) forms an acyclic refinement forest.
    """
    composed = ComposedCatalog(deployment_name="test")
    _extract_subtypes(REASONING_SCHEMA, "reasoning", composed)
    _extract_subtypes(EXTRACTION_SCHEMA, "extraction", composed)
    _extract_subtypes(ENGRAM_SCHEMA, "engram_runs", composed)
    _detect_refinement_cycles(composed)  # no raise expected


def test_schema_intra_narrowings_present() -> None:
    """The schema's EdgeNarrowings class declares all intra-
    module narrowings (evaluates, produced_during, surfaces,
    best_solution, refuted_by).
    """
    with ENGRAM_SCHEMA.open() as f:
        data = yaml.safe_load(f)
    en = data["classes"]["EdgeNarrowings"]
    attrs = en["attributes"]
    archetypes_present = set()
    for attr_def in attrs.values():
        annotations = attr_def.get("annotations") or {}
        archetypes_present.add(annotations.get("edge_archetype"))
    for needed in {
        "evaluates", "produced_during", "surfaces",
        "best_solution", "refuted_by",
    }:
        assert needed in archetypes_present, (
            f"schema EdgeNarrowings missing edge_archetype "
            f"{needed!r}; present: {archetypes_present}"
        )


# ---------- per-source: nodes + edges against fixture ----------


def test_research_digest_source_emits_digest_and_project() -> None:
    src = EngramResearchDigestSource(
        runs_dir=str(FIXTURE_RUNS_DIR), loaded_modules=MODULES_FULL,
    )
    nodes, edges, stats = _collect(src)
    assert "engram_digest:sr_demo_0001" in nodes
    digest = nodes["engram_digest:sr_demo_0001"]
    assert digest.subtype == "research_digest"
    assert digest.attrs["run_id"] == "sr_demo_0001"
    assert digest.attrs["problem_name"] == "cost-tuned-router-design"
    assert digest.attrs["source_generation_id"] == "cv-2026-04-30-a"
    assert "content_hash" in digest.attrs
    # source_record_id discipline.
    assert digest.source_record_id["engram_run_id"] == "sr_demo_0001"
    assert digest.source_record_id["problem_name"] == (
        "cost-tuned-router-design"
    )
    assert (
        digest.source_record_id["artifact_path"]
        == "sr_demo_0001/research_journal.md"
    )
    # bookkeeping is loaded → Project materialized.
    assert "proj:engram:sr_demo_0001" in nodes
    project = nodes["proj:engram:sr_demo_0001"]
    assert project.subtype == "project"
    assert project.attrs["produced_engram_digest_ref"] == (
        "engram_digest:sr_demo_0001"
    )
    assert stats["digests_emitted"] == 1
    assert stats["projects_emitted"] == 1


def test_experiment_result_source_emits_typed_evidence() -> None:
    src = EngramExperimentResultSource(
        runs_dir=str(FIXTURE_RUNS_DIR), loaded_modules=MODULES_FULL,
    )
    nodes, edges, stats = _collect(src)
    exp1 = nodes["engram_exp:sr_demo_0001:agent_0:exp_0001"]
    assert exp1.subtype == "experiment_result"
    assert exp1.attrs["score"] == pytest.approx(0.42)
    assert exp1.attrs["score_metric"] == "cost_objective"
    assert exp1.attrs["simulator"] == "vidur"
    exp2 = nodes["engram_exp:sr_demo_0001:agent_0:exp_0002"]
    assert exp2.attrs["score"] == pytest.approx(0.91)
    # `evaluates` keyed on (run_id, agent_id, experiment_id) →
    # links each ExperimentResult to its sibling
    # CandidateSolution (language=python in this fixture).
    evaluates_edges = [
        e for e in edges if e.edge_type == "evaluates"
    ]
    assert len(evaluates_edges) == 2
    assert {(e.src, e.dst) for e in evaluates_edges} == {
        (
            "engram_exp:sr_demo_0001:agent_0:exp_0001",
            "engram_cand:sr_demo_0001:agent_0:exp_0001:python",
        ),
        (
            "engram_exp:sr_demo_0001:agent_0:exp_0002",
            "engram_cand:sr_demo_0001:agent_0:exp_0002:python",
        ),
    }
    # `produced_during` to the per-run digest.
    produced_during = [
        e for e in edges if e.edge_type == "produced_during"
    ]
    assert len(produced_during) == 2
    for e in produced_during:
        assert e.dst == "engram_digest:sr_demo_0001"
    assert stats["experiments_emitted"] == 2


def test_candidate_solution_source_emits_typed_candidates() -> None:
    src = EngramCandidateSolutionSource(
        runs_dir=str(FIXTURE_RUNS_DIR), loaded_modules=MODULES_FULL,
    )
    nodes, edges, stats = _collect(src)
    cand1 = nodes["engram_cand:sr_demo_0001:agent_0:exp_0001:python"]
    assert cand1.subtype == "candidate_solution"
    assert cand1.attrs["language"] == "python"
    assert cand1.attrs["agent_id"] == "agent_0"
    assert cand1.attrs["experiment_id"] == "exp_0001"
    assert "def route(" in cand1.attrs["source_text"]
    # produced_during edge to the digest.
    pd = [e for e in edges if e.edge_type == "produced_during"]
    assert len(pd) == 2
    assert all(e.dst == "engram_digest:sr_demo_0001" for e in pd)


def test_final_result_source_stamps_best_candidate_ref() -> None:
    src = EngramFinalResultSource(
        runs_dir=str(FIXTURE_RUNS_DIR), loaded_modules=MODULES_FULL,
    )
    nodes, edges, stats = _collect(src)
    final = nodes["engram_final:sr_demo_0001"]
    assert final.subtype == "final_result"
    assert final.attrs["best_score"] == pytest.approx(0.42)
    assert final.attrs["total_simulations"] == 2
    assert final.attrs["total_agents"] == 1
    assert final.attrs["convergence_reason"] == "budget_exhausted"
    # best_candidate_ref points at the matching CandidateSolution
    # (language pulled from best_snapshot block).
    assert final.attrs["best_candidate_ref"] == (
        "engram_cand:sr_demo_0001:agent_0:exp_0001:python"
    )
    # produced_during edge to the digest.
    pd = [e for e in edges if e.edge_type == "produced_during"]
    assert len(pd) == 1
    assert pd[0].dst == "engram_digest:sr_demo_0001"
    # bookkeeping loaded → Project NodeFact stamps
    # produced_engram_final_ref.
    project = nodes["proj:engram:sr_demo_0001"]
    assert project.attrs["produced_engram_final_ref"] == (
        "engram_final:sr_demo_0001"
    )
    assert stats["best_solution_refs_stamped"] == 1
    assert stats["project_refs_stamped"] == 1


def test_agent_summary_decomposes_claims_and_failures() -> None:
    src = EngramAgentSummarySource(
        runs_dir=str(FIXTURE_RUNS_DIR), loaded_modules=MODULES_FULL,
    )
    nodes, edges, stats = _collect(src)
    # AgentSummary node.
    summary = nodes["engram_summary:sr_demo_0001:agent_0"]
    assert summary.subtype == "agent_summary"
    # paper_key in JSON → cite_key_ref stamped when scholarly is loaded.
    assert summary.attrs["cite_key_ref"] == (
        "paper:vale1986storage"
    )
    # Two EmpiricalClaim nodes (one per empirical_claims entry).
    ec0 = nodes["engram_claim:sr_demo_0001:agent_0:0"]
    assert ec0.subtype == "empirical_claim"
    assert "weighted-random" in ec0.attrs["statement"]
    assert ec0.attrs["observed_score_delta"] == pytest.approx(-0.49)
    # Per-entry paper_key wins over summary-level.
    assert ec0.attrs["cite_key_ref"] == "paper:vaswani2017attention"
    ec1 = nodes["engram_claim:sr_demo_0001:agent_0:1"]
    assert "regret budget" in ec1.attrs["statement"]
    assert "cite_key_ref" not in ec1.attrs  # no paper_key in entry
    # One FailedApproach node with refuted_by_experiment_ref.
    fa = nodes["engram_failed:sr_demo_0001:agent_0:0"]
    assert fa.subtype == "failed_approach"
    assert fa.attrs["failure_mode"] == "load_concentration"
    # The cross-link rule resolves this against the matching
    # ExperimentResult id once both nodes are projected.
    assert fa.attrs["refuted_by_experiment_ref"] == (
        "engram_exp:sr_demo_0001:agent_0:exp_0002"
    )
    # `surfaces` edges from AgentSummary to each decomposed node.
    surfaces = [e for e in edges if e.edge_type == "surfaces"]
    assert {(e.src, e.dst) for e in surfaces} == {
        (
            "engram_summary:sr_demo_0001:agent_0",
            "engram_claim:sr_demo_0001:agent_0:0",
        ),
        (
            "engram_summary:sr_demo_0001:agent_0",
            "engram_claim:sr_demo_0001:agent_0:1",
        ),
        (
            "engram_summary:sr_demo_0001:agent_0",
            "engram_failed:sr_demo_0001:agent_0:0",
        ),
    }
    assert stats["empirical_claims_emitted"] == 2
    assert stats["failed_approaches_emitted"] == 1
    assert stats["refuted_by_refs_stamped"] == 1


# ---------- evaluates keyed on (run_id, agent_id, experiment_id) ----------


def test_evaluates_edge_matches_run_agent_experiment_triple() -> None:
    """For every ExperimentResult emitted with a sibling
    snapshot.<lang>, the `evaluates` edge points at a
    CandidateSolution whose id encodes the same
    (run_id, agent_id, experiment_id, language) tuple.
    """
    exp_src = EngramExperimentResultSource(
        runs_dir=str(FIXTURE_RUNS_DIR), loaded_modules=MODULES_FULL,
    )
    cand_src = EngramCandidateSolutionSource(
        runs_dir=str(FIXTURE_RUNS_DIR), loaded_modules=MODULES_FULL,
    )
    exp_nodes, exp_edges, _ = _collect(exp_src)
    cand_nodes, _, _ = _collect(cand_src)
    evaluates_edges = [
        e for e in exp_edges if e.edge_type == "evaluates"
    ]
    assert evaluates_edges, "expected at least one evaluates edge"
    for e in evaluates_edges:
        # dst must resolve to a CandidateSolution that the
        # candidate-solution source emits in the same publish.
        assert e.dst in cand_nodes, (
            f"evaluates edge dst {e.dst!r} does not match any "
            f"CandidateSolution emitted by the candidate source: "
            f"{sorted(cand_nodes)}"
        )
        # The shared triple is encoded in both ids.
        exp_node = exp_nodes[e.src]
        cand_node = cand_nodes[e.dst]
        assert exp_node.attrs["run_id"] == cand_node.attrs["run_id"]
        assert exp_node.attrs["agent_id"] == cand_node.attrs["agent_id"]
        assert (
            exp_node.attrs["experiment_id"]
            == cand_node.attrs["experiment_id"]
        )


# ---------- refuted_by_experiment_ref + matching experiment ----------


def test_failed_approach_refuted_by_ref_points_at_real_experiment() -> None:
    """The FailedApproach's `refuted_by_experiment_ref` resolves
    to an ExperimentResult node id the experiment-result source
    emits in the same publish.
    """
    sum_src = EngramAgentSummarySource(
        runs_dir=str(FIXTURE_RUNS_DIR), loaded_modules=MODULES_FULL,
    )
    exp_src = EngramExperimentResultSource(
        runs_dir=str(FIXTURE_RUNS_DIR), loaded_modules=MODULES_FULL,
    )
    sum_nodes, _, _ = _collect(sum_src)
    exp_nodes, _, _ = _collect(exp_src)
    fa = sum_nodes["engram_failed:sr_demo_0001:agent_0:0"]
    target_ref = fa.attrs["refuted_by_experiment_ref"]
    assert target_ref in exp_nodes, (
        f"refuted_by_experiment_ref {target_ref!r} doesn't match "
        f"any emitted ExperimentResult: {sorted(exp_nodes)}"
    )


# ---------- conditional emission: scholarly absent ----------


def test_scholarly_absent_suppresses_cite_key_refs(
    tmp_path: Path,
) -> None:
    """Without scholarly in `loaded_modules`, the summary source
    omits `cite_key_ref` from emitted nodes and increments the
    `paper_citations_suppressed_count` stat.
    """
    src = EngramAgentSummarySource(
        runs_dir=str(FIXTURE_RUNS_DIR),
        loaded_modules=MODULES_NO_SCHOLARLY,
    )
    nodes, _, stats = _collect(src)
    summary = nodes["engram_summary:sr_demo_0001:agent_0"]
    assert "cite_key_ref" not in summary.attrs
    ec0 = nodes["engram_claim:sr_demo_0001:agent_0:0"]
    assert "cite_key_ref" not in ec0.attrs
    # Two paper-key references in the fixture: summary-level and
    # one empirical_claims entry's paper_key.
    assert stats["paper_citations_suppressed_count"] == 2
    assert stats["paper_citations_emitted_count"] == 0


# ---------- conditional emission: bookkeeping absent ----------


def test_bookkeeping_absent_suppresses_project_materialization() -> None:
    """Without bookkeeping in `loaded_modules`, the digest source
    skips the Project NodeFact and increments
    `project_materialization_suppressed`.
    """
    src = EngramResearchDigestSource(
        runs_dir=str(FIXTURE_RUNS_DIR),
        loaded_modules=MODULES_NO_BOOKKEEPING,
    )
    nodes, _, stats = _collect(src)
    assert "engram_digest:sr_demo_0001" in nodes
    assert "proj:engram:sr_demo_0001" not in nodes
    assert stats["projects_emitted"] == 0
    assert stats["project_materialization_suppressed"] == 1
    # Same gate on the final-result source.
    final_src = EngramFinalResultSource(
        runs_dir=str(FIXTURE_RUNS_DIR),
        loaded_modules=MODULES_NO_BOOKKEEPING,
    )
    final_nodes, _, final_stats = _collect(final_src)
    assert "engram_final:sr_demo_0001" in final_nodes
    assert "proj:engram:sr_demo_0001" not in final_nodes
    assert final_stats["project_refs_suppressed"] == 1


# ---------- configure-time gate: reasoning absent ----------


def test_reasoning_absent_raises_at_configure_time() -> None:
    """Without `reasoning` in `loaded_modules`, every engram_runs
    source SHALL raise `EngramSourceConfigError` at construction
    time (engram_runs's refined subtypes need reasoning's
    parents).
    """
    bad_modules = ("scholarly", "bookkeeping", "engram_runs")
    for cls in (
        EngramResearchDigestSource,
        EngramExperimentResultSource,
        EngramCandidateSolutionSource,
        EngramFinalResultSource,
        EngramAgentSummarySource,
    ):
        with pytest.raises(EngramSourceConfigError):
            cls(
                runs_dir=str(FIXTURE_RUNS_DIR),
                loaded_modules=bad_modules,
            )


# ---------- idempotency ----------


def test_reingest_is_idempotent_for_all_sources() -> None:
    """A second ingest on the unchanged fixture produces identical
    facts (same node ids, edges, content hashes). Verified by
    comparing the full fact tuples across two runs of each source.
    """
    for cls in (
        EngramResearchDigestSource,
        EngramExperimentResultSource,
        EngramCandidateSolutionSource,
        EngramFinalResultSource,
        EngramAgentSummarySource,
    ):
        src1 = cls(
            runs_dir=str(FIXTURE_RUNS_DIR),
            loaded_modules=MODULES_FULL,
        )
        src2 = cls(
            runs_dir=str(FIXTURE_RUNS_DIR),
            loaded_modules=MODULES_FULL,
        )
        n1, e1, _ = _collect(src1)
        n2, e2, _ = _collect(src2)
        # Same node id set + content_hash per node.
        assert set(n1) == set(n2), (
            f"{cls.__name__}: re-ingest changed node ids "
            f"({set(n2) - set(n1)} new, "
            f"{set(n1) - set(n2)} missing)"
        )
        for nid in n1:
            ch1 = n1[nid].attrs.get("content_hash")
            ch2 = n2[nid].attrs.get("content_hash")
            assert ch1 == ch2, (
                f"{cls.__name__}: node {nid} content_hash drifted "
                f"({ch1!r} -> {ch2!r})"
            )
        # Same edge identity set.
        edge_ids_1 = {(e.src, e.edge_type, e.dst) for e in e1}
        edge_ids_2 = {(e.src, e.edge_type, e.dst) for e in e2}
        assert edge_ids_1 == edge_ids_2, (
            f"{cls.__name__}: re-ingest changed edge identities"
        )


# ---------- malformed JSON: per-file YELLOW + skip without blocking ----------


def test_malformed_agent_summary_yields_yellow_and_skips(
    tmp_path: Path,
) -> None:
    """Malformed `agent_<N>_summary.json` produces one per-file
    YELLOW finding and the source continues to ingest sibling
    summary files (and the rest of the run's artifacts) without
    blocking.
    """
    # Copy the fixture into a tmp tree (so we can mutate it).
    import shutil
    src_dir = tmp_path / "runs"
    shutil.copytree(FIXTURE_RUNS_DIR, src_dir)
    run_dir = src_dir / "sr_demo_0001"

    # Corrupt agent_0_summary.json to invalid JSON.
    bad_summary = run_dir / "agent_0_summary.json"
    bad_summary.write_text(
        "{ \"agent_id\": \"agent_0\", \"summary_text\": ",
        encoding="utf-8",
    )

    # Add a SECOND valid agent summary so we can verify the source
    # continues with siblings.
    second_summary = run_dir / "agent_1_summary.json"
    second_summary.write_text(json.dumps({
        "agent_id": "agent_1",
        "summary_text": "ok",
        "empirical_claims": [],
        "failed_approaches": [],
    }), encoding="utf-8")

    src = EngramAgentSummarySource(
        runs_dir=str(src_dir), loaded_modules=MODULES_FULL,
    )
    nodes, _, stats = _collect(src)
    # Bad file produced a YELLOW finding.
    yellow = [
        f for f in src.last_run_findings
        if f.get("severity") == "YELLOW"
        and f.get("code") == "agent_summary_malformed"
    ]
    assert len(yellow) == 1, (
        f"expected 1 YELLOW finding, got "
        f"{src.last_run_findings}"
    )
    assert yellow[0]["agent_id"] == "agent_0"
    # The sibling agent_1_summary.json still ingests.
    assert "engram_summary:sr_demo_0001:agent_1" in nodes
    # The bad file does NOT emit an AgentSummary.
    assert "engram_summary:sr_demo_0001:agent_0" not in nodes
    # The stat counter records the malformed file.
    assert stats["summary_malformed_json"] == 1


def test_malformed_final_result_yields_yellow_and_skips(
    tmp_path: Path,
) -> None:
    """Malformed `final_result.json` produces YELLOW finding and
    the source emits nothing for that run; sibling runs still
    ingest.
    """
    import shutil
    src_dir = tmp_path / "runs"
    shutil.copytree(FIXTURE_RUNS_DIR, src_dir)
    bad_final = src_dir / "sr_demo_0001" / "final_result.json"
    bad_final.write_text("not json", encoding="utf-8")

    src = EngramFinalResultSource(
        runs_dir=str(src_dir), loaded_modules=MODULES_FULL,
    )
    nodes, _, stats = _collect(src)
    assert "engram_final:sr_demo_0001" not in nodes
    yellow = [
        f for f in src.last_run_findings
        if f.get("severity") == "YELLOW"
        and f.get("code") == "final_result_malformed"
    ]
    assert len(yellow) == 1
    assert stats["final_malformed_json"] == 1


# ---------- run missing optional artifacts still ingests ----------


def test_run_missing_summary_still_ingests_other_artifacts(
    tmp_path: Path,
) -> None:
    """A run without any `agent_<N>_summary.json` still produces
    its ResearchDigest, ExperimentResults, CandidateSolutions, and
    FinalResult; the summary source records
    `runs_missing_summary: 1` and emits zero summaries.
    """
    import shutil
    src_dir = tmp_path / "runs"
    shutil.copytree(FIXTURE_RUNS_DIR, src_dir)
    # Remove the agent summary.
    (src_dir / "sr_demo_0001" / "agent_0_summary.json").unlink()

    # Each non-summary source still emits its expected nodes.
    digest_src = EngramResearchDigestSource(
        runs_dir=str(src_dir), loaded_modules=MODULES_FULL,
    )
    digest_nodes, _, _ = _collect(digest_src)
    assert "engram_digest:sr_demo_0001" in digest_nodes

    exp_src = EngramExperimentResultSource(
        runs_dir=str(src_dir), loaded_modules=MODULES_FULL,
    )
    exp_nodes, _, _ = _collect(exp_src)
    assert len([n for n in exp_nodes.values() if n.subtype == "experiment_result"]) == 2

    cand_src = EngramCandidateSolutionSource(
        runs_dir=str(src_dir), loaded_modules=MODULES_FULL,
    )
    cand_nodes, _, _ = _collect(cand_src)
    assert len([n for n in cand_nodes.values() if n.subtype == "candidate_solution"]) == 2

    final_src = EngramFinalResultSource(
        runs_dir=str(src_dir), loaded_modules=MODULES_FULL,
    )
    final_nodes, _, _ = _collect(final_src)
    assert "engram_final:sr_demo_0001" in final_nodes

    # Summary source — zero AgentSummary nodes, stat increments.
    sum_src = EngramAgentSummarySource(
        runs_dir=str(src_dir), loaded_modules=MODULES_FULL,
    )
    sum_nodes, sum_edges, sum_stats = _collect(sum_src)
    assert all(
        n.subtype != "agent_summary" for n in sum_nodes.values()
    )
    assert sum_stats["runs_missing_summary"] == 1
    assert sum_stats["summaries_emitted"] == 0
    # No EmpiricalClaim / FailedApproach either.
    assert all(
        n.subtype not in {"empirical_claim", "failed_approach"}
        for n in sum_nodes.values()
    )


# ---------- run missing the mandatory research_journal.md ----------


def test_run_missing_journal_emits_yellow_and_skips(
    tmp_path: Path,
) -> None:
    """A run directory missing `research_journal.md` produces a
    per-run YELLOW finding and skips the run; sibling runs still
    ingest.
    """
    import shutil
    src_dir = tmp_path / "runs"
    shutil.copytree(FIXTURE_RUNS_DIR, src_dir)
    (src_dir / "sr_demo_0001" / "research_journal.md").unlink()

    src = EngramResearchDigestSource(
        runs_dir=str(src_dir), loaded_modules=MODULES_FULL,
    )
    nodes, _, stats = _collect(src)
    assert "engram_digest:sr_demo_0001" not in nodes
    yellow = [
        f for f in src.last_run_findings
        if f.get("severity") == "YELLOW"
        and f.get("code") == "research_journal_missing"
    ]
    assert len(yellow) == 1
    assert stats["runs_missing_journal"] == 1
    assert stats["digests_emitted"] == 0
