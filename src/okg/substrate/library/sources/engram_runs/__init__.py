"""Substrate-shipped Engram run-artifact source adapters.

Per `add-engram-run-artifact-sources` (Layer 3A of the
`add-portable-memex-deployments` umbrella). Pull-mode adapters
that walk a configured `runs_dir` and emit typed substrate facts
for the artifacts Engram (and Glia-compatible) runs produce on
disk.

Adapters in this package:

  - `EngramResearchDigestSource`     — `research_journal.md`
                                       → ResearchDigest
  - `EngramExperimentResultSource`   — `knowledgebase/agent_<N>/
                                       experiments/exp_<M>/score.txt`
                                       → ExperimentResult (refines
                                       reasoning.Evidence)
  - `EngramCandidateSolutionSource`  — `…/exp_<M>/snapshot.{py,cpp,…}`
                                       → CandidateSolution
  - `EngramFinalResultSource`        — `final_result.json`
                                       → FinalResult
  - `EngramAgentSummarySource`       — `agent_<N>_summary.json`
                                       → AgentSummary + decomposed
                                       EmpiricalClaim / FailedApproach
                                       (refine reasoning.Claim /
                                       Hypothesis)

All adapters honor the substrate's `discovery_crawl` profile —
idempotent re-runs, content-hash dedup, per-source watermark.
Each emits `source_record_id` carrying engram_run_id, problem_name,
agent_id (when applicable), experiment_id (when applicable),
artifact_path, content_hash, and `source_generation_id` when the
runs_dir manifest records the OKG generation the run consumed.

Conditional cross-module emission (per spec
`Conditional Cross-Module Emission`):

  - `reasoning` is a HARD dependency. Sources fail at configure
    time when reasoning is absent — EmpiricalClaim / FailedApproach
    / ExperimentResult refine reasoning subtypes via LinkML `is_a:`.
  - `scholarly` is optional. When absent, sources suppress
    `cites: scholarly.Paper` / `builds_on: scholarly.Paper` edge
    references; the per-run stats counter
    `paper_citations_suppressed_count` records the suppressed total.
  - `bookkeeping` is optional. When absent, sources suppress the
    Project materialization and the `produced_by` edge references.
"""
from importlib import import_module

__all__ = [
    "EngramResearchDigestSource",
    "EngramExperimentResultSource",
    "EngramCandidateSolutionSource",
    "EngramFinalResultSource",
    "EngramAgentSummarySource",
]

_SOURCE_MODULES = {
    "EngramResearchDigestSource": "research_digest",
    "EngramExperimentResultSource": "experiment_result",
    "EngramCandidateSolutionSource": "candidate_solution",
    "EngramFinalResultSource": "final_result",
    "EngramAgentSummarySource": "agent_summary",
}


def __getattr__(name: str):
    """Load adapters on demand so parallel direct-module imports cannot cycle."""
    module_name = _SOURCE_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(f"{__name__}.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value
