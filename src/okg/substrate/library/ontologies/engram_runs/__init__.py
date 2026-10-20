"""engram_runs ontology module.

Subtypes for Engram (and Glia-compatible) run-artifact ingestion:
ResearchDigest, ExperimentResult (refines reasoning.Evidence),
CandidateSolution, FinalResult, AgentSummary, EmpiricalClaim
(refines reasoning.Claim), FailedApproach (refines
reasoning.Hypothesis).

The module's source adapters live at
`okg.substrate.library.sources.engram_runs.*` — load via the
deployment's `modules:` list (opt-in; no shipped profile loads it).

Per `add-engram-run-artifact-sources` (Layer 3A of the
`add-portable-memex-deployments` umbrella).
"""
