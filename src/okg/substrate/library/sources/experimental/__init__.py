"""Substrate sources for the `experimental` ontology module.

Three source adapters + a shared predicate parser:

- :class:`ExperimentRecordsSource` — reads operator-authored
  experiment YAML files; emits `Experiment` nodes + cross-module
  edges from frontmatter (`tests_hypothesis`, `uses_dataset`,
  `concerns_concept`, `ran_on_compute_job`).
- :class:`BenchOutputsSource` — reads benchmark output files
  (JSON / YAML / CSV) against a deployment-supplied
  `result_schema`; emits `Result` nodes connected to their
  parent Experiment.
- :class:`ClusterJobsSource` — reads a local cache directory
  of cluster job logs (Slurm / HTCondor / local); emits
  `ComputeJob` nodes. Default `pull_mode: operator-pulled`
  (the operator runs `okg <deployment> import-cluster-jobs`
  to populate the cache); `substrate-pulled` is deferred.
- :mod:`predicates` — the narrow `success_condition` DSL
  parser used by the source-side validator and the
  `success_condition_eval` cross-link matcher.

Per `extend-substrate-experimental-with-compute-job-and-sources`
(Wave A6 child of `evolve-memex-to-ai-scientist-substrate`).
"""

from __future__ import annotations

from okg.substrate.library.sources.experimental.bench_outputs import (
    BenchOutputsSource,
    ResultSchemaError,
)
from okg.substrate.library.sources.experimental.cluster_jobs import (
    ClusterJobsSource,
    ClusterPullModeError,
)
from okg.substrate.library.sources.experimental.experiment_records import (
    ExperimentRecordsSource,
)
from okg.substrate.library.sources.experimental.predicates import (
    Predicate,
    PredicateParseError,
)


__all__ = [
    "BenchOutputsSource",
    "ClusterJobsSource",
    "ClusterPullModeError",
    "ExperimentRecordsSource",
    "Predicate",
    "PredicateParseError",
    "ResultSchemaError",
]
