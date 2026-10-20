"""End-to-end integration test for the A6 experiment registry.

Walks the full stack against a real Postgres test database:

  1. Compose `experimental` + `reasoning` + `dataset` + `compute_env`
     + their transitive substrate deps; apply the catalog.
  2. Seed:
     - one Hypothesis (in reasoning)
     - one Dataset (in dataset)
     - one ComputeJob (in compute_env)
     - one Experiment with `success_condition` + frontmatter-derived
       edges to Hypothesis, Dataset, ComputeJob (via
       ExperimentRecordsSource)
     - two Results — one satisfying the success_condition, one
       failing it (via BenchOutputsSource)
  3. Run a publish cycle; verify every promised edge lands in
     `edges_live`:
       Experiment tests Hypothesis
       Experiment uses Dataset
       Experiment ran_on ComputeJob
       Result supports Hypothesis      (from success_condition)
       Result contradicts Hypothesis   (from success_condition)
  4. (Experiment status verification removed — `okg experiment
     status` was retired in favor of `inspect(target={...})`.)

Run via:
  OKG_TEST_DSN=postgres://postgres:okg@localhost:5433/okg_test_a6_impl \\
    uv run python -m pytest \\
      tests/substrate/library/sources/experimental/test_integration.py \\
      -v --no-cov
"""

from __future__ import annotations

import csv
import json
import textwrap
from pathlib import Path

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


_MODULES = [
    "experimental", "reasoning", "dataset", "compute_env",
    "agent_sessions", "agent_memory", "git_graph", "openspec",
    "code_graph", "ci", "person",
]


def _make_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "narrowing-test"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "narrowing-test",
        "description": "A6 end-to-end integration test",
        "modules": _MODULES,
    }))
    (dep / "schemas").mkdir()
    return dep


def _write_experiment(experiments_dir: Path) -> None:
    experiments_dir.mkdir(parents=True, exist_ok=True)
    # NOTE: `concerns_concept` is intentionally omitted here.
    # The `(experiment, concerns, concept)` narrowing is declared by
    # A3's `concepts` module, not by A6. In a real deployment that
    # composes A3, the source emits the edge and the narrowing trigger
    # accepts it; this test focuses on the A6-owned narrowings.
    (experiments_dir / "k.yaml").write_text(textwrap.dedent("""
        name: krama-e2e
        title: KramaBench end-to-end
        kind: benchmark
        status: complete
        tests_hypothesis: hyp:krama-faster
        uses_dataset: ds:krama-v1
        ran_on_compute_job:
          cluster: cluster01
          job_id: 9999
        success_condition:
          metric: speedup
          predicate: ">= 1.0"
    """).lstrip())


def _write_bench_outputs(bench_dir: Path) -> None:
    bench_dir.mkdir(parents=True, exist_ok=True)
    with (bench_dir / "results.csv").open("w") as f:
        w = csv.writer(f)
        w.writerow(["experiment", "metric", "value", "unit"])
        w.writerow(["exp:krama-e2e", "speedup", "1.42", "x"])
        w.writerow(["exp:krama-e2e", "speedup", "0.87", "x"])


def _write_cluster_log(cache_dir: Path) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "sacct.json").write_text(json.dumps([{
        "JobID": "9999",
        "Submit": "2026-05-15T10:00:00Z",
        "Start": "2026-05-15T10:01:00Z",
        "End": "2026-05-15T10:30:00Z",
        "Elapsed": 1740,
        "ExitCode": 0,
    }]))


def _seed_nodes() -> list:
    """The Hypothesis / Dataset / Concept target nodes the A6 sources
    wire up to. A real deployment's sources would emit these; for this
    test we seed them directly into the Chronos graph.
    """
    from okg.substrate.library.sources.base import NodeFact

    return [
        NodeFact("hyp:krama-faster", "hypothesis", {
            "hypothesis_id": "hyp:krama-faster",
            "statement": "OKG-backed Krama is at least as fast as monolithic.",
            "status": "open",
        }, {"id": "hyp:krama-faster"}, {"v": 1}),
        NodeFact("ds:krama-v1", "dataset", {
            "dataset_id": "ds:krama-v1",
            "name": "KramaBench v1",
        }, {"id": "ds:krama-v1"}, {"v": 1}),
        NodeFact("concept:bitemporal", "concept", {
            "concept_id": "concept:bitemporal",
            "name": "Bitemporal Data",
        }, {"id": "concept:bitemporal"}, {"v": 1}),
    ]


def _apply_source_facts(
    conn: psycopg.Connection,
    *,
    fact_batches: list[list],
    extra_trial_nodes: list[tuple[str, dict]] = (),
) -> None:
    """Write the A6 sources' facts to the Chronos graph (the canonical
    write path; the chronos-native publisher checkpoints the branch head,
    it no longer projects facts into the dropped nodes_live/edges_live).

    Seed-target nodes + all source NodeFacts + synthetic Trial nodes are
    applied first, then the EdgeFacts, so the strict-endpoint check sees
    both ends.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    nodes: list[NodeFact] = list(_seed_nodes())
    edges: list[EdgeFact] = []
    for batch in fact_batches:
        for fact in batch:
            if isinstance(fact, NodeFact):
                nodes.append(fact)
            elif isinstance(fact, EdgeFact):
                edges.append(fact)
    for node_id, attrs in extra_trial_nodes:
        nodes.append(
            NodeFact(node_id, "trial", dict(attrs), {"id": node_id}, {"v": 1})
        )

    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(nodes, source="fixture", strict_endpoints=False)
    store.apply_facts(edges, source="fixture", strict_endpoints=False)


def test_a6_full_lifecycle(
    clean_runtime: str, tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.library.linkers.success_condition import (
        SuccessConditionLinker,
    )
    from okg.substrate.library.sources.experimental import (
        BenchOutputsSource,
        ClusterJobsSource,
        ExperimentRecordsSource,
    )

    # --- catalog ---
    dep = _make_deployment(tmp_path)
    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        cv = apply_composed_catalog(conn, composed)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.catalog_versions SET status='applied' "
                "WHERE catalog_version_id = %s",
                (cv,),
            )
        conn.commit()

        # --- sources: produce facts in-process ---
        _write_experiment(tmp_path / "experiments")
        _write_bench_outputs(tmp_path / "bench")
        _write_cluster_log(tmp_path / "cluster_logs" / "cluster01")

        records = list(ExperimentRecordsSource(
            records_path="experiments/*.yaml",
            base_dir=tmp_path,
        ).run("r-1", mode="release_new").facts)
        bench = list(BenchOutputsSource(
            results_glob="bench/*.csv",
            result_schema={
                "experiment_ref": "experiment",
                "metric_name": "metric",
                "metric_value": "value",
                "metric_unit": "unit",
            },
            base_dir=tmp_path,
        ).run("r-2", mode="release_new").facts)
        cluster = list(ClusterJobsSource(
            host="cluster01",
            cache_dir=tmp_path / "cluster_logs",
            job_log_glob="*.json",
        ).run("r-3", mode="release_new").facts)

        # --- write the source facts into the Chronos graph ---
        _apply_source_facts(
            conn,
            fact_batches=[records, bench, cluster],
            # BenchOutputsSource emits `Trial produces Result` edges
            # referencing a synthetic default-trial id; create the
            # Trial node so the edge endpoint exists.
            extra_trial_nodes=[(
                "trial:krama-e2e:default",
                {
                    "trial_id": "trial:krama-e2e:default",
                    "experiment_id": "exp:krama-e2e",
                    "status": "complete",
                },
            )],
        )
        conn.commit()

    # --- publish with the success_condition linker registered as an
    #     enricher (the production path): the cycle runs the enricher
    #     over the Chronos branch graph, ingests its emitted outcome
    #     edge_facts into the graph, and checkpoints the head. ---
    from okg.substrate.publisher import Publisher

    out = Publisher(
        clean_runtime, "narrowing-test",
        enrichers=[SuccessConditionLinker()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "narrowing-test"),
    ).cycle()
    assert out["status"] == "published", out
    gen_id = out["generation_id"]

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(gen_id)

        # All three narrowing-bound source-emitted edges land in the graph.
        exp_edges = sorted(
            (str(r["edge_type"]), str(r["dst"]))
            for r in graph.query(
                "SELECT edge_type, dst FROM okg.graph_edges "
                "WHERE src = 'exp:krama-e2e'"
            )
        )
        assert exp_edges == sorted([
            ("ran_on", "job:cluster01:9999"),
            ("tests", "hyp:krama-faster"),
            ("uses", "ds:krama-v1"),
        ])

        # The ComputeJob node came from ClusterJobsSource.
        job_rows = graph.query(
            "SELECT subtype, attrs->>'cluster' AS cluster, "
            "attrs->>'job_id' AS job_id FROM okg.graph_nodes "
            "WHERE node_id = 'job:cluster01:9999'"
        )
        assert len(job_rows) == 1
        assert (
            job_rows[0]["subtype"],
            job_rows[0]["cluster"],
            job_rows[0]["job_id"],
        ) == ("compute_job", "cluster01", "9999")

        # Both Results landed.
        n_results = int(graph.query(
            "SELECT count(*) AS n FROM okg.graph_nodes "
            "WHERE subtype = 'result' "
            "AND attrs->>'experiment_ref' = 'exp:krama-e2e'"
        )[0]["n"])
        assert n_results == 2

        # The success-condition linker emitted one supports + one
        # contradicts edge to the hypothesis; the publisher ingested them
        # into the Chronos graph.
        outcome_counts = {
            str(r["edge_type"]): int(r["n"])
            for r in graph.query(
                "SELECT edge_type, count(*) AS n FROM okg.graph_edges "
                "WHERE dst = 'hyp:krama-faster' "
                "AND edge_type IN ('supports', 'contradicts') "
                "GROUP BY edge_type"
            )
        }
        assert outcome_counts == {"contradicts": 1, "supports": 1}

        # Each outcome edge carries the predicate context for audit.
        supports_rows = graph.query(
            "SELECT attrs FROM okg.graph_edges "
            "WHERE dst = 'hyp:krama-faster' AND edge_type = 'supports'"
        )
        assert len(supports_rows) == 1
        attrs = dict(supports_rows[0]["attrs"])
        assert attrs["metric"] == "speedup"
        assert attrs["predicate"] == ">= 1.0"
        assert attrs["experiment_id"] == "exp:krama-e2e"
        assert attrs["metric_value"] == 1.42
    finally:
        store.close()
        assert attrs["metric_value"] == 1.42
