"""okg.bench — the shipped benchmark harness (PACT ship-okg-bench).

Harness CODE lives here and installs with the wheel so external consumers
(the okg-deployments repo hosting injection ring 1a, any deployment
benchmarking its own graph) can import it. Lab STATE — experiment
manifests, sweep locks, results, generated deployments — stays repo-side
under a bench root resolved from ``OKG_BENCH_ROOT`` (default: ``./bench``).

Modules (import them directly; nothing heavy loads on a bare
``import okg.bench``):

- ``okg.bench.injection`` — the synthetic inconsistency injection runner
  (issue #749 ring 0): generation-pinned targeting, sealed-artifact
  emission, live pipeline states, scorecard writing.
- ``okg.bench.orchestrator`` / ``extract`` / ``engine_sensitivity`` /
  ``differential_replay_{fixture,proof}`` — the commit-replay perf lab.
- ``okg.bench.agent_memory`` — the agent-quality harness (25-task
  workloads, generation-pinned grounder, run ledger, reports).
- ``okg.bench.plot`` / ``plot_full_history`` / ``per_source`` — plotting.
- ``okg.bench.views`` — non-LLM operator-view code the perf lab ships into
  generated deployments.

Heavy optional dependencies ride the ``[bench]`` extra (``pip install
'okg[bench]'``); modules that need them raise a loud install hint, never a
silent degrade.
"""
