# Commit-replay benchmark harness

Per the OpenSpec change `add-commit-replay-bench-harness`. Replays
the full (or sampled) git history of a target repo against a fresh
OKG substrate deployment and captures end-to-end per-commit
ingestion latency plus graph-growth metrics.

## Quick start

```bash
# Install bench deps:
uv pip install '.[bench]'

# List experiments in the manifest:
python bench/orchestrator.py list

# Run a single experiment end-to-end:
python bench/orchestrator.py run tinydb-files-only

# Run every experiment in the manifest:
python bench/orchestrator.py run --all

# Inspect the differential-engine sensitivity matrix:
python bench/orchestrator.py sensitivity

# Run one generated sensitivity cell:
python bench/orchestrator.py sensitivity --run --cell engsens-enall

# Generate per-experiment + cross-cutting PDFs:
python bench/orchestrator.py plot --all

# Drop all bench DBs + generated configs:
python bench/orchestrator.py clean
```

## Files

```
bench/
├── manifest.yaml                 the 12-experiment matrix
├── engine_sensitivity.yaml       sensitivity-test config for workload + engine axes
├── orchestrator.py               run / setup / plot CLI
├── extract.py                    raw CSV → standardized CSV
├── plot.py                       matplotlib → PDFs
├── style.py                      color palette + matplotlib rcParams
├── templates/                    Jinja: deployment.yaml.j2, source_registry.yaml.j2
├── deployments-generated/        per-experiment configs (gitignored)
├── results/                      per-experiment CSVs + worker logs (gitignored)
└── plots/                        PDFs (gitignored)
    ├── per_experiment/
    └── cross/
```

## Adding an experiment

Edit `manifest.yaml`. Each entry is one experiment:

```yaml
- id: my-new-bench
  repo:
    name: foo
    url: https://github.com/example/foo.git
  source_set: files+history         # or files-only or history-only
  replay: { strategy: full_history }
```

Then:

```bash
python bench/orchestrator.py run my-new-bench
```

## Replay strategies

- `full_history` — every commit reachable from HEAD via topo order. The
  initial commit serves as baseline; the rest are replayed.
- `head_only{last_n: K}` — last K commits via `HEAD~K..HEAD`.
- `first_n{n: K}` — first K commits chronologically (excluding the root,
  which is the baseline).
- `hybrid_head_then_stride{head_n: K1, total: K2}` — first K1 commits in
  detail, then stride to fill out to K2 total.
- `tail_window{last_n: K}` — replay the full chronological prefix from the
  root baseline, but record only the last K commits. Use this when comparing
  tail behavior with `full_history`; unlike `head_only`, the measured rows
  inherit the same accumulated graph/source-progress state shape.

The harness writes the explicit commit list to
`bench/deployments-generated/<id>/commits.txt` for every strategy.
`commit_replay.py` reads from there via the `OKG_BENCH_COMMITS_FILE` env var.

## Differential Engine Sensitivity

`bench/engine_sensitivity.yaml` defines a plan-first benchmark for the
differential engine. It builds on the tinydb commit-replay harness, but varies
workload shape and engine parameters from one config instead of adding
copy-pasted manifest rows.

The default mode is `one_factor_at_a_time`: the planner emits a baseline cell
plus one cell for each non-baseline axis value. That is the right default for a
sensitivity test because each delta is attributable and the matrix does not
explode into redundant Cartesian work. Set `mode: cartesian` only when you
intentionally want every workload/engine combination.

Workload axes live under `axes.workload`:

- `repo`: alternate repositories, each with `name` and `url`.
- `source_set`: source coverage such as `files-only`, `files+code`, or `tier1`.
- `replay`: replay strategy and size, for example `{strategy: first_n, n: 25}`.

Engine axes live under `axes.engine`:

- `enrichers`: `none`, `declarative`, `code`, `all`, or `default`.
- `views`: toggle native view materialization.
- `chronos_continuation_pct`: Chronos interval continuation percentage.
- `embedding`: toggle the embedding workflow when a real backend is configured.

`enrichers: default` follows the source-set default. Any explicit enricher
preset, including the shipped baseline's `none`, overrides the source-set
default so source coverage can be measured independently from enrichment cost.

Recognized knobs that are not deployment-wired are listed in
`unavailable_axes`. For example, `projection_batch` is documented there instead
of treated as a measured axis, because the current benchmark deployment template
does not pass it through to the publisher. Chronos branching is configured under
`reports.branching`, because it is a side workload run after a linear replay,
not a normal deployment config delta.

Commands:

```bash
# Write bench/results/engsens.sensitivity_plan.{json,md} and print the cells.
python bench/orchestrator.py sensitivity

# Machine-readable plan for CI or scripts.
python bench/orchestrator.py sensitivity --json

# Run the baseline plus every generated one-factor cell.
python bench/orchestrator.py sensitivity --run

# Run one cell while iterating.
python bench/orchestrator.py sensitivity --run --cell engsens-enall

# After a cell has been ingested, measure the branching side workload.
python bench/orchestrator.py sensitivity \
  --run-branching-report \
  --cell engsens-base
```
