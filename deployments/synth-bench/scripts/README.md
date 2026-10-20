# synth-bench scripts

Reusable runner for the synthetic commit-replay benchmark. Walks
through K commits of a real github repo via `git reset --hard`,
captures per-commit publisher metrics, and renders plots.

The benchmark answers: **"how fast does the publisher absorb each
commit, and where does the time go?"** Numbers from a fresh
`okg_bench` DB on pallets/click (149 files, 68 commits, May 2026):

  - publisher cycle work p50: **106 ms**, p95: **119 ms**
  - end-to-end commit→graph: **4.1 s** (fast path) / 49 s (cron-driven path)
  - throughput: **7.3 commits / minute**
  - source-adapter precision: `nodes_added == files_changed` for 87% of commits

## Files

- `commit_replay.py` — the replay runner. Reads commits from a
  local repo clone, resets HEAD to each, waits for a new
  generation, records metrics to CSV.
- `plot_bench.py` — emits a self-contained HTML page with 7
  SVG plots from the CSV. No matplotlib needed.

## One-time setup

```bash
# Pick a small public repo to replay. Shallow clone is fine.
git clone --depth 200 https://github.com/pallets/click \
  .scratch/bench-repos/click

# Create a fresh Postgres DB for the bench so its
# graph data doesn't mix with your live deployment.
PGPASSWORD=okg createdb -h localhost -p 5433 -U postgres okg_bench

# Install per-database extensions (timescaledb requires this on
# secondary DBs). pg_cron is OPTIONAL since the substrate
# retired its pg_cron usage in `retire_legacy_runtime`; the
# REQUIRED_EXTENSIONS check tolerates missing pg_cron as of
# commit b622ad0.
PGPASSWORD=okg psql -h localhost -p 5433 -U postgres -d okg_bench <<'SQL'
  CREATE EXTENSION IF NOT EXISTS timescaledb;
  CREATE EXTENSION IF NOT EXISTS vector;
  CREATE EXTENSION IF NOT EXISTS pg_textsearch;
  CREATE EXTENSION IF NOT EXISTS pg_trgm;
  CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
  CREATE EXTENSION IF NOT EXISTS btree_gist;
SQL

# Apply the substrate schema + DBOS bootstrap.
OKG_DSN=postgres://postgres:okg@localhost:5433/okg_bench \
  python -m okg.substrate.cli.main migrate \
    --deployment synth-bench --dsn "$OKG_DSN" --json

OKG_DSN=postgres://postgres:okg@localhost:5433/okg_bench \
  python -m okg.substrate.cli.main runtime bootstrap \
    --deployment synth-bench --dsn "$OKG_DSN" --json
```

## Run

```bash
# Start the synth-bench worker against okg_bench.
OKG_DSN=postgres://postgres:okg@localhost:5433/okg_bench \
  python -m okg.substrate.cli.main runtime worker \
    --deployment synth-bench \
    --dsn "$OKG_DSN" &

# Wait ~60s for the first source-poll cycle to discover the
# bench repo and publish the initial scan generation.

# Replay K commits and capture metrics.
OKG_BENCH_DSN=postgres://postgres:okg@localhost:5433/okg_bench \
OKG_BENCH_REPO=$PWD/.scratch/bench-repos/click \
OKG_BENCH_OUT=$PWD/.scratch \
  python deployments/synth-bench/scripts/commit_replay.py 30

# Render plots. Reads bench_results.csv from $OKG_BENCH_OUT.
OKG_BENCH_OUT=$PWD/.scratch \
  python deployments/synth-bench/scripts/plot_bench.py
open $PWD/.scratch/bench_plots.html
```

## Per-row CSV schema

| column | description |
|---|---|
| `i` | 1-based commit index in the replay |
| `commit` | first 10 chars of the commit SHA |
| `subject` | commit message subject (first 60 chars) |
| `files_changed` | `git diff --name-only PREV HEAD` line count |
| `lines_added` | from `git diff --shortstat` |
| `lines_deleted` | same |
| `end_to_end_s` | wall-clock from `git reset` until a new generation is published |
| `cycle_ms` | `published_at - created_at` on the resulting generation row |
| `nodes_added` | `count(*) FROM okg.nodes_live WHERE valid_from_gen=<gen>` |
| `edges_added` | `count(*) FROM okg.edges_live WHERE valid_from_gen=<gen>` |

## Plots emitted

1. End-to-end ingestion time per commit (line)
2. Publisher cycle work (ms) per commit (line)
3. Ingestion time vs files changed (scatter, exposes merge-commit outliers)
4. Source-adapter correctness: `nodes_added` vs `files_changed` (scatter with `y = x` reference)
5. Cycle work vs lines added (scatter)
6. Publisher cycle work distribution (histogram, p50/p95 markers)
7. End-to-end latency distribution (histogram, p50/p95 markers)

## Caveats

- The benchmark uses a separate `okg_bench` DB so its 6 M-edge live
  table doesn't pollute your live deployment. If you re-run against
  `okg` directly, expect cycle times to be ~50× slower due to the
  shared-table retraction-sweep tax — see commit `b622ad0`'s message.
- Merge commits with 0 file diffs produce a slow-path latency (~50 s
  to next cron tick) because the fs-watcher doesn't fire. This is
  visible as a bimodal pattern in plot #1 and plot #7.
- Source-adapter drift: ~13% of commits show
  `nodes_added != files_changed`. Most are no-op merges (0/0 vs
  expected 0/n) or compaction folding adjacent fs-events. Worth a
  follow-up if precision matters.
