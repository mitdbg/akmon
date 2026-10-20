"""Commit-replay benchmark runner for the synth-bench deployment.

For each of K commits (most recent K from a local clone of any
github repo), reset HEAD to that commit, wait for the worker to
publish a new generation, then record per-commit metrics into a
CSV row. See ``deployments/synth-bench/scripts/README.md`` for the
full how-to.

Captured per commit:
  - commit hash + short subject
  - files_changed, lines_added, lines_deleted (from git diff)
  - end_to_end_s (wall-clock from `git reset` to new gen published)
  - cycle_ms (generation.published_at - generation.created_at)
  - nodes_added / edges_added

Environment (all required unless noted):
  OKG_BENCH_DSN       Postgres DSN for the bench DB
                      (e.g. postgres://postgres:okg@localhost:5433/okg_bench)
  OKG_BENCH_REPO      Absolute path to the local clone whose
                      commit history we walk.
  OKG_BENCH_DEPLOYMENT  Deployment name (default: synth-bench).
  OKG_BENCH_OUT       Directory to write bench_results.csv
                      (default: ./.scratch).
  OKG_BENCH_COMMITS_FILE  Optional newline-delimited commit SHA list.
                      Used by bench/orchestrator.py replay strategies.
  OKG_BENCH_BASELINE_COMMIT  Optional explicit baseline SHA. Required
                      when the commit list does not start at repo root.
  OKG_BENCH_RECORD_FROM_INDEX  Optional 1-indexed replay position where
                      CSV recording starts. Earlier commits are warmup:
                      they advance repo/DB state but are not written as
                      measured rows.
  OKG_BENCH_DRAIN_BETWEEN_COMMITS  Optional "1" to wait for the DBOS
                      source-sync queue to drain between replay commits.
  OKG_BENCH_DRAIN_BETWEEN_COMMITS_MODE
                      Optional "semantic" (default) or "strict". Semantic
                      drain waits for successor queued work after the observed
                      publish, not for the already-published workflow's final
                      DBOS terminal update.

Usage:
  OKG_BENCH_DSN=... OKG_BENCH_REPO=... \\
    python deployments/synth-bench/scripts/commit_replay.py [K]
"""
from __future__ import annotations
import csv
import hashlib
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg
import pygit2
from psycopg import sql


def _env(name: str, default: str | None = None) -> str:
    val = os.environ.get(name, default)
    if val is None:
        raise SystemExit(f"missing required env var: {name}")
    return val


BENCH_REPO = Path(_env("OKG_BENCH_REPO")).resolve()
DSN = _env("OKG_BENCH_DSN")
DEPLOYMENT = _env("OKG_BENCH_DEPLOYMENT", "synth-bench")
OUT_DIR = Path(_env("OKG_BENCH_OUT", str(Path.cwd() / ".scratch")))
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / "bench_results.csv"
OUT_DELTA_PROOF = OUT_DIR / "delta_proof.json"
K = int(sys.argv[1]) if len(sys.argv) > 1 else 30
RECORD_FROM_INDEX = int(os.environ.get("OKG_BENCH_RECORD_FROM_INDEX", "1"))
if RECORD_FROM_INDEX <= 0:
    raise SystemExit("OKG_BENCH_RECORD_FROM_INDEX must be a positive integer")
DELTA_PROOF_ENABLED = os.environ.get("OKG_BENCH_DELTA_PROOF") == "1"

# Single pygit2 Repository handle for the whole replay. Each access
# is sub-millisecond vs ~15 ms per fork+exec for `subprocess git`.
# Per `add-pygit2-in-process-git-helpers`: the bench's own
# file_count_diff / line_delta / subject / reset_to calls used to
# add ~60 ms/commit to the measured wall — pure instrumentation
# overhead unrelated to substrate work. With pygit2 they drop to
# under 1 ms total.
REPO = pygit2.Repository(str(BENCH_REPO))


# A single autocommit psycopg connection for ALL bench-side reads.
#
# Prior behavior: `q()` / `q1()` opened a fresh `psycopg.connect(DSN)`
# per call. The replay's hot paths poll `latest_gen()` (every 10 ms
# until the new gen appears) and `queue_depth()` (every 10 ms until
# the partition queue drains). Per commit that's 10–30 connects ×
# ~10 ms TCP+TLS = 100–300 ms of pure bench-wrap overhead added on
# top of the substrate's actual cycle time, inflating `end_to_end_s`.
#
# A single persistent connection is fine because:
#   * The replay is strictly single-threaded.
#   * All bench queries are read-only against the worker's DB.
#   * autocommit=True means each `cur.execute` is its own implicit
#     short txn — no risk of a long-held snapshot blocking VACUUM.
_CONN: psycopg.Connection | None = None
_LISTEN_CONN: psycopg.Connection | None = None


def _conn() -> psycopg.Connection:
    global _CONN
    if _CONN is None or _CONN.closed:
        _CONN = psycopg.connect(DSN, autocommit=True)
    return _CONN


def generation_published_channel(deployment_name: str) -> str:
    raw = f"okg_generation_{deployment_name}_published"
    if (
        len(raw.encode("utf-8")) <= 63
        and not any(ch in deployment_name for ch in "/\\\0\r\n\t ")
    ):
        return raw
    digest = hashlib.sha256(deployment_name.encode("utf-8")).hexdigest()[:16]
    return f"okg_generation_{digest}_published"


def _listen_conn() -> psycopg.Connection:
    global _LISTEN_CONN
    if _LISTEN_CONN is None or _LISTEN_CONN.closed:
        _LISTEN_CONN = psycopg.connect(DSN, autocommit=True)
        with _LISTEN_CONN.cursor() as cur:
            cur.execute(
                sql.SQL("LISTEN {}").format(
                    sql.Identifier(generation_published_channel(DEPLOYMENT)),
                ),
            )
    return _LISTEN_CONN


def _next_generation_notify(timeout_s: float):
    return next(
        _listen_conn().notifies(timeout=timeout_s, stop_after=1),
        None,
    )


def _drain_generation_notifications() -> int:
    drained = 0
    while _next_generation_notify(0.0) is not None:
        drained += 1
    return drained


def q(sql, params=()):
    with _conn().cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def q1(sql, params=()):
    rows = q(sql, params)
    return rows[0][0] if rows else None


def _parse_ts(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _epoch_ms(value) -> int | None:
    parsed = _parse_ts(value)
    if parsed is None:
        return None
    return int(parsed.timestamp() * 1000)


def git(*args, check=True):
    """Subprocess git — kept for the rare write/admin operations we
    don't have a pygit2 equivalent for. The hot-path reads
    (file_count_diff, line_delta, subject, reset_to) now use the
    in-process helpers below."""
    res = subprocess.run(
        ["git", *args],
        cwd=BENCH_REPO,
        check=False,
        capture_output=True,
        text=True,
    )
    if check and res.returncode != 0:
        raise subprocess.CalledProcessError(
            res.returncode, res.args, res.stdout, res.stderr,
        )
    return res


def reset_to(commit: str, *, retries: int = 2):
    """Idempotently `git reset --hard <commit>` via pygit2.

    Retries on transient failures (e.g., `.git/index.lock` collision
    with the watcher's HEAD-poller).
    """
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            REPO.reset(commit, pygit2.GIT_RESET_HARD)
            return
        except pygit2.GitError as e:
            last_err = e
            if attempt < retries:
                time.sleep(0.05)
                continue
    sys.stderr.write(
        f"pygit2 reset --hard {commit} failed after {retries+1} attempts: "
        f"{last_err}\n"
    )
    error = last_err
    if error is None:
        raise RuntimeError(f"pygit2 reset --hard {commit} failed")
    raise error


def _resolve(ref: str) -> pygit2.Object:
    """Resolve a ref string (sha, `sha~1`, etc.) to a git object.

    pygit2's `repo[sha]` only takes hex/Oid; the bench's
    `prev_commit = f"{commits[0]}~1"` baseline uses the parent
    revision syntax. `revparse_single` handles it like git CLI.
    """
    return REPO.revparse_single(ref)


def _diff_stats(a: str, b: str) -> tuple[int, int, int]:
    """Return (files_changed, insertions, deletions) between two
    commit SHAs. In-process tree diff via pygit2."""
    tree_a = _resolve(a).peel(pygit2.Tree)
    tree_b = _resolve(b).peel(pygit2.Tree)
    diff = tree_a.diff_to_tree(tree_b)
    stats = diff.stats
    return stats.files_changed, stats.insertions, stats.deletions


def file_count_diff(a: str, b: str) -> int:
    """Return number of paths that changed between commits a and b."""
    return _diff_stats(a, b)[0]


def line_delta(a: str, b: str):
    """Returns (lines_added, lines_deleted) between commits a and b."""
    _, ins, dels = _diff_stats(a, b)
    return ins, dels


def subject(commit: str) -> str:
    """Return the commit subject line (first line of the message)."""
    msg = REPO[commit].message or ""
    return msg.split("\n", 1)[0][:60]


def latest_gen():
    chronos = None
    try:
        chronos = q1(
            """
            SELECT checkpoint_id
              FROM public._chronos_branch_interval_checkpoints
             WHERE metadata::jsonb->>'kind' = 'okg_generation'
               AND metadata::jsonb->>'deployment_name' = %s
               AND metadata::jsonb->>'status' = 'published'
             ORDER BY
               NULLIF(metadata::jsonb->>'target_generation_id', '')::numeric DESC,
               created_at DESC,
               checkpoint_id DESC
             LIMIT 1
            """,
            (DEPLOYMENT,),
        )
    except psycopg.Error:
        # Legacy deployments do not have Chronos metadata tables.
        chronos = None
    if chronos is not None:
        return str(chronos)
    return q1(
        "SELECT max(generation_id) FROM okg.graph_generations "
        " WHERE deployment_name=%s AND status='published'",
        (DEPLOYMENT,),
    )


def wait_for_next_gen(prev_gen, deadline: float):
    """Wait for a published generation after ``prev_gen``.

    Prefer the publisher's atomic Postgres NOTIFY signal. Keep a slow polling
    fallback so the replay still works if an old deployment lacks generation
    notifications or a best-effort NOTIFY is lost.
    """

    notify_count = 0
    poll_count = 0
    query_ms = 0.0
    query_max_ms = 0.0
    strategy = None
    prev_text = str(prev_gen)
    while time.time() < deadline:
        timeout_s = min(max(deadline - time.time(), 0.0), 0.25)
        notify = _next_generation_notify(timeout_s)
        if notify is not None:
            notify_count += 1
            payload = str(notify.payload)
            if payload and payload != prev_text:
                strategy = "notify"
                return (
                    payload,
                    notify_count,
                    poll_count,
                    query_ms,
                    query_max_ms,
                    strategy,
                )

        poll_t0 = time.perf_counter()
        gen = latest_gen()
        elapsed = (time.perf_counter() - poll_t0) * 1000
        poll_count += 1
        query_ms += elapsed
        query_max_ms = max(query_max_ms, elapsed)
        if gen != prev_gen:
            strategy = "poll"
            return (
                gen,
                notify_count,
                poll_count,
                query_ms,
                query_max_ms,
                strategy,
            )

    return (
        None,
        notify_count,
        poll_count,
        query_ms,
        query_max_ms,
        strategy,
    )


def queue_depth() -> int:
    """How many source_sync workflows are still ENQUEUED or PENDING
    on the partition queue. Used for opt-in throttling between bench
    commits — if we fire commits faster than the worker drains the
    queue, the bench's "latest_gen advanced" signal records the
    NEXT-IN-QUEUE workflow's gen rather than this commit's gen,
    grossly understating real per-commit latency.

    See `OKG_BENCH_DRAIN_BETWEEN_COMMITS` env var for usage.
    """
    return q1(
        "SELECT count(*) FROM dbos.workflow_status "
        " WHERE name = 'okg_source_sync' "
        "   AND status IN ('ENQUEUED', 'PENDING')",
    ) or 0


def generation_published_epoch_ms(gen_id: int | str) -> int | None:
    if isinstance(gen_id, str) and gen_id.startswith("okg:"):
        value = q1(
            """
            SELECT created_at
              FROM public._chronos_branch_interval_checkpoints
             WHERE checkpoint_id = %s
             LIMIT 1
            """,
            (gen_id,),
        )
        return _epoch_ms(value)
    value = q1(
        "SELECT published_at FROM okg.graph_generations WHERE generation_id=%s",
        (gen_id,),
    )
    return _epoch_ms(value)


def semantic_successor_queue_depth(published_epoch_ms: int | None) -> int:
    """Active source-sync work that can still publish after observed gen.

    The current workflow can remain PENDING for tens of milliseconds after its
    final generation is visible while DBOS records terminal bookkeeping. That
    is not successor graph work. ENQUEUED rows and PENDING rows that started
    after the observed generation publish still can rewrite the graph for the
    same replay commit, so the harness continues to wait for them.
    """
    if published_epoch_ms is None:
        return queue_depth()
    rows = q(
        """
        SELECT status, started_at_epoch_ms
          FROM dbos.workflow_status
         WHERE name = 'okg_source_sync'
           AND status IN ('ENQUEUED', 'PENDING')
         ORDER BY created_at, workflow_uuid
        """,
    )
    successor_count = 0
    for status, started_at in rows:
        if (
            status == "PENDING"
            and started_at is not None
            and int(started_at) <= published_epoch_ms
        ):
            continue
        successor_count += 1
    return successor_count


def gen_metrics(gen_id: int | str):
    """Pull metrics for the given published generation.

    Per `add-delta-proportional-substrate-work`: reads `nodes_added`
    and `edges_added` off `okg.graph_generations` directly. The
    publisher writes these values at commit time from its in-memory
    `PhaseAResult.delta`. The previous implementation issued two
    `SELECT count(*) FROM okg.{nodes,edges}_live WHERE
    valid_from_gen = %s` subqueries — each a Parallel Seq Scan on
    an unindexed predicate. Now a single-row indexed read from the
    `graph_generations` primary key.
    """
    if isinstance(gen_id, str) and gen_id.startswith("okg:"):
        rows = q(
            """
            SELECT metadata::jsonb, created_at
              FROM public._chronos_branch_interval_checkpoints
             WHERE checkpoint_id = %s
             LIMIT 1
            """,
            (gen_id,),
        )
        if not rows:
            return None
        metadata, checkpoint_created = rows[0]
        meta_created = _parse_ts(metadata.get("created_at"))
        published = _parse_ts(checkpoint_created)
        cycle_ms = 0
        if meta_created is not None and published is not None:
            cycle_ms = int((published - meta_created).total_seconds() * 1000)
        return {
            "cycle_ms": cycle_ms,
            "nodes_added": metadata.get("nodes_added", metadata.get("node_count")),
            "edges_added": metadata.get("edges_added", metadata.get("edge_count")),
            # console-count-rollup (#444 slice 2): the maintained-derivation marginal-
            # cost summary the publisher stamps (mode / forcing_reason /
            # dirty_bucket_count = rows-touched). Only the chronos-native arm reads
            # checkpoint metadata, so the bench must run chronos-native to populate the
            # maintained_derivation_scope boundary (None when the flag was off).
            "console_count_rollup": metadata.get("console_count_rollup"),
            "invariant_summary": metadata.get("invariant_summary"),
            "projection_deltas": metadata.get("projection_deltas"),
            "enricher_governance": metadata.get("enricher_governance"),
            "maintained_derivations": metadata.get("maintained_derivations"),
        }
    rows = q(
        "SELECT to_timestamp(extract(epoch from created_at))::time, "
        "       to_timestamp(extract(epoch from published_at))::time, "
        "       extract(epoch from "
        "         (published_at - created_at)) * 1000 AS cycle_ms, "
        "       nodes_added, "
        "       edges_added "
        "  FROM okg.graph_generations "
        " WHERE generation_id=%s",
        (gen_id,),
    )
    if not rows:
        return None
    created_t, published_t, cycle_ms, nodes, edges = rows[0]
    return {
        "cycle_ms": int(cycle_ms or 0),
        "nodes_added": nodes,
        "edges_added": edges,
    }


def db_size_bytes() -> int:
    """Current total Postgres database size in bytes. Captured per
    commit so the plot suite can show DB growth over the replay."""
    return q1("SELECT pg_database_size(current_database())") or 0


def _commit_selection() -> tuple[list[str], str, str]:
    """Return the replay commit list, baseline ref, and display label."""
    commits_file = os.environ.get("OKG_BENCH_COMMITS_FILE")
    if commits_file:
        commits = [
            ln.strip()
            for ln in Path(commits_file).read_text().splitlines()
            if ln.strip()
        ]
        if not commits:
            raise SystemExit(f"no commits found in {commits_file}")
        baseline = os.environ.get("OKG_BENCH_BASELINE_COMMIT")
        if baseline:
            baseline = baseline.strip()
            return commits, baseline, f"{baseline[:10]} (from orchestrator)"
        root_rows = git(
            "rev-list", "--max-parents=0", "HEAD",
        ).stdout.strip().split("\n")
        root = next((r for r in root_rows if r), None)
        if not root:
            raise SystemExit("could not derive repo root baseline")
        return commits, root, f"{root[:10]} (root, derived)"

    if os.environ.get("OKG_BENCH_FULL_HISTORY") == "1":
        rows = git(
            "log", "--reverse", "--topo-order", "--pretty=%H",
        ).stdout.strip().split("\n")
        all_commits = [c for c in rows if c]
        if len(all_commits) < 2:
            raise SystemExit("full-history replay needs at least 2 commits")
        return all_commits[1:], all_commits[0], f"{all_commits[0][:10]} (root)"

    if K <= 0:
        raise SystemExit(
            "legacy positional replay needs K > 0; set "
            "OKG_BENCH_COMMITS_FILE for orchestrated replay"
        )
    rows = git(
        "log", "--reverse", "--pretty=%H", f"HEAD~{K}..HEAD",
    ).stdout.strip().split("\n")
    commits = [c for c in rows if c]
    if not commits:
        raise SystemExit(f"no commits found for HEAD~{K}..HEAD")
    return commits, f"{commits[0]}~1", f"{commits[0][:10]}~1"


def _load_delta_proof_api():
    """Import bench proof helpers when proof emission is enabled.

    okg.bench ships in the installed package (PACT ship-okg-bench), so no
    sys.path surgery is needed.
    """
    from okg.bench.differential_replay_proof import (  # noqa: PLC0415
        build_delta_proof,
        build_delta_proof_row,
        changed_paths_between,
        chronos_dirty_scope_boundary_from_projection_deltas,
        consistency_gate_boundary_from_invariant_summary,
        enricher_scope_boundary_from_governance,
        maintained_derivation_boundary,
        write_delta_proof,
    )

    return (
        build_delta_proof,
        build_delta_proof_row,
        changed_paths_between,
        write_delta_proof,
        maintained_derivation_boundary,
        chronos_dirty_scope_boundary_from_projection_deltas,
        consistency_gate_boundary_from_invariant_summary,
        enricher_scope_boundary_from_governance,
    )


def trigger_manual_sync(changed_paths):
    """Deterministically enqueue exactly ONE source-sync for this commit.

    Replaces the filesystem watcher (disabled in the registry: git-files now
    declares only `[manual, reconcile]`). A single `git reset` otherwise
    produced a racy burst of fs-event syncs — one per changed file PLUS a junk
    `.git/HEAD` sync — that serialized on the concurrency-1 source-sync queue
    and showed up as multi-second "queue drain". One explicit manual job,
    scoped to the commit's changed paths and fanned out to dependents, gives
    one sync per source with the COMPLETE per-commit scope (so no partial-scope
    admission rejection), deterministically (no debounce/compaction race).
    Returns the enqueued workflow id.
    """
    from okg.substrate.source_sync import SourceSyncEvent, SourceSyncJob
    from okg.substrate.source_sync_enqueue import enqueue_source_sync_via_dbos

    paths = [str(p) for p in (changed_paths or ()) if p]
    if paths:
        events = tuple(
            SourceSyncEvent(
                deployment=DEPLOYMENT,
                source_name="git-files",
                trigger_kind="manual",
                scope_key=p,
            )
            for p in paths
        )
        run_mode = "scope_incomplete"
    else:
        # No changed paths in scope (e.g. empty commit) — fall back to a full
        # scope so the cursor still advances deterministically.
        events = ()
        run_mode = "scope_complete"
    job = SourceSyncJob(
        deployment=DEPLOYMENT,
        sources=("git-files",),
        run_mode=run_mode,
        events=events,
        include_predecessors=True,
        include_dependents=True,
        dependent_fanout="closure",
        publish=True,
    )
    return enqueue_source_sync_via_dbos(deployment=DEPLOYMENT, dsn=DSN, job=job)


def main():
    # Three commit-selection modes:
    #   1. `OKG_BENCH_COMMITS_FILE` -- orchestrator-provided commit SHAs.
    #   2. `OKG_BENCH_FULL_HISTORY=1` -- every reachable commit after root.
    #   3. Legacy `HEAD~K..HEAD` -- slice the tip K commits.
    commits, baseline_commit, baseline_label = _commit_selection()
    if RECORD_FROM_INDEX > len(commits):
        raise SystemExit(
            "OKG_BENCH_RECORD_FROM_INDEX "
            f"({RECORD_FROM_INDEX}) exceeds replay commit count "
            f"({len(commits)})"
        )
    print(f"Replaying {len(commits)} commits "
          f"({commits[0][:10]} .. {commits[-1][:10]})")
    if RECORD_FROM_INDEX > 1:
        print(
            f"Recording measured rows from replay index "
            f"{RECORD_FROM_INDEX}; earlier commits are warmup only"
        )

    HEAD = git("rev-parse", "HEAD").stdout.strip()
    # Set baseline. (Often a no-op if the orchestrator pre-reset us.)
    reset_to(baseline_commit)
    print(
        f"Reset to baseline {baseline_label}; waiting for the worker's "
        "initial-scan publish + queue drain..."
    )
    # Poll for the worker's first published generation, then wait for
    # the source_sync queue to drain. Replaces a blind 90s sleep: on
    # the baseline state (a single root commit) the worker publishes
    # in seconds, not minutes.
    baseline_deadline = time.time() + 180
    baseline_gen: int | None = None
    while time.time() < baseline_deadline:
        g = latest_gen()
        if g is not None:
            baseline_gen = g
            break
        time.sleep(0.5)
    if baseline_gen is None:
        raise SystemExit(
            "worker never published a baseline generation within 180s; "
            "check the worker log"
        )
    # Now drain: if the watcher fired a couple of redundant syncs while
    # we were polling, give them time to complete before commit #1.
    drain_deadline = time.time() + 60
    while time.time() < drain_deadline:
        if queue_depth() == 0:
            break
        time.sleep(0.2)
    # Re-check latest_gen — drain may have produced additional gens we
    # should count as part of the baseline.
    baseline_gen = latest_gen() or baseline_gen
    print(f"Baseline gen: {baseline_gen}\n")

    header = [
        "i", "commit", "subject",
        "files_changed", "lines_added", "lines_deleted",
        "end_to_end_s", "cycle_ms",
        "nodes_added", "edges_added",
        # Recording the actual gen_id per commit row means downstream
        # extract.py doesn't have to pair positionally with the
        # baseline (which may produce >1 gen if the worker scans a
        # multi-source baseline).
        "gen_id",
        # Previous published generation immediately before this commit.
        # Needed for tail-window extraction so the source timing window
        # starts at the prefix generation, not at the root baseline.
        "prev_gen_id",
        # Per-commit Postgres DB size in bytes (pg_database_size).
        # Captured immediately after the new gen is observed; lets
        # the plot suite render growth over the replay.
        "db_size_bytes",
        # Benchmark-side timing attribution. These are not substrate
        # timings, but they explain how much of the replay wall is reset,
        # polling, optional queue drain, and metrics reads.
        "bench_reset_started_epoch_ms",
        "bench_reset_completed_epoch_ms",
        "bench_prev_gen_query_ms",
        "bench_reset_ms",
        "bench_wait_new_gen_ms",
        "bench_generation_notify_count",
        "bench_generation_wait_strategy",
        "bench_latest_gen_poll_count",
        "bench_latest_gen_query_ms",
        "bench_latest_gen_query_max_ms",
        "bench_queue_drain_ms",
        "bench_queue_poll_count",
        "bench_queue_query_ms",
        "bench_gen_metrics_ms",
        "bench_db_size_ms",
        # Inter-commit preamble (diffs + delta-proof changed_paths +
        # notification drain) run while the worker is idle, BEFORE reset.
        # Previously untimed — it is the bulk of the extract-side residual,
        # so name it instead of leaving it a gap.
        "bench_preamble_ms",
        # Wall-clock epoch (ms) when the bench OBSERVED the new published
        # generation (via NOTIFY). Lets extract.py anchor the e2e window on
        # what the bench actually measured rather than the checkpoint
        # created_at (which lags by the post-NOTIFY publish tail and leaks
        # wall-clock across adjacent commit windows).
        "bench_gen_observed_epoch_ms",
    ]
    # Write the baseline gen so extract.py knows where the bench
    # starts (any gen <= baseline_gen is pre-bench).
    (OUT_DIR / "baseline_gen.txt").write_text(str(baseline_gen) + "\n")
    out_rows = []
    proof_rows = []
    proof_api = _load_delta_proof_api() if DELTA_PROOF_ENABLED else None
    print(("{:>3}  {:>10}  {:>5}  {:>5}  {:>5}  {:>9}  {:>8}  "
           "{:>5}  {:>5}  subject").format(
        "i", "commit", "fchg", "ladd", "ldel",
        "e2e_s", "cycle_ms", "nodes", "edges",
    ))

    prev_commit = baseline_commit
    for i, c in enumerate(commits, 1):
        record_row = i >= RECORD_FROM_INDEX
        prev_gen_t0 = time.perf_counter()
        prev_gen = latest_gen()
        prev_gen_query_ms = (time.perf_counter() - prev_gen_t0) * 1000
        files = file_count_diff(prev_commit, c)
        ladd, ldel = line_delta(prev_commit, c)
        subj = subject(c)
        changed_paths = (
            proof_api[2](BENCH_REPO, prev_commit, c)
            if proof_api is not None
            else ()
        )
        _drain_generation_notifications()
        # Whole inter-commit preamble (prev_gen query + file/line diffs +
        # delta-proof changed_paths + notification drain) ran while the worker
        # was idle, before the reset clock starts. Timing it names the residual
        # the extract-side window otherwise leaves as an unattributed gap.
        preamble_ms = (time.perf_counter() - prev_gen_t0) * 1000
        reset_t0 = time.time()
        reset_perf_t0 = time.perf_counter()
        reset_to(c)
        reset_completed_epoch_ms = int(time.time() * 1000)
        reset_ms = (time.perf_counter() - reset_perf_t0) * 1000
        # Deterministic one-sync-per-commit: enqueue the manual source-sync now
        # that the working tree is at the new commit (watcher is disabled).
        source_sync_workflow_ref = trigger_manual_sync(changed_paths)
        deadline = reset_t0 + 300
        wait_perf_t0 = time.perf_counter()
        (
            new_gen,
            generation_notify_count,
            latest_gen_poll_count,
            latest_gen_query_ms,
            latest_gen_query_max_ms,
            generation_wait_strategy,
        ) = wait_for_next_gen(prev_gen, deadline)
        wait_new_gen_ms = (time.perf_counter() - wait_perf_t0) * 1000
        # Epoch at which the bench actually observed the new gen (via NOTIFY).
        # extract.py anchors a leak-free e2e window on this instead of the
        # checkpoint created_at, which lags by the post-NOTIFY publish tail.
        gen_observed_epoch_ms = (
            int(time.time() * 1000) if new_gen is not None else None
        )
        queue_drain_ms = 0.0
        queue_poll_count = 0
        queue_query_ms = 0.0
        if new_gen is None:
            print(f"{i:>3}  {c[:10]:>10}  TIMEOUT")
            if not record_row:
                raise SystemExit(
                    f"warmup commit {i} ({c[:10]}) timed out; "
                    "tail-window state is not valid"
                )
            out_rows.append({
                "i": i, "commit": c[:10], "subject": subj,
                "files_changed": files,
                "lines_added": ladd, "lines_deleted": ldel,
                "end_to_end_s": None, "cycle_ms": None,
                "nodes_added": None, "edges_added": None,
                "gen_id": None,
                "prev_gen_id": prev_gen,
                "db_size_bytes": None,
                "bench_reset_started_epoch_ms": int(reset_t0 * 1000),
                "bench_reset_completed_epoch_ms": reset_completed_epoch_ms,
                "bench_prev_gen_query_ms": round(prev_gen_query_ms, 3),
                "bench_reset_ms": round(reset_ms, 3),
                "bench_wait_new_gen_ms": round(wait_new_gen_ms, 3),
                "bench_generation_notify_count": generation_notify_count,
                "bench_generation_wait_strategy": (
                    generation_wait_strategy or ""
                ),
                "bench_latest_gen_poll_count": latest_gen_poll_count,
                "bench_latest_gen_query_ms": round(latest_gen_query_ms, 3),
                "bench_latest_gen_query_max_ms": round(
                    latest_gen_query_max_ms, 3,
                ),
                "bench_queue_drain_ms": None,
                "bench_queue_poll_count": queue_poll_count,
                "bench_queue_query_ms": round(queue_query_ms, 3),
                "bench_gen_metrics_ms": None,
                "bench_db_size_ms": None,
                "bench_preamble_ms": round(preamble_ms, 3),
                "bench_gen_observed_epoch_ms": None,
            })
            if proof_api is not None:
                proof_rows.append(proof_api[1](
                    i=i,
                    commit_sha=c,
                    previous_commit_sha=prev_commit,
                    subject=subj,
                    changed_paths=changed_paths,
                    line_delta={
                        "files_changed": files,
                        "lines_added": ladd,
                        "lines_deleted": ldel,
                    },
                    previous_generation_id=prev_gen,
                    generation_id=None,
                    generation_metrics=None,
                    end_to_end_s=None,
                    db_size_bytes=None,
                    source_sync_workflow_ref=str(source_sync_workflow_ref),
                ))
            prev_commit = c
            continue
        # Wait for the commit's source-sync work to FULLY COMPLETE before
        # recording the row and moving to the next commit. Without this, the
        # row captures the first (layer-1 git-files) publish after the reset
        # while the successor layers (code-structure, code-lsp/pyright) are
        # still running; the next commit's reset then fires and its workflow
        # queues behind the still-running one (the partition queue is
        # concurrency=1), so the prior commit's successor work is
        # mis-attributed to the next commit's e2e (measured: heavy commits
        # inflated ~2s by the prior commit's tail).
        #
        # Default "strict" waits for queue_depth()==0 (the workflow reaches a
        # terminal state) => clean per-commit attribution, at the cost of
        # including the workflow's ~250ms DBOS terminal-bookkeeping tail. The
        # legacy "semantic" mode excludes any PENDING workflow that *started
        # before* the observed gen — which wrongly skips the ENTIRE successor
        # run (it starts before the layer-1 gen), not just the terminal tail —
        # so it UNDERSTATES the current commit and pushes the work onto the
        # next commit. Do not use "semantic" for attribution.
        if os.environ.get("OKG_BENCH_DRAIN_BETWEEN_COMMITS") == "1":
            drain_mode = os.environ.get(
                "OKG_BENCH_DRAIN_BETWEEN_COMMITS_MODE",
                "strict",
            ).strip().lower()
            published_epoch_ms = generation_published_epoch_ms(new_gen)
            drain_perf_t0 = time.perf_counter()
            drain_deadline = time.time() + 30
            while time.time() < drain_deadline:
                queue_t0 = time.perf_counter()
                if drain_mode == "strict":
                    depth = queue_depth()
                else:
                    depth = semantic_successor_queue_depth(published_epoch_ms)
                queue_query_ms += (time.perf_counter() - queue_t0) * 1000
                queue_poll_count += 1
                if depth <= 0:
                    break
                time.sleep(0.01)
            queue_drain_ms = (time.perf_counter() - drain_perf_t0) * 1000
            new_gen = latest_gen() or new_gen
        # 3-decimal precision (ms) since we now poll fast enough for it
        # to be meaningful. Includes optional queue drain when enabled.
        e2e_s = round(time.time() - reset_t0, 3)
        if record_row:
            gen_metrics_t0 = time.perf_counter()
            m = gen_metrics(new_gen) or {}
            gen_metrics_ms = (time.perf_counter() - gen_metrics_t0) * 1000
            db_size_t0 = time.perf_counter()
            db_size = db_size_bytes()
            db_size_ms = (time.perf_counter() - db_size_t0) * 1000
            row = {
                "i": i,
                "commit": c[:10],
                "subject": subj,
                "files_changed": files,
                "lines_added": ladd,
                "lines_deleted": ldel,
                "end_to_end_s": e2e_s,
                "cycle_ms": m.get("cycle_ms"),
                "nodes_added": m.get("nodes_added"),
                "edges_added": m.get("edges_added"),
                "gen_id": new_gen,
                "prev_gen_id": prev_gen,
                "db_size_bytes": db_size,
                "bench_reset_started_epoch_ms": int(reset_t0 * 1000),
                "bench_reset_completed_epoch_ms": reset_completed_epoch_ms,
                "bench_prev_gen_query_ms": round(prev_gen_query_ms, 3),
                "bench_reset_ms": round(reset_ms, 3),
                "bench_wait_new_gen_ms": round(wait_new_gen_ms, 3),
                "bench_generation_notify_count": generation_notify_count,
                "bench_generation_wait_strategy": (
                    generation_wait_strategy or ""
                ),
                "bench_latest_gen_poll_count": latest_gen_poll_count,
                "bench_latest_gen_query_ms": round(latest_gen_query_ms, 3),
                "bench_latest_gen_query_max_ms": round(
                    latest_gen_query_max_ms, 3,
                ),
                "bench_queue_drain_ms": round(queue_drain_ms, 3),
                "bench_queue_poll_count": queue_poll_count,
                "bench_queue_query_ms": round(queue_query_ms, 3),
                "bench_gen_metrics_ms": round(gen_metrics_ms, 3),
                "bench_db_size_ms": round(db_size_ms, 3),
                "bench_preamble_ms": round(preamble_ms, 3),
                "bench_gen_observed_epoch_ms": gen_observed_epoch_ms,
            }
            out_rows.append(row)
            if proof_api is not None:
                chronos_dirty_scope = proof_api[5](
                    m.get("projection_deltas")
                )
                dirty_scope_count = None
                if chronos_dirty_scope.get("status") == "available":
                    dirty_scope_count = (
                        int(chronos_dirty_scope.get("dirty_node_count") or 0)
                        + int(chronos_dirty_scope.get("dirty_edge_count") or 0)
                    )
                proof_rows.append(proof_api[1](
                    i=i,
                    commit_sha=c,
                    previous_commit_sha=prev_commit,
                    subject=subj,
                    changed_paths=changed_paths,
                    line_delta={
                        "files_changed": files,
                        "lines_added": ladd,
                        "lines_deleted": ldel,
                    },
                    previous_generation_id=prev_gen,
                    generation_id=new_gen,
                    generation_metrics=m,
                    end_to_end_s=e2e_s,
                    db_size_bytes=row["db_size_bytes"],
                    # console-count-rollup (#444 slice 2): populate the maintained-
                    # derivation marginal-cost boundary from the gen's stamped summary
                    # (available on a maintained cycle, loud-unavailable otherwise) --
                    # NOT a dead boundary like native_view_scope / derived_relation_scope.
                    maintained_derivation_scope=proof_api[4](
                        m.get("console_count_rollup")
                    ),
                    chronos_dirty_scope=chronos_dirty_scope,
                    gate_scope=proof_api[6](
                        m.get("invariant_summary")
                    ),
                    enricher_scope=proof_api[7](
                        m.get("enricher_governance"),
                        dirty_scope_count=dirty_scope_count,
                    ),
                    source_sync_workflow_ref=str(source_sync_workflow_ref),
                ))
            print(("{i:>3}  {commit:>10}  {files_changed:>5}  "
                   "{lines_added:>5}  {lines_deleted:>5}  "
                   "{end_to_end_s:>9}  {cycle_ms:>8}  "
                   "{nodes_added:>5}  {edges_added:>5}  {subject}"
                   ).format(**row))
        else:
            print(
                f"{i:>3}  {c[:10]:>10}  {files:>5}  "
                f"{ladd:>5}  {ldel:>5}  {e2e_s:>9}  "
                "WARMUP  "
                f"{subj}"
            )
        prev_commit = c
    # Restore HEAD.
    reset_to(HEAD)
    print(f"\nRestored HEAD to {HEAD[:10]}")

    OUT_CSV.write_text("")
    with OUT_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=header)
        w.writeheader()
        w.writerows(out_rows)
    print(f"Wrote {len(out_rows)} rows to {OUT_CSV}")
    if proof_api is not None:
        proof = proof_api[0](
            experiment_id=DEPLOYMENT,
            deployment=DEPLOYMENT,
            repo_root=BENCH_REPO,
            baseline_commit=baseline_commit,
            baseline_generation_id=baseline_gen,
            rows=proof_rows,
        )
        proof_api[3](OUT_DELTA_PROOF, proof)
        print(f"Wrote {len(proof_rows)} delta proof rows to {OUT_DELTA_PROOF}")


if __name__ == "__main__":
    main()
