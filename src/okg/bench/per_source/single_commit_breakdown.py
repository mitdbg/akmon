#!/usr/bin/env python3
"""Single-commit per-source perf breakdown.

For each tier-1 source, drives ONE measurement against the tinydb
fixture advancing from commit 4 to commit 5. Captures:

  - wall-clock around `run_ingest()` (the per-source workflow body)
  - the lightweight profiler's per-stage breakdown
    (source.X.run, publisher.phase_a, publisher.gates, etc.)
  - for git-files / git-history: per-`run_git` subprocess timings,
    captured by monkey-patching `okg.substrate.library.sources._git_helpers.run_git`.

Writes one JSON row per source to `bench/per_source/results/`.

Usage:
  python bench/per_source/single_commit_breakdown.py [<source> ...]
  (no args = all five tier-1 sources)

The script orchestrates each source independently:
  1. Generate a deployment dir registering ONLY that source.
  2. Drop+create okg_per_source_<source>; migrate; bootstrap; catalog load.
  3. Reset the tinydb fixture to commit 4.
  4. Run a WARM pass (run_ingest with publish=True) — this populates
     the source-watermarks cursor and gives the publisher a baseline
     gen to delta against.
  5. Reset to commit 5.
  6. Run the MEASUREMENT pass with run_git instrumented and the
     lightweight profiler enabled. Wall-clock the call.
  7. Read the profiler's _live/<deployment>/source_sync.<pid>.json.
  8. Emit one JSON result row.
  9. Drop the DB.

NOTE: bypasses the DBOS workflow envelope (no dispatch wait, no
setup probe, no setEvent step). For "where does run_ingest spend
time" this is exactly what we want — the workflow envelope was
already characterized in add-commit-replay-bench-harness.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import traceback
from contextlib import contextmanager
from pathlib import Path

from okg.substrate.cli._output import command_output_tail
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.profiling.artifacts import live_dir

try:
    from jinja2 import Template
except ImportError as _jinja_exc:  # pragma: no cover
    raise ImportError(
        "okg.bench.per_source.single_commit_breakdown needs jinja2 — install "
        "the bench extra: pip install 'okg[bench]'"
    ) from _jinja_exc

_BENCH = Path(os.environ.get("OKG_BENCH_ROOT", Path.cwd() / "bench")).resolve()
ROOT = _BENCH.parent
RIG_DIR = _BENCH / "per_source"
TEMPLATES_DIR = Path(__file__).parent / "templates"
TEMPLATES_DIR = TEMPLATES_DIR
RESULTS_DIR = RIG_DIR / "results"
GEN_DIR = RIG_DIR / "deployments-generated"
TINYDB_REPO = ROOT / ".scratch" / "bench-repos" / "tinydb"

# Commits used as the "delta" — picked from the bench's existing
# commits.txt so they're in the same chronological window as the
# bench-replay measurements.
COMMIT_PRIOR = "0f03c06408f5f579586813b9a1e1c24024715a53"   # commit 4
COMMIT_DELTA = "a3238551aebce948ff919b54f634428d92937fdf"   # commit 5

ADMIN_DSN = "postgres://postgres:okg@localhost:5433/postgres"
PYTHON = sys.executable
ENV_PYTHONPATH = str(ROOT / "src")

# Source -> (registry template filename, ontology modules needed).
# Each module's narrowings can reference subtypes owned by OTHER
# modules (code_graph narrows tool_use_event → code_symbol; openspec
# narrows openspec_change → source_file; agent_sessions narrows
# session → openspec_change; etc.). Loading less than the full
# tier-1 set produces `bridge_subtype_unknown` at catalog-load.
# Load the same superset everywhere; perf timing is per-source, so
# the extra catalog rows are harmless.
# Match okg-workspace's full module list. Narrowings reference
# subtypes across the whole set (e.g., code_graph narrows
# test_case_references_code_symbol → needs `ci`; agent_sessions
# narrows chapter → openspec_change → needs `openspec`). Loading
# the same superset everywhere keeps the catalog consistent.
ALL_MODULES = [
    "git_graph", "openspec", "agent_sessions", "compute_env",
    "code_graph", "extraction", "forge", "ci",
    "quality_evidence", "agent_memory", "views",
]

TIER_1 = {
    "git-files":      {"template": "registry_git_files.yaml.j2"},
    "git-history":    {"template": "registry_git_history.yaml.j2"},
    "code-structure": {"template": "registry_code_structure.yaml.j2"},
    "openspec":       {"template": "registry_openspec.yaml.j2"},
    "agent-sessions": {"template": "registry_agent_sessions.yaml.j2"},
}
for v in TIER_1.values():
    v["modules"] = ALL_MODULES


def deployment_name(source: str) -> str:
    return f"persrc-{source.replace('-', '_')}"


def db_name(source: str) -> str:
    return f"okg_per_source_{source.replace('-', '_')}"


def dsn(source: str) -> str:
    return f"postgres://postgres:okg@localhost:5433/{db_name(source)}"


def deployment_dir(source: str) -> Path:
    return GEN_DIR / source


# ---------------------------------------------------------------------------
# Postgres + repo lifecycle


def psql(sql: str, *, dsn_url: str = ADMIN_DSN, expect_error: bool = False) -> str:
    res = subprocess.run(
        ["psql", dsn_url, "-v", "ON_ERROR_STOP=1"],
        env={**os.environ, "PGPASSWORD": "okg"},
        input=sql, capture_output=True, text=True, check=False,
    )
    if not expect_error and res.returncode != 0:
        sys.stderr.write(res.stderr)
        raise RuntimeError(f"psql failed: {res.stderr[:300]}")
    return res.stdout


def setup_db_for(source: str) -> None:
    db = db_name(source)
    print(f"  [{source}] drop+create {db}")
    psql(
        f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        f"WHERE datname='{db}'; "
        f"DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db};"
    )
    psql(
        """
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        CREATE EXTENSION IF NOT EXISTS vector;
        CREATE EXTENSION IF NOT EXISTS pg_textsearch;
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
        CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
        CREATE EXTENSION IF NOT EXISTS btree_gist;
        """,
        dsn_url=dsn(source),
    )


def generate_deployment(source: str) -> Path:
    info = TIER_1[source]
    dep = deployment_dir(source)
    dep.mkdir(parents=True, exist_ok=True)
    ctx = {
        "deployment":  deployment_name(source),
        "source_name": source,
        "repo_root":   str(TINYDB_REPO),
        "modules":     info["modules"],
    }
    dep_yaml = Template(
        (TEMPLATES_DIR / "deployment.yaml.j2").read_text()
    ).render(**ctx)
    reg_yaml = Template(
        (TEMPLATES_DIR / info["template"]).read_text()
    ).render(**ctx)
    (dep / "deployment.yaml").write_text(dep_yaml)
    (dep / "source_registry.yaml").write_text(reg_yaml)
    return dep


def run_okg(args: list[str], *, dsn_url: str, dep: Path) -> None:
    res = subprocess.run(
        [*running_okg_argv(), *args,
         "--dsn", dsn_url, "--deployment", dep.name, "--json"],
        env={
            **os.environ,
            "OKG_DSN": dsn_url,
            "OKG_DEPLOYMENTS_DIR": str(GEN_DIR),
            "PYTHONPATH": ENV_PYTHONPATH,
        },
        capture_output=True, text=True, check=False,
    )
    if res.returncode != 0:
        # The CLI runs with --json: its structured failure report is on
        # STDOUT, warnings on STDERR. Carry BOTH into the error so a logged
        # warning cannot hide the report (the old stderr-first message did).
        raise RuntimeError(
            f"okg {' '.join(args)} failed for {dep.name}:\n"
            f"{command_output_tail(res)}"
        )


def bootstrap_for(source: str, dep: Path) -> None:
    print(f"  [{source}] migrate + bootstrap + catalog")
    # `okg migrate` requires --apply for every caller (#1288).
    run_okg(["migrate", "--apply"], dsn_url=dsn(source), dep=dep)
    run_okg(["runtime", "bootstrap"], dsn_url=dsn(source), dep=dep)
    # `catalog load --deployment <PATH>` (positional path, not name).
    res = subprocess.run(
        [*running_okg_argv(), "catalog", "load",
         "--deployment", str(dep), "--dsn", dsn(source), "--apply", "--json"],
        env={**os.environ, "OKG_DSN": dsn(source),
             "OKG_DEPLOYMENTS_DIR": str(GEN_DIR),
             "PYTHONPATH": ENV_PYTHONPATH},
        capture_output=True, text=True, check=False,
    )
    if res.returncode != 0:
        raise RuntimeError(
            f"catalog load failed for {source}:\n{command_output_tail(res)}"
        )


def reset_tinydb_to(commit: str) -> None:
    subprocess.run(
        ["git", "-C", str(TINYDB_REPO), "reset", "--hard",
         commit, "--quiet"],
        check=True,
    )


def teardown_db(source: str) -> None:
    db = db_name(source)
    psql(
        f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        f"WHERE datname='{db}'; "
        f"DROP DATABASE IF EXISTS {db};",
        expect_error=True,
    )


# ---------------------------------------------------------------------------
# run_git instrumentation: capture per-subprocess timing

@contextmanager
def instrument_run_git():
    """Monkey-patch `run_git` / `run_git_bytes` to record every call
    with its argv and elapsed_ms.

    Because each consumer module does `from _git_helpers import
    run_git`, we have to patch the name in EACH consumer module's
    namespace (not just `_git_helpers`). Python's `from X import Y`
    rebinds at import time. Consumers are discovered dynamically from
    the imported sources modules — hardcoding them rotted once before,
    when the retired `_git_graph_*_legacy` modules were deleted.
    """
    import okg.substrate.library.sources._git_helpers as gh
    samples: list[dict] = []

    consumers = [gh]
    for mod_name, mod in sorted(sys.modules.items()):
        if (
            mod is not None
            and mod is not gh
            and mod_name.startswith("okg.substrate.library.sources.")
            and (hasattr(mod, "run_git") or hasattr(mod, "run_git_bytes"))
        ):
            consumers.append(mod)
    targets = [
        (mod, name)
        for mod in consumers
        for name in ("run_git", "run_git_bytes")
        if hasattr(mod, name)
    ]

    originals = {(id(mod), name): getattr(mod, name)
                 for mod, name in targets if hasattr(mod, name)}

    def wrap(orig, label: str):
        def inner(repo_root, args, **kw):
            t0 = time.perf_counter_ns()
            try:
                return orig(repo_root, args, **kw)
            finally:
                dt = (time.perf_counter_ns() - t0) / 1e6
                samples.append({
                    "fn": label,
                    "argv": list(args)[:4],
                    "elapsed_ms": round(dt, 2),
                })
        return inner

    for mod, name in targets:
        if not hasattr(mod, name):
            continue
        orig = originals[(id(mod), name)]
        setattr(mod, name, wrap(orig, f"{mod.__name__.rsplit('.', 1)[-1]}.{name}"))
    try:
        yield samples
    finally:
        for (mod_id, name), orig in originals.items():
            for mod, mname in targets:
                if id(mod) == mod_id and mname == name:
                    setattr(mod, name, orig)
                    break


# ---------------------------------------------------------------------------
# Actually run one source through one delta


def ingest(dsn_url: str, dep: Path, source: str) -> dict:
    """Invoke run_ingest in-process. Returns the IngestRunResult.report."""
    # Ensure src/ is on path
    os.environ["OKG_DSN"] = dsn_url
    os.environ["OKG_DEPLOYMENTS_DIR"] = str(GEN_DIR)
    from okg.substrate.ingest.service import IngestOptions, run_ingest

    # scope_incomplete: cursor honored, completed_scope=False, runner
    # does NOT build a record_set diff. This is the realistic
    # incremental path the watcher takes when fsevents fires for a
    # specific path. scope_complete + cursor causes spurious
    # retractions (record_set diff against prior emission); avoid it
    # here.
    options = IngestOptions(
        dsn=dsn_url,
        deployment=dep.name,
        deployment_dir=dep,
        include=(source,),
        run_mode="scope_incomplete",
        publish=True,
        owner_id=f"per_source_rig:{source}",
        owner_meta={"command": "per_source_rig"},
    )
    result = run_ingest(options)
    return result.report


def published_gens(dsn_url: str, deployment: str) -> int:
    out = psql(
        f"SELECT COUNT(*) FROM okg.graph_generations "
        f"WHERE deployment_name='{deployment}' AND status='published';",
        dsn_url=dsn_url,
    )
    for line in out.splitlines():
        line = line.strip()
        if line.isdigit():
            return int(line)
    return 0


def live_counts(dsn_url: str, deployment: str) -> tuple[int, int]:
    import psycopg
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn_url) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest = store.latest_generation(deployment_name=deployment)
        if latest is None:
            return (0, 0)
        graph = store.checkout_generation(latest.checkpoint_id)
        nodes = graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
        edges = graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
        return int(nodes or 0), int(edges or 0)


def read_profile_snapshot(deployment: str) -> dict[str, float]:
    # The profile root is okg's user data folder (or OKG_PROFILE_DIR),
    # not the bench checkout: read where profiling now writes.
    live = live_dir(deployment)
    if not live.exists():
        return {}
    # Newest snapshot wins.
    files = sorted(live.glob("source_sync.*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        return {}
    data = json.loads(files[-1].read_text())
    return data.get("stage_p95_ms") or {}


# ---------------------------------------------------------------------------
# Main loop


def measure(source: str) -> dict:
    dep = generate_deployment(source)
    setup_db_for(source)
    bootstrap_for(source, dep)

    # Warm pass at commit 4.
    print(f"  [{source}] reset tinydb to {COMMIT_PRIOR[:10]} (warm pass)")
    reset_tinydb_to(COMMIT_PRIOR)
    _ = ingest(dsn(source), dep, source)
    gens_after_warm = published_gens(dsn(source), dep.name)
    n_after_warm, e_after_warm = live_counts(dsn(source), dep.name)

    # Measurement pass at commit 5.
    print(f"  [{source}] reset to {COMMIT_DELTA[:10]} (measurement pass)")
    reset_tinydb_to(COMMIT_DELTA)
    t0 = time.perf_counter_ns()
    with instrument_run_git() as git_calls:
        report = ingest(dsn(source), dep, source)
    wall_ms = (time.perf_counter_ns() - t0) / 1e6

    gens_after_delta = published_gens(dsn(source), dep.name)
    n_after_delta, e_after_delta = live_counts(dsn(source), dep.name)

    profile = read_profile_snapshot(dep.name)

    return {
        "source": source,
        "deployment": dep.name,
        "wall_ms_total": round(wall_ms, 2),
        "warm_gens": gens_after_warm,
        "delta_gens": gens_after_delta,
        "nodes_delta": n_after_delta - n_after_warm,
        "edges_delta": e_after_delta - e_after_warm,
        "facts_emitted_in_report": sum(
            (r.get("nodes_appended") or 0) + (r.get("edges_appended") or 0)
            for r in (report.get("results") or [])
        ),
        "profile_stage_p95_ms": profile,
        "git_calls": git_calls,
        "git_calls_count": len(git_calls),
        "git_calls_total_ms": round(sum(c["elapsed_ms"] for c in git_calls), 2),
    }


def main() -> int:
    sources = sys.argv[1:] or list(TIER_1.keys())
    bad = [s for s in sources if s not in TIER_1]
    if bad:
        sys.exit(f"unknown sources: {bad}. Pick from {list(TIER_1)}")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULTS_DIR / "single_commit_breakdown.json"
    rows: list[dict] = []

    for source in sources:
        print(f"\n===== {source} =====")
        try:
            row = measure(source)
            rows.append(row)
            print(f"  [{source}] wall_ms={row['wall_ms_total']}  "
                  f"nodes_delta={row['nodes_delta']}  "
                  f"edges_delta={row['edges_delta']}  "
                  f"git_calls={row['git_calls_count']}")
        except Exception:
            traceback.print_exc()
            rows.append({"source": source, "error": traceback.format_exc()})
        finally:
            teardown_db(source)

    out_path.write_text(json.dumps(rows, indent=2))
    print(f"\nWrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
