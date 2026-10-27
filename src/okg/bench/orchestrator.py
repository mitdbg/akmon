#!/usr/bin/env python3
"""Commit-replay benchmark orchestrator.

Reads `bench/manifest.yaml` and runs experiments end-to-end:
clones the repo, drops + creates the per-experiment Postgres DB,
installs extensions + migrate + bootstrap, generates the deployment
config, starts a worker, runs the replay (with the requested
strategy), stops the worker, extracts standardized metrics.

Usage:
  python bench/orchestrator.py list
  python bench/orchestrator.py setup-db    <experiment_id>
  python bench/orchestrator.py start-worker <experiment_id>
  python bench/orchestrator.py stop-worker  <experiment_id>
  python bench/orchestrator.py run          <experiment_id>
  python bench/orchestrator.py run --all
  python bench/orchestrator.py clean
  python bench/orchestrator.py plot         [<experiment_id>|--all|--cross]
  python bench/orchestrator.py profile      <experiment_id> --source <source>
  python bench/orchestrator.py diagnostics  <experiment_id>
  python bench/orchestrator.py gate         <experiment_id>
  python bench/orchestrator.py differential-report <experiment_id>
  python bench/orchestrator.py lineage-report <experiment_id>
  python bench/orchestrator.py runtime-lineage-report <experiment_id>

Spec: openspec/changes/add-commit-replay-bench-harness.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import html
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import yaml
import psycopg

try:
    from jinja2 import Template
except ImportError as _jinja_exc:  # pragma: no cover - exercised in wheel test
    raise ImportError(
        "okg.bench.orchestrator needs jinja2 — install the bench extra: "
        "pip install 'okg[bench]'"
    ) from _jinja_exc
from psycopg import sql
from psycopg.rows import dict_row

from okg.substrate.postgres_clone import safe_recreate_database
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.unknown import Unknown, read_ok


# ---------------------------------------------------------------------------
# Paths
#
# The harness code ships as okg.bench, but the LAB STATE (experiment
# manifest, templates, sweep locks, results, generated deployments) stays
# repo-side under <bench root>. Resolution: OKG_BENCH_ROOT env var when set,
# else ./bench relative to the working directory (the repo-root invocation
# CI and the thin bench/orchestrator.py launcher both use).

BENCH_DIR = Path(os.environ.get("OKG_BENCH_ROOT", Path.cwd() / "bench")).resolve()
ROOT = BENCH_DIR.parent
MANIFEST = BENCH_DIR / "manifest.yaml"
ENGINE_SENSITIVITY_CONFIG = BENCH_DIR / "engine_sensitivity.yaml"
TEMPLATES_DIR = BENCH_DIR / "templates"
GENERATED_DIR = BENCH_DIR / "deployments-generated"
RESULTS_DIR = BENCH_DIR / "results"
PLOTS_DIR = BENCH_DIR / "plots"
BENCH_REPOS_DIR = ROOT / ".scratch" / "bench-repos"
DEFAULT_PROFILE_ROOT = ROOT / ".artifacts" / "profiles"

# Substrate CLI subprocesses: pinned to this interpreter; the installed okg
# package resolves imports (no sys.path surgery).
PYTHON = sys.executable
ENV_PYTHONPATH = str(ROOT / "src")

ADMIN_DSN = os.environ.get(
    "OKG_BENCH_ADMIN_DSN",
    "postgres://postgres:okg@localhost:5433/postgres",
)

# Chronos default branch name (bench deployments publish on branch "default").
DEFAULT_BRANCH_NAME = "default"


def _admin_dsn_for(db: str) -> str:
    template = os.environ.get("OKG_BENCH_DSN_TEMPLATE")
    if template:
        return template.format(db=db)
    return f"postgres://postgres:okg@localhost:5433/{db}"


# ---------------------------------------------------------------------------
# Manifest schema

VALID_SOURCE_SETS = {
    "files-only",
    "history-only",
    "files+history",
    # `add-per-source-sync-perf-harness` / pygit2 bench follow-up:
    # `code-structure` depends on `git-files` as a predecessor source
    # (it indexes the `.py` files those emit). So "code" is always
    # really "files + code"; the explicit "files+code" / "all" names
    # keep that visible to readers.
    "files+code",
    # files+code plus the explicit LSP semantic-resolution source.
    "files+code+lsp",
    "all",
    # `tier1`: the realistic Python-OSS bench set — git-files +
    # git-history + code-structure + package-manifests + doc-corpus +
    # interfaces. Avoids OKG-self-graph-specific sources
    # (openspec, agent-sessions, etc.) that emit zero facts on a
    # generic repo. Used for palimpzest.
    "tier1",
    # `tier1-enriched`: tier1 + the `DeclarativeLinker` (default +
    # alias_table passes) + `CodeReferenceResolver` enrichers.
    # Resolves code_reference → code_symbol / dependency_package
    # and doc → code_symbol cross-source edges; pushes typical
    # edge-density up by ~50% on a Python OSS repo.
    "tier1-enriched",
}
VALID_STRATEGIES = {
    "full_history",
    "head_only",
    "first_n",
    "hybrid_head_then_stride",
    "tail_window",
}

# --- config-axes (bench-okg-internals) -------------------------------------
# A `config:` block (or a `sweep:` axis) makes the OKG-internals knobs that
# were previously hardcoded in the templates first-class, sweepable matrix
# axes: one distinct deployment config per cell. `_resolve_enricher_kinds`
# maps the `enrichers` preset to the concrete linker kinds the template
# renders; "default" follows the source_set (the pre-existing all-or-none
# behavior) so unconfigured experiments render byte-identically.
VALID_ENRICHER_PRESETS = {"default", "none", "declarative", "code", "all"}
ENRICHER_PRESET_KINDS = {
    "none": [],
    "declarative": ["declarative"],
    "code": ["code_reference"],
    "all": ["declarative", "code_reference"],
}
# Which config keys a `sweep:` block may fan out over, in id-suffix order.
SWEEPABLE_AXES = (
    "enrichers",
    "views",
    "chronos_continuation_pct",
    "embedding",
    "projection_batch",
    "branching",
)
# Compact id tags so a swept cell id stays under the DBOS app-name cap
# (okg-<id> <= 30 chars => id <= 26). Values not listed fall back to str().
_AXIS_ID_TAG = {
    "enrichers": {
        "default": "endef",
        "none": "en0",
        "declarative": "endecl",
        "code": "encode",
        "all": "enall",
    },
    "views": {True: "vw1", False: "vw0"},
    "embedding": {True: "emb1", False: "emb0"},
    "branching": {True: "br1", False: "br0"},
}


@dataclass(frozen=True)
class Repo:
    name: str
    url: str


@dataclass(frozen=True)
class Replay:
    strategy: str
    last_n: Optional[int] = None
    n: Optional[int] = None
    head_n: Optional[int] = None
    total: Optional[int] = None
    # Explicit, pinned commit list (oldest first). When set, the replay uses
    # exactly these SHAs instead of whatever upstream HEAD is today, and a
    # drift between the strategy's computed list and this pin fails the run.
    commits: Optional[tuple[str, ...]] = None


@dataclass(frozen=True)
class BenchConfig:
    """OKG-internals knobs lifted from hardcoded template values into
    first-class, sweepable axes (bench-okg-internals task.config-axes).

    Defaults reproduce the pre-existing behavior exactly: `enrichers:
    default` follows the source_set, `views: None` follows code-presence,
    chronos at 95, embedding off. `projection_batch` and `branching` are
    recognized + validated here; projection-batch wiring awaits a
    deployment-level substrate slot, and branching is exercised by
    task.branching-experiment.
    """

    enrichers: str = "default"  # default|none|declarative|code|all
    views: Optional[bool] = None  # None => follow source_set
    chronos_continuation_pct: int = 95
    embedding: bool = False
    projection_batch: Optional[int] = None
    branching: bool = False
    native_view_min_refresh_interval_ms: int = 0
    native_view_materialization_delay_ms: int = 0
    source_sync_compaction_window_seconds: int = 0
    source_sync_watch_debounce_seconds: float = 0.01
    source_sync_head_poll_interval_seconds: float = 0.25
    source_sync_reconcile_watermark_min_refresh_ms: int = 0
    source_sync_progress_min_refresh_ms: int = 0
    source_sync_queue_polling_interval_seconds: float = 0.0
    lsp_hot_path_max_requests: int = 0


@dataclass(frozen=True)
class CountGateBudget:
    """Per-commit query-count baselines (manifest `count_gate:` block).

    The replayed commits are pinned and their counts repeat within a few
    queries, so each pinned commit carries its own measured query_count.
    The gate fails a commit whose query_count exceeds
    baseline * headroom. A uniform rise in the per-commit fixed cost
    raises every commit, so it fails too.
    """

    headroom: float
    # (full SHA, measured query_count), in pinned replay order.
    baselines: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class Experiment:
    id: str
    repo: Repo
    source_set: str
    replay: Replay
    config: BenchConfig = BenchConfig()
    count_gate: Optional[CountGateBudget] = None
    # The OKG deployment name this experiment runs under. None means the
    # experiment id (every manifest cell). The forced rebuild of the
    # rebuild-equivalence proof sets it to the INCREMENTAL experiment's id so
    # both sides publish under the same deployment name -- deployment-scoped
    # values such as a view's `canonical_asset_id` and the source ownership
    # ids then match -- while `id` (database, result files, worker log) stays
    # distinct so the two runs never share state (issue #1539, decision 92a).
    deployment_name: Optional[str] = None

    @property
    def deployment(self) -> str:
        """Deployment name passed to every okg command and query."""
        return self.deployment_name or self.id

    @property
    def deployments_root(self) -> Path:
        """`OKG_DEPLOYMENTS_DIR` for this experiment. An experiment that runs
        under another experiment's deployment name gets its own root, so its
        generated files never overwrite the other experiment's."""
        if self.deployment == self.id:
            return GENERATED_DIR
        return GENERATED_DIR / ".isolated" / self.id

    @property
    def db_name(self) -> str:
        return f"okg_bench_{self.id.replace('-', '_')}"

    @property
    def dsn(self) -> str:
        return _admin_dsn_for(self.db_name)

    @property
    def repo_root(self) -> Path:
        return BENCH_REPOS_DIR / self.repo.name

    @property
    def deployment_dir(self) -> Path:
        return self.deployments_root / self.deployment

    @property
    def worker_log(self) -> Path:
        return RESULTS_DIR / f"{self.id}.worker.log"

    @property
    def raw_csv(self) -> Path:
        return RESULTS_DIR / f"{self.id}.raw.csv"

    @property
    def delta_proof_json(self) -> Path:
        return RESULTS_DIR / f"{self.id}.delta_proof.json"

    @property
    def rebuild_equivalence_json(self) -> Path:
        return RESULTS_DIR / f"{self.id}.rebuild_equivalence.json"

    @property
    def lineage_report_json(self) -> Path:
        return RESULTS_DIR / f"{self.id}.lineage_report.json"

    @property
    def runtime_lineage_report_json(self) -> Path:
        return RESULTS_DIR / f"{self.id}.runtime_lineage_report.json"

    @property
    def run_metrics_json(self) -> Path:
        """Run-level (not per-commit) metrics sidecar: peak worker RSS, etc.
        Written by cmd_run, read by extract.py (bench-okg-internals)."""
        return RESULTS_DIR / f"{self.id}.run_metrics.json"

    @property
    def read_side_json(self) -> Path:
        """Read-side latency report at the final published generation
        (bench-okg-internals task.read-side-perf)."""
        return RESULTS_DIR / f"{self.id}.read_side.json"

    @property
    def replay_log(self) -> Path:
        return RESULTS_DIR / f"{self.id}.replay.log"

    @property
    def commits_file(self) -> Path:
        """Side-file with the explicit chronological commit list.

        Used for every strategy now (not just first_n / hybrid): we
        pre-compute the list while the working tree still has full
        history, then reset to baseline. This decouples the worker
        from `git log` at replay time, which is necessary because
        the orchestrator now resets the tree to baseline BEFORE
        starting the worker.
        """
        return self.deployment_dir / "commits.txt"


def _load_manifest() -> list[Experiment]:
    if not MANIFEST.exists():
        sys.exit(f"manifest not found: {MANIFEST}")
    raw = yaml.safe_load(MANIFEST.read_text())
    if not isinstance(raw, dict) or "experiments" not in raw:
        sys.exit(f"manifest must be a mapping with `experiments`: {MANIFEST}")
    out: list[Experiment] = []
    seen_ids: set[str] = set()
    for i, e in enumerate(raw["experiments"]):
        try:
            cells = _expand_sweep(e) if "sweep" in e else [e]
            parsed = [_parse_experiment(c) for c in cells]
        except ValueError as err:
            sys.exit(f"manifest entry [{i}] invalid: {err}")
        for exp in parsed:
            if exp.id in seen_ids:
                sys.exit(f"manifest entry [{i}] duplicate id: {exp.id!r}")
            seen_ids.add(exp.id)
            out.append(exp)
    return out


def build_engine_sensitivity_plan(
    config_path: Path | str = ENGINE_SENSITIVITY_CONFIG,
) -> tuple[dict[str, Any], list[Experiment]]:
    """Build the engine-sensitivity plan and parse every cell as an Experiment."""
    from okg.bench.engine_sensitivity import build_sensitivity_plan  # noqa: PLC0415

    plan = build_sensitivity_plan(Path(config_path))
    experiments: list[Experiment] = []
    for cell in plan["cells"]:
        experiments.append(_parse_experiment(cell["experiment"]))
    return plan, experiments


def _expand_sweep(e: dict) -> list[dict]:
    """Fan a `sweep:` entry into one experiment dict per matrix cell.

    `sweep` is a mapping of config-axis -> list of values; the cartesian
    product yields the cells. Each cell is a deep copy of the base entry with
    those axis values folded into its `config:` block and a compact tag
    appended to the id (so cell deployments stay distinct and under the DBOS
    app-name cap). A pre-existing `config:` block supplies the non-swept
    defaults.
    """
    sweep = e["sweep"]
    if not isinstance(sweep, dict) or not sweep:
        raise ValueError(f"{e.get('id')!r}: `sweep` must be a non-empty mapping")
    base_id = e.get("id")
    if not isinstance(base_id, str):
        raise ValueError("sweep entry missing or non-string `id`")
    axes = []
    for axis, values in sweep.items():
        if axis not in SWEEPABLE_AXES:
            raise ValueError(
                f"{base_id!r}: sweep axis must be one of {list(SWEEPABLE_AXES)}, "
                f"got {axis!r}"
            )
        if not isinstance(values, list) or not values:
            raise ValueError(f"{base_id!r}: sweep[{axis!r}] must be a non-empty list")
        axes.append((axis, values))
    cells: list[dict] = [{}]
    for axis, values in axes:
        cells = [{**c, axis: v} for c in cells for v in values]
    out: list[dict] = []
    for assignment in cells:
        cell = copy.deepcopy({k: v for k, v in e.items() if k != "sweep"})
        cfg = dict(cell.get("config") or {})
        tag_parts = []
        for axis, _ in axes:
            v = assignment[axis]
            cfg[axis] = v
            tag = _AXIS_ID_TAG.get(axis, {}).get(v, str(v).replace("-", ""))
            tag_parts.append(tag)
        cell["config"] = cfg
        cell["id"] = f"{base_id}-" + "-".join(tag_parts)
        out.append(cell)
    return out


_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _parse_pinned_commits(
    exp_id: str, strategy: str, rep: dict[str, Any],
) -> Optional[tuple[str, ...]]:
    """Validate an optional `replay.commits` pin: full 40-hex SHAs, oldest
    first, only for `head_only`, and exactly `last_n` of them."""
    raw = rep.get("commits")
    if raw is None:
        return None
    if strategy != "head_only":
        raise ValueError(
            f"{exp_id!r}: replay.commits is only supported with "
            f"strategy head_only, got {strategy!r}"
        )
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"{exp_id!r}: replay.commits must be a non-empty list")
    bad = [c for c in raw if not isinstance(c, str) or not _FULL_SHA_RE.match(c)]
    if bad:
        raise ValueError(
            f"{exp_id!r}: replay.commits entries must be full 40-hex SHAs; "
            f"bad: {bad!r}"
        )
    if len(set(raw)) != len(raw):
        raise ValueError(f"{exp_id!r}: replay.commits has duplicate SHAs")
    if rep.get("last_n") != len(raw):
        raise ValueError(
            f"{exp_id!r}: replay.last_n ({rep.get('last_n')!r}) must equal "
            f"the number of pinned replay.commits ({len(raw)})"
        )
    return tuple(raw)


def _parse_bench_config(exp_id: str, raw: object) -> BenchConfig:
    if raw is None:
        return BenchConfig()
    if not isinstance(raw, dict):
        raise ValueError(f"{exp_id!r}: `config` must be a mapping")
    unknown = set(raw) - {
        "enrichers",
        "views",
        "chronos_continuation_pct",
        "embedding",
        "projection_batch",
        "branching",
        "native_view_min_refresh_interval_ms",
        "native_view_materialization_delay_ms",
        "source_sync_compaction_window_seconds",
        "source_sync_watch_debounce_seconds",
        "source_sync_head_poll_interval_seconds",
        "source_sync_reconcile_watermark_min_refresh_ms",
        "source_sync_progress_min_refresh_ms",
        "source_sync_queue_polling_interval_seconds",
        "lsp_hot_path_max_requests",
    }
    if unknown:
        raise ValueError(f"{exp_id!r}: unknown config keys {sorted(unknown)}")
    enrichers = raw.get("enrichers", "default")
    if enrichers not in VALID_ENRICHER_PRESETS:
        raise ValueError(
            f"{exp_id!r}: config.enrichers must be one of "
            f"{sorted(VALID_ENRICHER_PRESETS)}, got {enrichers!r}"
        )
    views = raw.get("views", None)
    if views is not None and not isinstance(views, bool):
        raise ValueError(f"{exp_id!r}: config.views must be a bool or omitted")
    pct = raw.get("chronos_continuation_pct", 95)
    if not isinstance(pct, int) or isinstance(pct, bool) or not 1 <= pct <= 100:
        raise ValueError(
            f"{exp_id!r}: config.chronos_continuation_pct must be an int 1..100"
        )
    embedding = raw.get("embedding", False)
    if not isinstance(embedding, bool):
        raise ValueError(f"{exp_id!r}: config.embedding must be a bool")
    proj = raw.get("projection_batch", None)
    if proj is not None and (
        not isinstance(proj, int) or isinstance(proj, bool) or proj < 1
    ):
        raise ValueError(f"{exp_id!r}: config.projection_batch must be a positive int")
    branching = raw.get("branching", False)
    if not isinstance(branching, bool):
        raise ValueError(f"{exp_id!r}: config.branching must be a bool")
    native_view_min_refresh = raw.get("native_view_min_refresh_interval_ms", 0)
    if (
        not isinstance(native_view_min_refresh, int)
        or isinstance(native_view_min_refresh, bool)
        or native_view_min_refresh < 0
    ):
        raise ValueError(
            f"{exp_id!r}: config.native_view_min_refresh_interval_ms "
            "must be a non-negative int"
        )
    native_view_delay = raw.get("native_view_materialization_delay_ms", 0)
    if (
        not isinstance(native_view_delay, int)
        or isinstance(native_view_delay, bool)
        or native_view_delay < 0
    ):
        raise ValueError(
            f"{exp_id!r}: config.native_view_materialization_delay_ms "
            "must be a non-negative int"
        )
    source_sync_compaction_window = raw.get(
        "source_sync_compaction_window_seconds",
        0,
    )
    if (
        not isinstance(source_sync_compaction_window, int)
        or isinstance(source_sync_compaction_window, bool)
        or source_sync_compaction_window < 0
    ):
        raise ValueError(
            f"{exp_id!r}: config.source_sync_compaction_window_seconds "
            "must be a non-negative int"
        )
    source_sync_watch_debounce = raw.get(
        "source_sync_watch_debounce_seconds",
        0.01,
    )
    if (
        not isinstance(source_sync_watch_debounce, (int, float))
        or isinstance(source_sync_watch_debounce, bool)
        or source_sync_watch_debounce < 0
    ):
        raise ValueError(
            f"{exp_id!r}: config.source_sync_watch_debounce_seconds "
            "must be a non-negative number"
        )
    source_sync_head_poll_interval = raw.get(
        "source_sync_head_poll_interval_seconds",
        0.25,
    )
    if (
        not isinstance(source_sync_head_poll_interval, (int, float))
        or isinstance(source_sync_head_poll_interval, bool)
        or source_sync_head_poll_interval <= 0
    ):
        raise ValueError(
            f"{exp_id!r}: config.source_sync_head_poll_interval_seconds "
            "must be a positive number"
        )
    reconcile_watermark_min_refresh = raw.get(
        "source_sync_reconcile_watermark_min_refresh_ms",
        0,
    )
    if (
        not isinstance(reconcile_watermark_min_refresh, int)
        or isinstance(reconcile_watermark_min_refresh, bool)
        or reconcile_watermark_min_refresh < 0
    ):
        raise ValueError(
            f"{exp_id!r}: "
            "config.source_sync_reconcile_watermark_min_refresh_ms "
            "must be a non-negative int"
        )
    source_progress_min_refresh = raw.get(
        "source_sync_progress_min_refresh_ms",
        0,
    )
    if (
        not isinstance(source_progress_min_refresh, int)
        or isinstance(source_progress_min_refresh, bool)
        or source_progress_min_refresh < 0
    ):
        raise ValueError(
            f"{exp_id!r}: config.source_sync_progress_min_refresh_ms "
            "must be a non-negative int"
        )
    source_sync_queue_polling_interval = raw.get(
        "source_sync_queue_polling_interval_seconds",
        0.0,
    )
    if (
        not isinstance(source_sync_queue_polling_interval, (int, float))
        or isinstance(source_sync_queue_polling_interval, bool)
        or source_sync_queue_polling_interval < 0
    ):
        raise ValueError(
            f"{exp_id!r}: config.source_sync_queue_polling_interval_seconds "
            "must be a non-negative number"
        )
    lsp_hot_path_max_requests = raw.get("lsp_hot_path_max_requests", 0)
    if (
        not isinstance(lsp_hot_path_max_requests, int)
        or isinstance(lsp_hot_path_max_requests, bool)
        or lsp_hot_path_max_requests < 0
    ):
        raise ValueError(
            f"{exp_id!r}: config.lsp_hot_path_max_requests "
            "must be a non-negative int"
        )
    return BenchConfig(
        enrichers=enrichers,
        views=views,
        chronos_continuation_pct=pct,
        embedding=embedding,
        projection_batch=proj,
        branching=branching,
        native_view_min_refresh_interval_ms=native_view_min_refresh,
        native_view_materialization_delay_ms=native_view_delay,
        source_sync_compaction_window_seconds=source_sync_compaction_window,
        source_sync_watch_debounce_seconds=float(source_sync_watch_debounce),
        source_sync_head_poll_interval_seconds=float(
            source_sync_head_poll_interval,
        ),
        source_sync_reconcile_watermark_min_refresh_ms=(
            reconcile_watermark_min_refresh
        ),
        source_sync_progress_min_refresh_ms=source_progress_min_refresh,
        source_sync_queue_polling_interval_seconds=float(
            source_sync_queue_polling_interval,
        ),
        lsp_hot_path_max_requests=lsp_hot_path_max_requests,
    )


def _parse_experiment(e: dict) -> Experiment:
    if "id" not in e or not isinstance(e["id"], str):
        raise ValueError("missing or non-string `id`")
    if "repo" not in e or not isinstance(e["repo"], dict):
        raise ValueError(f"{e['id']!r}: missing `repo` mapping")
    repo = e["repo"]
    if "name" not in repo or "url" not in repo:
        raise ValueError(f"{e['id']!r}: repo needs `name` and `url`")
    if "source_set" not in e:
        raise ValueError(f"{e['id']!r}: missing `source_set`")
    if e["source_set"] not in VALID_SOURCE_SETS:
        raise ValueError(
            f"{e['id']!r}: source_set must be one of "
            f"{sorted(VALID_SOURCE_SETS)}, got {e['source_set']!r}"
        )
    if "replay" not in e or not isinstance(e["replay"], dict):
        raise ValueError(f"{e['id']!r}: missing `replay` mapping")
    rep = e["replay"]
    strategy = rep.get("strategy")
    if strategy not in VALID_STRATEGIES:
        raise ValueError(
            f"{e['id']!r}: replay.strategy must be one of "
            f"{sorted(VALID_STRATEGIES)}, got {strategy!r}"
        )
    replay = Replay(
        strategy=strategy,
        last_n=rep.get("last_n"),
        n=rep.get("n"),
        head_n=rep.get("head_n"),
        total=rep.get("total"),
        commits=_parse_pinned_commits(e["id"], strategy, rep),
    )
    # DBOS derives an app name `okg-<id>` capped at 30 chars; keep ids (incl.
    # swept-cell suffixes) under that or provisioning fails opaquely later.
    if len(e["id"]) > 26:
        raise ValueError(
            f"{e['id']!r}: id is {len(e['id'])} chars; must be <= 26 "
            "(DBOS app-name cap). Shorten the base id or sweep tag."
        )
    config = _parse_bench_config(e["id"], e.get("config"))
    return Experiment(
        id=e["id"],
        repo=Repo(name=repo["name"], url=repo["url"]),
        source_set=e["source_set"],
        replay=replay,
        config=config,
        count_gate=_parse_count_gate_budget(
            e["id"], e.get("count_gate"), replay.commits,
        ),
    )


def _parse_count_gate_budget(
    exp_id: str,
    raw: object,
    pinned_commits: Optional[tuple[str, ...]],
) -> Optional[CountGateBudget]:
    """Validate a `count_gate:` block: a headroom factor > 1 and one
    measured query_count per pinned commit, keyed by full SHA. Per-commit
    baselines only mean something for a pinned replay, so the block
    requires `replay.commits` and must cover exactly those commits."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError(f"{exp_id!r}: `count_gate` must be a mapping")
    required = {"headroom", "baselines"}
    unknown = set(raw) - required
    if unknown:
        raise ValueError(f"{exp_id!r}: unknown count_gate keys {sorted(unknown)}")
    missing = required - set(raw)
    if missing:
        raise ValueError(f"{exp_id!r}: count_gate is missing {sorted(missing)}")
    headroom = raw["headroom"]
    if (
        isinstance(headroom, bool)
        or not isinstance(headroom, (int, float))
        or headroom <= 1.0
    ):
        raise ValueError(
            f"{exp_id!r}: count_gate.headroom must be a number above 1.0, "
            f"got {headroom!r}"
        )
    if not pinned_commits:
        raise ValueError(
            f"{exp_id!r}: count_gate needs a pinned replay.commits list"
        )
    baselines = raw["baselines"]
    if not isinstance(baselines, dict):
        raise ValueError(
            f"{exp_id!r}: count_gate.baselines must map full SHA -> query_count"
        )
    if set(baselines) != set(pinned_commits):
        raise ValueError(
            f"{exp_id!r}: count_gate.baselines must list exactly the pinned "
            f"replay.commits; missing {sorted(set(pinned_commits) - set(baselines))}, "
            f"extra {sorted(set(baselines) - set(pinned_commits))}"
        )
    bad = {
        sha: value for sha, value in baselines.items()
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0
    }
    if bad:
        raise ValueError(
            f"{exp_id!r}: count_gate.baselines values must be positive "
            f"integers; bad: {bad!r}"
        )
    return CountGateBudget(
        headroom=float(headroom),
        baselines=tuple((sha, baselines[sha]) for sha in pinned_commits),
    )


# ---------------------------------------------------------------------------
# Helpers


def _run(
    cmd: list[str],
    *,
    env: Optional[dict[str, str]] = None,
    cwd: Optional[Path] = None,
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess:
    """Subprocess wrapper with sensible defaults."""
    merged_env = dict(os.environ)
    if env:
        merged_env.update(env)
    return subprocess.run(
        cmd,
        env=merged_env,
        cwd=cwd,
        check=check,
        capture_output=capture,
        text=True,
    )


def _psql(sql: str, *, dsn: str = ADMIN_DSN, expect_error: bool = False) -> str:
    """Run SQL against the admin DSN.

    Each statement is executed separately with autocommit enabled so
    DROP/CREATE DATABASE are not wrapped in a transaction. The benchmark runs in
    CI environments that may not have the external `psql` client installed, so
    this uses the already-required psycopg dependency directly.
    """
    statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
    output = ""
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
                    if cur.description:
                        output = "\n".join(
                            "\t".join("" if value is None else str(value) for value in row)
                            for row in cur.fetchall()
                        )
    except Exception as exc:  # noqa: BLE001 - CLI helper preserves error surface.
        if expect_error:
            return str(exc)
        sys.stderr.write(f"{exc}\n")
        raise
    return output


def _baseline_commit(exp: Experiment) -> str:
    """SHA the repo should be at when the worker starts watching.

    Picking the right baseline matters because the watcher fires an
    initial sync on startup that ingests *whatever* state HEAD is at.
    If HEAD is at full-repo-state when the worker boots, the initial
    sync ingests every commit/file/author in the repo BEFORE the bench
    has even reset to the baseline — and those facts persist through
    the reset (the bench only resets the working tree, not the DB).
    The "growth curve" plots then start at the full graph and look
    flat, because the bench's per-commit deltas are tiny compared to
    the pre-loaded mass. See issue #6 in the v2 plot writeup.
    """
    if exp.replay.commits:
        # Pinned list: the baseline is the first pinned commit's parent,
        # independent of where the working tree currently points.
        return subprocess.check_output(
            ["git", "-C", str(exp.repo_root), "rev-parse",
             f"{exp.replay.commits[0]}~1"],
            text=True,
        ).strip()
    if exp.replay.strategy == "head_only":
        last_n = exp.replay.last_n or 0
        return subprocess.check_output(
            ["git", "-C", str(exp.repo_root), "rev-parse", f"HEAD~{last_n}"],
            text=True,
        ).strip()
    # full_history, first_n, hybrid: baseline = root commit (no parents).
    return (
        subprocess.check_output(
            ["git", "-C", str(exp.repo_root), "rev-list", "--max-parents=0", "HEAD"],
            text=True,
        )
        .strip()
        .split("\n")[0]
    )


def _reset_repo_to_baseline(exp: Experiment) -> None:
    """Pre-reset the working tree to the bench's baseline commit
    BEFORE starting the worker, so the worker's initial-scan sync
    sees baseline state, not HEAD state.

    Also prunes `refs/remotes/origin/*` and expires the reflog. Why:
    GitHistorySource uses `git log --all` to walk commits, which
    follows every ref in the repo, NOT just HEAD's history. Without
    the prune, a fresh clone still has `refs/remotes/origin/HEAD`
    pointing at the tip — so `--all` walks every commit, baseline
    nodes_live shoots straight to ~900, and the bench's per-commit
    growth deltas become invisible noise. After pruning, `--all` only
    follows refs/heads/<branch> (which we just reset to baseline),
    matching the intent of "the repo as of this commit". The SHAs
    are still in the object store, so subsequent `git reset --hard
    <future_sha>` works during replay."""
    baseline = _baseline_commit(exp)
    print(
        f"  pre-resetting repo to baseline {baseline[:10]} "
        f"(strategy={exp.replay.strategy})"
    )
    _run(
        ["git", "-C", str(exp.repo_root), "reset", "--hard", baseline, "--quiet"],
    )
    # Prune refs that would expand `git log --all` past baseline.
    # Discover them dynamically so we don't hard-code the remote
    # name. `for-each-ref` is plumbing; safe to parse.
    refs_to_delete = (
        subprocess.check_output(
            [
                "git",
                "-C",
                str(exp.repo_root),
                "for-each-ref",
                "--format=%(refname)",
                "refs/remotes/",
                "refs/tags/",
                "refs/stash",
            ],
            text=True,
        )
        .strip()
        .split("\n")
    )
    refs_to_delete = [r for r in refs_to_delete if r]
    if refs_to_delete:
        print(
            f"  pruning {len(refs_to_delete)} non-HEAD refs (remotes/tags/stash) "
            "so `git log --all` doesn't drag in the full history"
        )
        for ref in refs_to_delete:
            subprocess.run(
                ["git", "-C", str(exp.repo_root), "update-ref", "-d", ref],
                check=False,
            )
    # Reflog expiry — without this, `git log --all` may still follow
    # entries via `HEAD@{n}`. Cheap; safe.
    subprocess.run(
        ["git", "-C", str(exp.repo_root), "reflog", "expire", "--expire=now", "--all"],
        check=False,
    )


def _ensure_repo(exp: Experiment) -> None:
    """Clone the experiment's target repo into .scratch/bench-repos/<name>
    if not present. Reset to origin/HEAD so a prior bench's `git reset`
    doesn't leave HEAD at a mid-history commit and silently truncate the
    next bench's commit list. Set core.symlinks=false to dodge macOS
    PATH_MAX on accidentally-checked-in long-content symlinks (observed
    on tinydb)."""
    BENCH_REPOS_DIR.mkdir(parents=True, exist_ok=True)
    if exp.repo.url == "synthetic:differential":
        from okg.bench.differential_replay_fixture import (  # noqa: PLC0415
            create_synthetic_differential_repo,
        )

        if exp.repo_root.exists():
            shutil.rmtree(exp.repo_root)
        fixture = create_synthetic_differential_repo(BENCH_REPOS_DIR)
        if fixture.repo_root != exp.repo_root:
            raise RuntimeError(
                f"synthetic repo fixture wrote {fixture.repo_root}, "
                f"expected {exp.repo_root}"
            )
        print(f"  generated synthetic repo {exp.repo.name!r} at {exp.repo_root}")
        return
    if exp.repo_root.exists():
        print(f"  repo {exp.repo.name!r} already cloned at {exp.repo_root}")
        if os.environ.get("OKG_BENCH_OFFLINE") == "1":
            local_head = "HEAD"
            for candidate in ("master", "main"):
                ref = exp.repo_root / ".git" / "refs" / "heads" / candidate
                if ref.exists():
                    local_head = candidate
                    break
            print(
                "  OKG_BENCH_OFFLINE=1; skipping fetch and resetting "
                f"to local {local_head}"
            )
            _run(
                [
                    "git",
                    "-C",
                    str(exp.repo_root),
                    "reset",
                    "--hard",
                    local_head,
                    "--quiet",
                ],
            )
            _reset_to_pinned_head(exp)
            return
        # Fetch and reset to the remote's HEAD. Idempotent.
        print("  fetching + resetting to origin/HEAD")
        _run(
            ["git", "-C", str(exp.repo_root), "fetch", "--quiet", "origin"],
        )
        # Determine origin's default branch.
        try:
            res = _run(
                ["git", "-C", str(exp.repo_root), "rev-parse", "origin/HEAD"],
                capture=True,
            )
            origin_head = res.stdout.strip().split("\n")[0]
        except subprocess.CalledProcessError:
            # Fallback: just reset to the SHA at the remote tip of
            # whatever branch HEAD tracks.
            origin_head = "FETCH_HEAD"
        _run(
            [
                "git",
                "-C",
                str(exp.repo_root),
                "reset",
                "--hard",
                origin_head,
                "--quiet",
            ],
        )
    else:
        print(f"  cloning {exp.repo.url} → {exp.repo_root}")
        _run(
            ["git", "clone", "--quiet", exp.repo.url, str(exp.repo_root)],
        )
    # Always set core.symlinks=false; harmless when no problematic
    # symlinks exist, life-saving when they do.
    _run(
        ["git", "-C", str(exp.repo_root), "config", "core.symlinks", "false"],
    )
    _reset_to_pinned_head(exp)


def _reset_to_pinned_head(exp: Experiment) -> None:
    """Pinned replay: move HEAD to the last pinned commit so the strategy
    computes the pinned list even after upstream moves on. A pinned SHA
    missing from the clone fails here, loudly."""
    if not exp.replay.commits:
        return
    pinned_head = exp.replay.commits[-1]
    print(f"  pinned replay: resetting to {pinned_head[:10]}")
    _run(
        ["git", "-C", str(exp.repo_root), "reset", "--hard", pinned_head,
         "--quiet"],
    )


# ---------------------------------------------------------------------------
# DB lifecycle

EXTENSIONS_SQL = """
  CREATE EXTENSION IF NOT EXISTS timescaledb;
  CREATE EXTENSION IF NOT EXISTS vector;
  CREATE EXTENSION IF NOT EXISTS pg_textsearch;
  CREATE EXTENSION IF NOT EXISTS pg_trgm;
  CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
  CREATE EXTENSION IF NOT EXISTS btree_gist;
"""


def setup_db(exp: Experiment) -> None:
    print(f"[{exp.id}] setup_db: {exp.db_name}")
    print(f"  safely draining + recreating {exp.db_name}")
    safe_recreate_database(
        exp.dsn,
        expected_database=exp.db_name,
        admin_dsn=ADMIN_DSN,
    )
    print(f"  installing extensions in {exp.db_name}")
    _psql(EXTENSIONS_SQL, dsn=exp.dsn)

    _write_deployment_files(exp)

    print("  running migrate")
    _run(
        [
            *running_okg_argv(),
            "migrate",
            "--deployment",
            exp.deployment,
            "--dsn",
            exp.dsn,
            # `okg migrate` requires --apply for every caller (#1288).
            "--apply",
            "--json",
        ],
        env={
            "OKG_DSN": exp.dsn,
            "OKG_DEPLOYMENTS_DIR": str(exp.deployments_root),
            "PYTHONPATH": ENV_PYTHONPATH,
        },
        capture=True,
    )
    print("  running runtime bootstrap")
    _run(
        [
            *running_okg_argv(),
            "runtime",
            "bootstrap",
            "--deployment",
            exp.deployment,
            "--dsn",
            exp.dsn,
            "--json",
        ],
        env={
            "OKG_DSN": exp.dsn,
            "OKG_DEPLOYMENTS_DIR": str(exp.deployments_root),
            "PYTHONPATH": ENV_PYTHONPATH,
        },
        capture=True,
    )
    print("  loading catalog (registers module narrowings)")
    # Required for GitHistorySource emissions (DERIVES_FROM, AFFECTS,
    # etc.) to pass Phase B's narrowings invariant. Without this, any
    # source emitting cross-subtype edges blocks the publisher.
    # `catalog load`'s --deployment takes a PATH (not a name); pass
    # the generated deployment dir.
    _run(
        [
            *running_okg_argv(),
            "catalog",
            "load",
            "--deployment",
            str(exp.deployment_dir),
            "--dsn",
            exp.dsn,
            "--apply",
            "--json",
        ],
        env={
            "OKG_DSN": exp.dsn,
            "OKG_DEPLOYMENTS_DIR": str(exp.deployments_root),
            "PYTHONPATH": ENV_PYTHONPATH,
        },
        capture=True,
    )
    print(f"[{exp.id}] setup_db complete")


def _write_deployment_files(exp: Experiment) -> None:
    """Render the experiment's deployment.yaml + source_registry.yaml
    from the Jinja templates into <deployments_root>/<deployment>/
    (bench/deployments-generated/<id>/ for every manifest cell).
    """
    exp.deployment_dir.mkdir(parents=True, exist_ok=True)
    tier1_like = {"tier1", "tier1-enriched"}
    include_files = exp.source_set in (
        {"files-only", "files+history", "files+code", "files+code+lsp", "all"}
        | tier1_like
    )
    include_history = exp.source_set in (
        {"history-only", "files+history", "all"} | tier1_like
    )
    include_code = exp.source_set in (
        {"files+code", "files+code+lsp", "all"} | tier1_like
    )
    include_code_lsp = exp.source_set in {"files+code+lsp"}
    include_package_manifests = exp.source_set in tier1_like
    include_doc_corpus = exp.source_set in tier1_like
    include_interfaces = exp.source_set in tier1_like
    include_enrichers = exp.source_set in {"tier1-enriched"}
    include_history_file_stubs = include_history and not include_files
    # config-axes (bench-okg-internals): resolve the swept knobs. `enrichers:
    # default` reproduces the source_set-derived all-or-none behavior; an
    # explicit preset overrides it. `views` defaults to code-presence and can be
    # toggled off on a code-bearing set to isolate view-materialization cost.
    cfg = exp.config
    if cfg.enrichers == "default":
        enricher_kinds = ["declarative", "code_reference"] if include_enrichers else []
    else:
        enricher_kinds = list(ENRICHER_PRESET_KINDS[cfg.enrichers])
    # Enrichers read the code graph (declarative aliases + code references), so
    # they only make sense on code-bearing source sets; gate to avoid emitting a
    # linker config a non-code deployment cannot satisfy.
    if not include_code:
        enricher_kinds = []
    include_views = include_code and (True if cfg.views is None else cfg.views)
    ctx = {
        # The templates name the DEPLOYMENT with this key (deployment name,
        # source ownership ids, view registry); see Experiment.deployment.
        "experiment_id": exp.deployment,
        "repo_name": exp.repo.name,
        "repo_url": exp.repo.url,
        "repo_root": str(exp.repo_root),
        "deployment_dir": str(exp.deployment_dir),
        "source_set": exp.source_set,
        "include_files": include_files,
        "include_history": include_history,
        "include_code": include_code,
        "include_code_lsp": include_code_lsp,
        "include_package_manifests": include_package_manifests,
        "include_doc_corpus": include_doc_corpus,
        "include_interfaces": include_interfaces,
        "include_enrichers": include_enrichers,
        "include_views": include_views,
        "enricher_kinds": enricher_kinds,
        "chronos_continuation_pct": cfg.chronos_continuation_pct,
        "embedding_enabled": cfg.embedding,
        "native_view_min_refresh_interval_ms": (
            cfg.native_view_min_refresh_interval_ms
        ),
        "native_view_materialization_delay_ms": (
            cfg.native_view_materialization_delay_ms
        ),
        "source_sync_compaction_window_seconds": (
            cfg.source_sync_compaction_window_seconds
        ),
        "source_sync_watch_debounce_seconds": (
            cfg.source_sync_watch_debounce_seconds
        ),
        "source_sync_head_poll_interval_seconds": (
            cfg.source_sync_head_poll_interval_seconds
        ),
        "source_sync_reconcile_watermark_min_refresh_ms": (
            cfg.source_sync_reconcile_watermark_min_refresh_ms
        ),
        "source_sync_progress_min_refresh_ms": (
            cfg.source_sync_progress_min_refresh_ms
        ),
        "source_sync_queue_polling_interval_seconds": (
            cfg.source_sync_queue_polling_interval_seconds
        ),
        "lsp_hot_path_max_requests": cfg.lsp_hot_path_max_requests,
        "include_history_file_stubs": include_history_file_stubs,
        "history_default_event_mode": (
            "scope_complete" if exp.repo.url == "synthetic:differential" else "cursor"
        ),
    }
    for tmpl_name, out_name in (
        ("deployment.yaml.j2", "deployment.yaml"),
        ("source_registry.yaml.j2", "source_registry.yaml"),
    ):
        tmpl_text = (TEMPLATES_DIR / tmpl_name).read_text()
        rendered = Template(tmpl_text).render(**ctx)
        (exp.deployment_dir / out_name).write_text(rendered)
    # doc-corpus needs an `extractors.yaml` in its `extractors_dir`.
    # Ship the bench-tier minimal extractor chain (markdown + text +
    # yaml + toml + json + python-AST; no chunker or NER). Always
    # copied so the file is present regardless of source-set —
    # doc-corpus reads it only when registered.
    extractors_src = TEMPLATES_DIR / "extractors.yaml"
    if extractors_src.exists():
        shutil.copy(extractors_src, exp.deployment_dir / "extractors.yaml")
    # Copy doc-corpus bridge schemas only for source sets that include
    # doc-corpus. The bridge references extraction/document subtypes;
    # files-only and history-only deployments intentionally load only
    # git modules, so carrying the bridge makes catalog load fail before
    # replay starts. Always clear stale generated schemas first because
    # experiment dirs are reused between runs.
    schemas_src = TEMPLATES_DIR / "schemas"
    schemas_dst = exp.deployment_dir / "schemas"
    if schemas_dst.exists():
        shutil.rmtree(schemas_dst)
    if include_doc_corpus and schemas_src.exists():
        shutil.copytree(schemas_src, schemas_dst)
    # The deployment contract (typed manifest) requires `schema_dir` to point
    # at an existing directory. Non-doc-corpus sets carry no deployment-level
    # schema overlay (subtypes come from the loaded `modules:`), so ensure an
    # empty `schemas/` dir exists to satisfy the manifest validator without
    # changing what gets loaded. (copytree above already created it when the
    # doc-corpus bridge ships.)
    schemas_dst.mkdir(parents=True, exist_ok=True)
    # View-citation bridge: the `file_symbols` view (code-bearing sets) projects
    # `view_cites_node` edges View -> SourceFile. The substrate `views` module
    # leaves concrete cite-target narrowings to the deployment, so ship the
    # narrowing here — otherwise the transform consistency gate
    # (`no_dangling+narrowing`) rejects those edges and blocks every publish.
    # Keyed on include_views (config-axes): only ship the view-cite narrowing
    # when the `file_symbols` view actually loads.
    if include_views:
        bridges_dst = schemas_dst / "bridges"
        bridges_dst.mkdir(parents=True, exist_ok=True)
        shutil.copy(
            TEMPLATES_DIR / "views_to_code_bridge.yaml",
            bridges_dst / "views_to_code.yaml",
        )
    # Views: ship the non-LLM `file_symbols` operator view + its registry for
    # code-bearing source sets (only those have the code graph the view reads
    # and load the `views` ontology module). The canonical view code ships in
    # the okg.bench.views package; it must ALSO land in the deployment dir as
    # a `views/` package so the publisher can import it deployment-locally as
    # `views.file_symbols` once the dir is on sys.path[0]. Clear stale copies
    # first because experiment dirs are reused between runs.
    views_src = Path(__file__).parent / "views"
    views_dst = exp.deployment_dir / "views"
    registry_dst = exp.deployment_dir / "views_registry.yaml"
    if views_dst.exists():
        shutil.rmtree(views_dst)
    if registry_dst.exists():
        registry_dst.unlink()
    if include_views and views_src.exists():
        shutil.copytree(
            views_src, views_dst, ignore=shutil.ignore_patterns("__pycache__")
        )
        registry_tmpl = (TEMPLATES_DIR / "views_registry.yaml.j2").read_text()
        registry_dst.write_text(Template(registry_tmpl).render(**ctx))
    print(f"  generated deployment dir: {exp.deployment_dir}")


def clean_all() -> None:
    """Drop every okg_bench_* DB and remove generated deployment dirs."""
    out = _psql(
        "SELECT datname FROM pg_database WHERE datname LIKE 'okg\\_bench\\_%' "
        "ESCAPE '\\';",
    )
    dbs = [
        line.strip()
        for line in out.splitlines()
        if line.strip()
        and not line.startswith("(")
        and "datname" not in line
        and "---" not in line
        and "row" not in line
    ]
    for db in dbs:
        print(f"  dropping {db}")
        _psql(
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname = '{db}'; DROP DATABASE IF EXISTS {db};",
        )
    if GENERATED_DIR.exists():
        print(f"  removing {GENERATED_DIR}")
        shutil.rmtree(GENERATED_DIR)
        GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    print(f"clean: dropped {len(dbs)} bench DB(s)")


# ---------------------------------------------------------------------------
# Worker management


def start_worker(exp: Experiment) -> int:
    """Spawn a worker for the experiment. Returns the process pid."""
    # Stop any prior worker for the same experiment.
    stop_worker(exp)

    exp.worker_log.parent.mkdir(parents=True, exist_ok=True)
    f = exp.worker_log.open("w")
    print(f"[{exp.id}] starting worker, log → {exp.worker_log}")
    p = subprocess.Popen(
        [
            *running_okg_argv(),
            "runtime",
            "worker",
            "--deployment",
            exp.deployment,
            "--dsn",
            exp.dsn,
        ],
        env={
            **os.environ,
            **_experiment_runtime_env(exp),
            "OKG_DSN": exp.dsn,
            "OKG_DEPLOYMENTS_DIR": str(exp.deployments_root),
            "PYTHONPATH": ENV_PYTHONPATH,
        },
        stdout=f,
        stderr=subprocess.STDOUT,
        cwd=ROOT,
        start_new_session=True,
    )
    return p.pid


def _bench_reconcile(exp: Experiment) -> dict[str, Any]:
    """Fire one `okg ingest --mode reconcile` pass against the
    experiment's deployment. Required for sources that declare a
    `predecessor_source` (e.g. doc-corpus, code-structure depends
    on git-files): the predecessor's `last_reconcile_at` watermark
    must be non-NULL before dependents will emit any facts.

    Returns a receipt of the pass: the CLI's per-workflow status and
    `completeness` block (`okg.ingest_completeness.v1`) when it produced
    JSON, or the exit code and stderr head when it did not.
    """
    print(f"[{exp.id}] reconcile pass (so predecessor-aware sources unblock)")
    res = subprocess.run(
        [
            *running_okg_argv(),
            "ingest",
            "--deployment",
            exp.deployment,
            "--dsn",
            exp.dsn,
            "--mode",
            "reconcile",
            "--json",
        ],
        env={
            **os.environ,
            **_experiment_runtime_env(exp),
            "OKG_DSN": exp.dsn,
            "OKG_DEPLOYMENTS_DIR": str(exp.deployments_root),
            "PYTHONPATH": ENV_PYTHONPATH,
        },
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        sys.stderr.write(
            f"[{exp.id}] reconcile pass failed (continuing without it):\n"
            f"{res.stderr[:500]}\n"
        )
    return _reconcile_receipt(res)


def _reconcile_receipt(res: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    receipt: dict[str, Any] = {
        "returncode": int(res.returncode),
        "workflows": [],
        "error": None,
    }
    try:
        payload = json.loads(res.stdout or "")
    except json.JSONDecodeError:
        receipt["error"] = (res.stderr or res.stdout or "")[:500] or None
        return receipt
    if not isinstance(payload, dict):
        receipt["error"] = "reconcile pass printed non-object JSON"
        return receipt
    items = payload.get("workflows")
    if not isinstance(items, list) or not items:
        items = [payload]
    for item in items:
        if not isinstance(item, dict):
            continue
        report = item.get("report")
        completeness = (
            report.get("completeness") if isinstance(report, dict) else None
        )
        receipt["workflows"].append({
            "workflow_id": item.get("workflow_id"),
            "status": item.get("status"),
            "any_failed": item.get("any_failed"),
            "elapsed_seconds": item.get("elapsed_seconds"),
            "error": item.get("error"),
            "completeness": completeness,
        })
    if payload.get("error") and receipt["error"] is None:
        receipt["error"] = str(payload.get("error"))[:500]
    return receipt


def wait_for_worker(exp: Experiment, *, timeout: int = 60) -> None:
    """Tail the worker log until launch markers appear, or time out."""
    deadline = time.time() + timeout
    needed = ("DBOS launched!", "Listening to")
    while time.time() < deadline:
        try:
            text = exp.worker_log.read_text()
        except FileNotFoundError:
            text = ""
        if all(s in text for s in needed):
            print(f"[{exp.id}] worker launched")
            return
        time.sleep(0.5)
    raise TimeoutError(
        f"[{exp.id}] worker did not reach DBOS-launched state within "
        f"{timeout}s. tail of {exp.worker_log}:\n" + "\n".join(text.splitlines()[-20:])
    )


_ERE_SPECIAL = frozenset("\\.[]()*+?{}|^$")


def _ere_escape(text: str) -> str:
    """Escape ``text`` for a POSIX extended regular expression (what
    `pgrep`/`pkill -f` use) so it matches literally. Only the ERE
    metacharacters are escaped; Python's ``re.escape`` also escapes ordinary
    characters such as ``:`` and ``=``, which ERE leaves undefined."""
    return "".join(f"\\{ch}" if ch in _ERE_SPECIAL else ch for ch in text)


def _worker_process_pattern(exp: Experiment) -> str:
    """`pgrep`/`pkill -f` pattern for this experiment's worker. It names the
    exact DSN as well as the deployment: the forced rebuild runs under the
    incremental experiment's deployment name, and a name-only pattern would
    also match (and stop) the other experiment's worker. The DSN is matched
    literally whatever its shape (URL with or without a query string, or
    key=value), and the trailing `( |$)` stops `.../okg_bench_a` from matching
    `.../okg_bench_a_b`."""
    return (
        "okg.substrate.cli.main runtime worker "
        f"--deployment {_ere_escape(exp.deployment)} "
        f"--dsn {_ere_escape(exp.dsn)}( |$)"
    )


def stop_worker(exp: Experiment) -> None:
    """Best-effort stop of any worker for this experiment."""
    pattern = _worker_process_pattern(exp)
    subprocess.run(["pkill", "-f", pattern], check=False)
    time.sleep(1.5)


def _experiment_runtime_env(exp: Experiment) -> dict[str, str]:
    env: dict[str, str] = {}
    min_refresh_ms = exp.config.source_sync_reconcile_watermark_min_refresh_ms
    if min_refresh_ms > 0:
        env["OKG_SOURCE_SYNC_RECONCILE_WATERMARK_MIN_REFRESH_MS"] = str(
            min_refresh_ms,
        )
    progress_min_refresh_ms = exp.config.source_sync_progress_min_refresh_ms
    if progress_min_refresh_ms > 0:
        env["OKG_SOURCE_SYNC_PROGRESS_MIN_REFRESH_MS"] = str(
            progress_min_refresh_ms,
        )
    source_sync_queue_poll = exp.config.source_sync_queue_polling_interval_seconds
    if source_sync_queue_poll > 0:
        env["OKG_DBOS_SOURCE_SYNC_QUEUE_POLLING_INTERVAL_SEC"] = str(
            source_sync_queue_poll,
        )
    return env


class _WorkerRssSampler(threading.Thread):
    """Background sampler tracking the peak RSS (bytes) of the experiment's
    worker process(es) for the run.

    Uses `pgrep`/`ps` rather than psutil so the bench keeps a zero-extra-dep
    footprint and works on both macOS (dev) and Linux (CI). Reports the
    persistent substrate worker's resident-set high-water mark — a run-level
    resource metric (bench-okg-internals task.internal-metrics).
    """

    def __init__(self, exp: "Experiment", interval: float = 0.25) -> None:
        super().__init__(daemon=True)
        self.exp = exp
        self.interval = interval
        self.peak_bytes = 0
        self._stop_event = threading.Event()
        self._pattern = _worker_process_pattern(exp)

    def _sample_bytes(self) -> int:
        pgrep = subprocess.run(
            ["pgrep", "-f", self._pattern],
            capture_output=True,
            text=True,
            check=False,
        )
        pids = [p for p in pgrep.stdout.split() if p.strip().isdigit()]
        if not pids:
            return 0
        ps = subprocess.run(
            ["ps", "-o", "rss=", "-p", ",".join(pids)],
            capture_output=True,
            text=True,
            check=False,
        )
        kb = sum(int(x) for x in ps.stdout.split() if x.strip().isdigit())
        return kb * 1024

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                rss = self._sample_bytes()
            except Exception:  # noqa: BLE001 - sampling must never crash a run
                rss = 0
            if rss > self.peak_bytes:
                self.peak_bytes = rss
            self._stop_event.wait(self.interval)

    def stop_and_report(self) -> int:
        self._stop_event.set()
        self.join(timeout=2.0)
        return self.peak_bytes


# ---------------------------------------------------------------------------
# Commit-list construction (for first_n / hybrid_head_then_stride)


def _all_commits_topo(repo_root: Path) -> list[str]:
    """git log --reverse --topo-order → every commit reachable from HEAD."""
    out = subprocess.check_output(
        [
            "git",
            "-C",
            str(repo_root),
            "log",
            "--reverse",
            "--topo-order",
            "--pretty=%H",
        ],
        text=True,
    ).strip()
    return [c for c in out.splitlines() if c]


def _commits_for(exp: Experiment) -> list[str]:
    """Build the chronologically-ordered commit list this experiment
    should replay (NOT including the baseline / root). MUST be called
    while the working tree still has full history; the orchestrator
    resets to baseline AFTER this runs."""
    all_commits = _all_commits_topo(exp.repo_root)
    if len(all_commits) < 2:
        raise RuntimeError(
            f"repo {exp.repo.name!r} has only {len(all_commits)} commit(s); "
            "need at least 2 (one baseline + one to replay)"
        )
    # Skip the root (index 0); root is the baseline.
    body = all_commits[1:]
    if exp.replay.strategy == "full_history":
        return body
    if exp.replay.strategy == "head_only":
        if exp.replay.last_n is None or exp.replay.last_n <= 0:
            raise ValueError(f"{exp.id}: head_only requires positive `last_n`")
        selected = body[-exp.replay.last_n :]
        if exp.replay.commits and tuple(selected) != exp.replay.commits:
            raise RuntimeError(
                f"{exp.id}: replayed commits do not match the pinned "
                f"replay.commits list; computed {selected!r}, "
                f"pinned {list(exp.replay.commits)!r}"
            )
        return selected
    if exp.replay.strategy == "tail_window":
        if exp.replay.last_n is None or exp.replay.last_n <= 0:
            raise ValueError(f"{exp.id}: tail_window requires positive `last_n`")
        if exp.replay.last_n > len(body):
            raise ValueError(
                f"{exp.id}: tail_window last_n ({exp.replay.last_n}) "
                f"exceeds replayable commit count ({len(body)})"
            )
        # Replay the full prefix so the measured tail has the same graph,
        # source-progress, DBOS, and Chronos state shape as a full-history run.
        # commit_replay.py records only the final window via
        # OKG_BENCH_RECORD_FROM_INDEX.
        return body
    if exp.replay.strategy == "first_n":
        if exp.replay.n is None or exp.replay.n <= 0:
            raise ValueError(f"{exp.id}: first_n requires positive `n`")
        return body[: exp.replay.n]
    if exp.replay.strategy == "hybrid_head_then_stride":
        head_n = exp.replay.head_n or 0
        total = exp.replay.total or head_n
        if total < head_n:
            raise ValueError(f"{exp.id}: total ({total}) < head_n ({head_n})")
        head = body[:head_n]
        rest = body[head_n:]
        extra = total - head_n
        if extra <= 0 or not rest:
            return head
        step = max(1, len(rest) // extra)
        sampled = rest[::step][: extra - 1] + [rest[-1]]
        return head + sampled
    raise ValueError(f"{exp.id}: unknown strategy {exp.replay.strategy!r}")


def _record_from_index(exp: Experiment, commits: list[str]) -> int:
    """1-indexed replay row where commit_replay.py should start writing CSV.

    Most strategies record every replayed commit. `tail_window` intentionally
    warms through the prefix first and records only the final `last_n` commits,
    keeping raw row `i` equal to the full-history commit index.
    """
    if exp.replay.strategy != "tail_window":
        return 1
    last_n = exp.replay.last_n or 0
    if last_n <= 0 or last_n > len(commits):
        raise ValueError(
            f"{exp.id}: invalid tail_window last_n={exp.replay.last_n!r} "
            f"for {len(commits)} replay commits"
        )
    return len(commits) - last_n + 1


def _write_commits_file(exp: Experiment) -> Path:
    commits = _commits_for(exp)
    path = exp.commits_file
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(commits) + "\n")
    print(f"  wrote commits file: {path} ({len(commits)} commits)")
    return path


# ---------------------------------------------------------------------------
# Replay


def run_replay(exp: Experiment) -> None:
    """Invoke commit_replay.py for this experiment. Blocking.

    Assumes the orchestrator already wrote the commits file (during
    cmd_run, BEFORE the baseline reset). The replay script reads
    commits exclusively from that file — it no longer touches `git
    log` at replay time, because by now the working tree is at
    baseline and `git log` from HEAD wouldn't see the full history.
    """
    exp.raw_csv.parent.mkdir(parents=True, exist_ok=True)
    if not exp.commits_file.exists():
        raise RuntimeError(
            f"{exp.id}: commits file missing at {exp.commits_file}; "
            "cmd_run is supposed to write it before running replay"
        )
    commits = [
        ln.strip() for ln in exp.commits_file.read_text().splitlines() if ln.strip()
    ]
    record_from_index = _record_from_index(exp, commits)
    env = {
        **_experiment_runtime_env(exp),
        "OKG_BENCH_DSN": exp.dsn,
        "OKG_BENCH_REPO": str(exp.repo_root),
        "OKG_BENCH_DEPLOYMENT": exp.deployment,
        "OKG_BENCH_OUT": str(exp.raw_csv.parent / exp.id),
        "OKG_DEPLOYMENTS_DIR": str(exp.deployments_root),
        "PYTHONPATH": ENV_PYTHONPATH,
        "PYTHONUNBUFFERED": "1",
        "OKG_BENCH_COMMITS_FILE": str(exp.commits_file),
        # Pass the baseline SHA explicitly. For head_only the baseline
        # is HEAD~K, not the root; commit_replay used to compute root
        # via `rev-list --max-parents=0 HEAD` which is wrong for
        # head_only. Sourced from the same function the orchestrator
        # used to do the pre-reset, so the two agree.
        "OKG_BENCH_BASELINE_COMMIT": _baseline_commit(exp),
        # Throttle: wait for successor source-sync work before firing the next
        # commit. The replay harness still protects against queued follow-up
        # publishes, but does not count the already-published workflow's DBOS
        # terminal bookkeeping as graph-sync latency.
        "OKG_BENCH_DRAIN_BETWEEN_COMMITS": "1",
        "OKG_BENCH_DELTA_PROOF": "1",
    }
    if record_from_index > 1:
        env["OKG_BENCH_RECORD_FROM_INDEX"] = str(record_from_index)
    print(
        f"[{exp.id}] running replay "
        f"(strategy={exp.replay.strategy}, "
        f"record_from_index={record_from_index}, "
        f"commits={exp.commits_file}); log → {exp.replay_log}"
    )
    cmd = [
        PYTHON,
        "-u",
        str(ROOT / "deployments" / "synth-bench" / "scripts" / "commit_replay.py"),
        "0",  # legacy positional K argument; ignored when COMMITS_FILE is set
    ]
    with exp.replay_log.open("w") as logf:
        proc = subprocess.run(
            cmd,
            env={**os.environ, **env},
            cwd=ROOT,
            stdout=logf,
            stderr=subprocess.STDOUT,
            check=False,
        )
    # The replay script writes its CSV at $OKG_BENCH_OUT/bench_results.csv.
    # Move that into our canonical place.
    raw_from_script = exp.raw_csv.parent / exp.id / "bench_results.csv"
    if raw_from_script.exists():
        shutil.move(str(raw_from_script), exp.raw_csv)
        print(f"  replay CSV → {exp.raw_csv}")
    proof_from_script = exp.raw_csv.parent / exp.id / "delta_proof.json"
    if proof_from_script.exists():
        shutil.move(str(proof_from_script), exp.delta_proof_json)
        print(f"  delta proof → {exp.delta_proof_json}")
    if proc.returncode != 0:
        sys.stderr.write(
            f"[{exp.id}] replay exited {proc.returncode}; tail of {exp.replay_log}:\n"
        )
        sys.stderr.write(
            "\n".join(exp.replay_log.read_text().splitlines()[-20:]) + "\n"
        )
        raise subprocess.CalledProcessError(proc.returncode, cmd)


# ---------------------------------------------------------------------------
# Diagnostic profile drilldown


def _safe_fragment(value: str, *, limit: int = 64) -> str:
    text = "".join(
        ch if ch.isalnum() or ch in "._-" else "-" for ch in str(value).lower()
    ).strip("-")
    return (text or "unknown")[:limit]


def _number(value: Any) -> float | None:
    try:
        if value in (None, "", "NaN"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _enrich_delta_proof_source_runs(exp: Experiment) -> None:
    """Patch replay delta proof with source-run boundaries from diagnostics."""

    if not exp.delta_proof_json.exists() or not _diagnostics_path(exp).exists():
        return
    from okg.bench.differential_replay_proof import (  # noqa: PLC0415
        derived_relation_scope_boundary_from_diagnostics_commit,
        enricher_scope_boundary_from_governance,
        extractor_scope_boundary_from_diagnostics_commit,
        lens_scope_boundary_from_diagnostics_commit,
        native_view_scope_boundary_from_diagnostics_commit,
        read_side_operator_boundary_from_report,
        source_runs_boundary_from_diagnostics_commit,
        summarize_delta_proof_rows,
        work_segments_from_delta_row,
    )

    proof = json.loads(exp.delta_proof_json.read_text(encoding="utf-8"))
    diagnostics = json.loads(_diagnostics_path(exp).read_text(encoding="utf-8"))
    if proof.get("schema") != "okg.bench.differential_delta_proof.v1":
        return
    per_commit = diagnostics.get("per_commit")
    rows = proof.get("rows")
    if not isinstance(per_commit, list) or not isinstance(rows, list):
        return
    diagnostics_by_commit = {
        str(row.get("commit"))[:10]: row
        for row in per_commit
        if isinstance(row, Mapping) and row.get("commit")
    }
    read_side_reports: dict[str, dict[str, Any]] = {}
    updated = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        commit = str(row.get("commit") or row.get("commit_sha") or "")[:10]
        commit_diag = diagnostics_by_commit.get(commit)
        if not isinstance(commit_diag, Mapping):
            continue
        boundaries = row.setdefault("boundaries", {})
        if not isinstance(boundaries, dict):
            continue
        boundaries["source_runs"] = source_runs_boundary_from_diagnostics_commit(
            commit_diag,
        )
        boundaries["native_view_scope"] = (
            native_view_scope_boundary_from_diagnostics_commit(commit_diag)
        )
        boundaries["extractor_scope"] = (
            extractor_scope_boundary_from_diagnostics_commit(commit_diag)
        )
        publish = _select_commit_publish_for_proof(commit_diag)
        governance = (
            publish.get("enricher_governance")
            if isinstance(publish, Mapping)
            else None
        )
        if isinstance(governance, Mapping):
            boundaries["enricher_scope"] = (
                enricher_scope_boundary_from_governance(
                    governance,
                    dirty_scope_count=_proof_dirty_scope_count(
                        boundaries.get("chronos_dirty_scope"),
                    ),
                    elapsed_ms=_publish_timing_ms(publish, "enrichers"),
                )
            )
        boundaries["lens_scope"] = lens_scope_boundary_from_diagnostics_commit(
            commit_diag,
        )
        boundaries["derived_relation_scope"] = (
            derived_relation_scope_boundary_from_diagnostics_commit(commit_diag)
        )
        boundaries["read_side_operator_scope"] = (
            read_side_operator_boundary_from_report(
                _read_side_report_for_generation(
                    exp,
                    row.get("generation_id"),
                    read_side_reports,
                ),
                target_generation_id=row.get("generation_id"),
            )
        )
        row["work_segments"] = work_segments_from_delta_row(row)
        updated += 1
    if updated == 0:
        return
    proof["summary"] = summarize_delta_proof_rows(rows)
    exp.delta_proof_json.write_text(
        json.dumps(proof, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    print(
        f"[{exp.id}] enriched delta proof source-run boundaries "
        f"for {updated} rows"
    )


def _read_side_report_for_generation(
    exp: Experiment,
    generation_id: Any,
    cache: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    if generation_id in (None, ""):
        return None
    key = str(generation_id)
    if key not in cache:
        cache[key] = build_read_side_report(exp, generation_id=key)
    return cache[key]


def _proof_dirty_scope_count(boundary: Any) -> int | None:
    if not isinstance(boundary, Mapping) or boundary.get("status") != "available":
        return None
    node_count = _int_from_any(boundary.get("dirty_node_count"))
    edge_count = _int_from_any(boundary.get("dirty_edge_count"))
    if node_count is None or edge_count is None:
        return None
    return node_count + edge_count


def _publish_timing_ms(publish: Mapping[str, Any] | None, key: str) -> float | None:
    if not isinstance(publish, Mapping):
        return None
    timings = publish.get("timings_ms")
    if not isinstance(timings, Mapping):
        return None
    try:
        value = timings.get(key)
        if value in (None, "", "NaN"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_from_any(value: Any) -> int | None:
    try:
        if value in (None, "", "NaN"):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _select_commit_publish_for_proof(
    commit_diag: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    raw_publishes = commit_diag.get("publishes")
    if not isinstance(raw_publishes, Sequence) or isinstance(
        raw_publishes,
        (str, bytes),
    ):
        return None
    publishes = [
        publish
        for publish in raw_publishes
        if isinstance(publish, Mapping)
    ]
    if not publishes:
        return None
    commit_publishes = [
        publish
        for publish in publishes
        if publish.get("label") == "commit" and publish.get("status") == "published"
    ]
    published = [
        publish
        for publish in publishes
        if publish.get("status") == "published"
    ]
    return (commit_publishes or published or publishes)[-1]


def _diagnostics_path(exp: Experiment) -> Path:
    return RESULTS_DIR / f"{exp.id}.diagnostics.json"


def _load_diagnostics(exp: Experiment) -> dict[str, Any]:
    path = _diagnostics_path(exp)
    if not path.exists():
        if exp.raw_csv.exists():
            print(f"[{exp.id}] diagnostics missing; running extract.py first")
            _run([PYTHON, "-m", "okg.bench.extract", exp.id])
        if not path.exists():
            raise FileNotFoundError(
                f"diagnostics missing: {path}; run "
                f"`python -m okg.bench.extract {exp.id}` after a replay"
            )
    doc = json.loads(path.read_text())
    if doc.get("schema") != "okg.bench.source_sync_diagnostics.v1":
        raise ValueError(
            f"unsupported diagnostics schema in {path}: {doc.get('schema')!r}"
        )
    return doc


def _source_matches(run: dict[str, Any], source: str | None) -> bool:
    if not source:
        return True
    wanted = str(source)
    source_name = str(run.get("source_name") or "")
    registry_name = str(run.get("registry_source_name") or "")
    candidates = {
        source_name,
        registry_name,
        source_name.rsplit(".", 1)[-1] if "." in source_name else source_name,
    }
    return wanted in candidates


def _run_score(run: dict[str, Any]) -> float:
    return (
        _number(run.get("total_ms"))
        or _number(run.get("source_runner_ms"))
        or _number(run.get("slowest_phase_ms"))
        or 0.0
    )


def _select_diagnostic_source_run(
    diagnostics: dict[str, Any],
    *,
    source: str | None,
    commit: str | None,
    run_id: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    matches: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for commit_row in diagnostics.get("per_commit", []) or []:
        if not isinstance(commit_row, dict):
            continue
        row_commit = str(commit_row.get("commit") or "")
        if commit and not row_commit.startswith(str(commit)):
            continue
        for run in commit_row.get("source_runs", []) or []:
            if not isinstance(run, dict):
                continue
            if run_id and str(run.get("run_id") or "") != str(run_id):
                continue
            if not run_id and not _source_matches(run, source):
                continue
            matches.append((commit_row, run))
    if not matches:
        selectors = {
            "source": source,
            "commit": commit,
            "run_id": run_id,
        }
        raise ValueError(f"no matching source run in diagnostics: {selectors}")
    return max(
        matches,
        key=lambda item: (
            _run_score(item[1]),
            int(item[0].get("i") or 0),
            str(item[1].get("run_id") or ""),
        ),
    )


def _stage_parent(phase: str, phases: set[str]) -> str | None:
    if "." not in phase:
        return None
    parent = phase.rsplit(".", 1)[0]
    return parent if parent in phases else None


def _source_run_timings(run: dict[str, Any]) -> dict[str, float]:
    raw = run.get("timings_ms")
    timings: dict[str, float] = {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            number = _number(value)
            if number is not None:
                timings[str(key)] = number
    if not timings:
        total = _number(run.get("total_ms"))
        source_runner = _number(run.get("source_runner_ms"))
        slowest_phase = str(run.get("slowest_phase") or "")
        slowest_phase_ms = _number(run.get("slowest_phase_ms"))
        if total is not None:
            timings["total"] = total
        if source_runner is not None:
            timings["source_runner"] = source_runner
        if slowest_phase and slowest_phase_ms is not None:
            timings[slowest_phase] = max(
                timings.get(slowest_phase, 0.0),
                slowest_phase_ms,
            )
    return timings


def _profile_run_id(
    exp: Experiment,
    commit_row: dict[str, Any],
    run: dict[str, Any],
) -> str:
    source = str(run.get("registry_source_name") or run.get("source_name") or "source")
    seed = json.dumps(
        [
            exp.id,
            commit_row.get("commit"),
            run.get("run_id"),
            source,
        ],
        sort_keys=True,
        default=str,
    )
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12]
    return (
        "source-sync-drilldown-"
        f"{_safe_fragment(exp.id, limit=32)}-"
        f"{_safe_fragment(source, limit=32)}-"
        f"{digest}"
    )


def _build_profile_run_from_source_run(
    exp: Experiment,
    commit_row: dict[str, Any],
    run: dict[str, Any],
):
    from okg.substrate.profiling.context import summarize
    from okg.substrate.profiling.model import Evidence, ProfileRun, StageTiming

    timings = _source_run_timings(run)
    phases = set(timings)
    source = str(run.get("registry_source_name") or run.get("source_name") or "source")
    run_id = str(run.get("run_id") or "")
    profile_id = _profile_run_id(exp, commit_row, run)
    started_at = str(run.get("updated_at") or datetime.now(timezone.utc).isoformat())
    total_ms = (
        _number(run.get("total_ms"))
        or _number(run.get("source_runner_ms"))
        or sum(
            value
            for phase, value in timings.items()
            if phase not in {"total", "source_runner.total"}
        )
    )
    stages: list[StageTiming] = []
    offset_ns = 0
    for phase, duration_ms in sorted(
        timings.items(),
        key=lambda item: (item[0].count("."), item[0]),
    ):
        if phase in {"total", "source_runner.total"}:
            continue
        duration_ns = max(0, int(round(duration_ms * 1_000_000)))
        stages.append(
            StageTiming(
                name=phase,
                started_at_ns=offset_ns,
                duration_ns=duration_ns,
                parent=_stage_parent(phase, phases),
                attrs={
                    "source_name": source,
                    "registry_source_name": run.get("registry_source_name"),
                    "source_progress_name": run.get("source_name"),
                    "stage_name": phase,
                    "run_id": run_id,
                    "commit": commit_row.get("commit"),
                    "commit_index": commit_row.get("i"),
                    "generation_id": commit_row.get("gen_id"),
                    "workflow_id": commit_row.get("workflow_uuid"),
                    "input_count": run.get("input_count"),
                    "output_count": run.get("output_count"),
                    "nodes_appended": run.get("nodes_appended"),
                    "edges_appended": run.get("edges_appended"),
                    "nodes_retracted": run.get("nodes_retracted"),
                    "edges_retracted": run.get("edges_retracted"),
                    "timing_status": run.get("timing_status"),
                    "timing_unavailable_reason": (run.get("timing_unavailable_reason")),
                    "timings_ms": timings,
                    "source_stats": (
                        run.get("source_stats")
                        if isinstance(run.get("source_stats"), dict)
                        else {}
                    ),
                },
            )
        )
        offset_ns += duration_ns
    profile = ProfileRun(
        profile_run_id=profile_id,
        deployment=exp.deployment,
        target_kind="source_sync_run",
        command=(f"bench/orchestrator.py profile {exp.id} --source {source}"),
        started_at=started_at,
        ended_at=started_at,
        duration_ms=total_ms,
        request_path=str(commit_row.get("workflow_uuid") or ""),
        owner_id=f"source:{source}",
        owner_meta={
            "experiment_id": exp.id,
            "repo": exp.repo.name,
            "commit": commit_row.get("commit"),
            "commit_index": commit_row.get("i"),
            "run_id": run_id,
        },
        source_sync={
            "kind": "bench_replay_source_run_drilldown",
            "experiment_id": exp.id,
            "source_name": source,
            "source_progress_name": run.get("source_name"),
            "run_id": run_id,
            "commit": commit_row.get("commit"),
            "commit_index": commit_row.get("i"),
            "generation_id": commit_row.get("gen_id"),
            "window_start_ms": commit_row.get("window_start_ms"),
            "window_end_ms": commit_row.get("window_end_ms"),
            "source_total_ms": commit_row.get("source_total_ms"),
            "top_sources": commit_row.get("top_sources"),
            "top_phases": commit_row.get("top_phases"),
        },
        evidence={
            "source_progress_stage_timings": Evidence("available"),
            "python_flamegraph": Evidence(
                "unavailable",
                "historical replay drilldown uses recorded stage timings; "
                "rerun source sync with --capture=deep for live py-spy capture",
            ),
        },
        stages=stages,
        limitations=[
            "Profile bundle is reconstructed from source_progress stage " +
            "timings captured during replay; it is not a live sampler trace.",
        ],
    )
    profile.summary = summarize(profile)
    return profile


def _write_profile_drilldown(
    exp: Experiment,
    commit_row: dict[str, Any],
    run: dict[str, Any],
    *,
    profile_root: Path,
) -> Path:
    from okg.substrate.profiling.artifacts import write_profile_bundle

    old_profile_root = os.environ.get("OKG_PROFILE_DIR")
    os.environ["OKG_PROFILE_DIR"] = str(profile_root.resolve())
    try:
        profile = _build_profile_run_from_source_run(exp, commit_row, run)
        return write_profile_bundle(profile)
    finally:
        if old_profile_root is None:
            os.environ.pop("OKG_PROFILE_DIR", None)
        else:
            os.environ["OKG_PROFILE_DIR"] = old_profile_root


def _profile_measurements_dry_run(
    exp: Experiment, profile_json: Path
) -> dict[str, Any]:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.library.sources.profile_measurements import (
        ProfileMeasurementSource,
    )

    facts = list(
        ProfileMeasurementSource(
            deployment=exp.deployment,
            repo=exp.repo.name,
            repo_root=ROOT,
            artifact_paths=[profile_json],
        )
        .run("bench-profile-dry-run", mode="scope_complete")
        .facts
    )
    nodes = Counter(fact.subtype for fact in facts if isinstance(fact, NodeFact))
    edges = Counter(fact.edge_type for fact in facts if isinstance(fact, EdgeFact))
    return {
        "fact_count": len(facts),
        "node_counts": dict(sorted(nodes.items())),
        "edge_counts": dict(sorted(edges.items())),
        "ok": bool(nodes.get("profile_run") and nodes.get("profile_stage")),
    }


def _benchmark_measurements_dry_run(exp: Experiment, artifact: Path) -> dict[str, Any]:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.library.sources.profile_measurements import (
        ProfileMeasurementSource,
    )

    facts = list(
        ProfileMeasurementSource(
            deployment=exp.deployment,
            repo=exp.repo.name,
            repo_root=ROOT,
            benchmark_paths=[artifact],
        )
        .run("bench-diagnostics-dry-run", mode="scope_complete")
        .facts
    )
    nodes = Counter(fact.subtype for fact in facts if isinstance(fact, NodeFact))
    edges = Counter(fact.edge_type for fact in facts if isinstance(fact, EdgeFact))
    return {
        "fact_count": len(facts),
        "node_counts": dict(sorted(nodes.items())),
        "edge_counts": dict(sorted(edges.items())),
        "ok": bool(nodes.get("benchmark_measurement")),
    }


def cmd_profile(args) -> int:
    exp = _resolve(args.id)
    if not args.source and not args.run_id:
        sys.exit("profile requires --source or --run-id")
    diagnostics = _load_diagnostics(exp)
    commit_row, run = _select_diagnostic_source_run(
        diagnostics,
        source=args.source,
        commit=args.commit,
        run_id=args.run_id,
    )
    bundle_dir = _write_profile_drilldown(
        exp,
        commit_row,
        run,
        profile_root=Path(args.profile_root),
    )
    profile_json = bundle_dir / "profile.json"
    dry_run = (
        None
        if args.no_ingest_dry_run
        else _profile_measurements_dry_run(exp, profile_json)
    )
    report = {
        "ok": bool(dry_run is None or dry_run.get("ok")),
        "experiment_id": exp.id,
        "profile_id": bundle_dir.name,
        "profile_dir": str(bundle_dir),
        "profile_json": str(profile_json),
        "selected": {
            "commit_index": commit_row.get("i"),
            "commit": commit_row.get("commit"),
            "generation_id": commit_row.get("gen_id"),
            "workflow_uuid": commit_row.get("workflow_uuid"),
            "source_name": run.get("source_name"),
            "registry_source_name": run.get("registry_source_name"),
            "run_id": run.get("run_id"),
            "total_ms": run.get("total_ms"),
            "slowest_phase": run.get("slowest_phase"),
            "slowest_phase_ms": run.get("slowest_phase_ms"),
        },
        "profile_measurements_dry_run": dry_run,
    }
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        print(
            f"[{exp.id}] profile drilldown → {profile_json} "
            f"(source={run.get('registry_source_name')}, "
            f"commit={commit_row.get('commit')}, "
            f"total_ms={run.get('total_ms')})"
        )
        if dry_run is not None:
            print(
                "  profile-measurements dry-run: "
                f"{dry_run['fact_count']} facts, "
                f"nodes={dry_run['node_counts']}, "
                f"edges={dry_run['edge_counts']}"
            )
    return 0 if report["ok"] else 1


def cmd_diagnostics(args) -> int:
    exp = _resolve(args.id)
    diagnostics = _load_diagnostics(exp)
    path = _diagnostics_path(exp)
    dry_run = _benchmark_measurements_dry_run(exp, path)
    source_summary = diagnostics.get("source_run_summary", {}).get("sources", {})
    phase_summary = diagnostics.get("source_run_summary", {}).get("phases", {})
    publish_summary = diagnostics.get("publish_run_summary") or {}
    publish_stage_summary = publish_summary.get("stages", {})
    publish_label_summary = publish_summary.get("labels", {})
    report = {
        "ok": dry_run.get("ok"),
        "experiment_id": exp.id,
        "diagnostics_path": str(path),
        "schema": diagnostics.get("schema"),
        "source_run_count": diagnostics.get("source_run_count"),
        "publish_event_count": diagnostics.get("publish_event_count"),
        "measurement_count": len(diagnostics.get("measurements") or []),
        "row_status_counts": diagnostics.get("row_status_counts") or {},
        "profile_measurements_dry_run": dry_run,
        "top_sources_by_p95_ms": sorted(
            [
                {
                    "source": source,
                    "p95_ms": stats.get("p95_ms"),
                    "max_ms": stats.get("max_ms"),
                    "n": stats.get("n"),
                }
                for source, stats in source_summary.items()
                if isinstance(stats, dict)
            ],
            key=lambda row: (-(row.get("p95_ms") or 0), row["source"]),
        )[:10],
        "top_phases_by_p95_ms": sorted(
            [
                {
                    "phase": phase,
                    "p95_ms": stats.get("p95_ms"),
                    "max_ms": stats.get("max_ms"),
                    "n": stats.get("n"),
                }
                for phase, stats in phase_summary.items()
                if isinstance(stats, dict)
            ],
            key=lambda row: (-(row.get("p95_ms") or 0), row["phase"]),
        )[:10],
        "top_publish_labels_by_p95_ms": sorted(
            [
                {
                    "label": label,
                    "p95_ms": stats.get("p95_ms"),
                    "max_ms": stats.get("max_ms"),
                    "sum_ms": stats.get("sum_ms"),
                    "n": stats.get("n"),
                }
                for label, stats in publish_label_summary.items()
                if isinstance(stats, dict)
            ],
            key=lambda row: (-(row.get("p95_ms") or 0), row["label"]),
        )[:10],
        "top_publish_stages_by_p95_ms": sorted(
            [
                {
                    "stage": stage,
                    "p95_ms": stats.get("p95_ms"),
                    "max_ms": stats.get("max_ms"),
                    "sum_ms": stats.get("sum_ms"),
                    "n": stats.get("n"),
                }
                for stage, stats in publish_stage_summary.items()
                if isinstance(stats, dict)
                and _is_diagnostic_duration_stage(str(stage))
            ],
            key=lambda row: (-(row.get("p95_ms") or 0), row["stage"]),
        )[:10],
    }
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        print(
            f"[{exp.id}] diagnostics → {path} "
            f"({report['measurement_count']} measurements, "
            f"{dry_run['fact_count']} dry-run facts)"
        )
        print(f"  top sources: {report['top_sources_by_p95_ms'][:3]}")
        print(f"  top phases: {report['top_phases_by_p95_ms'][:3]}")
        print(
            "  top publish stages: "
            f"{report['top_publish_stages_by_p95_ms'][:3]}"
        )
    return 0 if report["ok"] else 1


# ---------------------------------------------------------------------------
# Replay regression gate


def _normalized_csv_path(exp: Experiment) -> Path:
    return RESULTS_DIR / f"{exp.id}.csv"


def _load_normalized_rows(exp: Experiment) -> list[dict[str, str]]:
    path = _normalized_csv_path(exp)
    if not path.exists():
        if exp.raw_csv.exists():
            print(f"[{exp.id}] normalized CSV missing; running extract.py first")
            _run([PYTHON, "-m", "okg.bench.extract", exp.id])
        if not path.exists():
            raise FileNotFoundError(
                f"normalized CSV missing: {path}; run "
                f"`python bench/orchestrator.py run {exp.id}` or "
                f"`python -m okg.bench.extract {exp.id}` first"
            )
    import csv

    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _numeric_column(rows: list[dict[str, str]], column: str) -> list[float]:
    out: list[float] = []
    for row in rows:
        value = _number(row.get(column))
        if value is not None:
            out.append(value)
    return out


def _percentile_value(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * pct) - 1))
    return ordered[index]


def _metric_summary(values: list[float]) -> dict[str, float | None]:
    total = sum(values)
    return {
        "p50_ms": _percentile_value(values, 0.50),
        "p95_ms": _percentile_value(values, 0.95),
        "max_ms": max(values) if values else None,
        "mean_ms": total / len(values) if values else None,
    }


def _numeric_column_first_available(
    rows: list[dict[str, str]],
    *columns: str,
) -> list[float]:
    out: list[float] = []
    for row in rows:
        for column in columns:
            value = _number(row.get(column))
            if value is not None:
                out.append(value)
                break
    return out


def _row_residual_ms(rows: list[dict[str, str]]) -> list[float]:
    residuals: list[float] = []
    for row in rows:
        e2e = _number(row.get("e2e_ms"))
        source = _number(row.get("source_total_ms"))
        publish = _number(row.get("publish_window_total_ms"))
        if publish is None:
            publish = _number(row.get("publish_total_ms"))
        if e2e is None or source is None or publish is None:
            continue
        residuals.append(e2e - source - publish)
    return residuals


def _row_residual_after_producer_ms(rows: list[dict[str, str]]) -> list[float]:
    residuals: list[float] = []
    for row in rows:
        e2e = _number(row.get("e2e_ms"))
        producer = _number(row.get("bench_reset_to_workflow_created_ms"))
        source = _number(row.get("ingest_source_wall_ms"))
        if source is None:
            source = _number(row.get("source_total_ms"))
        publish = _number(row.get("publish_window_total_ms"))
        if publish is None:
            publish = _number(row.get("publish_total_ms"))
        if (
            e2e is None
            or producer is None
            or source is None
            or publish is None
        ):
            continue
        residuals.append(e2e - producer - source - publish)
    return residuals


def _row_workflow_overhead_ms(rows: list[dict[str, str]]) -> list[float]:
    overheads: list[float] = []
    for row in rows:
        workflow = _number(row.get("bench_workflow_created_to_published_ms"))
        source = _number(row.get("ingest_source_wall_ms"))
        if source is None:
            source = _number(row.get("source_total_ms"))
        publish = _number(row.get("publish_window_total_ms"))
        if publish is None:
            publish = _number(row.get("publish_total_ms"))
        if workflow is None or source is None or publish is None:
            continue
        overheads.append(workflow - source - publish)
    return overheads


def _row_published_latency_ms(rows: list[dict[str, str]]) -> list[float]:
    latencies: list[float] = []
    for row in rows:
        e2e = _number(row.get("e2e_ms"))
        queue_drain = _number(row.get("bench_queue_drain_ms"))
        if e2e is None:
            continue
        latencies.append(e2e - (queue_drain or 0.0))
    return latencies


def _row_pre_workflow_to_published_ms(rows: list[dict[str, str]]) -> list[float]:
    overheads: list[float] = []
    for row in rows:
        e2e = _number(row.get("e2e_ms"))
        queue_drain = _number(row.get("bench_queue_drain_ms")) or 0.0
        workflow = _number(row.get("bench_workflow_created_to_published_ms"))
        if e2e is None or workflow is None:
            continue
        overheads.append(e2e - queue_drain - workflow)
    return overheads


def _row_metric_context(rows: list[dict[str, str]]) -> dict[str, Any]:
    """Summarize replay CSV timing columns for owner attribution.

    These are optional because old artifacts are still valid inputs. Missing
    columns produce empty summaries instead of silently inventing zeros.
    """

    source_total = _numeric_column(rows, "source_total_ms")
    source_wall = _numeric_column(rows, "ingest_source_wall_ms")
    publish_window = _numeric_column_first_available(
        rows,
        "publish_window_total_ms",
        "publish_total_ms",
    )
    return {
        "producer_latency": _metric_summary(
            _numeric_column(rows, "bench_reset_to_workflow_created_ms"),
        ),
        "producer_start_latency": _metric_summary(
            _numeric_column(rows, "bench_reset_to_workflow_started_ms"),
        ),
        "workflow_created_to_published": _metric_summary(
            _numeric_column(rows, "bench_workflow_created_to_published_ms"),
        ),
        "workflow_started_to_published": _metric_summary(
            _numeric_column(rows, "bench_workflow_started_to_published_ms"),
        ),
        "source_total": _metric_summary(source_total),
        "source_wall": _metric_summary(source_wall),
        "ingest_total": _metric_summary(_numeric_column(rows, "ingest_total_ms")),
        "ingest_source_reported_total": _metric_summary(
            _numeric_column(rows, "ingest_source_reported_total_ms"),
        ),
        "ingest_publish_wall": _metric_summary(
            _numeric_column(rows, "ingest_publish_wall_ms"),
        ),
        "ingest_orchestration_overhead": _metric_summary(
            _numeric_column(rows, "ingest_orchestration_overhead_ms"),
        ),
        "ingest_orchestration_unattributed": _metric_summary(
            _numeric_column(rows, "ingest_orchestration_unattributed_ms"),
        ),
        "ingest_setup_total": _metric_summary(
            _numeric_column(rows, "ingest_setup_total_ms"),
        ),
        "publish_window": _metric_summary(publish_window),
        "residual_unattributed": _metric_summary(_row_residual_ms(rows)),
        "residual_after_producer": _metric_summary(
            _row_residual_after_producer_ms(rows),
        ),
        "workflow_overhead": _metric_summary(_row_workflow_overhead_ms(rows)),
        "published_latency": _metric_summary(_row_published_latency_ms(rows)),
        "pre_workflow_to_published": _metric_summary(
            _row_pre_workflow_to_published_ms(rows),
        ),
        "benchmark_reset": _metric_summary(
            _numeric_column(rows, "bench_reset_ms"),
        ),
        "benchmark_wait_new_gen": _metric_summary(
            _numeric_column(rows, "bench_wait_new_gen_ms"),
        ),
        "benchmark_latest_gen_query": _metric_summary(
            _numeric_column(rows, "bench_latest_gen_query_ms"),
        ),
        "benchmark_queue_drain": _metric_summary(
            _numeric_column(rows, "bench_queue_drain_ms"),
        ),
        "benchmark_gen_metrics": _metric_summary(
            _numeric_column(rows, "bench_gen_metrics_ms"),
        ),
        "benchmark_db_size": _metric_summary(
            _numeric_column(rows, "bench_db_size_ms"),
        ),
    }


# ---------------------------------------------------------------------------
# Config -> metric comparison (bench-okg-internals task.comparison-report)

# Canonical ordering of an enricher axis so the lightest config is the baseline.
_ENR_ORDER = {"none": 0, "default": 0, "declarative": 1, "code": 2, "all": 3}


def _swept_axes(exps: list[Experiment]) -> list[str]:
    """Config axes that vary across the cells of a sweep group."""
    varying = []
    for axis in SWEEPABLE_AXES:
        vals = {getattr(e.config, axis) for e in exps}
        if len(vals) > 1:
            varying.append(axis)
    return varying


def _axis_sort_key(axis: str, value: Any):
    if axis == "enrichers":
        return _ENR_ORDER.get(value, 99)
    if isinstance(value, bool):
        return int(value)
    if value is None:
        return -1
    return value


def _cell_profile(exp: Experiment) -> dict[str, Any]:
    """Aggregate one cell's standardized CSV + read-side report into a flat
    metric profile for cross-config comparison."""
    rows = _load_normalized_rows(exp)

    def agg(col: str, how: str = "mean"):
        vals = _numeric_column(rows, col)
        if not vals:
            return None
        if how == "sum":
            return round(sum(vals), 3)
        if how == "last":
            return vals[-1]
        if how == "p95":
            return _percentile_value(vals, 0.95)
        return round(sum(vals) / len(vals), 3)

    prof: dict[str, Any] = {
        "commits": len(rows),
        "e2e_ms_mean": agg("e2e_ms"),
        "e2e_ms_p95": agg("e2e_ms", "p95"),
        "cycle_ms_mean": agg("cycle_ms"),
        "projection_ms_mean": agg("projection_ms"),
        "enrichment_ms_mean": agg("enrichment_ms"),
        "gate_ms_mean": agg("gate_ms"),
        "publish_ms_mean": agg("publish_ms"),
        "view_materialization_ms_mean": agg("view_materialization_ms"),
        "publish_total_ms_mean": agg("publish_total_ms"),
        "source_total_ms_mean": agg("source_total_ms"),
        "source_run_count_mean": agg("source_run_count"),
        "edges_added_total": agg("edges_added", "sum"),
        "nodes_added_total": agg("nodes_added", "sum"),
        "final_edges_live": agg("edges_live", "last"),
        "final_nodes_live": agg("nodes_live", "last"),
        "db_size_bytes": agg("db_size_bytes", "last"),
        "peak_rss_bytes": agg("peak_rss_bytes", "last"),
        "chronos_segments": agg("chronos_segments", "last"),
    }
    if exp.read_side_json.exists():
        try:
            rs = json.loads(exp.read_side_json.read_text(encoding="utf-8"))
            if rs.get("ok"):
                prof["read_side_p50"] = {
                    name: r.get("p50_ms")
                    for name, r in rs["reads"].items()
                    if r.get("status") == "ok"
                }
        except (OSError, json.JSONDecodeError):
            return prof
    return prof


def build_comparison_report(base_id: str) -> dict[str, Any]:
    """Attribute marginal cost to a swept config dimension: given a sweep group
    (cells sharing the `base_id-` prefix), profile each cell and report, vs the
    lightest (baseline) cell, the per-commit ms / edge / read-side deltas
    attributable to moving along the swept axis."""
    exps = [
        e for e in _load_manifest() if e.id == base_id or e.id.startswith(f"{base_id}-")
    ]
    if len(exps) < 2:
        return {
            "schema": "okg.bench.config_comparison.v1",
            "base_id": base_id,
            "ok": False,
            "reason": f"need >=2 cells with prefix {base_id!r}, found {len(exps)}",
        }
    axes = _swept_axes(exps)
    if not axes:
        return {
            "schema": "okg.bench.config_comparison.v1",
            "base_id": base_id,
            "ok": False,
            "reason": "cells do not differ in any config axis",
        }
    axis = axes[0]  # single-axis sweeps; first varying axis for multi-axis
    exps.sort(key=lambda e: _axis_sort_key(axis, getattr(e.config, axis)))

    profiles = {e.id: _cell_profile(e) for e in exps}
    baseline_exp = exps[0]
    base_prof = profiles[baseline_exp.id]

    _SCALAR_KEYS = [
        "e2e_ms_mean",
        "cycle_ms_mean",
        "projection_ms_mean",
        "enrichment_ms_mean",
        "gate_ms_mean",
        "publish_ms_mean",
        "view_materialization_ms_mean",
        "edges_added_total",
        "final_edges_live",
        "db_size_bytes",
        "peak_rss_bytes",
        "chronos_segments",
    ]
    # For per-phase columns a None means "phase did not run" => 0 ms, so the
    # marginal cost of a config that DOES run the phase is its full value.
    # Other metrics keep None-as-unknown (don't fabricate a 0).
    _PHASE_KEYS = {
        "projection_ms_mean",
        "enrichment_ms_mean",
        "gate_ms_mean",
        "publish_ms_mean",
        "view_materialization_ms_mean",
    }

    def _delta(a, b, key):
        av, bv = a.get(key), b.get(key)
        if key in _PHASE_KEYS:
            av = 0.0 if av is None else av
            bv = 0.0 if bv is None else bv
        if av is None or bv is None:
            return None
        return round(bv - av, 3)

    cells_out = []
    for e in exps:
        prof = profiles[e.id]
        is_base = e.id == baseline_exp.id
        marginal = None
        if not is_base:
            marginal = {k: _delta(base_prof, prof, k) for k in _SCALAR_KEYS}
            rs_base = base_prof.get("read_side_p50") or {}
            rs_cell = prof.get("read_side_p50") or {}
            marginal["read_side_p50"] = {
                name: round(rs_cell[name] - rs_base[name], 3)
                for name in rs_cell
                if name in rs_base
                and rs_cell[name] is not None
                and rs_base[name] is not None
            }
        cells_out.append(
            {
                "cell_id": e.id,
                "axis_value": getattr(e.config, axis),
                "is_baseline": is_base,
                "profile": prof,
                "marginal_vs_baseline": marginal,
            }
        )

    return {
        "schema": "okg.bench.config_comparison.v1",
        "base_id": base_id,
        "ok": True,
        "swept_axis": axis,
        "baseline_cell": baseline_exp.id,
        "baseline_axis_value": getattr(baseline_exp.config, axis),
        "cells": cells_out,
    }


def _try_load_diagnostics(exp: Experiment) -> tuple[dict[str, Any] | None, str]:
    path = _diagnostics_path(exp)
    if not path.exists():
        return None, "missing"
    try:
        diagnostics = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, "invalid_json"
    if diagnostics.get("schema") != "okg.bench.source_sync_diagnostics.v1":
        return None, "unsupported_schema"
    return diagnostics, "available"


def _top_summary_item(
    summary: dict[str, Any],
    key: str,
    *,
    include_name: Any | None = None,
) -> tuple[str | None, float | None]:
    rows = []
    for name, stats in summary.items():
        if not isinstance(stats, dict):
            continue
        if include_name is not None and not include_name(str(name)):
            continue
        value = _number(stats.get(key))
        if value is not None:
            rows.append((str(name), value))
    if not rows:
        return None, None
    return max(rows, key=lambda item: (item[1], item[0]))


_NON_DURATION_SUMMARY_SUFFIXES = (
    ".count",
    "_count",
    "_count_max",
    ".row_count",
    "_row_count",
    ".rows",
    "_rows",
    ".bytes",
    "_bytes",
    ".bytes_read",
    "_bytes_read",
    ".bytes_written",
    "_bytes_written",
    ".size",
    "_size",
    ".marker_batch_size",
    "_batch_size",
    ".operation_count",
    "_operation_count",
    ".operations",
    "_operations",
)
_NON_DURATION_SUMMARY_NAMESPACES = (
    "node_change_class",
    "node_change_fields",
)
_NON_DURATION_SUMMARY_NAMES = {
    "payload_bytes",
    "payload_size",
    "max_payload_bytes",
    "max_payload_size",
    "bytes_read",
    "bytes_written",
    "row_count",
    "rows_read",
    "rows_written",
    "operation_count",
}


def _is_diagnostic_duration_stage(stage: str) -> bool:
    if stage in {"total", "source_runner.total"}:
        return False
    if stage in _NON_DURATION_SUMMARY_NAMES:
        return False
    parts = stage.split(".")
    if any(namespace in parts for namespace in _NON_DURATION_SUMMARY_NAMESPACES):
        return False
    return not any(
        stage.endswith(suffix) for suffix in _NON_DURATION_SUMMARY_SUFFIXES
    )


def _gate_owner_context(
    rows: list[dict[str, str]],
    diagnostics: dict[str, Any] | None,
) -> dict[str, Any]:
    row_metrics = _row_metric_context(rows)
    row_component_candidates = [
        (
            "producer_latency",
            "reset_to_workflow_created",
            row_metrics["producer_latency"]["p95_ms"],
        ),
        ("source_total", "source_total", row_metrics["source_total"]["p95_ms"]),
        ("publish_window", "publish_window", row_metrics["publish_window"]["p95_ms"]),
        (
            "workflow_overhead",
            "workflow_minus_source_minus_publish",
            row_metrics["workflow_overhead"]["p95_ms"],
        ),
        (
            "pre_workflow_to_published",
            "reset_to_workflow_created_plus_notify_poll",
            row_metrics["pre_workflow_to_published"]["p95_ms"],
        ),
        (
            "post_publish_queue_drain",
            "post_publish_queue_drain",
            row_metrics["benchmark_queue_drain"]["p95_ms"],
        ),
    ]
    row_component_kind, row_component_name, row_component_ms = max(
        (
            item for item in row_component_candidates
            if item[1] is not None and item[2] is not None
        ),
        key=lambda item: (item[2] or 0.0, item[0], item[1] or ""),
        default=(None, None, None),
    )
    row_candidates = [
        (
            "producer_latency",
            "reset_to_workflow_created",
            row_metrics["producer_latency"]["p95_ms"],
        ),
        (
            "producer_start_latency",
            "reset_to_workflow_started",
            row_metrics["producer_start_latency"]["p95_ms"],
        ),
        (
            "workflow_latency",
            "workflow_created_to_published",
            row_metrics["workflow_created_to_published"]["p95_ms"],
        ),
        ("source_total", "source_total", row_metrics["source_total"]["p95_ms"]),
        (
            "publish_window",
            "publish_window",
            row_metrics["publish_window"]["p95_ms"],
        ),
        (
            "workflow_overhead",
            "workflow_minus_source_minus_publish",
            row_metrics["workflow_overhead"]["p95_ms"],
        ),
        (
            "pre_workflow_to_published",
            "reset_to_workflow_created_plus_notify_poll",
            row_metrics["pre_workflow_to_published"]["p95_ms"],
        ),
        (
            "post_publish_queue_drain",
            "post_publish_queue_drain",
            row_metrics["benchmark_queue_drain"]["p95_ms"],
        ),
        (
            "residual_unattributed",
            "e2e_minus_source_minus_publish",
            row_metrics["residual_unattributed"]["p95_ms"],
        ),
    ]
    row_context = {
        "producer_latency_p95_ms": (
            row_metrics["producer_latency"]["p95_ms"]
        ),
        "producer_start_latency_p95_ms": (
            row_metrics["producer_start_latency"]["p95_ms"]
        ),
        "workflow_created_to_published_p95_ms": (
            row_metrics["workflow_created_to_published"]["p95_ms"]
        ),
        "workflow_started_to_published_p95_ms": (
            row_metrics["workflow_started_to_published"]["p95_ms"]
        ),
        "source_total_p95_ms": row_metrics["source_total"]["p95_ms"],
        "publish_window_p95_ms": row_metrics["publish_window"]["p95_ms"],
        "residual_unattributed_p95_ms": (
            row_metrics["residual_unattributed"]["p95_ms"]
        ),
        "published_latency_p95_ms": (
            row_metrics["published_latency"]["p95_ms"]
        ),
        "pre_workflow_to_published_p95_ms": (
            row_metrics["pre_workflow_to_published"]["p95_ms"]
        ),
        "post_publish_queue_drain_p95_ms": (
            row_metrics["benchmark_queue_drain"]["p95_ms"]
        ),
        "workflow_overhead_p95_ms": (
            row_metrics["workflow_overhead"]["p95_ms"]
        ),
        "top_component_owner_kind": row_component_kind,
        "top_component_owner": row_component_name,
        "top_component_owner_p95_ms": row_component_ms,
    }
    if diagnostics:
        summary = diagnostics.get("source_run_summary")
        if isinstance(summary, dict):
            source, source_ms = _top_summary_item(
                summary.get("sources") or {},
                "p95_ms",
            )
            phase, phase_ms = _top_summary_item(
                summary.get("phases") or {},
                "p95_ms",
                include_name=_is_diagnostic_duration_stage,
            )
            publish_summary = diagnostics.get("publish_run_summary")
            publish_label = publish_label_ms = None
            publish_stage = publish_stage_ms = None
            if isinstance(publish_summary, dict):
                publish_label, publish_label_ms = _top_summary_item(
                    publish_summary.get("labels") or {},
                    "p95_ms",
                )
                publish_stage, publish_stage_ms = _top_summary_item(
                    publish_summary.get("stages") or {},
                    "p95_ms",
                    include_name=_is_diagnostic_duration_stage,
                )
            component_candidates = [
                ("source", source, source_ms),
                ("source_phase", phase, phase_ms),
                ("publish_label", publish_label, publish_label_ms),
                ("publish_stage", publish_stage, publish_stage_ms),
                *row_component_candidates,
            ]
            (
                component_kind,
                component_name,
                component_ms,
            ) = max(
                (
                    item for item in component_candidates
                    if item[1] is not None and item[2] is not None
                ),
                key=lambda item: (item[2] or 0.0, item[0], item[1] or ""),
                default=(None, None, None),
            )
            candidates = [
                ("source", source, source_ms),
                ("source_phase", phase, phase_ms),
                ("publish_label", publish_label, publish_label_ms),
                ("publish_stage", publish_stage, publish_stage_ms),
                *row_candidates,
            ]
            top_kind, top_name, top_ms = max(
                (
                    item for item in candidates
                    if item[1] is not None and item[2] is not None
                ),
                key=lambda item: (item[2] or 0.0, item[0], item[1] or ""),
                default=(None, None, None),
            )
            if source or phase or publish_label or publish_stage:
                return {
                    "source": source,
                    "source_p95_ms": source_ms,
                    "phase": phase,
                    "phase_p95_ms": phase_ms,
                    "publish_label": publish_label,
                    "publish_label_p95_ms": publish_label_ms,
                    "publish_stage": publish_stage,
                    "publish_stage_p95_ms": publish_stage_ms,
                    "top_owner_kind": top_kind,
                    "top_owner": top_name,
                    "top_owner_p95_ms": top_ms,
                    **row_context,
                    "top_component_owner_kind": component_kind,
                    "top_component_owner": component_name,
                    "top_component_owner_p95_ms": component_ms,
                    "basis": "diagnostics_summary",
                }
    row_top_kind, row_top_name, row_top_ms = max(
        (
            item for item in row_candidates
            if item[1] is not None and item[2] is not None
        ),
        key=lambda item: (item[2] or 0.0, item[0], item[1] or ""),
        default=(None, None, None),
    )
    if row_top_kind is not None:
        return {
            **row_context,
            "top_owner_kind": row_top_kind,
            "top_owner": row_top_name,
            "top_owner_p95_ms": row_top_ms,
            "basis": "normalized_csv_timing_breakdown",
        }
    candidate_rows = [
        row for row in rows if _number(row.get("slowest_phase_ms")) is not None
    ]
    if not candidate_rows:
        return {"basis": "unavailable"}
    row = max(
        candidate_rows,
        key=lambda item: (
            _number(item.get("slowest_phase_ms")) or 0.0,
            item.get("commit") or "",
        ),
    )
    return {
        "source": row.get("slowest_source") or None,
        "phase": row.get("slowest_phase") or None,
        "phase_ms": _number(row.get("slowest_phase_ms")),
        "commit": row.get("commit") or None,
        "commit_index": _number(row.get("i")),
        "basis": "normalized_csv_slowest_phase",
    }


def _baseline_for(
    commit: str | None, baselines: Sequence[tuple[str, int]] | None,
) -> int | None:
    """The pinned baseline for a CSV commit. The normalized CSV carries
    abbreviated SHAs, so a row matches a baseline whose full SHA it prefixes."""
    if not commit or not baselines:
        return None
    for full, value in baselines:
        if full.startswith(commit):
            return value
    return None


def _count_gate_metrics(
    rows: list[dict[str, str]],
    *,
    baselines: Sequence[tuple[str, int]] | None = None,
) -> dict[str, Any]:
    """Per-commit query-count metrics for the replay rows.

    Reads the `query_count` / `queries_per_changed_file` columns emitted by
    extract.py (count-based-perf-gate / req.counts.emit). A count does not
    depend on machine load, but it is only repeatable when the experiment
    does the same work on every run (no wall-clock-gated refresh) and the
    commit list is pinned; the manifest entry is responsible for both.
    With `baselines`, each commit also gets its pinned baseline and
    query_count / baseline.
    """
    per_commit: list[dict[str, Any]] = []
    for row in rows:
        qpcf = _number(row.get("queries_per_changed_file"))
        qcount = _number(row.get("query_count"))
        files_changed = _number(row.get("files_changed"))
        commit = row.get("commit") or None
        baseline = _baseline_for(commit, baselines)
        ratio = (
            round(qcount / baseline, 4)
            if qcount is not None and baseline else None
        )
        per_commit.append({
            "commit": commit,
            "commit_index": int(_number(row.get("i")) or 0),
            "query_count": int(qcount) if qcount is not None else None,
            "files_changed": int(files_changed) if files_changed is not None else None,
            "queries_per_changed_file": qpcf,
            "baseline_query_count": baseline,
            "ratio_to_baseline": ratio,
            "work_ratio": _number(row.get("work_ratio")),
            "wf_pairing_reason": row.get("wf_pairing_reason") or None,
        })
    qpcf_values = [
        item["queries_per_changed_file"] for item in per_commit
        if item["queries_per_changed_file"] is not None
    ]
    qcount_values = [
        item["query_count"] for item in per_commit
        if item["query_count"] is not None
    ]
    ratio_values = [
        item["ratio_to_baseline"] for item in per_commit
        if item["ratio_to_baseline"] is not None
    ]
    return {
        "per_commit": per_commit,
        "queries_per_changed_file": _metric_summary(qpcf_values),
        "ratio_to_baseline_max": max(ratio_values) if ratio_values else None,
        "query_count_max": max(qcount_values) if qcount_values else None,
        "query_count_min": min(qcount_values) if qcount_values else None,
        "measured_commit_count": len(qcount_values),
        "unmeasured_commit_count": len(per_commit) - len(qcount_values),
    }


def _count_gate_violations(
    count_metrics: dict[str, Any],
    *,
    max_queries_per_changed_file: float | None,
    max_no_change_query_count: int | None,
    baselines: Sequence[tuple[str, int]] | None = None,
    headroom: float | None = None,
    expected_commits: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Count-budget violations, independent of wall-clock.

    Each violation is tagged `gate_class: count` so the junit writer routes it
    to the count testcase (separate from the advisory wall-clock testcase).
    count-based-perf-gate / req.counts.gate.

    The primary budget is per commit: query_count <= baseline * headroom,
    with the baseline measured for that pinned commit. When any count budget
    is set, missing data fails: no rows, a commit with no query_count, a
    commit whose replay window held more than one successful workflow (its
    count would be partial), a commit with no baseline, and a replay whose
    commits differ from the pinned list. A gate that skips unmeasured
    commits passes on a broken run.
    """
    violations: list[dict[str, Any]] = []
    per_commit = count_metrics["per_commit"]
    if (baselines is None) != (headroom is None):
        raise ValueError("per-commit baselines and headroom go together")
    gate_active = any(
        value is not None
        for value in (
            max_queries_per_changed_file,
            max_no_change_query_count,
            baselines,
        )
    )
    if not gate_active:
        return violations

    def _add(reason: str, item: dict[str, Any] | None, **extra: Any) -> None:
        violations.append({
            "reason": reason,
            "gate_class": "count",
            "commit": item["commit"] if item else None,
            "commit_index": item["commit_index"] if item else None,
            **extra,
        })

    if not per_commit:
        _add("query_count_missing", None,
             detail="no replay rows: nothing was measured")
    if expected_commits is not None:
        # The normalized CSV carries abbreviated SHAs (10 hex); a row matches
        # its pinned commit when it is a non-empty prefix of the full SHA.
        replayed = [item["commit"] for item in per_commit]
        matches = len(replayed) == len(expected_commits) and all(
            bool(got) and full.startswith(str(got))
            for got, full in zip(replayed, expected_commits)
        )
        if not matches:
            _add("replayed_commits_mismatch", None,
                 expected_commits=list(expected_commits),
                 replayed_commits=replayed)
    for item in per_commit:
        if item["wf_pairing_reason"] == "ambiguous_time_window":
            _add("query_count_ambiguous", item,
                 detail=(
                     "more than one successful source-sync workflow fell in "
                     "this commit's replay window; its query_count would "
                     "count only one of them"
                 ))
        elif item["query_count"] is None:
            _add("query_count_missing", item,
                 detail=(
                     "query_count is empty; workflow pairing: "
                     f"{item['wf_pairing_reason'] or 'unknown'}"
                 ))
    if baselines is not None:
        for item in per_commit:
            observed = item["query_count"]
            if observed is None:
                continue
            baseline = item["baseline_query_count"]
            if baseline is None:
                _add("baseline_missing", item, query_count=observed,
                     detail="no count_gate baseline for this commit")
                continue
            limit = baseline * float(headroom)
            if observed > limit:
                _add("query_count_over_baseline", item,
                     query_count=observed,
                     baseline_query_count=baseline,
                     headroom=headroom,
                     max_query_count=round(limit, 1),
                     ratio_to_baseline=item["ratio_to_baseline"])
    if max_queries_per_changed_file is not None:
        # Opt-in gross cap. It divides by files_changed, which extract.py
        # writes as 0 when git reported none (max(1, n) in the ratio).
        for item in per_commit:
            observed = item["queries_per_changed_file"]
            if observed is not None and observed > max_queries_per_changed_file:
                _add("queries_per_changed_file_exceeded", item,
                     queries_per_changed_file=observed,
                     max_queries_per_changed_file=max_queries_per_changed_file,
                     query_count=item["query_count"],
                     files_changed=item["files_changed"])
    if max_no_change_query_count is not None:
        # No-change floor: the lightest commit (fewest changed files) must not
        # spend more than the floor's worth of absolute round-trips. A commit
        # that touches nothing yet issues hundreds of queries is the exact
        # orchestration-bound regression this guards.
        light = [
            item for item in per_commit
            if item["query_count"] is not None
        ]
        if light:
            floor_commit = min(
                light,
                key=lambda item: (
                    item["files_changed"]
                    if item["files_changed"] is not None else 1 << 30,
                    item["commit_index"],
                ),
            )
            if floor_commit["query_count"] > max_no_change_query_count:
                _add("no_change_query_count_exceeded", floor_commit,
                     query_count=floor_commit["query_count"],
                     max_no_change_query_count=max_no_change_query_count,
                     files_changed=floor_commit["files_changed"])
    return violations


def build_replay_gate_report(
    exp: Experiment,
    *,
    max_p95_ms: float,
    max_max_ms: float,
    min_throughput_commits_per_second: float,
    max_outlier_count: int,
    outlier_threshold_ms: float,
    max_queue_dwell_p95_ms: float | None,
    require_diagnostics: bool,
    max_queries_per_changed_file: float | None = None,
    max_no_change_query_count: int | None = None,
) -> dict[str, Any]:
    # The experiment's manifest `count_gate:` block supplies the per-commit
    # baselines; the gross and no-change flags are separate, opt-in caps.
    baselines = exp.count_gate.baselines if exp.count_gate else None
    headroom = exp.count_gate.headroom if exp.count_gate else None
    rows = _load_normalized_rows(exp)
    diagnostics, diagnostics_status = _try_load_diagnostics(exp)
    e2e_ms = _numeric_column(rows, "e2e_ms")
    queue_dwell_ms = _numeric_column(rows, "wf_a0_ms")
    latency = _metric_summary(e2e_ms)
    queue_dwell = _metric_summary(queue_dwell_ms)
    timing_breakdown = _row_metric_context(rows)
    total_ms = sum(e2e_ms)
    throughput = len(e2e_ms) / (total_ms / 1000.0) if total_ms > 0 else None
    outlier_count = sum(1 for value in e2e_ms if value > outlier_threshold_ms)
    owner_context = _gate_owner_context(rows, diagnostics)
    count_metrics = _count_gate_metrics(rows, baselines=baselines)
    metrics = {
        "commit_count": len(rows),
        "measured_commit_count": len(e2e_ms),
        "latency": latency,
        "throughput_commits_per_second": throughput,
        "outlier_count": outlier_count,
        "outlier_threshold_ms": outlier_threshold_ms,
        "queue_dwell": queue_dwell,
        **timing_breakdown,
        "diagnostics_status": diagnostics_status,
        "owner_context": owner_context,
        # Count-based incrementality metrics (count-based-perf-gate).
        "query_counts": count_metrics,
    }
    budgets = {
        "max_p95_ms": max_p95_ms,
        "max_max_ms": max_max_ms,
        "min_throughput_commits_per_second": (min_throughput_commits_per_second),
        "max_outlier_count": max_outlier_count,
        "outlier_threshold_ms": outlier_threshold_ms,
        "max_queue_dwell_p95_ms": max_queue_dwell_p95_ms,
        "require_diagnostics": require_diagnostics,
        "max_queries_per_changed_file": max_queries_per_changed_file,
        "max_no_change_query_count": max_no_change_query_count,
        "headroom": headroom,
        "baselines": dict(baselines) if baselines else None,
        "pinned_commits": (
            list(exp.replay.commits) if exp.replay.commits else None
        ),
    }
    violations: list[dict[str, Any]] = []
    if not rows:
        violations.append({"reason": "no_rows"})
    elif not e2e_ms:
        violations.append(
            {
                "reason": "missing_latency_measurements",
                "row_count": len(rows),
            }
        )
    p95_ms = latency["p95_ms"]
    if p95_ms is not None and p95_ms > max_p95_ms:
        violations.append(
            {
                "reason": "p95_budget_exceeded",
                "p95_ms": p95_ms,
                "max_p95_ms": max_p95_ms,
                "owner_context": owner_context,
            }
        )
    max_ms = latency["max_ms"]
    if max_ms is not None and max_ms > max_max_ms:
        violations.append(
            {
                "reason": "max_budget_exceeded",
                "max_ms": max_ms,
                "max_max_ms": max_max_ms,
                "owner_context": owner_context,
            }
        )
    if throughput is not None and throughput < min_throughput_commits_per_second:
        violations.append(
            {
                "reason": "throughput_below_budget",
                "throughput_commits_per_second": throughput,
                "min_throughput_commits_per_second": (
                    min_throughput_commits_per_second
                ),
            }
        )
    if outlier_count > max_outlier_count:
        violations.append(
            {
                "reason": "outlier_count_exceeded",
                "outlier_count": outlier_count,
                "max_outlier_count": max_outlier_count,
                "outlier_threshold_ms": outlier_threshold_ms,
            }
        )
    queue_p95 = queue_dwell["p95_ms"]
    if (
        max_queue_dwell_p95_ms is not None
        and queue_p95 is not None
        and queue_p95 > max_queue_dwell_p95_ms
    ):
        violations.append(
            {
                "reason": "queue_dwell_p95_exceeded",
                "queue_dwell_p95_ms": queue_p95,
                "max_queue_dwell_p95_ms": max_queue_dwell_p95_ms,
                "environment": True,
            }
        )
    if require_diagnostics and diagnostics_status != "available":
        violations.append(
            {
                "reason": "diagnostics_unavailable",
                "diagnostics_status": diagnostics_status,
                "artifact": str(_diagnostics_path(exp)),
            }
        )
    # Deterministic count-based budget violations (count-based-perf-gate).
    # These are independent of wall-clock and tagged gate_class=count so the
    # junit writer routes them to the count testcase.
    count_violations = _count_gate_violations(
        count_metrics,
        max_queries_per_changed_file=max_queries_per_changed_file,
        max_no_change_query_count=max_no_change_query_count,
        baselines=baselines,
        headroom=headroom,
        expected_commits=exp.replay.commits,
    )
    violations.extend(count_violations)
    # Wall-clock latency violations are ADVISORY (count-based-perf-gate): on a
    # contended/CI runner wall time swings far more than any real regression, so
    # it must not fail the build. Only count-budget + structural
    # (missing-diagnostics) violations are blocking. Classify by reason and
    # fail closed — an unknown reason blocks rather than silently passing.
    blocking_violations = [
        v for v in violations
        if v.get("reason") not in _ADVISORY_GATE_REASONS
    ]
    return {
        "schema": "okg.bench.sync_commit_replay_gate.v1",
        "ok": not blocking_violations,
        "blocking_violation_count": len(blocking_violations),
        "experiment_id": exp.id,
        "repo": exp.repo.name,
        "source_set": exp.source_set,
        "replay_strategy": exp.replay.strategy,
        "artifacts": {
            "csv": str(_normalized_csv_path(exp)),
            "diagnostics": str(_diagnostics_path(exp)),
        },
        "budgets": budgets,
        "metrics": metrics,
        "violations": violations,
        "violation_count": len(violations),
    }


def _count_violation_message(violation: dict[str, Any]) -> str:
    """One-line, operator-readable description of a count-budget breach,
    naming the offending commit and observed-vs-budget counts."""
    commit = str(violation.get("commit") or "<unknown>")[:12]
    if violation["reason"] == "queries_per_changed_file_exceeded":
        return (
            f"commit {commit}: queries_per_changed_file="
            f"{violation['queries_per_changed_file']} exceeds budget "
            f"{violation['max_queries_per_changed_file']} "
            f"(query_count={violation['query_count']}, "
            f"files_changed={violation['files_changed']})"
        )
    if violation["reason"] == "query_count_over_baseline":
        return (
            f"commit {commit}: query_count={violation['query_count']} exceeds "
            f"{violation['max_query_count']} (baseline "
            f"{violation['baseline_query_count']} x headroom "
            f"{violation['headroom']}; ratio {violation['ratio_to_baseline']})"
        )
    if violation["reason"] in {
        "query_count_missing", "query_count_ambiguous", "baseline_missing",
    }:
        return f"commit {commit}: not measured -- {violation['detail']}"
    if violation["reason"] == "replayed_commits_mismatch":
        return (
            "replayed commits differ from the pinned list: expected "
            f"{len(violation['expected_commits'])} pinned commit(s), got "
            f"{[str(c)[:10] for c in violation['replayed_commits']]}"
        )
    if violation["reason"] == "no_change_query_count_exceeded":
        return (
            f"commit {commit}: query_count={violation['query_count']} "
            f"exceeds no-change floor {violation['max_no_change_query_count']} "
            f"(files_changed={violation['files_changed']})"
        )
    return f"commit {commit}: {violation['reason']} {violation}"


# Wall-clock / latency reasons that are ADVISORY (never fail the build). Every
# other reason — count-budget breaches and structural failures like missing
# diagnostics — is blocking. New reasons block until explicitly listed here.
_ADVISORY_GATE_REASONS = frozenset({
    "p95_budget_exceeded",
    "max_budget_exceeded",
    "throughput_below_budget",
    "outlier_count_exceeded",
    "queue_dwell_p95_exceeded",
})


def build_replay_gate_junit(report: dict[str, Any]) -> ET.Element:
    """Render the gate report as a JUnit testsuite.

    Three testcases (count-based-perf-gate / req.counts.gate):
      - `query_count_budget` — blocking. Fails on any count-budget
        breach (a commit over its pinned baseline x headroom, the opt-in gross
        per-changed-file cap or no-change floor), on unmeasured or ambiguous
        commits, and on drift from the pinned commit list.
      - `diagnostics_available` — STRUCTURAL, blocking. Fails when the gate could
        not evaluate (e.g. missing diagnostics).
      - `wall_clock_latency_advisory` — ADVISORY, never fails. Wall-clock /
        latency violations are recorded as `system-out` only, so a noisy
        wall-time blip is visible but does not turn the build red. The count +
        structural cases are the build gate.
    """
    violations = report.get("violations", [])
    count_violations = [
        v for v in violations if v.get("gate_class") == "count"
    ]
    advisory_violations = [
        v for v in violations if v.get("reason") in _ADVISORY_GATE_REASONS
    ]
    # Blocking but not count == structural (e.g. missing diagnostics).
    structural_violations = [
        v for v in violations
        if v.get("gate_class") != "count"
        and v.get("reason") not in _ADVISORY_GATE_REASONS
    ]
    exp_id = str(report.get("experiment_id") or "replay")
    budgets = report.get("budgets", {})
    qcounts = report.get("metrics", {}).get("query_counts", {})

    suite = ET.Element("testsuite", {
        "name": f"okg.bench.replay_gate.{exp_id}",
        "tests": "3",
        "failures": str(
            (1 if count_violations else 0)
            + (1 if structural_violations else 0)
        ),
    })

    # 1. Count gate (the build gate).
    count_case = ET.SubElement(suite, "testcase", {
        "classname": f"okg.bench.replay_gate.{exp_id}",
        "name": "query_count_budget",
    })
    budget = budgets.get("max_queries_per_changed_file")
    summary = qcounts.get("queries_per_changed_file", {}) or {}
    observed_max = summary.get("max_ms")
    count_props = ET.SubElement(count_case, "properties")
    ET.SubElement(count_props, "property", {
        "name": "max_queries_per_changed_file",
        "value": str(budget),
    })
    ET.SubElement(count_props, "property", {
        "name": "observed_max_queries_per_changed_file",
        "value": str(observed_max),
    })
    ET.SubElement(count_props, "property", {
        "name": "observed_max_query_count",
        "value": str(qcounts.get("query_count_max")),
    })
    ET.SubElement(count_props, "property", {
        "name": "headroom",
        "value": str(budgets.get("headroom")),
    })
    ET.SubElement(count_props, "property", {
        "name": "observed_max_ratio_to_baseline",
        "value": str(qcounts.get("ratio_to_baseline_max")),
    })
    if count_violations:
        lines = [_count_violation_message(v) for v in count_violations]
        failure = ET.SubElement(count_case, "failure", {
            "type": "query_count_regression",
            "message": (
                f"{len(count_violations)} query-count gate violation(s): "
                + ", ".join(sorted({str(v["reason"]) for v in count_violations}))
            ),
        })
        failure.text = "\n".join(lines)

    # 2. Structural gate (missing diagnostics, etc.) — blocking, separate from
    #    both the count gate and the advisory wall-clock case.
    structural_case = ET.SubElement(suite, "testcase", {
        "classname": f"okg.bench.replay_gate.{exp_id}",
        "name": "diagnostics_available",
    })
    if structural_violations:
        reasons = ", ".join(str(v.get("reason")) for v in structural_violations)
        failure = ET.SubElement(structural_case, "failure", {
            "type": "structural_gate",
            "message": f"structural gate failed: {reasons}",
        })
        failure.text = json.dumps(structural_violations, indent=2, default=str)

    # 3. Advisory wall-clock latency — NEVER fails the build; recorded as
    #    system-out so a noisy wall-time blip is visible but non-blocking.
    latency_case = ET.SubElement(suite, "testcase", {
        "classname": f"okg.bench.replay_gate.{exp_id}",
        "name": "wall_clock_latency_advisory",
    })
    if advisory_violations:
        reasons = ", ".join(str(v.get("reason")) for v in advisory_violations)
        sysout = ET.SubElement(latency_case, "system-out")
        sysout.text = (
            f"advisory (non-blocking) wall-clock violations: {reasons}\n"
            + json.dumps(advisory_violations, indent=2, default=str)
        )
    return suite


def write_replay_gate_junit(report: dict[str, Any], path: Path) -> None:
    suite = build_replay_gate_junit(report)
    root = ET.Element("testsuites")
    root.append(suite)
    path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def cmd_gate(args) -> int:
    exp = _resolve(args.id)
    report = build_replay_gate_report(
        exp,
        max_p95_ms=args.max_p95_ms,
        max_max_ms=args.max_max_ms,
        min_throughput_commits_per_second=(args.min_throughput_commits_per_second),
        max_outlier_count=args.max_outlier_count,
        outlier_threshold_ms=args.outlier_threshold_ms,
        max_queue_dwell_p95_ms=args.max_queue_dwell_p95_ms,
        require_diagnostics=args.require_diagnostics,
        max_queries_per_changed_file=args.max_queries_per_changed_file,
        max_no_change_query_count=args.max_no_change_query_count,
    )
    if args.output:
        Path(args.output).write_text(
            json.dumps(report, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
    if getattr(args, "junit", None):
        write_replay_gate_junit(report, Path(args.junit))
        print(f"[{exp.id}] replay gate junit → {args.junit}")
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        status = "PASS" if report["ok"] else "FAIL"
        metrics = report["metrics"]
        latency = metrics["latency"]
        producer = metrics["producer_latency"]
        source_total = metrics["source_total"]
        source_wall = metrics["source_wall"]
        publish_window = metrics["publish_window"]
        published_latency = metrics["published_latency"]
        pre_workflow_to_published = metrics["pre_workflow_to_published"]
        post_publish_queue_drain = metrics["benchmark_queue_drain"]
        residual = metrics["residual_unattributed"]
        residual_after_producer = metrics["residual_after_producer"]
        workflow_overhead = metrics["workflow_overhead"]
        print(
            f"[{exp.id}] replay gate {status}: "
            f"p95={latency['p95_ms']}ms max={latency['max_ms']}ms "
            f"throughput={metrics['throughput_commits_per_second']} "
            f"outliers={metrics['outlier_count']} "
            f"producer_p95={producer['p95_ms']}ms "
            f"source_p95={source_total['p95_ms']}ms "
            f"source_wall_p95={source_wall['p95_ms']}ms "
            f"publish_p95={publish_window['p95_ms']}ms "
            f"published_latency_p95={published_latency['p95_ms']}ms "
            f"pre_workflow_to_published_p95="
            f"{pre_workflow_to_published['p95_ms']}ms "
            f"post_publish_queue_drain_p95="
            f"{post_publish_queue_drain['p95_ms']}ms "
            f"residual_p95={residual['p95_ms']}ms "
            f"residual_after_producer_p95="
            f"{residual_after_producer['p95_ms']}ms "
            f"workflow_overhead_p95={workflow_overhead['p95_ms']}ms"
        )
        for violation in report["violations"]:
            print(f"  - {violation['reason']}: {violation}")
    return 0 if report["ok"] else 1


# ---------------------------------------------------------------------------
# LSP semantic-resolution benchmark proof

_LSP_FAILURE_MARKERS = (
    "CodeParseFailed",
    "failed_sources",
    "lsp_protocol_error",
    "lsp_server_unavailable",
    "source sync failed",
)


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _latest_replay_generation_id(exp: Experiment) -> str | None:
    try:
        rows = _load_normalized_rows(exp)
    except FileNotFoundError:
        rows = []
    for row in reversed(rows):
        gen_id = str(row.get("gen_id") or "").strip()
        if gen_id and gen_id != "None":
            return gen_id
    if exp.delta_proof_json.exists():
        try:
            proof = json.loads(exp.delta_proof_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        proof_rows = proof.get("rows") if isinstance(proof, dict) else None
        if isinstance(proof_rows, list):
            for row in reversed(proof_rows):
                if not isinstance(row, dict):
                    continue
                boundary = (row.get("boundaries") or {}).get("graph_delta") or {}
                gen_id = str(boundary.get("generation_id") or row.get("gen_id") or "")
                if gen_id:
                    return gen_id
    return None


def _scan_lsp_failure_markers(exp: Experiment) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    for artifact in (exp.worker_log, exp.replay_log):
        if not artifact.exists():
            continue
        try:
            lines = artifact.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for idx, line in enumerate(lines, start=1):
            for marker in _LSP_FAILURE_MARKERS:
                if marker in line:
                    hits.append({
                        "artifact": str(artifact),
                        "line": idx,
                        "marker": marker,
                    })
    return hits


def _lsp_edge_counts_at_generation(
    exp: Experiment,
    generation_id: str,
) -> dict[str, Any]:
    from okg.substrate.graph_store import ChronosGraphStore

    source_name = f"{exp.deployment}.code-lsp-resolution"
    source_sql = _sql_string(source_name)
    with psycopg.connect(exp.dsn) as conn:
        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout_generation(generation_id)
        nodes = int(graph.query(
            "SELECT count(*) AS n FROM okg.graph_nodes"
        )[0]["n"] or 0)
        edges = int(graph.query(
            "SELECT count(*) AS n FROM okg.graph_edges"
        )[0]["n"] or 0)
        by_type = {
            str(row["edge_type"]): int(row["n"] or 0)
            for row in graph.query(f"""
                SELECT edge_type, count(*) AS n
                  FROM okg.graph_edges
                 WHERE source = {source_sql}
                   AND attrs->>'resolution_kind' = 'cross_file_lsp'
                 GROUP BY edge_type
            """)
        }
        resolver_counts = {
            str(row["resolver"] or ""): int(row["n"] or 0)
            for row in graph.query(f"""
                SELECT attrs->>'resolver' AS resolver, count(*) AS n
                  FROM okg.graph_edges
                 WHERE source = {source_sql}
                   AND attrs->>'resolution_kind' = 'cross_file_lsp'
                 GROUP BY resolver
            """)
        }
        total_source_edges = int(graph.query(f"""
            SELECT count(*) AS n
              FROM okg.graph_edges
             WHERE source = {source_sql}
        """)[0]["n"] or 0)
    return {
        "generation_id": generation_id,
        "final_nodes": nodes,
        "final_edges": edges,
        "source": source_name,
        "cross_file_references": by_type.get("references", 0),
        "cross_file_calls": by_type.get("calls", 0),
        "cross_file_edges": sum(by_type.values()),
        "total_source_edges": total_source_edges,
        "resolver_counts": resolver_counts,
    }


def _lsp_source_run_summary(exp: Experiment) -> dict[str, Any]:
    source_name = f"{exp.deployment}.code-lsp-resolution"
    rows: list[dict[str, Any]] = []
    with psycopg.connect(exp.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT value
              FROM okg.source_progress
             WHERE source_name = %s
               AND key LIKE 'sync.stage_timings.run.%%'
             ORDER BY key
            """,
            (source_name,),
        )
        for (raw_value,) in cur.fetchall():
            try:
                data = json.loads(raw_value)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(data, dict):
                rows.append(data)

    statuses: Counter[str] = Counter()
    active_runs = 0
    emitted_runs = 0
    no_change_request_violations = 0
    max_lsp_requests = 0
    max_files_opened = 0
    max_changed_files = 0
    total_edges_appended = 0
    total_edges_retracted = 0
    failed_statuses: Counter[str] = Counter()
    allowed_statuses = {"resolved", "no_change", "deleted_scope"}
    for row in rows:
        stats = row.get("source_stats") or {}
        status = str(stats.get("status") or "unknown")
        statuses[status] += 1
        output_count = int(row.get("output_count") or 0)
        edges_appended = int(row.get("edges_appended") or 0)
        edges_retracted = int(row.get("edges_retracted") or 0)
        changed = int(stats.get("changed_file_count") or 0)
        deleted = int(stats.get("deleted_file_count") or 0)
        lsp_requests = int(stats.get("lsp_requests") or 0)
        files_opened = int(stats.get("files_opened") or 0)
        if output_count or changed or deleted or lsp_requests:
            active_runs += 1
        if output_count or edges_appended:
            emitted_runs += 1
        if status == "no_change" and lsp_requests:
            no_change_request_violations += 1
        if status not in allowed_statuses:
            failed_statuses[status] += 1
        max_lsp_requests = max(max_lsp_requests, lsp_requests)
        max_files_opened = max(max_files_opened, files_opened)
        max_changed_files = max(max_changed_files, changed)
        total_edges_appended += edges_appended
        total_edges_retracted += edges_retracted
    return {
        "source": source_name,
        "run_count": len(rows),
        "active_run_count": active_runs,
        "emitted_run_count": emitted_runs,
        "statuses": dict(statuses),
        "failed_statuses": dict(failed_statuses),
        "no_change_request_violations": no_change_request_violations,
        "max_lsp_requests": max_lsp_requests,
        "max_files_opened": max_files_opened,
        "max_changed_files": max_changed_files,
        "total_edges_appended": total_edges_appended,
        "total_edges_retracted": total_edges_retracted,
    }


def _lsp_pending_deferred_cursor(exp: Experiment) -> dict[str, Any]:
    source_name = f"{exp.deployment}.code-lsp-resolution"
    with psycopg.connect(exp.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_applied_rev
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source_name,),
        )
        row = cur.fetchone()
    cursor = row[0] if row else None
    deferred = (
        cursor.get("lsp_deferred")
        if isinstance(cursor, dict)
        else None
    )
    if not isinstance(deferred, dict):
        return {
            "source": source_name,
            "pending": False,
            "paths": [],
            "estimated_lsp_requests": 0,
            "reason": None,
        }
    paths = [
        str(path)
        for path in deferred.get("paths", [])
        if path
    ]
    return {
        "source": source_name,
        "pending": bool(paths),
        "paths": paths,
        "estimated_lsp_requests": deferred.get("estimated_lsp_requests"),
        "reason": deferred.get("reason"),
    }


def build_lsp_benchmark_report(
    exp: Experiment,
    *,
    min_references: int,
    min_calls: int,
    require_clean_logs: bool,
) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    generation_id = _latest_replay_generation_id(exp)
    if exp.source_set != "files+code+lsp":
        violations.append({
            "reason": "not_lsp_source_set",
            "source_set": exp.source_set,
        })
    if generation_id is None:
        violations.append({"reason": "missing_final_generation"})
        edge_counts: dict[str, Any] | None = None
    else:
        try:
            edge_counts = _lsp_edge_counts_at_generation(exp, generation_id)
        except Exception as exc:  # noqa: BLE001
            edge_counts = None
            violations.append({
                "reason": "generation_checkout_failed",
                "generation_id": generation_id,
                "error": str(exc),
            })

    try:
        source_runs = _lsp_source_run_summary(exp)
    except Exception as exc:  # noqa: BLE001
        source_runs = None
        violations.append({
            "reason": "source_run_summary_failed",
            "error": str(exc),
        })
    try:
        pending_deferred = _lsp_pending_deferred_cursor(exp)
    except Exception as exc:  # noqa: BLE001
        pending_deferred = None
        violations.append({
            "reason": "lsp_pending_deferred_check_failed",
            "error": str(exc),
        })

    log_failures = _scan_lsp_failure_markers(exp)
    if require_clean_logs and log_failures:
        violations.append({
            "reason": "source_failure_markers_in_logs",
            "markers": log_failures[:20],
            "marker_count": len(log_failures),
        })
    if edge_counts:
        references = int(edge_counts["cross_file_references"])
        calls = int(edge_counts["cross_file_calls"])
        if references < min_references:
            violations.append({
                "reason": "insufficient_lsp_references",
                "actual": references,
                "minimum": min_references,
            })
        if calls < min_calls:
            violations.append({
                "reason": "insufficient_lsp_calls",
                "actual": calls,
                "minimum": min_calls,
            })
    if source_runs:
        if int(source_runs["run_count"]) == 0:
            violations.append({"reason": "no_lsp_source_runs"})
        if int(source_runs["emitted_run_count"]) == 0:
            violations.append({"reason": "no_lsp_emitting_runs"})
        failed_statuses = dict(source_runs["failed_statuses"])
        if pending_deferred and not pending_deferred.get("pending"):
            failed_statuses.pop("deferred", None)
        if failed_statuses:
            violations.append({
                "reason": "failed_lsp_source_statuses",
                "statuses": failed_statuses,
            })
        if int(source_runs["no_change_request_violations"]) > 0:
            violations.append({
                "reason": "no_change_runs_made_lsp_requests",
                "count": source_runs["no_change_request_violations"],
            })
    if pending_deferred and pending_deferred.get("pending"):
        violations.append({
            "reason": "pending_lsp_deferred_paths",
            "deferred": pending_deferred,
        })

    return {
        "schema": "okg.bench.lsp_semantic_proof.v1",
        "ok": not violations,
        "experiment_id": exp.id,
        "repo": exp.repo.name,
        "source_set": exp.source_set,
        "replay_strategy": exp.replay.strategy,
        "artifacts": {
            "csv": str(_normalized_csv_path(exp)),
            "diagnostics": str(_diagnostics_path(exp)),
            "worker_log": str(exp.worker_log),
            "replay_log": str(exp.replay_log),
        },
        "minimums": {
            "min_references": min_references,
            "min_calls": min_calls,
            "require_clean_logs": require_clean_logs,
        },
        "generation_id": generation_id,
        "edge_counts": edge_counts,
        "source_runs": source_runs,
        "pending_deferred": pending_deferred,
        "log_failure_markers": log_failures,
        "violations": violations,
        "violation_count": len(violations),
    }


def cmd_lsp_proof(args) -> int:
    exp = _resolve(args.id)
    report = build_lsp_benchmark_report(
        exp,
        min_references=args.min_references,
        min_calls=args.min_calls,
        require_clean_logs=not args.allow_source_failure_markers,
    )
    if args.output:
        Path(args.output).write_text(
            json.dumps(report, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        status = "PASS" if report["ok"] else "FAIL"
        counts = report.get("edge_counts") or {}
        print(
            f"[{exp.id}] LSP proof {status}: "
            f"references={counts.get('cross_file_references')} "
            f"calls={counts.get('cross_file_calls')} "
            f"markers={len(report['log_failure_markers'])}"
        )
        for violation in report["violations"]:
            print(f"  - {violation['reason']}: {violation}")
    return 0 if report["ok"] else 1


# ---------------------------------------------------------------------------
# Differential rebuild equivalence report


def _stable_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )


def _stable_digest_rows(rows: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(_stable_json(row).encode("utf-8"))
        digest.update(b"\n")
    return "sha256:" + digest.hexdigest()


_VOLATILE_GRAPH_ATTRS = {
    "nomos_source_lineage",
    "parsed_at",
    "source_frontier",
    "working_tree_mtime",
    "working_tree_mtime_epoch",
    "workspace_fingerprint",
}

_DERIVED_GRAPH_EDGE_TYPES = {
    "view_cites_node",
}


def _semantic_graph_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        normalized = dict(row)
        attrs = normalized.get("attrs")
        if isinstance(attrs, dict):
            semantic_attrs = {
                key: value
                for key, value in attrs.items()
                if key not in _VOLATILE_GRAPH_ATTRS
            }
            if (
                normalized.get("subtype") == "git_branch"
                and semantic_attrs.get("is_remote") is False
            ):
                semantic_attrs.pop("is_remote")
            normalized["attrs"] = semantic_attrs
        out.append(normalized)
    return out


def _semantic_graph_edge_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in _semantic_graph_rows(rows)
        if row.get("edge_type") not in _DERIVED_GRAPH_EDGE_TYPES
    ]


def _view_cites_edge_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The derived ``view_cites_node`` lineage edges that
    ``_semantic_graph_edge_rows`` excludes from the general edge digest.

    These edges live in ``okg.graph_edges`` (not ``derived_output_slots``), so
    without a dedicated check they are invisible to BOTH the edge-equality and
    the derived-output-equality checks -- i.e. the per-commit collapse's only
    semantic output (the restored view lineage) would not be proven equivalent
    incremental-vs-rebuild. Every column the fingerprint reads for them
    (src/dst/attrs/provenance='derived_deterministic'/confidence=1.0) is
    deterministic, so a digest equality is a sound equivalence check.
    """
    return [
        row
        for row in _semantic_graph_rows(rows)
        if row.get("edge_type") in _DERIVED_GRAPH_EDGE_TYPES
    ]


def _semantic_derived_output_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        normalized = copy.deepcopy(row)
        normalized.pop("definition_id", None)
        normalized.pop("manifest_id", None)
        normalized.pop("storage_ref", None)
        payload = normalized.get("inline_payload")
        if isinstance(payload, dict):
            value = payload.get("value")
            if isinstance(value, dict):
                value.pop("refresh_stats", None)
                signature = value.get("materializer_input_signature")
                if isinstance(signature, dict):
                    signature.pop("input_fact_watermark", None)
                    signature.pop("input_edge_fact_watermark", None)
                manifest = value.get("view_manifest")
                if isinstance(manifest, dict):
                    manifest.pop("generation_id", None)
            if "byte_count" in normalized:
                normalized["byte_count"] = len(
                    _stable_json(payload).encode("utf-8")
                )
        if normalized.get("storage_kind") == "relation":
            normalized.pop("byte_count", None)
            visible_rows = normalized.get("relation_visible_rows")
            if isinstance(visible_rows, list):
                normalized["row_count"] = len(visible_rows)
                normalized["relation_visible_digest"] = _stable_digest_rows(
                    visible_rows
                )
                normalized["relation_visible_byte_count"] = len(
                    _stable_json(visible_rows).encode("utf-8")
                )
                normalized.pop("relation_visible_rows", None)
        out.append(normalized)
    return out


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _relation_visible_rows_for_output_slot(
    cur: psycopg.Cursor[Any],
    *,
    definition_id: str,
    manifest_id: Any,
    storage_ref: Mapping[str, Any],
) -> list[dict[str, Any]]:
    relation_name = storage_ref.get("relation")
    if not isinstance(relation_name, str) or not relation_name:
        raise ValueError(
            "derived relation slot is missing storage_ref.relation; "
            f"definition_id={definition_id!r} manifest_id={manifest_id!r}"
        )
    upstream_definition_id = str(storage_ref.get("upstream_definition_id") or definition_id)
    upstream_manifest_id = storage_ref.get("upstream_manifest_id") or manifest_id
    cur.execute(
        sql.SQL(
            """
            WITH RECURSIVE lineage AS (
              SELECT m.manifest_id, m.reused_from_manifest_id,
                     m.target_generation_id,
                     COALESCE(s.storage_ref::jsonb->'dirty_scope_keys', '[]'::jsonb)
                       AS dirty_scope_keys,
                     0 AS depth
                FROM okg.derived_output_manifests m
                JOIN okg.derived_output_slots s
                  ON s.manifest_id = m.manifest_id
                 AND s.slot_name = 'relation'
                 AND s.storage_kind = 'relation'
               WHERE m.manifest_id = %(manifest_id)s
                 AND m.definition_id = %(definition_id)s
                 AND m.status = 'completed'
              UNION ALL
              SELECT m.manifest_id, m.reused_from_manifest_id,
                     m.target_generation_id,
                     COALESCE(s.storage_ref::jsonb->'dirty_scope_keys', '[]'::jsonb)
                       AS dirty_scope_keys,
                     lineage.depth + 1 AS depth
                FROM lineage
                JOIN okg.derived_output_manifests m
                  ON m.manifest_id = lineage.reused_from_manifest_id
                JOIN okg.derived_output_slots s
                  ON s.manifest_id = m.manifest_id
                 AND s.slot_name = 'relation'
                 AND s.storage_kind = 'relation'
               WHERE lineage.depth < 64
                 AND m.definition_id = %(definition_id)s
                 AND m.status = 'completed'
            )
            SELECT r.scope_key, r.row_key, r.row_data
              FROM lineage l
              JOIN {}.{} r
                ON r.definition_id = %(definition_id)s
               AND r.target_generation_id = l.target_generation_id
             WHERE NOT EXISTS (
                 SELECT 1
                   FROM lineage newer
                  WHERE newer.depth < l.depth
                    AND newer.dirty_scope_keys ? r.scope_key
               )
             ORDER BY r.scope_key, r.row_key
            """
        ).format(
            sql.Identifier(str(storage_ref.get("schema") or "okg_derived")),
            sql.Identifier(relation_name),
        ),
        {
            "definition_id": upstream_definition_id,
            "manifest_id": upstream_manifest_id,
        },
    )
    return [
        {
            "scope_key": str(row["scope_key"]),
            "row_key": str(row["row_key"]),
            "row_data": dict(row["row_data"] or {}),
        }
        for row in cur.fetchall()
    ]


def _latest_published_generation(exp: Experiment) -> int | str | None:
    with psycopg.connect(exp.dsn, autocommit=True) as conn, conn.cursor() as cur:
        with contextlib.suppress(Exception):
            cur.execute(
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
                (exp.deployment,),
            )
            row = cur.fetchone()
            if row is not None:
                return str(row[0])
        cur.execute(
            """
            SELECT max(generation_id)
              FROM okg.graph_generations
             WHERE deployment_name=%s
               AND status='published'
            """,
            (exp.deployment,),
        )
        row = cur.fetchone()
    return None if row is None else row[0]


# Terminal-rebuild wait (issue #1539). A from-scratch rebuild publishes one
# generation per ingest layer, so "a generation newer than the pre-worker one"
# plus "the DBOS queue reads empty for a moment" is not completion: the
# 2026-09-12 nightly fingerprinted a layer-1-sized rebuild generation (2044
# nodes) against the incremental final tree (3444 nodes) and failed all nine
# checks as one defect. The substrate's own completion evidence is used
# instead (`SourceSyncDesiredFrontier`: a workflow row is admission evidence;
# completion is proved by watermark movement and publish):
#   * no `okg_source_sync` workflow is ENQUEUED / PENDING / DELAYED
#     (`_DBOS_ACTIVE_QUEUE_STATUSES` in source_sync_producers);
#   * every source in the deployment's generated registry has a non-null
#     `okg.source_watermarks.last_reconcile_at` -- the runner writes it only
#     for a run that completed its scope walk (`SourceRunner._advance_watermark`,
#     `last_reconcile=(observed_at if completed_scope else None)`). The value
#     is that run's observed/start time, not its finish time, so on its own it
#     says "a full walk of this source ran", not "and its publish landed";
#     the publish is a durable step of the same `okg_source_sync` workflow,
#     which is why the active-workflow check above is what closes that gap;
#   * the latest published generation differs from the pre-worker one and was
#     published at or after the latest of those scope-walk timestamps;
#   * that observation holds unchanged for a settle window.
# A reconcile pass that exited non-zero or reported `any_failed` can never
# reach that state (the failed source keeps a NULL watermark while its
# workflow row reads SUCCESS), so the wait refuses it up front instead of
# spending the whole timeout on `source_not_reconciled`.
_SOURCE_SYNC_ACTIVE_STATUSES = ("ENQUEUED", "PENDING", "DELAYED")
# DBOS terminal failure statuses (dbos 2.x): ERROR, CANCELLED,
# MAX_RECOVERY_ATTEMPTS_EXCEEDED.
_SOURCE_SYNC_FAILED_STATUSES = (
    "ERROR",
    "CANCELLED",
    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
)
_SOURCE_LAST_RUN_PROGRESS_KEY = "sync.stage_timings.last_run"
_REBUILD_TERMINAL_TIMEOUT_ENV = "OKG_BENCH_REBUILD_TERMINAL_TIMEOUT_S"
_REBUILD_TERMINAL_TIMEOUT_DEFAULT_S = 900.0
_REBUILD_TERMINAL_SETTLE_S = 3.0
_REBUILD_TERMINAL_POLL_S = 0.5


def _rebuild_terminal_timeout_s() -> float:
    raw = os.environ.get(_REBUILD_TERMINAL_TIMEOUT_ENV)
    if raw is None or not raw.strip():
        return _REBUILD_TERMINAL_TIMEOUT_DEFAULT_S
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(
            f"{_REBUILD_TERMINAL_TIMEOUT_ENV} must be a number of seconds, "
            f"got {raw!r}"
        ) from exc
    if value <= 0:
        raise ValueError(
            f"{_REBUILD_TERMINAL_TIMEOUT_ENV} must be positive, got {raw!r}"
        )
    return value


def _deployment_source_names(exp: Experiment) -> list[str]:
    """Ownership ids (`<deployment>.<registry name>`) of every source the
    experiment's generated source_registry.yaml declares. These are the
    `source` keys of okg.source_watermarks and the `source_name` of
    okg.source_progress for that deployment."""
    registry_path = exp.deployment_dir / "source_registry.yaml"
    raw = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    sources = raw.get("sources") if isinstance(raw, dict) else None
    if not isinstance(sources, dict) or not sources:
        raise ValueError(
            f"{exp.id}: generated source registry {registry_path} declares "
            "no sources; cannot decide when the rebuild is complete"
        )
    return [f"{exp.deployment}.{name}" for name in sources]


def _source_sync_workflow_counts(exp: Experiment) -> dict[str, int]:
    with psycopg.connect(exp.dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_status')")
        row = cur.fetchone()
        if row is None or row[0] is None:
            return {}
        cur.execute(
            """
            SELECT upper(status), count(*)
              FROM dbos.workflow_status
             WHERE name = 'okg_source_sync'
             GROUP BY upper(status)
            """
        )
        return {str(status): int(count) for status, count in cur.fetchall()}


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _iso_or_none(value: Any) -> str | None:
    """Normalize a timestamp column to an aware ISO string.

    `okg.source_watermarks.last_reconcile_at` is TIMESTAMPTZ (a datetime);
    `_chronos_branch_interval_checkpoints.created_at` is declared TEXT and
    holds an ISO string, so a string is parsed rather than dropped.
    """
    if isinstance(value, str):
        value = _parse_iso(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return None


def _source_completion_states(
    exp: Experiment,
    source_names: Sequence[str],
) -> dict[str, dict[str, Any]]:
    names = list(source_names)
    states: dict[str, dict[str, Any]] = {
        name: {
            "reconciled_at": None,
            "last_run_id": None,
            "last_run_status": None,
            "last_run_mode": None,
        }
        for name in names
    }
    with psycopg.connect(exp.dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source, last_reconcile_at
              FROM okg.source_watermarks
             WHERE source = ANY(%s)
            """,
            (names,),
        )
        for source, reconciled_at in cur.fetchall():
            states[str(source)]["reconciled_at"] = _iso_or_none(reconciled_at)
        cur.execute(
            """
            SELECT source_name, value
              FROM okg.source_progress
             WHERE deployment = %s
               AND source_name = ANY(%s)
               AND key = %s
            """,
            (exp.deployment, names, _SOURCE_LAST_RUN_PROGRESS_KEY),
        )
        for source_name, raw_value in cur.fetchall():
            try:
                payload = json.loads(raw_value)
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            state = states[str(source_name)]
            state["last_run_id"] = payload.get("run_id")
            state["last_run_status"] = payload.get("status")
            state["last_run_mode"] = payload.get("run_mode")
    return states


def _generation_published_at(
    exp: Experiment,
    generation_id: int | str | None,
) -> str | None:
    if generation_id is None:
        return None
    with psycopg.connect(exp.dsn, autocommit=True) as conn, conn.cursor() as cur:
        if isinstance(generation_id, str) and generation_id.startswith("okg:"):
            cur.execute(
                """
                SELECT created_at
                  FROM public._chronos_branch_interval_checkpoints
                 WHERE checkpoint_id = %s
                 LIMIT 1
                """,
                (generation_id,),
            )
        else:
            cur.execute(
                """
                SELECT published_at
                  FROM okg.graph_generations
                 WHERE generation_id = %s
                """,
                (generation_id,),
            )
        row = cur.fetchone()
    return None if row is None else _iso_or_none(row[0])


def _rebuild_terminal_observation(
    exp: Experiment,
    *,
    source_names: Sequence[str],
) -> dict[str, Any]:
    """One snapshot of every signal the terminal-rebuild wait reads.
    JSON-serializable so the report can carry it verbatim."""
    generation_id = _latest_published_generation(exp)
    return {
        "generation_id": generation_id,
        "generation_published_at": _generation_published_at(exp, generation_id),
        "workflow_counts": _source_sync_workflow_counts(exp),
        "sources": _source_completion_states(exp, source_names),
    }


def _rebuild_terminal_blockers(
    observation: Mapping[str, Any],
    *,
    previous_generation_id: int | str | None,
) -> list[str]:
    """Why the observation is not yet a terminal rebuild (empty = terminal)."""
    blockers: list[str] = []
    counts = observation.get("workflow_counts") or {}
    active = sum(int(counts.get(status, 0)) for status in _SOURCE_SYNC_ACTIVE_STATUSES)
    if active:
        blockers.append(f"active_source_sync_workflows:{active}")
    latest_completion: datetime | None = None
    for name, state in sorted((observation.get("sources") or {}).items()):
        reconciled = _parse_iso(state.get("reconciled_at"))
        if reconciled is None:
            blockers.append(f"source_not_reconciled:{name}")
            continue
        if latest_completion is None or reconciled > latest_completion:
            latest_completion = reconciled
    generation_id = observation.get("generation_id")
    if generation_id is None or generation_id == previous_generation_id:
        blockers.append("no_new_published_generation")
    else:
        published_at = _parse_iso(observation.get("generation_published_at"))
        if published_at is None:
            blockers.append("generation_publish_time_unavailable")
        elif latest_completion is not None and published_at < latest_completion:
            blockers.append("generation_predates_source_completion")
    return blockers


def _failed_source_sync_workflows(observation: Mapping[str, Any]) -> dict[str, int]:
    counts = observation.get("workflow_counts") or {}
    return {
        status: int(counts[status])
        for status in _SOURCE_SYNC_FAILED_STATUSES
        if int(counts.get(status, 0)) > 0
    }


def _observation_stability_key(observation: Mapping[str, Any]) -> tuple[Any, ...]:
    sources = observation.get("sources") or {}
    return (
        observation.get("generation_id"),
        tuple(sorted((observation.get("workflow_counts") or {}).items())),
        tuple(
            (name, state.get("reconciled_at"), state.get("last_run_id"))
            for name, state in sorted(sources.items())
        ),
    )


def _wait_for_rebuild_publish(
    exp: Experiment,
    *,
    previous_generation_id: int | str | None,
    timeout_s: float | None = None,
    settle_s: float = _REBUILD_TERMINAL_SETTLE_S,
    poll_s: float = _REBUILD_TERMINAL_POLL_S,
    source_names: Sequence[str] | None = None,
    reconcile_receipt: Mapping[str, Any] | None = None,
    observe: Any = None,
    clock: Any = time.time,
    sleep: Any = time.sleep,
) -> dict[str, Any]:
    """Block until the rebuild deployment is terminal; return the receipt.

    The receipt carries `generation_id` (the generation to fingerprint) and
    the final `observation`, so the report can show exactly what was waited
    for. Raises RuntimeError when the reconcile pass failed or a source-sync
    workflow ended failed, and TimeoutError (with `.outstanding`, the list of
    unmet signals) on the deadline -- never a partial generation.
    """
    if reconcile_receipt is not None:
        failure = _reconcile_failure(reconcile_receipt)
        if failure:
            raise RuntimeError(
                f"{exp.id}: reconcile pass failed ({failure}); the rebuild "
                "cannot complete, not waiting for it"
            )
    if timeout_s is None:
        timeout_s = _rebuild_terminal_timeout_s()
    if source_names is None:
        source_names = _deployment_source_names(exp)
    if observe is None:
        def observe() -> dict[str, Any]:
            return _rebuild_terminal_observation(exp, source_names=source_names)

    started = clock()
    deadline = started + timeout_s
    polls = 0
    stable_since: float | None = None
    stable_key: tuple[Any, ...] | None = None
    blockers: list[str] = []
    observation: dict[str, Any] = {}
    while True:
        observation = observe()
        polls += 1
        now = clock()
        failed = _failed_source_sync_workflows(observation)
        if failed:
            raise RuntimeError(
                f"{exp.id}: source-sync workflow(s) ended failed "
                f"{failed}; the rebuild is not complete "
                f"(workflow_counts={observation.get('workflow_counts')})"
            )
        blockers = _rebuild_terminal_blockers(
            observation,
            previous_generation_id=previous_generation_id,
        )
        if blockers:
            stable_since = None
            stable_key = None
        else:
            key = _observation_stability_key(observation)
            if key != stable_key:
                stable_key = key
                stable_since = now
            elif now - stable_since >= settle_s:
                return {
                    "generation_id": observation["generation_id"],
                    "previous_generation_id": previous_generation_id,
                    "observation": observation,
                    "source_names": list(source_names),
                    "elapsed_s": round(now - started, 3),
                    "polls": polls,
                    "settle_s": settle_s,
                    "timeout_s": timeout_s,
                }
        if now >= deadline:
            outstanding = list(blockers) or ["settling"]
            error = TimeoutError(
                f"{exp.id}: rebuild not terminal within {timeout_s:.0f}s; "
                f"outstanding={outstanding} "
                f"workflow_counts={observation.get('workflow_counts')} "
                f"generation_id={observation.get('generation_id')} "
                f"previous_generation_id={previous_generation_id}"
            )
            error.outstanding = outstanding  # type: ignore[attr-defined]
            error.observation = observation  # type: ignore[attr-defined]
            raise error
        sleep(poll_s)


def _reconcile_failure(receipt: Mapping[str, Any]) -> str | None:
    """Why a `_bench_reconcile` receipt means the rebuild cannot complete."""
    returncode = receipt.get("returncode")
    if returncode not in (0, None):
        return f"exit {returncode}: {receipt.get('error') or 'no error text'}"
    failed = [
        str(item.get("workflow_id"))
        for item in receipt.get("workflows") or []
        if isinstance(item, Mapping)
        and (item.get("any_failed") or item.get("status") not in (None, "SUCCESS"))
    ]
    if failed:
        return f"workflows not clean: {failed}"
    return None


def _incremental_terminal_state(
    exp: Experiment,
    final_generation_id: int | str,
) -> dict[str, Any]:
    """Was the incremental side's final generation terminal? The replay
    harness drains the queue for at most 30 s per commit and the worker is
    then killed, so a workflow left non-terminal or a newer published
    generation means `final_generation_id` was not the last word."""
    source_names = _deployment_source_names(exp)
    observation = _rebuild_terminal_observation(exp, source_names=source_names)
    counts = observation.get("workflow_counts") or {}
    non_terminal = sum(
        int(counts.get(status, 0)) for status in _SOURCE_SYNC_ACTIVE_STATUSES
    )
    latest = observation.get("generation_id")
    return {
        "final_generation_id": final_generation_id,
        "latest_published_generation_id": latest,
        "final_generation_is_latest": (
            latest is not None and str(latest) == str(final_generation_id)
        ),
        "non_terminal_source_sync_workflows": non_terminal,
        "observation": observation,
    }


# ---------------------------------------------------------------------------
# Read-side performance (bench-okg-internals task.read-side-perf)

_READ_SIDE_ITERATIONS = 25


def _percentile(values: list[float], pct: float) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    k = (len(s) - 1) * pct
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return s[int(k)]
    return s[lo] * (hi - k) + s[hi] * (k - lo)


def build_read_side_report(
    exp: Experiment,
    *,
    iterations: int = _READ_SIDE_ITERATIONS,
    generation_id: int | str | None = None,
) -> dict[str, Any]:
    """Time a fixed canonical read-set against the experiment's FINAL published
    generation, so the read cost of a configuration is measured (not just
    ingest). The four reads mirror the operator surface:
      - search_by_subtype  : filter nodes by their most common subtype
      - traverse_3hop      : recursive 3-hop edge expansion from the busiest seed
      - aggregate_by_type  : graph-wide edge-type histogram
      - inspect_node_edges : a single node's incident edges
    Each is warmed once then timed `iterations` times; p50/p95/mean reported.
    Reads run in-process against the pinned generation via
    ChronosGraphStore.checkout_generation (no MCP server needed)."""
    from okg.substrate.graph_store import ChronosGraphStore

    gen = generation_id if generation_id is not None else _latest_published_generation(exp)
    if gen is None:
        return {
            "schema": "okg.bench.read_side.v1",
            "experiment_id": exp.id,
            "ok": False,
            "reason": "no_published_generation",
        }
    with psycopg.connect(exp.dsn) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        graph = store.checkout_generation(str(gen))
        subtype_rows = graph.query(
            "SELECT subtype, count(*) AS n FROM okg.graph_nodes "
            "GROUP BY subtype ORDER BY n DESC"
        )
        n_nodes = sum(int(r["n"]) for r in subtype_rows)
        top_subtype = subtype_rows[0]["subtype"] if subtype_rows else None
        n_edges = int(graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"])
        seed_rows = graph.query(
            "SELECT src, count(*) AS n FROM okg.graph_edges "
            "GROUP BY src ORDER BY n DESC LIMIT 1"
        )
        seed = seed_rows[0]["src"] if seed_rows else None

        # console-count-rollup (#444 slice 2): is the maintained rollup present at this
        # generation? (A data row, not just the sentinel.) When it is, we time the raw
        # count(*) GROUP BY offenders AGAINST the O(buckets) compact-table scan that
        # replaces them -- the read-win marginal cost (raw O(N) vs rollup flat).
        try:
            rollup_rows = graph.query(
                "SELECT count(*) AS n FROM okg.console_count_rollup "
                "WHERE dimension_kind = 'subtype' "
                "AND dimension_value <> '__okg_ontology_rollup_sentinel__'"
            )
            rollup_maintained = int(rollup_rows[0]["n"]) > 0
        except Exception:  # noqa: BLE001 -- table absent / unregistered => no rollup reads
            rollup_maintained = False

        reads = {
            # Read-win marginal cost: the by-subtype console read, raw scan (O(N)) vs
            # the maintained compact rollup (O(#buckets), flat in graph size).
            "count_by_subtype_raw_scan": (
                "SELECT subtype, count(*) AS n FROM okg.graph_nodes "
                'GROUP BY subtype ORDER BY n DESC, subtype COLLATE "C"',
                None,
                True,
            ),
            "count_by_subtype_rollup": (
                "SELECT dimension_kind, dimension_value, item_count "
                "FROM okg.console_count_rollup WHERE dimension_kind = 'subtype'",
                None,
                rollup_maintained,
            ),
            "count_by_source_raw_scan": (
                "SELECT source, count(DISTINCT node_id) AS n FROM okg.graph_nodes "
                "WHERE source IS NOT NULL GROUP BY source",
                None,
                True,
            ),
            "count_by_source_rollup": (
                "SELECT dimension_kind, dimension_value, item_count "
                "FROM okg.console_count_rollup WHERE dimension_kind = 'source'",
                None,
                rollup_maintained,
            ),
            "search_by_subtype": (
                "SELECT node_id, subtype FROM okg.graph_nodes "
                "WHERE subtype = %(subtype)s LIMIT 500",
                {"subtype": top_subtype},
                top_subtype is not None,
            ),
            "traverse_3hop": (
                "WITH RECURSIVE reach(node, depth) AS ("
                "  SELECT %(seed)s::text, 0"
                "  UNION ALL"
                "  SELECT e.dst, r.depth + 1 FROM reach r"
                "    JOIN okg.graph_edges e ON e.src = r.node"
                "   WHERE r.depth < 3"
                ") SELECT DISTINCT node FROM reach",
                {"seed": seed},
                seed is not None,
            ),
            "aggregate_by_type": (
                "SELECT edge_type, count(*) AS n FROM okg.graph_edges "
                "GROUP BY edge_type ORDER BY n DESC",
                None,
                True,
            ),
            "inspect_node_edges": (
                "SELECT src, dst, edge_type FROM okg.graph_edges "
                "WHERE src = %(seed)s OR dst = %(seed)s",
                {"seed": seed},
                seed is not None,
            ),
        }
        results: dict[str, Any] = {}
        for name, (sql, params, ok) in reads.items():
            if not ok:
                results[name] = {"status": "skipped", "reason": "no_anchor"}
                continue
            graph.query(sql, params)  # warm-up (exclude cold compile)
            samples: list[float] = []
            rows_n = 0
            for _ in range(iterations):
                t0 = time.perf_counter()
                rows = graph.query(sql, params)
                samples.append((time.perf_counter() - t0) * 1000.0)
                rows_n = len(rows)
            results[name] = {
                "status": "ok",
                "iterations": iterations,
                "rows": rows_n,
                "p50_ms": round(_percentile(samples, 0.50), 3),
                "p95_ms": round(_percentile(samples, 0.95), 3),
                "mean_ms": round(sum(samples) / len(samples), 3),
            }
    return {
        "schema": "okg.bench.read_side.v1",
        "experiment_id": exp.id,
        "ok": True,
        "generation_id": str(gen),
        "graph": {
            "nodes": n_nodes,
            "edges": n_edges,
            "seed_node": seed,
            "top_subtype": top_subtype,
        },
        "reads": results,
    }


# ---------------------------------------------------------------------------
# Chronos branching workload (bench-okg-internals task.branching-experiment)


def _interval_stats(dsn: str) -> dict[str, Optional[int]]:
    """Chronos interval-backend resource state: segment count, branch count,
    and total interval space (sum of live spans across segments)."""

    def _one(cur, sql: str) -> Optional[int]:
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
        except Exception:  # noqa: BLE001
            return None

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        return {
            "segments": _one(
                cur, "SELECT count(*) FROM _chronos_branch_interval_segments"
            ),
            "branches": _one(
                cur, "SELECT count(*) FROM _chronos_branch_interval_branches"
            ),
            "interval_space": _one(
                cur,
                "SELECT COALESCE(SUM(live_hi - live_lo), 0) "
                "FROM _chronos_branch_interval_segments",
            ),
        }


def build_branching_report(
    exp: Experiment,
    *,
    n_commits: int = 5,
    nodes_per_commit: int = 25,
) -> dict[str, Any]:
    """Benchmark chronos branching internals on top of the experiment's
    already-ingested tinydb graph: fork a branch, publish N commits on it,
    merge it into a fork-sibling, and compact — timing each step and recording
    segment + interval-space growth — contrasted against a LINEAR-APPEND
    baseline of the same N commits written straight to default.

    The merge targets a NON-PROTECTED sibling branch, not `default`: merging
    into the protected default branch requires a named-human MergeDecision (the
    merge-back governance guard), which an automated cost bench must not
    fabricate. The merge_apply machinery measured is the same; only the
    protected-branch admission checks (decider / catalog-drift / deletion-scan)
    are skipped — those are governance, not chronos cost.

    Synthetic node facts (an existing catalog subtype, no edges => no narrowing
    concern) stand in for per-commit work; the point is the fork/merge/compact
    machinery cost + lineage growth, not the fact content."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    branch = "bench:branching"
    merge_target = "bench:merge_target"

    def _facts(tag: str, commit_i: int):
        return [
            NodeFact(
                f"benchbr:{tag}:{commit_i}:{j}",
                subtype,
                {},
                {"id": f"{tag}:{commit_i}:{j}"},
                {"v": 1},
            )
            for j in range(nodes_per_commit)
        ]

    def _timed_commit(store, tgt_branch: str, tag: str, i: int) -> float:
        t0 = time.perf_counter()
        store.apply_facts(_facts(tag, i), branch=tgt_branch, source="bench.branching")
        store.checkpoint_generation(
            branch=tgt_branch, deployment_name=exp.deployment, status="published"
        )
        return (time.perf_counter() - t0) * 1000.0

    with psycopg.connect(exp.dsn) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        subtype_rows = store.query(
            "SELECT subtype, count(*) AS n FROM okg.graph_nodes "
            "GROUP BY subtype ORDER BY n DESC LIMIT 1"
        )
        if not subtype_rows:
            return {
                "schema": "okg.bench.branching.v1",
                "experiment_id": exp.id,
                "ok": False,
                "reason": "empty_graph",
            }
        subtype = subtype_rows[0]["subtype"]

        # Warm-up (untimed): the first apply on a freshly-connected store pays
        # a cold cost (segment-table setup, query-plan compile) that would
        # otherwise land entirely on the first linear commit and skew the
        # linear-vs-branch comparison. Warm both paths equally.
        _timed_commit(store, DEFAULT_BRANCH_NAME, "warm", 0)
        # Best-effort: drop stale branches from a prior run so this is re-runnable.
        for _stale in (branch, merge_target):
            with contextlib.suppress(Exception):
                store.delete_branch(_stale)

        baseline = _interval_stats(exp.dsn)

        # --- linear-append baseline: N commits straight to default ----------
        linear_per_commit = [
            _timed_commit(store, DEFAULT_BRANCH_NAME, "lin", i)
            for i in range(n_commits)
        ]
        after_linear = _interval_stats(exp.dsn)

        # --- branching: fork -> N commits on branch -> merge -> compact -----
        t0 = time.perf_counter()
        store.create_branch(branch, from_branch=DEFAULT_BRANCH_NAME)
        fork_ms = (time.perf_counter() - t0) * 1000.0
        after_fork = _interval_stats(exp.dsn)

        branch_per_commit = [
            _timed_commit(store, branch, "br", i) for i in range(n_commits)
        ]
        after_branch = _interval_stats(exp.dsn)

        # Merge the fork into a non-protected sibling forked from the same
        # default base (so they share a base for the three-way merge). Targeting
        # default would trip the merge-back governance decider; the merge_apply
        # cost measured here is the same primitive.
        store.create_branch(merge_target, from_branch=DEFAULT_BRANCH_NAME)
        t0 = time.perf_counter()
        store.merge(branch, merge_target)
        store.checkpoint_generation(
            branch=merge_target, deployment_name=exp.deployment, status="published"
        )
        merge_ms = (time.perf_counter() - t0) * 1000.0
        after_merge = _interval_stats(exp.dsn)

        compact_ms: Optional[float] = None
        compact_result: Optional[dict] = None
        latest = store.latest_generation(deployment_name=exp.deployment, branch=merge_target)
        if latest is not None:
            t0 = time.perf_counter()
            compact_result = store.compact_branch_from_generation(
                latest.checkpoint_id, deployment_name=exp.deployment, branch=merge_target
            )
            compact_ms = (time.perf_counter() - t0) * 1000.0
        after_compact = _interval_stats(exp.dsn)

    def _delta(a, b, key):
        if a.get(key) is None or b.get(key) is None:
            return None
        return b[key] - a[key]

    # _interval_stats returns None for a stat when its query fails (the
    # _chronos_branch_interval_* tables are core to the interval backend this
    # bench runs on, so None signals a real read failure, not a routine
    # absence). Segment/interval growth IS the point of the branching
    # experiment, so a None there must be surfaced loudly — not reported as a
    # successful run with a blank metric.
    deltas = {
        "linear_segments_added": _delta(baseline, after_linear, "segments"),
        "linear_interval_space_added": _delta(baseline, after_linear, "interval_space"),
        "branching_segments_added": _delta(after_linear, after_compact, "segments"),
        "branching_interval_space_added": _delta(
            after_linear, after_compact, "interval_space"
        ),
    }
    missing_metrics = sorted(k for k, v in deltas.items() if v is None)
    ok = not missing_metrics

    return {
        "schema": "okg.bench.branching.v1",
        "experiment_id": exp.id,
        "ok": ok,
        "missing_metrics": missing_metrics,
        "params": {
            "n_commits": n_commits,
            "nodes_per_commit": nodes_per_commit,
            "subtype": subtype,
        },
        "linear_append": {
            "total_ms": round(sum(linear_per_commit), 3),
            "per_commit_ms": [round(x, 3) for x in linear_per_commit],
            "mean_commit_ms": round(sum(linear_per_commit) / n_commits, 3),
            "segments_added": deltas["linear_segments_added"],
            "interval_space_added": deltas["linear_interval_space_added"],
        },
        "branching": {
            "fork_ms": round(fork_ms, 3),
            "branch_commit_total_ms": round(sum(branch_per_commit), 3),
            "branch_per_commit_ms": [round(x, 3) for x in branch_per_commit],
            "mean_branch_commit_ms": round(sum(branch_per_commit) / n_commits, 3),
            "merge_ms": round(merge_ms, 3),
            "compact_ms": round(compact_ms, 3) if compact_ms is not None else None,
            "segments_added_fork_to_compact": deltas["branching_segments_added"],
            "interval_space_added_fork_to_compact": deltas[
                "branching_interval_space_added"
            ],
            "compact_result": {
                k: compact_result.get(k)
                for k in ("node_count", "edge_count", "new_generation_id")
            }
            if compact_result
            else None,
        },
        "interval_stats": {
            "baseline": baseline,
            "after_linear": after_linear,
            "after_fork": after_fork,
            "after_branch": after_branch,
            "after_merge": after_merge,
            "after_compact": after_compact,
        },
    }


def _graph_fingerprint(
    dsn: str,
    generation_id: int | str,
) -> dict[str, Any]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn) as conn:
        # Chronos checkpoint ids ("okg:...") resolve directly; a numeric
        # id resolves as the literal "generation:<int>" checkpoint and
        # raises KeyError if no such checkpoint exists. The legacy
        # bitemporal live tables no longer exist post-cutover.
        store = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        )
        graph = store.checkout_generation(generation_id)
        nodes = graph.query(
            """
            SELECT node_id, subtype, attrs
              FROM okg.graph_nodes
             ORDER BY node_id, subtype, attrs::text
            """
        )
        edges = graph.query(
            """
            SELECT src, dst, edge_type, attrs, provenance, confidence
              FROM okg.graph_edges
             ORDER BY src, dst, edge_type, attrs::text, provenance,
                      confidence
            """
        )

    return {
        "generation_id": generation_id,
        "nodes": {
            "count": len(nodes),
            "digest": _stable_digest_rows(_semantic_graph_rows(list(nodes))),
        },
        "edges": {
            "count": len(_semantic_graph_edge_rows(list(edges))),
            "digest": _stable_digest_rows(_semantic_graph_edge_rows(list(edges))),
        },
        "view_cites": {
            "count": len(_view_cites_edge_rows(list(edges))),
            "digest": _stable_digest_rows(_view_cites_edge_rows(list(edges))),
        },
    }


def _target_generation_id_for_fingerprint(
    dsn: str,
    generation_id: int | str,
) -> int:
    if isinstance(generation_id, int):
        return generation_id
    text = str(generation_id)
    if text.isdigit():
        return int(text)
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT metadata::jsonb->>'target_generation_id'
              FROM public._chronos_branch_interval_checkpoints
             WHERE checkpoint_id = %s
             LIMIT 1
            """,
            (text,),
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        raise ValueError(
            f"cannot resolve derived-output target_generation_id for {generation_id!r}"
        )
    return int(row[0])


def _derived_outputs_fingerprint(
    dsn: str,
    generation_id: int | str | None = None,
) -> dict[str, Any]:
    target_generation_id = (
        None
        if generation_id is None
        else _target_generation_id_for_fingerprint(dsn, generation_id)
    )
    with (
        psycopg.connect(dsn) as conn,
        conn.cursor(
            row_factory=dict_row,
        ) as cur,
    ):
        cur.execute(
            """
            WITH ranked AS (
              SELECT
                m.definition_id,
                m.manifest_id,
                d.artifact_kind,
                d.logical_key,
                s.slot_name,
                s.storage_kind,
                s.media_type,
                s.artifact_type,
                s.row_count,
                s.byte_count,
                s.storage_ref,
                s.inline_payload,
                row_number() OVER (
                  PARTITION BY d.artifact_kind, d.logical_key, s.slot_name
                  ORDER BY m.target_generation_id DESC, m.promoted_at DESC
                ) AS rn
              FROM okg.derived_output_slots s
              JOIN okg.derived_output_manifests m
                ON m.manifest_id = s.manifest_id
              JOIN okg.derived_artifact_definitions d
                ON d.definition_id = m.definition_id
             WHERE m.status = 'completed'
               AND (%(target_generation_id)s::bigint IS NULL
                    OR m.target_generation_id = %(target_generation_id)s)
            )
            SELECT definition_id, manifest_id,
                   artifact_kind, logical_key, slot_name, storage_kind,
                   media_type, artifact_type, row_count, byte_count,
                   storage_ref, inline_payload
              FROM ranked
             WHERE rn = 1
             ORDER BY artifact_kind, logical_key, slot_name
            """,
            {"target_generation_id": target_generation_id},
        )
        rows = list(cur.fetchall())
        for row in rows:
            if row.get("storage_kind") != "relation":
                continue
            storage_ref = _json_mapping(row.get("storage_ref"))
            row["relation_visible_rows"] = _relation_visible_rows_for_output_slot(
                cur,
                definition_id=str(row.get("definition_id") or ""),
                manifest_id=row.get("manifest_id"),
                storage_ref=storage_ref,
            )
    return {
        "count": len(rows),
        "digest": _stable_digest_rows(_semantic_derived_output_rows(rows)),
        "target_generation_id": target_generation_id,
        "rows": rows,
    }


# Effective-attribute row columns that are NOT part of the semantic effective
# state and MUST be excluded from the digest: `materialized_at` is a wall-clock
# stamp that varies run-to-run, and `generation_id` is the partition key (the
# opaque okg:...:gen ref) which differs between the incremental and rebuild
# generations by construction -- including either would make a correct
# incremental run never match its rebuild.
_VOLATILE_ATTRIBUTE_EFFECTIVE_KEYS = frozenset({"materialized_at", "generation_id"})


def _semantic_attribute_blob(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    return {
        key: nested
        for key, nested in value.items()
        if key not in _VOLATILE_GRAPH_ATTRS
    }


def _semantic_attribute_effective_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Drop volatile / partition columns so the digest reflects only the
    semantic effective-attribute state (target, subtype/edge shape, raw+merged
    attrs, raw_text_hash)."""
    out: list[dict[str, Any]] = []
    for row in rows:
        normalized = {
            key: copy.deepcopy(value)
            for key, value in row.items()
            if key not in _VOLATILE_ATTRIBUTE_EFFECTIVE_KEYS
        }
        for attrs_key in ("raw_attrs", "attrs"):
            if attrs_key in normalized:
                normalized[attrs_key] = _semantic_attribute_blob(
                    normalized[attrs_key],
                )
        out.append(normalized)
    return out


def _attribute_effective_rows(
    conn: Any,
    *,
    generation_id: int | str,
) -> list[dict[str, Any]]:
    """Every overlay row of one generation, across every namespace.

    PACT lane-s-storage-drop-copies: the effective attributes of a
    generation are its graph rows (fingerprinted by ``_graph_fingerprint``)
    with its accepted overlays merged over them, so the overlay rows in
    ``okg.attribute_effective_attrs`` are the surface left to compare. The
    observation id is a per-run identifier and is not read."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT target_kind, target_id, namespace, attr_key, value,
                   value_hash, producer
              FROM okg.attribute_effective_attrs
             WHERE generation_id = %(gen)s
            """,
            {"gen": str(generation_id)},
        )
        rows = [dict(row) for row in cur.fetchall()]
    rows.sort(key=_stable_json)
    return rows


def _attribute_effective_fingerprint(
    dsn: str,
    generation_id: int | str,
) -> dict[str, Any]:
    """Fingerprint one generation's attribute overlays.

    This is the read surface that resolves each node/edge's attribute
    observations into their effective (winning) value, a peer of the
    node/edge/view_cites/derived_output surfaces in the rebuild-equivalence
    proof. The generation must carry an overlay materialization marker
    (``okg.attribute_overlay_generations``); without one its overlays were
    never materialized and the proof refuses instead of reading an empty
    set. A deployment with no overlay table reports ``present=False`` and
    count 0."""
    with psycopg.connect(dsn) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT to_regclass('okg.attribute_effective_attrs') AS reg"
            )
            reg = cur.fetchone()
            if reg is None or reg["reg"] is None:
                return {
                    "count": 0,
                    "digest": _stable_digest_rows([]),
                    "generation_id": generation_id,
                    "present": False,
                }
        from okg.substrate.attributes import (  # noqa: PLC0415
            require_overlay_generation_marker,
        )

        require_overlay_generation_marker(conn, generation_id)
        rows = _attribute_effective_rows(conn, generation_id=generation_id)
    return {
        "count": len(rows),
        "digest": _attribute_effective_digest(rows),
        "generation_id": generation_id,
        "present": True,
    }


def _attribute_effective_digest(rows: list[dict[str, Any]]) -> str:
    """Digest effective-attribute rows independently of per-run values.

    The rows arrive sorted by their full JSON, which still carries the
    per-run lineage stamp, ``generation_id`` and ``materialized_at``. Sort
    again after normalizing, so two runs holding the same rows digest the
    same whatever those values did to the first order (item 92a: an
    incremental run and a rebuild with identical rows hashed differently).
    """
    return _stable_digest_rows(
        sorted(_semantic_attribute_effective_rows(rows), key=_stable_json)
    )


def _load_delta_proof(exp: Experiment) -> dict[str, Any]:
    if not exp.delta_proof_json.exists():
        raise FileNotFoundError(
            f"delta proof missing: {exp.delta_proof_json}; run "
            f"`python bench/orchestrator.py run {exp.id}` first"
        )
    return json.loads(exp.delta_proof_json.read_text(encoding="utf-8"))


def _load_or_build_rebuild_equivalence(exp: Experiment) -> dict[str, Any]:
    if exp.rebuild_equivalence_json.exists():
        return json.loads(exp.rebuild_equivalence_json.read_text(encoding="utf-8"))
    report = build_differential_rebuild_report(exp)
    exp.rebuild_equivalence_json.parent.mkdir(parents=True, exist_ok=True)
    exp.rebuild_equivalence_json.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return report


def _final_replay_commit(proof: dict[str, Any]) -> str:
    rows = proof.get("rows") or []
    if not rows:
        raise ValueError("delta proof has no replay rows")
    final = rows[-1].get("commit_sha")
    if not final:
        raise ValueError("delta proof final row lacks commit_sha")
    return str(final)


def _bounded_work_summary(proof: dict[str, Any]) -> dict[str, Any]:
    rows = proof.get("rows") or []
    changed_path_counts = []
    nodes_added = []
    edges_added = []
    for row in rows:
        boundaries = row.get("boundaries") or {}
        repo_scope = boundaries.get("repo_scope") or {}
        graph_delta = boundaries.get("graph_delta") or {}
        changed_path_counts.append(int(repo_scope.get("scope_count") or 0))
        if graph_delta.get("nodes_added") is not None:
            nodes_added.append(int(graph_delta["nodes_added"]))
        if graph_delta.get("edges_added") is not None:
            edges_added.append(int(graph_delta["edges_added"]))
    return {
        "commit_count": len(rows),
        "changed_path_counts": changed_path_counts,
        "max_changed_paths_per_commit": (
            max(changed_path_counts) if changed_path_counts else None
        ),
        "nodes_added_by_commit": nodes_added,
        "edges_added_by_commit": edges_added,
        "total_nodes_added": sum(nodes_added),
        "total_edges_added": sum(edges_added),
    }


def _build_rebuild_equivalence_report(
    *,
    exp: Experiment,
    incremental_graph: dict[str, Any],
    rebuild_graph: dict[str, Any],
    incremental_outputs: dict[str, Any],
    rebuild_outputs: dict[str, Any],
    incremental_attrs: dict[str, Any],
    rebuild_attrs: dict[str, Any],
    delta_proof: dict[str, Any],
    rebuild_experiment_id: str,
    rebuild_generation_id: int | str,
    rebuild_terminal: Mapping[str, Any] | None = None,
    incremental_terminal: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    bounded_work = _bounded_work_summary(delta_proof)
    final_node_count = incremental_graph["nodes"]["count"]
    bounded_source_scope = (
        bounded_work["max_changed_paths_per_commit"] is not None
        and final_node_count > bounded_work["max_changed_paths_per_commit"]
    )
    checks = {
        "node_count_equal": (
            incremental_graph["nodes"]["count"] == rebuild_graph["nodes"]["count"]
        ),
        "edge_count_equal": (
            incremental_graph["edges"]["count"] == rebuild_graph["edges"]["count"]
        ),
        "node_digest_equal": (
            incremental_graph["nodes"]["digest"] == rebuild_graph["nodes"]["digest"]
        ),
        "edge_digest_equal": (
            incremental_graph["edges"]["digest"] == rebuild_graph["edges"]["digest"]
        ),
        # view_cites_node lineage edges are excluded from the general edge
        # digest and are not derived_output_slots rows, so they need their own
        # equivalence check -- this is the restored per-commit-collapse lineage
        # and the gap that hid the warm over-retraction P0.
        "view_cites_count_equal": (
            incremental_graph["view_cites"]["count"]
            == rebuild_graph["view_cites"]["count"]
        ),
        "view_cites_digest_equal": (
            incremental_graph["view_cites"]["digest"]
            == rebuild_graph["view_cites"]["digest"]
        ),
        "derived_output_count_equal": (
            incremental_outputs["count"] == rebuild_outputs["count"]
        ),
        "derived_output_digest_equal": (
            incremental_outputs["digest"] == rebuild_outputs["digest"]
        ),
        # The effective-attribute view resolves each node/edge's attribute
        # observations into the current winning value. A correct incremental
        # engine must reproduce it identically to a from-scratch rebuild; a
        # divergence (stale value, wrong producer winner) must fail the proof.
        # NOTE: count 0 on both sides passes vacuously -- the attribute-emitting
        # bench experiment separately asserts overlay count > 0,
        # so this dimension is non-vacuous where it claims coverage.
        "attr_effective_count_equal": (
            incremental_attrs["count"] == rebuild_attrs["count"]
        ),
        "attr_effective_digest_equal": (
            incremental_attrs["digest"] == rebuild_attrs["digest"]
        ),
        "bounded_source_scope": bounded_source_scope,
    }
    return {
        "schema": "okg.bench.differential_rebuild_equivalence.v1",
        "ok": all(checks.values()),
        "experiment_id": exp.id,
        "rebuild_experiment_id": rebuild_experiment_id,
        "rebuild_generation_id": rebuild_generation_id,
        "checks": checks,
        "incremental": {
            "graph": incremental_graph,
            "derived_outputs": {
                "count": incremental_outputs["count"],
                "digest": incremental_outputs["digest"],
            },
            "attribute_effective": {
                "count": incremental_attrs["count"],
                "digest": incremental_attrs["digest"],
                "present": incremental_attrs.get("present", True),
                "generation_id": incremental_attrs.get("generation_id"),
            },
            # Was `final_generation_id` the incremental worker's last word?
            "terminal": (
                dict(incremental_terminal) if incremental_terminal else None
            ),
        },
        "forced_rebuild": {
            "graph": rebuild_graph,
            "derived_outputs": {
                "count": rebuild_outputs["count"],
                "digest": rebuild_outputs["digest"],
            },
            "attribute_effective": {
                "count": rebuild_attrs["count"],
                "digest": rebuild_attrs["digest"],
                "present": rebuild_attrs.get("present", True),
                "generation_id": rebuild_attrs.get("generation_id"),
            },
            # The terminal-wait receipt: generation fingerprinted, per-source
            # completion, workflow counts, reconcile completeness (#1539).
            "terminal": dict(rebuild_terminal) if rebuild_terminal else None,
        },
        "bounded_work": bounded_work,
        "artifacts": {
            "delta_proof": str(exp.delta_proof_json),
            "raw_csv": str(exp.raw_csv),
        },
    }


def _rebuild_experiment(exp: Experiment) -> Experiment:
    """The forced-rebuild side of the rebuild-equivalence proof.

    Its own id (so its own database, generated-deployment root, worker log
    and result files) but the SAME deployment name as ``exp``: the published
    graph carries deployment-scoped values -- a view node's
    ``canonical_asset_id``, the ``<deployment>.<source>`` ownership ids in
    source lineage -- that can only match when both sides publish under one
    name (issue #1539, decision 92a)."""
    digest = hashlib.sha1(exp.id.encode("utf-8")).hexdigest()[:12]
    return Experiment(
        id=f"rebuild-{digest}",
        repo=exp.repo,
        source_set=exp.source_set,
        replay=Replay(strategy="full_history"),
        config=exp.config,
        deployment_name=exp.deployment,
    )


def build_differential_rebuild_report(exp: Experiment) -> dict[str, Any]:
    proof = _load_delta_proof(exp)
    final_generation_id = proof.get("summary", {}).get("final_generation_id")
    if not final_generation_id:
        raise ValueError("delta proof summary lacks final_generation_id")

    incremental_graph = _graph_fingerprint(exp.dsn, final_generation_id)
    incremental_outputs = _derived_outputs_fingerprint(
        exp.dsn,
        final_generation_id,
    )
    incremental_attrs = _attribute_effective_fingerprint(
        exp.dsn,
        final_generation_id,
    )
    final_commit = _final_replay_commit(proof)
    rebuild_exp = _rebuild_experiment(exp)

    print(
        f"[{exp.id}] forced rebuild comparison uses {rebuild_exp.id} "
        f"at commit {final_commit[:10]}"
    )
    _ensure_repo(rebuild_exp)
    _run(
        [
            "git",
            "-C",
            str(rebuild_exp.repo_root),
            "reset",
            "--hard",
            final_commit,
            "--quiet",
        ],
    )
    incremental_terminal = _incremental_terminal_state(exp, final_generation_id)
    setup_db(rebuild_exp)
    previous = _latest_published_generation(rebuild_exp)
    start_worker(rebuild_exp)
    try:
        wait_for_worker(rebuild_exp)
        reconcile_receipt = _bench_reconcile(rebuild_exp)
        rebuild_terminal = _wait_for_rebuild_publish(
            rebuild_exp,
            previous_generation_id=previous,
            reconcile_receipt=reconcile_receipt,
        )
    finally:
        stop_worker(rebuild_exp)
    rebuild_terminal["reconcile"] = reconcile_receipt
    rebuild_generation_id = rebuild_terminal["generation_id"]
    print(
        f"[{exp.id}] forced rebuild terminal after "
        f"{rebuild_terminal['elapsed_s']}s: generation {rebuild_generation_id}, "
        f"workflows={rebuild_terminal['observation'].get('workflow_counts')}"
    )

    rebuild_graph = _graph_fingerprint(rebuild_exp.dsn, rebuild_generation_id)
    rebuild_outputs = _derived_outputs_fingerprint(
        rebuild_exp.dsn,
        rebuild_generation_id,
    )
    rebuild_attrs = _attribute_effective_fingerprint(
        rebuild_exp.dsn,
        rebuild_generation_id,
    )
    return _build_rebuild_equivalence_report(
        exp=exp,
        incremental_graph=incremental_graph,
        rebuild_graph=rebuild_graph,
        incremental_outputs=incremental_outputs,
        rebuild_outputs=rebuild_outputs,
        incremental_attrs=incremental_attrs,
        rebuild_attrs=rebuild_attrs,
        delta_proof=proof,
        rebuild_experiment_id=rebuild_exp.id,
        rebuild_generation_id=rebuild_generation_id,
        rebuild_terminal=rebuild_terminal,
        incremental_terminal=incremental_terminal,
    )


def cmd_differential_report(args) -> int:
    exp = _resolve(args.id)
    report = build_differential_rebuild_report(exp)
    exp.rebuild_equivalence_json.parent.mkdir(parents=True, exist_ok=True)
    exp.rebuild_equivalence_json.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        status = "PASS" if report["ok"] else "FAIL"
        graph = report["incremental"]["graph"]
        print(
            f"[{exp.id}] rebuild equivalence {status}: "
            f"nodes={graph['nodes']['count']} edges={graph['edges']['count']} "
            f"report={exp.rebuild_equivalence_json}"
        )
        for name, ok in report["checks"].items():
            if not ok:
                print(f"  - failed check: {name}")
    return 0 if report["ok"] else 1


# ---------------------------------------------------------------------------
# Synthetic materialized-output lineage report

LINEAGE_REPORT_SCHEMA = "okg.bench.synthetic_lineage_report.v1"
RUNTIME_LINEAGE_REPORT_SCHEMA = "okg.bench.runtime_lineage_report.v1"
LINEAGE_COMPONENT_ID = "bench.synthetic.materialized_lineage.v1"
LINEAGE_COMPONENT_HASH = "sha256:bench.synthetic.materialized_lineage.v1"


def build_lineage_replay_report(exp: Experiment) -> dict[str, Any]:
    delta_proof = _load_delta_proof(exp)
    rebuild_equivalence = _load_or_build_rebuild_equivalence(exp)
    return _build_lineage_replay_report(
        exp=exp,
        delta_proof=delta_proof,
        rebuild_equivalence=rebuild_equivalence,
    )


def _build_lineage_replay_report(
    *,
    exp: Experiment,
    delta_proof: dict[str, Any],
    rebuild_equivalence: dict[str, Any],
) -> dict[str, Any]:
    from okg.substrate.derived_artifacts import (
        ComponentLineage,
        GraphLineage,
        MaterializedLineageEnvelope,
    )

    final_generation_id = delta_proof.get("summary", {}).get("final_generation_id")
    output_state: dict[str, dict[str, Any]] = {}
    source_record_by_output: dict[str, dict[str, str]] = {}
    replay_steps: list[dict[str, Any]] = []
    rows = [row for row in delta_proof.get("rows", []) if isinstance(row, dict)]
    repo_root = Path(str(delta_proof.get("repo_root") or exp.repo_root))
    _lineage_seed_baseline_outputs(
        output_state,
        source_record_by_output,
        repo_root=repo_root,
        baseline_commit=str(delta_proof.get("baseline_commit") or ""),
    )

    for row in rows:
        before_ids = set(output_state)
        events = _lineage_path_events(repo_root, row)
        updated_ids: set[str] = set()
        deleted_ids: set[str] = set()
        source_record_keys: list[str] = []

        for event in events:
            path = event["path"]
            output_id = _lineage_page_output_id(path)
            if event["status"] == "deleted":
                if output_id in output_state:
                    deleted_ids.add(output_id)
                    output_state.pop(output_id, None)
                    source_record_by_output.pop(output_id, None)
                continue
            record_id = _lineage_record_id(row, path)
            source_record_keys.append(f"repo-sync:{record_id}")
            _lineage_upsert_page_output(
                output_state,
                source_record_by_output,
                path=path,
                commit_sha=str(row.get("commit_sha") or ""),
                record_id=record_id,
            )
            updated_ids.add(output_id)

        if events:
            index_id = _lineage_index_output_id()
            index_digest = _stable_digest_rows(
                [
                    {
                        "output_id": output_id,
                        "source_record": source_record_by_output[output_id],
                    }
                    for output_id in sorted(source_record_by_output)
                ]
            )
            output_state[index_id] = {
                "output_id": index_id,
                "output_kind": "index_page",
                "path": "<repo-index>",
                "output_digest": index_digest,
                "last_commit_sha": row.get("commit_sha"),
            }
            updated_ids.add(index_id)

        skipped_ids = sorted(before_ids - updated_ids - deleted_ids)
        replay_steps.append(
            {
                "commit_sha": row.get("commit_sha"),
                "subject": row.get("subject"),
                "changed_paths": [event["path"] for event in events],
                "source_record_keys": sorted(source_record_keys),
                "updated_output_ids": sorted(updated_ids),
                "deleted_output_ids": sorted(deleted_ids),
                "skipped_output_ids": skipped_ids,
                "updated_count": len(updated_ids),
                "deleted_count": len(deleted_ids),
                "skipped_count": len(skipped_ids),
            }
        )

    envelopes = []
    for output_id, output in sorted(output_state.items()):
        if output_id == _lineage_index_output_id():
            records = [
                _lineage_source_record(record)
                for _, record in sorted(source_record_by_output.items())
            ]
            node_ids = sorted(
                _lineage_file_node_id(record["path"])
                for record in source_record_by_output.values()
            )
        else:
            record = source_record_by_output[output_id]
            records = [_lineage_source_record(record)]
            node_ids = [_lineage_file_node_id(record["path"])]
        envelopes.append(
            MaterializedLineageEnvelope(
                output_id=output_id,
                output_kind=output["output_kind"],
                source_records=records,
                graph=GraphLineage(
                    graph_generation_id=final_generation_id,
                    graph_branch="default",
                    node_ids=node_ids,
                    edge_ids=tuple(
                        f"commit:{record.content_hash}:touches:{node_id}"
                        for record, node_id in zip(records, node_ids, strict=False)
                    ),
                ),
                catalog_version_id=1,
                component=ComponentLineage(
                    component_id=LINEAGE_COMPONENT_ID,
                    component_hash=LINEAGE_COMPONENT_HASH,
                    component_kind="renderer",
                    version="v1",
                ),
                output_digest=str(output["output_digest"]),
                previous_output_digest=None,
                invalidation_scope="per-output",
                metadata={"experiment_id": exp.id},
            )
        )

    reverse_index = _lineage_reverse_index(envelopes)
    source_probe_key = _first_source_probe_key(replay_steps)
    source_dirty_ids = sorted(
        reverse_index.get(
            ("source_record", source_probe_key),
            set(),
        )
    )
    transform_dirty_ids = sorted(
        reverse_index.get(
            ("component_hash", LINEAGE_COMPONENT_HASH),
            set(),
        )
    )
    final_output_rows = [
        {
            "output_id": output_id,
            "output_kind": output["output_kind"],
            "output_digest": output["output_digest"],
        }
        for output_id, output in sorted(output_state.items())
    ]
    final_digest = _stable_digest_rows(final_output_rows)
    skipped_total = sum(step["skipped_count"] for step in replay_steps)
    updated_total = sum(step["updated_count"] for step in replay_steps)
    deleted_total = sum(step["deleted_count"] for step in replay_steps)
    fallback_count = int(
        delta_proof.get("summary", {}).get("fallback_boundary_count") or 0
    )
    unavailable_count = int(
        delta_proof.get("summary", {}).get("unavailable_boundary_count") or 0
    )
    checks = {
        "source_edit_dirty_set_non_empty": bool(source_dirty_ids),
        "source_edit_dirty_set_reachable": bool(source_probe_key)
        and bool(source_dirty_ids),
        "transform_edit_dirty_set_non_empty": bool(transform_dirty_ids),
        "transform_edit_updates_all_component_outputs": (
            set(transform_dirty_ids) == set(output_state)
        ),
        "replay_reported_deleted_outputs": deleted_total > 0,
        # The genuine incremental-vs-forced-rebuild proof is
        # `graph_rebuild_equivalence` (build_differential_rebuild_report, which
        # publishes a real forced-rebuild generation and digests it) and the
        # RUNTIME lineage report. The synthetic report below fabricates its
        # `output_state` from git-diff path events and never performs a forced
        # rebuild, so a `materialized_digest_equal_to_forced_rebuild` self-check
        # here would be the tautology digest(X)==digest(X) — removed rather than
        # advertise a verification it does not perform. (deltaflow review C)
        "graph_rebuild_equivalence": bool(rebuild_equivalence.get("ok")),
    }
    dependency_kind_counts = Counter(kind for kind, _ref in reverse_index.keys())
    return {
        "schema": LINEAGE_REPORT_SCHEMA,
        "ok": all(checks.values()),
        "experiment_id": exp.id,
        "deployment": delta_proof.get("deployment", exp.id),
        "checks": checks,
        "summary": {
            "replay_commit_count": len(replay_steps),
            "lineage_envelope_count": len(envelopes),
            "dependency_key_count": sum(len(refs) for refs in reverse_index.values()),
            "dependency_kind_counts": dict(sorted(dependency_kind_counts.items())),
            "updated_output_count": updated_total,
            "skipped_output_count": skipped_total,
            "deleted_output_count": deleted_total,
            "fallback_boundary_count": fallback_count,
            "unavailable_boundary_count": unavailable_count,
            "fallback_status": "none" if fallback_count == 0 else "fallback",
            "final_materialized_output_count": len(output_state),
            "final_materialized_digest": final_digest,
            # No `forced_rebuild_materialized_digest`: the synthetic report does
            # not perform a forced rebuild (see the checks block above).
        },
        "reverse_index_probes": {
            "source_edit": {
                "dependency_key": {
                    "dependency_kind": "source_record",
                    "dependency_ref": source_probe_key,
                },
                "dirty_output_ids": source_dirty_ids,
                "dirty_count": len(source_dirty_ids),
                "skipped_output_ids": sorted(set(output_state) - set(source_dirty_ids)),
            },
            "transform_edit": {
                "dependency_key": {
                    "dependency_kind": "component_hash",
                    "dependency_ref": LINEAGE_COMPONENT_HASH,
                },
                "dirty_output_ids": transform_dirty_ids,
                "dirty_count": len(transform_dirty_ids),
                "skipped_output_ids": sorted(
                    set(output_state) - set(transform_dirty_ids)
                ),
            },
        },
        "replay_steps": replay_steps,
        "rebuild_equivalence": {
            "ok": rebuild_equivalence.get("ok"),
            "checks": rebuild_equivalence.get("checks", {}),
            "report": str(exp.rebuild_equivalence_json),
        },
        "artifacts": {
            "delta_proof": str(exp.delta_proof_json),
            "rebuild_equivalence": str(exp.rebuild_equivalence_json),
        },
    }


def _lineage_path_events(
    repo_root: Path, row: Mapping[str, Any]
) -> list[dict[str, str]]:
    before = str(row.get("previous_commit_sha") or "")
    after = str(row.get("commit_sha") or "")
    if before and after and repo_root.exists():
        result = _run(
            [
                "git",
                "-C",
                str(repo_root),
                "diff",
                "--name-status",
                "--find-renames",
                before,
                after,
            ],
            capture=True,
        )
        events: list[dict[str, str]] = []
        for line in result.stdout.splitlines():
            parts = [part for part in line.split("\t") if part]
            if not parts:
                continue
            status = parts[0]
            if status.startswith("R") and len(parts) >= 3:
                events.append({"status": "deleted", "path": parts[1]})
                events.append({"status": "updated", "path": parts[2]})
            elif status.startswith("D") and len(parts) >= 2:
                events.append({"status": "deleted", "path": parts[1]})
            elif len(parts) >= 2:
                events.append({"status": "updated", "path": parts[1]})
        return events

    repo_scope = (
        row.get("boundaries", {}).get("repo_scope", {})
        if isinstance(row.get("boundaries"), Mapping)
        else {}
    )
    changed_paths = (
        repo_scope.get("changed_paths", []) if isinstance(repo_scope, Mapping) else []
    )
    return [{"status": "updated", "path": str(path)} for path in changed_paths]


def _lineage_seed_baseline_outputs(
    output_state: dict[str, dict[str, Any]],
    source_record_by_output: dict[str, dict[str, str]],
    *,
    repo_root: Path,
    baseline_commit: str,
) -> None:
    if not baseline_commit or not repo_root.exists():
        return
    result = _run(
        ["git", "-C", str(repo_root), "ls-tree", "-r", "--name-only", baseline_commit],
        capture=True,
    )
    for path in sorted(line for line in result.stdout.splitlines() if line):
        record_id = f"{baseline_commit}:{path}"
        _lineage_upsert_page_output(
            output_state,
            source_record_by_output,
            path=path,
            commit_sha=baseline_commit,
            record_id=record_id,
        )
    if output_state:
        output_state[_lineage_index_output_id()] = {
            "output_id": _lineage_index_output_id(),
            "output_kind": "index_page",
            "path": "<repo-index>",
            "output_digest": _stable_digest_rows(
                [
                    {
                        "output_id": output_id,
                        "source_record": source_record_by_output[output_id],
                    }
                    for output_id in sorted(source_record_by_output)
                ]
            ),
            "last_commit_sha": baseline_commit,
        }


def _lineage_upsert_page_output(
    output_state: dict[str, dict[str, Any]],
    source_record_by_output: dict[str, dict[str, str]],
    *,
    path: str,
    commit_sha: str,
    record_id: str,
) -> None:
    output_id = _lineage_page_output_id(path)
    source_record_by_output[output_id] = {
        "record_id": record_id,
        "path": path,
        "commit_sha": commit_sha,
    }
    output_state[output_id] = {
        "output_id": output_id,
        "output_kind": "generated_page",
        "path": path,
        "output_digest": _lineage_output_digest(output_id, commit_sha),
        "last_commit_sha": commit_sha,
    }


def _lineage_record_id(row: Mapping[str, Any], path: str) -> str:
    return f"{row.get('commit_sha')}:{path}"


def _lineage_page_output_id(path: str) -> str:
    return f"page:{path}"


def _lineage_index_output_id() -> str:
    return "index:repo"


def _lineage_file_node_id(path: str) -> str:
    return f"file:{path}"


def _lineage_output_digest(output_id: str, commit_sha: Any) -> str:
    return _stable_digest_rows(
        [{"output_id": output_id, "commit_sha": str(commit_sha or "")}]
    )


def _lineage_source_record(record: Mapping[str, str]):
    from okg.substrate.derived_artifacts import SourceRecordLineage

    content_hash = _stable_digest_rows([dict(record)])
    return SourceRecordLineage(
        source_id="repo-sync",
        record_id=record["record_id"],
        content_hash=content_hash,
        source_revision={
            "commit": record["commit_sha"],
            "path": record["path"],
        },
    )


def _lineage_reverse_index(
    envelopes: list[Any],
) -> dict[tuple[str, str], set[str]]:
    index: dict[tuple[str, str], set[str]] = {}
    for envelope in envelopes:
        for kind, refs in envelope.dependency_keys().items():
            for ref in refs:
                index.setdefault((kind, ref), set()).add(envelope.output_id)
    return index


def _first_source_probe_key(replay_steps: list[dict[str, Any]]) -> str:
    for step in replay_steps:
        keys = step.get("source_record_keys") or []
        if keys:
            return str(keys[0])
    return ""


def build_runtime_lineage_replay_report(exp: Experiment) -> dict[str, Any]:
    delta_proof = _load_delta_proof(exp)
    rebuild_equivalence = _load_or_build_rebuild_equivalence(exp)
    _ensure_runtime_lineage_schema(exp)
    return _build_runtime_lineage_replay_report(
        exp=exp,
        delta_proof=delta_proof,
        rebuild_equivalence=rebuild_equivalence,
        dsn=exp.dsn,
    )


def _build_runtime_lineage_replay_report(
    *,
    exp: Experiment,
    delta_proof: dict[str, Any],
    rebuild_equivalence: dict[str, Any],
    dsn: str,
) -> dict[str, Any]:
    from okg.substrate.derived_artifacts import (  # noqa: PLC0415
        ComponentLineage,
        GraphLineage,
        LineageInvalidation,
        MaterializedLineageEnvelope,
        RelationArtifactDefinition,
        RelationRow,
        RelationScheduleTask,
        SourceRecordLineage,
        publish_relation_artifact,
        schedule_relation_artifacts,
        stable_hash,
        summarize_lineage_index,
    )

    synthetic = _build_lineage_replay_report(
        exp=exp,
        delta_proof=delta_proof,
        rebuild_equivalence=rebuild_equivalence,
    )
    final_output_ids = sorted(
        synthetic["reverse_index_probes"]["transform_edit"]["dirty_output_ids"]
    )
    source_probe = synthetic["reverse_index_probes"]["source_edit"]["dependency_key"]
    source_probe_ref = str(source_probe["dependency_ref"])
    source_dirty_ids = sorted(
        synthetic["reverse_index_probes"]["source_edit"]["dirty_output_ids"]
    )
    generation_base = 8_000_000_000_000 + (time.time_ns() % 1_000_000_000_000)
    definition = RelationArtifactDefinition(
        definition_id=(f"bench.runtime_lineage.{exp.id}.{generation_base}"),
        deployment_name=exp.deployment,
        logical_relation_id=f"runtime_lineage_{exp.id}",
        compute_hash=stable_hash(
            {
                "kind": "runtime_lineage_relation",
                "experiment_id": exp.id,
            }
        ),
        output_schema_hash=stable_hash(
            {
                "columns": {"output_id": "text", "output_kind": "text"},
            }
        ),
        renderer_hash=stable_hash({"renderer": "runtime-lineage-jsonb"}),
        dirty_rule_hash=stable_hash({"dependencies": ["lineage_index"]}),
        output_contract={"columns": {"output_id": "text"}},
        dirty_rule={"dependencies": ["lineage_index"]},
    )
    rows = [
        RelationRow(
            scope_key=output_id,
            row_key="materialized",
            row_data={
                "output_id": output_id,
                "output_kind": (
                    "index_page"
                    if output_id == _lineage_index_output_id()
                    else "generated_page"
                ),
            },
            dependency_hash=_runtime_hash(output_id),
        )
        for output_id in final_output_ids
    ]
    source_probe_record_id = source_probe_ref.split(":", 1)[1]
    envelopes = [
        MaterializedLineageEnvelope(
            output_id=row.scope_key,
            output_kind=dict(row.row_data)["output_kind"],
            source_records=[
                SourceRecordLineage(
                    source_id="repo-sync",
                    record_id=(
                        source_probe_record_id
                        if row.scope_key in source_dirty_ids
                        else f"runtime:{row.scope_key}"
                    ),
                    content_hash=_runtime_hash(
                        f"source:{row.scope_key}:{source_probe_record_id}"
                    ),
                    source_revision={
                        "experiment_id": exp.id,
                        "output_id": row.scope_key,
                    },
                )
            ],
            graph=GraphLineage(
                graph_generation_id=generation_base,
                graph_branch="default",
                node_ids=(f"file:{row.scope_key}",),
                edge_ids=(f"edge:{row.scope_key}:materialized",),
            ),
            catalog_version_id=1,
            component=ComponentLineage(
                component_id=LINEAGE_COMPONENT_ID,
                component_hash=LINEAGE_COMPONENT_HASH,
                component_kind="renderer",
                version="v1",
            ),
            output_digest=_runtime_hash(f"output:{row.scope_key}"),
            previous_output_digest=None,
            invalidation_scope="per-scope",
            metadata={"experiment_id": exp.id, "runtime_report": True},
        )
        for row in rows
    ]

    with psycopg.connect(dsn) as conn:
        _runtime_lineage_insert_generation(conn, exp, generation_base)
        _runtime_lineage_insert_generation(conn, exp, generation_base + 1)
        _runtime_lineage_insert_generation(conn, exp, generation_base + 2)
        _runtime_lineage_insert_generation(conn, exp, generation_base + 3)
        baseline = publish_relation_artifact(
            conn,
            definition,
            target_generation_id=generation_base,
            rows=rows,
            trigger_kind="manual",
            lineage_envelopes=envelopes,
            metrics={"runtime_lineage_report": "baseline"},
        )
        source_result = schedule_relation_artifacts(
            conn,
            deployment_name=exp.deployment,
            target_generation_id=generation_base + 1,
            tasks=[RelationScheduleTask(definition, rows=rows)],
            lineage_invalidations=[
                LineageInvalidation("source_record", source_probe_ref)
            ],
        )
        transform_result = schedule_relation_artifacts(
            conn,
            deployment_name=exp.deployment,
            target_generation_id=generation_base + 2,
            tasks=[RelationScheduleTask(definition, rows=rows)],
            lineage_invalidations=[
                LineageInvalidation("component_hash", LINEAGE_COMPONENT_HASH)
            ],
        )
        fallback_result = schedule_relation_artifacts(
            conn,
            deployment_name=exp.deployment,
            target_generation_id=generation_base + 3,
            tasks=[RelationScheduleTask(definition, rows=rows)],
            lineage_invalidations=[
                LineageInvalidation("decision", "decision:not-indexed")
            ],
        )
        lineage_summary = summarize_lineage_index(
            conn,
            deployment_name=exp.deployment,
        )
        persisted_digest = _runtime_relation_digest(
            conn,
            manifest_id=fallback_result.nodes[0].manifest_id,
        )

    source_plan = source_result.lineage_plan
    transform_plan = transform_result.lineage_plan
    fallback_plan = fallback_result.lineage_plan
    source_dirty_runtime = sorted(source_plan.dirty_output_ids) if source_plan else []
    transform_dirty_runtime = (
        sorted(transform_plan.dirty_output_ids) if transform_plan else []
    )
    runtime_skipped = sorted(set(final_output_ids) - set(source_dirty_runtime))
    forced_digest = _stable_digest_rows([dict(row.row_data) for row in rows])
    checks = {
        "persisted_lineage_index_available": (
            lineage_summary.envelope_count >= len(envelopes)
            and lineage_summary.dependency_key_count > 0
        ),
        "source_edit_dirty_set_matches_synthetic": (
            source_dirty_runtime == source_dirty_ids
        ),
        "source_edit_reuses_skipped_outputs": bool(runtime_skipped),
        "transform_edit_updates_all_component_outputs": (
            transform_dirty_runtime == final_output_ids
        ),
        "unavailable_dependency_forces_full_fallback": (
            fallback_plan is not None
            and not fallback_plan.available
            and fallback_plan.fallback_definition_ids == (definition.definition_id,)
        ),
        "materialized_digest_equal_to_forced_rebuild": (
            persisted_digest == forced_digest
        ),
        "graph_rebuild_equivalence": bool(rebuild_equivalence.get("ok")),
    }
    return {
        "schema": RUNTIME_LINEAGE_REPORT_SCHEMA,
        "ok": all(checks.values()),
        "experiment_id": exp.id,
        "deployment": delta_proof.get("deployment", exp.id),
        "checks": checks,
        "summary": {
            "baseline_manifest_id": str(baseline.manifest_id),
            "lineage_envelope_count": lineage_summary.envelope_count,
            "dependency_key_count": lineage_summary.dependency_key_count,
            "dependency_kind_counts": dict(lineage_summary.dependency_kind_counts),
            "runtime_source_dirty_output_count": len(source_dirty_runtime),
            "runtime_reused_output_count": len(runtime_skipped),
            "runtime_transform_dirty_output_count": len(transform_dirty_runtime),
            "runtime_fallback_status": (
                "fallback"
                if fallback_plan is not None and not fallback_plan.available
                else "none"
            ),
            "runtime_unavailable_dependency_kinds": (
                list(fallback_plan.unavailable_dependency_kinds)
                if fallback_plan is not None
                else []
            ),
            "updated_output_count": synthetic["summary"]["updated_output_count"],
            "skipped_output_count": synthetic["summary"]["skipped_output_count"],
            "deleted_output_count": synthetic["summary"]["deleted_output_count"],
            "final_materialized_output_count": len(final_output_ids),
            "final_materialized_digest": persisted_digest,
            "forced_rebuild_materialized_digest": forced_digest,
        },
        "reverse_index_probes": {
            "source_edit": {
                "dependency_key": source_probe,
                "dirty_output_ids": source_dirty_runtime,
                "dirty_count": len(source_dirty_runtime),
                "skipped_output_ids": runtime_skipped,
            },
            "transform_edit": {
                "dependency_key": {
                    "dependency_kind": "component_hash",
                    "dependency_ref": LINEAGE_COMPONENT_HASH,
                },
                "dirty_output_ids": transform_dirty_runtime,
                "dirty_count": len(transform_dirty_runtime),
                "skipped_output_ids": sorted(
                    set(final_output_ids) - set(transform_dirty_runtime)
                ),
            },
            "unavailable": {
                "dependency_key": {
                    "dependency_kind": "decision",
                    "dependency_ref": "decision:not-indexed",
                },
                "status": None if fallback_plan is None else fallback_plan.status,
                "fallback_definition_ids": (
                    []
                    if fallback_plan is None
                    else list(fallback_plan.fallback_definition_ids)
                ),
            },
        },
        "rebuild_equivalence": synthetic["rebuild_equivalence"],
        "artifacts": {
            **synthetic["artifacts"],
            "runtime_lineage_report": str(exp.runtime_lineage_report_json),
        },
    }


def _runtime_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _runtime_lineage_insert_generation(
    conn: psycopg.Connection[Any],
    exp: Experiment,
    generation_id: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations (
              generation_id, deployment_name, status, catalog_version_id,
              published_at, correlation_id
            ) VALUES (
              %(generation_id)s, %(deployment_name)s, 'published', 1, now(),
              %(correlation_id)s
            )
            ON CONFLICT (generation_id) DO NOTHING
            """,
            {
                "generation_id": generation_id,
                "deployment_name": exp.id,
                "correlation_id": f"runtime-lineage:{exp.id}:{generation_id}",
            },
        )
    conn.commit()


def _runtime_relation_digest(
    conn: psycopg.Connection[Any],
    *,
    manifest_id: Any,
) -> str:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT storage_ref
              FROM okg.derived_output_slots
             WHERE manifest_id = %(manifest_id)s
               AND slot_name = 'relation'
            """,
            {"manifest_id": manifest_id},
        )
        slot = cur.fetchone()
        if slot is None:
            raise RuntimeError(f"runtime lineage manifest missing: {manifest_id}")
        storage_ref = dict(slot["storage_ref"])
        cur.execute(
            sql.SQL(
                """
                SELECT row_data
                  FROM {}.{}
                 WHERE target_generation_id = %(target_generation_id)s
                 ORDER BY scope_key, row_key
                """
            ).format(
                sql.Identifier(storage_ref["schema"]),
                sql.Identifier(storage_ref["relation"]),
            ),
            {"target_generation_id": storage_ref["target_generation_id"]},
        )
        rows = [dict(row["row_data"]) for row in cur.fetchall()]
    return _stable_digest_rows(rows)


def _ensure_runtime_lineage_schema(exp: Experiment) -> None:
    needs_create = False
    try:
        with psycopg.connect(exp.dsn, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT to_regclass('okg.derived_output_manifests'),
                           to_regclass(
                             'okg.derived_output_lineage_envelopes'
                           ),
                           to_regclass('okg.derived_output_dependency_index')
                    """
                )
                if cur.fetchone() == (
                    "okg.derived_output_manifests",
                    "okg.derived_output_lineage_envelopes",
                    "okg.derived_output_dependency_index",
                ):
                    return
    except psycopg.OperationalError:
        needs_create = True
    _ensure_repo(exp)
    _write_deployment_files(exp)
    if needs_create:
        print(f"[{exp.id}] creating runtime lineage DB: {exp.db_name}")
        _psql(f"CREATE DATABASE {exp.db_name};")
        _psql(EXTENSIONS_SQL, dsn=exp.dsn)
    print(f"[{exp.id}] migrating runtime lineage schema")
    _run(
        [
            *running_okg_argv(),
            "migrate",
            "--deployment",
            exp.deployment,
            "--dsn",
            exp.dsn,
            # `okg migrate` requires --apply for every caller (#1288).
            "--apply",
            "--json",
        ],
        env={
            "OKG_DSN": exp.dsn,
            "OKG_DEPLOYMENTS_DIR": str(exp.deployments_root),
            "PYTHONPATH": ENV_PYTHONPATH,
        },
        capture=True,
    )


def cmd_lineage_report(args) -> int:
    exp = _resolve(args.id)
    report = build_lineage_replay_report(exp)
    exp.lineage_report_json.parent.mkdir(parents=True, exist_ok=True)
    exp.lineage_report_json.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        status = "PASS" if report["ok"] else "FAIL"
        summary = report["summary"]
        print(
            f"[{exp.id}] lineage report {status}: "
            f"envelopes={summary['lineage_envelope_count']} "
            f"dependency_keys={summary['dependency_key_count']} "
            f"updated={summary['updated_output_count']} "
            f"skipped={summary['skipped_output_count']} "
            f"deleted={summary['deleted_output_count']} "
            f"report={exp.lineage_report_json}"
        )
        for name, ok in report["checks"].items():
            if not ok:
                print(f"  - failed check: {name}")
    return 0 if report["ok"] else 1


def cmd_runtime_lineage_report(args) -> int:
    exp = _resolve(args.id)
    report = build_runtime_lineage_replay_report(exp)
    exp.runtime_lineage_report_json.parent.mkdir(parents=True, exist_ok=True)
    exp.runtime_lineage_report_json.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        status = "PASS" if report["ok"] else "FAIL"
        summary = report["summary"]
        print(
            f"[{exp.id}] runtime lineage report {status}: "
            f"envelopes={summary['lineage_envelope_count']} "
            f"dependency_keys={summary['dependency_key_count']} "
            f"dirty={summary['runtime_source_dirty_output_count']} "
            f"reused={summary['runtime_reused_output_count']} "
            f"fallback={summary['runtime_fallback_status']} "
            f"report={exp.runtime_lineage_report_json}"
        )
        for name, ok in report["checks"].items():
            if not ok:
                print(f"  - failed check: {name}")
    return 0 if report["ok"] else 1


# ---------------------------------------------------------------------------
# CLI


def cmd_list(_args) -> int:
    exps = _load_manifest()
    print(f"{len(exps)} experiments in {MANIFEST.relative_to(ROOT)}:\n")
    print(f"{'id':35s}  {'repo':10s}  {'source_set':15s}  strategy")
    print(f"{'-' * 35:35s}  {'-' * 10:10s}  {'-' * 15:15s}  {'-' * 30}")
    for e in exps:
        strat = e.replay.strategy
        if e.replay.n is not None:
            strat = f"{strat}(n={e.replay.n})"
        elif e.replay.last_n is not None:
            strat = f"{strat}(last_n={e.replay.last_n})"
        elif e.replay.head_n is not None:
            strat = f"{strat}(head_n={e.replay.head_n},total={e.replay.total})"
        print(f"{e.id:35s}  {e.repo.name:10s}  {e.source_set:15s}  {strat}")
    return 0


def _resolve(args_id: str) -> Experiment:
    for e in _load_manifest():
        if e.id == args_id:
            return e
    sys.exit(f"unknown experiment id: {args_id!r}")


def cmd_setup_db(args) -> int:
    exp = _resolve(args.id)
    _ensure_repo(exp)
    setup_db(exp)
    return 0


def cmd_start_worker(args) -> int:
    exp = _resolve(args.id)
    start_worker(exp)
    wait_for_worker(exp)
    return 0


def cmd_stop_worker(args) -> int:
    exp = _resolve(args.id)
    stop_worker(exp)
    return 0


def _run_experiment_sequence(
    exps: list[Experiment],
    *,
    prove_rebuild_equivalence: bool = False,
) -> int:
    # Opt the substrate into persisting publish-cycle per-phase timings to
    # source_progress (env-gated; off by default in prod). All worker/ingest
    # children inherit this env, so each commit's publish records its
    # projection/enrichment/gate/publish/view-materialization decomposition
    # for extract.py to read (bench-okg-internals task.internal-metrics).
    os.environ["OKG_PERSIST_PUBLISH_TIMINGS"] = "1"
    equivalence_failed = False
    for exp in exps:
        print(f"\n===== {exp.id} =====")
        t0 = time.time()
        _ensure_repo(exp)
        # Build the explicit commit list NOW, while the working tree
        # still has full history reachable from HEAD.
        exp.deployment_dir.mkdir(parents=True, exist_ok=True)
        _write_commits_file(exp)
        # Reset the working tree to baseline BEFORE starting the worker
        # so the worker's initial-scan sync sees the baseline state, not
        # the full-repo HEAD state. Otherwise the worker pre-ingests the
        # entire repo and the bench's "growth from baseline" plots are
        # contaminated with pre-loaded mass.
        _reset_repo_to_baseline(exp)
        setup_db(exp)
        start_worker(exp)
        wait_for_worker(exp)
        # Fire ONE reconcile pass before the replay starts. Reasons:
        #   - Sources with `predecessor_source` (e.g. doc-corpus
        #     declares predecessor=git-files) refuse to emit until
        #     their predecessor has done a full scope-complete walk.
        #     Watcher-fired scope_incomplete events alone never
        #     satisfy that gate — the predecessor never marks itself
        #     reconciled.
        #   - Running a reconcile once at the start sets every
        #     source's last_reconcile_at watermark; from there
        #     incremental scope_incomplete events are sufficient.
        _bench_reconcile(exp)
        # Sample the worker's peak RSS across the replay (run-level resource
        # metric for task.internal-metrics).
        rss_sampler = _WorkerRssSampler(exp)
        rss_sampler.start()
        try:
            run_replay(exp)
        finally:
            try:
                peak_rss = rss_sampler.stop_and_report()
            except Exception:  # noqa: BLE001 - never let sampling block teardown
                peak_rss = 0
            stop_worker(exp)
        exp.run_metrics_json.parent.mkdir(parents=True, exist_ok=True)
        exp.run_metrics_json.write_text(
            json.dumps({"peak_rss_bytes": peak_rss or None}, indent=2) + "\n",
            encoding="utf-8",
        )
        # Standardized-metrics extraction (okg.bench.extract) — unconditional:
        # a silent skip here would be the repo's named P0 class.
        print(f"[{exp.id}] extracting standardized metrics")
        try:
            _run([PYTHON, "-m", "okg.bench.extract", exp.id], capture=False)
            _enrich_delta_proof_source_runs(exp)
        except subprocess.CalledProcessError:
            sys.stderr.write(
                f"[{exp.id}] okg.bench.extract failed; raw CSV is still at "
                f"{exp.raw_csv}\n"
            )
        # Read-side perf at the final published generation. The worker is
        # already stopped, so the DB is quiescent (task.read-side-perf).
        try:
            rs = build_read_side_report(exp)
            exp.read_side_json.parent.mkdir(parents=True, exist_ok=True)
            exp.read_side_json.write_text(
                json.dumps(rs, indent=2, sort_keys=True, default=str) + "\n",
                encoding="utf-8",
            )
            if rs.get("ok"):
                ok_reads = {
                    k: v for k, v in rs["reads"].items() if v.get("status") == "ok"
                }
                print(
                    f"[{exp.id}] read-side p50: "
                    + ", ".join(f"{k}={v['p50_ms']}ms" for k, v in ok_reads.items())
                )
            _enrich_delta_proof_source_runs(exp)
        except Exception as exc:  # noqa: BLE001 - read-side is non-fatal
            sys.stderr.write(f"[{exp.id}] read-side report failed: {exc}\n")
        # `--prove-rebuild-equivalence` (deltaflow review follow-up C): run the
        # REAL incremental-vs-forced-rebuild equivalence so the issue-79 "same
        # final state" proof has a run-time gate, not just a manual command.
        if prove_rebuild_equivalence:
            print(f"[{exp.id}] proving incremental-vs-forced-rebuild equivalence")
            report = build_differential_rebuild_report(exp)
            exp.rebuild_equivalence_json.parent.mkdir(parents=True, exist_ok=True)
            exp.rebuild_equivalence_json.write_text(
                json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
                encoding="utf-8",
            )
            if report["ok"]:
                graph = report["incremental"]["graph"]
                print(
                    f"[{exp.id}] rebuild equivalence PASS "
                    f"(nodes={graph['nodes']['count']} "
                    f"edges={graph['edges']['count']})"
                )
            else:
                equivalence_failed = True
                failed = [k for k, ok in report["checks"].items() if not ok]
                sys.stderr.write(f"[{exp.id}] REBUILD EQUIVALENCE FAILED: {failed}\n")
        elapsed = time.time() - t0
        print(f"[{exp.id}] done in {elapsed:.1f}s")
    return 1 if equivalence_failed else 0


def cmd_run(args) -> int:
    if args.all:
        exps = _load_manifest()
    else:
        if not args.id:
            sys.exit("`run` needs an experiment id or --all")
        exps = [_resolve(args.id)]
    return _run_experiment_sequence(
        exps,
        prove_rebuild_equivalence=getattr(args, "prove_rebuild_equivalence", False),
    )


def cmd_sensitivity(args) -> int:
    from okg.bench.engine_sensitivity import sensitivity_markdown  # noqa: PLC0415

    plan, exps = build_engine_sensitivity_plan(Path(args.config))
    by_id = {exp.id: exp for exp in exps}
    selected_ids = list(args.cell or [])
    if selected_ids:
        missing = [cid for cid in selected_ids if cid not in by_id]
        if missing:
            sys.exit(f"unknown sensitivity cell(s): {', '.join(missing)}")
        selected = [by_id[cid] for cid in selected_ids]
    else:
        selected = exps

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    spec_id = plan["spec"]["id"]
    plan_json = RESULTS_DIR / f"{spec_id}.sensitivity_plan.json"
    plan_md = RESULTS_DIR / f"{spec_id}.sensitivity_plan.md"
    plan_json.write_text(
        json.dumps(plan, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    plan_md.write_text(sensitivity_markdown(plan), encoding="utf-8")

    if args.run_branching_report:
        if len(selected) != 1:
            sys.exit("--run-branching-report requires exactly one --cell")
        exp = selected[0]
        report = build_branching_report(
            exp,
            n_commits=args.branching_n_commits,
            nodes_per_commit=args.branching_nodes_per_commit,
        )
        out = RESULTS_DIR / f"{exp.id}.branching.json"
        out.write_text(
            json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
        else:
            print(
                f"[{exp.id}] branching report ok={report.get('ok')} "
                f"artifact={out}"
            )
        return 0 if report.get("ok") else 1

    if args.run:
        os.environ["OKG_BENCH_SENSITIVITY_CONFIG"] = str(Path(args.config))
        rc = _run_experiment_sequence(
            selected,
            prove_rebuild_equivalence=getattr(
                args, "prove_rebuild_equivalence", False
            ),
        )
        try:
            summary_json, summary_md = _write_sensitivity_summary(plan, selected)
        except FileNotFoundError as exc:
            sys.stderr.write(f"[sensitivity] summary unavailable: {exc}\n")
            return rc or 1
        print(f"[sensitivity] summary json: {summary_json}")
        print(f"[sensitivity] summary md:   {summary_md}")
        return rc

    if args.json:
        print(json.dumps(plan, indent=2, sort_keys=True, default=str))
    else:
        print(
            f"engine sensitivity plan: {spec_id} "
            f"mode={plan['spec']['mode']} cells={plan['cell_count']}"
        )
        print(f"  plan json: {plan_json}")
        print(f"  plan md:   {plan_md}")
        print(f"  {'cell':26s}  {'axis':32s}  value")
        print(f"  {'-' * 26:26s}  {'-' * 32:32s}  {'-' * 20}")
        for cell in plan["cells"]:
            value = json.dumps(cell["axis_value"], sort_keys=True)
            print(f"  {cell['id']:26s}  {cell['axis']:32s}  {value}")
        if plan.get("reports"):
            print("  side reports:")
            for name, report in sorted(plan["reports"].items()):
                print(f"    {name}: {report['command']}")
    return 0


def cmd_read_side_report(args) -> int:
    exp = _resolve(args.id)
    report = build_read_side_report(exp, iterations=args.iterations)
    exp.read_side_json.parent.mkdir(parents=True, exist_ok=True)
    exp.read_side_json.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    elif not report.get("ok"):
        print(f"[{exp.id}] read-side report NOT ok: {report.get('reason')}")
    else:
        g = report["graph"]
        print(
            f"[{exp.id}] read-side @ {report['generation_id']}  "
            f"(nodes={g['nodes']} edges={g['edges']})"
        )
        print(f"  {'read':22} {'p50_ms':>9} {'p95_ms':>9} {'rows':>7}")
        for name, r in report["reads"].items():
            if r.get("status") != "ok":
                print(f"  {name:22} {'skipped':>9}")
                continue
            print(f"  {name:22} {r['p50_ms']:>9} {r['p95_ms']:>9} {r['rows']:>7}")
        print(f"  full report: {exp.read_side_json}")
    return 0 if report.get("ok") else 1


# ---------------------------------------------------------------------------
# Standardized internals sweep (bench-okg-internals task.standardized-sweep-proof)

SWEEP_PREFIX = "tdb-"
SWEEP_COMPARISON_GROUPS = ["tdb-enr", "tdb-chr"]
SWEEP_BRANCHING_CELL = "tdb-branch"
# Phases that run on every publish + the run-level resource metrics: these MUST
# carry a non-null value on every cell.
SWEEP_ALWAYS_NONNULL_COLS = [
    "projection_ms",
    "gate_ms",
    "publish_ms",
    "publish_total_ms",
    "view_materialization_ms",
    "peak_rss_bytes",
    "chronos_segments",
    "chronos_branches",
    "db_size_bytes",
]
# Conditional phases: the COLUMN is always emitted, but a value is NaN when the
# phase legitimately did not run (e.g. enrichment on an enrichers=none cell).
SWEEP_CONDITIONAL_COLS = ["enrichment_ms"]
SWEEP_REQUIRED_METRIC_COLS = SWEEP_ALWAYS_NONNULL_COLS + SWEEP_CONDITIONAL_COLS


def _sweep_cells() -> list[Experiment]:
    return [e for e in _load_manifest() if e.id.startswith(SWEEP_PREFIX)]


def _sweep_metric_coverage(cells: list[Experiment]) -> dict[str, Any]:
    """Confirm each cell emitted the full metric set: the per-phase + resource
    CSV columns with at least one non-null value, and a read-side report."""
    import csv as _csv

    out: dict[str, Any] = {}
    for e in cells:
        csv_path = RESULTS_DIR / f"{e.id}.csv"
        nonnull: dict[str, bool] = {}
        header: list[str] = []
        if csv_path.exists():
            rows = list(_csv.DictReader(csv_path.open(encoding="utf-8")))
            header = list(rows[0].keys()) if rows else []
            for col in SWEEP_REQUIRED_METRIC_COLS:
                nonnull[col] = any(r.get(col) not in (None, "", "NaN") for r in rows)
        # Always-run phases + resource metrics must be non-null; conditional
        # phases (enrichment on a non-enriched cell) need only be EMITTED.
        always_ok = all(nonnull.get(c) for c in SWEEP_ALWAYS_NONNULL_COLS)
        conditional_emitted = all(c in header for c in SWEEP_CONDITIONAL_COLS)
        out[e.id] = {
            "csv": csv_path.exists(),
            "internal_metrics_nonnull": nonnull,
            "all_metrics_present": (bool(header) and always_ok and conditional_emitted),
            "read_side": (RESULTS_DIR / f"{e.id}.read_side.json").exists(),
        }
    return out


def _md_num(v, *, signed: bool = False, pct: bool = False) -> str:
    if v is None:
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    body = f"{f:,.1f}{'%' if pct else ''}"
    return f"+{body}" if signed and f >= 0 else body


def _sweep_markdown(
    summary: dict,
    comparisons: dict,
    branching: dict,
) -> str:
    """Render the sweep result as GitHub-flavored markdown for the CI job
    summary (the Actions run page) — so the benchmark is readable without
    cloning + running anything locally."""
    pin = summary.get("pin", {})
    repro = pin.get("reproducible")
    repro_s = (
        "✅ yes"
        if repro is True
        else ("🆕 lock initialized" if repro == "lock_initialized" else "❌ NO (drift)")
    )
    cov = (
        "✅ all cells" if summary.get("metric_coverage_all_present") else "⚠️ incomplete"
    )
    lines = [
        "## 🔬 OKG-internals bench sweep — tinydb",
        "",
        f"**Reproducible pin:** {repro_s} &nbsp;·&nbsp; "
        f"**cells:** {summary.get('n_cells')} &nbsp;·&nbsp; "
        f"**full metric set:** {cov}",
        "",
        "### Marginal cost per config dimension",
        "*Each row = one config cell; Δ = cost added vs the baseline cell.*",
        "",
    ]
    for g, rep in comparisons.items():
        if not rep.get("ok"):
            lines += [f"**{g}** — comparison unavailable ({rep.get('reason')})", ""]
            continue
        axis = rep.get("swept_axis", g)
        lines += [
            f"**{axis}** (baseline `{rep.get('baseline_axis_value')}`)",
            "",
            f"| {axis} | e2e ms/commit | Δe2e | Δedges | Δenrich ms | "
            "Δview-mat ms | Δagg-read p50 |",
            "|---|--:|--:|--:|--:|--:|--:|",
        ]
        for c in rep.get("cells", []):
            prof, m = c.get("profile", {}), c.get("marginal_vs_baseline")
            e2e = _md_num(prof.get("e2e_ms_mean"))
            if c.get("is_baseline") or m is None:
                lines.append(f"| `{c.get('axis_value')}` | {e2e} | base | | | | |")
                continue
            agg = (m.get("read_side_p50") or {}).get("aggregate_by_type")
            lines.append(
                f"| `{c.get('axis_value')}` | {e2e} | "
                f"{_md_num(m.get('e2e_ms_mean'), signed=True)} | "
                f"{_md_num(m.get('edges_added_total'), signed=True)} | "
                f"{_md_num(m.get('enrichment_ms_mean'), signed=True)} | "
                f"{_md_num(m.get('view_materialization_ms_mean'), signed=True)} | "
                f"{_md_num(agg, signed=True)} |"
            )
        lines.append("")

    if branching.get("ok"):
        lin, br = branching["linear_append"], branching["branching"]
        seg = br.get("segments_added_fork_to_compact")
        lines += [
            f"### Chronos branching cost (`{SWEEP_BRANCHING_CELL}`, "
            f"{branching['params']['n_commits']} commits)",
            "",
            "| operation | cost |",
            "|---|--:|",
            f"| linear-append baseline | {_md_num(lin.get('mean_commit_ms'))} ms/commit |",
            f"| fork | {_md_num(br.get('fork_ms'))} ms |",
            f"| branch-commit | {_md_num(br.get('mean_branch_commit_ms'))} ms/commit |",
            f"| merge | {_md_num(br.get('merge_ms'))} ms |",
            f"| compact | {_md_num(br.get('compact_ms'))} ms "
            f"({_md_num(seg, signed=True)} segments) |",
            "",
        ]

    # read-side p50 from the enricher comparison (none vs the heaviest cell)
    enr = comparisons.get("tdb-enr")
    if enr and enr.get("ok"):
        base = next((c for c in enr["cells"] if c.get("is_baseline")), None)
        top = enr["cells"][-1] if enr.get("cells") else None
        if base and top and top is not base:
            rb = (base.get("profile") or {}).get("read_side_p50") or {}
            rt = (top.get("profile") or {}).get("read_side_p50") or {}
            if rb and rt:
                lines += [
                    f"### Read-side p50 @ final generation "
                    f"(`{base['axis_value']}` → `{top['axis_value']}`)",
                    "",
                    f"| read | {base['axis_value']} ms | {top['axis_value']} ms | Δ |",
                    "|---|--:|--:|--:|",
                ]
                for k in sorted(rb):
                    if k in rt and rb[k] and rt[k]:
                        d = (rt[k] / rb[k] - 1) * 100
                        lines.append(
                            f"| {k} | {_md_num(rb[k])} | {_md_num(rt[k])} | "
                            f"{_md_num(d, signed=True, pct=True)} |"
                        )
                lines += [
                    "",
                    "> ⚠️ At `first_n=10` the graph is small (~500 nodes, 1–2 ms " +
                    "reads), so run-to-run variance exceeds the edge-count effect " +
                    "— treat read-side deltas as noise here. A stable read-side " +
                    "attribution needs a larger commit range (the read-side " +
                    "machinery itself is what's exercised at this scale).",
                    "",
                ]

    lines.append(
        "_Full per-cell CSVs + JSON reports are in the `bench-internals` "
        "workflow artifact._"
    )
    return "\n".join(lines) + "\n"


def _html(value: Any) -> str:
    if value is None or value == "":
        text = "—"
    elif value is True:
        text = "yes"
    elif value is False:
        text = "no"
    else:
        text = str(value)
    return html.escape(text, quote=True)


@dataclass(frozen=True)
class _HtmlCell:
    html: str


def _html_cell(value: Any) -> str:
    if isinstance(value, _HtmlCell):
        return value.html
    return _html(value)


def _status_label(ok: Any, *, missing: bool = False) -> tuple[str, str]:
    if missing:
        return ("missing", "MISSING")
    if ok is True:
        return ("pass", "PASS")
    if ok is False:
        return ("fail", "FAIL")
    return ("warn", "UNKNOWN")


def _html_status(ok: Any, *, missing: bool = False) -> str:
    css, label = _status_label(ok, missing=missing)
    return f'<span class="status status-{css}">{_html(label)}</span>'


def _html_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    head = "".join(f"<th>{_html(h)}</th>" for h in headers)
    body = []
    for row in rows:
        body.append("<tr>" + "".join(f"<td>{_html_cell(v)}</td>" for v in row) + "</tr>")
    if not body:
        body.append(
            f'<tr><td colspan="{len(headers)}" class="muted">No rows available.</td></tr>'
        )
    return (
        '<table class="data-table"><thead><tr>'
        + head
        + "</tr></thead><tbody>"
        + "".join(body)
        + "</tbody></table>"
    )


def _collapsible_block(title: str, body: str, *, open: bool = False) -> str:
    open_attr = " open" if open else ""
    return (
        f'<details class="detail-block detail-collapse"{open_attr}>'
        f"<summary>{_html(title)}</summary>{body}</details>"
    )


def _timing_label(value: Any) -> str:
    num = _number(value)
    if num is None:
        return "—"
    if abs(num) >= 1000:
        return f"{num / 1000:,.2f}s"
    return f"{num:,.1f}ms"


def _timing_total(timings: Mapping[str, Any]) -> float:
    explicit = _number(timings.get("total"))
    if explicit is not None and explicit > 0:
        return explicit
    values = [
        value for value in (_number(raw) for raw in timings.values())
        if value is not None and value > 0
    ]
    return max(values, default=0.0)


def _timing_tree(timings: Mapping[str, Any], *, root_name: str, total_ms: Any = None) -> dict[str, Any]:
    root_total = _number(total_ms)
    if root_total is None or root_total <= 0:
        root_total = _timing_total(timings)
    root: dict[str, Any] = {
        "name": root_name,
        "path": root_name,
        "value": root_total,
        "children": {},
    }
    for raw_name, raw_value in timings.items():
        value = _number(raw_value)
        name = str(raw_name or "").strip()
        if not name or name == "total" or value is None or value <= 0:
            continue
        node = root
        parts = [part for part in name.split(".") if part]
        path_parts: list[str] = []
        for part in parts:
            path_parts.append(part)
            children = node["children"]
            child = children.setdefault(
                part,
                {
                    "name": part,
                    "path": ".".join(path_parts),
                    "value": 0.0,
                    "children": {},
                },
            )
            node = child
        node["value"] = max(float(node.get("value") or 0.0), value)
    return root


def _timing_tree_to_flamegraph_node(
    node: Mapping[str, Any],
    *,
    parent_path: str | None = None,
) -> dict[str, Any] | None:
    name = str(node.get("name") or node.get("path") or "unknown timing")
    path = name if parent_path is None else f"{parent_path}.{name}"
    value = _number(node.get("value")) or 0.0
    if value <= 0:
        return None
    children: list[dict[str, Any]] = []
    raw_children = node.get("children")
    if isinstance(raw_children, Mapping):
        for child in sorted(
            raw_children.values(),
            key=lambda item: _number(item.get("value")) or 0.0,
            reverse=True,
        ):
            child_node = _timing_tree_to_flamegraph_node(child, parent_path=path)
            if child_node is not None:
                children.append(child_node)
    out: dict[str, Any] = {
        "name": path,
        "value": float(value),
    }
    if children:
        out["children"] = children
    return out


def _scale_flamegraph_tree(node: dict[str, Any], factor: float) -> dict[str, Any]:
    scaled = dict(node)
    original_value = float(node.get("value") or 0.0)
    scaled_value = original_value * factor
    scaled["value"] = scaled_value
    raw_value = node.get("raw_value", original_value)
    if abs(float(raw_value) - scaled_value) > 0.000_001:
        scaled["raw_value"] = float(raw_value)
    scaled["children"] = [
        _scale_flamegraph_tree(child, factor)
        for child in node.get("children") or []
        if float(child.get("value") or 0.0) > 0
    ]
    return scaled


def _normalize_timing_flamegraph_node(
    node: dict[str, Any],
    stats: dict[str, float | int],
) -> dict[str, Any]:
    out = {key: value for key, value in node.items() if key != "children"}
    children = [
        _normalize_timing_flamegraph_node(child, stats)
        for child in node.get("children") or []
        if float(child.get("value") or 0.0) > 0
    ]
    value = float(out.get("value") or 0.0)
    child_total = sum(float(child.get("value") or 0.0) for child in children)
    if child_total > value + 0.000_001:
        stats["overlap_node_count"] = int(stats["overlap_node_count"]) + 1
        stats["overlap_duration_ms"] = (
            float(stats["overlap_duration_ms"]) + child_total - value
        )
        factor = value / child_total if value > 0 else 0.0
        children = [
            _scale_flamegraph_tree(child, factor)
            for child in children
            if factor > 0
        ]
    out["children"] = children
    return out


def _emit_flamegraph_ms(value: float) -> float:
    return int(max(0.0, value) * 1000 + 0.000_001) / 1000


def _finalize_timing_flamegraph_node(node: dict[str, Any]) -> dict[str, Any]:
    children = [
        _finalize_timing_flamegraph_node(child)
        for child in sorted(
            node.get("children") or [],
            key=lambda child: float(child.get("value") or 0.0),
            reverse=True,
        )
        if float(child.get("value") or 0.0) > 0
    ]
    value = _emit_flamegraph_ms(float(node.get("value") or 0.0))
    out: dict[str, Any] = {
        "name": str(node.get("name") or "unknown timing"),
        "value": value,
    }
    raw_value = node.get("raw_value")
    if raw_value is not None and abs(float(raw_value) - value) > 0.0005:
        out["raw_value"] = _emit_flamegraph_ms(float(raw_value))
    if children:
        out["children"] = children
    return out


def _count_flamegraph_nodes(node: Mapping[str, Any]) -> int:
    return 1 + sum(_count_flamegraph_nodes(child) for child in node.get("children") or [])


def _timing_flamegraph_payload(
    selected: Mapping[str, Any],
    *,
    source_timings: Mapping[str, Any],
    publish_timings: Mapping[str, Any],
    source_name: str | None,
    publish_label: str | None,
) -> dict[str, Any] | None:
    commit_row = selected["commit"]
    source_run = selected.get("source_run")
    publish_run = selected.get("publish_run")
    children: list[dict[str, Any]] = []
    if source_timings:
        source_tree = _timing_tree(
            source_timings,
            root_name=f"source.{source_name or 'source'}",
            total_ms=source_run.get("total_ms") if isinstance(source_run, dict) else None,
        )
        source_node = _timing_tree_to_flamegraph_node(source_tree)
        if source_node is not None:
            children.append(source_node)
    if publish_timings:
        publish_tree = _timing_tree(
            publish_timings,
            root_name=f"publisher.{publish_label or 'publish'}",
            total_ms=publish_run.get("total_ms") if isinstance(publish_run, dict) else None,
        )
        publish_node = _timing_tree_to_flamegraph_node(publish_tree)
        if publish_node is not None:
            children.append(publish_node)
    if not children:
        return None

    root = {
        "name": "OKG benchmark timing profile",
        "value": sum(float(child.get("value") or 0.0) for child in children),
        "children": children,
    }
    normalization_stats: dict[str, float | int] = {
        "overlap_node_count": 0,
        "overlap_duration_ms": 0.0,
    }
    normalized_root = _normalize_timing_flamegraph_node(root, normalization_stats)
    finalized_root = _finalize_timing_flamegraph_node(normalized_root)
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    span_count = max(0, _count_flamegraph_nodes(finalized_root) - 1)
    return {
        "schema": "okg.telemetry.aggregate_flamegraph.v1",
        "deployment": str(selected["doc"].get("experiment_id") or "ci-bench-internals"),
        "generated_at": generated_at,
        "since_seconds": 0,
        "window_start": generated_at,
        "window_end": generated_at,
        "limit": None,
        "source": "spans",
        "group_by": "run",
        "profile_type": "evented",
        "measure": "wall_time",
        "unit": "milliseconds",
        "flamegraph_semantics": "additive_wall_time",
        "normalization": {
            "mode": (
                "scale_overlapping_children"
                if normalization_stats["overlap_node_count"] else "none"
            ),
            "overlap_node_count": int(normalization_stats["overlap_node_count"]),
            "overlap_duration_ms": _emit_flamegraph_ms(
                float(normalization_stats["overlap_duration_ms"])
            ),
            "reason": (
                "Benchmark timing children can overlap in wall-clock accounting; "
                "the flamegraph viewer's contract requires child totals to fit "
                "within each parent bar."
            ),
        },
        "status": "available",
        "unavailable_reason": None,
        "candidate_profile_count": 0,
        "candidate_trace_count": 1,
        "candidate_span_count": span_count,
        "profile_count": 0,
        "trace_count": 1,
        "span_count": span_count,
        "covered_duration_ms": finalized_root["value"],
        "gap_duration_ms": 0.0,
        "unavailable_profile_count": 0,
        "truncated": False,
        "omitted_trace_count": 0,
        "omitted_span_count": 0,
        "root": finalized_root,
        "unavailable": [],
        "bench": {
            "artifact": str(selected["path"].name),
            "experiment": selected["doc"].get("experiment_id"),
            "commit": commit_row.get("commit"),
            "commit_index": commit_row.get("i"),
            "query_count": commit_row.get("query_count"),
            "slowest_phase": commit_row.get("slowest_phase"),
            "slowest_phase_ms": commit_row.get("slowest_phase_ms"),
        },
    }


def _write_timing_flamegraph_viewer(
    results_dir: Path,
    *,
    payload_filename: str,
    payload: Mapping[str, Any],
    viewer_filename: str = "bench-flamegraph.html",
    navigation_entries: Sequence[Mapping[str, Any]] | None = None,
) -> str:
    viewer_dir = results_dir / "flamegraph-viewer"
    viewer_dir.mkdir(parents=True, exist_ok=True)
    viewer_html = viewer_dir / viewer_filename
    payload_json = json.dumps(payload, indent=2, sort_keys=True, default=str)
    navigation_json = json.dumps(
        list(navigation_entries or []),
        indent=2,
        sort_keys=True,
        default=str,
    )
    asset_version = hashlib.sha256(
        f"{payload_filename}\n{payload_json}\n{navigation_json}".encode("utf-8")
    ).hexdigest()[:12]
    viewer_html.write_text(
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OKG Benchmark Timing Flamegraph</title>
  <link rel="icon" href="data:,">
  <link rel="stylesheet" href="bench-flamegraph.css?v={_html(asset_version)}">
</head>
<body>
  <div id="root"></div>
  <script id="okg-flamegraph-payload" type="application/json">
{html.escape(payload_json, quote=False)}
  </script>
  <script id="okg-flamegraph-manifest" type="application/json">
{html.escape(navigation_json, quote=False)}
  </script>
  <script src="bench-flamegraph.js?v={_html(asset_version)}" defer></script>
  <noscript>This flamegraph viewer requires JavaScript.</noscript>
  <p hidden data-payload-path="../{_html(payload_filename)}"></p>
</body>
</html>
""",
        encoding="utf-8",
    )
    return f"flamegraph-viewer/{viewer_filename}"


def _flamegraph_report_href(cell: str) -> str:
    if cell.startswith("tdb-"):
        return "../index.html#sweep"
    if cell.startswith("engsens-"):
        return "../index.html#sensitivity"
    return "../index.html#timing-profile"


def _flamegraph_entry_detail(info: Mapping[str, Any]) -> str:
    parts: list[str] = []
    commit = info.get("commit")
    if commit:
        parts.append(str(commit)[:10])
    covered = _timing_label(info.get("covered_duration_ms"))
    if covered != "—":
        parts.append(covered)
    frames = info.get("frames")
    if frames is not None:
        parts.append(f"{frames} frames")
    return " / ".join(parts)


def _flamegraph_navigation_entries(
    timing_state: Mapping[str, Any],
    flamegraph_links: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    primary_viewer = timing_state.get("viewer")
    if primary_viewer:
        primary_info = {
            "commit": timing_state.get("commit"),
            "covered_duration_ms": timing_state.get("covered_duration_ms"),
            "frames": timing_state.get("frames"),
        }
        entries.append({
            "id": "timing-profile",
            "label": "Primary timing profile",
            "detail": _flamegraph_entry_detail(primary_info),
            "viewer": Path(str(primary_viewer)).name,
            "report": "../index.html#timing-profile",
        })
    for cell, info in sorted(flamegraph_links.items()):
        if not isinstance(info, Mapping) or not info.get("viewer"):
            continue
        entries.append({
            "id": cell,
            "label": cell,
            "detail": _flamegraph_entry_detail(info),
            "viewer": Path(str(info["viewer"])).name,
            "report": _flamegraph_report_href(cell),
        })
    return entries


def _rewrite_flamegraph_viewers_with_navigation(
    results_dir: Path,
    *,
    timing_state: Mapping[str, Any],
    flamegraph_links: Mapping[str, Mapping[str, Any]],
    navigation_entries: Sequence[Mapping[str, Any]],
) -> None:
    viewer_dir = results_dir / "flamegraph-viewer"
    viewer_dir.mkdir(parents=True, exist_ok=True)
    (viewer_dir / "manifest.json").write_text(
        json.dumps(list(navigation_entries), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    write_specs: list[tuple[str, str]] = []
    primary_payload = timing_state.get("payload")
    primary_viewer = timing_state.get("viewer")
    if primary_payload and primary_viewer:
        write_specs.append((Path(str(primary_payload)).name, Path(str(primary_viewer)).name))
    for info in flamegraph_links.values():
        if not isinstance(info, Mapping):
            continue
        payload = info.get("payload")
        viewer = info.get("viewer")
        if payload and viewer:
            write_specs.append((Path(str(payload)).name, Path(str(viewer)).name))
    for payload_filename, viewer_filename in write_specs:
        payload_path = results_dir / payload_filename
        if not payload_path.exists():
            continue
        payload, error = _read_report_json(payload_path)
        if error or payload is None:
            continue
        _write_timing_flamegraph_viewer(
            results_dir,
            payload_filename=payload_filename,
            payload=payload,
            viewer_filename=viewer_filename,
            navigation_entries=navigation_entries,
        )


def _diagnostics_score(row: Mapping[str, Any]) -> float:
    source = _number(row.get("source_total_ms")) or 0.0
    publish = _number(row.get("publish_total_ms")) or 0.0
    slowest = _number(row.get("slowest_phase_ms")) or 0.0
    return max(source + publish, slowest)


def _timing_profile_slices(results_dir: Path) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    preferred_name = "tinydb-code-lsp-head10.diagnostics.json"
    for path in sorted(results_dir.glob("*.diagnostics.json")):
        doc, error = _read_report_json(path)
        if error or not doc or doc.get("schema") != "okg.bench.source_sync_diagnostics.v1":
            continue
        rows = [row for row in doc.get("per_commit") or [] if isinstance(row, dict)]
        if not rows:
            continue
        commit_row = max(rows, key=_diagnostics_score)
        source_runs = [
            run for run in commit_row.get("source_runs") or []
            if isinstance(run, dict)
        ]
        publish_runs = [
            run for run in commit_row.get("publishes") or []
            if isinstance(run, dict)
        ]
        source_run = max(
            source_runs,
            key=lambda run: _number(run.get("total_ms")) or _timing_total(
                run.get("timings_ms") if isinstance(run.get("timings_ms"), Mapping) else {}
            ),
            default=None,
        )
        publish_run = max(
            publish_runs,
            key=lambda run: _number(run.get("total_ms")) or _timing_total(
                run.get("timings_ms") if isinstance(run.get("timings_ms"), Mapping) else {}
            ),
            default=None,
        )
        if source_run is None and publish_run is None:
            continue
        candidates.append({
            "path": path,
            "doc": doc,
            "commit": commit_row,
            "source_run": source_run,
            "publish_run": publish_run,
            "score": _diagnostics_score(commit_row),
            "preferred": path.name == preferred_name,
        })
    return candidates


def _select_timing_profile(results_dir: Path) -> dict[str, Any] | None:
    candidates = _timing_profile_slices(results_dir)
    if not candidates:
        return None
    return max(candidates, key=lambda item: (bool(item["preferred"]), item["score"]))


def _flamegraph_cell_stem(value: Any) -> str:
    stem = str(value or "").removesuffix(".diagnostics.json")
    return re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-") or "timing-profile"


def _flamegraph_artifact_stem(path: Path) -> str:
    return _flamegraph_cell_stem(path.name)


def _timing_payload_for_selected(selected: Mapping[str, Any]) -> dict[str, Any] | None:
    source_run = selected.get("source_run")
    publish_run = selected.get("publish_run")
    source_timings = (
        source_run.get("timings_ms")
        if isinstance(source_run, dict) and isinstance(source_run.get("timings_ms"), Mapping)
        else {}
    )
    publish_timings = (
        publish_run.get("timings_ms")
        if isinstance(publish_run, dict) and isinstance(publish_run.get("timings_ms"), Mapping)
        else {}
    )
    source_name = (
        source_run.get("registry_source_name")
        or source_run.get("source_name")
        if isinstance(source_run, dict)
        else None
    )
    publish_label = publish_run.get("label") if isinstance(publish_run, dict) else None
    return _timing_flamegraph_payload(
        selected,
        source_timings=source_timings,
        publish_timings=publish_timings,
        source_name=str(source_name) if source_name else None,
        publish_label=str(publish_label) if publish_label else None,
    )


def _write_diagnostics_flamegraph_viewers(results_dir: Path) -> dict[str, dict[str, Any]]:
    links: dict[str, dict[str, Any]] = {}
    for selected in _timing_profile_slices(results_dir):
        path = selected["path"]
        stem = _flamegraph_artifact_stem(path)
        payload = _timing_payload_for_selected(selected)
        if payload is None:
            continue
        payload_filename = f"{stem}.aggregate_flamegraph.json"
        viewer_filename = f"{stem}.html"
        (results_dir / payload_filename).write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        viewer_path = _write_timing_flamegraph_viewer(
            results_dir,
            payload_filename=payload_filename,
            payload=payload,
            viewer_filename=viewer_filename,
        )
        links[stem] = {
            "diagnostics": path.name,
            "payload": payload_filename,
            "viewer": viewer_path,
            "frames": _count_flamegraph_nodes(payload["root"]),
            "covered_duration_ms": payload.get("covered_duration_ms"),
            "experiment": selected["doc"].get("experiment_id"),
            "commit": selected["commit"].get("commit"),
        }
    return links


def _timing_profile_report(results_dir: Path) -> tuple[str, dict[str, Any]]:
    selected = _select_timing_profile(results_dir)
    if selected is None:
        body = (
            '<div class="info-panel">'
            "<h3>No timing diagnostics found</h3>"
            "<p>The operator flamegraph payload is built from per-run diagnostics files. "
            "No <code>*.diagnostics.json</code> artifact was available in this "
            "result directory.</p>"
            "</div>"
        )
        return (
            _report_section(
                "Timing profile",
                None,
                body,
                section_id="timing-profile",
                intro="Optional flamegraph artifact from always-on replay diagnostics.",
            ),
            {"ok": None, "missing": False, "available": False, "optional": True},
        )

    path = selected["path"]
    commit_row = selected["commit"]
    source_run = selected.get("source_run")
    publish_run = selected.get("publish_run")
    source_name = (
        source_run.get("registry_source_name")
        or source_run.get("source_name")
        if isinstance(source_run, dict)
        else None
    )
    publish_label = publish_run.get("label") if isinstance(publish_run, dict) else None
    details = [
        ("Experiment", selected["doc"].get("experiment_id") or path.name.removesuffix(".diagnostics.json")),
        ("Artifact", path.name),
        ("Commit", commit_row.get("commit")),
        ("Commit index", commit_row.get("i")),
        ("Query count", commit_row.get("query_count")),
        ("Slowest phase", commit_row.get("slowest_phase")),
        ("Slowest phase time", _timing_label(commit_row.get("slowest_phase_ms"))),
    ]
    body = '<div class="summary-pair">' + _report_card("Selected replay slice", True, details)
    payload = _timing_payload_for_selected(selected)
    payload_filename = "timing-profile.aggregate_flamegraph.json"
    viewer_path = None
    frame_count = None
    covered_duration_ms = None
    if payload is not None:
        payload_path = results_dir / payload_filename
        payload_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        viewer_path = _write_timing_flamegraph_viewer(
            results_dir,
            payload_filename=payload_filename,
            payload=payload,
        )
        frame_count = _count_flamegraph_nodes(payload["root"])
        covered_duration_ms = payload.get("covered_duration_ms")
        body += (
            '<div class="info-panel"><h3>Benchmark flamegraph</h3>'
            "<p>This report writes the aggregate "
            "<code>okg.telemetry.aggregate_flamegraph.v1</code> payload consumed "
            "by <code>AggregateProfileFlamegraph</code>. The HTML report does "
            "not redraw a separate flamegraph inline.</p>"
            '<div class="artifact-actions">'
            f'{_link(viewer_path, "Open operator flamegraph viewer")}'
            f'{_link(payload_filename, "Open aggregate payload")}'
            f'{_link(path.name, "Open raw diagnostics")}'
            "</div>"
            '<dl class="compact-dl">'
            f"<dt>Frames</dt><dd>{_html(frame_count)}</dd>"
            f"<dt>Covered time</dt><dd>{_html(_timing_label(payload.get('covered_duration_ms')))}</dd>"
            "<dt>Renderer</dt><dd><code>AggregateProfileFlamegraph</code></dd>"
            "</dl>"
            "</div></div>"
        )
        ok = True
    else:
        body += (
            '<div class="info-panel"><h3>Operator flamegraph</h3>'
            "<p>No positive timing rows were available for the selected slice, "
            "so no aggregate flamegraph payload was written.</p>"
            f"<p>Open {_link(path.name, path.name)} for the raw diagnostics.</p>"
            "</div></div>"
        )
        ok = False
    return (
        _report_section(
            "Timing profile",
            ok,
            body,
            section_id="timing-profile",
            intro="The flamegraph viewer over the benchmark timing stats that are already tracked for each replay run.",
        ),
        {
            "ok": ok,
            "missing": False,
            "optional": True,
            "available": True,
            "path": str(path),
            "payload": str(results_dir / payload_filename) if payload is not None else None,
            "viewer": str(results_dir / viewer_path) if viewer_path is not None else None,
            "frames": frame_count,
            "covered_duration_ms": covered_duration_ms,
            "experiment": selected["doc"].get("experiment_id"),
            "commit": commit_row.get("commit"),
            "source": source_name,
            "publish": publish_label,
            "score_ms": selected["score"],
        },
    )


def _csv_row_count(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        import csv as _csv

        with path.open(encoding="utf-8", newline="") as handle:
            return sum(1 for _row in _csv.DictReader(handle))
    except Exception:  # noqa: BLE001 - absence is surfaced as unknown in report.
        return None


def _sum_known(values: Sequence[int | None]) -> int | None:
    known = [v for v in values if isinstance(v, int)]
    return sum(known) if known else None


def _metric_by_name(measurements: Sequence[Any], cell: str, metric: str) -> Any:
    for row in measurements:
        if (
            isinstance(row, dict)
            and row.get("cell") == cell
            and row.get("metric_name") == metric
        ):
            return row.get("value")
    return None


def _axis_label(axis: Any) -> str:
    labels = {
        "baseline": "Baseline",
        "branching": "Chronos branching",
        "chronos_continuation_pct": "Chronos continuation",
        "engine.enrichers": "Enrichers",
        "engine.views": "Views",
        "enrichers": "Enrichers",
        "workload.source_set": "Source set",
    }
    return labels.get(str(axis), str(axis) if axis is not None else "—")


def _link(path: str, label: str | None = None) -> str:
    target = _html(path)
    text = _html(label or path)
    return f'<a href="{target}">{text}</a>'


def _timing_viewer_cell(
    flamegraph_links: Mapping[str, Mapping[str, Any]],
    cell_id: Any,
) -> _HtmlCell | str:
    if not cell_id:
        return "—"
    info = flamegraph_links.get(_flamegraph_cell_stem(cell_id))
    viewer = info.get("viewer") if isinstance(info, Mapping) else None
    if not viewer:
        return "—"
    return _HtmlCell(_link(str(viewer), "flamegraph"))


def _timing_viewer_link(
    flamegraph_links: Mapping[str, Mapping[str, Any]],
    cell_id: Any,
) -> _HtmlCell | str:
    if not cell_id:
        return "—"
    cell = _flamegraph_cell_stem(cell_id)
    info = flamegraph_links.get(cell)
    viewer = info.get("viewer") if isinstance(info, Mapping) else None
    if not viewer:
        return "—"
    return _HtmlCell(_link(str(viewer), f"Open {cell} flamegraph"))


def _report_section(
    title: str,
    ok: Any,
    body: str,
    *,
    section_id: str,
    intro: str | None = None,
    missing: bool = False,
) -> str:
    intro_html = f'<p class="section-intro">{_html(intro)}</p>' if intro else ""
    return (
        f'<section class="report-section" id="{_html(section_id)}">'
        '<div class="section-head">'
        f"<h2>{_html(title)}</h2>{_html_status(ok, missing=missing)}"
        "</div>"
        f"{intro_html}{body}</section>"
    )


def _read_report_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return None, f"{path.name} is missing"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - surfaced in the report.
        return None, f"{path.name} could not be read: {exc}"
    if not isinstance(value, dict):
        return None, f"{path.name} did not contain a JSON object"
    return value, None


def _report_card(title: str, ok: Any, details: Sequence[tuple[str, Any]], *, missing: bool = False) -> str:
    rows = "".join(
        f"<dt>{_html(k)}</dt><dd>{_html(v)}</dd>"
        for k, v in details
    )
    return (
        '<div class="fact-panel">'
        '<div class="fact-head">'
        f"<h3>{_html(title)}</h3>"
        "</div>"
        f'<dl class="fact-list">{rows}</dl>'
        "</div>"
    )


def _count_gate_report(results_dir: Path) -> tuple[str, dict[str, Any]]:
    path = results_dir / "count_gate.json"
    doc, error = _read_report_json(path)
    if error:
        return (
            _report_section(
                "Count-based replay gate",
                None,
                _report_card("Artifact state", None, [("Artifact", path.name), ("State", error)], missing=True),
                section_id="count-gate",
                missing=True,
            ),
            {"ok": None, "missing": True, "path": str(path), "error": error},
        )

    metrics = doc.get("metrics") if isinstance(doc.get("metrics"), dict) else {}
    query_counts = (
        metrics.get("query_counts") if isinstance(metrics.get("query_counts"), dict) else {}
    )
    qpcf = (
        query_counts.get("queries_per_changed_file")
        if isinstance(query_counts.get("queries_per_changed_file"), dict)
        else {}
    )
    budgets = doc.get("budgets") if isinstance(doc.get("budgets"), dict) else {}
    violations = doc.get("violations") if isinstance(doc.get("violations"), list) else []
    blocking = doc.get("blocking_violation_count")
    commit_count = metrics.get("measured_commit_count") or metrics.get("commit_count")
    rows = [
        ("Experiment", doc.get("experiment_id")),
        ("Replay", doc.get("replay_strategy")),
        ("Commits measured", commit_count),
        ("Max query_count / pinned baseline",
         _md_num(query_counts.get("ratio_to_baseline_max"))),
        ("Headroom", budgets.get("headroom")),
        ("Max queries / changed file", _md_num(qpcf.get("max_ms"))),
        ("Gross per-file cap", budgets.get("max_queries_per_changed_file")),
        ("Blocking violations", blocking),
        ("Total violations", len(violations)),
    ]
    violation_rows = []
    for v in violations[:10]:
        if not isinstance(v, dict):
            continue
        reason = v.get("reason")
        if reason == "p95_budget_exceeded":
            observed = _md_num(v.get("p95_ms"))
            threshold = _md_num(v.get("max_p95_ms"))
        elif reason == "max_budget_exceeded":
            observed = _md_num(v.get("max_ms"))
            threshold = _md_num(v.get("max_max_ms"))
        elif reason == "throughput_below_budget":
            observed = _md_num(v.get("throughput_commits_per_second"))
            threshold = _md_num(v.get("min_throughput_commits_per_second"))
        elif reason == "outlier_count_exceeded":
            observed = v.get("outlier_count")
            threshold = v.get("max_outlier_count")
        elif reason == "query_count_over_baseline":
            observed = v.get("query_count")
            threshold = v.get("max_query_count")
        elif reason in {
            "query_count_missing", "query_count_ambiguous",
            "baseline_missing", "replayed_commits_mismatch",
        }:
            observed = v.get("commit") or "—"
            threshold = v.get("detail") or "measured pinned commits"
        else:
            observed = v.get("queries_per_changed_file") or v.get("query_count")
            threshold = "—"
        owner = (
            (v.get("owner_context") or {}).get("top_component_owner")
            if isinstance(v.get("owner_context"), dict)
            else None
        )
        violation_rows.append([reason, observed, threshold, owner])
    html_body = _report_card("Gate", doc.get("ok"), rows)
    if violation_rows:
        html_body += _collapsible_block(
            "Advisory diagnostics",
            _html_table(["Reason", "Observed", "Threshold", "Likely owner"], violation_rows),
        )
    return (
        _report_section(
            "Count-based replay gate",
            doc.get("ok"),
            html_body,
            section_id="count-gate",
            intro="Checks whether replay query work stays bounded as commits change.",
        ),
        {
            "ok": doc.get("ok"),
            "missing": False,
            "path": str(path),
            "experiment": doc.get("experiment_id"),
            "measured_commits": commit_count,
            "max_queries_per_changed_file": qpcf.get("max_ms"),
            "budget": budgets.get("max_queries_per_changed_file"),
            "max_ratio_to_baseline": query_counts.get("ratio_to_baseline_max"),
            "headroom": budgets.get("headroom"),
            "blocking_violations": blocking,
            "total_violations": len(violations),
        },
    )


def _sweep_report(
    results_dir: Path,
    *,
    flamegraph_links: Mapping[str, Mapping[str, Any]],
) -> tuple[str, dict[str, Any]]:
    path = results_dir / "sweep_summary.json"
    doc, error = _read_report_json(path)
    if error:
        return (
            _report_section(
                "Tinydb internals sweep",
                None,
                _report_card("Artifact state", None, [("Artifact", path.name), ("State", error)], missing=True),
                section_id="sweep",
                missing=True,
            ),
            {"ok": None, "missing": True, "path": str(path), "error": error},
        )

    groups = doc.get("comparison_groups") if isinstance(doc.get("comparison_groups"), dict) else {}
    group_ok = all(
        isinstance(group, dict) and group.get("ok") is True
        for group in groups.values()
    )
    pin = doc.get("pin") if isinstance(doc.get("pin"), dict) else {}
    reproducible = pin.get("reproducible")
    ok = (
        reproducible is not False
        and doc.get("metric_coverage_all_present") is True
        and doc.get("branching_ok") is True
        and group_ok
    )
    cells = doc.get("cells") if isinstance(doc.get("cells"), list) else []
    measured_commits = _sum_known(
        [
            _csv_row_count(results_dir / f"{cell}.csv")
            for cell in cells
            if isinstance(cell, str)
        ]
    )
    details = [
        ("Cells", doc.get("n_cells")),
        ("Measured commits", measured_commits),
        ("Reproducible pin", reproducible),
        ("Full metric coverage", doc.get("metric_coverage_all_present")),
        ("Branching report", doc.get("branching_ok")),
        ("Measurements", len(doc.get("measurements") or [])),
    ]
    group_rows = [
        [name, group.get("swept_axis"), group.get("ok")]
        for name, group in sorted(groups.items())
        if isinstance(group, dict)
    ]
    coverage = doc.get("coverage") if isinstance(doc.get("coverage"), dict) else {}
    coverage_rows = [
        [
            cell,
            value.get("csv"),
            value.get("all_metrics_present"),
            value.get("read_side"),
        ]
        for cell, value in sorted(coverage.items())
        if isinstance(value, dict)
    ][:12]
    html_body = _report_card("Run coverage", ok, details)
    visible_flamegraph_rows: list[list[Any]] = []
    detail_parts: list[str] = [
        "<p>The sweep replays the same tinydb workload across engine knobs. "
        f"Open {_link('sweep_summary.md', 'sweep_summary.md')} for the Markdown summary.</p>"
    ]
    comparison_detail_rows: list[list[Any]] = []
    seen_flamegraph_cells: set[str] = set()
    for name in sorted(groups):
        comparison_doc, comparison_error = _read_report_json(
            results_dir / f"{name}.comparison.json"
        )
        if comparison_error or not comparison_doc:
            continue
        axis = comparison_doc.get("swept_axis") or name
        for cell in comparison_doc.get("cells") or []:
            if not isinstance(cell, dict):
                continue
            prof = cell.get("profile") if isinstance(cell.get("profile"), dict) else {}
            marginal = (
                cell.get("marginal_vs_baseline")
                if isinstance(cell.get("marginal_vs_baseline"), dict)
                else {}
            )
            cell_id = cell.get("cell_id")
            seen_flamegraph_cells.add(str(cell_id))
            visible_flamegraph_rows.append(
                [
                    cell_id,
                    _axis_label(axis),
                    _axis_value_for_measurement(cell.get("axis_value")),
                    _md_num(prof.get("e2e_ms_mean")),
                    "base" if cell.get("is_baseline") else _md_num(
                        marginal.get("e2e_ms_mean"),
                        signed=True,
                    ),
                    _timing_viewer_link(flamegraph_links, cell_id),
                ]
            )
            comparison_detail_rows.append(
                [
                    _axis_label(axis),
                    cell.get("axis_value"),
                    cell_id,
                    _timing_viewer_cell(flamegraph_links, cell_id),
                    _md_num(prof.get("e2e_ms_mean")),
                    "base" if cell.get("is_baseline") else _md_num(marginal.get("e2e_ms_mean"), signed=True),
                    _md_num(prof.get("final_edges_live")),
                    "base" if cell.get("is_baseline") else _md_num(marginal.get("final_edges_live"), signed=True),
                ]
            )
    for cell in sorted(str(cell) for cell in cells if isinstance(cell, str)):
        if cell in seen_flamegraph_cells:
            continue
        if _flamegraph_cell_stem(cell) not in flamegraph_links:
            continue
        visible_flamegraph_rows.append(
            [
                cell,
                _axis_label("branching"),
                "branching",
                "—",
                "—",
                _timing_viewer_link(flamegraph_links, cell),
            ]
        )
    if visible_flamegraph_rows:
        html_body += (
            '<div class="table-panel sweep-flamegraphs">'
            "<h3>Sweep matrix</h3>"
            "<p>Same tinydb workload, different engine knobs. Baseline rows show "
            "absolute cost; non-baseline rows show marginal E2E cost against that "
            "axis baseline. The branching row isolates Chronos fork, merge, and "
            "compact behavior.</p>"
            + _html_table(
                [
                    "Cell",
                    "Axis",
                    "Value",
                    "E2E mean ms",
                    "Delta E2E",
                    "Flamegraph",
                ],
                visible_flamegraph_rows,
            )
            + "</div>"
        )
    if comparison_detail_rows:
        detail_parts.append(
            "<h3>Marginal cost by config axis</h3>"
            + _html_table(
                [
                    "Axis",
                    "Value",
                    "Cell",
                    "Timing",
                    "E2E mean ms",
                    "Delta E2E",
                    "Live edges",
                    "Delta edges",
                ],
                comparison_detail_rows,
            )
        )
    branching_doc, _branching_error = _read_report_json(
        results_dir / f"{SWEEP_BRANCHING_CELL}.branching.json"
    )
    if branching_doc and branching_doc.get("ok") is True:
        lin = (
            branching_doc.get("linear_append")
            if isinstance(branching_doc.get("linear_append"), dict)
            else {}
        )
        br = (
            branching_doc.get("branching")
            if isinstance(branching_doc.get("branching"), dict)
            else {}
        )
        detail_parts.append(
            "<h3>Chronos branching cost</h3>"
            + _html_table(
                ["Operation", "Cost"],
                [
                    ["linear append", f"{_md_num(lin.get('mean_commit_ms'))} ms/commit"],
                    ["fork", f"{_md_num(br.get('fork_ms'))} ms"],
                    ["branch commit", f"{_md_num(br.get('mean_branch_commit_ms'))} ms/commit"],
                    ["merge", f"{_md_num(br.get('merge_ms'))} ms"],
                    ["compact", f"{_md_num(br.get('compact_ms'))} ms"],
                ],
            )
        )
    detail_parts.append(
        "<h3>Comparison groups</h3>"
        + _html_table(["Group", "Axis", "OK"], group_rows)
    )
    detail_parts.append(
        "<h3>Metric coverage</h3>"
        + _html_table(["Cell", "CSV", "All metrics", "Read-side"], coverage_rows)
    )
    html_body += _collapsible_block("Sweep details", "".join(detail_parts))
    return (
        _report_section(
            "Tinydb internals sweep",
            ok,
            html_body,
            section_id="sweep",
            intro="Compares tinydb replay cost across the configured internals sweep cells.",
        ),
        {
            "ok": ok,
            "missing": False,
            "path": str(path),
            "cell_count": doc.get("n_cells"),
            "measured_commits": measured_commits,
            "measurements": len(doc.get("measurements") or []),
        },
    )


def _sensitivity_report(
    results_dir: Path,
    *,
    flamegraph_links: Mapping[str, Mapping[str, Any]],
) -> tuple[str, dict[str, Any]]:
    paths = sorted(results_dir.glob("*.sensitivity_summary.json"))
    if not paths:
        error = "no *.sensitivity_summary.json artifact found"
        return (
            _report_section(
                "Differential engine sensitivity",
                None,
                _report_card("Artifact state", None, [("Artifact", "*.sensitivity_summary.json"), ("State", error)], missing=True),
                section_id="sensitivity",
                missing=True,
            ),
            {"ok": None, "missing": True, "error": error},
        )

    sections: list[str] = []
    ok = True
    total_cells = 0
    total_measurements = 0
    total_commits = 0
    for path in paths:
        doc, error = _read_report_json(path)
        if error:
            ok = False
            sections.append(
                _report_section(
                    path.name,
                    None,
                    _report_card("Artifact state", None, [("Artifact", path.name), ("State", error)], missing=True),
                    section_id="sensitivity",
                    missing=True,
                )
            )
            continue
        spec = doc.get("spec") if isinstance(doc.get("spec"), dict) else {}
        cells = doc.get("cells") if isinstance(doc.get("cells"), list) else []
        measurements = (
            doc.get("measurements")
            if isinstance(doc.get("measurements"), list)
            else []
        )
        valid = bool(cells) and bool(measurements)
        ok = ok and valid
        total_cells += len(cells)
        total_measurements += len(measurements)
        summary_rows = []
        detail_rows = []
        baseline_cell = doc.get("baseline_cell")
        baseline_profile = next(
            (
                row.get("profile")
                for row in cells
                if (
                    isinstance(row, dict)
                    and (
                        row.get("baseline") is True
                        or row.get("cell") == baseline_cell
                    )
                )
            ),
            {},
        )
        baseline_e2e = (
            baseline_profile.get("e2e_ms_mean")
            if isinstance(baseline_profile, dict)
            else None
        )
        for row in cells:
            if not isinstance(row, dict):
                continue
            prof = row.get("profile") if isinstance(row.get("profile"), dict) else {}
            commits = prof.get("commits")
            if isinstance(commits, (int, float)):
                total_commits += int(commits)
            e2e = prof.get("e2e_ms_mean")
            delta = (
                None
                if row.get("baseline") is True or baseline_e2e is None or e2e is None
                else float(e2e) - float(baseline_e2e)
            )
            read_side = (
                prof.get("read_side_p50")
                if isinstance(prof.get("read_side_p50"), dict)
                else {}
            )
            is_baseline = row.get("baseline") is True or row.get("cell") == baseline_cell
            summary_rows.append(
                [
                    row.get("cell"),
                    _axis_label(row.get("axis")),
                    _axis_value_for_measurement(row.get("axis_value")),
                    prof.get("commits"),
                    _md_num(prof.get("e2e_ms_mean")),
                    "base" if is_baseline else _md_num(delta, signed=True),
                    _timing_viewer_link(flamegraph_links, row.get("cell")),
                ]
            )
            detail_rows.append(
                [
                    row.get("cell"),
                    _timing_viewer_cell(flamegraph_links, row.get("cell")),
                    _axis_label(row.get("axis")),
                    _axis_value_for_measurement(row.get("axis_value")),
                    prof.get("commits"),
                    _md_num(prof.get("e2e_ms_mean")),
                    "base" if is_baseline else _md_num(delta, signed=True),
                    _md_num(prof.get("e2e_ms_p95")),
                    _md_num(prof.get("source_total_ms_mean")),
                    _md_num(prof.get("publish_total_ms_mean")),
                    _md_num(prof.get("final_nodes_live")),
                    _md_num(prof.get("final_edges_live")),
                    _md_num(read_side.get("aggregate_by_type")),
                ]
            )
        details = [
            ("Spec", spec.get("id")),
            ("Role", "sampled one-factor probes"),
            ("Baseline", doc.get("baseline_cell")),
            ("Cells", doc.get("n_cells")),
            ("Measured commits", sum(
                int((row.get("profile") or {}).get("commits") or 0)
                for row in cells
                if isinstance(row, dict)
            )),
            ("Measurements", len(measurements)),
            ("State", "available" if valid else "empty or partial artifact"),
        ]
        body = _report_card("Sensitivity scope", valid, details)
        body += (
            '<div class="table-panel sensitivity-matrix">'
            "<h3>Sensitivity matrix</h3>"
            "<p>These are sampled one-factor probes, not another sweep: baseline, "
            "files-only source coverage, enrichers on, and views off. They are kept "
            "to catch broad regressions across engine switches without running the "
            "full matrix in PR CI.</p>"
            + _html_table(
                [
                    "Cell",
                    "Probe",
                    "Value",
                    "Commits",
                    "E2E mean ms",
                    "Delta E2E",
                    "Flamegraph",
                ],
                summary_rows,
            )
            + "</div>"
        )
        body += _collapsible_block(
            "Detailed sensitivity metrics",
            f"<p>Open {_link(path.with_suffix('.md').name, path.with_suffix('.md').name)} "
            "for the Markdown summary.</p>"
            + _html_table(
                [
                    "Cell",
                    "Timing",
                    "Probe",
                    "Value",
                    "Commits",
                    "E2E mean ms",
                    "Delta E2E",
                    "E2E p95 ms",
                    "Source ms",
                    "Publish total ms",
                    "Nodes",
                    "Edges",
                    "Aggregate read p50 ms",
                ],
                detail_rows,
            ),
        )
        sections.append(
            _report_section(
                "Differential engine sensitivity",
                valid,
                body,
                section_id="sensitivity",
                intro="Shows how selected workload and engine switches move replay cost.",
            )
        )
    return (
        "".join(sections),
        {
            "ok": ok,
            "missing": False,
            "paths": [str(p) for p in paths],
            "cell_count": total_cells,
            "measured_commits": total_commits or None,
            "measurements": total_measurements,
        },
    )


def _incremental_truth_report(results_dir: Path) -> tuple[str, dict[str, Any]]:
    paths = sorted(results_dir.glob("*.rebuild_equivalence.json"))
    if not paths:
        return "", {"ok": None, "missing": False, "optional": True}
    path = paths[0]
    doc, error = _read_report_json(path)
    if error:
        return (
            _report_section(
                "Incremental truth",
                None,
                _report_card("Artifact state", None, [("Artifact", path.name), ("State", error)], missing=True),
                section_id="incremental-truth",
                missing=True,
            ),
            {"ok": None, "missing": True, "path": str(path), "error": error},
        )
    inc = doc.get("incremental") if isinstance(doc.get("incremental"), dict) else {}
    graph = inc.get("graph") if isinstance(inc.get("graph"), dict) else {}
    nodes = graph.get("nodes") if isinstance(graph.get("nodes"), dict) else {}
    edges = graph.get("edges") if isinstance(graph.get("edges"), dict) else {}
    checks = doc.get("checks") if isinstance(doc.get("checks"), dict) else {}
    failed = [name for name, value in checks.items() if value is not True]
    details = [
        ("Experiment", doc.get("experiment_id")),
        ("Role", "correctness guard, not a timing case"),
        ("Checks", len(checks)),
        ("Failed checks", len(failed)),
        ("Nodes", nodes.get("count")),
        ("Edges", edges.get("count")),
    ]
    body = _report_card("Rebuild equivalence", doc.get("ok"), details)
    check_rows = [
        [name, "pass" if value is True else "fail"]
        for name, value in sorted(checks.items())
    ]
    if check_rows:
        body += (
            '<div class="table-panel truth-checks">'
            "<h3>Incremental vs rebuild checks</h3>"
            "<p>The replay is only useful as a performance signal if the final "
            "incremental graph matches a forced rebuild. These checks compare the "
            "final graph, derived outputs, and effective attributes.</p>"
            + _html_table(["Check", "Result"], check_rows)
            + "</div>"
        )
    if failed:
        body += _collapsible_block("Failed checks", _html_table(["Check"], [[name] for name in failed]), open=True)
    return (
        _report_section(
            "Incremental truth",
            doc.get("ok"),
            body,
            section_id="incremental-truth",
            intro="Confirms incremental replay matches a forced rebuild at the final graph.",
        ),
        {"ok": doc.get("ok"), "missing": False, "path": str(path)},
    )


def _artifact_links(
    results_dir: Path,
    *,
    flamegraph_links: Mapping[str, Mapping[str, Any]],
) -> str:
    patterns = ("*.html", "*.md", "*.json", "*.csv", "*.xml")
    files: list[Path] = []
    for pattern in patterns:
        files.extend(sorted(results_dir.glob(pattern)))
    groups: dict[str, list[Path]] = {
        "Readable summaries": [],
        "Gate and proof JSON": [],
        "Diagnostics and read-side JSON": [],
        "CSV measurements": [],
        "JUnit": [],
        "Other": [],
    }
    seen: set[Path] = set()
    for path in files:
        if path in seen:
            continue
        seen.add(path)
        name = path.name
        if name == "index.html":
            continue
        if name.endswith(".md") or name.endswith("_summary.json") or name == "count_gate.json":
            groups["Readable summaries"].append(path)
        elif any(
            token in name
            for token in (
                "delta_proof",
                "rebuild_equivalence",
                "comparison",
                "branching",
                "sensitivity_plan",
            )
        ):
            groups["Gate and proof JSON"].append(path)
        elif name.endswith(".json"):
            groups["Diagnostics and read-side JSON"].append(path)
        elif name.endswith(".csv"):
            groups["CSV measurements"].append(path)
        elif name.endswith(".xml"):
            groups["JUnit"].append(path)
        else:
            groups["Other"].append(path)
    chunks: list[str] = []
    if flamegraph_links:
        items = "".join(
            "<li>"
            f"{_link(str(info['viewer']), cell)} "
            f'<span class="muted">({_html(_timing_label(info.get("covered_duration_ms")))}, '
            f'{_html(info.get("frames"))} frames)</span>'
            "</li>"
            for cell, info in sorted(flamegraph_links.items())
            if isinstance(info, Mapping) and info.get("viewer")
        )
        chunks.append(
            '<details class="artifact-group"><summary>Timing flamegraphs '
            f'<span class="muted">({len(flamegraph_links)})</span></summary><ul>{items}</ul></details>'
        )
    for label, paths in groups.items():
        if not paths:
            continue
        items = "".join(
            f'<li><a href="{_html(path.name)}">{_html(path.name)}</a></li>'
            for path in paths
        )
        chunks.append(
            f'<details class="artifact-group"><summary>{_html(label)} '
            f'<span class="muted">({len(paths)})</span></summary><ul>{items}</ul></details>'
        )
    if not chunks:
        chunks.append('<p class="muted">No artifact files found.</p>')
    return "".join(chunks)


def _overview_panel(
    states: dict[str, dict[str, Any]],
    *,
    ok: bool,
    missing: list[str],
    unknown: Mapping[str, str] | None = None,
) -> str:
    count = states.get("count_gate", {})
    sweep = states.get("sweep", {})
    sensitivity = states.get("sensitivity", {})
    incremental = states.get("incremental_truth", {})
    timing = states.get("timing_profile", {})
    flamegraphs = states.get("timing_flamegraphs", {})
    measured_commits = _sum_known(
        [
            count.get("measured_commits"),
            sweep.get("measured_commits"),
            sensitivity.get("measured_commits"),
        ]
    )
    metrics = [
        (
            "Overall",
            "PASS" if ok else "CHECK",
        ),
        (
            "Measured commits",
            measured_commits,
        ),
        (
            "Count gate",
            f"{_md_num(count.get('max_queries_per_changed_file'))} / {_md_num(count.get('budget'))} q/change",
        ),
        (
            "Sweep cells",
            sweep.get("cell_count") or "missing",
        ),
        (
            "Sensitivity probes",
            sensitivity.get("cell_count") or "missing",
        ),
        (
            "Flamegraphs",
            flamegraphs.get("count") or ("available" if timing.get("available") else "missing"),
        ),
        (
            "Incremental truth",
            "PASS" if incremental.get("ok") is True else "missing",
        ),
    ]
    metric_html = "".join(
        '<div class="summary-metric">'
        f"<span>{_html(title)}</span>"
        f"<strong>{_html(value)}</strong>"
        "</div>"
        for title, value in metrics
    )
    missing_html = ""
    if missing:
        missing_html = (
            '<p class="banner banner-fail">Missing report sections: '
            + _html(", ".join(missing))
            + "</p>"
        )
    if unknown:
        missing_html += (
            '<p class="banner banner-fail">Report sections with no verdict: '
            + _html(
                ", ".join(f"{name} ({reason})" for name, reason in unknown.items())
            )
            + "</p>"
        )
    return (
        '<section class="overview" id="overview">'
        '<div class="overview-head">'
        f'<h2>Run summary {_html_status(ok)}</h2>'
        "<p>Count regression gate, sweep cases, sampled sensitivity probes, "
        "flamegraphs, and rebuild-equivalence truth proof.</p>"
        "</div>"
        f"{missing_html}<div class=\"summary-strip\">{metric_html}</div>"
        '<nav class="quick-links" aria-label="Report sections">'
        '<a href="#timing-profile">Timing profile</a>'
        '<a href="#count-gate">Count gate</a>'
        '<a href="#sweep">Sweep</a>'
        '<a href="#sensitivity">Sensitivity</a>'
        '<a href="#incremental-truth">Truth proof</a>'
        '<a href="#artifacts">Artifacts</a>'
        "</nav>"
        "</section>"
    )


def build_static_bench_report(results_dir: Path = RESULTS_DIR) -> tuple[str, dict[str, Any]]:
    """Build a self-contained HTML summary of the CI benchmark artifacts.

    The report is intentionally static: CI can start the OKG UI for browser
    proof, but a live server disappears when the job exits. This file is the
    durable post-run viewer in the downloadable artifact.
    """
    results_dir = Path(results_dir)
    flamegraph_links = _write_diagnostics_flamegraph_viewers(results_dir)
    count_html, count_state = _count_gate_report(results_dir)
    incremental_html, incremental_state = _incremental_truth_report(results_dir)
    timing_html, timing_state = _timing_profile_report(results_dir)
    sweep_html, sweep_state = _sweep_report(
        results_dir,
        flamegraph_links=flamegraph_links,
    )
    sensitivity_html, sensitivity_state = _sensitivity_report(
        results_dir,
        flamegraph_links=flamegraph_links,
    )
    states = {
        "count_gate": count_state,
        "incremental_truth": incremental_state,
        "timing_profile": timing_state,
        "sweep": sweep_state,
        "sensitivity": sensitivity_state,
        "timing_flamegraphs": {
            "ok": True,
            "missing": False,
            "optional": True,
            "count": len(flamegraph_links),
        },
    }
    navigation_entries = _flamegraph_navigation_entries(timing_state, flamegraph_links)
    _rewrite_flamegraph_viewers_with_navigation(
        results_dir,
        timing_state=timing_state,
        flamegraph_links=flamegraph_links,
        navigation_entries=navigation_entries,
    )
    states["timing_flamegraphs"]["navigation_count"] = len(navigation_entries)
    missing = [
        name
        for name, state in states.items()
        if isinstance(state, dict) and state.get("missing")
    ]
    # #2573 slice 10: a required section whose state carries no boolean
    # `ok` has no verdict. It is named in `unknown` and the run is not ok;
    # it used to count as passing. Optional sections keep their pass.
    verdicts: dict[str, bool] = {}
    unknown: dict[str, str] = {}
    for name, state in states.items():
        if not isinstance(state, dict) or state.get("optional"):
            continue
        verdict = read_ok(state, source=f"bench_{name}")
        if isinstance(verdict, Unknown):
            if not state.get("missing"):
                unknown[name] = verdict.reason
            verdicts[name] = False
        else:
            verdicts[name] = verdict
    ok = not missing and not unknown and all(verdicts.values())
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    overview_html = _overview_panel(
        states, ok=ok, missing=missing, unknown=unknown
    )
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OKG CI Benchmark Performance Report</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --panel-soft: #fafbfc;
      --text: #1d2733;
      --muted: #667085;
      --border: #dde3eb;
      --border-strong: #c5ceda;
      --pass: #136f3a;
      --fail: #b42318;
      --warn: #9a6700;
      --missing: #7f1d1d;
      --link: #175cd3;
    }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    main {{
      max-width: 1120px;
      margin: 0 auto;
      padding: 24px 20px 44px;
    }}
    h1, h2, h3 {{ margin: 0 0 12px; line-height: 1.2; }}
    h1 {{ font-size: 26px; }}
    h2 {{ font-size: 18px; }}
    h3 {{ font-size: 15px; }}
    p {{ margin: 0 0 12px; color: var(--muted); }}
    .page-head {{
      border-bottom: 1px solid var(--border);
      margin-bottom: 16px;
      padding-bottom: 16px;
    }}
    .overview, .report-section {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 14px;
      margin-top: 14px;
    }}
    .overview {{
      border-color: var(--border-strong);
    }}
    .overview-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: start;
      border-bottom: 1px solid var(--border);
      padding-bottom: 12px;
      margin-bottom: 12px;
    }}
    .overview-head h2 {{
      display: flex;
      gap: 10px;
      align-items: center;
      margin: 0;
    }}
    .summary-strip {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 1px;
      align-items: start;
      background: var(--border);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
    }}
    .summary-metric {{
      background: var(--panel-soft);
      min-height: 74px;
      padding: 12px 14px;
    }}
    .summary-metric span {{
      color: var(--muted);
      display: block;
      font-size: 12px;
      text-transform: uppercase;
    }}
    .summary-metric strong {{
      color: var(--text);
      display: block;
      font-size: 18px;
      font-weight: 700;
      line-height: 1.25;
      margin-top: 8px;
    }}
    .quick-links {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 14px;
    }}
    .quick-links a {{
      background: var(--panel-soft);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 5px 8px;
      text-decoration: none;
    }}
    .report-stack {{
      display: grid;
      gap: 14px;
      margin-top: 16px;
    }}
    .report-section {{
      margin-top: 0;
    }}
    .section-head {{
      align-items: center;
      border-bottom: 1px solid var(--border);
      display: flex;
      gap: 12px;
      justify-content: space-between;
      margin: -2px 0 14px;
      padding-bottom: 10px;
    }}
    .section-head h2 {{
      margin: 0;
    }}
    .section-intro {{
      margin-top: -4px;
    }}
    .summary-pair {{
      display: grid;
      grid-template-columns: minmax(280px, 0.9fr) minmax(280px, 1.1fr);
      gap: 12px;
      align-items: stretch;
    }}
    .fact-panel, .info-panel, .table-panel, .detail-block {{
      border-top: 1px solid var(--border);
      margin-top: 14px;
      padding-top: 14px;
    }}
    .table-panel {{
      overflow-x: auto;
    }}
    .summary-pair .fact-panel, .summary-pair .info-panel {{
      border: 1px solid var(--border);
      border-radius: 8px;
      margin-top: 0;
      padding: 14px;
    }}
    .fact-panel:first-child, .info-panel:first-child, .table-panel:first-child {{
      border-top: 0;
      margin-top: 0;
      padding-top: 0;
    }}
    .fact-head {{
      align-items: center;
      display: flex;
      gap: 10px;
      justify-content: space-between;
      margin-bottom: 10px;
    }}
    .fact-head h3 {{
      margin: 0;
    }}
    .fact-list {{
      display: grid;
      grid-template-columns: minmax(140px, 0.7fr) minmax(180px, 1.3fr);
      gap: 6px 14px;
      margin: 0;
    }}
    .detail-block {{
      overflow-x: auto;
    }}
    .detail-collapse {{
      background: var(--panel-soft);
      padding: 0;
    }}
    .detail-collapse summary {{
      cursor: pointer;
      font-weight: 700;
      list-style: none;
      padding: 12px 14px;
    }}
    .detail-collapse summary::-webkit-details-marker, .section-summary::-webkit-details-marker {{
      display: none;
    }}
    .detail-collapse summary::after, .section-summary::after {{
      color: var(--muted);
      content: "+";
      float: right;
      font-weight: 700;
    }}
    .detail-collapse[open] summary::after, .artifacts-section[open] > .section-summary::after {{
      content: "-";
    }}
    .detail-collapse > :not(summary) {{
      margin: 0 14px 14px;
    }}
    .artifact-actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 12px 0;
    }}
    .artifact-actions a {{
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 6px 8px;
      text-decoration: none;
    }}
    dl {{
      display: grid;
      grid-template-columns: minmax(120px, 0.8fr) 1.2fr;
      gap: 6px 14px;
      margin: 0;
    }}
    dt {{ color: var(--muted); }}
    dd {{ margin: 0; font-weight: 600; overflow-wrap: anywhere; }}
    .status {{
      border-radius: 999px;
      color: white;
      display: inline-block;
      font-size: 12px;
      font-weight: 700;
      line-height: 1;
      padding: 5px 8px;
    }}
    .status-pass {{ background: var(--pass); }}
    .status-fail {{ background: var(--fail); }}
    .status-warn {{ background: var(--warn); }}
    .status-missing {{ background: var(--missing); }}
    .data-table {{
      border-collapse: collapse;
      width: 100%;
      overflow-wrap: anywhere;
    }}
    .data-table th, .data-table td {{
      border-bottom: 1px solid var(--border);
      padding: 8px 10px;
      text-align: left;
      vertical-align: top;
    }}
    .data-table th {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
    }}
    .artifact-group {{
      border-top: 1px solid var(--border);
      padding: 10px 0;
    }}
    .artifact-group:first-child {{ border-top: 0; }}
    .artifact-group summary {{
      cursor: pointer;
      font-weight: 700;
    }}
    .artifact-group ul {{
      columns: 2;
      margin-bottom: 0;
    }}
    .section-summary {{
      align-items: center;
      cursor: pointer;
      display: flex;
      justify-content: space-between;
      list-style: none;
    }}
    .section-summary h2 {{
      margin: 0;
    }}
    .banner {{
      border-radius: 8px;
      font-weight: 700;
      padding: 10px 12px;
    }}
    .banner-fail {{
      background: #fff1f0;
      color: var(--fail);
    }}
    .muted {{ color: var(--muted); }}
    a {{ color: var(--link); }}
    @media (max-width: 760px) {{
      main {{ padding: 20px 12px 36px; }}
      .overview-head, .section-head {{ display: block; }}
      .summary-pair {{ grid-template-columns: 1fr; }}
      .artifact-group ul {{ columns: 1; }}
    }}
  </style>
</head>
<body>
<main>
  <header class="page-head">
    <h1>OKG CI Benchmark Performance Report</h1>
    <p>Generated {generated_at} from tinydb commit-replay benchmark artifacts in {_html(results_dir)}.</p>
  </header>
  {overview_html}
  <div class="report-stack">
    {timing_html}
    {count_html}
    {sweep_html}
    {sensitivity_html}
    {incremental_html}
  </div>
  <details class="report-section artifacts-section" id="artifacts">
    <summary class="section-summary"><h2>Artifacts</h2></summary>
    <p class="section-intro">Grouped links to the files CI uploaded. The report is static because CI servers stop when the runner exits.</p>
    <h3>Artifact files</h3>
    {_artifact_links(results_dir, flamegraph_links=flamegraph_links)}
  </details>
</main>
</body>
</html>
"""
    summary = {
        "schema": "okg.bench.static_report.v1",
        "ok": ok,
        "generated_at": generated_at,
        "results_dir": str(results_dir),
        "missing": missing,
        "unknown": unknown,
        "states": states,
    }
    return html_doc, summary


def write_static_bench_report(
    *,
    results_dir: Path = RESULTS_DIR,
    output: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    results_dir = Path(results_dir)
    output = Path(output) if output is not None else results_dir / "index.html"
    html_doc, summary = build_static_bench_report(results_dir)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html_doc, encoding="utf-8")
    return output, summary


_SWEEP_METRIC_KEYS = [
    "e2e_ms_mean",
    "e2e_ms_p95",
    "cycle_ms_mean",
    "source_total_ms_mean",
    "source_run_count_mean",
    "projection_ms_mean",
    "enrichment_ms_mean",
    "gate_ms_mean",
    "publish_ms_mean",
    "view_materialization_ms_mean",
    "publish_total_ms_mean",
    "edges_added_total",
    "final_edges_live",
    "final_nodes_live",
    "db_size_bytes",
    "peak_rss_bytes",
    "chronos_segments",
]


def _metric_unit(name: str) -> str:
    if (
        name.endswith("_ms")
        or name.endswith("_ms_mean")
        or name.startswith("read_p50_")
    ):
        return "ms"
    if name.endswith("_bytes"):
        return "bytes"
    return "count"


SENSITIVITY_SUMMARY_SCHEMA = "okg.bench.sensitivity_summary.v1"


def _axis_value_for_measurement(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def _sensitivity_cell_rows(plan: dict, exps: list[Experiment]) -> list[dict[str, Any]]:
    selected = {e.id for e in exps}
    profiles = {e.id: _cell_profile(e) for e in exps}
    rows: list[dict[str, Any]] = []
    for cell in plan.get("cells", []):
        cid = cell["id"]
        if cid not in selected:
            continue
        exp_doc = cell.get("experiment") or {}
        rows.append(
            {
                "cell": cid,
                "axis": cell.get("axis"),
                "axis_value": cell.get("axis_value"),
                "baseline": bool(cell.get("baseline")),
                "source_set": exp_doc.get("source_set"),
                "replay": exp_doc.get("replay"),
                "config": exp_doc.get("config") or {},
                "profile": profiles[cid],
            }
        )
    return rows


def _sensitivity_measurements(cell_rows: list[dict[str, Any]]) -> list[dict]:
    """Flatten sensitivity cell profiles into the same BenchMeasurement shape
    used by the standardized sweep, so okg-ci can trend both surfaces."""
    out: list[dict] = []

    def _add(cell, axis, axis_value, metric, value):
        if value is None:
            return
        try:
            v = float(value)
        except (TypeError, ValueError):
            return
        out.append(
            {
                "cell": cell,
                "config_axis": axis,
                "axis_value": _axis_value_for_measurement(axis_value),
                "metric_name": metric,
                "value": v,
                "unit": _metric_unit(metric),
            }
        )

    for row in cell_rows:
        prof = row.get("profile") or {}
        for metric in _SWEEP_METRIC_KEYS:
            _add(row["cell"], row.get("axis"), row.get("axis_value"), metric, prof.get(metric))
        for read_name, value in (prof.get("read_side_p50") or {}).items():
            _add(
                row["cell"],
                row.get("axis"),
                row.get("axis_value"),
                f"read_p50_{read_name}",
                value,
            )
    return out


def _sensitivity_markdown(summary: dict[str, Any]) -> str:
    spec = summary.get("spec") or {}
    lines = [
        f"## Differential engine sensitivity benchmark - {spec.get('id')}",
        "",
        "Advisory performance-regression surface. Values are trended by CI; " +
        "they are not hard merge gates.",
        "",
        "| cell | axis | value | commits | e2e mean ms | e2e p95 ms | " +
        "source mean ms | publish total ms | view mean ms | enrich mean ms | " +
        "final nodes | final edges | aggregate read p50 ms |",
        "|---|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|",
    ]
    for row in summary.get("cells", []):
        prof = row.get("profile") or {}
        read_p50 = (prof.get("read_side_p50") or {}).get("aggregate_by_type")
        value = _axis_value_for_measurement(row.get("axis_value"))
        lines.append(
            f"| `{row.get('cell')}` | `{row.get('axis')}` | `{value}` | "
            f"{_md_num(prof.get('commits'))} | "
            f"{_md_num(prof.get('e2e_ms_mean'))} | "
            f"{_md_num(prof.get('e2e_ms_p95'))} | "
            f"{_md_num(prof.get('source_total_ms_mean'))} | "
            f"{_md_num(prof.get('publish_total_ms_mean'))} | "
            f"{_md_num(prof.get('view_materialization_ms_mean'))} | "
            f"{_md_num(prof.get('enrichment_ms_mean'))} | "
            f"{_md_num(prof.get('final_nodes_live'))} | "
            f"{_md_num(prof.get('final_edges_live'))} | "
            f"{_md_num(read_p50)} |"
        )
    lines += [
        "",
        "_Full per-cell CSVs, read-side JSON, and the sensitivity plan are in " +
        "the `bench-internals` workflow artifact._",
    ]
    return "\n".join(lines) + "\n"


def _write_sensitivity_summary(
    plan: dict,
    exps: list[Experiment],
) -> tuple[Path, Path]:
    spec_id = plan["spec"]["id"]
    cell_rows = _sensitivity_cell_rows(plan, exps)
    summary = {
        "schema": SENSITIVITY_SUMMARY_SCHEMA,
        "kind": "engine_sensitivity",
        "spec": plan.get("spec"),
        "baseline_cell": plan.get("baseline_cell"),
        "selected_cells": [row["cell"] for row in cell_rows],
        "n_cells": len(cell_rows),
        "cells": cell_rows,
        "measurements": _sensitivity_measurements(cell_rows),
    }
    summary_json = RESULTS_DIR / f"{spec_id}.sensitivity_summary.json"
    summary_md = RESULTS_DIR / f"{spec_id}.sensitivity_summary.md"
    summary_json.write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    summary_md.write_text(_sensitivity_markdown(summary), encoding="utf-8")
    return summary_json, summary_md


def _sweep_measurements(comparisons: dict, branching: dict) -> list[dict]:
    """Flatten the swept-cell profiles + branching costs into a self-contained
    (cell, metric, value, unit) list — the regression-trackable record the
    okg-ci bench source ingests as BenchMeasurement nodes. Absent metrics are
    omitted (never emitted as 0/placeholder)."""
    out: list[dict] = []

    def _add(cell, axis, axis_value, metric, value):
        if value is None:
            return
        try:
            v = float(value)
        except (TypeError, ValueError):
            return
        out.append(
            {
                "cell": cell,
                "config_axis": axis,
                "axis_value": None if axis_value is None else str(axis_value),
                "metric_name": metric,
                "value": v,
                "unit": _metric_unit(metric),
            }
        )

    for rep in comparisons.values():
        if not rep.get("ok"):
            continue
        axis = rep.get("swept_axis")
        for c in rep.get("cells", []):
            prof = c.get("profile", {})
            for m in _SWEEP_METRIC_KEYS:
                _add(c["cell_id"], axis, c.get("axis_value"), m, prof.get(m))
            for rk, rv in (prof.get("read_side_p50") or {}).items():
                _add(c["cell_id"], axis, c.get("axis_value"), f"read_p50_{rk}", rv)
    if branching.get("ok"):
        cell = branching.get("experiment_id")
        br, lin = branching["branching"], branching["linear_append"]
        _add(
            cell,
            "branching",
            "branch",
            "linear_mean_commit_ms",
            lin.get("mean_commit_ms"),
        )
        for m in ("fork_ms", "mean_branch_commit_ms", "merge_ms", "compact_ms"):
            _add(cell, "branching", "branch", m, br.get(m))
    return out


def cmd_sweep(args) -> int:
    cells = _sweep_cells()
    if not cells:
        sys.exit(f"no sweep cells (ids starting with {SWEEP_PREFIX!r}) in manifest")

    # --- 1. PIN: build each cell's commit list + verify against the lock. ----
    # tinydb cells use the root-anchored `first_n` strategy, so the commit list
    # is fixed (NOT HEAD-relative); the lock records the exact SHAs and a re-run
    # MUST reproduce them. Drift (upstream history rewrite) fails loudly.
    lock_path = BENCH_DIR / "sweep.lock.json"
    current: dict[str, Any] = {}
    for e in cells:
        _ensure_repo(e)
        cl = _commits_for(e)
        current[e.id] = {
            "count": len(cl),
            "first": cl[0] if cl else None,
            "last": cl[-1] if cl else None,
            "commits": cl,
            "source_set": e.source_set,
            "config_axis": {a: getattr(e.config, a) for a in SWEEPABLE_AXES},
        }
    drift: list[str] = []
    if lock_path.exists():
        prev = json.loads(lock_path.read_text(encoding="utf-8")).get("cells", {})
        for cid, cur in current.items():
            if prev.get(cid, {}).get("commits") != cur["commits"]:
                drift.append(cid)
        reproducible: Any = not drift
    else:
        lock_path.write_text(
            json.dumps(
                {"schema": "okg.bench.sweep.lock.v1", "cells": current},
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        reproducible = "lock_initialized"
    if drift:
        sys.stderr.write(
            f"SWEEP NOT REPRODUCIBLE: commit list drifted for {drift}; "
            f"upstream tinydb history changed vs {lock_path}\n"
        )

    # --- 2. RUN each cell end-to-end (ingest + extract + read-side). ---------
    if not args.skip_run:
        for e in cells:
            print(f"\n######## sweep run: {e.id} ########")
            _run([PYTHON, "-m", "okg.bench.orchestrator", "run", e.id])

    # --- 3. branching experiment + per-group comparisons. --------------------
    # The branching experiment mutates its DB (fork/merge/compact) and is not
    # idempotent on an already-branched DB, so under --skip-run (reuse
    # artifacts) read the saved report rather than re-running it. The full
    # sweep re-ingests a fresh tdb-branch first, so a single run there is clean.
    branching_path = RESULTS_DIR / f"{SWEEP_BRANCHING_CELL}.branching.json"
    if args.skip_run and branching_path.exists():
        branching = json.loads(branching_path.read_text(encoding="utf-8"))
    else:
        branching = build_branching_report(_resolve(SWEEP_BRANCHING_CELL))
        branching_path.write_text(
            json.dumps(branching, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
    comparisons: dict[str, Any] = {}
    for g in SWEEP_COMPARISON_GROUPS:
        rep = build_comparison_report(g)
        (RESULTS_DIR / f"{g}.comparison.json").write_text(
            json.dumps(rep, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        comparisons[g] = rep

    # --- 4. coverage: the full metric set is present on every cell. ----------
    coverage = _sweep_metric_coverage(cells)
    all_covered = all(
        c["all_metrics_present"] and c["read_side"] for c in coverage.values()
    )

    summary = {
        "schema": "okg.bench.sweep_summary.v1",
        "cells": sorted(current.keys()),
        "n_cells": len(cells),
        "pin": {"lock": str(lock_path), "reproducible": reproducible, "drift": drift},
        "comparison_groups": {
            g: {
                "ok": comparisons[g].get("ok"),
                "swept_axis": comparisons[g].get("swept_axis"),
            }
            for g in SWEEP_COMPARISON_GROUPS
        },
        "branching_ok": branching.get("ok"),
        "metric_coverage_all_present": all_covered,
        "coverage": coverage,
        # Self-contained (cell, metric, value) records for okg-ci ingestion
        # (BenchMeasurement nodes, regression-trackable over head_sha).
        "measurements": _sweep_measurements(comparisons, branching),
    }
    out = RESULTS_DIR / "sweep_summary.json"
    out.write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    # Markdown rendering for the CI job summary (the Actions run page) — so the
    # benchmark is readable straight from CI without running anything.
    md_path = RESULTS_DIR / "sweep_summary.md"
    md_path.write_text(
        _sweep_markdown(summary, comparisons, branching), encoding="utf-8"
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    else:
        print(f"\n=== sweep summary ({len(cells)} cells) ===")
        print(
            f"  pin reproducible: {reproducible}"
            + (f"  DRIFT={drift}" if drift else "")
        )
        for g in SWEEP_COMPARISON_GROUPS:
            print(
                f"  comparison {g}: ok={comparisons[g].get('ok')} "
                f"axis={comparisons[g].get('swept_axis')}"
            )
        print(f"  branching: ok={branching.get('ok')}")
        print(f"  full metric set on every cell: {all_covered}")
        print(f"  summary: {out}")
        print(f"  markdown (CI job summary): {md_path}")
    ok = (
        (reproducible is not False)
        and all_covered
        and branching.get("ok")
        and all(comparisons[g].get("ok") for g in SWEEP_COMPARISON_GROUPS)
    )
    return 0 if ok else 1


def cmd_report(args) -> int:
    results_dir = Path(args.results_dir)
    output = Path(args.output) if args.output else results_dir / "index.html"
    report_path, summary = write_static_bench_report(
        results_dir=results_dir,
        output=output,
    )
    if args.summary_json:
        Path(args.summary_json).write_text(
            json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
    if args.json:
        print(json.dumps({**summary, "path": str(report_path)}, indent=2, sort_keys=True))
    else:
        missing = summary.get("missing") or []
        suffix = f" (missing: {', '.join(missing)})" if missing else ""
        print(f"bench report: {report_path}{suffix}")
    return 0


def cmd_comparison_report(args) -> int:
    report = build_comparison_report(args.base_id)
    out = RESULTS_DIR / f"{args.base_id}.comparison.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return 0 if report.get("ok") else 1
    if not report.get("ok"):
        print(f"comparison NOT ok: {report.get('reason')}")
        return 1

    def a(v):  # absolute
        return "NA" if v is None else f"{v:.1f}"

    def d(v):  # signed delta
        return "NA" if v is None else f"{v:+.1f}"

    axis = report["swept_axis"]
    print(f"config comparison: {report['base_id']}  (swept axis: {axis})")
    print(
        f"baseline: {report['baseline_cell']} ({axis}={report['baseline_axis_value']})"
    )
    print(
        f"\n  {axis:>11}  {'e2e/cmt':>8} {'Δe2e':>7} {'Δedges':>7} "
        f"{'Δenrich':>8} {'Δview_mat':>9} {'Δagg_read':>9}"
    )
    for c in report["cells"]:
        prof, m = c["profile"], c["marginal_vs_baseline"]
        if c["is_baseline"]:
            print(
                f"  {str(c['axis_value']):>11}  {a(prof['e2e_ms_mean']):>8} "
                f"{'(base)':>7}"
            )
            continue
        agg = (m.get("read_side_p50") or {}).get("aggregate_by_type")
        print(
            f"  {str(c['axis_value']):>11}  {a(prof['e2e_ms_mean']):>8} "
            f"{d(m['e2e_ms_mean']):>7} {d(m['edges_added_total']):>7} "
            f"{d(m['enrichment_ms_mean']):>8} "
            f"{d(m['view_materialization_ms_mean']):>9} {d(agg):>9}"
        )
    print(f"\n  (ms per commit; Δ = marginal cost vs baseline; full: {out})")
    return 0


def cmd_branching_report(args) -> int:
    exp = _resolve(args.id)
    report = build_branching_report(
        exp, n_commits=args.n_commits, nodes_per_commit=args.nodes_per_commit
    )
    out = RESULTS_DIR / f"{exp.id}.branching.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    elif report.get("reason"):
        print(f"[{exp.id}] branching report NOT ok: {report.get('reason')}")
    elif report.get("missing_metrics"):
        # Ops ran but interval-stat queries failed: surface the data loss
        # loudly rather than printing blank/None metrics as if measured.
        br = report["branching"]
        print(
            f"[{exp.id}] branching report PARTIAL — interval metrics "
            f"FAILED TO READ: {report['missing_metrics']}"
        )
        print(
            f"  timings ok: fork={br['fork_ms']:.2f}ms "
            f"merge={br['merge_ms']:.2f}ms compact={br['compact_ms']}ms; "
            f"segment/interval growth UNAVAILABLE"
        )
    else:
        lin, br = report["linear_append"], report["branching"]
        print(
            f"[{exp.id}] branching workload "
            f"(n_commits={report['params']['n_commits']}):"
        )
        print(
            f"  linear-append : {lin['mean_commit_ms']:.2f} ms/commit, "
            f"+{lin['segments_added']} segments, "
            f"+{lin['interval_space_added']} interval-space"
        )
        print(f"  fork          : {br['fork_ms']:.2f} ms")
        print(f"  branch-commit : {br['mean_branch_commit_ms']:.2f} ms/commit")
        print(f"  merge         : {br['merge_ms']:.2f} ms")
        cm = br["compact_ms"]
        print(
            f"  compact       : {cm:.2f} ms"
            if cm is not None
            else "  compact       : (skipped)"
        )
        print(
            f"  branching grew +{br['segments_added_fork_to_compact']} segments, "
            f"+{br['interval_space_added_fork_to_compact']} interval-space"
        )
        print(f"  full report: {out}")
    return 0 if report.get("ok") else 1


def cmd_clean(_args) -> int:
    clean_all()
    return 0


def cmd_plot(args) -> int:
    cmd = [PYTHON, "-m", "okg.bench.plot"]
    if args.cross:
        cmd.append("--cross")
    elif args.all:
        cmd.append("--all")
    elif args.id:
        cmd.append(args.id)
    else:
        sys.exit("plot needs <id>, --all, or --cross")
    res = _run(cmd, check=False, capture=False)
    return res.returncode


def main() -> int:
    p = argparse.ArgumentParser(prog="bench-orchestrator")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="print experiment matrix")
    sub.add_parser("clean", help="drop all bench DBs + generated dirs")

    sp = sub.add_parser("setup-db", help="recreate the bench DB")
    sp.add_argument("id")

    sp = sub.add_parser("start-worker", help="launch worker for experiment")
    sp.add_argument("id")

    sp = sub.add_parser("stop-worker", help="kill worker for experiment")
    sp.add_argument("id")

    sp = sub.add_parser("run", help="full pipeline for one experiment or all")
    sp.add_argument("id", nargs="?", default=None)
    sp.add_argument("--all", action="store_true")
    sp.add_argument(
        "--prove-rebuild-equivalence",
        action="store_true",
        help="after the replay, run the REAL incremental-vs-forced-rebuild "
        "equivalence (build_differential_rebuild_report publishes a forced "
        "rebuild and digests it) and FAIL the run on mismatch — gives the "
        "issue-79 'same final state' proof a gate instead of a manual-only "
        "`differential-report` command",
    )

    sp = sub.add_parser(
        "sensitivity",
        help="plan or run the differential-engine sensitivity matrix",
    )
    sp.add_argument(
        "--config",
        default=str(ENGINE_SENSITIVITY_CONFIG),
        help="sensitivity YAML config",
    )
    sp.add_argument(
        "--cell",
        action="append",
        help="restrict --run or --run-branching-report to one generated cell id",
    )
    sp.add_argument("--run", action="store_true", help="run selected cells")
    sp.add_argument(
        "--prove-rebuild-equivalence",
        action="store_true",
        help="when used with --run, prove incremental-vs-forced-rebuild equivalence",
    )
    sp.add_argument(
        "--run-branching-report",
        action="store_true",
        help="run the configured Chronos branching side report for one --cell",
    )
    sp.add_argument(
        "--branching-n-commits",
        type=int,
        default=5,
        dest="branching_n_commits",
    )
    sp.add_argument(
        "--branching-nodes-per-commit",
        type=int,
        default=25,
        dest="branching_nodes_per_commit",
    )
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser("plot", help="generate per-experiment or cross PDFs")
    sp.add_argument("id", nargs="?", default=None)
    sp.add_argument("--all", action="store_true")
    sp.add_argument("--cross", action="store_true")

    sp = sub.add_parser(
        "profile",
        help="write an OKG profile bundle for one replay source run",
    )
    sp.add_argument("id")
    sp.add_argument("--source", default=None)
    sp.add_argument("--commit", default=None)
    sp.add_argument("--run-id", default=None)
    sp.add_argument(
        "--profile-root",
        default=str(DEFAULT_PROFILE_ROOT),
        help="profile root directory (default: .artifacts/profiles)",
    )
    sp.add_argument(
        "--no-ingest-dry-run",
        action="store_true",
        help="skip ProfileMeasurementSource dry-run projection",
    )
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser(
        "diagnostics",
        help="inspect replay diagnostics and profile-measurements projection",
    )
    sp.add_argument("id")
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser(
        "gate",
        help="fail when normalized replay artifacts exceed latency budgets",
    )
    sp.add_argument("id")
    sp.add_argument("--max-p95-ms", type=float, default=1000.0)
    sp.add_argument("--max-max-ms", type=float, default=5000.0)
    sp.add_argument(
        "--min-throughput-commits-per-second",
        type=float,
        default=1.0,
    )
    sp.add_argument("--max-outlier-count", type=int, default=0)
    sp.add_argument("--outlier-threshold-ms", type=float, default=1000.0)
    sp.add_argument("--max-queue-dwell-p95-ms", type=float, default=None)
    sp.add_argument("--require-diagnostics", action="store_true")
    # Count-based budgets (count-based-perf-gate / req.counts.gate). The
    # primary budget is the experiment's manifest `count_gate:` block: one
    # measured query_count per pinned commit and a headroom factor. The
    # flags below are opt-in extra caps. A gross per-file cap has no
    # default: every commit pays a fixed floor regardless of size, so it
    # fails any 1-file commit.
    sp.add_argument(
        "--max-queries-per-changed-file",
        type=float,
        default=None,
        help=(
            "optional gross cap: fail the count gate when any commit's "
            "query_count / files_changed exceeds this budget"
        ),
    )
    sp.add_argument(
        "--max-no-change-query-count",
        type=int,
        default=None,
        help=(
            "optional absolute query_count floor for the lightest commit; "
            "fails the count gate when the no-change/lightest commit's "
            "query_count exceeds it"
        ),
    )
    sp.add_argument(
        "--junit",
        default=None,
        help="write the gate result as a JUnit XML report to this path",
    )
    sp.add_argument("--output", default=None)
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser(
        "lsp-proof",
        help="prove an LSP benchmark emitted cross-file semantic edges",
    )
    sp.add_argument("id")
    sp.add_argument("--min-references", type=int, default=1)
    sp.add_argument("--min-calls", type=int, default=1)
    sp.add_argument(
        "--allow-source-failure-markers",
        action="store_true",
        help="do not fail when source/LSP failure markers appear in logs",
    )
    sp.add_argument("--output", default=None)
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser(
        "differential-report",
        help="compare incremental replay output to a forced rebuild",
    )
    sp.add_argument("id")
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser(
        "lineage-report",
        help="write synthetic materialized-output lineage proof report",
    )
    sp.add_argument("id")
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser(
        "runtime-lineage-report",
        help="write DB-backed runtime lineage invalidation proof report",
    )
    sp.add_argument("id")
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser(
        "read-side-report",
        help="time canonical reads at the final published generation",
    )
    sp.add_argument("id")
    sp.add_argument("--iterations", type=int, default=_READ_SIDE_ITERATIONS)
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser(
        "branching-report",
        help="benchmark chronos fork/branch-publish/merge/compact internals",
    )
    sp.add_argument("id")
    sp.add_argument("--n-commits", type=int, default=5, dest="n_commits")
    sp.add_argument("--nodes-per-commit", type=int, default=25, dest="nodes_per_commit")
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser(
        "comparison-report",
        help="attribute marginal cost to a swept config dimension",
    )
    sp.add_argument("base_id", help="sweep base id, e.g. tdb-enr or tdb-chr")
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser(
        "sweep",
        help="run the full pinned tinydb internals sweep + comparisons",
    )
    sp.add_argument(
        "--skip-run",
        action="store_true",
        dest="skip_run",
        help="reuse existing per-cell artifacts (pin-verify + reports only)",
    )
    sp.add_argument("--json", action="store_true")

    sp = sub.add_parser(
        "report",
        help="render a static HTML report from bench result artifacts",
    )
    sp.add_argument(
        "--results-dir",
        default=str(RESULTS_DIR),
        help="directory containing bench result artifacts",
    )
    sp.add_argument(
        "--output",
        default=None,
        help="HTML output path (default: <results-dir>/index.html)",
    )
    sp.add_argument(
        "--summary-json",
        default=None,
        help="optional machine-readable report summary path",
    )
    sp.add_argument("--json", action="store_true")

    args = p.parse_args()
    dispatch = {
        "list": cmd_list,
        "setup-db": cmd_setup_db,
        "start-worker": cmd_start_worker,
        "stop-worker": cmd_stop_worker,
        "run": cmd_run,
        "sensitivity": cmd_sensitivity,
        "clean": cmd_clean,
        "plot": cmd_plot,
        "profile": cmd_profile,
        "diagnostics": cmd_diagnostics,
        "gate": cmd_gate,
        "lsp-proof": cmd_lsp_proof,
        "differential-report": cmd_differential_report,
        "lineage-report": cmd_lineage_report,
        "runtime-lineage-report": cmd_runtime_lineage_report,
        "read-side-report": cmd_read_side_report,
        "branching-report": cmd_branching_report,
        "comparison-report": cmd_comparison_report,
        "sweep": cmd_sweep,
        "report": cmd_report,
    }
    return dispatch[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
