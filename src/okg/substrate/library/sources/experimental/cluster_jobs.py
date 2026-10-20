"""ClusterJobsSource — emits ComputeJob nodes from a local cache.

Default mode is ``operator-pulled``: the operator runs
``okg import-cluster-jobs --deployment <name> --host <h>`` periodically;
that CLI command populates a local cache directory which this source
then reads on the next publish cycle. The source itself does NO SSH and
makes NO network calls in this mode — fully consistent with the
umbrella's D7 operator-triggered-batch discipline.

The ``substrate-pulled`` mode (auto-SSH at publish time) raises
:class:`ClusterPullModeError` until a follow-up proposal addresses
authentication.

Two cache layouts are recognised:

1. ``<cache_dir>/<host>/<file>`` — files matching ``job_log_glob``
   relative to ``<cache_dir>/<host>/``. The host directory keeps
   per-cluster pulls separable.
2. ``<cache_dir>/<file>`` — flat layout for single-cluster
   deployments. The host directory is auto-created when missing.

Each job log file may be a Slurm ``sacct`` JSON dump, an HTCondor
``condor_history -json`` dump, or a hand-rolled YAML/JSON list of
job records. The source recognises any of these by parsing the
file as JSON or YAML and looking for a list of mappings with the
expected fields.

Profile: ``reference_catalog`` — the local cache *is* the reference;
absence in the cache implies retraction of the corresponding job
nodes.

Per `extend-substrate-experimental-with-compute-job-and-sources` Phase 5.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar, Iterator, Optional

import yaml
from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceHealth,
    SourceRun,
)


__all__ = [
    "ClusterJobsSource",
    "ClusterPullModeError",
]


class ClusterPullModeError(SubstrateError):
    """Raised when the source is configured with ``pull_mode:
    substrate-pulled`` — that mode is deferred to a follow-up
    proposal addressing cluster authentication."""

    code: ClassVar[str] = "cluster_pull_mode_not_implemented"


# Threshold for stale-cache warnings (seconds). Operator can override
# via `staleness_warn_seconds`.
_DEFAULT_STALENESS_WARN_SECONDS = 86400  # 24 hours


@dataclass
class ClusterJobsSource:
    """Substrate adapter for cluster job logs (operator-pulled)."""

    host: str
    """Cluster slug used in the ComputeJob node identity (e.g.
    ``cluster01``, ``gridlogin``, ``local``). NOT a hostname — the
    deployment maps slugs to hostnames in its CLI invocation."""

    user: str = ""
    """Cluster username. Recorded as an attribute; not used to
    issue SSH in this mode."""

    job_log_glob: str = "*.json"
    """Glob pattern within the cache directory. Default matches
    Slurm/HTCondor JSON dumps; YAML files matched via ``*.yaml``."""

    cache_dir: Path | str = Path("~/.cache/okg/cluster_logs")
    """Local cache directory the operator's ``import-cluster-jobs``
    CLI command populates. Default is a substrate-agnostic
    ~/.cache/okg/ path; deployments override via their source
    registry (academic-researcher sets a profile-local path)."""

    pull_mode: str = "operator-pulled"
    """``operator-pulled`` (default) reads cache only.
    ``substrate-pulled`` raises :class:`ClusterPullModeError`."""

    staleness_warn_seconds: int = _DEFAULT_STALENESS_WARN_SECONDS

    name: str = "experimental.cluster_jobs"
    profile: str = "reference_catalog"

    # Incremental-source contract (issue #323): in the only supported mode
    # (``operator-pulled``) the scope is a set of local cluster-log files —
    # Slurm/HTCondor JSON dumps or operator-rolled YAML — that the operator's
    # ``import-cluster-jobs`` CLI deposits in the cache. There is no git head,
    # append offset, or upstream API the source itself reads (``substrate-pulled``
    # auto-SSH is deferred and RAISES), just bytes on disk, so the sound floor is
    # the O(content) ContentHashProbe over the source's OWN file enumeration
    # (``_iter_paths`` — which resolves the per-host vs flat cache layout that the
    # generic ``file_content_items`` helper does not). It is membership-sensitive,
    # so an add / edit / delete of any matched log file busts the token — and the
    # ``reference_catalog`` profile treats absence as retraction, so delete
    # sensitivity is required for soundness. It is content-based, so a
    # worktree-checkout / re-pull mtime bump with identical bytes does NOT bust it.
    # Emitted ``compute_job`` node attrs derive PURELY from each file's parsed
    # records (``_JobRecord`` fields); the only ``now()``-derived values
    # (``_now_iso`` / staleness comparison) feed ``SourceHealth``, never an emitted
    # node/edge attr, and ``run_id`` lands only on ``source_revision`` lineage — so
    # a content hash is a complete superset of the emission-determining content.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if isinstance(self.cache_dir, str):
            self.cache_dir = Path(self.cache_dir).expanduser()
        if self.pull_mode not in ("operator-pulled", "substrate-pulled"):
            raise ClusterPullModeError(
                f"unknown pull_mode {self.pull_mode!r}",
                fix_hint=(
                    "pull_mode must be 'operator-pulled' (default) or "
                    "'substrate-pulled' (deferred)"
                ),
            )
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Path]]:
            # Re-enumerate live scope each call (a deleted log file is simply
            # absent from the next enumeration) using the source's OWN cache-root
            # resolution + glob, so the per-host (``<cache_dir>/<host>/``) and
            # flat (``<cache_dir>/``) layouts are both covered exactly as ``run``
            # reads them.
            return [(str(p), p) for p in self._iter_paths()]

        return ContentHashProbe(
            content_items=_items,
            config={
                "host": self.host,
                "user": self.user,
                "job_log_glob": self.job_log_glob,
                "cache_dir": str(self.cache_dir),
                "pull_mode": self.pull_mode,
                "name": self.name,
                "profile": self.profile,
            },
            emit_targets=[sys.modules[__name__]],
        )

    # ---------- public API ----------

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        if self.pull_mode == "substrate-pulled":
            raise ClusterPullModeError(
                "substrate-pulled cluster ingest is deferred to a "
                "follow-up proposal that addresses cluster "
                "authentication",
                fix_hint=(
                    "set pull_mode: operator-pulled and run "
                    "`okg <deployment> import-cluster-jobs --host "
                    "<h>` to populate the cache."
                ),
            )

        jobs = list(self._read_jobs())
        revision = {"run_id": run_id, "host": self.host}
        completed_scope = mode != "cursor"
        record_set = self._build_record_set(jobs, revision)
        health = self._compute_health(jobs)

        def _facts() -> Iterator[Any]:
            for job in jobs:
                yield from job.to_facts(
                    source_name=self.name,
                    host=self.host,
                    revision=revision,
                )

        return SourceRun(
            facts=_facts(),
            completed_scope=completed_scope,
            next_cursor=cursor or {},
            record_set=record_set,
            health=health,
        )

    # ---------- impl ----------

    def _resolve_cache_root(self) -> Path:
        base = Path(self.cache_dir)
        per_host = base / self.host
        if per_host.is_dir():
            return per_host
        return base

    def _iter_paths(self) -> Iterator[Path]:
        root = self._resolve_cache_root()
        if not root.is_dir():
            return
        for p in sorted(root.glob(self.job_log_glob)):
            if p.is_file():
                yield p

    def _read_jobs(self) -> Iterator["_JobRecord"]:
        for path in self._iter_paths():
            ext = path.suffix.lower()
            try:
                if ext in (".yaml", ".yml"):
                    doc = fastyaml.safe_load(path.read_text() or "") or []
                else:
                    doc = json.loads(path.read_text() or "null") or []
            except (yaml.YAMLError, json.JSONDecodeError):
                # Unparseable log file — skip silently rather than
                # break the whole pull. The operator can inspect
                # the file directly.
                continue

            for raw in _flatten_jobs(doc):
                rec = _job_from_raw(raw, path=path, host=self.host)
                if rec is not None:
                    yield rec

    def _build_record_set(
        self,
        jobs: list["_JobRecord"],
        revision: dict[str, Any],
    ) -> RecordSet:
        emissions: dict[str, RecordEmission] = {}
        for job in jobs:
            emissions[job.compute_job_id] = job.to_record_emission(
                revision
            )
        return RecordSet(records=emissions)

    def _compute_health(
        self, jobs: list["_JobRecord"],
    ) -> SourceHealth:
        cache_root = self._resolve_cache_root()
        if not cache_root.exists():
            return SourceHealth(
                status="cache_missing",
                mode="cache",
                cache_path=str(cache_root),
                record_count=0,
                reason=(
                    "cluster log cache directory does not exist; run "
                    "`okg <deployment> import-cluster-jobs --host "
                    f"{self.host}` to populate it"
                ),
                checked_at=_now_iso(),
            )

        latest_mtime = 0.0
        for p in cache_root.glob(self.job_log_glob):
            if p.is_file():
                latest_mtime = max(latest_mtime, p.stat().st_mtime)

        now = datetime.now(timezone.utc).timestamp()
        if latest_mtime == 0.0:
            reason = (
                "cluster log cache is empty; the operator-pulled "
                "import has not produced any matching files yet"
            )
        elif now - latest_mtime > self.staleness_warn_seconds:
            reason = (
                f"cluster log cache is stale: latest file is "
                f"{int(now - latest_mtime)}s old (threshold "
                f"{self.staleness_warn_seconds}s)"
            )
        else:
            reason = None

        return SourceHealth(
            status="ok" if reason is None else "skipped_optional",
            mode="cache",
            cache_path=str(cache_root),
            record_count=len(jobs),
            reason=reason,
            checked_at=_now_iso(),
        )


@dataclass
class _JobRecord:
    """One ComputeJob worth of fields parsed from a cluster log."""

    compute_job_id: str
    cluster: str
    job_id: str
    source_path: Path
    submitted_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    wall_clock_seconds: Optional[int] = None
    exit_status: Optional[int] = None
    gpu_hours: Optional[float] = None
    submit_script_ref: Optional[str] = None
    user: Optional[str] = None

    def source_record_id(self) -> dict[str, Any]:
        return {
            "path": str(self.source_path),
            "cluster": self.cluster,
            "job_id": self.job_id,
        }

    def to_facts(
        self, *, source_name: str, host: str, revision: dict[str, Any],
    ) -> Iterator[Any]:
        sri = self.source_record_id()
        attrs: dict[str, Any] = {
            "compute_job_id": self.compute_job_id,
            "cluster": self.cluster,
            "job_id": self.job_id,
        }
        for field_name in (
            "submitted_at",
            "started_at",
            "finished_at",
            "wall_clock_seconds",
            "exit_status",
            "gpu_hours",
            "submit_script_ref",
            "user",
        ):
            v = getattr(self, field_name)
            if v is not None:
                attrs[field_name] = v

        yield NodeFact(
            node_id=self.compute_job_id,
            subtype="compute_job",
            attrs=attrs,
            source_record_id=sri,
            source_revision=revision,
            op="I",
        )

    def to_record_emission(
        self, revision: dict[str, Any]
    ) -> RecordEmission:
        return RecordEmission(
            primary=RecordNode(
                node_id=self.compute_job_id,
                subtype="compute_job",
                source_record_id=self.source_record_id(),
                source_revision=revision,
            ),
        )


def _flatten_jobs(doc: Any) -> Iterator[dict[str, Any]]:
    """Coerce assorted cluster log shapes into per-job mappings.

    Accepted shapes:
    - list of mappings (Slurm sacct's default JSON output, HTCondor
      condor_history)
    - mapping with `jobs: [...]` (operator-rolled YAML)
    - mapping with `data: [...]` (some sacct variants)
    """
    if doc is None:
        return
    if isinstance(doc, list):
        for item in doc:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(doc, dict):
        for key in ("jobs", "data", "records"):
            value = doc.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        yield item
                return
        yield doc


def _job_from_raw(
    raw: dict[str, Any], *, path: Path, host: str,
) -> Optional["_JobRecord"]:
    job_id_raw = _first_present(
        raw, ("job_id", "JobID", "JobId", "ClusterId", "id"),
    )
    if job_id_raw in (None, ""):
        return None
    job_id = str(job_id_raw)

    cluster_raw = _first_present(
        raw, ("cluster", "Cluster", "Partition"),
    )
    cluster = str(cluster_raw) if cluster_raw not in (None, "") else host

    return _JobRecord(
        compute_job_id=f"job:{cluster}:{job_id}",
        cluster=cluster,
        job_id=job_id,
        source_path=path,
        submitted_at=_str_or_none(_first_present(
            raw, ("submitted_at", "Submit", "QDate"),
        )),
        started_at=_str_or_none(_first_present(
            raw, ("started_at", "Start", "JobStartDate"),
        )),
        finished_at=_str_or_none(_first_present(
            raw, ("finished_at", "End", "CompletionDate"),
        )),
        wall_clock_seconds=_int_or_none(_first_present(
            raw, ("wall_clock_seconds", "Elapsed", "RemoteWallClockTime"),
        )),
        exit_status=_int_or_none(_first_present(
            raw, ("exit_status", "ExitCode", "ExitStatus"),
        )),
        gpu_hours=_float_or_none(raw.get("gpu_hours")),
        submit_script_ref=_str_or_none(_first_present(
            raw, ("submit_script_ref", "Command", "Cmd"),
        )),
        user=_str_or_none(_first_present(raw, ("user", "User"))),
    )


def _first_present(
    raw: dict[str, Any], keys: tuple[str, ...],
) -> Any:
    """First non-None value across ``keys``. Unlike ``a or b``, this
    preserves zero / empty-string values when they are explicitly
    present in the source mapping."""
    for key in keys:
        if key in raw and raw[key] is not None:
            return raw[key]
    return None


def _str_or_none(v: Any) -> Optional[str]:
    if v is None or v == "":
        return None
    return str(v)


def _int_or_none(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _float_or_none(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
