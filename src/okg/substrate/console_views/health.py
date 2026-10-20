"""Substrate-health composer for the console Health view.

Answers the operator's "is the substrate healthy right now?" from the pinned
Chronos generation. Every graph signal is read through the request Session's
Chronos checkout; the retired ``okg.nodes_live`` / ``okg.edges_live`` tables
are never touched. Queue metrics come from the DBOS-backed
``okg.v_runtime_queue_health`` view. Invariant summaries and publish status
come from the Chronos generation checkpoint metadata.

The full ``okg doctor`` suite (extraction-library imports, adapter
instantiation, role/nomos checks) is too heavy to run on a page load, and
fabricating a doctor verdict would hide real failures. So this composer derives
its verdict only from the cheap real signals it reads directly; the doctor
suite remains out-of-band.

The verdict + per-check findings map onto the existing HealthBody dashboard
(verdict pill, vitals gauges, finding rows). Direct call, request-time.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from shlex import join as shell_join
from typing import Any, Mapping

import psycopg

from okg.substrate.console_views._chronos import checkout, read_generation_fields
from okg.substrate.console_views.generations import (
    _blocked_reason_code,
    _blocked_summary,
)
from okg.substrate.console_views.console_count_rollup_read import (
    ConsoleCountRollupUnavailable,
    read_console_count_rollup,
    require_console_count_rollup_enabled,
)
from okg.substrate.console_views.ontology_rollup_read import (
    OntologyRollupUnavailable,
    read_ontology_rollup,
)
from okg.substrate.console_views.producer_identity import (
    registry_identities,
    runtime_key_index,
)
from okg.substrate.console_views.today_workspace import _humanize_age
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.mcp.session import Session
from okg.substrate.qa.metrics import _flatten_invariant_summary
from okg.substrate.runtime_dlq import RuntimeDLQAuthorityUnavailable
from okg.substrate.runtime_queue_health import (
    QUEUE_DEPTH_KINDS,
    summarize_runtime_queue_health,
)


# Soft backlog threshold: a healthy worker drains the partition queue, so a
# large standing backlog is worth a warn (not a hard fail).
logger = logging.getLogger(__name__)

_QUEUE_BACKLOG_WARN = 200

# The generation reads this composer makes, and the metadata keys each one
# projects (issue #2113). Both used to be `store.list_generations` calls,
# which have no LIMIT and no projection: on okg-workspace each moved all 88
# checkpoints' full metadata -- 131 MB -- to print ten rows.
#
# The recent-attempts panel prints the newest eight attempts and counts the
# blocked ones among the newest ten; `_publication_attempt` reads these four.
HEALTH_HISTORY_METADATA_KEYS: tuple[str, ...] = (
    "status",
    "target_generation_id",
    "blocked_reason",
    "projection_deltas",
)
HEALTH_HISTORY_WINDOW = 10
# The previous published generation's recorded counts, read ONLY when the
# pinned generation recorded no `projection_deltas`. Read through
# `_metadata_int`, so the projection guard cannot see these literals; the
# behaviour tests pin them instead.
HEALTH_PREVIOUS_METADATA_KEYS: tuple[str, ...] = (
    "node_count",
    "nodes_live",
    "edge_count",
    "edges_live",
)
# The pinned generation's own fields (issue #2546). This read used to be
# `store.get_generation`, which moves the whole document: 8.7 MB on
# okg-workspace, nearly all of it `invariant_summary.frontier_node_ids`. The
# top-level keys, then the keys taken from inside `invariant_summary`
# (what `_flatten_invariant_summary` reads) and `projection_deltas` (what
# `_projection_delta` reads).
HEALTH_CURRENT_METADATA_KEYS: tuple[str, ...] = (
    "status",
    "node_count",
    "nodes_live",
    "edge_count",
    "edges_live",
    "total_duration_ms",
    "excluded_sources",
)
HEALTH_CURRENT_NESTED_METADATA_KEYS: dict[str, tuple[str, ...]] = {
    "invariant_summary": (
        "cross_source",
        "source_local",
        "substrate_integrity",
        "total_duration_ms",
        "total_budget_exceeded",
    ),
    "projection_deltas": (
        "nodes_added",
        "nodes_retracted",
        "edges_added",
        "edges_retracted",
    ),
}
# How many of the newest checkpoints the previous-published read scans. The
# pinned generation is normally the newest published one; the window only has
# to reach past the blocked attempts published after it.
HEALTH_PREVIOUS_SCAN_WINDOW = 32
_RUNTIME_QUEUE_READ_TIMEOUT_MS = 5_000
_RESOURCE_STALE_AFTER_SECONDS = 15 * 60

_RESOURCE_METRICS = (
    "reachable",
    "load1",
    "cpu_count",
    "mem_used_pct",
    "mem_used_bytes",
    "mem_total_bytes",
    "disk_used_pct",
    "disk_used_bytes",
    "disk_total_bytes",
)

_SOURCE_PROGRESS_SQL = """
SELECT source_name, max(updated_at)
  FROM okg.source_progress
 WHERE deployment = %(deployment)s
 GROUP BY source_name
"""

_RESOURCE_LATEST_SQL = """
SELECT DISTINCT ON (node_id, metric)
       node_id,
       metric,
       value,
       observed_at,
       EXTRACT(EPOCH FROM (now() - observed_at))::double precision
  FROM okg.node_series
 WHERE deployment_name = %(deployment)s
   AND metric = ANY(%(metrics)s)
 ORDER BY node_id, metric, observed_at DESC, sample_id DESC
"""

_RESOURCE_HISTORY_SQL = """
WITH host_buckets AS (
    SELECT date_bin(
               make_interval(secs => %(bucket_seconds)s),
               observed_at,
               TIMESTAMPTZ '2001-01-01'
           ) AS bucket_start,
           node_id,
           avg(value) FILTER (WHERE metric = 'load1') AS load1,
           avg(value) FILTER (WHERE metric = 'cpu_count') AS cpu_count,
           avg(value) FILTER (WHERE metric = 'mem_used_pct') AS mem_used_pct,
           avg(value) FILTER (WHERE metric = 'mem_used_bytes') AS mem_used_bytes,
           avg(value) FILTER (WHERE metric = 'mem_total_bytes') AS mem_total_bytes,
           avg(value) FILTER (WHERE metric = 'disk_used_pct') AS disk_used_pct,
           avg(value) FILTER (WHERE metric = 'disk_used_bytes') AS disk_used_bytes,
           avg(value) FILTER (WHERE metric = 'disk_total_bytes') AS disk_total_bytes
      FROM okg.node_series
     WHERE deployment_name = %(deployment)s
       AND observed_at >= %(window_start)s
       AND observed_at <= %(window_end)s
       AND metric = ANY(%(metrics)s)
     GROUP BY bucket_start, node_id
)
SELECT bucket_start,
       100.0 * sum(load1) / NULLIF(sum(cpu_count), 0) AS cpu_avg_1m_pct,
       (array_agg(mem_used_bytes ORDER BY mem_used_pct DESC NULLS LAST, node_id)
          FILTER (WHERE mem_used_pct IS NOT NULL
                    AND mem_used_bytes IS NOT NULL
                    AND mem_total_bytes IS NOT NULL))[1] AS memory_used_bytes,
       (array_agg(mem_total_bytes ORDER BY mem_used_pct DESC NULLS LAST, node_id)
          FILTER (WHERE mem_used_pct IS NOT NULL
                    AND mem_used_bytes IS NOT NULL
                    AND mem_total_bytes IS NOT NULL))[1] AS memory_total_bytes,
       (array_agg(disk_used_bytes ORDER BY disk_used_pct DESC NULLS LAST, node_id)
          FILTER (WHERE disk_used_pct IS NOT NULL
                    AND disk_used_bytes IS NOT NULL
                    AND disk_total_bytes IS NOT NULL))[1] AS disk_used_bytes,
       (array_agg(disk_total_bytes ORDER BY disk_used_pct DESC NULLS LAST, node_id)
          FILTER (WHERE disk_used_pct IS NOT NULL
                    AND disk_used_bytes IS NOT NULL
                    AND disk_total_bytes IS NOT NULL))[1] AS disk_total_bytes,
       count(*) FILTER (WHERE load1 IS NOT NULL AND cpu_count IS NOT NULL) AS cpu_hosts,
       count(*) FILTER (WHERE mem_used_pct IS NOT NULL
                          AND mem_used_bytes IS NOT NULL
                          AND mem_total_bytes IS NOT NULL) AS memory_hosts,
       count(*) FILTER (WHERE disk_used_pct IS NOT NULL
                          AND disk_used_bytes IS NOT NULL
                          AND disk_total_bytes IS NOT NULL) AS disk_hosts,
       (array_agg(node_id ORDER BY mem_used_pct DESC NULLS LAST)
          FILTER (WHERE mem_used_pct IS NOT NULL
                    AND mem_used_bytes IS NOT NULL
                    AND mem_total_bytes IS NOT NULL))[1] AS memory_host,
       (array_agg(node_id ORDER BY disk_used_pct DESC NULLS LAST)
          FILTER (WHERE disk_used_pct IS NOT NULL
                    AND disk_used_bytes IS NOT NULL
                    AND disk_total_bytes IS NOT NULL))[1] AS disk_host
  FROM host_buckets
 GROUP BY bucket_start
 ORDER BY bucket_start
"""

_DATABASE_SIZE_HISTORY_SQL = """
SELECT date_bin(
           make_interval(secs => %(bucket_seconds)s),
           observed_at,
           TIMESTAMPTZ '2001-01-01'
       ) AS bucket_start,
       (array_agg(value ORDER BY observed_at DESC, sample_id DESC))[1] AS value
  FROM okg.node_series
 WHERE deployment_name = %(deployment)s
   AND metric = 'db_size_bytes'
   AND observed_at >= %(window_start)s
   AND observed_at <= %(window_end)s
 GROUP BY bucket_start
 ORDER BY bucket_start
"""


def _verdict(checks: list[dict[str, Any]]) -> str:
    if any(c["severity"] == "fail" for c in checks):
        return "failing"
    if any(c["severity"] == "warn" for c in checks):
        return "degraded"
    return "healthy"


def _runtime_queue_snapshot(cur: Any) -> dict[str, Any]:
    """Return console queue health using active runtime work as the signal."""
    return summarize_runtime_queue_health(cur)


#: The runtime queue's reason when the deployment has no DBOS runtime schema
#: at all. A deployment built by batch ingest (Archiv3 `cern-r3final`) never
#: ran a runtime worker, so there is no queue to observe: that is a gap in
#: what this read can see, not a queue that failed.
RUNTIME_SCHEMA_ABSENT = "runtime_schema_absent"


def _unavailable_runtime_queue_snapshot(
    reason: str, *, observed: bool = True,
) -> dict[str, Any]:
    """The queue snapshot when no queue measurement is available.

    ``observed`` is False only when there is no runtime queue to observe
    (``RUNTIME_SCHEMA_ABSENT``); every other unavailable snapshot is a queue
    that exists and could not be read.
    """
    return {
        "ok": False,
        "status": "unavailable",
        "observed": observed,
        "source": "okg.v_runtime_queue_health + dbos.workflow_status",
        "reason": reason,
        "queue_depth": {kind: None for kind in QUEUE_DEPTH_KINDS},
        "active_queue_depth": None,
        "dlq_count": None,
        "terminal_history_dlq_count": None,
        "runtime_failed_count": None,
        "current_runtime_dlq": {
            "status": "unavailable",
            "current_count": None,
            "current_failures": [],
        },
        "runtime_cancelled_count": None,
        "runtime_queue_health": [],
        "runtime_status_counts": {},
        "runtime_pending_count": None,
        "runtime_waiting_count": None,
        "runtime_delayed_count": None,
        "runtime_running_count": None,
        "terminal_history_count": None,
        "oldest_active_age_seconds": None,
        "newest_active_age_seconds": None,
    }


def _safe_runtime_queue_snapshot(conn: Any) -> dict[str, Any]:
    """Contain queue-history latency/failure without hiding unavailable evidence.

    The runtime schema is checked FIRST. Without `dbos.workflow_status` the
    queue view still answers -- one `unavailable` row of zero counts -- and
    those zeros are not measurements. Such a deployment has no runtime queue,
    so the snapshot says it was not observed rather than reading the view.
    """
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('dbos.workflow_status')")
                row = cur.fetchone()
                if row is None or row[0] is None:
                    return _unavailable_runtime_queue_snapshot(
                        RUNTIME_SCHEMA_ABSENT, observed=False,
                    )
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (f"{_RUNTIME_QUEUE_READ_TIMEOUT_MS}ms",),
                )
                return _runtime_queue_snapshot(cur)
    except RuntimeDLQAuthorityUnavailable:
        return _unavailable_runtime_queue_snapshot(
            "runtime_queue_history_bound_exceeded"
        )
    except psycopg.errors.QueryCanceled:
        return _unavailable_runtime_queue_snapshot("runtime_queue_read_timed_out")
    except psycopg.Error as exc:
        return _unavailable_runtime_queue_snapshot(
            f"runtime_queue_read_failed:{type(exc).__name__}"
        )


def _resource_snapshot(conn: Any, *, deployment_name: str) -> dict[str, Any]:
    """Return live operational host readings plus current database size.

    Resource samples intentionally live outside Chronos. They describe the
    runtime serving the selected deployment now, so their observation time is
    returned explicitly instead of pretending they share the graph pin.
    """
    omissions: list[str] = []
    hosts: dict[str, dict[str, Any]] = {}
    database_size_bytes: int | None = None
    bucket_seconds = 15 * 60
    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(hours=24)
    series: dict[str, dict[str, Any]] = {
        "cpu_avg_1m_pct": {
            "unit": "percent",
            "aggregation": "1m runnable demand / reporting CPU capacity",
            "state": "no_data",
            "points": [],
        },
        "memory_used_bytes": {
            "unit": "bytes",
            "aggregation": "used bytes on the highest-utilization host",
            "state": "no_data",
            "points": [],
        },
        "disk_used_bytes": {
            "unit": "bytes",
            "aggregation": "used bytes on the highest-utilization host",
            "state": "no_data",
            "points": [],
        },
        "database_size_bytes": {
            "unit": "bytes",
            "aggregation": "latest recorded size in each bucket",
            "state": "no_data",
            "points": [],
        },
    }

    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    _RESOURCE_LATEST_SQL,
                    {"deployment": deployment_name, "metrics": list(_RESOURCE_METRICS)},
                )
                for node_id, metric, value, observed_at, staleness_seconds in cur.fetchall():
                    host = hosts.setdefault(str(node_id), {"node_id": str(node_id), "metrics": {}})
                    host["metrics"][str(metric)] = {
                        "value": float(value) if value is not None else None,
                        "observed_at": observed_at.isoformat() if observed_at is not None else None,
                        "staleness_seconds": (
                            float(staleness_seconds) if staleness_seconds is not None else None
                        ),
                    }
    except psycopg.Error as exc:
        omissions.append(f"host_resource_read_failed:{type(exc).__name__}")

    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    _RESOURCE_HISTORY_SQL,
                    {
                        "deployment": deployment_name,
                        "window_start": window_start,
                        "window_end": window_end,
                        "bucket_seconds": bucket_seconds,
                        "metrics": list(_RESOURCE_METRICS[1:]),
                    },
                )
                for (
                    bucket_start,
                    cpu_value,
                    memory_value,
                    memory_capacity,
                    disk_value,
                    disk_capacity,
                    cpu_hosts,
                    memory_hosts,
                    disk_hosts,
                    memory_host,
                    disk_host,
                ) in cur.fetchall():
                    for metric, value, capacity, reporting_hosts, subject in (
                        ("cpu_avg_1m_pct", cpu_value, None, cpu_hosts, None),
                        (
                            "memory_used_bytes",
                            memory_value,
                            memory_capacity,
                            memory_hosts,
                            memory_host,
                        ),
                        (
                            "disk_used_bytes",
                            disk_value,
                            disk_capacity,
                            disk_hosts,
                            disk_host,
                        ),
                    ):
                        if value is None:
                            continue
                        series[metric]["points"].append(
                            {
                                "observed_at": bucket_start.isoformat(),
                                "value": float(value),
                                "reporting_hosts": int(reporting_hosts),
                                "subject": str(subject) if subject is not None else None,
                                "capacity": float(capacity) if capacity is not None else None,
                            }
                        )
                for metric in ("cpu_avg_1m_pct", "memory_used_bytes", "disk_used_bytes"):
                    if series[metric]["points"]:
                        series[metric]["state"] = "data"
    except psycopg.Error as exc:
        omissions.append(f"host_resource_history_read_failed:{type(exc).__name__}")

    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT pg_database_size(current_database())")
                row = cur.fetchone()
                database_size_bytes = int(row[0]) if row and row[0] is not None else None
    except psycopg.Error as exc:
        omissions.append(f"database_size_read_failed:{type(exc).__name__}")

    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    _DATABASE_SIZE_HISTORY_SQL,
                    {
                        "deployment": deployment_name,
                        "window_start": window_start,
                        "window_end": window_end,
                        "bucket_seconds": bucket_seconds,
                    },
                )
                for bucket_start, value in cur.fetchall():
                    if value is None:
                        continue
                    series["database_size_bytes"]["points"].append(
                        {
                            "observed_at": bucket_start.isoformat(),
                            "value": float(value),
                            "reporting_hosts": None,
                        }
                    )
        if series["database_size_bytes"]["points"]:
            series["database_size_bytes"]["state"] = "data"
        elif database_size_bytes is not None:
            series["database_size_bytes"]["state"] = "current_only"
            series["database_size_bytes"]["points"].append(
                {
                    "observed_at": window_end.isoformat(),
                    "value": float(database_size_bytes),
                    "reporting_hosts": None,
                }
            )
            omissions.append("database_size_history_starting")
    except psycopg.Error as exc:
        omissions.append(f"database_size_history_read_failed:{type(exc).__name__}")

    ordered_hosts = [hosts[key] for key in sorted(hosts)]
    if not ordered_hosts:
        omissions.append("no_host_resource_samples")
    if database_size_bytes is None:
        omissions.append("database_size_unavailable")

    available_parts = int(bool(ordered_hosts)) + int(database_size_bytes is not None)
    resource_read_failed = any("_read_failed:" in item for item in omissions)
    oldest_host_age = max(
        (
            float(reading["staleness_seconds"])
            for host in ordered_hosts
            for reading in host["metrics"].values()
            if reading.get("staleness_seconds") is not None
        ),
        default=None,
    )
    if available_parts == 0:
        status = "unavailable"
    elif resource_read_failed or (available_parts == 1 and bool(ordered_hosts)):
        status = "partial"
    elif not ordered_hosts:
        status = "missing"
    elif oldest_host_age is not None and oldest_host_age > _RESOURCE_STALE_AFTER_SECONDS:
        status = "stale"
    else:
        status = "available"
    return {
        "status": status,
        "source": "okg.node_series + pg_database_size",
        "hosts": ordered_hosts,
        "database_size_bytes": database_size_bytes,
        "window": {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
            "bucket_seconds": bucket_seconds,
        },
        "series": series,
        "omissions": omissions,
    }


def _count_graph_rows(graph: Any, table: str) -> int:
    row = graph.query(f"SELECT count(*) AS n FROM {table}")[0]
    return int(row["n"] or 0)


def _metadata_int(metadata: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = metadata.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _recorded_ms(value: Any) -> float | None:
    """A recorded duration in milliseconds, or None when none was recorded.

    Only a real number counts. A missing key, null, a boolean or a string is
    "not recorded" -- never 0, which would read as a measured instant.
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def _projection_delta(metadata: dict[str, Any], prefix: str) -> int | None:
    raw = metadata.get("projection_deltas")
    if not isinstance(raw, dict):
        return None
    added = _metadata_int(raw, f"{prefix}_added")
    retracted = _metadata_int(raw, f"{prefix}_retracted")
    if added is None and retracted is None:
        return None
    return (added or 0) - (retracted or 0)


def _parse_checkpoint_created_at(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _source_display_name(source: str, deployment_name: str) -> str:
    prefix = f"{deployment_name}."
    return source[len(prefix):] if source.startswith(prefix) else source


def _exclusion_spellings(*, name: str, recorded_keys: set[str]) -> set[str]:
    """Every spelling of this row that `excluded_sources` may name.

    Computed in one named place, because it is a contract and not a
    coincidence. `generation.metadata.excluded_sources` is
    `sorted(phase_b.excluded_sources.keys())` (`publisher/publisher.py:10887`),
    and both fillers of that map write the registry NAME: the failing
    invariant's own `source` column (`publisher/gates.py:511`), which
    `invariants.yaml` declares as a bare source name, and Phase A's
    `per_source_status` (`publisher/gates.py:433`), which the live publish
    cycle always passes empty.

    Two spellings, then:

    * The name, which is what the publisher writes.
    * Every key some read actually RECORDED for this row -- the declared
      `ownership_id`, and the `okg.source_progress` keys grouped into it. A
      producer the registry does not name has no other identity at all, and a
      seeded generation that excludes one names it by that key
      (`test_compose_health_includes_excluded_source_rows`).

    What is deliberately NOT here is `<deployment>.<name>`. This composer used
    to build that string as the row's key and match it, which is why the set
    changes for a source whose declared id is spelled differently --
    `archiv3.cmssw_releases`, not `archiv3.cmssw-releases`. Constructing a
    runtime key from a producer name is the rule this change exists to end,
    `test_no_console_view_builds_a_runtime_key_from_a_name` holds that no
    module under `console_views` does it, and a declared or recorded key is a
    real spelling of a real producer where a constructed one is a guess. This
    is the third behaviour change of `operator-console-live-sources` and is
    named in the change's `why`.
    """
    return {name} | recorded_keys


def _source_status(
    *,
    spellings: set[str],
    has_progress: bool,
    excluded: set[str],
) -> tuple[str, str, str]:
    if excluded & spellings:
        return (
            "excluded",
            "Excluded from this graph version.",
            "Confirm this source is intentionally excluded before relying on it.",
        )
    if has_progress:
        # A progress row says this source recorded an update, not how its
        # latest run ended or whether its watermark is within its cadence.
        # This pinned read has neither, so it does not call the source
        # healthy on the row alone (issue #2205); the producers read judges
        # health from the recorded outcome.
        return (
            "unknown",
            "An update is recorded; this read records no outcome for it.",
            "Read this source's state on Sources.",
        )
    return (
        "unknown",
        "No recent source update is recorded.",
        "Confirm this source has run at least once.",
    )


#: `source_breakdown.status_basis` when each row's status is the producers
#: read's own health for that source -- what the Sources page shows.
STATUS_BASIS_PRODUCER_STATE = "producer_state"
#: `source_breakdown.status_basis` when the producers read was not supplied:
#: the status then says only whether a progress row is recorded.
STATUS_BASIS_SOURCE_PROGRESS = "source_progress"


def producer_states_from_source_diagnostics(
    payload: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    """Each source's state as `console/source-diagnostics` composed it.

    Keyed by the row's `source_key` (the declared ownership id). The same
    payload is what `console/producers` turns into its source rows, so a
    breakdown built from this says what the Sources page says: `health` is
    the row's `health.status`, `state` and `state_reason` are the recorded
    convergence state and the reason code recorded beside it (for example
    `blocked` / `completed_scope_watermark_stale`).
    """
    states: dict[str, dict[str, Any]] = {}
    for row in payload.get("sources") or []:
        if not isinstance(row, Mapping):
            continue
        key = row.get("source_key")
        if not key:
            continue
        row_health = row.get("health")
        workflow = row.get("workflow")
        row_health = row_health if isinstance(row_health, Mapping) else {}
        workflow = workflow if isinstance(workflow, Mapping) else {}
        states[str(key)] = {
            "health": row_health.get("status"),
            "state": workflow.get("state"),
            "state_reason": workflow.get("reason"),
        }
    return states


def _health_source_breakdown(
    conn: Any,
    *,
    deployment_name: str,
    excluded_sources: list[str],
    producer_states: Mapping[str, Mapping[str, Any]] | None = None,
    producer_states_unavailable_reason: str | None = None,
) -> dict[str, Any]:
    """One row per source, and a summary of them.

    With ``producer_states`` (from `producer_states_from_source_diagnostics`)
    each configured source's status IS the producers read's health for it, so
    Health and Sources cannot disagree about the same source (Archiv3 served
    six healthy here beside three stale there). Without it the status is the
    older progress-row rule, and ``status_basis`` says so together with
    ``producer_states_unavailable_reason`` -- the caller's reason the producers
    read was not supplied. An exclusion recorded on the generation outranks
    either basis.
    """
    excluded = {str(source) for source in excluded_sources}
    # One configured producer is one row. Its runtime key is the ownership_id
    # the registry declares, not a string built from its document name: joining
    # on the latter listed a source twice, once empty under its configured name
    # and once under the key its runs were recorded against.
    identities = registry_identities(deployment_name)
    by_key = runtime_key_index(identities)

    last_update: dict[str, Any] = {}
    with conn.cursor() as cur:
        cur.execute(_SOURCE_PROGRESS_SQL, {"deployment": deployment_name})
        for source_name, updated_at in cur.fetchall():
            last_update[str(source_name)] = updated_at

    def _group(recorded: str) -> str:
        """The group a recorded key belongs to: a configured producer, or itself.

        The registry's declared identity answers first and answers alone for
        a configured producer. A key nothing configures is its OWN group. It
        is not grouped by its prefix-stripped display name, because two
        distinct recorded keys can strip to one -- `archiv3.ghost` and
        `ghost` -- and folding them attributes one producer's runs to
        another by a name-derived rule, which is the rule `producer_identity`
        exists to end. `compose_sources` refuses the same fold
        (`sources.py`), and the two composers must not disagree about how
        many producers a deployment has.
        """
        identity = by_key.get(recorded)
        return identity.name if identity is not None else recorded

    # group -> the recorded keys that belong to it.
    keys_for: dict[str, set[str]] = {
        identity.name: {identity.ownership_id, identity.name}
        for identity in identities
    }
    for recorded in list(last_update) + sorted(excluded):
        keys_for.setdefault(_group(recorded), set()).add(recorded)

    by_name = {identity.name: identity for identity in identities}
    rows: list[dict[str, Any]] = []

    for group in sorted(keys_for):
        recorded_keys = keys_for[group]
        identity = by_name.get(group)
        # A configured producer prints its registry name; an unconfigured key
        # prints the shortened key, the same display `compose_sources` gives
        # it. Two such rows may read alike and are two rows all the same,
        # told apart by the `source_key` beside them.
        name = (
            identity.name if identity is not None
            else _source_display_name(group, deployment_name)
        )
        # The declared ownership_id, or -- for a producer no registry names --
        # a key some read actually recorded. Never a key built by joining the
        # deployment name to a producer name: constructing `<deployment>.<name>`
        # is exactly the rule that listed one producer twice, and for a
        # producer outside the registry there is no declared id to build from.
        key = (
            identity.ownership_id
            if identity is not None
            else sorted(recorded_keys)[0]
        )
        stamps = [
            last_update[recorded]
            for recorded in sorted(recorded_keys)
            if last_update.get(recorded) is not None
        ]
        updated_at = max(stamps) if stamps else None
        status, issue, next_check = _source_status(
            spellings=_exclusion_spellings(
                name=name, recorded_keys=set(recorded_keys),
            ),
            has_progress=updated_at is not None,
            excluded=excluded,
        )
        state: Any = None
        state_reason: Any = None
        if producer_states is not None and status != "excluded":
            recorded = next(
                (
                    producer_states[candidate]
                    for candidate in [key, *sorted(recorded_keys)]
                    if candidate in producer_states
                ),
                None,
            )
            if recorded is None:
                status = "unknown"
                issue = "The producers read returned no row for this source."
                next_check = "Read this source on Sources."
            else:
                state = recorded.get("state")
                state_reason = recorded.get("state_reason")
                status = str(recorded.get("health") or "unknown")
                if status == "healthy":
                    issue, next_check = "", "No action needed."
                else:
                    issue = (
                        f"The producers read records this source as {status}"
                        + (f" ({state_reason})." if state_reason else ".")
                    )
                    next_check = "Read this source on Sources."
        rows.append({
            "name": name,
            "source_key": key,
            "status": status,
            # The recorded convergence state and its reason code, as the
            # producers read serves them (`workflow.state`, `workflow.reason`);
            # null under the progress basis, which records neither.
            "state": state,
            "state_reason": state_reason,
            "included": status != "excluded",
            "last_update": _humanize_age(updated_at) if updated_at else None,
            "latest_issue": issue or None,
            "next_check": next_check,
        })

    rows.sort(
        key=lambda row: (
            row["status"] == "healthy",
            row["status"] == "unknown",
            row["name"],
            # Two unconfigured keys can share a display name; their recorded
            # key is what orders them, so the list is stable.
            row["source_key"],
        )
    )
    summary = {
        "total": len(rows),
        "healthy": sum(1 for row in rows if row["status"] == "healthy"),
        "needs_review": sum(1 for row in rows if row["status"] != "healthy"),
        "unknown": sum(1 for row in rows if row["status"] == "unknown"),
        "excluded": sum(1 for row in rows if row["status"] == "excluded"),
    }
    return {
        "source_count": len(rows),
        "status_basis": (
            STATUS_BASIS_PRODUCER_STATE if producer_states is not None
            else STATUS_BASIS_SOURCE_PROGRESS
        ),
        "status_basis_unavailable_reason": (
            None if producer_states is not None
            else producer_states_unavailable_reason
            or "producers_read_not_supplied"
        ),
        "summary": summary,
        "sources": rows,
    }


def _critical_read_prerequisites(
    conn: Any,
    *,
    deployment_name: str,
    generation_id: int | str,
) -> dict[str, Any]:
    """Probe the maintained data required by the operator's critical reads.

    Health must use the same generation-pinned read functions its sibling reads
    use. A typed unavailable result is returned as data so the Health page can
    remain readable and actionable while refusing to claim the graph is usable.

    ``affected_reads`` names reads that EXIST. The Graph view's reads left that
    list in issue #1924; the ontology rollup is still maintained at publish time
    but has no reader left, which this probe states rather than implying a Graph
    view that is gone.
    """
    prerequisites: list[dict[str, Any]] = []

    try:
        require_console_count_rollup_enabled(generation_id=generation_id)
        counts = read_console_count_rollup(conn, generation_id=generation_id)
        prerequisites.append({
            "id": "console_count_rollup",
            "status": "available",
            "reason": None,
            "affected_reads": ["Sources", "Search records", "Subtype lists"],
            "row_count": len(counts),
            "next_check": "No action needed.",
        })
    except ConsoleCountRollupUnavailable as exc:
        disabled = exc.reason == "count_rollup_disabled"
        prerequisites.append({
            "id": "console_count_rollup",
            "status": "disabled" if disabled else "unavailable",
            "reason": exc.reason,
            "affected_reads": ["Sources", "Search records", "Subtype lists"],
            "row_count": None,
            "next_check": (
                "Enable maintained count data in the supervised worker, then "
                "publish a corrected graph."
                if disabled
                else "Check maintained count data for this graph version, then "
                "publish a corrected graph."
            ),
        })

    try:
        rollup = read_ontology_rollup(
            conn,
            deployment_name=deployment_name,
            generation_id=generation_id,
        )
        prerequisites.append({
            "id": "ontology_rollup",
            "status": "available",
            "reason": None,
            "affected_reads": [],  # no reader since issue #1924; still maintained
            "row_count": len(rollup.rows),
            "next_check": "No action needed.",
        })
    except OntologyRollupUnavailable as exc:
        prerequisites.append({
            "id": "ontology_rollup",
            "status": "unavailable",
            "reason": exc.reason,
            "affected_reads": [],  # no reader since issue #1924; still maintained
            "row_count": None,
            "next_check": (
                "Check maintained ontology data for this graph version, then "
                "publish a corrected graph."
            ),
        })

    return {
        "generation_id": generation_id,
        "ready": all(row["status"] == "available" for row in prerequisites),
        "prerequisites": prerequisites,
    }


def _publication_attempt(generation: Any, deployment: str, branch: str) -> dict[str, Any]:
    """Describe recorded evidence without substituting a compatibility ID."""
    metadata = dict(generation.metadata or {})
    stamp = _parse_checkpoint_created_at(generation.created_at)
    deltas = metadata.get("projection_deltas")
    def recorded_delta(prefix: str) -> int | None:
        if not isinstance(deltas, dict):
            return None
        added = _metadata_int(deltas, f"{prefix}_added")
        retracted = _metadata_int(deltas, f"{prefix}_retracted")
        return added - retracted if added is not None and retracted is not None else None

    return {
        "generation_id": generation.checkpoint_id,
        "compat_generation_id": metadata.get("target_generation_id"),
        "status": str(metadata.get("status") or "unknown"),
        "age": _humanize_age(stamp) if stamp else None,
        "created_at": stamp.isoformat() if stamp else None,
        "blocked_reason": _blocked_summary(metadata.get("blocked_reason")),
        # Why `blocked_reason` is null on a blocked attempt; null otherwise.
        "blocked_reason_unavailable_reason": (
            _blocked_reason_code(metadata.get("blocked_reason"))[1]
        ),
        "describe_command": shell_join([
            "okg", "generation", "describe", str(generation.checkpoint_id),
            "--deployment", deployment, "--branch", branch,
        ]),
        "nodes_delta": recorded_delta("nodes"),
        "edges_delta": recorded_delta("edges"),
    }


def _recent_attempts(store: Any, *, deployment_name: str, branch: str) -> list[Any]:
    """The newest `HEALTH_HISTORY_WINDOW` generation attempts, newest first.

    Exactly `list(reversed(list_generations(status=None)[-10:]))`, read as a
    bounded, projected window. The window counts every checkpoint on the
    branch, so when other checkpoints fill it and fewer than ten of this
    deployment's attempts come back, older attempts may exist that it never
    saw. That case takes the NAMED full fetch below, logged, rather than
    printing a shorter history than the deployment has.
    """
    recent = store.recent_generations(
        deployment_name=deployment_name,
        branch=branch,
        status=None,
        keys=HEALTH_HISTORY_METADATA_KEYS,
        scan_window=HEALTH_HISTORY_WINDOW,
    )
    if len(recent.rows) >= HEALTH_HISTORY_WINDOW or not recent.window_exhausted:
        return list(recent.rows[:HEALTH_HISTORY_WINDOW])
    return _recent_attempts_full_scan(
        store, deployment_name=deployment_name, branch=branch,
        seen=len(recent.rows), scanned=recent.scanned,
    )


def _recent_attempts_full_scan(
    store: Any, *, deployment_name: str, branch: str, seen: int, scanned: int,
) -> list[Any]:
    """Named fallback: the full-metadata fetch, when the window fell short."""
    logger.warning(
        "health_recent_attempts_outside_window: %d of the %d newest checkpoints "
        "on branch %r are %s's attempts; reading the whole history",
        seen, scanned, branch, deployment_name,
    )
    history = store.list_generations(
        deployment_name=deployment_name, branch=branch, status=None,
    )
    return list(reversed(history[-HEALTH_HISTORY_WINDOW:]))


def _history_total(store: Any, *, deployment_name: str, branch: str) -> int:
    """How many attempts the branch holds: `len(list_generations(status=None))`.

    Served as `history_total` beside the eight-row history ("Showing 8 of
    88"). Counted in one statement by `count_generations`; a backend that
    cannot count takes the NAMED full fetch below, logged, not a zero.
    """
    total = store.count_generations(deployment_name=deployment_name, branch=branch)
    if total is not None:
        return int(total)
    logger.warning(
        "health_history_total_full_scan: this backend cannot count "
        "generations in one statement; reading branch %r's whole history",
        branch,
    )
    return len(store.list_generations(
        deployment_name=deployment_name, branch=branch, status=None,
    ))


def _previous_published(
    store: Any, *, deployment_name: str, branch: str, generation_id: str,
) -> Any | None:
    """The published generation immediately before `generation_id`, or None.

    Exactly what the old walk over `list_generations(status="published")`
    found, read as a bounded window of the newest checkpoints. None means
    what it meant there: `generation_id` is the first published generation,
    or not a published generation on this branch at all -- and the window
    can only say either when it reached the start of the history. When the
    pinned generation is missing from an exhausted window, or is its oldest
    row, the predecessor lies outside it, and the NAMED full fetch below
    answers instead.
    """
    recent = store.recent_generations(
        deployment_name=deployment_name,
        branch=branch,
        status="published",
        keys=HEALTH_PREVIOUS_METADATA_KEYS,
        scan_window=HEALTH_PREVIOUS_SCAN_WINDOW,
    )
    rows = recent.rows
    index = next(
        (i for i, row in enumerate(rows) if row.checkpoint_id == generation_id),
        None,
    )
    if index is not None and index + 1 < len(rows):
        return rows[index + 1]
    if not recent.window_exhausted:
        return None
    return _previous_published_full_scan(
        store, deployment_name=deployment_name, branch=branch,
        generation_id=generation_id, scanned=recent.scanned,
    )


def _previous_published_full_scan(
    store: Any, *, deployment_name: str, branch: str, generation_id: str,
    scanned: int,
) -> Any | None:
    """Named fallback: the full-metadata fetch, when the window cannot answer."""
    logger.warning(
        "health_previous_published_outside_window: the predecessor of %s is "
        "not inside the %d newest checkpoints on branch %r; reading the whole "
        "published history",
        generation_id, scanned, branch,
    )
    published = store.list_generations(
        deployment_name=deployment_name, branch=branch, status="published",
    )
    for idx, candidate in enumerate(published):
        if candidate.checkpoint_id == generation_id and idx > 0:
            return published[idx - 1]
    return None


def compose_health(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
    producer_states: Mapping[str, Mapping[str, Any]] | None = None,
    producer_states_unavailable_reason: str | None = None,
) -> dict[str, Any]:
    """Substrate-health dashboard payload for the pinned Chronos generation.

    ``producer_states`` is the producers read's per-source state
    (`producer_states_from_source_diagnostics`); the route supplies it so the
    source breakdown agrees with Sources. See `_health_source_breakdown`.
    """
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    # The fields below, projected; never the whole document, whose
    # `invariant_summary` carries a list of every node id (8.7 MB for
    # okg-workspace's pinned generation, issue #2546). Same guards as
    # `get_generation`: compacted raises `GenerationCompacted`, missing
    # raises `BranchNotFoundError`.
    _checkpoint_id, current_created_at, current_meta = read_generation_fields(
        conn,
        session.generation_id,
        HEALTH_CURRENT_METADATA_KEYS,
        HEALTH_CURRENT_NESTED_METADATA_KEYS,
    )
    current_status = str(current_meta.get("status") or "unknown")
    published_at = _parse_checkpoint_created_at(current_created_at)

    nodes_live = _metadata_int(current_meta, "node_count", "nodes_live")
    edges_live = _metadata_int(current_meta, "edge_count", "edges_live")
    if nodes_live is None or edges_live is None:
        graph = checkout(conn, session)
        # derivation-read-lint: allow #444 unstamped health fallback only.
        nodes_live = _count_graph_rows(graph, "okg.graph_nodes")
        # derivation-read-lint: allow #444 unstamped health fallback only.
        edges_live = _count_graph_rows(graph, "okg.graph_edges")

    nodes_delta = _projection_delta(current_meta, "nodes")
    edges_delta = _projection_delta(current_meta, "edges")
    if nodes_delta is None or edges_delta is None:
        # Read only here: a generation that recorded its own deltas needs no
        # predecessor, and every checkpoint on okg-workspace records them.
        previous = _previous_published(
            store,
            deployment_name=deployment_name,
            branch=session.branch_name,
            generation_id=session.generation_id,
        )
        if previous is None:
            nodes_delta = nodes_live
            edges_delta = edges_live
        else:
            previous_meta = dict(previous.metadata or {})
            previous_nodes = _metadata_int(
                previous_meta,
                "node_count",
                "nodes_live",
            )
            previous_edges = _metadata_int(
                previous_meta,
                "edge_count",
                "edges_live",
            )
            if previous_nodes is None or previous_edges is None:
                previous_graph = store.checkout_generation(
                    previous.checkpoint_id,
                    branch=session.branch_name,
                )
                # derivation-read-lint: allow #444 previous unstamped fallback.
                previous_nodes = _count_graph_rows(
                    previous_graph, "okg.graph_nodes",
                )
                # derivation-read-lint: allow #444 previous unstamped fallback.
                previous_edges = _count_graph_rows(
                    previous_graph, "okg.graph_edges",
                )
            nodes_delta = nodes_live - previous_nodes
            edges_delta = edges_live - previous_edges

    recent_generations = _recent_attempts(
        store, deployment_name=deployment_name, branch=session.branch_name,
    )
    blocked = sum(
        1
        for generation in recent_generations
        if (generation.metadata or {}).get("status") == "blocked"
    )
    history = [
        _publication_attempt(generation, deployment_name, session.branch_name)
        for generation in recent_generations[:8]
    ]
    latest_attempt = history[0] if history else None

    invariant_summary = current_meta.get("invariant_summary") or {}
    inv = _flatten_invariant_summary(
        invariant_summary if isinstance(invariant_summary, dict) else {}
    )
    # Durations are read from the metadata itself, not from the flattened
    # summary: the flattener defaults an absent total to 0, and a console
    # must never print a zero for a value nobody measured (issue #2114).
    publish_ms = _recorded_ms(current_meta.get("total_duration_ms"))
    invariant_ms_raw = _recorded_ms(
        invariant_summary.get("total_duration_ms")
        if isinstance(invariant_summary, dict) else None
    )
    invariant_ms = int(invariant_ms_raw) if invariant_ms_raw is not None else None
    excluded_sources = [
        str(s) for s in (current_meta.get("excluded_sources") or [])
    ]
    source_breakdown = _health_source_breakdown(
        conn,
        deployment_name=deployment_name,
        excluded_sources=excluded_sources,
        producer_states=producer_states,
        producer_states_unavailable_reason=producer_states_unavailable_reason,
    )
    resources = _resource_snapshot(conn, deployment_name=deployment_name)
    critical_reads = _critical_read_prerequisites(
        conn,
        deployment_name=deployment_name,
        generation_id=session.generation_id,
    )

    runtime_queue = _safe_runtime_queue_snapshot(conn)
    # An unavailable queue serves NO counts. The queue view answers zeros
    # beside `availability: unavailable` (it did on Archiv3, next to
    # "dbos.workflow_status missing"), and a zero printed beside "queue
    # evidence unavailable" reads as a measured empty queue. The reason
    # stands in for the numbers.
    queue_unavailable = runtime_queue.get("status") == "unavailable"
    queue_unavailable_reason = (
        str(runtime_queue.get("reason") or "runtime_queue_unavailable")
        if queue_unavailable else None
    )
    queue_depth = {
        k: (int(v) if v is not None and not queue_unavailable else None)
        for k, v in runtime_queue.get("queue_depth", {}).items()
    }
    dlq_raw = None if queue_unavailable else runtime_queue.get("dlq_count")
    dlq = int(dlq_raw) if dlq_raw is not None else None
    terminal_history_dlq_raw = (
        None if queue_unavailable
        else runtime_queue.get("terminal_history_dlq_count")
    )
    terminal_history_dlq = (
        int(terminal_history_dlq_raw)
        if terminal_history_dlq_raw is not None
        else None
    )
    queue_total = (
        sum(value for value in queue_depth.values() if value is not None)
        if queue_depth and all(value is not None for value in queue_depth.values())
        else None
    )

    cs_total = int(inv["cross_source_invariants_total"])
    cs_failed = int(inv["cross_source_invariants_failed"])
    cs_timed_out = int(inv["cross_source_invariants_timed_out"])
    sl_total = int(inv["source_local_invariants_total"])
    sl_failed = int(inv["source_local_invariants_failed"])
    budget_exceeded = bool(inv["total_invariant_budget_exceeded"])
    inv_failed = cs_failed + sl_failed
    inv_total = cs_total + sl_total

    checks: list[dict[str, Any]] = []

    def _check(
        severity: str, code: str, scope: str, summary: str,
        actual: str, expected: str, fix_hint: str,
    ) -> None:
        checks.append({
            "severity": severity, "code": code, "scope": scope,
            "summary": summary, "actual": actual, "expected": expected,
            "fix_hint": fix_hint,
        })

    for prerequisite in critical_reads["prerequisites"]:
        if prerequisite["status"] == "available":
            continue
        affected = ", ".join(prerequisite["affected_reads"])
        reason = str(prerequisite.get("reason") or "unavailable")
        _check(
            "fail",
            f"critical_read_{prerequisite['id']}_unavailable",
            "operator reads",
            f"{affected} cannot load for the current graph version.",
            f"{reason} at {session.generation_id}",
            "maintained data available for this graph version",
            str(prerequisite["next_check"]),
        )

    if latest_attempt and latest_attempt["status"] == "blocked":
        _check(
            "warn", "generation_blocked", "publisher",
            "Latest recorded update was blocked; the published graph is unchanged.",
            latest_attempt["blocked_reason"] or "Blocking reason unavailable.",
            "published update",
            latest_attempt["describe_command"],
        )
    if inv_failed > 0:
        _check(
            "fail", "invariant_failures", "publisher",
            "Phase-B invariants failed for the published generation.",
            f"{inv_failed} failed (cross_source {cs_failed} · source_local {sl_failed})",
            "0 failed",
            "okg generation describe <g>; inspect the failing invariant's rows",
        )
    if cs_timed_out > 0:
        _check(
            "warn", "invariant_timeouts", "publisher",
            "Cross-source invariants timed out within their budget.",
            f"{cs_timed_out} timed out", "0 timed out",
            "raise the invariant budget or narrow the check scope",
        )
    if budget_exceeded:
        _check(
            "warn", "invariant_budget_exceeded", "publisher",
            "Total invariant evaluation exceeded its time budget.",
            (
                f"{invariant_ms} ms (budget exceeded)"
                if invariant_ms is not None
                else "duration not recorded (budget exceeded)"
            ),
            "within budget",
            "profile the slow invariant; raise the budget if legitimate",
        )
    if queue_unavailable and runtime_queue.get("observed") is False:
        # Warn, not fail: nothing failed. The deployment has no runtime queue,
        # so its state is unobserved, and the verdict must not call that a
        # failing check. It is still a check, so the gap stays visible.
        _check(
            "warn", "runtime_queue_not_observed", "runtime",
            "This deployment has no runtime queue, so queue state is not observed.",
            str(runtime_queue.get("reason")),
            "a runtime queue, when this deployment runs a runtime worker",
            "none if this deployment is built by batch ingest; otherwise "
            "start its runtime worker",
        )
    elif queue_unavailable:
        _check(
            "fail", "runtime_queue_unavailable", "runtime",
            "Runtime queue evidence is unavailable.",
            str(runtime_queue.get("reason") or "queue read unavailable"),
            "bounded current queue evidence",
            "inspect DBOS history volume and the runtime queue authority",
        )
    if dlq is not None and dlq > 0:
        _check(
            "warn", "dlq_nonzero", "runtime",
            "Jobs are sitting in the dead-letter queue.",
            f"{dlq} in dead_letter", "0",
            "okg metrics --qa-snapshot; inspect DBOS workflow failures",
        )
    if queue_total is not None and queue_total > _QUEUE_BACKLOG_WARN:
        _check(
            "warn", "queue_backlog", "runtime",
            "The partition queue has a large standing backlog.",
            f"{queue_total} queued/leased", f"<= {_QUEUE_BACKLOG_WARN}",
            "confirm `okg runtime worker` is running and draining",
        )

    return {
        "deployment": deployment_name,
        "generation_id": session.generation_id,
        "compat_generation_id": session.compat_generation_id,
        "generation_status": current_status,
        "generation_age": _humanize_age(published_at) if published_at else None,
        "verdict": _verdict(checks),
        "history": history,
        "history_total": _history_total(
            store, deployment_name=deployment_name, branch=session.branch_name,
        ),
        "latest_attempt": latest_attempt,
        "vitals": {
            "generation_blocked_count": blocked,
            "queue_depth": queue_depth,
            "queue_total": queue_total,
            "dlq_count": dlq,
            "terminal_history_dlq_count": terminal_history_dlq,
            # Null when the queue counts above are measured; otherwise why
            # they are null (`runtime_schema_absent` when there is no queue).
            "queue_unavailable_reason": queue_unavailable_reason,
            "nodes_live": nodes_live,
            "edges_live": edges_live,
            "nodes_live_delta": nodes_delta,
            "edges_live_delta": edges_delta,
            # Null, with the reason beside it, when the checkpoint recorded
            # no publish duration (issue #2114): a 0.0 would read as a
            # measured instant publish. A recorded 0 stays 0.0.
            "publish_wallclock_seconds": (
                publish_ms / 1000.0 if publish_ms is not None else None
            ),
            "publish_wallclock_reason": (
                None if publish_ms is not None else
                "not recorded: this generation's checkpoint metadata carries "
                "no numeric total_duration_ms"
            ),
            "invariants": {
                "cross_source_total": cs_total,
                "cross_source_failed": cs_failed,
                "cross_source_timed_out": cs_timed_out,
                "source_local_total": sl_total,
                "source_local_failed": sl_failed,
                "total": inv_total,
                "failed": inv_failed,
                "budget_exceeded": budget_exceeded,
                "duration_ms": invariant_ms,
                "duration_reason": (
                    None if invariant_ms is not None else
                    "not recorded: this generation's invariant summary "
                    "carries no numeric total_duration_ms"
                ),
            },
            "excluded_sources": excluded_sources,
        },
        "source_breakdown": source_breakdown,
        "resources": resources,
        "critical_reads": critical_reads,
        "checks": checks,
        "doctor_note": (
            "Substrate-state health from the pinned Chronos generation. "
            "The full `okg doctor` suite (extraction/config/role checks) "
            "runs out-of-band, not on page load."
        ),
    }
