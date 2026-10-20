"""Enricher diagnostics for the operator Health view.

The substrate records enricher output under the producer-source convention
``_enricher:<name>``. This composer keeps Health read-only and bounded: current
contribution comes from the pinned Chronos checkout and append-only fact logs;
recent activity comes from generation checkpoint metadata and
``okg.enricher_progress``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence, TYPE_CHECKING

from okg.substrate.admission import admission_mode_awaits_decision
from okg.substrate.console_views._chronos import checkout
from okg.substrate.console_views.console_count_rollup_read import (
    console_count_rollup_enabled,
)
from okg.substrate.console_views.today_workspace import _humanize_age
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    ENRICHER_PRODUCER_FILTER_SQL,
    ENRICHER_PRODUCER_SQL_EXPR,
)
# Issue #1528: the publisher (writer), `okg doctor` and this console view all
# name a withheld run from one vocabulary, so the three cannot drift apart.
from okg.substrate.enrichers.lineage import (
    UNKNOWN_DECLARED_SOURCE,
    withheld_reason_from_status,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal.generations import (
    DEFAULT_ENRICHER_RUNS_WINDOW,
)
from okg.substrate.mcp.session import Session

if TYPE_CHECKING:  # pragma: no cover - typing only
    from okg.substrate.console_views.console_count_rollup_read import (
        ConsoleCountRollupCache,
    )

# How far back the run history looks. The payload serves it by name
# (`enricher_runs_window`) because a bounded history that does not say it
# is bounded reads as a complete one, and a missing run then reads as an
# enricher that never ran.
ENRICHER_RUNS_WINDOW = DEFAULT_ENRICHER_RUNS_WINDOW

# The flag-OFF fallback scans (one row per enricher producer). They use the SAME
# enricher-attribution expression the rollup maintainer stamps (the shared
# ENRICHER_PRODUCER_SQL_EXPR), so the flag-OFF scan and the flag-ON rollup read are
# byte-identical by construction. count(*) == count(DISTINCT node_id/edge_id) under
# the PK, so it matches the rollup's count(*).
# Built via .format from the shared enricher expression/filter the rollup maintainer
# stamps. Kept on one physical line each so the read-lint sees a single candidate
# whose allow marker sits directly above. count(*) == count(DISTINCT id) under the PK.
# derivation-read-lint: allow #444 flag-off enricher fallback; flag-on reads the enricher_node rollup.
_LIVE_NODES_SQL = "SELECT ({0}) AS source, count(*) AS live FROM okg.graph_nodes WHERE {1} GROUP BY 1".format(ENRICHER_PRODUCER_SQL_EXPR, ENRICHER_PRODUCER_FILTER_SQL)  # noqa: E501
# derivation-read-lint: allow #444 flag-off enricher fallback; flag-on reads the enricher_edge rollup.
_LIVE_EDGES_SQL = "SELECT ({0}) AS source, count(*) AS live FROM okg.graph_edges WHERE {1} GROUP BY 1".format(ENRICHER_PRODUCER_SQL_EXPR, ENRICHER_PRODUCER_FILTER_SQL)  # noqa: E501

# The underscore is escaped (issue #2161). In LIKE a bare `_` matches any one
# character, so `'_enricher:%'` has no fixed prefix: the planner cannot turn it
# into an index range and filters every fact-log row instead. The pattern value
# `\_enricher:%` is a literal prefix: backslash is LIKE's default escape. It is
# written as the escape string E'\\_enricher:%' so the value is the same
# whatever standard_conforming_strings says. Postgres folds it to the same
# constant as the plain `'\_enricher:%'` predicate of the partial
# `*_facts_dedupe_probe_idx` indexes, so the planner can prove it may read
# those. Those indexes cover only uncompressed chunks (the last ~30 days);
# compressed history is still decompressed and filtered. These strings run with
# no parameters, so `%` is not doubled.
_NODE_FACTS_SQL = """
SELECT source, count(*)
  FROM okg.node_facts
 WHERE source LIKE E'\\\\_enricher:%'
 GROUP BY source
"""

_EDGE_FACTS_SQL = """
SELECT source, count(*)
  FROM okg.edge_facts
 WHERE source LIKE E'\\\\_enricher:%'
 GROUP BY source
"""

_PROGRESS_SQL = """
SELECT enricher_name, fact_watermark, edge_watermark, generation_id,
       catalog_version_id, last_run_at, last_status,
       lifetime_edges_emitted, dormant_ok
  FROM okg.enricher_progress
 WHERE deployment_name = %(deployment)s
 ORDER BY last_run_at DESC, enricher_name
"""

_EDGE_FACT_HISTORY_SQL = """
SELECT date_trunc('hour', observed_at) AS observed_bucket, count(*) AS edges
  FROM okg.edge_facts
 WHERE (
       source = %(producer_key)s
       OR (
         %(child_prefix)s <> ''
         AND left(source, length(%(child_prefix)s)) = %(child_prefix)s
       )
   )
   AND observed_at IS NOT NULL
 GROUP BY observed_bucket
 ORDER BY observed_bucket DESC
 LIMIT %(limit)s
"""


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in value]
    return value


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def _producer_key(name: str) -> str:
    return name if name.startswith("_enricher:") else f"_enricher:{name}"


def _child_prefix(name: str) -> str:
    if ":" in name:
        return ""
    return f"{_producer_key(name)}:"


def _count_for_name(counts: Mapping[str, int], name: str) -> int:
    key = _producer_key(name)
    total = int(counts.get(key, 0) or 0)
    prefix = _child_prefix(name)
    if prefix:
        total += sum(
            int(value or 0)
            for source, value in counts.items()
            if source.startswith(prefix)
        )
    return total


def _display_name(producer_key: str) -> str:
    return producer_key[len("_enricher:"):] if producer_key.startswith("_enricher:") else producer_key


def _generation_id(checkpoint: Any) -> str:
    metadata = getattr(checkpoint, "metadata", None) or {}
    return str(metadata.get("target_generation_id") or checkpoint.checkpoint_id)


def _parse_created_at(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _run_name(raw: Mapping[str, Any]) -> str:
    producer_id = str(raw.get("producer_id") or "")
    if producer_id.startswith("_enricher:"):
        return _display_name(producer_id)
    return str(raw.get("name") or producer_id)


def _run_matches(raw: Mapping[str, Any], name: str) -> bool:
    return _run_name(raw) == name or str(raw.get("producer_id") or "") == _producer_key(name)


def _run_from_metadata(
    checkpoint: Any,
    raw: Mapping[str, Any],
) -> dict[str, Any]:
    metadata = getattr(checkpoint, "metadata", None) or {}
    timings = raw.get("timings_ms") if isinstance(raw.get("timings_ms"), Mapping) else {}
    created_at = _parse_created_at(getattr(checkpoint, "created_at", None))
    total_ms = _as_float(timings.get("total"))
    return {
        "generation_id": _generation_id(checkpoint),
        "generation_status": str(metadata.get("status") or "unknown"),
        "observed_at": created_at.isoformat() if created_at else None,
        "age": _humanize_age(created_at) if created_at else None,
        "name": _run_name(raw),
        "producer_key": str(raw.get("producer_id") or _producer_key(_run_name(raw))),
        "status": raw.get("status"),
        "reason": raw.get("reason"),
        "dirty_node_count": _as_int(raw.get("dirty_node_count")),
        "dirty_edge_count": _as_int(raw.get("dirty_edge_count")),
        "n_edges_emitted": _as_int(raw.get("n_edges_emitted")) or 0,
        "n_edges_skipped": _as_int(raw.get("n_edges_skipped")) or 0,
        "total_ms": total_ms,
        "timings_ms": _jsonable(timings),
    }


def _read_enricher_run_window(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
    window: int = ENRICHER_RUNS_WINDOW,
) -> list[Any]:
    """The newest `window` generation checkpoints' enricher runs. ONE read.

    Every run row and every enricher name this composer serves comes out of
    this one list. It replaced a `store.list_generations` call per enricher
    name plus one for the name set -- 19 fetches of every checkpoint's full
    metadata per request, 45.6 s of a 59 s read on okg-workspace (#2202).
    """
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    return store.recent_generation_enricher_runs(
        deployment_name=deployment_name,
        branch=session.branch_name,
        limit=window,
    )


def _checkpoint_runs(checkpoint: Any) -> list[Any]:
    metadata = getattr(checkpoint, "metadata", None) or {}
    raw_runs = (
        metadata.get("enricher_runs")
        or (metadata.get("timing_details") or {}).get("enrichers")
        or []
    )
    return raw_runs if isinstance(raw_runs, list) else []


def _metadata_runs_for_name(
    checkpoints: Sequence[Any],
    *,
    name: str,
    limit: int,
) -> list[dict[str, Any]]:
    """This enricher's runs inside the window, newest first.

    A pure filter over the window `_read_enricher_run_window` already read:
    no statement is issued here, so adding an enricher name costs nothing.
    """
    runs: list[dict[str, Any]] = []
    for checkpoint in checkpoints:
        for raw in _checkpoint_runs(checkpoint):
            if not isinstance(raw, Mapping):
                continue
            if _run_matches(raw, name):
                runs.append(_run_from_metadata(checkpoint, raw))
                break
        if len(runs) >= limit:
            break
    return runs


def _edge_fact_history_for_name(
    conn: Any,
    *,
    name: str,
    limit: int,
) -> list[dict[str, Any]]:
    key = _producer_key(name)
    rows: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(
            _EDGE_FACT_HISTORY_SQL,
            {
                "producer_key": key,
                "child_prefix": _child_prefix(name),
                "limit": limit,
            },
        )
        for observed_at, edges in cur.fetchall():
            rows.append({
                "generation_id": None,
                "generation_status": "not_applicable",
                "observed_at": observed_at.isoformat() if observed_at else None,
                "age": _humanize_age(observed_at) if observed_at else None,
                "name": name,
                "producer_key": key,
                "status": "observed",
                "reason": "edge_fact_observation_bucket",
                "dirty_node_count": None,
                "dirty_edge_count": None,
                "n_edges_emitted": int(edges or 0),
                "n_edges_skipped": 0,
                "total_ms": None,
                "timings_ms": {},
            })
    return rows


def _all_metadata_names(checkpoints: Sequence[Any]) -> set[str]:
    """Every enricher name that ran inside the window. No statement."""
    names: set[str] = set()
    for checkpoint in checkpoints:
        for raw in _checkpoint_runs(checkpoint):
            if isinstance(raw, Mapping):
                name = _run_name(raw)
                if name and name != "_enricher_setup":
                    names.add(name)
    return names


def _run_summary(runs: list[dict[str, Any]]) -> dict[str, Any]:
    durations = [
        duration for run in runs
        if (duration := _as_float(run.get("total_ms"))) is not None
    ]
    failed = sum(1 for run in runs if str(run.get("status") or "").lower() == "failed")
    return {
        "run_count": len(runs),
        "failed_run_count": failed,
        "total_dirty_nodes": sum(_as_int(run.get("dirty_node_count")) or 0 for run in runs),
        "total_dirty_edges": sum(_as_int(run.get("dirty_edge_count")) or 0 for run in runs),
        "total_edges_emitted": sum(_as_int(run.get("n_edges_emitted")) or 0 for run in runs),
        "latest_total_ms": durations[0] if durations else None,
    }


_HELD_SOURCE_STATE_SQL = """
    SELECT DISTINCT ON (source) source, admission_mode, status
      FROM okg.admission_envelopes
     WHERE deployment_name = %(deployment)s
       AND intent = 'source_sync'
       AND source = ANY(%(sources)s)
     ORDER BY source, created_at DESC, admission_id DESC
"""


def _withheld_detail(progress: Mapping[str, Any] | None) -> tuple[str, str]:
    """``(reason_code, detail)`` the publisher recorded on a withheld run."""
    found = withheld_reason_from_status((progress or {}).get("last_status"))
    if found is None:
        return "", "(unknown)"
    reason, detail = found
    return reason, detail or "(unknown)"


def _held_source_states(
    conn: Any, deployment_name: str, sources: Sequence[str],
) -> dict[str, tuple[str, str]]:
    """Latest ``(admission_mode, status)`` per held source, for `next_check`.

    Issue #1528 F2. Without this the console cannot tell a DECIDABLE hold from
    a WEDGED one, and the two need opposite instructions.
    """
    wanted = sorted({str(s) for s in sources if s and s != "(unknown)"})
    if not wanted:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            _HELD_SOURCE_STATE_SQL,
            {"deployment": deployment_name, "sources": wanted},
        )
        return {
            str(row[0]): (str(row[1]), str(row[2])) for row in cur.fetchall()
        }


def _withheld_next_check(
    reason: str,
    detail: str,
    states: Mapping[str, tuple[str, str]],
) -> str:
    """What an operator can actually DO about a withheld enricher.

    Issue #1528 F2. The first version of this view told every withheld
    enricher to run `okg admission decide`. For most holds that command
    cannot work, and sending an operator to a no-op is worse than silence:

    * `review` or `quarantine` mode + `pending` -- the ONLY decidable case.
      `admission_decide_workflow` requires `pending`, and ingest finalizes
      a run in either held mode `pending` (issue #1752).
    * anything `quarantined` -- `runtime/admission_workflows.py:948-985`
      short-circuits BOTH accept and reject on a non-`pending` envelope to
      `already_decided`, so the run cannot be moved from the console at all.
    * `quarantine` mode past `pending` -- before issue #1752 ingest
      finalized such a run `accepted` and the publish sweep then marked it
      `published`/`blocked`, all with the rows still off `default`. Envelopes
      written before that fix stay in this wedge.

    The hold clears when a LATER sync of that source lands in a non-held
    admission mode, or when the source's admission policy is changed off
    `quarantine`.
    """
    if reason == UNKNOWN_DECLARED_SOURCE:
        return (
            f"Declaration error: this enricher declares input source "
            f"{detail}, which is not a registered source in this "
            "deployment's source_registry.yaml. It cannot be gated and does "
            "not run. Fix `reads_sources` to name a registered source (its "
            "registry name or its ownership_id)."
        )
    decidable = sorted(
        source for source, (mode, status) in states.items()
        if admission_mode_awaits_decision(mode) and status == "pending"
    )
    wedged = sorted(
        source for source in states if source not in decidable
    )
    if decidable and not wedged:
        return (
            f"Held: input source {', '.join(decidable)} awaits an admission "
            "decision, so this enricher did not run and its watermark is "
            "held. Release it with `okg admission decide <admission-id> "
            "accept`."
        )
    wedge_text = (
        f"Held: input source {', '.join(wedged) or detail} is off `default` "
        "and its admission envelope is NOT decidable -- `okg admission "
        "decide` returns already_decided for it. The hold clears only when a "
        "later sync of that source lands in a non-held admission mode "
        "(change the source's admission_policy off review/quarantine), or "
        "when the run is rejected."
    )
    if decidable:
        return (
            f"Held: input source {', '.join(decidable)} awaits an admission "
            "decision -- release it with `okg admission decide "
            "<admission-id> accept`. " + wedge_text
        )
    return wedge_text


def _health_status(progress: Mapping[str, Any] | None, latest_run: Mapping[str, Any] | None) -> str:
    prog = progress or {}
    last_status = str(prog.get("last_status") or "")
    # Issue #1528: read the withheld hold FIRST and only from the reason the
    # publisher wrote. A withheld enricher did not run because a declared input
    # source run is still held off `default` awaiting an admission decision.
    # It is neither failing nor unknown, and reporting it as `unknown` (with
    # "No action needed.") hid the one action that clears it.
    if withheld_reason_from_status(last_status) is not None:
        return "withheld"
    if last_status.startswith("failed:") or (latest_run and latest_run.get("status") == "failed"):
        return "failing"
    is_success = last_status == "success" or (
        latest_run and latest_run.get("status") in {"ok", "skipped", "observed"}
    )
    if is_success:
        # #707: a wired enricher (it has advanced at least once, so
        # last_status is 'success') that has emitted zero edges over its
        # whole lifetime has produced nothing since it was wired. That is
        # NOT healthy — a zero-emit cycle used to be byte-identical to a
        # productive one. The `dormant_ok` declaration (design point (i))
        # is the explicit escape for an enricher expected to be quiet.
        lifetime_emitted = int(prog.get("lifetime_edges_emitted") or 0)
        dormant_ok = bool(prog.get("dormant_ok"))
        if (
            last_status == "success"
            and lifetime_emitted == 0
            and not dormant_ok
        ):
            return "wired-but-silent"
        return "healthy"
    if last_status == "never":
        return "unknown"
    return "unknown"


def _progress_rows(conn: Any, deployment_name: str) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute(_PROGRESS_SQL, {"deployment": deployment_name})
        for (
            name,
            fact_watermark,
            edge_watermark,
            generation_id,
            catalog_version_id,
            last_run_at,
            last_status,
            lifetime_edges_emitted,
            dormant_ok,
        ) in cur.fetchall():
            rows[str(name)] = {
                "enricher_name": str(name),
                "producer_key": _producer_key(str(name)),
                "fact_watermark": int(fact_watermark or 0),
                "edge_watermark": int(edge_watermark or 0),
                "generation_id": int(generation_id or 0),
                "catalog_version_id": (
                    int(catalog_version_id) if catalog_version_id is not None else None
                ),
                "last_run_at": last_run_at.isoformat() if last_run_at else None,
                "last_run_age": _humanize_age(last_run_at) if last_run_at else None,
                "last_status": str(last_status or "never"),
                "lifetime_edges_emitted": int(lifetime_edges_emitted or 0),
                "dormant_ok": bool(dormant_ok),
            }
    return rows


def _progress_for_name(
    progress: dict[str, dict[str, Any]],
    name: str,
) -> dict[str, Any] | None:
    if name in progress:
        return progress[name]
    if ":" not in name:
        return None
    parent_name = name.split(":", 1)[0]
    parent = progress.get(parent_name)
    if parent is None:
        return None
    return {
        **parent,
        "enricher_name": name,
        "producer_key": _producer_key(name),
        "progress_scope": parent_name,
    }


def compose_enricher_diagnostics(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
    selected_enricher: str | None = None,
    run_limit: int = 8,
    run_scope: str = "all",
    count_rollup: ConsoleCountRollupCache | None = None,
) -> dict[str, Any]:
    """Return current contribution and recent activity for enrichers."""
    run_limit = max(1, min(int(run_limit), 20))
    if run_scope not in {"all", "selected", "none"}:
        raise ValueError("run_scope must be one of: all, selected, none")
    selected_name = _display_name(selected_enricher) if selected_enricher else None
    # Live enricher node/edge contribution O(1) from the maintained rollup's
    # enricher_node / enricher_edge dimensions (flag-on); flag-off keeps the raw
    # CASE GROUP BY. A missing rollup fails loud (never a silent empty/scan).
    if console_count_rollup_enabled():
        from okg.substrate.console_views.console_count_rollup_read import (
            COUNT_KIND_ENRICHER_EDGE,
            COUNT_KIND_ENRICHER_NODE,
            rollup_reader,
        )

        # The request's cache when a composed read supplied one: one lease per
        # request, not one per composer (issue #2202).
        rolled = rollup_reader(
            count_rollup, conn, generation_id=session.generation_id
        ).read({COUNT_KIND_ENRICHER_NODE, COUNT_KIND_ENRICHER_EDGE})
        live_nodes = {}
        live_edges = {}
        for (kind, name), count in rolled.items():
            if kind == COUNT_KIND_ENRICHER_NODE:
                live_nodes[name] = count
            else:
                live_edges[name] = count
    else:
        # The full-graph CASE scans are only needed on the flag-off path.
        graph = checkout(conn, session)
        live_nodes = {
            str(row["source"]): int(row["live"] or 0)
            for row in graph.query(_LIVE_NODES_SQL)
        }
        live_edges = {
            str(row["source"]): int(row["live"] or 0)
            for row in graph.query(_LIVE_EDGES_SQL)
        }
    node_facts: dict[str, int] = {}
    edge_facts: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute(_NODE_FACTS_SQL)
        for source, count in cur.fetchall():
            node_facts[str(source)] = int(count or 0)
        cur.execute(_EDGE_FACTS_SQL)
        for source, count in cur.fetchall():
            edge_facts[str(source)] = int(count or 0)

    progress = _progress_rows(conn, deployment_name)
    # ONE bounded checkpoint read serves both the name set and every run
    # history below, whatever the enricher count.
    run_window = (
        _read_enricher_run_window(
            conn, deployment_name=deployment_name, session=session
        )
        if run_scope != "none"
        else []
    )
    names = set(progress)
    names.update(
        _display_name(key)
        for key in set(live_nodes) | set(live_edges) | set(node_facts) | set(edge_facts)
    )
    if run_scope == "all":
        names.update(_all_metadata_names(run_window))
    if selected_name:
        names.add(selected_name)
    names.discard("_enricher_setup")

    rows: list[dict[str, Any]] = []
    runs_by_name: dict[str, list[dict[str, Any]]] = {}
    child_counts = {
        name: sum(1 for child in names if child.startswith(f"{name}:"))
        for name in names
        if ":" not in name
    }
    for name in sorted(names):
        key = _producer_key(name)
        if run_scope == "all" or (run_scope == "selected" and name == selected_name):
            recent_runs = _metadata_runs_for_name(
                run_window,
                name=name,
                limit=run_limit,
            )
        else:
            recent_runs = []
        runs_by_name[name] = recent_runs
        progress_row = _progress_for_name(progress, name)
        latest_run = recent_runs[0] if recent_runs else None
        status = _health_status(progress_row, latest_run)
        rows.append({
            "name": name,
            "producer_key": key,
            "status": status,
            "last_run_age": (
                (progress_row or {}).get("last_run_age")
                or (latest_run or {}).get("age")
            ),
            "latest_issue": (
                (progress_row or {}).get("last_status")
                if status in {"failing", "withheld"}
                else "0 edges emitted since wiring"
                if status == "wired-but-silent"
                else None
            ),
            "next_check": (
                "Inspect the failing enricher status."
                if status == "failing"
                else _withheld_next_check(
                    *_withheld_detail(progress_row),
                    states=_held_source_states(
                        conn,
                        deployment_name,
                        _withheld_detail(progress_row)[1].split(","),
                    ),
                )
                if status == "withheld"
                else (
                    "Enricher runs clean but has never emitted an edge; "
                    "check its inputs/LLM path, or declare dormant_ok if "
                    "it is expected to be quiet."
                )
                if status == "wired-but-silent"
                else "No action needed."
            ),
            "contribution": {
                "authority": "enricher_diagnostics",
                "horizon": "pinned_generation",
                "records_in_gen": _count_for_name(live_nodes, name),
                "edges_in_gen": _count_for_name(live_edges, name),
                "total_facts": _count_for_name(node_facts, name),
                "total_edge_facts": _count_for_name(edge_facts, name),
            },
            "progress": progress_row,
            "latest_run": latest_run,
            "run_summary": _run_summary(recent_runs),
            "child_count": child_counts.get(name, 0),
        })

    rows.sort(
        key=lambda row: (
            row["status"] == "healthy",
            -int(row["contribution"]["edges_in_gen"]),
            row["name"],
        )
    )

    if selected_name is None and run_scope != "none" and rows:
        selected_name = str(rows[0]["name"])
    selected_row = next(
        (row for row in rows if row["name"] == selected_name),
        None,
    )
    selected = None
    if selected_name is not None:
        selected_runs = runs_by_name.get(selected_name, [])
        if selected_row is not None and not selected_runs:
            selected_runs = _edge_fact_history_for_name(
                conn,
                name=selected_name,
                limit=run_limit,
            )
            if selected_runs:
                selected_row = {
                    **selected_row,
                    "latest_run": selected_runs[0],
                    "run_summary": _run_summary(selected_runs),
                }
        selected = {
            "enricher_name": selected_name,
            "found": selected_row is not None,
            "row": selected_row,
            "run_history": selected_runs,
            "run_summary": _run_summary(selected_runs),
            "selected_run": (
                selected_runs[0]
                if selected_runs
                else None
            ),
            "omitted_data": [{
                "authority": "historical_live_contribution",
                "reason": "historical live graph size is not recorded",
            }],
        }

    return {
        "schema": "okg.console.enricher_diagnostics.v1",
        "deployment": deployment_name,
        "generation_id": session.generation_id,
        "selected_enricher": selected_name,
        # The bound this read looked back over, served by name. A run history
        # that does not say how far back it looked reads as complete, and a
        # missing run then reads as an enricher that never ran.
        "enricher_runs_window": ENRICHER_RUNS_WINDOW,
        "summary": {
            "enricher_count": len(rows),
            "degraded_count": sum(1 for row in rows if row["status"] != "healthy"),
            "omitted_data_count": 1 if rows else 0,
        },
        "enrichers": rows,
        "selected": selected,
        "omitted_data": [{
            "authority": "historical_live_contribution",
            "reason": "historical live graph size is not recorded",
        }] if rows else [],
    }
