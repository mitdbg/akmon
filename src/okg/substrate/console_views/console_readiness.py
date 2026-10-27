"""The readiness the console prints, and nothing behind it.

`okg dogfood status` is a full release-readiness composition of roughly two
dozen checks. The Sources
view reads three things out of that payload -- each source's sync convergence
(state and reason), its freshness, and the published generation -- and prints
nothing from the rest. Measured on okg-workspace, the rest is most of the
wait: the Nomos source-lineage report costs 15.4 s (14.0 s of it inside
`count_laundered_derived_rows`) and the runtime queue's dead-letter scan costs
5.6 s, neither of which the page can show.

So the console composes its own bounded readiness. The rule is not "run the
cheap checks": it is run exactly the checks whose results reach the page, and
NAME every check declined, so a reader can see that this page is not a release
gate. `okg dogfood status`, the deployment gates and the CLI keep the full
composition untouched -- this module adds a second, narrower reader; it
replaces nothing.

One deliberate departure from the shortest possible set. The MCP graph-read
probe is run, although the page prints nothing from it, because a source's
convergence state is composed from it: `_source_convergence_row` reads a
source whose watermark is fresh as `blocked /
published_generation_not_visible_to_read_side` when the MCP status does not
show a visible generation. Declining the probe and passing nothing in its
place would therefore have turned every healthy source on the page into a
fabricated blocker. It stays; since issue #2547 a passing result is reused
while the generation, catalog, policy and enforcement mode it judged hold.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

#: The payload's own name, so a reader (and a test) can tell this read from
#: the full `okg dogfood status` composition it is NOT.
CONSOLE_READINESS_SCHEMA = "okg.console.readiness.v1"
CONSOLE_READINESS_BASIS = "console_bounded_readiness"

#: The bound on any one statement this read issues. The same value the full
#: readiness run uses per probe: a row limit does not bound execution time,
#: only a statement timeout does.
CONSOLE_PROBE_TIMEOUT_SECONDS = 30.0

#: The checks this read runs, by the id the full readiness run gives them.
CHECKS_RUN: tuple[str, ...] = (
    "generation.latest_published",
    "mcp.graph_read",
    "sources.freshness",
    "sources.sync_liveness",
)

#: Every check the full readiness run performs that this read declines, by the
#: same id. Served with the payload so the page can say how many checks it is
#: not showing, and so a reader who wants one knows the command that runs it.
#:
#: Pinned here rather than derived, because there is nothing to derive it
#: from: the full run declares its checks by calling `_ready_add_check` inline.
#: `tests/substrate/console_views/test_console_readiness.py` holds this list
#: against the ids that run actually emits, so a check added there and not
#: added here is a failing test and not a silent omission.
CHECKS_NOT_RUN: tuple[str, ...] = (
    "database.migrations",
    "database.postgres_capacity",
    "database.timescaledb_workers",
    "deployment.code_drift",
    "deployment.writer_driver",
    "doctor.deployment",
    "doctor.okg_workspace",
    "generation.count_integrity",
    "host.storage_headroom",
    "mcp.principal_configuration",
    "nomos.active_state",
    "nomos.deployment_policy",
    "nomos.read_path",
    "nomos.source_lineage",
    "publish.source_run_in_progress",
    "publish.source_undo",
    "publish.stale_run_bases",
    "publisher.lock_headroom",
    "queues.active_depth",
    "queues.bounds",
    "queues.dlq",
    "queues.stale",
    "rollout.workspace_graph",
    "runtime.worker_processes",
    "sources.freshness_advisory",
    "sources.optimization_budget",
    "sources.readiness_undeclared",
)

#: Ids the full readiness run emits that are NOT checks of their own.
#:
#: Two shapes. A `<check>.unavailable` id is the marker the full run writes
#: IN PLACE OF a check it could not reach -- no driver, no Nomos, no MCP
#: status -- and `nomos.deployment_policy.warnings` is a warnings row beside
#: the check of the same name, as `publisher.schedule_paused` is a warnings
#: row beside the doctor check that found the pause. Neither is a further
#: thing an operator could run; each is one of the listed checks reporting
#: on itself.
#:
#: They are classified here rather than in `CHECKS_NOT_RUN` because the page
#: prints that list's LENGTH as "N further checks run only in
#: okg dogfood status". Counting `sources.freshness.unavailable` there would
#: have said freshness runs only in the full command, while this read runs it
#: four lines above; counting `queues.unavailable` there counted the queue
#: checks twice. This read names its own unavailability in `read_authority`
#: and emits none of these ids.
#:
#: The drift guard in `tests/substrate/console_views/test_console_readiness.py`
#: holds the three tuples together against the full run's call sites, in both
#: directions, so an id can be neither added nor removed there unnoticed.
IDS_THAT_ARE_NOT_CHECKS: tuple[str, ...] = (
    "deployment.writer_driver.unavailable",
    "doctor.okg_workspace.unavailable",
    "generation.unavailable",
    "mcp.graph_read.unavailable",
    "nomos.deployment_policy.warnings",
    "nomos.read_path.unavailable",
    "nomos.source_lineage.unavailable",
    "publish.source_runs.unavailable",
    "publish.source_undo.unavailable",
    "publisher.schedule_paused",
    "queues.unavailable",
    "sources.freshness.unavailable",
    "sources.freshness_advisory_unmeasured",
    "sources.optimization_budget.unavailable",
    "sources.registry.unavailable",
    "sources.sync_liveness.unavailable",
    "sources.sync_status.unavailable",
)

#: The command that runs the declined checks. Named in the payload so the page
#: never has to invent one.
FULL_READINESS_COMMAND = "okg dogfood status"


# --- the probes, bound to module names so a test can hold the read to them ---

def _connect(dsn: str) -> Any:
    from okg.substrate.db.connection import connect as db_connect

    return db_connect(dsn, connect_timeout=5, autocommit=True)


def _statement_timeout(conn: Any, seconds: float) -> Any:
    from okg.substrate.cli.deployment_readiness import ready_statement_timeout

    return ready_statement_timeout(conn, seconds)


#: The checkpoint fields the generation check serves. Each is under 500 bytes
#: on every okg-workspace checkpoint; the document around them is up to 62 MB.
_GENERATION_STATUS_KEYS: tuple[str, ...] = (
    "kind",
    "deployment_name",
    "branch",
    "status",
    "catalog_version_id",
    "target_generation_id",
    "node_count",
    "edge_count",
    "publisher_configuration",
    "first_publish_budget_warning",
)


def _generation_status(
    conn: Any, deployment: str, session: Any = None
) -> dict[str, Any]:
    """The published generation, as the page states it.

    With the page's pinned `session` -- how the console calls it -- this is
    ONE projected read of that generation's checkpoint: no second search for
    the latest generation (6.3 s on okg-workspace, issue #2546) and no
    `count(*)` over every node and edge (4.7 s). The full run's
    `count_integrity` is that count; this read lists
    `generation.count_integrity` among the checks it does not run, so it
    serves no `count_integrity` key rather than a result it did not measure.
    Fields are derived as `resolve_okg_generation` derives them.

    Without a session it is the full run's own probe, unchanged.
    """
    if session is None:
        from okg.substrate.cli.deployment_readiness import _ready_generation_status

        return _ready_generation_status(conn, deployment)

    from okg.substrate.console_views._chronos import read_checkpoint_fields
    from okg.substrate.generation_resolver import (
        _metadata_matches,
        _required_catalog_version_id,
        _target_generation_id,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.history_read_guard import raise_if_compacted

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    checkpoint_id = store._generation_checkpoint_id(session.generation_id)
    raise_if_compacted(store.context.db, checkpoint_id)
    found = read_checkpoint_fields(conn, checkpoint_id, _GENERATION_STATUS_KEYS)
    missing = {"status": "missing", "deployment_name": deployment, "generation_id": None}
    if found is None:
        return missing
    created_at, metadata = found
    if not _metadata_matches(
        metadata,
        deployment_name=deployment,
        branch=session.branch_name,
        status="published",
        branch_required=False,
    ):
        return missing
    return {
        "generation_id": checkpoint_id,
        "deployment_name": metadata.get("deployment_name"),
        "status": metadata.get("status"),
        "catalog_version_id": _required_catalog_version_id(
            metadata.get("catalog_version_id"),
            generation_id=checkpoint_id,
            deployment_name=metadata.get("deployment_name"),
            branch=metadata.get("branch"),
        ),
        "target_generation_id": _target_generation_id(metadata, None),
        "created_at": created_at,
        "node_count": metadata.get("node_count"),
        "edge_count": metadata.get("edge_count"),
        "publisher_configuration": metadata.get("publisher_configuration"),
        "first_publish_budget_warning": metadata.get("first_publish_budget_warning"),
    }


#: The console process's passing MCP sample verdicts (issue #2547). Built on
#: first use; see `McpSampleVerdicts` for what keys a verdict and when it is
#: re-measured.
_MCP_SAMPLE_VERDICTS: Any = None


def _mcp_sample_verdicts() -> Any:
    global _MCP_SAMPLE_VERDICTS
    if _MCP_SAMPLE_VERDICTS is None:
        from okg.substrate.cli.deployment_readiness import McpSampleVerdicts

        _MCP_SAMPLE_VERDICTS = McpSampleVerdicts()
    return _MCP_SAMPLE_VERDICTS


def _mcp_status(conn: Any, deployment: str, session: Any = None) -> dict[str, Any]:
    """The MCP graph-read probe, on the page's pinned session when given.

    Passing the session skips the probe's own search for the latest
    generation (7.8 s on okg-workspace, issue #2546). With a session, a
    passing Nomos-filtered sample verdict is reused while the generation,
    catalog, active policy bundle and enforcement mode it was measured
    against are unchanged (issue #2547): the 300 policy evaluations behind it
    cost about 2 s a read and answer the same question each time. The
    principal login check still runs on every read, and the payload's
    `verdict_reuse` says whether this read measured the sample or reused it.
    """
    from okg.substrate.cli.deployment_readiness import _ready_mcp_status

    if session is None:
        return _ready_mcp_status(conn, deployment, session=session)
    return _ready_mcp_status(
        conn, deployment, session=session, sample_verdicts=_mcp_sample_verdicts()
    )


def _source_freshness_status(conn: Any, **kwargs: Any) -> dict[str, Any]:
    from okg.substrate.cli.deployment_readiness import (
        _ready_source_freshness_status,
    )

    return _ready_source_freshness_status(conn, **kwargs)


def _source_sync_liveness_status(**kwargs: Any) -> dict[str, Any]:
    from okg.substrate.cli.deployment_readiness import (
        _ready_source_sync_liveness_status,
    )

    return _ready_source_sync_liveness_status(**kwargs)


def _source_groups_for_profile(
    profile: str, *, deployment: str, deployment_dir: Path
) -> Any:
    from okg.substrate.cli.deployment_readiness import (
        _ready_source_groups_for_profile,
    )

    return _ready_source_groups_for_profile(
        profile, deployment=deployment, deployment_dir=deployment_dir
    )


def _resolved_readiness_profile(deployment_dir: Path) -> Any:
    from okg.substrate.cli.deployment_readiness import (
        _ready_resolved_readiness_profile,
    )

    return _ready_resolved_readiness_profile(deployment_dir)


def _source_aliases(deployment_dir: Path) -> Any:
    from okg.substrate.cli.deployment_readiness import _ready_source_aliases

    return _ready_source_aliases(deployment_dir)


def _progress_freshness_sources(deployment_dir: Path, profile: str) -> Any:
    from okg.substrate.cli.deployment_readiness import (
        _ready_progress_freshness_sources,
    )

    return _ready_progress_freshness_sources(deployment_dir, profile)


def _sync_status_contract(liveness: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    from okg.substrate.source_sync_status import build_source_sync_status_contract

    return build_source_sync_status_contract(liveness, **kwargs)


def _probe(
    authority: dict[str, Any], name: str, call: Any
) -> dict[str, Any] | None:
    """Run one probe, or record by name that it could not be run.

    The exception's CLASS, never its message. These probes dial a database
    from a DSN, so `str(exc)` is a driver error carrying the host it reached
    for and, on the usual spelling, the password with it -- into a payload a
    browser renders. The reason code says which authority failed and the class
    says how; the message belongs in the server log.
    """
    try:
        result = call()
    except Exception as exc:  # noqa: BLE001 - name the authority that failed.
        authority[name] = {
            "status": "unavailable",
            "reason": f"console_readiness_{name}_probe_failed",
            "error_type": type(exc).__name__,
        }
    else:
        authority[name] = {"status": "available"}
        return result
    # Not measured. The payload's `read_authority[name]`, written just above,
    # names the reason and the exception class; every caller treats None as
    # "absent" and never as a measured empty answer.
    return None


def compose_console_readiness(
    *,
    deployment_name: str,
    deployment_dir: Path,
    dsn: str,
    readiness_profile: str | None = None,
    session: Any = None,
) -> dict[str, Any]:
    """The bounded readiness the console's Sources reads consume.

    The payload keeps `details.readiness.source_sync_status` where the
    `okg dogfood status` payload puts it, so the composers that read it need no
    new shape. What is new is `basis` and `checks_not_run`: this read states
    that it is not the full run, and names what it left out.

    `session` is the page's pinned read session. Given it, the generation and
    MCP checks describe THAT generation instead of each searching for the
    latest one again, so the generation the page prints and the one these
    checks judged are the same one (issue #2546).

    `readiness_profile` defaults to the profile the deployment is judged by
    (issue #3958): the same resolver `okg dogfood status` uses, with that
    profile's source-age limit. A standard deployment that marks no baseline
    source reports freshness and sync liveness `not_checked`; a profile that
    cannot be resolved runs no source check and says so.
    """
    from okg.substrate.cli.deployment import (
        _READY_DOGFOOD_MAX_PRODUCER_AGE_SECONDS,
    )
    from okg.substrate.deployments.readiness_profile import (
        MAX_SOURCE_AGE_SECONDS,
    )

    deployment_path = Path(deployment_dir)
    authority: dict[str, Any] = {}
    generation: dict[str, Any] | None = None
    mcp: dict[str, Any] | None = None
    freshness: dict[str, Any] | None = None
    liveness: dict[str, Any] | None = None

    profile_name: str | None = readiness_profile
    if profile_name is None:
        resolved = _probe(
            authority,
            "readiness_profile",
            lambda: _resolved_readiness_profile(deployment_path),
        )
        if isinstance(resolved, dict) and resolved.get("problem"):
            authority["readiness_profile"] = {
                "status": "unavailable",
                "reason": str(resolved["problem"].get("code") or ""),
            }
        elif isinstance(resolved, dict):
            profile_name = str(resolved.get("name"))
            authority["readiness_profile"]["name"] = profile_name
            authority["readiness_profile"]["source"] = resolved.get("source")
    if profile_name is None:
        source_groups = None
        authority["source_groups"] = {
            "status": "unavailable",
            "reason": "readiness_profile_unresolved",
        }
    else:
        resolved_profile = profile_name
        source_groups = _probe(
            authority,
            "source_groups",
            lambda: _source_groups_for_profile(
                resolved_profile,
                deployment=deployment_name,
                deployment_dir=deployment_path,
            ),
        )
    # Measured and empty, under standard: the deployment promises no
    # freshness. Not `not_requested`, which reads as "nobody asked".
    no_baseline = profile_name == "standard" and source_groups == {}
    not_checked = {
        "status": "not_checked",
        "reason": "no_baseline_sources_declared",
    }

    with _connect(dsn) as conn, _statement_timeout(
        conn, CONSOLE_PROBE_TIMEOUT_SECONDS
    ):
        generation = _probe(
            authority,
            "generation",
            lambda: _generation_status(conn, deployment_name, session),
        )
        mcp = _probe(
            authority, "mcp", lambda: _mcp_status(conn, deployment_name, session)
        )
        if isinstance(mcp, dict) and "verdict_reuse" in mcp:
            # Whether this read measured the Nomos sample or reused a passing
            # verdict for the same generation and policy (issue #2547).
            authority["mcp"]["verdict_reuse"] = mcp["verdict_reuse"]
        if source_groups:
            # Only dogfood, ci and standard produce source groups, so the
            # profile always has an age limit here.
            freshness = _probe(
                authority,
                "source_freshness",
                lambda: _source_freshness_status(
                    conn,
                    deployment=deployment_name,
                    max_source_age_seconds=MAX_SOURCE_AGE_SECONDS[
                        str(profile_name)
                    ],
                    source_groups=source_groups,
                    progress_freshness_sources=_progress_freshness_sources(
                        deployment_path, str(profile_name)
                    ),
                    source_aliases=_source_aliases(deployment_path),
                ),
            )
        elif no_baseline:
            authority["source_freshness"] = dict(not_checked)
        elif profile_name is None:
            authority["source_freshness"] = {
                "status": "unavailable",
                "reason": "readiness_profile_unresolved",
            }
        else:
            authority["source_freshness"] = {
                "status": "not_requested",
                "reason": "no_readiness_source_groups_for_this_profile",
            }

    if source_groups:
        liveness = _probe(
            authority,
            "source_sync_liveness",
            lambda: _source_sync_liveness_status(
                dsn=dsn,
                deployment=deployment_name,
                deployment_dir=deployment_path,
                max_producer_age_seconds=_READY_DOGFOOD_MAX_PRODUCER_AGE_SECONDS,
                readiness_profile=str(profile_name),
                readiness_source_groups=source_groups,
            ),
        )
    elif no_baseline:
        authority["source_sync_liveness"] = dict(not_checked)
    elif profile_name is None:
        authority["source_sync_liveness"] = {
            "status": "unavailable",
            "reason": "readiness_profile_unresolved",
        }
    else:
        authority["source_sync_liveness"] = {
            "status": "not_requested",
            "reason": "no_readiness_source_groups_for_this_profile",
        }

    readiness: dict[str, Any] = {
        "generation": generation or {},
        "source_freshness": freshness or {},
    }
    if isinstance(liveness, dict):
        # Composed ONLY from measured input. The full run gates this the same
        # way: building the contract out of a probe that never ran is the
        # fabricated-blocker shape, not a conservative default.
        sync_status = _probe(
            authority,
            "source_sync_status",
            lambda: _sync_status_contract(
                liveness,
                source_freshness=freshness,
                generation=generation,
                mcp=mcp,
            ),
        )
        if sync_status is not None:
            readiness["source_sync_status"] = sync_status
    elif no_baseline:
        authority["source_sync_status"] = dict(not_checked)
    else:
        authority["source_sync_status"] = {
            "status": "unavailable",
            "reason": "source_sync_liveness_was_not_measured",
        }

    return {
        "schema": CONSOLE_READINESS_SCHEMA,
        "basis": CONSOLE_READINESS_BASIS,
        "deployment": deployment_name,
        "checks_run": list(CHECKS_RUN),
        "checks_not_run": list(CHECKS_NOT_RUN),
        "checks_not_run_count": len(CHECKS_NOT_RUN),
        "checks_not_run_command": FULL_READINESS_COMMAND,
        # Served so the classification is readable from the payload and not
        # only from this module: every id the full run emits is in one of
        # these three lists, and none of these is a check an operator runs.
        "ids_that_are_not_checks": list(IDS_THAT_ARE_NOT_CHECKS),
        "read_authority": authority,
        "details": {"readiness": readiness},
    }
