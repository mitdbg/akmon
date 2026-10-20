"""`okg generation` subcommand handlers.

Extracted from ``cli/main.py`` by ``extract-cli-generation``. Holds
the two ``okg generation`` handlers and the two private helpers
they use to summarize consistency-gate health for a generation.

Handlers defer their imports of ``cli.main`` helpers
(``_resolve_dsn``, ``_emit_error``, ``_emit_ok``) to call time to
avoid a top-level circular import.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any


def _checkpoint_release_identities(metadata: dict):
    from okg.substrate.publisher.repository_provenance import RepositoryProvenance
    from okg.substrate.distributions.packaged_release import PackagedReleaseIdentity
    raw = metadata.get("repository_provenance")
    repository = RepositoryProvenance.from_metadata(raw)
    packaged = None
    if "packaged_release" in metadata:
        packaged = PackagedReleaseIdentity.from_metadata(metadata["packaged_release"])
        if ((raw is not None and raw != RepositoryProvenance.unavailable().to_dict())
                or any(metadata.get(key) is not None for key in
                       ("core_revision", "deployment_revision", "deployment_tree_sha", "code_revision"))):
            raise ValueError("packaged and Git repository identities cannot be combined")
    return repository, packaged


def _consistency_gate_health_dict(
    cur: Any,
    *,
    check_id: str,
    gate_name: str,
    lookback: int,
    max_consecutive_blocked: int,
) -> dict:
    from okg.substrate.consistency import summarize_consistency_gate_health

    health = summarize_consistency_gate_health(
        cur.connection,
        check_id=check_id,
        gate_name=gate_name,
        lookback=lookback,
        max_consecutive_blocked=max_consecutive_blocked,
    )
    return {
        "check_id": health.check_id,
        "gate_name": health.gate_name,
        "total_runs": health.total_runs,
        "p50_elapsed_ms": health.p50_elapsed_ms,
        "p99_elapsed_ms": health.p99_elapsed_ms,
        "timeout_rate": str(health.timeout_rate),
        "error_rate": str(health.error_rate),
        "blocked_rate": str(health.blocked_rate),
        "operational_blocked_rate": str(health.operational_blocked_rate),
        "consecutive_blocked": health.consecutive_blocked,
        "consecutive_operational_blocked": (
            health.consecutive_operational_blocked
        ),
        "max_consecutive_blocked": health.max_consecutive_blocked,
        "degraded": health.degraded,
        "degraded_reason": health.degraded_reason,
    }


def _generation_consistency_diagnostics(
    cur: Any,
    *,
    generation_id: int,
) -> dict:
    """Return persisted consistency-gate evidence for a generation.

    Reads the gate result's persisted ``details`` payload for
    open-observation samples. That payload was captured at admission
    time, so ``okg generation describe <old-id>`` explains the old
    generation even if later generations resolve or reopen the same
    observation.
    """
    cur.execute(
        """
        SELECT result_id, run_id, check_id, gate_name, result,
               blocks_publish, reason, open_observation_count,
               coverage_complete, coverage_basis, budget_ms, elapsed_ms,
               preflight, details, created_at,
               CASE
                 WHEN (to_jsonb(cgr)->>'newly_opened_count') ~ '^[0-9]+$'
                   THEN (to_jsonb(cgr)->>'newly_opened_count')::bigint
                 WHEN (details->>'newly_opened_count') ~ '^[0-9]+$'
                   THEN (details->>'newly_opened_count')::bigint
                 ELSE 0
               END AS newly_opened_count,
               CASE
                 WHEN (to_jsonb(cgr)->>'resolved_count') ~ '^[0-9]+$'
                   THEN (to_jsonb(cgr)->>'resolved_count')::bigint
                 WHEN (details->>'resolved_count') ~ '^[0-9]+$'
                   THEN (details->>'resolved_count')::bigint
                 ELSE 0
               END AS resolved_count
          FROM okg.consistency_gate_results AS cgr
         WHERE target_generation_id = %s
         ORDER BY blocks_publish DESC, gate_name, check_id
        """,
        (generation_id,),
    )
    rows = cur.fetchall()
    gates: list[dict] = []
    for row in rows:
        details = row[13] or {}
        coverage = details.get("coverage")
        open_observations = details.get("open_observations") or []
        preflight = row[12] or {}
        policy = preflight.get("backpressure_policy") or {}
        health = _consistency_gate_health_dict(
            cur,
            check_id=row[2],
            gate_name=row[3],
            lookback=int(policy.get("lookback") or 20),
            max_consecutive_blocked=int(
                policy.get("max_consecutive_blocked") or 2
            ),
        )
        gate = {
            "result_id": str(row[0]),
            "run_id": str(row[1]),
            "check_id": row[2],
            "gate_name": row[3],
            "result": row[4],
            "blocks_publish": row[5],
            "reason": row[6],
            "open_observation_count": int(row[7] or 0),
            "newly_opened_count": int(row[15] or 0),
            "resolved_count": int(row[16] or 0),
            "coverage_complete": row[8],
            "coverage_basis": row[9],
            "budget_ms": row[10],
            "elapsed_ms": row[11],
            "preflight": preflight,
            "coverage": coverage,
            "open_observations": open_observations,
            "error": details.get("error"),
            "backpressure": details.get("backpressure"),
            "health": health,
            "details": details,
            "created_at": row[14].isoformat() if row[14] else None,
        }
        gates.append(gate)

    blocked = [gate for gate in gates if gate["blocks_publish"]]
    return {
        "gate_count": len(gates),
        "blocked_gate_count": len(blocked),
        "degraded_gate_count": sum(
            1 for gate in gates if gate["health"]["degraded"]
        ),
        "open_observation_count": sum(
            gate["open_observation_count"] for gate in gates
        ),
        "newly_opened_count": sum(
            gate["newly_opened_count"] for gate in gates
        ),
        "resolved_count": sum(gate["resolved_count"] for gate in gates),
        "gates": gates,
    }


def _cmd_generation(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg generation describe <id>` — describe one generation.
    `okg generation list` — list recent published generations.

    Used by the runbooks (``docs/runbooks/troubleshooting/generation-blocked.md``,
    ``docs/runbooks/troubleshooting/publisher-lock.md``) to investigate a
    ``blocked`` or ``pending`` generation, and by the MCP session
    error path's fix_hint.
    """
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.cli.main import _emit_error, _emit_ok, _resolve_dsn
    from okg.substrate.errors import SubstrateError

    subcmd = getattr(args, "subcmd", None)
    if subcmd == "list":
        return _cmd_generation_list(args, as_json=as_json)
    if subcmd == "compact":
        return _cmd_generation_compact(args, as_json=as_json)
    if subcmd != "describe":
        return _emit_error(
            SubstrateError(
                "okg generation: only `describe` and `list` are "
                "implemented alongside `compact`",
                fix_hint=(
                    "okg generation describe <generation_id>  OR  "
                    "okg generation list [--limit N]  OR  "
                    "okg generation compact --deployment <name>"
                ),
            ),
            as_json=as_json,
        )

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    # Chronos cutover: the chronos-native publisher no longer writes
    # okg.graph_generations rows — a published (or blocked) generation is a
    # Chronos CHECKPOINT whose `checkpoint_id` is a STRING id like
    # `okg:<deployment>:branch:default:gen:<ts>:<uuid>`. Resolve via the
    # checkpoint store (do NOT int()-cast; the store also resolves a bare-int
    # compat id to its `generation:<int>` checkpoint).
    generation_id = args.generation_id
    deployment = getattr(args, "deployment", None)
    # `--branch` omitted means "any branch" (issue #1439). Describe used to
    # resolve with branch='default' unconditionally and exposed no branch
    # option, so a fork checkpoint copied straight out of `generation list`
    # reported not-found even though the id was real and listed.
    branch = getattr(args, "branch", None)

    with db_connect(dsn, autocommit=True) as conn, \
         conn.cursor() as cur:
        from okg.substrate.generation_resolver import resolve_okg_generation

        resolved = resolve_okg_generation(
            conn,
            deployment_name=deployment,
            requested=generation_id,
            branch=branch,
            status=None,
            include_counts=True,
        )
        if resolved is None:
            return _emit_error(
                SubstrateError(
                    f"generation {generation_id} not found",
                    data={
                        "generation_id": generation_id,
                        "deployment_name": deployment,
                        "branch": branch,
                    },
                    fix_hint=(
                        "list recent generations: "
                        "okg generation list [--deployment X] [--branch B] "
                        "[--limit N]"
                    ),
                ),
                as_json=as_json,
            )

        # The integer `target_generation_id` token in checkpoint metadata is
        # the key the publisher uses for the consistency-gate side tables
        # (okg.consistency_gate_results.target_generation_id).
        meta = resolved.metadata
        gate_target_id = resolved.target_generation_id
        nodes_live = int(resolved.node_count or 0)
        edges_live = int(resolved.edge_count or 0)
        if isinstance(gate_target_id, int):
            consistency = _generation_consistency_diagnostics(
                cur,
                generation_id=gate_target_id,
            )
        else:
            consistency = {
                "gate_count": 0,
                "blocked_gate_count": 0,
                "degraded_gate_count": 0,
                "open_observation_count": 0,
                "gates": [],
            }

    invariant_summary = meta.get("invariant_summary")
    blocked_reason = meta.get("blocked_reason")
    # The publisher stamps blocked_reason={} when the generation published;
    # surface that as null so the JSON matches the legacy "no reason" shape.
    if not blocked_reason:
        blocked_reason = None
    try:
        repository_provenance, packaged_release = _checkpoint_release_identities(meta)
    except ValueError as exc:
        return _emit_error(
            SubstrateError(
                "generation checkpoint carries mixed release identities",
                data={"generation_id": resolved.generation_id, "detail": str(exc)},
                fix_hint="a checkpoint declares either repository provenance or a packaged release, never both",
            ),
            as_json=as_json,
        )
    payload = {
        **({"packaged_release": packaged_release.to_dict()} if packaged_release is not None else {}),
        # `generation_id` is the STRING checkpoint id (chronos-native).
        "generation_id": resolved.generation_id,
        "deployment_name": resolved.deployment_name,
        # The branch that published this checkpoint -- the same field
        # `generation list` emits, so a listed row round-trips.
        "branch": resolved.branch,
        "status": resolved.status,
        "blocked_reason": blocked_reason,
        # okg#3342: which sources were not current when this version was
        # published (status, failed and dependency-blocked sources); null for
        # a version published before the marker existed.
        "ingest_completeness": meta.get("ingest_completeness"),
        # Fact/edge watermarks have no chronos equivalent (no graph_generations
        # row); the chronos checkpoint is the generation boundary.
        "fact_watermark": None,
        "edge_fact_watermark": None,
        "catalog_version_id": resolved.catalog_version_id,
        # No published_at column on a checkpoint; created_at is the boundary.
        "published_at": resolved.created_at,
        "invariant_summary": invariant_summary,
        "substrate_integrity": (
            invariant_summary.get("substrate_integrity", {})
            if isinstance(invariant_summary, dict)
            else {}
        ),
        # No excluded_sources / correlation_id on the checkpoint.
        "excluded_sources": None,
        "correlation_id": None,
        "created_at": resolved.created_at,
        "nomos_policy_bundle_ids": list(
            meta.get("nomos_policy_bundle_ids") or []
        ),
        "nomos_policy_fingerprints": list(
            meta.get("nomos_policy_fingerprints") or []
        ),
        "publisher": meta.get("publisher"),
        "repository_provenance": repository_provenance.to_dict(),
        "core_revision": repository_provenance.core_revision,
        "core_distribution": repository_provenance.core_distribution,
        "deployment_revision": repository_provenance.deployment_revision,
        "deployment_tree_sha": repository_provenance.deployment_tree_sha,
        # Exact publisher-selected lineage. Incremental proof consumers must
        # resolve this checkpoint directly rather than infer a predecessor from
        # a later "latest generation" lookup.
        "predecessor_generation_id": meta.get("predecessor_generation_id"),
        "target_generation_id": gate_target_id,
        # node_count/edge_count from checkpoint metadata are the publisher's
        # recorded counts; nodes_live_at_gen/edges_live_at_gen are read back
        # from the pinned chronos checkout.
        "node_count": meta.get("node_count"),
        "edge_count": meta.get("edge_count"),
        "nodes_live_at_gen": nodes_live,
        "edges_live_at_gen": edges_live,
        # Delta-aware publish evidence (deltaflow-publish-projection): the
        # per-generation change receipt the publisher stamped into checkpoint
        # metadata — nodes/edges added/retracted/updated + the no-op flag.
        # Older checkpoints predate this and report null.
        "projection_deltas": meta.get("projection_deltas"),
        # Recovery is a one-predecessor, one-successor capability. Surface its
        # durable receipt so deployment lifecycle verification can prove that
        # the published successor consumed the exact authorization it issued.
        "count_stamp_recovery": meta.get("count_stamp_recovery"),
        "consistency": consistency,
    }
    if "deployment_attestations" in meta:
        payload["deployment_attestations"] = meta["deployment_attestations"]
    _emit_ok(payload, as_json=as_json)
    return 0


def _cmd_generation_list(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg generation list [--deployment X] [--limit N]` — list
    recent published generations from the Chronos checkpoint store.

    Added per ``align-substrate-cli-docs-and-runbooks`` Decision 1
    because the runbooks, the MCP session fix_hint, and operator
    intuition all assume the verb exists.

    Chronos cutover: published generations are Chronos checkpoints
    (``store.list_generations(...)``), not ``okg.graph_generations`` rows.
    """
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.cli.main import _emit_error, _resolve_dsn
    from okg.substrate.errors import SubstrateError

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    limit = max(1, min(int(getattr(args, "limit", 20) or 20), 500))
    deployment = getattr(args, "deployment", None)
    branch = getattr(args, "branch", None)

    with db_connect(dsn, autocommit=True) as conn:
        from okg.substrate.graph_store import ChronosGraphStore
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        # Omitting --deployment lists EVERY deployment, which is what the
        # flag's help has always claimed; the handler used to substitute
        # "default" instead, so the documented behavior was unreachable
        # (issue #1439). `branch=None` likewise means every branch.
        checkpoints = store.list_generations(
            deployment_name=deployment,
            branch=branch,
            status="published",
        )
        # Newest first, capped at limit (list_generations returns ascending).
        checkpoints = list(reversed(checkpoints))[:limit]
        rows = [
            {
                # `generation_id` is the STRING checkpoint id (chronos-native).
                "generation_id": cp.checkpoint_id,
                "deployment_name": (cp.metadata or {}).get("deployment_name"),
                # The branch that published this checkpoint. Listing spans
                # branches, and `generation describe` scopes by branch, so
                # every listed row must carry the scope needed to describe
                # it (issue #1439).
                "branch": (cp.metadata or {}).get("branch"),
                "status": (cp.metadata or {}).get("status"),
                "catalog_version_id": (
                    (cp.metadata or {}).get("catalog_version_id")
                ),
                # The checkpoint's created_at is the generation boundary; no
                # separate published_at column exists.
                "published_at": cp.created_at,
                "created_at": cp.created_at,
            }
            for cp in checkpoints
        ]

    if as_json:
        print(json.dumps({"generations": rows}, indent=2, default=str))
        return 0
    if not rows:
        print("(no generations found)")
        return 0
    print(
        f"{'gen_id':<52}  {'deployment':<24}  {'branch':<20}  {'status':<12}  "
        f"{'cat_ver':>8}  published_at"
    )
    for r in rows:
        print(
            f"{str(r['generation_id']):<52}  "
            f"{(r['deployment_name'] or '?'):<24}  "
            f"{(r['branch'] or '?'):<20}  "
            f"{(r['status'] or '?'):<12}  "
            f"{(r['catalog_version_id'] or 0):>8}  "
            f"{r['published_at'] or '(unpublished)'}"
        )
    return 0


def _cmd_generation_compact(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """Plan by default; apply only with the exact persisted plan id."""
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.cli.main import (
        _emit_error,
        _emit_ok,
        _resolve_dsn,
        enable_progress_logging,
    )
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )
    from okg.substrate.errors import SubstrateError
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.history_compaction import (
        apply_history_compaction_plan,
        create_history_compaction_plan,
        offline_reclaim_history_storage,
    )

    if bool(getattr(args, "progress", False)):
        enable_progress_logging()
    deployment = str(args.deployment)
    applying = bool(getattr(args, "apply", False))
    confirm = getattr(args, "confirm", None)
    if applying != bool(confirm):
        return _emit_error(
            SubstrateError(
                "generation compact requires --apply and --confirm <plan-id> together",
                fix_hint="run dry-run first, then rerun with both flags",
            ),
            as_json=as_json,
        )
    if bool(getattr(args, "offline_reclaim", False)) and not applying:
        return _emit_error(
            SubstrateError("--offline-reclaim is valid only while applying a confirmed plan"),
            as_json=as_json,
        )
    if bool(getattr(args, "offline_reclaim", False)) and not bool(
        getattr(args, "workers_stopped", False)
    ):
        return _emit_error(
            SubstrateError(
                "--offline-reclaim requires --workers-stopped before compaction starts"
            ),
            as_json=as_json,
        )
    if bool(getattr(args, "workers_stopped", False)) and not bool(
        getattr(args, "offline_reclaim", False)
    ):
        return _emit_error(
            SubstrateError("--workers-stopped is valid only with --offline-reclaim"),
            as_json=as_json,
        )
    try:
        dsn = _resolve_dsn(args)
        manifest = resolve_deployment(deployment)
        chronos = manifest.chronos or {}
        policy_raw = chronos.get("history_retention")
        with db_connect(dsn) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            if applying:
                result = apply_history_compaction_plan(
                    conn,
                    store=store,
                    plan_id=str(confirm),
                    confirm=str(confirm),
                    apply=True,
                    policy_raw=policy_raw,
                    expected_deployment_name=deployment,
                    expected_branch=str(getattr(args, "branch", "default")),
                    batch_size=int(getattr(args, "batch_size", 5000)),
                )
                if bool(getattr(args, "offline_reclaim", False)):
                    result["offline_reclaim"] = offline_reclaim_history_storage(
                        conn,
                        plan_id=str(confirm),
                        workers_stopped=bool(getattr(args, "workers_stopped", False)),
                    )
            else:
                result = create_history_compaction_plan(
                    conn,
                    store=store,
                    deployment_name=deployment,
                    branch=str(getattr(args, "branch", "default")),
                    policy_raw=policy_raw,
                )
    except DeploymentResolutionError as err:
        return _emit_error(SubstrateError(str(err)), as_json=as_json)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    # A default the operator never wrote must never be applied quietly. It is
    # in the JSON as `policy_source`/`policy_notice`, and it is said again here
    # so it survives a skim of a long plan (issue #1309).
    if result.get("policy_source") == "built_in_default":
        notice = result.get("policy_notice")
        if notice:
            print(notice, file=sys.stderr)
    _emit_ok(result, as_json=as_json)
    return 0


__all__ = [
    "_cmd_generation",
    "_cmd_generation_compact",
    "_cmd_generation_list",
]
