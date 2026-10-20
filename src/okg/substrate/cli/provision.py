"""CLI handler for `okg provision` — one-command deployment bootstrap.

Per ``canonical-dev-okg`` (task.cdo.bootstrap-command). Stands a deployment up
from an empty database in one command:

    ensure required extensions -> migrate schema -> catalog load --apply
    -> ingest (inline) + publish -> preflight gate

so a developer brings the canonical dev graph up without the manual,
failure-at-a-time sequence. Reuses the substrate's existing operations
(`db.migrate.migrate` / `ensure_required_extensions`, the catalog-load handler,
`ingest.service.run_ingest`) rather than reimplementing them.

cli.main helpers are loaded lazily via the shared ``_load_main_helpers()``
pattern (per the extract-cli-* waves), with the None-guard so test
monkeypatches survive the first invocation.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import math
import os
import stat
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any


# Module-level helper bindings, populated by _load_main_helpers().
_emit_error: Any = None
_emit_ok: Any = None
_resolve_dsn: Any = None
SubstrateError: Any = None


def _load_main_helpers() -> None:
    global _emit_error, _emit_ok, _resolve_dsn, SubstrateError
    from okg.substrate.cli.main import (
        _emit_error as _e,
        _emit_ok as _o,
        _resolve_dsn as _r,
    )
    from okg.substrate.errors import SubstrateError as _se
    if _emit_error is None:
        _emit_error = _e
    if _emit_ok is None:
        _emit_ok = _o
    if _resolve_dsn is None:
        _resolve_dsn = _r
    if SubstrateError is None:
        SubstrateError = _se


def _apply_catalog(deployment_dir, *, dsn: str) -> dict:
    """Compose + apply the deployment catalog, reusing the catalog-load handler
    (which owns the lease + apply path). Captures its JSON so `okg provision`
    emits a single summary. Raises SubstrateError on failure."""
    from okg.substrate.cli.catalog import _cmd_catalog_load

    ns = argparse.Namespace(
        deployment=str(deployment_dir),
        apply=True,
        dsn=dsn,
        owner_id=None,
        owner_meta=None,
    )
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = _cmd_catalog_load(ns, as_json=True)
    out = buf.getvalue().strip()
    if rc != 0:
        raise SubstrateError(
            "catalog load --apply failed during provision",
            data={"catalog_output": out[:500]},
            fix_hint="run `okg catalog load --deployment <dir> --apply` directly "
                     "to see the full error",
        )
    # Fail closed: a zero-exit catalog load whose JSON does not parse (or which
    # emitted nothing) must NOT be coerced to an empty payload. An empty {}
    # silently yields a null catalog_version_id, which provision would then
    # treat as a (failed) success. Refuse loudly instead.
    try:
        parsed = json.loads(out.splitlines()[-1]) if out else None
    except json.JSONDecodeError as exc:
        raise SubstrateError(
            "catalog load --apply returned unparseable output during provision",
            data={"catalog_output": out[:500]},
            fix_hint="run `okg catalog load --deployment <dir> --apply` directly "
                     "to see the full error",
        ) from exc
    if not isinstance(parsed, dict):
        raise SubstrateError(
            "catalog load --apply returned no catalog payload during provision",
            data={"catalog_output": out[:500]},
            fix_hint="run `okg catalog load --deployment <dir> --apply` directly "
                     "to see the full error",
        )
    return parsed


def _claim_rebound_staging_catalog_ownership(
    deployment: str,
    *,
    deployment_dir: Path,
    dsn: str,
) -> dict[str, Any] | None:
    """Claim catalog writer ownership for a verified sealed staging clone.

    Clone rebind deliberately removes the production writer claim while
    preserving catalog history. The first sealed staging provision must claim
    that history for its exact catalog identity before catalog load can write.
    No unsealed or non-clone provision is allowed onto this force-claim path.
    """
    descriptor = os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR")
    release_root = os.environ.get("OKG_RELEASE_ROOT")
    if not descriptor and not release_root:
        return None
    if not descriptor or not release_root:
        raise SubstrateError(
            "sealed deployment resolution environment is incomplete",
            fix_hint="run provision through the environment release launcher",
        )

    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
    )
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.deployments.environment import load_environment

    environment = load_environment(descriptor, environ=os.environ)
    if deployment not in {
        environment.id,
        environment.graph_deployment_name,
    }:
        raise SubstrateError(
            "sealed provision deployment does not match its environment",
            data={
                "requested": deployment,
                "environment_id": environment.id,
                "graph_deployment_name": environment.graph_deployment_name,
            },
        )
    if not environment.readiness.requires_clone_rebind:
        return None
    if environment.tier != "staging":
        raise SubstrateError(
            "clone-rebind catalog ownership claim requires a staging environment"
        )

    rebind_path = environment.runtime.state_root / "clone-rebind.json"
    try:
        if rebind_path.is_symlink() or not rebind_path.is_file():
            raise SubstrateError(
                "sealed staging provision requires clone-rebind evidence"
            )
        if stat.S_IMODE(rebind_path.stat().st_mode) != 0o600:
            raise SubstrateError("clone-rebind evidence mode is not 0600")
        report = json.loads(rebind_path.read_text(encoding="utf-8"))
    except SubstrateError:
        raise
    except (OSError, ValueError, TypeError) as exc:
        raise SubstrateError("clone-rebind evidence is unreadable") from exc

    target = report.get("target") if isinstance(report, dict) else None
    if (
        not isinstance(report, dict)
        or report.get("schema") != "okg.environment_clone_rebind/v1"
        or report.get("ok") is not True
        or report.get("apply") is not True
        or report.get("ordinary_sessions_read_only") is not False
        or not isinstance(target, dict)
        or target.get("environment_id") != environment.id
        or target.get("graph_deployment_name")
        != environment.graph_deployment_name
        or target.get("database") != environment.database.name
    ):
        raise SubstrateError("clone-rebind evidence is incomplete or foreign")

    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.generation_bootstrap import (
        formal_bootstrap_generation_inventory,
    )

    manifest = resolve_deployment(deployment)
    identity = catalog_identity_for(
        composed=compose_catalog(manifest),
        deployment_dir=deployment_dir,
        command="okg provision clone-rebind ownership claim",
    )
    with db_connect(dsn, autocommit=True) as conn:
        owner = conn.execute(
            "SELECT owner_deployment_name FROM okg.database_owner WHERE singleton"
        ).fetchone()
        generation_inventory = formal_bootstrap_generation_inventory(conn)
        generation_count = int(generation_inventory["non_bootstrap_count"])
        if owner is None or owner[0] != environment.graph_deployment_name:
            raise SubstrateError(
                "clone-rebind database owner does not match staging"
            )
        if generation_inventory["ok"] is not True:
            raise SubstrateError(
                "clone-rebind database has a malformed bootstrap generation",
                data=generation_inventory,
            )
        if generation_count != 0:
            raise SubstrateError(
                "clone-rebind catalog ownership claim requires zero "
                "non-bootstrap generations",
                data={"generation_count": generation_count},
            )
        claimed = claim_catalog_ownership(
            conn,
            identity=identity,
            force=True,
        )
    claimed_owner = claimed.get("owner") or {}
    if claimed_owner.get("owner_fingerprint") != identity.writer_fingerprint:
        raise SubstrateError(
            "staging catalog ownership claim did not persist",
            data={"status": claimed.get("status")},
        )
    return {
        "step": "catalog_ownership",
        "status": "claimed_pending_catalog_load",
        "deployment_name": claimed.get("deployment_name"),
        "source_backup_digest": report.get("source_backup_digest"),
    }


def _seed_nomos_baseline(
    deployment: str,
    deployment_dir,
    *,
    dsn: str,
    raw_manifest: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Seed the deployment's Nomos baseline exactly as `okg up` does.

    Mirrors ``deployment_upgrade._phase_nomos_baseline`` (same
    ``_manifest_requires_nomos_runtime`` guard, same ``code_repos``
    expansion, same audit + ``seed_deployment_baseline_nomos_state`` call,
    idempotent per ``nomos/store.py``), so a fresh enforce-mode deployment
    provisions without ``nomos.default_deny.no_active_bundle`` rejections
    and repeat provisions / provision-then-up sequences do not churn the
    active bundle. The registry is read from
    ``<deployment_dir>/source_registry.yaml`` — the same file provision's
    own ingest reads (``ingest.service.run_ingest``) — so the baseline
    covers exactly the sources this provision will run.

    Returns None when the manifest does not require the Nomos runtime
    (the step is not applicable); raises SubstrateError when the policy
    audit fails or the seed does not yield an active policy + config.
    """
    from pathlib import Path

    from okg.substrate import fastyaml
    from okg.substrate.deployment_upgrade import (
        _manifest_requires_nomos_runtime,
        _nomos_policy_source_ids,
        _nomos_source_policies,
    )

    deployment_dir = Path(deployment_dir)
    manifest_path = deployment_dir / "deployment.yaml"
    if raw_manifest is None:
        raw_manifest = (
            fastyaml.safe_load(manifest_path.read_text())
            if manifest_path.is_file()
            else {}
        ) or {}
    else:
        raw_manifest = dict(raw_manifest)
    if not _manifest_requires_nomos_runtime(raw_manifest):
        return None
    baseline_spec = None
    if manifest_path.is_file():
        from okg.substrate.nomos.baseline import deployment_baseline_spec

        baseline_spec = deployment_baseline_spec(
            deployment=deployment,
            deployment_dir=deployment_dir,
        )
        if not baseline_spec.get("required"):
            return None
        raw_manifest = baseline_spec["manifest"]
    else:
        nomos = raw_manifest.get("nomos") or {}
        if (
            isinstance(nomos, dict)
            and nomos.get("source_binding_policy") is not None
        ):
            raise SubstrateError(
                "environment Nomos seeding cannot resolve a deployment-owned "
                "source_binding_policy without deployment.yaml",
                fix_hint=(
                    "seed this policy through a deployment directory or remove "
                    "the unsupported environment policy pointer"
                ),
            )

    from okg.substrate.deployments import resolve_source_registry_path

    registry_path = resolve_source_registry_path(deployment_dir, raw_manifest)
    raw_registry = (
        fastyaml.safe_load(registry_path.read_text())
        if registry_path.is_file()
        else {}
    ) or {}
    from okg.substrate.sources.registry import (
        SourceRegistryUnknownKeyError,
        check_legacy_registry_keys,
    )

    try:
        check_legacy_registry_keys(raw_registry, registry_path)
    except SourceRegistryUnknownKeyError as exc:
        err = SubstrateError(
            f"source_registry.yaml is invalid: {exc}",
            data={"detail_code": exc.code},
            fix_hint="fix the key the message names in source_registry.yaml",
        )
        err.code = "source_registry_invalid"
        raise err from exc
    # Expand `code_repos:` into `sources` so the baseline covers the code
    # sources exactly as the rollout/audit/readiness paths do (else provision
    # seeds a bundle with no code-source allow rule and the first code publish
    # hits default_deny) — same expansion as `_phase_nomos_baseline`.
    if isinstance(raw_registry, dict) and raw_registry.get("code_repos"):
        from okg.substrate.sources.registry import expand_code_repos_sources

        raw_registry = {
            **raw_registry,
            "sources": expand_code_repos_sources(
                raw_registry,
                registry_path,
                allow_unresolved_base=True,
            ),
        }

    from okg.substrate.nomos import (
        audit_deployment_policy,
        seed_deployment_baseline_nomos_state,
    )

    audit = audit_deployment_policy(
        raw_manifest,
        source_registry=raw_registry,
        deployment=deployment,
    )
    if not audit.get("ok"):
        raise SubstrateError(
            "Nomos deployment policy audit failed during provision",
            data={"findings": audit.get("findings")},
            fix_hint=(
                "fix the manifest's nomos.deployment_posture / source "
                "policies (see findings), then re-run provision"
            ),
        )

    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.sources.registry import (
        runtime_source_subject_ids as _runtime_source_subject_ids,
    )

    if baseline_spec is not None:
        seed_arguments = {
            "manifest": baseline_spec["manifest"],
            "source_names": baseline_spec["source_names"],
            "source_policies": baseline_spec["source_policies"],
            "runtime_source_names": baseline_spec["runtime_source_names"],
            "policy_graph": baseline_spec["policy_graph"],
        }
    else:
        seed_arguments = {
            "manifest": raw_manifest,
            "source_names": _nomos_policy_source_ids(
                raw_registry,
                deployment=deployment,
                deployment_dir=deployment_dir,
            ),
            "source_policies": _nomos_source_policies(
                raw_registry,
                audit.get("source_policies") or {},
            ),
            "runtime_source_names": sorted(
                _runtime_source_subject_ids(raw_registry.get("sources") or {})
            ),
        }

    with db_connect(dsn) as conn:
        payload = seed_deployment_baseline_nomos_state(
            conn,
            deployment_name=deployment,
            **seed_arguments,
        )
        conn.commit()
    if not payload.get("ok"):
        raise SubstrateError(
            "Nomos baseline seed did not yield an active policy + config "
            "during provision",
            data={
                "active_policy": payload.get("active_policy"),
                "active_config": payload.get("active_config"),
            },
            fix_hint=(
                "inspect `okg nomos` state for this deployment; the "
                "baseline activation failed"
            ),
        )
    # fix-nomos-read-path-ux (req.readux.mcp-subject-default, #640): the
    # bring-up output names the MCP subject contract so non-default setups
    # are discoverable here instead of at the first denied mcp.tool.call.
    from okg.substrate.nomos import mcp_subject_hint_for_manifest

    payload["mcp_subject_hint"] = mcp_subject_hint_for_manifest(raw_manifest)
    return payload


def _failed_source_summaries(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Per-source failure payloads from the ingest report.

    ``run_ingest`` collapses per-source outcomes into ``any_failed`` for its
    own exit code; the raw per-source results (with ``error`` and
    ``admission_reject_reason``) ride in ``report["results"]``. Surface them
    so a failed provision names each failing source and WHY (per
    ``fix-provision-enforce-bringup`` req.provision.surface-reasons).
    """
    failed: list[dict[str, Any]] = []
    for result in report.get("results", []) or []:
        if not isinstance(result, dict) or result.get("status") == "OK":
            continue
        entry: dict[str, Any] = {
            "name": result.get("name"),
            "status": result.get("status"),
        }
        for key in ("stage", "error", "admission_reject_reason"):
            if result.get(key):
                entry[key] = result[key]
        failed.append(entry)
    return failed


def _distinct_failure_reasons(
    failed_sources: list[dict[str, Any]],
) -> list[str]:
    """Distinct reason codes/errors across the failing sources, in order."""
    reasons: list[str] = []
    for entry in failed_sources:
        reason = entry.get("admission_reject_reason") or entry.get("error")
        if reason and str(reason) not in reasons:
            reasons.append(str(reason))
    return reasons


def _numeric_timings(value: object) -> dict[str, int | float]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): timing
        for key, timing in value.items()
        if (
            isinstance(timing, (int, float))
            and not isinstance(timing, bool)
            and math.isfinite(float(timing))
        )
    }


def _final_publish_failure(
    deployment: str,
    publish: dict[str, Any],
    *,
    steps: list[dict[str, Any]] | None = None,
) -> SubstrateError:
    """Wrap a refused final publish in an error that keeps the publisher's
    `error`, `error_data`, and `fix_hint` verbatim, plus the provision
    steps that already completed so the summary is not lost."""
    # Imported here, not through the lazily bound module global, so the
    # helper also works before `_load_main_helpers()` has run.
    from okg.substrate.errors import SubstrateError

    status = publish.get("status")
    reason = publish.get("error")
    message = f"final publish did not publish (status={status!r})"
    if reason:
        message = f"{message}: {reason}"
    return SubstrateError(
        message,
        data={
            "deployment": deployment,
            "publish_status": status,
            "publish_label": publish.get("label"),
            "generation_id": publish.get("generation_id"),
            "error": publish.get("error"),
            "error_code": publish.get("code"),
            "error_data": publish.get("error_data"),
            "fix_hint": publish.get("fix_hint"),
            "steps": list(steps or []),
        },
        fix_hint=(
            publish.get("fix_hint")
            or "read `error` and `error_data`, fix the named cause, then run "
            "okg provision again"
        ),
    )


def _final_publish_timing_evidence(
    publish: dict[str, Any],
    *,
    wall_ms: float,
) -> dict[str, Any]:
    reported_ms = publish.get("total_duration_ms")
    timings_ms = _numeric_timings(publish.get("timings_ms"))
    if (
        not isinstance(reported_ms, (int, float))
        or isinstance(reported_ms, bool)
        or not math.isfinite(float(reported_ms))
        or float(reported_ms) < 0.0
        or "total" not in timings_ms
        or len(timings_ms) < 2
    ):
        raise ValueError(
            "final publish omitted finite total and stage timing evidence"
        )
    return {
        "status": publish.get("status"),
        "wall_ms": wall_ms,
        "reported_ms": round(float(reported_ms), 3),
        "timings_ms": timings_ms,
    }


def _ingest_timing_evidence(
    report: dict[str, Any],
    *,
    final_publish_timing: dict[str, Any] | None = None,
    pipeline_total_ms: float | None = None,
) -> dict[str, Any]:
    """Retain the ingest service's existing timing evidence in provision JSON."""
    source_timings = []
    for result in report.get("results") or []:
        if not isinstance(result, dict):
            continue
        source_timings.append({
            key: result.get(key)
            for key in (
                "name",
                "status",
                "stage",
                "elapsed_s",
                "timing_status",
                "timings_ms",
            )
        })
    timings = _numeric_timings(report.get("timings_ms"))
    layer_timings = [
        dict(layer)
        for layer in (report.get("layer_timings") or [])
        if isinstance(layer, dict)
    ]
    if final_publish_timing is not None:
        publish_wall_ms = float(final_publish_timing["wall_ms"])
        publish_reported_ms = float(final_publish_timing["reported_ms"])
        timings["publish_wall"] = round(
            float(timings.get("publish_wall") or 0.0) + publish_wall_ms,
            3,
        )
        timings["publish_reported_total"] = round(
            float(timings.get("publish_reported_total") or 0.0)
            + publish_reported_ms,
            3,
        )
        layer_timings.append({
            "index": "final",
            "sources": [],
            "source_wall_ms": 0.0,
            "source_reported_total_ms": 0.0,
            "publish_wall_ms": publish_wall_ms,
            "publish_reported_ms": publish_reported_ms,
            "publish_status": final_publish_timing.get("status"),
            "publish_timings_ms": final_publish_timing.get("timings_ms"),
        })
    if pipeline_total_ms is not None:
        timings["total"] = round(float(pipeline_total_ms), 3)
        timings["orchestration_overhead"] = round(
            max(
                0.0,
                float(pipeline_total_ms)
                - float(timings.get("source_wall") or 0.0)
                - float(timings.get("publish_wall") or 0.0),
            ),
            3,
        )
    evidence = {
        "timing_status": report.get("timing_status"),
        "timings_ms": timings,
        "layer_timings": layer_timings,
        "source_timings": source_timings,
    }
    if final_publish_timing is not None:
        evidence["final_publish_timing"] = final_publish_timing
    return evidence


def _apply_deferred_views_registry(
    deployment: str, *, dsn: str, generation_id: Any,
) -> dict:
    """Project the views registry a fresh deployment's catalog load deferred.

    `okg catalog load` defers the ``view_definition`` nodes while no
    generation is published. Provision calls this after its first publish, so
    the nodes land against that new generation in the same run (#2249).
    The target resolves exactly as `_apply_catalog` resolves it.
    """
    import psycopg

    from okg.substrate.cli.catalog import _resolve_catalog_target
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.views.registry import (
        apply_views_registry,
        load_views_registry,
    )

    target = _resolve_catalog_target(
        argparse.Namespace(deployment=deployment, dsn=dsn),
    )
    entries = load_views_registry(target.deployment_dir)
    try:
        with db_connect(dsn, autocommit=True) as conn:
            applied = apply_views_registry(
                conn,
                entries,
                deployment_name=target.composed.deployment_name,
                generation_id=generation_id,
            )
    except psycopg.Error as exc:
        raise SubstrateError(
            f"views_registry apply failed during provision: {exc}",
            data={"deployment": deployment, "generation_id": generation_id},
            fix_hint=(
                "the generation was published without its view definitions; "
                "fix the cause and re-run `okg provision`, which applies them"
            ),
        ) from exc
    return {
        "entries": len(entries),
        "upserted": applied.upserted,
        "retracted": applied.retracted,
        "generation_id": applied.generation_id,
    }


def _latest_published_generation(
    dsn: str,
    *,
    deployment_name: str,
) -> int | str | None:
    """Latest published generation id/checkpoint in the database (or None).

    Used so an idempotent re-provision (which publishes nothing new because the
    source dedups an unchanged repo) still reports the current generation rather
    than reading as a failure. Per ``canonical-dev-okg``.
    """
    from okg.substrate.db.connection import connect as db_connect

    try:
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.connect(dsn, ensure_schema=False)
        try:
            latest = store.latest_generation(
                deployment_name=deployment_name,
                status="published",
            )
        finally:
            store.close()
        if latest is not None:
            return latest.checkpoint_id
    except Exception:  # noqa: BLE001 — fall back to legacy graph_generations
        pass

    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.graph_generations')")
            if cur.fetchone()[0] is None:
                return None
            cur.execute(
                "SELECT max(generation_id) FROM okg.graph_generations "
                "WHERE status = 'published'"
            )
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else None


def _is_no_source_work_skip(publish: Any) -> bool:
    return (
        isinstance(publish, dict)
        and publish.get("status") == "skipped"
        and publish.get("publish_skipped") is True
        and publish.get("reason") == "no_source_work"
    )


def _judged_publish(publishes: list[dict[str, Any]]) -> dict[str, Any]:
    """The publish provision's proof must judge (#1889).

    Normally the last one. A retry after a crash first republishes the work
    the crashed attempt staged; its own sources then add nothing, so the
    run ends with a validated ``no_source_work`` skip. Judging that skip
    refused a run that did publish. Only then is the latest earlier
    ``published`` entry judged, and only when every other entry is a real
    publish or that same skip: any refused, blocked or failed publish keeps
    the last entry, so the proof still fails closed.
    """
    if not publishes:
        return {}
    last = publishes[-1]
    if not _is_no_source_work_skip(last):
        return last
    earlier = publishes[:-1]
    if not all(
        item.get("status") == "published" or _is_no_source_work_skip(item)
        for item in earlier
    ):
        return last
    published = [item for item in earlier if item.get("status") == "published"]
    return published[-1] if published else last


def _in_deployment_binding_scope(handler):
    """Run ``handler`` inside `deployment_binding_scope()`, so a manifest name
    a folder-valued ``--deployment`` resolves to keeps reaching that folder for
    the whole command, and the binding ends with it."""
    import functools

    @functools.wraps(handler)
    def wrapper(*args, **kwargs):
        from okg.substrate.deployments import deployment_binding_scope

        with deployment_binding_scope():
            return handler(*args, **kwargs)

    return wrapper


@_in_deployment_binding_scope
def _cmd_provision(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg provision --deployment <name> [--include <source> ...]`.

    Bring a deployment's database up from empty to a published generation in one
    command, then confirm readiness with the preflight.

    ``--deployment`` may also be a deployment folder or ``deployment.yaml``
    path; it is resolved to the manifest name once, below.
    """
    _load_main_helpers()
    from okg.substrate.db.management import bootstrap_preflight
    from okg.substrate.db.migrate import ensure_required_extensions, migrate
    from okg.substrate.deployments import (
        find_deployment_dir,
        inject_deployment_path,
        resolve_deployment,
    )
    from okg.substrate.cli.deployment import AGENT_CONTEXT_READY_SOURCES
    from okg.substrate.ingest.service import IngestOptions, run_ingest, _publish

    deployment = getattr(args, "deployment", None) or os.environ.get(
        "OKG_DEPLOYMENT"
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg provision requires --deployment (or OKG_DEPLOYMENT)",
                fix_hint="okg provision --deployment <name> --json",
            ),
            as_json=as_json,
        )
    # A folder or manifest path becomes the manifest name here, once, so the
    # migration, catalog ownership, source progress, and publish below all
    # use one deployment identity. Passing the raw path on recorded rows
    # under the path and refused the publish with
    # `catalog_writer_scope_conflict`.
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        canonical_deployment_reference,
    )

    try:
        deployment, deployment_dir = canonical_deployment_reference(
            str(deployment),
        )
    except DeploymentResolutionError as exc:
        return _emit_error(
            SubstrateError(
                str(exc),
                data={"deployment": str(deployment)},
                fix_hint=(
                    "pass a deployment name, a deployment folder, or a "
                    "deployment.yaml path"
                ),
            ),
            as_json=as_json,
        )
    if deployment_dir is None:
        deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None:
        return _emit_error(
            SubstrateError(
                f"deployment dir for {deployment!r} not found",
                fix_hint="check deployments/<name>/ exists",
            ),
            as_json=as_json,
        )
    packaged_stage_only = bool(getattr(args, "_packaged_stage_only", False))
    # Observed by the packaged installer BEFORE this refresh ran its sources:
    # the already-published populated generation a zero-addition repeat must
    # bind to. Never set by the CLI; an in-process caller may only supply it
    # inside an active verified packaged install, like ``_packaged_stage_only``.
    packaged_replay_predecessor = getattr(args, "_packaged_replay_predecessor", None)
    # Issue #1914: the packaged installer read the deployment's registry once and
    # found that no source declares relationship output. Records-only publishes
    # then satisfy the row-delta gate with nodes alone. Like the predecessor
    # above, an in-process caller may only supply it inside an active verified
    # packaged install; the CLI never sets it.
    # None means "no packaged caller supplied a declaration": ordinary provision
    # and `okg init` read no registry, so their verdict says nothing about the
    # rule rather than stamping an unread one.
    packaged_records_only = getattr(args, "_packaged_records_only", None)
    if packaged_records_only is not None:
        packaged_records_only = bool(packaged_records_only)
    if (packaged_stage_only or packaged_replay_predecessor is not None
            or packaged_records_only is not None):
        from okg.substrate.distributions.packaged_release import current_verified_release
        if current_verified_release(deployment_dir) is None:
            return _emit_error(SubstrateError("staging requires an active verified packaged install"), as_json=as_json)
    inject_deployment_path(deployment_dir)
    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    steps: list[dict[str, Any]] = []
    generation_id = None
    any_failed = False
    catalog_version_id: Any = None
    last_publish: dict[str, Any] = {}
    failed_sources: list[dict[str, Any]] = []
    failure_reasons: list[str] = []
    # #3003: hold the deployment's source-sync signal from the source runs
    # through `--publish-once`'s final publish; `run_ingest` reuses it.
    run_guard = contextlib.ExitStack()
    try:
        ext = ensure_required_extensions(dsn)
        steps.append({
            "step": "extensions",
            "created": ext["created"],
            "already_present": len(ext["already_present"]),
        })

        mig = migrate(dsn, deployment_name=deployment)
        steps.append({
            "step": "migrate",
            "statements_executed": mig.statements_executed,
        })

        ownership = _claim_rebound_staging_catalog_ownership(
            deployment,
            deployment_dir=Path(deployment_dir),
            dsn=dsn,
        )
        if ownership is not None:
            steps.append(ownership)

        # Preserve an environment id through catalog composition. Passing the resolved
        # shared definition directory here collapses staging back to the definition's
        # production graph name and checks ownership against the wrong writer.
        catalog = _apply_catalog(deployment, dsn=dsn)
        catalog_version_id = catalog.get("catalog_version_id")
        steps.append({
            "step": "catalog",
            "catalog_version_id": catalog_version_id,
            "subtype_count": catalog.get("subtype_count"),
        })
        views_deferred = bool(
            (catalog.get("views_registry") or {}).get("deferred")
        )

        # Seed the Nomos baseline BEFORE ingest for enforce/audit-mode
        # manifests — the same idempotent seed `okg up` runs — so a fresh
        # enforce-mode deployment does not default-deny every source run
        # (nomos.default_deny.no_active_bundle). Not-required manifests skip
        # the step entirely (no step entry), keeping the provision surface
        # unchanged for non-Nomos deployments.
        baseline = _seed_nomos_baseline(deployment, deployment_dir, dsn=dsn)
        if baseline is not None:
            active_policy = baseline.get("active_policy") or {}
            steps.append({
                "step": "nomos_baseline",
                "ok": bool(baseline.get("ok")),
                "policy_bundle_id": active_policy.get("policy_bundle_id"),
                "policy_fingerprint": active_policy.get("policy_fingerprint"),
                # None on an idempotent re-seed: nothing was (re)activated.
                "activated": baseline.get("policy_activation") is not None,
                # The MCP subject contract for this baseline (#640): which
                # subject okg mcp-serve presents by default, or which env
                # var must name a granted subject.
                "mcp_subject_hint": baseline.get("mcp_subject_hint"),
            })

        include = (
            set(args.include) if getattr(args, "include", None) else None
        )
        readiness_sources = bool(getattr(args, "readiness_sources", False))
        if readiness_sources:
            if include:
                raise SubstrateError(
                    "--readiness-sources cannot be combined with --include",
                    fix_hint="use either the standardized readiness set or an "
                             "explicit --include list",
                )
            include = set(AGENT_CONTEXT_READY_SOURCES)
        include_predecessors = not bool(getattr(args, "no_predecessors", False))
        source_workers = getattr(args, "source_workers", None)
        if source_workers is None and readiness_sources:
            source_workers = 1
        publish_once = bool(getattr(args, "publish_once", False)) or readiness_sources
        try:
            deployment_manifest = resolve_deployment(deployment)
        except Exception as exc:  # noqa: BLE001 - source authority must fail closed.
            raise SubstrateError(
                "could not resolve deployment manifest for provision",
                data={
                    "deployment": deployment,
                    "error": f"{type(exc).__name__}: {exc}",
                },
                fix_hint=(
                    "fix the deployment/environment descriptor before provision; "
                    "provision must not silently ignore sync_root"
                ),
            ) from exc
        # Same rule one level down: a DECLARED sync_root this machine cannot
        # resolve must not read as None and silently pick a default repo root
        # (issue #1014).
        from okg.substrate.deployments.sync_root import (
            require_resolved_sync_root,
        )

        require_resolved_sync_root(deployment_manifest, command="provision")
        ingest_options = IngestOptions(
            dsn=dsn,
            deployment=deployment,
            deployment_dir=deployment_dir,
            include=include,
            include_predecessors=include_predecessors,
            run_mode="scope_complete",
            run_modes_by_source=getattr(args, "_packaged_source_modes", {}),
            publish=not publish_once and not packaged_stage_only,
            # #1889: `--publish-once` turns ingest's own publishes off, which
            # also skipped the guarded pre-source republish of work a crashed
            # earlier attempt staged. Every source then refused its zero-output
            # rerun (`prior_emission_unpublished`) and the final publish never
            # ran, so retries wedged. A stage-only packaged run never publishes.
            redrive_staged_before_sources=(
                publish_once and not packaged_stage_only
            ),
            command="okg provision",
            max_source_workers=source_workers,
            source_registry_repo_root_override=deployment_manifest.sync_root,
        )
        ingest_started = time.perf_counter()
        from okg.substrate.runtime.source_sync_priority import ingest_run_guard

        run_guard.enter_context(ingest_run_guard(
            dsn=dsn,
            deployment=deployment,
            deployment_dir=deployment_dir,
            hold=True,
            wait_seconds=None,
        ))
        result = run_ingest(ingest_options)
        any_failed = result.any_failed
        failed_sources = _failed_source_summaries(result.report)
        failure_reasons = _distinct_failure_reasons(failed_sources)
        publishes = list(result.report.get("publishes", []))
        final_publish_timing: dict[str, Any] | None = None
        if publish_once and not any_failed and not packaged_stage_only:
            final_publish_started = time.perf_counter()
            final_publish = _publish(ingest_options, "final")
            final_publish_wall_ms = round(
                max(
                    0.0,
                    (time.perf_counter() - final_publish_started) * 1000.0,
                ),
                3,
            )
            if final_publish.get("status") == "fail":
                # `_publish` reports a refusal (release authority, framework
                # provenance, catalog ownership, ...) as a `fail` dict instead
                # of raising. Surface the publisher's own reason; asking a
                # failed publish for timing evidence would only report the
                # missing timings. A `blocked` cycle is a real generation with
                # timings and a structured `blocked_reason`, so it continues
                # to the publish proof below.
                raise _final_publish_failure(
                    deployment, final_publish, steps=steps,
                )
            try:
                final_publish_timing = _final_publish_timing_evidence(
                    final_publish,
                    wall_ms=final_publish_wall_ms,
                )
            except ValueError as exc:
                raise SubstrateError(
                    "final publish timing evidence is unavailable",
                    data={
                        "deployment": deployment,
                        "publish_status": final_publish.get("status"),
                    },
                    fix_hint=(
                        "publisher summaries must retain finite "
                        "total_duration_ms and per-stage timings"
                    ),
                ) from exc
            publishes.append(final_publish)
            if final_publish.get("status") != "published":
                any_failed = True
        last_publish = _judged_publish(publishes)
        if publishes:
            generation_id = last_publish.get("generation_id")
        ingest_step: dict[str, Any] = {
            "step": "ingest",
            "source_scope": (
                "agent-context-readiness" if readiness_sources else "custom"
            ),
            "ordered": result.report.get("ordered", []),
            "include_predecessors": include_predecessors,
            "publish_strategy": "final" if publish_once else "per_layer",
            "source_workers": source_workers,
            "generation_id": generation_id,
            "publishes": len(publishes),
            # surfaced so a re-run on an unchanged repo is visibly idempotent
            # (nodes_added == 0, noop == true) — per canonical-dev-okg.
            "nodes_added": last_publish.get("nodes_added"),
            "noop": last_publish.get("noop"),
            "any_failed": any_failed,
            # Per-source terminal admission verdicts (names, enums, ids only):
            # the pipeline's own caught-up-vs-wedged decision, retained so a
            # zero-addition repeat can be judged from evidence, not counters.
            "source_admissions": _source_admission_summaries(
                result.report.get("results", [])
            ),
            **_ingest_timing_evidence(
                result.report,
                final_publish_timing=final_publish_timing,
                pipeline_total_ms=round(
                    max(0.0, (time.perf_counter() - ingest_started) * 1000.0),
                    3,
                ),
            ),
        }
        # A bare `any_failed: True` is nonconformant: name each failing
        # source and its admission_reject_reason/error, plus the distinct
        # reason codes (req.provision.surface-reasons).
        if failed_sources:
            ingest_step["failed_sources"] = failed_sources
            ingest_step["failure_reasons"] = failure_reasons
        failed_publishes = [item for item in publishes if item.get("status") not in {"published", "skipped"}]
        if failed_publishes:
            from okg.substrate.sources.redaction import redact_mapping
            ingest_step["failed_publishes"] = [redact_mapping(item) for item in failed_publishes]
        steps.append(ingest_step)

        # #2249: on a fresh deployment the catalog step deferred the
        # view_definition nodes because no generation was published yet.
        # Project them against the generation just published and publish once
        # more, so the first provision converges and an unchanged re-provision
        # adds nothing. A deployment that already had a generation applied
        # them in the catalog step and publishes nothing extra.
        if (views_deferred and not any_failed and not packaged_stage_only
                and last_publish.get("status") == "published"):
            views_step: dict[str, Any] = {
                "step": "views_registry",
                **_apply_deferred_views_registry(
                    deployment, dsn=dsn,
                    generation_id=last_publish.get("generation_id"),
                ),
            }
            if views_step["upserted"] or views_step["retracted"]:
                views_publish = _publish(ingest_options, "views_registry")
                if views_publish.get("status") == "fail":
                    raise _final_publish_failure(
                        deployment, views_publish, steps=steps,
                    )
                views_step["publish_status"] = views_publish.get("status")
                views_step["publish_generation_id"] = views_publish.get(
                    "generation_id"
                )
                views_step["nodes_added"] = views_publish.get("nodes_added")
                if views_publish.get("status") != "published":
                    # Name why, like every other failed publish here
                    # (req.provision.surface-reasons).
                    from okg.substrate.sources.redaction import redact_mapping

                    any_failed = True
                    views_step["failed_publish"] = redact_mapping(views_publish)
                    reason = (
                        views_publish.get("blocked_reason")
                        or views_publish.get("reason")
                        or views_publish.get("error")
                        or f"views_registry_publish_{views_publish.get('status')}"
                    )
                    if not isinstance(reason, str):
                        reason = json.dumps(reason, sort_keys=True, default=str)
                    if reason not in failure_reasons:
                        failure_reasons.append(reason)
                elif views_publish.get("generation_id") is not None:
                    # The payload reports this newest generation. The publish
                    # proof below still judges the ingest publish: this one
                    # adds only view_definition nodes, never edges, so the
                    # strict row-delta gate would refuse it by design.
                    generation_id = views_publish.get("generation_id")
            steps.append(views_step)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    except Exception as exc:  # noqa: BLE001 — classify cold-start DB errors only
        # First DB touch against a DSN whose database does not exist fails
        # with a raw psycopg connect error; classify it into the one
        # createdb command that fixes it (req.provision.cold-start-hint).
        # Anything else re-raises unchanged.
        from okg.substrate.cli._db_bootstrap_errors import (
            classify_missing_database_error,
        )

        err = classify_missing_database_error(
            exc, dsn=dsn, surface="okg provision",
        )
        if err is None:
            raise
        return _emit_error(err, as_json=as_json)
    finally:
        run_guard.close()

    if packaged_stage_only:
        payload = {"ok": not any_failed and catalog_version_id is not None,
                   "state": "staged", "published": False, "generation_id": None,
                   "catalog_version_id": catalog_version_id, "steps": steps,
                   "admission_ids": sorted({str(item["admission_id"]) for item in result.report.get("results", []) if item.get("admission_id")})}
        _emit_ok(payload, as_json=as_json)
        return 0 if payload["ok"] else 1

    # An idempotent re-provision on an unchanged repo publishes nothing new (the
    # source dedups), so fall back to the latest published generation: the graph
    # is already current, which is a success, not a failure.
    if generation_id is None:
        generation_id = _latest_published_generation(
            dsn,
            deployment_name=deployment,
        )

    preflight = bootstrap_preflight(dsn, deployment_name=deployment)

    # Uniform real-publish gate (req.proof.uniform-gate): when this provision
    # produced a fresh publish, assert it terminated at the same bar as
    # `deployment bootstrap verify` — a published (not blocked) generation with
    # a positive node/edge row delta and the DLQ in bounds. An idempotent
    # re-provision on an unchanged repo publishes nothing new (or a no-op
    # publish), so it skips the row-delta assertion: the graph is already
    # current, which is a success, not a failure — but a *blocked* publish or a
    # dead-letter queue still fails.
    replay_context = None
    if packaged_replay_predecessor is not None:
        replay_context = {
            "ordered": result.report.get("ordered"),
            "results": result.report.get("results"),
            "predecessor": packaged_replay_predecessor,
        }
    publish_proof = _provision_publish_proof(
        dsn, last_publish, deployment_name=deployment,
        source_failure_reasons=failure_reasons,
        # Both passed only for a packaged install, so ordinary provision keeps
        # the unchanged call shape.
        **({"records_only": packaged_records_only}
           if packaged_records_only is not None else {}),
        **({"replay_context": replay_context} if replay_context is not None else {}),
    )

    # Fail closed on catalog identity (req.proof.fail-closed-output): a provision
    # that could not stamp a catalog_version cannot have applied a real catalog.
    ok = (
        preflight["ok"]
        and not any_failed
        and generation_id is not None
        and catalog_version_id is not None
        and publish_proof["ok"]
    )
    payload: dict[str, Any] = {
        "ok": ok,
        "deployment": deployment,
        "database": preflight.get("database"),
        "generation_id": generation_id,
        "catalog_version_id": catalog_version_id,
        "publish_proof": publish_proof,
        "steps": steps,
        "preflight": {"ok": preflight["ok"], "summary": preflight["summary"]},
    }
    # Surface the per-source failures at the top of the summary too (human
    # text renders nested lists; --json consumers get structured fields), so
    # a failed provision names every failing source and the distinct reason
    # codes without digging through steps (req.provision.surface-reasons).
    if failed_sources:
        payload["failed_sources"] = failed_sources
        payload["failure_reasons"] = failure_reasons
    # Decision 23a: a committed publish that held the lock past the capacity
    # bar succeeds with a warning and its own exit code, never exit 1.
    from okg.substrate.cli.run_summary import EXIT_CAPACITY_WARNING
    from okg.substrate.publish_proof import surface_capacity_warning

    capacity_warning = (
        surface_capacity_warning(
            publish_proof,
            surface=f"okg provision, exit {EXIT_CAPACITY_WARNING}",
        )
        if ok
        else None
    )
    if capacity_warning:
        payload["status"] = "capacity_warning"
        payload["exit_code"] = EXIT_CAPACITY_WARNING
        payload["capacity_warning"] = capacity_warning
        _emit_ok(payload, as_json=as_json)
        return EXIT_CAPACITY_WARNING
    _emit_ok(payload, as_json=as_json)
    return 0 if ok else 1


def _published_generation_row_counts(
    dsn: str,
    *,
    deployment_name: str,
) -> tuple[str | int | None, int | None, int | None]:
    """Observed (generation_id, node_count, edge_count) for the latest publish.

    Reads the latest *published* generation and counts the live node/edge rows
    visible at that generation's Chronos checkout — the same count the bootstrap
    proof asserts on (``deployment_bootstrap.workflow._run_real_publish_proof``).
    Returns ``(None, None, None)`` when no published generation exists; returns
    ``(gen, None, None)`` when the generation exists but its rows could not be
    counted (so the caller fails closed rather than treating an unreadable count
    as zero or as success).
    """
    generation_id = _latest_published_generation(
        dsn,
        deployment_name=deployment_name,
    )
    if generation_id is None:
        return (None, None, None)
    return _generation_row_counts(
        dsn,
        generation_id=generation_id,
        deployment_name=deployment_name,
    )


def _generation_row_counts(
    dsn: str,
    *,
    generation_id: str | int,
    deployment_name: str,
) -> tuple[str | int, int | None, int | None]:
    """Count live rows at one exact, published deployment generation.

    The exact-id lookup is required for predecessor evidence: resolving
    ``latest`` after a publish would race a concurrent publisher and could
    validate a one-sided delta against the wrong generation.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        checkpoint = store.resolve_generation(
            generation_id,
            deployment_name=deployment_name,
            status="published",
        )
        if (
            checkpoint is None
            or str(checkpoint.checkpoint_id) != str(generation_id)
        ):
            return (generation_id, None, None)
        graph = store.checkout_generation(checkpoint.checkpoint_id)
        node_count = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        edge_count = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
        )
    except Exception:  # noqa: BLE001 — unreadable counts fail closed below.
        return (generation_id, None, None)
    finally:
        store.close()
    return (generation_id, node_count, edge_count)


def _source_admission_summaries(results: Any) -> list[dict[str, Any]]:
    """Copy each source's terminal admission verdict as names, enums and ids.

    Never the verdict mapping itself: ``admission_verdict`` can carry admission
    samples and policy error text, and ``sync_scope`` / ``source_stats`` carry
    paths, none of which belong in a success receipt written without redaction.
    """
    def scalar(value: Any) -> str | bool | int | None:
        if isinstance(value, (str, bool)) or (isinstance(value, int) and not isinstance(value, bool)):
            return value
        return None

    summaries: list[dict[str, Any]] = []
    if not isinstance(results, list):
        return summaries
    for row in results:
        if not isinstance(row, Mapping):
            continue
        verdict = row.get("admission_verdict")
        policy = verdict.get("producer_policy") if isinstance(verdict, Mapping) else None
        policy = policy if isinstance(policy, Mapping) else {}
        summaries.append({
            "name": scalar(row.get("name")),
            "status": scalar(row.get("status")),
            "run_mode": scalar(row.get("run_mode")),
            "admission_id": scalar(row.get("admission_id")),
            "admission_status": scalar(row.get("admission_status")),
            "nodes_appended": scalar(row.get("nodes_appended")),
            "edges_appended": scalar(row.get("edges_appended")),
            "nodes_retracted": scalar(row.get("retract_n")),
            "edges_retracted": scalar(row.get("retract_e")),
            "terminal_state_authorized": scalar(policy.get("terminal_state_authorized")),
            "zero_output_reason": scalar(policy.get("reason")),
            "completed_scope": scalar(policy.get("completed_scope")),
        })
    return summaries


def _packaged_replay_proof(
    dsn: str,
    last_publish: dict[str, Any],
    strict_proof: dict[str, Any],
    replay_context: Mapping[str, Any],
    *,
    deployment_name: str,
    records_only: bool = False,
) -> dict[str, Any]:
    """Judge a zero-addition packaged repeat from evidence, never from counters.

    Entered only by a packaged installer repeat whose strict proof failed on a
    published 0/0 addition delta. Passes only when every condition holds:

    * the new generation's publisher-recorded predecessor is exactly the
      populated generation the installer observed before this refresh ran;
    * that predecessor still resolves with positive live node and edge counts;
    * the new generation's live counts equal the predecessor's (an unchanged
      graph, not merely an unchanged addition delta);
    * every requested source reached an ACCEPTED, authorized zero-output
      terminal state (the shared ``_is_caught_up_authoritative_replay``
      contract: status, pending, rejected, missing or malformed rows refuse);
    * the shared gate's blocked-status, dead-letter, runtime-queue and lock
      headroom checks pass.

    Every refusal keeps the strict verdict and names its own reason. Only
    names, ids and counts are copied out of the ingest rows.
    """
    from okg.substrate.publish_proof import (
        _caught_up_authorized_sources,
        _is_caught_up_authoritative_replay,
        assert_publish_proof,
    )

    def refuse(reason: str, **extra: Any) -> dict[str, Any]:
        verdict = dict(strict_proof)
        # Publisher-side mutation counters explain a refused zero-addition
        # republish (identical facts filtered, restamps, retractions).
        for key in ("nodes_updated", "nodes_retracted", "edges_updated", "edges_retracted"):
            verdict[key] = last_publish.get(key)
        verdict.update(extra)
        verdict["ok"] = False
        verdict["caught_up_replay"] = False
        verdict["reason"] = reason
        verdict["fix_hint"] = (
            "a repeat install may publish zero additions only over the same "
            "populated generation it observed, with every requested source "
            "authorized as caught up; inspect steps[].source_admissions"
        )
        return verdict

    observed = replay_context.get("predecessor")
    observed_id = observed.get("generation_id") if isinstance(observed, Mapping) else None
    predecessor_id = last_publish.get("predecessor_generation_id")
    if not isinstance(observed_id, str) or not observed_id:
        return refuse("no populated generation was observed before this repeat refresh")
    if predecessor_id is None or str(predecessor_id) != observed_id:
        return refuse(
            f"published predecessor {predecessor_id!r} is not the generation "
            f"observed before refresh {observed_id!r}",
            predecessor_generation_id=predecessor_id,
            observed_predecessor_generation_id=observed_id,
        )
    resolved_id, predecessor_nodes, predecessor_edges = _generation_row_counts(
        dsn, generation_id=observed_id, deployment_name=deployment_name,
    )
    populated = (
        str(resolved_id) == observed_id
        and isinstance(predecessor_nodes, int) and predecessor_nodes > 0
        and isinstance(predecessor_edges, int)
        # Issue #1914: a records-only deployment's populated generation holds
        # zero edge rows. Its node rows still have to be there.
        and (predecessor_edges > 0 or records_only)
    )
    if not populated:
        return refuse(
            f"observed predecessor {observed_id} is not a populated published "
            f"generation (node_count={predecessor_nodes}, edge_count={predecessor_edges})",
            predecessor_generation_id=observed_id,
            predecessor_nodes_live=predecessor_nodes,
            predecessor_edges_live=predecessor_edges,
        )
    node_count = last_publish.get("node_count")
    edge_count = last_publish.get("edge_count")

    def exact_int(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool)

    if not (exact_int(node_count) and exact_int(edge_count)):
        return refuse(
            "published payload did not report exact live node and edge counts",
            predecessor_generation_id=observed_id,
            predecessor_nodes_live=predecessor_nodes,
            predecessor_edges_live=predecessor_edges,
        )
    if node_count != predecessor_nodes or edge_count != predecessor_edges:
        return refuse(
            "repeat publish changed live counts "
            f"(node_count {predecessor_nodes}->{node_count}, "
            f"edge_count {predecessor_edges}->{edge_count})",
            predecessor_generation_id=observed_id,
            predecessor_nodes_live=predecessor_nodes,
            predecessor_edges_live=predecessor_edges,
            node_count=node_count,
            edge_count=edge_count,
        )
    ordered = replay_context.get("ordered")
    requested = [item for item in ordered if isinstance(item, str) and item] if isinstance(ordered, list) else []
    payload = {
        **last_publish,
        "ordered": ordered,
        "results": replay_context.get("results"),
    }
    if not _is_caught_up_authoritative_replay(payload):
        proven = _caught_up_authorized_sources(payload) or set()
        unproven = sorted(set(requested) - proven) if requested else []
        return refuse(
            "not every requested source reached an accepted authorized "
            "zero-output terminal state"
            + (f" (unproven: {', '.join(unproven)})" if unproven else " (no well-formed requested source list)"),
            predecessor_generation_id=observed_id,
            predecessor_nodes_live=predecessor_nodes,
            predecessor_edges_live=predecessor_edges,
            replay_sources_requested=requested,
            replay_sources_unproven=unproven,
        )
    proof = assert_publish_proof(
        payload,
        dsn=dsn,
        require_row_delta=True,
        allow_updated_rows=True,
        allow_caught_up_replay=True,
    )
    proof.update({
        "predecessor_generation_id": observed_id,
        "predecessor_nodes_live": predecessor_nodes,
        "predecessor_edges_live": predecessor_edges,
        "node_count": node_count,
        "edge_count": edge_count,
        "nodes_updated": last_publish.get("nodes_updated"),
        "nodes_retracted": last_publish.get("nodes_retracted"),
        "edges_updated": last_publish.get("edges_updated"),
        "edges_retracted": last_publish.get("edges_retracted"),
        "replay_sources": sorted(requested),
    })
    return proof


def _provision_publish_proof(
    dsn: str,
    last_publish: dict[str, Any],
    *,
    deployment_name: str,
    source_failure_reasons: list[str] | None = None,
    replay_context: Mapping[str, Any] | None = None,
    records_only: bool | None = None,
    expected_generation_id: str | None = None,
) -> dict[str, Any]:
    """Judge provision's final publish, and say which rule judged it.

    ``expected_generation_id`` pins the no-payload proof to one version (the
    one a caller's own run reported) instead of the deployment's latest
    published version (#2422).

    ``records_only`` is the packaged installer's registry declaration, or
    ``None`` when the caller read no registry. Only a caller that actually read
    one gets ``records_only`` / ``declares_relationships`` in the verdict, so an
    ordinary provision receipt never carries an unread claim (issue #1914).
    """
    declared = bool(records_only)
    verdict = _publish_gate_verdict(
        dsn, last_publish, deployment_name=deployment_name,
        source_failure_reasons=source_failure_reasons,
        replay_context=replay_context, records_only=declared,
        expected_generation_id=expected_generation_id,
    )
    if records_only is not None:
        verdict["records_only"] = declared
        verdict["declares_relationships"] = not declared
    nodes_added = verdict.get("nodes_added")
    if (verdict.get("ok") is not True
            and records_only is not None
            and not declared
            and verdict.get("edges_added") == 0
            and isinstance(nodes_added, int)
            and not isinstance(nodes_added, bool)
            and nodes_added > 0):
        # The publish produced records but no relationships, and the registry
        # asked for relationships. Name both ways out rather than the generic
        # catalog hint, which points at the wrong thing here.
        verdict["fix_hint"] = (
            "this deployment's source registry declares relationship output; "
            "remove the edge declaration from the source's admission policy, "
            "or publish the relationship it declares"
        )
    return verdict


def _unchanged_redeploy_proof(
    dsn: str,
    last_publish: Mapping[str, Any],
    strict_proof: dict[str, Any],
    *,
    deployment_name: str,
    records_only: bool,
) -> dict[str, Any]:
    """Accept a published version that added no rows, when it was observed.

    Re-provisioning a deployment that already has data (to refresh its
    access policy, after a source was retired) can publish a new version
    that adds no rows (#2490). That is success, reported as
    ``no_rows_added``, only when both are read back from the database:

    * the recorded predecessor is this deployment's and holds rows, and
    * the new version itself is this deployment's and holds rows.

    A first install (no predecessor), an empty predecessor, or an empty new
    version stays refused with the strict gate's reason.
    """
    from okg.substrate.publish_proof import assert_publish_proof

    generation_id = last_publish.get("generation_id")
    predecessor_generation_id = last_publish.get("predecessor_generation_id")
    if generation_id is None or predecessor_generation_id is None:
        return strict_proof

    def populated(expected_id: Any) -> tuple[bool, Any, Any]:
        observed_id, nodes, edges = _generation_row_counts(
            dsn, generation_id=expected_id, deployment_name=deployment_name,
        )
        ok = (
            str(observed_id) == str(expected_id)
            and nodes is not None and nodes > 0
            and edges is not None and (edges > 0 or records_only)
        )
        return ok, nodes, edges

    predecessor_ok, predecessor_nodes, predecessor_edges = populated(
        predecessor_generation_id,
    )
    current_ok, current_nodes, current_edges = populated(generation_id)
    observed = {
        "predecessor_generation_id": predecessor_generation_id,
        "predecessor_nodes_live": predecessor_nodes,
        "predecessor_edges_live": predecessor_edges,
        "nodes_live": current_nodes,
        "edges_live": current_edges,
    }
    if not (predecessor_ok and current_ok):
        return {**strict_proof, **observed}
    # Every other check of the uniform gate (published, not blocked, DLQ)
    # still applies; only the row-delta requirement is replaced by the
    # observed row counts above.
    proof = assert_publish_proof(
        last_publish, dsn=dsn, require_row_delta=False,
    )
    proof.update(observed)
    if proof.get("ok"):
        proof["no_rows_added"] = True
        proof["reason"] = (
            f"published graph version {generation_id} added no rows; it holds "
            f"{current_nodes} records and {current_edges} relationships "
            f"(the previous version held {predecessor_nodes} and "
            f"{predecessor_edges})"
        )
    return proof


def _publish_gate_verdict(
    dsn: str,
    last_publish: dict[str, Any],
    *,
    deployment_name: str,
    source_failure_reasons: list[str] | None = None,
    replay_context: Mapping[str, Any] | None = None,
    records_only: bool = False,
    expected_generation_id: str | None = None,
) -> dict[str, Any]:
    """Apply the uniform real-publish gate to provision's final publish.

    A fresh publish must be published-not-blocked with a positive row delta and
    a bounded DLQ. The row-delta assertion is exempted ONLY for an *explicit*
    no-op signal — the publisher's dedup/reuse path emits ``noop: True`` (see
    ``publisher._chronos_noop_reuse_result``) — or for a re-provision that
    published nothing new because its source dedups an unchanged repo. The mere
    *absence* of a row delta on a freshly published payload is NOT an exemption:
    a genuinely empty publish (``status == published`` with 0 nodes / 0 edges
    and no ``noop`` flag) MUST fail the gate, not be waved through.

    For the no-publish case (idempotent re-provision fell back to the latest
    published generation), the proof verifies that the already-current
    generation actually CONTAINS rows by querying its observed status and live
    node/edge counts — the literal ``{"status": "published"}`` payload was a
    synthesized claim, not an observation, so an empty already-current
    generation could have passed unobserved.
    """
    from okg.substrate.publish_proof import assert_publish_proof

    if not last_publish:
        # No publish this run (idempotent re-provision fell back to the latest
        # published generation). Build the proof payload from OBSERVED database
        # state — the published generation id and its live row counts — rather
        # than asserting against a synthesized literal status.
        #
        # Whichever refusal shape fires below, it must state WHY the run was
        # empty when per-source reasons exist (req.provision.surface-reasons):
        # an all-sources-denied run otherwise reads as an inexplicable empty
        # publish.
        reasons = [str(r) for r in (source_failure_reasons or []) if r]
        reason_suffix = (
            "; every publish was skipped because sources failed — "
            "distinct failure reasons: " + "; ".join(reasons)
            if reasons
            else ""
        )

        def _refusal(payload: dict[str, Any]) -> dict[str, Any]:
            if reasons:
                payload["reason"] += reason_suffix
                payload["source_failure_reasons"] = reasons
                payload["fix_hint"] = (
                    "resolve the per-source failures listed in "
                    "failed_sources / failure_reasons, then re-run provision"
                )
            return payload

        if expected_generation_id is None:
            generation_id, node_count, edge_count = _published_generation_row_counts(
                dsn,
                deployment_name=deployment_name,
            )
        else:
            observed_id, node_count, edge_count = _generation_row_counts(
                dsn,
                generation_id=expected_generation_id,
                deployment_name=deployment_name,
            )
            # The lookup echoes the requested id and returns no counts when
            # that id is not a published version of this deployment.
            if (
                str(observed_id) != str(expected_generation_id)
                or (node_count is None and edge_count is None)
            ):
                return _refusal({
                    "ok": False,
                    "reason": (
                        f"graph version {expected_generation_id} is not a "
                        f"published version of deployment {deployment_name!r}"
                    ),
                    "status": None,
                    "generation_id": expected_generation_id,
                    "fix_hint": (
                        "run `okg generation describe "
                        f"{expected_generation_id} --deployment "
                        f"{deployment_name} --json` to see its status"
                    ),
                })
            generation_id = expected_generation_id
        if generation_id is None:
            return _refusal({
                "ok": False,
                "reason": (
                    "provision published nothing this run and no published "
                    "generation exists in the database"
                ),
                "status": None,
                "generation_id": None,
                "fix_hint": (
                    "rerun provision with a source that emits facts, or check "
                    "why the publish produced no generation"
                ),
            })
        if node_count is None or edge_count is None:
            return _refusal({
                "ok": False,
                "reason": (
                    "could not read live row counts for the already-current "
                    f"published generation {generation_id}"
                ),
                "status": "published",
                "generation_id": generation_id,
                "fix_hint": (
                    "the published generation could not be checked out to count "
                    "live nodes/edges; inspect `okg generation describe`"
                ),
            })
        if node_count <= 0 or (edge_count <= 0 and not records_only):
            return _refusal({
                "ok": False,
                "reason": (
                    "the already-current published generation "
                    f"{generation_id} contains no live node/edge rows "
                    f"(node_count={node_count}, edge_count={edge_count})"
                ),
                "status": "published",
                "generation_id": generation_id,
                "nodes_added": node_count,
                "edges_added": edge_count,
                "fix_hint": (
                    "a re-provision that publishes nothing is only valid when "
                    "the current generation already holds rows; this one is "
                    "empty, so the graph is not actually populated"
                ),
            })
        # Observed: a published generation with rows. Assert the proof against
        # the observed payload (counts as the row delta) so the gate still
        # checks the DLQ and the verdict is self-sufficient.
        return assert_publish_proof(
            {
                "status": "published",
                "generation_id": generation_id,
                "nodes_added": node_count,
                "edges_added": edge_count,
            },
            dsn=dsn,
            require_row_delta=True,
            # No publish ran on this path: the already-current generation was
            # observed, so there is no lock hold to demonstrate headroom with
            # (issue #1291). Stated explicitly, never inferred from absence.
            lock_headroom_not_applicable=(
                "the already-current published generation was observed "
                "without republishing"
            ),
        )
    # Exempt the row-delta assertion ONLY on the publisher's explicit no-op
    # signal — never on the mere absence of a delta on a fresh published payload.
    # A populated predecessor is a separate, observed re-provision contract:
    # the new generation may legitimately refresh only nodes or only edges, but
    # it must still have a positive total delta. First apply the strict gate;
    # only its one-sided-delta failure is eligible for the narrow relaxation.
    is_noop = last_publish.get("noop") is True
    strict_proof = assert_publish_proof(
        last_publish,
        dsn=dsn,
        require_row_delta=not is_noop,
        # A records-only deployment can never add an edge row, so the strict
        # both-sides rule would refuse every one of its publishes. A zero-total
        # delta still fails below.
        allow_partial_row_delta=records_only,
    )
    if strict_proof["ok"] or is_noop:
        return strict_proof

    # Reuse the shared gate's already-validated counters. Calling int() on the
    # raw receipt would truncate fractions and coerce bools, making malformed
    # evidence eligible for the one-sided relaxation.
    nodes_added = strict_proof.get("nodes_added")
    edges_added = strict_proof.get("edges_added")
    if not (
        isinstance(nodes_added, int)
        and not isinstance(nodes_added, bool)
        and isinstance(edges_added, int)
        and not isinstance(edges_added, bool)
    ):
        return strict_proof
    if nodes_added == 0 and edges_added == 0:
        # A published 0/0 addition delta stays refused for a first install and
        # for ordinary provision. Only the packaged installer's repeat supplies
        # the observed predecessor and the per-source terminal verdicts that
        # can prove the graph was already current.
        if replay_context is None:
            return _unchanged_redeploy_proof(
                dsn, last_publish, strict_proof,
                deployment_name=deployment_name,
                records_only=records_only,
            )
        return _packaged_replay_proof(
            dsn, last_publish, strict_proof, replay_context,
            deployment_name=deployment_name,
            records_only=records_only,
        )
    if not (
        (nodes_added == 0 and edges_added > 0)
        or (edges_added == 0 and nodes_added > 0)
    ):
        return strict_proof

    predecessor_generation_id = last_publish.get("predecessor_generation_id")
    if predecessor_generation_id is None:
        return strict_proof
    (
        observed_predecessor_id,
        predecessor_nodes,
        predecessor_edges,
    ) = _generation_row_counts(
        dsn,
        generation_id=predecessor_generation_id,
        deployment_name=deployment_name,
    )
    has_populated_predecessor = (
        str(observed_predecessor_id) == str(predecessor_generation_id)
        and predecessor_nodes is not None
        and predecessor_nodes > 0
        and predecessor_edges is not None
        and (predecessor_edges > 0 or records_only)
    )
    if not has_populated_predecessor:
        strict_proof["predecessor_generation_id"] = predecessor_generation_id
        strict_proof["predecessor_nodes_live"] = predecessor_nodes
        strict_proof["predecessor_edges_live"] = predecessor_edges
        return strict_proof

    proof = assert_publish_proof(
        last_publish,
        dsn=dsn,
        require_row_delta=not is_noop,
        allow_partial_row_delta=True,
    )
    proof["predecessor_generation_id"] = predecessor_generation_id
    proof["predecessor_nodes_live"] = predecessor_nodes
    proof["predecessor_edges_live"] = predecessor_edges
    return proof
