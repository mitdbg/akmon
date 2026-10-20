"""`okg catalog` subcommand handlers.

Extracted from ``cli/main.py`` by ``extract-cli-catalog``. The
catalog handlers (``load``, ``history``, ``ownership``, plus the
dispatcher) used to live inline in ``main.py``; moving them here
shrinks ``main.py`` and makes catalog evolution reviewable in
isolation. (The legacy ``reconcile`` handler — a live-vs-superseded
orphan scan over the bitemporal tables — was removed by the chronos
cutover.)

The handlers import their shared helpers (``_resolve_dsn``,
``_resolve_deployment_dsn_from_dir``, ``_emit_error``, ``_emit_ok``)
from ``cli.main`` at call time to avoid a top-level circular import.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _catalog_advisory_keys(deployment_name: str) -> tuple[int, int]:
    digest = hashlib.sha256(
        f"okg:catalog_apply:{deployment_name}".encode("utf-8")
    ).digest()
    return 0x4F4B4743, int.from_bytes(digest[:4], "big") % 2_147_483_647


def _count_by_origin(narrowings: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in narrowings:
        out[row.origin] = out.get(row.origin, 0) + 1
    return out


def _catalog_claim_payload(
    report: dict[str, Any],
    *,
    deployment: str,
    writer_scope: str,
    environment_id: str | None,
) -> dict[str, Any]:
    """Build a claim receipt without inventing a readiness verdict."""
    from okg.substrate.errors import SubstrateError

    latest_compatible = report.get("latest_compatible")
    if not isinstance(latest_compatible, bool):
        raise SubstrateError(
            "catalog ownership claim returned an invalid compatibility verdict",
            data={
                "latest_compatible": latest_compatible,
                "expected_type": "boolean",
            },
            fix_hint=(
                "run `okg catalog ownership diagnose --json`; repair the "
                "diagnostic before relying on the ownership claim result"
            ),
        )
    next_command = None
    if not latest_compatible:
        authority_args = f" --writer-scope {shlex.quote(writer_scope)}"
        if environment_id:
            authority_args += (
                " --environment-id " + shlex.quote(environment_id)
            )
        next_command = (
            "okg catalog load --deployment "
            f"{shlex.quote(deployment)}{authority_args} --apply --json"
        )
    # Keep the claim receipt ahead of diagnostic history in text mode. A zero
    # exit code means ownership transferred; it does not imply that an old
    # catalog was recomposed (#1170).
    return {
        "claimed": True,
        "latest_compatible": latest_compatible,
        "resolved": latest_compatible,
        "next_command": next_command,
        **{
            key: value for key, value in report.items()
            if key != "latest_compatible"
        },
    }


@dataclass(frozen=True)
class CatalogTarget:
    """Resolved catalog target plus its explicit writer authority."""

    composed: Any
    deployment_dir: Path
    dsn: str | None
    writer_scope: str = "deployment_root"
    environment_id: str | None = None


def _catalog_target_error(
    message: str,
    *,
    reason_code: str,
    data: dict[str, Any] | None = None,
    fix_hint: str | None = None,
) -> Exception:
    """Return a CLI error whose public and nested classifications agree."""
    from okg.substrate.errors import SubstrateError

    err = SubstrateError(
        message,
        data={"reason_code": reason_code, **(data or {})},
        fix_hint=fix_hint,
    )
    err.code = reason_code
    return err


def _resolve_catalog_target(args: argparse.Namespace) -> CatalogTarget:
    """Resolve a target without consulting ambient writer-scope state."""
    import os

    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.deployments import (
        DeploymentConfigError,
        DeploymentResolutionError,
        require_postgres_dsn,
        resolve_deployment,
    )
    deployment_dir = Path(args.deployment).expanduser().resolve()
    requested_scope = getattr(args, "writer_scope", None)
    requested_environment = (
        getattr(args, "environment_id", None) or ""
    ).strip()
    if requested_environment and requested_scope != "environment":
        raise _catalog_target_error(
            "an environment id requires environment writer scope",
            reason_code="catalog_writer_scope_ambiguous",
            fix_hint=(
                "select --writer-scope=environment or remove --environment-id"
            ),
        )
    if deployment_dir.is_dir():
        if requested_scope == "environment" and not requested_environment:
            raise _catalog_target_error(
                "environment writer scope requires an explicit environment id",
                reason_code="catalog_environment_identity_required",
                fix_hint="pass --environment-id with --writer-scope=environment",
            )
        return CatalogTarget(
            composed=compose_catalog(deployment_dir),
            deployment_dir=deployment_dir,
            dsn=None,
            writer_scope=requested_scope or "deployment_root",
            environment_id=requested_environment or None,
        )
    if requested_scope != "environment":
        try:
            manifest = resolve_deployment(args.deployment)
        except DeploymentConfigError as exc:
            raise _catalog_target_error(
                f"deployment configuration is invalid: {exc}",
                reason_code="deployment_config_invalid",
                data={
                    "deployment_ref": str(args.deployment),
                    "resolution_error_type": type(exc).__name__,
                },
                fix_hint="repair the deployment manifest before catalog access",
            ) from exc
        except DeploymentResolutionError as exc:
            raise _catalog_target_error(
                f"deployment not found: {args.deployment!r}",
                reason_code="deployment_not_found",
                data={
                    "deployment_ref": str(args.deployment),
                    "resolution_error_type": type(exc).__name__,
                },
                fix_hint="pass a deployment directory or deployment name",
            ) from exc
        try:
            dsn = getattr(args, "dsn", None) or require_postgres_dsn(
                manifest, deployment=args.deployment,
            )
        except DeploymentResolutionError as exc:
            raise _catalog_target_error(
                f"deployment database configuration is invalid: {exc}",
                reason_code="deployment_config_invalid",
                data={
                    "deployment_ref": str(args.deployment),
                    "resolution_error_type": type(exc).__name__,
                },
                fix_hint=(
                    "set the deployment's PostgreSQL DSN or pass --dsn"
                ),
            ) from exc
        return CatalogTarget(
            composed=compose_catalog(manifest),
            deployment_dir=manifest.deployment_dir,
            dsn=dsn,
            writer_scope="deployment_root",
        )
    from okg.substrate.cli.deployment_identifier import IDENTIFIER_KIND_MISMATCH
    from okg.substrate.deployments.environment import (
        default_environments_dir,
        load_environment,
        load_environment_fleet,
    )

    descriptor = os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR")
    environment_target = descriptor or args.deployment
    try:
        environment = load_environment(environment_target, environ=os.environ)
    except FileNotFoundError:
        environment = None
    except OSError as exc:
        raise _catalog_target_error(
            f"environment configuration cannot be read: {exc}",
            reason_code="catalog_environment_invalid",
            data={
                "deployment_ref": str(args.deployment),
                "resolution_error_type": type(exc).__name__,
            },
            fix_hint="repair access to the environment descriptor",
        ) from exc
    except DeploymentConfigError as exc:
        raise _catalog_target_error(
            f"environment configuration is invalid: {exc}",
            reason_code="catalog_environment_invalid",
            data={
                "deployment_ref": str(args.deployment),
                "resolution_error_type": type(exc).__name__,
            },
            fix_hint="repair the environment descriptor before catalog access",
        ) from exc

    graph_matches: list[Any] = []
    if environment is None and descriptor is None:
        try:
            graph_matches = [
                candidate
                for candidate in load_environment_fleet(
                    default_environments_dir(), environ=os.environ,
                )
                if candidate.graph_deployment_name == args.deployment
            ]
        except FileNotFoundError:
            graph_matches = []
        except OSError as exc:
            raise _catalog_target_error(
                f"environment fleet cannot be read: {exc}",
                reason_code="catalog_environment_invalid",
                data={
                    "deployment_ref": str(args.deployment),
                    "resolution_error_type": type(exc).__name__,
                },
                fix_hint="repair access to the environment descriptors",
            ) from exc
        except DeploymentConfigError as exc:
            raise _catalog_target_error(
                f"environment fleet configuration is invalid: {exc}",
                reason_code="catalog_environment_invalid",
                data={
                    "deployment_ref": str(args.deployment),
                    "resolution_error_type": type(exc).__name__,
                },
                fix_hint="repair the environment descriptors before catalog access",
            ) from exc
    if len(graph_matches) > 1:
        raise _catalog_target_error(
            "catalog environment target is ambiguous",
            reason_code="catalog_environment_identity_ambiguous",
            data={
                "graph_deployment_name": str(args.deployment),
                "environment_ids": sorted(item.id for item in graph_matches),
            },
            fix_hint="pass one environment id to --deployment",
        )
    if len(graph_matches) == 1:
        environment = graph_matches[0]
    if environment is not None and args.deployment == environment.id:
        if requested_environment and requested_environment != environment.id:
            raise _catalog_target_error(
                "catalog environment identity is ambiguous",
                reason_code="catalog_environment_identity_ambiguous",
                data={
                    "target_environment_id": environment.id,
                    "asserted_environment_id": requested_environment,
                },
            )
        try:
            dsn = getattr(args, "dsn", None) or require_postgres_dsn(
                environment.manifest, deployment=environment.id,
            )
        except DeploymentResolutionError as exc:
            raise _catalog_target_error(
                f"environment database configuration is invalid: {exc}",
                reason_code="catalog_environment_invalid",
                data={
                    "deployment_ref": str(args.deployment),
                    "environment_id": environment.id,
                    "resolution_error_type": type(exc).__name__,
                },
                fix_hint=(
                    "set the environment's PostgreSQL DSN or pass --dsn"
                ),
            ) from exc
        return CatalogTarget(
            composed=compose_catalog(environment.manifest),
            deployment_dir=environment.manifest.deployment_dir,
            dsn=dsn,
            writer_scope="environment",
            environment_id=environment.id,
        )
    if (
        environment is not None
        and args.deployment == environment.graph_deployment_name
        and environment.id != environment.graph_deployment_name
    ):
        raise _catalog_target_error(
            "catalog environment resolution requires an environment id, "
            "not a graph deployment name",
            reason_code=IDENTIFIER_KIND_MISMATCH,
            data={
                "environment_id": environment.id,
                "graph_deployment_name": environment.graph_deployment_name,
                "supplied_identifier_kind": "graph_deployment_name",
                "expected_identifier_kind": "environment_id",
            },
            fix_hint=(
                "pass the environment id to --deployment, or omit "
                "--writer-scope=environment for a deployment-root writer"
            ),
        )
    raise _catalog_target_error(
        f"environment not found: {args.deployment!r}",
        reason_code="deployment_not_found",
        data={
            "deployment_ref": str(args.deployment),
            "resolution_error_type": "DeploymentResolutionError",
        },
        fix_hint="pass an environment id to --deployment",
    )


def _resolve_catalog_input(
    args: argparse.Namespace,
) -> tuple[Any, Path, str | None, str, str | None]:
    """Back-compat tuple view of :func:`_resolve_catalog_target`."""
    target = _resolve_catalog_target(args)
    return (
        target.composed,
        target.deployment_dir,
        target.dsn,
        target.writer_scope,
        target.environment_id,
    )


def _cmd_catalog(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg catalog <subcmd>` — catalog composition / load /
    reconcile / history.
    """
    from okg.substrate.cli.main import _emit_error
    from okg.substrate.errors import SubstrateError

    if args.subcmd == "load":
        return _cmd_catalog_load(args, as_json=as_json)
    if args.subcmd == "history":
        return _cmd_catalog_history(args, as_json=as_json)
    if args.subcmd == "ownership":
        return _cmd_catalog_ownership(args, as_json=as_json)
    if args.subcmd == "repair-restamp-ownership":
        return _cmd_catalog_repair_restamp_ownership(args, as_json=as_json)
    return _emit_error(
        SubstrateError(f"unknown subcommand: okg catalog {args.subcmd!r}"),
        as_json=as_json,
    )


def _cmd_catalog_repair_restamp_ownership(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg catalog repair-restamp-ownership` (issue #3318).

    Reports graph nodes an earlier catalog restamp took over under
    `_catalog:restamp`, counted by the source that wrote them, and with
    `--apply` returns them to that source. Without `--apply` its
    transaction is rolled back. Exits 1 while any such node's source cannot
    be determined; those nodes are never written, but with `--apply` the
    nodes it could resolve are. Nodes a source stopped emitting while the
    restamp owned them still need that source's reset-cursor run, which the
    output names.
    """
    import psycopg

    from okg.substrate.catalog.restamp_repair import repair_restamp_ownership
    from okg.substrate.cli.main import _emit_error, _resolve_dsn
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.errors import SubstrateError

    try:
        dsn = _resolve_dsn(args)
        with db_connect(dsn) as conn:
            report = repair_restamp_ownership(
                conn,
                deployment_name=str(args.deployment),
                branch=str(args.branch),
                apply=bool(args.apply),
            )
            if args.apply:
                conn.commit()
            else:
                conn.rollback()
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    except psycopg.Error as err:
        return _emit_error(
            SubstrateError(
                f"restamp ownership repair failed: {err}",
                fix_hint="check the deployment DSN; nothing was committed",
            ),
            as_json=as_json,
        )
    exit_code = 1 if report["unresolved"] else 0
    if as_json:
        print(json.dumps(report, indent=2, default=str))
        return exit_code
    applied = report["mode"] == "apply"
    print(
        f"{report['restamp_owned']} node(s) owned by _catalog:restamp on "
        f"branch {report['branch']}; "
        + (
            f"repaired {report['repaired']}"
            if applied
            else f"would repair {report['resolved']}"
        )
    )
    for source, count in report["resolved_by_source"].items():
        print(f"  {source}: {count}")
    if report["unresolved"]:
        print(
            f"{report['unresolved']} node(s) left alone: their source "
            "cannot be determined from the branch history"
        )
        for reason, count in report["unresolved_by_reason"].items():
            print(f"  {reason}: {count}")
        if applied and report["repaired"]:
            print(
                f"the {report['repaired']} resolved node(s) were written and "
                "committed; only the nodes above were left alone"
            )
    for source, pending in report["pending_retraction"].items():
        print(
            f"{pending['count']} node(s) {source} no longer emits stay "
            "published until it runs a reset-cursor ingest: "
            + ", ".join(pending["node_ids"])
        )
    for source, unchecked in report["pending_retraction_unchecked"].items():
        if unchecked["reason"] == "no_stored_record_set":
            print(
                f"inspect: {source} has no stored record set; a reset-cursor "
                f"run will not retract its {unchecked['count']} repaired "
                "node(s) if it no longer emits them"
            )
        else:
            print(
                f"inspect: {unchecked['count']} node(s) of {source} not "
                f"checked against its record set ({unchecked['reason']})"
            )
    if report["follow_up"]:
        print(
            "then run"
            + (" (after --apply)" if not applied else "")
            + " a reset-cursor ingest of each source alone; if a source's "
            "current read emits no records, add --allow-full-retraction "
            "<source>, which the full-retraction guard otherwise requires:"
        )
        for command in report["follow_up"]:
            print(f"  {command}")
    if not applied and report["resolved"]:
        print("read-only: nothing was written; rerun with --apply to repair")
    return exit_code


def _cmd_catalog_history(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg catalog history [--limit N]` — list recent catalog
    versions from ``okg.catalog_versions``. Added per
    ``align-substrate-cli-docs-and-runbooks`` Decision 1.
    """
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.cli.main import _emit_error, _resolve_dsn
    from okg.substrate.errors import SubstrateError

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    limit = max(1, min(int(getattr(args, "limit", 20) or 20), 500))
    with db_connect(dsn, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT catalog_version_id, status, applied_at, created_at
              FROM okg.catalog_versions
             ORDER BY catalog_version_id DESC
             LIMIT %s
            """,
            (limit,),
        )
        rows = [
            {
                "catalog_version_id": r[0],
                "status": r[1],
                "applied_at": r[2].isoformat() if r[2] else None,
                "created_at": r[3].isoformat() if r[3] else None,
            }
            for r in cur.fetchall()
        ]

    if as_json:
        print(json.dumps({"catalog_versions": rows}, indent=2, default=str))
        return 0
    if not rows:
        print("(no catalog versions found)")
        return 0
    print(f"{'cat_ver':>8}  {'status':<12}  applied_at")
    for r in rows:
        print(
            f"{r['catalog_version_id']:>8}  "
            f"{(r['status'] or '?'):<12}  "
            f"{r['applied_at'] or '(unapplied)'}"
        )
    return 0


def _cmd_catalog_load(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg catalog load --deployment <dir>` — compose the
    deployment's catalog from modules + bridges + aliases +
    identifier_patterns. Without ``--apply``, prints a summary.
    """
    from okg.substrate.cli.main import (
        _emit_error,
        _emit_ok,
        _resolve_deployment_dsn_from_dir,
    )
    from okg.substrate.errors import SubstrateError

    try:
        target = _resolve_catalog_target(args)
        composed = target.composed
        deployment_dir = target.deployment_dir
        environment_dsn = target.dsn
    except (SubstrateError, FileNotFoundError) as e:
        return _emit_error(e, as_json=as_json)

    # Identity: replayed from dev (commit 7fe9443e) — used so the
    # catalog-apply path can claim/guard ownership of a shared
    # OKG database. Defined here so both summary and apply branches
    # see the same identity object.
    from okg.substrate.catalog.ownership import catalog_identity_for
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=deployment_dir,
        command="okg catalog load --apply"
        if getattr(args, "apply", False)
        else "okg catalog load",
        writer_scope=target.writer_scope,
        environment_id=target.environment_id,
    )

    summary = {
        "deployment": composed.deployment_name,
        "modules": composed.modules,
        "subtype_count": len(composed.subtypes),
        "narrowing_count": len(composed.narrowings),
        "alias_count": len(composed.aliases),
        "identifier_pattern_count": len(composed.identifier_patterns),
        "bridge_paths": [str(p) for p in composed.bridge_paths],
        "narrowings_by_origin": _count_by_origin(composed.narrowings),
        "applied": False,
    }
    if getattr(args, "apply", False):
        import psycopg
        from okg.substrate.db.connection import connect as db_connect

        from okg.substrate.catalog.modules_apply import (
            apply_composed_catalog,
        )
        from okg.substrate.catalog.ownership import (
            ensure_catalog_write_allowed,
        )

        try:
            dsn = environment_dsn or _resolve_deployment_dsn_from_dir(
                args, deployment_dir,
            )
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        owner_meta_raw = getattr(args, "owner_meta", None) or "{}"
        try:
            owner_meta = json.loads(owner_meta_raw)
        except json.JSONDecodeError:
            return _emit_error(
                SubstrateError(
                    f"--owner-meta must be JSON; got {owner_meta_raw!r}",
                ),
                as_json=as_json,
            )
        owner_meta.setdefault("command", "okg catalog load --apply")
        deployment_name = (
            getattr(composed, "deployment_name", None)
            or Path(deployment_dir).name
        )
        try:
            with db_connect(dsn, autocommit=True) as conn:
                lock_key = _catalog_advisory_keys(deployment_name)
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_try_advisory_lock(%s, %s)", lock_key)
                    locked = bool(cur.fetchone()[0])
                if not locked:
                    err = SubstrateError(
                        f"catalog apply lock is held for {deployment_name!r}",
                        data={
                            "deployment_name": deployment_name,
                            "advisory_key": list(lock_key),
                            "owner_meta": owner_meta,
                        },
                        fix_hint=(
                            "wait for the active catalog apply to finish, "
                            "then retry"
                        ),
                    )
                    err.code = "catalog_apply_lock_held"
                    return _emit_error(err, as_json=as_json)
                try:
                    ensure_catalog_write_allowed(conn, identity=identity)
                    apply_report: dict[str, Any] = {}
                    cv = apply_composed_catalog(
                        conn,
                        composed,
                        identity=identity,
                        apply_report=apply_report,
                    )
                finally:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT pg_advisory_unlock(%s, %s)",
                            lock_key,
                        )
        except psycopg.Error as e:
            return _emit_error(
                SubstrateError(
                    f"catalog apply failed: {e}",
                    fix_hint=(
                        "the new catalog_version row remains "
                        "'pending' (or was rolled back). Inspect "
                        "okg.catalog_versions and re-run with the "
                        "underlying DB error fixed."
                    ),
                ),
                as_json=as_json,
            )
        summary["applied"] = True
        summary["catalog_version_id"] = cv
        summary.update(apply_report)

        # Per `viz-materialized-view-nodes` Slice B: after the
        # ontology catalog applies, also load the deployment's
        # views_registry.yaml (if present) and upsert
        # `view_definition` graph nodes into the Chronos graph
        # (`okg.graph_nodes`).
        from okg.substrate.views.registry import (
            apply_views_registry,
            load_views_registry,
        )
        try:
            entries = load_views_registry(deployment_dir)
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        summary["views_registry"] = {
            "entries": len(entries),
            "applied": False,
        }
        if entries:
            try:
                with db_connect(dsn, autocommit=True) as conn:
                    apply_result = apply_views_registry(
                        conn,
                        entries,
                        deployment_name=composed.deployment_name,
                        # A fresh deployment has no baseline generation yet;
                        # defer the (evidence-only) view_definition projection
                        # to the next load instead of failing. Views still
                        # materialize from the YAML at publish.
                        defer_if_unpublished=True,
                    )
                summary["views_registry"]["applied"] = not apply_result.deferred
                summary["views_registry"]["deferred"] = apply_result.deferred
                summary["views_registry"]["upserted"] = apply_result.upserted
                summary["views_registry"]["retracted"] = apply_result.retracted
                summary["views_registry"]["generation_id"] = (
                    apply_result.generation_id
                )
            except SubstrateError as e:
                return _emit_error(e, as_json=as_json)
            except psycopg.Error as e:
                return _emit_error(
                    SubstrateError(
                        f"views_registry apply failed: {e}",
                        fix_hint=(
                            "the catalog version was applied; "
                            "fix views_registry.yaml and re-run "
                            "`okg catalog load --apply`."
                        ),
                    ),
                    as_json=as_json,
                )

    _emit_ok(summary, as_json=as_json)
    return 0


def _writer_scope_kwargs(
    args: argparse.Namespace, target: CatalogTarget,
) -> dict[str, Any]:
    """Return the scope already resolved from explicit CLI authority."""
    return {
        "writer_scope": target.writer_scope,
        "environment_id": target.environment_id,
    }


def _cmd_catalog_ownership(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """Diagnose or claim writable catalog ownership for a deployment DB.

    Replayed from dev (commit 7fe9443e) by `merge-dev-into-cleanup-wave`.
    Lives here rather than `cli/main.py` because the cleanup branch
    extracted catalog handlers into this sibling module.
    """
    import psycopg
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
        diagnose_catalog_ownership,
    )
    from okg.substrate.cli.main import (
        _emit_error,
        _emit_ok,
        _resolve_deployment_dsn_from_dir,
    )
    from okg.substrate.errors import SubstrateError

    try:
        target = _resolve_catalog_target(args)
        identity = catalog_identity_for(
            composed=target.composed,
            deployment_dir=target.deployment_dir,
            command=f"okg catalog ownership {args.ownership_cmd}",
            **_writer_scope_kwargs(args, target),
        )
        dsn = target.dsn or _resolve_deployment_dsn_from_dir(
            args, target.deployment_dir,
        )
        with db_connect(dsn, autocommit=True) as conn:
            if args.ownership_cmd == "diagnose":
                payload = diagnose_catalog_ownership(
                    conn,
                    identity=identity,
                    limit=getattr(args, "limit", 12),
                )
            elif args.ownership_cmd == "claim":
                report = claim_catalog_ownership(
                    conn,
                    identity=identity,
                    force=bool(getattr(args, "force", False)),
                    allow_scope_change=bool(
                        getattr(args, "allow_scope_change", False)
                    ),
                )
                payload = _catalog_claim_payload(
                    report,
                    deployment=str(args.deployment),
                    writer_scope=target.writer_scope,
                    environment_id=target.environment_id,
                )
            else:
                raise SubstrateError(
                    f"unknown ownership subcommand: "
                    f"{args.ownership_cmd!r}",
                )
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    except psycopg.Error as e:
        return _emit_error(
            SubstrateError(
                f"catalog ownership check failed: {e}",
                fix_hint="check the deployment DSN and run okg migrate",
            ),
            as_json=as_json,
        )

    _emit_ok(payload, as_json=as_json)
    return 0


__all__ = [
    "CatalogTarget",
    "_catalog_claim_payload",
    "_cmd_catalog",
    "_cmd_catalog_history",
    "_cmd_catalog_load",
    "_cmd_catalog_ownership",
]
