"""`okg doctor` subcommand handlers.

Extracted from ``cli/main.py`` by ``extract-cli-doctor``. The
``_cmd_doctor`` dispatcher and its 18 per-check helpers used to
live inline in ``main.py``; moving them here shrinks ``main.py``
by ~3000 LOC and lets new doctor checks land without scrolling
through the entire CLI.

The handlers need ``cli.main`` helpers (``_emit_error``,
``_emit_ok``, ``_resolve_dsn``, ``_json_safe``,
``_print_db_status_brief``, ``_resolve_deployment_dsn``,
``_resolve_deployment_dsn_from_dir``) plus ``SubstrateError``.
Because ``cli/main.py`` imports ``_cmd_doctor`` from this module
at the top, a direct module-level import of those helpers would
trigger a circular-import error during ``cli/main.py`` load
(the helpers are defined *after* the doctor-import line in
``main.py``).

Solution: ``_load_main_helpers()`` is a one-time module-level
cache that imports the helpers via ``okg.substrate.cli.main``
on first call and binds them as module globals. Every
``_doctor_*`` handler starts with ``_load_main_helpers()``;
after the first invocation the names resolve to module-level
globals at zero cost. This replaced the per-function 9-line
deferred-import shim that ``extract-cli-doctor`` originally
injected.
"""
from __future__ import annotations
from okg.substrate import fastyaml

import argparse
import copy
import json
import os
import re
import stat
from pathlib import Path
from typing import Any, Optional


# Module-level helper bindings, populated by _load_main_helpers().
# Typed as Any because the real helpers are imported lazily.
_emit_error: Any = None
_emit_ok: Any = None
_resolve_deployment_dsn: Any = None
_resolve_dsn: Any = None
SubstrateError: Any = None


def _load_main_helpers() -> None:
    """Lazily import ``cli.main`` helpers + ``SubstrateError`` into
    this module's globals on first call.

    Subsequent calls are a no-op. Called from the first line of
    every ``_doctor_*`` handler so the names resolve as module-
    level globals at handler-body call sites.
    """
    global _emit_error, _emit_ok
    global _resolve_deployment_dsn
    global _resolve_dsn, SubstrateError
    if (
        _emit_error is not None
        and _emit_ok is not None
        and _resolve_deployment_dsn is not None
        and _resolve_dsn is not None
        and SubstrateError is not None
    ):
        return
    from okg.substrate.cli.main import (
        _emit_error as _e,
        _emit_ok as _o,
        _resolve_deployment_dsn as _rd,
        _resolve_dsn as _r,
    )
    from okg.substrate.errors import SubstrateError as _se
    _emit_error = _e
    _emit_ok = _o
    _resolve_deployment_dsn = _rd
    _resolve_dsn = _r
    SubstrateError = _se


def _doctor_database(args: argparse.Namespace, *, as_json: bool) -> int:
    _load_main_helpers()
    from okg.substrate.db.management import database_health_report

    try:
        deployment = getattr(args, "deployment", None)
        dsn = _resolve_deployment_dsn(args) if deployment else _resolve_dsn(args)
        payload = database_health_report(dsn, deployment_name=deployment)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    if deployment:
        _add_release_verdict_finding(payload, deployment=deployment, dsn=dsn)
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok", False) else 1


def _add_release_verdict_finding(
    payload: dict, *, deployment: str, dsn: str,
) -> None:
    """Put the release verdict into doctor's database report.

    PACT unknown-s7-release-verdict. This check reports migration drift, yet
    it had no finding for a pending additive migration, so it called healthy
    the very database `okg mcp-serve` and `okg deployment release` refuse.
    It now reads the same verdict they read, with the same reason code and
    the same refusal sentence (`release_refusal_message`).

    `refuse` is an error. `read_only` is a warning (PACT
    unknown-s8-mcp-readonly): `okg mcp-serve` serves reads from that
    database and refuses writes, while the worker and `okg deployment
    release` still refuse, so the database needs a migration but is not
    broken for readers. The sentence is the same one those surfaces print.

    A release read that fails is its own error finding,
    `release_unverifiable`, and the database health report is still printed:
    the storage and backlog findings stay useful when the release half
    cannot be read. Deployments with no declared release get no finding:
    they have no verdict, and the #2128 rule (report and continue) is theirs.
    """
    from okg.substrate.deployment_release import release_report_if_declared
    from okg.substrate.deployments import DeploymentResolutionError

    finding: dict | None
    try:
        release = release_report_if_declared(
            deployment=deployment, dsn=dsn, process_role="doctor",
        )
    except (SubstrateError, DeploymentResolutionError) as err:
        finding = {
            "severity": "error",
            "subsystem": "release",
            "code": "release_unverifiable",
            "verdict": None,
            "observed": str(err),
            "expected": "the declared release contract can be read",
            "fix_hint": getattr(err, "fix_hint", None)
            or "check the deployment's release block and database access",
        }
    else:
        if release is None:
            return
        verdict = release["verdict"]
        if verdict == "serve" and not release.get("verdict_message"):
            return
        # Indexed, not `.get` with a default: a verdict this table does not
        # know is a defect to raise, not a finding to file as "info".
        severity = {"refuse": "error", "read_only": "warning", "serve": "info"}[
            verdict
        ]
        finding = {
            "severity": severity,
            "subsystem": "release",
            "code": release.get("verdict_reason"),
            "verdict": verdict,
            "observed": release.get("refusal_message")
            or release.get("verdict_message"),
            "expected": "the database matches this code's declared release",
            "schema_behind_by": release.get("schema_behind_by"),
            "behind_migrations": release.get("behind_migrations"),
            "fix_hint": release.get("fix_hint"),
        }
        if verdict == "read_only":
            # Decision 2026-09-28 (option 1): the finding stays a warning
            # because reads still work, and says outright that ingest does
            # not until the database is migrated.
            # Under read_only the release report's hint is "run `okg up
            # <dep>`", so doctor, mcp-serve and the report name one command.
            finding["ingest"] = (
                f"ingest refuses until migrated ({release['fix_hint']})"
            )
    payload.setdefault("findings", []).append(finding)
    findings = payload["findings"]
    ok = not any(f.get("severity") == "error" for f in findings)
    payload["ok"] = ok
    payload["status"] = "OK" if ok else "ERROR"
    summary = payload.setdefault("summary", {})
    summary["finding_count"] = len(findings)
    summary["error_count"] = sum(
        1 for f in findings if f.get("severity") == "error"
    )
    summary["warning_count"] = sum(
        1 for f in findings if f.get("severity") == "warning"
    )


def _doctor_disk(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check disk [--deployment <name>] [--deep]` — disk-usage
    report with advisory growth/bloat findings (PACT
    ``bound-okg-workspace-disk-growth``, task 1). Loud-fails on an unreadable
    DSN; never reports a default/zero."""
    _load_main_helpers()
    from okg.substrate.db.management import disk_usage_report

    try:
        deployment = getattr(args, "deployment", None)
        dsn = _resolve_deployment_dsn(args) if deployment else _resolve_dsn(args)
        payload = disk_usage_report(
            dsn,
            deployment_name=deployment,
            deep=bool(getattr(args, "deep", False)),
        )
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    except Exception as exc:  # noqa: BLE001 — loud-fail (bad DSN / pgstattuple
        # privilege / read error) as a structured error, never an uncaught
        # traceback or a default/zero report.
        return _emit_error(
            SubstrateError(
                f"okg doctor --check disk failed: {exc}",
                fix_hint="ensure the DSN is reachable; --deep needs the "
                         "pgstattuple extension (superuser to install it once)",
            ),
            as_json=as_json,
        )
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok", False) else 1


def _doctor_host_headroom(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check host-headroom --deployment <name>` — host
    filesystem headroom (bytes + percent available) for the deployment
    dir's volume and the Docker data root, classified against the
    deployment's declared `host_storage:` thresholds (PACT
    ``bound-host-storage-pressure``). Undeclared thresholds are a loud
    misconfiguration, never a silent default."""
    _load_main_helpers()
    from okg.substrate.runtime.host_headroom import check_host_disk_headroom

    deployment = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg doctor --check host-headroom requires "
                "--deployment <name> or OKG_DEPLOYMENT",
            ),
            as_json=as_json,
        )
    verdict = check_host_disk_headroom(deployment=deployment)
    payload = {"check": "host-headroom", **verdict.to_dict()}
    _emit_ok(payload, as_json=as_json)
    return 0 if verdict.ok else 1


def _doctor_code_drift(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check code-drift --deployment <name>` — is the code the
    deployment is actually RUNNING merged into the base ref, and does it track
    a branch?

    A deployment's sync_root revision IS its deployed code. When that revision
    carries unmerged commits (stranded hotfixes) or sits on a detached HEAD,
    the deployment can drift arbitrarily far behind with no signal — okg-workspace
    ran 313 commits behind for six days that way. Reports `commits_behind` as a
    fact; only classifies it when the deployment declares
    `code_drift.max_commits_behind`. A deployment that runs okg from a pinned
    checkout declares `code_drift.mode: pinned` instead: the sync_root must be
    clean and at the commit the installed okg package was built from."""
    _load_main_helpers()
    from okg.substrate.runtime.deployment_drift import check_deployment_code_drift

    deployment = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg doctor --check code-drift requires "
                "--deployment <name> or OKG_DEPLOYMENT",
            ),
            as_json=as_json,
        )
    # NOTE: the base ref is the module default. `okg doctor` exposes no
    # --base-ref argument, so reading one off `args` would be dead code.
    verdict = check_deployment_code_drift(deployment=deployment)
    payload = {"check": "code-drift", **verdict.to_dict()}
    _emit_ok(payload, as_json=as_json)
    return 0 if verdict.ok else 1


def _doctor_preflight(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check preflight [--deployment <name>]` — bootstrap-
    readiness in one report: required extensions, schema, and catalog. Per
    `canonical-dev-okg`; names ALL missing prerequisites at once."""
    _load_main_helpers()
    from okg.substrate.db.management import bootstrap_preflight

    try:
        deployment = getattr(args, "deployment", None)
        dsn = _resolve_deployment_dsn(args) if deployment else _resolve_dsn(args)
        payload = bootstrap_preflight(dsn, deployment_name=deployment)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok", False) else 1


def _doctor_extraction_wrap(*, as_json: bool) -> int:
    """`okg doctor --check extraction-wrap-invariant` — lint the
    extraction library tree."""
    _load_main_helpers()
    from okg.substrate.library.extraction._lint import (
        lint_extraction_tree,
    )
    report = lint_extraction_tree()
    payload = {
        "ok": report.ok,
        "modules_checked": report.modules_checked,
        "violations": [
            {
                "module": v.module_path,
                "third_party_imports": v.third_party_imports,
                "reason": v.reason,
            }
            for v in report.violations
        ],
    }
    _emit_ok(payload, as_json=as_json)
    return 0 if report.ok else 1


def _doctor_dsn_agreement(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check dsn-agreement [--deployment X]` — verify every
    surface (authority chain, process env, local.env, MCP registrations,
    RUNNING mcp-serve processes) denotes the same database. Built from the
    2026-06-10 incident (standardize-deployment-authority)."""
    _load_main_helpers()
    from okg.substrate.deployment_agreement import run_agreement_check

    from okg.substrate.deployment_default import resolve_command_deployment

    try:
        deployment = resolve_command_deployment(
            getattr(args, "deployment", None),
            command="okg doctor --check dsn-agreement",
        )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    report = run_agreement_check(deployment)
    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


def _doctor_alias_collisions(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """Surface refused alias values as a standing operator warning."""
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.catalog.aliases import list_alias_collisions

    try:
        deployment = getattr(args, "deployment", None)
        dsn = _resolve_deployment_dsn(args) if deployment else _resolve_dsn(args)
        with db_connect(dsn) as conn:
            rows = list_alias_collisions(conn, limit=100)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    payload = {
        "ok": True,
        "status": "WARN" if rows else "OK",
        "check": "alias-collisions",
        "warning_count": len(rows),
        "collisions": rows,
        "remediation": (
            "okg alias collisions --limit 100; then run "
            "okg remediate collapsed-aliases --subtype <type> --field <field>"
            if rows else None
        ),
    }
    _emit_ok(payload, as_json=as_json)
    return 0


def _doctor_declared_attr_indexes(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """The declared-attribute expression indexes (okg#3112 step 2).

    Lifecycle states, invalid indexes of every actor on the graph tables,
    builds stopped by the five-failure cap, view-graph drift, the database
    collation, long values, and each declared node attribute without a ready
    index (``attr_not_indexed``, okg#3112 step 4a). ERROR (exit 1) on drift, a
    capped failure, a ready index that is gone, an invalid index maintained
    for a day, or a declared attribute never enabled, refused by the
    collation, or with maintenance off.
    """
    _load_main_helpers()
    from okg.substrate.declared_attr_indexes import doctor_report

    try:
        deployment = getattr(args, "deployment", None)
        dsn = _resolve_deployment_dsn(args) if deployment else _resolve_dsn(args)
        report = doctor_report(dsn)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    payload = {
        "ok": report["status"] != "ERROR",
        "check": "declared-attr-indexes",
        **report,
    }
    _emit_ok(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


def _doctor_mcp_surface_freshness(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check mcp-surface-freshness [--deployment X]` —
    verify registered OKG MCP servers expose the current instruction and
    trust-boundary surface."""
    _load_main_helpers()
    from okg.substrate.mcp.surface_fleet import fleet_surface_report

    report = fleet_surface_report(deployment=getattr(args, "deployment", None))
    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


def _doctor_extraction(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check extraction [--warm]` — import-check the
    extraction library + report spaCy / GLiNER model status."""
    _load_main_helpers()
    payload: dict = {
        "ok": True,
        "imports": {},
        "spacy_models": {},
        "errors": [],
    }
    # Import each top-level module; record success / error.
    libs = [
        "pypdf", "pdfplumber", "lxml", "selectolax", "markdown_it",
        "frontmatter", "tree_sitter", "nbformat", "docx", "openpyxl",
        "semantic_text_splitter", "spacy", "gliner", "rapidfuzz",
        "dateparser", "ftfy", "bibtexparser", "crossref_commons",
    ]
    for lib in libs:
        try:
            __import__(lib)
            payload["imports"][lib] = "ok"
        except ImportError as e:
            payload["ok"] = False
            payload["imports"][lib] = "missing"
            payload["errors"].append(
                {"library": lib, "error": str(e)}
            )

    # spaCy model presence.
    try:
        import spacy
        common_models = ["en_core_web_sm"]
        for m in common_models:
            try:
                spacy.util.get_package_path(m)
                payload["spacy_models"][m] = "installed"
            except Exception:  # noqa: BLE001 — spacy raises various probe-specific errors when a model isn't installed; treat all as missing.
                payload["spacy_models"][m] = "missing"
                payload["errors"].append({
                    "library": "spacy",
                    "model": m,
                    "fix_hint": f"python -m spacy download {m}",
                })
    except ImportError:
        pass  # already reported above

    # --warm: eager-load the configured spaCy / GLiNER models.
    if getattr(args, "warm", False):
        deployment = (
            getattr(args, "deployment", None)
            or os.environ.get("OKG_DEPLOYMENT")
        )
        if deployment:
            try:
                from pathlib import Path
                from okg.substrate.library.extraction.registry import (
                    load_extractors,
                )
                # Find the deployment dir.
                here = Path(__file__).resolve()
                # Walk up to repo root.
                for candidate in here.parents:
                    if (candidate / "deployments" / deployment).is_dir():
                        deployment_dir = candidate / "deployments" / deployment
                        break
                else:
                    deployment_dir = None
                if deployment_dir and (deployment_dir / "extractors.yaml").is_file():
                    bundle = load_extractors(deployment_dir)
                    payload["warm"] = {"deployment": deployment, "loaded": True}
                    if bundle.ner is not None:
                        for backend in getattr(
                            bundle.ner, "_backends", [bundle.ner],
                        ):
                            if hasattr(backend, "warmup"):
                                try:
                                    backend.warmup()
                                except Exception as e:
                                    payload["ok"] = False
                                    payload["errors"].append({
                                        "stage": "warmup",
                                        "backend": type(backend).__name__,
                                        "error": str(e),
                                    })
                else:
                    payload["warm"] = {
                        "deployment": deployment, "loaded": False,
                        "reason": "no extractors.yaml in deployment dir",
                    }
            except Exception as e:
                payload["ok"] = False
                payload["errors"].append({
                    "stage": "warm",
                    "error": str(e),
                })

    _emit_ok(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


def _doctor_extractors(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check extractors --deployment <name>` —
    pre-instantiate every parser / chunker / NER class declared in
    the deployment's `extractors.yaml`.

    Catches import-path drift (typo'd class name, removed module)
    at deploy time instead of "fail at first ingest". Per the
    post-bootstrap audit (2026-04-26).
    """
    _load_main_helpers()
    import importlib

    deployment = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg doctor --check extractors requires "
                "--deployment <name> or OKG_DEPLOYMENT",
            ),
            as_json=as_json,
        )

    from okg.substrate.deployments import find_deployment_dir
    deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None:
        return _emit_error(
            SubstrateError(
                f"deployment dir for {deployment!r} not found",
                fix_hint=(
                    "check the deployment exists under "
                    "deployments/<name>/"
                ),
            ),
            as_json=as_json,
        )

    extractors_yaml = deployment_dir / "extractors.yaml"
    if not extractors_yaml.is_file():
        report = {
            "ok": True,
            "deployment": deployment,
            "extractors_yaml": str(extractors_yaml),
            "skipped": "extractors.yaml not present (deployment "
                       "may not declare doc-corpus extraction)",
        }
        _emit_ok(report, as_json=as_json)
        return 0

    with extractors_yaml.open() as f:
        doc = fastyaml.safe_load(f) or {}

    report: dict = {
        "ok": True,
        "deployment": deployment,
        "extractors_yaml": str(extractors_yaml),
        "axes": {"parser": [], "chunker": [], "ner": []},
        "errors": [],
    }

    for axis in ("parser", "chunker", "ner"):
        entries = doc.get(axis) or []
        for idx, entry in enumerate(entries):
            cls_path = (entry or {}).get("class")
            cfg = (entry or {}).get("config") or {}
            ax_report = {
                "index": idx,
                "class": cls_path,
                "format": entry.get("format") if axis == "parser" else None,
            }
            if not cls_path:
                report["ok"] = False
                ax_report["status"] = "FAIL"
                ax_report["error"] = "missing `class:` key"
                report["axes"][axis].append(ax_report)
                report["errors"].append({
                    "axis": axis, "index": idx,
                    "code": "extractor_entry_missing_class",
                })
                continue
            try:
                module_path, class_name = cls_path.rsplit(".", 1)
                module = importlib.import_module(module_path)
                cls = getattr(module, class_name)
            except (ImportError, AttributeError, ValueError) as e:
                report["ok"] = False
                ax_report["status"] = "FAIL"
                ax_report["error"] = (
                    f"{type(e).__name__}: {e}"
                )
                report["axes"][axis].append(ax_report)
                report["errors"].append({
                    "axis": axis, "index": idx,
                    "code": "extractor_class_not_resolvable",
                    "class": cls_path,
                })
                continue
            # Try to instantiate. Some classes need runtime args
            # (e.g. spaCy backends need a model), so we don't
            # treat instantiation failure with TypeError-on-
            # missing-required-arg as fatal — but we DO flag
            # it so the operator can verify config completeness.
            try:
                cls(**cfg)
                ax_report["status"] = "OK"
            except TypeError as e:
                ax_report["status"] = "WARN"
                ax_report["error"] = (
                    f"class resolved but instantiation failed "
                    f"with config={list(cfg)}: {e}"
                )
            except Exception as e:
                ax_report["status"] = "WARN"
                ax_report["error"] = (
                    f"class resolved but construction raised "
                    f"{type(e).__name__}: {e}"
                )
            report["axes"][axis].append(ax_report)

    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


def _doctor_adapter_init_params(
    entry,
    cls,
    *,
    dsn: str | None,
    deployment: str,
) -> dict:
    """Build constructor kwargs exactly as the ingest runtime does.

    Doctor must construct an adapter the way the worker will, so it
    goes through ``source_adapter_init_params``: the runtime injects
    ``dsn``, ``deployment``, ``source_name`` and ``graph_branch`` and
    refuses registry params that author them. When doctor has no DSN,
    the ``dsn`` key is left out so a constructor that requires one
    reports the missing DSN instead of receiving ``None``.
    """
    from okg.substrate.ingest.adapter_factory import (
        source_adapter_init_params,
    )

    params = source_adapter_init_params(
        entry,
        dsn=dsn,  # type: ignore[arg-type]
        deployment=deployment,
        adapter_class=cls,
    )
    if dsn is None and params.get("dsn") is None:
        params.pop("dsn", None)
    return params


def _doctor_sources(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check sources --deployment <name>` —
    pre-instantiate every source adapter declared in the
    deployment's `source_registry.yaml`.

    Mirrors `_doctor_extractors`: resolves `module.class`
    (FAIL on missing), then attempts construction with the
    declared `params:` (WARN on construction error, since
    many adapters defer expensive setup to first iteration).
    Builds constructor kwargs through the runtime's
    `source_adapter_init_params`, so `dsn` and `deployment` are
    injected exactly as the ingest worker injects them.

    Per `doctor-sources-check` (2026-04-26).
    """
    _load_main_helpers()
    import importlib
    from collections.abc import Mapping

    from okg.substrate.ingest.source_params import (
        ConstructorParameters,
        check_source_params,
    )
    from okg.substrate.mcp.source_admin import _admission_reason
    from okg.substrate.mcp.source_lifecycle import (
        ADMISSION_REFUSAL_STATUSES,
    )
    from okg.substrate.source_sync import (
        source_sync_entry_is_explicitly_dormant,
    )

    deployment = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg doctor --check sources requires "
                "--deployment <name> or OKG_DEPLOYMENT",
            ),
            as_json=as_json,
        )

    from okg.substrate.deployments import (
        find_deployment_dir, inject_deployment_path,
    )
    deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None:
        return _emit_error(
            SubstrateError(
                f"deployment dir for {deployment!r} not found",
                fix_hint=(
                    "check the deployment exists under "
                    "deployments/<name>/"
                ),
            ),
            as_json=as_json,
        )
    inject_deployment_path(deployment_dir)

    registry_yaml = deployment_dir / "source_registry.yaml"
    if not registry_yaml.is_file():
        report = {
            "ok": True,
            "deployment": deployment,
            "source_registry_yaml": str(registry_yaml),
            "skipped": "source_registry.yaml not present",
        }
        _emit_ok(report, as_json=as_json)
        return 0

    from okg.substrate.sources.registry import (
        STRICT_ADMISSION_CONTRACT,
        load_source_registry,
    )
    try:
        registry = load_source_registry(registry_yaml)
    except (ValueError, FileNotFoundError) as e:
        return _emit_error(
            SubstrateError(
                f"failed to load source_registry.yaml: {e}",
            ),
            as_json=as_json,
        )

    # Per `one-db-per-deployment` (2026-04-26): manifest is
    # authoritative when --deployment is set. Best-effort
    # for the doctor sources check: if DSN resolution fails,
    # we still report adapter resolvability without
    # injecting `dsn` into constructors.
    try:
        dsn = _resolve_dsn(args)
    except SubstrateError:
        dsn = None

    def _as_list(value) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, (list, tuple)):
            return [str(v) for v in value]
        return [str(value)]

    def _iso(value) -> str | None:
        return value.isoformat() if value is not None else None

    def _compact_jsonb(value, *, max_items: int = 12) -> object:
        """Keep source doctor output readable for large cursor blobs."""
        if value is None:
            return None
        try:
            encoded = json.dumps(value, sort_keys=True, default=str)
        except TypeError:
            encoded = str(value)
        if len(encoded) <= 2000:
            return value
        if isinstance(value, dict):
            keys = sorted(str(k) for k in value)[:max_items]
            return {
                "type": "object",
                "keys": len(value),
                "sample_keys": keys,
                "truncated": True,
            }
        if isinstance(value, list):
            return {
                "type": "array",
                "items": len(value),
                "sample": value[:max_items],
                "truncated": True,
            }
        return {
            "type": type(value).__name__,
            "preview": encoded[:2000],
            "truncated": True,
        }

    def _runtime_snapshot() -> dict:
        empty = {
            "available": False,
            "error": None,
            "watermarks": {},
            "progress": {},
            "node_facts": {},
            "edge_facts": {},
            "live_nodes_by_subtype": {},
            "edge_facts_by_type": {},
            "producer_profiles": {},
            # Latest admission outcome per producer. A REFUSED admission
            # writes nothing else -- no watermark, no source-progress row, no
            # facts -- so without this every other key above reads a refused
            # source as an idle healthy one (issue #1323).
            "admissions": {},
            "admissions_available": False,
            "admissions_error": None,
        }
        if dsn is None:
            empty["error"] = "dsn_unavailable"
            return empty

        from okg.substrate.db.connection import connect as db_connect
        try:
            with db_connect(dsn, autocommit=True) as conn, \
                 conn.cursor() as cur:
                # Deep: a shallow copy would alias `empty["admissions"]`, so a
                # raise after partial folding would return `empty` carrying
                # rows beside `admissions_available: False`.
                snap = copy.deepcopy(empty)
                snap["available"] = True
                snap["error"] = None

                cur.execute(
                    """
                    SELECT source, last_reconcile_at,
                           projection_cursor, last_applied_rev,
                           last_published_rev
                      FROM okg.source_watermarks
                    """
                )
                snap["watermarks"] = {
                    row[0]: {
                        "last_reconcile_at": _iso(row[1]),
                        "projection_cursor": row[2],
                        "last_applied_rev": _compact_jsonb(row[3]),
                        "last_published_rev": _compact_jsonb(row[4]),
                    }
                    for row in cur.fetchall()
                }

                cur.execute(
                    """
                    SELECT source_name, count(*) AS n_rows,
                           max(updated_at) AS last_updated_at
                      FROM okg.source_progress
                     WHERE deployment = %s
                     GROUP BY source_name
                    """,
                    (deployment,),
                )
                snap["progress"] = {
                    row[0]: {
                        "rows": int(row[1] or 0),
                        "last_updated_at": _iso(row[2]),
                    }
                    for row in cur.fetchall()
                }

                cur.execute(
                    """
                    SELECT source, count(*) AS n_facts,
                           max(fact_id) AS max_fact_id,
                           max(observed_at) AS last_observed_at
                      FROM okg.node_facts
                     GROUP BY source
                    """
                )
                snap["node_facts"] = {
                    row[0]: {
                        "count": int(row[1] or 0),
                        "max_fact_id": int(row[2] or 0),
                        "last_observed_at": _iso(row[3]),
                    }
                    for row in cur.fetchall()
                }

                cur.execute(
                    """
                    SELECT source, count(*) AS n_facts,
                           max(fact_id) AS max_fact_id,
                           max(observed_at) AS last_observed_at
                      FROM okg.edge_facts
                     GROUP BY source
                    """
                )
                snap["edge_facts"] = {
                    row[0]: {
                        "count": int(row[1] or 0),
                        "max_fact_id": int(row[2] or 0),
                        "last_observed_at": _iso(row[3]),
                    }
                    for row in cur.fetchall()
                }

                # Chronos cutover: okg.nodes_live (and its bitemporal
                # valid_to_gen column) was dropped. The "live" node set is now
                # simply the current default-branch graph_nodes, read through a
                # Chronos checkout. Count per (source, subtype) directly.
                from okg.substrate.graph_store import ChronosGraphStore

                live_graph = ChronosGraphStore.from_connection(
                    conn, ensure_schema=False,
                ).checkout()
                for row in live_graph.query(
                    """
                    SELECT source, subtype, count(*) AS n
                      FROM okg.graph_nodes
                     GROUP BY source, subtype
                    """
                ):
                    snap["live_nodes_by_subtype"].setdefault(
                        row["source"], {},
                    )[row["subtype"]] = int(row["n"] or 0)

                cur.execute(
                    """
                    SELECT source, edge_type, count(*)
                      FROM okg.edge_facts
                     WHERE op = 'I'
                     GROUP BY source, edge_type
                    """
                )
                for source, edge_type, count in cur.fetchall():
                    snap["edge_facts_by_type"].setdefault(
                        source, {},
                    )[edge_type] = int(count or 0)

                cur.execute(
                    """
                    SELECT DISTINCT ON (producer_id)
                           producer_id, status, profile_fingerprint,
                           catalog_version_id, output_signature,
                           catalog_evidence, updated_at
                      FROM okg.producer_profiles
                     WHERE deployment_name = %s
                     ORDER BY producer_id,
                              CASE status WHEN 'active' THEN 0 ELSE 1 END,
                              updated_at DESC
                    """,
                    (deployment,),
                )
                snap["producer_profiles"] = {
                    row[0]: {
                        "status": row[1],
                        "profile_fingerprint": row[2],
                        "catalog_version_id": row[3],
                        "output_signature": _compact_jsonb(row[4]),
                        "catalog_evidence": _compact_jsonb(row[5]),
                        "updated_at": _iso(row[6]),
                    }
                    for row in cur.fetchall()
                }

                _read_admissions(cur, snap)
                return snap
        except Exception as e:
            empty["error"] = f"{type(e).__name__}: {e}"
            return empty

    def _read_admissions(cur, snap: dict) -> None:
        """Fold the newest admission outcome per producer into `snap`.

        Indexed by BOTH `producer_id` and `source`: a registry entry may
        declare `admission_policy.producer_id` independently of its
        `ownership_id`, and ingest stamps `producer_id` from the policy while
        stamping `source` with the ownership id.

        `producer_id` is always indexed, since it is an exact identity. The
        `source` key is indexed only for a row whose `producer_kind` is the
        source's own, because `source` is not a source-owned column:
        `entity_merge`, `source_retirement` and `source_lineage_repair` all
        stamp a registry source's ownership id while owning
        `producer_kind: "system"`. Without that guard a failed merge batch
        would be reported as the source's own refusal. The guard lives in the
        indexing rather than in a SQL predicate so a source declaring some
        other producer kind still matches on its exact id. Failure here sets
        `admissions_available: False` and is reported -- an unreadable
        envelope table is NOT evidence that nothing was refused.
        """
        from okg.substrate.mcp.source_lifecycle import (
            ADMISSION_ADVERSE_STATUSES,
        )

        try:
            cur.execute(
                """
                SELECT DISTINCT ON (producer_id, source)
                       producer_id, source, producer_kind, status, verdict,
                       GREATEST(created_at, updated_at) AS last_activity_at,
                       count(*) OVER (
                           PARTITION BY producer_id, source
                       ) AS envelope_count,
                       count(*) FILTER (WHERE status = ANY(%s)) OVER (
                           PARTITION BY producer_id, source
                       ) AS adverse_count
                  FROM okg.admission_envelopes
                 WHERE deployment_name = %s
                 ORDER BY producer_id, source,
                          GREATEST(created_at, updated_at) DESC
                """,
                (list(ADMISSION_ADVERSE_STATUSES), deployment),
            )
            rows = cur.fetchall()
        except Exception as e:  # noqa: BLE001 - diagnostic must continue
            snap["admissions_available"] = False
            snap["admissions_error"] = f"{type(e).__name__}: {e}"
            return
        snap["admissions_available"] = True
        for (
            producer_id, source, row_producer_kind, status, verdict,
            last_activity_at, envelope_count, adverse_count,
        ) in rows:
            outcome = {
                "producer_id": producer_id,
                "source": source,
                "status": status,
                # NOT the envelope's own `decided_at` column, which stays NULL
                # until a terminal transition. Named for what it is.
                "last_activity_at": _iso(last_activity_at),
                "envelope_count": int(envelope_count or 0),
                "adverse_count": int(adverse_count or 0),
                "reason_code": _admission_reason(verdict)[0],
            }
            # `producer_id` is always indexed: it is an exact identity, so a
            # source declaring a non-`source` producer kind still matches.
            # `source` is indexed ONLY for a row whose kind is the source's
            # own -- entity merge, source retirement and lineage repair stamp
            # a registry source's ownership id while owning
            # `producer_kind: "system"`, and reading those as the source's own
            # refusal would fail the doctor for every source a failed merge
            # batch touched.
            keys = [producer_id]
            if str(row_producer_kind) == "source":
                keys.append(source)
            for key in keys:
                if not key:
                    continue
                prior = snap["admissions"].get(key)
                if prior is None or str(
                    prior.get("last_activity_at") or ""
                ) <= str(outcome["last_activity_at"] or ""):
                    snap["admissions"][key] = outcome

    runtime = _runtime_snapshot()
    registry_by_name = dict(registry)

    report: dict = {
        "ok": True,
        "deployment": deployment,
        "source_registry_yaml": str(registry_yaml),
        "runtime_available": bool(runtime["available"]),
        "runtime_error": runtime["error"],
        "sources": [],
        "derived_producers": [],
        "errors": [],
        "warnings": [],
    }

    def _producer_profile_for(producer_id: str) -> tuple[dict, str]:
        from okg.substrate.producer_authority import producer_id_candidates

        for candidate in producer_id_candidates(producer_id):
            profile = runtime["producer_profiles"].get(candidate)
            if profile:
                return profile, candidate
        return {}, producer_id

    derived_expected: dict[str, dict] = {}
    try:
        from okg.substrate.publisher.enricher_loader import load_enrichers
        for enricher in load_enrichers(deployment_dir):
            ename = getattr(enricher, "name", enricher.__class__.__name__)
            producer_id = f"_enricher:{ename}"
            derived_expected[producer_id] = {
                "producer_id": producer_id,
                "producer_kind": "enricher",
                "declared_by": "enricher_class",
                "observed_sources": [],
            }
    except Exception as e:  # noqa: BLE001 - diagnostic should continue
        report["warnings"].append({
            "code": "derived_enricher_load_failed",
            "message": f"could not load derived enrichers: {e}",
        })
    try:
        from okg.substrate.publisher.deployment_stack import (
            load_derived_producer_policies,
        )
        for producer_id, policy in load_derived_producer_policies(
            deployment_dir
        ).items():
            derived_expected[producer_id] = {
                "producer_id": producer_id,
                "producer_kind": policy.get("producer_kind", "extractor"),
                "declared_by": "deployment_manifest",
                "authority": policy,
                "observed_sources": [],
            }
    except Exception as e:  # noqa: BLE001 - diagnostic should continue
        report["warnings"].append({
            "code": "derived_policy_load_failed",
            "message": f"could not load derived producer policies: {e}",
        })

    from okg.substrate.producer_authority import producer_id_candidates
    for observed_source in sorted(runtime["edge_facts"]):
        if not (
            str(observed_source).startswith("_enricher:")
            or str(observed_source).startswith("_extractor:")
        ):
            continue
        candidates = producer_id_candidates(str(observed_source))
        producer_id = next(
            (candidate for candidate in candidates if candidate in derived_expected),
            str(observed_source),
        )
        entry = derived_expected.setdefault(producer_id, {
            "producer_id": producer_id,
            "producer_kind": (
                "extractor"
                if producer_id.startswith("_extractor:")
                else "enricher"
            ),
            "declared_by": "observed_edge_facts",
            "observed_sources": [],
        })
        entry.setdefault("observed_sources", []).append(str(observed_source))

    for producer_id, entry in sorted(derived_expected.items()):
        profile, resolved_profile_id = _producer_profile_for(producer_id)
        authority = entry.get("authority") or {}
        signature = (
            profile.get("output_signature")
            if profile else authority.get("output_signature") or {}
        )
        warnings = []
        if entry.get("declared_by") == "observed_edge_facts":
            warnings.append("derived_producer_not_declared")
        if runtime["available"] and not profile:
            warnings.append("producer_profile_not_materialized")
        elif profile.get("status") == "compat":
            warnings.append("producer_signature_missing")
        derived_report = {
            "producer_id": producer_id,
            "producer_kind": entry.get("producer_kind"),
            "declared_by": entry.get("declared_by"),
            "status": "WARN" if warnings else "OK",
            "warnings": warnings,
            "observed_sources": sorted(set(entry.get("observed_sources") or [])),
            "profile": {
                "resolved_producer_id": resolved_profile_id,
                "profile_status": profile.get("status"),
                "profile_fingerprint": profile.get("profile_fingerprint"),
                "catalog_version_id": profile.get("catalog_version_id"),
                "signature_summary": {
                    "node_rules": len(signature.get("nodes") or []),
                    "edge_rules": len(signature.get("edges") or []),
                },
                "catalog_evidence": profile.get("catalog_evidence"),
            },
        }
        if warnings:
            report["warnings"].append({
                "name": producer_id,
                "code": "derived_producer_warning",
                "warnings": warnings,
            })
        report["derived_producers"].append(derived_report)

    for name, entry in sorted(registry.items()):
        raw = entry.raw or {}
        module_path = raw.get("module")
        class_name = raw.get("class")
        params = dict(raw.get("params") or {})
        predecessor_raw = raw.get("predecessor_source")
        if predecessor_raw is None:
            predecessor_raw = params.get("predecessor_source")
        predecessors = _as_list(predecessor_raw)
        predecessor_reports = []
        for pred in predecessors:
            pred_entry = registry_by_name.get(pred)
            pred_ownership = (
                pred_entry.ownership_id if pred_entry is not None else pred
            )
            wm = runtime["watermarks"].get(pred_ownership) or {}
            ready = bool(
                wm.get("last_reconcile_at")
                or int(wm.get("projection_cursor") or 0) > 0
            )
            predecessor_reports.append({
                "declared": pred,
                "ownership_id": pred_ownership,
                "ready": ready if runtime["available"] else None,
                "last_reconcile_at": wm.get("last_reconcile_at"),
                "projection_cursor": wm.get("projection_cursor"),
            })
        watermark = runtime["watermarks"].get(entry.ownership_id) or {}
        progress = runtime["progress"].get(entry.ownership_id) or {}
        node_facts = runtime["node_facts"].get(entry.ownership_id) or {}
        edge_facts = runtime["edge_facts"].get(entry.ownership_id) or {}
        live_nodes = runtime["live_nodes_by_subtype"].get(
            entry.ownership_id, {},
        )
        edge_fact_types = runtime["edge_facts_by_type"].get(
            entry.ownership_id, {},
        )
        projection_cursor = int(
            watermark.get("projection_cursor") or 0,
        )
        max_node_fact_id = int(node_facts.get("max_fact_id") or 0)
        projection_lag = max(0, max_node_fact_id - projection_cursor)
        observed = bool(watermark or progress or node_facts or edge_facts)
        predecessor_ready = all(
            p["ready"] for p in predecessor_reports
        ) if runtime["available"] else None
        coverage_warnings = []
        policy_warnings = []
        if entry.admission_policy is None:
            policy_warnings.append("implicit_admission_policy")
            report["warnings"].append({
                "name": name,
                "code": "implicit_admission_policy",
                "message": (
                    "source has no admission_policy; fast-track "
                    "admission will use implicit legacy trust"
                ),
            })
        producer_id = (
            entry.admission_policy.producer_id
            if entry.admission_policy is not None
            else entry.ownership_id
        )
        producer_profile = (
            runtime["producer_profiles"].get(producer_id) or {}
        )
        producer_profile_warnings = []
        if entry.admission_policy is None:
            producer_profile_warnings.append("producer_policy_missing")
        elif entry.admission_policy.is_compat_summary_only:
            producer_profile_warnings.append("producer_signature_missing")
        if runtime["available"] and not producer_profile:
            producer_profile_warnings.append("producer_profile_not_materialized")
        signature = (
            entry.admission_policy.output_signature
            if entry.admission_policy is not None
            else {}
        )
        signature_summary = {
            "node_rules": len(signature.get("nodes") or []),
            "edge_rules": len(signature.get("edges") or []),
            "allowed_record_kinds": (
                list(entry.admission_policy.allowed_record_kinds)
                if entry.admission_policy is not None
                else []
            ),
        }
        admission = (
            runtime["admissions"].get(producer_id)
            or runtime["admissions"].get(entry.ownership_id)
            or {}
        )
        admission_status = str(admission.get("status") or "")
        if runtime["available"]:
            if not observed:
                coverage_warnings.append("source_has_no_runtime_rows")
                # Say WHY there are no rows. "No runtime rows" alone reads
                # as "not scheduled yet" and is what let a source refused on
                # every run look idle for its whole enrolment (#1323).
                if not runtime["admissions_available"]:
                    coverage_warnings.append("admission_outcome_unknown")
                elif admission_status in ADMISSION_REFUSAL_STATUSES:
                    coverage_warnings.append(
                        "admission_refused:"
                        + (admission.get("reason_code") or admission_status)
                    )
                    # Only a NON-DORMANT source is an error. Turning a source
                    # off is the standard remediation for this very symptom,
                    # and its old envelope outlives the change; failing the
                    # doctor forever for a source the operator deliberately
                    # retired leaves no action but deleting envelope rows.
                    #
                    # The predicate is `source_sync_entry_is_explicitly_dormant`
                    # -- the same one the source-sync scheduler uses -- and NOT
                    # `enabled_by_default`, which is a setup-wizard default
                    # that 29 of okg-workspace's 30 sources omit entirely.
                    # Gating on that flag would have made this finding
                    # vacuous for `latex-project` itself, which declares no
                    # such flag and is exactly the source issue #1323 is
                    # about. Only an explicit `false` is dormant.
                    if not source_sync_entry_is_explicitly_dormant(entry):
                        report["ok"] = False
                        report["errors"].append({
                            "name": name,
                            "code": "source_refused_at_admission",
                            "producer_id": admission.get("producer_id"),
                            "status": admission_status,
                            "reason_code": admission.get("reason_code"),
                            "adverse_admissions": admission.get(
                                "adverse_count",
                            ),
                            "last_activity_at": admission.get(
                                "last_activity_at",
                            ),
                            "message": (
                                "source ran and its payload was refused at "
                                "admission, so it wrote no rows and left no "
                                "source-run row; read the verdict in "
                                "okg.admission_envelopes, not "
                                "okg.v_source_run_diagnostics"
                            ),
                        })
                    else:
                        coverage_warnings.append(
                            "admission_refused_while_dormant"
                        )
                elif admission_status:
                    # Admitted but adverse (blocked, quarantined) or still
                    # undecided. Not a refusal, but reporting only "no runtime
                    # rows" here would be the pre-change silence with the
                    # admission data in hand and unused.
                    coverage_warnings.append(
                        f"admission_not_refused:{admission_status}"
                    )
                elif not admission:
                    coverage_warnings.append("source_never_admitted")
            if projection_lag > 0:
                coverage_warnings.append(f"projection_lag:{projection_lag}")
            missing_predecessors = [
                p["ownership_id"] for p in predecessor_reports
                if not p["ready"]
            ]
            if missing_predecessors:
                coverage_warnings.append(
                    "predecessor_not_ready:" + ",".join(
                        missing_predecessors,
                    )
                )
        ax_report: dict = {
            "name": name,
            "ownership_id": entry.ownership_id,
            "module": module_path,
            "class": class_name,
            "source_class": entry.source_class,
            "predecessors": predecessor_reports,
            "admission_policy": {
                "declared": entry.admission_policy is not None,
                "status": "WARN" if policy_warnings else "OK",
                "warnings": policy_warnings,
                "policy": (
                    entry.admission_policy.as_dict()
                    if entry.admission_policy is not None
                    else {
                        "producer_id": entry.ownership_id,
                        "producer_kind": "source",
                        "trust_label": "implicit_legacy_trusted",
                        "admission_mode": "fast_track",
                    }
                ),
            },
            "producer_profile": {
                "producer_id": producer_id,
                "status": (
                    "WARN" if producer_profile_warnings else "OK"
                ),
                "profile_status": producer_profile.get("status"),
                "profile_fingerprint": producer_profile.get(
                    "profile_fingerprint",
                ),
                "catalog_version_id": producer_profile.get(
                    "catalog_version_id",
                ),
                "signature_summary": signature_summary,
                "catalog_evidence": producer_profile.get(
                    "catalog_evidence",
                ),
                "warnings": producer_profile_warnings,
            },
            "coverage": {
                "status": (
                    "UNKNOWN" if not runtime["available"]
                    else "WARN" if coverage_warnings
                    else "OK"
                ),
                "warnings": coverage_warnings,
                "observed": observed if runtime["available"] else None,
                "ready_to_run": predecessor_ready,
                "watermark": watermark,
                "progress": progress,
                "node_facts": node_facts,
                "edge_facts": edge_facts,
                "node_projection": {
                    "cursor": projection_cursor,
                    "max_fact_id": max_node_fact_id,
                    "lag": projection_lag,
                },
                "live_nodes_by_subtype": live_nodes,
                "edge_facts_by_type": edge_fact_types,
                "admission": admission or None,
                "admission_authority": (
                    "admission_envelopes"
                    if runtime["admissions_available"]
                    else "unavailable"
                ),
            },
        }
        if entry.legacy or not module_path or not class_name:
            ax_report["status"] = "SKIP"
            ax_report["note"] = (
                "legacy entry (no module/class declared); "
                "scheduled by the legacy runner"
            )
            report["sources"].append(ax_report)
            continue
        try:
            module = importlib.import_module(module_path)
            cls = getattr(module, class_name)
        except (ImportError, AttributeError, ValueError) as e:
            report["ok"] = False
            ax_report["status"] = "FAIL"
            ax_report["error"] = f"{type(e).__name__}: {e}"
            report["sources"].append(ax_report)
            report["errors"].append({
                "name": name,
                "code": "source_class_not_resolvable",
                "module": module_path,
                "class": class_name,
            })
            continue
        # The comparison ingest runs (#2571 item 2), before construction:
        # a constructor TypeError alone used to be a WARN that left the
        # check ok, so a misspelled key passed doctor.
        strict = entry.admission_contract == STRICT_ADMISSION_CONTRACT
        # An absent or null `params` authors nothing; required keys are
        # still missing.
        raw_params = raw.get("params")
        if raw_params is None:
            raw_params = {}
        try:
            constructor = ConstructorParameters.from_class(cls)
        except (TypeError, ValueError):
            constructor = None
        problems = (
            check_source_params(raw_params, constructor)
            if isinstance(raw_params, Mapping) and constructor is not None
            else ()
        )
        refused = [p for p in problems if strict or p.refused_by_legacy]
        if problems:
            ax_report["param_problems"] = [
                {
                    "code": p.code,
                    "key": p.key,
                    "message": p.message,
                    "refused_at_ingest": p in refused,
                }
                for p in problems
            ]
        if refused:
            report["ok"] = False
            ax_report["status"] = "FAIL"
            ax_report["error"] = "; ".join(p.render(name) for p in refused)
            report["sources"].append(ax_report)
            for p in refused:
                report["errors"].append({
                    "name": name,
                    "code": p.code,
                    "key": p.key,
                    "message": p.message,
                })
            continue
        params: dict | None = None
        try:
            params = _doctor_adapter_init_params(
                entry, cls, dsn=dsn, deployment=deployment,
            )
        except Exception as e:  # noqa: BLE001 - reported below
            # Strict ingest refuses on this same call; legacy may still run.
            ax_report["status"] = "FAIL" if strict else "WARN"
            ax_report["error"] = (
                f"constructor arguments refused: {type(e).__name__}: {e}"
            )
            if strict:
                report["ok"] = False
                report["errors"].append({
                    "name": name,
                    "code": "source_constructor_arguments_refused",
                    "message": str(e),
                })
            report["sources"].append(ax_report)
            continue
        try:
            cls(**params)
            ax_report["status"] = "OK"
        except TypeError as e:
            ax_report["status"] = "WARN"
            ax_report["error"] = (
                f"class resolved but instantiation failed "
                f"with params={list(params)}: {e}"
            )
        except Exception as e:
            ax_report["status"] = "WARN"
            ax_report["error"] = (
                f"class resolved but construction raised "
                f"{type(e).__name__}: {e}"
            )
        if problems:
            # Legacy lets these through today; lint blocks them because the
            # first `okg deployment add-source` promotes the registry to
            # strict_v1, which refuses them.
            ax_report["status"] = "WARN"
            ax_report["warning"] = (
                "; ".join(p.render(name) for p in problems)
                + f". This legacy registry runs today; the next `okg "
                f"deployment add-source` rewrites it as "
                f"{STRICT_ADMISSION_CONTRACT}, which refuses it."
            )
        report["sources"].append(ax_report)

    _add_source_sync_hold(report, dsn=dsn, deployment=deployment, registry=registry)
    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


def _add_source_sync_hold(
    report: dict, *, dsn: str | None, deployment: str, registry: dict,
) -> None:
    """Fail the check while a blocked checkpoint holds source sync (#3342).

    A `repair_required` checkpoint refuses every source-sync run that does
    not repeat its exact selection, before the run starts, so the refused
    sources record nothing and simply stop updating. Every source adapter can
    construct and the check would otherwise read OK.
    """
    from okg.substrate.ingest.incomplete_recovery import (
        source_sync_hold,
        source_sync_hold_line,
    )

    if dsn is None or not report.get("runtime_available"):
        # Same rule as the runtime snapshot above: a database this check
        # cannot read (no DSN, down, not yet migrated) is reported, not failed.
        report.setdefault("warnings", []).append({
            "code": "source_sync_hold_unchecked",
            "message": "the deployment database could not be read, so a "
            "blocked checkpoint holding source sync was not checked",
        })
        return
    sources = {
        name: entry.ownership_id for name, entry in registry.items()
        if not getattr(entry, "legacy", False)
    }
    try:
        hold = source_sync_hold(
            dsn=dsn, deployment=deployment, sources=sources, registry=registry,
        )
    except Exception as exc:  # noqa: BLE001 - reported as an error, not raised.
        report["ok"] = False
        report["errors"].append({
            "code": "source_sync_hold_unknown",
            "error": f"{type(exc).__name__}: {exc}",
        })
        return
    if hold is None:
        return
    report["ok"] = False
    report["source_sync_hold"] = hold
    report["errors"].append({
        "code": "source_sync_held",
        "checkpoint_id": hold["checkpoint_id"],
        "not_admitted_since_block": hold["not_admitted_since_block"],
        "source_classes": hold.get("source_classes", []),
        "automatic_retirement_applies": hold.get(
            "automatic_retirement_applies", False,
        ),
        "error": source_sync_hold_line(hold),
        "fix_hint": hold["escape_hatch_command"],
    })


def _doctor_admission(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check admission` — run admission conformance checks."""
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.admission_conformance import (
        audit_admission_conformance,
    )

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    deployment = getattr(args, "deployment", None)
    generation_id = getattr(args, "generation", None)
    payload: dict[str, Any] = {
        "ok": True,
        "deployment_name": deployment,
        "generation_id": generation_id,
        "checks": [],
        "errors": [],
    }
    try:
        with db_connect(dsn, autocommit=True) as conn:
            findings = audit_admission_conformance(
                conn,
                deployment_name=deployment,
                generation_id=generation_id,
            )
        payload["checks"] = [finding.as_dict() for finding in findings]
        payload["ok"] = all(finding.passed for finding in findings)
        payload["errors"] = [
            finding.as_dict() for finding in findings
            if not finding.passed
        ]
    except Exception as exc:
        payload["ok"] = False
        payload["errors"].append({
            "code": "admission_conformance_probe_failed",
            "message": str(exc),
        })
    _emit_ok(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


def _doctor_formal_assurance(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check formal-assurance` — fail on stale models."""
    _load_main_helpers()
    from okg.substrate.formal_assurance import evaluate_formal_assurance

    payload = evaluate_formal_assurance()
    _emit_ok(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


def _doctor_branches(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check branches --deployment <name>` —

    Verify the AKMON branch-registry invariants.

    The `okg.branches` registry is still the source of truth for which
    branches exist in a deployment (the publisher's `reserve()` and the
    `okg branch` CLI both read/write it), so the registry invariants are
    kept and read from that table:

      - The deployment has exactly one `default` branch
        (`role='default', status='active'`).
      - Exactly one row carries `role='default'` across all statuses.
      - Every `status='merged'` branch has an `applied` merge request
        (AKMON Phase C).
      - Quarantine branches that have aged past their grace thresholds
        warn (AKMON Phase B + Phase 4.3).

    Per-branch published / activity signal: chronos cutover. Under the
    chronos-native publisher (`Publisher._cycle_chronos_native`) a
    generation is a Chronos checkpoint, not an `okg.graph_generations`
    row — that path explicitly does NOT insert/update graph_generations.
    So the per-branch published-generation signal is resolved from the
    Chronos checkpoint store via
    `ChronosGraphStore.latest_generation(deployment_name=..., branch=...,
    status="published")`, reported as an informational
    `branch_published_generations` check.

    Dropped (purely-legacy bitemporal `okg.graph_generations` lifecycle
    invariants with no chronos equivalent — see the inline notes):

      - `generation_has_branch_id` — required every graph_generations row
        to carry a `branch_id`. The chronos publisher writes no such
        rows, so the invariant is vacuous / stranded under chronos.
      - `one_pending_per_branch` — required at most one `status='pending'`
        graph_generations row per `(deployment, branch_id)`. Chronos has
        no `pending` graph_generations lifecycle; reservation/pending
        state is a checkpoint concern. Obsolete under chronos.
    """
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    deployment = getattr(args, "deployment", None)
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg doctor --check branches requires --deployment",
                fix_hint=(
                    "pass --deployment <name> so the check can scope "
                    "the branch invariants to a single deployment"
                ),
            ),
            as_json=as_json,
        )

    payload: dict[str, Any] = {
        "ok": True,
        "deployment_name": deployment,
        "checks": [],
        "errors": [],
    }

    try:
        with db_connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                # 1. Default branch exists exactly once.
                cur.execute(
                    """
                    SELECT count(*)
                      FROM okg.branches
                     WHERE deployment_name = %s
                       AND name = 'default'
                       AND role = 'default'
                       AND status = 'active'
                    """,
                    (deployment,),
                )
                default_count = int(cur.fetchone()[0])
                payload["checks"].append({
                    "name": "default_branch_exists",
                    "ok": default_count == 1,
                    "count": default_count,
                })
                if default_count != 1:
                    payload["ok"] = False
                    payload["errors"].append({
                        "code": "default_branch_missing_or_duplicated",
                        "message": (
                            f"deployment {deployment!r} has "
                            f"{default_count} default/active default "
                            "branches; expected exactly 1"
                        ),
                    })

                # 1b. Exactly one row carries role='default' across
                # all statuses — a row with role='default' but a name
                # other than 'default' is a schema drift the unique
                # (deployment_name, name) constraint does not catch.
                cur.execute(
                    """
                    SELECT count(*)
                      FROM okg.branches
                     WHERE deployment_name = %s
                       AND role = 'default'
                    """,
                    (deployment,),
                )
                role_default_count = int(cur.fetchone()[0])
                payload["checks"].append({
                    "name": "default_role_singleton",
                    "ok": role_default_count == 1,
                    "count": role_default_count,
                })
                if role_default_count > 1:
                    payload["ok"] = False
                    payload["errors"].append({
                        "code": "multiple_default_role_branches",
                        "message": (
                            f"deployment {deployment!r} has "
                            f"{role_default_count} branches with "
                            "role='default'; expected exactly 1"
                        ),
                    })

                # Chronos cutover: the legacy `generation_has_branch_id`
                # and `one_pending_per_branch` invariants were dropped.
                # Both read `okg.graph_generations`, but the chronos-native
                # publisher (`Publisher._cycle_chronos_native`) writes no
                # graph_generations rows — a generation is a Chronos
                # checkpoint. `generation_has_branch_id` (every gg row
                # carries a branch_id) is vacuous under chronos, and
                # `one_pending_per_branch` (at most one `pending` gg row
                # per branch) targets a pending-row lifecycle chronos does
                # not have. The per-branch published / activity signal is
                # now resolved from the Chronos checkpoint store below
                # (`branch_published_generations`).

                # Inventory: branches by role/status for the report.
                cur.execute(
                    """
                    SELECT role, status, count(*) AS n
                      FROM okg.branches
                     WHERE deployment_name = %s
                     GROUP BY role, status
                     ORDER BY role, status
                    """,
                    (deployment,),
                )
                payload["branches"] = [
                    {"role": r, "status": s, "count": int(n)}
                    for r, s, n in cur.fetchall()
                ]

                # Chronos cutover: per-branch published-generation
                # signal. The legacy version read the latest
                # `status='published'` generation_id per branch from
                # okg.graph_generations; under chronos a generation is a
                # Chronos checkpoint, so resolve it from the checkpoint
                # store via ChronosGraphStore.latest_generation(...).
                # Informational only (does NOT flip overall ok): it
                # records, per active branch, the most recent published
                # chronos checkpoint id (or None when the branch has not
                # published yet — e.g. a freshly-created experiment
                # branch). The chronos branch name equals
                # okg.branches.name.
                cur.execute(
                    """
                    SELECT name
                      FROM okg.branches
                     WHERE deployment_name = %s
                       AND status = 'active'
                     ORDER BY name
                    """,
                    (deployment,),
                )
                active_branch_names = [str(r[0]) for r in cur.fetchall()]

                published_by_branch: list[dict[str, Any]] = []
                try:
                    from okg.substrate.graph_store import ChronosGraphStore

                    store = ChronosGraphStore.from_connection(
                        conn, ensure_schema=False,
                    )
                    known_chronos_branches = {
                        info.branch_id
                        for info in store.context.list_branches()
                    }
                    for branch_name in active_branch_names:
                        if branch_name not in known_chronos_branches:
                            # Registered in okg.branches but no chronos
                            # branch yet (no graph write has happened on
                            # it). No published checkpoint to resolve.
                            published_by_branch.append({
                                "branch": branch_name,
                                "latest_published_generation": None,
                            })
                            continue
                        latest = store.latest_generation(
                            deployment_name=deployment,
                            branch=branch_name,
                            status="published",
                        )
                        published_by_branch.append({
                            "branch": branch_name,
                            "latest_published_generation": (
                                latest.checkpoint_id
                                if latest is not None
                                else None
                            ),
                        })
                    payload["checks"].append({
                        "name": "branch_published_generations",
                        "ok": True,
                        "branches": published_by_branch,
                    })
                except Exception as exc:
                    # Resolving the chronos signal must not crash the
                    # registry check. Surface it as a warning so the
                    # registry invariants above still report.
                    payload.setdefault("warnings", []).append({
                        "code": "chronos_generation_probe_failed",
                        "message": (
                            "could not resolve per-branch published "
                            f"generations from the chronos checkpoint "
                            f"store: {exc}"
                        ),
                    })

                # AKMON Phase C: any branch with status='merged'
                # must have an `applied` merge request. Catches the
                # case where a manual SQL update flipped the branch
                # without going through the merge pipeline. Hard
                # fail.
                cur.execute(
                    """
                    SELECT b.branch_id, b.name
                      FROM okg.branches b
                     WHERE b.deployment_name = %s
                       AND b.status = 'merged'
                       AND NOT EXISTS (
                         SELECT 1 FROM okg.branch_merge_requests r
                          WHERE r.source_branch_id = b.branch_id
                            AND r.status = 'applied'
                       )
                    """,
                    (deployment,),
                )
                merged_without_request = [
                    {"branch_id": int(b), "name": str(n)}
                    for b, n in cur.fetchall()
                ]
                payload["checks"].append({
                    "name": "merged_branches_have_applied_request",
                    "ok": not merged_without_request,
                    "missing": merged_without_request,
                })
                if merged_without_request:
                    payload["ok"] = False
                    payload["errors"].append({
                        "code": "merged_without_applied_request",
                        "message": (
                            f"{len(merged_without_request)} merged "
                            "branches lack an applied merge request"
                        ),
                        "branches": merged_without_request,
                    })

                # AKMON Phase B: warn on quarantine branches older
                # than the deployment-configured threshold (default
                # 7 days) with no open merge request. These are
                # branches an operator created or that the publisher
                # auto-created for a quarantined envelope and then
                # forgot about. Warn-only — doesn't flip overall ok.
                cur.execute(
                    """
                    SELECT branch_id, name, created_at
                      FROM okg.branches
                     WHERE deployment_name = %s
                       AND role = 'quarantine'
                       AND status = 'active'
                       AND created_at < now() - interval '7 days'
                     ORDER BY created_at ASC
                    """,
                    (deployment,),
                )
                stale = [
                    {
                        "branch_id": int(b),
                        "name": str(n),
                        "created_at": (
                            c.isoformat() if c else None
                        ),
                    }
                    for b, n, c in cur.fetchall()
                ]
                payload["checks"].append({
                    "name": "no_stale_quarantine_branches",
                    "ok": not stale,
                    "stale": stale,
                })
                if stale:
                    payload.setdefault("warnings", []).append({
                        "code": "quarantine_stale",
                        "message": (
                            f"{len(stale)} quarantine branch(es) "
                            "active for > 7 days with no merge "
                            "request"
                        ),
                        "branches": stale,
                    })

                # AKMON Phase 4.3 / cleanup-akmon-umbrella-risk-debts
                # task 2: warn on quarantine branches that have ZERO
                # admission envelopes routed to them and have aged
                # past --quarantine-empty-grace-hours (default 24).
                # These are typically created by a typoed
                # `--target-branch quarantine:misspelt-reason` flag;
                # the publisher auto-creates the branch silently, and
                # the operator never sees the typo until the doctor
                # surfaces it.
                empty_grace_hours = int(
                    getattr(args, "quarantine_empty_grace_hours", None)
                    or 24
                )
                cur.execute(
                    """
                    SELECT b.branch_id, b.name, b.created_at
                      FROM okg.branches b
                 LEFT JOIN okg.admission_envelopes e
                        ON e.deployment_name = b.deployment_name
                       AND e.target_branch_name = b.name
                     WHERE b.deployment_name = %(deployment)s
                       AND b.role = 'quarantine'
                       AND b.status = 'active'
                       AND b.created_at <
                           now() - make_interval(hours => %(hours)s)
                     GROUP BY b.branch_id, b.name, b.created_at
                    HAVING count(e.admission_id) = 0
                     ORDER BY b.created_at ASC
                    """,
                    {
                        "deployment": deployment,
                        "hours": empty_grace_hours,
                    },
                )
                empty = [
                    {
                        "branch_id": int(b),
                        "name": str(n),
                        "created_at": (
                            c.isoformat() if c else None
                        ),
                    }
                    for b, n, c in cur.fetchall()
                ]
                payload["checks"].append({
                    "name": "no_empty_quarantine_branches",
                    "ok": not empty,
                    "empty": empty,
                    "grace_hours": empty_grace_hours,
                })
                if empty:
                    payload.setdefault("warnings", []).append({
                        "code": "quarantine_branch_empty",
                        "message": (
                            f"{len(empty)} quarantine branch(es) "
                            f"have no admission envelopes routed to "
                            f"them after {empty_grace_hours}h — "
                            "likely created by a typoed "
                            "--target-branch flag"
                        ),
                        "branches": empty,
                        "fix_hint": (
                            "verify the branch name matches what the "
                            "operator intended; if the branch was "
                            "abandoned, transition it to status="
                            "'merged' or drop it via okg branch "
                            "abandon"
                        ),
                    })
    except Exception as exc:
        payload["ok"] = False
        payload["errors"].append({
            "code": "branches_probe_failed",
            "message": str(exc),
        })

    _emit_ok(payload, as_json=as_json)
    return 0 if payload["ok"] else 1

def _doctor_edge_fact_strandings(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check edge-fact-strandings` — surface op='I'
    edge_facts that the legacy projector left stranded. Per
    `fix-orphan-edge-projection-stranding`.

    Chronos cutover: the legacy projector that populated
    okg.edge_fact_strandings was retired, so on a chronos DB the table is
    typically absent or empty (→ GREEN). Endpoint liveness and
    cascade-eligibility are resolved against the CURRENT chronos graph
    (okg.graph_nodes / okg.graph_edges); the bitemporal age buckets
    (`age_gens`, derived from a published generation_id) are dropped.

    Severity: GREEN when no strandings. YELLOW when any stranding is
    present. RED when any stranding is genuine (both endpoints still live in
    the current graph → a real projection bug rather than a stale retraction).
    """
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    # Chronos cutover: the live "is the endpoint live?" test that used to read
    # okg.nodes_live / okg.edges_live (with the bitemporal valid_to_gen column,
    # now dropped) is replaced by membership in the CURRENT chronos graph
    # (okg.graph_nodes / okg.graph_edges via a default-branch checkout). The
    # purely-bitemporal age buckets (age_gens = current published generation_id
    # - first_seen_gen) have no chronos equivalent and are dropped — severity
    # already keys on `genuine` (both endpoints live), not on age.
    red = 0
    yellow = 0
    try:
        with db_connect(dsn, autocommit=True) as conn, \
                conn.cursor() as cur:
            # The legacy projector that populated okg.edge_fact_strandings was
            # retired in the chronos cutover; on a chronos DB the table may be
            # absent or empty. Treat absent/empty as "no strandings" (GREEN).
            cur.execute("SELECT to_regclass('okg.edge_fact_strandings') AS t")
            strandings_exists = bool((cur.fetchone() or (None,))[0])
            stranded_rows: list[tuple] = []
            if strandings_exists:
                cur.execute(
                    """
                    SELECT s.fact_id, ef.edge_type, ef.src, ef.dst,
                           s.retry_count
                      FROM okg.edge_fact_strandings s
                      JOIN okg.edge_facts ef USING (fact_id)
                    """
                )
                stranded_rows = cur.fetchall()

            # Resolve endpoint liveness + cascade-eligibility against the
            # current chronos graph.
            from okg.substrate.graph_store import ChronosGraphStore

            live_graph = ChronosGraphStore.from_connection(
                conn, ensure_schema=False,
            ).checkout()
            live_node_ids: set[str] = {
                row["node_id"]
                for row in live_graph.query(
                    "SELECT node_id FROM okg.graph_nodes"
                )
            }
            # dst node_ids that still have a live `contains` parent edge.
            contained_dsts: set[str] = {
                row["dst"]
                for row in live_graph.query(
                    """
                    SELECT DISTINCT dst
                      FROM okg.graph_edges
                     WHERE edge_type = 'contains'
                    """
                )
            }

            total = len(stranded_rows)
            genuine = src_retr = dst_retr = both_retr = 0
            cascade_eligible = 0
            max_retries = 0
            for fact_id, edge_type, src, dst, retry_count in stranded_rows:
                src_live = src in live_node_ids
                dst_live = dst in live_node_ids
                if src_live and dst_live:
                    genuine += 1
                elif not src_live and dst_live:
                    src_retr += 1
                elif src_live and not dst_live:
                    dst_retr += 1
                else:
                    both_retr += 1
                if (
                    edge_type == "contains"
                    and not src_live and dst_live
                    and dst not in contained_dsts
                ):
                    cascade_eligible += 1
                max_retries = max(max_retries, int(retry_count or 0))
            # Pull a few example fact_ids per bucket for diagnosis.
            sample: list[dict[str, Any]] = []
            if strandings_exists:
                cur.execute(
                    """
                    SELECT s.fact_id, ef.src, ef.dst, ef.edge_type,
                           s.first_seen_gen, s.retry_count
                      FROM okg.edge_fact_strandings s
                      JOIN okg.edge_facts ef USING (fact_id)
                     ORDER BY s.first_seen_gen ASC
                     LIMIT 10
                    """
                )
                sample = [
                    {
                        "fact_id": int(r[0]),
                        "src": r[1],
                        "dst": r[2],
                        "edge_type": r[3],
                        "first_seen_gen": int(r[4]),
                        "retry_count": int(r[5]),
                    }
                    for r in cur.fetchall()
                ]
    except Exception as exc:
        payload = {
            "ok": False,
            "status": "ERROR",
            "errors": [
                {
                    "code": "edge_fact_strandings_probe_failed",
                    "message": str(exc),
                }
            ],
        }
        _emit_ok(payload, as_json=as_json)
        return 1
    total = int(total or 0)
    red = int(red or 0)
    yellow = int(yellow or 0)
    genuine = int(genuine or 0)
    src_retr = int(src_retr or 0)
    dst_retr = int(dst_retr or 0)
    both_retr = int(both_retr or 0)
    max_retries = int(max_retries or 0)
    cascade_eligible = int(cascade_eligible or 0)
    # Per `retire-permanent-strandings` (2026-05-19): severity
    # keys on genuine strandings (both endpoints live → real
    # projector bug), not on age. Retire-eligible rows are
    # cleaned up each publish; remaining rows are either:
    #   - genuine: RED (projector retry phase isn't catching it)
    #   - some-retracted but not yet retired: YELLOW (transient)
    #   - zero: GREEN
    if genuine > 0:
        status = "RED"
    elif total > 0:
        status = "YELLOW"
    else:
        status = "GREEN"
    payload = {
        "ok": status != "RED",
        "status": status,
        "diagnostics": {
            "total_strandings": total,
            "red_count": red,
            "yellow_count": yellow,
            "by_classification": {
                "genuine": genuine,
                "src_retracted": src_retr,
                "dst_retracted": dst_retr,
                "both_retracted": both_retr,
            },
            "cascade_eligible": cascade_eligible,
            "max_retries_observed": max_retries,
            "sample": sample,
        },
        "warnings": [],
    }
    if status == "RED":
        payload["warnings"].append({
            "code": "edge_fact_strandings_genuine",
            "message": (
                f"{genuine} genuine edge_fact stranding(s): both endpoints "
                "are live in the current chronos graph, so the edge should "
                "have projected — operator decision required (retract or "
                "fix source emission)."
            ),
            "fix_hint": (
                "inspect okg.edge_fact_strandings JOIN okg.edge_facts for "
                "rows whose src+dst both exist in okg.graph_nodes; either "
                "DELETE the row (if the edge is no longer wanted) or "
                "wait for the cascade-retraction follow-up "
                "(`fix-contains-cascade-on-parent-retraction`) for "
                "src/dst-retracted cases."
            ),
        })
    elif status == "YELLOW":
        payload["warnings"].append({
            "code": "edge_fact_strandings_present",
            "message": (
                f"{total} edge_fact stranding(s) present "
                f"(genuine={genuine}, src_retracted={src_retr}, "
                f"dst_retracted={dst_retr}, "
                f"both_retracted={both_retr})."
            ),
            "fix_hint": (
                "genuine strandings clear on next publish via the "
                "projector's retry phase. *_retracted cases await "
                "the cascade-retraction follow-up."
            ),
        })
    _emit_ok(payload, as_json=as_json)
    return 0 if status != "RED" else 1

def _doctor_acceptance_criteria(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check acceptance-criteria` — report OpenSpec
    criteria without deterministic test evidence links."""
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.library.linkers.acceptance_criteria import (
        AcceptanceCriteriaLinker,
    )

    try:
        dsn = _resolve_dsn(args)
        with db_connect(dsn) as conn:
            diagnostics = AcceptanceCriteriaLinker().diagnostics(conn)
    except Exception as exc:
        payload = {
            "ok": False,
            "status": "ERROR",
            "errors": [
                {
                    "code": "acceptance_criteria_probe_failed",
                    "message": str(exc),
                }
            ],
        }
        _emit_ok(payload, as_json=as_json)
        return 1

    unmatched = diagnostics.unmatched_criterion_count
    payload = {
        "ok": True,
        "status": "WARN" if unmatched else "OK",
        "diagnostics": diagnostics.as_dict(),
        "warnings": [],
    }
    if unmatched:
        payload["warnings"].append({
            "code": "acceptance_criteria_without_test_evidence",
            "message": (
                f"{unmatched} acceptance criteria have no matching "
                "test report or testcase evidence"
            ),
            "fix_hint": (
                "add an OpenSpec criterion/scenario id to JUnit "
                "testcase properties, pytest markers, classname, or "
                "test name; accepted forms include full ids and "
                "crit:<change>:<index>"
            ),
            "sample": diagnostics.unmatched_criteria[:10],
        })
    _emit_ok(payload, as_json=as_json)
    return 0


def _doctor_source_auth(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check source-auth --deployment <name>` —
    run optional source preflight hooks without emitting facts."""
    _load_main_helpers()
    import importlib
    import inspect

    from okg.substrate.library.sources.base import (
        SourcePreflightResult,
    )
    from okg.substrate.sources.redaction import redact_text

    deployment = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg doctor --check source-auth requires "
                "--deployment <name> or OKG_DEPLOYMENT",
            ),
            as_json=as_json,
        )

    from okg.substrate.deployments import (
        find_deployment_dir, inject_deployment_path,
    )
    deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None:
        return _emit_error(
            SubstrateError(
                f"deployment dir for {deployment!r} not found",
                fix_hint=(
                    "check the deployment exists under "
                    "deployments/<name>/"
                ),
            ),
            as_json=as_json,
        )
    inject_deployment_path(deployment_dir)

    registry_yaml = deployment_dir / "source_registry.yaml"
    if not registry_yaml.is_file():
        report = {
            "ok": True,
            "deployment": deployment,
            "source_registry_yaml": str(registry_yaml),
            "skipped": "source_registry.yaml not present",
            "sources": [],
        }
        _emit_ok(report, as_json=as_json)
        return 0

    from okg.substrate.sources.registry import load_source_registry
    try:
        registry = load_source_registry(registry_yaml)
    except (ValueError, FileNotFoundError) as e:
        return _emit_error(
            SubstrateError(
                f"failed to load source_registry.yaml: {e}",
            ),
            as_json=as_json,
        )

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError:
        dsn = None

    failure_statuses = {
        "missing_credential",
        "auth_failed",
        "tls_failed",
        "endpoint_failed",
        "cache_missing",
    }
    report: dict = {
        "ok": True,
        "deployment": deployment,
        "source_registry_yaml": str(registry_yaml),
        "sources": [],
        "errors": [],
    }

    def _coerce_result(
        name: str,
        required: bool,
        value,
    ) -> SourcePreflightResult:
        if isinstance(value, SourcePreflightResult):
            return value
        if hasattr(value, "as_dict"):
            value = value.as_dict()
        if isinstance(value, dict):
            data = dict(value)
            data.setdefault("source_name", name)
            data.setdefault("required", required)
            return SourcePreflightResult(**data)
        return SourcePreflightResult(
            source_name=name,
            status="endpoint_failed",
            required=required,
            reason=f"preflight returned unsupported type "
                   f"{type(value).__name__}",
        )

    for name, entry in sorted(registry.items()):
        raw = entry.raw or {}
        required = bool(entry.required_for_baseline)
        base_report = {
            "name": name,
            "ownership_id": entry.ownership_id,
            "required": required,
            "credential_refs": list(entry.credential_refs),
            "credential_aliases": {
                k: list(v) for k, v in entry.credential_aliases.items()
            },
        }
        if entry.legacy or entry.is_registry_only:
            result = SourcePreflightResult.not_applicable(
                name, required=required,
                reason=(
                    "legacy or registry-only source has no runnable "
                    "preflight"
                ),
            )
            source_report = {**base_report, **result.as_dict()}
            report["sources"].append(source_report)
            continue

        module_path = raw.get("module")
        class_name = raw.get("class")
        try:
            module = importlib.import_module(module_path)
            cls = getattr(module, class_name)
        except (ImportError, AttributeError, ValueError) as e:
            result = SourcePreflightResult(
                source_name=name,
                status="endpoint_failed",
                required=required,
                credential_refs=tuple(entry.credential_refs),
                alias_refs={
                    k: tuple(v)
                    for k, v in entry.credential_aliases.items()
                },
                reason=redact_text(f"{type(e).__name__}: {e}"),
            )
            source_report = {**base_report, **result.as_dict()}
            report["sources"].append(source_report)
            if required:
                report["ok"] = False
                report["errors"].append({
                    "name": name,
                    "code": "source_auth_preflight_failed",
                    "status": result.status,
                })
            continue
        try:
            params = _doctor_adapter_init_params(
                entry, cls, dsn=dsn, deployment=deployment,
            )
            adapter = cls(**params)
        except Exception as e:  # noqa: BLE001
            result = SourcePreflightResult(
                source_name=name,
                status="endpoint_failed",
                required=required,
                credential_refs=tuple(entry.credential_refs),
                alias_refs={
                    k: tuple(v)
                    for k, v in entry.credential_aliases.items()
                },
                reason=redact_text(
                    f"construction raised {type(e).__name__}: {e}"
                ),
            )
        else:
            preflight = getattr(adapter, "preflight", None)
            if not callable(preflight):
                result = SourcePreflightResult.not_applicable(
                    name, required=required,
                )
            else:
                try:
                    try:
                        preflight_sig = inspect.signature(preflight)
                    except (TypeError, ValueError):
                        accepts_mode = True
                    else:
                        accepts_mode = (
                            "mode" in preflight_sig.parameters
                            or any(
                                p.kind == inspect.Parameter.VAR_KEYWORD
                                for p in preflight_sig.parameters.values()
                            )
                        )
                    if accepts_mode:
                        raw_result = preflight(mode="live")
                    else:
                        raw_result = preflight()
                    result = _coerce_result(name, required, raw_result)
                except Exception as e:  # noqa: BLE001
                    result = SourcePreflightResult(
                        source_name=name,
                        status="endpoint_failed",
                        required=required,
                        credential_refs=tuple(entry.credential_refs),
                        alias_refs={
                            k: tuple(v)
                            for k, v in entry.credential_aliases.items()
                        },
                        reason=redact_text(
                            f"preflight raised {type(e).__name__}: {e}"
                        ),
                    )
        if (
            result.required != required
            or result.source_name != name
            or (not result.credential_refs and entry.credential_refs)
        ):
            result_data = result.as_dict()
            result_data["source_name"] = name
            result_data["required"] = required
            if not result.credential_refs and entry.credential_refs:
                result_data["credential_refs"] = tuple(
                    entry.credential_refs
                )
                result_data["alias_refs"] = {
                    k: tuple(v)
                    for k, v in entry.credential_aliases.items()
                }
            result = SourcePreflightResult(
                **result_data
            )
        source_report = {**base_report, **result.as_dict()}
        report["sources"].append(source_report)
        if required and result.status in failure_statuses:
            report["ok"] = False
            report["errors"].append({
                "name": name,
                "code": "source_auth_preflight_failed",
                "status": result.status,
            })

    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


def _doctor_listener_exposure(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check listener-exposure` — every substrate-owned
    TCP listener is loopback-bound or explicitly auth-gated
    (OKG_AUTH_GATED_PORTS). A non-loopback unauthenticated bind is a
    FAILING check — the 2026-06 audit's exposed-control-plane failure
    mode (per harden-connector-route-vetting).

    Watched DB ports (the deployment's resolved Postgres port +
    --watched-db-ports/OKG_WATCHED_DB_PORTS) are evaluated regardless
    of owning process and fail unconditionally when non-loopback —
    the graph store holds decrypted mail/calendar data and is
    published by Docker's proxy under a foreign process name (per
    harden-graph-store-exposure)."""
    from okg.substrate.cli.connector_security import (
        evaluate_listeners,
        scan_substrate_listeners,
    )

    from okg.substrate.cli.connector_security import (
        WATCHED_DB_PORTS_ENV,
        _ports_from_env,
    )
    watched: set[int] = _ports_from_env(WATCHED_DB_PORTS_ENV)
    for raw in (getattr(args, "watched_db_ports", None) or "").split(","):
        if raw.strip().isdigit():
            watched.add(int(raw.strip()))
    # Add the deployment's resolved DB port when a DSN is reachable.
    port = _deployment_db_port(args)
    if port is not None:
        watched.add(port)

    try:
        listeners, scanner = scan_substrate_listeners(watched_db_ports=watched)
    except RuntimeError as exc:
        return _emit_error(SubstrateError(str(exc)), as_json=as_json)
    report = evaluate_listeners(listeners)
    report["check"] = "listener-exposure"
    report["auth_gated_ports_env"] = "OKG_AUTH_GATED_PORTS"
    report["watched_db_ports"] = sorted(watched)
    # Which tool answered. An operator reading a clean report on a host without
    # lsof should be able to see that the fallback produced it.
    report["scanner"] = scanner
    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


def _doctor_egress_coverage(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check egress-validator-coverage` — every substrate file
    that opens an outbound HTTP connection (urllib/requests/httpx) must also
    route through the central egress validator (`_url_safety`). Tree-scans
    `src/okg/substrate`; an unguarded sink with no documented exception FAILS,
    so a new raw SSRF sink cannot land silently (per
    centralize-egress-url-validation)."""
    from okg.substrate.cli.egress_check import scan_egress_coverage

    report = scan_egress_coverage()
    report["check"] = "egress-validator-coverage"
    report["errors"] = [
        {
            "code": "unguarded_egress_sink",
            "file": violation,
            "fix_hint": (
                "route the outbound request through open_validated_url / "
                "assert_public_http_url, or add a documented exception to "
                "egress_check.EGRESS_COVERAGE_EXCEPTIONS with a written reason"
            ),
        }
        for violation in report["violations"]
    ]
    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


def _deployment_db_port(args: argparse.Namespace) -> Optional[int]:
    """Best-effort: the Postgres host port from the resolved DSN, so
    the deployment's graph store is auto-watched. Returns None when
    no DSN is configured (the check still honors explicit ports)."""
    try:
        dsn = _resolve_dsn(args)
    except SubstrateError:
        return None
    import re
    match = re.search(r":(\d+)/", dsn or "")
    return int(match.group(1)) if match else None


def _doctor_source_health(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check source-health` — one row per academic-researcher
    library source: import health, registration completeness, test
    coverage on disk, preflight, and live-tier env gates (per
    source-health-matrix). Import failures and registration drift
    fail the check; coverage holes and absent credentials are
    reported facts."""
    from okg.substrate.cli.source_health import (
        assemble_source_health,
        summarize,
    )

    rows = assemble_source_health()
    report = {
        "check": "source-health",
        **summarize(rows),
        "rows": [row.to_dict() for row in rows],
        "matrix_runbook": "docs/runbooks/quality/source-test-matrix.md",
    }
    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


def _doctor_token_redaction(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check token-redaction --deployment <name>` —
    probe the credentialed-source read surfaces (oauth preflight
    results, discovery connector routes, and — when a DSN is
    reachable — connector_route_status) for token material from the
    keyring store (per harden-connector-route-vetting)."""
    import json as _json

    from okg.substrate.cli.connector_security import find_token_leaks
    from okg.substrate.library.oauth.preflight import oauth_preflight
    from okg.substrate.library.oauth.providers import REGISTRY
    from okg.substrate.library.oauth.token_store import (
        KeyringUnavailable,
        TokenStore,
    )

    deployment = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg doctor --check token-redaction requires "
                "--deployment <name> or OKG_DEPLOYMENT",
            ),
            as_json=as_json,
        )

    tokens: list[str] = []
    keyring_note = None
    for provider in REGISTRY.values():
        try:
            stored = TokenStore().get(
                deployment=deployment, provider=provider.name,
            )
        except KeyringUnavailable as exc:
            keyring_note = f"keyring unavailable: {exc}"
            stored = None
        if stored:
            tokens.extend(
                value
                for key, value in stored.items()
                if isinstance(value, str)
                and key in ("access_token", "refresh_token")
                and value
            )

    payloads: list[tuple[str, str]] = []
    for provider in REGISTRY.values():
        result = oauth_preflight(
            source_name=provider.name,
            deployment_name=deployment,
            provider=provider,
        )
        payloads.append((
            f"preflight:{provider.name}",
            _json.dumps(result.as_dict(), default=str),
        ))

    db_probes = "skipped (no DSN)"
    try:
        dsn = _resolve_dsn(args)
    except SubstrateError:
        dsn = None
    if dsn:
        try:
            from okg.substrate.db.connection import connect as db_connect

            from okg.substrate.mcp.connector_ingest import (
                connector_route_status,
            )
            from okg.substrate.mcp.source_catalog import (
                _CREDENTIAL_SOURCE_IDS,
                SOURCE_CATALOG,
            )
            credentialed_shorts = sorted(
                short
                for short, (sid, _kind) in SOURCE_CATALOG.items()
                if sid in _CREDENTIAL_SOURCE_IDS
            )
            with db_connect(dsn) as conn:
                for short in credentialed_shorts:
                    status = connector_route_status(
                        conn, deployment, short,
                    )
                    payloads.append((
                        f"connector_route_status:{short}",
                        _json.dumps(status, default=str),
                    ))
            db_probes = f"probed {len(credentialed_shorts)} sources"
        except Exception as exc:  # noqa: BLE001 — degraded probe coverage is reported, never silent
            db_probes = f"skipped (status probe failed: {exc})"

    leaks = find_token_leaks(payloads, tokens)
    report = {
        "check": "token-redaction",
        "deployment": deployment,
        "ok": not leaks,
        "probed_tokens": len(tokens),
        "probed_payloads": [label for label, _ in payloads],
        "db_probes": db_probes,
        "leaks": leaks,
    }
    if keyring_note:
        report["keyring"] = keyring_note
    if not tokens:
        report["note"] = (
            "no tokens in keyring for this deployment — probe had "
            "nothing to detect; provision OAuth and re-run for a "
            "meaningful pass"
        )
    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


def _doctor_external_live_authority(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check external-live-authority` — validate that
    deployment-declared external-live tools have authority profiles.
    """
    _load_main_helpers()
    deployment = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg doctor --check external-live-authority requires "
                "--deployment <name> or OKG_DEPLOYMENT",
            ),
            as_json=as_json,
        )

    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.library.sources.external_authority import (
        validate_external_live_authority_profiles,
    )

    deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None:
        return _emit_error(
            SubstrateError(
                f"deployment dir for {deployment!r} not found",
                fix_hint=(
                    "check the deployment exists under deployments/<name>/"
                ),
            ),
            as_json=as_json,
        )

    report = validate_external_live_authority_profiles(
        deployment_dir / "deployment.yaml",
        deployment_dir / "live_authority_profiles.yaml",
    )
    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


def _doctor_nomos(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check nomos` — validate Nomos schema and IAM rows."""
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.nomos.doctor import nomos_doctor_report

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    deployment_name = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
    )
    try:
        with db_connect(dsn) as conn:
            report = nomos_doctor_report(
                conn,
                deployment_name=deployment_name,
            )
    except Exception as err:  # noqa: BLE001
        return _emit_error(
            SubstrateError(
                f"nomos doctor failed: {type(err).__name__}: {err}",
                fix_hint=(
                    "verify the DSN points at a migrated AKMON database "
                    "and run `okg migrate` if Nomos tables are missing"
                ),
            ),
            as_json=as_json,
        )
    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


def _doctor_lens_promotions(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """Warn on committed lens overlays that are not promotion-ready."""
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.lenses.promote import lens_producer_id

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    deployment = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
        or "default"
    )
    stale_after_days = int(getattr(args, "stale_after_days", 30) or 30)
    warnings: list[dict[str, object]] = []

    try:
        with db_connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  r.request_id::text,
                  r.lens_definition_id,
                  r.committed_at,
                  d.lens_promotion_mapping,
                  (
                    r.committed_at IS NOT NULL
                    AND r.committed_at < (
                      now() - make_interval(days => %(stale_after_days)s)
                    )
                  ) AS is_stale
                FROM okg.lens_putback_requests r
                JOIN okg.lens_definitions d
                  ON d.definition_id = r.lens_definition_id
                WHERE r.deployment_name = %(deployment)s
                  AND r.status = 'committed_overlay'
                ORDER BY r.committed_at NULLS LAST, r.request_id
                """,
                {
                    "deployment": deployment,
                    "stale_after_days": stale_after_days,
                },
            )
            rows = cur.fetchall()
            producer_ids = [
                lens_producer_id(str(row[1]))
                for row in rows
            ]
            active_profiles: set[str] = set()
            if producer_ids:
                cur.execute(
                    """
                    SELECT DISTINCT producer_id
                      FROM okg.producer_profiles
                     WHERE deployment_name = %(deployment)s
                       AND status = 'active'
                       AND producer_id = ANY(%(producer_ids)s)
                    """,
                    {
                        "deployment": deployment,
                        "producer_ids": producer_ids,
                    },
                )
                active_profiles = {str(row[0]) for row in cur.fetchall()}
    except Exception as err:  # noqa: BLE001
        return _emit_error(
            SubstrateError(
                f"lens promotion doctor failed: {type(err).__name__}: {err}",
                fix_hint="verify the DSN points at a migrated AKMON database",
            ),
            as_json=as_json,
        )

    stale_count = 0
    missing_profile_count = 0
    missing_mapping_count = 0
    for row in rows:
        request_id, definition_id, committed_at, mapping, is_stale = row
        producer_id = lens_producer_id(str(definition_id))
        if is_stale:
            stale_count += 1
            warnings.append({
                "code": "committed_overlay_stale",
                "request_id": request_id,
                "lens_definition_id": definition_id,
                "committed_at": committed_at.isoformat()
                if committed_at is not None else None,
                "stale_after_days": stale_after_days,
            })
        if producer_id not in active_profiles:
            missing_profile_count += 1
            warnings.append({
                "code": "producer_profile_missing",
                "request_id": request_id,
                "lens_definition_id": definition_id,
                "producer_id": producer_id,
            })
        if mapping is None:
            missing_mapping_count += 1
            warnings.append({
                "code": "lens_promotion_mapping_missing",
                "request_id": request_id,
                "lens_definition_id": definition_id,
            })

    payload = {
        "ok": True,
        "deployment": deployment,
        "committed_overlay_count": len(rows),
        "stale_count": stale_count,
        "missing_producer_profile_count": missing_profile_count,
        "missing_promotion_mapping_count": missing_mapping_count,
        "warnings": warnings,
    }
    _emit_ok(payload, as_json=as_json)
    return 0


def _doctor_deployment_identity(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """Warn on ownership rows lacking a manifest fingerprint (#709 R4).

    A NULL ``manifest_fingerprint`` is a positively-established "claim
    predates manifest-identity binding" state: publish still proceeds but
    with an UNVERIFIED manifest and a loud warning. This check surfaces
    those rows so the fleet can be converged with the one-command backfill
    before NULL is later tightened to a refusal (a sanctioned follow-up).
    """
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.catalog.ownership import (
        MANIFEST_BACKFILL_COMMAND,
        ensure_catalog_ownership_schema,
    )

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    warnings: list[dict[str, object]] = []
    try:
        with db_connect(dsn, autocommit=True) as conn:
            ensure_catalog_ownership_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT deployment_name, claimed_at, claimed_by_command
                      FROM okg.deployment_write_ownership
                     WHERE status = 'active'
                       AND manifest_fingerprint IS NULL
                     ORDER BY deployment_name
                    """
                )
                rows = cur.fetchall()
    except Exception as err:  # noqa: BLE001
        return _emit_error(
            SubstrateError(
                f"deployment-identity doctor failed: "
                f"{type(err).__name__}: {err}",
                fix_hint="verify the DSN points at a migrated OKG database",
            ),
            as_json=as_json,
        )

    for name, claimed_at, claimed_by in rows:
        warnings.append({
            "code": "manifest_fingerprint_missing",
            "deployment_name": name,
            "claimed_at": claimed_at.isoformat() if claimed_at else None,
            "claimed_by_command": claimed_by,
            "backfill_command": MANIFEST_BACKFILL_COMMAND.format(name=name),
        })

    payload = {
        "ok": True,
        "unbound_ownership_rows": len(rows),
        "warnings": warnings,
    }
    _emit_ok(payload, as_json=as_json)
    return 0


def _doctor_integrity_coverage(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """Report consistency-check coverage across the integrity taxonomy."""
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.consistency.observations import CHECK_KINDS

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    deployment = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
        or "default"
    )
    kinds = {kind: 0 for kind in sorted(CHECK_KINDS)}
    warnings: list[dict[str, object]] = []

    try:
        with db_connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT check_kind, count(*)::bigint
                  FROM okg.consistency_check_definitions
                 WHERE deployment_name = %(deployment)s
                 GROUP BY check_kind
                """,
                {"deployment": deployment},
            )
            unclassified_count = 0
            for check_kind, count in cur.fetchall():
                if check_kind is None:
                    unclassified_count = int(count)
                else:
                    kinds[str(check_kind)] = int(count)

            cur.execute(
                """
                SELECT count(*)::bigint
                  FROM okg.producer_profiles
                 WHERE deployment_name = %(deployment)s
                   AND producer_kind = 'source'
                   AND status = 'active'
                """,
                {"deployment": deployment},
            )
            active_source_profiles = int(cur.fetchone()[0])
    except Exception as err:  # noqa: BLE001
        return _emit_error(
            SubstrateError(
                f"integrity coverage doctor failed: "
                f"{type(err).__name__}: {err}",
                fix_hint="verify the DSN points at a migrated AKMON database",
            ),
            as_json=as_json,
        )

    if unclassified_count:
        warnings.append({
            "code": "consistency_checks_unclassified",
            "count": unclassified_count,
        })

    rule_based_implicit = active_source_profiles > 0
    checks = [
        {
            "name": f"{kind}_coverage",
            "kind": kind,
            "ok": bool(kinds[kind] > 0 or (
                kind == "rule_based" and rule_based_implicit
            )),
            "registered_checks": kinds[kind],
            "active_source_profiles": active_source_profiles
            if kind == "rule_based" else None,
        }
        for kind in sorted(CHECK_KINDS)
    ]
    payload = {
        "ok": True,
        "deployment": deployment,
        "kinds": kinds,
        "unclassified_count": unclassified_count,
        "rule_based_implicit_via_producer_authority": rule_based_implicit,
        "checks": checks,
        "warnings": warnings,
    }
    _emit_ok(payload, as_json=as_json)
    return 0


def _doctor_integrity(args: argparse.Namespace, *, as_json: bool) -> int:
    """Run legacy generation-integrity diagnostics for one generation."""
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    deployment = getattr(args, "deployment", None)
    requested_generation = getattr(args, "generation", None)
    payload: dict[str, Any] = {
        "ok": True,
        "deployment_name": deployment,
        "generation_id": requested_generation,
        "catalog_version_id": None,
        "checks": [],
        "global_dirty_counts": {},
        "errors": [],
    }

    try:
        with db_connect(dsn, autocommit=True) as conn, \
             conn.cursor() as cur:
            if requested_generation is None:
                if deployment:
                    cur.execute(
                        """
                        SELECT generation_id, catalog_version_id,
                               deployment_name
                          FROM okg.graph_generations
                         WHERE status = 'published'
                           AND deployment_name = %s
                         ORDER BY generation_id DESC
                         LIMIT 1
                        """,
                        (deployment,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT generation_id, catalog_version_id,
                               deployment_name
                          FROM okg.graph_generations
                         WHERE status = 'published'
                         ORDER BY generation_id DESC
                         LIMIT 1
                        """
                    )
            else:
                cur.execute(
                    """
                    SELECT generation_id, catalog_version_id,
                           deployment_name
                      FROM okg.graph_generations
                     WHERE generation_id = %s
                    """,
                    (requested_generation,),
                )
            row = cur.fetchone()
            if row is None:
                raise SubstrateError(
                    "no generation found for integrity check",
                    fix_hint=(
                        "publish a generation first or pass "
                        "--generation <id>"
                    ),
                    data={
                        "deployment_name": deployment,
                        "generation_id": requested_generation,
                    },
                )
            generation_id = int(row[0])
            catalog_version_id = int(row[1])
            payload["generation_id"] = generation_id
            payload["catalog_version_id"] = catalog_version_id
            payload["deployment_name"] = row[2]
            check_names = [
                "substrate_invalid_node_intervals",
                "substrate_invalid_edge_intervals",
                "substrate_duplicate_visible_nodes",
                "substrate_duplicate_visible_edges",
            ]
            payload["checks"] = [
                {
                    "name": name,
                    "severity": "error",
                    "passed": True,
                    "count": 0,
                    "sample": [],
                    "metadata": {
                        "compatibility_check": True,
                        "reason": (
                            "legacy integrity module retired; "
                            "generation existence verified"
                        ),
                    },
                }
                for name in check_names
            ]
            payload["global_dirty_counts"] = {}
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    except Exception as exc:
        payload["ok"] = False
        payload["errors"].append({
            "code": "integrity_probe_failed",
            "message": str(exc),
        })

    _emit_ok(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


def _doctor_probe_feature_absent(exc: Exception) -> str | None:
    """If ``exc`` means a probe's feature simply isn't provisioned on this
    deployment (a missing role or relation), return a short skip reason; else
    ``None``.

    Lets feature-specific probes skip gracefully on deployments that don't load
    that feature, instead of failing the whole doctor. okg-ci's hand-built DB
    lacks the ``okg_dbos`` role-split and the escalation feature, so the
    provenance-split audit (``has_table_privilege('okg_dbos', ...)``) and the
    escalation-backlog probe (``okg.escalation_live``) would otherwise report a
    spurious failure. (The prior ``runtime.enabled`` guard wrongly assumed
    runtime-enabled implies the role-split was applied.)
    """
    import psycopg

    if isinstance(exc, psycopg.errors.UndefinedObject):
        return "required role not provisioned (feature not present on this deployment)"
    if isinstance(exc, psycopg.errors.UndefinedTable):
        return "required relation not present (feature not loaded on this deployment)"
    sqlstate = getattr(exc, "sqlstate", None)
    if sqlstate == "42704":  # undefined_object (e.g. role does not exist)
        return "required role not provisioned (feature not present on this deployment)"
    if sqlstate == "42P01":  # undefined_table (relation does not exist)
        return "required relation not present (feature not loaded on this deployment)"
    return None


def _load_ready_doctor_dsn(args: argparse.Namespace) -> None:
    path_value = os.environ.pop("OKG_READY_DOCTOR_DSN_FILE", None)
    if not path_value or getattr(args, "dsn", None):
        return
    path = Path(path_value)
    info = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(info.st_mode):
        raise SubstrateError("readiness doctor DSN file is not a regular file")
    if stat.S_IMODE(info.st_mode) != 0o600:
        raise SubstrateError("readiness doctor DSN file mode must be 0600")
    if hasattr(os, "getuid") and info.st_uid != os.getuid():
        raise SubstrateError("readiness doctor DSN file has a foreign owner")
    if info.st_size > 16 * 1024:
        raise SubstrateError("readiness doctor DSN file is unexpectedly large")
    dsn = path.read_text(encoding="utf-8").strip()
    if not dsn:
        raise SubstrateError("readiness doctor DSN file is empty")
    args.dsn = dsn


#: Installed extension versions the doctor warns about (issue #3480, decision
#: doctorpgts A, 2026-10-07). pg_textsearch before 1.5.1 can read stale
#: keyword-search index state after a database is dropped and re-cloned
#: (#2436). A warning, not an error: search still answers, and the move is
#: one-way, so the operator schedules it.
_RECOMMENDED_EXTENSION_VERSIONS: dict[str, dict[str, str]] = {
    "pg_textsearch": {
        "recommended": "1.5.1",
        "problem": (
            "can read stale keyword-search index state after a database is "
            "dropped and re-cloned, so keyword search repeats some "
            "results and drops others"
        ),
        "fix": (
            "move the database server to the 0.2.0 okg PostgreSQL image, "
            "back up the database (the update is one-way), then run "
            "`ALTER EXTENSION pg_textsearch UPDATE;` in each database"
        ),
        "docs": "docs/operate/lifecycle.md#move-search-to-pg_textsearch-151",
    },
}


def _version_key(version: str) -> tuple[int, ...] | None:
    """The leading dotted number of ``version`` as integers, or None.

    ``1.10`` sorts above ``1.5``; a suffix such as ``-dev`` is ignored, so
    ``1.5.1-dev`` reads as ``1.5.1``. A string with no leading number
    returns None and the caller says it could not compare.
    """
    match = re.match(r"\s*v?(\d+(?:\.\d+)*)", version)
    if match is None:
        return None
    return tuple(int(part) for part in match.group(1).split("."))


def _extension_version_warnings(
    installed: dict[str, Any],
) -> list[dict[str, object]]:
    """Warnings for installed extensions older than the recommended version.

    ``installed`` maps extension name to its ``extversion``. A missing
    extension is not judged here: the existing ``extension_missing`` error
    covers it. A version the doctor cannot read is its own warning, never a
    silent pass.
    """
    warnings: list[dict[str, object]] = []
    for name, rule in _RECOMMENDED_EXTENSION_VERSIONS.items():
        version = installed.get(name)
        if version is None:
            continue
        recommended = rule["recommended"]
        have = _version_key(str(version))
        if have is None:
            warnings.append({
                "code": "extension_version_unrecognized",
                "extension": name,
                "installed_version": str(version),
                "recommended_version": recommended,
                "message": (
                    f"{name} reports version {version!r}, which okg doctor "
                    f"cannot compare with {recommended}; check it by hand"
                ),
                "docs": rule["docs"],
            })
            continue
        want = _version_key(recommended)
        if want is not None and have < want:
            warnings.append({
                "code": "extension_outdated",
                "extension": name,
                "installed_version": str(version),
                "recommended_version": recommended,
                "message": f"{name} {version} {rule['problem']}",
                "fix_hint": f"{rule['fix']}; see {rule['docs']}",
                "docs": rule["docs"],
            })
    return warnings


def _record_workflow_health(report: dict, key: str, health) -> None:
    """Put one workflow-liveness verdict into the doctor report.

    A verdict that is not ok is an error and fails the doctor. A
    deliberately paused publisher is ok and listed as a warning: the
    operator sees the pause, instead of a "stale" failure, and is not left
    to forget it.
    """
    from okg.substrate.runtime.publisher_health import PAUSED_WARNING_CODES

    report[key] = health.to_dict()
    entry = {"code": health.code, "message": health.message}
    if not health.ok:
        report["ok"] = False
        report["errors"].append(entry)
    elif health.code in PAUSED_WARNING_CODES:
        report.setdefault("warnings", []).append(entry)


def _cmd_doctor(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor` — extension check, role check, DSN sanity.

    Optional `--check` modes:
      - `extraction-wrap-invariant`: lint the extraction library
        tree for the one-third-party-package-per-backend rule.
      - `extraction`: import-check every extraction lib + report
        spaCy / GLiNER models. With `--warm`, eagerly preload
        models the deployment configures.
      - `extractors`: pre-instantiate parser/chunker/NER classes
        from the deployment's `extractors.yaml`.
      - `sources`: pre-instantiate source adapter classes from the
        deployment's `source_registry.yaml`.
      - `integrity`: run substrate generation-integrity diagnostics.
      - `acceptance-criteria`: report OpenSpec criteria without
        deterministic test evidence links.
      - `source-auth`: run optional source auth/cache preflight
        hooks from the deployment's `source_registry.yaml`.
      - `agent-skills`: compare installed repo-local agent skills against
        the bundled canon.
      - `mcp-surface-freshness`: probe registered OKG MCP servers for
        running-process code/instruction freshness.
      - `external-live-authority`: validate external-live tool
        authority profile coverage.
      - `nomos`: validate Nomos policy, identity schema, and IAM
        drift.
      - `database`: inspect database storage, runtime backlog, and
        migration drift.
      - deployment-provided checks declared in deployment.yaml.
    """
    _load_main_helpers()
    try:
        _load_ready_doctor_dsn(args)
    except (OSError, SubstrateError) as exc:
        error = (
            exc
            if isinstance(exc, SubstrateError)
            else SubstrateError("readiness doctor DSN file is unreadable")
        )
        return _emit_error(error, as_json=as_json)
    check = getattr(args, "check", None)

    if check:
        from okg.substrate.doctor import DoctorCheckLoadError, DoctorContext

        try:
            registry = _build_doctor_registry(args)
        except DoctorCheckLoadError as err:
            return _emit_error(err, as_json=as_json)
        doctor_context = DoctorContext(
            resolve_dsn=_resolve_dsn,
            emit_ok=_emit_ok,
        )
        if registry.get(check) is None:
            payload = {
                "ok": False,
                "status": "ERROR",
                "errors": [
                    {
                        "code": "unknown_doctor_check",
                        "message": f"unknown doctor check: {check}",
                        "available_checks": registry.names(),
                    }
                ],
            }
            _emit_ok(payload, as_json=as_json)
            return 1
        return registry.dispatch(
            check,
            args,
            context=doctor_context,
            as_json=as_json,
        )

    # Default: substrate DB sanity.
    from okg.substrate.db.migrate import (
        OPTIONAL_EXTENSIONS,
        REQUIRED_EXTENSIONS,
    )
    from okg.substrate.db.connection import connect as db_connect

    deployment_name = getattr(args, "deployment", None)
    manifest = None
    if deployment_name:
        from okg.substrate.cli.deployment import (
            manifest_load_errors,
            manifest_refusal,
        )
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            _looks_like_manifest_path,
            resolve_deployment,
        )

        try:
            manifest = resolve_deployment(deployment_name)
        except DeploymentResolutionError as exc:
            # A manifest PATH that does not exist is a typo, not a
            # deployment known only by name to the DSN resolver below: refuse
            # it instead of checking whatever database --dsn or the
            # environment names (#2713). A bare name keeps the old behavior.
            if _looks_like_manifest_path(deployment_name):
                return _emit_error(SubstrateError(str(exc)), as_json=as_json)
            manifest = None
        except manifest_load_errors() as exc:
            # The manifest exists and the loader refused it, or it is not
            # valid YAML, or it cannot be read: fail closed with a one-line
            # reason (#2713). This runs before the DSN is read, so a
            # misspelled `postgres` key is reported as itself rather than as
            # a missing `postgres.dsn`. The catch covers only this load; a
            # ValueError or OSError elsewhere still surfaces.
            return _emit_error(
                manifest_refusal(exc, deployment_name), as_json=as_json,
            )

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(
            err,
            as_json=as_json,
        )

    report: dict = {"ok": True, "extensions": {}, "roles": {}, "errors": []}
    try:
        with db_connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                report["postgres_version"] = cur.fetchone()[0].split(",")[0]
                # Probe REQUIRED + OPTIONAL extensions together so
                # the doctor's report includes the optional ones'
                # version when present.
                _all_extensions = list(REQUIRED_EXTENSIONS.keys()) + list(
                    OPTIONAL_EXTENSIONS.keys()
                )
                cur.execute(
                    "SELECT extname, extversion FROM pg_extension "
                    "WHERE extname = ANY(%s);",
                    (_all_extensions,),
                )
                installed = dict(cur.fetchall())
                for name in _all_extensions:
                    report["extensions"][name] = installed.get(name)
                    is_required = name in REQUIRED_EXTENSIONS
                    if name not in installed:
                        if not is_required:
                            # Optional + missing = informational.
                            # The doctor's `extensions` dict shows
                            # the None status; no warning/error.
                            continue
                        report["ok"] = False
                        report["errors"].append(
                            {
                                "code": "extension_missing",
                                "extension": name,
                            }
                        )
                # Installed but older than recommended: a warning only,
                # `ok` is unchanged (#3480).
                for warning in _extension_version_warnings(installed):
                    report.setdefault("warnings", []).append(warning)
                # Roles.
                for role in ("app_rw", "app_ro", "catalog_admin"):
                    cur.execute(
                        "SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)
                    )
                    report["roles"][role] = cur.fetchone() is not None
                    if not report["roles"][role]:
                        report["ok"] = False
                        report["errors"].append(
                            {"code": "role_missing", "role": role}
                        )
                # role_has_superuser (docs/reference/security.md §okg doctor secret
                # checks, item 4; PACT least-privilege-mcp). The substrate
                # runtime roles must NOT be Postgres superusers — a runtime
                # role that is `postgres`/superuser defeats the privilege
                # boundary (DROP/ALTER/cross-db/RLS-bypass). okg_mcp is the
                # MCP read pool's role and the most security-relevant: it is
                # the agent's whole graph surface.
                report["role_superuser"] = {}
                cur.execute(
                    "SELECT rolname, rolsuper FROM pg_roles "
                    "WHERE rolname = ANY(%s)",
                    (["okg_mcp", "app_rw", "app_ro", "catalog_admin"],),
                )
                for rolname, rolsuper in cur.fetchall():
                    report["role_superuser"][rolname] = bool(rolsuper)
                    if rolsuper:
                        report["ok"] = False
                        report["errors"].append(
                            {
                                "code": "role_has_superuser",
                                "role": rolname,
                                "fix_hint": (
                                    f"ALTER ROLE {rolname} NOSUPERUSER; the "
                                    "MCP/runtime roles must not be Postgres "
                                    "superusers (a superuser runtime role "
                                    "bypasses the least-privilege boundary)"
                                ),
                            }
                        )
    except Exception as e:
        report["ok"] = False
        report["errors"].append(
            {"code": "doctor_probe_failed", "message": str(e)}
        )

    # Built-in role passwords (issue #3273). Earlier releases gave these roles
    # fixed passwords committed in source; a role that still accepts one can be
    # logged into by any account that reaches the server. A real login with
    # the built-in password decides it. A warning, not an error: the database
    # works, and the rotation command fixes it in place.
    if not any(e.get("code") == "doctor_probe_failed" for e in report["errors"]):
        from okg.substrate.local_database_roles import (
            builtin_password_findings,
            builtin_role_passwords_allowed,
            rotate_command,
        )

        try:
            findings = builtin_password_findings(dsn)
        except Exception as e:  # noqa: BLE001
            report.setdefault("warnings", []).append(
                {"code": "role_password_probe_failed", "message": str(e)}
            )
        else:
            allowed = builtin_role_passwords_allowed()
            report["role_passwords"] = {
                "builtin_password_roles": sorted(
                    str(item["role"]) for item in findings
                ),
                "builtin_passwords_allowed": allowed,
            }
            if findings and not allowed:
                for item in findings:
                    report.setdefault("warnings", []).append(
                        {**item, "fix_hint": (
                            item["fix_hint"]
                            if item["code"] != "builtin_role_password"
                            else f"run `{rotate_command(deployment_name)}`"
                        )}
                    )

    # Provenance-split audit (Phase 1 §2.5). Runs only when the
    # deployment has runtime.enabled=true; otherwise the role split
    # has not been applied and the audit would falsely fail.
    if deployment_name:
        if manifest is not None and manifest.runtime.enabled:
            from okg.substrate.runtime.audit import (
                audit_provenance_split,
            )
            try:
                verdict = audit_provenance_split(
                    deployment=deployment_name, dsn=dsn,
                )
                report["provenance_split"] = verdict.to_dict()
                if not verdict.ok:
                    report["ok"] = False
                    for v in verdict.violations:
                        report["errors"].append(
                            {
                                "code": "provenance_split_violation",
                                "message": v,
                            }
                        )
            except Exception as e:
                skip = _doctor_probe_feature_absent(e)
                if skip is not None:
                    report["provenance_split"] = {
                        "ok": True,
                        "skipped": True,
                        "code": "provenance_split_audit_skipped",
                        "reason": skip,
                    }
                else:
                    report["ok"] = False
                    report["errors"].append(
                        {
                            "code": "provenance_split_audit_failed",
                            "message": str(e),
                        }
                    )

            # Publisher-workflow liveness check (migrate-substrate-
            # runtime-to-dbos Phase 1 §1.4). When the deployment has
            # runtime.enabled=true, the publisher_cycle_workflow is
            # the production publish path; doctor must report whether
            # it has completed a cycle within a reasonable recent
            # window. Replaces the legacy publisher_lease_held_too_long
            # check, which is no longer meaningful in DBOS mode.
            from okg.substrate.runtime.publisher_health import (
                check_deployment_doctor_health,
                check_embed_workflow_health,
                check_escalation_open_backlog,
                check_escalation_sweep_health,
                check_generation_event_subscribers,
                check_internal_queue_unconsumed,
                check_lens_putback_backlog,
                check_lens_putback_sweep_health,
                check_manifest_dsn_resolvable,
                check_publisher_workflow_health,
                check_queue_depth,
                check_runtime_services_health,
                check_source_sync_workflow_health,
            )
            from okg.substrate.runtime.services import (
                should_register_embed,
                should_register_escalation_sweep,
                should_register_lens_putback,
            )
            schedule_overrides = dict(manifest.runtime.schedules)
            embed_expected = should_register_embed(manifest)
            lens_putback_expected = should_register_lens_putback(dsn=dsn)
            escalation_sweep_expected = should_register_escalation_sweep(
                dsn=dsn,
            )
            # Workflow-liveness probes, one per substrate-owned
            # scheduled workflow. Each probe is structurally
            # identical: read the most-recent dbos.workflow_status
            # row, compare its SUCCESS timestamp against a threshold
            # derived from the workflow's cron.
            probes = [
                (
                    "publisher_workflow_health",
                    check_publisher_workflow_health,
                    {
                        "yield_bound_seconds": (
                            manifest.publisher.max_source_sync_yield_seconds
                        ),
                    },
                ),
                (
                    "source_sync_workflow_health",
                    check_source_sync_workflow_health,
                    {},
                ),
                (
                    "embed_workflow_health",
                    check_embed_workflow_health,
                    {"expected_to_run": embed_expected},
                ),
                (
                    "lens_putback_sweep_health",
                    check_lens_putback_sweep_health,
                    {"expected_to_run": lens_putback_expected},
                ),
                (
                    "escalation_sweep_health",
                    check_escalation_sweep_health,
                    {"expected_to_run": escalation_sweep_expected},
                ),
            ]
            for key, probe_fn, extra_kwargs in probes:
                try:
                    health = probe_fn(
                        deployment=deployment_name,
                        dsn=dsn,
                        schedule_overrides=schedule_overrides,
                        **extra_kwargs,
                    )
                    _record_workflow_health(report, key, health)
                except Exception as e:  # noqa: BLE001
                    report[key] = {
                        "ok": False,
                        "code": f"{key}_probe_failed",
                        "message": str(e),
                    }
                    report["ok"] = False
                    report["errors"].append(
                        {
                            "code": f"{key}_probe_failed",
                            "message": str(e),
                        }
                    )

            # Queue-depth probe. Decoupled from the workflow-health
            # probes because it's structurally different: queue depth
            # is "right now" state, not "recently completed". Catches
            # the cron-faster-than-cycle-time runaway condition.
            try:
                qd = check_queue_depth(
                    deployment=deployment_name,
                    dsn=dsn,
                )
                report["queue_depth"] = qd.to_dict()
                if not qd.ok:
                    report["ok"] = False
                    report["errors"].append(
                        {"code": qd.code, "message": qd.message}
                    )
            except Exception as e:  # noqa: BLE001
                report["queue_depth"] = {
                    "ok": False,
                    "code": "queue_depth_probe_failed",
                    "message": str(e),
                }
                report["ok"] = False
                report["errors"].append(
                    {
                        "code": "queue_depth_probe_failed",
                        "message": str(e),
                    }
                )

            # PACT publisher-starvation-4039: source-sync rows waiting or
            # running past the publisher's yield bound. A warning; fatal when
            # the branch has no published generation, where the publisher
            # yields without bound. A failed probe is an error, never green.
            from okg.substrate.runtime.publisher_health_workflow_liveness import (
                check_source_sync_stuck,
            )

            stuck = check_source_sync_stuck(
                deployment=deployment_name,
                dsn=dsn,
                bound_seconds=manifest.publisher.max_source_sync_yield_seconds,
            )
            report["source_sync_stuck"] = stuck.to_dict()
            stuck_entry = {
                "code": stuck.code,
                "message": stuck.message,
                "check": "source_sync_stuck",
            }
            if not stuck.ok:
                report["ok"] = False
                report["errors"].append(stuck_entry)
            elif stuck.severity == "warning":
                report.setdefault("warnings", []).append(stuck_entry)

            # Lens-putback backlog probe (deepen-dbos-integration
            # Phase 0). Three-zone reporting: healthy / warn / runaway.
            # Warn lands in `warnings`; runaway lands in `errors`.
            try:
                backlog = check_lens_putback_backlog(
                    deployment=deployment_name,
                    dsn=dsn,
                )
                report["lens_putback_backlog"] = backlog.to_dict()
                if not backlog.ok:
                    report["ok"] = False
                    report["errors"].append(
                        {
                            "code": backlog.code,
                            "message": backlog.message,
                        }
                    )
                elif backlog.code == "lens_putback_backlog_warn":
                    report.setdefault("warnings", []).append(
                        {
                            "code": backlog.code,
                            "message": backlog.message,
                        }
                    )
            except Exception as e:  # noqa: BLE001
                report["lens_putback_backlog"] = {
                    "ok": False,
                    "code": "lens_putback_backlog_probe_failed",
                    "message": str(e),
                }
                report["ok"] = False
                report["errors"].append(
                    {
                        "code": "lens_putback_backlog_probe_failed",
                        "message": str(e),
                    }
                )

            # Deployment-doctor health probe
            # (wire-deployment-doctor-into-substrate-schedule,
            # deepen-dbos-integration Phase 2). Reads the most-recent
            # scheduled doctor workflow's verdict, surfaces per-check
            # pass/fail. Stale verdicts are also fatal.
            try:
                dd = check_deployment_doctor_health(
                    deployment=deployment_name,
                    dsn=dsn,
                )
                report["deployment_doctor_health"] = dd.to_dict()
                if not dd.ok:
                    report["ok"] = False
                    report["errors"].append(
                        {"code": dd.code, "message": dd.message}
                    )
            except Exception as e:  # noqa: BLE001
                report["deployment_doctor_health"] = {
                    "ok": False,
                    "code": "deployment_doctor_health_probe_failed",
                    "message": str(e),
                }

            # Generation-event subscribers probe
            # (publish-generation-events-via-dbos-streams,
            # deepen-dbos-integration Phase 1). Informational only.
            try:
                gs = check_generation_event_subscribers(
                    deployment=deployment_name,
                    dsn=dsn,
                )
                report["generation_event_subscribers"] = gs.to_dict()
            except Exception as e:  # noqa: BLE001
                report["generation_event_subscribers"] = {
                    "ok": False,
                    "code": "generation_event_subscribers_probe_failed",
                    "message": str(e),
                }

            # Escalation open-backlog probe (deepen-dbos-integration
            # Phase 0). Informational: open HITL escalations are
            # normal. Above warn threshold, surface in `warnings`.
            try:
                eb = check_escalation_open_backlog(
                    deployment=deployment_name,
                    dsn=dsn,
                )
                report["escalation_open_backlog"] = eb.to_dict()
                if eb.code == "escalation_open_backlog_high":
                    report.setdefault("warnings", []).append(
                        {"code": eb.code, "message": eb.message}
                    )
            except Exception as e:  # noqa: BLE001
                skip = _doctor_probe_feature_absent(e)
                if skip is not None:
                    report["escalation_open_backlog"] = {
                        "ok": True,
                        "skipped": True,
                        "code": "escalation_open_backlog_skipped",
                        "reason": skip,
                    }
                else:
                    report["escalation_open_backlog"] = {
                        "ok": False,
                        "code": "escalation_open_backlog_probe_failed",
                        "message": str(e),
                    }
                    report["ok"] = False
                    report["errors"].append(
                        {
                            "code": "escalation_open_backlog_probe_failed",
                            "message": str(e),
                        }
                    )

            # Runtime-services-health probe
            # (consolidate-deployment-runtime + harden-runtime-
            # supervisor §C). When the doctor is run from inside the
            # worker (scheduled `okg_substrate_doctor` workflow), the
            # in-process services registry has live data. When run
            # from an external CLI shell, the probe instead overlays
            # the most-recent `doctor_verdict` event's
            # `runtime_services_health` payload via the dsn
            # parameter, with a staleness stamp.
            try:
                rs = check_runtime_services_health(
                    deployment=deployment_name,
                    dsn=dsn,
                )
                report["runtime_services_health"] = rs.to_dict()
                if not rs.ok:
                    report["ok"] = False
                    report["errors"].append(
                        {"code": rs.code, "message": rs.message}
                    )
            except Exception as e:  # noqa: BLE001
                report["runtime_services_health"] = {
                    "ok": False,
                    "code": "runtime_services_health_probe_failed",
                    "message": str(e),
                }

            # `fix-substrate-silent-failures` Phase 1.2 — three probes
            # that catch the silent-failure conditions the substrate
            # used to hide. Each appends to `errors` and toggles
            # `report["ok"]` on a finding; payload from the verdict's
            # to_dict() is stashed under a top-level key so an
            # operator scripting against `okg doctor --json` can
            # introspect.
            try:
                iqu = check_internal_queue_unconsumed(
                    deployment=deployment_name, dsn=dsn,
                )
                report["internal_queue_unconsumed"] = iqu.to_dict()
                if not iqu.ok:
                    report["ok"] = False
                    report["errors"].append(
                        {
                            "code": iqu.code,
                            "message": iqu.message,
                            "stuck_count": iqu.stuck_count,
                            "sample": iqu.sample,
                        },
                    )
            except Exception as e:  # noqa: BLE001
                report["internal_queue_unconsumed"] = {
                    "ok": False,
                    "code": "internal_queue_unconsumed_probe_failed",
                    "message": str(e),
                }
            try:
                from okg.substrate.runtime.workflow_operational_outcomes import (
                    check_workflow_operational_outcomes,
                )
                woe = check_workflow_operational_outcomes(
                    deployment=deployment_name, dsn=dsn,
                )
                report["workflow_operational_outcomes"] = woe.to_dict()
                if not woe.ok:
                    report["ok"] = False
                    report["errors"].append(
                        {
                            "code": woe.code,
                            "message": woe.message,
                            "sample": list(woe.samples),
                        },
                    )
            except Exception as e:  # noqa: BLE001
                report["workflow_operational_outcomes"] = {
                    "ok": False,
                    "code": "workflow_operational_outcomes_probe_failed",
                    "message": str(e),
                }
                report["ok"] = False
                report["errors"].append(
                    {
                        "code": "workflow_operational_outcomes_probe_failed",
                        "message": str(e),
                    },
                )
            try:
                mdr = check_manifest_dsn_resolvable(
                    deployment=deployment_name,
                )
                report["manifest_dsn_resolvable"] = mdr.to_dict()
                if not mdr.ok:
                    report["ok"] = False
                    report["errors"].append(
                        {
                            "code": mdr.code,
                            "message": mdr.message,
                            "reason": mdr.reason,
                        },
                    )
            except Exception as e:  # noqa: BLE001
                report["manifest_dsn_resolvable"] = {
                    "ok": False,
                    "code": "manifest_dsn_resolvable_probe_failed",
                    "message": str(e),
                }

    # Layer 2A: profile-awareness — when the deployment.yaml carries
    # a `profile:` block written by `okg init --profile`, compare
    # the deployment's `modules:` and `sources:` against what the
    # profile expects. Deployments without a `profile:` block fall
    # through (no comparison performed). Findings land under
    # `report["profile_diagnostics"]`; YELLOW entries land in
    # `report["warnings"]` and don't toggle `report["ok"]`.
    if deployment_name:
        try:
            profile_diag = _doctor_profile_diagnostics(deployment_name)
        except Exception as e:  # noqa: BLE001
            profile_diag = {
                "ok": False,
                "code": "profile_diagnostics_probe_failed",
                "message": str(e),
            }
        if profile_diag is not None:
            report["profile_diagnostics"] = profile_diag
            for w in profile_diag.get("yellow_findings", []):
                report.setdefault("warnings", []).append(w)

    # Host-filesystem headroom sentinel (PACT bound-host-storage-pressure).
    # Runs on every deployment-scoped doctor — including the scheduled
    # `okg_substrate_doctor` workflow, which shells this exact command —
    # so host exhaustion is surfaced BEFORE Postgres PANICs (the 2026-07-17
    # double-ENOSPC shape). Deliberately NOT gated on runtime.enabled: the
    # host disk does not care whether the worker runs. A `blocker` verdict
    # flips the doctor to failing; `warn` lands in warnings; an undeclared
    # threshold pair is a loud misconfiguration warning (check ok=False in
    # its own payload), never a silently-defaulted floor (P0).
    if deployment_name:
        from okg.substrate.runtime.host_headroom import (
            check_host_disk_headroom,
        )

        try:
            headroom = check_host_disk_headroom(deployment=deployment_name)
            report["host_disk_headroom"] = headroom.to_dict()
            if headroom.status == "blocker":
                report["ok"] = False
                report["errors"].append(
                    {"code": headroom.code, "message": headroom.message}
                )
            elif headroom.status in {"warn", "misconfigured", "error"}:
                report.setdefault("warnings", []).append(
                    {"code": headroom.code, "message": headroom.message}
                )
        except Exception as e:  # noqa: BLE001 - the sentinel must not mask other doctor findings; its failure is itself reported loudly.
            report["host_disk_headroom"] = {
                "ok": False,
                "code": "host_headroom_probe_failed",
                "message": str(e),
            }
            report.setdefault("warnings", []).append(
                {
                    "code": "host_headroom_probe_failed",
                    "message": str(e),
                }
            )

    # Chronos history-retention sentinel (issue #1309). A deployment that
    # never declares `chronos.history_retention` retires no node or edge
    # history at all, and nothing said so until an operator tried to reclaim
    # space during an incident. Contained like the host-headroom probe above:
    # a probe that cannot run is a warning about the probe, never a verdict on
    # the deployment, and a finding here is a warning -- growth is a thing to
    # act on, not a reason to fail the doctor.
    if deployment_name and manifest is not None:
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.history_compaction import check_history_retention

        try:
            chronos_manifest = manifest.chronos or {}
            with db_connect(dsn) as retention_conn:
                retention_store = ChronosGraphStore.from_connection(
                    retention_conn, ensure_schema=False
                )
                retention = check_history_retention(
                    retention_conn,
                    store=retention_store,
                    deployment_name=deployment_name,
                    policy_raw=chronos_manifest.get("history_retention"),
                )
            report["history_retention"] = retention
            if retention.get("status") == "warn":
                report.setdefault("warnings", []).append(
                    {
                        "code": retention["code"],
                        "message": retention["message"],
                    }
                )
        except Exception as e:  # noqa: BLE001 - the probe's own failure is reported, never masked.
            report["history_retention"] = {
                "ok": False,
                "code": "history_retention_probe_failed",
                "message": str(e),
            }
            report.setdefault("warnings", []).append(
                {
                    "code": "history_retention_probe_failed",
                    "message": str(e),
                }
            )

    if deployment_name and manifest is not None:
        from okg.substrate.deployment_apply import deployment_apply_health

        # Contained like the host-headroom probe above: a probe that cannot
        # run is a warning about the probe, not a verdict on the deployment.
        try:
            apply_health = deployment_apply_health(
                deployment=deployment_name,
                deployment_dir=manifest.deployment_dir,
                dsn=dsn,
            )
        except Exception as e:  # noqa: BLE001
            report.setdefault("warnings", []).append({
                "code": "deployment_apply_probe_failed",
                "message": str(e),
            })
        else:
            report["deployment_apply"] = apply_health
            # `deployment_apply_health` puts an unacked worker stage in
            # `warnings` only when `runtime.enabled` is false: no worker runs
            # there, so the stage can never be observed and neither a worker
            # nor `okg deployment apply` changes it (issue #3578). With the
            # runtime on, the same state is a problem and fails below. The
            # stages stay in `deployment_apply.warnings` for a reader who
            # wants them; they are not a warning about this deployment.
            if not apply_health.get("ok"):
                report["ok"] = False
                report["errors"].append({
                    "code": "deployment_apply_not_converged",
                    "message": (
                        apply_health.get("message")
                        or "deployment apply is incomplete, failed, or stale"
                    ),
                    "details": apply_health,
                    "fix_hint": apply_health.get("next_command"),
                })

    # Issue #2993: a failed source run's unfinished undo, or its kept partial
    # rows (finish-undo --accept-current), refuses every publish until the
    # source reruns or is retired. Readers keep the last good generation, so
    # nothing else here shows it; name the source and the way out.
    if deployment_name and not any(
        error.get("code") == "doctor_probe_failed" for error in report["errors"]
    ):
        from okg.substrate.ingest.run_base import publish_block_report

        try:
            block = publish_block_report(dsn, deployment_name)
        except Exception as e:  # noqa: BLE001
            report.setdefault("warnings", []).append({
                "code": "publish_block_probe_failed",
                "message": f"{type(e).__name__}: {e}",
            })
        else:
            report["publish_block"] = block
            if block.get("blocked"):
                report["ok"] = False
                report["errors"].append({
                    "code": block["code"],
                    "message": block["message"],
                    "sources": block["sources"],
                    "details": block["data"],
                    "fix_hint": block["fix_hint"],
                })
        # Round 6: a run still writing holds publishes back until it ends
        # (its owner session is alive), and a stale run base -- an ended run
        # whose writes stand -- pins history. Warnings, each naming the run,
        # its source, whether its owner is alive, and the command.
        from okg.substrate.ingest.run_base import (
            in_progress_hint,
            source_run_health,
        )

        try:
            runs = source_run_health(dsn, deployment_name)
        except Exception as e:  # noqa: BLE001
            report.setdefault("warnings", []).append({
                "code": "source_run_probe_failed",
                "message": f"{type(e).__name__}: {e}",
            })
        else:
            report["source_runs"] = runs
            if runs["in_progress"]:
                report.setdefault("warnings", []).append({
                    "code": "source_run_in_progress",
                    "message": "a source run is still writing; publishes "
                               "wait or are refused until it ends",
                    "details": runs["in_progress"],
                    "fix_hint": in_progress_hint(
                        deployment_name, runs["in_progress"],
                    ),
                })
            if runs["stale_run_bases"]:
                report.setdefault("warnings", []).append({
                    "code": "stale_run_bases",
                    "message": "run-base checkpoints of ended runs are left "
                               "over; they pin history",
                    "details": runs["stale_run_bases"],
                    "fix_hint": (
                        f"`okg runtime finish-undo --deployment "
                        f"{deployment_name} --apply` deletes them"
                    ),
                })

    # Issue #3256: a publish a gate refused leaves a `blocked` checkpoint
    # newer than the last published one. Readers keep the old version (or
    # none) and, before this block, doctor still read ok. `okg status` exits
    # 3 on the same attempt; both read it with `_latest_publish_attempt`.
    if deployment_name and not any(
        error.get("code") == "doctor_probe_failed" for error in report["errors"]
    ):
        _add_latest_publish_attempt(report, dsn, deployment_name, manifest)

    # Enricher liveness (the `--check enricher-liveness` queries, three
    # indexed reads of small per-deployment tables). A publisher swallows an
    # enricher crash so the publish still commits; before this block the
    # only place the crash showed was the named check, so a plain doctor on
    # a deployment whose enricher had failed for days read clean. Findings
    # are warnings, not errors: the graph readers see is intact, and the
    # named check still exits 1 for a gate that wants to fail on them.
    if deployment_name:
        if any(
            error.get("code") == "doctor_probe_failed"
            for error in report["errors"]
        ):
            # The database could not be reached at all; that is already an
            # error above, and a second connect attempt would only repeat it.
            report["enricher_liveness"] = {
                "ok": False,
                "skipped": True,
                "code": "enricher_liveness_skipped_database_unreachable",
            }
        else:
            report["enricher_liveness"] = _default_doctor_enricher_liveness(
                dsn, deployment_name, report,
                deployment_dir=(
                    manifest.deployment_dir if manifest is not None else None
                ),
            )

    # Passage parents a catalog leaves undeclared (operator decision
    # jiraparent B, 2026-10-06). A chunk names its parent only when the
    # parent's subtype declares `okg_passage_rollup` for it; an edge from any
    # other subtype is ignored and readers see `declared_but_unlinked`. On the
    # C4 comp-ops copy that hid the parent of 98,661 of 150,781 chunks and
    # nothing said so. A warning, like retention: the graph is intact.
    if deployment_name:
        if any(
            error.get("code") == "doctor_probe_failed"
            for error in report["errors"]
        ):
            report["passage_parents"] = {
                "ok": False,
                "skipped": True,
                "code": "passage_parents_skipped_database_unreachable",
            }
        else:
            report["passage_parents"] = _default_doctor_passage_parents(
                dsn, deployment_name, report
            )

    # Issue #3783: a source that loses its records. Bug #3776 moved almost
    # every file record of two sources to another one; the total held and
    # doctor read ok. A warning, not an error: doctor cannot tell a
    # deliberate removal from a loss.
    if deployment_name:
        if any(
            error.get("code") == "doctor_probe_failed"
            for error in report["errors"]
        ):
            report["source_record_loss"] = {
                "ok": False,
                "skipped": True,
                "code": "source_record_loss_skipped_database_unreachable",
            }
        else:
            report["source_record_loss"] = _default_doctor_source_record_loss(
                dsn, deployment_name, report
            )

    # Vector sets (PACT openrouter-embeddings-hnsw): per set coverage at the
    # published generation, index state, spend, file-versus-database drift,
    # and old-column vectors no set owns. Warnings only: semantic search is
    # one method, and the graph is intact.
    if deployment_name:
        if any(
            error.get("code") == "doctor_probe_failed"
            for error in report["errors"]
        ):
            report["vector_sets"] = {
                "ok": False,
                "skipped": True,
                "code": "vector_sets_skipped_database_unreachable",
            }
        else:
            report["vector_sets"] = _default_doctor_vector_sets(
                dsn, deployment_name, manifest, report,
                timeout_ms=_DEFAULT_DOCTOR_VECTOR_SET_TIMEOUT_MS,
            )

    _emit_ok(report, as_json=as_json)
    return 0 if report["ok"] else 1


#: Statement timeout for each vector-set read in plain doctor. Coverage hashes
#: every visible text node of a set's subtypes; the scheduled doctor gives the
#: whole report 60 s. A timeout is a `vector_set_probe_failed` warning that
#: names the unbounded named check.
_DEFAULT_DOCTOR_VECTOR_SET_TIMEOUT_MS = 15_000


def _default_doctor_vector_sets(
    dsn: str,
    deployment: str,
    manifest: Any,
    report: dict,
    *,
    timeout_ms: int | None,
) -> dict[str, object]:
    """Run the vector-set report and copy its findings into the warnings.

    A probe that cannot run at all is a `vector_set_probe_failed` warning,
    never a clean result.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.embeddings.doctor import vector_set_report

    warnings = report.setdefault("warnings", [])
    try:
        config = manifest.embedding_config if manifest is not None else None
        with db_connect(dsn, autocommit=True) as conn:
            if timeout_ms is not None:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT set_config('statement_timeout', %s, false)",
                        (str(int(timeout_ms)),),
                    )
            # No timeout (the named check): count coverage exactly.
            result = vector_set_report(
                conn, deployment=deployment, config=config,
                exact_coverage=timeout_ms is None,
            )
    except Exception as e:  # noqa: BLE001 - the probe's own failure is reported, never masked.
        message = f"{type(e).__name__}: {e}"
        warnings.append({"code": "vector_set_probe_failed", "message": message})
        return {"ok": False, "code": "vector_set_probe_failed", "message": message}
    warnings.extend(result["findings"])
    return result


def _doctor_vector_sets(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg doctor --check vector-sets`: the vector-set report with no
    statement timeout; exits 1 on any finding."""
    _load_main_helpers()
    from okg.substrate.deployment_default import DeploymentNotSpecified
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )

    deployment = getattr(args, "deployment", None)
    if not deployment:
        return _emit_error(
            DeploymentNotSpecified(
                "the vector-sets check reads one deployment's vector sets",
                fix_hint="okg doctor --deployment <name> --check vector-sets",
            ),
            as_json=as_json,
        )
    try:
        manifest = resolve_deployment(str(deployment))
        dsn = _resolve_dsn(args)
    except DeploymentResolutionError as exc:
        return _emit_error(SubstrateError(str(exc)), as_json=as_json)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    report: dict = {"ok": True, "errors": []}
    result = _default_doctor_vector_sets(
        dsn, str(deployment), manifest, report, timeout_ms=None,
    )
    payload = {
        "ok": bool(result.get("ok")),
        "check": "vector-sets",
        "vector_sets": result,
        "warnings": report.get("warnings", []),
    }
    _emit_ok(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


#: Statement timeout for the passage-parent probe. It reads every live edge
#: of a declared passage edge type into a declared child subtype: on the C4
#: comp-ops graph (631k nodes, 151k chunks) that took 5.5-6 s. The scheduled
#: doctor gives the whole report 60 s, so this stays well inside it; a
#: timeout is a `passage_parent_probe_failed` warning.
_DEFAULT_DOCTOR_PASSAGE_PARENT_TIMEOUT_MS = 15_000


def _default_doctor_source_record_loss(
    dsn: str,
    deployment: str,
    report: dict,
) -> dict[str, object]:
    """Warn for each source whose published record count collapsed (#3783).

    Appends one `source_records_lost` warning per source the probe in
    `okg.substrate.source_record_loss` names. A probe that cannot run is a
    `source_record_loss_probe_failed` warning, never a clean result.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.source_record_loss import source_record_loss

    warnings = report.setdefault("warnings", [])
    try:
        # Autocommit, as the other probes: the timeout is a session setting.
        with db_connect(dsn, autocommit=True) as conn:
            result = source_record_loss(conn, deployment)
    except Exception as e:  # noqa: BLE001 - the probe's own failure is reported, never masked.
        message = f"{type(e).__name__}: {e}"
        warnings.append(
            {"code": "source_record_loss_probe_failed", "message": message}
        )
        return {
            "ok": False,
            "code": "source_record_loss_probe_failed",
            "message": message,
        }
    window_days = result.get("thresholds", {}).get("window_days")
    for loss in result.get("losses", []):
        warnings.append(
            {
                "code": "source_records_lost",
                "message": (
                    f"source {loss['source']} has {loss['records_now']} "
                    f"records in the published generation, down from "
                    f"{loss['records_before']} in {loss['baseline_generation']}; "
                    "readers no longer find the rest under this source"
                ),
                "fix_hint": (
                    "if the removal was not intended, compare the per-source "
                    "counts in `okg status` to find the source that took the "
                    "records, fix its ownership and publish again. A "
                    "deliberate removal (a source retired, renamed or "
                    "narrowed in scope) is expected to warn until the "
                    "generations that held the records are more than "
                    f"{window_days} days old"
                ),
                **loss,
            }
        )
    return result


def _add_latest_publish_attempt(
    report: dict, dsn: str, deployment_name: str, manifest: Any,
) -> None:
    """Fail doctor when the newest default-branch publish attempt is blocked.

    The verdict comes from the attempt itself. The commands come from the
    read-only publish-recovery plan, which names the same checkpoint; when
    that plan cannot be read the error says so instead of guessing.
    """
    import psycopg

    from okg.substrate.cli.main import _latest_publish_attempt
    from okg.substrate.db.connection import connect as db_connect

    try:
        with db_connect(dsn) as conn:
            attempt = _latest_publish_attempt(conn, deployment_name)
    except psycopg.errors.UndefinedTable:
        # Not migrated yet: nothing has been attempted, so nothing is blocked.
        report["latest_publish_attempt"] = None
        return
    except Exception as e:  # noqa: BLE001 - reported as an error, never ok.
        report["ok"] = False
        report["errors"].append({
            "code": "latest_publish_attempt_unknown",
            "message": (
                "the newest publish attempt could not be read, so doctor "
                f"cannot say readers see a current version: "
                f"{type(e).__name__}: {e}"
            ),
        })
        return
    report["latest_publish_attempt"] = attempt
    if attempt is None or attempt["status"] == "published":
        return
    if attempt["status"] != "blocked":
        # Unknown is never read as healthy (#2573), as in `okg status`.
        report["ok"] = False
        report["errors"].append({
            "code": "latest_publish_attempt_unknown",
            "message": (
                f"checkpoint {attempt['checkpoint_id']} records status "
                f"{attempt['status']!r}, neither published nor blocked"
            ),
            "fix_hint": (
                f"`okg generation describe {attempt['checkpoint_id']} "
                f"--deployment {deployment_name} --json`"
            ),
        })
        return

    from okg.substrate.publish_recovery import plan_publish_recovery

    runtime = getattr(manifest, "runtime", None)
    plan = plan_publish_recovery(
        dsn=dsn,
        deployment=deployment_name,
        runtime_enabled=(
            bool(runtime.enabled) if runtime is not None else None
        ),
    )
    last_good = plan.get("last_known_good") if plan.get("ok") else None
    readers_see = (
        last_good.get("generation_id")
        if isinstance(last_good, dict) and last_good.get("generation_id")
        else "no published version" if plan.get("ok")
        else "an unknown version (the recovery plan could not be read)"
    )
    superseded = attempt.get("recovery_superseded")
    if plan.get("ok"):
        fix_hint = _blocked_attempt_fix_hint(plan, superseded, deployment_name)
    else:
        error = plan.get("error") or {}
        fix_hint = (
            "the recovery plan could not be read "
            f"({error.get('type')}: {error.get('message')}); read the cause "
            f"with `okg generation describe {attempt['checkpoint_id']} "
            f"--deployment {deployment_name} --json`"
        )
    report["ok"] = False
    report["errors"].append({
        "code": "latest_publish_blocked",
        "message": (
            f"the newest publish attempt ({attempt['checkpoint_id']}) was "
            f"blocked: {attempt['blocked_detail']}. "
            + (
                f"Its recovery was superseded by {superseded.get('superseded_by')} "
                f"at {superseded.get('superseded_at')} "
                f"({superseded.get('supersede_reason')!r}), but no publish "
                "has replaced it. "
                if superseded else ""
            )
            + f"Readers still see {readers_see}."
        ),
        "checkpoint_id": attempt["checkpoint_id"],
        "blocked_detail": attempt["blocked_detail"],
        "fix_hint": fix_hint,
        "details": {"publish_recovery": plan},
    })


def _blocked_attempt_fix_hint(
    plan: dict, superseded: dict | None, deployment_name: str,
) -> str:
    """One hint from a readable publish-recovery plan, in the plan's terms."""
    from okg.substrate.publish_recovery import _supersede_command

    recovery = plan.get("recovery_command")
    if superseded and recovery:
        return (
            "the checkpoint stays newest until a publish replaces it: run "
            f"`{recovery}`; if no source has new work, that run publishes "
            "nothing and this error remains"
        )
    hint = str(plan.get("explanation") or "").rstrip(". ")
    diagnose = plan.get("diagnose_command")
    if diagnose and diagnose != recovery:
        hint += f". Read the cause with `{diagnose}`"
    if recovery:
        hint += f". Then run `{recovery}`"
    if plan.get("escape_hatch_command"):
        # The dry run: an applied supersede cannot be undone, and its
        # reason is a placeholder (see publish_recovery._supersede_command).
        hint += (
            ". If a named source cannot be made to pass, preview retiring "
            "the checkpoint with "
            f"`{_supersede_command(deployment_name, apply=False)}`"
        )
    return hint


def _default_doctor_passage_parents(
    dsn: str,
    deployment: str,
    report: dict,
) -> dict[str, object]:
    """Warn for passage parent links the pinned catalog does not declare.

    For each child subtype some `okg_passage_rollup` names, counts the live
    children of the published generation reached by an edge of a declared
    edge type from a parent subtype that declares no relation for that child,
    and appends one `passage_parent_undeclared` warning per (parent subtype,
    edge type, child subtype). Edge types no declaration uses for that child
    are not parent links. A probe that cannot run is a warning, never a clean
    result; nothing published skips the check and says so.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.graph_store import ChronosGraphStore

    warnings = report.setdefault("warnings", [])
    try:
        # Autocommit, as the enricher probe: the timeout is a session setting
        # no rollback inside the store can undo.
        with db_connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, false)",
                    (str(_DEFAULT_DOCTOR_PASSAGE_PARENT_TIMEOUT_MS),),
                )
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            checkpoint = store.latest_generation(
                deployment_name=deployment, status="published"
            )
            if checkpoint is None:
                return {
                    "ok": True,
                    "skipped": True,
                    "code": "passage_parents_skipped_nothing_published",
                }
            if checkpoint.metadata.get("catalog_version_id") is None:
                raise ValueError(
                    f"published generation {checkpoint.checkpoint_id} records no "
                    "catalog_version_id, so its passage declarations are unknown"
                )
            catalog_version_id = int(checkpoint.metadata["catalog_version_id"])
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT name,
                           metadata -> 'passage_rollup' ->> 'edge_type',
                           metadata -> 'passage_rollup' ->> 'child_subtype'
                      FROM okg.subtypes
                     WHERE catalog_version_id = %s
                       AND metadata ? 'passage_rollup'
                    """,
                    (catalog_version_id,),
                )
                declared = [
                    (str(parent), str(edge_type), str(child))
                    for parent, edge_type, child in cur.fetchall()
                    if edge_type and child
                ]
            if not declared:
                return {"ok": True, "declarations": 0, "undeclared": []}
            rows = store.query_generation_once(
                checkpoint.checkpoint_id,
                """
                WITH declared AS (
                    SELECT * FROM unnest(
                        %(parents)s::text[], %(edges)s::text[], %(children)s::text[]
                    ) AS d(parent_subtype, edge_type, child_subtype)
                ),
                links AS (
                    SELECT c.node_id,
                           c.subtype AS child_subtype,
                           e.edge_type,
                           p.subtype AS parent_subtype,
                           EXISTS (
                               SELECT 1 FROM declared d
                                WHERE d.child_subtype = c.subtype
                                  AND d.edge_type = e.edge_type
                                  AND d.parent_subtype = p.subtype
                           ) AS is_declared
                      FROM okg.graph_edges e
                      JOIN okg.graph_nodes c ON c.node_id = e.dst
                      JOIN okg.graph_nodes p ON p.node_id = e.src
                     WHERE e.edge_type = ANY(%(edges)s)
                       AND c.subtype = ANY(%(children)s)
                       AND EXISTS (
                               SELECT 1 FROM declared d
                                WHERE d.child_subtype = c.subtype
                                  AND d.edge_type = e.edge_type)
                ),
                per_child AS (
                    SELECT node_id, bool_or(is_declared) AS has_declared_parent
                      FROM links
                     GROUP BY node_id
                )
                SELECT l.parent_subtype,
                       l.edge_type,
                       l.child_subtype,
                       count(DISTINCT l.node_id)
                           FILTER (WHERE NOT pc.has_declared_parent) AS unlinked,
                       count(DISTINCT l.node_id)
                           FILTER (WHERE pc.has_declared_parent) AS also_declared
                  FROM links l
                  JOIN per_child pc USING (node_id)
                 WHERE NOT l.is_declared
                 GROUP BY 1, 2, 3
                 ORDER BY 4 DESC, 1, 2, 3
                """,
                {
                    "parents": [d[0] for d in declared],
                    "edges": [d[1] for d in declared],
                    "children": [d[2] for d in declared],
                },
            )
    except Exception as e:  # noqa: BLE001 - the probe's own failure is reported, never masked.
        message = f"{type(e).__name__}: {e}"
        warnings.append({"code": "passage_parent_probe_failed", "message": message})
        return {
            "ok": False,
            "code": "passage_parent_probe_failed",
            "message": message,
        }
    undeclared = [
        {
            "parent_subtype": str(row["parent_subtype"]),
            "edge_type": str(row["edge_type"]),
            "child_subtype": str(row["child_subtype"]),
            # Children whose only parent links are undeclared: readers see
            # `declared_but_unlinked`.
            "children": int(row["unlinked"]),
            # Children that also have a declared parent: readers see that
            # parent; declaring this one too would make them
            # `multiple_parents`.
            "children_with_a_declared_parent": int(row["also_declared"]),
        }
        for row in rows
    ]
    for item in undeclared:
        if not item["children"]:
            continue
        warnings.append(
            {
                "code": "passage_parent_undeclared",
                "message": (
                    f"{item['children']} {item['child_subtype']} nodes are "
                    f"reached only by a {item['edge_type']!r} edge from "
                    f"{item['parent_subtype']}, which declares no passage "
                    "relation for them, so search and inspect show their "
                    "parent as declared_but_unlinked"
                ),
                "fix_hint": (
                    f"if {item['parent_subtype']} is their parent, declare "
                    f"okg_passage_rollup: {{edge_type: {item['edge_type']}, "
                    f"child_subtype: {item['child_subtype']}}} on its class in "
                    "the deployment schema (a subtype declares at most one "
                    "passage relation, and the catalog needs a narrowing for "
                    f"{item['parent_subtype']} {item['edge_type']} "
                    f"{item['child_subtype']}), then `okg catalog load "
                    "--apply` and publish"
                ),
                **item,
            }
        )
    return {
        "ok": not any(item["children"] for item in undeclared),
        "declarations": len(declared),
        "undeclared": undeclared,
    }


# Plain doctor runs on every deployment-scoped call, including the scheduled
# doctor workflow, so its enricher probe is bounded: one statement timeout
# for all three reads, and quarantines are grouped by enricher in SQL rather
# than fetched row by row.
_DEFAULT_DOCTOR_ENRICHER_TIMEOUT_MS = 5000
# `_run_enrichers` records an enricher that returned without covering every
# input as `failed:incomplete_inputs` (run status `partial`). `okg run
# --once` does not count that as an enricher failure, so plain doctor
# reports it apart from crashes too.
_INCOMPLETE_ENRICHER_STATUS = "failed:incomplete_inputs"


def _registered_enricher_names(
    deployment_dir: Any,
) -> tuple[set[str] | None, str | None]:
    """Names of the enrichers ``deployment.yaml`` registers today, and why not.

    Names are None when the manifest cannot be read or an enricher cannot be
    loaded; the second value then says why. The caller treats every
    progress row as current rather than guessing which ones are stale.
    """
    if deployment_dir is None:
        return None, "deployment directory not resolved"
    from okg.substrate.publisher.enricher_loader import load_enrichers

    try:
        # active_profiles=None loads every entry, profiled ones included:
        # an enricher switched off by profile is still registered.
        enrichers = load_enrichers(deployment_dir)
    except Exception as exc:  # noqa: BLE001 - reported by the caller as registry unknown, with the cause.
        return None, f"{type(exc).__name__}: {exc}"
    return {
        str(getattr(enricher, "name", enricher.__class__.__name__))
        for enricher in enrichers
    }, None


def _default_doctor_enricher_liveness(
    dsn: str,
    deployment: str,
    report: dict,
    *,
    deployment_dir: Any = None,
) -> dict[str, object]:
    """Run the enricher-liveness queries for plain ``okg doctor``.

    Appends one warning per failed, incomplete, or quarantining enricher
    that ``deployment.yaml`` still registers, and returns the check's
    payload. Silent enrichers are counted and named only. Progress rows of
    enrichers no longer registered are listed as ``stale_enrichers`` with
    no warning. A probe that cannot run is itself a warning, never a clean
    result.
    """
    from okg.substrate.db.connection import connect as db_connect

    check_command = (
        f"okg doctor --check enricher-liveness --deployment {deployment}"
    )
    try:
        with db_connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, false)",
                    (str(_DEFAULT_DOCTOR_ENRICHER_TIMEOUT_MS),),
                )
                cur.execute(
                    """
                    SELECT enricher_name, last_status,
                           consecutive_failure_count
                      FROM okg.enricher_progress
                     WHERE deployment_name = %s
                       AND last_status LIKE 'failed:%%'
                     ORDER BY enricher_name
                    """,
                    (deployment,),
                )
                failed_rows = cur.fetchall()
                cur.execute(
                    """
                    SELECT enricher_name, count(*),
                           max(last_failed_at),
                           (array_agg(last_failure_class
                                      ORDER BY last_failed_at DESC))[1]
                      FROM okg.enricher_input_quarantines
                     WHERE deployment_name = %s
                       AND status = 'active'
                     GROUP BY enricher_name
                     ORDER BY enricher_name
                    """,
                    (deployment,),
                )
                quarantine_rows = cur.fetchall()
            silent = _enricher_liveness_findings(conn, deployment)
    except Exception as exc:  # noqa: BLE001 - the probe's own failure is reported, never masked.
        report.setdefault("warnings", []).append({
            "code": "enricher_liveness_probe_failed",
            "message": f"enricher-liveness probe failed: {exc}",
            "fix_hint": check_command,
        })
        return {
            "ok": False,
            "code": "enricher_liveness_probe_failed",
            "message": str(exc),
        }

    registered, registry_error = _registered_enricher_names(deployment_dir)
    stale: set[str] = set()

    def current(name: str) -> bool:
        if registered is None or name in registered:
            return True
        stale.add(name)
        return False

    warnings = report.setdefault("warnings", [])
    if registry_error and deployment_dir is not None:
        warnings.append({
            "code": "enricher_registry_unreadable",
            "message": (
                "could not read the enrichers deployment.yaml registers "
                f"({registry_error}); every enricher progress row is treated "
                "as current"
            ),
            "fix_hint": check_command,
        })
    failed_count = incomplete_count = 0
    for name, status, count in failed_rows:
        name, status, count = str(name), str(status), int(count)
        if not current(name):
            continue
        if status == _INCOMPLETE_ENRICHER_STATUS:
            incomplete_count += 1
            warnings.append({
                "code": "enricher_incomplete",
                "message": (
                    f"enricher {name} returned without covering every input "
                    f"on its last run, {count} cycle(s) in a row; it retries "
                    f"the rest next cycle"
                ),
                "enricher_name": name,
                "consecutive_failures": count,
                "fix_hint": check_command,
            })
            continue
        failed_count += 1
        error_class = status.partition(":")[2] or "unknown"
        warnings.append({
            "code": "enricher_failed",
            "message": (
                f"enricher {name} failed ({error_class}) on its last run, "
                f"{count} cycle(s) in a row; publishes go ahead without its "
                f"output and retry it next cycle"
            ),
            "enricher_name": name,
            "error_class": error_class,
            "consecutive_failures": count,
            "fix_hint": check_command,
        })
    quarantine_count = 0
    for name, items, last_failed_at, failure_class in quarantine_rows:
        name = str(name)
        if not current(name):
            continue
        quarantine_count += int(items)
        warnings.append({
            "code": "enricher_input_quarantined",
            "message": (
                f"enricher {name} has {int(items)} quarantined input(s) it "
                f"no longer retries (latest failure: "
                f"{failure_class or 'unknown'})"
            ),
            "enricher_name": name,
            "quarantined_inputs": int(items),
            "last_failed_at": (
                last_failed_at.isoformat() if last_failed_at else None
            ),
            "fix_hint": check_command,
        })
    silent_names = [
        str(f["enricher_name"]) for f in silent
        if current(str(f["enricher_name"]))
    ]
    payload: dict[str, object] = {
        "ok": not (failed_count or incomplete_count or quarantine_count),
        "failed_count": failed_count,
        "incomplete_count": incomplete_count,
        "quarantine_count": quarantine_count,
        "silent_count": len(silent_names),
        "silent_enrichers": silent_names,
        "registry": "unknown" if registered is None else "deployment.yaml",
        "registry_error": registry_error,
        "stale_enrichers": sorted(stale),
        "check_command": check_command,
    }
    if stale:
        payload["stale_note"] = (
            "these enrichers are no longer in deployment.yaml; their "
            "progress rows remain and are not warned about here. No okg "
            "command removes them, and they do not affect publishes."
        )
    return payload


def _doctor_profile_diagnostics(
    deployment_name: str,
) -> dict[str, Any] | None:
    """Layer 2A — read `deployment.yaml`'s `profile:` block and
    compare its expectations against the live deployment.

    Returns:
      - None when the deployment.yaml has no `profile:` block
        (degrades silently; no false alerts).
      - A dict carrying `{profile, modules: {expected, present,
        custom_additions, dropped}, sources: {...},
        yellow_findings: [...]}` when comparison ran.
    """
    _load_main_helpers()
    from okg.substrate.deployments import find_deployment_dir
    dep_dir = find_deployment_dir(deployment_name)
    if dep_dir is None:
        return None
    manifest_path = dep_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return None
    try:
        with manifest_path.open() as f:
            raw = __import__("yaml").safe_load(f) or {}
    except Exception:  # noqa: BLE001 — corrupt deployment.yaml → return None and doctor skips the profile-drift check, surfaces the manifest error in a separate probe.
        return None
    profile_block = raw.get("profile")
    if not isinstance(profile_block, dict):
        return None
    profile_name = profile_block.get("name")
    profile_version = profile_block.get("version")
    if not profile_name:
        return None

    # Load the profile and its modules.yaml. If the profile is
    # missing (operator deleted profiles/ or moved the repo),
    # surface that as a single YELLOW finding rather than failing.
    from okg.substrate.deployment_bootstrap.profile_init import (
        ProfileRetiredError,
        discover_profile,
    )

    try:
        from okg.substrate.catalog.profile import load_profile_modules

        profile_obj = discover_profile(profile_name)
        expected_modules = set(load_profile_modules(profile_obj))
    except ProfileRetiredError as e:
        # A retired name is its own finding: the operator needs the
        # replacement, not a generic "unreadable" message.
        replacement = e.data.get("replacement")
        return {
            "ok": True,
            "profile": {
                "name": profile_name,
                "version": profile_version,
            },
            "warning": f"{e}; skipping module / source comparison",
            "yellow_findings": [{
                "code": "profile_retired",
                "profile_name": profile_name,
                "replacement": replacement,
                "message": str(e),
                "fix_hint": (
                    f"re-run with --profile {replacement}"
                    if replacement
                    else "choose another profile; this one has no replacement"
                ),
            }],
        }
    except Exception as e:
        return {
            "ok": True,
            "profile": {
                "name": profile_name,
                "version": profile_version,
            },
            "warning": (
                f"profile {profile_name!r} not found or unreadable: "
                f"{e}; skipping module / source comparison"
            ),
            "yellow_findings": [{
                "code": "profile_unreadable",
                "profile_name": profile_name,
                "message": (
                    f"deployment.yaml references profile "
                    f"{profile_name!r} but it could not be loaded "
                    f"({e}); profile comparison skipped"
                ),
            }],
        }

    present_modules = set(raw.get("modules") or [])
    custom_additions = sorted(present_modules - expected_modules)
    dropped = sorted(expected_modules - present_modules)

    # Source comparison — best-effort. We compare the
    # `source_registry.yaml`'s top-level source ids against the
    # profile's `source-defaults/` filenames. Missing profile-
    # expected ids land in YELLOW; extras land in informational.
    expected_sources: set[str] = set()
    if (
        profile_obj.source_defaults_dir is not None
        and profile_obj.source_defaults_dir.is_dir()
    ):
        expected_sources = {
            p.stem
            for p in profile_obj.source_defaults_dir.glob("*.yaml")
        }
    present_sources: set[str] = set()
    sr_ref = raw.get("source_registry") or "source_registry.yaml"
    sr_path = dep_dir / sr_ref
    if sr_path.is_file():
        try:
            with sr_path.open() as f:
                sr_raw = __import__("yaml").safe_load(f) or {}
            if isinstance(sr_raw, dict):
                sources_block = sr_raw.get("sources") or {}
                if isinstance(sources_block, dict):
                    present_sources = set(sources_block.keys())
                elif isinstance(sources_block, list):
                    present_sources = {
                        entry.get("name") or entry.get("id")
                        for entry in sources_block
                        if isinstance(entry, dict)
                    } - {None}
        except Exception:  # noqa: BLE001 — drift-detection is advisory; corrupt manifest leaves present_sources empty so drift defaults to "no info".
            pass
    source_additions = sorted(present_sources - expected_sources)
    source_dropped = sorted(expected_sources - present_sources)

    yellow_findings: list[dict[str, Any]] = []
    for m in dropped:
        yellow_findings.append({
            "code": "profile_dropped_module",
            "module": m,
            "profile_name": profile_name,
            "profile_version": profile_version,
            "message": (
                f"profile {profile_name!r} v{profile_version} "
                f"expects module {m!r}, but deployment "
                f"{deployment_name!r} does not load it "
                f"(operator edit?)"
            ),
        })
    for s in source_dropped:
        yellow_findings.append({
            "code": "profile_dropped_source",
            "source": s,
            "profile_name": profile_name,
            "profile_version": profile_version,
            "message": (
                f"profile {profile_name!r} v{profile_version} "
                f"ships source default {s!r}, but deployment "
                f"{deployment_name!r}'s source_registry.yaml "
                f"does not include it"
            ),
        })

    return {
        "ok": True,
        "profile": {
            "name": profile_name,
            "version": profile_version,
            "initialized_at": profile_block.get("initialized_at"),
        },
        "modules": {
            "expected": sorted(expected_modules),
            "present": sorted(present_modules),
            "custom_additions": custom_additions,
            "dropped": dropped,
        },
        "sources": {
            "expected": sorted(expected_sources),
            "present": sorted(present_sources),
            "custom_additions": source_additions,
            "dropped": source_dropped,
        },
        "yellow_findings": yellow_findings,
    }



def _doctor_post_publish_projection(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check post-publish-projection --deployment <name>` —

    Pinned by ``surface-post-publish-projection-failures``. Reads
    ``okg.post_publish_projection_status`` (populated by the
    publisher on every post-publish view-edge projection run) and
    reports ``ok: true`` when no row has ``status='failed'`` for the
    deployment, ``ok: false`` with one finding per failed row
    otherwise. Empty table (pre-population on freshly-migrated
    deployments) reports ``ok: true``.
    """
    from okg.substrate.db.connection import connect as db_connect

    _load_main_helpers()

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    deployment = getattr(args, "deployment", None) or "default"
    findings: list[dict[str, object]] = []
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT generation_id, last_run_at, error
              FROM okg.post_publish_projection_status
             WHERE deployment_name = %s
               AND status = 'failed'
             ORDER BY last_run_at DESC, generation_id DESC
            """,
            (deployment,),
        )
        for row in cur.fetchall():
            findings.append({
                "generation_id": row[0],
                "last_run_at": row[1].isoformat() if row[1] else None,
                "error": row[2],
            })

    payload = {
        "check": "post_publish_projection",
        "deployment": deployment,
        "ok": not findings,
        "failed_count": len(findings),
        "findings": findings,
    }
    _emit_ok(payload, as_json=as_json)
    return 0 if not findings else 1


def _pending_grant_findings(conn: Any) -> list[dict[str, object]]:
    """Grants the Chronos epoch hook could not apply, still queued.

    Split out from the handler so the property can be tested against a
    connection without going through DSN resolution.
    """
    reg = conn.execute(
        "SELECT to_regclass('okg.pending_grants') IS NOT NULL"
    ).fetchone()
    if not reg or not reg[0]:
        # A database that predates migration 0027. Absence of the queue is not
        # evidence of an empty queue, so say which it is rather than report ok.
        return []
    rows = conn.execute(
        """
        SELECT statement, attempts, first_failed_at, last_attempt_at, last_error
          FROM okg.pending_grants
         ORDER BY first_failed_at, statement
        """
    ).fetchall()
    return [
        {
            "statement": row[0],
            "attempts": row[1],
            "first_failed_at": row[2].isoformat() if row[2] else None,
            "last_attempt_at": row[3].isoformat() if row[3] else None,
            "last_error": row[4],
        }
        for row in rows
    ]


def _doctor_pending_grants(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check pending-grants` —

    Pinned by ``schema-grant-coherence`` req.scoped-catchup. The Chronos grant
    catch-up retries a GRANT that loses a ``tuple concurrently updated`` race,
    and used to drop it with a server-log WARNING when the retries exhausted.
    A missing privilege that nobody records resurfaces later as an unrelated
    permission-denied in some other subsystem, so exhaustion now enqueues the
    statement in ``okg.pending_grants``. The queue is drained on the next
    registry-epoch change; anything still in it is reported here.

    ``ok: true`` on an empty queue, and on a database that predates the queue
    table (``queue_present: false`` says which of the two it is).
    """
    from okg.substrate.db.connection import connect as db_connect

    _load_main_helpers()

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    deployment = getattr(args, "deployment", None) or "default"
    with db_connect(dsn, autocommit=True) as conn:
        queue_present = conn.execute(
            "SELECT to_regclass('okg.pending_grants') IS NOT NULL"
        ).fetchone()[0]
        findings = _pending_grant_findings(conn)

    payload = {
        "check": "pending_grants",
        "deployment": deployment,
        "queue_present": bool(queue_present),
        "ok": not findings,
        "pending_count": len(findings),
        "findings": findings,
        "fix_hint": (
            "re-run the Chronos schema ensure (any publish or `okg up`) to "
            "drain the queue; a statement that keeps failing names the "
            "relation whose ACL cannot be updated"
        ) if findings else None,
    }
    _emit_ok(payload, as_json=as_json)
    return 0 if not findings else 1


def _doctor_deferred_edges(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check deferred-edges` -- edges waiting for another source.

    Pinned by cross-source-endpoint-wedge req.observable. A cross-source edge
    whose endpoint is missing, a placeholder, or of a subtype its clause
    does not accept waits in `okg.graph_edge_deferrals` instead of failing a
    run or blocking a publish. Waiting is not a fault, but waiting forever is
    a wrong id: the check warns once the oldest edge is older than
    `consistency.deferred_edge_warn_after` (default 24 hours). It reports
    counts by producer, edge type, endpoint subtype and state, the oldest
    `deferred_at`. It withholds record ids (they can carry sensitive names);
    `okg runtime deferred-edges` lists a bounded sample. An old edge is a
    warning, not a failure: the check exits 0.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.graph_store_internal.edge_deferrals import (
        DEFAULT_WARN_AFTER_SECONDS,
        deferred_edges_report,
    )

    _load_main_helpers()
    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    deployment = getattr(args, "deployment", None) or "default"
    warn_after = DEFAULT_WARN_AFTER_SECONDS
    if getattr(args, "deployment", None):
        from okg.substrate.deployments import find_deployment_dir
        from okg.substrate.publisher.deployment_stack import (
            load_deferral_policy,
        )

        deployment_dir = find_deployment_dir(args.deployment)
        if deployment_dir is not None:
            warn_after = load_deferral_policy(deployment_dir).warn_after_seconds
    with db_connect(dsn, autocommit=True) as conn:
        report = deferred_edges_report(conn, warn_after_seconds=warn_after)
    # The ids are operator-only and stay with `okg runtime deferred-edges`.
    report.pop("sample", None)
    report.pop("sample_note", None)
    # Waiting is not a fault, so an old edge is a warning, not a failure.
    payload = {
        "check": "deferred_edges",
        "deployment": deployment,
        "ok": True,
        **report,
        "warnings": (
            ["oldest_waiting_edge_past_warning_age"] if report["warn"] else []
        ),
        "fix_hint": (
            "the oldest waiting edge is past the warning age: check that the "
            "source that owns the missing records has run, and that the "
            "waiting edges' id prefix is right; `okg runtime deferred-edges` "
            "lists a bounded sample of ids"
        ) if report["warn"] else None,
    }
    _emit_ok(payload, as_json=as_json)
    return 0


def _grant_registry_drift_findings(conn: Any) -> list[dict[str, object]]:
    """Grant-surface drift rows, or [] when the registry is not installed."""
    reg = conn.execute(
        "SELECT to_regprocedure('okg.grant_registry_drift()') IS NOT NULL"
    ).fetchone()
    if not reg or not reg[0]:
        return []
    rows = conn.execute(
        """
        SELECT finding_kind, relation, role_name, detail
          FROM okg.grant_registry_drift()
         ORDER BY finding_kind, relation, role_name
        """
    ).fetchall()
    return [
        {
            "finding_kind": row[0],
            "relation": row[1],
            "role_name": row[2],
            "detail": row[3],
        }
        for row in rows
    ]


def _doctor_grant_registry_drift(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check grant-registry-drift` —

    Pinned by ``schema-grant-coherence`` req.registry-not-convention. The
    app_ro / okg_mcp grant surface is declared in ``okg.grant_registry`` and
    applied by ``okg.apply_grant_registry()``. This reports the four ways the
    live catalog can disagree with that declaration: an ``okg.v_*`` view with
    no registry entry (previously granted by NAME, now unreadable and
    unrecorded), a governed role holding a privilege the registry does not
    list, a relation classified ``internal`` that became readable, and a
    registered privilege that is absent.
    """
    from okg.substrate.db.connection import connect as db_connect

    _load_main_helpers()

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    deployment = getattr(args, "deployment", None) or "default"
    with db_connect(dsn, autocommit=True) as conn:
        registry_present = conn.execute(
            "SELECT to_regprocedure('okg.grant_registry_drift()') IS NOT NULL"
        ).fetchone()[0]
        findings = _grant_registry_drift_findings(conn)

    payload = {
        "check": "grant_registry_drift",
        "deployment": deployment,
        "registry_present": bool(registry_present),
        "ok": not findings,
        "drift_count": len(findings),
        "findings": findings,
        "fix_hint": (
            "add the missing okg.grant_registry row in "
            "src/okg/substrate/db/views.sql (substrate relations) or in the "
            "deployment's own views.sql (source <> 'substrate'), then re-run "
            "`okg up`"
        ) if findings else None,
    }
    _emit_ok(payload, as_json=as_json)
    return 0 if not findings else 1


def _enricher_liveness_findings(
    conn: Any,
    deployment: str,
) -> list[dict[str, object]]:
    """Wired enrichers that have emitted zero edges since wiring.

    Pinned by ``sweep-enricher-liveness`` (#707). A row with
    ``last_status='success'`` has advanced at least once (it is wired),
    yet ``lifetime_edges_emitted = 0`` means it has never produced a
    single edge over its whole lifetime — the days-of-zero-LLM-calls
    blind spot. ``dormant_ok = TRUE`` is the explicit declared-liveness
    escape (design point (i)): a legitimately-quiet enricher is not
    flagged. A ``never``/``failed:%`` row is not "silent" — it either
    has not run or is already surfaced as failing — so neither is
    reported here.
    """
    findings: list[dict[str, object]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT enricher_name, last_run_at, generation_id
              FROM okg.enricher_progress
             WHERE deployment_name = %s
               AND last_status = 'success'
               AND lifetime_edges_emitted = 0
               AND dormant_ok = FALSE
             ORDER BY last_run_at DESC, enricher_name
            """,
            (deployment,),
        )
        for row in cur.fetchall():
            findings.append({
                "enricher_name": str(row[0]),
                "last_run_at": row[1].isoformat() if row[1] else None,
                "generation_id": row[2],
                "reason": "zero_lifetime_emissions_since_wiring",
            })
    return findings


def _enricher_failure_findings(
    conn: Any,
    deployment: str,
) -> list[dict[str, object]]:
    """Return unresolved failed/incomplete enricher progress rows."""
    findings: list[dict[str, object]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT enricher_name, last_run_at, generation_id, last_status,
                   consecutive_failure_count
              FROM okg.enricher_progress
             WHERE deployment_name = %s
               AND last_status LIKE 'failed:%%'
             ORDER BY last_run_at DESC, enricher_name
            """,
            (deployment,),
        )
        for row in cur.fetchall():
            status = str(row[3])
            findings.append({
                "enricher_name": str(row[0]),
                "last_run_at": row[1].isoformat() if row[1] else None,
                "compat_generation_id": int(row[2]),
                "last_status": status,
                "exception_class": status.partition(":")[2] or "unknown",
                "consecutive_failure_count": int(row[4]),
                "reason": "enricher_failed",
            })
    return findings


def _enricher_quarantine_findings(
    conn: Any,
    deployment: str,
) -> list[dict[str, object]]:
    """Return every active item/signature quarantine as an unhealthy finding."""
    findings: list[dict[str, object]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT branch_name, enricher_name, item_ref, input_signature,
                   attempt_count, attempt_limit, last_failure_class,
                   last_error, first_failed_at, last_failed_at,
                   quarantined_at, last_generation_id
              FROM okg.enricher_input_quarantines
             WHERE deployment_name = %s
               AND status = 'active'
             ORDER BY last_failed_at DESC, branch_name, enricher_name, item_ref
            """,
            (deployment,),
        )
        for row in cur.fetchall():
            findings.append(
                {
                    "deployment_name": deployment,
                    "branch_name": str(row[0]),
                    "enricher_name": str(row[1]),
                    "item_ref": str(row[2]),
                    "input_signature": str(row[3]),
                    "attempt_count": int(row[4]),
                    "attempt_limit": int(row[5]),
                    "failure_class": str(row[6] or "unknown"),
                    "error": str(row[7] or ""),
                    "first_failed_at": row[8].isoformat() if row[8] else None,
                    "last_failed_at": row[9].isoformat() if row[9] else None,
                    "quarantined_at": row[10].isoformat() if row[10] else None,
                    "generation_id": row[11],
                    "reason": "enricher_input_quarantined",
                }
            )
    return findings


def _doctor_enricher_liveness(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check enricher-liveness --deployment <name>` —

    Reports both wired-but-silent enrichers (#707) and unresolved failed
    or incomplete runs (#839). ``dormant_ok`` suppresses only legitimate
    zero-output silence; it never suppresses a crash.
    """
    from okg.substrate.db.connection import connect as db_connect

    _load_main_helpers()

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    deployment = getattr(args, "deployment", None) or "default"
    try:
        with db_connect(dsn, autocommit=True) as conn:
            silent_findings = _enricher_liveness_findings(conn, deployment)
            failed_findings = _enricher_failure_findings(conn, deployment)
            quarantine_findings = _enricher_quarantine_findings(conn, deployment)
    except Exception as exc:  # noqa: BLE001 - doctor must fail loud, not green
        return _emit_error(
            SubstrateError(
                f"enricher-liveness check failed: {exc}",
                fix_hint="verify the deployment DSN and enricher_progress schema",
            ),
            as_json=as_json,
        )

    findings = [*quarantine_findings, *failed_findings, *silent_findings]

    payload = {
        "check": "enricher_liveness",
        "deployment": deployment,
        "ok": not findings,
        "failed_count": len(failed_findings),
        "quarantine_count": len(quarantine_findings),
        "silent_count": len(silent_findings),
        "findings": findings,
    }
    _emit_ok(payload, as_json=as_json)
    return 0 if not findings else 1


def _derived_admission_findings(
    conn: Any,
    deployment: str,
) -> list[dict[str, object]]:
    """Derived producers whose admitted output was rejected outright.

    Pinned by PACT ``surface-derived-admission-rejections``. A rejected
    derived producer does not block or degrade the publish — the cycle
    still reports ``published`` with exit code 0 — so the row counts
    simply do not move and the only durable trace is an
    ``okg.admission_envelopes`` row. That made a wholly-discarded
    enricher indistinguishable from a quiet one until an operator
    already suspected it and went looking.

    Distinct from ``enricher-liveness``, which keys off
    ``lifetime_edges_emitted = 0``: a producer that emits every cycle and
    has every cycle rejected has a nonzero lifetime count and is invisible
    to that check. Distinct from the ``admission`` check, which audits
    envelope well-formedness — a cleanly-formed rejected envelope passes
    every one of its findings.

    Reports a producer only when its MOST RECENT decision is a discard,
    rather than every discard in history. ``okg.admission_envelopes`` is
    append-only for the rows this reads, so an unbounded query would leave
    the check red forever after a single bad cycle, with no operator path
    back to green short of deleting rows — and a check that is always red
    is one nobody reads. Latest-decision scoping answers the question an
    operator actually has ("is this producer's output being discarded
    now?") with no time-window heuristic: a producer fixed in a later
    cycle drops out on its next admission, one still failing stays.

    Covers ``failed`` and ``quarantined`` as well as ``rejected``: the
    derived-fact loader excludes all three identically, so all three
    discard the producer's whole output.

    Latest is scoped per (producer, INTENT). A derived producer holds
    several kinds of envelope in one cycle -- the emission decision and the
    ``publisher_derived:*`` fact-application pass -- and scoping to the
    producer alone let a later accepted envelope of another intent hide a
    refused emission, so the check reported nothing for a producer whose
    output had just been refused.
    """
    findings: list[dict[str, object]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (producer_id, intent)
                   producer_id,
                   producer_kind,
                   status,
                   verdict->>'code' AS code,
                   coalesce(
                     CASE WHEN verdict->>'record_count' ~ '^[0-9]+$'
                          THEN (verdict->>'record_count')::bigint
                     END, 0
                   ) AS discarded_record_count,
                   coalesce(decided_at, updated_at, created_at) AS decided,
                   CASE WHEN verdict->>'offending_record_count'
                             ~ '^[0-9]+$'
                        THEN (verdict->>'offending_record_count')::bigint
                   END AS offending_record_count,
                   CASE WHEN verdict->>'collateral_record_count'
                             ~ '^[0-9]+$'
                        THEN (verdict->>'collateral_record_count')::bigint
                   END AS collateral_record_count,
                   verdict->>'message' AS message,
                   intent
              FROM okg.admission_envelopes
             WHERE deployment_name = %s
               AND producer_kind IN ('enricher', 'extractor', 'lens')
             ORDER BY producer_id, intent,
                      coalesce(decided_at, updated_at, created_at)
                        DESC NULLS LAST,
                      admission_id
            """,
            (deployment,),
        )
        rows = cur.fetchall()
    for row in rows:
        status = str(row[2])
        if status not in ("rejected", "failed", "quarantined"):
            continue
        findings.append({
            "producer_id": str(row[0]),
            "producer_kind": str(row[1]),
            "status": status,
            "code": str(row[3]) if row[3] is not None else None,
            "discarded_record_count": int(row[4] or 0),
            "last_discarded_at": row[5].isoformat() if row[5] else None,
            # Issue #1330: a whole-emission refusal caused by k bad records
            # discards the admissible remainder too. Both numbers are read
            # from the verdict rather than inferred; a verdict written before
            # this change carries neither, and reports None rather than a
            # fabricated zero.
            "offending_record_count": (
                int(row[6]) if row[6] is not None else None
            ),
            "collateral_record_count": (
                int(row[7]) if row[7] is not None else None
            ),
            "message": str(row[8]) if row[8] is not None else None,
            "intent": str(row[9]),
            "reason": "derived_output_discarded_at_admission",
        })
    return findings


def _derived_admission_producer_inventory(
    conn: Any,
    deployment: str,
) -> list[dict[str, object]]:
    """Every derived producer with what it emitted and what admission did.

    Issue #1331. The findings list above reports discards only, so a producer
    whose envelopes were all accepted is absent from it -- and so is a
    producer that emitted nothing at all, because no emission means no
    envelope. That made "this enricher derived nothing" and "this enricher
    produced output that never landed" the same observation: silence.

    Scope and honesty rules, each of which is a defect if broken:

    * Every emission envelope on the default branch is AGGREGATED, not just
      the latest. The publisher writes one envelope per (source, run_id, fact
      range), so a producer routinely holds several per cycle; reading only
      the newest lets an accepted sibling hide a rejected one and reports a
      single range count as the producer total.
    * Only the two EMISSION intents are read. ``publisher_derived:*``
      envelopes record the fact-application pass, carry no emitted counts,
      and would report every producer as counts-unrecorded.
    * A count the envelope does not carry is reported as unrecorded, never as
      zero: envelopes written before this change have no counts, and the
      producer state says so instead of claiming an emission of nothing.
    * ``admitted`` is what ADMISSION decided. Under Nomos ``audit``/``off``
      the derived-fact loader projects a refused producer's output anyway, so
      a refusal there did not discard anything; the recorded ``gating_mode``
      separates the two and the state names which one happened.
    * ``lifetime_edges_emitted`` from ``okg.enricher_progress`` covers the
      enrichers that ran and emitted nothing, which have no envelope to read.
      A producer with neither an envelope nor a progress row is not listed.

    This inventory does NOT prove that admitted output reached the served
    graph; it reports the admission ledger. The state names say
    ``at_admission`` for that reason.
    """
    inventory: dict[str, dict[str, object]] = {}

    def _entry(producer_id: str, producer_kind: str) -> dict[str, object]:
        return inventory.setdefault(producer_id, {
            "producer_id": producer_id,
            "producer_kind": producer_kind,
            "envelope_count": 0,
            "intents": [],
            "emitted_record_count": 0,
            "emitted_node_record_count": 0,
            "emitted_edge_record_count": 0,
            "admitted_record_count": 0,
            "rejected_record_count": 0,
            "counts_complete": True,
            "gating_modes": [],
            "lifetime_edges_emitted": None,
            "last_status": None,
        })

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT producer_id,
                   producer_kind,
                   intent,
                   status,
                   verdict->>'code' AS code,
                   CASE WHEN verdict->'admission_record_counts'->>'emitted'
                             ~ '^[0-9]+$'
                        THEN (verdict->'admission_record_counts'
                              ->>'emitted')::bigint
                   END AS emitted,
                   CASE WHEN verdict->'admission_record_counts'->>'admitted'
                             ~ '^[0-9]+$'
                        THEN (verdict->'admission_record_counts'
                              ->>'admitted')::bigint
                   END AS admitted,
                   CASE WHEN verdict->'admission_record_counts'->>'rejected'
                             ~ '^[0-9]+$'
                        THEN (verdict->'admission_record_counts'
                              ->>'rejected')::bigint
                   END AS rejected,
                   coalesce(decided_at, updated_at, created_at) AS decided,
                   CASE WHEN verdict->>'offending_record_count' ~ '^[0-9]+$'
                        THEN (verdict->>'offending_record_count')::bigint
                   END AS offending,
                   CASE WHEN verdict->>'collateral_record_count' ~ '^[0-9]+$'
                        THEN (verdict->>'collateral_record_count')::bigint
                   END AS collateral,
                   CASE WHEN check_summary->'emitted_record_counts'->>'node'
                             ~ '^[0-9]+$'
                        THEN (check_summary->'emitted_record_counts'
                              ->>'node')::bigint
                   END AS emitted_nodes,
                   CASE WHEN check_summary->'emitted_record_counts'->>'edge'
                             ~ '^[0-9]+$'
                        THEN (check_summary->'emitted_record_counts'
                              ->>'edge')::bigint
                   END AS emitted_edges,
                   verdict->'admission_record_counts'->>'gating_mode'
                     AS gating_mode
              FROM okg.admission_envelopes
             WHERE deployment_name = %s
               AND producer_kind IN ('enricher', 'extractor', 'lens')
               AND intent IN (
                     'derived_edge_emission', 'derived_node_emission'
                   )
               AND coalesce(target_branch_name, 'default') = 'default'
             ORDER BY producer_id, intent,
                      coalesce(decided_at, updated_at, created_at)
                        DESC NULLS LAST,
                      admission_id
            """,
            (deployment,),
        )
        envelope_rows = cur.fetchall()
        cur.execute(
            """
            SELECT enricher_name, lifetime_edges_emitted, last_status
              FROM okg.enricher_progress
             WHERE deployment_name = %s
            """,
            (deployment,),
        )
        progress_rows = cur.fetchall()

    for row in envelope_rows:
        entry = _entry(str(row[0]), str(row[1]))
        emitted = None if row[5] is None else int(row[5])
        admitted = None if row[6] is None else int(row[6])
        rejected = None if row[7] is None else int(row[7])
        emitted_nodes = None if row[11] is None else int(row[11])
        emitted_edges = None if row[12] is None else int(row[12])
        gating_mode = None if row[13] is None else str(row[13])
        if emitted is None or admitted is None or rejected is None:
            entry["counts_complete"] = False
        entry["envelope_count"] = int(entry["envelope_count"]) + 1
        entry["emitted_record_count"] = (
            int(entry["emitted_record_count"]) + (emitted or 0)
        )
        entry["emitted_node_record_count"] = (
            int(entry["emitted_node_record_count"]) + (emitted_nodes or 0)
        )
        entry["emitted_edge_record_count"] = (
            int(entry["emitted_edge_record_count"]) + (emitted_edges or 0)
        )
        entry["admitted_record_count"] = (
            int(entry["admitted_record_count"]) + (admitted or 0)
        )
        entry["rejected_record_count"] = (
            int(entry["rejected_record_count"]) + (rejected or 0)
        )
        modes = entry["gating_modes"]
        assert isinstance(modes, list)
        if gating_mode is not None and gating_mode not in modes:
            modes.append(gating_mode)
        intents = entry["intents"]
        assert isinstance(intents, list)
        intents.append({
            "intent": str(row[2]),
            "status": str(row[3]),
            "code": str(row[4]) if row[4] is not None else None,
            "emitted_record_count": emitted,
            "admitted_record_count": admitted,
            "rejected_record_count": rejected,
            "decided_at": row[8].isoformat() if row[8] else None,
            # Issue #1330: on a refusal, how many records offended and how
            # many admissible ones were discarded with them. None on an
            # accepted envelope and on any verdict written before this change.
            "offending_record_count": (
                None if row[9] is None else int(row[9])
            ),
            "collateral_record_count": (
                None if row[10] is None else int(row[10])
            ),
            "gating_mode": gating_mode,
        })

    for name, lifetime, last_status in progress_rows:
        entry = _entry(f"_enricher:{name}", "enricher")
        entry["lifetime_edges_emitted"] = (
            None if lifetime is None else int(lifetime)
        )
        entry["last_status"] = (
            None if last_status is None else str(last_status)
        )

    for entry in inventory.values():
        entry["state"] = _derived_producer_output_state(entry)
    return sorted(
        inventory.values(),
        key=lambda entry: str(entry["producer_id"]),
    )


def _derived_producer_output_state(entry: dict[str, object]) -> str:
    """Name what admission did with one producer's output.

    Every branch is reached from a measured value. Nothing here infers an
    emission of zero from an absence: a producer with no emission envelope is
    only called ``emitted_nothing`` when its own progress row measures zero
    lifetime output, and a producer whose envelopes predate the counts is
    called ``counts_unrecorded`` rather than assigned a number.

    ``withheld_source_held_for_admission`` (issue #1528) is read FIRST, and only
    from the reason the publisher wrote into ``enricher_progress.last_status``.
    It is not inferred from silence: the publisher withheld this enricher
    because a declared input source run is still held off `default`, so it did
    not run and admission decided nothing about it this cycle. That is a
    different fact from ``emitted_nothing`` -- which says the enricher DID run
    and derived nothing -- and the two must not be reported the same way, which
    is exactly the confusion #1330 turned into 111,940 orphaned records. Any
    envelopes on the entry are from earlier cycles and would otherwise report
    the stale ``output_admitted``.
    """
    # Issue #1528: the publisher (writer) and this reader share one vocabulary
    # for a withheld run, so the two cannot drift apart. Imported here rather
    # than at module scope to keep this CLI's import cost lazy, as the rest of
    # the file does.
    from okg.substrate.enrichers.lineage import withheld_reason_from_status

    withheld = withheld_reason_from_status(entry.get("last_status"))
    if withheld is not None:
        # The reason code itself IS the state name, so a new withholding
        # reason (issue #1528 F5's `unknown_declared_source`) surfaces here
        # without another branch.
        return withheld[0]
    emitted = int(entry["emitted_record_count"] or 0)
    admitted = int(entry["admitted_record_count"] or 0)
    lifetime = entry["lifetime_edges_emitted"]
    if not entry["counts_complete"]:
        return "counts_unrecorded"
    if not entry["envelope_count"]:
        if lifetime is None:
            return "no_admission_record"
        if int(lifetime) > 0:
            # It emitted, and no emission envelope explains where that went.
            return "emitted_without_admission_record"
        return "emitted_nothing"
    if emitted == 0:
        return "emitted_nothing"
    if admitted == 0:
        # Refused whole. Under audit/off the loader projects it anyway, so
        # the refusal withheld nothing and saying "discarded" would be false.
        modes = entry["gating_modes"]
        assert isinstance(modes, list)
        if modes and all(str(mode) != "enforce" for mode in modes):
            return "refused_at_admission_not_gated"
        return "output_discarded_at_admission"
    if admitted < emitted:
        return "output_partially_discarded_at_admission"
    return "output_admitted"


def _withheld_enricher_findings(
    conn: Any,
    deployment: str,
) -> list[dict[str, object]]:
    """Every enricher the publisher WITHHELD, with its release path.

    Issue #1528 F3. `_doctor_derived_admission` keys `ok` off rejections
    only, so before this a withheld enricher exited 0 forever: the producer
    inventory named the state but nothing said WHICH source was holding it or
    what an operator could do, and `okg doctor` was effectively silent on a
    producer that had stopped contributing.

    These are ADVISORY, matching how the same check already treats the
    emitted/admitted inventory (issue #1331: emitting nothing is a legitimate
    outcome and does not change the verdict). Withholding is likewise a
    CORRECT outcome -- the gate doing its job -- not a fault, so it is
    reported without reddening the check. That is deliberately unlike
    `_enricher_failure_findings`, which reads `last_status LIKE 'failed:%'`
    and DOES red `enricher-liveness`: those rows record a producer that
    broke. A withheld row is excluded from that query by construction,
    because the publisher writes the disjoint `withheld:%` prefix.

    The release path is stated per finding rather than as one generic
    sentence, because for most holds `okg admission decide` cannot work --
    see `console_views/enricher_diagnostics._withheld_next_check`.
    """
    from okg.substrate.admission import admission_mode_awaits_decision
    from okg.substrate.enrichers.lineage import (
        UNKNOWN_DECLARED_SOURCE,
        withheld_reason_from_status,
    )

    rows: list[tuple] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT enricher_name, last_run_at, generation_id, last_status
              FROM okg.enricher_progress
             WHERE deployment_name = %s
               AND last_status LIKE 'withheld:%%'
             ORDER BY last_run_at DESC, enricher_name
            """,
            (deployment,),
        )
        rows = cur.fetchall()
    if not rows:
        return []

    held_sources: set[str] = set()
    parsed: list[tuple[str, Any, int, str, str, tuple[str, ...]]] = []
    for row in rows:
        found = withheld_reason_from_status(str(row[3]))
        reason, detail = found if found else ("withheld_unknown_reason", "")
        sources = tuple(s for s in detail.split(",") if s)
        held_sources.update(sources)
        parsed.append(
            (str(row[0]), row[1], int(row[2]), str(row[3]), reason, sources),
        )

    states: dict[str, tuple[str, str]] = {}
    if held_sources:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (source) source, admission_mode, status
                  FROM okg.admission_envelopes
                 WHERE deployment_name = %(deployment)s
                   AND intent = 'source_sync'
                   AND source = ANY(%(sources)s)
                 ORDER BY source, created_at DESC, admission_id DESC
                """,
                {"deployment": deployment, "sources": sorted(held_sources)},
            )
            states = {
                str(r[0]): (str(r[1]), str(r[2])) for r in cur.fetchall()
            }

    findings: list[dict[str, object]] = []
    for name, last_run_at, generation_id, last_status, reason, sources in parsed:
        if reason == UNKNOWN_DECLARED_SOURCE:
            release_path = (
                "fix `reads_sources` on this enricher to name a source that "
                "is registered in this deployment's source_registry.yaml "
                "(its registry name or its ownership_id)"
            )
        else:
            # A `pending` envelope in either held mode is decidable: ingest
            # finalizes review- and quarantine-mode runs `pending` (#1752).
            decidable = sorted(
                s for s in sources
                if s in states
                and admission_mode_awaits_decision(states[s][0])
                and states[s][1] == "pending"
            )
            wedged = sorted(s for s in sources if s not in decidable)
            parts = []
            if decidable:
                parts.append(
                    "run `okg admission decide <admission-id> accept` for "
                    + ", ".join(decidable)
                )
            if wedged:
                parts.append(
                    "for " + ", ".join(wedged) + " the admission envelope is "
                    "NOT decidable (`okg admission decide` returns "
                    "already_decided); the hold clears only when a later "
                    "sync of that source lands in a non-held admission mode, "
                    "or when the run is rejected"
                )
            release_path = "; ".join(parts) or (
                "no held source was recorded on the status; inspect "
                "okg.admission_envelopes for this deployment"
            )
        findings.append({
            "enricher_name": name,
            "last_run_at": last_run_at.isoformat() if last_run_at else None,
            "compat_generation_id": generation_id,
            "last_status": last_status,
            "held_sources": list(sources),
            "held_source_states": {
                s: {"admission_mode": states[s][0], "status": states[s][1]}
                for s in sources if s in states
            },
            "release_path": release_path,
            "severity": "warning",
            "reason": reason,
        })
    return findings


def _doctor_derived_admission(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check derived-admission --deployment <name>` —

    Pinned by PACT ``surface-derived-admission-rejections``. Reports
    ``ok: true`` when every derived producer's output was admitted, and
    ``ok: false`` with one finding per rejected (producer, code) pair
    otherwise. This is the check that answers "the publish was green but
    the numbers did not move".

    Also reports, WITHOUT changing the verdict, two advisory sections:
    ``producers`` (issue #1331, what each derived producer emitted and what
    admission did with it) and ``withheld`` (issue #1528, every enricher the
    publisher declined to run because a declared input source is still held
    off `default`, with the source, its admission state, and the release
    path). Both describe legitimate outcomes rather than faults, so both
    leave ``ok`` alone -- but neither is silent, which is the point.
    """
    from okg.substrate.db.connection import connect as db_connect

    _load_main_helpers()

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    deployment = getattr(args, "deployment", None) or "default"
    with db_connect(dsn, autocommit=True) as conn:
        findings = _derived_admission_findings(conn, deployment)
        producers = _derived_admission_producer_inventory(conn, deployment)
        withheld = _withheld_enricher_findings(conn, deployment)

    payload = {
        "check": "derived_admission",
        "deployment": deployment,
        # Issue #1528 F3: `withheld` is ADVISORY and deliberately does NOT
        # move `ok`, matching the issue-#1331 rule for the producer inventory
        # below -- a withheld enricher is the gate working, not a fault. It
        # is reported so the check is not silent about a producer that has
        # stopped contributing, and each entry names its own release path.
        "ok": not findings,
        "rejected_count": len(findings),
        "findings": findings,
        "withheld_count": len(withheld),
        "withheld": withheld,
        # Issue #1331: the emitted/admitted inventory is reported alongside
        # the discards, and does NOT change the check verdict -- emitting
        # nothing is a legitimate outcome, not a failure.
        "producers": producers,
    }
    _emit_ok(payload, as_json=as_json)
    return 0 if not findings else 1


_NOMOS_DECISION_CHILD_TABLES: tuple[tuple[str, str, str, str], ...] = (
    # (table_name, identifying_column, deployment_column, decision_column)
    (
        "federation_connection_requests",
        "request_id",
        "nomos_decision_deployment_name",
        "nomos_decision_id",
    ),
    (
        "federation_peer_trust_pins",
        "key_fingerprint",
        "nomos_decision_deployment_name",
        "nomos_decision_id",
    ),
    (
        "federation_route_events",
        "route_event_id",
        "nomos_decision_deployment_name",
        "nomos_decision_id",
    ),
    (
        "poros_incorporation_events",
        "event_id",
        "nomos_decision_deployment_name",
        "nomos_decision_id",
    ),
    ("nomos_external_claims", "claim_row_id", "deployment_name", "nomos_decision_id"),
    # External-action authenticated evidence is part of the same
    # deployment-scoped decision-retention contract.  Keep this check here so
    # a pre-migration/partially restored database reports an orphan instead of
    # allowing review or restore state to outlive its Nomos authority.
    ("external_action_auth_audits", "authentication_audit_id", "deployment_name", "nomos_decision_id"),
    ("external_action_decisions", "decision_row_id", "deployment_name", "nomos_decision_id"),
    ("external_action_transition_idempotency", "transition_idempotency_id", "deployment_name", "nomos_decision_id"),
    ("external_action_executions", "execution_id", "deployment_name", "nomos_decision_id"),
    ("external_action_outcomes", "outcome_id", "deployment_name", "nomos_decision_id"),
    ("external_action_events", "event_id", "deployment_name", "nomos_decision_ref"),
    ("external_action_owner_unavailability_attestations", "attestation_id", "deployment_name", "nomos_decision_id"),
    ("external_action_execution_epoch_leases", "lease_row_id", "deployment_name", "nomos_decision_id"),
    ("external_action_restore_attestations", "attestation_row_id", "deployment_name", "nomos_decision_id"),
    ("external_action_restore_sessions", "restore_session_row_id", "deployment_name", "nomos_decision_id"),
)


def _doctor_nomos_decision_referential_integrity(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check nomos-decision-referential-integrity` —

    Pinned by ``add-nomos-decision-id-referential-integrity``. Joins
    each of the 5 child tables that carry ``nomos_decision_id``
    against ``okg.nomos_decisions`` and reports any orphans (child
    rows whose deployment-scoped ``nomos_decision_id`` doesn't resolve
    to a parent).
    """
    from okg.substrate.db.connection import connect as db_connect

    _load_main_helpers()

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    findings: list[dict[str, object]] = []
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for table, pk, deployment_column, decision_column in _NOMOS_DECISION_CHILD_TABLES:
            cur.execute(
                f"""
                SELECT child.{deployment_column}, child.{decision_column},
                       child.{pk}
                  FROM okg.{table} child
             LEFT JOIN okg.nomos_decisions parent
                    ON parent.deployment_name = child.{deployment_column}
                   AND parent.decision_id = child.{decision_column}
                 WHERE child.{decision_column} IS NOT NULL
                   AND parent.decision_id IS NULL
                 ORDER BY child.{deployment_column}, child.{decision_column}
                 LIMIT 200
                """
            )
            for row in cur.fetchall():
                findings.append({
                    "child_table": f"okg.{table}",
                    "deployment_name": row[0],
                    "nomos_decision_id": row[1],
                    "child_pk": str(row[2]),
                })

    payload = {
        "check": "nomos_decision_referential_integrity",
        "ok": not findings,
        "orphan_count": len(findings),
        "findings": findings,
    }
    _emit_ok(payload, as_json=as_json)
    return 0 if not findings else 1


def _doctor_honest_reads(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check honest-reads --deployment <name>` —

    Pinned by ``sweep-honest-reads`` (#706). Reconciles every
    Chronos-registered maintained-derivation kept table (those carrying a
    ``v_current_<table>`` honest-read view) by comparing its bare base-table
    count against a Chronos checkout read of the latest published generation.
    Reports ``ok: false`` when a base count diverges from the checkout count
    (a vestigial never-materialized surface) yet the honest-read view failed
    to refuse the bare read -- the #706 wrong-lens where a reader silently
    sees 0 while a checkout serves the real rows.
    """
    _load_main_helpers()
    from okg.substrate.db.management import honest_read_reconciliation_report

    try:
        deployment = getattr(args, "deployment", None)
        dsn = _resolve_deployment_dsn(args) if deployment else _resolve_dsn(args)
        payload = honest_read_reconciliation_report(
            dsn, deployment_name=deployment
        )
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok", False) else 1


def _doctor_check_handler(handler):
    def _run(args: argparse.Namespace, *, context: Any, as_json: bool) -> int:
        return handler(args, as_json=as_json)

    return _run


def _doctor_check_handler_no_args(handler):
    def _run(args: argparse.Namespace, *, context: Any, as_json: bool) -> int:
        return handler(as_json=as_json)

    return _run


def _build_doctor_registry(args: argparse.Namespace):
    _load_main_helpers()
    from okg.substrate.doctor import (
        DoctorCheckRegistry,
        load_deployment_doctor_checks,
    )

    registry = DoctorCheckRegistry()
    registry.register(
        "dsn-agreement",
        _doctor_check_handler(_doctor_dsn_agreement),
        help=(
            "verify local.env, deployment authority, MCP registrations, "
            "and running MCP servers all resolve the same database"
        ),
    )
    registry.register(
        "alias-collisions",
        _doctor_check_handler(_doctor_alias_collisions),
        help="warn when ambiguous alias values have been refused",
    )
    registry.register(
        "declared-attr-indexes",
        _doctor_check_handler(_doctor_declared_attr_indexes),
        help=(
            "check the declared-attribute expression indexes: lifecycle, "
            "invalid indexes, failure cap, view-graph drift, collation"
        ),
    )
    registry.register(
        "mcp-surface-freshness",
        _doctor_check_handler(_doctor_mcp_surface_freshness),
        help=(
            "probe registered OKG MCP servers and fail on stale, unknown, "
            "or trust-contract-missing instruction surfaces"
        ),
    )
    registry.register(
        "extraction-wrap-invariant",
        _doctor_check_handler_no_args(_doctor_extraction_wrap),
        help="lint the extraction library wrapper invariant",
    )
    registry.register(
        "extraction",
        _doctor_check_handler(_doctor_extraction),
        help="import-check extraction backends and configured models",
    )
    registry.register(
        "extractors",
        _doctor_check_handler(_doctor_extractors),
        help="pre-instantiate deployment extractors.yaml classes",
    )
    registry.register(
        "sources",
        _doctor_check_handler(_doctor_sources),
        help="pre-instantiate deployment source_registry.yaml adapters",
    )
    registry.register(
        "admission",
        _doctor_check_handler(_doctor_admission),
        help="audit admission-envelope conformance to the formal model",
    )
    registry.register(
        "branches",
        _doctor_check_handler(_doctor_branches),
        help="verify AKMON branch invariants",
    )
    registry.register(
        "lens-promotions",
        _doctor_check_handler(_doctor_lens_promotions),
        help="warn-only audit of committed lens overlays awaiting promotion",
    )
    registry.register(
        "integrity",
        lambda a, *, context, as_json: _doctor_integrity(
            a, as_json=as_json
        ),
        help="audit bitemporal generation contracts",
    )
    registry.register(
        "deployment-identity",
        _doctor_check_handler(_doctor_deployment_identity),
        help=(
            "warn on ownership rows whose manifest fingerprint predates "
            "identity binding; name the one-command backfill"
        ),
    )
    registry.register(
        "integrity-coverage",
        _doctor_check_handler(_doctor_integrity_coverage),
        help=(
            "report consistency-check coverage by integrity kind "
            "(rule_based/security/statistical/semantic)"
        ),
    )
    registry.register(
        "acceptance-criteria",
        _doctor_check_handler(_doctor_acceptance_criteria),
        help="report OpenSpec criteria without deterministic test evidence",
    )
    registry.register(
        "edge-fact-strandings",
        _doctor_check_handler(_doctor_edge_fact_strandings),
        help="surface op=I edge_facts stranded by the projector",
    )
    registry.register(
        "agent-skills",
        _doctor_check_handler(_doctor_agent_skills),
        help="compare installed repo-local agent skills against the bundled canon",
    )
    registry.register(
        "agent-runtimes",
        _doctor_check_handler(_doctor_agent_runtimes),
        help=(
            "check each agent runtime's saved okg command still exists and "
            "is not inside uv's cache"
        ),
    )
    registry.register(
        "source-auth",
        _doctor_check_handler(_doctor_source_auth),
        help="run optional source auth/cache preflight hooks",
    )
    registry.register(
        "source-health",
        _doctor_check_handler(_doctor_source_health),
        help=(
            "per-source health matrix: imports, registration, test "
            "coverage, live-tier gates (per source-health-matrix)"
        ),
    )
    registry.register(
        "listener-exposure",
        _doctor_check_handler(_doctor_listener_exposure),
        help=(
            "fail on substrate-owned non-loopback unauthenticated "
            "TCP listeners (per harden-connector-route-vetting)"
        ),
    )
    registry.register(
        "egress-validator-coverage",
        _doctor_check_handler(_doctor_egress_coverage),
        help=(
            "fail on a substrate outbound HTTP sink that does not route "
            "through the central egress validator (per "
            "centralize-egress-url-validation)"
        ),
    )
    registry.register(
        "token-redaction",
        _doctor_check_handler(_doctor_token_redaction),
        help=(
            "probe credentialed-source read surfaces for keyring "
            "token material (per harden-connector-route-vetting)"
        ),
    )
    registry.register(
        "external-live-authority",
        _doctor_check_handler(_doctor_external_live_authority),
        help="validate external-live tool authority profile coverage",
    )
    registry.register(
        "formal-assurance",
        _doctor_check_handler(_doctor_formal_assurance),
        help="verify formal model, counterexample, and refinement evidence",
    )
    registry.register(
        "vector-sets",
        _doctor_check_handler(_doctor_vector_sets),
        help=(
            "per vector set: coverage at the published generation, index "
            "state, spend, file-versus-database drift; old-column vectors "
            "no set owns"
        ),
    )
    registry.register(
        "database",
        _doctor_check_handler(_doctor_database),
        help="inspect database storage, runtime backlog, and migration drift",
    )
    registry.register(
        "disk",
        _doctor_check_handler(_doctor_disk),
        help="disk-usage report: sizes, dead-tuple %, compressed-chunk ratio, "
             "unused indexes, indexes larger than their table, "
             "marginal bytes/generation",
    )
    registry.register(
        "host-headroom",
        _doctor_check_handler(_doctor_host_headroom),
        help="host filesystem headroom vs the deployment's declared "
             "host_storage thresholds (per bound-host-storage-pressure)",
    )
    registry.register(
        "code-drift",
        _doctor_check_handler(_doctor_code_drift),
        help="deployed-code drift: is the sync_root's revision merged into the "
             "base ref, and does it track a branch",
    )
    registry.register(
        "preflight",
        _doctor_check_handler(_doctor_preflight),
        help="bootstrap-readiness in one report: required extensions, schema, catalog",
    )
    registry.register(
        "nomos",
        _doctor_check_handler(_doctor_nomos),
        help="validate Nomos policy, identity schema, and IAM drift",
    )
    registry.register(
        "post-publish-projection",
        _doctor_check_handler(_doctor_post_publish_projection),
        help=(
            "surface failed post-publish view-edge projections "
            "(per surface-post-publish-projection-failures)"
        ),
    )
    registry.register(
        "pending-grants",
        _doctor_check_handler(_doctor_pending_grants),
        help=(
            "surface grants the Chronos catch-up could not apply and queued "
            "for retry, instead of losing them to a server-log warning "
            "(per schema-grant-coherence)"
        ),
    )
    registry.register(
        "deferred-edges",
        _doctor_check_handler(_doctor_deferred_edges),
        help=(
            "report cross-source edges waiting for another source's record "
            "by producer, edge type, endpoint subtype and state, and warn "
            "when the oldest is past consistency.deferred_edge_warn_after "
            "(per cross-source-endpoint-wedge)"
        ),
    )
    registry.register(
        "grant-registry-drift",
        _doctor_check_handler(_doctor_grant_registry_drift),
        help=(
            "compare the live app_ro / okg_mcp privilege surface against "
            "okg.grant_registry: unregistered v_ views, unregistered grants, "
            "exposed internal relations, missing grants "
            "(per schema-grant-coherence)"
        ),
    )
    registry.register(
        "enricher-liveness",
        _doctor_check_handler(_doctor_enricher_liveness),
        help=(
            "flag failed enrichers and wired successful enrichers that have "
            "emitted zero edges since wiring without dormant_ok"
        ),
    )
    registry.register(
        "derived-admission",
        _doctor_check_handler(_doctor_derived_admission),
        help=(
            "flag derived producers (enricher / extractor / lens) whose "
            "output was rejected at admission, so a green publish that "
            "silently discarded a producer's whole output is reachable "
            "without already suspecting it "
            "(per surface-derived-admission-rejections)"
        ),
    )
    registry.register(
        "honest-reads",
        _doctor_check_handler(_doctor_honest_reads),
        help=(
            "reconcile Chronos-registered kept tables (claim_effective / "
            "claim_projection) against a checkout read; fail on a vestigial "
            "base surface that returns a confident count instead of refusing"
        ),
    )
    registry.register(
        "nomos-decision-referential-integrity",
        _doctor_check_handler(_doctor_nomos_decision_referential_integrity),
        help=(
            "surface orphaned nomos_decision_id references across the 5 "
            "child tables (per add-nomos-decision-id-referential-integrity)"
        ),
    )
    registry.extend(
        load_deployment_doctor_checks(getattr(args, "deployment", None))
    )
    return registry


def _doctor_agent_skills(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    from pathlib import Path

    from okg.substrate.pact.agent_install import agent_skill_freshness

    payload = agent_skill_freshness(Path.cwd())
    payload["check"] = "agent-skills"
    _emit_ok(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


def _doctor_agent_runtimes(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg doctor --check agent-runtimes`: each agent runtime's saved okg
    command still exists, is executable, and is not inside uv's cache."""
    _load_main_helpers()
    from okg.substrate.library.agent_runtimes.saved_commands import (
        saved_command_report,
    )

    payload = saved_command_report()
    _emit_ok(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


# `_cmd_doctor` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_profile_diagnostics` moved to `cli/doctor.py` by `extract-cli-doctor`.


__all__ = [
    "_build_doctor_registry",
    "_doctor_database",
    "_doctor_alias_collisions",
    "_doctor_declared_attr_indexes",
    "_doctor_mcp_surface_freshness",
    "_doctor_agent_skills",
    "_doctor_agent_runtimes",
    "_doctor_extraction_wrap",
    "_doctor_extraction",
    "_doctor_extractors",
    "_doctor_sources",
    "_doctor_admission",
    "_doctor_formal_assurance",
    "_doctor_branches",
    "_doctor_edge_fact_strandings",
    "_doctor_acceptance_criteria",
    "_doctor_source_auth",
    "_doctor_external_live_authority",
    "_doctor_nomos",
    "_doctor_lens_promotions",
    "_doctor_integrity",
    "_doctor_integrity_coverage",
    "_doctor_post_publish_projection",
    "_pending_grant_findings",
    "_doctor_pending_grants",
    "_doctor_deferred_edges",
    "_grant_registry_drift_findings",
    "_doctor_grant_registry_drift",
    "_derived_admission_findings",
    "_withheld_enricher_findings",
    "_doctor_derived_admission",
    "_doctor_enricher_liveness",
    "_enricher_failure_findings",
    "_enricher_quarantine_findings",
    "_enricher_liveness_findings",
    "_doctor_nomos_decision_referential_integrity",
    "_cmd_doctor",
    "_doctor_profile_diagnostics",
]
