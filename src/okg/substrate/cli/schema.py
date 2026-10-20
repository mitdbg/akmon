"""`okg schema quality` — the schema-quality scorecard aggregator.

Aggregates the proven schema-quality primitives into one scored envelope:

  * migration-applicability manifest check   (gating, static)
  * Squawk migration lint                     (warning-only, non-gating)
  * SQL assertions, current-baseline tier     (gating, needs a migrated DB)
  * SQL assertions, target/advisory tier      (advisory, needs a migrated DB)
  * identity conventions                      (gating, needs a migrated DB)
  * promoted-view read honesty                (gating, needs a migrated DB)
  * relation hygiene                          (gating, needs a migrated DB)
  * pg-schema-diff advisory current->target   (advisory, needs a DB + --target)

Design decisions D3/D4 (see docs/reference/schema-golden-design-log-2026-06-21.md):
the scorecard distinguishes what the schema passes *today* (gating) from the
post-repair *target* contract (advisory until the repair lands). The gating set
is the migration manifest, the schema-contract SQL assertions, AND the role/grant
least-privilege assertions — the role tier was initially advisory (presumed
coupled to PR #366 / F-04) but a fresh migration passes it once the v_generations
grant (issue I3) is fixed, so it gates (D4). The only residual #366 gap —
migrate() actively revoking stale grants on an already-dirty deployment — is
tracked by the xfailed test_migrate_revokes_legacy_app_rw_catalog_write_grants,
not by this scorecard. The pg-schema-diff section is always advisory (PR #407).

This module shells the proven scripts under ``scripts/`` via their importable
Python APIs so there is one source of truth for each check.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

# src/okg/substrate/cli/schema.py -> repo root
ROOT = Path(__file__).resolve().parents[4]
SCRIPTS_DIR = ROOT / "scripts"

# Design decisions D3/D4: both assertion tiers gate. Kept as separate tiers for
# per-tier pass/fail visibility in the envelope.
SCHEMA_CONTRACT_GLOBS: tuple[str, ...] = ("tests/sql/schema/*.sql",)
ROLE_ASSERTION_GLOBS: tuple[str, ...] = ("tests/sql/roles/*.sql",)

_STATUS_FAIL = "fail"
_STATUS_PASS = "pass"
_STATUS_WARN = "warn"
_STATUS_ADVISORY = "advisory"
_STATUS_UNAVAILABLE = "unavailable"


def _load_script(name: str):
    """Import a ``scripts/<name>.py`` helper as a module (they are not a package)."""
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / f"{name}.py")
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError(f"cannot load scripts/{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _manifest_section() -> dict[str, Any]:
    mod = _load_script("check_schema_migration_manifest")
    try:
        manifest = mod.load_manifest()
        result = mod.validate_manifest(manifest)
        status = _STATUS_PASS if result["ok"] else _STATUS_FAIL
        return {
            "name": "migration_manifest",
            "gating": True,
            "status": status,
            "migration_count": result["migration_count"],
            "errors": result["errors"],
        }
    except Exception as exc:  # noqa: BLE001 - report, do not crash the scorecard.
        return {
            "name": "migration_manifest",
            "gating": True,
            "status": _STATUS_FAIL,
            "errors": [f"{type(exc).__name__}: {exc}"],
        }


def _lint_section() -> dict[str, Any]:
    mod = _load_script("check_schema_sql_lint")
    try:
        targets = mod.build_targets([], include_schema_snapshot=False)
        squawk_command = os.environ.get(
            "OKG_SQUAWK_COMMAND", mod.DEFAULT_SQUAWK_COMMAND
        )
        _command, completed = mod.run_squawk(
            squawk_command=squawk_command, targets=targets
        )
        if completed.returncode not in (0, 1):
            return {
                "name": "sql_lint",
                "gating": False,
                "status": _STATUS_UNAVAILABLE,
                "detail": (completed.stderr or "squawk runtime error").strip()[:500],
            }
        issues = mod.parse_squawk_payload(completed.stdout)
        summary = mod.summarize_issues(issues, sample_limit=10)
        return {
            "name": "sql_lint",
            "gating": False,
            "status": _STATUS_WARN if issues else _STATUS_PASS,
            "warning_count": summary["warning_count"],
            "by_rule": summary["by_rule"],
        }
    except Exception as exc:  # noqa: BLE001 - lint is advisory; never gate on its plumbing.
        return {
            "name": "sql_lint",
            "gating": False,
            "status": _STATUS_UNAVAILABLE,
            "detail": f"{type(exc).__name__}: {exc}",
        }


def _assertions_section(
    name: str, *, dsn: str, globs: tuple[str, ...], gating: bool
) -> dict[str, Any]:
    mod = _load_script("run_sql_assertions")
    try:
        files = mod.resolve_sql_files(list(globs))
        if not files:
            # No assertion files matched the globs. run_assertions([]) returns a
            # vacuous ok=True with zero assertions; the aggregator must NOT treat
            # that as a silent PASS. Mirror run_sql_assertions.py's own
            # ``if not sql_files: return 2`` guard (which the aggregator bypasses
            # by calling run_assertions() directly): a gating tier with nothing
            # to check is UNAVAILABLE — which forces ok=False — never pass.
            return {
                "name": name,
                "gating": gating,
                "status": _STATUS_UNAVAILABLE if gating else _STATUS_ADVISORY,
                "assertion_count": 0,
                "failures": [],
                "detail": (
                    f"no SQL assertion files matched {list(globs)} — "
                    "tier cannot be certified"
                ),
            }
        result = mod.run_assertions(dsn=dsn, sql_files=files)
        if result["ok"]:
            status = _STATUS_PASS
        else:
            status = _STATUS_FAIL if gating else _STATUS_ADVISORY
        return {
            "name": name,
            "gating": gating,
            "status": status,
            "assertion_count": result["assertion_count"],
            "failures": result["failures"],
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "name": name,
            "gating": gating,
            "status": _STATUS_FAIL if gating else _STATUS_ADVISORY,
            "failures": [{"error": f"{type(exc).__name__}: {exc}"}],
        }


def _identity_section(*, dsn: str) -> dict[str, Any]:
    """Blocking identity-convention section (schema-identity-convergence).

    Checks the three v1.0 identity conventions (§4 of the v1.0 design) against
    the migrated catalog. Divergences registered in
    ``src/okg/substrate/db/identity_divergences.yaml`` with an owning PACT are
    reported but do not gate; anything unregistered fails the envelope, so a
    new relation cannot mint a fourth identity convention.
    """
    mod = _load_script("check_schema_identity")
    try:
        result = mod.run_identity_checks(dsn=dsn)
        return {
            "name": "identity",
            "gating": True,
            "status": _STATUS_PASS if result["ok"] else _STATUS_FAIL,
            "violations": result["violations"],
            "registered_divergence_count": result["registered_divergence_count"],
            "checks": result["checks"],
        }
    except Exception as exc:  # noqa: BLE001 - report, do not crash the scorecard.
        # A gating section reported UNAVAILABLE still fails the envelope
        # (see run_quality), so a broken check is never a silent pass.
        return {
            "name": "identity",
            "gating": True,
            "status": _STATUS_UNAVAILABLE,
            "violations": [],
            "detail": f"{type(exc).__name__}: {exc}",
        }


def _view_reads_section(*, dsn: str) -> dict[str, Any]:
    """Blocking read-path-honesty section (schema-canonical-read-path).

    Asks whether every promoted view can actually return a value: no view whose
    every arm carries a constant-false qualifier, and no column every arm
    projects as a bare NULL literal. Both are shapes the pre-Chronos read path
    shipped — the `WHERE false` projectable stubs, and the history views'
    `NULL::bigint` generation bounds — and both read as healthy while being
    unable to answer.
    """
    mod = _load_script("check_view_reads")
    try:
        result = mod.run_view_read_checks(dsn=dsn)
        return {
            "name": "view_reads",
            "gating": True,
            "status": _STATUS_PASS if result["ok"] else _STATUS_FAIL,
            "findings": result["findings"],
            "view_count": result["view_count"],
            "detail": result.get("detail"),
        }
    except Exception as exc:  # noqa: BLE001 - report, do not crash the scorecard.
        # A gating section reported UNAVAILABLE still fails the envelope
        # (see run_quality), so a broken check is never a silent pass.
        return {
            "name": "view_reads",
            "gating": True,
            "status": _STATUS_UNAVAILABLE,
            "findings": [],
            "detail": f"{type(exc).__name__}: {exc}",
        }


def _relation_hygiene_section(*, dsn: str) -> dict[str, Any]:
    """Blocking relation-hygiene section (schema-dead-weight-retirement).

    Two standing gates, each with a static arm over the shipped DDL and a
    catalog arm over the migrated database: every CHECK constraint is born
    VALID (no unpaired ``NOT VALID``, no ``convalidated = false`` row), and
    the retired zero-consumer relations stay retired.
    """
    mod = _load_script("check_schema_relation_hygiene")
    try:
        result = mod.run_relation_hygiene_checks(dsn=dsn)
        return {
            "name": "relation_hygiene",
            "gating": True,
            "status": _STATUS_PASS if result["ok"] else _STATUS_FAIL,
            "violations": result["violations"],
            "catalog_status": result["catalog_status"],
            "checks": result["checks"],
        }
    except Exception as exc:  # noqa: BLE001 - report, do not crash the scorecard.
        # A gating section reported UNAVAILABLE still fails the envelope
        # (see run_quality), so a broken check is never a silent pass.
        return {
            "name": "relation_hygiene",
            "gating": True,
            "status": _STATUS_UNAVAILABLE,
            "violations": [],
            "detail": f"{type(exc).__name__}: {exc}",
        }


def _schema_diff_section(*, dsn: str, target: str) -> dict[str, Any]:
    """pg-schema-diff advisory of current DB vs the golden/target schema.

    Always advisory (PR #407 decision: developer diagnostic, do not gate).
    Degrades to ``unavailable`` when the pg-schema-diff binary is absent.
    """
    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "schema_diff_advisory.py"),
        "--from-dsn",
        dsn,
        "--target",
        target,
        "--json",
    ]
    try:
        completed = subprocess.run(
            cmd, cwd=ROOT, capture_output=True, text=True, check=False
        )
        payload = json.loads(completed.stdout or "{}")
    except Exception as exc:  # noqa: BLE001
        return {
            "name": "schema_diff",
            "gating": False,
            "status": _STATUS_UNAVAILABLE,
            "detail": f"{type(exc).__name__}: {exc}",
        }
    status = payload.get("status", _STATUS_UNAVAILABLE)
    return {
        "name": "schema_diff",
        "gating": False,
        "status": status,
        "advisory": True,
        "statement_count": payload.get("statement_count"),
        "detail": payload.get("detail"),
    }


def run_quality(
    *,
    dsn: str | None,
    target: str | None = None,
    include_lint: bool = True,
) -> dict[str, Any]:
    """Aggregate the scorecard sections into one scored envelope.

    ``dsn`` must point at a freshly *migrated* database for the SQL-assertion and
    schema-diff sections. When ``dsn`` is ``None`` only the static sections
    (manifest, lint) run, and the DB-backed sections are reported as skipped.
    """
    sections: list[dict[str, Any]] = []
    sections.append(_manifest_section())
    if include_lint:
        sections.append(_lint_section())

    if dsn:
        sections.append(
            _assertions_section(
                "schema_contract_assertions",
                dsn=dsn,
                globs=SCHEMA_CONTRACT_GLOBS,
                gating=True,
            )
        )
        sections.append(
            _assertions_section(
                "role_least_privilege_assertions",
                dsn=dsn,
                globs=ROLE_ASSERTION_GLOBS,
                gating=True,
            )
        )
        sections.append(_identity_section(dsn=dsn))
        sections.append(_view_reads_section(dsn=dsn))
        sections.append(_relation_hygiene_section(dsn=dsn))
        if target:
            sections.append(_schema_diff_section(dsn=dsn, target=target))
    else:
        skipped = {
            "name": "sql_assertions",
            "gating": True,
            "status": _STATUS_UNAVAILABLE,
            "detail": "no --dsn / OKG_TEST_DSN supplied; DB-backed checks skipped",
        }
        sections.append(skipped)

    # An UNAVAILABLE gating tier already fails the envelope; it must also be
    # NAMED, or the summary reads "schema quality FAIL" with an empty cause and
    # the operator has to go read the JSON to find out which tier could not be
    # certified.
    gating_fail = [
        s
        for s in sections
        if s.get("gating")
        and s["status"] in (_STATUS_FAIL, _STATUS_UNAVAILABLE)
    ]
    advisory_findings = [
        s
        for s in sections
        if not s.get("gating") and s["status"] in (_STATUS_ADVISORY, _STATUS_WARN)
    ]
    ok = not gating_fail
    return {
        "ok": ok,
        "scope": "target" if target else "current",
        "dsn_present": bool(dsn),
        "sections": sections,
        "gating_failures": [s["name"] for s in gating_fail],
        "advisory_findings": [s["name"] for s in advisory_findings],
        "summary": _summarize(ok, sections, gating_fail, advisory_findings),
    }


def _summarize(
    ok: bool,
    sections: list[dict[str, Any]],
    gating_fail: list[dict[str, Any]],
    advisory_findings: list[dict[str, Any]],
) -> str:
    parts = [f"{s['name']}={s['status']}" for s in sections]
    head = "PASS" if ok else "FAIL"
    tail = ""
    if gating_fail:
        tail = f"; gating failures: {', '.join(s['name'] for s in gating_fail)}"
    elif advisory_findings:
        tail = f"; advisory findings: {', '.join(s['name'] for s in advisory_findings)}"
    return f"schema quality {head} ({'; '.join(parts)}){tail}"


def cmd_schema_quality(args: argparse.Namespace, *, as_json: bool) -> int:
    dsn = getattr(args, "dsn", None) or os.environ.get("OKG_TEST_DSN")
    target = getattr(args, "target", None)
    envelope = run_quality(
        dsn=dsn,
        target=target,
        include_lint=not getattr(args, "no_lint", False),
    )
    if as_json:
        print(json.dumps(envelope, indent=2, sort_keys=True))
    else:
        print(envelope["summary"])
        for section in envelope["sections"]:
            print(f"  - {section['name']}: {section['status']}"
                  + (" (gating)" if section.get("gating") else " (advisory)"))
    return 0 if envelope["ok"] else 1
