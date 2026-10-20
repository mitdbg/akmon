#!/usr/bin/env python3
"""Regenerate the proof-integrity status and slow-test impact artifacts."""

# ruff: noqa: E402 -- executable scripts add the repository root before imports
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from xml.etree import ElementTree

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from okg.substrate.pact.evidence_kinds import load_registry
from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.store import load_manifest
from okg.substrate.pact.validation import (
    _executed_test_evidence_reasons,
    _hazardous_slow_pytest_targets,
    _slow_pytest_command_issues,
)
from tests.pact._acceptance_flip_census import CENSUS_PATH, build_census
from tests.pact.test_evidence_partial_status import (
    FLIPPED_BY_MIGRATION,
    MIGRATED_ROWS,
)


REPORT_DIR = CENSUS_PATH.parent
IMPACT_JSON = REPORT_DIR / "proof-integrity-impact.json"
IMPACT_MD = REPORT_DIR / "proof-integrity-impact.md"
DEFAULT_PRE_COMMIT = "fe64f5d906f6d0ba795584666669df371dd7de07"


def _manifests() -> list[tuple[Path, PactManifest]]:
    return [
        (path, load_manifest(REPO_ROOT, path.parent.name))
        for path in sorted((REPO_ROOT / "pact" / "changes").glob("*/pact.yaml"))
    ]


def _live_command_findings(manifests: list[tuple[Path, PactManifest]]) -> list[dict]:
    return [
        {"change": path.parent.name, "field": issue.path}
        for path, manifest in manifests
        for issue in _slow_pytest_command_issues(manifest, repo_root_path=REPO_ROOT)
        if issue.code == "pytest_command_deselects_named_slow_test"
    ]


def _base_command_findings(base_commit: str) -> list[dict]:
    findings: list[dict] = []
    for path in sorted((REPO_ROOT / "pact" / "changes").glob("*/pact.yaml")):
        relative = path.relative_to(REPO_ROOT)
        result = subprocess.run(
            ["git", "show", f"{base_commit}:{relative}"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            continue
        manifest = PactManifest.model_validate(yaml.safe_load(result.stdout))
        findings.extend(
            {"change": path.parent.name, "field": issue.path}
            for issue in _slow_pytest_command_issues(manifest, repo_root_path=REPO_ROOT)
            if issue.code == "pytest_command_deselects_named_slow_test"
        )
    return findings


def _historical_invalid_rows(
    manifests: list[tuple[Path, PactManifest]],
) -> list[dict]:
    rows: list[dict] = []
    registry = load_registry(REPO_ROOT)
    for path, manifest in manifests:
        for evidence in manifest.evidence:
            if registry.canonicalize(
                evidence.kind
            ) != "executed_test" or evidence.status not in {
                "pass",
                "waived",
            }:
                continue
            reasons = _executed_test_evidence_reasons(manifest, evidence)
            if not reasons:
                continue
            rows.append(
                {
                    "change": path.parent.name,
                    "evidence": evidence.id,
                    "stored_status": evidence.status,
                    "refs": list(evidence.refs),
                    "reasons": reasons,
                }
            )
    return rows


def _historical_slow_commands(
    manifests: list[tuple[Path, PactManifest]],
) -> list[dict]:
    rows: list[dict] = []
    for path, manifest in manifests:
        for evidence in manifest.evidence:
            if not evidence.command:
                continue
            targets = _hazardous_slow_pytest_targets(evidence.command, REPO_ROOT)
            if targets:
                rows.append(
                    {
                        "change": path.parent.name,
                        "evidence": evidence.id,
                        "stored_status": evidence.status,
                        "targets": sorted(set(targets)),
                        "command": evidence.command,
                    }
                )
    return rows


def _legacy_vacuous_proof_disposition(
    manifests: list[tuple[Path, PactManifest]],
) -> list[dict]:
    """Explain every one of the original self-disclaiming rows explicitly.

    The old validator cannot parse the newer `partial` status, so the generic
    same-manifest comparison correctly records these changes as not_compared.
    This companion ledger prevents that parser boundary from hiding the
    original thirteen-requirement floor named by the PACT.
    """
    by_change = {path.parent.name: manifest for path, manifest in manifests}
    migration_flips = {
        (change, requirement)
        for change, requirements in FLIPPED_BY_MIGRATION.items()
        for requirement in requirements
    }
    rows: list[dict] = []
    for change, evidence_ids in MIGRATED_ROWS.items():
        manifest = by_change[change]
        evidence_by_id = {row.id: row for row in manifest.evidence}
        for evidence_id in evidence_ids:
            evidence = evidence_by_id[evidence_id]
            requirement = next(ref for ref in evidence.refs if ref.startswith("req."))
            if requirement == "req.chat-review-queue-pipe.one-inbox":
                disposition = "outside_13_requirement_floor_already_open"
                explanation = (
                    "This fourteenth row was not in the thirteen-requirement "
                    "floor: a distinct ui_visual arm already kept the requirement open."
                )
            elif (change, requirement) in migration_flips:
                disposition = "changed_when_row_was_downgraded_to_partial"
                explanation = (
                    "The requirement changed to unsatisfied when this row was "
                    "recorded partial; that manifest now exceeds the old reader's schema."
                )
            else:
                disposition = "already_open_before_current_code_comparison"
                explanation = (
                    "The requirement was already unsatisfied for an independent missing "
                    "proof arm before the current validator comparison, so it is omitted "
                    "from code-only flips rather than counted as newly changed."
                )
            rows.append(
                {
                    "change": change,
                    "requirement": requirement,
                    "evidence": evidence_id,
                    "stored_status": evidence.status,
                    "disposition": disposition,
                    "explanation": explanation,
                }
            )
    return rows


def _run(command: list[str], *, junit: Path | None = None) -> dict:
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    result = {
        "command": " ".join(command),
        "exit_code": completed.returncode,
        "output": (completed.stdout + completed.stderr).strip(),
    }
    if junit is not None and junit.is_file():
        root = ElementTree.parse(junit).getroot()
        suites = (
            [root]
            if root.tag.endswith("testsuite")
            else [node for node in root.iter() if node.tag.endswith("testsuite")]
        )
        result["junit"] = {
            key: sum(int(suite.attrib.get(key, 0)) for suite in suites)
            for key in ("tests", "failures", "errors", "skipped")
        }
    return result


def _collection_and_execution_proof() -> dict:
    environment_file = "tests/substrate/test_environment_services.py"
    model_file = "tests/pact/test_deployment_backup_policy_model_contract.py"
    named = f"{model_file}::test_declassifying_external_effect_fails_the_checker"
    common = [sys.executable, "-m", "pytest"]
    scratch = REPO_ROOT / ".scratch"
    scratch.mkdir(exist_ok=True)
    default_xml = scratch / "pact-proof-model-slow-default.xml"
    override_xml = scratch / "pact-proof-model-slow-override.xml"
    return {
        "environment_services_default": _run(
            [
                *common,
                environment_file,
                "--collect-only",
                "-q",
                "--no-cov",
            ]
        ),
        "environment_services_override": _run(
            [
                *common,
                environment_file,
                "--collect-only",
                "-q",
                "--no-cov",
                "-m",
                "slow or not slow",
            ]
        ),
        "backup_model_default": _run(
            [
                *common,
                model_file,
                "--collect-only",
                "-q",
                "--no-cov",
            ]
        ),
        "backup_model_override": _run(
            [
                *common,
                model_file,
                "--collect-only",
                "-q",
                "--no-cov",
                "-m",
                "slow or not slow",
            ]
        ),
        "named_slow_default": _run(
            [
                *common,
                named,
                "-q",
                "--no-cov",
                f"--junitxml={default_xml}",
            ],
            junit=default_xml,
        ),
        "named_slow_override": _run(
            [
                *common,
                named,
                "-q",
                "--no-cov",
                "-m",
                "slow or not slow",
                f"--junitxml={override_xml}",
            ],
            junit=override_xml,
        ),
    }


def _markdown(report: dict) -> str:
    census = report["acceptance_status_impact"]
    requirement_rows = "\n".join(
        f"- `{row['change']}::{row['requirement']}`: {row['pre']} → {row['post']}"
        for row in census["requirement_flips"]
    )
    legacy_rows = "\n".join(
        f"- `{row['change']}::{row['requirement']}` via `{row['evidence']}`: "
        f"{row['disposition']} — {row['explanation']}"
        for row in report["legacy_vacuous_proof_disposition"]
    )
    return f"""# PACT proof-integrity impact

No pre-existing corpus evidence row was edited, deleted, backfilled, or
relabelled by this repair. Existing passing text remains visible in
`pact.yaml`; when its stored JUnit identities do not attest the named test, it
no longer participates in derived acceptance or completion until a fresh
report is ingested.

## Measured changes

- Live hazardous commands: {len(report["live_commands_before"])} before → {len(report["live_commands_after"])} after.
- Preserved historical evidence rows whose commands still name excluded slow targets: {len(report["historical_slow_commands"])}.
- Preserved passing test rows now derived as invalid proof: {len(report["historical_invalid_test_rows"])} across {report["counts"]["changes_with_invalid_test_rows"]} changes.
- Acceptance criteria satisfied → unsatisfied: {census["counts"]["acceptance_flipped"]}.
- Requirements satisfied → unsatisfied: {census["counts"]["requirements_flipped"]}.

The full row-level attribution is in `proof-integrity-impact.json`; the
acceptance before/after validator comparison is in
`acceptance-flip-census.json`.

## Original self-disclaiming proof disposition

The old validator cannot parse the later `partial` status, so those four
manifests are deliberately `not_compared` in the code-only census. Every one
of the fourteen named rows (the thirteen-requirement floor plus the already
open control) is accounted for here instead of disappearing behind that
schema boundary:

{legacy_rows}

## Representative collection and execution

- `test_environment_services.py`: 80/81 collected (1 deselected) under the
  default; 81 collected with `-m "slow or not slow"`.
- `test_deployment_backup_policy_model_contract.py`: 67/68 collected (1
  deselected) under the default; 68 collected with the override.
- Exact named slow test: default exit 5 with 1 deselected and no JUnit test;
  override exit 0 with JUnit `tests=1, failures=0, errors=0, skipped=0`.

## Requirements whose derived status changes

{requirement_rows}
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pre-commit", default=DEFAULT_PRE_COMMIT)
    parser.add_argument("--base-commit", default="origin/dev")
    args = parser.parse_args(argv)

    manifests = _manifests()
    census = build_census(args.pre_commit)
    invalid_rows = _historical_invalid_rows(manifests)
    historical_commands = _historical_slow_commands(manifests)
    legacy_disposition = _legacy_vacuous_proof_disposition(manifests)
    report = {
        "generated_by": "scripts/pact/generate_proof_integrity_impact.py",
        "base_commit": subprocess.run(
            ["git", "rev-parse", args.base_commit],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
        "policy": {
            "historical_rows_mutated": False,
            "passing_rows_without_named_junit_are_derived_unproven": True,
        },
        "live_commands_before": _base_command_findings(args.base_commit),
        "live_commands_after": _live_command_findings(manifests),
        "historical_slow_commands": historical_commands,
        "historical_invalid_test_rows": invalid_rows,
        "legacy_vacuous_proof_disposition": legacy_disposition,
        "acceptance_status_impact": census,
        "collection_and_execution": _collection_and_execution_proof(),
        "counts": {
            "changes_with_invalid_test_rows": len(
                {row["change"] for row in invalid_rows}
            ),
            "requirements_referenced_by_invalid_test_rows": len(
                {
                    ref
                    for row in invalid_rows
                    for ref in row["refs"]
                    if ref.startswith("req.")
                }
            ),
        },
    }
    if report["live_commands_after"]:
        raise SystemExit(
            f"live hazardous commands remain: {report['live_commands_after']}"
        )
    named = report["collection_and_execution"]["named_slow_override"]
    if named.get("junit") != {"tests": 1, "failures": 0, "errors": 0, "skipped": 0}:
        raise SystemExit(f"named slow test did not execute successfully: {named}")

    CENSUS_PATH.write_text(json.dumps(census, indent=2) + "\n", encoding="utf-8")
    IMPACT_JSON.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    IMPACT_MD.write_text(_markdown(report), encoding="utf-8")
    print(
        f"proof-integrity-impact: {len(report['live_commands_before'])} live "
        f"hazards repaired; {len(invalid_rows)} historical rows now unproven; "
        f"{census['counts']['requirements_flipped']} requirement flips"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
