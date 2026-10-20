#!/usr/bin/env python3
"""Check the external-action lifecycle and emit strict PACT formal evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import tlc_jvm_args, tlc_workers


ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = ROOT / "formal" / "external_action_review"
SOURCE = MODEL_DIR / "ExternalActionReview.tla"
CONFIG = MODEL_DIR / "ExternalActionReview.cfg"
PACT_CHANGE_ID = "external-automation-review-lifecycle"
TLA_TOOLS = ROOT / ".tools" / "tla2tools.jar"
DEFAULT_OUT = ROOT / ".scratch" / "formal" / "external-action-review"
COMMAND = (
    "uv run --extra dev python scripts/check_formal_external_action_review.py "
    "--out .scratch/formal/external-action-review"
)
BINDING_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_external_action_review.py "
    "--binding-only"
)
REFINEMENT_TESTS = (
    "tests/pact/test_external_action_review_model_contract.py::"
    "test_formal_refinement_map_covers_all_model_actions",
    "tests/substrate/external_actions/test_service.py",
    "tests/substrate/external_actions/test_store_postgres.py",
    "tests/e2e/test_external_action_review.py",
)

MUTATIONS = {
    "stale_decision": {
        "before": (
            "  /\\ preparedApprovalLifecycle = lifecycleVersion\n"
            "  /\\ preparedApprovalPayloadVersion = payloadVersion\n"
            "  /\\ preparedApprovalPayloadHash = currentPayloadHash\n"
        ),
        "after": "",
        "invariant": "ExternalActionReviewSafety",
        "config": "ExternalActionReviewStaleDecisionWitness.cfg",
        "secondary": (
            "  /\\ decisionPayloadVersion' = payloadVersion\n"
            "  /\\ decisionPayloadHash' = currentPayloadHash\n",
            "  /\\ decisionPayloadVersion' = preparedApprovalPayloadVersion\n"
            "  /\\ decisionPayloadHash' = preparedApprovalPayloadHash\n",
        ),
    },
    "expired_claim": {
        "before": "  /\\ databaseTime < ExecuteBefore\n",
        "after": "  /\\ databaseTime <= ExecuteBefore\n",
        "invariant": "ExternalActionExecutionSafety",
        "config": "ExternalActionReviewExpiredClaimWitness.cfg",
    },
    "stale_cancel": {
        "before": (
            "  /\\ preparedCancelLifecycle = lifecycleVersion\n"
            '  /\\ (reviewState = "approved" => preparedCancelDecisionId = decisionId)\n'
        ),
        "after": "",
        "invariant": "ExternalActionReviewSafety",
        "config": "ExternalActionReviewStaleCancelWitness.cfg",
    },
    "replay_owner": {
        "before": "  /\\ actor = outcomeReporter\n",
        "after": "",
        "invariant": "ExternalActionExecutionSafety",
        "config": "ExternalActionReviewReplayOwnerWitness.cfg",
    },
}


def _run(module: Path, config: Path) -> subprocess.CompletedProcess[str]:
    workers = tlc_workers()
    return subprocess.run(
        [
            "java",
            *tlc_jvm_args(workers),
            "-cp",
            str(TLA_TOOLS),
            "tlc2.TLC",
            "-config",
            str(config),
            "-deadlock",
            "-workers",
            workers,
            str(module),
        ],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def _digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _pact_model_dir() -> Path:
    candidates = [
        ROOT / "pact" / tree / PACT_CHANGE_ID / "formal"
        for tree in ("changes", "archive")
    ]
    existing = [path for path in candidates if path.is_dir()]
    if not existing:
        raise RuntimeError(
            "the approved PACT model directory is missing (expected under "
            f"pact/changes/ or pact/archive/{PACT_CHANGE_ID}/formal); a checkout "
            "without this repository's PACT change records, such as the public "
            "copy, can run the model alone with --without-pact-records "
            "(scripts/run_formal_suite.py --without-pact-records forwards it)"
        )
    if len(existing) != 1:
        rendered = ", ".join(str(path) for path in existing)
        raise RuntimeError(
            "expected exactly one active or archived PACT model directory; found "
            + rendered
        )
    return existing[0]


def _run_binding_only() -> dict[str, object]:
    if not TLA_TOOLS.is_file():
        raise RuntimeError(f"TLA+ tools are missing: {TLA_TOOLS}")
    baseline = _run(SOURCE, CONFIG)
    if baseline.returncode or "No error has been found" not in baseline.stdout:
        raise RuntimeError("baseline model failed\n" + baseline.stdout)

    witnesses: dict[str, str] = {}
    source = SOURCE.read_text(encoding="utf-8")
    with tempfile.TemporaryDirectory(prefix="okg-1182-tla-") as temp_dir:
        module = Path(temp_dir) / SOURCE.name
        config = Path(temp_dir) / CONFIG.name
        config.write_text(CONFIG.read_text(encoding="utf-8"), encoding="utf-8")
        for mutation_id, mutation in MUTATIONS.items():
            before = str(mutation["before"])
            if source.count(before) != 1:
                raise RuntimeError(f"mutation anchor drifted: {mutation_id}")
            mutated = source.replace(before, str(mutation["after"]))
            secondary = mutation.get("secondary")
            if secondary:
                old, new = secondary
                if mutated.count(old) != 1:
                    raise RuntimeError(
                        f"secondary mutation anchor drifted: {mutation_id}"
                    )
                mutated = mutated.replace(old, new)
            module.write_text(mutated, encoding="utf-8")
            result = _run(module, config)
            expected = f"Invariant {mutation['invariant']} is violated"
            if result.returncode == 0 or expected not in result.stdout:
                raise RuntimeError(
                    f"mutation {mutation_id} did not violate {mutation['invariant']}"
                )
            witnesses[mutation_id] = str(mutation["invariant"])
    return {
        "ok": True,
        "baseline": "pass",
        "mutation_witnesses": witnesses,
        "provider_calls": 0,
    }


def check(out: Path, *, pact_records: bool = True) -> dict[str, object]:
    out.mkdir(parents=True, exist_ok=True)
    if not TLA_TOOLS.is_file():
        raise RuntimeError(f"TLA+ tools are missing: {TLA_TOOLS}")
    if pact_records:
        pact_model_dir = _pact_model_dir()
        pact_source = pact_model_dir / "ExternalActionReview.tla"
        pact_config = pact_model_dir / "ExternalActionReview.cfg"
        if SOURCE.read_bytes() != pact_source.read_bytes():
            raise RuntimeError("production model drifted from the approved PACT model")
        if CONFIG.read_bytes() != pact_config.read_bytes():
            raise RuntimeError("production config drifted from the approved PACT config")
    binding_checker = subprocess.run(
        [
            "uv",
            "run",
            "--extra",
            "dev",
            "python",
            "scripts/check_formal_external_action_review.py",
            "--binding-only",
        ],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    binding_log = out / "pact-binding-checker.log"
    binding_log.write_text(binding_checker.stdout, encoding="utf-8")
    if binding_checker.returncode:
        raise RuntimeError("PACT binding checker failed; see " + str(binding_log))
    baseline = _run(SOURCE, CONFIG)
    baseline_log = out / "tlc-baseline.log"
    baseline_log.write_text(baseline.stdout, encoding="utf-8")
    if baseline.returncode or "No error has been found" not in baseline.stdout:
        raise RuntimeError("baseline model failed; see " + str(baseline_log))

    witnesses: dict[str, str] = {}
    source = SOURCE.read_text(encoding="utf-8")
    with tempfile.TemporaryDirectory(prefix="okg-1182-tla-") as temp_dir:
        module = Path(temp_dir) / SOURCE.name
        for mutation_id, mutation in MUTATIONS.items():
            before = str(mutation["before"])
            if source.count(before) != 1:
                raise RuntimeError(f"mutation anchor drifted: {mutation_id}")
            mutated = source.replace(before, str(mutation["after"]))
            secondary = mutation.get("secondary")
            if secondary:
                old, new = secondary
                if mutated.count(old) != 1:
                    raise RuntimeError(
                        f"secondary mutation anchor drifted: {mutation_id}"
                    )
                mutated = mutated.replace(old, new)
            module.write_text(mutated, encoding="utf-8")
            source_config = MODEL_DIR / str(mutation["config"])
            config = Path(temp_dir) / source_config.name
            config.write_text(
                source_config.read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            result = _run(module, config)
            log_path = out / f"tlc-witness-{mutation_id}.log"
            log_path.write_text(result.stdout, encoding="utf-8")
            expected = f"Invariant {mutation['invariant']} is violated"
            if result.returncode == 0 or expected not in result.stdout:
                raise RuntimeError(
                    f"mutation {mutation_id} did not violate {mutation['invariant']}"
                )
            witnesses[mutation_id] = str(mutation["invariant"])

    module_digest = _digest(SOURCE)
    config_digest = _digest(CONFIG)
    report = {
        "results": [
            {
                "property_id": property_id,
                "status": "pass",
                "fresh": True,
                "command": COMMAND,
                "checker_command": BINDING_CHECKER_COMMAND,
                "artifact": binding_log.name,
                "refinement_tests": list(REFINEMENT_TESTS),
                "module_digest": module_digest,
                "config_digest": config_digest,
                "summary": (
                    f"{property_id} passed with the approved checker and four "
                    "guard-removal witnesses"
                    if pact_records
                    else f"{property_id} passed with four guard-removal "
                    "witnesses; the comparison with the approved PACT model was "
                    "NOT run (--without-pact-records)"
                ),
                # Only a run that skipped the comparison says so in the row;
                # the default report rows keep their existing keys.
                **(
                    {}
                    if pact_records
                    else {"pact_model_comparison": "skipped: --without-pact-records"}
                ),
            }
            for property_id in (
                "ExternalActionReviewSafety",
                "ExternalActionExecutionSafety",
            )
        ]
    }
    report_path = out / "formal-report.yaml"
    report_path.write_text(yaml.safe_dump(report, sort_keys=False), encoding="utf-8")
    return {
        "ok": True,
        "baseline": "pass",
        "mutation_witnesses": witnesses,
        "provider_calls": 0,
        "pact_model_comparison": (
            "checked" if pact_records else "skipped: --without-pact-records"
        ),
        "module_digest": module_digest,
        "formal_report": str(report_path.relative_to(ROOT)),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--binding-only", action="store_true")
    parser.add_argument(
        "--without-pact-records",
        action="store_true",
        help=(
            "skip comparing the model with its approved copy in this "
            "repository's PACT change records, for a checkout that does not "
            "carry them; the TLC run and the witnesses are unchanged"
        ),
    )
    args = parser.parse_args(argv)
    try:
        if args.binding_only:
            result = _run_binding_only()
        else:
            result = check(
                args.out.resolve(), pact_records=not args.without_pact_records
            )
    except (OSError, RuntimeError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
