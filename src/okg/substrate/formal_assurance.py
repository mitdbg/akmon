"""Formal assurance registry and evidence checks."""

from __future__ import annotations

import os
import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FormalCoverageWaiver:
    reason: str
    owner: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "owner": self.owner,
        }


@dataclass(frozen=True)
class FormalCoverageRequirement:
    coverage_id: str
    kind: str
    description: str
    artifact: str
    required: bool = True
    waiver: FormalCoverageWaiver | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "coverage_id": self.coverage_id,
            "kind": self.kind,
            "description": self.description,
            "artifact": self.artifact,
            "required": self.required,
        }
        if self.waiver is not None:
            payload["waiver"] = self.waiver.to_dict()
        return payload


@dataclass(frozen=True)
class FormalModelRegistration:
    model_id: str
    capability: str
    model_path: str
    checker_command: str
    assurance_tier: str
    assumptions: tuple[str, ...]
    required_artifacts: tuple[str, ...]
    counterexample_artifact: str
    service_trace_artifacts: tuple[str, ...] = ()
    coverage_requirements: tuple[FormalCoverageRequirement, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "capability": self.capability,
            "model_path": self.model_path,
            "checker_command": self.checker_command,
            "assurance_tier": self.assurance_tier,
            "assumptions": list(self.assumptions),
            "required_artifacts": list(self.required_artifacts),
            "counterexample_artifact": self.counterexample_artifact,
            "service_trace_artifacts": list(self.service_trace_artifacts),
            "coverage_requirements": [
                requirement.to_dict()
                for requirement in self.coverage_requirements
            ],
        }


POROS_MODEL = FormalModelRegistration(
    model_id="poros-end-to-end-exchange",
    capability="poros-protocol",
    model_path="formal/poros/PorosExchange.tla",
    checker_command="make verify-poros",
    assurance_tier="tier2-protocol-security",
    assumptions=(
        "Ed25519 signatures are modeled as an ideal unforgeability predicate.",
        "SHA-256 body and object hashes are collision-resistant.",
        "HPKE private object confidentiality depends on a vetted backend.",
        "Runtime refinement is checked against finite two-OKG traces.",
    ),
    required_artifacts=(
        ".scratch/formal/poros/tlc.stdout.log",
        ".scratch/formal/poros-counterexample/counterexample.txt",
        ".scratch/formal/poros-refinement/refinement-state.json",
    ),
    counterexample_artifact=(
        ".scratch/formal/poros-counterexample/counterexample.txt"
    ),
    service_trace_artifacts=(
        ".scratch/formal/poros-db-refinement/runtime-trace.json",
        ".scratch/formal/poros-db-refinement/refinement-state.json",
    ),
    coverage_requirements=(
        FormalCoverageRequirement(
            coverage_id="poros-intentional-counterexample",
            kind="intentional_counterexample",
            description=(
                "The Poros model has a negative variant that TLC must reject "
                "and write as a counterexample artifact."
            ),
            artifact=".scratch/formal/poros-counterexample/counterexample.txt",
        ),
        FormalCoverageRequirement(
            coverage_id="poros-refinement-non-vacuity",
            kind="non_vacuity_refinement",
            description=(
                "A finite two-OKG runtime trace is translated into refinement "
                "state so the model is exercised by real protocol behavior."
            ),
            artifact=".scratch/formal/poros-refinement/refinement-state.json",
        ),
    ),
)


def formal_model_registry(
    root: Path | None = None,
    *,
    registrations: Sequence[FormalModelRegistration] | None = None,
) -> list[dict[str, Any]]:
    root = _repo_root(root)
    return [
        _model_status(
            root,
            registration,
            require_service_trace=_require_service_trace(),
        )
        for registration in (registrations or (POROS_MODEL,))
    ]


def evaluate_formal_assurance(
    root: Path | None = None,
    *,
    registrations: Sequence[FormalModelRegistration] | None = None,
) -> dict[str, Any]:
    root = _repo_root(root)
    models = formal_model_registry(root, registrations=registrations)
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for model in models:
        status = model["latest_status"]
        if status not in {"pass", "waived"}:
            errors.append(
                {
                    "code": f"formal_assurance_{status}",
                    "model_id": model["model_id"],
                    "message": model["status_reason"],
                }
            )
        for coverage in model.get("coverage_status", {}).get("coverage", []):
            if coverage.get("status") == "waived":
                warnings.append(
                    {
                        "code": "formal_assurance_coverage_waived",
                        "model_id": model["model_id"],
                        "coverage_id": coverage["coverage_id"],
                        "message": coverage["status_reason"],
                    }
                )
    return {
        "ok": not errors,
        "status": "ERROR" if errors else "WARN" if warnings else "OK",
        "models": models,
        "errors": errors,
        "warnings": warnings,
    }


def _model_status(
    root: Path,
    registration: FormalModelRegistration,
    *,
    require_service_trace: bool,
) -> dict[str, Any]:
    payload = registration.to_dict()
    artifact_paths = [root / path for path in registration.required_artifacts]
    coverage_artifacts = {
        root / requirement.artifact
        for requirement in registration.coverage_requirements
    }
    generic_artifact_paths = [
        path for path in artifact_paths
        if path not in coverage_artifacts
    ]
    missing = [
        str(path.relative_to(root))
        for path in generic_artifact_paths
        if not path.is_file()
    ]
    try:
        stale_inputs = _existing_paths(
            root,
            [
                registration.model_path,
                "formal/poros/PorosExchange.cfg",
                "formal/poros/PorosExchangeCounterexample.tla",
                "formal/poros/PorosExchangeCounterexample.cfg",
                "formal/poros/fixtures/two-okg-trace.json",
                "scripts/check_formal_poros.py",
                "scripts/check_poros_refinement_fixture.py",
            ],
        )
    except FileNotFoundError as exc:
        payload.update(
            {
                "latest_status": "missing",
                "status_reason": str(exc),
                "artifacts": _artifact_payload(root, artifact_paths),
            }
        )
        return payload
    coverage_status = _coverage_status(root, registration, stale_inputs)
    if missing:
        payload.update(
            {
                "latest_status": "missing",
                "status_reason": (
                    "missing formal assurance artifacts: "
                    + ", ".join(missing)
                ),
                "coverage_status": coverage_status,
                "artifacts": _artifact_payload(root, artifact_paths),
            }
        )
        return payload
    if generic_artifact_paths:
        newest_input = max(path.stat().st_mtime for path in stale_inputs)
        oldest_artifact = min(path.stat().st_mtime for path in generic_artifact_paths)
        if oldest_artifact < newest_input:
            payload.update(
                {
                    "latest_status": "stale",
                    "status_reason": (
                        "formal assurance artifacts are older than model, "
                        "fixture, or checker inputs"
                    ),
                    "coverage_status": coverage_status,
                    "artifacts": _artifact_payload(root, artifact_paths),
                }
            )
            return payload
    if not coverage_status["ok"]:
        payload.update(
            {
                "latest_status": "weak",
                "status_reason": coverage_status["status_reason"],
                "coverage_status": coverage_status,
                "artifacts": _artifact_payload(root, artifact_paths),
            }
        )
        return payload
    normal_stdout = root / ".scratch/formal/poros/tlc.stdout.log"
    report_status = _normal_model_report_status(
        normal_stdout.parent / "formal-report.yaml",
        coverage_status,
        artifact_paths,
        root,
    )
    if report_status is not None:
        payload.update(report_status)
        return payload
    final_artifact_paths = artifact_paths
    if require_service_trace:
        service_status = _service_trace_status(
            root,
            registration,
            formal_artifact_paths=artifact_paths,
        )
        if service_status is not None:
            service_status["coverage_status"] = coverage_status
            payload.update(service_status)
            return payload
        final_artifact_paths = artifact_paths + [
            root / path for path in registration.service_trace_artifacts
        ]
    if coverage_status["status"] == "waived":
        latest_status = "waived"
        status_reason = coverage_status["status_reason"]
    else:
        latest_status = "pass"
        status_reason = (
            "all Poros formal artifacts are present and fresh"
            + (
                "; service-trace refinement artifacts are present and fresh"
                if require_service_trace
                else ""
            )
        )
    payload.update(
        {
            "latest_status": latest_status,
            "status_reason": status_reason,
            "coverage_status": coverage_status,
            "artifacts": _artifact_payload(root, final_artifact_paths),
        }
    )
    return payload


def _normal_model_report_status(
    report_path: Path,
    coverage_status: dict[str, Any],
    artifact_paths: list[Path],
    root: Path,
) -> dict[str, Any] | None:
    """Consume the strict TLC formal report for the normal Poros model.

    The previous gate passed on the ABSENCE of ``Error: Invariant`` / ``is
    violated`` in the raw stdout log, so an OOM, a timeout, or a zero-state
    (vacuous) run all read as a clean pass (issue #684). A verdict must come
    from a positive, typed success artifact: this consumes the ``proof_status``,
    ``distinct_states``, and ``timed_out`` fields that ``run_tlc_formal_report``
    already writes to ``formal-report.yaml``.

    Returns a fail payload when the report is missing, unparseable, empty, or
    reports anything short of a freshly-proven, non-vacuous, non-timed-out run
    for every property; returns ``None`` (proceed) only when every result row
    positively attests a pass.
    """
    import yaml

    from okg.substrate.formal_tlc import DEFAULT_MIN_DISTINCT_STATES

    def _fail(reason: str) -> dict[str, Any]:
        return {
            "latest_status": "fail",
            "status_reason": reason,
            "coverage_status": coverage_status,
            "artifacts": _artifact_payload(root, artifact_paths),
        }

    if not report_path.is_file():
        return _fail(
            "normal Poros formal report is missing; a TLC pass must be proven "
            f"by the strict report at {report_path.relative_to(root)}, not "
            "inferred from the absence of error strings in the log"
        )
    try:
        parsed = yaml.safe_load(
            report_path.read_text(encoding="utf-8", errors="replace")
        )
    except (OSError, yaml.YAMLError) as exc:
        return _fail(f"normal Poros formal report is unparseable: {exc}")
    if not isinstance(parsed, dict):
        return _fail("normal Poros formal report is not a mapping")
    results = parsed.get("results")
    if not isinstance(results, list) or not results:
        return _fail(
            "normal Poros formal report has no result rows; the TLC run "
            "produced no positive proof of any property"
        )
    for row in results:
        if not isinstance(row, dict):
            return _fail("normal Poros formal report row is not a mapping")
        property_id = row.get("property_id") or "<unknown>"
        status = row.get("status")
        proof_status = row.get("proof_status")
        if status != "pass" or proof_status != "freshly_proven":
            return _fail(
                f"normal Poros property {property_id} is not freshly proven "
                f"(status={status!r}, proof_status={proof_status!r}, "
                f"failure_class={row.get('failure_class')!r})"
            )
        if row.get("timed_out"):
            return _fail(
                f"normal Poros property {property_id} timed out; a timeout is "
                "not a pass"
            )
        distinct_states = row.get("distinct_states")
        if (
            not isinstance(distinct_states, int)
            or isinstance(distinct_states, bool)
            or distinct_states < DEFAULT_MIN_DISTINCT_STATES
        ):
            return _fail(
                f"normal Poros property {property_id} explored "
                f"{distinct_states!r} distinct states, below the vacuity floor "
                f"of {DEFAULT_MIN_DISTINCT_STATES}; the pass is vacuous"
            )
    return None


def _coverage_status(
    root: Path,
    registration: FormalModelRegistration,
    input_paths: list[Path],
) -> dict[str, Any]:
    if not registration.coverage_requirements:
        return {
            "ok": False,
            "status": "not_declared",
            "status_reason": (
                "formal assurance model declares no counterexample, "
                "non-vacuity, or waiver coverage"
            ),
            "coverage": [],
            "required_count": 0,
            "satisfied_count": 0,
            "waived_count": 0,
            "missing_count": 0,
            "stale_count": 0,
            "invalid_count": 0,
        }

    newest_input = max(path.stat().st_mtime for path in input_paths)
    coverage: list[dict[str, Any]] = []
    missing_count = 0
    stale_count = 0
    invalid_count = 0
    waived_count = 0
    satisfied_count = 0
    for requirement in registration.coverage_requirements:
        path = root / requirement.artifact
        entry = requirement.to_dict()
        entry["artifact_exists"] = path.is_file()
        entry["artifact_mtime"] = path.stat().st_mtime if path.exists() else None
        if requirement.waiver is not None:
            waived_count += 1
            satisfied_count += 1
            entry["status"] = "waived"
            entry["status_reason"] = (
                f"coverage waived by {requirement.waiver.owner}: "
                f"{requirement.waiver.reason}"
            )
        elif not path.is_file():
            entry["status"] = "missing"
            entry["status_reason"] = (
                f"required {requirement.kind} coverage artifact is missing: "
                f"{requirement.artifact}"
            )
            if requirement.required:
                missing_count += 1
        elif path.stat().st_size == 0:
            entry["status"] = "empty"
            entry["status_reason"] = (
                f"required {requirement.kind} coverage artifact is empty: "
                f"{requirement.artifact}"
            )
            if requirement.required:
                missing_count += 1
        elif path.stat().st_mtime < newest_input:
            entry["status"] = "stale"
            entry["status_reason"] = (
                f"required {requirement.kind} coverage artifact is older "
                "than model, fixture, or checker inputs: "
                f"{requirement.artifact}"
            )
            if requirement.required:
                stale_count += 1
        elif invalid_reason := _invalid_coverage_reason(requirement, path):
            entry["status"] = "invalid"
            entry["status_reason"] = invalid_reason
            if requirement.required:
                invalid_count += 1
        else:
            satisfied_count += 1
            entry["status"] = "present"
            entry["status_reason"] = (
                f"required {requirement.kind} coverage artifact is present "
                f"and fresh: {requirement.artifact}"
            )
        coverage.append(entry)

    weak_count = missing_count + stale_count + invalid_count
    if weak_count:
        status = "weak"
        reason = (
            "formal assurance negative coverage is incomplete: "
            f"{missing_count} missing/empty, {stale_count} stale, "
            f"{invalid_count} invalid"
        )
    elif waived_count:
        status = "waived"
        reason = "formal assurance negative coverage is explicitly waived"
    else:
        status = "complete"
        reason = "formal assurance negative coverage is present and fresh"
    return {
        "ok": weak_count == 0,
        "status": status,
        "status_reason": reason,
        "coverage": coverage,
        "required_count": sum(
            1 for requirement in registration.coverage_requirements
            if requirement.required
        ),
        "satisfied_count": satisfied_count,
        "waived_count": waived_count,
        "missing_count": missing_count,
        "stale_count": stale_count,
        "invalid_count": invalid_count,
    }


def _invalid_coverage_reason(
    requirement: FormalCoverageRequirement,
    path: Path,
) -> str | None:
    if requirement.kind == "intentional_counterexample":
        text = path.read_text(encoding="utf-8", errors="replace")
        if "Invariant" in text and "violated" in text:
            return None
        return (
            "intentional counterexample coverage artifact does not contain "
            "a TLC invariant violation"
        )
    if requirement.kind == "non_vacuity_refinement":
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return f"non-vacuity refinement artifact is not valid JSON: {exc}"
        if not isinstance(state, dict):
            return "non-vacuity refinement artifact must be a JSON object"
        try:
            from okg.substrate.exchange.refinement import (
                PorosRefinementError,
                check_refinement_state,
            )

            check_refinement_state(state)
        except PorosRefinementError as exc:
            return f"non-vacuity refinement artifact is invalid: {exc}"
        if not state.get("messages"):
            return "non-vacuity refinement artifact contains no messages"
        if not state.get("outbox"):
            return "non-vacuity refinement artifact contains no outbox evidence"
        if not (
            state.get("admissions")
            or state.get("blockedState")
            or state.get("publishedMessages")
        ):
            return (
                "non-vacuity refinement artifact contains no admitted, "
                "blocked, or published messages"
            )
        messages = {str(message) for message in state.get("messages", [])}
        covered_messages = (
            {str(message) for message in state.get("admissions", [])}
            | {str(message) for message in state.get("blockedState", [])}
            | {str(message) for message in state.get("publishedMessages", [])}
        )
        outbox_evidence = state.get("outboxEvidence")
        outbox_messages = (
            {str(message) for message in outbox_evidence}
            if isinstance(outbox_evidence, dict)
            else set()
        )
        if not messages & outbox_messages:
            return (
                "non-vacuity refinement artifact contains no runtime "
                "outbox evidence for its messages"
            )
        inbox_evidence = state.get("inboxEvidence")
        receipts = state.get("receipts")
        receiver_messages = (
            {
                str(message)
                for mapping in (inbox_evidence, receipts)
                if isinstance(mapping, dict)
                for message in mapping
            }
        )
        if not messages & receiver_messages:
            return (
                "non-vacuity refinement artifact contains no runtime "
                "receiver evidence for its messages"
            )
        runtime_rows = []
        for key in ("admissionRows", "generationRows", "liveRowEvidence", "proofRows"):
            rows = state.get(key)
            if isinstance(rows, list):
                runtime_rows.extend(
                    row for row in rows
                    if isinstance(row, dict)
                )
        runtime_row_messages = {
            str(row.get("message_id") or "")
            for row in runtime_rows
        }
        if not messages & runtime_row_messages:
            return (
                "non-vacuity refinement artifact contains no runtime rows "
                "for its messages"
            )
        covered_with_runtime_evidence = (
            covered_messages
            & outbox_messages
            & receiver_messages
            & runtime_row_messages
        )
        if not covered_with_runtime_evidence:
            return (
                "non-vacuity refinement artifact has no single admitted, "
                "blocked, or published message with matching runtime evidence"
            )
        return None
    return f"unknown formal coverage kind: {requirement.kind}"


def _service_trace_status(
    root: Path,
    registration: FormalModelRegistration,
    *,
    formal_artifact_paths: list[Path],
) -> dict[str, Any] | None:
    service_paths = [root / path for path in registration.service_trace_artifacts]
    if not service_paths:
        return {
            "latest_status": "missing",
            "status_reason": (
                "service-trace refinement is required, but the formal "
                "model registration declares no service-trace artifacts"
            ),
            "artifacts": _artifact_payload(root, formal_artifact_paths),
        }
    missing = [
        str(path.relative_to(root))
        for path in service_paths
        if not path.is_file()
    ]
    artifacts = _artifact_payload(root, formal_artifact_paths + service_paths)
    if missing:
        return {
            "latest_status": "missing",
            "status_reason": (
                "missing Poros service-trace refinement artifacts: "
                + ", ".join(missing)
            ),
            "artifacts": artifacts,
        }
    try:
        service_inputs = _existing_paths(
            root,
            [
                registration.model_path,
                "formal/poros/PorosExchange.cfg",
                "formal/poros/PorosExchangeCounterexample.tla",
                "formal/poros/PorosExchangeCounterexample.cfg",
                "formal/poros/fixtures/two-okg-trace.json",
                "scripts/check_formal_poros.py",
                "scripts/check_poros_refinement_fixture.py",
                "scripts/check_poros_refinement_from_db.py",
                "src/okg/substrate/exchange/refinement.py",
            ],
        )
    except FileNotFoundError as exc:
        return {
            "latest_status": "missing",
            "status_reason": str(exc),
            "artifacts": artifacts,
        }
    formal_inputs = [path for path in formal_artifact_paths if path.exists()]
    newest_input = max(
        [path.stat().st_mtime for path in service_inputs + formal_inputs]
    )
    oldest_service = min(path.stat().st_mtime for path in service_paths)
    if oldest_service < newest_input:
        return {
            "latest_status": "stale",
            "status_reason": (
                "Poros service-trace refinement artifacts are older than "
                "formal artifacts, model, fixture, or checker inputs"
            ),
            "artifacts": artifacts,
        }
    return None


def _artifact_payload(root: Path, paths: list[Path]) -> list[dict[str, Any]]:
    out = []
    for path in paths:
        out.append(
            {
                "path": str(path.relative_to(root)),
                "exists": path.is_file(),
                "mtime": path.stat().st_mtime if path.exists() else None,
            }
        )
    return out


def _existing_paths(root: Path, paths: list[str]) -> list[Path]:
    existing = [root / path for path in paths if (root / path).exists()]
    if not existing:
        raise FileNotFoundError("formal assurance model inputs are missing")
    return existing


def _repo_root(root: Path | None) -> Path:
    if root is not None:
        return root.expanduser().resolve()
    env_root = os.environ.get("OKG_FORMAL_ASSURANCE_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path(__file__).resolve().parents[3]


def _require_service_trace() -> bool:
    value = os.environ.get("OKG_FORMAL_ASSURANCE_REQUIRE_SERVICE_TRACE", "")
    return value.lower() in {"1", "true", "yes", "on"}
