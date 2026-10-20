"""Markdown review views for PACT manifests."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import yaml
from okg.substrate import fastyaml
from pydantic import Field, field_validator

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import envelope, fsm
from okg.substrate.pact.models import Evidence, PactManifest, PactModel
from okg.substrate.pact.store import (
    change_dir,
    intent_digest,
    load_manifest,
    load_manifest_file,
    state_digest,
)
from okg.substrate.pact.validation import derive_alignment, validate_manifest


VIEW_GENERATOR = "okg-pact-markdown-v0"
EMPTY_SHA256 = "sha256:" + hashlib.sha256(b"").hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PactMarkdownViewFrontmatter(PactModel):
    pact_view: Literal["markdown"] = "markdown"
    change: str
    source: str = "pact.yaml"
    source_digest: str
    state_digest: str | None = None
    generator: str = VIEW_GENERATOR
    generated_at: str = Field(default_factory=_now)
    canonical: Literal[False] = False
    requirement_count: int
    task_count: int
    test_plan_count: int
    blocker_count: int
    design_count: int = 0
    design_digest: str = EMPTY_SHA256

    @field_validator("source_digest", "state_digest", "design_digest")
    @classmethod
    def _digest(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not value.startswith("sha256:"):
            raise ValueError("digest must start with sha256:")
        return value


@dataclass(frozen=True)
class DesignRecord:
    title: str
    design_id: str
    status: str
    refs: list[str]
    path: str
    decision: str


def dump_frontmatter(frontmatter: PactMarkdownViewFrontmatter) -> str:
    data = frontmatter.model_dump(mode="json", exclude_none=True)
    return "---\n" + yaml.safe_dump(data, sort_keys=False).strip() + "\n---\n"


def parse_frontmatter(text: str) -> PactMarkdownViewFrontmatter:
    if not text.startswith("---\n"):
        raise SubstrateError("PACT Markdown view is missing YAML frontmatter")
    end = text.find("\n---", 4)
    if end == -1:
        raise SubstrateError("PACT Markdown view frontmatter is not closed")
    raw = fastyaml.safe_load(text[4:end]) or {}
    if not isinstance(raw, dict):
        raise SubstrateError("PACT Markdown view frontmatter must be a mapping")
    return PactMarkdownViewFrontmatter.model_validate(raw)


def view_path(repo_root: Path, change_id: str) -> Path:
    return _change_root(repo_root, change_id) / "pact.md"


def _change_root(repo_root: Path, change_id: str) -> Path:
    active = change_dir(repo_root, change_id)
    if active.exists():
        return active
    archived = repo_root / "pact" / "archive" / change_id
    if archived.exists():
        return archived
    return active


def _load_manifest_for_view(repo_root: Path, change_id: str) -> PactManifest:
    active = change_dir(repo_root, change_id) / "pact.yaml"
    if active.is_file():
        return load_manifest(repo_root, change_id)
    archived = repo_root / "pact" / "archive" / change_id / "pact.yaml"
    if archived.is_file():
        return load_manifest_file(archived)
    return load_manifest(repo_root, change_id)


def render_markdown_view(
    manifest: PactManifest,
    *,
    repo_root: Path | None = None,
) -> str:
    alignment = derive_alignment(manifest, repo_root=repo_root)
    report = validate_manifest(manifest, repo_root=repo_root)
    design_records = _discover_design_records(repo_root, manifest.change.id)
    frontmatter = PactMarkdownViewFrontmatter(
        change=manifest.change.id,
        source_digest=intent_digest(manifest),
        state_digest=state_digest(manifest),
        requirement_count=len(manifest.requirements),
        task_count=len(manifest.tasks),
        test_plan_count=len(manifest.test_plan),
        blocker_count=_blocker_count(alignment),
        design_count=len(design_records),
        design_digest=_design_digest(repo_root, manifest.change.id),
    )
    body = "\n".join([
        "",
        f"# {manifest.change.title}",
        "",
        "This is a non-canonical human review view generated from "
        + "[pact.yaml](pact.yaml). The canonical intended state is `pact.yaml`.",
        "",
        "## Operator Brief",
        "",
        _operator_brief_section(manifest, alignment),
        "",
        "## Decision",
        "",
        manifest.change.rationale or "Review the PACT change.",
        "",
        "## Why",
        "",
        manifest.change.root_cause or "No root cause recorded.",
        "",
        "## Non-Goals",
        "",
        _paragraph_or_bullets(manifest.prose.get("non_goals")),
        "",
        "## Proposed Changes",
        "",
        _proposed_changes_section(manifest),
        "",
        "## Design Records",
        "",
        _design_records_section(design_records),
        "",
        "## Lifecycle Gates",
        "",
        _lifecycle_gates_section(alignment),
        "",
        "## Reviewer Actions",
        "",
        _reviewer_actions_section(manifest, alignment),
        "",
        "## Requirements",
        "",
        _requirements_table(manifest),
        "",
        "## Requirement Details",
        "",
        _requirement_details_section(manifest),
        "",
        "## Acceptance And Evidence",
        "",
        _evidence_table(manifest, alignment),
        "",
        "## Task Graph",
        "",
        _task_table(manifest, alignment),
        "",
        "## Blockers",
        "",
        _blockers_section(manifest, alignment),
        "",
        "## Waivers",
        "",
        _waivers_section(manifest),
        "",
        "## Loop Records",
        "",
        _loop_records_section(manifest),
        "",
        "## Risks And Review Questions",
        "",
        _risks_and_questions_section(manifest),
        "",
        "## Validation State",
        "",
        _validation_section(report),
        "",
        "## Next Work",
        "",
        _next_work_section(manifest, alignment),
        "",
    ])
    return dump_frontmatter(frontmatter) + body


def write_markdown_view(repo_root: Path, change_id: str) -> tuple[Path, str]:
    manifest = _load_manifest_for_view(repo_root, change_id)
    text = render_markdown_view(manifest, repo_root=repo_root)
    path = view_path(repo_root, change_id)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp:
            tmp.write(text)
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            # os.replace already moved the temp file onto the destination.
            pass
    return path, parse_frontmatter(text).source_digest


def check_markdown_view(repo_root: Path, change_id: str) -> dict:
    manifest = _load_manifest_for_view(repo_root, change_id)
    path = view_path(repo_root, change_id)
    if not path.is_file():
        return _view_report(
            ok=False,
            status="missing",
            path=path,
            issues=[_issue(
                "missing_view",
                "pact.md is missing",
                "okg pact view <change-id> --format=markdown",
            )],
        )

    text = path.read_text(encoding="utf-8")
    try:
        frontmatter = parse_frontmatter(text)
    except SubstrateError as exc:
        return _view_report(
            ok=False,
            status="invalid",
            path=path,
            issues=[_issue(
                "invalid_view",
                exc.message,
                "okg pact view <change-id> --format=markdown",
            )],
        )

    alignment = derive_alignment(manifest, repo_root=repo_root)
    expected_digest = intent_digest(manifest)
    expected_state_digest = state_digest(manifest)
    expected_blockers = _blocker_count(alignment)
    expected_design_digest = _design_digest(repo_root, change_id)
    expected_design_count = len(_discover_design_records(repo_root, change_id))
    issues: list[dict[str, str]] = []
    if frontmatter.change != manifest.change.id:
        issues.append(_issue(
            "wrong_change",
            f"pact.md is for {frontmatter.change}, expected {manifest.change.id}",
            "okg pact view <change-id> --format=markdown",
        ))
    if frontmatter.source != "pact.yaml":
        issues.append(_issue(
            "wrong_source",
            f"pact.md source is {frontmatter.source}, expected pact.yaml",
            "okg pact view <change-id> --format=markdown",
        ))
    if frontmatter.source_digest != expected_digest:
        issues.append(_issue(
            "stale_view",
            "pact.md was generated from a different pact.yaml digest",
            "okg pact view <change-id> --format=markdown",
        ))
    target_version = manifest.target_pact_version or manifest.pact_version
    if (
        target_version >= 3
        and frontmatter.state_digest
        and frontmatter.state_digest != expected_state_digest
    ):
        issues.append(_issue(
            "stale_view",
            "pact.md was generated from a different pact.yaml state",
            "okg pact view <change-id> --format=markdown",
        ))
    if frontmatter.design_digest != expected_design_digest:
        issues.append(_issue(
            "stale_view",
            "pact.md was generated from a different designs/ digest",
            "okg pact view <change-id> --format=markdown",
        ))
    expected_counts = {
        "requirement_count": len(manifest.requirements),
        "task_count": len(manifest.tasks),
        "test_plan_count": len(manifest.test_plan),
        "blocker_count": expected_blockers,
        "design_count": expected_design_count,
    }
    for field, expected in expected_counts.items():
        if getattr(frontmatter, field) != expected:
            issues.append(_issue(
                "count_mismatch",
                f"{field} is {getattr(frontmatter, field)}, expected {expected}",
                "okg pact view <change-id> --format=markdown",
            ))
    if f"Blocker count: {expected_blockers}" not in text:
        issues.append(_issue(
            "blocker_summary_mismatch",
            f"pact.md does not contain Blocker count: {expected_blockers}",
            "okg pact view <change-id> --format=markdown",
        ))
    return _view_report(
        ok=not issues,
        status="fresh" if not issues else "stale",
        path=path,
        issues=issues,
        frontmatter=frontmatter,
    )


def _requirements_table(manifest: PactManifest) -> str:
    rows = ["| Requirement | Intent |", "|---|---|"]
    for req in manifest.requirements:
        rows.append(
            f"| `{req.id}` | {_one_line(req.title or req.statement)} |"
        )
    return "\n".join(rows)


def _requirement_details_section(manifest: PactManifest) -> str:
    if not manifest.requirements:
        return "No requirements recorded."
    lines: list[str] = []
    tests_by_requirement: dict[str, list] = {}
    for entry in manifest.test_plan:
        tests_by_requirement.setdefault(entry.requirement, []).append(entry)
    acceptance_to_requirement = {
        acceptance.id: req.id
        for req in manifest.requirements
        for acceptance in req.acceptance
    }
    for entry in manifest.test_plan:
        req_id = acceptance_to_requirement.get(entry.requirement)
        if req_id:
            tests_by_requirement.setdefault(req_id, []).append(entry)

    for req in manifest.requirements:
        title = f" {req.title}" if req.title else ""
        lines.extend([f"### `{req.id}`{title}", "", req.statement, ""])
        if req.scenarios:
            lines.extend(["Scenarios:", ""])
            for scenario in req.scenarios:
                lines.append(
                    f"- Scenario: `{scenario.id}` {_one_line(scenario.title)}"
                )
                for prefix, values in (
                    ("GIVEN", scenario.given),
                    ("WHEN", scenario.when),
                    ("THEN", scenario.then),
                    ("AND", scenario.and_),
                ):
                    for value in values:
                        lines.append(f"  - {prefix} {_one_line(value)}")
            lines.append("")
        if req.acceptance:
            lines.extend(["Acceptance:", ""])
            for acceptance in req.acceptance:
                evidence = ", ".join(f"`{item}`" for item in acceptance.evidence)
                lines.append(
                    f"- Acceptance: `{acceptance.id}` {_one_line(acceptance.description)}"
                )
                lines.append(f"  - Evidence: {evidence or 'none'}")
            lines.append("")
        planned = tests_by_requirement.get(req.id, [])
        if planned:
            lines.extend(["Planned Proof:", "", "| Test | Command |", "|---|---|"])
            for entry in planned:
                lines.append(
                    f"| `{entry.test_id}` | `{entry.command or 'not recorded'}` |"
                )
            lines.append("")
    return "\n".join(lines).rstrip()


def _design_records_section(records: list[DesignRecord]) -> str:
    if not records:
        return "No design records recorded."
    rows = [
        "| Design | ID | Status | Refs | Decision |",
        "|---|---|---|---|---|",
    ]
    for record in records:
        refs = ", ".join(f"`{ref}`" for ref in record.refs) or "none"
        rows.append(
            "| "
            f"[{_one_line(record.title)}]({record.path}) | "
            f"`{record.design_id}` | "
            f"{_one_line(record.status)} | "
            f"{refs} | "
            f"{_one_line(record.decision) or 'not recorded'} |"
        )
    return "\n".join(rows)


def _proposed_changes_section(manifest: PactManifest) -> str:
    lines: list[str] = []
    pact_shape = (
        manifest.change.method_metadata.get("pact_shape")
        or manifest.prose.get("pact_shape")
    )
    if pact_shape:
        lines.append(f"- PACT shape: `{pact_shape}`")
    if manifest.change.affected_areas:
        lines.append(
            "- Affected areas: " + ", ".join(f"`{area}`" for area in manifest.change.affected_areas)
        )
    if manifest.change.affected_code:
        lines.append(
            "- Affected code: " + ", ".join(f"`{path}`" for path in manifest.change.affected_code)
        )
    if manifest.change.migration_notes:
        lines.append(f"- Migration: {manifest.change.migration_notes}")
    if not lines:
        lines.append("- No affected areas or migration notes recorded.")
    return "\n".join(lines)


def _risks_and_questions_section(manifest: PactManifest) -> str:
    risks = manifest.prose.get("risks")
    questions = manifest.prose.get("review_questions") or manifest.prose.get("open_questions")
    if not risks and not questions:
        return "No review questions recorded."
    lines: list[str] = []
    if risks:
        lines.extend(["Risks:", "", risks])
    if questions:
        if lines:
            lines.append("")
        lines.extend(["Review questions:", "", questions])
    return "\n".join(lines)


def _lifecycle_gates_section(alignment: dict) -> str:
    lifecycle = alignment.get("lifecycle", {})
    rows = ["| Gate | Ready |", "|---|---|"]
    for gate in ("proposal", "implementation", "verification"):
        rows.append(f"| {gate} | {'yes' if lifecycle.get(gate) else 'no'} |")
    return "\n".join(rows)


def _reviewer_actions_section(manifest: PactManifest, alignment: dict) -> str:
    lines: list[str] = []
    if alignment.get("lifecycle", {}).get("implementation"):
        lines.append("- Intent has digest-bound approval; implementation may proceed.")
    else:
        lines.append(
            "- Review the decision, non-goals, risks, and task graph, then record "
            f"`okg pact evidence approve {manifest.change.id} --reviewer <name>` when intent is approved."
        )
    if alignment.get("lifecycle", {}).get("verification"):
        lines.append("- Verification evidence satisfies the declared completion gate.")
    else:
        lines.append("- Do not treat planned tests as verification evidence.")
    return "\n".join(lines)


def _operator_brief_section(manifest: PactManifest, alignment: dict) -> str:
    brief = operator_brief(manifest, alignment)
    lifecycle = brief["lifecycle_gates"]
    gates = ", ".join(
        f"{gate}={'yes' if lifecycle.get(gate) else 'no'}"
        for gate in ("proposal", "implementation", "verification")
    )
    choices = ", ".join(f"`{choice}`" for choice in brief["human_choices"])
    return "\n".join([
        f"- Decision needed: {brief['decision_needed']}.",
        f"- Current state: `{brief['current_state']}`; {gates}; blocker count {brief['blocker_count']}.",
        f"- Next action: {brief['next_action']}",
        f"- Human choices: {choices}.",
        f"- Proof state: {brief['proof_state']}",
    ])


def _brief_fsm_state(manifest: PactManifest, alignment: dict) -> str:
    """The lifecycle state this brief describes, in the FSM's vocabulary.

    Derived through `fsm.derive_state` itself, from the same manifest-side
    inputs `review.manifest_condition` feeds it, so the brief cannot invent a
    state the machine does not agree with. A proxy built out of the three
    lifecycle gates was tried and was wrong in both directions: it read a
    tier-1 change with a satisfied proposal gate as `proposed` (the FSM says
    `in_progress`, where `revise` and `approve` are both illegal), and a
    tier-2 change with implementation satisfied and no ledger approval as
    `in_progress` (the FSM says `proposed`, where `approve` is exactly what
    the operator owes).

    The decision list is empty because the brief is a file-side view with no
    ledger read. That is the conservative direction: with no recorded
    approval a tier-2+ change stays at its go-ahead gate, which is what an
    unread ledger means. Callers that DID read the ledger pass their derived
    state to `operator_brief` instead of relying on this.
    """

    req_align = alignment.get("requirements") or {}
    task_align = alignment.get("tasks") or {}
    evidence_satisfied = all(
        (entry or {}).get("satisfaction_status") == "satisfied"
        for entry in req_align.values()
    )
    tasks_terminal = bool(manifest.tasks) and all(
        task.status == "done"
        or (task_align.get(task.id) or {}).get("satisfaction_status") == "satisfied"
        for task in manifest.tasks
    )
    return fsm.derive_state(
        # Validity is reported in its own section of the view; treating an
        # invalid manifest as `draft` here would only hide the verdict set.
        valid=True,
        tier=manifest.change.tier or 0,
        tasks_terminal=tasks_terminal,
        evidence_satisfied=evidence_satisfied,
        decisions=[],
    ).state


def operator_brief(
    manifest: PactManifest,
    alignment: dict,
    *,
    state: str | None = None,
) -> dict:
    """Return the structured brief shared by pact.md and CLI surfaces.

    `state` is the caller's authoritative lifecycle state when it has one -
    the CLI reads the decision ledger and must not publish a second, quieter
    answer in the same payload. Without it the brief derives the state from
    the manifest alone.
    """

    lifecycle = alignment.get("lifecycle", {})
    # PACT v4 verdict vocabulary (standardize-pact-review-interface §1):
    # approve / revise <note> / reject / supersede <target> / defer -- but only
    # the subset the FSM will actually accept HERE. This brief used to print
    # the whole vocabulary in every state, and an `in_progress` change
    # advertised `revise`, which `decide` then refused with "verdict revise is
    # not legal in state in_progress". Three operator approval rounds were
    # spent on that loop (issue #1476). The legal set is now read from the same
    # transition table the refusal reads, so the brief cannot offer a verdict
    # the next call rejects.
    brief_state = state or _brief_fsm_state(manifest, alignment)
    if brief_state not in fsm.load_table().get("states", {}):
        # A legacy authored status is not an FSM state. Fall back to the
        # derivation rather than asking the table about a name it never had.
        brief_state = _brief_fsm_state(manifest, alignment)
    human_choices = [
        verdict
        for verdict in fsm.legal_verdicts(brief_state)
        if verdict in fsm.HUMAN_VERDICTS
    ]
    if not human_choices:
        decision = f"none available in `{brief_state}`"
    elif "approve" in human_choices:
        approve = (
            "approve (retires)" if brief_state == "awaiting_review" else "approve"
        )
        rest = [c for c in human_choices if c != "approve"]
        decision = ", ".join([approve, *rest[:-1]]) + f", or {rest[-1]}"
    else:
        decision = (
            f"none pending (`{brief_state}`); "
            + " or ".join(human_choices)
            + " to redirect"
        )

    proof_gaps: list[str] = []
    proof_gap_details: list[dict[str, object]] = []
    for req in manifest.requirements:
        state = alignment.get("requirements", {}).get(req.id, {})
        missing = state.get("satisfaction_missing") or []
        if missing:
            proof_gaps.append(f"{req.id} needs {', '.join(missing)}")
            proof_gap_details.append({
                "requirement": req.id,
                "missing": list(missing),
            })
    if not proof_gaps:
        proof_state = "No unsatisfied requirement evidence gaps."
    else:
        proof_state = "; ".join(proof_gaps[:4])
        if len(proof_gaps) > 4:
            proof_state += f"; {len(proof_gaps) - 4} more gap(s)"

    next_work = _next_work_section(manifest, alignment)
    return {
        "decision_needed": decision,
        "current_state": manifest.change.status,
        "lifecycle_gates": {
            "proposal": bool(lifecycle.get("proposal")),
            "implementation": bool(lifecycle.get("implementation")),
            "verification": bool(lifecycle.get("verification")),
        },
        "blocker_count": _blocker_count(alignment),
        "next_action": (
            next_work.splitlines()[0].removeprefix("- ")
            if next_work
            else "No unblocked tasks."
        ),
        "human_choices": human_choices,
        "human_choices_state": brief_state,
        "proof_state": proof_state,
        "proof_gaps": proof_gap_details,
    }


def _evidence_table(manifest: PactManifest, alignment: dict) -> str:
    rows = [
        "| Requirement | Required Evidence | Traceability | Satisfaction |",
        "|---|---|---|---|",
    ]
    for req in manifest.requirements:
        state = alignment["requirements"].get(req.id, {})
        rows.append(
            "| "
            f"`{req.id}` | "
            f"{', '.join(req.evidence_policy.required) or 'none'} | "
            f"{state.get('traceability_status', 'unknown')} | "
            f"{state.get('satisfaction_status', 'unknown')} |"
        )
    return "\n".join(rows)


def _task_table(manifest: PactManifest, alignment: dict) -> str:
    rows = ["| Task | Status | Blockers | Verification |", "|---|---|---|---|"]
    for task in manifest.tasks:
        state = alignment["tasks"].get(task.id, {})
        blockers = ", ".join(f"`{item}`" for item in state.get("blockers", []))
        rows.append(
            "| "
            f"`{task.id}` | "
            f"{state.get('status', task.status)} | "
            f"{blockers or 'none'} | "
            f"`{task.verification}` |"
        )
    return "\n".join(rows)


def _blockers_section(manifest: PactManifest, alignment: dict) -> str:
    lines: list[str] = [f"Blocker count: {_blocker_count(alignment)}"]
    for task in manifest.tasks:
        blockers = alignment["tasks"].get(task.id, {}).get("blockers", [])
        if blockers:
            lines.append(
                f"- `{task.id}` is blocked by "
                + ", ".join(f"`{blocker}`" for blocker in blockers)
            )
    if len(lines) == 1:
        lines.append("- No blocking dependencies are currently active.")
    return "\n".join(lines)


def _waivers_section(manifest: PactManifest) -> str:
    if not manifest.waivers:
        return "No waivers recorded."
    return "\n".join(
        f"- `{waiver.id}` for `{waiver.scope}`: {waiver.reason}"
        for waiver in manifest.waivers
    )


def _loop_records_section(manifest: PactManifest) -> str:
    loop_records = [
        evidence for evidence in manifest.evidence
        if evidence.kind == "loop_record"
    ]
    if not loop_records:
        return "No loop records recorded."
    lines = [
        "| Record | Observation | Failure Mode | Ranked Improvement | Next Observation |",
        "|---|---|---|---|---|",
    ]
    for evidence in loop_records:
        data = evidence.data or {}
        lines.append(
            "| "
            f"`{evidence.id}` | "
            f"{_one_line(str(data.get('observation') or evidence.summary or ''))} | "
            f"{_one_line(str(data.get('failure_mode') or 'unknown'))} | "
            f"{_one_line(str(data.get('ranked_improvement') or 'not recorded'))} | "
            f"{_one_line(str(data.get('next_observation') or 'not recorded'))} |"
        )
    lines.extend(["", "### Loop Record Details", ""])
    for evidence in loop_records:
        lines.extend(_loop_record_detail(evidence))
        lines.append("")
    return "\n".join(lines).rstrip()


def _loop_record_detail(evidence: Evidence) -> list[str]:
    data = evidence.data or {}
    lines = [
        f"#### `{evidence.id}`",
        "",
        f"- Observation: {_one_line(str(data.get('observation') or evidence.summary or 'not recorded'))}",
        f"- Failure mode: `{_one_line(str(data.get('failure_mode') or 'unknown'))}`",
        f"- Ranked improvement: {_one_line(str(data.get('ranked_improvement') or 'not recorded'))}",
        f"- Decision: {_one_line(str(data.get('decision') or 'not recorded'))}",
        f"- Verification: {_one_line(str(data.get('verification') or 'not recorded'))}",
        f"- Smallest affected surface: {_one_line(str(data.get('smallest_affected_surface') or 'not recorded'))}",
        f"- Next observation: {_one_line(str(data.get('next_observation') or 'not recorded'))}",
    ]
    ledger_rows = _friction_ledger_rows(data.get("friction_ledger"))
    if ledger_rows:
        lines.extend(["", "Friction ledger:", "", *ledger_rows])
    return lines


def _discover_design_records(
    repo_root: Path | None,
    change_id: str,
) -> list[DesignRecord]:
    if repo_root is None:
        return []
    root = _change_root(repo_root, change_id)
    design_dir = root / "designs"
    if not design_dir.is_dir():
        return []
    records = []
    for path in sorted(design_dir.glob("*.md")):
        records.append(_parse_design_record(path, root))
    return records


def _parse_design_record(path: Path, change_root: Path) -> DesignRecord:
    text = path.read_text(encoding="utf-8")
    title_match = re.search(r"^#\s+(.+)$", text, re.MULTILINE)
    title = title_match.group(1).strip() if title_match else path.stem
    header = text[: text.find("\n## ") if "\n## " in text else len(text)]
    fields: dict[str, str] = {}
    for line in header.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key.strip().lower()] = value.strip()
    refs = [
        item.strip()
        for item in fields.get("refs", "").split(",")
        if item.strip()
    ]
    return DesignRecord(
        title=title,
        design_id=fields.get("design-id") or f"design.{path.stem}",
        status=fields.get("status") or "unknown",
        refs=refs,
        path=path.relative_to(change_root).as_posix(),
        decision=_section(text, "Decision") or "not recorded",
    )


def _design_digest(repo_root: Path | None, change_id: str) -> str:
    if repo_root is None:
        return EMPTY_SHA256
    root = _change_root(repo_root, change_id)
    design_dir = root / "designs"
    if not design_dir.is_dir():
        return EMPTY_SHA256
    h = hashlib.sha256()
    for path in sorted(design_dir.glob("*.md")):
        rel = path.relative_to(root).as_posix()
        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        h.update(path.read_bytes())
        h.update(b"\0")
    return "sha256:" + h.hexdigest()


def _section(text: str, heading: str) -> str:
    pattern = rf"^##\s+{re.escape(heading)}\s*$"
    match = re.search(pattern, text, flags=re.MULTILINE)
    if not match:
        return ""
    start = match.end()
    next_match = re.search(r"^##\s+", text[start:], flags=re.MULTILINE)
    end = start + next_match.start() if next_match else len(text)
    return text[start:end].strip()


def _friction_ledger_rows(value: object) -> list[str]:
    if not isinstance(value, list) or not value:
        return []
    rows = [
        "| Observation | Failure Mode | Severity | Frequency | Candidate Improvement | Verification |",
        "|---|---|---|---|---|---|",
    ]
    for item in value:
        if not isinstance(item, dict):
            rows.append(
                f"| {_one_line(str(item))} | unknown | unknown | unknown | not recorded | not recorded |"
            )
            continue
        rows.append(
            "| "
            f"{_one_line(str(item.get('observation') or 'not recorded'))} | "
            f"{_one_line(str(item.get('failure_mode') or 'unknown'))} | "
            f"{_one_line(str(item.get('severity') or 'unknown'))} | "
            f"{_one_line(str(item.get('frequency') or 'unknown'))} | "
            f"{_one_line(str(item.get('candidate_improvement') or 'not recorded'))} | "
            f"{_one_line(str(item.get('verification') or 'not recorded'))} |"
        )
    return rows


def _validation_section(report) -> str:
    if report.ok:
        return "PACT validation passes."
    return "\n".join(
        f"- `{issue.code}` at `{issue.path}`: {issue.message}"
        for issue in report.issues
    )


def _next_work_section(manifest: PactManifest, alignment: dict) -> str:
    lines = []
    for task in manifest.tasks:
        state = alignment["tasks"].get(task.id, {})
        if task.status in {"todo", "blocked"} and not state.get("blockers"):
            lines.append(f"- `{task.id}`: {task.title}")
    return "\n".join(lines) if lines else "No unblocked tasks."


def _blocker_count(alignment: dict) -> int:
    return sum(
        len(state.get("blockers", []))
        for state in alignment.get("tasks", {}).values()
    )


def _paragraph_or_bullets(value: str | None) -> str:
    return value or "No non-goals recorded."


def _one_line(value: str) -> str:
    return " ".join(value.split()).replace("|", "\\|")


def _issue(code: str, message: str, fix_hint: str) -> dict[str, str]:
    return {"code": code, "message": message, "fix_hint": fix_hint}


def _view_report(
    *,
    ok: bool,
    status: str,
    path: Path,
    issues: list[dict[str, str]],
    frontmatter: PactMarkdownViewFrontmatter | None = None,
) -> dict:
    return envelope.component(
        ok=ok,
        status=status,
        path=str(path),
        issues=issues,
        **(
            {"frontmatter": frontmatter.model_dump(mode="json")}
            if frontmatter is not None
            else {}
        ),
    )
