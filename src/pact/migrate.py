"""Rewrite a v4 manifest into the v5 shape, carrying intent and disclosing the rest.

Reviewed intent is kept: id, title, tier, rationale + root cause, affected
code, requirements, tasks. v4 requirement ``acceptance`` and ``scenarios`` fold
into the owning requirement's ``statement``; v4 ``test_plan`` rows fold into the
owning task's ``verification``. The text is copied verbatim under a heading, so
the v5 schema does not grow a field and no reviewed sentence is lost.

``change.migration_notes`` is human prose about the change itself, so it folds
into ``why`` under a ``Migration notes:`` heading.

Everything the v5 schema has no place for is *disclosed*, never dropped in
silence: each removed key -- top-level, inside ``change``, or on a requirement,
task or evidence row -- each unused evidence ref, each task ``status`` and
``environment`` becomes a note, and the same facts land in the machine-readable
migration report (``pact migrate --report``).

``formal[]`` rows fold into ``evidence[]`` as ``formal_check`` rows. Such a row
is ``pass`` only when the v4 row records its own passing status *and* names an
artifact that resolves here, whose sha256 is computed now; otherwise it is
``fail`` with a note naming the binding. Either way it carries a
``formal_carried:<id>:<status>`` note, so a formally proven change never
becomes unproven quietly.

Migrated evidence has no ``head`` and is therefore unbound: history is kept, but
nothing is claimed as proven until it is re-run on a known commit. Ledger
decisions are added from an export (``decisions.json``) when provided.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from pact.digest import file_sha256
from pact.schema import EVIDENCE_KINDS, Decision, Evidence, Manifest, Requirement, Task
from pact.store import PactError, inside

_KIND_MAP = {
    "planned_test": None,
    "executed_test": "executed_test",
    "pytest": "executed_test",
    "junit": "executed_test",
    "formal_check": "formal_check",
    "graph_report": "published_generation",
    "published_generation": "published_generation",
    "benchmark_report": "manual",
    "design_review": "design_review",
    "intent_review": "design_review",
    "operator_report": "manual",
    "manual": "manual",
    "manual_visual": "manual",
    "ui_visual": "manual",
    "ui_interaction": "manual",
    "ui_quality_gate": "manual",
    "loop_record": "manual",
    "waiver": None,
}

# Keys the migration reads. Anything else present in a v4 manifest is reported
# as dropped, so a field this code has never seen cannot vanish unannounced.
_TOP_CONSUMED = {"pact_version", "target_pact_version", "change", "requirements", "tasks", "test_plan", "formal", "evidence", "dependencies", "waivers"}
_CHANGE_CONSUMED = {"id", "title", "tier", "rationale", "root_cause", "affected_code", "migration_notes"}
_REQ_CONSUMED = {"id", "title", "statement", "acceptance", "scenarios"}
# `status` and `environment` get their own named notes below, so they are not
# repeated in the generic per-task sweep.
_TASK_CONSUMED = {"id", "title", "verification", "requirements", "requirement", "status", "environment"}
_EVIDENCE_CONSUMED = {"id", "kind", "status", "refs", "artifact", "command", "summary", "data"}


def _text(*parts: object) -> str:
    return "\n\n".join(str(p).strip() for p in parts if p and str(p).strip())


def _slug(value: str) -> str:
    out = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in value.lower())
    return out.strip("-") or "x"


def _lines(prefix: str, values: object) -> list[str]:
    if isinstance(values, str):
        values = [values]
    return [f"  {prefix} {str(v).strip()}" for v in (values or []) if str(v).strip()]


def _acceptance_block(rows: list[dict]) -> str:
    """Render v4 acceptance criteria verbatim under an ``Acceptance:`` heading."""
    out = ["Acceptance:"]
    for row in rows:
        head = f"- {row.get('id')}: {str(row.get('description') or '').strip()}".rstrip(":").rstrip()
        tokens = [str(t) for t in row.get("evidence") or []]
        if tokens:
            head += f" [evidence: {', '.join(tokens)}]"
        out.append(head)
        for arm in row.get("arms") or []:
            if isinstance(arm, dict):
                arm_tokens = ", ".join(str(t) for t in arm.get("evidence") or [])
                out.append(f"  - arm {arm.get('id')}: {str(arm.get('description') or '').strip()}" + (f" [evidence: {arm_tokens}]" if arm_tokens else ""))
    return "\n".join(out)


def _scenario_block(rows: list[dict]) -> str:
    """Render v4 scenarios verbatim under a ``Scenarios:`` heading."""
    out = ["Scenarios:"]
    for row in rows:
        out.append(f"- {row.get('id')} {str(row.get('title') or '').strip()}".rstrip())
        out.extend(_lines("Given", row.get("given")))
        out.extend(_lines("When", row.get("when")))
        out.extend(_lines("Then", row.get("then")))
        out.extend(_lines("And", row.get("and")))
    return "\n".join(out)


def _test_plan_block(rows: list[dict]) -> str:
    """Render v4 test-plan entries verbatim under a ``Test plan:`` heading."""
    out = ["Test plan:"]
    for row in rows:
        line = f"- {row.get('test_id')}"
        if row.get("command"):
            line += f" ({row.get('command')})"
        if row.get("fixture"):
            line += f" fixture: {row.get('fixture')}"
        tokens = [str(t) for t in row.get("evidence") or []]
        if tokens:
            line += f" [evidence: {', '.join(tokens)}]"
        out.append(line)
    return "\n".join(out)


def _dict_rows(value: object) -> list[dict]:
    return [row for row in (value or []) if isinstance(row, dict)] if isinstance(value, list) else []


def _is_manifest(relative: str) -> bool:
    """True for a PACT manifest path this migration rewrites."""
    parts = PurePosixPath(relative).parts
    return len(parts) == 4 and parts[0] == "pact" and parts[1] in {"changes", "archive"} and parts[3] == "pact.yaml"


def _resolve_artifact(root: Path | None, artifact: object) -> tuple[str | None, str | None, str]:
    """``(artifact, sha256, problem)`` for a v4-declared artifact path.

    A manifest is data, not a trusted path source: an absolute path, a ``..``
    hop or a symlink out of the tree would let the migration hash a file the
    repository does not contain and mint a passing receipt for it. Such a row
    is refused outright rather than merely unresolved.
    """
    if not artifact:
        return None, None, "no_artifact"
    relative = str(artifact)
    if root is None:
        return None, None, "unresolved"
    target = inside(root, relative)
    if target is None:
        return None, None, "outside_repo"
    if not target.is_file():
        return None, None, "unresolved"
    if _is_manifest(relative):
        # The migration rewrites every manifest in scope, so a digest taken
        # here is stale the moment it is written -- and when the artifact is
        # the row's OWN manifest, writing the digest changes the file the
        # digest describes, so no value can ever be correct. The path is
        # disclosed in the row's note; it is not carried as an artifact,
        # because `pact check` digests every artifact it is given and an
        # undigestable one refuses the whole change.
        return None, None, "artifact_is_manifest"
    return relative, file_sha256(target.read_bytes()), ""


@dataclass
class MigrationResult:
    """One migrated manifest plus the full account of what moved and what went."""

    manifest: Manifest
    notes: list[str] = field(default_factory=list)
    folded: dict[str, int] = field(default_factory=dict)
    dropped: dict[str, object] = field(default_factory=dict)
    formal: list[dict] = field(default_factory=list)
    decisions: int = 0

    def as_report(self) -> dict:
        return {"folded": self.folded, "dropped": self.dropped, "formal": self.formal, "decisions": self.decisions, "notes": self.notes}


def migrate_change(raw: dict, *, root: Path | None = None, decisions: list[dict] | None = None) -> MigrationResult:
    notes: list[str] = []
    folded: dict[str, int] = {"acceptance": 0, "scenarios": 0, "test_plan": 0, "migration_notes": 0, "requirements_touched": 0, "tasks_touched": 0}
    dropped: dict[str, object] = {}
    change = raw.get("change") or {}
    tier = change.get("tier")
    if not isinstance(tier, int):
        tier = 1
        notes.append("tier_defaulted_to_1")

    # --- requirements, with acceptance and scenarios folded into the statement
    raw_reqs = [r for r in _dict_rows(raw.get("requirements")) if r.get("id")]
    requirements: list[Requirement] = []
    by_req_extra: dict[str, list[str]] = {}
    for r in raw_reqs:
        rid = _slug(str(r.get("id")))
        requirements.append(Requirement(id=rid, statement=_text(r.get("title"), r.get("statement")) or "(no statement)"))
        by_req_extra.setdefault(rid, [])
    if not requirements:
        requirements = [Requirement(id="req.migrated", statement=_text(change.get("title"), change.get("rationale")) or "(migrated)")]
        by_req_extra[requirements[0].id] = []
        notes.append("requirement_synthesised")
    req_ids = {r.id for r in requirements}
    first_req = requirements[0].id

    # Requirement-scoped in v4; a top-level list is accepted too and linked by
    # its own `requirement` key, falling back to the first requirement.
    def _collect(key: str) -> dict[str, list[dict]]:
        owned: dict[str, list[dict]] = {}
        for r in raw_reqs:
            rid = _slug(str(r.get("id")))
            rows = _dict_rows(r.get(key))
            if rows:
                owned.setdefault(rid, []).extend(rows)
        for row in _dict_rows(raw.get(key)):
            target = _slug(str(row.get("requirement"))) if row.get("requirement") else ""
            if target not in req_ids:
                notes.append(f"{key}_unlinked:{row.get('id')}")
                target = first_req
            owned.setdefault(target, []).append(row)
        return owned

    acceptance = _collect("acceptance")
    scenarios = _collect("scenarios")
    for rid, rows in sorted(acceptance.items()):
        by_req_extra.setdefault(rid, []).append(_acceptance_block(rows))
        folded["acceptance"] += len(rows)
        notes.append(f"folded_acceptance:{rid}={len(rows)}")
    for rid, rows in sorted(scenarios.items()):
        by_req_extra.setdefault(rid, []).append(_scenario_block(rows))
        folded["scenarios"] += len(rows)
        notes.append(f"folded_scenarios:{rid}={len(rows)}")
    for r in requirements:
        extra = by_req_extra.get(r.id) or []
        if extra:
            r.statement = _text(r.statement, *extra)
            folded["requirements_touched"] += 1

    # --- tasks, with the test plan folded into the verification
    tasks: list[Task] = []
    task_status: dict[str, str] = {}
    task_environment: list[str] = []
    for t in _dict_rows(raw.get("tasks")):
        if not t.get("id"):
            continue
        linked = [_slug(str(x)) for x in (t.get("requirements") or []) if _slug(str(x)) in req_ids]
        req = linked[0] if linked else first_req
        if not linked:
            notes.append(f"task_relinked:{t.get('id')}")
        tid = _slug(str(t.get("id")))
        tasks.append(Task(id=tid, requirement=req, verification=_text(t.get("title"), t.get("verification")) or "(no verification)"))
        if t.get("status"):
            task_status[tid] = str(t.get("status"))
        if t.get("environment"):
            task_environment.append(tid)
    if not tasks:
        tasks = [Task(id="task.migrated", requirement=first_req, verification="(migrated)")]
        notes.append("task_synthesised")
    covered = {t.requirement for t in tasks}
    for r in requirements:
        if r.id not in covered:
            tasks.append(Task(id=f"task.{_slug(r.id)}", requirement=r.id, verification=f"Prove {r.id}"))
            notes.append(f"task_synthesised_for:{r.id}")

    plan_by_task: dict[str, list[dict]] = {}
    for row in _dict_rows(raw.get("test_plan")):
        target = _slug(str(row.get("requirement"))) if row.get("requirement") else ""
        owner = next((t.id for t in tasks if t.requirement == target), None)
        if owner is None:
            notes.append(f"test_plan_unlinked:{row.get('id')}")
            owner = tasks[0].id
        plan_by_task.setdefault(owner, []).append(row)
    for tid, rows in sorted(plan_by_task.items()):
        task = next(t for t in tasks if t.id == tid)
        task.verification = _text(task.verification, _test_plan_block(rows))
        folded["test_plan"] += len(rows)
        folded["tasks_touched"] += 1
        notes.append(f"folded_test_plan:{tid}={len(rows)}")

    # --- evidence
    evidence: list[Evidence] = []
    formal_report: list[dict] = []
    dropped_evidence: list[str] = []
    dropped_refs: dict[str, list[str]] = {}
    for e in _dict_rows(raw.get("evidence")):
        kind = _KIND_MAP.get(str(e.get("kind")), "manual")
        if kind is None:
            notes.append(f"evidence_dropped:{e.get('id')}")
            dropped_evidence.append(str(e.get("id")))
            continue
        refs = [_slug(str(x)) for x in (e.get("refs") or []) if _slug(str(x)) in req_ids]
        req = refs[0] if refs else first_req
        if len(refs) > 1:
            dropped_refs[str(e.get("id"))] = refs[1:]
        status = "pass" if e.get("status") == "pass" else "fail"
        if e.get("status") == "waived":
            notes.append(f"waived_becomes_fail:{e.get('id')}")
        artifact, sha, problem = _resolve_artifact(root, e.get("artifact"))
        reason = ""
        if problem == "outside_repo":
            # Never hash a path that escapes the repository.
            status, reason = "fail", f"artifact_outside_repo {e.get('artifact')}"
            notes.append(f"artifact_outside_repo:{e.get('id')}")
        elif problem == "unresolved":
            notes.append(f"artifact_unresolved:{e.get('id')}")
        elif problem == "artifact_is_manifest":
            reason = f"artifact_is_manifest {e.get('artifact')}"
            notes.append(f"artifact_is_manifest:{e.get('id')}")
        if kind == "formal_check" and status == "pass" and artifact is None:
            # A formal claim with nothing to point at is a claim, not a proof.
            status = "fail"
            reason = reason or (f"{problem} for {e.get('command') or 'no command'}")
            notes.append(f"formal_carried:{e.get('id')}:fail")
            formal_report.append({"id": str(e.get("id")), "requirement": req, "property_id": None, "module": None, "checker_command": e.get("command"), "status": "fail", "reason": f"evidence row {e.get('id')}: {reason}"})
        elif kind == "formal_check":
            notes.append(f"formal_carried:{e.get('id')}:{status}")
            formal_report.append({"id": str(e.get("id")), "requirement": req, "property_id": None, "module": None, "checker_command": e.get("command"), "status": status, "reason": reason or None})
        note = _text(e.get("summary"), f"({reason})" if reason else "") or None
        evidence.append(Evidence(kind=kind, requirement=req, status=status, command=e.get("command"), artifact=artifact, sha256=sha, at=str((e.get("data") or {}).get("attached_at") or "") or None, note=note))
    if dropped_refs:
        notes.append("dropped_evidence_refs:" + ";".join(f"{k}={','.join(v)}" for k, v in sorted(dropped_refs.items())))

    # --- formal bindings: carried, never silently downgraded
    for f in _dict_rows(raw.get("formal")):
        fid = str(f.get("id") or f.get("property_id") or "formal")
        req = _slug(str(f.get("requirement") or ""))
        req = req if req in req_ids else first_req
        binding = f"property {f.get('property_id')} in {f.get('module')}" + (f" via {f.get('checker_command')}" if f.get("checker_command") else "")
        artifact = f.get("artifact") or f.get("report")
        recorded = str(f.get("status") or "")
        kept_artifact, sha, problem = _resolve_artifact(root, artifact)
        status, reason = "fail", ""
        if recorded != "pass":
            kept_artifact, sha = None, None
            reason = f"v4 row records status {recorded or 'none'}"
        elif problem == "no_artifact":
            reason = "v4 row names no artifact"
        elif problem == "outside_repo":
            reason = f"artifact_outside_repo {artifact}"
        elif problem == "artifact_is_manifest":
            reason = f"artifact_is_manifest {artifact}"
        elif problem == "unresolved":
            reason = f"artifact {artifact} does not resolve"
        else:
            status = "pass"
        note = f"formal binding {binding}" + (f" (unproven after migration: {reason})" if status == "fail" else " (carried; unbound until re-run)")
        evidence.append(Evidence(kind="formal_check", requirement=req, status=status, command=f.get("checker_command"), artifact=kept_artifact, sha256=sha, note=note))
        notes.append(f"formal_carried:{fid}:{status}")
        formal_report.append({"id": fid, "requirement": req, "property_id": f.get("property_id"), "module": f.get("module"), "checker_command": f.get("checker_command"), "status": status, "reason": reason or None})

    # --- disclosure of everything with no place in v5
    for key in ("dependencies", "waivers"):
        rows = _dict_rows(raw.get(key))
        if rows:
            ids = [str(r.get("id") or r.get("from") or "?") for r in rows]
            notes.append(f"dropped_{key}:{';'.join(ids)}")
            dropped[key] = ids
    if dropped_evidence:
        dropped["evidence"] = dropped_evidence
    if dropped_refs:
        dropped["evidence_refs"] = dropped_refs
    if task_status:
        notes.append("dropped_task_status:" + ";".join(f"{k}={v}" for k, v in sorted(task_status.items())))
        dropped["task_status"] = task_status
    if task_environment:
        notes.append("dropped_task_environment:" + ";".join(sorted(task_environment)))
        dropped["task_environment"] = sorted(task_environment)
    unknown_top = sorted(k for k, v in raw.items() if v and k not in _TOP_CONSUMED)
    if unknown_top:
        notes.append("dropped_fields:" + ";".join(unknown_top))
        dropped["fields"] = unknown_top
    unknown_change = sorted(k for k, v in change.items() if v and k not in _CHANGE_CONSUMED)
    if unknown_change:
        notes.append("dropped_change_fields:" + ";".join(unknown_change))
        dropped["change_fields"] = unknown_change
    for label, rows, consumed in (("requirement_fields", raw_reqs, _REQ_CONSUMED), ("task_fields", _dict_rows(raw.get("tasks")), _TASK_CONSUMED), ("evidence_fields", _dict_rows(raw.get("evidence")), _EVIDENCE_CONSUMED)):
        keys = sorted({k for row in rows for k, v in row.items() if v and k not in consumed})
        if keys:
            notes.append(f"dropped_{label}:" + ";".join(keys))
            dropped[label] = keys

    # --- ledger decisions
    usable = [d for d in decisions or [] if str(d.get("intent_digest", "")).startswith("sha256:")]
    approvals = [d for d in usable if str(d.get("verdict") or "approve") == "approve"]
    decision_rows = [
        Decision(digest=str(d["intent_digest"]), reviewer=str(d.get("principal") or "operator"), at=str(d.get("decided_at") or "") or None, note=f"ledger decision {d.get('id')} ({d.get('from_state')}->{d.get('to_state')})")
        for d in approvals
    ]
    if decision_rows:
        notes.append(f"ledger_decisions_imported:{len(decision_rows)}")
    if len(usable) != len(approvals):
        notes.append(f"ledger_decisions_skipped_verdict:{len(usable) - len(approvals)}")

    why = _text(change.get("rationale"), change.get("root_cause")) or "(migrated without rationale)"
    migration_notes = change.get("migration_notes")
    if migration_notes:
        rows = migration_notes if isinstance(migration_notes, list) else [migration_notes]
        why = _text(why, "Migration notes:\n" + "\n".join(f"- {str(r).strip()}" for r in rows if str(r).strip()))
        notes.append(f"folded_migration_notes:{len(rows)}")
        folded["migration_notes"] = len(rows)

    manifest = Manifest(
        id=str(change.get("id")),
        title=str(change.get("title") or change.get("id")),
        tier=max(0, min(3, tier)),
        why=why,
        affected_code=[str(p) for p in change.get("affected_code") or []],
        requirements=requirements,
        tasks=tasks,
        evidence=evidence,
        decisions=decision_rows,
    )
    return MigrationResult(manifest=manifest, notes=notes, folded=folded, dropped=dropped, formal=formal_report, decisions=len(decision_rows))


def migrate(raw: dict, *, root: Path | None = None, decisions: list[dict] | None = None) -> tuple[Manifest, list[str]]:
    """The manifest and its notes. Callers wanting the full account use
    :func:`migrate_change`."""
    result = migrate_change(raw, root=root, decisions=decisions)
    return result.manifest, result.notes


def load_decisions(path: Path | None) -> dict[str, list[dict]]:
    """Group a ledger export by ``change_id``.

    Two shapes are accepted: the shipped export envelope
    ``{"exported_at", "note", "source", "rows": [...]}`` and a bare list of
    rows. Anything else is refused. An import that quietly yields nothing is
    worse than one that stops, because the migration then reports a clean run
    that silently carried no decisions at all.
    """
    if path is None:
        return {}
    if not path.is_file():
        raise PactError("decisions_file_invalid", f"decisions export not found: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PactError("decisions_file_invalid", f"{path} is not valid JSON: {exc}") from exc
    if isinstance(data, dict):
        rows = data.get("rows")
        if not isinstance(rows, list):
            raise PactError("decisions_file_invalid", f"{path}: an export object needs a 'rows' list, got {type(rows).__name__}")
    elif isinstance(data, list):
        rows = data
    else:
        raise PactError("decisions_file_invalid", f"{path}: expected an export object or a list of rows, got {type(data).__name__}")
    by_change: dict[str, list[dict]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise PactError("decisions_file_invalid", f"{path}: row {index} is {type(row).__name__}, not an object")
        change_id = row.get("change_id")
        if not str(change_id or "").strip():
            raise PactError("decisions_file_invalid", f"{path}: row {index} has no change_id")
        by_change.setdefault(str(change_id), []).append(row)
    return by_change


assert set(_KIND_MAP.values()) - {None} <= set(EVIDENCE_KINDS)
