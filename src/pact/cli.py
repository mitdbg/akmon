"""``pact`` command line: new, view, evidence, approve, check, migrate, usage.

Every invocation returns one envelope: ``verdict`` (``recorded`` |
``refused``), ``ok`` (derived), ``blocked_by`` tokens, and a one-line
``text``. Exit 0 on recorded, 1 on refused, 2 on usage error.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pact import check as checker
from pact import git, usage
from pact.digest import file_sha256, intent_digest
from pact.migrate import load_decisions, migrate_change
from pact.schema import EVIDENCE_KINDS, Evidence, Manifest, Requirement, Task
from pact.credentials import MASK_HINT, describe, scan_bytes
from pact.store import GENERATED_ARTIFACT_HINT, PactError, append_evidence, dump, generated_artifact, inside, is_archived, load, manifest_path, read_raw, write

REMOVED_VERBS = {
    "gate": "check",
    "decide": "approve (records the operator's approval as a commit trailer)",
    "hooks": "check (run it in CI; there are no git hooks)",
    "sync": "nothing (there is no ledger to sync)",
    "migrate-version": "migrate",
}
REMOVED_EVIDENCE_VERBS = {"ingest": "evidence attach (bind a receipt to HEAD)", "status": "view", "waive": "nothing (a waiver is not proof)", "sync": "nothing (there is no ledger to sync)", "template": "nothing"}


def _emit(payload: dict, *, as_json: bool) -> int:
    verdict = payload.get("verdict", "recorded")
    payload["ok"] = verdict == "recorded"
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(payload.get("text") or f"{verdict}: {payload.get('change')}")
        for token in payload.get("blocked_by", []):
            print(f"  blocked_by: {token}")
        for warning in payload.get("warnings", []):
            print(f"  warning: {warning}")
    return 0 if payload["ok"] else 1


def _refused(change: str | None, token: str, message: str) -> dict:
    return {"verdict": "refused", "change": change, "blocked_by": [token], "warnings": [], "text": message}


def cmd_new(root: Path, args) -> dict:
    target = root / "pact" / "changes" / args.id / "pact.yaml"
    if target.exists():
        return _refused(args.id, "change_exists", f"{target} already exists")
    manifest = Manifest(
        id=args.id,
        title=args.title or args.id.replace("-", " "),
        tier=args.tier,
        why=args.why or "Why this change is needed and what causes the problem.",
        affected_code=[],
        requirements=[Requirement(id=f"req.{args.id}.main", statement="What must be true when this change lands.")],
        tasks=[Task(id=f"task.{args.id}.deliver", requirement=f"req.{args.id}.main", verification="The named check that proves the requirement.")],
    )
    write(target, manifest)
    return {"verdict": "recorded", "change": args.id, "blocked_by": [], "warnings": [], "path": str(target.relative_to(root)), "text": f"wrote {target.relative_to(root)}"}


def cmd_view(root: Path, args) -> dict:
    try:
        path = manifest_path(root, args.id)
        manifest = load(path)
    except PactError as exc:
        return _refused(args.id, exc.token, exc.message)
    if args.format == "yaml":
        return {"verdict": "recorded", "change": args.id, "blocked_by": [], "warnings": [], "text": dump(manifest)}
    result = checker.run(root, args.id, base=args.base)
    brief = {
        "verdict": "recorded",
        "change": manifest.id,
        "title": manifest.title,
        "tier": manifest.tier,
        "state": result.state,
        "intent_digest": result.intent_digest,
        "archived": is_archived(path),
        "requirements": [r.id for r in manifest.requirements],
        "tasks": [{"id": t.id, "requirement": t.requirement} for t in manifest.tasks],
        "evidence": result.evidence,
        "approval": result.approval,
        "check": {"verdict": result.verdict, "blocked_by": result.blocked_by, "warnings": result.warnings},
        "blocked_by": [],
        "warnings": [],
        "next": _next(manifest, result),
    }
    brief["text"] = f"{manifest.id} [{result.state}] tier {manifest.tier}: {brief['next']}"
    return brief


def _next(manifest: Manifest, result: checker.CheckResult) -> str:
    unproven = [w.split(":", 1)[1] for w in result.warnings + result.blocked_by if w.startswith("unproven_requirement:")]
    if result.blocked_by and not all(b.startswith("unproven_requirement") or b.startswith("approval_") for b in result.blocked_by):
        return "fix: " + ", ".join(result.blocked_by)
    if unproven:
        return "prove " + ", ".join(unproven) + " (pact evidence attach)"
    if result.approval is None:
        return f"ask the operator to approve, then run: pact approve {manifest.id}"
    if result.state == "approved":
        return "merge"
    return "done"


def cmd_evidence(root: Path, args) -> dict:
    try:
        path = manifest_path(root, args.id)
        manifest = load(path)
    except PactError as exc:
        return _refused(args.id, exc.token, exc.message)
    if args.requirement not in {r.id for r in manifest.requirements}:
        return _refused(args.id, "unknown_requirement", f"{args.requirement} is not a requirement of {args.id}")
    head = args.head or git.head(root)
    if head is None:
        return _refused(args.id, "head_unavailable", "cannot resolve HEAD; pass --head")
    artifact = sha = None
    if args.artifact:
        # Order matters, and it is the order of how wrong the path is. A path
        # that escapes the repository is refused first and by its own code --
        # attach never checked that at all, so `--artifact /etc/hosts` used to
        # be hashed and recorded. Then the generated-artifact rule, which also
        # catches a `.scratch/` path (invisible to CI) before the weaker
        # "does not exist" answer, and tells the author what to attach
        # instead. Only then does the file have to be there.
        target = inside(root, args.artifact)
        if target is None:
            return _refused(args.id, "evidence_artifact_outside_repo", f"{args.artifact} resolves outside the repository")
        if not generated_artifact(root, manifest.id, args.artifact):
            return _refused(args.id, f"evidence_artifact_not_generated:{args.artifact}", f"{args.artifact} is a living repository file, not a generated artifact. {GENERATED_ARTIFACT_HINT}")
        if not target.is_file():
            return _refused(args.id, "evidence_artifact_missing", f"{args.artifact} does not exist")
        data = target.read_bytes()
        # A committed artifact is public. Refuse one that carries an unmasked
        # credential, naming the line and the kind but never the value.
        findings = scan_bytes(data, args.artifact)
        if findings:
            first = findings[0]
            others = f" (and {len(findings) - 1} more line(s))" if len(findings) > 1 else ""
            return _refused(args.id, f"evidence_artifact_contains_secret:{args.artifact}:{first.line}", f"{args.artifact} {describe(first)}{others}; {MASK_HINT}")
        artifact = args.artifact
        sha = file_sha256(data)
    row = Evidence(kind=args.kind, requirement=args.requirement, status=args.status, command=args.command, artifact=artifact, sha256=sha, head=head, at=datetime.now(timezone.utc).isoformat(timespec="seconds"), note=args.note)
    # Append the row to the file as written; a whole-model dump would
    # re-serialize the reviewed fields (block scalars lose their form and
    # their final newline).
    try:
        append_evidence(path, row.model_dump(mode="json", exclude_none=True))
    except PactError as exc:
        return _refused(args.id, exc.token, exc.message)
    return {"verdict": "recorded", "change": args.id, "blocked_by": [], "warnings": [], "row": row.model_dump(mode="json", exclude_none=True), "text": f"attached {args.kind} for {args.requirement} at {head[:12]}"}


def cmd_approve(root: Path, args) -> dict:
    """Record the operator's approval as a commit trailer.

    The operator says "approve" in chat; this is the one step the agent runs.
    It writes an EMPTY commit carrying ``Pact-Approved: <id> <digest>``, so
    the approval is tied to a tree (a later code push unbinds it) without
    touching the index or the working tree. It is an operator record, not
    third-party authority: an APPROVED pull-request review still outranks it.

    The digest is recomputed here, so an invalid or unknown manifest refuses
    before anything is committed.

    ``--allow-empty`` commits whatever is STAGED, so a file left in the index
    would ride into the approval commit and be approved by its own trailer.
    A dirty index is therefore refused (``index_not_empty``), the commit is
    made with ``--only --`` so no path can be picked up, and the resulting
    tree is compared with its parent's afterwards -- a commit that turned out
    to carry anything is reported as ``approval_commit_not_empty`` rather
    than returned as an approval.
    """
    try:
        manifest = load(manifest_path(root, args.id))
    except PactError as exc:
        return _refused(args.id, exc.token, exc.message)
    errors = manifest.structural_errors()
    if errors:
        return _refused(args.id, "invalid_manifest", f"{args.id} is not a valid manifest: {'; '.join(errors)}")
    digest = intent_digest(manifest)
    trailer = f"Pact-Approved: {args.id} {digest}"
    by = args.by or (git._run(root, "config", "user.name") or "").strip() or "operator"
    payload = {
        "verdict": "recorded",
        "change": args.id,
        "blocked_by": [],
        "warnings": [],
        "intent_digest": digest,
        "trailer": trailer,
        "approved_by": by,
        "authority": "operator_record",
        "commit": None,
        "dry_run": bool(args.dry_run),
    }
    if args.dry_run:
        payload["text"] = f"{trailer} (dry run; no commit written)"
        return payload
    clean = git.index_is_clean(root)
    if clean is not True:
        return _refused(
            args.id,
            "index_not_empty" if clean is False else "approval_commit_failed",
            f"refusing to approve {args.id} with a dirty index: commit or reset your staged changes first"
            if clean is False
            else "cannot read the git index",
        )
    out = git._run(root, "commit", "--allow-empty", "-q", "-m", f"Approve PACT {args.id}", "-m", f"{trailer}\nPact-Approved-By: {by}", "--only", "--")
    if out is None:
        return _refused(args.id, "approval_commit_failed", f"git could not write the approval commit for {args.id}")
    commit = git.head(root)
    parents = git.parents(root, commit) if commit else None
    made = git.tree_of(root, commit) if commit else None
    before = git.tree_of(root, parents[0]) if parents else None
    if made is None or before is None or made != before:
        return _refused(args.id, "approval_commit_not_empty", f"the approval commit for {args.id} changed the tree; inspect {commit} and reset it")
    payload["commit"] = commit
    payload["text"] = f"approved {args.id} as an operator record in {(payload['commit'] or '')[:12]}: {trailer}"
    return payload


def cmd_check(root: Path, args) -> dict:
    result = checker.run(
        root, args.id, strict=args.strict, base=args.base, head_ref=args.head_ref,
        approvals_json=Path(args.approvals_json) if args.approvals_json else None,
        pull_request=args.pr,
    )
    payload = result.as_dict()
    payload["text"] = f"check {args.id}: {result.verdict}" + (f" ({', '.join(result.blocked_by)})" if result.blocked_by else "") + f"; state={result.state}"
    return payload


def cmd_digest(root: Path, args) -> dict:
    try:
        manifest = load(manifest_path(root, args.id))
    except PactError as exc:
        return _refused(args.id, exc.token, exc.message)
    digest = intent_digest(manifest)
    return {"verdict": "recorded", "change": args.id, "blocked_by": [], "warnings": [], "intent_digest": digest, "approval_line": f"Pact-Approved {args.id} {digest}", "text": f"Pact-Approved {args.id} {digest}"}


def _manifest_paths(root: Path, *, include_archive: bool) -> list[tuple[str, Path]]:
    """``(change id, path)`` for every manifest in scope, active first.

    Paths, not ids: ``manifest_path`` resolves ``changes/`` before ``archive/``,
    so an id present in both would migrate the active manifest twice and never
    read the archived one -- and ``--check`` would pass it vacuously.
    """
    scopes = ["changes", "archive"] if include_archive else ["changes"]
    return [(path.parent.name, path) for scope in scopes for path in sorted((root / "pact" / scope).glob("*/pact.yaml"))]


def cmd_migrate(root: Path, args) -> dict:
    """Dry run by default; ``--apply`` writes; ``--check`` only asserts v5.

    Nothing the v4 manifest carried is dropped in silence: per-change notes and
    the ``--report`` file together account for every folded and every removed
    field. The report is written before anything is applied, so a run that
    cannot record what it did does not rewrite the corpus either.
    """
    if args.check and (args.apply or args.ids):
        return _refused(None, "usage", "--check takes no ids and cannot be combined with --apply")
    if args.check:
        known = _manifest_paths(root, include_archive=args.include_archive)
        stale = [change_id for change_id, path in known if read_raw(path).get("pact_version") != 5]
        return {
            "verdict": "refused" if stale else "recorded",
            "change": None,
            "blocked_by": [f"not_v5:{c}" for c in stale],
            "warnings": [],
            "checked": len(known),
            "stale": stale,
            "text": f"{len(stale)} of {len(known)} manifest(s) are not v5" if stale else f"all {len(known)} manifest(s) are v5",
        }
    decisions = load_decisions(Path(args.decisions) if args.decisions else None)
    if args.all:
        targets = _manifest_paths(root, include_archive=args.include_archive)
    else:
        targets = []
        for change_id in args.ids:
            try:
                targets.append((change_id, manifest_path(root, change_id)))
            except PactError:
                targets.append((change_id, None))
    migrated: list[dict] = []
    already_v5: list[str] = []
    refused: list[str] = []
    report: dict[str, dict] = {}
    pending: list[tuple[Path, Manifest]] = []
    totals = {"scanned": len(targets), "migrated": 0, "already_v5": 0, "refused": 0, "acceptance": 0, "scenarios": 0, "test_plan": 0, "migration_notes": 0, "formal_pass": 0, "formal_fail": 0, "decisions": 0}
    for change_id, path in targets:
        if path is None:
            refused.append(f"{change_id}:unknown_change")
            continue
        try:
            raw = read_raw(path)
        except PactError as exc:
            refused.append(f"{change_id}:{exc.token}")
            continue
        if raw.get("pact_version") == 5:
            already_v5.append(change_id)
            totals["already_v5"] += 1
            continue
        try:
            result = migrate_change(raw, root=root, decisions=decisions.get(change_id))
        except Exception as exc:  # pydantic or shape errors are the finding, not a crash
            refused.append(f"{change_id}:migration_failed:{type(exc).__name__}")
            continue
        errors = result.manifest.structural_errors()
        if errors:
            refused.append(f"{change_id}:{';'.join(errors)}")
            continue
        pending.append((path, result.manifest))
        migrated.append({"change": change_id, "notes": result.notes, "written": bool(args.apply)})
        report[change_id] = result.as_report()
        totals["migrated"] += 1
        for key in ("acceptance", "scenarios", "test_plan", "migration_notes"):
            totals[key] += result.folded[key]
        totals["formal_pass"] += sum(1 for f in result.formal if f["status"] == "pass")
        totals["formal_fail"] += sum(1 for f in result.formal if f["status"] == "fail")
        totals["decisions"] += result.decisions
    totals["refused"] = len(refused)

    report_path = None
    if args.report:
        target = Path(args.report)
        if not target.is_absolute():
            target = root / target
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps({"scope": {"active": True, "archive": bool(args.include_archive)}, "applied": bool(args.apply), "totals": totals, "refused": refused, "changes": report}, indent=2, sort_keys=True), encoding="utf-8")
        except OSError as exc:
            # The account of the rewrite comes first: without it, --apply would
            # rewrite the corpus with no record of what it folded or dropped.
            return _refused(None, "report_unwritable", f"cannot write the migration report to {target}: {exc}; nothing was applied")
        report_path = str(target)
    if args.apply:
        for path, manifest in pending:
            write(path, manifest)

    unmatched = sorted(set(decisions) - {change_id for change_id, _ in targets})
    payload = {
        "verdict": "recorded" if not refused else "refused",
        "change": None,
        "blocked_by": refused,
        "warnings": [],
        "migrated": migrated,
        "already_v5": already_v5,
        "count": totals["migrated"],
        "dry_run": not args.apply,
        "totals": totals,
        "decision_rows_loaded": sum(len(v) for v in decisions.values()),
        "decision_changes_unmatched": unmatched,
        "decision_rows_unmatched": sum(len(decisions[c]) for c in unmatched),
        "text": f"migrated {totals['migrated']} manifest(s){'' if args.apply else ' (dry run; pass --apply to write)'}; "
        + f"{totals['already_v5']} already v5; {len(refused)} refused; "
        + f"folded {totals['acceptance']} acceptance, {totals['scenarios']} scenario, {totals['test_plan']} test-plan rows; "
        + f"formal {totals['formal_pass']} pass / {totals['formal_fail']} fail; {totals['decisions']} decisions",
    }
    if report_path:
        payload["report"] = report_path
    return payload


def cmd_usage(root: Path, args) -> dict:
    paths = [Path(p) for p in args.logs] if args.logs else [usage.log_path(root) or root / ".okg" / "pact-usage.jsonl"]
    report = usage.report(paths)
    report.update({"verdict": "recorded", "change": None, "blocked_by": [], "warnings": [], "text": f"{report['invocations']} invocations, refusal rate {report['refusal_rate']}"})
    return report


def build_parser(prog: str = "pact") -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--root", default=None, help="repository root (default: the git toplevel of cwd)")
    common.add_argument("--json", action="store_true", help="print the JSON envelope")
    parser = argparse.ArgumentParser(prog=prog, description="PACT v5: git-native change manifests", parents=[common])
    sub = parser.add_subparsers(dest="verb", required=True)

    p = sub.add_parser("new", parents=[common], help="write a starter manifest")
    p.add_argument("id")
    p.add_argument("--title")
    p.add_argument("--tier", type=int, default=1)
    p.add_argument("--why")
    p.set_defaults(func=cmd_new)

    p = sub.add_parser("view", parents=[common], help="brief or raw view of a change")
    p.add_argument("id")
    p.add_argument("--format", choices=("brief", "yaml"), default="brief")
    p.add_argument("--base")
    p.set_defaults(func=cmd_view)

    p = sub.add_parser("evidence", parents=[common], help="attach a receipt bound to HEAD")
    esub = p.add_subparsers(dest="evidence_verb", required=True)
    a = esub.add_parser("attach", parents=[common])
    a.add_argument("id")
    a.add_argument("--kind", choices=EVIDENCE_KINDS, required=True)
    a.add_argument("--requirement", required=True)
    a.add_argument("--status", choices=("pass", "fail"), default="pass")
    a.add_argument("--command")
    a.add_argument("--artifact", help="repo-relative file to pin by sha256")
    a.add_argument("--head", help="commit the proof ran on (default: HEAD)")
    a.add_argument("--note")
    a.set_defaults(func=cmd_evidence)
    for verb, replacement in REMOVED_EVIDENCE_VERBS.items():
        r = esub.add_parser(verb, parents=[common], help=argparse.SUPPRESS)
        r.add_argument("rest", nargs=argparse.REMAINDER)
        r.set_defaults(func=None, removed=replacement)

    p = sub.add_parser("check", parents=[common], help="the one gate: validate, bind receipts, verify approval, compute tier")
    p.add_argument("id")
    p.add_argument("--strict", action="store_true", help="refuse unproven requirements and a missing approval")
    p.add_argument("--base", help="diff base (default: merge-base with origin/dev or dev, else the default branch: origin/HEAD, main, master)")
    p.add_argument("--head-ref", default="HEAD")
    p.add_argument("--pr", type=int, help="pull request number; reviews are fetched through gh (the trusted path)")
    p.add_argument("--approvals-json", help="a reviews file for local runs (`gh pr view N --json reviews,url,author`); ignored when --pr is given")
    p.set_defaults(func=cmd_check)

    p = sub.add_parser("approve", parents=[common], help="record the operator's approval as a Pact-Approved commit trailer")
    p.add_argument("id")
    p.add_argument("--by", help="who approved (default: git user.name)")
    p.add_argument("--dry-run", action="store_true", help="print the trailer without committing")
    p.set_defaults(func=cmd_approve)

    p = sub.add_parser("digest", parents=[common], help="print the intent digest and the approval line")
    p.add_argument("id")
    p.set_defaults(func=cmd_digest)

    p = sub.add_parser("migrate", parents=[common], help="rewrite v4 manifests into the v5 shape")
    p.add_argument("ids", nargs="*")
    p.add_argument("--all", action="store_true")
    p.add_argument("--apply", action="store_true", help="write the rewritten manifests (default: dry run)")
    p.add_argument("--dry-run", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--decisions", help="ledger export to import as decisions: an object with a 'rows' list, or a bare list of rows")
    p.add_argument("--include-archive", action="store_true", help="also migrate pact/archive/*/pact.yaml")
    p.add_argument("--report", help="write a machine-readable account of what was folded and what was dropped")
    p.add_argument("--check", action="store_true", help="exit 1 when a manifest is not already v5 (no writing, no migration)")
    p.set_defaults(func=cmd_migrate)

    p = sub.add_parser("usage", parents=[common], help="recompute the usage measures from the usage log")
    p.add_argument("logs", nargs="*")
    p.set_defaults(func=cmd_usage)

    for verb, replacement in REMOVED_VERBS.items():
        r = sub.add_parser(verb, parents=[common], help=argparse.SUPPRESS)
        r.add_argument("rest", nargs=argparse.REMAINDER)
        r.set_defaults(func=None, removed=replacement)
    return parser


def resolve_root(value: str | None) -> Path:
    if value:
        return Path(value).resolve()
    top = git._run(Path.cwd(), "rev-parse", "--show-toplevel")
    return Path(top.strip()).resolve() if top else Path.cwd().resolve()


def main(argv: list[str] | None = None, *, prog: str = "pact") -> int:
    started = time.monotonic()
    parser = build_parser(prog)
    args = parser.parse_args(argv)
    root = resolve_root(args.root)
    change_id = getattr(args, "id", None)
    if getattr(args, "removed", None):
        if "--json" in getattr(args, "rest", []):
            args.json = True
        verb_name = args.verb + (f" {args.evidence_verb}" if getattr(args, "evidence_verb", None) else "")
        payload = _refused(None, f"verb_removed:{verb_name.replace(' ', '_')}", f"`{prog} {verb_name}` was removed in PACT v5; use {args.removed}")
    else:
        try:
            payload = args.func(root, args)
        except PactError as exc:
            payload = _refused(change_id, exc.token, exc.message)
    verb = args.verb + (f" {args.evidence_verb}" if getattr(args, "evidence_verb", None) else "")
    usage.record(root, verb=verb, change_id=change_id, verdict=payload.get("verdict", "recorded"), blocked_by=list(payload.get("blocked_by", [])), started=started)
    return _emit(payload, as_json=args.json or os.environ.get("PACT_JSON") == "1")


if __name__ == "__main__":
    sys.exit(main())
