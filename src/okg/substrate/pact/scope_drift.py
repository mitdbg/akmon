"""Diff-vs-scope drift and evidence staleness.

Per `right-size-pact-authoring` req.rightsize.consistency: nothing checked
that a manifest matches the work actually done. This module derives the
branch's changed paths from git, compares them against the PACT's declared
scope, and compares evidence timestamps against the code history they claim
to prove — the two signals behind the reconcile-PR/status-drift class.

Pure read-side: git subprocess reads only, no manifest mutation.
"""

from __future__ import annotations

import fnmatch
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pact import git as pact_git

from okg.substrate.pact import envelope
from okg.substrate.pact.evaluation_context import PactEvaluationContext

_GIT_TIMEOUT_SECONDS = 10


def _git(repo_root: Path, *args: str) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), *args],
            capture_output=True,
            text=True,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout


def _merge_base_ref(repo_root: Path, base_branch: str) -> str | None:
    for candidate in (f"origin/{base_branch}", base_branch):
        if _git(repo_root, "rev-parse", "--verify", "--quiet", candidate) is not None:
            base = _git(repo_root, "merge-base", candidate, "HEAD")
            if base:
                return base.strip()
    return None


def changed_paths(
    repo_root: Path,
    *,
    base_branch: str = "dev",
    context: PactEvaluationContext | None = None,
) -> dict[str, Any]:
    """Paths this branch changes: committed vs merge-base with the PR base,
    plus staged and unstaged working-tree changes."""
    if context is not None and (
        context.git_authoritative or not context.git_available
    ):
        branch_matches = (
            context.git_authoritative
            and context.git_base_branch == base_branch
        )
        return {
            "base_ref": context.git_base_ref if branch_matches else None,
            "base_branch": base_branch,
            "paths": (
                list(context.git_changed_paths)
                if branch_matches and context.git_changed_paths is not None
                else []
            ),
            "git_available": context.git_available,
            "head_sha": context.git_head_sha,
            "captured": True,
            "authority_missing": (
                not context.git_authoritative
                or not context.git_available
                or context.git_head_sha is None
                or context.git_changed_paths is None
                or not branch_matches
            ),
        }
    base = _merge_base_ref(repo_root, base_branch)
    untracked = _git(
        repo_root, "ls-files", "--others", "--exclude-standard", "-z"
    )
    git_available = untracked is not None or base is not None
    paths: set[str] = set(
        entry for entry in (untracked or "").split("\0") if entry
    )
    unreadable: list[str] = []
    if git_available:
        # The shared change-set reader and path rule for the committed range,
        # the index and the working tree: both sides of a rename count (the
        # old `--name-only` and porcelain readers kept only the new path).
        # Local diffs keep copy detection to modified sources, as the local
        # preflight does.
        reads: list[tuple[tuple[str, ...], bool]] = [
            (("--cached",), False),
            ((), False),
        ]
        if base is not None:
            reads.insert(0, ((f"{base}..HEAD",), True))
        for range_args, copies_harder in reads:
            try:
                records = pact_git.diff_records(
                    repo_root, *range_args, find_copies_harder=copies_harder
                )
            except pact_git.DiffUnavailable as exc:
                unreadable.append(str(exc))
                continue
            paths.update(pact_git.record_paths(records))
    result: dict[str, Any] = {
        "base_ref": base,
        "base_branch": base_branch,
        "paths": sorted(paths),
        "git_available": git_available,
    }
    if unreadable:
        # A diff git could not produce is an unknown change set, never an
        # empty one (for example a blob-less partial clone whose remote is
        # unreachable: copy detection needs blobs that name-only did not).
        result["diff_unreadable"] = unreadable
    return result


def _path_like(token: str) -> bool:
    return "/" in token and " " not in token and not token.startswith(("sha256:", "subagent:"))


def scope_patterns(manifest: Any) -> dict[str, Any]:
    """The declared scope: affected_code globs plus every repo path the
    manifest itself names (evidence artifacts/refs, planned tests), plus the
    change's own pact directories."""
    declared = [str(p) for p in (manifest.change.affected_code or [])]
    derived: set[str] = set()
    for entry in manifest.test_plan:
        test_file = str(entry.test_id).split("::", 1)[0]
        if _path_like(test_file):
            derived.add(test_file)
    for evidence in manifest.evidence:
        if evidence.artifact and _path_like(str(evidence.artifact)):
            derived.add(str(evidence.artifact).split("::", 1)[0])
    change_id = manifest.change.id
    always = {
        f"pact/changes/{change_id}/*",
        f"pact/archive/{change_id}/*",
    }
    return {
        "declared": declared,
        "derived": sorted(derived),
        "always": sorted(always),
        "scope_declared": bool(declared),
    }


def _matches(path: str, pattern: str) -> bool:
    if fnmatch.fnmatch(path, pattern):
        return True
    # a bare directory (or file) prefix covers everything under it
    return path == pattern or path.startswith(pattern.rstrip("/*") + "/")


def drift_report(
    repo_root: Path,
    manifest: Any,
    *,
    base_branch: str = "dev",
    context: PactEvaluationContext | None = None,
) -> dict[str, Any]:
    """Uncovered changed paths — the first-class pre-merge/brief warning."""
    changed = changed_paths(
        repo_root,
        base_branch=base_branch,
        context=context,
    )
    scope = scope_patterns(manifest)
    patterns = [*scope["declared"], *scope["derived"], *scope["always"]]
    uncovered = [
        path
        for path in changed["paths"]
        if not any(_matches(path, pattern) for pattern in patterns)
    ]
    # Both "not evaluated" cases are decided BEFORE the component is built:
    # patching `report["ok"] = False` afterwards is the hand-written `ok` the
    # envelope contract forbids, and it is how an `ok` and its verdict drifted
    # apart everywhere else.
    authority_missing = bool(changed.get("authority_missing"))
    diff_unreadable = list(changed.get("diff_unreadable") or ())
    base_unresolved = changed["base_ref"] is None and changed["git_available"]
    report: dict[str, Any] = envelope.component(
        ok=(
            not uncovered
            and not authority_missing
            and not diff_unreadable
            and not base_unresolved
        ),
        base_ref=changed["base_ref"],
        changed_count=len(changed["paths"]),
        scope_declared=scope["scope_declared"],
        uncovered=uncovered,
        git_available=changed["git_available"],
    )
    if changed.get("head_sha") is not None:
        report["head_sha"] = changed["head_sha"]
    if authority_missing:
        report["authority_missing"] = True
        report["warning"] = (
            "scope drift NOT evaluated: the captured Git authority does not "
            f"contain changed paths for base branch `{base_branch}`"
        )
        return report
    if diff_unreadable:
        report["diff_unreadable"] = diff_unreadable
        report["warning"] = (
            "scope drift NOT evaluated: git could not produce the change set ("
            + "; ".join(diff_unreadable)
            + ")"
        )
        return report
    if base_unresolved:
        # An unresolvable merge base (shallow CI clone, missing base branch)
        # must never read as drift-clean — that would be a silent fallback.
        report["base_unresolved"] = True
        report["warning"] = (
            f"scope drift NOT evaluated against committed history: merge "
            f"base with `{base_branch}` is unresolvable (shallow clone or "
            "missing base ref); only working-tree changes were checked"
        )
        return report
    if uncovered:
        head = ", ".join(uncovered[:5]) + (", ..." if len(uncovered) > 5 else "")
        report["warning"] = (
            f"scope drift: {len(uncovered)} changed path(s) not covered by "
            f"the declared PACT scope ({head})"
            + (
                "" if scope["scope_declared"]
                else " — change.affected_code declares no scope at all"
            )
        )
    return report


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    # Hand-authored pact.yaml can carry naive datetimes (PyYAML converts
    # unquoted timestamps); normalize to UTC instead of crashing on the
    # aware-vs-naive comparison downstream.
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    try:
        return datetime.strptime(text, "%Y%m%dT%H%M%S%fZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None


def evidence_timestamp(evidence: Any) -> datetime | None:
    data = evidence.data or {}
    for key in ("ingested_at", "attached_at"):
        stamp = _parse_timestamp(data.get(key))
        if stamp is not None:
            return stamp
    # evidence ids end with the mint stamp: ev.<kind>.<target>.<stamp>
    return _parse_timestamp(str(evidence.id).rsplit(".", 1)[-1])


def latest_code_change(
    repo_root: Path,
    paths: list[str],
    *,
    context: PactEvaluationContext | None = None,
) -> datetime | None:
    """Commit time of the most recent commit touching any covered path."""
    if context is not None:
        captured, timestamp = context.affected_code_timestamp(paths)
        if captured:
            return _parse_timestamp(timestamp)
        if not context.git_available:
            return None
    existing = [p for p in paths if not any(ch in p for ch in "*?[")]
    globs = [p for p in paths if any(ch in p for ch in "*?[")]
    args = ["log", "-1", "--format=%cI", "--"]
    args.extend(existing)
    args.extend(globs)  # git pathspecs accept globs directly
    if not existing and not globs:
        return None
    out = _git(repo_root, *args)
    if not out or not out.strip():
        return None
    return _parse_timestamp(out.strip().splitlines()[0])


def code_proof_freshness_boundary(
    repo_root: Path,
    manifest: Any,
    *,
    context: PactEvaluationContext | None = None,
) -> dict[str, Any]:
    """Resolve the last code-bearing head when a local receipt proves closure.

    Hosted checks are intentionally absent: this boundary answers only whether
    local PACT/report/receipt metadata may preserve already-recorded evidence.
    Exact-head CI authority remains the preflight contract.
    """
    receipts = getattr(getattr(manifest, "proof_receipts", None), "code", ())
    if not receipts:
        return envelope.component(ok=True, applies=False)
    from okg.substrate.pact.proof_identity import (
        ProofIdentityError,
        validate_local_code_proof_receipts,
    )

    try:
        if context is None:
            validation = validate_local_code_proof_receipts(
                repo_root,
                manifest,
                "HEAD",
            )
        else:
            if not context.git_authoritative or not context.git_head_sha:
                return envelope.component(
                    ok=False,
                    applies=True,
                    error={
                        "code": "code_proof_captured_git_authority_missing",
                        "message": (
                            "captured evaluation has no exact Git head for the "
                            "code-proof receipt"
                        ),
                    },
                )
            # A captured Git authority is an immutable snapshot. Receipt
            # validation needs tree entries, ancestry, tuple bytes, and commit
            # time, none of which this context currently captures. Reopening
            # the live repository here would mix authorities, so refuse until
            # a future context explicitly carries prevalidated proof data.
            return envelope.component(
                ok=False,
                applies=True,
                error={
                    "code": "code_proof_captured_validation_unavailable",
                    "message": (
                        "captured Git authority does not include validated "
                        "code-proof receipt data"
                    ),
                    "closure_sha": context.git_head_sha,
                },
            )
    except ProofIdentityError as exc:
        return envelope.component(
            ok=False,
            applies=True,
            error=exc.to_dict(),
        )

    changed_at: list[datetime] = []
    for proven_head in validation.proven_code_heads:
        out = _git(repo_root, "show", "-s", "--format=%cI", proven_head)
        stamp = _parse_timestamp(out.strip()) if out and out.strip() else None
        if stamp is None:
            return envelope.component(
                ok=False,
                applies=True,
                validation=validation.to_dict(),
                error={
                    "code": "code_proof_proven_timestamp_unavailable",
                    "message": "the proven code head has no readable commit timestamp",
                    "proven_code_sha": proven_head,
                },
            )
        changed_at.append(stamp)
    if not changed_at:
        return envelope.component(
            ok=False,
            applies=True,
            validation=validation.to_dict(),
            error={
                "code": "code_proof_proven_head_missing",
                "message": "the code-proof receipt names no proven code head",
            },
        )
    return envelope.component(
        ok=True,
        applies=True,
        freshness_authority="validated_code_proof",
        code_changed_at=max(changed_at).isoformat(),
        validation=validation.to_dict(),
    )


def staleness_report(
    repo_root: Path,
    manifest: Any,
    requirement_id: str,
    *,
    context: PactEvaluationContext | None = None,
    code_proof_boundary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare `requirement_id`'s satisfying evidence against the most
    recent commit touching the declared scope. Satisfaction is revoked when
    satisfying evidence exists but none of it postdates the code change.
    Empty declared scope → no staleness signal (nothing to compare against).
    """
    scope = scope_patterns(manifest)
    covered = [*scope["declared"]]
    if not covered:
        return {"checked": False, "reason": "no declared affected_code scope"}
    proof_boundary = code_proof_boundary or code_proof_freshness_boundary(
        repo_root,
        manifest,
        context=context,
    )
    if proof_boundary.get("applies") and not proof_boundary.get("ok"):
        stale = []
        for evidence in manifest.evidence:
            if requirement_id not in (evidence.refs or []):
                continue
            if evidence.status not in {"pass", "waived"}:
                continue
            stamp = evidence_timestamp(evidence)
            stale.append(
                {
                    "evidence_id": evidence.id,
                    "evidence_at": stamp.isoformat() if stamp else None,
                    "reason": "code_proof_receipt_invalid",
                }
            )
        return {
            "checked": True,
            "freshness_authority": "invalid_code_proof",
            "proof_identity": proof_boundary,
            "stale": stale,
            "fresh_count": 0,
            "revoked": True,
        }
    if proof_boundary.get("applies"):
        code_changed_at = _parse_timestamp(proof_boundary.get("code_changed_at"))
    else:
        code_changed_at = latest_code_change(
            repo_root,
            covered,
            context=context,
        )
    if code_changed_at is None:
        if context is not None and (
            context.git_authoritative or not context.git_available
        ):
            return {
                "checked": False,
                "reason": (
                    "captured Git authority has no affected-code timestamp "
                    "for declared scope"
                ),
                "authority_missing": True,
                "revoked": True,
                **(
                    {"head_sha": context.git_head_sha}
                    if context.git_head_sha is not None
                    else {}
                ),
            }
        return {"checked": False, "reason": "no git history for declared scope"}
    stale: list[dict[str, Any]] = []
    fresh = 0
    for evidence in manifest.evidence:
        if requirement_id not in (evidence.refs or []):
            continue
        if evidence.status not in {"pass", "waived"}:
            continue
        stamp = evidence_timestamp(evidence)
        if stamp is None:
            continue
        if stamp < code_changed_at:
            stale.append({
                "evidence_id": evidence.id,
                "evidence_at": stamp.isoformat(),
                "code_changed_at": code_changed_at.isoformat(),
            })
        else:
            fresh += 1
    report = {
        "checked": True,
        "code_changed_at": code_changed_at.isoformat(),
        "stale": stale,
        "fresh_count": fresh,
        "revoked": bool(stale) and fresh == 0,
    }
    if proof_boundary.get("applies"):
        report["freshness_authority"] = "validated_code_proof"
        report["proof_identity"] = proof_boundary
    if context is not None and context.git_authoritative:
        report["captured"] = True
        if context.git_head_sha is not None:
            report["head_sha"] = context.git_head_sha
    return report
