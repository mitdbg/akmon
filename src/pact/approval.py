"""Approval binding: what counts as a "yes", and what it is worth.

Two sources bind, both tied to a commit and to the intent digest:

* An **operator record**: a ``Pact-Approved: <change-id> <sha256:digest>``
  trailer on a commit in the reviewed range. The operator says "approve" in
  chat and the agent runs ``pact approve <id>``, which writes the trailer.
  This is a *record of the operator's decision*, not third-party authority:
  the author writes it, so it proves that an approval was asked for and
  recorded, not that an independent party read the code. CI proof covers the
  code; this covers the intent.
* A **pull-request review** in state ``APPROVED`` by a non-bot, non-author
  reviewer whose body carries the same line. This is stronger -- a second
  principal -- so it is preferred when both exist, and it keeps the
  ``self_approval`` rule.

Both bind only when the digest equals the manifest's current intent digest
AND the commit they sit on still describes the code at the head: an approval
covers a tree, not just a manifest, so a code push after it unbinds it (see
:func:`code_rejection`). Editing the manifest or attaching evidence after it
does not.

``decisions[]`` rows in the manifest remain *records* (migrated ledger rows)
and never bind: they carry no commit and no range.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from pathlib import Path

from pact import git
from pact.schema import Manifest

CODE_REJECTIONS = frozenset({
    "review_commit_missing",
    "review_commit_unreachable",
    "code_changed_after_review",
    "review_commit_is_merge",
    "approval_commit_contains_code",
})

#: Which authority a source carries. A review is a second principal; a
#: trailer is the operator's own recorded decision.
AUTHORITY = {"pr_review": "review", "pr_review_file": "review", "git_trailer": "operator_record"}

LINE_RE = re.compile(r"Pact-Approved:?\s+(?P<id>[a-z0-9][a-z0-9._-]*)\s+(?P<digest>sha256:[0-9a-f]{64})")
TRUSTED_ASSOCIATIONS = frozenset({"OWNER", "MEMBER", "COLLABORATOR"})


@dataclass(frozen=True)
class Approval:
    change_id: str
    digest: str
    reviewer: str
    source: str
    url: str | None = None
    at: str | None = None
    binding: bool = True
    rejected: str | None = None
    #: The commit this approval is tied to: ``review.commit.oid`` for a
    #: review, the trailer's own commit for an operator record. A candidate
    #: with no commit cannot be tied to a tree and never binds.
    commit: str | None = None

    @property
    def authority(self) -> str:
        """``review`` (a second principal) or ``operator_record`` (the
        operator's decision, recorded by the author)."""
        return AUTHORITY.get(self.source, "record")


def parse_lines(text: str, *, reviewer: str, source: str, url: str | None = None, at: str | None = None, binding: bool = True, rejected: str | None = None, commit: str | None = None) -> list[Approval]:
    return [
        Approval(m.group("id"), m.group("digest"), reviewer, source, url, at, binding, rejected, commit)
        for m in LINE_RE.finditer(text or "")
    ]


def _is_bot(login: str) -> bool:
    return login.endswith("[bot]") or login in {"github-actions", "dependabot"}


def _login(value) -> str:
    if isinstance(value, dict):
        return str(value.get("login") or "unknown")
    return str(value or "unknown")


def _commit_oid(review: dict) -> str | None:
    """``review["commit"]["oid"]`` -- the commit the reviewer approved.

    ``gh`` nests it; a bare sha is tolerated. Absent (an older payload, a
    review submitted with no head) reads as None, which never binds.
    """
    value = review.get("commit")
    if isinstance(value, dict):
        value = value.get("oid")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def from_reviews_payload(data, *, source: str = "pr_review") -> list[Approval]:
    """``gh pr view N --json reviews,url,author`` output, or a bare list.

    Only the LATEST review per reviewer counts (an approval superseded by a
    later CHANGES_REQUESTED or DISMISSED review is gone). It binds only when
    that latest review is APPROVED, the reviewer is not a bot, not the
    pull-request author, and (when the field is present) a trusted
    association. The reviewed commit is captured here and checked against the
    head separately by :func:`with_code_binding`, which needs the checkout.
    Every line found is returned; non-binding ones carry the ``rejected``
    reason for the report.
    """
    reviews = data.get("reviews", data) if isinstance(data, dict) else data
    url = data.get("url") if isinstance(data, dict) else None
    pr_author = _login(data.get("author")) if isinstance(data, dict) and data.get("author") else None
    latest: dict[str, dict] = {}
    for review in reviews or []:
        if not isinstance(review, dict):
            continue
        login = _login(review.get("author"))
        stamp = str(review.get("submittedAt") or "")
        if login not in latest or stamp >= str(latest[login].get("submittedAt") or ""):
            latest[login] = review
    found: list[Approval] = []
    for login, review in latest.items():
        state = str(review.get("state") or "").upper()
        association = str(review.get("authorAssociation") or "").upper()
        rejected: str | None = None
        if state != "APPROVED":
            rejected = f"review_state:{state or 'missing'}"
        elif _is_bot(login):
            rejected = "bot_reviewer"
        elif pr_author is not None and login == pr_author:
            rejected = "self_approval"
        elif association and association not in TRUSTED_ASSOCIATIONS:
            rejected = f"author_association:{association}"
        found.extend(
            parse_lines(review.get("body", ""), reviewer=login, source=source, url=url, at=review.get("submittedAt"), binding=rejected is None, rejected=rejected, commit=_commit_oid(review))
        )
    return found


def from_reviews_json(path: Path) -> list[Approval]:
    """An operator-supplied reviews file. Fine for a local linter run; the
    merge-time check must use :func:`from_pull_request`, which fetches the
    reviews itself."""
    return from_reviews_payload(json.loads(path.read_text(encoding="utf-8")), source="pr_review_file")


def from_pull_request(root: Path, number: int) -> tuple[list[Approval], str | None]:
    """Fetch reviews through ``gh`` (a trusted producer) and bind them.

    Returns the approvals and an error token when the fetch failed; a failed
    fetch binds nothing.
    """
    import subprocess

    try:
        out = subprocess.run(
            ["gh", "pr", "view", str(number), "--json", "reviews,url,author"],
            cwd=root, capture_output=True, text=True, check=False,
        )
    except FileNotFoundError:
        return [], "gh_unavailable"
    if out.returncode != 0 or not out.stdout.strip():
        return [], "pr_reviews_unavailable"
    try:
        data = json.loads(out.stdout)
    except json.JSONDecodeError:
        return [], "pr_reviews_unavailable"
    return from_reviews_payload(data, source="pr_review"), None


def from_git_trailers(root: Path, base: str | None, head_ref: str = "HEAD") -> list[Approval]:
    """``Pact-Approved`` trailers in the reviewed range, as binding candidates.

    Each carries the commit it sits on, so :func:`with_code_binding` holds it
    to the same tree rule as a review: approve, then push code, and the
    record is stale. ``Pact-Approved-By`` on the same commit names who
    approved (default: the committer's git identity); it is a label on the
    record, not an identity check.
    """
    by = dict(git.trailer_entries(root, "Pact-Approved-By", base, head_ref))
    found: list[Approval] = []
    for commit, value in git.trailer_entries(root, "Pact-Approved", base, head_ref):
        found.extend(
            parse_lines(
                "Pact-Approved " + value,
                reviewer=by.get(commit, "commit-trailer"),
                source="git_trailer",
                binding=True,
                commit=commit,
            )
        )
    return found


def from_manifest(manifest: Manifest) -> list[Approval]:
    """Author-controlled: recorded, never binding."""
    return [
        Approval(manifest.id, d.digest, d.reviewer, "manifest", d.url, d.at, False, "author_controlled")
        for d in manifest.decisions
    ]


def code_rejection(root: Path, approval: Approval, head_ref: str = "HEAD") -> str | None:
    """Why this approval does not bind the code at ``head_ref``, or None.

    The intent digest guards the manifest; this guards the tree. The approved
    commit (a review's ``commit.oid``, a trailer's own commit) must be a real,
    non-merge commit, an ancestor of the head, and nothing outside ``pact/``
    may have changed AFTER it -- so attaching evidence or editing the manifest
    afterwards is fine, but any push of code is not.

    A **merge commit never approves**: its own diff against either parent is
    somebody else's branch, so ``git merge --no-ff -m "...Pact-Approved..."``
    would approve every commit on the side branch. ``git.trailer_entries``
    already skips merges; this refuses one that arrives any other way.

    An **operator record is also held to its OWN diff**. A review's commit is
    the tree the reviewer read, so code inside it is genuinely approved; a
    trailer is written by the author, so code inside the SAME commit was
    approved by nobody. Without this, one commit that both adds
    ``src/evil.py`` and carries the trailer passes: the ``commit..head`` range
    is empty by construction. ``pact approve`` writes an empty commit, so a
    legitimate record has nothing in its own diff to begin with.

    Every unknown answer (no commit recorded, no git, an invented sha, a
    parentless commit) is a rejection: an approval that cannot be tied to a
    tree must not bind.
    """
    if not approval.commit:
        return "review_commit_missing"
    if git.rev_parse(root, approval.commit) is None:
        return "review_commit_unreachable"
    if git.is_ancestor(root, approval.commit, head_ref) is not True:
        return "review_commit_unreachable"
    parents = git.parents(root, approval.commit)
    if parents is None:
        return "review_commit_unreachable"
    if len(parents) > 1:
        return "review_commit_is_merge"
    changed = git.changed_paths(root, approval.commit, head_ref)
    if changed is None:
        return "review_commit_unreachable"
    if any(not path.startswith("pact/") for path, _ in changed):
        return "code_changed_after_review"
    if approval.authority == "operator_record":
        if not parents:
            # A root commit has no "before" to compare against, so what it
            # approves cannot be established.
            return "approval_commit_contains_code"
        own = git.changed_paths(root, parents[0], approval.commit)
        if own is None:
            return "review_commit_unreachable"
        if any(not path.startswith("pact/") for path, _ in own):
            return "approval_commit_contains_code"
    return None


def with_code_binding(root: Path, candidates: list[Approval], head_ref: str = "HEAD") -> list[Approval]:
    """Re-check every still-binding candidate against the reviewed commit.

    A rejected one is returned non-binding, carrying the reason, so it is
    still reported rather than silently dropped.
    """
    checked: list[Approval] = []
    for candidate in candidates:
        reason = code_rejection(root, candidate, head_ref) if candidate.binding else None
        checked.append(candidate if reason is None else replace(candidate, binding=False, rejected=reason))
    return checked


def binding(manifest: Manifest, digest: str, candidates: list[Approval]) -> Approval | None:
    """The strongest binding candidate: a review before an operator record."""
    matched = [a for a in candidates if a.binding and a.change_id == manifest.id and a.digest == digest]
    for wanted in ("review", "operator_record"):
        for approval in matched:
            if approval.authority == wanted:
                return approval
    return matched[0] if matched else None
