"""``pact check``: the one gate.

Validates the manifest, binds every evidence row to a file digest and a
commit reachable from the head, verifies the approval against the intent
digest *and* the approved commit, and computes the tier from the diff. There
is no ``deferred``: every input is a file or git, so the answer is
``recorded`` or ``refused``.

Lenient mode (default) refuses only on invalid manifests, broken receipts, and
a declared tier below the computed one.

Strict mode (``--strict``, what the merge requires) reads the **effective
tier** -- the higher of the declared and the computed one -- and applies what
that tier costs:

===== ================ ==================================================
Tier  Approval         Proof
===== ================ ==================================================
0     not required     not required (docs, CI, lockfiles, pact/ only)
1     required         warning only (tests, or a small source change)
2     required         required: every requirement needs bound passing
3     required         evidence
===== ================ ==================================================

``require_proof`` and ``require_approval`` override the tier rule in either
direction; CI uses that to require proof on any diff that touches code.

Receipts that already merged stay proven: a row present byte-identical in the
manifest at the diff base keeps its artifact and reachability checks but is
not held to the in-range rule, so a change proven across several pull
requests does not lose its proof at every rebase. Editing such a row makes it
new again, and an unreadable base manifest treats every row as new. Note the
deliberate interaction with the digest: REWORDING a requirement keeps its
merged receipts (the receipt row did not change), and what stops that from
laundering old proof into new intent is the approval, not the receipt -- the
reworded manifest has a new intent digest, so every existing approval goes
``approval_stale`` and a fresh one must be recorded against the new wording.

Nothing here fails open: an artifact must resolve inside the repository, a
NEW receipt's head must lie between the diff base and the current head, an
approval is either an APPROVED pull-request review by a non-bot (authority
``review``) or a ``Pact-Approved`` commit trailer written by ``pact approve``
(authority ``operator_record``) -- in both cases the approved commit must
still be an ancestor of the head with no code pushed after it -- and a
manifest under ``pact/archive/`` reads as merged only when it was already
there at the diff base (otherwise ``archived_in_pull_request``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from pact import approval as approvals
from pact import git, tier as tiers
from pact.credentials import scan_bytes
from pact.digest import file_sha256, intent_digest
from pact.schema import Manifest
from pact.store import PactError, generated_artifact, inside, is_archived, load, load_text, manifest_path


@dataclass
class CheckResult:
    change_id: str
    verdict: str = "recorded"
    blocked_by: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    intent_digest: str | None = None
    state: str = "open"
    tier: dict = field(default_factory=dict)
    evidence: dict = field(default_factory=dict)
    approval: dict | None = None
    records: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.verdict == "recorded"

    def refuse(self, token: str) -> None:
        if token not in self.blocked_by:
            self.blocked_by.append(token)
        self.verdict = "refused"

    def as_dict(self) -> dict:
        return {
            "verdict": self.verdict,
            "ok": self.ok,
            "change": self.change_id,
            "blocked_by": list(self.blocked_by),
            "warnings": list(self.warnings),
            "intent_digest": self.intent_digest,
            "state": self.state,
            "tier": self.tier,
            "evidence": self.evidence,
            "approval": self.approval,
            "approval_records": list(self.records),
        }


_inside = inside  # the one path guard, shared with the migration


def _row_identity(row) -> tuple:
    """What makes two receipt rows the same receipt: the requirement it
    proves, what ran, what it pinned, and where."""
    return (row.kind, row.requirement, row.status, row.artifact, row.sha256, row.head)


def baseline_receipts(root: Path, path: Path, base: str | None) -> frozenset[tuple]:
    """The receipts this manifest ALREADY carried at the diff base.

    A change proven across several pull requests (the four requirements of a
    cutover, say) would otherwise lose its proof at every rebase: the earlier
    receipts sit before the new base, read as ``evidence_head_before_base``,
    and the requirement goes unproven in a later pull request that did not
    touch it. A row that was already in the merged manifest, byte-identical,
    is proof someone already reviewed and merged.

    Every unknown answer -- no base, no git, an unreadable or invalid base
    manifest -- returns empty, so every row is treated as new and pays the
    full rule. Failing closed here costs a rebase; failing open would let an
    old receipt prove new work.
    """
    if base is None:
        return frozenset()
    relative = _relative(root, path)
    if relative is None:
        return frozenset()
    text = git.file_at(root, base, relative)
    if text is None:
        return frozenset()
    try:
        baseline = load_text(text, where=f"{base}:{relative}")
    except PactError:
        return frozenset()
    return frozenset(_row_identity(row) for row in baseline.evidence)


def bind_evidence(root: Path, manifest: Manifest, head: str | None, base: str | None, result: CheckResult, *, require_tracked: bool = False, preexisting: frozenset[tuple] = frozenset()) -> set[str]:
    """Return the requirement ids with at least one bound passing row.

    A NEW row binds when its artifact resolves inside the repository, exists,
    and matches the recorded sha256 -- all checked only for a row carrying a
    ``head``, since a headless row claims nothing -- and its ``head`` is
    reachable from the current head AND not already contained in the diff base
    (so a receipt from before the work cannot prove the work). With no base, no
    new row binds.

    A row already present, byte-identical, in the manifest at the diff base
    (``preexisting``, from :func:`baseline_receipts`) keeps the artifact and
    reachability checks -- a tampered artifact or rewritten history still
    breaks it -- but skips the in-range check, because it is proof that
    already merged. Editing any field of such a row, ``head`` included, makes
    it a new row again.
    """
    proven: set[str] = set()
    bound = stale = unbound = broken = 0
    carried = 0
    for index, row in enumerate(manifest.evidence):
        was_merged = _row_identity(row) in preexisting
        if row.artifact is not None:
            path = _inside(root, row.artifact)
            # A path that escapes the repository is refused whatever the row
            # claims: it is a manifest-data fault, and resolving it at all
            # would read a file this repository does not contain.
            if path is None:
                broken += 1
                result.refuse(f"evidence_artifact_outside_repo:{index}")
                continue
            # Everything else about the artifact is checked only for a row
            # that CLAIMS to bind. A row with no ``head`` proves nothing
            # whatever its artifact says, and the migrated corpus is full of
            # them: each records the file and the sha256 the v4 proof ran
            # against, as history. Judging today's tree against that history
            # turned an ordinary edit -- or a deletion -- of any cited file
            # into a permanent refusal of an unrelated change, while adding no
            # proof. Such a row is reported as `evidence_unbound` below.
            if row.head is not None:
                # A living file cannot be proof: the receipt pins its sha256,
                # so the next ordinary edit to it refuses an unrelated change
                # forever (see `store.generated_artifact`). A row that ALREADY
                # MERGED is only warned about, because refusing it would red
                # every open pull request for proof someone else recorded and
                # a reviewer already accepted; the corpus test is what clears
                # those, and this refusal is what stops new ones. Checked only
                # for a row that claims to bind, like everything below it.
                if not generated_artifact(root, manifest.id, row.artifact):
                    if was_merged:
                        result.warnings.append(f"evidence_artifact_not_generated:{index}")
                    else:
                        broken += 1
                        result.refuse(f"evidence_artifact_not_generated:{index}")
                        continue
                if not path.is_file():
                    broken += 1
                    result.refuse(f"evidence_artifact_missing:{index}")
                    continue
                data = path.read_bytes()
                if file_sha256(data) != row.sha256:
                    broken += 1
                    result.refuse(f"evidence_digest_mismatch:{index}")
                    continue
                # A committed artifact is public, so one carrying an unmasked
                # credential cannot be proof. The scope rule is the generated
                # rule's: a row this branch adds or edits refuses, a row that
                # already merged only warns, so a receipt someone else
                # recorded cannot red an unrelated pull request. The token
                # names the row and the first line, never the value.
                findings = scan_bytes(data, row.artifact)
                if findings:
                    token = f"evidence_artifact_contains_secret:{index}:{findings[0].line}"
                    if was_merged:
                        result.warnings.append(token)
                    else:
                        broken += 1
                        result.refuse(token)
                        continue
                if require_tracked and not git.is_tracked(root, row.artifact):
                    broken += 1
                    result.refuse(f"evidence_artifact_untracked:{index}")
                    continue
        carried += int(was_merged)
        if row.head is None:
            unbound += 1
            result.warnings.append(f"evidence_unbound:{index}")
            continue
        # Both ancestry checks must be True. ``None`` (unknown object, no git,
        # no base) fails closed: counting it as bound would let an invented or
        # ancient sha pass.
        reachable = git.is_ancestor(root, row.head, head) if head is not None else None
        # "In range" means the receipt is NOT already contained in the base.
        # Asking instead whether the base is an ancestor of the receipt would
        # refuse every receipt recorded on a branch that diverged before the
        # base moved (a push event's base is the previous target tip, which
        # the merged branch never descends from). A receipt that already
        # merged is in range by construction: it sits in the base on purpose.
        if was_merged:
            in_range = True
        else:
            contained_in_base = git.is_ancestor(root, row.head, base) if base is not None else None
            in_range = None if contained_in_base is None else not contained_in_base
        if reachable is not True:
            stale += 1
            result.warnings.append(f"evidence_head_unreachable:{index}")
            continue
        if in_range is not True:
            stale += 1
            result.warnings.append(f"evidence_head_before_base:{index}")
            continue
        bound += 1
        if row.status == "pass":
            proven.add(row.requirement)
    result.evidence = {"rows": len(manifest.evidence), "bound": bound, "unbound": unbound, "unreachable": stale, "broken": broken, "preexisting": carried, "new": len(manifest.evidence) - carried, "proven_requirements": sorted(proven)}
    return proven


def scan_changed_evidence(root: Path, manifest_file: Path, changed: list[tuple[str, int]] | None, result: CheckResult) -> None:
    """Refuse a file this branch adds or changes under the change's own
    ``evidence/`` directory when it holds an unmasked credential.

    The row scan in :func:`bind_evidence` sees only artifacts a receipt names;
    a transcript committed without ``evidence attach`` would slip past it and
    still be published. Only paths in the branch diff are read, so a file that
    already merged is never judged here. A deleted path is skipped. With no
    diff (``changed`` is None) nothing can be scoped, so the scan is skipped
    with the warning ``evidence_files_unscanned``; strict already refuses
    that case as ``diff_base_unavailable`` / ``tier_unverified``.
    """
    directory = _relative(root, manifest_file.parent / "evidence")
    if directory is None:
        return
    if changed is None:
        result.warnings.append("evidence_files_unscanned")
        return
    prefix = directory + "/"
    for relative in sorted({p for p, _ in changed if p.startswith(prefix)}):
        target = _inside(root, relative)
        if target is None or not target.is_file():
            continue
        findings = scan_bytes(target.read_bytes(), relative)
        if findings:
            result.refuse(f"evidence_artifact_contains_secret:{relative}:{findings[0].line}")


def _relative(root: Path, path: Path) -> str | None:
    """``path`` as a repo-relative posix path, or None when it is not inside
    the repository (which every caller must fail closed on)."""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except (OSError, ValueError):
        return None


#: Where a derived diff base comes from, in order. ``origin/dev`` and ``dev``
#: stay first, so a repository with a ``dev`` branch (okg itself) resolves
#: exactly as before; a repository without one falls back to its remote
#: default branch and then the usual ``main`` / ``master`` names.
DEFAULT_BASE_REFS = ("origin/dev", "dev", "origin/HEAD", "origin/main", "main", "origin/master", "master")


def default_base(root: Path, head_ref: str = "HEAD") -> str | None:
    """The merge base of ``head_ref`` with the first default ref that resolves."""
    for ref in DEFAULT_BASE_REFS:
        found = git.merge_base(root, ref, head_ref)
        if found is not None:
            return found
    return None


def _archived_at_base(root: Path, path: Path, base: str | None, head_ref: str) -> bool:
    """Whether this archived manifest already existed at the same path at the
    diff base, i.e. the pull request did not add or move it into the archive.

    Every unknown answer (no base, no git, unreadable path) is False, so the
    lenient "merged" treatment is never granted by default.
    """
    resolved = base or default_base(root, head_ref)
    if resolved is None:
        return False
    relative = _relative(root, path)
    if relative is None:
        return False
    return git.path_exists_at(root, resolved, relative) is True


def run(
    root: Path,
    change_id: str,
    *,
    strict: bool = False,
    require_proof: bool | None = None,
    require_approval: bool | None = None,
    base: str | None = None,
    head_ref: str = "HEAD",
    approvals_json: Path | None = None,
    pull_request: int | None = None,
) -> CheckResult:
    """``strict`` applies the tier rule (see the module docstring: tier 0 costs
    nothing, tier 1 costs an approval, tier 2 and 3 cost an approval and bound
    proof for every requirement). ``require_proof`` and ``require_approval``
    override it in either direction; CI requires proof on any diff that
    touches code."""
    strict = bool(strict or require_proof or require_approval)
    result = CheckResult(change_id=change_id)
    try:
        path = manifest_path(root, change_id)
        manifest = load(path)
    except PactError as exc:
        result.refuse(exc.token)
        result.warnings.append(exc.message)
        return result
    if manifest.id != change_id:
        result.refuse("change_id_mismatch")
    for token in manifest.structural_errors():
        result.refuse(token)
    digest = intent_digest(manifest)
    result.intent_digest = digest

    head = git.head(root)
    # An archived manifest is only trusted as "merged" when it was already
    # archived at the diff base: the directory is author-controlled, so a
    # manifest this branch adds to (or moves into) ``pact/archive/`` would
    # otherwise skip the base, the tier, the proof, and the approval. When the
    # base cannot be resolved the answer is unknown, which fails closed too.
    archived_at_base = _archived_at_base(root, path, base, head_ref) if is_archived(path) else False
    if is_archived(path) and not archived_at_base:
        result.refuse("archived_in_pull_request")
    supplied = base is not None
    if base is None:
        base = default_base(root, head_ref)
    if base is None:
        (result.refuse if strict else result.warnings.append)("diff_base_unavailable")
    elif base is not None and head is not None:
        # A supplied base is normalised to its merge base with the head (CI
        # passes the target branch tip, which may have moved on), then it must
        # be a proper ancestor of the head: ``--base HEAD`` would empty the
        # diff and put every receipt "in range", so it is refused.
        #
        # A DERIVED base equal to the head is a different answer: the checkout
        # is the dev tip itself (a push to dev, a checkout of it), so there is
        # no branch diff to judge. That is "unknown", the same as no base at
        # all -- lenient warns, strict refuses -- not a spoofed range.
        normalised = git.merge_base(root, base, head_ref) if git.rev_parse(root, base) else None
        if normalised is None or normalised == git.rev_parse(root, head_ref):
            if supplied:
                result.refuse("diff_base_invalid")
            else:
                (result.refuse if strict else result.warnings.append)("diff_base_unavailable")
            base = None
        else:
            base = normalised

    changed = git.changed_paths(root, base, head_ref) if base else None
    code_changed = None if changed is None else any(not p.startswith("pact/") for p, _ in changed)

    # The archived relaxation is narrow: it exists so a pull request that
    # merely CARRIES an already-merged manifest is not asked to re-prove and
    # re-approve it. It applies only when this pull request neither edits that
    # manifest nor changes any code -- otherwise "edit an archived manifest's
    # `why` plus 400 lines of src/" would skip the tier, the proof and the
    # approval entirely. Editing a merged record is itself refused: it is
    # history, and new work belongs in a new change.
    archived = archived_at_base
    if archived:
        relative = _relative(root, path)
        at_base = git.file_at(root, base, relative) if (relative and base) else None
        try:
            current = path.read_text(encoding="utf-8")
        except OSError:
            current = None
        # Unknown reads as edited: an unverifiable merged record must not buy
        # the relaxation.
        if at_base is None or current is None or at_base != current:
            result.refuse("archived_manifest_edited")
            archived = False
        elif code_changed is not False:
            archived = False

    # Tier from the diff.
    if changed is None:
        result.tier = {"declared": manifest.tier, "computed": None, "reason": "diff_unavailable"}
        (result.refuse if strict and not archived else result.warnings.append)("tier_unverified")
    else:
        verdict = tiers.compute(changed)
        result.tier = {"declared": manifest.tier, "computed": verdict.computed, "reason": verdict.reason, "detail": verdict.detail}
        if manifest.tier < verdict.computed:
            result.refuse(f"tier_below_computed:{verdict.computed}")

    # What this change costs. The effective tier is the higher of what the
    # author declared and what the diff computes, so neither under-declaring
    # nor a small-looking diff can buy a cheaper gate. An unverifiable diff
    # leaves only the declared tier, which strict already refused above.
    computed = result.tier.get("computed")
    effective = max(manifest.tier, computed if computed is not None else 0)
    result.tier["effective"] = effective
    require_proof = (strict and effective >= tiers.PROOF_FROM_TIER) if require_proof is None else require_proof
    require_approval = (strict and effective >= tiers.APPROVAL_FROM_TIER) if require_approval is None else require_approval
    result.tier["requires"] = {"proof": bool(require_proof), "approval": bool(require_approval)}

    # Evidence receipts.
    proven = bind_evidence(root, manifest, head, base, result, require_tracked=require_proof, preexisting=baseline_receipts(root, path, base))
    scan_changed_evidence(root, path, changed, result)
    unproven = [r.id for r in manifest.requirements if r.id not in proven]
    if unproven and not archived:
        for req in unproven:
            (result.refuse if require_proof else result.warnings.append)(f"unproven_requirement:{req}")

    # Approval: an APPROVED pull-request review (authority "review") or a
    # `Pact-Approved` commit trailer written by `pact approve` (authority
    # "operator_record"). A review is preferred when both are present.
    # `decisions[]` rows carry no commit and stay records.
    candidates: list[approvals.Approval] = []
    if pull_request is not None:
        fetched, error = approvals.from_pull_request(root, pull_request)
        candidates.extend(fetched)
        if error:
            (result.refuse if require_approval else result.warnings.append)(error)
    elif approvals_json is not None and approvals_json.is_file():
        candidates.extend(approvals.from_reviews_json(approvals_json))
    candidates.extend(approvals.from_git_trailers(root, base, head_ref))
    records = approvals.from_manifest(manifest)
    # Only this change's approvals can affect the verdict: binding, the stale
    # tokens and the reported records all filter on ``change_id`` below. With
    # no diff base the trailer read spans all history (hundreds of other
    # changes' approvals), and verifying each costs several git calls, so
    # drop the others BEFORE the per-approval code check.
    candidates = [a for a in candidates if a.change_id == manifest.id]
    # An approval binds the approved commit as well as the intent digest: a
    # code push after it unbinds it, so a "yes" cannot be inherited by code
    # nobody approved.
    #
    # Only this change's candidates are tree-checked: everything below reads
    # `a.change_id == manifest.id` rows alone, and each check costs about four
    # git calls, one a diff from the approved commit to the head. With no
    # diff base (the dev tip) the trailers come from the whole history -- 714
    # of them on 2026-09-25 -- and checking them all took about 300 s a call.
    candidates = approvals.with_code_binding(
        root, [a for a in candidates if a.change_id == manifest.id], head_ref
    )
    bound = approvals.binding(manifest, digest, candidates)
    stale_for_change = [a for a in candidates if a.binding and a.change_id == manifest.id and a.digest != digest]
    stale_code = [a for a in candidates if a.change_id == manifest.id and a.digest == digest and a.rejected in approvals.CODE_REJECTIONS]
    result.records = [
        {"source": a.source, "reviewer": a.reviewer, "digest": a.digest, "matches": a.digest == digest, "rejected": a.rejected, "commit": a.commit}
        for a in candidates + records
        if a.change_id == manifest.id
    ]
    if bound is not None:
        result.approval = {"reviewer": bound.reviewer, "source": bound.source, "authority": bound.authority, "commit": bound.commit, "url": bound.url, "digest": bound.digest}
        result.state = "merged" if archived else "approved"
    else:
        result.state = "merged" if archived else "open"
        if not archived:
            if stale_code:
                token = "approval_stale_code"
            elif stale_for_change:
                token = "approval_stale"
            else:
                token = "approval_missing"
            (result.refuse if require_approval else result.warnings.append)(token)
    return result
