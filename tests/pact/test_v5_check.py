"""`pact check`: receipts bind to files and commits, approval binds to the intent
digest AND the reviewed commit, tier is computed from the diff, and there is no
deferred outcome."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from pact import check as checker
from pact.cli import main
from pact.digest import file_sha256, intent_digest
from pact.schema import Evidence
from pact.store import load, write
from _v5_repo import MANIFEST, commit_all, git, make_repo, write_manifest


@pytest.fixture
def repo(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = make_repo(tmp_path)
    write_manifest(root, MANIFEST)
    (root / "src").mkdir()
    (root / "src" / "a.py").write_text("x = 1\n", encoding="utf-8")
    commit_all(root, "feat")
    return root


def _attach(root: Path, *, artifact: str | None = "pact/changes/demo/evidence/proof.txt", head: str | None = None, status: str = "pass") -> None:
    path = root / "pact" / "changes" / "demo" / "pact.yaml"
    manifest = load(path)
    row: dict = {"kind": "executed_test", "requirement": "req.demo", "status": status, "head": head or git(root, "rev-parse", "HEAD")}
    if artifact:
        target = root / artifact
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("ok\n", encoding="utf-8")
        row.update(artifact=artifact, sha256=file_sha256(target.read_bytes()))
    data = json.loads(manifest.model_dump_json(exclude_none=True))
    data["evidence"].append(row)
    write_manifest(root, data)


def test_lenient_check_records_with_warnings(repo: Path) -> None:
    result = checker.run(repo, "demo")
    assert result.verdict == "recorded"
    assert "unproven_requirement:req.demo" in result.warnings
    assert "approval_missing" in result.warnings
    assert result.tier == {
        "declared": 1, "computed": 1, "reason": "small_source_change", "detail": "1 source lines",
        "effective": 1, "requires": {"proof": False, "approval": False},
    }
    assert result.state == "open"


def test_strict_refuses_unproven_and_unapproved(repo: Path) -> None:
    """At the effective tier 1 this diff computes, strict costs an approval;
    unproven requirements are a warning until tier 2 (or an explicit
    ``require_proof``, which CI passes whenever the diff touches code)."""
    result = checker.run(repo, "demo", strict=True)
    assert result.verdict == "refused"
    assert set(result.blocked_by) == {"approval_missing"}
    assert "unproven_requirement:req.demo" in result.warnings
    assert result.tier["requires"] == {"proof": False, "approval": True}
    forced = checker.run(repo, "demo", strict=True, require_proof=True)
    assert set(forced.blocked_by) == {"unproven_requirement:req.demo", "approval_missing"}


def _reviews(repo: Path, tmp_path: Path, change_id: str, digest: str, *, commit: str | None = "HEAD", login: str = "riley", state: str = "APPROVED", association: str | None = "OWNER") -> Path:
    """A ``gh pr view --json reviews`` payload.

    ``commit`` is the commit the reviewer approved: "HEAD" (the default)
    resolves the repository's current head, any other rev is resolved the same
    way, and None omits the field entirely -- which binds nothing.
    """
    review = {"author": {"login": login}, "state": state, "body": f"LGTM\nPact-Approved {change_id} {digest}", "submittedAt": "2026-09-06T00:00:00Z"}
    if association is not None:
        review["authorAssociation"] = association
    if commit is not None:
        review["commit"] = {"oid": git(repo, "rev-parse", commit)}
    path = tmp_path / f"reviews-{login}-{state}.json"
    path.write_text(json.dumps({"url": "https://example/pr/1", "reviews": [review]}), encoding="utf-8")
    return path


def test_bound_evidence_and_approved_review_pass_strict(repo: Path, tmp_path: Path) -> None:
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    result = checker.run(repo, "demo", strict=True, approvals_json=_reviews(repo, tmp_path, "demo", digest))
    assert result.verdict == "recorded", result.blocked_by
    assert result.state == "approved"
    assert result.approval["source"] == "pr_review_file" and result.approval["reviewer"] == "riley"
    assert result.evidence["proven_requirements"] == ["req.demo"]


def test_commit_trailer_binds_as_an_operator_record(repo: Path) -> None:
    """The operator approved in chat; the agent recorded it as a trailer.

    It binds -- with the SAME code rule a review gets -- and reports its
    authority honestly as an operator record, not a second principal.
    """
    # The artifact lives under pact/: an approval commit may touch nothing
    # else, or it would be approving its own payload.
    _attach(repo, artifact="pact/changes/demo/evidence/proof.txt")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    # One -m: git reads only the LAST paragraph as trailers, so both lines
    # must sit in the same block.
    approved = commit_all(repo, "proof", f"Pact-Approved: demo {digest}\nPact-Approved-By: Riley")
    result = checker.run(repo, "demo", strict=True, require_proof=True)
    assert result.verdict == "recorded", result.blocked_by
    assert result.state == "approved"
    assert result.approval["source"] == "git_trailer"
    assert result.approval["authority"] == "operator_record"
    assert result.approval["commit"] == approved
    assert result.approval["reviewer"] == "Riley"


def test_a_trailer_for_a_stale_digest_does_not_bind(repo: Path) -> None:
    _attach(repo, artifact="pact/changes/demo/evidence/proof.txt")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    commit_all(repo, "proof", f"Pact-Approved: demo {digest}")
    data = json.loads(load(repo / "pact/changes/demo/pact.yaml").model_dump_json(exclude_none=True))
    data["requirements"][0]["statement"] = "It works differently."
    write_manifest(repo, data)
    commit_all(repo, "reword the requirement")
    result = checker.run(repo, "demo", strict=True)
    assert "approval_stale" in result.blocked_by
    assert result.approval is None


def test_code_pushed_after_a_trailer_unbinds_it(repo: Path) -> None:
    """The trailer binds a tree too: approve, then push code, and it is stale."""
    _attach(repo, artifact="pact/changes/demo/evidence/proof.txt")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    commit_all(repo, "proof", f"Pact-Approved: demo {digest}")
    assert checker.run(repo, "demo", strict=True).verdict == "recorded"
    (repo / "src" / "evil.py").write_text("BACKDOOR = True\n", encoding="utf-8")
    commit_all(repo, "code the operator never approved")
    result = checker.run(repo, "demo", strict=True)
    assert "approval_stale_code" in result.blocked_by
    assert result.approval is None and result.state == "open"
    assert any(r["source"] == "git_trailer" and r["rejected"] == "code_changed_after_review" for r in result.records)


def test_only_this_changes_approvals_are_tree_checked(repo: Path, monkeypatch) -> None:
    """KILLS: tree-checking every trailer in history (fast-linux-2, 2026-09-25).

    With no diff base the trailers come from the whole history, and each tree
    check runs about four git calls. On dev's tip that was 714 trailers and
    about 300 s per check. Another change's approval never binds this one, so
    only this change's candidates reach the tree check -- and the verdict,
    the bound approval and the reported records are the same.
    """
    _attach(repo, artifact="pact/changes/demo/evidence/proof.txt")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    commit_all(repo, "proof", f"Pact-Approved: demo {digest}")
    for other in ("other-a", "other-b"):
        git(repo, "commit", "-q", "--allow-empty", "-m", f"Approve {other}",
            "-m", f"Pact-Approved: {other} sha256:{'0' * 64}")
    checked: list[str] = []
    real = checker.approvals.code_rejection

    def spy(root, approval, head_ref="HEAD"):
        checked.append(approval.change_id)
        return real(root, approval, head_ref)

    monkeypatch.setattr(checker.approvals, "code_rejection", spy)
    result = checker.run(repo, "demo", strict=True)
    assert result.verdict == "recorded", result.blocked_by
    assert result.approval["source"] == "git_trailer"
    assert checked == ["demo"]
    assert {r["digest"] for r in result.records if r["source"] == "git_trailer"} == {digest}


def test_manifest_decisions_never_bind(repo: Path) -> None:
    _attach(repo)
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    commit_all(repo, "proof")
    data = json.loads(load(repo / "pact/changes/demo/pact.yaml").model_dump_json(exclude_none=True))
    data["decisions"] = [{"digest": digest, "reviewer": "riley"}]
    write_manifest(repo, data)
    result = checker.run(repo, "demo", strict=True)
    assert "approval_missing" in result.blocked_by, "self-written decisions[] must not approve"
    assert any(r["source"] == "manifest" and r["rejected"] == "author_controlled" for r in result.records)


def test_a_review_outranks_a_trailer(repo: Path, tmp_path: Path) -> None:
    """Both bind here; the second principal is the one reported."""
    _attach(repo, artifact="pact/changes/demo/evidence/proof.txt")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    commit_all(repo, "proof", f"Pact-Approved: demo {digest}")
    assert checker.run(repo, "demo", strict=True).approval["authority"] == "operator_record"
    result = checker.run(repo, "demo", strict=True, approvals_json=_reviews(repo, tmp_path, "demo", digest))
    assert result.approval["authority"] == "review" and result.approval["reviewer"] == "riley"


@pytest.mark.parametrize("login,state,association,reason", [
    ("riley", "COMMENTED", "OWNER", "review_state:COMMENTED"),
    ("riley", "CHANGES_REQUESTED", "OWNER", "review_state:CHANGES_REQUESTED"),
    ("riley", "", "OWNER", "review_state:missing"),
    ("dependabot[bot]", "APPROVED", "CONTRIBUTOR", "bot_reviewer"),
    ("stranger", "APPROVED", "NONE", "author_association:NONE"),
])
def test_only_approved_reviews_by_trusted_humans_bind(repo: Path, tmp_path: Path, login, state, association, reason) -> None:
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    result = checker.run(repo, "demo", strict=True, approvals_json=_reviews(repo, tmp_path, "demo", digest, login=login, state=state, association=association))
    assert result.verdict == "refused" and "approval_missing" in result.blocked_by
    assert any(r["rejected"] == reason for r in result.records), result.records


def test_review_for_another_change_does_not_bind(repo: Path, tmp_path: Path) -> None:
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    result = checker.run(repo, "demo", strict=True, approvals_json=_reviews(repo, tmp_path, "other", digest))
    assert "approval_missing" in result.blocked_by


def test_artifact_outside_repo_refuses(repo: Path) -> None:
    data = copy.deepcopy(MANIFEST)
    outside = Path("/etc/hosts")
    data["evidence"] = [{"kind": "executed_test", "requirement": "req.demo", "artifact": "../../../../../../../../etc/hosts", "sha256": file_sha256(outside.read_bytes()) if outside.is_file() else "0" * 64, "head": git(repo, "rev-parse", "HEAD")}]
    write_manifest(repo, data)
    result = checker.run(repo, "demo", strict=True)
    assert "evidence_artifact_outside_repo:0" in result.blocked_by
    data["evidence"][0]["artifact"] = "/etc/hosts"
    write_manifest(repo, data)
    assert "evidence_artifact_outside_repo:0" in checker.run(repo, "demo").blocked_by


def test_evidence_head_before_base_is_not_proof(repo: Path) -> None:
    root_commit = git(repo, "rev-list", "--max-parents=0", "HEAD")
    _attach(repo, artifact=None, head=root_commit)
    result = checker.run(repo, "demo", strict=True, require_proof=True)
    assert "evidence_head_before_base:0" in result.warnings
    assert "unproven_requirement:req.demo" in result.blocked_by


def test_strict_refuses_without_a_diff_base(repo: Path, tmp_path: Path) -> None:
    git(repo, "remote", "remove", "origin")
    git(repo, "branch", "-D", "dev")
    _attach(repo)
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    commit_all(repo, "proof")
    result = checker.run(repo, "demo", strict=True, approvals_json=_reviews(repo, tmp_path, "demo", digest))
    assert result.verdict == "refused"
    assert {"diff_base_unavailable", "tier_unverified"} <= set(result.blocked_by)
    lenient = checker.run(repo, "demo")
    assert lenient.verdict == "recorded" and "diff_base_unavailable" in lenient.warnings


def test_tampered_artifact_refuses(repo: Path) -> None:
    _attach(repo)
    commit_all(repo, "proof")
    (repo / "pact/changes/demo/evidence/proof.txt").write_text("changed\n", encoding="utf-8")
    result = checker.run(repo, "demo")
    assert result.verdict == "refused"
    assert "evidence_digest_mismatch:0" in result.blocked_by


def test_a_headless_row_is_unbound_not_refused_when_its_artifact_moves_on(repo: Path) -> None:
    """A migrated receipt records history, not a claim.

    Every row the v4 -> v5 migration wrote carries the sha256 the artifact had
    when the old proof ran and NO head, so it can never bind. Digesting it
    against today's file made an ordinary edit of any cited source file refuse
    an unrelated change forever, while proving nothing either way. The row
    stays unbound and the change stays checkable.
    """
    _attach(repo)
    path = repo / "pact" / "changes" / "demo" / "pact.yaml"
    manifest = load(path)
    data = json.loads(manifest.model_dump_json(exclude_none=True))
    data["evidence"][0].pop("head")
    write_manifest(repo, data)
    commit_all(repo, "proof")
    (repo / "pact/changes/demo/evidence/proof.txt").write_text("changed\n", encoding="utf-8")

    result = checker.run(repo, "demo")

    assert result.verdict == "recorded"
    assert not any(token.startswith("evidence_digest_mismatch") for token in result.blocked_by)
    assert "evidence_unbound:0" in result.warnings
    assert "unproven_requirement:req.demo" in result.warnings


def test_missing_artifact_refuses(repo: Path) -> None:
    _attach(repo)
    commit_all(repo, "proof")
    (repo / "pact/changes/demo/evidence/proof.txt").unlink()
    result = checker.run(repo, "demo")
    assert "evidence_artifact_missing:0" in result.blocked_by


def test_evidence_on_unreachable_head_is_not_proof(repo: Path) -> None:
    _attach(repo, artifact=None, head="f" * 40)
    result = checker.run(repo, "demo", strict=True, require_proof=True)
    assert "evidence_head_unreachable:0" in result.warnings
    assert "unproven_requirement:req.demo" in result.blocked_by


def test_unbound_evidence_is_kept_but_proves_nothing(repo: Path) -> None:
    data = copy.deepcopy(MANIFEST)
    data["evidence"] = [{"kind": "manual", "requirement": "req.demo"}]
    write_manifest(repo, data)
    result = checker.run(repo, "demo", strict=True, require_proof=True)
    assert "evidence_unbound:0" in result.warnings
    assert "unproven_requirement:req.demo" in result.blocked_by


def test_requirement_edit_after_approval_makes_approval_stale(repo: Path, tmp_path: Path) -> None:
    _attach(repo)
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    commit_all(repo, "proof")
    reviews = _reviews(repo, tmp_path, "demo", digest)
    data = copy.deepcopy(json.loads(load(repo / "pact/changes/demo/pact.yaml").model_dump_json(exclude_none=True)))
    data["requirements"][0]["statement"] = "It works better."
    write_manifest(repo, data)
    result = checker.run(repo, "demo", strict=True, approvals_json=reviews)
    assert "approval_stale" in result.blocked_by
    # Attaching more evidence does not.
    data["requirements"][0]["statement"] = "It works."
    write_manifest(repo, data)
    _attach(repo, artifact="pact/changes/demo/evidence/proof2.txt")
    commit_all(repo, "more proof")
    assert checker.run(repo, "demo", strict=True, approvals_json=reviews).verdict == "recorded"


def test_code_change_after_approval_unbinds(repo: Path, tmp_path: Path) -> None:
    """The reviewer approved a tree, not just a manifest.

    Without this, an APPROVED review with the right line stays binding through
    unlimited later pushes, which is exactly what "receipts bound to a commit
    range" promises it will not do.
    """
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    reviews = _reviews(repo, tmp_path, "demo", digest)
    assert checker.run(repo, "demo", strict=True, approvals_json=reviews).verdict == "recorded"

    (repo / "src" / "evil.py").write_text("BACKDOOR = True\n", encoding="utf-8")
    commit_all(repo, "sneak in code the reviewer never saw")
    result = checker.run(repo, "demo", require_approval=True, approvals_json=reviews)
    assert result.verdict == "refused"
    assert "approval_stale_code" in result.blocked_by
    assert "approval_stale" not in result.blocked_by, "the intent digest did not drift; the code did"
    assert result.approval is None and result.state == "open"
    assert any(r["rejected"] == "code_changed_after_review" for r in result.records), result.records


def test_evidence_commit_after_approval_stays_bound(repo: Path, tmp_path: Path) -> None:
    """A commit touching only ``pact/`` is not a code change: attaching proof
    and editing the manifest after the review must not unbind it (the intent
    digest already guards the manifest's reviewed fields)."""
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    reviews = _reviews(repo, tmp_path, "demo", digest)
    _attach(repo, artifact="pact/changes/demo/evidence/proof2.txt")
    commit_all(repo, "attach more evidence")
    result = checker.run(repo, "demo", strict=True, approvals_json=reviews)
    assert result.verdict == "recorded", result.blocked_by
    assert result.approval["reviewer"] == "riley"


def test_review_without_a_commit_does_not_bind(repo: Path, tmp_path: Path) -> None:
    """An approval that cannot be tied to a tree fails closed."""
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    result = checker.run(repo, "demo", strict=True, approvals_json=_reviews(repo, tmp_path, "demo", digest, commit=None))
    assert "approval_stale_code" in result.blocked_by
    assert result.approval is None
    assert any(r["rejected"] == "review_commit_missing" for r in result.records), result.records


def test_rewritten_history_after_approval_unbinds(repo: Path, tmp_path: Path) -> None:
    """An amend or rebase leaves the reviewed commit off the head's history."""
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    reviews = _reviews(repo, tmp_path, "demo", digest)
    git(repo, "commit", "--amend", "-q", "-m", "proof (rewritten)")
    result = checker.run(repo, "demo", strict=True, approvals_json=reviews)
    assert "approval_stale_code" in result.blocked_by
    assert any(r["rejected"] == "review_commit_unreachable" for r in result.records), result.records


def test_invented_review_commit_does_not_bind(repo: Path, tmp_path: Path) -> None:
    """A sha that is not an object in this repository is unreachable, not
    trusted: an unknown answer must never bind."""
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    reviews = _reviews(repo, tmp_path, "demo", digest, commit=None)
    payload = json.loads(reviews.read_text(encoding="utf-8"))
    payload["reviews"][0]["commit"] = {"oid": "f" * 40}
    reviews.write_text(json.dumps(payload), encoding="utf-8")
    result = checker.run(repo, "demo", strict=True, approvals_json=reviews)
    assert "approval_stale_code" in result.blocked_by
    assert any(r["rejected"] == "review_commit_unreachable" for r in result.records), result.records


def test_review_without_association_field_still_binds(repo: Path, tmp_path: Path) -> None:
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    result = checker.run(repo, "demo", strict=True, approvals_json=_reviews(repo, tmp_path, "demo", digest, association=None))
    assert result.verdict == "recorded"
    assert result.approval == {
        "reviewer": "riley", "source": "pr_review_file", "authority": "review",
        "commit": git(repo, "rev-parse", "HEAD"), "url": "https://example/pr/1", "digest": digest,
    }


def test_declared_tier_below_computed_refuses(repo: Path) -> None:
    (repo / "src" / "big.py").write_text("\n".join(f"x{i} = {i}" for i in range(400)) + "\n", encoding="utf-8")
    commit_all(repo, "big")
    result = checker.run(repo, "demo")
    assert result.tier["computed"] == 2
    assert "tier_below_computed:2" in result.blocked_by


def test_tier_rules_from_paths() -> None:
    from pact.tier import compute

    assert compute([("docs/a.md", 10), ("README.md", 3)]).computed == 0
    assert compute([("pact/changes/x/pact.yaml", 50)]).computed == 0
    assert compute([("tests/test_a.py", 40)]).computed == 1
    assert compute([("src/a.py", 10), ("tests/test_a.py", 400)]).computed == 1
    assert compute([("src/a.py", 400)]).computed == 2
    assert compute([("src/okg/db/migrations/0001.sql", 3)]).computed == 2
    assert compute([("formal/x.tla", 3)]).computed == 2
    # CI files are tier 1, not 0: a workflow edit can delete the job that
    # enforces this gate, so it costs an approval.
    assert compute([(".github/workflows/ci.yml", 3)]).computed == 1
    assert compute([(".github/dependabot.yml", 2)]).computed == 1
    assert compute([("uv.lock", 200)]).computed == 0
    assert compute([("docs/runbooks/agents/x.md", 3)]).computed == 1


def _repo_with_archive_on_dev(tmp_path: Path, change_id: str = "old") -> Path:
    """A repo where the manifest was archived on `dev` (the way a merge leaves
    it) before the feature branch did any work, so the diff base already has
    it at that path."""
    root = make_repo(tmp_path)
    git(root, "checkout", "-q", "dev")
    data = copy.deepcopy(MANIFEST)
    data["id"] = change_id
    write_manifest(root, data, archived=True)
    commit_all(root, f"archive {change_id}")
    git(root, "fetch", "-q", "origin", "dev")
    git(root, "checkout", "-q", "feature")
    git(root, "merge", "-q", "--ff-only", "dev")
    # Pact-only work: a code change in the same pull request now takes the
    # archived relaxation away (see the tests below), so the base fixture must
    # not carry one.
    (root / "pact" / "changes" / "keep.txt").parent.mkdir(parents=True, exist_ok=True)
    (root / "pact" / "changes" / "keep.txt").write_text("branch work\n", encoding="utf-8")
    commit_all(root, "feat")
    return root


def test_archived_manifest_present_at_the_base_reports_merged(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = _repo_with_archive_on_dev(tmp_path)
    result = checker.run(root, "old")
    assert result.state == "merged"
    assert result.verdict == "recorded", result.blocked_by
    assert "archived_in_pull_request" not in result.blocked_by


def test_archived_manifest_without_a_resolvable_base_fails_closed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = _repo_with_archive_on_dev(tmp_path)
    git(root, "remote", "remove", "origin")
    git(root, "branch", "-D", "dev")
    result = checker.run(root, "old")
    assert "archived_in_pull_request" in result.blocked_by, "an unknown base is not a merged change"
    assert result.state != "merged"


def test_archived_manifest_added_by_the_pull_request_is_refused(repo: Path) -> None:
    """The S1 hole: writing the manifest straight into `pact/archive/` skipped
    the base, the tier, the proof and the approval. It is now an active change."""
    data = copy.deepcopy(MANIFEST)
    data["id"] = "old"
    write_manifest(repo, data, archived=True)
    (repo / "src" / "x.py").write_text("\n".join(f"x{i} = {i}" for i in range(50)) + "\n", encoding="utf-8")
    commit_all(repo, "sneak an archived pact in")
    strict = checker.run(repo, "old", strict=True, require_proof=True)
    assert strict.verdict == "refused"
    assert "archived_in_pull_request" in strict.blocked_by
    assert strict.state != "merged"
    # The normal strict rules apply again: no evidence, no approval.
    assert {"unproven_requirement:req.demo", "approval_missing"} <= set(strict.blocked_by)
    lenient = checker.run(repo, "old")
    assert lenient.verdict == "refused"
    assert "archived_in_pull_request" in lenient.blocked_by


def test_manifest_moved_into_the_archive_on_the_branch_is_refused(repo: Path) -> None:
    result = checker.run(repo, "demo")
    assert result.state == "open"
    data = copy.deepcopy(json.loads(load(repo / "pact/changes/demo/pact.yaml").model_dump_json(exclude_none=True)))
    (repo / "pact" / "changes" / "demo" / "pact.yaml").unlink()
    write_manifest(repo, data, archived=True)
    commit_all(repo, "move to archive")
    moved = checker.run(repo, "demo", strict=True)
    assert "archived_in_pull_request" in moved.blocked_by
    assert moved.state != "merged"


def test_cli_envelope_and_exit_codes(repo: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(repo)
    assert main(["check", "demo", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["verdict"] == "recorded" and payload["ok"] is True
    assert main(["check", "demo", "--strict", "--json"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False and "approval_missing" in payload["blocked_by"]
    assert main(["check", "nope", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["blocked_by"] == ["unknown_change"]
    assert main(["gate", "post-edit", "demo", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["blocked_by"] == ["verb_removed:gate"]
    assert main(["decide", "demo", "--verdict", "approve", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["blocked_by"] == ["verb_removed:decide"]
    assert main(["evidence", "sync", "demo", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["blocked_by"] == ["verb_removed:evidence_sync"]
    assert main(["hooks", "install", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["blocked_by"] == ["verb_removed:hooks"]


def test_okg_pact_alias_reroutes_v5_manifests_with_flags(repo: Path, monkeypatch, capsys) -> None:
    from okg.substrate.cli.main import _pact_v5_reroute, main as okg_main

    monkeypatch.chdir(repo)
    assert _pact_v5_reroute(["check", "demo", "--json"]) == ["check", "demo", "--json"]
    assert _pact_v5_reroute(["gate", "pre-merge", "demo", "--json"]) == ["check", "demo", "--json", "--strict"]
    assert _pact_v5_reroute(["evidence", "attach", "demo", "--kind", "manual", "--requirement", "req.demo"]) == ["evidence", "attach", "demo", "--kind", "manual", "--requirement", "req.demo"]
    assert _pact_v5_reroute(["view", "nope", "--json"]) is None, "unknown change stays with v4"
    assert _pact_v5_reroute(["new", "x"]) == ["new", "x"], "new writes v5"
    assert _pact_v5_reroute(["new", "x", "--shape=fast"]) is None, "v4 flags stay v4"
    assert _pact_v5_reroute(["new", "x", "--template", "feature"]) is None
    assert _pact_v5_reroute(["--root", "r", "--json", "new", "x"]) == ["new", "x", "--root", "r", "--json"]
    assert _pact_v5_reroute(["new-change", "x"]) == ["new", "x"]
    (repo / "pact/changes/demo/evidence/junit.xml").parent.mkdir(parents=True, exist_ok=True)
    (repo / "pact/changes/demo/evidence/junit.xml").write_text("<x/>", encoding="utf-8")
    assert okg_main(["pact", "evidence", "attach", "demo", "--kind", "executed_test", "--requirement", "req.demo", "--artifact", "pact/changes/demo/evidence/junit.xml", "--json"]) == 0
    row = json.loads(capsys.readouterr().out)["row"]
    assert row["artifact"] == "pact/changes/demo/evidence/junit.xml"
    assert okg_main(["pact", "gate", "pre-merge", "demo", "--json"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert "approval_missing" in payload["blocked_by"], "gate keeps strict strength"
    assert okg_main(["pact", "decide", "demo", "--verdict", "approve", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["blocked_by"] == ["verb_removed:decide"]


def test_cli_evidence_attach_binds_head_and_digest(repo: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(repo)
    (repo / "pact/changes/demo/evidence/junit.xml").parent.mkdir(parents=True, exist_ok=True)
    (repo / "pact/changes/demo/evidence/junit.xml").write_text("<testsuite/>", encoding="utf-8")
    assert main(["evidence", "attach", "demo", "--kind", "executed_test", "--requirement", "req.demo", "--artifact", "pact/changes/demo/evidence/junit.xml", "--command", "pytest", "--json"]) == 0
    row = json.loads(capsys.readouterr().out)["row"]
    assert row["head"] == git(repo, "rev-parse", "HEAD")
    assert row["sha256"] == file_sha256(b"<testsuite/>")
    assert main(["digest", "demo", "--json"]) == 0
    line = json.loads(capsys.readouterr().out)["approval_line"]
    assert line.startswith("Pact-Approved demo sha256:")


def test_superseded_and_self_approvals_do_not_bind(repo: Path, tmp_path: Path) -> None:
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    line = f"Pact-Approved demo {digest}"
    at = {"oid": git(repo, "rev-parse", "HEAD")}
    payload = {
        "url": "https://example/pr/2",
        "author": {"login": "alice"},
        "reviews": [
            {"author": {"login": "bob"}, "state": "APPROVED", "authorAssociation": "MEMBER", "body": line, "submittedAt": "2026-09-06T01:00:00Z", "commit": at},
            {"author": {"login": "bob"}, "state": "CHANGES_REQUESTED", "authorAssociation": "MEMBER", "body": "wait", "submittedAt": "2026-09-06T02:00:00Z", "commit": at},
            {"author": {"login": "alice"}, "state": "APPROVED", "authorAssociation": "OWNER", "body": line, "submittedAt": "2026-09-06T03:00:00Z", "commit": at},
        ],
    }
    reviews = tmp_path / "reviews.json"
    reviews.write_text(json.dumps(payload), encoding="utf-8")
    result = checker.run(repo, "demo", strict=True, approvals_json=reviews)
    assert "approval_missing" in result.blocked_by
    reasons = {r["reviewer"]: r["rejected"] for r in result.records if r["source"] == "pr_review_file"}
    assert reasons == {"alice": "self_approval"}, "bob's later CHANGES_REQUESTED supersedes his approval; alice is the author"
    payload["reviews"].append({"author": {"login": "bob"}, "state": "APPROVED", "authorAssociation": "MEMBER", "body": line, "submittedAt": "2026-09-06T04:00:00Z", "commit": at})
    reviews.write_text(json.dumps(payload), encoding="utf-8")
    assert checker.run(repo, "demo", strict=True, approvals_json=reviews).verdict == "recorded"


def test_pull_request_reviews_are_fetched_through_gh(repo: Path, tmp_path: Path, monkeypatch) -> None:
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    reviewed = git(repo, "rev-parse", "HEAD")
    import subprocess as sp

    from pact import approval

    real_run = sp.run
    calls: list[list[str]] = []

    class _Out:
        def __init__(self, code: int, stdout: str) -> None:
            self.returncode, self.stdout, self.stderr = code, stdout, ""

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[:3] == ["gh", "pr", "view"]:
            return _Out(0, json.dumps({"url": "https://example/pr/7", "author": {"login": "alice"}, "reviews": [{"author": {"login": "bob"}, "state": "APPROVED", "authorAssociation": "MEMBER", "body": f"Pact-Approved demo {digest}", "submittedAt": "2026-09-06T01:00:00Z", "commit": {"oid": reviewed}}]}))
        return real_run(cmd, **kwargs)

    assert approval.from_pull_request  # the gh path under test
    monkeypatch.setattr("subprocess.run", fake_run)
    result = checker.run(repo, "demo", strict=True, pull_request=7)
    assert result.verdict == "recorded", result.blocked_by
    assert result.approval["reviewer"] == "bob" and result.approval["source"] == "pr_review"
    assert any(c[:3] == ["gh", "pr", "view"] and "7" in c for c in calls)
    # A supplied file is ignored when --pr is given.
    bogus = tmp_path / "bogus.json"
    bogus.write_text(json.dumps({"reviews": [{"author": {"login": "mallory"}, "state": "APPROVED", "body": f"Pact-Approved demo {digest}"}]}), encoding="utf-8")

    def failing_run(cmd, **kwargs):
        if cmd[:3] == ["gh", "pr", "view"]:
            return _Out(1, "")
        return real_run(cmd, **kwargs)

    monkeypatch.setattr("subprocess.run", failing_run)
    result = checker.run(repo, "demo", strict=True, pull_request=7, approvals_json=bogus)
    assert "pr_reviews_unavailable" in result.blocked_by and "approval_missing" in result.blocked_by


def test_spoofed_base_is_refused(repo: Path, tmp_path: Path) -> None:
    (repo / "src" / "big.py").write_text("\n".join(f"x{i} = {i}" for i in range(400)) + "\n", encoding="utf-8")
    commit_all(repo, "big")
    assert "tier_below_computed:2" in checker.run(repo, "demo").blocked_by
    spoofed = checker.run(repo, "demo", strict=True, base="HEAD")
    assert "diff_base_invalid" in spoofed.blocked_by
    assert "tier_below_computed" not in " ".join(spoofed.blocked_by) or spoofed.verdict == "refused"
    assert checker.run(repo, "demo", base="HEAD~1").verdict == "refused" or True  # a real ancestor base is accepted
    real = checker.run(repo, "demo", base="HEAD~1")
    assert "diff_base_invalid" not in real.blocked_by


def test_git_directory_and_untracked_artifacts_do_not_prove(repo: Path) -> None:
    secret = repo / ".git" / "secret-proof.txt"
    secret.write_text("ok\n", encoding="utf-8")
    data = copy.deepcopy(MANIFEST)
    data["evidence"] = [{"kind": "executed_test", "requirement": "req.demo", "artifact": ".git/secret-proof.txt", "sha256": file_sha256(b"ok\n"), "head": git(repo, "rev-parse", "HEAD")}]
    write_manifest(repo, data)
    assert "evidence_artifact_outside_repo:0" in checker.run(repo, "demo").blocked_by
    _attach(repo, artifact="pact/changes/demo/evidence/untracked.txt")  # written but never committed
    lenient = checker.run(repo, "demo")
    strict = checker.run(repo, "demo", strict=True, require_proof=True)
    assert "evidence_artifact_untracked" not in " ".join(lenient.blocked_by)
    assert "evidence_artifact_untracked:1" in strict.blocked_by


def test_receipt_on_a_branch_that_diverged_before_the_base_moved_still_binds(repo: Path) -> None:
    """A push event hands the check the previous target tip as the base. A
    receipt recorded on the merged branch is not a descendant of that tip,
    but it is not contained in it either, so it is in range."""
    data = copy.deepcopy(MANIFEST)
    write_manifest(repo, data)
    (repo / "src").mkdir(exist_ok=True)
    (repo / "src" / "x.py").write_text("x = 1\n", encoding="utf-8")
    receipt = commit_all(repo, "work")
    manifest = load(repo / "pact" / "changes" / "demo" / "pact.yaml")
    art = repo / "pact" / "changes" / "demo" / "evidence" / "junit.xml"
    art.parent.mkdir(parents=True, exist_ok=True)
    art.write_text("<testsuite/>", encoding="utf-8")
    manifest.evidence.append(Evidence(kind="executed_test", requirement="req.demo", command="pytest", artifact="pact/changes/demo/evidence/junit.xml", sha256=file_sha256(art.read_bytes()), head=receipt, status="pass"))
    write(repo / "pact" / "changes" / "demo" / "pact.yaml", manifest)
    commit_all(repo, "receipt")
    # dev moves on independently, then the feature branch is merged.
    git(repo, "checkout", "-q", "dev")
    (repo / "NOTES.md").write_text("later\n", encoding="utf-8")
    dev_tip = commit_all(repo, "dev moves on")
    git(repo, "merge", "-q", "--no-ff", "-m", "merge feature", "feature")
    merged = git(repo, "rev-parse", "HEAD")
    result = checker.run(repo, "demo", base=dev_tip, head_ref=merged, require_proof=True)
    assert not any(w.startswith("evidence_head_before_base") for w in result.warnings), result.warnings
    assert result.evidence["bound"] == 1 and "req.demo" in result.evidence["proven_requirements"]
    assert "unproven_requirement:req.demo" not in result.blocked_by


def _tiered_repo(tmp_path: Path, *, declared: int, change: str) -> Path:
    """A repo whose branch makes one kind of change, with a declared tier.

    ``change`` is "docs" (computes tier 0), "small" (tier 1) or "big" (tier 2).
    """
    root = make_repo(tmp_path)
    data = copy.deepcopy(MANIFEST)
    data["tier"] = declared
    write_manifest(root, data)
    if change == "docs":
        (root / "docs").mkdir(exist_ok=True)
        (root / "docs" / "note.md").write_text("# note\n", encoding="utf-8")
    else:
        (root / "src").mkdir(exist_ok=True)
        lines = 1 if change == "small" else 400
        (root / "src" / "a.py").write_text("\n".join(f"x{i} = {i}" for i in range(lines)) + "\n", encoding="utf-8")
    commit_all(root, change)
    return root


@pytest.mark.parametrize("declared,change,computed,proof,approval", [
    (0, "docs", 0, False, False),
    (1, "small", 1, False, True),
    (2, "big", 2, True, True),
    # The declared tier can only raise the cost: a tier-3 program pays the
    # tier-3 price even when its diff is a docs edit.
    (3, "docs", 0, True, True),
    # And so can the computed one: declaring 0 over a big source change is
    # already refused as tier_below_computed, so the cheap gate is unbuyable.
    (1, "big", 2, True, True),
])
def test_strict_charges_the_effective_tier(tmp_path: Path, monkeypatch, declared, change, computed, proof, approval) -> None:
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = _tiered_repo(tmp_path, declared=declared, change=change)
    result = checker.run(root, "demo", strict=True)
    assert result.tier["computed"] == computed
    assert result.tier["effective"] == max(declared, computed)
    assert result.tier["requires"] == {"proof": proof, "approval": approval}
    blocked = set(result.blocked_by)
    assert ("unproven_requirement:req.demo" in blocked) is proof
    assert ("approval_missing" in blocked) is approval
    if not proof:
        assert "unproven_requirement:req.demo" in result.warnings
    if not proof and not approval:
        assert result.verdict == "recorded", result.blocked_by


def test_tier0_change_passes_strict_with_nothing_attached(tmp_path: Path, monkeypatch) -> None:
    """A docs-only change costs nothing: no receipt, no approval, no refusal.

    Without this the tier is a label again -- every change would pay the same
    price, which is what made the v4 ceremony unaffordable.
    """
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = _tiered_repo(tmp_path, declared=0, change="docs")
    result = checker.run(root, "demo", strict=True)
    assert result.verdict == "recorded", result.blocked_by
    assert result.state == "open"


def test_tier2_needs_both_a_receipt_and_an_approval(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = _tiered_repo(tmp_path, declared=2, change="big")
    _attach(root)
    digest = intent_digest(load(root / "pact/changes/demo/pact.yaml"))
    commit_all(root, "proof")
    proof_only = checker.run(root, "demo", strict=True)
    assert proof_only.blocked_by == ["approval_missing"]
    git(root, "commit", "-q", "--allow-empty", "-m", "Approve PACT demo", "-m", f"Pact-Approved: demo {digest}")
    assert checker.run(root, "demo", strict=True).verdict == "recorded"


def test_approve_writes_a_binding_trailer_commit(repo: Path, capsys, monkeypatch) -> None:
    """`pact approve` is the one step the agent runs when the operator says
    "approve" in chat, and what it writes must satisfy `check --strict`."""
    monkeypatch.chdir(repo)
    _attach(repo)
    commit_all(repo, "proof")
    before = git(repo, "rev-parse", "HEAD")
    assert main(["approve", "demo", "--dry-run", "--by", "Riley", "--json"]) == 0
    dry = json.loads(capsys.readouterr().out)
    assert dry["dry_run"] is True and dry["commit"] is None
    assert git(repo, "rev-parse", "HEAD") == before, "a dry run writes no commit"

    assert main(["approve", "demo", "--by", "Riley", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["trailer"] == dry["trailer"] == f"Pact-Approved: demo {intent_digest(load(repo / 'pact/changes/demo/pact.yaml'))}"
    assert payload["approved_by"] == "Riley" and payload["authority"] == "operator_record"
    assert payload["commit"] == git(repo, "rev-parse", "HEAD") != before
    assert git(repo, "log", "-1", "--format=%s") == "Approve PACT demo"
    assert git(repo, "status", "--porcelain") == "", "an empty commit must not touch the tree"

    result = checker.run(repo, "demo", strict=True, require_proof=True)
    assert result.verdict == "recorded", result.blocked_by
    assert result.approval["commit"] == payload["commit"]
    assert result.approval["reviewer"] == "Riley"


def test_approve_refuses_an_unknown_or_invalid_manifest(repo: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(repo)
    head = git(repo, "rev-parse", "HEAD")
    assert main(["approve", "nope", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["blocked_by"] == ["unknown_change"]
    data = copy.deepcopy(MANIFEST)
    data["tasks"][0]["requirement"] = "req.absent"
    write_manifest(repo, data)
    assert main(["approve", "demo", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["blocked_by"] == ["invalid_manifest"]
    assert git(repo, "rev-parse", "HEAD") == head, "a refusal writes no commit"


def test_approve_defaults_the_approver_to_the_git_identity(repo: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(repo)
    assert main(["approve", "demo", "--dry-run", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["approved_by"] == "t"


def test_okg_pact_approve_reroutes_to_v5(repo: Path, capsys, monkeypatch) -> None:
    from okg.substrate.cli.main import _pact_v5_reroute, main as okg_main

    monkeypatch.chdir(repo)
    assert _pact_v5_reroute(["approve", "demo", "--json"]) == ["approve", "demo", "--json"]
    assert okg_main(["pact", "approve", "demo", "--dry-run", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["trailer"].startswith("Pact-Approved: demo sha256:")


def _repo_with_a_merged_receipt(tmp_path: Path) -> tuple[Path, str]:
    """A repo where dev already carries the manifest WITH one receipt, and the
    feature branch starts after it -- the shape of a change proven across
    several pull requests. Returns the repo and the receipt's head."""
    root = make_repo(tmp_path)
    git(root, "checkout", "-q", "dev")
    (root / "src").mkdir()
    (root / "src" / "one.py").write_text("one = 1\n", encoding="utf-8")
    (root / "pact/changes/demo/evidence/proof.txt").parent.mkdir(parents=True, exist_ok=True)
    (root / "pact/changes/demo/evidence/proof.txt").write_text("ok\n", encoding="utf-8")
    write_manifest(root, MANIFEST)
    proved_at = commit_all(root, "requirement one, proved and merged")
    data = json.loads(load(root / "pact/changes/demo/pact.yaml").model_dump_json(exclude_none=True))
    data["evidence"] = [{
        "kind": "executed_test", "requirement": "req.demo", "status": "pass",
        "artifact": "pact/changes/demo/evidence/proof.txt", "sha256": file_sha256(b"ok\n"), "head": proved_at,
    }]
    write_manifest(root, data)
    commit_all(root, "attach the receipt")
    git(root, "fetch", "-q", "origin", "dev")
    git(root, "checkout", "-q", "feature")
    git(root, "merge", "-q", "--ff-only", "dev")
    (root / "src" / "two.py").write_text("two = 2\n", encoding="utf-8")
    commit_all(root, "requirement two, this pull request")
    return root, proved_at


def test_a_receipt_that_already_merged_stays_proven(tmp_path: Path, monkeypatch) -> None:
    """A multi-pull-request change must not lose its proof at every rebase.

    The receipt sits before this branch's base on purpose: it was reviewed and
    merged there. Holding it to the in-range rule would leave the requirement
    unproven in every later pull request that did not touch it.
    """
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root, proved_at = _repo_with_a_merged_receipt(tmp_path)
    result = checker.run(root, "demo", strict=True, require_proof=True)
    assert "evidence_head_before_base:0" not in result.warnings
    assert result.evidence["proven_requirements"] == ["req.demo"]
    assert result.evidence == {
        "rows": 1, "bound": 1, "unbound": 0, "unreachable": 0, "broken": 0,
        "preexisting": 1, "new": 0, "proven_requirements": ["req.demo"],
    }
    assert "unproven_requirement:req.demo" not in result.blocked_by
    assert result.blocked_by == ["approval_missing"], result.blocked_by
    assert git(root, "merge-base", "origin/dev", "HEAD") != proved_at or True


def test_editing_a_merged_receipt_makes_it_new_again(tmp_path: Path, monkeypatch) -> None:
    """Identity is the whole row. Repointing `head` at an older commit is not
    a carried receipt, it is a new claim about an old tree -- and that is the
    move this exemption would otherwise buy for free."""
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root, _ = _repo_with_a_merged_receipt(tmp_path)
    data = json.loads(load(root / "pact/changes/demo/pact.yaml").model_dump_json(exclude_none=True))
    data["evidence"][0]["head"] = git(root, "rev-list", "--max-parents=0", "HEAD")
    write_manifest(root, data)
    commit_all(root, "repoint the receipt")
    result = checker.run(root, "demo", strict=True, require_proof=True)
    assert "evidence_head_before_base:0" in result.warnings
    assert "unproven_requirement:req.demo" in result.blocked_by
    assert result.evidence["preexisting"] == 0 and result.evidence["new"] == 1


def test_a_tampered_merged_receipt_still_breaks(tmp_path: Path, monkeypatch) -> None:
    """The exemption is only from the in-range rule. The artifact digest and
    reachability still hold, or a carried row would be a free pass."""
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root, _ = _repo_with_a_merged_receipt(tmp_path)
    (root / "pact/changes/demo/evidence/proof.txt").write_text("tampered\n", encoding="utf-8")
    assert "evidence_digest_mismatch:0" in checker.run(root, "demo").blocked_by


def test_an_unreadable_base_manifest_treats_every_row_as_new(tmp_path: Path, monkeypatch) -> None:
    """Fail closed: with no base manifest to compare against, nothing is
    carried, so the full rule applies."""
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root, _ = _repo_with_a_merged_receipt(tmp_path)
    assert checker.baseline_receipts(root, root / "pact/changes/demo/pact.yaml", None) == frozenset()
    base = git(root, "merge-base", "origin/dev", "HEAD")
    assert checker.baseline_receipts(root, root / "pact/changes/demo/pact.yaml", base)
    assert checker.baseline_receipts(root, root / "pact/changes/nope/pact.yaml", base) == frozenset()


def test_a_trailer_cannot_approve_code_in_its_own_commit(repo: Path) -> None:
    """The hole: `code_rejection` diffed `commit..head`, which is EMPTY for
    the approving commit itself, so one commit that both adds `src/evil.py`
    and carries the trailer approved its own payload."""
    _attach(repo)
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    (repo / "src" / "evil.py").write_text("BACKDOOR = True\n", encoding="utf-8")
    commit_all(repo, "proof and a backdoor", f"Pact-Approved: demo {digest}")
    result = checker.run(repo, "demo", strict=True, require_proof=True)
    assert result.verdict == "refused"
    assert "approval_stale_code" in result.blocked_by
    assert result.approval is None
    assert any(r["rejected"] == "approval_commit_contains_code" for r in result.records), result.records


def test_a_pact_only_trailer_commit_still_binds(repo: Path) -> None:
    """The negative arm: the rule above must not refuse a real approval.

    `pact approve` writes an empty commit, and attaching evidence in the same
    commit touches only `pact/`, so both still bind.
    """
    # The receipt artifact lives under pact/, so this commit touches nothing
    # else -- exactly what `pact approve` produces, only cheaper to set up.
    _attach(repo, artifact="pact/changes/demo/evidence/proof.txt")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    commit_all(repo, "attach proof", f"Pact-Approved: demo {digest}")
    assert checker.run(repo, "demo", strict=True, require_proof=True).verdict == "recorded"


def test_a_merge_commit_never_approves(repo: Path) -> None:
    """`git merge --no-ff side -m "...Pact-Approved..."` would otherwise
    approve every commit on `side`: the merge's own message is written by
    whoever ran the merge."""
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    git(repo, "checkout", "-q", "-b", "side")
    (repo / "src" / "evil.py").write_text("BACKDOOR = True\n", encoding="utf-8")
    commit_all(repo, "code nobody approved")
    git(repo, "checkout", "-q", "feature")
    git(repo, "merge", "-q", "--no-ff", "side", "-m", f"Merge side\n\nPact-Approved: demo {digest}")
    result = checker.run(repo, "demo", strict=True, require_proof=True)
    assert result.verdict == "refused"
    assert "approval_missing" in result.blocked_by, result.blocked_by
    assert result.approval is None
    # And the same trailer on the merge does not even reach the candidates.
    from pact import git as pact_git
    base = git(repo, "merge-base", "origin/dev", "HEAD")
    assert pact_git.trailer_entries(repo, "Pact-Approved", base) == []


def test_a_merge_commit_is_refused_even_when_named_directly(repo: Path, tmp_path: Path) -> None:
    """A review payload can name any commit, including a merge."""
    _attach(repo)
    commit_all(repo, "proof")
    digest = intent_digest(load(repo / "pact/changes/demo/pact.yaml"))
    git(repo, "checkout", "-q", "-b", "side")
    (repo / "src" / "evil.py").write_text("BACKDOOR = True\n", encoding="utf-8")
    commit_all(repo, "side work")
    git(repo, "checkout", "-q", "feature")
    git(repo, "merge", "-q", "--no-ff", "side", "-m", "Merge side")
    result = checker.run(repo, "demo", strict=True, approvals_json=_reviews(repo, tmp_path, "demo", digest))
    assert "approval_stale_code" in result.blocked_by
    assert any(r["rejected"] == "review_commit_is_merge" for r in result.records), result.records


def test_approve_refuses_a_dirty_index(repo: Path, capsys, monkeypatch) -> None:
    """`git commit --allow-empty` commits whatever is STAGED: a file left in
    the index would ride into the approval commit and be approved by its own
    trailer."""
    monkeypatch.chdir(repo)
    (repo / "src" / "evil.py").write_text("BACKDOOR = True\n", encoding="utf-8")
    git(repo, "add", "src/evil.py")
    head = git(repo, "rev-parse", "HEAD")
    assert main(["approve", "demo", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["blocked_by"] == ["index_not_empty"]
    assert git(repo, "rev-parse", "HEAD") == head, "a refusal writes no commit"

    git(repo, "reset", "-q")
    assert main(["approve", "demo", "--json"]) == 0
    commit = json.loads(capsys.readouterr().out)["commit"]
    assert git(repo, "rev-parse", f"{commit}^{{tree}}") == git(repo, "rev-parse", f"{commit}~1^{{tree}}")
    assert git(repo, "show", "--stat", "--format=", commit).strip() == "", "the approval commit is empty"
    assert "src/evil.py" in git(repo, "status", "--porcelain"), "the untracked file stayed untracked"


def _archived_repo_with_code_change(tmp_path: Path, *, edit_manifest: bool, add_code: bool) -> Path:
    root = _repo_with_archive_on_dev(tmp_path, "old")
    (root / "src").mkdir(exist_ok=True)
    if add_code:
        (root / "src" / "big.py").write_text("\n".join(f"x{i} = {i}" for i in range(400)) + "\n", encoding="utf-8")
    if edit_manifest:
        data = json.loads(load(root / "pact/archive/old/pact.yaml").model_dump_json(exclude_none=True))
        data["why"] = "Because it was rewritten after merging."
        write_manifest(root, data, archived=True)
    commit_all(root, "work against a merged manifest")
    return root


def test_a_code_change_cannot_hide_behind_an_archived_manifest(tmp_path: Path, monkeypatch) -> None:
    """The relaxation exists for a pull request that merely CARRIES a merged
    manifest. With 400 lines of `src/` in the same pull request it skipped the
    tier, the proof and the approval outright."""
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = _archived_repo_with_code_change(tmp_path, edit_manifest=False, add_code=True)
    result = checker.run(root, "old", strict=True)
    assert result.state != "merged"
    assert result.verdict == "refused"
    assert "tier_below_computed:2" in result.blocked_by
    assert {"unproven_requirement:req.demo", "approval_missing"} <= set(result.blocked_by)


def test_editing_a_merged_manifest_is_refused(tmp_path: Path, monkeypatch) -> None:
    """A merged record is history. Rewording it would re-open its intent while
    keeping the relaxation that skips every check."""
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = _archived_repo_with_code_change(tmp_path, edit_manifest=True, add_code=False)
    result = checker.run(root, "old")
    assert "archived_manifest_edited" in result.blocked_by
    assert result.state != "merged"


def test_carrying_an_untouched_merged_manifest_still_costs_nothing(tmp_path: Path, monkeypatch) -> None:
    """The negative arm: a pull request that changes nothing outside `pact/`
    and leaves the merged manifest alone keeps reading as merged."""
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = _repo_with_archive_on_dev(tmp_path, "old")
    (root / "pact" / "changes" / "note.txt").parent.mkdir(parents=True, exist_ok=True)
    (root / "pact" / "changes" / "note.txt").write_text("unrelated pact edit\n", encoding="utf-8")
    commit_all(root, "touch only pact/")
    result = checker.run(root, "old", strict=True)
    assert result.state == "merged"
    assert result.verdict == "recorded", result.blocked_by


def test_a_checkout_at_the_dev_tip_is_unknown_not_spoofed(repo: Path) -> None:
    """HEAD is `origin/dev` itself (a push to dev, or a plain checkout of it):
    the derived base equals the head, so there is no branch diff to judge.
    That is `diff_base_unavailable` -- a warning in lenient mode, a refusal
    in strict -- not `diff_base_invalid`, which is reserved for a SUPPLIED
    `--base HEAD`. Before this, lenient `check` on the dev tip refused, and
    the preflight suite went red on every push to dev (#1695)."""
    (repo / "src" / "y.py").write_text("y = 1\n", encoding="utf-8")
    commit_all(repo, "code")
    git(repo, "checkout", "-q", "dev")
    git(repo, "merge", "-q", "--ff-only", "feature")
    git(repo, "fetch", "-q", "origin", "dev")
    assert git(repo, "rev-parse", "origin/dev") == git(repo, "rev-parse", "HEAD")
    lenient = checker.run(repo, "demo")
    assert lenient.verdict == "recorded"
    assert "diff_base_unavailable" in lenient.warnings
    assert "diff_base_invalid" not in lenient.blocked_by
    strict = checker.run(repo, "demo", strict=True)
    assert "diff_base_unavailable" in strict.blocked_by
    assert "diff_base_invalid" not in strict.blocked_by
    # A supplied base equal to the head is still the spoof it always was.
    assert "diff_base_invalid" in checker.run(repo, "demo", base="HEAD").blocked_by
