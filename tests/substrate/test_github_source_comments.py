"""Forge discussion + linkage: comment ingestion under a per-comment
cutoff, merged_as from payload merge shas, and reference extraction that
drops out-of-corpus targets with a count.

PACT okg-corpus-repo-history (req.repo.discussion, req.repo.linkage).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from okg.bench.ab import corpus as corpus_mod
from okg.bench.ab.errors import CorpusBuildError
from okg.substrate.library.linkers.forge_references import (
    ForgeReferenceLinker,
    extract_refs,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.github import (
    ForgeDiscussionError,
    GitHubSource,
)


REPO = "psf/requests"
CUTOFF = "2014-11-01T02:20:16Z"
GOLD_PR = 2317


def _issue(number: int, created: str, *, pr: bool = False, body: str = "",
           merged_at: str | None = None) -> dict:
    item: dict = {
        "number": number,
        "title": f"item {number}",
        "body": body,
        "state": "closed",
        "user": {"login": "maintainer"},
        "html_url": f"https://github.com/{REPO}/issues/{number}",
        "created_at": created,
        "updated_at": created,
        "closed_at": created,
        "labels": [],
    }
    if pr:
        item["pull_request"] = {
            "html_url": f"https://github.com/{REPO}/pull/{number}",
            "merged_at": merged_at,
        }
    return item


def _comment(cid: int, parent: int, created: str, body: str = "why",
             kind: str = "issue") -> dict:
    key = "issue_url" if kind == "issue" else "pull_request_url"
    path = "issues" if kind == "issue" else "pulls"
    row = {
        "id": cid,
        key: f"https://api.github.com/repos/{REPO}/{path}/{parent}",
        "user": {"login": "reviewer"},
        "body": body,
        "created_at": created,
        "updated_at": created,
        "author_association": "MEMBER",
        "html_url": f"https://github.com/{REPO}/{path}/{parent}#c{cid}",
    }
    if kind != "issue":
        row["path"] = "requests/sessions.py"
        row["commit_id"] = "a" * 40
    return row


def _run(source: GitHubSource) -> list:
    return list(source.run("run-1").facts)


def _write(tmp_path: Path, payload) -> Path:
    p = tmp_path / "issues.json"
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


# -- req.repo.discussion ------------------------------------------------


def test_comments_emit_with_parent_edges(tmp_path: Path) -> None:
    """Comments become first-class nodes hung off their parent thread."""
    payload = {
        "items": [
            _issue(10, "2013-01-01T00:00:00Z"),
            _issue(11, "2013-02-01T00:00:00Z", pr=True),
        ],
        "comments": [
            _comment(100, 10, "2013-01-02T00:00:00Z", "because of redirects"),
            _comment(101, 11, "2013-02-02T00:00:00Z", "lgtm"),
        ],
        "review_comments": [
            _comment(200, 11, "2013-02-03T00:00:00Z", "nit: rename",
                     kind="pull"),
        ],
    }
    facts = _run(GitHubSource(repo=REPO, fixture_path=_write(tmp_path, payload)))
    comments = {
        f.node_id: f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "forge_comment"
    }
    assert len(comments) == 3
    issue_comment = comments[f"comment:github:{REPO}:ic:100"]
    assert issue_comment.attrs["comment_kind"] == "issue_comment"
    assert issue_comment.attrs["parent_number"] == 10
    assert issue_comment.attrs["body"] == "because of redirects"
    assert issue_comment.attrs["author_login"] == "reviewer"
    review = comments[f"comment:github:{REPO}:rc:200"]
    assert review.attrs["comment_kind"] == "review_comment"
    assert review.attrs["path"] == "requests/sessions.py"

    edges = [
        (e.src, e.dst) for e in facts
        if isinstance(e, EdgeFact) and e.edge_type == "has_comment"
    ]
    assert (f"issue:github:{REPO}:#10", f"comment:github:{REPO}:ic:100") in edges
    assert (f"pr:github:{REPO}:#11", f"comment:github:{REPO}:ic:101") in edges
    assert (f"pr:github:{REPO}:#11", f"comment:github:{REPO}:rc:200") in edges
    assert len(edges) == 3


def test_comment_on_absent_parent_is_skipped_not_stubbed(
    tmp_path: Path,
) -> None:
    """A comment whose parent is outside the payload attaches to nothing —
    and does NOT conjure a parent node."""
    payload = {
        "items": [_issue(10, "2013-01-01T00:00:00Z")],
        "comments": [
            _comment(100, 10, "2013-01-02T00:00:00Z"),
            _comment(101, 9999, "2013-01-03T00:00:00Z"),
        ],
    }
    facts = _run(GitHubSource(repo=REPO, fixture_path=_write(tmp_path, payload)))
    node_ids = {f.node_id for f in facts if isinstance(f, NodeFact)}
    assert f"comment:github:{REPO}:ic:101" not in node_ids
    assert f"issue:github:{REPO}:#9999" not in node_ids
    assert not [
        e for e in facts
        if isinstance(e, EdgeFact) and "9999" in f"{e.src}{e.dst}"
    ]


def test_comments_parsed_to_zero_raises_with_counts(tmp_path: Path) -> None:
    """A payload that carries comments and normalizes to none is a broken
    parse, never a quiet repository."""
    payload = {
        "items": [_issue(10, "2013-01-01T00:00:00Z")],
        "comments": [_comment(100, 4242, "2013-01-02T00:00:00Z")],
    }
    src = GitHubSource(repo=REPO, fixture_path=_write(tmp_path, payload))
    with pytest.raises(ForgeDiscussionError) as exc:
        _run(src)
    assert "1 comment row(s)" in str(exc.value)
    assert exc.value.data["comments_in"] == 1
    assert exc.value.data["comments_out"] == 0


def test_comment_without_id_raises(tmp_path: Path) -> None:
    row = _comment(1, 10, "2013-01-02T00:00:00Z")
    del row["id"]
    payload = {"items": [_issue(10, "2013-01-01T00:00:00Z")], "comments": [row]}
    with pytest.raises(ForgeDiscussionError, match="without an `id`"):
        _run(GitHubSource(repo=REPO, fixture_path=_write(tmp_path, payload)))


def test_legacy_bare_array_payload_still_works(tmp_path: Path) -> None:
    """The historical fixture shape (a bare JSON array) keeps working and
    emits no comment facts."""
    facts = _run(GitHubSource(
        repo=REPO,
        fixture_path=_write(tmp_path, [_issue(10, "2013-01-01T00:00:00Z")]),
    ))
    assert [f for f in facts if isinstance(f, NodeFact)
            and f.subtype == "forge_comment"] == []
    assert any(
        isinstance(f, NodeFact) and f.subtype == "issue" for f in facts
    )


# -- the per-comment cutoff (the subtle requirement) --------------------


def test_comments_respect_the_cutoff(monkeypatch) -> None:
    """A comment created at/after the cutoff is excluded EVEN WHEN its
    parent issue predates the cutoff, and the gold PR's whole thread stays
    out. A per-parent filter would keep the first of these — that is the
    defect this test exists to catch.
    """
    kept_issue = _issue(10, "2013-01-01T00:00:00Z")
    issues = [
        kept_issue,
        _issue(2400, "2015-01-01T00:00:00Z"),      # post-cutoff parent
        _issue(GOLD_PR, "2014-10-30T00:00:00Z", pr=True),
    ]
    comments = [
        # on a PRE-cutoff parent, written BEFORE the cutoff -> kept
        _comment(100, 10, "2013-06-01T00:00:00Z"),
        # on the SAME pre-cutoff parent, written AFTER the cutoff -> DROPPED
        _comment(101, 10, "2019-06-01T00:00:00Z"),
        # exactly AT the cutoff -> dropped (the bound is strict)
        _comment(102, 10, CUTOFF),
        # gold PR thread, pre-cutoff -> dropped as gold
        _comment(103, GOLD_PR, "2014-10-31T00:00:00Z"),
        # parent itself post-cutoff -> dropped as orphan
        _comment(104, 2400, "2014-01-01T00:00:00Z"),
    ]
    review = [_comment(200, 10, "2013-07-01T00:00:00Z", kind="pull")]

    def fake_gh(args):
        path = args[-1]
        if "issues/comments" in path:
            return comments
        if "pulls/comments" in path:
            return review
        if "/pulls?" in path:
            return [{
                "number": 10, "merged_at": "2013-01-05T00:00:00Z",
                "merge_commit_sha": "b" * 40,
            }]
        return issues

    monkeypatch.setattr(corpus_mod, "_gh_json", fake_gh)
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        target = Path(td) / "issues.json"
        counts = corpus_mod.fetch_forge_fixture(
            repo=REPO, cutoff=CUTOFF, gold_pr_number=GOLD_PR,
            out_path=target,
        )
        payload = json.loads(target.read_text())

    kept_ids = {c["id"] for c in payload["comments"]}
    assert kept_ids == {100}, (
        "only the pre-cutoff comment on a pre-cutoff parent survives"
    )
    assert {c["id"] for c in payload["review_comments"]} == {200}
    assert counts["dropped_comments_post_cutoff"] == 2  # #101 and #102
    assert counts["dropped_comments_gold"] == 1
    assert counts["dropped_comments_orphan"] == 1
    assert counts["kept_comments"] == 1
    assert counts["kept_review_comments"] == 1

    # ...and the gold PR itself never reaches the fixture.
    assert GOLD_PR not in {i["number"] for i in payload["items"]}
    assert all(
        c["id"] != 103 for c in
        payload["comments"] + payload["review_comments"]
    )


def test_post_cutoff_comment_updated_at_is_clamped(monkeypatch) -> None:
    """An edited comment carries a post-cutoff updated_at that the leakage
    audit would flag; it is clamped to created_at."""
    edited = _comment(100, 10, "2013-06-01T00:00:00Z")
    edited["updated_at"] = "2020-01-01T00:00:00Z"

    def fake_gh(args):
        path = args[-1]
        if "issues/comments" in path:
            return [edited]
        if "pulls/comments" in path:
            return []
        if "/pulls?" in path:
            return [{
                "number": 10, "merged_at": "2013-01-05T00:00:00Z",
                "merge_commit_sha": "b" * 40,
            }]
        return [_issue(10, "2013-01-01T00:00:00Z")]

    monkeypatch.setattr(corpus_mod, "_gh_json", fake_gh)
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        target = Path(td) / "issues.json"
        counts = corpus_mod.fetch_forge_fixture(
            repo=REPO, cutoff=CUTOFF, gold_pr_number=GOLD_PR, out_path=target,
        )
        payload = json.loads(target.read_text())
    assert payload["comments"][0]["updated_at"] == "2013-06-01T00:00:00Z"
    assert counts["scrubbed_comment_updated_at"] == 1


def test_all_comments_dropped_fails_the_build(monkeypatch) -> None:
    """Zero surviving comments out of a non-empty fetch fails loudly rather
    than writing a discussion-free corpus."""
    def fake_gh(args):
        path = args[-1]
        if "issues/comments" in path:
            return [_comment(100, 10, "2019-01-01T00:00:00Z")]
        if "pulls/comments" in path:
            return []
        if "/pulls?" in path:
            return [{
                "number": 10, "merged_at": "2013-01-05T00:00:00Z",
                "merge_commit_sha": "b" * 40,
            }]
        return [_issue(10, "2013-01-01T00:00:00Z")]

    monkeypatch.setattr(corpus_mod, "_gh_json", fake_gh)
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        with pytest.raises(CorpusBuildError, match="0 survived the cutoff"):
            corpus_mod.fetch_forge_fixture(
                repo=REPO, cutoff=CUTOFF, gold_pr_number=GOLD_PR,
                out_path=Path(td) / "issues.json",
            )


# -- req.repo.linkage: merged_as ---------------------------------------


def test_merged_as_emits_from_payload_merge_sha(tmp_path: Path) -> None:
    payload = {
        "items": [
            _issue(11, "2013-02-01T00:00:00Z", pr=True,
                   merged_at="2013-02-05T00:00:00Z"),
            _issue(12, "2013-03-01T00:00:00Z", pr=True),
        ],
        "pulls": [
            {"number": 11, "merged_at": "2013-02-05T00:00:00Z",
             "merge_commit_sha": "c" * 40},
        ],
    }
    facts = _run(GitHubSource(
        repo=REPO, commit_repo="bench-repo",
        fixture_path=_write(tmp_path, payload),
    ))
    merged = [e for e in facts
              if isinstance(e, EdgeFact) and e.edge_type == "merged_as"]
    assert len(merged) == 1
    assert merged[0].src == f"pr:github:{REPO}:#11"
    assert merged[0].dst == f"commit:bench-repo:{'c' * 40}"
    assert merged[0].attrs["merge_commit_sha"] == "c" * 40


def test_merged_at_is_read_from_the_nested_pull_request_block(
    tmp_path: Path,
) -> None:
    """The issues endpoint nests merged_at under `pull_request`; reading
    only the top level marked every merged PR as unmerged."""
    payload = {"items": [
        _issue(11, "2013-02-01T00:00:00Z", pr=True,
               merged_at="2013-02-05T00:00:00Z"),
    ]}
    facts = _run(GitHubSource(repo=REPO, fixture_path=_write(tmp_path, payload)))
    pr = next(f for f in facts
              if isinstance(f, NodeFact) and f.subtype == "pull_request")
    assert pr.attrs["merged"] is True
    assert pr.attrs["state"] == "merged"
    assert pr.attrs["merged_at"] == "2013-02-05T00:00:00Z"


def test_merge_shas_without_commit_repo_raise(tmp_path: Path) -> None:
    """Silently dropping the join because a config field is unset is
    exactly the silent-fallback class this repo treats as P0."""
    payload = {
        "items": [_issue(11, "2013-02-01T00:00:00Z", pr=True,
                         merged_at="2013-02-05T00:00:00Z")],
        "pulls": [{"number": 11, "merged_at": "2013-02-05T00:00:00Z",
                   "merge_commit_sha": "c" * 40}],
    }
    with pytest.raises(ForgeDiscussionError, match="commit_repo is not"):
        _run(GitHubSource(repo=REPO, fixture_path=_write(tmp_path, payload)))


def test_opt_out_of_merge_link_imports_the_pull_without_merged_as(
    tmp_path: Path,
) -> None:
    """The explicit opt-out keeps the pull request and its comments and emits
    no merged_as edge; the strict default above still raises."""
    payload = {
        "items": [_issue(11, "2013-02-01T00:00:00Z", pr=True,
                         merged_at="2013-02-05T00:00:00Z")],
        "comments": [_comment(100, 11, "2013-02-02T00:00:00Z", kind="issue")],
        "pulls": [{"number": 11, "merged_at": "2013-02-05T00:00:00Z",
                   "merge_commit_sha": "c" * 40}],
    }
    facts = _run(GitHubSource(
        repo=REPO, fixture_path=_write(tmp_path, payload),
        link_merge_commits=False,
    ))
    assert any(isinstance(f, NodeFact) and f.subtype == "pull_request" for f in facts)
    assert any(isinstance(f, NodeFact) and f.subtype == "forge_comment" for f in facts)
    assert not any(isinstance(f, EdgeFact) and f.edge_type == "merged_as" for f in facts)


def test_opt_out_together_with_commit_repo_is_a_contradiction(
    tmp_path: Path,
) -> None:
    payload = {
        "items": [_issue(11, "2013-02-01T00:00:00Z", pr=True,
                         merged_at="2013-02-05T00:00:00Z")],
        "pulls": [{"number": 11, "merged_at": "2013-02-05T00:00:00Z",
                   "merge_commit_sha": "c" * 40}],
    }
    with pytest.raises(ForgeDiscussionError, match="pick one"):
        _run(GitHubSource(
            repo=REPO, fixture_path=_write(tmp_path, payload),
            commit_repo="bench-repo", link_merge_commits=False,
        ))


def test_unmerged_pull_with_test_merge_sha_emits_nothing(
    tmp_path: Path,
) -> None:
    """GitHub sets merge_commit_sha on UNMERGED PRs too. Only merged_at
    proves a real merge commit exists in history."""
    payload = {
        "items": [_issue(12, "2013-03-01T00:00:00Z", pr=True)],
        "pulls": [{"number": 12, "merged_at": None,
                   "merge_commit_sha": "d" * 40}],
    }
    facts = _run(GitHubSource(
        repo=REPO, commit_repo="bench-repo",
        fixture_path=_write(tmp_path, payload),
    ))
    assert not [e for e in facts
                if isinstance(e, EdgeFact) and e.edge_type == "merged_as"]


def test_merges_fetch_drops_post_cutoff_and_gold_with_counts(
    monkeypatch,
) -> None:
    pulls = [
        {"number": 10, "merged_at": "2013-01-05T00:00:00Z",
         "merge_commit_sha": "a" * 40},
        {"number": 11, "merged_at": "2015-01-05T00:00:00Z",     # post-cutoff
         "merge_commit_sha": "b" * 40},
        {"number": 12, "merged_at": None,                        # unmerged
         "merge_commit_sha": "c" * 40},
        {"number": GOLD_PR, "merged_at": "2014-10-31T00:00:00Z",  # gold
         "merge_commit_sha": "e" * 40},
        {"number": 4242, "merged_at": "2013-01-05T00:00:00Z",     # orphan
         "merge_commit_sha": "f" * 40},
    ]
    monkeypatch.setattr(corpus_mod, "_gh_json", lambda args: pulls)
    kept, counts = corpus_mod.fetch_forge_merges(
        repo=REPO, cutoff=CUTOFF, gold_pr_number=GOLD_PR,
        kept_numbers={10, 11, 12},
    )
    assert [row["number"] for row in kept] == [10]
    assert counts["dropped_merges_post_cutoff"] == 1
    assert counts["dropped_merges_unmerged"] == 1
    assert counts["dropped_merges_gold"] == 1
    assert counts["dropped_merges_orphan"] == 1


def test_no_merged_pulls_at_all_fails_the_build(monkeypatch) -> None:
    """A pulls stream with no merge record anywhere is a failed fetch."""
    monkeypatch.setattr(
        corpus_mod, "_gh_json",
        lambda args: [{"number": 10, "merged_at": None,
                       "merge_commit_sha": "a" * 40}],
    )
    with pytest.raises(CorpusBuildError, match="NO merged pull request"):
        corpus_mod.fetch_forge_merges(
            repo=REPO, cutoff=CUTOFF, gold_pr_number=GOLD_PR,
            kept_numbers={10},
        )


def test_merge_sha_absent_from_history_is_dropped_with_a_count(
    monkeypatch,
) -> None:
    """GitHub reports a TEST-MERGE sha for these PRs, which is not a commit
    in the repository. Dropping it here keeps a dangling edge out of the
    fixture instead of out of the publish."""
    monkeypatch.setattr(corpus_mod, "_gh_json", lambda args: [
        {"number": 10, "merged_at": "2013-01-05T00:00:00Z",
         "merge_commit_sha": "a" * 40},   # in history
        {"number": 11, "merged_at": "2013-02-05T00:00:00Z",
         "merge_commit_sha": "z" * 40},   # test-merge sha, not in history
    ])
    kept, counts = corpus_mod.fetch_forge_merges(
        repo=REPO, cutoff=CUTOFF, gold_pr_number=GOLD_PR,
        kept_numbers={10, 11}, known_shas={"a" * 40},
    )
    assert [row["number"] for row in kept] == [10]
    assert counts["dropped_merges_absent_from_history"] == 1
    assert counts["kept_merges"] == 1


def test_every_merge_sha_absent_does_not_crash_the_fetch(
    monkeypatch,
) -> None:
    """The measured psf/requests case: 714 merged PRs, 0 shas in history.
    The fetch reports it loudly in the counts and leaves the merged_as
    floor to be met by git's own merge-commit subjects — it does NOT
    fabricate an edge to keep a number up."""
    monkeypatch.setattr(corpus_mod, "_gh_json", lambda args: [
        {"number": 10, "merged_at": "2013-01-05T00:00:00Z",
         "merge_commit_sha": "z" * 40},
    ])
    kept, counts = corpus_mod.fetch_forge_merges(
        repo=REPO, cutoff=CUTOFF, gold_pr_number=GOLD_PR,
        kept_numbers={10}, known_shas={"a" * 40},
    )
    assert kept == []
    assert counts["dropped_merges_absent_from_history"] == 1
    assert counts["kept_merges"] == 0


# -- merge attribution from git's own merge-commit subjects -------------


def test_merge_attribution_resolves_pr_from_commit_subject(
    monkeypatch,
) -> None:
    nodes = [
        {"node_id": f"pr:github:{REPO}:#2306", "subtype": "pull_request",
         "attrs": {"number": 2306}},
        {"node_id": "commit:bench:091991be", "subtype": "git_commit",
         "attrs": {"message_subject": "Merge pull request #2306 from x/y"}},
        {"node_id": "commit:bench:deadbeef", "subtype": "git_commit",
         "attrs": {"message_subject": "Merge pull request #9999 from a/b"}},
        {"node_id": "commit:bench:cafe", "subtype": "git_commit",
         "attrs": {"message_subject": "Merge 583859b into be805f9"}},
    ]
    captured: dict = {}

    def fake_query(cur, sql, params=None):
        params = params or {}
        return [n for n in nodes if n["subtype"] == params.get("subtype")]

    def fake_insert(cur, *, source, candidates):
        captured["candidates"] = list(candidates)

        class R:
            inserted = len(captured["candidates"])
            skipped = 0
            sanitized_value_count = 0
        return R()

    from okg.substrate.library.linkers import forge_references as mod

    monkeypatch.setattr(mod._chronos, "query", fake_query)
    monkeypatch.setattr(mod, "insert_deterministic_edges", fake_insert)

    result = mod.ForgeMergeAttributionLinker().enrich(
        _FakeConn(), generation_id=1,
    )
    assert len(captured["candidates"]) == 1
    edge = captured["candidates"][0]
    assert edge.src == f"pr:github:{REPO}:#2306"
    assert edge.dst == "commit:bench:091991be"
    assert edge.edge_type == "merged_as"
    counts = result.output_lineage["merge_attribution_counts"]
    # GitHub's ephemeral test-merge subject ("Merge X into Y") is not an
    # attribution and must not be counted as one.
    assert counts["subjects_naming_a_pr"] == 2
    assert counts["resolved"] == 1
    assert counts["unresolved_out_of_corpus"] == 1
    assert result.output_lineage["unresolved_sample"] == [9999]


def test_merge_attribution_raises_when_nothing_resolves(monkeypatch) -> None:
    nodes = [
        {"node_id": "commit:bench:aaa", "subtype": "git_commit",
         "attrs": {"message_subject": "Merge pull request #7 from x/y"}},
    ]

    def fake_query(cur, sql, params=None):
        params = params or {}
        return [n for n in nodes if n["subtype"] == params.get("subtype")]

    from okg.substrate.library.linkers import forge_references as mod

    monkeypatch.setattr(mod._chronos, "query", fake_query)
    with pytest.raises(mod.ForgeReferencesUnresolved) as exc:
        mod.ForgeMergeAttributionLinker().enrich(_FakeConn(), generation_id=1)
    assert exc.value.data["subjects_naming_a_pr"] == 1
    assert exc.value.data["resolved"] == 0


def test_merge_attribution_declares_only_merged_as() -> None:
    from okg.substrate.library.linkers.forge_references import (
        ForgeMergeAttributionLinker,
    )

    linker = ForgeMergeAttributionLinker()
    assert linker.emits_edge_types == ("merged_as",)
    assert linker.requires_narrowings == (
        ("pull_request", "merged_as", "git_commit"),
    )


# -- req.repo.linkage: reference extraction ----------------------------


def _url_pattern(slug: str = REPO):
    return ForgeReferenceLinker(repo_slug=slug)._url_pattern


def test_extract_refs_hash_verb_and_url_forms() -> None:
    p = _url_pattern()
    assert extract_refs("see #123", url_pattern=p) == {123}
    assert extract_refs("fixes #123", url_pattern=p) == {123}
    assert extract_refs("Closes #7 and closes #8", url_pattern=p) == {7, 8}
    assert extract_refs("GH-99 backport", url_pattern=p) == {99}
    assert extract_refs(
        f"see https://github.com/{REPO}/issues/456", url_pattern=p,
    ) == {456}
    assert extract_refs(
        f"see https://github.com/{REPO}/pull/457", url_pattern=p,
    ) == {457}
    assert extract_refs(
        f"api https://api.github.com/repos/{REPO}/pulls/458", url_pattern=p,
    ) == {458}


def test_extract_refs_ignores_other_repos_and_non_refs() -> None:
    p = _url_pattern()
    assert extract_refs(
        "see https://github.com/other/proj/issues/456", url_pattern=p,
    ) == set()
    assert extract_refs("color #ffffff", url_pattern=p) == set()
    assert extract_refs("sha abc#12", url_pattern=p) == set()
    assert extract_refs("", url_pattern=p) == set()
    # Without a repo_slug, URL forms are not matched at all rather than
    # matched loosely across repositories.
    assert extract_refs(
        f"https://github.com/{REPO}/issues/456", url_pattern=None,
    ) == set()


def test_url_pattern_is_repo_scoped() -> None:
    p = _url_pattern("psf/requests")
    assert isinstance(p, re.Pattern)
    assert not p.search("https://github.com/psf/requests-x/issues/1")


def test_reference_linker_drops_out_of_corpus_targets(monkeypatch) -> None:
    """A reference to an excluded or post-cutoff item is dropped WITH A
    COUNT — never a dangling edge, never a stub that would smuggle a gold
    identifier into the graph."""
    nodes = [
        {"node_id": f"issue:github:{REPO}:#10", "subtype": "issue",
         "attrs": {"number": 10, "title": "t", "body": "root cause"}},
        {"node_id": f"pr:github:{REPO}:#11", "subtype": "pull_request",
         "attrs": {"number": 11, "title": "fix", "body": "fixes #10"}},
        {"node_id": "commit:bench:aaa", "subtype": "git_commit",
         "attrs": {"message_subject": f"merge #11, refs #{GOLD_PR} and #9999"}},
    ]
    captured: dict = {}

    def fake_query(cur, sql, params=None):
        params = params or {}
        if "subtypes" in params:
            want = set(params["subtypes"])
            return [n for n in nodes if n["subtype"] in want]
        return [n for n in nodes if n["subtype"] == params.get("subtype")]

    def fake_insert(cur, *, source, candidates):
        captured["candidates"] = list(candidates)

        class R:
            inserted = len(captured["candidates"])
            skipped = 0
            sanitized_value_count = 0
        return R()

    from okg.substrate.library.linkers import forge_references as mod

    monkeypatch.setattr(mod._chronos, "query", fake_query)
    monkeypatch.setattr(mod, "insert_deterministic_edges", fake_insert)

    linker = ForgeReferenceLinker(repo_slug=REPO)
    result = linker.enrich(_FakeConn(), generation_id=1)

    pairs = {(c.src, c.dst) for c in captured["candidates"]}
    assert (f"pr:github:{REPO}:#11", f"issue:github:{REPO}:#10") in pairs
    assert ("commit:bench:aaa", f"pr:github:{REPO}:#11") in pairs
    # The gold PR and the nonexistent #9999 resolve to nothing...
    assert not any(str(GOLD_PR) in dst for _src, dst in pairs)
    assert not any("9999" in dst for _src, dst in pairs)
    # ...and are COUNTED, not silently discarded.
    counts = result.output_lineage["reference_counts"]
    assert counts["unresolved_out_of_corpus"] == 2
    assert counts["resolved"] == 2
    assert set(result.output_lineage["unresolved_sample"]) == {GOLD_PR, 9999}
    # No candidate points at a node that is not live in the graph.
    live = {n["node_id"] for n in nodes}
    assert all(c.dst in live for c in captured["candidates"])


def test_reference_linker_raises_when_nothing_resolves(monkeypatch) -> None:
    """Refs present but none resolvable is a broken index, not an empty
    repository — "no matches" must not masquerade as "none exist"."""
    nodes = [
        {"node_id": "commit:bench:aaa", "subtype": "git_commit",
         "attrs": {"message_subject": "fixes #10 and #11"}},
    ]

    def fake_query(cur, sql, params=None):
        params = params or {}
        if "subtypes" in params:
            return []
        return [n for n in nodes if n["subtype"] == params.get("subtype")]

    from okg.substrate.library.linkers import forge_references as mod

    monkeypatch.setattr(mod._chronos, "query", fake_query)
    with pytest.raises(mod.ForgeReferencesUnresolved) as exc:
        ForgeReferenceLinker(repo_slug=REPO).enrich(_FakeConn(), generation_id=1)
    assert exc.value.data["refs_seen"] == 2
    assert exc.value.data["resolved"] == 0
    assert exc.value.data["numbered_targets"] == 0


def test_reference_linker_declares_its_narrowings() -> None:
    """An undeclared triple is refused at Publisher init; declaring the
    wrong ones is how a linker silently emits nothing."""
    triples = set(ForgeReferenceLinker().requires_narrowings)
    assert ("git_commit", "references", "issue") in triples
    assert ("pull_request", "references", "issue") in triples
    assert all(edge == "references" for _s, edge, _d in triples)


def test_record_set_covers_every_emitted_comment_and_edge(
    tmp_path: Path,
) -> None:
    """The verified-delta guard refuses a run whose record_set omits an
    emitted node. Comments are nodes, and merged_as points at a commit this
    source does not own — both have to land in the right record."""
    payload = {
        "items": [_issue(11, "2013-02-01T00:00:00Z", pr=True,
                         merged_at="2013-02-05T00:00:00Z")],
        "comments": [_comment(100, 11, "2013-02-02T00:00:00Z")],
        "pulls": [{"number": 11, "merged_at": "2013-02-05T00:00:00Z",
                   "merge_commit_sha": "c" * 40}],
    }
    run = GitHubSource(
        repo=REPO, commit_repo="bench-repo",
        fixture_path=_write(tmp_path, payload),
    ).run("run-1")
    facts = list(run.facts)
    emitted_nodes = {
        f.node_id for f in facts if isinstance(f, NodeFact)
    }
    covered = set(run.record_set.records)
    assert emitted_nodes <= covered, emitted_nodes - covered
    # The merged_as edge is filed under the PR (its only endpoint this
    # source owns), not under the foreign commit node.
    pr_record = run.record_set.records[f"pr:github:{REPO}:#11"]
    assert any(e.edge_type == "merged_as" for e in pr_record.edges)
    comment_record = run.record_set.records[f"comment:github:{REPO}:ic:100"]
    assert any(e.edge_type == "has_comment" for e in comment_record.edges)


class _FakeCursor:
    """The cursor half of `_FakeConn`. Declared at module scope rather
    than nested inside `cursor()` so its methods can just be named
    `self` -- nesting would shadow the connection's own `self`."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    """Minimal psycopg-shaped stand-in: enrich() only needs a cursor
    context manager, since every read goes through the patched
    `_chronos.query`."""

    def cursor(self):
        return _FakeCursor()
