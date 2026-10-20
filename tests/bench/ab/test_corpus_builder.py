"""req.abx.corpus: forge fixture filtering, registry wiring, gh parsing.

The full build-and-audit path against a real Postgres + real instance is the
postgres-marked integration in test_leakage_audit.py; these are the
no-database halves.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.bench.ab import corpus
from okg.bench.ab.errors import CorpusBuildError

CUTOFF = "2023-05-01T00:00:00Z"


def _issue(number: int, created_at: str, is_pr: bool = False) -> dict:
    item = {
        "number": number,
        "created_at": created_at,
        "updated_at": created_at,
        "title": f"item {number}",
        "state": "open",
        "user": {"login": "dev"},
    }
    if is_pr:
        item["pull_request"] = {"url": f"https://api.github.com/pr/{number}"}
    return item


def _endpoint_stub(*, items, comments=(), review_comments=(), pulls=()):
    """`fetch_forge_fixture` crawls four endpoints (items, issue comments,
    review comments, pulls). Dispatch on the request path so each stream
    gets its own rows — a single shared list would feed issue objects to
    the comment parser."""
    def _stub(args):
        path = args[-1]
        if "issues/comments" in path:
            return list(comments)
        if "pulls/comments" in path:
            return list(review_comments)
        if "/pulls?" in path:
            return list(pulls)
        return list(items)
    return _stub


def _pull(number: int, merged_at: str | None, sha: str = "a" * 40) -> dict:
    return {"number": number, "merged_at": merged_at, "merge_commit_sha": sha}


def _comment(cid: int, parent: int, created_at: str) -> dict:
    return {
        "id": cid,
        "issue_url": f"https://api.github.com/repos/acme/widgets/issues/{parent}",
        "user": {"login": "dev"},
        "body": "why",
        "created_at": created_at,
        "updated_at": created_at,
        "html_url": f"https://github.com/acme/widgets/issues/{parent}#c{cid}",
    }


def test_forge_fixture_filters_cutoff_and_gold(tmp_path, monkeypatch):
    items = [
        _issue(1, "2023-01-01T00:00:00Z"),
        _issue(2, "2023-04-30T23:59:59Z", is_pr=True),
        _issue(3, "2023-05-01T00:00:00Z"),          # exactly at cutoff: excluded
        _issue(4, "2023-06-01T00:00:00Z"),          # post-cutoff: excluded
        _issue(5, "2023-03-01T00:00:00Z", is_pr=True),  # the gold PR: excluded
    ]
    monkeypatch.setattr(corpus, "_gh_json", _endpoint_stub(
        items=items,
        comments=[_comment(100, 1, "2023-02-01T00:00:00Z")],
        pulls=[_pull(2, "2023-04-30T23:59:59Z")],
    ))
    out = tmp_path / "issues.json"
    counts = corpus.fetch_forge_fixture(
        repo="acme/widgets", cutoff=CUTOFF, gold_pr_number=5, out_path=out
    )
    payload = json.loads(out.read_text())
    assert [i["number"] for i in payload["items"]] == [1, 2]
    assert counts["kept"] == 2
    assert counts["dropped_post_cutoff"] == 2
    assert counts["dropped_gold"] == 1
    assert counts["scrubbed_post_cutoff_fields"] == 0
    # The discussion and merge streams ride the same filter.
    assert [c["id"] for c in payload["comments"]] == [100]
    assert [p["number"] for p in payload["pulls"]] == [2]
    assert counts["kept_comments"] == 1
    assert counts["kept_merges"] == 1


def test_scrub_restores_as_of_cutoff_state():
    item = {
        "number": 9,
        "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2024-06-01T00:00:00Z",   # post-cutoff edit stamp
        "closed_at": "2024-05-01T00:00:00Z",    # closed after the cutoff
        "state": "closed",
        "state_reason": "completed",
        "pull_request": {"merged_at": "2024-05-01T00:00:00Z"},
    }
    scrubbed, changed = corpus._scrub_post_cutoff_fields(item, CUTOFF)
    assert changed
    assert scrubbed["state"] == "open"
    assert scrubbed["closed_at"] is None
    assert "state_reason" not in scrubbed
    assert scrubbed["updated_at"] == "2023-01-01T00:00:00Z"
    assert scrubbed["pull_request"]["merged_at"] is None
    # The original is untouched and a pre-cutoff item passes through intact.
    assert item["state"] == "closed"
    clean = {"number": 1, "created_at": "2023-01-01T00:00:00Z",
             "updated_at": "2023-02-01T00:00:00Z",
             "closed_at": "2023-03-01T00:00:00Z", "state": "closed"}
    same, changed = corpus._scrub_post_cutoff_fields(clean, CUTOFF)
    assert not changed and same == clean


def test_forge_fixture_rejects_stampless_items(tmp_path, monkeypatch):
    monkeypatch.setattr(corpus, "_gh_json", lambda args: [{"number": 1}])
    with pytest.raises(CorpusBuildError, match="without created_at"):
        corpus.fetch_forge_fixture(
            repo="acme/widgets", cutoff=CUTOFF, gold_pr_number=None,
            out_path=tmp_path / "x.json",
        )


def test_parse_gh_paginated_merges_concatenated_arrays():
    text = '[{"a": 1}, {"a": 2}]\n[{"a": 3}]'
    assert corpus.parse_gh_paginated(text) == [{"a": 1}, {"a": 2}, {"a": 3}]
    with pytest.raises(CorpusBuildError, match="empty"):
        corpus.parse_gh_paginated("   ")


def _spec(tmp_path) -> corpus.CorpusSpec:
    return corpus.CorpusSpec(
        repo="acme/widgets",
        base_commit="a" * 40,
        cutoff=CUTOFF,
        gold_pr_number=1,
        deployment_name="bench-x",
        dsn="postgres://x/y",
        corpus_root=tmp_path / "corpus",
    )


def _scaffolded_registry(tmp_path) -> Path:
    """A registry in the scaffold's real shape: sources is a MAPPING, and it
    includes operator-environment sources that must be pruned."""
    deployment_dir = tmp_path / "dep"
    deployment_dir.mkdir()
    sources = {
        "git_history": {"ownership_id": "bench-x.git_history",
                        "predecessor_source": "bench-x.git_files"},
        "git_files": {"ownership_id": "bench-x.git_files"},
        "code_structure": {"ownership_id": "bench-x.code_structure",
                           "predecessor_source": "bench-x.git_files"},
        "package_manifests": {"ownership_id": "bench-x.package_manifests"},
        "codex_sessions": {"ownership_id": "bench-x.codex_sessions",
                           "params": {"codex_sessions_root": "~/.codex/sessions"}},
        "pact": {"ownership_id": "bench-x.pact"},
        "compute_env_local": {"ownership_id": "bench-x.compute_env_local"},
        "interfaces": {"ownership_id": "bench-x.interfaces",
                       "predecessor_source": "bench-x.code_structure"},
    }
    (deployment_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": sources}, sort_keys=False)
    )
    return deployment_dir


def test_prune_drops_operator_environment_sources(tmp_path):
    deployment_dir = _scaffolded_registry(tmp_path)
    pruned = corpus._prune_registry_to_corpus_sources(deployment_dir)
    assert pruned == {"codex_sessions", "pact", "compute_env_local",
                      "git_files", "interfaces"} - corpus.CORPUS_SOURCE_IDS
    registry = yaml.safe_load((deployment_dir / "source_registry.yaml").read_text())
    assert set(registry["sources"]) == set(corpus.CORPUS_SOURCE_IDS)


def test_prune_refuses_when_expected_sources_missing(tmp_path):
    deployment_dir = tmp_path / "dep"
    deployment_dir.mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": {"pact": {}}})
    )
    with pytest.raises(CorpusBuildError, match="missing expected corpus sources"):
        corpus._prune_registry_to_corpus_sources(deployment_dir)


def test_register_github_source_before_publish(tmp_path):
    deployment_dir = _scaffolded_registry(tmp_path)
    fixture = tmp_path / "issues.json"
    fixture.write_text("[]")
    spec = _spec(tmp_path)
    corpus._register_github_source(deployment_dir, spec, fixture)
    registry = yaml.safe_load((deployment_dir / "source_registry.yaml").read_text())
    gh = registry["sources"][corpus.GITHUB_SOURCE_ID]
    assert gh["params"]["fixture_path"] == str(fixture)
    assert gh["params"]["repo"] == "acme/widgets"
    assert gh["admission_policy"]["producer_id"] == "bench-x.github-issues"
    # Double registration is a bug, not an upsert.
    with pytest.raises(CorpusBuildError, match="already has"):
        corpus._register_github_source(deployment_dir, spec, fixture)


def test_sessions_source_registered_at_build_time(tmp_path):
    deployment_dir = _scaffolded_registry(tmp_path)
    spec = _spec(tmp_path)
    corpus._register_sessions_source(
        deployment_dir, spec, tmp_path / "checkout", tmp_path / "sessions"
    )
    registry = yaml.safe_load((deployment_dir / "source_registry.yaml").read_text())
    src = registry["sources"][corpus.SESSIONS_SOURCE_ID]
    assert src["params"]["claude_projects_root"] == str(tmp_path / "sessions")
    assert src["source_class"] == "discovery_crawl"
    # cwd basename != repo param orphans file stubs outside the record_set;
    # the universal cwd_to_repo mapping is load-bearing.
    assert src["params"]["cwd_to_repo"] == {"/": "bench-x"}


def test_sync_run_sessions_copies_without_registry_drift(tmp_path):
    """Between-task session flow copies transcripts into the REGISTERED
    sessions root; the registry file is never touched after catalog apply
    (editing it trips the catalog identity guard)."""
    dest = tmp_path / "corpus-sessions"
    run_projects = tmp_path / "run-projects" / "-workdir-a"
    run_projects.mkdir(parents=True)
    (run_projects / "session1.jsonl").write_text('{"type":"x"}\n')
    cfg = {"sessions_root": str(dest)}
    corpus._sync_run_sessions(cfg, tmp_path / "run-projects")
    assert (dest / "-workdir-a" / "session1.jsonl").exists()
    # Second sync with a new session is additive.
    (run_projects / "session2.jsonl").write_text('{"type":"y"}\n')
    corpus._sync_run_sessions(cfg, tmp_path / "run-projects")
    assert (dest / "-workdir-a" / "session2.jsonl").exists()


def test_sync_run_sessions_refuses_missing_inputs(tmp_path):
    with pytest.raises(CorpusBuildError, match="no sessions_root"):
        corpus._sync_run_sessions({}, tmp_path / "p")
    with pytest.raises(CorpusBuildError, match="left no transcripts"):
        corpus._sync_run_sessions(
            {"sessions_root": str(tmp_path / "d")}, tmp_path / "missing"
        )


def test_registry_without_sources_block_fails_loudly(tmp_path):
    deployment_dir = tmp_path / "dep"
    deployment_dir.mkdir()
    (deployment_dir / "source_registry.yaml").write_text(yaml.safe_dump({"x": 1}))
    with pytest.raises(CorpusBuildError, match="no sources mapping"):
        corpus._register_github_source(
            deployment_dir, _spec(tmp_path), tmp_path / "f.json"
        )


def test_published_generation_contract():
    native = "okg:bench-x:branch:default:gen:20260727T000000Z:abcdef012345"
    assert (
        corpus._published_generation(
            {"status": "published", "generation_id": native,
             "compat_generation_id": 41}
        )
        == native
    )
    assert corpus._published_generation(
        {"status": "published", "compat_generation_id": 7}
    ) == "7"
    with pytest.raises(CorpusBuildError, match="did not publish"):
        corpus._published_generation({"status": "blocked", "generation_id": 9})
    with pytest.raises(CorpusBuildError, match="did not publish"):
        corpus._published_generation({"status": "fail", "code": "x", "error": "boom"})
    with pytest.raises(CorpusBuildError, match="no generation id"):
        corpus._published_generation({"status": "published"})


def test_immutable_corpus_root(tmp_path, monkeypatch):
    spec = corpus.CorpusSpec(
        repo="acme/widgets",
        base_commit="a" * 40,
        cutoff=CUTOFF,
        gold_pr_number=1,
        deployment_name="bench-x",
        dsn="postgres://x/y",
        corpus_root=tmp_path / "corpus",
    )
    (tmp_path / "corpus" / "checkout").mkdir(parents=True)
    with pytest.raises(CorpusBuildError, match="immutable"):
        corpus.build(spec, data_root=tmp_path / "data")
