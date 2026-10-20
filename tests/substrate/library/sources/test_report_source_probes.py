"""Report/log sources resolve to a delete-detecting ContentHashProbe (#323).

self_play, agent_memory, instrumentation_log, junit_xml, and test_reports all
read non-git artifact drops, so the content-hash floor is the sound probe. Each
re-uses the source's OWN scope enumeration, so the probe scope equals the read
scope. DB-free: only the probe wiring + soundness properties.
"""
from __future__ import annotations

import os
import subprocess

from okg.substrate.library.sources.change_probe import tokens_equal
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.library.sources.agent_memory import AgentMemorySource
from okg.substrate.library.sources.instrumentation_log import (
    InstrumentationLogSource,
)
from okg.substrate.library.sources.junit_xml import JUnitXmlSource
from okg.substrate.library.sources.self_play import SelfPlayReportSource
from okg.substrate.library.sources.test_reports import TestReportSource


def _git_init(path) -> str:
    subprocess.run(["git", "-C", str(path), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(path), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(path), "config", "user.name", "t"], check=True)
    subprocess.run(["git", "-C", str(path), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(path), "commit", "-q", "-m", "i"], check=True)
    out = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=True,
    )
    return out.stdout.strip()


def test_self_play_content_hash_probe_churn_immune_and_delete(tmp_path):
    (tmp_path / "r1.json").write_text('{"self_play_run": "a"}')
    (tmp_path / "r2.json").write_text('{"self_play_run": "b"}')
    assert SelfPlayReportSource.change_probe_kind == "content_hash"
    s = SelfPlayReportSource(repo="demo", repo_root=str(tmp_path), report_globs=["*.json"])
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    os.utime(tmp_path / "r1.json", (2_000_000_000, 2_000_000_000))
    assert tokens_equal(t0, s.change_probe.build_token())  # mtime churn-immune
    os.remove(tmp_path / "r2.json")
    assert not tokens_equal(t0, s.change_probe.build_token())  # delete detected


def test_agent_memory_content_hash_probe_detects_edit_and_delete(tmp_path):
    (tmp_path / "p1.json").write_text('{"k": 1}')
    (tmp_path / "p2.jsonl").write_text('{"k": 2}\n')
    assert AgentMemorySource.change_probe_kind == "content_hash"
    s = AgentMemorySource(repo="demo", repo_root=str(tmp_path), packet_globs=["*.json", "*.jsonl"])
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    (tmp_path / "p1.json").write_text('{"k": 99}')
    assert not tokens_equal(t0, s.change_probe.build_token())  # edit detected
    t1 = s.change_probe.build_token()
    os.remove(tmp_path / "p2.jsonl")
    assert not tokens_equal(t1, s.change_probe.build_token())  # delete detected


def test_instrumentation_log_probe_scoped_to_register_file(tmp_path):
    reg = tmp_path / "register.yaml"
    reg.write_text("source: s\nrows: []\n")
    assert InstrumentationLogSource.change_probe_kind == "content_hash"
    s = InstrumentationLogSource(deployment="dep", repo="demo", register_path=str(reg))
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    # A sibling file NOT in scope must not bust the token.
    (tmp_path / "noise.yaml").write_text("ignored")
    assert tokens_equal(t0, s.change_probe.build_token())
    reg.write_text("source: s\nrows: [{}]\n")
    assert not tokens_equal(t0, s.change_probe.build_token())  # edit detected


def test_junit_xml_probe_folds_in_head_sha_when_git_derived(tmp_path):
    drop = tmp_path / "drop"
    drop.mkdir()
    (drop / "a.xml").write_text("<testsuite/>")
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "x.py").write_text("print(1)\n")
    _git_init(repo)
    assert JUnitXmlSource.change_probe_kind == "content_hash"
    s = JUnitXmlSource(drop_dir=str(drop), repo_root=str(repo), repo="okg")
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    # Same artifacts, but a new HEAD re-scopes emitted node ids -> token busts.
    (repo / "y.py").write_text("print(2)\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "j"], check=True)
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_junit_xml_pinned_sha_ignores_head_move(tmp_path):
    drop = tmp_path / "drop"
    drop.mkdir()
    (drop / "a.xml").write_text("<testsuite/>")
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "x.py").write_text("print(1)\n")
    _git_init(repo)
    # sha pinned -> HEAD is irrelevant to emission, so it must not bust the token.
    s = JUnitXmlSource(drop_dir=str(drop), repo_root=str(repo), repo="okg", sha="deadbeef")
    t0 = s.change_probe.build_token()
    (repo / "y.py").write_text("print(2)\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "j"], check=True)
    assert tokens_equal(t0, s.change_probe.build_token())


def test_test_reports_content_hash_probe_detects_delete(tmp_path):
    (tmp_path / "j1.xml").write_text("<testsuite/>")
    (tmp_path / "j2.xml").write_text("<testsuite name='b'/>")
    assert TestReportSource.change_probe_kind == "content_hash"
    s = TestReportSource(
        repo="okg", repo_root=str(tmp_path), artifact_globs=["*.xml"], infer_sha=False,
    )
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    os.remove(tmp_path / "j2.xml")
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_test_reports_head_sha_folded_in_when_infer_sha(tmp_path):
    (tmp_path / "j1.xml").write_text("<testsuite/>")
    _git_init(tmp_path)
    s = TestReportSource(
        repo="okg", repo_root=str(tmp_path), artifact_globs=["*.xml"], infer_sha=True,
    )
    t0 = s.change_probe.build_token()
    (tmp_path / "z.txt").write_text("z\n")
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "commit", "-q", "-m", "k"], check=True)
    # HEAD moved; infer_sha re-scopes emitted node ids -> token busts.
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_test_reports_reachable_commit_probe_tracks_git_history_refs(tmp_path):
    (tmp_path / "j1.xml").write_text("<testsuite/>")
    reachable = _git_init(tmp_path)
    default_branch = subprocess.run(
        ["git", "-C", str(tmp_path), "branch", "--show-current"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        ["git", "-C", str(tmp_path), "checkout", "-q", "--detach"],
        check=True,
    )
    (tmp_path / "orphan.txt").write_text("orphan\n")
    subprocess.run(["git", "-C", str(tmp_path), "add", "orphan.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "commit", "-q", "-m", "orphan"],
        check=True,
    )
    orphan = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        ["git", "-C", str(tmp_path), "checkout", "-q", default_branch],
        check=True,
    )
    source = TestReportSource(
        repo="okg",
        repo_root=str(tmp_path),
        artifact_globs=["*.xml"],
        infer_sha=False,
        sha_from_path=True,
        require_reachable_commit_endpoints=True,
    )
    token_without_ref = source.change_probe.build_token()
    assert source._commit_endpoint_available(reachable) is True
    assert source._commit_endpoint_available(orphan) is False

    subprocess.run(
        ["git", "-C", str(tmp_path), "branch", "ci-artifact", orphan],
        check=True,
    )
    assert not tokens_equal(token_without_ref, source.change_probe.build_token())
    assert source._commit_endpoint_available(orphan) is True
