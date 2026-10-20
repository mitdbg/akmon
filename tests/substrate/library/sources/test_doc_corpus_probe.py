"""DocCorpusSource change-probe (#323): the O(content) floor over the walk.

doc_corpus reads its candidate files (git-tracked by default since issue
#2357; the filesystem walk these fixtures opt into with `non_git_walk`) plus
the deployment's extractors.yaml, so it falls to the content-hash floor via
its OWN _walk_files() enumeration. The cross-source
`derived_from source_file` predecessor dependency is caught by reconcile, not
this cursor-mode probe.
"""
from __future__ import annotations

import inspect
import os
from pathlib import Path

from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    run_probe,
    tokens_equal,
    validate_token_composition,
)
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.source_sync_microbench import (
    FULL_PIPELINE_ALL_SOURCE_QUARANTINE,
    FULL_PIPELINE_SCENARIO,
    _quarantine_full_pipeline_all_source,
)
from okg.substrate.source_sync_producers import run_poll_once
from okg.substrate.sources.registry import load_source_registry


def _source(tmp_path):
    ex = tmp_path / "extractors_dir"
    ex.mkdir()
    (ex / "extractors.yaml").write_text("parser: []\n")
    return DocCorpusSource(
        repo="okg", repo_root=str(tmp_path), non_git_walk=True, extractors_dir=str(ex),
        include_globs=("**/*.md",),
    )


def test_declares_content_hash_probe(tmp_path):
    (tmp_path / "a.md").write_text("# a\n")
    assert DocCorpusSource.change_probe_kind == "content_hash"
    s = _source(tmp_path)
    assert isinstance(s.change_probe, ContentHashProbe)
    validate_token_composition(s.change_probe.build_token())


def test_mtime_churn_immune_and_doc_edit_detected(tmp_path):
    (tmp_path / "a.md").write_text("# a\n")
    s = _source(tmp_path)
    t0 = s.change_probe.build_token()
    os.utime(tmp_path / "a.md", (2_000_000_000, 2_000_000_000))
    assert tokens_equal(t0, s.change_probe.build_token())  # churn-immune
    (tmp_path / "a.md").write_text("# a edited\n")
    assert not tokens_equal(t0, s.change_probe.build_token())  # edit detected


def test_doc_delete_detected(tmp_path):
    (tmp_path / "a.md").write_text("# a\n")
    (tmp_path / "b.md").write_text("# b\n")
    s = _source(tmp_path)
    t0 = s.change_probe.build_token()
    (tmp_path / "b.md").unlink()
    assert not tokens_equal(t0, s.change_probe.build_token())  # membership drop


def test_extractors_yaml_edit_busts_token(tmp_path):
    (tmp_path / "a.md").write_text("# a\n")
    s = _source(tmp_path)
    t0 = s.change_probe.build_token()
    # An extractor-config edit changes parser/chunker/NER selection -> emission.
    (s.extractors_dir / "extractors.yaml").write_text("parser: []\nchunker: {}\n")
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_out_of_scope_file_is_ignored(tmp_path):
    (tmp_path / "a.md").write_text("# a\n")
    s = _source(tmp_path)
    t0 = s.change_probe.build_token()
    # include_globs is **/*.md, so a .py file is outside scope and must not bust.
    (tmp_path / "ignore.py").write_text("print(1)\n")
    assert tokens_equal(t0, s.change_probe.build_token())


# --- Poll enrolment: deferred (#1325 / #1759) --------------------------------
# DocCorpusSource declares its content-hash probe on the class, which is what
# deployment lint checks, but okg-workspace does NOT enrol doc-corpus in poll.
# Enrolling it would also subject the heaviest source in the deployment to the
# poll backstop full reconcile: `_poll_backstop_reconcile_due` declares a
# reconcile due once `last_reconcile_at` is older than
# interval x backstop_multiplier, and the producer then enqueues
# `run_mode=scope_complete` with closure dependent fanout -- at a 300 s
# interval and the producer-level default multiplier of 5, that is a full
# reconcile every <=25 minutes. `backstop_multiplier` is a single
# `run_poll_once` argument with no per-source override, so the enrolment
# cannot be taken without the reconcile. Deferred to #1759, which is to add a
# per-source backstop control. Until then these pin the absence, so a future
# enrolment has to change this test and state its reasoning.


def test_okg_workspace_defers_doc_corpus_poll_enrolment():
    registry_path = (
        Path(__file__).resolve().parents[4]
        / "deployments/okg-workspace/source_registry.yaml"
    )
    sync = load_source_registry(registry_path)["doc-corpus"].sync
    assert "poll" not in sync.triggers
    assert sync.poll is None
    # The reason the enrolment is deferred: the backstop reconcile rides along
    # with it, and its multiplier is a producer-level `run_poll_once` argument,
    # not a per-source setting. (The private helpers behind it are not imported
    # here: the source-sync boundary test allowlists facade imports.)
    assert "backstop_multiplier" in inspect.signature(run_poll_once).parameters


def test_second_poll_over_unchanged_corpus_is_a_probe_skip(tmp_path):
    (tmp_path / "a.md").write_text("# a\n")
    s = _source(tmp_path)
    first = run_probe(s.change_probe, cursor=None, sync_scope=None)
    assert first.changed is True  # no prior token -> full read
    assert first.reason == "no_prior_token"
    cursor = {CURSOR_TOKEN_KEY: first.token}
    second = run_probe(s.change_probe, cursor=cursor, sync_scope=None)
    assert second.changed is False  # unchanged corpus -> adapter skipped
    (tmp_path / "a.md").write_text("# a edited\n")
    third = run_probe(s.change_probe, cursor=cursor, sync_scope=None)
    assert third.changed is True


def test_doc_corpus_is_not_quarantined_from_full_pipeline_microbench():
    assert "doc-corpus" not in FULL_PIPELINE_ALL_SOURCE_QUARANTINE
    assert not _quarantine_full_pipeline_all_source(
        source_name="doc-corpus",
        pipeline_mode=FULL_PIPELINE_SCENARIO,
        all_source_selector=True,
        allow_quarantined_full_pipeline=False,
    )
