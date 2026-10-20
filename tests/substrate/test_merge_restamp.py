"""Merge provenance re-stamp (worktree-fork-merge-back): merged rows are relabeled to a
synthetic merge producer that reads back untrusted, and fork rows with null/system/derived
sources are refused (they would launder a trusted/derived label onto default).

The cross-module tests pin the re-stamp against the REAL read-side classifier
(mcp/provenance._classify_one), proving (a) the merge producer id is untrusted and (b) the
exact sources we refuse are the ones that would otherwise classify trusted.
"""
from __future__ import annotations

import pytest

from chronos_core.branching import RowDiff

from okg.substrate.mcp.provenance import _SYSTEM_SOURCE_PREFIXES, _classify_one
from okg.substrate.merge_guard import MergeRefused
from okg.substrate.merge_restamp import (
    RESERVED_SOURCE_PREFIXES,
    assert_source_mergeable,
    merge_producer_id,
    restamp_diff,
    restamp_row,
)


def test_null_source_refused() -> None:
    for src in (None, "", "   "):
        with pytest.raises(MergeRefused) as ei:
            assert_source_mergeable(src)
        assert ei.value.data["reason"] == "null_source"


def test_reserved_system_source_refused() -> None:
    for src in (
        "_enricher:dedup", "_extractor:slug", "_lens:foo", "derived_artifact:x",
        "derived_deterministic", "extracted_model", "  _Enricher:Caps  ",
    ):
        with pytest.raises(MergeRefused) as ei:
            assert_source_mergeable(src)
        assert ei.value.data["reason"] == "reserved_system_source", src


def test_ordinary_and_already_merged_sources_allowed() -> None:
    for src in ("git", "openspec", "alice@example.edu", "merge:m1@gen123"):
        assert_source_mergeable(src)  # no raise


def test_merge_producer_id_format() -> None:
    assert merge_producer_id("m1", "gen123") == "merge:m1@gen123"


def test_restamp_row_rewrites_source_without_mutating_input() -> None:
    after = {"node_id": "x", "subtype": "person", "source": "git", "attrs": {"a": 1}}
    out = restamp_row(after, worktree="m1", fork_tip_gen="gen9")
    assert out["source"] == "merge:m1@gen9"
    assert out["node_id"] == "x" and out["attrs"] == {"a": 1}
    assert after["source"] == "git", "input must not be mutated"


def test_restamp_row_normalizes_all_provenance_and_strips_promote_intent() -> None:
    # SEC-2: restamp must normalize EVERY provenance field (not just top-level source) and
    # strip the server-only promote marker, so no fork-controlled identity/marker lands on
    # default. source_record_id/source_revision are real columns the dedup indexes key on.
    after = {
        "node_id": "x", "subtype": "person", "source": "git",
        "source_record_id": {"fork_secret": "leak"},
        "source_revision": {"v": 7},
        "attrs": {"a": 1, "promote_intent": "durable"},
    }
    out = restamp_row(after, worktree="m1", fork_tip_gen="gen9")
    assert out["source"] == "merge:m1@gen9"
    # no fork-controlled identity survives onto the landed row
    assert out["source_record_id"] == {"merged_from": "m1", "fork_tip_gen": "gen9"}
    assert out["source_revision"] == {}
    # the reserved server-only marker is stripped (the rest of attrs is preserved)
    assert "promote_intent" not in out["attrs"]
    assert out["attrs"] == {"a": 1}
    # input is not mutated (fork's values intact on the original)
    assert after["source_record_id"] == {"fork_secret": "leak"}
    assert after["attrs"]["promote_intent"] == "durable"


def test_restamp_row_refuses_reserved_source() -> None:
    with pytest.raises(MergeRefused) as ei:
        restamp_row({"node_id": "x", "source": "_enricher:dedup"}, worktree="m1", fork_tip_gen="g")
    assert ei.value.data["reason"] == "reserved_system_source"


def test_restamp_diff_rewrites_adds_skips_deletes_refuses_reserved() -> None:
    add = RowDiff("okg.graph_nodes", {"node_id": "a"}, "added", None,
                  {"node_id": "a", "source": "git"})
    deleted = RowDiff("okg.graph_nodes", {"node_id": "b"}, "deleted",
                      {"node_id": "b", "source": "git"}, None)
    out = restamp_diff([add, deleted], worktree="m1", fork_tip_gen="g1")
    assert out[0].after["source"] == "merge:m1@g1"
    assert out[1].after is None  # deletion left untouched (no source to restamp)

    reserved = RowDiff("okg.graph_nodes", {"node_id": "c"}, "added", None,
                       {"node_id": "c", "source": "derived_artifact:x"})
    with pytest.raises(MergeRefused):
        restamp_diff([add, reserved], worktree="m1", fork_tip_gen="g1")


# --------------------------------------------------------------------------- #
# Cross-module: the re-stamp target is untrusted; the refused sources are trusted
# --------------------------------------------------------------------------- #

def test_merge_producer_id_reads_back_untrusted() -> None:
    # No producer profile -> the no-profile fail-safe classifies the merge id untrusted,
    # so merged facts are quoted/fenced by default (no trust column written).
    assert _classify_one(merge_producer_id("m1", "gen9"), profile=None) == "untrusted"


def test_reserved_is_superset_of_system_prefixes() -> None:
    # The soundness invariant: every source the read-side classifier trusts BY PREFIX must
    # be refused at merge, else a fork could launder a trusted label. Enforced by
    # construction (RESERVED is derived from _SYSTEM_SOURCE_PREFIXES) -- pinned here so a
    # future drift fails loudly rather than silently reopening the launder hole.
    assert set(_SYSTEM_SOURCE_PREFIXES) <= set(RESERVED_SOURCE_PREFIXES)
    for prefix in _SYSTEM_SOURCE_PREFIXES:
        with pytest.raises(MergeRefused):
            assert_source_mergeable(prefix + "x")


def test_system_prefixes_read_trusted_and_null_too() -> None:
    # The launder case: each system prefix (and a null source) reads back TRUSTED, which is
    # exactly why merging one is refused.
    assert _classify_one(None, profile=None) == "trusted"
    for prefix in _SYSTEM_SOURCE_PREFIXES:
        assert _classify_one(prefix + "x", profile=None) == "trusted", prefix


def test_derived_prefixes_read_untrusted_but_still_refused() -> None:
    # derived_/extracted_ read UNtrusted (they lack the system-prefix shape) but are
    # substrate-derived: refused so default re-derives them, not the trust-launder reason.
    for src in ("derived_deterministic", "extracted_model"):
        assert _classify_one(src, profile=None) == "untrusted", src
        with pytest.raises(MergeRefused) as ei:
            assert_source_mergeable(src)
        assert ei.value.data["reason"] == "reserved_system_source"
