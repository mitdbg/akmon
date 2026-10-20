"""The `content_hash` poll probe is a SOUND replacement for `stat_tree` (#323).

The poll/enqueue producer suppresses a sync when its fingerprint is unchanged.
`stat_tree` hashes mtime+size, so a mtime-preserving in-place edit is a silent
false-unchanged at the poll layer (before the sound runner-gate probe runs).
`content_hash` hashes the watch-scope file BYTES, so that edit busts it.
"""
from __future__ import annotations

import os
from pathlib import Path

from okg.substrate.source_sync import SourceSyncCapabilities
from okg.substrate.source_sync_producers import _run_probe


def _content_caps(root: Path) -> SourceSyncCapabilities:
    return SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": ["**/*"],
            "exclude_globs": ["**/__pycache__/**"],
        },
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        # The registry contract accepts only producer-neutral selectors.
        # These tests call the historical raw helper explicitly below.
        "poll": {"probe": "always_changed", "interval_seconds": 300},
        "compaction": {"window_seconds": 60, "max_events": 100, "max_scope_keys": 100},
    })


def test_content_hash_poll_probe_churn_immune_edit_delete(tmp_path):
    (tmp_path / "a.txt").write_text("alpha")
    (tmp_path / "b.txt").write_text("beta")
    caps = _content_caps(tmp_path)
    t0 = _run_probe("content_hash", caps)

    # mtime bump alone -> unchanged (content-based, not stat).
    os.utime(tmp_path / "a.txt", (2_000_000_000, 2_000_000_000))
    assert _run_probe("content_hash", caps) == t0

    # edit busts.
    (tmp_path / "a.txt").write_text("alpha-edited")
    assert _run_probe("content_hash", caps) != t0
    t1 = _run_probe("content_hash", caps)

    # delete busts (membership).
    (tmp_path / "b.txt").unlink()
    assert _run_probe("content_hash", caps) != t1


def test_content_hash_catches_mtime_preserving_edit_that_stat_tree_misses(tmp_path):
    # The decisive soundness win: an in-place edit that PRESERVES size + mtime.
    f = tmp_path / "doc.txt"
    f.write_text("0123456789")  # 10 bytes
    st = f.stat()
    caps_content = _content_caps(tmp_path)
    caps_stat = SourceSyncCapabilities.from_dict({
        "watch": {"roots": [str(tmp_path)], "include_globs": ["**/*"], "exclude_globs": []},
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {"probe": "always_changed", "interval_seconds": 300},
        "compaction": {"window_seconds": 60, "max_events": 100, "max_scope_keys": 100},
    })
    content_before = _run_probe("content_hash", caps_content)
    stat_before = _run_probe("stat_tree", caps_stat)

    # Same length (10 bytes), then restore the exact mtime/atime.
    f.write_text("9876543210")
    os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))

    # stat_tree is BLIND to this (size + mtime unchanged) -- the false-unchanged.
    assert _run_probe("stat_tree", caps_stat) == stat_before
    # content_hash catches it (bytes changed) -- the contract requires this.
    assert _run_probe("content_hash", caps_content) != content_before
