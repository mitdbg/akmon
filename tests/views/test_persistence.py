"""Unit tests for okg.views.persistence — Stage 1.3.

Covers:
  - JSON round-trip preserves View / Fragment exactly.
  - Atomic write: a crash mid-write leaves the prior manifest
    intact (umbrella's "Disk manifest persistence is atomic"
    requirement, mirrored implementation-side).
  - Manifest version is enforced on load.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from okg.views.persistence import (
    ASSEMBLED_FILENAME,
    FRAGMENT_OFFSETS_FILENAME,
    MANIFEST_FILENAME,
    MARKERED_FILENAME,
    SUPPORTED_MANIFEST_VERSION,
    load,
    save,
    view_from_dict,
    view_to_dict,
)
from okg.views.types import Anchor, Fragment, View


def _make_view(generation_id: int | None = 42) -> View:
    """Build a View with two fragments — one quote, one generated."""
    a = Anchor.from_content(id="pub:1:catalog", content="status: current")
    b = Anchor.from_content(id="pub:1:body:purpose", content="purpose text")
    return View(
        view_id="af-a3-brief",
        view_def_hash="def-hash-abc",
        manifest_version=1,
        generation_id=generation_id,
        fragments=[
            Fragment(
                id="row:pub:1",
                kind="quote",
                sources=("pub:1:catalog",),
                source_hashes={"pub:1:catalog": a.content_hash},
                content="status: current",
                display_label="Row for pub 1",
            ),
            Fragment(
                id="purpose:pub:1",
                kind="quote",
                sources=("pub:1:body:purpose",),
                source_hashes={"pub:1:body:purpose": b.content_hash},
                content="purpose text",
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Dict round-trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_view_to_dict_then_from_dict_is_lossless(self):
        v = _make_view(generation_id=42)
        d = view_to_dict(v)
        v2 = view_from_dict(d)
        assert v2.view_id == v.view_id
        assert v2.view_def_hash == v.view_def_hash
        assert v2.manifest_version == v.manifest_version
        assert v2.generation_id == v.generation_id
        assert len(v2.fragments) == len(v.fragments)
        for f, f2 in zip(v.fragments, v2.fragments, strict=True):
            assert f2.id == f.id
            assert f2.kind == f.kind
            assert f2.sources == f.sources
            assert f2.source_hashes == f.source_hashes
            assert f2.content == f.content
            assert f2.display_label == f.display_label

    def test_json_roundtrip_via_save_load(self, tmp_path):
        v = _make_view(generation_id=42)
        save(v, tmp_path, assembled="# assembled")
        v2 = load(tmp_path)
        assert v2.view_id == v.view_id
        assert v2.generation_id == v.generation_id
        assert len(v2.fragments) == 2

    def test_save_writes_both_manifest_and_view_md(self, tmp_path):
        v = _make_view()
        save(v, tmp_path, assembled="# the rendered body")
        assert (tmp_path / MANIFEST_FILENAME).exists()
        assert (tmp_path / ASSEMBLED_FILENAME).exists()
        assert (tmp_path / ASSEMBLED_FILENAME).read_text() == "# the rendered body"

    def test_save_creates_parent_directory(self, tmp_path):
        # save() should create its target directory if missing —
        # otherwise callers have to mkdir defensively before each call.
        target = tmp_path / "deep" / "nested" / "out"
        v = _make_view()
        save(v, target, assembled="# hi")
        assert target.is_dir()
        assert (target / MANIFEST_FILENAME).exists()

    def test_generation_id_none_roundtrips(self, tmp_path):
        # Snapshot-JSON test fixtures leave generation_id=None.
        v = _make_view(generation_id=None)
        save(v, tmp_path, assembled="# hi")
        v2 = load(tmp_path)
        assert v2.generation_id is None


# ---------------------------------------------------------------------------
# Manifest version
# ---------------------------------------------------------------------------


class TestManifestVersion:
    def test_save_records_supported_version(self, tmp_path):
        v = _make_view()
        save(v, tmp_path, assembled="# hi")
        data = json.loads((tmp_path / MANIFEST_FILENAME).read_text())
        assert data["manifest_version"] == SUPPORTED_MANIFEST_VERSION

    def test_load_rejects_unknown_manifest_version(self, tmp_path):
        # Hand-craft a manifest with a future version.
        future = {
            "manifest_version": 99,
            "view_id": "x",
            "view_def_hash": "abc",
            "generation_id": None,
            "fragments": [],
        }
        (tmp_path / MANIFEST_FILENAME).write_text(json.dumps(future))
        with pytest.raises(ValueError, match=r"manifest_version 99"):
            load(tmp_path)


# ---------------------------------------------------------------------------
# Atomic write
# ---------------------------------------------------------------------------


class TestAtomicity:
    def test_crash_during_write_leaves_prior_manifest_intact(self, tmp_path):
        """Simulate a mid-write crash: the prior manifest must stay
        valid and loadable after the failure.
        """
        # Stage A: write a valid manifest.
        v_old = _make_view(generation_id=10)
        save(v_old, tmp_path, assembled="# old")
        old_manifest_text = (tmp_path / MANIFEST_FILENAME).read_text()

        # Stage B: attempt a second write that crashes mid-write.
        # We patch os.replace to raise ONLY on the manifest's rename
        # so the temp file is created but never atomically swapped in.
        v_new = _make_view(generation_id=11)
        manifest_target = str(tmp_path / MANIFEST_FILENAME)
        original_replace = __import__("os").replace

        def fail_on_manifest_replace(src, dst):
            if str(dst) == manifest_target:
                raise OSError("simulated crash mid-rename")
            return original_replace(src, dst)

        with patch("okg.views.persistence.os.replace",
                   side_effect=fail_on_manifest_replace):
            with pytest.raises(OSError, match="simulated crash"):
                save(v_new, tmp_path, assembled="# new")

        # Stage C: the on-disk manifest is still the old one,
        # byte-identical.
        survivor_text = (tmp_path / MANIFEST_FILENAME).read_text()
        assert survivor_text == old_manifest_text

        # And it still loads cleanly.
        v_loaded = load(tmp_path)
        assert v_loaded.generation_id == 10

    def test_save_with_markered_writes_extra_artifacts(self, tmp_path):
        """save() with markered + fragment_offsets writes the
        portal-side artifacts alongside view.md.
        """
        v = _make_view()
        offsets = [
            {"fragment_id": "row:pub:1", "start": 0, "length": 5},
            {"fragment_id": "purpose:pub:1", "start": 7, "length": 12},
        ]
        save(
            v, tmp_path,
            assembled="hello\n\npurpose text",
            markered=(
                "<!--frag:row:pub:1 start-->hello"
                "<!--frag:row:pub:1 end-->\n\n"
                "<!--frag:purpose:pub:1 start-->purpose text"
                "<!--frag:purpose:pub:1 end-->"
            ),
            fragment_offsets=offsets,
        )
        assert (tmp_path / MARKERED_FILENAME).exists()
        assert (tmp_path / FRAGMENT_OFFSETS_FILENAME).exists()
        loaded = json.loads(
            (tmp_path / FRAGMENT_OFFSETS_FILENAME).read_text(),
        )
        assert loaded == offsets

    def test_save_drops_stale_markered_when_assembler_returns_str(
        self, tmp_path,
    ):
        """A view that switched from markered-aware to plain-str
        assembler clears the previous markered artifacts.
        """
        v = _make_view()
        # Cycle 1: markered.
        save(
            v, tmp_path,
            assembled="hello",
            markered="<!--frag:x start-->hello<!--frag:x end-->",
            fragment_offsets=[
                {"fragment_id": "x", "start": 0, "length": 5},
            ],
        )
        assert (tmp_path / MARKERED_FILENAME).exists()
        assert (tmp_path / FRAGMENT_OFFSETS_FILENAME).exists()
        # Cycle 2: same view, plain-str assembler return.
        save(v, tmp_path, assembled="hello updated")
        assert not (tmp_path / MARKERED_FILENAME).exists()
        assert not (tmp_path / FRAGMENT_OFFSETS_FILENAME).exists()
        # And view.md is the new content.
        assert (tmp_path / ASSEMBLED_FILENAME).read_text() == "hello updated"

    def test_save_rejects_partial_markered_pair(self, tmp_path):
        """Passing markered without fragment_offsets (or vice
        versa) is a programmer error — the artifacts are paired.
        """
        v = _make_view()
        with pytest.raises(ValueError, match="must be passed together"):
            save(v, tmp_path, assembled="x", markered="y")
        with pytest.raises(ValueError, match="must be passed together"):
            save(
                v, tmp_path,
                assembled="x",
                fragment_offsets=[
                    {"fragment_id": "f", "start": 0, "length": 1},
                ],
            )

    def test_temp_file_cleaned_up_on_failure(self, tmp_path):
        """A failed write must not leak temp files."""
        v = _make_view()
        manifest_target = str(tmp_path / MANIFEST_FILENAME)

        def always_fail(src, dst):
            if str(dst) == manifest_target:
                raise OSError("boom")
            return __import__("os").replace(src, dst)

        with patch("okg.views.persistence.os.replace",
                   side_effect=always_fail):
            with pytest.raises(OSError):
                save(v, tmp_path, assembled="# hi")

        # No temp files should remain in the directory.
        leftover_temps = [p for p in tmp_path.iterdir()
                          if p.name.startswith(".") and p.name.endswith(".tmp")]
        assert leftover_temps == []
