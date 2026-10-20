"""M3.5 — retraction-safety policy."""
from __future__ import annotations

import pytest

from okg.substrate.errors import RetractionNotPermitted
from okg.substrate.ingest import SourceRunner
from okg.substrate.policy import (
    RunMode,
    assert_retraction_permitted,
    retraction_permitted,
)


# ---------- mutable_api ----------

def test_mutable_api_reconcile_permits() -> None:
    ok, reason = retraction_permitted(
        source_class="mutable_api", run_mode=RunMode.RECONCILE,
    )
    assert ok
    assert reason is None


def test_mutable_api_cursor_refuses() -> None:
    ok, reason = retraction_permitted(
        source_class="mutable_api", run_mode=RunMode.CURSOR,
    )
    assert not ok
    assert "cursor" in reason


def test_mutable_api_assert_raises_in_cursor_mode() -> None:
    with pytest.raises(RetractionNotPermitted) as exc:
        assert_retraction_permitted(
            source_class="mutable_api",
            run_mode=RunMode.CURSOR,
            source_name="example.issues",
        )
    assert exc.value.code == "retraction_not_permitted"
    assert "example.issues" in str(exc.value.data) or \
        exc.value.data.get("source_name") == "example.issues"


# ---------- discovery_crawl ----------

def test_discovery_crawl_complete_permits() -> None:
    ok, _ = retraction_permitted(
        source_class="discovery_crawl",
        run_mode=RunMode.SCOPE_COMPLETE,
    )
    assert ok


def test_discovery_crawl_incomplete_refuses() -> None:
    ok, reason = retraction_permitted(
        source_class="discovery_crawl",
        run_mode=RunMode.SCOPE_INCOMPLETE,
    )
    assert not ok
    assert "incomplete" in reason


# ---------- reference_catalog ----------

def test_reference_catalog_new_release_permits() -> None:
    ok, _ = retraction_permitted(
        source_class="reference_catalog",
        run_mode=RunMode.RELEASE_NEW,
    )
    assert ok


def test_reference_catalog_unchanged_release_refuses() -> None:
    ok, reason = retraction_permitted(
        source_class="reference_catalog",
        run_mode=RunMode.RELEASE_UNCHANGED,
    )
    assert not ok
    assert "release" in reason


# ---------- append_feed (never permits) ----------

@pytest.mark.parametrize("mode", [
    RunMode.APPEND, RunMode.RECONCILE, RunMode.CURSOR,
    RunMode.SCOPE_COMPLETE, RunMode.RELEASE_NEW,
])
def test_append_feed_never_permits_retraction(mode: RunMode) -> None:
    ok, reason = retraction_permitted(
        source_class="append_feed", run_mode=mode,
    )
    assert not ok
    assert "append" in reason or "append_feed" in reason


# ---------- live_overlay (structurally refused) ----------

def test_live_overlay_structurally_refused() -> None:
    ok, reason = retraction_permitted(
        source_class="live_overlay", run_mode=RunMode.RECONCILE,
    )
    assert not ok
    assert "overlay" in reason


# ---------- registry_only ----------

def test_registry_only_refused() -> None:
    ok, reason = retraction_permitted(
        source_class="registry_only", run_mode=RunMode.RECONCILE,
    )
    assert not ok
    assert "no runnable" in reason or "registry_only" in reason


def test_registered_runner_uses_frozen_profile_not_mutable_instance() -> None:
    class Adapter:
        profile = "mutable_api"

    adapter = Adapter()
    adapter.profile = "append_feed"
    runner = SourceRunner(
        "postgres://unused",
        source_name="deployment.source",
        executable_profile="discovery_crawl",
        registry_name="source",
        ownership_id="deployment.source",
        registry_binding_fingerprint="binding-v1",
    )

    assert runner._policy_profile(adapter) == "discovery_crawl"


def test_unmarked_runner_preserves_instance_profile_lookup() -> None:
    class LegacyAdapter:
        def __init__(self) -> None:
            self.profile = "append_feed"

    runner = SourceRunner(
        "postgres://unused",
        source_name="deployment.legacy-source",
    )

    assert runner._policy_profile(LegacyAdapter()) == "append_feed"


# ---------- mode/profile mismatch ----------

def test_mutable_api_with_release_mode_refused() -> None:
    """A mode that doesn't belong to the profile is refused with a
    clear message — not silently treated as 'safe'."""
    ok, reason = retraction_permitted(
        source_class="mutable_api", run_mode=RunMode.RELEASE_NEW,
    )
    assert not ok
    assert "mutable_api" in reason


def test_unknown_source_class_refused() -> None:
    ok, reason = retraction_permitted(
        source_class="something_made_up", run_mode=RunMode.RECONCILE,
    )
    assert not ok
    assert "unknown source_class" in reason


# ---------- error data carries the runbook payload ----------

def test_retraction_error_carries_data() -> None:
    with pytest.raises(RetractionNotPermitted) as exc:
        assert_retraction_permitted(
            source_class="append_feed",
            run_mode=RunMode.APPEND,
            source_name="intersystems.openfda_faers",
        )
    err = exc.value
    assert err.data["source_class"] == "append_feed"
    assert err.data["run_mode"] == "append"
    assert err.data["source_name"] == "intersystems.openfda_faers"
    assert err.fix_hint
