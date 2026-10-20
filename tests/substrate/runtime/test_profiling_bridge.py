"""Tests for `restore-publisher-perf-and-profiling` §B.

Verifies that `workflow_profiling_context` enters a real
`profiling_context`, makes `current_profile()` non-None inside the
block, and tolerates the no-DBOS-context case (the linkage path
fails silently).
"""
from __future__ import annotations


def test_workflow_profiling_context_enters_context():
    """Inside the context, `current_profile()` returns a non-None
    ProfileContext. Outside, it's None again."""
    from okg.substrate.profiling import current_profile
    from okg.substrate.runtime.profiling_bridge import (
        workflow_profiling_context,
    )

    class _FakeManifest:
        profiling = None

    assert current_profile() is None
    with workflow_profiling_context(
        manifest=_FakeManifest(),
        deployment="test-dep",
        target_kind="publish",
        command="okg_publisher_cycle",
    ) as prof:
        assert current_profile() is not None
        assert prof is current_profile()
        assert prof.deployment == "test-dep"
        assert prof.target_kind == "publish"
        assert prof.command == "okg_publisher_cycle"
    assert current_profile() is None


def test_workflow_profiling_context_records_stages():
    """The 24 `profile_stage(...)` hooks inside the substrate emit
    into the active context. Verify by emitting a synthetic one
    and reading it back."""
    from okg.substrate.profiling import profile_stage
    from okg.substrate.runtime.profiling_bridge import (
        workflow_profiling_context,
    )

    class _FakeManifest:
        profiling = None

    with workflow_profiling_context(
        manifest=_FakeManifest(),
        deployment="test-dep",
        target_kind="publish",
        command="okg_publisher_cycle",
    ) as prof:
        with profile_stage("test.fake_stage"):
            pass
        # Lightweight mode records into the ring buffer; deep
        # capture records into the stages list. At minimum, one of
        # them should have an entry for our stage.
        rb_events = [e for e in prof.ring if e.get("name") == "test.fake_stage"]
        stage_entries = [s for s in prof.stages if s.name == "test.fake_stage"]
        assert rb_events or stage_entries, (
            "profile_stage hook did not emit into either ring "
            "buffer or stages list — bridge isn't entering the "
            "expected context"
        )


def test_workflow_profiling_context_tolerates_no_dbos():
    """The bridge tries to read DBOS.workflow_id() for linkage. In a
    test that hasn't constructed DBOS, this should fail silently
    and the context should still work."""
    from okg.substrate.profiling import current_profile
    from okg.substrate.runtime.profiling_bridge import (
        workflow_profiling_context,
    )

    class _FakeManifest:
        profiling = None

    # DBOS is not initialized in this test. The bridge should
    # silently catch the failure to read workflow_id and proceed.
    with workflow_profiling_context(
        manifest=_FakeManifest(),
        deployment="test-dep",
        target_kind="source_sync",
        command="okg_source_sync",
    ) as prof:
        # request_path stays None (no DBOS linkage).
        assert prof.request_path in (None, "")
        # But the context itself is active.
        assert current_profile() is prof


def test_workflow_profiling_context_respects_manifest_profiling():
    """When the manifest carries a profiling block, the policy
    resolver picks it up. We don't assert specific policy values
    (those are owned by the policy layer's tests); we just verify
    the block is read without error."""
    from okg.substrate.runtime.profiling_bridge import (
        workflow_profiling_context,
    )

    class _FakeManifest:
        profiling = {
            "lightweight": {"enabled": True},
            "deep_capture": {"mode": "off"},
        }

    with workflow_profiling_context(
        manifest=_FakeManifest(),
        deployment="test-dep",
        target_kind="publish",
        command="okg_publisher_cycle",
    ) as prof:
        # `collect_detail` should be False when deep_capture mode=off.
        assert prof.collect_detail is False
