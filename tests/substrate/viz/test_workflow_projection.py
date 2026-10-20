"""Unit tests for the workflow-trace LIST projection behind ``/viz/api/workflows``.

Home reads that route for its recent-pipeline-activity list, so the row shape
and the status vocabulary are what this module pins. Pure: no DB. Shapes mirror
the live okg-workspace data (source_run + publisher traces).

The span-timeline scene projections (``project_span``,
``project_workflow_scene``, ``focus_workflow_scene``) existed only for the Trace
and Performance views' GraphCanvas rendering and were removed with those views
in issue #1924; the last test here proves they are gone rather than merely
untested.
"""
from datetime import datetime, timezone

import pytest

from okg.viz import workflow_projection
from okg.viz.workflow_projection import (
    WORKFLOW_ROOT_KINDS,
    project_workflow_list_item,
)

T0 = datetime(2026, 6, 30, 3, 10, 0, tzinfo=timezone.utc)


# --- list shape --------------------------------------------------------------


def test_list_item_shape_and_status_normalization():
    item = project_workflow_list_item({
        "trace_id": "t1", "root_name": "publisher.cycle", "root_kind": "publisher",
        "status_code": "error", "duration_ms": 199116, "started_at": T0,
        "generation_id": "g", "workflow_id": None, "span_count": 1,
    })
    assert item["trace_id"] == "t1"
    assert item["root_kind"] == "publisher"
    assert item["status"] == "error"
    assert item["span_count"] == 1
    assert item["started_at"] == T0.isoformat()


def test_list_item_strips_the_deployment_segment_from_a_source_root_name():
    item = project_workflow_list_item({
        "trace_id": "t2",
        "root_name": "source.okg-workspace.git-files",
        "root_kind": "source_run",
        "status_code": "ok",
        "duration_ms": 116868,
        "started_at": T0,
        "generation_id": "okg:okg-workspace:gen:1",
        "workflow_id": None,
        "span_count": 4,
    })
    assert item["root_name"] == "git-files"
    assert item["status"] == "ok"


def test_workflow_root_kinds_excludes_mcp():
    assert "mcp_call" not in WORKFLOW_ROOT_KINDS
    assert "source_run" in WORKFLOW_ROOT_KINDS
    assert "publisher" in WORKFLOW_ROOT_KINDS


# --- the removed scene surface ----------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["project_span", "project_workflow_scene", "focus_workflow_scene"],
)
def test_span_scene_projection_is_gone(name: str) -> None:
    """The Trace/Performance scene projections must not come back silently."""
    assert not hasattr(workflow_projection, name), (
        f"{name} was removed with the Trace and Performance views (issue #1924); "
        "reintroducing it needs a caller and a PACT, not a quiet restore"
    )
