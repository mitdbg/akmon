from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from okg.substrate.console_views import sources
from okg.substrate.publisher import publisher as publisher_mod
from okg.substrate.publisher.native_view_stage import NativeViewStage
from okg.substrate.publisher.publisher import Publisher
from tests.helpers.governed_view_materializer import ViewMaterializer


def _raise_runtime(*args, **kwargs):
    raise RuntimeError("probe failed")


class _BadConnection:
    def cursor(self):
        raise RuntimeError("cursor failed")

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_source_registry_resolution_failure_is_loud(monkeypatch) -> None:
    import okg.substrate.deployments as deployments

    monkeypatch.setattr(deployments, "find_deployment_dir", _raise_runtime)

    with pytest.raises(RuntimeError, match="probe failed"):
        sources._registry_source_names("okg-workspace")


def test_native_view_probe_failures_are_loud() -> None:
    materializer = ViewMaterializer(
        view_def_factory=_raise_runtime,
        build_corpus=lambda *args, **kwargs: {},
        name="bad-view",
    )
    stage = NativeViewStage([materializer], deployment_name="okg-workspace")

    with pytest.raises(RuntimeError, match="probe failed"):
        stage.latest_completed_at(object())
    with pytest.raises(RuntimeError, match="probe failed"):
        stage.completed_watermark_at(object())
    with pytest.raises(RuntimeError, match="probe failed"):
        stage.can_reuse_noop_generation(object(), generation_id=1)


def test_native_view_lineage_reuse_probe_failure_is_loud() -> None:
    with pytest.raises(RuntimeError, match="cursor failed"):
        NativeViewStage._lineage_reuse_available(
            _BadConnection(),
            output_ids=["view:one"],
            cited_node_ids=["node:one"],
        )


def test_native_view_frontier_probe_failure_is_loud() -> None:
    stage = NativeViewStage([], deployment_name="okg-workspace")
    invalidation = SimpleNamespace(
        dependency_kind="graph_node",
        lookup_key=lambda: SimpleNamespace(
            dependency_kind="graph_node",
            dependency_ref="node:one",
            to_dict=lambda: {
                "dependency_kind": "graph_node",
                "dependency_ref": "node:one",
            },
        ),
    )

    with pytest.raises(RuntimeError, match="cursor failed"):
        stage._view_affected_by_frontier(
            _BadConnection(),
            definition=SimpleNamespace(definition_id="view:one"),
            generation_id=1,
            lineage_invalidations=[invalidation],
        )


def test_publisher_metadata_probe_failures_are_loud(monkeypatch) -> None:
    class BadDBOS:
        @staticmethod
        def workflow_id():
            raise RuntimeError("dbos failed")

    monkeypatch.setitem(sys.modules, "dbos", SimpleNamespace(DBOS=BadDBOS))
    with pytest.raises(RuntimeError, match="dbos failed"):
        publisher_mod._current_dbos_workflow_id()

    publisher = object.__new__(Publisher)
    publisher.chronos_interval_continuation_percent = None
    publisher.deployment_name = "okg-workspace"
    publisher.source_sync_publish_context = False
    publisher.source_sync_priority_probe = _raise_runtime

    import okg.substrate.graph_store as graph_store

    monkeypatch.setattr(
        graph_store,
        "deployment_interval_continuation_percent",
        _raise_runtime,
    )

    with pytest.raises(RuntimeError, match="probe failed"):
        publisher._chronos_interval_continuation_percent()
    with pytest.raises(RuntimeError, match="probe failed"):
        publisher._blocking_source_sync_workflow_id()


def test_publisher_extractor_input_probe_failure_is_loud() -> None:
    publisher = object.__new__(Publisher)
    publisher._extractor_stage = SimpleNamespace(source_subtypes=())

    with pytest.raises(RuntimeError, match="cursor failed"):
        publisher._chronos_current_segment_has_extractor_input_changes(
            _BadConnection(),
            branch_name="default",
        )


def test_publisher_chronos_probe_failures_are_loud(monkeypatch) -> None:
    import okg.substrate.graph_store as graph_store

    monkeypatch.setattr(
        graph_store.ChronosGraphStore,
        "from_connection",
        _raise_runtime,
    )
    with pytest.raises(RuntimeError, match="probe failed"):
        Publisher._query_dirty_from_chronos_branch(
            object(),
            (),
            branch_name="default",
        )

    conn = _BadConnection()
    with pytest.raises(RuntimeError, match="cursor failed"):
        Publisher._query_dirty_from_chronos_current_segment_by_type(
            conn,
            branch_name="default",
        )
    with pytest.raises(RuntimeError, match="cursor failed"):
        Publisher._chronos_current_segment_delta_counts(
            conn,
            branch_name="default",
            previous=SimpleNamespace(ref="1"),
        )
    with pytest.raises(RuntimeError, match="cursor failed"):
        Publisher._chronos_current_segment_has_graph_writes(
            conn,
            branch_name="default",
        )
    with pytest.raises(RuntimeError, match="cursor failed"):
        Publisher._chronos_current_segment_delta_counts_for_keys(
            conn,
            branch_name="default",
            previous=SimpleNamespace(ref="1"),
            node_ids=["node:one"],
            edge_ids=[],
        )


# The graph-module-attribution case that used to close this module tested
# `console_views.graph._compose_ontology`, which went with the Graph view in
# issue #1924. Nothing reads the ontology rollup now, so there is no surviving
# caller whose attribution failure could be reported as metadata.
