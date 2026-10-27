"""Real lineage-envelope emission from the live native-view publish path.

`deltaflow-publish-projection`: prove that a real publish cycle emits
`MaterializedLineageEnvelope`s for the views it materializes (the live
path never did this before — `scheduler.py` published artifacts with no
envelopes, leaving the reverse index empty), and that those envelopes are
keyed so a change frontier built from the view's cited node, its source
record, or its component hash actually finds the view output via
`lookup_dirty_outputs`. This is the loop the synthetic bench faked, now
on real data.
"""
from __future__ import annotations

import contextlib
import psycopg
import pytest

from okg.substrate.derived_artifacts import stable_hash
from okg.substrate.derived_artifacts.dependency_index import (
    DependencyLookupKey,
    lookup_dirty_outputs,
    summarize_lineage_index,
)
from okg.substrate.derived_artifacts.output_lineage import as_hash
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher.native_view_stage import NativeViewStage
from okg.substrate.publisher.publisher import Publisher as _Publisher
from okg.substrate.source_sync_contract import canonical_record_id
from tests.helpers.governed_view_materializer import ViewMaterializer
from okg.views.planners.base import (
    ViewDefinition,
    ViewOutputTemplate,
    default_output_templates,
)
from okg.views.types import Anchor
from tests.substrate.test_publisher_native_views import (
    _assembler,
    _materializer,
    _planner,
    _seed_publication_node,
    _seed_view_cites_catalog,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


class Publisher(_Publisher):
    """Use the current DBOS publisher mutex in native-view publisher tests."""

    def __init__(self, *args, **kwargs) -> None:
        kwargs.setdefault("coordination_mode", "dbos")
        super().__init__(*args, **kwargs)


def _dirty_output_ids(result) -> set[str]:
    return {output.output_id for output in result.dirty_outputs}


def _relation_view_materializer(cited_node_id: str):
    """A view that also emits a derived-relation table, citing one node."""
    from uuid import uuid4

    view_id = f"test.publisher.relation_view.{uuid4().hex[:12]}"

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=_planner,
            assembler=_assembler,
            view_def_hash=stable_hash({"view_id": view_id, "planner": "rel"}),
            output_templates=(
                *default_output_templates(),
                ViewOutputTemplate.sql_relation(
                    logical_relation_id=f"{view_id}.relation",
                    columns=("row_key", "label"),
                    sql="SELECT 'r1' AS row_key, 'hello' AS label",
                    scope_key="rel",
                ),
            ),
        )

    def build_corpus(conn, *, generation_id):
        return {"summary": Anchor.from_content("summary", "relation view")}

    materializer = ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
    )
    materializer.anchor_resolver = (
        lambda anchor_id: cited_node_id if anchor_id == "summary" else None
    )
    return view_id, materializer


def test_native_view_relations_also_emit_lineage_envelopes(
    clean_runtime: str,
) -> None:
    cited_node_id = "publication:relation-view-source"
    view_id, materializer = _relation_view_materializer(cited_node_id)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        _seed_publication_node(conn, cited_node_id)

    out = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert out["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        summary = summarize_lineage_index(conn, deployment_name="default")
        # Both the view AND its relation recorded an ingredients list.
        assert summary.output_kind_counts.get("native_view", 0) >= 1
        assert summary.output_kind_counts.get("derived_relation", 0) >= 1

        # A change to the cited node finds BOTH outputs (view + relation),
        # so neither can be wrongly reused while stale.
        by_node = lookup_dirty_outputs(
            conn,
            dependency_keys=[DependencyLookupKey("graph_node", cited_node_id)],
        )
        assert by_node.available
        kinds = {output.output_kind for output in by_node.dirty_outputs}
        assert "native_view" in kinds
        assert "derived_relation" in kinds


def test_unchanged_native_view_outputs_reemit_lineage_for_new_manifest(
    clean_runtime: str,
) -> None:
    cited_node_id = "publication:lineage-reuse-source"
    view_id, materializer = _relation_view_materializer(cited_node_id)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        _seed_publication_node(conn, cited_node_id)

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert first["status"] == "published"

    _apply_node_change(clean_runtime, cited_node_id, {"v": 2})
    publisher = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    )
    second = publisher.cycle()
    assert second["status"] == "published"
    outcome = second["views"][0]
    assert outcome["freshness"] == "fresh"
    assert outcome["fragments_refreshed"] == 0
    timings = publisher._last_view_outcomes[0].timings_ms
    assert timings is not None
    assert any(key.startswith("lineage.publish_view.") for key in timings)
    assert any(key.startswith("lineage.publish_relation.") for key in timings)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        by_node = lookup_dirty_outputs(
            conn,
            dependency_keys=[DependencyLookupKey("graph_node", cited_node_id)],
        )
        assert by_node.available
        dirty_ids = _dirty_output_ids(by_node)
        assert f"{view_id}:__view__" in dirty_ids
        assert f"view-relation:{view_id}:relation" in dirty_ids


def _counting_citing_view(
    view_id: str,
    cited_node_id: str,
    counter: dict,
    reads_subtypes: tuple[str, ...] = (),
):
    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=_planner,
            assembler=_assembler,
            view_def_hash=stable_hash({"view_id": view_id}),
        )

    def build_corpus(conn, *, generation_id):
        counter["builds"] += 1
        return {"summary": Anchor.from_content("summary", "reuse-test corpus")}

    materializer = ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
        reads_subtypes=reads_subtypes,
    )
    materializer.anchor_resolver = (
        lambda anchor_id: cited_node_id if anchor_id == "summary" else None
    )
    return materializer


def _apply_node_change(dsn: str, node_id: str, attrs: dict) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.apply_facts(
            [
                NodeFact(
                    node_id=node_id,
                    subtype="publication",
                    attrs=attrs,
                    source_record_id={"id": node_id},
                    source_revision=attrs,
                )
            ],
            source="test.publications",
        )


def test_view_rebuilds_only_when_its_cited_node_changes(
    clean_runtime: str,
) -> None:
    # The headline behavior: a view's corpus is rebuilt only when one of
    # ITS cited nodes changes. An unrelated graph change reuses the view;
    # a change to the cited node rebuilds it. (Under chronos-native the
    # fact-log watermark is blind to apply_facts changes, so without the
    # change-set this view would never rebuild — see the bug this fixes.)
    cited = "publication:reuse-cited"
    other = "publication:reuse-other"
    counter = {"builds": 0}
    view_id = "test.reuse.view"
    # PACT view-output-carry-forward-3981: a view that declares no reads is
    # rebuilt on ANY graph change, and one that declares `publication` is
    # rebuilt on any publication change. The `concept` declaration below is
    # deliberately synthetic: nothing in this test changes a concept, so the
    # declared-reads gate never fires and only the cited-node check can
    # trigger the rebuild. That keeps end-to-end coverage of that check.
    materializer = _counting_citing_view(
        view_id, cited, counter, reads_subtypes=("concept",)
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        _seed_publication_node(conn, cited)
        _seed_publication_node(conn, other)

    first = Publisher(
        clean_runtime, "default", materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert first["status"] == "published"
    assert first["views"][0]["freshness"] == "fresh"
    assert counter["builds"] == 1

    # Unrelated change → the view is reused, corpus not rebuilt.
    _apply_node_change(clean_runtime, other, {"v": 2})
    second = Publisher(
        clean_runtime, "default", materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert second["status"] == "published"
    assert second["views"][0]["freshness"] == "carried_forward"
    assert counter["builds"] == 1

    # The cited node changes → the view IS rebuilt.
    _apply_node_change(clean_runtime, cited, {"v": 3})
    third = Publisher(
        clean_runtime, "default", materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert third["status"] == "published"
    assert third["views"][0]["freshness"] == "fresh"
    assert counter["builds"] == 2


def test_native_view_emits_lineage_envelope_keyed_to_real_inputs(
    clean_runtime: str,
) -> None:
    view_id, materializer = _materializer()
    cited_node_id = "publication:native-view-source"
    materializer.anchor_resolver = (
        lambda anchor_id: cited_node_id if anchor_id == "summary" else None
    )
    view_def_hash = materializer.view_def_factory().view_def_hash

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        _seed_publication_node(conn, cited_node_id)

    out = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert out["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # The live publish actually populated the reverse index.
        summary = summarize_lineage_index(conn, deployment_name="default")
        assert summary.envelope_count >= 1
        assert "graph_node" in summary.dependency_kind_counts
        assert "source_record" in summary.dependency_kind_counts

        # A graph-node change (Chronos write-set key) finds the view output.
        by_node = lookup_dirty_outputs(
            conn,
            dependency_keys=[DependencyLookupKey("graph_node", cited_node_id)],
        )
        assert by_node.available
        node_outputs = _dirty_output_ids(by_node)
        assert any(oid.startswith(f"{view_id}:") for oid in node_outputs)

        # A source-record change (SourceDeltaBatch key) finds it too — the
        # ref is built with the SAME canonicalization the delta contract
        # uses, so the keys match.
        record_ref = (
            f"test.publications:{canonical_record_id({'id': cited_node_id})}"
        )
        by_record = lookup_dirty_outputs(
            conn,
            dependency_keys=[
                DependencyLookupKey("source_record", record_ref),
            ],
        )
        assert by_record.available
        assert any(
            oid.startswith(f"{view_id}:")
            for oid in _dirty_output_ids(by_record)
        )

        # A view-definition edit (component-hash key) finds only this
        # view's outputs — issue #79 acceptance point 4 on real data.
        by_component = lookup_dirty_outputs(
            conn,
            dependency_keys=[
                DependencyLookupKey("component_hash", as_hash(view_def_hash)),
            ],
        )
        assert by_component.available
        assert any(
            oid.startswith(f"{view_id}:")
            for oid in _dirty_output_ids(by_component)
        )

        # An unrelated change touches nothing.
        unrelated = lookup_dirty_outputs(
            conn,
            dependency_keys=[
                DependencyLookupKey("graph_node", "publication:does-not-exist"),
            ],
        )
        assert unrelated.available
        assert _dirty_output_ids(unrelated) == set()


def test_lineage_read_failure_fails_governed_views_without_blocking_generation(
    clean_runtime: str,
    monkeypatch,
) -> None:
    """A provenance-read failure fails each governed output closed.

    Native views remain non-gating derived artifacts, so the graph generation
    can publish. The per-view savepoint must roll back the manifest whose
    lineage cannot be established, and the stage must report every affected
    view as failed instead of exposing policy-visible bytes without lineage.
    """
    import psycopg as _pg

    cited = "publication:lineage-read-boom"
    view_id1, mat1 = _relation_view_materializer(cited)
    view_id2, mat2 = _relation_view_materializer(cited)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        _seed_publication_node(conn, cited)

    def boom(
        self,
        conn,
        node_ids,
        *,
        branch: str = "default",
        timing_sink=None,
    ):
        # Simulate the real failure: a query against okg.graph_nodes that errors
        # (and, on a non-autocommit conn, leaves the txn aborted) then raises a
        # psycopg.Error — exactly what a missing/non-interval table would do.
        with conn.cursor() as cur:
            with contextlib.suppress(_pg.Error):
                cur.execute("SELECT 1 FROM okg.__no_such_table_for_test__")
        raise _pg.errors.UndefinedTable("simulated provenance read failure")

    monkeypatch.setattr(NativeViewStage, "_collect_source_records", boom)

    pub = Publisher(clean_runtime, "default", materializers=[mat1, mat2], catalog_identity=bind_test_catalog_writer(clean_runtime, "default"))
    out = pub.cycle()

    assert out["status"] == "published"
    errored = [o for o in pub._last_view_outcomes if getattr(o, "error", None)]
    assert len(errored) == 2
    assert all(
        "governed native view lineage could not be persisted" in str(item.error)
        for item in errored
    )
    assert len(pub._last_view_outcomes) == 2
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.derived_artifact_runs
             WHERE definition_id = ANY(%s)
               AND status IN ('completed', 'carried_forward')
            """,
            ([f"view:{view_id1}", f"view:{view_id2}"],),
        )
        assert cur.fetchone() == (0,)


def test_unavailable_dirty_scope_forces_view_rebuild(
    clean_runtime: str,
    monkeypatch,
) -> None:
    """When the Chronos dirty scope cannot be computed (the segment-delta query
    returns None — non-interval tables or a swallowed query error), the
    publisher cannot prove a view's inputs are unchanged and MUST rebuild
    rather than carry forward a stale corpus.

    `harden-deltaflow-frontier-blockers`: before, a None scope made both the
    frontier and the reads-delta signals empty, and the dead chronos-native
    input-fact watermark let every view carry forward — silently serving stale
    while the generation still published the change.
    """
    cited = "publication:unavail-cited"
    other = "publication:unavail-other"
    counter = {"builds": 0}
    materializer = _counting_citing_view("test.unavail.view", cited, counter)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        _seed_publication_node(conn, cited)
        _seed_publication_node(conn, other)

    first = Publisher(
        clean_runtime, "default", materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert first["status"] == "published"
    assert first["views"][0]["freshness"] == "fresh"
    assert counter["builds"] == 1

    # An UNRELATED change would normally carry the view forward (see
    # test_view_rebuilds_only_when_its_cited_node_changes). Force the dirty
    # scope UNAVAILABLE: the publisher can no longer prove the view is
    # unaffected, so it must rebuild rather than serve stale.
    _apply_node_change(clean_runtime, other, {"v": 2})
    monkeypatch.setattr(
        Publisher,
        "_query_dirty_from_chronos_current_segment_by_type",
        lambda *a, **k: None,
    )
    second = Publisher(
        clean_runtime, "default", materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert second["status"] == "published"
    assert second["views"][0]["freshness"] == "fresh"
    assert counter["builds"] == 2
