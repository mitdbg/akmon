"""Publisher cutover tests for native V1 view artifacts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import psycopg
import pytest

from okg.substrate.consistency import ConsistencyPublishGateEvaluation
from okg.substrate.derived_artifacts import stable_hash
from okg.substrate.derived_artifacts.relations import (
    RelationArtifactDefinition,
    RelationRow,
    publish_relation_artifact,
    relation_visible_rows_for_manifest_id,
)
from okg.substrate.derived_artifacts.read_views import read_view_slot
from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.lenses import LensDefinition
from okg.substrate.publisher import Publisher as _Publisher
from okg.substrate.publisher import native_view_stage as native_view_stage_module
from okg.substrate.publisher.native_view_stage import (
    NativeViewStage,
    _definition_for,
    _relation_dirty_scope_keys,
    _relation_rows_for_template,
    _relation_visible_row_count_after_scoped_refresh,
)
from okg.substrate.views.base import ViewDeltaScope
from tests.helpers.governed_view_materializer import ViewMaterializer
from okg.views.assembly import assemble as default_assemble
from okg.views.planners.base import (
    GeneratedSpec,
    QuoteSpec,
    ViewDefinition,
    ViewOutputTemplate,
    ViewRelationColumn,
    default_output_templates,
)
from okg.views.types import Anchor
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


class Publisher(_Publisher):
    """Use the current DBOS publisher mutex in native-view publisher tests."""

    def __init__(self, *args, **kwargs) -> None:
        kwargs.setdefault("coordination_mode", "dbos")
        super().__init__(*args, **kwargs)


class BlockingGate:
    name = "blocking-native-view-test"

    def evaluate(
        self,
        conn: psycopg.Connection,
        *,
        target_generation_id: int,
        catalog_version_id: int,
        deployment_name: str,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        return ConsistencyPublishGateEvaluation(
            run_id=uuid4(),
            check_id="blocking-native-view-test",
            target_generation_id=target_generation_id,
            gate_name=self.name,
            result="evaluated",
            blocks_publish=True,
            reason="test_block",
            open_observation_count=1,
            coverage_complete=True,
            coverage_basis="exact_postgres",
            budget_ms=1000,
            elapsed_ms=1,
        )


def _watermark_materializer(view_id: str):
    def view_def_factory():
        return type("WatermarkViewDef", (), {"view_id": view_id})()

    return ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=lambda *_args, **_kwargs: {},
        name=view_id,
    )


def test_native_view_completion_watermark_uses_slowest_completed_view(
    clean_runtime: str,
) -> None:
    first = f"test.watermark.{uuid4().hex[:12]}.first"
    second = f"test.watermark.{uuid4().hex[:12]}.second"
    stage = NativeViewStage(
        [_watermark_materializer(first), _watermark_materializer(second)],
        deployment_name="test",
    )
    t1 = datetime.now(UTC) - timedelta(minutes=10)
    t2 = datetime.now(UTC) - timedelta(minutes=5)
    t3 = datetime.now(UTC) - timedelta(minutes=1)

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            for view_id in (first, second):
                cur.execute(
                    """
                    INSERT INTO okg.derived_artifact_definitions (
                      definition_id, artifact_kind, deployment_name, logical_key,
                      compute_hash, output_schema_hash, renderer_hash,
                      dirty_rule_hash, renderer_ref
                    ) VALUES (
                      %(definition_id)s, 'view', 'test', %(view_id)s,
                      'sha256:compute', 'sha256:schema', 'sha256:renderer',
                      'sha256:dirty', 'test.renderer'
                    )
                    """,
                    {
                        "definition_id": f"view:{view_id}",
                        "view_id": view_id,
                    },
                )
            cur.execute(
                """
                INSERT INTO okg.derived_artifact_runs (
                  definition_id, target_generation_id, status, trigger_kind,
                  compute_hash, output_schema_hash, renderer_hash,
                  dirty_rule_hash, completed_at
                ) VALUES (
                  %(definition_id)s, 1, 'completed', 'post_publish',
                  'sha256:compute', 'sha256:schema', 'sha256:renderer',
                  'sha256:dirty', %(completed_at)s
                )
                """,
                {"definition_id": f"view:{first}", "completed_at": t1},
            )
        conn.commit()

        assert stage.completed_watermark_at(conn) is None

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.derived_artifact_runs (
                  definition_id, target_generation_id, status, trigger_kind,
                  compute_hash, output_schema_hash, renderer_hash,
                  dirty_rule_hash, completed_at
                ) VALUES (
                  %(definition_id)s, 1, 'completed', 'post_publish',
                  'sha256:compute', 'sha256:schema', 'sha256:renderer',
                  'sha256:dirty', %(completed_at)s
                )
                """,
                {"definition_id": f"view:{second}", "completed_at": t2},
            )
            cur.execute(
                """
                INSERT INTO okg.derived_artifact_runs (
                  definition_id, target_generation_id, status, trigger_kind,
                  compute_hash, output_schema_hash, renderer_hash,
                  dirty_rule_hash, completed_at
                ) VALUES (
                  %(definition_id)s, 2, 'completed', 'post_publish',
                  'sha256:compute', 'sha256:schema', 'sha256:renderer',
                  'sha256:dirty', %(completed_at)s
                )
                """,
                {"definition_id": f"view:{first}", "completed_at": t3},
            )
        conn.commit()

        watermark = stage.completed_watermark_at(conn)

    assert watermark == t2


def test_cached_input_signature_match_reuses_positive_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native_view_stage_module._INPUT_SIGNATURE_MATCH_CACHE.clear()
    calls = 0

    class _Info:
        @staticmethod
        def get_parameters():
            return {
                "host": "localhost",
                "port": "5432",
                "dbname": "okg_unit",
                "user": "postgres",
            }

    class _Conn:
        info = _Info()

    def load_payload(_conn, *, definition_id: str, before_generation_id: int):
        nonlocal calls
        calls += 1
        assert definition_id == "view:test"
        assert before_generation_id == 101
        return {
            "materializer_input_signature": {
                "input_fact_watermark": 11,
                "input_edge_fact_watermark": 22,
            },
        }

    monkeypatch.setattr(
        native_view_stage_module,
        "_load_cached_view_payload",
        load_payload,
    )

    assert native_view_stage_module._cached_input_signature_matches(
        _Conn(),
        definition_id="view:test",
        before_generation_id=101,
        input_fact_watermark=11,
        input_edge_fact_watermark=22,
    )
    assert native_view_stage_module._cached_input_signature_matches(
        _Conn(),
        definition_id="view:test",
        before_generation_id=101,
        input_fact_watermark=11,
        input_edge_fact_watermark=22,
    )
    assert calls == 1


class CountingPassGate:
    name = "counting-native-view-test"

    def __init__(self) -> None:
        self.calls = 0

    def evaluate(
        self,
        conn: psycopg.Connection,
        *,
        target_generation_id: int,
        catalog_version_id: int,
        deployment_name: str,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        del conn, catalog_version_id, deployment_name, branch_name
        self.calls += 1
        return ConsistencyPublishGateEvaluation(
            run_id=uuid4(),
            check_id=self.name,
            target_generation_id=target_generation_id,
            gate_name=self.name,
            result="evaluated",
            blocks_publish=False,
            reason=None,
            open_observation_count=0,
            coverage_complete=True,
            coverage_basis="exact_postgres",
            budget_ms=1000,
            elapsed_ms=1,
        )


class CountingEnricher:
    name = "counting-native-view-enricher"
    publisher_skip_when_clean = True
    reads_subtypes = ("publication",)
    reads_edge_types = ()
    # Explicit minimal envelope: the boot guard demands a declared write
    # surface; this fixture's behavior under test is call counting on
    # noop publishes, not admission.
    write_envelope = {
        "nodes": [{"subtype": "publication"}],
        "nondeterministic": False,
    }
    # Node surface declared only to satisfy the write-envelope boot guard;
    # this fixture emits no node facts, so the provenance declaration is
    # vacuously satisfied. Behavior under test is call counting on noop
    # publishes, not admission.
    declares_node_provenance = True

    def __init__(self) -> None:
        self.calls = 0

    def enrich(self, conn, *, generation_id, incremental=None):
        del conn, generation_id, incremental
        self.calls += 1
        return EnrichResult(self.name, n_edges_emitted=0)


def _planner(
    corpus: dict[str, Anchor],
    view_def: ViewDefinition,
) -> list[QuoteSpec]:
    assert "summary" in corpus
    return [
        QuoteSpec(
            id="summary-fragment",
            anchor_id="summary",
            display_label="Summary",
        )
    ]


def _assembler(view) -> str:
    return "# Native Publisher View\n\n" + default_assemble(view)


def _resolver_one(anchor_id: str) -> str | None:
    return "publication:native-view-source-1" if anchor_id == "summary" else None


def _resolver_two(anchor_id: str) -> str | None:
    return "publication:native-view-source-2" if anchor_id == "summary" else None


def _planner_prompt_v1(
    corpus: dict[str, Anchor],
    view_def: ViewDefinition,
) -> list[GeneratedSpec]:
    return [
        GeneratedSpec(
            id="summary-fragment",
            sources=("summary",),
            system_prompt="You are a terse summarizer.",
            task_prompt="Summarize the sources in one sentence.",
        )
    ]


def _planner_prompt_v2(
    corpus: dict[str, Anchor],
    view_def: ViewDefinition,
) -> list[GeneratedSpec]:
    return [
        GeneratedSpec(
            id="summary-fragment",
            sources=("summary",),
            system_prompt="You are a VERBOSE, flowery essayist.",
            task_prompt="Write three lavish paragraphs about the sources.",
        )
    ]


def test_prompt_only_edit_busts_publisher_carry_forward_hash() -> None:
    """A prompt-only planner edit must change the view's ``compute_hash``.

    Regression for #693: the publisher's carry-forward fast path
    (``chronos_semantic_carry_forward``) gates on ``definition_stable``,
    which is derived from ``_definition_for(...).compute_hash``. Planners
    own their prompts in code, so a prompt edit changes neither
    ``view_def_hash`` (YAML wiring), ``_callable_ref`` (module.qualname),
    nor ``params``. Without folding the planner source into the hash, a
    prompt-only edit leaves ``compute_hash`` constant and the stale
    completion is carried forward indefinitely for a quiescent-data view.

    We make the two planners share an identical ``_callable_ref`` (same
    ``__module__``/``__qualname__``) so the ONLY thing that differs is the
    planner source — i.e. the prompt. Pre-fix these hashes were equal.
    """
    # Force the two planners to present an identical callable identity,
    # emulating the same planner function edited in place (prompt changed).
    _planner_prompt_v2.__module__ = _planner_prompt_v1.__module__
    _planner_prompt_v2.__qualname__ = _planner_prompt_v1.__qualname__
    _planner_prompt_v2.__name__ = _planner_prompt_v1.__name__

    shared_view_id = "test.publisher.prompt_edit"
    shared_view_def_hash = stable_hash(
        {"view_id": shared_view_id, "planner": "edited-in-place"}
    )

    def _view_def(planner) -> ViewDefinition:
        return ViewDefinition(
            view_id=shared_view_id,
            planner=planner,
            assembler=_assembler,
            params={"audience": "operator"},
            view_def_hash=shared_view_def_hash,
            display_name="Prompt Edit View",
        )

    templates = default_output_templates()
    definition_v1 = _definition_for(
        view_def=_view_def(_planner_prompt_v1),
        deployment_name="test-deployment",
        anchor_resolver=_resolver_one,
        output_templates=templates,
    )
    definition_v2 = _definition_for(
        view_def=_view_def(_planner_prompt_v2),
        deployment_name="test-deployment",
        anchor_resolver=_resolver_one,
        output_templates=templates,
    )

    # Callable identity is identical (the prompt-blind inputs the pre-fix
    # hash relied on), yet the prompt differs — so compute_hash MUST differ.
    from okg.substrate.publisher.native_view_stage import _callable_ref

    assert _callable_ref(_planner_prompt_v1) == _callable_ref(_planner_prompt_v2)
    assert definition_v1.compute_hash != definition_v2.compute_hash


def test_identical_planner_keeps_carry_forward_hash_stable() -> None:
    """No spurious rebuild: an unchanged planner keeps ``compute_hash`` stable.

    Guards against a fail-closed helper that returns a per-call token for
    source-hashable planners (which would defeat carry-forward every cycle).
    """
    shared_view_id = "test.publisher.prompt_stable"
    view_def = ViewDefinition(
        view_id=shared_view_id,
        planner=_planner_prompt_v1,
        assembler=_assembler,
        params={"audience": "operator"},
        view_def_hash=stable_hash({"view_id": shared_view_id}),
        display_name="Prompt Stable View",
    )
    templates = default_output_templates()
    first = _definition_for(
        view_def=view_def,
        deployment_name="test-deployment",
        anchor_resolver=_resolver_one,
        output_templates=templates,
    )
    second = _definition_for(
        view_def=view_def,
        deployment_name="test-deployment",
        anchor_resolver=_resolver_one,
        output_templates=templates,
    )
    assert first.compute_hash == second.compute_hash


def _materializer() -> tuple[str, ViewMaterializer]:
    suffix = uuid4().hex[:12]
    view_id = f"test.publisher.native_view.{suffix}"

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=_planner,
            assembler=_assembler,
            params={"audience": "operator"},
            view_def_hash=stable_hash(
                {
                    "view_id": view_id,
                    "planner": "_planner",
                    "params": {"audience": "operator"},
                }
            ),
            display_name="Native Publisher View",
        )

    def build_corpus(
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> dict[str, Anchor]:
        return {
            "summary": Anchor.from_content(
                "summary",
                "The native publisher view is backed by a V1 "
                "derived-artifact manifest.",
            )
        }

    return view_id, ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
    )


def _publication_timestamp_materializer(
    observed: list[datetime],
) -> tuple[str, ViewMaterializer]:
    suffix = uuid4().hex[:12]
    view_id = f"test.publisher.publication_timestamp.{suffix}"

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=_planner,
            assembler=_assembler,
            view_def_hash=stable_hash({"view_id": view_id}),
            output_templates=(
                *default_output_templates(),
                ViewOutputTemplate.sql_relation(
                    slot_name="publication_timestamp",
                    logical_relation_id=f"{view_id}.publication_timestamp",
                    columns=(
                        ViewRelationColumn("row_key"),
                        ViewRelationColumn("observed_at"),
                    ),
                    scope_key_column="scope_key",
                    row_key_column="row_key",
                    sql="""
                        SELECT
                          'publication' AS scope_key,
                          'publication' AS row_key,
                          %(published_at)s::timestamptz AS observed_at
                    """,
                ),
            ),
        )

    def build_corpus(
        conn: psycopg.Connection,
        *,
        generation_id: int,
        published_at: datetime,
    ) -> dict[str, Anchor]:
        del conn, generation_id
        observed.append(published_at)
        return {
            "summary": Anchor.from_content(
                "summary",
                "The view received the publisher-reserved timestamp.",
            )
        }

    return view_id, ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
    )


def _lens_for_view(view_id: str) -> LensDefinition:
    lens_contract = {
        "editable_slots": {
            "relation": {
                "row_identity": "row_key",
                "editable_fields": ["title"],
            }
        }
    }
    putback_policy = {
        "mode": "overlay",
        "validator": "expected_value_compare_v1",
    }
    source_authority_policy = {
        "allowed_authorities": ["test.publications"],
        "commit_modes": ["overlay"],
    }
    return LensDefinition(
        definition_id=f"lens:{view_id}:relation-editor",
        deployment_name="default",
        lens_id=f"{view_id}.relation_editor",
        view_definition_id=f"view:{view_id}",
        view_id=view_id,
        compute_hash=stable_hash(
            {
                "lens": view_id,
                "compute": "publisher-blocked-test",
            }
        ),
        output_schema_hash=stable_hash(lens_contract),
        renderer_hash=stable_hash({"renderer": "lens-overlay"}),
        dirty_rule_hash=stable_hash(
            {
                "putback_policy": putback_policy,
                "source_authority_policy": source_authority_policy,
            }
        ),
        lens_contract=lens_contract,
        putback_policy=putback_policy,
        source_authority_policy=source_authority_policy,
    )


def _bad_relation_materializer() -> tuple[str, ViewMaterializer]:
    suffix = uuid4().hex[:12]
    view_id = f"test.publisher.bad_relation.{suffix}"

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=_planner,
            assembler=_assembler,
            view_def_hash=stable_hash(
                {
                    "view_id": view_id,
                    "planner": "_planner",
                }
            ),
            output_templates=(
                *default_output_templates(),
                ViewOutputTemplate.sql_relation(
                    logical_relation_id=f"{view_id}.relation",
                    columns=("label",),
                    sql="SELECT 'missing-row-key' AS label",
                ),
            ),
        )

    def build_corpus(
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> dict[str, Anchor]:
        return {
            "summary": Anchor.from_content(
                "summary",
                "This view intentionally has an invalid relation template.",
            )
        }

    return view_id, ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
    )


def _seed_view_cites_catalog(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        for subtype in ("view", "publication"):
            cur.execute(
                """
                INSERT INTO okg.subtypes (name, catalog_version_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (subtype, cv),
            )
            cur.execute(
                """
                INSERT INTO okg.subtype_ancestors
                  (catalog_version_id, subtype_name, ancestor_name, depth)
                VALUES (%s, %s, %s, 0)
                ON CONFLICT DO NOTHING
                """,
                (cv, subtype, subtype),
            )
        cur.execute(
            """
            INSERT INTO okg.narrowings (
              parent_subtype, edge_type, child_subtype,
              catalog_version_id
            ) VALUES ('view', 'view_cites_node', 'publication', %s)
            ON CONFLICT DO NOTHING
            """,
            (cv,),
        )


def _seed_publication_node(conn: psycopg.Connection, node_id: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations (
              generation_id, deployment_name, status, catalog_version_id,
              published_at, correlation_id
            ) VALUES (
              1, 'default', 'published',
              (SELECT max(catalog_version_id)
                 FROM okg.catalog_versions
                WHERE status = 'applied'),
              now(), 'test:native-view-cites:1'
            )
            ON CONFLICT DO NOTHING
            """
        )
    # Chronos cutover: the publication endpoint lives in the Chronos graph
    # (okg.graph_nodes). okg.nodes_live is dropped; apply_facts is the seed.
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype="publication",
                attrs={},
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
        ],
        source="test.publications",
    )


def test_publisher_materializer_publishes_native_view_artifact(
    clean_runtime: str,
    tmp_path,
) -> None:
    view_id, materializer = _materializer()

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    second = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert not list(tmp_path.rglob("manifest.json"))
    assert not list(tmp_path.rglob("view.md"))

    assert first["status"] == "published"
    assert first["views"][0]["view_id"] == view_id
    assert first["views"][0]["fragments_added"] == 1
    assert first["views"][0]["fragments_reused"] == 0
    assert second["status"] == "published"
    assert second["generation_id"] == first["generation_id"]
    assert second["views"] == []
    assert second["noop"] is True
    assert second["reused_generation"] is True
    assert second["native_views_reused"] is True

    definition_id = f"view:{view_id}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT artifact_kind, logical_key, output_contract
              FROM okg.derived_artifact_definitions
             WHERE definition_id = %s
            """,
            (definition_id,),
        )
        definition_row = cur.fetchone()

        cur.execute(
            """
            SELECT r.status, m.target_generation_id, m.manifest_hash,
                   s.slot_name, s.inline_payload
              FROM okg.derived_artifact_runs r
              JOIN okg.derived_output_manifests m
                ON m.manifest_id = r.output_manifest_id
              JOIN okg.derived_output_slots s
                ON s.manifest_id = m.manifest_id
             WHERE r.definition_id = %s
             ORDER BY m.target_generation_id, s.slot_name
            """,
            (definition_id,),
        )
        rows = cur.fetchall()
        cur.execute(
            """
            SELECT DISTINCT execution_phase
              FROM okg.derived_schedule_plans
             ORDER BY execution_phase
            """
        )
        schedule_phases = {row[0] for row in cur.fetchall()}

        cur.execute(
            """
            SELECT count(*)
              FROM okg.node_facts
             WHERE node_id = %s
               AND subtype = 'view'
            """,
            (f"view:{view_id}",),
        )
        view_fact_count = cur.fetchone()[0]

    # Chronos cutover: the canonical graph is okg.graph_nodes (read via a
    # checkout of the published generation), not the dropped okg.nodes_live.
    # Under chronos-native publishing the publisher materializes the view's
    # own `view`-subtype node into the canonical graph, so it is present here
    # (the legacy assertion read 0 only because chronos_native left the old
    # okg.nodes_live empty -- that table is now gone).
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(second["generation_id"])
        view_node_count = graph.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = :node_id
               AND subtype = 'view'
            """,
            {"node_id": f"view:{view_id}"},
        )[0]["n"]
    finally:
        store.close()

    assert definition_row[:2] == ("view", view_id)
    assert definition_row[2]["slots"] == {
        "markdown": "text/markdown",
        "json": "application/json",
    }
    assert definition_row[2]["optional_slots"] == {}
    assert schedule_phases == {"post_publish"}
    assert len(rows) == 2
    assert {row[0] for row in rows} == {"completed"}
    assert [row[3] for row in rows] == ["json", "markdown"]
    json_payload = rows[0][4]
    assert json_payload["kind"] == "json"
    assert (
        json_payload["value"]["view_manifest"]["fragments"][0]["content"]
        == "The native publisher view is backed by a V1 "
        "derived-artifact manifest."
    )
    assert view_node_count == 1
    assert view_fact_count == 0


def test_publisher_binds_timestamp_before_views_and_persists_same_value(
    clean_runtime: str,
) -> None:
    """A current-cycle view does not depend on its future checkpoint row."""
    observed: list[datetime] = []
    view_id, materializer = _publication_timestamp_materializer(observed)

    out = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert out["status"] == "published"
    assert out["views"][0]["freshness"] == "fresh"
    assert len(observed) == 1
    reserved_at = observed[0]
    assert reserved_at.tzinfo is not None
    assert out["published_at"] == reserved_at.isoformat()

    relation_definition_id = (
        f"view-relation:{view_id}:publication_timestamp"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              (metadata::jsonb)->>'published_at',
              created_at::timestamptz
              FROM public._chronos_branch_interval_checkpoints
             WHERE checkpoint_id = %s
            """,
            (out["generation_id"],),
        )
        checkpoint_metadata_at, checkpoint_row_at = cur.fetchone()
        assert checkpoint_metadata_at == reserved_at.isoformat()
        assert checkpoint_row_at >= reserved_at

        slot_read = read_view_slot(
            conn,
            deployment_name="default",
            view_id=view_id,
            slot_name="publication_timestamp",
            generation_id=int(out["compat_generation_id"]),
        )
        upstream_manifest_id = UUID(
            str(slot_read.slot.storage_ref["upstream_manifest_id"])
        )
        rows = relation_visible_rows_for_manifest_id(
            conn,
            definition_id=relation_definition_id,
            manifest_id=upstream_manifest_id,
        )
        cur.execute(
            """
            SELECT published_at
              FROM okg.graph_generations
             WHERE generation_id = %s
            """,
            (out["compat_generation_id"],),
        )
        legacy_row = cur.fetchone()

    assert len(rows) == 1
    assert datetime.fromisoformat(rows[0].row_data["observed_at"]) == reserved_at
    assert legacy_row is None


def test_native_view_refresh_coalesces_until_forced_final(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An intermediate publish coalesces the native views; a forced final
    one refreshes them. (The effective-view snapshot assertions this test
    also made went with the view, PACT lane-s-storage-drop-copies.)"""
    view_id, materializer = _materializer()

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication_node(
            conn,
            f"publication:native-view-coalesce-{uuid4().hex[:12]}",
        )

    second = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        native_view_materialization_delay_ms=60_000,
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle(native_view_refresh_final=False)
    final = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        native_view_materialization_delay_ms=60_000,
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle(
        native_view_refresh_final=True,
        force_native_view_refresh=True,
    )

    assert first["status"] == "published"
    assert first["native_view_refresh"]["status"] == "materialized"
    assert second["status"] == "published"
    assert second["native_view_refresh"]["status"] == "coalesced"
    assert second["native_view_refresh"]["reason"] == "materialization_delay"
    assert second["views"] == []
    assert final["status"] == "published"
    assert final["noop_reuse_invalidation_reason"] == (
        "native_view_refresh_forced"
    )
    assert final["native_view_refresh"]["status"] == "materialized"
    assert final["views"][0]["view_id"] == view_id
    assert final["views"][0]["freshness"] in {"fresh", "carried_forward"}


def test_native_view_min_refresh_coalesces_final_until_forced(
    clean_runtime: str,
) -> None:
    view_id, materializer = _materializer()

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication_node(
            conn,
            f"publication:native-view-min-refresh-{uuid4().hex[:12]}",
        )

    coalesced = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        native_view_min_refresh_interval_ms=60_000,
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle(native_view_refresh_final=True)
    forced = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        native_view_min_refresh_interval_ms=60_000,
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle(
        native_view_refresh_final=True,
        force_native_view_refresh=True,
    )

    assert first["status"] == "published"
    assert first["native_view_refresh"]["status"] == "materialized"
    assert coalesced["status"] == "published"
    assert coalesced["native_view_refresh"]["status"] == "coalesced"
    assert coalesced["native_view_refresh"]["reason"] == "min_refresh_interval"
    assert coalesced["native_view_refresh"]["final_view_refresh"] is True
    assert coalesced["views"] == []
    assert forced["status"] == "published"
    assert forced["native_view_refresh"]["status"] == "materialized"
    assert forced["views"][0]["view_id"] == view_id


def test_forced_noop_native_view_refresh_reuses_consistency_gate_result(
    clean_runtime: str,
) -> None:
    view_id, materializer = _materializer()
    gate = CountingPassGate()

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        consistency_gates=[gate],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    # How many times a full publish evaluates the gate is the publisher's own
    # business (a pre_view and a post_view pass today, one before PR #1447).
    # What this test owns is that the FORCED no-op refresh reuses the previous
    # verdict instead of evaluating again, so baseline the counter here.
    gate_calls_after_first = gate.calls
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication_node(
            conn,
            f"publication:native-view-gate-reuse-{uuid4().hex[:12]}",
        )

    second = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        consistency_gates=[gate],
        native_view_materialization_delay_ms=60_000,
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle(native_view_refresh_final=False)
    gate_calls_before_final = gate.calls
    final = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        consistency_gates=[gate],
        native_view_materialization_delay_ms=60_000,
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle(
        native_view_refresh_final=True,
        force_native_view_refresh=True,
    )

    assert first["status"] == "published"
    assert second["status"] == "published"
    assert second["native_view_refresh"]["status"] == "coalesced"
    assert final["status"] == "published"
    assert final["native_view_refresh"]["status"] == "materialized"
    assert final["views"][0]["view_id"] == view_id
    # The claim under test: the forced no-op refresh REUSES the previous
    # generation's verdict instead of running the pre-view pass again. It still
    # runs the post-view pass -- that one covers the derived facts the forced
    # refresh writes after the gate, so skipping it would publish unchecked
    # facts (PR #1447). So the final cycle adds exactly one evaluation, and the
    # assertions below pin that the one it skipped is the pre-view pass.
    assert gate_calls_after_first > 0
    assert gate_calls_before_final > gate_calls_after_first
    assert gate.calls == gate_calls_before_final + 1
    assert "consistency_gates.post_view" in final["timings_ms"]

    timings = final["timings_ms"]
    assert "consistency_gates" not in timings
    assert (
        timings["consistency_gates.skipped_for_forced_native_view_refresh_noop"]
        == 1.0
    )
    reuse_summaries = [
        summary
        for summary in final["consistency_gates"]
        if summary["check_id"] == "consistency_gates"
    ]
    assert reuse_summaries == [
        {
            "check_id": "consistency_gates",
            "gate_name": "consistency_gates",
            "target_generation_id": final["compat_generation_id"],
            "result": "skipped_reused_previous",
            "blocks_publish": False,
            "reason": "forced_native_view_refresh_noop",
            "open_observation_count": 0,
            "newly_opened_count": 0,
            "resolved_count": 0,
            "coverage_complete": True,
            "coverage_basis": "previous_generation_same_gate_inputs",
            "budget_ms": None,
            "elapsed_ms": 0,
            "details": {
                "force_native_view_refresh": True,
                "previous_generation_id": str(second["generation_id"]),
            },
        }
    ]
    # The only other summaries come from the mandatory post-view pass over the
    # derived facts this cycle wrote; nothing re-ran the reused pre-view pass.
    other_summaries = [
        summary
        for summary in final["consistency_gates"]
        if summary["check_id"] != "consistency_gates"
    ]
    assert [summary["gate_name"] for summary in other_summaries] == [gate.name]
    assert all(
        summary["result"] == "evaluated" and not summary["blocks_publish"]
        for summary in other_summaries
    )


def test_chronos_noop_publish_reuses_generation_without_global_work(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view_id, materializer = _materializer()
    gate = CountingPassGate()
    enricher = CountingEnricher()
    pub = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        consistency_gates=[gate],
        enrichers=[enricher],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    )

    first = pub.cycle()
    # A full publish evaluates the gate however many times the publisher needs
    # (pre_view and post_view today, one before PR #1447). This test owns the
    # no-op REUSE cycle, so baseline the counters and assert the reuse adds
    # nothing.
    gate_calls_after_first = gate.calls
    enricher_calls_after_first = enricher.calls

    def fail_dirty_scope_scan(*_args, **_kwargs):
        raise AssertionError("no-op reuse should not scan dirty scope")

    def fail_cached_input_signature(*_args, **_kwargs):
        raise AssertionError(
            "proven no-op generation reuse should not depend on fact watermarks"
        )

    monkeypatch.setattr(
        Publisher,
        "_query_dirty_from_chronos_current_segment_by_type",
        staticmethod(fail_dirty_scope_scan),
    )
    monkeypatch.setattr(
        native_view_stage_module,
        "_cached_input_signature_matches",
        fail_cached_input_signature,
    )

    second = pub.cycle()

    assert first["status"] == "published"
    assert first["views"][0]["view_id"] == view_id
    assert second["status"] == "published"
    assert second["generation_id"] == first["generation_id"]
    assert second["noop"] is True
    assert second["reused_generation"] is True
    assert second["native_views_reused"] is True
    assert second["published_at"] == first["published_at"]
    assert second["native_view_refresh"] == {
        "status": "reused",
        "reason": "chronos_noop_same_catalog",
        "generation_id": first["generation_id"],
        "view_stage_ran": False,
        "failed_views": [],
        "failed_view_count": 0,
    }
    assert second["reuse_reason"] == "chronos_noop_same_catalog"
    assert second["noop_reuse_stage"] == "pre_enrichment_delta_probe"
    assert second["noop_reuse_checks"] == {
        "catalog_version": "same",
        "delta": "noop",
        "extractor_input_signature": "same",
        "gate_inputs": "same",
        "lenses": "absent",
        "native_views": "reusable",
        "nomos_policy_pin": "same",
    }

    assert enricher_calls_after_first > 0
    assert gate_calls_after_first > 0
    assert enricher.calls == enricher_calls_after_first
    assert gate.calls == gate_calls_after_first
    assert second["enricher_runs"] == []
    assert second["consistency_gates"] == []
    assert second["views"] == []

    timings = second["timings_ms"]
    skipped = set(second["publish_skipped_stages"])
    assert "current_segment_write_probe" in timings
    assert "delta_counts_noop_reuse_probe" in timings
    assert timings["delta_counts_noop_reuse_probe.segment_empty"] == 1.0
    assert "noop_reuse_check" in timings
    assert "chronos_dirty_scope.query" not in timings
    assert "extractor_dirty_input_check" not in timings
    assert "enrichers" not in timings
    assert "gates" not in timings
    assert "consistency_gates" not in timings
    assert "native_views" not in timings
    assert "checkpoint_generation" not in timings
    assert {
        "extractor_dirty_input_check",
        "enrichers",
        "gates",
        "consistency_gates",
        "native_views",
        "checkpoint_generation",
    }.issubset(skipped)
    assert second["timing_details"]["noop_reuse"]["stage"] == (
        "pre_enrichment_delta_probe"
    )


def test_chronos_noop_reuse_ignores_derived_artifact_fact_watermarks(
    clean_runtime: str,
) -> None:
    from psycopg.types.json import Jsonb

    view_id, materializer = _materializer()
    gate = CountingPassGate()
    pub = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        consistency_gates=[gate],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    )

    first = pub.cycle()
    # Baseline: the first publish's own gate evaluations are not this test's
    # subject; the reuse cycle adding none of its own is.
    gate_calls_after_first = gate.calls
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.edge_facts (
              edge_id, src, dst, edge_type, attrs, provenance, confidence,
              source, source_record_id, source_revision, observed_at,
              dedupe_key, run_id, op
            ) VALUES (
              nextval('okg.edge_facts_edge_id_seq'),
              'view:test', 'symbol:test', 'view_cites_node',
              '{}'::jsonb, 'derived_deterministic', 1.0,
              %s, %s, %s, now(), %s, %s::uuid, 'I'
            )
            """,
            (
                f"derived_artifact:view:{view_id}",
                Jsonb({"id": "derived-edge"}),
                Jsonb({"generation_id": first["generation_id"]}),
                f"derived-edge:{view_id}",
                str(uuid4()),
            ),
        )

    second = pub.cycle()

    assert second["status"] == "published"
    assert second["generation_id"] == first["generation_id"]
    assert second["noop"] is True
    assert second["reused_generation"] is True
    assert second["noop_reuse_stage"] == "pre_enrichment_delta_probe"
    assert second["noop_reuse_checks"]["gate_inputs"] == "same"
    assert second["views"] == []
    assert gate_calls_after_first > 0
    assert gate.calls == gate_calls_after_first


def test_publisher_native_view_carry_forward_skips_corpus(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex[:12]
    view_id = f"test.publisher.native_view.carry_forward.{suffix}"
    corpus_calls = 0

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=_planner,
            assembler=_assembler,
            view_def_hash=stable_hash(
                {
                    "view_id": view_id,
                    "planner": "_planner",
                }
            ),
        )

    def build_corpus(
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> dict[str, Anchor]:
        nonlocal corpus_calls
        corpus_calls += 1
        return {
            "summary": Anchor.from_content(
                "summary",
                "This corpus should be reused on a no-change generation.",
            )
        }

    materializer = ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
    )

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        lenses=[_lens_for_view(view_id)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    second = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        lenses=[_lens_for_view(view_id)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert first["views"][0]["freshness"] == "fresh"
    assert second["views"][0]["freshness"] == "carried_forward"
    assert second["views"][0]["fragments_reused"] == 1
    assert corpus_calls == 1


def test_native_view_relation_carry_forward_reuses_view_slot_manifest(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex[:12]
    view_id = f"test.publisher.native_view.relation_carry.{suffix}"
    relation_definition_id = f"view-relation:{view_id}:relation"
    corpus_calls = 0

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=_planner,
            assembler=_assembler,
            view_def_hash=stable_hash(
                {
                    "view_id": view_id,
                    "planner": "_planner",
                    "relation": "constant-row",
                }
            ),
            output_templates=(
                *default_output_templates(),
                ViewOutputTemplate.sql_relation(
                    logical_relation_id=f"{view_id}.relation",
                    columns=("row_key", "title"),
                    sql=(
                        "SELECT 'summary-fragment' AS row_key, "
                        "'Summary' AS title"
                    ),
                ),
            ),
        )

    def build_corpus(
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> dict[str, Anchor]:
        nonlocal corpus_calls
        del conn, generation_id
        corpus_calls += 1
        return {
            "summary": Anchor.from_content(
                "summary",
                "This relation-bearing view should carry forward without "
                "rewriting the relation artifact.",
            )
        }

    materializer = ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
    )

    first = Publisher(clean_runtime, "default", materializers=[materializer], catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    second_generation_id = int(first["compat_generation_id"]) + 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog_version_id = conn.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'"
        ).fetchone()[0]
        # a generation with an empty graph delta and an unchanged catalog
        second = NativeViewStage(
            [materializer],
            deployment_name="default",
        ).run(
            conn,
            generation_id=second_generation_id,
            catalog_version_id=catalog_version_id,
            graph_delta_empty=True,
        )[0]
        ChronosGraphStore.from_connection(conn).checkpoint_generation(
            second_generation_id,
            deployment_name="default",
            status="published",
        )

    assert first["views"][0]["freshness"] == "fresh"
    assert second.freshness == "carried_forward"
    assert corpus_calls == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)::bigint
              FROM okg.derived_output_manifests
             WHERE definition_id = %s
               AND status = 'completed'
            """,
            (relation_definition_id,),
        )
        assert cur.fetchone()[0] == 1

        slot_read = read_view_slot(
            conn,
            deployment_name="default",
            view_id=view_id,
            slot_name="relation",
            generation_id=second_generation_id,
        )
        storage_ref = dict(slot_read.slot.storage_ref)
        assert storage_ref["source_manifest_id"] == str(
            slot_read.artifact.reused_from_manifest_id
        )
        upstream_manifest_id = UUID(str(storage_ref["upstream_manifest_id"]))
        rows = relation_visible_rows_for_manifest_id(
            conn,
            definition_id=relation_definition_id,
            manifest_id=upstream_manifest_id,
        )

    assert [row.row_data for row in rows] == [
        {"row_key": "summary-fragment", "title": "Summary"}
    ]


def test_native_view_empty_dirty_scope_carries_forward_after_delta(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    suffix = uuid4().hex[:12]
    view_id = f"test.publisher.native_view.empty_delta.{suffix}"
    planner_calls = 0
    allow_planner = True

    def planner(
        corpus: dict[str, Anchor],
        view_def: ViewDefinition,
    ) -> list[QuoteSpec]:
        nonlocal planner_calls
        del view_def
        planner_calls += 1
        if not allow_planner:
            raise AssertionError("empty dirty scope should carry forward")
        assert "summary" in corpus
        return [QuoteSpec(id="summary-fragment", anchor_id="summary")]

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=planner,
            assembler=_assembler,
            view_def_hash=stable_hash(
                {
                    "view_id": view_id,
                    "planner": "empty-delta-planner",
                }
            ),
        )

    def build_corpus(
        conn: psycopg.Connection,
        *,
        generation_id: int,
        delta_scope: ViewDeltaScope | None = None,
    ) -> dict[str, Anchor]:
        del conn, generation_id
        if delta_scope is not None:
            delta_scope.scoped = True
            return {}
        return {
            "summary": Anchor.from_content(
                "summary",
                "Initial corpus for the empty-delta carry-forward test.",
            )
        }

    materializer = ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
        reads_subtypes=("concept",),
    )
    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        lenses=[_lens_for_view(view_id)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert first["views"][0]["freshness"] == "fresh"
    assert planner_calls == 1

    allow_planner = False
    refreshable_manifest_calls = 0
    original_refreshable_manifest_ids = (
        native_view_stage_module._refreshable_manifest_ids
    )

    def counting_refreshable_manifest_ids(*args, **kwargs):
        nonlocal refreshable_manifest_calls
        refreshable_manifest_calls += 1
        return original_refreshable_manifest_ids(*args, **kwargs)

    monkeypatch.setattr(
        native_view_stage_module,
        "_refreshable_manifest_ids",
        counting_refreshable_manifest_ids,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog_version_id = conn.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'"
        ).fetchone()[0]
        outcomes = NativeViewStage(
            [materializer],
            deployment_name="default",
        ).run(
            conn,
            generation_id=int(first["compat_generation_id"]) + 1,
            catalog_version_id=catalog_version_id,
            reads_affected_view_ids=frozenset({view_id}),
            reads_deltas={
                view_id: (
                    {"concept": ["concept:changed-but-unscoped"]},
                    {},
                    True,
                    {},
                )
            },
        )

    assert len(outcomes) == 1
    assert outcomes[0].freshness == "carried_forward"
    assert outcomes[0].corpus_scan_scoped is True
    assert outcomes[0].dirty_scope_count == 0
    assert outcomes[0].timings_ms["delta_scope_empty_carry_forward"] == 1.0
    assert refreshable_manifest_calls == 1


def test_publisher_native_view_rejects_relation_without_row_identity(
    clean_runtime: str,
) -> None:
    view_id, materializer = _bad_relation_materializer()

    out = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        lenses=[_lens_for_view(view_id)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert out["status"] == "published"
    assert out["views"][0]["view_id"] == view_id
    assert "row identity column" in out["views"][0]["error"]
    assert out["lenses"][0]["lens_id"] == f"{view_id}.relation_editor"
    assert out["lenses"][0]["view_id"] == view_id
    assert out["lenses"][0]["error"] == "view_manifest_not_completed"

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.derived_artifact_definitions
             WHERE definition_id IN (%s, %s)
            """,
            (
                f"view:{view_id}",
                f"view-relation:{view_id}:relation",
            ),
        )
        definition_count = cur.fetchone()[0]
        cur.execute(
            """
            SELECT count(*)
              FROM okg.lens_definitions
             WHERE view_definition_id = %s
            """,
            (f"view:{view_id}",),
        )
        lens_definition_count = cur.fetchone()[0]

    assert definition_count == 0
    assert lens_definition_count == 0


def test_native_view_relation_dirty_scope_keys_compare_previous_rows(
    clean_runtime: str,
) -> None:
    definition = RelationArtifactDefinition(
        definition_id=f"view-relation:test.native.dirty.{uuid4().hex}:relation",
        deployment_name="default",
        logical_relation_id="test.native.dirty.rows",
        compute_hash=stable_hash({"compute": "v1"}),
        output_schema_hash=stable_hash({"columns": ["value"]}),
        renderer_hash=stable_hash({"renderer": "jsonb"}),
        dirty_rule_hash=stable_hash({"dirty": "source-scoped"}),
    )
    previous_rows = [
        RelationRow(scope_key="a", row_key="a", row_data={"value": 1}),
        RelationRow(scope_key="b", row_key="b", row_data={"value": 2}),
        RelationRow(scope_key="removed", row_key="removed", row_data={"value": 3}),
    ]
    current_rows = [
        RelationRow(scope_key="a", row_key="a", row_data={"value": 1}),
        RelationRow(scope_key="b", row_key="b", row_data={"value": 20}),
    ]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        publish_relation_artifact(
            conn,
            definition,
            target_generation_id=1,
            rows=previous_rows,
        )
        dirty_scope_keys = _relation_dirty_scope_keys(
            conn,
            definition=definition,
            current_rows=current_rows,
            target_generation_id=2,
        )

    assert dirty_scope_keys == ["b", "removed"]


def test_native_view_scoped_relation_ref_counts_carried_forward_rows(
    clean_runtime: str,
) -> None:
    definition = RelationArtifactDefinition(
        definition_id=f"view-relation:test.native.count.{uuid4().hex}:relation",
        deployment_name="default",
        logical_relation_id="test.native.count.rows",
        compute_hash=stable_hash({"compute": "v1"}),
        output_schema_hash=stable_hash({"columns": ["value"]}),
        renderer_hash=stable_hash({"renderer": "jsonb"}),
        dirty_rule_hash=stable_hash({"dirty": "source-scoped"}),
    )
    previous_rows = [
        RelationRow(scope_key="a", row_key="a", row_data={"value": 1}),
        RelationRow(scope_key="b", row_key="b", row_data={"value": 2}),
        RelationRow(scope_key="c", row_key="c", row_data={"value": 3}),
    ]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        publish_relation_artifact(
            conn,
            definition,
            target_generation_id=1,
            rows=previous_rows,
        )
        visible_count = _relation_visible_row_count_after_scoped_refresh(
            conn,
            definition=definition,
            target_generation_id=2,
            dirty_scope_keys=["b"],
            dirty_row_count=2,
        )

    assert visible_count == 4


def test_native_view_relation_rows_can_use_delta_scope_direct_rows(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex[:12]
    view_id = f"test.publisher.direct_relation_rows.{suffix}"
    template = ViewOutputTemplate.sql_relation(
        slot_name="items",
        logical_relation_id=f"{view_id}.items",
        columns=("scope_key", "row_key", "value"),
        sql="SELECT missing_column FROM relation_rows_should_not_execute",
        scope_key_column="scope_key",
        row_key_column="row_key",
        direct_rows_key="items",
    )
    view_def = ViewDefinition(
        view_id=view_id,
        planner=_planner,
        assembler=_assembler,
        view_def_hash=stable_hash({"view_id": view_id, "planner": "_planner"}),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = _relation_rows_for_template(
            conn=conn,
            template=template,
            view_def=view_def,
            deployment_name="default",
            target_generation_id=2,
            direct_relation_rows={
                "items": [
                    {
                        "scope_key": "scope:a",
                        "row_key": "row:1",
                        "value": 42,
                    }
                ]
            },
        )

    assert rows == [
        RelationRow(
            scope_key="scope:a",
            row_key="row:1",
            row_data={"scope_key": "scope:a", "row_key": "row:1", "value": 42},
        )
    ]


def test_publisher_native_view_projects_citation_edges(
    clean_runtime: str,
) -> None:
    view_id, materializer = _materializer()
    cited_node_id = "publication:native-view-source"
    materializer.anchor_resolver = lambda anchor_id: (
        cited_node_id if anchor_id == "summary" else None
    )
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

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(out["generation_id"])
        rows = graph.query(
            """
            SELECT dst, edge_type, attrs, source
              FROM okg.graph_edges
             WHERE src = :src
               AND edge_type = 'view_cites_node'
            """,
            {"src": f"view:{view_id}"},
        )
    finally:
        store.close()

    assert len(rows) == 1
    edge = rows[0]
    assert edge["dst"] == cited_node_id
    assert edge["edge_type"] == "view_cites_node"
    assert edge["attrs"]["anchor_id"] == "summary"
    assert edge["attrs"]["fragment_id"] == "summary-fragment"
    assert edge["attrs"]["view_def_id"] == view_id
    # Derived facts now carry the producing view definition as their source
    # (producer-id cutover a083b466b1), not the generic "_publisher_derived".
    assert edge["source"] == f"derived_artifact:view:{view_id}"


def test_publisher_native_view_reuses_unchanged_citation_edges(
    clean_runtime: str,
) -> None:
    view_id, materializer = _materializer()
    cited_node_id = "publication:native-view-source"
    materializer.anchor_resolver = lambda anchor_id: (
        cited_node_id if anchor_id == "summary" else None
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        _seed_publication_node(conn, cited_node_id)

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    second = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle(force_native_view_refresh=True)

    assert first["status"] == "published"
    assert second["status"] == "published"
    assert second["views"][0]["fragments_reused"] == 1
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.edge_facts
             WHERE edge_type = 'view_cites_node'
               AND source LIKE 'derived_artifact:%'
            """
        )
        assert cur.fetchone()[0] == 1

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(second["generation_id"])
        rows = graph.query(
            """
            SELECT dst, attrs
              FROM okg.graph_edges
             WHERE src = :src
               AND edge_type = 'view_cites_node'
            """,
            {"src": f"view:{view_id}"},
        )
    finally:
        store.close()

    assert [(row["dst"], row["attrs"]["anchor_id"]) for row in rows] == [
        (cited_node_id, "summary")
    ]


def test_publisher_native_view_scoped_refresh_keeps_clean_manifest_fragments(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex[:12]
    view_id = f"test.publisher.native_view.scoped_manifest.{suffix}"
    node_a = f"publication:native-view-scoped-a-{suffix}"
    node_b = f"publication:native-view-scoped-b-{suffix}"
    anchor_prefix = "file_symbols:"
    content_by_node = {
        node_a: "file a v1",
        node_b: "file b stable",
    }

    def planner(
        corpus: dict[str, Anchor],
        view_def: ViewDefinition,
    ) -> list[QuoteSpec]:
        del view_def
        return [
            QuoteSpec(id=anchor_id, anchor_id=anchor_id)
            for anchor_id in sorted(corpus)
        ]

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=planner,
            assembler=_assembler,
            view_def_hash=stable_hash(
                {"view_id": view_id, "planner": "scoped-manifest"}
            ),
            output_templates=(ViewOutputTemplate.native_json(),),
        )

    def build_corpus(
        conn: psycopg.Connection,
        *,
        generation_id: int,
        delta_scope: ViewDeltaScope | None = None,
    ) -> dict[str, Anchor]:
        del conn, generation_id
        node_ids = sorted(content_by_node)
        if delta_scope is not None:
            changed = set(delta_scope.changed_nodes.get("publication", ()))
            if changed:
                delta_scope.dirty_scope_keys.update(changed)
                delta_scope.scoped = True
                node_ids = sorted(changed)
        return {
            f"{anchor_prefix}{node_id}": Anchor.from_content(
                f"{anchor_prefix}{node_id}",
                content_by_node[node_id],
            )
            for node_id in node_ids
            if node_id in content_by_node
        }

    def resolver(anchor_id: str) -> str | None:
        if not anchor_id.startswith(anchor_prefix):
            return None
        return anchor_id[len(anchor_prefix):]

    materializer = ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
        anchor_resolver=resolver,
        reads_subtypes=("publication",),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        _seed_publication_node(conn, node_a)
        _seed_publication_node(conn, node_b)

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    content_by_node[node_a] = "file a v2"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        from okg.substrate.library.sources.base import NodeFact

        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id=node_a,
                    subtype="publication",
                    attrs={"changed": True},
                    source_record_id={"id": node_a},
                    source_revision={"v": 2},
                )
            ],
            source="test.publications",
        )

    second = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert first["status"] == "published"
    assert second["status"] == "published"
    assert second["views"][0]["fragments_total"] == 2
    assert second["views"][0]["fragments_reused"] == 1
    assert second["views"][0]["fragments_refreshed"] == 1

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(second["generation_id"])
        rows = graph.query(
            """
            SELECT dst, attrs
              FROM okg.graph_edges
             WHERE src = :src
               AND edge_type = 'view_cites_node'
             ORDER BY dst
            """,
            {"src": f"view:{view_id}"},
        )
    finally:
        store.close()

    assert [(row["dst"], row["attrs"]["anchor_id"]) for row in rows] == [
        (node_a, f"{anchor_prefix}{node_a}"),
        (node_b, f"{anchor_prefix}{node_b}"),
    ]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        slot_read = read_view_slot(
            conn,
            deployment_name="default",
            view_id=view_id,
            slot_name="json",
        )
    payload = slot_read.slot.payload_value()
    fragment_ids = [
        fragment["id"]
        for fragment in payload["view_manifest"]["fragments"]
    ]
    assert fragment_ids == [
        f"{anchor_prefix}{node_a}",
        f"{anchor_prefix}{node_b}",
    ]


def test_publisher_native_view_projects_changed_citation_fragments_only(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex[:12]
    view_id = f"test.publisher.native_view.partial_cites.{suffix}"
    include_extra = True
    cited_by_anchor = {
        "summary": "publication:native-view-partial-cites-1",
        "extra": "publication:native-view-partial-cites-2",
    }

    def planner(
        corpus: dict[str, Anchor],
        view_def: ViewDefinition,
    ) -> list[QuoteSpec]:
        del view_def
        return [
            QuoteSpec(id=f"{anchor_id}-fragment", anchor_id=anchor_id)
            for anchor_id in sorted(corpus)
        ]

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=planner,
            assembler=_assembler,
            view_def_hash=stable_hash(
                {"view_id": view_id, "planner": "partial-cites"}
            ),
        )

    def build_corpus(
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> dict[str, Anchor]:
        del conn, generation_id
        corpus = {
            "summary": Anchor.from_content("summary", "stable citation"),
        }
        if include_extra:
            corpus["extra"] = Anchor.from_content("extra", "dropped citation")
        return corpus

    materializer = ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
        anchor_resolver=lambda anchor_id: cited_by_anchor.get(anchor_id),
        reads_subtypes=("publication",),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        for node_id in cited_by_anchor.values():
            _seed_publication_node(conn, node_id)

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    include_extra = False
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication_node(
            conn,
            f"publication:native-view-partial-cites-trigger-{suffix}",
        )
    second = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert first["status"] == "published"
    assert second["status"] == "published"
    view = second["views"][0]
    assert view["fragments_reused"] == 1
    assert view["fragments_dropped"] == 1
    timing_view = next(
        item
        for item in second["timing_details"]["native_views"]
        if item["view_id"] == view_id
    )
    assert timing_view["timings_ms"]["view_cites.fragment_delta_count"] == 1.0

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT op, count(*)
              FROM okg.edge_facts
             WHERE edge_type = 'view_cites_node'
               AND source LIKE 'derived_artifact:%'
             GROUP BY op
             ORDER BY op
            """
        )
        assert cur.fetchall() == [("I", 2), ("R", 1)]

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(second["generation_id"])
        rows = graph.query(
            """
            SELECT dst, attrs
              FROM okg.graph_edges
             WHERE src = :src
               AND edge_type = 'view_cites_node'
             ORDER BY dst
            """,
            {"src": f"view:{view_id}"},
        )
    finally:
        store.close()

    assert [(row["dst"], row["attrs"]["anchor_id"]) for row in rows] == [
        (cited_by_anchor["summary"], "summary")
    ]


def test_publisher_native_view_keeps_reused_citation_when_sibling_changes(
    clean_runtime: str,
) -> None:
    """Regression (PR #448 review): a warm publish where ONE fragment changes
    its citation while a sibling fragment is reused must NOT retract the reused
    fragment's citation.

    The native-view stage emits only the delta (an ``op="I"`` for the changed
    fragment plus an ``op="R"`` for its old target); the reused fragment emits
    nothing. A publisher-side safety net that rebuilt the view's "desired" set
    from only that partial insert set then retracted every OTHER live citation
    edge -- silently deleting the reused fragment's valid ``view_cites_node``
    edge. Unlike ``..._projects_changed_citation_fragments_only`` (which only
    DROPS a fragment, so the cycle emits no ``op="I"`` and the net no-ops), this
    exercises the changed-plus-reused shape that actually triggered the bug.
    """
    suffix = uuid4().hex[:12]
    view_id = f"test.publisher.native_view.reused_sibling.{suffix}"
    stable_node = f"publication:native-view-reused-stable-{suffix}"
    dynamic_old = f"publication:native-view-reused-dyn-old-{suffix}"
    dynamic_new = f"publication:native-view-reused-dyn-new-{suffix}"

    dynamic_content = ["v1"]
    dynamic_target = [dynamic_old]

    def planner(
        corpus: dict[str, Anchor],
        view_def: ViewDefinition,
    ) -> list[QuoteSpec]:
        del view_def
        return [
            QuoteSpec(id=f"{anchor_id}-fragment", anchor_id=anchor_id)
            for anchor_id in sorted(corpus)
        ]

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=planner,
            assembler=_assembler,
            view_def_hash=stable_hash(
                {"view_id": view_id, "planner": "reused-sibling"}
            ),
        )

    def build_corpus(
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> dict[str, Anchor]:
        del conn, generation_id
        # 'summary' is byte-identical across cycles (reused); 'dynamic' changes.
        return {
            "summary": Anchor.from_content("summary", "stable citation"),
            "dynamic": Anchor.from_content("dynamic", dynamic_content[0]),
        }

    materializer = ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
        anchor_resolver=lambda anchor_id: (
            stable_node if anchor_id == "summary" else dynamic_target[0]
        ),
        reads_subtypes=("publication",),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        for node_id in (stable_node, dynamic_old, dynamic_new):
            _seed_publication_node(conn, node_id)

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    # Change ONLY the dynamic fragment (its content re-projects it, and its
    # resolved citation moves old -> new). 'summary' stays byte-identical.
    dynamic_content[0] = "v2"
    dynamic_target[0] = dynamic_new
    # Seed a new publication so the view's dirty scope is non-empty and it
    # re-materializes this cycle (as the sibling native-view cite tests do).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication_node(
            conn,
            f"publication:native-view-reused-trigger-{suffix}",
        )
    second = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert first["status"] == "published"
    assert second["status"] == "published"
    assert second["views"][0]["fragments_reused"] == 1

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(second["generation_id"])
        rows = graph.query(
            """
            SELECT dst, attrs
              FROM okg.graph_edges
             WHERE src = :src
               AND edge_type = 'view_cites_node'
             ORDER BY dst
            """,
            {"src": f"view:{view_id}"},
        )
    finally:
        store.close()

    # The reused 'summary' citation MUST survive; only 'dynamic' moves to its
    # new target. With the over-retraction bug, 'summary' is missing here.
    assert [(row["dst"], row["attrs"]["anchor_id"]) for row in rows] == sorted(
        [(stable_node, "summary"), (dynamic_new, "dynamic")]
    )


def test_publisher_native_view_retracts_stale_citation_edges(
    clean_runtime: str,
) -> None:
    view_id, materializer = _materializer()
    materializer.anchor_resolver = _resolver_one
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        _seed_publication_node(conn, "publication:native-view-source-1")
        _seed_publication_node(conn, "publication:native-view-source-2")

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    materializer.anchor_resolver = _resolver_two
    second = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert first["status"] == "published"
    assert second["status"] == "published"

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        first_graph = store.checkout_generation(first["generation_id"])
        second_graph = store.checkout_generation(second["generation_id"])
        first_rows = first_graph.query(
            """
            SELECT dst
              FROM okg.graph_edges
             WHERE src = :src
               AND edge_type = 'view_cites_node'
             ORDER BY dst
            """,
            {"src": f"view:{view_id}"},
        )
        second_rows = second_graph.query(
            """
            SELECT dst
              FROM okg.graph_edges
             WHERE src = :src
               AND edge_type = 'view_cites_node'
             ORDER BY dst
            """,
            {"src": f"view:{view_id}"},
        )
    finally:
        store.close()

    assert [row["dst"] for row in first_rows] == [
        "publication:native-view-source-1",
    ]
    assert [row["dst"] for row in second_rows] == [
        "publication:native-view-source-2",
    ]


def test_blocked_generation_does_not_publish_native_view_manifest(
    clean_runtime: str,
) -> None:
    view_id, materializer = _materializer()

    out = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        lenses=[_lens_for_view(view_id)],
        consistency_gates=[BlockingGate()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert out["status"] == "blocked"
    assert out["phase_b_blocked"] is True
    assert out["views"] == []
    assert out["lenses"] == []

    definition_id = f"view:{view_id}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.derived_artifact_definitions
             WHERE definition_id = %s
            """,
            (definition_id,),
        )
        definition_count = cur.fetchone()[0]
        cur.execute(
            """
            SELECT count(*)
              FROM okg.lens_definitions
             WHERE view_definition_id = %s
            """,
            (definition_id,),
        )
        lens_definition_count = cur.fetchone()[0]

    # Chronos cutover: a blocked cycle publishes nothing, so no `view` node
    # may appear in the canonical graph. The cycle was blocked (no published
    # generation checkpoint), so read the default-branch checkout instead of
    # the dropped okg.nodes_live.
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        view_node_count = store.checkout().query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = :node_id
               AND subtype = 'view'
            """,
            {"node_id": f"view:{view_id}"},
        )[0]["n"]
    finally:
        store.close()

    assert definition_count == 0
    assert lens_definition_count == 0
    assert view_node_count == 0


def test_publisher_native_view_definition_hash_change_invalidates_cache(
    clean_runtime: str,
) -> None:
    view_id, materializer = _materializer()

    first = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    def changed_view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=_planner,
            assembler=_assembler,
            params={"audience": "operator", "revision": 2},
            view_def_hash=stable_hash(
                {
                    "view_id": view_id,
                    "planner": "_planner",
                    "params": {"audience": "operator", "revision": 2},
                }
            ),
            display_name="Native Publisher View",
        )

    materializer.view_def_factory = changed_view_def_factory
    second = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert first["views"][0]["fragments_added"] == 1
    assert first["views"][0]["fragments_reused"] == 0
    assert second["views"][0]["fragments_added"] == 1
    assert second["views"][0]["fragments_reused"] == 0


def _stale_column_materializer(view_id: str) -> ViewMaterializer:
    """A deployment view module that reads a column the schema removed.

    This is okg-workspace's `today_brief` after #1153 dropped
    `valid_from_gen` from `okg.v_nodes`, reduced to its essentials: the
    module imports fine and its definition is well-formed, and it raises
    inside `build_corpus` on every single publish.
    """

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=_planner,
            assembler=_assembler,
            view_def_hash=stable_hash(
                {"view_id": view_id, "planner": "stale-column-planner"}
            ),
        )

    def build_corpus(
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> dict[str, Anchor]:
        del generation_id
        with conn.cursor() as cur:
            cur.execute("SELECT valid_from_gen FROM okg.v_nodes LIMIT 1")
            cur.fetchall()
        raise AssertionError("unreachable: the column does not exist")

    return ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
    )


def _healthy_materializer(view_id: str) -> ViewMaterializer:
    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=_planner,
            assembler=_assembler,
            view_def_hash=stable_hash(
                {"view_id": view_id, "planner": "healthy-planner"}
            ),
        )

    def build_corpus(
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> dict[str, Anchor]:
        del conn, generation_id
        return {
            "summary": Anchor.from_content("summary", "A healthy corpus."),
        }

    return ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
    )


def test_broken_deployment_view_module_reds_the_stage_in_publish_output(
    clean_runtime: str,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A view module that raises must be VISIBLE in the publish result.

    The regression: `NativeViewStage.run` isolates per view and logged the
    traceback, but the publish result still said
    `native_view_refresh.status == "materialized"` with no failure count,
    so a view raising on every cycle read as a clean publish. Isolation is
    kept (views are non-gating; a dashboard must not prevent the admitted graph
    checkpoint) — what changes is that the STAGE reports degraded.
    """
    suffix = uuid4().hex[:12]
    broken_id = f"test.publisher.native_view.stale_column.{suffix}"
    healthy_id = f"test.publisher.native_view.healthy.{suffix}"
    phase_events: list[tuple[str, dict[str, object]]] = []

    def capture_phase_event(
        _timings_ms: dict[str, object],
        name: str,
        payload: dict[str, object],
    ) -> None:
        phase_events.append((name, payload))

    monkeypatch.setattr(
        "okg.substrate.publisher.publisher._emit_phase_event_with_timing",
        capture_phase_event,
    )

    logger_name = "okg.substrate.publisher.native_view_stage"
    with caplog.at_level("ERROR", logger=logger_name):
        out = Publisher(
            clean_runtime,
            "default",
            materializers=[
                _stale_column_materializer(broken_id),
                _healthy_materializer(healthy_id),
            ],
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()

    # Isolation held: the generation published and the healthy view ran.
    assert out["status"] == "published"
    by_view = {row["view_id"]: row for row in out["views"]}
    assert by_view[broken_id]["freshness"] == "failed"
    assert by_view[healthy_id]["freshness"] == "fresh"

    # ...and the STAGE is red in the publish output, not merely one row.
    refresh = out["native_view_refresh"]
    assert refresh["status"] == "materialized_with_failures"
    assert refresh["failed_view_count"] == 1
    assert [f["view_id"] for f in refresh["failed_views"]] == [broken_id]
    assert "valid_from_gen" in refresh["failed_views"][0]["error"]

    # The durable phase event must carry the same degraded-stage signal as
    # the in-process return value. Otherwise the cycle looks healthy after
    # its caller exits even though the publish result was honest.
    view_events = [
        payload
        for name, payload in phase_events
        if name == "post_publish_views_complete"
    ]
    assert len(view_events) == 1, phase_events
    assert view_events[0]["view_count"] == 2
    assert view_events[0]["failed_view_count"] == 1
    assert view_events[0]["failed_view_ids"] == [broken_id]

    # ...and it said so once, at ERROR, naming the stage and the view.
    stage_errors = [
        record.getMessage()
        for record in caplog.records
        if record.levelname == "ERROR"
        and "native view stage DEGRADED" in record.getMessage()
    ]
    assert len(stage_errors) == 1, stage_errors
    assert broken_id in stage_errors[0]


def test_broken_deployment_view_isolated_on_caller_owned_transaction(
    clean_runtime: str,
) -> None:
    """Failure isolation must survive Publisher.cycle(conn=...).

    Merge-back and other production callers pass a normal psycopg connection,
    whose default is ``autocommit=False``.  Invalid SQL aborts that transaction;
    the stage therefore needs a per-view savepoint, not only an exception
    handler, to let the healthy view and generation checkpoint proceed.
    """
    suffix = uuid4().hex[:12]
    broken_id = f"test.publisher.native_view.tx_stale_column.{suffix}"
    healthy_id = f"test.publisher.native_view.tx_healthy.{suffix}"

    with psycopg.connect(clean_runtime) as conn:
        assert conn.autocommit is False
        out = Publisher(
            clean_runtime,
            "default",
            materializers=[
                _stale_column_materializer(broken_id),
                _healthy_materializer(healthy_id),
            ],
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle(conn=conn)

    assert out["status"] == "published"
    by_view = {row["view_id"]: row for row in out["views"]}
    assert by_view[broken_id]["freshness"] == "failed"
    assert by_view[healthy_id]["freshness"] == "fresh"
    refresh = out["native_view_refresh"]
    assert refresh["status"] == "materialized_with_failures"
    assert refresh["failed_view_count"] == 1
    assert [row["view_id"] for row in refresh["failed_views"]] == [broken_id]


def test_healthy_view_stage_is_not_reported_as_degraded(
    clean_runtime: str,
) -> None:
    """Non-vacuity for the check above: with no broken module the same
    fields report a clean stage, so `materialized_with_failures` is a real
    signal rather than a constant."""
    view_id = f"test.publisher.native_view.clean.{uuid4().hex[:12]}"

    out = Publisher(
        clean_runtime,
        "default",
        materializers=[_healthy_materializer(view_id)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert out["status"] == "published"
    refresh = out["native_view_refresh"]
    assert refresh["status"] == "materialized"
    assert refresh["failed_view_count"] == 0
    assert refresh["failed_views"] == []
    assert refresh["view_stage_ran"] is True


def test_view_refresh_report_distinguishes_no_failures_from_no_run(
    clean_runtime: str,
) -> None:
    """The failure fields are present on EVERY refresh shape, and a zero
    from a stage that did not run is distinguishable from a zero from a
    stage that ran clean.

    Five code paths assign `_last_native_view_refresh` and only the
    materialized one knows about failures. A consumer reading
    `failed_view_count` would KeyError on the rest, and one defaulting it
    to 0 would read a coalesced or disabled cycle as healthy — absence
    dressed as agreement, which is the defect class this change closes.
    """
    out = Publisher(clean_runtime, "default", materializers=[], catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    assert out["status"] == "published"
    refresh = out["native_view_refresh"]
    assert refresh["status"] == "disabled"
    # Present, not absent...
    assert refresh["failed_view_count"] == 0
    assert refresh["failed_views"] == []
    # ...and honest about what that zero means.
    assert refresh["view_stage_ran"] is False


# --- PACT view-output-carry-forward-3981: reuse needs proof ------------------
#
# A fleet summary view over host attributes, run through Publisher.cycle. The
# view is undeclared unless a test says otherwise, so it is assumed to read any
# graph type; nothing outside the graph is read unless a test declares it.


def _fleet_catalog(
    conn: psycopg.Connection,
    *,
    live_relation: str | None = None,
) -> int:
    """An applied catalog version with host and rack subtypes.

    ``live_relation`` declares that live relation on ``host``.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        for subtype in ("host", "rack", "view"):
            metadata = (
                {
                    "live_relations": {
                        live_relation: {
                            "action": "host_now",
                            "principal_mode": "service",
                            "deadline_seconds": 5.0,
                            "retention_class": "warm",
                        }
                    }
                }
                if live_relation and subtype == "host"
                else {}
            )
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id, metadata) "
                "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (subtype, cv, psycopg.types.json.Jsonb(metadata)),
            )
            cur.execute(
                """
                INSERT INTO okg.subtype_ancestors
                  (catalog_version_id, subtype_name, ancestor_name, depth)
                VALUES (%s, %s, %s, 0)
                ON CONFLICT DO NOTHING
                """,
                (cv, subtype, subtype),
            )
        cur.execute(
            "INSERT INTO okg.narrowings (parent_subtype, edge_type, child_subtype, "
            "catalog_version_id) VALUES ('host', 'in_rack', 'rack', %s) "
            "ON CONFLICT DO NOTHING",
            (cv,),
        )
    return int(cv)


def _fleet_set(conn: psycopg.Connection, subtype: str, name: str, **attrs) -> None:
    """Write (or change) one host or rack node."""
    from okg.substrate.library.sources.base import NodeFact

    revision = uuid4().hex
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(
                node_id=f"{subtype}:{name}",
                subtype=subtype,
                attrs=dict(attrs),
                source_record_id={"id": f"{subtype}:{name}"},
                source_revision={"v": revision},
            )
        ],
        source="test.fleet",
    )


class _FleetView:
    """A fleet summary view and the calls its corpus received."""

    def __init__(
        self,
        *,
        reads_subtypes: tuple[str, ...] = (),
        external_inputs: tuple[str, ...] = (),
        takes_published_at: bool = False,
        sample_relation: str | None = None,
    ) -> None:
        self.view_id = f"test.publisher.fleet.{uuid4().hex[:12]}"
        self.corpus_calls: list[int] = []
        self.published_ats: list[datetime] = []
        view_id = self.view_id
        calls = self.corpus_calls
        stamps = self.published_ats

        def view_def_factory() -> ViewDefinition:
            return ViewDefinition(
                view_id=view_id,
                planner=_planner,
                assembler=_assembler,
                view_def_hash=stable_hash({"view_id": view_id, "fleet": 1}),
            )

        def summary(conn: psycopg.Connection, generation_id: int) -> str:
            calls.append(generation_id)
            rows = ChronosGraphStore.from_connection(
                conn, ensure_schema=False
            ).checkout().query(
                "SELECT attrs FROM okg.graph_nodes WHERE subtype = :subtype "
                "ORDER BY node_id",
                {"subtype": "host"},
            )
            cpus = sum(int((row["attrs"] or {}).get("cpus", 0)) for row in rows)
            text = f"hosts={len(rows)} cpus={cpus}"
            if sample_relation:
                load = conn.execute(
                    "SELECT coalesce(sum((payload->>'load')::int), 0) FROM ("
                    "  SELECT DISTINCT ON (node_id) payload"
                    "    FROM okg.call_sample_warm"
                    "   WHERE relation = %s AND status = 'ok'"
                    "   ORDER BY node_id, observed_at DESC) newest",
                    (sample_relation,),
                ).fetchone()[0]
                text += f" load={load}"
            return text

        if takes_published_at:

            def build_corpus(
                conn: psycopg.Connection,
                *,
                generation_id: int,
                published_at: datetime,
            ) -> dict[str, Anchor]:
                stamps.append(published_at)
                return {
                    "summary": Anchor.from_content(
                        "summary",
                        f"{summary(conn, generation_id)} "
                        f"at={published_at.isoformat()}",
                    )
                }

        else:

            def build_corpus(
                conn: psycopg.Connection,
                *,
                generation_id: int,
            ) -> dict[str, Anchor]:
                return {
                    "summary": Anchor.from_content(
                        "summary", summary(conn, generation_id)
                    )
                }

        self.materializer = ViewMaterializer(
            view_def_factory=view_def_factory,
            build_corpus=build_corpus,
            name=view_id,
            reads_subtypes=reads_subtypes,
            **({"external_inputs": external_inputs} if external_inputs else {}),
        )

    def cycle(self, dsn: str, *, force: bool = False):
        return Publisher(
            dsn,
            "default",
            materializers=[self.materializer],
            catalog_identity=bind_test_catalog_writer(dsn, "default"),
        ).cycle(
            native_view_refresh_final=True,
            force_native_view_refresh=force,
        )

    def forced(self, dsn: str):
        return self.cycle(dsn, force=True)

    def read(self, dsn: str):
        with psycopg.connect(dsn, autocommit=True) as conn:
            markdown = read_view_slot(
                conn,
                deployment_name="default",
                view_id=self.view_id,
                slot_name="markdown",
            )
            return markdown.artifact, markdown.slot.payload_value()


def _fleet_seed(dsn: str, **hosts: int) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        cv = _fleet_catalog(conn)
        for name, cpus in hosts.items():
            _fleet_set(conn, "host", name, cpus=cpus)
    return cv


def _freshness(result: dict) -> str:
    return result["views"][0]["freshness"]


def test_undeclared_fleet_view_rebuilds_after_a_host_attribute_changes(
    clean_runtime: str,
) -> None:
    view = _FleetView()
    _fleet_seed(clean_runtime, a=4, b=8)

    first = view.cycle(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _fleet_set(conn, "host", "a", cpus=16)
    second = view.cycle(clean_runtime)

    artifact, markdown = view.read(clean_runtime)
    assert _freshness(first) == "fresh"
    assert _freshness(second) == "fresh"
    assert "cpus=24" in markdown
    assert artifact.target_generation_id == second["compat_generation_id"]
    assert artifact.manifest_metadata["inputs_valid_through_generation"] == (
        second["compat_generation_id"]
    )


def test_undeclared_fleet_view_is_carried_on_a_forced_generation_without_graph_change(
    clean_runtime: str,
) -> None:
    view = _FleetView()
    _fleet_seed(clean_runtime, a=4, b=8)

    first = view.cycle(clean_runtime)
    calls_after_first = len(view.corpus_calls)
    forced = view.forced(clean_runtime)

    artifact, markdown = view.read(clean_runtime)
    assert forced["status"] == "published"
    assert forced["compat_generation_id"] != first["compat_generation_id"]
    assert _freshness(forced) == "carried_forward"
    assert len(view.corpus_calls) == calls_after_first
    assert "cpus=12" in markdown
    assert artifact.target_generation_id == forced["compat_generation_id"]
    assert artifact.manifest_metadata["inputs_valid_through_generation"] == (
        forced["compat_generation_id"]
    )
    # the carry names the first build as the origin of its content
    assert artifact.manifest_metadata["origin"]["generation_id"] == (
        first["compat_generation_id"]
    )


def test_fleet_view_is_rebuilt_on_a_generation_cut_by_a_catalog_change_alone(
    clean_runtime: str,
) -> None:
    view = _FleetView()
    _fleet_seed(clean_runtime, a=4)

    first = view.cycle(clean_runtime)
    calls_after_first = len(view.corpus_calls)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _fleet_catalog(conn)  # a new applied catalog version, no graph change
    second = view.forced(clean_runtime)

    assert _freshness(first) == "fresh"
    assert second["compat_generation_id"] != first["compat_generation_id"]
    assert _freshness(second) != "carried_forward"
    assert len(view.corpus_calls) == calls_after_first + 1


def test_declared_reads_view_is_carried_until_a_type_it_reads_changes(
    clean_runtime: str,
) -> None:
    view = _FleetView(reads_subtypes=("host",))
    _fleet_seed(clean_runtime, a=4)

    first = view.cycle(clean_runtime)
    calls_after_first = len(view.corpus_calls)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = conn.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'"
        ).fetchone()[0]
    second_generation = int(first["compat_generation_id"]) + 1
    third_generation = second_generation + 1
    stage = NativeViewStage([view.materializer], deployment_name="default")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # a generation whose change set touched only a type the view does not
        # read: no declared read is affected
        other_type = stage.run(
            conn, generation_id=second_generation, catalog_version_id=cv,
        )[0]
        calls_after_other_type = len(view.corpus_calls)
        ChronosGraphStore.from_connection(conn).checkpoint_generation(
            second_generation,
            deployment_name="default",
            status="published",
            catalog_version_id=cv,
        )
        _fleet_set(conn, "host", "a", cpus=32)
        host_changed = stage.run(
            conn,
            generation_id=third_generation,
            catalog_version_id=cv,
            reads_affected_view_ids=frozenset({view.view_id}),
            reads_deltas={
                view.view_id: ({"host": ["host:a"]}, {}, True, {}, {})
            },
        )[0]

    assert other_type.freshness == "carried_forward"
    assert calls_after_other_type == calls_after_first
    assert host_changed.freshness == "fresh"
    assert len(view.corpus_calls) == calls_after_first + 1


@pytest.mark.parametrize("reads", [(), ("host",)], ids=["undeclared", "declared"])
def test_view_is_rebuilt_after_a_generation_it_did_not_run_for(
    clean_runtime: str,
    reads: tuple[str, ...],
) -> None:
    """Built at generation 1; the view is coalesced at generation 2 while a
    host changes; nothing changes at generation 3. The change set of
    generation 3 is empty, but the view's last output is two generations old,
    so it must be rebuilt."""
    view = _FleetView(reads_subtypes=reads)
    _fleet_seed(clean_runtime, a=4)

    first = view.cycle(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _fleet_set(conn, "host", "a", cpus=64)
    coalesced = Publisher(
        clean_runtime,
        "default",
        materializers=[view.materializer],
        native_view_materialization_delay_ms=60_000,
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle(native_view_refresh_final=False)
    third = view.forced(clean_runtime)

    artifact, markdown = view.read(clean_runtime)
    assert coalesced["native_view_refresh"]["status"] == "coalesced"
    assert coalesced["compat_generation_id"] != first["compat_generation_id"]
    assert third["compat_generation_id"] != coalesced["compat_generation_id"]
    assert _freshness(third) != "carried_forward"
    assert "cpus=64" in markdown
    assert artifact.target_generation_id == third["compat_generation_id"]


def test_published_at_aware_view_shows_the_second_published_at(
    clean_runtime: str,
) -> None:
    view = _FleetView(takes_published_at=True)
    _fleet_seed(clean_runtime, a=4)

    first = view.cycle(clean_runtime)
    _, first_markdown = view.read(clean_runtime)
    second = view.forced(clean_runtime)
    artifact, second_markdown = view.read(clean_runtime)

    assert _freshness(second) != "carried_forward"
    assert len(view.published_ats) == 2
    assert view.published_ats[0] != view.published_ats[1]
    assert f"at={view.published_ats[0].isoformat()}" in first_markdown
    assert f"at={view.published_ats[1].isoformat()}" in second_markdown
    assert artifact.target_generation_id == second["compat_generation_id"]
    assert artifact.manifest_metadata["external_inputs_state"]["publish_time"] == {
        "as_of": view.published_ats[1].isoformat()
    }
    assert first["compat_generation_id"] != second["compat_generation_id"]


def _live_sample(
    conn: psycopg.Connection,
    node_id: str,
    *,
    status: str = "ok",
    load: int = 0,
    observed_at: datetime,
    relation: str = "host_now",
) -> None:
    conn.execute(
        """
        INSERT INTO okg.call_sample_warm (
          deployment_name, node_id, relation, observed_at, status,
          principal, decision, trace_id, session_id, policy, declared_policy,
          payload, payload_bytes
        ) VALUES (
          'default', %s, %s, %s, %s, 'svc', 'allow', 't', 's', 'inline',
          'inline', %s, 10
        )
        """,
        (
            node_id,
            relation,
            observed_at,
            status,
            psycopg.types.json.Jsonb({"load": load}),
        ),
    )


def test_live_relation_view_rebuilds_on_a_forced_refresh_after_a_new_sample(
    clean_runtime: str,
) -> None:
    view = _FleetView(
        external_inputs=("live:host_now",), sample_relation="host_now"
    )
    base = datetime.now(UTC) - timedelta(hours=1)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _fleet_catalog(conn, live_relation="host_now")
        _fleet_set(conn, "host", "a", cpus=4)
        _live_sample(conn, "host:a", load=1, observed_at=base)

    first = view.cycle(clean_runtime)
    _, first_markdown = view.read(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _live_sample(
            conn, "host:a", load=5, observed_at=base + timedelta(minutes=30)
        )
    second = view.forced(clean_runtime)
    artifact, second_markdown = view.read(clean_runtime)

    assert "load=1" in first_markdown
    assert _freshness(first) == "fresh"
    assert _freshness(second) == "fresh"
    assert "load=5" in second_markdown
    assert artifact.target_generation_id == second["compat_generation_id"]
    state = artifact.manifest_metadata["external_inputs_state"]["live:host_now"]
    assert state["targets"] == 1 and state["failed"] == 0
    assert artifact.manifest_metadata["inputs_as_of"] == state["as_of"]


def test_predecessor_generation_is_per_branch(clean_runtime: str) -> None:
    """A generation published on another branch is never the predecessor of a
    default-branch generation, so its manifests are never carried from."""
    from okg.substrate.publisher.native_view_stage import _predecessor_generation

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _fleet_catalog(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.checkpoint_generation(
            1_001, branch="default", deployment_name="default",
            catalog_version_id=cv, metadata={"target_generation_id": 1_001},
        )
        store.create_branch("experiment:x")
        store.checkpoint_generation(
            1_002, branch="experiment:x", deployment_name="default",
            catalog_version_id=cv, metadata={"target_generation_id": 1_002},
        )
        default = _predecessor_generation(
            conn, deployment_name="default", branch_name="default",
            generation_id=1_003,
        )
        branch = _predecessor_generation(
            conn, deployment_name="default", branch_name="experiment:x",
            generation_id=1_003,
        )
        none_yet = _predecessor_generation(
            conn, deployment_name="default", branch_name="default",
            generation_id=1_001,
        )

    assert default is not None and default.generation_id == 1_001
    assert default.catalog_version_id == cv
    assert branch is not None and branch.generation_id == 1_002
    assert none_yet is None


# --- PACT view-output-carry-forward-3981: what a manifest records ------------


def _utc(minutes_ago: int) -> datetime:
    return (datetime.now(UTC) - timedelta(minutes=minutes_ago)).replace(
        microsecond=0
    )


def test_inputs_recorded_origin_and_valid_through_follow_the_carry_chain(
    clean_runtime: str,
) -> None:
    """Built at generation 1, carried at 2 and 3: manifest 3 names the
    generation-1 origin and is valid through generation 3."""
    view = _FleetView()
    _fleet_seed(clean_runtime, a=4)

    first = view.cycle(clean_runtime)
    second = view.forced(clean_runtime)
    third = view.forced(clean_runtime)

    artifact, _ = view.read(clean_runtime)
    metadata = artifact.manifest_metadata
    assert _freshness(second) == "carried_forward"
    assert _freshness(third) == "carried_forward"
    assert artifact.target_generation_id == third["compat_generation_id"]
    assert metadata["inputs_valid_through_generation"] == (
        third["compat_generation_id"]
    )
    assert metadata["origin"]["generation_id"] == first["compat_generation_id"]
    assert "external_inputs_state" not in metadata
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first_manifest = conn.execute(
            "SELECT manifest_id FROM okg.derived_output_manifests "
            "WHERE definition_id = %s AND target_generation_id = %s",
            (f"view:{view.view_id}", first["compat_generation_id"]),
        ).fetchone()[0]
    assert metadata["origin"]["manifest_id"] == str(first_manifest)


def _live_view_with_hosts(dsn: str, hosts: dict[str, int]) -> _FleetView:
    view = _FleetView(
        external_inputs=("live:host_now",), sample_relation="host_now"
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        _fleet_catalog(conn, live_relation="host_now")
        for name, cpus in hosts.items():
            _fleet_set(conn, "host", name, cpus=cpus)
    return view


def _live_state(view: _FleetView, dsn: str) -> tuple[dict, str | None]:
    artifact, _ = view.read(dsn)
    return (
        artifact.manifest_metadata["external_inputs_state"]["live:host_now"],
        artifact.manifest_metadata["inputs_as_of"],
    )


def test_inputs_recorded_live_inputs_as_of_is_the_oldest_newest_sample(
    clean_runtime: str,
) -> None:
    view = _live_view_with_hosts(clean_runtime, {"a": 1, "b": 1, "c": 1})
    t1, t2, t3 = _utc(50), _utc(30), _utc(10)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # an older sample per host must not count: only the newest one does
        _live_sample(conn, "host:a", observed_at=_utc(90))
        _live_sample(conn, "host:a", observed_at=t1)
        _live_sample(conn, "host:b", observed_at=t2)
        _live_sample(conn, "host:c", observed_at=t3)

    view.cycle(clean_runtime)

    state, as_of = _live_state(view, clean_runtime)
    assert datetime.fromisoformat(as_of) == t1
    assert datetime.fromisoformat(state["as_of"]) == t1
    assert (state["targets"], state["with_success"]) == (3, 3)
    assert (state["missing"], state["failed"]) == (0, 0)


def test_inputs_recorded_counts_failed_and_missing_targets(
    clean_runtime: str,
) -> None:
    view = _live_view_with_hosts(clean_runtime, {"a": 1, "b": 1, "c": 1})
    t1 = _utc(40)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _live_sample(conn, "host:a", observed_at=t1)
        _live_sample(conn, "host:b", status="error", observed_at=_utc(20))
        # host:c has no sample at all

    view.cycle(clean_runtime)

    state, as_of = _live_state(view, clean_runtime)
    assert datetime.fromisoformat(as_of) == t1
    assert (state["targets"], state["with_success"]) == (3, 1)
    assert state["failed"] == 1
    assert state["missing"] == 1


def test_inputs_recorded_forced_rebuild_over_failed_samples_keeps_the_old_time(
    clean_runtime: str,
) -> None:
    view = _live_view_with_hosts(clean_runtime, {"a": 1, "b": 1})
    t1, t2 = _utc(60), _utc(50)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _live_sample(conn, "host:a", observed_at=t1)
        _live_sample(conn, "host:b", observed_at=t2)
    view.cycle(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _live_sample(conn, "host:a", status="timeout", observed_at=_utc(5))
        _live_sample(conn, "host:b", status="error", observed_at=_utc(4))

    view.forced(clean_runtime)

    state, as_of = _live_state(view, clean_runtime)
    # every newest attempt failed, so nothing newer than t1 and t2 exists
    assert datetime.fromisoformat(as_of) == t1
    assert state["failed"] == 2
    assert state["missing"] == 0
    artifact, _ = view.read(clean_runtime)
    published_at = datetime.fromisoformat(
        artifact.published_at.isoformat()
    )
    assert datetime.fromisoformat(as_of) < published_at


# --- PACT view-output-carry-forward-3981: retired views ----------------------


def test_retired_view_is_labelled_on_every_list_and_refused_by_show(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import json
    from types import SimpleNamespace

    import okg.substrate.cli.view as cli_view_module
    import okg.substrate.deployments as deployments_module
    from okg.substrate.derived_artifacts import list_view_artifacts

    kept, dropped = _FleetView(), _FleetView()
    _fleet_seed(clean_runtime, a=4)
    published = Publisher(
        clean_runtime,
        "default",
        materializers=[kept.materializer, dropped.materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    (tmp_path / "views_registry.yaml").write_text(
        "deployment: default\n"
        "views:\n"
        f"  - view_def_id: {kept.view_id}\n"
        "    view_def_factory: views.fleet:factory\n"
        "    build_corpus: views.fleet:build_corpus\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        deployments_module, "find_deployment_dir", lambda _name: tmp_path
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        summaries = {
            item.view_id: item
            for item in list_view_artifacts(conn, deployment_name="default")
        }
        manifests_before = conn.execute(
            "SELECT count(*) FROM okg.derived_output_manifests "
            "WHERE definition_id = %s",
            (f"view:{dropped.view_id}",),
        ).fetchone()[0]
    assert summaries[dropped.view_id].retired is True
    assert summaries[kept.view_id].retired is False
    assert summaries[dropped.view_id].to_json()["retired"] is True

    # okg view list
    cli_view_module._load_main_helpers()
    monkeypatch.setattr(
        cli_view_module, "_resolve_deployment_dsn", lambda _args: clean_runtime
    )
    capsys.readouterr()
    rc = cli_view_module._cmd_view_list(
        SimpleNamespace(deployment="default", generation=None, dsn=clean_runtime),
        as_json=True,
    )
    assert rc == 0
    listed = {
        item["view_id"]: item["retired"]
        for item in json.loads(capsys.readouterr().out)["items"]
    }
    assert listed == {dropped.view_id: True, kept.view_id: False}
    cli_view_module._cmd_view_list(
        SimpleNamespace(deployment="default", generation=None, dsn=clean_runtime),
        as_json=False,
    )
    text = capsys.readouterr().out
    assert any(
        line.startswith(dropped.view_id) and "retired" in line
        for line in text.splitlines()
    )

    # okg view show refuses it by name
    rc = cli_view_module._cmd_view_show(
        SimpleNamespace(
            deployment="default",
            view=dropped.view_id,
            slot="markdown",
            generation=None,
            definition_id=None,
            audience="reader",
            tenant="tenant",
            dsn=clean_runtime,
        ),
        as_json=True,
    )
    assert rc == 1
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["data"]["reason"] == "native_read_view_retired"
    assert "retired" in error["message"]

    # the portal views list
    from tests.substrate.viz.test_native_view_reads import (
        _client_for_native_view,
    )

    client, ctx = _client_for_native_view(
        clean_runtime,
        generation_id=int(published["compat_generation_id"]),
        tmp_path=tmp_path,
    )
    try:
        response = client.get("/viz/api/portal/views")
        assert response.status_code == 200, response.text
        portal = {
            item["view_id"]: item["retired"] for item in response.json()["items"]
        }
        # the reader node of an ungoverned view says so, too (a governed view
        # is refused without a principal, as before)
        from types import SimpleNamespace

        import okg.viz.endpoints as endpoints_module

        def stub_slot(conn, *, deployment_name, view_id, generation_id, slot_name):
            return SimpleNamespace(
                artifact=SimpleNamespace(
                    view_id=view_id, output_contract={}, target_generation_id=1,
                    run_status="completed", manifest_id=uuid4(),
                    manifest_hash="h", definition_id=f"view:{view_id}",
                    required_slots=("markdown",),
                ),
                slot=SimpleNamespace(payload_value=lambda: "body", renderer_ref="r"),
            )

        monkeypatch.setattr(endpoints_module, "_read_viz_view_slot", stub_slot)
        with psycopg.connect(clean_runtime, autocommit=True) as node_conn:
            for view_id, expected in ((dropped.view_id, True), (kept.view_id, False)):
                node = endpoints_module._native_view_markdown_node(
                    node_conn, ctx, view_id=view_id,
                )
                assert node["attrs"]["retired"] is expected
    finally:
        ctx.pool.close()
    assert portal == {dropped.view_id: True, kept.view_id: False}

    # no manifest is deleted
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM okg.derived_output_manifests "
            "WHERE definition_id = %s",
            (f"view:{dropped.view_id}",),
        ).fetchone()[0] == manifests_before >= 1


# --- PACT view-output-carry-forward-3981: what counts as an empty graph delta -


def _fleet_edge(conn: psycopg.Connection, src: str, dst: str) -> None:
    from okg.substrate.library.sources.base import EdgeFact

    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            EdgeFact(
                src=src,
                dst=dst,
                edge_type="in_rack",
                source_record_id={"id": f"{src}>{dst}"},
                source_revision={"v": uuid4().hex},
            )
        ],
        source="test.fleet",
    )


def _fleet_retract(conn: psycopg.Connection, subtype: str, name: str) -> None:
    from okg.substrate.library.sources.base import NodeFact

    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(
                node_id=f"{subtype}:{name}",
                subtype=subtype,
                attrs={},
                source_record_id={"id": f"{subtype}:{name}"},
                source_revision={"v": uuid4().hex},
                op="R",
            )
        ],
        source="test.fleet",
    )


@pytest.mark.parametrize("change", ["edge_only", "host_retracted", "rack_deleted"])
def test_undeclared_fleet_view_is_rebuilt_on_an_edge_only_or_retraction_only_change(
    clean_runtime: str,
    change: str,
) -> None:
    """A view that declares no reads is assumed to read any graph type, so an
    edge that appears, a node that is retracted and a node of another type that
    is deleted each count as a graph change and rebuild it."""
    view = _FleetView()
    _fleet_seed(clean_runtime, a=4, b=8)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _fleet_set(conn, "rack", "r1", row=1)
    view.cycle(clean_runtime)
    calls_after_first = len(view.corpus_calls)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        if change == "edge_only":
            _fleet_edge(conn, "host:a", "rack:r1")
        elif change == "host_retracted":
            _fleet_retract(conn, "host", "a")
        else:
            _fleet_retract(conn, "rack", "r1")
    second = view.cycle(clean_runtime)

    _, markdown = view.read(clean_runtime)
    assert _freshness(second) != "carried_forward"
    assert len(view.corpus_calls) == calls_after_first + 1
    if change == "host_retracted":
        assert "hosts=1 cpus=8" in markdown


def test_graph_delta_empty_needs_a_change_set_that_listed_its_edges() -> None:
    nodes: dict = {}
    edges: dict = {}
    assert Publisher._graph_delta_empty(None) is False
    assert Publisher._graph_delta_empty((nodes, edges, False, {}, {}, None)) is False
    assert Publisher._graph_delta_empty((nodes, edges, True, {}, {}, None)) is True
    assert Publisher._graph_delta_empty(
        ({"host": ["host:a"]}, edges, True, {}, {}, None)
    ) is False
    assert Publisher._graph_delta_empty(
        (nodes, {"in_rack": [1]}, True, {}, {}, None)
    ) is False
