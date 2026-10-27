"""PACT view-output-carry-forward-3981: the scheduler keeps a view's new output.

A view task that holds the outputs it just rendered is carried forward only
when every slot's content hash (generation id, refresh stats and input
signature left out) equals the previous manifest's; otherwise the scheduler
publishes the outputs. These tests need PostgreSQL. They live in their own
file so the scheduler tests that were already there keep their lane.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.derived_artifacts import (
    InlineViewOutput,
    ViewScheduleTask,
    get_view_slot,
    schedule_derived_artifacts,
)
from tests.substrate.test_derived_artifact_scheduler import (
    _generation_id,
    _insert_generation,
    _view_definition,
)

pytestmark = pytest.mark.postgres


def _native_json_payload(
    *,
    generation_id: int,
    text: str,
    refreshed: int = 0,
    watermark: int = 0,
) -> dict:
    """A JSON slot shaped like the native view stage's: it embeds the
    generation id, the refresh statistics and the input signature, none of
    which say anything about the content."""
    return {
        "view_manifest": {
            "manifest_version": 1,
            "view_id": "fleet",
            "generation_id": generation_id,
            "fragments": [{"id": "summary", "content": text}],
        },
        "refresh_stats": {"fragments_total": 1, "refreshed": refreshed},
        "materializer_input_signature": {"input_fact_watermark": watermark},
        "native_view_stage": {"version": 1},
    }


def _manifest_row(conn: psycopg.Connection, manifest_id) -> tuple:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT m.status, m.reused_from_manifest_id, m.metadata
              FROM okg.derived_output_manifests m
             WHERE m.manifest_id = %(manifest_id)s
            """,
            {"manifest_id": manifest_id},
        )
        return cur.fetchone()


def test_scheduler_publishes_new_native_view_output_when_dirty_scopes_are_empty(
    clean_runtime: str,
) -> None:
    """Different content at generation 2 with no dirty scopes and a trusted
    previous manifest is published, not replaced by a copy of generation 1."""
    view = _view_definition("changed-output")
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        first = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen1,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown("# Fleet\n\n4 CPUs."),
                        InlineViewOutput.json({"cpus": 4}),
                    ],
                    dirty_scope_keys=[],
                )
            ],
        )
        second = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            trusted_previous_manifest_ids={
                view.definition_id: first.nodes[0].manifest_id,
            },
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown("# Fleet\n\n8 CPUs."),
                        InlineViewOutput.json({"cpus": 8}),
                    ],
                    dirty_scope_keys=[],
                )
            ],
        )
        markdown = get_view_slot(
            conn,
            manifest_id=second.nodes[0].manifest_id,
            slot_name="markdown",
        ).payload_value()
        payload = get_view_slot(
            conn,
            manifest_id=second.nodes[0].manifest_id,
            slot_name="json",
        ).payload_value()
        status, reused_from, metadata = _manifest_row(
            conn, second.nodes[0].manifest_id
        )

    assert second.nodes[0].status == "completed"
    assert reused_from is None
    assert "8 CPUs" in markdown
    assert payload == {"cpus": 8}
    # a published manifest is the origin of its own content
    assert metadata["origin"] == {
        "manifest_id": str(second.nodes[0].manifest_id),
        "generation_id": gen2,
    }


def test_scheduler_publishes_new_native_view_output_without_a_trusted_manifest(
    clean_runtime: str,
) -> None:
    view = _view_definition("changed-output-untrusted")
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        for generation, text in ((gen1, "one"), (gen2, "two")):
            result = schedule_derived_artifacts(
                conn,
                deployment_name="test",
                target_generation_id=generation,
                tasks=[
                    ViewScheduleTask(
                        view,
                        outputs=[InlineViewOutput.markdown(f"# {text}")],
                        dirty_scope_keys=[],
                    )
                ],
            )

    assert result.nodes[0].status == "completed"


def test_scheduler_carries_native_view_output_that_differs_only_in_generation_fields(
    clean_runtime: str,
) -> None:
    """Equal content hashes equal even though the JSON slot embeds the
    generation id, refresh statistics and input signature."""
    view = _view_definition("generation-fields")
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        first = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen1,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown("# Fleet\n\n4 CPUs."),
                        InlineViewOutput.json(
                            _native_json_payload(
                                generation_id=gen1,
                                text="4 CPUs",
                                refreshed=1,
                                watermark=7,
                            )
                        ),
                    ],
                    dirty_scope_keys=[],
                    manifest_metadata={
                        "inputs_valid_through_generation": gen1,
                    },
                )
            ],
        )
        second = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown("# Fleet\n\n4 CPUs."),
                        InlineViewOutput.json(
                            _native_json_payload(
                                generation_id=gen2,
                                text="4 CPUs",
                                refreshed=0,
                                watermark=9,
                            )
                        ),
                    ],
                    dirty_scope_keys=[],
                    manifest_metadata={
                        "inputs_valid_through_generation": gen2,
                    },
                )
            ],
        )
        third = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2 + 1,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown("# Fleet\n\n4 CPUs."),
                        InlineViewOutput.json(
                            _native_json_payload(
                                generation_id=gen2 + 1,
                                text="4 CPUs",
                            )
                        ),
                    ],
                    dirty_scope_keys=[],
                )
            ],
        )
        _, reused_from, second_metadata = _manifest_row(
            conn, second.nodes[0].manifest_id
        )
        _, third_reused, third_metadata = _manifest_row(
            conn, third.nodes[0].manifest_id
        )

    assert first.nodes[0].status == "completed"
    assert second.nodes[0].status == "carried_forward"
    assert reused_from == first.nodes[0].manifest_id
    # the carry still records the new input time and the first build as origin
    assert second_metadata["inputs_valid_through_generation"] == gen2
    assert second_metadata["origin"]["manifest_id"] == str(
        first.nodes[0].manifest_id
    )
    assert second_metadata["origin"]["generation_id"] == gen1
    assert third.nodes[0].status == "carried_forward"
    assert third_reused == second.nodes[0].manifest_id
    assert third_metadata["origin"]["generation_id"] == gen1


def test_scheduler_publishes_native_view_output_when_one_slot_changes(
    clean_runtime: str,
) -> None:
    view = _view_definition("one-slot-changes")
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen1,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown("# Same"),
                        InlineViewOutput.json(
                            _native_json_payload(
                                generation_id=gen1, text="old"
                            )
                        ),
                    ],
                    dirty_scope_keys=[],
                )
            ],
        )
        second = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown("# Same"),
                        InlineViewOutput.json(
                            _native_json_payload(
                                generation_id=gen2, text="new"
                            )
                        ),
                    ],
                    dirty_scope_keys=[],
                )
            ],
        )

    assert second.nodes[0].status == "completed"


def test_scheduler_carries_a_json_slot_that_differs_only_in_a_top_level_generation_id(
    clean_runtime: str,
) -> None:
    """fragment_json_tables and view_graph_json slots put generation_id at the
    top level of their payload; it is not content."""
    view = _view_definition("top-level-generation")
    gen1 = _generation_id()
    gen2 = gen1 + 1

    def outputs(generation: int, rows: list) -> list:
        return [
            InlineViewOutput.json(
                {"generation_id": generation, "tables": rows},
                slot_name="fragment_tables",
            )
        ]

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        _insert_generation(conn, gen2 + 1)
        schedule_derived_artifacts(
            conn, deployment_name="test", target_generation_id=gen1,
            tasks=[ViewScheduleTask(view, outputs=outputs(gen1, [1, 2]), dirty_scope_keys=[])],
        )
        same = schedule_derived_artifacts(
            conn, deployment_name="test", target_generation_id=gen2,
            tasks=[ViewScheduleTask(view, outputs=outputs(gen2, [1, 2]), dirty_scope_keys=[])],
        )
        changed = schedule_derived_artifacts(
            conn, deployment_name="test", target_generation_id=gen2 + 1,
            tasks=[ViewScheduleTask(view, outputs=outputs(gen2 + 1, [1, 3]), dirty_scope_keys=[])],
        )

    assert same.nodes[0].status == "carried_forward"
    assert changed.nodes[0].status == "completed"
