"""Output-template contract tests for native view definitions."""
from __future__ import annotations

import pytest

from okg.views.assembly import assemble
from okg.views.planners.base import (
    QuoteSpec,
    ViewDefinition,
    ViewOutputTemplate,
    ViewRelationColumn,
    default_output_templates,
)


def _planner(corpus, view_def):
    return [QuoteSpec(id="summary", anchor_id="summary")]


def test_view_definition_declares_default_native_outputs() -> None:
    view_def = ViewDefinition(
        view_id="demo.output_templates",
        planner=_planner,
        assembler=assemble,
    )

    assert [template.slot_name for template in view_def.output_templates] == [
        "markdown",
        "json",
    ]
    assert [
        template.source for template in view_def.output_templates
    ] == [
        "assembled_markdown",
        "native_view_manifest",
    ]
    assert all(template.required for template in view_def.output_templates)


def test_view_definition_accepts_optional_table_template() -> None:
    templates = (
        *default_output_templates(),
        ViewOutputTemplate.fragment_json_tables(),
    )

    view_def = ViewDefinition(
        view_id="demo.output_templates.table",
        planner=_planner,
        assembler=assemble,
        output_templates=templates,
    )

    table_template = view_def.output_templates[-1]
    assert table_template.slot_name == "table"
    assert table_template.source == "fragment_json_tables"
    assert table_template.required is False
    assert table_template.media_type == "application/vnd.okg.table+json"
    assert table_template.to_contract()["required"] is False


def test_view_definition_accepts_view_graph_template() -> None:
    templates = (
        *default_output_templates(),
        ViewOutputTemplate.view_graph_json(),
    )

    view_def = ViewDefinition(
        view_id="demo.output_templates.view_graph",
        planner=_planner,
        assembler=assemble,
        output_templates=templates,
    )

    view_graph_template = view_def.output_templates[-1]
    assert view_graph_template.slot_name == "view_graph"
    assert view_graph_template.source == "view_graph_json"
    assert view_graph_template.media_type == "application/vnd.okg.view-graph+json"
    assert view_graph_template.artifact_type == "okg/view-graph-json-v1"


def test_view_definition_accepts_sql_relation_template() -> None:
    relation_template = ViewOutputTemplate.sql_relation(
        logical_relation_id="demo.output_templates.relation",
        title="Demo rows",
        columns=(
            ViewRelationColumn("label", label="Label", data_type="text"),
            ViewRelationColumn("count", label="Count", data_type="integer"),
        ),
        scope_key_column="scope_key",
        sql=(
            "SELECT 'demo' AS scope_key, 'row-1' AS row_key, "
            "'ok' AS label, 1 AS count"
        ),
    )

    view_def = ViewDefinition(
        view_id="demo.output_templates.relation",
        planner=_planner,
        assembler=assemble,
        output_templates=(
            *default_output_templates(),
            relation_template,
        ),
    )

    assert view_def.output_templates[-1].slot_name == "relation"
    assert view_def.output_templates[-1].source == "sql_relation"
    assert view_def.output_templates[-1].required is True
    assert view_def.output_templates[-1].config["columns"] == [
        "label",
        "count",
    ]
    assert view_def.output_templates[-1].config["column_contracts"] == [
        {
            "name": "label",
            "label": "Label",
            "data_type": "text",
            "required": True,
        },
        {
            "name": "count",
            "label": "Count",
            "data_type": "integer",
            "required": True,
        },
    ]
    assert view_def.output_templates[-1].config["scope"] == {
        "policy": "column",
        "column": "scope_key",
    }
    assert view_def.output_templates[-1].config["row_identity"] == {
        "policy": "column",
        "column": "row_key",
    }


def test_sql_relation_can_hash_declared_row_identity_columns() -> None:
    relation_template = ViewOutputTemplate.sql_relation(
        logical_relation_id="demo.output_templates.relation.hash_key",
        title="Demo rows",
        columns=("label", "count"),
        scope_key="demo.scope",
        row_key_column="",
        row_key_columns=("label",),
        sql="SELECT 'ok' AS label, 1 AS count",
    )

    assert relation_template.config["scope"] == {
        "policy": "constant",
        "scope_key": "demo.scope",
    }
    assert relation_template.config["row_identity"] == {
        "policy": "columns_hash",
        "columns": ["label"],
    }


def test_view_definition_rejects_duplicate_output_slots() -> None:
    with pytest.raises(ValueError, match="duplicate output template slots"):
        ViewDefinition(
            view_id="demo.output_templates.duplicate",
            planner=_planner,
            assembler=assemble,
            output_templates=(
                ViewOutputTemplate.markdown(),
                ViewOutputTemplate.markdown(),
            ),
        )
