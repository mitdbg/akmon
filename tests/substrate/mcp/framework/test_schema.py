"""Tests for the pre-join MCP tool framework's YAML schema parser.

Per add-substrate-pre-join-mcp-framework / S-A7-R1, S-A7-R2:
- A valid pre-join YAML parses to typed PreJoinToolSpec records.
- Schema-invalid entries are isolated as PreJoinValidationFailure so
  the substrate logs them and continues registering the rest.
- The substrate primitive whitelist is closed; unknown step kinds
  surface a distinct typed error code.
- `traverse_path` max_hops > 3 surfaces PreJoinTraverseHopsExceeded.
- `start: { resolve | from }` rules are enforced; `from` must point
  at a prior emit; `resolve` `input` must point at a param or prior
  emit.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from okg.substrate.mcp.framework import (
    PreJoinSchemaError,
    PreJoinStepKindUnknown,
    PreJoinToolSpec,
    PreJoinTraverseHopsExceeded,
    SUPPORTED_STEP_KINDS,
    load_pre_join_tools_from_path,
    parse_pre_join_tools,
)
from okg.substrate.mcp.framework.schema import (
    evaluate_predicate,
    parse_predicate,
)


_DRAFTING_TOOL = textwrap.dedent("""\
    tools:
      - name: memex_get_drafting_context
        description: "Pre-joined context for drafting a paper section"
        params:
          - name: topic
            kind: text
            required: true
          - name: section_id
            kind: text
            required: false
        bundle:
          - call: traverse_path
            args:
              start: {resolve: topic_to_concept, input: topic}
              path: ["defines:in", "uses:in", "refines:in"]
              max_hops: 2
            emit_as: papers
          - call: traverse_path
            args:
              start: {resolve: topic_to_concept, input: topic}
              path: ["embodies:in"]
              max_hops: 1
            emit_as: method_cards
          - call: traverse_path
            args:
              start: {resolve: section_to_paper_section, input: section_id}
              path: ["contains:out"]
              max_hops: 2
            emit_as: section_paragraphs
            when: "section_id != null"
        summary_template: |
          Drafting context for topic={topic}, section={section_id}.
          {papers.count} relevant papers; {method_cards.count} method cards.
""")


def _parse(yaml_text: str):
    import yaml
    raw = yaml.safe_load(yaml_text)
    return parse_pre_join_tools(raw)


def test_drafting_tool_round_trips() -> None:
    """The brief's memex_get_drafting_context example parses to a
    valid PreJoinToolSpec without unresolved fields (task 1.5)."""
    specs, failures = _parse(_DRAFTING_TOOL)
    assert failures == []
    assert len(specs) == 1
    spec = specs[0]
    assert isinstance(spec, PreJoinToolSpec)
    assert spec.name == "memex_get_drafting_context"
    assert len(spec.params) == 2
    assert spec.params[0].name == "topic" and spec.params[0].required
    assert len(spec.bundle) == 3
    assert spec.bundle[0].call == "traverse_path"
    assert spec.bundle[0].emit_as == "papers"
    assert spec.bundle[2].when == "section_id != null"
    assert spec.summary_template is not None
    assert "{topic}" in spec.summary_template


def test_step_kind_whitelist_is_closed() -> None:
    """A bundle step naming an unknown primitive surfaces the typed
    PreJoinStepKindUnknown error so the bootstrap log can spot
    configuration drift (S-A7-R2 negative-path scenario)."""
    yaml_text = textwrap.dedent("""\
        tools:
          - name: bad_tool
            description: Uses an unknown call.
            params:
              - {name: q, kind: text, required: true}
            bundle:
              - call: run_sql
                args: {query: "SELECT 1"}
                emit_as: rows
    """)
    specs, failures = _parse(yaml_text)
    assert specs == []
    assert len(failures) == 1
    err = failures[0].error
    assert isinstance(err, PreJoinStepKindUnknown)
    assert err.code == "pre_join_tool_step_kind_unknown"


def test_traverse_path_max_hops_capped_at_three() -> None:
    """max_hops > 3 surfaces PreJoinTraverseHopsExceeded; the
    substrate preserves the existing traverse_path ≤3-hop ceiling
    at the framework boundary (task 3.3)."""
    yaml_text = textwrap.dedent("""\
        tools:
          - name: deep_tool
            description: Hops too far.
            params:
              - {name: topic, kind: text, required: true}
            bundle:
              - call: traverse_path
                args:
                  start: {resolve: alias_table, input: topic}
                  path: ["uses:in", "refines:in", "cites:in", "cites:in"]
                  max_hops: 4
                emit_as: rows
    """)
    specs, failures = _parse(yaml_text)
    assert specs == []
    assert len(failures) == 1
    err = failures[0].error
    assert isinstance(err, PreJoinTraverseHopsExceeded)


def test_resolve_input_must_match_param_or_prior_emit() -> None:
    """`start.input` must reference either a declared param or a
    prior step's emit_as (task 1.3)."""
    yaml_text = textwrap.dedent("""\
        tools:
          - name: dangling_input_tool
            description: References an unknown param.
            params:
              - {name: topic, kind: text, required: true}
            bundle:
              - call: traverse_path
                args:
                  start: {resolve: alias_table, input: ghost_param}
                  path: ["uses:in"]
                  max_hops: 1
                emit_as: rows
    """)
    specs, failures = _parse(yaml_text)
    assert specs == []
    assert len(failures) == 1
    err = failures[0].error
    assert isinstance(err, PreJoinSchemaError)
    assert "ghost_param" in str(err)


def test_from_must_reference_prior_emit() -> None:
    yaml_text = textwrap.dedent("""\
        tools:
          - name: bad_from_tool
            description: References an emit_as that doesn't exist yet.
            params:
              - {name: topic, kind: text, required: true}
            bundle:
              - call: get_node
                args:
                  node_id: {from: later_step}
                emit_as: first_emit
    """)
    specs, failures = _parse(yaml_text)
    assert specs == []
    assert len(failures) == 1
    assert "later_step" in str(failures[0].error)


def test_isolated_validation_failures_keep_good_tools(tmp_path: Path) -> None:
    """S-A7-R1 scenario: schema-invalid YAML fails loud at bootstrap
    but other valid tools in the same YAML still register."""
    yaml_text = textwrap.dedent("""\
        tools:
          - name: good_tool
            description: A correct tool.
            params:
              - {name: t, kind: text, required: true}
            bundle:
              - call: get_node
                args:
                  node_id: {resolve: alias_table, input: t}
                emit_as: row
          - name: bad_tool
            description: Missing bundle.
    """)
    specs, failures = _parse(yaml_text)
    assert len(specs) == 1
    assert specs[0].name == "good_tool"
    assert len(failures) == 1
    assert failures[0].tool_name == "bad_tool"


def test_when_predicate_grammar_is_constrained() -> None:
    """Predicates outside the supported grammar (task 1.4) fail at
    parse time."""
    bad = "params.x.attrs.foo == 'bar'"  # nested dot-access, unsupported
    with pytest.raises(PreJoinSchemaError, match="unsupported"):
        parse_predicate(bad)


def test_when_predicate_evaluation_branches() -> None:
    """parse_predicate + evaluate_predicate cover the four supported
    forms (`!= null`, `== "<str>"`, `== <int>`, `== true|false`)."""
    assert evaluate_predicate("x != null", {"x": "v"}) is True
    assert evaluate_predicate("x != null", {"x": None}) is False
    assert evaluate_predicate('x == "hi"', {"x": "hi"}) is True
    assert evaluate_predicate('x == "hi"', {"x": "bye"}) is False
    assert evaluate_predicate("x == 42", {"x": 42}) is True
    assert evaluate_predicate("x == 42", {"x": 41}) is False
    assert evaluate_predicate("x == true", {"x": True}) is True
    assert evaluate_predicate("x == false", {"x": True}) is False


def test_load_from_path_returns_empty_for_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "absent.yaml"
    specs, failures = load_pre_join_tools_from_path(missing)
    assert specs == []
    assert failures == []


def test_duplicate_tool_name_fails() -> None:
    yaml_text = textwrap.dedent("""\
        tools:
          - name: dup
            description: First.
            params: [{name: x, kind: text}]
            bundle:
              - call: get_node
                args: {node_id: {resolve: alias_table, input: x}}
                emit_as: a
          - name: dup
            description: Second.
            params: [{name: y, kind: text}]
            bundle:
              - call: get_node
                args: {node_id: {resolve: alias_table, input: y}}
                emit_as: b
    """)
    specs, failures = _parse(yaml_text)
    assert len(specs) == 1
    assert len(failures) == 1
    assert "duplicate" in str(failures[0].error).lower()


def test_supported_step_kinds_match_substrate_whitelist() -> None:
    """The framework's whitelist matches the substrate's actually-
    exported read primitives (S-A7-R2). `aggregate_by_type` was
    retired upstream before this child landed; see finding
    2026-05-18-aggregate-by-type-retired-upstream.md.
    """
    assert SUPPORTED_STEP_KINDS == {
        "traverse_path",
        "get_node",
        "list_neighbors",
        "aggregate_neighbors",
        "search",
        "find_identifier_mentions",
    }
