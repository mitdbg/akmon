"""A blocked transform gate names each missing pattern and its module (#3624)."""

from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.cli.run_summary import _blocked_detail
from okg.substrate.consistency.missing_patterns import (
    LibraryDeclarations,
    blocked_reason_fix_lines,
    empty_pattern_details,
    library_declarations,
    narrowing_pattern_entry,
    node_subtype_pattern_entry,
    pattern_fix_lines,
    violation_pattern_details,
)
from okg.substrate.publish_proof import assert_publish_proof


def test_library_index_names_document_starter_for_document_contains_section() -> None:
    library = library_declarations()

    assert library.modules_for_narrowing(
        "document", "contains", "section",
    ) == (("document_starter",),)
    assert library.modules_for_subtype("section") == ("document_starter",)
    # The composer's class-name mapping: DocumentChunk -> document_chunk.
    assert "extraction" in library.modules_for_subtype("document_chunk")


def test_library_index_returns_no_module_for_an_undeclared_pattern() -> None:
    library = library_declarations()

    assert library.modules_for_narrowing(
        "document", "contains", "no_such_subtype_3624",
    ) == ()
    assert library.modules_for_subtype("no_such_subtype_3624") == ()


def test_library_index_names_the_owner_of_an_intra_cross_link_rule() -> None:
    # engram_runs declares this triple only in its intra_cross_link_rules
    # file, never in its EdgeNarrowings class.
    assert library_declarations().modules_for_narrowing(
        "failed_approach", "refuted_by", "experiment_result",
    ) == (("engram_runs",),)


def test_library_index_names_both_modules_of_an_extends_cross_link_rule() -> None:
    library = library_declarations()
    sets = library.modules_for_narrowing("review", "reviews", "paper")
    assert sets == (("academic_researcher", "scholarly"),)
    entry = narrowing_pattern_entry("review", "reviews", "paper", 4, library)
    assert entry["library_modules"] == ["academic_researcher", "scholarly"]
    assert entry["fix"].startswith(
        "add `academic_researcher` (with `scholarly`) under `modules:` in "
        "deployment.yaml"
    )


def test_library_index_finds_a_narrowing_declared_on_a_parent_subtype() -> None:
    library = library_declarations()
    # advisee refines person in the library; kinship declares
    # `person mother person`, which the gate's ancestor closure admits.
    assert library.subtype_parent["advisee"] == "person"
    assert ("person", "mother", "advisee") not in library.narrowings
    assert library.modules_for_narrowing(
        "person", "mother", "advisee",
    ) == (("kinship",),)


def test_library_index_is_cached() -> None:
    assert library_declarations() is library_declarations()


_LIBRARY = LibraryDeclarations(
    narrowings={
        ("document", "contains", "section"): (("document_starter",),),
        ("a", "links_to", "b"): (("mod_one",), ("mod_two", "mod_three")),
    },
    subtypes={"section": ("document_starter",)},
)


_DOC_FIX = (
    "add `document_starter` under `modules:` in deployment.yaml (if already "
    "listed, the applied catalog is stale), then re-apply the catalog: "
    "okg catalog load --deployment DEPLOYMENT_DIR --apply"
)


def test_pattern_entries_name_the_module_or_say_none_declares_it() -> None:
    declared = narrowing_pattern_entry(
        "document", "contains", "section", 1181, _LIBRARY,
    )
    assert declared == {
        "src_subtype": "document",
        "edge_type": "contains",
        "dst_subtype": "section",
        "pattern": "document contains section",
        "edge_count": 1181,
        "library_modules": ["document_starter"],
        "library_module_sets": [["document_starter"]],
        "fix": _DOC_FIX,
    }
    two = narrowing_pattern_entry("a", "links_to", "b", 2, _LIBRARY)
    assert two["library_modules"] == ["mod_one", "mod_two", "mod_three"]
    assert two["fix"].startswith(
        "add one of `mod_one` or `mod_two` (with `mod_three`) under "
        "`modules:` in deployment.yaml"
    )
    undeclared = narrowing_pattern_entry("x", "rel", "y", 3, _LIBRARY)
    assert undeclared["library_modules"] == []
    assert undeclared["fix"].startswith(
        "no okg library module declares `x rel y`; declare it in an "
        "EdgeNarrowings class in the deployment's own schemas/bridges/"
    )
    missing_endpoint = narrowing_pattern_entry(None, "rel", "y", 1, _LIBRARY)
    assert missing_endpoint["pattern"] == "(missing node) rel y"
    assert missing_endpoint["library_modules"] == []

    node = node_subtype_pattern_entry("storage_area", 7, _LIBRARY)
    assert node["library_modules"] == []
    assert node["fix"].startswith(
        "no okg library module declares subtype `storage_area`; declare it "
        "in the deployment's own schemas/ directory"
    )
    assert node_subtype_pattern_entry("section", 2, _LIBRARY)["fix"] == _DOC_FIX


class _FakeSession:
    def __init__(self, edge_rows: list[dict[str, Any]], node_rows: list[dict[str, Any]]):
        self.edge_rows = edge_rows
        self.node_rows = node_rows
        self.calls: list[dict[str, Any]] = []

    def query(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.calls.append(params)
        if "pattern_edge_ids" in params:
            return self.edge_rows
        return self.node_rows


def test_clean_run_reads_nothing() -> None:
    out = violation_pattern_details(
        None, narrowing_edge_ids=[], node_subtype_node_ids=[],
    )
    assert out == {
        "narrowing_violation_patterns": [],
        "narrowing_violation_patterns_truncated": False,
        "node_subtype_violation_patterns": [],
        "node_subtype_violation_patterns_truncated": False,
        "violation_patterns_error": None,
    }


def test_violation_details_group_by_pattern_and_bound_the_list() -> None:
    session = _FakeSession(
        edge_rows=[
            {"src_subtype": "document", "edge_type": "contains",
             "dst_subtype": "section", "n": 1181},
            {"src_subtype": "x", "edge_type": "rel", "dst_subtype": "y", "n": 1},
        ],
        node_rows=[{"subtype": "storage_area", "n": 7}],
    )
    out = violation_pattern_details(
        session,
        narrowing_edge_ids=[1, 2, 3],
        node_subtype_node_ids=["n1"],
        library=_LIBRARY,
        limit=1,
    )
    assert session.calls == [
        {"pattern_edge_ids": [1, 2, 3]}, {"pattern_node_ids": ["n1"]},
    ]
    assert [p["pattern"] for p in out["narrowing_violation_patterns"]] == [
        "document contains section",
    ]
    assert out["narrowing_violation_patterns_truncated"] is True
    assert out["node_subtype_violation_patterns"][0]["subtype"] == "storage_area"
    assert out["node_subtype_violation_patterns_truncated"] is False


def _blocked_reason() -> dict[str, Any]:
    details = {
        "narrowing_violation_edge_ids": [1, 2],
        "narrowing_violation_patterns": [
            narrowing_pattern_entry(
                "document", "contains", "section", 1181, _LIBRARY,
            ),
        ],
        "narrowing_violation_patterns_truncated": False,
        "node_subtype_violation_patterns": [
            node_subtype_pattern_entry("storage_area", 7, _LIBRARY),
        ],
        "node_subtype_violation_patterns_truncated": True,
    }
    gate = {
        "check_id": "transform.consistency.v4",
        "reason": "transform_consistency_violation",
        "blocks_publish": True,
        "details": details,
    }
    # The pre-view and post-view passes can both report one pattern, with
    # different counts; it is named once, with the larger count.
    post_view = {
        **gate,
        "details": {
            **details,
            "narrowing_violation_patterns": [
                narrowing_pattern_entry(
                    "document", "contains", "section", 900, _LIBRARY,
                ),
            ],
        },
    }
    return {
        "reason": "consistency_publish_gate_blocked",
        "failed_consistency_gates": [post_view, gate],
    }


_SECTION_LINE = (
    "1181 edge(s) `document contains section` have no declared narrowing: "
    + _DOC_FIX
)


def test_fix_lines_name_pattern_count_and_module_once() -> None:
    lines = blocked_reason_fix_lines(_blocked_reason())
    assert lines[0] == _SECTION_LINE
    assert lines[1].startswith(
        "7 node(s) of subtype `storage_area` are not in the catalog: "
        "no okg library module declares subtype `storage_area`"
    )
    assert lines[2] == (
        "more node subtypes are missing; okg generation describe lists "
        "every node id"
    )
    assert len(lines) == 3
    nested = {"reason": "multiple_gate_failures", "phase_b": _blocked_reason()}
    assert blocked_reason_fix_lines(nested) == lines
    assert pattern_fix_lines(None) == []
    assert blocked_reason_fix_lines("narrowing_violation") == []


def test_run_summary_block_detail_carries_the_module_fix() -> None:
    detail = _blocked_detail(_blocked_reason())
    assert detail.startswith(
        "consistency_publish_gate_blocked; failed: transform.consistency.v4"
    )
    assert _SECTION_LINE in detail
    assert detail.count("document contains section") == 1


def test_run_summary_block_detail_unchanged_without_patterns() -> None:
    reason = {
        "reason": "consistency_publish_gate_blocked",
        "failed_consistency_gates": [{"check_id": "g", "details": {}}],
    }
    assert _blocked_detail(reason) == "consistency_publish_gate_blocked; failed: g"


def test_publish_proof_fix_hint_names_the_module_for_a_blocked_publish() -> None:
    verdict = assert_publish_proof(
        {
            "status": "blocked",
            "generation_id": 42,
            "blocked_reason": _blocked_reason(),
        },
        dsn="postgresql://unused.invalid/okg",
    )
    assert verdict["ok"] is False
    assert verdict["fix_hint"].startswith(_SECTION_LINE + " | 7 node(s)")
    assert verdict["fix_hint"].endswith(
        "run `okg generation describe <g>` and fix the blocking invariant"
    )


@pytest.mark.parametrize("blocked_reason", [None, {"reason": "gate_timeout"}])
def test_publish_proof_fix_hint_unchanged_without_patterns(blocked_reason: Any) -> None:
    verdict = assert_publish_proof(
        {"status": "blocked", "generation_id": 42, "blocked_reason": blocked_reason},
        dsn="postgresql://unused.invalid/okg",
    )
    assert verdict["fix_hint"] == (
        "run `okg generation describe <g>` and fix the blocking invariant"
    )


def test_lookup_error_is_rendered_not_dropped() -> None:
    reason = {
        "reason": "consistency_publish_gate_blocked",
        "failed_consistency_gates": [{
            "check_id": "g",
            "details": {
                "narrowing_violation_edge_ids": [5],
                **empty_pattern_details(
                    {"type": "CatalogModuleMissing", "message": "boom"},
                ),
            },
        }],
    }
    assert blocked_reason_fix_lines(reason) == [
        "the gate could not name the missing patterns "
        "(CatalogModuleMissing: boom); okg generation describe lists the "
        "violating ids",
    ]
