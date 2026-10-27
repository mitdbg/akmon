"""History reads without a database: merging, change keys, paging, arguments.

PACT okg-attribute-history-read. The database tests
(`test_history_reads_db.py`, `tests/substrate/test_node_history_store.py`)
publish real generations; these pin the rules those reads apply to what the
store returns, including the cases a real publish cannot produce (the graph
writer stores no new version for a lineage-revision-only change).
"""
from __future__ import annotations

import json

import pytest
from pydantic import TypeAdapter, ValidationError

from okg.substrate.fact_equivalence import LINEAGE_ATTR
from okg.substrate.graph_store_internal.node_history import LineageGeneration
from okg.substrate.mcp.effective_surface import READ_INSPECT_KINDS
from okg.substrate.mcp.history_reads import (
    ShownRow,
    classify_changes,
    fit_page,
    merge_versions,
    paged_payload,
    read_node_history,
    read_subtype_diff,
)
from okg.substrate.mcp.operator_models import (
    HISTORY_MAX_LIMIT,
    INSPECT_TARGET_KINDS,
    InspectTarget,
    as_mapping,
)
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
from okg.substrate.nomos.read_masking import MASK_TOKEN, REDACTION_MARKER


def _gen(n: int) -> LineageGeneration:
    return LineageGeneration(
        generation_id=f"okg:g{n}", branch="default", ordinal=n, stab=100 * n,
        created_at=f"2026-10-21T00:0{n}:00Z", depth=10 - n,
    )


def _lineage(revision: int, record: str = "r1") -> dict:
    return {
        "schema": "okg.nomos.source_lineage/v1",
        "status": "complete",
        "source_ids": ["inventory"],
        "sources": [{
            "source_id": "inventory",
            "source_record_ids": [record],
            "source_revisions": [str(revision)],
            "source_records": [
                {"source_record_id": record, "source_revision": str(revision)},
            ],
        }],
    }


def _row(subtype: str = "compute_node", **attrs) -> ShownRow:
    return ShownRow(subtype, dict(attrs), "inventory")


VOLATILE = {"compute_node": frozenset({"sampled_at"})}


def test_the_pact_five_generation_sequence_is_three_versions() -> None:
    """g2 a real field, g3 a volatile field, g4 both, g5 the lineage revision
    only: three versions, and the newest names the real field only."""
    g = [_gen(n) for n in range(1, 6)]
    states = [
        _row(cpus=8, state="idle", sampled_at="t1", **{LINEAGE_ATTR: _lineage(1)}),
        _row(cpus=16, state="idle", sampled_at="t1", **{LINEAGE_ATTR: _lineage(1)}),
        _row(cpus=16, state="idle", sampled_at="t3", **{LINEAGE_ATTR: _lineage(1)}),
        _row(cpus=16, state="busy", sampled_at="t4", **{LINEAGE_ATTR: _lineage(1)}),
        _row(cpus=16, state="busy", sampled_at="t4", **{LINEAGE_ATTR: _lineage(2)}),
    ]
    versions = merge_versions(g, states, VOLATILE)
    spans = [(v.first.generation_id, v.last.generation_id) for v in versions]
    assert spans == [("okg:g1", "okg:g1"), ("okg:g2", "okg:g3"), ("okg:g4", "okg:g5")]
    assert versions[0].change == {"change": "oldest_shown"}
    assert versions[1].change == {
        "change": "changed", "added_keys": [], "removed_keys": [], "changed_keys": ["cpus"],
    }
    assert versions[2].change["changed_keys"] == ["state"]
    # The values a version reports are those at its last generation.
    assert versions[2].last_attrs[LINEAGE_ATTR] == _lineage(2)


def test_a_volatile_field_is_ignored_only_for_the_subtype_that_declares_it() -> None:
    g = [_gen(1), _gen(2)]
    other = merge_versions(
        g, [_row("rack", sampled_at="t1"), _row("rack", sampled_at="t2")], VOLATILE,
    )
    assert len(other) == 2 and other[1].change["changed_keys"] == ["sampled_at"]


def test_a_revision_beside_a_real_change_is_named_as_compare_attrs_names_it() -> None:
    changes = classify_changes(
        {"cpus": 16, LINEAGE_ATTR: _lineage(2)},
        {"cpus": 8, LINEAGE_ATTR: _lineage(1)},
        ignore=frozenset(),
    )
    assert changes == {
        "added_keys": [], "removed_keys": [],
        "changed_keys": ["cpus", LINEAGE_ATTR + ".source_revision"],
    }


def test_added_and_removed_keys_are_told_apart() -> None:
    changes = classify_changes(
        {"cpus": 8, "gpus": 2}, {"cpus": 8, "memory": 64}, ignore=frozenset(),
    )
    assert changes == {"added_keys": ["gpus"], "removed_keys": ["memory"], "changed_keys": []}
    assert classify_changes({"a": 1}, {"a": 1}, ignore=frozenset()) is None


def test_a_subtype_change_is_the_old_subtype_retracted_and_the_new_added() -> None:
    versions = merge_versions(
        [_gen(1), _gen(2)],
        [_row("compute_node", name="n1"), _row("rack", name="n1")],
        VOLATILE,
    )
    assert len(versions) == 2
    assert versions[1].change["change"] == "subtype_changed"
    assert versions[1].change["subtype_retracted"] == "compute_node"
    assert versions[1].change["subtype_added"] == "rack"


def test_a_masked_value_change_is_neither_a_key_nor_a_boundary() -> None:
    """Failure case for a leak: the stored owner changed between g1 and g2,
    but both versions are shown masked, so they merge into one."""
    masked = {"cpus": 8, "owner_email": MASK_TOKEN, REDACTION_MARKER: True}
    versions = merge_versions(
        [_gen(1), _gen(2), _gen(3)],
        [_row(**masked), _row(**masked), _row(**masked)],
        VOLATILE,
    )
    assert [(v.first.generation_id, v.last.generation_id) for v in versions] == [
        ("okg:g1", "okg:g3"),
    ]


def test_hidden_generations_leave_no_marker_and_changes_skip_them() -> None:
    """A hidden version (None) is left out: equal neighbours merge across it,
    and a change is computed against the next older version shown."""
    merged = merge_versions(
        [_gen(1), _gen(2), _gen(3)], [_row(cpus=8), None, _row(cpus=8)], VOLATILE,
    )
    assert [(v.first.generation_id, v.last.generation_id) for v in merged] == [
        ("okg:g1", "okg:g3"),
    ]
    changed = merge_versions(
        [_gen(1), _gen(2), _gen(3)],
        [_row(cpus=8, state="a"), None, _row(cpus=16, state="a")],
        VOLATILE,
    )
    assert len(changed) == 2
    assert changed[1].change["changed_keys"] == ["cpus"]
    assert merge_versions([_gen(1)], [None], VOLATILE) == []


def test_fit_page_keeps_the_longest_prefix_within_the_budget() -> None:
    def build(count: int) -> dict:
        return {"items": ["x" * 10] * count}

    def measure(payload: dict) -> int:
        return len(json.dumps(payload))

    assert fit_page(build, 20, max_bytes=None, measure=measure) == 20
    count = fit_page(build, 20, max_bytes=100, measure=measure)
    assert measure(build(count)) <= 100 < measure(build(count + 1))
    # One item always goes out; the reply's byte bound refuses it if too big.
    assert fit_page(build, 20, max_bytes=5, measure=measure) == 1


def test_a_cut_page_with_its_real_cursor_stays_inside_the_budget() -> None:
    items = [f"item-{n}" * 4 for n in range(30)]

    def build(count: int) -> dict:
        return {"items": items[:count], "truncated": count < len(items)}

    def measure(payload: dict) -> int:
        return len(json.dumps(payload))

    issued = []

    def issue_for(count: int) -> str:
        issued.append(count)
        return "tok-" + "z" * 40 + str(count)

    payload = paged_payload(
        build, 20, target={"kind": "node_history", "node_id": "n:1", "from_": "okg:g1"},
        issue_for=issue_for, max_bytes=600, measure=measure,
    )
    assert payload["truncated"] is True
    assert measure(payload) <= 600
    assert payload["cursor"] == issue_for(len(payload["items"]))
    assert payload["next_target"] == {
        "kind": "node_history", "node_id": "n:1", "from": "okg:g1",
        "cursor": payload["cursor"],
    }
    whole = paged_payload(
        lambda count: {"items": items[:count], "truncated": False}, 3,
        target={"kind": "subtype_diff"}, issue_for=issue_for, max_bytes=None, measure=None,
    )
    assert "cursor" not in whole and whole["items"] == items[:3]


@pytest.mark.parametrize(
    "target,field",
    [
        ({"kind": "node_history", "node_id": "n:1", "limit": 0}, "target.limit"),
        ({"kind": "node_history", "node_id": "n:1", "limit": HISTORY_MAX_LIMIT + 1}, "target.limit"),
        ({"kind": "node_history", "node_id": "n:1", "limit": True}, "target.limit"),
        ({"kind": "node_history", "node_id": "n:1", "attr_keys": []}, "target.attr_keys"),
        ({"kind": "node_history", "node_id": "n:1", "attr_keys": [""]}, "target.attr_keys"),
        ({"kind": "node_history", "node_id": "n:1", "from": ""}, "target.from"),
        ({"kind": "node_history", "node_id": "n:1", "cursor": ""}, "target.cursor"),
        ({"kind": "node_history"}, "target.node_id"),
    ],
)
def test_node_history_refuses_bad_arguments_before_any_read(target, field) -> None:
    """Failure cases: each refusal names its field, and nothing touches the
    database (the session check is the first read, and is never reached)."""
    with pytest.raises(InvalidOperatorArguments) as refused:
        read_node_history(_NoConn(), _session(), target)
    assert refused.value.data["invalid_fields"] == [field]


@pytest.mark.parametrize(
    "target,field",
    [
        ({"kind": "subtype_diff", "from_generation": "okg:g1"}, "target.subtype"),
        ({"kind": "subtype_diff", "subtype": "compute_node"}, "target.from_generation"),
        ({"kind": "subtype_diff", "subtype": "compute_node", "from_generation": "okg:g1",
          "limit": 501}, "target.limit"),
    ],
)
def test_subtype_diff_refuses_bad_arguments_before_any_read(target, field) -> None:
    with pytest.raises(InvalidOperatorArguments) as refused:
        read_subtype_diff(_NoConn(), _session(), target)
    assert refused.value.data["invalid_fields"] == [field]


class _NoConn:
    """A connection that fails any use: the checks above must not read."""

    def __getattr__(self, name):  # pragma: no cover - reached only on a bug
        raise AssertionError(f"argument checks read the database ({name})")


def _session():
    from okg.substrate.mcp.session import Session

    return Session(
        deployment_name="default", generation_id="okg:g5", catalog_version_id=1,
        db_fingerprint="fp", branch_name="default",
    )


@pytest.fixture(autouse=True)
def _session_check_first(monkeypatch):
    """Argument checks run before `assert_session_matches` reads anything."""
    import okg.substrate.mcp.history_reads as history_reads

    monkeypatch.setattr(history_reads, "assert_session_matches", lambda conn, session: None)


def test_both_kinds_join_the_read_inspect_kinds_and_the_published_union() -> None:
    assert {"node_history", "subtype_diff"} <= READ_INSPECT_KINDS
    assert {"node_history", "subtype_diff"} <= set(INSPECT_TARGET_KINDS)
    adapter = TypeAdapter(InspectTarget)
    history = as_mapping(adapter.validate_python({
        "kind": "node_history", "node_id": "n:1", "from": "okg:g1", "to": 7,
        "attr_keys": ["cpus"], "limit": 500, "cursor": "c1.x",
    }))
    assert history["from_"] == "okg:g1" and history["to"] == 7
    diff = as_mapping(adapter.validate_python({
        "kind": "subtype_diff", "subtype": "compute_node", "from_generation": "okg:g1",
    }))
    assert diff["subtype"] == "compute_node"


@pytest.mark.parametrize(
    "target",
    [
        {"kind": "node_history"},
        {"kind": "node_history", "node_id": "n:1", "limit": 501},
        {"kind": "node_history", "node_id": "n:1", "attr_keys": ["k"] * 51},
        {"kind": "node_history", "node_id": "n:1", "offset": 3},
        {"kind": "subtype_diff", "subtype": "compute_node"},
        {"kind": "subtype_diff", "subtype": "compute_node", "from_generation": "g", "to": "g"},
    ],
)
def test_the_published_models_refuse_out_of_range_and_unknown_fields(target) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(InspectTarget).validate_python(target)


@pytest.mark.parametrize(
    "target",
    [
        {"kind": "node_history", "node_id": "n:1"},
        {"kind": "subtype_diff", "subtype": "compute_node", "from_generation": "okg:g1"},
    ],
)
def test_a_multi_user_reader_with_no_subject_is_refused_before_any_read(
    target, monkeypatch,
) -> None:
    """Failure case. With Nomos off, `subtype_diff` never reaches the read
    filter (its second lock), and an absent node's history never filters a
    row; both kinds refuse an unidentified multi-user reader first."""
    from dataclasses import replace

    from okg.substrate.errors import SubstrateError

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    reader = read_node_history if target["kind"] == "node_history" else read_subtype_diff
    session = replace(_session(), nomos_multi_user=True, nomos_subject=None)
    with pytest.raises(SubstrateError) as refused:
        reader(_NoConn(), session, target)
    assert refused.value.code == "nomos_principal_unidentified"
    assert refused.value.data["refusal_kind"] == "unidentified_principal"
    assert refused.value.data["channel"] == "read_filter_no_request_subject"


def test_a_version_records_the_rows_its_key_names_came_from() -> None:
    older = _row(cpus=8)
    newer = ShownRow("compute_node", {"cpus": 16}, "scraper")
    versions = merge_versions([_gen(1), _gen(2)], [older, newer], VOLATILE)
    assert versions[0].sides() == [("inventory", {"cpus": 8}), ("inventory", {"cpus": 8})]
    assert versions[1].sides() == [
        ("scraper", {"cpus": 16}), ("scraper", {"cpus": 16}), ("inventory", {"cpus": 8}),
    ]


def _fenced(name: str, source: str) -> str:
    return f"⟦UNTRUSTED_DATA source={source}⟧\n{name}\n⟦/UNTRUSTED_DATA⟧"


def test_every_item_is_labelled_and_untrusted_key_names_are_fenced(monkeypatch) -> None:
    """A key name read from an untrusted row on either side of a change is
    fenced like a value; an item read only from trusted rows is not."""
    import okg.substrate.mcp.history_reads as history_reads
    import okg.substrate.mcp.provenance as provenance

    trust = {"inventory": "trusted", "scraper": "untrusted", None: "trusted"}
    monkeypatch.setattr(
        provenance, "classify_sources",
        lambda conn, *, deployment_name, sources: {s: trust[s] for s in sources},
    )
    keys = {"added_keys": ["gpus"], "removed_keys": [], "changed_keys": ["cpus"]}
    trusted = {"change": "changed", **keys}
    newer_untrusted = {"change": "changed", **keys}
    older_untrusted = {"change": "changed", **keys}
    oldest = {"change": "oldest_shown"}
    history_reads.label_items(None, _session(), [
        (trusted, [("inventory", {}), ("inventory", {}), (None, {})]),
        (newer_untrusted, [("scraper", {}), ("inventory", {})]),
        (older_untrusted, [("inventory", {}), ("scraper", {})]),
        (oldest, [("inventory", {})]),
    ])
    assert trusted["provenance"] == {"source": "inventory", "trust": "trusted"}
    assert trusted["changed_keys"] == ["cpus"] and trusted["added_keys"] == ["gpus"]
    assert newer_untrusted["provenance"] == {"source": "scraper", "trust": "untrusted"}
    assert newer_untrusted["changed_keys"] == [_fenced("cpus", "scraper")]
    assert newer_untrusted["added_keys"] == [_fenced("gpus", "scraper")]
    # Labelled as the row it reads as, fenced for the untrusted older row.
    assert older_untrusted["provenance"]["trust"] == "trusted"
    assert older_untrusted["changed_keys"] == [_fenced("cpus", "scraper")]
    assert oldest == {"change": "oldest_shown", "provenance": {"source": "inventory", "trust": "trusted"}}
