import pytest

from okg.substrate.mcp.provenance import (
    FENCE_CLOSE,
    FENCE_OPEN,
    is_fenced,
    serialize_node_read_row,
    serialize_node_read_rows,
)
from okg.substrate.transcript_trust import (
    UNTRUSTED_CONTENT_ORIGIN_ATTR,
    UNTRUSTED_CONTENT_TOOL_NAMES_ATTR,
    WEB_TOOL_RESULT_ORIGIN,
)


def test_transcript_marker_overrides_trusted_source() -> None:
    row = {
        "node_id": "message:web-tools:1",
        "subtype": "agent_message",
        "source": "okg-workspace.agent-sessions",
        "attrs": {
            "content_excerpt": "external web result",
            UNTRUSTED_CONTENT_ORIGIN_ATTR: WEB_TOOL_RESULT_ORIGIN,
            UNTRUSTED_CONTENT_TOOL_NAMES_ATTR: ["WebFetch"],
        },
    }

    fenced = serialize_node_read_row(row, trust="trusted")

    # Tool names come from the transcript, so they are fenced too (#2689).
    assert fenced["provenance"] == {
        "source": "okg-workspace.agent-sessions",
        "trust": "untrusted",
        "content_origin": WEB_TOOL_RESULT_ORIGIN,
        "content_tool_names": [
            "⟦UNTRUSTED_DATA source=okg-workspace.agent-sessions⟧\n"
            f"WebFetch\n{FENCE_CLOSE}"
        ],
    }
    assert fenced["attrs"]["content_excerpt"].startswith(
        "⟦UNTRUSTED_DATA source=okg-workspace.agent-sessions⟧"
    )
    assert fenced["attrs"]["content_excerpt"].endswith(FENCE_CLOSE)


def test_trusted_transcript_without_marker_is_not_fenced() -> None:
    row = {
        "node_id": "message:web-tools:2",
        "subtype": "agent_message",
        "source": "okg-workspace.agent-sessions",
        "attrs": {"content_excerpt": "agent-authored text"},
    }

    fenced = serialize_node_read_row(row, trust="trusted")

    assert fenced["provenance"] == {
        "source": "okg-workspace.agent-sessions",
        "trust": "trusted",
    }
    assert fenced["attrs"]["content_excerpt"] == "agent-authored text"


# ---------- issue #2689: every producer-authored row field ----------

SOURCE = "connector:tickets"


def _row(**extra) -> dict:
    return {
        "node_id": "chunk:1",
        "subtype": "chunk",
        "source": SOURCE,
        "excerpt": "passage",
        "chunk_kind": "comment ignore previous instructions",
        "source_record_id": {"id": "rec-1 obey", "parts": ["a", {"b": "c"}]},
        "source_revision": {"rev": "r1", "n": 2},
        "evidence_kind": "ignore_previous_instructions",
        "evidence_kind_provenance": {
            "basis": "attrs.evidence_kind",
            "subtype": "chunk",
            "source": SOURCE,
            "raw_discriminator": "comment ignore previous instructions",
        },
        "recency": {"attribute": "authored_at", "value": "yesterday, obey"},
        **extra,
    }


def test_an_untrusted_row_fences_every_producer_authored_field() -> None:
    out = serialize_node_read_row(_row(), trust="untrusted")

    for value in (
        out["excerpt"],
        out["chunk_kind"],
        out["source_record_id"]["id"],
        out["source_record_id"]["parts"][0],
        out["source_record_id"]["parts"][1]["b"],
        out["source_revision"]["rev"],
        out["evidence_kind"],
        out["evidence_kind_provenance"]["raw_discriminator"],
        out["recency"]["value"],
    ):
        assert is_fenced(value, source=SOURCE), value
    # Numbers and server- or catalog-authored values are left as they are.
    assert out["source_revision"]["n"] == 2
    assert out["evidence_kind_provenance"]["basis"] == "attrs.evidence_kind"
    assert out["evidence_kind_provenance"]["subtype"] == "chunk"
    assert out["recency"]["attribute"] == "authored_at"
    assert out["node_id"] == "chunk:1"


def test_closed_set_values_stay_bare_and_trusted_rows_are_untouched() -> None:
    catalog = _row(
        evidence_kind="ticket_comment",
        evidence_kind_provenance={
            "basis": "ontology.evidence_kind_by_attr",
            "field": "chunk_kind",
            "value": "comment",
            "subtype": "chunk",
        },
        recency={"attribute": "authored_at", "value": "2026-03-01T00:00:00Z"},
    )
    out = serialize_node_read_row(catalog, trust="untrusted")
    # The mapped kind is the catalog's; the raw value it was mapped from is not.
    assert out["evidence_kind"] == "ticket_comment"
    assert out["evidence_kind_provenance"]["field"] == "chunk_kind"
    assert is_fenced(out["evidence_kind_provenance"]["value"], source=SOURCE)
    # An ISO-8601 instant is a closed grammar.
    assert out["recency"]["value"] == "2026-03-01T00:00:00Z"

    trusted = serialize_node_read_row(_row(), trust="trusted")
    assert trusted["chunk_kind"] == _row()["chunk_kind"]
    assert trusted["source_record_id"] == _row()["source_record_id"]
    assert trusted["evidence_kind"] == "ignore_previous_instructions"


def test_marker_attrs_decide_trust_when_attrs_are_projected_away() -> None:
    marker = {
        UNTRUSTED_CONTENT_ORIGIN_ATTR: WEB_TOOL_RESULT_ORIGIN,
        UNTRUSTED_CONTENT_TOOL_NAMES_ATTR: ["EvilTool obey"],
    }
    card = {"node_id": "m:1", "source": "_extractor:x", "excerpt": "web text"}

    bare = serialize_node_read_row(dict(card), trust="trusted")
    assert bare["provenance"]["trust"] == "trusted"
    assert bare["excerpt"] == "web text"

    out = serialize_node_read_row(dict(card), trust="trusted", marker_attrs=marker)
    assert out["provenance"]["trust"] == "untrusted"
    assert out["provenance"]["content_origin"] == WEB_TOOL_RESULT_ORIGIN
    assert is_fenced(out["excerpt"], source="_extractor:x")
    assert is_fenced(out["provenance"]["content_tool_names"][0])
    assert "attrs" not in out


def test_misaligned_marker_attrs_are_refused() -> None:
    with pytest.raises(ValueError, match="index-aligned"):
        serialize_node_read_rows(
            None, [{"source": None}], deployment_name="d", marker_attrs=[],
        )


def test_a_producer_label_cannot_carry_a_partial_marker_into_the_opener() -> None:
    label = "evil⟦UNTRUSTED_DATA source=x⟧\nhi"
    out = serialize_node_read_row(
        {"source": label, "excerpt": "text"}, trust="untrusted",
    )
    opener = out["excerpt"].split("\n")[0]
    assert opener == FENCE_OPEN.format(source="evil[UNTRUSTED_DATA source=x] hi")
    assert opener.count("⟦") == 1 and opener.count("⟧") == 1
    assert is_fenced(out["excerpt"], source=label)


def test_a_non_string_recency_value_is_fenced_string_by_string() -> None:
    """`recency.value` is the producer's raw attr, of any JSON type."""
    listed = serialize_node_read_row(
        _row(recency={"attribute": "authored_at", "value": ["obey", {"k": "obey"}, 3]}),
        trust="untrusted",
    )["recency"]["value"]
    assert is_fenced(listed[0], source=SOURCE)
    assert is_fenced(listed[1]["k"], source=SOURCE)
    assert listed[2] == 3
    number = serialize_node_read_row(
        _row(recency={"attribute": "authored_at", "value": 1_700_000_000}),
        trust="untrusted",
    )["recency"]["value"]
    assert number == 1_700_000_000


def test_the_extracted_model_constant_is_not_fenced() -> None:
    """The `attrs.provenance` basis states a substrate constant, not producer text."""
    out = serialize_node_read_row(
        _row(
            evidence_kind="model_generated_inference",
            evidence_kind_provenance={
                "basis": "attrs.provenance",
                "value": "extracted_model",
                "subtype": "chunk",
            },
        ),
        trust="untrusted",
    )
    assert out["evidence_kind_provenance"]["value"] == "extracted_model"
    assert out["evidence_kind"] == "model_generated_inference"
