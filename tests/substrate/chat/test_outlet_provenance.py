"""Outlet provenance rendering (PACT chat-context-audit-filter).

The rule these tests defend is narrow and easy to break by accident: this
filter is a CONSUMER of the chat-deep-links contract and defines no URL
shape of its own. A plausible-looking dead link is worse than plain text,
and every way of producing one here is a string concatenation someone could
add in a hurry.

So the negative assertion is structural as well as behavioral: no URL
appears in the output when no link contract is available, AND no URL-shaped
literal exists in the module's own source.
"""

from __future__ import annotations

import ast
import inspect
import json
from pathlib import Path

import pytest

from okg.substrate.chat import audit_filter as audit_filter_module
from okg.substrate.chat.audit_filter import (
    LINEAGE_UNAVAILABLE,
    ContextAuditFilter,
    FilterSettings,
    render_citations,
)
from okg.substrate.chat.deep_links import (
    ENVELOPE_KIND_FIELD,
    REFUSAL_REASONS,
    Refusal,
)

BASE = "https://okg.example.org/operator/"
G1 = "okg:cooper:branch:default:gen:20260804T035442195199Z:08b1448381a9"

NODE_ENVELOPE = {
    ENVELOPE_KIND_FIELD: "node",
    "pinned_generation_id": G1,
}
ROWS = [
    {
        "node_id": "region:Signal_Region",
        "subtype": "region",
        "source": "cooper_regions",
    },
    {
        "node_id": "consistency_claim:139e1cf727a5d04d",
        "subtype": "consistency_claim",
        "source": "cooper_claims",
    },
]


def _settings(tmp_path: Path, **overrides: object) -> FilterSettings:
    base: dict[str, object] = {
        "deployment": "cooper",
        "preset": "okg-cooper-analyst",
        "openwebui_version": "0.11.0",
        "compaction_enabled": False,
        "operator_ui_base": BASE,
        "state_home": tmp_path / "operator-state",
    }
    base.update(overrides)
    return FilterSettings(**base)  # type: ignore[arg-type]


def _rendered(body: dict) -> str:
    return "\n".join(str(m.get("content", "")) for m in body["messages"])


def test_citation_per_cited_node_with_lineage(tmp_path: Path) -> None:
    filt = ContextAuditFilter(_settings(tmp_path))

    out = filt.outlet(
        {"messages": [{"role": "assistant", "content": "Two regions matched."}]},
        envelopes=[NODE_ENVELOPE],
        rows=ROWS,
        envelope=NODE_ENVELOPE,
    )

    text = _rendered(out)
    assert "Two regions matched." in text, "the answer is not replaced"
    assert text.count("- ") == len(ROWS), "one citation per cited node"

    for row in ROWS:
        assert row["node_id"] in text
        assert row["source"] in text, "the citation carries its source lineage"

    # No view shows a record since PACT operator-ui-remove-four-views, so the
    # deep-links contract's output here is the identifier plus a reason. The
    # citation still names the node and its lineage; it is simply not a link.
    assert "](" not in text, "no markdown link was emitted"
    assert "no view" in text
    assert BASE not in text

    # And the stamp rides along on the same block.
    assert f"generation_id: {G1}" in text


def test_missing_lineage_renders_a_named_absence_not_a_blank(tmp_path: Path) -> None:
    """A row with no source is stated as such, never quietly cited bare."""
    filt = ContextAuditFilter(_settings(tmp_path))

    out = filt.outlet(
        {"messages": [{"role": "assistant", "content": "One row."}]},
        envelopes=[NODE_ENVELOPE],
        rows=[{"node_id": "region:X", "subtype": "region"}],
        envelope=NODE_ENVELOPE,
    )

    assert LINEAGE_UNAVAILABLE in _rendered(out)


@pytest.mark.parametrize("base", [None, BASE])
def test_no_url_constructed_with_or_without_link_contract(
    tmp_path: Path, base: str | None,
) -> None:
    """Plain text with a reason — never a guessed URL, and never a dead one.

    Both bases run on purpose. With a base declared this is what catches a
    link to a deleted view creeping back in; with none it is the original
    no-default-host guarantee.
    """
    filt = ContextAuditFilter(_settings(tmp_path, operator_ui_base=base))

    out = filt.outlet(
        {"messages": [{"role": "assistant", "content": "Two regions matched."}]},
        envelopes=[NODE_ENVELOPE],
        rows=ROWS,
        envelope=NODE_ENVELOPE,
    )

    text = _rendered(out)
    # The ids are still there — the answer does not lose its citations.
    for row in ROWS:
        assert row["node_id"] in text
    # But nothing that could be clicked.
    assert "http://" not in text and "https://" not in text
    assert "](" not in text, "no markdown link was emitted"
    assert "#graph" not in text and "#trace" not in text
    assert REFUSAL_REASONS[Refusal.NO_CONSOLE_VIEW] in text

    # Structural: this module contains no URL shape of its own to fall back
    # on. The grammar lives in deep_links.py and nowhere else.
    source = inspect.getsource(audit_filter_module)
    for forbidden in ("#graph", "#trace", "http://", "https://", "mode=instances"):
        assert forbidden not in source, f"{forbidden!r} is a URL shape in this module"


def test_an_undeclared_kind_refuses_rather_than_guessing_from_the_id(
    tmp_path: Path,
) -> None:
    """An id's string shape is never a route. The envelope declares the kind."""
    filt = ContextAuditFilter(_settings(tmp_path))

    out = filt.outlet(
        {"messages": [{"role": "assistant", "content": "A run."}]},
        envelopes=[{"pinned_generation_id": G1}],
        rows=[{"run_key": "producer:x/run/7", "source": "cooper_runs"}],
        envelope={"pinned_generation_id": G1},  # no target_kind declared
    )

    text = _rendered(out)
    assert "producer:x/run/7" in text
    assert "](" not in text
    assert REFUSAL_REASONS[Refusal.UNDECLARED_KIND] in text


def test_malformed_or_edge_citation_stays_visible_as_plain_text(tmp_path: Path) -> None:
    filt = ContextAuditFilter(_settings(tmp_path))
    out = filt.outlet(
        {"messages": [{"role": "assistant", "content": "Malformed result."}]},
        envelopes=[{"pinned_generation_id": G1}],
        rows=[{"edge_id": "edge:1", "edge_type": "contains", "src": "a", "dst": "b"}],
        envelope={"pinned_generation_id": G1},
    )
    text = _rendered(out)
    # The edge id remains visible as plain text; the refusal reason is the
    # explicit marker that this row was not a routable node citation.
    assert "edge:1" in text
    assert "](" not in text
    assert "http" not in text
    assert "not a node-shaped row" in text


def test_non_mapping_citation_stays_visible_as_plain_text(tmp_path: Path) -> None:
    filt = ContextAuditFilter(_settings(tmp_path))
    out = filt.outlet(
        {"messages": [{"role": "assistant", "content": "Malformed result."}]},
        envelopes=[{"pinned_generation_id": G1}],
        rows=["not-an-MCP-row"],  # type: ignore[list-item]
        envelope={"pinned_generation_id": G1},
    )
    text = _rendered(out)
    assert "(malformed MCP row)" in text
    assert "](" not in text


def test_citations_are_absent_when_no_rows_were_cited(tmp_path: Path) -> None:
    """An answer that cited nothing gets no empty Sources heading."""
    filt = ContextAuditFilter(_settings(tmp_path))

    out = filt.outlet(
        {"messages": [{"role": "assistant", "content": "No rows."}]},
        envelopes=[{"pinned_generation_id": G1}],
        rows=[],
    )

    text = _rendered(out)
    assert "Sources:" not in text
    assert f"generation_id: {G1}" in text, "the stamp is still rendered"


def test_render_citations_builds_no_url_when_the_base_is_empty() -> None:
    """The unit-level negative, so the behavior is not only end-to-end."""
    for base in (None, "", "   "):
        lines = render_citations(ROWS, envelope=NODE_ENVELOPE, operator_ui_base=base)
        assert lines, "the rows must still be cited"
        assert not any("](" in line for line in lines)


LIVE_ENVELOPE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/chat_deep_links/live_mcp_search_capture.json"
)


def test_renders_a_real_mcp_search_envelope(tmp_path: Path) -> None:
    """Against a REAL capture, not a hand-written row shape.

    The fixture is a verbatim ``search`` response from a live deployment
    (``okg mcp-serve --deployment okg-ci``, okg-substrate 1.28.x). Writing
    the row shape by hand is how a renderer ends up reading a field the
    producer does not emit: the first version of ``_lineage_of`` read
    ``row["source"]`` and would have reported every real row's lineage as
    unavailable, because live rows carry ``provenance.source``.

    Two live facts are pinned here as MEASUREMENTS, not as aspirations:
    the envelope carries the pinned generation, and it does NOT declare
    ``target_kind`` — the node rows' authoritative ``subtype`` drives the
    closed route table instead.
    """
    payload = json.loads(LIVE_ENVELOPE.read_text("utf-8"))
    rows = payload["results"]
    assert rows, "an empty fixture would pass every assertion below"
    envelope = {k: v for k, v in payload.items() if k != "results"}

    filt = ContextAuditFilter(_settings(tmp_path))
    out = filt.outlet(
        {"messages": [{"role": "assistant", "content": "Regions matched."}]},
        envelopes=[envelope],
        rows=rows,
        envelope=envelope,
    )
    text = _rendered(out)

    # The stamp comes off the real envelope.
    assert f"generation_id: {payload['pinned_generation_id']}" in text

    # Real lineage, read from the shape the producer actually emits.
    assert "okg-ci.pact" in text
    assert LINEAGE_UNAVAILABLE not in text

    # Every cited node id is present.
    for row in rows:
        assert row["node_id"] in text

    # MEASURED: this envelope declares no target_kind. The real node rows still
    # link because their authoritative subtypes are in the closed table.
    assert ENVELOPE_KIND_FIELD not in envelope
    # The subtypes still resolve to the node kind; that kind simply has no
    # view, so every row renders as text rather than as a link.
    assert text.count("](") == 0
    assert text.count("no view") == len(rows)

    # NOT rendered: provenance.trust is a per-source marker, and showing it
    # beside the generation stamp would read as the per-answer trust_level
    # that nothing produces.
    assert any("trust" in (row.get("provenance") or {}) for row in rows)
    assert "trust" not in text


def test_module_defines_no_url_literal() -> None:
    """Parsed, not grepped: a URL literal anywhere in this module is a defect."""
    tree = ast.parse(inspect.getsource(audit_filter_module))
    literals = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]
    assert literals, "an empty literal set would pass this check vacuously"
    offenders = [
        value
        for value in literals
        if value.startswith(("http://", "https://", "#graph", "#trace"))
    ]
    assert offenders == []
