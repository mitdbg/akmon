"""The id-to-console-URL contract (PACT chat-deep-links).

Since PACT operator-ui-remove-four-views the console serves Home and Health
only. The Graph view showed records and the Trace view showed sessions,
producer runs and MCP traces; both are deleted, so NO declared kind has a
view and the renderer emits no URL at all. These tests pin that: each kind
refuses with ``NO_CONSOLE_VIEW`` and renders its identifier as plain text.

The failure mode this guards is silent. The console routes an unrecognized
hash to Home, so a link emitted for a deleted view would not raise, would not
404, and would simply land the reader on the wrong page — reading as though
the answer were wrong. Only an assertion catches that.

The companion vitest
(``src/okg/operator_ui/web/src/operator-ui/shared/data/deepLinks.test.tsx``)
reads the fixture this module builds: it checks that every case carries no
URL, and that each retired hash still resolves to Home in the console's own
``parseViewHash``. When a view that shows one of these objects lands, its
kind leaves ``_KINDS_WITHOUT_A_VIEW``, gains a linked fixture entry, and the
grammar-parity assertions come back with it.
"""

from __future__ import annotations

import json
import hashlib
import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote

import pytest
import yaml

from okg.substrate.chat.deep_links import (
    _KINDS_WITHOUT_A_VIEW,
    EMPTY_ID_TEXT,
    REFUSAL_REASONS,
    ENVELOPE_GENERATION_FIELD,
    ENVELOPE_KIND_FIELD,
    LinkTarget,
    Refusal,
    RenderOutcome,
    SUBTYPE_TO_TARGET_KIND,
    TargetKind,
    deep_link,
    encode_id,
    link_target_from_row,
    render_links,
    target_kind_from_envelope,
)
from tests.mirror_scope import private_path

#: A verbatim MCP ``search`` response captured from a live deployment
#: (PACT issue-1039-chat-deeplinks), kept as a test fixture.
LIVE_CAPTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/chat_deep_links/live_mcp_search_capture.json"
)
LIVE_CAPTURE_SHA256 = "8164d9571f0a8d8209f6ec4d55ca5a3830192056f7494fdcb16507a1f1901e9a"

# A base carrying a PATH PREFIX on purpose: operator_ui_base is defined as
# the URL where the console index is served, prefix included. If the
# renderer ever invented a segment of its own, this base would expose it.
BASE = "https://okg.example.org/operator/"

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "deep_link_urls.json"

# A node id carrying both separators that matter. The console's
# encodeURIComponent and Python's quote(safe="") must agree on these byte
# for byte, which the vitest re-checks against instancesSelectionHash.
PARITY_NODE_ID = "git://example-org/example-repo:src/okg/substrate/chat/deep_links.py"
PARITY_GENERATION = "gen:2026-08-03T00:00:00Z/17"


# --------------------------------------------------------------------------
# No kind has a view: every declared kind renders as text
# --------------------------------------------------------------------------


REFUSED_TARGETS: list[tuple[str, LinkTarget]] = [
    ("node", LinkTarget(id="node-42", kind=TargetKind.NODE)),
    (
        "node_pinned",
        LinkTarget(id="node-42", kind=TargetKind.NODE, generation="gen-9"),
    ),
    ("agent_session", LinkTarget(id="sess-7", kind=TargetKind.AGENT_SESSION)),
    (
        "producer_run",
        LinkTarget(
            id="run-2026-08-03-014",
            kind=TargetKind.PRODUCER_RUN,
            producer_key="git.commits",
        ),
    ),
    ("mcp_trace", LinkTarget(id="trc-abc123", kind=TargetKind.MCP_TRACE)),
]


@pytest.mark.parametrize(
    "target",
    [target for _, target in REFUSED_TARGETS],
    ids=[name for name, _ in REFUSED_TARGETS],
)
def test_every_declared_kind_renders_as_text_with_a_reason(
    target: LinkTarget,
) -> None:
    """No remaining console view shows any of these objects.

    PACT operator-ui-remove-four-views deleted the Graph view (records) and
    the Trace view (sessions, producer runs, MCP traces). The identifiers
    still name what they are, so the answer keeps them as text with a stated
    reason. A link would open Home and read as though the answer were wrong.
    """
    link = deep_link(target, operator_ui_base=BASE)

    assert link.url is None
    assert link.refusal is Refusal.NO_CONSOLE_VIEW
    assert link.reason is not None and "no view" in link.reason
    assert link.text == target.id
    # The refusal must not leak a hash or a host the reader could follow.
    rendered = link.to_markdown()
    assert rendered == target.id
    assert "http" not in rendered
    assert "#" not in rendered


def test_the_view_less_set_is_exactly_every_declared_kind() -> None:
    """The set and the enum cannot drift apart unnoticed.

    Add a kind without a view and it would fall through to UNKNOWN_KIND,
    which says "the console does not route this" — a different and wrong
    reason. Add a VIEW and this test is the one that must be updated, in the
    same change, deliberately.
    """
    assert _KINDS_WITHOUT_A_VIEW == set(TargetKind)


def test_no_declared_kind_produces_a_url_at_all() -> None:
    """The whole-module claim, not one kind at a time.

    `list(...)` rather than iterating the Enum class directly: CodeQL reads a
    bare `for kind in TargetKind` as iterating a non-iterable class and
    reports py/non-iterable-in-for-loop, which is an error-severity finding.
    """
    for kind in list(TargetKind):
        link = deep_link(LinkTarget(id="x-1", kind=kind), operator_ui_base=BASE)
        assert link.url is None, kind
        assert not link.linked, kind


# --------------------------------------------------------------------------
# Encoding
# --------------------------------------------------------------------------


NASTY_IDS = [
    "node#1",
    "a&b",
    "who?what",
    "git://example-org/example-repo:src/okg.py",
    "two words",
    "generación-ünïcødé",
    "mix: /a&b?c#d é",
]


@pytest.mark.parametrize("raw", NASTY_IDS)
def test_ids_are_percent_encoded_and_survive_a_decode_round_trip(raw: str) -> None:
    """``encode_id`` is tested directly while no kind emits a URL.

    It is still the module's exported encoder and the next view to land will
    build its hash with it, so letting this lapse would hand that change an
    untested encoder.
    """
    encoded = encode_id(raw)

    assert encoded == quote(raw, safe="")
    assert unquote(encoded) == raw
    # parse_qs decodes exactly as URLSearchParams does in the console.
    assert parse_qs(f"focus={encoded}")["focus"] == [raw]


def test_encoding_matches_encodeuricomponent_for_the_separators_that_matter() -> None:
    """/ and : are the separators OKG node ids actually carry."""
    assert encode_id("git://example-org/example-repo:src") == "git%3A%2F%2Fexample-org%2Fexample-repo%3Asrc"


def test_encoding_divergence_from_encodeuricomponent_is_pinned_not_discovered() -> None:
    """quote(safe="") escapes six sub-delimiters encodeURIComponent leaves.

    Both forms decode identically through URLSearchParams, so navigation is
    unaffected — but the two emitters are NOT byte-identical for ids
    carrying these characters. Pinning the set here means a future change
    to either emitter fails a test instead of quietly widening the gap.
    """
    diverging = "!*'()"
    for char in diverging:
        assert encode_id(char) == f"%{ord(char):02X}"
    # The property that actually matters is preserved:
    assert unquote(encode_id(diverging)) == diverging


def test_the_separator_carrying_parity_id_still_encodes_byte_for_byte() -> None:
    """The console had an emitter to compare bytes against; it is deleted.

    ``instancesSelectionHash`` lived in the Graph view. Until a view brings
    a console-side emitter back there is nothing to compare against, so what
    survives is the encoder's own output for the id that carries both
    separators OKG node ids actually use.
    """
    assert encode_id(PARITY_NODE_ID) == (
        "git%3A%2F%2Fexample-org%2Fexample-repo%3Asrc%2Fokg%2Fsubstrate%2Fchat%2Fdeep_links.py"
    )
    assert encode_id(PARITY_GENERATION) == "gen%3A2026-08-03T00%3A00%3A00Z%2F17"


# --------------------------------------------------------------------------
# Refusal (a): the kind is not envelope-declared, or is not one we route
# --------------------------------------------------------------------------


def test_an_id_with_no_envelope_declared_kind_renders_plain_text() -> None:
    link = deep_link(LinkTarget(id="who-knows-1", kind=None), operator_ui_base=BASE)

    rendered = link.to_markdown()
    assert link.url is None
    assert link.refusal is Refusal.UNDECLARED_KIND
    assert rendered == "who-knows-1"
    assert "http" not in rendered
    assert "#" not in rendered
    assert link.reason is not None and "http" not in link.reason


@pytest.mark.parametrize("empty", ["", "   "])
def test_an_empty_identifier_renders_a_visible_marker_not_nothing(empty: str) -> None:
    """The one refusal that could silently delete the token.

    Every other refusal still shows the identifier it refused. An empty id
    has nothing to show, so it must render a marker rather than collapse to
    the empty string and vanish from the answer.
    """
    link = deep_link(LinkTarget(id=empty, kind=TargetKind.NODE), operator_ui_base=BASE)

    assert link.url is None
    assert link.refusal is Refusal.EMPTY_ID
    assert link.to_markdown() == EMPTY_ID_TEXT
    assert link.to_markdown().strip() != ""
    assert "http" not in link.to_markdown()


def test_an_unrecognized_envelope_kind_is_not_declared_at_all() -> None:
    """A kind the console does not route reads as undeclared, never guessed."""
    assert target_kind_from_envelope({ENVELOPE_KIND_FIELD: "edge"}) is None
    assert target_kind_from_envelope({ENVELOPE_KIND_FIELD: "  "}) is None
    assert target_kind_from_envelope({}) is None
    assert target_kind_from_envelope(None) is None


def test_kind_is_read_from_the_envelope_never_inferred_from_the_id_shape() -> None:
    """An id that LOOKS like a session is still plain text without a kind.

    String-shape inference is precisely how a run key becomes a trace=
    link, so the absence of a declared kind must not be repaired by
    pattern-matching the identifier.
    """
    session_shaped = "session-2026-08-03-abc"
    row = {"session_id": session_shaped, "node_id": None}

    target = link_target_from_row(row, envelope={})
    assert target is not None
    assert target.kind is None

    link = deep_link(target, operator_ui_base=BASE)
    assert link.url is None
    assert link.to_markdown() == session_shaped


def test_row_typed_by_its_envelope_produces_the_declared_kind() -> None:
    target = link_target_from_row(
        {"node_id": "n-9", "subtype": "source_file"},
        envelope={
            ENVELOPE_KIND_FIELD: "node",
            ENVELOPE_GENERATION_FIELD: "gen-3",
        },
    )

    assert target == LinkTarget(id="n-9", kind=TargetKind.NODE, generation="gen-3")


def test_real_mcp_node_row_uses_authoritative_subtype_and_keeps_generation() -> None:
    """The live node envelope has subtype, not target_kind."""
    payload = json.loads(LIVE_CAPTURE.read_text())
    envelope = {key: value for key, value in payload.items() if key != "results"}
    row = payload["results"][0]

    assert ENVELOPE_KIND_FIELD not in envelope
    target = link_target_from_row(row, envelope)
    assert target is not None
    assert target.kind is TargetKind.NODE
    assert target.id == row["node_id"]
    assert target.generation == payload[ENVELOPE_GENERATION_FIELD]

    # The row is still read correctly end to end; what changed is that the
    # kind it resolves to has no view to open.
    link = deep_link(target, operator_ui_base=BASE)
    assert not link.linked
    assert link.url is None
    assert link.refusal is Refusal.NO_CONSOLE_VIEW
    assert link.to_markdown() == row["node_id"]


def test_live_capture_is_immutable_and_is_the_row_under_test() -> None:
    raw = LIVE_CAPTURE.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == LIVE_CAPTURE_SHA256
    payload = json.loads(raw)
    row = payload["results"][0]
    assert payload["deployment"] == "okg-ci"
    assert row["subtype"] == "verification_step"
    assert "target_kind" not in row


def test_subtype_route_table_is_closed_and_has_an_explicit_refusal_entry() -> None:
    assert SUBTYPE_TO_TARGET_KIND["source_file"] is TargetKind.NODE
    assert SUBTYPE_TO_TARGET_KIND["chat_message"] is None

    unknown = link_target_from_row(
        {"node_id": "session:looks-like-a-session", "subtype": "future_kind"},
        envelope={"pinned_generation_id": "gen-1"},
    )
    assert unknown is not None
    assert unknown.refusal is Refusal.UNKNOWN_SUBTYPE
    assert deep_link(unknown, operator_ui_base=BASE).to_markdown() == (
        "session:looks-like-a-session"
    )

    edge = link_target_from_row(
        {"edge_id": "edge:1", "edge_type": "contains", "src": "a", "dst": "b"},
        envelope={"pinned_generation_id": "gen-1"},
    )
    assert edge.refusal is Refusal.MALFORMED_ROW
    assert edge.id == "edge:1"
    mixed = link_target_from_row(
        {"node_id": "node:1", "subtype": "source_file", "edge_id": "edge:1"},
        envelope={"pinned_generation_id": "gen-1"},
    )
    assert mixed.refusal is Refusal.MALFORMED_ROW
    malformed_mixed = link_target_from_row(
        {"node_id": "node:1", "subtype": "source_file", "edge_id": 7},
        envelope={"pinned_generation_id": "gen-1"},
    )
    assert malformed_mixed.refusal is Refusal.MALFORMED_ROW

    unroutable = link_target_from_row(
        {"node_id": "chat-message:1", "subtype": "chat_message"},
        envelope={"pinned_generation_id": "gen-1"},
    )
    assert unroutable is not None
    assert unroutable.refusal is Refusal.UNROUTABLE_SUBTYPE
    assert not deep_link(unroutable, operator_ui_base=BASE).linked


def _registry_subtypes(path: Path) -> set[str]:
    registry = yaml.safe_load(path.read_text())
    active: set[str] = set()

    def collect(value: object) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key in {"subtype", "src_subtype", "dst_subtype"} and isinstance(child, str):
                    active.add(child)
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(registry)
    return active


def test_active_workspace_subtypes_are_explicitly_classified() -> None:
    # okg-workspace is this repository's own deployment; the public copy
    # skips this test (tests/mirror_scope.py) and runs the one below.
    active = _registry_subtypes(private_path("deployments/okg-workspace/source_registry.yaml"))
    assert active <= set(SUBTYPE_TO_TARGET_KIND)


def test_codebase_index_template_subtypes_are_explicitly_classified() -> None:
    """The same rule over the shipped codebase-index template's registry."""
    template = (
        Path(__file__).resolve().parents[3]
        / "src/okg/substrate/library/templates/codebase-index/source_registry.yaml"
    )
    active = _registry_subtypes(template)
    assert active, "the template declares no subtypes; the check would be vacuous"
    assert active <= set(SUBTYPE_TO_TARGET_KIND), active - set(SUBTYPE_TO_TARGET_KIND)


def test_node_subtype_is_authoritative_even_if_target_kind_is_present() -> None:
    target = link_target_from_row(
        {"node_id": "node:1", "subtype": "future_kind"},
        {ENVELOPE_KIND_FIELD: "node", ENVELOPE_GENERATION_FIELD: "gen-1"},
    )
    assert target is not None
    assert target.refusal is Refusal.UNKNOWN_SUBTYPE
    assert not deep_link(target, operator_ui_base=BASE).linked


@pytest.mark.parametrize(
    "row",
    [
        {"node_id": "node:missing-subtype"},
        {"subtype": "source_file"},
        {"edge_id": "edge:1", "edge_type": "contains", "src": "a", "dst": "b"},
    ],
)
def test_malformed_or_edge_rows_render_a_visible_plain_text_refusal(
    row: dict[str, object],
) -> None:
    target = link_target_from_row(row, {ENVELOPE_GENERATION_FIELD: "gen-1"})
    link = deep_link(target, operator_ui_base=BASE)
    assert link.url is None
    assert link.to_markdown().strip()
    assert "http" not in link.to_markdown()
    assert "#" not in link.to_markdown()
    assert link.refusal is Refusal.MALFORMED_ROW


# --------------------------------------------------------------------------
# Refusal (b): no declared console base
# --------------------------------------------------------------------------


@pytest.mark.parametrize("base", [None, "", "   "])
def test_an_absent_base_still_renders_plain_text_and_never_a_default_host(
    base: str | None,
) -> None:
    """The base check is not reached while no kind routes — and must not be
    weakened on the way past.

    The kind refuses first, so the reader is told "no view shows this"
    rather than "this deployment declares no console base". Either way the
    output is the bare identifier, and no host is invented anywhere.
    """
    link = deep_link(LinkTarget(id="node-42", kind=TargetKind.NODE), operator_ui_base=base)

    rendered = link.to_markdown()
    assert link.url is None
    assert rendered == "node-42"
    assert link.reason is not None
    # No default host of any kind. Default-value-as-fallback is a P0 here.
    for forbidden in ("localhost", "127.0.0.1", "http", "//"):
        assert forbidden not in rendered
        assert forbidden not in link.reason


def test_the_missing_base_refusal_text_survives_for_the_next_routed_kind() -> None:
    """MISSING_BASE is unreachable today, and stays honest for when it is not.

    Deleting it with the routes would mean the next view to land has to
    re-derive the no-default-host rule from scratch. Its reason must name the
    setting and hand nobody a base to copy.
    """
    reason = REFUSAL_REASONS[Refusal.MISSING_BASE]

    assert "operator_ui_base" in reason
    for forbidden in ("localhost", "127.0.0.1", "http", "//"):
        assert forbidden not in reason


# --------------------------------------------------------------------------
# Refusal (c): an empty render is distinguishable from a failed one
# --------------------------------------------------------------------------


def test_no_targets_is_distinguishable_from_everything_refusing() -> None:
    """The distinction matters MORE now, not less.

    ALL_REFUSED is the ordinary outcome while no view exists, so collapsing
    it into "no links" would hide every citation an answer made behind an
    answer that merely looks quiet.
    """
    nothing_cited = render_links([], operator_ui_base=BASE)
    ids_cited_but_unlinkable = render_links(
        [LinkTarget(id="node-42", kind=TargetKind.NODE)], operator_ui_base=BASE,
    )

    assert nothing_cited.outcome is RenderOutcome.NO_TARGETS
    assert ids_cited_but_unlinkable.outcome is RenderOutcome.ALL_REFUSED

    # Both cases have zero links; only the outcome tells an answer that cited
    # unlinkable ids apart from one that cited none.
    assert nothing_cited.linked_count == 0
    assert nothing_cited.refused_count == 0
    assert ids_cited_but_unlinkable.linked_count == 0
    assert ids_cited_but_unlinkable.refused_count == 1


def test_a_mixed_batch_refuses_every_target_and_keeps_each_identifier() -> None:
    """Nothing links, and nothing is dropped.

    Each refusal keeps its own reason: a declared kind with no view is a
    different fact from an id whose kind the envelope never declared, and an
    operator reading the answer should be able to tell them apart.
    """
    result = render_links(
        [
            LinkTarget(id="node-42", kind=TargetKind.NODE),
            LinkTarget(id="mystery", kind=None),
        ],
        operator_ui_base=BASE,
    )

    assert result.outcome is RenderOutcome.ALL_REFUSED
    assert result.linked_count == 0
    assert result.refused_count == 2
    assert [link.to_markdown() for link in result.links] == ["node-42", "mystery"]
    assert result.links[0].refusal is Refusal.NO_CONSOLE_VIEW
    assert result.links[1].refusal is Refusal.UNDECLARED_KIND


# --------------------------------------------------------------------------
# The no-dependency claim, asserted rather than assumed
# --------------------------------------------------------------------------

FORBIDDEN_MODULE_PREFIXES = (
    "open_webui",
    "openwebui",
    "psycopg",
    "psycopg2",
    "sqlalchemy",
    "dbos",
    "okg.substrate.db",
    "okg.substrate.graph_store",
    "okg.substrate.publisher",
    "okg.viz",
    "okg.operator_ui",
)

_IMPORT_PROBE = (
    "import json, sys;"
    " import okg.substrate.chat.deep_links as m;"
    " print(json.dumps(sorted(sys.modules)))"
)


def test_importing_the_contract_pulls_in_no_openwebui_console_or_database_module() -> None:
    """A POSITIVE assertion, in a clean interpreter.

    Asserting this in-process would be vacuous: tests/substrate/conftest.py
    imports psycopg before any test runs, so the pytest process always
    carries a database driver. The claim is about the MODULE's import
    graph, so it is checked in a fresh interpreter that imports nothing
    else.
    """
    completed = subprocess.run(
        [sys.executable, "-c", _IMPORT_PROBE],
        capture_output=True,
        text=True,
        check=True,
    )
    loaded = json.loads(completed.stdout)

    assert "okg.substrate.chat.deep_links" in loaded
    offenders = [
        name
        for name in loaded
        if any(
            name == prefix or name.startswith(prefix + ".")
            for prefix in FORBIDDEN_MODULE_PREFIXES
        )
    ]
    assert offenders == [], f"deep_links dragged in {offenders}"


# --------------------------------------------------------------------------
# The fixture the console-side parity test consumes
# --------------------------------------------------------------------------


#: Hashes the console used to serve and no longer does. Old chat answers and
#: bookmarks still carry them, and the vitest asserts each one resolves to
#: Home in ``parseViewHash`` — the console's half of "a dead hash lands
#: somewhere honest rather than on a blank page".
RETIRED_HASHES = [
    # The two pages the console replaced (PACT console-default-remove-old-views).
    "#home",
    "#health?producer=git.commits",
    "#graph?mode=instances&focus=node-42",
    "#graph?mode=instances&focus=node-42&generation=gen-9",
    "#trace?session=sess-7",
    "#trace?mode=producers&producer=git.commits&run=run-2026-08-03-014",
    "#trace?mode=producers&trace=trc-abc123",
    "#performance",
    "#deployment",
]


def _fixture_entries() -> list[dict[str, object]]:
    """One case per declared kind, each proving it emits no URL.

    The entries are written from the INPUT triple, never read back out of the
    renderer's output — a fixture derived from the output would be asserting
    the renderer against itself and could never fail.
    """
    cases: list[dict[str, object]] = []

    def add(name: str, target: LinkTarget) -> None:
        link = deep_link(target, operator_ui_base=BASE)
        assert link.url is None, name
        assert link.refusal is Refusal.NO_CONSOLE_VIEW, name
        cases.append({
            "name": name,
            "input": {
                "kind": target.kind.value if target.kind else None,
                "id": target.id,
                "generation": target.generation,
                "producer_key": target.producer_key,
            },
            "expect": {"refusal": Refusal.NO_CONSOLE_VIEW.value},
            "url": None,
            "rendered": link.to_markdown(),
        })

    add("node-plain", LinkTarget(id="node-42", kind=TargetKind.NODE))
    add(
        "node-pinned-generation",
        LinkTarget(id="node-42", kind=TargetKind.NODE, generation="gen-9"),
    )
    add(
        "node-with-separators",
        LinkTarget(
            id=PARITY_NODE_ID, kind=TargetKind.NODE, generation=PARITY_GENERATION,
        ),
    )
    add(
        "node-unicode-and-spaces",
        LinkTarget(id="mix: /a&b?c#d é", kind=TargetKind.NODE),
    )
    add("agent-session", LinkTarget(id="sess-7", kind=TargetKind.AGENT_SESSION))
    add(
        "producer-run",
        LinkTarget(
            id="run-2026-08-03-014",
            kind=TargetKind.PRODUCER_RUN,
            producer_key="git.commits",
        ),
    )
    add("mcp-trace", LinkTarget(id="trc-abc123", kind=TargetKind.MCP_TRACE))
    return cases


def build_fixture_payload() -> dict[str, object]:
    """The parity fixture's contents, derived from the emitter.

    Exported for ``scripts/regenerate_deep_link_fixture.py``. The TEST never
    writes this — see ``test_committed_parity_fixture_matches_the_emitter``.
    """
    payload: dict[str, object] = {
        "generated_by": "tests/substrate/chat/test_deep_links.py",
        "grammar_authority": (
            "src/okg/operator_ui/web/src/operator-ui/app/HomeApp.tsx "
            "(consoleHashFor)"
        ),
        "operator_ui_base": BASE,
        "entries": _fixture_entries(),
        "subtype_cases": _subtype_fixture_cases(),
        # Hashes the app no longer serves. The vitest asserts each one opens
        # the console's Home rather than a blank or broken view.
        "retired_hashes": RETIRED_HASHES,
    }
    return payload


def _subtype_fixture_cases() -> list[dict[str, object]]:
    """Cross-language cases for the live-subtype and refusal arms."""
    live_payload = json.loads(LIVE_CAPTURE.read_text())
    live_envelope = {
        key: value for key, value in live_payload.items() if key != "results"
    }
    live_target = link_target_from_row(live_payload["results"][0], live_envelope)
    assert live_target is not None and live_target.kind is TargetKind.NODE
    live_link = deep_link(live_target, operator_ui_base=BASE)
    # The live row still resolves to a node; the node kind just has no view.
    assert live_link.url is None
    assert live_link.refusal is Refusal.NO_CONSOLE_VIEW

    cases: list[dict[str, object]] = [{
        "name": "live-mcp-subtype-node",
        "subtype": live_payload["results"][0]["subtype"],
        "id": live_target.id,
        "url": None,
        "rendered": live_link.to_markdown(),
        "expect": {"refusal": Refusal.NO_CONSOLE_VIEW.value},
    }]
    for name, row, expected_refusal in (
        (
            "unknown-subtype",
            {"node_id": "session:future", "subtype": "future_kind"},
            Refusal.UNKNOWN_SUBTYPE,
        ),
        (
            "intentionally-unroutable-subtype",
            {"node_id": "chat-message:1", "subtype": "chat_message"},
            Refusal.UNROUTABLE_SUBTYPE,
        ),
        (
            "missing-node-id",
            {"subtype": "source_file"},
            Refusal.MALFORMED_ROW,
        ),
    ):
        target = link_target_from_row(
            row, {ENVELOPE_GENERATION_FIELD: "gen-refusal"},
        )
        assert target.refusal is expected_refusal
        rendered = deep_link(target, operator_ui_base=BASE)
        assert rendered.url is None
        cases.append({
            "name": name,
            "subtype": row["subtype"],
            "id": target.id,
            "url": None,
            "rendered": rendered.to_markdown(),
            "expect": {"refusal": expected_refusal.value},
        })

    edge_target = link_target_from_row(
        {"edge_id": "edge:1", "edge_type": "contains", "src": "a", "dst": "b"},
        {ENVELOPE_GENERATION_FIELD: "gen-refusal"},
    )
    assert edge_target.refusal is Refusal.MALFORMED_ROW
    cases.append({
        "name": "edge-shaped-row",
        "subtype": None,
        "id": edge_target.id,
        "url": None,
        "rendered": deep_link(edge_target, operator_ui_base=BASE).to_markdown(),
        "expect": {"refusal": Refusal.MALFORMED_ROW.value},
    })
    return cases


def render_fixture_text() -> str:
    """The exact bytes the committed fixture must contain."""
    return json.dumps(build_fixture_payload(), indent=2, ensure_ascii=False) + "\n"


def test_committed_parity_fixture_matches_the_emitter() -> None:
    """The fixture is COMPARED, never written by the test run.

    Writing it as a side effect would mean staleness could never fail: the
    file would silently re-derive itself from whatever the emitter currently
    does, so the vitest downstream would keep asserting against a moving
    target and a plain `pytest` would dirty a read-only checkout. Drift is a
    failure here, and regeneration is an explicit, separate act:

        uv run python scripts/regenerate_deep_link_fixture.py
    """
    assert FIXTURE_PATH.exists(), (
        f"{FIXTURE_PATH} is missing; "
        "run scripts/regenerate_deep_link_fixture.py"
    )

    committed = json.loads(FIXTURE_PATH.read_text())
    expected = json.loads(render_fixture_text())

    assert committed == expected, (
        "the committed parity fixture no longer matches the emitter. If the "
        "URL contract changed on purpose, regenerate it with "
        "`uv run python scripts/regenerate_deep_link_fixture.py` and review "
        "the diff — the console-side vitest asserts against this file."
    )


def test_the_parity_fixture_covers_every_declared_kind_and_both_node_shapes() -> None:
    """Guards the fixture against quietly shrinking to a vacuous set.

    Every declared kind must appear, and every entry must carry no URL. A
    fixture that lost its entries would let the vitest pass over an empty
    list while the renderer emitted anything at all.
    """
    committed = json.loads(FIXTURE_PATH.read_text())
    entries = committed["entries"]
    kinds = {entry["input"]["kind"] for entry in entries}

    assert kinds == {kind.value for kind in TargetKind}
    assert len(entries) == 7
    assert all(entry["url"] is None for entry in entries)
    assert all(
        entry["expect"]["refusal"] == Refusal.NO_CONSOLE_VIEW.value
        for entry in entries
    )
    # Every entry keeps its identifier as visible text and nothing else. The
    # equality is the whole claim: an id may itself contain '#' or "http"
    # (one of these does), so scanning for those substrings would be a
    # weaker test that also fails on a legitimate identifier.
    for entry in entries:
        assert entry["rendered"] == entry["input"]["id"]
        assert "](" not in entry["rendered"]

    node_generations = {
        entry["input"]["generation"] is None
        for entry in entries
        if entry["input"]["kind"] == "node"
    }
    # Both the pinned and the UNPINNED node shape stay covered, so the
    # generation field cannot start or stop mattering unnoticed.
    assert node_generations == {True, False}


def test_the_parity_fixture_lists_the_hashes_the_console_retired() -> None:
    """The console's half of the contract: a dead hash lands on Home.

    Without this list the vitest would have nothing to assert and the two
    sides would agree only that nothing links.
    """
    committed = json.loads(FIXTURE_PATH.read_text())
    retired = committed["retired_hashes"]

    assert retired == RETIRED_HASHES
    assert all(hash_.startswith("#") for hash_ in retired)
    # One hash per removed view, so no view drops off the list silently.
    assert {hash_.split("?", 1)[0] for hash_ in retired} == {
        "#home",
        "#health",
        "#graph",
        "#trace",
        "#performance",
        "#deployment",
    }


def test_the_parity_fixture_carries_live_subtype_and_refusal_arms() -> None:
    committed = json.loads(FIXTURE_PATH.read_text())
    cases = committed["subtype_cases"]
    assert {case["name"] for case in cases} == {
        "live-mcp-subtype-node",
        "unknown-subtype",
        "intentionally-unroutable-subtype",
        "missing-node-id",
        "edge-shaped-row",
    }
    live = next(case for case in cases if case["name"] == "live-mcp-subtype-node")
    # The live row resolves to a node and refuses for want of a view, not
    # for any defect in the row.
    assert live["url"] is None
    assert live["expect"]["refusal"] == Refusal.NO_CONSOLE_VIEW.value
    assert live["rendered"] == live["id"]
    assert all(case["url"] is None for case in cases)
    assert all(case["rendered"] is None or "http" not in case["rendered"] for case in cases)
