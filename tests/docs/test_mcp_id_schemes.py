"""expose-mcp-node-lookup / req.lookup.id-schemes-documented (GitHub #641).

The deterministic node-id schemes (``file:<repo>:<path>``,
``symbol:<repo>:<path>:<qname>[:<line>]``, ``manifest:<repo>:<path>``, and
siblings) are documented in ``docs/reference/mcp.md`` so direct
``inspect(node)`` is a first-class access path — and the list is
DOC-LINT-PINNED against the emitting source code so it cannot rot:

- forward: every scheme documented in the id-schemes table must appear as
  an id-minting f-string (``f"<scheme>:{...}"``) in each emitting source
  the row names;
- reverse: every id scheme minted by the CORE code/workspace sources must
  appear in the documented table;
- surface: the MCP server instructions carry the worked path-lookup
  example and the inspect not-found signal.

DB-free by design (reads docs + source text only).
"""
from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RETRIEVAL_DOC = REPO_ROOT / "docs/reference/mcp.md"
SERVER_PY = REPO_ROOT / "src/okg/substrate/mcp/server.py"
SOURCES_DIR = REPO_ROOT / "src/okg/substrate/library/sources"

# The core code/workspace emitting sources (the git/code-graph family a
# workspace-graph deployment like the issue's `marius` runs). Every
# id-minting f-string scheme in these files must be documented. Additional
# documented rows may point at other sources (e.g. doc_corpus.py's
# line-less symbol variant); those are checked forward-only.
CORE_EMITTING_SOURCES: tuple[str, ...] = (
    "_git_graph_files_emit.py",
    "_git_graph_history_emit.py",
    "_code_graph_common.py",
    "code_graph_manifest.py",
)

# An id-minting f-string: f"<scheme>:{...
_EMIT_RE = re.compile(r'f"([a-z_][a-z0-9_]*):\{')
# A documented table row inside the pinned block:
# | `scheme:<...>` | meaning | `path`, `path` |
_ROW_RE = re.compile(
    r"^\|\s*`(?P<scheme>[a-z_][a-z0-9_]*):[^`]*`\s*\|[^|]*\|(?P<sources>[^|]*)\|",
)
_SOURCE_REF_RE = re.compile(r"`([^`]+\.py)`")


def _documented_rows() -> list[tuple[str, list[str]]]:
    text = RETRIEVAL_DOC.read_text(encoding="utf-8")
    assert "<!-- id-schemes:begin -->" in text, (
        f"{RETRIEVAL_DOC} must carry the pinned id-schemes block "
        "(<!-- id-schemes:begin --> ... <!-- id-schemes:end -->)"
    )
    block = text.split("<!-- id-schemes:begin -->", 1)[1]
    block = block.split("<!-- id-schemes:end -->", 1)[0]
    rows: list[tuple[str, list[str]]] = []
    for line in block.splitlines():
        match = _ROW_RE.match(line.strip())
        if not match:
            continue
        sources = _SOURCE_REF_RE.findall(match.group("sources"))
        rows.append((match.group("scheme"), sources))
    return rows


def test_id_schemes_pinned() -> None:
    rows = _documented_rows()
    assert rows, "id-schemes table parsed zero rows"
    documented = {scheme for scheme, _sources in rows}

    # The three schemes the PACT names explicitly must be present.
    for required in ("file", "symbol", "manifest"):
        assert required in documented, (
            f"id-schemes table must document the {required!r} scheme; "
            f"documented: {sorted(documented)}"
        )

    # Forward pin: each documented (scheme, emitting source) pair is real —
    # the named source file exists and mints ids with that scheme prefix.
    for scheme, sources in rows:
        assert sources, (
            f"documented scheme {scheme!r} names no emitting source file"
        )
        for source_ref in sources:
            source_path = REPO_ROOT / source_ref
            assert source_path.is_file(), (
                f"documented emitting source {source_ref!r} for scheme "
                f"{scheme!r} does not exist"
            )
            source_text = source_path.read_text(encoding="utf-8")
            assert f'f"{scheme}:{{' in source_text, (
                f"documented scheme {scheme!r} is not minted by "
                f"{source_ref} (expected an f\"{scheme}:{{...}}\" id); "
                "update the id-schemes table in "
                "docs/reference/mcp.md"
            )

    # Reverse pin: every scheme the CORE code/workspace sources mint is
    # documented — a new id scheme cannot land without a docs row.
    for name in CORE_EMITTING_SOURCES:
        source_path = SOURCES_DIR / name
        assert source_path.is_file(), f"core emitting source {name} moved"
        emitted = {
            match.group(1)
            for match in _EMIT_RE.finditer(
                source_path.read_text(encoding="utf-8")
            )
        }
        undocumented = emitted - documented
        assert not undocumented, (
            f"{name} mints id scheme(s) {sorted(undocumented)} that are "
            "not documented in the id-schemes table of "
            "docs/reference/mcp.md"
        )


#: The taught query-less path lookup (read-model-s3-where-scope re-review).
#: The short-form condition since read-surface-diet-3666: one spelling per
#: tool, the one its schema shows.
TYPED_PATH_LOOKUP = 'search(where={"type": "source_file", "attrs.path": "src/app.py"})'


def test_mcp_instructions_carry_lookup_contract() -> None:
    """The MCP instructions surface carries the worked path-lookup example
    and the inspect not-found signal — the agent-visible half of the
    contract.

    Asserted on the RENDERED constants rather than on server.py's source
    text. Since `mcp-tool-surface-profiles` the node-lookup contract is
    assembled from per-tool segments, so a source-text scan between two
    assignment markers no longer sees the whole block — and it never proved
    the served string carried the contract anyway, only that the file
    mentioned it. This checks the bytes a client actually receives.
    """
    from okg.substrate.mcp.server import (
        DEFAULT_INSTRUCTIONS,
        NODE_LOOKUP_INSTRUCTIONS,
        OPERATOR_INSTRUCTIONS,
    )

    block = NODE_LOOKUP_INSTRUCTIONS

    # Worked example: find a file node by path, query-less. Since
    # read-model-s3-where-scope the example is a `where`; `attr_filters` and
    # `method="attrs"` are still accepted but no longer taught. It names a
    # type and an exact `attrs.path`, which narrow the lookup page under
    # every Nomos mode; an untyped catalog-`path` lookup does not.
    assert TYPED_PATH_LOOKUP in block
    assert 'search(where={"op": "eq", "field": "path"' not in block
    # Suffix spelling via filter.
    assert '"op": "suffix"' in block
    # PACT read-select-vocabulary-3666 dropped the id schemes; PACT
    # publish-paging-args-3666 restores three of them (file, commit, doc),
    # written with placeholders only, and that a known id goes straight to
    # inspect, at the orchestrator's request; the sentence's own effect on
    # agent results is not measured. The symbol and manifest schemes stay in
    # the docs table only.
    assert "Node ids are deterministic" in block
    assert "`file:<repo>:<path>`, `commit:<repo>:<sha>`, `doc:<repo>:<path>`" in block
    assert '"node_id": "<id>"' in block
    assert "symbol:<repo>:<path>:<qname>" not in block
    assert "docs/reference/mcp.md" not in block
    # The not-found signal is documented to agents.
    assert "node_not_found" in block
    assert "exists: false" in block

    # Both instruction variants embed the block VERBATIM — stronger than the
    # previous count of the literal `+ NODE_LOOKUP_INSTRUCTIONS` spelling,
    # which a refactor could satisfy while changing the served text.
    assert NODE_LOOKUP_INSTRUCTIONS in DEFAULT_INSTRUCTIONS
    assert NODE_LOOKUP_INSTRUCTIONS in OPERATOR_INSTRUCTIONS


def test_mcp_instructions_leave_path_search_to_the_checked_card() -> None:
    """Issue #2676: the node-lookup sentence used to say file paths are NOT
    in node text, which sent agents away from a search that works. PACT
    dataset-card: whether path words match lexical search is no longer a
    fixed sentence either way. The dataset card states it, and only when a
    start-up check on the server's own rows saw it
    (tests/substrate/mcp/test_dataset_card.py)."""
    from okg.substrate.mcp.server import OPERATOR_INSTRUCTIONS

    assert "usually are not in it" not in OPERATOR_INSTRUCTIONS
    assert "usually does not carry file paths" not in OPERATOR_INSTRUCTIONS
    assert "includes the path split into words" not in OPERATOR_INSTRUCTIONS
    assert "carry their path in `attrs.path`" in OPERATOR_INSTRUCTIONS
    # The exact-path and path-tail lookups stay.
    assert TYPED_PATH_LOOKUP in OPERATOR_INSTRUCTIONS
    assert '"op": "suffix"' in OPERATOR_INSTRUCTIONS
    assert "doc:<repo>:<path>" in OPERATOR_INSTRUCTIONS


def test_mcp_instructions_teach_reading_a_whole_document() -> None:
    """Issue #2676: agents read chunks one call at a time and were refused
    for `attrs.text` and a one-hop `target_subtype`; nothing told them how
    to read a whole document. Issue #2668 made `inspect` kind "text" that
    read: in chunk order, paged by bytes, filtered per chunk under Nomos.
    The path-shaped expand it replaces (id order, no cursor over the byte
    budget, nothing under enforce) is no longer taught."""
    from okg.substrate.mcp.server import OPERATOR_INSTRUCTIONS

    for phrase in (
        '"kind": "text"',
        '"node_id": "doc:<repo>:<path>"',
        "inspect(next=<token>)",
        "until a page ends `complete`",
        "`order_basis`",
        "`coverage`",
        "`heading_path`",
        "A one-hop expand returns the linked records.",
        # Issue #2669: a `file:` node's text is only its path; its inspect
        # lists the derived `doc:` nodes.
        "`file:` node",
        "`documents`",
        "BM25",
    ):
        assert phrase in OPERATOR_INSTRUCTIONS, phrase
    # read-surface-diet-3666: hidden spellings are not taught.
    for retired in ('path=["contains:out"]', "visibility_limited",
                    "predicate=", "input="):
        assert retired not in OPERATOR_INSTRUCTIONS, retired
    # publish-paging-args-3666: search publishes max_chars again, and the
    # reading paragraph names it for the passage length.
    assert "`max_chars` (up to 4096) lengthens passages" in OPERATOR_INSTRUCTIONS
    # read-passage-rows-3666 (R8/D6): a search row shows the matched passage
    # and the call that reads more; `select` of `attrs` is no longer taught
    # as the way to a chunk's full text, nor `select`'s field shape.
    assert "Each row shows the matched passage, and the reply names the exact call" in (
        OPERATOR_INSTRUCTIONS
    )
    for retired in ('`select=["node_id","attrs"]`', "chunk's full text",
                    "`attrs`, not `attrs.text`", "Excerpts stop at 280"):
        assert retired not in OPERATOR_INSTRUCTIONS, retired


def test_the_reading_sentences_follow_the_served_tools() -> None:
    """A surface without `inspect` must not be told to call it, nor one
    without `expand` about expand; the stop sentence names no tool and is
    always served."""
    from okg.substrate.mcp.server import operator_instructions_for

    discovery = operator_instructions_for(("inspect", "search", "aggregate"))
    assert '"kind": "text"' in discovery
    assert "one-hop expand" not in discovery
    assert "the matched passage" in discovery
    assert "BM25" in discovery
    only_expand = operator_instructions_for(("expand",))
    assert "BM25" not in only_expand
    assert '"kind": "text"' not in only_expand
    assert "A one-hop expand returns the linked records." in only_expand


def test_mcp_instructions_say_when_to_stop_on_every_surface() -> None:
    from okg.substrate.mcp.server import operator_instructions_for
    from okg.substrate.mcp.operators import OPERATOR_TOOLS

    for served in (tuple(OPERATOR_TOOLS), ("inspect", "search", "aggregate"), ("search",)):
        rendered = operator_instructions_for(served)
        assert "Stop once every part of the question is answered" in rendered
        assert "complete count or list" in rendered
