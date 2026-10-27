"""The served instructions name `file:` and `commit:` ids only where they exist.

Issue #4064 (the same defect as #4052, for git-derived ids): the instructions
told a model to look nodes up by `file:<repo>:<path>` and `commit:<repo>:<sha>`
ids and gave `source_file` lookups on a deployment whose source registry has no
source that produces them. Each id family is now rendered only for a deployment
whose source registry declares a source that emits it, using the same
unknown-registry rule as #4052.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.mcp.operators import OPERATOR_TOOLS
from okg.substrate.mcp.server import (
    NODE_LOOKUP_INSTRUCTIONS,
    OPERATOR_INSTRUCTIONS,
    IdFamilies,
    declared_id_families,
    operator_instructions_for,
    resolve_id_families,
)
from okg.substrate.mcp.tool_profiles import tools_named_in

_SRC = "okg.substrate.library.sources"

#: Registry body for one source: (module, class). Only `module` is read.
_GIT_FILES = (f"{_SRC}.git_graph_files", "GitFilesSource")
_GIT_HISTORY = (f"{_SRC}.git_graph_history", "GitHistorySource")
_AGENT_SESSIONS = (f"{_SRC}.agent_sessions", "AgentSessionsSource")
_EXTERNAL_REPOS = (f"{_SRC}.external_repos.factory", "ExternalRepoFactory")
_EXTERNAL_REPOS_PACKAGE = (f"{_SRC}.external_repos", "ExternalRepoFactory")
_DOC_CORPUS = (f"{_SRC}.doc_corpus", "DocCorpusSource")
_JIRA = (f"{_SRC}.jira", "JiraSource")
# Sources that only draw edges to `file:` / `commit:` ids: their target node
# may not exist, so they do not declare the family.
_CODEX = (f"{_SRC}.codex_sessions", "CodexSessionsSource")
_GITHUB = (f"{_SRC}.github", "GitHubSource")
_OPENSPEC = (f"{_SRC}.openspec", "OpenSpecSource")


def _registry(tmp_path: Path, *sources: tuple[str, str]) -> Path:
    body = "sources:\n" + "".join(
        f"  s{i}:\n    module: {module}\n    class: {cls}\n"
        for i, (module, cls) in enumerate(sources)
    )
    path = tmp_path / "source_registry.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def _raw(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "source_registry.yaml"
    path.write_text(text, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "sources,file_nodes,commit_nodes",
    [
        ([_GIT_FILES], True, False),
        ([_GIT_HISTORY], True, True),
        ([_GIT_FILES, _GIT_HISTORY], True, True),
        ([_AGENT_SESSIONS], True, False),
        ([_EXTERNAL_REPOS], True, True),
        ([_EXTERNAL_REPOS_PACKAGE], True, True),
        ([_DOC_CORPUS, _JIRA], False, False),
        ([_JIRA], False, False),
        ([_CODEX, _GITHUB, _OPENSPEC], False, False),
    ],
)
def test_each_family_follows_the_sources_that_emit_it(
    tmp_path: Path,
    sources: list[tuple[str, str]],
    file_nodes: bool,
    commit_nodes: bool,
) -> None:
    ids = declared_id_families(_registry(tmp_path, *sources))
    assert ids.file_nodes is file_nodes
    assert ids.commit_nodes is commit_nodes


def _code_repos(tmp_path: Path, expand: str) -> Path:
    return _raw(
        tmp_path,
        f"code_repos:\n  base: {tmp_path}\n  expand: {expand}\n"
        "  repos:\n    - slug: myrepo\n",
    )


def test_a_code_repos_block_declares_the_git_sources_it_expands(
    tmp_path: Path,
) -> None:
    """`code_repos:` is not in the raw `sources:` mapping; the expansion that
    ingest uses is what counts (the #4052 review's M1)."""
    ids = declared_id_families(_code_repos(tmp_path, "[git-files, code-structure]"))
    assert (ids.file_nodes, ids.commit_nodes, ids.doc_text) == (True, False, False)
    ids = declared_id_families(
        _code_repos(tmp_path, "[git-files, git-history, doc-corpus]")
    )
    assert (ids.file_nodes, ids.commit_nodes, ids.doc_text) == (True, True, True)


@pytest.mark.parametrize("text", ["", "# only a comment\n", "x-note: 1\n", "sources: {}\n"])
def test_an_empty_registry_is_a_real_no_for_every_family(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, text: str,
) -> None:
    with caplog.at_level("WARNING"):
        ids = declared_id_families(_raw(tmp_path, text))
    assert ids == IdFamilies(doc_text=False, file_nodes=False, commit_nodes=False)
    assert caplog.text == ""


@pytest.mark.parametrize(
    "text",
    [
        "other: 1\n",
        "sources:\n  - a\n",
        "- just\n- a list\n",
        "sources: 3\n",
        "sources: [unclosed",
        "code_repos:\n  base: /x\n  expand: [git-files]\n  repos: 5\n",
        "code_repos:\n  base: /x\n  expand: [git-files]\n  repos: true\n",
    ],
)
def test_an_unreadable_registry_is_unknown_for_every_family_and_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, text: str,
) -> None:
    with caplog.at_level("WARNING"):
        ids = declared_id_families(_raw(tmp_path, text))
    assert ids == IdFamilies()
    assert "keep their node id sentences" in caplog.text or "unreadable" in caplog.text


def test_no_registry_and_a_missing_registry_are_unknown(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    assert declared_id_families(None) == IdFamilies()
    with caplog.at_level("WARNING"):
        assert declared_id_families(tmp_path / "missing.yaml") == IdFamilies()
    assert "is missing" in caplog.text


# ── the rendered text ─────────────────────────────────────────────────────

_NO_GIT = IdFamilies(doc_text=False, file_nodes=False, commit_nodes=False)


def test_no_git_source_means_no_file_commit_or_source_file_claims() -> None:
    rendered = operator_instructions_for(OPERATOR_TOOLS, ids=_NO_GIT)
    for needle in ("file:", "commit:", "source_file", "doc:"):
        assert needle not in rendered, needle
    # What is left still teaches the lookup, the id read and the not-found signal.
    assert 'search(where={"type": "<type>", "attrs.<name>": "<value>"})' in rendered
    assert '"op": "suffix"' in rendered
    assert "An id from a result row goes straight to `inspect(" in rendered
    assert '"kind": "text"' in rendered
    assert "node_not_found" in rendered
    # The path-words sentence keeps `document` nodes, which this change does
    # not gate (no source here is named for them).
    assert "`document` nodes carry their path in `attrs.path`" in rendered
    assert len(rendered) < len(operator_instructions_for(OPERATOR_TOOLS))


def test_git_files_only_names_file_ids_but_not_commit_ids() -> None:
    ids = IdFamilies(doc_text=False, file_nodes=True, commit_nodes=False)
    rendered = operator_instructions_for(OPERATOR_TOOLS, ids=ids)
    assert "Node ids are deterministic: `file:<repo>:<path>`; a known id goes" in rendered
    assert "commit:" not in rendered
    assert '"type": "source_file", "attrs.path": "src/app.py"' in rendered
    assert "`source_file` and `document` nodes carry their path" in rendered


def test_commit_ids_alone_do_not_bring_back_source_file_examples() -> None:
    ids = IdFamilies(doc_text=False, file_nodes=False, commit_nodes=True)
    rendered = operator_instructions_for(OPERATOR_TOOLS, ids=ids)
    assert "Node ids are deterministic: `commit:<repo>:<sha>`; a known id goes" in rendered
    assert "file:" not in rendered
    assert "source_file" not in rendered


def test_the_file_to_doc_step_needs_both_a_doc_source_and_a_file_source() -> None:
    step = "lists its `doc:` nodes"
    both = operator_instructions_for(
        OPERATOR_TOOLS, ids=IdFamilies(doc_text=True, file_nodes=True),
    )
    doc_only = operator_instructions_for(
        OPERATOR_TOOLS, ids=IdFamilies(doc_text=True, file_nodes=False),
    )
    file_only = operator_instructions_for(
        OPERATOR_TOOLS, ids=IdFamilies(doc_text=False, file_nodes=True),
    )
    assert step in both
    assert step not in doc_only
    # A document source alone still names its own read.
    assert "doc:<repo>:<path>" in doc_only
    assert "file:" not in doc_only
    assert step not in file_only
    assert "doc:" not in file_only


def test_unknown_keeps_today_s_text_byte_for_byte() -> None:
    """`None` (and `IdFamilies()`) is the full text served before #4064: the
    #4052 doc: sentences and the file/commit sentences, unchanged."""
    assert operator_instructions_for(OPERATOR_TOOLS) == OPERATOR_INSTRUCTIONS
    assert (
        operator_instructions_for(OPERATOR_TOOLS, ids=IdFamilies())
        == OPERATOR_INSTRUCTIONS
    )
    assert NODE_LOOKUP_INSTRUCTIONS == (
        "NODE LOOKUP: lexical search matches node text; `source_file` and "
        "`document` nodes carry their path in `attrs.path`. "
        "To resolve a node by attrs, call search with only a `where` naming a "
        'type: `search(where={"type": "source_file", "attrs.path": '
        '"src/app.py"})` finds a file node by exact path; `"attrs.name"` finds '
        "symbols by name. For a path tail, use "
        '`filter(where={"type": "source_file", "attrs.path": '
        '{"op": "suffix", "value": "dir/file.py"}})`. '
        "Node ids are deterministic: `file:<repo>:<path>`, "
        "`commit:<repo>:<sha>`, `doc:<repo>:<path>` (a parsed file's text); "
        'a known id goes straight to `inspect(target={"kind": "node", '
        '"node_id": "<id>"})`. '
        "inspect on an id the graph lacks fails with `node_not_found` "
        "(`exists: false`); a found node with sparse attrs still returns its "
        "payload."
    )
    assert (
        "A `file:` node's text is only its path; inspecting it lists its "
        "`doc:` nodes under `documents`: read those. A one-hop expand"
    ) in OPERATOR_INSTRUCTIONS


@pytest.mark.parametrize(
    "ids",
    [
        _NO_GIT,
        IdFamilies(doc_text=False, file_nodes=True, commit_nodes=False),
        IdFamilies(doc_text=True, file_nodes=False, commit_nodes=True),
        IdFamilies(doc_text=True, file_nodes=True, commit_nodes=True),
    ],
)
@pytest.mark.parametrize(
    "served",
    [OPERATOR_TOOLS, ("inspect", "search", "aggregate"), ("inspect", "filter"), ("inspect",)],
)
def test_no_family_variant_names_an_unserved_tool(
    ids: IdFamilies, served: tuple[str, ...],
) -> None:
    rendered = operator_instructions_for(served, ids=ids)
    assert set(tools_named_in(rendered, candidates=OPERATOR_TOOLS)) <= set(served)


# ── the manifest override and the wiring ──────────────────────────────────


def test_the_manifest_override_wins_per_family(tmp_path: Path) -> None:
    git = _registry(tmp_path, _GIT_HISTORY)
    # Absent: the scan decides every family.
    assert resolve_id_families({}, git) == IdFamilies(
        doc_text=False, file_nodes=True, commit_nodes=True,
    )
    # One key settles its family in either direction; the others keep the scan.
    assert resolve_id_families({"commit_nodes": False}, git) == IdFamilies(
        doc_text=False, file_nodes=True, commit_nodes=False,
    )
    assert resolve_id_families({"file_nodes": False}, git).file_nodes is False
    no_git = _registry(tmp_path, _JIRA)
    # A deployment whose own module wraps a git source holds ids the scan
    # cannot see.
    assert resolve_id_families({"file_nodes": True, "commit_nodes": True}, no_git) == (
        IdFamilies(doc_text=False, file_nodes=True, commit_nodes=True)
    )


def test_the_scan_is_skipped_when_the_manifest_settles_every_family(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    block = {"doc_text_nodes": False, "file_nodes": True, "commit_nodes": False}
    with caplog.at_level("WARNING"):
        ids = resolve_id_families(block, tmp_path / "missing.yaml")
    assert ids == IdFamilies(doc_text=False, file_nodes=True, commit_nodes=False)
    assert caplog.text == ""
    # With one family open, an unreadable registry leaves it unknown.
    partial = resolve_id_families({"file_nodes": True}, tmp_path / "missing.yaml")
    assert partial == IdFamilies(file_nodes=True)


@pytest.mark.parametrize("key", ["doc_text_nodes", "file_nodes", "commit_nodes"])
@pytest.mark.parametrize("bad", ["yes", 1, "false", []])
def test_a_non_boolean_override_is_refused(key: str, bad: object) -> None:
    from okg.substrate.mcp.tool_profiles import (
        ToolProfileConfigInvalid,
        parse_tool_profile_config,
    )

    with pytest.raises(ToolProfileConfigInvalid, match=key):
        parse_tool_profile_config({key: bad})


def test_an_unknown_mcp_key_is_still_refused() -> None:
    from okg.substrate.mcp.tool_profiles import (
        ToolProfileConfigInvalid,
        parse_tool_profile_config,
    )

    with pytest.raises(ToolProfileConfigInvalid):
        parse_tool_profile_config({"file_node": True})


def test_the_okg_workspace_manifest_declares_git_files_and_history() -> None:
    from okg.substrate.mcp.server import _load_readside_metadata

    ids = _load_readside_metadata("okg-workspace")["id_families"]
    assert (ids.file_nodes, ids.commit_nodes) == (True, True)


def _build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, registry: Path, mcp_line: str):
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "none")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    directory = tmp_path / "git-ids-probe"
    (directory / "schemas").mkdir(parents=True)
    (directory / "source_registry.yaml").write_text(
        registry.read_text(encoding="utf-8"), encoding="utf-8",
    )
    (directory / "deployment.yaml").write_text(
        "name: git-ids-probe\ndescription: issue 4064\n"
        "schema_dir: schemas\nsource_registry: source_registry.yaml\n" + mcp_line,
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    return build_server(dsn=PROBE_DSN, deployment_name="git-ids-probe")


@pytest.mark.parametrize(
    "sources,mcp_line,expected",
    [
        ([_JIRA], "", (False, False)),
        ([_GIT_FILES], "", (True, False)),
        ([_GIT_HISTORY], "", (True, True)),
        ([_JIRA], "mcp:\n  file_nodes: true\n", (True, False)),
        ([_GIT_HISTORY], "mcp:\n  commit_nodes: false\n", (True, False)),
    ],
)
def test_the_server_serves_and_reports_the_decision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sources: list[tuple[str, str]],
    mcp_line: str,
    expected: tuple[bool, bool],
) -> None:
    """End to end on a built server: the served text, the
    `inspect(surface)` readback and the fleet doctor's recomputed hash all
    carry the same decision."""
    from okg.substrate.mcp.surface_fleet import expected_surface

    (tmp_path / "src").mkdir()
    registry = _registry(tmp_path / "src", *sources)
    mcp = _build(tmp_path, monkeypatch, registry, mcp_line)
    try:
        file_nodes, commit_nodes = expected
        surface = mcp._okg_surface_freshness["tool_surface"]
        assert surface["file_nodes"] is file_nodes
        assert surface["commit_nodes"] is commit_nodes
        assert surface["doc_text_nodes"] is False
        assert ("`file:<repo>:<path>`" in mcp.instructions) is file_nodes
        assert ("`commit:<repo>:<sha>`" in mcp.instructions) is commit_nodes
        assert ("source_file" in mcp.instructions) is file_nodes
        assert expected_surface("git-ids-probe")["instructions_hash"] == (
            mcp._okg_surface_freshness["authored_instructions_hash"]
        )
    finally:
        mcp._okg_pool.close()


def test_git_without_doc_text_serves_the_4052_doc_free_text_literally() -> None:
    """The okg-ci shape (git files and history, no doc source) is what #4052
    served for a deployment without doc text: pinned as literal text, so a
    change to either sentence builder cannot move it unseen."""
    rendered = operator_instructions_for(
        OPERATOR_TOOLS,
        ids=IdFamilies(doc_text=False, file_nodes=True, commit_nodes=True),
    )
    assert (
        "NODE LOOKUP: lexical search matches node text; `source_file` and "
        "`document` nodes carry their path in `attrs.path`. "
        "To resolve a node by attrs, call search with only a `where` naming a "
        'type: `search(where={"type": "source_file", "attrs.path": '
        '"src/app.py"})` finds a file node by exact path; `"attrs.name"` finds '
        "symbols by name. For a path tail, use "
        '`filter(where={"type": "source_file", "attrs.path": '
        '{"op": "suffix", "value": "dir/file.py"}})`. '
        "Node ids are deterministic: `file:<repo>:<path>`, "
        "`commit:<repo>:<sha>`; "
        'a known id goes straight to `inspect(target={"kind": "node", '
        '"node_id": "<id>"})`. '
        "inspect on an id the graph lacks fails with `node_not_found` "
        "(`exists: false`); a found node with sparse attrs still returns its "
        "payload."
    ) in rendered
    assert (
        "READING: lexical search returns rows matching any search term, "
        "ranked by BM25 with English stemming. Each row shows the matched "
        "passage, and the reply names the exact call that reads a record; "
        "`max_chars` (up to 4096) lengthens passages. "
        "For full text, read a node that `contains` chunks, or a ticket, in "
        'reading order: `inspect(target={"kind": "text", '
        '"node_id": "<id from a result row>"})` (pages of up to max_bytes, '
        "default 65536; a cut page says how much is left), then "
        "`inspect(next=<token>)` until a page ends `complete`. Each item's "
        "`heading_path` places it in its section. A ticket reads as its "
        "fields, its description and its dated comments; a cut thread names "
        "the call that reads the rest. A one-hop expand returns the linked "
        "records. Stop once every part of the question is answered from graph "
        "text you can cite; page on only for a complete count or list."
    ) in rendered
