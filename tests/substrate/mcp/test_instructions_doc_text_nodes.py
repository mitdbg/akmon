"""The served instructions point at `doc:` text nodes only where they exist.

Issue #4052: the instructions told a model to read file text through
`doc:<repo>:<path>` nodes on a deployment with no `doc_corpus` source. Every
such read was refused `node_not_found` (64 of 67 refusals in lane 2's cms-kb
benchmark). The sentences are now rendered only for a deployment whose source
registry declares a file-text source.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.mcp.operators import OPERATOR_TOOLS
from okg.substrate.mcp.server import (
    IdFamilies,
    authored_instructions_for,
    declared_id_families,
    operator_instructions_for,
    resolve_id_families,
)
from okg.substrate.mcp.tool_profiles import tools_named_in
from tests.mirror_scope import private_path

_DOC_ID = "doc:<repo>:<path>"

_WITH_DOC_CORPUS = """\
sources:
  git-files:
    module: okg.substrate.library.sources.git_graph_files
    class: GitFilesSource
  docs:
    module: okg.substrate.library.sources.doc_corpus
    class: DocCorpusSource
"""

_WITHOUT_DOC_CORPUS = """\
sources:
  git-files:
    module: okg.substrate.library.sources.git_graph_files
    class: GitFilesSource
  tickets:
    module: okg.substrate.library.sources.jira_api
    class: JiraSource
"""

_UPLOAD_ONLY = """\
sources:
  uploads:
    module: okg.substrate.library.sources.upload
    class: UploadSource
"""


def _registry(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "source_registry.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_a_deployment_with_a_doc_corpus_source_keeps_the_doc_sentences(
    tmp_path: Path,
) -> None:
    flag = declared_id_families(_registry(tmp_path, _WITH_DOC_CORPUS)).doc_text
    assert flag is True
    rendered = operator_instructions_for(OPERATOR_TOOLS, ids=IdFamilies(doc_text=flag))
    assert _DOC_ID in rendered
    assert "`file:<repo>:<path>`, `commit:<repo>:<sha>`, `doc:<repo>:<path>`" in rendered
    assert "lists its `doc:` nodes" in rendered


def test_a_deployment_without_one_does_not_point_at_doc_nodes(
    tmp_path: Path,
) -> None:
    flag = declared_id_families(_registry(tmp_path, _WITHOUT_DOC_CORPUS)).doc_text
    assert flag is False
    rendered = operator_instructions_for(OPERATOR_TOOLS, ids=IdFamilies(doc_text=flag))
    assert "doc:" not in rendered
    # What is left still teaches the id lookup and the text read.
    assert "`file:<repo>:<path>`, `commit:<repo>:<sha>`;" in rendered
    assert '"kind": "text"' in rendered
    assert "node_not_found" in rendered
    # And it is shorter than the full text by exactly the dropped claims.
    assert len(rendered) < len(operator_instructions_for(OPERATOR_TOOLS))


def test_an_upload_source_counts_as_a_file_text_source(tmp_path: Path) -> None:
    assert declared_id_families(_registry(tmp_path, _UPLOAD_ONLY)).doc_text is True


def test_unknown_keeps_the_full_text_and_says_so(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    """No manifest to read is `None`: unchanged text. A registry that cannot
    be parsed is also `None`, and is logged rather than hidden."""
    assert declared_id_families(None).doc_text is None
    assert declared_id_families(tmp_path / "missing.yaml").doc_text is None
    bad = _registry(tmp_path, "sources: [unclosed")
    with caplog.at_level("WARNING"):
        assert declared_id_families(bad).doc_text is None
    assert "unreadable" in caplog.text
    assert operator_instructions_for(
        OPERATOR_TOOLS, ids=IdFamilies(),
    ) == operator_instructions_for(OPERATOR_TOOLS)
    assert _DOC_ID in operator_instructions_for(OPERATOR_TOOLS)


def test_an_empty_sources_mapping_is_a_real_no(tmp_path: Path) -> None:
    assert declared_id_families(_registry(tmp_path, "sources: {}\n")).doc_text is False


@pytest.mark.parametrize(
    "text",
    [
        "other: 1\n",
        "sources:\n  - a\n  - b\n",
        "- just\n- a list\n",
        "sources: 3\n",
        "code_repos:\n  base: /x\n  expand: [doc-corpus]\n  repos: 5\n",
    ],
)
def test_a_malformed_registry_is_unknown_and_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, text: str,
) -> None:
    with caplog.at_level("WARNING"):
        assert declared_id_families(_registry(tmp_path, text)).doc_text is None
    assert "keep their node id sentences" in caplog.text


@pytest.mark.parametrize("text", ["", "# only a comment\n", "x-note: 1\nx-other: 2\n"])
def test_an_empty_registry_file_is_a_real_no_like_the_loader(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, text: str,
) -> None:
    """The loader reads `document or {}` and accepts `x-` keys, so an empty
    file, or one with only `x-` keys, is a valid registry with no sources:
    False, with no warning."""
    with caplog.at_level("WARNING"):
        assert declared_id_families(_registry(tmp_path, text)).doc_text is False
    assert caplog.text == ""


def _code_repos(tmp_path: Path, expand: str) -> Path:
    return _registry(
        tmp_path,
        f"code_repos:\n  base: {tmp_path}\n  expand: {expand}\n"
        "  repos:\n    - slug: myrepo\n",
    )


def test_a_code_repos_block_that_expands_doc_corpus_declares_it(
    tmp_path: Path,
) -> None:
    assert declared_id_families(_code_repos(tmp_path, "[git-files, doc-corpus]")).doc_text is True


def test_a_code_repos_block_without_doc_corpus_declares_none(tmp_path: Path) -> None:
    assert declared_id_families(_code_repos(tmp_path, "[git-files]")).doc_text is False


@pytest.mark.parametrize(
    "served",
    [
        OPERATOR_TOOLS,
        ("inspect", "search", "aggregate"),
        ("inspect",),
    ],
)
def test_the_doc_free_text_names_no_unserved_tool(served: tuple[str, ...]) -> None:
    rendered = operator_instructions_for(served, ids=IdFamilies(doc_text=False))
    named = set(tools_named_in(rendered, candidates=OPERATOR_TOOLS))
    assert named <= set(served)
    assert "doc:" not in rendered


def test_authored_instructions_carry_the_flag_for_the_fleet_doctor() -> None:
    """The fleet doctor recomputes the authored text from files alone; the
    flag has to ride through `authored_instructions_for` or a deployment
    without doc text would read `stale` for ever."""
    without = authored_instructions_for(OPERATOR_TOOLS, ids=IdFamilies(doc_text=False))
    full = authored_instructions_for(OPERATOR_TOOLS, ids=IdFamilies(doc_text=True))
    assert "doc:" not in without
    assert _DOC_ID in full
    assert without != full


def test_the_okg_workspace_manifest_declares_a_file_text_source() -> None:
    from okg.substrate.deployments import resolve_deployment

    manifest = resolve_deployment("okg-workspace")
    assert declared_id_families(manifest.source_registry).doc_text is True


# ── the manifest override, the missing-file warning, the readback ──────


def test_a_missing_registry_file_is_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        assert declared_id_families(tmp_path / "missing.yaml").doc_text is None
    assert "is missing" in caplog.text


def test_the_manifest_override_wins_in_both_directions(tmp_path: Path) -> None:
    """A deployment whose own module wraps `DocCorpusSource` holds `doc:`
    nodes the registry scan cannot see (true); one that says false drops the
    sentences even where the scan finds a doc-corpus source."""
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    no_doc = _registry(tmp_path / "a", _WITHOUT_DOC_CORPUS)
    with_doc = _registry(tmp_path / "b", _WITH_DOC_CORPUS)
    assert resolve_id_families({"doc_text_nodes": True}, no_doc).doc_text is True
    assert resolve_id_families({"doc_text_nodes": False}, with_doc).doc_text is False
    # Absent (or no `mcp:` block): the scan decides, and None stays None.
    assert resolve_id_families({}, with_doc).doc_text is True
    assert resolve_id_families(None, no_doc).doc_text is False
    assert resolve_id_families(None, None) == IdFamilies()
    # The override also settles a registry that cannot be read.
    assert resolve_id_families({"doc_text_nodes": True}, None).doc_text is True


@pytest.mark.parametrize("bad", ["yes", 1, "false", []])
def test_a_non_boolean_override_is_refused(bad: object) -> None:
    from okg.substrate.mcp.tool_profiles import (
        ToolProfileConfigInvalid,
        parse_tool_profile_config,
    )

    with pytest.raises(ToolProfileConfigInvalid, match="doc_text_nodes"):
        parse_tool_profile_config({"doc_text_nodes": bad})


def test_the_readside_metadata_flag_for_a_fixture_deployment() -> None:
    """The wiring from a real manifest: a fixture with no file-text source."""
    from okg.substrate.mcp.server import _load_readside_metadata

    assert _load_readside_metadata("opensearch-reference")["id_families"].doc_text is False
    # The public copy ships the fixture above but not this private manifest;
    # there the second half skips. Here private_path is the identity.
    private_path("deployments/okg-workspace/deployment.yaml")
    assert _load_readside_metadata("okg-workspace")["id_families"].doc_text is True


@pytest.mark.parametrize(
    "mcp_line,expected",
    [("", False), ("mcp:\n  doc_text_nodes: true\n", True)],
)
def test_the_server_serves_and_reports_the_decision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mcp_line: str, expected: bool,
) -> None:
    """End to end on a built server: the served text and the
    `inspect(surface)` readback carry the same decision."""
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN
    from okg.substrate.mcp.surface_fleet import expected_surface

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
    directory = tmp_path / "doc-nodes-probe"
    (directory / "schemas").mkdir(parents=True)
    (directory / "source_registry.yaml").write_text(_WITHOUT_DOC_CORPUS, encoding="utf-8")
    (directory / "deployment.yaml").write_text(
        "name: doc-nodes-probe\ndescription: issue 4052\n"
        "schema_dir: schemas\nsource_registry: source_registry.yaml\n" + mcp_line,
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    mcp = build_server(dsn=PROBE_DSN, deployment_name="doc-nodes-probe")
    try:
        assert mcp._okg_surface_freshness["tool_surface"]["doc_text_nodes"] is expected
        assert (_DOC_ID in mcp.instructions) is expected
        # The fleet doctor recomputes the authored text from files alone and
        # must agree with the server, so a healthy server is not reported stale.
        assert expected_surface("doc-nodes-probe")["instructions_hash"] == (
            mcp._okg_surface_freshness["authored_instructions_hash"]
        )
    finally:
        mcp._okg_pool.close()
