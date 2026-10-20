"""Pure-function tests for the non-Python AST parsers.

Per `cleanup-personal-okg-data-quality` C1 item 5.7.

Each parser is a pure function: text in, ParsedFile out. No DB
needed for these tests.
"""
from __future__ import annotations

from okg.substrate.library.enrichers.code_graph.markdown_structural import (
    parse_markdown,
)
from okg.substrate.library.enrichers.code_graph.config_structural import (
    iter_json_top_level_keys,
    iter_toml_top_level_keys,
    iter_yaml_top_level_keys,
    _parse_config_file,
)


# ---------- markdown ----------


def test_markdown_extracts_flat_h1_h2():
    text = (
        "# Top\n\n"
        "Para under top.\n\n"
        "## Sub\n\n"
        "Para under sub.\n"
    )
    pf = parse_markdown(text, repo="r", path="x.md")
    assert len(pf.nodes) == 2
    by_slug = {n.attrs["slug"]: n for n in pf.nodes}
    assert by_slug["top"].attrs["depth"] == 1
    assert by_slug["sub"].attrs["depth"] == 2
    # Both edges: file→top (depth 1, no parent), top→sub
    srcs = [(e.src, e.dst) for e in pf.edges]
    assert ("file:r:x.md", by_slug["top"].node_id) in srcs
    assert (by_slug["top"].node_id, by_slug["sub"].node_id) in srcs


def test_markdown_skips_headings_inside_code_fences():
    text = (
        "# Real\n\n"
        "```\n"
        "# This is a comment in a shell snippet\n"
        "```\n\n"
        "## After fence\n"
    )
    pf = parse_markdown(text, repo="r", path="x.md")
    slugs = {n.attrs["slug"] for n in pf.nodes}
    assert slugs == {"real", "after-fence"}


def test_markdown_nesting_jumps_pop_stack():
    text = (
        "# A\n\n## B\n\n### C\n\n## D\n\n# E\n"
    )
    pf = parse_markdown(text, repo="r", path="x.md")
    by_slug = {n.attrs["slug"]: n.node_id for n in pf.nodes}
    edges = {(e.src, e.dst) for e in pf.edges}
    # B→C (deeper), C closes when D appears (sibling of B), D under A
    # E is top-level (depth 1), parent is file
    assert ("file:r:x.md", by_slug["a"]) in edges
    assert ("file:r:x.md", by_slug["e"]) in edges
    assert (by_slug["a"], by_slug["b"]) in edges
    assert (by_slug["a"], by_slug["d"]) in edges
    assert (by_slug["b"], by_slug["c"]) in edges


def test_markdown_disambiguates_duplicate_slugs():
    text = "# Setup\n\n## Setup\n"
    pf = parse_markdown(text, repo="r", path="x.md")
    slugs = [n.attrs["slug"] for n in pf.nodes]
    assert slugs == ["setup", "setup-1"]


def test_markdown_empty_file_no_emissions():
    pf = parse_markdown("", repo="r", path="x.md")
    assert pf.nodes == []
    assert pf.edges == []


def test_markdown_atx_with_trailing_hashes():
    """`# Title #` is valid ATX; the trailing # gets stripped."""
    pf = parse_markdown("# Title #\n", repo="r", path="x.md")
    assert len(pf.nodes) == 1
    assert pf.nodes[0].attrs["text"] == "Title"


# ---------- yaml ----------


def test_yaml_extracts_top_level_keys():
    text = "name: foo\nversion: 1\nnested:\n  inner: skipped\n"
    keys = list(iter_yaml_top_level_keys(text))
    assert keys == [
        ("name", "yaml_root_key"),
        ("version", "yaml_root_key"),
        ("nested", "yaml_root_key"),
    ]


def test_yaml_emits_config_keys_via_parse():
    pf = _parse_config_file(
        "name: foo\nversion: 1\n",
        repo="r", path="conf.yaml",
        iter_keys=iter_yaml_top_level_keys,
        source_kind="yaml_root_key",
    )
    assert len(pf.nodes) == 2
    assert all(n.subtype == "config_key" for n in pf.nodes)
    assert {n.attrs["key"] for n in pf.nodes} == {"name", "version"}


def test_yaml_invalid_returns_empty():
    keys = list(iter_yaml_top_level_keys("name: [unclosed\n"))
    assert keys == []


def test_yaml_root_list_returns_empty():
    """A YAML root list (sequence) has no top-level keys."""
    keys = list(iter_yaml_top_level_keys("- one\n- two\n"))
    assert keys == []


# ---------- toml ----------


def test_toml_extracts_top_level_keys_and_tables():
    text = (
        'name = "foo"\n'
        'version = 1\n'
        "[tool]\n"
        'verbose = true\n'
    )
    keys = list(iter_toml_top_level_keys(text))
    assert ("name", "toml_table") in keys
    assert ("version", "toml_table") in keys
    assert ("tool", "toml_table") in keys


def test_toml_invalid_returns_empty():
    keys = list(iter_toml_top_level_keys("name = unclosed\n"))
    assert keys == []


# ---------- json ----------


def test_json_extracts_root_object_keys():
    text = '{"name": "foo", "version": 1, "nested": {"inner": 1}}'
    keys = list(iter_json_top_level_keys(text))
    assert keys == [
        ("name", "json_root_key"),
        ("version", "json_root_key"),
        ("nested", "json_root_key"),
    ]


def test_json_root_array_returns_empty():
    keys = list(iter_json_top_level_keys("[1, 2, 3]"))
    assert keys == []


def test_json_invalid_returns_empty():
    keys = list(iter_json_top_level_keys("{not: json}"))
    assert keys == []


def test_json_emits_config_keys_via_parse():
    pf = _parse_config_file(
        '{"a": 1, "b": 2}',
        repo="r", path="conf.json",
        iter_keys=iter_json_top_level_keys,
        source_kind="json_root_key",
    )
    assert {n.attrs["key"] for n in pf.nodes} == {"a", "b"}
    # All edges are file→key
    file_node = "file:r:conf.json"
    assert all(e.src == file_node for e in pf.edges)
