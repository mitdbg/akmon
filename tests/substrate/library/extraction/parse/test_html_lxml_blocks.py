"""`HtmlLxmlParser` chunks every block of body text, not only `<p>`.

Each test names the block kind it covers. The last group are the failure
cases: text that must NOT become a chunk (script, style, noscript, head)
and nested text that must not be chunked twice.
"""
from __future__ import annotations

import re

import pytest

from okg.substrate.library.extraction.parse.html_lxml import HtmlLxmlParser


def _parse(html: str):
    return HtmlLxmlParser().parse(html)


def _pairs(html: str) -> list[tuple[str, str]]:
    return [(c.chunk_kind, c.text) for c in _parse(html).chunks]


def _non_ws(text: str) -> str:
    return re.sub(r"\s+", "", text)


def test_headings_become_heading_chunks_and_set_the_path() -> None:
    doc = _parse(
        "<html><body><h1>3 Basic Shell Features</h1><p>intro</p>"
        "<h2>3.1 Syntax</h2><p>body</p><h2>3.2 Commands</h2></body></html>"
    )
    assert [(c.chunk_kind, c.text, c.heading_path) for c in doc.chunks] == [
        ("heading", "3 Basic Shell Features", ["3 Basic Shell Features"]),
        ("paragraph", "intro", ["3 Basic Shell Features"]),
        ("heading", "3.1 Syntax", ["3 Basic Shell Features", "3.1 Syntax"]),
        ("paragraph", "body", ["3 Basic Shell Features", "3.1 Syntax"]),
        ("heading", "3.2 Commands", ["3 Basic Shell Features", "3.2 Commands"]),
    ]


def test_list_items_and_definition_lists_are_chunked_in_order() -> None:
    assert _pairs(
        "<body><ul><li>one</li><li>two <code>x</code></li></ul>"
        "<dl><dt><code>${parameter#word}</code></dt>"
        "<dd>Remove matching prefix.</dd></dl></body>"
    ) == [
        ("list_item", "one"),
        ("list_item", "two x"),
        ("list_item", "${parameter#word}"),
        ("list_item", "Remove matching prefix."),
    ]


def test_pre_is_one_code_block_with_its_whitespace_kept() -> None:
    doc = _parse(
        "<body><p>The arithmetic for command:</p>"
        "<pre class=\"example\">\nfor (( expr1 ; expr2 ; expr3 )) ; do\n"
        "    commands ; done\n</pre></body>"
    )
    assert [(c.chunk_kind, c.text) for c in doc.chunks] == [
        ("paragraph", "The arithmetic for command:"),
        (
            "code_block",
            "for (( expr1 ; expr2 ; expr3 )) ; do\n    commands ; done",
        ),
    ]


def test_a_data_table_is_one_table_chunk_with_rows_and_tab_cells() -> None:
    assert _pairs(
        "<body><table><caption>Options</caption>"
        "<tr><th>Flag</th><th>Meaning</th></tr>"
        "<tr><td><code>-x</code></td><td>trace\n  commands</td></tr>"
        "<tr><td> </td><td></td></tr></table></body>"
    ) == [("table", "Options\nFlag\tMeaning\n-x\ttrace commands")]


def test_a_layout_table_is_walked_cell_by_cell() -> None:
    assert _pairs(
        "<body><table><tr><td><h2>Index</h2><p>alpha</p></td>"
        "<td>beta <a href='#'>gamma</a></td></tr></table></body>"
    ) == [
        ("heading", "Index"),
        ("paragraph", "alpha"),
        ("paragraph", "beta gamma"),
    ]


def test_text_between_table_rows_and_cells_is_kept() -> None:
    # Text in a table but in no cell is not dropped: the table is walked
    # block by block instead of read as a grid.
    doc = _parse(
        "<body><table>stray<tr>trtext<td>a</td></tr>tbl tail</table></body>"
    )
    joined = "".join(_non_ws(c.text) for c in doc.chunks)
    assert joined == _non_ws(doc.text) == "straytrtextatbltail"
    assert all(c.chunk_kind == "paragraph" for c in doc.chunks)


def test_inline_element_in_a_row_outside_cells_is_kept() -> None:
    doc = _parse(
        "<body><table><tr><a href='#'>link-in-tr</a><td>a</td></tr>"
        "</table></body>"
    )
    joined = "".join(_non_ws(c.text) for c in doc.chunks)
    assert joined == _non_ws(doc.text) == "link-in-tra"


def test_text_directly_in_body_or_div_becomes_a_paragraph() -> None:
    # The libxml2 API index pages hold their links directly in the body.
    assert _pairs(
        "<body><h2>Letter a:</h2><dl><dt>alloc</dt><dd>"
        "<a href='a.html'>xmlAlloc</a>, <a href='b.html'>xmlFree</a>"
        "</dd></dl>loose text<div>div text</div>"
        "<blockquote>quoted</blockquote></body>"
    ) == [
        ("heading", "Letter a:"),
        ("list_item", "alloc"),
        ("list_item", "xmlAlloc, xmlFree"),
        ("paragraph", "loose text"),
        ("paragraph", "div text"),
        ("paragraph", "quoted"),
    ]


def test_paragraph_chunks_keep_their_previous_text() -> None:
    # A <p> chunk is text_content().strip(), as when only <p> was chunked,
    # so existing paragraph chunk ids do not change.
    html = "<body><p>\n  first <b>bold</b>\n  line\n</p></body>"
    (chunk,) = _parse(html).chunks
    assert chunk.chunk_kind == "paragraph"
    assert chunk.text == "first bold\n  line"


def test_nested_blocks_are_not_duplicated() -> None:
    html = (
        "<body><div>before<ul><li>outer<ol><li>inner</li></ol>after inner"
        "</li></ul>after list<blockquote><p>quoted para</p></blockquote>"
        "</div></body>"
    )
    doc = _parse(html)
    assert [(c.chunk_kind, c.text) for c in doc.chunks] == [
        ("paragraph", "before"),
        ("list_item", "outer"),
        ("list_item", "inner"),
        ("list_item", "after inner"),
        ("paragraph", "after list"),
        ("paragraph", "quoted para"),
    ]
    # Every body character is chunked exactly once, in order.
    assert "".join(_non_ws(c.text) for c in doc.chunks) == _non_ws(doc.text)


def test_script_style_noscript_and_head_never_become_chunks() -> None:
    doc = _parse(
        "<html><head><title>T</title><style>p{color:red}</style>"
        "<script>var hidden = 1;</script></head><body>"
        "<p>visible<script>inline()</script></p>"
        "<div><noscript>enable js</noscript>tail kept</div>"
        "<!-- a comment --><pre>code<style>x{}</style></pre></body></html>"
    )
    assert doc.metadata["title"] == "T"
    assert [(c.chunk_kind, c.text) for c in doc.chunks] == [
        ("paragraph", "visible"),
        ("paragraph", "tail kept"),
        ("code_block", "code"),
    ]
    joined = " ".join(c.text for c in doc.chunks)
    for hidden in ("hidden", "inline()", "enable js", "color", "a comment"):
        assert hidden not in joined


def test_whitespace_only_blocks_make_no_chunk() -> None:
    assert _pairs("<body><div>  <p> </p>\n<ul><li>\t</li></ul></div></body>") == []


def test_fragment_without_html_or_body_is_chunked() -> None:
    assert _pairs("<span>just <b>inline</b></span>") == [
        ("paragraph", "just inline"),
    ]


def test_empty_input_keeps_the_empty_document_contract() -> None:
    doc = _parse("")
    assert doc.text == "" and doc.chunks == [] and doc.metadata == {"empty": True}


def test_non_bytes_input_is_refused() -> None:
    with pytest.raises(TypeError):
        HtmlLxmlParser().parse(12)  # type: ignore[arg-type]
