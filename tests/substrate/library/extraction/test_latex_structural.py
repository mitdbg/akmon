from __future__ import annotations

from okg.substrate.library.extraction.chunk.latex_structural import (
    LatexStructuralChunker,
)


GOLDEN_LATEX = r"""\documentclass{article}
\newcommand{\vect}[1]{\mathbf{#1}}
\begin{document}
\section{Introduction}

First paragraph with \vect{x}, café, and α.

Second paragraph with an escaped percent \% sign.

\begin{equation}
E = mc^2
\end{equation}

\begin{figure}
\includegraphics{never-read.png}
\caption{A bounded figure caption.}
\end{figure}

\begin{table}
\begin{tabular}{cc}
a & b \\
c & d \\
\end{tabular}
\end{table}

\begin{itemize}
\item First item.
\item Second item with nested content:
  \begin{enumerate}
  \item Nested item stays in its parent span.
  \end{enumerate}
\end{itemize}
\end{document}
"""


def test_structural_golden_counts_paths_exact_spans_and_determinism() -> None:
    chunker = LatexStructuralChunker()
    first = chunker.chunk(GOLDEN_LATEX)
    second = chunker.chunk(GOLDEN_LATEX)
    assert first == second
    assert chunker.last_warnings == []

    kinds = [chunk.chunk_kind for chunk in first]
    assert kinds.count("paragraph") == 2
    assert kinds.count("equation") == 1
    assert kinds.count("figure_caption") == 1
    assert kinds.count("table") == 1
    assert kinds.count("list_item") == 2
    assert len(first) == 7
    assert all(chunk.heading_path == ["Introduction"] for chunk in first)

    for chunk in first:
        assert chunk.span is not None
        if chunk.chunk_kind == "table":
            assert chunk.text == "a\tb\nc\td"
            assert chunk.extras["table_grid_status"] == "parsed"
        else:
            assert chunk.text == GOLDEN_LATEX[slice(*chunk.span)]
    for left, right in zip(first, first[1:]):
        assert left.span is not None and right.span is not None
        assert left.span[1] <= right.span[0]

    list_items = [chunk.text for chunk in first if chunk.chunk_kind == "list_item"]
    assert len(list_items) == 2
    assert "Nested item stays in its parent span" in list_items[1]
    assert not any(text.startswith(r"\item Nested") for text in list_items)


def test_comments_and_verbatim_are_inert_but_code_span_is_retained() -> None:
    raw = r"""\begin{document}
% \section{Fake}
\section{Real}

Visible prose.

\begin{verbatim}
\section{Not a heading}
\begin{equation}not math\end{equation}
\end{verbatim}
\end{document}
"""
    chunks = LatexStructuralChunker().chunk(raw)
    assert [chunk.chunk_kind for chunk in chunks] == ["paragraph", "code_block"]
    assert all(chunk.heading_path == ["Real"] for chunk in chunks)


def test_balanced_heading_hierarchy_and_display_delimiters() -> None:
    raw = r"""\begin{document}
\section{A {Nested} Title}

Alpha.

\subsection{B}

\[x + y\]

\subsubsection{C}

$$z$$
\end{document}
"""
    chunks = LatexStructuralChunker().chunk(raw)
    assert [chunk.heading_path for chunk in chunks] == [
        ["A {Nested} Title"],
        ["A {Nested} Title", "B"],
        ["A {Nested} Title", "B", "C"],
    ]
    assert [chunk.chunk_kind for chunk in chunks] == [
        "paragraph",
        "equation",
        "equation",
    ]


def test_optional_heading_title_uses_long_title_and_escaped_heading_is_inert() -> None:
    source = r"""\\section{Escaped}
\section[Short]{Long Title}
Visible body.
"""
    chunks = LatexStructuralChunker().chunk(source)

    assert len(chunks) == 2
    assert chunks[0].heading_path == []
    assert chunks[0].text == r"\\section{Escaped}"
    assert chunks[1].heading_path == ["Long Title"]
    assert chunks[1].text == "Visible body."


def test_malformed_constructs_warn_and_do_not_degrade_to_prose() -> None:
    raw = r"""\begin{document}
Safe paragraph.

\begin{equation}
unclosed math

After invalid span.
\end{document}
"""
    chunker = LatexStructuralChunker()
    chunks = chunker.chunk(raw)
    offset = raw.index(r"\begin{equation}")
    assert f"latex_unbalanced_environment:{offset}" in chunker.last_warnings
    assert all("unclosed math" not in chunk.text for chunk in chunks)


def test_include_commands_are_plain_source_and_never_followed() -> None:
    raw = r"""\begin{document}
Before \input{does-not-exist.tex} after.
\end{document}
"""
    chunks = LatexStructuralChunker().chunk(raw)
    assert [chunk.text for chunk in chunks] == [
        r"Before \input{does-not-exist.tex} after."
    ]


def test_escaped_structural_commands_remain_inert_source_text() -> None:
    raw = r"""\begin{document}
\\section{Not a heading}
\\begin{equation}not math\\end{equation}
\\caption{Not a caption}
\\item Not a list item.
\\[not display math\\]
\end{document}
"""
    chunker = LatexStructuralChunker()
    chunks = chunker.chunk(raw)
    assert chunker.last_warnings == []
    assert [chunk.chunk_kind for chunk in chunks] == ["paragraph"]
    assert chunks[0].heading_path == []
    assert chunks[0].text.startswith(r"\\section{Not a heading}")


def test_balanced_optional_and_unbraced_macros_are_masked_from_prose() -> None:
    raw = r"""\begin{document}
\newcommand{\optional}[2][x]{#1/#2}
\newcommand\bare[1]{bare #1}

Body after definitions.
\end{document}
"""
    chunker = LatexStructuralChunker()
    chunks = chunker.chunk(raw)
    assert [definition.name for definition in chunker.last_macro_definitions] == [
        "optional",
        "bare",
    ]
    assert [chunk.text for chunk in chunks] == ["Body after definitions."]


def test_escaped_display_close_does_not_truncate_equation() -> None:
    raw = r"""\begin{document}
\[x + \\] + y\]
\end{document}
"""
    chunks = LatexStructuralChunker().chunk(raw)
    assert [(chunk.chunk_kind, chunk.text) for chunk in chunks] == [
        ("equation", r"\[x + \\] + y\]"),
    ]


def test_escaped_list_close_does_not_truncate_items() -> None:
    raw = r"""\begin{document}
\begin{itemize}
\item Before \\end{itemize} remains first.
\item Second remains visible.
\end{itemize}
\end{document}
"""
    chunks = LatexStructuralChunker().chunk(raw)
    items = [chunk.text for chunk in chunks if chunk.chunk_kind == "list_item"]
    assert items == [
        r"\item Before \\end{itemize} remains first.",
        r"\item Second remains visible.",
    ]
