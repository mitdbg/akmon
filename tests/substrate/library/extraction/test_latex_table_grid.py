from __future__ import annotations

import hashlib
import socket
import subprocess
from pathlib import Path

import pytest

from okg.substrate.library.extraction import latex_table_grid as latex_grid_module
from okg.substrate.library.extraction._protocols import ParsedChunk
from okg.substrate.library.extraction.latex_macros import scan_macro_definitions
from okg.substrate.library.extraction.latex_table_grid import (
    LatexTableGridError,
    expand_table_macros,
    parse_latex_table_grid,
)
from okg.substrate.library.extraction.parse.latex import LatexParser


def _table_chunk(source: str) -> ParsedChunk:
    chunks = LatexParser().parse(source).chunks
    tables = [chunk for chunk in chunks if chunk.chunk_kind == "table"]
    assert len(tables) == 1
    return tables[0]


def test_wrapped_table_macros_comments_rules_spans_math_and_unicode() -> None:
    source = r"""\newcommand{\headers}[3]{#1 & #2 & #3}
\def\measurement#1{#1 ± 1}
\begin{document}
\section{Results}
\begin{table}
\caption{Measured results}
\resizebox{\linewidth}{!}{
\begin{tabular}{lcc}
\toprule
\headers{Method}{Metric A}{Metric B} \\
\midrule
Alpha \& Café & \multicolumn{2}{c}{$\measurement{10}$} \\ % ignored
Beta & {$x & y$} & ✓ \\
\bottomrule
\end{tabular}}
\end{table}
\end{document}
"""
    first = _table_chunk(source)
    second = _table_chunk(source)

    assert first == second
    assert first.heading_path == ["Results"]
    assert first.extras["table_grid_status"] == "parsed"
    assert first.extras["table_extraction_lane"] == "latex"
    assert first.extras["source_locator"] == f"tex:{first.span[0]}:{first.span[1]}"
    raw_slice = source[slice(*first.span)]
    assert (
        first.extras["source_content_sha256"]
        == hashlib.sha256(raw_slice.encode()).hexdigest()
    )
    assert first.text == (
        "Method\tMetric A\tMetric B\nAlpha \\& Café\t$10 ± 1$\t\nBeta\t{$x & y$}\t✓"
    )
    grid = first.extras["table_grid"]
    assert grid["rows"][0]["cells"][1]["column_span"] == 2
    assert grid["rows"][0]["cells"][1]["column_header"] == "Metric A / Metric B"


@pytest.mark.parametrize(
    "wrapper",
    [
        r"\begin{tabular}{lc}H & V \\ A & 1 \\ \end{tabular}",
        r"\begin{center}\begin{tabular}{lc}H & V \\ A & 1 \\ \end{tabular}\end{center}",
        r"\resizebox{10cm}{!}{\begin{tabular}{lc}H & V \\ A & 1 \\ \end{tabular}}",
    ],
)
def test_supported_bare_center_and_resizebox_wrappers(wrapper: str) -> None:
    chunk = _table_chunk(rf"\begin{{document}}{wrapper}\end{{document}}")
    assert chunk.extras["table_grid_status"] == "parsed"
    assert chunk.text == "H\tV\nA\t1"


def test_macro_forms_follow_source_order_and_document_scope() -> None:
    source = r"""\newcommand{\value}{A}
\providecommand{\value}{ignored}
\renewcommand{\value}{B}
\DeclareRobustCommand{\value}{C}
\def\value{D}
\begin{document}
\begin{tabular}{lc}
Name & Value \\
Alpha & \value \\
\end{tabular}
\renewcommand{\value}{after}
\end{document}
"""
    chunk = _table_chunk(source)
    assert chunk.extras["table_grid_status"] == "parsed"
    assert chunk.text == "Name\tValue\nAlpha\tD"

    after_only = r"""\begin{document}
\begin{tabular}{lc}Name & Value \\ Alpha & \late \\ \end{tabular}
\newcommand{\late}{after}
\end{document}"""
    assert _table_chunk(after_only).text == "Name\tValue\nAlpha\t\\late"


def test_macro_definitions_inside_table_apply_only_to_later_uses() -> None:
    wrapper_definition = r"""\begin{document}
\begin{table}
\newcommand{\row}[2]{#1 & #2}
\begin{tabular}{lc}Name & Value \\ \row{Alpha}{1} \\ \end{tabular}
\end{table}
\end{document}"""
    assert _table_chunk(wrapper_definition).text == "Name\tValue\nAlpha\t1"

    body_definition = r"""\begin{document}
\begin{tabular}{lc}
\newcommand{\row}[2]{#1 & #2}
Name & Value \\ \row{Alpha}{1} \\ \end{tabular}
\end{document}"""
    assert _table_chunk(body_definition).text == "Name\tValue\nAlpha\t1"

    declaration_after_use = r"""\begin{document}
\begin{tabular}{lc}
Name & Value \\ Alpha & \late \\
\newcommand{\late}{after}
\end{tabular}
\end{document}"""
    assert _table_chunk(declaration_after_use).text == "Name\tValue\nAlpha\t\\late"


def test_macro_arguments_are_brace_balanced_and_literal_hash_is_supported() -> None:
    source = r"""\newcommand{\format}[2]{{#1}: ## #2}
\begin{document}
\begin{tabular}{lc}
Name & Value \\
Alpha & \format{{nested}}{$x_{1}$} \\
\end{tabular}
\end{document}"""
    chunk = _table_chunk(source)
    assert chunk.text == "Name\tValue\nAlpha\t{{nested}}: # $x_{1}$"


@pytest.mark.parametrize(
    ("definitions", "code"),
    [
        (r"\newcommand{\x}{a}\newcommand{\x}{b}", "macro_already_defined"),
        (r"\renewcommand{\x}{b}", "macro_not_defined"),
        (r"\newcommand{\x}[1][a]{#1}", "unsupported_optional_macro_argument"),
        (r"\newcommand{\x}[5]{#1}", "macro_arity_exceeded"),
        (r"\def\x#2#1{#1}", "unsupported_macro_parameter_grammar"),
        (r"\newcommand{\x}[1]{#2}", "invalid_macro_placeholder"),
    ],
)
def test_unsupported_macro_definition_semantics_fail_loudly(
    definitions: str, code: str
) -> None:
    source = definitions + r"\begin{tabular}{l}H \\ A \\ \end{tabular}"
    scanned, _, _ = scan_macro_definitions(source)
    table_start = source.index(r"\begin{tabular}")
    with pytest.raises(LatexTableGridError) as exc_info:
        parse_latex_table_grid(
            source[table_start:],
            macro_definitions=scanned,
            table_start=table_start,
        )
    assert exc_info.value.code == code


def test_cycles_missing_arguments_and_malformed_rows_are_typed_failures() -> None:
    cases = [
        (
            r"\newcommand{\x}{\x}\begin{tabular}{l}H \\ \x \\ \end{tabular}",
            "macro_expansion_cycle",
        ),
        (
            r"\newcommand{\x}[1]{#1}\begin{tabular}{l}H \\ \x \\ \end{tabular}",
            "macro_argument_missing",
        ),
        (r"\begin{tabular}{lc}H & V \\ A \\ \end{tabular}", "non_rectangular_table"),
        (
            r"\begin{tabular}{lc}H & V \\ \multirow{2}{*}{A} & 1 \\ \end{tabular}",
            "unsupported_row_span",
        ),
    ]
    for source, code in cases:
        definitions, _, _ = scan_macro_definitions(source)
        table_start = source.index(r"\begin{tabular}")
        with pytest.raises(LatexTableGridError) as exc_info:
            parse_latex_table_grid(
                source[table_start:],
                macro_definitions=definitions,
                table_start=table_start,
            )
        assert exc_info.value.code == code


def test_malformed_table_emits_one_failed_occurrence_without_a_grid() -> None:
    source = r"""\begin{document}
\begin{table}
\begin{tabular}{lc}H & V \\ missing \\ \end{tabular}
\end{table}
\end{document}"""
    chunk = _table_chunk(source)

    assert chunk.extras["table_grid_status"] == "parse_failed"
    assert chunk.extras["table_grid_error_code"] == "non_rectangular_table"
    assert "table_grid" not in chunk.extras
    assert chunk.text == source[slice(*chunk.span)]


def test_over_budget_table_emits_one_budget_failure_without_partial_grid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(latex_grid_module, "MAX_CELL_BYTES", 3)
    source = (
        r"\begin{document}\begin{tabular}{lc}H & V \\ Alpha & long \\ "
        r"\end{tabular}\end{document}"
    )
    chunk = _table_chunk(source)

    assert chunk.extras["table_grid_status"] == "budget_exhausted"
    assert chunk.extras["table_grid_error_code"] == "cell_text_budget_exceeded"
    assert "table_grid" not in chunk.extras


@pytest.mark.parametrize(
    ("limit_name", "limit", "source", "code"),
    [
        (
            "MAX_MACRO_DEFINITIONS",
            1,
            r"\newcommand{\a}{A}\newcommand{\b}{B}\begin{tabular}{l}H \\ A \\ \end{tabular}",
            "macro_definition_budget_exceeded",
        ),
        (
            "MAX_EXPANSION_DEPTH",
            1,
            r"\newcommand{\a}{\b}\newcommand{\b}{B}\begin{tabular}{l}H \\ \a \\ \end{tabular}",
            "macro_expansion_depth_exceeded",
        ),
        (
            "MAX_MACRO_SUBSTITUTIONS",
            1,
            r"\newcommand{\a}{A}\begin{tabular}{ll}H & V \\ \a & \a \\ \end{tabular}",
            "macro_substitution_budget_exceeded",
        ),
        (
            "MAX_COLUMNS",
            1,
            r"\begin{tabular}{ll}H & V \\ A & B \\ \end{tabular}",
            "column_budget_exceeded",
        ),
        (
            "MAX_ROWS",
            1,
            r"\begin{tabular}{l}H \\ A \\ B \\ \end{tabular}",
            "row_budget_exceeded",
        ),
        (
            "MAX_ANCHOR_CELLS",
            1,
            r"\begin{tabular}{ll}H & V \\ A & B \\ \end{tabular}",
            "anchor_cell_budget_exceeded",
        ),
    ],
)
def test_every_structural_resource_bound_is_enforced(
    monkeypatch: pytest.MonkeyPatch,
    limit_name: str,
    limit: int,
    source: str,
    code: str,
) -> None:
    monkeypatch.setattr(latex_grid_module, limit_name, limit)
    definitions, _, _ = scan_macro_definitions(source)
    table_start = source.index(r"\begin{tabular}")
    with pytest.raises(LatexTableGridError) as exc_info:
        parse_latex_table_grid(
            source[table_start:],
            macro_definitions=definitions,
            table_start=table_start,
        )
    assert exc_info.value.code == code
    assert exc_info.value.status == "budget_exhausted"


def test_expanded_grid_and_searchable_text_byte_bounds_are_enforced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = r"\begin{tabular}{l}Header \\ Alpha \\ \end{tabular}"
    monkeypatch.setattr(latex_grid_module, "MAX_EXPANDED_BYTES", 8)
    with pytest.raises(LatexTableGridError) as exc_info:
        parse_latex_table_grid(source)
    assert exc_info.value.code == "expanded_table_budget_exceeded"

    monkeypatch.setattr(latex_grid_module, "MAX_EXPANDED_BYTES", 1 << 20)
    monkeypatch.setattr(latex_grid_module, "MAX_GRID_BYTES", 16)
    with pytest.raises(LatexTableGridError) as exc_info:
        parse_latex_table_grid(source)
    assert exc_info.value.code == "canonical_grid_budget_exceeded"

    monkeypatch.setattr(latex_grid_module, "MAX_GRID_BYTES", 1 << 20)
    monkeypatch.setattr(latex_grid_module, "MAX_SEARCHABLE_TEXT_BYTES", 8)
    with pytest.raises(LatexTableGridError) as exc_info:
        parse_latex_table_grid(source)
    assert exc_info.value.code == "searchable_text_budget_exceeded"


def test_anchor_budget_counts_only_represented_data_cells_at_exact_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(latex_grid_module, "MAX_ANCHOR_CELLS", 2)
    grid = parse_latex_table_grid(
        r"\begin{tabular}{ll}H & V \\ A & B \\ \end{tabular}"
    )

    assert sum(len(row.cells) for row in grid.rows) == 2


def test_booktabs_cmidrule_parenthesized_trim_syntax_is_stripped() -> None:
    grid = parse_latex_table_grid(
        r"\begin{tabular}{lc}"
        r"\cmidrule(lr){1-2} Method & Score \\ Alpha & 10 \\ "
        r"\end{tabular}"
    )

    assert grid.col_headers == ("Method", "Score")
    assert grid.searchable_text() == "Method\tScore\nAlpha\t10"


def test_macro_substitution_budget_is_shared_by_column_spec_and_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = (
        r"\newcommand{\column}{l}\newcommand{\value}{A}"
        r"\begin{tabular}{\column}Header \\ \value \\ \end{tabular}"
    )
    definitions, _, _ = scan_macro_definitions(source)
    table_start = source.index(r"\begin{tabular}")
    monkeypatch.setattr(latex_grid_module, "MAX_MACRO_SUBSTITUTIONS", 1)

    with pytest.raises(LatexTableGridError) as exc_info:
        parse_latex_table_grid(
            source[table_start:],
            macro_definitions=definitions,
            table_start=table_start,
        )
    assert exc_info.value.code == "macro_substitution_budget_exceeded"
    assert exc_info.value.status == "budget_exhausted"


def test_parser_does_not_touch_tex_shell_filesystem_or_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("external effect attempted")

    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    source = r"""\begin{document}
\input{never-read.tex}
\begin{tabular}{lc}Name & Value \\ Alpha & 1 \\ \end{tabular}
\end{document}"""
    assert _table_chunk(source).extras["table_grid_status"] == "parsed"


def test_expand_table_macros_is_deterministic() -> None:
    source = r"\newcommand{\pair}[2]{#1 & #2}"
    definitions, _, _ = scan_macro_definitions(source)
    fragment = r"\pair{A {nested}}{$x_1$}"
    first = expand_table_macros(fragment, definitions, table_start=len(source))
    second = expand_table_macros(fragment, definitions, table_start=len(source))
    assert first == second == r"A {nested} & $x_1$"
