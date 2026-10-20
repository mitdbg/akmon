"""Bounded, deterministic LaTeX tabular lowering into :mod:`table_grid`.

The implementation is deliberately a source scanner, not a TeX interpreter:
it performs no compilation, file access, shell invocation, or network access.
Unsupported grammar fails with a stable code instead of guessing a grid.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final, Mapping, Sequence

from okg.substrate.library.extraction.latex_macros import (
    MacroDefinition,
    extract_balanced_group,
)
from okg.substrate.library.extraction.table_grid import (
    TableGridError,
    TableGridV1,
    normalize_table_text,
)


MAX_MACRO_DEFINITIONS: Final = 256
MAX_EXPANSION_DEPTH: Final = 16
MAX_MACRO_SUBSTITUTIONS: Final = 10_000
MAX_EXPANDED_BYTES: Final = 1 << 20
MAX_COLUMNS: Final = 256
MAX_ROWS: Final = 10_000
MAX_ANCHOR_CELLS: Final = 100_000
MAX_CELL_BYTES: Final = 16 << 10
MAX_GRID_BYTES: Final = 1 << 20
MAX_SEARCHABLE_TEXT_BYTES: Final = 1 << 20

_TABULAR_BEGIN_RE = re.compile(r"\\begin\s*\{(?P<name>tabular\*?|longtable|array)\}")
_RULE_RE = re.compile(
    r"\\(?:hline|toprule|midrule|bottomrule)\b"
    r"|\\(?:cline|cmidrule)"
    r"(?:\s*(?:\[[^\]\n]*\]|\([^()\n]*\)))?\s*\{[^{}\n]*\}"
    r"|\\addlinespace(?:\s*\[[^\]\n]*\])?"
)
_COMMAND_NAME_RE = re.compile(r"[A-Za-z@]+")
_DEF_PARAMETERS_RE = re.compile(r"(?:#[1-4])*")


class LatexTableGridError(ValueError):
    """Stable LaTeX-table rejection classified for occurrence emission."""

    def __init__(
        self, code: str, message: str, *, status: str = "parse_failed"
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


def _fail(code: str, message: str) -> None:
    raise LatexTableGridError(code, message)


def _budget(code: str, message: str) -> None:
    raise LatexTableGridError(code, message, status="budget_exhausted")


def _is_escaped(text: str, position: int) -> bool:
    slashes = 0
    index = position - 1
    while index >= 0 and text[index] == "\\":
        slashes += 1
        index -= 1
    return slashes % 2 == 1


def _mask_comments(text: str) -> str:
    chars = list(text)
    index = 0
    while index < len(chars):
        if chars[index] == "%" and not _is_escaped(text, index):
            while index < len(chars) and chars[index] != "\n":
                chars[index] = " "
                index += 1
            continue
        index += 1
    return "".join(chars)


def _skip_space(text: str, position: int) -> int:
    while position < len(text) and text[position].isspace():
        position += 1
    return position


def _balanced_square(text: str, open_pos: int) -> int | None:
    if open_pos >= len(text) or text[open_pos] != "[":
        return None
    depth = 1
    index = open_pos + 1
    while index < len(text):
        if text[index] == "\\" and index + 1 < len(text):
            index += 2
            continue
        if text[index] == "[":
            depth += 1
        elif text[index] == "]":
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return None


def _definition_arity(definition: MacroDefinition) -> int:
    if definition.form == "def":
        parameters = "".join(definition.parameter_text.split())
        if _DEF_PARAMETERS_RE.fullmatch(parameters) is None:
            _fail(
                "unsupported_macro_parameter_grammar",
                rf"\def\{definition.name} must use contiguous undelimited parameters",
            )
        arity = len(parameters) // 2
        if parameters != "".join(f"#{index}" for index in range(1, arity + 1)):
            _fail(
                "unsupported_macro_parameter_grammar",
                rf"\def\{definition.name} parameters must be #1 through #N in order",
            )
        return arity
    if definition.optional_default is not None:
        _fail(
            "unsupported_optional_macro_argument",
            rf"\{definition.name} uses an unsupported optional default argument",
        )
    if definition.arity < 0 or definition.arity > 4:
        _fail("macro_arity_exceeded", "macro arity must be between zero and four")
    return definition.arity


def _validate_macro_body(definition: MacroDefinition, arity: int) -> None:
    index = 0
    while index < len(definition.body):
        if definition.body[index] != "#":
            index += 1
            continue
        if index + 1 >= len(definition.body):
            _fail("invalid_macro_placeholder", "macro body ends with a bare hash")
        token = definition.body[index + 1]
        if token == "#":
            index += 2
            continue
        if not token.isdigit() or int(token) < 1 or int(token) > arity:
            _fail(
                "invalid_macro_placeholder",
                rf"\{definition.name} references a placeholder outside its arity",
            )
        index += 2


@dataclass(frozen=True, slots=True)
class _BoundMacro:
    name: str
    body: str
    arity: int


def _macro_environment(
    definitions: Sequence[MacroDefinition], *, table_start: int
) -> dict[str, _BoundMacro]:
    applicable = [item for item in definitions if item.span[1] <= table_start]
    if len(applicable) > MAX_MACRO_DEFINITIONS:
        _budget(
            "macro_definition_budget_exceeded",
            f"more than {MAX_MACRO_DEFINITIONS} macro definitions precede the table",
        )
    environment: dict[str, _BoundMacro] = {}
    for definition in sorted(applicable, key=lambda item: item.span[0]):
        arity = _definition_arity(definition)
        _validate_macro_body(definition, arity)
        bound = _BoundMacro(definition.name, definition.body, arity)
        if definition.form == "newcommand":
            if definition.name in environment:
                _fail(
                    "macro_already_defined",
                    rf"\newcommand cannot replace \{definition.name}",
                )
            environment[definition.name] = bound
        elif definition.form == "renewcommand":
            if definition.name not in environment:
                _fail(
                    "macro_not_defined",
                    rf"\renewcommand cannot find \{definition.name}",
                )
            environment[definition.name] = bound
        elif definition.form == "providecommand":
            environment.setdefault(definition.name, bound)
        elif definition.form in {"DeclareRobustCommand", "def"}:
            environment[definition.name] = bound
        else:
            _fail("unsupported_macro_definition", "unsupported macro definition form")
    return environment


@dataclass(slots=True)
class _ExpansionBudget:
    substitutions: int = 0

    def count_substitution(self) -> None:
        self.substitutions += 1
        if self.substitutions > MAX_MACRO_SUBSTITUTIONS:
            _budget(
                "macro_substitution_budget_exceeded",
                f"macro expansion exceeded {MAX_MACRO_SUBSTITUTIONS} substitutions",
            )

    @staticmethod
    def check_size(text: str) -> None:
        if len(text.encode("utf-8")) > MAX_EXPANDED_BYTES:
            _budget(
                "expanded_table_budget_exceeded",
                f"expanded table source exceeds {MAX_EXPANDED_BYTES} bytes",
            )


def _substitute_body(body: str, arguments: list[str]) -> str:
    output: list[str] = []
    index = 0
    while index < len(body):
        if body[index] != "#":
            output.append(body[index])
            index += 1
            continue
        token = body[index + 1]
        if token == "#":
            output.append("#")
        else:
            output.append(arguments[int(token) - 1])
        index += 2
    return "".join(output)


def _expand_fragment(
    text: str,
    environment: Mapping[str, _BoundMacro],
    budget: _ExpansionBudget,
    *,
    depth: int,
    stack: tuple[str, ...],
) -> str:
    if depth > MAX_EXPANSION_DEPTH:
        _budget(
            "macro_expansion_depth_exceeded",
            f"macro expansion exceeded {MAX_EXPANSION_DEPTH} levels",
        )
    output: list[str] = []
    index = 0
    while index < len(text):
        if text[index] != "\\" or _is_escaped(text, index):
            output.append(text[index])
            index += 1
            continue
        name_match = _COMMAND_NAME_RE.match(text, index + 1)
        if name_match is None:
            output.append(text[index])
            index += 1
            continue
        name = name_match.group(0)
        macro = environment.get(name)
        if macro is None:
            output.append(text[index : name_match.end()])
            index = name_match.end()
            continue
        if name in stack:
            _fail("macro_expansion_cycle", rf"macro expansion cycle at \{name}")
        cursor = name_match.end()
        arguments: list[str] = []
        for _ in range(macro.arity):
            cursor = _skip_space(text, cursor)
            if cursor >= len(text) or text[cursor] != "{":
                _fail(
                    "macro_argument_missing",
                    rf"\{name} requires exactly {macro.arity} braced arguments",
                )
            balanced = extract_balanced_group(text, cursor)
            if balanced is None:
                _fail(
                    "unbalanced_macro_argument", rf"\{name} has an unbalanced argument"
                )
            argument, cursor = balanced
            arguments.append(argument)
        budget.count_substitution()
        replacement = _substitute_body(macro.body, arguments)
        replacement = _expand_fragment(
            replacement,
            environment,
            budget,
            depth=depth + 1,
            stack=(*stack, name),
        )
        output.append(replacement)
        budget.check_size("".join(output))
        index = cursor
    expanded = "".join(output)
    budget.check_size(expanded)
    return expanded


def expand_table_macros(
    text: str,
    definitions: Sequence[MacroDefinition],
    *,
    table_start: int,
    budget: _ExpansionBudget | None = None,
) -> str:
    environment = _macro_environment(definitions, table_start=table_start)
    return _expand_fragment(
        text,
        environment,
        budget or _ExpansionBudget(),
        depth=0,
        stack=(),
    )


@dataclass(frozen=True, slots=True)
class _TabularParts:
    column_spec: str
    column_spec_start: int
    body: str
    body_start: int


def _matching_environment_end(
    scan: str, begin_match: re.Match[str]
) -> tuple[int, int] | None:
    name = begin_match.group("name")
    token_re = re.compile(r"\\(?P<boundary>begin|end)\s*\{" + re.escape(name) + r"\}")
    depth = 1
    for token in token_re.finditer(scan, begin_match.end()):
        if _is_escaped(scan, token.start()):
            continue
        depth += 1 if token.group("boundary") == "begin" else -1
        if depth == 0:
            return token.start(), token.end()
    return None


def _tabular_parts(table_source: str) -> _TabularParts:
    scan = _mask_comments(table_source)
    candidates: list[tuple[re.Match[str], tuple[int, int]]] = []
    for begin in _TABULAR_BEGIN_RE.finditer(scan):
        if _is_escaped(scan, begin.start()):
            continue
        end = _matching_environment_end(scan, begin)
        if end is None:
            _fail(
                "unbalanced_tabular_environment", "tabular environment is not balanced"
            )
        candidates.append((begin, end))
    if not candidates:
        _fail(
            "tabular_not_found",
            "table wrapper contains no supported tabular environment",
        )
    top_level = [
        candidate
        for candidate in candidates
        if not any(
            other[0].start() < candidate[0].start() < other[1][1]
            for other in candidates
            if other is not candidate
        )
    ]
    if len(top_level) != 1:
        _fail(
            "multiple_tabular_environments",
            "one table occurrence must contain one grid",
        )
    begin, (end_start, _) = top_level[0]
    cursor = _skip_space(scan, begin.end())
    if begin.group("name") == "tabular*":
        if cursor >= len(scan) or scan[cursor] != "{":
            _fail("missing_tabular_width", "tabular* requires a width argument")
        width = extract_balanced_group(scan, cursor)
        if width is None:
            _fail("unbalanced_tabular_width", "tabular* width is unbalanced")
        _, cursor = width
        cursor = _skip_space(scan, cursor)
    if cursor < len(scan) and scan[cursor] == "[":
        optional_end = _balanced_square(scan, cursor)
        if optional_end is None:
            _fail(
                "unbalanced_tabular_position", "tabular position option is unbalanced"
            )
        cursor = _skip_space(scan, optional_end)
    if cursor >= len(scan) or scan[cursor] != "{":
        _fail("missing_column_spec", "tabular environment requires a column spec")
    column_group = extract_balanced_group(scan, cursor)
    if column_group is None:
        _fail("unbalanced_column_spec", "tabular column spec is unbalanced")
    column_spec, body_start = column_group
    return _TabularParts(
        column_spec=column_spec,
        column_spec_start=cursor + 1,
        body=scan[body_start:end_start],
        body_start=body_start,
    )


def _expand_fragment_in_source_order(
    text: str,
    definitions: Sequence[MacroDefinition],
    *,
    fragment_start: int,
    budget: _ExpansionBudget,
) -> str:
    """Expand a fragment while applying declarations only after their spans."""

    fragment_end = fragment_start + len(text)
    local_definitions = sorted(
        (
            definition
            for definition in definitions
            if fragment_start <= definition.span[0]
            and definition.span[1] <= fragment_end
        ),
        key=lambda definition: definition.span[0],
    )
    output: list[str] = []
    cursor = 0
    for definition in local_definitions:
        local_start = definition.span[0] - fragment_start
        local_end = definition.span[1] - fragment_start
        output.append(
            expand_table_macros(
                text[cursor:local_start],
                definitions,
                table_start=definition.span[0],
                budget=budget,
            )
        )
        output.append(" ")
        cursor = local_end
    output.append(
        expand_table_macros(
            text[cursor:],
            definitions,
            table_start=fragment_end,
            budget=budget,
        )
    )
    return "".join(output)


def _column_count(column_spec: str) -> int:
    def count_fragment(fragment: str) -> int:
        count = 0
        index = 0
        while index < len(fragment):
            char = fragment[index]
            if char.isspace() or char == "|":
                index += 1
                continue
            if char in "lcrXSD":
                count += 1
                index += 1
                continue
            if char in "pmb":
                cursor = _skip_space(fragment, index + 1)
                if cursor >= len(fragment) or fragment[cursor] != "{":
                    _fail("invalid_column_spec", f"{char} column requires a width")
                group = extract_balanced_group(fragment, cursor)
                if group is None:
                    _fail("unbalanced_column_spec", "column width is unbalanced")
                _, index = group
                count += 1
                continue
            if char in "@!><":
                cursor = _skip_space(fragment, index + 1)
                if cursor >= len(fragment) or fragment[cursor] != "{":
                    _fail("invalid_column_spec", f"{char} modifier requires a group")
                group = extract_balanced_group(fragment, cursor)
                if group is None:
                    _fail("unbalanced_column_spec", "column modifier is unbalanced")
                _, index = group
                continue
            if char == "*":
                cursor = _skip_space(fragment, index + 1)
                multiplier_group = extract_balanced_group(fragment, cursor)
                if multiplier_group is None:
                    _fail("invalid_column_spec", "star column spec requires a count")
                multiplier_text, cursor = multiplier_group
                if not multiplier_text.strip().isdigit():
                    _fail("invalid_column_spec", "star column count must be an integer")
                cursor = _skip_space(fragment, cursor)
                repeated_group = extract_balanced_group(fragment, cursor)
                if repeated_group is None:
                    _fail("invalid_column_spec", "star column spec requires a fragment")
                repeated, index = repeated_group
                count += int(multiplier_text.strip()) * count_fragment(repeated)
                continue
            if char == "\\":
                _fail(
                    "unresolved_structural_macro",
                    "column spec contains an unresolved structural macro",
                )
            _fail("unsupported_column_spec", f"unsupported column token {char!r}")
        return count

    count = count_fragment(column_spec)
    if count < 1:
        _fail("empty_column_spec", "column spec defines no physical columns")
    if count > MAX_COLUMNS:
        _budget("column_budget_exceeded", f"table exceeds {MAX_COLUMNS} columns")
    return count


def _split_grid(text: str, *, rows: bool) -> list[str]:
    parts: list[str] = []
    start = 0
    brace_depth = 0
    math_mode: str | None = None
    index = 0
    while index < len(text):
        char = text[index]
        escaped = _is_escaped(text, index)
        if not escaped and char == "$":
            delimiter = "$$" if text.startswith("$$", index) else "$"
            if math_mode is None:
                math_mode = delimiter
            elif math_mode == delimiter:
                math_mode = None
            index += len(delimiter)
            continue
        if char == "\\" and not escaped and index + 1 < len(text):
            delimiter = text[index : index + 2]
            if delimiter in {r"\(", r"\["} and math_mode is None:
                math_mode = delimiter
                index += 2
                continue
            if (
                delimiter in {r"\)", r"\]"}
                and math_mode == ({r"\)": r"\(", r"\]": r"\["}[delimiter])
            ):
                math_mode = None
                index += 2
                continue
        if math_mode is None:
            if char == "{" and not escaped:
                brace_depth += 1
            elif char == "}" and not escaped:
                brace_depth -= 1
                if brace_depth < 0:
                    _fail(
                        "unbalanced_table_braces",
                        "table contains an unmatched closing brace",
                    )
            elif brace_depth == 0:
                if not rows and char == "&" and not escaped:
                    parts.append(text[start:index])
                    start = index + 1
                elif rows and text.startswith(r"\\", index):
                    parts.append(text[start:index])
                    index += 2
                    if index < len(text) and text[index] == "*":
                        index += 1
                    index = _skip_space(text, index)
                    if index < len(text) and text[index] == "[":
                        optional_end = _balanced_square(text, index)
                        if optional_end is None:
                            _fail(
                                "unbalanced_row_spacing",
                                "row spacing option is unbalanced",
                            )
                        index = optional_end
                    start = index
                    continue
                elif rows and (
                    text.startswith(r"\tabularnewline", index)
                    or re.match(r"\\cr(?![A-Za-z@])", text[index:])
                ):
                    command_end = (
                        index + len(r"\tabularnewline")
                        if text.startswith(r"\tabularnewline", index)
                        else index + len(r"\cr")
                    )
                    parts.append(text[start:index])
                    index = command_end
                    start = index
                    continue
        index += 1
    if brace_depth != 0:
        _fail("unbalanced_table_braces", "table contains unbalanced braces")
    if math_mode is not None:
        _fail("unbalanced_table_math", "table contains unbalanced math delimiters")
    parts.append(text[start:])
    return parts


def _strip_rules(value: str) -> str:
    previous = value
    while True:
        current = _RULE_RE.sub(" ", previous)
        if current == previous:
            return current.strip()
        previous = current


def _group_argument(text: str, cursor: int, *, field: str) -> tuple[str, int]:
    cursor = _skip_space(text, cursor)
    if cursor >= len(text) or text[cursor] != "{":
        _fail("invalid_multicolumn", f"multicolumn {field} must be braced")
    group = extract_balanced_group(text, cursor)
    if group is None:
        _fail("invalid_multicolumn", f"multicolumn {field} is unbalanced")
    return group


def _parse_cell(raw: str) -> tuple[int, str]:
    value = raw.strip()
    if r"\multirow" in value:
        _fail(
            "unsupported_row_span", "row-spanning cells are unsupported in TableGridV1"
        )
    if not value.startswith(r"\multicolumn"):
        return 1, value
    cursor = len(r"\multicolumn")
    span_text, cursor = _group_argument(value, cursor, field="span")
    _, cursor = _group_argument(value, cursor, field="column spec")
    cell_text, cursor = _group_argument(value, cursor, field="content")
    if value[cursor:].strip():
        _fail("unsupported_span_form", "multicolumn must occupy the complete cell")
    if not span_text.strip().isdigit() or int(span_text.strip()) < 1:
        _fail("unsupported_span_form", "multicolumn span must be a positive integer")
    return int(span_text.strip()), cell_text


def _row_cells(row: str, *, column_count: int) -> list[tuple[int, int, str]]:
    cells: list[tuple[int, int, str]] = []
    cursor = 0
    for raw_cell in _split_grid(row, rows=False):
        span, text = _parse_cell(raw_cell)
        if cursor + span > column_count:
            _fail("non_rectangular_table", "row extends beyond the column spec")
        cells.append((cursor, span, normalize_table_text(text, field="cell text")))
        cursor += span
    if cursor != column_count:
        _fail("non_rectangular_table", "row does not cover the column spec")
    return cells


def _enforce_text_budget(text: str, *, code: str, limit: int, label: str) -> None:
    if len(text.encode("utf-8")) > limit:
        _budget(code, f"{label} exceeds {limit} bytes")


def parse_latex_table_grid(
    table_source: str,
    *,
    macro_definitions: Sequence[MacroDefinition] = (),
    table_start: int = 0,
) -> TableGridV1:
    """Parse one source-delimited table occurrence into ``TableGridV1``."""

    if not isinstance(table_source, str):
        raise TypeError("table_source must be a string")
    parts = _tabular_parts(table_source)
    expansion_budget = _ExpansionBudget()
    expanded_spec = expand_table_macros(
        parts.column_spec,
        macro_definitions,
        table_start=table_start + parts.column_spec_start,
        budget=expansion_budget,
    )
    expanded_body = _expand_fragment_in_source_order(
        parts.body,
        macro_definitions,
        fragment_start=table_start + parts.body_start,
        budget=expansion_budget,
    )
    _enforce_text_budget(
        expanded_spec + expanded_body,
        code="expanded_table_budget_exceeded",
        limit=MAX_EXPANDED_BYTES,
        label="expanded table source",
    )
    column_count = _column_count(expanded_spec)
    raw_rows = [
        stripped
        for row in _split_grid(expanded_body, rows=True)
        if (stripped := _strip_rules(row))
    ]
    if len(raw_rows) < 2:
        _fail(
            "empty_table_grid",
            "table requires one header row and at least one data row",
        )
    header_cells = _row_cells(raw_rows[0], column_count=column_count)
    headers = [""] * column_count
    for column_index, column_span, text in header_cells:
        _enforce_text_budget(
            text,
            code="cell_text_budget_exceeded",
            limit=MAX_CELL_BYTES,
            label="header cell text",
        )
        for physical_column in range(column_index, column_index + column_span):
            headers[physical_column] = text

    data_rows = raw_rows[1:]
    if len(data_rows) > MAX_ROWS:
        _budget("row_budget_exceeded", f"table exceeds {MAX_ROWS} data rows")
    rows: list[dict[str, object]] = []
    anchor_count = 0
    for row_index, raw_row in enumerate(data_rows):
        parsed_cells = _row_cells(raw_row, column_count=column_count)
        anchor_count += len(parsed_cells)
        if anchor_count > MAX_ANCHOR_CELLS:
            _budget(
                "anchor_cell_budget_exceeded",
                f"table exceeds {MAX_ANCHOR_CELLS} anchor cells",
            )
        cells: list[dict[str, object]] = []
        for column_index, column_span, text in parsed_cells:
            _enforce_text_budget(
                text,
                code="cell_text_budget_exceeded",
                limit=MAX_CELL_BYTES,
                label="cell text",
            )
            cells.append(
                {
                    "column_index": column_index,
                    "column_header": normalize_table_text(
                        " / ".join(headers[column_index : column_index + column_span]),
                        field="column header",
                    ),
                    "text": text,
                    "row_span": 1,
                    "column_span": column_span,
                }
            )
        rows.append(
            {
                "row_index": row_index,
                "row_label": cells[0]["text"],
                "cells": cells,
            }
        )
    try:
        grid = TableGridV1.from_mapping(
            {
                "schema_version": 1,
                "column_count": column_count,
                "col_headers": headers,
                "row_label_column_index": 0,
                "rows": rows,
            }
        )
    except TableGridError as exc:
        raise LatexTableGridError("invalid_table_grid", str(exc)) from exc
    _enforce_text_budget(
        grid.canonical_bytes().decode("utf-8"),
        code="canonical_grid_budget_exceeded",
        limit=MAX_GRID_BYTES,
        label="canonical table grid",
    )
    _enforce_text_budget(
        grid.searchable_text(),
        code="searchable_text_budget_exceeded",
        limit=MAX_SEARCHABLE_TEXT_BYTES,
        label="searchable table text",
    )
    return grid


__all__ = [
    "LatexTableGridError",
    "expand_table_macros",
    "parse_latex_table_grid",
]
