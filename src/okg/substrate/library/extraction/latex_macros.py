# SPDX-License-Identifier: MIT
"""Bounded, single-document LaTeX macro-definition scanning.

Adapted from archi-physics/cooper ``latex_macros.py``, MIT licensed:
Git blob 4a6f17bfd5b33ca4a3a39a6b8b0876092ca4d740. The retained license
notice is in ``NOTICE.archi-physics-cooper``.

This module does not walk directories, read files, or expand macros.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MacroDefinition:
    name: str
    body: str
    form: str
    line: int
    span: tuple[int, int]
    arity: int = 0
    optional_default: str | None = None
    parameter_text: str = ""


_NEWCOMMAND_HEAD_RE = re.compile(
    r"\\(?P<form>newcommand|DeclareRobustCommand|renewcommand|providecommand)"
    r"\*?\s*(?:\{\s*\\(?P<braced_name>[A-Za-z@]+)\s*\}"
    r"|\\(?P<bare_name>[A-Za-z@]+))"
    r"\s*(?:\[\s*(?P<arity>\d+)\s*\])?"
    r"\s*(?P<optional_default>\[[^\]\n]*\])?\s*\{"
)
_DEF_HEAD_RE = re.compile(
    r"\\def\s*\\(?P<def_name>[A-Za-z@]+)(?P<parameters>[^{\n]*?)\{"
)


def _is_escaped(text: str, position: int) -> bool:
    slashes = 0
    index = position - 1
    while index >= 0 and text[index] == "\\":
        slashes += 1
        index -= 1
    return slashes % 2 == 1


def extract_balanced_group(text: str, open_pos: int) -> tuple[str, int] | None:
    """Return ``(body, exclusive_end)`` for a balanced group at ``open_pos``."""
    if open_pos < 0 or open_pos >= len(text) or text[open_pos] != "{":
        raise ValueError("open_pos must point at an opening brace")
    depth = 1
    index = open_pos + 1
    start = index
    while index < len(text):
        char = text[index]
        if char == "\\" and index + 1 < len(text):
            index += 2
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start:index], index + 1
        index += 1
    return None


def _line_of(text: str, position: int) -> int:
    return text.count("\n", 0, position) + 1


def scan_macro_definitions(
    text: str,
    *,
    scan_text: str | None = None,
) -> tuple[list[MacroDefinition], list[str], list[tuple[int, int]]]:
    """Scan local definitions without expansion or filesystem access.

    ``scan_text`` may be a same-length comment/verbatim-masked view. The returned
    invalid spans bound malformed heads so paragraph fill does not silently treat
    them as prose.
    """
    scan = text if scan_text is None else scan_text
    if len(scan) != len(text):
        raise ValueError("scan_text must preserve raw-source length")
    found: list[MacroDefinition] = []
    warnings: list[str] = []
    invalid: list[tuple[int, int]] = []
    heads: list[tuple[int, re.Match[str], str]] = []
    heads.extend(
        (match.start(), match, match.group("form"))
        for match in _NEWCOMMAND_HEAD_RE.finditer(scan)
        if not _is_escaped(scan, match.start())
    )
    heads.extend(
        (match.start(), match, "def")
        for match in _DEF_HEAD_RE.finditer(scan)
        if not _is_escaped(scan, match.start())
    )
    for _, match, form in sorted(heads, key=lambda item: item[0]):
        open_brace = match.end() - 1
        balanced = extract_balanced_group(scan, open_brace)
        if balanced is None:
            warnings.append(f"latex_unbalanced_macro_definition:{match.start()}")
            line_end = text.find("\n\n", match.end())
            invalid.append((match.start(), len(text) if line_end < 0 else line_end))
            continue
        _, end = balanced
        raw_body = text[open_brace + 1 : end - 1]
        name = (
            match.groupdict().get("braced_name")
            or match.groupdict().get("bare_name")
            or match.groupdict().get("def_name")
        )
        assert name is not None
        arity_text = match.groupdict().get("arity")
        optional_default = match.groupdict().get("optional_default")
        parameter_text = match.groupdict().get("parameters") or ""
        found.append(
            MacroDefinition(
                name=name,
                body=raw_body.strip(),
                form=form,
                line=_line_of(text, match.start()),
                span=(match.start(), end),
                arity=int(arity_text) if arity_text is not None else 0,
                optional_default=(
                    optional_default[1:-1] if optional_default is not None else None
                ),
                parameter_text=parameter_text.strip(),
            )
        )
    return found, warnings, invalid


def parse_macro_definitions(text: str) -> list[MacroDefinition]:
    """Compatibility convenience returning balanced definitions only."""
    definitions, _, _ = scan_macro_definitions(text)
    return definitions


__all__ = [
    "MacroDefinition",
    "extract_balanced_group",
    "parse_macro_definitions",
    "scan_macro_definitions",
]
