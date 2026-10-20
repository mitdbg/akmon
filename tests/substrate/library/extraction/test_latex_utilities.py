from __future__ import annotations

from okg.substrate.library.extraction.latex_ascii import (
    find_latex_match_in_window,
    latex_normalize,
    latex_to_ascii,
)
from okg.substrate.library.extraction.latex_macros import (
    extract_balanced_group,
    scan_macro_definitions,
)


def test_ascii_helpers_only_normalize_candidate_aliases() -> None:
    assert latex_to_ascii(r"$\mathbf{E}_{total}$") == "Etotal"
    assert latex_normalize(r"\operatorname{Flux} ") == "flux"
    window = r"Compare $\mathbf{E}_{total}$ with the baseline."
    span = find_latex_match_in_window(window, "Etotal")
    assert span is not None
    assert window[slice(*span)] == r"\mathbf{E}_{total}"
    unicode_window = r"Compare \text{Café} with the baseline."
    unicode_span = find_latex_match_in_window(unicode_window, "Cafe")
    assert unicode_span is not None
    assert unicode_window[slice(*unicode_span)] == r"\text{Café}"


def test_balanced_groups_preserve_nested_and_escaped_braces() -> None:
    text = r"{outer {inner} \} literal} tail"
    body, end = extract_balanced_group(text, 0) or ("", 0)
    assert body == r"outer {inner} \} literal"
    assert text[:end] == r"{outer {inner} \} literal}"


def test_macro_scan_is_bounded_and_reports_malformed_definition() -> None:
    text = (
        r"\newcommand{\vect}[1]{\mathbf{#1}}" "\n"
        r"\newcommand{\optional}[2][x]{#1/#2}" "\n"
        r"\newcommand\bare[1]{bare #1}" "\n"
        r"\def\pair#1#2{{#1,#2}}" "\n\n"
        r"\\newcommand{\escaped}{not a definition}" "\n\n"
        r"\newcommand{\broken}{missing" "\n\n"
        r"\input{must-not-be-read.tex}"
    )
    definitions, warnings, invalid = scan_macro_definitions(text)
    assert [(item.name, item.form) for item in definitions] == [
        ("vect", "newcommand"),
        ("optional", "newcommand"),
        ("bare", "newcommand"),
        ("pair", "def"),
    ]
    assert warnings == [
        f"latex_unbalanced_macro_definition:{text.index(r'\newcommand{\broken}')}"
    ]
    assert invalid and text[slice(*invalid[0])].startswith(r"\newcommand{\broken}")
