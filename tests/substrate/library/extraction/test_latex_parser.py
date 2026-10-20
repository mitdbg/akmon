from __future__ import annotations

import pytest

from okg.substrate.library.extraction.parse.dispatch import parser_for
from okg.substrate.library.extraction.parse.latex import (
    LatexControlCharacterError,
    LatexParser,
)


def test_parser_retains_raw_text_chunks_warnings_and_macro_spans() -> None:
    raw = r"""\newcommand{\name}{Value}
\begin{document}
\section{One}

Body.
\end{document}
"""
    parsed = LatexParser().parse(raw.encode())
    assert parsed.text == raw
    assert parsed.metadata["format"] == "latex"
    assert parsed.metadata["parse_warnings"] == []
    start = raw.index(r"\newcommand")
    end = raw.index("\n")
    assert parsed.metadata["macro_definition_spans"] == [[start, end]]
    assert [chunk.text for chunk in parsed.chunks] == ["Body."]


def test_parser_registry_exposes_latex_and_tex() -> None:
    assert isinstance(parser_for("latex"), LatexParser)
    assert isinstance(parser_for("tex"), LatexParser)


def test_parser_rejects_invalid_utf8_instead_of_replacing() -> None:
    with pytest.raises(UnicodeDecodeError):
        LatexParser().parse(b"\\begin{document}\xff\\end{document}")


def test_parser_rejects_storage_unsafe_c0_as_typed_error() -> None:
    with pytest.raises(LatexControlCharacterError, match="offset 16"):
        LatexParser().parse("\\begin{document}\x00\\end{document}")
