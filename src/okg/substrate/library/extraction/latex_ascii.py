# SPDX-License-Identifier: MIT
"""Conservative LaTeX-to-ASCII matching helpers.

Adapted from archi-physics/cooper ``latex_ascii.py``, MIT licensed:
Git blob 5ae03b7b9be2bdbed667eb736b50d300a075647d. The retained license
notice is in ``NOTICE.archi-physics-cooper``.

These helpers transform candidate aliases only. They never replace raw source
text or become the authority for source spans.
"""

from __future__ import annotations

import re
import unicodedata


_LATEX_FRAGMENT_RE = re.compile(
    r"""
    \\abs\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}
    | \\left\|[^|]*\\right\|
    | \\(?:mathrm|text|mathbf|mathcal|mathit|boldsymbol|operatorname)
        \{[^{}]+\}(?:_\{[^{}]+\}|_[A-Za-z0-9]+)?
        (?:\^\{[^{}]+\}|\^[A-Za-z0-9]+)?
    | \\[A-Za-z]+\{[^{}]+\}(?:\{[^{}]+\})?
    | \\[A-Za-z]+
    | [A-Za-z]+(?:_\{[^{}]+\}|_[A-Za-z0-9]+)
        (?:\^\{[^{}]+\}|\^[A-Za-z0-9]+)?
    | [A-Za-z]+\^\{[^{}]+\}
    | abs\([^()]*\)
    """,
    re.VERBOSE,
)
_DISPLAY_WRAPPERS = r"mathrm|text|mathbf|mathcal|mathit|boldsymbol|operatorname"
_WS_RE = re.compile(r"\s+")


def latex_to_ascii(value: str) -> str:
    """Remove conservative LaTeX presentation syntax from an alias candidate."""
    if not value:
        return value
    value = re.sub(r"\\[()\[\]]", "", value).replace("$", "")
    previous = None
    while previous != value:
        previous = value
        value = re.sub(r"[_^]\{([^{}]*)\}", r"\1", value)
    value = re.sub(r"[_^]([A-Za-z0-9]+)", r"\1", value)
    value = re.sub(
        rf"\\(?:{_DISPLAY_WRAPPERS})\{{([^{{}}]*)\}}",
        r"\1",
        value,
    )
    value = re.sub(r"\\abs\{([^{}]*)\}", r"\1", value)
    value = re.sub(r"\\left\|([^|]*)\\right\|", r"\1", value)
    value = re.sub(r"(?i)\babs\(([^()]*)\)", r"\1", value)
    value = re.sub(r"\|([^|\s]+)\|", r"\1", value)
    value = re.sub(r"\\([A-Za-z]+)", r"\1", value)
    value = value.replace("{", "").replace("}", "")
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(char for char in decomposed if not unicodedata.combining(char))


def latex_normalize(value: str) -> str:
    """Normalize a LaTeX-ish alias for case-insensitive comparison."""
    return _WS_RE.sub(" ", latex_to_ascii(value).strip()).casefold()


def find_latex_match_in_window(window: str, name: str) -> tuple[int, int] | None:
    """Return the raw-window span of a LaTeX fragment matching ``name``."""
    if not window or not name:
        return None
    target = latex_normalize(name)
    if not target:
        return None
    for match in _LATEX_FRAGMENT_RE.finditer(window):
        if latex_normalize(match.group(0)) == target:
            return match.start(), match.end()
    return None


__all__ = ["find_latex_match_in_window", "latex_normalize", "latex_to_ascii"]
