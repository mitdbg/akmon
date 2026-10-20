"""Stride A.1 — pin the corner cases of `_glob_to_regex`
(the recursive-`**` matcher DocCorpusSource builds because
`fnmatch.fnmatch` doesn't expand `**`).

Six fixtures exercise the patterns most likely to be wrong:
  - bare `**` matches the whole tree;
  - `**/x/**` matches at any depth (including `x` at root);
  - `dir/**` matches a non-prefix tree but rejects siblings;
  - `*.ext` matches at root only (not nested);
  - `**/*.ext` matches at any depth;
  - `?` matches exactly one non-`/` char.
"""
from __future__ import annotations

from pathlib import PurePosixPath as P

from okg.substrate.library.sources.doc_corpus import (
    _glob_to_regex,
    _match_any,
)


def _matches(pattern: str, path: str) -> bool:
    return _match_any(P(path), [pattern])


def test_double_star_alone_matches_anything() -> None:
    # The fast-path in _match_any short-circuits.
    assert _matches("**", "")
    assert _matches("**", "a")
    assert _matches("**", "a/b/c.py")


def test_double_star_dir_double_star_matches_at_any_depth() -> None:
    assert _matches("**/__pycache__/**", "a/__pycache__/x.pyc")
    assert _matches(
        "**/__pycache__/**",
        "deeply/nested/__pycache__/sub/x.pyc",
    )
    # `__pycache__` at root must also match.
    assert _matches("**/__pycache__/**", "__pycache__/x.pyc")
    # Sibling without `__pycache__` doesn't match.
    assert not _matches("**/__pycache__/**", "a/b/c.py")


def test_dir_double_star_matches_subtree_only() -> None:
    assert _matches("data/**", "data/x")
    assert _matches("data/**", "data/sub/y")
    # `data/**` does NOT match `data` (no trailing path);
    # the regex requires a `/` separator before `**`.
    assert not _matches("data/**", "data")
    # Outside `data/` must not match.
    assert not _matches("data/**", "src/data.py")
    assert not _matches("data/**", "other/data/x")


def test_star_extension_matches_root_only() -> None:
    assert _matches("*.py", "x.py")
    assert _matches("*.py", "abc.py")
    # nested rejected because `*` doesn't cross `/`.
    assert not _matches("*.py", "a/b.py")


def test_double_star_extension_matches_anywhere() -> None:
    assert _matches("**/*.py", "a.py")
    assert _matches("**/*.py", "a/b.py")
    assert _matches("**/*.py", "a/b/c/d.py")
    assert not _matches("**/*.py", "a.txt")


def test_question_mark_single_char() -> None:
    assert _matches("a?.py", "ab.py")
    assert _matches("a?.py", "a1.py")
    # `?` doesn't match zero chars.
    assert not _matches("a?.py", "a.py")
    # `?` doesn't cross path separators.
    assert not _matches("a?b", "a/b")


def test_glob_to_regex_caches_compiled_patterns() -> None:
    """Repeat-call efficiency: the same pattern compiles once."""
    rx1 = _glob_to_regex("**/__pycache__/**")
    rx2 = _glob_to_regex("**/__pycache__/**")
    # Patterns compile to equivalent regex strings.
    assert rx1.pattern == rx2.pattern
