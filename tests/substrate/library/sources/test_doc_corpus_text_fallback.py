"""DocCorpusSource `text_fallback`: unknown-extension and extension-less text
files are read as text (or as Python code when they carry a python shebang)
instead of being skipped; binary files keep the no_format behaviour."""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.library.sources.doc_corpus import (
    _TEXT_SNIFF_BYTES,
    DocCorpusSource,
    _looks_like_text,
    _python_shebang,
)


def _source(tmp_path: Path, **kw) -> DocCorpusSource:
    (tmp_path / "e").mkdir(exist_ok=True)
    return DocCorpusSource(repo="t", repo_root=tmp_path, extractors_dir=tmp_path / "e", **kw)


@pytest.mark.parametrize(
    "name,content,expected",
    [
        ("setSiteCapacity", b"#!/usr/bin/env python3\nimport os\n", "code"),
        ("run.sh", b"#!/bin/bash\necho hi\n", "text"),
        ("Dockerfile", b"FROM python:3.12\n", "text"),
        ("app.js", "const s = 'héllo';\n".encode(), "text"),
        ("logo.bin", b"\x89PNG\r\n\x1a\n\x00\x00\x00", None),
        ("notes.md", b"# title\n", "markdown"),   # extension map still wins
        ("page", b"<!DOCTYPE html><html>", "html"),  # magic bytes still win
    ],
)
def test_text_fallback_resolves_formats(tmp_path, name, content, expected):
    assert _source(tmp_path, text_fallback=True)._format_for(Path(name), content) == expected


def test_without_text_fallback_unknown_files_stay_unresolved(tmp_path):
    src = _source(tmp_path)
    assert src._format_for(Path("setSiteCapacity"), b"#!/usr/bin/env python3\n") is None
    assert src._format_for(Path("run.sh"), b"#!/bin/bash\n") is None


def test_looks_like_text_allows_a_multibyte_character_cut_by_the_sniff_limit():
    e = "é".encode()
    head = b"x" * (_TEXT_SNIFF_BYTES - 1) + e  # the limit cuts 'é' in half
    assert _looks_like_text(head + b" more text")
    assert not _looks_like_text(b"x" * 10 + e[:1])  # cut at the real end: not UTF-8
    assert not _looks_like_text(b"\xff\xfe\x00a")
    assert not _looks_like_text(b"")


def test_looks_like_text_checks_nul_beyond_the_sniffed_head():
    assert _looks_like_text(b"a" * (_TEXT_SNIFF_BYTES + 100))
    assert not _looks_like_text(b"a" * (_TEXT_SNIFF_BYTES + 100) + b"\x00")


@pytest.mark.parametrize(
    "line",
    [
        b"#!/usr/bin/python\n",
        b"#!/usr/bin/env python3.12\n",
        b"#! /usr/bin/python2.7 -u\n",
        b"#!/usr/bin/env -S python3 -u\n",
        b"#!/usr/bin/env python3\r\n",
    ],
)
def test_python_shebang_variants(line):
    assert _python_shebang(line + b"import os\n")


@pytest.mark.parametrize(
    "content",
    [
        b"#!/bin/sh\n# python\n",
        b"#!/opt/python3/bin/bash\n",
        b"#!/usr/bin/pythonista\n",
        b"import os\n",
    ],
)
def test_not_a_python_shebang(content):
    assert not _python_shebang(content)
