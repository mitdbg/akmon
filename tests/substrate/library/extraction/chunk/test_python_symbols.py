"""PythonSymbolChunker: Python source reaches the graph as code, not docstrings."""

from __future__ import annotations

import textwrap

import pytest

from okg.substrate.library.extraction.chunk.python_symbols import PythonSymbolChunker

SOURCE = textwrap.dedent('''\
    #!/usr/bin/env python
    """Module docstring."""
    import os

    LIMIT = 3


    # comment that documents helper()
    def helper(x):
        """Return x doubled."""
        y = x * 2  # body identifier: doubled_value
        return y


    class Poller:
        """Polls things."""

        interval = 10

        def run(self):
            status = "NoTaskRunningSlot"
            return status

        @staticmethod
        def stop():
            return None


    if __name__ == "__main__":
        helper(LIMIT)
    ''')


def _covered(text, chunks):
    cov = bytearray(len(text))
    for c in chunks:
        assert c.text == text[c.span[0]:c.span[1]]
        cov[c.span[0]:c.span[1]] = b"\x01" * (c.span[1] - c.span[0])
    return all(cov[i] or ch.isspace() for i, ch in enumerate(text))


def test_prefers_raw_text_so_parser_docstring_chunks_are_bypassed():
    assert PythonSymbolChunker.prefers_raw_text is True


def test_function_body_is_in_a_chunk_with_its_symbol_and_lines():
    chunks = PythonSymbolChunker().chunk(SOURCE)
    helper = [c for c in chunks if c.extras["symbol"] == "helper"]
    assert len(helper) == 1
    assert "y = x * 2" in helper[0].text
    assert "# comment that documents helper()" in helper[0].text
    assert helper[0].heading_path == ["helper"]
    assert helper[0].chunk_kind == "code_block"


def test_small_class_is_one_chunk_with_method_bodies():
    chunks = PythonSymbolChunker().chunk(SOURCE)
    cls = [c for c in chunks if c.extras["symbol"] == "Poller"]
    assert len(cls) == 1 and '"NoTaskRunningSlot"' in cls[0].text


def test_large_class_splits_into_header_and_one_chunk_per_method():
    chunks = PythonSymbolChunker(max_chars=120, overlap=10).chunk(SOURCE)
    symbols = [c.extras["symbol"] for c in chunks]
    assert "Poller.run" in symbols and "Poller.stop" in symbols
    run = [c for c in chunks if c.extras["symbol"] == "Poller.run"][0]
    assert run.heading_path == ["Poller", "run"]
    assert '"NoTaskRunningSlot"' in run.text
    stop = [c for c in chunks if c.extras["symbol"] == "Poller.stop"][0]
    assert "@staticmethod" in stop.text  # decorators stay with their method


def test_chunks_tile_every_non_blank_character():
    for max_chars in (4000, 200, 60):
        chunks = PythonSymbolChunker(max_chars=max_chars, overlap=max_chars // 10).chunk(SOURCE)
        assert _covered(SOURCE, chunks)
        assert all(len(c.text) <= max_chars for c in chunks)


def test_line_numbers_match_the_source():
    chunks = PythonSymbolChunker().chunk(SOURCE)
    lines = SOURCE.splitlines()
    for c in chunks:
        first = lines[c.extras["start_line"] - 1]
        assert first.strip(), "a chunk never starts on a blank line"
        assert c.text.startswith(first)


def test_module_code_is_kept():
    chunks = PythonSymbolChunker().chunk(SOURCE)
    module = [c for c in chunks if c.extras["symbol"] == ""]
    joined = "".join(c.text for c in module)
    assert "import os" in joined and '__name__ == "__main__"' in joined


def test_unparseable_source_falls_back_to_windows():
    text = "print 'python 2'\n" * 50
    chunks = PythonSymbolChunker(max_chars=200, overlap=20).chunk(text)
    assert chunks and _covered(text, chunks)
    assert all(c.heading_path == [] for c in chunks)


def test_empty_source_gives_no_chunks():
    assert PythonSymbolChunker().chunk("") == []
    assert PythonSymbolChunker().chunk("\n\n  \n") == []


@pytest.mark.parametrize("bad", [dict(max_chars=0), dict(max_chars=10, overlap=10)])
def test_rejects_bad_config(bad):
    with pytest.raises(ValueError):
        PythonSymbolChunker(**bad)


def test_end_lines_match_the_source():
    chunks = PythonSymbolChunker().chunk(SOURCE)
    lines = SOURCE.splitlines()
    for c in chunks:
        last = lines[c.extras["end_line"] - 1]
        assert c.text.rstrip("\n").endswith(last)
    assert chunks[0].extras["start_line"] == 1
    assert chunks[-1].extras["end_line"] == len(lines)


def test_async_and_decorated_functions_are_symbols():
    text = textwrap.dedent('''\
        import functools


        @functools.lru_cache
        def cached(x):
            return x


        async def fetch(url):
            return await get(url)
        ''')
    chunks = {c.extras["symbol"]: c for c in PythonSymbolChunker().chunk(text)}
    assert chunks["cached"].text.lstrip().startswith("@functools.lru_cache")
    assert chunks["cached"].extras["start_line"] == 4  # the decorator, not the blank lines above
    assert chunks["cached"].extras["end_line"] == 6  # the blank lines after go with fetch
    assert "await get(url)" in chunks["fetch"].text


def test_class_that_starts_with_a_method_keeps_its_comment_with_the_method():
    text = textwrap.dedent('''\
        class Reaper(Base,
                     Mixin):
            # removes expired replicas
            def run(self):
                return "reap_marker" + "x" * 80

            # stops the daemon
            def stop(self):
                return None
        ''')
    chunks = PythonSymbolChunker(max_chars=120, overlap=10).chunk(text)
    by_symbol = {c.extras["symbol"]: c for c in chunks}
    assert by_symbol["Reaper"].text == "class Reaper(Base,\n             Mixin):\n"
    assert by_symbol["Reaper.run"].text.lstrip().startswith("# removes expired replicas")
    assert by_symbol["Reaper.stop"].text.lstrip().startswith("# stops the daemon")
    assert _covered(text, chunks)


def test_crlf_source_keeps_line_numbers_and_coverage():
    text = SOURCE.replace("\n", "\r\n")
    chunks = PythonSymbolChunker().chunk(text)
    assert _covered(text, chunks)
    lf = PythonSymbolChunker().chunk(SOURCE)
    assert [c.text for c in chunks] == [c.text.replace("\n", "\r\n") for c in lf]
    assert [c.extras for c in chunks] == [c.extras for c in lf]


@pytest.mark.parametrize(
    "text",
    [
        "x = " + "-" * 200_000 + "1\n",  # CPython: MemoryError "Parser stack overflowed"
        "x = " + "(" * 300 + ")" * 300 + "\n",  # SyntaxError "too many nested parentheses"
    ],
    # Short ids: pytest would otherwise put the whole 200,000-character source
    # in the node id, and scripts/qa.py's live-output redaction spends hours on
    # that one line, stalling the coverage lane (issue #1881).
    ids=["deep-unary-minus", "deep-parentheses"],
)
def test_source_too_deep_to_parse_falls_back_to_windows(text):
    chunks = PythonSymbolChunker(max_chars=4000, overlap=400).chunk(text)
    assert chunks and _covered(text, chunks)
    assert {c.extras["symbol"] for c in chunks} == {""}


def _start_lines(text, **kw):
    return {c.extras["symbol"]: c.extras["start_line"] for c in PythonSymbolChunker(**kw).chunk(text)}


def test_start_line_is_the_def_line_not_the_blank_lines_before_it():
    text = "import os\n\n\ndef a():\n    return 1\n\n\n\ndef b():\n    return 2\n"
    chunks = {c.extras["symbol"]: c for c in PythonSymbolChunker().chunk(text)}
    lines = text.splitlines()
    assert lines[3].startswith("def a") and lines[8].startswith("def b")
    assert chunks["a"].extras["start_line"] == 4
    assert chunks["b"].extras["start_line"] == 9
    assert chunks["b"].text.startswith("def b")
    assert chunks["a"].extras["end_line"] == 5  # unchanged by the fix
    assert chunks[""].extras["start_line"] == 1


def test_start_line_is_the_first_decorator_line():
    text = "import os\n\n\n@one\n@two(\n    1)\ndef f():\n    pass\n"
    chunk = [c for c in PythonSymbolChunker().chunk(text) if c.extras["symbol"] == "f"][0]
    assert chunk.extras["start_line"] == 4
    assert chunk.text.startswith("@one")


def test_first_definition_at_the_top_of_the_file():
    text = "\n\n\ndef top():\n    return 1\n"
    (chunk,) = PythonSymbolChunker().chunk(text)
    assert chunk.extras["start_line"] == 4
    assert chunk.text == "def top():\n    return 1\n"
    assert chunk.span == (3, len(text))


def test_start_line_skips_blank_lines_before_methods_of_a_split_class():
    text = (
        "class K:\n    x = 1\n\n\n    def a(self):\n        return 'a' * 90\n\n\n"
        "    def b(self):\n        return 'b' * 90\n"
    )
    got = _start_lines(text, max_chars=80, overlap=10)
    assert got["K.a"] == 5 and got["K.b"] == 9


def test_every_start_line_is_the_line_its_text_begins_on():
    for max_chars in (4000, 200, 60):
        chunks = PythonSymbolChunker(max_chars=max_chars, overlap=max_chars // 10).chunk(SOURCE)
        for c in chunks:
            line = SOURCE[:c.span[0]].count("\n") + 1
            assert c.extras["start_line"] == line
            assert c.text.strip() and not c.text.startswith(("\n", " \n"))


def test_unparseable_source_starts_at_its_first_non_blank_line():
    (chunk,) = PythonSymbolChunker().chunk("\n\n  def broken(:\n")
    assert chunk.extras["start_line"] == 3


def test_crlf_blank_lines_before_a_def_are_skipped():
    text = "import os\r\n\r\n\r\ndef a():\r\n    return 1\r\n"
    chunk = [c for c in PythonSymbolChunker().chunk(text) if c.extras["symbol"] == "a"][0]
    assert chunk.extras["start_line"] == 4
    assert chunk.text == "def a():\r\n    return 1\r\n"


def test_a_comment_separated_from_its_def_by_blank_lines_stays_with_it():
    text = "import os\n\n\n# section\n\n\ndef a():\n    return 1\n"
    chunk = [c for c in PythonSymbolChunker().chunk(text) if c.extras["symbol"] == "a"][0]
    assert chunk.text.startswith("# section")
    assert chunk.extras["start_line"] == 4  # the comment, three lines above the def


def test_separator_limit_matches_the_document_corpus_limit():
    from okg.substrate.library.extraction.chunk import python_symbols
    from okg.substrate.library.sources.doc_corpus import MAX_STORED_SEPARATOR_CHARS

    assert python_symbols._MAX_SEPARATOR_CHARS == MAX_STORED_SEPARATOR_CHARS


def test_a_blank_run_at_the_separator_limit_is_skipped():
    limit = 256
    text = "x = 1\n" + "\n" * limit + "def a():\n    return 1\n"
    chunk = [c for c in PythonSymbolChunker().chunk(text) if c.extras["symbol"] == "a"][0]
    assert chunk.text.startswith("def a")
    assert chunk.extras["start_line"] == 2 + limit  # the gap is exactly 256 newlines


def test_a_blank_run_over_the_separator_limit_stays_so_the_file_stays_complete():
    for lead in ("", "x = 1\n"):
        text = lead + "\n" * 300 + "def a():\n    return 1\n"
        chunks = PythonSymbolChunker().chunk(text)
        covered = sum(c.span[1] - c.span[0] for c in chunks)
        assert covered == len(text)  # no gap: the chunks tile the file
        a = [c for c in chunks if c.extras["symbol"] == "a"][0]
        assert a.text.startswith("\n")  # the long run is kept
        assert a.extras["start_line"] == SOURCE_LINE_OF(text, a)


def SOURCE_LINE_OF(text, chunk):
    return text[:chunk.span[0]].count("\n") + 1
