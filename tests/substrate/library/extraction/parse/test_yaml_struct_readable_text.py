"""YAML document text holds the characters and lines its author wrote (#2179).

`YamlParser` stores a sorted re-emission of the file as the document text.
It used `yaml.safe_dump` defaults, so a non-ASCII character became a
`\\u2229` escape, a line past 80 columns was folded with `\\` continuation
markers, and a multi-line string became one quoted line full of `\\n`.
Search excerpts and record reads showed those markers instead of the prose.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.library.extraction.parse.dispatch import parser_for
from okg.substrate.library.sources.doc_corpus import DocCorpusSource

LONG = (
    "A set \u2229 defined as a very long line of prose that will certainly "
    "wrap past eighty columns in the dump output"
)
BODY = (
    f'note: "{LONG}"\n'
    'steps: "first line\\nsecond line \u2014 with a dash\\n"\n'
)


def _paragraph_chunks(tmp_path: Path, body: str) -> list[str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "notes.yaml").write_text(body, encoding="utf-8")
    extr = tmp_path / "extr"
    extr.mkdir()
    base = "okg.substrate.library.extraction"
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [
            {"format": "yaml", "class": f"{base}.parse.yaml_struct.YamlParser"},
        ],
        "chunker": [
            {"format": "default", "class": f"{base}.chunk.window.WindowChunker",
             "config": {"max_chars": 1500, "overlap": 200}},
        ],
    }))
    run = DocCorpusSource(
        repo="t", repo_root=repo, non_git_walk=True, extractors_dir=extr,
        include_globs=("**/*",),
    ).run("probe", mode="scope_complete")
    return [
        str(f.attrs["text"]) for f in run.facts
        if getattr(f, "subtype", None) == "document_chunk"
        and f.attrs.get("chunk_kind") == "paragraph"
    ]


def _text(body: str) -> str:
    return parser_for("yaml").parse(body.encode("utf-8")).text


def test_text_keeps_characters_lines_and_line_breaks_as_written() -> None:
    text = _text(BODY)

    assert f"note: {LONG}\n" in text  # one line, real characters
    assert "steps: |\n  first line\n  second line \u2014 with a dash\n" in text
    for marker in ("\\u", "\\n", "\\\n", "\\ "):
        assert marker not in text


@pytest.mark.parametrize(
    "body",
    [
        BODY,
        'trailing: "spaces at the end  \\nnext"\n',  # no literal block possible
        'tab: "a\\tb\\nc"\n',
        "list:\n- \"x\\ny\"\n- plain\nnested:\n  b: 2\n  a: \"\u00e9\"\n",
        '"key\\nwith break": 1\n',
        "empty: ''\nnull_value: null\nnumber_text: '0123'\n",
        # NEL written raw would load as a line break (review r1).
        'k: "a\\Nb\\nc"\n',
        'k: "a\\Nb"\n',
        '"a\\Nk": 1\n"a k": 2\n',
        'k: "a\\Lb\\Pc"\n',
    ],
)
def test_text_still_loads_to_the_same_data(body: str) -> None:
    text = _text(body)

    assert yaml.safe_load(text) == yaml.safe_load(body)
    assert _text(text) == text  # the re-emission is a fixed point


def test_keys_stay_sorted() -> None:
    assert _text("b: 1\na: 2\n").splitlines() == ["a: 2", "b: 1"]


def test_stored_chunks_hold_the_prose(tmp_path: Path) -> None:
    paragraphs = _paragraph_chunks(tmp_path, BODY)
    assert paragraphs
    joined = "\n".join(paragraphs)
    assert LONG in joined
    assert "second line \u2014 with a dash" in joined
    assert "\\u2229" not in joined and "\\n" not in joined
