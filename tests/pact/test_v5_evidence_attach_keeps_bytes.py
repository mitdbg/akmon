"""`pact evidence attach` appends one row and leaves the rest of the file alone.

It used to load the manifest and dump the whole model back with
`yaml.safe_dump`. Every `|` block scalar -- `why`, each requirement's
`statement` -- came back as a quoted or folded scalar WITHOUT its final
newline (observed on concord-w2b, commit 8635ca3709d). That changes the parsed
strings, so it changes the intent digest (canonical JSON of the parsed
fields, no whitespace normalization) and any sha256 another change pins over
a statement's text. Attaching proof must never touch reviewed intent.
"""

from __future__ import annotations

import json
from difflib import SequenceMatcher
from pathlib import Path

import pytest
import yaml

from pact.cli import main
from pact.digest import intent_digest
from pact.store import load
from _v5_repo import commit_all, make_repo

MANIFEST_TEXT = """\
pact_version: 5
id: demo
title: Demo
tier: 1
why: |
  First line of the reason.

  A second paragraph, kept exactly as written.
affected_code:
- src/
requirements:
- id: req.demo
  statement: |
    It works, and this statement ends with a newline.
- id: req.other
  statement: |-
    A strip-chomped statement.
tasks:
- id: task.demo
  requirement: req.demo
  verification: pytest passes.
evidence: []
decisions: []
"""

WITH_ROWS = MANIFEST_TEXT.replace(
    "evidence: []\n",
    "evidence:\n"
    "- kind: manual\n"
    "  requirement: req.other\n"
    "  status: pass\n"
    "  note: |\n"
    "    an earlier row\n",
)

NO_EVIDENCE_KEY = MANIFEST_TEXT.replace("evidence: []\n", "")

INDENTED_ROWS = MANIFEST_TEXT.replace(
    "evidence: []\n",
    "evidence:\n"
    "  - kind: manual\n"
    "    requirement: req.other\n"
    "    status: pass\n",
)


def _repo(tmp_path: Path, monkeypatch, text: str) -> Path:
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = make_repo(tmp_path)
    path = root / "pact" / "changes" / "demo" / "pact.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(text, encoding="utf-8")
    commit_all(root, "feat")
    monkeypatch.chdir(root)
    return path


def _attach(capsys) -> dict:
    code = main(["evidence", "attach", "demo", "--kind", "manual", "--requirement", "req.demo", "--note", "checked", "--json"])
    envelope = json.loads(capsys.readouterr().out)
    assert code == 0, envelope
    return envelope


@pytest.mark.parametrize("text", [MANIFEST_TEXT, WITH_ROWS, NO_EVIDENCE_KEY, INDENTED_ROWS], ids=["empty", "rows", "no-key", "indented"])
def test_attach_keeps_reviewed_fields_and_bytes(tmp_path: Path, monkeypatch, capsys, text: str) -> None:
    path = _repo(tmp_path, monkeypatch, text)
    before_raw = yaml.safe_load(text)
    before_digest = intent_digest(load(path))

    envelope = _attach(capsys)

    after_text = path.read_text(encoding="utf-8")
    after_raw = yaml.safe_load(after_text)
    # The parsed reviewed strings are byte-for-byte what the author wrote.
    assert after_raw["why"] == before_raw["why"]
    assert after_raw["why"].endswith("kept exactly as written.\n")
    assert [r["statement"] for r in after_raw["requirements"]] == [
        "It works, and this statement ends with a newline.\n",
        "A strip-chomped statement.",
    ]
    assert intent_digest(load(path)) == before_digest
    # Exactly one row was appended, and it is the row the envelope reports.
    before_rows = before_raw.get("evidence") or []
    assert after_raw["evidence"][: len(before_rows)] == before_rows
    assert after_raw["evidence"][len(before_rows):] == [envelope["row"]]
    # Every other key parses to the same value.
    for key in before_raw.keys() - {"evidence"}:
        assert after_raw[key] == before_raw[key], key
    # And the text outside the evidence list is unchanged: the only edit is
    # one inserted block of lines (an empty `evidence: []` opens to
    # `evidence:` first).
    base = text.replace("evidence: []\n", "evidence:\n")
    ops = [
        op
        for op in SequenceMatcher(a=base.splitlines(keepends=True), b=after_text.splitlines(keepends=True), autojunk=False).get_opcodes()
        if op[0] != "equal"
    ]
    assert [op[0] for op in ops] == ["insert"], ops

def test_attach_twice_appends_in_order(tmp_path: Path, monkeypatch, capsys) -> None:
    path = _repo(tmp_path, monkeypatch, MANIFEST_TEXT)
    first = _attach(capsys)["row"]
    second = _attach(capsys)["row"]
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert raw["evidence"] == [first, second]
    assert raw["decisions"] == []
    assert raw["why"] == yaml.safe_load(MANIFEST_TEXT)["why"]


def test_a_continuation_line_starting_with_hash_stays_inside_its_row(tmp_path: Path, monkeypatch, capsys) -> None:
    """A quoted scalar folded onto a line that starts with `#` (an issue
    number) is part of the row above it, not a comment that ends the list.
    Seen in pact/changes/chat-provider-url-no-credential."""
    text = MANIFEST_TEXT.replace(
        "evidence: []\n",
        "evidence:\n"
        "- kind: manual\n"
        "  requirement: req.other\n"
        "  status: pass\n"
        "  note: 'points at open #2287 item 3, not closed\n"
        "    #1698, without the banned phrase'\n",
    )
    path = _repo(tmp_path, monkeypatch, text)
    row = _attach(capsys)["row"]
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert raw["evidence"] == [*yaml.safe_load(text)["evidence"], row]


def test_an_unsupported_layout_is_refused_and_nothing_is_written(tmp_path: Path, monkeypatch, capsys) -> None:
    text = MANIFEST_TEXT.replace("evidence: []\n", "evidence: [{kind: manual, requirement: req.other, status: pass}]\n")
    path = _repo(tmp_path, monkeypatch, text)
    code = main(["evidence", "attach", "demo", "--kind", "manual", "--requirement", "req.demo", "--json"])
    envelope = json.loads(capsys.readouterr().out)
    assert code == 1
    assert envelope["blocked_by"] == ["evidence_layout_unsupported"]
    assert path.read_text(encoding="utf-8") == text


def _only_inserted(before: str, after: str) -> list[str]:
    """The lines `after` adds to `before`; fails unless that is the only edit."""
    a, b = before.splitlines(keepends=True), after.splitlines(keepends=True)
    ops = [op for op in SequenceMatcher(a=a, b=b, autojunk=False).get_opcodes() if op[0] != "equal"]
    assert [op[0] for op in ops] == ["insert"], ops
    return b[ops[0][3]:ops[0][4]]


def test_crlf_line_endings_are_kept(tmp_path: Path, monkeypatch, capsys) -> None:
    text = WITH_ROWS.replace("\n", "\r\n")
    path = _repo(tmp_path, monkeypatch, "")
    path.write_bytes(text.encode("utf-8"))
    _attach(capsys)
    after = path.read_bytes().decode("utf-8")
    added = _only_inserted(text, after)
    assert all(line.endswith("\r\n") for line in added)
    assert after.count("\n") == after.count("\r\n")


@pytest.mark.parametrize(
    ("text", "kept"),
    [
        (MANIFEST_TEXT.replace("evidence: []\n", "evidence: [] # none yet\n"), "evidence: # none yet\n"),
        (WITH_ROWS.replace("evidence:\n", "evidence:   # receipts go here\n"), "evidence:   # receipts go here\n"),
    ],
    ids=["empty", "rows"],
)
def test_a_comment_on_the_evidence_line_is_kept(tmp_path: Path, monkeypatch, capsys, text: str, kept: str) -> None:
    path = _repo(tmp_path, monkeypatch, text)
    _attach(capsys)
    after = path.read_text(encoding="utf-8")
    assert kept in after
    assert yaml.safe_load(after)["why"] == yaml.safe_load(text)["why"]


def test_a_file_without_a_final_newline_gains_none_mid_file(tmp_path: Path, monkeypatch, capsys) -> None:
    # `evidence` sits before `tasks`, and the file ends without a newline.
    text = MANIFEST_TEXT.replace("evidence: []\n", "").replace("tasks:\n", "evidence: []\ntasks:\n").replace("decisions: []\n", "decisions: []")
    path = _repo(tmp_path, monkeypatch, text)
    _attach(capsys)
    after = path.read_text(encoding="utf-8")
    assert after.endswith("decisions: []")
    _only_inserted(text.replace("evidence: []\n", "evidence:\n"), after)


def test_the_file_mode_is_kept_and_no_temporary_file_is_left(tmp_path: Path, monkeypatch, capsys) -> None:
    path = _repo(tmp_path, monkeypatch, MANIFEST_TEXT)
    path.chmod(0o640)
    _attach(capsys)
    assert path.stat().st_mode & 0o777 == 0o640
    assert sorted(p.name for p in path.parent.iterdir()) == ["pact.yaml"]
