"""Shared approved-root path capability (constrain-graph-file-and-parser-ingestion)."""
from __future__ import annotations

from okg.substrate.file_capability import resolve_within_approved_root


def test_in_root_file_is_accepted(tmp_path):
    root = tmp_path / "corpus"
    root.mkdir()
    f = root / "paper.pdf"
    f.write_bytes(b"%PDF-1.4 ...")
    resolved = resolve_within_approved_root(str(f), [str(root)])
    assert resolved == f.resolve()


def test_absolute_out_of_root_is_rejected(tmp_path):
    root = tmp_path / "corpus"
    root.mkdir()
    outside = tmp_path / "secret.pem"
    outside.write_text("KEY")
    assert resolve_within_approved_root(str(outside), [str(root)]) is None


def test_dotdot_escape_is_rejected(tmp_path):
    root = tmp_path / "corpus"
    root.mkdir()
    outside = tmp_path / "secret.pem"
    outside.write_text("KEY")
    traversal = str(root / ".." / "secret.pem")
    assert resolve_within_approved_root(traversal, [str(root)]) is None


def test_symlink_in_root_to_outside_is_rejected(tmp_path):
    root = tmp_path / "corpus"
    root.mkdir()
    outside = tmp_path / "secret.pem"
    outside.write_text("KEY")
    link = root / "link.pdf"
    link.symlink_to(outside)
    # The symlink lives in-root but resolves outside -> denied.
    assert resolve_within_approved_root(str(link), [str(root)]) is None


def test_empty_roots_fails_closed(tmp_path):
    f = tmp_path / "x.pdf"
    f.write_bytes(b"x")
    assert resolve_within_approved_root(str(f), []) is None


def test_blank_or_none_candidate_rejected(tmp_path):
    root = tmp_path / "corpus"
    root.mkdir()
    assert resolve_within_approved_root(None, [str(root)]) is None
    assert resolve_within_approved_root("", [str(root)]) is None


def test_directory_rejected_when_must_be_file(tmp_path):
    root = tmp_path / "corpus"
    sub = root / "sub"
    sub.mkdir(parents=True)
    assert resolve_within_approved_root(str(sub), [str(root)]) is None
    assert resolve_within_approved_root(
        str(sub), [str(root)], must_be_file=False
    ) == sub.resolve()


def test_oversized_file_rejected(tmp_path):
    root = tmp_path / "corpus"
    root.mkdir()
    f = root / "big.pdf"
    f.write_bytes(b"x" * 100)
    assert resolve_within_approved_root(str(f), [str(root)], max_bytes=10) is None
    assert resolve_within_approved_root(str(f), [str(root)], max_bytes=1000) == f.resolve()
