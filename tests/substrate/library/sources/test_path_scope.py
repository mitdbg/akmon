from __future__ import annotations

from pathlib import Path

from okg.substrate.library.sources._path_scope import (
    path_scope_keys,
    path_scope_requested,
    resolve_scoped_paths,
)


def test_path_scope_keys_dedupes_and_normalizes_separators() -> None:
    scope = {
        "scope_keys": [
            "reports\\one.json",
            "reports/one.json",
            "",
            None,
            "reports/two.json",
        ],
    }

    assert path_scope_keys(scope) == (
        "reports/one.json",
        "reports/two.json",
    )
    assert path_scope_requested(scope) is True
    assert path_scope_requested(None) is False


def test_resolve_scoped_paths_accepts_relative_and_absolute_in_root(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    report = repo / "reports" / "one.json"
    report.parent.mkdir(parents=True)
    report.write_text("{}", encoding="utf-8")

    resolved = resolve_scoped_paths(
        {
            "scope_keys": [
                "reports/one.json",
                str(report),
            ],
        },
        roots=[repo],
        suffixes=[".json"],
    )

    assert resolved == (report.resolve(),)


def test_resolve_scoped_paths_rejects_escapes_missing_and_wrong_suffix(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    outside = tmp_path / "secret.json"
    wrong_suffix = repo / "reports" / "skip.txt"
    missing = repo / "reports" / "missing.json"
    wrong_suffix.parent.mkdir(parents=True)
    outside.write_text("{}", encoding="utf-8")
    wrong_suffix.write_text("not json", encoding="utf-8")

    resolved = resolve_scoped_paths(
        {
            "scope_keys": [
                "../secret.json",
                str(outside),
                "reports/skip.txt",
                "reports/missing.json",
            ],
        },
        roots=[repo],
        suffixes=[".json"],
    )

    assert not missing.exists()
    assert resolved == ()


def test_resolve_scoped_paths_checks_multiple_roots(tmp_path: Path) -> None:
    active = tmp_path / "active"
    archive = tmp_path / "archive"
    active_report = active / "2026" / "one.jsonl"
    archive_report = archive / "2026" / "one.jsonl"
    active_report.parent.mkdir(parents=True)
    archive_report.parent.mkdir(parents=True)
    active_report.write_text("{}", encoding="utf-8")
    archive_report.write_text("{}", encoding="utf-8")

    resolved = resolve_scoped_paths(
        {"scope_keys": ["2026/one.jsonl"]},
        roots=[active, archive],
        suffixes=[".jsonl"],
    )

    assert resolved == (
        active_report.resolve(),
        archive_report.resolve(),
    )
