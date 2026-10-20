"""Exact consumed-snapshot identity for fixture-backed GitHubSource."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.change_probe import tokens_equal
from okg.substrate.library.sources.github import GitHubSource


def _fixture_bytes(*, title: str) -> bytes:
    return json.dumps([
        {
            "number": 1,
            "title": title,
            "state": "open",
            "user": {"login": "alice"},
            "updated_at": "2026-07-25T00:00:00Z",
        }
    ]).encode()


def test_github_fixture_returns_exact_global_consumed_token(
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "issues.json"
    raw_bytes = _fixture_bytes(title="captured")
    fixture.write_bytes(raw_bytes)
    source = GitHubSource(
        repo="owner/repo",
        fixture_path=fixture,
    )

    run = source.run("run-exact", mode="reconcile")
    facts = list(run.facts)

    assert any(fact.attrs.get("title") == "captured" for fact in facts)
    assert run.completed_scope is True
    assert run.record_set is not None
    assert run.applied_raw_token == source.change_probe.token_from_item_hashes({
        str(fixture): hashlib.sha256(raw_bytes).hexdigest(),
    })
    assert run.applied_token_scope == "global"
    assert run.applied_token_authority == "exact"
    assert source.probe_short_circuit_safe is True


def test_github_fixture_mutation_keeps_facts_and_token_on_captured_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = tmp_path / "issues.json"
    first_bytes = _fixture_bytes(title="first")
    fixture.write_bytes(first_bytes)
    source = GitHubSource(repo="owner/repo", fixture_path=fixture)
    original_read_bytes = Path.read_bytes
    reads = 0

    def _read_then_mutate(path: Path) -> bytes:
        nonlocal reads
        raw_bytes = original_read_bytes(path)
        if path == fixture:
            reads += 1
            fixture.write_bytes(_fixture_bytes(title="replacement"))
        return raw_bytes

    monkeypatch.setattr(Path, "read_bytes", _read_then_mutate)

    run = source.run("run-race", mode="reconcile")
    facts = list(run.facts)

    assert reads == 1
    assert any(fact.attrs.get("title") == "first" for fact in facts)
    assert not tokens_equal(
        run.applied_raw_token,
        source.change_probe.build_token(),
    )


def test_github_fixture_read_error_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = tmp_path / "issues.json"
    fixture.write_bytes(_fixture_bytes(title="unreadable"))
    source = GitHubSource(repo="owner/repo", fixture_path=fixture)

    def _unreadable(_path: Path) -> bytes:
        raise PermissionError("sensitive operating-system detail")

    monkeypatch.setattr(Path, "read_bytes", _unreadable)

    with pytest.raises(SubstrateError, match="could not be read") as exc_info:
        source.run("run-error", mode="reconcile")

    assert "sensitive operating-system detail" not in str(exc_info.value)


def test_github_fixture_invalid_json_never_builds_applied_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = tmp_path / "issues.json"
    fixture.write_bytes(b'{"items": [')
    source = GitHubSource(repo="owner/repo", fixture_path=fixture)

    def _unexpected_token(_hashes) -> dict:
        raise AssertionError("invalid bytes must never receive an applied token")

    monkeypatch.setattr(
        source.change_probe,
        "token_from_item_hashes",
        _unexpected_token,
    )

    with pytest.raises(SubstrateError, match="not valid JSON"):
        source.run("run-invalid", mode="reconcile")


@pytest.mark.parametrize(
    "source",
    [
        GitHubSource(repo="owner/repo"),
        GitHubSource(
            repo="owner/repo",
            fixture_path=Path("issues.json"),
            since="2026-01-01T00:00:00Z",
        ),
    ],
)
def test_github_live_or_filtered_reads_are_not_short_circuit_authority(
    source: GitHubSource,
) -> None:
    assert source.probe_short_circuit_safe is False
