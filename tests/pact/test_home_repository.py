"""The code-proof repository comes from package metadata, and fails loudly.

A code-proof receipt must name the repository the package is built from. That
name is read from ``[project.urls] repository`` in the installed metadata, not
from a literal, so a mirror that publishes under another repository changes
one line. These tests pin the parse, the agreement with ``pyproject.toml``,
and every refusal: no guessed name is ever returned.
"""

from __future__ import annotations

from importlib import metadata
from pathlib import Path
import tomllib

import pytest
from pydantic import ValidationError

from okg.substrate.pact import home_repository as home
from okg.substrate.pact.models import PactCodeProofReceipt

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _fresh_cache():
    home.home_repository.cache_clear()
    yield
    home.home_repository.cache_clear()


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://github.com/example-org/example-repo", "example-org/example-repo"),
        ("https://github.com/example-org/example-repo.git", "example-org/example-repo"),
        ("https://github.com/example-org/example.repo/", "example-org/example.repo"),
        ("  https://github.com/a/b  ", "a/b"),
    ],
)
def test_parse_accepts_a_github_repository_url(url: str, expected: str) -> None:
    assert home.parse_repository_url(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "",
        "git@github.com:example-org/example-repo.git",
        "https://gitlab.com/example-org/example-repo",
        "https://github.com/example-org",
        "https://github.com/example-org/example-repo/tree/main",
        "http://github.com/example-org/example-repo",
    ],
)
def test_parse_refuses_anything_else(url: str) -> None:
    with pytest.raises(home.HomeRepositoryUnavailable):
        home.parse_repository_url(url)


def test_installed_metadata_agrees_with_pyproject() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    url = pyproject["project"]["urls"]["repository"]
    assert home.home_repository() == home.parse_repository_url(url)


class _FakeMetadata:
    def __init__(self, entries: list[str] | None) -> None:
        self._entries = entries

    def get_all(self, key: str):
        assert key == "Project-URL"
        return self._entries


def test_the_repository_entry_is_read_by_label(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        home.metadata,
        "metadata",
        lambda name: _FakeMetadata(
            [
                "Documentation, https://example.test/docs",
                "Repository, https://github.com/example-org/example-repo",
            ]
        ),
    )
    assert home.home_repository() == "example-org/example-repo"


@pytest.mark.parametrize(
    "entries",
    [None, [], ["Documentation, https://example.test/docs"]],
)
def test_missing_repository_entry_raises(
    monkeypatch: pytest.MonkeyPatch, entries: list[str] | None
) -> None:
    monkeypatch.setattr(home.metadata, "metadata", lambda name: _FakeMetadata(entries))
    with pytest.raises(home.HomeRepositoryUnavailable, match="no 'repository' project url"):
        home.home_repository()


def test_uninstalled_package_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def _missing(name: str):
        raise metadata.PackageNotFoundError(name)

    monkeypatch.setattr(home.metadata, "metadata", _missing)
    with pytest.raises(home.HomeRepositoryUnavailable, match="is not installed"):
        home.home_repository()


def _receipt(repository: str) -> PactCodeProofReceipt:
    return PactCodeProofReceipt(
        schema="okg.pact_code_proof_receipt/v1",
        repository=repository,
        base_sha="a" * 40,
        base_tree="b" * 40,
        proven_code_sha="c" * 40,
        proven_code_tree="d" * 40,
        pact_intent_digest="sha256:" + "0" * 64,
        code_digest="sha256:" + "1" * 64,
        receipt_digest="sha256:" + "2" * 64,
    )


def test_receipt_accepts_only_the_home_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        home.metadata,
        "metadata",
        lambda name: _FakeMetadata(["repository, https://github.com/example-org/example-repo"]),
    )
    assert _receipt("example-org/example-repo").repository == "example-org/example-repo"
    with pytest.raises(ValidationError, match="code proof repository must be example-org/example-repo"):
        _receipt("example-org/other-repo")


@pytest.mark.parametrize(
    "entries",
    [None, ["Documentation, https://example.test/docs"], ["repository, https://gitlab.example.test/a/b"]],
)
def test_receipt_check_fails_loudly_without_the_repository_url(
    monkeypatch: pytest.MonkeyPatch, entries: list[str] | None
) -> None:
    """A receipt check with no usable [project.urls] repository raises; it never
    falls back to a default name or turns into an ordinary validation refusal."""
    monkeypatch.setattr(home.metadata, "metadata", lambda name: _FakeMetadata(entries))
    with pytest.raises(home.HomeRepositoryUnavailable):
        _receipt("example-org/example-repo")


def test_receipt_builder_fails_loudly_without_the_repository_url(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Building a receipt reads the home repository before touching git, so a
    missing entry stops the build instead of writing a receipt for a guessed name."""
    from okg.substrate.pact.proof_identity import build_code_proof_receipt

    monkeypatch.setattr(home.metadata, "metadata", lambda name: _FakeMetadata([]))
    with pytest.raises(home.HomeRepositoryUnavailable, match="no 'repository' project url"):
        build_code_proof_receipt(
            tmp_path,
            None,  # never read: the repository check runs first
            base_ref="HEAD",
            proven_code_ref="HEAD",
            closure_allowlist=(),
        )
