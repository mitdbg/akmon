"""tests/private_deployments.py: names by role here, none in the public copy."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests import private_deployments
from tests.private_deployments import NAMES_FILE, name, name_params, names


def _root(tmp_path: Path, *, names_file: str | None, public: bool) -> Path:
    (tmp_path / "tests").mkdir()
    if names_file is not None:
        (tmp_path / NAMES_FILE).write_text(names_file, encoding="utf-8")
    if public:
        (tmp_path / "tests" / "mirror_deselect.txt").write_text("", encoding="utf-8")
    return tmp_path


def test_names_are_read_in_the_order_asked(tmp_path: Path) -> None:
    root = _root(tmp_path, names_file="first: one-deployment\nsecond: other-deployment\n", public=False)
    assert names("second", "first", root=root) == ["other-deployment", "one-deployment"]
    assert name("first", root=root) == "one-deployment"


def test_a_missing_names_file_fails_loudly_here(tmp_path: Path) -> None:
    root = _root(tmp_path, names_file=None, public=False)
    with pytest.raises(FileNotFoundError, match="private_deployments.yaml is missing"):
        names("first", root=root)
    with pytest.raises(FileNotFoundError):
        name("first", root=root)


def test_a_missing_role_fails_loudly(tmp_path: Path) -> None:
    root = _root(tmp_path, names_file="first: one-deployment\n", public=False)
    with pytest.raises(KeyError, match="second"):
        names("first", "second", root=root)


@pytest.mark.parametrize("text", ["- a list\n", "first: ''\n", "first: 3\n"])
def test_a_malformed_names_file_is_refused(tmp_path: Path, text: str) -> None:
    root = _root(tmp_path, names_file=text, public=False)
    with pytest.raises(ValueError, match="non-empty name"):
        names("first", root=root)


def test_the_public_copy_has_no_names_and_skips_a_single_lookup(tmp_path: Path) -> None:
    root = _root(tmp_path, names_file=None, public=True)
    assert names("first", "second", root=root) == []
    with pytest.raises(pytest.skip.Exception, match="does not ship"):
        name("first", root=root)


def test_the_public_copy_still_reads_a_names_file_that_is_present(tmp_path: Path) -> None:
    root = _root(tmp_path, names_file="first: one-deployment\n", public=True)
    assert names("first", root=root) == ["one-deployment"]


def test_name_params_are_the_names_here(tmp_path: Path) -> None:
    root = _root(tmp_path, names_file="first: one-deployment\n", public=False)
    assert name_params("first", root=root) == ["one-deployment"]


def test_name_params_skip_with_a_reason_in_the_public_copy(tmp_path: Path) -> None:
    root = _root(tmp_path, names_file=None, public=True)
    (param,) = name_params("first", root=root)
    assert param.id == "private-deployment"
    (mark,) = param.marks
    assert mark.name == "skip" and mark.kwargs["reason"] == private_deployments.SKIP_REASON


def test_this_repository_names_every_role_its_tests_ask_for() -> None:
    if private_deployments.is_public_copy():
        pytest.skip("the public copy has no names file by design")
    assert len(names("retired_prototype", "retired_prototype_check", "team_deployment")) == 3
