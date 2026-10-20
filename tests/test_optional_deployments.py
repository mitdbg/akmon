from __future__ import annotations

from pathlib import Path

import pytest

from tests.pytest.optional_deployments import (
    inject_optional_deployment_path,
    mark_optional_deployment_items,
    optional_deployment_available,
    should_ignore_optional_deployment_test,
    skip_if_optional_deployment_missing,
)


def test_absent_optional_deployment_skips_intentionally(tmp_path: Path) -> None:
    repo = tmp_path
    test_file = repo / "tests" / "af_pubs" / "test_example.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_example(): pass\n", encoding="utf-8")

    assert optional_deployment_available(repo, "af_pubs") is False
    assert inject_optional_deployment_path(repo, "af_pubs") is None
    assert should_ignore_optional_deployment_test(
        test_file,
        repo_root=repo,
        deployment="af_pubs",
    )
    with pytest.raises(pytest.skip.Exception) as exc:
        skip_if_optional_deployment_missing(repo, "af_pubs")
    assert "deployments/af_pubs" in str(exc.value)


def test_optional_deployment_marker_is_applied_to_matching_items() -> None:
    class DummyItem:
        def __init__(self, path: str) -> None:
            self.fspath = path
            self.markers = []

        def add_marker(self, marker) -> None:
            self.markers.append(marker)

    matching = DummyItem("/repo/tests/views/test_af_pubs_concept_views.py")
    other = DummyItem("/repo/tests/views/test_backbone.py")
    same_name_outside_root = DummyItem(
        "/repo/tests/af_pubs/test_af_pubs_concept_views.py"
    )

    mark_optional_deployment_items(
        [matching, other, same_name_outside_root],
        deployment="af_pubs",
        test_root=Path("/repo/tests/views"),
        filenames={"test_af_pubs_concept_views.py"},
    )

    assert len(matching.markers) == 1
    assert not other.markers
    assert not same_name_outside_root.markers


def test_optional_deployment_marker_without_filenames_stays_inside_root() -> None:
    class DummyItem:
        def __init__(self, path: str) -> None:
            self.fspath = path
            self.markers = []

        def add_marker(self, marker) -> None:
            self.markers.append(marker)

    matching = DummyItem("/repo/tests/af_pubs/test_records_slug.py")
    same_name_outside_root = DummyItem("/repo/tests/views/test_records_slug.py")

    mark_optional_deployment_items(
        [matching, same_name_outside_root],
        deployment="af_pubs",
        test_root=Path("/repo/tests/af_pubs"),
    )

    assert len(matching.markers) == 1
    assert not same_name_outside_root.markers
