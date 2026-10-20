"""Every retained page has a valid route from current task navigation."""

from scripts import check_docs_site as checker


def test_all_pages_are_reachable_and_all_local_links_resolve() -> None:
    errors: list[str] = []
    checker._check_all_active_links(errors)
    assert errors == []


def test_navigation_uses_current_tasks_and_strict_link_checks() -> None:
    errors: list[str] = []
    checker._check_mkdocs_nav(errors)
    assert errors == []

