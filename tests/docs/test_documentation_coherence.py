"""Current docs have one owner and no maintainer-facing residue."""

from scripts import check_docs_site as checker


def test_current_pages_have_unique_identity_and_reader_facing_text() -> None:
    errors: list[str] = []
    checker._check_front_matter(errors)
    assert errors == []


def test_multi_step_instructions_are_not_copied_between_pages() -> None:
    errors: list[str] = []
    checker._check_duplicate_instructions(errors)
    assert errors == []


def test_docs_site_contract_passes_as_one_check() -> None:
    assert checker.check() == []

