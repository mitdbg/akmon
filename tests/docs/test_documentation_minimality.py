"""The published documentation stays smaller than the pre-cleanup tree."""

from __future__ import annotations

from pathlib import Path
import re
import warnings

import pytest

from scripts import check_docs_site as checker
from tests.mirror_scope import shipped


ROOT = Path(__file__).resolve().parents[2]
# Publication budgets leave room for a genuinely new, linked page without
# allowing the retired monolithic tree to grow back.
MAX_MARKDOWN_PAGES = 105
# Jason, 2026-10-19: warn above 150,000 words and fail above 200,000, so
# docs-adding PRs stop trimming each other to fit a tight cap.
WARN_MARKDOWN_WORDS = 150_000
MAX_MARKDOWN_WORDS = 200_000
WORD_RE = re.compile(r"\b[\w'-]+\b")


class DocumentationWordBudgetWarning(UserWarning):
    """The docs tree is past the warning level but under the hard limit."""


def _check_word_budget(words: int) -> None:
    assert words <= MAX_MARKDOWN_WORDS, (
        f"docs/ has {words:,} words; the hard limit is {MAX_MARKDOWN_WORDS:,}"
    )
    if words > WARN_MARKDOWN_WORDS:
        warnings.warn(
            f"docs/ has {words:,} words, above the {WARN_MARKDOWN_WORDS:,}-word "
            f"warning level (hard limit {MAX_MARKDOWN_WORDS:,})",
            DocumentationWordBudgetWarning,
            stacklevel=2,
        )


def test_documentation_stays_within_the_reduced_budget() -> None:
    pages = sorted((ROOT / "docs").rglob("*.md"))
    words = sum(len(WORD_RE.findall(path.read_text(encoding="utf-8"))) for path in pages)
    assert len(pages) <= MAX_MARKDOWN_PAGES
    _check_word_budget(words)


def test_word_budget_warns_above_warning_level_and_fails_above_hard_limit() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _check_word_budget(WARN_MARKDOWN_WORDS)
    with pytest.warns(DocumentationWordBudgetWarning, match="150,001 words"):
        _check_word_budget(WARN_MARKDOWN_WORDS + 1)
    with pytest.warns(DocumentationWordBudgetWarning):
        _check_word_budget(MAX_MARKDOWN_WORDS)
    with pytest.raises(AssertionError, match="200,001 words"):
        _check_word_budget(MAX_MARKDOWN_WORDS + 1)


def test_historical_bookkeeping_is_not_published() -> None:
    errors: list[str] = []
    checker._check_retired_paths(errors)
    assert errors == []
    for relative in (
        "scripts/audit_cli_surface.py",
    ):
        assert not (ROOT / relative).exists(), relative


def test_no_hand_maintained_corpus_inventory_remains() -> None:
    for path in (ROOT / "docs").rglob("*.md"):
        text = path.read_text(encoding="utf-8")
        assert "docs-site-inventory:start" not in text, path
        assert "Complete Page Inventory" not in text, path


def test_core_explanations_have_focused_size_budgets() -> None:
    limits = {
        "docs/develop/architecture/pipeline.md": 180,
        "docs/reference/mcp.md": 600,
        "docs/develop/ui/operator-contract.md": 180,
        # Four live views state their own contract on this one page; the
        # shell keeps its own 180 in operator-contract.md above.
        "docs/develop/ui/operator-console.md": 215,
        "docs/reference/database-schema.md": 150,
        "docs/operate/deployments/runtime.md": 190,
    }
    for relative, limit in limits.items():
        lines = (ROOT / relative).read_text(encoding="utf-8").splitlines()
        assert len(lines) <= limit, f"{relative} has {len(lines)} lines; limit is {limit}"


def test_private_incident_and_obsolete_proposal_pages_are_not_published() -> None:
    removed = (
        "docs/runbooks/okg-key-rotation.md",
        "docs/architecture/deployment-control-plane.md",
        "docs/architecture/althea-rewrite-safety.md",
        "docs/reference/views-quickstart.md",
        "docs/runbooks/connector-setup-walkthrough.md",
        "docs/interfaces/component-contracts.md",
    )
    for relative in removed:
        assert not (ROOT / relative).exists(), relative

    nav = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    assert "operate/operator-ui.md" in nav
    for relative in removed:
        assert relative.removeprefix("docs/") not in nav


def test_current_docs_do_not_publish_future_connector_types() -> None:
    pages = [
        ROOT / "docs/develop/architecture/overview.md",
        ROOT / "docs/develop/architecture/pipeline.md",
        ROOT / "docs/develop/architecture/overview.md",
        ROOT / "docs/reference/glossary.md",
    ]
    text = "\n".join(path.read_text(encoding="utf-8") for path in pages)
    for retired in (
        "ConnectorDefinition",
        "ProviderConnectionDefinition",
        "SourceDefinition",
        "SourceBinding",
    ):
        assert retired not in text


def test_current_pact_recipe_uses_the_live_subtype_and_identifier() -> None:
    text = (ROOT / "docs/reference/mcp.md").read_text(encoding="utf-8")
    section = text.split("## Change-centered agent trace packet", 1)[1].split(
        "## Queue and DLQ health", 1
    )[0]
    sql = section.split("```sql", 1)[1].split("```", 1)[0]
    assert "subtype = 'pact_change'" in sql
    assert "pact-change:<slug>" in sql
    assert "open_spec_change" not in sql
    assert "change:<slug>" not in sql.replace("pact-change:<slug>", "")


def test_active_references_do_not_turn_into_change_logs() -> None:
    # The okg-workspace page is private; the public copy checks the rest.
    pages = shipped((
        "docs/reference/cli.md",
        "docs/reference/mcp.md",
        "docs/develop/extensions/consistency-api.md",
        "docs/reference/database-schema.md",
        "deployments/okg-workspace/canonical-dev-okg.md",
        "docs/develop/extensions/ontologies.md",
        "docs/operate/deployments/runtime.md",
    ))
    forbidden = (
        "Introduced by `",
        "issue #",
        "adjudication rows",
        "PACT Dogfood Observation",
        "## Migration Strategy",
        "Wave 1",
        "Wave 2",
        "cern-twiki",
    )
    # Any real home directory, not one person's: a reference page names
    # `<home>` or `~`, never `/Users/<name>/` or `/home/<name>/`.
    home_path = re.compile(r"(?:/Users|/home)/[A-Za-z0-9._-]+/")
    for relative in pages:
        text = (ROOT / relative).read_text(encoding="utf-8")
        found = [phrase for phrase in forbidden if phrase in text]
        found += home_path.findall(text)
        assert found == [], f"{relative}: {found}"
