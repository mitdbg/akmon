"""Source-admin catalog: maps short-names → (source_id, kind).

Extracted from ``mcp/source_admin.py`` by ``extract-mcp-source-
catalog`` (2026-05) per the MCP audit's "smallest first PACT"
recommendation. Pure data + one loader; no @mcp.tool registration.

The single source of truth for the catalog is
``wizards.WIZARDS`` from the academic-researcher profile. This module
projects that dict to ``SOURCE_CATALOG: dict[short_name,
(source_id, kind)]`` plus four membership-frozensets used by the
source-admin tools to dispatch on source class.

``source_admin`` re-exports ``SOURCE_CATALOG`` (and the other
constants) as module attributes so any existing
``source_admin.SOURCE_CATALOG`` access keeps working.
"""
from __future__ import annotations

import logging



_log = logging.getLogger(__name__)


def _load_catalog_from_wizards() -> dict[str, tuple[str, str]]:
    """Derive the source-admin catalog from ``wizards.WIZARDS`` so
    the two surfaces never diverge.

    Per ``unify-source-admin-catalog``: pre-fix, this module had a
    hand-written ``SOURCE_CATALOG`` that overlapped with
    ``wizards.WIZARDS`` for 10 short-names but missed 6 (``talks``,
    ``bib-references``, ``mit-courses``, ``author-aliases``,
    ``concept-aliases``, ``external-repos``) so the conversational
    setup agent returned ``unknown_source`` for them. Now: one
    catalog. The wizard tuple is ``(source_id, kind, wizard_fn)``;
    we project to ``(source_id, kind)`` for this module.

    Goes via ``get_profile_cli`` because the academic-researcher profile
    dir is hyphenated (``academic-researcher``) and not a valid Python
    module name — the profile-CLI loader uses
    ``importlib.util.spec_from_file_location`` to load it.

    Failure modes split two ways (post-Wave-D MCP audit):
      - The profile is not shipped (a minimal deployment may
        omit it). ``get_profile_cli`` raises ``ProfileCliError``;
        ``SOURCE_CATALOG`` legitimately empties and source-admin
        tools return ``unknown_source``. Log at info level.
      - The profile IS shipped but failed to load (syntax error,
        attribute error, transitive import failure). That's a
        broken install, not a graceful degradation — let the
        exception propagate so the server-build fails loud
        instead of silently disabling 9 MCP tools.
    """
    from okg.substrate.cli.profile_cli_loader import (
        get_profile_cli,
        ProfileCliError,
    )
    try:
        pkg = get_profile_cli("academic-researcher")
    except ProfileCliError as e:
        _log.info(
            "source_admin: academic-researcher profile not shipped; "
            "SOURCE_CATALOG will be empty. Cause: %s", e,
        )
        return {}
    wizards = getattr(pkg, "wizards", None)
    if wizards is None or not hasattr(wizards, "WIZARDS"):
        return {}
    return {
        short: (sid, kind)
        for short, (sid, kind, _fn) in wizards.WIZARDS.items()
    }


# Short-name → (registry source_id, kind) mapping. Kind drives the
# state-machine envelope:
#   - connector: needs a trusted connector route + allowlists.
#   - param: needs operator-provided config (path / URL / labels).
#   - filesystem: zero-config beyond install-time templating.
#   - llm: needs an OpenAI/Anthropic API key.
# Derived from `wizards.WIZARDS` at module import time; a unit
# test asserts the two stay in sync.
SOURCE_CATALOG: dict[str, tuple[str, str]] = _load_catalog_from_wizards()


_LOCAL_PROJECT_SOURCE_IDS = frozenset({
    "git_files",
    "git_history",
    "code_structure",
    "openspec",
    "agent_sessions",
})

_CREDENTIAL_SOURCE_IDS = frozenset({
    "gmail_api",
    "google_calendar_api",
    "slack_api",
    # Microsoft Graph pair (add-msgraph-sources); gap caught by the
    # token-redaction doctor probe during harden-connector-route-vetting.
    "outlook_mail_api",
    "outlook_calendar_api",
})

_DEEP_INDEXING_SOURCE_IDS = frozenset({
    "git_history",
    "code_structure",
    "agent_sessions",
})

_SOURCE_FAMILY_RANK: dict[str, tuple[int, str]] = {
    # 1. Identity and scholarly public record.
    "dblp_by_orcid": (10, "identity_public_scholarly"),
    "nsf_award_search": (11, "identity_public_scholarly"),
    "doe_osti": (12, "identity_public_scholarly"),
    "group_webpage_scrape": (13, "identity_public_scholarly"),
    # 2. Research library.
    "paper_pdf_inbox": (20, "research_library"),
    "bib_references": (21, "research_library"),
    "author_aliases": (22, "research_library"),
    "concept_aliases": (23, "research_library"),
    "identity_aliases": (24, "research_library"),
    "external_repos": (25, "research_library"),
    "latex_project_git_files": (26, "research_library"),
    "latex_project_doc_corpus": (27, "research_library"),
    "latex_project": (28, "research_library"),
    # 3. Local research corpora.
    "notes_dir": (30, "local_research_corpus"),
    "cv_ingest_llm": (31, "local_research_corpus"),
    "talks": (32, "local_research_corpus"),
    "lab_webpage_talks": (33, "local_research_corpus"),
    "mit_course_catalog": (34, "local_research_corpus"),
    "apple_calendar_local": (35, "local_research_corpus"),
    "calendar_ics": (36, "local_research_corpus"),
    # 4. Selected project repos.
    "git_files": (40, "selected_project_repo"),
    "openspec": (41, "selected_project_repo"),
    # 5. Agent/workflow memory and deep indexing.
    "agent_sessions": (50, "project_agent_memory"),
    "git_history": (51, "deep_indexing"),
    "code_structure": (52, "deep_indexing"),
    # 7. Communications and calendar connectors.
    "gmail_api": (70, "communications_calendar"),
    "google_calendar_api": (71, "communications_calendar"),
    "slack_api": (72, "communications_calendar"),
}


__all__ = [
    "SOURCE_CATALOG",
    "_LOCAL_PROJECT_SOURCE_IDS",
    "_CREDENTIAL_SOURCE_IDS",
    "_DEEP_INDEXING_SOURCE_IDS",
    "_SOURCE_FAMILY_RANK",
    "_load_catalog_from_wizards",
]
