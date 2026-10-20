"""Per-source recovery wizards for local/path sources.

Each wizard prompts the operator for the source's required local
config (path / URL) and returns a dict of params to merge into
`deployments/<name>/source_registry.yaml` under that source's
entry. Wizards do NOT modify the registry themselves; the caller
in `pi.py` handles registry write + scoped publish + delta print.

Academic-researcher connector setup does not use this module for Gmail,
Google Calendar, or Slack; those sources are planned through
connector routes.
"""
from __future__ import annotations

import urllib.error
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from okg.substrate.library.sources._url_safety import (
    UnsafeFetchURL,
    open_validated_url,
)

if TYPE_CHECKING:
    pass


# --- wizard primitives ---


def _prompt(label: str, *, required: bool = True) -> str:
    """Read a single-line input from the operator."""
    while True:
        value = input(f"{label}: ").strip()
        if value or not required:
            return value
        print("  (required — please enter a value)")


def _prompt_path(label: str, *, must_exist: bool = True) -> str:
    """Read a filesystem path, optionally validate existence."""
    while True:
        raw = input(f"{label}: ").strip()
        if not raw:
            print("  (required)")
            continue
        path = Path(raw).expanduser().resolve()
        if must_exist and not path.exists():
            print(f"  path not found: {path}")
            continue
        return str(path)


def _verify_url_reachable(url: str) -> bool:
    """HEAD the URL; return True if reachable, else False. The target is routed
    through the central egress validator (an unsafe/SSRF target counts as not
    reachable); a localhost dev endpoint can be permitted via
    OKG_EGRESS_ALLOW_HOSTS."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        with open_validated_url(req, timeout=10):
            return True
    except (UnsafeFetchURL, urllib.error.URLError, OSError):
        return False


# --- path/URL wizards ---


def _wizard_notes(deployment: str) -> dict:
    """Prompt for the notes directory."""
    del deployment
    notes_dir = _prompt_path("Notes directory path")
    return {"notes_dir": notes_dir}


def _wizard_cv(deployment: str) -> dict:
    """Prompt for the CV file path."""
    del deployment
    cv_path = _prompt_path("CV file path (PDF / DOCX / TXT)")
    return {"cv_path": cv_path}


def _wizard_papers(deployment: str) -> dict:
    """Prompt for the paper PDF inbox directory.

    The key SHALL be `inbox_path` — that's what
    `PaperPdfSource.__init__` accepts (`scholarly/paper_pdf_source.py`),
    and what the source-default YAML's templating uses.
    Pre-fix this returned `paper_pdf_inbox_dir`, which
    silently broke every `okg add papers` since the
    wizard catalog shipped (caught by `tighten-setup-feedback-loop`'s
    signature-agreement test).
    """
    del deployment
    papers_dir = _prompt_path("Paper PDF inbox directory")
    return {"inbox_path": papers_dir}


def _wizard_apple_cal(deployment: str) -> dict:
    """Apple Calendar uses the local sqlite DB; no prompt needed."""
    del deployment
    return {}


def _wizard_cal_ics(deployment: str) -> dict:
    """Prompt for an ICS feed URL + verify reachability.

    The key SHALL be `calendar_ics_url` — that's what
    `CalendarIcsSource.__init__` accepts, and what the source-
    default YAML's `params:` uses
    (`calendar_ics_url: ${calendar_ics}`). Pre-fix this returned
    `ics_url`, which `add_source` would have rejected (caught by
    `tighten-setup-feedback-loop`'s signature-agreement test).
    """
    del deployment
    while True:
        url = _prompt("ICS feed URL (https://...)")
        if _verify_url_reachable(url):
            return {"calendar_ics_url": url}
        print(
            "  URL didn't respond. Confirm it's correct or "
            "press Ctrl-C to abort."
        )
        if _prompt(
            "Use anyway? (yes/no)", required=False,
        ).lower().startswith("y"):
            return {"calendar_ics_url": url}


def _wizard_canvas(deployment: str) -> dict:
    """Prompt for the Canvas base URL (add-academic-corpora-sources).

    The key SHALL be `canvas_base_url` — what
    `CanvasApiSource.__init__` accepts. The token is NOT prompted:
    it's a self-serve instructor token supplied via the
    OKG_CANVAS_TOKEN env var (Account -> Settings -> New Access
    Token), and the course allowlist (`canvas_courses`) is approved
    in the registry like gmail_labels.
    """
    del deployment
    while True:
        url = _prompt("Canvas base URL (e.g. https://canvas.example.org)")
        if _verify_url_reachable(url):
            return {"canvas_base_url": url}
        print(
            "  URL didn't respond. Confirm it's correct or "
            "press Ctrl-C to abort."
        )
        if _prompt(
            "Use anyway? (yes/no)", required=False,
        ).lower().startswith("y"):
            return {"canvas_base_url": url}


def _wizard_group_page(deployment: str) -> dict:
    """Prompt for the group webpage URL + verify reachability.

    The key SHALL be `group_webpage` — that's what
    `GroupWebpageScrapeSource.__init__` accepts, and what the
    source-default YAML's `params:` uses. Pre-fix this returned
    `group_webpage_url`, which `add_source` would have rejected.
    """
    del deployment
    while True:
        url = _prompt("Group webpage URL")
        if _verify_url_reachable(url):
            return {"group_webpage": url}
        print(
            "  URL didn't respond. Confirm it's correct or "
            "press Ctrl-C to abort."
        )
        if _prompt(
            "Use anyway? (yes/no)", required=False,
        ).lower().startswith("y"):
            return {"group_webpage": url}


def _wizard_lab_talks(deployment: str) -> dict:
    """Prompt for the lab talks page URL.

    The key SHALL be `talks_url` — that's what
    `LabWebpageTalksSource.__init__` accepts, and what the
    source-default YAML's `params:` uses
    (`talks_url: ${lab_talks_url}` — the install-answer variable
    is `lab_talks_url`, the source param is `talks_url`). Pre-fix
    this returned `lab_talks_url`, which `add_source` would have
    rejected.
    """
    del deployment
    while True:
        url = _prompt("Lab talks page URL")
        if _verify_url_reachable(url):
            return {"talks_url": url}
        print(
            "  URL didn't respond. Confirm it's correct or "
            "press Ctrl-C to abort."
        )
        if _prompt(
            "Use anyway? (yes/no)", required=False,
        ).lower().startswith("y"):
            return {"talks_url": url}


# --- no-op wizard for filesystem-templated sources ---


def _wizard_no_params(deployment: str) -> dict:
    """Shared wizard for sources whose only config is "flip
    enabled_by_default" — params already template from install
    answers (`${papers_dir}`, `${pi_name}`, etc.). No operator
    prompt is needed.

    Per `unify-source-admin-catalog`: source-defaults YAMLs like
    `talks.yaml`, `bib_references.yaml`, `mit_course_catalog.yaml`,
    `author_aliases.yaml`, `concept_aliases.yaml`, and
    `external_repos.yaml` derive their config entirely from
    install-time templating. `okg add <short>` for those flips
    `enabled_by_default: true` + runs a scoped ingest; if the
    operator never set the relevant install answer (e.g.
    `papers_dir`), the source's own preflight soft-fails."""
    del deployment
    return {}


# --- catalog --


# `WIZARDS[<short-name>] -> (source-id-in-registry, kind, wizard-fn)`.
# The short-name is the stable operator-facing source label. The
# source-id is the key in source-defaults YAML. The kind drives
# the state-machine envelope in the MCP source-admin path
# (`connector | param | filesystem | llm`) — one canonical catalog
# instead of the previous parallel hand-written copy in
# `okg.substrate.mcp.source_admin`. Per
# `unify-source-admin-catalog`.
WIZARDS: dict[str, tuple[str, str, Callable[[str], dict]]] = {
    # Public identity sources. These derive params from install
    # answers (`pi_name`, `orcid`, `dblp_pid`) and are now wired
    # by the approval wizard rather than pre-ingested by install.
    "publications":    ("dblp_by_orcid",       "filesystem", _wizard_no_params),
    # DBLP venue scraper (rate-limit-heavy; enabled_by_default: false
    # in source-defaults). Cataloged so `okg add dblp-venues` is the
    # enable path instead of hand-editing source_registry.yaml —
    # gap found by the source-health view (source-health-matrix).
    "dblp-venues":     ("dblp",                "filesystem", _wizard_no_params),
    # ORCID's own works+employments API — the day-1 publication
    # source (academic-pi-source-tail). Distinct from
    # `publications`/dblp_by_orcid; the only source emitting the
    # PI's institutional affiliated_with edges. Params template
    # from the `${orcid}` install answer.
    "orcid-publications": ("orcid_publications", "filesystem", _wizard_no_params),
    "nsf-awards":      ("nsf_award_search",    "filesystem", _wizard_no_params),
    "doe-osti":        ("doe_osti",            "filesystem", _wizard_no_params),
    "gmail":           ("gmail_api",            "connector",  _wizard_no_params),
    "google-cal":      ("google_calendar_api",  "connector",  _wizard_no_params),
    "slack":           ("slack_api",            "connector",  _wizard_no_params),
    # Microsoft Graph pair (add-msgraph-sources): one tenant-internal
    # Entra app (the shared `msgraph` OAuth provider) serves both.
    "outlook-mail":    ("outlook_mail_api",     "connector",  _wizard_no_params),
    "outlook-cal":     ("outlook_calendar_api", "connector",  _wizard_no_params),
    "apple-cal":       ("apple_calendar_local", "filesystem", _wizard_apple_cal),
    "cal-ics":         ("calendar_ics",         "param",      _wizard_cal_ics),
    # Canvas LMS (add-academic-corpora-sources): the one teaching
    # surface with professor-self-serve API tokens.
    "canvas":          ("canvas_api",           "param",      _wizard_canvas),
    "notes":           ("notes_dir",            "param",      _wizard_notes),
    "cv":              ("cv_ingest_llm",        "llm",        _wizard_cv),
    "papers":          ("paper_pdf_inbox",      "param",      _wizard_papers),
    "group-page":      ("group_webpage_scrape", "param",      _wizard_group_page),
    "lab-talks":       ("lab_webpage_talks",    "param",      _wizard_lab_talks),
    # Filesystem-templated sources newly cataloged in
    # `unify-source-admin-catalog`. The wizard fn is a shared
    # no-op; install-time templating fills the params.
    "talks":           ("talks",                "filesystem", _wizard_no_params),
    "bib-references":  ("bib_references",       "filesystem", _wizard_no_params),
    "latex-project-git-files": (
        "latex_project_git_files", "filesystem", _wizard_no_params,
    ),
    "latex-project-doc-corpus": (
        "latex_project_doc_corpus", "filesystem", _wizard_no_params,
    ),
    "latex-project":   ("latex_project",         "filesystem", _wizard_no_params),
    "mit-courses":     ("mit_course_catalog",   "filesystem", _wizard_no_params),
    "author-aliases":  ("author_aliases",       "filesystem", _wizard_no_params),
    "concept-aliases": ("concept_aliases",      "filesystem", _wizard_no_params),
    "identity-aliases": ("identity_aliases",     "filesystem", _wizard_no_params),
    "external-repos":  ("external_repos",       "filesystem", _wizard_no_params),
    # Broad Workspace Graph sources. Default setup skips these until
    # the operator explicitly names a project repo.
    "git-files":       ("git_files",            "filesystem", _wizard_no_params),
    "git-history":     ("git_history",          "filesystem", _wizard_no_params),
    "code-structure":  ("code_structure",       "filesystem", _wizard_no_params),
    "openspec":        ("openspec",             "filesystem", _wizard_no_params),
    "agent-sessions":  ("agent_sessions",       "filesystem", _wizard_no_params),
}


# Allowlist of kind values. Source-admin's envelope decision
# tree branches on these; future kinds must be added here +
# wired into `source_admin.add_source` together.
VALID_KINDS: frozenset[str] = frozenset({
    "connector", "param", "filesystem", "llm",
})


def supported_short_names() -> list[str]:
    """Return every catalog short name."""
    return sorted(WIZARDS.keys())


def supported_recovery_short_names() -> list[str]:
    """Return short names accepted by the low-level recovery add
    command."""
    return sorted(
        short
        for short, (_source_id, kind, _fn) in WIZARDS.items()
        if kind != "connector"
    )
