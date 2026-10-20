from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "docs" / "develop" / "ui" / "operator-contract.md"
CONSOLE = ROOT / "docs" / "develop" / "ui" / "operator-console.md"
REMOVED_VIEWS = ("Graph", "Trace", "Performance", "Deployment")


def _contract() -> str:
    return CONTRACT.read_text(encoding="utf-8")


def _console() -> str:
    """The console's own contract page.

    PACT operator-console-live-sources moved the console's views, reads and
    states off the shell's page: the two together outgrew the 180-line budget
    in test_documentation_minimality. PACT console-default-remove-old-views
    then made the console the whole app. The assertions below follow the rows
    to the page that carries them.
    """
    return CONSOLE.read_text(encoding="utf-8")


def _both() -> str:
    return _contract() + "\n" + _console()


def _single_line_contract() -> str:
    return " ".join(_contract().split())


def test_operator_ui_contract_defines_minimal_read_only_surface() -> None:
    text = _contract()
    single_line = _single_line_contract()
    assert "one read-only app" in text
    assert "The app only issues `GET` requests" in text
    assert "There are no write actions" in text
    assert "No view can change records, relationships, or publication state" in text
    assert (
        "no restart, repair, publish, source-sync, or queue mutation controls"
        in single_line
    )


def test_operator_ui_contract_names_the_console_views_and_no_others() -> None:
    """PACT console-default-remove-old-views: the app is the console.

    The old Home and Health pages are deleted, as the four views PACT
    operator-ui-remove-four-views removed before them; that is what this test
    has always guarded and it still does. The contract has to say that every
    URL opens the console and that the old pages' hashes land on its Home.
    """
    text = _contract()
    single_line = _single_line_contract()
    assert "that app is the console: every URL opens it" in single_line
    assert "including `#home` and `#health`" in single_line
    assert "opens the console's Home" in single_line
    # The old pages have no section and no reads table row of their own.
    assert "## Home" not in text
    assert "## Health" not in text
    assert "| Home |" not in text
    assert "| Health |" not in text
    assert "preview" not in _both().lower()
    # The shell's page must route a reader to the console's contract rather
    # than leave them to discover a navigation row the document denies.
    assert "operator-console.md" in text
    console = _console()
    for view in ("Home", "Updates", "Sources", "Explore"):
        assert f"## {view}" in console, f"the console page does not describe {view}"
    both = _both()
    for view in REMOVED_VIEWS:
        assert f"## {view}" not in both, f"{view} still has a section"
    # Search is not a view on this app either: step 1's Search view is not on
    # dev, so neither page must describe one.
    assert "## Search" not in both
    for removed in ("GraphCanvas", "sigma", "graphology", "antd", "Ant Design"):
        assert removed not in both, f"{removed} still named"


def test_operator_ui_contract_pins_backend_behavior_per_view() -> None:
    text = _contract()
    # Still served, read by no view since the old Home and Health pages went.
    # The contract names them so a reader does not take them for dead routes
    # or for reads a view still depends on.
    for needle in (
        "GET /viz/api/health",
        "GET /viz/api/traces",
        "GET /viz/api/workflows",
    ):
        assert needle in text
    assert "no view reads them" in _single_line_contract()
    # The console reads these, and its own page names them. They are listed
    # here so a reader sees that the omission below is about routes that are
    # NOT served, not about routes nobody documented.
    console = _console()
    for served in (
        "GET /viz/api/console/health",
        "GET /viz/api/console/attempts",
        "GET /viz/api/console/attempts/detail",
        "GET /viz/api/console/producers",
        "GET /viz/api/console/ontology-rollup",
        "GET /viz/api/console/versions",
        # Explore ships live (PACT operator-console-live-explore), so the
        # record reads moved from the removed list to this one. They were
        # listed as removed when the old Search view was deleted and no
        # shipping view read them; a view reads them again now.
        "GET /viz/api/console/records/types",
        "GET /viz/api/console/records/search",
        "GET /viz/api/console/records/list",
        "GET /viz/api/console/records/detail",
        "GET /viz/api/console/records/artifact",
        "GET /viz/api/console/records/raw",
        # PACT console-graph-views.
        "GET /viz/api/console/graph/types",
        "GET /viz/api/console/graph/source-links",
        "GET /viz/api/console/graph/record",
        "GET /viz/api/console/graph/ontology",
    ):
        assert served in console, f"{served} is served and must be named"
    for removed in (
        # The old whole-graph read and its batch twin. The Graph view's reads
        # live under "/viz/api/console/graph/", so these are matched whole.
        "/viz/api/console/graph`",
        "/viz/api/console/graph?",
        "/viz/api/console/graph-batch",
        # The OLD Search view's route, which no deployment serves. It left
        # this list with "/viz/api/console/records/" when Explore shipped;
        # only the second of those two had to go, and they are different
        # routes ("records/search" does not contain this string).
        "/viz/api/console/search",
        "/viz/api/telemetry/",
        "/viz/api/trace?",
    ):
        assert removed not in _both(), f"{removed} is no longer served"


def test_operator_ui_contract_states_that_no_chat_link_is_emitted() -> None:
    """No remaining view shows a record, a session, a run or a trace.

    The contract must say so rather than leave a reader to infer it from a
    missing section: a link to a removed view would open Home and read as
    though the answer were wrong.
    """
    single_line = _single_line_contract()
    assert "a chat answer emits no console link at all" in single_line
    assert "renders as plain text with a stated reason" in single_line
    assert "src/okg/substrate/chat/deep_links.py" in single_line
    # No hash for a view this app does not serve.
    for removed in ("#search?record=", "#graph?", "#trace?"):
        assert removed not in single_line, f"{removed} still named"
