"""The source guide names the capabilities reference and refuses a stored secret.

Issue #1965. This assertion used to live in
``tests/substrate/cli/test_connector_operator_journey.py``, whose file-level
``pytest.mark.postgres`` kept it out of every lane CI runs per pull request.
It pinned ``docs/sources/connect.md``, a page ``523c8e9770`` moved to
``docs/use/sources/connect.md`` and ``104130c346`` folded into the single
source guide ``docs/use/sources/index.md``; it had therefore been failing,
unseen, for a month. It needs no database, so it belongs here, where the
``fast`` lane selects ``tests/docs``.

What this adds, and what it does not: ``test_documentation_link_graph.py``
already resolves every local link on every retained page per pull request, so
a dead link is caught there, not here. The coverage added here is that the
guide still *carries* the link to the capabilities reference at all -- a
rewrite could drop it and leave every surviving link valid -- and that it
still tells the operator to store a credential reference rather than the
secret itself. The target is opened as well so that this test fails on its own
terms rather than asserting a path it never reads.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

GUIDE = ROOT / "docs" / "use" / "sources" / "index.md"
CAPABILITIES_LINK = "../../reference/capabilities.md"


def test_source_guide_points_to_live_capabilities_without_copying_secrets() -> None:
    assert GUIDE.is_file(), f"source guide missing at {GUIDE}"
    text = GUIDE.read_text(encoding="utf-8")

    # It points at the live capabilities reference, and that page exists.
    assert CAPABILITIES_LINK in text, (
        "source guide no longer links the capabilities reference"
    )
    target = (GUIDE.parent / CAPABILITIES_LINK).resolve()
    assert target.is_file(), f"capabilities reference missing at {target}"

    # It names the credential reference instead of copying the secret.
    assert "credential reference" in text
    assert "Do not store a token" in text


# Issue #3102. The capabilities reference listed a Crossref adapter that does
# not exist; Crossref appears only as a provenance label inside other
# scholarly adapters. Every scholarly source the reference names must map to
# an adapter module that is really there.
SOURCES = ROOT / "src" / "okg" / "substrate" / "library" / "sources"
SCHOLARLY_ADAPTERS = {
    "BibTeX": SOURCES / "scholarly" / "bib_references.py",
    "ORCID": SOURCES / "academic_researcher" / "orcid_publications.py",
    "DBLP": SOURCES / "scholarly" / "dblp.py",
    "OpenAlex": SOURCES / "scholarly" / "openalex.py",
    "Semantic Scholar": SOURCES / "scholarly" / "semantic_scholar.py",
    "arXiv": SOURCES / "scholarly" / "arxiv.py",
    "OpenReview": SOURCES / "scholarly" / "openreview.py",
    "INSPIRE": SOURCES / "scholarly" / "inspire_hep.py",
}
NOT_ADAPTERS = {"PDFs and drafts"}


def _named_scholarly_sources(text: str) -> list[str]:
    summary = re.search(r"Adapters exist for sources including ([^.]+)\.", text)
    assert summary, "status summary no longer lists scholarly adapters"
    family = re.search(r"^\| Scholarly records \| ([^|]+) \|", text, re.MULTILINE)
    assert family, "source families table no longer has a scholarly row"
    names: list[str] = []
    for listing in (summary.group(1), family.group(1)):
        for part in listing.split(","):
            name = part.strip().removeprefix("and ").strip()
            if name and name not in NOT_ADAPTERS:
                names.append(name)
    return names


def test_capabilities_names_only_scholarly_adapters_that_exist() -> None:
    target = (GUIDE.parent / CAPABILITIES_LINK).resolve()
    names = _named_scholarly_sources(target.read_text(encoding="utf-8"))

    assert names
    unknown = sorted({n for n in names if n not in SCHOLARLY_ADAPTERS})
    assert not unknown, f"capabilities reference names no adapter for: {unknown}"
    missing = sorted(n for n in set(names) if not SCHOLARLY_ADAPTERS[n].is_file())
    assert not missing, f"adapter module missing for: {missing}"
