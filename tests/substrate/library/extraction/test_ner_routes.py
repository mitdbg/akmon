"""Per `refine-doc-corpus-extraction` (2026-04-26) — G2:
per-format NER routes.

Asserts that NER routes filter by parser format (NOT chunk
subtype): a route with `formats: [markdown, text]` runs on
markdown-derived chunks, doesn't run on json-derived chunks; a
route with `formats: ["*"]` runs on every chunk including
unknown/None formats.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.library.extraction.registry import (
    NerRoute,
    load_extractors,
)


def _write_extractors(tmp_path: Path, doc: dict) -> Path:
    out = tmp_path / "ext"
    out.mkdir()
    (out / "extractors.yaml").write_text(yaml.safe_dump(doc))
    return out


def test_per_format_ner_routes_filter_correctly(tmp_path):
    extr = _write_extractors(tmp_path, {
        "ner": [
            {
                "formats": ["markdown", "text"],
                "class": (
                    "okg.substrate.library.extraction.ner."
                    "regex_backend.RegexEntityExtractor"
                ),
                "config": {
                    "rules": [["change_slug", "fix-[a-z]+"]],
                },
            },
            {
                "formats": ["*"],
                "class": (
                    "okg.substrate.library.extraction.ner."
                    "regex_backend.RegexEntityExtractor"
                ),
                "config": {
                    "rules": [["arxiv_id", "arXiv:[0-9]{4}"]],
                },
            },
        ],
    })
    bundle = load_extractors(extr)
    assert len(bundle.ner_routes) == 2

    # Markdown chunk fires both routes.
    md_ners = bundle.ner_for_format("markdown")
    assert len(md_ners) == 2

    # JSON chunk fires only the wildcard.
    json_ners = bundle.ner_for_format("json")
    assert len(json_ners) == 1

    # No-format (None) chunk fires only the wildcard.
    none_ners = bundle.ner_for_format(None)
    assert len(none_ners) == 1


def test_legacy_bare_ner_list_wraps_as_wildcard_route(tmp_path):
    """Bare NER YAML (no `formats:` key) maps to one route
    with formats=("*",) so legacy yamls keep working."""
    extr = _write_extractors(tmp_path, {
        "ner": [{
            "class": (
                "okg.substrate.library.extraction.ner."
                "regex_backend.RegexEntityExtractor"
            ),
            "config": {"rules": [["doi", "10\\.[0-9]+"]]},
        }],
    })
    bundle = load_extractors(extr)
    assert len(bundle.ner_routes) == 1
    route = bundle.ner_routes[0]
    assert route.formats == ("*",)
    # Wildcard matches every format.
    assert route.matches("anything")
    assert route.matches(None)
    # Legacy `bundle.ner` returns the single extractor.
    assert bundle.ner is route.extractor


def test_route_matches_predicate() -> None:
    """Unit-test the matcher in isolation."""
    backend = object()  # stand-in
    r1 = NerRoute(formats=("markdown", "text"), extractor=backend)
    r2 = NerRoute(formats=("*",), extractor=backend)
    assert r1.matches("markdown") is True
    assert r1.matches("text") is True
    assert r1.matches("json") is False
    assert r1.matches(None) is False
    assert r2.matches("anything") is True
    assert r2.matches(None) is True


def test_string_formats_field_is_promoted_to_tuple(tmp_path):
    """Operators may pass a string `formats: markdown` (not a
    list); the registry promotes it to `("markdown",)`."""
    extr = _write_extractors(tmp_path, {
        "ner": [{
            "formats": "markdown",
            "class": (
                "okg.substrate.library.extraction.ner."
                "regex_backend.RegexEntityExtractor"
            ),
            "config": {"rules": []},
        }],
    })
    bundle = load_extractors(extr)
    assert bundle.ner_routes[0].formats == ("markdown",)
