"""Semantic checks for the current AKMON technical documentation.

These checks deliberately inspect repository paths, Python definitions, and
the registered MCP surface. They do not assert a particular sentence or line
wrap, so a plain-language rewrite does not create needless test churn.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from okg.substrate.mcp.operators import OPERATOR_TOOLS


ROOT = Path(__file__).resolve().parents[2]


def _defined_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
    return names


def _has_symbol(path: Path, symbol: str) -> bool:
    """Resolve a dotted class/method reference without importing production code."""
    parts = symbol.split(".")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    def find(body: list[ast.stmt], wanted: list[str]) -> bool:
        if not wanted:
            return True
        name = wanted[0]
        for node in body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name != name:
                    continue
                if len(wanted) == 1:
                    return True
                if isinstance(node, ast.ClassDef):
                    return find(node.body, wanted[1:])
        return False

    return find(tree.body, parts)


def _current_mcp_table(text: str) -> set[str]:
    table = text.split("| Tool | Class | Purpose |", 1)[1].split("\n## ", 1)[0]
    return set(re.findall(r"^\| `([a-z][a-z0-9_]*)` \|", table, re.MULTILINE))


def _authority_failures(text: str) -> list[str]:
    """Return claims that make retired/planned material look current."""
    failures: list[str] = []
    if "openspec/changes/archive" in text:
        failures.append("active OpenSpec archive path")
    if re.search(r"(?i)canonical home:.*openspec|authoritative.*openspec", text):
        failures.append("OpenSpec authority claim")
    if re.search(r"(?i)^> \*\*Status:\*\* stub", text, re.MULTILINE):
        failures.append("active stub")
    return failures


def _dead_repository_paths(text: str) -> list[str]:
    paths = re.findall(r"`(src/[^`]+|docs/[^`]+)`", text)
    return [path for path in paths if not (ROOT / path).exists()]


def test_current_component_anchors_resolve_to_live_definitions() -> None:
    references = {
        "src/okg/substrate/library/sources/base.py": (
            "SourceAdapter",
            "SourceRun",
            "NodeFact",
            "EdgeFact",
        ),
        "src/okg/substrate/ingest/runner.py": ("SourceRunner",),
        "src/okg/substrate/fact_application.py": ("FactBatchApplication",),
        "src/okg/substrate/graph_store.py": ("ChronosGraphStore",),
        "src/okg/substrate/publisher/publisher.py": ("Publisher", "Publisher.cycle"),
        "src/okg/substrate/publisher/extractor_stage.py": (
            "ChronosExtractorStage",
        ),
        "src/okg/substrate/projection/router.py": ("AliasResolver",),
        "src/okg/substrate/enrichers/base.py": ("Enricher", "EnrichResult"),
        "src/okg/substrate/library/linkers/declarative.py": ("DeclarativeLinker",),
        "src/okg/substrate/publisher/enricher_loader.py": (
            "load_enrichers",
        ),
        "src/okg/substrate/nomos/core.py": ("evaluate",),
        "src/okg/substrate/errors.py": ("PublisherLeaseHeld",),
    }
    for relative, symbols in references.items():
        path = ROOT / relative
        assert path.is_file(), f"documented implementation path moved: {relative}"
        names = _defined_names(path)
        for symbol in symbols:
            assert _has_symbol(path, symbol), (
                f"documented symbol {symbol!r} is not defined in {relative}; "
                f"defined names include {sorted(names)[:12]}"
            )


def test_component_docs_use_the_current_source_adapter_boundary() -> None:
    pipeline = (ROOT / "docs/develop/architecture/pipeline.md").read_text(encoding="utf-8")
    components = (ROOT / "docs/develop/architecture/overview.md").read_text(
        encoding="utf-8"
    )
    glossary = (ROOT / "docs/reference/glossary.md").read_text(encoding="utf-8")
    for text in (pipeline, components):
        assert "src/okg/substrate/library/sources/base.py" in text
    assert "## Source adapter (`SourceAdapter`)" in glossary
    assert "The Python integration that reads one kind of source" in glossary
    for retired in (
        "ConnectorDefinition",
        "ProviderConnectionDefinition",
        "SourceBinding",
    ):
        assert retired not in pipeline
        assert retired not in components
    assert "okg.substrate.sources.SourceAdapter" not in glossary
    assert "src/okg/substrate/sources/__init__.py" not in glossary


def test_documented_current_mcp_names_match_registration() -> None:
    text = (ROOT / "docs/reference/mcp.md").read_text(encoding="utf-8")
    assert _current_mcp_table(text) == set(OPERATOR_TOOLS)
    assert "run_sql" not in _current_mcp_table(text)
    assert "`run_sql` and other retired names are not callable tools" in text


def test_scoped_active_pages_do_not_present_legacy_authority_or_stubs() -> None:
    pages = [
        *sorted((ROOT / "docs/architecture").glob("*.md")),
        *sorted((ROOT / "docs/interfaces").glob("*.md")),
        ROOT / "docs/runbooks/workspace-graph/setup.md",
        ROOT / "docs/operate/deployments/external.md",
        *sorted((ROOT / "docs/develop/sdk").glob("*.md")),
        ROOT / "docs/reference/glossary.md",
    ]
    # Historical M1/M3/M5 pages are intentionally outside the current surface.
    failures = {
        str(path.relative_to(ROOT)): _authority_failures(path.read_text(encoding="utf-8"))
        for path in pages
    }
    failures = {path: reasons for path, reasons in failures.items() if reasons}
    assert not failures, failures


def test_semantic_fixture_rejects_bad_claims_but_accepts_plain_language() -> None:
    good = "The adapter is implemented by `src/okg/substrate/library/sources/base.py`."
    bad_path = "The adapter is implemented by `src/okg/substrate/sources/base.py`."
    bad_symbol_path = ROOT / "src/okg/substrate/sources/__init__.py"
    historical = "The old tool is historical; use `query` instead."
    stub = "> **Status:** stub. A replacement is planned."
    authority = "Canonical home: openspec/changes/archive/old/proposal.md"
    assert _authority_failures(good) == []
    assert _authority_failures(historical) == []
    assert _dead_repository_paths(good) == []
    assert _dead_repository_paths(bad_path) == ["src/okg/substrate/sources/base.py"]
    assert not _has_symbol(bad_symbol_path, "SourceAdapter")
    assert "active stub" in _authority_failures(stub)
    assert "active OpenSpec archive path" in _authority_failures(authority)
