"""Regression test: catalog constants live in mcp/source_catalog.py
and source_admin re-exports them.

Pinned by ``extract-mcp-source-catalog``.
"""
from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "src/okg/substrate/mcp"
SOURCE_ADMIN = SRC / "source_admin.py"
SOURCE_CATALOG = SRC / "source_catalog.py"


def test_source_catalog_module_defines_catalog() -> None:
    text = SOURCE_CATALOG.read_text()
    for name in (
        "SOURCE_CATALOG",
        "_LOCAL_PROJECT_SOURCE_IDS",
        "_CREDENTIAL_SOURCE_IDS",
        "_DEEP_INDEXING_SOURCE_IDS",
        "_SOURCE_FAMILY_RANK",
        "_load_catalog_from_wizards",
    ):
        assert name in text, (
            f"mcp/source_catalog.py is missing {name!r}"
        )


def test_source_admin_reexports_catalog() -> None:
    text = SOURCE_ADMIN.read_text()
    assert (
        "from okg.substrate.mcp.source_catalog import" in text
    ), (
        "mcp/source_admin.py MUST import the catalog constants "
        "from .source_catalog so source_admin.SOURCE_CATALOG "
        "keeps resolving for downstream callers"
    )


def test_source_admin_does_not_redefine_catalog() -> None:
    """The actual SOURCE_CATALOG assignment MUST live in
    source_catalog.py, not be re-assigned in source_admin.py."""
    text = SOURCE_ADMIN.read_text()
    # An assignment looks like `SOURCE_CATALOG: ... = ...` or
    # `SOURCE_CATALOG = ...`. Import-rebind via
    # `from .source_catalog import SOURCE_CATALOG` does NOT match.
    bad_lines = [
        line
        for line in text.splitlines()
        if line.startswith("SOURCE_CATALOG")
        and ("=" in line)
        and not line.lstrip().startswith("#")
    ]
    assert bad_lines == [], (
        f"mcp/source_admin.py re-assigned SOURCE_CATALOG: "
        f"{bad_lines} — it MUST come from .source_catalog"
    )


def test_attribute_access_preserved() -> None:
    """`source_admin.SOURCE_CATALOG` MUST resolve via re-export."""
    import okg.substrate.mcp.source_admin as sa
    assert hasattr(sa, "SOURCE_CATALOG"), (
        "source_admin module lost the SOURCE_CATALOG attribute "
        "— re-export broken"
    )
    assert isinstance(sa.SOURCE_CATALOG, dict)
