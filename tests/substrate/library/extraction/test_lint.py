"""Wrap-invariant lint tests."""
from __future__ import annotations


def test_extraction_tree_passes_lint() -> None:
    """The shipped extraction tree must pass its own lint."""
    from okg.substrate.library.extraction._lint import lint_extraction_tree
    report = lint_extraction_tree()
    assert report.ok, (
        f"wrap-invariant lint failed: "
        f"{[(v.module_path, v.third_party_imports, v.reason) for v in report.violations]}"
    )
    assert report.modules_checked > 0


def test_lint_finds_two_third_party_imports(tmp_path) -> None:
    """A backend module that imports 2 third-party packages
    without a waiver is flagged."""
    from okg.substrate.library.extraction._lint import (
        _top_level_third_party_imports,
    )
    bad = tmp_path / "bad.py"
    bad.write_text(
        "import lxml\n"
        "import pypdf\n"
        "class BadParser:\n"
        "    def parse(self, c): return None\n"
    )
    imports = _top_level_third_party_imports(bad)
    assert "lxml" in imports
    assert "pypdf" in imports


def test_lint_filters_stdlib() -> None:
    """Stdlib imports don't count toward the per-module budget."""
    from okg.substrate.library.extraction._lint import (
        _top_level_third_party_imports,
    )
    from pathlib import Path
    import tempfile
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False,
    ) as f:
        f.write(
            "import os\nimport ast\nimport datetime\n"
            "import re\nfrom typing import Any\n"
        )
        f.flush()
        path = Path(f.name)
    try:
        assert _top_level_third_party_imports(path) == set()
    finally:
        path.unlink()


def test_lint_filters_okg_internal() -> None:
    from okg.substrate.library.extraction._lint import (
        _top_level_third_party_imports,
    )
    from pathlib import Path
    import tempfile
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False,
    ) as f:
        f.write(
            "from okg.substrate.errors import SubstrateError\n"
            "import okg.substrate.publisher\n"
        )
        f.flush()
        path = Path(f.name)
    try:
        assert _top_level_third_party_imports(path) == set()
    finally:
        path.unlink()
