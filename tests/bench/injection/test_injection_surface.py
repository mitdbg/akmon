"""The relocated package's public surface, pinned.

``okg.bench.injection.__all__`` is the contract: the names a caller outside
the package may use, and the whole of what a later consumer has to adopt. Two
things are asserted here.

FIRST, that the surface is real — every advertised name resolves from the
package root, so a consumer never has to reach into a submodule to get one.

SECOND, and the reason this file exists rather than being folded into the
boundary guard: that the package does not leak substrate-private imports
through that surface. The substrate names it depends on are enumerated below.
A consumer folding this package into another tree inherits exactly that list
and nothing else, so the fold is an import change rather than an excavation.
Adding a substrate dependency here is a deliberate act that has to update this
test, not something that arrives unnoticed with a convenient fix.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PACKAGE = _REPO_ROOT / "src" / "okg" / "bench" / "injection"

# Every substrate name the package reaches for. All are public substrate API;
# all point bench -> substrate, which is the permitted direction.
_ALLOWED_SUBSTRATE_IMPORTS = {
    # The mutation library's single external dependency: claim_corpus_source
    # implements a corpus Source, so it emits the substrate's fact types.
    "okg.substrate.library.sources.base",
    # The ring-0 runner drives real publish cycles (deferred imports).
    "okg.substrate.graph_store",
    "okg.substrate.publisher.factory",
}


def test_the_declared_surface_resolves_from_the_package_root() -> None:
    module = importlib.import_module("okg.bench.injection")
    declared = list(module.__all__)

    # Ordering is ruff's job (RUF022); duplicates are a surface defect.
    dupes = sorted({n for n in declared if declared.count(n) > 1})
    assert not dupes, f"__all__ lists the same name twice: {dupes}"

    missing = [name for name in declared if not hasattr(module, name)]
    assert not missing, (
        f"names advertised in __all__ but absent from the package: {missing}"
    )


def test_the_package_reaches_only_the_enumerated_substrate_names() -> None:
    """No substrate-private import leaks through the package."""
    found: dict[str, list[str]] = {}

    modules = [
        p for p in sorted(_PACKAGE.rglob("*.py"))
        if "__pycache__" not in p.parts
    ]
    assert modules, "package scan found no modules"

    for path in modules:
        tree = ast.parse(path.read_text(), filename=str(path))
        rel = str(path.relative_to(_REPO_ROOT))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module] if node.module else []
            else:
                continue
            for name in names:
                if name.startswith("okg.substrate"):
                    found.setdefault(name, []).append(f"{rel}:{node.lineno}")

    unexpected = {k: v for k, v in found.items() if k not in _ALLOWED_SUBSTRATE_IMPORTS}
    assert not unexpected, (
        "the package reaches substrate names outside its declared seam: "
        f"{unexpected}. Widening the seam is a decision — update "
        "_ALLOWED_SUBSTRATE_IMPORTS and the package docstring deliberately."
    )

    stale = _ALLOWED_SUBSTRATE_IMPORTS - set(found)
    assert not stale, (
        f"declared substrate dependencies no longer used: {stale}. Narrow the "
        "seam rather than leaving it advertised wider than it is."
    )
