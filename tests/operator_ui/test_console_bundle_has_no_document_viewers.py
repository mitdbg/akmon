"""The live console carries no PDF, HTML, Markdown or code viewer.

The only byte store the record reads serve is ``okg.visual_crop_blobs``,
whose ``ARTIFACT_MEDIA_TYPES`` are image/png, image/jpeg, image/gif and
image/webp. No route serves PDF, HTML, Markdown or code bytes (#2121), so
the four viewers in ``console/viewers/`` can never fire in the console, and
carrying pdf.js, react-markdown and shiki into it would be paying about
200 KB gzip for a viewer with nothing to draw.

A byte count cannot hold that. ``console_js`` has headroom, so a re-added
viewer would fit under the ceiling and ship silently. This walks the module
graph from ``routes/console/ConsoleRoute.tsx`` instead and fails by name on
the first edge that reaches one of the four.

Both static and dynamic imports are followed: a dynamic import is still
built, still emitted and still fetched by the console when the branch that
names it runs, so "lazy" does not put a module outside this boundary.
"""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / "src/okg/operator_ui/web/src/operator-ui"
ENTRY = WEB / "routes/console/ConsoleRoute.tsx"

#: The four modules the live console must not reach, by path under `WEB`.
FORBIDDEN = {
    "console/viewers/PdfViewer.tsx",
    "console/viewers/HtmlFrame.tsx",
    "console/viewers/MarkdownViewer.tsx",
    "console/viewers/CodeViewer.tsx",
}

#: `import x from "..."`, `import "..."`, `export ... from "..."` and
#: `import("...")`. A specifier built at runtime is not matched and cannot
#: be: it would also defeat Vite's own graph, so none is written here.
_SPECIFIER = re.compile(
    r"""(?:\bfrom\s*|\bimport\s*\(?\s*|\bexport\s*\*\s*from\s*)["']([^"']+)["']""",
)
#: A type-only import is erased before the bundler sees it.
_TYPE_ONLY = re.compile(r"\bimport\s+type\b|\bexport\s+type\b")

_EXTENSIONS = (".ts", ".tsx", ".js", ".jsx")


def _resolve(specifier: str, importer: Path) -> Path | None:
    """The file a specifier names, or None when it leaves this tree."""
    if specifier.startswith("@operator-ui/"):
        base = WEB / specifier[len("@operator-ui/"):]
    elif specifier.startswith("."):
        base = (importer.parent / specifier).resolve()
    else:
        return None  # a package, a stylesheet alias, or an asset query
    base = Path(str(base).split("?", 1)[0])
    if base.suffix in _EXTENSIONS and base.is_file():
        return base
    for extension in _EXTENSIONS:
        candidate = base.with_suffix(base.suffix + extension)
        if candidate.is_file():
            return candidate
    for extension in _EXTENSIONS:
        candidate = base / f"index{extension}"
        if candidate.is_file():
            return candidate
    return None


def _imports(source: Path) -> list[str]:
    out: list[str] = []
    for line in source.read_text(encoding="utf-8").splitlines():
        if _TYPE_ONLY.search(line):
            continue
        out.extend(_SPECIFIER.findall(line))
    return out


def _reachable_from(entry: Path) -> dict[Path, Path | None]:
    """Every module the entry reaches, mapped to the module that imported it."""
    importer_of: dict[Path, Path | None] = {entry: None}
    stack = [entry]
    while stack:
        current = stack.pop()
        for specifier in _imports(current):
            target = _resolve(specifier, current)
            if target is None or target in importer_of:
                continue
            importer_of[target] = current
            stack.append(target)
    return importer_of


def _chain(importer_of: dict[Path, Path | None], target: Path) -> str:
    names = []
    node: Path | None = target
    while node is not None:
        names.append(str(node.relative_to(WEB)))
        node = importer_of[node]
    return " <- ".join(names)


def test_the_entry_and_the_four_viewers_are_all_on_disk() -> None:
    """The guard is aimed at files that exist, so a rename cannot pass it."""
    assert ENTRY.is_file(), ENTRY
    missing = [name for name in FORBIDDEN if not (WEB / name).is_file()]
    assert not missing, (
        f"{missing} are named by this guard but are not on disk. The viewers stay "
        "for the design harness; if one really was deleted, re-aim this guard and "
        "the explore_js / pdf_worker budgets together."
    )


def test_the_console_route_reaches_no_document_viewer() -> None:
    importer_of = _reachable_from(ENTRY)
    # The walk has to actually walk: an entry that resolved nothing would
    # pass the check below while proving nothing.
    assert len(importer_of) > 20, f"only {len(importer_of)} modules reached from {ENTRY}"
    found = {
        name: _chain(importer_of, WEB / name)
        for name in sorted(FORBIDDEN)
        if (WEB / name) in importer_of
    }
    assert not found, (
        "the live console reaches a document viewer:\n"
        + "\n".join(f"  {name}: {chain}" for name, chain in found.items())
        + "\nNo route serves PDF, HTML, Markdown or code bytes (#2121). Mount the "
        "viewer through DocumentViewersContext from the design harness instead."
    )


def test_the_design_harness_still_reaches_all_four() -> None:
    """The cut is a boundary, not a deletion: the harness still draws them."""
    importer_of = _reachable_from(WEB / "design-harness/DesignHarness.tsx")
    unreached = sorted(name for name in FORBIDDEN if (WEB / name) not in importer_of)
    assert not unreached, (
        f"{unreached} are no longer reachable from the design harness, so its "
        "explore_js and pdf_worker budgets measure nothing."
    )
