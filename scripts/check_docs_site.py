#!/usr/bin/env python3
"""Check that AKMON's documentation is small, connected, and current."""

from __future__ import annotations

from collections import defaultdict
from difflib import SequenceMatcher
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import yaml


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
MKDOCS = ROOT / "mkdocs.yml"
TYPE_VOCABULARY = {"tutorial", "how-to", "reference", "explanation", "overview"}
FORBIDDEN_PATHS = {
    "docs/history/README.md",
    "docs/architecture/db-schema-review-2026-07.md",
    "docs/architecture/db-schema-v1-design.md",
    "docs/reference/STYLE.md",
    "docs/reference/cli-surface-audit.md",
    "docs/reference/docs-site-inventory.md",
    "docs/reference/poros-demo.md",
    "docs/runbooks/migrate-openspec-to-pact.md",
    "docs/runbooks/no-silent-fallbacks.md",
    "docs/runbooks/operator-ui-latency-cutover.md",
    "docs/users/README.md",
    "docs/data-owners/README.md",
    "docs/researchers/README.md",
    "docs/operators/README.md",
    "docs/developers/README.md",
    "docs/agents/README.md",
}
FORBIDDEN_PREFIXES = (
    "docs/audits/",
    "docs/okg_spec/",
)
LIVE_NON_PUBLISHED_PROGRAM_ARTIFACTS = {
    # The active fallback-removal PACTs still use these machine-readable ledgers.
    # They live beside their tests, outside docs/, so the reader site never
    # carries them; the legacy inventory-less check still names them exactly.
    "tests/docs/ledgers/issue-979-program-ledger.yaml",
    "tests/docs/ledgers/pr-1003-wave1-extraction-ledger.yaml",
    "tests/docs/ledgers/repository-fallback-findings.yaml",
}
FORBIDDEN_PATH_PATTERNS = (
    re.compile(r"^docs/history(?:/|$)"),
    re.compile(r"^docs/.*/(?:review|audit)-20\d\d(?:[-./]|$)"),
    re.compile(r"^docs/.*/20\d\d(?:[-_.]\d\d){0,2}[-_](?:review|audit)(?:[-./]|$)"),
    re.compile(r"^docs/architecture/db-schema-(?:review|v1-design)(?:[-./]|$)"),
    re.compile(r"^docs/interfaces/m\d+[-_]"),
    re.compile(r"^docs/reference/(?:STYLE|cli-surface-audit|docs-site-inventory|poros-demo)(?:[-./]|$)"),
    re.compile(r"^docs/runbooks/(?:migrate-openspec-to-pact|no-silent-fallbacks|operator-ui-latency-cutover)(?:[-./]|$)"),
    re.compile(r"^docs/(?:users|data-owners|researchers|operators|developers|agents)/README\.md$"),
)
FORBIDDEN_HEADINGS = re.compile(
    r"^#{1,6}\s+(?:history and audits|documentation inventory|cli surface audit|poros demo evidence)\b",
    re.IGNORECASE | re.MULTILINE,
)
REQUIRED_LINK_VALIDATION = {
    "not_found": "warn",
    "absolute_links": "warn",
    "unrecognized_links": "warn",
    "anchors": "warn",
}
COMMENTARY_PATTERNS = (
    "organized around",
    "canonical home:",
    "canonical repository",
    "per the design discussion",
    "customer paths",
    "what this doc is not",
    "deepest link",
    "these pages describe",
    "choose a job",
    "choose the path",
    "the user asked",
    "we discussed",
    "after feedback",
    "this revision",
    "previous wording",
    "earlier draft",
    "reader proxy",
    "reviewer found",
    "pr #",
)

FRONT_MATTER_RE = re.compile(r"\A---\n(.*?)\n---\n", re.DOTALL)
LINK_RE = re.compile(r"(?<!!)\[([^]]+)\]\(([^)]+)\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)
EXPLICIT_ANCHOR_RE = re.compile(r"\{#([\w-]+)\}")
HTML_ANCHOR_RE = re.compile(r'<a\s+(?:name|id)=["\']([^"\']+)["\']')
FENCED_BLOCK_RE = re.compile(r"```[^\n]*\n(.*?)```", re.DOTALL)


def _parse_front_matter(text: str) -> dict[str, Any] | None:
    match = FRONT_MATTER_RE.match(text)
    if not match:
        return None
    value = yaml.safe_load(match.group(1)) or {}
    return value if isinstance(value, dict) else None


def _active_doc_paths() -> list[Path]:
    return sorted(DOCS.rglob("*.md"))


def _load_inventory() -> dict[str, Any] | None:
    path = ROOT / "documentation.yaml"
    if not path.exists():
        return None
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else None


def _unpublished(publication: dict[str, Any]) -> tuple[str, ...]:
    """Source-relative prefixes kept on disk but excluded from the site."""
    prefixes = publication.get("unpublished") or []
    return tuple(str(prefix) for prefix in prefixes)


def _is_unpublished(relative: str, prefixes: tuple[str, ...]) -> bool:
    return any(relative.startswith(prefix) for prefix in prefixes)


def _nav_paths(value: object) -> set[str]:
    paths: set[str] = set()
    if isinstance(value, list):
        for item in value:
            paths.update(_nav_paths(item))
    elif isinstance(value, dict):
        for item in value.values():
            paths.update(_nav_paths(item))
    elif isinstance(value, str) and value.endswith((".md", ".yaml", ".yml")):
        paths.add(value)
    return paths


def _nav_labels(value: object) -> list[str]:
    labels: list[str] = []
    if isinstance(value, list):
        for item in value:
            labels.extend(_nav_labels(item))
    elif isinstance(value, dict):
        for label, child in value.items():
            labels.append(str(label))
            labels.extend(_nav_labels(child))
    return labels


def _slug(text: str) -> str:
    text = re.sub(r"\s+\{#[^}]+\}\s*$", "", text.strip())
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[`*~]", "", text).lower()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"[-\s]+", "-", text).strip("-")


def _anchors(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    anchors = {_slug(title) for _, title in HEADING_RE.findall(text)}
    anchors.update(EXPLICIT_ANCHOR_RE.findall(text))
    anchors.update(HTML_ANCHOR_RE.findall(text))
    return {anchor for anchor in anchors if anchor}


def _local_target(source: Path, raw: str) -> tuple[Path | None, str | None]:
    target = unquote(raw.strip())
    if not target or target.startswith(("http://", "https://", "mailto:", "tel:")):
        return None, None
    target = target.split("?", 1)[0]
    path_text, _, anchor = target.partition("#")
    if path_text.startswith("/"):
        return None, None
    target_path = source if not path_text else (source.parent / path_text).resolve()
    if target_path.is_dir() and (target_path / "README.md").exists():
        target_path = target_path / "README.md"
    return target_path, anchor or None


def _check_front_matter(errors: list[str]) -> None:
    seen: dict[str, Path] = {}
    for path in _active_doc_paths():
        relative = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        front = _parse_front_matter(text)
        if front is None:
            errors.append(f"{relative}: missing YAML front matter")
            continue
        if not front.get("audience"):
            errors.append(f"{relative}: missing audience metadata")
        if front.get("type") not in TYPE_VOCABULARY:
            errors.append(f"{relative}: unknown documentation type {front.get('type')!r}")
        canonical_id = front.get("canonical_id")
        if not isinstance(canonical_id, str) or not canonical_id.strip():
            errors.append(f"{relative}: missing canonical_id")
        elif canonical_id in seen:
            errors.append(
                f"{relative}: canonical_id {canonical_id!r} already belongs to "
                f"{seen[canonical_id].relative_to(ROOT)}"
            )
        else:
            seen[canonical_id] = path
        if str(front.get("status", "active")).lower() == "historical":
            errors.append(f"{relative}: historical material belongs in Git or an archived PACT")
        if str(front.get("status", "active")).lower() in {"compatibility", "retired"}:
            errors.append(f"{relative}: compatibility-only or retired pages are not published")
        if str(front.get("visibility", "")).lower() == "search-only" or "replacement" in front:
            errors.append(f"{relative}: compatibility placeholders are not published")
        if re.search(r"^\*\*Audience:\*\*", text, re.MULTILINE):
            errors.append(f"{relative}: repeats audience metadata in reader-facing text")
        if FORBIDDEN_HEADINGS.search(text):
            errors.append(f"{relative}: forbidden history or audit section has returned")
        lowered = text.lower()
        for phrase in COMMENTARY_PATTERNS:
            if phrase in lowered:
                errors.append(f"{relative}: contains maintainer commentary {phrase!r}")


def _check_retired_paths(errors: list[str]) -> None:
    inventory = _load_inventory()
    if inventory is not None:
        for relative in inventory.get("retired_roots", []):
            if (ROOT / relative).exists():
                errors.append(f"{relative}: retired documentation path has returned")
        return
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(ROOT).as_posix()
        if relative in LIVE_NON_PUBLISHED_PROGRAM_ARTIFACTS:
            continue
        if (
            relative in FORBIDDEN_PATHS
            or relative.startswith(FORBIDDEN_PREFIXES)
            or any(pattern.search(relative) for pattern in FORBIDDEN_PATH_PATTERNS)
        ):
            errors.append(f"{relative}: retired documentation path has returned")


def _check_all_active_links(errors: list[str]) -> None:
    pages = _active_doc_paths()
    page_set = {path.resolve() for path in pages}
    edges: defaultdict[Path, set[Path]] = defaultdict(set)
    inventory = _load_inventory()
    unpublished_roots = [
        (DOCS / prefix).resolve()
        for publication in ((inventory or {}).get("publications") or {}).values()
        for prefix in _unpublished(publication)
    ]

    def _under_unpublished(path: Path) -> bool:
        resolved = path.resolve()
        return any(root == resolved or root in resolved.parents for root in unpublished_roots)

    for source in [ROOT / "README.md", *pages]:
        text = source.read_text(encoding="utf-8")
        source_published = source.parent != ROOT and not _under_unpublished(source)
        for _, raw in LINK_RE.findall(text):
            if raw.startswith(("/handbook/", "handbook/")):
                errors.append(
                    f"{source.relative_to(ROOT)}: retired documentation route: {raw}"
                )
                continue
            target, anchor = _local_target(source, raw)
            if target is None:
                continue
            if not target.exists():
                errors.append(
                    f"{source.relative_to(ROOT)}: link target does not exist: {raw}"
                )
                continue
            if source_published and _under_unpublished(target):
                errors.append(
                    f"{source.relative_to(ROOT)}: published page links to an "
                    f"unpublished page: {raw}"
                )
            if anchor and target.suffix.lower() == ".md" and anchor not in _anchors(target):
                errors.append(
                    f"{source.relative_to(ROOT)}: heading anchor does not exist: {raw}"
                )
            if source.resolve() in page_set and target.resolve() in page_set:
                edges[source.resolve()].add(target.resolve())

    # Small mutation fixtures do not carry documentation.yaml, so retain the
    # direct route check that proves an unlinked page is rejected. In the real
    # repository, the exact inventory and complete navigation own reachability.
    if _load_inventory() is not None:
        return

    # The docs home and links from task/section pages are the route authority.
    # Navigation validates that a page exists, but does not make an otherwise
    # orphaned page reachable merely by listing it.
    seeds = {DOCS / "README.md"}
    reachable = {path.resolve() for path in seeds if path.exists()}
    frontier = list(reachable)
    while frontier:
        source = frontier.pop()
        for target in edges[source]:
            if target not in reachable:
                reachable.add(target)
                frontier.append(target)
    for path in pages:
        if path.resolve() not in reachable:
            errors.append(
                f"{path.relative_to(ROOT)}: no route from the docs home or task navigation"
            )


def _check_mkdocs_nav(errors: list[str]) -> None:
    inventory = _load_inventory()
    if inventory is not None:
        for name, publication in inventory.get("publications", {}).items():
            config_path = ROOT / publication["config"]
            config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            label = config_path.relative_to(ROOT).as_posix()
            if config.get("strict") is not True:
                errors.append(f"{label}: strict mode must remain enabled")
            if config.get("docs_dir") != publication["source_dir"]:
                errors.append(
                    f"{label}: docs_dir must be {publication['source_dir']!r}"
                )
            if config.get("site_dir") != publication["build_dir"]:
                errors.append(
                    f"{label}: site_dir must be {publication['build_dir']!r}"
                )
            plugins = config.get("plugins") or []
            if "search" not in plugins:
                errors.append(f"{label}: search must remain enabled")
            nav_paths = {
                path for path in _nav_paths(config.get("nav", [])) if path.endswith(".md")
            }
            expected = set(publication.get("pages", []))
            unpublished = _unpublished(publication)
            excluded = {
                line.strip()
                for line in str(config.get("exclude_docs") or "").splitlines()
                if line.strip() and not line.strip().startswith("#")
            }
            if set(unpublished) != excluded:
                errors.append(
                    f"{label}: exclude_docs {sorted(excluded)} must equal "
                    f"documentation.yaml unpublished {sorted(unpublished)}"
                )
            for path in sorted(nav_paths):
                if _is_unpublished(path, unpublished):
                    errors.append(f"{label}: unpublished page appears in navigation: {path}")
            for path in sorted(expected - nav_paths):
                errors.append(f"{label}: canonical page missing from navigation: {path}")
            for path in sorted(nav_paths - expected):
                errors.append(f"{label}: unowned navigation page: {path}")
            for path in sorted(nav_paths):
                if not (ROOT / publication["source_dir"] / path).exists():
                    errors.append(f"{label}: navigation target does not exist: {path}")
            asset_paths = {
                asset["path"] if isinstance(asset, dict) else asset
                for asset in publication.get("assets", [])
            }
            for path in sorted(asset_paths & _nav_paths(config.get("nav", []))):
                errors.append(f"{label}: non-page asset appears in navigation: {path}")
            link_config = ((config.get("validation") or {}).get("links") or {})
            expected_links = {
                "not_found": "warn",
                "absolute_links": "ignore",
                "unrecognized_links": "warn",
                "anchors": "warn",
            }
            for key, expected_value in expected_links.items():
                if link_config.get(key) != expected_value:
                    errors.append(
                        f"{label}: link validation {key} must remain "
                        f"{expected_value!r}; got {link_config.get(key)!r}"
                    )
        return

    config = yaml.safe_load(MKDOCS.read_text(encoding="utf-8")) or {}
    if config.get("strict") is not True:
        errors.append("mkdocs.yml: strict mode must remain enabled")
    nav = config.get("nav", [])
    labels = _nav_labels(nav)
    for retired in ("History and audits", "Role routes (compatibility)"):
        if retired in labels:
            errors.append(f"mkdocs.yml: retired navigation section {retired!r}")
    top_level_labels = [
        next(iter(item)) for item in nav if isinstance(item, dict) and item
    ] if isinstance(nav, list) else []
    if "Home" in top_level_labels:
        errors.append("mkdocs.yml: retired navigation section 'Home'")
    for relative in _nav_paths(config.get("nav", [])):
        if not (DOCS / relative).exists():
            errors.append(f"mkdocs.yml: navigation target does not exist: {relative}")
    link_config = ((config.get("validation") or {}).get("links") or {})
    for key, expected in REQUIRED_LINK_VALIDATION.items():
        if link_config.get(key) != expected:
            errors.append(
                f"mkdocs.yml: link validation {key} must remain {expected!r}; "
                f"got {link_config.get(key)!r}"
            )


def _check_duplicate_instructions(errors: list[str]) -> None:
    owners: defaultdict[str, set[str]] = defaultdict(set)
    procedures: list[tuple[str, str, set[str]]] = []
    for path in _active_doc_paths():
        text = path.read_text(encoding="utf-8")
        for block in FENCED_BLOCK_RE.findall(text):
            normalized = "\n".join(line.rstrip() for line in block.strip().splitlines())
            if len(normalized) >= 120:
                owners[normalized].add(path.relative_to(ROOT).as_posix())
            joined = re.sub(r"\\\n\s*", " ", block)
            signatures: set[str] = set()
            for match in re.finditer(
                r"(?:^|\n)\s*(?:uv run )?okg\s+([^\n]+)", joined
            ):
                tokens = match.group(1).split()
                if not tokens:
                    continue
                width = 2 if tokens[0] in {
                    "admission", "catalog", "consistency", "db", "deployment",
                    "environment", "generation", "jobs", "library", "pact",
                    "runtime", "schema", "source", "sync",
                } and len(tokens) > 1 else 1
                signatures.add(" ".join(tokens[:width]))
            if len(signatures) >= 2:
                procedures.append(
                    (path.relative_to(ROOT).as_posix(), joined.strip(), signatures)
                )
    for block, paths in owners.items():
        if len(paths) > 1:
            errors.append(
                "copied instruction block appears in " + ", ".join(sorted(paths))
            )
    for index, (left_path, left_block, left) in enumerate(procedures):
        for right_path, right_block, right in procedures[index + 1 :]:
            if left_path == right_path:
                continue
            shared = left & right
            similarity = SequenceMatcher(None, left_block, right_block).ratio()
            if (
                len(shared) >= 2
                and len(shared) / min(len(left), len(right)) >= 0.67
                and similarity >= 0.82
            ):
                errors.append(
                    "overlapping command procedure appears in "
                    f"{left_path}, {right_path}: {', '.join(sorted(shared))}"
                )


def _check_retired_terms(errors: list[str]) -> None:
    for path in _active_doc_paths():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if re.search(r"\brun_sql\s*\(", line):
                errors.append(
                    f"{path.relative_to(ROOT)}:{number}: retired run_sql call is current guidance"
                )


def _check_runbook_routing_and_recovery(errors: list[str]) -> None:
    inventory = _load_inventory()
    if inventory is not None:
        for publication in inventory.get("publications", {}).values():
            root = ROOT / publication["source_dir"]
            for asset in publication.get("assets", []):
                if not isinstance(asset, dict):
                    errors.append(
                        f"documentation.yaml: asset entry must name path and owner_page: {asset}"
                    )
                    continue
                asset_path = root / asset["path"]
                owner_path = root / asset["owner_page"]
                if not asset_path.is_file():
                    errors.append(
                        f"{asset_path.relative_to(ROOT)}: declared asset does not exist"
                    )
                    continue
                if not owner_path.is_file():
                    errors.append(
                        f"{owner_path.relative_to(ROOT)}: declared asset owner does not exist"
                    )
                    continue
                owner_text = owner_path.read_text(encoding="utf-8")
                if asset_path.name not in owner_text:
                    errors.append(
                        f"{owner_path.relative_to(ROOT)}: does not link declared asset "
                        f"{asset_path.name}"
                    )
        return

    index = DOCS / "runbooks/README.md"
    text = index.read_text(encoding="utf-8")
    linked = [
        Path(raw.split("#", 1)[0]).name
        for _, raw in LINK_RE.findall(text)
        if raw.endswith(".md") and not raw.startswith("../")
    ]
    expected = sorted(path.name for path in (DOCS / "runbooks").glob("*.md") if path != index)
    if sorted(linked) != expected:
        errors.append("docs/runbooks/index.md: list each retained runbook exactly once")


def _check_akmon_terminology(errors: list[str]) -> None:
    inventory = _load_inventory()
    if inventory is not None:
        paths = ("README.md", "start/concepts.md", "use/sources/index.md")
    else:
        paths = ("README.md", "get-started/README.md", "sources/README.md")
    for relative in paths:
        text = (DOCS / relative).read_text(encoding="utf-8").lower()
        if "ingestion" in text and "import" not in text:
            errors.append(f"docs/{relative}: explain ingestion as import")


def _check_inventory(errors: list[str]) -> None:
    inventory = _load_inventory()
    if inventory is None:
        return
    if inventory.get("version") != 1:
        errors.append("documentation.yaml: version must be 1")
        return
    publications = inventory.get("publications") or {}
    if set(publications) != {"product"}:
        errors.append("documentation.yaml: product must be the only publication")
        return
    all_targets: set[str] = set()
    for name, publication in publications.items():
        source_dir = publication["source_dir"]
        root = ROOT / source_dir
        expected = set(publication.get("pages", []))
        unpublished = _unpublished(publication)
        for prefix in unpublished:
            if not prefix.endswith("/") or not (root / prefix).is_dir():
                errors.append(
                    f"documentation.yaml: unpublished entry must name an existing "
                    f"directory ending in '/': {prefix!r}"
                )
            elif not any((root / prefix).rglob("*.md")):
                errors.append(
                    f"documentation.yaml: unpublished directory holds no pages: {prefix!r}"
                )
        for path in sorted(expected):
            if _is_unpublished(path, unpublished):
                errors.append(
                    f"{source_dir}/{path}: page is both published and unpublished"
                )
        actual = {
            relative
            for relative in (
                path.relative_to(root).as_posix() for path in root.rglob("*.md")
            )
            if not _is_unpublished(relative, unpublished)
        }
        for path in sorted(expected - actual):
            errors.append(f"{source_dir}/{path}: canonical page is missing")
        for path in sorted(actual - expected):
            errors.append(f"{source_dir}/{path}: page is not owned by documentation.yaml")
        if len(expected) > int(publication["page_limit"]):
            errors.append(
                f"documentation.yaml: {name} has {len(expected)} pages, "
                f"above its limit of {publication['page_limit']}"
            )
        for page in expected:
            target = f"{source_dir}/{page}"
            if target in all_targets:
                errors.append(f"documentation.yaml: duplicate page target {target}")
            all_targets.add(target)
        for asset in publication.get("assets", []):
            asset_path = asset["path"] if isinstance(asset, dict) else asset
            target = f"{source_dir}/{asset_path}"
            if target in all_targets:
                errors.append(f"documentation.yaml: duplicate asset target {target}")
            all_targets.add(target)
    for gated in inventory.get("gated_pages", []):
        path = ROOT / gated["path"]
        if path.exists():
            errors.append(
                f"{gated['path']}: gated page was published before its promotion gate"
            )
    unpublished_targets = tuple(
        prefix
        for publication in publications.values()
        for prefix in _unpublished(publication)
    )
    for old_path, target in (inventory.get("redirects") or {}).items():
        if _is_unpublished(target.lstrip("/"), unpublished_targets):
            errors.append(
                f"documentation.yaml: redirect target is unpublished: "
                f"{old_path!r} -> {target!r}"
            )
            continue
        if old_path.startswith("/") or target.endswith("/"):
            errors.append(
                f"documentation.yaml: redirect paths must identify Markdown owners: "
                f"{old_path!r} -> {target!r}"
            )
            continue
        destination = ROOT / "docs" / target.lstrip("/")
        if not destination.is_file():
            errors.append(
                f"documentation.yaml: redirect target does not exist: "
                f"{old_path!r} -> {target!r}"
            )


def check() -> list[str]:
    errors: list[str] = []
    _check_inventory(errors)
    _check_retired_paths(errors)
    _check_front_matter(errors)
    _check_mkdocs_nav(errors)
    _check_all_active_links(errors)
    _check_duplicate_instructions(errors)
    _check_retired_terms(errors)
    _check_runbook_routing_and_recovery(errors)
    _check_akmon_terminology(errors)
    return errors


def main() -> int:
    errors = check()
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        print(f"docs-site contract: {len(errors)} error(s)", file=sys.stderr)
        return 1
    print(f"docs-site contract: ok ({len(_active_doc_paths())} pages)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
