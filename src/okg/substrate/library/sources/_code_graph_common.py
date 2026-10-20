"""Shared helpers + Python AST analyzer for code_graph_*.py.

Extracted from ``library/sources/code_graph.py`` by Wave D-27
(2026-05) per ``extract-code-graph-dataflow``. Leading
underscore in the filename: this module is an internal base
that the public ``code_graph_*`` source modules import from.
YAML manifests never reference it directly; they always go
through ``library.sources.code_graph.<ClassName>``.

Contents:
  - 11 utility functions used across every code-graph source
    (`_sha`, `_dedupe_facts`, `_rel`, `_file_id`, `_symbol_id`,
    `_module_qname`, `_language_for_path`,
    `_ensure_tree_sitter_registered`, `_glob_match`,
    `_walk_files`, `_read_text`).
  - `_Symbol` dataclass + `_PythonAnalyzer` AST visitor +
    `_call_name` + `_analyze_python` (the Python AST analysis
    layer used by `CodeStructureSource`, `InterfaceSource`,
    and `CodeDataFlowSource`).
"""
from __future__ import annotations

import ast
import fnmatch
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence
from urllib.parse import quote

from okg.substrate.library.sources.base import EdgeFact, NodeFact


__all__ = [
    "CODE_LANGUAGE_BY_EXT",
    "DEFAULT_CODE_EXTS",
    "DEFAULT_EXCLUDES",
    "_sha",
    "_dedupe_facts",
    "_rel",
    "_file_id",
    "_symbol_id",
    "_module_qname",
    "_language_for_path",
    "_ensure_tree_sitter_registered",
    "_glob_match",
    "_walk_files",
    "_read_text",
    "_Symbol",
    "_PythonAnalyzer",
    "_call_name",
    "_analyze_python",
    "_literal_str",
]


CODE_LANGUAGE_BY_EXT = {
    ".py": "python",
    ".js": "javascript",
    ".jsx": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".go": "go",
    ".rs": "rust",
    ".c": "c",
    ".h": "c",
    ".cc": "cpp",
    ".cpp": "cpp",
    ".cxx": "cpp",
    ".hh": "cpp",
    ".hpp": "cpp",
}
DEFAULT_CODE_EXTS = tuple(sorted(CODE_LANGUAGE_BY_EXT))
DEFAULT_EXCLUDES = (
    "__pycache__/**",
    "**/__pycache__/**",
    ".venv/**",
    "**/.venv/**",
    ".agents/**",
    "**/.agents/**",
    "node_modules/**",
    "**/node_modules/**",
    ".git/**",
    "**/.git/**",
    ".mypy_cache/**",
    "**/.mypy_cache/**",
    ".pytest_cache/**",
    "**/.pytest_cache/**",
    ".claude/**",
    "data/**",
    ".scratch/**",
    ".tmp/**",
    ".artifacts/**",
)



def _symbol_scope_prefix(repo: str, path: str) -> str:
    """An injective `<repo>:<relative path>::` prefix for a symbol name.

    The repository and path are percent-quoted so a `:` inside either one
    cannot forge another file's prefix. The path is relative to the
    repository root, never absolute, so the prefix is stable across machines.
    """
    path = path.replace("\\", "/").removeprefix("./")
    return f"{quote(repo, safe='')}:{quote(path, safe='/')}::"


def _archive_symbol_prefix(repo: str, path: str) -> str:
    """An injective repository/path prefix for archived code's alias names."""
    path = path.replace("\\", "/").removeprefix("./")
    if not path.startswith(("archive/", "pact/archive/")):
        return ""
    return _symbol_scope_prefix(repo, path)


def _scoped_symbol_name(repo: str, path: str, qualified_name: str) -> str:
    """The unique alias key for a code symbol (issue #1747).

    `qualified_name` alone repeats across files (`pkg/util.ts` and
    `pkg/util.js` both define `helper`), so it cannot be an alias. Scoping the
    RAW (unprefixed) symbol name by repository and relative path makes the key
    unique across the crawled scope.
    """
    return _symbol_scope_prefix(repo, path) + qualified_name


def _lexical_symbol_name(repo: str, path: str, qualified_name: str) -> str:
    """Keep lexical lookup working alongside archived alias qualification."""
    prefix = _archive_symbol_prefix(repo, path)
    return qualified_name.removeprefix(prefix) if prefix else qualified_name


def _sha(text: str, n: int = 16) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:n]


def _dedupe_facts(facts: list[NodeFact | EdgeFact]) -> list[NodeFact | EdgeFact]:
    out: list[NodeFact | EdgeFact] = []
    seen_nodes: set[tuple[str, str, str, str]] = set()
    seen_edges: set[tuple[str, str, str, str, str]] = set()
    for fact in facts:
        revision = json.dumps(fact.source_revision, sort_keys=True, default=str)
        if isinstance(fact, NodeFact):
            key = (fact.node_id, fact.subtype, revision, fact.op)
            if key in seen_nodes:
                continue
            seen_nodes.add(key)
            out.append(fact)
            continue
        key = (fact.src, fact.edge_type, fact.dst, revision, fact.op)
        if key in seen_edges:
            continue
        seen_edges.add(key)
        out.append(fact)
    return out


def _rel(repo_root: Path, path: Path) -> str:
    return path.relative_to(repo_root).as_posix()


def _file_id(repo: str, rel_path: str) -> str:
    return f"file:{repo}:{rel_path}"


def _symbol_id(repo: str, rel_path: str, qname: str, line: int) -> str:
    return f"symbol:{repo}:{rel_path}:{qname}:{line}"


def _module_qname(rel_path: str) -> str:
    stem = rel_path.rsplit(".", 1)[0]
    return stem.replace("/", ".").replace("\\", ".")


def _language_for_path(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in CODE_LANGUAGE_BY_EXT:
        return CODE_LANGUAGE_BY_EXT[ext]
    # Treat configured unknown extensions as configured languages.
    # `code_extractor_for()` will raise CodeExtractorNotFound with the
    # registry's fix hint instead of silently accepting the file.
    return ext.lstrip(".")


def _ensure_tree_sitter_registered(language: str) -> None:
    if language == "python":
        return
    # Importing the module registers the shipped tree-sitter extractors.
    # It does not import tree_sitter_language_pack until a specific
    # extractor is constructed.
    from okg.substrate.library.extraction.code import treesitter  # noqa: F401


def _glob_match(rel_path: str, patterns: Sequence[str]) -> bool:
    return any(fnmatch.fnmatch(rel_path, pat) for pat in patterns)


def _carry_cursor(
    cursor: Optional[dict[str, Any]],
    *,
    scoped: bool,
    authoritative_replay: bool,
) -> Optional[dict[str, Any]]:
    """The cursor a code-graph run may carry prior records from.

    An unscoped authoritative replay -- which the runner forces when the
    registry binding changes, a new include or exclude glob among them --
    must re-read the whole configured scope and declare that as the complete
    footprint. Carrying the prior record set forward would keep every record
    this read no longer produces, such as a file a new exclude glob now
    matches, and the runner's record-set diff would never retract it (issue
    #2483). The runner still holds the prior authority and diffs against it.
    """
    if authoritative_replay and not scoped:
        return None
    return cursor


def _glob_match_dir(rel_path: str, patterns: Sequence[str]) -> bool:
    rel = rel_path.replace("\\", "/").rstrip("/")
    if not rel or rel == ".":
        return False
    for pattern in patterns:
        pat = str(pattern).replace("\\", "/")
        if fnmatch.fnmatch(rel, pat) or fnmatch.fnmatch(f"{rel}/", pat):
            return True
        stem = pat.rstrip("/")
        if not stem.endswith("/**"):
            continue
        stem = stem[:-3].rstrip("/")
        candidates = (stem[3:],) if stem.startswith("**/") else ()
        if fnmatch.fnmatch(rel, stem) or any(
            fnmatch.fnmatch(rel, candidate) for candidate in candidates
        ):
            return True
    return False


def _scope_key_in_walk(
    rel_path: str,
    *,
    include_exts: Sequence[str],
    exclude_globs: Sequence[str],
) -> bool:
    """Whether the full walk (`_walk_files`) would select this path.

    A scoped run narrows the full walk to watcher-supplied paths; it must
    never widen it (issue #3259). So a scope key passes exactly the filters
    the walk applies: the extension allowlist, a file-level exclude match,
    and the directory pruning the walk does before it descends -- which also
    catches directory-style patterns such as ``**/gen/**`` on a top-level
    ``gen/`` or a bare ``vendor``, which a file-level ``fnmatch`` misses.
    The byte-size cap is not a path filter and is not applied here.
    """
    import os

    rel = rel_path.replace("\\", "/").strip("/")
    if not rel:
        return False
    include = {e.lower() for e in include_exts}
    if os.path.splitext(rel)[1].lower() not in include:
        return False
    if _glob_match(rel, exclude_globs):
        return False
    parts = rel.split("/")[:-1]
    for depth in range(1, len(parts) + 1):
        if _glob_match_dir("/".join(parts[:depth]), exclude_globs):
            return False
    return True


def _root_spellings(repo_root: Path) -> tuple[str, ...]:
    """The root as given (absolute) and resolved."""
    root = Path(repo_root)
    return tuple(
        dict.fromkeys(
            base.as_posix().rstrip("/")
            for base in (root.absolute(), root.resolve())
        )
    )


def _scope_key_relative(
    repo_root: Path,
    key: object,
    *,
    root_spellings: Optional[tuple[str, ...]] = None,
) -> Optional[str]:
    """Repo-relative posix form of a scope key, or None when it names none.

    `./a` and `a//b` name `a`. An absolute key names its path relative to
    `repo_root` (as spelled, or resolved) when it lies inside; otherwise, or
    when a relative key climbs out, it names nothing in this source.
    """
    import posixpath

    if key is None:
        return None
    raw = str(key).replace("\\", "/")
    if not raw:
        return None
    if raw.startswith("/"):
        absolute = posixpath.normpath(raw)
        bases = root_spellings or _root_spellings(Path(repo_root))

        def _strip(spelling: str) -> Optional[str]:
            for base in bases:
                if spelling.startswith(base + "/"):
                    return spelling[len(base) + 1:]
            return None

        # As spelled first, so a key through an in-repo symlinked directory
        # keeps that name (and is then dropped as the walk drops it). Only a
        # key spelled through another alias of the root -- `/var/...` for a
        # root at `/private/var/...` -- needs its parent resolved, and only
        # then is it resolved; the file name itself is never resolved, so a
        # symlinked file keeps its name.
        stripped = _strip(absolute)
        if stripped is None:
            resolved_parent = Path(absolute).parent.resolve().as_posix()
            stripped = _strip(
                posixpath.join(resolved_parent, posixpath.basename(absolute)),
            )
        if stripped is None:
            return None
        raw = stripped
    rel = posixpath.normpath(raw)
    if rel in ("", ".", "..") or rel.startswith("../"):
        return None
    return rel


def _git_snapshot_readable_keys(
    repo_root: Path,
    rel_keys: set[str],
) -> set[str]:
    """The keys a git-visible snapshot full run would read (see
    `in_proc_git_visible_paths`): a clean tracked symlink and a gitignored
    file are dropped, so a scoped run does not emit a record the full run
    retracts. A repository with no commits yet has no tree to mirror; its
    keys are returned as given. Any other HEAD read failure raises."""
    from okg.substrate.library.sources._git_helpers import (
        _r,
        in_proc_git_visible_paths,
    )

    if _r(Path(repo_root)).head_is_unborn:
        return set(rel_keys)
    return in_proc_git_visible_paths(Path(repo_root), rel_keys)


def _scope_record_keys_in_walk(
    repo_root: Path,
    scope_keys: Sequence[str],
    *,
    include_exts: Sequence[str],
    exclude_globs: Sequence[str],
) -> set[str]:
    """Repo-relative record keys a scoped code-graph run governs.

    Keys that climb out of `repo_root` or that the full walk would not
    select are dropped: the scoped run neither reads nor retracts them.

    A key is judged as the path `os.walk` would report, not as its resolved
    target: a symlinked file is governed under its own name (as the walk
    lists it), and a path below a symlinked directory is dropped (the walk
    does not descend into one). Resolving the key used to govern a link as
    its target, a record the full walk keys under the link's name. An
    absolute key inside `repo_root` names its repo-relative path; any other
    absolute key is outside.

    This is the os.walk rule, which code-structure and code-lsp-resolution
    use on every repository. On a git repository, interfaces and
    code-data-flow read a git-visible snapshot instead; they keep these keys
    as the retraction set but read only the subset `in_proc_git_visible_paths`
    returns (a clean tracked symlink and a gitignored file are not read).
    """
    root = Path(repo_root)
    out: set[str] = set()
    # Once per call: thousands of keys share a root and parent directories.
    root_spellings: Optional[tuple[str, ...]] = None
    symlinked: dict[str, bool] = {}

    def _is_symlinked_dir(prefix: str) -> bool:
        if prefix not in symlinked:
            symlinked[prefix] = (root / prefix).is_symlink()
        return symlinked[prefix]

    for key in scope_keys:
        if root_spellings is None and str(key).replace("\\", "/").startswith("/"):
            root_spellings = _root_spellings(root)
        rel_str = _scope_key_relative(root, key, root_spellings=root_spellings)
        if rel_str is None:
            continue
        parts = rel_str.split("/")
        if any(
            _is_symlinked_dir("/".join(parts[:depth]))
            for depth in range(1, len(parts))
        ):
            continue
        if _scope_key_in_walk(
            rel_str,
            include_exts=include_exts,
            exclude_globs=exclude_globs,
        ):
            out.add(rel_str)
    return out


def _scope_keys_outside_walk(
    repo_root: Path,
    scope_keys: Sequence[str],
    *,
    include_exts: Sequence[str],
    exclude_globs: Sequence[str],
) -> int:
    """How many distinct scope keys fall outside the source's paths."""
    keys = tuple(dict.fromkeys(scope_keys))
    admitted = _scope_record_keys_in_walk(
        repo_root,
        keys,
        include_exts=include_exts,
        exclude_globs=exclude_globs,
    )
    root = Path(repo_root)
    root_spellings = (
        _root_spellings(root)
        if any(str(key).replace("\\", "/").startswith("/") for key in keys)
        else None
    )
    return sum(
        1
        for key in keys
        if _scope_key_relative(root, key, root_spellings=root_spellings)
        not in admitted
    )


def _walk_files(
    repo_root: Path,
    *,
    include_exts: Sequence[str],
    exclude_globs: Sequence[str],
    max_files: int,
    max_bytes_per_file: int,
) -> list[Path]:
    """Enumerate files under repo_root matching include_exts /
    not matching exclude_globs / under max_bytes_per_file.

    Per `shrink-source-walk-cost` per-thread profile: a previous
    `rglob('*')` + `path.is_file()` shape stat'd every entry in
    the tree (both files AND directories) just to filter down to
    the matching extension. For palimpzest that's 540 stats to
    keep 163 .py files — ~400 ms of redundant stat work across
    the concurrent source threads.
    """
    import os

    out: list[Path] = []
    include = {e.lower() for e in include_exts}
    repo_root_str = str(repo_root)
    for dirpath, dirnames, filenames in os.walk(repo_root):
        dirnames[:] = [
            dirname
            for dirname in dirnames
            if not _glob_match_dir(
                os.path.relpath(os.path.join(dirpath, dirname), repo_root_str),
                exclude_globs,
            )
        ]
        # Sort filenames for stable ordering (preserves the prior
        # rglob-sorted semantics).
        for fname in sorted(filenames):
            # Cheap extension check — no stat. The `splitext`
            # gives `.py` (lowercase) for `foo.py`; matches the
            # set we built above.
            ext = os.path.splitext(fname)[1].lower()
            if ext not in include:
                continue
            full = os.path.join(dirpath, fname)
            rel = os.path.relpath(full, repo_root_str).replace("\\", "/")
            if _glob_match(rel, exclude_globs):
                continue
            # Size check is the only stat we MUST do — it's needed
            # for the max_bytes_per_file guard.
            try:
                if os.path.getsize(full) > max_bytes_per_file:
                    continue
            except OSError:
                continue
            out.append(Path(full))
            if len(out) > max_files:
                raise ValueError(
                    f"code graph source exceeded max_files={max_files}; "
                    "tighten exclude_globs or raise the cap"
                )
    # Sort to preserve deterministic order across runs (matches
    # the prior sorted(rglob(...)) behavior).
    out.sort()
    return out


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


@dataclass(frozen=True)
class _Symbol:
    symbol_id: str
    name: str
    qname: str
    kind: str
    start_line: int
    end_line: int
    parent_symbol_id: Optional[str] = None
    signature: str = ""
    docstring: str = ""


class _PythonAnalyzer(ast.NodeVisitor):
    def __init__(self, *, repo: str, rel_path: str) -> None:
        self.repo = repo
        self.rel_path = rel_path
        self.stack: list[_Symbol] = []
        self.symbols: list[_Symbol] = []
        self.imports: list[dict[str, Any]] = []
        self.references: list[dict[str, Any]] = []
        self.name_to_symbol: dict[str, _Symbol] = {}
        # The module AST this analyzer visited. `_analyze_python` already
        # parses the text; publishing the tree lets a caller that needs it
        # reuse this parse instead of compiling the same file a second time
        # (issue #1325 item 3).
        self.tree: ast.AST | None = None

    def visit_ClassDef(self, node: ast.ClassDef) -> Any:
        self._enter_symbol(node, "class")

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        self._enter_symbol(node, "method" if self.stack and self.stack[-1].kind == "class" else "function")

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
        self._enter_symbol(node, "method" if self.stack and self.stack[-1].kind == "class" else "function")

    def visit_Import(self, node: ast.Import) -> Any:
        for alias in node.names:
            self.imports.append({
                "module": alias.name,
                "name": "",
                "alias": alias.asname or "",
                "kind": "import",
                "start_line": node.lineno,
                "end_line": getattr(node, "end_lineno", node.lineno),
                "start_col": getattr(node, "col_offset", 0),
                "end_col": getattr(node, "end_col_offset", 0),
            })

    def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
        module = "." * int(node.level or 0) + (node.module or "")
        for alias in node.names:
            self.imports.append({
                "module": module,
                "name": alias.name,
                "alias": alias.asname or "",
                "kind": "from_import",
                "start_line": node.lineno,
                "end_line": getattr(node, "end_lineno", node.lineno),
                "start_col": getattr(node, "col_offset", 0),
                "end_col": getattr(node, "end_col_offset", 0),
            })

    def visit_Call(self, node: ast.Call) -> Any:
        name = _call_name(node.func)
        if name:
            enclosing = self.stack[-1] if self.stack else None
            self.references.append({
                "name": name.split(".")[-1],
                "qualified_name": name,
                "reference_kind": "call",
                "enclosing_symbol_id": enclosing.symbol_id if enclosing else "",
                "start_line": node.lineno,
                "end_line": getattr(node, "end_lineno", node.lineno),
                "start_col": getattr(node, "col_offset", 0),
                "end_col": getattr(node, "end_col_offset", 0),
            })
        self.generic_visit(node)

    def _enter_symbol(
        self, node: ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef,
        kind: str,
    ) -> None:
        qname = ".".join([*(s.name for s in self.stack), node.name])
        sid = _symbol_id(self.repo, self.rel_path, qname, node.lineno)
        args = getattr(node, "args", None)
        sig = ""
        if args is not None:
            sig = "(" + ", ".join(a.arg for a in args.args) + ")"
        sym = _Symbol(
            symbol_id=sid,
            name=node.name,
            qname=qname,
            kind=kind,
            start_line=node.lineno,
            end_line=getattr(node, "end_lineno", node.lineno),
            parent_symbol_id=self.stack[-1].symbol_id if self.stack else None,
            signature=sig,
            docstring=ast.get_docstring(node) or "",
        )
        self.symbols.append(sym)
        self.name_to_symbol.setdefault(node.name, sym)
        self.name_to_symbol.setdefault(qname, sym)
        self.stack.append(sym)
        self.generic_visit(node)
        self.stack.pop()


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _call_name(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return ""


def _analyze_python(repo: str, rel_path: str, text: str) -> _PythonAnalyzer:
    tree = ast.parse(text)
    analyzer = _PythonAnalyzer(repo=repo, rel_path=rel_path)
    analyzer.tree = tree
    module_qname = rel_path[:-3].replace("/", ".") if rel_path.endswith(".py") else rel_path.replace("/", ".")
    # Capture the module-level docstring so cross-link rules like
    # `code_symbol_implements_concept` can match `@implements_concept`
    # tags written at the TOP of a module (not just on classes /
    # functions). Per dogfood-canary follow-up (2026-05-18):
    # module-level tags were previously silently ignored because the
    # module Symbol carried `docstring=""`.
    module_docstring = ast.get_docstring(tree) or ""
    module = _Symbol(
        symbol_id=_symbol_id(repo, rel_path, module_qname, 1),
        name=Path(rel_path).stem,
        qname=module_qname,
        kind="module",
        start_line=1,
        end_line=max(1, len(text.splitlines())),
        docstring=module_docstring,
    )
    analyzer.symbols.append(module)
    analyzer.name_to_symbol[module_qname] = module
    analyzer.visit(tree)
    return analyzer


def _literal_str(node: ast.AST) -> str:
    """Return the string value of an `ast.Constant` string literal,
    or `""` for any other node. Used by both `InterfaceSource`
    (decorator-arg / kw-arg extraction) and `CodeDataFlowSource`
    (open-mode kw-arg + similar string-literal scrapes)."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return ""
