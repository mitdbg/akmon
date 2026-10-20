"""Pure, bounded discovery for local LaTeX projects.

The manifest builder never executes TeX, expands macros, or reads outside the
configured project directory.  It is intentionally independent of graph writes so
DocCorpusSource, BibReferencesSource, and LatexProjectSource can agree on one byte-
and-configuration-pinned project scope before any of them emits facts.
"""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Iterator, Mapping, Sequence

from okg.substrate.library.sources._file_walk import (
    FileContainmentError,
    FileReadLimitError,
)


DEFAULT_MAX_FILES = 512
DEFAULT_MAX_DEPTH = 64
DEFAULT_MAX_FILE_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_TOTAL_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_INVENTORY_FILES = 4096
DEFAULT_MAX_INVENTORY_TOTAL_BYTES = 512 * 1024 * 1024

_TEXT_SUFFIXES = frozenset({
    ".tex", ".sty", ".cls", ".bst", ".bbx", ".cbx", ".lbx",
    ".dbx", ".def", ".dtx", ".ins",
})
_SUPPORT_SUFFIXES = frozenset(_TEXT_SUFFIXES - {".tex"})
_GENERATED_SUFFIXES = frozenset({
    ".aux", ".bbl", ".blg", ".fls", ".fdb_latexmk", ".log",
    ".out", ".toc", ".lof", ".lot", ".nav", ".snm", ".vrb",
})
_GENERATED_DIRS = frozenset({
    ".latexmk", "latex.out", "build", "dist", "tmp", ".tmp", "_build",
})
_GRAPHIC_EXTENSIONS = ("", ".pdf", ".png", ".jpg", ".jpeg", ".svg", ".eps")
_CODE_ENVS = frozenset({"verbatim", "verbatim*", "Verbatim", "lstlisting", "minted"})
_COMMAND_RE = re.compile(r"\\(?P<name>[A-Za-z@]+)\*?")
_URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


@dataclass(frozen=True, order=True)
class LatexProjectWarning:
    source_path: str
    start: int
    code: str
    literal: str = ""
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "source_path": self.source_path,
            "start": self.start,
            "literal": self.literal,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class LatexDependencyOccurrence:
    source_path: str
    command: str
    role: str
    start: int
    end: int
    literal: str
    resolved_path: str | None
    status: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_path": self.source_path,
            "command": self.command,
            "role": self.role,
            "start": self.start,
            "end": self.end,
            "literal": self.literal,
            "resolved_path": self.resolved_path,
            "status": self.status,
        }


@dataclass(frozen=True)
class LatexManifestFile:
    path: str
    content_sha256: str
    byte_count: int
    roles: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "content_sha256": self.content_sha256,
            "byte_count": self.byte_count,
            "roles": list(self.roles),
        }


@dataclass(frozen=True)
class LatexManifestProject:
    project_dir: str
    root_path: str
    paper_id: str
    files: tuple[LatexManifestFile, ...]
    dependencies: tuple[LatexDependencyOccurrence, ...]
    warnings: tuple[LatexProjectWarning, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "project_dir": self.project_dir,
            "root_path": self.root_path,
            "paper_id": self.paper_id,
            "files": [item.as_dict() for item in self.files],
            "dependencies": [item.as_dict() for item in self.dependencies],
            "warnings": [item.as_dict() for item in self.warnings],
        }


@dataclass(frozen=True)
class LatexProjectManifest:
    repository_authority: str
    project_configuration: tuple[LatexProjectSpec, ...]
    limits: tuple[tuple[str, int], ...]
    include_generated_paths: tuple[str, ...]
    generated_dirs: tuple[str, ...]
    projects: tuple[LatexManifestProject, ...]
    source_inventory: tuple[LatexManifestFile, ...]
    warnings: tuple[LatexProjectWarning, ...]
    digest: str

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "repository_authority": self.repository_authority,
            "configuration": {
                "projects": [
                    {
                        "project_dir": spec.project_dir,
                        "entrypoints": list(spec.entrypoints),
                        "entrypoint_mode": "explicit" if spec.explicit else "automatic",
                    }
                    for spec in self.project_configuration
                ],
                "include_generated_paths": list(self.include_generated_paths),
                "generated_dirs": list(self.generated_dirs),
            },
            "limits": dict(self.limits),
            "projects": [project.as_dict() for project in self.projects],
            "source_inventory": [item.as_dict() for item in self.source_inventory],
            "warnings": [warning.as_dict() for warning in self.warnings],
        }


@dataclass(frozen=True)
class LatexProjectSpec:
    project_dir: str
    entrypoints: tuple[str, ...] = ()
    explicit: bool = False


@dataclass(frozen=True)
class _Argument:
    value: str
    start: int
    end: int


@dataclass(frozen=True)
class _Request:
    command: str
    role: str
    literal: str
    start: int
    end: int
    base_dirs: tuple[Path, ...]
    extensions: tuple[str, ...]
    external_when_missing: bool = False


@dataclass
class LatexProjectManifestBuilder:
    source_root: str | Path
    repository_authority: str
    projects: Sequence[LatexProjectSpec | Mapping[str, Any]]
    max_files: int = DEFAULT_MAX_FILES
    max_depth: int = DEFAULT_MAX_DEPTH
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES
    max_inventory_files: int = DEFAULT_MAX_INVENTORY_FILES
    max_inventory_total_bytes: int = DEFAULT_MAX_INVENTORY_TOTAL_BYTES
    include_generated_paths: Sequence[str] = field(default_factory=tuple)
    generated_dirs: Sequence[str] = field(default_factory=tuple)
    contained_reader: Any | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        authority = _nfc(self.repository_authority).strip()
        if not authority:
            raise ValueError("repository_authority must be a non-empty string")
        self.repository_authority = authority
        self.source_root = Path(self.source_root).expanduser().resolve()
        for name in (
            "max_files", "max_depth", "max_file_bytes", "max_total_bytes",
            "max_inventory_files", "max_inventory_total_bytes",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        normalized: list[LatexProjectSpec] = []
        for raw in self.projects:
            if isinstance(raw, LatexProjectSpec):
                entrypoints = tuple(_normalize_rel(value) for value in raw.entrypoints)
                normalized.append(LatexProjectSpec(
                    project_dir=_normalize_rel(raw.project_dir),
                    entrypoints=entrypoints,
                    explicit=bool(entrypoints),
                ))
            else:
                entrypoints = tuple(
                    _normalize_rel(str(value))
                    for value in raw.get("entrypoints", ())
                )
                normalized.append(LatexProjectSpec(
                    project_dir=_normalize_rel(str(raw["project_dir"])),
                    entrypoints=entrypoints,
                    explicit=bool(entrypoints),
                ))
        if not normalized:
            raise ValueError("projects must contain at least one project specification")
        self._projects = tuple(normalized)
        custom_generated_dirs = frozenset(
            _normalize_rel(value)
            for value in self.generated_dirs
            if _nfc(value).strip()
        )
        self._generated_dirs = _GENERATED_DIRS | custom_generated_dirs
        self._generated_overrides = tuple(
            _normalize_rel(value) for value in self.include_generated_paths
        )

    def build(self) -> LatexProjectManifest:
        from okg.substrate.library.sources._file_walk import ContainedFileReader

        if self.contained_reader is not None:
            self._contained_reader = self.contained_reader
            return self._build_with_reader()
        with ContainedFileReader(self.source_root) as reader:
            self._contained_reader = reader
            try:
                return self._build_with_reader()
            finally:
                self._contained_reader = None

    def _build_with_reader(self) -> LatexProjectManifest:
        projects: list[LatexManifestProject] = []
        project_configuration: list[LatexProjectSpec] = []
        inventory: dict[str, LatexManifestFile] = {}
        top_warnings: list[LatexProjectWarning] = []
        for spec in sorted(self._projects, key=lambda item: _normalize_rel(item.project_dir)):
            project_dir, project_rel = self._project_path(spec.project_dir)
            items, inventory_warnings = self._source_inventory(project_dir)
            inventory.update((item.path, item) for item in items)
            top_warnings.extend(inventory_warnings)
            inventory_paths = {item.path for item in items}
            inventory_tex = tuple(
                self.source_root / item.path
                for item in items
                if Path(item.path).suffix.lower() == ".tex"
            )
            discovery_incomplete = not spec.entrypoints and any(
                warning.code in {
                    "latex_inventory_over_files",
                    "latex_inventory_over_total_bytes",
                }
                or (
                    warning.code == "latex_inventory_over_file_bytes"
                    and Path(warning.source_path).suffix.lower() == ".tex"
                )
                for warning in inventory_warnings
            )
            if discovery_incomplete:
                roots = []
                warnings = [LatexProjectWarning(
                    project_rel, 0, "latex_root_discovery_incomplete", project_rel,
                )]
            else:
                roots, warnings = self._roots(
                    project_dir,
                    project_rel,
                    spec.entrypoints,
                    automatic_candidates=inventory_tex,
                )
            top_warnings.extend(warnings)
            project_configuration.append(LatexProjectSpec(
                project_dir=project_rel,
                entrypoints=tuple(_lexical_rel(root, self.source_root) for root in roots)
                if spec.entrypoints else (),
                explicit=bool(spec.entrypoints),
            ))
            for root in roots:
                root_rel = _lexical_rel(root, self.source_root)
                if root_rel not in inventory_paths:
                    top_warnings.append(LatexProjectWarning(
                        root_rel, 0, "latex_root_not_in_inventory", root_rel,
                    ))
                    continue
                project = self._build_project(
                    project_dir,
                    project_rel,
                    root,
                    inventory_paths=inventory_paths,
                )
                if not any("root" in item.roles for item in project.files):
                    top_warnings.extend(project.warnings)
                    continue
                projects.append(project)
        projects.sort(key=lambda item: (item.project_dir, item.root_path))
        top_warnings.sort()
        limits = (
            ("max_files", self.max_files),
            ("max_depth", self.max_depth),
            ("max_file_bytes", self.max_file_bytes),
            ("max_total_bytes", self.max_total_bytes),
            ("max_inventory_files", self.max_inventory_files),
            ("max_inventory_total_bytes", self.max_inventory_total_bytes),
        )
        manifest = LatexProjectManifest(
            repository_authority=self.repository_authority,
            project_configuration=tuple(project_configuration),
            limits=limits,
            include_generated_paths=tuple(sorted(self._generated_overrides)),
            generated_dirs=tuple(sorted(self._generated_dirs)),
            projects=tuple(projects),
            source_inventory=tuple(inventory[key] for key in sorted(inventory)),
            warnings=tuple(top_warnings),
            digest="",
        )
        digest = hashlib.sha256(_canonical_json(manifest.canonical_payload())).hexdigest()
        return LatexProjectManifest(
            repository_authority=self.repository_authority,
            project_configuration=manifest.project_configuration,
            limits=manifest.limits,
            include_generated_paths=manifest.include_generated_paths,
            generated_dirs=manifest.generated_dirs,
            projects=tuple(projects),
            source_inventory=tuple(inventory[key] for key in sorted(inventory)),
            warnings=tuple(top_warnings),
            digest=digest,
        )

    def _project_path(self, configured: str) -> tuple[Path, str]:
        path = Path(configured).expanduser()
        if not path.is_absolute():
            path = self.source_root / path
        resolved = path.resolve()
        if not _is_within(resolved, self.source_root):
            raise ValueError(f"project_dir escapes source_root: {configured!r}")
        try:
            self._secure_stat(path, directory=True)
        except FileContainmentError:
            raise
        except OSError as exc:
            raise ValueError(
                f"project_dir is not a directory: {configured!r}"
            ) from exc
        return resolved, _rel(resolved, self.source_root)

    def _source_inventory(
        self,
        project_dir: Path,
    ) -> tuple[list[LatexManifestFile], list[LatexProjectWarning]]:
        items: list[LatexManifestFile] = []
        warnings: list[LatexProjectWarning] = []
        total_bytes = 0
        seen: set[str] = set()
        candidates = [
            path for path in sorted(project_dir.rglob("*"))
            if path.suffix.lower() in (_TEXT_SUFFIXES | {".bib"})
        ]
        for lexical_path in candidates:
            lexical_rel = _nfc(lexical_path.relative_to(self.source_root).as_posix())
            try:
                path = lexical_path.resolve(strict=False)
            except (OSError, RuntimeError) as exc:
                warnings.append(LatexProjectWarning(
                    lexical_rel, 0, "latex_inventory_symlink_cycle",
                    lexical_rel, type(exc).__name__,
                ))
                continue
            if not _is_within(path, project_dir):
                if not _is_within(path, self.source_root):
                    raise FileContainmentError(
                        f"LaTeX inventory target escapes source root: {lexical_rel}"
                    )
                warnings.append(LatexProjectWarning(
                    lexical_rel, 0, "latex_inventory_escape", lexical_rel,
                ))
                continue
            if self._is_generated(path, project_dir):
                continue
            if lexical_rel in seen:
                continue
            seen.add(lexical_rel)
            rel = lexical_rel
            if len(items) + 1 > self.max_inventory_files:
                warnings.append(LatexProjectWarning(
                    rel, 0, "latex_inventory_over_files", rel,
                ))
                break
            try:
                content = self._read_bytes(lexical_path)
            except FileReadLimitError:
                warnings.append(LatexProjectWarning(
                    rel, 0, "latex_inventory_over_file_bytes", rel,
                ))
                continue
            except FileContainmentError:
                raise
            except FileNotFoundError:
                warnings.append(LatexProjectWarning(
                    rel, 0, "latex_inventory_missing", rel,
                ))
                continue
            size = len(content)
            if size > self.max_file_bytes:
                warnings.append(LatexProjectWarning(
                    rel, 0, "latex_inventory_over_file_bytes", rel,
                ))
                continue
            if total_bytes + size > self.max_inventory_total_bytes:
                warnings.append(LatexProjectWarning(
                    rel, 0, "latex_inventory_over_total_bytes", rel,
                ))
                break
            total_bytes += size
            items.append(LatexManifestFile(
                path=rel,
                content_sha256=hashlib.sha256(content).hexdigest(),
                byte_count=size,
                roles=("source_inventory",),
            ))
        return items, warnings

    def _roots(
        self,
        project_dir: Path,
        project_rel: str,
        entrypoints: Sequence[str],
        *,
        automatic_candidates: Sequence[Path] = (),
    ) -> tuple[list[Path], list[LatexProjectWarning]]:
        warnings: list[LatexProjectWarning] = []
        if entrypoints:
            roots: list[Path] = []
            seen: set[Path] = set()
            for value in entrypoints:
                lexical_candidate = project_dir / value
                if lexical_candidate.suffix == "":
                    lexical_candidate = lexical_candidate.with_suffix(".tex")
                try:
                    candidate = lexical_candidate.resolve()
                except (OSError, RuntimeError) as exc:
                    warnings.append(LatexProjectWarning(
                        project_rel, 0, "latex_root_symlink_cycle", str(value),
                        type(exc).__name__,
                    ))
                    continue
                if not _is_within(candidate, project_dir):
                    warnings.append(LatexProjectWarning(
                        project_rel, 0, "latex_root_escape", str(value),
                    ))
                    continue
                if self._is_generated(candidate, project_dir):
                    warnings.append(LatexProjectWarning(
                        project_rel, 0, "latex_root_excluded", str(value),
                    ))
                    continue
                try:
                    self._secure_stat(lexical_candidate)
                except FileNotFoundError:
                    warnings.append(LatexProjectWarning(
                        project_rel, 0, "latex_root_missing", str(value),
                    ))
                    continue
                if candidate not in seen:
                    seen.add(candidate)
                    roots.append(lexical_candidate)
            return sorted(roots), sorted(warnings)

        tex_files: list[Path] = []
        seen: set[Path] = set()
        for lexical_path in sorted(automatic_candidates):
            lexical_rel = _rel(lexical_path, self.source_root)
            try:
                path = lexical_path.resolve(strict=False)
            except (OSError, RuntimeError) as exc:
                warnings.append(LatexProjectWarning(
                    lexical_rel, 0, "latex_root_symlink_cycle", lexical_rel,
                    type(exc).__name__,
                ))
                continue
            if not _is_within(path, project_dir):
                warnings.append(LatexProjectWarning(
                    lexical_rel, 0, "latex_root_escape", lexical_rel,
                ))
                continue
            if self._is_generated(path, project_dir):
                continue
            try:
                self._secure_stat(lexical_path)
            except FileNotFoundError:
                continue
            if path in seen:
                continue
            seen.add(path)
            tex_files.append(path)
        tex_files.sort()
        candidates: list[Path] = []
        adjacency: dict[Path, set[Path]] = {path: set() for path in tex_files}
        known = set(tex_files)
        for path in tex_files:
            try:
                raw = self._read_text(path)
            except FileContainmentError:
                raise
            except (UnicodeDecodeError, OSError, ValueError):
                continue
            requests = tuple(self._requests(raw, path, project_dir))
            if any(
                request.command == "documentclass"
                and request.role == "class"
                and _is_literal(request.literal)
                for request in requests
            ):
                candidates.append(path)
            for request in requests:
                if request.role != "input":
                    continue
                resolved, status = self._resolve_request(request, project_dir)
                if status == "resolved" and resolved in known:
                    adjacency[path].add(resolved)
        if not candidates:
            warnings.append(LatexProjectWarning(
                project_rel, 0, "latex_root_missing", project_rel,
            ))
            return [], sorted(warnings)

        reachable = {candidate: _reachable(candidate, adjacency) for candidate in candidates}
        groups: list[set[Path]] = []
        remaining = set(candidates)
        while remaining:
            seed = min(remaining)
            group = {
                other for other in remaining
                if other in reachable[seed] and seed in reachable[other]
            } | {seed}
            groups.append(group)
            remaining -= group
        roots: list[Path] = []
        for group in groups:
            incoming = any(
                member in reachable[other]
                for other in candidates if other not in group
                for member in group
            )
            if incoming:
                continue
            selected = min(group)
            roots.append(selected)
            if len(group) > 1:
                warnings.append(LatexProjectWarning(
                    _rel(selected, self.source_root), 0, "latex_root_cycle",
                    ",".join(_rel(path, self.source_root) for path in sorted(group)),
                ))
        return sorted(roots), sorted(warnings)

    def _build_project(
        self,
        project_dir: Path,
        project_rel: str,
        root: Path,
        *,
        inventory_paths: set[str],
    ) -> LatexManifestProject:
        roles: dict[Path, set[str]] = {}
        file_info: dict[Path, tuple[str, int]] = {}
        selected_order: list[Path] = []
        dependencies: list[LatexDependencyOccurrence] = []
        warnings: list[LatexProjectWarning] = []
        total_bytes = 0
        active: list[Path] = []

        def select(path: Path, role: str, depth: int, occurrence: _Request | None) -> bool:
            nonlocal total_bytes
            source_path = (
                _rel(active[-1], self.source_root) if active
                else _rel(root, self.source_root)
            )
            start = occurrence.start if occurrence else 0
            literal = occurrence.literal if occurrence else _rel(path, self.source_root)
            if depth > self.max_depth:
                warnings.append(LatexProjectWarning(
                    source_path, start, "latex_dependency_over_depth", literal,
                ))
                return False
            if path in active:
                warnings.append(LatexProjectWarning(
                    source_path, start, "latex_dependency_cycle", literal,
                ))
                return False
            if path in roles:
                roles[path].add(role)
                return False
            if len(roles) + 1 > self.max_files:
                warnings.append(LatexProjectWarning(
                    source_path, start, "latex_dependency_over_files", literal,
                ))
                return False
            try:
                content = self._read_bytes(path)
                size = len(content)
            except FileReadLimitError:
                warnings.append(LatexProjectWarning(
                    source_path, start, "latex_dependency_over_file_bytes", literal,
                ))
                return False
            except FileContainmentError:
                raise
            except OSError as exc:
                warnings.append(LatexProjectWarning(
                    source_path, start, "latex_dependency_missing", literal, str(exc),
                ))
                return False
            if size > self.max_file_bytes:
                warnings.append(LatexProjectWarning(
                    source_path, start, "latex_dependency_over_file_bytes", literal,
                ))
                return False
            path_rel = _rel(path, self.source_root)
            if (
                path.suffix.lower() in (_TEXT_SUFFIXES | {".bib"})
                and path_rel not in inventory_paths
            ):
                warnings.append(LatexProjectWarning(
                    source_path, start, "latex_dependency_not_in_inventory", literal,
                ))
                return False
            if total_bytes + size > self.max_total_bytes:
                warnings.append(LatexProjectWarning(
                    source_path, start, "latex_dependency_over_total_bytes", literal,
                ))
                return False
            size = len(content)
            if size > self.max_file_bytes:
                warnings.append(LatexProjectWarning(
                    source_path, start, "latex_dependency_over_file_bytes", literal,
                ))
                return False
            if total_bytes + size > self.max_total_bytes:
                warnings.append(LatexProjectWarning(
                    source_path, start, "latex_dependency_over_total_bytes", literal,
                ))
                return False
            total_bytes += size
            roles[path] = {role}
            selected_order.append(path)
            file_info[path] = (hashlib.sha256(content).hexdigest(), size)
            if path.suffix.lower() not in _TEXT_SUFFIXES:
                return True
            try:
                text = content.decode("utf-8", errors="strict")
            except UnicodeDecodeError as exc:
                warnings.append(LatexProjectWarning(
                    _rel(path, self.source_root), exc.start,
                    "latex_dependency_invalid_utf8", _rel(path, self.source_root),
                ))
                return True
            active.append(path)
            for request in self._requests(text, path, project_dir):
                resolved, status = self._resolve_request(request, project_dir)
                resolved_rel = _rel(resolved, self.source_root) if resolved else None
                if status == "resolved" and resolved is not None:
                    if self._is_generated(resolved, project_dir):
                        status = "excluded"
                        resolved_rel = None
                    elif resolved in active:
                        status = "cycle"
                dependencies.append(LatexDependencyOccurrence(
                    source_path=_rel(path, self.source_root),
                    command=request.command,
                    role=request.role,
                    start=request.start,
                    end=request.end,
                    literal=request.literal,
                    resolved_path=resolved_rel,
                    status=status,
                ))
                if status == "resolved" and resolved is not None:
                    select(resolved, request.role, depth + 1, request)
                elif status not in {"external", "cycle"}:
                    warnings.append(LatexProjectWarning(
                        _rel(path, self.source_root), request.start,
                        f"latex_dependency_{status}", request.literal,
                    ))
                elif status == "cycle":
                    warnings.append(LatexProjectWarning(
                        _rel(path, self.source_root), request.start,
                        "latex_dependency_cycle", request.literal,
                    ))
            active.pop()
            return True

        select(root, "root", 0, None)
        manifest_files = tuple(
            LatexManifestFile(
                path=_rel(path, self.source_root),
                content_sha256=file_info[path][0],
                byte_count=file_info[path][1],
                roles=tuple(sorted(roles[path])),
            )
            for path in selected_order
        )
        warnings.sort()
        root_rel = _rel(root, self.source_root)
        paper_seed = "\0".join((self.repository_authority, project_rel, root_rel))
        paper_id = f"paper:latex:{hashlib.sha256(paper_seed.encode()).hexdigest()[:32]}"
        return LatexManifestProject(
            project_dir=project_rel,
            root_path=root_rel,
            paper_id=paper_id,
            files=manifest_files,
            dependencies=tuple(dependencies),
            warnings=tuple(warnings),
        )

    def _requests(self, text: str, source: Path, project_dir: Path) -> Iterator[_Request]:
        scan = _mask_inert(text)
        graphic_dirs: list[Path] = []
        for match in _COMMAND_RE.finditer(scan):
            if _is_escaped(scan, match.start()):
                continue
            name = match.group("name")
            pos = match.end()
            try:
                if name in {"input", "include", "subfile"}:
                    arg = _required_arg(scan, pos, "{")
                    yield self._request(
                        name, "input", arg, source.parent, ("", ".tex"),
                        project_dir=project_dir,
                    )
                elif name in {"import", "subimport"}:
                    directory = _required_arg(scan, pos, "{")
                    filename = _required_arg(scan, directory.end, "{")
                    if _is_literal(directory.value):
                        base = source.parent / directory.value.strip()
                        yield self._request(name, "input", filename, base, ("", ".tex"))
                    else:
                        yield self._dynamic_request(name, "input", directory, filename)
                elif name == "bibliography":
                    arg = _required_arg(scan, pos, "{")
                    for item in _split_list(arg):
                        yield self._request(
                            name, "bibliography", item, source.parent, ("", ".bib"),
                            project_dir=project_dir,
                        )
                elif name == "addbibresource":
                    optional, pos = _optional_arg(scan, pos)
                    arg = _required_arg(scan, pos, "{")
                    yield self._request(
                        name, "bibliography", arg, source.parent, ("", ".bib"),
                        project_dir=project_dir,
                    )
                elif name in {"documentclass", "LoadClass"}:
                    _options, pos = _optional_arg(scan, pos)
                    arg = _required_arg(scan, pos, "{")
                    yield self._request(
                        name, "class", arg, source.parent, ("", ".cls"),
                        project_dir=project_dir, external_when_missing=True,
                    )
                elif name == "LoadClassWithOptions":
                    arg = _required_arg(scan, pos, "{")
                    yield self._request(
                        name, "class", arg, source.parent, ("", ".cls"),
                        project_dir=project_dir, external_when_missing=True,
                    )
                elif name in {"usepackage", "RequirePackage"}:
                    options, pos = _optional_arg(scan, pos)
                    arg = _required_arg(scan, pos, "{")
                    items = _split_list(arg)
                    for item in items:
                        yield self._request(
                            name, "package", item, source.parent, ("", ".sty"),
                            project_dir=project_dir, external_when_missing=True,
                        )
                    if any(item.value.strip() == "biblatex" for item in items) and options:
                        yield from self._biblatex_option_requests(options, source, project_dir)
                elif name == "RequirePackageWithOptions":
                    arg = _required_arg(scan, pos, "{")
                    for item in _split_list(arg):
                        yield self._request(
                            name, "package", item, source.parent, ("", ".sty"),
                            project_dir=project_dir, external_when_missing=True,
                        )
                elif name == "bibliographystyle":
                    arg = _required_arg(scan, pos, "{")
                    yield self._request(
                        name, "bibliography_style", arg, source.parent, ("", ".bst"),
                        project_dir=project_dir, external_when_missing=True,
                    )
                elif name in {"RequireBibliographyStyle", "RequireCitationStyle"}:
                    arg = _required_arg(scan, pos, "{")
                    suffix = ".bbx" if name == "RequireBibliographyStyle" else ".cbx"
                    yield self._request(
                        name, "bibliography_style", arg, source.parent, ("", suffix),
                        project_dir=project_dir, external_when_missing=True,
                    )
                elif name == "DeclareLanguageMapping":
                    _language = _required_arg(scan, pos, "{")
                    mapping = _required_arg(scan, _language.end, "{")
                    yield self._request(
                        name, "language_style", mapping, source.parent, ("", ".lbx"),
                        project_dir=project_dir, external_when_missing=True,
                    )
                elif name == "graphicspath":
                    outer = _required_arg(scan, pos, "{")
                    for nested in _nested_groups(outer):
                        if _is_literal(nested.value):
                            graphic_dirs.append(project_dir / nested.value.strip())
                elif name == "includegraphics":
                    _options, pos = _optional_arg(scan, pos)
                    arg = _required_arg(scan, pos, "{")
                    yield _Request(
                        command=name,
                        role="figure",
                        literal=arg.value.strip(),
                        start=match.start(),
                        end=arg.end,
                        base_dirs=tuple(dict.fromkeys((
                            project_dir, *graphic_dirs, source.parent,
                        ))),
                        extensions=_GRAPHIC_EXTENSIONS,
                    )
            except ValueError:
                yield _Request(
                    command=name,
                    role="unknown",
                    literal=scan[match.end():min(len(scan), match.end() + 80)],
                    start=match.start(),
                    end=match.end(),
                    base_dirs=(source.parent,),
                    extensions=("",),
                )

    def _request(
        self,
        command: str,
        role: str,
        arg: _Argument,
        base: Path,
        extensions: tuple[str, ...],
        *,
        project_dir: Path | None = None,
        external_when_missing: bool = False,
    ) -> _Request:
        bases = (
            (base,)
            if project_dir is None or project_dir == base
            else (project_dir, base)
        )
        return _Request(
            command=command,
            role=role,
            literal=arg.value.strip(),
            start=arg.start,
            end=arg.end,
            base_dirs=bases,
            extensions=extensions,
            external_when_missing=external_when_missing,
        )

    def _dynamic_request(
        self,
        command: str,
        role: str,
        first: _Argument,
        second: _Argument,
    ) -> _Request:
        return _Request(
            command=command,
            role=role,
            literal=f"{first.value}}}{{{second.value}",
            start=first.start,
            end=second.end,
            base_dirs=(),
            extensions=("",),
        )

    def _biblatex_option_requests(
        self,
        options: _Argument,
        source: Path,
        project_dir: Path,
    ) -> Iterator[_Request]:
        for raw in options.value.split(","):
            key, sep, value = raw.partition("=")
            if not sep:
                continue
            key = key.strip()
            value = value.strip()
            extensions = {
                "style": (".bbx", ".cbx"),
                "bibstyle": (".bbx",),
                "citestyle": (".cbx",),
                "datamodel": (".dbx",),
            }.get(key)
            if not extensions:
                continue
            arg = _Argument(value=value, start=options.start, end=options.end)
            for extension in extensions:
                yield self._request(
                    "biblatex-option", "bibliography_style", arg, source.parent,
                    ("", extension), project_dir=project_dir,
                    external_when_missing=True,
                )

    def _resolve_request(
        self,
        request: _Request,
        project_dir: Path,
    ) -> tuple[Path | None, str]:
        literal = request.literal.strip()
        if request.role == "unknown" or not _is_literal(literal):
            return None, "dynamic"
        saw_escape = False
        saw_source_escape = False
        candidates: list[Path] = []
        for value, extension_elided in _literal_variants(literal):
            for base in request.base_dirs:
                for extension in request.extensions:
                    candidate = base / value
                    if not extension:
                        candidates.append(candidate)
                        continue
                    if extension_elided:
                        # The grouping declared the whole value to be the
                        # basename, so a dot inside it is data: append the
                        # extension instead of replacing the false suffix.
                        candidate = candidate.with_name(candidate.name + extension)
                    elif candidate.suffix == "":
                        candidate = candidate.with_suffix(extension)
                    candidates.append(candidate)
        for candidate in candidates:
            try:
                resolved = candidate.resolve(strict=False)
            except (OSError, RuntimeError):
                continue
            if not _is_within(resolved, project_dir):
                saw_escape = True
                if not _is_within(resolved, self.source_root):
                    saw_source_escape = True
                continue
            try:
                self._secure_stat(candidate)
            except FileNotFoundError:
                continue
            return resolved, "resolved"
        if saw_source_escape:
            raise FileContainmentError(
                f"LaTeX dependency target escapes source root: {literal}"
            )
        if saw_escape:
            return None, "escape"
        if request.external_when_missing:
            return None, "external"
        return None, "missing"

    def _read_text(self, path: Path) -> str:
        content = self._read_bytes(path)
        if len(content) > self.max_file_bytes:
            raise ValueError("candidate exceeds max_file_bytes")
        return content.decode("utf-8")

    def _read_bytes(self, path: Path) -> bytes:
        reader = getattr(self, "_contained_reader", None)
        if reader is None:
            raise FileContainmentError("manifest secure reader is not active")
        _stat, content, _rel = reader.read_bytes(path, max_bytes=self.max_file_bytes)
        return content

    def _secure_stat(
        self, path: Path, *, directory: bool = False,
    ) -> object:
        reader = getattr(self, "_contained_reader", None)
        if reader is None:
            raise FileContainmentError("manifest secure reader is not active")
        return reader.stat(path, directory=directory)

    def _is_generated(self, path: Path, project_dir: Path) -> bool:
        rel = _rel(path.resolve(strict=False), project_dir)
        if any(rel == value or rel.startswith(f"{value}/") for value in self._generated_overrides):
            return False
        lower_name = path.name.lower()
        if lower_name.endswith(".synctex") or lower_name.endswith(".synctex.gz"):
            return True
        if path.suffix.lower() in _GENERATED_SUFFIXES:
            return True
        return any(
            rel == value
            or rel.startswith(f"{value}/")
            or value in PurePosixPath(rel).parts
            for value in self._generated_dirs
        )


def _nfc(value: str) -> str:
    return unicodedata.normalize("NFC", value)


def _normalize_rel(value: str) -> str:
    normalized = _nfc(value.replace("\\", "/")).strip()
    return posixpath.normpath(normalized)


def _rel(path: Path, root: Path) -> str:
    try:
        value = path.resolve(strict=False).relative_to(root.resolve())
    except ValueError:
        value = path.resolve(strict=False)
    return _nfc(value.as_posix())


def _lexical_rel(path: Path, root: Path) -> str:
    return _nfc(path.absolute().relative_to(root.absolute()).as_posix())


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _canonical_json(payload: Any) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def _is_escaped(text: str, index: int) -> bool:
    count = 0
    index -= 1
    while index >= 0 and text[index] == "\\":
        count += 1
        index -= 1
    return count % 2 == 1


def _mask_inert(text: str) -> str:
    chars = list(text)
    index = 0
    while index < len(chars):
        if chars[index] == "%" and not _is_escaped(text, index):
            while index < len(chars) and chars[index] != "\n":
                chars[index] = " "
                index += 1
            continue
        index += 1
    scan = "".join(chars)
    begin_re = re.compile(r"\\begin\s*\{(?P<name>[A-Za-z*]+)\}")
    for match in list(begin_re.finditer(scan)):
        if _is_escaped(scan, match.start()):
            continue
        name = match.group("name")
        if name not in _CODE_ENVS:
            continue
        end_re = re.compile(r"\\end\s*\{" + re.escape(name) + r"\}")
        end = end_re.search(scan, match.end())
        stop = len(scan) if end is None else end.end()
        chars[match.start():stop] = " " * (stop - match.start())
    scan = "".join(chars)
    verb_re = re.compile(r"\\verb\*?")
    for match in list(verb_re.finditer(scan)):
        if _is_escaped(scan, match.start()) or match.end() >= len(scan):
            continue
        delimiter = scan[match.end()]
        if delimiter.isspace() or delimiter.isalpha():
            continue
        stop = scan.find(delimiter, match.end() + 1)
        stop = len(scan) if stop < 0 else stop + 1
        chars[match.start():stop] = " " * (stop - match.start())
    scan = "".join(chars)
    from okg.substrate.library.extraction.latex_macros import (
        scan_macro_definitions,
    )

    definitions, _warnings, invalid = scan_macro_definitions(text, scan_text=scan)
    for start, stop in (
        *(definition.span for definition in definitions),
        *invalid,
    ):
        chars[start:stop] = " " * (stop - start)
    return "".join(chars)


def _skip_space(text: str, pos: int) -> int:
    while pos < len(text) and text[pos].isspace():
        pos += 1
    return pos


def _balanced_arg(text: str, pos: int, opener: str) -> _Argument | None:
    pos = _skip_space(text, pos)
    closer = "}" if opener == "{" else "]"
    if pos >= len(text) or text[pos] != opener:
        return None
    depth = 1
    index = pos + 1
    while index < len(text):
        char = text[index]
        if char == opener and not _is_escaped(text, index):
            depth += 1
        elif char == closer and not _is_escaped(text, index):
            depth -= 1
            if depth == 0:
                return _Argument(text[pos + 1:index], pos, index + 1)
        index += 1
    return None


def _required_arg(text: str, pos: int, opener: str) -> _Argument:
    arg = _balanced_arg(text, pos, opener)
    if arg is None:
        raise ValueError("missing or unbalanced argument")
    return arg


def _optional_arg(text: str, pos: int) -> tuple[_Argument | None, int]:
    arg = _balanced_arg(text, pos, "[")
    return (arg, arg.end if arg else pos)


def _split_list(arg: _Argument) -> tuple[_Argument, ...]:
    out: list[_Argument] = []
    cursor = 0
    for raw in arg.value.split(","):
        stripped = raw.strip()
        if stripped:
            offset = arg.value.find(raw, cursor)
            start = arg.start + 1 + offset + (len(raw) - len(raw.lstrip()))
            out.append(_Argument(stripped, start, start + len(stripped)))
            cursor = offset + len(raw)
    return tuple(out)


def _nested_groups(outer: _Argument) -> tuple[_Argument, ...]:
    groups: list[_Argument] = []
    pos = 0
    while pos < len(outer.value):
        local = _balanced_arg(outer.value, pos, "{")
        if local is None:
            break
        groups.append(_Argument(
            local.value,
            outer.start + 1 + local.start,
            outer.start + 1 + local.end,
        ))
        pos = local.end
    return tuple(groups)


def _balanced_braces(value: str) -> bool:
    depth = 0
    for char in value:
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


def _literal_variants(literal: str) -> tuple[tuple[str, bool], ...]:
    """``(value, extension_elided)`` pairs to probe, the literal as written first.

    TeX lets an author group part of a path argument to protect a dot from
    being read as the file extension, which is the whole reason the idiom
    exists. Both forms name a real file::

        \\includegraphics{{images/fig}.pdf}   -> images/fig.pdf
        \\includegraphics{{images/fig.v2}}    -> images/fig.v2.<ext>

    The braces are grouping syntax, not part of the path. When the group is
    followed by a dotted tail the extension was written out; when it is not,
    the extension was elided and graphicx searches for it, so the caller must
    append a candidate extension rather than treat ``.v2`` as one.

    The verbatim literal is probed first. That is a compatibility tie-break
    rather than a claim about TeX: real TeX always reads braces as grouping,
    but probing as-written first means no path that resolved before this rule
    existed can start resolving somewhere else.

    Only balanced, properly nested braces are unwrapped. Production never
    reaches this function with unbalanced ones -- ``_dynamic_request`` builds
    its ``first}{second`` literal only when the directory argument already
    fails ``_is_literal``, which makes ``_resolve_request`` return ``dynamic``
    first -- so the check is a guard against a future caller, not the thing
    that classifies ``\\import`` today.
    """
    if "{" not in literal and "}" not in literal:
        return ((literal, False),)
    if not _balanced_braces(literal):
        return ((literal, False),)
    ungrouped = literal.replace("{", "").replace("}", "").strip()
    if ungrouped == literal or not _is_literal(ungrouped):
        return ((literal, False),)
    extension_elided = "." not in literal.rpartition("}")[2]
    return ((literal, False), (ungrouped, extension_elided))


def _is_literal(value: str) -> bool:
    value = value.strip()
    return bool(value) and not (
        "\\" in value
        or "#" in value
        or "$" in value
        or _URI_RE.match(value)
        or Path(value).is_absolute()
    )


def _reachable(start: Path, adjacency: Mapping[Path, set[Path]]) -> set[Path]:
    seen: set[Path] = set()
    stack = list(adjacency.get(start, ()))
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(adjacency.get(node, ()))
    return seen


__all__ = [
    "DEFAULT_MAX_DEPTH",
    "DEFAULT_MAX_FILE_BYTES",
    "DEFAULT_MAX_FILES",
    "DEFAULT_MAX_INVENTORY_FILES",
    "DEFAULT_MAX_INVENTORY_TOTAL_BYTES",
    "DEFAULT_MAX_TOTAL_BYTES",
    "LatexDependencyOccurrence",
    "LatexManifestFile",
    "LatexManifestProject",
    "LatexProjectManifest",
    "LatexProjectManifestBuilder",
    "LatexProjectSpec",
    "LatexProjectWarning",
]
