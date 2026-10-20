"""Package-manifest dependency source.

Extracted from ``library/sources/code_graph.py`` by Wave D-30
(2026-05) per ``extract-code-graph-structure-and-manifest``.
Provides `PackageManifestSource`, the substrate's source for
declared dependencies extracted from pyproject.toml /
package.json / Cargo.toml / etc.

``library/sources/code_graph.py`` re-exports
`PackageManifestSource` at module-import time so YAML manifests
with `module: okg.substrate.library.sources.code_graph;
class: PackageManifestSource` keep resolving.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

import tomllib

from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)

from ._code_graph_common import (
    _file_id,
    _rel,
)


__all__ = ["PackageManifestSource"]



@dataclass
class PackageManifestSource:
    repo: str
    repo_root: Path | str
    manifest_globs: Sequence[str] = (
        "pyproject.toml",
        "package.json",
        "go.mod",
        "Cargo.toml",
    )
    source_file_stub_globs: Sequence[str] = ()

    name: str = "package_manifests"
    profile: str = "discovery_crawl"
    # Incremental-source contract (issue #323): git scope probe. No scope_filter
    # — the whole-repo tree is a sound superset of the manifest read scope, and
    # the manifest re-glob is cheap when the probe does fire.
    change_probe_kind = "git_scope"

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self.change_probe = self._build_change_probe()
        self.probe_short_circuit_safe = self._has_exact_git_snapshot()

    def _has_exact_git_snapshot(self) -> bool:
        from okg.substrate.library.sources._git_helpers import in_proc_workdir

        try:
            return in_proc_workdir(Path(self.repo_root)) == Path(self.repo_root)
        except Exception:  # noqa: BLE001 - non-git direct callers always read
            return False

    def _build_change_probe(self):
        import sys

        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )
        from okg.substrate.library.sources.git_scope_probe import GitScopeProbe

        return GitScopeProbe(
            repo_root=self.repo_root,
            config={
                "repo": self.repo,
                "manifest_globs": sorted(self.manifest_globs),
                "source_file_stub_globs": sorted(self.source_file_stub_globs),
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
            ),
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        facts: list[NodeFact | EdgeFact] = []
        record_set = RecordSet()
        captured, applied_raw_token = self._capture_manifest_inputs()
        for path, raw_bytes in captured:
            manifest_facts, emission = self._facts_for_manifest(
                path,
                raw_bytes=raw_bytes,
            )
            facts.extend(manifest_facts)
            record_set.records[_rel(Path(self.repo_root), path)] = emission
        completed_scope = mode in {"reconcile", "scope_complete"}
        return SourceRun(
            facts=iter(facts),
            completed_scope=completed_scope,
            next_cursor={"run_id": run_id},
            applied_raw_token=applied_raw_token,
            applied_token_scope=(
                "global" if applied_raw_token is not None else "unavailable"
            ),
            applied_token_authority=(
                "exact" if applied_raw_token is not None else "unavailable"
            ),
            run_mode=mode,
            record_set=record_set if completed_scope else None,
        )

    def _manifest_paths(self) -> tuple[Path, ...]:
        paths: list[Path] = []
        for pattern in self.manifest_globs:
            paths.extend(
                path
                for path in sorted(Path(self.repo_root).glob(pattern))
                if path.is_file()
            )
        return tuple(paths)

    def _capture_manifest_inputs(
        self,
    ) -> tuple[tuple[tuple[Path, bytes], ...], Optional[dict[str, Any]]]:
        from okg.substrate.library.sources.change_probe import ChangeProbeError

        paths = self._manifest_paths()
        captured: list[tuple[Path, bytes]] = []
        for path in paths:
            try:
                captured.append((path, path.read_bytes()))
            except OSError as exc:
                raise ChangeProbeError(
                    "package manifest input could not be read"
                ) from exc

        if not self.probe_short_circuit_safe:
            return tuple(captured), None

        from okg.substrate.library.sources._git_helpers import (
            in_proc_head_blob_bytes,
        )

        snapshot = self.change_probe.capture_snapshot()
        if paths != self._manifest_paths():
            raise ChangeProbeError(
                "package manifest membership changed during snapshot"
            )

        dirty_manifest = snapshot.get("dirty_manifest") or {}
        clean_paths: list[str] = []
        raw_by_rel: dict[str, bytes] = {}
        for path, raw_bytes in captured:
            rel_path = _rel(Path(self.repo_root), path)
            raw_by_rel[rel_path] = raw_bytes
            identity = dirty_manifest.get(rel_path)
            if identity is None:
                clean_paths.append(rel_path)
                continue
            if identity.get("sha256") != hashlib.sha256(raw_bytes).hexdigest():
                raise ChangeProbeError(
                    "package manifest changed during snapshot"
                )

        head_blobs = in_proc_head_blob_bytes(
            Path(self.repo_root),
            clean_paths,
            tree_sha=str(snapshot["tree_sha"]),
        )
        if any(
            head_blobs.get(rel_path) != raw_by_rel[rel_path]
            for rel_path in clean_paths
        ):
            raise ChangeProbeError(
                "package manifest is not covered by the captured Git tree"
            )

        return (
            tuple(captured),
            self.change_probe.token_for_snapshot(**snapshot),
        )

    def _facts_for_manifest(
        self,
        path: Path,
        *,
        raw_bytes: Optional[bytes] = None,
    ) -> tuple[list[NodeFact | EdgeFact], RecordEmission]:
        rel_path = _rel(Path(self.repo_root), path)
        if raw_bytes is None:
            raw_bytes = path.read_bytes()
        text = raw_bytes.decode("utf-8", errors="replace")
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        fmt = path.name
        mid = f"manifest:{self.repo}:{rel_path}"
        revision = {"text_hash": text_hash}
        manifest_record = {"repo": self.repo, "path": rel_path}
        package_name, deps = _parse_manifest(path, text)
        out: list[NodeFact | EdgeFact] = [
            NodeFact(
                node_id=mid,
                subtype="package_manifest",
                attrs={
                    "manifest_id": mid,
                    "repo": self.repo,
                    "path": rel_path,
                    "format": fmt,
                    "package_name": package_name,
                    "text": f"{fmt} {package_name}".strip(),
                },
                source_record_id=manifest_record,
                source_revision=revision,
            ),
            EdgeFact(
                src=_file_id(self.repo, rel_path),
                dst=mid,
                edge_type="contains",
                attrs={},
                source_record_id=manifest_record,
                source_revision=revision,
            ),
        ]
        emission = RecordEmission(
            primary=RecordNode(
                node_id=mid,
                subtype="package_manifest",
                source_record_id=manifest_record,
                source_revision=revision,
            ),
            edges=[
                EdgeKey(
                    src=_file_id(self.repo, rel_path),
                    dst=mid,
                    edge_type="contains",
                    attrs={},
                    source_record_id=manifest_record,
                    source_revision=revision,
                )
            ],
        )
        for ecosystem, name, spec in deps:
            pid = f"dep:{ecosystem}:{name.lower()}"
            dependency_record = {
                "repo": self.repo,
                "path": rel_path,
                "dependency": name,
            }
            out.append(NodeFact(
                node_id=pid,
                subtype="dependency_package",
                attrs={
                    "package_id": pid,
                    "name": name,
                    "ecosystem": ecosystem,
                    "version_spec": spec,
                    "source": rel_path,
                    "text": f"{ecosystem} {name} {spec}".strip(),
                },
                source_record_id=dependency_record,
                source_revision=revision,
            ))
            out.append(EdgeFact(
                src=mid,
                dst=pid,
                edge_type="contains",
                attrs={"version_spec": spec},
                source_record_id=dependency_record,
                source_revision=revision,
            ))
            emission.descendants.append(RecordNode(
                node_id=pid,
                subtype="dependency_package",
                source_record_id=dependency_record,
                source_revision=revision,
            ))
            emission.edges.append(EdgeKey(
                src=mid,
                dst=pid,
                edge_type="contains",
                attrs={"version_spec": spec},
                source_record_id=dependency_record,
                source_revision=revision,
            ))
        return out, emission


def _parse_manifest(path: Path, text: str) -> tuple[str, list[tuple[str, str, str]]]:
    if path.name in {"pyproject.toml", "Cargo.toml"}:
        doc = tomllib.loads(text)
        if path.name == "pyproject.toml":
            project = doc.get("project", {})
            deps = []
            for dep in project.get("dependencies", []) or []:
                name = re.split(r"[<>=!~ ;\[]", str(dep), maxsplit=1)[0]
                deps.append(("python", name, str(dep)))
            for group in (project.get("optional-dependencies", {}) or {}).values():
                for dep in group or []:
                    name = re.split(r"[<>=!~ ;\[]", str(dep), maxsplit=1)[0]
                    deps.append(("python", name, str(dep)))
            return str(project.get("name") or ""), deps
        package = doc.get("package", {})
        deps = [("rust", k, str(v)) for k, v in (doc.get("dependencies", {}) or {}).items()]
        return str(package.get("name") or ""), deps
    if path.name == "package.json":
        doc = json.loads(text)
        deps = []
        for section in ("dependencies", "devDependencies", "peerDependencies"):
            deps.extend(("npm", k, str(v)) for k, v in (doc.get(section, {}) or {}).items())
        return str(doc.get("name") or ""), deps
    if path.name == "go.mod":
        deps = []
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("//") or line in {"require (", ")"}:
                continue
            if line.startswith("module "):
                package = line.split(None, 1)[1]
                continue
            parts = line.split()
            if len(parts) >= 2 and "/" in parts[0]:
                deps.append(("go", parts[0], parts[1]))
        return locals().get("package", ""), deps
    return "", []
