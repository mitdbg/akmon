"""Test report evidence source for okg-workspace coding context."""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess  # nosec B404
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from collections.abc import Iterator
from typing import Any, Optional, Sequence

from okg.substrate.errors import SourceRootMissing, SubstrateError
from okg.substrate.library.sources._path_scope import (
    glob_matches_path,
    path_scope_requested,
    resolve_scoped_paths,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
from okg.substrate.support.env_provenance import (
    annotate_optional_dependency_error,
)


__all__ = ["TestReportSource"]


_ARTIFACT_MANIFEST = ".okg-ci-artifact.json"
_ARTIFACT_MANIFEST_SCHEMA = "okg-ci.artifact-manifest.v1"


def _sha(text: str, n: int = 16) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:n]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _xml_parse(path: Path) -> Any:
    try:
        from defusedxml import ElementTree as ET
    except ImportError as exc:
        # Issue #639: name the interpreter that lacked the package so a
        # uv-tool-shim run is distinguishable from a missing install.
        message, fix_hint, provenance = annotate_optional_dependency_error(
            "TestReportSource requires defusedxml for XML report import",
            "install okg[quality] or configure only JSON reports",
            extras=("quality",),
        )
        raise SubstrateError(
            message,
            data=provenance,
            fix_hint=fix_hint,
        ) from exc
    return ET.parse(path)


def _xml_from_bytes(raw_bytes: bytes) -> Any:
    try:
        from defusedxml import ElementTree as ET
    except ImportError as exc:
        message, fix_hint, provenance = annotate_optional_dependency_error(
            "TestReportSource requires defusedxml for XML report import",
            "install okg[quality] or configure only JSON reports",
            extras=("quality",),
        )
        raise SubstrateError(
            message,
            data=provenance,
            fix_hint=fix_hint,
        ) from exc
    return ET.fromstring(raw_bytes)


@dataclass
class TestReportSource:
    repo: str
    repo_root: Path | str
    artifact_paths: Sequence[Path | str] = ()
    artifact_globs: Sequence[str] = ()
    report_dirs: Sequence[Path | str] = ()
    coverage_paths: Sequence[Path | str] = ()
    coverage_globs: Sequence[str] = ()
    coverage_dirs: Sequence[Path | str] = ()
    suite_name: str = "pytest"
    sha: Optional[str] = None
    infer_sha: bool = True
    sha_from_path: bool = False
    # Some deployments ingest artifacts for ephemeral PR commits that are not
    # reachable in their local git mirror. When enabled, cross-source commit
    # edges are emitted only when git can prove the commit endpoint exists.
    # Default-off preserves the standalone importer contract.
    require_reachable_commit_endpoints: bool = False
    max_artifacts: int = 200
    # Append-only ingestion (scalable-ci-test-result-ingestion). When True the
    # source never claims completed_scope, so absence never retracts -- correct
    # for immutable CI artifacts read from a prunable cache. okg-ci opts in via
    # the registry; other deployments keep the discovery_crawl default.
    append_only: bool = False

    name: str = "test_reports"
    profile: str = "discovery_crawl"

    # Change-probe contract (#323): test-report artifacts are non-git (CI drops,
    # generated XML/JSON), so this falls to the O(content) floor over the
    # configured artifact + coverage files via the source's OWN enumeration
    # (_configured_artifact_files()). When the SHA is git-derived (infer_sha,
    # unpinned, not sha_from_path) a HEAD move re-scopes the emitted node ids, so
    # the live HEAD is folded into the digest; otherwise it is omitted so a
    # commit does not spuriously bust the token.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self._last_run_stats: dict[str, Any] = {}
        self._reachable_commit_cache: tuple[bytes, dict[str, bool]] | None = None
        # A prunable append-only cache cannot authoritatively replay historical
        # facts after a pipeline-identity change. Decline probe short-circuits
        # for that selector; ordinary discovery walks can prove a complete
        # current RecordSet and remain probe-managed.
        self.probe_short_circuit_safe = not self.append_only
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Any]]:
            self._require_repo_root()
            out: list[tuple[str, Any]] = [
                (str(p), p) for p in self._configured_artifact_files()
            ]
            if self.infer_sha and not self.sha and not self.sha_from_path:
                out.append(
                    ("__head_sha__", (self._head_sha() or "").encode("utf-8"))
                )
            if self.require_reachable_commit_endpoints:
                # Commit edges depend on the same refs that git-history walks.
                # Fold those refs into the token even for sha_from_path so a ref
                # appearing/disappearing forces the source to reconsider edges.
                out.append(("__git_history_refs__", self._git_history_ref_state()))
            return out

        return ContentHashProbe(
            content_items=_items,
            config={
                "repo": self.repo,
                "artifact_paths": [str(p) for p in self.artifact_paths],
                "artifact_globs": [str(g) for g in self.artifact_globs],
                "report_dirs": [str(d) for d in self.report_dirs],
                "coverage_paths": [str(p) for p in self.coverage_paths],
                "coverage_globs": [str(g) for g in self.coverage_globs],
                "coverage_dirs": [str(d) for d in self.coverage_dirs],
                "suite_name": self.suite_name,
                "sha": self.sha,
                "infer_sha": self.infer_sha,
                "sha_from_path": self.sha_from_path,
                "require_reachable_commit_endpoints": (
                    self.require_reachable_commit_endpoints
                ),
                "append_only": self.append_only,
            },
            emit_targets=[sys.modules[__name__]],
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        del cursor
        self._require_repo_root()
        fallback_sha = self.sha or (self._head_sha() if self.infer_sha else None)
        scoped = path_scope_requested(sync_scope)
        paths = list(self._artifact_files(sync_scope if scoped else None))
        stats = self._new_run_stats(run_id, paths, mode=mode)
        self._last_run_stats = stats
        completed_scope = (
            False if self.append_only
            else (not scoped and mode in {"reconcile", "scope_complete"})
        )
        expected_item_hashes: dict[str, str] = {}
        record_set: RecordSet | None = None
        if completed_scope:
            # SourceRunner needs the complete RecordSet before it consumes the
            # fact stream. Build that authority one artifact at a time instead
            # of materializing the full fact history, and retain only content
            # hashes so the later streaming pass can fail closed if an input
            # changes between the two reads.
            records: dict[str, RecordEmission] = {}
            emitted_artifact_nodes: set[str] = set()
            for path in paths:
                raw_bytes = path.read_bytes()
                expected_item_hashes[str(path)] = hashlib.sha256(
                    raw_bytes
                ).hexdigest()
                sha = self._sha_for_artifact(path, fallback=fallback_sha)
                facts = self._facts_for_artifact(
                    path,
                    sha=sha,
                    raw_bytes=raw_bytes,
                )
                _add_report_facts_to_records(
                    records,
                    facts,
                    emitted_artifact_nodes=emitted_artifact_nodes,
                )
            record_set = RecordSet(records=records)

        def _stream() -> "Iterator[NodeFact | EdgeFact]":
            # Stream facts per-artifact so peak memory is bounded to ONE
            # artifact's facts, not the whole scan. The SourceRunner consumes
            # this generator in bounded chunks (COPY_CHUNK_ROWS), so a
            # full-history scan no longer materializes ~1.9M facts (2.4GB) at
            # once (scalable-ci-test-result-ingestion). Per-artifact facts are
            # self-contained (sha-scoped, unique ids); the runner + chronos
            # apply dedupe by id, so no global _dedupe_facts pass is needed.
            emitted_artifact_nodes: set[str] = set()
            applied_item_hashes: dict[str, str] = {}
            for path in paths:
                raw_bytes = path.read_bytes()
                item_hash = hashlib.sha256(raw_bytes).hexdigest()
                expected_item_hash = expected_item_hashes.get(str(path))
                if (
                    expected_item_hash is not None
                    and item_hash != expected_item_hash
                ):
                    raise SubstrateError(
                        "test-report artifact changed during exact snapshot",
                        data={
                            "source": self.name,
                            "artifact": self._relpath(path),
                            "expected_sha256": expected_item_hash,
                            "observed_sha256": item_hash,
                        },
                        fix_hint=(
                            "retry test_reports ingestion after artifact "
                            "publication is complete"
                        ),
                    )
                applied_item_hashes[str(path)] = item_hash
                sha = self._sha_for_artifact(path, fallback=fallback_sha)
                facts = self._facts_for_artifact(
                    path,
                    sha=sha,
                    raw_bytes=raw_bytes,
                )
                self._update_run_stats(stats, path, facts)
                for fact in facts:
                    if (
                        isinstance(fact, NodeFact)
                        and fact.subtype == "ci_artifact"
                    ):
                        if fact.node_id in emitted_artifact_nodes:
                            continue
                        emitted_artifact_nodes.add(fact.node_id)
                    yield fact
            if self.infer_sha and not self.sha and not self.sha_from_path:
                applied_item_hashes["__head_sha__"] = hashlib.sha256(
                    (fallback_sha or "").encode("utf-8")
                ).hexdigest()
            exact_ref_state = (
                self._reachable_commit_cache[0]
                if self._reachable_commit_cache is not None
                else None
            )
            if self.require_reachable_commit_endpoints and exact_ref_state is not None:
                applied_item_hashes["__git_history_refs__"] = hashlib.sha256(
                    exact_ref_state
                ).hexdigest()
            exact_global = (
                not scoped
                and (
                    not self.require_reachable_commit_endpoints
                    or exact_ref_state is not None
                )
            )
            if exact_global:
                source_run.applied_raw_token = (
                    self.change_probe.token_from_item_hashes(
                        applied_item_hashes
                    )
                )
                source_run.applied_token_scope = "global"
                source_run.applied_token_authority = "exact"

        source_run = SourceRun(
            facts=iter(()),
            # When append_only, never claim completed_scope. Test artifacts are
            # immutable per run and nonempty runs do not supply a record_set, so
            # it cannot compute retractions-from-absence; claiming
            # completed_scope would be a false claim AND would let a pruned /
            # cache-missing scan retract the entire historical test graph.
            completed_scope=completed_scope,
            next_cursor={"run_id": run_id},
            run_mode=mode,
            record_set=record_set,
        )
        source_run.facts = _stream()
        return source_run

    def _require_repo_root(self) -> None:
        # A missing root reads as zero artifacts: the run would succeed with
        # no facts (or retract every report on a complete-scope run). Refuse.
        root = Path(self.repo_root)
        if not root.is_dir():
            raise SourceRootMissing(
                f"test_reports source {self.name!r}: repo_root {str(root)!r} "
                "does not exist or is not a directory",
                data={"source": self.name, "setting": "repo_root", "path": str(root)},
                fix_hint=SourceRootMissing.FIX_HINT,
            )

    def progress_after_run(self) -> dict[str, str]:
        payload = dict(getattr(self, "_last_run_stats", {}) or {})
        return {
            "diagnostics.test_reports.last_run": json.dumps(
                payload,
                sort_keys=True,
            ),
        }

    def _head_sha(self) -> Optional[str]:
        git = shutil.which("git")
        if git is None:
            return None
        try:
            proc = subprocess.run(
                [git, "-C", str(self.repo_root), "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                timeout=5,
            )  # nosec B603
            if proc.returncode == 0:
                return proc.stdout.strip() or None
        except (subprocess.TimeoutExpired, OSError):
            # Git metadata is optional for imported reports.
            pass
        return None

    _RUN_SHA_RE = re.compile(r"(?:^|/)runs/([0-9a-fA-F]{7,40})/")
    _RUN_LANE_RE = re.compile(
        r"(?:^|/)runs/[0-9a-fA-F]{7,40}/([A-Za-z0-9._-]+)(?:/|$)"
    )

    def _sha_for_artifact(
        self, path: Path, *, fallback: Optional[str],
    ) -> Optional[str]:
        """Per-artifact commit sha.

        When ``sha_from_path`` is enabled, derive the sha structurally from a
        ``runs/<sha>/`` path segment (the durable ci-results layout); otherwise
        fall back to the single hoisted sha (``self.sha`` or the inferred git
        HEAD). Default-off, so existing infer_sha deployments are byte-identical.
        The sha comes from the trusted path layout, never from artifact content.
        """
        if not self.sha_from_path:
            return fallback
        match = self._RUN_SHA_RE.search(self._relpath(path))
        return match.group(1).lower() if match else fallback

    def _lineage_for_artifact(self, path: Path) -> dict[str, Any]:
        manifest_path = self._artifact_manifest_path(path)
        lane = self._lane_for_artifact(path)
        if manifest_path is None:
            if self._RUN_SHA_RE.search(self._relpath(path)):
                return {
                    "lineage_status": "missing_artifact_manifest",
                    "lane": lane,
                }
            return {}
        rel_manifest = self._relpath(manifest_path)
        try:
            doc = json.loads(_read_text(manifest_path))
        except ValueError:
            return {
                "lineage_status": "invalid_artifact_manifest",
                "lineage_manifest_path": rel_manifest,
                "lane": lane,
            }
        if (
            not isinstance(doc, dict)
            or doc.get("schema") != _ARTIFACT_MANIFEST_SCHEMA
        ):
            return {
                "lineage_status": "invalid_artifact_manifest",
                "lineage_manifest_path": rel_manifest,
                "lane": lane,
            }
        repo = _clean_str(doc.get("repo"))
        artifact_id = _int_or_none(doc.get("artifact_id"))
        artifact_node_id = _clean_str(doc.get("artifact_node_id"))
        if not artifact_node_id and repo and artifact_id is not None:
            artifact_node_id = f"ci-artifact:github:{repo}:{artifact_id}"
        declared_status = _clean_str(doc.get("lineage_status"))
        status = declared_status or (
            "exact" if artifact_node_id else "missing_artifact_metadata"
        )
        return {
            "lineage_status": status,
            "lineage_manifest_path": rel_manifest,
            "github_repo": repo,
            "workflow_run_id": _int_or_none(doc.get("run_id")),
            "github_run_id": _int_or_none(doc.get("run_id")),
            "run_id": _int_or_none(doc.get("run_id")),
            "run_attempt": _int_or_none(doc.get("run_attempt")),
            "workflow_name": _clean_str(doc.get("workflow_name")),
            "workflow_id": _int_or_none(doc.get("workflow_id")),
            "head_branch": _clean_str(doc.get("head_branch")),
            "event": _clean_str(doc.get("event")),
            "workflow_created_at": _clean_str(doc.get("created_at")),
            "workflow_updated_at": _clean_str(doc.get("updated_at")),
            "workflow_url": _clean_str(doc.get("run_url")),
            "artifact_name": _clean_str(doc.get("artifact_name")),
            "artifact_id": artifact_id,
            "artifact_node_id": artifact_node_id,
            "artifact_url": _clean_str(doc.get("artifact_url")),
            "artifact_created_at": _clean_str(doc.get("artifact_created_at")),
            "artifact_updated_at": _clean_str(doc.get("artifact_updated_at")),
            "artifact_expires_at": _clean_str(doc.get("artifact_expires_at")),
            "artifact_expired": bool(doc.get("artifact_expired")),
            "artifact_publication_status": _clean_str(
                doc.get("artifact_publication_status")
            ),
            "artifact_publication_reason": _clean_str(
                doc.get("artifact_publication_reason")
            ),
            "lane": _clean_str(doc.get("lane")) or lane,
            "content_kinds": _clean_str_list(doc.get("content_kinds")),
        }

    def _artifact_manifest_path(self, path: Path) -> Optional[Path]:
        try:
            repo_root = Path(self.repo_root).resolve()
            current = path.resolve().parent
        except OSError:
            return None
        while True:
            candidate = current / _ARTIFACT_MANIFEST
            if candidate.is_file():
                return candidate
            if current == repo_root:
                return None
            try:
                current.relative_to(repo_root)
            except ValueError:
                return None
            parent = current.parent
            if parent == current:
                return None
            current = parent

    def _lane_for_artifact(self, path: Path) -> Optional[str]:
        match = self._RUN_LANE_RE.search(self._relpath(path))
        return match.group(1) if match else None

    def _new_run_stats(
        self,
        run_id: str,
        paths: list[Path],
        *,
        mode: str,
    ) -> dict[str, Any]:
        return {
            "schema": "okg.test_reports.run_stats.v1",
            "run_id": run_id,
            "mode": mode,
            "artifacts_seen": len(paths),
            "artifacts_parsed": 0,
            "test_facts_appended": 0,
            "coverage_facts_appended": 0,
            "latest_test_observed_at": None,
            "latest_workflow_observed_at": None,
            "freshness_lag_s": None,
            "lineage_status_counts": {},
        }

    def _update_run_stats(
        self,
        stats: dict[str, Any],
        path: Path,
        facts: list[NodeFact | EdgeFact],
    ) -> None:
        if facts:
            stats["artifacts_parsed"] += 1
        statuses: set[str] = set()
        for fact in facts:
            if not isinstance(fact, NodeFact):
                continue
            if fact.subtype in {"test_report", "test_run", "test_case"}:
                stats["test_facts_appended"] += 1
            if fact.subtype == "coverage_report":
                stats["coverage_facts_appended"] += 1
            status = _clean_str(fact.attrs.get("lineage_status"))
            if status:
                statuses.add(status)
            if fact.subtype in {"test_report", "coverage_report"}:
                stats["latest_test_observed_at"] = _max_iso(
                    stats.get("latest_test_observed_at"),
                    fact.attrs.get("observed_at"),
                )
                stats["latest_workflow_observed_at"] = _max_iso(
                    stats.get("latest_workflow_observed_at"),
                    fact.attrs.get("workflow_updated_at")
                    or fact.attrs.get("workflow_created_at"),
                )
        if not statuses and self._RUN_SHA_RE.search(self._relpath(path)):
            statuses.add("unparsed")
        counts = stats.setdefault("lineage_status_counts", {})
        for status in statuses:
            counts[status] = int(counts.get(status) or 0) + 1
        latest_test = _parse_iso(stats.get("latest_test_observed_at"))
        latest_workflow = _parse_iso(stats.get("latest_workflow_observed_at"))
        if latest_test and latest_workflow:
            stats["freshness_lag_s"] = int(
                (latest_workflow - latest_test).total_seconds()
            )

    def _artifact_files(
        self,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> list[Path]:
        if sync_scope is not None:
            return self._scoped_artifact_files(sync_scope)
        return self._configured_artifact_files()

    def _configured_artifact_files(self) -> list[Path]:
        paths = _configured_paths(
            Path(self.repo_root),
            self.artifact_paths,
            self.artifact_globs,
            self.report_dirs,
            suffixes={".xml", ".json"},
        )
        paths.extend(_configured_paths(
            Path(self.repo_root),
            self.coverage_paths,
            self.coverage_globs,
            self.coverage_dirs,
            suffixes={".xml", ".json"},
        ))
        deduped = sorted(set(paths))
        if len(deduped) > self.max_artifacts:
            raise ValueError(
                f"test report source exceeded max_artifacts={self.max_artifacts}; "
                "tighten artifact_globs/report_dirs or raise the cap"
            )
        return deduped

    def _scoped_artifact_files(
        self,
        sync_scope: dict[str, Any],
    ) -> list[Path]:
        scoped = resolve_scoped_paths(
            sync_scope,
            roots=self._scope_roots(),
            suffixes=[".xml", ".json"],
        )
        out = [
            path for path in scoped
            if self._is_configured_artifact(path)
        ]
        return sorted(set(out))

    def _scope_roots(self) -> tuple[Path, ...]:
        roots = [Path(self.repo_root)]
        for raw in (*self.artifact_paths, *self.coverage_paths):
            roots.append(_resolve_configured_path(Path(self.repo_root), raw).parent)
        for raw in (*self.report_dirs, *self.coverage_dirs):
            roots.append(_resolve_configured_path(Path(self.repo_root), raw))
        return tuple(roots)

    def _is_configured_artifact(self, path: Path) -> bool:
        return _configured_path_matches(
            Path(self.repo_root),
            path,
            self.artifact_paths,
            self.artifact_globs,
            self.report_dirs,
            suffixes={".xml", ".json"},
        ) or _configured_path_matches(
            Path(self.repo_root),
            path,
            self.coverage_paths,
            self.coverage_globs,
            self.coverage_dirs,
            suffixes={".xml", ".json"},
        )

    def _facts_for_artifact(
        self,
        path: Path,
        *,
        sha: Optional[str],
        raw_bytes: Optional[bytes] = None,
    ) -> list[NodeFact | EdgeFact]:
        if raw_bytes is None:
            raw_bytes = path.read_bytes()
        raw_text = raw_bytes.decode("utf-8", errors="replace")
        if path.suffix.lower() == ".json":
            return self._json_facts(
                path,
                sha=sha,
                raw_text=raw_text,
            )
        if path.suffix.lower() == ".xml":
            try:
                root = _xml_from_bytes(raw_bytes)
            except SubstrateError:
                raise
            except Exception as exc:  # noqa: BLE001 - parse library varies.
                raise SubstrateError(
                    "malformed test-report XML artifact",
                    data={
                        "source": self.name,
                        "artifact": self._relpath(path),
                    },
                    fix_hint=(
                        "repair or remove the malformed XML report before "
                        "running test_reports ingestion"
                    ),
                ) from exc
            tag = _local_tag(root.tag)
            if tag in {"testsuites", "testsuite"}:
                return self._junit_facts(
                    path,
                    root,
                    sha=sha,
                    raw_text=raw_text,
                )
            if tag == "coverage":
                return self._coverage_xml_facts(
                    path,
                    root,
                    sha=sha,
                    raw_text=raw_text,
                )
        return []

    def _json_facts(
        self,
        path: Path,
        *,
        sha: Optional[str],
        raw_text: Optional[str] = None,
    ) -> list[NodeFact | EdgeFact]:
        try:
            doc = json.loads(
                raw_text if raw_text is not None else _read_text(path)
            )
        except ValueError as exc:
            raise SubstrateError(
                "malformed test-report JSON artifact",
                data={
                    "source": self.name,
                    "artifact": self._relpath(path),
                },
                fix_hint=(
                    "repair or remove the malformed JSON report before "
                    "running test_reports ingestion"
                ),
            ) from exc
        if (
            isinstance(doc, dict)
            and doc.get("schema")
            in {
                "okg.bench.sweep_summary.v1",
                "okg.bench.sensitivity_summary.v1",
            }
        ):
            return self._bench_facts(
                path,
                doc,
                sha=sha,
                raw_text=raw_text,
            )
        if isinstance(doc, dict) and "files" in doc and "totals" in doc:
            return self._coverage_json_facts(
                path,
                doc,
                sha=sha,
                raw_text=raw_text,
            )
        return []

    def _junit_facts(
        self,
        path: Path,
        root: Any,
        *,
        sha: Optional[str],
        raw_text: Optional[str] = None,
    ) -> list[NodeFact | EdgeFact]:
        text = raw_text if raw_text is not None else _read_text(path)
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        rel_path = self._relpath(path)
        revision = {"path": rel_path, "text_hash": text_hash}
        lineage = self._lineage_for_artifact(path)
        lineage_attrs = _lineage_node_attrs(lineage)
        run_lineage_attrs = _lineage_node_attrs(lineage, preserve_run_id=True)
        report_id = f"test-report:{self.repo}:junit:{_sha(rel_path + ':' + text_hash)}"
        run_key = _sha(f"{rel_path}:{text_hash}:{sha or ''}")
        run_id_node = f"test-run:{self.suite_name}:{run_key}"
        suites = (
            list(root.iter("testsuite"))
            if _local_tag(root.tag) == "testsuites" else [root]
        )
        observed_at = _artifact_observed_at(
            lineage,
            *(suite.attrib.get("timestamp") for suite in suites),
        )
        cases: list[tuple[str, dict[str, Any], Any]] = []
        n_passed = n_failed = n_skipped = 0
        duration_total = 0.0
        for suite in suites:
            duration_total += _float_or_zero(suite.attrib.get("time"))
            for case in suite.iter("testcase"):
                classname = case.attrib.get("classname", "")
                name = case.attrib.get("name", "")
                duration = _float_or_zero(case.attrib.get("time"))
                failures = case.findall("failure")
                errors = case.findall("error")
                skipped = case.findall("skipped")
                msg_node = None
                if failures:
                    status = "failed"
                    n_failed += 1
                    msg_node = failures[0]
                elif errors:
                    status = "error"
                    n_failed += 1
                    msg_node = errors[0]
                elif skipped:
                    status = "skipped"
                    n_skipped += 1
                    msg_node = skipped[0] if skipped else None
                else:
                    status = "passed"
                    n_passed += 1
                case_id = f"test-case:{run_key}:{_sha(classname + ':' + name, 24)}"
                message = ""
                if msg_node is not None:
                    message = (msg_node.attrib.get("message") or msg_node.text or "")[:2000]
                properties = _properties_for_case(case)
                attrs = {
                    "case_id": case_id,
                    "classname": classname,
                    "name": name,
                    "status": status,
                    "duration_s": duration,
                    "message": message,
                    "file": self._relpath_str(case.attrib.get("file") or ""),
                }
                attrs.update(lineage_attrs)
                if properties:
                    attrs["properties"] = properties
                criteria_refs = _criteria_refs_for_case(attrs)
                if criteria_refs:
                    attrs["criteria_refs"] = criteria_refs
                cases.append((case_id, attrs, msg_node))
        report_attrs = {
            "report_id": report_id,
            "repo": self.repo,
            "path": rel_path,
            "format": "junit_xml",
            "suite_name": self.suite_name,
            "artifact_hash": text_hash,
            "sha": sha,
            "text": f"JUnit XML {self.suite_name} {rel_path}",
        }
        if observed_at:
            report_attrs["observed_at"] = observed_at
            report_attrs["created_at"] = observed_at
        report_attrs.update(lineage_attrs)
        run_attrs = {
            "run_id": run_id_node,
            "suite_name": self.suite_name,
            "n_passed": n_passed,
            "n_failed": n_failed,
            "n_skipped": n_skipped,
            "duration_s": duration_total,
            "sha": sha,
        }
        run_attrs.update(run_lineage_attrs)
        out: list[NodeFact | EdgeFact] = [
            NodeFact(
                node_id=report_id,
                subtype="test_report",
                attrs=report_attrs,
                source_record_id={"repo": self.repo, "path": rel_path},
                source_revision=revision,
            ),
            NodeFact(
                node_id=run_id_node,
                subtype="test_run",
                attrs=run_attrs,
                source_record_id={"repo": self.repo, "path": rel_path, "run_id": run_id_node},
                source_revision=revision,
            ),
            EdgeFact(
                src=report_id,
                dst=run_id_node,
                edge_type="contains",
                attrs={},
                source_record_id={"repo": self.repo, "path": rel_path, "run_id": run_id_node},
                source_revision=revision,
            ),
        ]
        artifact_node_id = _clean_str(lineage.get("artifact_node_id"))
        if artifact_node_id:
            out.append(self._artifact_anchor_fact(lineage))
            out.append(EdgeFact(
                src=report_id,
                dst=artifact_node_id,
                edge_type="references",
                attrs=_lineage_edge_attrs(lineage),
                source_record_id={
                    "repo": self.repo,
                    "path": rel_path,
                    "report_id": report_id,
                    "artifact_node_id": artifact_node_id,
                },
                source_revision=revision,
            ))
        if sha and self._commit_endpoint_available(sha):
            out.append(EdgeFact(
                src=run_id_node,
                dst=f"commit:{self.repo}:{sha}",
                edge_type="affects",
                attrs={},
                source_record_id={"repo": self.repo, "path": rel_path, "sha": sha},
                source_revision=revision,
            ))
        for case_id, attrs, msg_node in cases:
            out.append(NodeFact(
                node_id=case_id,
                subtype="test_case",
                attrs=attrs,
                source_record_id={"repo": self.repo, "path": rel_path, "case_id": case_id},
                source_revision=revision,
            ))
            out.append(EdgeFact(
                src=run_id_node,
                dst=case_id,
                edge_type="contains",
                attrs={},
                source_record_id={"repo": self.repo, "path": rel_path, "run_id": run_id_node, "case_id": case_id},
                source_revision=revision,
            ))
            out.append(EdgeFact(
                src=report_id,
                dst=case_id,
                edge_type="contains",
                attrs={},
                source_record_id={"repo": self.repo, "path": rel_path, "report_id": report_id, "case_id": case_id},
                source_revision=revision,
            ))
            if attrs.get("file"):
                out.append(EdgeFact(
                    src=case_id,
                    dst=f"file:{self.repo}:{attrs['file']}",
                    edge_type="references",
                    attrs={},
                    source_record_id={"repo": self.repo, "path": rel_path, "case_id": case_id, "file": attrs["file"]},
                    source_revision=revision,
                ))
            if msg_node is not None:
                out.extend(self._runtime_event_facts(
                    report_id,
                    case_id,
                    rel_path,
                    attrs,
                    msg_node,
                    revision,
                ))
        return out

    def _runtime_event_facts(
        self,
        report_id: str,
        case_id: str,
        rel_path: str,
        case_attrs: dict[str, Any],
        msg_node: Any,
        revision: dict[str, Any],
    ) -> list[NodeFact | EdgeFact]:
        status = str(case_attrs.get("status") or "failed")
        message = (msg_node.attrib.get("message") or msg_node.text or "")[:4000]
        trace = msg_node.text or message
        event_id = f"runtime-event:{_sha(json.dumps([report_id, case_id, status, message], sort_keys=True), 24)}"
        out: list[NodeFact | EdgeFact] = [
            NodeFact(
                node_id=event_id,
                subtype="runtime_event",
                attrs={
                    "event_id": event_id,
                    "repo": self.repo,
                    "kind": "test_failure" if status == "failed" else "test_error",
                    "status": status,
                    "message": message,
                    "duration_s": case_attrs.get("duration_s") or 0.0,
                    "text": f"{status} {case_attrs.get('classname')}::{case_attrs.get('name')} {message}".strip(),
                },
                source_record_id={"repo": self.repo, "path": rel_path, "event_id": event_id},
                source_revision=revision,
            ),
            EdgeFact(
                src=report_id,
                dst=event_id,
                edge_type="contains",
                attrs={},
                source_record_id={"repo": self.repo, "path": rel_path, "event_id": event_id},
                source_revision=revision,
            ),
            EdgeFact(
                src=event_id,
                dst=case_id,
                edge_type="references",
                attrs={},
                source_record_id={"repo": self.repo, "path": rel_path, "event_id": event_id, "case_id": case_id},
                source_revision=revision,
            ),
        ]
        for index, frame in enumerate(self._stack_frames(trace)):
            frame_id = f"stack-frame:{_sha(json.dumps([event_id, index, frame], sort_keys=True), 24)}"
            frame_attrs = {
                "frame_id": frame_id,
                "repo": self.repo,
                "path": frame["path"],
                "line": frame["line"],
                "index": index,
                "function": frame.get("function") or "",
                "column": frame.get("column") or 0,
                "excerpt": frame.get("excerpt") or "",
                "text": f"{frame['path']}:{frame['line']} {frame.get('function') or ''}".strip(),
            }
            out.append(NodeFact(
                node_id=frame_id,
                subtype="stack_frame",
                attrs=frame_attrs,
                source_record_id={"repo": self.repo, "path": rel_path, "frame_id": frame_id},
                source_revision=revision,
            ))
            out.append(EdgeFact(
                src=event_id,
                dst=frame_id,
                edge_type="contains",
                attrs={"index": index},
                source_record_id={"repo": self.repo, "path": rel_path, "event_id": event_id, "frame_id": frame_id},
                source_revision=revision,
            ))
            out.append(EdgeFact(
                src=frame_id,
                dst=f"file:{self.repo}:{frame['path']}",
                edge_type="points_to",
                attrs={"line": frame["line"]},
                source_record_id={"repo": self.repo, "path": rel_path, "frame_id": frame_id, "file": frame["path"]},
                source_revision=revision,
            ))
        return out

    def _coverage_xml_facts(
        self,
        path: Path,
        root: Any,
        *,
        sha: Optional[str],
        raw_text: Optional[str] = None,
    ) -> list[NodeFact | EdgeFact]:
        rel_path = self._relpath(path)
        text = raw_text if raw_text is not None else _read_text(path)
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        revision = {
            "path": rel_path,
            "text_hash": text_hash,
        }
        lineage = self._lineage_for_artifact(path)
        files: dict[str, tuple[list[int], list[int]]] = {}
        for cls in root.iter("class"):
            filename = self._relpath_str(cls.attrib.get("filename") or "")
            if not filename:
                continue
            covered: list[int] = []
            missing: list[int] = []
            for line in cls.iter("line"):
                number = _int_or_zero(line.attrib.get("number"))
                hits = _int_or_zero(line.attrib.get("hits"))
                if not number:
                    continue
                if hits > 0:
                    covered.append(number)
                else:
                    missing.append(number)
            existing = files.setdefault(filename, ([], []))
            existing[0].extend(covered)
            existing[1].extend(missing)
        return self._coverage_facts(
            rel_path=rel_path,
            fmt="coverage_xml",
            line_rate=_float_or_zero(root.attrib.get("line-rate")),
            branch_rate=_float_or_zero(root.attrib.get("branch-rate")),
            files=files,
            revision=revision,
            sha=sha,
            lineage=lineage,
        )

    def _coverage_json_facts(
        self,
        path: Path,
        doc: dict[str, Any],
        *,
        sha: Optional[str],
        raw_text: Optional[str] = None,
    ) -> list[NodeFact | EdgeFact]:
        rel_path = self._relpath(path)
        text = raw_text if raw_text is not None else _read_text(path)
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        revision = {
            "path": rel_path,
            "text_hash": text_hash,
        }
        lineage = self._lineage_for_artifact(path)
        totals = doc.get("totals") or {}
        files: dict[str, tuple[list[int], list[int]]] = {}
        for filename, payload in (doc.get("files") or {}).items():
            rel = self._relpath_str(str(filename))
            if not rel:
                continue
            files[rel] = (
                [_int_or_zero(v) for v in payload.get("executed_lines", [])],
                [_int_or_zero(v) for v in payload.get("missing_lines", [])],
            )
        return self._coverage_facts(
            rel_path=rel_path,
            fmt="coverage_json",
            line_rate=_coverage_rate(totals),
            branch_rate=_float_or_zero(totals.get("percent_covered_branches")) / 100.0,
            files=files,
            revision=revision,
            sha=sha,
            lineage=lineage,
        )

    def _bench_facts(
        self,
        path: Path,
        doc: dict[str, Any],
        *,
        sha: Optional[str],
        raw_text: Optional[str] = None,
    ) -> list[NodeFact | EdgeFact]:
        """Emit one BenchMeasurement node per (cell, metric) from a bench
        sweep_summary.json, attributed to the run's head commit. An `affects`
        edge links each measurement to its git_commit for regression traversal.
        An absent value is skipped -- never emitted as a 0/placeholder."""
        rel_path = self._relpath(path)
        text = raw_text if raw_text is not None else _read_text(path)
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        revision = {
            "path": rel_path,
            "text_hash": text_hash,
        }
        lineage = self._lineage_for_artifact(path)
        observed_at = _artifact_observed_at(
            lineage,
            doc.get("generated_at"),
            doc.get("created_at"),
            doc.get("timestamp"),
        )
        out: list[NodeFact | EdgeFact] = []
        for m in doc.get("measurements") or []:
            cell = m.get("cell")
            metric = m.get("metric_name")
            value = m.get("value")
            if not cell or not metric or value is None:
                continue
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue
            mid = f"bench-measurement:{self.repo}:{sha or 'nosha'}:{cell}:{metric}"
            out.append(NodeFact(
                node_id=mid,
                subtype="bench_measurement",
                attrs={
                    "measurement_id": mid,
                    "repo": self.repo,
                    "cell": str(cell),
                    "config_axis": m.get("config_axis"),
                    "axis_value": m.get("axis_value"),
                    "metric_name": str(metric),
                    "value": value,
                    "unit": m.get("unit"),
                    "head_sha": sha,
                    **(
                        {
                            "observed_at": observed_at,
                            "created_at": observed_at,
                        }
                        if observed_at
                        else {}
                    ),
                },
                source_record_id={
                    "repo": self.repo, "path": rel_path,
                    "cell": str(cell), "metric": str(metric),
                },
                source_revision=revision,
            ))
            if sha and self._commit_endpoint_available(sha):
                out.append(EdgeFact(
                    src=mid,
                    dst=f"commit:{self.repo}:{sha}",
                    edge_type="affects",
                    source_record_id={
                        "repo": self.repo, "path": rel_path,
                        "cell": str(cell), "metric": str(metric), "sha": sha,
                    },
                    source_revision=revision,
                ))
        return out

    def _coverage_facts(
        self,
        *,
        rel_path: str,
        fmt: str,
        line_rate: float,
        branch_rate: float,
        files: dict[str, tuple[list[int], list[int]]],
        revision: dict[str, Any],
        sha: Optional[str],
        lineage: dict[str, Any],
    ) -> list[NodeFact | EdgeFact]:
        coverage_id = f"coverage-report:{self.repo}:{_sha(rel_path + ':' + json.dumps(revision, sort_keys=True))}"
        covered_lines = {
            path: sorted(set(line for line in lines[0] if line))
            for path, lines in files.items()
        }
        missing_lines = {
            path: sorted(set(line for line in lines[1] if line))
            for path, lines in files.items()
        }
        observed_at = _artifact_observed_at(lineage)
        coverage_attrs = {
            "coverage_id": coverage_id,
            "repo": self.repo,
            "path": rel_path,
            "format": fmt,
            "line_rate": line_rate,
            "branch_rate": branch_rate,
            "covered_lines": covered_lines,
            "missing_lines": missing_lines,
            "artifact_hash": str(revision.get("text_hash") or ""),
            "sha": sha,
            "text": f"{fmt} {rel_path} line_rate={line_rate:.3f}",
        }
        if observed_at:
            coverage_attrs["observed_at"] = observed_at
            coverage_attrs["created_at"] = observed_at
        coverage_attrs.update(_lineage_node_attrs(lineage))
        out: list[NodeFact | EdgeFact] = [
            NodeFact(
                node_id=coverage_id,
                subtype="coverage_report",
                attrs=coverage_attrs,
                source_record_id={"repo": self.repo, "path": rel_path, "coverage_id": coverage_id},
                source_revision=revision,
            )
        ]
        artifact_node_id = _clean_str(lineage.get("artifact_node_id"))
        if artifact_node_id:
            out.append(self._artifact_anchor_fact(lineage))
            out.append(EdgeFact(
                src=coverage_id,
                dst=artifact_node_id,
                edge_type="references",
                attrs=_lineage_edge_attrs(lineage),
                source_record_id={
                    "repo": self.repo,
                    "path": rel_path,
                    "coverage_id": coverage_id,
                    "artifact_node_id": artifact_node_id,
                },
                source_revision=revision,
            ))
        for filename in sorted(files):
            covered = covered_lines.get(filename, [])
            missing = missing_lines.get(filename, [])
            out.append(EdgeFact(
                src=coverage_id,
                dst=f"file:{self.repo}:{filename}",
                edge_type="references",
                attrs={
                    "covered_line_count": len(covered),
                    "missing_line_count": len(missing),
                    "has_missing_lines": bool(missing),
                },
                source_record_id={"repo": self.repo, "path": rel_path, "coverage_id": coverage_id, "file": filename},
                source_revision=revision,
            ))
        return out

    def _artifact_anchor_fact(self, lineage: dict[str, Any]) -> NodeFact:
        artifact_node_id = _clean_str(lineage.get("artifact_node_id"))
        artifact_id = _int_or_none(lineage.get("artifact_id"))
        repo = _clean_str(lineage.get("github_repo")) or self.repo
        name = _clean_str(lineage.get("artifact_name")) or "GitHub artifact"
        content_kinds = _clean_str_list(lineage.get("content_kinds"))
        content_kind = ",".join(content_kinds)
        updated_at = _clean_str(lineage.get("artifact_updated_at")) or _clean_str(
            lineage.get("workflow_updated_at")
        )
        revision = {
            "repo": repo,
            "artifact_id": str(artifact_id or ""),
            "updated_at": updated_at or "",
            "run_id": str(_int_or_none(lineage.get("run_id")) or ""),
        }
        return NodeFact(
            node_id=artifact_node_id,
            subtype="ci_artifact",
            attrs={
                "artifact_id": artifact_id,
                "repo": repo,
                "run_id": _int_or_none(lineage.get("run_id")),
                "run_attempt": _int_or_none(lineage.get("run_attempt")),
                "job_id": None,
                "name": name,
                "path": name,
                "size_in_bytes": None,
                "archive_download_url": _clean_str(lineage.get("artifact_url")),
                "url": _clean_str(lineage.get("artifact_url")),
                "expired": bool(lineage.get("artifact_expired")),
                "expires_at": _clean_str(lineage.get("artifact_expires_at")),
                "artifact_hash": "",
                "content_kind": content_kind,
                "content_kinds": content_kinds,
                "lineage_status": _clean_str(lineage.get("lineage_status")) or "exact",
                "artifact_publication_status": _clean_str(
                    lineage.get("artifact_publication_status")
                ),
                "artifact_publication_reason": _clean_str(
                    lineage.get("artifact_publication_reason")
                ),
                "lineage_manifest_path": _clean_str(
                    lineage.get("lineage_manifest_path")
                ),
                "observed_at": updated_at,
                "created_at": _clean_str(lineage.get("artifact_created_at")),
                "updated_at": updated_at,
                "text": f"{name} {content_kind}".strip(),
            },
            source_record_id={
                "repo": repo,
                "artifact_id": str(artifact_id or artifact_node_id),
            },
            source_revision=revision,
        )

    def _stack_frames(self, text: str) -> list[dict[str, Any]]:
        frames: list[dict[str, Any]] = []
        for match in re.finditer(r'File "([^"]+)", line (\d+), in ([^\n]+)', text):
            rel = self._relpath_str(match.group(1))
            if rel:
                frames.append({
                    "path": rel,
                    "line": int(match.group(2)),
                    "function": match.group(3).strip(),
                    "excerpt": match.group(0),
                })
        for match in re.finditer(r"(?m)^(.+?\.(?:py|js|ts|go|rs|c|cpp)):(\d+)(?::(\d+))?", text):
            rel = self._relpath_str(match.group(1).strip())
            if rel and not any(f["path"] == rel and f["line"] == int(match.group(2)) for f in frames):
                frames.append({
                    "path": rel,
                    "line": int(match.group(2)),
                    "column": _int_or_zero(match.group(3)),
                    "excerpt": match.group(0),
                })
        return frames

    def _relpath(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(Path(self.repo_root)).as_posix()
        except ValueError:
            return path.name

    def _relpath_str(self, raw: str) -> str:
        if not raw:
            return ""
        # Pytest assertion-diff lines can resemble ``path.py:line`` after an
        # ``E     `` prefix. They are diagnostic prose, not stack frames.
        if re.match(r"^[A-Z]\s{2,}", raw):
            return ""
        path = Path(raw)
        try:
            candidate = (
                path.resolve()
                if path.is_absolute()
                else (Path(self.repo_root) / path).resolve()
            )
            return candidate.relative_to(Path(self.repo_root)).as_posix()
        except ValueError:
            return ""

    def _commit_endpoint_available(self, sha: str) -> bool:
        if not self.require_reachable_commit_endpoints:
            return True
        ref_state = self._git_history_ref_state()
        cached = self._reachable_commit_cache
        if cached is None or cached[0] != ref_state:
            cached = (ref_state, {})
            self._reachable_commit_cache = cached
        if sha in cached[1]:
            return cached[1][sha]
        git = shutil.which("git")
        if git is None:
            cached[1][sha] = False
            return False
        try:
            proc = subprocess.run(
                [
                    git,
                    "-C",
                    str(self.repo_root),
                    "for-each-ref",
                    f"--contains={sha}",
                    "--format=%(refname)",
                    "refs/heads",
                    "refs/remotes",
                    "refs/tags",
                ],
                capture_output=True,
                timeout=5,
            )  # nosec B603
            available = proc.returncode == 0 and bool(proc.stdout.strip())
        except (subprocess.TimeoutExpired, OSError):
            available = False
        cached[1][sha] = available
        return available

    def _git_history_ref_state(self) -> bytes:
        """Return the ref state consumed by git-history's commit walk."""
        git = shutil.which("git")
        if git is None:
            return b""
        try:
            proc = subprocess.run(
                [
                    git,
                    "-C",
                    str(self.repo_root),
                    "for-each-ref",
                    "--format=%(refname)=%(objectname)=%(*objectname)",
                    "refs/heads",
                    "refs/remotes",
                    "refs/tags",
                ],
                capture_output=True,
                timeout=5,
            )  # nosec B603
            return proc.stdout if proc.returncode == 0 else b""
        except (subprocess.TimeoutExpired, OSError):
            return b""


def _configured_paths(
    repo_root: Path,
    paths: Sequence[Path | str],
    globs: Sequence[str],
    dirs: Sequence[Path | str],
    *,
    suffixes: set[str],
    ) -> list[Path]:
    out: list[Path] = []
    for raw in paths:
        path = _resolve_configured_path(repo_root, raw)
        if path.is_file():
            out.append(path.resolve())
    for pattern in globs:
        out.extend(
            path.resolve()
            for path in repo_root.glob(pattern)
            if path.is_file() and path.name != _ARTIFACT_MANIFEST
        )
    for raw_dir in dirs:
        directory = Path(raw_dir).expanduser()
        if not directory.is_absolute():
            directory = repo_root / directory
        if not directory.is_dir():
            continue
        out.extend(
            path.resolve()
            for path in directory.rglob("*")
            if (
                path.is_file()
                and path.suffix.lower() in suffixes
                and path.name != _ARTIFACT_MANIFEST
            )
        )
    return out


def _resolve_configured_path(repo_root: Path, raw: Path | str) -> Path:
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def _artifact_observed_at(
    lineage: dict[str, Any],
    *payload_candidates: Any,
) -> Optional[str]:
    """Return only an explicit payload or CI-lineage timestamp."""
    for value in (
        *payload_candidates,
        lineage.get("artifact_updated_at"),
        lineage.get("artifact_created_at"),
        lineage.get("workflow_updated_at"),
        lineage.get("workflow_created_at"),
    ):
        timestamp = _clean_str(value)
        if timestamp:
            return timestamp
    return None


def _configured_path_matches(
    repo_root: Path,
    path: Path,
    paths: Sequence[Path | str],
    globs: Sequence[str],
    dirs: Sequence[Path | str],
    *,
    suffixes: set[str],
) -> bool:
    resolved = path.resolve()
    if resolved.name == _ARTIFACT_MANIFEST:
        return False
    for raw in paths:
        if resolved == _resolve_configured_path(repo_root, raw):
            return True
    try:
        rel = resolved.relative_to(repo_root).as_posix()
    except ValueError:
        rel = ""
    if rel and any(_glob_matches(rel, pattern) for pattern in globs):
        return True
    for raw_dir in dirs:
        directory = _resolve_configured_path(repo_root, raw_dir)
        try:
            resolved.relative_to(directory)
        except ValueError:
            continue
        return resolved.suffix.lower() in suffixes
    return False


def _glob_matches(rel_path: str, pattern: str) -> bool:
    return glob_matches_path(rel_path, pattern)


def _properties_for_case(case: Any) -> dict[str, str]:
    properties: dict[str, str] = {}
    for child in list(case):
        if _local_tag(child.tag) != "properties":
            continue
        for prop in list(child):
            if _local_tag(prop.tag) != "property":
                continue
            name = str(prop.attrib.get("name") or "").strip()
            if not name:
                continue
            value = str(prop.attrib.get("value") or prop.text or "").strip()
            properties[name] = value
    return properties


def _criteria_refs_for_case(attrs: dict[str, Any]) -> list[str]:
    refs: set[str] = set()
    for key in (
        "classname",
        "name",
        "message",
        "criterion_id",
        "acceptance_criterion_id",
        "acceptance_criteria",
        "acceptance_criterion_ids",
        "openspec_criterion",
        "openspec_criteria",
        "scenario_id",
        "acceptance_scenario_id",
        "openspec_scenario",
        "openspec_scenarios",
    ):
        refs.update(_criteria_refs_from_value(attrs.get(key)))
    refs.update(_criteria_refs_from_value(attrs.get("properties")))
    return sorted(refs)


def _criteria_refs_from_value(value: Any) -> set[str]:
    refs: set[str] = set()
    if value is None:
        return refs
    if isinstance(value, dict):
        for key, nested in value.items():
            refs.update(_criteria_refs_from_value(key))
            refs.update(_criteria_refs_from_value(nested))
        return refs
    if isinstance(value, (list, tuple, set)):
        for nested in value:
            refs.update(_criteria_refs_from_value(nested))
        return refs
    text = str(value)
    refs.update(re.findall(r"\b(?:crit|scn):[A-Za-z0-9_.:/-]+", text))
    refs.update(re.findall(
        r"\b[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+",
        text,
    ))
    return refs


def _lineage_node_attrs(
    lineage: dict[str, Any],
    *,
    preserve_run_id: bool = False,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in lineage.items():
        if preserve_run_id and key == "run_id":
            continue
        if value is None or value == "" or value == []:
            continue
        out[key] = value
    return out


def _lineage_edge_attrs(lineage: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "lineage_status",
        "github_repo",
        "workflow_run_id",
        "github_run_id",
        "run_id",
        "run_attempt",
        "workflow_name",
        "artifact_id",
        "artifact_name",
        "lane",
        "lineage_manifest_path",
        "artifact_publication_status",
        "artifact_publication_reason",
    )
    return {
        key: lineage[key]
        for key in keys
        if key in lineage
        and lineage[key] is not None
        and lineage[key] != ""
        and lineage[key] != []
    }


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _add_report_facts_to_records(
    records: dict[str, RecordEmission],
    facts: list[NodeFact | EdgeFact],
    *,
    emitted_artifact_nodes: set[str],
) -> None:
    for fact in facts:
        if isinstance(fact, NodeFact) and fact.subtype == "ci_artifact":
            if fact.node_id in emitted_artifact_nodes:
                continue
            emitted_artifact_nodes.add(fact.node_id)
        source_record_id = dict(fact.source_record_id)
        record_key = str(source_record_id.get("path") or "") or json.dumps(
            source_record_id,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        emission = records.setdefault(record_key, RecordEmission())
        if isinstance(fact, NodeFact):
            emission.descendants.append(
                RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=source_record_id,
                    source_revision=dict(fact.source_revision),
                )
            )
        else:
            emission.edges.append(
                EdgeKey(
                    src=fact.src,
                    dst=fact.dst,
                    edge_type=fact.edge_type,
                    attrs=dict(fact.attrs),
                    provenance=fact.provenance,
                    confidence=fact.confidence,
                    source_record_id=source_record_id,
                    source_revision=dict(fact.source_revision),
                )
            )


def _clean_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        text = _clean_str(item)
        if text:
            out.append(text)
    return out


def _int_or_none(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_iso(value: Any) -> Optional[datetime]:
    text = _clean_str(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _max_iso(left: Any, right: Any) -> Optional[str]:
    left_dt = _parse_iso(left)
    right_dt = _parse_iso(right)
    if left_dt is None:
        return _clean_str(right)
    if right_dt is None:
        return _clean_str(left)
    return max(left_dt, right_dt).isoformat().replace("+00:00", "Z")


def _local_tag(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _float_or_zero(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _coverage_rate(totals: dict[str, Any]) -> float:
    if "percent_covered" in totals:
        return _float_or_zero(totals.get("percent_covered")) / 100.0
    covered = _float_or_zero(totals.get("covered_lines"))
    statements = _float_or_zero(totals.get("num_statements"))
    return covered / statements if statements else 0.0
