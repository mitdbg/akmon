"""Profile and benchmark artifact import for quality evidence."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

from okg.substrate import fastyaml
from okg.substrate.errors import SubstrateError

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
from okg.substrate.source_sync_benchmark import (
    is_source_sync_benchmark,
    source_sync_benchmark_measurement_rows,
)


__all__ = ["ProfileMeasurementSource"]


def _sha(value: str, n: int = 16) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:n]


def _record_set_from_facts(
    facts: Sequence[NodeFact | EdgeFact],
) -> RecordSet:
    records: dict[str, RecordEmission] = {}
    nodes = {
        fact.node_id: fact
        for fact in facts
        if isinstance(fact, NodeFact) and fact.op == "I"
    }
    for node in nodes.values():
        records[node.node_id] = RecordEmission(
            primary=RecordNode(
                node_id=node.node_id,
                subtype=node.subtype,
                source_record_id=node.source_record_id,
                source_revision=node.source_revision,
                ownership_mode=node.ownership_mode,
            )
        )
    for edge in (
        fact
        for fact in facts
        if isinstance(fact, EdgeFact) and fact.op == "I"
    ):
        record_key = edge.src if edge.src in nodes else edge.dst
        records.setdefault(record_key, RecordEmission()).edges.append(
            EdgeKey(
                src=edge.src,
                dst=edge.dst,
                edge_type=edge.edge_type,
                attrs=edge.attrs,
                provenance=edge.provenance,
                confidence=edge.confidence,
                source_record_id=edge.source_record_id,
                source_revision=edge.source_revision,
            )
        )
    return RecordSet(records=records)


@dataclass
class ProfileMeasurementSource:
    """Import local profiling and benchmark artifacts.

    The adapter imports artifacts produced by OKG profiling,
    Speedscope-compatible samplers, Scalene, Memray, pytest-benchmark, or a
    compact benchmark JSON shape. It does not execute profilers.
    """

    deployment: str
    repo: str
    repo_root: Path | str
    artifact_paths: Sequence[Path | str] = ()
    artifact_globs: Sequence[str] = ()
    benchmark_paths: Sequence[Path | str] = ()
    benchmark_globs: Sequence[str] = ()
    performance_budgets: Sequence[dict[str, Any]] = ()
    deployment_config_path: Path | str | None = None
    openspec_roots: Sequence[Path | str] = ()
    max_artifacts: int = 200
    max_hotspots_per_artifact: int = 25

    name: str = "profile_measurements"
    profile: str = "discovery_crawl"
    # Incremental-source contract (issue #323): content-hash floor over the
    # imported profile + benchmark artifacts (CI-generated, not git-tracked) and
    # the deployment config file; inline budgets/openspec roots fold into config.
    change_probe_kind = "content_hash"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self):
        import sys

        from okg.substrate.library.sources.content_hash_probe import ContentHashProbe

        def _scope():
            items: list[tuple[str, Path]] = []
            for path in self._profile_artifact_files(None):
                items.append((f"profile::{path}", path))
            for path in self._benchmark_artifact_files(None):
                items.append((f"benchmark::{path}", path))
            if self.deployment_config_path:
                items.append(
                    (f"config::{self.deployment_config_path}",
                     Path(self.deployment_config_path)),
                )
            return items

        return ContentHashProbe(
            content_items=_scope,
            config={
                "repo": self.repo,
                "deployment": self.deployment,
                "performance_budgets": [dict(b) for b in self.performance_budgets],
                "openspec_roots": [str(r) for r in self.openspec_roots],
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
        scoped = path_scope_requested(sync_scope)
        scoped_arg = sync_scope if scoped else None
        requested_scope_keys = [
            str(key)
            for key in (sync_scope or {}).get("scope_keys", [])
            if isinstance(key, str)
        ]
        prior_frontier = _cursor_artifact_frontier(cursor)
        profile_candidates = self._profile_artifact_files(scoped_arg)
        benchmark_candidates = self._benchmark_artifact_files(scoped_arg)
        skipped_profile_paths: list[Path] = []
        skipped_benchmark_paths: list[Path] = []
        if scoped:
            profile_paths, skipped_profile_paths = _changed_artifact_paths(
                Path(self.repo_root),
                profile_candidates,
                prior_frontier,
            )
            benchmark_paths, skipped_benchmark_paths = _changed_artifact_paths(
                Path(self.repo_root),
                benchmark_candidates,
                prior_frontier,
            )
        else:
            profile_paths = profile_candidates
            benchmark_paths = benchmark_candidates
        facts: list[NodeFact | EdgeFact] = []
        profile_run_ids: set[str] = set()
        for path in profile_paths:
            profile_facts = self._profile_facts(path)
            profile_run_ids.update(
                fact.node_id
                for fact in profile_facts
                if isinstance(fact, NodeFact)
                and fact.subtype == "profile_run"
            )
            facts.extend(profile_facts)
        budget_rows = (
            _performance_budget_rows(
                repo=self.repo,
                deployment=self.deployment,
                repo_root=Path(self.repo_root),
                explicit_budgets=self.performance_budgets,
                deployment_config_path=self.deployment_config_path,
                openspec_roots=self.openspec_roots,
            )
            if not scoped or profile_paths or benchmark_paths
            else []
        )
        for budget in budget_rows:
            revision = {"source": str(budget.get("source") or "performance_budget")}
            node = _performance_budget_node(budget, revision)
            facts.append(node)
            criterion_id = str(budget.get("criterion_id") or "")
            if criterion_id:
                facts.append(EdgeFact(
                    src=node.node_id,
                    dst=criterion_id,
                    edge_type="budgeted_by",
                    attrs={"scope": budget["scope"], "metric": budget["metric"]},
                    source_record_id={
                        "repo": self.repo,
                        "artifact": budget.get("source") or "performance_budget",
                        "budget_id": node.node_id,
                        "criterion_id": criterion_id,
                    },
                    source_revision=revision,
                ))
        for path in benchmark_paths:
            facts.extend(self._benchmark_facts(
                path,
                budget_rows,
                profile_run_ids=profile_run_ids,
            ))
        next_frontier = dict(prior_frontier)
        for path in sorted(set(profile_candidates + benchmark_candidates)):
            key = _artifact_frontier_key(Path(self.repo_root), path)
            fingerprint = _artifact_stat_fingerprint(path)
            if fingerprint:
                next_frontier[key] = fingerprint
        deduped_facts = list(_dedupe_facts(facts))
        completed_scope = (
            not scoped and mode in {"reconcile", "scope_complete"}
        )
        emitted_nodes = sum(isinstance(fact, NodeFact) for fact in deduped_facts)
        emitted_edges = sum(isinstance(fact, EdgeFact) for fact in deduped_facts)
        skipped_unchanged = skipped_profile_paths + skipped_benchmark_paths
        parsed_artifacts = profile_paths + benchmark_paths
        if scoped and not profile_candidates and not benchmark_candidates:
            scope_status = "scoped_no_matching_artifacts"
        elif scoped and not parsed_artifacts and skipped_unchanged:
            scope_status = "scoped_unchanged_noop"
        elif scoped:
            scope_status = "scoped"
        elif mode in {"reconcile", "scope_complete"}:
            scope_status = "full"
        else:
            scope_status = "cursor"
        self.last_run_stats = {
            "scope_status": scope_status,
            "run_mode": mode,
            "scope_keys": requested_scope_keys,
            "profile_artifacts_considered": len(profile_candidates),
            "benchmark_artifacts_considered": len(benchmark_candidates),
            "artifacts_considered": len(profile_candidates) + len(benchmark_candidates),
            "profile_artifacts_parsed": len(profile_paths),
            "benchmark_artifacts_parsed": len(benchmark_paths),
            "artifacts_parsed": len(parsed_artifacts),
            "artifacts_skipped_unchanged": len(skipped_unchanged),
            "artifacts_missing_or_unmatched": (
                len(requested_scope_keys)
                if scoped and not profile_candidates and not benchmark_candidates
                else 0
            ),
            "emitted_nodes": emitted_nodes,
            "emitted_edges": emitted_edges,
            "cursor_frontier_size": len(next_frontier),
        }
        return SourceRun(
            facts=iter(deduped_facts),
            completed_scope=completed_scope,
            next_cursor={
                **(cursor or {}),
                "run_id": run_id,
                "artifact_frontier": next_frontier,
            },
            run_mode=mode,
            record_set=(
                _record_set_from_facts(deduped_facts)
                if completed_scope else None
            ),
        )

    def _profile_artifact_files(
        self,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> list[Path]:
        return _artifact_files(
            Path(self.repo_root),
            self.artifact_paths,
            self.artifact_globs,
            self.max_artifacts,
            sync_scope=sync_scope,
        )

    def _benchmark_artifact_files(
        self,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> list[Path]:
        return _artifact_files(
            Path(self.repo_root),
            self.benchmark_paths,
            self.benchmark_globs,
            self.max_artifacts,
            sync_scope=sync_scope,
        )

    def _profile_facts(self, path: Path) -> list[NodeFact | EdgeFact]:
        doc = _read_json(path)
        if not isinstance(doc, dict):
            return []
        artifact = _rel_or_name(Path(self.repo_root), path)
        revision = _revision(path, doc)
        if _looks_like_okg_profile(doc):
            return _okg_profile_facts(
                self.repo,
                self.deployment,
                artifact,
                doc,
                revision,
                max_hotspots=self.max_hotspots_per_artifact,
            )
        run_id = _external_profile_run_id(self.deployment, artifact, doc)
        out: list[NodeFact | EdgeFact] = [
            _profile_run_node(
                repo=self.repo,
                deployment=self.deployment,
                profile_run_id=run_id,
                target_kind=_profile_tool(doc, artifact),
                command=str(doc.get("command") or doc.get("cmd") or ""),
                started_at=str(doc.get("started_at") or doc.get("start_time") or _artifact_mtime_iso(path)),
                duration_ms=_duration_ms(doc.get("duration_ms") or doc.get("duration") or 0),
                generation_id=_int_or_none(doc.get("generation_id")),
                policy={},
                collectors=[_profile_tool(doc, artifact)],
                summary=f"{_profile_tool(doc, artifact)} {artifact}",
                artifact=artifact,
                revision=revision,
                request_path=str(
                    doc.get("request_path") or doc.get("workflow_id") or "",
                ),
                owner_id=str(doc.get("owner_id") or ""),
                owner_meta=(
                    doc.get("owner_meta")
                    if isinstance(doc.get("owner_meta"), dict) else {}
                ),
                source_sync=(
                    doc.get("source_sync")
                    if isinstance(doc.get("source_sync"), dict) else {}
                ),
                collector_evidence=(
                    doc.get("evidence")
                    if isinstance(doc.get("evidence"), dict) else {}
                ),
                artifacts=(
                    doc.get("artifacts")
                    if isinstance(doc.get("artifacts"), list) else []
                ),
            )
        ]
        hotspots = _external_hotspots(run_id, artifact, doc, self.max_hotspots_per_artifact)
        for hotspot in hotspots:
            out.append(_hotspot_node(self.repo, run_id, hotspot, artifact, revision))
            out.append(_contains_edge(self.repo, run_id, hotspot["hotspot_id"], artifact, revision))
        return out

    def _benchmark_facts(
        self,
        path: Path,
        budget_rows: Sequence[dict[str, Any]],
        *,
        profile_run_ids: set[str] | None = None,
    ) -> list[NodeFact | EdgeFact]:
        doc = _read_json(path)
        if not isinstance(doc, dict):
            return []
        artifact = _rel_or_name(Path(self.repo_root), path)
        revision = _revision(path, doc)
        out: list[NodeFact | EdgeFact] = []
        for row in _benchmark_rows(self.repo, artifact, doc, budget_rows):
            node = _benchmark_node(row, revision)
            out.append(node)
            profile_run_id = str(row.get("profile_run_id") or "")
            if profile_run_id and (
                profile_run_ids is None or profile_run_id in profile_run_ids
            ):
                out.append(EdgeFact(
                    src=node.node_id,
                    dst=profile_run_id,
                    edge_type="references",
                    attrs={"metric": row["metric"], "scenario": row["scenario"]},
                    source_record_id={
                        "repo": self.repo,
                        "artifact": artifact,
                        "measurement_id": node.node_id,
                        "profile_run_id": profile_run_id,
                    },
                    source_revision=revision,
                ))
            for budget in _matching_budgets(row, budget_rows):
                out.append(EdgeFact(
                    src=node.node_id,
                    dst=str(budget["budget_id"]),
                    edge_type="references",
                    attrs={
                        "scenario": row["scenario"],
                        "metric": row["metric"],
                        "value": row["value"],
                        "threshold": budget["threshold"],
                        "status": row["status"],
                    },
                    source_record_id={
                        "repo": self.repo,
                        "artifact": artifact,
                        "measurement_id": node.node_id,
                        "budget_id": budget["budget_id"],
                    },
                    source_revision=revision,
                ))
        return out


def _okg_profile_facts(
    repo: str,
    deployment: str,
    artifact: str,
    doc: dict[str, Any],
    revision: dict[str, Any],
    *,
    max_hotspots: int,
) -> list[NodeFact | EdgeFact]:
    run_id = _profile_run_id(deployment, str(doc.get("profile_run_id") or _sha(artifact)))
    out: list[NodeFact | EdgeFact] = [
        _profile_run_node(
            repo=repo,
            deployment=deployment,
            profile_run_id=run_id,
            target_kind=str(doc.get("target_kind") or "okg"),
            command=str(doc.get("command") or ""),
            started_at=str(doc.get("started_at") or datetime.now(timezone.utc).isoformat()),
            duration_ms=_duration_ms(doc.get("duration_ms")),
            generation_id=_int_or_none(doc.get("generation_id")),
            policy=doc.get("policy") if isinstance(doc.get("policy"), dict) else {},
            collectors=_collectors(doc),
            summary=_summary_text(doc.get("summary")),
            artifact=artifact,
            revision=revision,
            request_path=str(
                doc.get("request_path") or doc.get("workflow_id") or "",
            ),
            owner_id=str(doc.get("owner_id") or ""),
            owner_meta=(
                doc.get("owner_meta")
                if isinstance(doc.get("owner_meta"), dict) else {}
            ),
            source_sync=(
                doc.get("source_sync")
                if isinstance(doc.get("source_sync"), dict) else {}
            ),
            collector_evidence=(
                doc.get("evidence")
                if isinstance(doc.get("evidence"), dict) else {}
            ),
            artifacts=(
                doc.get("artifacts")
                if isinstance(doc.get("artifacts"), list) else []
            ),
        )
    ]
    for idx, stage in enumerate(doc.get("stages", []) or []):
        if not isinstance(stage, dict):
            continue
        node = _stage_node(repo, run_id, idx, stage, artifact, revision)
        out.append(node)
        out.append(_contains_edge(repo, run_id, node.node_id, artifact, revision))
    for idx, query in enumerate(doc.get("queries", []) or []):
        if not isinstance(query, dict):
            continue
        node = _query_node(repo, run_id, idx, query, artifact, revision)
        out.append(node)
        out.append(_contains_edge(repo, run_id, node.node_id, artifact, revision))
    hotspots = _okg_hotspots(run_id, doc, max_hotspots)
    for hotspot in hotspots:
        out.append(_hotspot_node(repo, run_id, hotspot, artifact, revision))
        out.append(_contains_edge(repo, run_id, hotspot["hotspot_id"], artifact, revision))
    return out


def _profile_run_node(
    *,
    repo: str,
    deployment: str,
    profile_run_id: str,
    target_kind: str,
    command: str,
    started_at: str,
    duration_ms: int,
    generation_id: int | None,
    policy: dict[str, Any],
    collectors: list[str],
    summary: str,
    artifact: str,
    revision: dict[str, Any],
    request_path: str = "",
    owner_id: str = "",
    owner_meta: dict[str, Any] | None = None,
    source_sync: dict[str, Any] | None = None,
    collector_evidence: dict[str, Any] | None = None,
    artifacts: list[Any] | None = None,
) -> NodeFact:
    attrs: dict[str, Any] = {
        "profile_run_id": profile_run_id,
        "repo": repo,
        "deployment": deployment,
        "target_kind": target_kind or "unknown",
        "command": command,
        "started_at": started_at,
        "duration_ms": duration_ms,
        "policy": policy,
        "collectors": collectors,
        "summary": summary,
        "artifact": artifact,
        "request_path": request_path,
        "workflow_id": request_path,
        "owner_id": owner_id,
        "owner_meta": owner_meta or {},
        "source_sync": source_sync or {},
        "collector_evidence": collector_evidence or {},
        "artifacts": artifacts or [],
        **_artifact_metadata_attrs(revision),
        "text": f"{deployment} {target_kind} {command} {summary}".strip(),
    }
    if generation_id is not None:
        attrs["generation_id"] = generation_id
    return NodeFact(
        profile_run_id,
        "profile_run",
        attrs,
        {
            "repo": repo,
            "deployment": deployment,
            "artifact": artifact,
            "profile_run_id": profile_run_id,
        },
        revision,
    )


def _stage_node(
    repo: str,
    run_id: str,
    idx: int,
    stage: dict[str, Any],
    artifact: str,
    revision: dict[str, Any],
) -> NodeFact:
    name = str(stage.get("name") or f"stage-{idx + 1}")
    stage_id = f"profile-stage:{_sha(json.dumps([run_id, idx, name], sort_keys=True))}"
    stage_attrs = (
        stage.get("attrs") if isinstance(stage.get("attrs"), dict) else {}
    )
    attrs = {
        "stage_id": stage_id,
        "repo": repo,
        "profile_run_id": run_id,
        "name": name,
        "duration_ms": _duration_ms(stage.get("duration_ms") or stage.get("duration_ns")),
        "parent": stage.get("parent"),
        "attrs": stage_attrs,
        "source_name": _first_stage_attr(stage, stage_attrs, "source_name"),
        "stage_name": _first_stage_attr(stage, stage_attrs, "stage_name") or name,
        "enricher_name": _first_stage_attr(stage, stage_attrs, "enricher_name"),
        "workflow_id": _first_stage_attr(stage, stage_attrs, "workflow_id"),
        "owner_id": _first_stage_attr(stage, stage_attrs, "owner_id"),
        "next_action": _first_stage_attr(stage, stage_attrs, "next_action"),
        "input_count": _int_or_none(_first_stage_attr(stage, stage_attrs, "input_count")),
        "output_count": _int_or_none(_first_stage_attr(stage, stage_attrs, "output_count")),
        "timing_status": _first_stage_attr(stage, stage_attrs, "timing_status"),
        "timing_unavailable_reason": _first_stage_attr(
            stage,
            stage_attrs,
            "timing_unavailable_reason",
        ),
        "unavailable_reason": _first_stage_attr(
            stage,
            stage_attrs,
            "unavailable_reason",
        ),
        "nodes_appended": _int_or_none(_first_stage_attr(stage, stage_attrs, "nodes_appended")),
        "edges_appended": _int_or_none(_first_stage_attr(stage, stage_attrs, "edges_appended")),
        "retract_n": _int_or_none(_first_stage_attr(stage, stage_attrs, "retract_n")),
        "retract_e": _int_or_none(_first_stage_attr(stage, stage_attrs, "retract_e")),
        "timings_ms": (
            stage_attrs.get("timings_ms")
            if isinstance(stage_attrs.get("timings_ms"), dict) else {}
        ),
        "source_stats": (
            stage_attrs.get("source_stats")
            if isinstance(stage_attrs.get("source_stats"), dict) else {}
        ),
        "artifact": artifact,
        **_artifact_metadata_attrs(revision),
        "text": f"{name} {_duration_ms(stage.get('duration_ms') or stage.get('duration_ns'))}ms",
    }
    return NodeFact(
        stage_id,
        "profile_stage",
        attrs,
        {"repo": repo, "artifact": artifact, "stage_id": stage_id},
        revision,
    )


def _query_node(
    repo: str,
    run_id: str,
    idx: int,
    query: dict[str, Any],
    artifact: str,
    revision: dict[str, Any],
) -> NodeFact:
    fingerprint = str(query.get("fingerprint") or _sha(json.dumps(query, sort_keys=True, default=str)))
    query_id = f"profile-query:{_sha(json.dumps([run_id, idx, fingerprint], sort_keys=True))}"
    attrs = {
        "query_id": query_id,
        "repo": repo,
        "profile_run_id": run_id,
        "fingerprint": fingerprint,
        "duration_ms": _duration_ms(query.get("duration_ms") or query.get("duration_ns")),
        "stage": str(query.get("stage") or ""),
        "rowcount": _int_or_none(query.get("rowcount")),
        "sql_shape": str(query.get("sql_shape") or ""),
        "workflow_id": str(
            query.get("workflow_id") or query.get("request_path") or "",
        ),
        "artifact": artifact,
        **_artifact_metadata_attrs(revision),
        "text": f"{fingerprint} {query.get('stage') or ''} {query.get('sql_shape') or ''}".strip(),
    }
    return NodeFact(
        query_id,
        "profile_query",
        attrs,
        {"repo": repo, "artifact": artifact, "query_id": query_id},
        revision,
    )


def _hotspot_node(
    repo: str,
    run_id: str,
    hotspot: dict[str, Any],
    artifact: str,
    revision: dict[str, Any],
) -> NodeFact:
    attrs = {
        "hotspot_id": hotspot["hotspot_id"],
        "repo": repo,
        "profile_run_id": run_id,
        "kind": hotspot["kind"],
        "name": hotspot["name"],
        "duration_ms": _duration_ms(hotspot.get("duration_ms")),
        "rank": _int_or_one(hotspot.get("rank")),
        "path": hotspot.get("path") or "",
        "line": _int_or_none(hotspot.get("line")),
        "symbol_hint": hotspot.get("symbol_hint") or "",
        "asset_hint": hotspot.get("asset_hint") or "",
        "artifact": artifact,
        **_artifact_metadata_attrs(revision),
        "text": f"{hotspot['kind']} {hotspot['name']} {hotspot.get('path') or ''}".strip(),
    }
    return NodeFact(
        hotspot["hotspot_id"],
        "profile_hotspot",
        attrs,
        {
            "repo": repo,
            "artifact": artifact,
            "hotspot_id": hotspot["hotspot_id"],
        },
        revision,
    )


def _benchmark_node(row: dict[str, Any], revision: dict[str, Any]) -> NodeFact:
    attrs = {
        "measurement_id": row["measurement_id"],
        "repo": row["repo"],
        "scenario": row["scenario"],
        "metric": row["metric"],
        "value": float(row["value"]),
        "unit": row["unit"],
        "status": row["status"],
        "baseline_value": _float_or_none(row.get("baseline_value")),
        "threshold": _float_or_none(row.get("threshold")),
        "generation_id": _int_or_none(row.get("generation_id")),
        "profile_run_id": row.get("profile_run_id") or "",
        "source_name": row.get("source_name") or "",
        "stage_name": row.get("stage_name") or "",
        "enricher_name": row.get("enricher_name") or "",
        "workflow_id": row.get("workflow_id") or "",
        "owner_id": row.get("owner_id") or "",
        "next_action": row.get("next_action") or "",
        "percentile": row.get("percentile") or "",
        "input_count": _int_or_none(row.get("input_count")),
        "output_count": _int_or_none(row.get("output_count")),
        "artifact": row["artifact"],
        **_artifact_metadata_attrs(revision),
        "text": f"{row['scenario']} {row['metric']}={row['value']} {row['unit']} {row['status']}",
    }
    return NodeFact(
        row["measurement_id"],
        "benchmark_measurement",
        attrs,
        {
            "repo": row["repo"],
            "artifact": row["artifact"],
            "measurement_id": row["measurement_id"],
        },
        revision,
    )


def _performance_budget_node(row: dict[str, Any], revision: dict[str, Any]) -> NodeFact:
    attrs = {
        "budget_id": row["budget_id"],
        "repo": row["repo"],
        "scope": row["scope"],
        "metric": row["metric"],
        "threshold": float(row["threshold"]),
        "unit": row["unit"],
        "operator": row.get("operator") or "<=",
        "source": row.get("source") or "",
        "criterion_id": row.get("criterion_id") or "",
        **_artifact_metadata_attrs(revision),
        "text": f"{row['scope']} {row['metric']} {row.get('operator') or '<='} {row['threshold']} {row['unit']}",
    }
    return NodeFact(
        row["budget_id"],
        "performance_budget",
        attrs,
        {
            "repo": row["repo"],
            "artifact": row.get("source") or "performance_budget",
            "budget_id": row["budget_id"],
            "source": row.get("source") or "",
        },
        revision,
    )


def _contains_edge(
    repo: str,
    run_id: str,
    child_id: str,
    artifact: str,
    revision: dict[str, Any],
) -> EdgeFact:
    return EdgeFact(
        src=run_id,
        dst=child_id,
        edge_type="contains",
        attrs={"artifact": artifact},
        source_record_id={
            "repo": repo,
            "artifact": artifact,
            "src": run_id,
            "dst": child_id,
        },
        source_revision=revision,
    )


def _first_stage_attr(
    stage: dict[str, Any],
    attrs: dict[str, Any],
    key: str,
) -> Any:
    value = stage.get(key)
    if value in (None, ""):
        value = attrs.get(key)
    aliases = {
        "source_name": ("source",),
        "stage_name": ("stage",),
        "workflow_id": ("request_path",),
        "owner_id": ("owner",),
    }
    for alias in aliases.get(key, ()):
        if value not in (None, ""):
            break
        value = stage.get(alias)
        if value in (None, ""):
            value = attrs.get(alias)
    return value


def _okg_hotspots(
    run_id: str,
    doc: dict[str, Any],
    max_hotspots: int,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for stage in doc.get("stages", []) or []:
        if isinstance(stage, dict):
            candidates.append({
                "kind": "stage",
                "name": str(stage.get("name") or "stage"),
                "duration_ms": _duration_ms(stage.get("duration_ms") or stage.get("duration_ns")),
                "path": str(stage.get("path") or stage.get("file") or ""),
                "line": _int_or_none(stage.get("line")),
                "symbol_hint": str(stage.get("name") or ""),
            })
    for query in doc.get("queries", []) or []:
        if isinstance(query, dict):
            name = str(query.get("fingerprint") or query.get("sql_shape") or "query")
            candidates.append({
                "kind": "query",
                "name": name,
                "duration_ms": _duration_ms(query.get("duration_ms") or query.get("duration_ns")),
                "path": str(query.get("path") or query.get("file") or ""),
                "line": _int_or_none(query.get("line")),
                "asset_hint": str(query.get("asset_hint") or ""),
                "symbol_hint": str(query.get("sql_shape") or name),
            })
    candidates.sort(key=lambda row: float(row.get("duration_ms") or 0), reverse=True)
    return _ranked_hotspots(run_id, candidates[:max_hotspots])


def _external_hotspots(
    run_id: str,
    artifact: str,
    doc: dict[str, Any],
    max_hotspots: int,
) -> list[dict[str, Any]]:
    tool = _profile_tool(doc, artifact)
    if _looks_like_speedscope(doc):
        return _speedscope_hotspots(run_id, doc, max_hotspots)
    if tool == "scalene":
        return _generic_profiler_hotspots(run_id, doc, "scalene", max_hotspots)
    if tool == "memray":
        return _generic_profiler_hotspots(run_id, doc, "memray", max_hotspots)
    return _generic_profiler_hotspots(run_id, doc, tool, max_hotspots)


def _speedscope_hotspots(
    run_id: str,
    doc: dict[str, Any],
    max_hotspots: int,
) -> list[dict[str, Any]]:
    frames = ((doc.get("shared") or {}).get("frames") or [])
    totals: dict[int, float] = {}
    for profile in doc.get("profiles", []) or []:
        if not isinstance(profile, dict):
            continue
        samples = profile.get("samples") or []
        weights = profile.get("weights") or []
        unit = str(profile.get("unit") or "seconds")
        for idx, sample in enumerate(samples):
            if not isinstance(sample, list) or not sample:
                continue
            frame_idx = sample[-1]
            if not isinstance(frame_idx, int):
                continue
            weight = weights[idx] if idx < len(weights) else 1
            totals[frame_idx] = totals.get(frame_idx, 0.0) + _unit_to_ms(weight, unit)
    candidates: list[dict[str, Any]] = []
    for frame_idx, duration_ms in totals.items():
        if frame_idx >= len(frames) or not isinstance(frames[frame_idx], dict):
            continue
        frame = frames[frame_idx]
        candidates.append({
            "kind": "frame",
            "name": str(frame.get("name") or "frame"),
            "duration_ms": duration_ms,
            "path": str(frame.get("file") or ""),
            "line": _int_or_none(frame.get("line")),
            "symbol_hint": str(frame.get("name") or ""),
        })
    candidates.sort(key=lambda row: float(row.get("duration_ms") or 0), reverse=True)
    return _ranked_hotspots(run_id, candidates[:max_hotspots])


def _generic_profiler_hotspots(
    run_id: str,
    doc: dict[str, Any],
    kind: str,
    max_hotspots: int,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    def visit(value: Any, path_hint: str = "") -> None:
        if isinstance(value, dict):
            name = value.get("function") or value.get("name") or value.get("symbol")
            duration = (
                value.get("duration_ms")
                or value.get("time_ms")
                or value.get("time")
                or value.get("total_time")
                or value.get("self_time")
                or value.get("size")
                or value.get("allocated")
            )
            if name and duration is not None:
                candidates.append({
                    "kind": kind,
                    "name": str(name),
                    "duration_ms": _duration_ms(duration),
                    "path": str(value.get("file") or value.get("path") or path_hint),
                    "line": _int_or_none(value.get("line")),
                    "symbol_hint": str(name),
                })
            for key, nested in value.items():
                next_path = path_hint
                if key in {"file", "path"} and isinstance(nested, str):
                    next_path = nested
                visit(nested, next_path)
        elif isinstance(value, list):
            for item in value:
                visit(item, path_hint)

    visit(doc)
    candidates.sort(key=lambda row: float(row.get("duration_ms") or 0), reverse=True)
    return _ranked_hotspots(run_id, candidates[:max_hotspots])


def _ranked_hotspots(run_id: str, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rank, row in enumerate(candidates, start=1):
        hotspot_id = f"profile-hotspot:{_sha(json.dumps([run_id, rank, row], sort_keys=True, default=str))}"
        out.append({**row, "hotspot_id": hotspot_id, "rank": rank})
    return out


def _benchmark_rows(
    repo: str,
    artifact: str,
    doc: dict[str, Any],
    budget_rows: Sequence[dict[str, Any]] = (),
) -> list[dict[str, Any]]:
    if is_source_sync_benchmark(doc):
        rows = source_sync_benchmark_measurement_rows(
            repo=repo,
            artifact=artifact,
            doc=doc,
        )
        for row in rows:
            matched_budgets = _matching_budgets(row, budget_rows)
            if row.get("threshold") is None and matched_budgets:
                row["threshold"] = matched_budgets[0]["threshold"]
                operator = matched_budgets[0].get("operator")
                if str(row.get("status") or "") in {"informational", "unknown"}:
                    row["status"] = _benchmark_status(
                        row["value"],
                        row["threshold"],
                        operator,
                    )
        return rows
    rows: list[dict[str, Any]] = []
    raw_rows = doc.get("measurements") or doc.get("benchmarks") or []
    if isinstance(raw_rows, dict):
        raw_rows = list(raw_rows.values())
    for idx, raw in enumerate(raw_rows):
        if not isinstance(raw, dict):
            continue
        stats = raw.get("stats") if isinstance(raw.get("stats"), dict) else {}
        scenario = str(raw.get("scenario") or raw.get("name") or raw.get("fullname") or f"benchmark-{idx + 1}")
        metric = str(raw.get("metric") or ("mean" if "mean" in stats else "value"))
        value = raw.get("value")
        if value is None and stats:
            value = stats.get(metric) or stats.get("mean")
        if value is None:
            continue
        unit = str(raw.get("unit") or doc.get("unit") or ("seconds" if stats else ""))
        matched_budgets = _matching_budgets(
            {
                "scenario": scenario,
                "metric": metric,
                "unit": unit or "unknown",
            },
            budget_rows,
        )
        threshold = raw.get("threshold")
        operator = raw.get("operator")
        threshold_from_budget = False
        if threshold is None and matched_budgets:
            threshold = matched_budgets[0]["threshold"]
            operator = matched_budgets[0].get("operator")
            threshold_from_budget = True
        raw_status = raw.get("status")
        status = str(raw_status or _benchmark_status(value, threshold, operator))
        if threshold_from_budget and status in {"informational", "unknown"}:
            status = _benchmark_status(value, threshold, operator)
        profile_run_id = str(raw.get("profile_run_id") or doc.get("profile_run_id") or "")
        if profile_run_id and not profile_run_id.startswith("profile-run:"):
            profile_run_id = _profile_run_id(str(doc.get("deployment") or ""), profile_run_id)
        measurement_id = str(raw.get("measurement_id") or "")
        if not measurement_id:
            measurement_id = f"benchmark:{_sha(json.dumps([repo, artifact, idx, scenario, metric], sort_keys=True))}"
        source_name = (
            raw.get("source_name")
            or raw.get("source")
            or doc.get("source_name")
            or doc.get("source")
            or _source_name_from_source_sync_scenario(scenario)
            or ""
        )
        rows.append({
            "measurement_id": measurement_id,
            "repo": repo,
            "scenario": scenario,
            "metric": metric,
            "value": _float_or_zero(value),
            "unit": unit or "unknown",
            "status": status or "unknown",
            "baseline_value": raw.get("baseline_value") or raw.get("baseline"),
            "threshold": threshold,
            "generation_id": raw.get("generation_id") or doc.get("generation_id"),
            "profile_run_id": profile_run_id,
            "source_name": source_name,
            "stage_name": raw.get("stage_name") or raw.get("stage") or doc.get("stage_name") or doc.get("stage") or "",
            "enricher_name": raw.get("enricher_name") or doc.get("enricher_name") or "",
            "workflow_id": raw.get("workflow_id") or raw.get("request_path") or doc.get("workflow_id") or doc.get("request_path") or "",
            "owner_id": raw.get("owner_id") or raw.get("owner") or doc.get("owner_id") or doc.get("owner") or "",
            "next_action": raw.get("next_action") or doc.get("next_action") or "",
            "percentile": raw.get("percentile") or "",
            "input_count": raw.get("input_count"),
            "output_count": raw.get("output_count"),
            "artifact": artifact,
        })
    return rows


def _performance_budget_rows(
    *,
    repo: str,
    deployment: str,
    repo_root: Path,
    explicit_budgets: Sequence[dict[str, Any]],
    deployment_config_path: Path | str | None,
    openspec_roots: Sequence[Path | str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(explicit_budgets):
        rows.extend(_budget_rows_from_raw(repo, deployment, [raw], f"explicit:{idx}"))
    if deployment_config_path is not None:
        config_path = Path(deployment_config_path).expanduser()
        if not config_path.is_absolute():
            config_path = repo_root / config_path
        rows.extend(_budget_rows_from_deployment_config(repo, deployment, config_path))
    for raw_root in openspec_roots:
        root = Path(raw_root).expanduser()
        if not root.is_absolute():
            root = repo_root / root
        rows.extend(_budget_rows_from_openspec(repo, deployment, root))

    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row["budget_id"] not in deduped:
            deduped[row["budget_id"]] = row
    return list(deduped.values())


def _budget_rows_from_deployment_config(
    repo: str,
    deployment: str,
    path: Path,
) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        doc = fastyaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001 - YAML loader exceptions vary.
        raise SubstrateError(
            "malformed profile-measurement budget YAML",
            data={
                "source": "profile_measurements",
                "artifact": path.name,
            },
            fix_hint=(
                "repair or remove the malformed deployment budget YAML before "
                "running profile_measurements ingestion"
            ),
        ) from exc
    if not isinstance(doc, dict):
        return []
    raw: list[Any] = []
    for key in ("performance_budgets", "budgets"):
        value = doc.get(key)
        if isinstance(value, list):
            raw.extend(value)
    for section_key in ("quality", "profiling", "coding_context"):
        section = doc.get(section_key)
        if isinstance(section, dict):
            value = section.get("performance_budgets")
            if isinstance(value, list):
                raw.extend(value)
    return _budget_rows_from_raw(repo, deployment, raw, f"deployment_config:{path.name}")


def _budget_rows_from_openspec(
    repo: str,
    deployment: str,
    root: Path,
) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.md")):
        text = path.read_text(encoding="utf-8", errors="replace")
        rows.extend(_budget_rows_from_yaml_front_matter(repo, deployment, path, text))
        rows.extend(_budget_rows_from_inline_comments(repo, deployment, path, text))
        rows.extend(_budget_rows_from_budget_lines(repo, deployment, path, text))
    return rows


def _budget_rows_from_yaml_front_matter(
    repo: str,
    deployment: str,
    path: Path,
    text: str,
) -> list[dict[str, Any]]:
    if not text.startswith("---\n"):
        return []
    end = text.find("\n---", 4)
    if end == -1:
        return []
    try:
        doc = fastyaml.safe_load(text[4:end]) or {}
    except Exception as exc:  # noqa: BLE001 - YAML loader exceptions vary.
        raise SubstrateError(
            "malformed profile-measurement budget front matter",
            data={
                "source": "profile_measurements",
                "artifact": path.name,
            },
            fix_hint=(
                "repair or remove the malformed budget front matter before "
                "running profile_measurements ingestion"
            ),
        ) from exc
    if not isinstance(doc, dict):
        return []
    raw = doc.get("performance_budgets")
    return _budget_rows_from_raw(repo, deployment, raw if isinstance(raw, list) else [], f"openspec:{path.name}")


def _budget_rows_from_inline_comments(
    repo: str,
    deployment: str,
    path: Path,
    text: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pattern = re.compile(r"<!--\s*okg:performance-budget\s+(.+?)\s*-->", re.DOTALL)
    for idx, match in enumerate(pattern.finditer(text)):
        body = match.group(1).strip()
        raw: dict[str, Any] | None = None
        if body.startswith("{"):
            try:
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    raw = parsed
            except ValueError:
                raw = None
        if raw is None:
            raw = _key_value_attrs(body)
        rows.extend(_budget_rows_from_raw(repo, deployment, [raw], f"openspec:{path.name}:{idx}"))
    return rows


def _budget_rows_from_budget_lines(
    repo: str,
    deployment: str,
    path: Path,
    text: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pattern = re.compile(
        r"performance_budget:\s*"
        r"(?P<scope>[A-Za-z0-9_.:-]+)\s+"
        r"(?P<metric>[A-Za-z0-9_.:-]+)\s*"
        r"(?P<operator><=|>=)\s*"
        r"(?P<threshold>[0-9]+(?:\.[0-9]+)?)\s*"
        r"(?P<unit>[A-Za-z0-9_/.-]+)"
        r"(?:\s+criterion_id=(?P<criterion_id>[A-Za-z0-9_.:-]+))?",
    )
    for idx, match in enumerate(pattern.finditer(text)):
        raw = match.groupdict()
        rows.extend(_budget_rows_from_raw(repo, deployment, [raw], f"openspec:{path.name}:line:{idx}"))
    return rows


def _budget_rows_from_raw(
    repo: str,
    deployment: str,
    raw_rows: Sequence[Any],
    source: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(raw_rows):
        if not isinstance(raw, dict):
            continue
        scope = str(raw.get("scope") or raw.get("scenario") or "")
        metric = str(raw.get("metric") or "")
        threshold = _float_or_none(raw.get("threshold"))
        unit = str(raw.get("unit") or "")
        if not scope or not metric or threshold is None or not unit:
            continue
        operator = str(raw.get("operator") or raw.get("op") or "<=")
        budget_id = str(raw.get("budget_id") or raw.get("id") or "")
        if not budget_id:
            budget_id = f"performance-budget:{_sha(json.dumps([repo, deployment, source, idx, scope, metric], sort_keys=True))}"
        rows.append({
            "budget_id": budget_id,
            "repo": repo,
            "scope": scope,
            "metric": metric,
            "threshold": threshold,
            "unit": unit,
            "operator": operator,
            "criterion_id": str(raw.get("criterion_id") or raw.get("acceptance_criterion_id") or ""),
            "source": source,
        })
    return rows


def _matching_budgets(
    row: dict[str, Any],
    budget_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    scenario = str(row.get("scenario") or "")
    metric = str(row.get("metric") or "")
    unit = _normalize_unit(str(row.get("unit") or ""))
    return [
        budget
        for budget in budget_rows
        if _budget_scope_matches(str(budget.get("scope") or ""), scenario)
        and str(budget.get("metric") or "") == metric
        and (
            not unit
            or _normalize_unit(str(budget.get("unit") or "")) == unit
        )
    ]


def _budget_scope_matches(scope: str, scenario: str) -> bool:
    if scope == scenario:
        return True
    if scope == "*":
        return True
    if scope.endswith(".*"):
        return scenario.startswith(scope[:-1])
    return False


def _source_name_from_source_sync_scenario(scenario: str) -> str:
    prefix = "source_sync."
    if not scenario.startswith(prefix):
        return ""
    suffix = scenario[len(prefix):]
    if not suffix or suffix in {"all", "inventory"}:
        return ""
    return suffix


def _key_value_attrs(body: str) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    for match in re.finditer(r"([A-Za-z_][A-Za-z0-9_-]*)=(\"[^\"]*\"|'[^']*'|[^\s]+)", body):
        value = match.group(2).strip("\"'")
        attrs[match.group(1).replace("-", "_")] = value
    return attrs


def _benchmark_status(value: Any, threshold: Any, operator: Any) -> str:
    if threshold is None:
        return "unknown"
    actual = _float_or_zero(value)
    limit = _float_or_zero(threshold)
    op = str(operator or "<=")
    if op in {"<=", "max", "under"}:
        return "passed" if actual <= limit else "failed"
    if op in {">=", "min", "over"}:
        return "passed" if actual >= limit else "failed"
    return "unknown"


def _normalize_unit(value: str) -> str:
    text = value.lower()
    mapping = {
        "millisecond": "ms",
        "milliseconds": "ms",
        "msec": "ms",
        "second": "s",
        "seconds": "s",
        "sec": "s",
    }
    return mapping.get(text, text)


def _configured_artifacts(
    repo_root: Path,
    artifact_paths: Sequence[Path | str],
    artifact_globs: Sequence[str],
    max_artifacts: int,
) -> list[Path]:
    paths: list[Path] = []
    for raw in artifact_paths:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = repo_root / path
        if path.is_file():
            paths.append(path.resolve())
    for pattern in artifact_globs:
        paths.extend(path.resolve() for path in repo_root.glob(pattern) if path.is_file())
    deduped = sorted(set(paths))
    if len(deduped) > max_artifacts:
        raise ValueError(
            f"profile measurement source exceeded max_artifacts={max_artifacts}; "
            "tighten artifact_globs or raise the cap"
        )
    return deduped


def _artifact_files(
    repo_root: Path,
    artifact_paths: Sequence[Path | str],
    artifact_globs: Sequence[str],
    max_artifacts: int,
    *,
    sync_scope: Optional[dict[str, Any]] = None,
) -> list[Path]:
    if sync_scope is None:
        return _configured_artifacts(
            repo_root,
            artifact_paths,
            artifact_globs,
            max_artifacts,
        )
    scoped = resolve_scoped_paths(
        sync_scope,
        roots=_scope_roots(repo_root, artifact_paths),
        suffixes=[".json"],
    )
    return sorted({
        path for path in scoped
        if _configured_artifact_matches(
            repo_root,
            path,
            artifact_paths,
            artifact_globs,
        )
    })


def _cursor_artifact_frontier(cursor: Optional[dict[str, Any]]) -> dict[str, str]:
    if not isinstance(cursor, dict):
        return {}
    raw = cursor.get("artifact_frontier")
    if not isinstance(raw, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in raw.items()
        if isinstance(key, str) and isinstance(value, str)
    }


def _changed_artifact_paths(
    repo_root: Path,
    paths: Sequence[Path],
    prior_frontier: dict[str, str],
) -> tuple[list[Path], list[Path]]:
    changed: list[Path] = []
    skipped: list[Path] = []
    for path in paths:
        key = _artifact_frontier_key(repo_root, path)
        fingerprint = _artifact_stat_fingerprint(path)
        if fingerprint and prior_frontier.get(key) == fingerprint:
            skipped.append(path)
            continue
        changed.append(path)
    return changed, skipped


def _artifact_frontier_key(repo_root: Path, path: Path) -> str:
    return _rel_or_name(repo_root, path)


def _artifact_stat_fingerprint(path: Path) -> str | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return f"stat:{stat.st_size}:{stat.st_mtime_ns}:{stat.st_ctime_ns}"


def _scope_roots(
    repo_root: Path,
    artifact_paths: Sequence[Path | str],
) -> tuple[Path, ...]:
    roots = [repo_root]
    for raw in artifact_paths:
        roots.append(_resolve_configured_path(repo_root, raw).parent)
    return tuple(roots)


def _configured_artifact_matches(
    repo_root: Path,
    path: Path,
    artifact_paths: Sequence[Path | str],
    artifact_globs: Sequence[str],
) -> bool:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        return False
    for raw in artifact_paths:
        if resolved == _resolve_configured_path(repo_root, raw):
            return True
    try:
        rel = resolved.relative_to(repo_root)
    except ValueError:
        return False
    rel_posix = rel.as_posix()
    return any(glob_matches_path(rel_posix, pattern) for pattern in artifact_globs)


def _resolve_configured_path(repo_root: Path, raw: Path | str) -> Path:
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except ValueError as exc:
        raise SubstrateError(
            "malformed profile-measurement JSON artifact",
            data={
                "source": "profile_measurements",
                "artifact": path.name,
            },
            fix_hint=(
                "repair or remove the malformed profile or benchmark JSON "
                "artifact before running profile_measurements ingestion"
            ),
        ) from exc


def _revision(path: Path, doc: Any) -> dict[str, Any]:
    return {
        "artifact": path.name,
        "text_hash": hashlib.sha256(
            json.dumps(doc, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest(),
        "mtime": _artifact_mtime_iso(path),
    }


def _artifact_metadata_attrs(revision: dict[str, Any]) -> dict[str, Any]:
    return {
        "artifact_hash": str(revision.get("text_hash") or ""),
        "observed_at": str(revision.get("mtime") or ""),
        "mtime": str(revision.get("mtime") or ""),
    }


def _rel_or_name(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.name


def _looks_like_okg_profile(doc: dict[str, Any]) -> bool:
    return bool(doc.get("profile_run_id")) and (
        "stages" in doc or "queries" in doc or "target_kind" in doc
    )


def _looks_like_speedscope(doc: dict[str, Any]) -> bool:
    return "$schema" in doc and "speedscope" in str(doc.get("$schema")) or (
        isinstance(doc.get("shared"), dict) and isinstance(doc.get("profiles"), list)
    )


def _profile_tool(doc: dict[str, Any], artifact: str) -> str:
    text = " ".join([
        str(doc.get("tool") or ""),
        str(doc.get("profiler") or ""),
        artifact,
    ]).lower()
    if "speedscope" in text:
        return "speedscope"
    if "py-spy" in text or "pyspy" in text:
        return "py-spy"
    if "scalene" in text:
        return "scalene"
    if "memray" in text:
        return "memray"
    return str(doc.get("tool") or doc.get("profiler") or "profiler")


def _profile_run_id(deployment: str, raw: str) -> str:
    return raw if raw.startswith("profile-run:") else f"profile-run:{deployment}:{raw}"


def _external_profile_run_id(deployment: str, artifact: str, doc: dict[str, Any]) -> str:
    raw = str(doc.get("profile_run_id") or doc.get("run_id") or _sha(artifact))
    return _profile_run_id(deployment, raw)


def _collectors(doc: dict[str, Any]) -> list[str]:
    collectors = doc.get("collectors")
    if isinstance(collectors, list):
        return [str(item) for item in collectors]
    evidence = doc.get("evidence")
    if isinstance(evidence, dict):
        return [str(key) for key in evidence]
    return []


def _summary_text(summary: Any) -> str:
    if isinstance(summary, dict):
        return " ".join(
            str(summary.get(key) or "")
            for key in ("bottleneck_class", "top_stage", "top_query")
        ).strip()
    return str(summary or "")


def _artifact_mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()


def _duration_ms(value: Any) -> int:
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return 0
    if value is None:
        return 0
    number = float(value)
    if number > 1_000_000:
        return int(number / 1_000_000)
    return int(number)


def _unit_to_ms(value: Any, unit: str) -> float:
    number = _float_or_zero(value)
    text = unit.lower()
    if text.startswith("nano"):
        return number / 1_000_000
    if text.startswith("micro"):
        return number / 1_000
    if text.startswith("milli"):
        return number
    if text.startswith("second") or text in {"s", "sec"}:
        return number * 1000
    return number


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_or_one(value: Any) -> int:
    return _int_or_none(value) or 1


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _float_or_zero(value: Any) -> float:
    return _float_or_none(value) or 0.0


def _dedupe_facts(facts: list[NodeFact | EdgeFact]) -> list[NodeFact | EdgeFact]:
    out: list[NodeFact | EdgeFact] = []
    seen: set[tuple[str, ...]] = set()
    for fact in facts:
        if isinstance(fact, NodeFact):
            key = (
                "node",
                fact.node_id,
                fact.op,
                json.dumps(fact.source_revision, sort_keys=True, default=str),
            )
        else:
            key = (
                "edge",
                fact.src,
                fact.dst,
                fact.edge_type,
                fact.op,
                json.dumps(fact.source_revision, sort_keys=True, default=str),
            )
        if key in seen:
            continue
        seen.add(key)
        out.append(fact)
    return out
