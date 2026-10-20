"""Self-play report source for okg-workspace agent-evaluation loops."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

from okg.substrate.library.sources._path_scope import (
    glob_matches_path,
    path_scope_requested,
    resolve_scoped_paths,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)
from okg.substrate.unknown import Unknown, read_ok


__all__ = ["SelfPlayReportSource"]


_EVIDENCE_PREFIX_SUBTYPE: tuple[tuple[str, str], ...] = (
    ("symbol:", "code_symbol"),
    ("code-ref:", "code_reference"),
    ("import:", "code_import"),
    ("endpoint:", "interface_endpoint"),
    ("interface:", "interface_endpoint"),
    ("asset:", "data_asset"),
    ("config:", "config_key"),
    ("change:", "open_spec_change"),
    ("task:", "proposal_task"),
    ("crit:", "acceptance_criterion"),
    ("scn:", "acceptance_scenario"),
    ("test-report:", "test_report"),
    ("test-run:", "test_run"),
    ("test-case:", "test_case"),
    ("security-finding:", "security_finding"),
    ("secret-finding:", "secret_finding"),
    ("vuln:", "dependency_vulnerability"),
    ("profile-run:", "profile_run"),
    ("profile-stage:", "profile_stage"),
    ("profile-query:", "profile_query"),
    ("profile-hotspot:", "profile_hotspot"),
    ("benchmark:", "benchmark_measurement"),
    ("performance-budget:", "performance_budget"),
    ("self-play-run:", "self_play_run"),
    ("self-play-scenario:", "self_play_scenario"),
    ("self-play-finding:", "self_play_finding"),
    ("message:", "agent_message"),
    ("tool_use:", "tool_use_event"),
    ("session:", "agent_session"),
    ("subsession:", "sub_agent_session"),
    ("commit:", "git_commit"),
    ("issue:", "issue"),
    ("pr:", "pull_request"),
    ("doc:", "document"),
    ("chunk:", "document_chunk"),
    ("mention:", "entity_mention"),
    ("file:", "source_file"),
)


def _sha(text: str, n: int = 16) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:n]


def _slug(value: Any, *, fallback: str = "unknown") -> str:
    raw = str(value or fallback).strip().lower()
    out = []
    for ch in raw:
        if ch.isalnum():
            out.append(ch)
        elif ch in {"-", "_", ".", ":"}:
            out.append("-")
    slug = "".join(out).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or fallback


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _node_subtype_for_id(node_id: str) -> Optional[str]:
    for prefix, subtype in _EVIDENCE_PREFIX_SUBTYPE:
        if node_id.startswith(prefix):
            return subtype
    return None


def _known_evidence_ids(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        if value in seen:
            continue
        if _node_subtype_for_id(value) is None:
            continue
        seen.add(value)
        out.append(value)
    return out


def _stable_finding_id(
    value: Any,
    *,
    run_key: str,
    scenario_slug: str,
) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.startswith("self-play-finding:"):
        return raw
    return (
        f"self-play-finding:{run_key}:{scenario_slug}:"
        f"{_slug(raw, fallback='manual')}-{_sha(raw, 12)}"
    )


def _dedupe_facts(
    facts: list[NodeFact | EdgeFact],
) -> list[NodeFact | EdgeFact]:
    nodes: dict[tuple[str, str], NodeFact] = {}
    edges: dict[tuple[str, str, str, str], EdgeFact] = {}
    for fact in facts:
        if isinstance(fact, NodeFact):
            nodes[(fact.node_id, fact.op)] = fact
        else:
            edges[(fact.src, fact.edge_type, fact.dst, fact.op)] = fact
    return [*nodes.values(), *edges.values()]


@dataclass
class SelfPlayReportSource:
    repo: str
    repo_root: Path | str
    report_paths: Sequence[Path | str] = ()
    report_globs: Sequence[str] = ()
    report_dirs: Sequence[Path | str] = ()
    deployment: str = "unknown"
    max_artifacts: int = 100
    max_evidence_edges_per_finding: int = 40

    name: str = "self_play_reports"
    profile: str = "discovery_crawl"
    validation_warnings: list[str] = field(default_factory=list, init=False)

    # Change-probe contract (#323): no cheaper churn-immune probe exists for a
    # non-git JSON report drop, so this falls to the O(content) floor. The probe
    # re-uses the source's OWN full-scope enumeration (_report_files()), so the
    # probe scope is provably equal to the read scope and a deleted report drops
    # out of the next enumeration (caught by ContentHashProbe membership digest).
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Path]]:
            return [(str(path), path) for path in self._report_files()]

        return ContentHashProbe(
            content_items=_items,
            config={
                "repo": self.repo,
                "deployment": self.deployment,
                "report_paths": [str(p) for p in self.report_paths],
                "report_globs": [str(g) for g in self.report_globs],
                "report_dirs": [str(d) for d in self.report_dirs],
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
        self.validation_warnings.clear()
        del cursor
        facts: list[NodeFact | EdgeFact] = []
        scoped = path_scope_requested(sync_scope)
        report_paths = self._report_files(sync_scope if scoped else None)
        meta_paths = self._report_files() if scoped else report_paths
        report_meta = {
            path: meta for path in meta_paths
            if (meta := self._report_meta(path)) is not None
        }
        latest_generation_by_deployment: dict[str, int] = {}
        for deployment, generation_id in report_meta.values():
            if generation_id is None:
                continue
            latest_generation_by_deployment[deployment] = max(
                latest_generation_by_deployment.get(deployment, -1),
                generation_id,
            )

        for path in report_paths:
            superseded_generation_id = None
            meta = report_meta.get(path)
            if meta is not None:
                deployment, generation_id = meta
                latest_generation_id = latest_generation_by_deployment.get(
                    deployment,
                )
                if (
                    generation_id is not None
                    and latest_generation_id is not None
                    and generation_id < latest_generation_id
                ):
                    superseded_generation_id = latest_generation_id
            facts.extend(self._facts_for_report(
                path,
                superseded_generation_id=superseded_generation_id,
            ))
        return SourceRun(
            facts=iter(_dedupe_facts(facts)),
            completed_scope=(
                not scoped and mode in {"reconcile", "scope_complete"}
            ),
            next_cursor={"run_id": run_id},
            run_mode=mode,
        )

    def _report_files(
        self,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> list[Path]:
        if sync_scope is not None:
            return self._scoped_report_files(sync_scope)
        paths: list[Path] = []
        for configured in self.report_paths:
            path = self._resolve_path(configured)
            if path.is_file() and path.suffix.lower() == ".json":
                paths.append(path)
        for configured_dir in self.report_dirs:
            directory = self._resolve_path(configured_dir)
            if directory.is_dir():
                paths.extend(directory.glob("*.json"))
        for pattern in self.report_globs:
            paths.extend(self.repo_root.glob(str(pattern)))
        deduped = sorted({p.resolve() for p in paths if p.is_file()})
        if len(deduped) > self.max_artifacts:
            raise ValueError(
                "self-play source exceeded "
                f"max_artifacts={self.max_artifacts}; tighten "
                "report_globs/report_dirs or raise the cap"
            )
        return deduped

    def _scoped_report_files(self, sync_scope: dict[str, Any]) -> list[Path]:
        scoped = resolve_scoped_paths(
            sync_scope,
            roots=self._scope_roots(),
            suffixes=[".json"],
        )
        return sorted({
            path for path in scoped
            if self._is_configured_report(path)
        })

    def _scope_roots(self) -> tuple[Path, ...]:
        roots = [Path(self.repo_root)]
        for raw in self.report_paths:
            roots.append(self._resolve_path(raw).resolve().parent)
        for raw in self.report_dirs:
            roots.append(self._resolve_path(raw).resolve())
        return tuple(roots)

    def _is_configured_report(self, path: Path) -> bool:
        resolved = path.expanduser().resolve()
        if not resolved.is_file() or resolved.suffix.lower() != ".json":
            return False
        for raw in self.report_paths:
            if resolved == self._resolve_path(raw).resolve():
                return True
        for raw in self.report_dirs:
            directory = self._resolve_path(raw).resolve()
            try:
                rel = resolved.relative_to(directory)
            except ValueError:
                continue
            if len(rel.parts) == 1:
                return True
        try:
            rel = resolved.relative_to(self.repo_root)
        except ValueError:
            return False
        rel_posix = rel.as_posix()
        return any(
            glob_matches_path(rel_posix, pattern)
            for pattern in self.report_globs
        )

    def _resolve_path(self, value: Path | str) -> Path:
        path = Path(value).expanduser()
        if path.is_absolute():
            return path
        return (self.repo_root / path).resolve()

    def _relpath(self, path: Path) -> str:
        try:
            return str(path.resolve().relative_to(self.repo_root)).replace(
                "\\", "/",
            )
        except ValueError:
            return str(path.resolve())

    def _report_meta(
        self,
        path: Path,
    ) -> Optional[tuple[str, Optional[int]]]:
        try:
            report = json.loads(_read_text(path))
        except json.JSONDecodeError:
            return None
        if not isinstance(report, dict):
            return None
        deployment = str(report.get("deployment") or self.deployment)
        return deployment, _int_or_none(report.get("generation_id"))

    def _facts_for_report(
        self,
        path: Path,
        *,
        superseded_generation_id: Optional[int] = None,
    ) -> list[NodeFact | EdgeFact]:
        raw = _read_text(path)
        text_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        try:
            report = json.loads(raw)
        except json.JSONDecodeError as exc:
            self.validation_warnings.append(
                f"{path}: invalid JSON self-play report: {exc}",
            )
            return []
        if not isinstance(report, dict):
            self.validation_warnings.append(
                f"{path}: self-play report root is not an object",
            )
            return []

        rel_path = self._relpath(path)
        revision = {"path": rel_path, "text_hash": text_hash}
        report_format = str(report.get("report_format") or "unknown")
        deployment = str(report.get("deployment") or self.deployment)
        generation_id = _int_or_none(report.get("generation_id"))
        generation_label = (
            f"gen{generation_id}" if generation_id is not None else "genunknown"
        )
        run_key = _sha(f"{deployment}:{generation_label}:{rel_path}")
        run_node_id = f"self-play-run:{deployment}:{generation_label}:{run_key}"
        scenarios = [
            s for s in report.get("scenario_results", [])
            if isinstance(s, dict)
        ]

        scenario_finding_counts: dict[str, int] = {}
        scenario_findings: dict[str, list[dict[str, Any]]] = {}
        for scenario in scenarios:
            scenario_id = str(scenario.get("scenario_id") or "unknown")
            findings = self._findings_for_scenario(
                scenario,
                run_key=run_key,
                run_node_id=run_node_id,
                rel_path=rel_path,
                generation_id=generation_id,
                superseded_generation_id=superseded_generation_id,
            )
            scenario_findings[scenario_id] = findings
            scenario_finding_counts[scenario_id] = len(findings)

        graph_summary = report.get("graph_summary") or {}
        finding_count = sum(scenario_finding_counts.values())
        answerability_scores = [
            score for scenario in scenarios
            if (score := _answerability_score(scenario)) is not None
        ]
        shell_fallback_count = sum(
            len(scenario.get("shell_fallbacks", []) or [])
            for scenario in scenarios
        )
        failed_assumption_count = sum(
            len(scenario.get("failed_assumptions", []) or [])
            for scenario in scenarios
        )
        tool_failure_count = sum(
            _tool_failure_count(scenario) for scenario in scenarios
        )
        manual_review_required_count = sum(
            1 for scenario in scenarios
            if bool(
                (scenario.get("answerability") or {}).get(
                    "manual_review_required", False,
                )
            )
        )
        facts: list[NodeFact | EdgeFact] = [
            NodeFact(
                node_id=run_node_id,
                subtype="self_play_run",
                attrs={
                    "run_id": run_node_id,
                    "deployment": deployment,
                    "artifact_path": rel_path,
                    "report_format": report_format,
                    "generation_id": generation_id,
                    "catalog_version_id": report.get("catalog_version_id"),
                    "scenario_count": len(scenarios),
                    "finding_count": finding_count,
                    "answerability_score_avg": (
                        sum(answerability_scores) / len(answerability_scores)
                        if answerability_scores else None
                    ),
                    "answerability_score_min": (
                        min(answerability_scores)
                        if answerability_scores else None
                    ),
                    "answerability_score_max": (
                        max(answerability_scores)
                        if answerability_scores else None
                    ),
                    "shell_fallback_count": shell_fallback_count,
                    "failed_assumption_count": failed_assumption_count,
                    "tool_failure_count": tool_failure_count,
                    "manual_review_required_count": (
                        manual_review_required_count
                    ),
                    "created_at": report.get("created_at"),
                    "node_count": graph_summary.get("node_count"),
                    "edge_count": graph_summary.get("edge_count"),
                    "text": (
                        f"{deployment} self-play report {rel_path} "
                        f"against {generation_label}: {len(scenarios)} "
                        f"scenarios, {finding_count} findings"
                    ),
                },
                source_record_id={
                    "repo": self.repo,
                    "path": rel_path,
                    "kind": "self_play_run",
                },
                source_revision=revision,
                op="I",
            )
        ]

        for scenario in scenarios:
            facts.extend(self._scenario_facts(
                scenario,
                run_node_id=run_node_id,
                run_key=run_key,
                rel_path=rel_path,
                revision=revision,
                findings=scenario_findings.get(
                    str(scenario.get("scenario_id") or "unknown"), [],
                ),
            ))

        return facts

    def _scenario_facts(
        self,
        scenario: dict[str, Any],
        *,
        run_node_id: str,
        run_key: str,
        rel_path: str,
        revision: dict[str, Any],
        findings: list[dict[str, Any]],
    ) -> list[NodeFact | EdgeFact]:
        scenario_id = str(scenario.get("scenario_id") or "unknown")
        scenario_slug = _slug(scenario_id)
        scenario_node_id = (
            f"self-play-scenario:{run_key}:{scenario_slug}"
        )
        answerability = scenario.get("answerability") or {}
        tool_failure_count = _tool_failure_count(scenario)
        attrs = {
            "scenario_result_id": scenario_node_id,
            "run_id": run_node_id,
            "scenario_id": scenario_id,
            "question_class": str(
                scenario.get("question_class") or "unknown",
            ),
            "substrate_stage_target": str(
                scenario.get("substrate_stage_target") or "unknown",
            ),
            "question": str(scenario.get("question") or "").strip(),
            "answerability_score": _float_or_none(
                answerability.get("score"),
            ),
            "answerability_status": answerability.get("status"),
            "manual_review_required": bool(
                answerability.get("manual_review_required", False),
            ),
            "expected_evidence_subtypes": scenario.get(
                "expected_evidence_subtypes", [],
            ),
            "observed_subtype_counts": scenario.get(
                "observed_subtype_counts", {},
            ),
            "shell_fallback_count": len(
                scenario.get("shell_fallbacks", []) or [],
            ),
            "failed_assumption_count": len(
                scenario.get("failed_assumptions", []) or [],
            ),
            "tool_failure_count": tool_failure_count,
            "text": (
                f"{scenario_id} self-play scenario "
                f"({scenario.get('question_class') or 'unknown'}): "
                f"{len(findings)} findings"
            ),
        }
        facts: list[NodeFact | EdgeFact] = [
            NodeFact(
                node_id=scenario_node_id,
                subtype="self_play_scenario",
                attrs=attrs,
                source_record_id={
                    "repo": self.repo,
                    "path": rel_path,
                    "scenario_id": scenario_id,
                },
                source_revision=revision,
                op="I",
            ),
            EdgeFact(
                src=run_node_id,
                dst=scenario_node_id,
                edge_type="contains",
                attrs={},
                source_record_id={
                    "repo": self.repo,
                    "path": rel_path,
                    "scenario_id": scenario_id,
                    "edge": "run_contains_scenario",
                },
                source_revision=revision,
                op="I",
            ),
        ]

        for finding in findings:
            facts.append(NodeFact(
                node_id=finding["finding_id"],
                subtype="self_play_finding",
                attrs=finding["attrs"],
                source_record_id={
                    "repo": self.repo,
                    "path": rel_path,
                    "scenario_id": scenario_id,
                    "finding_id": finding["finding_id"],
                },
                source_revision=revision,
                op="I",
            ))
            facts.append(EdgeFact(
                src=scenario_node_id,
                dst=finding["finding_id"],
                edge_type="contains",
                attrs={"kind": finding["attrs"]["kind"]},
                source_record_id={
                    "repo": self.repo,
                    "path": rel_path,
                    "scenario_id": scenario_id,
                    "finding_id": finding["finding_id"],
                    "edge": "scenario_contains_finding",
                },
                source_revision=revision,
                op="I",
            ))
            for idx, evidence_id in enumerate(finding["evidence_node_ids"]):
                facts.append(EdgeFact(
                    src=finding["finding_id"],
                    dst=evidence_id,
                    edge_type="references",
                    attrs={
                        "evidence_rank": idx,
                        "evidence_subtype": (
                            _node_subtype_for_id(evidence_id)
                        ),
                    },
                    source_record_id={
                        "repo": self.repo,
                        "path": rel_path,
                        "scenario_id": scenario_id,
                        "finding_id": finding["finding_id"],
                        "evidence_node_id": evidence_id,
                    },
                    source_revision=revision,
                    op="I",
                ))
        return facts

    def _findings_for_scenario(
        self,
        scenario: dict[str, Any],
        *,
        run_key: str,
        run_node_id: str,
        rel_path: str,
        generation_id: Optional[int],
        superseded_generation_id: Optional[int],
    ) -> list[dict[str, Any]]:
        scenario_id = str(scenario.get("scenario_id") or "unknown")
        scenario_slug = _slug(scenario_id)
        stage = str(scenario.get("substrate_stage_target") or "unknown")
        evidence_ids = _known_evidence_ids(
            scenario.get("evidence_node_ids", []),
        )
        findings: list[dict[str, Any]] = []

        for edge_path in scenario.get("evidence_edge_paths", []) or []:
            if not isinstance(edge_path, dict):
                continue
            if (
                "observed_edge_count" not in edge_path
                and superseded_generation_id is None
            ):
                continue
            observed = _int_or_zero(edge_path.get("observed_edge_count"))
            if observed > 0:
                continue
            name = str(edge_path.get("name") or "expected-edge-path")
            path = edge_path.get("path") or []
            preferred = {
                str(edge_path.get("from_subtype") or ""),
                str(edge_path.get("to_subtype") or ""),
            }
            summary = (
                f"Expected self-play evidence path {name!r} "
                f"({path}) from {edge_path.get('from_subtype')} "
                f"to {edge_path.get('to_subtype')} had zero live edges."
            )
            findings.append(self._finding(
                run_key=run_key,
                run_node_id=run_node_id,
                scenario_slug=scenario_slug,
                scenario_id=scenario_id,
                kind="missing_edge_path",
                stage=stage,
                severity="high" if stage == "read_side_surfacing" else "medium",
                summary=summary,
                rel_path=rel_path,
                generation_id=generation_id,
                superseded_generation_id=superseded_generation_id,
                answerability_score=_answerability_score(scenario),
                expected_path=name,
                observed_count=observed,
                evidence_ids=self._select_evidence(
                    evidence_ids, preferred_subtypes=preferred,
                ),
            ))

        observed_counts = scenario.get("observed_subtype_counts") or {}
        for subtype in scenario.get("expected_evidence_subtypes", []) or []:
            subtype_s = str(subtype)
            if _int_or_zero(observed_counts.get(subtype_s)) > 0:
                continue
            summary = (
                f"Expected evidence subtype {subtype_s!r} was absent "
                f"from the pinned graph during this self-play scenario."
            )
            findings.append(self._finding(
                run_key=run_key,
                run_node_id=run_node_id,
                scenario_slug=scenario_slug,
                scenario_id=scenario_id,
                kind="missing_evidence_subtype",
                stage="source_coverage",
                severity="high",
                summary=summary,
                rel_path=rel_path,
                generation_id=generation_id,
                superseded_generation_id=superseded_generation_id,
                answerability_score=_answerability_score(scenario),
                expected_path=subtype_s,
                observed_count=0,
                evidence_ids=[],
            ))

        score = _answerability_score(scenario)
        if score is not None and score < 0.75:
            findings.append(self._finding(
                run_key=run_key,
                run_node_id=run_node_id,
                scenario_slug=scenario_slug,
                scenario_id=scenario_id,
                kind="low_answerability",
                stage=stage,
                severity="medium",
                summary=(
                    f"Self-play answerability score {score:.2f} was "
                    "below the 0.75 acceptance threshold."
                ),
                rel_path=rel_path,
                generation_id=generation_id,
                superseded_generation_id=superseded_generation_id,
                answerability_score=score,
                expected_path="answerability>=0.75",
                observed_count=None,
                evidence_ids=self._select_evidence(evidence_ids),
            ))

        for idx, fallback in enumerate(
            scenario.get("shell_fallbacks", []) or [],
        ):
            summary = (
                fallback.get("reason") if isinstance(fallback, dict)
                else str(fallback)
            )
            findings.append(self._finding(
                run_key=run_key,
                run_node_id=run_node_id,
                scenario_slug=scenario_slug,
                scenario_id=scenario_id,
                kind=f"shell_fallback:{idx}",
                stage="read_side_surfacing",
                severity="medium",
                summary=summary or "Scenario required shell fallback.",
                rel_path=rel_path,
                generation_id=generation_id,
                superseded_generation_id=superseded_generation_id,
                answerability_score=score,
                expected_path="native_mcp_answer",
                observed_count=None,
                evidence_ids=self._select_evidence(evidence_ids),
            ))

        for idx, assumption in enumerate(
            scenario.get("failed_assumptions", []) or [],
        ):
            findings.append(self._finding(
                run_key=run_key,
                run_node_id=run_node_id,
                scenario_slug=scenario_slug,
                scenario_id=scenario_id,
                kind=f"failed_assumption:{idx}",
                stage=stage,
                severity="high",
                summary=str(assumption),
                rel_path=rel_path,
                generation_id=generation_id,
                superseded_generation_id=superseded_generation_id,
                answerability_score=score,
                expected_path="agent_assumption",
                observed_count=None,
                evidence_ids=self._select_evidence(evidence_ids),
            ))

        for idx, call in enumerate(scenario.get("tool_calls", []) or []):
            if not isinstance(call, dict):
                continue
            # #2573 slice 10: a tool call with no boolean `ok` has an unknown
            # outcome. It is reported as such, not read as a success.
            verdict = read_ok(call, source="self_play_tool_call")
            if isinstance(verdict, Unknown):
                kind = f"tool_outcome_unknown:{idx}"
                summary = (
                    f"MCP tool {call.get('tool') or '<unknown>'} outcome "
                    f"unknown: {verdict.reason}"
                )
            elif verdict:
                continue
            else:
                kind = f"tool_failure:{idx}"
                summary = (
                    f"MCP tool {call.get('tool') or '<unknown>'} failed: "
                    f"{call.get('error') or call.get('summary') or ''}"
                ).strip()
            findings.append(self._finding(
                run_key=run_key,
                run_node_id=run_node_id,
                scenario_slug=scenario_slug,
                scenario_id=scenario_id,
                kind=kind,
                stage="read_side_surfacing",
                severity="high",
                summary=summary,
                rel_path=rel_path,
                generation_id=generation_id,
                superseded_generation_id=superseded_generation_id,
                answerability_score=score,
                expected_path=str(call.get("tool") or "tool_call"),
                observed_count=None,
                evidence_ids=self._select_evidence(evidence_ids),
            ))

        for idx, observation in enumerate(
            scenario.get("manual_observations", []) or [],
        ):
            finding = self._finding_from_manual_observation(
                observation,
                idx=idx,
                run_key=run_key,
                run_node_id=run_node_id,
                scenario_slug=scenario_slug,
                scenario_id=scenario_id,
                default_stage=stage,
                rel_path=rel_path,
                generation_id=generation_id,
                superseded_generation_id=superseded_generation_id,
                answerability_score=score,
                fallback_evidence_ids=evidence_ids,
            )
            if finding:
                findings.append(finding)

        return findings

    def _finding(
        self,
        *,
        run_key: str,
        run_node_id: str,
        scenario_slug: str,
        scenario_id: str,
        kind: str,
        stage: str,
        severity: str,
        summary: str,
        rel_path: str,
        generation_id: Optional[int],
        superseded_generation_id: Optional[int] = None,
        answerability_score: Optional[float],
        expected_path: Optional[str],
        observed_count: Optional[int],
        evidence_ids: list[str],
        status: str = "open",
        introduced_generation_id: Optional[int] = None,
        resolved_generation_id: Optional[int] = None,
        finding_id_override: Optional[str] = None,
    ) -> dict[str, Any]:
        finding_id = finding_id_override or (
            f"self-play-finding:{run_key}:{scenario_slug}:"
            f"{_sha(kind + ':' + summary, 20)}"
        )
        if status == "open" and superseded_generation_id is not None:
            status = "superseded"
        effective_superseded_generation_id = (
            superseded_generation_id if status == "superseded" else None
        )
        attrs: dict[str, Any] = {
            "finding_id": finding_id,
            "run_id": run_node_id,
            "scenario_result_id": (
                f"self-play-scenario:{run_key}:{scenario_slug}"
            ),
            "scenario_id": scenario_id,
            "kind": kind,
            "stage": stage,
            "severity": severity,
            "status": status,
            "evidence_summary": summary,
            "artifact_path": rel_path,
            "introduced_generation_id": (
                introduced_generation_id
                if introduced_generation_id is not None
                else generation_id
            ),
            "resolved_generation_id": resolved_generation_id,
            "superseded_generation_id": effective_superseded_generation_id,
            "expected_path": expected_path,
            "observed_count": observed_count,
            "answerability_score": answerability_score,
            "text": (
                f"{severity} {kind} in {scenario_id}: {summary}"
            ),
        }
        return {
            "finding_id": finding_id,
            "attrs": {k: v for k, v in attrs.items() if v is not None},
            "evidence_node_ids": evidence_ids[
                : self.max_evidence_edges_per_finding
            ],
        }

    def _finding_from_manual_observation(
        self,
        observation: Any,
        *,
        idx: int,
        run_key: str,
        run_node_id: str,
        scenario_slug: str,
        scenario_id: str,
        default_stage: str,
        rel_path: str,
        generation_id: Optional[int],
        superseded_generation_id: Optional[int],
        answerability_score: Optional[float],
        fallback_evidence_ids: list[str],
    ) -> Optional[dict[str, Any]]:
        if isinstance(observation, dict):
            summary = str(
                observation.get("evidence_summary")
                or observation.get("summary")
                or observation.get("text")
                or "",
            ).strip()
            if not summary:
                return None
            evidence_ids = _known_evidence_ids(
                observation.get("evidence_node_ids")
                or observation.get("node_ids")
                or [],
            )
            stable_id = _stable_finding_id(
                observation.get("finding_id")
                or observation.get("stable_id")
                or observation.get("id"),
                run_key=run_key,
                scenario_slug=scenario_slug,
            )
            return self._finding(
                run_key=run_key,
                run_node_id=run_node_id,
                scenario_slug=scenario_slug,
                scenario_id=scenario_id,
                kind=str(observation.get("kind") or f"manual:{idx}"),
                stage=str(observation.get("stage") or default_stage),
                severity=str(observation.get("severity") or "medium"),
                summary=summary,
                rel_path=rel_path,
                generation_id=generation_id,
                superseded_generation_id=superseded_generation_id,
                answerability_score=answerability_score,
                expected_path=observation.get("expected_path"),
                observed_count=_int_or_none(
                    observation.get("observed_count"),
                ),
                evidence_ids=evidence_ids or self._select_evidence(
                    fallback_evidence_ids,
                ),
                status=str(observation.get("status") or "open"),
                introduced_generation_id=_int_or_none(
                    observation.get("introduced_generation_id"),
                ),
                resolved_generation_id=_int_or_none(
                    observation.get("resolved_generation_id"),
                ),
                finding_id_override=stable_id,
            )
        summary = str(observation).strip()
        if not summary:
            return None
        return self._finding(
            run_key=run_key,
            run_node_id=run_node_id,
            scenario_slug=scenario_slug,
            scenario_id=scenario_id,
            kind=f"manual:{idx}",
            stage=default_stage,
            severity="medium",
            summary=summary,
            rel_path=rel_path,
            generation_id=generation_id,
            superseded_generation_id=superseded_generation_id,
            answerability_score=answerability_score,
            expected_path=None,
            observed_count=None,
            evidence_ids=self._select_evidence(fallback_evidence_ids),
        )

    def _select_evidence(
        self,
        evidence_ids: list[str],
        *,
        preferred_subtypes: Optional[set[str]] = None,
    ) -> list[str]:
        if not preferred_subtypes:
            return evidence_ids[: self.max_evidence_edges_per_finding]
        preferred = {
            subtype for subtype in preferred_subtypes
            if subtype and subtype != "None"
        }
        selected = [
            node_id for node_id in evidence_ids
            if _node_subtype_for_id(node_id) in preferred
        ]
        if selected:
            return selected[: self.max_evidence_edges_per_finding]
        return evidence_ids[: self.max_evidence_edges_per_finding]


def _int_or_none(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: Any) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None else 0


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _answerability_score(scenario: dict[str, Any]) -> Optional[float]:
    answerability = scenario.get("answerability") or {}
    if not isinstance(answerability, dict):
        return None
    return _float_or_none(answerability.get("score"))


def _tool_failure_count(scenario: dict[str, Any]) -> int:
    return sum(
        1 for call in scenario.get("tool_calls", [])
        if isinstance(call, dict) and call.get("ok") is False
    )
