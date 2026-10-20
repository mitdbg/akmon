"""Semgrep + CodeQL SARIF-based data-flow sources.

Extracted from ``library/sources/code_graph.py`` by Wave D-28
(2026-05) per ``extract-code-graph-sarif``. Two source
classes:

  - ``SemgrepDataFlowSource`` — projects ``semgrep --json``
    output as data-flow observation + step facts.
  - ``CodeQLDataFlowSource`` — projects CodeQL SARIF output
    (or the legacy CSV format) the same way.

The 22 helper functions in this module are 100% specific to
SARIF projection (observation-attr building, step-attr
building, SARIF location/message normalization, source-path
flattening). They were inlined alongside the two source
classes in the original 2,611-LOC code_graph.py; they form
a coherent ~515-LOC cluster that has no callers outside
these two classes.

``library/sources/code_graph.py`` re-exports the two source
classes at module-import time so YAML manifests with
`module: okg.substrate.library.sources.code_graph;
class: SemgrepDataFlowSource` (or `CodeQLDataFlowSource`)
keep resolving.
"""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

from okg.substrate.library.sources.base import (
    EdgeKey,
    EdgeFact,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)

from ._code_graph_common import (
    CODE_LANGUAGE_BY_EXT,
    _read_text,
    _rel,
    _sha,
)


__all__ = [
    "SemgrepDataFlowSource",
    "CodeQLDataFlowSource",
]




def _artifact_change_probe(
    *,
    repo: str,
    repo_root: Path | str,
    artifact_paths: Sequence[Path | str],
    artifact_globs: Sequence[str],
    max_artifacts: int,
):
    """ContentHashProbe over exactly this source's artifact enumeration.

    Reuses ``_configured_artifacts`` (the same set the reader parses) as the
    probe scope, so read_scope == probe_scope. These artifacts are CI-generated
    and may not be git-tracked, so the content-hash floor — not a git probe — is
    the sound choice; it is content-based (mtime-immune) and detects a removed
    artifact as a dropped membership entry.
    """
    import sys

    from okg.substrate.library.sources.content_hash_probe import ContentHashProbe

    root = Path(repo_root)

    def _scope():
        return [
            (_rel_or_name(root, path), path)
            for path in _configured_artifacts(
                root, artifact_paths, artifact_globs, max_artifacts,
            )
        ]

    return ContentHashProbe(
        content_items=_scope,
        config={
            "repo": repo,
            "artifact_paths": [str(p) for p in artifact_paths],
            "artifact_globs": sorted(artifact_globs),
            "max_artifacts": max_artifacts,
        },
        emit_targets=[sys.modules[__name__]],
    )


@dataclass
class SemgrepDataFlowSource:
    repo: str
    repo_root: Path | str
    artifact_paths: Sequence[Path | str] = ()
    artifact_globs: Sequence[str] = ()
    max_artifacts: int = 100

    name: str = "semgrep_data_flow"
    profile: str = "discovery_crawl"
    # Incremental-source contract (issue #323): content-hash floor over the
    # CI-generated artifacts (not git-tracked) — detects edit/add/delete.
    change_probe_kind = "content_hash"
    # The current reader reparses artifact paths after the probe instead of
    # consuming one captured byte snapshot.  Keep the probe useful for change
    # signalling, but do not let it acknowledge a runner short-circuit.
    probe_short_circuit_safe = False

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self.change_probe = _artifact_change_probe(
            repo=self.repo,
            repo_root=self.repo_root,
            artifact_paths=self.artifact_paths,
            artifact_globs=self.artifact_globs,
            max_artifacts=self.max_artifacts,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        facts: list[NodeFact | EdgeFact] = []
        for path in _configured_artifacts(
            Path(self.repo_root),
            self.artifact_paths,
            self.artifact_globs,
            self.max_artifacts,
        ):
            facts.extend(self._facts_for_artifact(path))
        completed_scope = mode in {"reconcile", "scope_complete"}
        return SourceRun(
            facts=iter(facts),
            completed_scope=completed_scope,
            next_cursor={"run_id": run_id},
            run_mode=mode,
            record_set=(
                _record_set_from_facts(facts)
                if completed_scope else None
            ),
        )

    def _facts_for_artifact(self, path: Path) -> list[NodeFact | EdgeFact]:
        artifact_id = _rel_or_name(Path(self.repo_root), path)
        text = _read_text(path)
        revision = {
            "artifact": artifact_id,
            "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        }
        try:
            doc = json.loads(text)
        except json.JSONDecodeError:
            return []
        tool_version = str(doc.get("version") or doc.get("semgrep_version") or "")
        out: list[NodeFact | EdgeFact] = []
        for result in doc.get("results", []) or []:
            out.extend(_semgrep_observation_facts(
                self.repo,
                artifact_id,
                result,
                tool_version,
                revision,
            ))
        return out


@dataclass
class CodeQLDataFlowSource:
    repo: str
    repo_root: Path | str
    artifact_paths: Sequence[Path | str] = ()
    artifact_globs: Sequence[str] = ()
    max_artifacts: int = 100

    name: str = "codeql_data_flow"
    profile: str = "discovery_crawl"
    # Incremental-source contract (issue #323): content-hash floor over the
    # CI-generated artifacts (not git-tracked) — detects edit/add/delete.
    change_probe_kind = "content_hash"
    probe_short_circuit_safe = False

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self.change_probe = _artifact_change_probe(
            repo=self.repo,
            repo_root=self.repo_root,
            artifact_paths=self.artifact_paths,
            artifact_globs=self.artifact_globs,
            max_artifacts=self.max_artifacts,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        facts: list[NodeFact | EdgeFact] = []
        for path in _configured_artifacts(
            Path(self.repo_root),
            self.artifact_paths,
            self.artifact_globs,
            self.max_artifacts,
        ):
            facts.extend(self._facts_for_artifact(path))
        completed_scope = mode in {"reconcile", "scope_complete"}
        return SourceRun(
            facts=iter(facts),
            completed_scope=completed_scope,
            next_cursor={"run_id": run_id},
            run_mode=mode,
            record_set=(
                _record_set_from_facts(facts)
                if completed_scope else None
            ),
        )

    def _facts_for_artifact(self, path: Path) -> list[NodeFact | EdgeFact]:
        artifact_id = _rel_or_name(Path(self.repo_root), path)
        text = _read_text(path)
        revision = {
            "artifact": artifact_id,
            "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        }
        if path.suffix.lower() == ".csv":
            return _codeql_csv_facts(self.repo, artifact_id, text, revision)
        try:
            doc = json.loads(text)
        except json.JSONDecodeError:
            return []
        return _codeql_sarif_facts(self.repo, artifact_id, doc, revision)


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
            f"data-flow evidence source exceeded max_artifacts={max_artifacts}; "
            "tighten artifact_globs or raise the cap"
        )
    return deduped


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


def _rel_or_name(repo_root: Path, path: Path) -> str:
    try:
        return _rel(repo_root, path)
    except ValueError:
        return path.name


def _semgrep_observation_facts(
    repo: str,
    artifact_id: str,
    result: dict[str, Any],
    tool_version: str,
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    rel_path = str(result.get("path") or "")
    start = _line_from_mapping(result.get("start"))
    end = _line_from_mapping(result.get("end")) or start
    if not rel_path or not start:
        return []
    extra = result.get("extra") or {}
    metadata = extra.get("metadata") or {}
    rule_id = str(result.get("check_id") or result.get("rule_id") or "")
    message = str(extra.get("message") or result.get("message") or "")
    steps = _semgrep_steps(result, rel_path)
    attrs = _observation_attrs(
        repo=repo,
        tool="semgrep",
        tool_version=tool_version,
        rule_id=rule_id,
        rule_version=str(metadata.get("rule_version") or metadata.get("version") or ""),
        rel_path=rel_path,
        start_line=start,
        end_line=end,
        language=str(metadata.get("language") or _language_for_rel_path(rel_path)),
        flow_kind="taint" if steps else str(metadata.get("flow_kind") or "finding"),
        confidence=str(metadata.get("confidence") or extra.get("confidence") or "medium"),
        message=message,
        access_kind=str(metadata.get("access_kind") or ""),
        source_span=_span_from_mapping(rel_path, result.get("start")),
        sink_span=_span_from_mapping(rel_path, result.get("end")),
        path_steps=steps,
        artifact_id=artifact_id,
    )
    return _observation_with_steps(repo, attrs, steps, revision)


def _semgrep_steps(result: dict[str, Any], rel_path: str) -> list[dict[str, Any]]:
    trace = (result.get("extra") or {}).get("dataflow_trace") or {}
    out: list[dict[str, Any]] = []
    for kind in ("taint_source", "intermediate_vars", "taint_sink"):
        for loc in _flatten_locations(trace.get(kind), rel_path):
            loc["kind"] = kind
            out.append(loc)
    return out


def _codeql_sarif_facts(
    repo: str,
    artifact_id: str,
    doc: dict[str, Any],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    try:
        from sarif_pydantic import Sarif

        doc = Sarif.model_validate(doc).model_dump(by_alias=True, exclude_none=True)
    except Exception:  # noqa: BLE001 — sarif_pydantic is optional; fall through to the raw-dict parse path below.
        if "runs" not in doc:
            return []
    out: list[NodeFact | EdgeFact] = []
    for run in doc.get("runs", []) or []:
        driver = ((run.get("tool") or {}).get("driver") or {})
        tool_version = str(driver.get("semanticVersion") or driver.get("version") or "")
        rule_versions = {
            str(rule.get("id") or ""): str(rule.get("version") or "")
            for rule in driver.get("rules", []) or []
        }
        for result in run.get("results", []) or []:
            out.extend(_codeql_result_facts(
                repo,
                artifact_id,
                result,
                tool_version,
                rule_versions,
                revision,
            ))
    return out


def _codeql_result_facts(
    repo: str,
    artifact_id: str,
    result: dict[str, Any],
    tool_version: str,
    rule_versions: dict[str, str],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    location = _first_sarif_location(result.get("locations"))
    rel_path = str(location.get("path") or "")
    start = int(location.get("start_line") or 0)
    end = int(location.get("end_line") or start)
    if not rel_path or not start:
        return []
    rule_id = str(result.get("ruleId") or (result.get("rule") or {}).get("id") or "")
    message = _sarif_message(result.get("message"))
    steps = _codeql_steps(result)
    attrs = _observation_attrs(
        repo=repo,
        tool="codeql",
        tool_version=tool_version,
        rule_id=rule_id,
        rule_version=rule_versions.get(rule_id, ""),
        rel_path=rel_path,
        start_line=start,
        end_line=end,
        language=_language_for_rel_path(rel_path),
        flow_kind="path_problem" if steps else str(result.get("kind") or "problem"),
        confidence=str(result.get("level") or "medium"),
        message=message,
        access_kind="",
        source_span=location,
        sink_span=steps[-1] if steps else location,
        path_steps=steps,
        artifact_id=artifact_id,
    )
    return _observation_with_steps(repo, attrs, steps, revision)


def _codeql_steps(result: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for code_flow in result.get("codeFlows", []) or []:
        for thread_flow in code_flow.get("threadFlows", []) or []:
            for item in thread_flow.get("locations", []) or []:
                loc = _sarif_location(item.get("location") or {})
                if loc:
                    loc["kind"] = "thread_flow"
                    out.append(loc)
    return out


def _codeql_csv_facts(
    repo: str,
    artifact_id: str,
    text: str,
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    out: list[NodeFact | EdgeFact] = []
    for row in csv.DictReader(text.splitlines()):
        rel_path = str(row.get("Path") or row.get("path") or row.get("File") or "")
        start = _int_or_zero(row.get("Start line") or row.get("startLine") or row.get("Line"))
        end = _int_or_zero(row.get("End line") or row.get("endLine")) or start
        if not rel_path or not start:
            continue
        rule_id = str(row.get("Rule") or row.get("Name") or row.get("ruleId") or "")
        attrs = _observation_attrs(
            repo=repo,
            tool="codeql",
            tool_version="",
            rule_id=rule_id,
            rule_version="",
            rel_path=rel_path,
            start_line=start,
            end_line=end,
            language=_language_for_rel_path(rel_path),
            flow_kind=str(row.get("Kind") or "csv_finding"),
            confidence=str(row.get("Severity") or row.get("level") or "medium"),
            message=str(row.get("Message") or row.get("Description") or ""),
            access_kind="",
            source_span={"path": rel_path, "start_line": start, "end_line": end},
            sink_span={"path": rel_path, "start_line": start, "end_line": end},
            path_steps=[],
            artifact_id=artifact_id,
        )
        out.extend(_observation_with_steps(repo, attrs, [], revision))
    return out


def _observation_attrs(
    *,
    repo: str,
    tool: str,
    tool_version: str,
    rule_id: str,
    rule_version: str,
    rel_path: str,
    start_line: int,
    end_line: int,
    language: str,
    flow_kind: str,
    confidence: str,
    message: str,
    access_kind: str,
    source_span: dict[str, Any],
    sink_span: dict[str, Any],
    path_steps: list[dict[str, Any]],
    artifact_id: str,
) -> dict[str, Any]:
    observation_id = f"flow:{tool}:{_sha(json.dumps([repo, artifact_id, rule_id, rel_path, start_line, end_line, message], sort_keys=True))}"
    return {
        "observation_id": observation_id,
        "repo": repo,
        "tool": tool,
        "rule_id": rule_id,
        "language": language,
        "path": rel_path,
        "flow_kind": flow_kind,
        "start_line": start_line,
        "end_line": end_line,
        "confidence": confidence,
        "tool_version": tool_version,
        "rule_version": rule_version,
        "message": message,
        "source_span": source_span,
        "sink_span": sink_span,
        "path_steps": path_steps,
        "access_kind": access_kind,
        "artifact": artifact_id,
        "text": f"{tool} {rule_id} {rel_path}:{start_line} {message}".strip(),
    }


def _observation_with_steps(
    repo: str,
    attrs: dict[str, Any],
    steps: list[dict[str, Any]],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    observation_id = attrs["observation_id"]
    out: list[NodeFact | EdgeFact] = [
        NodeFact(
            observation_id,
            "data_flow_observation",
            attrs,
            {"repo": repo, "observation_id": observation_id},
            revision,
        )
    ]
    for index, step in enumerate(steps):
        step_attrs = _step_attrs(observation_id, index, step)
        step_id = step_attrs["step_id"]
        out.append(NodeFact(
            step_id,
            "data_flow_step",
            step_attrs,
            {"repo": repo, "observation_id": observation_id, "step_id": step_id},
            revision,
        ))
        out.append(EdgeFact(
            src=observation_id,
            dst=step_id,
            edge_type="contains",
            attrs={"index": index},
            source_record_id={
                "repo": repo,
                "observation_id": observation_id,
                "step_id": step_id,
            },
            source_revision=revision,
        ))
    return out


def _step_attrs(observation_id: str, index: int, step: dict[str, Any]) -> dict[str, Any]:
    rel_path = str(step.get("path") or "")
    start_line = int(step.get("start_line") or 1)
    end_line = int(step.get("end_line") or start_line)
    kind = str(step.get("kind") or "step")
    message = str(step.get("message") or "")
    step_id = f"flowstep:{_sha(json.dumps([observation_id, index, kind, rel_path, start_line, end_line, message], sort_keys=True))}"
    return {
        "step_id": step_id,
        "observation_id": observation_id,
        "index": index,
        "kind": kind,
        "path": rel_path,
        "start_line": start_line,
        "end_line": end_line,
        "symbol_hint": str(step.get("symbol_hint") or ""),
        "asset_hint": str(step.get("asset_hint") or ""),
        "message": message,
        "text": f"{kind} {rel_path}:{start_line} {message}".strip(),
    }


def _language_for_rel_path(rel_path: str) -> str:
    return CODE_LANGUAGE_BY_EXT.get(Path(rel_path).suffix.lower(), "")


def _line_from_mapping(value: Any) -> int:
    if not isinstance(value, dict):
        return 0
    return _int_or_zero(value.get("line") or value.get("startLine"))


def _span_from_mapping(rel_path: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"path": rel_path}
    return {
        "path": rel_path,
        "line": _int_or_zero(value.get("line") or value.get("startLine")),
        "col": _int_or_zero(value.get("col") or value.get("startColumn")),
    }


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _flatten_locations(value: Any, default_path: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(value, list):
        for item in value:
            out.extend(_flatten_locations(item, default_path))
        return out
    if not isinstance(value, dict):
        return out
    loc = value.get("location") if isinstance(value.get("location"), dict) else value
    path = str(loc.get("path") or value.get("path") or default_path)
    start = _line_from_mapping(loc.get("start") or loc.get("region") or loc)
    end = _line_from_mapping(loc.get("end") or loc) or start
    if path and start:
        out.append({
            "path": path,
            "start_line": start,
            "end_line": end,
            "message": str(value.get("message") or value.get("content") or ""),
        })
        return out
    for nested in value.values():
        if isinstance(nested, (dict, list)) and nested is not loc:
            out.extend(_flatten_locations(nested, default_path))
    return _dedupe_steps(out)


def _dedupe_steps(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, int, int, str]] = set()
    out: list[dict[str, Any]] = []
    for step in steps:
        key = (
            str(step.get("path") or ""),
            int(step.get("start_line") or 0),
            int(step.get("end_line") or 0),
            str(step.get("message") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(step)
    return out


def _first_sarif_location(locations: Any) -> dict[str, Any]:
    if not isinstance(locations, list) or not locations:
        return {}
    return _sarif_location(locations[0])


def _sarif_location(location: dict[str, Any]) -> dict[str, Any]:
    physical = location.get("physicalLocation") or {}
    artifact = physical.get("artifactLocation") or {}
    region = physical.get("region") or {}
    rel_path = str(artifact.get("uri") or "")
    start = _int_or_zero(region.get("startLine"))
    if not rel_path or not start:
        return {}
    return {
        "path": rel_path,
        "start_line": start,
        "end_line": _int_or_zero(region.get("endLine")) or start,
        "start_col": _int_or_zero(region.get("startColumn")),
        "end_col": _int_or_zero(region.get("endColumn")),
        "message": _sarif_message(location.get("message")),
    }


def _sarif_message(message: Any) -> str:
    if not isinstance(message, dict):
        return ""
    return str(message.get("text") or message.get("markdown") or "")
