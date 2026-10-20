"""Agent-memory packet source.

Imports explicit ``okg-agent-memory/v1`` JSON/JSONL packets. The source is
deliberately conservative: it emits only structured records and never promotes
free-form transcript prose into supported claims.
"""
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
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
# Catalog-backed, deployment-extensible prefix registry (substrate-operability),
# shared with the `record` MCP tool. Replaces the source's old closed
# _KNOWN_PREFIX_SUBTYPE tuple.
from okg.substrate.library.agent_memory_prefixes import (
    RecordPrefix,
    normalize_prefixes,
    subtype_for_id,
)


__all__ = ["AgentMemorySource"]


_VALID_FORMAT = "okg-agent-memory/v1"

# The prefix -> subtype table moved to the shared, deployment-extensible
# registry (okg.substrate.library.agent_memory_prefixes). The source resolves
# through `self._prefixes` = normalize_prefixes(self.record_prefixes), so a
# deployment can add prefixes via its source config without editing this file.

_DETERMINISTIC_EVIDENCE_SUBTYPES = {
    "agent_observation",
    "verification_result",
    "test_report",
    "coverage_report",
    "runtime_event",
    "stack_frame",
    "test_run",
    "test_case",
    "security_finding",
    "secret_finding",
    "dependency_vulnerability",
    "profile_run",
    "profile_stage",
    "profile_query",
    "profile_hotspot",
    "benchmark_measurement",
    "performance_budget",
    "data_flow_observation",
    "data_flow_step",
    "source_file",
    "code_symbol",
    "acceptance_criterion",
    "acceptance_scenario",
}

_COMMON_REQUIRED = (
    "status",
    "confidence_kind",
    "observed_at",
    "extraction_method",
)


def _sha(text: str, n: int | None = None) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return digest if n is None else digest[:n]


def _slug(value: Any, *, fallback: str = "unknown") -> str:
    raw = str(value or fallback).strip().lower()
    out = []
    for ch in raw:
        if ch.isalnum():
            out.append(ch)
        elif ch in {"-", "_", ".", ":", "/"}:
            out.append("-")
    slug = "".join(out).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or fallback


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _node_subtype_for_id(
    node_id: str,
    prefixes: tuple[RecordPrefix, ...],
) -> str | None:
    return subtype_for_id(node_id, prefixes)


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _clean_attrs(attrs: dict[str, Any]) -> dict[str, Any]:
    return {
        k: v for k, v in attrs.items()
        if v is not None and v != ""
    }


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


def _record_set(facts: list[NodeFact | EdgeFact]) -> RecordSet:
    records: dict[str, RecordEmission] = {}
    for fact in facts:
        record_key = _record_key(fact)
        if record_key is None:
            continue
        emission = records.setdefault(record_key, RecordEmission())
        if isinstance(fact, NodeFact):
            emission.descendants.append(
                RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=dict(fact.source_record_id),
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
                    source_record_id=dict(fact.source_record_id),
                    source_revision=dict(fact.source_revision),
                )
            )
    return RecordSet(records=records)


def _record_key(fact: NodeFact | EdgeFact) -> str | None:
    path = fact.source_revision.get("path")
    if isinstance(path, str) and path:
        return path
    path = fact.source_record_id.get("path")
    if isinstance(path, str) and path:
        return path
    return None


@dataclass(frozen=True)
class _PacketRecord:
    packet: dict[str, Any]
    artifact_hash: str
    record_hash: str
    rel_path: str
    record_index: int


@dataclass
class AgentMemorySource:
    repo: str
    repo_root: Path | str
    packet_paths: Sequence[Path | str] = ()
    packet_globs: Sequence[str] = ()
    packet_dirs: Sequence[Path | str] = ()
    max_artifacts: int = 100
    # Deployment-supplied prefix extensions (a sequence of dicts or
    # RecordPrefix), merged with the builtin registry so a deployment can add
    # its own node-id prefixes without editing substrate code. Populated from
    # the deployment's source config; empty default => builtin prefixes only.
    record_prefixes: Sequence[Any] = ()

    name: str = "agent_memory"
    profile: str = "discovery_crawl"
    validation_warnings: list[str] = field(default_factory=list, init=False)
    _prefixes: tuple[RecordPrefix, ...] = field(
        default_factory=tuple, init=False,
    )

    # Change-probe contract (#323): non-git agent-memory packets fall to the
    # O(content) floor. The probe re-uses the source's OWN full-scope
    # enumeration (_packet_files()), so the probe scope equals the read scope
    # and a deleted packet drops from the next enumeration.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self._prefixes = normalize_prefixes(self.record_prefixes)
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Path]]:
            return [(str(path), path) for path in self._packet_files()]

        return ContentHashProbe(
            content_items=_items,
            config={
                "repo": self.repo,
                "packet_paths": [str(p) for p in self.packet_paths],
                "packet_globs": [str(g) for g in self.packet_globs],
                "packet_dirs": [str(d) for d in self.packet_dirs],
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
        self.validation_warnings.clear()
        scoped = path_scope_requested(sync_scope)
        facts: list[NodeFact | EdgeFact] = []
        for path in self._packet_files(sync_scope if scoped else None):
            facts.extend(self._facts_for_file(path, run_id=run_id))
        deduped = _dedupe_facts(facts)
        completed_scope = not scoped and mode in {"reconcile", "scope_complete"}
        return SourceRun(
            facts=iter(deduped),
            completed_scope=completed_scope,
            next_cursor={"run_id": run_id},
            run_mode=mode,
            record_set=_record_set(deduped) if completed_scope else None,
        )

    def _packet_files(
        self,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> list[Path]:
        if sync_scope is not None:
            return self._scoped_packet_files(sync_scope)
        paths: list[Path] = []
        for configured in self.packet_paths:
            path = self._resolve_path(configured)
            if path.is_file() and path.suffix.lower() in {".json", ".jsonl"}:
                paths.append(path)
        for configured_dir in self.packet_dirs:
            directory = self._resolve_path(configured_dir)
            if directory.is_dir():
                paths.extend(directory.glob("*.json"))
                paths.extend(directory.glob("*.jsonl"))
        for pattern in self.packet_globs:
            paths.extend(self.repo_root.glob(str(pattern)))
        deduped = sorted({p.resolve() for p in paths if p.is_file()})
        if len(deduped) > self.max_artifacts:
            raise ValueError(
                "agent-memory source exceeded "
                f"max_artifacts={self.max_artifacts}; tighten "
                "packet_globs/packet_dirs or raise the cap"
            )
        return deduped

    def _scoped_packet_files(self, sync_scope: dict[str, Any]) -> list[Path]:
        scoped = resolve_scoped_paths(
            sync_scope,
            roots=self._scope_roots(),
            suffixes=[".json", ".jsonl"],
        )
        return sorted({
            path for path in scoped
            if self._is_configured_packet(path)
        })

    def _scope_roots(self) -> tuple[Path, ...]:
        roots = [Path(self.repo_root)]
        for raw in self.packet_paths:
            roots.append(self._resolve_path(raw).resolve().parent)
        for raw in self.packet_dirs:
            roots.append(self._resolve_path(raw).resolve())
        return tuple(roots)

    def _is_configured_packet(self, path: Path) -> bool:
        resolved = path.expanduser().resolve()
        if (
            not resolved.is_file()
            or resolved.suffix.lower() not in {".json", ".jsonl"}
        ):
            return False
        for raw in self.packet_paths:
            if resolved == self._resolve_path(raw).resolve():
                return True
        for raw in self.packet_dirs:
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
            for pattern in self.packet_globs
        )

    def _resolve_path(self, value: Path | str) -> Path:
        path = Path(value).expanduser()
        if path.is_absolute():
            return path
        return (self.repo_root / path).resolve()

    def _relpath(self, path: Path) -> str:
        try:
            return str(path.resolve().relative_to(self.repo_root)).replace("\\", "/")
        except ValueError:
            return str(path.resolve())

    def _facts_for_file(self, path: Path, *, run_id: str) -> list[NodeFact | EdgeFact]:
        raw = _read_text(path)
        artifact_hash = _sha(raw)
        records = self._packet_records(
            path,
            raw=raw,
            artifact_hash=artifact_hash,
        )
        facts: list[NodeFact | EdgeFact] = []
        for record in records:
            facts.extend(self._facts_for_packet(record, run_id=run_id))
        return facts

    def _packet_records(
        self,
        path: Path,
        *,
        raw: str,
        artifact_hash: str,
    ) -> list[_PacketRecord]:
        rel_path = self._relpath(path)
        if path.suffix.lower() == ".jsonl":
            out: list[_PacketRecord] = []
            for idx, line in enumerate(raw.splitlines()):
                if not line.strip():
                    continue
                try:
                    packet = json.loads(line)
                except json.JSONDecodeError as exc:
                    self.validation_warnings.append(
                        f"{rel_path}:{idx + 1}: invalid JSONL record: {exc}",
                    )
                    continue
                if not isinstance(packet, dict):
                    self.validation_warnings.append(
                        f"{rel_path}:{idx + 1}: record root is not an object",
                    )
                    continue
                out.append(_PacketRecord(
                    packet=packet,
                    artifact_hash=artifact_hash,
                    record_hash=_sha(line),
                    rel_path=rel_path,
                    record_index=idx,
                ))
            return out

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            self.validation_warnings.append(
                f"{rel_path}: invalid JSON memory packet: {exc}",
            )
            return []
        packets = parsed if isinstance(parsed, list) else [parsed]
        out = []
        for idx, packet in enumerate(packets):
            if not isinstance(packet, dict):
                self.validation_warnings.append(
                    f"{rel_path}:{idx}: packet root is not an object",
                )
                continue
            record_text = json.dumps(packet, sort_keys=True, separators=(",", ":"))
            out.append(_PacketRecord(
                packet=packet,
                artifact_hash=artifact_hash,
                record_hash=_sha(record_text),
                rel_path=rel_path,
                record_index=idx,
            ))
        return out

    def _facts_for_packet(
        self,
        record: _PacketRecord,
        *,
        run_id: str,
    ) -> list[NodeFact | EdgeFact]:
        packet = record.packet
        if packet.get("format") != _VALID_FORMAT:
            self.validation_warnings.append(
                f"{record.rel_path}:{record.record_index}: unsupported "
                f"agent-memory format {packet.get('format')!r}",
            )
            return []

        revision = {
            "path": record.rel_path,
            "artifact_hash": record.artifact_hash,
            "record_hash": record.record_hash,
            "record_index": record.record_index,
        }
        defaults = {
            "source_artifact": record.rel_path,
            "source_hash": record.record_hash,
            "observed_at": packet.get("observed_at"),
            "extraction_method": packet.get("extraction_method")
            or "agent_memory_packet",
            "confidence_kind": packet.get("confidence_kind") or "model_inferred",
            "scope": packet.get("scope") or self.repo,
            "status": packet.get("status") or "hypothesis",
            "session_id": packet.get("session_id"),
        }

        facts: list[NodeFact | EdgeFact] = []
        for idx, item in enumerate(_as_list(packet.get("observations"))):
            if isinstance(item, dict):
                facts.extend(self._node_and_edges(
                    item,
                    defaults=defaults,
                    rel_path=record.rel_path,
                    revision=revision,
                    run_id=run_id,
                    record_kind="observation",
                    record_idx=idx,
                ))
        for idx, item in enumerate(_as_list(packet.get("verification_results"))):
            if isinstance(item, dict):
                facts.extend(self._node_and_edges(
                    item,
                    defaults=defaults,
                    rel_path=record.rel_path,
                    revision=revision,
                    run_id=run_id,
                    record_kind="verification",
                    record_idx=idx,
                ))
        for idx, item in enumerate(_as_list(packet.get("experiments"))):
            if isinstance(item, dict):
                facts.extend(self._node_and_edges(
                    item,
                    defaults=defaults,
                    rel_path=record.rel_path,
                    revision=revision,
                    run_id=run_id,
                    record_kind="experiment",
                    record_idx=idx,
                ))
        for idx, item in enumerate(_as_list(packet.get("claims"))):
            if isinstance(item, dict):
                facts.extend(self._node_and_edges(
                    item,
                    defaults=defaults,
                    rel_path=record.rel_path,
                    revision=revision,
                    run_id=run_id,
                    record_kind="claim",
                    record_idx=idx,
                ))
        for idx, item in enumerate(_as_list(packet.get("decisions"))):
            if isinstance(item, dict):
                facts.extend(self._node_and_edges(
                    item,
                    defaults=defaults,
                    rel_path=record.rel_path,
                    revision=revision,
                    run_id=run_id,
                    record_kind="decision",
                    record_idx=idx,
                ))
        return facts

    def _node_and_edges(
        self,
        item: dict[str, Any],
        *,
        defaults: dict[str, Any],
        rel_path: str,
        revision: dict[str, Any],
        run_id: str,
        record_kind: str,
        record_idx: int,
    ) -> list[NodeFact | EdgeFact]:
        node_id, subtype, id_attr, text_attr = _record_identity(
            item,
            record_kind=record_kind,
            rel_path=rel_path,
            record_idx=record_idx,
        )
        warnings = _quality_warnings(
            item,
            defaults=defaults,
            record_kind=record_kind,
            prefixes=self._prefixes,
        )
        attrs = _attrs_for_record(
            item,
            defaults=defaults,
            id_attr=id_attr,
            node_id=node_id,
            text_attr=text_attr,
            warnings=warnings,
        )
        for warning in warnings:
            self.validation_warnings.append(f"{rel_path}:{node_id}: {warning}")

        facts: list[NodeFact | EdgeFact] = [
            NodeFact(
                node_id=node_id,
                subtype=subtype,
                attrs=attrs,
                source_record_id={
                    "repo": self.repo,
                    "path": rel_path,
                    "record_kind": record_kind,
                    "node_id": node_id,
                },
                source_revision=revision,
                op="I",
            )
        ]
        facts.extend(_edges_for_record(
            node_id,
            item,
            record_kind=record_kind,
            repo=self.repo,
            revision=revision,
            run_id=run_id,
            prefixes=self._prefixes,
        ))
        return facts


def _record_identity(
    item: dict[str, Any],
    *,
    record_kind: str,
    rel_path: str,
    record_idx: int,
) -> tuple[str, str, str, str]:
    if record_kind == "claim":
        subtype, id_attr, text_attr = "agent_claim", "claim_id", "statement"
        prefix = "claim"
    elif record_kind == "observation":
        subtype, id_attr, text_attr = "agent_observation", "observation_id", "summary"
        prefix = "observation"
    elif record_kind == "experiment":
        subtype, id_attr, text_attr = "agent_experiment", "experiment_id", "summary"
        prefix = "experiment"
    elif record_kind == "verification":
        subtype, id_attr, text_attr = "verification_result", "verification_id", "summary"
        prefix = "verification"
    elif record_kind == "decision":
        subtype, id_attr, text_attr = "agent_decision", "decision_id", "summary"
        prefix = "decision"
    else:
        raise ValueError(f"unknown agent-memory record kind {record_kind!r}")

    raw_id = str(item.get(id_attr) or item.get("id") or "").strip()
    if raw_id:
        return raw_id, subtype, id_attr, text_attr
    stable = _sha(
        json.dumps(item, sort_keys=True, separators=(",", ":")),
        16,
    )
    return f"{prefix}:{_slug(rel_path)}:{record_idx}:{stable}", subtype, id_attr, text_attr


def _attrs_for_record(
    item: dict[str, Any],
    *,
    defaults: dict[str, Any],
    id_attr: str,
    node_id: str,
    text_attr: str,
    warnings: list[str],
) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    attrs[id_attr] = node_id
    for key, value in defaults.items():
        attrs[key] = item.get(key, value)

    for key, value in item.items():
        if key.endswith("_ids") or key in {
            "evidence_node_ids",
            "contradicting_node_ids",
            "target_node_ids",
            "produced_node_ids",
            "reference_node_ids",
            "dependency_node_ids",
            "supports",
            "contradicts",
            "verifies",
            "produces",
            "references",
        }:
            continue
        attrs[key] = value

    if node_id != attrs.get(id_attr):
        attrs[id_attr] = node_id
    if text_attr in item:
        attrs["text"] = str(item.get(text_attr) or "")[:1000]
    elif "statement" in item:
        attrs["text"] = str(item.get("statement") or "")[:1000]
    elif "summary" in item:
        attrs["text"] = str(item.get("summary") or "")[:1000]
    if warnings:
        attrs["validation_warnings"] = warnings
    return _clean_attrs(attrs)


def _quality_warnings(
    item: dict[str, Any],
    *,
    defaults: dict[str, Any],
    record_kind: str,
    prefixes: tuple[RecordPrefix, ...],
) -> list[str]:
    warnings: list[str] = []

    def known(value: Any) -> list[str]:
        return _known_node_ids(value, prefixes)

    if record_kind == "claim":
        for key in ("statement", "claim_kind", "scope", "subject_id", *_COMMON_REQUIRED):
            if not item.get(key, defaults.get(key)):
                warnings.append(f"missing_required_attr:{key}")
        if str(item.get("status") or "").lower() == "supported":
            evidence = known(item.get("evidence_node_ids"))
            evidence.extend(known(item.get("supports")))
            if not evidence:
                warnings.append("supported_claim_without_known_evidence")
            if str(item.get("confidence_kind") or "").lower() == "model_inferred":
                deterministic = [
                    node_id for node_id in evidence
                    if _node_subtype_for_id(node_id, prefixes) in _DETERMINISTIC_EVIDENCE_SUBTYPES
                ]
                if not deterministic:
                    warnings.append(
                        "supported_model_inferred_claim_without_deterministic_evidence"
                    )
    else:
        text_key = "summary"
        kind_key = {
            "observation": "observation_kind",
            "experiment": "experiment_kind",
            "verification": "verdict",
            "decision": "decision_kind",
        }.get(record_kind, "kind")
        for key in (text_key, kind_key, "scope", *_COMMON_REQUIRED):
            if not item.get(key, defaults.get(key)):
                warnings.append(f"missing_required_attr:{key}")

    for key in (
        "subject_id",
        "session_id",
        "evidence_node_ids",
        "contradicting_node_ids",
        "target_node_ids",
        "produced_node_ids",
        "reference_node_ids",
        "dependency_node_ids",
        "supports",
        "contradicts",
        "verifies",
        "produces",
        "references",
    ):
        for node_id in _string_values(item.get(key)):
            if node_id and _node_subtype_for_id(node_id, prefixes) is None:
                warnings.append(f"unrecognized_node_id:{node_id}")
    return sorted(set(warnings))


def _edges_for_record(
    node_id: str,
    item: dict[str, Any],
    *,
    record_kind: str,
    repo: str,
    revision: dict[str, Any],
    run_id: str,
    prefixes: tuple[RecordPrefix, ...],
) -> list[EdgeFact]:
    edges: list[EdgeFact] = []

    def known(value: Any) -> list[str]:
        return _known_node_ids(value, prefixes)

    def add(edge_type: str, dst: str, *, role: str) -> None:
        subtype = _node_subtype_for_id(dst, prefixes)
        if not subtype:
            return
        evidence_confidence = (
            "deterministic"
            if subtype in _DETERMINISTIC_EVIDENCE_SUBTYPES
            else str(item.get("confidence_kind") or "")
        )
        edges.append(EdgeFact(
            src=node_id,
            dst=dst,
            edge_type=edge_type,
            attrs=_clean_attrs({
                "role": role,
                "target_subtype": subtype,
                "confidence_kind": evidence_confidence,
                "source": "agent_memory",
            }),
            source_record_id={
                "repo": repo,
                "node_id": node_id,
                "target_id": dst,
                "edge": edge_type,
                "role": role,
            },
            source_revision={**revision, "run_id": run_id},
            op="I",
        ))

    for dst in known(item.get("session_id")):
        add("references", dst, role="session_provenance")
    for dst in known(item.get("reference_node_ids")):
        add("references", dst, role="reference")
    for dst in known(item.get("references")):
        add("references", dst, role="reference")
    for dst in known(item.get("dependency_node_ids")):
        add("references", dst, role="dependency")

    if record_kind == "claim":
        for dst in known(item.get("subject_id")):
            add("references", dst, role="subject")
        for dst in known(item.get("evidence_node_ids")):
            add("supported_by", dst, role="supporting_evidence")
        for dst in known(item.get("supports")):
            add("supported_by", dst, role="supporting_evidence")
        for dst in known(item.get("contradicting_node_ids")):
            add("contradicted_by", dst, role="contradicting_evidence")
        for dst in known(item.get("contradicts")):
            add("contradicted_by", dst, role="contradicting_evidence")
    elif record_kind == "experiment":
        for dst in known(item.get("produced_node_ids")):
            add("produces", dst, role="produced")
        for dst in known(item.get("produces")):
            add("produces", dst, role="produced")
    elif record_kind == "verification":
        for dst in known(item.get("target_node_ids")):
            add("verifies", dst, role="verification_target")
        for dst in known(item.get("verifies")):
            add("verifies", dst, role="verification_target")
    return edges


def _string_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(v) for v in value if isinstance(v, str)]
    return []


def _known_node_ids(
    value: Any,
    prefixes: tuple[RecordPrefix, ...],
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for node_id in _string_values(value):
        if node_id in seen:
            continue
        if _node_subtype_for_id(node_id, prefixes) is None:
            continue
        seen.add(node_id)
        out.append(node_id)
    return out
