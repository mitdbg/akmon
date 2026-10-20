"""Normalized lineage envelopes for materialized outputs.

The envelope is the read-side contract that lets incremental maintenance
explain why an output is dirty or reusable. It is intentionally pure Python in
this first slice: persistence and reverse indexes build on the normalized
dictionary shape here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from okg.substrate.derived_artifacts.relations import stable_hash


LINEAGE_ENVELOPE_SCHEMA = "okg.materialized_lineage_envelope.v1"

OUTPUT_KINDS = frozenset({
    "derived_relation",
    "generated_page",
    "index_page",
    "consistency_observation",
    "native_view",
})

COMPONENT_KINDS = frozenset({
    "transform",
    "enricher",
    "checker",
    "view_component",
    "renderer",
})

INVALIDATION_SCOPES = frozenset({
    "per-page",
    "per-output",
    "per-scope",
    "per-node",
    "per-edge",
    "per-neighborhood",
    "per-index",
    "per-component",
    "global",
})


class LineageEnvelopeError(ValueError):
    """Raised when a materialized-output lineage envelope is incomplete."""


@dataclass(frozen=True)
class SourceRecordLineage:
    """Lineage for one raw/source record that contributed to an output."""

    source_id: str
    record_id: str
    content_hash: str
    source_revision: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty(self.source_id, "source_records[].source_id")
        _require_non_empty(self.record_id, "source_records[].record_id")
        _require_hash(self.content_hash, "source_records[].content_hash")
        _require_mapping(self.source_revision, "source_records[].source_revision")

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "record_id": self.record_id,
            "content_hash": self.content_hash,
            "source_revision": _stable_mapping(self.source_revision),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SourceRecordLineage":
        _require_mapping(value, "source_records[]")
        return cls(
            source_id=_required_str(value, "source_id", "source_records[]"),
            record_id=_required_str(value, "record_id", "source_records[]"),
            content_hash=_required_str(
                value, "content_hash", "source_records[]",
            ),
            source_revision=_optional_mapping(
                value, "source_revision", "source_records[]",
            ),
        )


@dataclass(frozen=True)
class GraphLineage:
    """Pinned graph/catalog context and touched graph object ids."""

    graph_generation_id: str | int | None = None
    graph_branch: str | None = None
    graph_checkpoint_id: str | int | None = None
    node_ids: Sequence[str] = ()
    edge_ids: Sequence[str] = ()

    def __post_init__(self) -> None:
        if (
            self.graph_generation_id is None
            and self.graph_checkpoint_id is None
            and not self.graph_branch
        ):
            raise LineageEnvelopeError(
                "graph requires graph_generation_id, graph_checkpoint_id, "
                "or graph_branch"
            )
        if self.graph_branch is not None:
            _require_non_empty(self.graph_branch, "graph.graph_branch")
        object.__setattr__(
            self, "node_ids", _string_tuple(self.node_ids, "graph.node_ids"),
        )
        object.__setattr__(
            self, "edge_ids", _string_tuple(self.edge_ids, "graph.edge_ids"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "graph_generation_id": (
                None if self.graph_generation_id is None
                else str(self.graph_generation_id)
            ),
            "graph_branch": self.graph_branch,
            "graph_checkpoint_id": (
                None if self.graph_checkpoint_id is None
                else str(self.graph_checkpoint_id)
            ),
            "node_ids": list(self.node_ids),
            "edge_ids": list(self.edge_ids),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "GraphLineage":
        _require_mapping(value, "graph")
        return cls(
            graph_generation_id=value.get("graph_generation_id"),
            graph_branch=value.get("graph_branch"),
            graph_checkpoint_id=value.get("graph_checkpoint_id"),
            node_ids=_optional_str_sequence(value, "node_ids", "graph"),
            edge_ids=_optional_str_sequence(value, "edge_ids", "graph"),
        )


@dataclass(frozen=True)
class ComponentLineage:
    """The transformation/check/view component whose code produced output."""

    component_id: str
    component_hash: str
    component_kind: str
    version: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty(self.component_id, "component.component_id")
        _require_hash(self.component_hash, "component.component_hash")
        if self.component_kind not in COMPONENT_KINDS:
            raise LineageEnvelopeError(
                "component.component_kind must be one of "
                f"{sorted(COMPONENT_KINDS)}; got {self.component_kind!r}"
            )
        if self.version is not None:
            _require_non_empty(self.version, "component.version")

    def to_dict(self) -> dict[str, Any]:
        return {
            "component_id": self.component_id,
            "component_hash": self.component_hash,
            "component_kind": self.component_kind,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ComponentLineage":
        _require_mapping(value, "component")
        return cls(
            component_id=_required_str(value, "component_id", "component"),
            component_hash=_required_str(value, "component_hash", "component"),
            component_kind=_required_str(value, "component_kind", "component"),
            version=_optional_str(value, "version", "component"),
        )


@dataclass(frozen=True)
class MaterializedLineageEnvelope:
    """Normalized dependency envelope for one materialized output."""

    output_id: str
    output_kind: str
    source_records: Sequence[SourceRecordLineage]
    graph: GraphLineage
    catalog_version_id: str | int
    component: ComponentLineage
    output_digest: str
    previous_output_digest: str | None
    invalidation_scope: str
    proposal_ids: Sequence[str] = ()
    evidence_ids: Sequence[str] = ()
    decision_ids: Sequence[str] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty(self.output_id, "output_id")
        if self.output_kind not in OUTPUT_KINDS:
            raise LineageEnvelopeError(
                "output_kind must be one of "
                f"{sorted(OUTPUT_KINDS)}; got {self.output_kind!r}"
            )
        source_records = tuple(self.source_records)
        if not source_records:
            raise LineageEnvelopeError(
                "source_records must contain at least one source record"
            )
        if not all(
            isinstance(item, SourceRecordLineage) for item in source_records
        ):
            raise LineageEnvelopeError(
                "source_records must contain SourceRecordLineage values"
            )
        if not isinstance(self.graph, GraphLineage):
            raise LineageEnvelopeError("graph must be a GraphLineage value")
        if not isinstance(self.component, ComponentLineage):
            raise LineageEnvelopeError(
                "component must be a ComponentLineage value"
            )
        _require_non_empty(str(self.catalog_version_id), "catalog_version_id")
        _require_hash(self.output_digest, "output_digest")
        if self.previous_output_digest is not None:
            _require_hash(
                self.previous_output_digest,
                "previous_output_digest",
            )
        if self.invalidation_scope not in INVALIDATION_SCOPES:
            raise LineageEnvelopeError(
                "invalidation_scope must be one of "
                f"{sorted(INVALIDATION_SCOPES)}; "
                f"got {self.invalidation_scope!r}"
            )
        object.__setattr__(self, "source_records", source_records)
        object.__setattr__(
            self,
            "proposal_ids",
            _string_tuple(self.proposal_ids, "proposal_ids"),
        )
        object.__setattr__(
            self,
            "evidence_ids",
            _string_tuple(self.evidence_ids, "evidence_ids"),
        )
        object.__setattr__(
            self,
            "decision_ids",
            _string_tuple(self.decision_ids, "decision_ids"),
        )
        _require_mapping(self.metadata, "metadata")

    @property
    def envelope_hash(self) -> str:
        return "sha256:" + stable_hash(self.to_dict())

    def dependency_keys(self) -> dict[str, list[str]]:
        """Return normalized reverse-index keys derivable from this envelope."""

        keys = {
            "source_record": [
                f"{record.source_id}:{record.record_id}"
                for record in self.source_records
            ],
            "source_content_hash": [
                f"{record.source_id}:{record.content_hash}"
                for record in self.source_records
            ],
            "graph_node": list(self.graph.node_ids),
            "graph_edge": list(self.graph.edge_ids),
            "component": [self.component.component_id],
            "component_hash": [self.component.component_hash],
            "proposal": list(self.proposal_ids),
            "evidence": list(self.evidence_ids),
            "decision": list(self.decision_ids),
        }
        return {
            key: sorted(dict.fromkeys(value))
            for key, value in keys.items()
            if value
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": LINEAGE_ENVELOPE_SCHEMA,
            "output_id": self.output_id,
            "output_kind": self.output_kind,
            "source_records": [
                record.to_dict() for record in self.source_records
            ],
            "graph": self.graph.to_dict(),
            "catalog_version_id": str(self.catalog_version_id),
            "component": self.component.to_dict(),
            "output_digest": self.output_digest,
            "previous_output_digest": self.previous_output_digest,
            "invalidation_scope": self.invalidation_scope,
            "proposal_ids": list(self.proposal_ids),
            "evidence_ids": list(self.evidence_ids),
            "decision_ids": list(self.decision_ids),
            "metadata": _stable_mapping(self.metadata),
        }

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "MaterializedLineageEnvelope":
        _require_mapping(value, "lineage_envelope")
        schema = _required_str(value, "schema", "lineage_envelope")
        if schema != LINEAGE_ENVELOPE_SCHEMA:
            raise LineageEnvelopeError(
                f"unsupported lineage envelope schema {schema!r}"
            )
        raw_records = value.get("source_records")
        if not isinstance(raw_records, Sequence) or isinstance(
            raw_records,
            (str, bytes),
        ):
            raise LineageEnvelopeError(
                "lineage_envelope.source_records must be a list"
            )
        return cls(
            output_id=_required_str(value, "output_id", "lineage_envelope"),
            output_kind=_required_str(value, "output_kind", "lineage_envelope"),
            source_records=[
                SourceRecordLineage.from_dict(_as_mapping(
                    item,
                    "lineage_envelope.source_records[]",
                ))
                for item in raw_records
            ],
            graph=GraphLineage.from_dict(_required_mapping(
                value,
                "graph",
                "lineage_envelope",
            )),
            catalog_version_id=_required_str(
                value, "catalog_version_id", "lineage_envelope",
            ),
            component=ComponentLineage.from_dict(_required_mapping(
                value,
                "component",
                "lineage_envelope",
            )),
            output_digest=_required_str(
                value, "output_digest", "lineage_envelope",
            ),
            previous_output_digest=_optional_str(
                value,
                "previous_output_digest",
                "lineage_envelope",
            ),
            invalidation_scope=_required_str(
                value, "invalidation_scope", "lineage_envelope",
            ),
            proposal_ids=_optional_str_sequence(
                value, "proposal_ids", "lineage_envelope",
            ),
            evidence_ids=_optional_str_sequence(
                value, "evidence_ids", "lineage_envelope",
            ),
            decision_ids=_optional_str_sequence(
                value, "decision_ids", "lineage_envelope",
            ),
            metadata=_optional_mapping(
                value, "metadata", "lineage_envelope",
            ),
        )


def _require_non_empty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise LineageEnvelopeError(f"{field_name} is required")


def _require_hash(value: str, field_name: str) -> None:
    _require_non_empty(value, field_name)
    if not value.startswith(("sha256:", "hash:", "dep:")):
        raise LineageEnvelopeError(
            f"{field_name} must be an explicit hash string"
        )


def _require_mapping(value: Any, field_name: str) -> None:
    if not isinstance(value, Mapping):
        raise LineageEnvelopeError(f"{field_name} must be a mapping")


def _required_str(
    value: Mapping[str, Any],
    key: str,
    context: str,
) -> str:
    if key not in value:
        raise LineageEnvelopeError(f"{context}.{key} is required")
    raw = value[key]
    if not isinstance(raw, str) or not raw.strip():
        raise LineageEnvelopeError(f"{context}.{key} must be a non-empty string")
    return raw


def _optional_str(
    value: Mapping[str, Any],
    key: str,
    context: str,
) -> str | None:
    if key not in value or value[key] is None:
        return None
    raw = value[key]
    if not isinstance(raw, str) or not raw.strip():
        raise LineageEnvelopeError(f"{context}.{key} must be a non-empty string")
    return raw


def _required_mapping(
    value: Mapping[str, Any],
    key: str,
    context: str,
) -> Mapping[str, Any]:
    if key not in value:
        raise LineageEnvelopeError(f"{context}.{key} is required")
    return _as_mapping(value[key], f"{context}.{key}")


def _optional_mapping(
    value: Mapping[str, Any],
    key: str,
    context: str,
) -> Mapping[str, Any]:
    if key not in value or value[key] is None:
        return {}
    return _as_mapping(value[key], f"{context}.{key}")


def _as_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    _require_mapping(value, field_name)
    return value


def _optional_str_sequence(
    value: Mapping[str, Any],
    key: str,
    context: str,
) -> tuple[str, ...]:
    if key not in value or value[key] is None:
        return ()
    return _string_tuple(value[key], f"{context}.{key}")


def _string_tuple(values: Sequence[str], field_name: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise LineageEnvelopeError(f"{field_name} must be a list of strings")
    out: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise LineageEnvelopeError(
                f"{field_name} must contain only non-empty strings"
            )
        out.append(value)
    return tuple(out)


def _stable_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): value[key] for key in sorted(value)}


__all__ = [
    "COMPONENT_KINDS",
    "INVALIDATION_SCOPES",
    "LINEAGE_ENVELOPE_SCHEMA",
    "OUTPUT_KINDS",
    "ComponentLineage",
    "GraphLineage",
    "LineageEnvelopeError",
    "MaterializedLineageEnvelope",
    "SourceRecordLineage",
]
