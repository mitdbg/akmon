"""Bounded deployment attestations evaluated immediately before checkpointing.

The public protocol deliberately exposes no database handle or arbitrary query
surface.  Deployment code receives a typed context whose reads are implemented
by fixed, bounded query templates, and returns one exact result record.  The
publisher validates and canonicalizes that record before adding it to checkpoint
metadata.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import sys
import threading
import unicodedata
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    ClassVar,
    Iterator,
    Mapping,
    Protocol,
    Sequence,
    runtime_checkable,
)

import psycopg
import yaml
from jsonschema import Draft202012Validator


ATTESTATION_ENVELOPE_SCHEMA = "okg.deployment_attestations.v1"
MAX_ATTESTORS = 8
MAX_PAYLOAD_BYTES = 65_536
MAX_ENVELOPE_BYTES = 262_144
MAX_JSON_DEPTH = 16
MAX_COLLECTION_MEMBERS = 2_048
MAX_STRING_BYTES = 16_384
MAX_CONTEXT_ROWS = 10_000
MAX_TRAVERSAL_VALUES = 131_072
MAX_CONFIG_DEPTH = 32
MAX_CONFIG_VALUES = 131_072
_DECLARATIONS_UNSET = object()
_ATTESTOR_IMPLEMENTATION_ROOTS: set[Path] = set()

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_IDENTIFIER_RE = re.compile(
    r"^[a-z][a-z0-9]*(?:\.[a-z][a-z0-9_-]*)+$"
)
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_ENV_PLACEHOLDER_RE = re.compile(r"\$\{[^{}]+\}")
_PUBLIC_REFUSAL_CODES = frozenset({
    "attestation_invalid",
    "attestation_unavailable",
})
_FAILURE_CODES = frozenset({
    "attestor_exception",
    "attestation_unavailable",
    "attestation_invalid",
    "attestation_digest_mismatch",
    "attestation_limit_exceeded",
    "attestation_duplicate_id",
})
_RESERVED_CONFIG_KEYS = frozenset({
    "database",
    "dsn",
    "connection",
    "conn",
    "deployment",
    "deployment_name",
    "deployment_root",
    "branch",
    "branch_name",
    "generation_id",
    "native_generation_id",
    "target_generation_id",
    "target_generation_token",
    "catalog_identity",
    "catalog_version_id",
    "context",
})


class CheckpointAttestationRefused(Exception):
    """A deployment attestor's explicit, retry-safe refusal."""

    def __init__(self, code: str, detail: str | None = None) -> None:
        if code not in _PUBLIC_REFUSAL_CODES:
            raise ValueError(
                "checkpoint attestation refusal code must be "
                "attestation_invalid or attestation_unavailable"
            )
        super().__init__(code)
        self.code = code
        self.detail = _bounded_public_detail(detail)


class CheckpointAttestationFailure(RuntimeError):
    """Publisher-normalized attestation failure safe for operator output."""

    def __init__(
        self,
        *,
        attestor_id: str,
        code: str,
        detail: str | None = None,
    ) -> None:
        if code not in _FAILURE_CODES:
            raise ValueError(f"unknown checkpoint attestation failure code: {code}")
        super().__init__(f"checkpoint attestation failed: {attestor_id} ({code})")
        self.attestor_id = attestor_id
        self.code = code
        self.detail = _bounded_public_detail(detail)

    def blocked_reason(self) -> dict[str, Any]:
        reason: dict[str, Any] = {
            "reason": "checkpoint_attestation_failed",
            "attestor_id": self.attestor_id,
            "code": self.code,
        }
        if self.detail:
            reason["detail"] = self.detail
        return reason


class CheckpointAttestationConfigurationError(ValueError):
    """Manifest or class configuration cannot form a valid attestor."""

    def __init__(self, message: str, *, code: str = "attestation_invalid") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class CheckpointAttestationResult:
    """The only successful value an attestor may return."""

    attestor_id: str
    schema: str
    status: str
    payload: Any
    payload_digest: str


@runtime_checkable
class CheckpointAttestor(Protocol):
    """Deployment-owned deterministic, read-only checkpoint validator."""

    attestor_id: ClassVar[str]
    config_schema: ClassVar[Mapping[str, Any]]

    def attest(
        self,
        context: "CheckpointAttestationContext",
    ) -> CheckpointAttestationResult: ...


@dataclass(frozen=True)
class LoadedCheckpointAttestor:
    """One manifest declaration bound to its loaded class instance."""

    manifest_id: str
    class_name: str
    attestor: CheckpointAttestor
    implementation_root: Path | None = None
    local_module_prefixes: tuple[str, ...] = ()


@dataclass(frozen=True)
class CheckpointAttestorDeclaration:
    """One strictly parsed manifest declaration, before custom-code import."""

    manifest_id: str
    class_ref: str
    config: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class CheckpointCandidatePage:
    """One deterministic page from the complete publication candidate."""

    rows: tuple[dict[str, Any], ...]
    next_cursor: str | None
    complete: bool


class _CandidateReader:
    """Internal fixed-query adapter; never passed to deployment code directly."""

    __slots__ = ("_branch", "_conn", "_session")

    def __init__(self, conn: psycopg.Connection, session: Any, branch: str) -> None:
        self._conn = conn
        self._session = session
        self._branch = branch

    @staticmethod
    def _limit(value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise CheckpointAttestationRefused(
                "attestation_invalid", "read limit must be an integer"
            )
        if value < 1 or value > MAX_CONTEXT_ROWS:
            raise CheckpointAttestationRefused(
                "attestation_invalid",
                f"read limit must be between 1 and {MAX_CONTEXT_ROWS}",
            )
        return value

    @staticmethod
    def _filters(values: Sequence[str] | None, *, label: str) -> tuple[str, ...]:
        if values is None:
            return ()
        if isinstance(values, (str, bytes)):
            raise CheckpointAttestationRefused(
                "attestation_invalid", f"{label} must be a sequence of strings"
            )
        normalized: list[str] = []
        for value in values:
            if not isinstance(value, str) or not value:
                raise CheckpointAttestationRefused(
                    "attestation_invalid", f"{label} must contain non-empty strings"
                )
            normalized.append(value)
        if len(normalized) > MAX_COLLECTION_MEMBERS:
            raise CheckpointAttestationRefused(
                "attestation_invalid", f"{label} exceeds the fixed member limit"
            )
        return tuple(dict.fromkeys(normalized))

    @staticmethod
    def _cursor(value: str | None, *, label: str) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise CheckpointAttestationRefused(
                "attestation_invalid", f"{label} must be a non-empty string"
            )
        return value

    def nodes(
        self,
        *,
        subtypes: Sequence[str] | None,
        sources: Sequence[str] | None,
        limit: int,
    ) -> tuple[dict[str, Any], ...]:
        bounded = self._limit(limit)
        subtype_values = self._filters(subtypes, label="subtypes")
        source_values = self._filters(sources, label="sources")
        clauses: list[str] = []
        params: dict[str, Any] = {}
        if subtype_values:
            clauses.append("subtype = ANY(:subtypes)")
            params["subtypes"] = list(subtype_values)
        if source_values:
            clauses.append("source = ANY(:sources)")
            params["sources"] = list(source_values)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._session.query(
            "SELECT node_id, subtype, attrs, text_hash, source, "
            "source_record_id, source_revision, run_id, observed_at "
            f"FROM okg.graph_nodes{where} ORDER BY node_id LIMIT {bounded + 1}",
            params,
        )
        return _complete_bounded_rows(rows, bounded, label="candidate nodes")

    def edges(
        self,
        *,
        edge_types: Sequence[str] | None,
        sources: Sequence[str] | None,
        limit: int,
    ) -> tuple[dict[str, Any], ...]:
        bounded = self._limit(limit)
        type_values = self._filters(edge_types, label="edge_types")
        source_values = self._filters(sources, label="sources")
        clauses: list[str] = []
        params: dict[str, Any] = {}
        if type_values:
            clauses.append("edge_type = ANY(:edge_types)")
            params["edge_types"] = list(type_values)
        if source_values:
            clauses.append("source = ANY(:sources)")
            params["sources"] = list(source_values)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._session.query(
            "SELECT edge_id, src, dst, edge_type, attrs, provenance, confidence, "
            "source, source_record_id, source_revision, run_id, observed_at "
            f"FROM okg.graph_edges{where} ORDER BY edge_id LIMIT {bounded + 1}",
            params,
        )
        return _complete_bounded_rows(rows, bounded, label="candidate edges")

    def node_page(
        self,
        *,
        after_node_id: str | None,
        subtypes: Sequence[str] | None,
        sources: Sequence[str] | None,
        limit: int,
    ) -> CheckpointCandidatePage:
        bounded = self._limit(limit)
        cursor = self._cursor(after_node_id, label="after_node_id")
        subtype_values = self._filters(subtypes, label="subtypes")
        source_values = self._filters(sources, label="sources")
        clauses: list[str] = []
        params: dict[str, Any] = {}
        if cursor is not None:
            clauses.append("node_id > :after_node_id")
            params["after_node_id"] = cursor
        if subtype_values:
            clauses.append("subtype = ANY(:subtypes)")
            params["subtypes"] = list(subtype_values)
        if source_values:
            clauses.append("source = ANY(:sources)")
            params["sources"] = list(source_values)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._session.query(
            "SELECT node_id, subtype, attrs, text_hash, source, "
            "source_record_id, source_revision, run_id, observed_at "
            f"FROM okg.graph_nodes{where} ORDER BY node_id LIMIT {bounded + 1}",
            params,
        )
        return _candidate_page(
            rows,
            bounded,
            cursor_key="node_id",
            after_cursor=cursor,
            label="candidate nodes",
        )

    def edge_page(
        self,
        *,
        after_edge_id: str | None,
        edge_types: Sequence[str] | None,
        sources: Sequence[str] | None,
        limit: int,
    ) -> CheckpointCandidatePage:
        bounded = self._limit(limit)
        cursor = self._cursor(after_edge_id, label="after_edge_id")
        type_values = self._filters(edge_types, label="edge_types")
        source_values = self._filters(sources, label="sources")
        clauses: list[str] = []
        params: dict[str, Any] = {}
        if cursor is not None:
            clauses.append("edge_id > :after_edge_id")
            params["after_edge_id"] = cursor
        if type_values:
            clauses.append("edge_type = ANY(:edge_types)")
            params["edge_types"] = list(type_values)
        if source_values:
            clauses.append("source = ANY(:sources)")
            params["sources"] = list(source_values)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._session.query(
            "SELECT edge_id, src, dst, edge_type, attrs, provenance, confidence, "
            "source, source_record_id, source_revision, run_id, observed_at "
            f"FROM okg.graph_edges{where} ORDER BY edge_id LIMIT {bounded + 1}",
            params,
        )
        return _candidate_page(
            rows,
            bounded,
            cursor_key="edge_id",
            after_cursor=cursor,
            label="candidate edges",
        )

    def watermarks(
        self,
        *,
        source_ids: Sequence[str] | None,
        limit: int,
    ) -> tuple[dict[str, Any], ...]:
        bounded = self._limit(limit)
        sources = self._filters(source_ids, label="source_ids")
        query = (
            "SELECT source, cutover_watermark, last_applied_rev, "
            "last_published_rev, projection_cursor, last_reconcile_at, "
            "replay_required, replay_required_at "
            "FROM okg.source_watermarks"
        )
        params: list[Any] = []
        if sources:
            query += " WHERE source = ANY(%s)"
            params.append(list(sources))
        query += " ORDER BY source LIMIT %s"
        params.append(bounded + 1)
        with self._conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc.name for desc in cur.description or ()]
            rows = [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
        return _complete_bounded_rows(rows, bounded, label="source watermarks")


_CANDIDATE_READERS: dict[object, _CandidateReader] = {}
_CANDIDATE_READERS_LOCK = threading.RLock()


def _candidate_reader(token: object) -> _CandidateReader:
    with _CANDIDATE_READERS_LOCK:
        reader = _CANDIDATE_READERS.get(token)
    if reader is None:
        raise CheckpointAttestationRefused(
            "attestation_unavailable", "candidate read context is no longer active"
        )
    return reader


@dataclass(frozen=True, slots=True)
class CheckpointAttestationContext:
    """Publisher-owned identities plus fixed, bounded read operations."""

    deployment_name: str
    branch_name: str
    native_generation_id: str
    target_generation_token: int
    catalog_version_id: int
    node_count: int
    edge_count: int
    _reader_token: object

    def candidate_nodes(
        self,
        *,
        subtypes: Sequence[str] | None = None,
        sources: Sequence[str] | None = None,
        limit: int = MAX_CONTEXT_ROWS,
    ) -> tuple[dict[str, Any], ...]:
        return _candidate_reader(self._reader_token).nodes(
            subtypes=subtypes, sources=sources, limit=limit
        )

    def candidate_edges(
        self,
        *,
        edge_types: Sequence[str] | None = None,
        sources: Sequence[str] | None = None,
        limit: int = MAX_CONTEXT_ROWS,
    ) -> tuple[dict[str, Any], ...]:
        return _candidate_reader(self._reader_token).edges(
            edge_types=edge_types, sources=sources, limit=limit
        )

    def candidate_nodes_page(
        self,
        *,
        after_node_id: str | None = None,
        subtypes: Sequence[str] | None = None,
        sources: Sequence[str] | None = None,
        limit: int = MAX_CONTEXT_ROWS,
    ) -> CheckpointCandidatePage:
        """Read one keyset page without imposing a whole-candidate row cap."""

        return _candidate_reader(self._reader_token).node_page(
            after_node_id=after_node_id,
            subtypes=subtypes,
            sources=sources,
            limit=limit,
        )

    def candidate_edges_page(
        self,
        *,
        after_edge_id: str | None = None,
        edge_types: Sequence[str] | None = None,
        sources: Sequence[str] | None = None,
        limit: int = MAX_CONTEXT_ROWS,
    ) -> CheckpointCandidatePage:
        """Read one keyset page without imposing a whole-candidate row cap."""

        return _candidate_reader(self._reader_token).edge_page(
            after_edge_id=after_edge_id,
            edge_types=edge_types,
            sources=sources,
            limit=limit,
        )

    def source_watermarks(
        self,
        *,
        source_ids: Sequence[str] | None = None,
        limit: int = MAX_COLLECTION_MEMBERS,
    ) -> tuple[dict[str, Any], ...]:
        return _candidate_reader(self._reader_token).watermarks(
            source_ids=source_ids, limit=limit
        )

    def candidate_counts(self) -> dict[str, int]:
        return {"nodes": self.node_count, "edges": self.edge_count}


def load_checkpoint_attestors(
    deployment_dir: Path | str,
    *,
    deployment_name: str,
    declarations: Any = _DECLARATIONS_UNSET,
    implementation_root: Path | str | None = None,
) -> list[LoadedCheckpointAttestor]:
    """Load checkpoint attestors from one explicit declaration authority.

    Omitting ``declarations`` preserves the tracking-mode ``deployment.yaml``
    contract. Passing a value, including an empty list, selects an already
    composed authority such as a sealed deployment definition and never reopens
    the legacy manifest. ``implementation_root`` may select a different
    in-bundle code and bounded-data root without changing declaration authority.
    """

    authority_dir = Path(deployment_dir).resolve()
    attestor_root = Path(implementation_root or authority_dir).resolve()
    if not attestor_root.is_dir():
        raise CheckpointAttestationConfigurationError(
            "checkpoint attestor implementation root is missing or not a directory: "
            f"{attestor_root}"
        )
    inspected = (
        inspect_checkpoint_attestor_declarations(
            authority_dir,
            deployment_name=deployment_name,
        )
        if declarations is _DECLARATIONS_UNSET
        else inspect_checkpoint_attestor_declaration_block(
            declarations,
            deployment_name=deployment_name,
        )
    )

    loaded: list[LoadedCheckpointAttestor] = []
    for declaration in inspected:
        manifest_id = declaration.manifest_id
        class_ref = declaration.class_ref
        config = declaration.config
        cls, fqcn, local_module_prefixes = _load_attestor_class(
            class_ref,
            deployment_dir=attestor_root,
            deployment_name=deployment_name,
        )
        class_id = getattr(cls, "attestor_id", None)
        _validate_identifier(class_id, label=f"{fqcn}.attestor_id")
        if class_id != manifest_id:
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor id mismatch: manifest {manifest_id!r}, "
                f"class {class_id!r}"
            )
        schema = getattr(cls, "config_schema", None)
        _validate_config_schema(schema, fqcn=fqcn)
        validator = Draft202012Validator(schema)
        errors = sorted(validator.iter_errors(config), key=lambda item: list(item.path))
        if errors:
            first = errors[0]
            location = ".".join(str(part) for part in first.path) or "config"
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor {manifest_id!r} config rejected at {location}: "
                f"{first.message}"
            )
        try:
            instance = cls(deployment_root=attestor_root, **config)
        except Exception as exc:
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor {manifest_id!r} could not be constructed"
            ) from exc
        if not callable(getattr(instance, "attest", None)):
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor class {fqcn!r} does not implement attest(context)"
            )
        loaded.append(
            LoadedCheckpointAttestor(
                manifest_id,
                fqcn,
                instance,
                implementation_root=attestor_root,
                local_module_prefixes=local_module_prefixes,
            )
        )
    return validate_loaded_checkpoint_attestors(loaded)


def inspect_checkpoint_attestor_declarations(
    deployment_dir: Path | str,
    *,
    deployment_name: str,
) -> list[CheckpointAttestorDeclaration]:
    """Strictly inspect legacy declarations without importing deployment code."""

    dep_dir = Path(deployment_dir).resolve()
    manifest_path = dep_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return []
    declarations = _checkpoint_attestor_block_from_yaml(
        manifest_path,
        document_label="deployment.yaml",
        authority_label="deployment manifest",
    )
    if declarations is _DECLARATIONS_UNSET:
        return []
    return inspect_checkpoint_attestor_declaration_block(
        declarations,
        deployment_name=deployment_name,
    )


def inspect_checkpoint_attestor_definition_declarations(
    definition_path: Path | str,
    *,
    deployment_name: str,
) -> list[CheckpointAttestorDeclaration]:
    """Strictly inspect declarations authored in a sealed definition file."""

    declarations = _checkpoint_attestor_block_from_yaml(
        Path(definition_path),
        document_label="deployment definition",
        authority_label="deployment definition",
    )
    if declarations is _DECLARATIONS_UNSET:
        return []
    return inspect_checkpoint_attestor_declaration_block(
        declarations,
        deployment_name=deployment_name,
    )


def _checkpoint_attestor_block_from_yaml(
    path: Path,
    *,
    document_label: str,
    authority_label: str,
) -> Any:
    """Return one duplicate-safe YAML subtree without constructing siblings."""

    try:
        document_node = yaml.compose(
            path.read_text(encoding="utf-8"),
            Loader=yaml.SafeLoader,
        )
    except CheckpointAttestationConfigurationError:
        raise
    except Exception as exc:
        raise CheckpointAttestationConfigurationError(
            f"{document_label} could not be parsed for checkpoint attestors"
        ) from exc
    if document_node is None:
        return _DECLARATIONS_UNSET
    if not isinstance(document_node, yaml.nodes.MappingNode):
        raise CheckpointAttestationConfigurationError(
            f"{document_label} must be a mapping"
        )
    attestor_nodes = [
        value_node
        for key_node, value_node in document_node.value
        if isinstance(key_node, yaml.nodes.ScalarNode)
        and key_node.value == "checkpoint_attestors"
    ]
    if not attestor_nodes:
        return _DECLARATIONS_UNSET
    if len(attestor_nodes) > 1:
        raise CheckpointAttestationConfigurationError(
            f"{authority_label} contains duplicate key 'checkpoint_attestors'"
        )
    return _construct_strict_yaml_subtree(
        attestor_nodes[0],
        path="checkpoint_attestors",
        active=set(),
        budget=[MAX_CONFIG_VALUES],
        depth=0,
    )


def inspect_checkpoint_attestor_declaration_block(
    declarations: Any,
    *,
    deployment_name: str,
) -> list[CheckpointAttestorDeclaration]:
    """Validate one constructed declaration block without importing code."""

    if not isinstance(declarations, list):
        raise CheckpointAttestationConfigurationError(
            "checkpoint_attestors must be a list"
        )
    if len(declarations) > MAX_ATTESTORS:
        raise CheckpointAttestationConfigurationError(
            f"checkpoint_attestors permits at most {MAX_ATTESTORS} entries",
            code="attestation_limit_exceeded",
        )

    inspected: list[CheckpointAttestorDeclaration] = []
    seen_ids: set[str] = set()
    for index, declaration in enumerate(declarations):
        if not isinstance(declaration, dict):
            raise CheckpointAttestationConfigurationError(
                f"checkpoint_attestors[{index}] must be a mapping"
            )
        unknown = set(declaration) - {"id", "class", "config"}
        missing = {"id", "class", "config"} - set(declaration)
        if unknown or missing:
            raise CheckpointAttestationConfigurationError(
                f"checkpoint_attestors[{index}] must contain exactly id, class, config"
            )
        manifest_id = declaration["id"]
        class_ref = declaration["class"]
        config = declaration["config"]
        _validate_identifier(manifest_id, label=f"checkpoint_attestors[{index}].id")
        if manifest_id in seen_ids:
            raise CheckpointAttestationConfigurationError(
                f"duplicate checkpoint attestor id: {manifest_id}",
                code="attestation_duplicate_id",
            )
        seen_ids.add(manifest_id)
        if not isinstance(class_ref, str) or not class_ref.strip():
            raise CheckpointAttestationConfigurationError(
                f"checkpoint_attestors[{index}].class must be a non-empty string"
            )
        normalized_class_ref = class_ref.strip()
        _split_attestor_class_ref(
            normalized_class_ref,
            deployment_name=deployment_name,
        )
        if not isinstance(config, dict):
            raise CheckpointAttestationConfigurationError(
                f"checkpoint_attestors[{index}].config must be a mapping"
            )
        _reject_placeholders(config, path=f"checkpoint_attestors[{index}].config")
        _reject_reserved_config(config, path=f"checkpoint_attestors[{index}].config")
        inspected.append(CheckpointAttestorDeclaration(
            manifest_id=manifest_id,
            class_ref=normalized_class_ref,
            config=dict(config),
        ))
    return inspected


def _construct_strict_yaml_subtree(
    node: yaml.nodes.Node,
    *,
    path: str,
    active: set[int],
    budget: list[int],
    depth: int,
) -> Any:
    """Safely construct only the attestor subtree with duplicate/cycle checks."""

    if depth > MAX_CONFIG_DEPTH:
        raise CheckpointAttestationConfigurationError(
            f"{path} exceeds the configuration nesting limit",
            code="attestation_limit_exceeded",
        )
    budget[0] -= 1
    if budget[0] < 0:
        raise CheckpointAttestationConfigurationError(
            f"{path} exceeds the configuration value limit",
            code="attestation_limit_exceeded",
        )
    node_id = id(node)
    if node_id in active:
        raise CheckpointAttestationConfigurationError(
            f"{path} contains a recursive YAML alias"
        )
    active.add(node_id)
    try:
        if isinstance(node, yaml.nodes.ScalarNode):
            loader = yaml.SafeLoader("")
            try:
                return loader.construct_object(node, deep=True)
            finally:
                loader.dispose()
        if isinstance(node, yaml.nodes.SequenceNode):
            return [
                _construct_strict_yaml_subtree(
                    item,
                    path=f"{path}[{index}]",
                    active=active,
                    budget=budget,
                    depth=depth + 1,
                )
                for index, item in enumerate(node.value)
            ]
        if isinstance(node, yaml.nodes.MappingNode):
            result: dict[Any, Any] = {}
            for key_node, value_node in node.value:
                if not isinstance(key_node, yaml.nodes.ScalarNode):
                    raise CheckpointAttestationConfigurationError(
                        f"{path} mappings must use scalar keys"
                    )
                key = _construct_strict_yaml_subtree(
                    key_node,
                    path=f"{path}.<key>",
                    active=active,
                    budget=budget,
                    depth=depth + 1,
                )
                try:
                    duplicate = key in result
                except TypeError as exc:
                    raise CheckpointAttestationConfigurationError(
                        f"{path} mappings must use scalar keys"
                    ) from exc
                if duplicate:
                    raise CheckpointAttestationConfigurationError(
                        f"{path} contains duplicate key {key!r}"
                    )
                result[key] = _construct_strict_yaml_subtree(
                    value_node,
                    path=f"{path}.{key}",
                    active=active,
                    budget=budget,
                    depth=depth + 1,
                )
            return result
        raise CheckpointAttestationConfigurationError(
            f"{path} contains an unsupported YAML value"
        )
    finally:
        active.remove(node_id)


def validate_loaded_checkpoint_attestors(
    attestors: Sequence[LoadedCheckpointAttestor] | None,
) -> list[LoadedCheckpointAttestor]:
    if attestors is None:
        normalized: list[LoadedCheckpointAttestor] = []
    elif isinstance(attestors, (str, bytes, Mapping)):
        raise CheckpointAttestationConfigurationError(
            "checkpoint_attestors must be an ordered sequence"
        )
    else:
        normalized = list(attestors)
    if len(normalized) > MAX_ATTESTORS:
        raise CheckpointAttestationConfigurationError(
            f"checkpoint attestations permit at most {MAX_ATTESTORS} attestors",
            code="attestation_limit_exceeded",
        )
    seen: set[str] = set()
    for item in normalized:
        if not isinstance(item, LoadedCheckpointAttestor):
            raise CheckpointAttestationConfigurationError(
                "checkpoint_attestors must contain LoadedCheckpointAttestor values"
            )
        _validate_identifier(item.manifest_id, label="checkpoint attestor id")
        if item.manifest_id in seen:
            raise CheckpointAttestationConfigurationError(
                f"duplicate checkpoint attestor id: {item.manifest_id}",
                code="attestation_duplicate_id",
            )
        seen.add(item.manifest_id)
        if getattr(item.attestor, "attestor_id", None) != item.manifest_id:
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor instance id does not match {item.manifest_id!r}"
            )
        if not callable(getattr(item.attestor, "attest", None)):
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor {item.manifest_id!r} has no attest(context) method"
            )
    return normalized


@contextmanager
def checkpoint_attestation_context(
    *,
    conn: psycopg.Connection,
    candidate_session: Any,
    deployment_name: str,
    branch_name: str,
    native_generation_id: str,
    target_generation_token: int,
    catalog_version_id: int,
    node_count: int,
    edge_count: int,
) -> Iterator[CheckpointAttestationContext]:
    token = object()
    with _CANDIDATE_READERS_LOCK:
        _CANDIDATE_READERS[token] = _CandidateReader(
            conn, candidate_session, branch_name
        )
    context = CheckpointAttestationContext(
        deployment_name=deployment_name,
        branch_name=branch_name,
        native_generation_id=native_generation_id,
        target_generation_token=target_generation_token,
        catalog_version_id=catalog_version_id,
        node_count=node_count,
        edge_count=edge_count,
        _reader_token=token,
    )
    try:
        yield context
    finally:
        with _CANDIDATE_READERS_LOCK:
            _CANDIDATE_READERS.pop(token, None)


def run_checkpoint_attestors(
    attestors: Sequence[LoadedCheckpointAttestor],
    *,
    context: CheckpointAttestationContext,
) -> dict[str, Any]:
    """Run each required attestor once and return a verified metadata envelope."""

    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for loaded in validate_loaded_checkpoint_attestors(attestors):
        attestor_id = loaded.manifest_id
        if attestor_id in seen:
            raise CheckpointAttestationFailure(
                attestor_id=attestor_id,
                code="attestation_duplicate_id",
            )
        seen.add(attestor_id)
        try:
            result = _run_bound_checkpoint_attestor(loaded, context=context)
        except CheckpointAttestationRefused as exc:
            raise CheckpointAttestationFailure(
                attestor_id=attestor_id,
                code=exc.code,
                detail=exc.detail,
            ) from exc
        except CheckpointAttestationConfigurationError as exc:
            raise CheckpointAttestationFailure(
                attestor_id=attestor_id,
                code=exc.code,
            ) from exc
        except Exception as exc:
            raise CheckpointAttestationFailure(
                attestor_id=attestor_id,
                code="attestor_exception",
            ) from exc
        validated = _validate_result(result, expected_id=attestor_id)
        entries.append({
            "id": validated.attestor_id,
            "class": loaded.class_name,
            "schema": validated.schema,
            "status": validated.status,
            "payload": validated.payload,
            "payload_digest": validated.payload_digest,
        })

    envelope = {
        "schema": ATTESTATION_ENVELOPE_SCHEMA,
        "attestations": entries,
    }
    try:
        canonicalize_json(envelope, max_bytes=MAX_ENVELOPE_BYTES)
    except CheckpointAttestationFailure as exc:
        raise CheckpointAttestationFailure(
            attestor_id=(entries[-1]["id"] if entries else "publisher.attestations"),
            code=exc.code,
        ) from exc
    return envelope


def _run_bound_checkpoint_attestor(
    loaded: LoadedCheckpointAttestor,
    *,
    context: CheckpointAttestationContext,
) -> CheckpointAttestationResult:
    root = loaded.implementation_root
    if root is None:
        return loaded.attestor.attest(context)

    from okg.substrate.deployments import (
        deployment_module_import_lock,
        inject_deployment_path,
    )

    module_name = type(loaded.attestor).__module__
    with deployment_module_import_lock():
        local_prefixes = _prepare_attestor_implementation_import(
            root,
            module_name=module_name,
            class_ref=loaded.class_name,
        )
        if local_prefixes != loaded.local_module_prefixes:
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor implementation root changed after loading: {root}"
            )
        inject_deployment_path(root)
        importlib.invalidate_caches()
        _preload_attestor_namespace_packages(
            local_prefixes,
            implementation_root=root,
            class_ref=loaded.class_name,
        )
        result: CheckpointAttestationResult | None = None
        attestor_error: Exception | None = None
        try:
            result = loaded.attestor.attest(context)
        except Exception as exc:  # noqa: BLE001 - caller normalizes deployment code.
            attestor_error = exc
        _require_attestor_support_origins(
            local_prefixes,
            implementation_root=root,
            class_ref=loaded.class_name,
        )
        if attestor_error is not None:
            raise attestor_error
        assert result is not None
        return result


def canonicalize_json(value: Any, *, max_bytes: int | None = None) -> bytes:
    """Return RFC 8785 JCS bytes for the admitted integer-only I-JSON subset."""

    _validate_json_value(value, depth=0, budget=[MAX_TRAVERSAL_VALUES])
    encoded = _encode_json_value(value).encode("utf-8", errors="strict")
    if max_bytes is not None and len(encoded) > max_bytes:
        raise CheckpointAttestationFailure(
            attestor_id="publisher.attestations",
            code="attestation_limit_exceeded",
        )
    return encoded


def payload_digest(payload: Any) -> str:
    encoded = canonicalize_json(payload, max_bytes=MAX_PAYLOAD_BYTES)
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _validate_result(
    result: Any,
    *,
    expected_id: str,
) -> CheckpointAttestationResult:
    if (
        type(result) is not CheckpointAttestationResult
        or set(vars(result))
        != {"attestor_id", "schema", "status", "payload", "payload_digest"}
    ):
        raise CheckpointAttestationFailure(
            attestor_id=expected_id,
            code="attestation_invalid",
        )
    try:
        _validate_identifier(result.attestor_id, label="result attestor_id")
        _validate_identifier(result.schema, label="result schema")
    except CheckpointAttestationConfigurationError as exc:
        raise CheckpointAttestationFailure(
            attestor_id=expected_id,
            code="attestation_invalid",
        ) from exc
    if result.attestor_id != expected_id or result.status != "valid":
        raise CheckpointAttestationFailure(
            attestor_id=expected_id,
            code="attestation_invalid",
        )
    if not isinstance(result.payload_digest, str) or not _DIGEST_RE.fullmatch(
        result.payload_digest
    ):
        raise CheckpointAttestationFailure(
            attestor_id=expected_id,
            code="attestation_invalid",
        )
    try:
        canonical_payload = canonicalize_json(
            result.payload,
            max_bytes=MAX_PAYLOAD_BYTES,
        )
    except CheckpointAttestationFailure as exc:
        raise CheckpointAttestationFailure(
            attestor_id=expected_id,
            code=exc.code,
        ) from exc
    computed = f"sha256:{hashlib.sha256(canonical_payload).hexdigest()}"
    if computed != result.payload_digest:
        raise CheckpointAttestationFailure(
            attestor_id=expected_id,
            code="attestation_digest_mismatch",
        )
    return CheckpointAttestationResult(
        attestor_id=result.attestor_id,
        schema=result.schema,
        status=result.status,
        payload=json.loads(canonical_payload),
        payload_digest=result.payload_digest,
    )


def _validate_json_value(value: Any, *, depth: int, budget: list[int]) -> None:
    budget[0] -= 1
    if budget[0] < 0:
        raise CheckpointAttestationFailure(
            attestor_id="publisher.attestations",
            code="attestation_limit_exceeded",
        )
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        if value < _INT64_MIN or value > _INT64_MAX:
            raise CheckpointAttestationFailure(
                attestor_id="publisher.attestations",
                code="attestation_invalid",
            )
        return
    if isinstance(value, float):
        raise CheckpointAttestationFailure(
            attestor_id="publisher.attestations",
            code="attestation_invalid",
        )
    if isinstance(value, str):
        _validate_string(value)
        return
    if isinstance(value, list):
        collection_depth = depth + 1
        if collection_depth > MAX_JSON_DEPTH:
            raise CheckpointAttestationFailure(
                attestor_id="publisher.attestations",
                code="attestation_limit_exceeded",
            )
        if len(value) > MAX_COLLECTION_MEMBERS:
            raise CheckpointAttestationFailure(
                attestor_id="publisher.attestations",
                code="attestation_limit_exceeded",
            )
        for item in value:
            _validate_json_value(item, depth=collection_depth, budget=budget)
        return
    if isinstance(value, dict):
        collection_depth = depth + 1
        if collection_depth > MAX_JSON_DEPTH:
            raise CheckpointAttestationFailure(
                attestor_id="publisher.attestations",
                code="attestation_limit_exceeded",
            )
        if len(value) > MAX_COLLECTION_MEMBERS:
            raise CheckpointAttestationFailure(
                attestor_id="publisher.attestations",
                code="attestation_limit_exceeded",
            )
        for key, item in value.items():
            if not isinstance(key, str):
                raise CheckpointAttestationFailure(
                    attestor_id="publisher.attestations",
                    code="attestation_invalid",
            )
            _validate_string(key)
            _validate_json_value(item, depth=collection_depth, budget=budget)
        return
    raise CheckpointAttestationFailure(
        attestor_id="publisher.attestations",
        code="attestation_invalid",
    )


def _validate_string(value: str) -> None:
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise CheckpointAttestationFailure(
            attestor_id="publisher.attestations",
            code="attestation_invalid",
        ) from exc
    if unicodedata.normalize("NFC", value) != value:
        raise CheckpointAttestationFailure(
            attestor_id="publisher.attestations",
            code="attestation_invalid",
        )
    if len(encoded) > MAX_STRING_BYTES:
        raise CheckpointAttestationFailure(
            attestor_id="publisher.attestations",
            code="attestation_limit_exceeded",
        )


def _encode_json_value(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return _encode_json_string(value)
    if isinstance(value, list):
        return "[" + ",".join(_encode_json_value(item) for item in value) + "]"
    ordered = sorted(value, key=lambda key: key.encode("utf-16-be"))
    return "{" + ",".join(
        f"{_encode_json_string(key)}:{_encode_json_value(value[key])}"
        for key in ordered
    ) + "}"


def _encode_json_string(value: str) -> str:
    escaped: list[str] = ['"']
    short = {
        "\b": "\\b",
        "\t": "\\t",
        "\n": "\\n",
        "\f": "\\f",
        "\r": "\\r",
        '"': '\\"',
        "\\": "\\\\",
    }
    for char in value:
        replacement = short.get(char)
        if replacement is not None:
            escaped.append(replacement)
        elif ord(char) <= 0x1F:
            escaped.append(f"\\u{ord(char):04x}")
        else:
            escaped.append(char)
    escaped.append('"')
    return "".join(escaped)


def _load_attestor_class(
    class_ref: str,
    *,
    deployment_dir: Path,
    deployment_name: str,
) -> tuple[type[Any], str, tuple[str, ...]]:
    module_name, class_name = _split_attestor_class_ref(
        class_ref,
        deployment_name=deployment_name,
    )
    from okg.substrate.deployments import (
        deployment_module_import_lock,
        evict_deployment_modules,
        inject_deployment_path,
    )

    with deployment_module_import_lock():
        _preflight_attestor_module_path(
            module_name,
            implementation_root=deployment_dir,
            class_ref=class_ref,
        )
        local_prefixes = _prepare_attestor_implementation_import(
            deployment_dir,
            module_name=module_name,
            class_ref=class_ref,
        )
        evict_deployment_modules(
            deployment_dir,
            include_current=True,
            prefixes={"attestors", "checkpoint_attestors"},
        )
        inject_deployment_path(deployment_dir)
        importlib.invalidate_caches()
        try:
            _preload_attestor_namespace_packages(
                local_prefixes,
                implementation_root=deployment_dir,
                class_ref=class_ref,
            )
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
        except Exception as exc:
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor class {class_ref!r} is not importable"
            ) from exc
        try:
            _require_attestor_module_origin(
                module,
                implementation_root=deployment_dir,
                class_ref=class_ref,
            )
            class_module_name = getattr(cls, "__module__", None)
            class_module = (
                sys.modules.get(class_module_name)
                if isinstance(class_module_name, str)
                else None
            )
            _require_attestor_module_origin(
                class_module,
                implementation_root=deployment_dir,
                class_ref=class_ref,
            )
            _require_attestor_support_origins(
                local_prefixes,
                implementation_root=deployment_dir,
                class_ref=class_ref,
            )
            _ATTESTOR_IMPLEMENTATION_ROOTS.add(deployment_dir.resolve())
        except CheckpointAttestationConfigurationError:
            evict_deployment_modules(
                deployment_dir,
                include_current=True,
                prefixes={"attestors", "checkpoint_attestors"},
            )
            raise
    if not isinstance(cls, type):
        raise CheckpointAttestationConfigurationError(
            f"checkpoint attestor reference {class_ref!r} is not a class"
        )
    return cls, f"{cls.__module__}.{cls.__qualname__}", local_prefixes


def _prepare_attestor_implementation_import(
    implementation_root: Path,
    *,
    module_name: str,
    class_ref: str,
) -> tuple[str, ...]:
    root = implementation_root.resolve()
    prefixes = _attestor_local_module_prefixes(root)
    attestor_prefix = module_name.partition(".")[0]
    prior_roots = _ATTESTOR_IMPLEMENTATION_ROOTS - {root}
    _remove_attestor_roots_from_import_path(prior_roots)
    _evict_modules_from_attestor_roots(prior_roots)
    for name, module in tuple(sys.modules.items()):
        prefix = name.partition(".")[0]
        if prefix not in prefixes:
            continue
        origins = _attestor_module_origins(module)
        if origins and all(_path_is_within(origin, root=root) for origin in origins):
            continue
        candidates = {
            _attestor_root_for_origin(
                origin,
                module_prefix=prefix,
                attestor_prefix=attestor_prefix,
            )
            for origin in origins
        }
        if origins and None not in candidates:
            prior_roots.update(candidate for candidate in candidates if candidate)
            continue
        observed = ", ".join(str(origin) for origin in origins) or "<unknown>"
        raise CheckpointAttestationConfigurationError(
            f"checkpoint attestor class {class_ref!r} has deployment-local "
            f"support package {prefix!r} cached outside checkpoint attestor "
            f"implementation root {root}: {observed}"
        )
    _remove_attestor_roots_from_import_path(prior_roots)
    _evict_modules_from_attestor_roots(prior_roots)
    return prefixes


def _remove_attestor_roots_from_import_path(roots: set[Path]) -> None:
    if not roots:
        return
    retained: list[str] = []
    for entry in sys.path:
        try:
            resolved = Path(entry).resolve()
        except (OSError, TypeError):
            retained.append(entry)
            continue
        if resolved not in roots:
            retained.append(entry)
    sys.path[:] = retained


def _evict_modules_from_attestor_roots(roots: set[Path]) -> None:
    if not roots:
        return
    for name, module in tuple(sys.modules.items()):
        origins = _attestor_module_origins(module)
        if origins and all(
            any(_path_is_within(origin, root=root) for root in roots)
            for origin in origins
        ):
            sys.modules.pop(name, None)


def _attestor_root_for_origin(
    origin: Path,
    *,
    module_prefix: str,
    attestor_prefix: str,
) -> Path | None:
    for candidate in origin.parents:
        module_root = candidate / module_prefix
        module_file = candidate / f"{module_prefix}.py"
        if not (
            _path_is_within(origin, root=module_root)
            or origin == module_file.resolve()
        ):
            continue
        if _is_attestor_file_within(
            candidate / attestor_prefix / "__init__.py",
            root=candidate,
        ):
            return candidate
    return None


def _attestor_local_module_prefixes(root: Path) -> tuple[str, ...]:
    prefixes: set[str] = set()
    try:
        children = tuple(root.iterdir())
    except OSError as exc:
        raise CheckpointAttestationConfigurationError(
            "checkpoint attestor implementation root cannot be inspected: "
            f"{root}"
        ) from exc
    for child in children:
        if child.name.startswith(".") or child.name == "__pycache__":
            continue
        if child.is_dir() and child.name.isidentifier():
            if _is_attestor_file_within(child / "__init__.py", root=root) or any(
                path.is_file() for path in child.rglob("*.py")
            ):
                prefixes.add(child.name)
            continue
        if (
            child.is_file()
            and child.suffix == ".py"
            and child.stem != "__init__"
            and child.stem.isidentifier()
        ):
            prefixes.add(child.stem)
    return tuple(sorted(prefixes))


def _preload_attestor_namespace_packages(
    prefixes: Sequence[str],
    *,
    implementation_root: Path,
    class_ref: str,
) -> None:
    root = implementation_root.resolve()
    for prefix in prefixes:
        package_dir = root / prefix
        if not package_dir.is_dir() or (package_dir / "__init__.py").exists():
            continue
        try:
            importlib.import_module(prefix)
        except Exception as exc:
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor class {class_ref!r} has an invalid "
                f"deployment-local namespace package {prefix!r}"
            ) from exc
    _require_attestor_support_origins(
        prefixes,
        implementation_root=root,
        class_ref=class_ref,
    )


def _require_attestor_support_origins(
    prefixes: Sequence[str],
    *,
    implementation_root: Path,
    class_ref: str,
) -> None:
    root = implementation_root.resolve()
    for name, module in tuple(sys.modules.items()):
        if name.partition(".")[0] not in prefixes:
            continue
        origins = _attestor_module_origins(module)
        if origins and all(_path_is_within(origin, root=root) for origin in origins):
            continue
        observed = ", ".join(str(origin) for origin in origins) or "<unknown>"
        raise CheckpointAttestationConfigurationError(
            f"checkpoint attestor class {class_ref!r} loaded deployment-local "
            f"support outside checkpoint attestor implementation root {root}: "
            f"{name} ({observed})"
        )


def _attestor_module_origins(module: Any) -> tuple[Path, ...]:
    """Filesystem locations a module was loaded from.

    Only real path values count. Some modules in ``sys.modules`` answer any
    attribute with a placeholder object: torch's ``torch.classes`` returns a
    truthy, non-iterable ``_ClassNamespace`` for ``__file__`` and ``__path__``.
    Such a module has no filesystem origin, so it cannot lie inside an
    attestor implementation root.
    """
    origins: list[Path] = []
    file_attr = getattr(module, "__file__", None)
    if isinstance(file_attr, (str, os.PathLike)) and file_attr:
        origins.append(Path(file_attr).resolve())
    path_attr = getattr(module, "__path__", None)
    if isinstance(path_attr, Iterable) and not isinstance(path_attr, (str, bytes)):
        origins.extend(
            Path(raw).resolve()
            for raw in path_attr
            if isinstance(raw, (str, os.PathLike)) and raw
        )
    return tuple(origins)


def _path_is_within(path: Path, *, root: Path) -> bool:
    return path == root or root in path.parents


def _preflight_attestor_module_path(
    module_name: str,
    *,
    implementation_root: Path,
    class_ref: str,
) -> None:
    """Refuse an import before Python can search outside the selected root."""

    root = implementation_root.resolve()
    parts = module_name.split(".")
    if not parts or any(not part.isidentifier() for part in parts):
        raise CheckpointAttestationConfigurationError(
            f"checkpoint attestor class {class_ref!r} has an invalid module path"
        )
    current = root
    for index, part in enumerate(parts):
        package_dir = current / part
        package_init = package_dir / "__init__.py"
        module_file = current / f"{part}.py"
        is_final = index == len(parts) - 1
        if _is_attestor_file_within(package_init, root=root):
            current = package_dir
            continue
        if is_final and _is_attestor_file_within(module_file, root=root):
            return
        raise CheckpointAttestationConfigurationError(
            f"checkpoint attestor class {class_ref!r} has no regular local "
            f"module beneath checkpoint attestor implementation root {root}"
        )


def _is_attestor_file_within(path: Path, *, root: Path) -> bool:
    try:
        resolved = path.resolve(strict=True)
    except OSError:
        return False
    return resolved.is_file() and (resolved == root or root in resolved.parents)


def _require_attestor_module_origin(
    module: Any,
    *,
    implementation_root: Path,
    class_ref: str,
) -> None:
    """Prove imported code belongs to the selected implementation root."""

    root = implementation_root.resolve()
    origins: list[Path] = []
    file_attr = getattr(module, "__file__", None)
    if file_attr:
        origins.append(Path(file_attr).resolve())
    path_attr = getattr(module, "__path__", None)
    if path_attr:
        origins.extend(Path(raw).resolve() for raw in path_attr)
    if not origins or any(
        origin != root and root not in origin.parents for origin in origins
    ):
        observed = ", ".join(str(origin) for origin in origins) or "<unknown>"
        raise CheckpointAttestationConfigurationError(
            f"checkpoint attestor class {class_ref!r} resolved outside "
            f"checkpoint attestor implementation root {root}: {observed}"
        )


def _split_attestor_class_ref(
    class_ref: str,
    *,
    deployment_name: str,
) -> tuple[str, str]:
    module_name, separator, class_name = class_ref.partition(":")
    if not separator:
        module_name, separator, class_name = class_ref.rpartition(".")
    if not module_name or not class_name:
        raise CheckpointAttestationConfigurationError(
            "checkpoint attestor class must be module.Class or module:Class"
        )
    root_package = module_name.split(".", 1)[0]
    if root_package not in {"attestors", "checkpoint_attestors"}:
        raise CheckpointAttestationConfigurationError(
            "checkpoint attestor module must use the deployment-local attestors "
            "or checkpoint_attestors package"
        )
    return module_name, class_name


def _validate_config_schema(schema: Any, *, fqcn: str) -> None:
    if not isinstance(schema, dict):
        raise CheckpointAttestationConfigurationError(
            f"checkpoint attestor {fqcn!r} must declare config_schema"
        )
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        raise CheckpointAttestationConfigurationError(
            f"checkpoint attestor {fqcn!r} config_schema must be a closed object"
        )
    _validate_closed_config_schema_node(schema, fqcn=fqcn, path="config_schema")
    try:
        Draft202012Validator.check_schema(schema)
    except Exception as exc:
        raise CheckpointAttestationConfigurationError(
            f"checkpoint attestor {fqcn!r} config_schema is invalid"
        ) from exc


def _validate_closed_config_schema_node(
    schema: Mapping[str, Any],
    *,
    fqcn: str,
    path: str,
) -> None:
    declared_type = schema.get("type")
    describes_object = declared_type == "object" or (
        isinstance(declared_type, list) and "object" in declared_type
    )
    if describes_object:
        properties = schema.get("properties")
        if not isinstance(properties, dict):
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor {fqcn!r} {path}.properties must be a mapping"
            )
        if schema.get("additionalProperties") is not False:
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor {fqcn!r} {path} must be a closed object"
            )
        required = schema.get("required")
        if (
            not isinstance(required, list)
            or any(not isinstance(key, str) for key in required)
            or len(set(required)) != len(required)
            or not set(required).issubset(properties)
        ):
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor {fqcn!r} {path} must declare a unique "
                "required list containing only declared properties"
            )
        undeclared_types = sorted(
            str(key)
            for key, property_schema in properties.items()
            if not isinstance(key, str)
            or not isinstance(property_schema, dict)
            or "type" not in property_schema
        )
        if undeclared_types:
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor {fqcn!r} {path} properties must declare "
                "types: " + ", ".join(undeclared_types)
            )
        if schema.get("patternProperties"):
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor {fqcn!r} {path} must declare fixed keys "
                "through properties"
            )
        reserved = sorted(
            set(str(key).lower() for key in properties) & _RESERVED_CONFIG_KEYS
        )
        if reserved:
            raise CheckpointAttestationConfigurationError(
                f"checkpoint attestor {fqcn!r} {path} declares reserved "
                "publisher authority: " + ", ".join(reserved)
            )

    for key, child in schema.items():
        if key in {"properties", "$defs", "dependentSchemas"}:
            if isinstance(child, dict):
                for child_key, child_schema in child.items():
                    if isinstance(child_schema, dict):
                        _validate_closed_config_schema_node(
                            child_schema,
                            fqcn=fqcn,
                            path=f"{path}.{key}.{child_key}",
                        )
        elif key in {"items", "not", "if", "then", "else"}:
            if isinstance(child, dict):
                _validate_closed_config_schema_node(
                    child, fqcn=fqcn, path=f"{path}.{key}"
                )
        elif key in {"prefixItems", "allOf", "anyOf", "oneOf"}:
            if isinstance(child, list):
                for index, child_schema in enumerate(child):
                    if isinstance(child_schema, dict):
                        _validate_closed_config_schema_node(
                            child_schema,
                            fqcn=fqcn,
                            path=f"{path}.{key}[{index}]",
                        )


def _validate_identifier(value: Any, *, label: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) > 128
        or not _IDENTIFIER_RE.fullmatch(value)
    ):
        raise CheckpointAttestationConfigurationError(
            f"{label} must be a lowercase dotted identifier"
        )


def _reject_placeholders(value: Any, *, path: str) -> None:
    if isinstance(value, str) and _ENV_PLACEHOLDER_RE.search(value):
        raise CheckpointAttestationConfigurationError(
            f"{path} must not contain environment substitutions"
        )
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str) and _ENV_PLACEHOLDER_RE.search(key):
                raise CheckpointAttestationConfigurationError(
                    f"{path} keys must not contain environment substitutions"
                )
            _reject_placeholders(item, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_placeholders(item, path=f"{path}[{index}]")


def _reject_reserved_config(value: Any, *, path: str) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() in _RESERVED_CONFIG_KEYS:
                raise CheckpointAttestationConfigurationError(
                    f"{path}.{key} attempts to override publisher authority"
                )
            _reject_reserved_config(item, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_reserved_config(item, path=f"{path}[{index}]")


def _complete_bounded_rows(
    rows: Sequence[Mapping[str, Any]],
    limit: int,
    *,
    label: str,
) -> tuple[dict[str, Any], ...]:
    if len(rows) > limit:
        raise CheckpointAttestationRefused(
            "attestation_unavailable",
            f"{label} exceed the requested complete-read limit",
        )
    return tuple(_json_safe_row(row) for row in rows)


def _candidate_page(
    rows: Sequence[Mapping[str, Any]],
    limit: int,
    *,
    cursor_key: str,
    after_cursor: str | None,
    label: str,
) -> CheckpointCandidatePage:
    normalized = tuple(_json_safe_row(row) for row in rows[: limit + 1])
    cursor_values: list[str] = []
    for row in normalized:
        value = row.get(cursor_key)
        if not isinstance(value, str) or not value:
            raise CheckpointAttestationRefused(
                "attestation_unavailable",
                f"{label} contain an invalid pagination identity",
            )
        cursor_values.append(value)
    previous = after_cursor
    for value in cursor_values:
        if previous is not None and value <= previous:
            raise CheckpointAttestationRefused(
                "attestation_unavailable",
                f"{label} are not in strict pagination order",
            )
        previous = value
    complete = len(normalized) <= limit
    page_rows = normalized[:limit]
    return CheckpointCandidatePage(
        rows=page_rows,
        next_cursor=None if complete else cursor_values[limit - 1],
        complete=complete,
    )


def _json_safe_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in row.items():
        if hasattr(value, "isoformat") and callable(value.isoformat):
            normalized[str(key)] = value.isoformat()
        else:
            normalized[str(key)] = value
    return normalized


def _bounded_public_detail(detail: str | None) -> str | None:
    if detail is None:
        return None
    if not isinstance(detail, str):
        raise ValueError("checkpoint attestation refusal detail must be a string")
    cleaned = " ".join(detail.split())
    return cleaned[:256] or None


__all__ = [
    "ATTESTATION_ENVELOPE_SCHEMA",
    "CheckpointAttestationConfigurationError",
    "CheckpointAttestationContext",
    "CheckpointAttestationFailure",
    "CheckpointAttestationRefused",
    "CheckpointAttestationResult",
    "CheckpointAttestor",
    "CheckpointCandidatePage",
    "LoadedCheckpointAttestor",
    "canonicalize_json",
    "checkpoint_attestation_context",
    "load_checkpoint_attestors",
    "payload_digest",
    "run_checkpoint_attestors",
    "validate_loaded_checkpoint_attestors",
]
