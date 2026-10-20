"""Sealed answer-key artifacts: canonical JSON, ids, schemas, writers.

Determinism contract (req.bench.determinism / req.bench.manifest):

- every artifact byte derives from pinned inputs through canonical JSON
  (sorted keys, fixed separators, no wall clock);
- ``mutation_id`` hashes (tree digest, seed, operator id+version, site id,
  params), so distinct seeds yield disjoint id sets by construction;
- ids live in a reserved namespace (``okgmut_`` / ``okgfam_``) guaranteed
  absent from natural corpus text — the leakage byte-scan keys on it;
- ``benchmark-manifest.json`` is SEALED at injection time: its schema
  forbids run outcomes (generation ids, timestamps, latencies), which bind
  later in the separate volatile ``run-binding.json``.

Schemas are versioned in-code (``$id`` pins the version) and validated with
``jsonschema`` on every emitted row — a malformed row is a loud failure.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

import jsonschema

from okg.bench.injection.types import (
    HARDNESS_TIERS,
    REJECTION_REASONS,
    InjectionError,
)

MUTATION_ID_PREFIX = "okgmut_"
FAMILY_ID_PREFIX = "okgfam_"
CANARY_PREFIX = "okgcanary_"

MUTATIONS_FILENAME = "mutations.jsonl"
REJECTED_FILENAME = "rejected_sites.jsonl"
MANIFEST_FILENAME = "benchmark-manifest.json"
MUTATED_CORPUS_DIRNAME = "corpus"


def canonical_json_bytes(obj: Any) -> bytes:
    """Canonical JSON encoding — the only serialization artifacts use."""
    try:
        text = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise InjectionError(f"value is not canonically JSON-serializable: {exc}") from exc
    return (text + "\n").encode("utf-8")


def _hex(material: bytes) -> str:
    return hashlib.sha256(material).hexdigest()


def compute_config_digest(config: Mapping[str, Any]) -> str:
    return _hex(canonical_json_bytes(dict(config)))


def compute_mutation_id(
    *,
    tree_digest: str,
    seed: int,
    operator_id: str,
    operator_version: str,
    site_id: str,
    params: Mapping[str, Any],
) -> str:
    material = canonical_json_bytes(
        [tree_digest, seed, operator_id, operator_version, site_id, dict(params)]
    )
    return MUTATION_ID_PREFIX + _hex(material)[:32]


def compute_family_id(*, tree_digest: str, seed: int, family_key: str) -> str:
    material = canonical_json_bytes([tree_digest, seed, family_key])
    return FAMILY_ID_PREFIX + _hex(material)[:32]


def compute_canary(*, tree_digest: str, seed: int) -> str:
    """The manifest-only canary token the leakage scan hunts for."""
    material = canonical_json_bytes([tree_digest, seed, "leakage-canary"])
    return CANARY_PREFIX + _hex(material)[:32]


_SPAN_SCHEMA = {
    "type": "object",
    "properties": {
        "start": {"type": "integer", "minimum": 0},
        "end": {"type": "integer", "minimum": 0},
    },
    "required": ["start", "end"],
    "additionalProperties": False,
}

_OPERATOR_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "string", "minLength": 1},
        "version": {"type": "string", "minLength": 1},
    },
    "required": ["id", "version"],
    "additionalProperties": False,
}

MUTATION_ROW_SCHEMA_V1: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "okg.injection.mutation-row.v1",
    "type": "object",
    "properties": {
        "mutation_id": {"type": "string", "pattern": "^okgmut_[0-9a-f]{32}$"},
        "family_id": {"type": "string", "pattern": "^okgfam_[0-9a-f]{32}$"},
        "mode": {"enum": ["graph", "independent"]},
        "doc": {"type": "string", "minLength": 1},
        "span": _SPAN_SCHEMA,
        "pre_text": {"type": "string"},
        "post_text": {"type": "string"},
        "operator": _OPERATOR_SCHEMA,
        "params": {"type": "object"},
        "facets": {
            "type": "object",
            "properties": {
                "relation": {"type": "string", "minLength": 1},
                "object": {"type": "string", "minLength": 1},
                "scope": {"type": "string", "minLength": 1},
                "semantic_control": {"type": "string", "minLength": 1},
                "consequence": {"type": "string", "minLength": 1},
            },
            "required": ["relation", "object", "scope", "semantic_control", "consequence"],
            "additionalProperties": False,
        },
        "hardness_tier": {"enum": list(HARDNESS_TIERS)},
        "expected": {
            "type": "object",
            "properties": {"kind": {"type": "string", "minLength": 1}},
            "required": ["kind"],
        },
        "inverse": {
            "type": "object",
            "properties": {"span": _SPAN_SCHEMA, "text": {"type": "string"}},
            "required": ["span", "text"],
            "additionalProperties": False,
        },
    },
    "required": [
        "mutation_id",
        "family_id",
        "mode",
        "doc",
        "span",
        "pre_text",
        "post_text",
        "operator",
        "params",
        "facets",
        "hardness_tier",
        "expected",
        "inverse",
    ],
    "additionalProperties": False,
}

REJECTED_ROW_SCHEMA_V1: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "okg.injection.rejected-row.v1",
    "type": "object",
    "properties": {
        "site_id": {"type": "string", "minLength": 1},
        "operator": _OPERATOR_SCHEMA,
        "doc": {"type": "string", "minLength": 1},
        "reason": {"enum": list(REJECTION_REASONS)},
        "detail": {"type": "string"},
    },
    "required": ["site_id", "operator", "doc", "reason", "detail"],
    "additionalProperties": False,
}

# Sealed by construction: additionalProperties=false forbids run outcomes
# (generation ids of the mutated run, timestamps, latencies) structurally.
BENCHMARK_MANIFEST_SCHEMA_V1: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "okg.injection.benchmark-manifest.v1",
    "type": "object",
    "properties": {
        "schema": {"const": "okg.injection.benchmark-manifest.v1"},
        "corpus": {
            "type": "object",
            "properties": {
                "tree_digest": {"type": "string", "minLength": 1},
                "file_count": {"type": "integer", "minimum": 1},
                "files": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "prefixItems": [{"type": "string"}, {"type": "string"}],
                        "minItems": 2,
                        "maxItems": 2,
                    },
                },
            },
            "required": ["tree_digest", "file_count", "files"],
            "additionalProperties": False,
        },
        "seed": {"type": "integer"},
        "canary": {"type": "string", "pattern": "^okgcanary_[0-9a-f]{32}$"},
        "operators": {"type": "array", "items": _OPERATOR_SCHEMA},
        "config_digest": {"type": "string", "minLength": 1},
        "targeting": {
            "type": "object",
            "properties": {
                "claims_digest": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                "modes": {"type": "array", "items": {"enum": ["graph", "independent"]}},
            },
            "required": ["claims_digest", "modes"],
            "additionalProperties": False,
        },
        "counts": {
            "type": "object",
            "properties": {
                "mutations": {"type": "integer", "minimum": 0},
                "rejected_sites": {"type": "integer", "minimum": 0},
                "candidate_sites": {"type": "integer", "minimum": 0},
            },
            "required": ["mutations", "rejected_sites", "candidate_sites"],
            "additionalProperties": False,
        },
    },
    "required": [
        "schema",
        "corpus",
        "seed",
        "canary",
        "operators",
        "config_digest",
        "targeting",
        "counts",
    ],
    "additionalProperties": False,
}


def validate_row(row: Mapping[str, Any], schema: Mapping[str, Any]) -> None:
    """Validate one artifact row; raise loudly with the schema id on failure."""
    try:
        jsonschema.validate(instance=dict(row), schema=dict(schema))
    except jsonschema.ValidationError as exc:
        raise InjectionError(
            f"artifact row violates {schema.get('$id', '<schema>')}: {exc.message}"
        ) from exc


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]], schema: Mapping[str, Any]) -> None:
    lines: list[bytes] = []
    for row in rows:
        validate_row(row, schema)
        lines.append(canonical_json_bytes(row))
    path.write_bytes(b"".join(lines))


def write_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    validate_row(manifest, BENCHMARK_MANIFEST_SCHEMA_V1)
    path.write_bytes(canonical_json_bytes(manifest))
