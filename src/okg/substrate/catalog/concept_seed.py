"""Catalog loader for operator-authored Concept seeds.

A deployment that imports the substrate ``concepts``
ontology module ships its concept seed list at
``<deployment_dir>/concepts/seed.yaml``. Each entry has a
canonical ``name`` (short label) and a ``description``
(2-4 sentence natural-language definition). The DBOS
``okg_embed_batch`` workflow populates each Concept's ``embedding`` from
canonical ``attrs.text``, stamped from the searchable description field.

Per ``add-substrate-semantic-concept-linking``. Replaces the
retired regex-pattern loader (``concept_patterns.py``).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import psycopg
from okg.substrate import fastyaml
from psycopg.pq import TransactionStatus

from okg.substrate.errors import SchemaValidationFailed


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slug(name: str) -> str:
    s = _SLUG_RE.sub("_", name.lower()).strip("_")
    if not s:
        raise SchemaValidationFailed(
            f"concept name {name!r} produces an empty slug",
            data={"name": name},
        )
    return s


def load_seed_path(deployment_dir: Path) -> Path:
    return Path(deployment_dir) / "concepts" / "seed.yaml"


def has_seed(deployment_dir: Path) -> bool:
    return load_seed_path(deployment_dir).is_file()


def parse_seed(deployment_dir: Path) -> list[dict[str, Any]]:
    """Read + validate ``<deployment>/concepts/seed.yaml``.

    Returns one dict per concept with keys
    ``{node_id, name, description}``.

    Raises ``SchemaValidationFailed`` on missing top-level
    ``concepts:`` key, missing required field on an entry,
    duplicate canonical names, or empty description.
    """
    path = load_seed_path(deployment_dir)
    if not path.is_file():
        raise SchemaValidationFailed(
            f"concepts/seed.yaml not found: {path}",
            data={"path": str(path)},
            fix_hint=(
                "create the file at "
                "<deployment>/concepts/seed.yaml or remove the "
                "concepts module / SemanticConceptLinker enricher "
                "registration"
            ),
        )

    with path.open() as f:
        doc = fastyaml.safe_load(f) or {}

    if "concepts" not in doc:
        raise SchemaValidationFailed(
            f"{path}: missing top-level 'concepts:' key",
            data={"path": str(path)},
            fix_hint="wrap entries under a top-level `concepts:` list",
        )
    raw = doc["concepts"]
    if not isinstance(raw, list):
        raise SchemaValidationFailed(
            f"{path}: 'concepts' must be a list",
            data={"path": str(path)},
        )

    seen_names: set[str] = set()
    out: list[dict[str, Any]] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise SchemaValidationFailed(
                f"{path}: concepts[{i}] must be a mapping",
                data={"path": str(path), "index": i},
            )
        name_raw = entry.get("name")
        if not isinstance(name_raw, str) or not name_raw.strip():
            raise SchemaValidationFailed(
                f"{path}: concepts[{i}] missing or empty 'name'",
                data={"path": str(path), "index": i},
            )
        desc_raw = entry.get("description")
        if not isinstance(desc_raw, str) or not desc_raw.strip():
            raise SchemaValidationFailed(
                f"{path}: concepts[{i}] '{name_raw}' missing or "
                f"empty 'description'",
                data={"path": str(path), "index": i, "name": name_raw},
            )

        name = name_raw.strip().lower()
        if name in seen_names:
            raise SchemaValidationFailed(
                f"{path}: duplicate concept name {name!r}",
                data={"path": str(path), "name": name},
            )
        seen_names.add(name)

        slug = _slug(name)
        out.append({
            "node_id": f"concept:{slug}",
            "name": name,
            "description": desc_raw.strip(),
        })

    if not out:
        raise SchemaValidationFailed(
            f"{path}: no concepts loaded — empty list",
            data={"path": str(path)},
        )

    return out


def upsert_concept_nodes(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int,
    generation_id: int = 0,
    deployment_name: str = "default",
    seeds: list[dict[str, Any]],
) -> int:
    """Upsert one Concept node per seed entry into the Chronos graph.

    Idempotent on ``node_id``; re-running against the same seed
    list is a no-op (modulo description edits that produce a new
    attrs blob — Chronos upsert is last-write-wins). The DBOS
    ``okg_embed_batch`` workflow populates each Concept's ``embedding`` from
    canonical ``attrs.text``, stamped from the searchable ``description``.

    Concept entries are operator-authored config rather than
    source-derived facts from a publish cycle; they are written
    directly to the branch-visible graph via ``apply_facts``.
    """
    from okg.substrate.fact_application import apply_bounded_fact_batch
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    if not seeds:
        return 0

    facts = [
        NodeFact(
            seed["node_id"],
            "concept",
            {
                "name": seed["name"],
                "description": seed["description"],
                "text": seed["description"],
            },
            {"node_id": seed["node_id"]},
            {"catalog_version_id": catalog_version_id},
        )
        for seed in seeds
    ]
    store = ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=bool(
            conn.autocommit
            and conn.info.transaction_status == TransactionStatus.IDLE
        ),
        catalog_version_id=catalog_version_id,
    )
    result = apply_bounded_fact_batch(
        conn,
        store,
        facts,
        deployment_name=deployment_name,
        producer_id="_catalog:concept_seed",
        producer_kind="system",
        intent="catalog_concept_seed",
        catalog_version_id=catalog_version_id,
        authority_scope={
            "catalog_version_id": catalog_version_id,
            "generation_id": generation_id,
            "subtype": "concept",
        },
        source="_catalog:concept_seed",
        strict_endpoints=True,
    )
    return result.primitive_total("nodes_upserted")


__all__ = [
    "parse_seed",
    "load_seed_path",
    "has_seed",
    "upsert_concept_nodes",
]
