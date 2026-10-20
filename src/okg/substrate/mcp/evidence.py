"""Metadata-driven evidence classification for agent-facing search rows.

Evidence identity is deployment data, not substrate vocabulary.  The catalog
may declare a direct ``evidence_kind`` or a mapping from an attribute (for
example, a deployment's ``chunk_kind``) to the stable read-side vocabulary.
Rows without a justified mapping remain explicitly ``unknown``.
"""
from __future__ import annotations

import re
from typing import Any, Mapping

import psycopg
from psycopg.rows import dict_row

from okg.substrate.mcp.session import Session


UNKNOWN_EVIDENCE_KIND = "unknown"
_TOKEN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


def _token(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip().lower()
    return value if _TOKEN.fullmatch(value) else None


def _bounded(value: Any, *, limit: int = 128) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value[:limit] if value else None


def _metadata_for_rows(
    conn: psycopg.Connection,
    session: Session,
    rows: list[dict[str, Any]],
    *,
    metadata: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    subtypes = sorted({
        str(row.get("subtype"))
        for row in rows
        if row.get("subtype") is not None
    })
    known = {name: dict(meta) for name, meta in (metadata or {}).items()}
    missing = [name for name in subtypes if name not in known]
    if not missing:
        return known
    # One catalog read per result page, regardless of hit count.  The subtype
    # set is both bounded by the page size and generation-pinned by the session.
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT name, metadata
              FROM okg.subtypes
             WHERE catalog_version_id = %(catalog_version_id)s
               AND name = ANY(%(subtypes)s)
            """,
            {
                "catalog_version_id": session.catalog_version_id,
                "subtypes": missing,
            },
        )
        found = {
            str(row["name"]): dict(row.get("metadata") or {})
            for row in cur.fetchall()
            if row.get("name") is not None
        }
    # An absent catalog row was requested and is known absent. Retaining its
    # empty entry distinguishes that result from a type not yet requested.
    return {**known, **{name: {} for name in missing}, **found}


def _classify_row(
    row: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    attrs = row.get("attrs")
    attrs = attrs if isinstance(attrs, dict) else {}
    subtype = _bounded(row.get("subtype")) or "unknown"
    source = _bounded(row.get("source"))
    # Keep the deployment discriminator verbatim (but bounded) even when it
    # is not one of the catalog's mapped values. Unknown raw data is useful to
    # callers and must not be silently erased or interpreted as a guess.
    raw_kind = _bounded(attrs.get("chunk_kind"), limit=64)

    kind = _token(attrs.get("evidence_kind"))
    basis: dict[str, Any] | None = None
    if kind:
        basis = {"basis": "attrs.evidence_kind"}
    else:
        by_attr = metadata.get("evidence_kind_by_attr")
        if isinstance(by_attr, dict):
            for attr_name, mapping in by_attr.items():
                if not isinstance(attr_name, str) or not isinstance(mapping, dict):
                    continue
                raw_value = attrs.get(attr_name)
                mapped = None
                if isinstance(raw_value, (str, int, float, bool)):
                    mapped = _token(mapping.get(raw_value))
                    if mapped is None:
                        mapped = _token(mapping.get(str(raw_value)))
                if mapped:
                    kind = mapped
                    basis = {
                        "basis": "ontology.evidence_kind_by_attr",
                        "field": attr_name[:64],
                        "value": _bounded(raw_value),
                    }
                    break
    if not kind:
        kind = _token(metadata.get("evidence_kind"))
        if kind:
            basis = {"basis": "ontology.evidence_kind"}
    provenance_field = (
        "provenance"
        if attrs.get("provenance") is not None
        else "_provenance"
    )
    if not kind and attrs.get(provenance_field) == "extracted_model":
        # This is a generic substrate provenance enum, not a domain vocabulary.
        kind = "model_generated_inference"
        basis = {
            "basis": f"attrs.{provenance_field}",
            "value": "extracted_model",
        }
    if not kind:
        kind = UNKNOWN_EVIDENCE_KIND
        basis = {"basis": "unclassified"}

    assert basis is not None
    provenance: dict[str, Any] = {
        **basis,
        "subtype": subtype,
    }
    if source:
        provenance["source"] = source
    if raw_kind:
        provenance["raw_discriminator"] = raw_kind
    out = dict(row)
    out["evidence_kind"] = kind
    out["evidence_kind_provenance"] = provenance
    if raw_kind:
        out["chunk_kind"] = raw_kind
    return out


def classify_evidence_rows(
    conn: psycopg.Connection,
    session: Session,
    rows: list[dict[str, Any]],
    *,
    metadata: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Attach bounded evidence fields to a page of node-shaped rows."""
    if not rows:
        return rows
    metadata = _metadata_for_rows(conn, session, rows, metadata=metadata)
    return [
        _classify_row(row, metadata.get(str(row.get("subtype")), {}))
        for row in rows
    ]


def curated_alias_evidence(*, alias: Any) -> dict[str, Any]:
    """Return the stable evidence contract for an outer alias result."""
    provenance: dict[str, Any] = {
        "basis": "alias_catalog",
        "source": "alias_catalog",
    }
    bounded_alias = _bounded(alias)
    if bounded_alias:
        provenance["alias"] = bounded_alias
    return {
        "evidence_kind": "curated_alias",
        "evidence_kind_provenance": provenance,
    }


__all__ = [
    "UNKNOWN_EVIDENCE_KIND",
    "classify_evidence_rows",
    "curated_alias_evidence",
]
