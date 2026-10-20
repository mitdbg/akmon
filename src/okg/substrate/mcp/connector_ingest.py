"""Connector-route status + ingest tools for trusted-source records.

Extracted from ``mcp/source_admin.py`` by
``extract-mcp-connector-ingest`` (2026-05).

Two MCP tools live here:
  - ``connector_route_status(conn, deployment, source)`` — report
    readiness (which allowlists are missing / which routes are
    trusted).
  - ``connector_ingest_batch(conn, source, route_id, records,
    provenance=None)`` — accept a batch of normalized records from
    a trusted connector route and turn them into NodeFacts +
    EdgeFacts via COPY.

Plus their five private helpers (``_stable_hash``,
``_normalize_connector_record``, ``_missing_connector_allowlists``,
``_connector_fact_rows``, ``_copy_connector_rows``).

``mcp/source_admin.py`` re-exports the two public tools so
``server.py``'s thin wrappers (which call
``_source_admin.connector_route_status(...)`` /
``_source_admin.connector_ingest_batch(...)``) keep working
without an MCP server-build change.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg

from okg.substrate.images.values import (
    ImageRefInvalid,
    ImageValueForged,
    check_incoming,
)
from okg.substrate.mcp.source_catalog import (
    SOURCE_CATALOG,
    _CREDENTIAL_SOURCE_IDS,
)


_LEGACY_CONNECTOR_COMPATIBILITY = {
    "authority": "registered_route_and_allowlist",
    "owner": "build-akmon-connector-suite",
    "exit_condition": (
        "retire connector_ingest_batch after ProviderConnection and "
        "SourceBinding route these providers through SourceRunner"
    ),
}

_LEGACY_CONNECTOR_OUTPUTS = {
    "gmail_api": {
        "nodes": ("email", "email_thread"),
        "edges": (("email_thread", "contains", "email"),),
    },
    "google_calendar_api": {
        "nodes": ("calendar_feed", "calendar_event"),
        "edges": (("calendar_event", "at_feed", "calendar_feed"),),
    },
    "slack_api": {
        "nodes": ("chat_channel", "chat_message"),
        "edges": (("chat_channel", "contains", "chat_message"),),
    },
}


#: Per-record payload bounds (in addition to the 500-record batch cap) so a
#: connector record cannot drive unbounded memory/CPU (harden-mcp-write-boundaries).
_MAX_RECORD_BYTES = 256 * 1024
_MAX_RECORD_FIELDS = 250
_MAX_RECORD_DEPTH = 8


def _json_depth(value: Any, _depth: int = 0) -> int:
    if isinstance(value, dict):
        return max((_json_depth(v, _depth + 1) for v in value.values()), default=_depth)
    if isinstance(value, list):
        return max((_json_depth(v, _depth + 1) for v in value), default=_depth)
    return _depth


def _field_count(value: Any) -> int:
    if isinstance(value, dict):
        return len(value) + sum(_field_count(v) for v in value.values())
    if isinstance(value, list):
        return sum(_field_count(v) for v in value)
    return 0


def _record_shape_violation(record: dict) -> Optional[str]:
    """Return a violation reason if the record exceeds a payload bound, else None."""
    try:
        raw = json.dumps(record, default=str)
    except (TypeError, ValueError):
        return "record_unserializable"
    if len(raw.encode("utf-8")) > _MAX_RECORD_BYTES:
        return "record_too_large_bytes"
    if _field_count(record) > _MAX_RECORD_FIELDS:
        return "record_too_many_fields"
    if _json_depth(record) > _MAX_RECORD_DEPTH:
        return "record_too_deep"
    return None


def _assert_record_in_allowlist(
    source_id: str, record: dict, allowlist: dict,
) -> None:
    """Raise ValueError unless the record's labels/calendar/channel are within
    the route's provenance allowlist (per-record membership, not mere presence).
    """
    if source_id == "gmail_api":
        allowed = set(allowlist.get("labels") or [])
        record_labels = {str(lbl) for lbl in (record.get("labels") or [])}
        # Per-record membership, like calendar/slack: the record must NAME an
        # allowlisted label. A record with no labels (or labels disjoint from the
        # allowlist) is refused — otherwise an agent ingests arbitrary email
        # content by simply omitting `labels` (harden-mcp-write-boundaries;
        # adversarial-review remediation).
        if allowed and not (record_labels & allowed):
            raise ValueError(
                "gmail record carries no label within the route allowlist "
                f"(labels={sorted(record_labels)}, allowlist={sorted(allowed)}); "
                "a record must name an allowlisted label to be ingested"
            )
    elif source_id == "google_calendar_api":
        allowed = set(allowlist.get("calendars") or [])
        cal = str(record.get("calendar_id") or record.get("feed_id") or "primary")
        if allowed and cal not in allowed:
            raise ValueError(
                f"calendar {cal!r} is outside the route allowlist"
            )
    elif source_id == "slack_api":
        allowed = set(allowlist.get("channels") or [])
        ch = str(record.get("channel_id") or record.get("channel") or "")
        if allowed and ch not in allowed:
            raise ValueError(
                f"channel {ch!r} is outside the route allowlist"
            )


def connector_route_status(
    conn: psycopg.Connection,
    deployment: str,
    source: str,
) -> dict:
    """Report connector readiness for one credentialed source."""
    from okg.substrate.mcp.source_admin import (  # deferred to break circular import
        _credentialed_connector_row,
        discover_sources,
    )
    target_source_id = SOURCE_CATALOG.get(source, (source, ""))[0]
    discovery = discover_sources(conn, deployment)
    if discovery.get("status") != "ok":
        return discovery
    for entry in discovery.get("candidates") or []:
        if entry.get("source_id") == target_source_id or entry.get("short") == source:
            if entry.get("source_id") not in _CREDENTIAL_SOURCE_IDS:
                return {
                    "status": "not_connector_source",
                    "source_id": entry.get("source_id"),
                    "short": entry.get("short"),
                    "summary": f"`{source}` is not a connector-backed source.",
                }
            row = _credentialed_connector_row(entry)
            return {
                "status": row["setup_state"],
                "deployment": deployment,
                **row,
                "act_routes": _act_routes_for_source(
                    entry.get("source_id") or "",
                ),
            }
    return {
        "status": "unknown_source",
        "source": source,
        "valid_short_names": sorted(SOURCE_CATALOG.keys()),
    }


# Which vetted act route serves each ingest source's provider
# (adopt-act-mcp-routes). Reported on connector_route_status so the
# operator sees the act surface's pinned modes, custody, and
# approval state next to the ingest route.
_ACT_ROUTE_BY_SOURCE_ID = {
    "gmail_api": "google_workspace",
    "google_calendar_api": "google_workspace",
    "outlook_mail_api": "ms365",
    "outlook_calendar_api": "ms365",
}


def _approved_route_ids(source_id: str) -> set[str]:
    """Approved connector route_ids for a credentialed source. The canonical
    host-connector route is `<source_id>:host_connector`; the vetted act route
    name (and its qualified form) is also accepted. A caller-supplied route_id
    outside this set is rejected before ingest (harden-mcp-write-boundaries)."""
    routes = {f"{source_id}:host_connector"}
    act = _ACT_ROUTE_BY_SOURCE_ID.get(source_id)
    if act:
        routes.add(act)
        routes.add(f"{source_id}:{act}")
    return routes


def _act_routes_for_source(source_id: str) -> list[dict]:
    from okg.substrate.library.sources.discovery.act_routes import (
        VETTED_ACT_ROUTES,
    )
    name = _ACT_ROUTE_BY_SOURCE_ID.get(source_id)
    if name is None:
        return []
    route = VETTED_ACT_ROUTES[name]
    return [{
        **route.to_dict(),
        # Approval is per-activation operator state; there is no
        # persisted approval yet, so the surface reports the gate
        # honestly rather than implying activation.
        "operator_approved": False,
        "activation_state": "available_not_activated",
    }]


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8"),
    ).hexdigest()


def _normalize_connector_record(
    *,
    source_id: str,
    route_id: str,
    record: dict,
) -> tuple[list[dict], list[dict]]:
    """Map a normalized connector record to node/edge fact dicts.

    Connector content is treated as inert data. Text fields are copied
    into attrs; they are never interpreted as setup instructions.
    """
    kind = str(record.get("kind") or record.get("type") or "").lower()
    external_id = str(
        record.get("external_id")
        or record.get("id")
        or record.get("message_id")
        or record.get("event_id")
        or record.get("ts")
        or _stable_hash(record)[:16]
    )
    revision = {
        "content_sha": _stable_hash(record),
        "route_id": route_id,
        "connector_import": True,
    }
    record_id = {"external_id": external_id, "route_id": route_id}
    nodes: list[dict] = []
    edges: list[dict] = []

    if source_id == "gmail_api":
        email_id = f"email:gmail:{external_id}"
        thread_raw = str(record.get("thread_id") or "")
        nodes.append({
            "node_id": email_id,
            "subtype": "email",
            "attrs": {
                "email_id": email_id,
                "subject": record.get("subject", ""),
                "snippet": record.get("snippet") or record.get("body_text", ""),
                "date": record.get("date", ""),
                "sender_ref": record.get("sender_ref") or record.get("from", ""),
                "labels": record.get("labels") or [],
                "external_ref": external_id,
                "connector_route": route_id,
            },
            "source_record_id": record_id,
            "source_revision": revision,
        })
        if thread_raw:
            thread_id = f"email_thread:gmail:{thread_raw}"
            nodes.append({
                "node_id": thread_id,
                "subtype": "email_thread",
                "attrs": {
                    "thread_id": thread_id,
                    "external_ref": thread_raw,
                    "connector_route": route_id,
                },
                "source_record_id": {"thread_id": thread_raw, "route_id": route_id},
                "source_revision": revision,
            })
            edges.append({
                "src": thread_id,
                "dst": email_id,
                "edge_type": "contains",
                "attrs": {"connector_route": route_id},
                "source_record_id": record_id,
                "source_revision": revision,
            })
        return nodes, edges

    if source_id == "google_calendar_api":
        cal_raw = str(record.get("calendar_id") or record.get("feed_id") or "primary")
        feed_id = f"feed:gcal:{_stable_hash(cal_raw)[:12]}"
        event_id = f"event:gcal:{external_id}"
        nodes.append({
            "node_id": feed_id,
            "subtype": "calendar_feed",
            "attrs": {
                "feed_id": feed_id,
                "name": cal_raw,
                "source_kind": "host_connector",
                "external_ref": cal_raw,
                "connector_route": route_id,
            },
            "source_record_id": {"calendar_id": cal_raw, "route_id": route_id},
            "source_revision": {"content_sha": "static", "route_id": route_id},
        })
        nodes.append({
            "node_id": event_id,
            "subtype": "calendar_event",
            "attrs": {
                "event_id": event_id,
                "title": record.get("title") or record.get("summary", ""),
                "kind": record.get("event_kind") or "other",
                "start_dt": record.get("start") or record.get("start_dt", ""),
                "end_dt": record.get("end") or record.get("end_dt", ""),
                "location": record.get("location", ""),
                "external_ref": external_id,
                "feed_ref": feed_id,
                "connector_route": route_id,
            },
            "source_record_id": record_id,
            "source_revision": revision,
        })
        edges.append({
            "src": event_id,
            "dst": feed_id,
            "edge_type": "at_feed",
            "attrs": {"connector_route": route_id},
            "source_record_id": record_id,
            "source_revision": revision,
        })
        return nodes, edges

    if source_id == "slack_api":
        channel_raw = str(record.get("channel_id") or record.get("channel") or "")
        if not channel_raw:
            raise ValueError("slack connector records require channel_id")
        ch_node_id = f"chat_channel:slack:{channel_raw}"
        nodes.append({
            "node_id": ch_node_id,
            "subtype": "chat_channel",
            "attrs": {
                "channel_id": ch_node_id,
                "platform": record.get("platform") or "slack",
                "name": record.get("channel_name") or channel_raw,
                "external_ref": channel_raw,
                "connector_route": route_id,
            },
            "source_record_id": {"channel_id": channel_raw, "route_id": route_id},
            "source_revision": revision,
        })
        if kind in {"channel", "chat_channel"}:
            return nodes, edges
        msg_id = f"chat_message:slack:{channel_raw}:{external_id}"
        nodes.append({
            "node_id": msg_id,
            "subtype": "chat_message",
            "attrs": {
                "message_id": msg_id,
                "channel_ref": ch_node_id,
                "sender_ref": record.get("sender_ref") or record.get("user", ""),
                "ts": record.get("ts") or external_id,
                "body_text": record.get("body_text") or record.get("text", ""),
                "external_ref": external_id,
                "connector_route": route_id,
            },
            "source_record_id": record_id,
            "source_revision": revision,
        })
        edges.append({
            "src": ch_node_id,
            "dst": msg_id,
            "edge_type": "contains",
            "attrs": {"connector_route": route_id},
            "source_record_id": record_id,
            "source_revision": revision,
        })
        return nodes, edges

    raise ValueError(f"unsupported connector source {source_id!r}")


def _missing_connector_allowlists(
    source_id: str, provenance: dict,
) -> list[str]:
    allowlist = provenance.get("allowlist") if isinstance(provenance, dict) else None
    if not isinstance(allowlist, dict):
        allowlist = {}
    required_by_source = {
        "gmail_api": ("labels",),
        "google_calendar_api": ("calendars",),
        "slack_api": ("channels",),
    }
    missing: list[str] = []
    for field in required_by_source.get(source_id, ()):
        value = allowlist.get(field)
        if not value:
            missing.append(field)
    return missing


def _mint_connector_edge_id(src: str, edge_type: str, dst: str) -> int:
    """Mint a 63-bit positive BIGINT edge_id from (src, edge_type, dst).

    Mirrors `okg.substrate.ingest.runner._mint_edge_id` exactly so the
    connector ingest path and the canonical projector/ingest path agree
    on edge_id for the same logical edge (correct dedupe at edges_live).
    """
    return uuid.uuid5(
        uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}",
    ).int & ((1 << 63) - 1)


def _connector_fact_rows(
    *,
    source_name: str,
    nodes: list[dict],
    edges: list[dict],
    run_id: str,
    observed_at: datetime,
) -> tuple[list[tuple], list[tuple]]:
    node_rows: list[tuple] = []
    edge_rows: list[tuple] = []
    for node in nodes:
        revision_bytes = json.dumps(
            node["source_revision"], sort_keys=True, default=str,
        ).encode("utf-8")
        dedupe = (
            f"{source_name}:n:"
            f"{uuid.uuid5(uuid.NAMESPACE_URL, node['node_id'] + '|' + revision_bytes.hex()).hex}"
            ":I"
        )
        node_rows.append((
            node["node_id"],
            node["subtype"],
            json.dumps(node["attrs"], default=str),
            source_name,
            json.dumps(node["source_record_id"], default=str),
            json.dumps(node["source_revision"], default=str),
            observed_at,
            dedupe,
            run_id,
            "I",
        ))
    for edge in edges:
        revision_bytes = json.dumps(
            edge["source_revision"], sort_keys=True, default=str,
        ).encode("utf-8")
        # edge_facts.edge_id is BIGINT. Mint a 63-bit positive int from
        # (src, edge_type, dst), matching the canonical projector/ingest
        # `_mint_edge_id`, so this path and the canonical path agree and
        # dedupe correctly at edges_live. (Previously `.hex` — a 32-char
        # string — which failed the COPY with `invalid input syntax for
        # type bigint`, breaking every connector ingest that emits edges.)
        edge_id = _mint_connector_edge_id(
            edge["src"], edge["edge_type"], edge["dst"],
        )
        dedupe = (
            f"{source_name}:e:"
            f"{uuid.uuid5(uuid.NAMESPACE_URL, f'{edge['src']}|{edge['edge_type']}|{edge['dst']}|' + revision_bytes.hex()).hex}"
            ":I"
        )
        edge_rows.append((
            edge_id,
            edge["src"],
            edge["dst"],
            edge["edge_type"],
            json.dumps(edge.get("attrs") or {}, default=str),
            # provenance must satisfy the edge_facts CHECK
            # ('explicit','derived_deterministic','linked_heuristic',
            # 'extracted_model','derived_semantic'). Connector edges are
            # read directly from the source -> 'explicit'. (Was the
            # literal "connector", which is not an allowed value and
            # failed the CHECK at COPY.)
            "explicit",
            1.0,
            source_name,
            json.dumps(edge["source_record_id"], default=str),
            json.dumps(edge["source_revision"], default=str),
            observed_at,
            dedupe,
            run_id,
            "I",
        ))
    return node_rows, edge_rows


def _copy_connector_rows(
    conn: psycopg.Connection,
    *,
    node_rows: list[tuple],
    edge_rows: list[tuple],
) -> None:
    with conn.transaction():
        if node_rows:
            with conn.cursor() as cur:
                with cur.copy(
                    "COPY okg.node_facts ("
                    "  node_id, subtype, attrs, source, "
                    "  source_record_id, source_revision, observed_at, "
                    "  dedupe_key, run_id, op"
                    ") FROM STDIN"
                ) as cp:
                    for row in node_rows:
                        cp.write_row(row)
        if edge_rows:
            with conn.cursor() as cur:
                with cur.copy(
                    "COPY okg.edge_facts ("
                    "  edge_id, src, dst, edge_type, attrs, provenance, "
                    "  confidence, source, source_record_id, "
                    "  source_revision, observed_at, dedupe_key, run_id, op"
                    ") FROM STDIN"
                ) as cp:
                    for row in edge_rows:
                        cp.write_row(row)


def _legacy_connector_profile(
    *,
    deployment: str,
    source_name: str,
    source_id: str,
    route_id: str,
    allowlist: dict[str, Any],
    catalog_version_id: int | None,
):
    """Resolve the bounded legacy route's server-owned ProducerProfile."""
    from okg.substrate.producer_authority import (
        ProducerProfile,
        profile_fingerprint,
    )

    declared = _LEGACY_CONNECTOR_OUTPUTS[source_id]
    output_signature = {
        "nodes": [{"subtype": subtype} for subtype in declared["nodes"]],
        "edges": [
            {
                "src_subtype": src,
                "edge_type": edge_type,
                "dst_subtype": dst,
            }
            for src, edge_type, dst in declared["edges"]
        ],
    }
    authority_scope = {
        "source_id": source_id,
        "route_id": route_id,
        "allowlist": allowlist,
    }
    fingerprint = profile_fingerprint(
        deployment_name=deployment,
        producer_id=source_name,
        producer_kind="source",
        trust_label="external-source-trusted",
        admission_mode="fast_track",
        authority_scope=authority_scope,
        allowed_record_kinds=("node", "edge"),
        output_signature=output_signature,
        policy_clauses=(),
        catalog_version_id=catalog_version_id,
    )
    return ProducerProfile(
        deployment_name=deployment,
        producer_id=source_name,
        producer_kind="source",
        trust_label="external-source-trusted",
        admission_mode="fast_track",
        authority_scope=authority_scope,
        allowed_record_kinds=("node", "edge"),
        output_signature=output_signature,
        policy_clauses=(),
        output_scope_summary={
            "nodes": list(declared["nodes"]),
            "edges": [" ".join(edge) for edge in declared["edges"]],
        },
        source_ref={
            "legacy_connector": True,
            "source_id": source_id,
            "route_id": route_id,
            **_LEGACY_CONNECTOR_COMPATIBILITY,
        },
        catalog_version_id=catalog_version_id,
        profile_fingerprint=fingerprint,
        status="active",
    )


def _current_catalog_version_id(conn: psycopg.Connection) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'"
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def connector_ingest_batch(
    conn: psycopg.Connection,
    deployment: str,
    source: str,
    route_id: str,
    records: list[dict],
    provenance: Optional[dict] = None,
    *,
    dry_run: bool = False,
) -> dict:
    from okg.substrate.mcp.source_admin import _load_registry_yaml  # noqa: F401  # deferred to break circular import
    """Import a bounded normalized batch from an agent host connector.

    This is intentionally narrow: it accepts normalized records for the
    first connector pilots and stores connector content as inert data.
    """
    target_source_id = SOURCE_CATALOG.get(source, (source, ""))[0]
    if target_source_id not in _CREDENTIAL_SOURCE_IDS:
        return {
            "status": "unsupported_source",
            "source": source,
            "summary": f"`{source}` is not a supported connector import.",
        }
    # Bind the caller-supplied route_id to an approved connector route for this
    # (deployment, source); a bogus route_id must not be honored as provenance.
    approved_routes = _approved_route_ids(target_source_id)
    if route_id not in approved_routes:
        return {
            "status": "unapproved_route",
            "source_id": target_source_id,
            "route_id": route_id,
            "approved_routes": sorted(approved_routes),
            "summary": (
                f"route_id {route_id!r} is not an approved connector route "
                f"for `{source}`."
            ),
        }
    if len(records) > 500:
        return {
            "status": "batch_too_large",
            "record_count": len(records),
            "max_records": 500,
            "summary": "Connector batch exceeds the 500-record setup cap.",
        }
    provenance = dict(provenance or {})
    missing_allowlists = _missing_connector_allowlists(
        target_source_id, provenance,
    )
    if missing_allowlists:
        return {
            "status": "awaiting_allowlist",
            "source_id": target_source_id,
            "route_id": route_id,
            "missing_allowlists": missing_allowlists,
            "summary": (
                "Connector import needs allowlist: "
                + ", ".join(missing_allowlists)
            ),
        }
    # Per-record payload bounds: refuse an oversized / over-deep / over-wide
    # record before normalization (distinct from validation_failed).
    for idx, record in enumerate(records):
        if isinstance(record, dict):
            violation = _record_shape_violation(record)
            if violation:
                return {
                    "status": "record_too_large",
                    "index": idx,
                    "reason": violation,
                    "max_bytes": _MAX_RECORD_BYTES,
                    "max_fields": _MAX_RECORD_FIELDS,
                    "max_depth": _MAX_RECORD_DEPTH,
                    "summary": (
                        f"Connector record {idx} exceeds a payload bound "
                        f"({violation})."
                    ),
                }
    record_allowlist = (
        provenance.get("allowlist")
        if isinstance(provenance.get("allowlist"), dict) else {}
    )
    all_nodes: list[dict] = []
    all_edges: list[dict] = []
    errors: list[dict] = []
    for idx, record in enumerate(records):
        if not isinstance(record, dict):
            errors.append({"index": idx, "error": "record must be an object"})
            continue
        try:
            # Per-record allowlist membership: a label/calendar/channel outside
            # the route's approved allowlist is rejected (loudly), not ingested.
            _assert_record_in_allowlist(
                target_source_id, record, record_allowlist,
            )
            nodes, edges = _normalize_connector_record(
                source_id=target_source_id,
                route_id=route_id,
                record={
                    **record,
                    "provenance": provenance,
                },
            )
        except ValueError as exc:
            errors.append({"index": idx, "error": str(exc)})
            continue
        # Image values (#3046): connector content is copied into attrs, so a
        # record could carry an image value. One with `png` or `error` (which
        # only okg's ingest image step adds) or a malformed one fails the
        # batch, with its code, before anything is written.
        try:
            for fact in (*nodes, *edges):
                check_incoming(fact.get("attrs"))
        except (ImageRefInvalid, ImageValueForged) as exc:
            errors.append({"index": idx, "code": exc.code, "error": exc.message})
            continue
        all_nodes.extend(nodes)
        all_edges.extend(edges)
    if errors:
        return {
            "status": "validation_failed",
            "errors": errors[:10],
            "summary": f"{len(errors)} connector record(s) failed validation.",
        }

    source_name = f"{deployment}.{target_source_id}"
    try:
        _path, raw = _load_registry_yaml(deployment)
        entry = (raw.get("sources") or {}).get(target_source_id) or {}
        source_name = entry.get("ownership_id") or source_name
    except FileNotFoundError:
        # Missing registry metadata is acceptable here; keep the deterministic
        # deployment/source fallback name for direct connector ingestion.
        pass
    if dry_run:
        return {
            "status": "dry_run_ok",
            "deployment": deployment,
            "source_id": target_source_id,
            "route_id": route_id,
            "records_seen": len(records),
            "nodes_would_emit": len(all_nodes),
            "edges_would_emit": len(all_edges),
            "summary": (
                f"Would ingest {len(all_nodes)} nodes / "
                f"{len(all_edges)} edges from {source}."
            ),
        }

    if not all_nodes and not all_edges:
        return {
            "status": "no_records",
            "deployment": deployment,
            "source_id": target_source_id,
            "route_id": route_id,
            "records_seen": len(records),
            "summary": (
                "Connector produced no graph records; no admission was "
                "accepted and no terminal state advanced."
            ),
        }

    from okg.substrate.admission import (
        attach_fact_range_refs_for_run,
        ensure_envelope,
        record_event,
        stable_idempotency_key,
        transition_envelope,
    )
    from okg.substrate.fact_application import (
        FactBatchApplication,
        chronos_apply_callback,
    )
    from okg.substrate.fact_batch import (
        FactBatchHeader,
        FactChunk,
        PostgresApplicationReceiptStore,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    idempotency_key = stable_idempotency_key(
        "legacy_connector_ingest",
        deployment,
        target_source_id,
        route_id,
        records,
        record_allowlist,
    )
    run_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"okg:legacy-connector:{idempotency_key}",
        )
    )
    observed_at = datetime.now(timezone.utc)
    node_rows, edge_rows = _connector_fact_rows(
        source_name=source_name,
        nodes=all_nodes,
        edges=all_edges,
        run_id=run_id,
        observed_at=observed_at,
    )
    catalog_version_id = _current_catalog_version_id(conn)
    profile = _legacy_connector_profile(
        deployment=deployment,
        source_name=source_name,
        source_id=target_source_id,
        route_id=route_id,
        allowlist=record_allowlist,
        catalog_version_id=catalog_version_id,
    )
    envelope = ensure_envelope(
        conn,
        deployment_name=deployment,
        producer_id=source_name,
        producer_kind="source",
        intent="legacy_connector_ingest",
        admission_mode="fast_track",
        status="pending",
        idempotency_key=idempotency_key,
        catalog_version_id=catalog_version_id,
        source=source_name,
        run_id=run_id,
        authority={
            **profile.evidence(),
            "authority_scope": profile.authority_scope,
            "compatibility": _LEGACY_CONNECTOR_COMPATIBILITY,
        },
        check_summary={
            "records_seen": len(records),
            "node_count": len(all_nodes),
            "edge_count": len(all_edges),
            "route_allowlist_validated": True,
        },
    )
    if envelope.inserted:
        record_event(
            conn,
            admission_id=envelope.admission_id,
            event_type="submitted",
            actor=source_name,
            payload={"route_id": route_id, "source_id": target_source_id},
        )
    header = FactBatchHeader(
        schema_version=1,
        run_id=run_id,
        admission_id=envelope.admission_id,
        producer_profile_fingerprint=profile.profile_fingerprint,
        deployment=deployment,
        target_branch="default",
        catalog_version_id=catalog_version_id or 0,
        intent="legacy_connector_ingest",
        idempotency_key=idempotency_key,
        requested_scope=profile.authority_scope,
    )
    facts = [
        *[
            NodeFact(
                node_id=node["node_id"],
                subtype=node["subtype"],
                attrs=node["attrs"],
                source_record_id=node["source_record_id"],
                source_revision=node["source_revision"],
            )
            for node in all_nodes
        ],
        *[
            EdgeFact(
                src=edge["src"],
                dst=edge["dst"],
                edge_type=edge["edge_type"],
                attrs=edge["attrs"],
                provenance=edge.get("provenance") or "explicit",
                confidence=float(edge.get("confidence") or 1.0),
                source_record_id=edge["source_record_id"],
                source_revision=edge["source_revision"],
            )
            for edge in all_edges
        ],
    ]

    def _write_legacy_audit(_chunk, _receipt):
        _copy_connector_rows(conn, node_rows=node_rows, edge_rows=edge_rows)
        return attach_fact_range_refs_for_run(
            conn,
            admission_id=envelope.admission_id,
            source=source_name,
            run_id=run_id,
        )

    application = FactBatchApplication(
        header=header,
        profile=profile,
        receipt_store=PostgresApplicationReceiptStore(conn),
        apply_graph=chronos_apply_callback(
            ChronosGraphStore.from_connection(conn),
            source=source_name,
            observed_at=observed_at,
            strict_endpoints=False,
        ),
        write_chunk_evidence=_write_legacy_audit,
    )
    applied = application.apply_chunk(
        FactChunk.create(run_id=run_id, ordinal=0, facts=facts)
    )
    application.finish_observation(
        completion="delta",
        observed_scope=profile.output_scope_summary,
    )
    transition_envelope(
        conn,
        admission_id=envelope.admission_id,
        status="accepted",
        actor=source_name,
        reason="registered_connector_route",
        verdict={
            "profile_fingerprint": profile.profile_fingerprint,
            "fact_batch_receipt": applied.receipt.to_dict(),
            "compatibility": _LEGACY_CONNECTOR_COMPATIBILITY,
        },
    )
    publish_summary: Optional[dict] = None
    publish_failed = False
    try:
        dsn = conn.info.dsn
        if dsn:
            from psycopg.conninfo import make_conninfo

            from okg.substrate.publisher.factory import (
                build_deployment_publisher,
            )

            # conn.info.dsn redacts the password; reattach it so the Publisher (which
            # opens its own connection for the chronos publish) can authenticate.
            publish_summary = build_deployment_publisher(
                dsn=make_conninfo(dsn, password=conn.info.password),
                deployment_name=deployment,
                command="okg mcp connector ingest",
            ).cycle()
    except Exception as exc:  # noqa: BLE001 — publish failure surfaces in top-level status
        publish_failed = True
        publish_summary = {
            "status": "publish_failed",
            "error": str(exc),
        }
    return {
        # Top-level status reflects WHOLE-call success. The legacy
        # behavior of returning `"ingest_complete"` regardless of
        # publish outcome silently masked publish failures from
        # callers that only checked the top-level field; fix per
        # the post-Wave-D MCP audit.
        "status": (
            "ingest_complete_publish_failed"
            if publish_failed
            else "ingest_complete"
        ),
        "deployment": deployment,
        "source_id": target_source_id,
        "route_id": route_id,
        "run_id": run_id,
        "admission_id": envelope.admission_id,
        "idempotency_key": idempotency_key,
        "records_seen": len(records),
        "nodes_added": len(node_rows),
        "edges_added": len(edge_rows),
        "publish": publish_summary,
        "summary": (
            f"Ingested {len(node_rows)} nodes / {len(edge_rows)} "
            f"edges from {source}."
        ),
    }
