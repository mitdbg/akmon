"""Compact, resumable whole-subtype schema summaries."""

from __future__ import annotations

import json
from typing import Any

import psycopg

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.continuation import (
    consume_continuation,
    continuation_binding,
    issue_continuation,
)
from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes
from okg.substrate.mcp.session import Session


DEFAULT_SECTION_PAGE_SIZE = 8
MAX_SECTION_PAGE_SIZE = 32
MAX_SAMPLE_VALUES_PER_KEY = 3
MAX_SAMPLE_VALUE_CHARS = 120
MAX_RECOMMENDED_FILTERS = 32


class InvalidSubtypeInspection(SubstrateError, ValueError):
    code = "invalid_subtype_inspection"


class SubtypeSummaryItemTooLarge(SubstrateError, ValueError):
    code = "subtype_summary_item_too_large"


#: Added to a measured page size before it is offered as a page budget: a
#: page sized under a larger budget can render a few bytes longer.
_RETRY_MARGIN_BYTES = 64


def _cut_sample_nodes(payload: dict[str, Any], *, max_bytes: int) -> dict[str, Any] | None:
    """`payload` with its sample records cut to fit `max_bytes`, or None.

    Every sample keeps the same number of leading characters of each text
    attribute (#3666 step 3b); when emptying the text is not enough, lists
    and objects are left out, largest first. A record that lost anything
    is marked as a cut `inspect(node)` record is (`status: cut`,
    `attrs_cut`, `read_whole`); the others are served unchanged. When even
    the samples cut to their floor do not fit, the last ones are left out
    and named in `sample_nodes_omitted` (each is one inspect(node) away);
    None only when the page does not fit with no sample at all."""
    from okg.substrate.mcp.node_batch import _cut_form, _cuttable, _size
    from okg.substrate.mcp.response_bounds import _MIN_KEPT_CHARS, _text_length

    every = [node for node in payload["sample_nodes"] if isinstance(node, dict)]
    if len(every) != len(payload["sample_nodes"]):
        return None

    def fits(candidate: dict[str, Any]) -> bool:
        return public_call_tool_result_bytes(
            candidate, tool="inspect", kind="subtype",
        ) <= max_bytes

    for count in range(len(every), -1, -1):
        samples = every[:count]
        omitted = [str(node.get("node_id")) for node in every[count:]]
        base = {**payload, **({"sample_nodes_omitted": omitted} if omitted else {})}

        def cut_all(
            keep: int, dropped: int,
            samples: list[dict[str, Any]] = samples, base: dict[str, Any] = base,
        ) -> dict[str, Any]:
            nodes = []
            for node in samples:
                attrs = node.get("attrs") if isinstance(node.get("attrs"), dict) else {}
                _, whole = _cuttable(attrs)
                by_size = sorted(whole, key=lambda key: (-_size(attrs[key]), key))
                out = _cut_form(node, keep, frozenset(by_size[:dropped]))
                nodes.append(out if out["attrs_cut"] else node)
            return {**base, "sample_nodes": nodes}

        longest = max(
            (
                _text_length(value)
                for node in samples
                for value in (node.get("attrs") or {}).values()
                if isinstance(value, str)
            ),
            default=0,
        )
        most_whole = max(
            (len(_cuttable(node.get("attrs") or {})[1]) for node in samples), default=0,
        )
        for dropped in range(most_whole + 1):
            if not fits(cut_all(0, dropped)):
                continue
            low, high = 0, longest
            while low < high:
                middle = (low + high + 1) // 2
                if fits(cut_all(middle, dropped)):
                    low = middle
                else:
                    high = middle - 1
            if low < _MIN_KEPT_CHARS and fits(cut_all(0, dropped)):
                low = 0
            return cut_all(low, dropped)
    return None


def catalog_subtype_shape(
    conn: psycopg.Connection,
    session: Session,
    subtype: str,
) -> dict[str, Any]:
    """Return declared attributes, filter hints, and routes at the catalog pin."""

    with conn.cursor() as cur:
        cur.execute(
            "SELECT metadata FROM okg.subtypes "
            "WHERE catalog_version_id = %s AND name = %s",
            (session.catalog_version_id, subtype),
        )
        row = cur.fetchone()
        metadata = row[0] if row and isinstance(row[0], dict) else {}
        cur.execute(
            "SELECT parent_subtype, edge_type, child_subtype "
            "FROM okg.narrowings WHERE catalog_version_id = %s "
            "AND (parent_subtype = %s OR child_subtype = %s) "
            "ORDER BY parent_subtype, edge_type, child_subtype",
            (session.catalog_version_id, subtype, subtype),
        )
        narrowing_rows = cur.fetchall()

    filterable = metadata.get("filterable_attrs") or {}
    pii = metadata.get("pii_classes") or {}
    merge = metadata.get("merge_semantics") or {}
    declared_names = sorted(
        {
            str(name)
            for mapping in (filterable, pii, merge)
            if isinstance(mapping, dict)
            for name in mapping
        }
    )
    declared = []
    for name in declared_names:
        declared.append(
            {
                "name": name,
                "declared": True,
                "observed": False,
                "declared_value_type": (
                    str(filterable[name]) if name in filterable else None
                ),
            }
        )
    routes_out = [
        {
            "edge_type": str(edge_type),
            "dst_subtype": str(child),
            "source": "declared",
        }
        for parent, edge_type, child in narrowing_rows
        if str(parent) == subtype
    ]
    routes_in = [
        {
            "edge_type": str(edge_type),
            "src_subtype": str(parent),
            "source": "declared",
        }
        for parent, edge_type, child in narrowing_rows
        if str(child) == subtype
    ]
    recommended = [
        {
            "attr": str(name),
            "operator": "eq",
            "value_type": str(value_type),
            "reason": "catalog_filterable_attribute",
        }
        for name, value_type in sorted(filterable.items())
    ][:MAX_RECOMMENDED_FILTERS]
    # Issue #2013: the subtype's declared LIVE relations, carried on the same
    # `okg.subtypes.metadata` jsonb the catalog already stamps -- no new
    # column and no migration. What is stored is the COMPILED declaration
    # (`LiveRelation.published()`), so what `inspect` advertises here and what
    # the dispatcher validates arguments against are rendered from one object.
    #
    # A subtype that declares none returns the key ABSENT, never `{}`: "this
    # subtype declares no live relations" and "this surface withholds them"
    # have to stay distinguishable, the same distinction `InspectKindNotServed`
    # preserves one level up.
    # The catalog stores the WHOLE compiled declaration (LiveRelation
    # .to_metadata) because the dispatcher rebuilds from it. `inspect`
    # publishes the caller-facing SUBSET: `principal_mode` and
    # `deadline_seconds` are operator facts a caller cannot act on.
    stored = metadata.get("live_relations")
    live_relations = None
    if isinstance(stored, dict) and stored:
        from okg.substrate.external_backed.live_relations import LiveRelation

        live_relations = {
            name: LiveRelation.from_metadata(name, subtype, body).published()
            for name, body in sorted(stored.items())
            if isinstance(body, dict)
        }
    return {
        "declared_attributes": declared,
        "recommended_filters": recommended,
        "declared_edges_out": routes_out,
        "declared_edges_in": routes_in,
        **(
            {"live_relations": live_relations}
            if isinstance(live_relations, dict) and live_relations
            else {}
        ),
    }


def merge_declared_and_observed_attributes(
    declared: list[dict[str, Any]],
    observed: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_name = {str(item["name"]): dict(item) for item in declared}
    for item in observed:
        name = str(item["name"])
        prior = by_name.get(name, {"name": name, "declared": False})
        by_name[name] = {**prior, **item, "observed": True}
    return [by_name[name] for name in sorted(by_name)]


def _route_key(route: dict[str, Any], direction: str) -> tuple[str, str]:
    other = "dst_subtype" if direction == "out" else "src_subtype"
    return str(route.get("edge_type") or ""), str(route.get(other) or "")


def _after(items: list[dict[str, Any]], after: Any, key) -> list[dict[str, Any]]:
    if after is None:
        return items
    marker = tuple(after) if isinstance(after, list) else after
    return [item for item in items if key(item) > marker]


def _preview(value: Any) -> dict[str, Any]:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    truncated = len(encoded) > MAX_SAMPLE_VALUE_CHARS
    return {
        "value": encoded[:MAX_SAMPLE_VALUE_CHARS],
        "truncated": truncated,
    }


def bounded_sample_values(
    graph: Any,
    *,
    subtype: str,
    attr_names: list[str],
    exclude_link_endpoints: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Read at most a small constant of deterministic previews per key.

    ``exclude_link_endpoints`` (issue #3666, true under audit and enforce):
    a "known by key, not loaded" endpoint is hidden there, so its key is
    never a sample value."""
    from okg.substrate.catalog.linker_config import (
        LINK_ENDPOINT_SOURCE,
        LOAD_STATE_ATTR,
        NOT_LOADED,
    )

    endpoint_clause = (
        "AND NOT (coalesce(source, '') = :endpoint_source "
        "AND coalesce(attrs->>:state_attr, '') = :not_loaded) "
        if exclude_link_endpoints
        else ""
    )
    samples: dict[str, list[dict[str, Any]]] = {}
    for attr_name in attr_names:
        params: dict[str, Any] = {
            "attr": attr_name,
            "subtype": subtype,
            "sample_limit": MAX_SAMPLE_VALUES_PER_KEY,
        }
        if exclude_link_endpoints:
            params.update(
                endpoint_source=LINK_ENDPOINT_SOURCE,
                state_attr=LOAD_STATE_ATTR,
                not_loaded=NOT_LOADED,
            )
        rows = graph.query(
            "SELECT jsonb_extract_path(attrs, CAST(:attr AS text)) AS value "
            "FROM okg.graph_nodes "
            "WHERE subtype = :subtype AND attrs ? :attr "
            + endpoint_clause
            + "ORDER BY node_id LIMIT :sample_limit",
            params,
        )
        samples[attr_name] = [_preview(row["value"]) for row in rows]
    return samples


def compact_subtype_summary(
    conn: psycopg.Connection,
    session: Session,
    *,
    subtype: str,
    node_count: int | None,
    attributes: list[dict[str, Any]],
    edges_out: list[dict[str, Any]],
    edges_in: list[dict[str, Any]],
    recommended_filters: list[dict[str, Any]],
    visibility: dict[str, Any],
    max_bytes: int,
    requested_page_size: int,
    continuation_token: str | None,
    detail: str,
    sample_nodes: list[dict[str, Any]] | None = None,
    sample_value_graph: Any | None = None,
) -> dict[str, Any]:
    """Page all independent summary sections and fit the public byte ceiling."""

    requested_page_size = max(1, min(int(requested_page_size), MAX_SECTION_PAGE_SIZE))
    binding = continuation_binding(
        conn,
        session,
        kind="inspect_subtype",
        subtype=subtype,
        sort="attribute:name;route:edge_type,subtype",
        projection={
            "detail": detail,
            "page_size": requested_page_size,
            "max_bytes": max_bytes,
            "sample_values_per_key": MAX_SAMPLE_VALUES_PER_KEY,
            "sample_value_chars": MAX_SAMPLE_VALUE_CHARS,
        },
    )
    cursor: dict[str, Any] = {}
    if continuation_token is not None:
        cursor = consume_continuation(
            continuation_token,
            expected_binding=binding,
        )

    attrs_remaining = _after(
        attributes, cursor.get("attribute_after"), lambda item: str(item["name"])
    )
    filters_remaining = _after(
        recommended_filters,
        cursor.get("filter_after"),
        lambda item: str(item["attr"]),
    )
    out_remaining = _after(
        edges_out, cursor.get("out_route_after"), lambda item: _route_key(item, "out")
    )
    in_remaining = _after(
        edges_in, cursor.get("in_route_after"), lambda item: _route_key(item, "in")
    )

    page_size = requested_page_size
    while True:
        attr_page = [dict(item) for item in attrs_remaining[:page_size]]
        filter_page = filters_remaining[:page_size]
        if sample_value_graph is not None and attr_page:
            from okg.substrate.nomos.runtime_enforcement import (
                deployment_requires_nomos_runtime,
            )

            values = bounded_sample_values(
                sample_value_graph,
                subtype=subtype,
                attr_names=[str(item["name"]) for item in attr_page],
                exclude_link_endpoints=deployment_requires_nomos_runtime(
                    session.deployment_name,
                ),
            )
            for item in attr_page:
                item["sample_values"] = values.get(str(item["name"]), [])
        out_page = out_remaining[:page_size]
        in_page = in_remaining[:page_size]
        truncated_sections = {
            "attributes": len(attrs_remaining) > len(attr_page),
            "recommended_filters": len(filters_remaining) > len(filter_page),
            "edges_out": len(out_remaining) > len(out_page),
            "edges_in": len(in_remaining) > len(in_page),
        }
        truncated = any(truncated_sections.values())
        next_cursor = {
            "attribute_after": (
                str(attr_page[-1]["name"])
                if attr_page
                else cursor.get("attribute_after")
            ),
            "filter_after": (
                str(filter_page[-1]["attr"])
                if filter_page
                else cursor.get("filter_after")
            ),
            "out_route_after": (
                list(_route_key(out_page[-1], "out"))
                if out_page
                else cursor.get("out_route_after")
            ),
            "in_route_after": (
                list(_route_key(in_page[-1], "in"))
                if in_page
                else cursor.get("in_route_after")
            ),
        }
        payload = {
            "status": "ok",
            "subtype": subtype,
            "node_count": node_count,
            "attr_keys": attr_page,
            "recommended_filters": filter_page,
            "edges_out": out_page,
            "edges_in": in_page,
            "sample_nodes": sample_nodes if detail == "sample_nodes" else None,
            "visibility": visibility,
            "generation_id": session.generation_id,
            "pinned_generation_id": session.generation_id,
            "catalog_version_id": session.catalog_version_id,
            "truncated": truncated,
            "truncated_sections": truncated_sections,
            "section_page_size": page_size,
            "detail": detail,
            **(
                {
                    "continuation_token": issue_continuation(
                        binding,
                        cursor=next_cursor,
                    )
                }
                if truncated
                else {}
            ),
        }
        payload_bytes = public_call_tool_result_bytes(payload, tool="inspect", kind="subtype")
        if payload_bytes <= max_bytes:
            return payload
        if page_size == 1 and payload.get("sample_nodes"):
            # #3666 step 3b: size alone never refuses. The sample records
            # are cut (text keeps its start; lists and objects are left out),
            # each marked as a cut `inspect(node)` is, before the page is.
            cut = _cut_sample_nodes(payload, max_bytes=max_bytes)
            if cut is not None:
                return cut
        if page_size == 1:
            needed = payload_bytes + _RETRY_MARGIN_BYTES
            error = SubtypeSummaryItemTooLarge(
                "one subtype-summary item exceeds the requested MCP byte budget",
                data={
                    "subtype": subtype,
                    "max_bytes": max_bytes,
                    "minimum_item_bytes": payload_bytes,
                    "pinned_generation_id": session.generation_id,
                },
                fix_hint=(
                    "increase max_bytes, omit sample_nodes and continue the compact "
                    "summary, or inspect a specific node for bounded detail"
                ),
            )
            # The page size a budget must leave for this page; the caller,
            # which knows its own reserves and ceiling, turns it into a call.
            error.needed_page_bytes = needed
            raise error
        page_size = max(1, page_size // 2)
