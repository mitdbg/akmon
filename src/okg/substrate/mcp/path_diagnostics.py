"""Typed, bounded diagnostics for MCP path expansion."""

from __future__ import annotations

import difflib
from collections.abc import Iterable
from typing import Any

import psycopg

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.continuation import authority_fingerprint
from okg.substrate.mcp.session import Session
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
)


class InvalidPathGrammar(SubstrateError, ValueError):
    code = "invalid_path_grammar"


class InvalidPathDirection(SubstrateError, ValueError):
    code = "invalid_path_direction"


class InvalidPathEdgeType(SubstrateError, ValueError):
    code = "invalid_path_edge_type"


class InvalidExpandShape(SubstrateError, ValueError):
    code = "invalid_expand_shape"


PATH_RAW_BATCH = 256
PATH_WORK_CAP = 4_096
#: The link type that means any link: a path hop '*:out' and a one-hop
#: `edge_type="*"` read every edge type, as `edge_type=None` does.
ANY_LINK = "*"


def any_link(edge_type: Any) -> Any:
    """`edge_type` as the readers take it: None for :data:`ANY_LINK`."""

    return None if edge_type == ANY_LINK else edge_type


def validate_path_limit(limit: Any) -> int:
    """Reject path pages that cannot return a resumable item."""

    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise InvalidExpandShape(
            "expand: path limit must be a positive integer",
            data={"requested_shape": "path", "field": "limit", "limit": limit},
            fix_hint="set limit to a positive integer and retry",
        )
    return limit


def path_authority_summary(
    conn: psycopg.Connection,
    session: Session,
) -> dict[str, Any]:
    digest, policy = authority_fingerprint(conn, session)
    return {
        "mode": "nomos",
        "fingerprint": digest,
        "policy_fingerprint": policy,
    }


def parse_path_hops(
    path: Any, *, max_hops: int, default_direction: str | None = None,
) -> list[tuple[str | None, str]]:
    """Parse the public grammar without executing a graph query.

    A hop's edge type :data:`ANY_LINK` (``'*:out'``) is read as None, the
    any-edge hop the path query already takes. With `default_direction`
    ('out' or 'in': the call's own `direction`), a hop named without one
    (``'*'``, ``'contains'``) takes it, and the reply says so in its
    `adjusted` note; without it such a hop is refused."""

    if not isinstance(path, list):
        raise InvalidPathGrammar(
            "expand: path must be a list of 'edge_type:direction' hops",
            data={"hop_index": 0},
            fix_hint="pass a non-empty list such as ['contains:out']",
        )
    if not path:
        raise InvalidPathGrammar(
            "expand: path must have at least one hop",
            data={"hop_index": 0},
            fix_hint="pass a non-empty list such as ['contains:out']",
        )
    if len(path) > max_hops:
        raise InvalidPathGrammar(
            f"expand: path exceeds the maximum of {max_hops} hops",
            data={"hop_index": max_hops, "max_hops": max_hops},
            fix_hint=f"shorten the path to at most {max_hops} hops",
        )
    parsed: list[tuple[str | None, str]] = []
    # Noted only once every hop parses: a call refused at a later hop
    # carries no note about how an earlier one was read.
    notes: list[dict[str, str]] = []
    for hop_index, raw_step in enumerate(path):
        if (
            isinstance(raw_step, str) and raw_step and ":" not in raw_step
            and default_direction in {"out", "in"}
        ):
            served = f"{raw_step}:{default_direction}"
            notes.append({
                "rule": "path_hop_direction",
                "sent": f"path hop {raw_step!r}",
                "served": repr(served),
            })
            raw_step = served
        if not isinstance(raw_step, str) or ":" not in raw_step:
            fix_hint = "add ':out' or ':in' to the named edge type"
            if default_direction is not None and default_direction not in {"out", "in"}:
                # `direction="both"` is a one-hop direction: a path hop goes
                # one way, so it gives a bare hop no direction.
                fix_hint = (
                    f"a path hop goes one way, so direction={default_direction!r} "
                    "does not give it one; " + fix_hint
                )
            raise InvalidPathGrammar(
                f"expand: hop {hop_index} must use 'edge_type:out' or 'edge_type:in'",
                data={"hop_index": hop_index, "requested_hop": raw_step},
                fix_hint=fix_hint,
            )
        edge_type, direction = raw_step.rsplit(":", 1)
        if not edge_type:
            raise InvalidPathGrammar(
                f"expand: hop {hop_index} has an empty edge type",
                data={
                    "hop_index": hop_index,
                    "edge_type": edge_type,
                    "direction": direction,
                },
                fix_hint="name a catalog edge type before the colon",
            )
        if direction not in {"out", "in"}:
            raise InvalidPathDirection(
                f"expand: hop {hop_index} direction must be 'out' or 'in'",
                data={
                    "hop_index": hop_index,
                    "edge_type": edge_type,
                    "direction": direction,
                },
                fix_hint="replace the direction with 'out' or 'in'",
            )
        parsed.append((any_link(edge_type), direction))
    if notes:
        from okg.substrate.mcp.response_bounds import note_adjusted

        note_adjusted(notes)
    return parsed


def validate_path_edge_types(
    conn: psycopg.Connection,
    session: Session,
    hops: list[tuple[str | None, str]],
) -> dict[str, Any]:
    """Validate every hop against the catalog pinned with ``session``."""

    valid = pinned_edge_types(conn, session)
    authority = path_authority_summary(conn, session)
    # Narrowing enforcement is conditional in the catalog contract: an empty
    # narrowing set is permissive rather than a whitelist that admits nothing.
    # In that shape the pinned catalog has no edge-type registry against which
    # to reject a requested name, so preserve the same permissive semantics as
    # the writer and database trigger.
    if not valid:
        return authority
    for hop_index, (edge_type, direction) in enumerate(hops):
        if edge_type is None:
            continue
        if edge_type not in valid:
            raise _unknown_edge_type(
                f"expand: hop {hop_index} requests edge type {edge_type!r}, "
                "which is absent from the pinned catalog",
                session, valid, authority,
                hop_index=hop_index, edge_type=edge_type, direction=direction,
            )
    return authority


def pinned_edge_types(conn: psycopg.Connection, session: Session) -> list[str]:
    """The edge types the catalog pinned with ``session`` declares, sorted.

    Empty when the catalog declares no narrowing: then it has no edge-type
    registry, and no name is refused (the writer's own rule)."""

    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT edge_type FROM okg.narrowings "
            "WHERE catalog_version_id = %s ORDER BY edge_type",
            (session.catalog_version_id,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def _unknown_edge_type(
    message: str,
    session: Session,
    valid: list[str],
    authority: dict[str, Any],
    *,
    hop_index: int,
    edge_type: str,
    direction: str,
) -> InvalidPathEdgeType:
    """The refusal of an edge type the pinned catalog does not declare, for a
    path hop and a one-hop expand alike.

    #3666: the hint used to send the caller to the whole ontology, an unpaged
    payload larger than the default reply budget. The closest catalog names
    come with the refusal, and the pointer is a bounded block: the edge types
    with links at this pin. '*' follows any link.
    """

    closest = difflib.get_close_matches(edge_type, sorted(valid), n=3, cutoff=0.6)
    data: dict[str, Any] = {
        "hop_index": hop_index,
        "edge_type": edge_type,
        "direction": direction,
        "pinned_generation_id": session.generation_id,
        "catalog_version_id": session.catalog_version_id,
        "authority": authority,
        "catalog_edge_type_count": len(valid),
        "any_link": ANY_LINK,
    }
    if closest:
        data["closest_edge_types"] = closest
    nearest = f"did you mean {', '.join(closest)}? " if closest else ""
    return InvalidPathEdgeType(
        message,
        data=data,
        fix_hint=(
            f"{nearest}inspect(target={{'kind':'graph'}}, "
            "select=['edge_types']) lists the edge types with links "
            f"at this pin; retry with one of them, or {ANY_LINK!r} for any link"
        ),
    )


def validate_one_hop_edge_type(
    conn: psycopg.Connection,
    session: Session,
    edge_type: str | None,
    direction: str,
) -> None:
    """Refuse a one-hop `edge_type` the pinned catalog does not declare.

    Before this check an unknown name (a typo, or '*' taken literally) went
    to the edge query and came back as an empty page stamped complete. None
    (any link) and a catalog with no narrowings pass, as in a path."""

    if edge_type is None:
        return
    valid = pinned_edge_types(conn, session)
    if not valid or edge_type in valid:
        return
    raise _unknown_edge_type(
        f"expand: edge type {edge_type!r} is absent from the pinned catalog",
        session, valid, path_authority_summary(conn, session),
        hop_index=0, edge_type=edge_type, direction=direction,
    )


def stamp_path_error(
    error: SubstrateError,
    *,
    session: Session,
    authority: dict[str, Any],
) -> SubstrateError:
    """Add the established pin to a pre-graph grammar refusal."""

    error.data.setdefault("pinned_generation_id", session.generation_id)
    error.data.setdefault("catalog_version_id", session.catalog_version_id)
    error.data.setdefault("authority", authority)
    return error


def _path_query(
    hops: list[tuple[str | None, str]],
    *,
    target_subtype: str | None,
    after_terminal: str | None,
) -> str:
    """Build a bounded raw-path query over registered Chronos surfaces."""

    select_parts: list[str] = []
    joins: list[str] = []
    predicates: list[str] = []
    order_parts: list[str] = []
    previous_node = ":start"
    for ordinal, (_edge_type, direction) in enumerate(hops, start=1):
        edge = f"e{ordinal}"
        node = f"n{ordinal}"
        match_col = "src" if direction == "out" else "dst"
        next_col = "dst" if direction == "out" else "src"
        if ordinal == 1:
            joins.append(f"FROM okg.graph_edges {edge}")
            predicates.append(f"{edge}.{match_col} = {previous_node}")
        else:
            joins.append(
                f"JOIN okg.graph_edges {edge} ON {edge}.{match_col} = {previous_node}"
            )
        joins.append(
            f"JOIN okg.graph_nodes {node} ON {node}.node_id = {edge}.{next_col}"
        )
        if _edge_type is not None:
            predicates.append(f"{edge}.edge_type = :etype_{ordinal}")
        select_parts.extend(
            [
                f"{edge}.edge_id AS hop_{ordinal}_edge_id",
                f"{edge}.src AS hop_{ordinal}_src",
                f"{edge}.dst AS hop_{ordinal}_dst",
                f"{edge}.edge_type AS hop_{ordinal}_edge_type",
                f"{edge}.attrs AS hop_{ordinal}_edge_attrs",
                f"{edge}.source AS hop_{ordinal}_edge_source",
                f"{edge}.source_record_id AS hop_{ordinal}_edge_source_record_id",
                f"{edge}.source_revision AS hop_{ordinal}_edge_source_revision",
                f"{node}.node_id AS hop_{ordinal}_node_id",
                f"{node}.subtype AS hop_{ordinal}_node_subtype",
                f"{node}.attrs AS hop_{ordinal}_node_attrs",
                f"{node}.source AS hop_{ordinal}_node_source",
                f"{node}.source_record_id AS hop_{ordinal}_node_source_record_id",
                f"{node}.source_revision AS hop_{ordinal}_node_source_revision",
            ]
        )
        order_parts.append(f"{edge}.edge_id")
        previous_node = f"{node}.node_id"
    terminal = f"n{len(hops)}"
    if target_subtype is not None:
        predicates.append(f"{terminal}.subtype = :target_subtype")
    if after_terminal is not None:
        predicates.append(f"{terminal}.node_id > :after_terminal")
    return (
        "SELECT "
        + ", ".join(select_parts)
        + " "
        + " ".join(joins)
        + " WHERE "
        + " AND ".join(predicates)
        + f" ORDER BY {terminal}.node_id, "
        + ", ".join(order_parts)
        + " LIMIT :raw_limit OFFSET :raw_offset"
    )


def _edge_row(raw: dict[str, Any], ordinal: int) -> dict[str, Any]:
    return {
        "edge_id": raw[f"hop_{ordinal}_edge_id"],
        "src": raw[f"hop_{ordinal}_src"],
        "dst": raw[f"hop_{ordinal}_dst"],
        "edge_type": raw[f"hop_{ordinal}_edge_type"],
        "attrs": raw[f"hop_{ordinal}_edge_attrs"],
        "source": raw[f"hop_{ordinal}_edge_source"],
        "source_record_id": raw[f"hop_{ordinal}_edge_source_record_id"],
        "source_revision": raw[f"hop_{ordinal}_edge_source_revision"],
    }


def _node_row(raw: dict[str, Any], ordinal: int) -> dict[str, Any]:
    return {
        "node_id": raw[f"hop_{ordinal}_node_id"],
        "subtype": raw[f"hop_{ordinal}_node_subtype"],
        "attrs": raw[f"hop_{ordinal}_node_attrs"],
        "source": raw[f"hop_{ordinal}_node_source"],
        "source_record_id": raw[f"hop_{ordinal}_node_source_record_id"],
        "source_revision": raw[f"hop_{ordinal}_node_source_revision"],
    }


def _visible_path_rows(
    conn: psycopg.Connection,
    session: Session,
    raw_rows: Iterable[dict[str, Any]],
    *,
    hop_count: int,
) -> list[dict[str, Any]]:
    """Keep a raw path only when every edge and reached node is visible."""

    rows = list(raw_rows)
    if not rows:
        return []
    visible_edge_ids: list[set[str]] = []
    visible_nodes: list[dict[str, dict[str, Any]]] = []
    for ordinal in range(1, hop_count + 1):
        edge_rows = {
            _edge_row(row, ordinal)["edge_id"]: _edge_row(row, ordinal) for row in rows
        }
        node_rows = {
            _node_row(row, ordinal)["node_id"]: _node_row(row, ordinal) for row in rows
        }
        visible_edge_ids.append(
            {
                str(row["edge_id"])
                for row in filter_edge_rows_for_session(
                    conn,
                    session,
                    edge_rows.values(),
                )
            }
        )
        visible_nodes.append(
            {
                str(row["node_id"]): row
                for row in filter_node_rows_for_session(
                    conn,
                    session,
                    node_rows.values(),
                )
            }
        )
    visible: list[dict[str, Any]] = []
    for raw in rows:
        if all(
            str(raw[f"hop_{ordinal}_edge_id"]) in visible_edge_ids[ordinal - 1]
            and str(raw[f"hop_{ordinal}_node_id"]) in visible_nodes[ordinal - 1]
            for ordinal in range(1, hop_count + 1)
        ):
            terminal = visible_nodes[-1][str(raw[f"hop_{hop_count}_node_id"])]
            visible.append(terminal)
    return visible


def scan_visible_path(
    conn: psycopg.Connection,
    session: Session,
    *,
    start_node: str,
    hops: list[tuple[str | None, str]],
    target_subtype: str | None,
    after_terminal: str | None,
    visible_limit: int,
) -> tuple[list[dict[str, Any]], bool, bool]:
    """Return bounded visible terminals, raw exhaustion, and work-cap state."""

    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False
    ).checkout_generation(
        session.generation_id,
        branch=session.branch_name,
    )
    params: dict[str, Any] = {
        "start": start_node,
        "raw_limit": PATH_RAW_BATCH,
        "raw_offset": 0,
    }
    for ordinal, (edge_type, _direction) in enumerate(hops, start=1):
        if edge_type is not None:
            params[f"etype_{ordinal}"] = edge_type
    if target_subtype is not None:
        params["target_subtype"] = target_subtype
    if after_terminal is not None:
        params["after_terminal"] = after_terminal
    sql = _path_query(
        hops,
        target_subtype=target_subtype,
        after_terminal=after_terminal,
    )
    visible_by_id: dict[str, dict[str, Any]] = {}
    examined = 0
    exhausted = False
    while examined < PATH_WORK_CAP and len(visible_by_id) < visible_limit:
        batch_size = min(PATH_RAW_BATCH, PATH_WORK_CAP - examined)
        params["raw_limit"] = batch_size
        params["raw_offset"] = examined
        raw = [dict(row) for row in graph.query(sql, params)]
        examined += len(raw)
        for row in _visible_path_rows(conn, session, raw, hop_count=len(hops)):
            visible_by_id.setdefault(str(row["node_id"]), row)
        if len(raw) < batch_size:
            exhausted = True
            break
    ordered = [visible_by_id[node_id] for node_id in sorted(visible_by_id)]
    return (
        ordered[:visible_limit],
        exhausted,
        not exhausted and examined >= PATH_WORK_CAP,
    )


def start_node_visibility(
    conn: psycopg.Connection,
    session: Session,
    *,
    start_node: str,
) -> bool:
    """Collapse absent and Nomos-hidden starts to the same boolean."""

    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False
    ).checkout_generation(
        session.generation_id,
        branch=session.branch_name,
    )
    rows = graph.query(
        "SELECT node_id, subtype, attrs, source, source_record_id, source_revision "
        "FROM okg.graph_nodes WHERE node_id = :node_id LIMIT 1",
        {"node_id": start_node},
    )
    return bool(filter_node_rows_for_session(conn, session, rows))


def empty_path_outcome(
    *,
    session: Session,
    authority: dict[str, Any],
    start_node: str,
    hops: list[tuple[str | None, str]],
    outcome: str,
    hop_index: int,
    target_subtype: str | None = None,
    visibility_limited: bool = False,
) -> dict[str, Any]:
    edge_type, direction = hops[min(hop_index, len(hops) - 1)]
    next_moves = {
        "start_not_visible": "check the node id or use search to resolve a visible start node",
        "no_matching_first_hop": "inspect the subtype routes or choose another first edge",
        "empty_intermediate_hop": "shorten the path at the named hop or choose another route",
        "empty_terminal_result": "remove or change target_subtype and retry",
        "visibility_limited": "narrow the path or ask an authorized caller to retry",
    }
    return {
        "status": "ok",
        "start_node": start_node,
        "path": [f"{edge if edge is not None else '*'}:{way}" for edge, way in hops],
        "target_subtype": target_subtype,
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        "catalog_version_id": session.catalog_version_id,
        "authority": authority,
        "outcome": {
            "code": outcome,
            "hop_index": hop_index,
            "edge_type": edge_type,
            "direction": direction,
            "next_move": next_moves[outcome],
            **(
                {"visibility": "observed_diagnostics_withheld"}
                if visibility_limited
                else {}
            ),
        },
        **(
            {"error": "start_node is not visible at the pinned generation"}
            if outcome == "start_not_visible"
            else {}
        ),
        "results": [],
        "total_count": None if visibility_limited else 0,
        "count_state": "withheld" if visibility_limited else "exact_visible",
        "truncated": False,
        "diagnostic_work_cap": PATH_WORK_CAP,
    }
