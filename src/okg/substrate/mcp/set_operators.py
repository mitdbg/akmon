"""Set-shaped operator inputs (operator-surface-list-work
req.listwork.set-operators).

The 2026-06-11 dogfood wave showed every list-shaped task collapsing to
the SQL escape hatch because aggregate/filter/expand only accepted
single-node anchors. These helpers decide which inputs are sets and which
scopes are valid. The reads themselves run in ``server.py`` on the
effective-attribute SQL relation, with every predicate compiled by
``operator_sql.compile_read_predicate`` and every caller value bound as a
parameter (issue #3201); none of them goes through ``run_sql``.
"""

from __future__ import annotations

from typing import Any

import psycopg

from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
from okg.substrate.mcp.session import Session


def _serialize_edge_items(
    conn: psycopg.Connection,
    session: Session,
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not items or not any("attrs" in item for item in items):
        return items
    from okg.substrate.mcp.provenance import (
        serialize_edge_read_rows_at_pin,
    )

    return serialize_edge_read_rows_at_pin(
        conn,
        items,
        deployment_name=session.deployment_name,
        generation_id=session.generation_id,
        branch_name=session.branch_name,
    )


def is_set_input(input: Any) -> bool:
    """A dict input naming a scope/subtype (not a single node anchor)."""
    if not isinstance(input, dict):
        return False
    if input.get("node_id") or input.get("start_node"):
        return False
    return bool(
        input.get("subtype")
        or input.get("filter")
        or input.get("kind") in {"nodes", "edges"}
    )


def validate_set_scope(kind: str, input: Any) -> None:
    """Refuse a scope narrowing that does not apply to its kind.

    `subtype` narrows nodes and `edge_type` narrows edges. Naming the other
    one is a mistake, never "ignore it and read the whole kind".
    """
    if not isinstance(input, dict):
        return
    if input.get("subtype") and kind != "nodes":
        raise InvalidOperatorArguments("set input: subtype applies to kind='nodes'")
    if input.get("edge_type") and kind != "edges":
        raise InvalidOperatorArguments("set input: edge_type applies to kind='edges'")
