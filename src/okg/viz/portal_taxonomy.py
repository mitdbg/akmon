"""Manifest-backed portal tree readers."""
from __future__ import annotations

from functools import lru_cache
from typing import Any

from okg.substrate import fastyaml


@lru_cache(maxsize=64)
def load_portal_taxonomies(deployment_name: str) -> dict[str, Any]:
    from okg.substrate.deployments import find_deployment_dir

    deployment_dir = find_deployment_dir(deployment_name)
    if deployment_dir is None:
        return {}
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return {}
    manifest = fastyaml.safe_load(manifest_path.read_text()) or {}
    viz = manifest.get("viz") if isinstance(manifest, dict) else {}
    portal = viz.get("portal") if isinstance(viz, dict) else {}
    taxonomies = portal.get("taxonomies") if isinstance(portal, dict) else {}
    return dict(taxonomies) if isinstance(taxonomies, dict) else {}


def read_portal_tree(
    graph,
    *,
    taxonomies: dict[str, Any],
    taxonomy: str,
    path: str,
    limit: int,
) -> dict[str, Any] | None:
    spec = taxonomies.get(taxonomy)
    if not isinstance(spec, dict):
        return None
    kind = str(spec.get("type") or "")
    parts = [p for p in (path or "").split("/") if p]
    if kind == "attribute_tree":
        return _read_attribute_tree(
            graph,
            taxonomy=taxonomy,
            spec=spec,
            parts=parts,
            path=path,
            limit=limit,
        )
    if kind == "flat":
        return _read_flat(
            graph,
            taxonomy=taxonomy,
            spec=spec,
            path=path,
            limit=limit,
        )
    return None


def _read_attribute_tree(
    graph,
    *,
    taxonomy: str,
    spec: dict[str, Any],
    parts: list[str],
    path: str,
    limit: int,
) -> dict[str, Any]:
    subtype = str(spec["subtype"])
    group_levels = list(spec.get("group_levels") or [])
    if len(parts) < len(group_levels):
        level = dict(group_levels[len(parts)])
        attr = str(level["attr"])
        missing = str(level.get("missing") or "(missing)")
        filters, params = _filter_sql(
            group_levels[:len(parts)],
            parts,
            subtype=subtype,
            limit=limit,
        )
        params.update({"group_attr": attr, "group_missing": missing})
        rows = graph.query(
            f"""
            SELECT COALESCE(NULLIF(attrs->>:group_attr, ''),
                            :group_missing) AS value,
                   count(*) AS n
              FROM okg.graph_nodes
             WHERE subtype = :subtype
               {filters}
             GROUP BY value
             ORDER BY (value = :group_missing) ASC, n DESC, value
             LIMIT :limit
            """,
            params,
        )
        return {
            "items": [
                {
                    "id": row["value"],
                    "label": _group_label(row["value"], level),
                    "count": row["n"],
                    "leaf": False,
                }
                for row in rows
            ],
            "meta": {
                "taxonomy": taxonomy,
                "path": path,
                "level": str(level.get("id") or attr),
            },
        }

    if len(parts) == len(group_levels):
        leaf = dict(spec.get("leaf") or {})
        filters, params = _filter_sql(
            group_levels,
            parts,
            subtype=subtype,
            limit=limit,
        )
        order_attr = leaf.get("order_attr")
        order_sql = _order_sql(order_attr, str(leaf.get("order") or "asc"))
        rows = graph.query(
            f"""
            SELECT node_id, attrs
              FROM okg.graph_nodes
             WHERE subtype = :subtype
               {filters}
             {order_sql}
             LIMIT :limit
            """,
            params | ({"order_attr": str(order_attr)} if order_attr else {}),
        )
        return {
            "items": [
                _leaf_item(row["node_id"], row["attrs"] or {}, leaf)
                for row in rows
            ],
            "meta": {
                "taxonomy": taxonomy,
                "path": path,
                "level": str(leaf.get("id") or "leaf"),
            },
        }

    return {
        "items": [],
        "meta": {"taxonomy": taxonomy, "path": path, "level": "leaf"},
    }


def _read_flat(
    graph,
    *,
    taxonomy: str,
    spec: dict[str, Any],
    path: str,
    limit: int,
) -> dict[str, Any]:
    leaf = dict(spec.get("leaf") or {})
    subtype = str(spec["subtype"])
    params: dict[str, Any] = {
        "subtype": subtype,
        "limit": min(limit, int(spec.get("max_limit") or limit)),
    }
    require_attr = spec.get("require_attr")
    require_sql = ""
    if require_attr:
        require_sql = "AND NULLIF(attrs->>:require_attr, '') IS NOT NULL"
        params["require_attr"] = str(require_attr)
    order_attr = spec.get("order_attr") or leaf.get("order_attr")
    order_sql = _order_sql(order_attr, str(spec.get("order") or "asc"))
    if order_attr:
        params["order_attr"] = str(order_attr)
    rows = graph.query(
        f"""
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = :subtype
           {require_sql}
         {order_sql}
         LIMIT :limit
        """,
        params,
    )
    return {
        "items": [
            _leaf_item(row["node_id"], row["attrs"] or {}, leaf)
            for row in rows
        ],
        "meta": {
            "taxonomy": taxonomy,
            "path": path,
            "level": str(leaf.get("id") or "leaf"),
        },
    }


def _filter_sql(
    levels: list[dict[str, Any]],
    parts: list[str],
    *,
    subtype: str,
    limit: int,
) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = {
        "subtype": subtype,
        "limit": limit,
    }
    conditions: list[str] = []
    for idx, (level, value) in enumerate(zip(levels, parts, strict=True)):
        params[f"filter_attr_{idx}"] = str(level["attr"])
        params[f"filter_missing_{idx}"] = str(
            level.get("missing") or "(missing)"
        )
        params[f"filter_value_{idx}"] = value
        conditions.append(
            "AND COALESCE(NULLIF(attrs->>:filter_attr_{idx}, ''), "
            ":filter_missing_{idx}) = :filter_value_{idx}".format(
                idx=idx
            )
        )
    return "\n               ".join(conditions), params


def _order_sql(order_attr: Any, order: str) -> str:
    if not order_attr:
        return "ORDER BY node_id"
    direction = "DESC" if order.lower() == "desc" else "ASC"
    return f"ORDER BY attrs->>:order_attr {direction} NULLS LAST, node_id"


def _group_label(value: str, level: dict[str, Any]) -> str:
    prefix = str(level.get("label_prefix") or "")
    suffix = str(level.get("label_suffix") or "")
    return f"{prefix}{value}{suffix}"


def _leaf_item(node_id: str, attrs: dict[str, Any], leaf: dict[str, Any]) -> dict:
    label_attr = str(leaf.get("label_attr") or "")
    item = {
        "id": node_id,
        "label": attrs.get(label_attr) or node_id,
        "node_id": node_id,
        "count": None,
        "leaf": True,
    }
    for field in leaf.get("fields") or []:
        key = str(field)
        item[key] = attrs.get(key)
    return item
