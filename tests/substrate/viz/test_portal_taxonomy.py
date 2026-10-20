from __future__ import annotations

from okg.viz.portal_taxonomy import read_portal_tree


class FakeGraph:
    """Stand-in for a Chronos checkout session.

    ``read_portal_tree`` now runs against ``graph.query(sql, params)``
    (dict rows over ``okg.graph_nodes``) instead of a raw cursor over
    ``okg.nodes_live``. The fake returns dict rows keyed by the
    converted queries' output columns (``value``/``n`` for the group
    aggregate; ``node_id``/``attrs`` for leaf/flat reads).
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def query(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "GROUP BY value" in sql:
            return [{"value": "ops", "n": 2}]
        return [
            {
                "node_id": "doc:1",
                "attrs": {
                    "doc_number": "DOC 1",
                    "title": "Example",
                    "status": "current",
                    "published_at": "2026-05-03",
                },
            }
        ]


def test_attribute_tree_uses_manifest_levels_for_groups_and_leaves() -> None:
    graph = FakeGraph()
    taxonomies = {
        "by_owner": {
            "type": "attribute_tree",
            "subtype": "document",
            "group_levels": [
                {"id": "owner", "attr": "owner", "missing": "(none)"},
            ],
            "leaf": {
                "id": "doc",
                "label_attr": "doc_number",
                "fields": ["title", "status"],
                "order_attr": "doc_number",
            },
        }
    }

    root = read_portal_tree(
        graph,
        taxonomies=taxonomies,
        taxonomy="by_owner",
        path="",
        limit=10,
    )
    assert root == {
        "items": [{"id": "ops", "label": "ops", "count": 2, "leaf": False}],
        "meta": {"taxonomy": "by_owner", "path": "", "level": "owner"},
    }

    leaf = read_portal_tree(
        graph,
        taxonomies=taxonomies,
        taxonomy="by_owner",
        path="ops",
        limit=10,
    )
    assert leaf == {
        "items": [
            {
                "id": "doc:1",
                "label": "DOC 1",
                "node_id": "doc:1",
                "count": None,
                "leaf": True,
                "title": "Example",
                "status": "current",
            }
        ],
        "meta": {"taxonomy": "by_owner", "path": "ops", "level": "doc"},
    }


def test_flat_taxonomy_uses_manifest_fields() -> None:
    graph = FakeGraph()
    taxonomies = {
        "recent": {
            "type": "flat",
            "subtype": "document",
            "require_attr": "published_at",
            "order_attr": "published_at",
            "order": "desc",
            "leaf": {
                "id": "doc",
                "label_attr": "doc_number",
                "fields": ["title", "published_at"],
            },
        }
    }

    tree = read_portal_tree(
        graph,
        taxonomies=taxonomies,
        taxonomy="recent",
        path="",
        limit=10,
    )
    assert tree["items"][0]["label"] == "DOC 1"
    assert tree["items"][0]["published_at"] == "2026-05-03"
    assert tree["meta"] == {"taxonomy": "recent", "path": "", "level": "doc"}
