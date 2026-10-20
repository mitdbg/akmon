from __future__ import annotations

import pytest

pytest.importorskip("mcp")

from okg.substrate.mcp.server import add_pagination_hint  # noqa: E402


def test_truncated_payload_includes_actionable_next_offset_hint() -> None:
    payload = {
        "operator_name": "search",
        "results": [{"node_id": "chunk:1"}],
        "truncated": True,
        "limit": 10,
        "offset": 10,
        "next_offset": 20,
    }

    out = add_pagination_hint(payload, "search")

    assert out["pagination"] == {
        "truncated": True,
        "offset": 10,
        "limit": 10,
        "next_offset": 20,
        "operator": "search",
        "parameter": "offset",
    }
    assert "call search again with offset=20" in out["pagination_hint"]


def test_truncated_payload_derives_next_offset_from_limit_and_offset() -> None:
    payload = {
        "operator_name": "filter",
        "items": [{"node_id": "chunk:1"}],
        "truncated": True,
        "limit": 25,
        "offset": 50,
    }

    out = add_pagination_hint(payload, "filter")

    assert out["next_offset"] == 75
    assert out["pagination"]["next_offset"] == 75
