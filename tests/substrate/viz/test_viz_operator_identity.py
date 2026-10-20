"""Server-derived operator identity for viz governance/overlay routes
(secure-viz-operator-routes).

A caller-supplied reviewer/actor_id in the request body must NOT be honored:
governance + overlay attribution comes only from the server-resolved operator
principal, so a request body cannot impersonate another operator.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import okg.substrate.pact.operations as pact_ops

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from okg.viz.app import build_app  # noqa: E402


class _Ctx:
    deployment_name = "demo"


def test_pact_review_reviewer_is_server_derived_not_body(monkeypatch):
    monkeypatch.setenv("OKG_OPERATOR_NAME", "RealOperator")
    monkeypatch.setenv("OKG_OPERATOR_EMAIL", "real@op")
    captured: dict[str, str] = {}

    def fake_approve(root, change_id, *, reviewer, decision, notes=None):
        captured["reviewer"] = reviewer
        return SimpleNamespace(evidence=[])

    monkeypatch.setattr(pact_ops, "approve_intent", fake_approve)
    # No operator token configured -> a LOOPBACK peer is trusted in local dev
    # (a non-loopback peer would be refused; that fail-closed default is covered
    # in test_viz_operator_auth.py). Here we only assert reviewer derivation.
    client = TestClient(
        build_app(ctx_provider=_Ctx, is_ready=lambda: True),
        client=("127.0.0.1", 40000),
    )

    resp = client.post(
        "/viz/api/console/pact-review",
        json={"change_id": "c1", "decision": "approved", "reviewer": "attacker"},
    )
    assert resp.status_code == 200, resp.text
    # The body-supplied 'attacker' reviewer is ignored; the server principal wins.
    assert captured["reviewer"] == "RealOperator"
    assert resp.json()["reviewer"] == "RealOperator"


def test_lens_routes_do_not_honor_body_actor_id():
    # Structural guard: the overlay revoke + lens putback call sites use the
    # server-derived actor, not body.actor_id.
    src = Path("src/okg/viz/endpoints.py").read_text()
    assert "actor_id=body.actor_id" not in src
    assert src.count("actor_id=_operator_actor_id()") >= 2
