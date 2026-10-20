"""Server-owned admission identity (harden-mcp-write-boundaries).

A write-mode MCP caller cannot forge the deciding authority or self-elevate to
an auto-accept admission mode. The deciding actor is server-derived (launch
consent), and a caller-asserted fast_track is downgraded to review.
"""
from __future__ import annotations

import importlib

# The package may re-export `server`; import the real module object.
_sm = importlib.import_module("okg.substrate.mcp.server")
_launch_consent_actor = _sm._launch_consent_actor
_sanitize_caller_admission_mode = _sm._sanitize_caller_admission_mode


def test_decide_actor_is_server_derived(monkeypatch):
    monkeypatch.delenv("OKG_OPERATOR_EMAIL", raising=False)
    # The function takes no caller input at all -> cannot be forged.
    assert _launch_consent_actor("okg-workspace") == \
        "operator:mcp-launch-consent:okg-workspace"


def test_decide_actor_uses_configured_operator(monkeypatch):
    monkeypatch.setenv("OKG_OPERATOR_EMAIL", "riley@example.org")
    assert _launch_consent_actor("d") == "operator:riley@example.org"


def test_caller_fast_track_in_producer_policy_downgraded():
    out = _sanitize_caller_admission_mode(
        {"producer_policy": {"admission_mode": "fast_track", "trust_label": "x"}}
    )
    assert out["producer_policy"]["admission_mode"] == "review"
    # Other fields are preserved.
    assert out["producer_policy"]["trust_label"] == "x"


def test_caller_fast_track_top_level_downgraded():
    out = _sanitize_caller_admission_mode({"admission_mode": "fast_track"})
    assert out["admission_mode"] == "review"


def test_caller_fast_track_in_admission_policy_downgraded():
    out = _sanitize_caller_admission_mode(
        {"admission_policy": {"admission_mode": "FAST_TRACK"}}
    )
    assert out["admission_policy"]["admission_mode"] == "review"


def test_review_mode_is_unchanged():
    out = _sanitize_caller_admission_mode(
        {"producer_policy": {"admission_mode": "review"}}
    )
    assert out["producer_policy"]["admission_mode"] == "review"


def test_empty_authority_is_safe():
    assert _sanitize_caller_admission_mode({}) == {}
