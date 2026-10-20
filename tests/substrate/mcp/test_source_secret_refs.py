"""Source secret-reference handling (require-secret-refs-not-literals).

api_key-class source params must be secret references, never literals: the
source-lifecycle gate rejects literals, param echoes are redacted, and a
value-free doctor scan finds existing literal secrets in source registries.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.mcp.source_lifecycle import (
    _literal_secret_param_keys,
    _param_is_secret_ref,
    _redact_params,
)
from okg.substrate.nomos.doctor import scan_source_registry_literal_secrets


# ---- secret-ref classification + gate ----


def test_param_is_secret_ref_accepts_references_and_rejects_literals():
    assert _param_is_secret_ref("secret://dep/openai") is True
    assert _param_is_secret_ref("vault://x") is True
    assert _param_is_secret_ref({"secret_ref": "secret://dep/x"}) is True
    assert _param_is_secret_ref("sk-literal-key") is False
    assert _param_is_secret_ref({"value": "sk-literal"}) is False


def test_literal_secret_param_keys_flags_only_literal_secrets():
    params = {
        "api_key": "sk-literal",          # literal secret -> flagged
        "access_key": "AKIA...",          # literal secret -> flagged
        "client_secret": "secret://d/x",  # reference -> ok
        "notes_root": "/data/notes",      # not a secret key -> ok
    }
    flagged = set(_literal_secret_param_keys(params))
    assert flagged == {"api_key", "access_key"}


def test_literal_secret_param_keys_empty_when_all_refs_or_nonsecret():
    params = {
        "api_key": {"secret_ref": "secret://d/x"},
        "endpoint": "https://api.example.com",
    }
    assert _literal_secret_param_keys(params) == []


# ---- redaction of param echoes ----


def test_redact_params_redacts_secrets_keeps_benign():
    out = _redact_params({
        "api_key": "sk-should-not-appear",
        "token": "tok-should-not-appear",
        "password": "p",
        "notes_root": "/data/notes",
        "endpoint": "https://api.example.com",
    })
    assert out["api_key"] == "<redacted>"
    assert out["token"] == "<redacted>"
    assert out["password"] == "<redacted>"
    assert out["notes_root"] == "/data/notes"
    assert out["endpoint"] == "https://api.example.com"
    assert "sk-should-not-appear" not in str(out)


@pytest.mark.parametrize("key", [
    # The expanded SECRET_KEY_MARKERS the doctor scan detects. These were NOT
    # matched by the old redaction key-regex, so a SHORT (non-PAT-shaped) literal
    # leaked in plaintext through MCP envelopes. Redaction must be at least as
    # wide as detection (adversarial-review remediation).
    "private_key", "credential", "access_key", "accesskey", "apikey",
    "passphrase", "connection_string", "bearer",
    "openai_credential", "db_access_key",
])
def test_redact_params_covers_full_detector_vocabulary(key):
    # A short, non-PAT-shaped value (would slip past the PAT-length heuristic).
    leak = "hunter2"
    out = _redact_params({key: leak, "host": "db.local"})
    assert out[key] == "<redacted>", f"{key} value leaked: {out[key]!r}"
    assert leak not in str(out)
    assert out["host"] == "db.local"  # benign keys untouched


def test_redaction_is_at_least_as_wide_as_detection():
    # Mechanical guard against future drift: every shared detector marker, as a
    # bare key, must redact.
    from okg.substrate.nomos.core import SECRET_KEY_MARKERS

    for marker in SECRET_KEY_MARKERS:
        out = _redact_params({marker: "shortval"})
        assert out[marker] == "<redacted>", (
            f"detector marker {marker!r} is not redacted — redaction has drifted "
            "narrower than detection"
        )


# ---- value-free doctor registry scan ----


def test_scan_source_registry_literal_secrets_is_value_free(tmp_path):
    # Two deployments: one with a literal api_key, one with a secret_ref.
    d1 = tmp_path / "dep-literal"
    (d1).mkdir()
    (d1 / "source_registry.yaml").write_text(
        "sources:\n"
        "  llm_a:\n"
        "    params:\n"
        "      api_key: sk-LITERAL-SECRET\n"
        "      notes_root: /data/notes\n"
    )
    d2 = tmp_path / "dep-ref"
    (d2).mkdir()
    (d2 / "source_registry.yaml").write_text(
        "sources:\n"
        "  llm_b:\n"
        "    params:\n"
        "      api_key: secret://dep-ref/openai\n"
    )

    report = scan_source_registry_literal_secrets(tmp_path)
    assert report["count"] == 1
    loc = report["locations"][0]
    assert loc == {
        "deployment": "dep-literal", "source_id": "llm_a", "config_key": "api_key",
    }
    # The literal secret value never appears in the serialized report.
    assert "sk-LITERAL-SECRET" not in str(report)


def test_scan_clean_registry_reports_zero(tmp_path):
    d = tmp_path / "dep"
    d.mkdir()
    (d / "source_registry.yaml").write_text(
        "sources:\n  s:\n    params:\n      root: /x\n"
    )
    assert scan_source_registry_literal_secrets(tmp_path)["count"] == 0


# ---- agent_hint instructs a secret reference, not a raw key ----


def test_llm_agent_hint_instructs_secret_reference():
    src = Path("src/okg/substrate/mcp/source_lifecycle.py").read_text()
    # The kind=='llm' awaiting-params hint must point at a secret:// reference
    # and must NOT instruct passing a raw api_key literal.
    assert 'params={\\"api_key\\": \\"secret://' in src
    assert 'add_source` with `params={\\"api_key\\": \\"...\\"}`' not in src
