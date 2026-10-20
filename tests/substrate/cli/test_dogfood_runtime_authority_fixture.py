from __future__ import annotations

import json
from pathlib import Path
from typing import Any


FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "dogfood_runtime_split_authority.json"
)


def _strings(value: Any) -> list[str]:
    if isinstance(value, dict):
        return [
            item
            for child in value.values()
            for item in _strings(child)
        ]
    if isinstance(value, list):
        return [item for child in value for item in _strings(child)]
    return [value] if isinstance(value, str) else []


def test_split_authority_fixture_is_redacted_and_binds_canonical_proof(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.baseline test.runtime-authority.baseline",
    )
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))

    assert payload["schema"] == "okg.runtime_authority.split_fixture.v1"
    assert payload["deployment"] == "okg-workspace"
    assert payload["severity"] == "p0"

    authorities = payload["authorities"]
    assert set(authorities) == {
        "canonical_runtime",
        "request_worktree",
        "supervisor_before_reconcile",
    }
    canonical = authorities["canonical_runtime"]
    assert canonical["database_state"] == "available"
    for name in ("request_worktree", "supervisor_before_reconcile"):
        observed = authorities[name]
        assert observed["working_directory"] != canonical["working_directory"]
        assert observed["database_identity"] != canonical["database_identity"]
        assert observed["code_revision"] != canonical["code_revision"]

    proof = payload["proof_binding"]
    assert proof["reported_status"] == "published"
    assert proof["present_in_canonical_database"] is False
    assert proof["canonical_match_count"] == 0

    assert [row["code"] for row in payload["failure_chain"]] == [
        "supervisor_definition_drift",
        "replacement_ack_timeout",
        "release_schema_skew_refusal",
        "structured_error_serialization_failed",
        "proof_generation_absent_from_canonical_runtime",
    ]
    recovery = payload["recovery_observation"]
    assert recovery["eventual_generation_status"] == "published"
    assert recovery["trust_level_after_publish"] == "stale_context"
    assert recovery["classification"] == (
        "eventual_progress_without_coherent_authority"
    )

    serialized = json.dumps(payload, sort_keys=True)
    lowered = serialized.lower()
    assert "postgres://" not in lowered
    assert "postgresql://" not in lowered
    assert "password=" not in lowered
    assert "/users/" not in lowered
    assert payload["redaction"] == {
        "credentials_present": False,
        "dsn_present": False,
        "literal_user_home_paths_present": False,
        "retired_database_name_present": False,
    }
