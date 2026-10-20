"""#1791: the publish proof names the refusing cause and the republish.

No database: `assert_publish_proof` returns before any query for a publish
whose status is not `published`, so the hint contract is provable here.
"""
from __future__ import annotations

import json

from okg.substrate.publish_proof import assert_publish_proof
def test_publish_proof_names_the_refusing_cause_when_no_generation_exists() -> None:
    """#1791: a publish refused before a generation existed must not send the
    operator to `okg generation describe None`; it carries the publish's own
    cause and names the command that republishes the staged records."""
    refused = {
        "label": "commit",
        "status": "fail",
        "code": "substrate_error",
        "error": "repository provenance inputs are not committed",
        "error_data": {"repository": "/dep", "paths": ["?? r1/deployment.yaml"]},
    }
    proof = assert_publish_proof(refused, dsn="postgresql://unused")

    assert proof["ok"] is False
    assert proof["generation_id"] is None
    assert proof["reason"] == "publish generation None was not published"
    assert proof["publish_error"] == "repository provenance inputs are not committed"
    assert proof["publish_code"] == "substrate_error"
    assert proof["publish_error_data"] == refused["error_data"]
    assert "before a generation existed" in proof["fix_hint"]
    assert "okg ingest --deployment <name> --json" in proof["fix_hint"]
    assert "generation describe" not in proof["fix_hint"]
    json.dumps(proof)


def test_publish_proof_appends_the_republish_to_the_publish_own_hint() -> None:
    """A refusal that already names its repair (manifest drift) keeps that
    hint first and still names the republish; a blocked generation with an
    id keeps the describe hint."""
    drifted = {
        "status": "fail",
        "code": "catalog_manifest_drifted",
        "error": "deployment manifest content differs from the claimed identity",
        "fix_hint": "re-claim ownership: uv run okg catalog ownership claim --deployment /dep --json",
    }
    proof = assert_publish_proof(drifted, dsn="postgresql://unused")
    assert proof["publish_code"] == "catalog_manifest_drifted"
    assert proof["fix_hint"].startswith(drifted["fix_hint"] + "; then run `okg ingest")
    assert "republishes the staged records" in proof["fix_hint"]

    blocked = {"status": "blocked", "generation_id": "okg:dep:branch:default:gen:1:x"}
    proof = assert_publish_proof(blocked, dsn="postgresql://unused")
    assert proof["fix_hint"] == (
        "run `okg generation describe <g>` and fix the blocking invariant"
    )
