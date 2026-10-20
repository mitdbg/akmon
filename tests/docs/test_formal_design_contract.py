from __future__ import annotations

import os
import re
import subprocess
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FORMAL_ROOT = ROOT / "formal"
CONTRACT_PATH = FORMAL_ROOT / "design-contract.md"
GAP_INVENTORY_PATH = FORMAL_ROOT / "gap-inventory.toml"
BOUNDARY_INVENTORY_PATH = FORMAL_ROOT / "boundary-inventory.toml"
MAKEFILE_PATH = ROOT / "Makefile"

DESIGN_FIELDS = (
    "Safety property:",
    "Authority boundary:",
    "Valid runtime paths:",
    "Out of scope:",
    "Preconditions/refinement:",
)
TAXONOMY = {
    "code_bug",
    "model_bug",
    "missing_formal_coverage",
    "boundary_precondition",
    "mapping_bug",
}
BOUNDARY_CLASSIFICATIONS = {
    "needs_formal_model",
    "runtime_test_boundary",
    "covered_indirectly",
    "obsolete_archive_only",
}
EXPECTED_OPEN_GAP_IDS = {
    "nomos.iam-scope-too-abstract",
}
EXPECTED_BOUNDARY_IDS = {
    "federation.mailbox_transport",
    "llm.embeddings_tracking",
    "security.sql_role_grants",
    "extraction.parser_alias_linker",
    "nomos.iam_scope",
    "console.legacy_operator_models",
}
ISSUE_337_BOUNDARY_IDS = {
    "federation.mailbox_transport",
    "llm.embeddings_tracking",
    "security.sql_role_grants",
    "extraction.parser_alias_linker",
}
EXPECTED_DIRECT_MODEL_IDS = {
    "merge_back.restamp_partition",
}
ISSUE_337_SURFACE_IDS = ISSUE_337_BOUNDARY_IDS | EXPECTED_DIRECT_MODEL_IDS
NEEDS_MODEL_GAP_CLASSIFICATIONS = {"missing_formal_coverage", "boundary_precondition"}
BOUNDARY_REQUIRED_FIELDS = {
    "id",
    "area",
    "title",
    "classification",
    "owner_surface",
    "rationale",
    "evidence_refs",
}
DIRECT_MODEL_REQUIRED_FIELDS = {
    "id",
    "area",
    "title",
    "owner_surface",
    "model_refs",
    "evidence_refs",
    "rationale",
}
BOUNDARY_REF_PREFIXES = (
    "archive/",
    "deployments/",
    "docs/",
    "formal/",
    "scripts/",
    "src/",
    "tests/",
)


def _direct_model_dirs() -> list[Path]:
    return sorted(
        path
        for path in FORMAL_ROOT.iterdir()
        if path.is_dir() and (any(path.glob("*.tla")) or any(path.glob("*.py")))
    )


def _section_text(text: str, heading: str) -> str:
    start = text.index(heading)
    next_heading = text.find("\n## ", start + len(heading))
    if next_heading == -1:
        return text[start:]
    return text[start:next_heading]


def _split_ref_path(ref: object) -> str:
    assert isinstance(ref, str), f"boundary ref must be a string, got {type(ref)}"
    return ref.split("::", 1)[0]


def _assert_existing_refs(refs: object, *, row_id: str) -> None:
    assert isinstance(refs, list), f"{row_id} refs must be a list"
    assert refs, f"{row_id} must name at least one ref"
    for ref in refs:
        rel = _split_ref_path(ref)
        assert rel.startswith(BOUNDARY_REF_PREFIXES), (
            f"{row_id} reference {rel!r} must use a repo-local path prefix"
        )
        assert (ROOT / rel).exists(), f"{row_id} references missing path {rel}"


def test_formal_design_contract_defines_authority_and_taxonomy() -> None:
    text = CONTRACT_PATH.read_text(encoding="utf-8")

    for heading in (
        "## Decision Authority",
        "## Required README Section",
        "## Mismatch Taxonomy",
        "## Preconditions And Refinement",
        "## Current Gap Inventory",
        "## Boundary Inventory",
    ):
        assert heading in text
    for field in DESIGN_FIELDS:
        assert f"`{field}`" in text
    for classification in TAXONOMY:
        assert f"`{classification}`" in text
    for classification in BOUNDARY_CLASSIFICATIONS:
        assert f"`{classification}`" in text
    for field in BOUNDARY_REQUIRED_FIELDS | {"gap_id", "model_refs"}:
        assert f"`{field}`" in text


def test_each_formal_readme_declares_design_contract() -> None:
    for model_dir in _direct_model_dirs():
        readme = model_dir / "README.md"
        text = readme.read_text(encoding="utf-8")

        assert "## Design Contract" in text, f"{readme} missing Design Contract"
        design_contract = _section_text(text, "## Design Contract")
        for field in DESIGN_FIELDS:
            assert field in design_contract, f"{readme} missing {field}"
        preconditions = design_contract.split("Preconditions/refinement:", 1)[1]
        assert (
            "formal/gap-inventory.toml" in design_contract
            or "No additional preconditions" in design_contract
            or re.search(r"\b(test|tests|runtime check|formal model)\b", preconditions)
        ), f"{readme} must bind preconditions to evidence or an inventory entry"


def test_gap_inventory_classifies_current_high_priority_findings() -> None:
    inventory = tomllib.loads(GAP_INVENTORY_PATH.read_text(encoding="utf-8"))
    gaps = inventory["gaps"]
    gap_ids = {gap["id"] for gap in gaps}

    assert inventory["schema"] == "okg.formal.gap-inventory.v1"
    assert len(gaps) >= len(EXPECTED_OPEN_GAP_IDS)
    for gap in gaps:
        assert gap["classification"] in TAXONOMY
        assert gap["priority"] in {"P1", "P2", "P3"}
        assert gap["area"]
        assert gap["title"]
        assert gap["owner_surface"]
        assert gap["recommended_verification"]
        assert re.match(r"^[a-z0-9_]+[.][a-z0-9_.-]+$", gap["id"])

    assert EXPECTED_OPEN_GAP_IDS <= gap_ids
    assert "poros.default-gate-misses-db-refinement" not in gap_ids
    assert "transform.catalog-version-precondition" not in gap_ids
    assert "coordination.gate-semantics-conflated" not in gap_ids
    assert "coordination.legacy-history-vacuous" not in gap_ids


def test_boundary_inventory_classifies_runtime_only_surfaces() -> None:
    inventory = tomllib.loads(BOUNDARY_INVENTORY_PATH.read_text(encoding="utf-8"))
    boundaries = inventory["boundaries"]
    direct_models = inventory["direct_models"]
    boundary_ids = {boundary["id"] for boundary in boundaries}
    direct_model_ids = {direct_model["id"] for direct_model in direct_models}

    assert inventory["schema"] == "okg.formal.boundary-inventory.v1"
    assert len(boundary_ids) == len(boundaries)
    assert len(direct_model_ids) == len(direct_models)
    assert boundary_ids.isdisjoint(direct_model_ids)
    assert EXPECTED_BOUNDARY_IDS <= boundary_ids
    assert ISSUE_337_BOUNDARY_IDS <= boundary_ids
    assert EXPECTED_DIRECT_MODEL_IDS <= direct_model_ids
    assert ISSUE_337_SURFACE_IDS <= boundary_ids | direct_model_ids

    for boundary in boundaries:
        boundary_id = boundary.get("id", "<missing id>")
        missing = BOUNDARY_REQUIRED_FIELDS - boundary.keys()
        assert not missing, f"{boundary_id} missing {sorted(missing)}"
        assert re.match(r"^[a-z0-9_]+[.][a-z0-9_.-]+$", boundary["id"])
        assert boundary["classification"] in BOUNDARY_CLASSIFICATIONS
        assert boundary["area"]
        assert boundary["title"]
        assert boundary["owner_surface"]
        assert boundary["rationale"]
        _assert_existing_refs(boundary["evidence_refs"], row_id=boundary["id"])
        if "formal_refs" in boundary:
            _assert_existing_refs(boundary["formal_refs"], row_id=boundary["id"])

    for direct_model in direct_models:
        direct_id = direct_model.get("id", "<missing id>")
        missing = DIRECT_MODEL_REQUIRED_FIELDS - direct_model.keys()
        assert not missing, f"{direct_id} missing {sorted(missing)}"
        assert re.match(r"^[a-z0-9_]+[.][a-z0-9_.-]+$", direct_model["id"])
        assert direct_model["area"]
        assert direct_model["title"]
        assert direct_model["owner_surface"]
        assert direct_model["rationale"]
        _assert_existing_refs(direct_model["model_refs"], row_id=direct_model["id"])
        _assert_existing_refs(direct_model["evidence_refs"], row_id=direct_model["id"])


def test_needs_formal_model_boundaries_have_gap_entries() -> None:
    boundaries = tomllib.loads(
        BOUNDARY_INVENTORY_PATH.read_text(encoding="utf-8")
    )["boundaries"]
    gaps = tomllib.loads(GAP_INVENTORY_PATH.read_text(encoding="utf-8"))["gaps"]
    gap_by_id = {gap["id"]: gap for gap in gaps}

    for boundary in boundaries:
        if boundary["classification"] == "needs_formal_model":
            gap_id = boundary.get("gap_id")
            assert gap_id, f"{boundary['id']} needs a gap_id"
            gap = gap_by_id.get(gap_id)
            assert gap is not None, f"{boundary['id']} missing gap entry {gap_id}"
            assert gap["area"] == boundary["area"]
            assert gap["classification"] in NEEDS_MODEL_GAP_CLASSIFICATIONS
            assert gap["owner_surface"]
            assert gap["recommended_verification"]
        else:
            assert "gap_id" not in boundary, (
                f"{boundary['id']} should not masquerade as a formal gap"
            )

        for related_gap_id in boundary.get("related_gap_ids", []):
            assert related_gap_id in gap_by_id


def test_precondition_gaps_have_verification_paths() -> None:
    inventory = tomllib.loads(GAP_INVENTORY_PATH.read_text(encoding="utf-8"))

    precondition_gaps = [
        gap
        for gap in inventory["gaps"]
        if gap["classification"] == "boundary_precondition"
    ]
    for gap in precondition_gaps:
        verification = gap["recommended_verification"].lower()
        assert any(
            keyword in verification
            for keyword in ("document", "prove", "test", "refinement", "gate")
        ), f"{gap['id']} does not name how the precondition is checked"


def test_verify_poros_requires_db_refinement_gate() -> None:
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")

    match = re.search(r"^verify-poros:\s*(?P<deps>.+)$", makefile, re.MULTILINE)
    assert match, "Makefile missing verify-poros target"
    deps = match.group("deps").split()
    assert "formal-poros-db-refinement" in deps
    assert "formal-poros-db-refinement-env:" in makefile


def test_poros_db_refinement_env_preflight_names_missing_inputs() -> None:
    base_env = dict(os.environ)
    for key in ("SENDER_DSN", "RECEIVER_DSN", "MESSAGE_ID"):
        base_env.pop(key, None)

    cases = (
        ({}, "SENDER_DSN is required for Poros DB refinement"),
        (
            {"SENDER_DSN": "sender"},
            "RECEIVER_DSN is required for Poros DB refinement",
        ),
        (
            {"SENDER_DSN": "sender", "RECEIVER_DSN": "receiver"},
            "MESSAGE_ID is required for Poros DB refinement",
        ),
    )
    for env, message in cases:
        completed = subprocess.run(
            ["make", "formal-poros-db-refinement-env"],
            cwd=ROOT,
            env=base_env | env,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode != 0
        assert message in completed.stderr

    completed = subprocess.run(
        ["make", "formal-poros-db-refinement-env"],
        cwd=ROOT,
        env=base_env
        | {
            "SENDER_DSN": "sender",
            "RECEIVER_DSN": "receiver",
            "MESSAGE_ID": "message",
        },
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0


def test_poros_readme_documents_strict_reports_and_runtime_helpers() -> None:
    text = (FORMAL_ROOT / "poros" / "README.md").read_text(encoding="utf-8")

    assert ".scratch/formal/poros/formal-report.yaml" in text
    assert "formal.poros.exchange-safety" in text
    assert "formal.poros.receipt-publication-nonvacuity" in text
    assert "BrokenReceiptImpliesPublication" in text
    assert "required runtime-to-model helper, not a TLC invariant" in text
    assert "SENDER_DSN" in text
    assert "RECEIVER_DSN" in text
    assert "MESSAGE_ID" in text


def test_design_contract_classifies_tlc_timeout_separately() -> None:
    text = (FORMAL_ROOT / "design-contract.md").read_text(encoding="utf-8")

    assert "## TLC Outcome Classification" in text
    assert "failure_class: timeout" in text
    assert "proof_status: not_freshly_proven" in text
    assert "failure_class: invariant_violation" in text
    assert "proof_status: semantic_failure" in text
    assert "proof_status: freshly_proven" in text
