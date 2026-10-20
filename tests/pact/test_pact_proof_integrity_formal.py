"""The PACT formal binding covers all three completion preconditions."""

import json
import subprocess
import sys
from pathlib import Path

from _v5_formal import carried_bindings


REPO_ROOT = Path(__file__).resolve().parents[2]
CHANGE = REPO_ROOT / "pact" / "archive" / "pact-acceptance-prose-is-enforceable"


def test_model_names_each_proof_integrity_invariant() -> None:
    model = (REPO_ROOT / "formal" / "pact" / "PactProofIntegrity.tla").read_text()
    config = (REPO_ROOT / "formal" / "pact" / "PactProofIntegrity.cfg").read_text()

    for invariant in (
        "CompletionRequiresBoundArms",
        "CompletionRequiresIntegrationOwner",
        "CompletionRequiresNamedTestExecution",
        "ProofIntegrity",
    ):
        assert invariant in model
        assert invariant in {line.strip() for line in config.splitlines()}


def test_formal_binding_maps_model_to_its_change() -> None:
    """The binding survives the v5 cutover as a carried receipt.

    v5 has no `formal[]`, so the property/module link is read from the
    `formal_check` evidence row `pact migrate` wrote. The refinement-test list
    was not carried; the tests it named are asserted to exist here instead.
    """
    binding = carried_bindings("pact-acceptance-prose-is-enforceable")[
        "PactProofIntegrity"
    ]

    assert binding.module == "formal/pact/PactProofIntegrity.tla"
    assert (REPO_ROOT / "formal" / "pact" / "PactProofIntegrity.cfg").is_file()
    for name in (
        "test_acceptance_arm_binding.py",
        "test_integration_acceptance_validation.py",
        "test_slow_verification_commands.py",
    ):
        assert (REPO_ROOT / "tests" / "pact" / name).is_file(), name


def test_checker_is_registered_in_the_gating_formal_suite() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/run_formal_suite.py", "--list"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    registry = json.loads(result.stdout)
    checker = next(
        row
        for row in registry["checkers"]
        if row["name"] == "check_formal_pact_proof_integrity.py"
    )

    assert checker["kind"] == "tlc"
    assert checker["smoke_is_full_model"] is True
    assert "check_formal_pact_proof_integrity.py" in registry["registered"]
    assert "check_formal_pact_proof_integrity.py" not in registry["orphans"]
