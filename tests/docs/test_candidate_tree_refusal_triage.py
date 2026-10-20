"""Rot-gate: the triage runbook stays in step with the candidate-tree refusals.

PACT ``fix-effect-inventory-tracked-symlinks`` /
``req.fix-effect-inventory-tracked-symlinks.containment-still-refused``: every
refusal the candidate-tree projection can raise must reach the operator with a
remedy. Issue #1012 was hard to diagnose precisely because the refusal named a
file and nothing else, so the message text alone is not the whole contract --
the triage runbook has to carry the same list, and the bound it quotes has to be
the bound the code enforces.

The completeness half is the point. A hand-kept list cannot notice a refusal
that is missing from *both* the runbook and the list, so this gate parses the
projection's own functions with ``ast`` and requires every ``raise ValueError``
in them to be registered below -- as operator-facing (and therefore documented)
or as an explicitly-justified planner-internal invariant. Adding a refusal
without classifying it turns the gate red.

Pairs with the behavioral coverage in
tests/substrate/test_deployment_upgrade.py (``test_candidate_tree_*``), which
proves each refusal actually fires and carries a member name and a remedy. This
test proves the operator can find out what to do about it.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TRIAGE = ROOT / "docs" / "runbooks" / "troubleshooting" / "first-run.md"
SOURCE = ROOT / "src" / "okg" / "substrate" / "deployment_upgrade.py"
BEHAVIORAL_TESTS = ROOT / "tests" / "substrate" / "test_deployment_upgrade.py"

# The functions that make up the candidate-tree projection. A refusal raised
# anywhere in here is in scope for this gate.
PROJECTION_FUNCTIONS = frozenset({
    "_candidate_link_target",
    "_scan_candidate_archive_members",
    "_extract_candidate_tree",
    "_candidate_target_covered",
    "_require_no_surviving_symlinks",
    "_claim_candidate_member",
    "_require_link_target_authority",
    "_require_tracked_link_targets",
    "_materialize_candidate_link",
})

_SECTION_RE = re.compile(
    r"\n## 6b\. Upgrade stops at `backup`.*?(?=\n## )",
    re.DOTALL,
)

# Operator-facing refusals: something in the operator's tree caused this, and
# the runbook must say so. Each entry is (phrase a test asserts on, phrase the
# runbook must carry).
OPERATOR_REFUSALS = (
    ("pointing outside the repository", "outside the repository"),
    ("pointing at itself or its own parent", "points at itself or its own parent"),
    ("not tracked at the planned revision", "not tracked at the planned revision"),
    ("symlink chain longer than", "chain longer than"),
    ("does not resolve to a tracked path", "does not resolve to a tracked path"),
    ("neither a file, a directory, nor a", "neither a file, a directory, nor a symlink"),
    ("crosses repository authority", "crosses repository authority"),
    ("which is neither a file nor a directory", "neither a file nor a directory"),
    (
        "lands on a path the candidate tree already holds",
        "lands on a path the candidate tree already holds",
    ),
)

# Planner-internal invariants: these fire only if the projection itself is
# broken, so there is nothing for an operator to change. Each needs a reason.
INTERNAL_REFUSALS = {
    "git archive of": "a git that returns non-bytes is a broken toolchain, not a tree defect",
    "was claimed without being recorded": "closure bookkeeping invariant; unreachable by construction",
    "left unresolved symlinks": "post-condition sweep; names itself a planner bug",
    "contains unsafe archive member": "git cannot emit such a name; kept as defense in depth",
}


def _section() -> str:
    match = _SECTION_RE.search(TRIAGE.read_text())
    assert match, "first-run.md lost the effect_inventory section"
    return match.group(0)


def _raised_messages() -> list[str]:
    """The leading literal text of every `raise ValueError` in the projection."""
    tree = ast.parse(SOURCE.read_text())
    messages: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        if node.name not in PROJECTION_FUNCTIONS:
            continue
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Raise):
                continue
            exc = inner.exc
            if not isinstance(exc, ast.Call) or not exc.args:
                continue
            if getattr(exc.func, "id", None) != "ValueError":
                continue
            messages.append(_literal_text(exc.args[0]))
    return messages


def _literal_text(node: ast.AST) -> str:
    """Flatten a message expression to the literal text it always contains.

    Messages are built by implicit concatenation of plain strings and f-strings;
    the interpolated values differ per call, the literal chunks do not.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            part.value
            for part in node.values
            if isinstance(part, ast.Constant) and isinstance(part.value, str)
        )
    if isinstance(node, ast.BinOp):
        return _literal_text(node.left) + _literal_text(node.right)
    return ""


def test_every_projection_refusal_is_classified() -> None:
    """No refusal can be added without being documented or justified."""
    known = [phrase for phrase, _doc in OPERATOR_REFUSALS] + list(INTERNAL_REFUSALS)
    unclassified = [
        message
        for message in _raised_messages()
        if not any(phrase in message for phrase in known)
    ]
    assert not unclassified, (
        "candidate-tree refusals are not classified as operator-facing or "
        f"internal: {unclassified}. Add each to OPERATOR_REFUSALS (and to "
        "docs/runbooks/troubleshooting/first-run.md) or to INTERNAL_REFUSALS with a reason."
    )


def test_the_gate_sees_the_refusals_it_claims_to() -> None:
    """Guard against a parse that silently returns nothing."""
    messages = _raised_messages()
    assert len(messages) >= len(OPERATOR_REFUSALS), (
        f"ast walk found only {len(messages)} refusals; the function list is "
        "probably stale"
    )


def test_triage_section_covers_every_operator_refusal() -> None:
    section = _section().lower()
    for phrase, documented in OPERATOR_REFUSALS:
        assert documented.lower() in section, (
            f"triage runbook does not cover refusal: {phrase}"
        )

    # Each documented refusal must still be one a test asserts actually fires.
    # Binding to the assertions rather than to the raise sites keeps the gate
    # honest without being brittle about how a message is concatenated.
    proof = BEHAVIORAL_TESTS.read_text()
    for phrase, _documented in OPERATOR_REFUSALS:
        assert f'"{phrase}"' in proof, (
            f"no test asserts the refusal the runbook documents: {phrase}"
        )


def test_triage_section_quotes_the_enforced_link_bound() -> None:
    """The documented bound is the bound the code enforces.

    Round 0 is spent on the pathspec itself, so the planner follows
    ``CANDIDATE_TREE_LINK_ROUNDS - 1`` links. Operators count links, not
    rounds, so the runbook has to state the link count.
    """
    from okg.substrate import deployment_upgrade as du

    spelled = {3: "three", 5: "five", 7: "seven", 9: "nine", 11: "eleven", 15: "fifteen"}
    links = du.CANDIDATE_TREE_LINK_ROUNDS - 1
    assert links in spelled, (
        f"CANDIDATE_TREE_LINK_ROUNDS={du.CANDIDATE_TREE_LINK_ROUNDS} implies "
        f"{links} links, which has no spelling here; add it and update "
        "docs/runbooks/troubleshooting/first-run.md in the same diff"
    )
    section = _section().lower()
    assert f"{spelled[links]} links" in section, (
        f"triage runbook does not state the enforced bound ({links} links)"
    )


def test_triage_section_tells_the_operator_to_commit_the_fix() -> None:
    """The trap from #1012: the projection reads the COMMITTED tree.

    Removing the symlink in the working tree changes nothing, so the first
    correct diagnosis looks wrong and gets abandoned. The runbook has to say
    both halves -- what is read, and that editing the other one is a no-op.
    """
    section = _section().lower()
    assert "committed tree" in section, (
        "triage runbook does not say the projection reads the committed tree"
    )
    assert "working tree" in section and "does nothing" in section, (
        "triage runbook does not warn that a working-tree-only fix is a no-op"
    )
