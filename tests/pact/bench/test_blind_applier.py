"""The blind applier is blind, and refuses rather than guesses.

`pact-self-repairing-refusals` req.refusal.executable, AUTHOR-ACTION arm. The
claim under test is that a manifest-content refusal carries enough to apply its
own repair. An applier that could read the models, the validator or the schema
would prove something weaker — that the SYSTEM knows the repair — so two
properties are pinned here rather than assumed:

1. the import graph. Not "it happens not to call them today": the module's own
   AST may not import `okg` at all;
2. no `legal_values[0]`. An underdetermined descriptor fails loudly. A fallback
   pick would clear the code while asserting something nobody checked, which is
   the same defect class as a fabricated proof row.
"""
from __future__ import annotations

import ast
import copy
from pathlib import Path

import pytest
import yaml

from okg.substrate.pact import repair
from okg.substrate.pact.bench import applier

MODULE = Path(applier.__file__)


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_the_applier_imports_nothing_from_okg() -> None:
    """The fence, checked on the AST — including function-local imports.

    A deferred `from okg.substrate.pact.models import ...` inside a helper
    reads as "no import" to a header-only check and would let the applier
    consult the very models whose completeness it is supposed to measure.
    """
    offenders = sorted(
        name for name in _imported_modules(MODULE) if name.split(".")[0] == "okg"
    )
    assert offenders == [], offenders


def test_the_applier_cutoff_mirrors_the_emitters(tmp_path: Path) -> None:
    """Two constants that must agree, pinned so the mirror cannot drift.

    The applier cannot import `repair.NEAR_MISS_CUTOFF` (see the fence above),
    so it restates it. A test is what keeps a restatement honest.
    """
    assert applier.MATCH_CUTOFF == repair.NEAR_MISS_CUTOFF


def test_an_underdetermined_descriptor_fails_instead_of_picking_the_first() -> None:
    """No `legal_values[0]`, in either shape it could take."""
    document = {"requirements": [{"evidence_policy": {"required": []}}]}

    with pytest.raises(applier.Underdetermined):
        applier.apply_edit(
            copy.deepcopy(document),
            {
                "code": "missing_evidence_policy",
                "pointer": "requirements[0].evidence_policy.required",
                "offending_value": [],
                "legal_values": ["executed_test", "design_review", "graph_report"],
            },
        )

    # ...and a value with no close match in a multi-member set is equally
    # underdetermined: "it starts with the same letter" is not a correction.
    with pytest.raises(applier.Underdetermined):
        applier.apply_edit(
            {"evidence": [{"kind": "rogue_kind"}]},
            {
                "code": "unknown_evidence_kind",
                "pointer": "evidence[0].kind",
                "offending_value": "rogue_kind",
                "legal_values": ["executed_test", "design_review", "graph_report"],
            },
        )


def test_a_descriptor_with_one_legal_value_determines_the_edit() -> None:
    document = {"tasks": [{"requirements": []}]}
    applied = applier.apply_edit(
        document,
        {
            "code": "task_requirements_link",
            "pointer": "tasks[0].requirements",
            "offending_value": [],
            "legal_values": ["req.one"],
        },
    )
    assert document["tasks"][0]["requirements"] == ["req.one"]
    assert applied.before == []
    assert applied.after == ["req.one"]


def test_a_single_close_match_determines_a_replacement() -> None:
    document = {"evidence": [{"refs": ["req.does-not-exist"]}]}
    applier.apply_edit(
        document,
        {
            "code": "unknown_evidence_ref",
            "pointer": "evidence[0].refs",
            "offending_value": "req.does-not-exist",
            "legal_values": ["req.one", "task.one", "test.req-one.probe"],
        },
    )
    assert document["evidence"][0]["refs"] == ["req.one"]


def test_an_id_addressed_pointer_is_refused_not_guessed() -> None:
    """`tasks.task.one.requirements` is unparseable BY DESIGN.

    Ids contain dots, so a walker cannot tell a key from an id. The emitter
    publishes the index form; a blind applier that tried to segment this would
    be guessing about which prefix is the id.
    """
    with pytest.raises(applier.Underdetermined):
        applier.parse_pointer("tasks.task.one.requirements[0")


def test_a_partial_apply_leaves_the_file_untouched(tmp_path: Path) -> None:
    """Half a repair produces a manifest nobody intended."""
    path = tmp_path / "pact.yaml"
    document = {"tasks": [{"requirements": []}], "requirements": []}
    original = yaml.safe_dump(document, sort_keys=False)
    path.write_text(original, encoding="utf-8")

    result, _document = applier.apply_to_file(
        path,
        [
            {
                "code": "task_requirements_link",
                "pointer": "tasks[0].requirements",
                "offending_value": [],
                "legal_values": ["req.one"],
            },
            {
                "code": "missing_evidence_policy",
                "pointer": "requirements",
                "offending_value": [],
                "legal_values": [],
            },
        ],
    )

    assert result.ok is False
    assert [row.reason for row in result.failed] == [applier.UNDERDETERMINED]
    assert path.read_text(encoding="utf-8") == original
