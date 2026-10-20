"""PACT ``concord-system-stress`` — req.stress.semantics, task.stress.semantics.

Consistency semantics, measured in the ONLY honest direction available.

The capability census established that the four policy modes — strict, bounded,
eventual, lazy — are validated by the specification loader and consumed by
NOTHING else in the source tree. They therefore cannot be stress-tested
behaviourally, and this module does not pretend otherwise. What it delivers is
a CONFORMANCE measurement: which declared semantics are expressible, which are
inexpressible, and which are indistinguishable from one another by any
observable behaviour of the running pipeline.

The distinction that matters
----------------------------
EXPRESSIBLE   the specification accepts it and two specs differing only in that
              mode are different objects with different digests.
DISTINGUISHABLE
              some code outside the loader reads the mode, so the running
              system can behave differently because of it.

A mode can be expressible and indistinguishable at the same time — and all four
currently are. That is the finding: the specification can SAY four different
things about how hard to try, and the pipeline does the same thing for all
four.
"""
from __future__ import annotations

import ast
import copy
import pathlib

import pytest

from okg.substrate.consistency.invariant_spec import (
    MODES,
    MODE_BOUNDED,
    MODE_LAZY,
    load_invariant_spec,
)
from okg.substrate.consistency.violation_identity import WitnessParserRegistry
from tests.substrate.consistency._invariant_spec_support import base_spec, build_registry

SRC = pathlib.Path(__file__).resolve().parents[3] / "src"
LOADER_MODULE = "okg/substrate/consistency/invariant_spec.py"


@pytest.fixture()
def registry_and_digests():
    return build_registry()


@pytest.fixture()
def parsers() -> WitnessParserRegistry:
    registry = WitnessParserRegistry()
    registry.register(
        "parser://section-header@v1", "regex",
        lambda text: tuple(
            line[len("SECTION: "):] for line in text.splitlines()
            if line.startswith("SECTION: ")
        ),
    )
    return registry


def _load(spec, registry, parsers):
    return load_invariant_spec(
        spec, program_resolver=registry.resolve, parser_registry=parsers
    )


def _mode_spec(digests, mode: str) -> dict:
    """The same specification, differing ONLY in one invariant's policy mode."""
    spec = copy.deepcopy(base_spec(digests))
    policy = spec["invariants"][0]["policy"]
    policy.clear()
    policy["mode"] = mode
    if mode == MODE_BOUNDED:
        policy["k"] = 3
        policy["time_bound_days"] = 7
    elif mode == MODE_LAZY:
        policy["escalation"] = "eventual"
    return spec


# --------------------------------------------------------------------------- #
# EXPRESSIBLE: the specification can say all four, and they are distinct.      #
# --------------------------------------------------------------------------- #
def test_all_four_policy_modes_are_expressible(registry_and_digests, parsers) -> None:
    registry, digests = registry_and_digests
    digest_by_mode = {}
    for mode in MODES:
        loaded = _load(_mode_spec(digests, mode), registry, parsers)
        assert loaded is not None, mode
        digest_by_mode[mode] = loaded.spec_digest
    assert len(set(digest_by_mode.values())) == len(MODES), (
        "two policy modes produced the same spec digest — they would be the "
        f"same declaration: {digest_by_mode}"
    )


# --------------------------------------------------------------------------- #
# INDISTINGUISHABLE: nothing outside the loader reads the mode.                #
# --------------------------------------------------------------------------- #
def _modules_referencing(symbols: set[str]) -> set[str]:
    """Source modules, EXCLUDING the loader, that name any of these symbols."""
    out: set[str] = set()
    for path in SRC.rglob("*.py"):
        rel = str(path.relative_to(SRC))
        if rel == LOADER_MODULE:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover
            continue
        for node in ast.walk(tree):
            name = None
            if isinstance(node, ast.Name):
                name = node.id
            elif isinstance(node, ast.Attribute):
                name = node.attr
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                name = node.value
            if name in symbols:
                out.add(rel)
    return out


def test_no_policy_mode_reaches_a_runtime() -> None:
    """THE finding, as an assertion so that wiring a mode BREAKS it and forces
    the claim to be revised rather than quietly outliving the code."""
    consumers = _modules_referencing({
        "MODE_STRICT", "MODE_BOUNDED", "MODE_EVENTUAL", "MODE_LAZY",
    })
    assert consumers == set(), (
        "a policy mode gained a consumer outside the loader — consistency "
        f"semantics may now have runtime behaviour: {sorted(consumers)}"
    )


def test_the_conformance_verdict_is_reported(registry_and_digests, parsers) -> None:
    """The deliverable: a per-mode verdict with the observation behind it."""
    registry, digests = registry_and_digests
    consumers = _modules_referencing({
        "MODE_STRICT", "MODE_BOUNDED", "MODE_EVENTUAL", "MODE_LAZY",
    })
    verdict = {}
    for mode in MODES:
        expressible = _load(_mode_spec(digests, mode), registry, parsers) is not None
        verdict[mode] = {
            "expressible": expressible,
            "behaviourally_distinguishable": bool(consumers),
        }
    print("\n\nCONSISTENCY-SEMANTICS CONFORMANCE")
    print("  mode        expressible   distinguishable by any runtime")
    for mode, cell in verdict.items():
        print(f"  {mode:<12}{str(cell['expressible']):<14}"
              f"{cell['behaviourally_distinguishable']}")
    print("\n  All four are EXPRESSIBLE and NONE is DISTINGUISHABLE: the")
    print("  specification can say four different things about how hard to try,")
    print("  and the pipeline does the same thing for all four.")
    print("\n  Smallest change that would make one observable: have the reuse")
    print("  scheduler read the invariant's policy mode and refuse to skip a")
    print("  STRICT invariant's check on a pass where it skipped a LAZY one.")
    print("  That is one consumer, and it would flip this module's verdict.")

    assert all(c["expressible"] for c in verdict.values())
    assert not any(c["behaviourally_distinguishable"] for c in verdict.values())


def test_a_bounded_policy_still_refuses_without_a_clock(
    registry_and_digests, parsers
) -> None:
    """Expressibility is not vacuous: the loader still enforces the mode's own
    coherence rules, so 'expressible' means accepted-when-valid rather than
    accepted-always."""
    from okg.substrate.consistency.invariant_spec import SpecLoadRefused

    registry, digests = registry_and_digests
    spec = _mode_spec(digests, MODE_BOUNDED)
    spec["invariants"][0]["policy"]["k"] = 0
    spec["invariants"][0]["policy"]["time_bound_days"] = 0
    spec["defaults"]["bounded_k"] = 0
    spec["defaults"]["bounded_time_bound_days"] = 0
    with pytest.raises(SpecLoadRefused):
        _load(spec, registry, parsers)
