"""Stage 5 cross-cutting tests for ``add-incremental-views``.

Three gates the framework's invariants must pass end-to-end,
not just at the unit level:

1. **Property-A positive isolation** through ``refresh()``: an
   out-of-scope corpus addition produces a byte-identical
   assembled view, with zero LLM calls.

2. **Hermeticity fault injection** through ``refresh()``: a
   custom synthesizer that tries to read an undeclared anchor
   raises :class:`HermeticityViolation` without any chance to
   silently succeed.

3. **Cross-interpreter planner determinism**: the
   ``supersedure_health`` planner produces byte-identical
   FragmentSpec lists across two ``subprocess.run`` calls with
   different ``PYTHONHASHSEED`` values. Catches dict-iteration
   / set-ordering / salted-``hash()`` failure modes that
   same-process tests miss.

The exact-counts test against the live af_pubs substrate
(R1=627, R2=620, R3=7, R4=12) lives in
``test_supersedure_health_counts.py``.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from okg.views.assembly import assemble
from okg.views.execution.llm import DeterministicStubLLM
from okg.views.planners.base import (
    GeneratedSpec,
    QuoteSpec,
    ViewDefinition,
)
from okg.views.refresh import fresh, refresh
from okg.views.restricted_corpus import HermeticityViolation
from okg.views.types import Anchor


# ---------------------------------------------------------------------------
# 5.1(a) Positive isolation through refresh
# ---------------------------------------------------------------------------


class TestPositiveHermeticIsolation:
    """Stage 5 gate: out-of-scope corpus growth → byte-identical view."""

    def test_unrelated_anchor_addition_yields_byte_identical_assembled_view(self):
        corpus_before = {
            "a:catalog": Anchor.from_content("a:catalog", "status: current"),
            "b:catalog": Anchor.from_content("b:catalog", "status: rescinded"),
        }

        def planner(_corpus, _view_def):
            return [
                QuoteSpec(id="row:a", anchor_id="a:catalog"),
                QuoteSpec(id="row:b", anchor_id="b:catalog"),
            ]

        view_def = ViewDefinition(
            view_id="v",
            planner=planner,
            assembler=assemble,
            view_def_hash="v1",
        )

        cached, _ = fresh(
            new_corpus=corpus_before,
            view_def=view_def,
            llm_client=DeterministicStubLLM(),
        )
        rendered_before = assemble(cached)

        # Add a new anchor that no fragment references.
        corpus_after = dict(corpus_before)
        corpus_after["unrelated:catalog"] = Anchor.from_content(
            "unrelated:catalog", "status: current",
        )

        new_view, stats = refresh(
            cached_view=cached,
            new_corpus=corpus_after,
            view_def=view_def,
            llm_client=DeterministicStubLLM(),
        )

        # The umbrella's Property-A compositional-isolation
        # corollary in three concrete checks:
        rendered_after = assemble(new_view)
        assert rendered_after == rendered_before  # byte-identical
        assert stats.refreshed == 0
        assert stats.llm_calls == 0
        # Every fragment is the SAME Python object (strict reuse).
        for old, new in zip(cached.fragments, new_view.fragments, strict=True):
            assert old is new


# ---------------------------------------------------------------------------
# 5.1(b) Hermeticity fault injection through refresh
# ---------------------------------------------------------------------------


class TestFaultInjectedHermeticity:
    """Stage 5 gate: a malicious synthesizer cannot silently
    read undeclared anchors. The framework rejects it
    structurally."""

    def test_custom_synthesize_reading_undeclared_raises(self):
        # The corpus contains TWO anchors, but our spec declares
        # only one as a source. The malicious synthesize tries
        # to read the OTHER anchor through the wrapper —
        # RestrictedCorpus must raise.
        corpus = {
            "declared:catalog": Anchor.from_content(
                "declared:catalog", "status: current",
            ),
            "secret:catalog": Anchor.from_content(
                "secret:catalog", "status: rescinded",
            ),
        }

        def planner(_corpus, _view_def):
            # Declares ONLY "declared:catalog" — synthesizer's
            # attempt to reach "secret:catalog" must fail.
            return [GeneratedSpec(
                id="x",
                sources=("declared:catalog",),
                system_prompt="",
                task_prompt="",
            )]

        def malicious_synthesize(spec, restricted_corpus, llm_client, **_):
            # Attempt the undeclared read. RestrictedCorpus
            # raises before the LLM ever sees the prompt.
            _ = restricted_corpus["secret:catalog"]
            raise AssertionError(
                "RestrictedCorpus should have raised before this line"
            )

        view_def = ViewDefinition(
            view_id="v",
            planner=planner,
            assembler=assemble,
            view_def_hash="v1",
        )

        with pytest.raises(HermeticityViolation) as excinfo:
            fresh(
                new_corpus=corpus,
                view_def=view_def,
                llm_client=DeterministicStubLLM(),
                synthesize=malicious_synthesize,
            )
        # Error message names the violating id and the declared
        # sources so a planner author sees the diagnosis at a
        # glance.
        msg = str(excinfo.value)
        assert "'secret:catalog'" in msg
        assert "['declared:catalog']" in msg

    def test_undeclared_read_in_quote_path_also_raises(self):
        # The default synthesize for QuoteSpec reads only
        # ``spec.anchor_id``. We exercise the same fault model
        # via a malicious synthesize on a quote spec.
        corpus = {
            "a:catalog": Anchor.from_content("a:catalog", "A"),
            "b:catalog": Anchor.from_content("b:catalog", "B"),
        }

        def planner(_corpus, _view_def):
            return [QuoteSpec(id="row:a", anchor_id="a:catalog")]

        def malicious_synthesize(spec, restricted_corpus, llm_client, **_):
            return restricted_corpus["b:catalog"]

        view_def = ViewDefinition(
            view_id="v",
            planner=planner,
            assembler=assemble,
            view_def_hash="v1",
        )

        with pytest.raises(HermeticityViolation):
            fresh(
                new_corpus=corpus,
                view_def=view_def,
                llm_client=DeterministicStubLLM(),
                synthesize=malicious_synthesize,
            )


# ---------------------------------------------------------------------------
# 5.2 Cross-interpreter planner determinism
# ---------------------------------------------------------------------------


_DRIVER = Path(__file__).parent / "_determinism_driver.py"
_AF_PUBS_DEPLOYMENT = Path(__file__).resolve().parents[2] / "deployments" / "af_pubs"
_AF_PUBS_SUPERSEDURE_VIEW = (
    _AF_PUBS_DEPLOYMENT / "views" / "supersedure_health.py"
)


def _run_driver(seed: str) -> list[str]:
    """Run ``_determinism_driver.py`` in a subprocess with the
    given ``PYTHONHASHSEED`` and return the parsed spec-id list.
    """
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = seed
    result = subprocess.run(
        [sys.executable, str(_DRIVER)],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    payload = result.stdout.strip()
    if not payload:
        raise AssertionError(
            f"driver produced no stdout for seed={seed!r}; "
            f"stderr: {result.stderr[:400]}"
        )
    return json.loads(payload)


@pytest.mark.skipif(
    not _AF_PUBS_SUPERSEDURE_VIEW.is_file(),
    reason="af_pubs deployment views are optional in this workspace",
)
class TestCrossInterpreterDeterminism:
    """Stage 5 gate: the supersedure_health planner is
    deterministic across Python interpreter processes with
    different ``PYTHONHASHSEED`` values.

    Catches the failure modes that same-process tests miss:

    * salted ``hash()`` of strings → different bucket order
      across processes
    * ``set`` iteration order
    * ``dict`` iteration over non-sorted keys

    The driver script lives at ``_determinism_driver.py`` so it
    can be inspected and re-run by hand (``uv run python
    tests/views/_determinism_driver.py``) when debugging a
    failure.
    """

    def test_pythonhashseed_zero_vs_42_produce_identical_specs(self):
        out_zero = _run_driver("0")
        out_fortytwo = _run_driver("42")
        assert out_zero == out_fortytwo, (
            "supersedure_health_planner produced different "
            "FragmentSpec orderings across PYTHONHASHSEED=0 and "
            "PYTHONHASHSEED=42 — likely caused by an unsorted "
            "dict / set / hash() iteration in the planner."
        )

    def test_specs_contain_expected_findings(self):
        # Sanity-check on the synthetic corpus the driver builds:
        # R1, R2, R3 each fire on at least one edge; R4 fires on
        # the date-inverted pub. Confirms the determinism check
        # actually exercised non-trivial planner output.
        out = _run_driver("0")
        rules_present = {
            spec_id.split(":")[1]
            for spec_id in out
            if spec_id.startswith("finding:")
        }
        assert rules_present == {"R1", "R2", "R3", "R4"}
